package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utxochecksum"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

var (
	metaProfileKey           = []byte("meta/profile")
	metaHeaderTipHeightKey   = []byte("meta/header_tip_height")
	metaHeaderTipHeaderKey   = []byte("meta/header_tip_header")
	metaTipHeightKey         = []byte("meta/tip_height")
	metaTipHeaderKey         = []byte("meta/tip_header")
	metaBlockSizeStateKey    = []byte("meta/block_size_state")
	metaUTXOChecksumKey      = []byte("meta/utxo_checksum")
	metaFastSyncStateKey     = []byte("meta/fast_sync_state")
	metaMempoolStateKey      = []byte("meta/mempool_state")
	metaLocalityNextSeqKey   = []byte("meta/locality_next_seq")
	metaJournalNextSeqKey    = []byte("meta/journal_next_seq")
	metaDerivedJournalSeqKey = []byte("meta/derived_journal_seq")
	mempoolEntryPrefix       = []byte("mempool_entry/")
	mempoolOrphanPrefix      = []byte("mempool_orphan/")
	blockPrefix              = []byte("blocks/")
	blockIndexPrefix         = []byte("block_index/")
	blockUndoPrefix          = []byte("block_undo/")
	headerHeightIndexPrefix  = []byte("header_height_index/")
	heightIndexPrefix        = []byte("height_index/")
	knownPeerPrefix          = []byte("known_peer/")
	journalPrefix            = []byte("journal/")
	utxoPrefix               = []byte("utxo/")
	snapshotUTXOPrefix       = []byte("snapshot_utxo/")
	localitySeqPrefix        = []byte("locality_seq/")
	localityMetaPrefix       = []byte("locality_meta/")
)

var (
	knownPeerPrefixEnd     = prefixUpperBound(knownPeerPrefix)
	mempoolEntryPrefixEnd  = prefixUpperBound(mempoolEntryPrefix)
	mempoolOrphanPrefixEnd = prefixUpperBound(mempoolOrphanPrefix)
	utxoPrefixEnd          = prefixUpperBound(utxoPrefix)
	snapshotUTXOPrefixEnd  = prefixUpperBound(snapshotUTXOPrefix)
	localitySeqPrefixEnd   = prefixUpperBound(localitySeqPrefix)
	localityMetaPrefixEnd  = prefixUpperBound(localityMetaPrefix)
)

var (
	// Canonical chain/header state must cross a durable sync boundary before the
	// store acknowledges progress to higher layers.
	consensusCriticalWriteOptions = pebble.Sync
	// Non-consensus metadata can remain best-effort and rely on Pebble's normal
	// flush cadence.
	bestEffortWriteOptions = pebble.NoSync
)

type StoredChainState struct {
	Profile        types.ChainProfile
	Height         uint64
	TipHeader      types.BlockHeader
	BlockSizeState consensus.BlockSizeState
	UTXOChecksum   [32]byte
	UTXOs          consensus.UtxoSet
}

// StoredChainStateMeta holds the chain tip metadata without materializing the
// full UTXO set. This is the additive building block for the disk-backed UTXO
// migration path.
type StoredChainStateMeta struct {
	Profile        types.ChainProfile
	Height         uint64
	TipHeader      types.BlockHeader
	BlockSizeState consensus.BlockSizeState
	UTXOChecksum   [32]byte
}

// FastSyncState persists the trust boundary for an imported snapshot until a
// background replay from genesis reconstructs the same state locally.
type FastSyncState struct {
	SnapshotHeight     uint64   `json:"snapshot_height"`
	SnapshotHeaderHash [32]byte `json:"snapshot_header_hash"`
	SnapshotUTXORoot   [32]byte `json:"snapshot_utxo_root"`
	SnapshotChecksum   [32]byte `json:"snapshot_utxo_checksum"`
	SnapshotUTXOCount  int      `json:"snapshot_utxo_count"`
	LastError          string   `json:"last_error,omitempty"`
}

type LocalityIndexedUTXO struct {
	Sequence uint64
	OutPoint types.OutPoint
	Entry    consensus.UtxoEntry
}

type KnownPeerRecord struct {
	LastSeen     time.Time
	LastSuccess  time.Time
	LastAttempt  time.Time
	BannedUntil  time.Time
	FailureCount uint32
	Manual       bool
}

type StoredHeaderState struct {
	Profile   types.ChainProfile
	Height    uint64
	TipHeader types.BlockHeader
}

// StoredMempoolState keeps restart-only tx relay state out of consensus tables.
// The encoded transactions are replayed through normal mempool policy when the
// persisted tip no longer matches the committed chain tip.
type StoredMempoolState struct {
	Version   uint32               `json:"version"`
	Profile   types.ChainProfile   `json:"profile"`
	TipHeight uint64               `json:"tip_height"`
	TipHash   [32]byte             `json:"tip_hash"`
	Entries   []StoredMempoolEntry `json:"entries,omitempty"`
	Orphans   []StoredMempoolEntry `json:"orphans,omitempty"`
}

// StoredMempoolStateMeta is the durable checkpoint header for restart-only
// mempool persistence. The actual accepted/orphan records live under per-tx
// keys so flushes can upsert or delete only what changed.
type StoredMempoolStateMeta struct {
	Version   uint32             `json:"version"`
	Profile   types.ChainProfile `json:"profile"`
	TipHeight uint64             `json:"tip_height"`
	TipHash   [32]byte           `json:"tip_hash"`
}

type StoredMempoolEntry struct {
	Tx      []byte                        `json:"tx"`
	Summary consensus.TxValidationSummary `json:"summary"`
	AddedAt uint64                        `json:"added_at"`
	Missing []types.OutPoint              `json:"missing,omitempty"`
}

type StoredMempoolDeltaEntry struct {
	TxID  [32]byte
	Entry StoredMempoolEntry
}

// StoredMempoolStateDelta applies one atomically visible checkpoint update: a
// new tip header plus any accepted/orphan record upserts and deletes needed to
// bring the stored restart cache in sync with the live mempool.
type StoredMempoolStateDelta struct {
	Meta          StoredMempoolStateMeta
	EntryUpserts  []StoredMempoolDeltaEntry
	EntryDeletes  [][32]byte
	OrphanUpserts []StoredMempoolDeltaEntry
	OrphanDeletes [][32]byte
}

type BlockIndexEntry struct {
	Height         uint64
	ParentHash     [32]byte
	Header         types.BlockHeader
	ChainWork      [32]byte
	Validated      bool
	BlockSizeState consensus.BlockSizeState
}

type BlockUndoEntry struct {
	OutPoint types.OutPoint
	Entry    consensus.UtxoEntry
}

type HeaderBatchEntry struct {
	Height    uint64
	Header    types.BlockHeader
	ChainWork [32]byte
}

type ChainStore struct {
	db     *pebble.DB
	logger *slog.Logger

	deriveNotify chan struct{}
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

type chainJournalKind uint8

const (
	journalSetBlockHeight chainJournalKind = 1 + iota
	journalRewriteBlockHeights
	journalSetHeaderHeight
	journalRewriteHeaderHeights
)

type chainJournalHeightHash struct {
	Height uint64
	Hash   [32]byte
}

type chainJournalEntry struct {
	Kind         chainJournalKind
	ForkHeight   uint64
	OldTipHeight uint64
	Pairs        []chainJournalHeightHash
}

type noopLogger struct{}

func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Fatalf(string, ...interface{}) {}

// OpenOptions holds optional Pebble tuning for point-read-heavy workloads.
// Zero values preserve Pebble defaults so existing callers keep their current
// behavior unless they opt in.
type OpenOptions struct {
	PebbleCacheBytes      int64
	BloomFilterBitsPerKey int
}

func Open(path string) (*ChainStore, error) {
	return OpenWithLogger(path, logging.Component("storage"))
}

func OpenWithLogger(path string, logger *slog.Logger) (*ChainStore, error) {
	return OpenWithLoggerAndOptions(path, logger, OpenOptions{})
}

func OpenWithLoggerAndOptions(path string, logger *slog.Logger, opts OpenOptions) (*ChainStore, error) {
	if logger == nil {
		logger = logging.Component("storage")
	}
	logger.Info("opening pebble chain store", slog.String("path", path))
	pebbleOpts := &pebble.Options{Logger: noopLogger{}}
	if opts.PebbleCacheBytes > 0 {
		cache := pebble.NewCache(opts.PebbleCacheBytes)
		defer cache.Unref()
		pebbleOpts.Cache = cache
	}
	if opts.BloomFilterBitsPerKey > 0 {
		pebbleOpts.Levels = []pebble.LevelOptions{{
			FilterPolicy: bloom.FilterPolicy(opts.BloomFilterBitsPerKey),
			FilterType:   pebble.TableFilter,
		}}
	}
	db, err := pebble.Open(filepath.Clean(path), pebbleOpts)
	if err != nil {
		return nil, err
	}
	store := &ChainStore{
		logger:       logger,
		db:           db,
		deriveNotify: make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}
	store.wg.Add(1)
	go func() {
		defer store.wg.Done()
		store.derivedIndexLoop()
	}()
	store.notifyDerivedReplay()
	return store, nil
}

func (s *ChainStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	s.logger.Info("closing pebble chain store")
	if s.stopCh != nil {
		close(s.stopCh)
	}
	s.wg.Wait()
	return s.db.Close()
}

func (s *ChainStore) LoadChainState() (*StoredChainState, error) {
	meta, hasChecksum, err := s.loadChainStateMeta()
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, nil
	}
	utxos := make(consensus.UtxoSet)
	if err := s.ForEachUTXO(func(outpoint types.OutPoint, entry consensus.UtxoEntry) error {
		utxos[outpoint] = entry
		return nil
	}); err != nil {
		return nil, err
	}
	checksum := meta.UTXOChecksum
	if !hasChecksum {
		checksum = utxochecksum.Compute(utxos)
	}
	return &StoredChainState{
		Profile:        meta.Profile,
		Height:         meta.Height,
		TipHeader:      meta.TipHeader,
		BlockSizeState: meta.BlockSizeState,
		UTXOChecksum:   checksum,
		UTXOs:          utxos,
	}, nil
}

// LoadChainStateMeta loads only the persisted canonical chain metadata. It
// intentionally avoids scanning the UTXO keyspace so callers can opt into a
// disk-backed view without paying the startup RAM cost up front.
func (s *ChainStore) LoadChainStateMeta() (*StoredChainStateMeta, error) {
	meta, _, err := s.loadChainStateMeta()
	return meta, err
}

func (s *ChainStore) loadChainStateMeta() (*StoredChainStateMeta, bool, error) {
	profileBytes, err := s.get(metaProfileKey)
	if err != nil {
		return nil, false, err
	}
	if profileBytes == nil {
		return nil, false, nil
	}
	heightBytes, err := s.get(metaTipHeightKey)
	if err != nil {
		return nil, false, err
	}
	headerBytes, err := s.get(metaTipHeaderKey)
	if err != nil {
		return nil, false, err
	}
	blockSizeBytes, err := s.get(metaBlockSizeStateKey)
	if err != nil {
		return nil, false, err
	}
	checksumBytes, err := s.get(metaUTXOChecksumKey)
	if err != nil {
		return nil, false, err
	}
	if heightBytes == nil && headerBytes == nil && blockSizeBytes == nil {
		return nil, false, nil
	}
	if heightBytes == nil || headerBytes == nil || blockSizeBytes == nil {
		return nil, false, errors.New("invalid data: missing chain metadata")
	}

	profile, err := types.ParseChainProfile(string(profileBytes))
	if err != nil {
		return nil, false, err
	}
	height, err := decodeU64(heightBytes)
	if err != nil {
		return nil, false, err
	}
	header, err := types.DecodeBlockHeader(headerBytes)
	if err != nil {
		return nil, false, err
	}
	blockSizeState, err := decodeBlockSizeState(blockSizeBytes)
	if err != nil {
		return nil, false, err
	}
	var checksum [32]byte
	switch {
	case checksumBytes == nil:
		// Older stores may predate persisted checksum metadata. Callers that also
		// need the UTXO set can recompute it after scanning.
	case len(checksumBytes) != len(checksum):
		return nil, false, errors.New("invalid data: bad utxo checksum metadata")
	default:
		copy(checksum[:], checksumBytes)
	}
	return &StoredChainStateMeta{
		Profile:        profile,
		Height:         height,
		TipHeader:      header,
		BlockSizeState: blockSizeState,
		UTXOChecksum:   checksum,
	}, checksumBytes != nil, nil
}

// GetUTXO performs a single-key Pebble lookup for a committed UTXO entry.
// A returned nil error cleanly distinguishes "not found" from I/O failure.
func (s *ChainStore) GetUTXO(outPoint types.OutPoint) (consensus.UtxoEntry, bool, error) {
	val, closer, err := s.db.Get(utxoKey(outPoint))
	if errors.Is(err, pebble.ErrNotFound) {
		return consensus.UtxoEntry{}, false, nil
	}
	if err != nil {
		return consensus.UtxoEntry{}, false, err
	}
	defer closer.Close()
	entry, err := decodeUTXOEntry(val)
	if err != nil {
		return consensus.UtxoEntry{}, false, err
	}
	return entry, true, nil
}

// UTXOLookupWithErr exposes a consensus lookup that preserves I/O failures for
// correctness-critical callers.
func (s *ChainStore) UTXOLookupWithErr() consensus.UtxoLookupWithErr {
	return func(out types.OutPoint) (consensus.UtxoEntry, bool, error) {
		return s.GetUTXO(out)
	}
}

// UTXOLookupFunc exposes a read-only lookup that treats disk faults like
// misses. This is suitable for non-consensus read paths only.
func (s *ChainStore) UTXOLookupFunc() consensus.UtxoLookup {
	return func(out types.OutPoint) (consensus.UtxoEntry, bool) {
		entry, ok, _ := s.GetUTXO(out)
		return entry, ok
	}
}

// ForEachUTXO scans the committed UTXO keyspace in key order.
func (s *ChainStore) ForEachUTXO(fn func(types.OutPoint, consensus.UtxoEntry) error) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: utxoPrefix,
		UpperBound: utxoPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		outpoint, err := decodeOutPoint(iter.Key()[len(utxoPrefix):])
		if err != nil {
			return err
		}
		entry, err := decodeUTXOEntry(iter.Value())
		if err != nil {
			return err
		}
		if err := fn(outpoint, entry); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *ChainStore) LoadFastSyncState() (*FastSyncState, error) {
	buf, err := s.get(metaFastSyncStateKey)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	var state FastSyncState
	if err := json.Unmarshal(buf, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *ChainStore) LoadFastSyncSnapshotUTXOs() (consensus.UtxoSet, error) {
	utxos := make(consensus.UtxoSet)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: snapshotUTXOPrefix,
		UpperBound: snapshotUTXOPrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		outpoint, err := decodeOutPoint(iter.Key()[len(snapshotUTXOPrefix):])
		if err != nil {
			return nil, err
		}
		entry, err := decodeUTXOEntry(iter.Value())
		if err != nil {
			return nil, err
		}
		utxos[outpoint] = entry
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	if len(utxos) == 0 {
		return nil, nil
	}
	return utxos, nil
}

func (s *ChainStore) LoadLocalityOrderedUTXOs(limit int) ([]LocalityIndexedUTXO, error) {
	items := make([]LocalityIndexedUTXO, 0)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: localitySeqPrefix,
		UpperBound: localitySeqPrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if limit > 0 && len(items) >= limit {
			break
		}
		seq, err := decodeLocalitySeqFromKey(iter.Key())
		if err != nil {
			return nil, err
		}
		outPoint, err := decodeOutPoint(iter.Value())
		if err != nil {
			return nil, err
		}
		entryBuf, err := s.get(utxoKey(outPoint))
		if err != nil {
			return nil, err
		}
		if entryBuf == nil {
			// The locality index is non-consensus metadata. If a stale row slips
			// through during recovery, skip it instead of poisoning canonical UTXO
			// reads.
			continue
		}
		entry, err := decodeUTXOEntry(entryBuf)
		if err != nil {
			return nil, err
		}
		items = append(items, LocalityIndexedUTXO{
			Sequence: seq,
			OutPoint: outPoint,
			Entry:    entry,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *ChainStore) LocalitySequence(outPoint types.OutPoint) (uint64, bool, error) {
	return s.localitySeqForOutPoint(outPoint)
}

func (s *ChainStore) LoadHeaderState() (*StoredHeaderState, error) {
	profileBytes, err := s.get(metaProfileKey)
	if err != nil {
		return nil, err
	}
	if profileBytes == nil {
		return nil, nil
	}

	heightBytes, err := s.get(metaHeaderTipHeightKey)
	if err != nil {
		return nil, err
	}
	headerBytes, err := s.get(metaHeaderTipHeaderKey)
	if err != nil {
		return nil, err
	}
	if heightBytes == nil && headerBytes == nil {
		heightBytes, err = s.get(metaTipHeightKey)
		if err != nil {
			return nil, err
		}
		headerBytes, err = s.get(metaTipHeaderKey)
		if err != nil {
			return nil, err
		}
	}
	if heightBytes == nil || headerBytes == nil {
		return nil, nil
	}

	profile, err := types.ParseChainProfile(string(profileBytes))
	if err != nil {
		return nil, err
	}
	height, err := decodeU64(heightBytes)
	if err != nil {
		return nil, err
	}
	header, err := types.DecodeBlockHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	return &StoredHeaderState{
		Profile:   profile,
		Height:    height,
		TipHeader: header,
	}, nil
}

func (s *ChainStore) LoadMempoolState() (*StoredMempoolState, error) {
	buf, err := s.get(metaMempoolStateKey)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	var meta StoredMempoolStateMeta
	if err := json.Unmarshal(buf, &meta); err != nil {
		return nil, err
	}
	// Legacy restart checkpoints stored the full mempool in one JSON blob. Keep
	// decoding that format so reopening an older local data dir does not discard
	// restart state just because the storage layout changed.
	if meta.Version < 2 {
		var legacy StoredMempoolState
		if err := json.Unmarshal(buf, &legacy); err != nil {
			return nil, err
		}
		return &legacy, nil
	}
	entries, err := s.loadMempoolEntries(mempoolEntryPrefix, mempoolEntryPrefixEnd)
	if err != nil {
		return nil, err
	}
	orphans, err := s.loadMempoolEntries(mempoolOrphanPrefix, mempoolOrphanPrefixEnd)
	if err != nil {
		return nil, err
	}
	return &StoredMempoolState{
		Version:   meta.Version,
		Profile:   meta.Profile,
		TipHeight: meta.TipHeight,
		TipHash:   meta.TipHash,
		Entries:   entries,
		Orphans:   orphans,
	}, nil
}

func (s *ChainStore) WriteMempoolState(state *StoredMempoolState) error {
	if state == nil {
		return errors.New("mempool state is required")
	}
	version := state.Version
	if version < 2 {
		version = 2
	}
	delta := StoredMempoolStateDelta{
		Meta: StoredMempoolStateMeta{
			// Version 2 switches the on-disk layout to metadata plus per-tx keys.
			Version:   version,
			Profile:   state.Profile,
			TipHeight: state.TipHeight,
			TipHash:   state.TipHash,
		},
		EntryUpserts:  make([]StoredMempoolDeltaEntry, 0, len(state.Entries)),
		OrphanUpserts: make([]StoredMempoolDeltaEntry, 0, len(state.Orphans)),
	}
	for _, entry := range state.Entries {
		tx, err := types.DecodeTransactionWithLimits(entry.Tx, types.DefaultCodecLimits())
		if err != nil {
			return err
		}
		delta.EntryUpserts = append(delta.EntryUpserts, StoredMempoolDeltaEntry{
			TxID:  consensus.TxID(&tx),
			Entry: entry,
		})
	}
	for _, orphan := range state.Orphans {
		tx, err := types.DecodeTransactionWithLimits(orphan.Tx, types.DefaultCodecLimits())
		if err != nil {
			return err
		}
		delta.OrphanUpserts = append(delta.OrphanUpserts, StoredMempoolDeltaEntry{
			TxID:  consensus.TxID(&tx),
			Entry: orphan,
		})
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.clearMempoolStateWithBatch(batch); err != nil {
		return err
	}
	if err := applyMempoolStateDeltaWithBatch(batch, delta); err != nil {
		return err
	}
	if err := batch.Commit(bestEffortWriteOptions); err != nil {
		return err
	}
	s.logger.Debug("wrote full mempool state",
		slog.Uint64("tip_height", delta.Meta.TipHeight),
		slog.Int("entries", len(delta.EntryUpserts)),
		slog.Int("orphans", len(delta.OrphanUpserts)),
	)
	return nil
}

func (s *ChainStore) ApplyMempoolStateDelta(delta StoredMempoolStateDelta) error {
	if delta.Meta.Profile == "" {
		return errors.New("mempool state profile is required")
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := applyMempoolStateDeltaWithBatch(batch, delta); err != nil {
		return err
	}
	if err := batch.Commit(bestEffortWriteOptions); err != nil {
		return err
	}
	s.logger.Debug("applied mempool state delta",
		slog.Uint64("tip_height", delta.Meta.TipHeight),
		slog.Int("entry_upserts", len(delta.EntryUpserts)),
		slog.Int("entry_deletes", len(delta.EntryDeletes)),
		slog.Int("orphan_upserts", len(delta.OrphanUpserts)),
		slog.Int("orphan_deletes", len(delta.OrphanDeletes)),
	)
	return nil
}

func (s *ChainStore) ClearMempoolState() error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.clearMempoolStateWithBatch(batch); err != nil {
		return err
	}
	if err := batch.Commit(bestEffortWriteOptions); err != nil {
		return err
	}
	s.logger.Debug("cleared mempool state")
	return nil
}

func (s *ChainStore) clearMempoolStateWithBatch(batch *pebble.Batch) error {
	if batch == nil {
		return errors.New("mempool clear batch is required")
	}
	if err := batch.Delete(metaMempoolStateKey, bestEffortWriteOptions); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	for _, prefixRange := range []struct {
		lower []byte
		upper []byte
	}{
		{lower: mempoolEntryPrefix, upper: mempoolEntryPrefixEnd},
		{lower: mempoolOrphanPrefix, upper: mempoolOrphanPrefixEnd},
	} {
		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: prefixRange.lower,
			UpperBound: prefixRange.upper,
		})
		if err != nil {
			return err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			if err := batch.Delete(append([]byte(nil), iter.Key()...), bestEffortWriteOptions); err != nil && !errors.Is(err, pebble.ErrNotFound) {
				_ = iter.Close()
				return err
			}
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *ChainStore) loadMempoolEntries(lower, upper []byte) ([]StoredMempoolEntry, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	entries := make([]StoredMempoolEntry, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		var entry StoredMempoolEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

func applyMempoolStateDeltaWithBatch(batch *pebble.Batch, delta StoredMempoolStateDelta) error {
	if batch == nil {
		return errors.New("mempool delta batch is required")
	}
	encoded, err := json.Marshal(delta.Meta)
	if err != nil {
		return err
	}
	if err := batch.Set(metaMempoolStateKey, encoded, bestEffortWriteOptions); err != nil {
		return err
	}
	for _, entry := range delta.EntryUpserts {
		if err := writeMempoolBatchEntry(batch, mempoolEntryPrefix, entry); err != nil {
			return err
		}
	}
	for _, txid := range delta.EntryDeletes {
		if err := batch.Delete(mempoolEntryKey(mempoolEntryPrefix, txid), bestEffortWriteOptions); err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
	}
	for _, orphan := range delta.OrphanUpserts {
		if err := writeMempoolBatchEntry(batch, mempoolOrphanPrefix, orphan); err != nil {
			return err
		}
	}
	for _, txid := range delta.OrphanDeletes {
		if err := batch.Delete(mempoolEntryKey(mempoolOrphanPrefix, txid), bestEffortWriteOptions); err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
	}
	return nil
}

func writeMempoolBatchEntry(batch *pebble.Batch, prefix []byte, entry StoredMempoolDeltaEntry) error {
	encoded, err := json.Marshal(entry.Entry)
	if err != nil {
		return err
	}
	return batch.Set(mempoolEntryKey(prefix, entry.TxID), encoded, bestEffortWriteOptions)
}

func mempoolEntryKey(prefix []byte, txid [32]byte) []byte {
	key := make([]byte, len(prefix)+len(txid))
	copy(key, prefix)
	copy(key[len(prefix):], txid[:])
	return key
}

func (s *ChainStore) LoadKnownPeers() (map[string]KnownPeerRecord, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: knownPeerPrefix,
		UpperBound: knownPeerPrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	peers := make(map[string]KnownPeerRecord)
	for iter.First(); iter.Valid(); iter.Next() {
		addr := string(iter.Key()[len(knownPeerPrefix):])
		if addr == "" {
			continue
		}
		record, err := decodeKnownPeerRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		peers[addr] = record
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return peers, nil
}

func (s *ChainStore) WriteKnownPeers(peers map[string]KnownPeerRecord) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: knownPeerPrefix,
		UpperBound: knownPeerPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for addr, record := range peers {
		if addr == "" {
			continue
		}
		if err := batch.Set(knownPeerKey(addr), encodeKnownPeerRecord(record), nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(bestEffortWriteOptions); err != nil {
		return err
	}
	s.logger.Debug("wrote known peers", slog.Int("count", len(peers)))
	return nil
}

func (s *ChainStore) WriteFullState(state *StoredChainState) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMeta(batch, state); err != nil {
		return err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: utxoPrefix,
		UpperBound: utxoPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for outPoint, entry := range state.UTXOs {
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(entry), nil); err != nil {
			return err
		}
	}
	if err := s.rebuildLocalityIndexBatch(batch, state.UTXOs); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	s.logger.Info("wrote full chain state",
		slog.Uint64("height", state.Height),
		slog.Int("utxo_count", len(state.UTXOs)),
	)
	return nil
}

func (s *ChainStore) WriteFastSyncState(state *FastSyncState, snapshot consensus.UtxoSet) error {
	if state == nil {
		return errors.New("fast sync state is required")
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(metaFastSyncStateKey, encoded, nil); err != nil {
		return err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: snapshotUTXOPrefix,
		UpperBound: snapshotUTXOPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for outPoint, entry := range snapshot {
		if err := batch.Set(snapshotUTXOKey(outPoint), encodeUTXOEntry(entry), nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.logger.Info("wrote fast-sync snapshot state",
		slog.Uint64("height", state.SnapshotHeight),
		slog.Int("utxo_count", len(snapshot)),
	)
	return nil
}

func (s *ChainStore) UpdateFastSyncState(state *FastSyncState) error {
	if state == nil {
		return errors.New("fast sync state is required")
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.db.Set(metaFastSyncStateKey, encoded, consensusCriticalWriteOptions)
}

func (s *ChainStore) ClearFastSyncState() error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Delete(metaFastSyncStateKey, nil); err != nil {
		return err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: snapshotUTXOPrefix,
		UpperBound: snapshotUTXOPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.logger.Info("cleared fast-sync snapshot state")
	return nil
}

func (s *ChainStore) RewriteFullStateDelta(previous *StoredChainState, next *StoredChainState) error {
	if next == nil {
		return errors.New("next chain state is required")
	}
	if previous == nil {
		return s.WriteFullState(next)
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMeta(batch, next); err != nil {
		return err
	}

	deleted := 0
	written := 0
	for outPoint, previousEntry := range previous.UTXOs {
		nextEntry, ok := next.UTXOs[outPoint]
		if !ok {
			if err := batch.Delete(utxoKey(outPoint), nil); err != nil {
				return err
			}
			deleted++
			continue
		}
		if nextEntry == previousEntry {
			continue
		}
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(nextEntry), nil); err != nil {
			return err
		}
		written++
	}
	for outPoint, nextEntry := range next.UTXOs {
		if previousEntry, ok := previous.UTXOs[outPoint]; ok && previousEntry == nextEntry {
			continue
		}
		if _, ok := previous.UTXOs[outPoint]; ok {
			continue
		}
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(nextEntry), nil); err != nil {
			return err
		}
		written++
	}
	if err := s.applyLocalityRewriteBatch(batch, previous.UTXOs, next.UTXOs); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	s.logger.Info("rewrote chain state via utxo delta",
		slog.Uint64("height", next.Height),
		slog.Int("deleted_utxos", deleted),
		slog.Int("written_utxos", written),
		slog.Int("final_utxo_count", len(next.UTXOs)),
	)
	return nil
}

// CommitReorgDelta atomically persists the post-reorg canonical metadata, UTXO
// delta, locality updates, and active-height journal rewrite. The validated
// branch blocks themselves are expected to have been stored already.
func (s *ChainStore) CommitReorgDelta(meta *StoredChainStateMeta, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry, forkHeight uint64, oldTipHeight uint64, activeEntries []BlockIndexEntry) error {
	if meta == nil {
		return errors.New("reorg chain metadata is required")
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMetaFromMeta(batch, meta); err != nil {
		return err
	}
	for _, outPoint := range spent {
		if err := batch.Delete(utxoKey(outPoint), nil); err != nil {
			return err
		}
	}
	for outPoint, entry := range created {
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(entry), nil); err != nil {
			return err
		}
	}
	if err := s.applyLocalityDeltaBatch(batch, spent, created); err != nil {
		return err
	}
	pairs := journalPairsFromEntries(activeEntries)
	if err := s.appendJournalEntriesBatch(batch,
		chainJournalEntry{
			Kind:         journalRewriteBlockHeights,
			ForkHeight:   forkHeight,
			OldTipHeight: oldTipHeight,
			Pairs:        pairs,
		},
		chainJournalEntry{
			Kind:         journalRewriteHeaderHeights,
			ForkHeight:   forkHeight,
			OldTipHeight: oldTipHeight,
			Pairs:        pairs,
		},
	); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	s.logger.Info("committed reorg delta",
		slog.Uint64("height", meta.Height),
		slog.Int("spent_utxos", len(spent)),
		slog.Int("created_utxos", len(created)),
		slog.Int("active_entries", len(activeEntries)),
	)
	return nil
}

func (s *ChainStore) WriteHeaderState(state *StoredHeaderState) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeHeaderMeta(batch, state); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&state.TipHeader)
	if err := s.appendJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetHeaderHeight,
		Pairs: []chainJournalHeightHash{{Height: state.Height, Hash: hash}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	s.logger.Debug("wrote header state", slog.Uint64("height", state.Height))
	return nil
}

func (s *ChainStore) CommitHeaderChain(state *StoredHeaderState, entries []HeaderBatchEntry, forkHeight uint64, oldTipHeight uint64, activeEntries []HeaderBatchEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeHeaderMeta(batch, state); err != nil {
		return err
	}
	for _, entry := range entries {
		indexEntry := BlockIndexEntry{
			Height:     entry.Height,
			ParentHash: entry.Header.PrevBlockHash,
			Header:     entry.Header,
			ChainWork:  entry.ChainWork,
		}
		preserved, err := s.preserveValidatedBlockIndex(indexEntry)
		if err != nil {
			return err
		}
		if err := putHeaderBatch(batch, preserved, false); err != nil {
			return err
		}
	}
	if len(activeEntries) != 0 {
		pairs := make([]chainJournalHeightHash, 0, len(activeEntries))
		for _, entry := range activeEntries {
			pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: consensus.HeaderHash(&entry.Header)})
		}
		if err := s.appendJournalEntryBatch(batch, chainJournalEntry{
			Kind:         journalRewriteHeaderHeights,
			ForkHeight:   forkHeight,
			OldTipHeight: oldTipHeight,
			Pairs:        pairs,
		}); err != nil {
			return err
		}
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	s.logger.Debug("wrote header batch",
		slog.Uint64("height", state.Height),
		slog.Int("count", len(entries)),
	)
	return nil
}

func (s *ChainStore) PutBlock(height uint64, block *types.Block) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	entry, err := s.buildLinearIndexEntry(height, &block.Header, false, consensus.BlockSizeState{})
	if err != nil {
		return err
	}
	if err := putBlockBatch(batch, block, entry, false); err != nil {
		return err
	}
	if err := s.appendJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetBlockHeight,
		Pairs: []chainJournalHeightHash{{Height: height, Hash: consensus.HeaderHash(&block.Header)}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	hash := consensus.HeaderHash(&block.Header)
	s.logger.Debug("stored block",
		slog.Uint64("height", height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
	)
	return nil
}

func (s *ChainStore) PutHeader(height uint64, header *types.BlockHeader) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	entry, err := s.buildLinearIndexEntry(height, header, false, consensus.BlockSizeState{})
	if err != nil {
		return err
	}
	entry, err = s.preserveValidatedBlockIndex(entry)
	if err != nil {
		return err
	}
	if err := putHeaderBatch(batch, entry, false); err != nil {
		return err
	}
	hash := consensus.HeaderHash(header)
	if err := s.appendJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetHeaderHeight,
		Pairs: []chainJournalHeightHash{{Height: height, Hash: hash}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	s.logger.Debug("stored header",
		slog.Uint64("height", height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
	)
	return nil
}

func (s *ChainStore) AppendBlock(state *StoredChainState, block *types.Block, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	entry, err := s.buildLinearIndexEntry(state.Height, &block.Header, true, state.BlockSizeState)
	if err != nil {
		return err
	}
	return s.AppendValidatedBlock(state, block, &entry, nil, spent, created)
}

func (s *ChainStore) AppendValidatedBlock(state *StoredChainState, block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMeta(batch, state); err != nil {
		return err
	}
	if err := putBlockBatch(batch, block, *entry, false); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	for _, outPoint := range spent {
		if err := batch.Delete(utxoKey(outPoint), nil); err != nil {
			return err
		}
	}
	for outPoint, entry := range created {
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(entry), nil); err != nil {
			return err
		}
	}
	if err := s.applyLocalityDeltaBatch(batch, spent, created); err != nil {
		return err
	}
	if err := s.appendJournalEntriesBatch(batch,
		chainJournalEntry{
			Kind:  journalSetBlockHeight,
			Pairs: []chainJournalHeightHash{{Height: state.Height, Hash: hash}},
		},
		chainJournalEntry{
			Kind:  journalSetHeaderHeight,
			Pairs: []chainJournalHeightHash{{Height: state.Height, Hash: hash}},
		},
	); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	s.logger.Debug("appended block delta",
		slog.Uint64("height", state.Height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
		slog.Int("spent_utxos", len(spent)),
		slog.Int("created_utxos", len(created)),
	)
	return nil
}

func (s *ChainStore) PutValidatedBlock(block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := putBlockBatch(batch, block, *entry, false); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) preserveValidatedBlockIndex(entry BlockIndexEntry) (BlockIndexEntry, error) {
	hash := consensus.HeaderHash(&entry.Header)
	existing, err := s.GetBlockIndex(&hash)
	if err != nil {
		return BlockIndexEntry{}, err
	}
	if existing == nil || !existing.Validated {
		return entry, nil
	}
	// Header-only persistence must never downgrade an already validated block
	// index entry back to header-only state for the same hash.
	entry.Validated = true
	entry.BlockSizeState = existing.BlockSizeState
	return entry, nil
}

func (s *ChainStore) RewriteActiveHeights(forkHeight uint64, oldTipHeight uint64, entries []BlockIndexEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	pairs := make([]chainJournalHeightHash, 0, len(entries))
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: hash})
	}
	if err := s.appendJournalEntryBatch(batch, chainJournalEntry{
		Kind:         journalRewriteBlockHeights,
		ForkHeight:   forkHeight,
		OldTipHeight: oldTipHeight,
		Pairs:        pairs,
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	return nil
}

func (s *ChainStore) RewriteActiveHeaderHeights(forkHeight uint64, oldTipHeight uint64, entries []BlockIndexEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	pairs := make([]chainJournalHeightHash, 0, len(entries))
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: hash})
	}
	if err := s.appendJournalEntryBatch(batch, chainJournalEntry{
		Kind:         journalRewriteHeaderHeights,
		ForkHeight:   forkHeight,
		OldTipHeight: oldTipHeight,
		Pairs:        pairs,
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	return nil
}

func (s *ChainStore) SetHeaderHashByHeight(height uint64, hash [32]byte) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.appendJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetHeaderHeight,
		Pairs: []chainJournalHeightHash{{Height: height, Hash: hash}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyDerivedReplay()
	return nil
}

func (s *ChainStore) GetBlock(blockHash *[32]byte) (*types.Block, error) {
	buf, err := s.get(blockKey(*blockHash))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	block, err := types.DecodeBlockWithLimits(buf, types.DefaultCodecLimits())
	if err != nil {
		return nil, err
	}
	return &block, nil
}

func (s *ChainStore) GetUndo(blockHash *[32]byte) ([]BlockUndoEntry, error) {
	buf, err := s.get(blockUndoKey(*blockHash))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return decodeBlockUndo(buf)
}

func (s *ChainStore) GetBlockHashByHeight(height uint64) (*[32]byte, error) {
	buf, err := s.get(heightIndexKey(height))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return s.derivedHashByHeight(height, false)
	}
	if len(buf) != 32 {
		return nil, errors.New("invalid height index encoding")
	}
	var hash [32]byte
	copy(hash[:], buf)
	return &hash, nil
}

func (s *ChainStore) GetHeaderHashByHeight(height uint64) (*[32]byte, error) {
	buf, err := s.get(headerHeightIndexKey(height))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return s.derivedHashByHeight(height, true)
	}
	if len(buf) != 32 {
		return nil, errors.New("invalid header height index encoding")
	}
	var hash [32]byte
	copy(hash[:], buf)
	return &hash, nil
}

func (s *ChainStore) GetIndexedHeaderHashByHeight(height uint64) (*[32]byte, error) {
	buf, err := s.get(headerHeightIndexKey(height))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	if len(buf) != 32 {
		return nil, errors.New("invalid header height index encoding")
	}
	var hash [32]byte
	copy(hash[:], buf)
	return &hash, nil
}

func (s *ChainStore) GetCanonicalHeaderHashByHeight(height uint64) (*[32]byte, error) {
	return s.derivedHashByHeight(height, true)
}

func (s *ChainStore) GetBlockIndex(blockHash *[32]byte) (*BlockIndexEntry, error) {
	buf, err := s.get(blockIndexKey(*blockHash))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return decodeBlockIndexEntry(buf)
}

func (s *ChainStore) GetBlockIndexByHeight(height uint64) (*BlockIndexEntry, error) {
	hash, err := s.GetBlockHashByHeight(height)
	if err != nil {
		return nil, err
	}
	if hash == nil {
		return nil, nil
	}
	return s.GetBlockIndex(hash)
}

func (s *ChainStore) GetBlockByHeight(height uint64) (*types.Block, error) {
	hash, err := s.GetBlockHashByHeight(height)
	if err != nil {
		return nil, err
	}
	if hash == nil {
		return nil, nil
	}
	return s.GetBlock(hash)
}

func (s *ChainStore) get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return cloneBytes(value), nil
}

func (s *ChainStore) derivedHashByHeight(height uint64, header bool) (*[32]byte, error) {
	tipHeightKey := metaTipHeightKey
	tipHeaderKey := metaTipHeaderKey
	if header {
		tipHeightKey = metaHeaderTipHeightKey
		tipHeaderKey = metaHeaderTipHeaderKey
	}
	heightBytes, err := s.get(tipHeightKey)
	if err != nil {
		return nil, err
	}
	headerBytes, err := s.get(tipHeaderKey)
	if err != nil {
		return nil, err
	}
	if (heightBytes == nil || headerBytes == nil) && header {
		heightBytes, err = s.get(metaTipHeightKey)
		if err != nil {
			return nil, err
		}
		headerBytes, err = s.get(metaTipHeaderKey)
		if err != nil {
			return nil, err
		}
	}
	if heightBytes == nil || headerBytes == nil {
		return nil, nil
	}
	tipHeight, err := decodeU64(heightBytes)
	if err != nil {
		return nil, err
	}
	if height > tipHeight {
		return nil, nil
	}
	tipHeader, err := types.DecodeBlockHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	hash := consensus.HeaderHash(&tipHeader)
	if height == tipHeight {
		return &hash, nil
	}
	cursorHash := hash
	cursorHeight := tipHeight
	for cursorHeight > height {
		entry, err := s.GetBlockIndex(&cursorHash)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, fmt.Errorf("missing block index for derived height lookup %x", cursorHash)
		}
		cursorHash = entry.ParentHash
		cursorHeight--
	}
	return &cursorHash, nil
}

func (s *ChainStore) appendJournalEntriesBatch(batch *pebble.Batch, entries ...chainJournalEntry) error {
	if len(entries) == 0 {
		return nil
	}
	nextSeq, err := s.journalNextSeq()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := batch.Set(journalKey(nextSeq), encodeChainJournalEntry(entry), nil); err != nil {
			return err
		}
		nextSeq++
	}
	return batch.Set(metaJournalNextSeqKey, encodeU64(nextSeq), nil)
}

func (s *ChainStore) appendJournalEntryBatch(batch *pebble.Batch, entry chainJournalEntry) error {
	return s.appendJournalEntriesBatch(batch, entry)
}

func (s *ChainStore) journalNextSeq() (uint64, error) {
	buf, err := s.get(metaJournalNextSeqKey)
	if err != nil {
		return 0, err
	}
	if buf == nil {
		return 0, nil
	}
	return decodeU64(buf)
}

func (s *ChainStore) derivedJournalSeq() (uint64, error) {
	buf, err := s.get(metaDerivedJournalSeqKey)
	if err != nil {
		return 0, err
	}
	if buf == nil {
		return 0, nil
	}
	return decodeU64(buf)
}

func (s *ChainStore) notifyDerivedReplay() {
	if s == nil || s.deriveNotify == nil {
		return
	}
	select {
	case s.deriveNotify <- struct{}{}:
	default:
	}
}

func (s *ChainStore) derivedIndexLoop() {
	for {
		if err := s.replayDerivedIndexes(); err != nil {
			s.logger.Warn("derived index replay failed", slog.Any("error", err))
			select {
			case <-time.After(50 * time.Millisecond):
			case <-s.stopCh:
				return
			}
			continue
		}
		select {
		case <-s.stopCh:
			return
		case <-s.deriveNotify:
		}
	}
}

func (s *ChainStore) replayDerivedIndexes() error {
	for {
		applied, err := s.applyNextJournalEntry()
		if err != nil {
			return err
		}
		if !applied {
			return nil
		}
	}
}

func (s *ChainStore) applyNextJournalEntry() (bool, error) {
	derivedSeq, err := s.derivedJournalSeq()
	if err != nil {
		return false, err
	}
	nextSeq, err := s.journalNextSeq()
	if err != nil {
		return false, err
	}
	if derivedSeq >= nextSeq {
		return false, nil
	}
	buf, err := s.get(journalKey(derivedSeq))
	if err != nil {
		return false, err
	}
	if buf == nil {
		batch := s.db.NewBatch()
		defer batch.Close()
		if err := batch.Set(metaDerivedJournalSeqKey, encodeU64(derivedSeq+1), nil); err != nil {
			return false, err
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return false, err
		}
		return true, nil
	}
	entry, err := decodeChainJournalEntry(buf)
	if err != nil {
		return false, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := applyJournalEntryBatch(batch, entry); err != nil {
		return false, err
	}
	if err := batch.Set(metaDerivedJournalSeqKey, encodeU64(derivedSeq+1), nil); err != nil {
		return false, err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return false, err
	}
	return true, nil
}

func (s *ChainStore) WaitForDerivedIndexes(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		derivedSeq, err := s.derivedJournalSeq()
		if err != nil {
			return err
		}
		nextSeq, err := s.journalNextSeq()
		if err != nil {
			return err
		}
		if derivedSeq >= nextSeq {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for derived indexes")
		}
		s.notifyDerivedReplay()
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *ChainStore) rebuildLocalityIndexBatch(batch *pebble.Batch, utxos consensus.UtxoSet) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: localitySeqPrefix,
		UpperBound: localitySeqPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	iter, err = s.db.NewIter(&pebble.IterOptions{
		LowerBound: localityMetaPrefix,
		UpperBound: localityMetaPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	ordered := make([]types.OutPoint, 0, len(utxos))
	for outPoint := range utxos {
		ordered = append(ordered, outPoint)
	}
	sortOutPointsCanonical(ordered)
	for seq, outPoint := range ordered {
		if err := batch.Set(localitySeqKey(uint64(seq)), encodeOutPoint(outPoint), nil); err != nil {
			return err
		}
		if err := batch.Set(localityMetaKey(outPoint), encodeU64(uint64(seq)), nil); err != nil {
			return err
		}
	}
	return batch.Set(metaLocalityNextSeqKey, encodeU64(uint64(len(ordered))), nil)
}

func (s *ChainStore) applyLocalityDeltaBatch(batch *pebble.Batch, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	nextSeq, err := s.localityNextSeq()
	if err != nil {
		return err
	}
	for _, outPoint := range spent {
		seq, ok, err := s.localitySeqForOutPoint(outPoint)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := batch.Delete(localitySeqKey(seq), nil); err != nil {
			return err
		}
		if err := batch.Delete(localityMetaKey(outPoint), nil); err != nil {
			return err
		}
	}
	orderedCreated := make([]types.OutPoint, 0, len(created))
	for outPoint := range created {
		orderedCreated = append(orderedCreated, outPoint)
	}
	sortOutPointsCanonical(orderedCreated)
	for _, outPoint := range orderedCreated {
		if err := batch.Set(localitySeqKey(nextSeq), encodeOutPoint(outPoint), nil); err != nil {
			return err
		}
		if err := batch.Set(localityMetaKey(outPoint), encodeU64(nextSeq), nil); err != nil {
			return err
		}
		nextSeq++
	}
	return batch.Set(metaLocalityNextSeqKey, encodeU64(nextSeq), nil)
}

func (s *ChainStore) applyLocalityRewriteBatch(batch *pebble.Batch, previous consensus.UtxoSet, next consensus.UtxoSet) error {
	spent := make([]types.OutPoint, 0)
	created := make(map[types.OutPoint]consensus.UtxoEntry)
	for outPoint := range previous {
		if _, ok := next[outPoint]; !ok {
			spent = append(spent, outPoint)
		}
	}
	for outPoint, entry := range next {
		if _, ok := previous[outPoint]; !ok {
			created[outPoint] = entry
		}
	}
	return s.applyLocalityDeltaBatch(batch, spent, created)
}

func (s *ChainStore) localityNextSeq() (uint64, error) {
	buf, err := s.get(metaLocalityNextSeqKey)
	if err != nil {
		return 0, err
	}
	if buf == nil {
		return 0, nil
	}
	return decodeU64(buf)
}

func (s *ChainStore) localitySeqForOutPoint(outPoint types.OutPoint) (uint64, bool, error) {
	buf, err := s.get(localityMetaKey(outPoint))
	if err != nil {
		return 0, false, err
	}
	if buf == nil {
		return 0, false, nil
	}
	seq, err := decodeU64(buf)
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

func sortOutPointsCanonical(outPoints []types.OutPoint) {
	slices.SortFunc(outPoints, func(a, b types.OutPoint) int {
		switch cmp := bytes.Compare(a.TxID[:], b.TxID[:]); {
		case cmp < 0:
			return -1
		case cmp > 0:
			return 1
		case a.Vout < b.Vout:
			return -1
		case a.Vout > b.Vout:
			return 1
		default:
			return 0
		}
	})
}

func writeMeta(batch *pebble.Batch, state *StoredChainState) error {
	checksum := state.UTXOChecksum
	if checksum == ([32]byte{}) {
		checksum = utxochecksum.Compute(state.UTXOs)
	}
	return writeMetaFromMeta(batch, &StoredChainStateMeta{
		Profile:        state.Profile,
		Height:         state.Height,
		TipHeader:      state.TipHeader,
		BlockSizeState: state.BlockSizeState,
		UTXOChecksum:   checksum,
	})
}

func writeMetaFromMeta(batch *pebble.Batch, state *StoredChainStateMeta) error {
	if err := writeHeaderMeta(batch, &StoredHeaderState{
		Profile:   state.Profile,
		Height:    state.Height,
		TipHeader: state.TipHeader,
	}); err != nil {
		return err
	}
	if err := batch.Set(metaTipHeightKey, encodeU64(state.Height), nil); err != nil {
		return err
	}
	if err := batch.Set(metaTipHeaderKey, state.TipHeader.Encode(), nil); err != nil {
		return err
	}
	if err := batch.Set(metaBlockSizeStateKey, encodeBlockSizeState(state.BlockSizeState), nil); err != nil {
		return err
	}
	return batch.Set(metaUTXOChecksumKey, state.UTXOChecksum[:], nil)
}

func putBlockBatch(batch *pebble.Batch, block *types.Block, entry BlockIndexEntry, active bool) error {
	blockHash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockKey(blockHash), block.Encode(), nil); err != nil {
		return err
	}
	return putHeaderBatch(batch, entry, active)
}

func journalPairsFromEntries(entries []BlockIndexEntry) []chainJournalHeightHash {
	pairs := make([]chainJournalHeightHash, 0, len(entries))
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: hash})
	}
	return pairs
}

func writeHeaderMeta(batch *pebble.Batch, state *StoredHeaderState) error {
	if err := batch.Set(metaProfileKey, []byte(state.Profile.String()), nil); err != nil {
		return err
	}
	if err := batch.Set(metaHeaderTipHeightKey, encodeU64(state.Height), nil); err != nil {
		return err
	}
	return batch.Set(metaHeaderTipHeaderKey, state.TipHeader.Encode(), nil)
}

func putHeaderBatch(batch *pebble.Batch, entry BlockIndexEntry, active bool) error {
	blockHash := consensus.HeaderHash(&entry.Header)
	return batch.Set(blockIndexKey(blockHash), encodeBlockIndexEntry(entry), nil)
}

func cloneBytes(buf []byte) []byte {
	return append([]byte(nil), buf...)
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] == 0xff {
			continue
		}
		out[i]++
		return out[:i+1]
	}
	return nil
}

func encodeU64(v uint64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, v)
	return out
}

func encodeI64(v int64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(v))
	return out
}

func decodeU64(buf []byte) (uint64, error) {
	if len(buf) != 8 {
		return 0, errors.New("invalid u64 encoding")
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func decodeI64(buf []byte) (int64, error) {
	if len(buf) != 8 {
		return 0, errors.New("invalid i64 encoding")
	}
	return int64(binary.LittleEndian.Uint64(buf)), nil
}

func blockKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockPrefix...), hash[:]...)
}

func blockIndexKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockIndexPrefix...), hash[:]...)
}

func blockUndoKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockUndoPrefix...), hash[:]...)
}

func journalKey(seq uint64) []byte {
	return append(append([]byte(nil), journalPrefix...), encodeU64(seq)...)
}

func heightIndexKey(height uint64) []byte {
	return append(append([]byte(nil), heightIndexPrefix...), encodeU64(height)...)
}

func headerHeightIndexKey(height uint64) []byte {
	return append(append([]byte(nil), headerHeightIndexPrefix...), encodeU64(height)...)
}

func knownPeerKey(addr string) []byte {
	return append(append([]byte(nil), knownPeerPrefix...), []byte(addr)...)
}

func localitySeqKey(seq uint64) []byte {
	return append(append([]byte(nil), localitySeqPrefix...), encodeU64(seq)...)
}

func localityMetaKey(outPoint types.OutPoint) []byte {
	buf := append([]byte(nil), localityMetaPrefix...)
	outPoint.Encode(&buf)
	return buf
}

func utxoKey(outPoint types.OutPoint) []byte {
	buf := append([]byte(nil), utxoPrefix...)
	outPoint.Encode(&buf)
	return buf
}

func snapshotUTXOKey(outPoint types.OutPoint) []byte {
	buf := append([]byte(nil), snapshotUTXOPrefix...)
	outPoint.Encode(&buf)
	return buf
}

func decodeOutPoint(buf []byte) (types.OutPoint, error) {
	if len(buf) != 36 {
		return types.OutPoint{}, errors.New("invalid outpoint encoding")
	}
	var outPoint types.OutPoint
	copy(outPoint.TxID[:], buf[:32])
	outPoint.Vout = binary.LittleEndian.Uint32(buf[32:36])
	return outPoint, nil
}

func encodeOutPoint(outPoint types.OutPoint) []byte {
	buf := make([]byte, 0, 36)
	outPoint.Encode(&buf)
	return buf
}

func decodeLocalitySeqFromKey(key []byte) (uint64, error) {
	if len(key) != len(localitySeqPrefix)+8 {
		return 0, errors.New("invalid locality sequence key")
	}
	return decodeU64(key[len(localitySeqPrefix):])
}

func encodeUTXOEntry(entry consensus.UtxoEntry) []byte {
	buf := make([]byte, 0, 49)
	buf = append(buf, types.CanonicalVarIntBytes(entry.Type)...)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, entry.ValueAtoms)
	buf = append(buf, value...)
	payload32 := entry.Payload32
	if payload32 == ([32]byte{}) && entry.Type == types.OutputXOnlyP2PK {
		payload32 = entry.PubKey
	}
	buf = append(buf, payload32[:]...)
	return buf
}

func encodeChainJournalEntry(entry chainJournalEntry) []byte {
	buf := make([]byte, 1+8+8+4)
	buf[0] = byte(entry.Kind)
	binary.LittleEndian.PutUint64(buf[1:9], entry.ForkHeight)
	binary.LittleEndian.PutUint64(buf[9:17], entry.OldTipHeight)
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(entry.Pairs)))
	for _, pair := range entry.Pairs {
		rawHeight := make([]byte, 8)
		binary.LittleEndian.PutUint64(rawHeight, pair.Height)
		buf = append(buf, rawHeight...)
		buf = append(buf, pair.Hash[:]...)
	}
	return buf
}

func decodeChainJournalEntry(buf []byte) (chainJournalEntry, error) {
	if len(buf) < 21 {
		return chainJournalEntry{}, errors.New("invalid chain journal entry encoding")
	}
	entry := chainJournalEntry{
		Kind:         chainJournalKind(buf[0]),
		ForkHeight:   binary.LittleEndian.Uint64(buf[1:9]),
		OldTipHeight: binary.LittleEndian.Uint64(buf[9:17]),
	}
	count := binary.LittleEndian.Uint32(buf[17:21])
	buf = buf[21:]
	if len(buf) != int(count)*(8+32) {
		return chainJournalEntry{}, errors.New("invalid chain journal height/hash payload")
	}
	entry.Pairs = make([]chainJournalHeightHash, 0, count)
	for i := uint32(0); i < count; i++ {
		pair := chainJournalHeightHash{Height: binary.LittleEndian.Uint64(buf[:8])}
		copy(pair.Hash[:], buf[8:40])
		entry.Pairs = append(entry.Pairs, pair)
		buf = buf[40:]
	}
	return entry, nil
}

func applyJournalEntryBatch(batch *pebble.Batch, entry chainJournalEntry) error {
	switch entry.Kind {
	case journalSetBlockHeight:
		for _, pair := range entry.Pairs {
			if err := batch.Set(heightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	case journalRewriteBlockHeights:
		for height := entry.ForkHeight + 1; height <= entry.OldTipHeight; height++ {
			if err := batch.Delete(heightIndexKey(height), nil); err != nil {
				return err
			}
			if height == ^uint64(0) {
				break
			}
		}
		for _, pair := range entry.Pairs {
			if err := batch.Set(heightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	case journalSetHeaderHeight:
		for _, pair := range entry.Pairs {
			if err := batch.Set(headerHeightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	case journalRewriteHeaderHeights:
		for height := entry.ForkHeight + 1; height <= entry.OldTipHeight; height++ {
			if err := batch.Delete(headerHeightIndexKey(height), nil); err != nil {
				return err
			}
			if height == ^uint64(0) {
				break
			}
		}
		for _, pair := range entry.Pairs {
			if err := batch.Set(headerHeightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown chain journal kind %d", entry.Kind)
	}
	return nil
}

func decodeUTXOEntry(buf []byte) (consensus.UtxoEntry, error) {
	entry, _, err := decodeUTXOEntryWithLen(buf)
	return entry, err
}

func decodeUTXOEntryWithLen(buf []byte) (consensus.UtxoEntry, int, error) {
	outputType, n, err := decodeCanonicalVarInt(buf)
	if err != nil {
		return consensus.UtxoEntry{}, 0, err
	}
	remaining := buf[n:]
	if len(remaining) < 40 {
		return consensus.UtxoEntry{}, 0, errors.New("invalid utxo entry encoding")
	}
	var payload32 [32]byte
	copy(payload32[:], remaining[8:40])
	entry := consensus.UtxoEntry{
		Type:       outputType,
		ValueAtoms: binary.LittleEndian.Uint64(remaining[:8]),
	}
	if outputType == types.OutputXOnlyP2PK {
		entry.PubKey = payload32
	} else {
		entry.Payload32 = payload32
	}
	return entry, n + 40, nil
}

func decodeCanonicalVarInt(buf []byte) (uint64, int, error) {
	if len(buf) == 0 {
		return 0, 0, errors.New("truncated varint")
	}
	first := buf[0]
	switch {
	case first <= 0xfc:
		return uint64(first), 1, nil
	case first == 0xfd:
		if len(buf) < 3 {
			return 0, 0, errors.New("truncated varint")
		}
		v := uint64(binary.LittleEndian.Uint16(buf[1:3]))
		if v <= 0xfc {
			return 0, 0, errors.New("non-canonical varint")
		}
		return v, 3, nil
	case first == 0xfe:
		if len(buf) < 5 {
			return 0, 0, errors.New("truncated varint")
		}
		v := uint64(binary.LittleEndian.Uint32(buf[1:5]))
		if v <= 0xffff {
			return 0, 0, errors.New("non-canonical varint")
		}
		return v, 5, nil
	default:
		if len(buf) < 9 {
			return 0, 0, errors.New("truncated varint")
		}
		v := binary.LittleEndian.Uint64(buf[1:9])
		if v <= 0xffff_ffff {
			return 0, 0, errors.New("non-canonical varint")
		}
		return v, 9, nil
	}
}

func encodeBlockIndexEntry(entry BlockIndexEntry) []byte {
	blockSizeState := encodeBlockSizeState(entry.BlockSizeState)
	buf := make([]byte, 8, 8+32+types.BlockHeaderEncodedLen+32+1+4+len(blockSizeState))
	binary.LittleEndian.PutUint64(buf, entry.Height)
	buf = append(buf, entry.ParentHash[:]...)
	buf = append(buf, entry.Header.Encode()...)
	buf = append(buf, entry.ChainWork[:]...)
	if entry.Validated {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = append(buf, consensus.EncodeLenPrefixed(blockSizeState)...)
	return buf
}

func decodeBlockIndexEntry(buf []byte) (*BlockIndexEntry, error) {
	if len(buf) < 8+32+types.BlockHeaderEncodedLen+32+1+4 {
		return nil, errors.New("invalid block index entry encoding")
	}
	height := binary.LittleEndian.Uint64(buf[:8])
	var parentHash [32]byte
	copy(parentHash[:], buf[8:40])
	header, err := types.DecodeBlockHeader(buf[40 : 40+types.BlockHeaderEncodedLen])
	if err != nil {
		return nil, err
	}
	var chainWork [32]byte
	chainWorkOffset := 40 + types.BlockHeaderEncodedLen
	copy(chainWork[:], buf[chainWorkOffset:chainWorkOffset+32])
	validatedOffset := chainWorkOffset + 32
	validated := buf[validatedOffset] != 0
	stateBytes, remaining, err := consensus.DecodeLenPrefixed(buf[validatedOffset+1:])
	if err != nil {
		return nil, err
	}
	if len(remaining) != 0 {
		return nil, errors.New("unexpected trailing block index entry data")
	}
	blockSizeState, err := decodeBlockSizeState(stateBytes)
	if err != nil {
		return nil, err
	}
	return &BlockIndexEntry{
		Height:         height,
		ParentHash:     parentHash,
		Header:         header,
		ChainWork:      chainWork,
		Validated:      validated,
		BlockSizeState: blockSizeState,
	}, nil
}

func encodeBlockUndo(entries []BlockUndoEntry) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(entries)))
	for _, entry := range entries {
		entry.OutPoint.Encode(&buf)
		buf = append(buf, encodeUTXOEntry(entry.Entry)...)
	}
	return buf
}

func decodeBlockUndo(buf []byte) ([]BlockUndoEntry, error) {
	if len(buf) < 8 {
		return nil, errors.New("invalid block undo encoding")
	}
	count := binary.LittleEndian.Uint64(buf[:8])
	buf = buf[8:]
	entries := make([]BlockUndoEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(buf) < 36 {
			return nil, errors.New("truncated block undo entry")
		}
		outPoint, err := decodeOutPoint(buf[:36])
		if err != nil {
			return nil, err
		}
		entry, consumed, err := decodeUTXOEntryWithLen(buf[36:])
		if err != nil {
			return nil, err
		}
		entries = append(entries, BlockUndoEntry{OutPoint: outPoint, Entry: entry})
		buf = buf[36+consumed:]
	}
	if len(buf) != 0 {
		return nil, errors.New("unexpected trailing block undo data")
	}
	return entries, nil
}

func (s *ChainStore) buildLinearIndexEntry(height uint64, header *types.BlockHeader, validated bool, blockSizeState consensus.BlockSizeState) (BlockIndexEntry, error) {
	work, err := consensus.BlockWork(header.NBits)
	if err != nil {
		return BlockIndexEntry{}, err
	}
	chainWork := work
	if height > 0 {
		parentEntry, err := s.GetBlockIndex(&header.PrevBlockHash)
		if err != nil {
			return BlockIndexEntry{}, err
		}
		if parentEntry == nil {
			return BlockIndexEntry{}, fmt.Errorf("missing parent block index for %x", header.PrevBlockHash)
		}
		chainWork = consensus.AddChainWork(parentEntry.ChainWork, work)
	}
	return BlockIndexEntry{
		Height:         height,
		ParentHash:     header.PrevBlockHash,
		Header:         *header,
		ChainWork:      chainWork,
		Validated:      validated,
		BlockSizeState: blockSizeState,
	}, nil
}

func encodeBlockSizeState(state consensus.BlockSizeState) []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[:8], state.BlockSize)
	binary.LittleEndian.PutUint64(buf[8:16], state.Epsilon)
	binary.LittleEndian.PutUint64(buf[16:24], state.Beta)
	return buf
}

func decodeBlockSizeState(buf []byte) (consensus.BlockSizeState, error) {
	switch len(buf) {
	case 24:
		if looksLikeLegacyBlockSizeState(buf) {
			return decodeLegacyBlockSizeState(buf)
		}
		return consensus.BlockSizeState{
			BlockSize: binary.LittleEndian.Uint64(buf[:8]),
			Epsilon:   binary.LittleEndian.Uint64(buf[8:16]),
			Beta:      binary.LittleEndian.Uint64(buf[16:24]),
		}, nil
	default:
		if len(buf) < 24 {
			return consensus.BlockSizeState{}, errors.New("invalid block size state encoding")
		}
		return decodeLegacyBlockSizeState(buf)
	}
}

func looksLikeLegacyBlockSizeState(buf []byte) bool {
	// The old encoding was {limit, ewma, recent_count}. A 24-byte payload with a
	// very small third word is almost certainly that legacy form because modern
	// ABLA state keeps Beta at or above the multi-megabyte floor, not a tiny
	// recent-block counter.
	return binary.LittleEndian.Uint64(buf[16:24]) <= 65_536
}

func decodeLegacyBlockSizeState(buf []byte) (consensus.BlockSizeState, error) {
	count := binary.LittleEndian.Uint64(buf[16:24])
	expected := 24 + int(count)*8
	if len(buf) != expected {
		return consensus.BlockSizeState{}, fmt.Errorf("invalid block size state length: %d", len(buf))
	}
	limit := binary.LittleEndian.Uint64(buf[:8])
	ewma := binary.LittleEndian.Uint64(buf[8:16])
	blockSize := ewma
	if count > 0 {
		start := 24 + (int(count)-1)*8
		blockSize = binary.LittleEndian.Uint64(buf[start : start+8])
	}
	epsilon := limit / 2
	beta := limit - epsilon
	// Legacy nodes stored only the current limit, EWMA, and recent block sizes.
	// Seed the newer ABLA state with the same total limit and the latest observed
	// block size (or EWMA when history is empty) so reopen/migration preserves the
	// operator-visible ceiling instead of failing on decode.
	return consensus.BlockSizeState{
		BlockSize: blockSize,
		Epsilon:   epsilon,
		Beta:      beta,
	}, nil
}

func encodeKnownPeerRecord(record KnownPeerRecord) []byte {
	buf := make([]byte, 37)
	binary.LittleEndian.PutUint64(buf[:8], encodeKnownPeerTime(record.LastSeen))
	binary.LittleEndian.PutUint64(buf[8:16], encodeKnownPeerTime(record.LastSuccess))
	binary.LittleEndian.PutUint64(buf[16:24], encodeKnownPeerTime(record.LastAttempt))
	binary.LittleEndian.PutUint64(buf[24:32], encodeKnownPeerTime(record.BannedUntil))
	binary.LittleEndian.PutUint32(buf[32:36], record.FailureCount)
	if record.Manual {
		buf[36] = 1
	}
	return buf
}

func decodeKnownPeerRecord(buf []byte) (KnownPeerRecord, error) {
	if len(buf) == 8 {
		// Legacy encoding only stored the last-seen timestamp.
		lastSeenUnix, err := decodeI64(buf)
		if err != nil {
			return KnownPeerRecord{}, err
		}
		lastSeen := time.Unix(0, lastSeenUnix).UTC()
		return KnownPeerRecord{
			LastSeen:    lastSeen,
			LastSuccess: lastSeen,
		}, nil
	}
	if len(buf) != 29 && len(buf) != 37 {
		return KnownPeerRecord{}, fmt.Errorf("invalid known peer encoding length: %d", len(buf))
	}
	record := KnownPeerRecord{
		LastSeen:    decodeKnownPeerTime(binary.LittleEndian.Uint64(buf[:8])),
		LastSuccess: decodeKnownPeerTime(binary.LittleEndian.Uint64(buf[8:16])),
		LastAttempt: decodeKnownPeerTime(binary.LittleEndian.Uint64(buf[16:24])),
	}
	if len(buf) == 29 {
		record.FailureCount = binary.LittleEndian.Uint32(buf[24:28])
		record.Manual = buf[28] == 1
		return record, nil
	}
	record.BannedUntil = decodeKnownPeerTime(binary.LittleEndian.Uint64(buf[24:32]))
	record.FailureCount = binary.LittleEndian.Uint32(buf[32:36])
	record.Manual = buf[36] == 1
	return record, nil
}

func encodeKnownPeerTime(value time.Time) uint64 {
	if value.IsZero() {
		return 0
	}
	return uint64(value.UTC().UnixNano())
}

func decodeKnownPeerTime(raw uint64) time.Time {
	if raw == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(raw)).UTC()
}
