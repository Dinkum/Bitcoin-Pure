package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"bitcoin-pure/internal/utxochecksum"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

var (
	metaProfileKey                  = []byte("meta/profile")
	metaHeaderTipHeightKey          = []byte("meta/header_tip_height")
	metaHeaderTipHeaderKey          = []byte("meta/header_tip_header")
	metaTipHeightKey                = []byte("meta/tip_height")
	metaTipHeaderKey                = []byte("meta/tip_header")
	metaBlockSizeStateKey           = []byte("meta/block_size_state")
	metaUTXOChecksumKey             = []byte("meta/utxo_checksum")
	metaUTXOCountKey                = []byte("meta/utxo_count")
	metaUTXOAccumulatorRootKey      = []byte("meta/utxo_accumulator_root")
	metaUTXOAccumulatorRootPathKey  = []byte("meta/utxo_accumulator_root_path")
	metaUTXOAccumulatorVersionKey   = []byte("meta/utxo_accumulator_version")
	metaChainstateSchemaVersionKey  = []byte("meta/chainstate_schema_version")
	metaFastSyncStateKey            = []byte("meta/fast_sync_state")
	metaMempoolStateKey             = []byte("meta/mempool_state")
	metaLocalityNextSeqKey          = []byte("meta/locality_next_seq")
	metaJournalNextSeqKey           = []byte("meta/journal_next_seq")
	metaDerivedJournalSeqKey        = []byte("meta/derived_journal_seq")
	metaWalletIndexHeightKey        = []byte("meta/wallet_index_height")
	mempoolEntryPrefix              = []byte("mempool_entry/")
	mempoolOrphanPrefix             = []byte("mempool_orphan/")
	blockPrefix                     = []byte("blocks/")
	blockChunkPrefix                = []byte("block_chunks/")
	blockIndexPrefix                = []byte("block_index/")
	blockUndoPrefix                 = []byte("block_undo/")
	headerHeightIndexPrefix         = []byte("header_height_index/")
	heightIndexPrefix               = []byte("height_index/")
	knownPeerPrefix                 = []byte("known_peer/")
	journalPrefix                   = []byte("journal/")
	utxoPrefix                      = []byte("utxo/")
	legacyUTXOAccumulatorNodePrefix = []byte("utxo_acc_node/")
	utxoAccumulatorNodePrefix       = []byte("utxo_acc_node_v2/")
	snapshotUTXOPrefix              = []byte("snapshot_utxo/")
	localitySeqPrefix               = []byte("locality_seq_v2/")
	legacyLocalitySeqPrefix         = []byte("locality_seq/")
	localityMetaPrefix              = []byte("locality_meta/")
	walletOriginPrefix              = []byte("wallet_origin/")
	walletUTXOPrefix                = []byte("wallet_utxo/")
	walletActivityItemPrefix        = []byte("wallet_activity_item/")
	walletActivityHtPrefix          = []byte("wallet_activity_height/")
)

const (
	utxoAccumulatorIndexVersion = uint64(2)
	utxoAccumulatorIndexV1      = uint64(1)
	chainstateSchemaVersion     = uint64(3)
	chainstateSchemaV2          = uint64(2)
	storedBlockManifestVersion  = byte(1)
	storedBlockChunkBytes       = 4 << 20
)

var ErrChainstateReindexRequired = errors.New("chainstate reindex required")

var (
	knownPeerPrefixEnd                 = prefixUpperBound(knownPeerPrefix)
	mempoolEntryPrefixEnd              = prefixUpperBound(mempoolEntryPrefix)
	mempoolOrphanPrefixEnd             = prefixUpperBound(mempoolOrphanPrefix)
	utxoPrefixEnd                      = prefixUpperBound(utxoPrefix)
	legacyUTXOAccumulatorNodePrefixEnd = prefixUpperBound(legacyUTXOAccumulatorNodePrefix)
	utxoAccumulatorNodePrefixEnd       = prefixUpperBound(utxoAccumulatorNodePrefix)
	snapshotUTXOPrefixEnd              = prefixUpperBound(snapshotUTXOPrefix)
	localitySeqPrefixEnd               = prefixUpperBound(localitySeqPrefix)
	legacyLocalitySeqPrefixEnd         = prefixUpperBound(legacyLocalitySeqPrefix)
	localityMetaPrefixEnd              = prefixUpperBound(localityMetaPrefix)
	walletOriginPrefixEnd              = prefixUpperBound(walletOriginPrefix)
	walletUTXOPrefixEnd                = prefixUpperBound(walletUTXOPrefix)
	walletActItemPrefixEnd             = prefixUpperBound(walletActivityItemPrefix)
	walletActHtPrefixEnd               = prefixUpperBound(walletActivityHtPrefix)
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
	Profile         types.ChainProfile
	Height          uint64
	TipHeader       types.BlockHeader
	BlockSizeState  consensus.BlockSizeState
	UTXOChecksum    [32]byte
	UTXOs           consensus.UtxoSet
	UTXOAccumulator *utreexo.Accumulator
}

// StoredChainStateMeta holds the chain tip metadata without materializing the
// full UTXO set. This is the additive building block for the disk-backed UTXO
// migration path.
type StoredChainStateMeta struct {
	Profile              types.ChainProfile
	Height               uint64
	TipHeader            types.BlockHeader
	BlockSizeState       consensus.BlockSizeState
	UTXOChecksum         [32]byte
	UTXOCount            int
	UTXOAccumulatorRoot  [32]byte
	UTXOAccumulator      *utreexo.Accumulator
	UTXOAccumulatorDelta *utreexo.AccumulatorNodeDelta
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

type BlockIndexEntry struct {
	Height         uint64
	ParentHash     [32]byte
	Header         types.BlockHeader
	ChainWork      [32]byte
	Validated      bool
	BlockSizeState consensus.BlockSizeState
}

// BlockUndoEntry is the durable representation of consensus's authoritative
// pre-block spend delta. The alias keeps the existing byte layout unchanged.
type BlockUndoEntry = consensus.SpentUTXO

type HeaderBatchEntry struct {
	Height    uint64
	Header    types.BlockHeader
	ChainWork [32]byte
}

type ChainStore struct {
	db     *pebble.DB
	logger *slog.Logger

	filterMu       sync.Mutex
	filterNotify   chan struct{}
	txIndexEnabled bool
	txIndexRebuild atomic.Bool
	txIndexNotify  chan struct{}
	// Lock order is node state, then mutationMu. Keep every dependent store
	// read and its commit together; Pebble batches do not isolate readers.
	mutationMu sync.Mutex
	stopCh     chan struct{}
	wg         sync.WaitGroup
	closeOnce  sync.Once
	closeErr   error
}

type pebbleLogger struct {
	logger *slog.Logger
}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	if l.logger != nil {
		l.logger.Debug("pebble", slog.String("pebble_message", fmt.Sprintf(format, args...)))
	}
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if l.logger != nil {
		l.logger.Error("pebble fatal error", slog.String("pebble_message", msg))
	}
	panic(msg)
}

// OpenOptions controls optional derived indexes and Pebble tuning.
// Zero values keep the transaction index disabled and preserve Pebble defaults.
type OpenOptions struct {
	// TxIndexEnabled builds an optional transaction lookup index in the background.
	TxIndexEnabled        bool
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
	pebbleOpts := &pebble.Options{Logger: pebbleLogger{logger: logger}}
	if opts.PebbleCacheBytes > 0 {
		cache := pebble.NewCache(opts.PebbleCacheBytes)
		defer cache.Unref()
		pebbleOpts.Cache = cache
	}
	if opts.BloomFilterBitsPerKey > 0 {
		// Pebble inherits the final LevelOptions for all deeper levels.
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
		filterNotify: make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}
	if err := store.migrateUTXOAccumulatorIndex(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.markCurrentChainstateSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.recoverLegacyHeightJournal(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover legacy height index: %w", err)
	}
	if err := store.removeLegacyWalletOrigins(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.migrateLocalityIndex(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate locality index: %w", err)
	}
	if opts.TxIndexEnabled {
		store.txIndexEnabled = true
		store.txIndexNotify = make(chan struct{}, 1)
		store.wg.Add(1)
		go func() { defer store.wg.Done(); store.txIndexLoop() }()
	}
	store.wg.Add(1)
	go func() { defer store.wg.Done(); store.compactFilterIndexLoop() }()
	store.notifyBackgroundIndexes()
	return store, nil
}

// Advance the schema marker before retiring older derived keyspaces.
// Older binaries refuse this marker instead of writing indexes they can
// no longer read. The individual migrations remain restartable.
func (s *ChainStore) markCurrentChainstateSchema() error {
	raw, err := s.get(metaChainstateSchemaVersionKey)
	if err != nil || raw == nil {
		return err
	}
	version, err := decodeU64(raw)
	if err != nil {
		return err
	}
	switch version {
	case chainstateSchemaVersion:
		return nil
	case chainstateSchemaV2:
		return s.db.Set(metaChainstateSchemaVersionKey, encodeU64(chainstateSchemaVersion), consensusCriticalWriteOptions)
	default:
		return fmt.Errorf("%w: unsupported chainstate schema version %d", ErrChainstateReindexRequired, version)
	}
}

func (s *ChainStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.logger.Info("closing pebble chain store")
		if s.stopCh != nil {
			close(s.stopCh)
		}
		s.wg.Wait()
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

func (s *ChainStore) LoadChainState() (*StoredChainState, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	meta, hasChecksum, err := loadChainStateMeta(snap)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, nil
	}
	utxos := make(consensus.UtxoSet)
	if err := forEachUTXO(snap, func(outpoint types.OutPoint, entry consensus.UtxoEntry) error {
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
		Profile:         meta.Profile,
		Height:          meta.Height,
		TipHeader:       meta.TipHeader,
		BlockSizeState:  meta.BlockSizeState,
		UTXOChecksum:    checksum,
		UTXOs:           utxos,
		UTXOAccumulator: meta.UTXOAccumulator,
	}, nil
}

// LoadChainStateMeta loads only the persisted canonical chain metadata. It
// intentionally avoids scanning the UTXO keyspace so callers can opt into a
// disk-backed view without paying the startup RAM cost up front.
func (s *ChainStore) LoadChainStateMeta() (*StoredChainStateMeta, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	meta, _, err := loadChainStateMeta(snap)
	return meta, err
}

func loadChainStateMeta(reader valueReader) (*StoredChainStateMeta, bool, error) {
	profileBytes, err := readValue(reader, metaProfileKey)
	if err != nil {
		return nil, false, err
	}
	if profileBytes == nil {
		return nil, false, nil
	}
	heightBytes, err := readValue(reader, metaTipHeightKey)
	if err != nil {
		return nil, false, err
	}
	headerBytes, err := readValue(reader, metaTipHeaderKey)
	if err != nil {
		return nil, false, err
	}
	blockSizeBytes, err := readValue(reader, metaBlockSizeStateKey)
	if err != nil {
		return nil, false, err
	}
	checksumBytes, err := readValue(reader, metaUTXOChecksumKey)
	if err != nil {
		return nil, false, err
	}
	countBytes, err := readValue(reader, metaUTXOCountKey)
	if err != nil {
		return nil, false, err
	}
	accRootBytes, err := readValue(reader, metaUTXOAccumulatorRootKey)
	if err != nil {
		return nil, false, err
	}
	if heightBytes == nil && headerBytes == nil && blockSizeBytes == nil {
		return nil, false, nil
	}
	if heightBytes == nil || headerBytes == nil || blockSizeBytes == nil {
		return nil, false, errors.New("invalid data: missing chain metadata")
	}
	schemaBytes, err := readValue(reader, metaChainstateSchemaVersionKey)
	if err != nil {
		return nil, false, err
	}
	if schemaBytes == nil {
		return nil, false, fmt.Errorf("%w: missing coin-origin schema metadata", ErrChainstateReindexRequired)
	}
	schemaVersion, err := decodeU64(schemaBytes)
	if err != nil {
		return nil, false, err
	}
	if schemaVersion != chainstateSchemaVersion && schemaVersion != chainstateSchemaV2 {
		return nil, false, fmt.Errorf("%w: unsupported chainstate schema version %d", ErrChainstateReindexRequired, schemaVersion)
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
	var utxoCount int
	if countBytes != nil {
		count, err := decodeU64(countBytes)
		if err != nil {
			return nil, false, err
		}
		if count > uint64(math.MaxInt) {
			return nil, false, errors.New("invalid data: utxo count exceeds platform int capacity")
		}
		utxoCount = int(count)
	}
	var accRoot [32]byte
	switch {
	case accRootBytes == nil:
	case len(accRootBytes) != len(accRoot):
		return nil, false, errors.New("invalid data: bad utxo accumulator root metadata")
	default:
		copy(accRoot[:], accRootBytes)
	}
	return &StoredChainStateMeta{
		Profile:             profile,
		Height:              height,
		TipHeader:           header,
		BlockSizeState:      blockSizeState,
		UTXOChecksum:        checksum,
		UTXOCount:           utxoCount,
		UTXOAccumulatorRoot: accRoot,
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
		entry, ok, err := s.GetUTXO(out)
		if err != nil && s.logger != nil {
			s.logger.Warn("utxo lookup failed",
				slog.String("txid", fmt.Sprintf("%x", out.TxID)),
				slog.Uint64("vout", uint64(out.Vout)),
				slog.Any("error", err),
			)
		}
		return entry, ok
	}
}

// ForEachUTXO scans the committed UTXO keyspace in key order.
func (s *ChainStore) ForEachUTXO(fn func(types.OutPoint, consensus.UtxoEntry) error) error {
	return forEachUTXO(s.db, fn)
}

func forEachUTXO(reader storeReader, fn func(types.OutPoint, consensus.UtxoEntry) error) error {
	iter, err := reader.NewIter(&pebble.IterOptions{
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
	count := 0
	if err := s.ForEachFastSyncSnapshotUTXO(func(outpoint types.OutPoint, entry consensus.UtxoEntry) error {
		utxos[outpoint] = entry
		count++
		return nil
	}); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return utxos, nil
}

func (s *ChainStore) ForEachFastSyncSnapshotUTXO(fn func(types.OutPoint, consensus.UtxoEntry) error) error {
	if fn == nil {
		return nil
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
		outpoint, err := decodeOutPoint(iter.Key()[len(snapshotUTXOPrefix):])
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

func (s *ChainStore) LoadHeaderState() (*StoredHeaderState, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	profileBytes, err := readValue(snap, metaProfileKey)
	if err != nil {
		return nil, err
	}
	if profileBytes == nil {
		return nil, nil
	}

	heightBytes, err := readValue(snap, metaHeaderTipHeightKey)
	if err != nil {
		return nil, err
	}
	headerBytes, err := readValue(snap, metaHeaderTipHeaderKey)
	if err != nil {
		return nil, err
	}
	if heightBytes == nil && headerBytes == nil {
		heightBytes, err = readValue(snap, metaTipHeightKey)
		if err != nil {
			return nil, err
		}
		headerBytes, err = readValue(snap, metaTipHeaderKey)
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

	existing := make(map[string][]byte)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: knownPeerPrefix,
		UpperBound: knownPeerPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		addr := string(iter.Key()[len(knownPeerPrefix):])
		if addr == "" {
			continue
		}
		if _, keep := peers[addr]; !keep {
			if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
				return err
			}
			continue
		}
		existing[addr] = cloneBytes(iter.Value())
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for addr, record := range peers {
		if addr == "" {
			continue
		}
		encoded := encodeKnownPeerRecord(record)
		if bytes.Equal(existing[addr], encoded) {
			continue
		}
		if err := batch.Set(knownPeerKey(addr), encoded, nil); err != nil {
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
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.writeFullStateLocked(state, nil, nil, nil)
}

func (s *ChainStore) WriteFullStateWithHeaderMetadata(state *StoredChainState, headerState *StoredHeaderState, activeEntries []BlockIndexEntry) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.writeFullStateLocked(state, nil, headerState, activeEntries)
}

func (s *ChainStore) WriteFullStateWithFastSyncStateMetadata(state *StoredChainState, fastSyncState *FastSyncState, headerState *StoredHeaderState, activeEntries []BlockIndexEntry) error {
	if fastSyncState == nil {
		return errors.New("fast sync state is required")
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.writeFullStateLocked(state, fastSyncState, headerState, activeEntries)
}

func (s *ChainStore) writeFullStateLocked(state *StoredChainState, fastSyncState *FastSyncState, headerState *StoredHeaderState, activeEntries []BlockIndexEntry) error {
	if state.UTXOAccumulator == nil {
		acc, err := consensus.UtxoAccumulator(state.UTXOs)
		if err != nil {
			return err
		}
		state.UTXOAccumulator = acc
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMeta(batch, state); err != nil {
		return err
	}
	if headerState != nil {
		if err := writeHeaderMeta(batch, headerState); err != nil {
			return err
		}
	}
	if fastSyncState != nil {
		encoded, err := json.Marshal(fastSyncState)
		if err != nil {
			return err
		}
		if err := batch.Set(metaFastSyncStateKey, encoded, nil); err != nil {
			return err
		}
		if err := batch.DeleteRange(snapshotUTXOPrefix, snapshotUTXOPrefixEnd, nil); err != nil {
			return err
		}
	}
	if err := batch.DeleteRange(utxoPrefix, utxoPrefixEnd, nil); err != nil {
		return err
	}
	for outPoint, entry := range state.UTXOs {
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(entry), nil); err != nil {
			return err
		}
	}
	if err := invalidateWalletIndexesBatch(batch); err != nil {
		return err
	}
	if err := rebuildLocalityIndexBatch(batch, state.UTXOs); err != nil {
		return err
	}
	if err := replaceAccumulatorIndexBatch(batch, state.UTXOAccumulator); err != nil {
		return err
	}
	if len(activeEntries) > 0 {
		pairs := journalPairsFromEntries(activeEntries)
		if err := writeHeightIndexesBatch(batch,
			chainJournalEntry{
				Kind:         journalRewriteBlockHeights,
				ForkHeight:   0,
				OldTipHeight: 0,
				Pairs:        pairs,
			},
			chainJournalEntry{
				Kind:         journalRewriteHeaderHeights,
				ForkHeight:   0,
				OldTipHeight: 0,
				Pairs:        pairs,
			},
		); err != nil {
			return err
		}
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	s.logger.Info("wrote full chain state",
		slog.Uint64("height", state.Height),
		slog.Int("utxo_count", len(state.UTXOs)),
	)
	return nil
}

func (s *ChainStore) WriteFastSyncState(state *FastSyncState, snapshot consensus.UtxoSet) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if state == nil {
		return errors.New("fast sync state is required")
	}
	if state.SnapshotUTXOCount != len(snapshot) {
		return fmt.Errorf("fast sync snapshot count mismatch: state=%d snapshot=%d", state.SnapshotUTXOCount, len(snapshot))
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
	if err := batch.DeleteRange(snapshotUTXOPrefix, snapshotUTXOPrefixEnd, nil); err != nil {
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

func (s *ChainStore) WriteFastSyncStateMetadata(state *FastSyncState) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

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
	if err := batch.DeleteRange(snapshotUTXOPrefix, snapshotUTXOPrefixEnd, nil); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.logger.Info("wrote fast-sync snapshot metadata",
		slog.Uint64("height", state.SnapshotHeight),
		slog.Int("utxo_count", state.SnapshotUTXOCount),
	)
	return nil
}

func (s *ChainStore) UpdateFastSyncState(state *FastSyncState) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

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
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Delete(metaFastSyncStateKey, nil); err != nil {
		return err
	}
	if err := batch.DeleteRange(snapshotUTXOPrefix, snapshotUTXOPrefixEnd, nil); err != nil {
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
	if next.UTXOAccumulator == nil {
		acc, err := consensus.UtxoAccumulator(next.UTXOs)
		if err != nil {
			return err
		}
		next.UTXOAccumulator = acc
	}
	if previous == nil {
		return s.WriteFullState(next)
	}

	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

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
	if err := replaceAccumulatorIndexBatch(batch, next.UTXOAccumulator); err != nil {
		return err
	}
	if err := invalidateWalletIndexesBatch(batch); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
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
	return s.CommitReorgDeltaWithHeaderUpdate(meta, spent, created, forkHeight, oldTipHeight, activeEntries, nil)
}

func (s *ChainStore) CommitReorgDeltaWithHeaderUpdate(meta *StoredChainStateMeta, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry, forkHeight uint64, oldTipHeight uint64, activeEntries []BlockIndexEntry, headerUpdate *HeaderChainUpdate) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if meta == nil {
		return errors.New("reorg chain metadata is required")
	}
	if err := s.populateAccumulatorDeltaFromUTXODelta(meta, spent, created); err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	writeMeta := writeBlockMetaFromMeta
	if headerUpdate == nil {
		writeMeta = writeMetaFromMeta
	}
	if err := writeMeta(batch, meta); err != nil {
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
	if err := applyAccumulatorStateBatch(batch, meta); err != nil {
		return err
	}
	if err := s.applyLocalityDeltaBatch(batch, spent, created); err != nil {
		return err
	}
	walletReady, err := s.walletIndexReadyForReorg(oldTipHeight)
	if err != nil {
		return err
	}
	if walletReady {
		err = s.applyWalletReorgBatch(batch, spent, created, forkHeight, oldTipHeight, activeEntries)
	} else {
		err = invalidateWalletIndexesBatch(batch)
	}
	if err != nil {
		return err
	}
	pairs := journalPairsFromEntries(activeEntries)
	if err := writeHeightIndexesBatch(batch,
		chainJournalEntry{
			Kind:         journalRewriteBlockHeights,
			ForkHeight:   forkHeight,
			OldTipHeight: oldTipHeight,
			Pairs:        pairs,
		},
	); err != nil {
		return err
	}
	if headerUpdate == nil {
		if err := applyJournalEntryBatch(batch, chainJournalEntry{Kind: journalRewriteHeaderHeights, ForkHeight: forkHeight, OldTipHeight: oldTipHeight, Pairs: pairs}); err != nil {
			return err
		}
	} else if err := s.applyHeaderUpdateBatch(batch, headerUpdate); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	s.logger.Info("committed reorg delta",
		slog.Uint64("height", meta.Height),
		slog.Int("spent_utxos", len(spent)),
		slog.Int("created_utxos", len(created)),
		slog.Int("active_entries", len(activeEntries)),
	)
	return nil
}

func (s *ChainStore) WriteHeaderState(state *StoredHeaderState) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeHeaderMeta(batch, state); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&state.TipHeader)
	if err := applyJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetHeaderHeight,
		Pairs: []chainJournalHeightHash{{Height: state.Height, Hash: hash}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	s.logger.Debug("wrote header state", slog.Uint64("height", state.Height))
	return nil
}

func (s *ChainStore) CommitHeaderChain(state *StoredHeaderState, entries []HeaderBatchEntry, forkHeight uint64, oldTipHeight uint64, activeEntries []HeaderBatchEntry) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

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
		if err := putHeaderBatch(batch, preserved); err != nil {
			return err
		}
	}
	if len(activeEntries) != 0 {
		pairs := make([]chainJournalHeightHash, 0, len(activeEntries))
		for _, entry := range activeEntries {
			pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: consensus.HeaderHash(&entry.Header)})
		}
		if err := applyJournalEntryBatch(batch, chainJournalEntry{
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
	s.notifyBackgroundIndexes()
	s.logger.Debug("wrote header batch",
		slog.Uint64("height", state.Height),
		slog.Int("count", len(entries)),
	)
	return nil
}

func (s *ChainStore) PutBlock(height uint64, block *types.Block) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	entry, err := s.buildLinearIndexEntry(height, &block.Header, false, consensus.BlockSizeState{})
	if err != nil {
		return err
	}
	entry, err = s.preserveValidatedBlockIndex(entry)
	if err != nil {
		return err
	}
	if err := putBlockBatch(batch, block, entry); err != nil {
		return err
	}
	if err := applyJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetBlockHeight,
		Pairs: []chainJournalHeightHash{{Height: height, Hash: consensus.HeaderHash(&block.Header)}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	hash := consensus.HeaderHash(&block.Header)
	s.logger.Debug("stored block",
		slog.Uint64("height", height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
	)
	return nil
}

func (s *ChainStore) PutHeader(height uint64, header *types.BlockHeader) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

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
	if err := putHeaderBatch(batch, entry); err != nil {
		return err
	}
	hash := consensus.HeaderHash(header)
	if err := applyJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetHeaderHeight,
		Pairs: []chainJournalHeightHash{{Height: height, Hash: hash}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	s.logger.Debug("stored header",
		slog.Uint64("height", height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
	)
	return nil
}

func (s *ChainStore) AppendBlock(state *StoredChainState, block *types.Block, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	if state.UTXOAccumulator == nil {
		acc, err := consensus.UtxoAccumulator(state.UTXOs)
		if err != nil {
			return err
		}
		state.UTXOAccumulator = acc
	}
	entry, err := s.buildLinearIndexEntry(state.Height, &block.Header, true, state.BlockSizeState)
	if err != nil {
		return err
	}
	return s.AppendValidatedBlockMeta(storedChainStateMeta(state), block, &entry, nil, spent, created)
}

func (s *ChainStore) AppendValidatedBlock(state *StoredChainState, block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	if state.UTXOAccumulator == nil {
		acc, err := consensus.UtxoAccumulator(state.UTXOs)
		if err != nil {
			return err
		}
		state.UTXOAccumulator = acc
	}
	return s.AppendValidatedBlockMeta(storedChainStateMeta(state), block, entry, undo, spent, created)
}

func (s *ChainStore) AppendValidatedBlockMeta(state *StoredChainStateMeta, block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	return s.AppendValidatedBlockMetaWithHeaderUpdate(state, block, entry, undo, spent, created, nil)
}

func (s *ChainStore) AppendValidatedBlockMetaWithHeaderUpdate(state *StoredChainStateMeta, block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry, headerUpdate *HeaderChainUpdate) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if state == nil {
		return errors.New("chain state metadata is required")
	}
	if err := s.populateAccumulatorDeltaFromUTXODelta(state, spent, created); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	writeMeta := writeBlockMetaFromMeta
	if headerUpdate == nil {
		writeMeta = writeMetaFromMeta
	}
	if err := writeMeta(batch, state); err != nil {
		return err
	}
	if err := putBlockBatch(batch, block, *entry); err != nil {
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
	if err := applyAccumulatorStateBatch(batch, state); err != nil {
		return err
	}
	if err := s.applyLocalityDeltaBatch(batch, spent, created); err != nil {
		return err
	}
	walletReady, err := s.walletIndexReadyForAppend(state.Height)
	if err != nil {
		return err
	}
	if walletReady {
		err = applyWalletActiveBlockBatch(batch, state.Height, hash, block, undo, created)
	} else {
		err = invalidateWalletIndexesBatch(batch)
	}
	if err != nil {
		return err
	}
	if err := applyJournalEntryBatch(batch, chainJournalEntry{
		Kind: journalSetBlockHeight, Pairs: []chainJournalHeightHash{{Height: state.Height, Hash: hash}},
	}); err != nil {
		return err
	}
	if headerUpdate == nil {
		if err := applyJournalEntryBatch(batch, chainJournalEntry{
			Kind: journalSetHeaderHeight, Pairs: []chainJournalHeightHash{{Height: state.Height, Hash: hash}},
		}); err != nil {
			return err
		}
	} else if err := s.applyHeaderUpdateBatch(batch, headerUpdate); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	s.logger.Debug("appended block delta",
		slog.Uint64("height", state.Height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
		slog.Int("spent_utxos", len(spent)),
		slog.Int("created_utxos", len(created)),
	)
	return nil
}

func (s *ChainStore) PutValidatedBlock(block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry) error {
	return s.PutValidatedBlockWithHeaderUpdate(block, entry, undo, nil)
}

func (s *ChainStore) PutValidatedBlockWithHeaderUpdate(block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry, headerUpdate *HeaderChainUpdate) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := putBlockBatch(batch, block, *entry); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	if err := s.applyHeaderUpdateBatch(batch, headerUpdate); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) PutValidatedBlockWithoutWalletIndex(block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if block == nil || entry == nil {
		return errors.New("block and index entry are required")
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := putBlockBatch(batch, block, *entry); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) PutValidatedBlocksWithoutWalletIndex(blocks []types.Block, entries []BlockIndexEntry) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if len(blocks) != len(entries) {
		return fmt.Errorf("block batch length mismatch: blocks=%d entries=%d", len(blocks), len(entries))
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for i := range blocks {
		if err := putBlockBatch(batch, &blocks[i], entries[i]); err != nil {
			return err
		}

	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) PutValidatedBlockUndo(entry *BlockIndexEntry, undo []BlockUndoEntry) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if entry == nil {
		return errors.New("block index entry is required")
	}
	hash := consensus.HeaderHash(&entry.Header)
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(blockIndexKey(hash), encodeBlockIndexEntry(*entry), nil); err != nil {
		return err
	}
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	return nil
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

func (s *ChainStore) GetBlock(blockHash *[32]byte) (*types.Block, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	return getBlockFrom(snap, blockHash)
}

// HasBlock checks the block manifest without materializing the block body.
func (s *ChainStore) HasBlock(blockHash *[32]byte) (bool, error) {
	size, err := s.BlockSize(blockHash)
	return size > 0, err
}

// BlockSize reads the encoded body size from the manifest. A missing block
// returns zero without an error.
func (s *ChainStore) BlockSize(blockHash *[32]byte) (uint64, error) {
	size, _, err := s.BlockManifest(blockHash)
	return size, err
}

// BlockManifest returns the stored size and checksum without loading chunks.
// A missing block returns zero size and checksum without an error.
func (s *ChainStore) BlockManifest(blockHash *[32]byte) (uint64, [32]byte, error) {
	size, _, checksum, err := readBlockManifest(s.db, blockHash)
	return size, checksum, err
}

func blockSizeFrom(reader valueReader, blockHash *[32]byte) (uint64, error) {
	size, _, _, err := readBlockManifest(reader, blockHash)
	return size, err
}

func readBlockManifest(reader valueReader, blockHash *[32]byte) (uint64, uint32, [32]byte, error) {
	manifest, closer, err := reader.Get(blockKey(*blockHash))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, 0, [32]byte{}, nil
	}
	if err != nil {
		return 0, 0, [32]byte{}, err
	}
	defer closer.Close()
	return decodeStoredBlockManifest(manifest)
}

func getBlockFrom(reader valueReader, blockHash *[32]byte) (*types.Block, error) {
	totalSize, chunkCount, checksum, err := readBlockManifest(reader, blockHash)
	if err != nil || totalSize == 0 {
		return nil, err
	}
	if totalSize > uint64(math.MaxInt) {
		return nil, fmt.Errorf("stored block is too large to materialize: %d bytes", totalSize)
	}
	maxBytes := totalSize
	index, err := getBlockIndexFrom(reader, blockHash)
	if err != nil {
		return nil, err
	}
	if index != nil && index.Height > 0 {
		parent, err := getBlockIndexFrom(reader, &index.Header.PrevBlockHash)
		if err != nil {
			return nil, err
		}
		if parent == nil {
			return nil, fmt.Errorf("missing parent block index for stored block %x", *blockHash)
		}
		profileBytes, err := readValue(reader, metaProfileKey)
		if err != nil {
			return nil, err
		}
		profile, err := types.ParseChainProfile(string(profileBytes))
		if err != nil {
			return nil, err
		}
		maxBytes = consensus.NextBlockSizeLimit(parent.BlockSizeState, consensus.ParamsForProfile(profile))
		if totalSize > maxBytes {
			return nil, fmt.Errorf("stored block exceeds parent-derived size limit: size=%d limit=%d", totalSize, maxBytes)
		}
	}
	capacity := int(totalSize)
	if index == nil || index.Height == 0 {
		capacity = min(capacity, storedBlockChunkBytes)
	}
	buf := make([]byte, 0, capacity)
	for chunkIndex := uint32(0); chunkIndex < chunkCount; chunkIndex++ {
		chunk, err := readValue(reader, blockChunkKey(*blockHash, chunkIndex))
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			return nil, fmt.Errorf("missing stored block chunk %d for %x", chunkIndex, *blockHash)
		}
		if uint64(len(chunk)) != min(totalSize-uint64(len(buf)), uint64(storedBlockChunkBytes)) {
			return nil, fmt.Errorf("invalid stored block chunk size: chunk=%d length=%d", chunkIndex, len(chunk))
		}
		buf = append(buf, chunk...)
	}
	if uint64(len(buf)) != totalSize {
		return nil, fmt.Errorf("stored block size mismatch: manifest=%d chunks=%d", totalSize, len(buf))
	}
	if got := crypto.Sha256d(buf); got != checksum {
		return nil, errors.New("stored block chunk checksum mismatch")
	}
	if len(buf) < types.BlockHeaderEncodedLen {
		return nil, errors.New("stored block is shorter than its canonical header")
	}
	header, err := types.DecodeBlockHeader(buf[:types.BlockHeaderEncodedLen])
	if err != nil {
		return nil, err
	}
	if got := consensus.HeaderHash(&header); got != *blockHash {
		return nil, fmt.Errorf("stored block header hash mismatch: key=%x header=%x", *blockHash, got)
	}
	block, err := types.DecodeBlockOwnedBuffer(buf, maxBytes)
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

func (s *ChainStore) GetBlockIndex(blockHash *[32]byte) (*BlockIndexEntry, error) {
	return getBlockIndexFrom(s.db, blockHash)
}

func getBlockIndexFrom(reader valueReader, blockHash *[32]byte) (*BlockIndexEntry, error) {
	buf, err := readValue(reader, blockIndexKey(*blockHash))
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

type valueReader interface {
	Get([]byte) ([]byte, io.Closer, error)
}

type storeReader interface {
	valueReader
	NewIter(*pebble.IterOptions) (*pebble.Iterator, error)
}

func (s *ChainStore) get(key []byte) ([]byte, error) {
	return readValue(s.db, key)
}

func readValue(reader valueReader, key []byte) ([]byte, error) {
	value, closer, err := reader.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return cloneBytes(value), nil
}

func (s *ChainStore) notifyBackgroundIndexes() {
	if s.filterNotify != nil {
		select {
		case s.filterNotify <- struct{}{}:
		default:
		}
	}
	if s.txIndexNotify != nil {
		select {
		case s.txIndexNotify <- struct{}{}:
		default:
		}
	}
}

func writeMeta(batch *pebble.Batch, state *StoredChainState) error {
	return writeMetaFromMeta(batch, storedChainStateMeta(state))
}

func storedChainStateMeta(state *StoredChainState) *StoredChainStateMeta {
	if state == nil {
		return nil
	}
	checksum := state.UTXOChecksum
	if checksum == ([32]byte{}) {
		checksum = utxochecksum.Compute(state.UTXOs)
	}
	accRoot := state.TipHeader.UTXORoot
	if state.UTXOAccumulator != nil {
		accRoot = state.UTXOAccumulator.Root()
	}
	return &StoredChainStateMeta{
		Profile:             state.Profile,
		Height:              state.Height,
		TipHeader:           state.TipHeader,
		BlockSizeState:      state.BlockSizeState,
		UTXOChecksum:        checksum,
		UTXOCount:           len(state.UTXOs),
		UTXOAccumulatorRoot: accRoot,
		UTXOAccumulator:     state.UTXOAccumulator,
	}
}

func writeMetaFromMeta(batch *pebble.Batch, state *StoredChainStateMeta) error {
	if err := writeHeaderMeta(batch, &StoredHeaderState{
		Profile:   state.Profile,
		Height:    state.Height,
		TipHeader: state.TipHeader,
	}); err != nil {
		return err
	}
	return writeBlockMetaFromMeta(batch, state)
}

func writeBlockMetaFromMeta(batch *pebble.Batch, state *StoredChainStateMeta) error {
	if err := batch.Set(metaProfileKey, []byte(state.Profile.String()), nil); err != nil {
		return err
	}
	if err := batch.Set(metaTipHeightKey, encodeU64(state.Height), nil); err != nil {
		return err
	}
	if err := batch.Set(metaChainstateSchemaVersionKey, encodeU64(chainstateSchemaVersion), nil); err != nil {
		return err
	}
	if err := batch.Set(metaTipHeaderKey, state.TipHeader.Encode(), nil); err != nil {
		return err
	}
	if err := batch.Set(metaBlockSizeStateKey, encodeBlockSizeState(state.BlockSizeState), nil); err != nil {
		return err
	}
	if err := batch.Set(metaUTXOChecksumKey, state.UTXOChecksum[:], nil); err != nil {
		return err
	}
	utxoCount := state.UTXOCount
	if utxoCount == 0 && state.UTXOAccumulator != nil {
		utxoCount = state.UTXOAccumulator.Count()
	}
	if err := batch.Set(metaUTXOCountKey, encodeU64(uint64(utxoCount)), nil); err != nil {
		return err
	}
	accRoot := state.UTXOAccumulatorRoot
	if accRoot == ([32]byte{}) {
		accRoot = state.TipHeader.UTXORoot
	}
	if err := batch.Set(metaUTXOAccumulatorRootKey, accRoot[:], nil); err != nil {
		return err
	}
	return nil
}

func putBlockBatch(batch *pebble.Batch, block *types.Block, entry BlockIndexEntry) error {
	blockHash := consensus.HeaderHash(&block.Header)
	totalSize := block.EncodedLen()
	chunkCount64 := (uint64(totalSize) + storedBlockChunkBytes - 1) / storedBlockChunkBytes
	if chunkCount64 > math.MaxUint32 {
		return errors.New("encoded block requires too many storage chunks")
	}
	chunkCount := uint32(chunkCount64)
	chunkWriter := blockBatchChunkWriter{
		batch:     batch,
		blockHash: blockHash,
		buf:       make([]byte, min(totalSize, storedBlockChunkBytes)),
	}
	firstHasher := sha256.New()
	written, err := block.WriteTo(io.MultiWriter(firstHasher, &chunkWriter))
	if err != nil {
		return err
	}
	if err := chunkWriter.flush(); err != nil {
		return err
	}
	if written != int64(totalSize) || chunkWriter.written != uint64(totalSize) || chunkWriter.index != chunkCount {
		return fmt.Errorf("streamed stored block size mismatch: encoded=%d chunks=%d expected_bytes=%d expected_chunks=%d", chunkWriter.written, chunkWriter.index, totalSize, chunkCount)
	}
	var first [32]byte
	firstHasher.Sum(first[:0])
	checksum := sha256.Sum256(first[:])
	manifest := encodeStoredBlockManifest(uint64(totalSize), chunkCount, checksum)
	if err := batch.Set(blockKey(blockHash), manifest, nil); err != nil {
		return err
	}
	return putHeaderBatch(batch, entry)
}

type blockBatchChunkWriter struct {
	batch     *pebble.Batch
	blockHash [32]byte
	buf       []byte
	used      int
	index     uint32
	written   uint64
}

func (w *blockBatchChunkWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if w.used == 0 && len(p) >= len(w.buf) {
			if err := w.writeChunk(p[:len(w.buf)]); err != nil {
				return written, err
			}
			w.written += uint64(len(w.buf))
			written += len(w.buf)
			p = p[len(w.buf):]
			continue
		}
		copied := copy(w.buf[w.used:], p)
		w.used += copied
		w.written += uint64(copied)
		written += copied
		p = p[copied:]
		if w.used == len(w.buf) {
			if err := w.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (w *blockBatchChunkWriter) flush() error {
	if w.used == 0 {
		return nil
	}
	if err := w.writeChunk(w.buf[:w.used]); err != nil {
		return err
	}
	w.used = 0
	return nil
}

func (w *blockBatchChunkWriter) writeChunk(chunk []byte) error {
	if err := w.batch.Set(blockChunkKey(w.blockHash, w.index), chunk, nil); err != nil {
		return err
	}
	w.index++
	return nil
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

func putHeaderBatch(batch *pebble.Batch, entry BlockIndexEntry) error {
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

func encodeU64BE(v uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, v)
	return out
}

func decodeU64(buf []byte) (uint64, error) {
	if len(buf) != 8 {
		return 0, errors.New("invalid u64 encoding")
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func decodeU64BE(buf []byte) (uint64, error) {
	if len(buf) != 8 {
		return 0, errors.New("invalid u64 encoding")
	}
	return binary.BigEndian.Uint64(buf), nil
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

func blockChunkKey(hash [32]byte, chunkIndex uint32) []byte {
	key := append(append([]byte(nil), blockChunkPrefix...), hash[:]...)
	var index [4]byte
	binary.BigEndian.PutUint32(index[:], chunkIndex)
	return append(key, index[:]...)
}

func encodeStoredBlockManifest(totalSize uint64, chunkCount uint32, checksum [32]byte) []byte {
	manifest := make([]byte, 1+8+4+32)
	manifest[0] = storedBlockManifestVersion
	binary.LittleEndian.PutUint64(manifest[1:9], totalSize)
	binary.LittleEndian.PutUint32(manifest[9:13], chunkCount)
	copy(manifest[13:], checksum[:])
	return manifest
}

func decodeStoredBlockManifest(manifest []byte) (uint64, uint32, [32]byte, error) {
	var checksum [32]byte
	if len(manifest) != 1+8+4+32 || manifest[0] != storedBlockManifestVersion {
		return 0, 0, checksum, errors.New("invalid stored block manifest")
	}
	totalSize := binary.LittleEndian.Uint64(manifest[1:9])
	chunkCount := binary.LittleEndian.Uint32(manifest[9:13])
	expectedChunks := uint64(0)
	if totalSize > 0 {
		expectedChunks = 1 + (totalSize-1)/storedBlockChunkBytes
	}
	if totalSize < types.BlockHeaderEncodedLen || expectedChunks > math.MaxUint32 || uint64(chunkCount) != expectedChunks {
		return 0, 0, checksum, errors.New("invalid stored block manifest chunk count")
	}
	copy(checksum[:], manifest[13:])
	return totalSize, chunkCount, checksum, nil
}

func blockIndexKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockIndexPrefix...), hash[:]...)
}

func blockUndoKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockUndoPrefix...), hash[:]...)
}

func knownPeerKey(addr string) []byte {
	return append(append([]byte(nil), knownPeerPrefix...), []byte(addr)...)
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

func decodeBoolByte(raw byte) (bool, error) {
	switch raw {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errors.New("invalid bool encoding")
	}
}

func encodeUTXOEntry(entry consensus.UtxoEntry) []byte {
	buf := encodeCommittedCoin(entry)
	var height [8]byte
	binary.LittleEndian.PutUint64(height[:], entry.CreatedHeight)
	buf = append(buf, height[:]...)
	if entry.Coinbase {
		return append(buf, 1)
	}
	return append(buf, 0)
}

// encodeCommittedCoin is the commitment-only coin shape used inside durable
// accumulator nodes. Creation origin must not enter the committed leaf hash.
func encodeCommittedCoin(entry consensus.UtxoEntry) []byte {
	entry = consensus.NormalizeUtxoEntry(entry)
	buf := make([]byte, 0, 49)
	buf = append(buf, types.CanonicalVarIntBytes(entry.Type)...)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, entry.ValueAtoms)
	buf = append(buf, value...)
	buf = append(buf, entry.Payload32[:]...)
	return buf
}

func decodeUTXOEntry(buf []byte) (consensus.UtxoEntry, error) {
	entry, consumed, err := decodeUTXOEntryWithLen(buf)
	if err != nil {
		return consensus.UtxoEntry{}, err
	}
	if consumed != len(buf) {
		return consensus.UtxoEntry{}, errors.New("unexpected trailing utxo entry data")
	}
	return entry, nil
}

func decodeUTXOEntryWithLen(buf []byte) (consensus.UtxoEntry, int, error) {
	entry, consumed, err := decodeCommittedCoinWithLen(buf)
	if err != nil {
		return consensus.UtxoEntry{}, 0, err
	}
	remaining := buf[consumed:]
	if len(remaining) < 9 {
		return consensus.UtxoEntry{}, 0, errors.New("invalid utxo origin encoding")
	}
	entry.CreatedHeight = binary.LittleEndian.Uint64(remaining[:8])
	switch remaining[8] {
	case 0:
		entry.Coinbase = false
	case 1:
		entry.Coinbase = true
	default:
		return consensus.UtxoEntry{}, 0, errors.New("invalid utxo coinbase flag")
	}
	return entry, consumed + 9, nil
}

func decodeCommittedCoin(buf []byte) (consensus.UtxoEntry, error) {
	entry, consumed, err := decodeCommittedCoinWithLen(buf)
	if err != nil {
		return consensus.UtxoEntry{}, err
	}
	if consumed != len(buf) {
		return consensus.UtxoEntry{}, errors.New("unexpected trailing committed coin data")
	}
	return entry, nil
}

func decodeCommittedCoinWithLen(buf []byte) (consensus.UtxoEntry, int, error) {
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
		Payload32:  payload32,
	}
	if outputType == types.OutputXOnlyP2PK {
		entry.PubKey = payload32
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
	const minEncodedUndoEntryBytes = 36 + 1 + 8 + 32 + 8 + 1
	if count > uint64(len(buf)/minEncodedUndoEntryBytes) {
		return nil, errors.New("invalid block undo count")
	}
	entries := make([]BlockUndoEntry, 0, int(count))
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
	const headerSize = 24
	if len(buf) < headerSize {
		return consensus.BlockSizeState{}, errors.New("truncated legacy block size state")
	}
	count := binary.LittleEndian.Uint64(buf[16:24])
	if (len(buf)-headerSize)%8 != 0 || count != uint64((len(buf)-headerSize)/8) {
		return consensus.BlockSizeState{}, fmt.Errorf("invalid block size state length: %d", len(buf))
	}
	limit := binary.LittleEndian.Uint64(buf[:8])
	ewma := binary.LittleEndian.Uint64(buf[8:16])
	blockSize := ewma
	if count > 0 {
		blockSize = binary.LittleEndian.Uint64(buf[len(buf)-8:])
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
