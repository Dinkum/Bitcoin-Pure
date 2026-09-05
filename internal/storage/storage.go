package storage

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"slices"
	"sync"
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
	localitySeqPrefix               = []byte("locality_seq/")
	localityMetaPrefix              = []byte("locality_meta/")
	walletOriginPrefix              = []byte("wallet_origin/")
	walletUTXOPrefix                = []byte("wallet_utxo/")
	walletActivityItemPrefix        = []byte("wallet_activity_item/")
	walletActivityHtPrefix          = []byte("wallet_activity_height/")
)

const (
	walletIndexChunkSize        = 10_000
	utxoAccumulatorIndexVersion = uint64(2)
	utxoAccumulatorIndexV1      = uint64(1)
	chainstateSchemaVersion     = uint64(2)
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

// BlockUndoEntry is the durable representation of consensus's authoritative
// pre-block spend delta. The alias keeps the existing byte layout unchanged.
type BlockUndoEntry = consensus.SpentUTXO

type WalletWatchItem struct {
	Type      uint64
	Payload32 [32]byte
}

type WalletIndexedUTXO struct {
	OutPoint types.OutPoint
	Entry    consensus.UtxoEntry
	Height   uint64
	Coinbase bool
}

type WalletActivityRecord struct {
	TxID      [32]byte
	BlockHash [32]byte
	Height    uint64
	Timestamp uint64
	Coinbase  bool
	Received  uint64
	Sent      uint64
	Fee       uint64
}

type walletOriginRecord struct {
	Entry    consensus.UtxoEntry
	Height   uint64
	Coinbase bool
}

type HeaderBatchEntry struct {
	Height    uint64
	Header    types.BlockHeader
	ChainWork [32]byte
}

type ChainStore struct {
	db     *pebble.DB
	logger *slog.Logger

	walletIndexMu sync.Mutex
	deriveNotify  chan struct{}
	stopCh        chan struct{}
	wg            sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error
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
	pebbleOpts := &pebble.Options{Logger: pebbleLogger{logger: logger}}
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
	if err := store.migrateUTXOAccumulatorIndex(); err != nil {
		_ = db.Close()
		return nil, err
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
	countBytes, err := s.get(metaUTXOCountKey)
	if err != nil {
		return nil, false, err
	}
	accRootBytes, err := s.get(metaUTXOAccumulatorRootKey)
	if err != nil {
		return nil, false, err
	}
	if heightBytes == nil && headerBytes == nil && blockSizeBytes == nil {
		return nil, false, nil
	}
	if heightBytes == nil || headerBytes == nil || blockSizeBytes == nil {
		return nil, false, errors.New("invalid data: missing chain metadata")
	}
	schemaBytes, err := s.get(metaChainstateSchemaVersionKey)
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
	if schemaVersion != chainstateSchemaVersion {
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

// migrateUTXOAccumulatorIndex rebuilds the v1 derived index from the canonical
// UTXO keyspace into a separate, bounded-write staging namespace. Only the
// final synced metadata batch activates v2, so an interrupted migration leaves
// the complete v1 index visible rather than a partially written v2 index.
func (s *ChainStore) migrateUTXOAccumulatorIndex() error {
	versionBytes, err := s.get(metaUTXOAccumulatorVersionKey)
	if err != nil || versionBytes == nil {
		return err
	}
	version, err := decodeU64(versionBytes)
	if err != nil {
		return err
	}
	switch version {
	case utxoAccumulatorIndexVersion:
		if _, ok, err := s.LoadUTXOAccumulator(); err != nil {
			return fmt.Errorf("validate utxo accumulator index: %w", err)
		} else if !ok {
			return errors.New("utxo accumulator v2 metadata is present without an index")
		}
		s.cleanupLegacyUTXOAccumulatorIndex()
		return nil
	case utxoAccumulatorIndexV1:
	default:
		return fmt.Errorf("unsupported utxo accumulator index version %d", version)
	}

	meta, err := s.LoadChainStateMeta()
	if err != nil {
		return fmt.Errorf("load chain metadata for utxo accumulator migration: %w", err)
	}
	if meta == nil {
		return errors.New("cannot migrate utxo accumulator index without chain metadata")
	}
	acc := utreexo.NewAccumulator()
	if err := s.ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		next, err := acc.Add(consensus.UtxoLeafFromEntry(outPoint, entry))
		if err != nil {
			return err
		}
		acc = next
		return nil
	}); err != nil {
		return fmt.Errorf("scan canonical utxos for accumulator migration: %w", err)
	}
	if acc.Count() != meta.UTXOCount {
		return fmt.Errorf("refusing utxo accumulator migration: rebuilt count=%d meta=%d", acc.Count(), meta.UTXOCount)
	}
	root := acc.Root()
	if root != meta.TipHeader.UTXORoot {
		return fmt.Errorf("refusing utxo accumulator migration: rebuilt root=%x header=%x", root, meta.TipHeader.UTXORoot)
	}
	if meta.UTXOAccumulatorRoot != ([32]byte{}) && root != meta.UTXOAccumulatorRoot {
		return fmt.Errorf("refusing utxo accumulator migration: rebuilt root=%x meta=%x", root, meta.UTXOAccumulatorRoot)
	}

	// v2 lives in a separate namespace. Populate it in bounded synced chunks
	// while v1 remains active, then make the completed generation visible with
	// one metadata batch. A restart after interrupted staging simply discards
	// the inactive v2 namespace and starts it again.
	clearBatch := s.db.NewBatch()
	if err := clearBatch.DeleteRange(utxoAccumulatorNodePrefix, utxoAccumulatorNodePrefixEnd, nil); err != nil {
		clearBatch.Close()
		return err
	}
	if err := clearBatch.Commit(consensusCriticalWriteOptions); err != nil {
		clearBatch.Close()
		return fmt.Errorf("clear incomplete utxo accumulator staging index: %w", err)
	}
	clearBatch.Close()

	if err := s.stageAccumulatorIndex(acc); err != nil {
		return err
	}
	if err := s.validateAccumulatorIndexRecords(acc); err != nil {
		return fmt.Errorf("validate staged utxo accumulator index: %w", err)
	}

	activation := s.db.NewBatch()
	if rootPath, ok := utreexo.AccumulatorRootPath(acc); ok {
		if err := activation.Set(metaUTXOAccumulatorRootPathKey, encodeAccumulatorPath(rootPath), nil); err != nil {
			activation.Close()
			return err
		}
	} else if err := activation.Delete(metaUTXOAccumulatorRootPathKey, nil); err != nil {
		activation.Close()
		return err
	}
	if err := activation.Set(metaUTXOAccumulatorVersionKey, encodeU64(utxoAccumulatorIndexVersion), nil); err != nil {
		activation.Close()
		return err
	}
	if err := activation.Commit(consensusCriticalWriteOptions); err != nil {
		activation.Close()
		return fmt.Errorf("activate utxo accumulator index migration: %w", err)
	}
	activation.Close()

	s.cleanupLegacyUTXOAccumulatorIndex()
	s.logger.Info("migrated utxo accumulator index", slog.Uint64("from_version", version), slog.Uint64("to_version", utxoAccumulatorIndexVersion), slog.Int("utxo_count", acc.Count()))
	return nil
}

func (s *ChainStore) stageAccumulatorIndex(acc *utreexo.Accumulator) error {
	const migrationRecordBatchSize = 4096
	batch := s.db.NewBatch()
	defer func() { batch.Close() }()
	batchCount := 0
	flush := func() error {
		if batchCount == 0 {
			return nil
		}
		if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
			return err
		}
		batch.Close()
		batch = s.db.NewBatch()
		batchCount = 0
		return nil
	}
	err := utreexo.ForEachAccumulatorNodeRecord(acc, func(record utreexo.AccumulatorNodeRecord) error {
		if err := batch.Set(accumulatorNodeKey(record.Path), encodeAccumulatorNodeValue(record), nil); err != nil {
			return err
		}
		batchCount++
		if batchCount == migrationRecordBatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("stage utxo accumulator index records: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("stage utxo accumulator index records: %w", err)
	}
	return nil
}

func (s *ChainStore) validateAccumulatorIndexRecords(acc *utreexo.Accumulator) error {
	expectedCount := 0
	if err := utreexo.ForEachAccumulatorNodeRecord(acc, func(record utreexo.AccumulatorNodeRecord) error {
		expectedCount++
		value, closer, err := s.db.Get(accumulatorNodeKey(record.Path))
		if err != nil {
			return err
		}
		matches := bytes.Equal(value, encodeAccumulatorNodeValue(record))
		closer.Close()
		if !matches {
			return fmt.Errorf("staged accumulator record mismatch at depth %d", record.Path.Depth)
		}
		return nil
	}); err != nil {
		return err
	}
	actualCount := 0
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: utxoAccumulatorNodePrefix, UpperBound: utxoAccumulatorNodePrefixEnd})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		actualCount++
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if actualCount != expectedCount {
		return fmt.Errorf("staged accumulator record count=%d, want %d", actualCount, expectedCount)
	}
	return nil
}

func (s *ChainStore) cleanupLegacyUTXOAccumulatorIndex() {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: legacyUTXOAccumulatorNodePrefix, UpperBound: legacyUTXOAccumulatorNodePrefixEnd})
	if err != nil {
		s.logger.Warn("failed to inspect legacy utxo accumulator index", slog.String("error", err.Error()))
		return
	}
	hasLegacy := iter.First()
	iterErr := iter.Error()
	iter.Close()
	if iterErr != nil {
		s.logger.Warn("failed to inspect legacy utxo accumulator index", slog.String("error", iterErr.Error()))
		return
	}
	if !hasLegacy {
		return
	}
	cleanup := s.db.NewBatch()
	defer cleanup.Close()
	if err := cleanup.DeleteRange(legacyUTXOAccumulatorNodePrefix, legacyUTXOAccumulatorNodePrefixEnd, nil); err != nil {
		s.logger.Warn("failed to schedule legacy utxo accumulator cleanup", slog.String("error", err.Error()))
		return
	}
	if err := cleanup.Commit(bestEffortWriteOptions); err != nil {
		s.logger.Warn("failed to clean legacy utxo accumulator index", slog.String("error", err.Error()))
	}
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

func (s *ChainStore) LoadLocalityOrderedUTXOs(limit int) ([]LocalityIndexedUTXO, error) {
	items := make(localitySelectionHeap, 0)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: localitySeqPrefix,
		UpperBound: localitySeqPrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		seq, err := decodeLocalitySeqFromKey(iter.Key())
		if err != nil {
			return nil, err
		}
		outPoint, entry, ok, err := decodeLocalitySeqValue(iter.Value())
		if err != nil {
			return nil, err
		}
		if !ok {
			outPoint, err = decodeOutPoint(iter.Value())
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
			entry, err = decodeUTXOEntry(entryBuf)
			if err != nil {
				return nil, err
			}
		}
		item := LocalityIndexedUTXO{
			Sequence: seq,
			OutPoint: outPoint,
			Entry:    entry,
		}
		keepLocalityItem(&items, item, limit)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	slices.SortFunc(items, compareLocalityItems)
	return items, nil
}

func (s *ChainStore) WalletUTXOsByWatchItems(items []WalletWatchItem) ([]WalletIndexedUTXO, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := s.requireWalletIndexReady(); err != nil {
		return nil, err
	}
	out := make([]WalletIndexedUTXO, 0)
	for _, item := range uniqueWalletWatchItems(items) {
		prefix := walletUTXOItemPrefix(item)
		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			outPoint, err := decodeOutPoint(iter.Key()[len(prefix):])
			if err != nil {
				iter.Close()
				return nil, err
			}
			record, err := decodeWalletIndexedUTXO(iter.Value())
			if err != nil {
				iter.Close()
				return nil, err
			}
			record.OutPoint = outPoint
			out = append(out, record)
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return nil, err
		}
		iter.Close()
	}
	slices.SortFunc(out, compareWalletIndexedUTXOs)
	return out, nil
}

func (s *ChainStore) WalletActivityByWatchItems(items []WalletWatchItem, limit int) ([]WalletActivityRecord, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := s.requireWalletIndexReady(); err != nil {
		return nil, err
	}
	uniqueItems := uniqueWalletWatchItems(items)
	cursors := make([]*walletActivityCursor, 0, len(uniqueItems))
	defer func() {
		for _, cursor := range cursors {
			cursor.iter.Close()
		}
	}()
	for _, item := range uniqueItems {
		prefix := walletActivityItemWatchPrefix(item)
		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		cursor := &walletActivityCursor{iter: iter}
		if err := cursor.first(); err != nil {
			iter.Close()
			return nil, err
		}
		if cursor.valid {
			cursors = append(cursors, cursor)
		} else {
			iter.Close()
		}
	}

	activityHeap := walletActivityHeap(cursors)
	heap.Init(&activityHeap)
	out := make([]WalletActivityRecord, 0)
	for activityHeap.Len() > 0 && (limit <= 0 || len(out) < limit) {
		cursor := heap.Pop(&activityHeap).(*walletActivityCursor)
		record := cursor.record
		if err := cursor.next(); err != nil {
			return nil, err
		}
		if cursor.valid {
			heap.Push(&activityHeap, cursor)
		}
		for activityHeap.Len() > 0 {
			next := activityHeap[0]
			if next.record.Height != record.Height || next.record.TxID != record.TxID {
				break
			}
			next = heap.Pop(&activityHeap).(*walletActivityCursor)
			record.Received += next.record.Received
			record.Sent += next.record.Sent
			if next.record.Fee > record.Fee {
				record.Fee = next.record.Fee
			}
			if err := next.next(); err != nil {
				return nil, err
			}
			if next.valid {
				heap.Push(&activityHeap, next)
			}
		}
		out = append(out, record)
	}
	return out, nil
}

func (s *ChainStore) WalletIndexHeight() (*uint64, error) {
	buf, err := s.get(metaWalletIndexHeightKey)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	height, err := decodeU64(buf)
	if err != nil {
		return nil, err
	}
	return &height, nil
}

func (s *ChainStore) requireWalletIndexReady() error {
	height, err := s.WalletIndexHeight()
	if err != nil {
		return err
	}
	if height == nil {
		return errors.New("wallet index is not ready")
	}
	chainMeta, err := s.LoadChainStateMeta()
	if err != nil {
		return err
	}
	if chainMeta == nil {
		return errors.New("chain state is not ready")
	}
	if *height != chainMeta.Height {
		return fmt.Errorf("wallet index height %d does not match chain height %d", *height, chainMeta.Height)
	}
	return nil
}

func uniqueWalletWatchItems(items []WalletWatchItem) []WalletWatchItem {
	if len(items) < 2 {
		return items
	}
	seen := make(map[WalletWatchItem]struct{}, len(items))
	out := make([]WalletWatchItem, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

type walletActivityCursor struct {
	iter   *pebble.Iterator
	record WalletActivityRecord
	valid  bool
}

func (c *walletActivityCursor) first() error {
	c.valid = c.iter.First()
	if !c.valid {
		return c.iter.Error()
	}
	return c.decode()
}

func (c *walletActivityCursor) next() error {
	c.valid = c.iter.Next()
	if !c.valid {
		return c.iter.Error()
	}
	return c.decode()
}

func (c *walletActivityCursor) decode() error {
	record, err := decodeWalletActivityRecord(c.iter.Value())
	if err != nil {
		return err
	}
	c.record = record
	return nil
}

type walletActivityHeap []*walletActivityCursor

func (h walletActivityHeap) Len() int { return len(h) }

func (h walletActivityHeap) Less(i, j int) bool {
	return compareWalletActivityRecords(h[i].record, h[j].record) < 0
}

func (h walletActivityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *walletActivityHeap) Push(x any) {
	*h = append(*h, x.(*walletActivityCursor))
}

func (h *walletActivityHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Keep a max-heap so selecting the smallest numeric sequences costs O(log limit)
// per retained row instead of rescanning every selected row for every input.
func keepLocalityItem(items *localitySelectionHeap, item LocalityIndexedUTXO, limit int) {
	if limit <= 0 {
		*items = append(*items, item)
		return
	}
	if len(*items) < limit {
		heap.Push(items, item)
	} else if compareLocalityItems(item, (*items)[0]) < 0 {
		(*items)[0] = item
		heap.Fix(items, 0)
	}
}

type localitySelectionHeap []LocalityIndexedUTXO

func (h localitySelectionHeap) Len() int           { return len(h) }
func (h localitySelectionHeap) Less(i, j int) bool { return compareLocalityItems(h[i], h[j]) > 0 }
func (h localitySelectionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *localitySelectionHeap) Push(x any)        { *h = append(*h, x.(LocalityIndexedUTXO)) }
func (h *localitySelectionHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func compareLocalityItems(a, b LocalityIndexedUTXO) int {
	switch {
	case a.Sequence < b.Sequence:
		return -1
	case a.Sequence > b.Sequence:
		return 1
	default:
		return compareOutPoints(a.OutPoint, b.OutPoint)
	}
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
	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()
	return s.writeFullStateLocked(state, nil, nil, nil)
}

func (s *ChainStore) WriteFullStateWithHeaderMetadata(state *StoredChainState, headerState *StoredHeaderState, activeEntries []BlockIndexEntry) error {
	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()
	return s.writeFullStateLocked(state, nil, headerState, activeEntries)
}

func (s *ChainStore) WriteFullStateWithFastSyncStateMetadata(state *StoredChainState, fastSyncState *FastSyncState, headerState *StoredHeaderState, activeEntries []BlockIndexEntry) error {
	if fastSyncState == nil {
		return errors.New("fast sync state is required")
	}
	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()
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
		if err := deletePrefixBatch(s.db, batch, snapshotUTXOPrefix, snapshotUTXOPrefixEnd); err != nil {
			return err
		}
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
	if err := invalidateWalletIndexesBatch(batch); err != nil {
		return err
	}
	if err := s.rebuildLocalityIndexBatch(batch, state.UTXOs); err != nil {
		return err
	}
	if err := replaceAccumulatorIndexBatch(s.db, batch, state.UTXOAccumulator); err != nil {
		return err
	}
	if len(activeEntries) > 0 {
		pairs := journalPairsFromEntries(activeEntries)
		if err := s.appendJournalEntriesBatch(batch,
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

func (s *ChainStore) WriteFastSyncStateMetadata(state *FastSyncState) error {
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
	if err := replaceAccumulatorIndexBatch(s.db, batch, next.UTXOAccumulator); err != nil {
		return err
	}
	if err := invalidateWalletIndexesBatch(batch); err != nil {
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
	if err := s.populateAccumulatorDeltaFromUTXODelta(meta, spent, created); err != nil {
		return err
	}

	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()

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
	if err := applyAccumulatorStateBatch(s.db, batch, meta); err != nil {
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
	if state == nil {
		return errors.New("chain state metadata is required")
	}
	if err := s.populateAccumulatorDeltaFromUTXODelta(state, spent, created); err != nil {
		return err
	}
	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMetaFromMeta(batch, state); err != nil {
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
	if err := applyAccumulatorStateBatch(s.db, batch, state); err != nil {
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
		err = s.applyWalletActiveBlockBatch(batch, state.Height, hash, block, undo, spent, created)
	} else {
		err = invalidateWalletIndexesBatch(batch)
	}
	if err != nil {
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
	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := putBlockBatch(batch, block, *entry, false); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	if err := putWalletOriginsForBlockBatch(batch, entry.Height, hash, block); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) PutValidatedBlockWithoutWalletIndex(block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry) error {
	if block == nil || entry == nil {
		return errors.New("block and index entry are required")
	}
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

func (s *ChainStore) PutValidatedBlocksWithoutWalletIndex(blocks []types.Block, entries []BlockIndexEntry) error {
	if len(blocks) != len(entries) {
		return fmt.Errorf("block batch length mismatch: blocks=%d entries=%d", len(blocks), len(entries))
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for i := range blocks {
		if err := putBlockBatch(batch, &blocks[i], entries[i], false); err != nil {
			return err
		}
		hash := consensus.HeaderHash(&blocks[i].Header)
		if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(nil), nil); err != nil {
			return err
		}
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) PutValidatedBlockUndo(entry *BlockIndexEntry, undo []BlockUndoEntry) error {
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
	manifest, err := s.get(blockKey(*blockHash))
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}
	totalSize, chunkCount, checksum, err := decodeStoredBlockManifest(manifest)
	if err != nil {
		return nil, err
	}
	if totalSize > uint64(math.MaxInt) {
		return nil, fmt.Errorf("stored block is too large to materialize: %d bytes", totalSize)
	}
	buf := make([]byte, 0, int(totalSize))
	for chunkIndex := uint32(0); chunkIndex < chunkCount; chunkIndex++ {
		chunk, err := s.get(blockChunkKey(*blockHash, chunkIndex))
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			return nil, fmt.Errorf("missing stored block chunk %d for %x", chunkIndex, *blockHash)
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
	maxBytes := uint64(len(buf))
	index, err := s.GetBlockIndex(blockHash)
	if err != nil {
		return nil, err
	}
	if index != nil && index.Height > 0 {
		parent, err := s.GetBlockIndex(&header.PrevBlockHash)
		if err != nil {
			return nil, err
		}
		if parent == nil {
			return nil, fmt.Errorf("missing parent block index for stored block %x", *blockHash)
		}
		profileBytes, err := s.get(metaProfileKey)
		if err != nil {
			return nil, err
		}
		profile, err := types.ParseChainProfile(string(profileBytes))
		if err != nil {
			return nil, err
		}
		maxBytes = consensus.NextBlockSizeLimit(parent.BlockSizeState, consensus.ParamsForProfile(profile))
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
	startSeq := nextSeq
	derivedSeq, err := s.derivedJournalSeq()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := batch.Set(journalKey(nextSeq), encodeChainJournalEntry(entry), nil); err != nil {
			return err
		}
		if err := applyJournalEntryBatch(batch, entry); err != nil {
			return err
		}
		nextSeq++
	}
	if derivedSeq >= startSeq {
		if err := batch.Set(metaDerivedJournalSeqKey, encodeU64(nextSeq), nil); err != nil {
			return err
		}
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
		if err := batch.Set(localitySeqKey(uint64(seq)), encodeLocalitySeqValue(outPoint, utxos[outPoint]), nil); err != nil {
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
		if oldSeq, ok, err := s.localitySeqForOutPoint(outPoint); err != nil {
			return err
		} else if ok {
			if err := batch.Delete(localitySeqKey(oldSeq), nil); err != nil {
				return err
			}
			if err := batch.Delete(localityMetaKey(outPoint), nil); err != nil {
				return err
			}
		}
		if err := batch.Set(localitySeqKey(nextSeq), encodeLocalitySeqValue(outPoint, created[outPoint]), nil); err != nil {
			return err
		}
		if err := batch.Set(localityMetaKey(outPoint), encodeU64(nextSeq), nil); err != nil {
			return err
		}
		nextSeq++
	}
	return batch.Set(metaLocalityNextSeqKey, encodeU64(nextSeq), nil)
}

func (s *ChainStore) RebuildWalletIndexes(tipHeight uint64) error {
	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()
	return s.rebuildWalletIndexesLocked(tipHeight)
}

func (s *ChainStore) RebuildWalletIndexesAtCurrentTip() (uint64, error) {
	s.walletIndexMu.Lock()
	defer s.walletIndexMu.Unlock()
	meta, err := s.LoadChainStateMeta()
	if err != nil {
		return 0, err
	}
	if meta == nil {
		return 0, errors.New("chain state is not ready")
	}
	return meta.Height, s.rebuildWalletIndexesLocked(meta.Height)
}

func (s *ChainStore) rebuildWalletIndexesLocked(tipHeight uint64) error {
	batch := s.db.NewBatch()
	if err := s.deleteWalletIndexesBatch(batch); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	batch = s.db.NewBatch()
	for height := uint64(0); height <= tipHeight; height++ {
		hash, err := s.GetBlockHashByHeight(height)
		if err != nil {
			batch.Close()
			return err
		}
		if hash == nil {
			batch.Close()
			return fmt.Errorf("missing active block hash at height %d during wallet index rebuild", height)
		}
		block, err := s.GetBlock(hash)
		if err != nil {
			batch.Close()
			return err
		}
		if block == nil {
			batch.Close()
			return fmt.Errorf("missing active block %x at height %d during wallet index rebuild", *hash, height)
		}
		undo, err := s.GetUndo(hash)
		if err != nil {
			batch.Close()
			return err
		}
		if err := putWalletOriginsForBlockBatchWithMap(batch, height, *hash, block, nil); err != nil {
			batch.Close()
			return err
		}
		if err := putWalletActivityForBlockBatch(batch, height, *hash, block, undo); err != nil {
			batch.Close()
			return err
		}
		if height%1000 == 999 {
			if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
				batch.Close()
				return err
			}
			batch.Close()
			batch = s.db.NewBatch()
		}
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	batch = s.db.NewBatch()
	writtenUTXOs := 0
	if err := s.ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		origin, ok, err := s.walletOrigin(outPoint)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := batch.Set(walletUTXOKey(walletWatchItemForEntry(entry), outPoint), encodeWalletIndexedUTXO(WalletIndexedUTXO{
			Entry:    entry,
			Height:   origin.Height,
			Coinbase: origin.Coinbase,
		}), nil); err != nil {
			return err
		}
		writtenUTXOs++
		if writtenUTXOs%10_000 != 0 {
			return nil
		}
		if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
			return err
		}
		batch.Close()
		batch = s.db.NewBatch()
		return nil
	}); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Set(metaWalletIndexHeightKey, encodeU64(tipHeight), nil); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		batch.Close()
		return err
	}
	batch.Close()
	s.logger.Info("rebuilt wallet indexes", slog.Uint64("height", tipHeight))
	return nil
}

func (s *ChainStore) walletIndexReadyForAppend(height uint64) (bool, error) {
	indexHeight, err := s.WalletIndexHeight()
	if err != nil {
		return false, err
	}
	if indexHeight == nil {
		return height == 0, nil
	}
	if height == 0 {
		return *indexHeight == 0, nil
	}
	return *indexHeight == height-1, nil
}

func (s *ChainStore) walletIndexReadyForReorg(oldTipHeight uint64) (bool, error) {
	indexHeight, err := s.WalletIndexHeight()
	if err != nil {
		return false, err
	}
	return indexHeight != nil && *indexHeight == oldTipHeight, nil
}

func (s *ChainStore) applyWalletActiveBlockBatch(batch *pebble.Batch, height uint64, hash [32]byte, block *types.Block, undo []BlockUndoEntry, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	origins := make(map[types.OutPoint]walletOriginRecord)
	if err := putWalletOriginsForBlockBatchWithMap(batch, height, hash, block, origins); err != nil {
		return err
	}
	if err := putWalletActivityForBlockBatch(batch, height, hash, block, undo); err != nil {
		return err
	}
	if err := s.applyWalletUTXODeltaBatch(batch, spent, created, origins); err != nil {
		return err
	}
	return batch.Set(metaWalletIndexHeightKey, encodeU64(height), nil)
}

func (s *ChainStore) applyWalletReorgBatch(batch *pebble.Batch, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry, forkHeight uint64, oldTipHeight uint64, activeEntries []BlockIndexEntry) error {
	if err := s.deleteWalletActivityHeightRangeBatch(batch, forkHeight+1, oldTipHeight); err != nil {
		return err
	}
	origins := make(map[types.OutPoint]walletOriginRecord)
	for _, entry := range activeEntries {
		hash := consensus.HeaderHash(&entry.Header)
		block, err := s.GetBlock(&hash)
		if err != nil {
			return err
		}
		if block == nil {
			return fmt.Errorf("missing active block for wallet reorg index %x", hash)
		}
		undo, err := s.GetUndo(&hash)
		if err != nil {
			return err
		}
		if err := putWalletOriginsForBlockBatchWithMap(batch, entry.Height, hash, block, origins); err != nil {
			return err
		}
		if err := putWalletActivityForBlockBatch(batch, entry.Height, hash, block, undo); err != nil {
			return err
		}
	}
	if err := s.applyWalletUTXODeltaBatch(batch, spent, created, origins); err != nil {
		return err
	}
	if len(activeEntries) == 0 {
		return batch.Delete(metaWalletIndexHeightKey, nil)
	}
	return batch.Set(metaWalletIndexHeightKey, encodeU64(activeEntries[len(activeEntries)-1].Height), nil)
}

func (s *ChainStore) applyWalletUTXODeltaBatch(batch *pebble.Batch, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry, origins map[types.OutPoint]walletOriginRecord) error {
	for _, outPoint := range spent {
		origin, ok, err := s.walletOrigin(outPoint)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := batch.Delete(walletUTXOKey(walletWatchItemForEntry(origin.Entry), outPoint), nil); err != nil {
			return err
		}
	}
	for outPoint, entry := range created {
		origin, ok := origins[outPoint]
		if !ok {
			var err error
			origin, ok, err = s.walletOrigin(outPoint)
			if err != nil {
				return err
			}
		}
		if !ok {
			continue
		}
		origin.Entry = entry
		if err := batch.Set(walletUTXOKey(walletWatchItemForEntry(entry), outPoint), encodeWalletIndexedUTXO(WalletIndexedUTXO{
			Entry:    entry,
			Height:   origin.Height,
			Coinbase: origin.Coinbase,
		}), nil); err != nil {
			return err
		}
	}
	return nil
}

func putWalletOriginsForBlockBatch(batch *pebble.Batch, height uint64, hash [32]byte, block *types.Block) error {
	return putWalletOriginsForBlockBatchWithMap(batch, height, hash, block, nil)
}

func putWalletOriginsForBlockBatchWithMap(batch *pebble.Batch, height uint64, _ [32]byte, block *types.Block, origins map[types.OutPoint]walletOriginRecord) error {
	if block == nil {
		return nil
	}
	for txIndex := range block.Txs {
		txid := consensus.TxID(&block.Txs[txIndex])
		for vout, output := range block.Txs[txIndex].Base.Outputs {
			outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			entry := consensus.UtxoEntryFromOutputAtHeight(output, height, txIndex == 0)
			record := walletOriginRecord{Entry: entry, Height: height, Coinbase: txIndex == 0}
			if origins != nil {
				origins[outPoint] = record
			}
			if err := batch.Set(walletOriginKey(outPoint), encodeWalletOrigin(record), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func putWalletActivityForBlockBatch(batch *pebble.Batch, height uint64, hash [32]byte, block *types.Block, undo []BlockUndoEntry) error {
	if block == nil {
		return nil
	}
	resolvedInputs, err := consensus.ResolveBlockInputEntries(block, undo)
	if err != nil {
		return fmt.Errorf("resolve wallet activity inputs at height %d: %w", height, err)
	}
	timestamp := block.Header.Timestamp
	for txIndex, tx := range block.Txs {
		type itemDelta struct {
			received uint64
			sent     uint64
		}
		deltas := make(map[WalletWatchItem]itemDelta)
		for _, output := range tx.Base.Outputs {
			item := walletWatchItemForOutput(output)
			delta := deltas[item]
			delta.received += output.ValueAtoms
			deltas[item] = delta
		}
		inputSum := uint64(0)
		if txIndex > 0 {
			for _, input := range tx.Base.Inputs {
				entry, ok := resolvedInputs[input.PrevOut]
				if !ok {
					return fmt.Errorf("missing resolved wallet activity input at height %d: %v", height, input.PrevOut)
				}
				inputSum += entry.ValueAtoms
				item := walletWatchItemForEntry(entry)
				delta := deltas[item]
				delta.sent += entry.ValueAtoms
				deltas[item] = delta
			}
		}
		txid := consensus.TxID(&tx)
		if len(deltas) == 0 {
			continue
		}
		outputSum := uint64(0)
		for _, output := range tx.Base.Outputs {
			outputSum += output.ValueAtoms
		}
		fee := uint64(0)
		if txIndex > 0 && inputSum >= outputSum {
			fee = inputSum - outputSum
		}
		for item, delta := range deltas {
			if delta.received == 0 && delta.sent == 0 {
				continue
			}
			itemFee := uint64(0)
			if delta.sent > 0 {
				itemFee = fee
			}
			record := WalletActivityRecord{
				TxID:      txid,
				BlockHash: hash,
				Height:    height,
				Timestamp: timestamp,
				Coinbase:  txIndex == 0,
				Received:  delta.received,
				Sent:      delta.sent,
				Fee:       itemFee,
			}
			encoded := encodeWalletActivityRecord(record)
			if err := batch.Set(walletActivityItemKey(item, height, txid), encoded, nil); err != nil {
				return err
			}
			if err := batch.Set(walletActivityHeightKey(height, item, txid), nil, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *ChainStore) deleteWalletActivityHeightBatch(batch *pebble.Batch, height uint64) error {
	prefix := walletActivityHeightPrefixForHeight(height)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		item, txid, err := decodeWalletActivityHeightKey(iter.Key(), height)
		if err != nil {
			return err
		}
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
		if err := batch.Delete(walletActivityItemKey(item, height, txid), nil); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *ChainStore) deleteWalletActivityHeightRangeBatch(batch *pebble.Batch, firstHeight uint64, lastHeight uint64) error {
	if firstHeight > lastHeight {
		return nil
	}
	lower := walletActivityHeightPrefixForHeight(firstHeight)
	var upper []byte
	if lastHeight == ^uint64(0) {
		upper = walletActHtPrefixEnd
	} else {
		upper = walletActivityHeightPrefixForHeight(lastHeight + 1)
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) < len(walletActivityHtPrefix)+8 {
			return errors.New("invalid wallet activity height key")
		}
		height, err := decodeU64BE(iter.Key()[len(walletActivityHtPrefix) : len(walletActivityHtPrefix)+8])
		if err != nil {
			return err
		}
		item, txid, err := decodeWalletActivityHeightKey(iter.Key(), height)
		if err != nil {
			return err
		}
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
		if err := batch.Delete(walletActivityItemKey(item, height, txid), nil); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *ChainStore) deleteWalletIndexesBatch(batch *pebble.Batch) error {
	for _, bounds := range []struct {
		lower []byte
		upper []byte
	}{
		{walletOriginPrefix, walletOriginPrefixEnd},
		{walletUTXOPrefix, walletUTXOPrefixEnd},
		{walletActivityItemPrefix, walletActItemPrefixEnd},
		{walletActivityHtPrefix, walletActHtPrefixEnd},
	} {
		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: bounds.lower,
			UpperBound: bounds.upper,
		})
		if err != nil {
			return err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
				iter.Close()
				return err
			}
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return err
		}
		iter.Close()
	}
	return batch.Delete(metaWalletIndexHeightKey, nil)
}

func invalidateWalletIndexesBatch(batch *pebble.Batch) error {
	return batch.Delete(metaWalletIndexHeightKey, nil)
}

func deletePrefixBatch(db *pebble.DB, batch *pebble.Batch, lower, upper []byte) error {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
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
	return iter.Error()
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
		return compareOutPoints(a, b)
	})
}

func compareOutPoints(a, b types.OutPoint) int {
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

func putBlockBatch(batch *pebble.Batch, block *types.Block, entry BlockIndexEntry, active bool) error {
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
		buf:       make([]byte, storedBlockChunkBytes),
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
	return putHeaderBatch(batch, entry, active)
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

func encodeU64BE(v uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, v)
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
		expectedChunks = (totalSize + storedBlockChunkBytes - 1) / storedBlockChunkBytes
	}
	if expectedChunks > math.MaxUint32 || uint64(chunkCount) != expectedChunks {
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

func walletOriginKey(outPoint types.OutPoint) []byte {
	buf := append([]byte(nil), walletOriginPrefix...)
	outPoint.Encode(&buf)
	return buf
}

func walletUTXOItemPrefix(item WalletWatchItem) []byte {
	buf := append([]byte(nil), walletUTXOPrefix...)
	encodeWalletWatchItem(&buf, item)
	return buf
}

func walletUTXOKey(item WalletWatchItem, outPoint types.OutPoint) []byte {
	buf := walletUTXOItemPrefix(item)
	outPoint.Encode(&buf)
	return buf
}

func walletActivityItemWatchPrefix(item WalletWatchItem) []byte {
	buf := append([]byte(nil), walletActivityItemPrefix...)
	encodeWalletWatchItem(&buf, item)
	return buf
}

func walletActivityItemKey(item WalletWatchItem, height uint64, txid [32]byte) []byte {
	buf := walletActivityItemWatchPrefix(item)
	buf = append(buf, encodeU64BE(^height)...)
	buf = append(buf, txid[:]...)
	return buf
}

func walletActivityHeightPrefixForHeight(height uint64) []byte {
	return append(append([]byte(nil), walletActivityHtPrefix...), encodeU64BE(height)...)
}

func walletActivityHeightKey(height uint64, item WalletWatchItem, txid [32]byte) []byte {
	buf := walletActivityHeightPrefixForHeight(height)
	encodeWalletWatchItem(&buf, item)
	buf = append(buf, txid[:]...)
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

func encodeWalletWatchItem(buf *[]byte, item WalletWatchItem) {
	*buf = append(*buf, encodeU64(item.Type)...)
	*buf = append(*buf, item.Payload32[:]...)
}

func decodeWalletWatchItem(buf []byte) (WalletWatchItem, error) {
	if len(buf) != 40 {
		return WalletWatchItem{}, errors.New("invalid wallet watch item encoding")
	}
	itemType, err := decodeU64(buf[:8])
	if err != nil {
		return WalletWatchItem{}, err
	}
	var payload [32]byte
	copy(payload[:], buf[8:40])
	return WalletWatchItem{Type: itemType, Payload32: payload}, nil
}

func encodeWalletOrigin(record walletOriginRecord) []byte {
	buf := make([]byte, 0, 8+1+49)
	buf = append(buf, encodeU64(record.Height)...)
	if record.Coinbase {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return append(buf, encodeUTXOEntry(record.Entry)...)
}

func decodeWalletOrigin(buf []byte) (walletOriginRecord, error) {
	if len(buf) < 10 {
		return walletOriginRecord{}, errors.New("invalid wallet origin encoding")
	}
	height, err := decodeU64(buf[:8])
	if err != nil {
		return walletOriginRecord{}, err
	}
	coinbase, err := decodeBoolByte(buf[8])
	if err != nil {
		return walletOriginRecord{}, err
	}
	entry, err := decodeUTXOEntry(buf[9:])
	if err != nil {
		return walletOriginRecord{}, err
	}
	return walletOriginRecord{Height: height, Coinbase: coinbase, Entry: entry}, nil
}

func encodeWalletIndexedUTXO(record WalletIndexedUTXO) []byte {
	return encodeWalletOrigin(walletOriginRecord{Entry: record.Entry, Height: record.Height, Coinbase: record.Coinbase})
}

func decodeWalletIndexedUTXO(buf []byte) (WalletIndexedUTXO, error) {
	origin, err := decodeWalletOrigin(buf)
	if err != nil {
		return WalletIndexedUTXO{}, err
	}
	return WalletIndexedUTXO{Entry: origin.Entry, Height: origin.Height, Coinbase: origin.Coinbase}, nil
}

func encodeWalletActivityRecord(record WalletActivityRecord) []byte {
	buf := make([]byte, 0, 105)
	buf = append(buf, record.TxID[:]...)
	buf = append(buf, record.BlockHash[:]...)
	buf = append(buf, encodeU64(record.Height)...)
	buf = append(buf, encodeU64(record.Timestamp)...)
	if record.Coinbase {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = append(buf, encodeU64(record.Received)...)
	buf = append(buf, encodeU64(record.Sent)...)
	buf = append(buf, encodeU64(record.Fee)...)
	return buf
}

func decodeWalletActivityRecord(buf []byte) (WalletActivityRecord, error) {
	if len(buf) != 105 {
		return WalletActivityRecord{}, errors.New("invalid wallet activity encoding")
	}
	var record WalletActivityRecord
	copy(record.TxID[:], buf[:32])
	copy(record.BlockHash[:], buf[32:64])
	var err error
	if record.Height, err = decodeU64(buf[64:72]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Timestamp, err = decodeU64(buf[72:80]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Coinbase, err = decodeBoolByte(buf[80]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Received, err = decodeU64(buf[81:89]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Sent, err = decodeU64(buf[89:97]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Fee, err = decodeU64(buf[97:105]); err != nil {
		return WalletActivityRecord{}, err
	}
	return record, nil
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

func decodeWalletActivityHeightKey(key []byte, height uint64) (WalletWatchItem, [32]byte, error) {
	prefixLen := len(walletActivityHtPrefix) + 8
	if len(key) != prefixLen+40+32 {
		return WalletWatchItem{}, [32]byte{}, errors.New("invalid wallet activity height key")
	}
	gotHeight, err := decodeU64BE(key[len(walletActivityHtPrefix):prefixLen])
	if err != nil {
		return WalletWatchItem{}, [32]byte{}, err
	}
	if gotHeight != height {
		return WalletWatchItem{}, [32]byte{}, errors.New("wallet activity height key mismatch")
	}
	item, err := decodeWalletWatchItem(key[prefixLen : prefixLen+40])
	if err != nil {
		return WalletWatchItem{}, [32]byte{}, err
	}
	var txid [32]byte
	copy(txid[:], key[prefixLen+40:])
	return item, txid, nil
}

func walletWatchItemForOutput(output types.TxOutput) WalletWatchItem {
	return WalletWatchItem{Type: output.Type, Payload32: output.CanonicalPayload32()}
}

func walletWatchItemForEntry(entry consensus.UtxoEntry) WalletWatchItem {
	item := WalletWatchItem{Type: entry.Type, Payload32: entry.Payload32}
	if item.Type == types.OutputXOnlyP2PK && item.Payload32 == ([32]byte{}) {
		item.Payload32 = entry.PubKey
	}
	return item
}

func (s *ChainStore) walletOrigin(outPoint types.OutPoint) (walletOriginRecord, bool, error) {
	buf, err := s.get(walletOriginKey(outPoint))
	if err != nil {
		return walletOriginRecord{}, false, err
	}
	if buf == nil {
		return walletOriginRecord{}, false, nil
	}
	record, err := decodeWalletOrigin(buf)
	if err != nil {
		return walletOriginRecord{}, false, err
	}
	return record, true, nil
}

func compareWalletIndexedUTXOs(a, b WalletIndexedUTXO) int {
	if a.Entry.Type < b.Entry.Type {
		return -1
	}
	if a.Entry.Type > b.Entry.Type {
		return 1
	}
	aItem := walletWatchItemForEntry(a.Entry)
	bItem := walletWatchItemForEntry(b.Entry)
	if cmp := bytes.Compare(aItem.Payload32[:], bItem.Payload32[:]); cmp != 0 {
		return cmp
	}
	return compareOutPoints(a.OutPoint, b.OutPoint)
}

func compareWalletActivityRecords(a, b WalletActivityRecord) int {
	switch {
	case a.Height > b.Height:
		return -1
	case a.Height < b.Height:
		return 1
	}
	if cmp := bytes.Compare(a.TxID[:], b.TxID[:]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(a.BlockHash[:], b.BlockHash[:])
}

func encodeLocalitySeqValue(outPoint types.OutPoint, entry consensus.UtxoEntry) []byte {
	buf := make([]byte, 0, 36+49)
	outPoint.Encode(&buf)
	return append(buf, encodeUTXOEntry(entry)...)
}

func decodeLocalitySeqValue(buf []byte) (types.OutPoint, consensus.UtxoEntry, bool, error) {
	if len(buf) == 36 {
		return types.OutPoint{}, consensus.UtxoEntry{}, false, nil
	}
	if len(buf) < 37 {
		return types.OutPoint{}, consensus.UtxoEntry{}, false, errors.New("invalid locality sequence value")
	}
	outPoint, err := decodeOutPoint(buf[:36])
	if err != nil {
		return types.OutPoint{}, consensus.UtxoEntry{}, false, err
	}
	entry, err := decodeUTXOEntry(buf[36:])
	if err != nil {
		return types.OutPoint{}, consensus.UtxoEntry{}, false, err
	}
	return outPoint, entry, true, nil
}

func decodeLocalitySeqFromKey(key []byte) (uint64, error) {
	if len(key) != len(localitySeqPrefix)+8 {
		return 0, errors.New("invalid locality sequence key")
	}
	return decodeU64(key[len(localitySeqPrefix):])
}

func (s *ChainStore) LoadUTXOAccumulator() (*utreexo.Accumulator, bool, error) {
	versionBytes, err := s.get(metaUTXOAccumulatorVersionKey)
	if err != nil {
		return nil, false, err
	}
	if versionBytes == nil {
		return nil, false, nil
	}
	version, err := decodeU64(versionBytes)
	if err != nil {
		return nil, false, err
	}
	if version != utxoAccumulatorIndexVersion {
		return nil, false, fmt.Errorf("unsupported utxo accumulator index version %d", version)
	}
	rootPathBytes, err := s.get(metaUTXOAccumulatorRootPathKey)
	if err != nil {
		return nil, false, err
	}
	meta, err := s.LoadChainStateMeta()
	if err != nil {
		return nil, false, err
	}
	if meta == nil {
		return nil, false, nil
	}
	acc, physicalRecordCount, err := s.loadAccumulatorFromLeafRecords(utxoAccumulatorNodePrefix, utxoAccumulatorNodePrefixEnd)
	if err != nil {
		return nil, false, err
	}
	if physicalRecordCount != utreexo.MaterialNodeCount(acc) {
		return nil, false, fmt.Errorf("utxo accumulator physical record count=%d, want %d", physicalRecordCount, utreexo.MaterialNodeCount(acc))
	}
	if err := s.validateAccumulatorIndexRecords(acc); err != nil {
		return nil, false, err
	}
	if acc.Count() != meta.UTXOCount {
		return nil, false, fmt.Errorf("utxo accumulator count mismatch: index=%d meta=%d", acc.Count(), meta.UTXOCount)
	}
	if acc.Root() != meta.TipHeader.UTXORoot {
		return nil, false, fmt.Errorf("utxo accumulator root mismatch: index=%x header=%x", acc.Root(), meta.TipHeader.UTXORoot)
	}
	if meta.UTXOAccumulatorRoot != ([32]byte{}) && acc.Root() != meta.UTXOAccumulatorRoot {
		return nil, false, fmt.Errorf("utxo accumulator root mismatch: index=%x meta=%x", acc.Root(), meta.UTXOAccumulatorRoot)
	}
	rootPath, hasRoot := utreexo.AccumulatorRootPath(acc)
	if !hasRoot {
		if len(rootPathBytes) != 0 {
			return nil, false, errors.New("utxo accumulator root path present for empty index")
		}
	} else {
		persistedRootPath, err := decodeAccumulatorPath(rootPathBytes)
		if err != nil {
			return nil, false, fmt.Errorf("invalid utxo accumulator root path: %w", err)
		}
		if persistedRootPath != rootPath {
			return nil, false, errors.New("utxo accumulator root path mismatch")
		}
	}
	return acc, true, nil
}

func (s *ChainStore) loadAccumulatorFromLeafRecords(lower, upper []byte) (*utreexo.Accumulator, int, error) {
	acc := utreexo.NewAccumulator()
	physicalRecordCount := 0
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		physicalRecordCount++
		path, err := decodeAccumulatorNodeKey(iter.Key())
		if err != nil {
			return nil, 0, err
		}
		record, err := decodeAccumulatorNodeValue(path, iter.Value())
		if err != nil {
			return nil, 0, err
		}
		if record.Leaf != nil {
			next, err := acc.Add(*record.Leaf)
			if err != nil {
				return nil, 0, err
			}
			acc = next
		}
	}
	if err := iter.Error(); err != nil {
		return nil, 0, err
	}
	return acc, physicalRecordCount, nil
}

func (s *ChainStore) UTXOAccumulatorProof(outPoint types.OutPoint) (utreexo.OutPointProof, error) {
	proof := utreexo.OutPointProof{Version: utreexo.ProofVersion, OutPoint: outPoint}
	meta, err := s.LoadChainStateMeta()
	if err != nil {
		return proof, err
	}
	if meta == nil || meta.UTXOCount == 0 {
		return proof, nil
	}
	rootPathBytes, err := s.get(metaUTXOAccumulatorRootPathKey)
	if err != nil {
		return proof, err
	}
	path, err := decodeAccumulatorPath(rootPathBytes)
	if err != nil {
		return proof, fmt.Errorf("invalid utxo accumulator root path: %w", err)
	}
	node, ok, err := s.accumulatorNode(path)
	if err != nil {
		return proof, err
	}
	if !ok {
		return proof, errors.New("missing utxo accumulator root node")
	}
	queryKey := utreexo.OutPointKey(outPoint)
	if !accumulatorPathMatchesKey(path, queryKey) {
		terminal, err := s.firstAccumulatorLeaf(path)
		if err != nil {
			return proof, err
		}
		membership, err := s.UTXOAccumulatorProof(terminal.OutPoint)
		if err != nil {
			return proof, err
		}
		terminalCopy := *terminal
		proof.Terminal = &terminalCopy
		proof.Steps = membership.Steps
		return proof, nil
	}
	steps := make([]utreexo.ProofStep, utreexo.KeyBits)
	for node.Leaf == nil {
		if node.LeftPath == nil || node.RightPath == nil || !validAccumulatorChildren(path, *node.LeftPath, *node.RightPath) {
			return proof, errors.New("invalid accumulator branch record")
		}
		queryRight := accumulatorBitSet(queryKey, path.Depth)
		nextPath := node.LeftPath
		siblingPath := node.RightPath
		if queryRight {
			nextPath, siblingPath = node.RightPath, node.LeftPath
		}
		sibling, siblingOK, err := s.accumulatorNode(*siblingPath)
		if err != nil {
			return proof, err
		}
		if !siblingOK {
			return proof, errors.New("missing accumulator sibling node")
		}
		steps[path.Depth] = utreexo.ProofStep{HasSibling: true, SiblingHash: sibling.Hash}
		next, nextOK, err := s.accumulatorNode(*nextPath)
		if err != nil {
			return proof, err
		}
		if !nextOK {
			return proof, errors.New("missing accumulator child node")
		}
		if !accumulatorPathMatchesKey(*nextPath, queryKey) {
			terminal, err := s.firstAccumulatorLeaf(*nextPath)
			if err != nil {
				return proof, err
			}
			membership, err := s.UTXOAccumulatorProof(terminal.OutPoint)
			if err != nil {
				return proof, err
			}
			if !membership.Exists {
				return proof, errors.New("accumulator exclusion witness is not a member")
			}
			terminalCopy := *terminal
			proof.Terminal = &terminalCopy
			proof.Steps = membership.Steps
			return proof, nil
		}
		path = *nextPath
		node = next
	}
	if node.Leaf == nil {
		return proof, errors.New("invalid accumulator proof leaf")
	}
	if node.Leaf.OutPoint != outPoint {
		return proof, fmt.Errorf("accumulator proof leaf mismatch: got %x:%d", node.Leaf.OutPoint.TxID, node.Leaf.OutPoint.Vout)
	}
	proof.Exists = true
	proof.Type = node.Leaf.Type
	proof.ValueAtoms = node.Leaf.ValueAtoms
	proof.Payload32 = node.Leaf.Payload32
	proof.PubKey = node.Leaf.PubKey
	proof.Steps = steps
	return proof, nil
}

func (s *ChainStore) firstAccumulatorLeaf(path utreexo.AccumulatorNodePath) (*utreexo.UtxoLeaf, error) {
	for {
		node, ok, err := s.accumulatorNode(path)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("missing accumulator exclusion witness node")
		}
		if node.Leaf != nil {
			leaf := *node.Leaf
			return &leaf, nil
		}
		if node.LeftPath == nil || node.RightPath == nil || !validAccumulatorChildren(path, *node.LeftPath, *node.RightPath) {
			return nil, errors.New("invalid accumulator branch record")
		}
		path = *node.LeftPath
	}
}

func (s *ChainStore) WriteUTXOAccumulator(acc *utreexo.Accumulator) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := replaceAccumulatorIndexBatch(s.db, batch, acc); err != nil {
		return err
	}
	count := 0
	if acc != nil {
		count = acc.Count()
	}
	if err := batch.Set(metaUTXOCountKey, encodeU64(uint64(count)), nil); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) accumulatorNode(path utreexo.AccumulatorNodePath) (utreexo.AccumulatorNodeRecord, bool, error) {
	key := accumulatorNodeKey(path)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return utreexo.AccumulatorNodeRecord{}, false, nil
	}
	if err != nil {
		return utreexo.AccumulatorNodeRecord{}, false, err
	}
	defer closer.Close()
	record, err := decodeAccumulatorNodeValue(path, val)
	if err != nil {
		return utreexo.AccumulatorNodeRecord{}, false, err
	}
	return record, true, nil
}

func applyAccumulatorStateBatch(db *pebble.DB, batch *pebble.Batch, state *StoredChainStateMeta) error {
	if state == nil {
		return nil
	}
	switch {
	case state.UTXOAccumulatorDelta != nil:
		return applyAccumulatorDeltaBatch(batch, *state.UTXOAccumulatorDelta)
	case state.UTXOAccumulator != nil:
		return replaceAccumulatorIndexBatch(db, batch, state.UTXOAccumulator)
	default:
		return nil
	}
}

func (s *ChainStore) populateAccumulatorDeltaFromUTXODelta(state *StoredChainStateMeta, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	if state == nil || state.UTXOAccumulator != nil || state.UTXOAccumulatorDelta != nil {
		return nil
	}
	current, ok, err := s.LoadUTXOAccumulator()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	createdLeaves := make([]utreexo.UtxoLeaf, 0, len(created))
	for outPoint, entry := range created {
		createdLeaves = append(createdLeaves, consensus.UtxoLeafFromEntry(outPoint, entry))
	}
	next, err := current.Apply(spent, createdLeaves)
	if err != nil {
		return err
	}
	delta := utreexo.AccumulatorNodeDeltaBetween(current, next)
	state.UTXOAccumulatorDelta = &delta
	state.UTXOAccumulatorRoot = next.Root()
	state.UTXOCount = next.Count()
	return nil
}

func replaceAccumulatorIndexBatch(db *pebble.DB, batch *pebble.Batch, acc *utreexo.Accumulator) error {
	if err := deletePrefixBatch(db, batch, utxoAccumulatorNodePrefix, utxoAccumulatorNodePrefixEnd); err != nil {
		return err
	}
	if err := utreexo.ForEachAccumulatorNodeRecord(acc, func(record utreexo.AccumulatorNodeRecord) error {
		return batch.Set(accumulatorNodeKey(record.Path), encodeAccumulatorNodeValue(record), nil)
	}); err != nil {
		return err
	}
	if err := batch.Set(metaUTXOAccumulatorVersionKey, encodeU64(utxoAccumulatorIndexVersion), nil); err != nil {
		return err
	}
	if rootPath, ok := utreexo.AccumulatorRootPath(acc); ok {
		if err := batch.Set(metaUTXOAccumulatorRootPathKey, encodeAccumulatorPath(rootPath), nil); err != nil {
			return err
		}
	} else if err := batch.Delete(metaUTXOAccumulatorRootPathKey, nil); err != nil {
		return err
	}
	if acc == nil {
		root := utreexo.NewAccumulator().Root()
		return batch.Set(metaUTXOAccumulatorRootKey, root[:], nil)
	}
	root := acc.Root()
	return batch.Set(metaUTXOAccumulatorRootKey, root[:], nil)
}

func applyAccumulatorDeltaBatch(batch *pebble.Batch, delta utreexo.AccumulatorNodeDelta) error {
	for _, path := range delta.Deletes {
		if err := batch.Delete(accumulatorNodeKey(path), nil); err != nil {
			return err
		}
	}
	for _, record := range delta.Upserts {
		if err := batch.Set(accumulatorNodeKey(record.Path), encodeAccumulatorNodeValue(record), nil); err != nil {
			return err
		}
	}
	if delta.RootPath != nil {
		if err := batch.Set(metaUTXOAccumulatorRootPathKey, encodeAccumulatorPath(*delta.RootPath), nil); err != nil {
			return err
		}
	} else if err := batch.Delete(metaUTXOAccumulatorRootPathKey, nil); err != nil {
		return err
	}
	return batch.Set(metaUTXOAccumulatorVersionKey, encodeU64(utxoAccumulatorIndexVersion), nil)
}

func accumulatorNodeKey(path utreexo.AccumulatorNodePath) []byte {
	if path.Depth < 0 || path.Depth > utreexo.KeyBits {
		panic("invalid accumulator node path depth")
	}
	keyLen := (path.Depth + 7) / 8
	buf := make([]byte, 0, len(utxoAccumulatorNodePrefix)+2+keyLen)
	buf = append(buf, utxoAccumulatorNodePrefix...)
	buf = append(buf, byte(path.Depth>>8), byte(path.Depth))
	buf = append(buf, path.Key[:keyLen]...)
	return buf
}

func accumulatorChildPath(parent utreexo.AccumulatorNodePath, right bool) utreexo.AccumulatorNodePath {
	child := utreexo.AccumulatorNodePath{
		Depth: parent.Depth + 1,
		Key:   parent.Key,
	}
	if right {
		byteIndex := parent.Depth / 8
		bitOffset := 7 - (parent.Depth % 8)
		child.Key[byteIndex] |= 1 << bitOffset
	}
	return child
}

func accumulatorBitSet(key [utreexo.OutPointKeyBytes]byte, depth int) bool {
	byteIndex := depth / 8
	bitOffset := 7 - (depth % 8)
	return ((key[byteIndex] >> bitOffset) & 1) == 1
}

func decodeAccumulatorNodeKey(key []byte) (utreexo.AccumulatorNodePath, error) {
	if len(key) < len(utxoAccumulatorNodePrefix)+2 || !bytes.HasPrefix(key, utxoAccumulatorNodePrefix) {
		return utreexo.AccumulatorNodePath{}, errors.New("invalid accumulator node key")
	}
	raw := key[len(utxoAccumulatorNodePrefix):]
	depth := int(raw[0])<<8 | int(raw[1])
	if depth < 0 || depth > utreexo.KeyBits {
		return utreexo.AccumulatorNodePath{}, errors.New("invalid accumulator node depth")
	}
	keyLen := (depth + 7) / 8
	if len(raw) != 2+keyLen {
		return utreexo.AccumulatorNodePath{}, errors.New("invalid accumulator node path length")
	}
	if depth%8 != 0 && keyLen > 0 {
		unusedMask := byte((1 << (8 - depth%8)) - 1)
		if raw[2+keyLen-1]&unusedMask != 0 {
			return utreexo.AccumulatorNodePath{}, errors.New("non-canonical accumulator node path key")
		}
	}
	var pathKey [utreexo.OutPointKeyBytes]byte
	copy(pathKey[:], raw[2:])
	return utreexo.AccumulatorNodePath{Depth: depth, Key: pathKey}, nil
}

func encodeAccumulatorNodeValue(record utreexo.AccumulatorNodeRecord) []byte {
	buf := make([]byte, 0, 1+8+32+2*(2+utreexo.OutPointKeyBytes)+36+49)
	if record.Leaf != nil {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = append(buf, encodeU64(uint64(record.Count))...)
	buf = append(buf, record.Hash[:]...)
	if record.Leaf != nil {
		record.Leaf.OutPoint.Encode(&buf)
		entry := consensus.UtxoEntryFromLeaf(*record.Leaf)
		buf = append(buf, encodeCommittedCoin(entry)...)
	} else {
		if record.LeftPath == nil || record.RightPath == nil {
			panic("internal accumulator record is missing child paths")
		}
		buf = append(buf, encodeAccumulatorPath(*record.LeftPath)...)
		buf = append(buf, encodeAccumulatorPath(*record.RightPath)...)
	}
	return buf
}

func decodeAccumulatorNodeValue(path utreexo.AccumulatorNodePath, buf []byte) (utreexo.AccumulatorNodeRecord, error) {
	if len(buf) < 41 {
		return utreexo.AccumulatorNodeRecord{}, errors.New("invalid accumulator node value")
	}
	kind := buf[0]
	count, err := decodeU64(buf[1:9])
	if err != nil {
		return utreexo.AccumulatorNodeRecord{}, err
	}
	record := utreexo.AccumulatorNodeRecord{
		Path:  path,
		Count: int(count),
	}
	copy(record.Hash[:], buf[9:41])
	remaining := buf[41:]
	switch kind {
	case 0:
		left, consumed, err := decodeAccumulatorPathPrefix(remaining)
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, fmt.Errorf("invalid left accumulator child path: %w", err)
		}
		right, rightConsumed, err := decodeAccumulatorPathPrefix(remaining[consumed:])
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, fmt.Errorf("invalid right accumulator child path: %w", err)
		}
		if consumed+rightConsumed != len(remaining) {
			return utreexo.AccumulatorNodeRecord{}, errors.New("invalid internal accumulator node payload")
		}
		record.LeftPath = &left
		record.RightPath = &right
	case 1:
		if len(remaining) < 36 {
			return utreexo.AccumulatorNodeRecord{}, errors.New("invalid leaf accumulator node payload")
		}
		outPoint, err := decodeOutPoint(remaining[:36])
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, err
		}
		entry, err := decodeCommittedCoin(remaining[36:])
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, err
		}
		leaf := consensus.UtxoLeafFromEntry(outPoint, entry)
		record.Leaf = &leaf
	default:
		return utreexo.AccumulatorNodeRecord{}, errors.New("invalid accumulator node kind")
	}
	return record, nil
}

func encodeAccumulatorPath(path utreexo.AccumulatorNodePath) []byte {
	if path.Depth < 0 || path.Depth > utreexo.KeyBits {
		panic("invalid accumulator path depth")
	}
	keyLen := (path.Depth + 7) / 8
	buf := make([]byte, 2+keyLen)
	buf[0] = byte(path.Depth >> 8)
	buf[1] = byte(path.Depth)
	copy(buf[2:], path.Key[:keyLen])
	return buf
}

func decodeAccumulatorPath(buf []byte) (utreexo.AccumulatorNodePath, error) {
	path, consumed, err := decodeAccumulatorPathPrefix(buf)
	if err != nil {
		return utreexo.AccumulatorNodePath{}, err
	}
	if consumed != len(buf) {
		return utreexo.AccumulatorNodePath{}, errors.New("trailing accumulator path bytes")
	}
	return path, nil
}

func decodeAccumulatorPathPrefix(buf []byte) (utreexo.AccumulatorNodePath, int, error) {
	if len(buf) < 2 {
		return utreexo.AccumulatorNodePath{}, 0, errors.New("truncated accumulator path")
	}
	depth := int(buf[0])<<8 | int(buf[1])
	if depth < 0 || depth > utreexo.KeyBits {
		return utreexo.AccumulatorNodePath{}, 0, errors.New("invalid accumulator path depth")
	}
	keyLen := (depth + 7) / 8
	if len(buf) < 2+keyLen {
		return utreexo.AccumulatorNodePath{}, 0, errors.New("truncated accumulator path key")
	}
	var key [utreexo.OutPointKeyBytes]byte
	copy(key[:], buf[2:2+keyLen])
	if depth%8 != 0 && keyLen > 0 {
		unusedMask := byte((1 << (8 - depth%8)) - 1)
		if key[keyLen-1]&unusedMask != 0 {
			return utreexo.AccumulatorNodePath{}, 0, errors.New("non-canonical accumulator path key")
		}
	}
	return utreexo.AccumulatorNodePath{Depth: depth, Key: key}, 2 + keyLen, nil
}

func accumulatorPathMatchesKey(path utreexo.AccumulatorNodePath, key [utreexo.OutPointKeyBytes]byte) bool {
	fullBytes := path.Depth / 8
	if !bytes.Equal(path.Key[:fullBytes], key[:fullBytes]) {
		return false
	}
	remaining := path.Depth % 8
	if remaining == 0 {
		return true
	}
	mask := byte(0xff << (8 - remaining))
	return path.Key[fullBytes]&mask == key[fullBytes]&mask
}

func validAccumulatorChildren(parent, left, right utreexo.AccumulatorNodePath) bool {
	if parent.Depth < 0 || parent.Depth >= utreexo.KeyBits ||
		left.Depth <= parent.Depth || left.Depth > utreexo.KeyBits ||
		right.Depth <= parent.Depth || right.Depth > utreexo.KeyBits {
		return false
	}
	if !accumulatorPathMatchesKey(parent, left.Key) || !accumulatorPathMatchesKey(parent, right.Key) {
		return false
	}
	return !accumulatorBitSet(left.Key, parent.Depth) && accumulatorBitSet(right.Key, parent.Depth)
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
