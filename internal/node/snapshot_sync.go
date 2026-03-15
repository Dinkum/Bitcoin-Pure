package node

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

var ErrHistoricalSnapshotVerificationPending = fmt.Errorf("historical snapshot verification still pending")

type SnapshotChainMaterial struct {
	Height         uint64
	TipHeader      types.BlockHeader
	TipHash        [32]byte
	BlockSizeState consensus.BlockSizeState
	Entries        []storage.BlockIndexEntry
	HeaderState    storage.StoredHeaderState
}

type SnapshotImportSummary struct {
	Height     uint64
	HeaderHash [32]byte
	UTXORoot   [32]byte
	Checksum   [32]byte
	UTXOCount  int
}

type SnapshotHistoricalVerificationSummary struct {
	Height     uint64
	HeaderHash [32]byte
	UTXORoot   [32]byte
	Checksum   [32]byte
	UTXOCount  int
}

// ExportUTXOSnapshotFile writes a deterministic snapshot fixture for the
// current committed chainstate so operators can move that state to a fresh
// node or archive it for offline verification.
func ExportUTXOSnapshotFile(path string, view CommittedChainView, iterate func(func(types.OutPoint, consensus.UtxoEntry) error) error, profile types.ChainProfile, genesisFixture string, chainFixture string) error {
	entries, err := encodeSnapshotFixtureEntriesFromIterator(view.UTXOCount, iterate)
	if err != nil {
		return err
	}
	fixture := UTXOSnapshotFixture{
		Version:               UTXOSnapshotFixtureVersion,
		Profile:               profile.String(),
		GenesisFixture:        genesisFixture,
		ChainFixture:          chainFixture,
		Height:                view.Height,
		ExpectedHeaderHashHex: hex.EncodeToString(view.TipHash[:]),
		ExpectedUTXORootHex:   hex.EncodeToString(view.UTXORoot[:]),
		ExpectedChecksumHex:   hex.EncodeToString(view.UTXOChecksum[:]),
		ExpectedUTXOCount:     view.UTXOCount,
		UTXOs:                 entries,
	}
	buf, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Clean(path), append(buf, '\n'), 0o644); err != nil {
		return err
	}
	return nil
}

// BuildSnapshotChainMaterial validates the supplied header chain and derives
// the active-chain metadata needed to import a snapshot into persistent
// storage without replaying the full UTXO history first.
func BuildSnapshotChainMaterial(profile types.ChainProfile, genesis *types.Block, blocks []types.Block, height uint64) (SnapshotChainMaterial, error) {
	if genesis == nil {
		return SnapshotChainMaterial{}, fmt.Errorf("genesis block is required")
	}
	if height > uint64(len(blocks)) {
		return SnapshotChainMaterial{}, fmt.Errorf("snapshot height %d exceeds available blocks %d", height, len(blocks))
	}
	params := consensus.ParamsForProfile(profile)
	headerChain := NewHeaderChain(profile)
	if err := headerChain.InitializeFromGenesisHeader(genesis.Header); err != nil {
		return SnapshotChainMaterial{}, err
	}
	genesisWork, err := consensus.BlockWork(genesis.Header.NBits)
	if err != nil {
		return SnapshotChainMaterial{}, err
	}
	blockSizeState := consensus.NewBlockSizeState(params)
	blockSizeState.BlockSize = uint64(len(genesis.Encode()))
	entries := make([]storage.BlockIndexEntry, 0, height+1)
	entries = append(entries, storage.BlockIndexEntry{
		Height:         0,
		ParentHash:     genesis.Header.PrevBlockHash,
		Header:         genesis.Header,
		ChainWork:      genesisWork,
		Validated:      true,
		BlockSizeState: blockSizeState,
	})
	tipHeader := genesis.Header
	tipHash := consensus.HeaderHash(&tipHeader)
	chainWork := genesisWork
	for i := uint64(0); i < height; i++ {
		block := blocks[i]
		if err := headerChain.ApplyHeader(&block.Header); err != nil {
			return SnapshotChainMaterial{}, fmt.Errorf("apply snapshot header at height %d: %w", i+1, err)
		}
		work, err := consensus.BlockWork(block.Header.NBits)
		if err != nil {
			return SnapshotChainMaterial{}, err
		}
		chainWork = consensus.AddChainWork(chainWork, work)
		blockSizeState = consensus.AdvanceBlockSizeState(blockSizeState, uint64(len(block.Encode())), params)
		entry := storage.BlockIndexEntry{
			Height:         i + 1,
			ParentHash:     block.Header.PrevBlockHash,
			Header:         block.Header,
			ChainWork:      chainWork,
			Validated:      true,
			BlockSizeState: blockSizeState,
		}
		entries = append(entries, entry)
		tipHeader = block.Header
		tipHash = consensus.HeaderHash(&tipHeader)
	}
	return SnapshotChainMaterial{
		Height:         height,
		TipHeader:      tipHeader,
		TipHash:        tipHash,
		BlockSizeState: blockSizeState,
		Entries:        entries,
		HeaderState: storage.StoredHeaderState{
			Profile:   profile,
			Height:    height,
			TipHeader: tipHeader,
		},
	}, nil
}

// ImportUTXOSnapshotFastSync seeds persistent chain state from a validated
// snapshot anchor immediately, then leaves a retained copy of the imported
// UTXO set for background historical replay to verify from genesis.
func ImportUTXOSnapshotFastSync(dbPath string, loaded *LoadedUTXOSnapshotFixture, genesis *types.Block, blocks []types.Block, logger *slog.Logger) (SnapshotImportSummary, error) {
	if loaded == nil {
		return SnapshotImportSummary{}, fmt.Errorf("snapshot fixture is required")
	}
	if logger == nil {
		logger = logging.Component("snapshot")
	}
	profile, err := types.ParseChainProfile(loaded.Fixture.Profile)
	if err != nil {
		return SnapshotImportSummary{}, err
	}
	material, err := BuildSnapshotChainMaterial(profile, genesis, blocks, loaded.Fixture.Height)
	if err != nil {
		return SnapshotImportSummary{}, err
	}
	if material.TipHash != loaded.ExpectedHeaderHash {
		return SnapshotImportSummary{}, fmt.Errorf("snapshot header hash mismatch: expected %x, got %x", loaded.ExpectedHeaderHash, material.TipHash)
	}
	if material.TipHeader.UTXORoot != loaded.ComputedUTXORoot {
		return SnapshotImportSummary{}, fmt.Errorf("snapshot root does not match imported tip header: expected %x, got %x", material.TipHeader.UTXORoot, loaded.ComputedUTXORoot)
	}
	if loaded.ExpectedChecksum != loaded.ComputedChecksum {
		return SnapshotImportSummary{}, fmt.Errorf("snapshot checksum mismatch: expected %x, got %x", loaded.ExpectedChecksum, loaded.ComputedChecksum)
	}
	store, err := storage.OpenWithLogger(filepath.Clean(dbPath), logging.ComponentWith(logger, "storage"))
	if err != nil {
		return SnapshotImportSummary{}, err
	}
	defer store.Close()
	if err := ensureSnapshotImportTargetEmpty(store); err != nil {
		return SnapshotImportSummary{}, err
	}
	if err := store.WriteFullState(&storage.StoredChainState{
		Profile:        profile,
		Height:         material.Height,
		TipHeader:      material.TipHeader,
		BlockSizeState: material.BlockSizeState,
		UTXOChecksum:   loaded.ComputedChecksum,
		UTXOs:          cloneUtxos(loaded.UTXOs),
	}); err != nil {
		return SnapshotImportSummary{}, err
	}
	if err := store.WriteHeaderState(&material.HeaderState); err != nil {
		return SnapshotImportSummary{}, err
	}
	if err := store.PutValidatedBlock(genesis, &material.Entries[0], nil); err != nil {
		return SnapshotImportSummary{}, err
	}
	for i := uint64(1); i <= material.Height; i++ {
		if err := store.PutValidatedBlock(&blocks[i-1], &material.Entries[i], nil); err != nil {
			return SnapshotImportSummary{}, err
		}
	}
	if err := store.RewriteActiveHeights(0, 0, material.Entries); err != nil {
		return SnapshotImportSummary{}, err
	}
	if err := store.RewriteActiveHeaderHeights(0, 0, material.Entries); err != nil {
		return SnapshotImportSummary{}, err
	}
	if err := store.WriteFastSyncState(&storage.FastSyncState{
		SnapshotHeight:     material.Height,
		SnapshotHeaderHash: material.TipHash,
		SnapshotUTXORoot:   loaded.ComputedUTXORoot,
		SnapshotChecksum:   loaded.ComputedChecksum,
		SnapshotUTXOCount:  len(loaded.UTXOs),
	}, cloneUtxos(loaded.UTXOs)); err != nil {
		return SnapshotImportSummary{}, err
	}
	logger.Info("imported snapshot fast-sync state",
		slog.String("db_path", filepath.Clean(dbPath)),
		slog.Uint64("height", material.Height),
		slog.String("header_hash", fmt.Sprintf("%x", material.TipHash)),
		slog.String("utxo_checksum", fmt.Sprintf("%x", loaded.ComputedChecksum)),
		slog.Int("utxo_count", len(loaded.UTXOs)),
	)
	return SnapshotImportSummary{
		Height:     material.Height,
		HeaderHash: material.TipHash,
		UTXORoot:   loaded.ComputedUTXORoot,
		Checksum:   loaded.ComputedChecksum,
		UTXOCount:  len(loaded.UTXOs),
	}, nil
}

// VerifyFastSyncSnapshotFromStore replays the stored active chain from genesis
// and backfills undo data for imported blocks while proving that the imported
// snapshot state was historically correct.
func VerifyFastSyncSnapshotFromStore(store *storage.ChainStore, profile types.ChainProfile, genesis *types.Block, logger *slog.Logger) (SnapshotHistoricalVerificationSummary, error) {
	if store == nil {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("chain store is required")
	}
	if genesis == nil {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("genesis block is required")
	}
	if logger == nil {
		logger = logging.Component("snapshot")
	}
	fastSyncState, err := store.LoadFastSyncState()
	if err != nil {
		return SnapshotHistoricalVerificationSummary{}, err
	}
	if fastSyncState == nil {
		return SnapshotHistoricalVerificationSummary{}, nil
	}
	snapshotUTXOs, err := store.LoadFastSyncSnapshotUTXOs()
	if err != nil {
		return SnapshotHistoricalVerificationSummary{}, err
	}
	if snapshotUTXOs == nil {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("missing retained fast-sync snapshot utxos")
	}
	state := NewChainState(profile).WithLogger(logger)
	if _, err := state.InitializeFromGenesisBlock(genesis); err != nil {
		return SnapshotHistoricalVerificationSummary{}, err
	}
	for height := uint64(1); height <= fastSyncState.SnapshotHeight; height++ {
		block, err := store.GetBlockByHeight(height)
		if err != nil {
			return SnapshotHistoricalVerificationSummary{}, err
		}
		if block == nil {
			return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("missing stored block at height %d", height)
		}
		undo, err := captureUndoEntries(block, state.utxoLookup)
		if err != nil {
			return SnapshotHistoricalVerificationSummary{}, err
		}
		if _, err := state.ApplyBlock(block); err != nil {
			fastSyncState.LastError = err.Error()
			_ = store.UpdateFastSyncState(fastSyncState)
			return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("historical snapshot replay failed at height %d: %w", height, err)
		}
		entry, err := store.GetBlockIndexByHeight(height)
		if err != nil {
			return SnapshotHistoricalVerificationSummary{}, err
		}
		if entry == nil {
			return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("missing stored block index at height %d", height)
		}
		entry.Validated = true
		entry.BlockSizeState = state.BlockSizeState()
		if err := store.PutValidatedBlock(block, entry, undo); err != nil {
			return SnapshotHistoricalVerificationSummary{}, err
		}
	}
	if tip := state.TipHeight(); tip == nil || *tip != fastSyncState.SnapshotHeight {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("historical snapshot replay stopped at unexpected height")
	}
	if hash := consensus.HeaderHash(state.TipHeader()); hash != fastSyncState.SnapshotHeaderHash {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("historical snapshot header hash mismatch: expected %x, got %x", fastSyncState.SnapshotHeaderHash, hash)
	}
	if state.UTXORoot() != fastSyncState.SnapshotUTXORoot {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("historical snapshot root mismatch: expected %x, got %x", fastSyncState.SnapshotUTXORoot, state.UTXORoot())
	}
	if state.UTXOChecksum() != fastSyncState.SnapshotChecksum {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("historical snapshot checksum mismatch: expected %x, got %x", fastSyncState.SnapshotChecksum, state.UTXOChecksum())
	}
	if err := compareSnapshotUTXOsIter(state.UTXOCount(), state.ForEachUTXO, snapshotUTXOs); err != nil {
		return SnapshotHistoricalVerificationSummary{}, err
	}
	if err := store.ClearFastSyncState(); err != nil {
		return SnapshotHistoricalVerificationSummary{}, err
	}
	summary := SnapshotHistoricalVerificationSummary{
		Height:     fastSyncState.SnapshotHeight,
		HeaderHash: fastSyncState.SnapshotHeaderHash,
		UTXORoot:   fastSyncState.SnapshotUTXORoot,
		Checksum:   fastSyncState.SnapshotChecksum,
		UTXOCount:  len(snapshotUTXOs),
	}
	logger.Info("completed historical snapshot verification",
		slog.Uint64("height", summary.Height),
		slog.String("header_hash", fmt.Sprintf("%x", summary.HeaderHash)),
		slog.String("utxo_checksum", fmt.Sprintf("%x", summary.Checksum)),
		slog.Int("utxo_count", summary.UTXOCount),
	)
	return summary, nil
}

func encodeSnapshotFixtureEntries(utxos consensus.UtxoSet) []UTXOSnapshotFixtureEntry {
	ordered := make([]types.OutPoint, 0, len(utxos))
	for outPoint := range utxos {
		ordered = append(ordered, outPoint)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return compareSnapshotOutPoints(ordered[i], ordered[j]) < 0
	})
	entries := make([]UTXOSnapshotFixtureEntry, 0, len(ordered))
	for _, outPoint := range ordered {
		entry := utxos[outPoint]
		entries = append(entries, UTXOSnapshotFixtureEntry{
			TxIDHex:    hex.EncodeToString(outPoint.TxID[:]),
			Vout:       outPoint.Vout,
			ValueAtoms: entry.ValueAtoms,
			PubKeyHex:  hex.EncodeToString(entry.PubKey[:]),
		})
	}
	return entries
}

func encodeSnapshotFixtureEntriesFromIterator(utxoCount int, iterate func(func(types.OutPoint, consensus.UtxoEntry) error) error) ([]UTXOSnapshotFixtureEntry, error) {
	utxos := make(consensus.UtxoSet, utxoCount)
	if err := iterate(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		utxos[outPoint] = entry
		return nil
	}); err != nil {
		return nil, err
	}
	return encodeSnapshotFixtureEntries(utxos), nil
}

func ensureSnapshotImportTargetEmpty(store *storage.ChainStore) error {
	if stored, err := store.LoadChainStateMeta(); err != nil {
		return err
	} else if stored != nil {
		return fmt.Errorf("snapshot import target already has chain state at height %d", stored.Height)
	}
	if stored, err := store.LoadHeaderState(); err != nil {
		return err
	} else if stored != nil {
		return fmt.Errorf("snapshot import target already has header state at height %d", stored.Height)
	}
	if hash, err := store.GetBlockHashByHeight(0); err != nil {
		return err
	} else if hash != nil {
		return fmt.Errorf("snapshot import target already has indexed blocks")
	}
	return nil
}
