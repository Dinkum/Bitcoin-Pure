package node

import (
	"encoding/hex"
	"encoding/json"
	"errors"
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
	blockSizeState.BlockSize = uint64(genesis.EncodedLen())
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
		blockSizeState = consensus.AdvanceBlockSizeState(blockSizeState, uint64(block.EncodedLen()), params)
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

// ImportUTXOSnapshotFastSync validates the complete supplied history before it
// makes snapshot-derived chainstate active. Snapshot import remains faster than
// ordinary sync persistence, but unverified bodies never cross the live-state
// trust boundary.
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
	material, undos, err := validateSnapshotHistory(profile, genesis, blocks, loaded)
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
	if err := verifySnapshotOrigins(loaded.UTXOs, genesis, blocks, loaded.Fixture.Height); err != nil {
		return SnapshotImportSummary{}, err
	}
	store, err := storage.OpenWithLogger(filepath.Clean(dbPath), logging.ComponentWith(logger, "storage"))
	if err != nil {
		return SnapshotImportSummary{}, err
	}
	defer store.Close()
	if err := ensureSnapshotImportTargetEmpty(store); err != nil {
		return SnapshotImportSummary{}, err
	}
	importBlocks := make([]types.Block, 0, len(material.Entries))
	importBlocks = append(importBlocks, *genesis)
	if material.Height > 0 {
		importBlocks = append(importBlocks, blocks[:material.Height]...)
	}
	if err := store.PutValidatedBlocksWithoutWalletIndex(importBlocks, material.Entries); err != nil {
		return SnapshotImportSummary{}, err
	}
	if err := store.WriteFullStateWithHeaderMetadata(&storage.StoredChainState{
		Profile:        profile,
		Height:         material.Height,
		TipHeader:      material.TipHeader,
		BlockSizeState: material.BlockSizeState,
		UTXOChecksum:   loaded.ComputedChecksum,
		UTXOs:          loaded.UTXOs,
	}, &material.HeaderState, material.Entries); err != nil {
		return SnapshotImportSummary{}, err
	}
	for height := uint64(1); height <= material.Height; height++ {
		entry := material.Entries[height]
		if err := store.PutValidatedBlockUndo(&entry, undos[height-1]); err != nil {
			return SnapshotImportSummary{}, err
		}
	}
	if _, err := store.RebuildWalletIndexesAtCurrentTip(); err != nil {
		return SnapshotImportSummary{}, err
	}
	logger.Info("imported historically validated snapshot state",
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

func validateSnapshotHistory(profile types.ChainProfile, genesis *types.Block, blocks []types.Block, loaded *LoadedUTXOSnapshotFixture) (SnapshotChainMaterial, [][]storage.BlockUndoEntry, error) {
	if loaded == nil || genesis == nil {
		return SnapshotChainMaterial{}, nil, errors.New("snapshot history and genesis are required")
	}
	if loaded.Fixture.Height > uint64(len(blocks)) {
		return SnapshotChainMaterial{}, nil, fmt.Errorf("snapshot height %d exceeds available blocks %d", loaded.Fixture.Height, len(blocks))
	}
	state := NewChainState(profile).WithLogger(logging.Component("snapshot-validation"))
	if _, err := state.InitializeFromGenesisBlock(genesis); err != nil {
		return SnapshotChainMaterial{}, nil, fmt.Errorf("validate snapshot genesis: %w", err)
	}
	undos := make([][]storage.BlockUndoEntry, 0, loaded.Fixture.Height)
	for height := uint64(1); height <= loaded.Fixture.Height; height++ {
		detail, err := state.applyBlockDetailed(&blocks[height-1])
		if err != nil {
			return SnapshotChainMaterial{}, nil, fmt.Errorf("validate snapshot block at height %d: %w", height, err)
		}
		undos = append(undos, []storage.BlockUndoEntry(detail.spentPreBlock))
	}
	view, ok := state.CommittedView()
	if !ok {
		return SnapshotChainMaterial{}, nil, ErrNoTip
	}
	if view.Height != loaded.Fixture.Height || view.TipHash != loaded.ExpectedHeaderHash {
		return SnapshotChainMaterial{}, nil, errors.New("validated snapshot history does not reach the declared anchor")
	}
	if view.UTXORoot != loaded.ComputedUTXORoot || view.UTXOChecksum != loaded.ComputedChecksum || view.UTXOCount != len(loaded.UTXOs) {
		return SnapshotChainMaterial{}, nil, errors.New("validated snapshot history does not produce the imported UTXO state")
	}
	if err := compareSnapshotUTXOs(state.UTXOs(), loaded.UTXOs); err != nil {
		return SnapshotChainMaterial{}, nil, fmt.Errorf("validated snapshot UTXOs: %w", err)
	}
	material, err := BuildSnapshotChainMaterial(profile, genesis, blocks, loaded.Fixture.Height)
	if err != nil {
		return SnapshotChainMaterial{}, nil, err
	}
	if material.BlockSizeState != view.BlockSizeState {
		return SnapshotChainMaterial{}, nil, errors.New("snapshot ABLA state differs from validated history")
	}
	return material, undos, nil
}

// verifySnapshotOrigins authenticates the non-committed maturity metadata
// against transaction bodies whose roots are committed by the header chain.
// It intentionally does not replace the later full historical validation.
func verifySnapshotOrigins(snapshot consensus.UtxoSet, genesis *types.Block, blocks []types.Block, height uint64) error {
	if genesis == nil || len(genesis.Txs) != 1 {
		return fmt.Errorf("snapshot origin verification requires a canonical genesis block")
	}
	verifyBodyRoots := func(block *types.Block, blockHeight uint64) error {
		if block == nil || len(block.Txs) == 0 {
			return fmt.Errorf("snapshot block at height %d has no transactions", blockHeight)
		}
		_, _, txRoot, authRoot := consensus.BuildBlockRoots(block.Txs)
		if txRoot != block.Header.MerkleTxIDRoot || authRoot != block.Header.MerkleAuthRoot {
			return fmt.Errorf("snapshot block body roots mismatch at height %d", blockHeight)
		}
		return nil
	}
	if err := verifyBodyRoots(genesis, 0); err != nil {
		return err
	}

	live := make(consensus.UtxoSet)
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	for vout, output := range genesis.Txs[0].Base.Outputs {
		outPoint := types.OutPoint{TxID: genesisTxID, Vout: uint32(vout)}
		live[outPoint] = consensus.UtxoEntryFromOutputAtHeight(output, 0, true)
	}
	type createdOutput struct {
		txIndex int
		entry   consensus.UtxoEntry
	}
	for blockHeight := uint64(1); blockHeight <= height; blockHeight++ {
		block := &blocks[blockHeight-1]
		if err := verifyBodyRoots(block, blockHeight); err != nil {
			return err
		}
		created := make(map[types.OutPoint]createdOutput)
		for txIndex := range block.Txs {
			txid := consensus.TxID(&block.Txs[txIndex])
			for vout, output := range block.Txs[txIndex].Base.Outputs {
				outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
				created[outPoint] = createdOutput{
					txIndex: txIndex,
					entry:   consensus.UtxoEntryFromOutputAtHeight(output, blockHeight, txIndex == 0),
				}
			}
		}
		claimed := make(map[types.OutPoint]struct{})
		for txIndex := 1; txIndex < len(block.Txs); txIndex++ {
			for _, input := range block.Txs[txIndex].Base.Inputs {
				if _, duplicate := claimed[input.PrevOut]; duplicate {
					return fmt.Errorf("duplicate snapshot block input at height %d: %v", blockHeight, input.PrevOut)
				}
				claimed[input.PrevOut] = struct{}{}
				if _, ok := live[input.PrevOut]; ok {
					delete(live, input.PrevOut)
					continue
				}
				output, ok := created[input.PrevOut]
				if !ok || output.txIndex == 0 || output.txIndex == txIndex {
					return fmt.Errorf("unresolvable snapshot block input at height %d: %v", blockHeight, input.PrevOut)
				}
				delete(created, input.PrevOut)
			}
		}
		for outPoint, output := range created {
			live[outPoint] = output.entry
		}
	}
	if err := compareSnapshotUTXOs(live, snapshot); err != nil {
		return fmt.Errorf("snapshot origin metadata mismatch: %w", err)
	}
	return nil
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
		detail, err := state.applyBlockDetailed(block)
		if err != nil {
			fastSyncState.LastError = err.Error()
			_ = store.UpdateFastSyncState(fastSyncState)
			return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("historical snapshot replay failed at height %d: %w", height, err)
		}
		undo := []storage.BlockUndoEntry(detail.spentPreBlock)
		entry, err := store.GetBlockIndexByHeight(height)
		if err != nil {
			return SnapshotHistoricalVerificationSummary{}, err
		}
		if entry == nil {
			return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("missing stored block index at height %d", height)
		}
		entry.Validated = true
		entry.BlockSizeState = state.BlockSizeState()
		if err := store.PutValidatedBlockUndo(entry, undo); err != nil {
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
	snapshotCount := state.UTXOCount()
	if snapshotCount != fastSyncState.SnapshotUTXOCount {
		return SnapshotHistoricalVerificationSummary{}, fmt.Errorf("historical snapshot count mismatch: expected %d, got %d", fastSyncState.SnapshotUTXOCount, snapshotCount)
	}
	retainedCount, err := countFastSyncSnapshotUTXOs(store.ForEachFastSyncSnapshotUTXO)
	if err != nil {
		return SnapshotHistoricalVerificationSummary{}, err
	}
	if retainedCount > 0 {
		snapshotCount, err = compareFastSyncSnapshotUTXOs(state.UTXOCount(), state.utxoLookup, fastSyncState.SnapshotUTXOCount, store.ForEachFastSyncSnapshotUTXO)
		if err != nil {
			return SnapshotHistoricalVerificationSummary{}, err
		}
	}
	if _, err := store.RebuildWalletIndexesAtCurrentTip(); err != nil {
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
		UTXOCount:  snapshotCount,
	}
	logger.Info("completed historical snapshot verification",
		slog.Uint64("height", summary.Height),
		slog.String("header_hash", fmt.Sprintf("%x", summary.HeaderHash)),
		slog.String("utxo_checksum", fmt.Sprintf("%x", summary.Checksum)),
		slog.Int("utxo_count", summary.UTXOCount),
	)
	return summary, nil
}

func compareFastSyncSnapshotUTXOs(liveCount int, liveLookup consensus.UtxoLookupWithErr, expectedSnapshotCount int, iterateSnapshot func(func(types.OutPoint, consensus.UtxoEntry) error) error) (int, error) {
	if liveLookup == nil {
		return 0, fmt.Errorf("live utxo lookup unavailable")
	}
	if iterateSnapshot == nil {
		return 0, fmt.Errorf("snapshot utxo iteration unavailable")
	}
	if liveCount != expectedSnapshotCount {
		return 0, fmt.Errorf("snapshot utxo set size mismatch: expected %d, got %d", expectedSnapshotCount, liveCount)
	}
	seen := 0
	if err := iterateSnapshot(func(outPoint types.OutPoint, expected consensus.UtxoEntry) error {
		got, ok, err := liveLookup(outPoint)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("live utxo set missing retained snapshot outpoint %x:%d", outPoint.TxID, outPoint.Vout)
		}
		if got != expected {
			return fmt.Errorf("snapshot entry mismatch for %x:%d", outPoint.TxID, outPoint.Vout)
		}
		seen++
		return nil
	}); err != nil {
		return 0, err
	}
	if seen == 0 {
		return 0, fmt.Errorf("missing retained fast-sync snapshot utxos")
	}
	if seen != expectedSnapshotCount {
		return 0, fmt.Errorf("snapshot utxo iteration count mismatch: expected %d, got %d", expectedSnapshotCount, seen)
	}
	return seen, nil
}

func countFastSyncSnapshotUTXOs(iterateSnapshot func(func(types.OutPoint, consensus.UtxoEntry) error) error) (int, error) {
	if iterateSnapshot == nil {
		return 0, nil
	}
	count := 0
	if err := iterateSnapshot(func(types.OutPoint, consensus.UtxoEntry) error {
		count++
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
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
			TxIDHex:       hex.EncodeToString(outPoint.TxID[:]),
			Vout:          outPoint.Vout,
			ValueAtoms:    entry.ValueAtoms,
			PubKeyHex:     hex.EncodeToString(entry.PubKey[:]),
			CreatedHeight: entry.CreatedHeight,
			Coinbase:      entry.Coinbase,
		})
	}
	return entries
}

func encodeSnapshotFixtureEntriesFromIterator(utxoCount int, iterate func(func(types.OutPoint, consensus.UtxoEntry) error) error) ([]UTXOSnapshotFixtureEntry, error) {
	type keyedEntry struct {
		outPoint types.OutPoint
		entry    consensus.UtxoEntry
	}
	entries := make([]keyedEntry, 0, utxoCount)
	if err := iterate(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		entries = append(entries, keyedEntry{outPoint: outPoint, entry: entry})
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return compareSnapshotOutPoints(entries[i].outPoint, entries[j].outPoint) < 0
	})
	fixtureEntries := make([]UTXOSnapshotFixtureEntry, 0, len(entries))
	for _, item := range entries {
		fixtureEntries = append(fixtureEntries, UTXOSnapshotFixtureEntry{
			TxIDHex:       hex.EncodeToString(item.outPoint.TxID[:]),
			Vout:          item.outPoint.Vout,
			ValueAtoms:    item.entry.ValueAtoms,
			PubKeyHex:     hex.EncodeToString(item.entry.PubKey[:]),
			CreatedHeight: item.entry.CreatedHeight,
			Coinbase:      item.entry.Coinbase,
		})
	}
	return fixtureEntries, nil
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
	if state, err := store.LoadFastSyncState(); err != nil {
		return err
	} else if state != nil {
		return fmt.Errorf("snapshot import target already has pending fast-sync state at height %d", state.SnapshotHeight)
	}
	return nil
}
