package node

import (
	"path/filepath"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utxochecksum"
)

func TestExportUTXOSnapshotFileRoundTrip(t *testing.T) {
	loaded, err := LoadUTXOSnapshotFixture("../../fixtures/snapshots/regtest_bootstrap_tip.json")
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	view := CommittedChainView{
		Height:       loaded.Fixture.Height,
		TipHash:      loaded.ExpectedHeaderHash,
		UTXORoot:     loaded.ExpectedUTXORoot,
		UTXOChecksum: loaded.ExpectedChecksum,
		UTXOCount:    len(loaded.UTXOs),
		TipHeader:    types.BlockHeader{UTXORoot: loaded.ExpectedUTXORoot},
	}
	if err := ExportUTXOSnapshotFile(path, view, func(fn func(types.OutPoint, consensus.UtxoEntry) error) error {
		for outPoint, entry := range loaded.UTXOs {
			if err := fn(outPoint, entry); err != nil {
				return err
			}
		}
		return nil
	}, types.Regtest, "fixtures/genesis/regtest.json", "fixtures/chains/regtest_bootstrap.json"); err != nil {
		t.Fatalf("ExportUTXOSnapshotFile: %v", err)
	}
	exported, err := LoadUTXOSnapshotFixture(path)
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture(exported): %v", err)
	}
	if exported.Fixture.Height != loaded.Fixture.Height {
		t.Fatalf("exported height = %d, want %d", exported.Fixture.Height, loaded.Fixture.Height)
	}
	if exported.ExpectedHeaderHash != loaded.ExpectedHeaderHash {
		t.Fatalf("exported header hash = %x, want %x", exported.ExpectedHeaderHash, loaded.ExpectedHeaderHash)
	}
	if exported.ExpectedUTXORoot != loaded.ExpectedUTXORoot {
		t.Fatalf("exported root = %x, want %x", exported.ExpectedUTXORoot, loaded.ExpectedUTXORoot)
	}
	if exported.ExpectedChecksum != loaded.ExpectedChecksum {
		t.Fatalf("exported checksum = %x, want %x", exported.ExpectedChecksum, loaded.ExpectedChecksum)
	}
	if err := compareSnapshotUTXOs(exported.UTXOs, loaded.UTXOs); err != nil {
		t.Fatalf("exported snapshot mismatch: %v", err)
	}
}

func TestImportUTXOSnapshotFastSyncStoresChainState(t *testing.T) {
	loaded, err := LoadUTXOSnapshotFixture("../../fixtures/snapshots/regtest_bootstrap_tip.json")
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	genesis, err := loadSnapshotTestGenesis(loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture))
	if err != nil {
		t.Fatalf("loadSnapshotTestGenesis: %v", err)
	}
	blocks, err := loadSnapshotTestBlocks(loaded.ResolveReferencePath(loaded.Fixture.ChainFixture))
	if err != nil {
		t.Fatalf("loadSnapshotTestBlocks: %v", err)
	}
	dbPath := t.TempDir()
	summary, err := ImportUTXOSnapshotFastSync(dbPath, loaded, &genesis, blocks, nil)
	if err != nil {
		t.Fatalf("ImportUTXOSnapshotFastSync: %v", err)
	}
	if summary.Height != loaded.Fixture.Height {
		t.Fatalf("imported height = %d, want %d", summary.Height, loaded.Fixture.Height)
	}
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()
	stored, err := store.LoadChainState()
	if err != nil {
		t.Fatalf("LoadChainState: %v", err)
	}
	if stored == nil || stored.Height != loaded.Fixture.Height {
		t.Fatalf("stored chain height = %+v, want %d", stored, loaded.Fixture.Height)
	}
	if got := consensus.ComputedUTXORoot(stored.UTXOs); got != loaded.ExpectedUTXORoot {
		t.Fatalf("stored root = %x, want %x", got, loaded.ExpectedUTXORoot)
	}
	if stored.UTXOChecksum != loaded.ExpectedChecksum {
		t.Fatalf("stored checksum = %x, want %x", stored.UTXOChecksum, loaded.ExpectedChecksum)
	}
	fastSyncState, err := store.LoadFastSyncState()
	if err != nil {
		t.Fatalf("LoadFastSyncState: %v", err)
	}
	if fastSyncState != nil {
		t.Fatalf("validated import left pending fast sync state: %+v", fastSyncState)
	}
	retained, err := store.LoadFastSyncSnapshotUTXOs()
	if err != nil {
		t.Fatalf("LoadFastSyncSnapshotUTXOs: %v", err)
	}
	if len(retained) != 0 {
		t.Fatalf("retained fast-sync snapshot utxos = %d, want metadata-only state", len(retained))
	}
	walletIndexHeight, err := store.WalletIndexHeight()
	if err != nil {
		t.Fatalf("WalletIndexHeight after import: %v", err)
	}
	if walletIndexHeight == nil || *walletIndexHeight != loaded.Fixture.Height {
		t.Fatalf("wallet index height after import = %v, want %d", walletIndexHeight, loaded.Fixture.Height)
	}
}

func TestVerifyFastSyncSnapshotFromStoreClearsTrustState(t *testing.T) {
	loaded, err := LoadUTXOSnapshotFixture("../../fixtures/snapshots/regtest_bootstrap_tip.json")
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	genesis, err := loadSnapshotTestGenesis(loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture))
	if err != nil {
		t.Fatalf("loadSnapshotTestGenesis: %v", err)
	}
	blocks, err := loadSnapshotTestBlocks(loaded.ResolveReferencePath(loaded.Fixture.ChainFixture))
	if err != nil {
		t.Fatalf("loadSnapshotTestBlocks: %v", err)
	}
	dbPath := t.TempDir()
	if _, err := ImportUTXOSnapshotFastSync(dbPath, loaded, &genesis, blocks, nil); err != nil {
		t.Fatalf("ImportUTXOSnapshotFastSync: %v", err)
	}
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()
	summary, err := VerifyFastSyncSnapshotFromStore(store, types.Regtest, &genesis, nil)
	if err != nil {
		t.Fatalf("VerifyFastSyncSnapshotFromStore: %v", err)
	}
	if summary != (SnapshotHistoricalVerificationSummary{}) {
		t.Fatalf("already validated import returned verification work: %+v", summary)
	}
	fastSyncState, err := store.LoadFastSyncState()
	if err != nil {
		t.Fatalf("LoadFastSyncState after verify: %v", err)
	}
	if fastSyncState != nil {
		t.Fatalf("expected fast sync state to clear, got %+v", fastSyncState)
	}
	walletIndexHeight, err := store.WalletIndexHeight()
	if err != nil {
		t.Fatalf("WalletIndexHeight after verify: %v", err)
	}
	if walletIndexHeight == nil || *walletIndexHeight != loaded.Fixture.Height {
		t.Fatalf("wallet index height after verify = %v, want %d", walletIndexHeight, loaded.Fixture.Height)
	}
	entry, err := store.GetBlockIndexByHeight(2)
	if err != nil {
		t.Fatalf("GetBlockIndexByHeight(2): %v", err)
	}
	if entry == nil {
		t.Fatal("expected indexed block entry at height 2")
	}
	if !entry.Validated {
		t.Fatal("expected historical verification to leave height 2 validated")
	}
}

func TestImportUTXOSnapshotRejectsBodyPoisonBeforeActivation(t *testing.T) {
	loaded, err := LoadUTXOSnapshotFixture("../../fixtures/snapshots/regtest_bootstrap_tip.json")
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := loadSnapshotTestGenesis(loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture))
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := loadSnapshotTestBlocks(loaded.ResolveReferencePath(loaded.Fixture.ChainFixture))
	if err != nil {
		t.Fatal(err)
	}
	blocks[0].Txs = append(blocks[0].Txs, types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{1}}}},
			Outputs: []types.TxOutput{types.NewXOnlyOutput(1, [32]byte{2})},
		},
		Auth: types.TxAuth{Entries: []types.TxAuthEntry{{AuthPayload: make([]byte, 1024)}}},
	})
	dbPath := t.TempDir()
	if _, err := ImportUTXOSnapshotFastSync(dbPath, loaded, &genesis, blocks, nil); err == nil {
		t.Fatal("body-poisoned snapshot import succeeded")
	}
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state, err := store.LoadChainStateMeta()
	if err != nil {
		t.Fatal(err)
	}
	if state != nil {
		t.Fatalf("rejected snapshot activated chain state: %+v", state)
	}
}

func TestImportUTXOSnapshotRejectsOriginTamperBeforeActivation(t *testing.T) {
	loaded, err := LoadUTXOSnapshotFixture("../../fixtures/snapshots/regtest_bootstrap_tip.json")
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := loadSnapshotTestGenesis(loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture))
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := loadSnapshotTestBlocks(loaded.ResolveReferencePath(loaded.Fixture.ChainFixture))
	if err != nil {
		t.Fatal(err)
	}
	originalRoot := consensus.ComputedUTXORoot(loaded.UTXOs)
	originalChecksum := loaded.ComputedChecksum
	for outPoint, entry := range loaded.UTXOs {
		entry.Coinbase = !entry.Coinbase
		entry.CreatedHeight++
		loaded.UTXOs[outPoint] = entry
		break
	}
	// Origin is deliberately outside both commitments, so successful rejection
	// proves import authenticates it from validated block history.
	if got := consensus.ComputedUTXORoot(loaded.UTXOs); got != originalRoot {
		t.Fatalf("origin tamper changed UTXO root: got %x want %x", got, originalRoot)
	}
	if got := utxochecksum.Compute(loaded.UTXOs); got != originalChecksum {
		t.Fatalf("origin tamper changed UTXO checksum: got %x want %x", got, originalChecksum)
	}

	dbPath := t.TempDir()
	if _, err := ImportUTXOSnapshotFastSync(dbPath, loaded, &genesis, blocks, nil); err == nil {
		t.Fatal("origin-tampered snapshot import succeeded")
	}
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state, err := store.LoadChainStateMeta()
	if err != nil {
		t.Fatal(err)
	}
	if state != nil {
		t.Fatalf("rejected origin-tampered snapshot activated chain state: %+v", state)
	}
}

func TestOpenPersistentChainStateRejectsLegacyPendingFastSync(t *testing.T) {
	dbPath := t.TempDir()
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteFastSyncStateMetadata(&storage.FastSyncState{SnapshotHeight: 42}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := OpenPersistentChainState(dbPath, types.Regtest)
	if err == nil {
		state.Close()
		t.Fatal("legacy pending fast-sync state was activated")
	}
}
