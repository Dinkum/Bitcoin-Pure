package node

import (
	"path/filepath"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
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
		UTXOs:        cloneUtxos(loaded.UTXOs),
		TipHeader:    types.BlockHeader{UTXORoot: loaded.ExpectedUTXORoot},
	}
	if err := ExportUTXOSnapshotFile(path, view, types.Regtest, "fixtures/genesis/regtest.json", "fixtures/chains/regtest_bootstrap.json"); err != nil {
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
	if fastSyncState == nil || fastSyncState.SnapshotHeight != loaded.Fixture.Height {
		t.Fatalf("fast sync state = %+v, want height %d", fastSyncState, loaded.Fixture.Height)
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
	if summary.Height != loaded.Fixture.Height {
		t.Fatalf("verification height = %d, want %d", summary.Height, loaded.Fixture.Height)
	}
	fastSyncState, err := store.LoadFastSyncState()
	if err != nil {
		t.Fatalf("LoadFastSyncState after verify: %v", err)
	}
	if fastSyncState != nil {
		t.Fatalf("expected fast sync state to clear, got %+v", fastSyncState)
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
