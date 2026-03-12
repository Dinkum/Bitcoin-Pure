package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestVerifyUTXOSnapshotFixtureMatchesGenesis(t *testing.T) {
	loaded, err := LoadUTXOSnapshotFixture("../../fixtures/snapshots/regtest_genesis.json")
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	genesis, err := loadSnapshotTestGenesis(loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture))
	if err != nil {
		t.Fatalf("loadSnapshotTestGenesis: %v", err)
	}
	summary, err := VerifyUTXOSnapshotFixture(loaded, &genesis, nil)
	if err != nil {
		t.Fatalf("VerifyUTXOSnapshotFixture: %v", err)
	}
	if summary.Height != 0 {
		t.Fatalf("snapshot height = %d, want 0", summary.Height)
	}
	if summary.UTXORoot != loaded.ExpectedUTXORoot {
		t.Fatalf("snapshot utxo_root = %x, want %x", summary.UTXORoot, loaded.ExpectedUTXORoot)
	}
}

func TestVerifyUTXOSnapshotFixtureMatchesBootstrapTip(t *testing.T) {
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
	summary, err := VerifyUTXOSnapshotFixture(loaded, &genesis, blocks)
	if err != nil {
		t.Fatalf("VerifyUTXOSnapshotFixture: %v", err)
	}
	if summary.Height != loaded.Fixture.Height {
		t.Fatalf("snapshot height = %d, want %d", summary.Height, loaded.Fixture.Height)
	}
	if summary.UTXOCount != loaded.Fixture.ExpectedUTXOCount {
		t.Fatalf("snapshot utxo count = %d, want %d", summary.UTXOCount, loaded.Fixture.ExpectedUTXOCount)
	}
}

func TestVerifyUTXOSnapshotAtHeightRejectsMismatchedSet(t *testing.T) {
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
	bad := cloneUtxos(loaded.UTXOs)
	for outPoint := range bad {
		delete(bad, outPoint)
		break
	}
	if _, err := VerifyUTXOSnapshotAtHeight(types.Regtest, &genesis, blocks, loaded.Fixture.Height, bad); err == nil {
		t.Fatal("expected mismatched snapshot set to fail")
	}
}

func loadSnapshotTestGenesis(path string) (types.Block, error) {
	var fixture struct {
		BlockHex string `json:"block_hex"`
	}
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return types.Block{}, err
	}
	if err := json.Unmarshal(buf, &fixture); err != nil {
		return types.Block{}, err
	}
	return consensus.DecodeBlockHex(fixture.BlockHex, types.DefaultCodecLimits())
}

func loadSnapshotTestBlocks(path string) ([]types.Block, error) {
	var fixture struct {
		Blocks []string `json:"blocks"`
	}
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(buf, &fixture); err != nil {
		return nil, err
	}
	blocks := make([]types.Block, 0, len(fixture.Blocks))
	for _, raw := range fixture.Blocks {
		block, err := consensus.DecodeBlockHex(raw, types.DefaultCodecLimits())
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}
