package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/storage"
)

func TestRunSnapshotRoot(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	loaded, err := node.LoadUTXOSnapshotFixture(fixturePath)
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	output := captureStdout(t, func() {
		if err := runSnapshot([]string{"root", "--file", fixturePath}); err != nil {
			t.Fatalf("runSnapshot root: %v", err)
		}
	})
	if !strings.Contains(output, fmt.Sprintf("utxo_root: %x", loaded.ExpectedUTXORoot)) {
		t.Fatalf("snapshot root output missing expected root:\n%s", output)
	}
	if !strings.Contains(output, fmt.Sprintf("utxo_checksum: %x", loaded.ExpectedChecksum)) {
		t.Fatalf("snapshot root output missing expected checksum:\n%s", output)
	}
}

func TestRunSnapshotVerify(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	loaded, err := node.LoadUTXOSnapshotFixture(fixturePath)
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	output := captureStdout(t, func() {
		if err := runSnapshot([]string{"verify", "--file", fixturePath}); err != nil {
			t.Fatalf("runSnapshot verify: %v", err)
		}
	})
	if !strings.Contains(output, "height: 2") {
		t.Fatalf("snapshot verify output missing height:\n%s", output)
	}
	if !strings.Contains(output, "utxo_count: 3") {
		t.Fatalf("snapshot verify output missing utxo count:\n%s", output)
	}
	if !strings.Contains(output, fmt.Sprintf("utxo_checksum: %x", loaded.ExpectedChecksum)) {
		t.Fatalf("snapshot verify output missing checksum:\n%s", output)
	}
}

func TestRunSnapshotVerifyRejectsMismatchedExpectation(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	buf, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	loaded, err := node.LoadUTXOSnapshotFixture(fixturePath)
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	bad := strings.Replace(string(buf), fmt.Sprintf("%x", loaded.ExpectedUTXORoot), strings.Repeat("0", 64), 1)
	path := t.TempDir() + "/bad_snapshot.json"
	if err := os.WriteFile(path, []byte(bad), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := runSnapshot([]string{"verify", "--file", path}); err == nil {
		t.Fatal("expected mismatched snapshot verify to fail")
	}
}

func TestRunSnapshotExport(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	loaded, err := node.LoadUTXOSnapshotFixture(fixturePath)
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture: %v", err)
	}
	genesisPath := loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture)
	loadedGenesis, err := loadGenesisFixtureFromPath(genesisPath)
	if err != nil {
		t.Fatalf("loadGenesisFixtureFromPath: %v", err)
	}
	chainPath := loaded.ResolveReferencePath(loaded.Fixture.ChainFixture)
	blocks, err := loadValidatedChainFixtureBlocks(chainPath)
	if err != nil {
		t.Fatalf("loadValidatedChainFixtureBlocks: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "chain")
	if _, err := node.ImportUTXOSnapshotFastSync(dbPath, loaded, &loadedGenesis.Block, blocks, nil); err != nil {
		t.Fatalf("ImportUTXOSnapshotFastSync: %v", err)
	}
	outPath := filepath.Join(t.TempDir(), "exported.json")
	output := captureStdout(t, func() {
		if err := runSnapshot([]string{"export", "--db", dbPath, "--out", outPath}); err != nil {
			t.Fatalf("runSnapshot export: %v", err)
		}
	})
	if !strings.Contains(output, "utxo_count: 3") {
		t.Fatalf("snapshot export output missing utxo count:\n%s", output)
	}
	exported, err := node.LoadUTXOSnapshotFixture(outPath)
	if err != nil {
		t.Fatalf("LoadUTXOSnapshotFixture(exported): %v", err)
	}
	if exported.ExpectedUTXORoot != loaded.ExpectedUTXORoot {
		t.Fatalf("exported root = %x, want %x", exported.ExpectedUTXORoot, loaded.ExpectedUTXORoot)
	}
	if exported.ExpectedChecksum != loaded.ExpectedChecksum {
		t.Fatalf("exported checksum = %x, want %x", exported.ExpectedChecksum, loaded.ExpectedChecksum)
	}
}

func TestRunSnapshotImport(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	dbPath := filepath.Join(t.TempDir(), "imported-chain")
	output := captureStdout(t, func() {
		if err := runSnapshot([]string{"import", "--db", dbPath, "--file", fixturePath}); err != nil {
			t.Fatalf("runSnapshot import: %v", err)
		}
	})
	if !strings.Contains(output, "height: 2") {
		t.Fatalf("snapshot import output missing height:\n%s", output)
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
	if stored == nil || stored.Height != 2 {
		t.Fatalf("stored chain state = %+v, want height 2", stored)
	}
	if got := consensus.ComputedUTXORoot(stored.UTXOs); got == ([32]byte{}) {
		t.Fatal("expected imported utxo root")
	}
	if stored.UTXOChecksum == ([32]byte{}) {
		t.Fatal("expected imported utxo checksum")
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = old
	}()
	fn()
	if err := w.Close(); err != nil {
		t.Fatalf("Close stdout writer: %v", err)
	}
	buf, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return string(buf)
}
