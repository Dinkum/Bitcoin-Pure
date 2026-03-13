package main

import (
	"flag"
	"fmt"
	"os"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

func runSnapshot(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing snapshot subcommand")
	}
	switch args[0] {
	case "root":
		return runSnapshotRoot(args[1:])
	case "verify":
		return runSnapshotVerify(args[1:])
	case "export":
		return runSnapshotExport(args[1:])
	case "import":
		return runSnapshotImport(args[1:])
	default:
		return fmt.Errorf("unknown snapshot subcommand")
	}
}

func runSnapshotRoot(args []string) error {
	fs := newFlagSet("snapshot root")
	file := fs.String("file", "fixtures/snapshots/regtest_bootstrap_tip.json", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	loaded, err := node.LoadUTXOSnapshotFixture(*file)
	if err != nil {
		return err
	}
	fmt.Printf("fixture: %s\n", *file)
	fmt.Printf("profile: %s\n", loaded.Fixture.Profile)
	fmt.Printf("height: %d\n", loaded.Fixture.Height)
	fmt.Printf("header_hash: %x\n", loaded.ExpectedHeaderHash)
	fmt.Printf("utxo_root: %x\n", loaded.ComputedUTXORoot)
	fmt.Printf("utxo_checksum: %x\n", loaded.ComputedChecksum)
	fmt.Printf("utxo_count: %d\n", len(loaded.UTXOs))
	return nil
}

func runSnapshotVerify(args []string) error {
	fs := newFlagSet("snapshot verify")
	file := fs.String("file", "fixtures/snapshots/regtest_bootstrap_tip.json", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	loaded, err := node.LoadUTXOSnapshotFixture(*file)
	if err != nil {
		return err
	}
	genesisPath := loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture)
	loadedGenesis, err := loadGenesisFixtureFromPath(genesisPath)
	if err != nil {
		return err
	}
	var blocks []types.Block
	if loaded.Fixture.Height > 0 {
		if loaded.Fixture.ChainFixture == "" {
			return fmt.Errorf("snapshot height %d requires chain_fixture", loaded.Fixture.Height)
		}
		chainPath := loaded.ResolveReferencePath(loaded.Fixture.ChainFixture)
		blocks, err = loadValidatedChainFixtureBlocks(chainPath)
		if err != nil {
			return err
		}
	}
	summary, err := node.VerifyUTXOSnapshotFixture(loaded, &loadedGenesis.Block, blocks)
	if err != nil {
		return err
	}
	fmt.Printf("fixture: %s\n", *file)
	fmt.Printf("profile: %s\n", loaded.Fixture.Profile)
	fmt.Printf("height: %d\n", summary.Height)
	fmt.Printf("header_hash: %x\n", summary.HeaderHash)
	fmt.Printf("utxo_root: %x\n", summary.UTXORoot)
	fmt.Printf("utxo_checksum: %x\n", summary.Checksum)
	fmt.Printf("utxo_count: %d\n", summary.UTXOCount)
	return nil
}

func runSnapshotExport(args []string) error {
	fs := newFlagSet("snapshot export")
	dbPath := fs.String("db", "", "")
	outPath := fs.String("out", "snapshot.json", "")
	genesisFixture := fs.String("genesis-fixture", "", "")
	chainFixture := fs.String("chain-fixture", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return fmt.Errorf("--db is required")
	}
	store, err := storage.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	stored, err := store.LoadChainState()
	if err != nil {
		return err
	}
	if stored == nil {
		return fmt.Errorf("no persisted chain state found at %s", *dbPath)
	}
	view := node.CommittedChainView{
		Height:         stored.Height,
		TipHeader:      stored.TipHeader,
		TipHash:        consensus.HeaderHash(&stored.TipHeader),
		BlockSizeState: stored.BlockSizeState,
		UTXOs:          stored.UTXOs,
		UTXORoot:       consensus.ComputedUTXORoot(stored.UTXOs),
		UTXOChecksum:   stored.UTXOChecksum,
	}
	if err := node.ExportUTXOSnapshotFile(*outPath, view, stored.Profile, *genesisFixture, *chainFixture); err != nil {
		return err
	}
	fmt.Printf("db: %s\n", *dbPath)
	fmt.Printf("file: %s\n", *outPath)
	fmt.Printf("profile: %s\n", stored.Profile)
	fmt.Printf("height: %d\n", view.Height)
	fmt.Printf("header_hash: %x\n", view.TipHash)
	fmt.Printf("utxo_root: %x\n", view.UTXORoot)
	fmt.Printf("utxo_checksum: %x\n", view.UTXOChecksum)
	fmt.Printf("utxo_count: %d\n", len(view.UTXOs))
	return nil
}

func runSnapshotImport(args []string) error {
	fs := newFlagSet("snapshot import")
	dbPath := fs.String("db", "", "")
	file := fs.String("file", "fixtures/snapshots/regtest_bootstrap_tip.json", "")
	genesisFixture := fs.String("genesis-fixture", "", "")
	chainFixture := fs.String("chain-fixture", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return fmt.Errorf("--db is required")
	}
	loaded, err := node.LoadUTXOSnapshotFixture(*file)
	if err != nil {
		return err
	}
	genesisPath := loaded.ResolveReferencePath(loaded.Fixture.GenesisFixture)
	if *genesisFixture != "" {
		genesisPath = *genesisFixture
	}
	if genesisPath == "" {
		return fmt.Errorf("snapshot import requires a genesis fixture path")
	}
	loadedGenesis, err := loadGenesisFixtureFromPath(genesisPath)
	if err != nil {
		return err
	}
	var blocks []types.Block
	if loaded.Fixture.Height > 0 {
		chainPath := loaded.ResolveReferencePath(loaded.Fixture.ChainFixture)
		if *chainFixture != "" {
			chainPath = *chainFixture
		}
		if chainPath == "" {
			return fmt.Errorf("snapshot import at height %d requires a chain fixture path", loaded.Fixture.Height)
		}
		blocks, err = loadValidatedChainFixtureBlocks(chainPath)
		if err != nil {
			return err
		}
	}
	summary, err := node.ImportUTXOSnapshotFastSync(*dbPath, loaded, &loadedGenesis.Block, blocks, nil)
	if err != nil {
		return err
	}
	fmt.Printf("db: %s\n", *dbPath)
	fmt.Printf("file: %s\n", *file)
	fmt.Printf("height: %d\n", summary.Height)
	fmt.Printf("header_hash: %x\n", summary.HeaderHash)
	fmt.Printf("utxo_root: %x\n", summary.UTXORoot)
	fmt.Printf("utxo_checksum: %x\n", summary.Checksum)
	fmt.Printf("utxo_count: %d\n", summary.UTXOCount)
	return nil
}

func loadValidatedChainFixtureBlocks(path string) ([]types.Block, error) {
	fixture, err := loadChainFixture(path)
	if err != nil {
		return nil, err
	}
	blocks := make([]types.Block, 0, len(fixture.Blocks))
	for i, blockHex := range fixture.Blocks {
		block, err := consensus.DecodeBlockHex(blockHex, types.DefaultCodecLimits())
		if err != nil {
			return nil, fmt.Errorf("failed to decode fixture block at index %d: %w", i, err)
		}
		gotHash := fmt.Sprintf("%x", consensus.HeaderHash(&block.Header))
		if i >= len(fixture.ExpectedBlockHashesHex) {
			return nil, fmt.Errorf("missing expected block hash for index %d", i)
		}
		if gotHash != fixture.ExpectedBlockHashesHex[i] {
			return nil, fmt.Errorf("block hash mismatch at index %d: expected %s, got %s", i, fixture.ExpectedBlockHashesHex[i], gotHash)
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}
