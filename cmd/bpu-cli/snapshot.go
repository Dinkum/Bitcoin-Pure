package main

import (
	"flag"
	"fmt"
	"os"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/node"
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
