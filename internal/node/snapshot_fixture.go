package node

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utxochecksum"
)

const UTXOSnapshotFixtureVersion = 2

type UTXOSnapshotFixture struct {
	Version               uint32                     `json:"version"`
	Profile               string                     `json:"profile"`
	GenesisFixture        string                     `json:"genesis_fixture"`
	ChainFixture          string                     `json:"chain_fixture,omitempty"`
	Height                uint64                     `json:"height"`
	ExpectedHeaderHashHex string                     `json:"expected_header_hash_hex"`
	ExpectedUTXORootHex   string                     `json:"expected_utxo_root_hex"`
	ExpectedChecksumHex   string                     `json:"expected_utxo_checksum_hex"`
	ExpectedUTXOCount     int                        `json:"expected_utxo_count"`
	UTXOs                 []UTXOSnapshotFixtureEntry `json:"utxos"`
}

type UTXOSnapshotFixtureEntry struct {
	TxIDHex    string `json:"txid_hex"`
	Vout       uint32 `json:"vout"`
	ValueAtoms uint64 `json:"value_atoms"`
	PubKeyHex  string `json:"pubkey_hex"`
}

type LoadedUTXOSnapshotFixture struct {
	Path               string
	Fixture            UTXOSnapshotFixture
	ExpectedHeaderHash [32]byte
	ExpectedUTXORoot   [32]byte
	ExpectedChecksum   [32]byte
	UTXOs              consensus.UtxoSet
	ComputedUTXORoot   [32]byte
	ComputedChecksum   [32]byte
}

type SnapshotVerificationSummary struct {
	Height     uint64
	HeaderHash [32]byte
	UTXORoot   [32]byte
	Checksum   [32]byte
	UTXOCount  int
}

func LoadUTXOSnapshotFixture(path string) (*LoadedUTXOSnapshotFixture, error) {
	var fixture UTXOSnapshotFixture
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(buf, &fixture); err != nil {
		return nil, err
	}
	if fixture.Version != UTXOSnapshotFixtureVersion {
		return nil, fmt.Errorf("unsupported snapshot fixture version %d", fixture.Version)
	}
	expectedHeaderHash, err := decodeSnapshotHex32(fixture.ExpectedHeaderHashHex, "expected_header_hash_hex")
	if err != nil {
		return nil, err
	}
	expectedUTXORoot, err := decodeSnapshotHex32(fixture.ExpectedUTXORootHex, "expected_utxo_root_hex")
	if err != nil {
		return nil, err
	}
	expectedChecksum, err := decodeSnapshotHex32(fixture.ExpectedChecksumHex, "expected_utxo_checksum_hex")
	if err != nil {
		return nil, err
	}
	utxos, err := decodeSnapshotUTXOs(fixture.UTXOs)
	if err != nil {
		return nil, err
	}
	return &LoadedUTXOSnapshotFixture{
		Path:               filepath.Clean(path),
		Fixture:            fixture,
		ExpectedHeaderHash: expectedHeaderHash,
		ExpectedUTXORoot:   expectedUTXORoot,
		ExpectedChecksum:   expectedChecksum,
		UTXOs:              utxos,
		ComputedUTXORoot:   consensus.ComputedUTXORoot(utxos),
		ComputedChecksum:   utxochecksum.Compute(utxos),
	}, nil
}

func (f *LoadedUTXOSnapshotFixture) ResolveReferencePath(reference string) string {
	if reference == "" || filepath.IsAbs(reference) {
		return reference
	}
	return filepath.Clean(filepath.Join(filepath.Dir(f.Path), reference))
}

func VerifyUTXOSnapshotFixture(loaded *LoadedUTXOSnapshotFixture, genesis *types.Block, blocks []types.Block) (SnapshotVerificationSummary, error) {
	if loaded == nil {
		return SnapshotVerificationSummary{}, fmt.Errorf("snapshot fixture is required")
	}
	profile, err := types.ParseChainProfile(loaded.Fixture.Profile)
	if err != nil {
		return SnapshotVerificationSummary{}, err
	}
	summary, err := VerifyUTXOSnapshotAtHeight(profile, genesis, blocks, loaded.Fixture.Height, loaded.UTXOs)
	if err != nil {
		return SnapshotVerificationSummary{}, err
	}
	if summary.HeaderHash != loaded.ExpectedHeaderHash {
		return SnapshotVerificationSummary{}, fmt.Errorf("snapshot header hash mismatch: expected %x, got %x", loaded.ExpectedHeaderHash, summary.HeaderHash)
	}
	if summary.UTXORoot != loaded.ExpectedUTXORoot {
		return SnapshotVerificationSummary{}, fmt.Errorf("snapshot utxo_root mismatch: expected %x, got %x", loaded.ExpectedUTXORoot, summary.UTXORoot)
	}
	if summary.Checksum != loaded.ExpectedChecksum {
		return SnapshotVerificationSummary{}, fmt.Errorf("snapshot utxo checksum mismatch: expected %x, got %x", loaded.ExpectedChecksum, summary.Checksum)
	}
	if summary.UTXOCount != loaded.Fixture.ExpectedUTXOCount {
		return SnapshotVerificationSummary{}, fmt.Errorf("snapshot utxo count mismatch: expected %d, got %d", loaded.Fixture.ExpectedUTXOCount, summary.UTXOCount)
	}
	return summary, nil
}

func VerifyUTXOSnapshotAtHeight(profile types.ChainProfile, genesis *types.Block, blocks []types.Block, height uint64, snapshot consensus.UtxoSet) (SnapshotVerificationSummary, error) {
	if genesis == nil {
		return SnapshotVerificationSummary{}, fmt.Errorf("genesis block is required")
	}
	if height > uint64(len(blocks)) {
		return SnapshotVerificationSummary{}, fmt.Errorf("snapshot height %d exceeds available blocks %d", height, len(blocks))
	}
	state := NewChainState(profile)
	if _, err := state.InitializeFromGenesisBlock(genesis); err != nil {
		return SnapshotVerificationSummary{}, err
	}
	header := genesis.Header
	if height > 0 {
		for i := uint64(0); i < height; i++ {
			if _, err := state.ApplyBlock(&blocks[i]); err != nil {
				return SnapshotVerificationSummary{}, fmt.Errorf("apply block at height %d: %w", i+1, err)
			}
		}
		header = blocks[height-1].Header
	}
	snapshotRoot := consensus.ComputedUTXORoot(snapshot)
	if header.UTXORoot != snapshotRoot {
		return SnapshotVerificationSummary{}, fmt.Errorf("snapshot root does not match header utxo_root at height %d: expected %x, got %x", height, header.UTXORoot, snapshotRoot)
	}
	if err := compareSnapshotUTXOsIter(state.UTXOCount(), state.ForEachUTXO, snapshot); err != nil {
		return SnapshotVerificationSummary{}, err
	}
	if liveRoot := state.UTXORoot(); liveRoot != snapshotRoot {
		return SnapshotVerificationSummary{}, fmt.Errorf("chainstate utxo_root mismatch at height %d: expected %x, got %x", height, snapshotRoot, liveRoot)
	}
	snapshotChecksum := utxochecksum.Compute(snapshot)
	if liveChecksum := state.UTXOChecksum(); liveChecksum != snapshotChecksum {
		return SnapshotVerificationSummary{}, fmt.Errorf("chainstate utxo checksum mismatch at height %d: expected %x, got %x", height, snapshotChecksum, liveChecksum)
	}
	return SnapshotVerificationSummary{
		Height:     height,
		HeaderHash: consensus.HeaderHash(&header),
		UTXORoot:   snapshotRoot,
		Checksum:   snapshotChecksum,
		UTXOCount:  len(snapshot),
	}, nil
}

func decodeSnapshotUTXOs(entries []UTXOSnapshotFixtureEntry) (consensus.UtxoSet, error) {
	utxos := make(consensus.UtxoSet, len(entries))
	var prev types.OutPoint
	hasPrev := false
	for i, entry := range entries {
		txid, err := decodeSnapshotHex32(entry.TxIDHex, fmt.Sprintf("utxos[%d].txid_hex", i))
		if err != nil {
			return nil, err
		}
		pubKey, err := decodeSnapshotHex32(entry.PubKeyHex, fmt.Sprintf("utxos[%d].pubkey_hex", i))
		if err != nil {
			return nil, err
		}
		outPoint := types.OutPoint{TxID: txid, Vout: entry.Vout}
		if hasPrev && compareSnapshotOutPoints(prev, outPoint) >= 0 {
			return nil, fmt.Errorf("snapshot utxos must be strictly ordered by outpoint at index %d", i)
		}
		hasPrev = true
		prev = outPoint
		utxos[outPoint] = consensus.UtxoEntry{
			ValueAtoms: entry.ValueAtoms,
			PubKey:     pubKey,
		}
	}
	return utxos, nil
}

func compareSnapshotUTXOs(live, snapshot consensus.UtxoSet) error {
	if len(live) != len(snapshot) {
		return fmt.Errorf("snapshot utxo set size mismatch: expected %d, got %d", len(live), len(snapshot))
	}
	for outPoint, expected := range snapshot {
		got, ok := live[outPoint]
		if !ok {
			return fmt.Errorf("snapshot missing live outpoint %x:%d", outPoint.TxID, outPoint.Vout)
		}
		if got != expected {
			return fmt.Errorf("snapshot entry mismatch for %x:%d", outPoint.TxID, outPoint.Vout)
		}
	}
	return nil
}

func compareSnapshotUTXOsIter(liveCount int, iterate func(func(types.OutPoint, consensus.UtxoEntry) error) error, snapshot consensus.UtxoSet) error {
	if liveCount != len(snapshot) {
		return fmt.Errorf("snapshot utxo set size mismatch: expected %d, got %d", len(snapshot), liveCount)
	}
	seen := 0
	if err := iterate(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		expected, ok := snapshot[outPoint]
		if !ok {
			return fmt.Errorf("snapshot contains unexpected live outpoint %x:%d", outPoint.TxID, outPoint.Vout)
		}
		if entry != expected {
			return fmt.Errorf("snapshot entry mismatch for %x:%d", outPoint.TxID, outPoint.Vout)
		}
		seen++
		return nil
	}); err != nil {
		return err
	}
	if seen != len(snapshot) {
		return fmt.Errorf("snapshot utxo iteration count mismatch: expected %d, got %d", len(snapshot), seen)
	}
	return nil
}

func compareSnapshotOutPoints(a, b types.OutPoint) int {
	if cmp := bytes.Compare(a.TxID[:], b.TxID[:]); cmp != 0 {
		return cmp
	}
	switch {
	case a.Vout < b.Vout:
		return -1
	case a.Vout > b.Vout:
		return 1
	default:
		return 0
	}
}

func decodeSnapshotHex32(raw, field string) ([32]byte, error) {
	var out [32]byte
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return out, fmt.Errorf("%s: %w", field, err)
	}
	if len(buf) != len(out) {
		return out, fmt.Errorf("%s: expected %d-byte hex", field, len(out))
	}
	copy(out[:], buf)
	return out, nil
}
