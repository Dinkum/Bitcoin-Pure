package consensus

import (
	"errors"
	"math/big"
	"testing"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func TestTxIDAndAuthIDAreStable(t *testing.T) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{{
				PrevOut: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
			}},
			Outputs: []types.TxOutput{{
				ValueAtoms: 10,
				KeyHash:    [32]byte{9},
			}},
		},
		Auth: types.TxAuth{
			Entries: []types.TxAuthEntry{{
				PubKey:    [32]byte{3},
				Signature: [64]byte{4},
			}},
		},
	}
	txid := TxID(&tx)
	authid := AuthID(&tx)
	tx.Auth.Entries[0].Signature[0] ^= 0xff
	if TxID(&tx) != txid {
		t.Fatal("txid should ignore auth section")
	}
	if AuthID(&tx) == authid {
		t.Fatal("authid should change with auth bytes")
	}
}

func TestValidateSignedSpend(t *testing.T) {
	seed := [32]byte{7}
	msgPub, _ := crypto.SignSchnorrForTest(seed, &[32]byte{})
	utxos := UtxoSet{
		types.OutPoint{TxID: [32]byte{2}, Vout: 0}: {
			ValueAtoms: 50,
			KeyHash:    crypto.KeyHash(&msgPub),
		},
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{{
				PrevOut: types.OutPoint{TxID: [32]byte{2}, Vout: 0},
			}},
			Outputs: []types.TxOutput{{
				ValueAtoms: 40,
				KeyHash:    [32]byte{8},
			}},
		},
	}
	msg, err := Sighash(&tx, 0, []uint64{50})
	if err != nil {
		t.Fatal(err)
	}
	pubKey, sig := crypto.SignSchnorrForTest(seed, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{PubKey: pubKey, Signature: sig}}}
	summary, err := ValidateTx(&tx, utxos, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("validate tx: %v", err)
	}
	if summary.Fee != 10 {
		t.Fatalf("unexpected fee: %d", summary.Fee)
	}
}

func TestComputedUTXORootOrderInvariant(t *testing.T) {
	a := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	b := types.OutPoint{TxID: [32]byte{2}, Vout: 1}
	left := UtxoSet{
		a: {ValueAtoms: 10, KeyHash: [32]byte{1}},
		b: {ValueAtoms: 20, KeyHash: [32]byte{2}},
	}
	right := UtxoSet{
		b: {ValueAtoms: 20, KeyHash: [32]byte{2}},
		a: {ValueAtoms: 10, KeyHash: [32]byte{1}},
	}
	if ComputedUTXORoot(left) != ComputedUTXORoot(right) {
		t.Fatal("utxo root should be order-invariant")
	}
}

func compactTargetForTest(compact uint32) *big.Int {
	target, err := compactToTarget(compact)
	if err != nil {
		panic(err)
	}
	return target
}

func mineHeaderForTest(header types.BlockHeader) types.BlockHeader {
	target := compactTargetForTest(header.NBits)
	for nonce := uint32(0); nonce < ^uint32(0); nonce++ {
		header.Nonce = nonce
		hash := HeaderHash(&header)
		if new(big.Int).SetBytes(hash[:]).Cmp(target) <= 0 {
			return header
		}
	}
	panic("unable to mine header")
}

func TestCoinbaseOnlyBlockValidatesOnRegtest(t *testing.T) {
	params := RegtestParams()
	genesisTx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{ValueAtoms: 50, KeyHash: [32]byte{7}}},
		},
	}
	genesisTxID := TxID(&genesisTx)
	genesisUTXOs := UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, KeyHash: [32]byte{7}},
	}
	genesisHeader := types.BlockHeader{
		Version:        1,
		MerkleTxIDRoot: MerkleRoot([][32]byte{genesisTxID}),
		MerkleAuthRoot: MerkleRoot([][32]byte{AuthID(&genesisTx)}),
		UTXORoot:       ComputedUTXORoot(genesisUTXOs),
		Timestamp:      params.GenesisTimestamp,
		NBits:          params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: genesisHeader}

	blockTx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: [32]byte{3}}},
		},
	}
	nextUTXOs := cloneUtxos(genesisUTXOs)
	blockTxID := TxID(&blockTx)
	nextUTXOs[types.OutPoint{TxID: blockTxID, Vout: 0}] = UtxoEntry{ValueAtoms: 1, KeyHash: [32]byte{3}}

	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&genesisHeader),
			MerkleTxIDRoot: MerkleRoot([][32]byte{blockTxID}),
			MerkleAuthRoot: MerkleRoot([][32]byte{AuthID(&blockTx)}),
			UTXORoot:       ComputedUTXORoot(nextUTXOs),
			Timestamp:      genesisHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: []types.Transaction{blockTx},
	}
	block.Header = mineHeaderForTest(block.Header)
	utxos := cloneUtxos(genesisUTXOs)
	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), utxos, params, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("validate block: %v", err)
	}
}

func TestUTXORootMismatchRejects(t *testing.T) {
	params := RegtestParams()
	genesis := types.BlockHeader{Timestamp: params.GenesisTimestamp, NBits: params.GenesisBits}
	block := types.Block{
		Header: types.BlockHeader{
			PrevBlockHash: HeaderHash(&genesis),
			Timestamp:     genesis.Timestamp + 600,
			NBits:         params.GenesisBits,
		},
		Txs: []types.Transaction{{
			Base: types.TxBase{
				Version: 1,
				Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: [32]byte{1}}},
			},
		}},
	}
	txid := TxID(&block.Txs[0])
	block.Header.MerkleTxIDRoot = MerkleRoot([][32]byte{txid})
	block.Header.MerkleAuthRoot = MerkleRoot([][32]byte{AuthID(&block.Txs[0])})
	block.Header = mineHeaderForTest(block.Header)
	_, err := ValidateAndApplyBlock(&block, PrevBlockContext{Height: 0, Header: genesis}, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
	if !errors.Is(err, ErrUTXORootMismatch) {
		t.Fatalf("expected utxo root mismatch, got %v", err)
	}
}

func TestValidateHeaderAcceptsValidRegtestHeader(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	nbits, err := NextWorkRequired(PrevBlockContext{Height: 0, Header: prev}, params)
	if err != nil {
		t.Fatal(err)
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     prev.Timestamp + 600,
		NBits:         nbits,
	}
	header = mineHeaderForTest(header)
	if err := ValidateHeader(&header, PrevBlockContext{Height: 0, Header: prev}, params); err != nil {
		t.Fatalf("validate header: %v", err)
	}
}

func TestValidateHeaderRejectsPrevHashMismatch(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: [32]byte{9},
		Timestamp:     prev.Timestamp + 600,
		NBits:         params.GenesisBits,
	}
	header = mineHeaderForTest(header)
	err := ValidateHeader(&header, PrevBlockContext{Height: 0, Header: prev}, params)
	if !errors.Is(err, ErrPrevHashMismatch) {
		t.Fatalf("expected prev hash mismatch, got %v", err)
	}
}

func TestNextWorkRequiredASERTOnScheduleMatchesGenesisBits(t *testing.T) {
	params := RegtestParams()
	prev := PrevBlockContext{
		Height: 0,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp,
			NBits:     params.GenesisBits,
		},
	}
	got, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	if got != params.GenesisBits {
		t.Fatalf("expected genesis bits 0x%08x, got 0x%08x", params.GenesisBits, got)
	}
}

func TestNextWorkRequiredASERTUsesParentTimestampNotCandidateTimestamp(t *testing.T) {
	params := RegtestParams()
	prev := PrevBlockContext{
		Height: 1,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp + 3600,
			NBits:     params.GenesisBits,
		},
	}
	gotA, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	if gotA != gotB {
		t.Fatalf("expected parent-timestamp anchored bits to be stable: %08x vs %08x", gotA, gotB)
	}
}

func TestNextWorkRequiredASERTLateParentEasesDifficulty(t *testing.T) {
	params := RegtestParams()
	prev := PrevBlockContext{
		Height: 10,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp + 10*600 + 3600,
			NBits:     params.GenesisBits,
		},
	}
	got, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	genesisTarget, err := compactToTarget(params.GenesisBits)
	if err != nil {
		t.Fatal(err)
	}
	gotTarget, err := compactToTarget(got)
	if err != nil {
		t.Fatal(err)
	}
	if gotTarget.Cmp(genesisTarget) < 0 {
		t.Fatalf("expected easier or equal target than genesis, got genesis=%s current=%s", genesisTarget.String(), gotTarget.String())
	}
}

func TestNextWorkRequiredBitcoinLegacyRetargetHelperClampsToPowLimit(t *testing.T) {
	params := RegtestParams()
	first := &types.BlockHeader{Timestamp: params.GenesisTimestamp, NBits: params.GenesisBits}
	prev := &types.BlockHeader{Timestamp: params.GenesisTimestamp + uint64(params.TargetSpacingSecs*2016*10), NBits: params.GenesisBits}
	got, err := NextWorkRequiredBitcoinLegacy(first, prev, params)
	if err != nil {
		t.Fatal(err)
	}
	if got != params.PowLimitBits {
		t.Fatalf("expected pow limit bits 0x%08x, got 0x%08x", params.PowLimitBits, got)
	}
}
