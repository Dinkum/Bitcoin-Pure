package consensus

import (
	"bytes"
	"errors"
	"math/big"
	"runtime"
	"testing"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func consensusTestKeyHash(seed byte) [32]byte {
	pubKey, _ := crypto.SignSchnorrForTest([32]byte{seed}, &[32]byte{})
	return crypto.KeyHash(&pubKey)
}

func signedSpendTxForConsensusTest(t *testing.T, spenderSeed byte, prevOut types.OutPoint, value uint64, recipientSeed byte, fee uint64) types.Transaction {
	t.Helper()
	if fee >= value {
		t.Fatalf("fee %d must be less than value %d", fee, value)
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: value - fee, KeyHash: consensusTestKeyHash(recipientSeed)}},
		},
	}
	msg, err := Sighash(&tx, 0, []uint64{value})
	if err != nil {
		t.Fatal(err)
	}
	pubKey, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{PubKey: pubKey, Signature: sig}}}
	return tx
}

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

func TestMerkleRootParallelMatchesSequential(t *testing.T) {
	items := make([][32]byte, 513)
	for i := range items {
		items[i][0] = byte(i)
		items[i][1] = byte(i >> 8)
	}
	if got, want := MerkleRootParallel(items), MerkleRoot(items); got != want {
		t.Fatalf("parallel merkle root = %x, want %x", got, want)
	}
}

func TestBuildBlockRootsMatchesDirectComputation(t *testing.T) {
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		t.Skip("parallel root path needs multiple workers to exercise")
	}
	txs := make([]types.Transaction, 0, 256)
	for i := 0; i < 256; i++ {
		tx := types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs: []types.TxInput{{
					PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 1)}, Vout: 0},
				}},
				Outputs: []types.TxOutput{{
					ValueAtoms: uint64(i + 10),
					KeyHash:    [32]byte{byte(i + 2)},
				}},
			},
			Auth: types.TxAuth{
				Entries: []types.TxAuthEntry{{
					PubKey:    [32]byte{byte(i + 3)},
					Signature: [64]byte{byte(i + 4)},
				}},
			},
		}
		txs = append(txs, tx)
	}
	txids, authids, txRoot, authRoot := BuildBlockRoots(txs)
	directTxIDs := make([][32]byte, len(txs))
	directAuthIDs := make([][32]byte, len(txs))
	for i := range txs {
		directTxIDs[i] = TxID(&txs[i])
		directAuthIDs[i] = AuthID(&txs[i])
	}
	if len(txids) != len(directTxIDs) || len(authids) != len(directAuthIDs) {
		t.Fatal("unexpected root leaf count")
	}
	for i := range txs {
		if txids[i] != directTxIDs[i] {
			t.Fatalf("txid %d mismatch", i)
		}
		if authids[i] != directAuthIDs[i] {
			t.Fatalf("authid %d mismatch", i)
		}
	}
	if txRoot != MerkleRoot(directTxIDs) {
		t.Fatalf("tx root mismatch")
	}
	if authRoot != MerkleRoot(directAuthIDs) {
		t.Fatalf("auth root mismatch")
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

func TestSubsidyAtomsMatchesSpecSchedule(t *testing.T) {
	params := MainnetParams()
	if got := SubsidyAtoms(0, params); got != 1_000_000_000_000 {
		t.Fatalf("genesis subsidy = %d, want %d", got, uint64(1_000_000_000_000))
	}
	if got := SubsidyAtoms(params.HalvingInterval, params); got != 500_000_000_000 {
		t.Fatalf("first halving subsidy = %d, want %d", got, uint64(500_000_000_000))
	}
	if got := SubsidyAtoms(params.HalvingInterval*39, params); got != 1 {
		t.Fatalf("39th halving subsidy = %d, want 1", got)
	}
	if got := SubsidyAtoms(params.HalvingInterval*40, params); got != 1 {
		t.Fatalf("tail-emission subsidy = %d, want 1", got)
	}
	if got := SubsidyAtoms(params.HalvingInterval*80, params); got != 1 {
		t.Fatalf("far-future subsidy = %d, want 1", got)
	}
}

func TestMineHeaderInterruptibleStopsWhenTemplateTurnsStale(t *testing.T) {
	params := RegtestParams()
	header := types.BlockHeader{NBits: params.GenesisBits}
	mined, ok, err := MineHeaderInterruptible(header, params, func(uint32) bool { return false })
	if err != nil {
		t.Fatalf("MineHeaderInterruptible: %v", err)
	}
	if ok {
		t.Fatal("expected interruptible mining to stop before finding work")
	}
	if mined != (types.BlockHeader{}) {
		t.Fatalf("interrupted header = %+v, want zero header", mined)
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

func TestValidateAndApplyBlockRejectsNonLTOROrder(t *testing.T) {
	params := RegtestParams()
	firstPrev := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	secondPrev := types.OutPoint{TxID: [32]byte{2}, Vout: 0}
	utxos := UtxoSet{
		firstPrev:  {ValueAtoms: 50, KeyHash: consensusTestKeyHash(1)},
		secondPrev: {ValueAtoms: 50, KeyHash: consensusTestKeyHash(2)},
	}
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	txA := signedSpendTxForConsensusTest(t, 1, firstPrev, 50, 3, 1)
	txB := signedSpendTxForConsensusTest(t, 2, secondPrev, 50, 4, 1)
	txAID := TxID(&txA)
	txBID := TxID(&txB)
	ordered := []types.Transaction{txA, txB}
	if bytes.Compare(txAID[:], txBID[:]) < 0 {
		ordered = []types.Transaction{txB, txA}
	}
	coinbase := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: [32]byte{9}}},
		},
	}
	txs := append([]types.Transaction{coinbase}, ordered...)
	_, _, txRoot, authRoot := BuildBlockRoots(txs)

	nextUTXOs := cloneUtxos(utxos)
	delete(nextUTXOs, firstPrev)
	delete(nextUTXOs, secondPrev)
	for _, tx := range []types.Transaction{txA, txB} {
		txid := TxID(&tx)
		nextUTXOs[types.OutPoint{TxID: txid, Vout: 0}] = UtxoEntry{
			ValueAtoms: tx.Base.Outputs[0].ValueAtoms,
			KeyHash:    tx.Base.Outputs[0].KeyHash,
		}
	}
	coinbaseTxID := TxID(&coinbase)
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = UtxoEntry{
		ValueAtoms: coinbase.Base.Outputs[0].ValueAtoms,
		KeyHash:    coinbase.Base.Outputs[0].KeyHash,
	}
	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&prevHeader),
			MerkleTxIDRoot: txRoot,
			MerkleAuthRoot: authRoot,
			UTXORoot:       ComputedUTXORoot(nextUTXOs),
			Timestamp:      prevHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: txs,
	}
	block.Header = mineHeaderForTest(block.Header)

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, DefaultConsensusRules())
	if !errors.Is(err, ErrTxOrderInvalid) {
		t.Fatalf("expected tx order error, got %v", err)
	}
}

func TestValidateAndApplyBlockRejectsSameBlockSpend(t *testing.T) {
	params := RegtestParams()
	prevOut := types.OutPoint{TxID: [32]byte{5}, Vout: 0}
	utxos := UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: consensusTestKeyHash(5)},
	}
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	var parent types.Transaction
	var child types.Transaction
	var parentTxID [32]byte
	foundLTORPair := false
	for parentSeed := byte(6); parentSeed < 64 && !foundLTORPair; parentSeed++ {
		candidateParent := signedSpendTxForConsensusTest(t, 5, prevOut, 50, parentSeed, 1)
		candidateParentTxID := TxID(&candidateParent)
		for childSeed := byte(64); childSeed < 128; childSeed++ {
			candidateChild := signedSpendTxForConsensusTest(t, parentSeed, types.OutPoint{TxID: candidateParentTxID, Vout: 0}, 49, childSeed, 1)
			candidateChildTxID := TxID(&candidateChild)
			if bytes.Compare(candidateParentTxID[:], candidateChildTxID[:]) < 0 {
				parent = candidateParent
				child = candidateChild
				parentTxID = candidateParentTxID
				foundLTORPair = true
				break
			}
		}
	}
	if !foundLTORPair {
		t.Fatal("failed to construct LTOR-compliant same-block spend fixture")
	}
	coinbase := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: [32]byte{8}}},
		},
	}
	txs := []types.Transaction{coinbase, parent, child}
	_, _, txRoot, authRoot := BuildBlockRoots(txs)

	nextUTXOs := cloneUtxos(utxos)
	delete(nextUTXOs, prevOut)
	nextUTXOs[types.OutPoint{TxID: parentTxID, Vout: 0}] = UtxoEntry{
		ValueAtoms: parent.Base.Outputs[0].ValueAtoms,
		KeyHash:    parent.Base.Outputs[0].KeyHash,
	}
	coinbaseTxID := TxID(&coinbase)
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = UtxoEntry{
		ValueAtoms: coinbase.Base.Outputs[0].ValueAtoms,
		KeyHash:    coinbase.Base.Outputs[0].KeyHash,
	}
	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&prevHeader),
			MerkleTxIDRoot: txRoot,
			MerkleAuthRoot: authRoot,
			UTXORoot:       ComputedUTXORoot(nextUTXOs),
			Timestamp:      prevHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: txs,
	}
	block.Header = mineHeaderForTest(block.Header)

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, DefaultConsensusRules())
	if !errors.Is(err, ErrMissingUTXO) {
		t.Fatalf("expected missing utxo error, got %v", err)
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

func TestNewBlockSizeStateUsesABLAFloors(t *testing.T) {
	params := MainnetParams()
	state := NewBlockSizeState(params)
	if state.BlockSize != 0 {
		t.Fatalf("block size = %d, want 0", state.BlockSize)
	}
	if state.Epsilon != 16_000_000 {
		t.Fatalf("epsilon = %d, want 16000000", state.Epsilon)
	}
	if state.Beta != 16_000_000 {
		t.Fatalf("beta = %d, want 16000000", state.Beta)
	}
	if state.Limit() != params.BlockSizeFloor {
		t.Fatalf("limit = %d, want %d", state.Limit(), params.BlockSizeFloor)
	}
}

func TestAdvanceBlockSizeStateABLAPositiveBranch(t *testing.T) {
	params := MainnetParams()
	prev := BlockSizeState{
		BlockSize: params.BlockSizeFloor,
		Epsilon:   16_000_000,
		Beta:      16_000_000,
	}
	next := AdvanceBlockSizeState(prev, 1_000, params)
	if next.BlockSize != 1_000 {
		t.Fatalf("next block size = %d, want 1000", next.BlockSize)
	}
	if next.Epsilon != 16_000_210 {
		t.Fatalf("epsilon = %d, want 16000210", next.Epsilon)
	}
	if next.Beta != 16_001_679 {
		t.Fatalf("beta = %d, want 16001679", next.Beta)
	}
	if got := NextBlockSizeLimit(prev, params); got != 32_001_889 {
		t.Fatalf("next limit = %d, want 32001889", got)
	}
}

func TestAdvanceBlockSizeStateABLANegativeBranchClampsToFloor(t *testing.T) {
	params := MainnetParams()
	prev := BlockSizeState{
		BlockSize: 0,
		Epsilon:   16_000_000,
		Beta:      16_000_000,
	}
	next := AdvanceBlockSizeState(prev, 512, params)
	if next.BlockSize != 512 {
		t.Fatalf("next block size = %d, want 512", next.BlockSize)
	}
	if next.Epsilon != 16_000_000 {
		t.Fatalf("epsilon = %d, want floor 16000000", next.Epsilon)
	}
	if next.Beta != 16_000_000 {
		t.Fatalf("beta = %d, want floor 16000000", next.Beta)
	}
	if got := NextBlockSizeLimit(prev, params); got != params.BlockSizeFloor {
		t.Fatalf("next limit = %d, want %d", got, params.BlockSizeFloor)
	}
}
