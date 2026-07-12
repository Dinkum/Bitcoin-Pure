package consensus

import (
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"bytes"
	"errors"
	"testing"
)

func validateAndApplyBlockForTest(t *testing.T, block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, utxos UtxoSet, params ChainParams, rules ConsensusRules) (BlockValidationSummary, error) {
	t.Helper()
	summary, overlay, _, err := ValidateAndApplyBlockOverlayWithLookup(block, prev, blockSizeState, utxos, LookupWithErrFromSet(utxos), nil, params, rules)
	if err != nil {
		return BlockValidationSummary{}, err
	}
	overlay.ApplyToSet(utxos)
	return summary, nil
}

func validateAndApplyBlockWithAccumulatorForTest(t *testing.T, block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, utxos UtxoSet, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules) (BlockValidationSummary, *utreexo.Accumulator, error) {
	t.Helper()
	summary, overlay, nextAcc, err := ValidateAndApplyBlockOverlayWithLookup(block, prev, blockSizeState, utxos, LookupWithErrFromSet(utxos), accumulator, params, rules)
	if err != nil {
		return BlockValidationSummary{}, nil, err
	}
	overlay.ApplyToSet(utxos)
	return summary, nextAcc, nil
}

func TestCoinbaseOnlyBlockValidatesOnRegtest(t *testing.T) {
	params := RegtestParams()
	genesisTx := types.Transaction{
		Base: coinbaseTxForConsensusTest(0, []types.TxOutput{{ValueAtoms: 50, PubKey: consensusTestPubKey(7)}}).Base,
	}
	genesisTxID := TxID(&genesisTx)
	genesisUTXOs := UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, PubKey: consensusTestPubKey(7)},
	}
	genesisHeader := types.BlockHeader{
		Version:        1,
		MerkleTxIDRoot: specMerkleRootForTest([][32]byte{genesisTxID}),
		MerkleAuthRoot: specMerkleRootForTest([][32]byte{AuthID(&genesisTx)}),
		UTXORoot:       ComputedUTXORoot(genesisUTXOs),
		Timestamp:      params.GenesisTimestamp,
		NBits:          params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: genesisHeader}

	blockTx := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(3)}})
	nextUTXOs := cloneUtxos(genesisUTXOs)
	blockTxID := TxID(&blockTx)
	nextUTXOs[types.OutPoint{TxID: blockTxID, Vout: 0}] = UtxoEntry{ValueAtoms: 1, PubKey: consensusTestPubKey(3)}

	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&genesisHeader),
			MerkleTxIDRoot: specMerkleRootForTest([][32]byte{blockTxID}),
			MerkleAuthRoot: specMerkleRootForTest([][32]byte{AuthID(&blockTx)}),
			UTXORoot:       ComputedUTXORoot(nextUTXOs),
			Timestamp:      genesisHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: []types.Transaction{blockTx},
	}
	block.Header = mineHeaderForTest(block.Header)
	utxos := cloneUtxos(genesisUTXOs)
	_, err = validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), utxos, params, DefaultConsensusRules())
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
			Base: coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(1)}}).Base,
		}},
	}
	txid := TxID(&block.Txs[0])
	block.Header.MerkleTxIDRoot = specMerkleRootForTest([][32]byte{txid})
	block.Header.MerkleAuthRoot = specMerkleRootForTest([][32]byte{AuthID(&block.Txs[0])})
	block.Header = mineHeaderForTest(block.Header)
	_, err := validateAndApplyBlockForTest(t, &block, PrevBlockContext{Height: 0, Header: genesis}, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
	if !errors.Is(err, ErrUTXORootMismatch) {
		t.Fatalf("expected utxo root mismatch, got %v", err)
	}
}

func TestValidateTxRejectsZeroValueOutputs(t *testing.T) {
	prevOut := types.OutPoint{TxID: [32]byte{11}, Vout: 0}
	utxos := UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: consensusTestPubKey(1)},
	}
	pqLock := [32]byte{0xaa}
	tests := []struct {
		name    string
		outputs []types.TxOutput
	}{
		{
			name:    "xonly zero output",
			outputs: []types.TxOutput{types.NewXOnlyOutput(0, consensusTestPubKey(2))},
		},
		{
			name:    "pq zero output",
			outputs: []types.TxOutput{types.NewPQLockOutput(0, pqLock)},
		},
		{
			name: "mixed outputs with one zero",
			outputs: []types.TxOutput{
				types.NewXOnlyOutput(25, consensusTestPubKey(3)),
				types.NewPQLockOutput(0, pqLock),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := signedSpendTxToOutputsForConsensusTest(t, 1, prevOut, 50, tt.outputs)
			_, err := ValidateTxWithParams(&tx, utxos, RegtestParams(), DefaultConsensusRules())
			if !errors.Is(err, ErrZeroOutputValue) {
				t.Fatalf("expected zero output value error, got %v", err)
			}
		})
	}
}

func TestValidateTxEnforcesCoinbaseMaturityBoundary(t *testing.T) {
	params := RegtestParams()
	createdHeight := uint64(7)
	prevOut := types.OutPoint{TxID: [32]byte{0x44}, Vout: 0}
	coin := UtxoEntryFromOutputAtHeight(types.NewXOnlyOutput(50, consensusTestPubKey(1)), createdHeight, true)
	utxos := UtxoSet{prevOut: coin}
	tx := signedSpendTxForConsensusTest(t, 1, prevOut, 50, 2, 1)

	_, err := ValidateTx(&tx, utxos, TxValidationContext{
		Params:      params,
		SpendHeight: createdHeight + params.CoinbaseMaturity - 1,
	}, DefaultConsensusRules())
	if !errors.Is(err, ErrImmatureCoinbase) {
		t.Fatalf("height H+99 error = %v, want ErrImmatureCoinbase", err)
	}
	if _, err := ValidateTx(&tx, utxos, TxValidationContext{
		Params:      params,
		SpendHeight: createdHeight + params.CoinbaseMaturity,
	}, DefaultConsensusRules()); err != nil {
		t.Fatalf("height H+100 rejected: %v", err)
	}
}

func TestValidateAndApplyBlockRejectsZeroValueCoinbaseOutputs(t *testing.T) {
	params := RegtestParams()
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		output types.TxOutput
	}{
		{
			name:   "xonly coinbase output",
			output: types.NewXOnlyOutput(0, consensusTestPubKey(4)),
		},
		{
			name:   "pq coinbase output",
			output: types.NewPQLockOutput(0, [32]byte{0xbb}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{tt.output})
			coinbaseTxID := TxID(&coinbase)
			block := types.Block{
				Header: types.BlockHeader{
					Version:        1,
					PrevBlockHash:  HeaderHash(&prevHeader),
					MerkleTxIDRoot: specMerkleRootForTest([][32]byte{coinbaseTxID}),
					MerkleAuthRoot: specMerkleRootForTest([][32]byte{AuthID(&coinbase)}),
					UTXORoot:       ComputedUTXORoot(UtxoSet{}),
					Timestamp:      prevHeader.Timestamp + 600,
					NBits:          nbits,
				},
				Txs: []types.Transaction{coinbase},
			}
			block.Header = mineHeaderForTest(block.Header)

			_, err := validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
			if !errors.Is(err, ErrZeroOutputValue) {
				t.Fatalf("expected zero output value error, got %v", err)
			}
		})
	}
}

func TestValidateAndApplyBlockRejectsMerkleTxIDMismatch(t *testing.T) {
	params := RegtestParams()
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(3)}})
	coinbaseTxID := TxID(&coinbase)
	utxos := UtxoSet{
		types.OutPoint{TxID: coinbaseTxID, Vout: 0}: {ValueAtoms: 1, PubKey: consensusTestPubKey(3)},
	}
	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&prevHeader),
			MerkleTxIDRoot: [32]byte{0xaa},
			MerkleAuthRoot: specMerkleRootForTest([][32]byte{AuthID(&coinbase)}),
			UTXORoot:       ComputedUTXORoot(utxos),
			Timestamp:      prevHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: []types.Transaction{coinbase},
	}
	block.Header = mineHeaderForTest(block.Header)

	_, err = validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
	if !errors.Is(err, ErrMerkleTxIDMismatch) {
		t.Fatalf("expected merkle txid mismatch, got %v", err)
	}
}

func TestValidateAndApplyBlockRejectsMerkleAuthMismatch(t *testing.T) {
	params := RegtestParams()
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(4)}})
	coinbaseTxID := TxID(&coinbase)
	utxos := UtxoSet{
		types.OutPoint{TxID: coinbaseTxID, Vout: 0}: {ValueAtoms: 1, PubKey: consensusTestPubKey(4)},
	}
	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&prevHeader),
			MerkleTxIDRoot: specMerkleRootForTest([][32]byte{coinbaseTxID}),
			MerkleAuthRoot: [32]byte{0xbb},
			UTXORoot:       ComputedUTXORoot(utxos),
			Timestamp:      prevHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: []types.Transaction{coinbase},
	}
	block.Header = mineHeaderForTest(block.Header)

	_, err = validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
	if !errors.Is(err, ErrMerkleAuthMismatch) {
		t.Fatalf("expected merkle auth mismatch, got %v", err)
	}
}

func TestValidateAndApplyBlockRejectsCoinbaseOverpay(t *testing.T) {
	params := RegtestParams()
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{
		ValueAtoms: params.InitialSubsidyAtoms + 1,
		PubKey:     consensusTestPubKey(5),
	}})
	coinbaseTxID := TxID(&coinbase)
	utxos := UtxoSet{
		types.OutPoint{TxID: coinbaseTxID, Vout: 0}: {ValueAtoms: params.InitialSubsidyAtoms + 1, PubKey: consensusTestPubKey(5)},
	}
	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&prevHeader),
			MerkleTxIDRoot: specMerkleRootForTest([][32]byte{coinbaseTxID}),
			MerkleAuthRoot: specMerkleRootForTest([][32]byte{AuthID(&coinbase)}),
			UTXORoot:       ComputedUTXORoot(utxos),
			Timestamp:      prevHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: []types.Transaction{coinbase},
	}
	block.Header = mineHeaderForTest(block.Header)

	_, err = validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
	if !errors.Is(err, ErrCoinbaseOverpay) {
		t.Fatalf("expected coinbase overpay, got %v", err)
	}
}

func TestValidateAndApplyBlockRejectsNonLTOROrder(t *testing.T) {
	params := RegtestParams()
	firstPrev := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	secondPrev := types.OutPoint{TxID: [32]byte{2}, Vout: 0}
	utxos := UtxoSet{
		firstPrev:  {ValueAtoms: 50, PubKey: consensusTestPubKey(1)},
		secondPrev: {ValueAtoms: 50, PubKey: consensusTestPubKey(2)},
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
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(9)}})
	txs := append([]types.Transaction{coinbase}, ordered...)
	_, _, txRoot, authRoot := BuildBlockRoots(txs)

	nextUTXOs := cloneUtxos(utxos)
	delete(nextUTXOs, firstPrev)
	delete(nextUTXOs, secondPrev)
	for _, tx := range []types.Transaction{txA, txB} {
		txid := TxID(&tx)
		nextUTXOs[types.OutPoint{TxID: txid, Vout: 0}] = UtxoEntry{
			ValueAtoms: tx.Base.Outputs[0].ValueAtoms,
			PubKey:     tx.Base.Outputs[0].PubKey,
		}
	}
	coinbaseTxID := TxID(&coinbase)
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = UtxoEntry{
		ValueAtoms: coinbase.Base.Outputs[0].ValueAtoms,
		PubKey:     coinbase.Base.Outputs[0].PubKey,
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

	_, err = validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, DefaultConsensusRules())
	if !errors.Is(err, ErrTxOrderInvalid) {
		t.Fatalf("expected tx order error, got %v", err)
	}
}

func TestValidateAndApplyBlockAcceptsSameBlockSpend(t *testing.T) {
	params := RegtestParams()
	prevOut := types.OutPoint{TxID: [32]byte{5}, Vout: 0}
	utxos := UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: consensusTestPubKey(5)},
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
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(8)}})
	txs := []types.Transaction{coinbase, parent, child}
	txids, _, txRoot, authRoot := BuildBlockRoots(txs)

	nextUTXOs := cloneUtxos(utxos)
	delete(nextUTXOs, prevOut)
	delete(nextUTXOs, types.OutPoint{TxID: parentTxID, Vout: 0})
	childTxID := txids[2]
	nextUTXOs[types.OutPoint{TxID: childTxID, Vout: 0}] = UtxoEntryFromOutputAtHeight(child.Base.Outputs[0], 1, false)
	coinbaseTxID := txids[0]
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = UtxoEntryFromOutputAtHeight(coinbase.Base.Outputs[0], 1, true)
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

	applied := cloneUtxos(utxos)
	summary, err := validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), applied, params, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("validate block: %v", err)
	}
	if got, want := summary.TotalFees, uint64(2); got != want {
		t.Fatalf("total fees = %d, want %d", got, want)
	}
	if _, ok := applied[types.OutPoint{TxID: parentTxID, Vout: 0}]; ok {
		t.Fatal("parent output should be spent by same-block child")
	}
	if got, want := applied, nextUTXOs; !equalUtxoSets(got, want) {
		t.Fatalf("post-block utxos mismatch: got %v want %v", got, want)
	}
}

func TestValidateAndApplyBlockAcceptsAtomicLTORSameBlockSpendWithChildBeforeParent(t *testing.T) {
	params := RegtestParams()
	prevOut := types.OutPoint{TxID: [32]byte{9}, Vout: 0}
	utxos := UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: consensusTestPubKey(9)},
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
	foundAtomicLTORPair := false
	for parentSeed := byte(10); parentSeed < 96 && !foundAtomicLTORPair; parentSeed++ {
		candidateParent := signedSpendTxForConsensusTest(t, 9, prevOut, 50, parentSeed, 1)
		candidateParentTxID := TxID(&candidateParent)
		for childSeed := byte(96); childSeed < 180; childSeed++ {
			candidateChild := signedSpendTxForConsensusTest(t, parentSeed, types.OutPoint{TxID: candidateParentTxID, Vout: 0}, 49, childSeed, 1)
			candidateChildTxID := TxID(&candidateChild)
			if bytes.Compare(candidateChildTxID[:], candidateParentTxID[:]) < 0 {
				parent = candidateParent
				child = candidateChild
				parentTxID = candidateParentTxID
				foundAtomicLTORPair = true
				break
			}
		}
	}
	if !foundAtomicLTORPair {
		t.Fatal("failed to construct child-before-parent LTOR same-block spend fixture")
	}
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(8)}})
	txs := []types.Transaction{coinbase, child, parent}
	txids, _, txRoot, authRoot := BuildBlockRoots(txs)
	if bytes.Compare(txids[1][:], txids[2][:]) >= 0 {
		t.Fatalf("fixture is not LTOR: child %x parent %x", txids[1], txids[2])
	}

	nextUTXOs := cloneUtxos(utxos)
	delete(nextUTXOs, prevOut)
	childTxID := txids[1]
	nextUTXOs[types.OutPoint{TxID: childTxID, Vout: 0}] = UtxoEntryFromOutputAtHeight(child.Base.Outputs[0], 1, false)
	coinbaseTxID := txids[0]
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = UtxoEntryFromOutputAtHeight(coinbase.Base.Outputs[0], 1, true)
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

	applied := cloneUtxos(utxos)
	summary, err := validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), applied, params, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("validate block: %v", err)
	}
	if got, want := summary.TotalFees, uint64(2); got != want {
		t.Fatalf("total fees = %d, want %d", got, want)
	}
	if _, ok := applied[types.OutPoint{TxID: parentTxID, Vout: 0}]; ok {
		t.Fatal("parent output should be spent by child even though parent serializes later")
	}
	if got, want := applied, nextUTXOs; !equalUtxoSets(got, want) {
		t.Fatalf("post-block utxos mismatch: got %v want %v", got, want)
	}

	acc, err := UtxoAccumulator(utxos)
	if err != nil {
		t.Fatalf("pre-block accumulator: %v", err)
	}
	accApplied := cloneUtxos(utxos)
	_, nextAcc, err := validateAndApplyBlockWithAccumulatorForTest(t, &block, prev, NewBlockSizeState(params), accApplied, acc, params, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("validate block with accumulator: %v", err)
	}
	if nextAcc.Root() != block.Header.UTXORoot {
		t.Fatalf("next accumulator root = %x, want %x", nextAcc.Root(), block.Header.UTXORoot)
	}
	delta, err := ValidateAndApplyBlockDeltaWithLookup(
		&block,
		prev,
		NewBlockSizeState(params),
		utxos,
		LookupWithErrFromSet(utxos),
		acc,
		params,
		DefaultConsensusRules(),
	)
	if err != nil {
		t.Fatalf("validate detailed block delta: %v", err)
	}
	if len(delta.SpentPreBlock) != 1 || delta.SpentPreBlock[0].OutPoint != prevOut {
		t.Fatalf("pre-block spends = %+v, want only %v", delta.SpentPreBlock, prevOut)
	}
	resolved, err := ResolveBlockInputEntries(&block, delta.SpentPreBlock)
	if err != nil {
		t.Fatalf("resolve block inputs: %v", err)
	}
	if _, ok := resolved[types.OutPoint{TxID: parentTxID, Vout: 0}]; !ok {
		t.Fatal("same-block parent output was not resolved for child-before-parent LTOR")
	}
}

func TestValidateAndApplyBlockRejectsInvalidSignatureAcrossBatch(t *testing.T) {
	params := RegtestParams()
	firstPrev := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	secondPrev := types.OutPoint{TxID: [32]byte{2}, Vout: 0}
	utxos := UtxoSet{
		firstPrev:  {ValueAtoms: 50, PubKey: consensusTestPubKey(1)},
		secondPrev: {ValueAtoms: 50, PubKey: consensusTestPubKey(2)},
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
	if bytes.Compare(txAID[:], txBID[:]) >= 0 {
		ordered = []types.Transaction{txB, txA}
	}
	ordered[1].Auth.Entries[0].Signature[0] ^= 0xff

	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 2, PubKey: consensusTestPubKey(9)}})
	txs := append([]types.Transaction{coinbase}, ordered...)
	txids, _, txRoot, authRoot := BuildBlockRoots(txs)

	nextUTXOs := cloneUtxos(utxos)
	delete(nextUTXOs, firstPrev)
	delete(nextUTXOs, secondPrev)
	for i := 1; i < len(txs); i++ {
		txid := txids[i]
		nextUTXOs[types.OutPoint{TxID: txid, Vout: 0}] = UtxoEntry{
			ValueAtoms: txs[i].Base.Outputs[0].ValueAtoms,
			PubKey:     txs[i].Base.Outputs[0].PubKey,
		}
	}
	coinbaseTxID := txids[0]
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = UtxoEntry{
		ValueAtoms: coinbase.Base.Outputs[0].ValueAtoms,
		PubKey:     coinbase.Base.Outputs[0].PubKey,
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

	_, err = validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, DefaultConsensusRules())
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected invalid signature, got %v", err)
	}
}

func TestValidateAndApplyBlockSkipsCachedValidatedAuth(t *testing.T) {
	params := RegtestParams()
	prevOut := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	utxos := UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: consensusTestPubKey(1)},
	}
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	spend := signedSpendTxForConsensusTest(t, 1, prevOut, 50, 2, 1)
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(9)}})
	txs := []types.Transaction{coinbase, spend}
	txids, authids, txRoot, authRoot := BuildBlockRoots(txs)

	nextUTXOs := cloneUtxos(utxos)
	delete(nextUTXOs, prevOut)
	nextUTXOs[types.OutPoint{TxID: txids[1], Vout: 0}] = UtxoEntry{
		ValueAtoms: spend.Base.Outputs[0].ValueAtoms,
		PubKey:     spend.Base.Outputs[0].PubKey,
	}
	nextUTXOs[types.OutPoint{TxID: txids[0], Vout: 0}] = UtxoEntry{
		ValueAtoms: coinbase.Base.Outputs[0].ValueAtoms,
		PubKey:     coinbase.Base.Outputs[0].PubKey,
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

	uncached, err := validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("uncached validate block: %v", err)
	}
	if got, want := uncached.SignatureChecks, 1; got != want {
		t.Fatalf("uncached signature checks = %d, want %d", got, want)
	}

	rules := DefaultConsensusRules()
	rules.ValidatedAuthCache = func(txid, authid [32]byte, gotParams ChainParams) bool {
		return gotParams.SighashTag() == params.SighashTag() && txid == txids[1] && authid == authids[1]
	}
	cached, err := validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, rules)
	if err != nil {
		t.Fatalf("cached validate block: %v", err)
	}
	if got, want := cached.SignatureChecks, 0; got != want {
		t.Fatalf("cached signature checks = %d, want %d", got, want)
	}
}

func TestValidateAndApplyBlockRejectsCoinbaseHeightMismatch(t *testing.T) {
	params := RegtestParams()
	prevHeader := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	prev := PrevBlockContext{Height: 0, Header: prevHeader}
	blockTx := coinbaseTxForConsensusTest(2, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(3)}})
	blockTxID := TxID(&blockTx)
	nextUTXOs := UtxoSet{
		types.OutPoint{TxID: blockTxID, Vout: 0}: {ValueAtoms: 1, PubKey: consensusTestPubKey(3)},
	}
	nbits, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  HeaderHash(&prevHeader),
			MerkleTxIDRoot: specMerkleRootForTest([][32]byte{blockTxID}),
			MerkleAuthRoot: specMerkleRootForTest([][32]byte{AuthID(&blockTx)}),
			UTXORoot:       ComputedUTXORoot(nextUTXOs),
			Timestamp:      prevHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: []types.Transaction{blockTx},
	}
	block.Header = mineHeaderForTest(block.Header)

	_, err = validateAndApplyBlockForTest(t, &block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
	if !errors.Is(err, ErrCoinbaseHeightInvalid) {
		t.Fatalf("expected coinbase height error, got %v", err)
	}
}

func TestValidateTxRejectsDuplicateInputs(t *testing.T) {
	prevOut := types.OutPoint{TxID: [32]byte{7}, Vout: 0}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{
				{PrevOut: prevOut},
				{PrevOut: prevOut},
			},
			Outputs: []types.TxOutput{{ValueAtoms: 10, PubKey: consensusTestPubKey(9)}},
		},
		Auth: types.TxAuth{Entries: []types.TxAuthEntry{
			{Signature: [64]byte{1}},
			{Signature: [64]byte{2}},
		}},
	}
	_, err := ValidateTx(&tx, UtxoSet{
		prevOut: {ValueAtoms: 20, PubKey: consensusTestPubKey(7)},
	}, TxValidationContext{Params: MainnetParams()}, DefaultConsensusRules())
	if !errors.Is(err, ErrDuplicateInput) {
		t.Fatalf("expected duplicate input error, got %v", err)
	}
}
