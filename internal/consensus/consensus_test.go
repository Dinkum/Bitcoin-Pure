package consensus

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"runtime"
	"testing"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func consensusTestPubKey(seed byte) [32]byte {
	return crypto.XOnlyPubKeyFromSecret([32]byte{seed})
}

func coinbaseTxForConsensusTest(height uint64, outputs []types.TxOutput) types.Transaction {
	var extraNonce [types.CoinbaseExtraNonceLen]byte
	return types.Transaction{
		Base: types.TxBase{
			Version:            1,
			CoinbaseHeight:     &height,
			CoinbaseExtraNonce: &extraNonce,
			Outputs:            outputs,
		},
	}
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
			Outputs: []types.TxOutput{{ValueAtoms: value - fee, PubKey: consensusTestPubKey(recipientSeed)}},
		},
	}
	msg, err := Sighash(&tx, 0, []UtxoEntry{{ValueAtoms: value, PubKey: consensusTestPubKey(spenderSeed)}})
	if err != nil {
		t.Fatal(err)
	}
	_, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	return tx
}

func specSighashForTest(tx *types.Transaction, inputIndex int, spentCoins []UtxoEntry) ([32]byte, error) {
	if inputIndex < 0 || inputIndex >= len(tx.Base.Inputs) {
		return [32]byte{}, errors.New("input index out of range")
	}
	if len(spentCoins) != len(tx.Base.Inputs) {
		return [32]byte{}, errors.New("spent coins length mismatch")
	}

	prevouts := make([]byte, 0)
	writeVarInt(&prevouts, uint64(len(tx.Base.Inputs)))
	for _, input := range tx.Base.Inputs {
		prevouts = append(prevouts, input.PrevOut.TxID[:]...)
		prevouts = append(prevouts,
			byte(input.PrevOut.Vout),
			byte(input.PrevOut.Vout>>8),
			byte(input.PrevOut.Vout>>16),
			byte(input.PrevOut.Vout>>24),
		)
	}

	outputs := make([]byte, 0)
	writeVarInt(&outputs, uint64(len(tx.Base.Outputs)))
	for _, output := range tx.Base.Outputs {
		outputs = appendValuePubKeyEncoding(outputs, output.ValueAtoms, output.PubKey)
	}

	spentCoinPayload := make([]byte, 0)
	writeVarInt(&spentCoinPayload, uint64(len(spentCoins)))
	for _, coin := range spentCoins {
		spentCoinPayload = appendValuePubKeyEncoding(spentCoinPayload, coin.ValueAtoms, coin.PubKey)
	}

	preimage := make([]byte, 0, 108)
	preimage = append(preimage,
		byte(tx.Base.Version),
		byte(tx.Base.Version>>8),
		byte(tx.Base.Version>>16),
		byte(tx.Base.Version>>24),
	)
	index := uint64(inputIndex)
	preimage = append(preimage,
		byte(index),
		byte(index>>8),
		byte(index>>16),
		byte(index>>24),
		byte(index>>32),
		byte(index>>40),
		byte(index>>48),
		byte(index>>56),
	)
	prevoutsHash := crypto.Sha256d(prevouts)
	outputsHash := crypto.Sha256d(outputs)
	spentCoinsHash := crypto.Sha256d(spentCoinPayload)
	preimage = append(preimage, prevoutsHash[:]...)
	preimage = append(preimage, outputsHash[:]...)
	preimage = append(preimage, spentCoinsHash[:]...)
	return crypto.TaggedHash(SighashTag, preimage), nil
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
				PubKey:     consensusTestPubKey(9),
			}},
		},
		Auth: types.TxAuth{
			Entries: []types.TxAuthEntry{{
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

func taggedMerkleLeafForTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x00
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

func taggedMerkleNodeForTest(left, right [32]byte) [32]byte {
	var buf [65]byte
	buf[0] = 0x01
	copy(buf[1:33], left[:])
	copy(buf[33:], right[:])
	return crypto.Sha256d(buf[:])
}

func taggedMerkleSoloForTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x02
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

// specMerkleRootForTest mirrors the spec's leaf/node/solo rules so these
// tests don't derive their expectations from the production MerkleRoot code.
func specMerkleRootForTest(items [][32]byte) [32]byte {
	if len(items) == 0 {
		panic("specMerkleRootForTest requires at least one item")
	}
	level := make([][32]byte, len(items))
	for i, item := range items {
		level[i] = taggedMerkleLeafForTest(item)
	}
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, taggedMerkleSoloForTest(level[i]))
				continue
			}
			next = append(next, taggedMerkleNodeForTest(level[i], level[i+1]))
		}
		level = next
	}
	return level[0]
}

func equalUtxoSets(left, right UtxoSet) bool {
	if len(left) != len(right) {
		return false
	}
	for outPoint, entry := range left {
		if other, ok := right[outPoint]; !ok || other != entry {
			return false
		}
	}
	return true
}

func TestMerkleRootRequiresNonEmptyLeafSet(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected merkle root to panic on empty input")
		}
	}()
	MerkleRoot(nil)
}

func TestMerkleRootUsesTaggedLeafNodeAndSoloHashes(t *testing.T) {
	items := [][32]byte{
		{0x11},
		{0x22},
		{0x33},
	}

	leaf0 := taggedMerkleLeafForTest(items[0])
	leaf1 := taggedMerkleLeafForTest(items[1])
	leaf2 := taggedMerkleLeafForTest(items[2])

	if got, want := MerkleRoot(items[:1]), leaf0; got != want {
		t.Fatalf("single-leaf merkle root = %x, want %x", got, want)
	}

	pairRoot := taggedMerkleNodeForTest(leaf0, leaf1)
	if got, want := MerkleRoot(items[:2]), pairRoot; got != want {
		t.Fatalf("pair merkle root = %x, want %x", got, want)
	}

	oddRoot := taggedMerkleNodeForTest(pairRoot, taggedMerkleSoloForTest(leaf2))
	if got, want := MerkleRoot(items), oddRoot; got != want {
		t.Fatalf("odd-leaf merkle root = %x, want %x", got, want)
	}
}

func TestMerkleRootParallelMatchesSequential(t *testing.T) {
	items := make([][32]byte, 513)
	for i := range items {
		items[i][0] = byte(i)
		items[i][1] = byte(i >> 8)
	}
	want := specMerkleRootForTest(items)
	if got := MerkleRoot(items); got != want {
		t.Fatalf("sequential merkle root = %x, want %x", got, want)
	}
	if got := MerkleRootParallel(items); got != want {
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
					PubKey:     consensusTestPubKey(byte(i + 2)),
				}},
			},
			Auth: types.TxAuth{
				Entries: []types.TxAuthEntry{{
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
	if txRoot != specMerkleRootForTest(directTxIDs) {
		t.Fatalf("tx root mismatch")
	}
	if authRoot != specMerkleRootForTest(directAuthIDs) {
		t.Fatalf("auth root mismatch")
	}
}

func TestBuildBlockRootsFromIDsMatchesBuildBlockRoots(t *testing.T) {
	coinbase := coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 50, PubKey: consensusTestPubKey(1)}})
	first := signedSpendTxForConsensusTest(t, 1, types.OutPoint{TxID: TxID(&coinbase), Vout: 0}, 50, 2, 1)
	second := signedSpendTxForConsensusTest(t, 2, types.OutPoint{TxID: [32]byte{9}, Vout: 0}, 25, 3, 1)
	txs := []types.Transaction{coinbase, first, second}

	txids, authids, txRoot, authRoot := BuildBlockRoots(txs)
	reusedTxRoot, reusedAuthRoot := BuildBlockRootsFromIDs(txids, authids)
	if reusedTxRoot != txRoot {
		t.Fatalf("tx root mismatch: got %x want %x", reusedTxRoot, txRoot)
	}
	if reusedAuthRoot != authRoot {
		t.Fatalf("auth root mismatch: got %x want %x", reusedAuthRoot, authRoot)
	}
}

func TestMedianTimePastUsesMedianOfLastElevenTimestamps(t *testing.T) {
	timestamps := []uint64{100, 70, 90, 110, 80, 60, 120, 50, 130, 40, 140}
	if got, want := MedianTimePast(timestamps), uint64(90); got != want {
		t.Fatalf("median time past = %d, want %d", got, want)
	}
}

func TestMedianTimePastHandlesLargerInputs(t *testing.T) {
	timestamps := []uint64{15, 1, 8, 13, 3, 21, 5, 2, 34, 55, 89, 144, 233}
	if got, want := MedianTimePast(timestamps), uint64(15); got != want {
		t.Fatalf("median time past = %d, want %d", got, want)
	}
}

func BenchmarkMedianTimePast11(b *testing.B) {
	timestamps := []uint64{100, 70, 90, 110, 80, 60, 120, 50, 130, 40, 140}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MedianTimePast(timestamps)
	}
}

func TestValidateSignedSpend(t *testing.T) {
	seed := [32]byte{7}
	msgPub, _ := crypto.SignSchnorrForTest(seed, &[32]byte{})
	utxos := UtxoSet{
		types.OutPoint{TxID: [32]byte{2}, Vout: 0}: {
			ValueAtoms: 50,
			PubKey:     msgPub,
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
				PubKey:     consensusTestPubKey(8),
			}},
		},
	}
	msg, err := Sighash(&tx, 0, []UtxoEntry{{ValueAtoms: 50, PubKey: msgPub}})
	if err != nil {
		t.Fatal(err)
	}
	_, sig := crypto.SignSchnorrForTest(seed, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	summary, err := ValidateTx(&tx, utxos, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("validate tx: %v", err)
	}
	if summary.Fee != 10 {
		t.Fatalf("unexpected fee: %d", summary.Fee)
	}
}

func TestSighashMatchesSpecForMultiInputSpend(t *testing.T) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{
				{PrevOut: types.OutPoint{TxID: [32]byte{1}, Vout: 0}},
				{PrevOut: types.OutPoint{TxID: [32]byte{2}, Vout: 1}},
			},
			Outputs: []types.TxOutput{
				{ValueAtoms: 60, PubKey: consensusTestPubKey(8)},
				{ValueAtoms: 15, PubKey: consensusTestPubKey(9)},
			},
		},
	}
	spentCoins := []UtxoEntry{
		{ValueAtoms: 50, PubKey: consensusTestPubKey(1)},
		{ValueAtoms: 30, PubKey: consensusTestPubKey(2)},
	}

	for i := range tx.Base.Inputs {
		got, err := Sighash(&tx, i, spentCoins)
		if err != nil {
			t.Fatalf("sighash input %d: %v", i, err)
		}
		want, err := specSighashForTest(&tx, i, spentCoins)
		if err != nil {
			t.Fatalf("spec sighash input %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("sighash input %d = %x, want %x", i, got, want)
		}
	}
}

func TestSighashCommitsToSpentCoinPubKey(t *testing.T) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{{
				PrevOut: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
			}},
			Outputs: []types.TxOutput{{
				ValueAtoms: 49,
				PubKey:     consensusTestPubKey(9),
			}},
		},
	}
	left, err := Sighash(&tx, 0, []UtxoEntry{{ValueAtoms: 50, PubKey: consensusTestPubKey(1)}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := Sighash(&tx, 0, []UtxoEntry{{ValueAtoms: 50, PubKey: consensusTestPubKey(2)}})
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("expected sighash to change when spent coin pubkey changes")
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
	mined, ok, err := MineHeaderInterruptible(header, params, func(uint64) bool { return false })
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
		a: {ValueAtoms: 10, PubKey: consensusTestPubKey(1)},
		b: {ValueAtoms: 20, PubKey: consensusTestPubKey(2)},
	}
	right := UtxoSet{
		b: {ValueAtoms: 20, PubKey: consensusTestPubKey(2)},
		a: {ValueAtoms: 10, PubKey: consensusTestPubKey(1)},
	}
	if ComputedUTXORoot(left) != ComputedUTXORoot(right) {
		t.Fatal("utxo root should be order-invariant")
	}
}

func TestComputedUTXORootMatchesAccumulatorRoot(t *testing.T) {
	utxos := UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: consensusTestPubKey(1)},
		types.OutPoint{TxID: [32]byte{2}, Vout: 1}: {ValueAtoms: 20, PubKey: consensusTestPubKey(2)},
		types.OutPoint{TxID: [32]byte{3}, Vout: 2}: {ValueAtoms: 30, PubKey: consensusTestPubKey(3)},
	}
	acc, err := UtxoAccumulator(utxos)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ComputedUTXORoot(utxos), acc.Root(); got != want {
		t.Fatalf("utxo root = %x, want %x", got, want)
	}
}

func TestComputedUTXORootFromOverlayMatchesMaterializedSet(t *testing.T) {
	base := UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: consensusTestPubKey(1)},
		types.OutPoint{TxID: [32]byte{2}, Vout: 1}: {ValueAtoms: 20, PubKey: consensusTestPubKey(2)},
		types.OutPoint{TxID: [32]byte{3}, Vout: 2}: {ValueAtoms: 30, PubKey: consensusTestPubKey(3)},
	}
	overlay := NewUtxoOverlay(base)
	overlay.Spend(types.OutPoint{TxID: [32]byte{1}, Vout: 0})
	overlay.Set(types.OutPoint{TxID: [32]byte{2}, Vout: 1}, UtxoEntry{ValueAtoms: 25, PubKey: consensusTestPubKey(4)})
	overlay.Set(types.OutPoint{TxID: [32]byte{9}, Vout: 0}, UtxoEntry{ValueAtoms: 99, PubKey: consensusTestPubKey(5)})

	got := computedUTXORootFromOverlay(overlay)
	want := ComputedUTXORoot(overlay.Materialize())
	if got != want {
		t.Fatalf("overlay utxo root = %x, want %x", got, want)
	}
}

func TestUtxoOverlayRecordsFirstLookupError(t *testing.T) {
	lookupErr := errors.New("disk read failed")
	overlay := NewUtxoOverlayWithLookup(func(types.OutPoint) (UtxoEntry, bool, error) {
		return UtxoEntry{}, false, lookupErr
	})

	if _, ok := overlay.Lookup(types.OutPoint{TxID: [32]byte{7}, Vout: 1}); ok {
		t.Fatal("unexpected lookup hit")
	}
	if err := overlay.Err(); !errors.Is(err, lookupErr) {
		t.Fatalf("overlay.Err() = %v, want %v", err, lookupErr)
	}
}

func compactTargetForTest(compact uint32) *big.Int {
	target, err := compactToTarget(compact)
	if err != nil {
		panic(err)
	}
	return target
}

func referenceAsertBits(t *testing.T, anchor AsertAnchor, prev PrevBlockContext, params ChainParams) uint32 {
	t.Helper()
	anchorTarget, err := compactToTarget(anchor.Bits)
	if err != nil {
		t.Fatal(err)
	}
	powLimit, err := compactToTarget(params.PowLimitBits)
	if err != nil {
		t.Fatal(err)
	}

	timeDelta := int64(prev.Header.Timestamp) - anchor.ParentTime
	heightDelta := int64(prev.Height - anchor.Height)
	exponentFP := ((timeDelta - params.TargetSpacingSecs*(heightDelta+1)) * (1 << 16)) / params.AsertHalfLifeSecs

	// Implement the required arithmetic shift explicitly so this helper stays
	// independent from the production fixed-point decomposition.
	numShifts := exponentFP / (1 << 16)
	if exponentFP < 0 && exponentFP%(1<<16) != 0 {
		numShifts--
	}
	frac := exponentFP - numShifts*(1<<16)

	poly := new(big.Int).Mul(big.NewInt(195766423245049), big.NewInt(frac))
	fracSquared := frac * frac
	fracCubed := fracSquared * frac
	poly.Add(poly, new(big.Int).Mul(big.NewInt(971821376), big.NewInt(fracSquared)))
	poly.Add(poly, new(big.Int).Mul(big.NewInt(5127), big.NewInt(fracCubed)))
	poly.Add(poly, new(big.Int).Lsh(big.NewInt(1), 47))
	poly.Rsh(poly, 48)
	poly.Add(poly, big.NewInt(1<<16))

	nextTarget := new(big.Int).Mul(new(big.Int).Set(anchorTarget), poly)
	if numShifts < 0 {
		nextTarget.Rsh(nextTarget, uint(-numShifts))
	} else if numShifts > 0 {
		nextTarget.Lsh(nextTarget, uint(numShifts))
	}
	nextTarget.Rsh(nextTarget, 16)

	if nextTarget.Sign() <= 0 {
		bits, err := targetToCompact(big.NewInt(1))
		if err != nil {
			t.Fatal(err)
		}
		return bits
	}
	if nextTarget.Cmp(powLimit) > 0 {
		return params.PowLimitBits
	}
	bits, err := targetToCompact(nextTarget)
	if err != nil {
		t.Fatal(err)
	}
	return bits
}

func mineHeaderForTest(header types.BlockHeader) types.BlockHeader {
	target := compactTargetForTest(header.NBits)
	for nonce := uint64(0); ; nonce++ {
		header.Nonce = nonce
		hash := HeaderHash(&header)
		if new(big.Int).SetBytes(hash[:]).Cmp(target) <= 0 {
			return header
		}
		if nonce == math.MaxUint64 {
			break
		}
	}
	panic("unable to mine header")
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
			Base: coinbaseTxForConsensusTest(1, []types.TxOutput{{ValueAtoms: 1, PubKey: consensusTestPubKey(1)}}).Base,
		}},
	}
	txid := TxID(&block.Txs[0])
	block.Header.MerkleTxIDRoot = specMerkleRootForTest([][32]byte{txid})
	block.Header.MerkleAuthRoot = specMerkleRootForTest([][32]byte{AuthID(&block.Txs[0])})
	block.Header = mineHeaderForTest(block.Header)
	_, err := ValidateAndApplyBlock(&block, PrevBlockContext{Height: 0, Header: genesis}, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
	if !errors.Is(err, ErrUTXORootMismatch) {
		t.Fatalf("expected utxo root mismatch, got %v", err)
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

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
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

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
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

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
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

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, DefaultConsensusRules())
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
	nextUTXOs[types.OutPoint{TxID: childTxID, Vout: 0}] = UtxoEntry{
		ValueAtoms: child.Base.Outputs[0].ValueAtoms,
		PubKey:     child.Base.Outputs[0].PubKey,
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

	applied := cloneUtxos(utxos)
	summary, err := ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), applied, params, DefaultConsensusRules())
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

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), cloneUtxos(utxos), params, DefaultConsensusRules())
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected invalid signature, got %v", err)
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

	_, err = ValidateAndApplyBlock(&block, prev, NewBlockSizeState(params), UtxoSet{}, params, DefaultConsensusRules())
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
	}, DefaultConsensusRules())
	if !errors.Is(err, ErrDuplicateInput) {
		t.Fatalf("expected duplicate input error, got %v", err)
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

func TestValidateHeaderRejectsTimestampAtOrBelowMedianTimePast(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     100,
		NBits:         params.GenesisBits,
	}
	err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: 100,
	}, params)
	if !errors.Is(err, ErrTimestampTooEarly) {
		t.Fatalf("expected timestamp-too-early error, got %v", err)
	}
}

func TestValidateHeaderAcceptsTimestampAboveMedianTimePast(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     101,
		NBits:         params.GenesisBits,
	}
	header = mineHeaderForTest(header)
	if err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: 100,
		CurrentTime:    101,
	}, params); err != nil {
		t.Fatalf("ValidateHeader: %v", err)
	}
}

func TestValidateHeaderRejectsTimestampBeyondLocalSystemTimeWindow(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	currentTime := params.GenesisTimestamp + 100
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     currentTime + MaxFutureBlockTimeSeconds + 1,
		NBits:         params.GenesisBits,
	}
	err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: params.GenesisTimestamp,
		CurrentTime:    currentTime,
	}, params)
	if !errors.Is(err, ErrTimestampTooFarFuture) {
		t.Fatalf("expected timestamp-too-far-future error, got %v", err)
	}
}

func TestValidateHeaderAcceptsTimestampAtLocalSystemTimeWindowBoundary(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	currentTime := params.GenesisTimestamp + 100
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     currentTime + MaxFutureBlockTimeSeconds,
		NBits:         params.GenesisBits,
	}
	header = mineHeaderForTest(header)
	if err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: params.GenesisTimestamp,
		CurrentTime:    currentTime,
	}, params); err != nil {
		t.Fatalf("ValidateHeader: %v", err)
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

func TestNextWorkRequiredASERTMatchesReferenceCases(t *testing.T) {
	params := RegtestParams()
	anchor := GenesisAsertAnchor(params)
	cases := []struct {
		name      string
		height    uint64
		timestamp uint64
	}{
		{name: "block1 on schedule", height: 0, timestamp: params.GenesisTimestamp},
		{name: "block1 early", height: 0, timestamp: params.GenesisTimestamp - 300},
		{name: "block1 late", height: 0, timestamp: params.GenesisTimestamp + 3600},
		{name: "several blocks on schedule", height: 143, timestamp: params.GenesisTimestamp + 143*600},
		{name: "large positive delta", height: 500, timestamp: params.GenesisTimestamp + 500*600 + 14*86400},
		{name: "large negative delta", height: 500, timestamp: params.GenesisTimestamp + 500*600 - 12*3600},
		{name: "overflow saturates", height: 20_000, timestamp: params.GenesisTimestamp + 20_000*600 + 10*365*86400},
		{name: "underflow clamps to one", height: 20_000, timestamp: params.GenesisTimestamp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prev := PrevBlockContext{
				Height: tc.height,
				Header: types.BlockHeader{
					Timestamp: tc.timestamp,
					NBits:     params.GenesisBits,
				},
			}
			got, err := NextWorkRequiredASERT(anchor, prev, params)
			if err != nil {
				t.Fatal(err)
			}
			want := referenceAsertBits(t, anchor, prev, params)
			if got != want {
				t.Fatalf("bits = 0x%08x, want 0x%08x", got, want)
			}
		})
	}
}

func BenchmarkNextWorkRequiredASERT(b *testing.B) {
	params := MainnetParams()
	anchor := GenesisAsertAnchor(params)
	prev := PrevBlockContext{
		Height: 50_000,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp + uint64(params.TargetSpacingSecs*50_000+17),
			NBits:     params.GenesisBits,
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := NextWorkRequiredASERT(anchor, prev, params); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMineHeaderInterruptible4096(b *testing.B) {
	params := RegtestParams()
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  [32]byte{1},
		MerkleTxIDRoot: [32]byte{2},
		MerkleAuthRoot: [32]byte{3},
		UTXORoot:       [32]byte{4},
		Timestamp:      params.GenesisTimestamp + uint64(params.TargetSpacingSecs),
		NBits:          0x1b0404cb,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := MineHeaderInterruptible(header, params, func(nonce uint64) bool { return nonce < 4096 })
		if err != nil {
			b.Fatal(err)
		}
		if ok {
			b.Fatal("expected benchmark header to stop before finding pow")
		}
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
