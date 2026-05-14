package consensus

import (
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"bytes"
	"encoding/hex"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
)

func TestConsensusBootstrapVectorsMatchGenesisFixtures(t *testing.T) {
	var vectors bootstrapVectors
	readJSONForConsensusTest(t, filepath.Join("..", "..", "fixtures", "vectors", "consensus_bootstrap_vectors.json"), &vectors)
	if vectors.VectorsVersion != 1 {
		t.Fatalf("vectors version = %d, want 1", vectors.VectorsVersion)
	}
	if got := MainnetParams().SighashTag(); got != vectors.SighashTags[types.Mainnet.String()] {
		t.Fatalf("mainnet sighash tag = %q, want %q", got, vectors.SighashTags[types.Mainnet.String()])
	}
	if got := RegtestParams().SighashTag(); got != vectors.SighashTags[types.Regtest.String()] {
		t.Fatalf("regtest sighash tag = %q, want %q", got, vectors.SighashTags[types.Regtest.String()])
	}
	if vectors.UTXORootTag != utreexo.UTXORootTag {
		t.Fatalf("utxo root tag = %q, want %q", vectors.UTXORootTag, utreexo.UTXORootTag)
	}
	if vectors.UTXOLeafTag != utreexo.UTXOLeafTag {
		t.Fatalf("utxo leaf tag = %q, want %q", vectors.UTXOLeafTag, utreexo.UTXOLeafTag)
	}
	if vectors.UTXOBranchTag != utreexo.UTXOBranchTag {
		t.Fatalf("utxo branch tag = %q, want %q", vectors.UTXOBranchTag, utreexo.UTXOBranchTag)
	}
	emptyRoot := ComputedUTXORoot(UtxoSet{})
	if got := hex.EncodeToString(emptyRoot[:]); got != vectors.EmptyUTXORootHex {
		t.Fatalf("empty utxo root = %s, want %s", got, vectors.EmptyUTXORootHex)
	}
	assertGenesisVector(t, filepath.Join("..", "..", "fixtures", "genesis", "mainnet.json"), vectors.Mainnet)
	assertGenesisVector(t, filepath.Join("..", "..", "fixtures", "genesis", "regtest.json"), vectors.Regtest)
}

func TestParsePQAuthPayloadUsesSpecFixedLayout(t *testing.T) {
	verificationKey := bytes.Repeat([]byte{0x11}, crypto.MLDSA65VerificationKeySize)
	signature := bytes.Repeat([]byte{0x22}, crypto.MLDSA65SignatureSize)
	payload := append(append([]byte(nil), verificationKey...), signature...)

	parsed, err := parsePQAuthPayload(payload)
	if err != nil {
		t.Fatalf("parsePQAuthPayload: %v", err)
	}
	if !bytes.Equal(parsed.VerificationKey, verificationKey) {
		t.Fatal("verification key parsed from the wrong payload segment")
	}
	if !bytes.Equal(parsed.Signature, signature) {
		t.Fatal("signature parsed from the wrong payload segment")
	}

	if _, err := parsePQAuthPayload(payload[:len(payload)-1]); !errors.Is(err, ErrInvalidAuthPayload) {
		t.Fatalf("short payload err = %v, want ErrInvalidAuthPayload", err)
	}
	if _, err := parsePQAuthPayload(append(payload, 0x00)); !errors.Is(err, ErrInvalidAuthPayload) {
		t.Fatalf("long payload err = %v, want ErrInvalidAuthPayload", err)
	}

	legacyFramed := make([]byte, 0, len(payload)+8)
	legacyFramed = types.AppendCanonicalVarInt(legacyFramed, types.AlgMLDSA65)
	legacyFramed = types.AppendCanonicalVarInt(legacyFramed, uint64(len(verificationKey)))
	legacyFramed = append(legacyFramed, verificationKey...)
	legacyFramed = types.AppendCanonicalVarInt(legacyFramed, uint64(len(signature)))
	legacyFramed = append(legacyFramed, signature...)
	if _, err := parsePQAuthPayload(legacyFramed); !errors.Is(err, ErrInvalidAuthPayload) {
		t.Fatalf("legacy framed payload err = %v, want ErrInvalidAuthPayload", err)
	}
}

func TestTypedUTXOEntryAndLeafPreservePayloads(t *testing.T) {
	xonlyPayload := consensusTestPubKey(4)
	xonly := UtxoEntryFromOutput(types.NewXOnlyOutput(50, xonlyPayload))
	if xonly.Type != types.OutputXOnlyP2PK || xonly.Payload32 != xonlyPayload || xonly.PubKey != xonlyPayload {
		t.Fatalf("x-only entry not fully typed: %+v", xonly)
	}
	xonlyLeaf := UtxoLeafFromEntry(types.OutPoint{TxID: [32]byte{1}, Vout: 0}, xonly)
	if roundTrip := UtxoEntryFromLeaf(xonlyLeaf); roundTrip != xonly {
		t.Fatalf("x-only leaf round trip = %+v, want %+v", roundTrip, xonly)
	}

	pqLock := crypto.TaggedHash("test-pq-lock", []byte("vk"))
	pq := UtxoEntryFromOutput(types.NewPQLockOutput(75, pqLock))
	if pq.Type != types.OutputPQLock32 || pq.Payload32 != pqLock || pq.PubKey != ([32]byte{}) {
		t.Fatalf("PQ entry not fully typed: %+v", pq)
	}
	pqLeaf := UtxoLeafFromOutput(types.OutPoint{TxID: [32]byte{2}, Vout: 1}, types.NewPQLockOutput(75, pqLock))
	if pqLeaf.Type != types.OutputPQLock32 || pqLeaf.Payload32 != pqLock || pqLeaf.PubKey != ([32]byte{}) {
		t.Fatalf("PQ leaf not fully typed: %+v", pqLeaf)
	}
}

func TestVerifyBlockSignatureChecksUsesExactVerifier(t *testing.T) {
	items := make([]crypto.SchnorrBatchItem, 0, 4)
	for i := 0; i < 4; i++ {
		msg := crypto.Sha256([]byte{byte(50 + i), byte(60 + i)})
		pubKey, sig := crypto.RandomSignSchnorrForTest(&msg)
		items = append(items, crypto.SchnorrBatchItem{
			PubKey:    pubKey,
			Signature: sig,
			Msg:       msg,
		})
	}

	result := verifyBlockSignatureChecks(items)
	if !result.Valid {
		t.Fatal("valid block signature checks unexpectedly failed")
	}
	if result.Fallback {
		t.Fatal("consensus exact verifier should not report batch fallback")
	}

	items[2].Signature[12] ^= 0x01
	result = verifyBlockSignatureChecks(items)
	if result.Valid {
		t.Fatal("tampered block signature checks unexpectedly verified")
	}
	if result.Fallback {
		t.Fatal("consensus exact verifier should not report batch fallback on failure")
	}
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
