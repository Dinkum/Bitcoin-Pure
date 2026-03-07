package utreexo

import (
	"bytes"
	"testing"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func testLeaf(txidByte byte, vout uint32, value uint64, keyHashByte byte) UtxoLeaf {
	return UtxoLeaf{
		OutPoint: types.OutPoint{
			TxID: [32]byte{txidByte},
			Vout: vout,
		},
		ValueAtoms: value,
		KeyHash:    [32]byte{keyHashByte},
	}
}

func TestDeterministicUnderPermutation(t *testing.T) {
	a := testLeaf(1, 0, 10, 11)
	b := testLeaf(2, 1, 20, 22)
	c := testLeaf(3, 2, 30, 33)
	if UtxoRoot([]UtxoLeaf{a, b, c}) != UtxoRoot([]UtxoLeaf{c, a, b}) {
		t.Fatal("utxo root should be stable under permutation")
	}
}

func TestRootChangesWithData(t *testing.T) {
	a := testLeaf(1, 0, 10, 11)
	b := testLeaf(2, 1, 20, 22)
	if UtxoRoot([]UtxoLeaf{a, b}) == UtxoRoot([]UtxoLeaf{a, testLeaf(2, 1, 21, 22)}) {
		t.Fatal("utxo root should change")
	}
}

func TestEmptySetRootIsStable(t *testing.T) {
	if UtxoRoot(nil) != UtxoRoot(nil) {
		t.Fatal("empty root should be stable")
	}
	if got := UtxoRoot(nil); got != crypto.TaggedHash(UTXORootTag, nil) {
		t.Fatalf("empty root = %x, want %x", got, crypto.TaggedHash(UTXORootTag, nil))
	}
}

func TestSingleLeafRootWrapsLeafHash(t *testing.T) {
	leaf := testLeaf(9, 3, 90, 99)
	got := UtxoRoot([]UtxoLeaf{leaf})
	wantLeaf := LeafHash(leaf)
	want := crypto.TaggedHash(UTXORootTag, wantLeaf[:])
	if got != want {
		t.Fatalf("single leaf root = %x, want %x", got, want)
	}
}

func TestBranchHashDependsOnTrieSplit(t *testing.T) {
	left := testLeaf(0x10, 0, 10, 11)
	right := testLeaf(0x90, 1, 20, 22)
	root := UtxoRoot([]UtxoLeaf{left, right})
	leftHash := LeafHash(left)
	rightHash := LeafHash(right)
	branch := BranchHash(leftHash, rightHash)
	want := crypto.TaggedHash(UTXORootTag, branch[:])
	if root != want {
		t.Fatalf("two-leaf root = %x, want %x", root, want)
	}
}

func TestTrieUsesLexicalBitOrder(t *testing.T) {
	a := testLeaf(0x7f, 0, 10, 1)
	b := testLeaf(0x80, 0, 20, 2)
	rootAB := UtxoRoot([]UtxoLeaf{a, b})
	rootBA := UtxoRoot([]UtxoLeaf{b, a})
	if rootAB != rootBA {
		t.Fatal("bitwise trie ordering should remain permutation-invariant")
	}
	aHash := LeafHash(a)
	bHash := LeafHash(b)
	if bytes.Equal(aHash[:], bHash[:]) {
		t.Fatal("fixture leaves should not collide")
	}
}

func TestAccumulatorMatchesBulkRoot(t *testing.T) {
	leaves := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
		testLeaf(3, 2, 30, 33),
		testLeaf(4, 3, 40, 44),
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	if got, want := acc.Root(), UtxoRoot(leaves); got != want {
		t.Fatalf("accumulator root = %x, want %x", got, want)
	}
}

func TestAccumulatorApplyMatchesBulkRootAfterUpdates(t *testing.T) {
	original := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
		testLeaf(3, 2, 30, 33),
	}
	acc, err := NewAccumulatorFromLeaves(original)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	created := []UtxoLeaf{
		testLeaf(9, 0, 90, 99),
		testLeaf(10, 1, 100, 100),
	}
	next, err := acc.Apply(
		[]types.OutPoint{original[0].OutPoint, original[2].OutPoint},
		created,
	)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	wantLeaves := []UtxoLeaf{original[1], created[0], created[1]}
	if got, want := next.Root(), UtxoRoot(wantLeaves); got != want {
		t.Fatalf("updated accumulator root = %x, want %x", got, want)
	}
}

func TestAccumulatorRejectsMissingDelete(t *testing.T) {
	acc, err := NewAccumulatorFromLeaves([]UtxoLeaf{testLeaf(1, 0, 10, 11)})
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	if _, err := acc.Delete(types.OutPoint{TxID: [32]byte{9}, Vout: 0}); err == nil {
		t.Fatal("expected missing delete error")
	}
}
