package utreexo

import (
	"testing"

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
}
