package utxochecksum

import (
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestComputeIsOrderIndependent(t *testing.T) {
	left := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
	}
	right := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
	}
	if got, want := Compute(left), Compute(right); got != want {
		t.Fatalf("checksum mismatch: got %x want %x", got, want)
	}
}

func TestApplyDeltaMatchesFullRecompute(t *testing.T) {
	base := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
	}
	spent := map[types.OutPoint]consensus.UtxoEntry{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
	}
	created := map[types.OutPoint]consensus.UtxoEntry{
		types.OutPoint{TxID: [32]byte{9}, Vout: 2}: {ValueAtoms: 99, PubKey: [32]byte{8}},
	}
	next := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
		types.OutPoint{TxID: [32]byte{9}, Vout: 2}: {ValueAtoms: 99, PubKey: [32]byte{8}},
	}
	if got, want := ApplyDelta(Compute(base), spent, created), Compute(next); got != want {
		t.Fatalf("delta checksum mismatch: got %x want %x", got, want)
	}
}
