package consensus

import (
	"bitcoin-pure/internal/types"
	"errors"
	"reflect"
	"testing"
)

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

func TestUtxoOverlayApplyToSetMatchesMaterialize(t *testing.T) {
	base := UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: consensusTestPubKey(1)},
		types.OutPoint{TxID: [32]byte{2}, Vout: 1}: {ValueAtoms: 20, PubKey: consensusTestPubKey(2)},
		types.OutPoint{TxID: [32]byte{3}, Vout: 2}: {ValueAtoms: 30, PubKey: consensusTestPubKey(3)},
	}
	overlay := NewUtxoOverlay(base)
	overlay.Spend(types.OutPoint{TxID: [32]byte{1}, Vout: 0})
	overlay.Set(types.OutPoint{TxID: [32]byte{2}, Vout: 1}, UtxoEntry{ValueAtoms: 25, PubKey: consensusTestPubKey(4)})
	overlay.Set(types.OutPoint{TxID: [32]byte{9}, Vout: 0}, UtxoEntry{ValueAtoms: 99, PubKey: consensusTestPubKey(5)})

	got := cloneUtxos(base)
	overlay.ApplyToSet(got)
	want := overlay.Materialize()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ApplyToSet result = %+v, want %+v", got, want)
	}
}
