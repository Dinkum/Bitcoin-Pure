package compactfilter

import (
	"bitcoin-pure/internal/types"
	"encoding/binary"
	"reflect"
	"testing"
)

func batchMatchFixture() (Filter, []WatchItem) {
	block := &types.Block{Txs: []types.Transaction{{}}}
	for i := 0; i < 10000; i++ {
		var p [32]byte
		binary.LittleEndian.PutUint64(p[:], uint64(i))
		block.Txs[0].Base.Outputs = append(block.Txs[0].Base.Outputs, types.NewXOnlyOutput(1, p))
	}
	items := make([]WatchItem, 100)
	for i := range items {
		binary.LittleEndian.PutUint64(items[i].Payload32[:], uint64(i*201))
	}
	return Build([32]byte{1}, block, nil), items
}

func TestMatchWatchItemsMatchesIndividualQueries(t *testing.T) {
	f, items := batchMatchFixture()
	want := make([]bool, len(items))
	for i, item := range items {
		v, err := MatchWatchItem(f.BlockHash, f.Encoded, item)
		if err != nil {
			t.Fatal(err)
		}
		want[i] = v
	}
	got, err := MatchWatchItems(f.BlockHash, f.Encoded, items)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("batch mismatch", err)
	}
	bad := [][]byte{nil, {0x80, 0}, append(append([]byte{}, f.Encoded...), 1), f.Encoded[:len(f.Encoded)-1]}
	for _, x := range bad {
		_, e1 := MatchWatchItem(f.BlockHash, x, items[0])
		_, e2 := MatchWatchItems(f.BlockHash, x, items)
		if e1 == nil || e2 == nil {
			t.Fatal("malformed accepted")
		}
	}
}

func TestMatchWatchItemsEmptyDuplicateAndTypedRequests(t *testing.T) {
	item := WatchItem{Type: types.OutputXOnlyP2PK, Payload32: [32]byte{1}}
	typed := item
	typed.Type = types.OutputXOnlyP2PK + 1
	block := &types.Block{Txs: []types.Transaction{{Base: types.TxBase{Outputs: []types.TxOutput{types.NewXOnlyOutput(1, item.Payload32)}}}}}
	f := Build([32]byte{1}, block, nil)
	queries := []WatchItem{item, item, typed}
	got, err := MatchWatchItems(f.BlockHash, f.Encoded, queries)
	if err != nil || !reflect.DeepEqual(got, []bool{true, true, false}) {
		t.Fatalf("matches = %v, %v", got, err)
	}
	got, err = MatchWatchItems(f.BlockHash, f.Encoded, nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty batch = %v, %v", got, err)
	}
	for _, bad := range [][]byte{nil, {0x80, 0}, append(append([]byte{}, f.Encoded...), 1)} {
		if _, err := MatchWatchItems(f.BlockHash, bad, nil); err == nil {
			t.Fatal("empty query bypassed filter validation")
		}
	}
}
