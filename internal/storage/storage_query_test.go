package storage

import (
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestWalletActivityClosesCursorsOnInitializationError(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	block, utxos := sampleBlockAndUTXOs()
	if err := store.WriteFullState(&StoredChainState{Profile: types.Regtest, Height: 0, TipHeader: block.Header, BlockSizeState: sampleBlockSizeState(), UTXOs: utxos}); err != nil {
		t.Fatal(err)
	}
	items := []WalletWatchItem{{Type: 0, Payload32: [32]byte{1}}, {Type: 0, Payload32: [32]byte{2}}}
	if err := store.db.Set(metaWalletIndexHeightKey, encodeU64(0), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	record := WalletActivityRecord{Height: 0, TxID: [32]byte{3}}
	if err := store.db.Set(walletActivityItemKey(items[0], 0, record.TxID), encodeWalletActivityRecord(record), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Set(walletActivityItemKey(items[1], 0, record.TxID), []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_, err = store.WalletActivityByWatchItems(items, 10)
	if err == nil {
		t.Fatal("corruption accepted")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("iterator leaked: %v", err)
	}
}

func TestLocalitySelectionMatchesSortedPrefix(t *testing.T) {
	rng := rand.New(rand.NewPCG(4, 9))
	input := make([]LocalityIndexedUTXO, 513)
	for i := range input {
		input[i] = LocalityIndexedUTXO{Sequence: uint64(rng.IntN(1024)), OutPoint: types.OutPoint{Vout: uint32(i)}}
	}
	expected := slices.Clone(input)
	slices.SortFunc(expected, compareLocalityItems)
	for _, limit := range []int{-1, 0, 1, 2, 255, 256, 260, 513, 1000} {
		selected := make(localitySelectionHeap, 0)
		for _, item := range input {
			keepLocalityItem(&selected, item, limit)
		}
		slices.SortFunc(selected, compareLocalityItems)
		want := expected
		if limit > 0 && limit < len(want) {
			want = want[:limit]
		}
		if !slices.Equal([]LocalityIndexedUTXO(selected), want) {
			t.Fatalf("limit %d differs from sorted prefix", limit)
		}
	}
}
