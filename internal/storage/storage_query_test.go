package storage

import (
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
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

func TestLocalityMigrationPreservesSequenceOrder(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	block, utxos := sampleBlockAndUTXOs()
	if err := store.WriteFullState(&StoredChainState{Profile: types.Regtest, TipHeader: block.Header, BlockSizeState: sampleBlockSizeState(), UTXOs: utxos}); err != nil {
		t.Fatal(err)
	}
	batch := store.db.NewBatch()
	if err := batch.Set(metaChainstateSchemaVersionKey, encodeU64(chainstateSchemaV2), nil); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{256, 1, 255} {
		for outPoint, entry := range utxos {
			key := append(cloneBytes(legacyLocalitySeqPrefix), encodeU64(seq)...)
			if err := batch.Set(key, encodeLocalitySeqValue(outPoint, entry), nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	raw, err := store.get(metaChainstateSchemaVersionKey)
	if err != nil {
		t.Fatal(err)
	}
	if version, err := decodeU64(raw); err != nil || version != chainstateSchemaVersion {
		t.Fatalf("schema after locality migration=%d, %v", version, err)
	}
	rows, err := store.LoadLocalityOrderedUTXOs(2)
	if err != nil || len(rows) != 2 || rows[0].Sequence != 1 || rows[1].Sequence != 255 {
		t.Fatalf("migrated numeric prefix = %+v, %v", rows, err)
	}
}
