package storage

import (
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func sampleBlockAndUTXOs() (types.Block, consensus.UtxoSet) {
	coinbase := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{ValueAtoms: 50, KeyHash: [32]byte{7}}},
		},
	}
	coinbaseTxID := consensus.TxID(&coinbase)
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: coinbaseTxID, Vout: 0}: {ValueAtoms: 50, KeyHash: [32]byte{7}},
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			MerkleTxIDRoot: consensus.MerkleRoot([][32]byte{coinbaseTxID}),
			MerkleAuthRoot: consensus.MerkleRoot([][32]byte{consensus.AuthID(&coinbase)}),
			UTXORoot:       consensus.ComputedUTXORoot(utxos),
			Timestamp:      1,
			NBits:          0x207fffff,
		},
		Txs: []types.Transaction{coinbase},
	}
	return block, utxos
}

func TestWriteAndLoadChainStateRoundtrip(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block, utxos := sampleBlockAndUTXOs()
	state := &StoredChainState{
		Profile:   types.Regtest,
		Height:    0,
		TipHeader: block.Header,
		BlockSizeState: consensus.BlockSizeState{
			Limit:            32_000_000,
			EWMA:             148,
			RecentBlockSizes: []uint64{148},
		},
		UTXOs: utxos,
	}
	if err := store.WriteFullState(state); err != nil {
		t.Fatal(err)
	}
	if err := store.PutBlock(0, &block); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadChainState()
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.Profile != types.Regtest || loaded.Height != 0 || len(loaded.UTXOs) != 1 {
		t.Fatal("loaded state mismatch")
	}
	hash := consensus.HeaderHash(&block.Header)
	gotBlock, err := store.GetBlock(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if gotBlock == nil || gotBlock.Header != block.Header {
		t.Fatal("block load mismatch")
	}
	index, err := store.GetBlockIndex(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if index == nil || index.Height != 0 {
		t.Fatal("block index mismatch")
	}
}

func TestWriteAndLoadHeaderStateRoundtrip(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block, _ := sampleBlockAndUTXOs()
	headerState := &StoredHeaderState{
		Profile:   types.Regtest,
		Height:    0,
		TipHeader: block.Header,
	}
	if err := store.WriteHeaderState(headerState); err != nil {
		t.Fatal(err)
	}
	if err := store.PutHeader(0, &block.Header); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadHeaderState()
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.Profile != types.Regtest || loaded.Height != 0 {
		t.Fatal("loaded header state mismatch")
	}

	hash := consensus.HeaderHash(&block.Header)
	index, err := store.GetBlockIndex(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if index == nil || index.Height != 0 || index.Header != block.Header {
		t.Fatal("header index mismatch")
	}
}

func TestPutValidatedBlockRoundtrip(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block, utxos := sampleBlockAndUTXOs()
	work, err := consensus.BlockWork(block.Header.NBits)
	if err != nil {
		t.Fatal(err)
	}
	entry := &BlockIndexEntry{
		Height:         0,
		ParentHash:     block.Header.PrevBlockHash,
		Header:         block.Header,
		ChainWork:      work,
		Validated:      true,
		BlockSizeState: consensus.BlockSizeState{Limit: 32_000_000, EWMA: 148, RecentBlockSizes: []uint64{148}},
	}
	undo := []BlockUndoEntry{{
		OutPoint: types.OutPoint{TxID: consensus.TxID(&block.Txs[0]), Vout: 0},
		Entry:    utxos[types.OutPoint{TxID: consensus.TxID(&block.Txs[0]), Vout: 0}],
	}}

	if err := store.PutValidatedBlock(&block, entry, undo); err != nil {
		t.Fatal(err)
	}

	hash := consensus.HeaderHash(&block.Header)
	gotEntry, err := store.GetBlockIndex(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if gotEntry == nil || !gotEntry.Validated || gotEntry.ChainWork != work || gotEntry.ParentHash != block.Header.PrevBlockHash {
		t.Fatal("validated block entry mismatch")
	}
	gotUndo, err := store.GetUndo(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotUndo) != 1 || gotUndo[0] != undo[0] {
		t.Fatal("undo roundtrip mismatch")
	}
}
