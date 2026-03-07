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

func sampleBlockSizeState() consensus.BlockSizeState {
	return consensus.BlockSizeState{
		BlockSize: 148,
		Epsilon:   16_000_000,
		Beta:      16_000_000,
	}
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
		Profile:        types.Regtest,
		Height:         0,
		TipHeader:      block.Header,
		BlockSizeState: sampleBlockSizeState(),
		UTXOs:          utxos,
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
		BlockSizeState: sampleBlockSizeState(),
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

func TestWriteHeaderBatchPersistsChainWorkAndTip(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	genesis, _ := sampleBlockAndUTXOs()
	genesisWork, err := consensus.BlockWork(genesis.Header.NBits)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteHeaderState(&StoredHeaderState{
		Profile:   types.Regtest,
		Height:    0,
		TipHeader: genesis.Header,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutHeader(0, &genesis.Header); err != nil {
		t.Fatal(err)
	}

	secondHeader := genesis.Header
	secondHeader.PrevBlockHash = consensus.HeaderHash(&genesis.Header)
	secondHeader.Timestamp++
	secondWork, err := consensus.BlockWork(secondHeader.NBits)
	if err != nil {
		t.Fatal(err)
	}
	secondChainWork := consensus.AddChainWork(genesisWork, secondWork)
	if err := store.WriteHeaderBatch(&StoredHeaderState{
		Profile:   types.Regtest,
		Height:    1,
		TipHeader: secondHeader,
	}, []HeaderBatchEntry{{
		Height:    1,
		Header:    secondHeader,
		ChainWork: secondChainWork,
	}}); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadHeaderState()
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.Height != 1 || loaded.TipHeader != secondHeader {
		t.Fatal("header batch tip mismatch")
	}
	entry, err := store.GetBlockIndex(&secondHeader.PrevBlockHash)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || entry.ChainWork != genesisWork {
		t.Fatal("genesis chainwork mismatch after header batch")
	}
	secondHash := consensus.HeaderHash(&secondHeader)
	secondEntry, err := store.GetBlockIndex(&secondHash)
	if err != nil {
		t.Fatal(err)
	}
	if secondEntry == nil || secondEntry.ChainWork != secondChainWork {
		t.Fatal("batched header chainwork mismatch")
	}
}

func TestAppendValidatedBlockPersistsDeltaUndoAndHeight(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	genesis, utxos := sampleBlockAndUTXOs()
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesisOut := types.OutPoint{TxID: genesisTxID, Vout: 0}
	genesisWork, err := consensus.BlockWork(genesis.Header.NBits)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteFullState(&StoredChainState{
		Profile:        types.Regtest,
		Height:         0,
		TipHeader:      genesis.Header,
		BlockSizeState: sampleBlockSizeState(),
		UTXOs:          utxos,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutValidatedBlock(&genesis, &BlockIndexEntry{
		Height:         0,
		ParentHash:     genesis.Header.PrevBlockHash,
		Header:         genesis.Header,
		ChainWork:      genesisWork,
		Validated:      true,
		BlockSizeState: sampleBlockSizeState(),
	}, nil); err != nil {
		t.Fatal(err)
	}

	next := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  consensus.HeaderHash(&genesis.Header),
			MerkleTxIDRoot: consensus.MerkleRoot([][32]byte{consensus.TxID(&types.Transaction{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 25, KeyHash: [32]byte{9}}}}})}),
			MerkleAuthRoot: consensus.MerkleRoot([][32]byte{consensus.AuthID(&types.Transaction{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 25, KeyHash: [32]byte{9}}}}})}),
			UTXORoot:       [32]byte{1},
			Timestamp:      2,
			NBits:          genesis.Header.NBits,
		},
		Txs: []types.Transaction{{
			Base: types.TxBase{
				Version: 1,
				Outputs: []types.TxOutput{{ValueAtoms: 25, KeyHash: [32]byte{9}}},
			},
		}},
	}
	nextTxID := consensus.TxID(&next.Txs[0])
	nextOut := types.OutPoint{TxID: nextTxID, Vout: 0}
	nextEntry := consensus.UtxoEntry{ValueAtoms: 25, KeyHash: [32]byte{9}}
	nextWork, err := consensus.BlockWork(next.Header.NBits)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendValidatedBlock(&StoredChainState{
		Profile:        types.Regtest,
		Height:         1,
		TipHeader:      next.Header,
		BlockSizeState: consensus.BlockSizeState{BlockSize: 296, Epsilon: 16_000_000, Beta: 16_000_000},
		UTXOs:          consensus.UtxoSet{nextOut: nextEntry},
	}, &next, &BlockIndexEntry{
		Height:         1,
		ParentHash:     next.Header.PrevBlockHash,
		Header:         next.Header,
		ChainWork:      consensus.AddChainWork(genesisWork, nextWork),
		Validated:      true,
		BlockSizeState: consensus.BlockSizeState{BlockSize: 296, Epsilon: 16_000_000, Beta: 16_000_000},
	}, []BlockUndoEntry{{
		OutPoint: genesisOut,
		Entry:    utxos[genesisOut],
	}}, []types.OutPoint{genesisOut}, map[types.OutPoint]consensus.UtxoEntry{
		nextOut: nextEntry,
	}); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadChainState()
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.Height != 1 {
		t.Fatal("delta append height mismatch")
	}
	if _, ok := loaded.UTXOs[genesisOut]; ok {
		t.Fatal("spent outpoint should be removed after delta append")
	}
	if got := loaded.UTXOs[nextOut]; got != nextEntry {
		t.Fatal("created outpoint mismatch after delta append")
	}
	blockHash := consensus.HeaderHash(&next.Header)
	undo, err := store.GetUndo(&blockHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(undo) != 1 || undo[0].OutPoint != genesisOut {
		t.Fatal("delta append undo mismatch")
	}
	hashAtHeight, err := store.GetBlockHashByHeight(1)
	if err != nil {
		t.Fatal(err)
	}
	if hashAtHeight == nil || *hashAtHeight != blockHash {
		t.Fatal("active height index mismatch after delta append")
	}
}
