package storage

import (
	"encoding/binary"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func testCoinbase(height uint64, outputs []types.TxOutput) types.Transaction {
	return types.Transaction{
		Base: types.TxBase{
			Version:        1,
			CoinbaseHeight: &height,
			Outputs:        outputs,
		},
	}
}

func sampleBlockAndUTXOs() (types.Block, consensus.UtxoSet) {
	coinbase := testCoinbase(0, []types.TxOutput{{ValueAtoms: 50, KeyHash: [32]byte{7}}})
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

func encodeLegacyBlockSizeState(limit, ewma uint64, recent []uint64) []byte {
	buf := make([]byte, 24, 24+len(recent)*8)
	binary.LittleEndian.PutUint64(buf[:8], limit)
	binary.LittleEndian.PutUint64(buf[8:16], ewma)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(recent)))
	for _, size := range recent {
		next := make([]byte, 8)
		binary.LittleEndian.PutUint64(next, size)
		buf = append(buf, next...)
	}
	return buf
}

func TestDecodeLegacyBlockSizeStateUsesLastRecentBlock(t *testing.T) {
	state, err := decodeBlockSizeState(encodeLegacyBlockSizeState(64_000_000, 40_000_000, []uint64{12_000_000, 18_000_000}))
	if err != nil {
		t.Fatal(err)
	}
	if state.BlockSize != 18_000_000 {
		t.Fatalf("block size = %d, want %d", state.BlockSize, 18_000_000)
	}
	if state.Epsilon+state.Beta != 64_000_000 {
		t.Fatalf("limit = %d, want %d", state.Epsilon+state.Beta, 64_000_000)
	}
}

func TestDecodeLegacyBlockSizeStateFallsBackToEWMA(t *testing.T) {
	state, err := decodeBlockSizeState(encodeLegacyBlockSizeState(32_000_000, 21_000_000, nil))
	if err != nil {
		t.Fatal(err)
	}
	if state.BlockSize != 21_000_000 {
		t.Fatalf("block size = %d, want %d", state.BlockSize, 21_000_000)
	}
	if state.Epsilon != 16_000_000 || state.Beta != 16_000_000 {
		t.Fatalf("epsilon/beta = %d/%d, want 16000000/16000000", state.Epsilon, state.Beta)
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

func TestWriteAndLoadKnownPeersRoundtrip(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	base := time.Unix(1_700_000_000, 123).UTC()
	peers := map[string]time.Time{
		"127.0.0.1:18444": base,
		"127.0.0.1:18445": base.Add(5 * time.Minute),
	}
	if err := store.WriteKnownPeers(peers); err != nil {
		t.Fatalf("WriteKnownPeers: %v", err)
	}

	loaded, err := store.LoadKnownPeers()
	if err != nil {
		t.Fatalf("LoadKnownPeers: %v", err)
	}
	if len(loaded) != len(peers) {
		t.Fatalf("loaded peer count = %d, want %d", len(loaded), len(peers))
	}
	for addr, want := range peers {
		got, ok := loaded[addr]
		if !ok {
			t.Fatalf("missing loaded peer %q", addr)
		}
		if !got.Equal(want) {
			t.Fatalf("loaded peer %q time = %v, want %v", addr, got, want)
		}
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

func TestCommitHeaderChainPersistsChainWorkAndTip(t *testing.T) {
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
	if err := store.CommitHeaderChain(&StoredHeaderState{
		Profile:   types.Regtest,
		Height:    1,
		TipHeader: secondHeader,
	}, []HeaderBatchEntry{{
		Height:    1,
		Header:    secondHeader,
		ChainWork: secondChainWork,
	}}, 0, 0, []HeaderBatchEntry{{
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

func TestCommitHeaderChainInactiveBatchDoesNotRewriteActiveHeightIndex(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	genesis, _ := sampleBlockAndUTXOs()
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

	mainHeader := genesis.Header
	mainHeader.PrevBlockHash = consensus.HeaderHash(&genesis.Header)
	mainHeader.Timestamp = genesis.Header.Timestamp + 1
	mainHash := consensus.HeaderHash(&mainHeader)
	if err := store.CommitHeaderChain(&StoredHeaderState{
		Profile:   types.Regtest,
		Height:    1,
		TipHeader: mainHeader,
	}, []HeaderBatchEntry{{
		Height:    1,
		Header:    mainHeader,
		ChainWork: [32]byte{1},
	}}, 0, 0, []HeaderBatchEntry{{
		Height:    1,
		Header:    mainHeader,
		ChainWork: [32]byte{1},
	}}); err != nil {
		t.Fatal(err)
	}

	sideHeader := genesis.Header
	sideHeader.PrevBlockHash = consensus.HeaderHash(&genesis.Header)
	sideHeader.Timestamp = genesis.Header.Timestamp + 2
	sideHeader.Nonce = 9
	if err := store.CommitHeaderChain(&StoredHeaderState{
		Profile:   types.Regtest,
		Height:    1,
		TipHeader: mainHeader,
	}, []HeaderBatchEntry{{
		Height:    1,
		Header:    sideHeader,
		ChainWork: [32]byte{1},
	}}, 0, 1, nil); err != nil {
		t.Fatal(err)
	}

	hashAtHeight, err := store.GetBlockHashByHeight(1)
	if err != nil {
		t.Fatal(err)
	}
	if hashAtHeight == nil || *hashAtHeight != mainHash {
		t.Fatalf("active height hash = %x, want %x", hashAtHeight, mainHash)
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

	nextCoinbase := testCoinbase(1, []types.TxOutput{{ValueAtoms: 25, KeyHash: [32]byte{9}}})
	next := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  consensus.HeaderHash(&genesis.Header),
			MerkleTxIDRoot: consensus.MerkleRoot([][32]byte{consensus.TxID(&nextCoinbase)}),
			MerkleAuthRoot: consensus.MerkleRoot([][32]byte{consensus.AuthID(&nextCoinbase)}),
			UTXORoot:       [32]byte{1},
			Timestamp:      2,
			NBits:          genesis.Header.NBits,
		},
		Txs: []types.Transaction{nextCoinbase},
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

func TestRewriteFullStateDeltaReplacesOnlyChangedUTXOs(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block, _ := sampleBlockAndUTXOs()
	unchangedOut := types.OutPoint{TxID: [32]byte{7}, Vout: 0}
	updatedOut := types.OutPoint{TxID: [32]byte{8}, Vout: 0}
	deletedOut := types.OutPoint{TxID: [32]byte{9}, Vout: 0}
	createdOut := types.OutPoint{TxID: [32]byte{10}, Vout: 0}
	initial := &StoredChainState{
		Profile:        types.Regtest,
		Height:         1,
		TipHeader:      block.Header,
		BlockSizeState: sampleBlockSizeState(),
		UTXOs: consensus.UtxoSet{
			unchangedOut: {ValueAtoms: 11, KeyHash: [32]byte{1}},
			updatedOut:   {ValueAtoms: 12, KeyHash: [32]byte{2}},
			deletedOut:   {ValueAtoms: 13, KeyHash: [32]byte{3}},
		},
	}
	if err := store.WriteFullState(initial); err != nil {
		t.Fatalf("WriteFullState: %v", err)
	}

	nextHeader := block.Header
	nextHeader.Timestamp++
	next := &StoredChainState{
		Profile:        types.Regtest,
		Height:         2,
		TipHeader:      nextHeader,
		BlockSizeState: consensus.BlockSizeState{BlockSize: 512, Epsilon: 16_000_000, Beta: 16_000_000},
		UTXOs: consensus.UtxoSet{
			unchangedOut: {ValueAtoms: 11, KeyHash: [32]byte{1}},
			updatedOut:   {ValueAtoms: 99, KeyHash: [32]byte{4}},
			createdOut:   {ValueAtoms: 21, KeyHash: [32]byte{5}},
		},
	}
	if err := store.RewriteFullStateDelta(initial, next); err != nil {
		t.Fatalf("RewriteFullStateDelta: %v", err)
	}

	loaded, err := store.LoadChainState()
	if err != nil {
		t.Fatalf("LoadChainState: %v", err)
	}
	if loaded == nil || loaded.Height != next.Height || loaded.TipHeader != next.TipHeader {
		t.Fatal("rewritten state metadata mismatch")
	}
	if len(loaded.UTXOs) != len(next.UTXOs) {
		t.Fatalf("utxo count = %d, want %d", len(loaded.UTXOs), len(next.UTXOs))
	}
	if got := loaded.UTXOs[unchangedOut]; got != next.UTXOs[unchangedOut] {
		t.Fatal("unchanged utxo mismatch after delta rewrite")
	}
	if got := loaded.UTXOs[updatedOut]; got != next.UTXOs[updatedOut] {
		t.Fatal("updated utxo mismatch after delta rewrite")
	}
	if _, ok := loaded.UTXOs[deletedOut]; ok {
		t.Fatal("deleted utxo should not remain after delta rewrite")
	}
	if got := loaded.UTXOs[createdOut]; got != next.UTXOs[createdOut] {
		t.Fatal("created utxo mismatch after delta rewrite")
	}
}
