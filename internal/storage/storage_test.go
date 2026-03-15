package storage

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utxochecksum"
	"github.com/cockroachdb/pebble"
)

func testCoinbase(height uint64, outputs []types.TxOutput) types.Transaction {
	var extraNonce [types.CoinbaseExtraNonceLen]byte
	return types.Transaction{
		Base: types.TxBase{
			Version:            1,
			CoinbaseHeight:     &height,
			CoinbaseExtraNonce: &extraNonce,
			Outputs:            outputs,
		},
	}
}

func sampleBlockAndUTXOs() (types.Block, consensus.UtxoSet) {
	coinbase := testCoinbase(0, []types.TxOutput{{ValueAtoms: 50, PubKey: [32]byte{7}}})
	coinbaseTxID := consensus.TxID(&coinbase)
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: coinbaseTxID, Vout: 0}: {ValueAtoms: 50, PubKey: [32]byte{7}},
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
		BlockSize: types.BlockHeaderEncodedLen,
		Epsilon:   16_000_000,
		Beta:      16_000_000,
	}
}

func TestStorageWritePoliciesMatchDurabilityIntent(t *testing.T) {
	if consensusCriticalWriteOptions == nil || !consensusCriticalWriteOptions.Sync {
		t.Fatal("consensus-critical storage writes must be synced")
	}
	if bestEffortWriteOptions == nil {
		t.Fatal("best-effort write options must be configured")
	}
	if bestEffortWriteOptions.Sync {
		t.Fatal("best-effort storage writes should remain unsynced")
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
	if loaded.UTXOChecksum == ([32]byte{}) {
		t.Fatal("expected stored utxo checksum")
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

func TestLoadChainStateMetaSkipsUTXOScanButPreservesChecksum(t *testing.T) {
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

	meta, err := store.LoadChainStateMeta()
	if err != nil {
		t.Fatal(err)
	}
	if meta == nil {
		t.Fatal("expected chain metadata")
	}
	if meta.Profile != state.Profile || meta.Height != state.Height || meta.TipHeader != state.TipHeader {
		t.Fatalf("loaded metadata = %+v, want %+v", meta, state)
	}
	wantChecksum := utxochecksum.Compute(utxos)
	if meta.UTXOChecksum != wantChecksum {
		t.Fatalf("checksum = %x, want %x", meta.UTXOChecksum, wantChecksum)
	}
}

func TestGetUTXOAndForEachUTXO(t *testing.T) {
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

	var expectedOutPoint types.OutPoint
	var expectedEntry consensus.UtxoEntry
	for outPoint, entry := range utxos {
		expectedOutPoint = outPoint
		expectedEntry = entry
		break
	}

	entry, ok, err := store.GetUTXO(expectedOutPoint)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || entry != expectedEntry {
		t.Fatalf("GetUTXO = (%+v, %t), want (%+v, true)", entry, ok, expectedEntry)
	}

	if _, ok, err := store.GetUTXO(types.OutPoint{TxID: [32]byte{9}, Vout: 99}); err != nil || ok {
		t.Fatalf("missing GetUTXO = ok %t err %v, want ok false err nil", ok, err)
	}

	visited := make(consensus.UtxoSet)
	if err := store.ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		visited[outPoint] = entry
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(visited) != len(utxos) {
		t.Fatalf("ForEachUTXO visited %d entries, want %d", len(visited), len(utxos))
	}
	if visited[expectedOutPoint] != expectedEntry {
		t.Fatalf("ForEachUTXO entry = %+v, want %+v", visited[expectedOutPoint], expectedEntry)
	}
}

func TestForEachUTXOStopsOnCallbackError(t *testing.T) {
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

	stopErr := errors.New("stop scan")
	err = store.ForEachUTXO(func(types.OutPoint, consensus.UtxoEntry) error {
		return stopErr
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("ForEachUTXO err = %v, want %v", err, stopErr)
	}
}

func TestFastSyncStateRoundtripAndClear(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block, utxos := sampleBlockAndUTXOs()
	state := &FastSyncState{
		SnapshotHeight:     3,
		SnapshotHeaderHash: consensus.HeaderHash(&block.Header),
		SnapshotUTXORoot:   consensus.ComputedUTXORoot(utxos),
		SnapshotUTXOCount:  len(utxos),
	}
	if err := store.WriteFastSyncState(state, utxos); err != nil {
		t.Fatalf("WriteFastSyncState: %v", err)
	}

	loadedState, err := store.LoadFastSyncState()
	if err != nil {
		t.Fatalf("LoadFastSyncState: %v", err)
	}
	if loadedState == nil || *loadedState != *state {
		t.Fatalf("loaded fast sync state = %+v, want %+v", loadedState, state)
	}
	loadedUTXOs, err := store.LoadFastSyncSnapshotUTXOs()
	if err != nil {
		t.Fatalf("LoadFastSyncSnapshotUTXOs: %v", err)
	}
	if len(loadedUTXOs) != len(utxos) {
		t.Fatalf("loaded snapshot utxo count = %d, want %d", len(loadedUTXOs), len(utxos))
	}
	if err := store.ClearFastSyncState(); err != nil {
		t.Fatalf("ClearFastSyncState: %v", err)
	}
	loadedState, err = store.LoadFastSyncState()
	if err != nil {
		t.Fatalf("LoadFastSyncState after clear: %v", err)
	}
	if loadedState != nil {
		t.Fatalf("expected fast sync state to be cleared, got %+v", loadedState)
	}
	loadedUTXOs, err = store.LoadFastSyncSnapshotUTXOs()
	if err != nil {
		t.Fatalf("LoadFastSyncSnapshotUTXOs after clear: %v", err)
	}
	if loadedUTXOs != nil {
		t.Fatalf("expected retained snapshot utxos to be cleared, got %d entries", len(loadedUTXOs))
	}
}

func TestLocalityIndexTracksAppendAndRewrite(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	genesis, utxos := sampleBlockAndUTXOs()
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesisOutPoint := types.OutPoint{TxID: genesisTxID, Vout: 0}
	initialState := &StoredChainState{
		Profile:        types.Regtest,
		Height:         0,
		TipHeader:      genesis.Header,
		BlockSizeState: sampleBlockSizeState(),
		UTXOs:          utxos,
	}
	if err := store.WriteFullState(initialState); err != nil {
		t.Fatalf("WriteFullState: %v", err)
	}

	secondCoinbase := testCoinbase(1, []types.TxOutput{{ValueAtoms: 25, PubKey: [32]byte{9}}})
	secondTxID := consensus.TxID(&secondCoinbase)
	secondOutPoint := types.OutPoint{TxID: secondTxID, Vout: 0}
	secondBlock := types.Block{
		Header: types.BlockHeader{
			PrevBlockHash:  genesis.Header.PrevBlockHash,
			MerkleTxIDRoot: consensus.MerkleRoot([][32]byte{secondTxID}),
			MerkleAuthRoot: consensus.MerkleRoot([][32]byte{consensus.AuthID(&secondCoinbase)}),
			Timestamp:      2,
			NBits:          genesis.Header.NBits,
		},
		Txs: []types.Transaction{secondCoinbase},
	}
	work, err := consensus.BlockWork(secondBlock.Header.NBits)
	if err != nil {
		t.Fatalf("BlockWork: %v", err)
	}
	if err := store.AppendValidatedBlock(&StoredChainState{
		Profile:        types.Regtest,
		Height:         1,
		TipHeader:      secondBlock.Header,
		BlockSizeState: sampleBlockSizeState(),
	}, &secondBlock, &BlockIndexEntry{
		Height:         1,
		ParentHash:     genesis.Header.PrevBlockHash,
		Header:         secondBlock.Header,
		ChainWork:      work,
		Validated:      true,
		BlockSizeState: sampleBlockSizeState(),
	}, nil, nil, map[types.OutPoint]consensus.UtxoEntry{
		secondOutPoint: {ValueAtoms: 25, PubKey: [32]byte{9}},
	}); err != nil {
		t.Fatalf("AppendValidatedBlock: %v", err)
	}

	ordered, err := store.LoadLocalityOrderedUTXOs(0)
	if err != nil {
		t.Fatalf("LoadLocalityOrderedUTXOs: %v", err)
	}
	if len(ordered) != 2 {
		t.Fatalf("locality count = %d, want 2", len(ordered))
	}
	if ordered[0].OutPoint != genesisOutPoint || ordered[1].OutPoint != secondOutPoint {
		t.Fatalf("unexpected locality order after append: %+v", ordered)
	}

	rewritten := &StoredChainState{
		Profile:        types.Regtest,
		Height:         1,
		TipHeader:      secondBlock.Header,
		BlockSizeState: sampleBlockSizeState(),
		UTXOs: consensus.UtxoSet{
			secondOutPoint: {ValueAtoms: 25, PubKey: [32]byte{9}},
			types.OutPoint{TxID: [32]byte{33}, Vout: 1}: {ValueAtoms: 10, PubKey: [32]byte{7}},
		},
	}
	if err := store.RewriteFullStateDelta(&StoredChainState{
		Profile:        types.Regtest,
		Height:         1,
		TipHeader:      secondBlock.Header,
		BlockSizeState: sampleBlockSizeState(),
		UTXOs: consensus.UtxoSet{
			genesisOutPoint: utxos[genesisOutPoint],
			secondOutPoint:  {ValueAtoms: 25, PubKey: [32]byte{9}},
		},
	}, rewritten); err != nil {
		t.Fatalf("RewriteFullStateDelta: %v", err)
	}
	ordered, err = store.LoadLocalityOrderedUTXOs(0)
	if err != nil {
		t.Fatalf("LoadLocalityOrderedUTXOs after rewrite: %v", err)
	}
	if len(ordered) != 2 {
		t.Fatalf("locality count after rewrite = %d, want 2", len(ordered))
	}
	if ordered[0].OutPoint != secondOutPoint {
		t.Fatalf("expected surviving output to keep earliest locality rank, got %+v", ordered)
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
	peers := map[string]KnownPeerRecord{
		"127.0.0.1:18444": {
			LastSeen:     base,
			LastSuccess:  base,
			LastAttempt:  base.Add(-time.Minute),
			BannedUntil:  base.Add(time.Hour),
			FailureCount: 1,
			Manual:       true,
		},
		"127.0.0.1:18445": {
			LastSeen:     base.Add(5 * time.Minute),
			LastSuccess:  base.Add(4 * time.Minute),
			LastAttempt:  base.Add(5 * time.Minute),
			FailureCount: 3,
		},
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
		if got.LastSeen != want.LastSeen || got.LastSuccess != want.LastSuccess || got.LastAttempt != want.LastAttempt || got.BannedUntil != want.BannedUntil || got.FailureCount != want.FailureCount || got.Manual != want.Manual {
			t.Fatalf("loaded peer %q = %+v, want %+v", addr, got, want)
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

func TestPutHeaderDoesNotDowngradeValidatedEntry(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block, _ := sampleBlockAndUTXOs()
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
	if err := store.PutValidatedBlock(&block, entry, nil); err != nil {
		t.Fatal(err)
	}

	if err := store.PutHeader(0, &block.Header); err != nil {
		t.Fatal(err)
	}

	hash := consensus.HeaderHash(&block.Header)
	got, err := store.GetBlockIndex(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected block index entry")
	}
	if !got.Validated {
		t.Fatal("validated entry was downgraded by PutHeader")
	}
	if got.BlockSizeState != entry.BlockSizeState {
		t.Fatal("block size state was not preserved")
	}
}

func TestCommitHeaderChainDoesNotDowngradeValidatedEntry(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block, _ := sampleBlockAndUTXOs()
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
	if err := store.PutValidatedBlock(&block, entry, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteHeaderState(&StoredHeaderState{
		Profile:   types.RegtestMedium,
		Height:    0,
		TipHeader: block.Header,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitHeaderChain(&StoredHeaderState{
		Profile:   types.RegtestMedium,
		Height:    0,
		TipHeader: block.Header,
	}, []HeaderBatchEntry{{
		Height:    0,
		Header:    block.Header,
		ChainWork: work,
	}}, 0, 0, nil); err != nil {
		t.Fatal(err)
	}

	hash := consensus.HeaderHash(&block.Header)
	got, err := store.GetBlockIndex(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected block index entry")
	}
	if !got.Validated {
		t.Fatal("validated entry was downgraded by CommitHeaderChain")
	}
	if got.BlockSizeState != entry.BlockSizeState {
		t.Fatal("block size state was not preserved")
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

	hashAtHeight, err := store.GetHeaderHashByHeight(1)
	if err != nil {
		t.Fatal(err)
	}
	if hashAtHeight == nil || *hashAtHeight != mainHash {
		t.Fatalf("active header hash = %x, want %x", hashAtHeight, mainHash)
	}
	blockHashAtHeight, err := store.GetBlockHashByHeight(1)
	if err != nil {
		t.Fatal(err)
	}
	if blockHashAtHeight != nil {
		t.Fatalf("validated block height hash = %x, want nil", blockHashAtHeight)
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

	nextCoinbase := testCoinbase(1, []types.TxOutput{{ValueAtoms: 25, PubKey: [32]byte{9}}})
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
	nextEntry := consensus.UtxoEntry{ValueAtoms: 25, PubKey: [32]byte{9}}
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
	if err := store.WaitForDerivedIndexes(time.Second); err != nil {
		t.Fatalf("WaitForDerivedIndexes: %v", err)
	}
	rawHeightIndex, err := store.get(heightIndexKey(1))
	if err != nil {
		t.Fatalf("raw height index: %v", err)
	}
	if len(rawHeightIndex) != 32 {
		t.Fatalf("raw height index len = %d, want 32", len(rawHeightIndex))
	}
}

func TestHeaderHeightLookupsSeparateIndexedAndCanonicalViews(t *testing.T) {
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

	first := genesis.Header
	first.PrevBlockHash = consensus.HeaderHash(&genesis.Header)
	first.Timestamp = genesis.Header.Timestamp + 1
	firstHash := consensus.HeaderHash(&first)
	if err := store.CommitHeaderChain(&StoredHeaderState{
		Profile:   types.Regtest,
		Height:    1,
		TipHeader: first,
	}, []HeaderBatchEntry{{
		Height:    1,
		Header:    first,
		ChainWork: [32]byte{1},
	}}, 0, 0, []HeaderBatchEntry{{
		Height:    1,
		Header:    first,
		ChainWork: [32]byte{1},
	}}); err != nil {
		t.Fatal(err)
	}

	second := first
	second.PrevBlockHash = firstHash
	second.Timestamp = first.Timestamp + 1
	secondHash := consensus.HeaderHash(&second)
	if err := store.CommitHeaderChain(&StoredHeaderState{
		Profile:   types.Regtest,
		Height:    2,
		TipHeader: second,
	}, []HeaderBatchEntry{{
		Height:    2,
		Header:    second,
		ChainWork: [32]byte{2},
	}}, 1, 1, []HeaderBatchEntry{{
		Height:    2,
		Header:    second,
		ChainWork: [32]byte{2},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForDerivedIndexes(time.Second); err != nil {
		t.Fatalf("WaitForDerivedIndexes: %v", err)
	}

	// Simulate the derived index lagging behind the canonical header tip.
	if err := store.db.Delete(headerHeightIndexKey(2), pebble.Sync); err != nil {
		t.Fatal(err)
	}

	indexedHash, err := store.GetIndexedHeaderHashByHeight(2)
	if err != nil {
		t.Fatal(err)
	}
	if indexedHash != nil {
		t.Fatalf("indexed header hash at height 2 = %x, want nil", indexedHash)
	}

	canonicalHash, err := store.GetCanonicalHeaderHashByHeight(2)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalHash == nil || *canonicalHash != secondHash {
		t.Fatalf("canonical header hash at height 2 = %x, want %x", canonicalHash, secondHash)
	}
}

func TestDerivedIndexesReplayFromJournalOnReopen(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	block, utxos := sampleBlockAndUTXOs()
	hash := consensus.HeaderHash(&block.Header)
	if err := store.WriteFullState(&StoredChainState{
		Profile:        types.Regtest,
		Height:         0,
		TipHeader:      block.Header,
		BlockSizeState: sampleBlockSizeState(),
		UTXOs:          utxos,
	}); err != nil {
		t.Fatal(err)
	}
	work, err := consensus.BlockWork(block.Header.NBits)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutValidatedBlock(&block, &BlockIndexEntry{
		Height:         0,
		ParentHash:     block.Header.PrevBlockHash,
		Header:         block.Header,
		ChainWork:      work,
		Validated:      true,
		BlockSizeState: sampleBlockSizeState(),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RewriteActiveHeights(0, 0, []BlockIndexEntry{{
		Height:         0,
		ParentHash:     block.Header.PrevBlockHash,
		Header:         block.Header,
		ChainWork:      work,
		Validated:      true,
		BlockSizeState: sampleBlockSizeState(),
	}}); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash before the async derived-index worker catches up.
	if err := store.db.Set(metaDerivedJournalSeqKey, encodeU64(0), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Delete(heightIndexKey(0), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.WaitForDerivedIndexes(time.Second); err != nil {
		t.Fatalf("WaitForDerivedIndexes reopen: %v", err)
	}
	rawHeightIndex, err := reopened.get(heightIndexKey(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(rawHeightIndex) != 32 {
		t.Fatalf("raw height index len = %d, want 32", len(rawHeightIndex))
	}
	if got, err := reopened.GetBlockHashByHeight(0); err != nil || got == nil || *got != hash {
		t.Fatalf("GetBlockHashByHeight(0) = (%x, %v), want (%x, nil)", got, err, hash)
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
			unchangedOut: {ValueAtoms: 11, PubKey: [32]byte{1}},
			updatedOut:   {ValueAtoms: 12, PubKey: [32]byte{2}},
			deletedOut:   {ValueAtoms: 13, PubKey: [32]byte{3}},
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
			unchangedOut: {ValueAtoms: 11, PubKey: [32]byte{1}},
			updatedOut:   {ValueAtoms: 99, PubKey: [32]byte{4}},
			createdOut:   {ValueAtoms: 21, PubKey: [32]byte{5}},
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
