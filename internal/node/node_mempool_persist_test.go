package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"testing"
)

func TestApplyPeerBlockPromotesReadyOrphans(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	genesisTxID := consensus.TxID(&genesis.Txs[0])
	chainUTXOs := consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {
			ValueAtoms: 50,
			PubKey:     nodeSignerPubKey(7),
		},
	}
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	block := blockWithTxsForNodeTest(t, 0, genesis.Header, chainUTXOs, []types.Transaction{coinbase}, genesis.Header.Timestamp+1)
	orphanTx := spendTxForNodeTest(t, 9, types.OutPoint{TxID: consensus.TxID(&coinbase), Vout: 0}, 1, 8, 0)
	peer := newPeerConnForTests("127.0.0.1:18444")

	if err := svc.onPeerMessage(peer, p2p.TxBatchMessage{Txs: []types.Transaction{orphanTx}}); err != nil {
		t.Fatalf("onPeerMessage orphan batch: %v", err)
	}
	if got := svc.pool.Count(); got != 0 {
		t.Fatalf("mempool count before parent block = %d, want 0", got)
	}
	if got := svc.pool.OrphanCount(); got != 1 {
		t.Fatalf("orphan count before parent block = %d, want 1", got)
	}

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if err := svc.acceptPeerBlockMessage(peer, &block); err != nil {
		t.Fatalf("acceptPeerBlockMessage: %v", err)
	}
	if got := svc.pool.Count(); got != 1 {
		t.Fatalf("mempool count after parent block = %d, want 1", got)
	}
	if got := svc.pool.OrphanCount(); got != 0 {
		t.Fatalf("orphan count after parent block = %d, want 0", got)
	}
	if !svc.pool.Contains(consensus.TxID(&orphanTx)) {
		t.Fatal("promoted orphan tx missing from mempool")
	}
}

func TestReorgEvictsMempoolConflictsConfirmedOnWinningBranch(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	baseState := NewChainState(types.Regtest)
	if _, err := baseState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	active := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{active.Header}); err != nil {
		t.Fatalf("applyPeerHeaders active: %v", err)
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: active}); err != nil {
		t.Fatalf("onPeerMessage active block: %v", err)
	}

	genesisOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	mempoolTx := spendTxForNodeTest(t, 7, genesisOut, 50, 8, 1)
	mempoolTxID := consensus.TxID(&mempoolTx)
	if _, err := svc.SubmitTx(mempoolTx); err != nil {
		t.Fatalf("SubmitTx mempool tx: %v", err)
	}
	if !svc.pool.Contains(mempoolTxID) {
		t.Fatalf("expected mempool to contain %x before reorg", mempoolTxID)
	}

	altState := NewChainState(types.Regtest)
	if _, err := altState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	alt1 := nextCoinbaseBlock(0, genesis.Header, altState.UTXOs(), 4, genesis.Header.Timestamp+601)
	if _, err := altState.ApplyBlock(&alt1); err != nil {
		t.Fatal(err)
	}
	alt2 := nextCoinbaseBlock(1, alt1.Header, altState.UTXOs(), 5, alt1.Header.Timestamp+600)
	if _, err := altState.ApplyBlock(&alt2); err != nil {
		t.Fatal(err)
	}
	conflict := spendTxForNodeTest(t, 7, genesisOut, 50, 9, 1)
	alt3Coinbase := coinbaseTxForHeight(3, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(6)}})
	alt3 := blockWithTxsForNodeTest(t, 2, alt2.Header, altState.UTXOs(), []types.Transaction{alt3Coinbase, conflict}, alt2.Header.Timestamp+600)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{alt1.Header, alt2.Header, alt3.Header}); err != nil {
		t.Fatalf("applyPeerHeaders competing branch: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt1}); err != nil {
		t.Fatalf("onPeerMessage alt1: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt2}); err != nil {
		t.Fatalf("onPeerMessage alt2: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt3}); err != nil {
		t.Fatalf("onPeerMessage alt3: %v", err)
	}

	if svc.pool.Contains(mempoolTxID) {
		t.Fatalf("mempool conflict %x survived winning-branch confirmation", mempoolTxID)
	}
	if got := svc.pool.Count(); got != 0 {
		t.Fatalf("mempool count after reorg = %d, want 0", got)
	}
	if got, want := consensus.HeaderHash(svc.chainState.ChainState().TipHeader()), consensus.HeaderHash(&alt3.Header); got != want {
		t.Fatalf("tip hash = %x, want %x", got, want)
	}
}

func TestServiceReloadsPersistedMempoolOnSameTip(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	path := t.TempDir()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}

	genesisOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	parent := spendTxForNodeTest(t, 7, genesisOut, 50, 8, 1)
	parentAdmission, err := svc.SubmitTx(parent)
	if err != nil {
		t.Fatalf("SubmitTx(parent): %v", err)
	}
	child := spendTxForNodeTest(t, 8, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 9, 1)
	childTxID := consensus.TxID(&child)
	if _, err := svc.SubmitTx(child); err != nil {
		t.Fatalf("SubmitTx(child): %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("reopen service: %v", err)
	}
	defer reopened.Close()

	if got := reopened.pool.Count(); got != 2 {
		t.Fatalf("reopened mempool count = %d, want 2", got)
	}
	if !reopened.pool.Contains(parentAdmission.TxID) || !reopened.pool.Contains(childTxID) {
		t.Fatal("expected reopened mempool to contain parent and child")
	}
	foundChild := false
	for _, entry := range reopened.pool.Snapshot() {
		if entry.TxID != childTxID {
			continue
		}
		foundChild = true
		if entry.AncestorCount != 2 {
			t.Fatalf("child ancestor_count = %d, want 2", entry.AncestorCount)
		}
		if entry.AncestorFees != 2 {
			t.Fatalf("child ancestor_fees = %d, want 2", entry.AncestorFees)
		}
	}
	if !foundChild {
		t.Fatalf("missing child snapshot %x", childTxID)
	}
}

func TestBuildStoredMempoolDeltaOnlyTouchesChangedRecords(t *testing.T) {
	entryTx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: [32]byte{1}, Vout: 0}, 50, 8, 1)
	entryTxID := consensus.TxID(&entryTx)
	entryAuthID := consensus.AuthID(&entryTx)
	orphanTx := spendTxForNodeTest(t, 9, types.OutPoint{TxID: [32]byte{2}, Vout: 0}, 50, 10, 1)
	orphanTxID := consensus.TxID(&orphanTx)
	orphanAuthID := consensus.AuthID(&orphanTx)
	newTx := spendTxForNodeTest(t, 11, types.OutPoint{TxID: [32]byte{3}, Vout: 0}, 50, 12, 1)
	newTxID := consensus.TxID(&newTx)

	previous := persistedMempoolState{
		Valid: true,
		Meta: storage.StoredMempoolStateMeta{
			Version:   2,
			Profile:   types.Regtest,
			TipHeight: 5,
			TipHash:   [32]byte{5},
		},
		Entries: map[[32]byte]persistedMempoolEntryFingerprint{
			entryTxID: {
				AuthID:  entryAuthID,
				Summary: consensus.TxValidationSummary{Fee: 1},
				AddedAt: 4,
			},
		},
		Orphans: map[[32]byte]persistedMempoolOrphanFingerprint{
			orphanTxID: {
				AuthID:  orphanAuthID,
				AddedAt: 6,
				Missing: []types.OutPoint{{TxID: [32]byte{9}, Vout: 1}},
			},
		},
	}
	current := livePersistedMempoolSnapshot{
		State: persistedMempoolState{
			Valid: true,
			Meta: storage.StoredMempoolStateMeta{
				Version:   2,
				Profile:   types.Regtest,
				TipHeight: 6,
				TipHash:   [32]byte{6},
			},
			Entries: map[[32]byte]persistedMempoolEntryFingerprint{
				entryTxID: {
					AuthID:  entryAuthID,
					Summary: consensus.TxValidationSummary{Fee: 1},
					AddedAt: 4,
				},
				newTxID: {
					AuthID:  consensus.AuthID(&newTx),
					Summary: consensus.TxValidationSummary{Fee: 2},
					AddedAt: 7,
				},
			},
			Orphans: map[[32]byte]persistedMempoolOrphanFingerprint{},
		},
		Entries: map[[32]byte]mempool.PersistedEntry{
			entryTxID: {
				TxID:    entryTxID,
				AuthID:  entryAuthID,
				Tx:      entryTx,
				Summary: consensus.TxValidationSummary{Fee: 1},
				AddedAt: 4,
			},
			newTxID: {
				TxID:    newTxID,
				AuthID:  consensus.AuthID(&newTx),
				Tx:      newTx,
				Summary: consensus.TxValidationSummary{Fee: 2},
				AddedAt: 7,
			},
		},
		Orphans: map[[32]byte]mempool.PersistedOrphan{},
	}

	delta, changed := buildStoredMempoolDelta(current, previous)
	if !changed {
		t.Fatal("expected mempool delta to detect tip and tx-set changes")
	}
	if delta.Meta.TipHeight != 6 || delta.Meta.TipHash != ([32]byte{6}) {
		t.Fatalf("delta meta = %+v, want tip height 6 hash 06", delta.Meta)
	}
	if len(delta.EntryUpserts) != 1 || delta.EntryUpserts[0].TxID != newTxID {
		t.Fatalf("entry upserts = %+v, want only new tx", delta.EntryUpserts)
	}
	if len(delta.EntryDeletes) != 0 {
		t.Fatalf("entry deletes = %d, want 0", len(delta.EntryDeletes))
	}
	if len(delta.OrphanUpserts) != 0 {
		t.Fatalf("orphan upserts = %d, want 0", len(delta.OrphanUpserts))
	}
	if len(delta.OrphanDeletes) != 1 || delta.OrphanDeletes[0] != orphanTxID {
		t.Fatalf("orphan deletes = %+v, want only old orphan", delta.OrphanDeletes)
	}
}

func TestServiceReprocessesPersistedMempoolAfterTipChange(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	path := t.TempDir()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}

	genesisOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, genesisOut, 50, 8, 1)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store, err := storage.Open(path)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	storedMempool, err := store.LoadMempoolState()
	if err != nil {
		t.Fatalf("LoadMempoolState: %v", err)
	}
	if storedMempool == nil {
		t.Fatal("expected persisted mempool state")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatalf("OpenPersistentChainState: %v", err)
	}
	chain := persistent.ChainState()
	conflict := spendTxForNodeTest(t, 7, genesisOut, 50, 9, 1)
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(6)}})
	block := blockWithTxsForNodeTest(t, 0, *chain.TipHeader(), chain.UTXOs(), []types.Transaction{coinbase, conflict}, chain.TipHeader().Timestamp+600)
	if _, err := persistent.ApplyBlock(&block); err != nil {
		t.Fatalf("ApplyBlock: %v", err)
	}
	tipHeight := persistent.ChainState().TipHeight()
	tipHeader := persistent.ChainState().TipHeader()
	if tipHeight == nil || tipHeader == nil {
		t.Fatal("missing tip after conflicting block")
	}
	if err := persistent.Store().WriteHeaderState(&storage.StoredHeaderState{
		Profile:   types.Regtest,
		Height:    *tipHeight,
		TipHeader: *tipHeader,
	}); err != nil {
		t.Fatalf("WriteHeaderState: %v", err)
	}
	if err := persistent.Store().SetHeaderHashByHeight(*tipHeight, consensus.HeaderHash(tipHeader)); err != nil {
		t.Fatalf("SetHeaderHashByHeight: %v", err)
	}
	if err := persistent.Close(); err != nil {
		t.Fatalf("persistent.Close: %v", err)
	}

	store, err = storage.Open(path)
	if err != nil {
		t.Fatalf("storage.Open second: %v", err)
	}
	if err := store.WriteMempoolState(storedMempool); err != nil {
		t.Fatalf("WriteMempoolState: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close second: %v", err)
	}

	reopened, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("reopen service: %v", err)
	}
	defer reopened.Close()

	if got := reopened.pool.Count(); got != 0 {
		t.Fatalf("reprocessed mempool count = %d, want 0", got)
	}
}

func TestReorgReprocessesDisconnectedBranchTransactions(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	genesisOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	confirmed := spendTxForNodeTest(t, 7, genesisOut, 50, 8, 1)
	confirmedTxID := consensus.TxID(&confirmed)

	activeState := NewChainState(types.Regtest)
	if _, err := activeState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	activeCoinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(3)}})
	active := blockWithTxsForNodeTest(t, 0, genesis.Header, activeState.UTXOs(), []types.Transaction{activeCoinbase, confirmed}, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{active.Header}); err != nil {
		t.Fatalf("applyPeerHeaders active: %v", err)
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: active}); err != nil {
		t.Fatalf("onPeerMessage active: %v", err)
	}
	if got := svc.pool.Count(); got != 0 {
		t.Fatalf("mempool count after confirmation = %d, want 0", got)
	}

	altState := NewChainState(types.Regtest)
	if _, err := altState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	alt1 := nextCoinbaseBlock(0, genesis.Header, altState.UTXOs(), 4, genesis.Header.Timestamp+601)
	if _, err := altState.ApplyBlock(&alt1); err != nil {
		t.Fatal(err)
	}
	alt2 := nextCoinbaseBlock(1, alt1.Header, altState.UTXOs(), 5, alt1.Header.Timestamp+600)
	if _, err := altState.ApplyBlock(&alt2); err != nil {
		t.Fatal(err)
	}
	alt3 := nextCoinbaseBlock(2, alt2.Header, altState.UTXOs(), 6, alt2.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{alt1.Header, alt2.Header, alt3.Header}); err != nil {
		t.Fatalf("applyPeerHeaders competing branch: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt1}); err != nil {
		t.Fatalf("onPeerMessage alt1: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt2}); err != nil {
		t.Fatalf("onPeerMessage alt2: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt3}); err != nil {
		t.Fatalf("onPeerMessage alt3: %v", err)
	}

	if !svc.pool.Contains(confirmedTxID) {
		t.Fatalf("disconnected tx %x was not reprocessed into mempool", confirmedTxID)
	}
	if got := svc.pool.Count(); got != 1 {
		t.Fatalf("mempool count after reorg = %d, want 1", got)
	}
}
