package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPlanTxRelayReconReducesDenseFanoutAndDistributesTxs(t *testing.T) {
	peers := make([]*peerConn, 0, 16)
	for i := 0; i < 16; i++ {
		peers = append(peers, &peerConn{addr: fmt.Sprintf("peer-%02d:18444", i)})
	}
	txids := [][32]byte{{1}, {2}, {3}, {4}, {5}, {6}}
	batches := planTxRelayRecon(peers, txids)
	if len(batches) == 0 {
		t.Fatal("expected relay batches")
	}
	seenPeers := make(map[string]struct{})
	txFanout := make(map[[32]byte]int)
	for _, batch := range batches {
		seenPeers[batch.peer.addr] = struct{}{}
		for _, txid := range batch.txids {
			txFanout[txid]++
		}
	}
	if len(seenPeers) <= 4 {
		t.Fatalf("dense relay only used %d peers, want broader distribution", len(seenPeers))
	}
	wantFanout := txRelayFanout(len(peers))
	for _, txid := range txids {
		if got := txFanout[txid]; got != wantFanout {
			t.Fatalf("tx %x fanout = %d, want %d", txid, got, wantFanout)
		}
	}
}

func TestPeerConnCoalescesTxRequests(t *testing.T) {
	peer := &peerConn{
		sendQ:  make(chan outboundMessage, 8),
		closed: make(chan struct{}),
	}

	if err := peer.enqueueTxRequest(p2p.TxRequestMessage{TxIDs: [][32]byte{{1}}}); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if err := peer.enqueueTxRequest(p2p.TxRequestMessage{TxIDs: [][32]byte{{2}}}); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}

	req := waitForTxRequestMessage(t, peer, 50*time.Millisecond)
	if len(req.TxIDs) != 2 {
		t.Fatalf("coalesced request size = %d, want 2", len(req.TxIDs))
	}

	select {
	case extra := <-peer.sendQ:
		t.Fatalf("unexpected extra envelope: %#v", extra.msg)
	default:
	}
}

func TestPeerConnStagesWireMaxTxRequests(t *testing.T) {
	peer := &peerConn{
		sendQ:  make(chan outboundMessage, 8),
		closed: make(chan struct{}),
	}
	txids := make([][32]byte, 0, txRelayBatchMaxItems+1)
	for i := 0; i < txRelayBatchMaxItems+1; i++ {
		var txid [32]byte
		binary.LittleEndian.PutUint64(txid[:8], uint64(i+1))
		txids = append(txids, txid)
	}

	ready, armFlush := peer.stagePendingTxRequests(txids)
	if len(ready) != 1 {
		t.Fatalf("ready tx request batches = %d, want 1", len(ready))
	}
	if len(ready[0]) != txRelayBatchMaxItems {
		t.Fatalf("ready tx request batch size = %d, want %d", len(ready[0]), txRelayBatchMaxItems)
	}
	if !armFlush {
		t.Fatal("remaining tx request should arm delayed flush")
	}
	if len(peer.pendingReqOrder) != 1 {
		t.Fatalf("pending tx requests = %d, want 1", len(peer.pendingReqOrder))
	}
}

func TestRunErlayReconcileRoundQueuesUnknownMempoolTxs(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{71}, Vout: 0}
	first := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	firstAdmission, err := pool.AcceptTxWithParams(first, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}, consensus.RegtestParams(), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept first tx: %v", err)
	}
	second := spendTxForNodeTest(t, 2, types.OutPoint{TxID: firstAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTxWithParams(second, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept second tx: %v", err)
	}

	svc := &Service{
		pool:  pool,
		peers: make(map[string]*peerConn),
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = svc
	peer.noteKnownTxIDs([][32]byte{firstAdmission.TxID})
	svc.peers[peer.addr] = peer

	svc.runErlayReconcileRound()

	envelope := <-peer.sendQ
	recon, ok := envelope.msg.(p2p.TxReconMessage)
	if !ok {
		t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
	}
	if len(recon.TxIDs) != 1 || recon.TxIDs[0] == firstAdmission.TxID {
		t.Fatalf("reconcile txids = %x, want only unknown tx", recon.TxIDs)
	}
}

func TestSubmitTxTracksAndRebroadcastsLocalOriginTransactions(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	peer := newPeerConnForTests("127.0.0.1:18444")
	svc.peerMu.Lock()
	svc.peers[peer.addr] = peer
	svc.peerMu.Unlock()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	svc.rebroadcastMu.Lock()
	if _, ok := svc.localRebroadcast[txid]; !ok {
		svc.rebroadcastMu.Unlock()
		t.Fatalf("local rebroadcast set missing %x", txid)
	}
	svc.rebroadcastMu.Unlock()

	initial := waitForTxBatchMessage(t, peer, 100*time.Millisecond)
	if len(initial.Txs) != 1 || consensus.TxID(&initial.Txs[0]) != txid {
		t.Fatalf("initial tx batch = %s, want [%x]", txDebugSummary(initial.Txs, 2), txid)
	}
	peer.releaseRelayBatch(initial)

	svc.rebroadcastLocalTxs()

	select {
	case envelope := <-peer.sendQ:
		recon, ok := envelope.msg.(p2p.TxReconMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
		}
		if len(recon.TxIDs) != 1 || recon.TxIDs[0] != txid {
			t.Fatalf("rebroadcast txids = %x, want [%x]", recon.TxIDs, txid)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for local rebroadcast")
	}
}

func TestSubmitTxUsesSingleStemPeerBeforeFluff(t *testing.T) {
	previousDelay := dandelionFluffDelay
	dandelionFluffDelay = 30 * time.Millisecond
	defer func() {
		dandelionFluffDelay = previousDelay
	}()

	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile:          types.Regtest,
		DBPath:           t.TempDir(),
		DandelionEnabled: true,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	stemPeer := newPeerConnForTests("127.0.0.1:18444")
	stemPeer.outbound = true
	fluffPeer := newPeerConnForTests("127.0.0.1:18445")
	fluffPeer.outbound = true
	svc.peerMu.Lock()
	svc.peers[stemPeer.addr] = stemPeer
	svc.peers[fluffPeer.addr] = fluffPeer
	svc.peerMu.Unlock()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	recon := waitForTxReconMessage(t, stemPeer, 100*time.Millisecond)
	if len(recon.TxIDs) != 1 || recon.TxIDs[0] != txid {
		t.Fatalf("stem txids = %x, want [%x]", recon.TxIDs, txid)
	}
	assertNoPeerMessage(t, fluffPeer, 10*time.Millisecond)

	fluff := waitForTxReconMessage(t, fluffPeer, 100*time.Millisecond)
	if len(fluff.TxIDs) != 1 || fluff.TxIDs[0] != txid {
		t.Fatalf("fluff txids = %x, want [%x]", fluff.TxIDs, txid)
	}
}

func TestPeerOriginTxUsesStemRelayBeforeFluff(t *testing.T) {
	previousDelay := dandelionFluffDelay
	dandelionFluffDelay = 30 * time.Millisecond
	defer func() {
		dandelionFluffDelay = previousDelay
	}()

	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile:          types.Regtest,
		DBPath:           t.TempDir(),
		DandelionEnabled: true,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	stemPeer := newPeerConnForTests("127.0.0.1:18445")
	stemPeer.outbound = true
	fluffPeer := newPeerConnForTests("127.0.0.1:18446")
	fluffPeer.outbound = true
	svc.peerMu.Lock()
	svc.peers[stemPeer.addr] = stemPeer
	svc.peers[fluffPeer.addr] = fluffPeer
	svc.peerMu.Unlock()

	sourcePeer := newPeerConnForTests("127.0.0.1:18444")
	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if err := svc.onPeerMessage(sourcePeer, p2p.TxBatchMessage{Txs: []types.Transaction{tx}}); err != nil {
		t.Fatalf("onPeerMessage: %v", err)
	}

	recon := waitForTxReconMessage(t, stemPeer, 100*time.Millisecond)
	if len(recon.TxIDs) != 1 || recon.TxIDs[0] != txid {
		t.Fatalf("stem txids = %x, want [%x]", recon.TxIDs, txid)
	}
	assertNoPeerMessage(t, fluffPeer, 10*time.Millisecond)
	fluff := waitForTxReconMessage(t, fluffPeer, 100*time.Millisecond)
	if len(fluff.TxIDs) != 1 || fluff.TxIDs[0] != txid {
		t.Fatalf("fluff txids = %x, want [%x]", fluff.TxIDs, txid)
	}
}

func TestSubmitTxDefaultsToImmediateNormalRelayWhenDandelionDisabled(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	peerA := newPeerConnForTests("127.0.0.1:18444")
	peerA.outbound = true
	peerB := newPeerConnForTests("127.0.0.1:18445")
	peerB.outbound = true
	svc.peerMu.Lock()
	svc.peers[peerA.addr] = peerA
	svc.peers[peerB.addr] = peerB
	svc.peerMu.Unlock()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	batchA := waitForTxBatchMessage(t, peerA, 100*time.Millisecond)
	batchB := waitForTxBatchMessage(t, peerB, 100*time.Millisecond)
	if len(batchA.Txs) != 1 || consensus.TxID(&batchA.Txs[0]) != txid {
		t.Fatalf("peerA tx batch = %s, want [%x]", txDebugSummary(batchA.Txs, 2), txid)
	}
	if len(batchB.Txs) != 1 || consensus.TxID(&batchB.Txs[0]) != txid {
		t.Fatalf("peerB tx batch = %s, want [%x]", txDebugSummary(batchB.Txs, 2), txid)
	}
}

func TestSubmitTxLocalRelayWaitsForSendQueueSpace(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.outbound = true
	peer.sendQ = make(chan outboundMessage, 1)
	peer.sendQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}
	svc.peerMu.Lock()
	svc.peers[peer.addr] = peer
	svc.peerMu.Unlock()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	done := make(chan error, 1)
	go func() {
		_, err := svc.SubmitTx(tx)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("SubmitTx returned before send queue space was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	<-peer.sendQ
	if err := <-done; err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	batch := waitForTxBatchMessage(t, peer, 100*time.Millisecond)
	if len(batch.Txs) != 1 || consensus.TxID(&batch.Txs[0]) != txid {
		t.Fatalf("tx batch = %s, want [%x]", txDebugSummary(batch.Txs, 2), txid)
	}
}

func TestRebroadcastLocalTxsRetriesPeersMarkedKnown(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	peer := newPeerConnForTests("127.0.0.1:18444")
	svc.peerMu.Lock()
	svc.peers[peer.addr] = peer
	svc.peerMu.Unlock()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	initial := waitForTxBatchMessage(t, peer, 100*time.Millisecond)
	if len(initial.Txs) != 1 || consensus.TxID(&initial.Txs[0]) != txid {
		t.Fatalf("initial tx batch = %s, want [%x]", txDebugSummary(initial.Txs, 2), txid)
	}
	peer.releaseRelayBatch(initial)

	peer.noteKnownTxIDs([][32]byte{txid})
	svc.rebroadcastLocalTxs()

	select {
	case envelope := <-peer.sendQ:
		recon, ok := envelope.msg.(p2p.TxReconMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
		}
		if len(recon.TxIDs) != 1 || recon.TxIDs[0] != txid {
			t.Fatalf("rebroadcast txids = %x, want [%x]", recon.TxIDs, txid)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for retry rebroadcast")
	}
}

func TestLocalVersionAdvertisesErlayCompactAndExtendedRelayServices(t *testing.T) {
	svc := &Service{}
	version := svc.localVersion()
	for _, want := range []uint64{
		p2p.ServiceNodeNetwork,
		p2p.ServiceErlayTxRelay,
		p2p.ServiceCompactBlockRelay,
		p2p.ServiceGrapheneExtended,
	} {
		if version.Services&want == 0 {
			t.Fatalf("services bitmap %b missing capability %b", version.Services, want)
		}
	}
}

func TestPeerOriginTransactionsAreNotTrackedForLocalRebroadcast(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	peer := newPeerConnForTests("127.0.0.1:18444")
	admissions, errs, _, _ := svc.submitDecodedTxsFrom([]types.Transaction{tx}, peer)
	if errs[0] != nil {
		t.Fatalf("submitDecodedTxsFrom: %v", errs[0])
	}
	if admissions[0].Orphaned {
		t.Fatal("peer-origin tx unexpectedly orphaned")
	}

	svc.rebroadcastMu.Lock()
	defer svc.rebroadcastMu.Unlock()
	if len(svc.localRebroadcast) != 0 {
		t.Fatalf("local rebroadcast set size = %d, want 0", len(svc.localRebroadcast))
	}
}

func TestApplyPeerBlockRemovesConfirmedLocalRebroadcastTransactions(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 9, genesis.Header.Timestamp+600)
	block.Txs = append(block.Txs, tx)
	block.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{
		consensus.TxID(&block.Txs[0]),
		txid,
	})
	block.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{
		consensus.AuthID(&block.Txs[0]),
		consensus.AuthID(&tx),
	})
	utxos := make(consensus.UtxoSet, len(state.UTXOs()))
	for outPoint, entry := range state.UTXOs() {
		utxos[outPoint] = entry
	}
	delete(utxos, prevOut)
	utxos[types.OutPoint{TxID: txid, Vout: 0}] = consensus.UtxoEntry{
		ValueAtoms: tx.Base.Outputs[0].ValueAtoms,
		PubKey:     tx.Base.Outputs[0].PubKey,
	}
	coinbaseTxID := consensus.TxID(&block.Txs[0])
	utxos[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = consensus.UtxoEntry{
		ValueAtoms: block.Txs[0].Base.Outputs[0].ValueAtoms,
		PubKey:     block.Txs[0].Base.Outputs[0].PubKey,
	}
	block.Header.UTXORoot = consensus.ComputedUTXORoot(utxos)
	block.Header = mineHeaderForNodeTest(block.Header)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if applied, _, _, err := svc.applyPeerBlock(&block); err != nil || !applied {
		t.Fatalf("applyPeerBlock = (%v, %v), want (true, nil)", applied, err)
	}

	svc.rebroadcastMu.Lock()
	defer svc.rebroadcastMu.Unlock()
	if _, ok := svc.localRebroadcast[txid]; ok {
		t.Fatalf("confirmed tx %x still tracked for rebroadcast", txid)
	}
}

func TestPeerConnCoalescesTxBatches(t *testing.T) {
	peer := &peerConn{
		sendQ:    make(chan outboundMessage, 8),
		closed:   make(chan struct{}),
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}
	first := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	second := coinbaseTxForHeight(2, []types.TxOutput{{ValueAtoms: 2, PubKey: nodeSignerPubKey(10)}})

	if err := peer.enqueueTxBatch(p2p.TxBatchMessage{Txs: []types.Transaction{first}}); err != nil {
		t.Fatalf("enqueue first tx: %v", err)
	}
	if err := peer.enqueueTxBatch(p2p.TxBatchMessage{Txs: []types.Transaction{second}}); err != nil {
		t.Fatalf("enqueue second tx: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		batch, ok := envelope.msg.(p2p.TxBatchMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxBatchMessage", envelope.msg)
		}
		if len(batch.Txs) != 2 {
			t.Fatalf("coalesced batch size = %d, want 2", len(batch.Txs))
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for coalesced tx batch")
	}

	select {
	case extra := <-peer.sendQ:
		t.Fatalf("unexpected extra envelope: %#v", extra.msg)
	default:
	}
}

func TestPeerConnStagesWireMaxTxBatches(t *testing.T) {
	peer := &peerConn{
		sendQ:    make(chan outboundMessage, 8),
		closed:   make(chan struct{}),
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}
	txs := make([]types.Transaction, 0, txRelayBatchMaxItems+1)
	for i := 0; i < txRelayBatchMaxItems+1; i++ {
		txs = append(txs, coinbaseTxForHeight(uint64(i+1), []types.TxOutput{{ValueAtoms: uint64(i + 1), PubKey: nodeSignerPubKey(byte(i + 1))}}))
	}

	ready, armFlush := peer.stagePendingTxs(txs)
	if len(ready) != 1 {
		t.Fatalf("ready tx batches = %d, want 1", len(ready))
	}
	if len(ready[0]) != txRelayBatchMaxItems {
		t.Fatalf("ready tx batch size = %d, want %d", len(ready[0]), txRelayBatchMaxItems)
	}
	if !armFlush {
		t.Fatal("remaining tx should arm delayed flush")
	}
	if len(peer.pendingTxOrder) != 1 {
		t.Fatalf("pending txs = %d, want 1", len(peer.pendingTxOrder))
	}
}

func TestPeerConnPendingTxStoreMaterializesRelayBatchOnce(t *testing.T) {
	peer := &peerConn{
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}
	first := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	second := coinbaseTxForHeight(2, []types.TxOutput{{ValueAtoms: 2, PubKey: nodeSignerPubKey(10)}})

	ready, armFlush := peer.stagePendingTxs([]types.Transaction{first, second})
	if len(ready) != 0 {
		t.Fatalf("ready batches = %d, want 0", len(ready))
	}
	if !armFlush {
		t.Fatal("expected flush timer to arm for partial relay batch")
	}
	if len(peer.pendingTxOrder) != 2 || len(peer.pendingTxByID) != 2 {
		t.Fatalf("pending tx store = (%d order, %d payloads), want 2/2", len(peer.pendingTxOrder), len(peer.pendingTxByID))
	}

	batches := peer.takePendingTxs()
	if len(batches) != 1 {
		t.Fatalf("batch count = %d, want 1", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Fatalf("batch tx count = %d, want 2", len(batches[0]))
	}
	if consensus.TxID(&batches[0][0]) != consensus.TxID(&first) || consensus.TxID(&batches[0][1]) != consensus.TxID(&second) {
		t.Fatal("materialized relay batch lost tx order")
	}
	if len(peer.pendingTxOrder) != 0 || len(peer.pendingTxByID) != 0 {
		t.Fatal("expected pending tx store cleared after materialization")
	}
}

func TestPeerConnCoalescesTxReconAnnouncements(t *testing.T) {
	peer := &peerConn{
		sendQ:    make(chan outboundMessage, 8),
		closed:   make(chan struct{}),
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}

	if err := peer.enqueueTxRecon(p2p.TxReconMessage{TxIDs: [][32]byte{{1}}}); err != nil {
		t.Fatalf("enqueue first recon: %v", err)
	}
	if err := peer.enqueueTxRecon(p2p.TxReconMessage{TxIDs: [][32]byte{{2}}}); err != nil {
		t.Fatalf("enqueue second recon: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		recon, ok := envelope.msg.(p2p.TxReconMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
		}
		if len(recon.TxIDs) != 2 {
			t.Fatalf("coalesced recon size = %d, want 2", len(recon.TxIDs))
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for coalesced recon message")
	}
}

func TestEnqueuePriorityInvLogsSaturatedQueueAndTracksLaneDrops(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "debug"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = &Service{logger: logger}
	peer.relayPriorityQ = make(chan outboundMessage, 1)
	peer.relayPriorityQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	items := []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: [32]byte{9}}}
	if err := peer.enqueueInvItems(items, true); err != nil {
		t.Fatalf("enqueueInvItems: %v", err)
	}

	stats := peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount())
	if stats.DroppedInv != 1 || stats.DroppedPriorityInv != 1 {
		t.Fatalf("unexpected dropped inv stats: %+v", stats)
	}
	logged := buf.String()
	if !strings.Contains(logged, "dropped relay inv due to saturated queue") || !strings.Contains(logged, "\"lane\":\"priority\"") {
		t.Fatalf("expected saturated priority queue log, got %s", logged)
	}
}

func TestRequestedTxBatchUsesPriorityLaneBelowControl(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 1)
	peer.relayPriorityQ = make(chan outboundMessage, 1)

	tx := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	if err := peer.sendRequestedTxBatch(p2p.TxBatchMessage{Txs: []types.Transaction{tx}}); err != nil {
		t.Fatalf("sendRequestedTxBatch: %v", err)
	}
	if got := len(peer.controlQ); got != 0 {
		t.Fatalf("control queue depth = %d, want 0", got)
	}
	if got := len(peer.relayPriorityQ); got != 1 {
		t.Fatalf("priority queue depth = %d, want 1", got)
	}
	envelope := <-peer.relayPriorityQ
	if envelope.lane != relayQueueLanePriority {
		t.Fatalf("lane = %s, want priority", envelope.lane)
	}
}

func TestFilterQueuedTxIDsLogsSuppressionBurst(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "debug"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = &Service{logger: logger}
	known := [32]byte{1}
	peer.knownTx[known] = struct{}{}

	txids := make([][32]byte, 0, 10)
	for i := 0; i < 10; i++ {
		txids = append(txids, known)
	}
	filtered := peer.filterQueuedTxIDs(txids, true)
	if len(filtered) != 0 {
		t.Fatalf("filtered txids = %d, want 0", len(filtered))
	}
	if !strings.Contains(buf.String(), "suppressed relay work before enqueue") || !strings.Contains(buf.String(), "\"kind\":\"tx_recon\"") {
		t.Fatalf("expected suppression log, got %s", buf.String())
	}
}

func TestRememberKnownTxLockedRetainsWideExactWindow(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")
	for i := 0; i < peerKnownTxLimit+1; i++ {
		var txid [32]byte
		binary.LittleEndian.PutUint64(txid[:8], uint64(i))
		peer.rememberKnownTxLocked(txid)
	}
	if len(peer.knownTx) != peerKnownTxLimit {
		t.Fatalf("known tx size = %d, want %d", len(peer.knownTx), peerKnownTxLimit)
	}
	if len(peer.knownTxOrder) != peerKnownTxLimit {
		t.Fatalf("known tx ring size = %d, want %d", len(peer.knownTxOrder), peerKnownTxLimit)
	}
	var oldest [32]byte
	if _, ok := peer.knownTx[oldest]; ok {
		t.Fatal("expected oldest txid to be evicted from known tx window")
	}
	var newest [32]byte
	binary.LittleEndian.PutUint64(newest[:8], peerKnownTxLimit)
	if _, ok := peer.knownTx[newest]; !ok {
		t.Fatal("expected newest txid to remain in known tx window")
	}
	if peer.knownTxNext != 1 {
		t.Fatalf("known tx next = %d, want 1", peer.knownTxNext)
	}
}

func TestRelayPeerStatsExposeLanePressureCounters(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.telemetry.noteDroppedInv(2, relayQueueLanePriority)
	peer.telemetry.noteDroppedInv(3, relayQueueLaneSend)
	peer.telemetry.noteDroppedTxs(4, relayQueueLaneSend)
	peer.telemetry.noteWriterStarvation(relayQueueLaneControl)
	peer.telemetry.noteWriterStarvation(relayQueueLanePriority)
	peer.telemetry.noteWriterStarvation(relayQueueLaneSend)

	stats := peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount())
	if stats.DroppedPriorityInv != 2 || stats.DroppedSendInv != 3 || stats.DroppedSendTxs != 4 {
		t.Fatalf("unexpected lane drop stats: %+v", stats)
	}
	if stats.ControlStarvation != 1 || stats.PriorityStarvation != 1 || stats.SendStarvation != 1 {
		t.Fatalf("unexpected lane starvation stats: %+v", stats)
	}
}
