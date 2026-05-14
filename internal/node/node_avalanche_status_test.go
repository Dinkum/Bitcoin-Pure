package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAvalanchePollRespondsWithPreferredTx(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	preferred := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	admissions, errs, _, _ := svc.submitDecodedTxs([]types.Transaction{preferred})
	if errs[0] != nil || len(admissions[0].Accepted) != 1 {
		t.Fatalf("submit preferred = (%+v, %v), want accepted", admissions[0], errs[0])
	}
	conflict := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 9, 1)
	_, errs, _, _ = svc.submitDecodedTxs([]types.Transaction{conflict})
	if !errors.Is(errs[0], mempool.ErrInputAlreadySpent) {
		t.Fatalf("conflict err = %v, want ErrInputAlreadySpent", errs[0])
	}

	peer := newPeerConnForTests("127.0.0.1:20001")
	if err := svc.onPeerMessage(peer, p2p.AvaPollMessage{
		PollID: 9,
		Items:  []types.OutPoint{{TxID: genesisTxID, Vout: 0}},
	}); err != nil {
		t.Fatalf("onPeerMessage poll: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		vote, ok := envelope.msg.(p2p.AvaVoteMessage)
		if !ok {
			t.Fatalf("message type = %T, want AvaVoteMessage", envelope.msg)
		}
		if vote.PollID != 9 || len(vote.Votes) != 1 || !vote.Votes[0].HasOpinion {
			t.Fatalf("unexpected vote payload: %+v", vote)
		}
		if want := consensus.TxID(&preferred); vote.Votes[0].TxID != want {
			t.Fatalf("vote txid = %x, want %x", vote.Votes[0].TxID, want)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for avalanche vote")
	}
}

func TestAvalancheFinalizesAndRejectsConflicts(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:                   types.Regtest,
		DBPath:                    t.TempDir(),
		AvalancheKSample:          3,
		AvalancheBeta:             2,
		AvalanchePollInterval:     50 * time.Millisecond,
		AvalancheAlphaNumerator:   1,
		AvalancheAlphaDenominator: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	preferred := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	conflict := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 9, 1)
	if _, err := svc.SubmitTx(preferred); err != nil {
		t.Fatalf("SubmitTx preferred: %v", err)
	}
	_, errs, _, _ := svc.submitDecodedTxs([]types.Transaction{conflict})
	if !errors.Is(errs[0], mempool.ErrInputAlreadySpent) {
		t.Fatalf("conflict err = %v, want ErrInputAlreadySpent", errs[0])
	}

	peers := []*peerConn{
		newPeerConnForTests("127.0.0.1:21001"),
		newPeerConnForTests("127.0.0.1:21002"),
		newPeerConnForTests("127.0.0.1:21003"),
	}
	svc.peerMu.Lock()
	for _, peer := range peers {
		peer.svc = svc
		svc.peers[peer.addr] = peer
	}
	svc.peerMu.Unlock()

	want := consensus.TxID(&preferred)
	for round := 0; round < 2; round++ {
		svc.avalancheManager().runPollStep(time.Now())
		for _, peer := range peers {
			select {
			case envelope := <-peer.sendQ:
				poll, ok := envelope.msg.(p2p.AvaPollMessage)
				if !ok {
					t.Fatalf("message type = %T, want AvaPollMessage", envelope.msg)
				}
				votes := make([]p2p.AvaVote, len(poll.Items))
				for i := range votes {
					votes[i] = p2p.AvaVote{HasOpinion: true, TxID: want}
				}
				svc.onPeerMessage(peer, p2p.AvaVoteMessage{PollID: poll.PollID, Votes: votes})
			case <-time.After(100 * time.Millisecond):
				t.Fatalf("timed out waiting for avalanche poll in round %d", round)
			}
		}
	}

	if err := svc.avalancheManager().rejectionError(conflict); !errors.Is(err, ErrAvalancheFinalConflict) {
		t.Fatalf("rejection err = %v, want ErrAvalancheFinalConflict", err)
	}
	info := svc.avalancheManager().info()
	if info.FinalizedConflictSets != 1 {
		t.Fatalf("finalized conflict sets = %d, want 1", info.FinalizedConflictSets)
	}
	if info.VoteResponses != 6 {
		t.Fatalf("vote responses = %d, want 6", info.VoteResponses)
	}
}

func TestAvalancheTrackedTransactionsFollowActiveConflictSets(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:                   types.Regtest,
		DBPath:                    t.TempDir(),
		AvalancheKSample:          3,
		AvalancheBeta:             2,
		AvalanchePollInterval:     50 * time.Millisecond,
		AvalancheAlphaNumerator:   1,
		AvalancheAlphaDenominator: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	preferred := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	conflict := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 9, 1)
	now := time.Now()
	svc.avalancheManager().trackTx(preferred, true, now)
	svc.avalancheManager().trackTx(conflict, false, now)

	if got := svc.avalancheManager().info().TrackedTransactions; got != 2 {
		t.Fatalf("tracked transactions = %d, want 2", got)
	}

	svc.avalancheManager().mu.Lock()
	svc.avalancheManager().pruneExpiredLocked(now.Add(avalancheConflictTTL + time.Second))
	svc.avalancheManager().mu.Unlock()

	info := svc.avalancheManager().info()
	if info.TrackedConflictSets != 0 || info.TrackedTransactions != 0 {
		t.Fatalf("tracked after prune = conflict_sets:%d transactions:%d, want 0/0", info.TrackedConflictSets, info.TrackedTransactions)
	}
}

func TestEmitThroughputSummaryLogsExpectedFields(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}
	tx := spendTxForNodeTest(t, 1, types.OutPoint{TxID: [32]byte{1}, Vout: 0}, 50, 2, 1)
	if _, err := pool.AcceptTxWithParams(tx, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept tx: %v", err)
	}

	startedAt := time.Unix(1_700_000_000, 0).UTC()
	peerA := newPeerConnForTests("127.0.0.1:18444")
	peerA.sendQ <- outboundMessage{}
	peerA.sendQ <- outboundMessage{}
	peerA.telemetry.noteEnqueue(queueDepthSnapshot{total: 2, send: 2})
	peerA.syncState.lastUsefulAt = startedAt.Add(30 * time.Second)
	peerB := newPeerConnForTests("127.0.0.1:18445")
	peerB.telemetry.noteEnqueue(queueDepthSnapshot{total: 1, send: 1})

	svc := &Service{
		cfg: ServiceConfig{
			StallTimeout:              15 * time.Second,
			ThroughputSummaryInterval: time.Minute,
		},
		logger:        logger,
		pool:          pool,
		peers:         map[string]*peerConn{peerA.addr: peerA, peerB.addr: peerB},
		blockRequests: map[[32]byte]blockDownloadRequest{{1}: {}, {2}: {}},
		txRequests:    map[[32]byte]blockDownloadRequest{{3}: {}, {4}: {}, {5}: {}},
		pendingBlocks: map[[32]byte]pendingPeerBlock{{9}: {}},
		startedAt:     startedAt,
		stopCh:        make(chan struct{}),
	}
	peerA.svc = svc
	peerB.svc = svc

	svc.noteAcceptedAdmissions([]mempool.Admission{{
		Accepted: []mempool.AcceptedTx{{TxID: [32]byte{6}}, {TxID: [32]byte{7}}},
	}})
	svc.noteRelaySent(relayMessageClass{txBatchItems: 6, blockInvItems: 1})
	svc.noteBlockAccepted()
	svc.noteBlockSignatureVerification(consensus.BlockValidationSummary{
		SignatureChecks:        12,
		SignatureBatchFallback: true,
		SignatureVerifyTime:    9 * time.Millisecond,
	})
	svc.noteTemplateRebuild()
	svc.noteTemplateInterruption()

	svc.emitThroughputSummary(startedAt.Add(time.Minute))

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("unmarshal summary log: %v", err)
	}
	if got := entry["message"]; got != "throughput summary" {
		t.Fatalf("message = %v, want throughput summary", got)
	}
	if got := int(entry["admitted_txs"].(float64)); got != 2 {
		t.Fatalf("admitted_txs = %d, want 2", got)
	}
	if got := entry["admitted_txs_per_sec"].(float64); got < 0.033 || got > 0.034 {
		t.Fatalf("admitted_txs_per_sec = %.6f, want about 0.0333", got)
	}
	if got := int(entry["relayed_tx_items"].(float64)); got != 6 {
		t.Fatalf("relayed_tx_items = %d, want 6", got)
	}
	if got := int(entry["relayed_block_items"].(float64)); got != 1 {
		t.Fatalf("relayed_block_items = %d, want 1", got)
	}
	if got := int(entry["blocks_accepted"].(float64)); got != 1 {
		t.Fatalf("blocks_accepted = %d, want 1", got)
	}
	if got := int(entry["block_sig_checks"].(float64)); got != 12 {
		t.Fatalf("block_sig_checks = %d, want 12", got)
	}
	if got := int(entry["block_sig_fallbacks"].(float64)); got != 1 {
		t.Fatalf("block_sig_fallbacks = %d, want 1", got)
	}
	if got := int(entry["template_rebuilds"].(float64)); got != 1 {
		t.Fatalf("template_rebuilds = %d, want 1", got)
	}
	if got := int(entry["template_interruptions"].(float64)); got != 1 {
		t.Fatalf("template_interruptions = %d, want 1", got)
	}
	if got := int(entry["orphan_promotions"].(float64)); got != 1 {
		t.Fatalf("orphan_promotions = %d, want 1", got)
	}
	if got := int(entry["mempool_txs"].(float64)); got != 1 {
		t.Fatalf("mempool_txs = %d, want 1", got)
	}
	if got := int(entry["candidate_frontier"].(float64)); got != 1 {
		t.Fatalf("candidate_frontier = %d, want 1", got)
	}
	if got := int(entry["peer_count"].(float64)); got != 2 {
		t.Fatalf("peer_count = %d, want 2", got)
	}
	if got := int(entry["useful_peers"].(float64)); got != 1 {
		t.Fatalf("useful_peers = %d, want 1", got)
	}
	if got := int(entry["relay_queue_depth"].(float64)); got != 2 {
		t.Fatalf("relay_queue_depth = %d, want 2", got)
	}
	if got := int(entry["relay_queue_depth_peak"].(float64)); got != 2 {
		t.Fatalf("relay_queue_depth_peak = %d, want 2", got)
	}
	if got := int(entry["pending_peer_blocks"].(float64)); got != 1 {
		t.Fatalf("pending_peer_blocks = %d, want 1", got)
	}
	if got := int(entry["inflight_block_requests"].(float64)); got != 2 {
		t.Fatalf("inflight_block_requests = %d, want 2", got)
	}
	if got := int(entry["inflight_tx_requests"].(float64)); got != 3 {
		t.Fatalf("inflight_tx_requests = %d, want 3", got)
	}
}

func TestEmitNodeStatusLogsPhaseTransitionsAndHealthSnapshot(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}
	tx := spendTxForNodeTest(t, 1, types.OutPoint{TxID: [32]byte{1}, Vout: 0}, 50, 2, 1)
	if _, err := pool.AcceptTxWithParams(tx, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept tx: %v", err)
	}

	now := time.Unix(1_700_000_120, 0).UTC()
	tipHeight := uint64(2)
	headerHeight := uint64(4)
	tipHeader := types.BlockHeader{Timestamp: uint64(now.Add(-16 * time.Second).Unix())}
	headerTip := types.BlockHeader{Timestamp: uint64(now.Add(-8 * time.Second).Unix())}
	peerA := newPeerConnForTests("127.0.0.1:18444")
	peerA.outbound = true
	peerA.syncState.lastUsefulAt = now.Add(-5 * time.Second)
	peerB := newPeerConnForTests("127.0.0.1:18445")
	peerB.syncState.headersRequestedAt = now.Add(-2 * time.Second)

	svc := &Service{
		cfg: ServiceConfig{
			P2PAddr:      "127.0.0.1:18444",
			StallTimeout: 15 * time.Second,
			MaxOrphans:   8,
		},
		logger: logger.With("node", "NODE1234"),
		pool:   pool,
		peers:  map[string]*peerConn{peerA.addr: peerA, peerB.addr: peerB},
		blockRequests: map[[32]byte]blockDownloadRequest{
			{9}:  {},
			{10}: {},
		},
		txRequests: map[[32]byte]blockDownloadRequest{
			{11}: {},
		},
		pendingBlocks: map[[32]byte]pendingPeerBlock{
			{12}: {},
		},
		chainState: &PersistentChainState{
			state: &ChainState{
				params:    consensus.ParamsForProfile(types.Regtest),
				height:    &tipHeight,
				tipHeader: &tipHeader,
			},
		},
		headerChain: &HeaderChain{
			params:    consensus.ParamsForProfile(types.Regtest),
			height:    &headerHeight,
			tipHeader: &headerTip,
		},
		stopCh: make(chan struct{}),
	}
	peerA.svc = svc
	peerB.svc = svc

	svc.emitNodeStatus(now)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("status log line count = %d, want 3\n%s", len(lines), buf.String())
	}
	entries := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		entries = append(entries, entry)
	}

	if got := entries[0]["message"]; got != "sync phase changed" {
		t.Fatalf("first message = %v, want sync phase changed", got)
	}
	if got := entries[0]["to"]; got != "catching_up_blocks" {
		t.Fatalf("sync phase = %v, want catching_up_blocks", got)
	}
	if got := entries[1]["message"]; got != "mempool pressure changed" {
		t.Fatalf("second message = %v, want mempool pressure changed", got)
	}
	if got := entries[1]["to"]; got != "normal" {
		t.Fatalf("mempool pressure = %v, want normal", got)
	}
	status := entries[2]
	if got := status["message"]; got != "node status" {
		t.Fatalf("status message = %v, want node status", got)
	}
	if got := status["node"]; got != "NODE1234" {
		t.Fatalf("node = %v, want NODE1234", got)
	}
	if got := status["phase"]; got != "catching_up_blocks" {
		t.Fatalf("phase = %v, want catching_up_blocks", got)
	}
	if got := int(status["peer_count"].(float64)); got != 2 {
		t.Fatalf("peer_count = %d, want 2", got)
	}
	if got := int(status["outbound_peers"].(float64)); got != 1 {
		t.Fatalf("outbound_peers = %d, want 1", got)
	}
	if got := int(status["inbound_peers"].(float64)); got != 1 {
		t.Fatalf("inbound_peers = %d, want 1", got)
	}
	if got := int(status["mempool_txs"].(float64)); got != 1 {
		t.Fatalf("mempool_txs = %d, want 1", got)
	}
	if got := int(status["inflight_block_requests"].(float64)); got != 2 {
		t.Fatalf("inflight_block_requests = %d, want 2", got)
	}
	if got := int(status["pending_peer_blocks"].(float64)); got != 1 {
		t.Fatalf("pending_peer_blocks = %d, want 1", got)
	}
	if got := int(status["useful_peers"].(float64)); got != 1 {
		t.Fatalf("useful_peers = %d, want 1", got)
	}
}
