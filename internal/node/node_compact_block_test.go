package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"testing"
	"time"
)

func TestReconstructXThinBlockFromMempoolOverlap(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}
	parent := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTxWithParams(parent, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTxForNodeTest(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTxWithParams(child, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}

	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 99},
		Txs: []types.Transaction{
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}}),
			parent,
			child,
		},
	}
	msg := buildXThinBlockMessage(block)
	thin, ok := msg.(p2p.XThinBlockMessage)
	if !ok {
		t.Fatalf("relay message type = %T, want XThinBlockMessage", msg)
	}
	matches := pool.ShortIDMatches(func(txid [32]byte) uint64 {
		return thinBlockShortID(thin.Nonce, txid)
	}, xThinShortIDSet(thin))
	state, missing := reconstructXThinBlock(thin, matches)
	if len(missing) != 0 {
		t.Fatalf("missing indexes = %v, want none", missing)
	}
	if !state.complete() {
		t.Fatal("expected complete thin block reconstruction")
	}
	reconstructed := state.block()
	if len(reconstructed.Txs) != len(block.Txs) {
		t.Fatalf("tx count = %d, want %d", len(reconstructed.Txs), len(block.Txs))
	}
	for i := range block.Txs {
		if consensus.TxID(&reconstructed.Txs[i]) != consensus.TxID(&block.Txs[i]) {
			t.Fatalf("tx %d mismatch", i)
		}
	}
}

func TestReconstructCompactBlockFromMempoolOverlap(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{11}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}
	parent := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTxWithParams(parent, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTxForNodeTest(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTxWithParams(child, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}

	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 199},
		Txs: []types.Transaction{
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}}),
			parent,
			child,
		},
	}
	msg := buildCompactBlockMessage(block)
	compact, ok := msg.(p2p.CompactBlockMessage)
	if !ok {
		t.Fatalf("relay message type = %T, want CompactBlockMessage", msg)
	}
	matches := pool.ShortIDMatches(func(txid [32]byte) uint64 {
		return thinBlockShortID(compact.Nonce, txid)
	}, compactShortIDSet(compact))
	state, missing := reconstructCompactBlock(compact, matches)
	if len(missing) != 0 {
		t.Fatalf("missing indexes = %v, want none", missing)
	}
	if !state.complete() {
		t.Fatal("expected complete compact block reconstruction")
	}
	reconstructed := state.block()
	for i := range block.Txs {
		if consensus.TxID(&reconstructed.Txs[i]) != consensus.TxID(&block.Txs[i]) {
			t.Fatalf("tx %d mismatch", i)
		}
	}
}

func TestOnXThinBlockRequestsMissingIndexes(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{2}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}
	parent := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTxWithParams(parent, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTxForNodeTest(t, 2, types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}, 49, 3, 1)
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 100},
		Txs: []types.Transaction{
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}}),
			parent,
			child,
		},
	}
	thin := buildXThinBlockMessage(block).(p2p.XThinBlockMessage)
	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	if err := svc.onXThinBlockMessage(peer, thin); err != nil {
		t.Fatalf("onXThinBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetXBlockTxMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetXBlockTxMessage", envelope.msg)
	}
	if len(req.Indexes) != 1 || req.Indexes[0] != 2 {
		t.Fatalf("requested indexes = %v, want [2]", req.Indexes)
	}
	if _, ok := peer.pendingThinState(req.BlockHash); !ok {
		t.Fatal("expected pending thin block state")
	}
}

func TestOnXThinBlockFallsBackToFullBlockWhenOverlapIsLow(t *testing.T) {
	svc := &Service{pool: mempool.New()}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}

	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 101},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})},
	}
	for i := 0; i < 8; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 1)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1), PubKey: nodeSignerPubKey(byte(i + 1))}},
			},
		})
	}

	thin := buildXThinBlockMessage(block).(p2p.XThinBlockMessage)
	if err := svc.onXThinBlockMessage(peer, thin); err != nil {
		t.Fatalf("onXThinBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetDataMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetDataMessage", envelope.msg)
	}
	if len(req.Items) != 1 || req.Items[0].Type != p2p.InvTypeBlockFull {
		t.Fatalf("unexpected fallback request: %+v", req.Items)
	}
}

func TestOnCompactBlockRequestsMissingIndexes(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{12}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}
	parent := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTxWithParams(parent, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTxForNodeTest(t, 2, types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}, 49, 3, 1)
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 200},
		Txs: []types.Transaction{
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}}),
			parent,
			child,
		},
	}
	compact := buildCompactBlockMessage(block).(p2p.CompactBlockMessage)
	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	if err := svc.onCompactBlockMessage(peer, compact); err != nil {
		t.Fatalf("onCompactBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetBlockTxMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetBlockTxMessage", envelope.msg)
	}
	if len(req.Indexes) != 1 || req.Indexes[0] != 2 {
		t.Fatalf("requested indexes = %v, want [2]", req.Indexes)
	}
}

func TestOnCompactBlockFallsBackToFullBlockWhenOverlapIsLow(t *testing.T) {
	svc := &Service{pool: mempool.New()}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 201},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})},
	}
	for i := 0; i < 8; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 1)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1), PubKey: nodeSignerPubKey(byte(i + 1))}},
			},
		})
	}
	compact := buildCompactBlockMessage(block).(p2p.CompactBlockMessage)
	if err := svc.onCompactBlockMessage(peer, compact); err != nil {
		t.Fatalf("onCompactBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetDataMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetDataMessage", envelope.msg)
	}
	if len(req.Items) != 1 || req.Items[0].Type != p2p.InvTypeBlockExtended {
		t.Fatalf("unexpected fallback request: %+v", req.Items)
	}
}

func TestOnCompactBlockFallsBackToFullBlockWithoutExtendedSupport(t *testing.T) {
	svc := &Service{pool: mempool.New()}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		version: p2p.VersionMessage{
			Services: p2p.ServiceNodeNetwork | p2p.ServiceCompactBlockRelay,
		},
	}
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 202},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})},
	}
	for i := 0; i < 8; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 21)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1), PubKey: nodeSignerPubKey(byte(i + 1))}},
			},
		})
	}
	compact := buildCompactBlockMessage(block).(p2p.CompactBlockMessage)
	if err := svc.onCompactBlockMessage(peer, compact); err != nil {
		t.Fatalf("onCompactBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetDataMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetDataMessage", envelope.msg)
	}
	if len(req.Items) != 1 || req.Items[0].Type != p2p.InvTypeBlockFull {
		t.Fatalf("unexpected fallback request: %+v", req.Items)
	}
}

func TestSelectBlockRelayPlanChoosesExtendedForPoorOverlap(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 203},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})},
	}
	for i := 0; i < 16; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 41)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1), PubKey: nodeSignerPubKey(byte(i + 1))}},
			},
		})
	}
	if plan := selectBlockRelayPlan(peer, block); plan != blockRelayPlanGrapheneExtended {
		t.Fatalf("relay plan = %d, want extended", plan)
	}
	for _, tx := range block.Txs[1:] {
		peer.noteKnownTxIDs([][32]byte{consensus.TxID(&tx)})
	}
	if plan := selectBlockRelayPlan(peer, block); plan != blockRelayPlanCompactFallback {
		t.Fatalf("relay plan = %d, want compact fallback", plan)
	}
}

func TestPreferredBlockRelayMessageUsesExtendedThenCompactAsPeerWarms(t *testing.T) {
	svc := &Service{
		recentBlks: recentBlockCache{items: make(map[[32]byte]types.Block)},
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.version.Services = p2p.ServiceNodeNetwork | p2p.ServiceCompactBlockRelay | p2p.ServiceGrapheneExtended

	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 204},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})},
	}
	for i := 0; i < 160; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 41)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1), PubKey: nodeSignerPubKey(byte(i + 1))}},
			},
		})
	}
	hash := consensus.HeaderHash(&block.Header)
	svc.recentMu.Lock()
	cacheRecentBlockLocked(&svc.recentBlks, hash, block)
	svc.recentMu.Unlock()

	msg, ok, err := svc.preferredBlockRelayMessage(peer, hash)
	if err != nil {
		t.Fatalf("preferredBlockRelayMessage: %v", err)
	}
	if !ok {
		t.Fatal("expected cached block to be available")
	}
	if _, ok := msg.(p2p.XThinBlockMessage); !ok {
		t.Fatalf("message type = %T, want XThinBlockMessage", msg)
	}

	for _, tx := range block.Txs[1:] {
		peer.noteKnownTxIDs([][32]byte{consensus.TxID(&tx)})
	}

	msg, ok, err = svc.preferredBlockRelayMessage(peer, hash)
	if err != nil {
		t.Fatalf("preferredBlockRelayMessage warmed: %v", err)
	}
	if !ok {
		t.Fatal("expected cached block to be available after warm peer")
	}
	if _, ok := msg.(p2p.CompactBlockMessage); !ok {
		t.Fatalf("message type = %T, want CompactBlockMessage", msg)
	}
}

func TestBroadcastMinedCompactBlockOnlyTargetsCompactPeers(t *testing.T) {
	svc := &Service{peers: make(map[string]*peerConn)}
	compactPeer := newPeerConnForTests("127.0.0.1:18444")
	fullOnlyPeer := newPeerConnForTests("127.0.0.1:18445")
	fullOnlyPeer.version.Services = p2p.ServiceNodeNetwork
	svc.peers[compactPeer.addr] = compactPeer
	svc.peers[fullOnlyPeer.addr] = fullOnlyPeer
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 204},
		Txs: []types.Transaction{
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}}),
			{
				Base: types.TxBase{
					Version: 1,
					Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{0xaa}, Vout: 0}}},
					Outputs: []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(10)}},
				},
			},
		},
	}

	svc.broadcastMinedCompactBlock(block)

	select {
	case envelope := <-compactPeer.sendQ:
		if _, ok := envelope.msg.(p2p.CompactBlockMessage); !ok {
			t.Fatalf("compact peer message = %T, want CompactBlockMessage", envelope.msg)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for compact relay")
	}
	select {
	case envelope := <-fullOnlyPeer.sendQ:
		t.Fatalf("full-only peer received unexpected message: %T", envelope.msg)
	default:
	}
}

func TestKnownCompactBlockDoesNotTriggerRecovery(t *testing.T) {
	genesis := genesisBlock()
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
	block := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	hash := consensus.HeaderHash(&block.Header)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 16)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: block}); err != nil {
		t.Fatalf("onPeerMessage block: %v", err)
	}
	if scheduled := svc.scheduleBlockRequests(peer.addr, [][32]byte{hash}, 1); len(scheduled) != 1 {
		t.Fatalf("scheduled block requests = %d, want 1", len(scheduled))
	}

	compact := buildCompactBlockMessage(block).(p2p.CompactBlockMessage)
	if err := svc.onCompactBlockMessage(peer, compact); err != nil {
		t.Fatalf("onCompactBlockMessage duplicate: %v", err)
	}
	if got := svc.inflightBlockRequestCount(); got != 0 {
		t.Fatalf("inflight block requests after duplicate compact = %d, want 0", got)
	}
	if got := svc.throughput.compactBlockFallbacks.Load(); got != 0 {
		t.Fatalf("compact fallbacks = %d, want 0", got)
	}
	select {
	case envelope := <-peer.controlQ:
		t.Fatalf("duplicate compact block queued unexpected message: %T", envelope.msg)
	default:
	}
}
