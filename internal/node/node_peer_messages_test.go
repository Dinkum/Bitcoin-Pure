package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestOnInvMessageRequestsHeadersThroughLastUnknownBlock(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	first := [32]byte{0x11}
	second := [32]byte{0x22}
	msg := p2p.InvMessage{Items: []p2p.InvVector{
		{Type: p2p.InvTypeBlock, Hash: first},
		{Type: p2p.InvTypeBlock, Hash: second},
	}}
	if err := svc.onInvMessage(peer, msg); err != nil {
		t.Fatalf("onInvMessage: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		req, ok := envelope.msg.(p2p.GetHeadersMessage)
		if !ok {
			t.Fatalf("message type = %T, want GetHeadersMessage", envelope.msg)
		}
		if req.StopHash != second {
			t.Fatalf("stop hash = %x, want %x", req.StopHash, second)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for getheaders request")
	}
}

func TestOnInvMessageRequestsFullBlockForKnownHeader(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	svc.cacheRecentHeader(block.Header)

	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := svc.onInvMessage(peer, p2p.InvMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}}}); err != nil {
		t.Fatalf("onInvMessage: %v", err)
	}

	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetDataMessage)
	if !ok {
		t.Fatalf("message type = %T, want GetDataMessage", envelope.msg)
	}
	if len(req.Items) != 1 || req.Items[0].Hash != hash || req.Items[0].Type != p2p.InvTypeBlockFull {
		t.Fatalf("GetData items = %+v, want full block request for %x", req.Items, hash)
	}
}

func TestOnInvMessageSkipsRecentlyRejectedBlock(t *testing.T) {
	svc := &Service{
		pool:           mempool.New(),
		rejectedBlocks: make(map[[32]byte]struct{}),
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	hash := [32]byte{0xaa}
	svc.rememberRejectedBlock(hash)

	if err := svc.onInvMessage(peer, p2p.InvMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}}}); err != nil {
		t.Fatalf("onInvMessage: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		t.Fatalf("unexpected outbound message for rejected block inv: %T", envelope.msg)
	default:
	}
}

func TestOnTxReconMessageRequestsOnlyMissingTxIDs(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{61}, Vout: 0}
	tx := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTxWithParams(tx, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept tx: %v", err)
	}

	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	missing := [32]byte{0xaa}
	if err := svc.onTxReconMessage(peer, p2p.TxReconMessage{TxIDs: [][32]byte{consensus.TxID(&tx), missing}}); err != nil {
		t.Fatalf("onTxReconMessage: %v", err)
	}

	req := waitForTxRequestMessage(t, peer, 50*time.Millisecond)
	if len(req.TxIDs) != 1 || req.TxIDs[0] != missing {
		t.Fatalf("requested txids = %x, want only missing tx", req.TxIDs)
	}
}

func TestOnTxReconMessageDoesNotDuplicateInflightTxRequestsAcrossPeers(t *testing.T) {
	svc := &Service{
		pool:       mempool.NewWithConfig(mempool.PoolConfig{MinRelayFeePerByte: 0, MaxTxSize: 1_000_000, MaxAncestors: 256, MaxDescendants: 256, MaxOrphans: 8}),
		txRequests: make(map[[32]byte]blockDownloadRequest),
	}
	firstPeer := &peerConn{
		addr:        "127.0.0.1:18444",
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		svc:         svc,
	}
	secondPeer := &peerConn{
		addr:        "127.0.0.1:18445",
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		svc:         svc,
	}
	missing := [32]byte{0xbb}

	if err := svc.onTxReconMessage(firstPeer, p2p.TxReconMessage{TxIDs: [][32]byte{missing}}); err != nil {
		t.Fatalf("first onTxReconMessage: %v", err)
	}
	firstReq := waitForTxRequestMessage(t, firstPeer, 50*time.Millisecond)
	if len(firstReq.TxIDs) != 1 || firstReq.TxIDs[0] != missing {
		t.Fatalf("first requested txids = %x, want %x", firstReq.TxIDs, missing)
	}

	if err := svc.onTxReconMessage(secondPeer, p2p.TxReconMessage{TxIDs: [][32]byte{missing}}); err != nil {
		t.Fatalf("second onTxReconMessage: %v", err)
	}
	select {
	case envelope := <-secondPeer.sendQ:
		t.Fatalf("unexpected duplicate tx request message: %T", envelope.msg)
	default:
	}
}

func TestOnGetDataMessageBatchesTxLookupsAndReportsMisses(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{62}, Vout: 0}
	tx := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTxWithParams(tx, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}, consensus.RegtestParams(), consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept tx: %v", err)
	}

	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	missing := [32]byte{0xbb}
	msg := p2p.GetDataMessage{Items: []p2p.InvVector{
		{Type: p2p.InvTypeTx, Hash: consensus.TxID(&tx)},
		{Type: p2p.InvTypeTx, Hash: missing},
	}}
	if err := svc.onGetDataMessage(peer, msg); err != nil {
		t.Fatalf("onGetDataMessage: %v", err)
	}

	var sawBatch bool
	var sawNotFound bool
	for i := 0; i < 2; i++ {
		envelope := <-peer.sendQ
		switch msg := envelope.msg.(type) {
		case p2p.TxBatchMessage:
			if len(msg.Txs) != 1 || consensus.TxID(&msg.Txs[0]) != consensus.TxID(&tx) {
				t.Fatalf("unexpected tx batch payload")
			}
			sawBatch = true
		case p2p.NotFoundMessage:
			if len(msg.Items) != 1 || msg.Items[0].Hash != missing {
				t.Fatalf("unexpected notfound payload: %+v", msg.Items)
			}
			sawNotFound = true
		default:
			t.Fatalf("unexpected message type %T", envelope.msg)
		}
	}
	if !sawBatch || !sawNotFound {
		t.Fatalf("expected both tx batch and notfound, sawBatch=%t sawNotFound=%t", sawBatch, sawNotFound)
	}
}

func TestOnGetDataMessageCapsServedBlocksPerRequest(t *testing.T) {
	svc := &Service{
		recentHdrs: recentHeaderCache{items: make(map[[32]byte]types.BlockHeader)},
		recentBlks: recentBlockCache{items: make(map[[32]byte]types.Block)},
	}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, maxServedBlocksPerGetData+2),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}

	items := make([]p2p.InvVector, 0, maxServedBlocksPerGetData+2)
	overflow := make([][32]byte, 0, 2)
	for i := 0; i < maxServedBlocksPerGetData+2; i++ {
		block := pendingPeerBlockForTest(uint64(200+i), 2)
		hash := consensus.HeaderHash(&block.Header)
		svc.cacheRecentBlock(block)
		items = append(items, p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: hash})
		if i >= maxServedBlocksPerGetData {
			overflow = append(overflow, hash)
		}
	}

	if err := svc.onGetDataMessage(peer, p2p.GetDataMessage{Items: items}); err != nil {
		t.Fatalf("onGetDataMessage: %v", err)
	}

	sawBlocks := 0
	var sawNotFound bool
	for i := 0; i < maxServedBlocksPerGetData+1; i++ {
		envelope := <-peer.sendQ
		if envelope.blockRef != nil {
			if envelope.blockRef.item.Type != p2p.InvTypeBlockFull {
				t.Fatalf("unexpected lazy block type: %+v", envelope.blockRef.item)
			}
			sawBlocks++
			continue
		}
		switch msg := envelope.msg.(type) {
		case p2p.NotFoundMessage:
			if len(msg.Items) != len(overflow) {
				t.Fatalf("notfound count = %d, want %d", len(msg.Items), len(overflow))
			}
			for idx, item := range msg.Items {
				if item.Type != p2p.InvTypeBlockFull || item.Hash != overflow[idx] {
					t.Fatalf("unexpected notfound item %d: %+v", idx, item)
				}
			}
			sawNotFound = true
		default:
			t.Fatalf("unexpected message type %T", envelope.msg)
		}
	}
	if sawBlocks != maxServedBlocksPerGetData || !sawNotFound {
		t.Fatalf("served blocks=%d notfound=%t", sawBlocks, sawNotFound)
	}
}

func TestOnPeerHeadersUpdatesObservedPeerHeight(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, state.UTXOs(), 4, first.Header.Timestamp+600)

	peer := &peerConn{
		addr:        "127.0.0.1:18444",
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		version:     p2p.VersionMessage{Height: 0, UserAgent: "bpu/go"},
	}

	if err := svc.onPeerMessage(peer, p2p.HeadersMessage{Headers: []types.BlockHeader{first.Header, second.Header}}); err != nil {
		t.Fatalf("onPeerMessage headers: %v", err)
	}
	if got := peer.snapshotHeight(); got != 2 {
		t.Fatalf("observed peer height = %d, want 2", got)
	}
}

func TestOnPeerHeadersAcceptsCompetingBranchFromKnownParent(t *testing.T) {
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
	mainFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if err := svc.onPeerMessage(newPeerConnForTests("127.0.0.1:18445"), p2p.HeadersMessage{Headers: []types.BlockHeader{mainFirst.Header}}); err != nil {
		t.Fatalf("seed main header: %v", err)
	}

	altFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 4, genesis.Header.Timestamp+601)
	altState := baseState.Clone()
	if _, err := altState.ApplyBlock(&altFirst); err != nil {
		t.Fatal(err)
	}
	altSecond := nextCoinbaseBlock(1, altFirst.Header, altState.UTXOs(), 5, altFirst.Header.Timestamp+600)

	peer := newPeerConnForTests("127.0.0.1:18446")
	if err := svc.onPeerMessage(peer, p2p.HeadersMessage{Headers: []types.BlockHeader{altFirst.Header, altSecond.Header}}); err != nil {
		t.Fatalf("competing headers: %v", err)
	}
	if got := peer.snapshotHeight(); got != 2 {
		t.Fatalf("observed peer height = %d, want 2", got)
	}
	if got := svc.HeaderHeight(); got != 2 {
		t.Fatalf("header height = %d, want 2", got)
	}
	if got := consensus.HeaderHash(svc.headerChain.TipHeader()); got != consensus.HeaderHash(&altSecond.Header) {
		t.Fatalf("header tip = %x, want %x", got, consensus.HeaderHash(&altSecond.Header))
	}
}

func TestApplyPeerHeadersInactiveBranchDoesNotRewriteActiveLocatorBase(t *testing.T) {
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
	mainFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if err := svc.onPeerMessage(newPeerConnForTests("127.0.0.1:18445"), p2p.HeadersMessage{Headers: []types.BlockHeader{mainFirst.Header}}); err != nil {
		t.Fatalf("seed main header: %v", err)
	}

	altFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 4, genesis.Header.Timestamp+601)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{altFirst.Header}); err != nil {
		t.Fatalf("apply competing header: %v", err)
	}

	svc.stateMu.RLock()
	height, err := svc.findLocatorHeightLocked([][32]byte{consensus.HeaderHash(&altFirst.Header), consensus.HeaderHash(&genesis.Header)})
	svc.stateMu.RUnlock()
	if err != nil {
		t.Fatalf("findLocatorHeightLocked: %v", err)
	}
	if height != 0 {
		t.Fatalf("locator height = %d, want 0 because competing header is not active", height)
	}

	hashAtHeight, err := svc.chainState.Store().GetHeaderHashByHeight(1)
	if err != nil {
		t.Fatalf("GetHeaderHashByHeight: %v", err)
	}
	mainHash := consensus.HeaderHash(&mainFirst.Header)
	if hashAtHeight == nil || *hashAtHeight != mainHash {
		t.Fatalf("active height hash = %x, want %x", hashAtHeight, mainHash)
	}
}

func TestOpenServiceRestoresPromotedHigherWorkHeaderBranch(t *testing.T) {
	path := t.TempDir()
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}

	baseState := NewChainState(types.Regtest)
	if _, err := baseState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	mainFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{mainFirst.Header}); err != nil {
		t.Fatalf("apply main header: %v", err)
	}

	altFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 4, genesis.Header.Timestamp+601)
	altState := baseState.Clone()
	if _, err := altState.ApplyBlock(&altFirst); err != nil {
		t.Fatal(err)
	}
	altSecond := nextCoinbaseBlock(1, altFirst.Header, altState.UTXOs(), 5, altFirst.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{altFirst.Header, altSecond.Header}); err != nil {
		t.Fatalf("apply competing headers: %v", err)
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

	if got := reopened.HeaderHeight(); got != 2 {
		t.Fatalf("reopened header height = %d, want 2", got)
	}
	if got := consensus.HeaderHash(reopened.headerChain.TipHeader()); got != consensus.HeaderHash(&altSecond.Header) {
		t.Fatalf("reopened header tip = %x, want %x", got, consensus.HeaderHash(&altSecond.Header))
	}

	altFirstHash := consensus.HeaderHash(&altFirst.Header)
	hashAtHeight, err := reopened.chainState.Store().GetHeaderHashByHeight(1)
	if err != nil {
		t.Fatalf("GetHeaderHashByHeight(1): %v", err)
	}
	if hashAtHeight == nil || *hashAtHeight != altFirstHash {
		t.Fatalf("active header hash at height 1 = %x, want %x", hashAtHeight, altFirstHash)
	}

	altSecondHash := consensus.HeaderHash(&altSecond.Header)
	hashAtHeight, err = reopened.chainState.Store().GetHeaderHashByHeight(2)
	if err != nil {
		t.Fatalf("GetHeaderHashByHeight(2): %v", err)
	}
	if hashAtHeight == nil || *hashAtHeight != altSecondHash {
		t.Fatalf("active header hash at height 2 = %x, want %x", hashAtHeight, altSecondHash)
	}
}

func TestOnPeerTxBatchIgnoresDuplicateAdmissionError(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()
	matureGenesisForNodeTest(t, svc)

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	if err := svc.onPeerMessage(peer, p2p.TxBatchMessage{Txs: []types.Transaction{tx}}); err != nil {
		t.Fatalf("onPeerMessage duplicate batch: %v", err)
	}
}

func TestHandlePeerTxAdmissionErrorsScoresInvalidSignaturesBeforeBan(t *testing.T) {
	svc := &Service{
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		knownPeers:  make(map[string]storage.KnownPeerRecord),
		bannedPeers: make(map[string]time.Time),
	}
	peer := newPeerConnForTests("127.0.0.1:18444")

	for attempt := 1; attempt < peerInvalidTxSignatureLimit; attempt++ {
		if err := svc.handlePeerTxAdmissionErrors(peer, []error{consensus.ErrInvalidSignature}); err != nil {
			t.Fatalf("attempt %d returned error %v, want warning-only", attempt, err)
		}
	}

	err := svc.handlePeerTxAdmissionErrors(peer, []error{consensus.ErrInvalidSignature})
	if !errors.Is(err, consensus.ErrInvalidSignature) {
		t.Fatalf("threshold error = %v, want %v", err, consensus.ErrInvalidSignature)
	}
	until, banned := svc.peerManager().bannedUntil(peer.addr)
	if !banned {
		t.Fatal("peer should be banned after repeated invalid signatures")
	}
	if until.Before(time.Now().UTC()) {
		t.Fatalf("ban expiry = %s, want future time", until)
	}
}

func TestApplyPeerBlockDoesNotRequireServiceStateMu(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}

	done := make(chan error, 1)
	svc.stateMu.RLock()
	go func() {
		applied, _, _, err := svc.applyPeerBlock(&block)
		if err != nil {
			done <- err
			return
		}
		if !applied {
			done <- fmt.Errorf("peer block did not become active")
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		svc.stateMu.RUnlock()
		if err != nil {
			t.Fatalf("applyPeerBlock under stateMu reader: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		svc.stateMu.RUnlock()
		t.Fatal("applyPeerBlock blocked on service stateMu")
	}
}

func TestOnPeerBlockMessageIgnoresKnownBlock(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if applied, _, _, err := svc.applyPeerBlock(&block); err != nil || !applied {
		t.Fatalf("applyPeerBlock = (%v, %v), want (true, nil)", applied, err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 4)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: block}); err != nil {
		t.Fatalf("onPeerMessage known block: %v", err)
	}
}

func TestOnPeerBlockMessageRequestsHeadersWhenParentUnknown(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 4)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: block}); err != nil {
		t.Fatalf("onPeerMessage child without header: %v", err)
	}

	select {
	case envelope := <-peer.controlQ:
		req, ok := envelope.msg.(p2p.GetHeadersMessage)
		if !ok {
			t.Fatalf("message type = %T, want GetHeadersMessage", envelope.msg)
		}
		if req.StopHash != consensus.HeaderHash(&block.Header) {
			t.Fatalf("stop hash = %x, want %x", req.StopHash, consensus.HeaderHash(&block.Header))
		}
	default:
		t.Fatal("expected catch-up headers request")
	}
}

func TestOnPeerBlockMessageRequestsCatchUpForUnavailableParentState(t *testing.T) {
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
	first := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	firstState := baseState.Clone()
	if _, err := firstState.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, firstState.UTXOs(), 4, first.Header.Timestamp+600)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{first.Header, second.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 8)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: second}); err != nil {
		t.Fatalf("onPeerMessage child before parent block: %v", err)
	}

	var sawHeaders bool
	var sawGetData bool
	for i := 0; i < 2; i++ {
		select {
		case envelope := <-peer.controlQ:
			switch msg := envelope.msg.(type) {
			case p2p.GetHeadersMessage:
				sawHeaders = true
				if msg.StopHash != consensus.HeaderHash(&second.Header) {
					t.Fatalf("stop hash = %x, want %x", msg.StopHash, consensus.HeaderHash(&second.Header))
				}
			case p2p.GetDataMessage:
				sawGetData = true
				if len(msg.Items) == 0 {
					t.Fatal("expected missing block requests")
				}
			default:
				t.Fatalf("unexpected message type %T", envelope.msg)
			}
		default:
		}
	}
	if !sawHeaders {
		t.Fatal("expected catch-up GetHeaders request")
	}
	if !sawGetData {
		t.Fatal("expected catch-up GetData request")
	}
}

func TestPeerAcceptedBlockSuppressesLaterInvRequest(t *testing.T) {
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
	if _, ok := svc.recentBlock(hash); !ok {
		t.Fatal("peer-accepted block was not cached for later inv suppression")
	}

	if err := svc.onInvMessage(peer, p2p.InvMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}}}); err != nil {
		t.Fatalf("onInvMessage duplicate block: %v", err)
	}
	select {
	case envelope := <-peer.controlQ:
		t.Fatalf("duplicate block inv queued unexpected message: %T", envelope.msg)
	default:
	}
}
