package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectPeerReconnectsAfterDisconnect(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var (
		mu      sync.Mutex
		accepts int
	)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			accepts++
			mu.Unlock()
			go func(conn net.Conn) {
				defer conn.Close()
				wire := p2p.NewConn(conn, p2p.MagicForProfile(types.Regtest), 8<<20)
				if _, err := p2p.Handshake(wire, p2p.VersionMessage{
					Protocol:  1,
					Height:    0,
					Nonce:     1,
					UserAgent: "peer-test",
				}, 2*time.Second); err != nil {
					return
				}
			}(conn)
		}
	}()

	if err := svc.ConnectPeer(ln.Addr().String()); err != nil {
		t.Fatalf("ConnectPeer: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := accepts
		mu.Unlock()
		if got >= 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	got := accepts
	mu.Unlock()
	t.Fatalf("accepted connections = %d, want at least 2", got)
}

func TestAutomaticOutboundHandshakeFailuresEvictTarget(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:          types.Regtest,
		DBPath:           t.TempDir(),
		MaxOutboundPeers: 1,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var accepts atomic.Int64
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			_ = conn.Close()
		}
	}()

	if err := svc.peerManager().connectPeer(ln.Addr().String(), false); err != nil {
		t.Fatalf("connectPeer: %v", err)
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if svc.outboundPeerCount() == 0 && accepts.Load() >= autoPeerFailureCeiling {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("outbound target was not evicted after handshake failures; accepts=%d outbound=%d", accepts.Load(), svc.outboundPeerCount())
}

func TestInboundHandshakeTimeoutIsExpectedFailure(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	_ = server.SetReadDeadline(time.Now().Add(-time.Second))
	_, err := server.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected read timeout")
	}
	if !isExpectedInboundHandshakeFailure(err) {
		t.Fatalf("inbound handshake timeout should be expected: %v", err)
	}
	if isExpectedPeerCloseError(err) {
		t.Fatalf("plain peer close classifier should not hide established peer timeout: %v", err)
	}
}

func TestOpenServiceRestoresPersistedVettedPeers(t *testing.T) {
	path := t.TempDir()
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	svc.recordKnownPeerSuccess("8.8.8.8:18444", time.Unix(1_700_000_000, 0))
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

	addrs := reopened.knownPeerAddrs()
	if len(addrs) != 1 || addrs[0] != "8.8.8.8:18444" {
		t.Fatalf("known peers after reopen = %v, want [8.8.8.8:18444]", addrs)
	}
	loaded, err := reopened.chainState.Store().LoadKnownPeers()
	if err != nil {
		t.Fatalf("LoadKnownPeers: %v", err)
	}
	record, ok := loaded["8.8.8.8:18444"]
	if !ok {
		t.Fatal("persisted vetted peer missing after reopen")
	}
	if record.LastSuccess.IsZero() {
		t.Fatal("persisted vetted peer missing last-success metadata")
	}
}

func TestRememberKnownPeersRejectsUnroutableAutoDiscoveryAddrs(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:          types.Regtest,
		DBPath:           t.TempDir(),
		MaxOutboundPeers: 1,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	svc.rememberKnownPeers([]string{
		" 8.8.8.8:18444 ",
		"[2001:4860:4860::8888]:18444",
		"127.0.0.1:18444",
		"10.0.0.2:18444",
		"100.64.0.1:18444",
		"localhost:18444",
		"203.0.113.1:18444",
		strings.Repeat("1", maxLearnedPeerAddrBytes+1) + ":18444",
	})

	addrs := svc.knownPeerAddrs()
	want := []string{"8.8.8.8:18444", "[2001:4860:4860::8888]:18444"}
	if !slices.Equal(addrs, want) {
		t.Fatalf("known peer addrs = %v, want %v", addrs, want)
	}

	if candidates := svc.outboundRefillCandidates(4); !slices.Equal(candidates, want) {
		t.Fatalf("outbound refill candidates = %v, want %v", candidates, want)
	}
}

func TestLoadPersistedKnownPeersFiltersAutoButPreservesManual(t *testing.T) {
	svc := &Service{
		cfg: ServiceConfig{P2PAddr: "0.0.0.0:18444"},
		knownPeers: map[string]storage.KnownPeerRecord{
			"1.1.1.1:18444": {Manual: true},
		},
	}
	manager := &peerManager{svc: svc}

	manager.loadPersistedKnownPeers(map[string]storage.KnownPeerRecord{
		"1.1.1.1:18444":   {},
		"8.8.4.4:18444":   {},
		"10.0.0.2:18444":  {},
		"localhost:18444": {},
		"10.0.0.3:18444":  {Manual: true},
	})

	addrs := manager.knownPeerAddrs()
	want := []string{"1.1.1.1:18444", "10.0.0.3:18444", "8.8.4.4:18444"}
	if !slices.Equal(addrs, want) {
		t.Fatalf("loaded known peers = %v, want %v", addrs, want)
	}
	if !svc.knownPeers["1.1.1.1:18444"].Manual {
		t.Fatal("persisted automatic record should not downgrade configured peer")
	}
}

func TestOutboundRefillCandidatesPreferDistinctNetgroupsAndHealthyPeers(t *testing.T) {
	svc := &Service{
		cfg: ServiceConfig{P2PAddr: "127.0.0.1:18444"},
		knownPeers: map[string]storage.KnownPeerRecord{
			"8.8.8.8:18444": {
				LastSeen:    time.Unix(1_700_000_000, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_000, 0).UTC(),
			},
			"8.8.4.4:18444": {
				LastSeen:    time.Unix(1_700_000_100, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_100, 0).UTC(),
			},
			"1.1.1.1:18444": {
				LastSeen:    time.Unix(1_700_000_050, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_050, 0).UTC(),
			},
			"9.9.9.9:18444": {
				LastSeen:     time.Unix(1_700_000_200, 0).UTC(),
				LastSuccess:  time.Unix(1_700_000_200, 0).UTC(),
				LastAttempt:  time.Now().UTC(),
				FailureCount: 5,
			},
		},
		outboundPeers: make(map[string]struct{}),
		peers:         make(map[string]*peerConn),
		stopCh:        make(chan struct{}),
	}
	manager := &peerManager{svc: svc}

	addrs := manager.outboundRefillCandidates(2)
	if len(addrs) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(addrs))
	}
	if peerNetgroup(addrs[0]) == peerNetgroup(addrs[1]) {
		t.Fatalf("candidate netgroups should differ, got %v", addrs)
	}
	for _, addr := range addrs {
		if addr == "9.9.9.9:18444" {
			t.Fatalf("unhealthy peer should be deprioritized, got %v", addrs)
		}
	}
}

func TestKnownPeerAddrsSkipsSelfEquivalentAddresses(t *testing.T) {
	svc := &Service{
		cfg: ServiceConfig{
			P2PAddr: "0.0.0.0:18444",
			Peers:   []string{"127.0.0.1:18444", "10.9.0.2:18444"},
		},
		knownPeers: map[string]storage.KnownPeerRecord{
			"localhost:18444": {},
			"8.8.8.8:18444":   {},
		},
	}
	manager := &peerManager{svc: svc}

	addrs := manager.knownPeerAddrs()
	if len(addrs) != 2 {
		t.Fatalf("known peer count = %d, want 2 (%v)", len(addrs), addrs)
	}
	for _, addr := range addrs {
		if addr == "127.0.0.1:18444" || addr == "localhost:18444" || addr == "0.0.0.0:18444" {
			t.Fatalf("self-equivalent address leaked into known peers: %v", addrs)
		}
	}
}

func TestOutboundRefillCandidatesSkipSelfEquivalentAddresses(t *testing.T) {
	svc := &Service{
		cfg: ServiceConfig{P2PAddr: "0.0.0.0:18444"},
		knownPeers: map[string]storage.KnownPeerRecord{
			"127.0.0.1:18444": {
				LastSeen:    time.Unix(1_700_000_000, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_000, 0).UTC(),
			},
			"8.8.8.8:18444": {
				LastSeen:    time.Unix(1_700_000_010, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_010, 0).UTC(),
			},
		},
		outboundPeers: make(map[string]struct{}),
		peers:         make(map[string]*peerConn),
		stopCh:        make(chan struct{}),
	}
	manager := &peerManager{svc: svc}

	addrs := manager.outboundRefillCandidates(2)
	if len(addrs) != 1 || addrs[0] != "8.8.8.8:18444" {
		t.Fatalf("outbound refill candidates = %v, want [8.8.8.8:18444]", addrs)
	}
}

func TestConnectPeerSkipsSelfEquivalentAddress(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
		P2PAddr: "0.0.0.0:18444",
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	if err := svc.ConnectPeer("127.0.0.1:18444"); err != nil {
		t.Fatalf("ConnectPeer: %v", err)
	}
	if got := svc.outboundPeerCount(); got != 0 {
		t.Fatalf("outbound peer count = %d, want 0", got)
	}
	if addrs := svc.knownPeerAddrs(); len(addrs) != 0 {
		t.Fatalf("known peers = %v, want none", addrs)
	}
}

func TestHandlePeerBlockAcceptanceErrorBansPeerAndCachesRejectedBlock(t *testing.T) {
	svc := &Service{
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		knownPeers:     make(map[string]storage.KnownPeerRecord),
		bannedPeers:    make(map[string]time.Time),
		rejectedBlocks: make(map[[32]byte]struct{}),
		blockRequests:  make(map[[32]byte]blockDownloadRequest),
		pendingBlocks:  make(map[[32]byte]pendingPeerBlock),
		stopCh:         make(chan struct{}),
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	block := pendingPeerBlockForTest(99, 2)

	err := svc.handlePeerBlockAcceptanceError(peer, &block, consensus.ErrMerkleTxIDMismatch)
	if !errors.Is(err, consensus.ErrMerkleTxIDMismatch) {
		t.Fatalf("handlePeerBlockAcceptanceError error = %v, want %v", err, consensus.ErrMerkleTxIDMismatch)
	}
	hash := consensus.HeaderHash(&block.Header)
	if !svc.hasRejectedBlock(hash) {
		t.Fatalf("rejected block hash %x was not cached", hash)
	}
	until, banned := svc.peerManager().bannedUntil(peer.addr)
	if !banned {
		t.Fatal("peer was not banned after misbehavior block error")
	}
	if until.Before(time.Now().UTC()) {
		t.Fatalf("peer ban expiry = %s, want future time", until)
	}
}

func TestAcceptLoopRetriesTemporaryErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener := &scriptedListener{
		results: []scriptedAcceptResult{
			{err: temporaryAcceptTestError{err: errors.New("too many open files")}},
			{err: net.ErrClosed},
		},
		secondAccept: make(chan struct{}),
	}
	svc := &Service{
		logger:   logger,
		listener: listener,
		stopCh:   make(chan struct{}),
	}
	manager := &peerManager{svc: svc}

	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.acceptLoop()
	}()

	select {
	case <-listener.secondAccept:
	case <-time.After(time.Second):
		t.Fatal("accept loop did not retry after temporary accept error")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("accept loop did not exit after listener closed")
	}
	if got := listener.acceptCalls(); got != 2 {
		t.Fatalf("accept calls = %d, want 2", got)
	}
}

func TestSafeGoRecoversPanicAndRunsCleanup(t *testing.T) {
	var buf bytes.Buffer
	svc := &Service{
		logger: slog.New(slog.NewTextHandler(&buf, nil)),
	}
	cleaned := make(chan struct{})
	svc.safeGoWithCleanup("panic-test", func() {
		close(cleaned)
	}, func() {
		panic("boom")
	})

	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("panic cleanup did not run")
	}
	svc.wg.Wait()

	logOutput := buf.String()
	if !strings.Contains(logOutput, "goroutine panic") {
		t.Fatalf("expected panic log, got %q", logOutput)
	}
	if !strings.Contains(logOutput, "panic-test") {
		t.Fatalf("expected goroutine name in log, got %q", logOutput)
	}
}

func TestPeerWriteLoopSetsWriteDeadline(t *testing.T) {
	conn := &deadlineSpyConn{}
	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 1),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		wire:        p2p.NewConn(conn, p2p.MagicForProfile(types.Regtest), 8<<20),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	peer.sendQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if conn.sawNonZeroWriteDeadline() {
			close(svc.stopCh)
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(svc.stopCh)
	<-done
	t.Fatal("expected peer write loop to set a write deadline")
}

func TestPeerSendFailsFastWhenControlQueueIsSaturated(t *testing.T) {
	peer := &peerConn{
		controlQ:    make(chan outboundMessage, 1),
		sendQ:       make(chan outboundMessage, 1),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	peer.controlQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	start := time.Now()
	err := peer.send(p2p.PongMessage{Nonce: 2})
	if err == nil {
		t.Fatal("expected saturated control queue error")
	}
	if !strings.Contains(err.Error(), "peer send queue saturated") {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > controlMessageEnqueueTimeout*3 {
		t.Fatalf("send blocked too long: %s", elapsed)
	}
}

func TestPeerWriteLoopWakesForControlQueueTraffic(t *testing.T) {
	conn := &deadlineSpyConn{}
	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		wire:        p2p.NewConn(conn, p2p.MagicForProfile(types.Regtest), 1_000_000),
		controlQ:    make(chan outboundMessage, 1),
		sendQ:       make(chan outboundMessage, 1),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	time.Sleep(20 * time.Millisecond)
	peer.controlQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if conn.sawNonZeroWriteDeadline() {
			close(svc.stopCh)
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(svc.stopCh)
	<-done
	t.Fatal("expected peer write loop to wake up for control-queue traffic")
}

func TestPeerWriteLoopPrefersPriorityRelayQueue(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()

	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		wire:           p2p.NewConn(local, p2p.MagicForProfile(types.Regtest), 1_000_000),
		controlQ:       make(chan outboundMessage, 1),
		relayPriorityQ: make(chan outboundMessage, 1),
		sendQ:          make(chan outboundMessage, 1),
		closed:         make(chan struct{}),
		queuedInv:      make(map[p2p.InvVector]int),
		queuedTx:       make(map[[32]byte]int),
		knownTx:        make(map[[32]byte]struct{}),
		pendingThin:    make(map[[32]byte]*pendingThinBlock),
	}
	remoteWire := p2p.NewConn(remote, p2p.MagicForProfile(types.Regtest), 1_000_000)

	tx := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	peer.sendQ <- outboundMessage{
		msg:        p2p.TxBatchMessage{Txs: []types.Transaction{tx}},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.TxBatchMessage{Txs: []types.Transaction{tx}}),
	}
	priorityItems := []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: [32]byte{9}}}
	peer.relayPriorityQ <- outboundMessage{
		msg:        p2p.InvMessage{Items: priorityItems},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.InvMessage{Items: priorityItems}),
		invItems:   priorityItems,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	msgCh := make(chan p2p.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, err := remoteWire.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		msgCh <- msg
	}()

	select {
	case err := <-errCh:
		close(svc.stopCh)
		<-done
		t.Fatalf("read message: %v", err)
	case msg := <-msgCh:
		inv, ok := msg.(p2p.InvMessage)
		if !ok {
			close(svc.stopCh)
			<-done
			t.Fatalf("first message type = %T, want InvMessage", msg)
		}
		if len(inv.Items) != 1 || inv.Items[0].Type != p2p.InvTypeBlock {
			close(svc.stopCh)
			<-done
			t.Fatalf("unexpected first inv payload: %#v", inv.Items)
		}
	case <-time.After(500 * time.Millisecond):
		close(svc.stopCh)
		<-done
		t.Fatal("timed out waiting for prioritized relay message")
	}

	close(svc.stopCh)
	<-done
}

func TestPeerWriteLoopPrefersControlThenPriorityThenSend(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()

	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		wire:           p2p.NewConn(local, p2p.MagicForProfile(types.Regtest), 1_000_000),
		controlQ:       make(chan outboundMessage, 2),
		relayPriorityQ: make(chan outboundMessage, 2),
		sendQ:          make(chan outboundMessage, 2),
		closed:         make(chan struct{}),
		queuedInv:      make(map[p2p.InvVector]int),
		queuedTx:       make(map[[32]byte]int),
		knownTx:        make(map[[32]byte]struct{}),
		pendingThin:    make(map[[32]byte]*pendingThinBlock),
	}
	remoteWire := p2p.NewConn(remote, p2p.MagicForProfile(types.Regtest), 1_000_000)

	tx := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	peer.sendQ <- outboundMessage{
		msg:        p2p.TxBatchMessage{Txs: []types.Transaction{tx}},
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneSend,
		class:      classifyRelayMessage(p2p.TxBatchMessage{Txs: []types.Transaction{tx}}),
	}
	priorityItems := []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: [32]byte{9}}}
	peer.relayPriorityQ <- outboundMessage{
		msg:        p2p.InvMessage{Items: priorityItems},
		enqueuedAt: time.Now(),
		lane:       relayQueueLanePriority,
		class:      classifyRelayMessage(p2p.InvMessage{Items: priorityItems}),
		invItems:   priorityItems,
	}
	peer.controlQ <- outboundMessage{
		msg:        p2p.PingMessage{Nonce: 7},
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneControl,
		class:      classifyRelayMessage(p2p.PingMessage{Nonce: 7}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	msg1, err := remoteWire.ReadMessage()
	if err != nil {
		close(svc.stopCh)
		<-done
		t.Fatalf("read first message: %v", err)
	}
	if _, ok := msg1.(p2p.PingMessage); !ok {
		close(svc.stopCh)
		<-done
		t.Fatalf("first message type = %T, want PingMessage", msg1)
	}
	msg2, err := remoteWire.ReadMessage()
	if err != nil {
		close(svc.stopCh)
		<-done
		t.Fatalf("read second message: %v", err)
	}
	if _, ok := msg2.(p2p.InvMessage); !ok {
		close(svc.stopCh)
		<-done
		t.Fatalf("second message type = %T, want InvMessage", msg2)
	}
	msg3, err := remoteWire.ReadMessage()
	if err != nil {
		close(svc.stopCh)
		<-done
		t.Fatalf("read third message: %v", err)
	}
	if _, ok := msg3.(p2p.TxBatchMessage); !ok {
		close(svc.stopCh)
		<-done
		t.Fatalf("third message type = %T, want TxBatchMessage", msg3)
	}

	close(svc.stopCh)
	<-done
}

func TestPeerCloseDrainsBufferedRelayState(t *testing.T) {
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}

	inv := peer.filterQueuedInv([]p2p.InvVector{{Type: p2p.InvTypeTx, Hash: [32]byte{1}}})
	peer.sendQ <- outboundMessage{
		msg:      p2p.InvMessage{Items: inv},
		invItems: inv,
	}
	tx := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	filteredTxs := peer.filterQueuedTxs([]types.Transaction{tx})
	peer.sendQ <- outboundMessage{
		msg: p2p.TxBatchMessage{Txs: filteredTxs},
	}
	peer.pendingTxOrder = [][32]byte{consensus.TxID(&tx)}
	peer.pendingTxByID = map[[32]byte]*types.Transaction{consensus.TxID(&tx): &tx}
	peer.pendingRecon = [][32]byte{{2}}
	peer.txFlushArmed = true
	peer.reconFlushArmed = true
	peer.knownTx[[32]byte{3}] = struct{}{}
	peer.knownTxOrder = append(peer.knownTxOrder, [32]byte{3})
	peer.pendingThin[[32]byte{4}] = &pendingThinBlock{}

	peer.close()

	if len(peer.sendQ) != 0 {
		t.Fatalf("buffered send queue len = %d, want 0", len(peer.sendQ))
	}
	if len(peer.queuedInv) != 0 {
		t.Fatalf("queued inv len = %d, want 0", len(peer.queuedInv))
	}
	if len(peer.queuedTx) != 0 {
		t.Fatalf("queued tx len = %d, want 0", len(peer.queuedTx))
	}
	if len(peer.pendingTxOrder) != 0 || len(peer.pendingTxByID) != 0 || len(peer.pendingRecon) != 0 {
		t.Fatalf("expected pending relay buffers cleared")
	}
	if peer.txFlushArmed || peer.reconFlushArmed {
		t.Fatal("expected flush flags cleared")
	}
	if len(peer.knownTx) != 0 || len(peer.knownTxOrder) != 0 {
		t.Fatal("expected known tx cache cleared")
	}
	if len(peer.pendingThin) != 0 {
		t.Fatal("expected pending thin state cleared")
	}
}

func TestInboundPeerTxRateLimitDefaultsAllowLargeRelayBurst(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")

	allowed, err := peer.allowInboundTxs(int(inboundPeerTxBurst))
	if err != nil {
		t.Fatalf("allow default burst: %v", err)
	}
	if !allowed {
		t.Fatalf("default inbound tx burst %.0f was not allowed", inboundPeerTxBurst)
	}
	allowed, err = peer.allowInboundTxs(1)
	if err != nil {
		t.Fatalf("single tx after burst should be soft-limited, not banned: %v", err)
	}
	if allowed {
		t.Fatal("token bucket should be empty immediately after consuming the full burst")
	}
}

func TestOnPeerMessageRateLimitsInboundTxFlood(t *testing.T) {
	withInboundPeerTxRateLimitForTest(t, 0, 2, 2)
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	peer := newPeerConnForTests("127.0.0.1:18444")
	batch := p2p.TxBatchMessage{Txs: []types.Transaction{{}, {}, {}}}

	if err := svc.onPeerMessage(peer, batch); err != nil {
		t.Fatalf("first over-limit batch should be dropped, got %v", err)
	}
	if len(peer.knownTx) != 0 {
		t.Fatal("dropped over-limit batch should not enter known-tx tracking")
	}

	if err := svc.onPeerMessage(peer, batch); !errors.Is(err, errPeerInboundTxRateLimit) {
		t.Fatalf("second over-limit batch error = %v, want %v", err, errPeerInboundTxRateLimit)
	}
}

type deadlineSpyConn struct {
	mu                  sync.Mutex
	sawNonZeroWriteTime bool
}

type scriptedAcceptResult struct {
	conn net.Conn
	err  error
}

type scriptedListener struct {
	mu           sync.Mutex
	results      []scriptedAcceptResult
	calls        int
	secondAccept chan struct{}
}

func TestPeerInfoUsesObservedHeight(t *testing.T) {
	svc := &Service{
		peers:      make(map[string]*peerConn),
		knownPeers: make(map[string]storage.KnownPeerRecord),
	}
	peer := &peerConn{
		addr:           "127.0.0.1:18444",
		outbound:       true,
		connectedAt:    time.Now().Add(-2 * time.Minute),
		version:        p2p.VersionMessage{Height: 7, UserAgent: "bpu/go"},
		controlQ:       make(chan outboundMessage, 1),
		relayPriorityQ: make(chan outboundMessage, 1),
		sendQ:          make(chan outboundMessage, 1),
		localRelayTxs:  make(map[[32]byte]localRelayFallbackState),
	}
	peer.noteProgress(time.Unix(100, 0))
	peer.controlQ <- outboundMessage{}
	peer.relayPriorityQ <- outboundMessage{}
	peer.sendQ <- outboundMessage{}
	peer.localRelayTxs[[32]byte{1}] = localRelayFallbackState{announcedAt: time.Unix(101, 0)}
	peer.noteUsefulTxs(1, time.Now())
	peer.telemetry.noteTxRequestReceived(2)
	peer.telemetry.noteTxNotFoundReceived(1)
	peer.telemetry.noteKnownTxClears(3)
	svc.peers[peer.addr] = peer

	info := svc.PeerInfo()[0]
	if got := info.Height; got != 7 {
		t.Fatalf("peer height = %d, want handshake height 7", got)
	}
	if info.RelayQueueDepth != 3 || info.ControlQueueDepth != 1 || info.PriorityQueueDepth != 1 || info.SendQueueDepth != 1 {
		t.Fatalf("unexpected relay queue snapshot: %+v", info)
	}
	if info.PendingLocalRelayTxs != 1 || info.TxReqRecvItems != 2 || info.TxNotFoundReceived != 1 || info.KnownTxClears != 3 {
		t.Fatalf("unexpected peer relay details: %+v", info)
	}
	if info.UsefulnessClass == "" || info.UsefulnessScore == 0 {
		t.Fatalf("expected usefulness fields in peer info, got %+v", info)
	}

	peer.noteHeight(11)
	if got := svc.PeerInfo()[0].Height; got != 11 {
		t.Fatalf("peer height = %d, want observed height 11", got)
	}
}

func TestPeerInfoReportsProtectionClass(t *testing.T) {
	svc := &Service{
		peers: make(map[string]*peerConn),
		knownPeers: map[string]storage.KnownPeerRecord{
			"127.0.0.1:18444": {Manual: true},
		},
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.outbound = true
	peer.connectedAt = time.Now().Add(-2 * time.Minute)
	peer.noteUsefulBlocks(1, time.Now())
	svc.peers[peer.addr] = peer

	info := svc.PeerInfo()[0]
	if !info.Manual || !info.Protected {
		t.Fatalf("expected manual peer to be protected, got %+v", info)
	}
	if info.ProtectedClass != "manual" || info.UsefulnessClass != "manual" {
		t.Fatalf("unexpected protection classification: %+v", info)
	}
}

func TestReserveInboundHandshakeRejectsWhenEstablishedSlotsFull(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	now := time.Now()
	goodA := newPeerConnForTests("127.0.0.1:18444")
	goodA.connectedAt = now.Add(-2 * time.Minute)
	goodA.noteHeight(25)
	goodA.noteUsefulBlocks(1, now)

	goodB := newPeerConnForTests("127.0.0.1:18445")
	goodB.connectedAt = now.Add(-2 * time.Minute)
	goodB.noteHeight(24)
	goodB.noteUsefulHeaders(1, now)

	low := newPeerConnForTests("127.0.0.1:18446")
	low.connectedAt = now.Add(-10 * time.Minute)

	svc := &Service{
		cfg:    ServiceConfig{MaxInboundPeers: 3},
		logger: logger,
		peers: map[string]*peerConn{
			goodA.addr: goodA,
			goodB.addr: goodB,
			low.addr:   low,
		},
		stopCh: make(chan struct{}),
	}

	if ok := svc.peerManager().reserveInboundHandshake("127.0.0.1:19000"); ok {
		t.Fatal("expected inbound handshake reservation to reject when established slots are full")
	}
	if _, ok := svc.peers[low.addr]; !ok {
		t.Fatalf("expected established low-value peer %s to remain until a candidate completes handshake", low.addr)
	}
	logged := buf.String()
	if !strings.Contains(logged, "inbound slots or handshakes are saturated") {
		t.Fatalf("expected inbound reservation rejection log, got %s", logged)
	}
	if !strings.Contains(logged, "127.0.0.1:19000") {
		t.Fatalf("expected candidate in inbound reservation rejection log, got %s", logged)
	}
}

func TestReserveInboundHandshakeCountsPendingHandshakes(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	now := time.Now()
	peerA := newPeerConnForTests("127.0.0.1:18444")
	peerA.connectedAt = now.Add(-2 * time.Minute)
	peerA.noteHeight(25)
	peerA.noteUsefulBlocks(1, now)

	peerB := newPeerConnForTests("127.0.0.1:18445")
	peerB.connectedAt = now.Add(-2 * time.Minute)
	peerB.noteHeight(24)
	peerB.noteUsefulHeaders(1, now)

	svc := &Service{
		cfg:    ServiceConfig{MaxInboundPeers: 2},
		logger: logger,
		peers: map[string]*peerConn{
			peerA.addr: peerA,
			peerB.addr: peerB,
		},
		stopCh: make(chan struct{}),
	}

	if ok := svc.peerManager().reserveInboundHandshake("127.0.0.1:19001"); ok {
		t.Fatal("expected inbound handshake reservation to reject candidate when slots are full")
	}
	if !strings.Contains(buf.String(), "inbound slots or handshakes are saturated") {
		t.Fatalf("expected inbound reservation rejection log, got %s", buf.String())
	}

	svc.cfg.MaxInboundPeers = 3
	if ok := svc.peerManager().reserveInboundHandshake("127.0.0.1:19002"); !ok {
		t.Fatal("expected one pending inbound handshake to fit")
	}
	if ok := svc.peerManager().reserveInboundHandshake("127.0.0.1:19003"); ok {
		t.Fatal("expected pending inbound handshake to count against capacity")
	}
	svc.peerManager().releaseInboundHandshake()
}

func TestReserveOutboundTargetReplacesLowValuePeerAndLogs(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	now := time.Now().UTC()
	manual := newPeerConnForTests("127.0.0.1:18444")
	manual.outbound = true
	manual.connectedAt = now.Add(-3 * time.Minute)
	manual.noteHeight(50)
	manual.noteUsefulBlocks(1, now)

	low := newPeerConnForTests("127.0.0.1:18445")
	low.outbound = true
	low.connectedAt = now.Add(-10 * time.Minute)

	svc := &Service{
		cfg:    ServiceConfig{MaxOutboundPeers: 2},
		logger: logger,
		peers: map[string]*peerConn{
			manual.addr: manual,
			low.addr:    low,
		},
		outboundPeers: map[string]struct{}{
			manual.addr: {},
			low.addr:    {},
		},
		knownPeers: map[string]storage.KnownPeerRecord{
			manual.addr:       {Manual: true, LastSeen: now, LastSuccess: now},
			low.addr:          {LastSeen: now.Add(-6 * time.Hour), FailureCount: 2},
			"127.0.0.1:18446": {LastSeen: now, LastSuccess: now},
		},
		stopCh: make(chan struct{}),
	}

	reservation, reserved, err := svc.peerManager().reserveOutboundTarget("127.0.0.1:18446", false)
	if err != nil {
		t.Fatalf("reserve outbound target: %v", err)
	}
	if !reserved {
		t.Fatal("expected outbound candidate to reserve a replacement slot")
	}
	if reservation.evictedAddr != low.addr {
		t.Fatalf("evicted addr = %s, want %s", reservation.evictedAddr, low.addr)
	}
	if _, ok := svc.outboundPeers[low.addr]; ok {
		t.Fatalf("expected low-value outbound target %s to be removed", low.addr)
	}
	if _, ok := svc.outboundPeers["127.0.0.1:18446"]; !ok {
		t.Fatal("expected replacement outbound target to be installed")
	}
	logged := buf.String()
	if !strings.Contains(logged, "evicting lower-value outbound target to make room for candidate") {
		t.Fatalf("expected outbound replacement log, got %s", logged)
	}
	if !strings.Contains(logged, low.addr) || !strings.Contains(logged, "127.0.0.1:18446") {
		t.Fatalf("expected candidate and victim in outbound replacement log, got %s", logged)
	}
}

func TestOutboundRefillCandidatesRebalanceLowValueLearnedPeer(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:          types.Regtest,
		DBPath:           t.TempDir(),
		MaxOutboundPeers: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	now := time.Now().UTC()
	manual := newPeerConnForTests("127.0.0.1:18444")
	manual.outbound = true
	manual.connectedAt = now.Add(-3 * time.Minute)
	manual.noteHeight(50)
	manual.noteUsefulBlocks(1, now)

	low := newPeerConnForTests("127.0.0.1:18445")
	low.outbound = true
	low.connectedAt = now.Add(-10 * time.Minute)

	svc.peerMu.Lock()
	svc.peers = map[string]*peerConn{
		manual.addr: manual,
		low.addr:    low,
	}
	svc.outboundPeers = map[string]struct{}{
		manual.addr: {},
		low.addr:    {},
	}
	svc.knownPeers = map[string]storage.KnownPeerRecord{
		manual.addr:     {Manual: true, LastSeen: now, LastSuccess: now},
		low.addr:        {LastSeen: now.Add(-6 * time.Hour), FailureCount: 2},
		"8.8.8.8:18446": {LastSeen: now, LastSuccess: now},
	}
	svc.peerMu.Unlock()

	candidates := svc.peerManager().outboundRefillCandidates(1)
	if len(candidates) != 1 || candidates[0] != "8.8.8.8:18446" {
		t.Fatalf("outbound refill candidates = %v, want [8.8.8.8:18446]", candidates)
	}
	reservation, reserved, err := svc.peerManager().reserveOutboundTarget(candidates[0], false)
	if err != nil {
		t.Fatalf("reserve outbound target: %v", err)
	}
	if !reserved {
		t.Fatal("expected learned candidate to reserve a replacement slot")
	}
	if reservation.evictedAddr != low.addr {
		t.Fatalf("evicted addr = %s, want %s", reservation.evictedAddr, low.addr)
	}

	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if _, ok := svc.outboundPeers[low.addr]; ok {
		t.Fatalf("expected rebalance to remove low-value outbound target %s", low.addr)
	}
	if _, ok := svc.outboundPeers["8.8.8.8:18446"]; !ok {
		t.Fatal("expected rebalance to install the better learned outbound target")
	}
}
