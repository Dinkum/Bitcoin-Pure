package node

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"time"

	"bitcoin-pure/internal/p2p"
)

// peerManager owns peer lifecycle: accept, connect, reconnect, bookkeeping,
// and peer-local reader/writer goroutine startup.
type peerManager struct {
	svc *Service
}

func (m *peerManager) acceptLoop() {
	for {
		conn, err := m.svc.listener.Accept()
		if err != nil {
			select {
			case <-m.svc.stopCh:
				return
			default:
				m.svc.logger.Error("accept loop failed", slog.Any("error", err))
				return
			}
		}
		if !m.canAcceptInboundPeer() {
			m.svc.logger.Warn("rejecting inbound peer: inbound limit reached", slog.String("addr", conn.RemoteAddr().String()))
			_ = conn.Close()
			continue
		}
		m.svc.wg.Add(1)
		go func() {
			defer m.svc.wg.Done()
			m.handlePeer(conn, false, "")
		}()
	}
}

func (m *peerManager) canAcceptInboundPeer() bool {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	if m.svc.cfg.MaxInboundPeers <= 0 {
		return true
	}
	inbound := 0
	for _, peer := range m.svc.peers {
		if !peer.outbound {
			inbound++
		}
	}
	return inbound < m.svc.cfg.MaxInboundPeers
}

func (m *peerManager) ConnectPeer(addr string) error {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return nil
	}
	m.svc.peerMu.Lock()
	if _, ok := m.svc.outboundPeers[addr]; ok {
		m.svc.peerMu.Unlock()
		return nil
	}
	if m.svc.cfg.MaxOutboundPeers > 0 && len(m.svc.outboundPeers) >= m.svc.cfg.MaxOutboundPeers {
		m.svc.peerMu.Unlock()
		return fmt.Errorf("outbound peer limit reached")
	}
	m.svc.outboundPeers[addr] = struct{}{}
	m.svc.knownPeers[addr] = time.Now()
	m.svc.peerMu.Unlock()

	m.svc.wg.Add(1)
	go func() {
		defer m.svc.wg.Done()
		m.maintainOutboundPeer(addr)
	}()
	return nil
}

func (m *peerManager) outboundPeerCount() int {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	return len(m.svc.outboundPeers)
}

func (m *peerManager) maintainOutboundPeer(addr string) {
	backoff := time.Second
	dialer := &net.Dialer{Timeout: m.svc.cfg.HandshakeTimeout}
	for {
		if !m.shouldMaintainOutboundPeer(addr) {
			return
		}
		if m.hasOutboundPeer(addr) {
			if !m.svc.sleepUntilStop(time.Second) {
				return
			}
			continue
		}

		m.svc.logger.Info("connecting peer", slog.String("addr", addr))
		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			m.svc.logger.Warn("peer dial failed", slog.String("addr", addr), slog.Any("error", err), slog.Duration("retry_in", backoff))
			if !m.svc.sleepUntilStop(jitterDuration(backoff)) {
				return
			}
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			continue
		}
		backoff = time.Second
		m.handlePeer(conn, true, addr)
		if !m.svc.sleepUntilStop(time.Second) {
			return
		}
	}
}

func (m *peerManager) handlePeer(conn net.Conn, outbound bool, targetAddr string) {
	remoteAddr := conn.RemoteAddr().String()
	addr := remoteAddr
	if outbound && targetAddr != "" {
		addr = targetAddr
	}
	wire := p2p.NewConn(conn, p2p.MagicForProfile(m.svc.cfg.Profile), m.svc.cfg.MaxMessageBytes)
	remoteVersion, err := p2p.Handshake(wire, m.svc.localVersion(), m.svc.cfg.HandshakeTimeout)
	if err != nil {
		m.svc.logger.Warn("peer handshake failed", slog.String("addr", addr), slog.String("remote_addr", remoteAddr), slog.Any("error", err))
		_ = conn.Close()
		return
	}
	peer := &peerConn{
		addr:           addr,
		targetAddr:     targetAddr,
		outbound:       outbound,
		wire:           wire,
		version:        remoteVersion,
		controlQ:       make(chan outboundMessage, 64),
		relayPriorityQ: make(chan outboundMessage, 64),
		sendQ:          make(chan outboundMessage, 512),
		closed:         make(chan struct{}),
		queuedInv:      make(map[p2p.InvVector]int),
		queuedTx:       make(map[[32]byte]int),
		knownTx:        make(map[[32]byte]struct{}),
		pendingThin:    make(map[[32]byte]*pendingThinBlock),
	}
	peer.noteProgress(time.Now())
	peer.noteHeight(remoteVersion.Height)
	if outbound {
		m.recordKnownPeerSuccess(addr, time.Now())
	}
	m.svc.peerMu.Lock()
	m.svc.peers[addr] = peer
	m.svc.peerMu.Unlock()
	m.svc.logger.Info("peer connected", slog.String("addr", addr), slog.Int("peer_count", m.svc.peerCount()))
	defer func() {
		peer.close()
		m.svc.releasePeerBlockRequests(addr)
		m.svc.releasePeerTxRequests(addr)
		m.svc.peerMu.Lock()
		delete(m.svc.peers, addr)
		m.svc.peerMu.Unlock()
		_ = wire.Close()
		m.svc.logger.Info("peer disconnected",
			slog.String("addr", addr),
			slog.String("remote_addr", remoteAddr),
			slog.Int("peer_count", m.svc.peerCount()),
			slog.Uint64("tip_height", m.svc.blockHeight()),
			slog.Uint64("header_height", m.svc.headerHeight()),
			slog.Uint64("peer_best_height", peer.snapshotHeight()),
			slog.Time("peer_last_progress", unixTimeOrZero(peer.snapshotProgressUnix())),
		)
	}()

	m.svc.wg.Add(1)
	go func() {
		defer m.svc.wg.Done()
		m.peerWriteLoop(peer)
	}()

	m.svc.requestSync(peer)
	m.svc.wg.Add(1)
	go func() {
		defer m.svc.wg.Done()
		m.peerPingLoop(peer)
	}()

	for {
		if m.svc.cfg.StallTimeout > 0 {
			_ = wire.SetReadDeadline(time.Now().Add(m.svc.cfg.StallTimeout))
		}
		msg, err := wire.ReadMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				m.svc.logger.Warn("peer read loop failed", slog.String("addr", addr), slog.String("remote_addr", remoteAddr), slog.Any("error", err))
			}
			return
		}
		peer.noteProgress(time.Now())
		if err := m.svc.onPeerMessage(peer, msg); err != nil {
			m.svc.logger.Warn("peer message handling failed",
				slog.String("addr", addr),
				slog.String("remote_addr", remoteAddr),
				slog.String("type", fmt.Sprintf("%T", msg)),
				slog.Any("error", err),
				slog.Uint64("tip_height", m.svc.blockHeight()),
				slog.Uint64("header_height", m.svc.headerHeight()),
				slog.Uint64("peer_best_height", peer.snapshotHeight()),
				slog.Time("peer_last_progress", unixTimeOrZero(peer.snapshotProgressUnix())),
			)
			return
		}
	}
}

func (m *peerManager) peerPingLoop(peer *peerConn) {
	interval := m.svc.cfg.StallTimeout / 2
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.svc.stopCh:
			return
		case <-ticker.C:
			if err := peer.send(p2p.PingMessage{Nonce: randomNonce()}); err != nil {
				return
			}
		}
	}
}

func (m *peerManager) peerWriteLoop(peer *peerConn) {
	for {
		if peer.controlQ != nil {
			select {
			case <-m.svc.stopCh:
				return
			case <-peer.closed:
				return
			case envelope := <-peer.controlQ:
				if !m.svc.writePeerEnvelope(peer, envelope) {
					return
				}
				continue
			default:
			}
		}
		if peer.relayPriorityQ != nil {
			select {
			case <-m.svc.stopCh:
				return
			case <-peer.closed:
				return
			case envelope := <-peer.relayPriorityQ:
				if !m.svc.writePeerEnvelope(peer, envelope) {
					return
				}
				continue
			default:
			}
		}

		controlQ := peer.controlQ
		relayPriorityQ := peer.relayPriorityQ
		sendQ := peer.sendQ
		select {
		case <-m.svc.stopCh:
			return
		case <-peer.closed:
			return
		case envelope := <-controlQ:
			if !m.svc.writePeerEnvelope(peer, envelope) {
				return
			}
		case envelope := <-relayPriorityQ:
			if !m.svc.writePeerEnvelope(peer, envelope) {
				return
			}
		case envelope := <-sendQ:
			if !m.svc.writePeerEnvelope(peer, envelope) {
				return
			}
		}
	}
}

func (m *peerManager) outboundRefillLoop() {
	ticker := time.NewTicker(outboundRefillInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.svc.stopCh:
			return
		case <-ticker.C:
			m.refillOutboundPeers()
		}
	}
}

func (m *peerManager) refillOutboundPeers() {
	if m.svc.cfg.MaxOutboundPeers <= 0 {
		return
	}
	need := m.svc.cfg.MaxOutboundPeers - m.outboundPeerCount()
	if need <= 0 {
		return
	}
	for _, addr := range m.outboundRefillCandidates(need) {
		if err := m.ConnectPeer(addr); err != nil {
			m.svc.logger.Debug("outbound refill skipped candidate",
				slog.String("addr", addr),
				slog.Any("error", err),
			)
		}
	}
}

func (m *peerManager) outboundRefillCandidates(limit int) []string {
	if limit <= 0 {
		return nil
	}
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	type seenPeer struct {
		addr string
		last time.Time
	}
	candidates := make([]seenPeer, 0, len(m.svc.knownPeers))
	for addr, last := range m.svc.knownPeers {
		addr = normalizePeerAddr(addr)
		if addr == "" || addr == normalizePeerAddr(m.svc.cfg.P2PAddr) {
			continue
		}
		if _, ok := m.svc.outboundPeers[addr]; ok {
			continue
		}
		if peer, ok := m.svc.peers[addr]; ok && peer.outbound {
			continue
		}
		candidates = append(candidates, seenPeer{addr: addr, last: last})
	}
	slices.SortFunc(candidates, func(a, b seenPeer) int {
		if a.last.Equal(b.last) {
			return strings.Compare(a.addr, b.addr)
		}
		if a.last.After(b.last) {
			return -1
		}
		return 1
	})
	out := make([]string, 0, limit)
	for _, candidate := range candidates {
		out = append(out, candidate.addr)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (m *peerManager) peerSnapshot() []*peerConn {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	out := make([]*peerConn, 0, len(m.svc.peers))
	for _, peer := range m.svc.peers {
		out = append(out, peer)
	}
	return out
}

func (m *peerManager) peerSnapshotExcluding(skip *peerConn) []*peerConn {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	out := make([]*peerConn, 0, len(m.svc.peers))
	for _, peer := range m.svc.peers {
		if skip != nil && peer == skip {
			continue
		}
		out = append(out, peer)
	}
	return out
}

func (m *peerManager) PeerInfo() []PeerInfo {
	now := time.Now()
	localHeaderHeight := m.svc.headerHeight()
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	preferred := m.svc.syncManager().preferredDownloadPeerAddrsLocked(now, localHeaderHeight)
	out := make([]PeerInfo, 0, len(m.svc.peers))
	for _, peer := range m.svc.peers {
		stats := peer.syncSnapshot()
		out = append(out, PeerInfo{
			Addr:               peer.addr,
			Outbound:           peer.outbound,
			Height:             peer.snapshotHeight(),
			UserAgent:          peer.version.UserAgent,
			LastProgress:       peer.snapshotProgressUnix(),
			LastUseful:         stats.lastUsefulUnix(),
			PreferredDownload:  preferred[peer.addr],
			DownloadScore:      stats.downloadScore(now, localHeaderHeight),
			HeaderStalls:       stats.HeaderStalls,
			BlockStalls:        stats.BlockStalls,
			TxStalls:           stats.TxStalls,
			DownloadCooldownMS: stats.cooldownRemainingMS(now),
		})
	}
	slices.SortFunc(out, func(a, b PeerInfo) int {
		return strings.Compare(a.Addr, b.Addr)
	})
	return out
}

func (m *peerManager) peerByAddr(addr string) *peerConn {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return nil
	}
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	return m.svc.peers[addr]
}

func (m *peerManager) knownPeerAddrs() []string {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	set := make(map[string]struct{})
	if m.svc.cfg.P2PAddr != "" {
		set[m.svc.cfg.P2PAddr] = struct{}{}
	}
	for _, addr := range m.svc.cfg.Peers {
		addr = normalizePeerAddr(addr)
		if addr != "" {
			set[addr] = struct{}{}
		}
	}
	for addr := range m.svc.knownPeers {
		if addr != "" {
			set[addr] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for addr := range set {
		out = append(out, addr)
	}
	slices.Sort(out)
	return out
}

func (m *peerManager) hasPeer(addr string) bool {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	_, ok := m.svc.peers[addr]
	return ok
}

func (m *peerManager) hasOutboundPeer(addr string) bool {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	peer, ok := m.svc.peers[addr]
	return ok && peer.outbound
}

func (m *peerManager) loadPersistedKnownPeers(peers map[string]time.Time) {
	if len(peers) == 0 {
		return
	}
	m.svc.peerMu.Lock()
	defer m.svc.peerMu.Unlock()
	for addr, last := range peers {
		addr = normalizePeerAddr(addr)
		if addr == "" || addr == m.svc.cfg.P2PAddr {
			continue
		}
		m.svc.knownPeers[addr] = last
		m.svc.vettedPeers[addr] = last
	}
}

func (m *peerManager) recordKnownPeerSuccess(addr string, at time.Time) {
	addr = normalizePeerAddr(addr)
	if addr == "" || addr == normalizePeerAddr(m.svc.cfg.P2PAddr) {
		return
	}
	m.svc.peerMu.Lock()
	if m.svc.knownPeers == nil {
		m.svc.knownPeers = make(map[string]time.Time)
	}
	if m.svc.vettedPeers == nil {
		m.svc.vettedPeers = make(map[string]time.Time)
	}
	at = at.UTC()
	m.svc.knownPeers[addr] = at
	m.svc.vettedPeers[addr] = at
	trimKnownPeerMapLocked(m.svc.knownPeers)
	trimKnownPeerMapLocked(m.svc.vettedPeers)
	snapshot := cloneKnownPeerMap(m.svc.vettedPeers)
	m.svc.peerMu.Unlock()
	if err := m.svc.chainState.Store().WriteKnownPeers(snapshot); err != nil {
		m.svc.logger.Warn("failed to persist vetted peer address book",
			slog.String("addr", addr),
			slog.Any("error", err),
		)
	}
}

func (m *peerManager) shouldMaintainOutboundPeer(addr string) bool {
	select {
	case <-m.svc.stopCh:
		return false
	default:
	}
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	_, ok := m.svc.outboundPeers[addr]
	return ok
}

func (m *peerManager) rememberKnownPeers(addrs []string) {
	if len(addrs) == 0 {
		return
	}
	m.svc.peerMu.Lock()
	defer m.svc.peerMu.Unlock()
	if m.svc.knownPeers == nil {
		m.svc.knownPeers = make(map[string]time.Time)
	}
	for _, addr := range addrs {
		addr = normalizePeerAddr(addr)
		if addr == "" || addr == m.svc.cfg.P2PAddr {
			continue
		}
		m.svc.knownPeers[addr] = time.Now()
	}
	trimKnownPeerMapLocked(m.svc.knownPeers)
}

func (m *peerManager) restartKnownPeers() {
	for _, addr := range m.knownPeerAddrs() {
		addr = normalizePeerAddr(addr)
		if addr == "" || addr == normalizePeerAddr(m.svc.cfg.P2PAddr) {
			continue
		}
		m.restartOutboundPeer(addr)
	}
}

func (m *peerManager) restartOutboundPeer(addr string) {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return
	}
	m.svc.peerMu.Lock()
	delete(m.svc.outboundPeers, addr)
	m.svc.peerMu.Unlock()
	if err := m.ConnectPeer(addr); err != nil {
		m.svc.logger.Warn("failed to restart outbound peer", slog.String("addr", addr), slog.Any("error", err))
	}
}

func trimKnownPeerMapLocked(peers map[string]time.Time) {
	if len(peers) <= maxKnownPeerAddrs {
		return
	}
	type seenPeer struct {
		addr string
		last time.Time
	}
	known := make([]seenPeer, 0, len(peers))
	for addr, last := range peers {
		known = append(known, seenPeer{addr: addr, last: last})
	}
	slices.SortFunc(known, func(a, b seenPeer) int {
		if a.last.Equal(b.last) {
			return strings.Compare(a.addr, b.addr)
		}
		if a.last.Before(b.last) {
			return -1
		}
		return 1
	})
	for len(known) > maxKnownPeerAddrs {
		delete(peers, known[0].addr)
		known = known[1:]
	}
}

func cloneKnownPeerMap(peers map[string]time.Time) map[string]time.Time {
	if len(peers) == 0 {
		return nil
	}
	out := make(map[string]time.Time, len(peers))
	for addr, last := range peers {
		out[addr] = last
	}
	return out
}
