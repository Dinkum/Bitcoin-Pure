package node

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"strings"
	"time"

	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
)

// peerManager owns peer lifecycle: accept, connect, reconnect, bookkeeping,
// and peer-local reader/writer goroutine startup.
type peerManager struct {
	svc *Service
}

const (
	addrSelectionCooldown   = 2 * time.Minute
	autoPeerFailureCeiling  = 3
	controlQueueBurstLimit  = 8
	priorityQueueBurstLimit = 4
	sendQueueBurstLimit     = 1
	inboundProtectionWindow = 45 * time.Second
	outboundReplacementGap  = 250.0
)

type outboundAddrCandidate struct {
	addr     string
	record   storage.KnownPeerRecord
	netgroup string
	score    float64
}

type peerProtectionClass string

const (
	peerProtectionNone              peerProtectionClass = ""
	peerProtectionManual            peerProtectionClass = "manual"
	peerProtectionPreferredDownload peerProtectionClass = "preferred_download"
	peerProtectionRecentlyUseful    peerProtectionClass = "recently_useful"
)

type peerUsefulnessClass string

const (
	peerUsefulnessLowValue          peerUsefulnessClass = "low_value"
	peerUsefulnessCatchUp           peerUsefulnessClass = "catch_up"
	peerUsefulnessFullRelay         peerUsefulnessClass = "full_relay"
	peerUsefulnessPreferredDownload peerUsefulnessClass = "preferred_download"
	peerUsefulnessManual            peerUsefulnessClass = "manual"
	peerUsefulnessInactiveTarget    peerUsefulnessClass = "inactive_target"
)

type peerRetentionState struct {
	Score             float64
	Protected         bool
	Manual            bool
	PreferredDownload bool
	ProtectionClass   peerProtectionClass
	UsefulnessClass   peerUsefulnessClass
	LastUsefulUnix    int64
	LastRelayActivity int64
	HeaderStalls      int
	BlockStalls       int
	TxStalls          int
	PendingLocalRelay int
	SessionAgeSeconds int64
}

type outboundReservation struct {
	snapshot     map[string]storage.KnownPeerRecord
	evictedPeer  *peerConn
	evictedAddr  string
	candidate    peerRetentionState
	evictedState peerRetentionState
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
		if !m.ensureInboundCapacity(conn.RemoteAddr().String()) {
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

func (m *peerManager) ensureInboundCapacity(candidateAddr string) bool {
	m.svc.peerMu.Lock()
	if m.svc.cfg.MaxInboundPeers <= 0 {
		m.svc.peerMu.Unlock()
		return true
	}
	inbound := 0
	for _, peer := range m.svc.peers {
		if !peer.outbound {
			inbound++
		}
	}
	if inbound < m.svc.cfg.MaxInboundPeers {
		m.svc.peerMu.Unlock()
		return true
	}
	now := time.Now()
	localHeaderHeight := m.svc.headerHeight()
	preferred := m.svc.syncManager().preferredDownloadPeerAddrsLocked(now, localHeaderHeight)
	victim, victimState := m.selectInboundEvictionTargetLocked(now, localHeaderHeight, preferred)
	if victim == nil {
		m.svc.peerMu.Unlock()
		m.svc.logger.Warn("rejecting inbound peer: all inbound slots currently protected",
			slog.String("candidate_addr", candidateAddr),
			slog.Int("max_inbound", m.svc.cfg.MaxInboundPeers),
			slog.Int("inbound_peers", inbound),
		)
		return false
	}
	delete(m.svc.peers, victim.addr)
	m.svc.peerMu.Unlock()
	m.logPeerEvictionDecision("evicting low-value inbound peer to admit candidate", candidateAddr, victim.addr, false, victimState, peerRetentionState{})
	m.disconnectPeer(victim)
	return true
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
	return m.connectPeer(addr, true)
}

func (m *peerManager) connectPeer(addr string, manual bool) error {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return nil
	}
	if m.isSelfPeerAddr(addr) {
		m.svc.logger.Debug("skipping self peer address",
			slog.String("addr", addr),
			slog.String("p2p_addr", m.svc.cfg.P2PAddr),
		)
		return nil
	}
	reservation, reserved, err := m.reserveOutboundTarget(addr, manual)
	if err != nil {
		return err
	}
	if !reserved {
		return nil
	}
	if reservation.evictedPeer != nil {
		m.disconnectPeer(reservation.evictedPeer)
	}
	if err := m.svc.chainState.Store().WriteKnownPeers(reservation.snapshot); err != nil {
		m.svc.logger.Warn("failed to persist peer address book",
			slog.String("addr", addr),
			slog.Any("error", err),
		)
	}

	m.svc.wg.Add(1)
	go func() {
		defer m.svc.wg.Done()
		m.maintainOutboundPeer(addr)
	}()
	return nil
}

func (m *peerManager) reserveOutboundTarget(addr string, manual bool) (outboundReservation, bool, error) {
	now := time.Now().UTC()
	m.svc.peerMu.Lock()
	if _, ok := m.svc.outboundPeers[addr]; ok {
		m.svc.peerMu.Unlock()
		return outboundReservation{}, false, nil
	}
	reservation := outboundReservation{}
	if m.svc.cfg.MaxOutboundPeers > 0 && len(m.svc.outboundPeers) >= m.svc.cfg.MaxOutboundPeers {
		localHeaderHeight := m.svc.headerHeight()
		preferred := m.svc.syncManager().preferredDownloadPeerAddrsLocked(now, localHeaderHeight)
		candidate := m.outboundCandidateStateLocked(addr, manual, now)
		victimAddr, victimPeer, victimState, ok := m.selectOutboundEvictionTargetLocked(addr, candidate, now, localHeaderHeight, preferred)
		if !ok {
			m.svc.peerMu.Unlock()
			log := m.svc.logger.Debug
			if manual {
				log = m.svc.logger.Warn
			}
			log("refusing outbound candidate: all outbound slots are protected or higher value",
				slog.String("candidate_addr", addr),
				slog.Bool("candidate_manual", candidate.Manual),
				slog.String("candidate_usefulness", string(candidate.UsefulnessClass)),
				slog.Float64("candidate_score", candidate.Score),
				slog.Int("outbound_targets", len(m.svc.outboundPeers)),
				slog.Int("max_outbound", m.svc.cfg.MaxOutboundPeers),
			)
			return outboundReservation{}, false, fmt.Errorf("outbound peer limit reached")
		}
		delete(m.svc.outboundPeers, victimAddr)
		if victimPeer != nil {
			delete(m.svc.peers, victimAddr)
		}
		reservation.evictedPeer = victimPeer
		reservation.evictedAddr = victimAddr
		reservation.candidate = candidate
		reservation.evictedState = victimState
	}
	m.svc.outboundPeers[addr] = struct{}{}
	record := m.svc.knownPeers[addr]
	if manual {
		record.Manual = true
	}
	if record.LastSeen.IsZero() {
		record.LastSeen = now
	}
	m.svc.knownPeers[addr] = normalizeKnownPeerRecord(record)
	trimKnownPeerMapLocked(m.svc.knownPeers)
	reservation.snapshot = cloneKnownPeerMap(m.svc.knownPeers)
	m.svc.peerMu.Unlock()
	if reservation.evictedAddr != "" {
		m.logPeerEvictionDecision("evicting lower-value outbound target to make room for candidate", addr, reservation.evictedAddr, true, reservation.evictedState, reservation.candidate)
	}
	return reservation, true, nil
}

func (m *peerManager) outboundPeerCount() int {
	m.svc.peerMu.RLock()
	defer m.svc.peerMu.RUnlock()
	return len(m.svc.outboundPeers)
}

func (m *peerManager) maintainOutboundPeer(addr string) {
	backoff := time.Second
	dialer := &net.Dialer{Timeout: m.svc.cfg.HandshakeTimeout}
	consecutiveFailures := 0
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
			consecutiveFailures++
			m.recordKnownPeerAttempt(addr, time.Now())
			m.recordKnownPeerFailure(addr, time.Now())
			m.svc.logger.Warn("peer dial failed", slog.String("addr", addr), slog.Any("error", err), slog.Duration("retry_in", backoff))
			if consecutiveFailures >= autoPeerFailureCeiling && m.evictUnhealthyAutoPeer(addr) {
				return
			}
			if !m.svc.sleepUntilStop(jitterDuration(backoff)) {
				return
			}
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			continue
		}
		consecutiveFailures = 0
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
	traffic := &peerTrafficMeter{}
	wire := p2p.NewConn(&meteredNetConn{Conn: conn, meter: traffic}, p2p.MagicForProfile(m.svc.cfg.Profile), m.svc.cfg.MaxMessageBytes)
	if outbound {
		m.recordKnownPeerAttempt(addr, time.Now())
	}
	remoteVersion, err := p2p.Handshake(wire, m.svc.localVersion(), m.svc.cfg.HandshakeTimeout)
	if err != nil {
		if outbound {
			m.recordKnownPeerFailure(addr, time.Now())
		}
		m.svc.logger.Warn("peer handshake failed",
			slog.String("addr", addr),
			slog.String("remote_addr", remoteAddr),
			slog.Bool("outbound", outbound),
			slog.String("target_addr", targetAddr),
			slog.Uint64("tip_height", m.svc.blockHeight()),
			slog.Uint64("header_height", m.svc.headerHeight()),
			slog.Any("error", err),
		)
		_ = conn.Close()
		return
	}
	peer := &peerConn{
		svc:            m.svc,
		addr:           addr,
		targetAddr:     targetAddr,
		outbound:       outbound,
		connectedAt:    time.Now(),
		traffic:        traffic,
		wire:           wire,
		version:        remoteVersion,
		controlQ:       make(chan outboundMessage, 64),
		relayPriorityQ: make(chan outboundMessage, 64),
		sendQ:          make(chan outboundMessage, 512),
		closed:         make(chan struct{}),
		queuedInv:      make(map[p2p.InvVector]int),
		queuedTx:       make(map[[32]byte]int),
		knownTx:        make(map[[32]byte]struct{}),
		localRelayTxs:  make(map[[32]byte]localRelayFallbackState),
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
	m.svc.logger.Info("peer connected",
		slog.String("addr", addr),
		slog.String("remote_addr", remoteAddr),
		slog.Bool("outbound", outbound),
		slog.String("target_addr", targetAddr),
		slog.String("user_agent", shortUserAgent(remoteVersion.UserAgent)),
		slog.Uint64("peer_best_height", remoteVersion.Height),
		slog.Uint64("tip_height", m.svc.blockHeight()),
		slog.Uint64("header_height", m.svc.headerHeight()),
		slog.Int("peer_count", m.svc.peerCount()),
		slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
	)
	defer func() {
		peer.close()
		m.svc.releasePeerBlockRequests(addr)
		m.svc.releasePeerTxRequests(addr)
		m.svc.peerMu.Lock()
		delete(m.svc.peers, addr)
		m.svc.peerMu.Unlock()
		_ = wire.Close()
		stats := peer.syncSnapshot()
		m.svc.logger.Info("peer disconnected",
			slog.String("addr", addr),
			slog.String("remote_addr", remoteAddr),
			slog.Bool("outbound", outbound),
			slog.String("user_agent", shortUserAgent(peer.version.UserAgent)),
			slog.Int("peer_count", m.svc.peerCount()),
			slog.Uint64("tip_height", m.svc.blockHeight()),
			slog.Uint64("header_height", m.svc.headerHeight()),
			slog.Uint64("peer_best_height", peer.snapshotHeight()),
			slog.Time("peer_last_progress", unixTimeOrZero(peer.snapshotProgressUnix())),
			slog.Int("header_stalls", stats.HeaderStalls),
			slog.Int("block_stalls", stats.BlockStalls),
			slog.Int("tx_stalls", stats.TxStalls),
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
				m.svc.logger.Warn("peer read loop failed",
					slog.String("addr", addr),
					slog.String("remote_addr", remoteAddr),
					slog.Any("error", err),
					slog.Uint64("tip_height", m.svc.blockHeight()),
					slog.Uint64("header_height", m.svc.headerHeight()),
					slog.Uint64("peer_best_height", peer.snapshotHeight()),
					slog.Time("peer_last_progress", unixTimeOrZero(peer.snapshotProgressUnix())),
				)
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
				slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
				slog.String("inflight_blocks", m.svc.inflightBlockDebugSummary(6)),
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
		// Drain in strict priority order with bounded bursts so tx fanout cannot
		// indefinitely starve control traffic or block-relay announcements.
		if drained, stop := m.drainPeerQueue(peer, peer.controlQ, controlQueueBurstLimit); stop {
			return
		} else if drained > 0 {
			continue
		}
		if drained, stop := m.drainPeerQueue(peer, peer.relayPriorityQ, priorityQueueBurstLimit); stop {
			return
		} else if drained > 0 {
			continue
		}
		if drained, stop := m.drainPeerQueue(peer, peer.sendQ, sendQueueBurstLimit); stop {
			return
		} else if drained > 0 {
			continue
		}
		if !m.waitAndWriteNextPeerEnvelope(peer) {
			return
		}
	}
}

func (m *peerManager) drainPeerQueue(peer *peerConn, q chan outboundMessage, limit int) (int, bool) {
	if q == nil || limit <= 0 {
		return 0, false
	}
	drained := 0
	for i := 0; i < limit; i++ {
		select {
		case <-m.svc.stopCh:
			return drained, true
		case <-peer.closed:
			return drained, true
		case envelope := <-q:
			drained++
			if !m.svc.writePeerEnvelope(peer, envelope) {
				return drained, true
			}
		default:
			return drained, false
		}
	}
	return drained, false
}

func (m *peerManager) waitAndWriteNextPeerEnvelope(peer *peerConn) bool {
	if envelope, ok := m.tryDequeuePeerEnvelope(peer.controlQ); ok {
		return m.svc.writePeerEnvelope(peer, envelope)
	}
	if envelope, ok := m.tryDequeuePeerEnvelope(peer.relayPriorityQ); ok {
		return m.svc.writePeerEnvelope(peer, envelope)
	}
	if envelope, ok := m.tryDequeuePeerEnvelope(peer.sendQ); ok {
		return m.svc.writePeerEnvelope(peer, envelope)
	}
	select {
	case <-m.svc.stopCh:
		return false
	case <-peer.closed:
		return false
	case envelope := <-peer.controlQ:
		return m.svc.writePeerEnvelope(peer, envelope)
	case envelope := <-peer.relayPriorityQ:
		return m.svc.writePeerEnvelope(peer, envelope)
	case envelope := <-peer.sendQ:
		return m.svc.writePeerEnvelope(peer, envelope)
	}
}

func (m *peerManager) tryDequeuePeerEnvelope(q chan outboundMessage) (outboundMessage, bool) {
	if q == nil {
		return outboundMessage{}, false
	}
	select {
	case envelope := <-q:
		return envelope, true
	default:
		return outboundMessage{}, false
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
		m.rebalanceOutboundPeers()
		return
	}
	for _, addr := range m.outboundRefillCandidates(need) {
		if err := m.connectPeer(addr, false); err != nil {
			m.svc.logger.Debug("outbound refill skipped candidate",
				slog.String("addr", addr),
				slog.Any("error", err),
			)
		}
	}
}

func (m *peerManager) rebalanceOutboundPeers() {
	for _, addr := range m.outboundRefillCandidates(1) {
		if err := m.connectPeer(addr, false); err != nil {
			m.svc.logger.Debug("outbound rebalance skipped candidate",
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
	now := time.Now().UTC()
	candidates := make([]outboundAddrCandidate, 0, len(m.svc.knownPeers))
	for addr, record := range m.svc.knownPeers {
		addr = normalizePeerAddr(addr)
		if addr == "" || m.isSelfPeerAddr(addr) {
			continue
		}
		if _, ok := m.svc.outboundPeers[addr]; ok {
			continue
		}
		if peer, ok := m.svc.peers[addr]; ok && peer.outbound {
			continue
		}
		candidates = append(candidates, outboundAddrCandidate{
			addr:     addr,
			record:   record,
			netgroup: peerNetgroup(addr),
			score:    scoreKnownPeer(record, now),
		})
	}
	slices.SortFunc(candidates, func(a, b outboundAddrCandidate) int {
		if a.score == b.score {
			return strings.Compare(a.addr, b.addr)
		}
		if a.score > b.score {
			return -1
		}
		return 1
	})
	out := make([]string, 0, limit)
	usedGroups := make(map[string]struct{}, limit)
	deferred := make([]outboundAddrCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.netgroup != "" {
			if _, ok := usedGroups[candidate.netgroup]; ok {
				deferred = append(deferred, candidate)
				continue
			}
			usedGroups[candidate.netgroup] = struct{}{}
		}
		out = append(out, candidate.addr)
		if len(out) >= limit {
			return out
		}
	}
	for _, candidate := range deferred {
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
		relay := peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount())
		ava := peer.avalancheSnapshot()
		avaSupported := peer.supportsAvalancheOverlay()
		avaWeight := uint64(0)
		if avaSupported && m.svc.avalancheManager().enabled() {
			avaWeight = 1
		}
		retention := m.peerRetentionStateLocked(peer, now, localHeaderHeight, preferred)
		out = append(out, PeerInfo{
			Addr:                  peer.addr,
			Outbound:              peer.outbound,
			Manual:                retention.Manual,
			Height:                peer.snapshotHeight(),
			UserAgent:             peer.version.UserAgent,
			LastProgress:          peer.snapshotProgressUnix(),
			LastUseful:            stats.lastUsefulUnix(),
			Protected:             retention.Protected,
			ProtectedClass:        string(retention.ProtectionClass),
			UsefulnessClass:       string(retention.UsefulnessClass),
			UsefulnessScore:       int(retention.Score),
			PreferredDownload:     preferred[peer.addr],
			DownloadScore:         stats.downloadScore(now, localHeaderHeight),
			HeaderStalls:          stats.HeaderStalls,
			BlockStalls:           stats.BlockStalls,
			TxStalls:              stats.TxStalls,
			DownloadCooldownMS:    stats.cooldownRemainingMS(now),
			RelayQueueDepth:       relay.QueueDepth,
			ControlQueueDepth:     relay.ControlQueueDepth,
			PriorityQueueDepth:    relay.PriorityQueueDepth,
			SendQueueDepth:        relay.SendQueueDepth,
			PendingLocalRelayTxs:  relay.PendingLocalRelayTxs,
			LastRelayActivityUnix: relay.LastRelayActivityUnix,
			TxReqRecvItems:        relay.TxReqRecvItems,
			TxNotFoundReceived:    relay.TxNotFoundReceived,
			KnownTxClears:         relay.KnownTxClears,
			WriterStarvation:      relay.WriterStarvationEvents,
			AvalancheSupported:    avaSupported,
			AvalancheWeight:       avaWeight,
			AvalanchePollsSent:    ava.pollsSent,
			AvalanchePollsRecv:    ava.pollsReceived,
			AvalancheVotesSent:    ava.votesSent,
			AvalancheVotesRecv:    ava.votesReceived,
		})
	}
	slices.SortFunc(out, func(a, b PeerInfo) int {
		return strings.Compare(a.Addr, b.Addr)
	})
	return out
}

func (m *peerManager) selectInboundEvictionTargetLocked(now time.Time, localHeaderHeight uint64, preferred map[string]bool) (*peerConn, peerRetentionState) {
	var victim *peerConn
	var victimState peerRetentionState
	for _, peer := range m.svc.peers {
		if peer.outbound {
			continue
		}
		state := m.peerRetentionStateLocked(peer, now, localHeaderHeight, preferred)
		if state.Protected || state.UsefulnessClass != peerUsefulnessLowValue {
			continue
		}
		if victim == nil || m.peerStateLessUseful(state, victimState, peer.addr, victim.addr) {
			victim = peer
			victimState = state
		}
	}
	return victim, victimState
}

func (m *peerManager) selectOutboundEvictionTargetLocked(candidateAddr string, candidate peerRetentionState, now time.Time, localHeaderHeight uint64, preferred map[string]bool) (string, *peerConn, peerRetentionState, bool) {
	var victimPeer *peerConn
	var victimAddr string
	var victimState peerRetentionState
	for addr := range m.svc.outboundPeers {
		if addr == candidateAddr {
			continue
		}
		record := m.svc.knownPeers[addr]
		if record.Manual {
			continue
		}
		peer, active := m.svc.peers[addr]
		if active && !peer.outbound {
			continue
		}
		var state peerRetentionState
		if active {
			state = m.peerRetentionStateLocked(peer, now, localHeaderHeight, preferred)
			if state.Protected {
				continue
			}
			if !candidate.Manual && state.UsefulnessClass != peerUsefulnessLowValue {
				continue
			}
		} else {
			state = m.inactiveOutboundTargetStateLocked(record, now)
			if state.Protected {
				continue
			}
		}
		if !candidate.Manual && candidate.Score <= state.Score+outboundReplacementGap {
			continue
		}
		if victimAddr == "" || m.peerStateLessUseful(state, victimState, addr, victimAddr) {
			victimAddr = addr
			victimPeer = peer
			victimState = state
		}
	}
	return victimAddr, victimPeer, victimState, victimAddr != ""
}

func (m *peerManager) peerRetentionStateLocked(peer *peerConn, now time.Time, localHeaderHeight uint64, preferred map[string]bool) peerRetentionState {
	stats := peer.syncSnapshot()
	relay := peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount())
	var lastRelayAt time.Time
	if relay.LastRelayActivityUnix > 0 {
		lastRelayAt = time.Unix(relay.LastRelayActivityUnix, 0)
	}
	var lastUsefulAt time.Time
	if stats.lastUsefulUnix() > 0 {
		lastUsefulAt = time.Unix(stats.lastUsefulUnix(), 0)
	}
	record := m.svc.knownPeers[peer.addr]
	if peer.targetAddr != "" {
		if targetRecord, ok := m.svc.knownPeers[peer.targetAddr]; ok {
			record = targetRecord
		}
	}
	state := peerRetentionState{
		Score:             float64(m.svc.syncManager().downloadPeerScore(peer, now, localHeaderHeight)),
		Manual:            record.Manual,
		PreferredDownload: preferred[peer.addr],
		LastUsefulUnix:    stats.lastUsefulUnix(),
		LastRelayActivity: relay.LastRelayActivityUnix,
		HeaderStalls:      stats.HeaderStalls,
		BlockStalls:       stats.BlockStalls,
		TxStalls:          stats.TxStalls,
		PendingLocalRelay: relay.PendingLocalRelayTxs,
		SessionAgeSeconds: int64(now.Sub(peer.connectedAt).Seconds()),
	}
	if state.Manual {
		state.Protected = true
		state.ProtectionClass = peerProtectionManual
		state.UsefulnessClass = peerUsefulnessManual
		state.Score += 1_000_000
		return state
	}
	preferredUseful := state.PreferredDownload && (stats.UsefulBlocks > 0 || stats.UsefulHeaders > 0 || stats.UsefulTxs > 0 || peer.snapshotHeight() > localHeaderHeight || stats.lastUsefulUnix() > 0)
	if preferredUseful {
		state.Protected = true
		state.ProtectionClass = peerProtectionPreferredDownload
		state.UsefulnessClass = peerUsefulnessPreferredDownload
		state.Score += 50_000
	}
	if !lastUsefulAt.IsZero() {
		switch age := now.Sub(lastUsefulAt); {
		case age <= 15*time.Second:
			state.Score += 5_000
		case age <= time.Minute:
			state.Score += 1_500
		case age <= 5*time.Minute:
			state.Score += 300
		}
	}
	if relay.LastRelayActivityUnix > 0 {
		switch age := now.Sub(lastRelayAt); {
		case age <= 15*time.Second:
			state.Score += 1_200
		case age <= time.Minute:
			state.Score += 400
		}
	}
	if !peer.canServeDownloads(now) {
		state.Score -= 750
	}
	if relay.WriterStarvationEvents > 0 {
		state.Score -= float64(relay.WriterStarvationEvents * 100)
	}
	if !state.Protected && (!lastUsefulAt.IsZero() && now.Sub(lastUsefulAt) <= inboundProtectionWindow || relay.LastRelayActivityUnix > 0 && now.Sub(lastRelayAt) <= inboundProtectionWindow) {
		state.Protected = true
		state.ProtectionClass = peerProtectionRecentlyUseful
		state.Score += 10_000
	}
	switch {
	case state.UsefulnessClass != "":
	case stats.UsefulBlocks > 0 || stats.UsefulHeaders > 0 || stats.UsefulTxs > 0 || relay.LastRelayActivityUnix > 0:
		state.UsefulnessClass = peerUsefulnessFullRelay
	case state.Score <= 0 || (state.SessionAgeSeconds >= 60 && stats.lastUsefulUnix() == 0 && relay.LastRelayActivityUnix == 0):
		state.UsefulnessClass = peerUsefulnessLowValue
	default:
		state.UsefulnessClass = peerUsefulnessCatchUp
	}
	if !state.Protected && state.UsefulnessClass == peerUsefulnessFullRelay && !lastUsefulAt.IsZero() && now.Sub(lastUsefulAt) <= inboundProtectionWindow {
		state.Protected = true
		state.ProtectionClass = peerProtectionRecentlyUseful
		state.Score += 10_000
	}
	return state
}

func (m *peerManager) outboundCandidateStateLocked(addr string, manual bool, now time.Time) peerRetentionState {
	record := m.svc.knownPeers[addr]
	if manual {
		record.Manual = true
	}
	state := peerRetentionState{
		Score:           scoreKnownPeer(record, now),
		Manual:          record.Manual,
		UsefulnessClass: peerUsefulnessInactiveTarget,
	}
	if state.Manual {
		state.Protected = true
		state.ProtectionClass = peerProtectionManual
		state.UsefulnessClass = peerUsefulnessManual
	}
	return state
}

func (m *peerManager) inactiveOutboundTargetStateLocked(record storage.KnownPeerRecord, now time.Time) peerRetentionState {
	state := peerRetentionState{
		Score:           scoreKnownPeer(record, now),
		Manual:          record.Manual,
		UsefulnessClass: peerUsefulnessInactiveTarget,
	}
	if state.Manual {
		state.Protected = true
		state.ProtectionClass = peerProtectionManual
		state.UsefulnessClass = peerUsefulnessManual
		return state
	}
	if state.Score <= 0 {
		state.UsefulnessClass = peerUsefulnessLowValue
	}
	return state
}

func (m *peerManager) peerStateLessUseful(left peerRetentionState, right peerRetentionState, leftAddr string, rightAddr string) bool {
	if left.Score != right.Score {
		return left.Score < right.Score
	}
	if left.SessionAgeSeconds != right.SessionAgeSeconds {
		return left.SessionAgeSeconds > right.SessionAgeSeconds
	}
	return comparePeerAddrs(leftAddr, rightAddr) < 0
}

func (m *peerManager) disconnectPeer(peer *peerConn) {
	if peer == nil {
		return
	}
	m.svc.releasePeerBlockRequests(peer.addr)
	m.svc.releasePeerTxRequests(peer.addr)
	peer.close()
	if peer.wire != nil {
		_ = peer.wire.Close()
	}
}

func (m *peerManager) logPeerEvictionDecision(msg string, candidateAddr string, evictedAddr string, outbound bool, evicted peerRetentionState, candidate peerRetentionState) {
	attrs := []any{
		slog.String("candidate_addr", candidateAddr),
		slog.String("evicted_addr", evictedAddr),
		slog.Bool("outbound", outbound),
		slog.String("evicted_usefulness", string(evicted.UsefulnessClass)),
		slog.String("evicted_protected_class", string(evicted.ProtectionClass)),
		slog.Float64("evicted_score", evicted.Score),
		slog.Int("evicted_header_stalls", evicted.HeaderStalls),
		slog.Int("evicted_block_stalls", evicted.BlockStalls),
		slog.Int("evicted_tx_stalls", evicted.TxStalls),
		slog.Int64("evicted_last_useful_unix", evicted.LastUsefulUnix),
		slog.Int64("evicted_last_relay_activity_unix", evicted.LastRelayActivity),
		slog.Int64("evicted_session_age_seconds", evicted.SessionAgeSeconds),
	}
	if candidateAddr != "" {
		attrs = append(attrs,
			slog.Bool("candidate_manual", candidate.Manual),
			slog.String("candidate_usefulness", string(candidate.UsefulnessClass)),
			slog.Float64("candidate_score", candidate.Score),
		)
	}
	m.svc.logger.Info(msg, attrs...)
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
	for _, addr := range m.svc.cfg.Peers {
		addr = normalizePeerAddr(addr)
		if addr != "" && !m.isSelfPeerAddr(addr) {
			set[addr] = struct{}{}
		}
	}
	for addr := range m.svc.knownPeers {
		if addr != "" && !m.isSelfPeerAddr(addr) {
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

func (m *peerManager) loadPersistedKnownPeers(peers map[string]storage.KnownPeerRecord) {
	if len(peers) == 0 {
		return
	}
	m.svc.peerMu.Lock()
	defer m.svc.peerMu.Unlock()
	for addr, record := range peers {
		addr = normalizePeerAddr(addr)
		if addr == "" || m.isSelfPeerAddr(addr) {
			continue
		}
		m.svc.knownPeers[addr] = normalizeKnownPeerRecord(record)
	}
}

func (m *peerManager) recordKnownPeerSuccess(addr string, at time.Time) {
	addr = normalizePeerAddr(addr)
	if addr == "" || m.isSelfPeerAddr(addr) {
		return
	}
	m.svc.peerMu.Lock()
	if m.svc.knownPeers == nil {
		m.svc.knownPeers = make(map[string]storage.KnownPeerRecord)
	}
	at = at.UTC()
	record := m.svc.knownPeers[addr]
	record.LastSeen = at
	record.LastSuccess = at
	record.LastAttempt = at
	record.FailureCount = 0
	m.svc.knownPeers[addr] = normalizeKnownPeerRecord(record)
	trimKnownPeerMapLocked(m.svc.knownPeers)
	snapshot := cloneKnownPeerMap(m.svc.knownPeers)
	m.svc.peerMu.Unlock()
	if err := m.svc.chainState.Store().WriteKnownPeers(snapshot); err != nil {
		m.svc.logger.Warn("failed to persist known peer address book",
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
	if m.svc.knownPeers == nil {
		m.svc.knownPeers = make(map[string]storage.KnownPeerRecord)
	}
	now := time.Now().UTC()
	for _, addr := range addrs {
		addr = normalizePeerAddr(addr)
		if addr == "" || m.isSelfPeerAddr(addr) {
			continue
		}
		record := m.svc.knownPeers[addr]
		record.LastSeen = now
		m.svc.knownPeers[addr] = normalizeKnownPeerRecord(record)
	}
	trimKnownPeerMapLocked(m.svc.knownPeers)
	snapshot := cloneKnownPeerMap(m.svc.knownPeers)
	m.svc.peerMu.Unlock()
	if err := m.svc.chainState.Store().WriteKnownPeers(snapshot); err != nil {
		m.svc.logger.Warn("failed to persist peer address book", slog.Any("error", err))
	}
}

func (m *peerManager) restartKnownPeers() {
	for _, addr := range m.knownPeerAddrs() {
		addr = normalizePeerAddr(addr)
		if addr == "" || m.isSelfPeerAddr(addr) {
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
	manual := false
	m.svc.peerMu.Lock()
	if record, ok := m.svc.knownPeers[addr]; ok {
		manual = record.Manual
	}
	delete(m.svc.outboundPeers, addr)
	m.svc.peerMu.Unlock()
	if err := m.connectPeer(addr, manual); err != nil {
		m.svc.logger.Warn("failed to restart outbound peer", slog.String("addr", addr), slog.Any("error", err))
	}
}

func trimKnownPeerMapLocked(peers map[string]storage.KnownPeerRecord) {
	if len(peers) <= maxKnownPeerAddrs {
		return
	}
	type seenPeer struct {
		addr   string
		record storage.KnownPeerRecord
		score  float64
	}
	now := time.Now().UTC()
	known := make([]seenPeer, 0, len(peers))
	for addr, record := range peers {
		known = append(known, seenPeer{addr: addr, record: record, score: scoreKnownPeer(record, now)})
	}
	slices.SortFunc(known, func(a, b seenPeer) int {
		if a.record.Manual != b.record.Manual {
			if a.record.Manual {
				return 1
			}
			return -1
		}
		if a.score == b.score {
			return strings.Compare(a.addr, b.addr)
		}
		if a.score < b.score {
			return -1
		}
		return 1
	})
	for len(known) > maxKnownPeerAddrs {
		delete(peers, known[0].addr)
		known = known[1:]
	}
}

func cloneKnownPeerMap(peers map[string]storage.KnownPeerRecord) map[string]storage.KnownPeerRecord {
	if len(peers) == 0 {
		return nil
	}
	out := make(map[string]storage.KnownPeerRecord, len(peers))
	for addr, record := range peers {
		out[addr] = record
	}
	return out
}

func normalizeKnownPeerRecord(record storage.KnownPeerRecord) storage.KnownPeerRecord {
	record.LastSeen = record.LastSeen.UTC()
	record.LastSuccess = record.LastSuccess.UTC()
	record.LastAttempt = record.LastAttempt.UTC()
	if record.LastSeen.IsZero() {
		record.LastSeen = record.LastSuccess
	}
	return record
}

func scoreKnownPeer(record storage.KnownPeerRecord, now time.Time) float64 {
	score := 0.0
	if record.Manual {
		score += 1_000_000
	}
	if !record.LastSuccess.IsZero() {
		ageHours := now.Sub(record.LastSuccess).Hours()
		if ageHours < 0 {
			ageHours = 0
		}
		score += 10_000 - minFloat(ageHours, 10_000)
	}
	if !record.LastSeen.IsZero() {
		ageHours := now.Sub(record.LastSeen).Hours()
		if ageHours < 0 {
			ageHours = 0
		}
		score += 1_000 - minFloat(ageHours, 1_000)
	}
	score -= float64(record.FailureCount) * 500
	if !record.LastAttempt.IsZero() && now.Sub(record.LastAttempt) < addrSelectionCooldown {
		score -= 1_000
	}
	return score
}

func minFloat(left float64, right float64) float64 {
	if left < right {
		return left
	}
	return right
}

func peerNetgroup(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return strings.ToLower(host)
	}
	if ip.Is4() {
		raw := ip.As4()
		return fmt.Sprintf("v4:%d.%d", raw[0], raw[1])
	}
	raw := ip.As16()
	return fmt.Sprintf("v6:%x:%x:%x:%x", raw[0], raw[1], raw[2], raw[3])
}

func (m *peerManager) recordKnownPeerAttempt(addr string, at time.Time) {
	m.updateKnownPeerRecord(addr, func(record *storage.KnownPeerRecord) {
		record.LastSeen = at.UTC()
		record.LastAttempt = at.UTC()
	})
}

func (m *peerManager) recordKnownPeerFailure(addr string, at time.Time) {
	m.updateKnownPeerRecord(addr, func(record *storage.KnownPeerRecord) {
		record.LastSeen = at.UTC()
		record.LastAttempt = at.UTC()
		record.FailureCount++
	})
}

func (m *peerManager) updateKnownPeerRecord(addr string, mutate func(*storage.KnownPeerRecord)) {
	addr = normalizePeerAddr(addr)
	if addr == "" || m.isSelfPeerAddr(addr) {
		return
	}
	m.svc.peerMu.Lock()
	if m.svc.knownPeers == nil {
		m.svc.knownPeers = make(map[string]storage.KnownPeerRecord)
	}
	record := m.svc.knownPeers[addr]
	mutate(&record)
	m.svc.knownPeers[addr] = normalizeKnownPeerRecord(record)
	trimKnownPeerMapLocked(m.svc.knownPeers)
	snapshot := cloneKnownPeerMap(m.svc.knownPeers)
	m.svc.peerMu.Unlock()
	if err := m.svc.chainState.Store().WriteKnownPeers(snapshot); err != nil {
		m.svc.logger.Warn("failed to persist peer address book",
			slog.String("addr", addr),
			slog.Any("error", err),
		)
	}
}

func (m *peerManager) evictUnhealthyAutoPeer(addr string) bool {
	m.svc.peerMu.Lock()
	record, ok := m.svc.knownPeers[addr]
	if !ok || record.Manual {
		m.svc.peerMu.Unlock()
		return false
	}
	delete(m.svc.outboundPeers, addr)
	m.svc.peerMu.Unlock()
	m.svc.logger.Info("dropping unhealthy automatic outbound target",
		slog.String("addr", addr),
		slog.Uint64("failures", uint64(record.FailureCount)),
	)
	return true
}

func (m *peerManager) isSelfPeerAddr(addr string) bool {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return false
	}
	listenAddr := normalizePeerAddr(m.svc.cfg.P2PAddr)
	if listenAddr == "" {
		return false
	}
	if addr == listenAddr {
		return true
	}

	peerHost, peerPort, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	listenHost, listenPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return false
	}
	if peerPort != listenPort {
		return false
	}
	if isWildcardOrLoopbackHost(peerHost) {
		return true
	}
	if normalizePeerHost(peerHost) == normalizePeerHost(listenHost) && normalizePeerHost(listenHost) != "" {
		return true
	}
	peerIP, ok := parsePeerAddr(peerHost)
	if !ok {
		return false
	}
	return localInterfaceIPsContain(peerIP)
}

func isWildcardOrLoopbackHost(host string) bool {
	host = normalizePeerHost(host)
	switch host {
	case "", "0.0.0.0", "::", "::1", "127.0.0.1", "localhost":
		return true
	default:
		return false
	}
}

func normalizePeerHost(host string) string {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	return strings.ToLower(host)
}

func parsePeerAddr(host string) (netip.Addr, bool) {
	addr, err := netip.ParseAddr(normalizePeerHost(host))
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

func localInterfaceIPsContain(target netip.Addr) bool {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, addr := range addrs {
		var ip net.IP
		switch value := addr.(type) {
		case *net.IPNet:
			ip = value.IP
		case *net.IPAddr:
			ip = value.IP
		}
		if ip == nil {
			continue
		}
		if parsed, ok := netip.AddrFromSlice(ip); ok && parsed.Unmap() == target {
			return true
		}
	}
	return false
}
