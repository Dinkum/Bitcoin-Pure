package node

import (
	"log/slog"
	"sync"
	"time"
)

const nodeStatusInterval = 10 * time.Second

type runtimeStatusTelemetry struct {
	mu              sync.Mutex
	lastSyncPhase   string
	lastMempoolLoad string
}

func (s *Service) nodeStatusLoop() {
	ticker := time.NewTicker(nodeStatusInterval)
	defer ticker.Stop()

	s.emitNodeStatus(time.Now())
	for {
		select {
		case <-s.stopCh:
			return
		case now := <-ticker.C:
			s.emitNodeStatus(now)
		}
	}
}

func (s *Service) emitNodeStatus(now time.Time) {
	if s.logger == nil {
		return
	}

	peerSnapshot := s.snapshotPeers()
	peerInfo := s.PeerInfo()
	relayStats := s.RelayPeerStats()
	mempoolInfo := s.MempoolInfo()
	inflightBlocks, inflightTxs, pendingBlocks := s.downloadSummary()
	usefulPeers := countUsefulPeers(peerInfo, now, usefulPeerWindow(nodeStatusInterval, s.cfg.StallTimeout))
	relayQueueDepth, _, _, _, _, pendingLocalRelayTxs := relayQueueDepths(relayStats)
	inboundPeers, outboundPeers := countPeerDirections(peerInfo)
	activeHeaderRequests := countActiveHeaderRequests(peerSnapshot)
	tipHeight := s.blockHeight()
	headerHeight := s.headerHeight()
	phase := classifySyncPhase(s.cfg, len(peerInfo), usefulPeers, tipHeight, headerHeight, inflightBlocks, pendingBlocks, activeHeaderRequests)
	mempoolLoad := classifyMempoolPressure(mempoolInfo, s.cfg.MaxOrphans)

	if previous, changed := s.runtimeStatus.noteSyncPhase(phase); changed {
		s.logger.Info("sync phase changed",
			slog.String("from", defaultRuntimeState(previous)),
			slog.String("to", phase),
			slog.Int("peer_count", len(peerInfo)),
			slog.Int("useful_peers", usefulPeers),
			slog.Uint64("tip_height", tipHeight),
			slog.Uint64("header_height", headerHeight),
			slog.Int("inflight_block_requests", inflightBlocks),
			slog.Int("pending_peer_blocks", pendingBlocks),
		)
	}
	if previous, changed := s.runtimeStatus.noteMempoolLoad(mempoolLoad); changed {
		s.logger.Info("mempool pressure changed",
			slog.String("from", defaultRuntimeState(previous)),
			slog.String("to", mempoolLoad),
			slog.Int("txs", mempoolInfo.Count),
			slog.Int("bytes", mempoolInfo.Bytes),
			slog.Int("orphans", mempoolInfo.Orphans),
		)
	}

	attrs := []any{
		slog.String("phase", phase),
		slog.Uint64("tip_height", tipHeight),
		slog.Uint64("header_height", headerHeight),
		slog.Int("peer_count", len(peerInfo)),
		slog.Int("inbound_peers", inboundPeers),
		slog.Int("outbound_peers", outboundPeers),
		slog.Int("useful_peers", usefulPeers),
		slog.Int("mempool_txs", mempoolInfo.Count),
		slog.Int("mempool_bytes", mempoolInfo.Bytes),
		slog.Int("orphans", mempoolInfo.Orphans),
		slog.Int("relay_queue_depth", relayQueueDepth),
		slog.Int("pending_local_relay_txs", pendingLocalRelayTxs),
		slog.Int("inflight_block_requests", inflightBlocks),
		slog.Int("inflight_tx_requests", inflightTxs),
		slog.Int("pending_peer_blocks", pendingBlocks),
		slog.Uint64("erlay_rounds", s.throughput.erlayRounds.Load()),
		slog.Uint64("graphene_recoveries", s.throughput.grapheneExtRecoveries.Load()),
		slog.Bool("mining_enabled", s.cfg.MinerEnabled),
	}
	if tipTime := s.blockTipTime(); !tipTime.IsZero() && !now.Before(tipTime) {
		attrs = append(attrs, slog.Duration("last_block_ago", now.Sub(tipTime).Round(time.Second)))
	}
	s.logger.Info("node status", attrs...)
}

func (t *runtimeStatusTelemetry) noteSyncPhase(phase string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastSyncPhase == phase {
		return t.lastSyncPhase, false
	}
	previous := t.lastSyncPhase
	t.lastSyncPhase = phase
	return previous, true
}

func (t *runtimeStatusTelemetry) noteMempoolLoad(load string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastMempoolLoad == load {
		return t.lastMempoolLoad, false
	}
	previous := t.lastMempoolLoad
	t.lastMempoolLoad = load
	return previous, true
}

func defaultRuntimeState(state string) string {
	if state == "" {
		return "unknown"
	}
	return state
}

func countPeerDirections(peers []PeerInfo) (int, int) {
	inbound := 0
	outbound := 0
	for _, peer := range peers {
		if peer.Outbound {
			outbound++
			continue
		}
		inbound++
	}
	return inbound, outbound
}

func countActiveHeaderRequests(peers []*peerConn) int {
	count := 0
	for _, peer := range peers {
		if peer.syncSnapshot().HeadersRequestedAt.IsZero() {
			continue
		}
		count++
	}
	return count
}

func classifySyncPhase(cfg ServiceConfig, peerCount int, usefulPeers int, tipHeight uint64, headerHeight uint64, inflightBlocks int, pendingBlocks int, activeHeaderRequests int) string {
	networkEnabled := cfg.P2PAddr != "" || len(cfg.Peers) > 0
	switch {
	case !networkEnabled:
		return "standalone"
	case peerCount == 0:
		return "seeking_peers"
	case headerHeight > tipHeight || inflightBlocks > 0 || pendingBlocks > 0:
		return "catching_up_blocks"
	case activeHeaderRequests > 0:
		return "catching_up_headers"
	case usefulPeers == 0:
		return "peering"
	default:
		return "steady"
	}
}

func classifyMempoolPressure(info MempoolInfo, maxOrphans int) string {
	if maxOrphans <= 0 {
		maxOrphans = 128
	}
	maxBytes := info.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	switch {
	case info.Count == 0 && info.Orphans == 0 && info.Bytes == 0:
		return "idle"
	case info.Bytes >= (maxBytes*8)/10 || info.Count >= 10_000 || info.Orphans >= maxInt(32, maxOrphans/2):
		return "high"
	case info.Bytes >= maxInt(16<<20, maxBytes/4) || info.Count >= 1_000 || info.Orphans > 0:
		return "active"
	default:
		return "normal"
	}
}

func (s *Service) blockTipTime() time.Time {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if s.chainState == nil {
		return time.Time{}
	}
	tip := s.chainState.ChainState().TipHeader()
	if tip == nil || tip.Timestamp == 0 {
		return time.Time{}
	}
	return time.Unix(int64(tip.Timestamp), 0).UTC()
}
