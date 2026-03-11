package node

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"bitcoin-pure/internal/mempool"
)

const minimumUsefulPeerWindow = time.Minute

type throughputSummaryTelemetry struct {
	admittedTxs           atomic.Uint64
	orphanPromotions      atomic.Uint64
	relayedTxItems        atomic.Uint64
	relayedBlockItems     atomic.Uint64
	blocksAccepted        atomic.Uint64
	templateRebuilds      atomic.Uint64
	templateInterruptions atomic.Uint64
	erlayRounds           atomic.Uint64
	erlayRequestedTxs     atomic.Uint64
	grapheneP1Plans       atomic.Uint64
	grapheneExtPlans      atomic.Uint64
	grapheneDecodeFails   atomic.Uint64
	grapheneExtRecoveries atomic.Uint64
	legacyRelayFallbacks  atomic.Uint64
	txReconRetries        atomic.Uint64
	directFallbackBatches atomic.Uint64
	directFallbackTxs     atomic.Uint64
	txRequestsReceived    atomic.Uint64
	txNotFoundSent        atomic.Uint64
	txNotFoundReceived    atomic.Uint64
	knownTxClears         atomic.Uint64
	duplicateSuppressions atomic.Uint64
	writerStarvation      atomic.Uint64

	mu   sync.Mutex
	last throughputCounterSnapshot
}

type throughputCounterSnapshot struct {
	takenAt               time.Time
	admittedTxs           uint64
	orphanPromotions      uint64
	relayedTxItems        uint64
	relayedBlockItems     uint64
	blocksAccepted        uint64
	templateRebuilds      uint64
	templateInterruptions uint64
}

func (s *Service) throughputSummaryLoop() {
	ticker := time.NewTicker(s.cfg.ThroughputSummaryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case now := <-ticker.C:
			s.emitThroughputSummary(now)
		}
	}
}

func (s *Service) noteAcceptedAdmissions(admissions []mempool.Admission) {
	accepted := 0
	promoted := 0
	for _, admission := range admissions {
		accepted += len(admission.Accepted)
		if len(admission.Accepted) > 1 {
			// The first accepted tx is the directly submitted item; anything after
			// that came from orphan promotion becoming newly ready.
			promoted += len(admission.Accepted) - 1
		}
	}
	if accepted > 0 {
		s.throughput.admittedTxs.Add(uint64(accepted))
	}
	if promoted > 0 {
		s.throughput.orphanPromotions.Add(uint64(promoted))
	}
}

func (s *Service) noteRelaySent(class relayMessageClass) {
	txItems := class.txInvItems + class.txBatchItems + class.txReconItems
	if txItems > 0 {
		s.throughput.relayedTxItems.Add(uint64(txItems))
	}
	if class.blockInvItems > 0 {
		s.throughput.relayedBlockItems.Add(uint64(class.blockInvItems))
	}
}

func (s *Service) noteTxReconRetry(count int) {
	if count > 0 {
		s.throughput.txReconRetries.Add(uint64(count))
	}
}

func (s *Service) noteErlayRound(requested int) {
	if requested > 0 {
		s.throughput.erlayRounds.Add(1)
		s.throughput.erlayRequestedTxs.Add(uint64(requested))
	}
}

func (s *Service) noteGraphenePlan(plan blockRelayPlan) {
	switch plan {
	case blockRelayPlanGrapheneP1:
		s.throughput.grapheneP1Plans.Add(1)
	case blockRelayPlanGrapheneExtended:
		s.throughput.grapheneExtPlans.Add(1)
	default:
		s.throughput.legacyRelayFallbacks.Add(1)
	}
}

func (s *Service) noteGrapheneDecodeFailure() {
	s.throughput.grapheneDecodeFails.Add(1)
}

func (s *Service) noteGrapheneExtendedRecovery() {
	s.throughput.grapheneExtRecoveries.Add(1)
}

func (s *Service) noteDirectFallback(msgs int, items int) {
	if msgs > 0 {
		s.throughput.directFallbackBatches.Add(uint64(msgs))
	}
	if items > 0 {
		s.throughput.directFallbackTxs.Add(uint64(items))
	}
}

func (s *Service) noteTxRequestsReceived(count int) {
	if count > 0 {
		s.throughput.txRequestsReceived.Add(uint64(count))
	}
}

func (s *Service) noteTxNotFoundSent(count int) {
	if count > 0 {
		s.throughput.txNotFoundSent.Add(uint64(count))
	}
}

func (s *Service) noteTxNotFoundReceived(count int) {
	if count > 0 {
		s.throughput.txNotFoundReceived.Add(uint64(count))
	}
}

func (s *Service) noteKnownTxClears(count int) {
	if count > 0 {
		s.throughput.knownTxClears.Add(uint64(count))
	}
}

func (s *Service) noteDuplicateSuppression(count int) {
	if count > 0 {
		s.throughput.duplicateSuppressions.Add(uint64(count))
	}
}

func (s *Service) noteWriterStarvation(count int) {
	if count > 0 {
		s.throughput.writerStarvation.Add(uint64(count))
	}
}

func (s *Service) noteBlockAccepted() {
	s.throughput.blocksAccepted.Add(1)
}

func (s *Service) noteTemplateRebuild() {
	s.throughput.templateRebuilds.Add(1)
}

func (s *Service) noteTemplateInterruption() {
	s.throughput.templateInterruptions.Add(1)
}

func (s *Service) emitThroughputSummary(now time.Time) {
	if s.logger == nil {
		return
	}
	current, previous, window := s.snapshotThroughputCounters(now)
	if window <= 0 {
		return
	}

	mempoolInfo := s.MempoolInfo()
	peers := s.PeerInfo()
	relay := s.RelayPeerStats()
	usefulPeers := countUsefulPeers(peers, now, usefulPeerWindow(s.cfg.ThroughputSummaryInterval, s.cfg.StallTimeout))
	currentQueueDepth, maxQueueDepth, controlQueueDepth, priorityQueueDepth, sendQueueDepth, pendingLocalRelayTxs := relayQueueDepths(relay)
	inflightBlocks, inflightTxs, pendingPeerBlocks := s.downloadSummary()

	s.logger.LogAttrs(context.Background(), slog.LevelInfo, "throughput summary",
		slog.Duration("window", window),
		slog.Int("admitted_txs", int(current.admittedTxs-previous.admittedTxs)),
		slog.Float64("admitted_txs_per_sec", ratePerSecond(current.admittedTxs-previous.admittedTxs, window)),
		slog.Int("relayed_tx_items", int(current.relayedTxItems-previous.relayedTxItems)),
		slog.Float64("relayed_tx_items_per_sec", ratePerSecond(current.relayedTxItems-previous.relayedTxItems, window)),
		slog.Int("relayed_block_items", int(current.relayedBlockItems-previous.relayedBlockItems)),
		slog.Float64("relayed_block_items_per_sec", ratePerSecond(current.relayedBlockItems-previous.relayedBlockItems, window)),
		slog.Int("blocks_accepted", int(current.blocksAccepted-previous.blocksAccepted)),
		slog.Float64("blocks_accepted_per_sec", ratePerSecond(current.blocksAccepted-previous.blocksAccepted, window)),
		slog.Int("template_rebuilds", int(current.templateRebuilds-previous.templateRebuilds)),
		slog.Int("template_interruptions", int(current.templateInterruptions-previous.templateInterruptions)),
		slog.Int("orphan_promotions", int(current.orphanPromotions-previous.orphanPromotions)),
		slog.Int("mempool_txs", mempoolInfo.Count),
		slog.Int("mempool_orphans", mempoolInfo.Orphans),
		slog.Int("candidate_frontier", mempoolInfo.CandidateFrontier),
		slog.Int("peer_count", len(peers)),
		slog.Int("useful_peers", usefulPeers),
		slog.Int("relay_queue_depth", currentQueueDepth),
		slog.Int("relay_queue_depth_peak", maxQueueDepth),
		slog.Int("control_queue_depth", controlQueueDepth),
		slog.Int("priority_queue_depth", priorityQueueDepth),
		slog.Int("send_queue_depth", sendQueueDepth),
		slog.Int("pending_local_relay_txs", pendingLocalRelayTxs),
		slog.Int("pending_peer_blocks", pendingPeerBlocks),
		slog.Int("inflight_block_requests", inflightBlocks),
		slog.Int("inflight_tx_requests", inflightTxs),
	)
}

func (s *Service) snapshotThroughputCounters(now time.Time) (throughputCounterSnapshot, throughputCounterSnapshot, time.Duration) {
	current := throughputCounterSnapshot{
		takenAt:               now,
		admittedTxs:           s.throughput.admittedTxs.Load(),
		orphanPromotions:      s.throughput.orphanPromotions.Load(),
		relayedTxItems:        s.throughput.relayedTxItems.Load(),
		relayedBlockItems:     s.throughput.relayedBlockItems.Load(),
		blocksAccepted:        s.throughput.blocksAccepted.Load(),
		templateRebuilds:      s.throughput.templateRebuilds.Load(),
		templateInterruptions: s.throughput.templateInterruptions.Load(),
	}

	s.throughput.mu.Lock()
	defer s.throughput.mu.Unlock()

	previous := s.throughput.last
	if previous.takenAt.IsZero() {
		previous.takenAt = s.startedAt
	}
	s.throughput.last = current
	return current, previous, current.takenAt.Sub(previous.takenAt)
}

func usefulPeerWindow(summaryInterval time.Duration, stallTimeout time.Duration) time.Duration {
	window := minimumUsefulPeerWindow
	if summaryInterval > window {
		window = summaryInterval
	}
	if stallTimeout > 0 && 2*stallTimeout > window {
		window = 2 * stallTimeout
	}
	return window
}

func countUsefulPeers(peers []PeerInfo, now time.Time, window time.Duration) int {
	count := 0
	cutoff := now.Add(-window)
	for _, peer := range peers {
		if peer.LastUseful == 0 {
			continue
		}
		if time.Unix(peer.LastUseful, 0).Before(cutoff) {
			continue
		}
		count++
	}
	return count
}

func relayQueueDepths(stats []PeerRelayStats) (int, int, int, int, int, int) {
	total := 0
	maxDepth := 0
	control := 0
	priority := 0
	send := 0
	pendingLocal := 0
	for _, peer := range stats {
		total += peer.QueueDepth
		control += peer.ControlQueueDepth
		priority += peer.PriorityQueueDepth
		send += peer.SendQueueDepth
		pendingLocal += peer.PendingLocalRelayTxs
		if peer.MaxQueueDepth > maxDepth {
			maxDepth = peer.MaxQueueDepth
		}
	}
	return total, maxDepth, control, priority, send, pendingLocal
}

func (s *Service) downloadSummary() (int, int, int) {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	return len(s.blockRequests), len(s.txRequests), len(s.pendingBlocks)
}

func ratePerSecond(delta uint64, window time.Duration) float64 {
	if delta == 0 || window <= 0 {
		return 0
	}
	return float64(delta) / window.Seconds()
}
