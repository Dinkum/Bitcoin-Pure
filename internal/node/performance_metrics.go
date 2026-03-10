package node

import (
	"slices"
	"sync"
	"time"
)

const performanceMetricSampleLimit = 256

type PerformanceMetrics struct {
	GeneratedAt time.Time               `json:"generated_at"`
	Counters    PerformanceCounters     `json:"counters"`
	Gauges      PerformanceGauges       `json:"gauges"`
	Latency     PerformanceLatencyGroup `json:"latency"`
}

type PerformanceCounters struct {
	AdmittedTxs           uint64 `json:"admitted_txs"`
	OrphanPromotions      uint64 `json:"orphan_promotions"`
	RelayedTxItems        uint64 `json:"relayed_tx_items"`
	RelayedBlockItems     uint64 `json:"relayed_block_items"`
	BlocksAccepted        uint64 `json:"blocks_accepted"`
	TemplateRebuilds      uint64 `json:"template_rebuilds"`
	TemplateInterruptions uint64 `json:"template_interruptions"`
	PeerStallEvents       uint64 `json:"peer_stall_events"`
	TxReconRetries        uint64 `json:"tx_recon_retries"`
	DirectFallbackBatches uint64 `json:"direct_fallback_batches"`
	DirectFallbackTxs     uint64 `json:"direct_fallback_txs"`
	TxRequestsReceived    uint64 `json:"tx_requests_received"`
	TxNotFoundSent        uint64 `json:"tx_not_found_sent"`
	TxNotFoundReceived    uint64 `json:"tx_not_found_received"`
	KnownTxClears         uint64 `json:"known_tx_clears"`
	DuplicateSuppressions uint64 `json:"duplicate_suppressions"`
	WriterStarvation      uint64 `json:"writer_starvation_events"`
}

type PerformanceGauges struct {
	MempoolTxs           int `json:"mempool_txs"`
	MempoolOrphans       int `json:"mempool_orphans"`
	CandidateFrontier    int `json:"candidate_frontier"`
	PeerCount            int `json:"peer_count"`
	UsefulPeers          int `json:"useful_peers"`
	RelayQueueDepth      int `json:"relay_queue_depth"`
	RelayQueueDepthPeak  int `json:"relay_queue_depth_peak"`
	ControlQueueDepth    int `json:"control_queue_depth"`
	PriorityQueueDepth   int `json:"priority_queue_depth"`
	SendQueueDepth       int `json:"send_queue_depth"`
	PendingLocalRelayTxs int `json:"pending_local_relay_txs"`
	PendingPeerBlocks    int `json:"pending_peer_blocks"`
	InflightBlockReqs    int `json:"inflight_block_requests"`
	InflightTxReqs       int `json:"inflight_tx_requests"`
}

type PerformanceLatencyGroup struct {
	Admission          DurationHistogramSummary `json:"admission"`
	Template           DurationHistogramSummary `json:"template"`
	BlockApply         DurationHistogramSummary `json:"block_apply"`
	BlockApplyLockWait DurationHistogramSummary `json:"block_apply_lock_wait"`
	RelayFlush         DurationHistogramSummary `json:"relay_flush"`
	SyncReq            DurationHistogramSummary `json:"sync_request"`
}

type DurationHistogramSummary struct {
	Count int     `json:"count"`
	AvgMS float64 `json:"avg_ms,omitempty"`
	P50MS float64 `json:"p50_ms,omitempty"`
	P95MS float64 `json:"p95_ms,omitempty"`
	MaxMS float64 `json:"max_ms,omitempty"`
}

type performanceMetricsCollector struct {
	mu                 sync.Mutex
	admission          durationMetricWindow
	template           durationMetricWindow
	blockApply         durationMetricWindow
	blockApplyLockWait durationMetricWindow
	relayFlush         durationMetricWindow
	syncReq            durationMetricWindow
}

type durationMetricWindow struct {
	samples []float64
	next    int
	full    bool
	totalMS float64
	maxMS   float64
}

func (c *performanceMetricsCollector) noteAdmissionDuration(d time.Duration) {
	c.record(&c.admission, d)
}

func (c *performanceMetricsCollector) noteTemplateDuration(d time.Duration) {
	c.record(&c.template, d)
}

func (c *performanceMetricsCollector) noteBlockApplyDuration(d time.Duration) {
	c.record(&c.blockApply, d)
}

func (c *performanceMetricsCollector) noteBlockApplyLockWaitDuration(d time.Duration) {
	c.record(&c.blockApplyLockWait, d)
}

func (c *performanceMetricsCollector) noteRelayFlushDuration(d time.Duration) {
	c.record(&c.relayFlush, d)
}

func (c *performanceMetricsCollector) noteSyncRequestDuration(d time.Duration) {
	c.record(&c.syncReq, d)
}

func (c *performanceMetricsCollector) record(target *durationMetricWindow, d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	target.add(float64(d.Microseconds()) / 1000)
}

func (w *durationMetricWindow) add(sampleMS float64) {
	if sampleMS <= 0 {
		return
	}
	if len(w.samples) < performanceMetricSampleLimit {
		w.samples = append(w.samples, sampleMS)
	} else {
		w.samples[w.next] = sampleMS
		w.next = (w.next + 1) % len(w.samples)
		w.full = true
	}
	w.totalMS += sampleMS
	if sampleMS > w.maxMS {
		w.maxMS = sampleMS
	}
}

func (c *performanceMetricsCollector) snapshot() PerformanceLatencyGroup {
	c.mu.Lock()
	defer c.mu.Unlock()
	return PerformanceLatencyGroup{
		Admission:          c.admission.summary(),
		Template:           c.template.summary(),
		BlockApply:         c.blockApply.summary(),
		BlockApplyLockWait: c.blockApplyLockWait.summary(),
		RelayFlush:         c.relayFlush.summary(),
		SyncReq:            c.syncReq.summary(),
	}
}

func (w durationMetricWindow) summary() DurationHistogramSummary {
	if len(w.samples) == 0 {
		return DurationHistogramSummary{}
	}
	ordered := append([]float64(nil), w.samples...)
	slices.Sort(ordered)
	count := len(ordered)
	total := 0.0
	for _, sample := range ordered {
		total += sample
	}
	return DurationHistogramSummary{
		Count: count,
		AvgMS: total / float64(count),
		P50MS: ordered[(count-1)*50/100],
		P95MS: ordered[(count-1)*95/100],
		MaxMS: ordered[count-1],
	}
}

func (s *Service) PerformanceMetrics() PerformanceMetrics {
	now := time.Now()
	mempoolInfo := s.MempoolInfo()
	peers := s.PeerInfo()
	relay := s.RelayPeerStats()
	usefulPeers := countUsefulPeers(peers, now, usefulPeerWindow(s.cfg.ThroughputSummaryInterval, s.cfg.StallTimeout))
	relayQueueDepth, relayQueuePeak, controlQueueDepth, priorityQueueDepth, sendQueueDepth, pendingLocalRelayTxs := relayQueueDepths(relay)
	inflightBlocks, inflightTxs, pendingPeerBlocks := s.downloadSummary()
	stallEvents := uint64(0)
	for _, peer := range peers {
		stallEvents += uint64(peer.HeaderStalls + peer.BlockStalls + peer.TxStalls)
	}
	return PerformanceMetrics{
		GeneratedAt: now,
		Counters: PerformanceCounters{
			AdmittedTxs:           s.throughput.admittedTxs.Load(),
			OrphanPromotions:      s.throughput.orphanPromotions.Load(),
			RelayedTxItems:        s.throughput.relayedTxItems.Load(),
			RelayedBlockItems:     s.throughput.relayedBlockItems.Load(),
			BlocksAccepted:        s.throughput.blocksAccepted.Load(),
			TemplateRebuilds:      s.throughput.templateRebuilds.Load(),
			TemplateInterruptions: s.throughput.templateInterruptions.Load(),
			PeerStallEvents:       stallEvents,
			TxReconRetries:        s.throughput.txReconRetries.Load(),
			DirectFallbackBatches: s.throughput.directFallbackBatches.Load(),
			DirectFallbackTxs:     s.throughput.directFallbackTxs.Load(),
			TxRequestsReceived:    s.throughput.txRequestsReceived.Load(),
			TxNotFoundSent:        s.throughput.txNotFoundSent.Load(),
			TxNotFoundReceived:    s.throughput.txNotFoundReceived.Load(),
			KnownTxClears:         s.throughput.knownTxClears.Load(),
			DuplicateSuppressions: s.throughput.duplicateSuppressions.Load(),
			WriterStarvation:      s.throughput.writerStarvation.Load(),
		},
		Gauges: PerformanceGauges{
			MempoolTxs:           mempoolInfo.Count,
			MempoolOrphans:       mempoolInfo.Orphans,
			CandidateFrontier:    mempoolInfo.CandidateFrontier,
			PeerCount:            len(peers),
			UsefulPeers:          usefulPeers,
			RelayQueueDepth:      relayQueueDepth,
			RelayQueueDepthPeak:  relayQueuePeak,
			ControlQueueDepth:    controlQueueDepth,
			PriorityQueueDepth:   priorityQueueDepth,
			SendQueueDepth:       sendQueueDepth,
			PendingLocalRelayTxs: pendingLocalRelayTxs,
			PendingPeerBlocks:    pendingPeerBlocks,
			InflightBlockReqs:    inflightBlocks,
			InflightTxReqs:       inflightTxs,
		},
		Latency: s.perf.snapshot(),
	}
}
