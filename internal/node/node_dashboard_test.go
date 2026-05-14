package node

import (
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/types"
	"encoding/hex"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestRenderBlockFlowMarksTip(t *testing.T) {
	blocks := []dashboardBlockPage{
		{Height: 10, Hash: [32]byte{0xaa}},
		{Height: 11, Hash: [32]byte{0xbb}},
	}
	out := renderBlockFlow(blocks)
	if !strings.Contains(out, "tip 11") {
		t.Fatalf("expected tip marker in block flow: %q", out)
	}
	if !strings.Contains(out, "height 10") {
		t.Fatalf("expected height label in block flow: %q", out)
	}
	if !strings.Contains(out, "...") {
		t.Fatalf("expected ellipsis-truncated hashes in block flow: %q", out)
	}
	if strings.Count(out, "--->") != 1 {
		t.Fatalf("expected single arrows in block flow: %q", out)
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected four block flow rows, got %d: %q", len(lines), out)
	}
	if !strings.Contains(lines[2], "--->") {
		t.Fatalf("expected hash row to include arrows: %q", out)
	}
	tagPattern := regexp.MustCompile(`<[^>]+>`)
	plainLines := make([]string, 0, len(lines))
	for _, line := range lines {
		plainLines = append(plainLines, tagPattern.ReplaceAllString(line, ""))
	}
	for i := 1; i < len(plainLines); i++ {
		if len(plainLines[i]) != len(plainLines[0]) {
			t.Fatalf("expected aligned block flow rows, got %d vs %d: %q", len(plainLines[i]), len(plainLines[0]), out)
		}
	}
}

func TestRenderDashboardSystemSectionIncludesHumanReadableStats(t *testing.T) {
	section := renderDashboardSystemSection(dashboardSystemSummary{
		Window:          10 * time.Minute,
		CPUPercent:      42.5,
		HasCPU:          true,
		RxBytesPerSec:   2 * 1024 * 1024,
		TxBytesPerSec:   512 * 1024,
		HasNetwork:      true,
		AvgMemUsedBytes: 3 * 1024 * 1024 * 1024,
		MemTotalBytes:   8 * 1024 * 1024 * 1024,
		HasMemory:       true,
		Load1:           0.42,
		Load5:           0.37,
		Load15:          0.31,
		HasLoad:         true,
		RunningProcs:    2,
		TotalProcs:      140,
		Cores:           4,
	})

	for _, want := range []string{
		"NODE SYSTEM (AVG 10M)",
		"CPU Avg      : 42.5%",
		"Network Up   : 512.0 KB/s",
		"Network Down : 2.0 MB/s",
		"42.5%",
		"2.0 MB/s",
		"512.0 KB/s",
		"3.0 GB / 8.0 GB (38%)",
		"Load Avg     : 0.42 / 0.37 / 0.31",
		"0.42 / 0.37 / 0.31",
		"Processes    : 2 run / 140 total",
		"CPU Cores    : 4 cores",
		"2 run / 140 total",
		"4 cores",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in section:\n%s", want, section)
		}
	}
}

func TestRenderMempoolSectionUsesQueuedFeeList(t *testing.T) {
	section := renderMempoolSection(dashboardMempoolSummary{
		Count:         256,
		Bytes:         2_190,
		Orphans:       3,
		MedianFee:     1_000,
		LowFee:        1_000,
		HighFee:       1_000,
		EstimatedNext: 10 * time.Minute,
		Top: []mempool.SnapshotEntry{{
			Tx:   types.Transaction{},
			TxID: [32]byte{0xaa},
			Fee:  219,
			Size: 219,
		}},
	})

	for _, want := range []string{
		"Tx Count                          : 256",
		"Total Size                        : 2,190 bytes",
		"Orphan Tx Count                   : 3",
		"Fee Min / Median / Max (atoms/kB) : 1,000 / 1,000 / 1,000",
		"top queued tx by fee",
		"aa00000...",
		"1,000 atoms/kB",
		"219 B",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in mempool section:\n%s", want, section)
		}
	}
	if strings.Contains(section, "Next Block") || strings.Contains(section, "Queue Top") {
		t.Fatalf("expected legacy mempool labels to be removed:\n%s", section)
	}
}

func TestRenderCandidateBlockSectionUsesDedicatedLabels(t *testing.T) {
	section := renderCandidateBlockSection(BlockTemplateStats{
		FrontierCandidates: 10,
		Rebuilds:           253,
		Interruptions:      157,
		Invalidations:      269,
		LastBuildAgeMS:     int((38 * time.Second).Milliseconds()),
		LastReason:         "interrupted",
	})

	for _, want := range []string{
		"Status",
		"rebuilding",
		"Age",
		"38s",
		"Tx Candidate Txs",
		"10",
		"Rebuilds (1h)",
		"253",
		"Interrupts (1h)",
		"157",
		"Invalidations (1h)",
		"269",
		"Last Reason",
		"interrupted",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in candidate block section:\n%s", want, section)
		}
	}
}

func TestRenderPerformanceSectionShowsCountersGaugesAndLatency(t *testing.T) {
	section := renderPerformanceSection(PerformanceMetrics{
		GeneratedAt: time.Unix(1_700_000_000, 0).UTC(),
		Counters: PerformanceCounters{
			AdmittedTxs:           128,
			OrphanPromotions:      7,
			RelayedTxItems:        256,
			RelayedBlockItems:     4,
			BlocksAccepted:        3,
			TemplateRebuilds:      5,
			TemplateInterruptions: 2,
			PeerStallEvents:       6,
		},
		Gauges: PerformanceGauges{
			MempoolTxs:          512,
			MempoolOrphans:      9,
			CandidateFrontier:   64,
			PeerCount:           8,
			UsefulPeers:         3,
			RelayQueueDepth:     14,
			RelayQueueDepthPeak: 29,
			PendingPeerBlocks:   2,
			InflightBlockReqs:   5,
			InflightTxReqs:      11,
		},
		Latency: PerformanceLatencyGroup{
			Admission:  DurationHistogramSummary{Count: 8, AvgMS: 12.5, P50MS: 11, P95MS: 19, MaxMS: 22},
			Template:   DurationHistogramSummary{Count: 5, AvgMS: 40, P50MS: 35, P95MS: 70, MaxMS: 72},
			BlockApply: DurationHistogramSummary{Count: 3, AvgMS: 55, P50MS: 54, P95MS: 61, MaxMS: 61},
			RelayFlush: DurationHistogramSummary{Count: 10, AvgMS: 2.5, P50MS: 2.2, P95MS: 4.8, MaxMS: 5.1},
			SyncReq:    DurationHistogramSummary{Count: 7, AvgMS: 7.5, P50MS: 7.0, P95MS: 11.2, MaxMS: 12.0},
		},
	})

	for _, want := range []string{
		"Generated    : 2023-11-14 22:13:20 UTC",
		"Throughput   : admitted tx",
		"Relay        : tx items",
		"Template     : rebuilds",
		"Peers        : connected",
		"Queues       : relay now",
		"Admission : count 8",
		"Template  : count 5",
		"Relay     : count 10",
		"Sync      : count 7",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in performance section:\n%s", want, section)
		}
	}
}

func TestRenderTPSChartUsesBlockThroughputLayout(t *testing.T) {
	section := renderTPSChart(dashboardTPSChart{
		Label: "THROUGHPUT (BY BLOCK)",
		Blocks: []dashboardThroughputBlock{
			{TxCount: 121, BarWidth: 12},
			{TxCount: 167, BarWidth: 17},
			{TxCount: 38, BarWidth: 4},
			{TxCount: 241, BarWidth: 24},
			{TxCount: 92, BarWidth: 9},
			{TxCount: 184, Candidate: true, BarWidth: 18},
		},
		TotalTx:     843,
		AvgTPS:      0.23,
		AvgTxPerBlk: 140.5,
	})

	for _, want := range []string{
		"121 tx  |",
		"167 tx  |",
		"38 tx  |",
		"241 tx  |",
		"92 tx  |",
		"184 tx* |",
		"843 tx total over last 6 blocks (0.23 avg TPS)",
		"140.5 tx avg/block",
		"* candidate block",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in throughput section:\n%s", want, section)
		}
	}
}

func TestDashboardHealthPrioritizesZeroPeers(t *testing.T) {
	svc := &Service{}
	got := svc.dashboardHealth(ServiceInfo{
		P2PAddr:      "127.0.0.1:18444",
		TipHeight:    7,
		HeaderHeight: 9,
	}, nil)
	if got != "LONELY-BUT-RUNNING" {
		t.Fatalf("dashboard health = %q, want LONELY-BUT-RUNNING", got)
	}
}

func TestDashboardFeeSummaryUsesRecent24Blocks(t *testing.T) {
	var svc Service
	blocks := make([]dashboardBlockPage, 0, 30)
	for i := 0; i < 30; i++ {
		blocks = append(blocks, dashboardBlockPage{
			Height:        uint64(300 - i),
			MedianFeeRate: uint64(i * 100),
			LowFeeRate:    uint64(i * 50),
			HighFeeRate:   uint64(i * 150),
			PaidTxs:       min(i, 2),
			TotalUserTxs:  3,
			TxCount:       2 + (i % 3),
		})
	}

	summary := svc.dashboardFeeSummary(blocks, 20, dashboardPowSummary{
		TargetSpacing:    10 * time.Minute,
		AvgBlockInterval: 10 * time.Minute,
	}, dashboardCandidateFeeLine{
		Height:    301,
		MedianFee: 900,
		LowFee:    100,
		HighFee:   1500,
		PaidTxs:   4,
		TotalTxs:  4,
		Available: true,
	}, []mempool.SnapshotEntry{
		{Fee: 2000, Size: 1_000},
		{Fee: 700, Size: 1_000},
		{Fee: 300, Size: 1_000},
		{Fee: 50, Size: 1_000},
		{Fee: 0, Size: 1_000},
	})

	if summary.TotalBlocks != 24 {
		t.Fatalf("expected 24 fee blocks, got %d", summary.TotalBlocks)
	}
	if summary.CurrentMedian != 900 {
		t.Fatalf("expected current median from candidate block, got %d", summary.CurrentMedian)
	}
	if summary.Median6 != 300 {
		t.Fatalf("expected 6-block median 300, got %d", summary.Median6)
	}
	if summary.Median24 != 1200 {
		t.Fatalf("expected 24-block median 1200, got %d", summary.Median24)
	}
	if summary.RecentMin != 0 || summary.RecentMax != 3450 {
		t.Fatalf("expected recent min/max 0/3450, got %d/%d", summary.RecentMin, summary.RecentMax)
	}
	if summary.PaidBlocks != 23 {
		t.Fatalf("expected 23 paid blocks, got %d", summary.PaidBlocks)
	}
	if summary.FeePayingTxs != 45 || summary.TotalTxs != 72 {
		t.Fatalf("expected fee-paying ratio inputs 45/72, got %d/%d", summary.FeePayingTxs, summary.TotalTxs)
	}
	if summary.Bands.Above1000 != 1 || summary.Bands.Band500 != 1 || summary.Bands.Band100 != 1 || summary.Bands.Band1 != 1 || summary.Bands.Zero != 1 {
		t.Fatalf("unexpected fee bands: %+v", summary.Bands)
	}
	if summary.Recent[0].Height != 277 || summary.Recent[len(summary.Recent)-1].Height != 300 {
		t.Fatalf("expected oldest-to-newest fee window, got first=%d last=%d", summary.Recent[0].Height, summary.Recent[len(summary.Recent)-1].Height)
	}
}

func TestRenderFeeSectionUsesFeeMarketLayout(t *testing.T) {
	recent := make([]dashboardBlockFeeLine, 0, 24)
	for i := 0; i < 24; i++ {
		recent = append(recent, dashboardBlockFeeLine{
			Height:    uint64(230 + i),
			MedianFee: uint64(i * 25),
			LowFee:    uint64(i * 10),
			HighFee:   uint64(i * 40),
		})
	}
	section := renderFeeSection(dashboardFeeSummary{
		Recent:        recent,
		CurrentMedian: 300,
		Median6:       200,
		PaidBlocks:    8,
		TotalBlocks:   24,
	})

	for _, want := range []string{
		"24-block medians",
		"current / 6-block median",
		"300 atoms/kB / 200 atoms/kB",
		"paid blocks",
		"8 / 24",
		"Full fee chart",
		"/fees",
		".",
		"#",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in fee market section:\n%s", want, section)
		}
	}
}

func TestRenderPublicDashboardPagesExposeRecentBlockAndTxLinks(t *testing.T) {
	blockHash := [32]byte{0xaa}
	txID := [32]byte{0xbb}
	generatedAt := time.Unix(1_000, 0).UTC()
	recentFees := make([]dashboardBlockFeeLine, 0, 24)
	for i := 0; i < 24; i++ {
		recentFees = append(recentFees, dashboardBlockFeeLine{
			Height:    uint64(100 + i),
			MedianFee: uint64(i * 10),
			LowFee:    uint64(i * 5),
			HighFee:   uint64(i * 15),
			PaidTxs:   i,
			TotalTxs:  i + 1,
		})
	}
	view := &publicDashboardView{
		generatedAt: generatedAt,
		nodeID:      "NODE1234",
		health:      "HEALTHY",
		info:        ServiceInfo{TipHeight: 12, TipHeaderHash: hex.EncodeToString(blockHash[:]), UTXORoot: strings.Repeat("c", 64)},
		pow: dashboardPowSummary{
			Algorithm:          "ASERT per-block",
			TargetSpacing:      10 * time.Minute,
			AvgBlockInterval:   10 * time.Minute,
			RecentBlockGap:     10 * time.Minute,
			HasObservedGap:     true,
			LastBlockTimestamp: generatedAt.Add(-38 * time.Second),
			NetworkHashrate:    12.4e12,
		},
		fees: dashboardFeeSummary{
			Recent:        recentFees,
			CurrentMedian: 300,
			Median6:       50,
			Median24:      120,
			RecentMin:     0,
			RecentMax:     345,
			PaidBlocks:    23,
			TotalBlocks:   24,
			FeePayingTxs:  100,
			TotalTxs:      150,
			Candidate: dashboardCandidateFeeLine{
				Height:    13,
				MedianFee: 300,
				LowFee:    100,
				HighFee:   600,
				PaidTxs:   14,
				TotalTxs:  14,
				Available: true,
			},
			Bands: dashboardMempoolFeeBands{
				Above1000: 2,
				Band500:   11,
				Band100:   39,
				Band1:     22,
				Zero:      8,
			},
			Clear: dashboardMempoolClearEstimate{Blocks: 1, Time: 10 * time.Minute},
		},
		mempool: dashboardMempoolSummary{Count: 10},
		performance: PerformanceMetrics{
			Gauges: PerformanceGauges{PendingPeerBlocks: 0},
		},
		tpsChart: dashboardTPSChart{
			Label: "THROUGHPUT (BY BLOCK)",
			Blocks: []dashboardThroughputBlock{
				{TxCount: 121, BarWidth: 12},
				{TxCount: 167, BarWidth: 17},
				{TxCount: 38, BarWidth: 4},
				{TxCount: 241, BarWidth: 24},
				{TxCount: 92, BarWidth: 9},
				{TxCount: 184, Candidate: true, BarWidth: 18},
			},
			TotalTx:     843,
			AvgTPS:      0.23,
			AvgTxPerBlk: 140.5,
		},
		blocks: []dashboardBlockPage{{
			Height:    12,
			Hash:      blockHash,
			Timestamp: time.Unix(100, 0).UTC(),
			PreviewTxs: []dashboardTxPage{{
				TxID:      txID,
				BlockHash: blockHash,
				Timestamp: time.Unix(100, 0).UTC(),
			}},
		}},
		peerHosts: []dashboardPeerHostPage{{
			Host:           "198.51.100.42",
			Health:         "MIXED",
			Direction:      "in/out",
			Sockets:        2,
			LastSeenUnix:   time.Now().Add(-time.Second).Unix(),
			BlocksBehind:   0,
			BestHeight:     12,
			TipHash:        "0000000d1b...",
			LatencyAvgS:    0.05,
			LatencyP95S:    0.09,
			TxSent:         5,
			TxRequested:    2,
			BlockSent:      0,
			BlockRequested: 1,
			BytesIn:        184 * 1024,
			BytesOut:       231 * 1024,
			Reason:         "healthy and current",
		}},
	}

	home, status := renderPublicDashboardPage(view, "/")
	if status != 200 || !strings.Contains(home, "/block/"+hex.EncodeToString(blockHash[:])) {
		t.Fatalf("home page missing block link:\n%s", home)
	}
	for _, want := range []string{
		"Health",
		"HEALTHY",
		"Tip Height",
		"12",
		"Last Block",
		"38s ago",
		"Network Hashrate",
		"12.4 TH/s",
		"Peers",
		"Mempool",
		"10 tx",
		"Current Orphan Blocks",
		"Tx Relay Mode",
		"erlay reconciliation",
		"Block Relay Mode",
		"graphene planner",
	} {
		if !strings.Contains(home, want) {
			t.Fatalf("home page missing %q:\n%s", want, home)
		}
	}
	if !strings.Contains(home, "/peer/198.51.100.42") {
		t.Fatalf("home page missing peer link:\n%s", home)
	}
	if !strings.Contains(home, "/fees") || !strings.Contains(home, "FEE MARKET") {
		t.Fatalf("home page missing fee market link:\n%s", home)
	}
	blockPage, status := renderPublicDashboardPage(view, "/block/"+hex.EncodeToString(blockHash[:]))
	if status != 200 || !strings.Contains(blockPage, "/tx/"+hex.EncodeToString(txID[:])) {
		t.Fatalf("block page missing tx link:\n%s", blockPage)
	}
	txPage, status := renderPublicDashboardPage(view, "/tx/"+hex.EncodeToString(txID[:]))
	if status != 200 || !strings.Contains(txPage, "TRANSACTION") {
		t.Fatalf("tx page missing transaction section:\n%s", txPage)
	}
	peerPage, status := renderPublicDashboardPage(view, "/peer/198.51.100.42")
	if status != 200 || !strings.Contains(peerPage, "PEER DETAIL") || !strings.Contains(peerPage, "bytes in") {
		t.Fatalf("peer page missing detail section:\n%s", peerPage)
	}
	feePage, status := renderPublicDashboardPage(view, "/fees")
	if status != 200 || !strings.Contains(feePage, "FEE MARKET DETAILS") || !strings.Contains(feePage, "mempool by fee band") || !strings.Contains(feePage, "candidate block fee") {
		t.Fatalf("fee page missing detail section:\n%s", feePage)
	}
}

func TestSummarizeDashboardPeerHostUsesHostLevelHealthAndTraffic(t *testing.T) {
	now := time.Unix(10_000, 0)
	info := ServiceInfo{TipHeight: 253, TipHeaderHash: strings.Repeat("a", 64)}
	host := summarizeDashboardPeerHost(now, "198.51.100.42", info, []dashboardPeerSocketPage{
		{
			Addr:         "198.51.100.42:18444",
			Outbound:     true,
			Height:       253,
			Lag:          0,
			LastSeenUnix: now.Unix(),
			SessionAge:   102 * time.Minute,
			LatencyAvgS:  0.05,
			LatencyP95S:  0.07,
			TxSent:       5,
			TxRequested:  3,
			BytesIn:      80 * 1024,
			BytesOut:     90 * 1024,
		},
		{
			Addr:           "198.51.100.42:49346",
			Outbound:       false,
			Height:         2,
			Lag:            251,
			LastSeenUnix:   now.Unix() - 1,
			SessionAge:     100 * time.Minute,
			LatencyAvgS:    0.04,
			LatencyP95S:    0.09,
			TxSent:         6,
			TxRequested:    2,
			BlockRequested: 1,
			BytesIn:        104 * 1024,
			BytesOut:       141 * 1024,
		},
	})

	if host.Health != "MIXED" {
		t.Fatalf("health = %q, want MIXED", host.Health)
	}
	if host.Direction != "in/out" {
		t.Fatalf("direction = %q, want in/out", host.Direction)
	}
	if host.BlocksBehind != 0 {
		t.Fatalf("blocks behind = %d, want 0", host.BlocksBehind)
	}
	if host.TxSent != 5 || host.TxRequested != 2 {
		t.Fatalf("traffic summary = sent %d requested %d, want 5/2", host.TxSent, host.TxRequested)
	}
	if host.LatencyAvgS < 0.044 || host.LatencyAvgS > 0.046 {
		t.Fatalf("latency avg = %.3f, want about 0.045", host.LatencyAvgS)
	}
	if host.LatencyP95S != 0.09 {
		t.Fatalf("latency p95 = %.2f, want 0.09", host.LatencyP95S)
	}
	if host.BytesIn != 184*1024 || host.BytesOut != 231*1024 {
		t.Fatalf("bytes summary = %d/%d, want %d/%d", host.BytesIn, host.BytesOut, 184*1024, 231*1024)
	}
}
