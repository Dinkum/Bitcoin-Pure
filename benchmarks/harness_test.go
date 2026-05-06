package benchmarks

import (
	"context"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestRunP2PRelaySmoke(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:     ScenarioP2PRelay,
		Profile:      types.Regtest,
		NodeCount:    2,
		Topology:     TopologyLine,
		TxCount:      32,
		BatchSize:    16,
		Timeout:      15 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.NodeCount != 2 {
		t.Fatalf("node_count = %d, want 2", report.NodeCount)
	}
	if len(report.Nodes) != 2 {
		t.Fatalf("len(nodes) = %d, want 2", len(report.Nodes))
	}
	for _, node := range report.Nodes {
		if node.MempoolCount != 32 {
			t.Fatalf("%s mempool_count = %d, want 32", node.Name, node.MempoolCount)
		}
		if node.CompletionMS == nil {
			t.Fatalf("%s missing completion time", node.Name)
		}
	}
}

func TestRunP2PRelayMeshKeepsExpectedPeerCounts(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:     ScenarioP2PRelay,
		Profile:      types.Regtest,
		NodeCount:    3,
		Topology:     TopologyMesh,
		TxCount:      12,
		BatchSize:    6,
		Timeout:      20 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for _, node := range report.Nodes {
		if node.PeerCount != 2 {
			t.Fatalf("%s peer_count = %d, want 2", node.Name, node.PeerCount)
		}
	}
}

func TestRunP2PRelaySingleOriginTracksSubmittedTxs(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:       ScenarioP2PRelay,
		Profile:        types.Regtest,
		NodeCount:      2,
		Topology:       TopologyLine,
		TxCount:        10,
		BatchSize:      4,
		TxOriginSpread: TxOriginOneNode,
		Timeout:        30 * time.Second,
		SuppressLogs:   true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	got := submittedCountsByNode(report.Nodes)
	want := map[string]int{"node-0": 10, "node-1": 0}
	if len(got) != len(want) {
		t.Fatalf("submitted counts = %v, want %v", got, want)
	}
	for name, wantCount := range want {
		if got[name] != wantCount {
			t.Fatalf("%s submitted_txs = %d, want %d (all counts: %v)", name, got[name], wantCount, got)
		}
	}
}

func TestRunP2PRelayEvenOriginSpreadTracksSubmittedTxs(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:       ScenarioP2PRelay,
		Profile:        types.Regtest,
		NodeCount:      2,
		Topology:       TopologyLine,
		TxCount:        10,
		BatchSize:      4,
		TxOriginSpread: TxOriginEven,
		Timeout:        30 * time.Second,
		SuppressLogs:   true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	got := submittedCountsByNode(report.Nodes)
	want := map[string]int{"node-0": 5, "node-1": 5}
	if len(got) != len(want) {
		t.Fatalf("submitted counts = %v, want %v", got, want)
	}
	for name, wantCount := range want {
		if got[name] != wantCount {
			t.Fatalf("%s submitted_txs = %d, want %d (all counts: %v)", name, got[name], wantCount, got)
		}
	}
}

func TestRunP2PRelayMeshShowsErlayTraffic(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:       ScenarioP2PRelay,
		Profile:        types.Regtest,
		NodeCount:      3,
		Topology:       TopologyMesh,
		TxCount:        48,
		BatchSize:      12,
		TxOriginSpread: TxOriginEven,
		Timeout:        20 * time.Second,
		SuppressLogs:   true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.Metrics.TxReconMessages == 0 {
		t.Fatal("expected erlay tx reconciliation traffic in relay report")
	}
	if report.Metrics.TxReconItems == 0 {
		t.Fatal("expected erlay tx reconciliation items in relay report")
	}
	if report.Metrics.TxBatchMessages == 0 || report.Metrics.TxBatchItems == 0 {
		t.Fatalf("expected full transaction delivery in relay report: batch_msgs=%d batch_items=%d", report.Metrics.TxBatchMessages, report.Metrics.TxBatchItems)
	}
}

func TestRunConfirmedBlocksSmoke(t *testing.T) {
	report, err := RunE2E(context.Background(), RunOptions{
		Profile:       types.Regtest,
		NodeCount:     2,
		Topology:      TopologyLine,
		TxCount:       24,
		BatchSize:     8,
		TxsPerBlock:   8,
		BlockCount:    3,
		BlockInterval: 100 * time.Millisecond,
		MiningMode:    MiningModeSynthetic,
		Timeout:       20 * time.Second,
		SuppressLogs:  true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.ConfirmedProcessingTPS <= 0 {
		t.Fatalf("confirmed_processing_tps = %.2f, want > 0", report.ConfirmedProcessingTPS)
	}
	if report.ConfirmedWallTPS <= 0 {
		t.Fatalf("confirmed_wall_tps = %.2f, want > 0", report.ConfirmedWallTPS)
	}
	if report.Metrics.ConfirmedBlocks != 3 {
		t.Fatalf("confirmed_blocks = %d, want 3", report.Metrics.ConfirmedBlocks)
	}
	if report.Nodes[0].BlockHeight < 4 {
		t.Fatalf("submitter block_height = %d, want at least 4", report.Nodes[0].BlockHeight)
	}
	if report.Metrics.FinalBlockLag > 1 {
		t.Fatalf("final_block_lag = %d, want <= 1", report.Metrics.FinalBlockLag)
	}
	if report.Metrics.FinalMempoolTxs > 8 {
		t.Fatalf("final_mempool_txs = %d, want <= 8", report.Metrics.FinalMempoolTxs)
	}
}

func TestRunConfirmedBlocksMeshKeepsExpectedPeerCounts(t *testing.T) {
	report, err := RunE2E(context.Background(), RunOptions{
		Profile:        types.Regtest,
		NodeCount:      5,
		Topology:       TopologyMesh,
		TxCount:        20,
		BatchSize:      5,
		TxsPerBlock:    5,
		BlockCount:     4,
		BlockInterval:  100 * time.Millisecond,
		MiningMode:     MiningModeSynthetic,
		TxOriginSpread: TxOriginEven,
		Timeout:        30 * time.Second,
		SuppressLogs:   true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for _, node := range report.Nodes {
		if node.PeerCount != 4 {
			t.Fatalf("%s peer_count = %d, want 4", node.Name, node.PeerCount)
		}
	}
}

func TestRunConfirmedBlocksSyntheticMiningReportsFixedCadenceTPS(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:               ScenarioConfirmedBlocks,
		Profile:                types.Regtest,
		NodeCount:              3,
		Topology:               TopologyMesh,
		TxCount:                12,
		BatchSize:              4,
		TxsPerBlock:            4,
		BlockCount:             3,
		BlockInterval:          100 * time.Millisecond,
		TxOriginSpread:         TxOriginEven,
		Timeout:                10 * time.Second,
		SuppressLogs:           true,
		SyntheticMining:        true,
		SyntheticBlockInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := float64(12) / (3 * 100 * time.Millisecond).Seconds()
	if diff := math.Abs(report.SyntheticIntervalTPS - want); diff > want*0.001 {
		t.Fatalf("synthetic_interval_tps = %.6f, want %.6f", report.SyntheticIntervalTPS, want)
	}
	if !report.SyntheticMining {
		t.Fatal("expected synthetic mining flag in report")
	}
}

func TestRunConfirmedBlocksSteadyStateBacklogStartsCadenceAfterConvergence(t *testing.T) {
	report, err := RunE2E(context.Background(), RunOptions{
		Profile:            types.Regtest,
		NodeCount:          2,
		Topology:           TopologyLine,
		TxCount:            16,
		BatchSize:          4,
		TxsPerBlock:        8,
		BlockCount:         2,
		BlockInterval:      100 * time.Millisecond,
		MiningMode:         MiningModeSynthetic,
		TxOriginSpread:     TxOriginOneNode,
		SteadyStateBacklog: true,
		Timeout:            20 * time.Second,
		SuppressLogs:       true,
	})
	if err != nil {
		t.Fatalf("RunE2E() error = %v", err)
	}
	if !report.SteadyStateBacklog {
		t.Fatal("expected steady-state backlog flag in report")
	}
	if report.Metrics.AcceptedTxs != 16 {
		t.Fatalf("accepted_txs = %d, want 16", report.Metrics.AcceptedTxs)
	}
	if report.Metrics.ConfirmedTxs != 16 {
		t.Fatalf("confirmed_txs = %d, want 16", report.Metrics.ConfirmedTxs)
	}
	if report.Metrics.ConfirmedBlocks != 2 {
		t.Fatalf("confirmed_blocks = %d, want 2", report.Metrics.ConfirmedBlocks)
	}
	if report.ConfirmedWallDurationMS < 200 {
		t.Fatalf("confirmed_wall_duration_ms = %.2f, want at least two 100ms intervals", report.ConfirmedWallDurationMS)
	}
	if report.Metrics.FinalMempoolTxs != 0 {
		t.Fatalf("final_mempool_txs = %d, want 0", report.Metrics.FinalMempoolTxs)
	}
}

func TestRunConfirmedBlocksSyntheticMiningFindsMaxWithoutRestartingCluster(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:               ScenarioConfirmedBlocks,
		Profile:                types.Regtest,
		NodeCount:              2,
		Topology:               TopologyLine,
		TxCount:                32,
		BatchSize:              8,
		TxsPerBlock:            8,
		TxOriginSpread:         TxOriginOneNode,
		Timeout:                20 * time.Second,
		SuppressLogs:           true,
		SyntheticMining:        true,
		SyntheticBlockInterval: 250 * time.Millisecond,
		FindMaxTPS:             true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.Metrics.RampSteps == 0 {
		t.Fatal("expected at least one ramp step")
	}
	if report.Metrics.MaxSustainableTxsPerBlock == 0 && report.Metrics.FirstUnsustainedTxsPerBlock == 0 {
		t.Fatal("expected either a sustainable or unsustained synthetic target")
	}
	expectedTarget := report.Metrics.MaxSustainableTxsPerBlock
	if expectedTarget == 0 {
		expectedTarget = report.Metrics.FirstUnsustainedTxsPerBlock
	}
	if report.TxsPerBlock != expectedTarget {
		t.Fatalf("report txs_per_block = %d, want %d", report.TxsPerBlock, expectedTarget)
	}
	want := float64(report.Metrics.MaxSustainableTxsPerBlock) / (250 * time.Millisecond).Seconds()
	if report.Metrics.MaxSustainableTxsPerBlock == 0 {
		want = float64(report.Metrics.ConfirmedTxs) / (float64(report.Metrics.ConfirmedBlocks) * (250 * time.Millisecond).Seconds())
	}
	if diff := math.Abs(report.SyntheticIntervalTPS - want); diff > max(0.001, want*0.001) {
		t.Fatalf("synthetic_interval_tps = %.6f, want %.6f", report.SyntheticIntervalTPS, want)
	}
}

func TestRunConfirmedBlocksTimeoutReturnsError(t *testing.T) {
	_, err := Run(context.Background(), RunOptions{
		Scenario:     ScenarioConfirmedBlocks,
		Profile:      types.RegtestMedium,
		NodeCount:    3,
		Topology:     TopologyMesh,
		TxCount:      512,
		BatchSize:    64,
		TxsPerBlock:  128,
		Timeout:      time.Second,
		SuppressLogs: true,
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("timeout error = %v, want context deadline exceeded", err)
	}
}

func TestFundingBatchCounts(t *testing.T) {
	tests := []struct {
		name     string
		total    int
		batch    int
		expected []int
	}{
		{name: "empty", total: 0, batch: 8, expected: nil},
		{name: "single batch", total: 8, batch: 8, expected: []int{8}},
		{name: "multiple full batches", total: 24, batch: 8, expected: []int{8, 8, 8}},
		{name: "trailing partial batch", total: 25, batch: 8, expected: []int{8, 8, 8, 1}},
		{name: "invalid batch size", total: 25, batch: 0, expected: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fundingBatchCounts(tt.total, tt.batch)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Fatalf("fundingBatchCounts(%d, %d) = %v, want %v", tt.total, tt.batch, got, tt.expected)
			}
		})
	}
}

func TestRunDirectSubmitCapturesProfiles(t *testing.T) {
	profileDir := t.TempDir()
	report, err := Run(context.Background(), RunOptions{
		Scenario:     ScenarioDirectSubmit,
		Profile:      types.Regtest,
		NodeCount:    1,
		TxCount:      8,
		BatchSize:    4,
		Timeout:      15 * time.Second,
		ProfileDir:   profileDir,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(report.Profiling.Artifacts) != 4 {
		t.Fatalf("artifact count = %d, want 4", len(report.Profiling.Artifacts))
	}
	for _, artifact := range report.Profiling.Artifacts {
		if _, err := os.Stat(artifact.Path); err != nil {
			t.Fatalf("missing profile artifact %s: %v", artifact.Path, err)
		}
	}
}

func TestRunDirectSubmitSmoke(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:     ScenarioDirectSubmit,
		Profile:      types.Regtest,
		NodeCount:    1,
		TxCount:      16,
		BatchSize:    8,
		Timeout:      15 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(report.Nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1", len(report.Nodes))
	}
	if report.Nodes[0].MempoolCount != 16 {
		t.Fatalf("mempool_count = %d, want 16", report.Nodes[0].MempoolCount)
	}
	expectedAdmissionTPS := float64(report.TxCount) / (report.Phases.ValidateAdmitMS / 1000)
	if diff := math.Abs(report.AdmissionTPS - expectedAdmissionTPS); diff > expectedAdmissionTPS*0.02 {
		t.Fatalf("admission_tps = %.2f, want close to %.2f based on validate/admit window", report.AdmissionTPS, expectedAdmissionTPS)
	}
}

func TestRunOrphanStormSmoke(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:     ScenarioOrphanStorm,
		Profile:      types.Regtest,
		NodeCount:    1,
		TxCount:      512,
		BatchSize:    64,
		Timeout:      30 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(report.Nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1", len(report.Nodes))
	}
	if report.Nodes[0].MempoolCount != 512 {
		t.Fatalf("mempool_count = %d, want 512", report.Nodes[0].MempoolCount)
	}
	if report.Metrics.OrphanedTxs == 0 || report.Metrics.PromotedOrphans == 0 {
		t.Fatalf("expected orphan metrics in report: %+v", report.Metrics)
	}
}

func TestRenderMarkdownIncludesSummary(t *testing.T) {
	propagation := 12.5
	text := RenderMarkdown(&Report{
		Benchmark:            string(BenchmarkThroughput),
		Mode:                 string(ThroughputModeTx),
		Scenario:             string(ScenarioDirectSubmit),
		Profile:              "regtest",
		NodeCount:            1,
		TxCount:              64,
		BatchSize:            32,
		RequestedDurationMS:  60_000,
		AdmissionDurationMS:  10,
		CompletionDurationMS: 20,
		AdmissionTPS:         6400,
		CompletionTPS:        3200,
		Phases: PhaseReport{
			DecodeMS:        1.5,
			ValidateAdmitMS: 10,
			RelayFanoutMS:   0,
			ConvergenceMS:   20,
		},
		Nodes: []NodeReport{{
			Name:                "node-0",
			PeerCount:           0,
			SubmittedTxs:        64,
			BlockHeight:         1,
			HeaderHeight:        1,
			MempoolCount:        64,
			BlockSigChecks:      64,
			BlockSigFallbacks:   1,
			BlockSigVerifyAvgMS: 1.25,
			CompletionMS:        &propagation,
		}},
	})
	if !strings.Contains(text, "# BPU Throughput Benchmark") {
		t.Fatalf("markdown missing title: %q", text)
	}
	if !strings.Contains(text, "`throughput`") {
		t.Fatalf("markdown missing benchmark: %q", text)
	}
	if !strings.Contains(text, "node-0") {
		t.Fatalf("markdown missing node row: %q", text)
	}
	if !strings.Contains(text, "| Name | Peers | Submitted |") {
		t.Fatalf("markdown missing submitted column: %q", text)
	}
	if !strings.Contains(text, "## Timings") {
		t.Fatalf("markdown missing timings section: %q", text)
	}
	if !strings.Contains(text, "Block Signature Verification") {
		t.Fatalf("markdown missing signature section: %q", text)
	}
}

func TestRunSuiteSmoke(t *testing.T) {
	report, err := RunSuite(context.Background(), SuiteOptions{
		Profile:       types.Regtest,
		NodeCount:     3,
		TxCount:       16,
		BatchSize:     8,
		TxsPerBlock:   8,
		BlockCount:    2,
		BlockInterval: 100 * time.Millisecond,
		Duration:      time.Second,
		Timeout:       20 * time.Second,
		SuppressLogs:  true,
	})
	if err != nil {
		t.Fatalf("RunSuite() error = %v", err)
	}
	if len(report.Cases) != 4 {
		t.Fatalf("len(cases) = %d, want 4", len(report.Cases))
	}
	if !strings.Contains(RenderSuiteMarkdown(report), "BPU Benchmark Suite") {
		t.Fatalf("suite markdown missing title")
	}
	if !strings.Contains(RenderSuiteASCIISummary(report), "BPU Benchmark Suite") {
		t.Fatalf("suite ascii summary missing title")
	}
}

func TestRunE2EUsesBenchmarkMetadata(t *testing.T) {
	report, err := RunE2E(context.Background(), RunOptions{
		Profile:       types.Regtest,
		NodeCount:     2,
		Topology:      TopologyLine,
		BatchSize:     4,
		TxsPerBlock:   4,
		BlockCount:    2,
		BlockInterval: 100 * time.Millisecond,
		MiningMode:    MiningModeSynthetic,
		Timeout:       20 * time.Second,
		SuppressLogs:  true,
	})
	if err != nil {
		t.Fatalf("RunE2E() error = %v", err)
	}
	if report.Benchmark != string(BenchmarkE2E) {
		t.Fatalf("benchmark = %q, want %q", report.Benchmark, BenchmarkE2E)
	}
	if report.Mining != string(MiningModeSynthetic) {
		t.Fatalf("mining = %q, want %q", report.Mining, MiningModeSynthetic)
	}
	if report.TargetBlocks != 2 {
		t.Fatalf("target_blocks = %d, want 2", report.TargetBlocks)
	}
	if report.Profile != string(types.BenchNet) {
		t.Fatalf("profile = %q, want %q", report.Profile, types.BenchNet)
	}
}

func TestBenchNetDifficultyPresetForInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		expected uint32
	}{
		{name: "short cadence", interval: 2 * time.Second, expected: consensus.RegtestParams().GenesisBits},
		{name: "medium cadence", interval: 20 * time.Second, expected: consensus.RegtestMediumParams().GenesisBits},
		{name: "long cadence", interval: 90 * time.Second, expected: consensus.RegtestHardParams().GenesisBits},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := benchNetDifficultyPreset(tt.interval).GenesisBits; got != tt.expected {
				t.Fatalf("benchNetDifficultyPreset(%s) = 0x%08x, want 0x%08x", tt.interval, got, tt.expected)
			}
		})
	}
}

func TestRunThroughputUsesModeMetadata(t *testing.T) {
	report, err := RunThroughput(context.Background(), RunOptions{
		Profile:        types.Regtest,
		ThroughputMode: ThroughputModeTx,
		TxCount:        16,
		Duration:       time.Second,
		Timeout:        10 * time.Second,
		SuppressLogs:   true,
	})
	if err != nil {
		t.Fatalf("RunThroughput() error = %v", err)
	}
	if report.Benchmark != string(BenchmarkThroughput) {
		t.Fatalf("benchmark = %q, want %q", report.Benchmark, BenchmarkThroughput)
	}
	if report.Mode != string(ThroughputModeTx) {
		t.Fatalf("mode = %q, want %q", report.Mode, ThroughputModeTx)
	}
}

func submittedCountsByNode(nodes []NodeReport) map[string]int {
	out := make(map[string]int, len(nodes))
	for _, node := range nodes {
		out[node.Name] = node.SubmittedTxs
	}
	return out
}
