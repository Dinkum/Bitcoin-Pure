package benchmarks

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

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

func TestRunConfirmedBlocksSmoke(t *testing.T) {
	report, err := Run(context.Background(), RunOptions{
		Scenario:     ScenarioConfirmedBlocks,
		Profile:      types.Regtest,
		NodeCount:    2,
		Topology:     TopologyLine,
		TxCount:      24,
		BatchSize:    8,
		TxsPerBlock:  8,
		Timeout:      20 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.ConfirmedTPS <= 0 {
		t.Fatalf("confirmed_tps = %.2f, want > 0", report.ConfirmedTPS)
	}
	if report.Metrics.ConfirmedBlocks != 3 {
		t.Fatalf("confirmed_blocks = %d, want 3", report.Metrics.ConfirmedBlocks)
	}
	for _, node := range report.Nodes {
		if node.BlockHeight < 4 {
			t.Fatalf("%s block_height = %d, want at least 4", node.Name, node.BlockHeight)
		}
		if node.MempoolCount != 0 {
			t.Fatalf("%s mempool_count = %d, want 0", node.Name, node.MempoolCount)
		}
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
		Scenario:             string(ScenarioP2PRelay),
		Profile:              "regtest",
		NodeCount:            2,
		Topology:             string(TopologyMesh),
		TxCount:              64,
		BatchSize:            32,
		TxsPerBlock:          16,
		TxOriginSpread:       string(TxOriginEven),
		AdmissionDurationMS:  10,
		CompletionDurationMS: 20,
		AdmissionTPS:         6400,
		CompletionTPS:        3200,
		ConfirmedTPS:         1600,
		Phases: PhaseReport{
			DecodeMS:        1.5,
			ValidateAdmitMS: 10,
			RelayFanoutMS:   18,
			ConvergenceMS:   20,
		},
		Metrics: ScenarioMetrics{
			DirectRelayPeerCount: 1,
		},
		Nodes: []NodeReport{{
			Name:                "node-0",
			PeerCount:           1,
			SubmittedTxs:        32,
			BlockHeight:         2,
			HeaderHeight:        2,
			MempoolCount:        64,
			BlockSigChecks:      64,
			BlockSigFallbacks:   1,
			BlockSigVerifyAvgMS: 1.25,
			CompletionMS:        &propagation,
		}},
	})
	if !strings.Contains(text, "# Benchmark Report") {
		t.Fatalf("markdown missing title: %q", text)
	}
	if !strings.Contains(text, "`p2p-relay`") {
		t.Fatalf("markdown missing scenario: %q", text)
	}
	if !strings.Contains(text, "node-0") {
		t.Fatalf("markdown missing node row: %q", text)
	}
	if !strings.Contains(text, "`even`") {
		t.Fatalf("markdown missing tx origin: %q", text)
	}
	if !strings.Contains(text, "| Name | Peers | Submitted |") {
		t.Fatalf("markdown missing submitted column: %q", text)
	}
	if !strings.Contains(text, "Phase Timings") {
		t.Fatalf("markdown missing phases: %q", text)
	}
	if !strings.Contains(text, "Block Signature Verification") {
		t.Fatalf("markdown missing signature section: %q", text)
	}
}

func TestRunSuiteSmoke(t *testing.T) {
	report, err := RunSuite(context.Background(), SuiteOptions{
		Profile:      types.Regtest,
		NodeCount:    3,
		TxCount:      16,
		BatchSize:    8,
		TxsPerBlock:  8,
		Timeout:      40 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("RunSuite() error = %v", err)
	}
	if len(report.Cases) != 10 {
		t.Fatalf("len(cases) = %d, want 10", len(report.Cases))
	}
	if !strings.Contains(RenderSuiteMarkdown(report), "Benchmark Suite Report") {
		t.Fatalf("suite markdown missing title")
	}
	if !strings.Contains(RenderSuiteASCIISummary(report), "BPU Benchmark Suite") {
		t.Fatalf("suite ascii summary missing title")
	}
}

func TestAvailableScenariosIncludesExpandedCoverage(t *testing.T) {
	got := AvailableScenarios()
	want := []Scenario{
		ScenarioDirectSubmit,
		ScenarioRPCBatch,
		ScenarioP2PRelay,
		ScenarioConfirmedBlocks,
		ScenarioUserMix,
		ScenarioChainedPackages,
		ScenarioOrphanStorm,
		ScenarioBlockTemplateBuild,
	}
	for _, scenario := range want {
		if !containsScenario(got, scenario) {
			t.Fatalf("missing scenario %q in %v", scenario, got)
		}
	}
}

func containsScenario(list []Scenario, want Scenario) bool {
	for _, scenario := range list {
		if scenario == want {
			return true
		}
	}
	return false
}

func submittedCountsByNode(nodes []NodeReport) map[string]int {
	out := make(map[string]int, len(nodes))
	for _, node := range nodes {
		out[node.Name] = node.SubmittedTxs
	}
	return out
}
