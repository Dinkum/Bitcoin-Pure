package benchmarks

import (
	"context"
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
		if node.FullPropagationMS == nil {
			t.Fatalf("%s missing propagation time", node.Name)
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
		Scenario:              string(ScenarioP2PRelay),
		Profile:               "regtest",
		NodeCount:             2,
		Topology:              string(TopologyMesh),
		TxCount:               64,
		BatchSize:             32,
		SubmissionDurationMS:  10,
		ConvergenceDurationMS: 20,
		SubmitTPS:             6400,
		EndToEndTPS:           3200,
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
			Name:              "node-0",
			PeerCount:         1,
			BlockHeight:       2,
			HeaderHeight:      2,
			MempoolCount:      64,
			FullPropagationMS: &propagation,
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
	if !strings.Contains(text, "Phase Timings") {
		t.Fatalf("markdown missing phases: %q", text)
	}
}

func TestRunSuiteSmoke(t *testing.T) {
	report, err := RunSuite(context.Background(), SuiteOptions{
		Profile:      types.Regtest,
		NodeCount:    3,
		TxCount:      16,
		BatchSize:    8,
		Timeout:      40 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		t.Fatalf("RunSuite() error = %v", err)
	}
	if len(report.Cases) != 9 {
		t.Fatalf("len(cases) = %d, want 9", len(report.Cases))
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
