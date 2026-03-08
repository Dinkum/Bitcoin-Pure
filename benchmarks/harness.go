package benchmarks

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
)

type Scenario string

const (
	ScenarioDirectSubmit       Scenario = "direct-submit"
	ScenarioRPCBatch           Scenario = "rpc-batch"
	ScenarioP2PRelay           Scenario = "p2p-relay"
	ScenarioUserMix            Scenario = "user-mix"
	ScenarioChainedPackages    Scenario = "chained-packages"
	ScenarioOrphanStorm        Scenario = "orphan-storm"
	ScenarioBlockTemplateBuild Scenario = "block-template-rebuild"
)

type Topology string

const (
	TopologyLine Topology = "line"
	TopologyMesh Topology = "mesh"
)

type RunOptions struct {
	Scenario     Scenario
	Profile      types.ChainProfile
	NodeCount    int
	Topology     Topology
	TxCount      int
	BatchSize    int
	Timeout      time.Duration
	DBRoot       string
	SuppressLogs bool
}

type SuiteOptions struct {
	Profile      types.ChainProfile
	NodeCount    int
	TxCount      int
	BatchSize    int
	Timeout      time.Duration
	DBRoot       string
	SuppressLogs bool
}

type Report struct {
	Scenario              string          `json:"scenario"`
	Profile               string          `json:"profile"`
	NodeCount             int             `json:"node_count"`
	Topology              string          `json:"topology,omitempty"`
	TxCount               int             `json:"tx_count"`
	BatchSize             int             `json:"batch_size,omitempty"`
	StartedAt             time.Time       `json:"started_at"`
	CompletedAt           time.Time       `json:"completed_at"`
	SubmissionDurationMS  float64         `json:"submission_duration_ms"`
	ConvergenceDurationMS float64         `json:"convergence_duration_ms"`
	SubmitTPS             float64         `json:"submit_tps"`
	EndToEndTPS           float64         `json:"end_to_end_tps"`
	Phases                PhaseReport     `json:"phases"`
	Metrics               ScenarioMetrics `json:"metrics,omitempty"`
	Environment           Environment     `json:"environment"`
	Nodes                 []NodeReport    `json:"nodes"`
	Notes                 []string        `json:"notes,omitempty"`
}

type SuiteReport struct {
	Profile     string            `json:"profile"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at"`
	Environment Environment       `json:"environment"`
	Cases       []SuiteCaseReport `json:"cases"`
	Notes       []string          `json:"notes,omitempty"`
}

type SuiteCaseReport struct {
	Name   string `json:"name"`
	Report Report `json:"report"`
}

type Environment struct {
	GoOS      string `json:"go_os"`
	GoArch    string `json:"go_arch"`
	GoVersion string `json:"go_version"`
	NumCPU    int    `json:"num_cpu"`
}

type NodeReport struct {
	Name              string            `json:"name"`
	RPCAddr           string            `json:"rpc_addr,omitempty"`
	P2PAddr           string            `json:"p2p_addr,omitempty"`
	PeerCount         int               `json:"peer_count"`
	BlockHeight       uint64            `json:"block_height"`
	HeaderHeight      uint64            `json:"header_height"`
	MempoolCount      int               `json:"mempool_count"`
	FullPropagationMS *float64          `json:"full_propagation_ms,omitempty"`
	RelayPeers        []PeerRelayReport `json:"relay_peers,omitempty"`
}

type PhaseReport struct {
	DecodeMS        float64 `json:"decode_ms"`
	ValidateAdmitMS float64 `json:"validate_admit_ms"`
	RelayFanoutMS   float64 `json:"relay_fanout_ms"`
	ConvergenceMS   float64 `json:"convergence_ms"`
}

type ScenarioMetrics struct {
	AcceptedTxs          int     `json:"accepted_txs,omitempty"`
	OrphanedTxs          int     `json:"orphaned_txs,omitempty"`
	PromotedOrphans      int     `json:"promoted_orphans,omitempty"`
	MaxOrphanCount       int     `json:"max_orphan_count,omitempty"`
	PackageDepth         int     `json:"package_depth,omitempty"`
	PackageCount         int     `json:"package_count,omitempty"`
	TemplateRebuilds     int     `json:"template_rebuilds,omitempty"`
	TemplateCacheHits    int     `json:"template_cache_hits,omitempty"`
	TemplateFrontier     int     `json:"template_frontier_candidates,omitempty"`
	TemplateRebuildAvgMS float64 `json:"template_rebuild_avg_ms,omitempty"`
	TemplateRebuildP95MS float64 `json:"template_rebuild_p95_ms,omitempty"`
	TemplateRebuildMaxMS float64 `json:"template_rebuild_max_ms,omitempty"`
	TemplateSelectedTxs  int     `json:"template_selected_txs,omitempty"`
	DirectRelayPeerCount int     `json:"direct_relay_peer_count,omitempty"`
	TxBatchMessages      int     `json:"tx_batch_messages,omitempty"`
	TxBatchItems         int     `json:"tx_batch_items,omitempty"`
	TxReconMessages      int     `json:"tx_recon_messages,omitempty"`
	TxReconItems         int     `json:"tx_recon_items,omitempty"`
	TxRequestMessages    int     `json:"tx_request_messages,omitempty"`
	TxRequestItems       int     `json:"tx_request_items,omitempty"`
	UserCount            int     `json:"user_count,omitempty"`
	ShortChainTxs        int     `json:"short_chain_txs,omitempty"`
	MultiOutputTxs       int     `json:"multi_output_txs,omitempty"`
}

type PeerRelayReport struct {
	Addr          string  `json:"addr"`
	Outbound      bool    `json:"outbound"`
	QueueDepth    int     `json:"queue_depth"`
	MaxQueueDepth int     `json:"max_queue_depth"`
	SentMessages  int     `json:"sent_messages"`
	TxInvItems    int     `json:"tx_inv_items"`
	BlockInvItems int     `json:"block_inv_items"`
	TxBatchMsgs   int     `json:"tx_batch_messages,omitempty"`
	TxBatchItems  int     `json:"tx_batch_items,omitempty"`
	TxReconMsgs   int     `json:"tx_recon_messages,omitempty"`
	TxReconItems  int     `json:"tx_recon_items,omitempty"`
	TxReqMsgs     int     `json:"tx_request_messages,omitempty"`
	TxReqItems    int     `json:"tx_request_items,omitempty"`
	DroppedTxs    int     `json:"dropped_tx_items,omitempty"`
	RelayEvents   int     `json:"relay_events"`
	RelayAvgMS    float64 `json:"relay_avg_ms,omitempty"`
	RelayP95MS    float64 `json:"relay_p95_ms,omitempty"`
	RelayMaxMS    float64 `json:"relay_max_ms,omitempty"`
}

type rpcRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params,omitempty"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error"`
}

type clusterNode struct {
	name      string
	svc       *node.Service
	rpcAddr   string
	authToken string
	p2pAddr   string
	cancel    context.CancelFunc
	errCh     chan error
}

type mempoolBenchConfig struct {
	maxTxSize      int
	maxAncestors   int
	maxDescendants int
	maxOrphans     int
}

type workloadOutcome struct {
	submitDone  time.Time
	completed   time.Time
	propagation map[string]time.Duration
	phases      PhaseReport
	metrics     ScenarioMetrics
	extraNotes  []string
}

type submissionOutcome struct {
	submitDone      time.Time
	decodeMS        float64
	validateAdmitMS float64
	metrics         ScenarioMetrics
}

func DefaultRunOptions() RunOptions {
	return RunOptions{
		Scenario:     ScenarioP2PRelay,
		Profile:      types.Regtest,
		NodeCount:    5,
		Topology:     TopologyMesh,
		TxCount:      4_096,
		BatchSize:    64,
		Timeout:      30 * time.Second,
		SuppressLogs: true,
	}
}

func AvailableScenarios() []Scenario {
	return []Scenario{
		ScenarioDirectSubmit,
		ScenarioRPCBatch,
		ScenarioP2PRelay,
		ScenarioUserMix,
		ScenarioChainedPackages,
		ScenarioOrphanStorm,
		ScenarioBlockTemplateBuild,
	}
}

func DefaultSuiteOptions() SuiteOptions {
	defaults := DefaultRunOptions()
	return SuiteOptions{
		Profile:      defaults.Profile,
		NodeCount:    defaults.NodeCount,
		TxCount:      defaults.TxCount,
		BatchSize:    defaults.BatchSize,
		Timeout:      defaults.Timeout,
		SuppressLogs: defaults.SuppressLogs,
	}
}

func Run(ctx context.Context, opts RunOptions) (*Report, error) {
	opts = withDefaults(opts)
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	restoreLogs := func() {}
	if opts.SuppressLogs {
		restoreLogs = suppressLogs()
	}
	defer restoreLogs()

	runCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	cluster, cleanup, err := openCluster(runCtx, opts)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if err := connectCluster(runCtx, cluster, opts.Topology); err != nil {
		return nil, err
	}
	started := time.Now()
	outcome, err := executeScenario(runCtx, cluster, opts, started)
	if err != nil {
		return nil, err
	}

	report := &Report{
		Scenario:              string(opts.Scenario),
		Profile:               opts.Profile.String(),
		NodeCount:             opts.NodeCount,
		Topology:              reportTopology(opts),
		TxCount:               opts.TxCount,
		BatchSize:             opts.BatchSize,
		StartedAt:             started,
		CompletedAt:           outcome.completed,
		SubmissionDurationMS:  durationMS(outcome.submitDone.Sub(started)),
		ConvergenceDurationMS: outcome.phases.ConvergenceMS,
		SubmitTPS:             ratePerSecond(opts.TxCount, outcome.submitDone.Sub(started)),
		EndToEndTPS:           ratePerSecond(opts.TxCount, outcome.completed.Sub(started)),
		Phases:                outcome.phases,
		Metrics:               outcome.metrics,
		Environment: Environment{
			GoOS:      runtime.GOOS,
			GoArch:    runtime.GOARCH,
			GoVersion: runtime.Version(),
			NumCPU:    runtime.NumCPU(),
		},
		Nodes: buildNodeReports(cluster, outcome.propagation),
		Notes: buildNotes(opts, outcome.extraNotes),
	}
	report.Metrics.TxBatchMessages, report.Metrics.TxBatchItems, report.Metrics.TxReconMessages, report.Metrics.TxReconItems, report.Metrics.TxRequestMessages, report.Metrics.TxRequestItems = aggregateRelayMetrics(report.Nodes)
	return report, nil
}

func RunSuite(ctx context.Context, opts SuiteOptions) (*SuiteReport, error) {
	opts = withSuiteDefaults(opts)
	cases := suiteCases(opts)
	started := time.Now()
	suite := &SuiteReport{
		Profile:   opts.Profile.String(),
		StartedAt: started,
		Environment: Environment{
			GoOS:      runtime.GOOS,
			GoArch:    runtime.GOARCH,
			GoVersion: runtime.Version(),
			NumCPU:    runtime.NumCPU(),
		},
		Notes: []string{
			"Suite coverage mixes direct in-process submit, authenticated RPC batch submit, and real multi-node loopback relay.",
			"Loopback cluster cases show BPU stack throughput with real node-to-node propagation but without WAN latency.",
		},
	}
	for _, suiteCase := range cases {
		report, err := Run(ctx, suiteCase.Options)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", suiteCase.Name, err)
		}
		suite.Cases = append(suite.Cases, SuiteCaseReport{
			Name:   suiteCase.Name,
			Report: *report,
		})
	}
	suite.CompletedAt = time.Now()
	return suite, nil
}

func RenderMarkdown(report *Report) string {
	var b strings.Builder
	b.WriteString("# Benchmark Report\n\n")
	b.WriteString(fmt.Sprintf("- Scenario: `%s`\n", report.Scenario))
	b.WriteString(fmt.Sprintf("- Profile: `%s`\n", report.Profile))
	b.WriteString(fmt.Sprintf("- Nodes: `%d`\n", report.NodeCount))
	if report.Topology != "" {
		b.WriteString(fmt.Sprintf("- Topology: `%s`\n", report.Topology))
	}
	b.WriteString(fmt.Sprintf("- Transactions: `%d`\n", report.TxCount))
	if report.BatchSize > 0 {
		b.WriteString(fmt.Sprintf("- Batch size: `%d`\n", report.BatchSize))
	}
	b.WriteString(fmt.Sprintf("- Submit duration: `%.2f ms`\n", report.SubmissionDurationMS))
	b.WriteString(fmt.Sprintf("- Full convergence: `%.2f ms`\n", report.ConvergenceDurationMS))
	b.WriteString(fmt.Sprintf("- Submit TPS: `%.2f`\n", report.SubmitTPS))
	b.WriteString(fmt.Sprintf("- End-to-end TPS: `%.2f`\n", report.EndToEndTPS))
	b.WriteString("\n## Phase Timings\n\n")
	b.WriteString("| Decode | Validate / Admit | Relay Fanout | Convergence |\n")
	b.WriteString("| ---: | ---: | ---: | ---: |\n")
	b.WriteString(fmt.Sprintf("| %.2f ms | %.2f ms | %.2f ms | %.2f ms |\n",
		report.Phases.DecodeMS,
		report.Phases.ValidateAdmitMS,
		report.Phases.RelayFanoutMS,
		report.Phases.ConvergenceMS,
	))
	if rows := scenarioMetricRows(report.Metrics); len(rows) != 0 {
		b.WriteString("\n## Scenario Metrics\n\n")
		b.WriteString("| Metric | Value |\n")
		b.WriteString("| --- | ---: |\n")
		for _, row := range rows {
			b.WriteString(fmt.Sprintf("| %s | %s |\n", row[0], row[1]))
		}
	}
	b.WriteString("\n## Nodes\n\n")
	b.WriteString("| Name | Peers | Block Height | Header Height | Mempool | Full Propagation |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, node := range report.Nodes {
		propagation := "-"
		if node.FullPropagationMS != nil {
			propagation = fmt.Sprintf("%.2f ms", *node.FullPropagationMS)
		}
		b.WriteString(fmt.Sprintf("| %s | %d | %d | %d | %d | %s |\n",
			node.Name, node.PeerCount, node.BlockHeight, node.HeaderHeight, node.MempoolCount, propagation))
	}
	if peers := flattenRelayPeers(report.Nodes); len(peers) != 0 {
		b.WriteString("\n## Relay Peers\n\n")
		b.WriteString("| Node | Peer | Dir | Sent | Tx Inv | Max Queue | Relay Avg | Relay P95 | Relay Max |\n")
		b.WriteString("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, peer := range peers {
			b.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %d | %.2f ms | %.2f ms | %.2f ms |\n",
				peer.NodeName,
				peer.Addr,
				map[bool]string{true: "out", false: "in"}[peer.Outbound],
				peer.SentMessages,
				peer.TxInvItems,
				peer.MaxQueueDepth,
				peer.RelayAvgMS,
				peer.RelayP95MS,
				peer.RelayMaxMS,
			))
		}
	}
	if len(report.Notes) != 0 {
		b.WriteString("\n## Notes\n\n")
		for _, note := range report.Notes {
			b.WriteString("- " + note + "\n")
		}
	}
	return b.String()
}

func RenderSuiteMarkdown(report *SuiteReport) string {
	var b strings.Builder
	b.WriteString("# Benchmark Suite Report\n\n")
	b.WriteString(fmt.Sprintf("- Profile: `%s`\n", report.Profile))
	b.WriteString(fmt.Sprintf("- Cases: `%d`\n", len(report.Cases)))
	b.WriteString(fmt.Sprintf("- Started: `%s`\n", report.StartedAt.UTC().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("- Completed: `%s`\n", report.CompletedAt.UTC().Format(time.RFC3339)))
	b.WriteString("\n## Summary\n\n")
	b.WriteString("| Case | Scenario | Nodes | Topology | Tx Count | Submit TPS | End-to-End TPS | Validate / Admit | Relay Fanout | Convergence |\n")
	b.WriteString("| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, suiteCase := range report.Cases {
		topology := suiteCase.Report.Topology
		if topology == "" {
			topology = "-"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %d | %s | %d | %.2f | %.2f | %.2f ms | %.2f ms | %.2f ms |\n",
			suiteCase.Name,
			suiteCase.Report.Scenario,
			suiteCase.Report.NodeCount,
			topology,
			suiteCase.Report.TxCount,
			suiteCase.Report.SubmitTPS,
			suiteCase.Report.EndToEndTPS,
			suiteCase.Report.Phases.ValidateAdmitMS,
			suiteCase.Report.Phases.RelayFanoutMS,
			suiteCase.Report.Phases.ConvergenceMS,
		))
	}
	if len(report.Notes) != 0 {
		b.WriteString("\n## Notes\n\n")
		for _, note := range report.Notes {
			b.WriteString("- " + note + "\n")
		}
	}
	return b.String()
}

func RenderASCIISummary(report *Report) string {
	lines := []string{
		fmt.Sprintf("Scenario    %s", report.Scenario),
		fmt.Sprintf("Profile     %s", report.Profile),
		fmt.Sprintf("Layout      %d nodes%s", report.NodeCount, asciiTopologySuffix(report.Topology)),
		fmt.Sprintf("Workload    %d txs%s", report.TxCount, asciiBatchSuffix(report.BatchSize)),
		fmt.Sprintf("Throughput  submit %.2f tx/s | end-to-end %.2f tx/s", report.SubmitTPS, report.EndToEndTPS),
		fmt.Sprintf("Phases      decode %.2f ms | admit %.2f ms | fanout %.2f ms | converge %.2f ms",
			report.Phases.DecodeMS,
			report.Phases.ValidateAdmitMS,
			report.Phases.RelayFanoutMS,
			report.Phases.ConvergenceMS,
		),
	}
	for _, row := range scenarioMetricRows(report.Metrics) {
		lines = append(lines, fmt.Sprintf("Metric      %s: %s", row[0], row[1]))
	}
	if slowest := slowestNode(report.Nodes); slowest != "" {
		lines = append(lines, slowest)
	}
	if hottest := hottestPeer(flattenRelayPeers(report.Nodes)); hottest != "" {
		lines = append(lines, hottest)
	}
	return renderASCIIBox("BPU Benchmark Summary", lines)
}

func RenderSuiteASCIISummary(report *SuiteReport) string {
	lines := []string{
		fmt.Sprintf("Profile     %s", report.Profile),
		fmt.Sprintf("Cases       %d", len(report.Cases)),
		fmt.Sprintf("Window      %s -> %s",
			report.StartedAt.UTC().Format("15:04:05Z"),
			report.CompletedAt.UTC().Format("15:04:05Z"),
		),
	}
	lines = append(lines, asciiSuiteTable(report.Cases)...)
	return renderASCIIBox("BPU Benchmark Suite", lines)
}

func WriteReportFiles(report *Report, jsonPath, markdownPath string) error {
	if jsonPath != "" {
		if err := writeJSON(jsonPath, report); err != nil {
			return err
		}
	}
	if markdownPath != "" {
		if err := writeText(markdownPath, RenderMarkdown(report)); err != nil {
			return err
		}
	}
	return nil
}

func WriteSuiteReportFiles(report *SuiteReport, dir string) error {
	root := filepath.Clean(dir)
	if err := writeJSON(filepath.Join(root, "suite.json"), report); err != nil {
		return err
	}
	if err := writeText(filepath.Join(root, "suite.md"), RenderSuiteMarkdown(report)); err != nil {
		return err
	}
	for _, suiteCase := range report.Cases {
		slug := slugify(suiteCase.Name)
		if err := WriteReportFiles(&suiteCase.Report,
			filepath.Join(root, slug+".json"),
			filepath.Join(root, slug+".md"),
		); err != nil {
			return err
		}
	}
	return nil
}

func suppressLogs() func() {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	return func() {
		slog.SetDefault(previous)
	}
}

func withDefaults(opts RunOptions) RunOptions {
	defaults := DefaultRunOptions()
	if opts.Scenario == "" {
		opts.Scenario = defaults.Scenario
	}
	if opts.Profile == "" {
		opts.Profile = defaults.Profile
	}
	if opts.NodeCount <= 0 {
		opts.NodeCount = defaults.NodeCount
	}
	if opts.Topology == "" {
		opts.Topology = defaults.Topology
	}
	if opts.TxCount <= 0 {
		opts.TxCount = defaults.TxCount
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaults.BatchSize
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaults.Timeout
	}
	return opts
}

func withSuiteDefaults(opts SuiteOptions) SuiteOptions {
	defaults := DefaultSuiteOptions()
	if opts.Profile == "" {
		opts.Profile = defaults.Profile
	}
	if opts.NodeCount <= 0 {
		opts.NodeCount = defaults.NodeCount
	}
	if opts.TxCount <= 0 {
		opts.TxCount = defaults.TxCount
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaults.BatchSize
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaults.Timeout
	}
	return opts
}

func validateOptions(opts RunOptions) error {
	switch opts.Scenario {
	case ScenarioDirectSubmit, ScenarioRPCBatch, ScenarioP2PRelay, ScenarioUserMix, ScenarioChainedPackages, ScenarioOrphanStorm, ScenarioBlockTemplateBuild:
	default:
		return fmt.Errorf("unsupported scenario: %s", opts.Scenario)
	}
	if !opts.Profile.IsRegtestLike() {
		return fmt.Errorf("benchmark suite currently supports regtest-style profiles only")
	}
	if opts.Scenario == ScenarioP2PRelay && opts.NodeCount < 2 {
		return fmt.Errorf("p2p-relay requires at least 2 nodes")
	}
	if opts.Scenario != ScenarioP2PRelay && opts.NodeCount != 1 {
		return fmt.Errorf("%s requires exactly 1 node", opts.Scenario)
	}
	switch opts.Topology {
	case TopologyLine, TopologyMesh:
	default:
		return fmt.Errorf("unsupported topology: %s", opts.Topology)
	}
	maxSeedable, err := maxSeedableTxCount(opts.Profile)
	if err != nil {
		return err
	}
	if opts.TxCount > maxSeedable {
		return fmt.Errorf("tx count %d exceeds current benchmark seed capacity %d for %s; lower --txs or extend the funding strategy", opts.TxCount, maxSeedable, opts.Profile)
	}
	return nil
}

type suiteCase struct {
	Name    string
	Options RunOptions
}

func suiteCases(opts SuiteOptions) []suiteCase {
	cases := []suiteCase{
		{
			Name: "direct-submit",
			Options: RunOptions{
				Scenario:     ScenarioDirectSubmit,
				Profile:      opts.Profile,
				NodeCount:    1,
				TxCount:      opts.TxCount,
				BatchSize:    opts.BatchSize,
				Timeout:      opts.Timeout,
				DBRoot:       suiteDBRoot(opts.DBRoot, "direct-submit"),
				SuppressLogs: opts.SuppressLogs,
			},
		},
		{
			Name: "rpc-batch",
			Options: RunOptions{
				Scenario:     ScenarioRPCBatch,
				Profile:      opts.Profile,
				NodeCount:    1,
				TxCount:      opts.TxCount,
				BatchSize:    opts.BatchSize,
				Timeout:      opts.Timeout,
				DBRoot:       suiteDBRoot(opts.DBRoot, "rpc-batch"),
				SuppressLogs: opts.SuppressLogs,
			},
		},
		{
			Name: "user-mix",
			Options: RunOptions{
				Scenario:     ScenarioUserMix,
				Profile:      opts.Profile,
				NodeCount:    1,
				TxCount:      opts.TxCount,
				BatchSize:    opts.BatchSize,
				Timeout:      opts.Timeout,
				DBRoot:       suiteDBRoot(opts.DBRoot, "user-mix"),
				SuppressLogs: opts.SuppressLogs,
			},
		},
		{
			Name: "chained-packages",
			Options: RunOptions{
				Scenario:     ScenarioChainedPackages,
				Profile:      opts.Profile,
				NodeCount:    1,
				TxCount:      opts.TxCount,
				BatchSize:    opts.BatchSize,
				Timeout:      opts.Timeout,
				DBRoot:       suiteDBRoot(opts.DBRoot, "chained-packages"),
				SuppressLogs: opts.SuppressLogs,
			},
		},
		{
			Name: "orphan-storm",
			Options: RunOptions{
				Scenario:     ScenarioOrphanStorm,
				Profile:      opts.Profile,
				NodeCount:    1,
				TxCount:      opts.TxCount,
				BatchSize:    opts.BatchSize,
				Timeout:      opts.Timeout,
				DBRoot:       suiteDBRoot(opts.DBRoot, "orphan-storm"),
				SuppressLogs: opts.SuppressLogs,
			},
		},
		{
			Name: "block-template-rebuild",
			Options: RunOptions{
				Scenario:     ScenarioBlockTemplateBuild,
				Profile:      opts.Profile,
				NodeCount:    1,
				TxCount:      opts.TxCount,
				BatchSize:    opts.BatchSize,
				Timeout:      opts.Timeout,
				DBRoot:       suiteDBRoot(opts.DBRoot, "block-template-rebuild"),
				SuppressLogs: opts.SuppressLogs,
			},
		},
		{
			Name: "p2p-relay-2-line",
			Options: RunOptions{
				Scenario:     ScenarioP2PRelay,
				Profile:      opts.Profile,
				NodeCount:    2,
				Topology:     TopologyLine,
				TxCount:      opts.TxCount,
				BatchSize:    opts.BatchSize,
				Timeout:      opts.Timeout,
				DBRoot:       suiteDBRoot(opts.DBRoot, "p2p-relay-2-line"),
				SuppressLogs: opts.SuppressLogs,
			},
		},
	}
	if opts.NodeCount > 2 {
		cases = append(cases,
			suiteCase{
				Name: "p2p-relay-line",
				Options: RunOptions{
					Scenario:     ScenarioP2PRelay,
					Profile:      opts.Profile,
					NodeCount:    opts.NodeCount,
					Topology:     TopologyLine,
					TxCount:      opts.TxCount,
					BatchSize:    opts.BatchSize,
					Timeout:      opts.Timeout,
					DBRoot:       suiteDBRoot(opts.DBRoot, "p2p-relay-line"),
					SuppressLogs: opts.SuppressLogs,
				},
			},
			suiteCase{
				Name: "p2p-relay-mesh",
				Options: RunOptions{
					Scenario:     ScenarioP2PRelay,
					Profile:      opts.Profile,
					NodeCount:    opts.NodeCount,
					Topology:     TopologyMesh,
					TxCount:      opts.TxCount,
					BatchSize:    opts.BatchSize,
					Timeout:      opts.Timeout,
					DBRoot:       suiteDBRoot(opts.DBRoot, "p2p-relay-mesh"),
					SuppressLogs: opts.SuppressLogs,
				},
			},
		)
	}
	return cases
}

func openCluster(ctx context.Context, opts RunOptions) ([]*clusterNode, func(), error) {
	genesis, err := loadGenesisFixture()
	if err != nil {
		return nil, nil, err
	}
	mempoolCfg := benchmarkMempoolConfig(opts)
	dbRoot := opts.DBRoot
	if dbRoot == "" {
		dbRoot, err = os.MkdirTemp("", "bpu-bench-cluster-*")
		if err != nil {
			return nil, nil, err
		}
	}

	nodes := make([]*clusterNode, 0, opts.NodeCount)
	cleanup := func() {
		for _, benchNode := range nodes {
			if benchNode.cancel != nil {
				benchNode.cancel()
			}
			if benchNode.svc != nil {
				_ = benchNode.svc.Close()
			}
			if benchNode.errCh != nil {
				select {
				case <-benchNode.errCh:
				case <-time.After(2 * time.Second):
				}
			}
		}
		if opts.DBRoot == "" {
			_ = os.RemoveAll(dbRoot)
		}
	}

	for i := 0; i < opts.NodeCount; i++ {
		p2pAddr, err := reserveLoopbackAddr()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		rpcAddr := ""
		authToken := ""
		if opts.Scenario == ScenarioRPCBatch && i == 0 {
			rpcAddr, err = reserveLoopbackAddr()
			if err != nil {
				cleanup()
				return nil, nil, err
			}
			authToken = "bench-token"
		}
		dbPath := filepath.Join(dbRoot, fmt.Sprintf("node-%d", i), "chain")
		benchNode := &clusterNode{
			name:      fmt.Sprintf("node-%d", i),
			rpcAddr:   rpcAddr,
			authToken: authToken,
			p2pAddr:   p2pAddr,
			errCh:     make(chan error, 1),
		}
		svc, err := node.OpenService(node.ServiceConfig{
			Profile:            opts.Profile,
			DBPath:             dbPath,
			RPCAddr:            rpcAddr,
			RPCAuthToken:       authToken,
			RPCReadTimeout:     5 * time.Second,
			RPCWriteTimeout:    5 * time.Second,
			RPCHeaderTimeout:   2 * time.Second,
			RPCMaxBodyBytes:    8 << 20,
			P2PAddr:            p2pAddr,
			MaxInboundPeers:    max(32, opts.NodeCount*4),
			MaxOutboundPeers:   max(16, opts.NodeCount*4),
			HandshakeTimeout:   5 * time.Second,
			StallTimeout:       15 * time.Second,
			MaxMessageBytes:    8 << 20,
			MinRelayFeePerByte: 1,
			MaxTxSize:          mempoolCfg.maxTxSize,
			MaxAncestors:       mempoolCfg.maxAncestors,
			MaxDescendants:     mempoolCfg.maxDescendants,
			MaxOrphans:         mempoolCfg.maxOrphans,
			MinerKeyHash:       keyHashForSeed(byte(i + 1)),
			GenesisFixture:     "fixtures/genesis/regtest.json",
		}, genesis)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		benchNode.svc = svc
		nodeCtx, cancel := context.WithCancel(ctx)
		benchNode.cancel = cancel
		go func(n *clusterNode) {
			n.errCh <- n.svc.Start(nodeCtx)
		}(benchNode)
		if p2pAddr != "" {
			if err := waitForTCPListener(p2pAddr); err != nil {
				cleanup()
				return nil, nil, err
			}
		}
		if rpcAddr != "" {
			if err := waitForRPC(rpcAddr, authToken); err != nil {
				cleanup()
				return nil, nil, err
			}
		}
		nodes = append(nodes, benchNode)
	}
	return nodes, cleanup, nil
}

func connectCluster(ctx context.Context, nodes []*clusterNode, topology Topology) error {
	errCh := make(chan error, max(1, expectedConnectionCount(len(nodes), topology)))
	switch topology {
	case TopologyLine:
		for i := 0; i < len(nodes)-1; i++ {
			from := nodes[i]
			to := nodes[i+1]
			go func() {
				if err := from.svc.ConnectPeer(to.p2pAddr); err != nil {
					errCh <- fmt.Errorf("%s -> %s: %w", from.name, to.name, err)
				}
			}()
		}
	case TopologyMesh:
		for i := 0; i < len(nodes); i++ {
			for j := i + 1; j < len(nodes); j++ {
				from := nodes[i]
				to := nodes[j]
				go func() {
					if err := from.svc.ConnectPeer(to.p2pAddr); err != nil {
						errCh <- fmt.Errorf("%s -> %s: %w", from.name, to.name, err)
					}
				}()
			}
		}
	}
	return waitForPeerConnectivity(ctx, nodes, topology, errCh)
}

func waitForPeerConnectivity(ctx context.Context, nodes []*clusterNode, topology Topology, errCh <-chan error) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for peer connectivity: %w (%s)", ctx.Err(), peerConnectivityStatus(nodes, topology))
		case err := <-errCh:
			return err
		default:
		}
		ok := true
		for i, benchNode := range nodes {
			got := benchNode.svc.PeerCount()
			want := expectedPeerCount(len(nodes), i, topology)
			if got < want {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func expectedPeerCount(total, index int, topology Topology) int {
	if total <= 1 {
		return 0
	}
	switch topology {
	case TopologyMesh:
		return total - 1
	case TopologyLine:
		switch index {
		case 0, total - 1:
			return 1
		default:
			return 2
		}
	default:
		return 0
	}
}

func expectedConnectionCount(total int, topology Topology) int {
	switch topology {
	case TopologyMesh:
		return total * (total - 1) / 2
	case TopologyLine:
		if total <= 1 {
			return 0
		}
		return total - 1
	default:
		return 0
	}
}

func benchmarkMempoolConfig(opts RunOptions) mempoolBenchConfig {
	cfg := mempoolBenchConfig{
		maxTxSize:      1_000_000,
		maxAncestors:   256,
		maxDescendants: 256,
		maxOrphans:     128,
	}
	if opts.Scenario == ScenarioOrphanStorm {
		// The orphan-storm benchmark is intended to measure orphan handling cost, not the
		// steady-state eviction policy ceiling, so provision enough orphan capacity for the run.
		cfg.maxOrphans = max(cfg.maxOrphans, opts.TxCount)
	}
	return cfg
}

func executeScenario(ctx context.Context, cluster []*clusterNode, opts RunOptions, started time.Time) (workloadOutcome, error) {
	submitter := cluster[0]
	const seedReadyHeight = 1
	switch opts.Scenario {
	case ScenarioDirectSubmit, ScenarioRPCBatch, ScenarioP2PRelay:
		txs, err := seedSpendableFanout(submitter.svc, opts.TxCount)
		if err != nil {
			return workloadOutcome{}, err
		}
		if err := waitForHeights(ctx, cluster, seedReadyHeight); err != nil {
			return workloadOutcome{}, err
		}
		if _, err := waitForMempoolCounts(ctx, cluster, 0, time.Time{}); err != nil {
			return workloadOutcome{}, err
		}
		return runSubmissionScenario(ctx, cluster, submitter, opts, started, txs)
	case ScenarioUserMix:
		txs, metrics, err := seedUserMixWorkload(submitter.svc, opts.TxCount)
		if err != nil {
			return workloadOutcome{}, err
		}
		if err := waitForHeights(ctx, cluster, seedReadyHeight); err != nil {
			return workloadOutcome{}, err
		}
		if _, err := waitForMempoolCounts(ctx, cluster, 0, time.Time{}); err != nil {
			return workloadOutcome{}, err
		}
		outcome, err := runSubmissionScenario(ctx, cluster, submitter, opts, started, txs)
		if err != nil {
			return workloadOutcome{}, err
		}
		outcome.metrics.UserCount = metrics.UserCount
		outcome.metrics.ShortChainTxs = metrics.ShortChainTxs
		outcome.metrics.MultiOutputTxs = metrics.MultiOutputTxs
		return outcome, nil
	case ScenarioChainedPackages:
		txs, packageDepth, packageCount, err := seedChainedPackages(submitter.svc, opts.TxCount, opts.BatchSize)
		if err != nil {
			return workloadOutcome{}, err
		}
		if err := waitForHeights(ctx, cluster, seedReadyHeight); err != nil {
			return workloadOutcome{}, err
		}
		outcome, err := runDirectSubmitSequence(ctx, cluster, submitter, started, txs)
		if err != nil {
			return workloadOutcome{}, err
		}
		outcome.metrics.PackageDepth = packageDepth
		outcome.metrics.PackageCount = packageCount
		return outcome, nil
	case ScenarioOrphanStorm:
		txs, err := seedOrphanStorm(submitter.svc, opts.TxCount)
		if err != nil {
			return workloadOutcome{}, err
		}
		if err := waitForHeights(ctx, cluster, seedReadyHeight); err != nil {
			return workloadOutcome{}, err
		}
		outcome, err := runDirectSubmitSequence(ctx, cluster, submitter, started, txs)
		if err != nil {
			return workloadOutcome{}, err
		}
		return outcome, nil
	case ScenarioBlockTemplateBuild:
		txs, packageDepth, packageCount, err := seedChainedPackages(submitter.svc, opts.TxCount, opts.BatchSize)
		if err != nil {
			return workloadOutcome{}, err
		}
		if err := waitForHeights(ctx, cluster, seedReadyHeight); err != nil {
			return workloadOutcome{}, err
		}
		outcome, err := runTemplateRebuildScenario(ctx, cluster, submitter, started, txs, opts.BatchSize)
		if err != nil {
			return workloadOutcome{}, err
		}
		outcome.metrics.PackageDepth = packageDepth
		outcome.metrics.PackageCount = packageCount
		outcome.extraNotes = append(outcome.extraNotes, fmt.Sprintf("Template rebuild cadence follows batches of up to %d admitted transactions.", max(1, opts.BatchSize)))
		return outcome, nil
	default:
		return workloadOutcome{}, fmt.Errorf("unsupported scenario: %s", opts.Scenario)
	}
}

func runSubmissionScenario(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, opts RunOptions, started time.Time, txs []types.Transaction) (workloadOutcome, error) {
	submitStarted := time.Now()
	submission, err := submitWorkload(submitter, txs, opts)
	if err != nil {
		return workloadOutcome{}, err
	}
	propagation, err := waitForMempoolCounts(ctx, cluster, len(txs), started)
	if err != nil {
		return workloadOutcome{}, err
	}
	outcome := workloadOutcome{
		submitDone:  submission.submitDone,
		completed:   time.Now(),
		propagation: propagation,
		phases: PhaseReport{
			DecodeMS:        submission.decodeMS,
			ValidateAdmitMS: submission.validateAdmitMS,
			ConvergenceMS:   durationMS(maxDuration(propagation)),
		},
		metrics: submission.metrics,
	}
	if outcome.phases.ValidateAdmitMS == 0 {
		outcome.phases.ValidateAdmitMS = durationMS(submission.submitDone.Sub(submitStarted))
	}
	if opts.Scenario == ScenarioP2PRelay {
		direct := directRelayTargets(cluster, opts.Topology)
		outcome.phases.RelayFanoutMS = durationMS(subsetMaxDuration(propagation, direct))
		outcome.metrics.DirectRelayPeerCount = len(direct)
	} else {
		outcome.phases.RelayFanoutMS = 0
	}
	return outcome, nil
}

func runDirectSubmitSequence(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, started time.Time, txs []types.Transaction) (workloadOutcome, error) {
	submitStarted := time.Now()
	metrics := ScenarioMetrics{}
	for _, tx := range txs {
		admission, err := submitter.svc.SubmitTx(tx)
		if err != nil {
			return workloadOutcome{}, err
		}
		if admission.Orphaned {
			metrics.OrphanedTxs++
		}
		metrics.AcceptedTxs += len(admission.Accepted)
		if len(admission.Accepted) > 1 {
			metrics.PromotedOrphans += len(admission.Accepted) - 1
		}
		if orphanCount := submitter.svc.OrphanCount(); orphanCount > metrics.MaxOrphanCount {
			metrics.MaxOrphanCount = orphanCount
		}
	}
	submitDone := time.Now()
	propagation, err := waitForMempoolCounts(ctx, cluster, len(txs), started)
	if err != nil {
		return workloadOutcome{}, err
	}
	return workloadOutcome{
		submitDone:  submitDone,
		completed:   time.Now(),
		propagation: propagation,
		phases: PhaseReport{
			ValidateAdmitMS: durationMS(submitDone.Sub(submitStarted)),
			ConvergenceMS:   durationMS(maxDuration(propagation)),
		},
		metrics: metrics,
	}, nil
}

func runTemplateRebuildScenario(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, started time.Time, txs []types.Transaction, batchSize int) (workloadOutcome, error) {
	submitStarted := time.Now()
	metrics := ScenarioMetrics{}
	if batchSize <= 0 {
		batchSize = 1
	}
	rebuildSamples := make([]float64, 0, (len(txs)+batchSize-1)/batchSize)
	for start := 0; start < len(txs); start += batchSize {
		end := start + batchSize
		if end > len(txs) {
			end = len(txs)
		}
		for _, tx := range txs[start:end] {
			admission, err := submitter.svc.SubmitTx(tx)
			if err != nil {
				return workloadOutcome{}, err
			}
			metrics.AcceptedTxs += len(admission.Accepted)
		}
		rebuildStarted := time.Now()
		template, err := submitter.svc.BuildBlockTemplate()
		if err != nil {
			return workloadOutcome{}, err
		}
		rebuildSamples = append(rebuildSamples, durationMS(time.Since(rebuildStarted)))
		metrics.TemplateSelectedTxs = max(metrics.TemplateSelectedTxs, len(template.Txs)-1)
	}
	submitDone := time.Now()
	propagation, err := waitForMempoolCounts(ctx, cluster, len(txs), started)
	if err != nil {
		return workloadOutcome{}, err
	}
	metrics.TemplateRebuilds = len(rebuildSamples)
	metrics.TemplateRebuildAvgMS = averageFloat64(rebuildSamples)
	metrics.TemplateRebuildP95MS = percentileFloat64(rebuildSamples, 95)
	metrics.TemplateRebuildMaxMS = maxFloat64(rebuildSamples)
	templateStats := submitter.svc.BlockTemplateStats()
	metrics.TemplateCacheHits = templateStats.CacheHits
	metrics.TemplateFrontier = templateStats.FrontierCandidates
	return workloadOutcome{
		submitDone:  submitDone,
		completed:   time.Now(),
		propagation: propagation,
		phases: PhaseReport{
			ValidateAdmitMS: durationMS(submitDone.Sub(submitStarted)),
			ConvergenceMS:   durationMS(maxDuration(propagation)),
		},
		metrics: metrics,
	}, nil
}

func submitWorkload(submitter *clusterNode, txs []types.Transaction, opts RunOptions) (submissionOutcome, error) {
	switch opts.Scenario {
	case ScenarioDirectSubmit, ScenarioP2PRelay, ScenarioUserMix:
		started := time.Now()
		metrics := ScenarioMetrics{}
		for _, tx := range txs {
			admission, err := submitter.svc.SubmitTx(tx)
			if err != nil {
				return submissionOutcome{}, err
			}
			metrics.AcceptedTxs += len(admission.Accepted)
		}
		submitDone := time.Now()
		return submissionOutcome{
			submitDone:      submitDone,
			validateAdmitMS: durationMS(submitDone.Sub(started)),
			metrics:         metrics,
		}, nil
	case ScenarioRPCBatch:
		client := &http.Client{Timeout: 10 * time.Second}
		metrics := ScenarioMetrics{}
		var decodeMS float64
		var validateAdmitMS float64
		for start := 0; start < len(txs); start += opts.BatchSize {
			end := start + opts.BatchSize
			if end > len(txs) {
				end = len(txs)
			}
			var out struct {
				Accepted                int     `json:"accepted"`
				DecodeDurationMS        float64 `json:"decode_duration_ms"`
				ValidateAdmitDurationMS float64 `json:"validate_admit_duration_ms"`
			}
			if err := callRPC(client, submitter.rpcAddr, submitter.authToken, "submitpackedtxbatch", map[string]string{
				"packed": encodePackedTransactions(txs[start:end]),
			}, &out); err != nil {
				return submissionOutcome{}, err
			}
			metrics.AcceptedTxs += out.Accepted
			decodeMS += out.DecodeDurationMS
			validateAdmitMS += out.ValidateAdmitDurationMS
		}
		return submissionOutcome{
			submitDone:      time.Now(),
			decodeMS:        decodeMS,
			validateAdmitMS: validateAdmitMS,
			metrics:         metrics,
		}, nil
	default:
		return submissionOutcome{}, fmt.Errorf("unsupported scenario: %s", opts.Scenario)
	}
}

func encodePackedTransactions(txs []types.Transaction) string {
	if len(txs) == 0 {
		return ""
	}
	buf := make([]byte, 0)
	for _, tx := range txs {
		encoded := tx.Encode()
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(encoded)))
		buf = append(buf, lenBuf...)
		buf = append(buf, encoded...)
	}
	return base64.StdEncoding.EncodeToString(buf)
}

func waitForHeights(ctx context.Context, nodes []*clusterNode, height uint64) error {
	return waitForCondition(ctx, func() (bool, error) {
		for _, benchNode := range nodes {
			if benchNode.svc.BlockHeight() < height {
				return false, nil
			}
		}
		return true, nil
	}, func() string {
		return heightStatus(nodes, height)
	})
}

func waitForMempoolCounts(ctx context.Context, nodes []*clusterNode, target int, started time.Time) (map[string]time.Duration, error) {
	reached := make(map[string]time.Duration, len(nodes))
	err := waitForCondition(ctx, func() (bool, error) {
		allReached := true
		for _, benchNode := range nodes {
			count := benchNode.svc.MempoolCount()
			targetReached := false
			if target <= 0 {
				targetReached = count == 0
			} else {
				targetReached = count >= target
			}
			if targetReached {
				if !started.IsZero() {
					if _, ok := reached[benchNode.name]; !ok {
						reached[benchNode.name] = time.Since(started)
					}
				}
				continue
			}
			allReached = false
		}
		return allReached, nil
	}, func() string {
		return mempoolStatus(nodes, target, reached)
	})
	return reached, err
}

func waitForCondition(ctx context.Context, fn func() (bool, error), status func() string) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := fn()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			if status == nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w (%s)", ctx.Err(), status())
		case <-ticker.C:
		}
	}
}

func peerConnectivityStatus(nodes []*clusterNode, topology Topology) string {
	parts := make([]string, 0, len(nodes))
	for i, benchNode := range nodes {
		parts = append(parts, fmt.Sprintf("%s peers=%d/%d", benchNode.name, benchNode.svc.PeerCount(), expectedPeerCount(len(nodes), i, topology)))
	}
	return strings.Join(parts, ", ")
}

func heightStatus(nodes []*clusterNode, target uint64) string {
	parts := make([]string, 0, len(nodes))
	for _, benchNode := range nodes {
		parts = append(parts, fmt.Sprintf("%s height=%d target=%d", benchNode.name, benchNode.svc.BlockHeight(), target))
	}
	return strings.Join(parts, ", ")
}

func mempoolStatus(nodes []*clusterNode, target int, reached map[string]time.Duration) string {
	parts := make([]string, 0, len(nodes))
	for _, benchNode := range nodes {
		count := benchNode.svc.MempoolCount()
		status := fmt.Sprintf("%s mempool=%d target=%d", benchNode.name, count, target)
		if dur, ok := reached[benchNode.name]; ok {
			status = fmt.Sprintf("%s reached=%s", status, dur.Round(time.Millisecond))
		}
		parts = append(parts, status)
	}
	return strings.Join(parts, ", ")
}

func waitForTCPListener(addr string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func buildNodeReports(nodes []*clusterNode, reached map[string]time.Duration) []NodeReport {
	out := make([]NodeReport, 0, len(nodes))
	for _, benchNode := range nodes {
		report := NodeReport{
			Name:         benchNode.name,
			RPCAddr:      benchNode.rpcAddr,
			P2PAddr:      benchNode.p2pAddr,
			PeerCount:    benchNode.svc.PeerCount(),
			BlockHeight:  benchNode.svc.BlockHeight(),
			HeaderHeight: benchNode.svc.HeaderHeight(),
			MempoolCount: benchNode.svc.MempoolCount(),
			RelayPeers:   buildPeerRelayReports(benchNode.svc.RelayPeerStats()),
		}
		if reachedAt, ok := reached[benchNode.name]; ok {
			ms := durationMS(reachedAt)
			report.FullPropagationMS = &ms
		}
		out = append(out, report)
	}
	return out
}

func reportTopology(opts RunOptions) string {
	if opts.Scenario != ScenarioP2PRelay {
		return ""
	}
	return string(opts.Topology)
}

func buildNotes(opts RunOptions, extra []string) []string {
	notes := []string{
		"All nodes run on local loopback, so the report measures BPU stack overhead rather than WAN latency.",
	}
	switch opts.Scenario {
	case ScenarioDirectSubmit, ScenarioRPCBatch, ScenarioP2PRelay:
		notes = append([]string{"Independent fanout spends are used so relay and admission are not dominated by tx chaining."}, notes...)
	case ScenarioUserMix:
		notes = append([]string{"Mixed-user traffic combines broad one-input payments, change outputs, and short follow-up chains so the workload looks more like many wallets than a single lab pattern."}, notes...)
	case ScenarioChainedPackages:
		notes = append([]string{"Dependent chains are submitted in package order so ancestor accounting and package-aware template selection stay hot."}, notes...)
	case ScenarioOrphanStorm:
		notes = append([]string{"Children arrive before parents to stress orphan storage, promotion, and eviction accounting."}, notes...)
	case ScenarioBlockTemplateBuild:
		notes = append([]string{"Package-heavy mempool batches are followed by live block-template rebuilds against the real miner selection path."}, notes...)
	}
	if opts.Scenario == ScenarioP2PRelay {
		notes = append(notes, fmt.Sprintf("Real P2P relay is measured across %d nodes using the %s topology.", opts.NodeCount, opts.Topology))
	}
	if opts.Scenario == ScenarioRPCBatch {
		notes = append(notes, "Submission uses authenticated HTTP JSON-RPC submitpackedtxbatch to keep live stress focused on transport and admission rather than hex expansion.")
	}
	notes = append(notes, extra...)
	return notes
}

func durationMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func ratePerSecond(count int, elapsed time.Duration) float64 {
	if count <= 0 || elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

func maxDuration(reached map[string]time.Duration) time.Duration {
	var maxValue time.Duration
	for _, value := range reached {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func subsetMaxDuration(reached map[string]time.Duration, nodes []*clusterNode) time.Duration {
	var maxValue time.Duration
	for _, benchNode := range nodes {
		if value := reached[benchNode.name]; value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func averageFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total float64
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func maxFloat64(values []float64) float64 {
	var maxValue float64
	for _, value := range values {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func percentileFloat64(values []float64, p int) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	if p <= 0 {
		return ordered[0]
	}
	if p >= 100 {
		return ordered[len(ordered)-1]
	}
	index := (len(ordered) - 1) * p / 100
	return ordered[index]
}

func directRelayTargets(cluster []*clusterNode, topology Topology) []*clusterNode {
	if len(cluster) <= 1 {
		return nil
	}
	switch topology {
	case TopologyMesh:
		return cluster[1:]
	case TopologyLine:
		return cluster[1:2]
	default:
		return nil
	}
}

func buildPeerRelayReports(stats []node.PeerRelayStats) []PeerRelayReport {
	if len(stats) == 0 {
		return nil
	}
	out := make([]PeerRelayReport, 0, len(stats))
	for _, stat := range stats {
		out = append(out, PeerRelayReport{
			Addr:          stat.Addr,
			Outbound:      stat.Outbound,
			QueueDepth:    stat.QueueDepth,
			MaxQueueDepth: stat.MaxQueueDepth,
			SentMessages:  stat.SentMessages,
			TxInvItems:    stat.TxInvItems,
			BlockInvItems: stat.BlockInvItems,
			TxBatchMsgs:   stat.TxBatchMsgs,
			TxBatchItems:  stat.TxBatchItems,
			TxReconMsgs:   stat.TxReconMsgs,
			TxReconItems:  stat.TxReconItems,
			TxReqMsgs:     stat.TxReqMsgs,
			TxReqItems:    stat.TxReqItems,
			DroppedTxs:    stat.DroppedTxs,
			RelayEvents:   stat.RelayEvents,
			RelayAvgMS:    stat.RelayAvgMS,
			RelayP95MS:    stat.RelayP95MS,
			RelayMaxMS:    stat.RelayMaxMS,
		})
	}
	return out
}

func aggregateRelayMetrics(nodes []NodeReport) (int, int, int, int, int, int) {
	var batchMessages int
	var batchItems int
	var reconMessages int
	var reconItems int
	var reqMessages int
	var reqItems int
	for _, node := range nodes {
		for _, peer := range node.RelayPeers {
			batchMessages += peer.TxBatchMsgs
			batchItems += peer.TxBatchItems
			reconMessages += peer.TxReconMsgs
			reconItems += peer.TxReconItems
			reqMessages += peer.TxReqMsgs
			reqItems += peer.TxReqItems
		}
	}
	return batchMessages, batchItems, reconMessages, reconItems, reqMessages, reqItems
}

type relayPeerRow struct {
	NodeName string
	PeerRelayReport
}

func flattenRelayPeers(nodes []NodeReport) []relayPeerRow {
	rows := make([]relayPeerRow, 0)
	for _, node := range nodes {
		for _, peer := range node.RelayPeers {
			rows = append(rows, relayPeerRow{
				NodeName:        node.Name,
				PeerRelayReport: peer,
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].NodeName == rows[j].NodeName {
			return rows[i].Addr < rows[j].Addr
		}
		return rows[i].NodeName < rows[j].NodeName
	})
	return rows
}

func scenarioMetricRows(metrics ScenarioMetrics) [][2]string {
	rows := make([][2]string, 0, 10)
	if metrics.AcceptedTxs > 0 {
		rows = append(rows, [2]string{"Accepted txs", strconv.Itoa(metrics.AcceptedTxs)})
	}
	if metrics.OrphanedTxs > 0 {
		rows = append(rows, [2]string{"Orphaned txs", strconv.Itoa(metrics.OrphanedTxs)})
	}
	if metrics.PromotedOrphans > 0 {
		rows = append(rows, [2]string{"Promoted orphans", strconv.Itoa(metrics.PromotedOrphans)})
	}
	if metrics.MaxOrphanCount > 0 {
		rows = append(rows, [2]string{"Max orphan pool", strconv.Itoa(metrics.MaxOrphanCount)})
	}
	if metrics.PackageDepth > 0 {
		rows = append(rows, [2]string{"Package depth", strconv.Itoa(metrics.PackageDepth)})
	}
	if metrics.PackageCount > 0 {
		rows = append(rows, [2]string{"Package count", strconv.Itoa(metrics.PackageCount)})
	}
	if metrics.UserCount > 0 {
		rows = append(rows, [2]string{"User lanes", strconv.Itoa(metrics.UserCount)})
	}
	if metrics.MultiOutputTxs > 0 {
		rows = append(rows, [2]string{"Multi-output txs", strconv.Itoa(metrics.MultiOutputTxs)})
	}
	if metrics.ShortChainTxs > 0 {
		rows = append(rows, [2]string{"Short-chain txs", strconv.Itoa(metrics.ShortChainTxs)})
	}
	if metrics.DirectRelayPeerCount > 0 {
		rows = append(rows, [2]string{"Direct relay peers", strconv.Itoa(metrics.DirectRelayPeerCount)})
	}
	if metrics.TxBatchMessages > 0 {
		rows = append(rows, [2]string{"Tx batch msgs", strconv.Itoa(metrics.TxBatchMessages)})
		rows = append(rows, [2]string{"Tx batch items", strconv.Itoa(metrics.TxBatchItems)})
	}
	if metrics.TxReconMessages > 0 {
		rows = append(rows, [2]string{"Tx recon msgs", strconv.Itoa(metrics.TxReconMessages)})
		rows = append(rows, [2]string{"Tx recon items", strconv.Itoa(metrics.TxReconItems)})
	}
	if metrics.TxRequestMessages > 0 {
		rows = append(rows, [2]string{"Tx request msgs", strconv.Itoa(metrics.TxRequestMessages)})
		rows = append(rows, [2]string{"Tx request items", strconv.Itoa(metrics.TxRequestItems)})
	}
	if metrics.TemplateRebuilds > 0 {
		rows = append(rows, [2]string{"Template rebuilds", strconv.Itoa(metrics.TemplateRebuilds)})
		if metrics.TemplateCacheHits > 0 {
			rows = append(rows, [2]string{"Template cache hits", strconv.Itoa(metrics.TemplateCacheHits)})
		}
		if metrics.TemplateFrontier > 0 {
			rows = append(rows, [2]string{"Template frontier", strconv.Itoa(metrics.TemplateFrontier)})
		}
		rows = append(rows, [2]string{"Template rebuild avg", fmt.Sprintf("%.2f ms", metrics.TemplateRebuildAvgMS)})
		rows = append(rows, [2]string{"Template rebuild p95", fmt.Sprintf("%.2f ms", metrics.TemplateRebuildP95MS)})
		rows = append(rows, [2]string{"Template rebuild max", fmt.Sprintf("%.2f ms", metrics.TemplateRebuildMaxMS)})
		rows = append(rows, [2]string{"Template selected txs", strconv.Itoa(metrics.TemplateSelectedTxs)})
	}
	return rows
}

func asciiTopologySuffix(topology string) string {
	if strings.TrimSpace(topology) == "" {
		return ""
	}
	return " / " + topology
}

func asciiBatchSuffix(batchSize int) string {
	if batchSize <= 0 {
		return ""
	}
	return fmt.Sprintf(" / batch %d", batchSize)
}

func slowestNode(nodes []NodeReport) string {
	var (
		name string
		best float64
	)
	for _, node := range nodes {
		if node.FullPropagationMS == nil {
			continue
		}
		if *node.FullPropagationMS > best {
			best = *node.FullPropagationMS
			name = node.Name
		}
	}
	if name == "" {
		return ""
	}
	return fmt.Sprintf("Slowest     %s full propagation %.2f ms", name, best)
}

func hottestPeer(rows []relayPeerRow) string {
	var best relayPeerRow
	for _, row := range rows {
		if row.MaxQueueDepth > best.MaxQueueDepth || row.TxReconItems > best.TxReconItems || row.TxReqItems > best.TxReqItems || row.TxBatchItems > best.TxBatchItems || row.TxInvItems > best.TxInvItems || row.RelayMaxMS > best.RelayMaxMS {
			best = row
		}
	}
	if best.Addr == "" {
		return ""
	}
	return fmt.Sprintf("Relay Hot   %s -> %s avg %.2f ms | p95 %.2f ms | qmax %d | recon %d/%d | req %d/%d | txbatch %d/%d",
		best.NodeName,
		best.Addr,
		best.RelayAvgMS,
		best.RelayP95MS,
		best.MaxQueueDepth,
		best.TxReconMsgs,
		best.TxReconItems,
		best.TxReqMsgs,
		best.TxReqItems,
		best.TxBatchMsgs,
		best.TxBatchItems,
	)
}

func asciiSuiteTable(cases []SuiteCaseReport) []string {
	header := fmt.Sprintf("%-24s %10s %10s %10s %10s", "Case", "Submit", "End2End", "Fanout", "Converge")
	rows := []string{header}
	for _, suiteCase := range cases {
		rows = append(rows, fmt.Sprintf("%-24s %10.2f %10.2f %10.2f %10.2f",
			suiteCase.Name,
			suiteCase.Report.SubmitTPS,
			suiteCase.Report.EndToEndTPS,
			suiteCase.Report.Phases.RelayFanoutMS,
			suiteCase.Report.Phases.ConvergenceMS,
		))
	}
	return rows
}

func renderASCIIBox(title string, lines []string) string {
	width := len(title)
	for _, line := range lines {
		if len(line) > width {
			width = len(line)
		}
	}
	var b strings.Builder
	b.WriteString("+" + strings.Repeat("-", width+2) + "+\n")
	b.WriteString(fmt.Sprintf("| %-*s |\n", width, title))
	b.WriteString("+" + strings.Repeat("-", width+2) + "+\n")
	for _, line := range lines {
		b.WriteString(fmt.Sprintf("| %-*s |\n", width, line))
	}
	b.WriteString("+" + strings.Repeat("-", width+2) + "+")
	return b.String()
}

func loadGenesisFixture() (*types.Block, error) {
	raw, err := os.ReadFile(resolveRepoPath("fixtures", "genesis", "regtest.json"))
	if err != nil {
		return nil, err
	}
	var fixture struct {
		BlockHex string `json:"block_hex"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		return nil, err
	}
	block, err := consensus.DecodeBlockHex(fixture.BlockHex, types.DefaultCodecLimits())
	if err != nil {
		return nil, err
	}
	return &block, nil
}

func keyHashForSeed(seed byte) [32]byte {
	pub, _ := crypto.SignSchnorrForTest([32]byte{seed}, &[32]byte{})
	return crypto.KeyHash(&pub)
}

func signSingleInputTx(tx types.Transaction, spenderSeed byte, prevValue uint64) (types.Transaction, error) {
	msg, err := consensus.Sighash(&tx, 0, []uint64{prevValue})
	if err != nil {
		return types.Transaction{}, err
	}
	pubKey, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{PubKey: pubKey, Signature: sig}}}
	return tx, nil
}

func buildFanoutTx(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, outputKeyHash [32]byte, outputs int) (types.Transaction, []uint64, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: make([]types.TxOutput, outputs),
		},
	}
	for i := range tx.Base.Outputs {
		tx.Base.Outputs[i] = types.TxOutput{ValueAtoms: 1, KeyHash: outputKeyHash}
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	fee := uint64(len(tx.Encode()))
	if prevValue < fee {
		return types.Transaction{}, nil, consensus.ErrInputsLessThanOutputs
	}
	remaining := prevValue - fee
	if remaining < uint64(outputs) {
		return types.Transaction{}, nil, consensus.ErrInputsLessThanOutputs
	}
	perOutput := remaining / uint64(outputs)
	remainder := remaining % uint64(outputs)
	values := make([]uint64, outputs)
	for i := range tx.Base.Outputs {
		value := perOutput
		if i == outputs-1 {
			value += remainder
		}
		values[i] = value
		tx.Base.Outputs[i].ValueAtoms = value
	}
	tx, err = signSingleInputTx(tx, spenderSeed, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	return tx, values, nil
}

func buildChildTx(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, outputKeyHash [32]byte) (types.Transaction, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: outputKeyHash}},
		},
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue)
	if err != nil {
		return types.Transaction{}, err
	}
	fee := uint64(len(tx.Encode()))
	if prevValue < fee {
		return types.Transaction{}, consensus.ErrInputsLessThanOutputs
	}
	tx.Base.Outputs[0].ValueAtoms = prevValue - fee
	return signSingleInputTx(tx, spenderSeed, prevValue)
}

type fundingOutput struct {
	OutPoint types.OutPoint
	Value    uint64
}

type userFundingOutput struct {
	OutPoint     types.OutPoint
	Value        uint64
	SpenderSeed  byte
	RecipientTag byte
}

func seedFundingOutputs(svc *node.Service, outputCount int, recipientSeed byte) ([]fundingOutput, error) {
	if outputCount <= 0 {
		return nil, nil
	}
	keyHashes := make([][32]byte, outputCount)
	for i := range keyHashes {
		keyHashes[i] = keyHashForSeed(recipientSeed)
	}
	funding, err := svc.MineFundingOutputs(keyHashes)
	if err != nil {
		return nil, err
	}
	outputs := make([]fundingOutput, 0, outputCount)
	for _, output := range funding {
		outputs = append(outputs, fundingOutput{
			OutPoint: output.OutPoint,
			Value:    output.Value,
		})
	}
	return outputs, nil
}

func buildFanoutTxToRecipients(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, recipients [][32]byte) (types.Transaction, []uint64, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: make([]types.TxOutput, len(recipients)),
		},
	}
	for i, keyHash := range recipients {
		tx.Base.Outputs[i] = types.TxOutput{ValueAtoms: 1, KeyHash: keyHash}
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	fee := uint64(len(tx.Encode()))
	if prevValue < fee {
		return types.Transaction{}, nil, consensus.ErrInputsLessThanOutputs
	}
	remaining := prevValue - fee
	if remaining < uint64(len(recipients)) {
		return types.Transaction{}, nil, consensus.ErrInputsLessThanOutputs
	}
	perOutput := remaining / uint64(len(recipients))
	remainder := remaining % uint64(len(recipients))
	values := make([]uint64, len(recipients))
	for i := range tx.Base.Outputs {
		value := perOutput
		if i == len(tx.Base.Outputs)-1 {
			value += remainder
		}
		values[i] = value
		tx.Base.Outputs[i].ValueAtoms = value
	}
	tx, err = signSingleInputTx(tx, spenderSeed, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	return tx, values, nil
}

func seedFundingOutputsForUsers(svc *node.Service, outputCount, userCount int) ([]userFundingOutput, error) {
	if outputCount <= 0 {
		return nil, nil
	}
	userCount = max(1, userCount)
	recipients := make([][32]byte, 0, outputCount)
	seeds := make([]byte, 0, outputCount)
	for i := 0; i < outputCount; i++ {
		seed := byte(2 + (i % userCount))
		recipients = append(recipients, keyHashForSeed(seed))
		seeds = append(seeds, seed)
	}
	funding, err := svc.MineFundingOutputs(recipients)
	if err != nil {
		return nil, err
	}
	outputs := make([]userFundingOutput, 0, outputCount)
	for vout, output := range funding {
		outputs = append(outputs, userFundingOutput{
			OutPoint:     output.OutPoint,
			Value:        output.Value,
			SpenderSeed:  seeds[vout],
			RecipientTag: byte(vout % 251),
		})
	}
	return outputs, nil
}

func buildPaymentWithChangeTx(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, paymentKeyHash, changeKeyHash [32]byte) (types.Transaction, uint64, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{
				{ValueAtoms: 1, KeyHash: paymentKeyHash},
				{ValueAtoms: 1, KeyHash: changeKeyHash},
			},
		},
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue)
	if err != nil {
		return types.Transaction{}, 0, err
	}
	fee := uint64(len(tx.Encode()))
	if prevValue <= fee+1 {
		return types.Transaction{}, 0, consensus.ErrInputsLessThanOutputs
	}
	remaining := prevValue - fee
	paymentValue := remaining * 2 / 3
	if paymentValue == 0 {
		paymentValue = 1
	}
	if paymentValue >= remaining {
		paymentValue = remaining - 1
	}
	changeValue := remaining - paymentValue
	tx.Base.Outputs[0].ValueAtoms = paymentValue
	tx.Base.Outputs[1].ValueAtoms = changeValue
	tx, err = signSingleInputTx(tx, spenderSeed, prevValue)
	if err != nil {
		return types.Transaction{}, 0, err
	}
	return tx, changeValue, nil
}

func seedSpendableFanout(svc *node.Service, txCount int) ([]types.Transaction, error) {
	outputs, err := seedFundingOutputs(svc, txCount, 2)
	if err != nil {
		return nil, err
	}
	childKeyHash := keyHashForSeed(3)
	txs := make([]types.Transaction, 0, txCount)
	for _, output := range outputs {
		tx, err := buildChildTx(2, output.OutPoint, output.Value, childKeyHash)
		if err != nil {
			if errors.Is(err, consensus.ErrInputsLessThanOutputs) {
				return nil, fmt.Errorf("tx count %d exceeds current benchmark seed capacity; lower --txs (the default suite uses %d)", txCount, DefaultRunOptions().TxCount)
			}
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func seedUserMixWorkload(svc *node.Service, txCount int) ([]types.Transaction, ScenarioMetrics, error) {
	userCount := min(64, max(8, txCount/32))
	outputs, err := seedFundingOutputsForUsers(svc, txCount, userCount)
	if err != nil {
		return nil, ScenarioMetrics{}, err
	}
	type queuedChange struct {
		outPoint     types.OutPoint
		value        uint64
		spenderSeed  byte
		recipientTag byte
	}
	pendingChanges := make(map[byte][]queuedChange)
	txs := make([]types.Transaction, 0, txCount)
	metrics := ScenarioMetrics{UserCount: userCount}

	for i, output := range outputs {
		useFollowUp := i%5 == 4 && len(pendingChanges[output.SpenderSeed]) > 0
		if useFollowUp {
			queue := pendingChanges[output.SpenderSeed]
			change := queue[0]
			pendingChanges[output.SpenderSeed] = queue[1:]
			tx, err := buildChildTx(change.spenderSeed, change.outPoint, change.value, keyHashForSeed(byte(90+(change.recipientTag%64))))
			if err != nil {
				return nil, ScenarioMetrics{}, err
			}
			txs = append(txs, tx)
			metrics.ShortChainTxs++
			continue
		}

		paymentSeed := byte(120 + (output.RecipientTag % 80))
		tx, changeValue, err := buildPaymentWithChangeTx(output.SpenderSeed, output.OutPoint, output.Value, keyHashForSeed(paymentSeed), keyHashForSeed(output.SpenderSeed))
		if err != nil {
			return nil, ScenarioMetrics{}, err
		}
		txs = append(txs, tx)
		metrics.MultiOutputTxs++
		changeOut := queuedChange{
			outPoint:     types.OutPoint{TxID: consensus.TxID(&tx), Vout: 1},
			value:        changeValue,
			spenderSeed:  output.SpenderSeed,
			recipientTag: output.RecipientTag,
		}
		pendingChanges[output.SpenderSeed] = append(pendingChanges[output.SpenderSeed], changeOut)
	}

	return txs, metrics, nil
}

func seedChainedPackages(svc *node.Service, txCount, batchSize int) ([]types.Transaction, int, int, error) {
	packageDepth := max(2, min(txCount, max(2, min(8, max(1, batchSize/8)))))
	packageCount := (txCount + packageDepth - 1) / packageDepth
	outputs, err := seedFundingOutputs(svc, packageCount, 2)
	if err != nil {
		return nil, 0, 0, err
	}
	keyHash := keyHashForSeed(2)
	txs := make([]types.Transaction, 0, txCount)
	for _, output := range outputs {
		prevOut := output.OutPoint
		prevValue := output.Value
		for depth := 0; depth < packageDepth && len(txs) < txCount; depth++ {
			tx, err := buildChildTx(2, prevOut, prevValue, keyHash)
			if err != nil {
				return nil, 0, 0, err
			}
			txs = append(txs, tx)
			prevOut = types.OutPoint{TxID: consensus.TxID(&tx), Vout: 0}
			prevValue = tx.Base.Outputs[0].ValueAtoms
		}
	}
	return txs, packageDepth, packageCount, nil
}

func seedOrphanStorm(svc *node.Service, txCount int) ([]types.Transaction, error) {
	parentCount := (txCount + 1) / 2
	outputs, err := seedFundingOutputs(svc, parentCount, 2)
	if err != nil {
		return nil, err
	}
	parents := make([]types.Transaction, 0, parentCount)
	children := make([]types.Transaction, 0, txCount/2)
	for _, output := range outputs {
		parent, err := buildChildTx(2, output.OutPoint, output.Value, keyHashForSeed(3))
		if err != nil {
			return nil, err
		}
		parents = append(parents, parent)
		if len(parents)+len(children) >= txCount {
			continue
		}
		parentOut := types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}
		child, err := buildChildTx(3, parentOut, parent.Base.Outputs[0].ValueAtoms, keyHashForSeed(4))
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}
	ordered := make([]types.Transaction, 0, len(children)+len(parents))
	ordered = append(ordered, children...)
	ordered = append(ordered, parents...)
	return ordered, nil
}

func reserveLoopbackAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func waitForRPC(addr, authToken string) error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var out map[string]any
		if err := callRPC(client, addr, authToken, "getinfo", nil, &out); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("rpc %s did not become ready", addr)
}

func callRPC(client *http.Client, addr, authToken, method string, params interface{}, out interface{}) error {
	reqBody, err := json.Marshal(rpcRequest{Method: method, Params: params})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var rpcResp rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return err
	}
	if rpcResp.Error != "" {
		return fmt.Errorf("rpc %s failed: %s", method, rpcResp.Error)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(rpcResp.Result, out)
}

func writeJSON(path string, value any) error {
	buf, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, append(buf, '\n'))
}

func writeText(path, text string) error {
	return writeFile(path, []byte(text))
}

func writeFile(path string, data []byte) error {
	clean := filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(clean), 0o755); err != nil {
		return err
	}
	return os.WriteFile(clean, data, 0o644)
}

func resolveRepoPath(parts ...string) string {
	joined := filepath.Join(parts...)
	if _, err := os.Stat(joined); err == nil {
		return joined
	}
	return filepath.Join(append([]string{".."}, parts...)...)
}

func slugify(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "report"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "report"
	}
	return out
}

func suiteDBRoot(base, name string) string {
	if strings.TrimSpace(base) == "" {
		return ""
	}
	return filepath.Join(base, name)
}

func maxSeedableTxCount(profile types.ChainProfile) (int, error) {
	params := consensus.ParamsForProfile(profile)
	coinbaseValue := consensus.SubsidyAtoms(1, params)
	low := 1
	high := 1
	for high < 1<<15 {
		ok, err := canSeedTxCount(high, coinbaseValue)
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		low = high
		high *= 2
	}
	for low < high {
		mid := low + (high-low+1)/2
		ok, err := canSeedTxCount(mid, coinbaseValue)
		if err != nil {
			return 0, err
		}
		if ok {
			low = mid
			continue
		}
		high = mid - 1
	}
	return low, nil
}

func canSeedTxCount(txCount int, coinbaseValue uint64) (bool, error) {
	if txCount <= 0 || coinbaseValue < uint64(txCount) {
		return false, nil
	}
	perOutput := coinbaseValue / uint64(txCount)
	if perOutput == 0 {
		return false, nil
	}
	_, err := buildChildTx(2, types.OutPoint{TxID: [32]byte{1}, Vout: 0}, perOutput, keyHashForSeed(3))
	if err != nil {
		if errors.Is(err, consensus.ErrInputsLessThanOutputs) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
