package benchmarks

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
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
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/profiling"
	"bitcoin-pure/internal/types"
)

type Scenario string

const (
	ScenarioDirectSubmit       Scenario = "direct-submit"
	ScenarioRPCBatch           Scenario = "rpc-batch"
	ScenarioP2PRelay           Scenario = "p2p-relay"
	ScenarioConfirmedBlocks    Scenario = "confirmed-blocks"
	ScenarioUserMix            Scenario = "user-mix"
	ScenarioChainedPackages    Scenario = "chained-packages"
	ScenarioOrphanStorm        Scenario = "orphan-storm"
	ScenarioBlockTemplateBuild Scenario = "block-template-rebuild"
)

type Benchmark string

const (
	BenchmarkE2E        Benchmark = "e2e"
	BenchmarkThroughput Benchmark = "throughput"
)

type ThroughputMode string

const (
	ThroughputModeTx    ThroughputMode = "tx"
	ThroughputModeBlock ThroughputMode = "block"
)

type MiningMode string

const (
	MiningModeSynthetic MiningMode = "synthetic"
	MiningModeReal      MiningMode = "real"
)

type Topology string

const (
	TopologyLine Topology = "line"
	TopologyMesh Topology = "mesh"
)

type TxOriginSpread string

const (
	// TxOriginOneNode preserves the historical benchmark behavior: one node
	// originates the whole workload and the rest only receive relay traffic.
	TxOriginOneNode TxOriginSpread = "one-node"
	// TxOriginEven round-robins independent transactions across all nodes so
	// relay measurements are not biased toward a single submitter.
	TxOriginEven TxOriginSpread = "even"
)

type RunOptions struct {
	Benchmark              Benchmark
	ThroughputMode         ThroughputMode
	MiningMode             MiningMode
	Scenario               Scenario
	Profile                types.ChainProfile
	NodeCount              int
	Topology               Topology
	TxCount                int
	BatchSize              int
	TxsPerBlock            int
	BlockCount             int
	Duration               time.Duration
	BlockInterval          time.Duration
	TxOriginSpread         TxOriginSpread
	Timeout                time.Duration
	DBRoot                 string
	ProfileDir             string
	SuppressLogs           bool
	SyntheticMining        bool
	SyntheticBlockInterval time.Duration
	FindMaxTPS             bool
	RelayConfirmedBlocks   bool
	SteadyStateBacklog     bool
	ProgressWriter         io.Writer
	ProgressInterval       time.Duration
}

type SuiteOptions struct {
	Profile                types.ChainProfile
	NodeCount              int
	TxCount                int
	BatchSize              int
	TxsPerBlock            int
	BlockCount             int
	Duration               time.Duration
	BlockInterval          time.Duration
	TxOriginSpread         TxOriginSpread
	Timeout                time.Duration
	DBRoot                 string
	ProfileDir             string
	SuppressLogs           bool
	SyntheticMining        bool
	SyntheticBlockInterval time.Duration
}

type ProfilingReport struct {
	Artifacts []profiling.Artifact `json:"artifacts,omitempty"`
}

type Report struct {
	Benchmark                     string          `json:"benchmark"`
	Mode                          string          `json:"mode,omitempty"`
	Mining                        string          `json:"mining,omitempty"`
	Scenario                      string          `json:"scenario"`
	Profile                       string          `json:"profile"`
	NodeCount                     int             `json:"node_count"`
	Topology                      string          `json:"topology,omitempty"`
	TxCount                       int             `json:"tx_count"`
	BatchSize                     int             `json:"batch_size,omitempty"`
	TxsPerBlock                   int             `json:"txs_per_block,omitempty"`
	TargetBlocks                  int             `json:"target_blocks,omitempty"`
	RequestedDurationMS           float64         `json:"requested_duration_ms,omitempty"`
	BlockIntervalMS               float64         `json:"block_interval_ms,omitempty"`
	TxOriginSpread                string          `json:"tx_origin,omitempty"`
	SteadyStateBacklog            bool            `json:"steady_state_backlog,omitempty"`
	SyntheticMining               bool            `json:"synthetic_mining,omitempty"`
	SyntheticBlockIntervalMS      float64         `json:"synthetic_block_interval_ms,omitempty"`
	StartedAt                     time.Time       `json:"started_at"`
	CompletedAt                   time.Time       `json:"completed_at"`
	AdmissionDurationMS           float64         `json:"admission_duration_ms"`
	CompletionDurationMS          float64         `json:"completion_duration_ms"`
	AdmissionTPS                  float64         `json:"admission_tps"`
	CompletionTPS                 float64         `json:"completion_tps"`
	ConfirmedProcessingDurationMS float64         `json:"confirmed_processing_duration_ms,omitempty"`
	ConfirmedProcessingTPS        float64         `json:"confirmed_processing_tps,omitempty"`
	ConfirmedWallDurationMS       float64         `json:"confirmed_wall_duration_ms,omitempty"`
	ConfirmedWallTPS              float64         `json:"confirmed_wall_tps,omitempty"`
	SyntheticIntervalTPS          float64         `json:"synthetic_interval_tps,omitempty"`
	Phases                        PhaseReport     `json:"phases"`
	Metrics                       ScenarioMetrics `json:"metrics,omitempty"`
	Profiling                     ProfilingReport `json:"profiling,omitempty"`
	Environment                   Environment     `json:"environment"`
	Nodes                         []NodeReport    `json:"nodes"`
	Notes                         []string        `json:"notes,omitempty"`
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
	Name                   string            `json:"name"`
	RPCAddr                string            `json:"rpc_addr,omitempty"`
	P2PAddr                string            `json:"p2p_addr,omitempty"`
	PeerCount              int               `json:"peer_count"`
	BlockHeight            uint64            `json:"block_height"`
	HeaderHeight           uint64            `json:"header_height"`
	MempoolCount           int               `json:"mempool_count"`
	SubmittedTxs           int               `json:"submitted_txs,omitempty"`
	BlockSigChecks         uint64            `json:"block_sig_checks,omitempty"`
	BlockSigFallbacks      uint64            `json:"block_sig_fallbacks,omitempty"`
	BlockSigVerifyAvgMS    float64           `json:"block_sig_verify_avg_ms,omitempty"`
	ErlayRounds            uint64            `json:"erlay_rounds,omitempty"`
	ErlayRequestedTxs      uint64            `json:"erlay_requested_txs,omitempty"`
	CompactBlockPlans      uint64            `json:"compact_block_plans,omitempty"`
	CompactBlocksReceived  uint64            `json:"compact_blocks_received,omitempty"`
	CompactBlocksRecovered uint64            `json:"compact_blocks_recovered,omitempty"`
	CompactBlockMissingTxs uint64            `json:"compact_block_missing_txs,omitempty"`
	CompactBlockTxRequests uint64            `json:"compact_block_tx_requests,omitempty"`
	CompactBlockFallbacks  uint64            `json:"compact_block_fallbacks,omitempty"`
	GraphenePlans          uint64            `json:"graphene_extended_plans,omitempty"`
	GrapheneDecodeFails    uint64            `json:"graphene_decode_failures,omitempty"`
	GrapheneRecoveries     uint64            `json:"graphene_extended_recoveries,omitempty"`
	TemplateSelectAvgMS    float64           `json:"template_select_avg_ms,omitempty"`
	TemplateAccAvgMS       float64           `json:"template_accumulate_avg_ms,omitempty"`
	TemplateAssembleAvgMS  float64           `json:"template_assemble_avg_ms,omitempty"`
	CompletionMS           *float64          `json:"completion_ms,omitempty"`
	RelayPeers             []PeerRelayReport `json:"relay_peers,omitempty"`
}

type PhaseReport struct {
	DecodeMS        float64 `json:"decode_ms"`
	ValidateAdmitMS float64 `json:"validate_admit_ms"`
	RelayFanoutMS   float64 `json:"relay_fanout_ms"`
	ConvergenceMS   float64 `json:"convergence_ms"`
}

type ScenarioMetrics struct {
	AcceptedTxs                 int     `json:"accepted_txs,omitempty"`
	ConfirmedTxs                int     `json:"confirmed_txs,omitempty"`
	ConfirmedBlocks             int     `json:"confirmed_blocks,omitempty"`
	TargetTxsPerBlock           int     `json:"target_txs_per_block,omitempty"`
	MaxSustainableTxsPerBlock   int     `json:"max_sustainable_txs_per_block,omitempty"`
	FirstUnsustainedTxsPerBlock int     `json:"first_unsustained_txs_per_block,omitempty"`
	RampSteps                   int     `json:"ramp_steps,omitempty"`
	BlockConvergeAvgMS          float64 `json:"block_converge_avg_ms,omitempty"`
	BlockConvergeP95MS          float64 `json:"block_converge_p95_ms,omitempty"`
	BlockConvergeMaxMS          float64 `json:"block_converge_max_ms,omitempty"`
	BlockRoundAvgMS             float64 `json:"block_round_avg_ms,omitempty"`
	BlockRoundP95MS             float64 `json:"block_round_p95_ms,omitempty"`
	BlockRoundMaxMS             float64 `json:"block_round_max_ms,omitempty"`
	BlockBuildAvgMS             float64 `json:"block_build_avg_ms,omitempty"`
	BlockBuildP95MS             float64 `json:"block_build_p95_ms,omitempty"`
	BlockBuildMaxMS             float64 `json:"block_build_max_ms,omitempty"`
	BlockSealAvgMS              float64 `json:"block_seal_avg_ms,omitempty"`
	BlockSealP95MS              float64 `json:"block_seal_p95_ms,omitempty"`
	BlockSealMaxMS              float64 `json:"block_seal_max_ms,omitempty"`
	MissedIntervals             int     `json:"missed_intervals,omitempty"`
	ScheduleLagAvgMS            float64 `json:"schedule_lag_avg_ms,omitempty"`
	ScheduleLagP95MS            float64 `json:"schedule_lag_p95_ms,omitempty"`
	ScheduleLagMaxMS            float64 `json:"schedule_lag_max_ms,omitempty"`
	FinalMempoolTxs             int     `json:"final_mempool_txs,omitempty"`
	FinalBlockLag               uint64  `json:"final_block_lag,omitempty"`
	FinalHeaderLag              uint64  `json:"final_header_lag,omitempty"`
	OrphanedTxs                 int     `json:"orphaned_txs,omitempty"`
	PromotedOrphans             int     `json:"promoted_orphans,omitempty"`
	MaxOrphanCount              int     `json:"max_orphan_count,omitempty"`
	PackageDepth                int     `json:"package_depth,omitempty"`
	PackageCount                int     `json:"package_count,omitempty"`
	TemplateRebuilds            int     `json:"template_rebuilds,omitempty"`
	TemplateCacheHits           int     `json:"template_cache_hits,omitempty"`
	TemplateFrontier            int     `json:"template_frontier_candidates,omitempty"`
	TemplateRebuildAvgMS        float64 `json:"template_rebuild_avg_ms,omitempty"`
	TemplateRebuildP95MS        float64 `json:"template_rebuild_p95_ms,omitempty"`
	TemplateRebuildMaxMS        float64 `json:"template_rebuild_max_ms,omitempty"`
	TemplateSelectedTxs         int     `json:"template_selected_txs,omitempty"`
	TemplateFullBuilds          int     `json:"template_full_builds,omitempty"`
	TemplateAppendExtends       int     `json:"template_append_extends,omitempty"`
	TemplateNoChangeRefreshes   int     `json:"template_no_change_refreshes,omitempty"`
	TemplateSelectAvgMS         float64 `json:"template_select_avg_ms,omitempty"`
	TemplateSelectP95MS         float64 `json:"template_select_p95_ms,omitempty"`
	TemplateSelectMaxMS         float64 `json:"template_select_max_ms,omitempty"`
	TemplateAccumulateAvgMS     float64 `json:"template_accumulate_avg_ms,omitempty"`
	TemplateAccumulateP95MS     float64 `json:"template_accumulate_p95_ms,omitempty"`
	TemplateAccumulateMaxMS     float64 `json:"template_accumulate_max_ms,omitempty"`
	TemplateAssembleAvgMS       float64 `json:"template_assemble_avg_ms,omitempty"`
	TemplateAssembleP95MS       float64 `json:"template_assemble_p95_ms,omitempty"`
	TemplateAssembleMaxMS       float64 `json:"template_assemble_max_ms,omitempty"`
	DirectRelayPeerCount        int     `json:"direct_relay_peer_count,omitempty"`
	TxBatchMessages             int     `json:"tx_batch_messages,omitempty"`
	TxBatchItems                int     `json:"tx_batch_items,omitempty"`
	TxReconMessages             int     `json:"tx_recon_messages,omitempty"`
	TxReconItems                int     `json:"tx_recon_items,omitempty"`
	TxRequestMessages           int     `json:"tx_request_messages,omitempty"`
	TxRequestItems              int     `json:"tx_request_items,omitempty"`
	TxBatchItemsPerMessage      float64 `json:"tx_batch_items_per_message,omitempty"`
	TxReconItemsPerMessage      float64 `json:"tx_recon_items_per_message,omitempty"`
	TxRequestItemsPerMessage    float64 `json:"tx_request_items_per_message,omitempty"`
	DuplicateTxSuppressed       int     `json:"duplicate_tx_suppressed,omitempty"`
	KnownTxSuppressed           int     `json:"known_tx_suppressed,omitempty"`
	CoalescedTxItems            int     `json:"coalesced_tx_items,omitempty"`
	CoalescedReconItems         int     `json:"coalesced_recon_items,omitempty"`
	WriterStarvationEvents      int     `json:"writer_starvation_events,omitempty"`
	UserCount                   int     `json:"user_count,omitempty"`
	ShortChainTxs               int     `json:"short_chain_txs,omitempty"`
	MultiOutputTxs              int     `json:"multi_output_txs,omitempty"`
}

type PeerRelayReport struct {
	Addr                   string  `json:"addr"`
	Outbound               bool    `json:"outbound"`
	QueueDepth             int     `json:"queue_depth"`
	MaxQueueDepth          int     `json:"max_queue_depth"`
	SentMessages           int     `json:"sent_messages"`
	TxInvItems             int     `json:"tx_inv_items"`
	BlockInvItems          int     `json:"block_inv_items"`
	TxBatchMsgs            int     `json:"tx_batch_messages,omitempty"`
	TxBatchItems           int     `json:"tx_batch_items,omitempty"`
	TxReconMsgs            int     `json:"tx_recon_messages,omitempty"`
	TxReconItems           int     `json:"tx_recon_items,omitempty"`
	TxReqMsgs              int     `json:"tx_request_messages,omitempty"`
	TxReqItems             int     `json:"tx_request_items,omitempty"`
	DuplicateTxSuppressed  int     `json:"duplicate_tx_suppressed,omitempty"`
	KnownTxSuppressed      int     `json:"known_tx_suppressed,omitempty"`
	CoalescedTxItems       int     `json:"coalesced_tx_items,omitempty"`
	CoalescedReconItems    int     `json:"coalesced_recon_items,omitempty"`
	DroppedTxs             int     `json:"dropped_tx_items,omitempty"`
	WriterStarvationEvents int     `json:"writer_starvation_events,omitempty"`
	RelayEvents            int     `json:"relay_events"`
	RelayAvgMS             float64 `json:"relay_avg_ms,omitempty"`
	RelayP95MS             float64 `json:"relay_p95_ms,omitempty"`
	RelayMaxMS             float64 `json:"relay_max_ms,omitempty"`
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
	submitStarted       time.Time
	submitDone          time.Time
	admissionTime       time.Duration
	completed           time.Time
	propagation         map[string]time.Duration
	submittedByNode     map[string]int
	confirmedProcessing time.Duration
	phases              PhaseReport
	metrics             ScenarioMetrics
	extraNotes          []string
}

type progressContextKey struct{}

type progressReporter struct {
	writer       io.Writer
	interval     time.Duration
	started      time.Time
	mu           sync.Mutex
	lastProgress time.Time
}

type submissionOutcome struct {
	submitDone      time.Time
	decodeMS        float64
	validateAdmitMS float64
	submittedByNode map[string]int
	metrics         ScenarioMetrics
}

func DefaultRunOptions() RunOptions {
	return RunOptions{
		Benchmark:              BenchmarkThroughput,
		ThroughputMode:         ThroughputModeTx,
		MiningMode:             MiningModeSynthetic,
		Scenario:               ScenarioP2PRelay,
		Profile:                types.Regtest,
		NodeCount:              5,
		Topology:               TopologyMesh,
		TxCount:                4_096,
		BatchSize:              64,
		TxsPerBlock:            256,
		BlockCount:             20,
		Duration:               60 * time.Second,
		BlockInterval:          15 * time.Second,
		TxOriginSpread:         TxOriginOneNode,
		Timeout:                30 * time.Second,
		SuppressLogs:           true,
		SyntheticBlockInterval: time.Minute,
		ProgressInterval:       5 * time.Second,
	}
}

func newProgressReporter(w io.Writer, interval time.Duration) *progressReporter {
	if w == nil || interval <= 0 {
		return nil
	}
	return &progressReporter{
		writer:   w,
		interval: interval,
		started:  time.Now(),
	}
}

func withProgressReporter(ctx context.Context, reporter *progressReporter) context.Context {
	if reporter == nil {
		return ctx
	}
	return context.WithValue(ctx, progressContextKey{}, reporter)
}

func progressFromContext(ctx context.Context) *progressReporter {
	reporter, _ := ctx.Value(progressContextKey{}).(*progressReporter)
	return reporter
}

func progressEventf(ctx context.Context, format string, args ...interface{}) {
	reporter := progressFromContext(ctx)
	if reporter == nil {
		return
	}
	reporter.eventf(format, args...)
}

func progressHeartbeat(ctx context.Context, phase string, status func() string) {
	reporter := progressFromContext(ctx)
	if reporter == nil || status == nil {
		return
	}
	reporter.heartbeat(phase, status)
}

func (p *progressReporter) eventf(format string, args ...interface{}) {
	p.emit("event", fmt.Sprintf(format, args...), true)
}

func (p *progressReporter) heartbeat(phase string, status func() string) {
	p.emit(phase, status(), false)
}

func (p *progressReporter) emit(phase, message string, force bool) {
	if p == nil || p.writer == nil {
		return
	}
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if !force && !p.lastProgress.IsZero() && now.Sub(p.lastProgress) < p.interval {
		return
	}
	p.lastProgress = now
	elapsed := now.Sub(p.started).Round(time.Second)
	fmt.Fprintf(p.writer, "bench progress elapsed=%s phase=%s %s\n", elapsed, phase, message)
}

func DefaultE2EOptions() RunOptions {
	defaults := DefaultRunOptions()
	defaults.Benchmark = BenchmarkE2E
	defaults.Scenario = ScenarioConfirmedBlocks
	defaults.NodeCount = 5
	defaults.Topology = TopologyMesh
	defaults.TxsPerBlock = 1_024
	defaults.BlockCount = 5
	defaults.TxCount = defaults.TxsPerBlock * defaults.BlockCount
	defaults.MiningMode = MiningModeSynthetic
	defaults.BlockInterval = 30 * time.Second
	defaults.SyntheticMining = true
	defaults.SyntheticBlockInterval = defaults.BlockInterval
	defaults.TxOriginSpread = TxOriginEven
	defaults.RelayConfirmedBlocks = true
	defaults.Timeout = 5 * time.Minute
	return defaults
}

func DefaultThroughputOptions() RunOptions {
	defaults := DefaultRunOptions()
	defaults.Benchmark = BenchmarkThroughput
	defaults.ThroughputMode = ThroughputModeTx
	defaults.Scenario = ScenarioDirectSubmit
	defaults.NodeCount = 1
	defaults.TxCount = 32_768
	defaults.Duration = 60 * time.Second
	defaults.TxsPerBlock = 256
	defaults.BlockCount = 0
	defaults.SyntheticMining = false
	defaults.RelayConfirmedBlocks = false
	defaults.Timeout = 90 * time.Second
	return defaults
}

func DefaultSuiteOptions() SuiteOptions {
	defaults := DefaultE2EOptions()
	return SuiteOptions{
		Profile:                defaults.Profile,
		NodeCount:              defaults.NodeCount,
		TxCount:                defaults.TxCount,
		BatchSize:              defaults.BatchSize,
		TxsPerBlock:            defaults.TxsPerBlock,
		BlockCount:             defaults.BlockCount,
		Duration:               60 * time.Second,
		BlockInterval:          defaults.BlockInterval,
		TxOriginSpread:         defaults.TxOriginSpread,
		Timeout:                8 * time.Minute,
		SuppressLogs:           defaults.SuppressLogs,
		SyntheticMining:        defaults.SyntheticMining,
		SyntheticBlockInterval: defaults.BlockInterval,
	}
}

func RunE2E(ctx context.Context, opts RunOptions) (*Report, error) {
	defaults := DefaultE2EOptions()
	if opts.Benchmark == "" {
		opts.Benchmark = BenchmarkE2E
	}
	if opts.Scenario == "" {
		opts.Scenario = ScenarioConfirmedBlocks
	}
	if opts.NodeCount <= 0 {
		opts.NodeCount = defaults.NodeCount
	}
	if opts.Topology == "" {
		opts.Topology = defaults.Topology
	}
	if opts.TxsPerBlock <= 0 {
		opts.TxsPerBlock = defaults.TxsPerBlock
	}
	if opts.BlockCount <= 0 {
		opts.BlockCount = defaults.BlockCount
	}
	if opts.TxCount <= 0 {
		opts.TxCount = opts.TxsPerBlock * opts.BlockCount
	}
	if opts.BlockInterval <= 0 {
		opts.BlockInterval = defaults.BlockInterval
	}
	if opts.MiningMode == "" {
		opts.MiningMode = defaults.MiningMode
	}
	opts.RelayConfirmedBlocks = true
	opts.SyntheticMining = opts.MiningMode == MiningModeSynthetic
	opts.SyntheticBlockInterval = opts.BlockInterval
	if opts.Timeout <= 0 {
		opts.Timeout = defaults.Timeout
	}
	return Run(ctx, opts)
}

func durationAbs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func RunThroughput(ctx context.Context, opts RunOptions) (*Report, error) {
	defaults := DefaultThroughputOptions()
	if opts.Benchmark == "" {
		opts.Benchmark = BenchmarkThroughput
	}
	if opts.ThroughputMode == "" {
		opts.ThroughputMode = defaults.ThroughputMode
	}
	if opts.Duration <= 0 {
		opts.Duration = defaults.Duration
	}
	switch opts.ThroughputMode {
	case ThroughputModeBlock:
		if opts.Scenario == "" {
			opts.Scenario = ScenarioConfirmedBlocks
		}
		if opts.NodeCount <= 0 {
			opts.NodeCount = 1
		}
		if opts.TxsPerBlock <= 0 {
			opts.TxsPerBlock = defaults.TxsPerBlock
		}
		if opts.TxCount <= 0 {
			opts.TxCount = max(opts.TxsPerBlock*8, defaults.TxCount)
		}
		opts.BlockCount = max(1, (opts.TxCount+opts.TxsPerBlock-1)/opts.TxsPerBlock)
		opts.MiningMode = MiningModeSynthetic
		opts.SyntheticMining = true
		opts.BlockInterval = defaults.BlockInterval
		opts.SyntheticBlockInterval = opts.BlockInterval
		opts.RelayConfirmedBlocks = false
	default:
		if opts.Scenario == "" {
			opts.Scenario = ScenarioDirectSubmit
		}
		if opts.NodeCount <= 0 {
			opts.NodeCount = 1
		}
		if opts.TxCount <= 0 {
			opts.TxCount = defaults.TxCount
		}
		opts.RelayConfirmedBlocks = false
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaults.Timeout
	}
	return Run(ctx, opts)
}

func Run(ctx context.Context, opts RunOptions) (*Report, error) {
	opts = withDefaults(opts)
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	progress := newProgressReporter(opts.ProgressWriter, opts.ProgressInterval)
	ctx = withProgressReporter(ctx, progress)

	restoreLogs := func() {}
	if opts.SuppressLogs {
		restoreLogs = suppressLogs()
	}
	defer restoreLogs()

	runCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	// The benchmark timeout should stop the workload, not tear down live node
	// services underneath an in-flight MineBlocks/submit call. Cluster lifetime is
	// managed explicitly by cleanup so timeouts return errors instead of crashing
	// the process with closed-store races.
	cluster, cleanup, err := openCluster(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	progressEventf(ctx, "cluster opened benchmark=%s mode=%s mining=%s nodes=%d txs=%d blocks=%d interval=%s",
		reportBenchmark(opts), reportMode(opts), reportMining(opts), opts.NodeCount, opts.TxCount, opts.BlockCount, opts.BlockInterval)

	if err := connectCluster(runCtx, cluster, opts.Topology); err != nil {
		return nil, err
	}
	progressEventf(ctx, "cluster connected topology=%s", opts.Topology)
	started := time.Now()
	var outcome workloadOutcome
	var profilingReport ProfilingReport
	if opts.ProfileDir != "" {
		capture, err := profiling.StartCapture(profiling.DefaultCaptureConfig(opts.ProfileDir, slugify(reportFileStem(opts))+"-"))
		if err != nil {
			return nil, err
		}
		outcome, err = executeScenario(runCtx, cluster, opts)
		if err != nil {
			_, _ = capture.Stop()
			return nil, err
		}
		artifacts, stopErr := capture.Stop()
		if stopErr != nil {
			return nil, stopErr
		}
		profilingReport.Artifacts = artifacts
	} else {
		outcome, err = executeScenario(runCtx, cluster, opts)
		if err != nil {
			return nil, err
		}
	}

	// Submit and end-to-end throughput should measure the actual workload window,
	// not earlier cluster warmup such as funding seeding or readiness waits.
	submitStarted := outcome.submitStarted
	if submitStarted.IsZero() {
		submitStarted = started
	}
	admissionElapsed := outcome.admissionTime
	if admissionElapsed <= 0 {
		admissionElapsed = outcome.submitDone.Sub(submitStarted)
	}
	reportTxCount := opts.TxCount
	if opts.Benchmark == BenchmarkThroughput {
		switch reportMode(opts) {
		case string(ThroughputModeBlock):
			if outcome.metrics.ConfirmedTxs > 0 {
				reportTxCount = outcome.metrics.ConfirmedTxs
			}
		default:
			if outcome.metrics.AcceptedTxs > 0 {
				reportTxCount = outcome.metrics.AcceptedTxs
			}
		}
	}
	if opts.FindMaxTPS && outcome.metrics.AcceptedTxs > 0 {
		reportTxCount = outcome.metrics.AcceptedTxs
	}
	report := &Report{
		Benchmark:                string(reportBenchmark(opts)),
		Mode:                     reportMode(opts),
		Mining:                   reportMining(opts),
		Scenario:                 string(opts.Scenario),
		Profile:                  benchmarkClusterProfile(opts).String(),
		NodeCount:                opts.NodeCount,
		Topology:                 reportTopology(opts),
		TxCount:                  reportTxCount,
		BatchSize:                opts.BatchSize,
		TxOriginSpread:           string(opts.TxOriginSpread),
		SteadyStateBacklog:       opts.SteadyStateBacklog,
		SyntheticMining:          opts.SyntheticMining,
		SyntheticBlockIntervalMS: durationMS(opts.SyntheticBlockInterval),
		StartedAt:                started,
		CompletedAt:              outcome.completed,
		AdmissionDurationMS:      durationMS(admissionElapsed),
		CompletionDurationMS:     durationMS(outcome.completed.Sub(submitStarted)),
		AdmissionTPS:             ratePerSecond(reportTxCount, admissionElapsed),
		CompletionTPS:            ratePerSecond(reportTxCount, outcome.completed.Sub(submitStarted)),
		Phases:                   outcome.phases,
		Metrics:                  outcome.metrics,
		Profiling:                profilingReport,
		Environment: Environment{
			GoOS:      runtime.GOOS,
			GoArch:    runtime.GOARCH,
			GoVersion: runtime.Version(),
			NumCPU:    runtime.NumCPU(),
		},
		Nodes: buildNodeReports(cluster, outcome.propagation, outcome.submittedByNode),
		Notes: buildNotes(opts, outcome.extraNotes),
	}
	if report.Benchmark == string(BenchmarkE2E) {
		report.TargetBlocks = opts.BlockCount
		report.BlockIntervalMS = durationMS(opts.BlockInterval)
	}
	if report.Benchmark == string(BenchmarkThroughput) {
		report.RequestedDurationMS = durationMS(opts.Duration)
	}
	if opts.Scenario == ScenarioConfirmedBlocks {
		report.TxsPerBlock = outcome.metrics.TargetTxsPerBlock
		if report.TxsPerBlock == 0 {
			report.TxsPerBlock = opts.TxsPerBlock
		}
		if opts.FindMaxTPS && report.Metrics.MaxSustainableTxsPerBlock > 0 {
			report.TxsPerBlock = report.Metrics.MaxSustainableTxsPerBlock
		} else if opts.FindMaxTPS && report.Metrics.FirstUnsustainedTxsPerBlock > 0 {
			report.TxsPerBlock = report.Metrics.FirstUnsustainedTxsPerBlock
		}
	}
	if report.Metrics.ConfirmedTxs > 0 {
		report.ConfirmedProcessingDurationMS = durationMS(outcome.confirmedProcessing)
		report.ConfirmedProcessingTPS = ratePerSecond(report.Metrics.ConfirmedTxs, outcome.confirmedProcessing)
		report.ConfirmedWallDurationMS = durationMS(outcome.completed.Sub(submitStarted))
		report.ConfirmedWallTPS = ratePerSecond(report.Metrics.ConfirmedTxs, outcome.completed.Sub(submitStarted))
		if opts.SyntheticMining && report.Metrics.ConfirmedBlocks > 0 {
			syntheticTxCount := report.Metrics.ConfirmedTxs
			syntheticBlocks := report.Metrics.ConfirmedBlocks
			if opts.FindMaxTPS && report.Metrics.MaxSustainableTxsPerBlock > 0 {
				syntheticTxCount = report.Metrics.MaxSustainableTxsPerBlock
				syntheticBlocks = 1
			}
			report.SyntheticIntervalTPS = ratePerSecond(
				syntheticTxCount,
				time.Duration(syntheticBlocks)*opts.SyntheticBlockInterval,
			)
		}
	}
	relayMetrics := aggregateRelayMetrics(report.Nodes)
	report.Metrics.TxBatchMessages = relayMetrics.BatchMessages
	report.Metrics.TxBatchItems = relayMetrics.BatchItems
	report.Metrics.TxReconMessages = relayMetrics.ReconMessages
	report.Metrics.TxReconItems = relayMetrics.ReconItems
	report.Metrics.TxRequestMessages = relayMetrics.RequestMessages
	report.Metrics.TxRequestItems = relayMetrics.RequestItems
	report.Metrics.TxBatchItemsPerMessage = ratioOrZero(relayMetrics.BatchItems, relayMetrics.BatchMessages)
	report.Metrics.TxReconItemsPerMessage = ratioOrZero(relayMetrics.ReconItems, relayMetrics.ReconMessages)
	report.Metrics.TxRequestItemsPerMessage = ratioOrZero(relayMetrics.RequestItems, relayMetrics.RequestMessages)
	report.Metrics.DuplicateTxSuppressed = relayMetrics.DuplicateTxSuppressed
	report.Metrics.KnownTxSuppressed = relayMetrics.KnownTxSuppressed
	report.Metrics.CoalescedTxItems = relayMetrics.CoalescedTxItems
	report.Metrics.CoalescedReconItems = relayMetrics.CoalescedReconItems
	report.Metrics.WriterStarvationEvents = relayMetrics.WriterStarvationEvents
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
			"Core suite coverage is intentionally narrow: synthetic and real e2e runs, plus tx and block throughput baselines.",
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
	switch report.Benchmark {
	case string(BenchmarkE2E):
		b.WriteString("# BPU E2E Benchmark\n\n")
	default:
		b.WriteString("# BPU Throughput Benchmark\n\n")
	}
	b.WriteString(fmt.Sprintf("- Benchmark: `%s`\n", report.Benchmark))
	if report.Mode != "" {
		b.WriteString(fmt.Sprintf("- Mode: `%s`\n", report.Mode))
	}
	if report.Mining != "" {
		b.WriteString(fmt.Sprintf("- Mining: `%s`\n", report.Mining))
	}
	b.WriteString(fmt.Sprintf("- Profile: `%s`\n", report.Profile))
	b.WriteString(fmt.Sprintf("- Nodes: `%d`\n", report.NodeCount))
	if report.Topology != "" {
		b.WriteString(fmt.Sprintf("- Topology: `%s`\n", report.Topology))
	}
	b.WriteString(fmt.Sprintf("- Transactions: `%d`\n", report.TxCount))
	if report.BatchSize > 0 {
		b.WriteString(fmt.Sprintf("- Batch size: `%d`\n", report.BatchSize))
	}
	if report.TxsPerBlock > 0 {
		b.WriteString(fmt.Sprintf("- Txs per block: `%d`\n", report.TxsPerBlock))
	}
	if report.TargetBlocks > 0 {
		b.WriteString(fmt.Sprintf("- Blocks: `%d`\n", report.TargetBlocks))
	}
	if report.RequestedDurationMS > 0 {
		b.WriteString(fmt.Sprintf("- Requested duration: `%.0f ms`\n", report.RequestedDurationMS))
	}
	if report.BlockIntervalMS > 0 {
		b.WriteString(fmt.Sprintf("- Block interval: `%.0f ms`\n", report.BlockIntervalMS))
	}
	if report.TxOriginSpread != "" {
		b.WriteString(fmt.Sprintf("- Tx origin: `%s`\n", report.TxOriginSpread))
	}
	if report.SteadyStateBacklog {
		b.WriteString("- Steady-state backlog: `on`\n")
	}
	if report.SyntheticMining {
		b.WriteString(fmt.Sprintf("- Synthetic mining: `on` (`%.0f ms` cadence)\n", report.SyntheticBlockIntervalMS))
	}
	b.WriteString("\n## Throughput\n\n")
	switch report.Benchmark {
	case string(BenchmarkE2E):
		b.WriteString("| Metric | Value |\n")
		b.WriteString("| --- | ---: |\n")
		b.WriteString(fmt.Sprintf("| Admission TPS | %.2f |\n", report.AdmissionTPS))
		b.WriteString(fmt.Sprintf("| E2E TPS | %.2f |\n", report.CompletionTPS))
		if report.ConfirmedProcessingTPS > 0 {
			b.WriteString(fmt.Sprintf("| Confirmed processing TPS | %.2f |\n", report.ConfirmedProcessingTPS))
		}
		if report.ConfirmedWallTPS > 0 {
			b.WriteString(fmt.Sprintf("| Confirmed wall TPS | %.2f |\n", report.ConfirmedWallTPS))
		}
		if report.SyntheticIntervalTPS > 0 {
			b.WriteString(fmt.Sprintf("| Synthetic interval TPS | %.2f |\n", report.SyntheticIntervalTPS))
		}
	default:
		b.WriteString("| Metric | Value |\n")
		b.WriteString("| --- | ---: |\n")
		if report.Mode == string(ThroughputModeBlock) {
			b.WriteString(fmt.Sprintf("| Confirmed apply TPS | %.2f |\n", report.ConfirmedProcessingTPS))
			b.WriteString(fmt.Sprintf("| Wall TPS | %.2f |\n", report.ConfirmedWallTPS))
		} else {
			b.WriteString(fmt.Sprintf("| Tx admission TPS | %.2f |\n", report.AdmissionTPS))
			b.WriteString(fmt.Sprintf("| Completion TPS | %.2f |\n", report.CompletionTPS))
		}
	}
	b.WriteString("\n## Timings\n\n")
	b.WriteString("| Metric | Value |\n")
	b.WriteString("| --- | ---: |\n")
	b.WriteString(fmt.Sprintf("| Admission duration | %.2f ms |\n", report.AdmissionDurationMS))
	b.WriteString(fmt.Sprintf("| Completion duration | %.2f ms |\n", report.CompletionDurationMS))
	b.WriteString(fmt.Sprintf("| Decode | %.2f ms |\n", report.Phases.DecodeMS))
	b.WriteString(fmt.Sprintf("| Validate / Admit | %.2f ms |\n", report.Phases.ValidateAdmitMS))
	b.WriteString(fmt.Sprintf("| Relay fanout | %.2f ms |\n", report.Phases.RelayFanoutMS))
	b.WriteString(fmt.Sprintf("| Convergence | %.2f ms |\n", report.Phases.ConvergenceMS))
	if len(report.Profiling.Artifacts) != 0 {
		b.WriteString("\n## Profiles\n\n")
		for _, artifact := range report.Profiling.Artifacts {
			b.WriteString(fmt.Sprintf("- `%s`: `%s`\n", artifact.Kind, artifact.Path))
		}
	}
	if rows := scenarioMetricRows(report.Metrics); len(rows) != 0 {
		b.WriteString("\n## Workload Metrics\n\n")
		b.WriteString("| Metric | Value |\n")
		b.WriteString("| --- | ---: |\n")
		for _, row := range rows {
			b.WriteString(fmt.Sprintf("| %s | %s |\n", row[0], row[1]))
		}
	}
	b.WriteString("\n## Nodes\n\n")
	b.WriteString("| Name | Peers | Submitted | Block Height | Header Height | Mempool | Complete |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, node := range report.Nodes {
		propagation := "-"
		if node.CompletionMS != nil {
			propagation = fmt.Sprintf("%.2f ms", *node.CompletionMS)
		}
		b.WriteString(fmt.Sprintf("| %s | %d | %d | %d | %d | %d | %s |\n",
			node.Name, node.PeerCount, node.SubmittedTxs, node.BlockHeight, node.HeaderHeight, node.MempoolCount, propagation))
	}
	if rows := signatureMetricRows(report.Nodes); len(rows) != 0 {
		b.WriteString("\n## Block Signature Verification\n\n")
		b.WriteString("| Node | Sig Checks | Batch Fallbacks | Verify Avg |\n")
		b.WriteString("| --- | ---: | ---: | ---: |\n")
		for _, row := range rows {
			b.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n", row[0], row[1], row[2], row[3]))
		}
	}
	if rows := relayMechanismRows(report.Nodes); len(rows) != 0 {
		b.WriteString("\n## Relay Mechanisms\n\n")
		b.WriteString("| Node | Erlay Rounds | Erlay Requested | Compact Plans | Compact Received | Compact Recovered | Compact Missing Txs | Compact Tx Requests | Compact Fallbacks | Graphene Plans | Graphene Decode Fails | Graphene Recoveries |\n")
		b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, row := range rows {
			b.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]))
		}
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
	b.WriteString("# BPU Benchmark Suite\n\n")
	b.WriteString(fmt.Sprintf("- Profile: `%s`\n", report.Profile))
	b.WriteString(fmt.Sprintf("- Cases: `%d`\n", len(report.Cases)))
	b.WriteString(fmt.Sprintf("- Started: `%s`\n", report.StartedAt.UTC().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("- Completed: `%s`\n", report.CompletedAt.UTC().Format(time.RFC3339)))
	b.WriteString("\n## Summary\n\n")
	b.WriteString("| Case | Benchmark | Mode | Mining | Nodes | Tx Count | Admit TPS | Complete TPS | Confirm TPS | Converge |\n")
	b.WriteString("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, suiteCase := range report.Cases {
		mode := suiteCase.Report.Mode
		if mode == "" {
			mode = "-"
		}
		mining := suiteCase.Report.Mining
		if mining == "" {
			mining = "-"
		}
		confirmTPS := suiteCase.Report.ConfirmedProcessingTPS
		if confirmTPS == 0 {
			confirmTPS = suiteCase.Report.ConfirmedWallTPS
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %d | %d | %.2f | %.2f | %.2f | %.2f ms |\n",
			suiteCase.Name,
			suiteCase.Report.Benchmark,
			mode,
			mining,
			suiteCase.Report.NodeCount,
			suiteCase.Report.TxCount,
			suiteCase.Report.AdmissionTPS,
			suiteCase.Report.CompletionTPS,
			confirmTPS,
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
		fmt.Sprintf("Benchmark   %s%s%s", report.Benchmark, asciiModeSuffix(report.Mode), asciiMiningSuffix(report.Mining)),
		fmt.Sprintf("Profile     %s", report.Profile),
		fmt.Sprintf("Layout      %d nodes%s", report.NodeCount, asciiTopologySuffix(report.Topology)),
		fmt.Sprintf("Workload    %d txs%s%s%s%s", report.TxCount, asciiBatchSuffix(report.BatchSize), asciiBlockTargetSuffix(report.TxsPerBlock), asciiTxOriginSuffix(report.TxOriginSpread), asciiBacklogSuffix(report.SteadyStateBacklog)),
	}
	switch report.Benchmark {
	case string(BenchmarkE2E):
		lines = append(lines,
			fmt.Sprintf("Cadence     %.0f ms interval / %d blocks", report.BlockIntervalMS, report.TargetBlocks),
			fmt.Sprintf("Throughput  admission %.2f tx/s | e2e %.2f tx/s", report.AdmissionTPS, report.CompletionTPS),
			fmt.Sprintf("Confirm     processing %.2f tx/s | wall %.2f tx/s", report.ConfirmedProcessingTPS, report.ConfirmedWallTPS),
		)
		if report.SyntheticIntervalTPS > 0 {
			lines = append(lines, fmt.Sprintf("Target TPS  synthetic %.2f tx/s", report.SyntheticIntervalTPS))
		}
	default:
		if report.Mode == string(ThroughputModeBlock) {
			lines = append(lines, fmt.Sprintf("Throughput  confirmed apply %.2f tx/s | wall %.2f tx/s", report.ConfirmedProcessingTPS, report.ConfirmedWallTPS))
		} else {
			lines = append(lines, fmt.Sprintf("Throughput  tx admission %.2f tx/s | completion %.2f tx/s", report.AdmissionTPS, report.CompletionTPS))
		}
		if report.RequestedDurationMS > 0 {
			lines = append(lines, fmt.Sprintf("Window      requested %.0f ms | actual %.2f ms", report.RequestedDurationMS, report.CompletionDurationMS))
		}
	}
	lines = append(lines, fmt.Sprintf("Phases      decode %.2f ms | admit %.2f ms | fanout %.2f ms | converge %.2f ms",
		report.Phases.DecodeMS,
		report.Phases.ValidateAdmitMS,
		report.Phases.RelayFanoutMS,
		report.Phases.ConvergenceMS,
	))
	if len(report.Profiling.Artifacts) != 0 {
		lines = append(lines, fmt.Sprintf("Profiles    %d artifacts in %s", len(report.Profiling.Artifacts), filepath.Dir(report.Profiling.Artifacts[0].Path)))
	}
	for _, row := range scenarioMetricRows(report.Metrics) {
		lines = append(lines, fmt.Sprintf("Metric      %s: %s", row[0], row[1]))
	}
	if sigLine := asciiSignatureSummary(report.Nodes); sigLine != "" {
		lines = append(lines, sigLine)
	}
	if slowest := slowestNode(report.Nodes); slowest != "" {
		lines = append(lines, slowest)
	}
	if hottest := hottestPeer(flattenRelayPeers(report.Nodes)); hottest != "" {
		lines = append(lines, hottest)
	}
	return renderASCIIBox(asciiSummaryTitle(report), lines)
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
	logger, err := logging.NewLogger(io.Discard, logging.Config{Format: "text"})
	if err != nil {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	} else {
		slog.SetDefault(logger)
	}
	return func() {
		slog.SetDefault(previous)
	}
}

func withDefaults(opts RunOptions) RunOptions {
	defaults := DefaultRunOptions()
	if opts.Benchmark == "" {
		switch opts.Scenario {
		case ScenarioConfirmedBlocks:
			opts.Benchmark = BenchmarkE2E
		default:
			opts.Benchmark = defaults.Benchmark
		}
	}
	if (opts.Benchmark == BenchmarkE2E || opts.Scenario == ScenarioConfirmedBlocks) && !opts.SyntheticMining && opts.MiningMode == "" {
		opts.SyntheticMining = true
	}
	if opts.ThroughputMode == "" {
		opts.ThroughputMode = defaults.ThroughputMode
	}
	if opts.MiningMode == "" {
		if opts.SyntheticMining {
			opts.MiningMode = MiningModeSynthetic
		} else {
			opts.MiningMode = MiningModeReal
		}
	}
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
	if opts.TxsPerBlock <= 0 {
		opts.TxsPerBlock = defaults.TxsPerBlock
	}
	if opts.BlockCount <= 0 && (opts.Benchmark == BenchmarkE2E || opts.Scenario == ScenarioConfirmedBlocks) {
		opts.BlockCount = defaults.BlockCount
	}
	if opts.Duration <= 0 {
		opts.Duration = defaults.Duration
	}
	if opts.BlockInterval <= 0 && (opts.Benchmark == BenchmarkE2E || opts.Scenario == ScenarioConfirmedBlocks) {
		opts.BlockInterval = defaults.BlockInterval
	}
	if opts.TxOriginSpread == "" {
		opts.TxOriginSpread = defaults.TxOriginSpread
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaults.Timeout
	}
	if opts.SyntheticBlockInterval <= 0 {
		opts.SyntheticBlockInterval = defaults.SyntheticBlockInterval
	}
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = defaults.ProgressInterval
	}
	if opts.Benchmark == BenchmarkE2E && opts.Scenario == ScenarioConfirmedBlocks && opts.TxCount <= 0 && opts.TxsPerBlock > 0 && opts.BlockCount > 0 {
		opts.TxCount = opts.TxsPerBlock * opts.BlockCount
	}
	if opts.Benchmark == BenchmarkE2E && opts.BlockInterval > 0 && opts.SyntheticBlockInterval <= 0 {
		opts.SyntheticBlockInterval = opts.BlockInterval
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
	if opts.TxsPerBlock <= 0 {
		opts.TxsPerBlock = defaults.TxsPerBlock
	}
	if opts.BlockCount <= 0 {
		opts.BlockCount = defaults.BlockCount
	}
	if opts.Duration <= 0 {
		opts.Duration = defaults.Duration
	}
	if opts.BlockInterval <= 0 {
		opts.BlockInterval = defaults.BlockInterval
	}
	if opts.TxOriginSpread == "" {
		opts.TxOriginSpread = defaults.TxOriginSpread
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaults.Timeout
	}
	if opts.SyntheticBlockInterval <= 0 {
		opts.SyntheticBlockInterval = defaults.SyntheticBlockInterval
	}
	return opts
}

func validateOptions(opts RunOptions) error {
	switch opts.Benchmark {
	case "", BenchmarkE2E, BenchmarkThroughput:
	default:
		return fmt.Errorf("unsupported benchmark: %s", opts.Benchmark)
	}
	switch opts.Scenario {
	case ScenarioDirectSubmit, ScenarioRPCBatch, ScenarioP2PRelay, ScenarioConfirmedBlocks, ScenarioUserMix, ScenarioChainedPackages, ScenarioOrphanStorm, ScenarioBlockTemplateBuild:
	default:
		return fmt.Errorf("unsupported scenario: %s", opts.Scenario)
	}
	switch opts.ThroughputMode {
	case "", ThroughputModeTx, ThroughputModeBlock:
	default:
		return fmt.Errorf("unsupported throughput mode: %s", opts.ThroughputMode)
	}
	switch opts.MiningMode {
	case "", MiningModeSynthetic, MiningModeReal:
	default:
		return fmt.Errorf("unsupported mining mode: %s", opts.MiningMode)
	}
	switch opts.TxOriginSpread {
	case TxOriginOneNode, TxOriginEven:
	default:
		return fmt.Errorf("unsupported tx origin spread: %s", opts.TxOriginSpread)
	}
	if !opts.Profile.IsRegtestLike() {
		return fmt.Errorf("benchmark suite currently supports regtest-style profiles only")
	}
	if opts.Scenario == ScenarioP2PRelay && opts.NodeCount < 2 {
		return fmt.Errorf("p2p-relay requires at least 2 nodes")
	}
	if opts.Scenario == ScenarioConfirmedBlocks && opts.NodeCount < 1 {
		return fmt.Errorf("confirmed-blocks requires at least 1 node")
	}
	if opts.Scenario != ScenarioP2PRelay && opts.Scenario != ScenarioConfirmedBlocks && opts.NodeCount != 1 {
		return fmt.Errorf("%s requires exactly 1 node", opts.Scenario)
	}
	if opts.Scenario == ScenarioConfirmedBlocks && opts.TxsPerBlock <= 0 {
		return fmt.Errorf("confirmed-blocks requires txs_per_block > 0")
	}
	if opts.Benchmark == BenchmarkE2E && opts.BlockCount <= 0 {
		return fmt.Errorf("e2e requires block_count > 0")
	}
	if opts.Benchmark == BenchmarkE2E && opts.BlockInterval <= 0 {
		return fmt.Errorf("e2e requires block_interval > 0")
	}
	if opts.Benchmark == BenchmarkThroughput && opts.Duration <= 0 {
		return fmt.Errorf("throughput requires duration > 0")
	}
	if opts.SyntheticMining && opts.Scenario != ScenarioConfirmedBlocks {
		return fmt.Errorf("synthetic mining is only supported for confirmed-blocks")
	}
	if opts.SyntheticMining && opts.SyntheticBlockInterval <= 0 {
		return fmt.Errorf("synthetic mining requires synthetic_block_interval > 0")
	}
	if opts.FindMaxTPS && (!opts.SyntheticMining || opts.Scenario != ScenarioConfirmedBlocks) {
		return fmt.Errorf("find_max_tps requires confirmed-blocks with synthetic mining enabled")
	}
	if opts.SteadyStateBacklog && opts.Scenario != ScenarioConfirmedBlocks {
		return fmt.Errorf("steady-state backlog requires confirmed-blocks")
	}
	switch opts.Topology {
	case TopologyLine, TopologyMesh:
	default:
		return fmt.Errorf("unsupported topology: %s", opts.Topology)
	}
	if opts.Scenario == ScenarioConfirmedBlocks && opts.TxsPerBlock > opts.TxCount {
		return fmt.Errorf("txs_per_block %d exceeds tx count %d", opts.TxsPerBlock, opts.TxCount)
	}
	return nil
}

type suiteCase struct {
	Name    string
	Options RunOptions
}

func suiteCases(opts SuiteOptions) []suiteCase {
	blockTxsPerBlock := min(opts.TxsPerBlock, opts.TxCount)
	if blockTxsPerBlock <= 0 {
		blockTxsPerBlock = max(1, min(opts.BatchSize, opts.TxCount))
	}
	e2eTxCount := blockTxsPerBlock * max(1, opts.BlockCount)
	return []suiteCase{
		{
			Name: "e2e-synthetic",
			Options: RunOptions{
				Benchmark:              BenchmarkE2E,
				Scenario:               ScenarioConfirmedBlocks,
				Profile:                opts.Profile,
				NodeCount:              opts.NodeCount,
				Topology:               TopologyMesh,
				TxCount:                e2eTxCount,
				BatchSize:              opts.BatchSize,
				TxsPerBlock:            blockTxsPerBlock,
				BlockCount:             opts.BlockCount,
				BlockInterval:          opts.BlockInterval,
				TxOriginSpread:         opts.TxOriginSpread,
				Timeout:                opts.Timeout,
				DBRoot:                 suiteDBRoot(opts.DBRoot, "e2e-synthetic"),
				ProfileDir:             suiteProfileRoot(opts.ProfileDir, "e2e-synthetic"),
				SuppressLogs:           opts.SuppressLogs,
				MiningMode:             MiningModeSynthetic,
				SyntheticMining:        true,
				SyntheticBlockInterval: opts.BlockInterval,
				RelayConfirmedBlocks:   true,
			},
		},
		{
			Name: "e2e-real",
			Options: RunOptions{
				Benchmark:            BenchmarkE2E,
				Scenario:             ScenarioConfirmedBlocks,
				Profile:              opts.Profile,
				NodeCount:            opts.NodeCount,
				Topology:             TopologyMesh,
				TxCount:              e2eTxCount,
				BatchSize:            opts.BatchSize,
				TxsPerBlock:          blockTxsPerBlock,
				BlockCount:           opts.BlockCount,
				BlockInterval:        opts.BlockInterval,
				TxOriginSpread:       opts.TxOriginSpread,
				Timeout:              opts.Timeout,
				DBRoot:               suiteDBRoot(opts.DBRoot, "e2e-real"),
				ProfileDir:           suiteProfileRoot(opts.ProfileDir, "e2e-real"),
				SuppressLogs:         opts.SuppressLogs,
				MiningMode:           MiningModeReal,
				RelayConfirmedBlocks: true,
			},
		},
		{
			Name: "throughput-tx",
			Options: RunOptions{
				Benchmark:              BenchmarkThroughput,
				ThroughputMode:         ThroughputModeTx,
				Scenario:               ScenarioDirectSubmit,
				Profile:                opts.Profile,
				NodeCount:              1,
				TxCount:                opts.TxCount,
				BatchSize:              opts.BatchSize,
				Duration:               opts.Duration,
				TxOriginSpread:         opts.TxOriginSpread,
				Timeout:                opts.Timeout,
				DBRoot:                 suiteDBRoot(opts.DBRoot, "throughput-tx"),
				ProfileDir:             suiteProfileRoot(opts.ProfileDir, "throughput-tx"),
				SuppressLogs:           opts.SuppressLogs,
				SyntheticBlockInterval: opts.BlockInterval,
			},
		},
		{
			Name: "throughput-block",
			Options: RunOptions{
				Benchmark:              BenchmarkThroughput,
				ThroughputMode:         ThroughputModeBlock,
				Scenario:               ScenarioConfirmedBlocks,
				Profile:                opts.Profile,
				NodeCount:              1,
				TxCount:                max(opts.TxCount, blockTxsPerBlock*8),
				BatchSize:              opts.BatchSize,
				TxsPerBlock:            blockTxsPerBlock,
				BlockCount:             max(1, (max(opts.TxCount, blockTxsPerBlock*8)+blockTxsPerBlock-1)/blockTxsPerBlock),
				Duration:               opts.Duration,
				BlockInterval:          opts.BlockInterval,
				TxOriginSpread:         opts.TxOriginSpread,
				Timeout:                opts.Timeout,
				DBRoot:                 suiteDBRoot(opts.DBRoot, "throughput-block"),
				ProfileDir:             suiteProfileRoot(opts.ProfileDir, "throughput-block"),
				SuppressLogs:           opts.SuppressLogs,
				MiningMode:             MiningModeSynthetic,
				SyntheticMining:        true,
				SyntheticBlockInterval: opts.BlockInterval,
				RelayConfirmedBlocks:   false,
			},
		},
	}
}

func suiteProfileRoot(root, name string) string {
	if strings.TrimSpace(root) == "" {
		return ""
	}
	return filepath.Join(root, slugify(name))
}

func openCluster(ctx context.Context, opts RunOptions) ([]*clusterNode, func(), error) {
	clusterProfile := benchmarkClusterProfile(opts)
	if clusterProfile == types.BenchNet {
		consensus.SetBenchNetParams(benchNetParamsForRun(opts))
	}
	genesis, err := loadBenchmarkGenesis(clusterProfile)
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
			if benchNode.errCh != nil {
				select {
				case <-benchNode.errCh:
				case <-time.After(2 * time.Second):
					if benchNode.svc != nil {
						_ = benchNode.svc.Close()
					}
					select {
					case <-benchNode.errCh:
					case <-time.After(2 * time.Second):
					}
				}
			} else if benchNode.svc != nil {
				_ = benchNode.svc.Close()
			}
		}
		if opts.DBRoot == "" {
			_ = os.RemoveAll(dbRoot)
		}
	}

	maxOutboundPeers := max(16, opts.NodeCount*4)
	if opts.NodeCount > 1 && (opts.Scenario == ScenarioP2PRelay || opts.Scenario == ScenarioConfirmedBlocks) {
		// Multi-node benchmark topologies are wired explicitly by connectCluster.
		// Disable background outbound refill so the benchmark measures the chosen
		// line/mesh graph instead of a second auto-connected overlay.
		maxOutboundPeers = 0
	}

	for i := 0; i < opts.NodeCount; i++ {
		p2pAddr, err := reserveLoopbackAddr()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		rpcAddr := ""
		authToken := ""
		if (opts.Scenario == ScenarioRPCBatch && i == 0) || opts.Scenario == ScenarioConfirmedBlocks {
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
			Profile:          clusterProfile,
			DBPath:           dbPath,
			RPCAddr:          rpcAddr,
			RPCAuthToken:     authToken,
			RPCReadTimeout:   5 * time.Second,
			RPCWriteTimeout:  5 * time.Second,
			RPCHeaderTimeout: 2 * time.Second,
			RPCMaxBodyBytes:  8 << 20,
			P2PAddr:          p2pAddr,
			MaxInboundPeers:  max(32, opts.NodeCount*4),
			MaxOutboundPeers: maxOutboundPeers,
			HandshakeTimeout: 5 * time.Second,
			// Confirmed-block benchmarks need the same large-message headroom as a
			// normal node; otherwise higher tx-per-block cases can fail at the
			// transport ceiling before block validation or relay is actually tested.
			StallTimeout:       15 * time.Second,
			MaxMessageBytes:    64_000_000,
			MinRelayFeePerByte: 1,
			MaxTxSize:          mempoolCfg.maxTxSize,
			MaxAncestors:       mempoolCfg.maxAncestors,
			MaxDescendants:     mempoolCfg.maxDescendants,
			MaxOrphans:         mempoolCfg.maxOrphans,
			MinerPubKey:        pubKeyForSeed(byte(i + 1)),
			GenesisFixture:     benchmarkGenesisLabel(clusterProfile),
			// Benchmarks wire an explicit line/mesh topology and need that graph to
			// stay fixed while measuring relay and block propagation. Letting normal
			// addr gossip and known-peer restart logic mutate the peer set makes the
			// benchmark report a moving target instead of the requested topology.
			StaticPeerTopology: opts.NodeCount > 1 && (opts.Scenario == ScenarioP2PRelay || opts.Scenario == ScenarioConfirmedBlocks),
			SyntheticMining:    opts.SyntheticMining,
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

func executeScenario(ctx context.Context, cluster []*clusterNode, opts RunOptions) (workloadOutcome, error) {
	submitter := cluster[0]
	submitters := benchmarkSubmitters(cluster, opts)
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
		return runSubmissionScenario(ctx, cluster, submitters, opts, txs)
	case ScenarioConfirmedBlocks:
		if opts.FindMaxTPS {
			lanes, err := seedSpendableLanes(submitter.svc, opts.TxCount, 2)
			if err != nil {
				return workloadOutcome{}, err
			}
			if err := waitForHeights(ctx, cluster, seedReadyHeight); err != nil {
				return workloadOutcome{}, err
			}
			if _, err := waitForMempoolCounts(ctx, cluster, 0, time.Time{}); err != nil {
				return workloadOutcome{}, err
			}
			return runConfirmedBlocksMaxRampScenario(ctx, cluster, submitter, submitters, opts, lanes)
		}
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
		if opts.SteadyStateBacklog {
			return runConfirmedBlocksBacklogScenario(ctx, cluster, submitter, submitters, opts, txs)
		}
		return runConfirmedBlocksScenario(ctx, cluster, submitter, submitters, opts, txs)
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
		outcome, err := runSubmissionScenario(ctx, cluster, submitters, opts, txs)
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
		outcome, err := runDirectSubmitSequence(ctx, cluster, submitter, txs)
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
		outcome, err := runDirectSubmitSequence(ctx, cluster, submitter, txs)
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
		outcome, err := runTemplateRebuildScenario(ctx, cluster, submitter, txs, opts.BatchSize)
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

func benchmarkSubmitters(cluster []*clusterNode, opts RunOptions) []*clusterNode {
	if len(cluster) == 0 {
		return nil
	}
	if opts.TxOriginSpread == TxOriginEven && opts.NodeCount > 1 &&
		(opts.Scenario == ScenarioP2PRelay || opts.Scenario == ScenarioConfirmedBlocks) {
		return cluster
	}
	return []*clusterNode{cluster[0]}
}

func runConfirmedBlocksBacklogScenario(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, submitters []*clusterNode, opts RunOptions, txs []types.Transaction) (workloadOutcome, error) {
	targetTxsPerBlock := min(opts.TxsPerBlock, len(txs))
	if targetTxsPerBlock <= 0 {
		targetTxsPerBlock = len(txs)
	}
	initialTargetTxsPerBlock := targetTxsPerBlock
	metrics := ScenarioMetrics{
		TargetTxsPerBlock: targetTxsPerBlock,
	}
	blockCapacity := targetTxsPerBlock * max(1, opts.BlockCount)
	preloadTxs := txs
	if len(preloadTxs) > blockCapacity {
		preloadTxs = preloadTxs[:blockCapacity]
	}
	submittedByNode := make(map[string]int, len(submitters))
	progressEventf(ctx, "steady-state preload txs=%d target_txs_per_block=%d", len(preloadTxs), targetTxsPerBlock)
	admitStarted := time.Now()
	acceptedTxs, _, err := submitConfirmedWave(ctx, submitters, 0, preloadTxs, opts.BatchSize, submittedByNode)
	if err != nil {
		return workloadOutcome{}, err
	}
	admissionElapsed := time.Since(admitStarted)
	metrics.AcceptedTxs = acceptedTxs
	if acceptedTxs == 0 {
		return workloadOutcome{}, errors.New("steady-state backlog accepted no transactions")
	}
	if _, err := waitForMempoolCounts(ctx, cluster, acceptedTxs, time.Time{}); err != nil {
		return workloadOutcome{}, err
	}
	progressEventf(ctx, "steady-state backlog converged txs=%d", acceptedTxs)

	submitStarted := time.Now()
	currentHeight := submitter.svc.BlockHeight()
	expectedMempoolTxs := acceptedTxs
	blockConvergenceSamples := make([]float64, 0, opts.BlockCount)
	blockRoundSamples := make([]float64, 0, opts.BlockCount)
	blockBuildSamples := make([]float64, 0, opts.BlockCount)
	blockSealSamples := make([]float64, 0, opts.BlockCount)
	scheduleLagSamples := make([]float64, 0, opts.BlockCount)
	const syntheticCadenceMissThreshold = 5 * time.Millisecond
	var confirmedProcessing time.Duration

	for metrics.ConfirmedBlocks < opts.BlockCount && expectedMempoolTxs > 0 {
		roundStarted := time.Now()
		nextBlock := metrics.ConfirmedBlocks + 1
		scheduledAt := syntheticE2EBlockTime(submitStarted, opts, nextBlock)
		preparedBlock, err := buildConfirmedBenchmarkBlock(ctx, submitter.svc, opts)
		if err != nil {
			return workloadOutcome{}, err
		}
		if !scheduledAt.IsZero() {
			if err := waitUntil(ctx, scheduledAt); err != nil {
				return workloadOutcome{}, err
			}
			if lag := time.Since(scheduledAt); lag > 0 {
				scheduleLagSamples = append(scheduleLagSamples, durationMS(lag))
				if lag > syntheticCadenceMissThreshold {
					metrics.MissedIntervals++
				}
			}
		}
		blockStarted := time.Now()
		accepted, err := callWithContext(ctx, func() (acceptedBenchmarkBlock, error) {
			hash, height, err := submitter.svc.AcceptLocalBenchmarkBlock(preparedBlock.block)
			return acceptedBenchmarkBlock{hash: hash, height: height}, err
		})
		if err != nil {
			return workloadOutcome{}, err
		}
		if hex.EncodeToString(accepted.hash[:]) != preparedBlock.hashHex {
			return workloadOutcome{}, fmt.Errorf("accepted block hash mismatch: got %x want %s", accepted.hash, preparedBlock.hashHex)
		}
		if !opts.RelayConfirmedBlocks {
			if err := submitBlockToFollowers(ctx, cluster[1:], preparedBlock.block); err != nil {
				return workloadOutcome{}, err
			}
		}
		currentHeight = accepted.height
		blockProcessing := time.Since(blockStarted)
		if !(opts.Benchmark == BenchmarkE2E && opts.SyntheticMining) {
			heightReached, err := waitForHeightsReached(ctx, cluster, currentHeight, blockStarted)
			if err != nil {
				return workloadOutcome{}, err
			}
			blockProcessing = maxDuration(heightReached)
		}
		confirmedProcessing += blockProcessing
		blockBuildSamples = append(blockBuildSamples, durationMS(preparedBlock.buildDuration))
		blockSealSamples = append(blockSealSamples, durationMS(preparedBlock.sealDuration))
		blockConvergenceSamples = append(blockConvergenceSamples, durationMS(blockProcessing))
		blockRoundSamples = append(blockRoundSamples, durationMS(time.Since(roundStarted)))
		confirmedInBlock := max(0, len(preparedBlock.block.Txs)-1)
		metrics.ConfirmedTxs += confirmedInBlock
		expectedMempoolTxs -= confirmedInBlock
		if expectedMempoolTxs < 0 {
			expectedMempoolTxs = 0
		}
		metrics.ConfirmedBlocks++
		progressEventf(ctx, "block=%d accepted height=%d hash=%s max_mempool=%d",
			metrics.ConfirmedBlocks,
			accepted.height,
			shortHash(preparedBlock.hashHex),
			maxNodeMempoolCount(cluster),
		)
	}

	if _, err := waitForHeightsReached(ctx, cluster, currentHeight, submitStarted); err != nil {
		return workloadOutcome{}, err
	}
	completion, err := waitForMempoolCounts(ctx, cluster, expectedMempoolTxs, submitStarted)
	if err != nil {
		return workloadOutcome{}, err
	}
	if len(blockConvergenceSamples) > 0 {
		metrics.BlockConvergeAvgMS = averageFloat64(blockConvergenceSamples)
		metrics.BlockConvergeP95MS = percentileFloat64(blockConvergenceSamples, 95)
		metrics.BlockConvergeMaxMS = maxFloat64(blockConvergenceSamples)
	}
	if len(blockRoundSamples) > 0 {
		metrics.BlockRoundAvgMS = averageFloat64(blockRoundSamples)
		metrics.BlockRoundP95MS = percentileFloat64(blockRoundSamples, 95)
		metrics.BlockRoundMaxMS = maxFloat64(blockRoundSamples)
	}
	if len(blockBuildSamples) > 0 {
		metrics.BlockBuildAvgMS = averageFloat64(blockBuildSamples)
		metrics.BlockBuildP95MS = percentileFloat64(blockBuildSamples, 95)
		metrics.BlockBuildMaxMS = maxFloat64(blockBuildSamples)
	}
	if len(blockSealSamples) > 0 {
		metrics.BlockSealAvgMS = averageFloat64(blockSealSamples)
		metrics.BlockSealP95MS = percentileFloat64(blockSealSamples, 95)
		metrics.BlockSealMaxMS = maxFloat64(blockSealSamples)
	}
	if len(scheduleLagSamples) > 0 {
		metrics.ScheduleLagAvgMS = averageFloat64(scheduleLagSamples)
		metrics.ScheduleLagP95MS = percentileFloat64(scheduleLagSamples, 95)
		metrics.ScheduleLagMaxMS = maxFloat64(scheduleLagSamples)
	}
	applyTemplateDiagnostics(&metrics, submitter.svc)
	metrics.FinalMempoolTxs = maxNodeMempoolCount(cluster)
	metrics.FinalBlockLag, metrics.FinalHeaderLag = maxNodeChainLag(cluster, currentHeight)
	completed := time.Now()
	extraNotes := []string{
		fmt.Sprintf("Steady-state backlog preloads and converges %d accepted transactions before the measured block cadence starts.", acceptedTxs),
		fmt.Sprintf("Confirmed-blocks runs then seal one block every %s and counts only transactions actually included in accepted blocks. The starting block target was %d txs.", opts.SyntheticBlockInterval, initialTargetTxsPerBlock),
	}
	return workloadOutcome{
		submitStarted:       submitStarted,
		submitDone:          admitStarted.Add(admissionElapsed),
		admissionTime:       admissionElapsed,
		completed:           completed,
		propagation:         completion,
		submittedByNode:     submittedByNode,
		confirmedProcessing: confirmedProcessing,
		phases: PhaseReport{
			ValidateAdmitMS: durationMS(admissionElapsed),
			ConvergenceMS:   durationMS(maxDuration(completion)),
		},
		metrics:    metrics,
		extraNotes: extraNotes,
	}, nil
}

func runConfirmedBlocksScenario(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, submitters []*clusterNode, opts RunOptions, txs []types.Transaction) (workloadOutcome, error) {
	submitStarted := time.Now()
	step := max(1, opts.BatchSize)
	targetTxsPerBlock := min(opts.TxsPerBlock, len(txs))
	if targetTxsPerBlock <= 0 {
		targetTxsPerBlock = len(txs)
	}
	targetTxsPerBlock = min(len(txs), alignUp(targetTxsPerBlock, step))
	initialTargetTxsPerBlock := targetTxsPerBlock
	metrics := ScenarioMetrics{
		TargetTxsPerBlock: targetTxsPerBlock,
	}
	blockConvergenceSamples := make([]float64, 0, (len(txs)+targetTxsPerBlock-1)/targetTxsPerBlock)
	blockRoundSamples := make([]float64, 0, cap(blockConvergenceSamples))
	blockBuildSamples := make([]float64, 0, cap(blockConvergenceSamples))
	blockSealSamples := make([]float64, 0, cap(blockConvergenceSamples))
	scheduleLagSamples := make([]float64, 0, cap(blockConvergenceSamples))
	const syntheticCadenceMissThreshold = 5 * time.Millisecond
	var admissionElapsed time.Duration
	var confirmedProcessing time.Duration
	submittedByNode := make(map[string]int, len(submitters))
	currentHeight := submitter.svc.BlockHeight()
	submitterOffset := 0
	rampTried := make([]int, 0, cap(blockConvergenceSamples))
	expectedMempoolTxs := 0
	for start := 0; start < len(txs); {
		if opts.Benchmark == BenchmarkThroughput && opts.Duration > 0 && time.Since(submitStarted) >= opts.Duration {
			break
		}
		remaining := len(txs) - start
		waveTarget := min(metrics.TargetTxsPerBlock, remaining)
		if waveTarget <= 0 {
			break
		}
		roundStarted := time.Now()
		end := start + waveTarget
		wave := txs[start:end]
		progressEventf(ctx, "round=%d/%d target_txs=%d remaining=%d", metrics.ConfirmedBlocks+1, max(1, opts.BlockCount), waveTarget, remaining)
		// Keep each round bounded so the run spans multiple confirmed blocks
		// instead of collapsing into a single oversized mempool wave.
		admitStarted := time.Now()
		acceptedTxs, nextOffset, submitErr := submitConfirmedWave(ctx, submitters, submitterOffset, wave, opts.BatchSize, submittedByNode)
		if submitErr != nil {
			return workloadOutcome{}, submitErr
		}
		metrics.AcceptedTxs += acceptedTxs
		expectedMempoolTxs += acceptedTxs
		submitterOffset = nextOffset
		admissionElapsed += time.Since(admitStarted)
		scheduledAt := syntheticE2EBlockTime(submitStarted, opts, metrics.ConfirmedBlocks+1)
		if scheduledAt.IsZero() {
			if _, err := waitForMempoolCounts(ctx, []*clusterNode{submitter}, expectedMempoolTxs, time.Time{}); err != nil {
				return workloadOutcome{}, err
			}
		} else {
			if _, _, err := waitForMempoolCountsUntil(ctx, []*clusterNode{submitter}, expectedMempoolTxs, time.Time{}, scheduledAt); err != nil {
				return workloadOutcome{}, err
			}
		}
		for {
			preparedBlock, err := buildConfirmedBenchmarkBlock(ctx, submitter.svc, opts)
			if err != nil {
				return workloadOutcome{}, err
			}
			if !scheduledAt.IsZero() {
				if err := waitUntil(ctx, scheduledAt); err != nil {
					return workloadOutcome{}, err
				}
				if lag := time.Since(scheduledAt); lag > 0 {
					scheduleLagSamples = append(scheduleLagSamples, durationMS(lag))
					if lag > syntheticCadenceMissThreshold {
						metrics.MissedIntervals++
					}
				}
			}
			blockStarted := time.Now()
			accepted, err := callWithContext(ctx, func() (acceptedBenchmarkBlock, error) {
				hash, height, err := submitter.svc.AcceptLocalBenchmarkBlock(preparedBlock.block)
				return acceptedBenchmarkBlock{hash: hash, height: height}, err
			})
			if err != nil {
				if strings.Contains(err.Error(), "stale template") {
					continue
				}
				return workloadOutcome{}, err
			}
			if hex.EncodeToString(accepted.hash[:]) != preparedBlock.hashHex {
				return workloadOutcome{}, fmt.Errorf("accepted block hash mismatch: got %x want %s", accepted.hash, preparedBlock.hashHex)
			}
			blockBuildSamples = append(blockBuildSamples, durationMS(preparedBlock.buildDuration))
			blockSealSamples = append(blockSealSamples, durationMS(preparedBlock.sealDuration))
			if !opts.RelayConfirmedBlocks {
				if err := submitBlockToFollowers(ctx, cluster[1:], preparedBlock.block); err != nil {
					return workloadOutcome{}, err
				}
			}
			currentHeight = accepted.height
			blockProcessing := time.Since(blockStarted)
			if !(opts.Benchmark == BenchmarkE2E && opts.SyntheticMining) {
				heightReached, err := waitForHeightsReached(ctx, cluster, currentHeight, blockStarted)
				if err != nil {
					return workloadOutcome{}, err
				}
				blockProcessing = maxDuration(heightReached)
			}
			confirmedProcessing += blockProcessing
			blockConvergenceSamples = append(blockConvergenceSamples, durationMS(blockProcessing))
			roundDuration := time.Since(roundStarted)
			blockRoundSamples = append(blockRoundSamples, durationMS(roundDuration))
			confirmedInBlock := max(0, len(preparedBlock.block.Txs)-1)
			metrics.ConfirmedTxs += confirmedInBlock
			expectedMempoolTxs -= confirmedInBlock
			if expectedMempoolTxs < 0 {
				expectedMempoolTxs = 0
			}
			metrics.ConfirmedBlocks++
			progressEventf(ctx, "block=%d accepted height=%d hash=%s round=%s max_mempool=%d",
				metrics.ConfirmedBlocks,
				accepted.height,
				shortHash(preparedBlock.hashHex),
				roundDuration.Round(time.Millisecond),
				maxNodeMempoolCount(cluster),
			)
			start = end
			if opts.FindMaxTPS {
				rampTried = append(rampTried, waveTarget)
				metrics.RampSteps++
				if waveTarget == metrics.TargetTxsPerBlock && roundDuration <= opts.SyntheticBlockInterval {
					metrics.MaxSustainableTxsPerBlock = waveTarget
					if start < len(txs) {
						nextTarget := min(len(txs), alignUp(metrics.TargetTxsPerBlock+step, step))
						if nextTarget > metrics.TargetTxsPerBlock {
							metrics.TargetTxsPerBlock = nextTarget
						}
					}
				} else if waveTarget == metrics.TargetTxsPerBlock && roundDuration > opts.SyntheticBlockInterval {
					metrics.FirstUnsustainedTxsPerBlock = waveTarget
					start = len(txs)
				}
			}
			break
		}
	}
	completion := make(map[string]time.Duration)
	var err error
	if opts.Benchmark == BenchmarkE2E && opts.SyntheticMining {
		if _, err := waitForHeightsReached(ctx, cluster, currentHeight, submitStarted); err != nil {
			return workloadOutcome{}, err
		}
		completion, err = waitForMempoolCounts(ctx, cluster, expectedMempoolTxs, submitStarted)
		if err != nil {
			return workloadOutcome{}, err
		}
	} else {
		completion, err = waitForMempoolCounts(ctx, cluster, 0, submitStarted)
		if err != nil {
			return workloadOutcome{}, err
		}
	}
	if len(blockConvergenceSamples) > 0 {
		metrics.BlockConvergeAvgMS = averageFloat64(blockConvergenceSamples)
		metrics.BlockConvergeP95MS = percentileFloat64(blockConvergenceSamples, 95)
		metrics.BlockConvergeMaxMS = maxFloat64(blockConvergenceSamples)
	}
	if len(blockRoundSamples) > 0 {
		metrics.BlockRoundAvgMS = averageFloat64(blockRoundSamples)
		metrics.BlockRoundP95MS = percentileFloat64(blockRoundSamples, 95)
		metrics.BlockRoundMaxMS = maxFloat64(blockRoundSamples)
	}
	if len(blockBuildSamples) > 0 {
		metrics.BlockBuildAvgMS = averageFloat64(blockBuildSamples)
		metrics.BlockBuildP95MS = percentileFloat64(blockBuildSamples, 95)
		metrics.BlockBuildMaxMS = maxFloat64(blockBuildSamples)
	}
	if len(blockSealSamples) > 0 {
		metrics.BlockSealAvgMS = averageFloat64(blockSealSamples)
		metrics.BlockSealP95MS = percentileFloat64(blockSealSamples, 95)
		metrics.BlockSealMaxMS = maxFloat64(blockSealSamples)
	}
	if len(scheduleLagSamples) > 0 {
		metrics.ScheduleLagAvgMS = averageFloat64(scheduleLagSamples)
		metrics.ScheduleLagP95MS = percentileFloat64(scheduleLagSamples, 95)
		metrics.ScheduleLagMaxMS = maxFloat64(scheduleLagSamples)
	}
	applyTemplateDiagnostics(&metrics, submitter.svc)
	metrics.FinalMempoolTxs = maxNodeMempoolCount(cluster)
	metrics.FinalBlockLag, metrics.FinalHeaderLag = maxNodeChainLag(cluster, currentHeight)
	completed := time.Now()
	if opts.FindMaxTPS && metrics.MaxSustainableTxsPerBlock > 0 {
		metrics.TargetTxsPerBlock = metrics.MaxSustainableTxsPerBlock
	}
	extraNotes := []string{
		fmt.Sprintf("Confirmed-blocks runs submit transactions in bounded rounds, waits for mempool convergence, seals one block on node-0, and counts only transactions actually included in accepted blocks. The starting block target was %d txs.", initialTargetTxsPerBlock),
	}
	if opts.FindMaxTPS {
		sort.Ints(rampTried)
		extraNotes = append(extraNotes,
			fmt.Sprintf("Synthetic max-TPS mode reused one live cluster and increased the block workload by %d txs after each sustainable round.", step),
			fmt.Sprintf("Synthetic ramp attempted txs_per_block values: %s.", joinInts(rampTried)),
		)
		if metrics.FirstUnsustainedTxsPerBlock > 0 {
			if metrics.MaxSustainableTxsPerBlock == 0 {
				extraNotes = append(extraNotes,
					fmt.Sprintf("The initial synthetic block target of %d txs/block already exceeded the cadence.", metrics.FirstUnsustainedTxsPerBlock),
				)
			} else {
				extraNotes = append(extraNotes,
					fmt.Sprintf("The largest sustained block load was %d txs/block; %d txs/block exceeded the synthetic cadence.", metrics.MaxSustainableTxsPerBlock, metrics.FirstUnsustainedTxsPerBlock),
				)
			}
		} else {
			extraNotes = append(extraNotes,
				fmt.Sprintf("The run stayed within the synthetic cadence through %d txs/block; the search stopped because the seeded workload was exhausted.", metrics.MaxSustainableTxsPerBlock),
			)
		}
	}
	return workloadOutcome{
		submitStarted:       submitStarted,
		submitDone:          submitStarted.Add(admissionElapsed),
		admissionTime:       admissionElapsed,
		completed:           completed,
		propagation:         completion,
		submittedByNode:     submittedByNode,
		confirmedProcessing: confirmedProcessing,
		phases: PhaseReport{
			ValidateAdmitMS: durationMS(admissionElapsed),
			ConvergenceMS:   durationMS(maxDuration(completion)),
		},
		metrics:    metrics,
		extraNotes: extraNotes,
	}, nil
}

type preparedBenchmarkBlock struct {
	block         types.Block
	hashHex       string
	buildDuration time.Duration
	sealDuration  time.Duration
}

type acceptedBenchmarkBlock struct {
	hash   [32]byte
	height uint64
}

type spendableLane struct {
	OutPoint    types.OutPoint
	Value       uint64
	SpenderSeed byte
}

type preparedLaneTx struct {
	tx        types.Transaction
	nextLanes []spendableLane
	submitter *clusterNode
}

type batchSubmitResult struct {
	admissions []mempool.Admission
	errs       []error
}

type submitGroupResult struct {
	nodeName    string
	submitted   int
	acceptedTxs int
	err         error
}

func submitConfirmedWave(ctx context.Context, submitters []*clusterNode, submitterOffset int, wave []types.Transaction, batchSize int, submittedByNode map[string]int) (int, int, error) {
	if len(submitters) == 0 {
		return 0, submitterOffset, errors.New("benchmark has no submitter nodes")
	}
	grouped := make([][]types.Transaction, len(submitters))
	for i, tx := range wave {
		idx := (submitterOffset + i) % len(submitters)
		grouped[idx] = append(grouped[idx], tx)
	}
	acceptedTxs, err := submitGroupedDecodedTxs(ctx, submitters, grouped, batchSize, submittedByNode)
	if err != nil {
		return 0, submitterOffset, err
	}
	return acceptedTxs, (submitterOffset + len(wave)) % len(submitters), nil
}

func submitPreparedLaneWave(ctx context.Context, wave []preparedLaneTx, batchSize int, submittedByNode map[string]int) (int, error) {
	submitters := make([]*clusterNode, 0)
	indexes := make(map[*clusterNode]int)
	for _, prepared := range wave {
		idx, ok := indexes[prepared.submitter]
		if !ok {
			idx = len(submitters)
			indexes[prepared.submitter] = idx
			submitters = append(submitters, prepared.submitter)
		}
		_ = idx
	}
	grouped := make([][]types.Transaction, len(submitters))
	for _, prepared := range wave {
		grouped[indexes[prepared.submitter]] = append(grouped[indexes[prepared.submitter]], prepared.tx)
	}
	return submitGroupedDecodedTxs(ctx, submitters, grouped, batchSize, submittedByNode)
}

func submitGroupedDecodedTxs(ctx context.Context, submitters []*clusterNode, grouped [][]types.Transaction, batchSize int, submittedByNode map[string]int) (int, error) {
	resultCh := make(chan submitGroupResult, len(submitters))
	started := 0
	for i, submitter := range submitters {
		if i >= len(grouped) || len(grouped[i]) == 0 {
			continue
		}
		started++
		txs := grouped[i]
		go func(submitter *clusterNode, txs []types.Transaction) {
			accepted, err := submitDecodedTxBatches(ctx, submitter, txs, batchSize)
			resultCh <- submitGroupResult{
				nodeName:    submitter.name,
				submitted:   len(txs),
				acceptedTxs: accepted,
				err:         err,
			}
		}(submitter, txs)
	}
	acceptedTxs := 0
	for i := 0; i < started; i++ {
		result := <-resultCh
		if result.err != nil {
			return 0, result.err
		}
		acceptedTxs += result.acceptedTxs
		submittedByNode[result.nodeName] += result.submitted
	}
	return acceptedTxs, nil
}

func submitDecodedTxBatches(ctx context.Context, submitter *clusterNode, txs []types.Transaction, batchSize int) (int, error) {
	if len(txs) == 0 {
		return 0, nil
	}
	batchSize = max(1, batchSize)
	acceptedTxs := 0
	for start := 0; start < len(txs); start += batchSize {
		end := min(len(txs), start+batchSize)
		batch := txs[start:end]
		result, err := callWithContext(ctx, func() (batchSubmitResult, error) {
			admissions, errs := submitter.svc.SubmitTxBatch(batch)
			return batchSubmitResult{admissions: admissions, errs: errs}, nil
		})
		if err != nil {
			return 0, err
		}
		for i, admitErr := range result.errs {
			if admitErr != nil {
				return 0, fmt.Errorf("submit batch tx %d on %s: %w", start+i, submitter.name, admitErr)
			}
		}
		for _, admission := range result.admissions {
			acceptedTxs += len(admission.Accepted)
		}
	}
	return acceptedTxs, nil
}

func runConfirmedBlocksMaxRampScenario(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, submitters []*clusterNode, opts RunOptions, lanes []spendableLane) (workloadOutcome, error) {
	submitStarted := time.Now()
	step := max(1, opts.BatchSize)
	targetTxsPerBlock := max(step, opts.TxsPerBlock)
	targetTxsPerBlock = alignUp(targetTxsPerBlock, step)
	metrics := ScenarioMetrics{
		TargetTxsPerBlock: targetTxsPerBlock,
	}
	blockConvergenceSamples := make([]float64, 0, 16)
	blockRoundSamples := make([]float64, 0, 16)
	blockBuildSamples := make([]float64, 0, 16)
	blockSealSamples := make([]float64, 0, 16)
	var admissionElapsed time.Duration
	var confirmedProcessing time.Duration
	submittedByNode := make(map[string]int, len(submitters))
	currentHeight := uint64(1)
	submitterOffset := 0
	rampTried := make([]int, 0, 16)
	exhaustedLanes := false

	for {
		if len(lanes) == 0 {
			exhaustedLanes = true
			break
		}
		waveTarget := min(metrics.TargetTxsPerBlock, len(lanes))
		if waveTarget <= 0 {
			exhaustedLanes = true
			break
		}
		roundStarted := time.Now()
		wave, remainingLanes, nextOffset, err := prepareLaneWave(lanes, waveTarget, submitters, submitterOffset)
		if err != nil {
			if errors.Is(err, consensus.ErrInputsLessThanOutputs) {
				exhaustedLanes = true
				break
			}
			return workloadOutcome{}, err
		}
		submitterOffset = nextOffset
		admitStarted := time.Now()
		acceptedTxs, err := submitPreparedLaneWave(ctx, wave, opts.BatchSize, submittedByNode)
		if err != nil {
			return workloadOutcome{}, err
		}
		metrics.AcceptedTxs += acceptedTxs
		admissionElapsed += time.Since(admitStarted)
		if _, err := waitForMempoolCounts(ctx, []*clusterNode{submitter}, len(wave), time.Time{}); err != nil {
			return workloadOutcome{}, err
		}
		preparedBlock, err := buildConfirmedBenchmarkBlock(ctx, submitter.svc, opts)
		if err != nil {
			return workloadOutcome{}, err
		}
		blockStarted := time.Now()
		accepted, err := callWithContext(ctx, func() (acceptedBenchmarkBlock, error) {
			hash, height, err := submitter.svc.AcceptLocalBenchmarkBlock(preparedBlock.block)
			return acceptedBenchmarkBlock{hash: hash, height: height}, err
		})
		if err != nil {
			return workloadOutcome{}, err
		}
		if hex.EncodeToString(accepted.hash[:]) != preparedBlock.hashHex {
			return workloadOutcome{}, fmt.Errorf("accepted block hash mismatch: got %x want %s", accepted.hash, preparedBlock.hashHex)
		}
		blockBuildSamples = append(blockBuildSamples, durationMS(preparedBlock.buildDuration))
		blockSealSamples = append(blockSealSamples, durationMS(preparedBlock.sealDuration))
		if !opts.RelayConfirmedBlocks {
			if err := submitBlockToFollowers(ctx, cluster[1:], preparedBlock.block); err != nil {
				return workloadOutcome{}, err
			}
		}
		currentHeight++
		heightReached, err := waitForHeightsReached(ctx, cluster, currentHeight, blockStarted)
		if err != nil {
			return workloadOutcome{}, err
		}
		blockProcessing := maxDuration(heightReached)
		confirmedProcessing += blockProcessing
		blockConvergenceSamples = append(blockConvergenceSamples, durationMS(blockProcessing))
		roundDuration := time.Since(roundStarted)
		blockRoundSamples = append(blockRoundSamples, durationMS(roundDuration))
		metrics.ConfirmedBlocks++
		rampTried = append(rampTried, waveTarget)

		laneOutputs := confirmedLaneOutputs(preparedBlock.block, wave)
		if len(laneOutputs) == 0 {
			exhaustedLanes = true
			break
		}
		metrics.ConfirmedTxs += len(laneOutputs) / 2
		lanes = append(remainingLanes, laneOutputs...)
		if roundDuration <= opts.SyntheticBlockInterval {
			metrics.MaxSustainableTxsPerBlock = waveTarget
			metrics.RampSteps++
			metrics.TargetTxsPerBlock = nextSyntheticRampTarget(waveTarget, step, roundDuration, opts.SyntheticBlockInterval)
			continue
		}
		metrics.FirstUnsustainedTxsPerBlock = waveTarget
		metrics.RampSteps++
		break
	}

	completion, err := waitForMempoolCounts(ctx, cluster, 0, submitStarted)
	if err != nil {
		return workloadOutcome{}, err
	}
	if len(blockConvergenceSamples) > 0 {
		metrics.BlockConvergeAvgMS = averageFloat64(blockConvergenceSamples)
		metrics.BlockConvergeP95MS = percentileFloat64(blockConvergenceSamples, 95)
		metrics.BlockConvergeMaxMS = maxFloat64(blockConvergenceSamples)
	}
	if len(blockRoundSamples) > 0 {
		metrics.BlockRoundAvgMS = averageFloat64(blockRoundSamples)
		metrics.BlockRoundP95MS = percentileFloat64(blockRoundSamples, 95)
		metrics.BlockRoundMaxMS = maxFloat64(blockRoundSamples)
	}
	if len(blockBuildSamples) > 0 {
		metrics.BlockBuildAvgMS = averageFloat64(blockBuildSamples)
		metrics.BlockBuildP95MS = percentileFloat64(blockBuildSamples, 95)
		metrics.BlockBuildMaxMS = maxFloat64(blockBuildSamples)
	}
	if len(blockSealSamples) > 0 {
		metrics.BlockSealAvgMS = averageFloat64(blockSealSamples)
		metrics.BlockSealP95MS = percentileFloat64(blockSealSamples, 95)
		metrics.BlockSealMaxMS = maxFloat64(blockSealSamples)
	}
	applyTemplateDiagnostics(&metrics, submitter.svc)
	if metrics.MaxSustainableTxsPerBlock > 0 {
		metrics.TargetTxsPerBlock = metrics.MaxSustainableTxsPerBlock
	}
	sort.Ints(rampTried)
	extraNotes := []string{
		fmt.Sprintf("Confirmed-blocks runs submit transactions in bounded rounds, waits for mempool convergence, seals one block on node-0, and waits for block-height convergence before the next round. The starting block target was %d txs.", alignUp(max(step, opts.TxsPerBlock), step)),
		fmt.Sprintf("Synthetic max-TPS mode reused one live cluster, ramped aggressively while rounds stayed far below the cadence, and then refined in %d-tx steps near the edge.", step),
		fmt.Sprintf("Synthetic ramp attempted txs_per_block values: %s.", joinInts(rampTried)),
		"The synthetic max-TPS workload replenishes its spend queue from the benchmark's own confirmed outputs, so the ramp is limited by block processing rather than one-shot seeding.",
	}
	if metrics.FirstUnsustainedTxsPerBlock > 0 {
		if metrics.MaxSustainableTxsPerBlock == 0 {
			extraNotes = append(extraNotes,
				fmt.Sprintf("The initial synthetic block target of %d txs/block already exceeded the cadence.", metrics.FirstUnsustainedTxsPerBlock),
			)
		} else {
			extraNotes = append(extraNotes,
				fmt.Sprintf("The largest sustained block load was %d txs/block; %d txs/block exceeded the synthetic cadence.", metrics.MaxSustainableTxsPerBlock, metrics.FirstUnsustainedTxsPerBlock),
			)
		}
	} else if exhaustedLanes {
		extraNotes = append(extraNotes,
			fmt.Sprintf("The run stayed within the synthetic cadence through %d txs/block; the lane queue stopped growing because outputs became too small to split again.", metrics.MaxSustainableTxsPerBlock),
		)
	}
	completed := time.Now()
	return workloadOutcome{
		submitStarted:       submitStarted,
		submitDone:          submitStarted.Add(admissionElapsed),
		admissionTime:       admissionElapsed,
		completed:           completed,
		propagation:         completion,
		submittedByNode:     submittedByNode,
		confirmedProcessing: confirmedProcessing,
		phases: PhaseReport{
			ValidateAdmitMS: durationMS(admissionElapsed),
			ConvergenceMS:   durationMS(maxDuration(completion)),
		},
		metrics:    metrics,
		extraNotes: extraNotes,
	}, nil
}

func applyTemplateDiagnostics(metrics *ScenarioMetrics, svc *node.Service) {
	if metrics == nil || svc == nil {
		return
	}
	templateStats := svc.BlockTemplateStats()
	perf := svc.PerformanceMetrics()
	metrics.TemplateCacheHits = templateStats.CacheHits
	metrics.TemplateFrontier = templateStats.FrontierCandidates
	metrics.TemplateFullBuilds = templateStats.FullBuilds
	metrics.TemplateAppendExtends = templateStats.AppendExtends
	metrics.TemplateNoChangeRefreshes = templateStats.NoChangeRefreshes
	metrics.TemplateSelectAvgMS = perf.Latency.TemplateSelect.AvgMS
	metrics.TemplateSelectP95MS = perf.Latency.TemplateSelect.P95MS
	metrics.TemplateSelectMaxMS = perf.Latency.TemplateSelect.MaxMS
	metrics.TemplateAccumulateAvgMS = perf.Latency.TemplateAccumulate.AvgMS
	metrics.TemplateAccumulateP95MS = perf.Latency.TemplateAccumulate.P95MS
	metrics.TemplateAccumulateMaxMS = perf.Latency.TemplateAccumulate.MaxMS
	metrics.TemplateAssembleAvgMS = perf.Latency.TemplateAssemble.AvgMS
	metrics.TemplateAssembleP95MS = perf.Latency.TemplateAssemble.P95MS
	metrics.TemplateAssembleMaxMS = perf.Latency.TemplateAssemble.MaxMS
}

func prepareLaneWave(lanes []spendableLane, count int, submitters []*clusterNode, submitterOffset int) ([]preparedLaneTx, []spendableLane, int, error) {
	if count <= 0 {
		return nil, append([]spendableLane(nil), lanes...), submitterOffset, nil
	}
	if count > len(lanes) {
		count = len(lanes)
	}
	wave := make([]preparedLaneTx, 0, count)
	remaining := append([]spendableLane(nil), lanes[count:]...)
	offset := submitterOffset
	for i := 0; i < count; i++ {
		lane := lanes[i]
		tx, values, err := buildFanoutTx(lane.SpenderSeed, lane.OutPoint, lane.Value, pubKeyForSeed(lane.SpenderSeed), 2, consensus.ParamsForProfile(submitters[0].svc.Profile()))
		if err != nil {
			return nil, nil, submitterOffset, err
		}
		txid := consensus.TxID(&tx)
		next := make([]spendableLane, 0, len(values))
		for vout, value := range values {
			next = append(next, spendableLane{
				OutPoint:    types.OutPoint{TxID: txid, Vout: uint32(vout)},
				Value:       value,
				SpenderSeed: lane.SpenderSeed,
			})
		}
		target := submitters[offset%len(submitters)]
		offset++
		wave = append(wave, preparedLaneTx{
			tx:        tx,
			nextLanes: next,
			submitter: target,
		})
	}
	return wave, remaining, offset % len(submitters), nil
}

func confirmedLaneOutputs(block types.Block, wave []preparedLaneTx) []spendableLane {
	if len(block.Txs) <= 1 || len(wave) == 0 {
		return nil
	}
	confirmed := make(map[[32]byte][]spendableLane, len(wave))
	for _, prepared := range wave {
		confirmed[consensus.TxID(&prepared.tx)] = prepared.nextLanes
	}
	outputs := make([]spendableLane, 0, len(wave)*2)
	for _, tx := range block.Txs[1:] {
		if next, ok := confirmed[consensus.TxID(&tx)]; ok {
			outputs = append(outputs, next...)
		}
	}
	return outputs
}

func nextSyntheticRampTarget(current, step int, roundDuration, cadence time.Duration) int {
	if current <= 0 {
		return max(step, 1)
	}
	if cadence <= 0 {
		return current + step
	}
	if roundDuration*16 <= cadence {
		return alignUp(current*4, step)
	}
	if roundDuration*4 <= cadence {
		return alignUp(current*2, step)
	}
	return alignUp(current+step, step)
}

func buildConfirmedBenchmarkBlock(ctx context.Context, svc *node.Service, opts RunOptions) (preparedBenchmarkBlock, error) {
	buildStarted := time.Now()
	block, err := callWithContext(ctx, func() (types.Block, error) {
		if opts.SteadyStateBacklog {
			return svc.BuildBenchmarkBlockTemplate(opts.TxsPerBlock)
		}
		return svc.BuildBlockTemplate()
	})
	if err != nil {
		return preparedBenchmarkBlock{}, err
	}
	buildDuration := time.Since(buildStarted)
	sealStarted := time.Now()
	if opts.SyntheticMining {
		info := svc.ChainStateInfo()
		stepSeconds := uint64(opts.SyntheticBlockInterval / time.Second)
		if stepSeconds == 0 {
			stepSeconds = 1
		}
		block.Header.Timestamp = info.TipTimestamp + stepSeconds
	} else {
		minedHeader, err := consensus.MineHeader(block.Header, consensus.ParamsForProfile(opts.Profile))
		if err != nil {
			return preparedBenchmarkBlock{}, err
		}
		block.Header = minedHeader
	}
	sealDuration := time.Since(sealStarted)
	hash := consensus.HeaderHash(&block.Header)
	return preparedBenchmarkBlock{
		block:         block,
		hashHex:       hex.EncodeToString(hash[:]),
		buildDuration: buildDuration,
		sealDuration:  sealDuration,
	}, nil
}

type callResult[T any] struct {
	value T
	err   error
}

func callWithContext[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	resultCh := make(chan callResult[T], 1)
	go func() {
		value, err := fn()
		resultCh <- callResult[T]{value: value, err: err}
	}()
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case result := <-resultCh:
		return result.value, result.err
	}
}

func submitBlockToFollowers(ctx context.Context, followers []*clusterNode, block types.Block) error {
	if len(followers) == 0 {
		return nil
	}
	blockHash := consensus.HeaderHash(&block.Header)
	blockHashHex := hex.EncodeToString(blockHash[:])
	errCh := make(chan error, len(followers))
	for _, follower := range followers {
		follower := follower
		go func() {
			_, err := callWithContext(ctx, func() (acceptedBenchmarkBlock, error) {
				hash, height, err := follower.svc.SubmitBenchmarkBlock(block)
				return acceptedBenchmarkBlock{hash: hash, height: height}, err
			})
			if err != nil {
				if errors.Is(err, node.ErrBlockAlreadyKnown) {
					errCh <- nil
					return
				}
				if strings.Contains(err.Error(), "stale template") {
					info := follower.svc.ChainStateInfo()
					if info.TipHeaderHash == blockHashHex {
						errCh <- nil
						return
					}
				}
				errCh <- err
				return
			}
			errCh <- nil
		}()
	}
	for range followers {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func syntheticE2EBlockTime(started time.Time, opts RunOptions, blockNumber int) time.Time {
	if opts.Benchmark != BenchmarkE2E || !opts.SyntheticMining || opts.SyntheticBlockInterval <= 0 || blockNumber <= 0 {
		return time.Time{}
	}
	return started.Add(time.Duration(blockNumber) * opts.SyntheticBlockInterval)
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	if deadline.IsZero() {
		return nil
	}
	for {
		wait := time.Until(deadline)
		if wait <= 0 {
			return nil
		}
		next := wait
		if next > time.Second {
			next = time.Second
		}
		timer := time.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			progressHeartbeat(ctx, "wait-schedule", func() string {
				return fmt.Sprintf("next_block_in=%s", time.Until(deadline).Round(time.Second))
			})
		}
	}
}

func runSubmissionScenario(ctx context.Context, cluster []*clusterNode, submitters []*clusterNode, opts RunOptions, txs []types.Transaction) (workloadOutcome, error) {
	submitStarted := time.Now()
	submission, err := submitWorkload(submitters, txs, opts)
	if err != nil {
		return workloadOutcome{}, err
	}
	targetCount := submission.metrics.AcceptedTxs
	if targetCount == 0 {
		targetCount = len(txs)
	}
	propagation, err := waitForMempoolCounts(ctx, cluster, targetCount, submitStarted)
	if err != nil {
		return workloadOutcome{}, err
	}
	outcome := workloadOutcome{
		submitStarted:   submitStarted,
		submitDone:      submission.submitDone,
		admissionTime:   submission.submitDone.Sub(submitStarted),
		completed:       time.Now(),
		propagation:     propagation,
		submittedByNode: submission.submittedByNode,
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

func runDirectSubmitSequence(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, txs []types.Transaction) (workloadOutcome, error) {
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
	propagation, err := waitForMempoolCounts(ctx, cluster, len(txs), submitStarted)
	if err != nil {
		return workloadOutcome{}, err
	}
	return workloadOutcome{
		submitStarted: submitStarted,
		submitDone:    submitDone,
		admissionTime: submitDone.Sub(submitStarted),
		completed:     time.Now(),
		propagation:   propagation,
		phases: PhaseReport{
			ValidateAdmitMS: durationMS(submitDone.Sub(submitStarted)),
			ConvergenceMS:   durationMS(maxDuration(propagation)),
		},
		metrics: metrics,
	}, nil
}

func runTemplateRebuildScenario(ctx context.Context, cluster []*clusterNode, submitter *clusterNode, txs []types.Transaction, batchSize int) (workloadOutcome, error) {
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
	propagation, err := waitForMempoolCounts(ctx, cluster, len(txs), submitStarted)
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
		submitStarted: submitStarted,
		submitDone:    submitDone,
		admissionTime: submitDone.Sub(submitStarted),
		completed:     time.Now(),
		propagation:   propagation,
		phases: PhaseReport{
			ValidateAdmitMS: durationMS(submitDone.Sub(submitStarted)),
			ConvergenceMS:   durationMS(maxDuration(propagation)),
		},
		metrics: metrics,
	}, nil
}

func submitWorkload(submitters []*clusterNode, txs []types.Transaction, opts RunOptions) (submissionOutcome, error) {
	if len(submitters) == 0 {
		return submissionOutcome{}, errors.New("benchmark has no submitter nodes")
	}
	switch opts.Scenario {
	case ScenarioDirectSubmit, ScenarioP2PRelay, ScenarioUserMix:
		started := time.Now()
		metrics := ScenarioMetrics{}
		submittedByNode := make(map[string]int, len(submitters))
		for i, tx := range txs {
			if opts.Benchmark == BenchmarkThroughput && opts.Duration > 0 && time.Since(started) >= opts.Duration {
				break
			}
			submitter := submitters[i%len(submitters)]
			admission, err := submitter.svc.SubmitTx(tx)
			if err != nil {
				return submissionOutcome{}, err
			}
			metrics.AcceptedTxs += len(admission.Accepted)
			submittedByNode[submitter.name]++
		}
		submitDone := time.Now()
		return submissionOutcome{
			submitDone:      submitDone,
			validateAdmitMS: durationMS(submitDone.Sub(started)),
			submittedByNode: submittedByNode,
			metrics:         metrics,
		}, nil
	case ScenarioRPCBatch:
		client := &http.Client{Timeout: 10 * time.Second}
		metrics := ScenarioMetrics{}
		var decodeMS float64
		var validateAdmitMS float64
		started := time.Now()
		submittedByNode := make(map[string]int, len(submitters))
		batchIndex := 0
		for start := 0; start < len(txs); start += opts.BatchSize {
			if opts.Benchmark == BenchmarkThroughput && opts.Duration > 0 && time.Since(started) >= opts.Duration {
				break
			}
			end := start + opts.BatchSize
			if end > len(txs) {
				end = len(txs)
			}
			submitter := submitters[batchIndex%len(submitters)]
			batchIndex++
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
			submittedByNode[submitter.name] += end - start
		}
		return submissionOutcome{
			submitDone:      time.Now(),
			decodeMS:        decodeMS,
			validateAdmitMS: validateAdmitMS,
			submittedByNode: submittedByNode,
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
	_, err := waitForHeightsReached(ctx, nodes, height, time.Time{})
	return err
}

func waitForHeightsReached(ctx context.Context, nodes []*clusterNode, height uint64, started time.Time) (map[string]time.Duration, error) {
	reached := make(map[string]time.Duration, len(nodes))
	err := waitForCondition(ctx, "wait-heights", func() (bool, error) {
		allReached := true
		for _, benchNode := range nodes {
			if benchNode.svc.BlockHeight() >= height {
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
		return heightStatus(nodes, height)
	})
	return reached, err
}

func waitForMempoolCounts(ctx context.Context, nodes []*clusterNode, target int, started time.Time) (map[string]time.Duration, error) {
	reached, _, err := waitForMempoolCountsUntil(ctx, nodes, target, started, time.Time{})
	return reached, err
}

func waitForMempoolCountsUntil(ctx context.Context, nodes []*clusterNode, target int, started time.Time, deadline time.Time) (map[string]time.Duration, bool, error) {
	reached := make(map[string]time.Duration, len(nodes))
	var deadlineExceeded bool
	err := waitForCondition(ctx, "wait-mempool", func() (bool, error) {
		allReached := true
		for _, benchNode := range nodes {
			count := benchNode.svc.MempoolCount()
			targetReached := count == target
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
		if !deadline.IsZero() && time.Now().After(deadline) {
			deadlineExceeded = true
			return true, nil
		}
		return allReached, nil
	}, func() string {
		return mempoolStatus(nodes, target, reached)
	})
	if err != nil {
		return reached, false, err
	}
	return reached, !deadlineExceeded, nil
}

func waitForCondition(ctx context.Context, phase string, fn func() (bool, error), status func() string) error {
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
			progressHeartbeat(ctx, phase, status)
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
		debug := benchNode.svc.SyncDebugSnapshot()
		parts = append(parts, fmt.Sprintf("%s height=%d target=%d header=%d mempool=%d inflight=%s pending=%s peers=%s",
			benchNode.name,
			debug.BlockHeight,
			target,
			debug.HeaderHeight,
			debug.MempoolCount,
			debug.InflightBlocks,
			debug.PendingBlocks,
			debug.PeerSync,
		))
	}
	return strings.Join(parts, ", ")
}

func mempoolStatus(nodes []*clusterNode, target int, reached map[string]time.Duration) string {
	parts := make([]string, 0, len(nodes))
	for _, benchNode := range nodes {
		debug := benchNode.svc.SyncDebugSnapshot()
		status := fmt.Sprintf("%s mempool=%d target=%d height=%d header=%d inflight=%s pending=%s peers=%s",
			benchNode.name,
			debug.MempoolCount,
			target,
			debug.BlockHeight,
			debug.HeaderHeight,
			debug.InflightBlocks,
			debug.PendingBlocks,
			debug.PeerSync,
		)
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

func buildNodeReports(nodes []*clusterNode, reached map[string]time.Duration, submittedByNode map[string]int) []NodeReport {
	out := make([]NodeReport, 0, len(nodes))
	for _, benchNode := range nodes {
		perf := benchNode.svc.PerformanceMetrics()
		report := NodeReport{
			Name:                   benchNode.name,
			RPCAddr:                benchNode.rpcAddr,
			P2PAddr:                benchNode.p2pAddr,
			PeerCount:              benchNode.svc.PeerCount(),
			BlockHeight:            benchNode.svc.BlockHeight(),
			HeaderHeight:           benchNode.svc.HeaderHeight(),
			MempoolCount:           benchNode.svc.MempoolCount(),
			SubmittedTxs:           submittedByNode[benchNode.name],
			BlockSigChecks:         perf.Counters.BlockSigChecks,
			BlockSigFallbacks:      perf.Counters.BlockSigFallbacks,
			BlockSigVerifyAvgMS:    perf.Latency.BlockSigVerify.AvgMS,
			ErlayRounds:            perf.Counters.ErlayRounds,
			ErlayRequestedTxs:      perf.Counters.ErlayRequestedTxs,
			CompactBlockPlans:      perf.Counters.CompactBlockPlans,
			CompactBlocksReceived:  perf.Counters.CompactBlocksReceived,
			CompactBlocksRecovered: perf.Counters.CompactBlocksRecovered,
			CompactBlockMissingTxs: perf.Counters.CompactBlockMissingTxs,
			CompactBlockTxRequests: perf.Counters.CompactBlockTxRequests,
			CompactBlockFallbacks:  perf.Counters.CompactBlockFallbacks,
			GraphenePlans:          perf.Counters.GrapheneExtendedPlans,
			GrapheneDecodeFails:    perf.Counters.GrapheneDecodeFails,
			GrapheneRecoveries:     perf.Counters.GrapheneExtRecoveries,
			TemplateSelectAvgMS:    perf.Latency.TemplateSelect.AvgMS,
			TemplateAccAvgMS:       perf.Latency.TemplateAccumulate.AvgMS,
			TemplateAssembleAvgMS:  perf.Latency.TemplateAssemble.AvgMS,
			RelayPeers:             buildPeerRelayReports(benchNode.svc.RelayPeerStats()),
		}
		if reachedAt, ok := reached[benchNode.name]; ok {
			ms := durationMS(reachedAt)
			report.CompletionMS = &ms
		}
		out = append(out, report)
	}
	return out
}

func maxNodeMempoolCount(nodes []*clusterNode) int {
	best := 0
	for _, benchNode := range nodes {
		if count := benchNode.svc.MempoolCount(); count > best {
			best = count
		}
	}
	return best
}

func maxNodeChainLag(nodes []*clusterNode, targetHeight uint64) (blockLag uint64, headerLag uint64) {
	for _, benchNode := range nodes {
		debug := benchNode.svc.SyncDebugSnapshot()
		if debug.BlockHeight < targetHeight {
			blockLag = max(blockLag, targetHeight-debug.BlockHeight)
		}
		if debug.HeaderHeight < targetHeight {
			headerLag = max(headerLag, targetHeight-debug.HeaderHeight)
		}
	}
	return blockLag, headerLag
}

func reportBenchmark(opts RunOptions) Benchmark {
	if opts.Benchmark != "" {
		return opts.Benchmark
	}
	switch opts.Scenario {
	case ScenarioConfirmedBlocks:
		return BenchmarkE2E
	default:
		return BenchmarkThroughput
	}
}

func reportMode(opts RunOptions) string {
	if reportBenchmark(opts) != BenchmarkThroughput {
		return ""
	}
	if opts.ThroughputMode != "" {
		return string(opts.ThroughputMode)
	}
	if opts.Scenario == ScenarioConfirmedBlocks {
		return string(ThroughputModeBlock)
	}
	return string(ThroughputModeTx)
}

func reportMining(opts RunOptions) string {
	if reportBenchmark(opts) != BenchmarkE2E {
		return ""
	}
	if opts.MiningMode != "" {
		return string(opts.MiningMode)
	}
	if opts.SyntheticMining {
		return string(MiningModeSynthetic)
	}
	return string(MiningModeReal)
}

func reportFileStem(opts RunOptions) string {
	stem := string(reportBenchmark(opts))
	if mode := reportMode(opts); mode != "" {
		return stem + "-" + mode
	}
	if mining := reportMining(opts); mining != "" {
		return stem + "-" + mining
	}
	return stem
}

func reportTopology(opts RunOptions) string {
	if opts.NodeCount <= 1 {
		return ""
	}
	if opts.Scenario != ScenarioP2PRelay && opts.Scenario != ScenarioConfirmedBlocks {
		return ""
	}
	return string(opts.Topology)
}

func buildNotes(opts RunOptions, extra []string) []string {
	notes := []string{
		"All nodes run on local loopback, so the report measures BPU stack overhead rather than WAN latency.",
		"For WAN-like latency, jitter, serialization pressure, and node churn, use `bench sim`; the cluster benchmarks intentionally stay on loopback so node overhead and real protocol behavior stay separable from network modeling.",
	}
	switch reportBenchmark(opts) {
	case BenchmarkE2E:
		notes = append([]string{
			"E2E runs submit transactions in waves, wait for mempool convergence, seal one block, and wait for the new height to converge before the next wave.",
			"E2E TPS includes submission, relay, template build, sealing, block propagation, and chain convergence.",
		}, notes...)
		if opts.SyntheticMining {
			notes = append(notes,
				fmt.Sprintf("Synthetic mining is enabled with a fixed %.0f ms external block cadence. PoW is skipped inside the local benchmark cluster, but the benchmark clock does not slow down when the cluster falls behind.", durationMS(opts.SyntheticBlockInterval)),
				"If tx admission, relay, or block processing cannot keep up, transactions remain in the mempool and missed-interval / schedule-lag metrics increase instead of stretching the synthetic block interval or waiting for a hidden post-run drain.",
				"Synthetic interval TPS is the configured-cadence throughput ceiling for the offered workload; compare it against end-to-end TPS, missed intervals, final mempool backlog, and final block/header lag to see whether the cluster stayed on schedule.",
			)
		} else {
			preset := benchNetDifficultyPreset(opts.BlockInterval)
			notes = append(notes,
				fmt.Sprintf("Real mining runs on a fresh benchmark-only `benchnet` chain from genesis. The requested %.0f ms block interval selects a tuned starting difficulty preset with genesis bits `0x%08x`.", durationMS(opts.BlockInterval), preset.GenesisBits),
				"The achieved cadence still varies with nonce-search luck around that starting difficulty, so compare requested versus actual block interval metrics before treating real-mining TPS as a stable regression number.",
			)
		}
	case BenchmarkThroughput:
		switch reportMode(opts) {
		case string(ThroughputModeBlock):
			notes = append([]string{
				"Throughput block mode isolates block build and apply cost. Followers, if present, are fed through the benchmark path instead of waiting on network relay.",
				fmt.Sprintf("Requested duration is %.0f ms; actual runtime may end sooner if the seeded workload is exhausted.", durationMS(opts.Duration)),
			}, notes...)
		default:
			notes = append([]string{
				"Throughput tx mode isolates decode, validation, signature verification, and mempool admission.",
				fmt.Sprintf("Requested duration is %.0f ms; actual runtime may end sooner if the seeded workload is exhausted.", durationMS(opts.Duration)),
				"Independent fanout spends are used so raw tx throughput is not dominated by deep chaining.",
			}, notes...)
		}
	}
	if opts.NodeCount > 1 && (opts.Scenario == ScenarioP2PRelay || opts.Scenario == ScenarioConfirmedBlocks) {
		notes = append(notes, fmt.Sprintf("Real P2P relay is measured across %d nodes using the %s topology.", opts.NodeCount, opts.Topology))
		switch opts.TxOriginSpread {
		case TxOriginEven:
			notes = append(notes, fmt.Sprintf("Independent transactions are submitted round-robin across all %d nodes.", opts.NodeCount))
		default:
			notes = append(notes, "Transactions originate from node-0 only so the rest of the cluster acts purely as relay and confirmation followers.")
		}
	}
	notes = append(notes, extra...)
	return notes
}

func durationMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func alignUp(value, step int) int {
	if step <= 1 {
		return value
	}
	remainder := value % step
	if remainder == 0 {
		return value
	}
	return value + step - remainder
}

func alignDown(value, step int) int {
	if step <= 1 {
		return value
	}
	return value - (value % step)
}

func joinInts(values []int) string {
	if len(values) == 0 {
		return ""
	}
	out := make([]string, 0, len(values))
	last := -1
	for _, value := range values {
		if value == last {
			continue
		}
		out = append(out, strconv.Itoa(value))
		last = value
	}
	return strings.Join(out, ", ")
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
			Addr:                   stat.Addr,
			Outbound:               stat.Outbound,
			QueueDepth:             stat.QueueDepth,
			MaxQueueDepth:          stat.MaxQueueDepth,
			SentMessages:           stat.SentMessages,
			TxInvItems:             stat.TxInvItems,
			BlockInvItems:          stat.BlockInvItems,
			TxBatchMsgs:            stat.TxBatchMsgs,
			TxBatchItems:           stat.TxBatchItems,
			TxReconMsgs:            stat.TxReconMsgs,
			TxReconItems:           stat.TxReconItems,
			TxReqMsgs:              stat.TxReqMsgs,
			TxReqItems:             stat.TxReqItems,
			DuplicateTxSuppressed:  stat.DuplicateTxSuppressed,
			KnownTxSuppressed:      stat.KnownTxSuppressed,
			CoalescedTxItems:       stat.CoalescedTxItems,
			CoalescedReconItems:    stat.CoalescedReconItems,
			DroppedTxs:             stat.DroppedTxs,
			WriterStarvationEvents: stat.WriterStarvationEvents,
			RelayEvents:            stat.RelayEvents,
			RelayAvgMS:             stat.RelayAvgMS,
			RelayP95MS:             stat.RelayP95MS,
			RelayMaxMS:             stat.RelayMaxMS,
		})
	}
	return out
}

type relayAggregateMetrics struct {
	BatchMessages          int
	BatchItems             int
	ReconMessages          int
	ReconItems             int
	RequestMessages        int
	RequestItems           int
	DuplicateTxSuppressed  int
	KnownTxSuppressed      int
	CoalescedTxItems       int
	CoalescedReconItems    int
	WriterStarvationEvents int
}

func aggregateRelayMetrics(nodes []NodeReport) relayAggregateMetrics {
	var out relayAggregateMetrics
	for _, node := range nodes {
		for _, peer := range node.RelayPeers {
			out.BatchMessages += peer.TxBatchMsgs
			out.BatchItems += peer.TxBatchItems
			out.ReconMessages += peer.TxReconMsgs
			out.ReconItems += peer.TxReconItems
			out.RequestMessages += peer.TxReqMsgs
			out.RequestItems += peer.TxReqItems
			out.DuplicateTxSuppressed += peer.DuplicateTxSuppressed
			out.KnownTxSuppressed += peer.KnownTxSuppressed
			out.CoalescedTxItems += peer.CoalescedTxItems
			out.CoalescedReconItems += peer.CoalescedReconItems
			out.WriterStarvationEvents += peer.WriterStarvationEvents
		}
	}
	return out
}

func ratioOrZero(numerator int, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
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
	if metrics.ConfirmedTxs > 0 {
		rows = append(rows, [2]string{"Confirmed txs", strconv.Itoa(metrics.ConfirmedTxs)})
	}
	if metrics.ConfirmedBlocks > 0 {
		rows = append(rows, [2]string{"Confirmed blocks", strconv.Itoa(metrics.ConfirmedBlocks)})
	}
	if metrics.TargetTxsPerBlock > 0 {
		rows = append(rows, [2]string{"Target txs per block", strconv.Itoa(metrics.TargetTxsPerBlock)})
	}
	if metrics.MaxSustainableTxsPerBlock > 0 {
		rows = append(rows, [2]string{"Max sustainable txs per block", strconv.Itoa(metrics.MaxSustainableTxsPerBlock)})
	}
	if metrics.FirstUnsustainedTxsPerBlock > 0 {
		rows = append(rows, [2]string{"First unsustained txs per block", strconv.Itoa(metrics.FirstUnsustainedTxsPerBlock)})
	}
	if metrics.RampSteps > 0 {
		rows = append(rows, [2]string{"Ramp steps", strconv.Itoa(metrics.RampSteps)})
	}
	if metrics.BlockConvergeAvgMS > 0 {
		rows = append(rows, [2]string{"Block converge avg", fmt.Sprintf("%.2f ms", metrics.BlockConvergeAvgMS)})
		rows = append(rows, [2]string{"Block converge p95", fmt.Sprintf("%.2f ms", metrics.BlockConvergeP95MS)})
		rows = append(rows, [2]string{"Block converge max", fmt.Sprintf("%.2f ms", metrics.BlockConvergeMaxMS)})
	}
	if metrics.BlockRoundAvgMS > 0 {
		rows = append(rows, [2]string{"Block round avg", fmt.Sprintf("%.2f ms", metrics.BlockRoundAvgMS)})
		rows = append(rows, [2]string{"Block round p95", fmt.Sprintf("%.2f ms", metrics.BlockRoundP95MS)})
		rows = append(rows, [2]string{"Block round max", fmt.Sprintf("%.2f ms", metrics.BlockRoundMaxMS)})
	}
	if metrics.BlockBuildAvgMS > 0 {
		rows = append(rows, [2]string{"Block build avg", fmt.Sprintf("%.2f ms", metrics.BlockBuildAvgMS)})
		rows = append(rows, [2]string{"Block build p95", fmt.Sprintf("%.2f ms", metrics.BlockBuildP95MS)})
		rows = append(rows, [2]string{"Block build max", fmt.Sprintf("%.2f ms", metrics.BlockBuildMaxMS)})
	}
	if metrics.BlockSealAvgMS > 0 || metrics.BlockSealMaxMS > 0 {
		rows = append(rows, [2]string{"Block seal avg", fmt.Sprintf("%.2f ms", metrics.BlockSealAvgMS)})
		rows = append(rows, [2]string{"Block seal p95", fmt.Sprintf("%.2f ms", metrics.BlockSealP95MS)})
		rows = append(rows, [2]string{"Block seal max", fmt.Sprintf("%.2f ms", metrics.BlockSealMaxMS)})
	}
	if metrics.MissedIntervals > 0 {
		rows = append(rows, [2]string{"Missed intervals", strconv.Itoa(metrics.MissedIntervals)})
	}
	if metrics.ScheduleLagAvgMS > 0 {
		rows = append(rows, [2]string{"Schedule lag avg", fmt.Sprintf("%.2f ms", metrics.ScheduleLagAvgMS)})
		rows = append(rows, [2]string{"Schedule lag p95", fmt.Sprintf("%.2f ms", metrics.ScheduleLagP95MS)})
		rows = append(rows, [2]string{"Schedule lag max", fmt.Sprintf("%.2f ms", metrics.ScheduleLagMaxMS)})
	}
	if metrics.FinalMempoolTxs > 0 {
		rows = append(rows, [2]string{"Final mempool txs", strconv.Itoa(metrics.FinalMempoolTxs)})
	}
	if metrics.FinalBlockLag > 0 {
		rows = append(rows, [2]string{"Final block lag", strconv.FormatUint(metrics.FinalBlockLag, 10)})
	}
	if metrics.FinalHeaderLag > 0 {
		rows = append(rows, [2]string{"Final header lag", strconv.FormatUint(metrics.FinalHeaderLag, 10)})
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
		if metrics.TxRequestItemsPerMessage > 0 {
			rows = append(rows, [2]string{"Tx request items/msg", fmt.Sprintf("%.2f", metrics.TxRequestItemsPerMessage)})
		}
	}
	if metrics.TxReconMessages > 0 && metrics.TxReconItemsPerMessage > 0 {
		rows = append(rows, [2]string{"Tx recon items/msg", fmt.Sprintf("%.2f", metrics.TxReconItemsPerMessage)})
	}
	if metrics.TxBatchMessages > 0 && metrics.TxBatchItemsPerMessage > 0 {
		rows = append(rows, [2]string{"Tx batch items/msg", fmt.Sprintf("%.2f", metrics.TxBatchItemsPerMessage)})
	}
	if metrics.DuplicateTxSuppressed > 0 {
		rows = append(rows, [2]string{"Duplicate tx suppressed", strconv.Itoa(metrics.DuplicateTxSuppressed)})
	}
	if metrics.KnownTxSuppressed > 0 {
		rows = append(rows, [2]string{"Known tx suppressed", strconv.Itoa(metrics.KnownTxSuppressed)})
	}
	if metrics.CoalescedTxItems > 0 {
		rows = append(rows, [2]string{"Coalesced tx items", strconv.Itoa(metrics.CoalescedTxItems)})
	}
	if metrics.CoalescedReconItems > 0 {
		rows = append(rows, [2]string{"Coalesced recon items", strconv.Itoa(metrics.CoalescedReconItems)})
	}
	if metrics.WriterStarvationEvents > 0 {
		rows = append(rows, [2]string{"Writer starvation", strconv.Itoa(metrics.WriterStarvationEvents)})
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
	if metrics.TemplateFullBuilds > 0 || metrics.TemplateAppendExtends > 0 || metrics.TemplateNoChangeRefreshes > 0 {
		if metrics.TemplateFullBuilds > 0 {
			rows = append(rows, [2]string{"Template full builds", strconv.Itoa(metrics.TemplateFullBuilds)})
		}
		if metrics.TemplateAppendExtends > 0 {
			rows = append(rows, [2]string{"Template append extends", strconv.Itoa(metrics.TemplateAppendExtends)})
		}
		if metrics.TemplateNoChangeRefreshes > 0 {
			rows = append(rows, [2]string{"Template no-change refreshes", strconv.Itoa(metrics.TemplateNoChangeRefreshes)})
		}
	}
	if metrics.TemplateSelectAvgMS > 0 {
		rows = append(rows, [2]string{"Template select avg", fmt.Sprintf("%.2f ms", metrics.TemplateSelectAvgMS)})
		rows = append(rows, [2]string{"Template select p95", fmt.Sprintf("%.2f ms", metrics.TemplateSelectP95MS)})
		rows = append(rows, [2]string{"Template select max", fmt.Sprintf("%.2f ms", metrics.TemplateSelectMaxMS)})
	}
	if metrics.TemplateAccumulateAvgMS > 0 {
		rows = append(rows, [2]string{"Template accumulate avg", fmt.Sprintf("%.2f ms", metrics.TemplateAccumulateAvgMS)})
		rows = append(rows, [2]string{"Template accumulate p95", fmt.Sprintf("%.2f ms", metrics.TemplateAccumulateP95MS)})
		rows = append(rows, [2]string{"Template accumulate max", fmt.Sprintf("%.2f ms", metrics.TemplateAccumulateMaxMS)})
	}
	if metrics.TemplateAssembleAvgMS > 0 {
		rows = append(rows, [2]string{"Template assemble avg", fmt.Sprintf("%.2f ms", metrics.TemplateAssembleAvgMS)})
		rows = append(rows, [2]string{"Template assemble p95", fmt.Sprintf("%.2f ms", metrics.TemplateAssembleP95MS)})
		rows = append(rows, [2]string{"Template assemble max", fmt.Sprintf("%.2f ms", metrics.TemplateAssembleMaxMS)})
	}
	return rows
}

func asciiTopologySuffix(topology string) string {
	if strings.TrimSpace(topology) == "" {
		return ""
	}
	return " / " + topology
}

func asciiModeSuffix(mode string) string {
	if strings.TrimSpace(mode) == "" {
		return ""
	}
	return " / " + mode
}

func asciiMiningSuffix(mining string) string {
	if strings.TrimSpace(mining) == "" {
		return ""
	}
	return " / " + mining
}

func asciiBatchSuffix(batchSize int) string {
	if batchSize <= 0 {
		return ""
	}
	return fmt.Sprintf(" / batch %d", batchSize)
}

func asciiBlockTargetSuffix(txsPerBlock int) string {
	if txsPerBlock <= 0 {
		return ""
	}
	return fmt.Sprintf(" / block %d", txsPerBlock)
}

func asciiTxOriginSuffix(origin string) string {
	if origin == "" {
		return ""
	}
	return fmt.Sprintf(" / origin %s", origin)
}

func asciiBacklogSuffix(enabled bool) string {
	if !enabled {
		return ""
	}
	return " / backlog"
}

func formatCountWithCommas(value uint64) string {
	raw := strconv.FormatUint(value, 10)
	if len(raw) <= 3 {
		return raw
	}
	var b strings.Builder
	prefix := len(raw) % 3
	if prefix == 0 {
		prefix = 3
	}
	b.WriteString(raw[:prefix])
	for i := prefix; i < len(raw); i += 3 {
		b.WriteByte(',')
		b.WriteString(raw[i : i+3])
	}
	return b.String()
}

func signatureMetricRows(nodes []NodeReport) [][]string {
	rows := make([][]string, 0, len(nodes))
	for _, node := range nodes {
		if node.BlockSigChecks == 0 && node.BlockSigFallbacks == 0 && node.BlockSigVerifyAvgMS == 0 {
			continue
		}
		rows = append(rows, []string{
			node.Name,
			formatCountWithCommas(node.BlockSigChecks),
			formatCountWithCommas(node.BlockSigFallbacks),
			fmt.Sprintf("%.2f ms", node.BlockSigVerifyAvgMS),
		})
	}
	return rows
}

func relayMechanismRows(nodes []NodeReport) [][]string {
	rows := make([][]string, 0, len(nodes))
	for _, node := range nodes {
		if node.ErlayRounds == 0 &&
			node.ErlayRequestedTxs == 0 &&
			node.CompactBlockPlans == 0 &&
			node.CompactBlocksReceived == 0 &&
			node.CompactBlocksRecovered == 0 &&
			node.CompactBlockMissingTxs == 0 &&
			node.CompactBlockTxRequests == 0 &&
			node.CompactBlockFallbacks == 0 &&
			node.GraphenePlans == 0 &&
			node.GrapheneDecodeFails == 0 &&
			node.GrapheneRecoveries == 0 {
			continue
		}
		rows = append(rows, []string{
			node.Name,
			formatCountWithCommas(node.ErlayRounds),
			formatCountWithCommas(node.ErlayRequestedTxs),
			formatCountWithCommas(node.CompactBlockPlans),
			formatCountWithCommas(node.CompactBlocksReceived),
			formatCountWithCommas(node.CompactBlocksRecovered),
			formatCountWithCommas(node.CompactBlockMissingTxs),
			formatCountWithCommas(node.CompactBlockTxRequests),
			formatCountWithCommas(node.CompactBlockFallbacks),
			formatCountWithCommas(node.GraphenePlans),
			formatCountWithCommas(node.GrapheneDecodeFails),
			formatCountWithCommas(node.GrapheneRecoveries),
		})
	}
	return rows
}

func asciiSignatureSummary(nodes []NodeReport) string {
	var totalChecks uint64
	var totalFallbacks uint64
	var weightedVerifyMS float64
	for _, node := range nodes {
		totalChecks += node.BlockSigChecks
		totalFallbacks += node.BlockSigFallbacks
		weightedVerifyMS += node.BlockSigVerifyAvgMS * float64(node.BlockSigChecks)
	}
	if totalChecks == 0 && totalFallbacks == 0 {
		return ""
	}
	avgVerifyMS := 0.0
	if totalChecks > 0 {
		avgVerifyMS = weightedVerifyMS / float64(totalChecks)
	}
	return fmt.Sprintf("Sig Verify  %d checks | %d fallbacks | %.2f ms avg", totalChecks, totalFallbacks, avgVerifyMS)
}

func slowestNode(nodes []NodeReport) string {
	var (
		name string
		best float64
	)
	for _, node := range nodes {
		if node.CompletionMS == nil {
			continue
		}
		if *node.CompletionMS > best {
			best = *node.CompletionMS
			name = node.Name
		}
	}
	if name == "" {
		return ""
	}
	return fmt.Sprintf("Slowest     %s completion %.2f ms", name, best)
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
	header := fmt.Sprintf("%-20s %-10s %-7s %10s %10s %10s", "Case", "Benchmark", "Mode", "Admit", "Complete", "Confirm")
	rows := []string{header}
	for _, suiteCase := range cases {
		mode := suiteCase.Report.Mode
		if mode == "" {
			mode = "-"
		}
		confirmTPS := suiteCase.Report.ConfirmedProcessingTPS
		if confirmTPS == 0 {
			confirmTPS = suiteCase.Report.ConfirmedWallTPS
		}
		rows = append(rows, fmt.Sprintf("%-20s %-10s %-7s %10.2f %10.2f %10.2f",
			suiteCase.Name,
			suiteCase.Report.Benchmark,
			mode,
			suiteCase.Report.AdmissionTPS,
			suiteCase.Report.CompletionTPS,
			confirmTPS,
		))
	}
	return rows
}

func asciiSummaryTitle(report *Report) string {
	switch {
	case report.Benchmark == string(BenchmarkE2E):
		return "BPU E2E Benchmark"
	case report.Mode == string(ThroughputModeBlock):
		return "BPU Throughput Benchmark (Block)"
	default:
		return "BPU Throughput Benchmark (Tx)"
	}
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

func loadGenesisTemplate() (*types.Block, error) {
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

func loadBenchmarkGenesis(profile types.ChainProfile) (*types.Block, error) {
	template, err := loadGenesisTemplateForProfile(profile)
	if err != nil {
		return nil, err
	}
	if profile != types.BenchNet {
		return template, nil
	}
	params := consensus.ParamsForProfile(types.BenchNet)
	genesis := *template
	genesis.Header.Timestamp = params.GenesisTimestamp
	genesis.Header.NBits = params.GenesisBits
	genesis.Header.Nonce = 0
	minedHeader, err := consensus.MineHeader(genesis.Header, params)
	if err != nil {
		return nil, err
	}
	genesis.Header = minedHeader
	return &genesis, nil
}

func loadGenesisTemplateForProfile(profile types.ChainProfile) (*types.Block, error) {
	if profile == types.BenchNet {
		return loadGenesisTemplate()
	}
	raw, err := os.ReadFile(resolveRepoPath("fixtures", "genesis", profile.String()+".json"))
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

func benchmarkClusterProfile(opts RunOptions) types.ChainProfile {
	if opts.Profile.IsRegtestLike() {
		return types.BenchNet
	}
	return opts.Profile
}

func benchmarkGenesisLabel(profile types.ChainProfile) string {
	if profile == types.BenchNet {
		params := consensus.ParamsForProfile(types.BenchNet)
		return fmt.Sprintf("benchmarks:benchnet:bits=%08x", params.GenesisBits)
	}
	return fmt.Sprintf("fixtures/genesis/%s.json", profile)
}

func benchNetParamsForRun(opts RunOptions) consensus.ChainParams {
	params := consensus.RegtestParams()
	params.Profile = types.BenchNet
	if opts.Benchmark == BenchmarkE2E && opts.MiningMode == MiningModeReal {
		preset := benchNetDifficultyPreset(opts.BlockInterval)
		params.PowLimitBits = preset.PowLimitBits
		params.GenesisBits = preset.GenesisBits
	}
	return params
}

func benchNetDifficultyPreset(target time.Duration) consensus.ChainParams {
	candidates := []struct {
		params   consensus.ChainParams
		interval time.Duration
	}{
		{params: consensus.RegtestParams(), interval: time.Second},
		{params: consensus.RegtestMediumParams(), interval: 15 * time.Second},
		{params: consensus.RegtestHardParams(), interval: 2 * time.Minute},
	}
	if target <= 0 {
		return candidates[1].params
	}
	best := candidates[0]
	bestDiff := durationAbs(target - best.interval)
	for _, candidate := range candidates[1:] {
		if diff := durationAbs(target - candidate.interval); diff < bestDiff {
			best = candidate
			bestDiff = diff
		}
	}
	return best.params
}

func pubKeyForSeed(seed byte) [32]byte {
	return crypto.XOnlyPubKeyFromSecret([32]byte{seed})
}

func signSingleInputTx(tx types.Transaction, spenderSeed byte, prevValue uint64, params consensus.ChainParams) (types.Transaction, error) {
	msg, err := consensus.SighashWithParams(&tx, 0, []consensus.UtxoEntry{{
		ValueAtoms: prevValue,
		PubKey:     pubKeyForSeed(spenderSeed),
	}}, params)
	if err != nil {
		return types.Transaction{}, err
	}
	_, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	return tx, nil
}

func buildFanoutTx(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, outputPubKey [32]byte, outputs int, params consensus.ChainParams) (types.Transaction, []uint64, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: make([]types.TxOutput, outputs),
		},
	}
	for i := range tx.Base.Outputs {
		tx.Base.Outputs[i] = types.TxOutput{ValueAtoms: 1, PubKey: outputPubKey}
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue, params)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	fee := uint64(tx.EncodedLen())
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
	tx, err = signSingleInputTx(tx, spenderSeed, prevValue, params)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	return tx, values, nil
}

func buildChildTx(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, outputPubKey [32]byte, params consensus.ChainParams) (types.Transaction, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: 1, PubKey: outputPubKey}},
		},
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue, params)
	if err != nil {
		return types.Transaction{}, err
	}
	fee := uint64(tx.EncodedLen())
	if prevValue < fee {
		return types.Transaction{}, consensus.ErrInputsLessThanOutputs
	}
	tx.Base.Outputs[0].ValueAtoms = prevValue - fee
	return signSingleInputTx(tx, spenderSeed, prevValue, params)
}

type fundingOutput struct {
	OutPoint types.OutPoint
	Value    uint64
	PubKey   [32]byte
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
	outputs := make([]fundingOutput, 0, outputCount)
	batchSize, err := maxSeedableTxCount(svc.Profile())
	if err != nil {
		return nil, err
	}
	if batchSize <= 0 {
		return nil, errors.New("benchmark funding batch size must be positive")
	}
	for _, batchCount := range fundingBatchCounts(outputCount, batchSize) {
		keyHashes := make([][32]byte, batchCount)
		for i := range keyHashes {
			keyHashes[i] = pubKeyForSeed(recipientSeed)
		}
		funding, err := svc.MineFundingOutputs(keyHashes)
		if err != nil {
			return nil, err
		}
		for _, output := range funding {
			outputs = append(outputs, fundingOutput{
				OutPoint: output.OutPoint,
				Value:    output.Value,
				PubKey:   pubKeyForSeed(recipientSeed),
			})
		}
	}
	return outputs, nil
}

func buildFanoutTxToRecipients(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, recipients [][32]byte, params consensus.ChainParams) (types.Transaction, []uint64, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: make([]types.TxOutput, len(recipients)),
		},
	}
	for i, keyHash := range recipients {
		tx.Base.Outputs[i] = types.TxOutput{ValueAtoms: 1, PubKey: keyHash}
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue, params)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	fee := uint64(tx.EncodedLen())
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
	tx, err = signSingleInputTx(tx, spenderSeed, prevValue, params)
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
	outputs := make([]userFundingOutput, 0, outputCount)
	batchSize, err := maxSeedableTxCount(svc.Profile())
	if err != nil {
		return nil, err
	}
	if batchSize <= 0 {
		return nil, errors.New("benchmark funding batch size must be positive")
	}
	produced := 0
	for _, batchCount := range fundingBatchCounts(outputCount, batchSize) {
		recipients := make([][32]byte, 0, batchCount)
		seeds := make([]byte, 0, batchCount)
		for i := 0; i < batchCount; i++ {
			globalIndex := produced + i
			seed := byte(2 + (globalIndex % userCount))
			recipients = append(recipients, pubKeyForSeed(seed))
			seeds = append(seeds, seed)
		}
		funding, err := svc.MineFundingOutputs(recipients)
		if err != nil {
			return nil, err
		}
		for vout, output := range funding {
			globalIndex := produced + vout
			outputs = append(outputs, userFundingOutput{
				OutPoint:     output.OutPoint,
				Value:        output.Value,
				SpenderSeed:  seeds[vout],
				RecipientTag: byte(globalIndex % 251),
			})
		}
		produced += batchCount
	}
	return outputs, nil
}

func buildPaymentWithChangeTx(spenderSeed byte, prevOut types.OutPoint, prevValue uint64, paymentPubKey, changePubKey [32]byte, params consensus.ChainParams) (types.Transaction, uint64, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{
				{ValueAtoms: 1, PubKey: paymentPubKey},
				{ValueAtoms: 1, PubKey: changePubKey},
			},
		},
	}
	tx, err := signSingleInputTx(tx, spenderSeed, prevValue, params)
	if err != nil {
		return types.Transaction{}, 0, err
	}
	fee := uint64(tx.EncodedLen())
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
	tx, err = signSingleInputTx(tx, spenderSeed, prevValue, params)
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
	childPubKey := pubKeyForSeed(3)
	txs := make([]types.Transaction, 0, txCount)
	for _, output := range outputs {
		tx, err := buildChildTx(2, output.OutPoint, output.Value, childPubKey, consensus.ParamsForProfile(svc.Profile()))
		if err != nil {
			if errors.Is(err, consensus.ErrInputsLessThanOutputs) {
				return nil, fmt.Errorf("benchmark funding output %x:%d is too small to build a child transaction; funding batches should keep outputs above the child-tx fee floor", output.OutPoint.TxID, output.OutPoint.Vout)
			}
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func seedSpendableLanes(svc *node.Service, laneCount int, recipientSeed byte) ([]spendableLane, error) {
	outputs, err := seedFundingOutputs(svc, laneCount, recipientSeed)
	if err != nil {
		return nil, err
	}
	lanes := make([]spendableLane, 0, len(outputs))
	for _, output := range outputs {
		lanes = append(lanes, spendableLane{
			OutPoint:    output.OutPoint,
			Value:       output.Value,
			SpenderSeed: recipientSeed,
		})
	}
	return lanes, nil
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
			tx, err := buildChildTx(change.spenderSeed, change.outPoint, change.value, pubKeyForSeed(byte(90+(change.recipientTag%64))), consensus.ParamsForProfile(svc.Profile()))
			if err != nil {
				return nil, ScenarioMetrics{}, err
			}
			txs = append(txs, tx)
			metrics.ShortChainTxs++
			continue
		}

		paymentSeed := byte(120 + (output.RecipientTag % 80))
		tx, changeValue, err := buildPaymentWithChangeTx(output.SpenderSeed, output.OutPoint, output.Value, pubKeyForSeed(paymentSeed), pubKeyForSeed(output.SpenderSeed), consensus.ParamsForProfile(svc.Profile()))
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
	keyHash := pubKeyForSeed(2)
	txs := make([]types.Transaction, 0, txCount)
	for _, output := range outputs {
		prevOut := output.OutPoint
		prevValue := output.Value
		for depth := 0; depth < packageDepth && len(txs) < txCount; depth++ {
			tx, err := buildChildTx(2, prevOut, prevValue, keyHash, consensus.ParamsForProfile(svc.Profile()))
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
		parent, err := buildChildTx(2, output.OutPoint, output.Value, pubKeyForSeed(3), consensus.ParamsForProfile(svc.Profile()))
		if err != nil {
			return nil, err
		}
		parents = append(parents, parent)
		if len(parents)+len(children) >= txCount {
			continue
		}
		parentOut := types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}
		child, err := buildChildTx(3, parentOut, parent.Base.Outputs[0].ValueAtoms, pubKeyForSeed(4), consensus.ParamsForProfile(svc.Profile()))
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

func fundingBatchCounts(total, batchSize int) []int {
	if total <= 0 || batchSize <= 0 {
		return nil
	}
	batches := make([]int, 0, (total+batchSize-1)/batchSize)
	for remaining := total; remaining > 0; remaining -= batchSize {
		batches = append(batches, min(batchSize, remaining))
	}
	return batches
}

func canSeedTxCount(txCount int, coinbaseValue uint64) (bool, error) {
	if txCount <= 0 || coinbaseValue < uint64(txCount) {
		return false, nil
	}
	perOutput := coinbaseValue / uint64(txCount)
	if perOutput == 0 {
		return false, nil
	}
	_, err := buildChildTx(2, types.OutPoint{TxID: [32]byte{1}, Vout: 0}, perOutput, pubKeyForSeed(3), consensus.RegtestParams())
	if err != nil {
		if errors.Is(err, consensus.ErrInputsLessThanOutputs) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
