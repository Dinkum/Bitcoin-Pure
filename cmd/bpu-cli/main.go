package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	netpprof "net/http/pprof"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"bitcoin-pure/benchmarks"
	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/wallet"
)

type genesisFixture struct {
	Profile                      string `json:"profile"`
	ExpectedHeaderHashHex        string `json:"expected_header_hash_hex"`
	ExpectedTxIDHex              string `json:"expected_txid_hex"`
	ExpectedAuthIDHex            string `json:"expected_authid_hex"`
	ExpectedUTXORootAfterGenesis string `json:"expected_utxo_root_after_genesis_hex"`
	BlockHex                     string `json:"block_hex"`
}

type loadedGenesisFixture struct {
	Fixture genesisFixture
	Block   types.Block
}

type chainFixture struct {
	Profile                  string   `json:"profile"`
	GenesisFixture           string   `json:"genesis_fixture"`
	Blocks                   []string `json:"blocks"`
	ExpectedTipHeight        uint64   `json:"expected_tip_height"`
	ExpectedTipHeaderHashHex string   `json:"expected_tip_header_hash_hex"`
	ExpectedTipUTXORootHex   string   `json:"expected_tip_utxo_root_hex"`
	ExpectedUTXOCount        int      `json:"expected_utxo_count"`
	ExpectedBlockHashesHex   []string `json:"expected_block_hashes_hex"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usageError()
	}
	switch args[0] {
	case "serve":
		return runServe(args[1:])
	case "bench":
		return runBench(args[1:])
	case "wallet":
		return runWallet(args[1:])
	case "validate-tx":
		return runValidateTx(args[1:])
	case "validate-block":
		return runValidateBlock(args[1:])
	case "chain":
		return runChain(args[1:])
	case "snapshot":
		return runSnapshot(args[1:])
	case "config":
		return runConfig(args[1:])
	default:
		return usageError()
	}
}

func runConfig(args []string) error {
	if len(args) == 0 {
		return errors.New("missing config subcommand")
	}
	switch args[0] {
	case "normalize":
		return runConfigNormalize(args[1:])
	default:
		return errors.New("unknown config subcommand")
	}
}

func runConfigNormalize(args []string) error {
	fs := flag.NewFlagSet("config normalize", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	inPath := fs.String("in", "", "")
	outPath := fs.String("out", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*outPath) == "" {
		return errors.New("usage: bpu-cli config normalize --out PATH [--in PATH]")
	}

	cfg := config.Default()
	if strings.TrimSpace(*inPath) != "" {
		loaded, err := config.Load(strings.TrimSpace(*inPath))
		if err != nil {
			return err
		}
		cfg = loaded
	}
	return config.Save(strings.TrimSpace(*outPath), cfg)
}

func runBench(args []string) error {
	if len(args) == 0 {
		return errors.New("missing bench subcommand")
	}
	switch args[0] {
	case "e2e":
		return runBenchE2E(args[1:])
	case "micro":
		return runBenchMicro(args[1:])
	case "gate":
		return runBenchGate(args[1:])
	case "sim":
		return runBenchSim(args[1:])
	default:
		return errors.New("unknown bench subcommand (supported: e2e, micro, gate, sim; use `go test ./benchmarks -run '^$' -bench ...` for the raw in-process throughput loop)")
	}
}

func runBenchE2E(args []string) error {
	defaults := benchmarks.DefaultE2EOptions()
	fs := flag.NewFlagSet("bench e2e", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profileRaw := fs.String("profile", string(defaults.Profile), "")
	nodes := fs.Int("nodes", defaults.NodeCount, "")
	topologyRaw := fs.String("topology", string(defaults.Topology), "")
	batchSize := fs.Int("batch-size", defaults.BatchSize, "")
	txsPerBlock := fs.Int("txs-per-block", defaults.TxsPerBlock, "")
	blocks := fs.Int("blocks", defaults.BlockCount, "")
	blockInterval := fs.Duration("block-interval", defaults.BlockInterval, "")
	miningRaw := fs.String("mining", string(defaults.MiningMode), "")
	txOrigin := fs.String("tx-origin", string(defaults.TxOriginSpread), "")
	steadyStateBacklog := fs.Bool("steady-state-backlog", defaults.SteadyStateBacklog, "")
	timeout := fs.Duration("timeout", defaults.Timeout, "")
	reportPath := fs.String("report", "", "")
	markdownPath := fs.String("markdown", "", "")
	dbRoot := fs.String("db-root", "", "")
	profileDir := fs.String("profile-dir", "", "")
	suppressLogs := fs.Bool("suppress-logs", true, "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	profile, err := types.ParseChainProfile(*profileRaw)
	if err != nil {
		return err
	}
	opts := benchmarks.RunOptions{
		Profile:            profile,
		NodeCount:          *nodes,
		Topology:           benchmarks.Topology(*topologyRaw),
		BatchSize:          *batchSize,
		TxsPerBlock:        *txsPerBlock,
		BlockCount:         *blocks,
		BlockInterval:      *blockInterval,
		MiningMode:         benchmarks.MiningMode(*miningRaw),
		TxOriginSpread:     benchmarks.TxOriginSpread(*txOrigin),
		SteadyStateBacklog: *steadyStateBacklog,
		Timeout:            *timeout,
		DBRoot:             *dbRoot,
		ProfileDir:         *profileDir,
		SuppressLogs:       *suppressLogs,
		ProgressWriter:     os.Stderr,
	}

	report, err := benchmarks.RunE2E(context.Background(), opts)
	if err != nil {
		return err
	}
	if *reportPath == "" || *markdownPath == "" {
		base := filepath.Join("benchmarks", "reports", time.Now().UTC().Format("20060102-150405")+"-e2e-"+*miningRaw)
		if *reportPath == "" {
			*reportPath = base + ".json"
		}
		if *markdownPath == "" {
			*markdownPath = base + ".md"
		}
	}
	if err := benchmarks.WriteReportFiles(report, *reportPath, *markdownPath); err != nil {
		return err
	}

	printBenchmarkReport(report, *reportPath, *markdownPath)
	return nil
}

func runBenchMicro(args []string) error {
	defaults := benchmarks.DefaultMicroBenchOptions()
	fs := flag.NewFlagSet("bench micro", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	benchRE := fs.String("bench", defaults.Bench, "")
	count := fs.Int("count", defaults.Count, "")
	benchtime := fs.String("benchtime", "", "")
	reportPath := fs.String("report", "", "")
	markdownPath := fs.String("markdown", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	report, err := benchmarks.RunMicroBenchmarks(context.Background(), benchmarks.MicroBenchOptions{
		Package:   defaults.Package,
		Bench:     *benchRE,
		Count:     *count,
		Benchtime: *benchtime,
	})
	if err != nil {
		return err
	}
	if *reportPath == "" || *markdownPath == "" {
		defaultJSON, defaultMarkdown := benchmarks.DefaultMicroReportPaths(time.Now().UTC())
		if *reportPath == "" {
			*reportPath = defaultJSON
		}
		if *markdownPath == "" {
			*markdownPath = defaultMarkdown
		}
	}
	if err := benchmarks.WriteMicroReportFiles(report, *reportPath, *markdownPath); err != nil {
		return err
	}
	printMicroBenchReport(report, *reportPath, *markdownPath)
	return nil
}

func runBenchGate(args []string) error {
	if len(args) == 0 {
		return errors.New("missing bench gate subcommand")
	}
	switch args[0] {
	case "run":
		return runBenchGateRun(args[1:])
	case "compare":
		return runBenchGateCompare(args[1:])
	default:
		return errors.New("unknown bench gate subcommand (supported: run, compare)")
	}
}

func runBenchGateRun(args []string) error {
	fs := flag.NewFlagSet("bench gate run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	budgetPath := fs.String("budget", benchmarks.DefaultPerfGateBudgetPath, "")
	outDir := fs.String("out-dir", filepath.Join("benchmarks", "reports", "gate", time.Now().UTC().Format("20060102-150405")), "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	budget, err := benchmarks.LoadPerfGateBudget(*budgetPath)
	if err != nil {
		return err
	}
	reports, err := budget.Run(context.Background(), *outDir)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "perf_gate_budget: %s\n", filepath.Clean(*budgetPath))
	fmt.Fprintf(os.Stdout, "micro_report_json: %s\n", reports.MicroJSON)
	fmt.Fprintf(os.Stdout, "micro_report_markdown: %s\n", reports.MicroMD)
	fmt.Fprintf(os.Stdout, "e2e_report_json: %s\n", reports.E2EJSON)
	fmt.Fprintf(os.Stdout, "e2e_report_markdown: %s\n", reports.E2EMD)
	return nil
}

func runBenchGateCompare(args []string) error {
	fs := flag.NewFlagSet("bench gate compare", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	budgetPath := fs.String("budget", benchmarks.DefaultPerfGateBudgetPath, "")
	baselineDir := fs.String("baseline-dir", "", "")
	candidateDir := fs.String("candidate-dir", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*baselineDir) == "" || strings.TrimSpace(*candidateDir) == "" {
		return errors.New("usage: bpu-cli bench gate compare --baseline-dir PATH --candidate-dir PATH [--budget PATH]")
	}

	budget, err := benchmarks.LoadPerfGateBudget(*budgetPath)
	if err != nil {
		return err
	}
	comparison, err := benchmarks.ComparePerfGateReports(budget, *baselineDir, *candidateDir)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, benchmarks.RenderPerfGateComparison(comparison))
	return comparison.Error()
}

func runBenchSim(args []string) error {
	defaults := benchmarks.DefaultSimulationOptions()
	fs := flag.NewFlagSet("bench sim", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	nodes := fs.Int("nodes", defaults.NodeCount, "")
	topology := fs.String("topology", string(defaults.Topology), "")
	seed := fs.Int64("seed", defaults.Seed, "")
	txs := fs.Int("txs", defaults.TxCount, "")
	blocks := fs.Int("blocks", defaults.BlockCount, "")
	baseLatency := fs.Duration("base-latency", defaults.BaseLatency, "")
	latencyJitter := fs.Duration("latency-jitter", defaults.LatencyJitter, "")
	txProcessingDelay := fs.Duration("tx-processing-delay", defaults.TxProcessingDelay, "")
	blockProcessingDelay := fs.Duration("block-processing-delay", defaults.BlockProcessingDelay, "")
	txSpacing := fs.Duration("tx-spacing", defaults.TxSpacing, "")
	blockSpacing := fs.Duration("block-spacing", defaults.BlockSpacing, "")
	churnEvents := fs.Int("churn-events", defaults.ChurnEvents, "")
	churnDuration := fs.Duration("churn-duration", defaults.ChurnDuration, "")
	smallWorldDegree := fs.Int("small-world-degree", defaults.SmallWorldDegree, "")
	reportPath := fs.String("report", "", "")
	markdownPath := fs.String("markdown", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	report, err := benchmarks.RunSimulation(context.Background(), benchmarks.SimulationOptions{
		NodeCount:            *nodes,
		Topology:             benchmarks.SimulationTopology(*topology),
		Seed:                 *seed,
		TxCount:              *txs,
		BlockCount:           *blocks,
		BaseLatency:          *baseLatency,
		LatencyJitter:        *latencyJitter,
		TxProcessingDelay:    *txProcessingDelay,
		BlockProcessingDelay: *blockProcessingDelay,
		TxSpacing:            *txSpacing,
		BlockSpacing:         *blockSpacing,
		ChurnEvents:          *churnEvents,
		ChurnDuration:        *churnDuration,
		SmallWorldDegree:     *smallWorldDegree,
	})
	if err != nil {
		return err
	}
	if *reportPath == "" || *markdownPath == "" {
		base := filepath.Join("benchmarks", "reports", time.Now().UTC().Format("20060102-150405")+"-sim")
		if *reportPath == "" {
			*reportPath = base + ".json"
		}
		if *markdownPath == "" {
			*markdownPath = base + ".md"
		}
	}
	if err := benchmarks.WriteSimulationReportFiles(report, *reportPath, *markdownPath); err != nil {
		return err
	}
	printSimulationReport(report, *reportPath, *markdownPath)
	return nil
}

func printBenchmarkReport(report *benchmarks.Report, reportPath, markdownPath string) {
	fmt.Printf("benchmark: %s\n", report.Benchmark)
	if report.Mode != "" {
		fmt.Printf("mode: %s\n", report.Mode)
	}
	if report.Mining != "" {
		fmt.Printf("mining: %s\n", report.Mining)
	}
	fmt.Printf("profile: %s\n", report.Profile)
	fmt.Printf("nodes: %d\n", report.NodeCount)
	if report.Topology != "" {
		fmt.Printf("topology: %s\n", report.Topology)
	}
	fmt.Printf("tx_count: %d\n", report.TxCount)
	if report.BatchSize > 0 {
		fmt.Printf("batch_size: %d\n", report.BatchSize)
	}
	if report.TxsPerBlock > 0 {
		fmt.Printf("txs_per_block: %d\n", report.TxsPerBlock)
	}
	if report.Benchmark == string(benchmarks.BenchmarkE2E) && report.TargetBlocks > 0 {
		fmt.Printf("blocks: %d\n", report.TargetBlocks)
	}
	if report.Benchmark == string(benchmarks.BenchmarkE2E) && report.BlockIntervalMS > 0 {
		fmt.Printf("block_interval_ms: %.2f\n", report.BlockIntervalMS)
	}
	if report.Benchmark == string(benchmarks.BenchmarkThroughput) && report.RequestedDurationMS > 0 {
		fmt.Printf("requested_duration_ms: %.2f\n", report.RequestedDurationMS)
	}
	fmt.Printf("admission_tps: %.2f\n", report.AdmissionTPS)
	fmt.Printf("completion_tps: %.2f\n", report.CompletionTPS)
	if report.ConfirmedProcessingTPS > 0 {
		fmt.Printf("confirmed_processing_tps: %.2f\n", report.ConfirmedProcessingTPS)
	}
	if report.ConfirmedWallTPS > 0 {
		fmt.Printf("confirmed_wall_tps: %.2f\n", report.ConfirmedWallTPS)
	}
	if report.SyntheticIntervalTPS > 0 {
		fmt.Printf("synthetic_interval_tps: %.2f\n", report.SyntheticIntervalTPS)
	}
	fmt.Printf("admission_duration_ms: %.2f\n", report.AdmissionDurationMS)
	fmt.Printf("completion_duration_ms: %.2f\n", report.CompletionDurationMS)
	if len(report.Profiling.Artifacts) != 0 {
		fmt.Printf("profile_dir: %s\n", filepath.Dir(report.Profiling.Artifacts[0].Path))
	}
	fmt.Printf("report_json: %s\n", reportPath)
	fmt.Printf("report_markdown: %s\n", markdownPath)
	fmt.Println()
	fmt.Println(benchmarks.RenderASCIISummary(report))
}

func printMicroBenchReport(report *benchmarks.MicroBenchReport, reportPath, markdownPath string) {
	fmt.Printf("benchmark: micro\n")
	fmt.Printf("package: %s\n", report.Package)
	fmt.Printf("bench_regex: %s\n", report.Bench)
	if report.Count > 0 {
		fmt.Printf("count: %d\n", report.Count)
	}
	if report.Benchtime != "" {
		fmt.Printf("benchtime: %s\n", report.Benchtime)
	}
	fmt.Printf("duration_ms: %.2f\n", report.DurationMS)
	fmt.Printf("report_json: %s\n", reportPath)
	fmt.Printf("report_markdown: %s\n", markdownPath)
	fmt.Println()
	fmt.Println("microbenchmarks:")
	for _, bench := range report.Benchmarks {
		name := bench.Name
		if bench.Procs > 0 {
			name = fmt.Sprintf("%s (P=%d)", name, bench.Procs)
		}
		fmt.Printf("  - %s: %.0f ns/op, %.0f B/op, %.0f allocs/op\n", name, bench.NsPerOp, bench.BytesPerOp, bench.AllocsPerOp)
	}
}

func printSimulationReport(report *benchmarks.SimulationReport, reportPath, markdownPath string) {
	fmt.Printf("benchmark: sim\n")
	fmt.Printf("nodes: %d\n", report.Config.NodeCount)
	fmt.Printf("topology: %s\n", report.Config.Topology)
	fmt.Printf("seed: %d\n", report.Config.Seed)
	fmt.Printf("tx_count: %d\n", report.Config.TxCount)
	fmt.Printf("block_count: %d\n", report.Config.BlockCount)
	fmt.Printf("base_latency_ms: %.2f\n", report.Config.BaseLatencyMS)
	fmt.Printf("latency_jitter_ms: %.2f\n", report.Config.LatencyJitterMS)
	fmt.Printf("report_json: %s\n", reportPath)
	fmt.Printf("report_markdown: %s\n", markdownPath)
	fmt.Println()
	fmt.Println(benchmarks.RenderSimulationASCIISummary(report))
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	profileRaw := fs.String("profile", "", "")
	db := fs.String("db", "", "")
	logPath := fs.String("log", "", "")
	logLevel := fs.String("log-level", "", "")
	logFormat := fs.String("log-format", "", "")
	pprofAddr := fs.String("pprof", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	rpcReadTimeout := fs.Duration("rpc-read-timeout", 0, "")
	rpcWriteTimeout := fs.Duration("rpc-write-timeout", 0, "")
	rpcHeaderTimeout := fs.Duration("rpc-header-timeout", 0, "")
	rpcIdleTimeout := fs.Duration("rpc-idle-timeout", 0, "")
	rpcMaxHeaderBytes := fs.Int("rpc-max-header-bytes", 0, "")
	rpcMaxBodyBytes := fs.Int("rpc-max-body-bytes", 0, "")
	p2pAddr := fs.String("p2p", "", "")
	peerList := fs.String("peers", "", "")
	maxInboundPeers := fs.Int("max-inbound-peers", 0, "")
	maxOutboundPeers := fs.Int("max-outbound-peers", 0, "")
	handshakeTimeout := fs.Duration("handshake-timeout", 0, "")
	stallTimeout := fs.Duration("stall-timeout", 0, "")
	maxMessageBytes := fs.Int("max-message-bytes", 0, "")
	minRelayFeePerByte := fs.Uint64("min-relay-fee-per-byte", 0, "")
	maxMempoolBytes := fs.Int("max-mempool-bytes", 0, "")
	avalancheMode := fs.String("avalanche", "", "")
	dandelionMode := fs.String("dandelion", "", "")
	avalancheK := fs.Int("avalanche-k", 0, "")
	avalancheAlphaNumerator := fs.Int("avalanche-alpha-numerator", 0, "")
	avalancheAlphaDenominator := fs.Int("avalanche-alpha-denominator", 0, "")
	avalancheBeta := fs.Int("avalanche-beta", 0, "")
	avalanchePollInterval := fs.Duration("avalanche-poll-interval", 0, "")
	miningMode := fs.String("mining", "", "")
	minerWorkers := fs.Int("miner-workers", 0, "")
	minerPubKeyHex := fs.String("miner-pubkey", "", "")
	genesisFixture := fs.String("genesis", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Default()
	resolvedConfigPath := strings.TrimSpace(*configPath)
	if resolvedConfigPath == "" {
		for _, candidate := range config.DefaultPathCandidates() {
			if fileExists(candidate) {
				resolvedConfigPath = candidate
				break
			}
		}
	}
	if resolvedConfigPath != "" {
		loaded, err := config.Load(resolvedConfigPath)
		if err != nil {
			return err
		}
		cfg = loaded
	}
	if *profileRaw != "" {
		cfg.Profile = *profileRaw
	}
	if *db != "" {
		cfg.DBPath = *db
	}
	if *logPath != "" {
		cfg.LogPath = *logPath
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if *logFormat != "" {
		cfg.LogFormat = *logFormat
	}
	if *pprofAddr != "" {
		cfg.PprofAddr = *pprofAddr
	}
	if *rpcAddr != "" {
		cfg.RPCAddr = *rpcAddr
	}
	if *rpcAuthToken != "" {
		cfg.RPCAuthToken = *rpcAuthToken
	}
	if *rpcReadTimeout > 0 {
		cfg.RPCReadTimeoutMS = int(rpcReadTimeout.Milliseconds())
	}
	if *rpcWriteTimeout > 0 {
		cfg.RPCWriteTimeoutMS = int(rpcWriteTimeout.Milliseconds())
	}
	if *rpcHeaderTimeout > 0 {
		cfg.RPCHeaderTimeoutMS = int(rpcHeaderTimeout.Milliseconds())
	}
	if *rpcIdleTimeout > 0 {
		cfg.RPCIdleTimeoutMS = int(rpcIdleTimeout.Milliseconds())
	}
	if *rpcMaxHeaderBytes > 0 {
		cfg.RPCMaxHeaderBytes = *rpcMaxHeaderBytes
	}
	if *rpcMaxBodyBytes > 0 {
		cfg.RPCMaxBodyBytes = *rpcMaxBodyBytes
	}
	if *p2pAddr != "" {
		cfg.P2PAddr = *p2pAddr
	}
	if *peerList != "" {
		cfg.Peers = splitCSV(*peerList)
	}
	if *maxInboundPeers > 0 {
		cfg.MaxInboundPeers = *maxInboundPeers
	}
	if *maxOutboundPeers > 0 {
		cfg.MaxOutboundPeers = *maxOutboundPeers
	}
	if *handshakeTimeout > 0 {
		cfg.HandshakeTimeoutMS = int(handshakeTimeout.Milliseconds())
	}
	if *stallTimeout > 0 {
		cfg.StallTimeoutMS = int(stallTimeout.Milliseconds())
	}
	if *maxMessageBytes > 0 {
		cfg.MaxMessageBytes = *maxMessageBytes
	}
	if *minRelayFeePerByte > 0 {
		cfg.MinRelayFeePerByte = *minRelayFeePerByte
	}
	if *maxMempoolBytes > 0 {
		cfg.MaxMempoolBytes = *maxMempoolBytes
	}
	if *avalancheMode != "" {
		switch strings.ToLower(strings.TrimSpace(*avalancheMode)) {
		case "on", "off":
			cfg.AvalancheMode = strings.ToLower(strings.TrimSpace(*avalancheMode))
		default:
			return fmt.Errorf("invalid --avalanche value %q: want on or off", *avalancheMode)
		}
	}
	if *dandelionMode != "" {
		switch strings.ToLower(strings.TrimSpace(*dandelionMode)) {
		case "on":
			cfg.DandelionEnabled = true
		case "off":
			cfg.DandelionEnabled = false
		default:
			return fmt.Errorf("invalid --dandelion value %q: want on or off", *dandelionMode)
		}
	}
	if *avalancheK > 0 {
		cfg.AvalancheKSample = *avalancheK
	}
	if *avalancheAlphaNumerator > 0 {
		cfg.AvalancheAlphaNumerator = *avalancheAlphaNumerator
	}
	if *avalancheAlphaDenominator > 0 {
		cfg.AvalancheAlphaDenominator = *avalancheAlphaDenominator
	}
	if *avalancheBeta > 0 {
		cfg.AvalancheBeta = *avalancheBeta
	}
	if *avalanchePollInterval > 0 {
		cfg.AvalanchePollIntervalMS = int(avalanchePollInterval.Milliseconds())
	}
	if *miningMode != "" {
		switch strings.ToLower(strings.TrimSpace(*miningMode)) {
		case "on":
			cfg.MinerEnabled = true
		case "off":
			cfg.MinerEnabled = false
		default:
			return fmt.Errorf("invalid --mining value %q: want on or off", *miningMode)
		}
	}
	if *minerWorkers > 0 {
		cfg.MinerWorkers = *minerWorkers
		cfg.MinerEnabled = true
	}
	if *minerPubKeyHex != "" {
		cfg.MinerPubKeyHex = *minerPubKeyHex
	}
	if *genesisFixture != "" {
		cfg.GenesisFixture = *genesisFixture
	}

	profile, err := types.ParseChainProfile(cfg.Profile)
	if err != nil {
		return err
	}
	if profile == types.BenchNet {
		return errors.New("benchnet is benchmark-only; use `bpu-cli bench ...` instead of `serve`")
	}
	if cfg.GenesisFixture == "" {
		cfg.GenesisFixture = defaultGenesisFixture(profile)
	}
	if cfg.LogPath == "" {
		cfg.LogPath = deriveLogPath(cfg.DBPath)
	}
	logger, logCloser, err := logging.Setup(logging.Config{
		Path:         cfg.LogPath,
		Level:        cfg.LogLevel,
		Format:       cfg.LogFormat,
		MaxSizeBytes: logging.DefaultMaxSizeBytes,
	})
	if err != nil {
		return err
	}
	defer logCloser.Close()

	pprofServer, err := maybeStartPprofServer(cfg.PprofAddr, logger)
	if err != nil {
		return err
	}
	if pprofServer != nil {
		defer pprofServer.Close()
	}

	loadedGenesis, err := loadGenesisFixtureFromPath(cfg.GenesisFixture)
	if err != nil {
		return err
	}
	if addr, walletPath, err := ensureMiningWalletProvisioned(resolvedConfigPath, &cfg); err != nil {
		return err
	} else if addr.PubKeyHex != "" {
		logger.Info("provisioned mining wallet",
			slog.String("wallet", "miner"),
			slog.String("wallet_path", walletPath),
			slog.String("receive_address", addr.Address),
			slog.String("pubkey", addr.PubKeyHex),
		)
	}
	pubKey, err := node.ParseMinerPubKey(cfg.MinerPubKeyHex)
	if err != nil {
		return err
	}
	svc, err := node.OpenService(node.ServiceConfig{
		Profile:                   profile,
		DBPath:                    cfg.DBPath,
		ThroughputSummaryInterval: time.Duration(cfg.ThroughputSummaryIntervalMS) * time.Millisecond,
		RPCAddr:                   cfg.RPCAddr,
		RPCAuthToken:              cfg.RPCAuthToken,
		RPCReadTimeout:            time.Duration(cfg.RPCReadTimeoutMS) * time.Millisecond,
		RPCWriteTimeout:           time.Duration(cfg.RPCWriteTimeoutMS) * time.Millisecond,
		RPCHeaderTimeout:          time.Duration(cfg.RPCHeaderTimeoutMS) * time.Millisecond,
		RPCIdleTimeout:            time.Duration(cfg.RPCIdleTimeoutMS) * time.Millisecond,
		RPCMaxHeaderBytes:         cfg.RPCMaxHeaderBytes,
		RPCMaxBodyBytes:           cfg.RPCMaxBodyBytes,
		P2PAddr:                   cfg.P2PAddr,
		Peers:                     cfg.Peers,
		MaxInboundPeers:           cfg.MaxInboundPeers,
		MaxOutboundPeers:          cfg.MaxOutboundPeers,
		HandshakeTimeout:          time.Duration(cfg.HandshakeTimeoutMS) * time.Millisecond,
		StallTimeout:              time.Duration(cfg.StallTimeoutMS) * time.Millisecond,
		MaxMessageBytes:           cfg.MaxMessageBytes,
		MinRelayFeePerByte:        cfg.MinRelayFeePerByte,
		MaxMempoolBytes:           cfg.MaxMempoolBytes,
		MaxAncestors:              cfg.MaxAncestors,
		MaxDescendants:            cfg.MaxDescendants,
		MaxOrphans:                cfg.MaxOrphans,
		AvalancheMode:             cfg.AvalancheMode,
		AvalancheKSample:          cfg.AvalancheKSample,
		AvalancheAlphaNumerator:   cfg.AvalancheAlphaNumerator,
		AvalancheAlphaDenominator: cfg.AvalancheAlphaDenominator,
		AvalancheBeta:             cfg.AvalancheBeta,
		AvalanchePollInterval:     time.Duration(cfg.AvalanchePollIntervalMS) * time.Millisecond,
		DandelionEnabled:          cfg.DandelionEnabled,
		MinerEnabled:              cfg.MinerEnabled,
		MinerWorkers:              cfg.MinerWorkers,
		MinerPubKey:               pubKey,
		GenesisFixture:            cfg.GenesisFixture,
	}, &loadedGenesis.Block)
	if err != nil {
		return err
	}
	logger.Info("node service configured",
		slog.String("profile", profile.String()),
		slog.String("db_path", cfg.DBPath),
		slog.String("log_path", cfg.LogPath),
		slog.String("rpc_addr", cfg.RPCAddr),
		slog.String("p2p_addr", cfg.P2PAddr),
		slog.Int("peers", len(cfg.Peers)),
		slog.Int("max_inbound_peers", cfg.MaxInboundPeers),
		slog.Int("max_outbound_peers", cfg.MaxOutboundPeers),
		slog.Int("max_ancestors", cfg.MaxAncestors),
		slog.Int("max_descendants", cfg.MaxDescendants),
		slog.Int("max_mempool_bytes", cfg.MaxMempoolBytes),
		slog.Int("max_orphans", cfg.MaxOrphans),
		slog.String("avalanche_mode", cfg.AvalancheMode),
		slog.Bool("dandelion_enabled", cfg.DandelionEnabled),
		slog.Bool("miner_enabled", cfg.MinerEnabled),
		slog.Int("miner_workers", cfg.MinerWorkers),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("profile: %s\n", profile)
	fmt.Printf("db: %s\n", cfg.DBPath)
	if cfg.RPCAddr != "" {
		fmt.Printf("rpc: %s\n", cfg.RPCAddr)
	}
	if cfg.P2PAddr != "" {
		fmt.Printf("p2p: %s\n", cfg.P2PAddr)
	}
	if cfg.PprofAddr != "" {
		fmt.Printf("pprof: %s\n", cfg.PprofAddr)
	}
	fmt.Printf("log: %s\n", cfg.LogPath)
	fmt.Printf("log_format: %s\n", cfg.LogFormat)
	fmt.Printf("avalanche: %s\n", cfg.AvalancheMode)
	fmt.Printf("dandelion: %t\n", cfg.DandelionEnabled)
	if cfg.MinerEnabled {
		if cfg.MinerWorkers > 0 {
			fmt.Printf("miner_workers: %d\n", cfg.MinerWorkers)
		} else {
			fmt.Println("miner_workers: auto")
		}
	}
	return svc.Start(ctx)
}

type pprofServer struct {
	server *http.Server
	ln     net.Listener
}

func maybeStartPprofServer(addr string, logger *slog.Logger) (*pprofServer, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, nil
	}
	if err := validateLoopbackListenAddr(addr); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", netpprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", netpprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", netpprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", netpprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", netpprof.Trace)
	mux.Handle("/debug/pprof/allocs", netpprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", netpprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", netpprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", netpprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", netpprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", netpprof.Handler("threadcreate"))
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && logger != nil {
			logger.Warn("pprof server stopped unexpectedly", slog.String("addr", addr), slog.Any("error", err))
		}
	}()
	if logger != nil {
		logger.Info("pprof server listening", slog.String("addr", addr))
	}
	return &pprofServer{server: srv, ln: ln}, nil
}

func (s *pprofServer) Close() error {
	if s == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	runtime.SetMutexProfileFraction(0)
	runtime.SetBlockProfileRate(0)
	return err
}

func validateLoopbackListenAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" || strings.EqualFold(host, "localhost") {
		return nil
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("pprof listen addr must use loopback host: %s", addr)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("pprof listen addr must use loopback host: %s", addr)
	}
	return nil
}

func runValidateTx(args []string) error {
	fs := flag.NewFlagSet("validate-tx", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	rawHex := fs.String("hex", "", "")
	file := fs.String("file", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := readHexInput(*rawHex, *file)
	if err != nil {
		return err
	}
	tx, err := consensus.DecodeTxHex(raw, types.DefaultCodecLimits())
	if err != nil {
		return err
	}
	fmt.Println("tx decoded")
	fmt.Printf("inputs: %d\n", len(tx.Base.Inputs))
	fmt.Printf("outputs: %d\n", len(tx.Base.Outputs))
	fmt.Printf("txid: %x\n", consensus.TxID(&tx))
	fmt.Printf("authid: %x\n", consensus.AuthID(&tx))
	return nil
}

func runValidateBlock(args []string) error {
	fs := flag.NewFlagSet("validate-block", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	rawHex := fs.String("hex", "", "")
	file := fs.String("file", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := readHexInput(*rawHex, *file)
	if err != nil {
		return err
	}
	block, err := consensus.DecodeBlockHex(raw, types.DefaultCodecLimits())
	if err != nil {
		return err
	}
	fmt.Println("block decoded")
	fmt.Printf("txs: %d\n", len(block.Txs))
	fmt.Printf("header_hash: %x\n", consensus.HeaderHash(&block.Header))
	return nil
}

func runChain(args []string) error {
	if len(args) == 0 {
		return errors.New("missing chain subcommand")
	}
	switch args[0] {
	case "init":
		return runChainInit(args[1:])
	case "sync-fixture":
		return runChainSyncFixture(args[1:])
	case "validate-headers-fixture":
		return runChainValidateHeadersFixture(args[1:])
	case "validate-fixture":
		return runChainValidateFixture(args[1:])
	default:
		return errors.New("unknown chain subcommand")
	}
}

func runWallet(args []string) error {
	if len(args) == 0 {
		return errors.New("missing wallet subcommand")
	}
	switch args[0] {
	case "create":
		return runWalletCreate(args[1:])
	case "list":
		return runWalletList(args[1:])
	case "balance":
		return runWalletBalance(args[1:])
	case "history":
		return runWalletHistory(args[1:])
	case "fee":
		return runWalletFee(args[1:])
	case "receive":
		return runWalletReceive(args[1:])
	case "send":
		return runWalletSend(args[1:])
	case "cpfp":
		return runWalletCPFP(args[1:])
	default:
		return errors.New("unknown wallet subcommand")
	}
}

func runWalletCreate(args []string) error {
	fs := flag.NewFlagSet("wallet create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	family := fs.String("family", wallet.AddressFamilyXOnly, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet create [--family xonly|pq] [--config PATH] [--wallet-dir DIR] <name>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	outputType, err := wallet.ParseAddressFamily(*family)
	if err != nil {
		return err
	}
	entry, addr, err := store.CreateWalletWithType(fs.Arg(0), outputType)
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", entry.Name)
	fmt.Printf("created_at: %s\n", entry.CreatedAt.Format(time.RFC3339))
	fmt.Printf("family: %s\n", wallet.AddressFamilyLabel(addr.OutputType()))
	fmt.Printf("receive_address: %s\n", addr.Address)
	printWalletAddressDetails(addr)
	return nil
}

func runWalletList(args []string) error {
	fs := flag.NewFlagSet("wallet list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	wallets := store.List()
	if len(wallets) == 0 {
		fmt.Println("no wallets")
		return nil
	}
	for _, entry := range wallets {
		receive := "-"
		if latest := entry.LatestReceiveAddress(); latest != nil {
			receive = latest.Address
		}
		fmt.Printf("%s  addresses=%d  pending=%d  receive=%s\n", entry.Name, len(entry.Addresses), len(entry.Pending), receive)
	}
	return nil
}

func runWalletBalance(args []string) error {
	fs := flag.NewFlagSet("wallet balance", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet balance [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] <wallet>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken))
	if err := reconcileWalletPending(store, client, fs.Arg(0)); err != nil {
		return err
	}
	watchItems, err := store.SpendableWatchItems(fs.Arg(0))
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return err
	}
	balance, err := store.Balance(fs.Arg(0), utxos)
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", fs.Arg(0))
	fmt.Printf("confirmed: %d\n", balance.Confirmed)
	fmt.Printf("available: %d\n", balance.Available)
	fmt.Printf("reserved: %d\n", balance.Reserved)
	fmt.Printf("pending_txs: %d\n", balance.PendingCount)
	fmt.Printf("addresses: %d\n", balance.AddressCount)
	return nil
}

func runWalletHistory(args []string) error {
	fs := flag.NewFlagSet("wallet history", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	limit := fs.Int("limit", 20, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet history [--limit N] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] <wallet>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	watchItems, err := store.SpendableWatchItems(fs.Arg(0))
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken))
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, *limit)
	if err != nil {
		return err
	}
	if len(activity) == 0 {
		fmt.Println("no wallet activity")
		return nil
	}
	for _, item := range activity {
		fmt.Printf("%d  %s  tx=%s  received=%d  sent=%d  fee=%d  net=%d  %s\n",
			item.Height,
			item.Timestamp,
			item.TxID,
			item.Received,
			item.Sent,
			item.Fee,
			item.Net,
			item.BlockHash,
		)
	}
	return nil
}

func runWalletFee(args []string) error {
	fs := flag.NewFlagSet("wallet fee", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	targetBlocks := fs.Int("target-blocks", 1, "")
	txBytes := fs.Int("tx-bytes", 250, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken))
	feePerByte, err := rpcEstimateFee(client, *targetBlocks)
	if err != nil {
		return err
	}
	if *txBytes < 0 {
		return errors.New("tx-bytes must be non-negative")
	}
	fmt.Printf("target_blocks: %d\n", *targetBlocks)
	fmt.Printf("fee_per_byte: %d\n", feePerByte)
	fmt.Printf("estimated_fee: %d\n", feePerByte*uint64(*txBytes))
	return nil
}

func runWalletReceive(args []string) error {
	fs := flag.NewFlagSet("wallet receive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	family := fs.String("family", wallet.AddressFamilyXOnly, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet receive [--family xonly|pq] [--config PATH] [--wallet-dir DIR] <wallet>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	outputType, err := wallet.ParseAddressFamily(*family)
	if err != nil {
		return err
	}
	addr, err := store.NewReceiveAddressWithType(fs.Arg(0), outputType)
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", fs.Arg(0))
	fmt.Printf("family: %s\n", wallet.AddressFamilyLabel(addr.OutputType()))
	fmt.Printf("receive_address: %s\n", addr.Address)
	printWalletAddressDetails(addr)
	return nil
}

func runWalletSend(args []string) error {
	fs := flag.NewFlagSet("wallet send", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	from := fs.String("from", "", "")
	to := fs.String("to", "", "")
	amount := fs.Uint64("amount", 0, "")
	fee := fs.Uint64("fee", 0, "")
	targetBlocks := fs.Int("target-blocks", 1, "")
	targetMinutes := fs.Int("target-minutes", 0, "")
	priority := fs.String("priority", "", "")
	yes := fs.Bool("yes", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *from == "" || *to == "" || *amount == 0 {
		return errors.New("usage: bpu-cli wallet send --from NAME --to ADDRESS --amount ATOMS [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken))
	if err := reconcileWalletPending(store, client, *from); err != nil {
		return err
	}
	watchItems, err := store.SpendableWatchItems(*from)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return err
	}
	plan := wallet.SendPlan{}
	var feeQuote *walletFeeQuote
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
		}
		plan, err = store.BuildSend(*from, *to, *amount, *fee, utxos)
		if err != nil {
			return err
		}
	} else {
		quote, err := resolveWalletFeeQuote(client, walletFeeRequest{
			TargetBlocks:          *targetBlocks,
			TargetBlocksExplicit:  flagWasPassed(fs, "target-blocks"),
			TargetMinutes:         *targetMinutes,
			TargetMinutesExplicit: flagWasPassed(fs, "target-minutes"),
			Priority:              *priority,
			PriorityExplicit:      flagWasPassed(fs, "priority"),
			AllowInteractive:      stdinLooksInteractive() && !*yes,
		})
		if err != nil {
			return err
		}
		feeQuote = &quote
		plan, err = store.BuildSendAuto(*from, *to, *amount, quote.FeeRate, utxos)
		if err != nil {
			return err
		}
	}
	if err := maybeConfirmWalletAction(renderSendPreview(plan, feeQuote), *yes); err != nil {
		return err
	}
	var result struct {
		TxID string `json:"txid"`
		Fee  uint64 `json:"fee"`
	}
	if err := client.Call("submittx", map[string]string{"hex": plan.TransactionHex}, &result); err != nil {
		return err
	}
	reportedTxID, err := decodeHex32(result.TxID)
	if err != nil {
		return err
	}
	if reportedTxID != plan.TransactionID {
		return fmt.Errorf("submitted txid mismatch: planned %x, node returned %s", plan.TransactionID, result.TxID)
	}
	if err := store.MarkSubmitted(*from, reportedTxID, plan.Transaction, plan.Inputs); err != nil {
		return err
	}
	printWalletAction(renderSendResult(plan, result.TxID, feeQuote))
	return nil
}

func runWalletCPFP(args []string) error {
	fs := flag.NewFlagSet("wallet cpfp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	from := fs.String("from", "", "")
	parent := fs.String("txid", "", "")
	fee := fs.Uint64("fee", 0, "")
	targetBlocks := fs.Int("target-blocks", 1, "")
	targetMinutes := fs.Int("target-minutes", 0, "")
	priority := fs.String("priority", "", "")
	yes := fs.Bool("yes", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *from == "" || *parent == "" {
		return errors.New("usage: bpu-cli wallet cpfp --from NAME --txid PARENT_TXID [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	parentTxID, err := decodeHex32(*parent)
	if err != nil {
		return err
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken))
	if err := reconcileWalletPending(store, client, *from); err != nil {
		return err
	}
	var plan wallet.CPFPPlan
	var feeQuote *walletFeeQuote
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
		}
		plan, err = store.BuildCPFPWithExactFee(*from, parentTxID, *fee)
		if err != nil {
			return err
		}
	} else {
		quote, err := resolveWalletFeeQuote(client, walletFeeRequest{
			TargetBlocks:          *targetBlocks,
			TargetBlocksExplicit:  flagWasPassed(fs, "target-blocks"),
			TargetMinutes:         *targetMinutes,
			TargetMinutesExplicit: flagWasPassed(fs, "target-minutes"),
			Priority:              *priority,
			PriorityExplicit:      flagWasPassed(fs, "priority"),
			AllowInteractive:      stdinLooksInteractive() && !*yes,
		})
		if err != nil {
			return err
		}
		feeQuote = &quote
		plan, err = store.BuildCPFP(*from, parentTxID, quote.FeeRate)
		if err != nil {
			return err
		}
	}
	if err := maybeConfirmWalletAction(renderCPFPPreview(plan, feeQuote), *yes); err != nil {
		return err
	}
	var result struct {
		TxID string `json:"txid"`
		Fee  uint64 `json:"fee"`
	}
	if err := client.Call("submittx", map[string]string{"hex": plan.TransactionHex}, &result); err != nil {
		return err
	}
	reportedTxID, err := decodeHex32(result.TxID)
	if err != nil {
		return err
	}
	if reportedTxID != plan.TransactionID {
		return fmt.Errorf("submitted txid mismatch: planned %x, node returned %s", plan.TransactionID, result.TxID)
	}
	if err := store.MarkSubmitted(*from, reportedTxID, plan.Transaction, []wallet.SelectedInput{plan.Input}); err != nil {
		return err
	}
	printWalletAction(renderCPFPResult(plan, result.TxID, feeQuote))
	return nil
}

func runChainInit(args []string) error {
	fs := flag.NewFlagSet("chain init", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profileRaw := fs.String("profile", "mainnet", "")
	db := fs.String("db", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	profile, err := types.ParseChainProfile(*profileRaw)
	if err != nil {
		return err
	}
	params := consensus.ParamsForProfile(profile)
	loaded, err := loadGenesisFixture(profile)
	if err != nil {
		return err
	}

	var summary node.GenesisBootstrapSummary
	if *db != "" {
		state, err := node.OpenPersistentChainState(*db, profile)
		if err != nil {
			return err
		}
		defer state.Close()
		summary, err = state.InitializeFromGenesisBlock(&loaded.Block)
		if err != nil {
			return err
		}
	} else {
		state := node.NewChainState(profile)
		summary, err = state.InitializeFromGenesisBlock(&loaded.Block)
		if err != nil {
			return err
		}
	}

	fmt.Printf("profile: %s\n", profile)
	fmt.Printf("target_spacing_secs: %d\n", params.TargetSpacingSecs)
	fmt.Printf("asert_half_life_secs: %d\n", params.AsertHalfLifeSecs)
	fmt.Printf("pow_limit_bits: 0x%08x\n", params.PowLimitBits)
	fmt.Printf("genesis_header_hash: %s\n", loaded.Fixture.ExpectedHeaderHashHex)
	fmt.Printf("genesis_txid: %s\n", loaded.Fixture.ExpectedTxIDHex)
	fmt.Printf("genesis_authid: %s\n", loaded.Fixture.ExpectedAuthIDHex)
	fmt.Printf("post_genesis_utxo_root: %s\n", loaded.Fixture.ExpectedUTXORootAfterGenesis)
	fmt.Printf("tip_height: %d\n", summary.Height)
	fmt.Printf("utxo_count: %d\n", summary.UTXOCount)
	fmt.Printf("seeded_block_size_limit: %d\n", summary.BlockSizeLimit)
	if *db != "" {
		fmt.Printf("db: %s\n", *db)
	}
	return nil
}

func runChainValidateFixture(args []string) error {
	fs := flag.NewFlagSet("chain validate-fixture", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "fixtures/chains/regtest_bootstrap.json", "")
	db := fs.String("db", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fixture, err := loadChainFixture(*file)
	if err != nil {
		return err
	}
	profile, err := types.ParseChainProfile(fixture.Profile)
	if err != nil {
		return err
	}
	loadedGenesis, err := loadGenesisFixtureFromPath(fixture.GenesisFixture)
	if err != nil {
		return err
	}
	if loadedGenesis.Fixture.Profile != fixture.Profile {
		return fmt.Errorf("chain fixture profile mismatch: chain says %s, genesis says %s", fixture.Profile, loadedGenesis.Fixture.Profile)
	}

	blocks := make([]types.Block, 0, len(fixture.Blocks))
	for i, blockHex := range fixture.Blocks {
		block, err := consensus.DecodeBlockHex(blockHex, types.DefaultCodecLimits())
		if err != nil {
			return fmt.Errorf("failed to decode fixture block at index %d: %w", i, err)
		}
		gotHash := fmt.Sprintf("%x", consensus.HeaderHash(&block.Header))
		if i >= len(fixture.ExpectedBlockHashesHex) {
			return fmt.Errorf("missing expected block hash for index %d", i)
		}
		if gotHash != fixture.ExpectedBlockHashesHex[i] {
			return fmt.Errorf("block hash mismatch at index %d: expected %s, got %s", i, fixture.ExpectedBlockHashesHex[i], gotHash)
		}
		blocks = append(blocks, block)
	}

	var summary node.ChainReplaySummary
	if *db != "" {
		state, err := node.OpenPersistentChainState(*db, profile)
		if err != nil {
			return err
		}
		if _, err := state.InitializeFromGenesisBlock(&loadedGenesis.Block); err != nil {
			state.Close()
			return err
		}
		summary, err = state.ReplayBlocks(blocks)
		if err != nil {
			state.Close()
			return err
		}
		if err := state.Close(); err != nil {
			return err
		}

		reopened, err := node.OpenPersistentChainState(*db, profile)
		if err != nil {
			return err
		}
		defer reopened.Close()
		reopenedHeight := reopened.ChainState().TipHeight()
		if reopenedHeight == nil || *reopenedHeight != summary.TipHeight {
			return fmt.Errorf("reopened tip height mismatch: expected %d, got %v", summary.TipHeight, reopenedHeight)
		}
		expectedTip := loadedGenesis.Block.Header
		if len(blocks) != 0 {
			expectedTip = blocks[len(blocks)-1].Header
		}
		reopenedTip := reopened.ChainState().TipHeader()
		if reopenedTip == nil || *reopenedTip != expectedTip {
			return errors.New("reopened tip header mismatch")
		}
		reopenedRoot := reopened.ChainState().UTXORoot()
		if reopenedRoot != summary.UTXORoot {
			return fmt.Errorf("reopened utxo_root mismatch: expected %x, got %x", summary.UTXORoot, reopenedRoot)
		}
	} else {
		state := node.NewChainState(profile)
		if _, err := state.InitializeFromGenesisBlock(&loadedGenesis.Block); err != nil {
			return err
		}
		summary, err = state.ReplayBlocks(blocks)
		if err != nil {
			return err
		}
	}

	gotTipHash := fmt.Sprintf("%x", summary.TipHeaderHash)
	gotUTXORoot := fmt.Sprintf("%x", summary.UTXORoot)
	if summary.TipHeight != fixture.ExpectedTipHeight {
		return fmt.Errorf("tip height mismatch: expected %d, got %d", fixture.ExpectedTipHeight, summary.TipHeight)
	}
	if gotTipHash != fixture.ExpectedTipHeaderHashHex {
		return fmt.Errorf("tip hash mismatch: expected %s, got %s", fixture.ExpectedTipHeaderHashHex, gotTipHash)
	}
	if gotUTXORoot != fixture.ExpectedTipUTXORootHex {
		return fmt.Errorf("tip utxo_root mismatch: expected %s, got %s", fixture.ExpectedTipUTXORootHex, gotUTXORoot)
	}
	if summary.UTXOCount != fixture.ExpectedUTXOCount {
		return fmt.Errorf("utxo count mismatch: expected %d, got %d", fixture.ExpectedUTXOCount, summary.UTXOCount)
	}

	fmt.Printf("fixture: %s\n", *file)
	fmt.Printf("profile: %s\n", fixture.Profile)
	fmt.Printf("validated_blocks: %d\n", len(blocks))
	fmt.Printf("tip_height: %d\n", summary.TipHeight)
	fmt.Printf("tip_header_hash: %s\n", gotTipHash)
	fmt.Printf("tip_utxo_root: %s\n", gotUTXORoot)
	fmt.Printf("utxo_count: %d\n", summary.UTXOCount)
	if *db != "" {
		fmt.Printf("db: %s\n", *db)
	}
	return nil
}

func runChainValidateHeadersFixture(args []string) error {
	fs := flag.NewFlagSet("chain validate-headers-fixture", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "fixtures/chains/regtest_bootstrap.json", "")
	db := fs.String("db", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fixture, err := loadChainFixture(*file)
	if err != nil {
		return err
	}
	profile, err := types.ParseChainProfile(fixture.Profile)
	if err != nil {
		return err
	}
	loadedGenesis, err := loadGenesisFixtureFromPath(fixture.GenesisFixture)
	if err != nil {
		return err
	}
	if loadedGenesis.Fixture.Profile != fixture.Profile {
		return fmt.Errorf("chain fixture profile mismatch: chain says %s, genesis says %s", fixture.Profile, loadedGenesis.Fixture.Profile)
	}

	headers := make([]types.BlockHeader, 0, len(fixture.Blocks))
	for i, blockHex := range fixture.Blocks {
		block, err := consensus.DecodeBlockHex(blockHex, types.DefaultCodecLimits())
		if err != nil {
			return fmt.Errorf("failed to decode fixture block at index %d: %w", i, err)
		}
		gotHash := fmt.Sprintf("%x", consensus.HeaderHash(&block.Header))
		if i >= len(fixture.ExpectedBlockHashesHex) {
			return fmt.Errorf("missing expected block hash for index %d", i)
		}
		if gotHash != fixture.ExpectedBlockHashesHex[i] {
			return fmt.Errorf("block hash mismatch at index %d: expected %s, got %s", i, fixture.ExpectedBlockHashesHex[i], gotHash)
		}
		headers = append(headers, block.Header)
	}

	var summary node.HeaderReplaySummary
	if *db != "" {
		chain, err := node.OpenPersistentHeaderChain(*db, profile)
		if err != nil {
			return err
		}
		if err := chain.InitializeFromGenesisHeader(loadedGenesis.Block.Header); err != nil {
			chain.Close()
			return err
		}
		summary, err = chain.ReplayHeaders(headers)
		if err != nil {
			chain.Close()
			return err
		}
		if err := chain.Close(); err != nil {
			return err
		}

		reopened, err := node.OpenPersistentHeaderChain(*db, profile)
		if err != nil {
			return err
		}
		defer reopened.Close()
		reopenedHeight := reopened.HeaderChain().TipHeight()
		if reopenedHeight == nil || *reopenedHeight != summary.TipHeight {
			return fmt.Errorf("reopened tip height mismatch: expected %d, got %v", summary.TipHeight, reopenedHeight)
		}
		reopenedTip := reopened.HeaderChain().TipHeader()
		expectedTip := loadedGenesis.Block.Header
		if len(headers) != 0 {
			expectedTip = headers[len(headers)-1]
		}
		if reopenedTip == nil || *reopenedTip != expectedTip {
			return errors.New("reopened tip header mismatch")
		}
	} else {
		chain := node.NewHeaderChain(profile)
		if err := chain.InitializeFromGenesisHeader(loadedGenesis.Block.Header); err != nil {
			return err
		}
		summary, err = chain.ReplayHeaders(headers)
		if err != nil {
			return err
		}
	}

	gotTipHash := fmt.Sprintf("%x", summary.TipHeaderHash)
	if summary.TipHeight != fixture.ExpectedTipHeight {
		return fmt.Errorf("tip height mismatch: expected %d, got %d", fixture.ExpectedTipHeight, summary.TipHeight)
	}
	if gotTipHash != fixture.ExpectedTipHeaderHashHex {
		return fmt.Errorf("tip hash mismatch: expected %s, got %s", fixture.ExpectedTipHeaderHashHex, gotTipHash)
	}

	fmt.Printf("fixture: %s\n", *file)
	fmt.Printf("profile: %s\n", fixture.Profile)
	fmt.Printf("validated_headers: %d\n", len(headers))
	fmt.Printf("tip_height: %d\n", summary.TipHeight)
	fmt.Printf("tip_header_hash: %s\n", gotTipHash)
	if *db != "" {
		fmt.Printf("db: %s\n", *db)
	}
	return nil
}

func runChainSyncFixture(args []string) error {
	fs := flag.NewFlagSet("chain sync-fixture", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "fixtures/chains/regtest_bootstrap.json", "")
	db := fs.String("db", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fixture, err := loadChainFixture(*file)
	if err != nil {
		return err
	}
	profile, err := types.ParseChainProfile(fixture.Profile)
	if err != nil {
		return err
	}
	loadedGenesis, err := loadGenesisFixtureFromPath(fixture.GenesisFixture)
	if err != nil {
		return err
	}
	if loadedGenesis.Fixture.Profile != fixture.Profile {
		return fmt.Errorf("chain fixture profile mismatch: chain says %s, genesis says %s", fixture.Profile, loadedGenesis.Fixture.Profile)
	}

	blocks := make([]types.Block, 0, len(fixture.Blocks))
	for i, blockHex := range fixture.Blocks {
		block, err := consensus.DecodeBlockHex(blockHex, types.DefaultCodecLimits())
		if err != nil {
			return fmt.Errorf("failed to decode fixture block at index %d: %w", i, err)
		}
		gotHash := fmt.Sprintf("%x", consensus.HeaderHash(&block.Header))
		if i >= len(fixture.ExpectedBlockHashesHex) {
			return fmt.Errorf("missing expected block hash for index %d", i)
		}
		if gotHash != fixture.ExpectedBlockHashesHex[i] {
			return fmt.Errorf("block hash mismatch at index %d: expected %s, got %s", i, fixture.ExpectedBlockHashesHex[i], gotHash)
		}
		blocks = append(blocks, block)
	}

	var summary node.HeadersFirstIBDSummary
	if *db != "" {
		summary, err = node.ReplayBlocksHeadersFirstPersistent(*db, profile, &loadedGenesis.Block, blocks)
		if err != nil {
			return err
		}
	} else {
		summary, err = node.ReplayBlocksHeadersFirst(profile, &loadedGenesis.Block, blocks)
		if err != nil {
			return err
		}
	}

	gotTipHash := fmt.Sprintf("%x", summary.TipHeaderHash)
	gotUTXORoot := fmt.Sprintf("%x", summary.UTXORoot)
	if summary.HeaderTipHeight != fixture.ExpectedTipHeight {
		return fmt.Errorf("header tip height mismatch: expected %d, got %d", fixture.ExpectedTipHeight, summary.HeaderTipHeight)
	}
	if summary.BlockTipHeight != fixture.ExpectedTipHeight {
		return fmt.Errorf("block tip height mismatch: expected %d, got %d", fixture.ExpectedTipHeight, summary.BlockTipHeight)
	}
	if gotTipHash != fixture.ExpectedTipHeaderHashHex {
		return fmt.Errorf("tip hash mismatch: expected %s, got %s", fixture.ExpectedTipHeaderHashHex, gotTipHash)
	}
	if gotUTXORoot != fixture.ExpectedTipUTXORootHex {
		return fmt.Errorf("tip utxo_root mismatch: expected %s, got %s", fixture.ExpectedTipUTXORootHex, gotUTXORoot)
	}
	if summary.UTXOCount != fixture.ExpectedUTXOCount {
		return fmt.Errorf("utxo count mismatch: expected %d, got %d", fixture.ExpectedUTXOCount, summary.UTXOCount)
	}

	fmt.Printf("fixture: %s\n", *file)
	fmt.Printf("profile: %s\n", fixture.Profile)
	fmt.Printf("validated_headers: %d\n", len(blocks))
	fmt.Printf("validated_blocks: %d\n", len(blocks))
	fmt.Printf("header_tip_height: %d\n", summary.HeaderTipHeight)
	fmt.Printf("block_tip_height: %d\n", summary.BlockTipHeight)
	fmt.Printf("tip_header_hash: %s\n", gotTipHash)
	fmt.Printf("tip_utxo_root: %s\n", gotUTXORoot)
	fmt.Printf("utxo_count: %d\n", summary.UTXOCount)
	if *db != "" {
		fmt.Printf("db: %s\n", *db)
	}
	return nil
}

func loadGenesisFixture(profile types.ChainProfile) (*loadedGenesisFixture, error) {
	switch profile {
	case types.Mainnet:
		return loadGenesisFixtureFromPath("fixtures/genesis/mainnet.json")
	case types.Regtest:
		return loadGenesisFixtureFromPath("fixtures/genesis/regtest.json")
	case types.RegtestMedium:
		return loadGenesisFixtureFromPath("fixtures/genesis/regtest_medium.json")
	case types.RegtestHard:
		return loadGenesisFixtureFromPath("fixtures/genesis/regtest_hard.json")
	case types.BenchNet:
		return nil, errors.New("benchnet genesis is generated by the benchmark harness")
	default:
		return nil, fmt.Errorf("unsupported profile: %s", profile)
	}
}

func loadGenesisFixtureFromPath(path string) (*loadedGenesisFixture, error) {
	var fixture genesisFixture
	if err := readJSON(path, &fixture); err != nil {
		return nil, err
	}
	block, err := consensus.DecodeBlockHex(fixture.BlockHex, types.DefaultCodecLimits())
	if err != nil {
		return nil, err
	}
	gotHeaderHash := fmt.Sprintf("%x", consensus.HeaderHash(&block.Header))
	gotTxID := fmt.Sprintf("%x", consensus.TxID(&block.Txs[0]))
	gotAuthID := fmt.Sprintf("%x", consensus.AuthID(&block.Txs[0]))
	utxos := make(consensus.UtxoSet)
	txID := consensus.TxID(&block.Txs[0])
	for vout, output := range block.Txs[0].Base.Outputs {
		utxos[types.OutPoint{TxID: txID, Vout: uint32(vout)}] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			PubKey:     output.PubKey,
		}
	}
	gotUTXORoot := fmt.Sprintf("%x", consensus.ComputedUTXORoot(utxos))
	if gotHeaderHash != fixture.ExpectedHeaderHashHex {
		return nil, fmt.Errorf("genesis fixture header hash mismatch: expected %s, got %s", fixture.ExpectedHeaderHashHex, gotHeaderHash)
	}
	if gotTxID != fixture.ExpectedTxIDHex {
		return nil, fmt.Errorf("genesis fixture txid mismatch: expected %s, got %s", fixture.ExpectedTxIDHex, gotTxID)
	}
	if gotAuthID != fixture.ExpectedAuthIDHex {
		return nil, fmt.Errorf("genesis fixture authid mismatch: expected %s, got %s", fixture.ExpectedAuthIDHex, gotAuthID)
	}
	if gotUTXORoot != fixture.ExpectedUTXORootAfterGenesis {
		return nil, fmt.Errorf("genesis fixture utxo_root mismatch: expected %s, got %s", fixture.ExpectedUTXORootAfterGenesis, gotUTXORoot)
	}
	return &loadedGenesisFixture{Fixture: fixture, Block: block}, nil
}

func loadChainFixture(path string) (*chainFixture, error) {
	var fixture chainFixture
	if err := readJSON(path, &fixture); err != nil {
		return nil, err
	}
	return &fixture, nil
}

func readJSON(path string, out any) error {
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

func readHexInput(rawHex, file string) (string, error) {
	switch {
	case rawHex != "" && file != "":
		return "", errors.New("provide either --hex or --file, not both")
	case rawHex != "":
		if _, err := hex.DecodeString(rawHex); err != nil {
			return "", err
		}
		return rawHex, nil
	case file != "":
		buf, err := os.ReadFile(filepath.Clean(file))
		if err != nil {
			return "", err
		}
		raw := string(bytesTrimSpace(buf))
		if _, err := hex.DecodeString(raw); err != nil {
			return "", err
		}
		return raw, nil
	default:
		return "", errors.New("provide --hex or --file")
	}
}

func bytesTrimSpace(buf []byte) []byte {
	start := 0
	for start < len(buf) && (buf[start] == ' ' || buf[start] == '\n' || buf[start] == '\t' || buf[start] == '\r') {
		start++
	}
	end := len(buf)
	for end > start && (buf[end-1] == ' ' || buf[end-1] == '\n' || buf[end-1] == '\t' || buf[end-1] == '\r') {
		end--
	}
	return buf[start:end]
}

type cliRPCClient struct {
	addr  string
	token string
	http  *http.Client
}

type cliRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error"`
}

func newRPCClient(addr, token string) *cliRPCClient {
	return &cliRPCClient{
		addr:  strings.TrimSpace(addr),
		token: strings.TrimSpace(token),
		http:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *cliRPCClient) Call(method string, params any, out any) error {
	if c.addr == "" {
		return errors.New("rpc address is required")
	}
	body, err := json.Marshal(map[string]any{
		"method": method,
		"params": params,
	})
	if err != nil {
		return err
	}
	endpoint := c.addr
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rpc %s returned %s: %s", method, resp.Status, strings.TrimSpace(string(respBody)))
	}
	var rpcResp cliRPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return err
	}
	if rpcResp.Error != "" {
		return errors.New(rpcResp.Error)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(rpcResp.Result, out)
}

func resolveCLIConfig(configPath string) (config.Config, string, error) {
	cfg := config.Default()
	resolved := strings.TrimSpace(configPath)
	if resolved == "" {
		for _, candidate := range config.DefaultPathCandidates() {
			if fileExists(candidate) {
				resolved = candidate
				break
			}
		}
	}
	if resolved == "" {
		return cfg, "", nil
	}
	loaded, err := config.Load(resolved)
	if err != nil {
		return config.Config{}, "", err
	}
	return loaded, resolved, nil
}

func openWalletStore(walletDir string, cfg config.Config) (*wallet.Store, string, error) {
	dir := strings.TrimSpace(walletDir)
	if dir == "" {
		dir = filepath.Join(filepath.Dir(filepath.Clean(cfg.DBPath)), "wallets")
	}
	path := wallet.StorePath(dir)
	profile, err := types.ParseChainProfile(cfg.Profile)
	if err != nil {
		return nil, "", err
	}
	store, err := wallet.OpenWithProfile(path, profile)
	if err != nil {
		return nil, "", err
	}
	return store, path, nil
}

func ensureMiningWalletProvisioned(configPath string, cfg *config.Config) (wallet.Address, string, error) {
	if cfg == nil || !cfg.MinerEnabled || strings.TrimSpace(cfg.MinerPubKeyHex) != "" {
		return wallet.Address{}, "", nil
	}
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return wallet.Address{}, "", errors.New("mining auto-setup requires a config file path when miner_pubkey_hex is empty")
	}
	store, walletPath, err := openWalletStore("", *cfg)
	if err != nil {
		return wallet.Address{}, "", err
	}
	const minerWalletName = "miner"
	existing, err := store.Wallet(minerWalletName)
	var addr wallet.Address
	switch {
	case err == nil:
		if latest := existing.LatestReceiveAddress(); latest != nil {
			addr = *latest
		} else {
			addr, err = store.NewReceiveAddress(minerWalletName)
			if err != nil {
				return wallet.Address{}, "", err
			}
		}
	case errors.Is(err, wallet.ErrWalletNotFound):
		_, addr, err = store.CreateWallet(minerWalletName)
		if err != nil {
			return wallet.Address{}, "", err
		}
	default:
		return wallet.Address{}, "", err
	}
	cfg.MinerPubKeyHex = addr.PubKeyHex
	if err := config.Save(configPath, *cfg); err != nil {
		return wallet.Address{}, "", err
	}
	return addr, walletPath, nil
}

func resolveRPCAddr(cfg config.Config, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.TrimSpace(override)
	}
	return cfg.RPCAddr
}

func resolveRPCAuthToken(cfg config.Config, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.TrimSpace(override)
	}
	return cfg.RPCAuthToken
}

func reconcileWalletPending(store *wallet.Store, client *cliRPCClient, walletName string) error {
	var txids []string
	if err := client.Call("getmempool", map[string]any{}, &txids); err != nil {
		return err
	}
	mempool := make(map[[32]byte]struct{}, len(txids))
	for _, raw := range txids {
		txid, err := decodeHex32(raw)
		if err != nil {
			continue
		}
		mempool[txid] = struct{}{}
	}
	_, err := store.ReconcilePending(walletName, mempool)
	return err
}

func rpcUTXOsByPubKeys(client *cliRPCClient, pubKeys [][32]byte) ([]wallet.SpendableUTXO, error) {
	items := make([]wallet.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, wallet.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return rpcUTXOsByWatchItems(client, items)
}

func rpcUTXOsByWatchItems(client *cliRPCClient, items []wallet.WatchItem) ([]wallet.SpendableUTXO, error) {
	params := struct {
		WatchItems []struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		} `json:"watchitems"`
	}{WatchItems: make([]struct {
		Type      uint64 `json:"type"`
		Payload32 string `json:"payload32"`
	}, 0, len(items))}
	for _, item := range items {
		params.WatchItems = append(params.WatchItems, struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		}{
			Type:      item.Type,
			Payload32: hex.EncodeToString(item.Payload32[:]),
		})
	}
	var result struct {
		UTXOs []struct {
			TxID      string `json:"txid"`
			Vout      uint32 `json:"vout"`
			Value     uint64 `json:"value"`
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		} `json:"utxos"`
	}
	if err := client.Call("getutxosbywatchitems", params, &result); err != nil {
		return nil, err
	}
	out := make([]wallet.SpendableUTXO, 0, len(result.UTXOs))
	for _, item := range result.UTXOs {
		txid, err := decodeHex32(item.TxID)
		if err != nil {
			return nil, err
		}
		payload32, err := decodeHex32(item.Payload32)
		if err != nil {
			return nil, err
		}
		out = append(out, wallet.SpendableUTXO{
			OutPoint:  types.OutPoint{TxID: txid, Vout: item.Vout},
			Value:     item.Value,
			Type:      item.Type,
			Payload32: payload32,
			PubKey:    legacyWalletPubKey(item.Type, payload32),
		})
	}
	return out, nil
}

type walletActivityRPCItem struct {
	TxID      string `json:"txid"`
	BlockHash string `json:"block_hash"`
	Height    uint64 `json:"height"`
	Timestamp string `json:"timestamp"`
	Coinbase  bool   `json:"coinbase"`
	Received  uint64 `json:"received"`
	Sent      uint64 `json:"sent"`
	Fee       uint64 `json:"fee"`
	Net       int64  `json:"net"`
}

func rpcWalletActivityByPubKeys(client *cliRPCClient, pubKeys [][32]byte, limit int) ([]walletActivityRPCItem, error) {
	items := make([]wallet.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, wallet.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return rpcWalletActivityByWatchItems(client, items, limit)
}

func rpcWalletActivityByWatchItems(client *cliRPCClient, items []wallet.WatchItem, limit int) ([]walletActivityRPCItem, error) {
	params := struct {
		WatchItems []struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		} `json:"watchitems"`
		Limit int `json:"limit"`
	}{WatchItems: make([]struct {
		Type      uint64 `json:"type"`
		Payload32 string `json:"payload32"`
	}, 0, len(items)), Limit: limit}
	for _, item := range items {
		params.WatchItems = append(params.WatchItems, struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		}{
			Type:      item.Type,
			Payload32: hex.EncodeToString(item.Payload32[:]),
		})
	}
	var result struct {
		Activity []walletActivityRPCItem `json:"activity"`
	}
	if err := client.Call("getwalletactivitybywatchitems", params, &result); err != nil {
		return nil, err
	}
	return result.Activity, nil
}

func legacyWalletPubKey(outputType uint64, payload32 [32]byte) [32]byte {
	if outputType == types.OutputXOnlyP2PK {
		return payload32
	}
	return [32]byte{}
}

func rpcEstimateFee(client *cliRPCClient, targetBlocks int) (uint64, error) {
	var result struct {
		FeePerByte uint64 `json:"fee_per_byte"`
	}
	if err := client.Call("estimatefee", map[string]int{"target_blocks": targetBlocks}, &result); err != nil {
		return 0, err
	}
	return result.FeePerByte, nil
}

func rpcMempoolInfo(client *cliRPCClient) (*node.MempoolInfo, error) {
	var result node.MempoolInfo
	if err := client.Call("getmempoolinfo", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func printWalletAddressDetails(addr wallet.Address) {
	switch addr.OutputType() {
	case types.OutputPQLock32:
		fmt.Printf("pq_lock: %s\n", addr.PayloadHex)
		fmt.Printf("alg: ml-dsa-65\n")
	default:
		fmt.Printf("pubkey: %s\n", addr.PubKeyHex)
	}
}

type walletActionRow struct {
	label string
	value string
}

type walletActionView struct {
	title string
	rows  []walletActionRow
}

type walletFeeRequest struct {
	TargetBlocks          int
	TargetBlocksExplicit  bool
	TargetMinutes         int
	TargetMinutesExplicit bool
	Priority              string
	PriorityExplicit      bool
	AllowInteractive      bool
}

type walletFeeQuote struct {
	Label         string
	TargetBlocks  int
	TargetMinutes int
	FeeRate       uint64
	Mempool       *node.MempoolInfo
}

func renderSendPreview(plan wallet.SendPlan, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "to", value: plan.ToAddress},
		{label: "amount", value: fmt.Sprintf("%d atoms", plan.Amount)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
		walletActionRow{label: "inputs", value: fmt.Sprintf("%d (%d atoms)", len(plan.Inputs), plan.InputTotal)},
		walletActionRow{label: "txid", value: fmt.Sprintf("%x", plan.TransactionID)},
	)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		rows = append(rows, walletActionRow{label: "change", value: fmt.Sprintf("%d atoms -> %s", plan.Change, plan.ChangeAddress.Address)})
	}
	return walletActionView{title: "send", rows: rows}
}

func renderSendResult(plan wallet.SendPlan, txid string, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "txid", value: txid},
		{label: "amount", value: fmt.Sprintf("%d atoms", plan.Amount)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
	)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		rows = append(rows, walletActionRow{label: "change", value: fmt.Sprintf("%d atoms -> %s", plan.Change, plan.ChangeAddress.Address)})
	}
	return walletActionView{title: "submitted", rows: rows}
}

func renderCPFPPreview(plan wallet.CPFPPlan, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "parent", value: hex.EncodeToString(plan.ParentTxID[:])},
		{label: "source", value: fmt.Sprintf("%x:%d (%d atoms)", plan.Input.OutPoint.TxID, plan.Input.OutPoint.Vout, plan.Input.Value)},
		{label: "child", value: fmt.Sprintf("%d atoms -> %s", plan.Amount, plan.SweepAddress.Address)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
		walletActionRow{label: "txid", value: fmt.Sprintf("%x", plan.TransactionID)},
	)
	return walletActionView{title: "cpfp", rows: rows}
}

func renderCPFPResult(plan wallet.CPFPPlan, txid string, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "parent", value: hex.EncodeToString(plan.ParentTxID[:])},
		{label: "txid", value: txid},
		{label: "child", value: fmt.Sprintf("%d atoms -> %s", plan.Amount, plan.SweepAddress.Address)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows, walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)})
	return walletActionView{title: "submitted", rows: rows}
}

func formatWalletFee(fee uint64, feeRate uint64, estimatedBytes int) string {
	if feeRate == 0 || estimatedBytes == 0 {
		return fmt.Sprintf("%d atoms", fee)
	}
	return fmt.Sprintf("%d atoms (%d atoms/B, %d B)", fee, feeRate, estimatedBytes)
}

func resolveWalletFeeQuote(client *cliRPCClient, req walletFeeRequest) (walletFeeQuote, error) {
	selectors := 0
	if req.TargetBlocksExplicit {
		selectors++
	}
	if req.TargetMinutesExplicit {
		selectors++
	}
	if req.PriorityExplicit && strings.TrimSpace(req.Priority) != "" {
		selectors++
	}
	if selectors > 1 {
		return walletFeeQuote{}, errors.New("choose only one of --priority, --target-blocks, or --target-minutes")
	}
	info, err := rpcMempoolInfo(client)
	if err != nil {
		info = nil
	}
	estimate := func(targetBlocks int) (uint64, error) {
		return rpcEstimateFee(client, targetBlocks)
	}
	switch {
	case req.PriorityExplicit && strings.TrimSpace(req.Priority) != "":
		label, blocks, err := parseWalletFeePriority(req.Priority)
		if err != nil {
			return walletFeeQuote{}, err
		}
		return buildWalletFeeQuote(label, blocks, blocks*10, info, estimate)
	case req.TargetMinutesExplicit:
		if req.TargetMinutes <= 0 {
			return walletFeeQuote{}, errors.New("--target-minutes must be positive")
		}
		return buildWalletFeeQuote("custom", minutesToTargetBlocks(req.TargetMinutes), req.TargetMinutes, info, estimate)
	case req.TargetBlocksExplicit:
		if req.TargetBlocks <= 0 {
			return walletFeeQuote{}, errors.New("--target-blocks must be positive")
		}
		return buildWalletFeeQuote("custom", req.TargetBlocks, req.TargetBlocks*10, info, estimate)
	case req.AllowInteractive:
		return promptWalletFeeQuoteInteractive(os.Stdin, os.Stdout, info, estimate)
	default:
		return buildWalletFeeQuote("now", 1, 10, info, estimate)
	}
}

func buildWalletFeeQuote(label string, targetBlocks int, targetMinutes int, info *node.MempoolInfo, estimate func(int) (uint64, error)) (walletFeeQuote, error) {
	if targetBlocks <= 0 {
		return walletFeeQuote{}, errors.New("target blocks must be positive")
	}
	feeRate, err := estimate(targetBlocks)
	if err != nil {
		return walletFeeQuote{}, err
	}
	if targetMinutes <= 0 {
		targetMinutes = targetBlocks * 10
	}
	return walletFeeQuote{
		Label:         label,
		TargetBlocks:  targetBlocks,
		TargetMinutes: targetMinutes,
		FeeRate:       feeRate,
		Mempool:       info,
	}, nil
}

func promptWalletFeeQuoteInteractive(in io.Reader, out io.Writer, info *node.MempoolInfo, estimate func(int) (uint64, error)) (walletFeeQuote, error) {
	presets := []struct {
		label  string
		blocks int
	}{
		{label: "now", blocks: 1},
		{label: "soon", blocks: 2},
		{label: "relaxed", blocks: 3},
		{label: "cheap", blocks: 6},
	}
	quotes := make([]walletFeeQuote, 0, len(presets))
	for _, preset := range presets {
		quote, err := buildWalletFeeQuote(preset.label, preset.blocks, preset.blocks*10, info, estimate)
		if err != nil {
			return walletFeeQuote{}, err
		}
		quotes = append(quotes, quote)
	}
	defaultIndex := 1
	recommended := recommendedWalletFeeLabel(info)
	for i := range quotes {
		if quotes[i].Label == recommended {
			defaultIndex = i
			break
		}
	}
	reader := bufio.NewReader(in)
	fmt.Fprintln(out, "fee target")
	if info != nil {
		fmt.Fprintf(out, "  mempool  %s\n", formatWalletMempoolSummary(*info))
	}
	for i, quote := range quotes {
		suffix := ""
		if i == defaultIndex {
			suffix = " (recommended)"
		}
		fmt.Fprintf(out, "  %d) %-7s %s  %d atoms/B%s\n", i+1, quote.Label, formatWalletTargetSummary(&quote), quote.FeeRate, suffix)
	}
	fmt.Fprintln(out, "  5) blocks   custom block target")
	fmt.Fprintln(out, "  6) minutes  custom minute target")
	for {
		fmt.Fprintf(out, "choose fee target [%d]: ", defaultIndex+1)
		raw, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return walletFeeQuote{}, err
		}
		choice := strings.TrimSpace(raw)
		if choice == "" {
			return quotes[defaultIndex], nil
		}
		switch choice {
		case "1", "2", "3", "4":
			return quotes[int(choice[0]-'1')], nil
		case "5":
			blocks, err := promptPositiveInt(reader, out, "confirm in how many blocks? ")
			if err != nil {
				return walletFeeQuote{}, err
			}
			return buildWalletFeeQuote("custom", blocks, blocks*10, info, estimate)
		case "6":
			minutes, err := promptPositiveInt(reader, out, "confirm in roughly how many minutes? ")
			if err != nil {
				return walletFeeQuote{}, err
			}
			return buildWalletFeeQuote("custom", minutesToTargetBlocks(minutes), minutes, info, estimate)
		default:
			fmt.Fprintln(out, "choose 1-6, or press enter for the recommended target")
			if errors.Is(err, io.EOF) {
				return walletFeeQuote{}, errors.New("fee target selection cancelled")
			}
		}
	}
}

func promptPositiveInt(reader *bufio.Reader, out io.Writer, prompt string) (int, error) {
	for {
		fmt.Fprint(out, prompt)
		raw, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		value, convErr := strconv.Atoi(strings.TrimSpace(raw))
		if convErr == nil && value > 0 {
			return value, nil
		}
		fmt.Fprintln(out, "enter a positive integer")
		if errors.Is(err, io.EOF) {
			return 0, errors.New("fee target selection cancelled")
		}
	}
}

func parseWalletFeePriority(raw string) (string, int, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "now", "fast", "asap":
		return "now", 1, nil
	case "soon", "normal":
		return "soon", 2, nil
	case "relaxed", "standard":
		return "relaxed", 3, nil
	case "cheap", "slow":
		return "cheap", 6, nil
	default:
		return "", 0, errors.New("unknown --priority value (expected: now, soon, relaxed, cheap)")
	}
}

func minutesToTargetBlocks(minutes int) int {
	if minutes <= 0 {
		return 1
	}
	return (minutes + 9) / 10
}

func recommendedWalletFeeLabel(info *node.MempoolInfo) string {
	if info == nil {
		return "soon"
	}
	switch classifyWalletMempoolPressure(*info) {
	case "high":
		return "now"
	case "active":
		return "soon"
	case "idle":
		return "cheap"
	default:
		return "relaxed"
	}
}

func classifyWalletMempoolPressure(info node.MempoolInfo) string {
	maxBytes := info.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	switch {
	case info.Count == 0 && info.Orphans == 0 && info.Bytes == 0:
		return "idle"
	case info.Bytes >= (maxBytes*8)/10 || info.Count >= 10_000 || info.Orphans >= walletMaxInt(32, 128/2):
		return "high"
	case info.Bytes >= walletMaxInt(16<<20, maxBytes/4) || info.Count >= 1_000 || info.Orphans > 0:
		return "active"
	default:
		return "normal"
	}
}

func walletMaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func formatWalletTargetSummary(quote *walletFeeQuote) string {
	if quote == nil {
		return ""
	}
	blockWord := "blocks"
	if quote.TargetBlocks == 1 {
		blockWord = "block"
	}
	return fmt.Sprintf("~%d min / %d %s", quote.TargetMinutes, quote.TargetBlocks, blockWord)
}

func formatWalletMempoolSummary(info node.MempoolInfo) string {
	pressure := classifyWalletMempoolPressure(info)
	if info.Count == 0 && info.Orphans == 0 && info.Bytes == 0 {
		return fmt.Sprintf("%s, empty, min relay %d atoms/B", pressure, info.MinRelayFeePerByte)
	}
	return fmt.Sprintf("%s, %d tx, median %d atoms/B, range %d-%d", pressure, info.Count, info.MedianFee, info.LowFee, info.HighFee)
}

func walletFeeQuoteRows(quote *walletFeeQuote) []walletActionRow {
	if quote == nil {
		return nil
	}
	rows := []walletActionRow{
		{label: "target", value: fmt.Sprintf("%s (%s)", quote.Label, formatWalletTargetSummary(quote))},
	}
	if quote.Mempool != nil {
		rows = append(rows, walletActionRow{label: "market", value: formatWalletMempoolSummary(*quote.Mempool)})
	}
	return rows
}

func printWalletAction(view walletActionView) {
	fmt.Println(view.title)
	for _, row := range view.rows {
		fmt.Printf("  %-8s %s\n", row.label, row.value)
	}
}

func maybeConfirmWalletAction(view walletActionView, yes bool) error {
	if yes {
		return nil
	}
	if !stdinLooksInteractive() {
		return errors.New("wallet action requires --yes when stdin is not interactive")
	}
	printWalletAction(view)
	fmt.Print("broadcast transaction? [y/N]: ")
	var response string
	if _, err := fmt.Fscanln(os.Stdin, &response); err != nil {
		return errors.New("transaction cancelled")
	}
	switch strings.ToLower(strings.TrimSpace(response)) {
	case "y", "yes":
		return nil
	default:
		return errors.New("transaction cancelled")
	}
}

func stdinLooksInteractive() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func flagWasPassed(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func decodeHex32(raw string) ([32]byte, error) {
	var out [32]byte
	buf, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil || len(buf) != 32 {
		return out, fmt.Errorf("invalid 32-byte hex: %q", raw)
	}
	copy(out[:], buf)
	return out, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func usageError() error {
	return errors.New("usage: bpu-cli <serve|bench|wallet|validate-tx|validate-block|chain|snapshot|config>")
}

func defaultGenesisFixture(profile types.ChainProfile) string {
	switch profile {
	case types.Mainnet:
		return "fixtures/genesis/mainnet.json"
	case types.Regtest:
		return "fixtures/genesis/regtest.json"
	case types.RegtestMedium:
		return "fixtures/genesis/regtest_medium.json"
	case types.RegtestHard:
		return "fixtures/genesis/regtest_hard.json"
	case types.BenchNet:
		return ""
	default:
		return ""
	}
}

func deriveLogPath(dbPath string) string {
	dir := filepath.Dir(filepath.Clean(dbPath))
	if dir == "." || dir == "" {
		return "node.log"
	}
	return filepath.Join(dir, "node.log")
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
