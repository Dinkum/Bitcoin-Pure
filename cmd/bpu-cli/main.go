package main

import (
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
	default:
		return usageError()
	}
}

func runBench(args []string) error {
	if len(args) == 0 {
		return errors.New("missing bench subcommand")
	}
	switch args[0] {
	case "list":
		for _, scenario := range benchmarks.AvailableScenarios() {
			fmt.Println(string(scenario))
		}
		return nil
	case "run":
		return runBenchRun(args[1:])
	case "suite":
		return runBenchSuite(args[1:])
	case "sim":
		return runBenchSimulation(args[1:])
	default:
		return errors.New("unknown bench subcommand")
	}
}

func runBenchRun(args []string) error {
	fs := flag.NewFlagSet("bench run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	scenarioRaw := fs.String("scenario", string(benchmarks.DefaultRunOptions().Scenario), "")
	profileRaw := fs.String("profile", string(benchmarks.DefaultRunOptions().Profile), "")
	nodes := fs.Int("nodes", benchmarks.DefaultRunOptions().NodeCount, "")
	topologyRaw := fs.String("topology", string(benchmarks.DefaultRunOptions().Topology), "")
	txs := fs.Int("txs", benchmarks.DefaultRunOptions().TxCount, "")
	batchSize := fs.Int("batch-size", benchmarks.DefaultRunOptions().BatchSize, "")
	timeout := fs.Duration("timeout", benchmarks.DefaultRunOptions().Timeout, "")
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
		Scenario:     benchmarks.Scenario(*scenarioRaw),
		Profile:      profile,
		NodeCount:    *nodes,
		Topology:     benchmarks.Topology(*topologyRaw),
		TxCount:      *txs,
		BatchSize:    *batchSize,
		Timeout:      *timeout,
		DBRoot:       *dbRoot,
		ProfileDir:   *profileDir,
		SuppressLogs: *suppressLogs,
	}

	report, err := benchmarks.Run(context.Background(), opts)
	if err != nil {
		return err
	}
	if *reportPath == "" || *markdownPath == "" {
		base := filepath.Join("benchmarks", "reports", time.Now().UTC().Format("20060102-150405")+"-"+string(opts.Scenario))
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

	fmt.Printf("scenario: %s\n", report.Scenario)
	fmt.Printf("profile: %s\n", report.Profile)
	fmt.Printf("nodes: %d\n", report.NodeCount)
	if report.Topology != "" {
		fmt.Printf("topology: %s\n", report.Topology)
	}
	fmt.Printf("tx_count: %d\n", report.TxCount)
	if report.BatchSize > 0 {
		fmt.Printf("batch_size: %d\n", report.BatchSize)
	}
	fmt.Printf("submit_tps: %.2f\n", report.SubmitTPS)
	fmt.Printf("end_to_end_tps: %.2f\n", report.EndToEndTPS)
	fmt.Printf("submission_duration_ms: %.2f\n", report.SubmissionDurationMS)
	fmt.Printf("convergence_duration_ms: %.2f\n", report.ConvergenceDurationMS)
	if len(report.Profiling.Artifacts) != 0 {
		fmt.Printf("profile_dir: %s\n", filepath.Dir(report.Profiling.Artifacts[0].Path))
	}
	fmt.Printf("report_json: %s\n", *reportPath)
	fmt.Printf("report_markdown: %s\n", *markdownPath)
	fmt.Println()
	fmt.Println(benchmarks.RenderASCIISummary(report))
	return nil
}

func runBenchSuite(args []string) error {
	fs := flag.NewFlagSet("bench suite", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profileRaw := fs.String("profile", string(benchmarks.DefaultSuiteOptions().Profile), "")
	nodes := fs.Int("nodes", benchmarks.DefaultSuiteOptions().NodeCount, "")
	txs := fs.Int("txs", benchmarks.DefaultSuiteOptions().TxCount, "")
	batchSize := fs.Int("batch-size", benchmarks.DefaultSuiteOptions().BatchSize, "")
	timeout := fs.Duration("timeout", benchmarks.DefaultSuiteOptions().Timeout, "")
	reportDir := fs.String("out", "", "")
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
	opts := benchmarks.SuiteOptions{
		Profile:      profile,
		NodeCount:    *nodes,
		TxCount:      *txs,
		BatchSize:    *batchSize,
		Timeout:      *timeout,
		DBRoot:       *dbRoot,
		ProfileDir:   *profileDir,
		SuppressLogs: *suppressLogs,
	}
	report, err := benchmarks.RunSuite(context.Background(), opts)
	if err != nil {
		return err
	}
	if *reportDir == "" {
		*reportDir = filepath.Join("benchmarks", "reports", time.Now().UTC().Format("20060102-150405")+"-suite")
	}
	if err := benchmarks.WriteSuiteReportFiles(report, *reportDir); err != nil {
		return err
	}

	fmt.Printf("profile: %s\n", report.Profile)
	fmt.Printf("cases: %d\n", len(report.Cases))
	if *profileDir != "" {
		fmt.Printf("profile_dir: %s\n", *profileDir)
	}
	fmt.Printf("report_dir: %s\n", *reportDir)
	for _, suiteCase := range report.Cases {
		fmt.Printf("%s_submit_tps: %.2f\n", strings.ReplaceAll(suiteCase.Name, "-", "_"), suiteCase.Report.SubmitTPS)
		fmt.Printf("%s_end_to_end_tps: %.2f\n", strings.ReplaceAll(suiteCase.Name, "-", "_"), suiteCase.Report.EndToEndTPS)
	}
	fmt.Println()
	fmt.Println(benchmarks.RenderSuiteASCIISummary(report))
	return nil
}

func runBenchSimulation(args []string) error {
	fs := flag.NewFlagSet("bench sim", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	topologyRaw := fs.String("topology", string(benchmarks.DefaultSimulationOptions().Topology), "")
	nodes := fs.Int("nodes", benchmarks.DefaultSimulationOptions().NodeCount, "")
	seed := fs.Int64("seed", benchmarks.DefaultSimulationOptions().Seed, "")
	txs := fs.Int("txs", benchmarks.DefaultSimulationOptions().TxCount, "")
	blocks := fs.Int("blocks", benchmarks.DefaultSimulationOptions().BlockCount, "")
	baseLatency := fs.Duration("base-latency", benchmarks.DefaultSimulationOptions().BaseLatency, "")
	latencyJitter := fs.Duration("latency-jitter", benchmarks.DefaultSimulationOptions().LatencyJitter, "")
	txProcessing := fs.Duration("tx-processing", benchmarks.DefaultSimulationOptions().TxProcessingDelay, "")
	blockProcessing := fs.Duration("block-processing", benchmarks.DefaultSimulationOptions().BlockProcessingDelay, "")
	txSpacing := fs.Duration("tx-spacing", benchmarks.DefaultSimulationOptions().TxSpacing, "")
	blockSpacing := fs.Duration("block-spacing", benchmarks.DefaultSimulationOptions().BlockSpacing, "")
	churnEvents := fs.Int("churn-events", benchmarks.DefaultSimulationOptions().ChurnEvents, "")
	churnDuration := fs.Duration("churn-duration", benchmarks.DefaultSimulationOptions().ChurnDuration, "")
	smallWorldDegree := fs.Int("small-world-degree", benchmarks.DefaultSimulationOptions().SmallWorldDegree, "")
	reportDir := fs.String("out", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	report, err := benchmarks.RunSimulation(context.Background(), benchmarks.SimulationOptions{
		NodeCount:            *nodes,
		Topology:             benchmarks.SimulationTopology(*topologyRaw),
		Seed:                 *seed,
		TxCount:              *txs,
		BlockCount:           *blocks,
		BaseLatency:          *baseLatency,
		LatencyJitter:        *latencyJitter,
		TxProcessingDelay:    *txProcessing,
		BlockProcessingDelay: *blockProcessing,
		TxSpacing:            *txSpacing,
		BlockSpacing:         *blockSpacing,
		ChurnEvents:          *churnEvents,
		ChurnDuration:        *churnDuration,
		SmallWorldDegree:     *smallWorldDegree,
	})
	if err != nil {
		return err
	}
	if *reportDir == "" {
		*reportDir = filepath.Join("benchmarks", "reports", time.Now().UTC().Format("20060102-150405")+"-simulation")
	}
	if err := benchmarks.WriteSimulationReportDir(report, *reportDir); err != nil {
		return err
	}

	fmt.Printf("topology: %s\n", report.Config.Topology)
	fmt.Printf("nodes: %d\n", report.Config.NodeCount)
	fmt.Printf("seed: %d\n", report.Config.Seed)
	fmt.Printf("tx_items: %d\n", report.Config.TxCount)
	fmt.Printf("block_items: %d\n", report.Config.BlockCount)
	fmt.Printf("tx_p95_to_90pct_ms: %.2f\n", report.Summary.Tx.P95TargetMS)
	fmt.Printf("block_p95_to_90pct_ms: %.2f\n", report.Summary.Block.P95TargetMS)
	fmt.Printf("message_drops: %d\n", report.Summary.Messages.Dropped)
	fmt.Printf("report_dir: %s\n", *reportDir)
	fmt.Println()
	fmt.Println(benchmarks.RenderSimulationASCIISummary(report))
	return nil
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
	miningMode := fs.String("mining", "", "")
	minerWorkers := fs.Int("miner-workers", 0, "")
	minerKeyHashHex := fs.String("miner-keyhash", "", "")
	genesisFixture := fs.String("genesis", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Default()
	resolvedConfigPath := strings.TrimSpace(*configPath)
	if resolvedConfigPath == "" && fileExists("/etc/bitcoin-pure/config.json") {
		resolvedConfigPath = "/etc/bitcoin-pure/config.json"
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
	if *minerKeyHashHex != "" {
		cfg.MinerKeyHashHex = *minerKeyHashHex
	}
	if *genesisFixture != "" {
		cfg.GenesisFixture = *genesisFixture
	}

	profile, err := types.ParseChainProfile(cfg.Profile)
	if err != nil {
		return err
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
	} else if addr.KeyHashHex != "" {
		logger.Info("provisioned mining wallet",
			slog.String("wallet", "miner"),
			slog.String("wallet_path", walletPath),
			slog.String("receive_address", addr.Address),
			slog.String("keyhash", addr.KeyHashHex),
		)
	}
	keyHash, err := node.ParseMinerKeyHash(cfg.MinerKeyHashHex)
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
		MaxAncestors:              cfg.MaxAncestors,
		MaxDescendants:            cfg.MaxDescendants,
		MaxOrphans:                cfg.MaxOrphans,
		MinerEnabled:              cfg.MinerEnabled,
		MinerWorkers:              cfg.MinerWorkers,
		MinerKeyHash:              keyHash,
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
		slog.Int("max_orphans", cfg.MaxOrphans),
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
	default:
		return errors.New("unknown wallet subcommand")
	}
}

func runWalletCreate(args []string) error {
	fs := flag.NewFlagSet("wallet create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	walletDir := fs.String("wallet-dir", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet create [--config PATH] [--wallet-dir DIR] <name>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	entry, addr, err := store.CreateWallet(fs.Arg(0))
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", entry.Name)
	fmt.Printf("created_at: %s\n", entry.CreatedAt.Format(time.RFC3339))
	fmt.Printf("receive_address: %s\n", addr.Address)
	fmt.Printf("keyhash: %s\n", addr.KeyHashHex)
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
	keyHashes, err := store.SpendableKeyHashes(fs.Arg(0))
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByKeyHashes(client, keyHashes)
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
	keyHashes, err := store.SpendableKeyHashes(fs.Arg(0))
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken))
	activity, err := rpcWalletActivityByKeyHashes(client, keyHashes, *limit)
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet receive [--config PATH] [--wallet-dir DIR] <wallet>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	addr, err := store.NewReceiveAddress(fs.Arg(0))
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", fs.Arg(0))
	fmt.Printf("receive_address: %s\n", addr.Address)
	fmt.Printf("keyhash: %s\n", addr.KeyHashHex)
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
	fee := fs.Uint64("fee", 1, "")
	yes := fs.Bool("yes", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *from == "" || *to == "" || *amount == 0 {
		return errors.New("usage: bpu-cli wallet send --from NAME --to ADDRESS --amount ATOMS [--fee ATOMS] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
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
	keyHashes, err := store.SpendableKeyHashes(*from)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByKeyHashes(client, keyHashes)
	if err != nil {
		return err
	}
	plan, err := store.BuildSend(*from, *to, *amount, *fee, utxos)
	if err != nil {
		return err
	}
	if err := maybeConfirmSend(plan, *yes); err != nil {
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
	spent := make([]types.OutPoint, 0, len(plan.Inputs))
	for _, input := range plan.Inputs {
		spent = append(spent, input.OutPoint)
	}
	if err := store.MarkSubmitted(*from, reportedTxID, spent); err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", *from)
	fmt.Printf("txid: %s\n", result.TxID)
	fmt.Printf("amount: %d\n", plan.Amount)
	fmt.Printf("fee: %d\n", plan.Fee)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		fmt.Printf("change: %d -> %s\n", plan.Change, plan.ChangeAddress.Address)
	}
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
			KeyHash:    output.KeyHash,
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
	if resolved == "" && fileExists("/etc/bitcoin-pure/config.json") {
		resolved = "/etc/bitcoin-pure/config.json"
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
	store, err := wallet.Open(path)
	if err != nil {
		return nil, "", err
	}
	return store, path, nil
}

func ensureMiningWalletProvisioned(configPath string, cfg *config.Config) (wallet.Address, string, error) {
	if cfg == nil || !cfg.MinerEnabled || strings.TrimSpace(cfg.MinerKeyHashHex) != "" {
		return wallet.Address{}, "", nil
	}
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return wallet.Address{}, "", errors.New("mining auto-setup requires a config file path when miner_keyhash_hex is empty")
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
	cfg.MinerKeyHashHex = addr.KeyHashHex
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

func rpcUTXOsByKeyHashes(client *cliRPCClient, keyHashes [][32]byte) ([]wallet.SpendableUTXO, error) {
	params := struct {
		KeyHashes []string `json:"keyhashes"`
	}{KeyHashes: make([]string, 0, len(keyHashes))}
	for _, keyHash := range keyHashes {
		params.KeyHashes = append(params.KeyHashes, hex.EncodeToString(keyHash[:]))
	}
	var result struct {
		UTXOs []struct {
			TxID    string `json:"txid"`
			Vout    uint32 `json:"vout"`
			Value   uint64 `json:"value"`
			KeyHash string `json:"keyhash"`
		} `json:"utxos"`
	}
	if err := client.Call("getutxosbykeyhashes", params, &result); err != nil {
		return nil, err
	}
	out := make([]wallet.SpendableUTXO, 0, len(result.UTXOs))
	for _, item := range result.UTXOs {
		txid, err := decodeHex32(item.TxID)
		if err != nil {
			return nil, err
		}
		keyHash, err := decodeHex32(item.KeyHash)
		if err != nil {
			return nil, err
		}
		out = append(out, wallet.SpendableUTXO{
			OutPoint: types.OutPoint{TxID: txid, Vout: item.Vout},
			Value:    item.Value,
			KeyHash:  keyHash,
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

func rpcWalletActivityByKeyHashes(client *cliRPCClient, keyHashes [][32]byte, limit int) ([]walletActivityRPCItem, error) {
	params := struct {
		KeyHashes []string `json:"keyhashes"`
		Limit     int      `json:"limit"`
	}{KeyHashes: make([]string, 0, len(keyHashes)), Limit: limit}
	for _, keyHash := range keyHashes {
		params.KeyHashes = append(params.KeyHashes, hex.EncodeToString(keyHash[:]))
	}
	var result struct {
		Activity []walletActivityRPCItem `json:"activity"`
	}
	if err := client.Call("getwalletactivitybykeyhashes", params, &result); err != nil {
		return nil, err
	}
	return result.Activity, nil
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

func maybeConfirmSend(plan wallet.SendPlan, yes bool) error {
	if yes {
		return nil
	}
	if !stdinLooksInteractive() {
		return errors.New("wallet send requires --yes when stdin is not interactive")
	}
	fmt.Printf("from: %s\n", plan.WalletName)
	fmt.Printf("to: %s\n", plan.ToAddress)
	fmt.Printf("txid: %x\n", plan.TransactionID)
	fmt.Printf("inputs: %d\n", len(plan.Inputs))
	fmt.Printf("input_total: %d\n", plan.InputTotal)
	fmt.Printf("amount: %d\n", plan.Amount)
	fmt.Printf("fee: %d\n", plan.Fee)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		fmt.Printf("change: %d -> %s\n", plan.Change, plan.ChangeAddress.Address)
	}
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
	return errors.New("usage: bpu-cli <serve|bench|wallet|validate-tx|validate-block|chain>")
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
