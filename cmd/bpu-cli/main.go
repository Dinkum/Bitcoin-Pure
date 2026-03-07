package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"bitcoin-pure/benchmarks"
	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
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
	fmt.Printf("report_dir: %s\n", *reportDir)
	for _, suiteCase := range report.Cases {
		fmt.Printf("%s_submit_tps: %.2f\n", strings.ReplaceAll(suiteCase.Name, "-", "_"), suiteCase.Report.SubmitTPS)
		fmt.Printf("%s_end_to_end_tps: %.2f\n", strings.ReplaceAll(suiteCase.Name, "-", "_"), suiteCase.Report.EndToEndTPS)
	}
	fmt.Println()
	fmt.Println(benchmarks.RenderSuiteASCIISummary(report))
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
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	rpcReadTimeout := fs.Duration("rpc-read-timeout", 0, "")
	rpcWriteTimeout := fs.Duration("rpc-write-timeout", 0, "")
	rpcHeaderTimeout := fs.Duration("rpc-header-timeout", 0, "")
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
	if *configPath != "" {
		loaded, err := config.Load(*configPath)
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
		MaxSizeBytes: logging.DefaultMaxSizeBytes,
	})
	if err != nil {
		return err
	}
	defer logCloser.Close()

	loadedGenesis, err := loadGenesisFixtureFromPath(cfg.GenesisFixture)
	if err != nil {
		return err
	}
	keyHash, err := node.ParseMinerKeyHash(cfg.MinerKeyHashHex)
	if err != nil {
		return err
	}
	svc, err := node.OpenService(node.ServiceConfig{
		Profile:            profile,
		DBPath:             cfg.DBPath,
		RPCAddr:            cfg.RPCAddr,
		RPCAuthToken:       cfg.RPCAuthToken,
		RPCReadTimeout:     time.Duration(cfg.RPCReadTimeoutMS) * time.Millisecond,
		RPCWriteTimeout:    time.Duration(cfg.RPCWriteTimeoutMS) * time.Millisecond,
		RPCHeaderTimeout:   time.Duration(cfg.RPCHeaderTimeoutMS) * time.Millisecond,
		RPCMaxBodyBytes:    cfg.RPCMaxBodyBytes,
		P2PAddr:            cfg.P2PAddr,
		Peers:              cfg.Peers,
		MaxInboundPeers:    cfg.MaxInboundPeers,
		MaxOutboundPeers:   cfg.MaxOutboundPeers,
		HandshakeTimeout:   time.Duration(cfg.HandshakeTimeoutMS) * time.Millisecond,
		StallTimeout:       time.Duration(cfg.StallTimeoutMS) * time.Millisecond,
		MaxMessageBytes:    cfg.MaxMessageBytes,
		MinRelayFeePerByte: cfg.MinRelayFeePerByte,
		MinerEnabled:       cfg.MinerEnabled,
		MinerWorkers:       cfg.MinerWorkers,
		MinerKeyHash:       keyHash,
		GenesisFixture:     cfg.GenesisFixture,
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
	fmt.Printf("log: %s\n", cfg.LogPath)
	if cfg.MinerEnabled {
		if cfg.MinerWorkers > 0 {
			fmt.Printf("miner_workers: %d\n", cfg.MinerWorkers)
		} else {
			fmt.Println("miner_workers: auto")
		}
	}
	return svc.Start(ctx)
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
		reopenedRoot := consensus.ComputedUTXORoot(reopened.ChainState().UTXOs())
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

func usageError() error {
	return errors.New("usage: bpu-cli <serve|bench|validate-tx|validate-block|chain>")
}

func defaultGenesisFixture(profile types.ChainProfile) string {
	switch profile {
	case types.Mainnet:
		return "fixtures/genesis/mainnet.json"
	case types.Regtest:
		return "fixtures/genesis/regtest.json"
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
