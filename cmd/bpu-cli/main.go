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

const walletActivityRPCLimitMax = 10_000

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
	case "logs":
		return runLogs(args[1:])
	case "status":
		return runStatus(args[1:])
	default:
		return usageError()
	}
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli status [--config PATH] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	status, err := fetchNodeStatus(client)
	if err != nil {
		return err
	}
	fmt.Print(renderNodeStatus(status, cfg))
	return nil
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
	if err := config.Validate(cfg); err != nil {
		return err
	}

	profile, err := types.ParseChainProfile(cfg.Profile)
	if err != nil {
		return err
	}
	if profile == types.BenchNet {
		return errors.New("benchnet is not an operator profile")
	}
	if cfg.GenesisFixture == "" {
		cfg.GenesisFixture = defaultGenesisFixture(profile)
	}
	if err := rejectInstalledMiningAutoProvision(resolvedConfigPath, cfg); err != nil {
		return err
	}
	cfg.LogPath = resolveLogPath(cfg)
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
	if pprofServer != nil {
		fmt.Printf("pprof: %s\n", pprofServer.addr)
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
	server                   *http.Server
	ln                       net.Listener
	addr                     string
	previousMutexProfileRate int
}

func maybeStartPprofServer(addr string, logger *slog.Logger) (*pprofServer, error) {
	addr, err := normalizePprofListenAddr(addr)
	if err != nil {
		return nil, err
	}
	if addr == "" {
		return nil, nil
	}
	if err := validateLoopbackListenAddr(addr); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", netpprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "pprof cmdline disabled", http.StatusNotFound)
	})
	mux.HandleFunc("/debug/pprof/profile", netpprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", netpprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", netpprof.Trace)
	mux.Handle("/debug/pprof/allocs", netpprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", netpprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", netpprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", netpprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", netpprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", netpprof.Handler("threadcreate"))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	previousMutexProfileRate := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
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
	return &pprofServer{server: srv, ln: ln, addr: ln.Addr().String(), previousMutexProfileRate: previousMutexProfileRate}, nil
}

func (s *pprofServer) Close() error {
	if s == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	runtime.SetMutexProfileFraction(s.previousMutexProfileRate)
	runtime.SetBlockProfileRate(0)
	return err
}

func validateLoopbackListenAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		return fmt.Errorf("pprof listen addr must use explicit loopback host: %s", addr)
	}
	if strings.EqualFold(host, "localhost") {
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

func normalizePprofListenAddr(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", nil
	}
	if strings.EqualFold(addr, "auto") {
		return "127.0.0.1:6060", nil
	}
	if _, err := strconv.Atoi(addr); err == nil {
		return "127.0.0.1:" + addr, nil
	}
	if strings.HasPrefix(addr, ":") {
		if _, err := strconv.Atoi(strings.TrimPrefix(addr, ":")); err != nil {
			return "", fmt.Errorf("invalid pprof port: %s", addr)
		}
		return "127.0.0.1" + addr, nil
	}
	return addr, nil
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
		return errors.New(walletUsage())
	}
	var err error
	switch args[0] {
	case "create":
		err = runWalletCreate(args[1:])
	case "list":
		err = runWalletList(args[1:])
	case "balance":
		err = runWalletBalance(args[1:])
	case "history":
		err = runWalletHistory(args[1:])
	case "fee":
		err = runWalletFee(args[1:])
	case "receive":
		err = runWalletReceive(args[1:])
	case "send":
		err = runWalletSend(args[1:])
	case "fanout":
		err = runWalletFanout(args[1:])
	case "backup":
		err = runWalletBackup(args[1:])
	case "restore":
		err = runWalletRestore(args[1:])
	case "export":
		err = runWalletExport(args[1:])
	case "import":
		err = runWalletImport(args[1:])
	case "cpfp":
		err = runWalletCPFP(args[1:])
	case "help", "--help", "-h":
		fmt.Print(walletUsage())
		return nil
	default:
		return fmt.Errorf("unknown wallet subcommand %q\n\n%s", args[0], walletUsage())
	}
	if errors.Is(err, errFlagHelpHandled) {
		return nil
	}
	return err
}

func walletUsage() string {
	return strings.TrimSpace(`wallet commands:
  bpu-cli wallet create main                 create your first wallet
  bpu-cli wallet receive [wallet]            get a fresh receive address
  bpu-cli wallet balance [wallet]            show spendable, pending, and immature funds
  bpu-cli wallet history [wallet]            show recent wallet activity
  bpu-cli wallet send ADDRESS AMOUNT         send BPU with guided fee selection
  bpu-cli wallet backup                      write a private local backup
  bpu-cli wallet list                        show wallets, profile, and store path

Advanced:
  fee, fanout, cpfp, export, import, restore
`) + "\n"
}

func setWalletFlagUsage(fs *flag.FlagSet, usage string) {
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s\n\nOptions:\n", usage)
		fs.PrintDefaults()
	}
}

var errFlagHelpHandled = errors.New("flag help handled")

func parseWalletFlags(fs *flag.FlagSet, args []string) error {
	reordered, err := reorderFlagsBeforePositionals(fs, args)
	if err != nil {
		return err
	}
	if err := fs.Parse(reordered); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return errFlagHelpHandled
		}
		return err
	}
	return nil
}

type boolFlagValue interface {
	IsBoolFlag() bool
}

func reorderFlagsBeforePositionals(fs *flag.FlagSet, args []string) ([]string, error) {
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			positionals = append(positionals, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if idx := strings.IndexByte(name, '='); idx >= 0 {
			name = name[:idx]
		}
		defined := fs.Lookup(name)
		if defined == nil {
			flags = append(flags, arg)
			continue
		}
		flags = append(flags, arg)
		if strings.Contains(arg, "=") {
			continue
		}
		if boolValue, ok := defined.Value.(boolFlagValue); ok && boolValue.IsBoolFlag() {
			continue
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("flag needs an argument: -%s", name)
		}
		i++
		flags = append(flags, args[i])
	}
	return append(flags, positionals...), nil
}

func runWalletCreate(args []string) error {
	fs := flag.NewFlagSet("wallet create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet create [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [name]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	family := fs.String("family", wallet.AddressFamilyXOnly, "receive address family: xonly or pq")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet create [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [name]")
	}
	walletName := "main"
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	}
	cfg, resolvedConfigPath, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	outputType, err := wallet.ParseAddressFamily(*family)
	if err != nil {
		return err
	}
	entry, addr, err := store.CreateWalletWithType(walletName, outputType)
	if err != nil {
		return err
	}
	fmt.Println("wallet created")
	fmt.Printf("wallet: %s\n", entry.Name)
	fmt.Printf("profile: %s\n", cfg.Profile)
	fmt.Printf("store: %s\n", walletPath)
	fmt.Printf("created_at: %s\n", entry.CreatedAt.Format(time.RFC3339))
	fmt.Printf("receive_address: %s\n", addr.Address)
	fmt.Printf("family: %s\n", wallet.AddressFamilyLabel(addr.OutputType()))
	printWalletAddressDetails(addr)
	fmt.Println("share only receive_address")
	fmt.Printf("next: after funds arrive, run bpu-cli wallet balance %s\n", entry.Name)
	fmt.Printf("backup: %s\n", walletBackupCommand(resolvedConfigPath, walletPath))
	return nil
}

func walletBackupCommand(configPath string, walletPath string) string {
	prefix := ""
	if isInstalledWalletContext(configPath, walletPath) {
		prefix = "sudo "
	}
	return fmt.Sprintf("%sbpu-cli wallet backup%s --wallet-dir %s", prefix, formatConfigFlag(configPath), filepath.Dir(walletPath))
}

func isInstalledWalletContext(configPath string, walletPath string) bool {
	configPath = filepath.Clean(strings.TrimSpace(configPath))
	walletPath = filepath.Clean(strings.TrimSpace(walletPath))
	return strings.HasPrefix(configPath, "/etc/bitcoin-pure/") ||
		configPath == "/etc/bitcoin-pure/config.yaml" ||
		strings.HasPrefix(walletPath, "/var/lib/bitcoin-pure/")
}

func runWalletList(args []string) error {
	fs := flag.NewFlagSet("wallet list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet list [--config PATH] [--wallet-dir DIR]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet list [--config PATH] [--wallet-dir DIR]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	wallets := store.List()
	fmt.Println("wallets")
	fmt.Printf("  profile  %s\n", cfg.Profile)
	fmt.Printf("  store    %s\n", walletPath)
	if len(wallets) == 0 {
		fmt.Println("  status   no wallets yet")
		fmt.Println("  next     bpu-cli wallet create main")
		return nil
	}
	if len(wallets) == 1 {
		fmt.Printf("  default  %s\n", wallets[0].Name)
	} else {
		fmt.Println("  default  none; pass a wallet name")
	}
	for _, entry := range wallets {
		receive := "-"
		if latest := entry.LatestReceiveAddress(); latest != nil {
			receive = latest.Address
		}
		fmt.Printf("  %-8s addresses=%d  pending=%d  receive=%s\n", entry.Name, len(entry.Addresses), len(entry.Pending), receive)
	}
	return nil
}

func runWalletBalance(args []string) error {
	fs := flag.NewFlagSet("wallet balance", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet balance [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet balance [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	if _, err := store.Wallet(walletName); err != nil {
		return walletCommandError(walletName, err)
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(walletName)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, walletName, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, walletName, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, walletName, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	balance, err := store.Balance(walletName, utxos)
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", walletName)
	fmt.Printf("profile: %s\n", cfg.Profile)
	fmt.Printf("store: %s\n", walletPath)
	fmt.Printf("confirmed: %s (%d atoms)\n", wallet.FormatAmount(balance.Confirmed), balance.Confirmed)
	fmt.Printf("mature: %s (%d atoms)\n", wallet.FormatAmount(balance.Mature), balance.Mature)
	fmt.Printf("available: %s (%d atoms)\n", wallet.FormatAmount(balance.Available), balance.Available)
	fmt.Printf("immature: %s (%d atoms)\n", wallet.FormatAmount(balance.Immature), balance.Immature)
	fmt.Printf("reserved: %s (%d atoms)\n", wallet.FormatAmount(balance.Reserved), balance.Reserved)
	fmt.Printf("pending_txs: %d\n", balance.PendingCount)
	fmt.Printf("addresses: %d\n", balance.AddressCount)
	if balance.Confirmed == 0 && balance.Available == 0 && balance.PendingCount == 0 {
		fmt.Printf("next: bpu-cli wallet receive %s\n", walletName)
	}
	return nil
}

func runWalletHistory(args []string) error {
	fs := flag.NewFlagSet("wallet history", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet history [--limit N] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	limit := fs.Int("limit", 20, "maximum activity rows to show")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet history [--limit N] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	}
	if *limit <= 0 {
		return errors.New("--limit must be positive")
	}
	if *limit > walletActivityRPCLimitMax {
		return fmt.Errorf("--limit must be <= %d", walletActivityRPCLimitMax)
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	watchItems, err := store.SpendableWatchItems(walletName)
	if err != nil {
		return walletCommandError(walletName, err)
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	activityLimit := walletMaxInt(*limit, walletReconcileActivityLimit(store, walletName, 20))
	if activityLimit > walletActivityRPCLimitMax {
		activityLimit = walletActivityRPCLimitMax
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, activityLimit)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, walletName, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, walletName, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	entry, err := store.Wallet(walletName)
	if err != nil {
		return walletCommandError(walletName, err)
	}
	if len(activity) > *limit {
		activity = activity[:*limit]
	}
	if len(activity) == 0 && len(entry.Pending) == 0 {
		fmt.Printf("wallet: %s\n", walletName)
		fmt.Println("activity: none yet")
		fmt.Printf("next: bpu-cli wallet receive %s\n", walletName)
		return nil
	}
	fmt.Printf("wallet: %s\n", walletName)
	for _, pending := range entry.Pending {
		fmt.Printf("pending  %s  tx=%s\n", pending.CreatedAt.Format(time.RFC3339), pending.TxID)
	}
	for _, item := range activity {
		fmt.Printf("%d  %s  tx=%s  received=%s  sent=%s  fee=%s  net=%s  %s\n",
			item.Height,
			item.Timestamp,
			item.TxID,
			wallet.FormatAmount(item.Received),
			wallet.FormatAmount(item.Sent),
			wallet.FormatAmount(item.Fee),
			formatSignedWalletAmount(item.Net),
			item.BlockHash,
		)
	}
	return nil
}

func runWalletFee(args []string) error {
	fs := flag.NewFlagSet("wallet fee", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet fee [--target-blocks N] [--tx-bytes N] [--config PATH] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	txBytes := fs.Int("tx-bytes", 250, "estimated transaction size in bytes")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet fee [--target-blocks N] [--tx-bytes N] [--config PATH] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	if *targetBlocks <= 0 {
		return errors.New("--target-blocks must be positive")
	}
	if *txBytes < 0 {
		return errors.New("--tx-bytes must be non-negative")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	feePerByte, err := rpcEstimateFee(client, *targetBlocks)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	fmt.Println("fee estimate")
	fmt.Printf("target_blocks: %d\n", *targetBlocks)
	fmt.Printf("fee_rate: %d atoms/B\n", feePerByte)
	estimatedFee := feePerByte * uint64(*txBytes)
	fmt.Printf("estimated_fee: %s (%d atoms)\n", wallet.FormatAmount(estimatedFee), estimatedFee)
	return nil
}

func runWalletReceive(args []string) error {
	fs := flag.NewFlagSet("wallet receive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet receive [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	family := fs.String("family", wallet.AddressFamilyXOnly, "receive address family: xonly or pq")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet receive [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [wallet]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	outputType, err := wallet.ParseAddressFamily(*family)
	if err != nil {
		return err
	}
	addr, err := store.NewReceiveAddressWithType(walletName, outputType)
	if err != nil {
		return walletCommandError(walletName, err)
	}
	fmt.Println("receive")
	fmt.Printf("wallet: %s\n", walletName)
	fmt.Printf("receive_address: %s\n", addr.Address)
	fmt.Printf("family: %s\n", wallet.AddressFamilyLabel(addr.OutputType()))
	printWalletAddressDetails(addr)
	fmt.Println("next: share only receive_address")
	return nil
}

func runWalletBackup(args []string) error {
	fs := flag.NewFlagSet("wallet backup", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet backup [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	out := fs.String("out", "", "backup output path")
	overwrite := fs.Bool("overwrite", false, "allow replacing an existing backup file")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet backup [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	if len(store.List()) == 0 {
		return errors.New("wallet backup refused: no wallets found; run bpu-cli wallet create first or pass the correct --config/--wallet-dir")
	}
	backupPath := strings.TrimSpace(*out)
	if backupPath == "" {
		stamp := time.Now().UTC().Format("20060102T150405Z")
		backupPath = filepath.Join(filepath.Dir(walletPath), "wallets-"+stamp+".backup.json")
	}
	if samePath(backupPath, walletPath) {
		return errors.New("backup output cannot be the live wallet store")
	}
	if err := store.BackupWithOptions(backupPath, *overwrite); err != nil {
		return err
	}
	fmt.Printf("wallet_store: %s\n", walletPath)
	fmt.Printf("backup: %s\n", backupPath)
	fmt.Println("keep this file private; it can spend these wallets")
	return nil
}

func runWalletRestore(args []string) error {
	fs := flag.NewFlagSet("wallet restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet restore --from PATH [--yes] [--force-profile-mismatch] [--config PATH] [--wallet-dir DIR]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	from := fs.String("from", "", "backup file to restore")
	yes := fs.Bool("yes", false, "restore without interactive confirmation")
	forceProfileMismatch := fs.Bool("force-profile-mismatch", false, "restore a backup from another chain profile")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() == 1 && *from == "" {
		*from = fs.Arg(0)
	}
	if *from == "" || fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet restore --from PATH [--yes] [--force-profile-mismatch] [--config PATH] [--wallet-dir DIR]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	backup, err := wallet.LoadBackupFile(*from)
	if err != nil {
		return err
	}
	if backup.Profile != "" && backup.Profile != types.ChainProfile(cfg.Profile) && !*forceProfileMismatch {
		return fmt.Errorf("backup profile %q does not match current profile %q; pass --force-profile-mismatch only if you are sure", backup.Profile, cfg.Profile)
	}
	if !*yes && !stdinLooksInteractive() {
		return errors.New("wallet restore requires --yes when stdin is not interactive")
	}
	existingWallets := store.List()
	if !*yes {
		fmt.Println("restore wallet backup")
		fmt.Printf("  source   %s\n", *from)
		fmt.Printf("  target   %s\n", walletPath)
		fmt.Printf("  profile  %s\n", cfg.Profile)
		fmt.Printf("  replace  %d wallet(s) with %d wallet(s)\n", len(existingWallets), len(backup.Wallets))
		fmt.Print("replace local wallet store? [y/N]: ")
		var response string
		if _, err := fmt.Fscanln(os.Stdin, &response); err != nil {
			return errors.New("restore cancelled")
		}
		if strings.ToLower(strings.TrimSpace(response)) != "y" && strings.ToLower(strings.TrimSpace(response)) != "yes" {
			return errors.New("restore cancelled")
		}
	}
	safetyBackup := ""
	if len(existingWallets) > 0 {
		stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
		safetyBackup = filepath.Join(filepath.Dir(walletPath), "pre-restore-"+stamp+".backup.json")
		if err := store.BackupWithOptions(safetyBackup, false); err != nil {
			return err
		}
	}
	if err := store.RestoreBackupWithOptions(*from, *forceProfileMismatch); err != nil {
		return err
	}
	fmt.Printf("restored: %s\n", walletPath)
	if safetyBackup != "" {
		fmt.Printf("previous_backup: %s\n", safetyBackup)
	}
	return nil
}

func runWalletExport(args []string) error {
	fs := flag.NewFlagSet("wallet export", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet export [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	out := fs.String("out", "", "export output path")
	overwrite := fs.Bool("overwrite", false, "allow replacing an existing export file")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet export [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR] [wallet]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	export, err := store.ExportWallet(walletName)
	if err != nil {
		return err
	}
	outPath := strings.TrimSpace(*out)
	if outPath == "" {
		outPath = filepath.Join(filepath.Dir(walletPath), safeWalletFileStem(walletName)+"-wallet-export.json")
	}
	if samePath(outPath, walletPath) {
		return errors.New("export output cannot be the live wallet store")
	}
	if err := wallet.SaveExportFileWithOptions(outPath, export, *overwrite); err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", walletName)
	fmt.Printf("export: %s\n", outPath)
	fmt.Println("keep this file private; it can spend this wallet")
	return nil
}

func runWalletImport(args []string) error {
	fs := flag.NewFlagSet("wallet import", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet import [--name NAME] [--force] [--config PATH] [--wallet-dir DIR] <export-file>")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	name := fs.String("name", "", "imported wallet name")
	force := fs.Bool("force", false, "import a wallet from another chain profile")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet import [--name NAME] [--force] [--config PATH] [--wallet-dir DIR] <export-file>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	export, err := wallet.LoadExportFile(fs.Arg(0))
	if err != nil {
		return err
	}
	imported, err := store.ImportWalletWithOptions(export, *name, *force)
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", imported.Name)
	fmt.Printf("profile: %s\n", cfg.Profile)
	fmt.Printf("addresses: %d\n", len(imported.Addresses))
	return nil
}

func runWalletSend(args []string) error {
	fs := flag.NewFlagSet("wallet send", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet send [ADDRESS AMOUNT] [--from NAME] [--amount BPU | --amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	from := fs.String("from", "", "wallet to spend from")
	to := fs.String("to", "", "destination receive address")
	amountRaw := fs.String("amount", "", "amount in BPU")
	amountAtoms := fs.Uint64("amount-atoms", 0, "amount in atoms")
	fee := fs.Uint64("fee", 0, "exact fee in atoms")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	targetMinutes := fs.Int("target-minutes", 0, "rough confirmation target in minutes")
	priority := fs.String("priority", "", "fee target: now, soon, relaxed, or cheap")
	yes := fs.Bool("yes", false, "broadcast without interactive confirmation")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	switch fs.NArg() {
	case 0:
	case 2:
		if *to != "" || *amountRaw != "" || *amountAtoms != 0 {
			return errors.New("positional ADDRESS AMOUNT cannot be combined with --to, --amount, or --amount-atoms")
		}
		if *to == "" {
			*to = fs.Arg(0)
		}
		if *amountRaw == "" && *amountAtoms == 0 {
			*amountRaw = fs.Arg(1)
		}
	default:
		return errors.New("usage: bpu-cli wallet send [ADDRESS AMOUNT] [--from NAME] [--amount BPU | --amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	if err := completeWalletSendInputs(store, from, to, amountRaw, amountAtoms, *yes); err != nil {
		return err
	}
	amount, err := resolveWalletAmount(*amountRaw, *amountAtoms)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(*from)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, *from, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, *from, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, *from, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	plan := wallet.SendPlan{}
	var feeQuote *walletFeeQuote
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
		}
		plan, err = store.BuildSend(*from, *to, amount, *fee, utxos)
		if err != nil {
			return walletCommandError(*from, err)
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
		plan, err = store.BuildSendAuto(*from, *to, amount, quote.FeeRate, utxos)
		if err != nil {
			return walletCommandError(*from, err)
		}
	}
	if err := maybeConfirmWalletAction(withWalletContext(renderSendPreview(plan, feeQuote), cfg, resolveRPCAddr(cfg, *rpcAddr)), *yes); err != nil {
		return err
	}
	result, err := submitWalletSendPlan(store, client, *from, plan)
	if err != nil {
		return err
	}
	printWalletAction(renderSendResult(plan, result.TxID, feeQuote))
	return nil
}

func runWalletFanout(args []string) error {
	fs := flag.NewFlagSet("wallet fanout", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet fanout --to ADDRESS[,ADDRESS...] --amount BPU --count N [--from NAME] [--amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	from := fs.String("from", "", "wallet to spend from")
	toRaw := fs.String("to", "", "comma-separated destination addresses")
	amountRaw := fs.String("amount", "", "amount per transaction in BPU")
	amountAtoms := fs.Uint64("amount-atoms", 0, "amount per transaction in atoms")
	count := fs.Int("count", 0, "number of transactions to create")
	fee := fs.Uint64("fee", 0, "exact fee per transaction in atoms")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	targetMinutes := fs.Int("target-minutes", 0, "rough confirmation target in minutes")
	priority := fs.String("priority", "", "fee target: now, soon, relaxed, or cheap")
	yes := fs.Bool("yes", false, "broadcast without interactive confirmation")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet fanout --to ADDRESS[,ADDRESS...] --amount BPU --count N [--from NAME] [--amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	if strings.TrimSpace(*toRaw) == "" || *count <= 0 {
		return errors.New("wallet fanout requires --to ADDRESS[,ADDRESS...] and --count N")
	}
	amount, err := resolveWalletAmount(*amountRaw, *amountAtoms)
	if err != nil {
		return err
	}
	destinations := splitCSV(*toRaw)
	if len(destinations) == 0 {
		return errors.New("wallet fanout requires at least one destination")
	}
	if amount > ^uint64(0)/uint64(*count) {
		return errors.New("wallet fanout total amount overflows atoms")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*from) == "" {
		name, err := defaultWalletNameFromFlag(store, stdinLooksInteractive() && !*yes)
		if err != nil {
			return err
		}
		*from = name
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(*from)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, *from, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, *from, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, *from, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	var feeQuote *walletFeeQuote
	feeRate := uint64(0)
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
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
		feeRate = quote.FeeRate
	}
	plans := make([]wallet.SendPlan, 0, *count)
	workingUTXOs := append([]wallet.SpendableUTXO(nil), utxos...)
	knownAddresses := make([]wallet.Address, 0, *count)
	for i := 0; i < *count; i++ {
		to := destinations[i%len(destinations)]
		var plan wallet.SendPlan
		if *fee > 0 {
			plan, err = store.BuildSendWithKnownAddresses(*from, to, amount, *fee, workingUTXOs, knownAddresses)
		} else {
			plan, err = store.BuildSendAutoWithKnownAddresses(*from, to, amount, feeRate, workingUTXOs, knownAddresses)
		}
		if err != nil {
			return walletCommandError(*from, fmt.Errorf("fanout plan %d/%d: %w", i+1, *count, err))
		}
		plans = append(plans, plan)
		if plan.ChangeAddress != nil {
			knownAddresses = append(knownAddresses, *plan.ChangeAddress)
		}
		workingUTXOs = applyFanoutPlanToUTXOs(workingUTXOs, plan)
	}
	preview := withWalletContext(renderFanoutPlansPreview(*from, destinations, plans, feeQuote), cfg, resolveRPCAddr(cfg, *rpcAddr))
	if err := maybeConfirmWalletAction(preview, *yes); err != nil {
		return err
	}
	results := make([]walletFanoutResult, 0, *count)
	for i, plan := range plans {
		result, err := submitWalletSendPlan(store, client, *from, plan)
		if err != nil {
			if len(results) > 0 {
				printWalletFanoutPartialResult(*from, results, len(plans), feeQuote)
			}
			return fmt.Errorf("fanout tx %d/%d: %w", i+1, *count, err)
		}
		results = append(results, walletFanoutResult{Plan: plan, TxID: result.TxID})
	}
	printWalletFanoutResult(*from, results, feeQuote)
	return nil
}

func runWalletCPFP(args []string) error {
	fs := flag.NewFlagSet("wallet cpfp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet cpfp [PARENT_TXID] [--from NAME] [--txid PARENT_TXID] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	from := fs.String("from", "", "wallet to spend from")
	parent := fs.String("txid", "", "parent transaction id to accelerate")
	fee := fs.Uint64("fee", 0, "exact child fee in atoms")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	targetMinutes := fs.Int("target-minutes", 0, "rough confirmation target in minutes")
	priority := fs.String("priority", "", "fee target: now, soon, relaxed, or cheap")
	yes := fs.Bool("yes", false, "broadcast without interactive confirmation")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() == 1 && *parent == "" {
		*parent = fs.Arg(0)
	}
	if *parent == "" || fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet cpfp [PARENT_TXID] [--from NAME] [--txid PARENT_TXID] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
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
	if strings.TrimSpace(*from) == "" {
		name, err := defaultWalletNameFromFlag(store, stdinLooksInteractive() && !*yes)
		if err != nil {
			return err
		}
		*from = name
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(*from)
	if err != nil {
		return walletCommandError(*from, err)
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, *from, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, *from, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, *from, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	var plan wallet.CPFPPlan
	var feeQuote *walletFeeQuote
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
		}
		plan, err = store.BuildCPFPWithExactFee(*from, parentTxID, *fee)
		if err != nil {
			return walletCommandError(*from, err)
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
			return walletCommandError(*from, err)
		}
	}
	if err := maybeConfirmWalletAction(withWalletContext(renderCPFPPreview(plan, feeQuote), cfg, resolveRPCAddr(cfg, *rpcAddr)), *yes); err != nil {
		return err
	}
	if err := store.MarkSubmitted(*from, plan.TransactionID, plan.Transaction, []wallet.SelectedInput{plan.Input}, &plan.SweepAddress); err != nil {
		return err
	}
	var result struct {
		TxID     string `json:"txid"`
		Fee      uint64 `json:"fee"`
		Orphaned bool   `json:"orphaned"`
	}
	if err := client.Call("submittx", map[string]string{"hex": plan.TransactionHex}, &result); err != nil {
		var remoteErr cliRPCRemoteError
		if errors.As(err, &remoteErr) {
			_ = store.ForgetPending(*from, plan.TransactionID)
		}
		return err
	}
	reportedTxID, err := decodeHex32(result.TxID)
	if err != nil {
		_ = store.ForgetPending(*from, plan.TransactionID)
		return err
	}
	if reportedTxID != plan.TransactionID {
		_ = store.ForgetPending(*from, plan.TransactionID)
		return fmt.Errorf("submitted txid mismatch: planned %x, node returned %s", plan.TransactionID, result.TxID)
	}
	if result.Orphaned {
		_ = store.ForgetPending(*from, plan.TransactionID)
		return fmt.Errorf("node stored CPFP child %s as an orphan; parent %x is not currently spendable by the node", result.TxID, plan.ParentTxID)
	}
	printWalletAction(renderCPFPResult(plan, result.TxID, feeQuote))
	return nil
}

func runChainInit(args []string) error {
	fs := flag.NewFlagSet("chain init", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profileRaw := fs.String("profile", config.Default().Profile, "")
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
		return nil, errors.New("benchnet genesis is not available in public CLI fixtures")
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
		utxos[types.OutPoint{TxID: txID, Vout: uint32(vout)}] = consensus.UtxoEntryFromOutput(output)
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

type cliRPCRemoteError struct {
	Method  string
	Message string
}

func (e cliRPCRemoteError) Error() string {
	return e.Message
}

func newRPCClient(addr string, token string, timeout time.Duration) *cliRPCClient {
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	return &cliRPCClient{
		addr:  strings.TrimSpace(addr),
		token: strings.TrimSpace(token),
		http:  &http.Client{Timeout: timeout},
	}
}

func rpcClientTimeout(cfg config.Config) time.Duration {
	timeout := time.Duration(cfg.RPCWriteTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	timeout += 5 * time.Second
	if timeout < 15*time.Second {
		return 15 * time.Second
	}
	return timeout
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
	started := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("rpc %s to %s failed after %s: %w", method, endpoint, time.Since(started).Round(time.Millisecond), err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("rpc %s read response from %s after %s: %w", method, endpoint, time.Since(started).Round(time.Millisecond), err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rpc %s returned %s: %s", method, resp.Status, strings.TrimSpace(string(respBody)))
	}
	var rpcResp cliRPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return err
	}
	if rpcResp.Error != "" {
		return cliRPCRemoteError{Method: method, Message: rpcResp.Error}
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
		if errors.Is(err, os.ErrPermission) {
			return nil, "", fmt.Errorf("wallet store %s is not readable or writable; run as the wallet owner or pass --wallet-dir DIR", path)
		}
		return nil, "", err
	}
	return store, path, nil
}

func samePath(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	leftEval, leftEvalErr := filepath.EvalSymlinks(left)
	rightEval, rightEvalErr := filepath.EvalSymlinks(right)
	if leftEvalErr == nil && rightEvalErr == nil {
		return leftEval == rightEval
	}
	leftAbs, leftErr := filepath.Abs(filepath.Clean(left))
	rightAbs, rightErr := filepath.Abs(filepath.Clean(right))
	if leftErr == nil && rightErr == nil {
		return leftAbs == rightAbs
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func formatConfigFlag(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return " --config " + path
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
		if latest := latestReceiveAddressWithType(existing, types.OutputXOnlyP2PK); latest != nil {
			addr = *latest
		} else {
			addr, err = store.NewReceiveAddressWithType(minerWalletName, types.OutputXOnlyP2PK)
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
	if strings.TrimSpace(addr.PubKeyHex) == "" {
		return wallet.Address{}, "", errors.New("miner wallet did not produce an xonly mining pubkey")
	}
	cfg.MinerPubKeyHex = addr.PubKeyHex
	if err := config.Save(configPath, *cfg); err != nil {
		return wallet.Address{}, "", err
	}
	return addr, walletPath, nil
}

func rejectInstalledMiningAutoProvision(configPath string, cfg config.Config) error {
	if !cfg.MinerEnabled || strings.TrimSpace(cfg.MinerPubKeyHex) != "" {
		return nil
	}
	cleanConfig := filepath.Clean(strings.TrimSpace(configPath))
	if cleanConfig == "" || !isInstalledConfigPath(cleanConfig) {
		return nil
	}
	return errors.New("installed service config has mining enabled without miner_pubkey_hex; run ./install --mining on to provision the miner wallet before service start")
}

func isInstalledConfigPath(path string) bool {
	clean := filepath.Clean(path)
	for _, candidate := range config.DefaultPathCandidates() {
		if clean == filepath.Clean(candidate) {
			return true
		}
	}
	return false
}

func latestReceiveAddressWithType(entry wallet.Wallet, outputType uint64) *wallet.Address {
	for i := len(entry.Addresses) - 1; i >= 0; i-- {
		if entry.Addresses[i].Change || entry.Addresses[i].OutputType() != outputType {
			continue
		}
		addr := entry.Addresses[i]
		return &addr
	}
	return nil
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

func walletRPCError(err error, cfg config.Config, override string) error {
	if err == nil {
		return nil
	}
	addr := resolveRPCAddr(cfg, override)
	if strings.TrimSpace(addr) == "" {
		addr = "(not configured)"
	}
	return fmt.Errorf("wallet cannot reach node RPC at %s\nnext: start bpu-cli serve or pass --rpc ADDR\ndetail: %w", addr, err)
}

func walletCommandError(walletName string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, wallet.ErrInsufficientFunds):
		return fmt.Errorf("%w\nwallet %q does not have enough spendable funds yet; run `bpu-cli wallet balance %s` to check available, pending, and immature funds", err, walletName, walletName)
	case errors.Is(err, wallet.ErrWalletNotFound):
		return fmt.Errorf("%w\nwallet %q was not found; run `bpu-cli wallet list`", err, walletName)
	default:
		return err
	}
}

func reconcileWalletPending(store *wallet.Store, client *cliRPCClient, walletName string, utxos []wallet.SpendableUTXO, confirmed map[[32]byte]struct{}) error {
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
	_, err := store.ReconcilePendingWithStatus(walletName, mempool, utxos, confirmed)
	return err
}

func walletReconcileActivityLimit(store *wallet.Store, walletName string, fallback int) int {
	entry, err := store.Wallet(walletName)
	if err != nil {
		return fallback
	}
	return walletMinInt(walletActivityRPCLimitMax, walletMaxInt(fallback, len(entry.Pending)*2))
}

func confirmedWalletTxIDs(client *cliRPCClient, store *wallet.Store, walletName string, activity []walletActivityRPCItem) (map[[32]byte]struct{}, error) {
	out := make(map[[32]byte]struct{}, len(activity))
	for _, item := range activity {
		txid, err := decodeHex32(item.TxID)
		if err != nil {
			continue
		}
		out[txid] = struct{}{}
	}
	entry, err := store.Wallet(walletName)
	if err != nil {
		return nil, err
	}
	for _, pending := range entry.Pending {
		txid, err := decodeHex32(pending.TxID)
		if err != nil {
			continue
		}
		if _, ok := out[txid]; ok {
			continue
		}
		status, err := rpcTxStatus(client, pending.TxID)
		if err != nil {
			return nil, err
		}
		if status.Confirmed {
			out[txid] = struct{}{}
		}
	}
	return out, nil
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
			TxID          string `json:"txid"`
			Vout          uint32 `json:"vout"`
			Value         uint64 `json:"value"`
			Type          uint64 `json:"type"`
			Payload32     string `json:"payload32"`
			Height        uint64 `json:"height"`
			Confirmations uint64 `json:"confirmations"`
			Coinbase      bool   `json:"coinbase"`
			Mature        bool   `json:"mature"`
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
			OutPoint:      types.OutPoint{TxID: txid, Vout: item.Vout},
			Value:         item.Value,
			Type:          item.Type,
			Payload32:     payload32,
			PubKey:        legacyWalletPubKey(item.Type, payload32),
			Height:        item.Height,
			Confirmations: item.Confirmations,
			Coinbase:      item.Coinbase,
			Mature:        item.Mature,
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

type walletTxStatusRPCItem struct {
	TxID      string `json:"txid"`
	Confirmed bool   `json:"confirmed"`
	Mempool   bool   `json:"mempool"`
	BlockHash string `json:"block_hash"`
}

func rpcTxStatus(client *cliRPCClient, txid string) (walletTxStatusRPCItem, error) {
	var result walletTxStatusRPCItem
	if err := client.Call("gettxstatus", map[string]string{"txid": txid}, &result); err != nil {
		return walletTxStatusRPCItem{}, err
	}
	return result, nil
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

type cliNodeStatus struct {
	Info    node.ServiceInfo
	Mempool node.MempoolInfo
	Mining  node.MiningInfo
	Peers   []node.PeerInfo
}

func fetchNodeStatus(client *cliRPCClient) (cliNodeStatus, error) {
	var status cliNodeStatus
	if err := client.Call("getinfo", nil, &status.Info); err != nil {
		return cliNodeStatus{}, err
	}
	if err := client.Call("getmempoolinfo", nil, &status.Mempool); err != nil {
		return cliNodeStatus{}, err
	}
	if err := client.Call("getmininginfo", nil, &status.Mining); err != nil {
		return cliNodeStatus{}, err
	}
	if err := client.Call("getpeerinfo", nil, &status.Peers); err != nil {
		return cliNodeStatus{}, err
	}
	return status, nil
}

func renderNodeStatus(status cliNodeStatus, cfg config.Config) string {
	peerWord := "peers"
	if len(status.Peers) == 1 {
		peerWord = "peer"
	}
	syncState := "synced"
	if len(status.Peers) == 0 {
		syncState = "waiting for peers"
	} else if status.Info.HeaderHeight > status.Info.TipHeight {
		syncState = fmt.Sprintf("syncing (%d headers ahead)", status.Info.HeaderHeight-status.Info.TipHeight)
	}
	mining := "off"
	if status.Mining.Enabled {
		if status.Mining.Workers > 0 {
			mining = fmt.Sprintf("on (%d workers)", status.Mining.Workers)
		} else {
			mining = "on (auto workers)"
		}
	}
	dashboard := status.Info.RPCAddr
	if dashboard == "" {
		dashboard = cfg.RPCAddr
	}
	if dashboard != "" && !strings.Contains(dashboard, "://") {
		dashboard = "http://" + dashboard + "/"
	}
	rows := []walletActionRow{
		{label: "health", value: syncState},
		{label: "profile", value: status.Info.Profile},
		{label: "height", value: fmt.Sprintf("blocks=%d headers=%d", status.Info.TipHeight, status.Info.HeaderHeight)},
		{label: "tip", value: shortenHex(status.Info.TipHeaderHash, 16)},
		{label: "peers", value: fmt.Sprintf("%d %s", len(status.Peers), peerWord)},
		{label: "mempool", value: fmt.Sprintf("%d tx / %d bytes / %d orphans", status.Mempool.Count, status.Mempool.Bytes, status.Mempool.Orphans)},
		{label: "fees", value: fmt.Sprintf("min=%d median=%d high=%d atoms/B", status.Mempool.MinRelayFeePerByte, status.Mempool.MedianFee, status.Mempool.HighFee)},
		{label: "mining", value: mining},
	}
	if dashboard != "" {
		rows = append(rows, walletActionRow{label: "monitor", value: dashboard})
	}
	rows = append(rows, walletActionRow{label: "logs", value: shortenDisplayPath(resolveLogPath(cfg), 56)})
	return renderTerminalBox("Bitcoin Pure status", rows)
}

func shortenHex(raw string, keep int) string {
	raw = strings.TrimSpace(raw)
	if keep <= 0 || len(raw) <= keep {
		return raw
	}
	return raw[:keep] + "..."
}

func shortenDisplayPath(path string, keep int) string {
	path = filepath.Clean(strings.TrimSpace(path))
	if keep <= 0 || len(path) <= keep {
		return path
	}
	base := filepath.Base(path)
	parent := filepath.Base(filepath.Dir(path))
	suffix := filepath.Join(parent, base)
	if len(suffix)+4 <= keep {
		return ".../" + suffix
	}
	if len(base)+4 <= keep {
		return ".../" + base
	}
	return "..." + base[len(base)-(keep-3):]
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

type walletSubmitResult struct {
	TxID     string `json:"txid"`
	Fee      uint64 `json:"fee"`
	Orphaned bool   `json:"orphaned"`
}

type walletFanoutResult struct {
	Plan wallet.SendPlan
	TxID string
}

func submitWalletSendPlan(store *wallet.Store, client *cliRPCClient, walletName string, plan wallet.SendPlan) (walletSubmitResult, error) {
	if err := store.MarkSubmitted(walletName, plan.TransactionID, plan.Transaction, plan.Inputs, plan.ChangeAddress); err != nil {
		return walletSubmitResult{}, err
	}
	var result walletSubmitResult
	if err := client.Call("submittx", map[string]string{"hex": plan.TransactionHex}, &result); err != nil {
		var remoteErr cliRPCRemoteError
		if errors.As(err, &remoteErr) {
			_ = store.ForgetPending(walletName, plan.TransactionID)
		}
		return walletSubmitResult{}, err
	}
	reportedTxID, err := decodeHex32(result.TxID)
	if err != nil {
		_ = store.ForgetPending(walletName, plan.TransactionID)
		return walletSubmitResult{}, err
	}
	if reportedTxID != plan.TransactionID {
		_ = store.ForgetPending(walletName, plan.TransactionID)
		return walletSubmitResult{}, fmt.Errorf("submitted txid mismatch: planned %x, node returned %s", plan.TransactionID, result.TxID)
	}
	if result.Orphaned {
		_ = store.ForgetPending(walletName, plan.TransactionID)
		return walletSubmitResult{}, fmt.Errorf("node stored transaction %s as an orphan; the node is missing at least one input", result.TxID)
	}
	return result, nil
}

func completeWalletSendInputs(store *wallet.Store, from *string, to *string, amountRaw *string, amountAtoms *uint64, yes bool) error {
	if strings.TrimSpace(*from) == "" {
		name, err := defaultWalletNameFromFlag(store, stdinLooksInteractive() && !yes)
		if err != nil {
			return err
		}
		*from = name
	}
	if strings.TrimSpace(*to) == "" || (strings.TrimSpace(*amountRaw) == "" && *amountAtoms == 0) {
		if yes || !stdinLooksInteractive() {
			return errors.New("usage: bpu-cli wallet send [ADDRESS AMOUNT] [--from NAME] [--amount BPU | --amount-atoms ATOMS]")
		}
		reader := bufio.NewReader(os.Stdin)
		if strings.TrimSpace(*to) == "" {
			line, err := promptLine(reader, "to: ")
			if err != nil {
				return err
			}
			*to = line
		}
		if strings.TrimSpace(*amountRaw) == "" && *amountAtoms == 0 {
			line, err := promptLine(reader, "amount (BPU, or append atoms): ")
			if err != nil {
				return err
			}
			*amountRaw = line
		}
	}
	if strings.TrimSpace(*to) == "" {
		return errors.New("destination address is required")
	}
	return nil
}

func defaultWalletName(store *wallet.Store, allowPrompt bool) (string, error) {
	return defaultWalletNameWithHint(store, allowPrompt, "pass a wallet name")
}

func defaultWalletNameFromFlag(store *wallet.Store, allowPrompt bool) (string, error) {
	return defaultWalletNameWithHint(store, allowPrompt, "pass --from NAME")
}

func defaultWalletNameWithHint(store *wallet.Store, allowPrompt bool, hint string) (string, error) {
	wallets := store.List()
	switch len(wallets) {
	case 0:
		return "", errors.New("no wallets yet\nnext: bpu-cli wallet create")
	case 1:
		return wallets[0].Name, nil
	}
	if !allowPrompt {
		names := make([]string, 0, len(wallets))
		for _, entry := range wallets {
			names = append(names, entry.Name)
		}
		return "", fmt.Errorf("multiple wallets found (%s); %s", strings.Join(names, ", "), hint)
	}
	fmt.Println("choose wallet")
	for i, entry := range wallets {
		fmt.Printf("  %d) %s\n", i+1, entry.Name)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := promptLine(reader, "choose wallet: ")
		if err != nil {
			return "", err
		}
		choice, err := strconv.Atoi(strings.TrimSpace(line))
		if err == nil && choice >= 1 && choice <= len(wallets) {
			return wallets[choice-1].Name, nil
		}
		for _, entry := range wallets {
			if strings.EqualFold(entry.Name, strings.TrimSpace(line)) {
				return entry.Name, nil
			}
		}
		fmt.Println("enter a wallet number or name")
	}
}

func promptLine(reader *bufio.Reader, prompt string) (string, error) {
	fmt.Print(prompt)
	raw, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	line := strings.TrimSpace(raw)
	if line == "" {
		return "", errors.New("input cancelled")
	}
	return line, nil
}

func resolveWalletAmount(raw string, atoms uint64) (uint64, error) {
	if strings.TrimSpace(raw) != "" && atoms != 0 {
		return 0, errors.New("--amount and --amount-atoms cannot be combined")
	}
	if atoms != 0 {
		return atoms, nil
	}
	return wallet.ParseAmount(raw)
}

func formatSignedWalletAmount(value int64) string {
	if value < 0 {
		return "-" + wallet.FormatAmount(uint64(-value))
	}
	return wallet.FormatAmount(uint64(value))
}

type walletActionRow struct {
	label string
	value string
}

type walletActionView struct {
	title string
	rows  []walletActionRow
}

func withWalletContext(view walletActionView, cfg config.Config, rpcAddr string) walletActionView {
	rows := make([]walletActionRow, 0, len(view.rows)+2)
	rows = append(rows,
		walletActionRow{label: "profile", value: cfg.Profile},
		walletActionRow{label: "rpc", value: rpcAddr},
	)
	rows = append(rows, view.rows...)
	view.rows = rows
	return view
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
		{label: "amount", value: formatWalletAmountWithAtoms(plan.Amount)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
		walletActionRow{label: "inputs", value: fmt.Sprintf("%d (%s)", len(plan.Inputs), formatWalletAmountWithAtoms(plan.InputTotal))},
		walletActionRow{label: "txid", value: fmt.Sprintf("%x", plan.TransactionID)},
	)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		rows = append(rows, walletActionRow{label: "change", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Change), plan.ChangeAddress.Address)})
	}
	return walletActionView{title: "send", rows: rows}
}

func renderSendResult(plan wallet.SendPlan, txid string, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "txid", value: txid},
		{label: "amount", value: formatWalletAmountWithAtoms(plan.Amount)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
	)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		rows = append(rows, walletActionRow{label: "change", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Change), plan.ChangeAddress.Address)})
	}
	return walletActionView{title: "submitted", rows: rows}
}

func renderCPFPPreview(plan wallet.CPFPPlan, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "parent", value: hex.EncodeToString(plan.ParentTxID[:])},
		{label: "scope", value: "child fee only; parent package rate not estimated"},
		{label: "source", value: fmt.Sprintf("%x:%d (%s)", plan.Input.OutPoint.TxID, plan.Input.OutPoint.Vout, formatWalletAmountWithAtoms(plan.Input.Value))},
		{label: "child", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Amount), plan.SweepAddress.Address)},
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
		{label: "scope", value: "child fee only; parent package rate not estimated"},
		{label: "child", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Amount), plan.SweepAddress.Address)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows, walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)})
	return walletActionView{title: "submitted", rows: rows}
}

func formatWalletFee(fee uint64, feeRate uint64, estimatedBytes int) string {
	if feeRate == 0 || estimatedBytes == 0 {
		return formatWalletAmountWithAtoms(fee)
	}
	return fmt.Sprintf("%s (%d atoms/B, %d B)", formatWalletAmountWithAtoms(fee), feeRate, estimatedBytes)
}

func formatWalletAmountWithAtoms(atoms uint64) string {
	return fmt.Sprintf("%s / %d atoms", wallet.FormatAmount(atoms), atoms)
}

func renderFanoutPreview(walletName string, destinations []string, amount uint64, count int, fee uint64, feeRate uint64, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: walletName},
		{label: "txs", value: fmt.Sprintf("%d", count)},
		{label: "to", value: formatWalletDestinationSummary(destinations)},
		{label: "amount", value: formatWalletAmountWithAtoms(amount)},
		{label: "total", value: formatWalletAmountWithAtoms(amount * uint64(count))},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	switch {
	case fee > 0:
		rows = append(rows, walletActionRow{label: "fee", value: formatWalletAmountWithAtoms(fee) + " each"})
	case feeRate > 0:
		rows = append(rows, walletActionRow{label: "fee", value: fmt.Sprintf("%d atoms/B", feeRate)})
	}
	return walletActionView{title: "fanout", rows: rows}
}

func renderFanoutPlansPreview(walletName string, destinations []string, plans []wallet.SendPlan, feeQuote *walletFeeQuote) walletActionView {
	totalAmount := uint64(0)
	totalFee := uint64(0)
	totalInputs := 0
	plannedTxIDs := make(map[[32]byte]struct{}, len(plans))
	dependentTxs := 0
	for _, plan := range plans {
		dependsOnEarlier := false
		for _, input := range plan.Inputs {
			if _, ok := plannedTxIDs[input.OutPoint.TxID]; ok {
				dependsOnEarlier = true
			}
		}
		if dependsOnEarlier {
			dependentTxs++
		}
		plannedTxIDs[plan.TransactionID] = struct{}{}
		totalAmount += plan.Amount
		totalFee += plan.Fee
		totalInputs += len(plan.Inputs)
	}
	rows := []walletActionRow{
		{label: "wallet", value: walletName},
		{label: "txs", value: fmt.Sprintf("%d", len(plans))},
		{label: "to", value: formatWalletDestinationSummary(destinations)},
		{label: "total", value: formatWalletAmountWithAtoms(totalAmount)},
		{label: "fee", value: formatWalletAmountWithAtoms(totalFee)},
		{label: "inputs", value: fmt.Sprintf("%d total", totalInputs)},
	}
	if dependentTxs > 0 {
		rows = append(rows, walletActionRow{label: "chain", value: fmt.Sprintf("%d txs spend earlier fanout change", dependentTxs)})
	} else {
		rows = append(rows, walletActionRow{label: "chain", value: "independent inputs"})
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	return walletActionView{title: "fanout", rows: rows}
}

func printWalletFanoutResult(walletName string, results []walletFanoutResult, feeQuote *walletFeeQuote) {
	printWalletFanoutResultWithTitle("fanout submitted", walletName, results, len(results), feeQuote)
}

func printWalletFanoutPartialResult(walletName string, results []walletFanoutResult, plannedCount int, feeQuote *walletFeeQuote) {
	printWalletFanoutResultWithTitle("fanout partial failure", walletName, results, plannedCount, feeQuote)
}

func printWalletFanoutResultWithTitle(title string, walletName string, results []walletFanoutResult, plannedCount int, feeQuote *walletFeeQuote) {
	totalAmount := uint64(0)
	totalFee := uint64(0)
	for _, result := range results {
		totalAmount += result.Plan.Amount
		totalFee += result.Plan.Fee
	}
	rows := []walletActionRow{
		{label: "wallet", value: walletName},
		{label: "txs", value: fmt.Sprintf("%d/%d", len(results), plannedCount)},
		{label: "amount", value: formatWalletAmountWithAtoms(totalAmount)},
		{label: "fee", value: formatWalletAmountWithAtoms(totalFee)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	printWalletAction(walletActionView{title: title, rows: rows})
	for i, result := range results {
		fmt.Printf("  %03d  %s  %s\n", i+1, result.TxID, result.Plan.ToAddress)
	}
}

func applyFanoutPlanToUTXOs(utxos []wallet.SpendableUTXO, plan wallet.SendPlan) []wallet.SpendableUTXO {
	spent := make(map[types.OutPoint]struct{}, len(plan.Inputs))
	for _, input := range plan.Inputs {
		spent[input.OutPoint] = struct{}{}
	}
	next := make([]wallet.SpendableUTXO, 0, len(utxos)+1)
	for _, utxo := range utxos {
		if _, ok := spent[utxo.OutPoint]; ok {
			continue
		}
		next = append(next, utxo)
	}
	if plan.Change > 0 && plan.ChangeAddress != nil {
		item, err := plan.ChangeAddress.WatchItem()
		if err == nil {
			next = append(next, wallet.SpendableUTXO{
				OutPoint:  types.OutPoint{TxID: plan.TransactionID, Vout: 1},
				Value:     plan.Change,
				Type:      item.Type,
				Payload32: item.Payload32,
				PubKey:    legacyWalletPubKey(item.Type, item.Payload32),
			})
		}
	}
	return next
}

func formatWalletDestinationSummary(destinations []string) string {
	if len(destinations) == 0 {
		return "-"
	}
	if len(destinations) == 1 {
		return destinations[0]
	}
	return fmt.Sprintf("%d addresses, round-robin from %s", len(destinations), destinations[0])
}

func safeWalletFileStem(name string) string {
	var b strings.Builder
	for _, ch := range strings.TrimSpace(name) {
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			b.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			b.WriteRune(ch)
		case ch == '-' || ch == '_':
			b.WriteRune(ch)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "wallet"
	}
	return b.String()
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
		label, blocks, err := parseWalletFeePriority(recommendedWalletFeeLabel(info))
		if err != nil {
			return walletFeeQuote{}, err
		}
		return buildWalletFeeQuote(label, blocks, blocks*10, info, estimate)
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

func walletMinInt(a, b int) int {
	if a < b {
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
	return fmt.Sprintf("%s, %d tx, fee totals median %d atoms, range %d-%d", pressure, info.Count, info.MedianFee, info.LowFee, info.HighFee)
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
	title := strings.TrimSpace(view.title)
	if title == "" {
		title = "wallet"
	}
	fmt.Print(renderTerminalBox(title, view.rows))
}

const (
	terminalBoxWidth      = 72
	terminalBoxLabelWidth = 9
)

func renderTerminalBox(title string, rows []walletActionRow) string {
	title = strings.TrimSpace(title)
	if title == "" {
		title = "status"
	}
	innerWidth := terminalBoxWidth - 2
	contentWidth := innerWidth - 2
	var b strings.Builder
	border := "+" + strings.Repeat("-", innerWidth) + "+\n"
	b.WriteString(border)
	b.WriteString(formatTerminalBoxLine(title))
	b.WriteString(border)
	valueWidth := contentWidth - terminalBoxLabelWidth - 2
	if valueWidth < 16 {
		valueWidth = 16
	}
	for _, row := range rows {
		label := strings.TrimSpace(row.label)
		if len(label) > terminalBoxLabelWidth {
			label = label[:terminalBoxLabelWidth]
		}
		lines := wrapTerminalText(strings.TrimSpace(row.value), valueWidth)
		if len(lines) == 0 {
			lines = []string{""}
		}
		for i, line := range lines {
			rowLabel := label
			if i > 0 {
				rowLabel = ""
			}
			b.WriteString(formatTerminalBoxLine(fmt.Sprintf("%-*s  %s", terminalBoxLabelWidth, rowLabel, line)))
		}
	}
	b.WriteString(border)
	return b.String()
}

func formatTerminalBoxLine(text string) string {
	contentWidth := terminalBoxWidth - 4
	if len(text) > contentWidth {
		text = text[:contentWidth]
	}
	return fmt.Sprintf("| %-*s |\n", contentWidth, text)
}

func wrapTerminalText(text string, width int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}
	lines := []string{}
	current := ""
	flushCurrent := func() {
		if current != "" {
			lines = append(lines, current)
			current = ""
		}
	}
	for _, word := range words {
		for len(word) > width {
			if current != "" {
				flushCurrent()
			}
			lines = append(lines, word[:width])
			word = word[width:]
		}
		if current == "" {
			current = word
			continue
		}
		if len(current)+1+len(word) <= width {
			current += " " + word
			continue
		}
		flushCurrent()
		current = word
	}
	flushCurrent()
	return lines
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
	return errors.New("usage: bpu-cli <serve|status|wallet|validate-tx|validate-block|chain|snapshot|config|logs>")
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
		return "events.jsonl"
	}
	return filepath.Join(dir, "events.jsonl")
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
