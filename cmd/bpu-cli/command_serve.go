package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	netpprof "net/http/pprof"
	"net/netip"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
)

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
