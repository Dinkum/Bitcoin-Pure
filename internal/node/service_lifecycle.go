package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"time"
)

func OpenService(cfg ServiceConfig, genesis *types.Block) (*Service, error) {
	nodeID := deriveNodeID(cfg)
	rootLogger := slog.Default().With("node", nodeID)
	logger := logging.ComponentWith(rootLogger, "service")
	headerLogger := logging.ComponentWith(rootLogger, "headers")
	if cfg.DBPath == "" {
		return nil, errors.New("db path is required")
	}
	if cfg.RPCAddr != "" && cfg.RPCAuthToken == "" && !isLoopbackAddr(cfg.RPCAddr) {
		return nil, errors.New("rpc auth token is required for non-loopback rpc binds")
	}
	if cfg.MinerEnabled && cfg.MinerPubKey == ([32]byte{}) {
		return nil, errors.New("miner pubkey is required when mining is enabled")
	}
	if cfg.MaxInboundPeers <= 0 {
		cfg.MaxInboundPeers = 32
	}
	if cfg.MaxOutboundPeers <= 0 {
		cfg.MaxOutboundPeers = 8
	}
	if cfg.MaxMessageBytes <= 0 {
		// Keep the transport ceiling comfortably above the 32 MB consensus floor.
		cfg.MaxMessageBytes = defaultMaxMessageBytes
	}
	if cfg.MaxTxSize <= 0 {
		cfg.MaxTxSize = 1_000_000
	}
	if cfg.MaxMempoolBytes <= 0 {
		cfg.MaxMempoolBytes = 64 << 20
	}
	if cfg.MaxAncestors <= 0 {
		cfg.MaxAncestors = 256
	}
	if cfg.MaxDescendants <= 0 {
		cfg.MaxDescendants = 256
	}
	if cfg.MaxOrphans <= 0 {
		cfg.MaxOrphans = 128
	}
	if cfg.AvalancheMode == "" {
		cfg.AvalancheMode = "on"
	}
	if cfg.AvalancheKSample <= 0 {
		cfg.AvalancheKSample = 16
	}
	if cfg.AvalancheAlphaNumerator <= 0 {
		cfg.AvalancheAlphaNumerator = 3
	}
	if cfg.AvalancheAlphaDenominator <= 0 {
		cfg.AvalancheAlphaDenominator = 4
	}
	if cfg.AvalancheBeta <= 0 {
		cfg.AvalancheBeta = 15
	}
	if cfg.AvalanchePollInterval <= 0 {
		cfg.AvalanchePollInterval = 200 * time.Millisecond
	}
	if cfg.MinerEnabled && cfg.MinerWorkers <= 0 {
		cfg.MinerWorkers = defaultMinerWorkers()
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 5 * time.Second
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = 15 * time.Second
	}
	if cfg.RPCMaxBodyBytes <= 0 {
		cfg.RPCMaxBodyBytes = 1 << 20
	}
	if cfg.RPCMaxHeaderBytes <= 0 {
		cfg.RPCMaxHeaderBytes = 8 << 10
	}
	if cfg.RPCReadTimeout <= 0 {
		cfg.RPCReadTimeout = 5 * time.Second
	}
	if cfg.RPCWriteTimeout <= 0 {
		cfg.RPCWriteTimeout = 15 * time.Minute
	}
	if cfg.RPCHeaderTimeout <= 0 {
		cfg.RPCHeaderTimeout = 2 * time.Second
	}
	if cfg.RPCIdleTimeout <= 0 {
		cfg.RPCIdleTimeout = 30 * time.Second
	}
	if cfg.PebbleCacheBytes <= 0 {
		cfg.PebbleCacheBytes = 2 << 30
	}
	if cfg.PebbleBloomBitsPerKey <= 0 {
		cfg.PebbleBloomBitsPerKey = 10
	}
	logger.Info("opening node service",
		slog.String("profile", cfg.Profile.String()),
		slog.String("db_path", cfg.DBPath),
		slog.String("rpc_addr", cfg.RPCAddr),
		slog.String("p2p_addr", cfg.P2PAddr),
	)
	var svc *Service
	rules := consensus.DefaultConsensusRules()
	rules.ValidatedTxCache = func(txid, authid [32]byte, params consensus.ChainParams) bool {
		if svc == nil {
			return false
		}
		return svc.hasValidTxAuth(txid, authid, params)
	}
	if cfg.SyntheticMining {
		rules.SkipPow = true
	}
	chainState, err := openPersistentChainStateFromMeta(
		filepath.Clean(cfg.DBPath),
		cfg.Profile,
		rules,
		rootLogger,
		storage.OpenOptions{
			PebbleCacheBytes:      cfg.PebbleCacheBytes,
			BloomFilterBitsPerKey: cfg.PebbleBloomBitsPerKey,
		},
	)
	if err != nil {
		return nil, err
	}
	if chainState.ChainState().TipHeight() == nil {
		if _, err := chainState.InitializeFromGenesisBlock(genesis); err != nil {
			chainState.Close()
			return nil, err
		}
	}

	storeHeader, err := chainState.Store().LoadHeaderState()
	if err != nil {
		chainState.Close()
		return nil, err
	}
	var headerChain *HeaderChain
	if storeHeader != nil {
		headerChain, err = HeaderChainFromStoredState(storeHeader)
		if err != nil {
			chainState.Close()
			return nil, err
		}
		headerChain.WithLogger(headerLogger)
	} else {
		headerChain = NewHeaderChainWithLogger(cfg.Profile, headerLogger)
		if err := headerChain.InitializeFromGenesisHeader(genesis.Header); err != nil {
			chainState.Close()
			return nil, err
		}
		stored, err := headerChain.StoredState()
		if err != nil {
			chainState.Close()
			return nil, err
		}
		if err := chainState.Store().WriteHeaderState(stored); err != nil {
			chainState.Close()
			return nil, err
		}
		if err := chainState.Store().PutHeader(0, &genesis.Header); err != nil {
			chainState.Close()
			return nil, err
		}
	}
	headerChain.SetSkipPow(cfg.SyntheticMining)

	svc = &Service{
		cfg:         cfg,
		logger:      logger,
		chainState:  chainState,
		headerChain: headerChain,
		pool: mempool.NewWithConfig(mempool.PoolConfig{
			MinRelayFeePerByte: cfg.MinRelayFeePerByte,
			MaxTxSize:          cfg.MaxTxSize,
			MaxMempoolBytes:    cfg.MaxMempoolBytes,
			MaxAncestors:       cfg.MaxAncestors,
			MaxDescendants:     cfg.MaxDescendants,
			MaxOrphans:         cfg.MaxOrphans,
		}),
		genesis:             genesis,
		peers:               make(map[string]*peerConn),
		outboundPeers:       make(map[string]struct{}),
		knownPeers:          make(map[string]storage.KnownPeerRecord),
		bannedPeers:         make(map[string]time.Time),
		blockRequests:       make(map[[32]byte]blockDownloadRequest),
		txRequests:          make(map[[32]byte]blockDownloadRequest),
		pendingBlocks:       make(map[[32]byte]pendingPeerBlock),
		pendingBlocksByPeer: make(map[string]int),
		rejectedBlocks:      make(map[[32]byte]struct{}),
		pendingChildren:     make(map[[32]byte]map[[32]byte]struct{}),
		localRebroadcast:    make(map[[32]byte]time.Time),
		stressPending:       make(map[[32]byte]stressLaneBatch),
		recentHdrs:          recentHeaderCache{items: make(map[[32]byte]types.BlockHeader)},
		recentBlks:          recentBlockCache{items: make(map[[32]byte]types.Block)},
		rejectCache:         newTxRejectCache(txRejectCacheCapacity, txRejectCachePermanentTTL, txRejectCacheTemporaryTTL),
		validAuth:           newValidAuthCache(validAuthCacheCapacity),
		mempoolPersistCh:    make(chan struct{}, 1),
		startedAt:           time.Now(),
		nodeID:              nodeID,
		publicPage:          shouldServePublicDashboard(),
		stopCh:              make(chan struct{}),
		mineHeaderFn:        consensus.MineHeaderInterruptible,
	}
	svc.peerMgr = &peerManager{svc: svc}
	svc.syncMgr = &syncManager{svc: svc}
	svc.relaySched = &relayScheduler{svc: svc}
	svc.minerMgr = &minerManager{svc: svc}
	svc.avaMgr = newAvalancheManager(svc)
	if !cfg.StaticPeerTopology {
		svc.peerManager().rememberConfiguredPeers(cfg.Peers)
		if persistedPeers, err := chainState.Store().LoadKnownPeers(); err != nil {
			chainState.Close()
			return nil, err
		} else {
			svc.loadPersistedKnownPeers(persistedPeers)
		}
	}
	if genesis != nil {
		svc.cacheRecentHeader(genesis.Header)
		svc.cacheRecentBlock(*genesis)
	}
	if err := svc.reloadPersistedMempool(); err != nil {
		chainState.Close()
		return nil, err
	}
	svc.avalancheManager().logConfig()
	return svc, nil
}

func (s *Service) Close() error {
	s.closeOnce.Do(func() {
		s.stopOnce.Do(func() {
			s.logger.Info("shutting down node service")
			close(s.stopCh)
		})
		if s.listener != nil {
			_ = s.listener.Close()
		}
		if s.rpcSrv != nil {
			_ = s.rpcSrv.Close()
		}
		s.peerMu.Lock()
		for _, peer := range s.peers {
			peer.close()
			if peer.wire != nil {
				_ = peer.wire.Close()
			}
		}
		s.peerMu.Unlock()
		s.wg.Wait()
		if err := s.flushMempoolPersistence(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.logger.Info("node service stopped")
		if err := s.chainState.Close(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *Service) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.logger.Info("starting node service")
	started := false
	defer func() {
		if !started {
			_ = s.Close()
		}
	}()
	if s.cfg.RPCAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/", s.handleHTTP)
		var listenConfig net.ListenConfig
		rpcListener, err := listenConfig.Listen(ctx, "tcp", s.cfg.RPCAddr)
		if err != nil {
			return err
		}
		s.rpcSrv = &http.Server{
			Addr:              rpcListener.Addr().String(),
			Handler:           mux,
			ReadTimeout:       s.cfg.RPCReadTimeout,
			ReadHeaderTimeout: s.cfg.RPCHeaderTimeout,
			WriteTimeout:      s.cfg.RPCWriteTimeout,
			IdleTimeout:       s.cfg.RPCIdleTimeout,
			MaxHeaderBytes:    s.cfg.RPCMaxHeaderBytes,
		}
		s.logger.Info("rpc server enabled", slog.String("addr", s.rpcSrv.Addr))
		if s.publicPage {
			s.logger.Info("public ascii dashboard enabled", slog.String("addr", s.rpcSrv.Addr), slog.Duration("cache_ttl", time.Minute))
		}
		s.safeGo("rpc-server", func() {
			if err := s.rpcSrv.Serve(rpcListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Error("rpc server failed", slog.String("addr", s.rpcSrv.Addr), slog.Any("error", err))
			}
		})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.cfg.P2PAddr != "" {
		var listenConfig net.ListenConfig
		ln, err := listenConfig.Listen(ctx, "tcp", s.cfg.P2PAddr)
		if err != nil {
			return err
		}
		s.listener = ln
		s.logger.Info("p2p listener enabled", slog.String("addr", s.cfg.P2PAddr))
		s.safeGo("accept-loop", func() {
			s.acceptLoop()
		})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, addr := range s.cfg.Peers {
		if addr == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ConnectPeer(addr); err != nil {
			s.logger.Warn("peer dial failed", slog.String("addr", addr), slog.Any("error", err))
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.publicPage {
		if err := s.recordDashboardSystemSample(); err != nil {
			s.logger.Debug("dashboard system sampler warmup failed", slog.Any("error", err))
		}
		s.safeGo("dashboard-system-loop", func() {
			s.dashboardSystemLoop()
		})
	}
	if s.cfg.ThroughputSummaryInterval > 0 {
		s.safeGo("throughput-summary-loop", func() {
			s.throughputSummaryLoop()
		})
	}
	s.safeGo("mempool-persist-loop", func() {
		s.mempoolPersistLoop()
	})
	s.startFastSyncHistoricalVerification()
	s.safeGo("node-status-loop", func() {
		s.nodeStatusLoop()
	})
	started = true
	if s.cfg.P2PAddr != "" || len(s.cfg.Peers) > 0 {
		s.safeGo("sync-watchdog-loop", func() {
			s.syncWatchdogLoop()
		})
		s.safeGo("outbound-refill-loop", func() {
			s.outboundRefillLoop()
		})
		s.safeGo("local-rebroadcast-loop", func() {
			s.localRebroadcastLoop()
		})
		s.safeGo("erlay-reconcile-loop", func() {
			s.erlayReconcileLoop()
		})
		if s.avalancheManager().enabled() {
			s.safeGo("avalanche-poll-loop", func() {
				s.avalancheManager().pollLoop()
			})
		}
	}
	if s.cfg.MinerEnabled {
		s.logger.Info("continuous miner enabled", slog.Int("workers", s.cfg.MinerWorkers))
		for workerID := 0; workerID < s.cfg.MinerWorkers; workerID++ {
			workerID := workerID
			s.safeGo(fmt.Sprintf("miner-loop-%d", workerID), func() {
				s.minerManager().minerLoop(workerID)
			})
		}
	}
	select {
	case <-ctx.Done():
		s.logger.Info("shutdown signal received")
		return s.Close()
	case <-s.stopCh:
		return s.Close()
	}
}

func (s *Service) startFastSyncHistoricalVerification() {
	fastSyncState, err := s.chainState.Store().LoadFastSyncState()
	if err != nil {
		s.logger.Warn("failed to inspect fast-sync snapshot state", slog.Any("error", err))
		return
	}
	if fastSyncState == nil {
		return
	}
	// Historical replay runs outside the live chainstate lock and only touches
	// the retained imported snapshot plus historical block records. The active
	// node can keep extending above the imported height while this catches up.
	s.logger.Info("starting background historical snapshot verification",
		slog.Uint64("height", fastSyncState.SnapshotHeight),
		slog.String("header_hash", fmt.Sprintf("%x", fastSyncState.SnapshotHeaderHash)),
	)
	s.safeGo("snapshot-history-verify", func() {
		logger := logging.ComponentWith(s.logger, "snapshot")
		if _, err := VerifyFastSyncSnapshotFromStore(s.chainState.Store(), s.cfg.Profile, s.genesis, logger); err != nil {
			logger.Warn("background historical snapshot verification failed", slog.Any("error", err))
		}
	})
}

func (s *Service) peerManager() *peerManager {
	if s.peerMgr == nil {
		s.peerMgr = &peerManager{svc: s}
	}
	return s.peerMgr
}

func (s *Service) syncManager() *syncManager {
	if s.syncMgr == nil {
		s.syncMgr = &syncManager{svc: s}
	}
	return s.syncMgr
}

func (s *Service) relayManager() *relayScheduler {
	if s.relaySched == nil {
		s.relaySched = &relayScheduler{svc: s}
	}
	return s.relaySched
}

func (s *Service) avalancheManager() *avalancheManager {
	if s.avaMgr == nil {
		s.avaMgr = newAvalancheManager(s)
	}
	return s.avaMgr
}

func (s *Service) minerManager() *minerManager {
	if s.minerMgr == nil {
		s.minerMgr = &minerManager{svc: s}
	}
	return s.minerMgr
}

func (s *Service) acceptLoop()                      { s.peerManager().acceptLoop() }
func (s *Service) canAcceptInboundPeer() bool       { return s.peerManager().canAcceptInboundPeer() }
func (s *Service) ConnectPeer(addr string) error    { return s.peerManager().ConnectPeer(addr) }
func (s *Service) outboundPeerCount() int           { return s.peerManager().outboundPeerCount() }
func (s *Service) maintainOutboundPeer(addr string) { s.peerManager().maintainOutboundPeer(addr) }
func (s *Service) handlePeer(conn net.Conn, outbound bool, targetAddr string) {
	s.peerManager().handlePeer(conn, outbound, targetAddr, nil)
}
func (s *Service) peerPingLoop(peer *peerConn)  { s.peerManager().peerPingLoop(peer) }
func (s *Service) peerWriteLoop(peer *peerConn) { s.peerManager().peerWriteLoop(peer) }
