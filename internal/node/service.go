package node

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"math"
	"math/big"
	mrand "math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

type ServiceConfig struct {
	Profile            types.ChainProfile
	DBPath             string
	RPCAddr            string
	RPCAuthToken       string
	RPCReadTimeout     time.Duration
	RPCWriteTimeout    time.Duration
	RPCHeaderTimeout   time.Duration
	RPCIdleTimeout     time.Duration
	RPCMaxHeaderBytes  int
	RPCMaxBodyBytes    int
	P2PAddr            string
	Peers              []string
	MaxInboundPeers    int
	MaxOutboundPeers   int
	HandshakeTimeout   time.Duration
	StallTimeout       time.Duration
	MaxMessageBytes    int
	MinRelayFeePerByte uint64
	MaxTxSize          int
	MaxAncestors       int
	MaxDescendants     int
	MaxOrphans         int
	MinerEnabled       bool
	MinerWorkers       int
	MinerKeyHash       [32]byte
	GenesisFixture     string
}

type ServiceInfo struct {
	Profile        string   `json:"profile"`
	TipHeight      uint64   `json:"tip_height"`
	HeaderHeight   uint64   `json:"header_height"`
	TipHeaderHash  string   `json:"tip_header_hash"`
	UTXORoot       string   `json:"utxo_root"`
	MempoolSize    int      `json:"mempool_size"`
	RPCAddr        string   `json:"rpc_addr"`
	P2PAddr        string   `json:"p2p_addr"`
	Peers          []string `json:"peers"`
	MinerEnabled   bool     `json:"miner_enabled"`
	MinerWorkers   int      `json:"miner_workers"`
	GenesisFixture string   `json:"genesis_fixture"`
}

type ChainStateInfo struct {
	Profile            string `json:"profile"`
	TipHeight          uint64 `json:"tip_height"`
	HeaderHeight       uint64 `json:"header_height"`
	TipHeaderHash      string `json:"tip_header_hash"`
	UTXORoot           string `json:"utxo_root"`
	UTXOCount          int    `json:"utxo_count"`
	NextBlockSizeLimit uint64 `json:"next_block_size_limit"`
	TipTimestamp       uint64 `json:"tip_timestamp"`
	ChainWork          string `json:"chainwork,omitempty"`
}

type MempoolInfo struct {
	Count              int    `json:"count"`
	Orphans            int    `json:"orphans"`
	Bytes              int    `json:"bytes"`
	TotalFees          uint64 `json:"total_fees"`
	MedianFee          uint64 `json:"median_fee"`
	LowFee             uint64 `json:"low_fee"`
	HighFee            uint64 `json:"high_fee"`
	MinRelayFeePerByte uint64 `json:"min_relay_fee_per_byte"`
	CandidateFrontier  int    `json:"candidate_frontier"`
}

type MiningInfo struct {
	Enabled           bool               `json:"enabled"`
	Workers           int                `json:"workers"`
	MinerKeyHash      string             `json:"miner_keyhash,omitempty"`
	CurrentBits       uint32             `json:"current_bits"`
	NextBits          uint32             `json:"next_bits"`
	Difficulty        float64            `json:"difficulty"`
	TargetSpacingSecs uint64             `json:"target_spacing_secs"`
	Template          BlockTemplateStats `json:"template"`
}

type KeyHashUTXO struct {
	OutPoint types.OutPoint
	Value    uint64
	KeyHash  [32]byte
}

type WalletActivity struct {
	TxID      [32]byte  `json:"-"`
	BlockHash [32]byte  `json:"-"`
	Height    uint64    `json:"height"`
	Timestamp time.Time `json:"timestamp"`
	Coinbase  bool      `json:"coinbase"`
	Received  uint64    `json:"received"`
	Sent      uint64    `json:"sent"`
	Fee       uint64    `json:"fee"`
	Net       int64     `json:"net"`
}

type PeerRelayStats struct {
	Addr          string  `json:"addr"`
	Outbound      bool    `json:"outbound"`
	QueueDepth    int     `json:"queue_depth"`
	MaxQueueDepth int     `json:"max_queue_depth"`
	SentMessages  int     `json:"sent_messages"`
	TxInvItems    int     `json:"tx_inv_items"`
	BlockInvItems int     `json:"block_inv_items"`
	TxBatchMsgs   int     `json:"tx_batch_messages"`
	TxBatchItems  int     `json:"tx_batch_items"`
	TxReconMsgs   int     `json:"tx_recon_messages"`
	TxReconItems  int     `json:"tx_recon_items"`
	TxReqMsgs     int     `json:"tx_request_messages"`
	TxReqItems    int     `json:"tx_request_items"`
	DroppedInv    int     `json:"dropped_inv_items"`
	DroppedTxs    int     `json:"dropped_tx_items"`
	RelayEvents   int     `json:"relay_events"`
	RelayAvgMS    float64 `json:"relay_avg_ms,omitempty"`
	RelayP95MS    float64 `json:"relay_p95_ms,omitempty"`
	RelayMaxMS    float64 `json:"relay_max_ms,omitempty"`
}

type BlockTemplateStats struct {
	CacheHits          int    `json:"cache_hits"`
	Rebuilds           int    `json:"rebuilds"`
	FrontierCandidates int    `json:"frontier_candidates"`
	Invalidations      int    `json:"invalidations"`
	Interruptions      int    `json:"interruptions"`
	LastBuildAgeMS     int    `json:"last_build_age_ms"`
	LastReason         string `json:"last_reason,omitempty"`
}

type Service struct {
	cfg         ServiceConfig
	logger      *slog.Logger
	chainState  *PersistentChainState
	headerChain *HeaderChain
	pool        *mempool.Pool
	genesis     *types.Block

	stateMu            sync.RWMutex
	templateMu         sync.Mutex
	peerMu             sync.RWMutex
	downloadMu         sync.Mutex
	recentMu           sync.RWMutex
	peers              map[string]*peerConn
	outboundPeers      map[string]struct{}
	knownPeers         map[string]time.Time
	vettedPeers        map[string]time.Time
	blockRequests      map[[32]byte]blockDownloadRequest
	txRequests         map[[32]byte]blockDownloadRequest
	templateGeneration uint64
	template           *blockTemplateCache
	recentHdrs         recentHeaderCache
	recentBlks         recentBlockCache
	templateStats      templateBuildTelemetry
	peerMgr            *peerManager
	syncMgr            *syncManager
	relaySched         *relayScheduler
	mineHeaderFn       func(types.BlockHeader, consensus.ChainParams, func(uint32) bool) (types.BlockHeader, bool, error)
	nodeID             string
	dashboard          dashboardCache
	systemStats        dashboardSystemStats
	startedAt          time.Time
	publicPage         bool
	listener           net.Listener
	rpcSrv             *http.Server
	stopCh             chan struct{}
	stopOnce           sync.Once
	closeOnce          sync.Once
	closeErr           error
	wg                 sync.WaitGroup
}

type peerConn struct {
	addr            string
	targetAddr      string
	outbound        bool
	wire            *p2p.Conn
	version         p2p.VersionMessage
	lastProgress    atomic.Int64
	bestHeight      atomic.Uint64
	controlQ        chan outboundMessage
	relayPriorityQ  chan outboundMessage
	sendQ           chan outboundMessage
	closed          chan struct{}
	closeOnce       sync.Once
	invMu           sync.Mutex
	queuedInv       map[p2p.InvVector]int
	txMu            sync.Mutex
	queuedTx        map[[32]byte]int
	knownTx         map[[32]byte]struct{}
	knownTxOrder    [][32]byte
	pendingTxs      []types.Transaction
	txFlushArmed    bool
	pendingRecon    [][32]byte
	reconFlushArmed bool
	thinMu          sync.Mutex
	pendingThin     map[[32]byte]*pendingThinBlock
	telemetry       peerRelayTelemetry
	syncMu          sync.Mutex
	syncState       peerSyncState
}

type peerSyncState struct {
	headersRequestedAt time.Time
	lastUsefulAt       time.Time
	cooldownUntil      time.Time
	usefulHeaders      int
	usefulBlocks       int
	usefulTxs          int
	headerStalls       int
	blockStalls        int
	txStalls           int
}

type outboundMessage struct {
	msg        p2p.Message
	enqueuedAt time.Time
	class      relayMessageClass
	invItems   []p2p.InvVector
}

type relayMessageClass struct {
	txInvItems    int
	blockInvItems int
	txBatchMsgs   int
	txBatchItems  int
	txReconMsgs   int
	txReconItems  int
	txReqMsgs     int
	txReqItems    int
}

type peerRelayTelemetry struct {
	mu            sync.Mutex
	maxQueueDepth int
	sentMessages  int
	txInvItems    int
	blockInvItems int
	txBatchMsgs   int
	txBatchItems  int
	txReconMsgs   int
	txReconItems  int
	txReqMsgs     int
	txReqItems    int
	droppedInv    int
	droppedTxs    int
	relaySamples  []float64
}

type blockTemplateCache struct {
	tipHash       [32]byte
	mempoolEpoch  uint64
	generation    uint64
	builtAt       time.Time
	block         types.Block
	selected      []mempool.SnapshotEntry
	totalFees     uint64
	usedTxBytes   int
	baseUtxos     consensus.UtxoSet
	selectionView *consensus.UtxoOverlay
	baseAcc       *utreexo.Accumulator
	selectionAcc  *utreexo.Accumulator
}

type blockDownloadRequest struct {
	peerAddr    string
	requestedAt time.Time
	attempts    int
}

type templateBuildTelemetry struct {
	mu                 sync.Mutex
	cacheHits          int
	rebuilds           int
	frontierCandidates int
	invalidations      int
	interruptions      int
	lastBuildAt        time.Time
	lastReason         string
}

type chainSelectionSnapshot struct {
	tipHash        [32]byte
	height         uint64
	tipHeader      types.BlockHeader
	blockSizeState consensus.BlockSizeState
	utxos          consensus.UtxoSet
	utxoAcc        *utreexo.Accumulator
}

type chainTemplateContext struct {
	tipHash        [32]byte
	height         uint64
	tipHeader      types.BlockHeader
	blockSizeState consensus.BlockSizeState
}

type recentHeaderCache struct {
	order [][32]byte
	items map[[32]byte]types.BlockHeader
}

type recentBlockCache struct {
	order [][32]byte
	items map[[32]byte]types.Block
}

type dashboardCache struct {
	mu         sync.Mutex
	renderedAt time.Time
	view       *publicDashboardView
	pages      map[string]string
}

type dashboardBlockWindow struct {
	recent []dashboardBlockPage
	chart  []dashboardBlockPage
}

type publicDashboardView struct {
	generatedAt time.Time
	nodeID      string
	createdAt   time.Time
	info        ServiceInfo
	health      string
	system      dashboardSystemSummary
	blocks      []dashboardBlockPage
	peers       []PeerInfo
	relay       []PeerRelayStats
	template    BlockTemplateStats
	pow         dashboardPowSummary
	fees        dashboardFeeSummary
	mining      dashboardMiningSummary
	mempool     dashboardMempoolSummary
	tpsChart    dashboardTPSChart
}

type dashboardBlockPage struct {
	Height        uint64
	Hash          [32]byte
	PrevHash      [32]byte
	Timestamp     time.Time
	NBits         uint32
	TxRoot        [32]byte
	AuthRoot      [32]byte
	UTXORoot      [32]byte
	Size          int
	TxCount       int
	TotalFees     uint64
	MedianFee     uint64
	LowFee        uint64
	HighFee       uint64
	MinedByNode   bool
	PreviewTxs    []dashboardTxPage
	HiddenTxCount int
}

type dashboardTxPage struct {
	BlockHeight uint64
	BlockHash   [32]byte
	Timestamp   time.Time
	TxID        [32]byte
	Coinbase    bool
	Size        int
	Fee         uint64
	FeeRate     uint64
	InputSum    uint64
	OutputSum   uint64
	AuthCount   int
	Inputs      []dashboardTxInput
	Outputs     []dashboardTxOutput
}

type dashboardTxInput struct {
	PrevOut types.OutPoint
	Amount  uint64
}

type dashboardTxOutput struct {
	Index   int
	Amount  uint64
	KeyHash [32]byte
}

type dashboardPowSummary struct {
	Algorithm          string
	TargetSpacing      time.Duration
	CurrentBits        uint32
	NextBits           uint32
	Difficulty         float64
	AvgBlockInterval   time.Duration
	HasObservedGap     bool
	LastBlockTimestamp time.Time
}

type dashboardFeeSummary struct {
	Recent []dashboardBlockFeeLine
	Clear  dashboardMempoolClearEstimate
}

type dashboardBlockFeeLine struct {
	Height    uint64
	MedianFee uint64
	LowFee    uint64
	HighFee   uint64
}

type dashboardMempoolClearEstimate struct {
	Blocks int
	Time   time.Duration
}

type dashboardMiningSummary struct {
	Enabled       bool
	Workers       int
	RecentHeights []uint64
	RecentHashes  [][32]byte
}

type dashboardMempoolSummary struct {
	Count         int
	Orphans       int
	Top           []mempool.SnapshotEntry
	MedianFee     uint64
	LowFee        uint64
	HighFee       uint64
	EstimatedNext time.Duration
}

type dashboardTPSChart struct {
	Label     string
	Buckets   []float64
	BucketEnd []time.Time
	MaxTPS    float64
}

type dashboardSystemStats struct {
	mu      sync.Mutex
	samples []dashboardSystemSample
}

type dashboardSystemSample struct {
	takenAt       time.Time
	cpuBusyTicks  uint64
	cpuTotalTicks uint64
	rxBytes       uint64
	txBytes       uint64
	memUsedBytes  uint64
	memTotalBytes uint64
	load1         float64
	load5         float64
	load15        float64
	runningProcs  int
	totalProcs    int
	cores         int
}

type dashboardSystemSummary struct {
	Window          time.Duration
	CPUPercent      float64
	HasCPU          bool
	RxBytesPerSec   float64
	TxBytesPerSec   float64
	HasNetwork      bool
	AvgMemUsedBytes uint64
	MemTotalBytes   uint64
	HasMemory       bool
	Load1           float64
	Load5           float64
	Load15          float64
	HasLoad         bool
	RunningProcs    int
	TotalProcs      int
	Cores           int
}

type FundingOutput struct {
	OutPoint  types.OutPoint
	Value     uint64
	KeyHash   [32]byte
	BlockHash [32]byte
}

const (
	dashboardSystemSampleInterval = 10 * time.Second
	dashboardSystemMinimumWindow  = 10 * time.Second
	dashboardSystemWindow         = 10 * time.Minute
	dashboardSystemRetention      = 12 * time.Minute
	syncWatchdogInterval          = 10 * time.Second
	outboundRefillInterval        = 5 * time.Second
	controlMessageEnqueueTimeout  = 100 * time.Millisecond
	blockRequestBatchSize         = 128
	maxKnownPeerAddrs             = 256
	defaultMaxMessageBytes        = 64_000_000
)

type pendingThinBlock struct {
	hash   [32]byte
	header types.BlockHeader
	txs    []types.Transaction
	filled []bool
}

type rpcRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcResponse struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func OpenService(cfg ServiceConfig, genesis *types.Block) (*Service, error) {
	logger := logging.Component("service")
	if cfg.DBPath == "" {
		return nil, errors.New("db path is required")
	}
	if cfg.RPCAddr != "" && cfg.RPCAuthToken == "" && !isLoopbackAddr(cfg.RPCAddr) {
		return nil, errors.New("rpc auth token is required for non-loopback rpc binds")
	}
	if cfg.MinerEnabled && cfg.MinerKeyHash == ([32]byte{}) {
		return nil, errors.New("miner keyhash is required when mining is enabled")
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
	if cfg.MaxAncestors <= 0 {
		cfg.MaxAncestors = 256
	}
	if cfg.MaxDescendants <= 0 {
		cfg.MaxDescendants = 256
	}
	if cfg.MaxOrphans <= 0 {
		cfg.MaxOrphans = 128
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
	if cfg.RPCIdleTimeout <= 0 {
		cfg.RPCIdleTimeout = 30 * time.Second
	}
	logger.Info("opening node service",
		slog.String("profile", cfg.Profile.String()),
		slog.String("db_path", cfg.DBPath),
		slog.String("rpc_addr", cfg.RPCAddr),
		slog.String("p2p_addr", cfg.P2PAddr),
	)
	chainState, err := OpenPersistentChainState(filepath.Clean(cfg.DBPath), cfg.Profile)
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
	} else {
		headerChain = NewHeaderChain(cfg.Profile)
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

	svc := &Service{
		cfg:         cfg,
		logger:      logger,
		chainState:  chainState,
		headerChain: headerChain,
		pool: mempool.NewWithConfig(mempool.PoolConfig{
			MinRelayFeePerByte: cfg.MinRelayFeePerByte,
			MaxTxSize:          cfg.MaxTxSize,
			MaxAncestors:       cfg.MaxAncestors,
			MaxDescendants:     cfg.MaxDescendants,
			MaxOrphans:         cfg.MaxOrphans,
		}),
		genesis:       genesis,
		peers:         make(map[string]*peerConn),
		outboundPeers: make(map[string]struct{}),
		knownPeers:    make(map[string]time.Time),
		vettedPeers:   make(map[string]time.Time),
		blockRequests: make(map[[32]byte]blockDownloadRequest),
		txRequests:    make(map[[32]byte]blockDownloadRequest),
		recentHdrs:    recentHeaderCache{items: make(map[[32]byte]types.BlockHeader)},
		recentBlks:    recentBlockCache{items: make(map[[32]byte]types.Block)},
		startedAt:     time.Now(),
		nodeID:        deriveNodeID(cfg),
		publicPage:    shouldServePublicDashboard(),
		stopCh:        make(chan struct{}),
		mineHeaderFn:  consensus.MineHeaderInterruptible,
	}
	svc.peerMgr = &peerManager{svc: svc}
	svc.syncMgr = &syncManager{svc: svc}
	svc.relaySched = &relayScheduler{svc: svc}
	svc.rememberKnownPeers(cfg.Peers)
	if persistedPeers, err := chainState.Store().LoadKnownPeers(); err != nil {
		chainState.Close()
		return nil, err
	} else {
		svc.loadPersistedKnownPeers(persistedPeers)
	}
	if genesis != nil {
		svc.cacheRecentHeader(genesis.Header)
		svc.cacheRecentBlock(*genesis)
	}
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
		s.logger.Info("node service stopped")
		s.closeErr = s.chainState.Close()
	})
	return s.closeErr
}

func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("starting node service")
	if s.cfg.RPCAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/", s.handleHTTP)
		s.rpcSrv = &http.Server{
			Addr:              s.cfg.RPCAddr,
			Handler:           mux,
			ReadTimeout:       s.cfg.RPCReadTimeout,
			ReadHeaderTimeout: s.cfg.RPCHeaderTimeout,
			WriteTimeout:      s.cfg.RPCWriteTimeout,
			IdleTimeout:       s.cfg.RPCIdleTimeout,
			MaxHeaderBytes:    s.cfg.RPCMaxHeaderBytes,
		}
		s.logger.Info("rpc server enabled", slog.String("addr", s.cfg.RPCAddr))
		if s.publicPage {
			s.logger.Info("public ascii dashboard enabled", slog.String("addr", s.cfg.RPCAddr), slog.Duration("cache_ttl", time.Minute))
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.rpcSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Error("rpc server failed", slog.String("addr", s.cfg.RPCAddr), slog.Any("error", err))
			}
		}()
	}
	if s.cfg.P2PAddr != "" {
		ln, err := net.Listen("tcp", s.cfg.P2PAddr)
		if err != nil {
			return err
		}
		s.listener = ln
		s.logger.Info("p2p listener enabled", slog.String("addr", s.cfg.P2PAddr))
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.acceptLoop()
		}()
	}
	for _, addr := range s.cfg.Peers {
		if addr == "" {
			continue
		}
		if err := s.ConnectPeer(addr); err != nil {
			s.logger.Warn("peer dial failed", slog.String("addr", addr), slog.Any("error", err))
		}
	}
	if s.publicPage {
		if err := s.recordDashboardSystemSample(); err != nil {
			s.logger.Debug("dashboard system sampler warmup failed", slog.Any("error", err))
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.dashboardSystemLoop()
		}()
	}
	if s.cfg.P2PAddr != "" || len(s.cfg.Peers) > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.syncWatchdogLoop()
		}()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.outboundRefillLoop()
		}()
	}
	if s.cfg.MinerEnabled {
		s.logger.Info("continuous miner enabled", slog.Int("workers", s.cfg.MinerWorkers))
		for workerID := 0; workerID < s.cfg.MinerWorkers; workerID++ {
			s.wg.Add(1)
			go func(workerID int) {
				defer s.wg.Done()
				s.minerLoop(workerID)
			}(workerID)
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

func (s *Service) acceptLoop()                      { s.peerManager().acceptLoop() }
func (s *Service) canAcceptInboundPeer() bool       { return s.peerManager().canAcceptInboundPeer() }
func (s *Service) ConnectPeer(addr string) error    { return s.peerManager().ConnectPeer(addr) }
func (s *Service) outboundPeerCount() int           { return s.peerManager().outboundPeerCount() }
func (s *Service) maintainOutboundPeer(addr string) { s.peerManager().maintainOutboundPeer(addr) }
func (s *Service) handlePeer(conn net.Conn, outbound bool, targetAddr string) {
	s.peerManager().handlePeer(conn, outbound, targetAddr)
}
func (s *Service) peerPingLoop(peer *peerConn)  { s.peerManager().peerPingLoop(peer) }
func (s *Service) peerWriteLoop(peer *peerConn) { s.peerManager().peerWriteLoop(peer) }

func (p *peerConn) send(msg p2p.Message) error {
	if inv, ok := msg.(p2p.InvMessage); ok {
		return p.enqueueInv(inv)
	}
	if recon, ok := msg.(p2p.TxReconMessage); ok {
		return p.enqueueTxRecon(recon)
	}
	if batch, ok := msg.(p2p.TxBatchMessage); ok {
		return p.enqueueTxBatch(batch)
	}
	envelope := outboundMessage{
		msg:        msg,
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(msg),
	}
	return p.enqueueDirectMessage(envelope)
}

func (p *peerConn) enqueueDirectMessage(envelope outboundMessage) error {
	timer := time.NewTimer(controlMessageEnqueueTimeout)
	defer timer.Stop()
	q := p.sendQ
	if p.controlQ != nil {
		q = p.controlQ
	}
	select {
	case <-p.closed:
		return io.EOF
	case q <- envelope:
		p.telemetry.noteEnqueue(p.queueDepth())
		return nil
	case <-timer.C:
		return errors.New("peer send queue saturated")
	}
}

func (p *peerConn) enqueueTxRecon(msg p2p.TxReconMessage) error {
	filtered := p.filterQueuedTxIDs(msg.TxIDs)
	if len(filtered) == 0 {
		return nil
	}
	immediate, armFlush := p.stagePendingRecon(filtered)
	for _, batch := range immediate {
		p.enqueueRelayRecon(batch)
	}
	if armFlush {
		go p.flushPendingReconAfterDelay()
	}
	return nil
}

func (p *peerConn) enqueueInv(msg p2p.InvMessage) error {
	filtered := p.filterQueuedInv(msg.Items)
	if len(filtered) == 0 {
		return nil
	}
	priorityItems, normalItems := splitPrioritizedInvItems(filtered)
	if err := p.enqueueInvItems(priorityItems, true); err != nil {
		return err
	}
	return p.enqueueInvItems(normalItems, false)
}

func (p *peerConn) enqueueInvItems(items []p2p.InvVector, priority bool) error {
	if len(items) == 0 {
		return nil
	}
	envelope := outboundMessage{
		msg:        p2p.InvMessage{Items: items},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.InvMessage{Items: items}),
		invItems:   items,
	}
	q := p.sendQ
	if priority && p.relayPriorityQ != nil {
		q = p.relayPriorityQ
	}
	select {
	case <-p.closed:
		p.releaseQueuedInv(items)
		return io.EOF
	case q <- envelope:
		p.telemetry.noteEnqueue(p.queueDepth())
		return nil
	default:
		p.releaseQueuedInv(items)
		p.telemetry.noteDroppedInv(len(items))
		return nil
	}
}

func (p *peerConn) enqueueTxBatch(msg p2p.TxBatchMessage) error {
	filtered := p.filterQueuedTxs(msg.Txs)
	if len(filtered) == 0 {
		return nil
	}
	immediate, armFlush := p.stagePendingTxs(filtered)
	for _, batch := range immediate {
		p.enqueueRelayTxs(batch)
	}
	if armFlush {
		go p.flushPendingTxsAfterDelay()
	}
	return nil
}

func (p *peerConn) sendRequestedTxBatch(msg p2p.TxBatchMessage) error {
	if len(msg.Txs) == 0 {
		return nil
	}
	envelope := outboundMessage{
		msg:        msg,
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(msg),
	}
	return p.enqueueDirectMessage(envelope)
}

func (p *peerConn) close() {
	p.closeOnce.Do(func() {
		close(p.closed)
		p.drainControlQueue()
		p.drainPriorityRelayQueue()
		p.drainSendQueue()
		p.clearRelayState()
	})
}

func (p *peerConn) drainControlQueue() {
	if p.controlQ == nil {
		return
	}
	for {
		select {
		case envelope := <-p.controlQ:
			p.releaseQueuedInv(envelope.invItems)
			p.releaseRelayBatch(envelope.msg)
		default:
			return
		}
	}
}

func (p *peerConn) drainPriorityRelayQueue() {
	if p.relayPriorityQ == nil {
		return
	}
	for {
		select {
		case envelope := <-p.relayPriorityQ:
			p.releaseQueuedInv(envelope.invItems)
			p.releaseRelayBatch(envelope.msg)
		default:
			return
		}
	}
}

func (p *peerConn) drainSendQueue() {
	for {
		select {
		case envelope := <-p.sendQ:
			p.releaseQueuedInv(envelope.invItems)
			p.releaseRelayBatch(envelope.msg)
		default:
			return
		}
	}
}

func (p *peerConn) queueDepth() int {
	depth := len(p.sendQ)
	if p.relayPriorityQ != nil {
		depth += len(p.relayPriorityQ)
	}
	if p.controlQ != nil {
		depth += len(p.controlQ)
	}
	return depth
}

func (p *peerConn) clearRelayState() {
	p.invMu.Lock()
	p.queuedInv = make(map[p2p.InvVector]int)
	p.invMu.Unlock()

	p.txMu.Lock()
	p.queuedTx = make(map[[32]byte]int)
	p.knownTx = make(map[[32]byte]struct{})
	p.knownTxOrder = nil
	p.pendingTxs = nil
	p.pendingRecon = nil
	p.txFlushArmed = false
	p.reconFlushArmed = false
	p.txMu.Unlock()

	p.thinMu.Lock()
	p.pendingThin = make(map[[32]byte]*pendingThinBlock)
	p.thinMu.Unlock()
}

func (s *Service) writePeerEnvelope(peer *peerConn, envelope outboundMessage) bool {
	if s.cfg.StallTimeout > 0 {
		_ = peer.wire.SetWriteDeadline(time.Now().Add(s.cfg.StallTimeout))
	}
	if err := peer.wire.WriteMessage(envelope.msg); err != nil {
		peer.releaseQueuedInv(envelope.invItems)
		peer.releaseRelayBatch(envelope.msg)
		peer.close()
		_ = peer.wire.Close()
		return false
	}
	if s.cfg.StallTimeout > 0 {
		_ = peer.wire.SetWriteDeadline(time.Time{})
	}
	peer.noteKnownTxs(envelope.msg)
	peer.releaseQueuedInv(envelope.invItems)
	peer.releaseRelayBatch(envelope.msg)
	peer.telemetry.noteSent(envelope, peer.queueDepth())
	return true
}

func (s *Service) onPeerMessage(peer *peerConn, msg p2p.Message) error {
	s.logger.Debug("peer message received", slog.String("addr", peer.addr), slog.String("type", fmt.Sprintf("%T", msg)))
	switch m := msg.(type) {
	case p2p.PingMessage:
		return peer.send(p2p.PongMessage{Nonce: m.Nonce})
	case p2p.PongMessage:
		return nil
	case p2p.GetAddrMessage:
		return peer.send(p2p.AddrMessage{Addrs: s.knownPeerAddrs()})
	case p2p.AddrMessage:
		s.rememberKnownPeers(m.Addrs)
		return nil
	case p2p.InvMessage:
		return s.onInvMessage(peer, m)
	case p2p.GetDataMessage:
		return s.onGetDataMessage(peer, m)
	case p2p.NotFoundMessage:
		return s.onNotFoundMessage(peer, m)
	case p2p.GetHeadersMessage:
		headers, err := s.headersFromLocator(m.Locator, m.StopHash)
		if err != nil {
			return err
		}
		return peer.send(p2p.HeadersMessage{Headers: headers})
	case p2p.HeadersMessage:
		applied, err := s.applyPeerHeaders(m.Headers)
		if err != nil {
			return err
		}
		if applied > 0 {
			peer.noteHeight(s.headerHeight())
		}
		peer.noteUsefulHeaders(applied, time.Now())
		if applied > 0 {
			s.logger.Info("applied peer headers", slog.String("addr", peer.addr), slog.Int("count", applied), slog.Uint64("header_height", s.headerHeight()))
		}
		return s.requestBlocks(peer)
	case p2p.GetBlocksMessage:
		inv, err := s.blocksFromLocator(m.Locator, m.StopHash)
		if err != nil {
			return err
		}
		return peer.send(p2p.InvMessage{Items: inv})
	case p2p.BlockMessage:
		peer.deletePendingThin(consensus.HeaderHash(&m.Block.Header))
		return s.acceptPeerBlockMessage(peer, &m.Block)
	case p2p.XThinBlockMessage:
		return s.onXThinBlockMessage(peer, m)
	case p2p.GetXBlockTxMessage:
		return s.onGetXBlockTxMessage(peer, m)
	case p2p.XBlockTxMessage:
		return s.onXBlockTxMessage(peer, m)
	case p2p.TxMessage:
		peer.noteKnownTxs(m)
		admissions, errs, _, _ := s.submitDecodedTxsFrom([]types.Transaction{m.Tx}, peer)
		peer.noteUsefulTxs(countAcceptedAdmissions(admissions), time.Now())
		return s.handlePeerTxAdmissionErrors(peer, errs)
	case p2p.TxBatchMessage:
		peer.noteKnownTxs(m)
		admissions, errs, _, _ := s.submitDecodedTxsFrom(m.Txs, peer)
		peer.noteUsefulTxs(countAcceptedAdmissions(admissions), time.Now())
		return s.handlePeerTxAdmissionErrors(peer, errs)
	case p2p.TxReconMessage:
		peer.noteKnownTxIDs(m.TxIDs)
		return s.onTxReconMessage(peer, m)
	case p2p.TxRequestMessage:
		return s.onTxRequestMessage(peer, m)
	case p2p.CompactBlockMessage:
		return s.onCompactBlockMessage(peer, m)
	case p2p.GetBlockTxMessage:
		return s.onGetBlockTxMessage(peer, m)
	case p2p.BlockTxMessage:
		return s.onBlockTxMessage(peer, m)
	}
	return nil
}

func (s *Service) Info() ServiceInfo {
	s.stateMu.RLock()
	headerHeight := uint64(0)
	tipHeight := uint64(0)
	var tipHash [32]byte
	var utxoRoot [32]byte
	view, haveView := s.chainState.CommittedView()
	if haveView {
		tipHeight = view.Height
		tipHash = view.TipHash
		utxoRoot = view.UTXORoot
	}
	if tip := s.headerChain.TipHeight(); tip != nil {
		headerHeight = *tip
	}
	mempoolSize := s.pool.Count()
	s.stateMu.RUnlock()
	peers := s.activePeerAddrs()
	return ServiceInfo{
		Profile:        s.cfg.Profile.String(),
		TipHeight:      tipHeight,
		HeaderHeight:   headerHeight,
		TipHeaderHash:  hex.EncodeToString(tipHash[:]),
		UTXORoot:       hex.EncodeToString(utxoRoot[:]),
		MempoolSize:    mempoolSize,
		RPCAddr:        s.cfg.RPCAddr,
		P2PAddr:        s.cfg.P2PAddr,
		Peers:          peers,
		MinerEnabled:   s.cfg.MinerEnabled,
		MinerWorkers:   s.cfg.MinerWorkers,
		GenesisFixture: s.cfg.GenesisFixture,
	}
}

func (s *Service) ChainStateInfo() ChainStateInfo {
	s.stateMu.RLock()
	view, ok := s.chainState.CommittedView()
	headerHeight := uint64(0)
	if tip := s.headerChain.TipHeight(); tip != nil {
		headerHeight = *tip
	}
	s.stateMu.RUnlock()
	if !ok {
		return ChainStateInfo{Profile: s.cfg.Profile.String(), HeaderHeight: headerHeight}
	}
	info := ChainStateInfo{
		Profile:            s.cfg.Profile.String(),
		TipHeight:          view.Height,
		HeaderHeight:       headerHeight,
		TipHeaderHash:      hex.EncodeToString(view.TipHash[:]),
		UTXORoot:           hex.EncodeToString(view.UTXORoot[:]),
		UTXOCount:          len(view.UTXOs),
		NextBlockSizeLimit: consensus.NextBlockSizeLimit(view.BlockSizeState, consensus.ParamsForProfile(s.cfg.Profile)),
		TipTimestamp:       view.TipHeader.Timestamp,
	}
	if entry, err := s.chainState.Store().GetBlockIndex(&view.TipHash); err == nil && entry != nil {
		info.ChainWork = hex.EncodeToString(entry.ChainWork[:])
	}
	return info
}

func (s *Service) MempoolInfo() MempoolInfo {
	entries := s.pool.Snapshot()
	info := MempoolInfo{
		Count:              len(entries),
		Orphans:            s.pool.OrphanCount(),
		MinRelayFeePerByte: s.cfg.MinRelayFeePerByte,
		CandidateFrontier:  s.pool.SelectionCandidateCount(),
	}
	if len(entries) == 0 {
		return info
	}
	fees := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		info.Bytes += entry.Size
		info.TotalFees += entry.Fee
		fees = append(fees, entry.Fee)
	}
	info.MedianFee, info.LowFee, info.HighFee = summarizeFeeSet(fees)
	return info
}

func (s *Service) MiningInfo() MiningInfo {
	params := consensus.ParamsForProfile(s.cfg.Profile)
	info := MiningInfo{
		Enabled:           s.cfg.MinerEnabled,
		Workers:           s.cfg.MinerWorkers,
		TargetSpacingSecs: uint64(params.TargetSpacingSecs),
		Template:          s.BlockTemplateStats(),
	}
	if s.cfg.MinerKeyHash != ([32]byte{}) {
		info.MinerKeyHash = hex.EncodeToString(s.cfg.MinerKeyHash[:])
	}
	s.stateMu.RLock()
	view, ok := s.chainState.CommittedView()
	s.stateMu.RUnlock()
	if !ok {
		return info
	}
	info.CurrentBits = view.TipHeader.NBits
	nextBits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: view.Height, Header: view.TipHeader}, params)
	if err == nil {
		info.NextBits = nextBits
	}
	info.Difficulty = dashboardDifficulty(view.TipHeader.NBits, params.PowLimitBits)
	return info
}

func (s *Service) SubmitTx(tx types.Transaction) (mempool.Admission, error) {
	admissions, errs, orphanCount, mempoolSize := s.submitDecodedTxs([]types.Transaction{tx})
	if errs[0] != nil {
		s.logger.Warn("transaction rejected", slog.Any("error", errs[0]))
		return mempool.Admission{}, errs[0]
	}
	admission := admissions[0]
	if admission.Orphaned {
		s.logger.Debug("transaction stored as orphan",
			slog.String("txid", hex.EncodeToString(admission.TxID[:])),
			slog.Int("inputs", len(tx.Base.Inputs)),
			slog.Int("outputs", len(tx.Base.Outputs)),
			slog.Int("orphan_count", orphanCount),
			slog.Int("evicted_orphans", admission.EvictedOrphans),
		)
		return admission, nil
	}
	s.logger.Debug("transaction accepted",
		slog.String("txid", hex.EncodeToString(admission.TxID[:])),
		slog.Int("inputs", len(tx.Base.Inputs)),
		slog.Int("outputs", len(tx.Base.Outputs)),
		slog.Uint64("fee", admission.Summary.Fee),
		slog.Int("accepted_txs", len(admission.Accepted)),
		slog.Int("mempool_size", mempoolSize),
	)
	return admission, nil
}

func (s *Service) UTXOsByKeyHashes(keyHashes [][32]byte) []KeyHashUTXO {
	if len(keyHashes) == 0 {
		return nil
	}
	wanted := make(map[[32]byte]struct{}, len(keyHashes))
	for _, keyHash := range keyHashes {
		wanted[keyHash] = struct{}{}
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	utxos := s.chainState.ChainState().UTXOs()
	if !ok {
		utxos = s.chainState.ChainState().UTXOs()
	} else {
		utxos = view.UTXOs
	}
	out := make([]KeyHashUTXO, 0)
	for outPoint, entry := range utxos {
		if _, ok := wanted[entry.KeyHash]; !ok {
			continue
		}
		out = append(out, KeyHashUTXO{
			OutPoint: outPoint,
			Value:    entry.ValueAtoms,
			KeyHash:  entry.KeyHash,
		})
	}
	slices.SortFunc(out, func(a, b KeyHashUTXO) int {
		if cmp := bytes.Compare(a.KeyHash[:], b.KeyHash[:]); cmp != 0 {
			return cmp
		}
		if cmp := bytes.Compare(a.OutPoint.TxID[:], b.OutPoint.TxID[:]); cmp != 0 {
			return cmp
		}
		switch {
		case a.OutPoint.Vout < b.OutPoint.Vout:
			return -1
		case a.OutPoint.Vout > b.OutPoint.Vout:
			return 1
		default:
			return 0
		}
	})
	return out
}

func (s *Service) EstimateFeeRate(targetBlocks int) uint64 {
	if targetBlocks <= 0 {
		targetBlocks = 1
	}
	minRelay := s.cfg.MinRelayFeePerByte
	if minRelay == 0 {
		minRelay = 1
	}
	entries := s.pool.Snapshot()
	if len(entries) == 0 {
		return minRelay
	}
	rates := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.Size <= 0 {
			continue
		}
		rate := entry.Fee / uint64(entry.Size)
		if entry.Fee%uint64(entry.Size) != 0 {
			rate++
		}
		if rate < minRelay {
			rate = minRelay
		}
		rates = append(rates, rate)
	}
	if len(rates) == 0 {
		return minRelay
	}
	slices.Sort(rates)
	percentile := 75
	switch {
	case targetBlocks <= 1:
		percentile = 75
	case targetBlocks == 2:
		percentile = 60
	case targetBlocks == 3:
		percentile = 50
	default:
		percentile = 35
	}
	index := (len(rates) - 1) * percentile / 100
	if rates[index] < minRelay {
		return minRelay
	}
	return rates[index]
}

func (s *Service) WalletActivityByKeyHashes(keyHashes [][32]byte, limit int) ([]WalletActivity, error) {
	if len(keyHashes) == 0 {
		return nil, nil
	}
	s.stateMu.RLock()
	view, ok := s.chainState.CommittedView()
	s.stateMu.RUnlock()
	if !ok {
		return nil, ErrNoTip
	}
	wanted := make(map[[32]byte]struct{}, len(keyHashes))
	for _, keyHash := range keyHashes {
		wanted[keyHash] = struct{}{}
	}
	out := make([]WalletActivity, 0)
	for height := view.Height + 1; height > 0; height-- {
		blockHeight := height - 1
		hash, err := s.chainState.Store().GetBlockHashByHeight(blockHeight)
		if err != nil {
			return nil, err
		}
		if hash == nil {
			continue
		}
		block, err := s.chainState.Store().GetBlock(hash)
		if err != nil {
			return nil, err
		}
		if block == nil {
			continue
		}
		undo, err := s.chainState.Store().GetUndo(hash)
		if err != nil {
			return nil, err
		}
		items, err := walletActivityFromBlock(blockHeight, *hash, block, undo, wanted)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		if limit > 0 && len(out) >= limit {
			return out[:limit], nil
		}
	}
	return out, nil
}

func walletActivityFromBlock(height uint64, hash [32]byte, block *types.Block, undo []storage.BlockUndoEntry, wanted map[[32]byte]struct{}) ([]WalletActivity, error) {
	if block == nil {
		return nil, nil
	}
	timestamp := time.Unix(int64(block.Header.Timestamp), 0).UTC()
	out := make([]WalletActivity, 0, len(block.Txs))
	undoIndex := 0
	for i, tx := range block.Txs {
		received := uint64(0)
		for _, output := range tx.Base.Outputs {
			if _, ok := wanted[output.KeyHash]; ok {
				received += output.ValueAtoms
			}
		}
		sent := uint64(0)
		inputSum := uint64(0)
		if i > 0 {
			for _, input := range tx.Base.Inputs {
				if undoIndex >= len(undo) {
					return nil, fmt.Errorf("missing undo entry for wallet activity block %x input %v", hash, input.PrevOut)
				}
				entry := undo[undoIndex]
				undoIndex++
				inputSum += entry.Entry.ValueAtoms
				if _, ok := wanted[entry.Entry.KeyHash]; ok {
					sent += entry.Entry.ValueAtoms
				}
			}
		}
		if received == 0 && sent == 0 {
			continue
		}
		outputSum := uint64(0)
		for _, output := range tx.Base.Outputs {
			outputSum += output.ValueAtoms
		}
		fee := uint64(0)
		if i > 0 {
			fee = inputSum - outputSum
		}
		out = append(out, WalletActivity{
			TxID:      consensus.TxID(&tx),
			BlockHash: hash,
			Height:    height,
			Timestamp: timestamp,
			Coinbase:  i == 0,
			Received:  received,
			Sent:      sent,
			Fee:       fee,
			Net:       int64(received) - int64(sent),
		})
	}
	return out, nil
}

func (s *Service) submitDecodedTxs(txs []types.Transaction) ([]mempool.Admission, []error, int, int) {
	return s.submitDecodedTxsFrom(txs, nil)
}

func (s *Service) submitDecodedTxsFrom(txs []types.Transaction, source *peerConn) ([]mempool.Admission, []error, int, int) {
	s.releaseTxRequestsForTransactions(txs)
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	rules := consensus.DefaultConsensusRules()
	if len(txs) == 1 {
		admission, err := s.pool.AcceptTx(txs[0], s.chainUtxoSnapshot(), rules)
		if err != nil {
			errs[0] = err
		} else {
			admissions[0] = admission
			accepted = append(accepted, admission.Accepted...)
		}
		orphanCount := s.pool.OrphanCount()
		mempoolSize := s.pool.Count()
		if len(accepted) > 0 {
			s.invalidateBlockTemplate("tx_admission")
		}
		s.broadcastAcceptedTxsToPeers(s.peerSnapshotExcluding(source), accepted)
		return admissions, errs, orphanCount, mempoolSize
	}
	if batchHasInternalDependencies(txs) {
		admissions, errs, accepted = s.submitDependentDecodedTxs(txs, rules)
		orphanCount := s.pool.OrphanCount()
		mempoolSize := s.pool.Count()
		if len(accepted) > 0 {
			s.invalidateBlockTemplate("tx_admission")
		}
		s.broadcastAcceptedTxsToPeers(s.peerSnapshotExcluding(source), accepted)
		return admissions, errs, orphanCount, mempoolSize
	}
	tipHash, chainUtxos := s.chainUtxoSnapshotWithTip()
	snapshot := s.pool.AdmissionSnapshot()
	prepared, prepareErrs := s.prepareAdmissionsParallel(txs, snapshot, chainUtxos, rules)
	for i := range txs {
		if prepareErrs[i] != nil {
			errs[i] = prepareErrs[i]
			continue
		}
		if s.chainTipHash() != tipHash {
			retryAdmissions, retryErrs, retryAccepted := s.retryDecodedTxSuffix(txs[i:], rules)
			copy(admissions[i:], retryAdmissions)
			copy(errs[i:], retryErrs)
			accepted = append(accepted, retryAccepted...)
			break
		}
		admission, err := s.pool.CommitPrepared(prepared[i], chainUtxos, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admissions[i] = admission
		accepted = append(accepted, admission.Accepted...)
	}
	orphanCount := s.pool.OrphanCount()
	mempoolSize := s.pool.Count()
	if len(accepted) > 0 {
		s.invalidateBlockTemplate("tx_admission")
	}
	s.broadcastAcceptedTxsToPeers(s.peerSnapshotExcluding(source), accepted)
	return admissions, errs, orphanCount, mempoolSize
}

func (s *Service) submitDependentDecodedTxs(txs []types.Transaction, rules consensus.ConsensusRules) ([]mempool.Admission, []error, []mempool.AcceptedTx) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	tipHash, chainUtxos := s.chainUtxoSnapshotWithTip()
	snapshot := s.pool.AdmissionSnapshot()
	retried := false
	var fallbackTipHash [32]byte
	var fallbackChainUtxos consensus.UtxoSet

	for i, tx := range txs {
		if currentTip := s.chainTipHash(); currentTip != tipHash {
			if retried {
				if fallbackTipHash != currentTip {
					fallbackTipHash, fallbackChainUtxos = s.chainUtxoSnapshotWithTip()
				}
				admission, err := s.pool.AcceptTx(tx, fallbackChainUtxos, rules)
				if err != nil {
					errs[i] = err
					continue
				}
				admissions[i] = admission
				accepted = append(accepted, admission.Accepted...)
				continue
			}
			tipHash, chainUtxos = s.chainUtxoSnapshotWithTip()
			snapshot = s.pool.AdmissionSnapshot()
			retried = true
		}
		prepared, err := s.pool.PrepareAdmission(tx, snapshot, chainUtxos, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admission, err := s.pool.CommitPrepared(prepared, chainUtxos, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admissions[i] = admission
		accepted = append(accepted, admission.Accepted...)
		if admission.Orphaned {
			snapshot.Orphans[prepared.TxID] = struct{}{}
			continue
		}
		if err := mempool.AdvanceAdmissionSnapshot(&snapshot, chainUtxos, admission.Accepted); err != nil {
			snapshot = s.pool.AdmissionSnapshot()
		}
	}
	return admissions, errs, accepted
}

func batchHasInternalDependencies(txs []types.Transaction) bool {
	if len(txs) < 2 {
		return false
	}
	batchIDs := make(map[[32]byte]struct{}, len(txs))
	for i := range txs {
		batchIDs[consensus.TxID(&txs[i])] = struct{}{}
	}
	for i := range txs {
		for _, input := range txs[i].Base.Inputs {
			if _, ok := batchIDs[input.PrevOut.TxID]; ok {
				return true
			}
		}
	}
	return false
}

func (s *Service) retryDecodedTxSuffix(txs []types.Transaction, rules consensus.ConsensusRules) ([]mempool.Admission, []error, []mempool.AcceptedTx) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	if len(txs) == 0 {
		return admissions, errs, accepted
	}
	tipHash, chainUtxos := s.chainUtxoSnapshotWithTip()
	snapshot := s.pool.AdmissionSnapshot()
	prepared, prepareErrs := s.prepareAdmissionsParallel(txs, snapshot, chainUtxos, rules)
	var fallbackTipHash [32]byte
	var fallbackChainUtxos consensus.UtxoSet
	for i, tx := range txs {
		if prepareErrs[i] != nil {
			errs[i] = prepareErrs[i]
			continue
		}
		if currentTip := s.chainTipHash(); currentTip != tipHash {
			if fallbackTipHash != currentTip {
				fallbackTipHash, fallbackChainUtxos = s.chainUtxoSnapshotWithTip()
			}
			admission, err := s.pool.AcceptTx(tx, fallbackChainUtxos, rules)
			if err != nil {
				errs[i] = err
				continue
			}
			admissions[i] = admission
			accepted = append(accepted, admission.Accepted...)
			continue
		}
		admission, err := s.pool.CommitPrepared(prepared[i], chainUtxos, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admissions[i] = admission
		accepted = append(accepted, admission.Accepted...)
	}
	return admissions, errs, accepted
}

func decodePackedTransactions(encoded string) ([]types.Transaction, error) {
	if encoded == "" {
		return nil, nil
	}
	buf, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	txs := make([]types.Transaction, 0)
	limits := types.DefaultCodecLimits()
	for len(buf) > 0 {
		if len(buf) < 4 {
			return nil, errors.New("truncated packed transaction length")
		}
		size := binary.LittleEndian.Uint32(buf[:4])
		buf = buf[4:]
		if size == 0 {
			return nil, errors.New("packed transaction length must be non-zero")
		}
		if uint32(len(buf)) < size {
			return nil, errors.New("truncated packed transaction payload")
		}
		tx, err := types.DecodeTransactionWithLimits(buf[:size], limits)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
		buf = buf[size:]
	}
	return txs, nil
}

func (s *Service) prepareAdmissionsParallel(txs []types.Transaction, snapshot mempool.AdmissionSnapshot, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) ([]mempool.PreparedAdmission, []error) {
	prepared := make([]mempool.PreparedAdmission, len(txs))
	errs := make([]error, len(txs))
	if len(txs) == 0 {
		return prepared, errs
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(txs) {
		workers = len(txs)
	}
	indexCh := make(chan int, len(txs))
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range indexCh {
				item, err := s.pool.PrepareAdmission(txs[idx], snapshot, chainUtxos, rules)
				if err != nil {
					errs[idx] = err
					continue
				}
				prepared[idx] = item
			}
		}()
	}
	for i := range txs {
		indexCh <- i
	}
	close(indexCh)
	wg.Wait()
	return prepared, errs
}

func (s *Service) MineBlocks(count int) ([]string, error) {
	hashes := make([]string, 0, count)
	for len(hashes) < count {
		hash, err := s.mineOneBlock()
		if err != nil {
			return hashes, err
		}
		hashes = append(hashes, hex.EncodeToString(hash[:]))
	}
	return hashes, nil
}

func (s *Service) currentTemplateGeneration() uint64 {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	return s.templateGeneration
}

func (s *Service) invalidateBlockTemplate(reason string) {
	s.templateMu.Lock()
	s.templateGeneration++
	s.template = nil
	s.templateMu.Unlock()
	s.templateStats.noteInvalidation(reason)
}

func (s *Service) mineOneBlock() ([32]byte, error) {
	for {
		block, generation, err := s.buildBlockTemplateWithGeneration()
		if err != nil {
			return [32]byte{}, err
		}
		block, fresh, err := s.mineBlockTemplate(block, generation)
		if err != nil {
			return [32]byte{}, err
		}
		if !fresh {
			s.templateStats.noteInterruption()
			continue
		}
		hash, _, err := s.acceptMinedBlock(block)
		if err == nil {
			return hash, nil
		}
		if errors.Is(err, ErrNoTip) {
			return [32]byte{}, err
		}
		if strings.Contains(err.Error(), "stale template") {
			continue
		}
		return [32]byte{}, err
	}
}

func (s *Service) mineBlockTemplate(block types.Block, generation uint64) (types.Block, bool, error) {
	params := consensus.ParamsForProfile(s.cfg.Profile)
	mineHeader := s.mineHeaderFn
	if mineHeader == nil {
		mineHeader = consensus.MineHeaderInterruptible
	}
	minedHeader, ok, err := mineHeader(block.Header, params, func(uint32) bool {
		return s.currentTemplateGeneration() == generation
	})
	if err != nil {
		return types.Block{}, false, err
	}
	if !ok {
		return types.Block{}, ok, err
	}
	block.Header = minedHeader
	return block, true, nil
}

func (s *Service) MineFundingOutputs(keyHashes [][32]byte) ([]FundingOutput, error) {
	if !s.cfg.Profile.IsRegtestLike() {
		return nil, errors.New("funding outputs are only available on regtest-style profiles")
	}
	if len(keyHashes) == 0 {
		return nil, nil
	}
	for {
		block, outputs, err := s.buildFundingBlock(keyHashes)
		if err != nil {
			return nil, err
		}
		hash, _, err := s.acceptMinedBlock(block)
		if err == nil {
			for i := range outputs {
				outputs[i].BlockHash = hash
			}
			return outputs, nil
		}
		if strings.Contains(err.Error(), "stale template") {
			continue
		}
		return nil, err
	}
}

func (s *Service) acceptMinedBlock(block types.Block) ([32]byte, uint64, error) {
	hash := consensus.HeaderHash(&block.Header)

	s.stateMu.Lock()
	currentTip := s.chainState.ChainState().TipHeader()
	if currentTip == nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, ErrNoTip
	}
	if block.Header.PrevBlockHash != consensus.HeaderHash(currentTip) {
		s.stateMu.Unlock()
		return [32]byte{}, 0, errors.New("stale template")
	}
	if _, err := s.chainState.ApplyBlock(&block); err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	if err := s.headerChain.ApplyHeader(&block.Header); err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	stored, err := s.headerChain.StoredState()
	if err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	if err := s.chainState.Store().WriteHeaderState(stored); err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	height := uint64(0)
	if tip := s.chainState.ChainState().TipHeight(); tip != nil {
		height = *tip
	}
	s.stateMu.Unlock()

	s.pool.RemoveConfirmed(&block)
	s.invalidateBlockTemplate("tip_advanced")
	s.cacheRecentBlock(block)
	s.cacheRecentHeader(block.Header)
	s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	s.logger.Info("block accepted",
		slog.Uint64("height", height),
		slog.String("hash", hex.EncodeToString(hash[:])),
		slog.Int("txs", len(block.Txs)),
		slog.Int("mempool_size", s.pool.Count()),
	)
	return hash, height, nil
}

func (s *Service) BuildBlockTemplate() (types.Block, error) {
	block, _, err := s.buildBlockTemplateWithGeneration()
	return block, err
}

func (s *Service) buildBlockTemplateWithGeneration() (types.Block, uint64, error) {
	ctx, err := s.chainTemplateContext()
	if err != nil {
		return types.Block{}, 0, err
	}
	mempoolEpoch := s.pool.Epoch()
	if cached, generation, ok := s.cachedBlockTemplate(ctx.tipHash, mempoolEpoch); ok {
		s.templateStats.noteCacheHit(s.pool.SelectionCandidateCount())
		return cloneBlock(cached), generation, nil
	}
	if block, generation, ok, err := s.extendBlockTemplate(ctx, mempoolEpoch); err != nil {
		return types.Block{}, 0, err
	} else if ok {
		s.templateStats.noteRebuild(s.pool.SelectionCandidateCount())
		return cloneBlock(block), generation, nil
	}
	snapshot, err := s.chainSelectionSnapshot()
	if err != nil {
		return types.Block{}, 0, err
	}
	block, selectedEntries, totalFees, usedTxBytes, baseUtxos, selectionView, selectionAcc, err := s.buildBlockCandidate(snapshot)
	if err != nil {
		return types.Block{}, 0, err
	}
	generation := s.storeBlockTemplate(snapshot.tipHash, mempoolEpoch, block, selectedEntries, totalFees, usedTxBytes, baseUtxos, selectionView, snapshot.utxoAcc, selectionAcc)
	s.templateStats.noteRebuild(s.pool.SelectionCandidateCount())
	return cloneBlock(block), generation, nil
}

func (s *Service) minerLoop(workerID int) {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		hash, err := s.mineOneBlock()
		if err != nil {
			if errors.Is(err, ErrNoTip) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			s.logger.Warn("continuous mining failed", slog.Int("worker", workerID), slog.Any("error", err))
			select {
			case <-s.stopCh:
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		s.logger.Debug("miner worker found block", slog.Int("worker", workerID), slog.String("hash", hex.EncodeToString(hash[:])))
	}
}

func (s *Service) buildBlockCandidate(snapshot chainSelectionSnapshot) (types.Block, []mempool.SnapshotEntry, uint64, int, consensus.UtxoSet, *consensus.UtxoOverlay, *utreexo.Accumulator, error) {
	baseUtxos := snapshot.utxos
	maxTemplateBytes := int(consensus.NextBlockSizeLimit(snapshot.blockSizeState, consensus.ParamsForProfile(s.cfg.Profile)))
	if maxTemplateBytes > 1024 {
		maxTemplateBytes -= 1024
	}
	selectedEntries, totalFees, selectionView := s.pool.SelectForBlockOverlay(baseUtxos, consensus.DefaultConsensusRules(), maxTemplateBytes)
	selectionAcc, err := snapshot.utxoAcc.Apply(selectedEntrySpends(selectedEntries), selectedEntryLeaves(selectedEntries))
	if err != nil {
		return types.Block{}, nil, 0, 0, nil, nil, nil, err
	}
	block, usedTxBytes, err := s.assembleBlockTemplate(chainTemplateContext{
		tipHash:        snapshot.tipHash,
		height:         snapshot.height,
		tipHeader:      snapshot.tipHeader,
		blockSizeState: snapshot.blockSizeState,
	}, selectedEntries, totalFees, selectionAcc)
	if err != nil {
		return types.Block{}, nil, 0, 0, nil, nil, nil, err
	}
	s.logger.Debug("building block candidate",
		slog.Uint64("next_height", snapshot.height+1),
		slog.Int("selected_txs", len(selectedEntries)),
		slog.Uint64("total_fees", totalFees),
	)
	return block, selectedEntries, totalFees, usedTxBytes, baseUtxos, selectionView, selectionAcc, nil
}

func (s *Service) assembleBlockTemplate(ctx chainTemplateContext, selectedEntries []mempool.SnapshotEntry, totalFees uint64, selectionAcc *utreexo.Accumulator) (types.Block, int, error) {
	params := consensus.ParamsForProfile(s.cfg.Profile)
	nextTimestamp := ctx.tipHeader.Timestamp + uint64(params.TargetSpacingSecs)
	if nextTimestamp <= ctx.tipHeader.Timestamp {
		nextTimestamp = ctx.tipHeader.Timestamp + 1
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: ctx.height, Header: ctx.tipHeader}, params)
	if err != nil {
		return types.Block{}, 0, err
	}

	orderedEntries := append([]mempool.SnapshotEntry(nil), selectedEntries...)
	sort.Slice(orderedEntries, func(i, j int) bool {
		return bytes.Compare(orderedEntries[i].TxID[:], orderedEntries[j].TxID[:]) < 0
	})

	selected := make([]types.Transaction, 0, len(orderedEntries))
	usedTxBytes := 0
	for _, entry := range orderedEntries {
		selected = append(selected, entry.Tx)
		usedTxBytes += entry.Size
	}

	coinbase := coinbaseTxForHeight(ctx.height+1, []types.TxOutput{{
		ValueAtoms: consensus.SubsidyAtoms(ctx.height+1, params) + totalFees,
		KeyHash:    s.cfg.MinerKeyHash,
	}})
	coinbaseTxID := consensus.TxID(&coinbase)

	txs := make([]types.Transaction, 0, len(selected)+1)
	txs = append(txs, coinbase)
	txs = append(txs, selected...)
	_, _, txRoot, authRoot := consensus.BuildBlockRoots(txs)
	coinbaseLeaves := make([]utreexo.UtxoLeaf, 0, len(coinbase.Base.Outputs))
	for vout, output := range coinbase.Base.Outputs {
		outPoint := types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}
		coinbaseLeaves = append(coinbaseLeaves, utreexo.UtxoLeaf{
			OutPoint:   outPoint,
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		})
	}
	finalAcc, err := selectionAcc.Apply(nil, coinbaseLeaves)
	if err != nil {
		return types.Block{}, 0, err
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  ctx.tipHash,
		MerkleTxIDRoot: txRoot,
		MerkleAuthRoot: authRoot,
		UTXORoot:       finalAcc.Root(),
		Timestamp:      nextTimestamp,
		NBits:          nbits,
	}
	return types.Block{Header: header, Txs: txs}, usedTxBytes, nil
}

func (s *Service) buildFundingBlock(keyHashes [][32]byte) (types.Block, []FundingOutput, error) {
	ctx, err := s.chainSelectionSnapshot()
	if err != nil {
		return types.Block{}, nil, err
	}
	if len(keyHashes) == 0 {
		return types.Block{}, nil, errors.New("at least one key hash is required")
	}
	params := consensus.ParamsForProfile(s.cfg.Profile)
	nextTimestamp := ctx.tipHeader.Timestamp + uint64(params.TargetSpacingSecs)
	if nextTimestamp <= ctx.tipHeader.Timestamp {
		nextTimestamp = ctx.tipHeader.Timestamp + 1
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: ctx.height, Header: ctx.tipHeader}, params)
	if err != nil {
		return types.Block{}, nil, err
	}
	subsidy := consensus.SubsidyAtoms(ctx.height+1, params)
	if subsidy < uint64(len(keyHashes)) {
		return types.Block{}, nil, consensus.ErrInputsLessThanOutputs
	}
	outputs := make([]types.TxOutput, len(keyHashes))
	perOutput := subsidy / uint64(len(keyHashes))
	remainder := subsidy % uint64(len(keyHashes))
	for i, keyHash := range keyHashes {
		value := perOutput
		if i == len(keyHashes)-1 {
			value += remainder
		}
		outputs[i] = types.TxOutput{ValueAtoms: value, KeyHash: keyHash}
	}
	coinbase := coinbaseTxForHeight(ctx.height+1, outputs)
	coinbaseTxID := consensus.TxID(&coinbase)
	funding := make([]FundingOutput, 0, len(outputs))
	fundingLeaves := make([]utreexo.UtxoLeaf, 0, len(outputs))
	for vout, output := range outputs {
		outPoint := types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}
		fundingLeaves = append(fundingLeaves, utreexo.UtxoLeaf{
			OutPoint:   outPoint,
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		})
		funding = append(funding, FundingOutput{
			OutPoint: outPoint,
			Value:    output.ValueAtoms,
			KeyHash:  output.KeyHash,
		})
	}
	finalAcc, err := ctx.utxoAcc.Apply(nil, fundingLeaves)
	if err != nil {
		return types.Block{}, nil, err
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  ctx.tipHash,
		MerkleTxIDRoot: consensus.MerkleRoot([][32]byte{coinbaseTxID}),
		MerkleAuthRoot: consensus.MerkleRoot([][32]byte{consensus.AuthID(&coinbase)}),
		UTXORoot:       finalAcc.Root(),
		Timestamp:      nextTimestamp,
		NBits:          nbits,
	}
	mined, err := consensus.MineHeader(header, params)
	if err != nil {
		return types.Block{}, nil, err
	}
	return types.Block{Header: mined, Txs: []types.Transaction{coinbase}}, funding, nil
}

func (s *Service) chainUtxoSnapshot() consensus.UtxoSet {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	if !ok {
		return s.chainState.ChainState().UTXOs()
	}
	return view.UTXOs
}

func coinbaseTxForHeight(height uint64, outputs []types.TxOutput) types.Transaction {
	return types.Transaction{
		Base: types.TxBase{
			Version:        1,
			CoinbaseHeight: &height,
			Outputs:        outputs,
		},
	}
}

func (s *Service) chainUtxoSnapshotWithTip() ([32]byte, consensus.UtxoSet) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	if !ok {
		return [32]byte{}, s.chainState.ChainState().UTXOs()
	}
	return view.TipHash, view.UTXOs
}

func (s *Service) chainTipHash() [32]byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	if ok {
		return view.TipHash
	}
	return [32]byte{}
}

func (s *Service) chainTemplateContext() (chainTemplateContext, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	if !ok {
		return chainTemplateContext{}, ErrNoTip
	}
	return chainTemplateContext{
		tipHash:        view.TipHash,
		height:         view.Height,
		tipHeader:      view.TipHeader,
		blockSizeState: view.BlockSizeState,
	}, nil
}

func (s *Service) chainSelectionSnapshot() (chainSelectionSnapshot, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	if !ok {
		return chainSelectionSnapshot{}, ErrNoTip
	}
	return chainSelectionSnapshot{
		tipHash:        view.TipHash,
		height:         view.Height,
		tipHeader:      view.TipHeader,
		blockSizeState: view.BlockSizeState,
		utxos:          view.UTXOs,
		utxoAcc:        view.UTXOAcc,
	}, nil
}

func (s *Service) cachedBlockTemplate(tipHash [32]byte, mempoolEpoch uint64) (types.Block, uint64, bool) {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	if s.template == nil || s.template.tipHash != tipHash || s.template.mempoolEpoch != mempoolEpoch {
		return types.Block{}, 0, false
	}
	return cloneBlock(s.template.block), s.template.generation, true
}

func (s *Service) extendBlockTemplate(ctx chainTemplateContext, mempoolEpoch uint64) (types.Block, uint64, bool, error) {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	if s.template == nil || s.template.tipHash != ctx.tipHash || s.template.mempoolEpoch >= mempoolEpoch {
		return types.Block{}, 0, false, nil
	}
	maxTemplateBytes := int(consensus.NextBlockSizeLimit(ctx.blockSizeState, consensus.ParamsForProfile(s.cfg.Profile)))
	if maxTemplateBytes > 1024 {
		maxTemplateBytes -= 1024
	}
	if s.template.usedTxBytes >= maxTemplateBytes {
		return types.Block{}, 0, false, nil
	}
	if !s.pool.ContainsAll(s.template.selected) {
		return types.Block{}, 0, false, nil
	}

	added, addedFees := s.pool.AppendForBlockOverlay(s.template.baseUtxos, s.template.selectionView, consensus.DefaultConsensusRules(), maxTemplateBytes, s.template.selected)
	if len(added) == 0 {
		s.template.mempoolEpoch = mempoolEpoch
		block, _, err := s.assembleBlockTemplate(ctx, s.template.selected, s.template.totalFees, s.template.selectionAcc)
		if err != nil {
			return types.Block{}, 0, false, err
		}
		s.template.block = cloneBlock(block)
		s.template.builtAt = time.Now()
		s.templateStats.noteBuildTime(s.template.builtAt)
		return cloneBlock(s.template.block), s.template.generation, true, nil
	}

	s.template.selected = append(s.template.selected, added...)
	s.template.totalFees += addedFees
	for _, entry := range added {
		s.template.usedTxBytes += entry.Size
	}
	nextAcc, err := s.template.selectionAcc.Apply(selectedEntrySpends(added), selectedEntryLeaves(added))
	if err != nil {
		return types.Block{}, 0, false, err
	}
	s.template.selectionAcc = nextAcc
	block, _, err := s.assembleBlockTemplate(ctx, s.template.selected, s.template.totalFees, s.template.selectionAcc)
	if err != nil {
		return types.Block{}, 0, false, err
	}
	s.template.block = cloneBlock(block)
	s.template.mempoolEpoch = mempoolEpoch
	s.template.builtAt = time.Now()
	s.templateStats.noteBuildTime(s.template.builtAt)
	return cloneBlock(s.template.block), s.template.generation, true, nil
}

func (s *Service) storeBlockTemplate(tipHash [32]byte, mempoolEpoch uint64, block types.Block, selected []mempool.SnapshotEntry, totalFees uint64, usedTxBytes int, baseUtxos consensus.UtxoSet, selectionView *consensus.UtxoOverlay, baseAcc *utreexo.Accumulator, selectionAcc *utreexo.Accumulator) uint64 {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	if s.templateGeneration == 0 {
		s.templateGeneration = 1
	}
	generation := s.templateGeneration
	s.template = &blockTemplateCache{
		tipHash:       tipHash,
		mempoolEpoch:  mempoolEpoch,
		generation:    generation,
		builtAt:       time.Now(),
		block:         cloneBlock(block),
		selected:      append([]mempool.SnapshotEntry(nil), selected...),
		totalFees:     totalFees,
		usedTxBytes:   usedTxBytes,
		baseUtxos:     baseUtxos,
		selectionView: selectionView,
		baseAcc:       baseAcc,
		selectionAcc:  selectionAcc,
	}
	s.templateStats.noteBuildTime(s.template.builtAt)
	return generation
}

func cloneBlock(block types.Block) types.Block {
	out := block
	if len(block.Txs) != 0 {
		out.Txs = append([]types.Transaction(nil), block.Txs...)
	}
	return out
}

func selectedEntrySpends(entries []mempool.SnapshotEntry) []types.OutPoint {
	spent := make([]types.OutPoint, 0)
	for _, entry := range entries {
		for _, input := range entry.Tx.Base.Inputs {
			spent = append(spent, input.PrevOut)
		}
	}
	return spent
}

func selectedEntryLeaves(entries []mempool.SnapshotEntry) []utreexo.UtxoLeaf {
	created := make([]utreexo.UtxoLeaf, 0)
	for _, entry := range entries {
		for vout, output := range entry.Tx.Base.Outputs {
			created = append(created, utreexo.UtxoLeaf{
				OutPoint:   types.OutPoint{TxID: entry.TxID, Vout: uint32(vout)},
				ValueAtoms: output.ValueAtoms,
				KeyHash:    output.KeyHash,
			})
		}
	}
	return created
}

func buildXThinBlockMessage(block types.Block) p2p.Message {
	if len(block.Txs) == 0 {
		return p2p.BlockMessage{Block: block}
	}
	nonce := randomNonce()
	shortIDs := make([]uint64, 0, len(block.Txs)-1)
	seen := make(map[uint64]struct{}, len(block.Txs))
	for _, tx := range block.Txs[1:] {
		shortID := thinBlockShortID(nonce, consensus.TxID(&tx))
		if _, ok := seen[shortID]; ok {
			return p2p.BlockMessage{Block: block}
		}
		seen[shortID] = struct{}{}
		shortIDs = append(shortIDs, shortID)
	}
	return p2p.XThinBlockMessage{
		Header:   block.Header,
		Nonce:    nonce,
		Coinbase: block.Txs[0],
		ShortIDs: shortIDs,
	}
}

func buildCompactBlockMessage(block types.Block) p2p.Message {
	if len(block.Txs) == 0 {
		return p2p.BlockMessage{Block: block}
	}
	nonce := randomNonce()
	prefilled := []p2p.PrefilledTx{{Index: 0, Tx: block.Txs[0]}}
	shortIDs := make([]uint64, 0, len(block.Txs)-1)
	seen := make(map[uint64]struct{}, len(block.Txs))
	for _, tx := range block.Txs[1:] {
		shortID := thinBlockShortID(nonce, consensus.TxID(&tx))
		if _, ok := seen[shortID]; ok {
			return p2p.BlockMessage{Block: block}
		}
		seen[shortID] = struct{}{}
		shortIDs = append(shortIDs, shortID)
	}
	return p2p.CompactBlockMessage{
		Header:    block.Header,
		Nonce:     nonce,
		Prefilled: prefilled,
		ShortIDs:  shortIDs,
	}
}

func reconstructXThinBlock(msg p2p.XThinBlockMessage, matches map[uint64][]types.Transaction) (*pendingThinBlock, []uint32) {
	hash := consensus.HeaderHash(&msg.Header)
	state := &pendingThinBlock{
		hash:   hash,
		header: msg.Header,
		txs:    make([]types.Transaction, len(msg.ShortIDs)+1),
		filled: make([]bool, len(msg.ShortIDs)+1),
	}
	state.txs[0] = msg.Coinbase
	state.filled[0] = true
	if len(msg.ShortIDs) == 0 {
		return state, nil
	}

	missing := make([]uint32, 0)
	for i, shortID := range msg.ShortIDs {
		candidates := matches[shortID]
		if len(candidates) != 1 {
			missing = append(missing, uint32(i+1))
			continue
		}
		state.txs[i+1] = candidates[0]
		state.filled[i+1] = true
	}
	return state, missing
}

func reconstructCompactBlock(msg p2p.CompactBlockMessage, matches map[uint64][]types.Transaction) (*pendingThinBlock, []uint32) {
	hash := consensus.HeaderHash(&msg.Header)
	totalTxs := len(msg.Prefilled) + len(msg.ShortIDs)
	state := &pendingThinBlock{
		hash:   hash,
		header: msg.Header,
		txs:    make([]types.Transaction, totalTxs),
		filled: make([]bool, totalTxs),
	}
	for _, item := range msg.Prefilled {
		if int(item.Index) >= len(state.txs) {
			return state, allPendingIndexes(state)
		}
		state.txs[item.Index] = item.Tx
		state.filled[item.Index] = true
	}
	missing := make([]uint32, 0)
	shortIndex := 0
	for txIndex := range state.txs {
		if state.filled[txIndex] {
			continue
		}
		if shortIndex >= len(msg.ShortIDs) {
			missing = append(missing, uint32(txIndex))
			continue
		}
		candidates := matches[msg.ShortIDs[shortIndex]]
		shortIndex++
		if len(candidates) != 1 {
			missing = append(missing, uint32(txIndex))
			continue
		}
		state.txs[txIndex] = candidates[0]
		state.filled[txIndex] = true
	}
	return state, missing
}

func allPendingIndexes(state *pendingThinBlock) []uint32 {
	indexes := make([]uint32, 0, len(state.txs))
	for i := range state.txs {
		if !state.filled[i] {
			indexes = append(indexes, uint32(i))
		}
	}
	return indexes
}

func xThinShortIDSet(msg p2p.XThinBlockMessage) map[uint64]struct{} {
	if len(msg.ShortIDs) == 0 {
		return nil
	}
	set := make(map[uint64]struct{}, len(msg.ShortIDs))
	for _, shortID := range msg.ShortIDs {
		set[shortID] = struct{}{}
	}
	return set
}

func compactShortIDSet(msg p2p.CompactBlockMessage) map[uint64]struct{} {
	if len(msg.ShortIDs) == 0 {
		return nil
	}
	set := make(map[uint64]struct{}, len(msg.ShortIDs))
	for _, shortID := range msg.ShortIDs {
		set[shortID] = struct{}{}
	}
	return set
}

func shouldFallbackToFullBlock(state *pendingThinBlock, missing []uint32) bool {
	// Thin-block recovery stays cheap only when overlap is high. If most of the
	// block is missing locally, the extra round trip is worse than asking for the
	// full block directly.
	return len(missing) > 128 || len(missing)*2 > len(state.txs)
}

func thinBlockShortID(nonce uint64, txid [32]byte) uint64 {
	var seed [40]byte
	binary.LittleEndian.PutUint64(seed[:8], nonce)
	copy(seed[8:], txid[:])
	hash := bpcrypto.Sha256d(seed[:])
	return binary.LittleEndian.Uint64(hash[:8])
}

func (p *pendingThinBlock) fill(index uint32, tx types.Transaction) bool {
	if int(index) >= len(p.txs) {
		return false
	}
	p.txs[index] = tx
	p.filled[index] = true
	return true
}

func (p *pendingThinBlock) complete() bool {
	for _, filled := range p.filled {
		if !filled {
			return false
		}
	}
	return true
}

func (p *pendingThinBlock) block() types.Block {
	return types.Block{
		Header: p.header,
		Txs:    append([]types.Transaction(nil), p.txs...),
	}
}

func (s *Service) cacheRecentHeader(header types.BlockHeader) {
	hash := consensus.HeaderHash(&header)
	s.recentMu.Lock()
	defer s.recentMu.Unlock()
	cacheRecentHeaderLocked(&s.recentHdrs, hash, header)
}

func (s *Service) recentHeader(hash [32]byte) (types.BlockHeader, bool) {
	s.recentMu.RLock()
	defer s.recentMu.RUnlock()
	header, ok := s.recentHdrs.items[hash]
	return header, ok
}

func (s *Service) cacheRecentBlock(block types.Block) {
	hash := consensus.HeaderHash(&block.Header)
	s.recentMu.Lock()
	defer s.recentMu.Unlock()
	cacheRecentBlockLocked(&s.recentBlks, hash, block)
	cacheRecentHeaderLocked(&s.recentHdrs, hash, block.Header)
}

func (s *Service) recentBlock(hash [32]byte) (types.Block, bool) {
	s.recentMu.RLock()
	defer s.recentMu.RUnlock()
	block, ok := s.recentBlks.items[hash]
	return block, ok
}

func cacheRecentHeaderLocked(cache *recentHeaderCache, hash [32]byte, header types.BlockHeader) {
	if _, ok := cache.items[hash]; !ok {
		cache.order = append(cache.order, hash)
	}
	cache.items[hash] = header
	trimRecentHeaderCacheLocked(cache)
}

func cacheRecentBlockLocked(cache *recentBlockCache, hash [32]byte, block types.Block) {
	if _, ok := cache.items[hash]; !ok {
		cache.order = append(cache.order, hash)
	}
	cache.items[hash] = cloneBlock(block)
	trimRecentBlockCacheLocked(cache)
}

func trimRecentHeaderCacheLocked(cache *recentHeaderCache) {
	const limit = 256
	for len(cache.order) > limit {
		evict := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.items, evict)
	}
}

func trimRecentBlockCacheLocked(cache *recentBlockCache) {
	const limit = 256
	for len(cache.order) > limit {
		evict := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.items, evict)
	}
}

func (s *Service) headersFromLocator(locator [][32]byte, stopHash [32]byte) ([]types.BlockHeader, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]types.BlockHeader, 0)
	tip := s.headerChain.TipHeight()
	if tip == nil {
		return out, nil
	}
	startHeight, err := s.findLocatorHeightLocked(locator)
	if err != nil {
		return nil, err
	}
	for height := startHeight + 1; height <= *tip && len(out) < 2000; height++ {
		entry, err := s.chainState.Store().GetBlockIndexByHeight(height)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			break
		}
		hash := consensus.HeaderHash(&entry.Header)
		out = append(out, entry.Header)
		if stopHash != ([32]byte{}) && hash == stopHash {
			break
		}
	}
	return out, nil
}

func (s *Service) blocksFromLocator(locator [][32]byte, stopHash [32]byte) ([]p2p.InvVector, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]p2p.InvVector, 0)
	tip := s.chainState.ChainState().TipHeight()
	if tip == nil {
		return out, nil
	}
	startHeight, err := s.findLocatorHeightLocked(locator)
	if err != nil {
		return nil, err
	}
	for height := startHeight + 1; height <= *tip && len(out) < 500; height++ {
		hash, err := s.chainState.Store().GetBlockHashByHeight(height)
		if err != nil {
			return nil, err
		}
		if hash == nil {
			break
		}
		if stopHash != ([32]byte{}) && *hash == stopHash {
			break
		}
		out = append(out, p2p.InvVector{Type: p2p.InvTypeBlock, Hash: *hash})
	}
	return out, nil
}

func (s *Service) applyPeerHeaders(headers []types.BlockHeader) (int, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if len(headers) == 0 {
		return 0, nil
	}
	if s.headerChain.TipHeader() == nil {
		return 0, ErrNoTip
	}
	parentHash := headers[0].PrevBlockHash
	parentEntry, err := s.chainState.Store().GetBlockIndex(&parentHash)
	if err != nil {
		return 0, err
	}
	if parentEntry == nil {
		return 0, fmt.Errorf("missing parent entry for header parent %x", parentHash)
	}
	prevEntry := *parentEntry
	applied := 0
	batchEntries := make([]storage.HeaderBatchEntry, 0, len(headers))
	batchEntryByHash := make(map[[32]byte]storage.BlockIndexEntry, len(headers))
	for _, header := range headers {
		if err := consensus.ValidateHeader(&header, consensus.PrevBlockContext{
			Height: prevEntry.Height,
			Header: prevEntry.Header,
		}, s.headerChain.params); err != nil {
			return applied, err
		}
		work, err := consensus.BlockWork(header.NBits)
		if err != nil {
			return applied, err
		}
		prevEntry = storage.BlockIndexEntry{
			Height:     prevEntry.Height + 1,
			ParentHash: header.PrevBlockHash,
			Header:     header,
			ChainWork:  consensus.AddChainWork(prevEntry.ChainWork, work),
		}
		batchEntries = append(batchEntries, storage.HeaderBatchEntry{
			Height:    prevEntry.Height,
			Header:    header,
			ChainWork: prevEntry.ChainWork,
		})
		batchEntryByHash[consensus.HeaderHash(&header)] = prevEntry
		s.cacheRecentHeader(header)
		applied++
	}
	currentTipHash := consensus.HeaderHash(s.headerChain.TipHeader())
	currentTipEntry, err := s.chainState.Store().GetBlockIndex(&currentTipHash)
	if err != nil {
		return applied, err
	}
	if currentTipEntry == nil {
		return applied, fmt.Errorf("missing current header tip entry %x", currentTipHash)
	}
	nextChain := s.headerChain
	stored, err := s.headerChain.StoredState()
	if err != nil {
		return applied, err
	}
	oldTipHeight := currentTipEntry.Height
	var activeEntries []storage.HeaderBatchEntry
	var activeForkHeight uint64
	if consensus.CompareChainWork(prevEntry.ChainWork, currentTipEntry.ChainWork) > 0 {
		promoted := NewHeaderChain(s.headerChain.Profile())
		if err := promoted.InitializeTip(prevEntry.Height, prevEntry.Header); err != nil {
			return applied, err
		}
		nextChain = promoted
		stored = &storage.StoredHeaderState{
			Profile:   s.headerChain.Profile(),
			Height:    prevEntry.Height,
			TipHeader: prevEntry.Header,
		}
		activeEntries, activeForkHeight, err = s.headerBranchEntriesLocked(prevEntry, batchEntryByHash)
		if err != nil {
			return applied, err
		}
	}
	if err := s.chainState.Store().CommitHeaderChain(stored, batchEntries, activeForkHeight, oldTipHeight, activeEntries); err != nil {
		return 0, err
	}
	s.headerChain = nextChain
	return applied, nil
}

func (s *Service) headerBranchEntriesLocked(tip storage.BlockIndexEntry, batchEntries map[[32]byte]storage.BlockIndexEntry) ([]storage.HeaderBatchEntry, uint64, error) {
	entries := make([]storage.HeaderBatchEntry, 0, 8)
	cursor := tip
	for {
		activeHash, err := s.chainState.Store().GetBlockHashByHeight(cursor.Height)
		if err != nil {
			return nil, 0, err
		}
		cursorHash := consensus.HeaderHash(&cursor.Header)
		if activeHash != nil && *activeHash == cursorHash {
			slices.Reverse(entries)
			return entries, cursor.Height, nil
		}
		entries = append(entries, storage.HeaderBatchEntry{
			Height:    cursor.Height,
			Header:    cursor.Header,
			ChainWork: cursor.ChainWork,
		})
		if parent, ok := batchEntries[cursor.ParentHash]; ok {
			cursor = parent
			continue
		}
		parent, err := s.chainState.Store().GetBlockIndex(&cursor.ParentHash)
		if err != nil {
			return nil, 0, err
		}
		if parent == nil {
			return nil, 0, fmt.Errorf("missing parent entry for promoted header branch %x", cursor.ParentHash)
		}
		cursor = *parent
	}
}

func (s *Service) applyPeerBlock(block *types.Block) (bool, error) {
	s.stateMu.Lock()
	hash := consensus.HeaderHash(&block.Header)
	entry, err := s.chainState.Store().GetBlockIndex(&hash)
	if err != nil {
		s.stateMu.Unlock()
		return false, err
	}
	if entry == nil {
		s.stateMu.Unlock()
		return false, fmt.Errorf("cannot apply block %x without indexed header", hash)
	}
	if _, err := s.chainState.ApplyBlock(block); err != nil {
		s.stateMu.Unlock()
		return false, err
	}
	s.stateMu.Unlock()
	s.pool.RemoveConfirmed(block)
	s.cacheRecentBlock(*block)
	return true, nil
}

func (s *Service) acceptPeerBlockMessage(peer *peerConn, block *types.Block) error {
	hash := consensus.HeaderHash(&block.Header)
	applied, err := s.applyPeerBlock(block)
	if err != nil {
		return s.handlePeerBlockAcceptanceError(peer, block, err)
	}
	s.releaseBlockRequest(hash)
	if applied {
		peer.noteHeight(s.blockHeight())
		peer.noteUsefulBlocks(1, time.Now())
		s.invalidateBlockTemplate("peer_block")
		s.logger.Info("applied peer block", slog.String("addr", peer.addr), slog.Uint64("block_height", s.blockHeight()))
		s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	}
	return s.requestBlocks(peer)
}

func (s *Service) handlePeerTxAdmissionErrors(peer *peerConn, errs []error) error {
	for _, err := range errs {
		if err == nil {
			continue
		}
		if isBenignPeerTxAdmissionError(err) {
			s.logger.Debug("ignoring non-fatal peer tx admission error",
				slog.String("addr", peer.addr),
				slog.Any("error", err),
			)
			continue
		}
		return err
	}
	return nil
}

func isBenignPeerTxAdmissionError(err error) bool {
	return errors.Is(err, mempool.ErrTxAlreadyExists) ||
		errors.Is(err, mempool.ErrInputAlreadySpent) ||
		errors.Is(err, mempool.ErrRelayFeeTooLow) ||
		errors.Is(err, mempool.ErrTooManyAncestors) ||
		errors.Is(err, mempool.ErrTooManyDescendants) ||
		errors.Is(err, mempool.ErrTxTooLarge)
}

func countAcceptedAdmissions(admissions []mempool.Admission) int {
	total := 0
	for _, admission := range admissions {
		total += len(admission.Accepted)
	}
	return total
}

func (s *Service) handlePeerBlockAcceptanceError(peer *peerConn, block *types.Block, err error) error {
	hash := consensus.HeaderHash(&block.Header)
	switch {
	case errors.Is(err, ErrBlockAlreadyKnown):
		s.releaseBlockRequest(hash)
		s.logger.Debug("ignoring duplicate peer block",
			slog.String("addr", peer.addr),
			slog.String("hash", fmt.Sprintf("%x", hash)),
		)
		return s.requestBlocks(peer)
	case errors.Is(err, ErrUnknownParent):
		s.logger.Debug("peer block arrived before parent header",
			slog.String("addr", peer.addr),
			slog.String("hash", fmt.Sprintf("%x", hash)),
			slog.String("parent", fmt.Sprintf("%x", block.Header.PrevBlockHash)),
		)
		return s.requestHeaders(peer, hash)
	case errors.Is(err, ErrParentStateUnavailable) || strings.Contains(err.Error(), "without indexed header"):
		s.logger.Debug("peer block requires catch-up before apply",
			slog.String("addr", peer.addr),
			slog.String("hash", fmt.Sprintf("%x", hash)),
			slog.Any("error", err),
		)
		if syncErr := s.requestHeaders(peer, hash); syncErr != nil {
			return syncErr
		}
		return s.requestBlocks(peer)
	default:
		return err
	}
}

func (s *Service) peerCount() int {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return len(s.peers)
}

func (s *Service) PeerCount() int {
	return s.peerCount()
}

func (s *Service) blockHeight() uint64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if tip := s.chainState.ChainState().TipHeight(); tip != nil {
		return *tip
	}
	return 0
}

func (s *Service) BlockHeight() uint64 {
	return s.blockHeight()
}

func (s *Service) headerHeight() uint64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if s.headerChain == nil {
		return 0
	}
	if tip := s.headerChain.TipHeight(); tip != nil {
		return *tip
	}
	return 0
}

func (s *Service) HeaderHeight() uint64 {
	return s.headerHeight()
}

func (s *Service) MempoolCount() int {
	return s.pool.Count()
}

func (s *Service) OrphanCount() int {
	return s.pool.OrphanCount()
}

func (s *Service) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && s.publicPage && s.isPublicDashboardPath(r.URL.Path) {
		s.handlePublicDashboard(w, r)
		return
	}
	if r.Method == http.MethodPost {
		s.handleRPC(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *Service) isPublicDashboardPath(path string) bool {
	if path == "/" {
		return true
	}
	return strings.HasPrefix(path, "/block/") || strings.HasPrefix(path, "/tx/")
}

func (s *Service) handlePublicDashboard(w http.ResponseWriter, r *http.Request) {
	body, renderedAt, status, err := s.cachedDashboardHTML(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=us-ascii")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("Last-Modified", renderedAt.UTC().Format(http.TimeFormat))
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.WriteString(w, body)
}

func (s *Service) cachedDashboardHTML(path string) (string, time.Time, int, error) {
	s.dashboard.mu.Lock()
	defer s.dashboard.mu.Unlock()
	if s.dashboard.view == nil || time.Since(s.dashboard.renderedAt) >= time.Minute {
		view, err := s.buildPublicDashboardView()
		if err != nil {
			return "", time.Time{}, http.StatusInternalServerError, err
		}
		s.dashboard.view = view
		s.dashboard.pages = make(map[string]string)
		s.dashboard.renderedAt = time.Now()
	}
	if body, ok := s.dashboard.pages[path]; ok {
		return body, s.dashboard.renderedAt, http.StatusOK, nil
	}
	body, status := renderPublicDashboardPage(s.dashboard.view, path)
	if status == http.StatusOK {
		s.dashboard.pages[path] = body
	}
	return body, s.dashboard.renderedAt, status, nil
}

func (s *Service) dashboardSystemLoop() {
	ticker := time.NewTicker(dashboardSystemSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := s.recordDashboardSystemSample(); err != nil {
				s.logger.Debug("dashboard system sampler failed", slog.Any("error", err))
			}
		}
	}
}

func (s *Service) recordDashboardSystemSample() error {
	sample, err := readDashboardSystemSample(time.Now())
	if err != nil {
		return err
	}
	s.systemStats.record(sample)
	return nil
}

func (s *Service) buildPublicDashboardView() (*publicDashboardView, error) {
	now := time.Now()
	info := s.Info()
	peers := s.PeerInfo()
	relay := s.RelayPeerStats()
	template := s.BlockTemplateStats()
	system := s.systemStats.summary(now, dashboardSystemWindow)
	health := s.dashboardHealth(info, peers)
	blockWindow, err := s.dashboardBlockWindow(now, 6, 6, 128)
	if err != nil {
		return nil, err
	}
	blocks := blockWindow.recent
	if len(blocks) > 5 {
		blocks = blocks[:5]
	}
	mempoolEntries := s.pool.Snapshot()
	pow := s.dashboardPowSummary(blockWindow.recent)
	fees := s.dashboardFeeSummary(blockWindow.recent, mempoolEntries, pow)
	mining := s.dashboardMiningSummary(blocks)
	tpsChart := s.dashboardTPSChartFromBlocks(now, blockWindow.chart)
	mempool := s.dashboardMempoolSummary(mempoolEntries, fees.Clear.Time)
	return &publicDashboardView{
		generatedAt: now,
		nodeID:      s.nodeID,
		createdAt:   s.startedAt,
		info:        info,
		health:      health,
		system:      system,
		blocks:      blocks,
		peers:       peers,
		relay:       relay,
		template:    template,
		pow:         pow,
		fees:        fees,
		mining:      mining,
		mempool:     mempool,
		tpsChart:    tpsChart,
	}, nil
}

func (s *Service) dashboardBlockWindow(now time.Time, recentLimit int, previewLimit int, chartLimit int) (dashboardBlockWindow, error) {
	out := dashboardBlockWindow{}
	if recentLimit <= 0 && chartLimit <= 0 {
		return out, nil
	}
	chartCutoff := now.Add(-time.Hour)
	s.stateMu.RLock()
	tip := s.chainState.ChainState().TipHeight()
	s.stateMu.RUnlock()
	if tip == nil {
		return out, nil
	}
	maxScan := chartLimit
	if maxScan < recentLimit {
		maxScan = recentLimit
	}
	out.recent = make([]dashboardBlockPage, 0, recentLimit)
	out.chart = make([]dashboardBlockPage, 0, maxScan)
	for height := *tip + 1; height > 0; {
		height--
		pagePreviewLimit := 0
		if len(out.recent) < recentLimit {
			pagePreviewLimit = previewLimit
		}
		page, err := s.dashboardBlockPageAt(height, pagePreviewLimit)
		if err != nil {
			return dashboardBlockWindow{}, err
		}
		if len(out.recent) < recentLimit {
			out.recent = append(out.recent, page)
		}
		if len(out.chart) < chartLimit {
			out.chart = append(out.chart, page)
		}
		chartSatisfied := len(out.chart) >= chartLimit || (page.Timestamp.Before(chartCutoff) && len(out.chart) != 0)
		if len(out.recent) >= recentLimit && chartSatisfied {
			break
		}
		if height == 0 {
			break
		}
	}
	return out, nil
}

func (s *Service) dashboardBlockPageAt(height uint64, previewLimit int) (dashboardBlockPage, error) {
	hash, err := s.chainState.Store().GetBlockHashByHeight(height)
	if err != nil {
		return dashboardBlockPage{}, err
	}
	if hash == nil {
		return dashboardBlockPage{}, fmt.Errorf("missing block hash at height %d", height)
	}
	block, err := s.chainState.Store().GetBlock(hash)
	if err != nil {
		return dashboardBlockPage{}, err
	}
	if block == nil {
		return dashboardBlockPage{}, fmt.Errorf("missing block at height %d", height)
	}
	undo, err := s.chainState.Store().GetUndo(hash)
	if err != nil {
		return dashboardBlockPage{}, err
	}
	page := dashboardBlockPage{
		Height:      height,
		Hash:        *hash,
		PrevHash:    block.Header.PrevBlockHash,
		Timestamp:   time.Unix(int64(block.Header.Timestamp), 0).UTC(),
		NBits:       block.Header.NBits,
		TxRoot:      block.Header.MerkleTxIDRoot,
		AuthRoot:    block.Header.MerkleAuthRoot,
		UTXORoot:    block.Header.UTXORoot,
		Size:        len(block.Encode()),
		TxCount:     len(block.Txs),
		MinedByNode: blockMinedByKeyHash(block, s.cfg.MinerKeyHash),
	}
	fees := make([]uint64, 0, len(block.Txs))
	undoIndex := 0
	for i, tx := range block.Txs {
		txPage := dashboardTxPage{
			BlockHeight: height,
			BlockHash:   *hash,
			Timestamp:   page.Timestamp,
			TxID:        consensus.TxID(&tx),
			Coinbase:    i == 0,
			Size:        len(tx.Encode()),
			AuthCount:   len(tx.Auth.Entries),
		}
		for _, output := range tx.Base.Outputs {
			txPage.OutputSum += output.ValueAtoms
		}
		txPage.Outputs = make([]dashboardTxOutput, 0, len(tx.Base.Outputs))
		for idx, output := range tx.Base.Outputs {
			txPage.Outputs = append(txPage.Outputs, dashboardTxOutput{
				Index:   idx,
				Amount:  output.ValueAtoms,
				KeyHash: output.KeyHash,
			})
		}
		if !txPage.Coinbase {
			txPage.Inputs = make([]dashboardTxInput, 0, len(tx.Base.Inputs))
			for _, input := range tx.Base.Inputs {
				if undoIndex >= len(undo) {
					return dashboardBlockPage{}, fmt.Errorf("missing undo entry for block %x", *hash)
				}
				entry := undo[undoIndex]
				undoIndex++
				txPage.InputSum += entry.Entry.ValueAtoms
				txPage.Inputs = append(txPage.Inputs, dashboardTxInput{
					PrevOut: input.PrevOut,
					Amount:  entry.Entry.ValueAtoms,
				})
			}
			txPage.Fee = txPage.InputSum - txPage.OutputSum
			if txPage.Size > 0 {
				txPage.FeeRate = txPage.Fee / uint64(txPage.Size)
			}
			page.TotalFees += txPage.Fee
			fees = append(fees, txPage.Fee)
		}
		if len(page.PreviewTxs) < previewLimit {
			page.PreviewTxs = append(page.PreviewTxs, txPage)
		}
	}
	if len(block.Txs) > len(page.PreviewTxs) {
		page.HiddenTxCount = len(block.Txs) - len(page.PreviewTxs)
	}
	page.MedianFee, page.LowFee, page.HighFee = summarizeFeeSet(fees)
	return page, nil
}

func (s *Service) dashboardPowSummary(blocks []dashboardBlockPage) dashboardPowSummary {
	params := consensus.ParamsForProfile(s.cfg.Profile)
	summary := dashboardPowSummary{
		Algorithm:     "ASERT per-block",
		TargetSpacing: time.Duration(params.TargetSpacingSecs) * time.Second,
	}
	s.stateMu.RLock()
	height := s.chainState.ChainState().TipHeight()
	header := s.chainState.ChainState().TipHeader()
	s.stateMu.RUnlock()
	if height == nil || header == nil {
		return summary
	}
	summary.CurrentBits = header.NBits
	nextBits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: *height, Header: *header}, params)
	if err == nil {
		summary.NextBits = nextBits
	}
	summary.Difficulty = dashboardDifficulty(header.NBits, params.PowLimitBits)
	summary.LastBlockTimestamp = time.Unix(int64(header.Timestamp), 0).UTC()
	if len(blocks) >= 2 {
		var total time.Duration
		var count int
		for i := 0; i < len(blocks)-1; i++ {
			delta := blocks[i].Timestamp.Sub(blocks[i+1].Timestamp)
			if delta > 0 {
				total += delta
				count++
			}
		}
		if count > 0 {
			summary.AvgBlockInterval = total / time.Duration(count)
			summary.HasObservedGap = true
		}
	}
	if summary.AvgBlockInterval <= 0 {
		summary.AvgBlockInterval = summary.TargetSpacing
	}
	return summary
}

func (s *Service) dashboardFeeSummary(blocks []dashboardBlockPage, entries []mempool.SnapshotEntry, pow dashboardPowSummary) dashboardFeeSummary {
	out := dashboardFeeSummary{
		Recent: make([]dashboardBlockFeeLine, 0, len(blocks)),
	}
	var avgTxs float64
	var txBlocks int
	for _, block := range blocks {
		out.Recent = append(out.Recent, dashboardBlockFeeLine{
			Height:    block.Height,
			MedianFee: block.MedianFee,
			LowFee:    block.LowFee,
			HighFee:   block.HighFee,
		})
		if block.TxCount > 1 {
			avgTxs += float64(block.TxCount - 1)
			txBlocks++
		}
	}
	if txBlocks == 0 {
		out.Clear.Blocks = 1
		out.Clear.Time = pow.TargetSpacing
		return out
	}
	avgPerBlock := avgTxs / float64(txBlocks)
	neededBlocks := 1
	if len(entries) > 0 {
		neededBlocks = int(math.Ceil(float64(len(entries)) / avgPerBlock))
		if neededBlocks < 1 {
			neededBlocks = 1
		}
	}
	out.Clear.Blocks = neededBlocks
	out.Clear.Time = time.Duration(neededBlocks) * pow.AvgBlockInterval
	return out
}

func (s *Service) dashboardMiningSummary(blocks []dashboardBlockPage) dashboardMiningSummary {
	out := dashboardMiningSummary{
		Enabled: s.cfg.MinerEnabled,
		Workers: s.cfg.MinerWorkers,
	}
	if !out.Enabled {
		return out
	}
	for _, block := range blocks {
		if !block.MinedByNode {
			continue
		}
		out.RecentHeights = append(out.RecentHeights, block.Height)
		out.RecentHashes = append(out.RecentHashes, block.Hash)
	}
	return out
}

func (s *Service) dashboardMempoolSummary(entries []mempool.SnapshotEntry, clearEstimate time.Duration) dashboardMempoolSummary {
	out := dashboardMempoolSummary{
		Count:         len(entries),
		Orphans:       s.OrphanCount(),
		EstimatedNext: clearEstimate,
	}
	if len(entries) == 0 {
		return out
	}
	slices.SortFunc(entries, func(a, b mempool.SnapshotEntry) int {
		switch {
		case a.Fee > b.Fee:
			return -1
		case a.Fee < b.Fee:
			return 1
		default:
			return 0
		}
	})
	out.HighFee = entries[0].Fee
	out.LowFee = entries[len(entries)-1].Fee
	fees := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		fees = append(fees, entry.Fee)
	}
	out.MedianFee, _, _ = summarizeFeeSet(fees)
	out.Top = append([]mempool.SnapshotEntry(nil), entries...)
	if len(out.Top) > 8 {
		out.Top = out.Top[:8]
	}
	return out
}

func (s *Service) dashboardTPSChartFromBlocks(now time.Time, blocks []dashboardBlockPage) dashboardTPSChart {
	cutoff := now.Add(-time.Hour)
	start := cutoff
	if len(blocks) != 0 && blocks[len(blocks)-1].Timestamp.After(start) {
		start = blocks[len(blocks)-1].Timestamp
	}
	span := now.Sub(start)
	if span < time.Minute {
		span = time.Minute
	}
	bucketCount := 12
	bucketWidth := span / time.Duration(bucketCount)
	if bucketWidth <= 0 {
		bucketWidth = time.Minute
	}
	chart := dashboardTPSChart{
		Label:     fmt.Sprintf("TPS last %s", formatDashboardWindow(span)),
		Buckets:   make([]float64, bucketCount),
		BucketEnd: make([]time.Time, bucketCount),
	}
	for i := range chart.BucketEnd {
		chart.BucketEnd[i] = start.Add(bucketWidth * time.Duration(i+1))
	}
	for _, block := range blocks {
		if block.Timestamp.Before(start) {
			continue
		}
		idx := int(block.Timestamp.Sub(start) / bucketWidth)
		if idx >= bucketCount {
			idx = bucketCount - 1
		}
		if idx < 0 {
			idx = 0
		}
		txs := float64(maxInt(block.TxCount-1, 0))
		chart.Buckets[idx] += txs / bucketWidth.Seconds()
		if chart.Buckets[idx] > chart.MaxTPS {
			chart.MaxTPS = chart.Buckets[idx]
		}
	}
	return chart
}

func (s *Service) dashboardHealth(info ServiceInfo, peers []PeerInfo) string {
	switch {
	case info.HeaderHeight > info.TipHeight:
		return fmt.Sprintf("CATCHING-UP (%d headers ahead)", info.HeaderHeight-info.TipHeight)
	case info.P2PAddr != "" && len(peers) == 0:
		return "LONELY-BUT-RUNNING"
	default:
		return "HEALTHY"
	}
}

func (s *Service) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	if !s.authorizeRPC(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body := r.Body
	if s.cfg.RPCMaxBodyBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, int64(s.cfg.RPCMaxBodyBytes))
	}
	var req rpcRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		s.logger.Warn("rpc decode failed", slog.String("remote_addr", r.RemoteAddr), slog.Any("error", err))
		_ = json.NewEncoder(w).Encode(rpcResponse{Error: err.Error()})
		return
	}
	s.logger.Debug("rpc request", slog.String("remote_addr", r.RemoteAddr), slog.String("method", req.Method))
	result, err := s.dispatchRPC(req)
	resp := rpcResponse{Result: result}
	if err != nil {
		s.logger.Warn("rpc request failed", slog.String("method", req.Method), slog.Any("error", err))
		resp.Error = err.Error()
		resp.Result = nil
	} else {
		s.logger.Debug("rpc request completed", slog.String("method", req.Method))
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Service) dispatchRPC(req rpcRequest) (any, error) {
	switch req.Method {
	case "getinfo":
		return s.Info(), nil
	case "getchainstate":
		return s.ChainStateInfo(), nil
	case "getmempoolinfo":
		return s.MempoolInfo(), nil
	case "getmininginfo":
		return s.MiningInfo(), nil
	case "getpeerinfo":
		return s.PeerInfo(), nil
	case "getheader":
		var params struct {
			Hash string `json:"hash"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		entry, err := s.blockIndexByHashHex(params.Hash)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"height": entry.Height,
			"hash":   params.Hash,
			"nbits":  entry.Header.NBits,
		}, nil
	case "getblockhashbyheight":
		var params struct {
			Height uint64 `json:"height"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		entry, err := s.blockIndexByHeight(params.Height)
		if err != nil {
			return nil, err
		}
		hash := consensus.HeaderHash(&entry.Header)
		return map[string]any{
			"height": params.Height,
			"hash":   hex.EncodeToString(hash[:]),
		}, nil
	case "getblock":
		var params struct {
			Hash string `json:"hash"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		block, err := s.blockByHashHex(params.Hash)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"hash":   params.Hash,
			"txs":    len(block.Txs),
			"header": hex.EncodeToString(block.Header.Encode()),
		}, nil
	case "getmempool":
		entries := s.pool.Snapshot()
		out := make([]string, 0, len(entries))
		for _, entry := range entries {
			txid := consensus.TxID(&entry.Tx)
			out = append(out, hex.EncodeToString(txid[:]))
		}
		return out, nil
	case "getutxosbykeyhashes":
		var params struct {
			KeyHashes []string `json:"keyhashes"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		keyHashes := make([][32]byte, 0, len(params.KeyHashes))
		for _, raw := range params.KeyHashes {
			keyHash, err := ParseMinerKeyHash(raw)
			if err != nil {
				return nil, err
			}
			keyHashes = append(keyHashes, keyHash)
		}
		utxos := s.UTXOsByKeyHashes(keyHashes)
		out := make([]map[string]any, 0, len(utxos))
		for _, utxo := range utxos {
			out = append(out, map[string]any{
				"txid":    hex.EncodeToString(utxo.OutPoint.TxID[:]),
				"vout":    utxo.OutPoint.Vout,
				"value":   utxo.Value,
				"keyhash": hex.EncodeToString(utxo.KeyHash[:]),
			})
		}
		return map[string]any{"utxos": out, "count": len(out)}, nil
	case "getwalletactivitybykeyhashes":
		var params struct {
			KeyHashes []string `json:"keyhashes"`
			Limit     int      `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		keyHashes := make([][32]byte, 0, len(params.KeyHashes))
		for _, raw := range params.KeyHashes {
			keyHash, err := ParseMinerKeyHash(raw)
			if err != nil {
				return nil, err
			}
			keyHashes = append(keyHashes, keyHash)
		}
		activity, err := s.WalletActivityByKeyHashes(keyHashes, params.Limit)
		if err != nil {
			return nil, err
		}
		out := make([]map[string]any, 0, len(activity))
		for _, item := range activity {
			out = append(out, map[string]any{
				"txid":       hex.EncodeToString(item.TxID[:]),
				"block_hash": hex.EncodeToString(item.BlockHash[:]),
				"height":     item.Height,
				"timestamp":  item.Timestamp.Format(time.RFC3339),
				"coinbase":   item.Coinbase,
				"received":   item.Received,
				"sent":       item.Sent,
				"fee":        item.Fee,
				"net":        item.Net,
			})
		}
		return map[string]any{"activity": out, "count": len(out)}, nil
	case "estimatefee":
		var params struct {
			TargetBlocks int `json:"target_blocks"`
		}
		if len(req.Params) != 0 {
			if err := json.Unmarshal(req.Params, &params); err != nil {
				return nil, err
			}
		}
		feeRate := s.EstimateFeeRate(params.TargetBlocks)
		return map[string]any{
			"target_blocks": params.TargetBlocks,
			"fee_per_byte":  feeRate,
		}, nil
	case "submittx":
		var params struct {
			Hex string `json:"hex"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, err := consensus.DecodeTxHex(params.Hex, types.DefaultCodecLimits())
		if err != nil {
			return nil, err
		}
		admission, err := s.SubmitTx(tx)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"txid":            hex.EncodeToString(admission.TxID[:]),
			"fee":             admission.Summary.Fee,
			"orphaned":        admission.Orphaned,
			"accepted_txs":    len(admission.Accepted),
			"evicted_orphans": admission.EvictedOrphans,
		}, nil
	case "submittxbatch":
		var params struct {
			Hexes []string `json:"hexes"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		decodeStarted := time.Now()
		txs := make([]types.Transaction, 0, len(params.Hexes))
		for _, raw := range params.Hexes {
			tx, err := consensus.DecodeTxHex(raw, types.DefaultCodecLimits())
			if err != nil {
				return nil, err
			}
			txs = append(txs, tx)
		}
		decodeDuration := time.Since(decodeStarted)
		admitStarted := time.Now()
		admissions, errs, orphanCount, mempoolSize := s.submitDecodedTxs(txs)
		validateAdmitDuration := time.Since(admitStarted)
		results := make([]map[string]any, 0, len(txs))
		accepted := 0
		for i := range txs {
			if errs[i] != nil {
				results = append(results, map[string]any{
					"error": errs[i].Error(),
				})
				continue
			}
			admission := admissions[i]
			if !admission.Orphaned {
				accepted++
			}
			results = append(results, map[string]any{
				"txid":            hex.EncodeToString(admission.TxID[:]),
				"fee":             admission.Summary.Fee,
				"orphaned":        admission.Orphaned,
				"accepted_txs":    len(admission.Accepted),
				"evicted_orphans": admission.EvictedOrphans,
			})
		}
		s.logger.Debug("transaction batch processed",
			slog.Int("submitted", len(txs)),
			slog.Int("accepted", accepted),
			slog.Int("orphan_count", orphanCount),
			slog.Int("mempool_size", mempoolSize),
		)
		return map[string]any{
			"results":                    results,
			"submitted":                  len(txs),
			"accepted":                   accepted,
			"orphan_count":               orphanCount,
			"mempool_size":               mempoolSize,
			"decode_duration_ms":         float64(decodeDuration.Microseconds()) / 1000,
			"validate_admit_duration_ms": float64(validateAdmitDuration.Microseconds()) / 1000,
		}, nil
	case "submitpackedtxbatch":
		if !s.cfg.Profile.IsRegtestLike() {
			return nil, errors.New("submitpackedtxbatch is only available on regtest-style profiles")
		}
		var params struct {
			Packed string `json:"packed"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		decodeStarted := time.Now()
		txs, err := decodePackedTransactions(params.Packed)
		if err != nil {
			return nil, err
		}
		decodeDuration := time.Since(decodeStarted)
		admitStarted := time.Now()
		admissions, errs, orphanCount, mempoolSize := s.submitDecodedTxs(txs)
		validateAdmitDuration := time.Since(admitStarted)
		results := make([]map[string]any, 0, len(txs))
		accepted := 0
		for i := range txs {
			if errs[i] != nil {
				results = append(results, map[string]any{
					"error": errs[i].Error(),
				})
				continue
			}
			admission := admissions[i]
			if !admission.Orphaned {
				accepted++
			}
			results = append(results, map[string]any{
				"txid":            hex.EncodeToString(admission.TxID[:]),
				"fee":             admission.Summary.Fee,
				"orphaned":        admission.Orphaned,
				"accepted_txs":    len(admission.Accepted),
				"evicted_orphans": admission.EvictedOrphans,
			})
		}
		s.logger.Debug("packed transaction batch processed",
			slog.Int("submitted", len(txs)),
			slog.Int("accepted", accepted),
			slog.Int("orphan_count", orphanCount),
			slog.Int("mempool_size", mempoolSize),
		)
		return map[string]any{
			"results":                    results,
			"submitted":                  len(txs),
			"accepted":                   accepted,
			"orphan_count":               orphanCount,
			"mempool_size":               mempoolSize,
			"decode_duration_ms":         float64(decodeDuration.Microseconds()) / 1000,
			"validate_admit_duration_ms": float64(validateAdmitDuration.Microseconds()) / 1000,
		}, nil
	case "mine":
		var params struct {
			Count int `json:"count"`
		}
		if len(req.Params) == 0 {
			params.Count = 1
		} else if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Count <= 0 {
			params.Count = 1
		}
		return s.MineBlocks(params.Count)
	case "seedstresslanes":
		if !s.cfg.Profile.IsRegtestLike() {
			return nil, errors.New("seedstresslanes is only available on regtest-style profiles")
		}
		var params struct {
			KeyHashes []string `json:"keyhashes"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if len(params.KeyHashes) == 0 {
			return nil, errors.New("keyhashes is required")
		}
		keyHashes := make([][32]byte, 0, len(params.KeyHashes))
		for _, raw := range params.KeyHashes {
			keyHash, err := ParseMinerKeyHash(raw)
			if err != nil {
				return nil, err
			}
			keyHashes = append(keyHashes, keyHash)
		}
		outputs, err := s.MineFundingOutputs(keyHashes)
		if err != nil {
			return nil, err
		}
		result := make([]map[string]any, 0, len(outputs))
		for _, output := range outputs {
			result = append(result, map[string]any{
				"txid":       hex.EncodeToString(output.OutPoint.TxID[:]),
				"vout":       output.OutPoint.Vout,
				"value":      output.Value,
				"keyhash":    hex.EncodeToString(output.KeyHash[:]),
				"block_hash": hex.EncodeToString(output.BlockHash[:]),
			})
		}
		return map[string]any{"outputs": result, "count": len(result)}, nil
	case "submitblock":
		var params struct {
			Hex string `json:"hex"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		block, err := consensus.DecodeBlockHex(params.Hex, types.DefaultCodecLimits())
		if err != nil {
			return nil, err
		}
		applied, err := s.applyPeerBlock(&block)
		if err != nil {
			return nil, err
		}
		if applied {
			hash := consensus.HeaderHash(&block.Header)
			s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
		}
		return map[string]any{"applied": applied}, nil
	case "addpeer":
		var params struct {
			Addr string `json:"addr"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Addr == "" {
			return nil, errors.New("addr is required")
		}
		go func() {
			if err := s.ConnectPeer(params.Addr); err != nil {
				s.logger.Warn("rpc addpeer failed", slog.String("addr", params.Addr), slog.Any("error", err))
			}
		}()
		return map[string]any{"addr": params.Addr}, nil
	case "stop":
		go func() {
			_ = s.Close()
		}()
		return map[string]any{"stopping": true}, nil
	default:
		return nil, fmt.Errorf("unknown rpc method: %s", req.Method)
	}
}

type PeerInfo struct {
	Addr               string `json:"addr"`
	Outbound           bool   `json:"outbound"`
	Height             uint64 `json:"height"`
	UserAgent          string `json:"user_agent"`
	LastProgress       int64  `json:"last_progress_unix"`
	LastUseful         int64  `json:"last_useful_unix,omitempty"`
	PreferredDownload  bool   `json:"preferred_download,omitempty"`
	DownloadScore      int    `json:"download_score,omitempty"`
	HeaderStalls       int    `json:"header_stalls,omitempty"`
	BlockStalls        int    `json:"block_stalls,omitempty"`
	TxStalls           int    `json:"tx_stalls,omitempty"`
	DownloadCooldownMS int64  `json:"download_cooldown_ms,omitempty"`
}

func (s *Service) PeerInfo() []PeerInfo             { return s.peerManager().PeerInfo() }
func (s *Service) RelayPeerStats() []PeerRelayStats { return s.relayManager().RelayPeerStats() }

func (s *Service) BlockTemplateStats() BlockTemplateStats {
	return s.templateStats.snapshot()
}

func (s *Service) requestSync(peer *peerConn) { s.syncManager().requestSync(peer) }
func (s *Service) requestHeaders(peer *peerConn, stopHash [32]byte) error {
	return s.syncManager().requestHeaders(peer, stopHash)
}
func (s *Service) requestBlocks(peer *peerConn) error { return s.syncManager().requestBlocks(peer) }
func (s *Service) blockRequestTimeout() time.Duration { return s.syncManager().blockRequestTimeout() }
func (s *Service) txRequestTimeout() time.Duration    { return s.syncManager().txRequestTimeout() }

func (s *Service) scheduleBlockRequests(peerAddr string, hashes [][32]byte, limit int) [][32]byte {
	return s.syncManager().scheduleBlockRequests(peerAddr, hashes, limit)
}
func (s *Service) releaseBlockRequest(hash [32]byte) { s.syncManager().releaseBlockRequest(hash) }
func (s *Service) releasePeerBlockRequests(addr string) {
	s.syncManager().releasePeerBlockRequests(addr)
}
func (s *Service) scheduleTxInvRequests(peerAddr string, items []p2p.InvVector, limit int) []p2p.InvVector {
	return s.syncManager().scheduleTxInvRequests(peerAddr, items, limit)
}
func (s *Service) releaseTxRequest(hash [32]byte) { s.syncManager().releaseTxRequest(hash) }
func (s *Service) releaseTxRequestsForTransactions(txs []types.Transaction) {
	s.syncManager().releaseTxRequestsForTransactions(txs)
}
func (s *Service) releasePeerTxRequests(addr string) { s.syncManager().releasePeerTxRequests(addr) }
func (s *Service) expireStaleBlockRequests()         { s.syncManager().expireStaleBlockRequests() }
func (s *Service) expireStaleTxRequests()            { s.syncManager().expireStaleTxRequests() }
func (s *Service) inflightBlockRequestCount() int    { return s.syncManager().inflightBlockRequestCount() }
func (s *Service) inflightTxRequestCount() int       { return s.syncManager().inflightTxRequestCount() }
func (s *Service) syncWatchdogLoop()                 { s.syncManager().syncWatchdogLoop() }
func (s *Service) outboundRefillLoop()               { s.peerManager().outboundRefillLoop() }
func (s *Service) refillOutboundPeers()              { s.peerManager().refillOutboundPeers() }

func (s *Service) outboundRefillCandidates(limit int) []string {
	return s.peerManager().outboundRefillCandidates(limit)
}
func (s *Service) runSyncWatchdogStep()              { s.syncManager().runSyncWatchdogStep() }
func (s *Service) syncStallThreshold() time.Duration { return s.syncManager().syncStallThreshold() }
func (s *Service) peerByAddr(addr string) *peerConn  { return s.peerManager().peerByAddr(addr) }

func (s *Service) onInvMessage(peer *peerConn, msg p2p.InvMessage) error {
	txItems := make([]p2p.InvVector, 0, len(msg.Items))
	blockItems := make([]p2p.InvVector, 0, len(msg.Items))
	var stopHash [32]byte
	for _, item := range msg.Items {
		switch item.Type {
		case p2p.InvTypeTx:
			if !s.pool.Contains(item.Hash) {
				txItems = append(txItems, item)
			}
		case p2p.InvTypeBlock:
			if _, ok := s.recentHeader(item.Hash); ok {
				if _, ok := s.recentBlock(item.Hash); !ok {
					blockItems = append(blockItems, p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: item.Hash})
				}
				continue
			}
			entry, err := s.chainState.Store().GetBlockIndex(&item.Hash)
			if err != nil {
				return err
			}
			if entry == nil {
				stopHash = item.Hash
				continue
			}
			block, err := s.chainState.Store().GetBlock(&item.Hash)
			if err != nil {
				return err
			}
			if block == nil {
				blockItems = append(blockItems, p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: item.Hash})
			}
		}
	}
	if stopHash != ([32]byte{}) {
		if err := s.requestHeaders(peer, stopHash); err != nil {
			return err
		}
	}
	scheduledTxItems := s.scheduleTxInvRequests(peer.addr, txItems, blockRequestBatchSize)
	getData := make([]p2p.InvVector, 0, len(scheduledTxItems)+len(blockItems))
	getData = append(getData, scheduledTxItems...)
	getData = append(getData, blockItems...)
	if len(getData) == 0 {
		return nil
	}
	if err := peer.send(p2p.GetDataMessage{Items: getData}); err != nil {
		for _, item := range scheduledTxItems {
			s.releaseTxRequest(item.Hash)
		}
		return err
	}
	return nil
}

func (p *peerConn) noteProgress(at time.Time) {
	p.lastProgress.Store(at.Unix())
}

func (p *peerConn) noteHeight(height uint64) {
	for {
		current := p.bestHeight.Load()
		if height <= current {
			return
		}
		if p.bestHeight.CompareAndSwap(current, height) {
			return
		}
	}
}

func (p *peerConn) snapshotHeight() uint64 {
	if height := p.bestHeight.Load(); height > 0 {
		return height
	}
	return p.version.Height
}

func (p *peerConn) snapshotProgressUnix() int64 {
	if unix := p.lastProgress.Load(); unix > 0 {
		return unix
	}
	return 0
}

func unixTimeOrZero(unix int64) time.Time {
	if unix <= 0 {
		return time.Time{}
	}
	return time.Unix(unix, 0)
}

func defaultDashboardValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func (s *Service) onGetDataMessage(peer *peerConn, msg p2p.GetDataMessage) error {
	notFound := make([]p2p.InvVector, 0)
	send := make([]p2p.Message, 0, len(msg.Items))
	requestedTxIDs := make([][32]byte, 0, len(msg.Items))
	for _, item := range msg.Items {
		switch item.Type {
		case p2p.InvTypeTx:
			requestedTxIDs = append(requestedTxIDs, item.Hash)
		case p2p.InvTypeBlock:
			blockMsg, ok, err := s.preferredBlockRelayMessage(item.Hash)
			if err != nil {
				return err
			}
			if !ok {
				notFound = append(notFound, item)
				continue
			}
			send = append(send, blockMsg)
		case p2p.InvTypeBlockFull:
			block, ok, err := s.loadBlock(item.Hash)
			if err != nil {
				return err
			}
			if !ok {
				notFound = append(notFound, item)
				continue
			}
			send = append(send, p2p.BlockMessage{Block: block})
		default:
			notFound = append(notFound, item)
		}
	}
	txs := s.pool.TransactionsByID(requestedTxIDs)
	if len(txs) != len(requestedTxIDs) {
		found := make(map[[32]byte]struct{}, len(txs))
		for _, tx := range txs {
			found[consensus.TxID(&tx)] = struct{}{}
		}
		for _, txid := range requestedTxIDs {
			if _, ok := found[txid]; ok {
				continue
			}
			notFound = append(notFound, p2p.InvVector{Type: p2p.InvTypeTx, Hash: txid})
		}
	}
	for start := 0; start < len(txs); start += 64 {
		end := start + 64
		if end > len(txs) {
			end = len(txs)
		}
		send = append(send, p2p.TxBatchMessage{Txs: append([]types.Transaction(nil), txs[start:end]...)})
	}
	for _, msg := range send {
		if err := peer.send(msg); err != nil {
			return err
		}
	}
	if len(notFound) == 0 {
		return nil
	}
	return peer.send(p2p.NotFoundMessage{Items: notFound})
}

func (s *Service) onNotFoundMessage(peer *peerConn, msg p2p.NotFoundMessage) error {
	return s.syncManager().onNotFoundMessage(peer, msg)
}

func (s *Service) onXThinBlockMessage(peer *peerConn, msg p2p.XThinBlockMessage) error {
	matches := s.pool.ShortIDMatches(func(txid [32]byte) uint64 {
		return thinBlockShortID(msg.Nonce, txid)
	}, xThinShortIDSet(msg))
	state, missing := reconstructXThinBlock(msg, matches)
	if len(missing) == 0 {
		peer.deletePendingThin(state.hash)
		if err := s.acceptThinBlock(peer, state.block()); err != nil {
			return s.requestFullBlock(peer, state.hash)
		}
		return nil
	}
	if shouldFallbackToFullBlock(state, missing) {
		peer.deletePendingThin(state.hash)
		return s.requestFullBlock(peer, state.hash)
	}
	peer.storePendingThin(state)
	return peer.send(p2p.GetXBlockTxMessage{BlockHash: state.hash, Indexes: missing})
}

func (s *Service) onCompactBlockMessage(peer *peerConn, msg p2p.CompactBlockMessage) error {
	matches := s.pool.ShortIDMatches(func(txid [32]byte) uint64 {
		return thinBlockShortID(msg.Nonce, txid)
	}, compactShortIDSet(msg))
	state, missing := reconstructCompactBlock(msg, matches)
	if len(missing) == 0 {
		peer.deletePendingThin(state.hash)
		if err := s.acceptThinBlock(peer, state.block()); err != nil {
			return s.requestFullBlock(peer, state.hash)
		}
		return nil
	}
	if shouldFallbackToFullBlock(state, missing) {
		peer.deletePendingThin(state.hash)
		return s.requestFullBlock(peer, state.hash)
	}
	peer.storePendingThin(state)
	return peer.send(p2p.GetBlockTxMessage{BlockHash: state.hash, Indexes: missing})
}

func (s *Service) onGetXBlockTxMessage(peer *peerConn, msg p2p.GetXBlockTxMessage) error {
	block, ok, err := s.loadBlock(msg.BlockHash)
	if err != nil {
		return err
	}
	if !ok {
		return peer.send(p2p.NotFoundMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlockFull, Hash: msg.BlockHash}}})
	}
	indexes := make([]uint32, 0, len(msg.Indexes))
	txs := make([]types.Transaction, 0, len(msg.Indexes))
	for _, index := range msg.Indexes {
		if int(index) >= len(block.Txs) {
			continue
		}
		indexes = append(indexes, index)
		txs = append(txs, block.Txs[index])
	}
	if len(indexes) == 0 {
		return peer.send(p2p.NotFoundMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlockFull, Hash: msg.BlockHash}}})
	}
	return peer.send(p2p.XBlockTxMessage{BlockHash: msg.BlockHash, Indexes: indexes, Txs: txs})
}

func (s *Service) onGetBlockTxMessage(peer *peerConn, msg p2p.GetBlockTxMessage) error {
	block, ok, err := s.loadBlock(msg.BlockHash)
	if err != nil {
		return err
	}
	if !ok {
		return peer.send(p2p.NotFoundMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlockFull, Hash: msg.BlockHash}}})
	}
	indexes := make([]uint32, 0, len(msg.Indexes))
	txs := make([]types.Transaction, 0, len(msg.Indexes))
	for _, index := range msg.Indexes {
		if int(index) >= len(block.Txs) {
			continue
		}
		indexes = append(indexes, index)
		txs = append(txs, block.Txs[index])
	}
	if len(indexes) == 0 {
		return peer.send(p2p.NotFoundMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlockFull, Hash: msg.BlockHash}}})
	}
	return peer.send(p2p.BlockTxMessage{BlockHash: msg.BlockHash, Indexes: indexes, Txs: txs})
}

func (s *Service) onXBlockTxMessage(peer *peerConn, msg p2p.XBlockTxMessage) error {
	state, ok := peer.pendingThinState(msg.BlockHash)
	if !ok {
		return nil
	}
	if len(msg.Indexes) != len(msg.Txs) {
		peer.deletePendingThin(msg.BlockHash)
		return s.requestFullBlock(peer, msg.BlockHash)
	}
	for i, index := range msg.Indexes {
		if !state.fill(index, msg.Txs[i]) {
			peer.deletePendingThin(msg.BlockHash)
			return s.requestFullBlock(peer, msg.BlockHash)
		}
	}
	if !state.complete() {
		peer.deletePendingThin(msg.BlockHash)
		return s.requestFullBlock(peer, msg.BlockHash)
	}
	peer.deletePendingThin(msg.BlockHash)
	if err := s.acceptThinBlock(peer, state.block()); err != nil {
		return s.requestFullBlock(peer, msg.BlockHash)
	}
	return nil
}

func (s *Service) onBlockTxMessage(peer *peerConn, msg p2p.BlockTxMessage) error {
	state, ok := peer.pendingThinState(msg.BlockHash)
	if !ok {
		return nil
	}
	if len(msg.Indexes) != len(msg.Txs) {
		peer.deletePendingThin(msg.BlockHash)
		return s.requestFullBlock(peer, msg.BlockHash)
	}
	for i, index := range msg.Indexes {
		if !state.fill(index, msg.Txs[i]) {
			peer.deletePendingThin(msg.BlockHash)
			return s.requestFullBlock(peer, msg.BlockHash)
		}
	}
	if !state.complete() {
		peer.deletePendingThin(msg.BlockHash)
		return s.requestFullBlock(peer, msg.BlockHash)
	}
	peer.deletePendingThin(msg.BlockHash)
	if err := s.acceptThinBlock(peer, state.block()); err != nil {
		return s.requestFullBlock(peer, msg.BlockHash)
	}
	return nil
}

func (s *Service) preferredBlockRelayMessage(hash [32]byte) (p2p.Message, bool, error) {
	block, ok, err := s.loadBlock(hash)
	if err != nil || !ok {
		return nil, ok, err
	}
	return buildCompactBlockMessage(block), true, nil
}

func (s *Service) loadBlock(hash [32]byte) (types.Block, bool, error) {
	if block, ok := s.recentBlock(hash); ok {
		return block, true, nil
	}
	block, err := s.chainState.Store().GetBlock(&hash)
	if err != nil {
		return types.Block{}, false, err
	}
	if block == nil {
		return types.Block{}, false, nil
	}
	return *block, true, nil
}

func (s *Service) acceptThinBlock(peer *peerConn, block types.Block) error {
	return s.acceptPeerBlockMessage(peer, &block)
}

func (s *Service) requestFullBlock(peer *peerConn, hash [32]byte) error {
	return peer.send(p2p.GetDataMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlockFull, Hash: hash}}})
}

func (s *Service) onTxReconMessage(peer *peerConn, msg p2p.TxReconMessage) error {
	if len(msg.TxIDs) == 0 {
		return nil
	}
	missing := s.pool.MissingTxIDs(msg.TxIDs)
	if len(missing) == 0 {
		return nil
	}
	return peer.send(p2p.TxRequestMessage{TxIDs: missing})
}

func (s *Service) onTxRequestMessage(peer *peerConn, msg p2p.TxRequestMessage) error {
	if len(msg.TxIDs) == 0 {
		return nil
	}
	txs := s.pool.TransactionsByID(msg.TxIDs)
	for start := 0; start < len(txs); start += 64 {
		end := start + 64
		if end > len(txs) {
			end = len(txs)
		}
		if err := peer.sendRequestedTxBatch(p2p.TxBatchMessage{Txs: append([]types.Transaction(nil), txs[start:end]...)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) broadcastInv(items []p2p.InvVector) { s.relayManager().broadcastInv(items) }
func (s *Service) broadcastInvToPeers(peers []*peerConn, items []p2p.InvVector) {
	s.relayManager().broadcastInvToPeers(peers, items)
}
func (s *Service) broadcastAcceptedTxsToPeers(peers []*peerConn, accepted []mempool.AcceptedTx) {
	s.relayManager().broadcastAcceptedTxsToPeers(peers, accepted)
}

type txRelayReconBatch struct {
	peer  *peerConn
	txids [][32]byte
}

func planTxRelayRecon(peers []*peerConn, txids [][32]byte) []txRelayReconBatch {
	if len(peers) == 0 || len(txids) == 0 {
		return nil
	}
	assignments := assignTxRelayPeers(peers, txids)
	if len(assignments) == 0 {
		return nil
	}
	const txReconChunkSize = 256
	batches := make([]txRelayReconBatch, 0, len(assignments))
	for _, peer := range peers {
		assigned := assignments[peer]
		for start := 0; start < len(assigned); start += txReconChunkSize {
			end := start + txReconChunkSize
			if end > len(assigned) {
				end = len(assigned)
			}
			batches = append(batches, txRelayReconBatch{
				peer:  peer,
				txids: append([][32]byte(nil), assigned[start:end]...),
			})
		}
	}
	return batches
}

func assignTxRelayPeers(peers []*peerConn, txids [][32]byte) map[*peerConn][][32]byte {
	assignments := make(map[*peerConn][][32]byte, len(peers))
	fanout := txRelayFanout(len(peers))
	if fanout >= len(peers) {
		for _, peer := range peers {
			assignments[peer] = append(assignments[peer], txids...)
		}
		return assignments
	}
	for _, txid := range txids {
		for _, peer := range topRelayPeersForTx(peers, txid, fanout) {
			assignments[peer] = append(assignments[peer], txid)
		}
	}
	return assignments
}

func txRelayFanout(peerCount int) int {
	const directRelayFloor = 4
	if peerCount <= directRelayFloor {
		return peerCount
	}
	target := int(math.Ceil(math.Sqrt(float64(peerCount))))
	if target < directRelayFloor {
		target = directRelayFloor
	}
	if target > peerCount {
		target = peerCount
	}
	return target
}

func topRelayPeersForTx(peers []*peerConn, txid [32]byte, target int) []*peerConn {
	if target <= 0 || len(peers) == 0 {
		return nil
	}
	if target >= len(peers) {
		return append([]*peerConn(nil), peers...)
	}
	type scoredPeer struct {
		peer  *peerConn
		score [32]byte
	}
	scored := make([]scoredPeer, 0, len(peers))
	for _, peer := range peers {
		score := relayPeerScore(txid, peer.addr)
		scored = append(scored, scoredPeer{peer: peer, score: score})
	}
	slices.SortFunc(scored, func(a, b scoredPeer) int {
		if cmp := bytes.Compare(a.score[:], b.score[:]); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.peer.addr, b.peer.addr)
	})
	selected := make([]*peerConn, 0, target)
	for _, item := range scored[:target] {
		selected = append(selected, item.peer)
	}
	return selected
}

func relayPeerScore(seed [32]byte, addr string) [32]byte {
	payload := make([]byte, 0, len(seed)+len(addr))
	payload = append(payload, seed[:]...)
	payload = append(payload, addr...)
	return bpcrypto.Sha256d(payload)
}

func (s *Service) peerSnapshot() []*peerConn { return s.peerManager().peerSnapshot() }
func (s *Service) peerSnapshotExcluding(skip *peerConn) []*peerConn {
	return s.peerManager().peerSnapshotExcluding(skip)
}

func classifyRelayMessage(msg p2p.Message) relayMessageClass {
	class := relayMessageClass{}
	switch m := msg.(type) {
	case p2p.InvMessage:
		for _, item := range m.Items {
			switch item.Type {
			case p2p.InvTypeTx:
				class.txInvItems++
			case p2p.InvTypeBlock:
				class.blockInvItems++
			}
		}
	case p2p.TxBatchMessage:
		class.txBatchMsgs = 1
		class.txBatchItems = len(m.Txs)
	case p2p.TxMessage:
		class.txBatchMsgs = 1
		class.txBatchItems = 1
	case p2p.TxReconMessage:
		class.txReconMsgs = 1
		class.txReconItems = len(m.TxIDs)
	case p2p.TxRequestMessage:
		class.txReqMsgs = 1
		class.txReqItems = len(m.TxIDs)
	}
	return class
}

func splitPrioritizedInvItems(items []p2p.InvVector) ([]p2p.InvVector, []p2p.InvVector) {
	if len(items) == 0 {
		return nil, nil
	}
	priority := make([]p2p.InvVector, 0, len(items))
	normal := make([]p2p.InvVector, 0, len(items))
	for _, item := range items {
		if item.Type == p2p.InvTypeBlock {
			priority = append(priority, item)
			continue
		}
		normal = append(normal, item)
	}
	return priority, normal
}

func (p *peerConn) filterQueuedInv(items []p2p.InvVector) []p2p.InvVector {
	p.invMu.Lock()
	defer p.invMu.Unlock()
	filtered := make([]p2p.InvVector, 0, len(items))
	for _, item := range items {
		if p.queuedInv[item] > 0 {
			continue
		}
		p.queuedInv[item]++
		filtered = append(filtered, item)
	}
	return filtered
}

func (p *peerConn) releaseQueuedInv(items []p2p.InvVector) {
	if len(items) == 0 {
		return
	}
	p.invMu.Lock()
	defer p.invMu.Unlock()
	for _, item := range items {
		if remaining := p.queuedInv[item] - 1; remaining > 0 {
			p.queuedInv[item] = remaining
			continue
		}
		delete(p.queuedInv, item)
	}
}

func (p *peerConn) filterQueuedTxs(txs []types.Transaction) []types.Transaction {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	filtered := make([]types.Transaction, 0, len(txs))
	for _, tx := range txs {
		txid := consensus.TxID(&tx)
		if p.queuedTx[txid] > 0 {
			continue
		}
		if _, ok := p.knownTx[txid]; ok {
			continue
		}
		p.queuedTx[txid]++
		filtered = append(filtered, tx)
	}
	return filtered
}

func (p *peerConn) filterQueuedTxIDs(txids [][32]byte) [][32]byte {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	filtered := make([][32]byte, 0, len(txids))
	for _, txid := range txids {
		if p.queuedTx[txid] > 0 {
			continue
		}
		if _, ok := p.knownTx[txid]; ok {
			continue
		}
		p.queuedTx[txid]++
		filtered = append(filtered, txid)
	}
	return filtered
}

func (p *peerConn) releaseQueuedTxs(txs []types.Transaction) {
	if len(txs) == 0 {
		return
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	for _, tx := range txs {
		txid := consensus.TxID(&tx)
		if remaining := p.queuedTx[txid] - 1; remaining > 0 {
			p.queuedTx[txid] = remaining
			continue
		}
		delete(p.queuedTx, txid)
	}
}

func (p *peerConn) releaseQueuedTxIDs(txids [][32]byte) {
	if len(txids) == 0 {
		return
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	for _, txid := range txids {
		if remaining := p.queuedTx[txid] - 1; remaining > 0 {
			p.queuedTx[txid] = remaining
			continue
		}
		delete(p.queuedTx, txid)
	}
}

func (p *peerConn) stagePendingTxs(txs []types.Transaction) ([][]types.Transaction, bool) {
	const maxBatch = 64
	p.txMu.Lock()
	defer p.txMu.Unlock()
	p.pendingTxs = append(p.pendingTxs, txs...)
	ready := make([][]types.Transaction, 0, len(p.pendingTxs)/maxBatch+1)
	for len(p.pendingTxs) >= maxBatch {
		batch := append([]types.Transaction(nil), p.pendingTxs[:maxBatch]...)
		ready = append(ready, batch)
		p.pendingTxs = p.pendingTxs[maxBatch:]
	}
	armFlush := false
	if len(p.pendingTxs) != 0 && !p.txFlushArmed {
		p.txFlushArmed = true
		armFlush = true
	}
	return ready, armFlush
}

func (p *peerConn) stagePendingRecon(txids [][32]byte) ([][][32]byte, bool) {
	const maxBatch = 256
	p.txMu.Lock()
	defer p.txMu.Unlock()
	p.pendingRecon = append(p.pendingRecon, txids...)
	ready := make([][][32]byte, 0, len(p.pendingRecon)/maxBatch+1)
	for len(p.pendingRecon) >= maxBatch {
		batch := append([][32]byte(nil), p.pendingRecon[:maxBatch]...)
		ready = append(ready, batch)
		p.pendingRecon = p.pendingRecon[maxBatch:]
	}
	armFlush := false
	if len(p.pendingRecon) != 0 && !p.reconFlushArmed {
		p.reconFlushArmed = true
		armFlush = true
	}
	return ready, armFlush
}

func (p *peerConn) flushPendingTxsAfterDelay() {
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-p.closed:
		return
	case <-timer.C:
	}
	for _, batch := range p.takePendingTxs() {
		p.enqueueRelayTxs(batch)
	}
}

func (p *peerConn) flushPendingReconAfterDelay() {
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-p.closed:
		return
	case <-timer.C:
	}
	for _, batch := range p.takePendingRecon() {
		p.enqueueRelayRecon(batch)
	}
}

func (p *peerConn) takePendingTxs() [][]types.Transaction {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	defer func() {
		p.txFlushArmed = false
	}()
	if len(p.pendingTxs) == 0 {
		return nil
	}
	const maxBatch = 64
	batches := make([][]types.Transaction, 0, (len(p.pendingTxs)+maxBatch-1)/maxBatch)
	for len(p.pendingTxs) != 0 {
		end := maxBatch
		if end > len(p.pendingTxs) {
			end = len(p.pendingTxs)
		}
		batch := append([]types.Transaction(nil), p.pendingTxs[:end]...)
		batches = append(batches, batch)
		p.pendingTxs = p.pendingTxs[end:]
	}
	return batches
}

func (p *peerConn) takePendingRecon() [][][32]byte {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	defer func() {
		p.reconFlushArmed = false
	}()
	if len(p.pendingRecon) == 0 {
		return nil
	}
	const maxBatch = 256
	batches := make([][][32]byte, 0, (len(p.pendingRecon)+maxBatch-1)/maxBatch)
	for len(p.pendingRecon) != 0 {
		end := maxBatch
		if end > len(p.pendingRecon) {
			end = len(p.pendingRecon)
		}
		batch := append([][32]byte(nil), p.pendingRecon[:end]...)
		batches = append(batches, batch)
		p.pendingRecon = p.pendingRecon[end:]
	}
	return batches
}

func (p *peerConn) enqueueRelayTxs(txs []types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}
	envelope := outboundMessage{
		msg:        p2p.TxBatchMessage{Txs: txs},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.TxBatchMessage{Txs: txs}),
	}
	select {
	case <-p.closed:
		p.releaseQueuedTxs(txs)
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepth())
		return nil
	default:
		p.releaseQueuedTxs(txs)
		p.telemetry.noteDroppedTxs(len(txs))
		return nil
	}
}

func (p *peerConn) enqueueRelayRecon(txids [][32]byte) error {
	if len(txids) == 0 {
		return nil
	}
	envelope := outboundMessage{
		msg:        p2p.TxReconMessage{TxIDs: txids},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.TxReconMessage{TxIDs: txids}),
	}
	select {
	case <-p.closed:
		p.releaseQueuedTxIDs(txids)
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepth())
		return nil
	default:
		p.releaseQueuedTxIDs(txids)
		p.telemetry.noteDroppedTxs(len(txids))
		return nil
	}
}

func (p *peerConn) noteKnownTxs(msg p2p.Message) {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	switch m := msg.(type) {
	case p2p.TxBatchMessage:
		for _, tx := range m.Txs {
			p.rememberKnownTxLocked(consensus.TxID(&tx))
		}
	case p2p.TxMessage:
		p.rememberKnownTxLocked(consensus.TxID(&m.Tx))
	case p2p.TxReconMessage:
		for _, txid := range m.TxIDs {
			p.rememberKnownTxLocked(txid)
		}
	}
}

func (p *peerConn) noteKnownTxIDs(txids [][32]byte) {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	for _, txid := range txids {
		p.rememberKnownTxLocked(txid)
	}
}

func (p *peerConn) rememberKnownTxLocked(txid [32]byte) {
	if _, ok := p.knownTx[txid]; ok {
		return
	}
	p.knownTx[txid] = struct{}{}
	p.knownTxOrder = append(p.knownTxOrder, txid)
	const knownLimit = 8192
	for len(p.knownTxOrder) > knownLimit {
		evict := p.knownTxOrder[0]
		p.knownTxOrder = p.knownTxOrder[1:]
		delete(p.knownTx, evict)
	}
}

func (p *peerConn) releaseRelayBatch(msg p2p.Message) {
	switch m := msg.(type) {
	case p2p.TxBatchMessage:
		p.releaseQueuedTxs(m.Txs)
	case p2p.TxMessage:
		p.releaseQueuedTxs([]types.Transaction{m.Tx})
	case p2p.TxReconMessage:
		p.releaseQueuedTxIDs(m.TxIDs)
	}
}

func (p *peerConn) storePendingThin(state *pendingThinBlock) {
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	p.pendingThin[state.hash] = state
}

func (p *peerConn) pendingThinState(hash [32]byte) (*pendingThinBlock, bool) {
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	state, ok := p.pendingThin[hash]
	return state, ok
}

func (p *peerConn) deletePendingThin(hash [32]byte) {
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	delete(p.pendingThin, hash)
}

func (t *peerRelayTelemetry) noteEnqueue(depth int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if depth > t.maxQueueDepth {
		t.maxQueueDepth = depth
	}
}

func (t *peerRelayTelemetry) noteDroppedInv(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.droppedInv += count
}

func (t *peerRelayTelemetry) noteDroppedTxs(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.droppedTxs += count
}

func (t *peerRelayTelemetry) noteSent(envelope outboundMessage, _ int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sentMessages++
	t.txInvItems += envelope.class.txInvItems
	t.blockInvItems += envelope.class.blockInvItems
	t.txBatchMsgs += envelope.class.txBatchMsgs
	t.txBatchItems += envelope.class.txBatchItems
	t.txReconMsgs += envelope.class.txReconMsgs
	t.txReconItems += envelope.class.txReconItems
	t.txReqMsgs += envelope.class.txReqMsgs
	t.txReqItems += envelope.class.txReqItems
	if envelope.class.txInvItems != 0 || envelope.class.blockInvItems != 0 || envelope.class.txBatchItems != 0 || envelope.class.txReconItems != 0 || envelope.class.txReqItems != 0 {
		t.relaySamples = append(t.relaySamples, float64(time.Since(envelope.enqueuedAt).Microseconds())/1000)
	}
}

func (t *peerRelayTelemetry) snapshot(addr string, outbound bool, queueDepth int) PeerRelayStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := PeerRelayStats{
		Addr:          addr,
		Outbound:      outbound,
		QueueDepth:    queueDepth,
		MaxQueueDepth: t.maxQueueDepth,
		SentMessages:  t.sentMessages,
		TxInvItems:    t.txInvItems,
		BlockInvItems: t.blockInvItems,
		TxBatchMsgs:   t.txBatchMsgs,
		TxBatchItems:  t.txBatchItems,
		TxReconMsgs:   t.txReconMsgs,
		TxReconItems:  t.txReconItems,
		TxReqMsgs:     t.txReqMsgs,
		TxReqItems:    t.txReqItems,
		DroppedInv:    t.droppedInv,
		DroppedTxs:    t.droppedTxs,
		RelayEvents:   len(t.relaySamples),
	}
	if len(t.relaySamples) == 0 {
		return stats
	}
	samples := append([]float64(nil), t.relaySamples...)
	slices.Sort(samples)
	var total float64
	for _, sample := range samples {
		total += sample
	}
	stats.RelayAvgMS = total / float64(len(samples))
	stats.RelayP95MS = samples[(len(samples)-1)*95/100]
	stats.RelayMaxMS = samples[len(samples)-1]
	return stats
}

func (t *templateBuildTelemetry) noteCacheHit(frontierCandidates int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cacheHits++
	if frontierCandidates > t.frontierCandidates {
		t.frontierCandidates = frontierCandidates
	}
}

func (t *templateBuildTelemetry) noteRebuild(frontierCandidates int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rebuilds++
	if frontierCandidates > t.frontierCandidates {
		t.frontierCandidates = frontierCandidates
	}
}

func (t *templateBuildTelemetry) noteInvalidation(reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.invalidations++
	t.lastReason = reason
}

func (t *templateBuildTelemetry) noteInterruption() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.interruptions++
	t.lastReason = "interrupted"
}

func (t *templateBuildTelemetry) noteBuildTime(at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastBuildAt = at
}

func (t *templateBuildTelemetry) snapshot() BlockTemplateStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	lastBuildAgeMS := 0
	if !t.lastBuildAt.IsZero() {
		lastBuildAgeMS = int(time.Since(t.lastBuildAt) / time.Millisecond)
	}
	return BlockTemplateStats{
		CacheHits:          t.cacheHits,
		Rebuilds:           t.rebuilds,
		FrontierCandidates: t.frontierCandidates,
		Invalidations:      t.invalidations,
		Interruptions:      t.interruptions,
		LastBuildAgeMS:     lastBuildAgeMS,
		LastReason:         t.lastReason,
	}
}

func (s *Service) activePeerAddrs() []string {
	peers := s.peerSnapshot()
	addrs := make([]string, 0, len(peers))
	for _, peer := range peers {
		addrs = append(addrs, peer.addr)
	}
	slices.Sort(addrs)
	return addrs
}

func (s *Service) blockLocator() [][32]byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.blockLocatorLocked()
}

func (s *Service) blockLocatorLocked() [][32]byte {
	locator := make([][32]byte, 0, 16)
	tip := s.headerChain.TipHeight()
	if tip == nil {
		return append(locator, consensus.HeaderHash(&s.genesis.Header))
	}
	step := uint64(1)
	height := *tip
	for {
		hash, err := s.chainState.Store().GetBlockHashByHeight(height)
		if err != nil {
			break
		}
		if hash != nil {
			locator = append(locator, *hash)
		}
		if height == 0 {
			break
		}
		if len(locator) >= 10 {
			step *= 2
		}
		if height <= step {
			height = 0
		} else {
			height -= step
		}
	}
	if len(locator) == 0 {
		locator = append(locator, consensus.HeaderHash(&s.genesis.Header))
	}
	return locator
}

func (s *Service) missingBlockHashes(limit int) [][32]byte {
	hashes, _, err := s.missingBlockHashesDetailed(limit)
	if err != nil {
		return nil
	}
	return hashes
}

func (s *Service) missingBlockHashesDetailed(limit int) ([][32]byte, bool, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([][32]byte, 0, limit)
	blockTip := s.chainState.ChainState().TipHeight()
	headerTip := s.headerChain.TipHeight()
	if blockTip == nil || headerTip == nil {
		return out, false, nil
	}
	for height := *blockTip + 1; height <= *headerTip && len(out) < limit; height++ {
		entry, err := s.chainState.Store().GetBlockIndexByHeight(height)
		if err != nil {
			return nil, false, err
		}
		if entry == nil {
			return out, true, nil
		}
		hash := consensus.HeaderHash(&entry.Header)
		block, err := s.chainState.Store().GetBlock(&hash)
		if err != nil {
			return nil, false, err
		}
		if block == nil {
			out = append(out, hash)
		}
	}
	return out, false, nil
}

func (s *Service) repairActiveHeightIndex() (int, error) {
	return s.syncManager().repairActiveHeightIndex()
}
func (s *Service) repairActiveHeightIndexLocked() (int, error) {
	return s.syncManager().repairActiveHeightIndexLocked()
}
func (s *Service) snapshotPeers() []*peerConn       { return s.peerManager().peerSnapshot() }
func (s *Service) restartKnownPeers()               { s.peerManager().restartKnownPeers() }
func (s *Service) restartOutboundPeer(addr string)  { s.peerManager().restartOutboundPeer(addr) }
func (s *Service) knownPeerAddrs() []string         { return s.peerManager().knownPeerAddrs() }
func (s *Service) hasPeer(addr string) bool         { return s.peerManager().hasPeer(addr) }
func (s *Service) hasOutboundPeer(addr string) bool { return s.peerManager().hasOutboundPeer(addr) }
func (s *Service) loadPersistedKnownPeers(peers map[string]time.Time) {
	s.peerManager().loadPersistedKnownPeers(peers)
}
func (s *Service) recordKnownPeerSuccess(addr string, at time.Time) {
	s.peerManager().recordKnownPeerSuccess(addr, at)
}
func (s *Service) shouldMaintainOutboundPeer(addr string) bool {
	return s.peerManager().shouldMaintainOutboundPeer(addr)
}
func (s *Service) rememberKnownPeers(addrs []string) { s.peerManager().rememberKnownPeers(addrs) }

func normalizePeerAddr(addr string) string {
	return strings.TrimSpace(addr)
}

func jitterDuration(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	extra := time.Duration(mrand.Int63n(int64(base / 4)))
	return base + extra
}

func (s *Service) sleepUntilStop(delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-s.stopCh:
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) findLocatorHeightLocked(locator [][32]byte) (uint64, error) {
	for _, hash := range locator {
		entry, err := s.chainState.Store().GetBlockIndex(&hash)
		if err != nil {
			return 0, err
		}
		if entry == nil {
			continue
		}
		activeHash, err := s.chainState.Store().GetBlockHashByHeight(entry.Height)
		if err != nil {
			return 0, err
		}
		if activeHash != nil && *activeHash == hash {
			return entry.Height, nil
		}
	}
	return 0, nil
}

func (s *Service) authorizeRPC(r *http.Request) bool {
	if s.cfg.RPCAuthToken == "" {
		return true
	}
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if strings.HasPrefix(header, prefix) && strings.TrimPrefix(header, prefix) == s.cfg.RPCAuthToken {
		return true
	}
	return r.Header.Get("X-BPU-Auth") == s.cfg.RPCAuthToken
}

func (s *Service) blockIndexByHashHex(raw string) (*storage.BlockIndexEntry, error) {
	hash, err := decodeHashHex(raw)
	if err != nil {
		return nil, err
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	entry, err := s.chainState.Store().GetBlockIndex(&hash)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, errors.New("unknown block hash")
	}
	return entry, nil
}

func (s *Service) blockIndexByHeight(height uint64) (*storage.BlockIndexEntry, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	entry, err := s.chainState.Store().GetBlockIndexByHeight(height)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, errors.New("unknown block height")
	}
	return entry, nil
}

func (s *Service) blockByHashHex(raw string) (*types.Block, error) {
	hash, err := decodeHashHex(raw)
	if err != nil {
		return nil, err
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	block, err := s.chainState.Store().GetBlock(&hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errors.New("unknown block hash")
	}
	return block, nil
}

func renderPublicDashboardPage(view *publicDashboardView, path string) (string, int) {
	switch {
	case path == "/":
		return renderPublicDashboardHome(view), http.StatusOK
	case strings.HasPrefix(path, "/block/"):
		hash := strings.TrimPrefix(path, "/block/")
		for _, block := range view.blocks {
			if hex.EncodeToString(block.Hash[:]) == hash {
				return renderPublicBlockPage(view, block), http.StatusOK
			}
		}
	case strings.HasPrefix(path, "/tx/"):
		txid := strings.TrimPrefix(path, "/tx/")
		for _, block := range view.blocks {
			for i, tx := range block.PreviewTxs {
				if i >= 5 {
					break
				}
				if hex.EncodeToString(tx.TxID[:]) == txid {
					return renderPublicTxPage(view, block, tx), http.StatusOK
				}
			}
		}
	}
	return renderPublicNotFoundPage(view), http.StatusNotFound
}

func renderPublicDashboardHome(view *publicDashboardView) string {
	var body strings.Builder
	body.Grow(32768)
	body.WriteString(renderHTMLPrologue("Bitcoin Pure Monitor"))
	body.WriteString(renderDashboardBanner())
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("CHAIN OVERVIEW"))
	body.WriteString(fmt.Sprintf(" Node ID      : %s\n", view.nodeID))
	body.WriteString(fmt.Sprintf(" Created      : %s UTC\n", view.createdAt.UTC().Format("2006-01-02 15:04:05")))
	body.WriteString(fmt.Sprintf(" Refreshed    : %s UTC\n", view.generatedAt.UTC().Format("2006-01-02 15:04:05")))
	body.WriteString(fmt.Sprintf(" Health       : %s\n", view.health))
	body.WriteString(fmt.Sprintf(" Tip          : height %-8d  hash %s\n", view.info.TipHeight, linkHash("/block/", view.info.TipHeaderHash, 14)))
	body.WriteString(fmt.Sprintf(" UTXO Root    : %s\n", shortHexString(view.info.UTXORoot, 20)))
	body.WriteString(fmt.Sprintf(" Peers        : %-4d  mempool %-6d  orphans %-4d\n", len(view.peers), view.mempool.Count, view.mempool.Orphans))
	body.WriteString(fmt.Sprintf(" Tx Relay     : reconciled batches\n"))
	body.WriteString(fmt.Sprintf(" Block Relay  : short-id blocks\n"))
	body.WriteString(fmt.Sprintf(" Template     : cache %-4d  rebuilds %-4d  frontier %-4d  invalid %-4d  interrupt %-4d  age_ms %-6d  reason %s\n",
		view.template.CacheHits,
		view.template.Rebuilds,
		view.template.FrontierCandidates,
		view.template.Invalidations,
		view.template.Interruptions,
		view.template.LastBuildAgeMS,
		defaultDashboardValue(view.template.LastReason, "-"),
	))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("BLOCK FLOW"))
	body.WriteString(renderBlockFlow(view.blocks))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader(view.tpsChart.Label))
	body.WriteString(renderTPSChart(view.tpsChart))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("FEE WINDOW"))
	body.WriteString(renderFeeSection(view.fees))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("POW / DAA"))
	body.WriteString(renderPowSection(view.pow))
	body.WriteString("\n")

	if view.mining.Enabled {
		body.WriteString(renderSectionHeader("MINING"))
		body.WriteString(renderMiningSection(view.mining, view.pow))
		body.WriteString("\n")
	}

	body.WriteString(renderSectionHeader("MEMPOOL"))
	body.WriteString(renderMempoolSection(view.mempool))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("PEER MESH"))
	body.WriteString(renderPeerMesh(view.peers, view.relay, view.info.TipHeight))
	body.WriteString("\n")

	body.WriteString(renderDashboardSystemSection(view.system))
	body.WriteString(renderHTMLEpilogue())
	return body.String()
}

func renderPublicBlockPage(view *publicDashboardView, block dashboardBlockPage) string {
	var body strings.Builder
	body.Grow(16384)
	body.WriteString(renderHTMLPrologue(fmt.Sprintf("Block %d", block.Height)))
	body.WriteString(renderDashboardBannerCompact())
	body.WriteString("\n")
	body.WriteString(renderSectionHeader(fmt.Sprintf("BLOCK %d", block.Height)))
	body.WriteString(fmt.Sprintf(" <a href=\"/\">[home]</a>\n"))
	body.WriteString(renderBlockVisual(block))
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("BLOCK META"))
	body.WriteString(fmt.Sprintf(" Hash         : %s\n", fullHashString(block.Hash)))
	body.WriteString(fmt.Sprintf(" Prev         : %s\n", shortHexBytes(block.PrevHash, 24)))
	body.WriteString(fmt.Sprintf(" Time         : %s UTC\n", block.Timestamp.Format("2006-01-02 15:04:05")))
	body.WriteString(fmt.Sprintf(" Size         : %s bytes\n", formatWithCommas(block.Size)))
	body.WriteString(fmt.Sprintf(" Tx Count     : %d\n", block.TxCount))
	body.WriteString(fmt.Sprintf(" Fees         : median %s  low %s  high %s  total %s\n",
		formatAtoms(block.MedianFee), formatAtoms(block.LowFee), formatAtoms(block.HighFee), formatAtoms(block.TotalFees)))
	body.WriteString(fmt.Sprintf(" Pow Bits     : 0x%08x\n", block.NBits))
	body.WriteString(fmt.Sprintf(" Tx Root      : %s\n", shortHexBytes(block.TxRoot, 24)))
	body.WriteString(fmt.Sprintf(" Auth Root    : %s\n", shortHexBytes(block.AuthRoot, 24)))
	body.WriteString(fmt.Sprintf(" UTXO Root    : %s\n", shortHexBytes(block.UTXORoot, 24)))
	body.WriteString(fmt.Sprintf(" Mined Here   : %t\n", block.MinedByNode))
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("TX PREVIEW"))
	body.WriteString(renderBlockTxPreview(block))
	body.WriteString(renderHTMLEpilogue())
	return body.String()
}

func renderPublicTxPage(_ *publicDashboardView, block dashboardBlockPage, tx dashboardTxPage) string {
	var body strings.Builder
	body.Grow(16384)
	body.WriteString(renderHTMLPrologue("Transaction"))
	body.WriteString(renderDashboardBannerCompact())
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("TRANSACTION"))
	body.WriteString(fmt.Sprintf(" <a href=\"/\">[home]</a>  <a href=\"/block/%s\">[block %d]</a>\n",
		hex.EncodeToString(block.Hash[:]), block.Height))
	body.WriteString(renderTxVisual(tx))
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("TX META"))
	body.WriteString(fmt.Sprintf(" TxID         : %s\n", fullHashString(tx.TxID)))
	body.WriteString(fmt.Sprintf(" Block        : %d / %s\n", block.Height, shortHexBytes(block.Hash, 20)))
	body.WriteString(fmt.Sprintf(" Time         : %s UTC\n", tx.Timestamp.Format("2006-01-02 15:04:05")))
	body.WriteString(fmt.Sprintf(" Coinbase     : %t\n", tx.Coinbase))
	body.WriteString(fmt.Sprintf(" Size         : %s bytes\n", formatWithCommas(tx.Size)))
	body.WriteString(fmt.Sprintf(" Auth Entries : %d\n", tx.AuthCount))
	body.WriteString(fmt.Sprintf(" Inputs       : %d (%s)\n", len(tx.Inputs), formatAtoms(tx.InputSum)))
	body.WriteString(fmt.Sprintf(" Outputs      : %d (%s)\n", len(tx.Outputs), formatAtoms(tx.OutputSum)))
	body.WriteString(fmt.Sprintf(" Fee          : %s (%s/byte)\n", formatAtoms(tx.Fee), formatAtoms(tx.FeeRate)))
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("INPUTS"))
	body.WriteString(renderTxInputs(tx))
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("OUTPUTS"))
	body.WriteString(renderTxOutputs(tx))
	body.WriteString(renderHTMLEpilogue())
	return body.String()
}

func renderPublicNotFoundPage(view *publicDashboardView) string {
	var body strings.Builder
	body.WriteString(renderHTMLPrologue("Not Found"))
	body.WriteString(renderDashboardBannerCompact())
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("NOT FOUND"))
	body.WriteString(" <a href=\"/\">[home]</a>\n")
	body.WriteString(" This page is not in the current public cache window.\n")
	body.WriteString(" Only the latest 5 blocks and up to 25 recent transaction pages stay live here.\n")
	body.WriteString(fmt.Sprintf(" Current node: %s\n", view.nodeID))
	body.WriteString(renderHTMLEpilogue())
	return body.String()
}

func renderHTMLPrologue(title string) string {
	return fmt.Sprintf(
		"<!doctype html>\n<html><head><meta charset=\"us-ascii\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>%s</title><style>html,body{margin:0;padding:0;background:#f3f3f3;color:#111}body{padding:14px;font:16px/1.15 Menlo,Consolas,\"Liberation Mono\",\"Courier New\",monospace}pre{margin:0;white-space:pre;font:inherit}a,a:visited,a:hover,a:active{color:inherit;text-decoration:underline}</style></head><body><pre>\n",
		html.EscapeString(title),
	)
}

func renderHTMLEpilogue() string {
	return "</pre></body></html>\n"
}

func renderDashboardBanner() string {
	return strings.Join([]string{
		"      ____  _ _            _         ____",
		"     | __ )(_) |_ ___ ___ (_)_ __   |  _ \\ _   _ _ __ ___",
		"     |  _ \\| | __/ __/ _ \\| | '_ \\  | |_) | | | | '__/ _ \\",
		"     | |_) | | || (_| (_) | | | | | |  __/| |_| | | |  __/",
		"     |____/|_|\\__\\___\\___/|_|_| |_| |_|    \\__,_|_|  \\___|",
		" .====================================================================.",
		" |                         node monitor                                |",
		" '===================================================================='",
	}, "\n")
}

func renderDashboardBannerCompact() string {
	return strings.Join([]string{
		"      ____  _ _            _         ____",
		"     | __ )(_) |_ ___ ___ (_)_ __   |  _ \\ _   _ _ __ ___",
		"     |  _ \\| | __/ __/ _ \\| | '_ \\  | |_) | | | | '__/ _ \\",
		"     | |_) | | || (_| (_) | | | | | |  __/| |_| | | |  __/",
		"     |____/|_|\\__\\___\\___/|_|_| |_| |_|    \\__,_|_|  \\___|",
	}, "\n")
}

func renderSectionHeader(title string) string {
	title = strings.ToUpper(title)
	width := 70
	if len(title)+2 > width {
		width = len(title) + 2
	}
	padding := width - len(title)
	left := padding / 2
	right := padding - left
	return fmt.Sprintf(".:%s:.\n||%s%s%s||\n':%s:'\n",
		strings.Repeat("=", width),
		strings.Repeat(" ", left),
		title,
		strings.Repeat(" ", right),
		strings.Repeat("=", width))
}

func renderDashboardSystemSection(summary dashboardSystemSummary) string {
	var out strings.Builder
	out.WriteString(renderSectionHeader(fmt.Sprintf("NODE SYSTEM (AVG %s)", formatDashboardWindow(summary.Window))))
	out.WriteString(fmt.Sprintf(" CPU Avg      : %s\n", formatDashboardCPU(summary)))
	out.WriteString(fmt.Sprintf(" Network Avg  : up %-12s down %-12s\n",
		formatDashboardNetworkRate(summary.TxBytesPerSec, summary.HasNetwork), formatDashboardNetworkRate(summary.RxBytesPerSec, summary.HasNetwork)))
	out.WriteString(fmt.Sprintf(" RAM Avg      : %s\n", formatDashboardMemory(summary)))
	out.WriteString(fmt.Sprintf(" Load Avg     : %s\n", formatDashboardLoad(summary)))
	out.WriteString(fmt.Sprintf(" Processes    : %s\n", formatDashboardProcesses(summary)))
	out.WriteString(fmt.Sprintf(" CPU Cores    : %s\n", formatDashboardCores(summary)))
	return out.String()
}

func renderBlockFlow(blocks []dashboardBlockPage) string {
	if len(blocks) == 0 {
		return " no blocks in active view\n"
	}
	ordered := append([]dashboardBlockPage(nil), blocks...)
	slices.SortFunc(ordered, func(a, b dashboardBlockPage) int {
		switch {
		case a.Height < b.Height:
			return -1
		case a.Height > b.Height:
			return 1
		default:
			return 0
		}
	})
	top := make([]string, 0, len(ordered))
	mid := make([]string, 0, len(ordered))
	bot := make([]string, 0, len(ordered))
	cap := make([]string, 0, len(ordered))
	for i, block := range ordered {
		label := fmt.Sprintf("h:%d", block.Height)
		if i == len(ordered)-1 {
			label = fmt.Sprintf("tip:%d", block.Height)
		}
		hash := linkHashPadded("/block/", hex.EncodeToString(block.Hash[:]), 12, 12)
		top = append(top, ".--------------.")
		mid = append(mid, fmt.Sprintf("| %-12s |", label))
		bot = append(bot, fmt.Sprintf("| %s |", hash))
		cap = append(cap, "'--------------'")
	}
	gap := "   "
	arrow := " ---> "
	return " " + strings.Join(top, gap) + "\n" +
		" " + strings.Join(mid, gap) + "\n" +
		" " + strings.Join(bot, arrow) + "\n" +
		" " + strings.Join(cap, gap) + "\n"
}

func renderTPSChart(chart dashboardTPSChart) string {
	if len(chart.Buckets) == 0 || chart.MaxTPS == 0 {
		return " no recent block throughput yet\n"
	}
	const rows = 8
	var out strings.Builder
	for row := rows; row >= 1; row-- {
		threshold := chart.MaxTPS * float64(row) / float64(rows)
		out.WriteString(fmt.Sprintf(" %7.1f |", threshold))
		for _, bucket := range chart.Buckets {
			if bucket >= threshold {
				out.WriteByte('#')
			} else {
				out.WriteByte(' ')
			}
		}
		out.WriteString("\n")
	}
	out.WriteString("     0.0 +")
	out.WriteString(strings.Repeat("-", len(chart.Buckets)))
	out.WriteString("\n")
	out.WriteString("         ")
	for _, end := range chart.BucketEnd {
		out.WriteString(string(end.Format("15:04")[3]))
	}
	out.WriteString("\n")
	out.WriteString(fmt.Sprintf(" max %.1f tx/s\n", chart.MaxTPS))
	return out.String()
}

func renderFeeSection(summary dashboardFeeSummary) string {
	var out strings.Builder
	if len(summary.Recent) == 0 {
		out.WriteString(" no recent block fees yet\n")
	} else {
		for _, line := range summary.Recent {
			out.WriteString(fmt.Sprintf(" h%-6d median %-10s low %-10s high %-10s\n",
				line.Height, formatAtoms(line.MedianFee), formatAtoms(line.LowFee), formatAtoms(line.HighFee)))
		}
	}
	out.WriteString(fmt.Sprintf(" mempool clear estimate : ~%d block(s) / %s\n", summary.Clear.Blocks, formatDashboardDuration(summary.Clear.Time)))
	return out.String()
}

func renderPowSection(summary dashboardPowSummary) string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf(" Algo         : %s\n", summary.Algorithm))
	out.WriteString(fmt.Sprintf(" Target Space : %s\n", formatDashboardDuration(summary.TargetSpacing)))
	out.WriteString(fmt.Sprintf(" Current Bits : 0x%08x\n", summary.CurrentBits))
	out.WriteString(fmt.Sprintf(" Next Bits    : 0x%08x\n", summary.NextBits))
	out.WriteString(fmt.Sprintf(" Difficulty   : %.4fx\n", summary.Difficulty))
	if summary.HasObservedGap {
		out.WriteString(fmt.Sprintf(" Recent Gap   : %s\n", formatDashboardDuration(summary.AvgBlockInterval)))
	} else {
		out.WriteString(" Recent Gap   : warming up\n")
	}
	if !summary.LastBlockTimestamp.IsZero() {
		out.WriteString(fmt.Sprintf(" Last Block   : %s UTC\n", summary.LastBlockTimestamp.Format("2006-01-02 15:04:05")))
	}
	return out.String()
}

func renderMiningSection(summary dashboardMiningSummary, pow dashboardPowSummary) string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf(" Status       : enabled\n"))
	out.WriteString(fmt.Sprintf(" Workers      : %d\n", summary.Workers))
	out.WriteString(" Hashrate     : not sampled yet\n")
	out.WriteString(fmt.Sprintf(" Target Space : %s\n", formatDashboardDuration(pow.TargetSpacing)))
	if len(summary.RecentHeights) == 0 {
		out.WriteString(" Recent Wins  : none in current on-screen block window\n")
		return out.String()
	}
	out.WriteString(" Recent Wins  :")
	for i := range summary.RecentHeights {
		out.WriteString(fmt.Sprintf(" h%d", summary.RecentHeights[i]))
	}
	out.WriteString("\n")
	return out.String()
}

func renderMempoolSection(summary dashboardMempoolSummary) string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf(" Tx Count     : %d\n", summary.Count))
	out.WriteString(fmt.Sprintf(" Orphans      : %d\n", summary.Orphans))
	out.WriteString(fmt.Sprintf(" Median Fee   : %s\n", formatAtoms(summary.MedianFee)))
	out.WriteString(fmt.Sprintf(" Fee Low / Hi : %s / %s\n", formatAtoms(summary.LowFee), formatAtoms(summary.HighFee)))
	out.WriteString(fmt.Sprintf(" Next Block   : est %s\n", formatDashboardDuration(summary.EstimatedNext)))
	if len(summary.Top) == 0 {
		out.WriteString(" Queue        : empty\n")
		return out.String()
	}
	out.WriteString(" Queue Top    :\n")
	maxFee := uint64(1)
	for _, entry := range summary.Top {
		if entry.Fee > maxFee {
			maxFee = entry.Fee
		}
	}
	for _, entry := range summary.Top {
		width := int((entry.Fee * 20) / maxFee)
		if width == 0 && entry.Fee > 0 {
			width = 1
		}
		bar := strings.Repeat("#", width) + strings.Repeat(".", 20-width)
		out.WriteString(fmt.Sprintf("   %s  fee %-10s  size %-6s  [%s]\n",
			shortHexBytes(entry.TxID, 10), formatAtoms(entry.Fee), formatWithCommas(entry.Size), bar))
	}
	if summary.Count > len(summary.Top) {
		out.WriteString(fmt.Sprintf("   ... %d more waiting\n", summary.Count-len(summary.Top)))
	}
	return out.String()
}

func renderPeerMesh(peers []PeerInfo, relay []PeerRelayStats, tipHeight uint64) string {
	if len(peers) == 0 {
		return " no live peers\n"
	}
	relayByAddr := make(map[string]PeerRelayStats, len(relay))
	for _, stat := range relay {
		relayByAddr[stat.Addr] = stat
	}
	var out strings.Builder
	for _, peer := range peers {
		stat := relayByAddr[peer.Addr]
		lag := int64(tipHeight) - int64(peer.Height)
		if lag < 0 {
			lag = 0
		}
		out.WriteString(fmt.Sprintf(" %s\n", dashboardPeerLabel(peer.Addr)))
		out.WriteString(fmt.Sprintf("    out=%-5t lag=%-3d q=%-3d p95=%-7.2fms avg=%-7.2fms batches=%-5d recon=%-5d req=%-5d last=%s\n",
			peer.Outbound, lag, stat.QueueDepth, stat.RelayP95MS, stat.RelayAvgMS, stat.TxBatchItems, stat.TxReconItems, stat.TxReqItems, formatAgeUnix(peer.LastProgress)))
	}
	return out.String()
}

func renderBlockVisual(block dashboardBlockPage) string {
	var out strings.Builder
	out.WriteString(" +------------------------------------------------------------------+\n")
	out.WriteString(fmt.Sprintf(" | BLOCK h:%-56d |\n", block.Height))
	out.WriteString(" +------------------------------------------------------------------+\n")
	out.WriteString(fmt.Sprintf(" | hash : %-58s |\n", shortHexBytes(block.Hash, 30)))
	out.WriteString(fmt.Sprintf(" | prev : %-58s |\n", shortHexBytes(block.PrevHash, 30)))
	out.WriteString(fmt.Sprintf(" | time : %-58s |\n", block.Timestamp.Format("2006-01-02 15:04:05")+" UTC"))
	out.WriteString(fmt.Sprintf(" | txs  : %-58s |\n", fmt.Sprintf("%d shown=%d hidden=%d", block.TxCount, len(block.PreviewTxs), block.HiddenTxCount)))
	out.WriteString(" +------------------------------------------------------------------+\n")
	for i, tx := range block.PreviewTxs {
		label := shortHexBytes(tx.TxID, 18)
		if i < 5 {
			label = linkHash("/tx/", hex.EncodeToString(tx.TxID[:]), 18)
		}
		out.WriteString(fmt.Sprintf(" |  o-- tx %-2d  %-18s fee %-10s size %-8s |\n",
			i, label, formatAtoms(tx.Fee), formatWithCommas(tx.Size)))
	}
	if block.HiddenTxCount > 0 {
		out.WriteString(fmt.Sprintf(" |  ... %d more transactions not expanded here%-20s|\n", block.HiddenTxCount, ""))
	}
	out.WriteString(" +------------------------------------------------------------------+\n")
	return out.String()
}

func renderBlockTxPreview(block dashboardBlockPage) string {
	if len(block.PreviewTxs) == 0 {
		return " no transactions\n"
	}
	var out strings.Builder
	for i, tx := range block.PreviewTxs {
		txLabel := shortHexBytes(tx.TxID, 20)
		if i < 5 {
			txLabel = linkHash("/tx/", hex.EncodeToString(tx.TxID[:]), 20)
		}
		kind := "tx"
		if tx.Coinbase {
			kind = "coinbase"
		}
		out.WriteString(fmt.Sprintf(" %-8s %-20s in=%-2d out=%-2d fee=%-10s size=%s\n",
			kind, txLabel, len(tx.Inputs), len(tx.Outputs), formatAtoms(tx.Fee), formatWithCommas(tx.Size)))
	}
	if block.HiddenTxCount > 0 {
		out.WriteString(fmt.Sprintf(" ... %d more transactions\n", block.HiddenTxCount))
	}
	return out.String()
}

func renderTxVisual(tx dashboardTxPage) string {
	var out strings.Builder
	out.WriteString(" +--------------------------------------------------------------+\n")
	out.WriteString(fmt.Sprintf(" | TX %-57s|\n", shortHexBytes(tx.TxID, 28)))
	out.WriteString(" +--------------------------------------------------------------+\n")
	out.WriteString(fmt.Sprintf(" | inputs  %-5d  sum %-12s outputs %-5d sum %-12s |\n",
		len(tx.Inputs), formatAtoms(tx.InputSum), len(tx.Outputs), formatAtoms(tx.OutputSum)))
	out.WriteString(fmt.Sprintf(" | fee     %-12s rate %-12s auth    %-12d |\n",
		formatAtoms(tx.Fee), formatAtoms(tx.FeeRate), tx.AuthCount))
	out.WriteString(" +--------------------------------------------------------------+\n")
	return out.String()
}

func renderTxInputs(tx dashboardTxPage) string {
	if tx.Coinbase {
		return " coinbase transaction has no spendable inputs\n"
	}
	var out strings.Builder
	for _, input := range tx.Inputs {
		out.WriteString(fmt.Sprintf(" %s:%d  amount %-10s\n",
			shortHexBytes(input.PrevOut.TxID, 18), input.PrevOut.Vout, formatAtoms(input.Amount)))
	}
	return out.String()
}

func renderTxOutputs(tx dashboardTxPage) string {
	var out strings.Builder
	for _, output := range tx.Outputs {
		out.WriteString(fmt.Sprintf(" vout %-2d  amount %-10s  keyhash %s\n",
			output.Index, formatAtoms(output.Amount), shortHexBytes(output.KeyHash, 18)))
	}
	return out.String()
}

func linkHash(prefix string, hash string, width int) string {
	label := shortHexString(hash, width)
	return fmt.Sprintf("<a href=\"%s%s\">%s</a>", prefix, hash, label)
}

func linkHashPadded(prefix string, hash string, shortWidth int, cellWidth int) string {
	label := shortHexString(hash, shortWidth)
	if cellWidth > len(label) {
		label = label + strings.Repeat(" ", cellWidth-len(label))
	}
	return fmt.Sprintf("<a href=\"%s%s\">%s</a>", prefix, hash, label)
}

func deriveNodeID(cfg ServiceConfig) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	seed := hostname + "\x00" + cfg.DBPath + "\x00" + cfg.RPCAddr + "\x00" + cfg.P2PAddr + "\x00" + cfg.GenesisFixture
	sum := bpcrypto.Sha256d([]byte(seed))
	return strings.ToUpper(hex.EncodeToString(sum[:6]))
}

func blockMinedByKeyHash(block *types.Block, keyHash [32]byte) bool {
	if len(block.Txs) == 0 || len(block.Txs[0].Base.Outputs) == 0 {
		return false
	}
	for _, output := range block.Txs[0].Base.Outputs {
		if output.KeyHash == keyHash {
			return true
		}
	}
	return false
}

func summarizeFeeSet(fees []uint64) (uint64, uint64, uint64) {
	if len(fees) == 0 {
		return 0, 0, 0
	}
	values := append([]uint64(nil), fees...)
	slices.Sort(values)
	return values[len(values)/2], values[0], values[len(values)-1]
}

func dashboardDifficulty(nBits uint32, powLimitBits uint32) float64 {
	target, ok := compactTargetForDashboard(nBits)
	if !ok || target.Sign() <= 0 {
		return 0
	}
	powLimit, ok := compactTargetForDashboard(powLimitBits)
	if !ok || powLimit.Sign() <= 0 {
		return 0
	}
	ratio := new(big.Rat).SetFrac(powLimit, target)
	value, _ := ratio.Float64()
	return value
}

func compactTargetForDashboard(compact uint32) (*big.Int, bool) {
	size := byte(compact >> 24)
	mantissa := compact & 0x007fffff
	if mantissa == 0 {
		return nil, false
	}
	target := new(big.Int).SetUint64(uint64(mantissa))
	if size <= 3 {
		target.Rsh(target, uint(8*(3-int(size))))
	} else {
		target.Lsh(target, uint(8*(int(size)-3)))
	}
	return target, true
}

func shortHexBytes(hash [32]byte, width int) string {
	return shortHexString(hex.EncodeToString(hash[:]), width)
}

func fullHashString(hash [32]byte) string {
	return hex.EncodeToString(hash[:])
}

func formatAtoms(value uint64) string {
	return formatUintWithCommas(value)
}

func formatWithCommas[T ~int | ~int64 | ~uint64](value T) string {
	return formatUintWithCommas(uint64(value))
}

func formatUintWithCommas(value uint64) string {
	raw := strconv.FormatUint(value, 10)
	if len(raw) <= 3 {
		return raw
	}
	var out strings.Builder
	for i, ch := range raw {
		if i != 0 && (len(raw)-i)%3 == 0 {
			out.WriteByte(',')
		}
		out.WriteRune(ch)
	}
	return out.String()
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func formatAgeUnix(unix int64) string {
	if unix <= 0 {
		return "n/a"
	}
	return formatDashboardDuration(time.Since(time.Unix(unix, 0)))
}

func dashboardPeerLabel(addr string) string {
	return fmt.Sprintf("peer %-28s", addr)
}

func (stats *dashboardSystemStats) record(sample dashboardSystemSample) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.samples = append(stats.samples, sample)
	cutoff := sample.takenAt.Add(-dashboardSystemRetention)
	keep := 0
	for keep < len(stats.samples) && stats.samples[keep].takenAt.Before(cutoff) {
		keep++
	}
	if keep > 0 {
		stats.samples = append([]dashboardSystemSample(nil), stats.samples[keep:]...)
	}
}

func (stats *dashboardSystemStats) summary(now time.Time, window time.Duration) dashboardSystemSummary {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	if len(stats.samples) == 0 {
		return dashboardSystemSummary{}
	}
	start := 0
	cutoff := now.Add(-window)
	for i := range stats.samples {
		if !stats.samples[i].takenAt.Before(cutoff) {
			if i > 0 {
				start = i - 1
			} else {
				start = i
			}
			break
		}
		start = i
	}
	windowSamples := stats.samples[start:]
	if len(windowSamples) == 0 {
		return dashboardSystemSummary{}
	}
	first := windowSamples[0]
	last := windowSamples[len(windowSamples)-1]
	summary := dashboardSystemSummary{
		Window:        last.takenAt.Sub(first.takenAt),
		MemTotalBytes: last.memTotalBytes,
		RunningProcs:  last.runningProcs,
		TotalProcs:    last.totalProcs,
		Cores:         last.cores,
	}
	var memUsedTotal uint64
	var load1Total float64
	var load5Total float64
	var load15Total float64
	for _, sample := range windowSamples {
		memUsedTotal += sample.memUsedBytes
		load1Total += sample.load1
		load5Total += sample.load5
		load15Total += sample.load15
	}
	summary.AvgMemUsedBytes = memUsedTotal / uint64(len(windowSamples))
	summary.HasMemory = summary.MemTotalBytes > 0
	summary.Load1 = load1Total / float64(len(windowSamples))
	summary.Load5 = load5Total / float64(len(windowSamples))
	summary.Load15 = load15Total / float64(len(windowSamples))
	summary.HasLoad = true
	if len(windowSamples) >= 2 && summary.Window >= dashboardSystemMinimumWindow {
		totalDelta := last.cpuTotalTicks - first.cpuTotalTicks
		busyDelta := last.cpuBusyTicks - first.cpuBusyTicks
		if totalDelta > 0 {
			summary.CPUPercent = (float64(busyDelta) / float64(totalDelta)) * 100
			summary.HasCPU = true
		}
		elapsed := last.takenAt.Sub(first.takenAt).Seconds()
		if elapsed > 0 {
			summary.RxBytesPerSec = float64(last.rxBytes-first.rxBytes) / elapsed
			summary.TxBytesPerSec = float64(last.txBytes-first.txBytes) / elapsed
			summary.HasNetwork = true
		}
	}
	return summary
}

func readDashboardSystemSample(now time.Time) (dashboardSystemSample, error) {
	if runtime.GOOS != "linux" {
		return dashboardSystemSample{}, errors.New("dashboard system stats are only supported on linux")
	}
	busyTicks, totalTicks, err := readProcCPUTicks()
	if err != nil {
		return dashboardSystemSample{}, err
	}
	rxBytes, txBytes, err := readProcNetworkBytes()
	if err != nil {
		return dashboardSystemSample{}, err
	}
	memUsedBytes, memTotalBytes, err := readProcMemoryUsage()
	if err != nil {
		return dashboardSystemSample{}, err
	}
	load1, load5, load15, runningProcs, totalProcs, err := readProcLoadAvg()
	if err != nil {
		return dashboardSystemSample{}, err
	}
	return dashboardSystemSample{
		takenAt:       now,
		cpuBusyTicks:  busyTicks,
		cpuTotalTicks: totalTicks,
		rxBytes:       rxBytes,
		txBytes:       txBytes,
		memUsedBytes:  memUsedBytes,
		memTotalBytes: memTotalBytes,
		load1:         load1,
		load5:         load5,
		load15:        load15,
		runningProcs:  runningProcs,
		totalProcs:    totalProcs,
		cores:         runtime.NumCPU(),
	}, nil
}

func formatDashboardDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	totalSeconds := int64(d / time.Second)
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	return fmt.Sprintf("%02dh %02dm %02ds", hours, minutes, seconds)
}

func formatDashboardWindow(d time.Duration) string {
	if d <= 0 {
		return "warming up"
	}
	totalSeconds := int64(d / time.Second)
	if totalSeconds >= 3600 {
		hours := totalSeconds / 3600
		minutes := (totalSeconds % 3600) / 60
		if minutes == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh%02dm", hours, minutes)
	}
	if totalSeconds >= 60 {
		minutes := totalSeconds / 60
		seconds := totalSeconds % 60
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", totalSeconds)
}

func formatDashboardCPU(summary dashboardSystemSummary) string {
	if !summary.HasCPU {
		return "warming up"
	}
	return fmt.Sprintf("%.1f%%", summary.CPUPercent)
}

func formatDashboardNetworkRate(rate float64, ok bool) string {
	if !ok {
		return "warming up"
	}
	return formatHumanRate(rate)
}

func formatDashboardMemory(summary dashboardSystemSummary) string {
	if !summary.HasMemory {
		return "unavailable"
	}
	used := formatHumanBytes(summary.AvgMemUsedBytes)
	total := formatHumanBytes(summary.MemTotalBytes)
	if summary.MemTotalBytes == 0 {
		return used
	}
	pct := (float64(summary.AvgMemUsedBytes) / float64(summary.MemTotalBytes)) * 100
	return fmt.Sprintf("%s / %s (%.0f%%)", used, total, pct)
}

func formatDashboardLoad(summary dashboardSystemSummary) string {
	if !summary.HasLoad {
		return "unavailable"
	}
	return fmt.Sprintf("%.2f / %.2f / %.2f", summary.Load1, summary.Load5, summary.Load15)
}

func formatDashboardProcesses(summary dashboardSystemSummary) string {
	if summary.TotalProcs == 0 {
		return "unavailable"
	}
	return fmt.Sprintf("%d run / %d total", summary.RunningProcs, summary.TotalProcs)
}

func formatDashboardCores(summary dashboardSystemSummary) string {
	if summary.Cores <= 0 {
		return "host stats pending"
	}
	if summary.Cores == 1 {
		return "1 core"
	}
	return fmt.Sprintf("%d cores", summary.Cores)
}

func formatHumanBytes(bytes uint64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	value := float64(bytes)
	unit := units[0]
	for i := 1; i < len(units) && value >= 1024; i++ {
		value /= 1024
		unit = units[i]
	}
	if unit == "B" {
		return fmt.Sprintf("%d %s", bytes, unit)
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}

func formatHumanRate(bytesPerSecond float64) string {
	units := []string{"B/s", "KB/s", "MB/s", "GB/s", "TB/s"}
	value := bytesPerSecond
	unit := units[0]
	for i := 1; i < len(units) && value >= 1024; i++ {
		value /= 1024
		unit = units[i]
	}
	if unit == "B/s" {
		return fmt.Sprintf("%.0f %s", value, unit)
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}

func readProcCPUTicks() (uint64, uint64, error) {
	buf, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(buf), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 8 || fields[0] != "cpu" {
			continue
		}
		values := make([]uint64, 0, len(fields)-1)
		for _, raw := range fields[1:] {
			value, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return 0, 0, err
			}
			values = append(values, value)
		}
		if len(values) < 8 {
			return 0, 0, errors.New("proc stat cpu line missing counters")
		}
		busy := values[0] + values[1] + values[2] + values[5] + values[6] + values[7]
		var total uint64
		for _, value := range values {
			total += value
		}
		return busy, total, nil
	}
	return 0, 0, errors.New("proc stat cpu line not found")
}

func readProcNetworkBytes() (uint64, uint64, error) {
	buf, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0, err
	}
	var rxTotal uint64
	var txTotal uint64
	for _, line := range strings.Split(string(buf), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "" || iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 16 {
			continue
		}
		rx, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		tx, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		rxTotal += rx
		txTotal += tx
	}
	return rxTotal, txTotal, nil
}

func readProcMemoryUsage() (uint64, uint64, error) {
	buf, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	values := make(map[string]uint64)
	for _, line := range strings.Split(string(buf), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		values[key] = value * 1024
	}
	total := values["MemTotal"]
	if total == 0 {
		return 0, 0, errors.New("meminfo missing MemTotal")
	}
	available := values["MemAvailable"]
	if available > total {
		available = total
	}
	return total - available, total, nil
}

func readProcLoadAvg() (float64, float64, float64, int, int, error) {
	buf, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	fields := strings.Fields(string(buf))
	if len(fields) < 4 {
		return 0, 0, 0, 0, 0, errors.New("loadavg missing fields")
	}
	load1, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	load5, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	load15, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	processParts := strings.SplitN(fields[3], "/", 2)
	if len(processParts) != 2 {
		return 0, 0, 0, 0, 0, errors.New("loadavg missing process counts")
	}
	running, err := strconv.Atoi(processParts[0])
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	total, err := strconv.Atoi(processParts[1])
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	return load1, load5, load15, running, total, nil
}

func shortHash(hash [32]byte) string {
	return hex.EncodeToString(hash[:4])
}

func shortHexString(raw string, chars int) string {
	if len(raw) <= chars {
		return raw
	}
	return raw[:chars]
}

func shouldServePublicDashboard() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	buf, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return false
	}
	return osReleaseLooksLikeUbuntu(string(buf))
}

func osReleaseLooksLikeUbuntu(raw string) bool {
	info := parseOSRelease(raw)
	id := strings.ToLower(info["ID"])
	if id == "ubuntu" {
		return true
	}
	return strings.Contains(strings.ToLower(info["ID_LIKE"]), "ubuntu")
}

func defaultMinerWorkers() int {
	workers := runtime.NumCPU() / 2
	if workers < 1 {
		return 1
	}
	return workers
}

func parseOSRelease(raw string) map[string]string {
	info := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		info[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "\"")
	}
	return info
}

func decodeHashHex(raw string) ([32]byte, error) {
	var hash [32]byte
	buf, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return hash, err
	}
	if len(buf) != len(hash) {
		return hash, errors.New("hash must be 32 bytes hex")
	}
	copy(hash[:], buf)
	return hash, nil
}

func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func randomNonce() uint64 {
	var buf [8]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return uint64(buf[0]) |
		uint64(buf[1])<<8 |
		uint64(buf[2])<<16 |
		uint64(buf[3])<<24 |
		uint64(buf[4])<<32 |
		uint64(buf[5])<<40 |
		uint64(buf[6])<<48 |
		uint64(buf[7])<<56
}

func (s *Service) localVersion() p2p.VersionMessage {
	return p2p.VersionMessage{
		Protocol:  1,
		Services:  1,
		Height:    s.blockHeight(),
		Nonce:     randomNonce(),
		UserAgent: "bpu/go",
	}
}

func ParseMinerKeyHash(raw string) ([32]byte, error) {
	var keyHash [32]byte
	if raw == "" {
		return keyHash, nil
	}
	raw = strings.TrimSpace(raw)
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return keyHash, err
	}
	if len(buf) != 32 {
		return keyHash, errors.New("miner keyhash must be 32 bytes hex")
	}
	copy(keyHash[:], buf)
	return keyHash, nil
}
