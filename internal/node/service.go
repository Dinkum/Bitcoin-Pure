package node

import (
	"context"
	crand "crypto/rand"
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
	"slices"
	"strings"
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

type ServiceConfig struct {
	Profile            types.ChainProfile
	DBPath             string
	RPCAddr            string
	RPCAuthToken       string
	RPCReadTimeout     time.Duration
	RPCWriteTimeout    time.Duration
	RPCHeaderTimeout   time.Duration
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
	MinerKeyHash       [32]byte
	MineInterval       time.Duration
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
	MineIntervalMS int64    `json:"mine_interval_ms"`
	GenesisFixture string   `json:"genesis_fixture"`
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
	CacheHits          int `json:"cache_hits"`
	Rebuilds           int `json:"rebuilds"`
	FrontierCandidates int `json:"frontier_candidates"`
}

type Service struct {
	cfg         ServiceConfig
	logger      *slog.Logger
	chainState  *PersistentChainState
	headerChain *HeaderChain
	pool        *mempool.Pool
	genesis     *types.Block

	stateMu       sync.RWMutex
	templateMu    sync.Mutex
	peerMu        sync.RWMutex
	recentMu      sync.RWMutex
	peers         map[string]*peerConn
	template      *blockTemplateCache
	recentHdrs    recentHeaderCache
	recentBlks    recentBlockCache
	templateStats templateBuildTelemetry
	dashboard     dashboardCache
	startedAt     time.Time
	publicPage    bool
	listener      net.Listener
	rpcSrv        *http.Server
	stopCh        chan struct{}
	stopOnce      sync.Once
	closeOnce     sync.Once
	closeErr      error
	wg            sync.WaitGroup
}

type peerConn struct {
	addr            string
	outbound        bool
	wire            *p2p.Conn
	version         p2p.VersionMessage
	lastProgress    time.Time
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
	tipHash        [32]byte
	mempoolEpoch   uint64
	block          types.Block
	selected       []mempool.SnapshotEntry
	totalFees      uint64
	usedTxBytes    int
	selectionUtxos consensus.UtxoSet
}

type templateBuildTelemetry struct {
	mu                 sync.Mutex
	cacheHits          int
	rebuilds           int
	frontierCandidates int
}

type chainSelectionSnapshot struct {
	tipHash        [32]byte
	height         uint64
	tipHeader      types.BlockHeader
	blockSizeState consensus.BlockSizeState
	utxos          consensus.UtxoSet
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
	body       string
}

type dashboardBlock struct {
	Height uint64
	Hash   [32]byte
}

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
	if cfg.MaxInboundPeers <= 0 {
		cfg.MaxInboundPeers = 32
	}
	if cfg.MaxOutboundPeers <= 0 {
		cfg.MaxOutboundPeers = 8
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 8 << 20
	}
	if cfg.MaxTxSize <= 0 {
		cfg.MaxTxSize = 1_000_000
	}
	if cfg.MaxAncestors <= 0 {
		cfg.MaxAncestors = 25
	}
	if cfg.MaxDescendants <= 0 {
		cfg.MaxDescendants = 25
	}
	if cfg.MaxOrphans <= 0 {
		cfg.MaxOrphans = 128
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
		genesis:    genesis,
		peers:      make(map[string]*peerConn),
		recentHdrs: recentHeaderCache{items: make(map[[32]byte]types.BlockHeader)},
		recentBlks: recentBlockCache{items: make(map[[32]byte]types.Block)},
		startedAt:  time.Now(),
		publicPage: shouldServePublicDashboard(),
		stopCh:     make(chan struct{}),
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
			_ = peer.wire.Close()
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
		s.logger.Info("dialing configured peer", slog.String("addr", addr))
		s.wg.Add(1)
		go func(addr string) {
			defer s.wg.Done()
			if err := s.ConnectPeer(addr); err != nil {
				s.logger.Warn("peer dial failed", slog.String("addr", addr), slog.Any("error", err))
			}
		}(addr)
	}
	if s.cfg.MinerEnabled && s.cfg.MineInterval > 0 {
		s.logger.Info("background miner enabled", slog.Duration("interval", s.cfg.MineInterval))
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.minerLoop()
		}()
	}
	select {
	case <-ctx.Done():
		s.logger.Info("shutdown signal received")
		return s.Close()
	case <-s.stopCh:
		return s.Close()
	}
}

func (s *Service) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				s.logger.Error("accept loop failed", slog.Any("error", err))
				return
			}
		}
		if !s.canAcceptInboundPeer() {
			s.logger.Warn("rejecting inbound peer: inbound limit reached", slog.String("addr", conn.RemoteAddr().String()))
			_ = conn.Close()
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handlePeer(conn, false)
		}()
	}
}

func (s *Service) canAcceptInboundPeer() bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	if s.cfg.MaxInboundPeers <= 0 {
		return true
	}
	inbound := 0
	for _, peer := range s.peers {
		if !peer.outbound {
			inbound++
		}
	}
	return inbound < s.cfg.MaxInboundPeers
}

func (s *Service) ConnectPeer(addr string) error {
	s.logger.Info("connecting peer", slog.String("addr", addr))
	if s.hasPeer(addr) {
		return nil
	}
	if s.cfg.MaxOutboundPeers > 0 && s.outboundPeerCount() >= s.cfg.MaxOutboundPeers {
		return fmt.Errorf("outbound peer limit reached")
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	s.handlePeer(conn, true)
	return nil
}

func (s *Service) outboundPeerCount() int {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	outbound := 0
	for _, peer := range s.peers {
		if peer.outbound {
			outbound++
		}
	}
	return outbound
}

func (s *Service) handlePeer(conn net.Conn, outbound bool) {
	addr := conn.RemoteAddr().String()
	wire := p2p.NewConn(conn, p2p.MagicForProfile(s.cfg.Profile), s.cfg.MaxMessageBytes)
	remoteVersion, err := p2p.Handshake(wire, s.localVersion(), s.cfg.HandshakeTimeout)
	if err != nil {
		s.logger.Warn("peer handshake failed", slog.String("addr", addr), slog.Any("error", err))
		_ = conn.Close()
		return
	}
	peer := &peerConn{
		addr:         addr,
		outbound:     outbound,
		wire:         wire,
		version:      remoteVersion,
		lastProgress: time.Now(),
		sendQ:        make(chan outboundMessage, 512),
		closed:       make(chan struct{}),
		queuedInv:    make(map[p2p.InvVector]int),
		queuedTx:     make(map[[32]byte]int),
		knownTx:      make(map[[32]byte]struct{}),
		pendingThin:  make(map[[32]byte]*pendingThinBlock),
	}
	s.peerMu.Lock()
	s.peers[addr] = peer
	s.peerMu.Unlock()
	s.logger.Info("peer connected", slog.String("addr", addr), slog.Int("peer_count", s.peerCount()))
	defer func() {
		peer.close()
		s.peerMu.Lock()
		delete(s.peers, addr)
		s.peerMu.Unlock()
		_ = wire.Close()
		s.logger.Info("peer disconnected", slog.String("addr", addr), slog.Int("peer_count", s.peerCount()))
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.peerWriteLoop(peer)
	}()

	s.requestSync(peer)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.peerPingLoop(peer)
	}()

	for {
		if s.cfg.StallTimeout > 0 {
			_ = wire.SetReadDeadline(time.Now().Add(s.cfg.StallTimeout))
		}
		msg, err := wire.ReadMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.logger.Warn("peer read loop failed", slog.String("addr", addr), slog.Any("error", err))
			}
			return
		}
		peer.lastProgress = time.Now()
		if err := s.onPeerMessage(peer, msg); err != nil {
			s.logger.Warn("peer message handling failed", slog.String("addr", addr), slog.String("type", fmt.Sprintf("%T", msg)), slog.Any("error", err))
			return
		}
	}
}

func (s *Service) peerPingLoop(peer *peerConn) {
	interval := s.cfg.StallTimeout / 2
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := peer.send(p2p.PingMessage{Nonce: randomNonce()}); err != nil {
				return
			}
		}
	}
}

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
	select {
	case <-p.closed:
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(len(p.sendQ))
		return nil
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
	envelope := outboundMessage{
		msg:        p2p.InvMessage{Items: filtered},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.InvMessage{Items: filtered}),
		invItems:   filtered,
	}
	select {
	case <-p.closed:
		p.releaseQueuedInv(filtered)
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(len(p.sendQ))
		return nil
	default:
		p.releaseQueuedInv(filtered)
		p.telemetry.noteDroppedInv(len(filtered))
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
	select {
	case <-p.closed:
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(len(p.sendQ))
		return nil
	}
}

func (p *peerConn) close() {
	p.closeOnce.Do(func() {
		close(p.closed)
	})
}

func (s *Service) peerWriteLoop(peer *peerConn) {
	for {
		select {
		case <-s.stopCh:
			peer.close()
			return
		case <-peer.closed:
			return
		case envelope := <-peer.sendQ:
			if err := peer.wire.WriteMessage(envelope.msg); err != nil {
				peer.releaseQueuedInv(envelope.invItems)
				peer.releaseRelayBatch(envelope.msg)
				peer.close()
				_ = peer.wire.Close()
				return
			}
			peer.noteKnownTxs(envelope.msg)
			peer.releaseQueuedInv(envelope.invItems)
			peer.releaseRelayBatch(envelope.msg)
			peer.telemetry.noteSent(envelope, len(peer.sendQ))
		}
	}
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
		return nil
	case p2p.InvMessage:
		return s.onInvMessage(peer, m)
	case p2p.GetDataMessage:
		return s.onGetDataMessage(peer, m)
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
		_, errs, _, _ := s.submitDecodedTxsFrom([]types.Transaction{m.Tx}, peer)
		return errs[0]
	case p2p.TxBatchMessage:
		peer.noteKnownTxs(m)
		_, errs, _, _ := s.submitDecodedTxsFrom(m.Txs, peer)
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	case p2p.TxReconMessage:
		peer.noteKnownTxIDs(m.TxIDs)
		return s.onTxReconMessage(peer, m)
	case p2p.TxRequestMessage:
		return s.onTxRequestMessage(peer, m)
	}
	return nil
}

func (s *Service) Info() ServiceInfo {
	s.stateMu.RLock()
	tipHeight := uint64(0)
	headerHeight := uint64(0)
	var tipHash [32]byte
	var utxoRoot [32]byte
	if tip := s.chainState.ChainState().TipHeight(); tip != nil {
		tipHeight = *tip
	}
	if tip := s.headerChain.TipHeight(); tip != nil {
		headerHeight = *tip
	}
	if tip := s.chainState.ChainState().TipHeader(); tip != nil {
		tipHash = consensus.HeaderHash(tip)
	}
	utxoRoot = consensus.ComputedUTXORoot(s.chainState.ChainState().UTXOs())
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
		MineIntervalMS: s.cfg.MineInterval.Milliseconds(),
		GenesisFixture: s.cfg.GenesisFixture,
	}
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

func (s *Service) submitDecodedTxs(txs []types.Transaction) ([]mempool.Admission, []error, int, int) {
	return s.submitDecodedTxsFrom(txs, nil)
}

func (s *Service) submitDecodedTxsFrom(txs []types.Transaction, source *peerConn) ([]mempool.Admission, []error, int, int) {
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
		s.broadcastAcceptedTxsToPeers(s.peerSnapshotExcluding(source), accepted)
		return admissions, errs, orphanCount, mempoolSize
	}
	tipHash, chainUtxos := s.chainUtxoSnapshotWithTip()
	snapshot := s.pool.AdmissionSnapshot()
	prepared, prepareErrs := s.prepareAdmissionsParallel(txs, snapshot, chainUtxos, rules)
	for i, tx := range txs {
		if prepareErrs[i] != nil {
			errs[i] = prepareErrs[i]
			continue
		}
		if s.chainTipHash() != tipHash {
			admission, err := s.pool.AcceptTx(tx, s.chainUtxoSnapshot(), rules)
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
	orphanCount := s.pool.OrphanCount()
	mempoolSize := s.pool.Count()

	s.broadcastAcceptedTxsToPeers(s.peerSnapshotExcluding(source), accepted)
	return admissions, errs, orphanCount, mempoolSize
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
	items := make([]p2p.InvVector, 0, count)
	for len(hashes) < count {
		block, err := s.BuildBlockTemplate()
		if err != nil {
			return hashes, err
		}
		hash := consensus.HeaderHash(&block.Header)

		s.stateMu.Lock()
		currentTip := s.chainState.ChainState().TipHeader()
		if currentTip == nil {
			s.stateMu.Unlock()
			return hashes, ErrNoTip
		}
		if block.Header.PrevBlockHash != consensus.HeaderHash(currentTip) {
			s.stateMu.Unlock()
			continue
		}
		if _, err := s.chainState.ApplyBlock(&block); err != nil {
			s.stateMu.Unlock()
			return hashes, err
		}
		if err := s.headerChain.ApplyHeader(&block.Header); err != nil {
			s.stateMu.Unlock()
			return hashes, err
		}
		stored, err := s.headerChain.StoredState()
		if err != nil {
			s.stateMu.Unlock()
			return hashes, err
		}
		if err := s.chainState.Store().WriteHeaderState(stored); err != nil {
			s.stateMu.Unlock()
			return hashes, err
		}
		height := uint64(0)
		if tip := s.chainState.ChainState().TipHeight(); tip != nil {
			height = *tip
		}
		s.stateMu.Unlock()

		s.pool.RemoveConfirmed(&block)
		s.cacheRecentBlock(block)
		s.cacheRecentHeader(block.Header)
		hashes = append(hashes, hex.EncodeToString(hash[:]))
		items = append(items, p2p.InvVector{Type: p2p.InvTypeBlock, Hash: hash})
		s.logger.Info("block accepted",
			slog.Uint64("height", height),
			slog.String("hash", hex.EncodeToString(hash[:])),
			slog.Int("txs", len(block.Txs)),
			slog.Int("mempool_size", s.pool.Count()),
		)
	}
	s.broadcastInvToPeers(s.peerSnapshot(), items)
	return hashes, nil
}

func (s *Service) BuildBlockTemplate() (types.Block, error) {
	ctx, err := s.chainTemplateContext()
	if err != nil {
		return types.Block{}, err
	}
	mempoolEpoch := s.pool.Epoch()
	if cached, ok := s.cachedBlockTemplate(ctx.tipHash, mempoolEpoch); ok {
		s.templateStats.noteCacheHit(s.pool.SelectionCandidateCount())
		return cloneBlock(cached), nil
	}
	if block, ok, err := s.extendBlockTemplate(ctx, mempoolEpoch); err != nil {
		return types.Block{}, err
	} else if ok {
		s.templateStats.noteRebuild(s.pool.SelectionCandidateCount())
		return cloneBlock(block), nil
	}
	snapshot, err := s.chainSelectionSnapshot()
	if err != nil {
		return types.Block{}, err
	}
	block, selectedEntries, totalFees, usedTxBytes, selectionUtxos, err := s.buildBlockCandidate(snapshot)
	if err != nil {
		return types.Block{}, err
	}
	s.storeBlockTemplate(snapshot.tipHash, mempoolEpoch, block, selectedEntries, totalFees, usedTxBytes, selectionUtxos)
	s.templateStats.noteRebuild(s.pool.SelectionCandidateCount())
	return cloneBlock(block), nil
}

func (s *Service) minerLoop() {
	ticker := time.NewTicker(s.cfg.MineInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if _, err := s.MineBlocks(1); err != nil {
				s.logger.Warn("background mining failed", slog.Any("error", err))
			}
		}
	}
}

func (s *Service) buildBlockCandidate(snapshot chainSelectionSnapshot) (types.Block, []mempool.SnapshotEntry, uint64, int, consensus.UtxoSet, error) {
	tempUtxos := snapshot.utxos
	maxTemplateBytes := int(snapshot.blockSizeState.Limit)
	if maxTemplateBytes > 1024 {
		maxTemplateBytes -= 1024
	}
	selectedEntries, totalFees := s.pool.SelectForBlock(tempUtxos, consensus.DefaultConsensusRules(), maxTemplateBytes)
	for _, entry := range selectedEntries {
		txid := entry.TxID
		for _, input := range entry.Tx.Base.Inputs {
			delete(tempUtxos, input.PrevOut)
		}
		for vout, output := range entry.Tx.Base.Outputs {
			tempUtxos[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = consensus.UtxoEntry{
				ValueAtoms: output.ValueAtoms,
				KeyHash:    output.KeyHash,
			}
		}
	}
	block, usedTxBytes, err := s.assembleBlockTemplate(chainTemplateContext{
		tipHash:        snapshot.tipHash,
		height:         snapshot.height,
		tipHeader:      snapshot.tipHeader,
		blockSizeState: snapshot.blockSizeState,
	}, selectedEntries, totalFees, tempUtxos)
	if err != nil {
		return types.Block{}, nil, 0, 0, nil, err
	}
	s.logger.Debug("building block candidate",
		slog.Uint64("next_height", snapshot.height+1),
		slog.Int("selected_txs", len(selectedEntries)),
		slog.Uint64("total_fees", totalFees),
	)
	return block, selectedEntries, totalFees, usedTxBytes, tempUtxos, nil
}

func (s *Service) assembleBlockTemplate(ctx chainTemplateContext, selectedEntries []mempool.SnapshotEntry, totalFees uint64, selectionUtxos consensus.UtxoSet) (types.Block, int, error) {
	params := consensus.ParamsForProfile(s.cfg.Profile)
	nextTimestamp := ctx.tipHeader.Timestamp + uint64(params.TargetSpacingSecs)
	if nextTimestamp <= ctx.tipHeader.Timestamp {
		nextTimestamp = ctx.tipHeader.Timestamp + 1
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: ctx.height, Header: ctx.tipHeader}, params)
	if err != nil {
		return types.Block{}, 0, err
	}

	selected := make([]types.Transaction, 0, len(selectedEntries))
	usedTxBytes := 0
	for _, entry := range selectedEntries {
		selected = append(selected, entry.Tx)
		usedTxBytes += entry.Size
	}

	coinbase := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{
				ValueAtoms: consensus.SubsidyAtoms(ctx.height+1, params) + totalFees,
				KeyHash:    s.cfg.MinerKeyHash,
			}},
		},
	}
	finalUtxos := cloneUtxos(selectionUtxos)
	coinbaseTxID := consensus.TxID(&coinbase)
	for vout, output := range coinbase.Base.Outputs {
		finalUtxos[types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		}
	}

	txs := make([]types.Transaction, 0, len(selected)+1)
	txs = append(txs, coinbase)
	txs = append(txs, selected...)
	txids := make([][32]byte, 0, len(txs))
	authids := make([][32]byte, 0, len(txs))
	for i := range txs {
		txids = append(txids, consensus.TxID(&txs[i]))
		authids = append(authids, consensus.AuthID(&txs[i]))
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  ctx.tipHash,
		MerkleTxIDRoot: consensus.MerkleRoot(txids),
		MerkleAuthRoot: consensus.MerkleRoot(authids),
		UTXORoot:       consensus.ComputedUTXORoot(finalUtxos),
		Timestamp:      nextTimestamp,
		NBits:          nbits,
	}
	mined, err := consensus.MineHeader(header, params)
	if err != nil {
		return types.Block{}, 0, err
	}
	return types.Block{Header: mined, Txs: txs}, usedTxBytes, nil
}

func (s *Service) chainUtxoSnapshot() consensus.UtxoSet {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return cloneUtxos(s.chainState.ChainState().UTXOs())
}

func (s *Service) chainUtxoSnapshotWithTip() ([32]byte, consensus.UtxoSet) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	var tipHash [32]byte
	if tip := s.chainState.ChainState().TipHeader(); tip != nil {
		tipHash = consensus.HeaderHash(tip)
	}
	return tipHash, cloneUtxos(s.chainState.ChainState().UTXOs())
}

func (s *Service) chainTipHash() [32]byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if tip := s.chainState.ChainState().TipHeader(); tip != nil {
		return consensus.HeaderHash(tip)
	}
	return [32]byte{}
}

func (s *Service) chainTemplateContext() (chainTemplateContext, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	height := s.chainState.ChainState().TipHeight()
	header := s.chainState.ChainState().TipHeader()
	if height == nil || header == nil {
		return chainTemplateContext{}, ErrNoTip
	}
	return chainTemplateContext{
		tipHash:        consensus.HeaderHash(header),
		height:         *height,
		tipHeader:      *header,
		blockSizeState: s.chainState.ChainState().BlockSizeState(),
	}, nil
}

func (s *Service) chainSelectionSnapshot() (chainSelectionSnapshot, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	height := s.chainState.ChainState().TipHeight()
	header := s.chainState.ChainState().TipHeader()
	if height == nil || header == nil {
		return chainSelectionSnapshot{}, ErrNoTip
	}
	return chainSelectionSnapshot{
		tipHash:        consensus.HeaderHash(header),
		height:         *height,
		tipHeader:      *header,
		blockSizeState: s.chainState.ChainState().BlockSizeState(),
		utxos:          cloneUtxos(s.chainState.ChainState().UTXOs()),
	}, nil
}

func (s *Service) cachedBlockTemplate(tipHash [32]byte, mempoolEpoch uint64) (types.Block, bool) {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	if s.template == nil || s.template.tipHash != tipHash || s.template.mempoolEpoch != mempoolEpoch {
		return types.Block{}, false
	}
	return s.template.block, true
}

func (s *Service) extendBlockTemplate(ctx chainTemplateContext, mempoolEpoch uint64) (types.Block, bool, error) {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	if s.template == nil || s.template.tipHash != ctx.tipHash || s.template.mempoolEpoch >= mempoolEpoch {
		return types.Block{}, false, nil
	}
	maxTemplateBytes := int(ctx.blockSizeState.Limit)
	if maxTemplateBytes > 1024 {
		maxTemplateBytes -= 1024
	}
	if s.template.usedTxBytes >= maxTemplateBytes {
		return types.Block{}, false, nil
	}
	if !s.pool.ContainsAll(s.template.selected) {
		return types.Block{}, false, nil
	}

	added, addedFees := s.pool.AppendForBlock(s.template.selectionUtxos, consensus.DefaultConsensusRules(), maxTemplateBytes, s.template.selected)
	if len(added) == 0 {
		s.template.mempoolEpoch = mempoolEpoch
		block, _, err := s.assembleBlockTemplate(ctx, s.template.selected, s.template.totalFees, s.template.selectionUtxos)
		if err != nil {
			return types.Block{}, false, err
		}
		s.template.block = cloneBlock(block)
		return s.template.block, true, nil
	}

	s.template.selected = append(s.template.selected, added...)
	s.template.totalFees += addedFees
	for _, entry := range added {
		s.template.usedTxBytes += entry.Size
	}
	block, _, err := s.assembleBlockTemplate(ctx, s.template.selected, s.template.totalFees, s.template.selectionUtxos)
	if err != nil {
		return types.Block{}, false, err
	}
	s.template.block = cloneBlock(block)
	s.template.mempoolEpoch = mempoolEpoch
	return s.template.block, true, nil
}

func (s *Service) storeBlockTemplate(tipHash [32]byte, mempoolEpoch uint64, block types.Block, selected []mempool.SnapshotEntry, totalFees uint64, usedTxBytes int, selectionUtxos consensus.UtxoSet) {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	s.template = &blockTemplateCache{
		tipHash:        tipHash,
		mempoolEpoch:   mempoolEpoch,
		block:          cloneBlock(block),
		selected:       append([]mempool.SnapshotEntry(nil), selected...),
		totalFees:      totalFees,
		usedTxBytes:    usedTxBytes,
		selectionUtxos: cloneUtxos(selectionUtxos),
	}
}

func cloneBlock(block types.Block) types.Block {
	out := block
	if len(block.Txs) != 0 {
		out.Txs = append([]types.Transaction(nil), block.Txs...)
	}
	return out
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

func reconstructXThinBlock(msg p2p.XThinBlockMessage, entries []mempool.SnapshotEntry) (*pendingThinBlock, []uint32) {
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

	matches := make(map[uint64][]types.Transaction, len(entries))
	for _, entry := range entries {
		shortID := thinBlockShortID(msg.Nonce, entry.TxID)
		matches[shortID] = append(matches[shortID], entry.Tx)
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
	applied := 0
	for _, header := range headers {
		if err := s.headerChain.ApplyHeader(&header); err != nil {
			return applied, err
		}
		stored, err := s.headerChain.StoredState()
		if err != nil {
			return applied, err
		}
		if err := s.chainState.Store().WriteHeaderState(stored); err != nil {
			return applied, err
		}
		if err := s.chainState.Store().PutHeader(stored.Height, &header); err != nil {
			return applied, err
		}
		s.cacheRecentHeader(header)
		applied++
	}
	return applied, nil
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
	applied, err := s.applyPeerBlock(block)
	if err != nil {
		return err
	}
	if applied {
		hash := consensus.HeaderHash(&block.Header)
		s.logger.Info("applied peer block", slog.String("addr", peer.addr), slog.Uint64("block_height", s.blockHeight()))
		s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	}
	return s.requestBlocks(peer)
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
	if tip := s.headerChain.TipHeight(); tip != nil {
		return *tip
	}
	return 0
}

func (s *Service) HeaderHeight() uint64 {
	return s.headerHeight()
}

func (s *Service) MempoolCount() int {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.pool.Count()
}

func (s *Service) OrphanCount() int {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.pool.OrphanCount()
}

func (s *Service) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && r.URL.Path == "/" && s.publicPage {
		s.handlePublicDashboard(w, r)
		return
	}
	if r.Method == http.MethodPost {
		s.handleRPC(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *Service) handlePublicDashboard(w http.ResponseWriter, r *http.Request) {
	body, renderedAt, err := s.cachedDashboardHTML()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=us-ascii")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("Last-Modified", renderedAt.UTC().Format(http.TimeFormat))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = io.WriteString(w, body)
}

func (s *Service) cachedDashboardHTML() (string, time.Time, error) {
	s.dashboard.mu.Lock()
	defer s.dashboard.mu.Unlock()
	if s.dashboard.body != "" && time.Since(s.dashboard.renderedAt) < time.Minute {
		return s.dashboard.body, s.dashboard.renderedAt, nil
	}
	body, err := s.renderDashboardHTML()
	if err != nil {
		return "", time.Time{}, err
	}
	s.dashboard.body = body
	s.dashboard.renderedAt = time.Now()
	return s.dashboard.body, s.dashboard.renderedAt, nil
}

func (s *Service) renderDashboardHTML() (string, error) {
	info := s.Info()
	peers := s.PeerInfo()
	relay := s.RelayPeerStats()
	template := s.BlockTemplateStats()
	blocks, err := s.dashboardRecentBlocks(4)
	if err != nil {
		return "", err
	}
	mempoolEntries := s.pool.Snapshot()
	health := s.dashboardHealth(info, peers)

	var body strings.Builder
	body.Grow(8192)
	body.WriteString("<!doctype html>\n<html><head><meta charset=\"us-ascii\"><title>BPU Node</title></head><body><pre>\n")
	body.WriteString(renderDashboardBanner())
	body.WriteString("\n")
	body.WriteString(fmt.Sprintf("Generated : %s UTC\n", time.Now().UTC().Format("2006-01-02 15:04:05")))
	body.WriteString(fmt.Sprintf("Profile   : %s\n", info.Profile))
	body.WriteString(fmt.Sprintf("Health    : %s\n", health))
	body.WriteString(fmt.Sprintf("Uptime    : %s\n", formatDashboardDuration(time.Since(s.startedAt))))
	body.WriteString(fmt.Sprintf("RPC       : %s\n", info.RPCAddr))
	body.WriteString(fmt.Sprintf("P2P       : %s\n", info.P2PAddr))
	body.WriteString(fmt.Sprintf("Genesis   : %s\n", filepath.Base(info.GenesisFixture)))
	body.WriteString("\n")

	body.WriteString("+-------------------- Node Stats --------------------+\n")
	body.WriteString(fmt.Sprintf(" Tip Height    : %-12d Header Height : %-12d\n", info.TipHeight, info.HeaderHeight))
	body.WriteString(fmt.Sprintf(" Tip Hash      : %s\n", shortHexString(info.TipHeaderHash, 16)))
	body.WriteString(fmt.Sprintf(" UTXO Root     : %s\n", shortHexString(info.UTXORoot, 16)))
	body.WriteString(fmt.Sprintf(" Mempool       : %-12d Orphans       : %-12d\n", s.pool.Count(), s.pool.OrphanCount()))
	body.WriteString(fmt.Sprintf(" Peers         : %-12d Miner         : %-12t\n", len(peers), info.MinerEnabled))
	body.WriteString(fmt.Sprintf(" Mine Interval : %-12d Template      : hits=%d rebuilds=%d frontier=%d\n",
		info.MineIntervalMS, template.CacheHits, template.Rebuilds, template.FrontierCandidates))
	body.WriteString("+----------------------------------------------------+\n\n")

	body.WriteString("Block Flow\n")
	body.WriteString(renderBlockFlow(blocks))
	body.WriteString("\n")

	body.WriteString("Peer Mesh\n")
	if len(peers) == 0 {
		body.WriteString("  no live peers\n\n")
	} else {
		for _, peer := range peers {
			body.WriteString(fmt.Sprintf("  %s  out=%-5t height=%-6d last=%d\n",
				peer.Addr, peer.Outbound, peer.Height, peer.LastProgress))
		}
		body.WriteString("\n")
	}

	body.WriteString("Relay Pulse\n")
	if len(relay) == 0 {
		body.WriteString("  no relay stats yet\n\n")
	} else {
		for _, stat := range relay {
			body.WriteString(fmt.Sprintf("  %-21s q=%-3d max=%-3d txbatch=%-5d recon=%-5d req=%-5d avg=%.2fms p95=%.2fms\n",
				stat.Addr, stat.QueueDepth, stat.MaxQueueDepth, stat.TxBatchItems, stat.TxReconItems, stat.TxReqItems, stat.RelayAvgMS, stat.RelayP95MS))
		}
		body.WriteString("\n")
	}

	body.WriteString("Mempool Sketch\n")
	body.WriteString(renderMempoolSketch(mempoolEntries))
	body.WriteString("\n")

	body.WriteString("Legend\n")
	body.WriteString("  GET /  : public cached node page on Ubuntu installs\n")
	body.WriteString("  POST / : authenticated JSON-RPC\n")
	body.WriteString("  Block relay : xthin preferred, full block fallback\n")
	body.WriteString("</pre></body></html>\n")
	return body.String(), nil
}

func (s *Service) dashboardRecentBlocks(limit int) ([]dashboardBlock, error) {
	if limit <= 0 {
		return nil, nil
	}
	s.stateMu.RLock()
	tip := s.chainState.ChainState().TipHeight()
	s.stateMu.RUnlock()
	if tip == nil {
		return nil, nil
	}
	start := uint64(0)
	if *tip+1 > uint64(limit) {
		start = *tip + 1 - uint64(limit)
	}
	blocks := make([]dashboardBlock, 0, limit)
	for height := start; height <= *tip; height++ {
		hash, err := s.chainState.Store().GetBlockHashByHeight(height)
		if err != nil {
			return nil, err
		}
		if hash == nil {
			continue
		}
		blocks = append(blocks, dashboardBlock{Height: height, Hash: *hash})
	}
	return blocks, nil
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
		s.stateMu.RLock()
		entries := s.pool.Snapshot()
		s.stateMu.RUnlock()
		out := make([]string, 0, len(entries))
		for _, entry := range entries {
			txid := consensus.TxID(&entry.Tx)
			out = append(out, hex.EncodeToString(txid[:]))
		}
		return out, nil
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
	Addr         string `json:"addr"`
	Outbound     bool   `json:"outbound"`
	Height       uint64 `json:"height"`
	UserAgent    string `json:"user_agent"`
	LastProgress int64  `json:"last_progress_unix"`
}

func (s *Service) PeerInfo() []PeerInfo {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	out := make([]PeerInfo, 0, len(s.peers))
	for _, peer := range s.peers {
		out = append(out, PeerInfo{
			Addr:         peer.addr,
			Outbound:     peer.outbound,
			Height:       peer.version.Height,
			UserAgent:    peer.version.UserAgent,
			LastProgress: peer.lastProgress.Unix(),
		})
	}
	slices.SortFunc(out, func(a, b PeerInfo) int {
		return strings.Compare(a.Addr, b.Addr)
	})
	return out
}

func (s *Service) RelayPeerStats() []PeerRelayStats {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	out := make([]PeerRelayStats, 0, len(s.peers))
	for _, peer := range s.peers {
		out = append(out, peer.telemetry.snapshot(peer.addr, peer.outbound, len(peer.sendQ)))
	}
	slices.SortFunc(out, func(a, b PeerRelayStats) int {
		return strings.Compare(a.Addr, b.Addr)
	})
	return out
}

func (s *Service) BlockTemplateStats() BlockTemplateStats {
	return s.templateStats.snapshot()
}

func (s *Service) requestSync(peer *peerConn) {
	_ = peer.send(p2p.GetAddrMessage{})
	_ = peer.send(p2p.GetHeadersMessage{Locator: s.blockLocator()})
}

func (s *Service) requestBlocks(peer *peerConn) error {
	hashes := s.missingBlockHashes(128)
	if len(hashes) == 0 {
		return nil
	}
	items := make([]p2p.InvVector, 0, len(hashes))
	for _, hash := range hashes {
		items = append(items, p2p.InvVector{Type: p2p.InvTypeBlock, Hash: hash})
	}
	return peer.send(p2p.GetDataMessage{Items: items})
}

func (s *Service) onInvMessage(peer *peerConn, msg p2p.InvMessage) error {
	var getData []p2p.InvVector
	var requestHeaders bool
	var stopHash [32]byte
	for _, item := range msg.Items {
		switch item.Type {
		case p2p.InvTypeTx:
			if !s.pool.Contains(item.Hash) {
				getData = append(getData, item)
			}
		case p2p.InvTypeBlock:
			if _, ok := s.recentHeader(item.Hash); ok {
				if _, ok := s.recentBlock(item.Hash); !ok {
					getData = append(getData, item)
				}
				continue
			}
			entry, err := s.chainState.Store().GetBlockIndex(&item.Hash)
			if err != nil {
				return err
			}
			if entry == nil {
				requestHeaders = true
				stopHash = item.Hash
				break
			}
			block, err := s.chainState.Store().GetBlock(&item.Hash)
			if err != nil {
				return err
			}
			if block == nil {
				getData = append(getData, item)
			}
		}
		if requestHeaders {
			break
		}
	}
	if requestHeaders {
		return peer.send(p2p.GetHeadersMessage{Locator: s.blockLocator(), StopHash: stopHash})
	}
	if len(getData) == 0 {
		return nil
	}
	return peer.send(p2p.GetDataMessage{Items: getData})
}

func (s *Service) onGetDataMessage(peer *peerConn, msg p2p.GetDataMessage) error {
	notFound := make([]p2p.InvVector, 0)
	send := make([]p2p.Message, 0, len(msg.Items))
	txs := make([]types.Transaction, 0, len(msg.Items))
	for _, item := range msg.Items {
		switch item.Type {
		case p2p.InvTypeTx:
			tx := s.pool.Get(item.Hash)
			if tx == nil {
				notFound = append(notFound, item)
				continue
			}
			txs = append(txs, *tx)
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

func (s *Service) onXThinBlockMessage(peer *peerConn, msg p2p.XThinBlockMessage) error {
	state, missing := reconstructXThinBlock(msg, s.pool.Snapshot())
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

func (s *Service) preferredBlockRelayMessage(hash [32]byte) (p2p.Message, bool, error) {
	block, ok, err := s.loadBlock(hash)
	if err != nil || !ok {
		return nil, ok, err
	}
	return buildXThinBlockMessage(block), true, nil
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
	missing := make([][32]byte, 0, len(msg.TxIDs))
	for _, txid := range msg.TxIDs {
		if s.pool.Contains(txid) {
			continue
		}
		missing = append(missing, txid)
	}
	if len(missing) == 0 {
		return nil
	}
	return peer.send(p2p.TxRequestMessage{TxIDs: missing})
}

func (s *Service) onTxRequestMessage(peer *peerConn, msg p2p.TxRequestMessage) error {
	if len(msg.TxIDs) == 0 {
		return nil
	}
	txs := make([]types.Transaction, 0, len(msg.TxIDs))
	for _, txid := range msg.TxIDs {
		tx := s.pool.Get(txid)
		if tx == nil {
			continue
		}
		txs = append(txs, *tx)
	}
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

func (s *Service) broadcastInv(items []p2p.InvVector) {
	s.broadcastInvToPeers(s.peerSnapshot(), items)
}

func (s *Service) broadcastInvToPeers(peers []*peerConn, items []p2p.InvVector) {
	if len(items) == 0 {
		return
	}
	msg := p2p.InvMessage{Items: items}
	for _, peer := range peers {
		_ = peer.send(msg)
	}
}

func (s *Service) broadcastAcceptedTxsToPeers(peers []*peerConn, accepted []mempool.AcceptedTx) {
	if len(accepted) == 0 {
		return
	}
	txids := make([][32]byte, 0, len(accepted))
	for _, item := range accepted {
		txids = append(txids, item.TxID)
	}
	for _, peer := range peers {
		_ = peer.send(p2p.TxReconMessage{TxIDs: txids})
	}
}

func (s *Service) peerSnapshot() []*peerConn {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	peers := make([]*peerConn, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (s *Service) peerSnapshotExcluding(skip *peerConn) []*peerConn {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	peers := make([]*peerConn, 0, len(s.peers))
	for _, peer := range s.peers {
		if skip != nil && peer == skip {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
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
	time.Sleep(2 * time.Millisecond)
	for _, batch := range p.takePendingTxs() {
		p.enqueueRelayTxs(batch)
	}
}

func (p *peerConn) flushPendingReconAfterDelay() {
	time.Sleep(2 * time.Millisecond)
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
		p.telemetry.noteEnqueue(len(p.sendQ))
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
		p.telemetry.noteEnqueue(len(p.sendQ))
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

func (t *templateBuildTelemetry) snapshot() BlockTemplateStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return BlockTemplateStats{
		CacheHits:          t.cacheHits,
		Rebuilds:           t.rebuilds,
		FrontierCandidates: t.frontierCandidates,
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
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([][32]byte, 0, limit)
	blockTip := s.chainState.ChainState().TipHeight()
	headerTip := s.headerChain.TipHeight()
	if blockTip == nil || headerTip == nil {
		return out
	}
	for height := *blockTip + 1; height <= *headerTip && len(out) < limit; height++ {
		entry, err := s.chainState.Store().GetBlockIndexByHeight(height)
		if err != nil || entry == nil {
			break
		}
		hash := consensus.HeaderHash(&entry.Header)
		block, err := s.chainState.Store().GetBlock(&hash)
		if err != nil {
			break
		}
		if block == nil {
			out = append(out, hash)
		}
	}
	return out
}

func (s *Service) knownPeerAddrs() []string {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	set := make(map[string]struct{})
	if s.cfg.P2PAddr != "" {
		set[s.cfg.P2PAddr] = struct{}{}
	}
	for _, addr := range s.cfg.Peers {
		if addr != "" {
			set[addr] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for addr := range set {
		out = append(out, addr)
	}
	slices.Sort(out)
	return out
}

func (s *Service) hasPeer(addr string) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	_, ok := s.peers[addr]
	return ok
}

func (s *Service) findLocatorHeightLocked(locator [][32]byte) (uint64, error) {
	for _, hash := range locator {
		entry, err := s.chainState.Store().GetBlockIndex(&hash)
		if err != nil {
			return 0, err
		}
		if entry != nil {
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

func renderDashboardBanner() string {
	return strings.Join([]string{
		"  ____  ____  _   _",
		" | __ )|  _ \\| | | |",
		" |  _ \\| |_) | | | |",
		" | |_) |  __/| |_| |",
		" |____/|_|    \\___/",
		" Bitcoin Pure node monitor",
	}, "\n")
}

func renderBlockFlow(blocks []dashboardBlock) string {
	if len(blocks) == 0 {
		return "  no blocks indexed yet\n"
	}
	top := make([]string, 0, len(blocks))
	mid := make([]string, 0, len(blocks))
	bot := make([]string, 0, len(blocks))
	for i, block := range blocks {
		label := fmt.Sprintf("h:%-6d", block.Height)
		hash := shortHash(block.Hash)
		if i == len(blocks)-1 {
			label = fmt.Sprintf("tip:%-4d", block.Height)
		}
		top = append(top, "+--------------+")
		mid = append(mid, fmt.Sprintf("| %-12s |", label))
		bot = append(bot, fmt.Sprintf("| %-12s |", hash))
	}
	return "  " + strings.Join(top, " -> ") + "\n" +
		"  " + strings.Join(mid, " -> ") + "\n" +
		"  " + strings.Join(bot, " -> ") + "\n" +
		"  " + strings.Join(top, " -> ") + "\n"
}

func renderMempoolSketch(entries []mempool.SnapshotEntry) string {
	if len(entries) == 0 {
		return "  [empty]\n"
	}
	total := len(entries)
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
	if len(entries) > 8 {
		entries = entries[:8]
	}
	maxFee := uint64(1)
	for _, entry := range entries {
		if entry.Fee > maxFee {
			maxFee = entry.Fee
		}
	}
	var out strings.Builder
	for _, entry := range entries {
		width := int((entry.Fee * 18) / maxFee)
		if width == 0 && entry.Fee > 0 {
			width = 1
		}
		bar := strings.Repeat("#", width) + strings.Repeat(".", 18-width)
		out.WriteString(fmt.Sprintf("  %s fee=%-6d size=%-5d [%s]\n", shortHash(entry.TxID), entry.Fee, entry.Size, bar))
	}
	if total > len(entries) {
		out.WriteString("  ... top 8 by fee shown ...\n")
	}
	return out.String()
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
	switch host {
	case "127.0.0.1", "localhost", "::1", "":
		return true
	default:
		return false
	}
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
