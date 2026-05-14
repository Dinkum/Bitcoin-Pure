package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	localOriginRebroadcastInterval = 30 * time.Second
	maxPendingPeerBlocks           = 512
	maxServedBlocksPerGetData      = 16
	erlayReconcileInterval         = 5 * time.Second
	erlayReconcileBatchLimit       = 256
	maxWalletActivityLimit         = 10_000
	txRequestCoalesceDelay         = 2 * time.Millisecond
	// Give distributed-origin submissions a slightly larger window to
	// coalesce Erlay announcements before they fan out into tiny
	// TxRecon -> TxRequest -> TxBatch chains.
	erlayReconFlushDelay = 5 * time.Millisecond
)

var (
	dandelionFluffDelay = 2 * time.Second
	// Keep the orphan/side-branch staging queue bounded by memory footprint,
	// not just object count, so one peer cannot pin arbitrarily many large
	// blocks in RAM while their parents are still missing.
	maxPendingPeerBlockBytes    uint64 = 512 << 20
	maxPendingPeerBlocksPerPeer        = 32
	// Keep inbound tx flood protection high enough for large-block relay.
	// Peers still need sustained capacity, but the token bucket should not
	// discard honest bursty txbatch waves before normal backpressure can work.
	inboundPeerTxRatePerSecond  = 10_000.0
	inboundPeerTxBurst          = 25_000.0
	inboundPeerTxViolationLimit = 3
	peerInvalidTxSignatureLimit = 3
)

const (
	peerMisbehaviorBanDuration = time.Hour
	maxRejectedBlockHashes     = 4096
)

var (
	ErrBlockHeaderNotIndexed  = errors.New("block header not indexed")
	errPeerInboundTxRateLimit = errors.New("peer exceeded inbound tx rate limit")
)

type ServiceConfig struct {
	Profile                   types.ChainProfile
	DBPath                    string
	ThroughputSummaryInterval time.Duration
	RPCAddr                   string
	RPCAuthToken              string
	RPCReadTimeout            time.Duration
	RPCWriteTimeout           time.Duration
	RPCHeaderTimeout          time.Duration
	RPCIdleTimeout            time.Duration
	RPCMaxHeaderBytes         int
	RPCMaxBodyBytes           int
	P2PAddr                   string
	Peers                     []string
	MaxInboundPeers           int
	MaxOutboundPeers          int
	HandshakeTimeout          time.Duration
	StallTimeout              time.Duration
	MaxMessageBytes           int
	PebbleCacheBytes          int64
	PebbleBloomBitsPerKey     int
	MinRelayFeePerByte        uint64
	MaxTxSize                 int
	MaxMempoolBytes           int
	MaxAncestors              int
	MaxDescendants            int
	MaxOrphans                int
	AvalancheMode             string
	AvalancheKSample          int
	AvalancheAlphaNumerator   int
	AvalancheAlphaDenominator int
	AvalancheBeta             int
	AvalanchePollInterval     time.Duration
	DandelionEnabled          bool
	MinerEnabled              bool
	MinerWorkers              int
	MinerPubKey               [32]byte
	GenesisFixture            string
	StaticPeerTopology        bool
	// SyntheticMining is benchmark-only. It disables PoW checks inside this
	// process so fixed-cadence block benchmarks do not wait on nonce search.
	SyntheticMining bool
}

type ServiceInfo struct {
	Profile        string        `json:"profile"`
	TipHeight      uint64        `json:"tip_height"`
	HeaderHeight   uint64        `json:"header_height"`
	TipHeaderHash  string        `json:"tip_header_hash"`
	UTXORoot       string        `json:"utxo_root"`
	MempoolSize    int           `json:"mempool_size"`
	RPCAddr        string        `json:"rpc_addr"`
	P2PAddr        string        `json:"p2p_addr"`
	Peers          []string      `json:"peers"`
	Avalanche      AvalancheInfo `json:"avalanche"`
	MinerEnabled   bool          `json:"miner_enabled"`
	MinerWorkers   int           `json:"miner_workers"`
	GenesisFixture string        `json:"genesis_fixture"`
}

type ChainStateInfo struct {
	Profile            string `json:"profile"`
	TipHeight          uint64 `json:"tip_height"`
	HeaderHeight       uint64 `json:"header_height"`
	TipHeaderHash      string `json:"tip_header_hash"`
	UTXORoot           string `json:"utxo_root"`
	UTXOChecksum       string `json:"utxo_checksum"`
	UTXOCount          int    `json:"utxo_count"`
	NextBlockSizeLimit uint64 `json:"next_block_size_limit"`
	TipTimestamp       uint64 `json:"tip_timestamp"`
	ChainWork          string `json:"chainwork,omitempty"`
}

type MempoolInfo struct {
	Count              int    `json:"count"`
	Orphans            int    `json:"orphans"`
	Bytes              int    `json:"bytes"`
	MaxBytes           int    `json:"max_bytes"`
	TotalFees          uint64 `json:"total_fees"`
	MedianFee          uint64 `json:"median_fee"`
	LowFee             uint64 `json:"low_fee"`
	HighFee            uint64 `json:"high_fee"`
	MinRelayFeePerByte uint64 `json:"min_relay_fee_per_byte"`
	CandidateFrontier  int    `json:"candidate_frontier"`
	AvalancheConflicts int    `json:"avalanche_conflicts"`
	AvalancheFinalized int    `json:"avalanche_finalized"`
}

type MiningInfo struct {
	Enabled           bool               `json:"enabled"`
	Workers           int                `json:"workers"`
	MinerPubKey       string             `json:"miner_pubkey,omitempty"`
	CurrentBits       uint32             `json:"current_bits"`
	NextBits          uint32             `json:"next_bits"`
	Difficulty        float64            `json:"difficulty"`
	TargetSpacingSecs uint64             `json:"target_spacing_secs"`
	Template          BlockTemplateStats `json:"template"`
}

type PubKeyUTXO struct {
	OutPoint      types.OutPoint
	Value         uint64
	Type          uint64
	Payload32     [32]byte
	PubKey        [32]byte
	Height        uint64
	Confirmations uint64
	Coinbase      bool
	Mature        bool
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
	Addr                   string  `json:"addr"`
	Outbound               bool    `json:"outbound"`
	QueueDepth             int     `json:"queue_depth"`
	MaxQueueDepth          int     `json:"max_queue_depth"`
	ControlQueueDepth      int     `json:"control_queue_depth"`
	PriorityQueueDepth     int     `json:"priority_queue_depth"`
	SendQueueDepth         int     `json:"send_queue_depth"`
	MaxControlQueueDepth   int     `json:"max_control_queue_depth"`
	MaxPriorityQueueDepth  int     `json:"max_priority_queue_depth"`
	MaxSendQueueDepth      int     `json:"max_send_queue_depth"`
	PendingLocalRelayTxs   int     `json:"pending_local_relay_txs"`
	SentMessages           int     `json:"sent_messages"`
	TxInvItems             int     `json:"tx_inv_items"`
	BlockInvItems          int     `json:"block_inv_items"`
	BlockSendItems         int     `json:"block_send_items"`
	BlockReqItems          int     `json:"block_request_items"`
	TxBatchMsgs            int     `json:"tx_batch_messages"`
	TxBatchItems           int     `json:"tx_batch_items"`
	TxReconMsgs            int     `json:"tx_recon_messages"`
	TxReconItems           int     `json:"tx_recon_items"`
	TxReconRetries         int     `json:"tx_recon_retries"`
	TxReqMsgs              int     `json:"tx_request_messages"`
	TxReqItems             int     `json:"tx_request_items"`
	TxReqRecvMsgs          int     `json:"tx_request_received_messages"`
	TxReqRecvItems         int     `json:"tx_request_received_items"`
	FallbackTxBatchMsgs    int     `json:"fallback_tx_batch_messages"`
	FallbackTxBatchItems   int     `json:"fallback_tx_batch_items"`
	TxNotFoundSent         int     `json:"tx_not_found_sent"`
	TxNotFoundReceived     int     `json:"tx_not_found_received"`
	KnownTxClears          int     `json:"known_tx_clears"`
	DuplicateInvSuppressed int     `json:"duplicate_inv_suppressed"`
	DuplicateTxSuppressed  int     `json:"duplicate_tx_suppressed"`
	KnownTxSuppressed      int     `json:"known_tx_suppressed"`
	CoalescedTxItems       int     `json:"coalesced_tx_items"`
	CoalescedReconItems    int     `json:"coalesced_recon_items"`
	DroppedInv             int     `json:"dropped_inv_items"`
	DroppedTxs             int     `json:"dropped_tx_items"`
	WriterStarvationEvents int     `json:"writer_starvation_events"`
	DroppedPriorityInv     int     `json:"dropped_priority_inv_items,omitempty"`
	DroppedSendInv         int     `json:"dropped_send_inv_items,omitempty"`
	DroppedSendTxs         int     `json:"dropped_send_tx_items,omitempty"`
	ControlStarvation      int     `json:"control_starvation_events,omitempty"`
	PriorityStarvation     int     `json:"priority_starvation_events,omitempty"`
	SendStarvation         int     `json:"send_starvation_events,omitempty"`
	LastRelayActivityUnix  int64   `json:"last_relay_activity_unix,omitempty"`
	RelayEvents            int     `json:"relay_events"`
	RelayAvgMS             float64 `json:"relay_avg_ms,omitempty"`
	RelayP95MS             float64 `json:"relay_p95_ms,omitempty"`
	RelayMaxMS             float64 `json:"relay_max_ms,omitempty"`
}

type BlockTemplateStats struct {
	CacheHits          int    `json:"cache_hits"`
	Rebuilds           int    `json:"rebuilds"`
	FullBuilds         int    `json:"full_builds,omitempty"`
	AppendExtends      int    `json:"append_extends,omitempty"`
	NoChangeRefreshes  int    `json:"no_change_refreshes,omitempty"`
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

	stateMu             sync.RWMutex
	stressMu            sync.Mutex
	peerMu              sync.RWMutex
	pendingInboundPeers int
	downloadMu          sync.Mutex
	recentMu            sync.RWMutex
	rebroadcastMu       sync.Mutex
	peers               map[string]*peerConn
	outboundPeers       map[string]struct{}
	knownPeers          map[string]storage.KnownPeerRecord
	bannedPeers         map[string]time.Time
	blockRequests       map[[32]byte]blockDownloadRequest
	txRequests          map[[32]byte]blockDownloadRequest
	pendingBlocks       map[[32]byte]pendingPeerBlock
	pendingBlockBytes   uint64
	pendingBlocksByPeer map[string]int
	rejectedBlocks      map[[32]byte]struct{}
	rejectedBlockOrder  [][32]byte
	pendingChildren     map[[32]byte]map[[32]byte]struct{}
	pendingBlockFIFO    [][32]byte
	localRebroadcast    map[[32]byte]time.Time
	stressPending       map[[32]byte]stressLaneBatch
	recentHdrs          recentHeaderCache
	recentBlks          recentBlockCache
	rejectCache         *txRejectCache
	validAuth           *validAuthCache
	mempoolPersistCh    chan struct{}
	mempoolPersistMu    sync.Mutex
	mempoolPersistState persistedMempoolState
	peerMgr             *peerManager
	syncMgr             *syncManager
	relaySched          *relayScheduler
	minerMgr            *minerManager
	avaMgr              *avalancheManager
	mineHeaderFn        func(types.BlockHeader, consensus.ChainParams, func(uint64) bool) (types.BlockHeader, bool, error)
	nodeID              string
	dashboard           dashboardCache
	systemStats         dashboardSystemStats
	perf                performanceMetricsCollector
	throughput          throughputSummaryTelemetry
	runtimeStatus       runtimeStatusTelemetry
	startedAt           time.Time
	publicPage          bool
	listener            net.Listener
	rpcSrv              *http.Server
	stopCh              chan struct{}
	stopOnce            sync.Once
	closeOnce           sync.Once
	closeErr            error
	rpcRequestSeq       atomic.Uint64
	wg                  sync.WaitGroup
}

type SyncDebugSnapshot struct {
	BlockHeight    uint64
	HeaderHeight   uint64
	MempoolCount   int
	PeerSync       string
	InflightBlocks string
	PendingBlocks  string
}

// persistedMempoolState mirrors the last successfully flushed restart cache so
// the service can translate a full in-memory mempool snapshot into a minimal
// set of per-tx storage upserts and deletes.

const (
	dashboardSystemSampleInterval     = 10 * time.Second
	dashboardSystemWindow             = 10 * time.Minute
	syncWatchdogInterval              = 3 * time.Second
	outboundRefillInterval            = 5 * time.Second
	controlMessageEnqueueTimeout      = 100 * time.Millisecond
	blockRequestBatchSize             = 128
	txRelayBatchMaxItems              = 256
	maxKnownPeerAddrs                 = 256
	defaultMaxMessageBytes            = 64_000_000
	stressLaneConfirmTimeout          = 8 * time.Minute
	defaultLocalTxBatchEnqueueTimeout = 500 * time.Millisecond
	maxLocalTxBatchEnqueueTimeout     = 5 * time.Second
	compactCatchupMempoolMin          = 1024
)
