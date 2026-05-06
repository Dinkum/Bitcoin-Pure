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
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

const (
	localOriginRebroadcastInterval = 30 * time.Second
	maxPendingPeerBlocks           = 512
	maxServedBlocksPerGetData      = 16
	erlayReconcileInterval         = 5 * time.Second
	erlayReconcileBatchLimit       = 256
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
	OutPoint  types.OutPoint
	Value     uint64
	Type      uint64
	Payload32 [32]byte
	PubKey    [32]byte
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
type persistedMempoolState struct {
	Valid   bool
	Meta    storage.StoredMempoolStateMeta
	Entries map[[32]byte]persistedMempoolEntryFingerprint
	Orphans map[[32]byte]persistedMempoolOrphanFingerprint
}

type livePersistedMempoolSnapshot struct {
	State   persistedMempoolState
	Entries map[[32]byte]mempool.PersistedEntry
	Orphans map[[32]byte]mempool.PersistedOrphan
}

type persistedMempoolEntryFingerprint struct {
	AuthID  [32]byte
	Summary consensus.TxValidationSummary
	AddedAt uint64
}

type persistedMempoolOrphanFingerprint struct {
	AuthID  [32]byte
	AddedAt uint64
	Missing []types.OutPoint
}

type peerConn struct {
	svc                 *Service
	addr                string
	targetAddr          string
	outbound            bool
	connectedAt         time.Time
	traffic             *peerTrafficMeter
	wire                *p2p.Conn
	version             p2p.VersionMessage
	lastProgress        atomic.Int64
	bestHeight          atomic.Uint64
	controlQ            chan outboundMessage
	relayPriorityQ      chan outboundMessage
	sendQ               chan outboundMessage
	closed              chan struct{}
	closeOnce           sync.Once
	invMu               sync.Mutex
	queuedInv           map[p2p.InvVector]int
	txMu                sync.Mutex
	queuedTx            map[[32]byte]int
	knownTx             map[[32]byte]struct{}
	knownTxOrder        [][32]byte
	knownTxNext         int
	inboundTxTokens     float64
	inboundTxLastRefill time.Time
	inboundTxViolations int
	invalidTxSignatures int
	// Keep buffered relay payloads keyed by txid so coalescing does not repeatedly copy
	// whole transaction structs through intermediate queue slices on busy fanout paths.
	pendingTxOrder  [][32]byte
	pendingTxByID   map[[32]byte]*types.Transaction
	txFlushArmed    bool
	pendingRecon    [][32]byte
	reconFlushArmed bool
	pendingReqOrder [][32]byte
	pendingReqSet   map[[32]byte]struct{}
	reqFlushArmed   bool
	localRelayTxs   map[[32]byte]localRelayFallbackState
	thinMu          sync.Mutex
	pendingThin     map[[32]byte]*pendingThinBlock
	blockRelayMu    sync.Mutex
	blockRelay      peerBlockRelayState
	erlayMu         sync.Mutex
	erlayState      peerErlayState
	telemetry       peerRelayTelemetry
	syncMu          sync.Mutex
	syncState       peerSyncState
	avaMu           sync.Mutex
	avaState        peerAvalancheState
}

type peerAvalancheState struct {
	pollsSent     int
	pollsReceived int
	votesSent     int
	votesReceived int
}

type peerBlockRelayState struct {
	pendingExtendedRecovery map[[32]byte]struct{}
}

type peerErlayState struct {
	roundsStarted int
	roundsHit     int
	cursor        int
	lastRoundAt   time.Time
	lastSetSize   int
	lastMissing   int
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

// Keep a wider exact per-peer known-tx window so hot relay paths do not churn
// through suppression state after only a few busy rounds. Bitcoin Core uses a
// larger rolling filter here; we keep exact semantics for now and stabilize the
// memory profile with a fixed-size ring.
const peerKnownTxLimit = 65536

type outboundMessage struct {
	msg        p2p.Message
	enqueuedAt time.Time
	lane       relayQueueLane
	class      relayMessageClass
	invItems   []p2p.InvVector
}

type relayQueueLane uint8

const (
	relayQueueLaneControl relayQueueLane = iota + 1
	relayQueueLanePriority
	relayQueueLaneSend
)

func (l relayQueueLane) String() string {
	switch l {
	case relayQueueLaneControl:
		return "control"
	case relayQueueLanePriority:
		return "priority"
	case relayQueueLaneSend:
		return "send"
	default:
		return "unknown"
	}
}

type relayMessageClass struct {
	txInvItems           int
	blockInvItems        int
	blockSendItems       int
	blockReqItems        int
	txBatchMsgs          int
	txBatchItems         int
	fallbackTxBatchMsgs  int
	fallbackTxBatchItems int
	txReconMsgs          int
	txReconItems         int
	txReqMsgs            int
	txReqItems           int
}

func (c relayMessageClass) logAttrs() []slog.Attr {
	return []slog.Attr{
		slog.Int("tx_inv_items", c.txInvItems),
		slog.Int("block_inv_items", c.blockInvItems),
		slog.Int("block_send_items", c.blockSendItems),
		slog.Int("block_request_items", c.blockReqItems),
		slog.Int("tx_batch_messages", c.txBatchMsgs),
		slog.Int("tx_batch_items", c.txBatchItems),
		slog.Int("tx_recon_messages", c.txReconMsgs),
		slog.Int("tx_recon_items", c.txReconItems),
		slog.Int("tx_request_messages", c.txReqMsgs),
		slog.Int("tx_request_items", c.txReqItems),
		slog.Int("fallback_tx_batch_messages", c.fallbackTxBatchMsgs),
		slog.Int("fallback_tx_batch_items", c.fallbackTxBatchItems),
	}
}

type peerRelayTelemetry struct {
	mu                    sync.Mutex
	maxQueueDepth         int
	maxControlQueueDepth  int
	maxPriorityQueueDepth int
	maxSendQueueDepth     int
	sentMessages          int
	txInvItems            int
	blockInvItems         int
	blockSendItems        int
	blockReqItems         int
	txBatchMsgs           int
	txBatchItems          int
	txReconMsgs           int
	txReconItems          int
	txReconRetries        int
	txReqMsgs             int
	txReqItems            int
	txReqRecvMsgs         int
	txReqRecvItems        int
	fallbackTxBatchMsgs   int
	fallbackTxBatchItems  int
	txNotFoundSent        int
	txNotFoundReceived    int
	knownTxClears         int
	duplicateInv          int
	duplicateTx           int
	knownTxSuppressed     int
	coalescedTxItems      int
	coalescedReconItems   int
	droppedInv            int
	droppedTxs            int
	writerStarvation      int
	droppedPriorityInv    int
	droppedSendInv        int
	droppedSendTxs        int
	controlStarvation     int
	priorityStarvation    int
	sendStarvation        int
	lastRelayActivityUnix int64
	relaySamples          []float64
}

type queueDepthSnapshot struct {
	total    int
	control  int
	priority int
	send     int
}

type localRelayFallbackState struct {
	announcedAt time.Time
}

type blockRelayPlan uint8

const (
	blockRelayPlanFull blockRelayPlan = iota + 1
	blockRelayPlanCompactFallback
	blockRelayPlanGrapheneExtended
)

type blockOverlapEstimate struct {
	KnownTxs   int
	MissingTxs int
	TotalTxs   int
	HitRate    float64
}

type blockDownloadRequest struct {
	peerAddr    string
	requestedAt time.Time
	attempts    int
}

type pendingPeerBlock struct {
	block      types.Block
	peerAddr   string
	sizeBytes  uint64
	receivedAt time.Time
}

type pendingPeerBlockStoreResult struct {
	Added        bool
	PendingCount int
	PendingBytes uint64
	Evicted      int
	Dropped      bool
	DropReason   string
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
	performance PerformanceMetrics
	blocks      []dashboardBlockPage
	peers       []PeerInfo
	relay       []PeerRelayStats
	peerHosts   []dashboardPeerHostPage
	template    BlockTemplateStats
	pow         dashboardPowSummary
	fees        dashboardFeeSummary
	mining      dashboardMiningSummary
	mempool     dashboardMempoolSummary
	tpsChart    dashboardTPSChart
}

type dashboardPeerSocketPage struct {
	Addr           string
	Port           string
	Outbound       bool
	Height         uint64
	Lag            uint64
	LastSeenUnix   int64
	LastUsefulUnix int64
	UsefulBlocks   int
	SessionAge     time.Duration
	QueueDepth     int
	LatencyAvgS    float64
	LatencyP95S    float64
	TxSent         int
	TxRequested    int
	BlockSent      int
	BlockRequested int
	BytesIn        uint64
	BytesOut       uint64
}

type dashboardPeerHostPage struct {
	Host           string
	Health         string
	Direction      string
	Reason         string
	Sockets        int
	LastSeenUnix   int64
	SessionAge     time.Duration
	BlocksBehind   uint64
	BestHeight     uint64
	TipHash        string
	LastBlockUnix  int64
	LatencyAvgS    float64
	LatencyP95S    float64
	OutboundQueue  int
	TxSent         int
	TxRequested    int
	BlockSent      int
	BlockRequested int
	BytesIn        uint64
	BytesOut       uint64
	SocketPages    []dashboardPeerSocketPage
}

type peerTrafficMeter struct {
	rxBytes atomic.Uint64
	txBytes atomic.Uint64
}

type meteredNetConn struct {
	net.Conn
	meter *peerTrafficMeter
}

func (c *meteredNetConn) Read(buf []byte) (int, error) {
	n, err := c.Conn.Read(buf)
	if n > 0 && c.meter != nil {
		c.meter.rxBytes.Add(uint64(n))
	}
	return n, err
}

func (c *meteredNetConn) Write(buf []byte) (int, error) {
	n, err := c.Conn.Write(buf)
	if n > 0 && c.meter != nil {
		c.meter.txBytes.Add(uint64(n))
	}
	return n, err
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
	MedianFeeRate uint64
	LowFeeRate    uint64
	HighFeeRate   uint64
	PaidTxs       int
	TotalUserTxs  int
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
	Index  int
	Amount uint64
	PubKey [32]byte
}

type dashboardPowSummary struct {
	Algorithm          string
	TargetSpacing      time.Duration
	CurrentBits        uint32
	NextBits           uint32
	Difficulty         float64
	NetworkHashrate    float64
	AvgBlockInterval   time.Duration
	RecentBlockGap     time.Duration
	HasObservedGap     bool
	LastBlockTimestamp time.Time
}

type dashboardFeeSummary struct {
	Recent        []dashboardBlockFeeLine
	CurrentMedian uint64
	Median6       uint64
	Median24      uint64
	RecentMin     uint64
	RecentMax     uint64
	PaidBlocks    int
	TotalBlocks   int
	FeePayingTxs  int
	TotalTxs      int
	Candidate     dashboardCandidateFeeLine
	Bands         dashboardMempoolFeeBands
	Clear         dashboardMempoolClearEstimate
}

type dashboardBlockFeeLine struct {
	Height    uint64
	MedianFee uint64
	LowFee    uint64
	HighFee   uint64
	PaidTxs   int
	TotalTxs  int
	Candidate bool
}

type dashboardCandidateFeeLine struct {
	Height    uint64
	MedianFee uint64
	LowFee    uint64
	HighFee   uint64
	PaidTxs   int
	TotalTxs  int
	Available bool
}

type dashboardMempoolFeeBands struct {
	Above1000 int
	Band500   int
	Band100   int
	Band1     int
	Zero      int
}

type dashboardMempoolClearEstimate struct {
	Blocks int
	Time   time.Duration
}

type dashboardMiningSummary struct {
	Enabled           bool
	Workers           int
	EstimatedHashrate float64
	RecentWins        int
	RecentWindow      int
	RecentHeights     []uint64
	RecentHashes      [][32]byte
}

type dashboardMempoolSummary struct {
	Count         int
	Bytes         int
	Orphans       int
	Top           []mempool.SnapshotEntry
	MedianFee     uint64
	LowFee        uint64
	HighFee       uint64
	EstimatedNext time.Duration
}

type dashboardTPSChart struct {
	Label       string
	Blocks      []dashboardThroughputBlock
	TotalTx     int
	AvgTPS      float64
	AvgTxPerBlk float64
}

type dashboardThroughputBlock struct {
	TxCount   int
	Candidate bool
	BarWidth  int
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
	PubKey    [32]byte
	BlockHash [32]byte
}

type StressLaneInfo struct {
	ReservePubKey   string `json:"reserve_pubkey"`
	ReserveUTXOs    int    `json:"reserve_utxos"`
	ReserveAtoms    uint64 `json:"reserve_atoms"`
	PendingBatches  int    `json:"pending_batches"`
	PendingOutputs  int    `json:"pending_outputs"`
	ReadyOutputs    int    `json:"ready_outputs"`
	LastPendingTxID string `json:"last_pending_txid,omitempty"`
}

const (
	dashboardSystemSampleInterval     = 10 * time.Second
	dashboardSystemMinimumWindow      = 10 * time.Second
	dashboardSystemWindow             = 10 * time.Minute
	dashboardSystemRetention          = 12 * time.Minute
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

type pendingThinBlock struct {
	hash   [32]byte
	header types.BlockHeader
	txs    []types.Transaction
	filled []bool
}

type stressLaneBatch struct {
	TxID      [32]byte
	Outputs   []FundingOutput
	CreatedAt time.Time
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
		svc.rememberKnownPeers(cfg.Peers)
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

func (s *Service) scheduleMempoolPersistence() {
	if s == nil || s.mempoolPersistCh == nil {
		return
	}
	select {
	case s.mempoolPersistCh <- struct{}{}:
	default:
	}
}

func (s *Service) mempoolPersistLoop() {
	const persistDebounce = 25 * time.Millisecond
	var timer *time.Timer
	var timerCh <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.mempoolPersistCh:
			if timer == nil {
				timer = time.NewTimer(persistDebounce)
				timerCh = timer.C
				continue
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(persistDebounce)
		case <-timerCh:
			timerCh = nil
			if err := s.flushMempoolPersistence(); err != nil && s.logger != nil {
				s.logger.Warn("mempool persistence flush failed", slog.Any("error", err))
			}
		}
	}
}

func (s *Service) flushMempoolPersistence() error {
	if s == nil || s.chainState == nil || s.chainState.Store() == nil {
		return nil
	}
	s.mempoolPersistMu.Lock()
	defer s.mempoolPersistMu.Unlock()

	snapshot, ok := s.buildPersistableMempoolSnapshot()
	if !ok {
		if !s.mempoolPersistState.Valid {
			return nil
		}
		if err := s.chainState.Store().ClearMempoolState(); err != nil {
			return err
		}
		s.mempoolPersistState = persistedMempoolState{}
		return nil
	}
	delta, changed := buildStoredMempoolDelta(snapshot, s.mempoolPersistState)
	if !changed {
		return nil
	}
	if err := s.chainState.Store().ApplyMempoolStateDelta(delta); err != nil {
		return err
	}
	s.mempoolPersistState = snapshot.State
	return nil
}

func (s *Service) buildPersistableMempoolSnapshot() (livePersistedMempoolSnapshot, bool) {
	if s == nil || s.pool == nil || s.chainState == nil {
		return livePersistedMempoolSnapshot{}, false
	}
	tip, ok := s.chainState.tipSnapshot()
	if !ok {
		return livePersistedMempoolSnapshot{}, false
	}
	entries := s.pool.PersistedEntries()
	orphans := s.pool.PersistedOrphans()
	if len(entries) == 0 && len(orphans) == 0 {
		return livePersistedMempoolSnapshot{}, false
	}
	snapshot := livePersistedMempoolSnapshot{
		State: persistedMempoolState{
			Valid: true,
			Meta: storage.StoredMempoolStateMeta{
				Version:   2,
				Profile:   s.cfg.Profile,
				TipHeight: tip.Height,
				TipHash:   tip.TipHash,
			},
			Entries: make(map[[32]byte]persistedMempoolEntryFingerprint, len(entries)),
			Orphans: make(map[[32]byte]persistedMempoolOrphanFingerprint, len(orphans)),
		},
		Entries: make(map[[32]byte]mempool.PersistedEntry, len(entries)),
		Orphans: make(map[[32]byte]mempool.PersistedOrphan, len(orphans)),
	}
	for _, entry := range entries {
		snapshot.State.Entries[entry.TxID] = persistedMempoolEntryFingerprint{
			AuthID:  entry.AuthID,
			Summary: entry.Summary,
			AddedAt: entry.AddedAt,
		}
		snapshot.Entries[entry.TxID] = entry
	}
	for _, orphan := range orphans {
		snapshot.State.Orphans[orphan.TxID] = persistedMempoolOrphanFingerprint{
			AuthID:  orphan.AuthID,
			AddedAt: orphan.AddedAt,
			Missing: append([]types.OutPoint(nil), orphan.Missing...),
		}
		snapshot.Orphans[orphan.TxID] = orphan
	}
	return snapshot, true
}

func buildStoredMempoolDelta(current livePersistedMempoolSnapshot, previous persistedMempoolState) (storage.StoredMempoolStateDelta, bool) {
	delta := storage.StoredMempoolStateDelta{
		Meta: current.State.Meta,
	}
	changed := !previous.Valid || previous.Meta != current.State.Meta

	for txid, entry := range current.Entries {
		if fingerprint, ok := previous.Entries[txid]; ok && samePersistedEntryFingerprint(fingerprint, entry) {
			continue
		}
		delta.EntryUpserts = append(delta.EntryUpserts, storage.StoredMempoolDeltaEntry{
			TxID: txid,
			Entry: storage.StoredMempoolEntry{
				Tx:      entry.Tx.Encode(),
				Summary: entry.Summary,
				AddedAt: entry.AddedAt,
			},
		})
		changed = true
	}
	for txid := range previous.Entries {
		if _, ok := current.State.Entries[txid]; ok {
			continue
		}
		delta.EntryDeletes = append(delta.EntryDeletes, txid)
		changed = true
	}
	for txid, orphan := range current.Orphans {
		if fingerprint, ok := previous.Orphans[txid]; ok && samePersistedOrphanFingerprint(fingerprint, orphan) {
			continue
		}
		delta.OrphanUpserts = append(delta.OrphanUpserts, storage.StoredMempoolDeltaEntry{
			TxID: txid,
			Entry: storage.StoredMempoolEntry{
				Tx:      orphan.Tx.Encode(),
				AddedAt: orphan.AddedAt,
				Missing: append([]types.OutPoint(nil), orphan.Missing...),
			},
		})
		changed = true
	}
	for txid := range previous.Orphans {
		if _, ok := current.State.Orphans[txid]; ok {
			continue
		}
		delta.OrphanDeletes = append(delta.OrphanDeletes, txid)
		changed = true
	}
	return delta, changed
}

func samePersistedEntryFingerprint(fingerprint persistedMempoolEntryFingerprint, entry mempool.PersistedEntry) bool {
	return fingerprint.AuthID == entry.AuthID &&
		fingerprint.Summary == entry.Summary &&
		fingerprint.AddedAt == entry.AddedAt
}

func samePersistedOrphanFingerprint(fingerprint persistedMempoolOrphanFingerprint, orphan mempool.PersistedOrphan) bool {
	return fingerprint.AuthID == orphan.AuthID &&
		fingerprint.AddedAt == orphan.AddedAt &&
		slices.Equal(fingerprint.Missing, orphan.Missing)
}

func (s *Service) reloadPersistedMempool() error {
	stored, err := s.chainState.Store().LoadMempoolState()
	if err != nil {
		return err
	}
	if stored == nil {
		return nil
	}
	if stored.Profile != s.cfg.Profile {
		s.logger.Warn("discarding persisted mempool for different profile",
			slog.String("stored_profile", stored.Profile.String()),
			slog.String("active_profile", s.cfg.Profile.String()),
		)
		return s.chainState.Store().ClearMempoolState()
	}
	accepted := make([]mempool.PersistedEntry, 0, len(stored.Entries))
	for i, entry := range stored.Entries {
		tx, err := types.DecodeTransactionWithLimits(entry.Tx, types.DefaultCodecLimits())
		if err != nil {
			s.logger.Warn("discarding persisted mempool after decode failure",
				slog.Int("entry_index", i),
				slog.Any("error", err),
			)
			_ = s.chainState.Store().ClearMempoolState()
			return nil
		}
		accepted = append(accepted, mempool.PersistedEntry{
			Tx:      tx,
			Summary: entry.Summary,
			AddedAt: entry.AddedAt,
		})
	}
	orphans := make([]mempool.PersistedOrphan, 0, len(stored.Orphans))
	for i, entry := range stored.Orphans {
		tx, err := types.DecodeTransactionWithLimits(entry.Tx, types.DefaultCodecLimits())
		if err != nil {
			s.logger.Warn("discarding persisted orphan after decode failure",
				slog.Int("orphan_index", i),
				slog.Any("error", err),
			)
			_ = s.chainState.Store().ClearMempoolState()
			return nil
		}
		orphans = append(orphans, mempool.PersistedOrphan{
			Tx:      tx,
			AddedAt: entry.AddedAt,
			Missing: append([]types.OutPoint(nil), entry.Missing...),
		})
	}
	tip, ok := s.chainState.tipSnapshot()
	if !ok {
		return nil
	}
	if stored.TipHeight == tip.Height && stored.TipHash == tip.TipHash {
		if err := s.pool.RestorePersistedState(accepted, orphans); err != nil {
			return err
		}
		s.logger.Info("restored mempool from persisted state",
			slog.Int("entries", len(accepted)),
			slog.Int("orphans", len(orphans)),
			slog.Uint64("tip_height", tip.Height),
		)
		return s.flushMempoolPersistence()
	}
	reprocess := make([]types.Transaction, 0, len(accepted)+len(orphans))
	for _, entry := range accepted {
		reprocess = append(reprocess, entry.Tx)
	}
	for _, orphan := range orphans {
		reprocess = append(reprocess, orphan.Tx)
	}
	admissions, errs, acceptedTxs := s.reprocessTransactions(reprocess, "mempool_reload", false)
	s.logger.Info("reprocessed persisted mempool against current tip",
		slog.Int("submitted", len(reprocess)),
		slog.Int("accepted", countAcceptedAdmissions(admissions)),
		slog.Int("rejected", countNonNilErrors(errs)),
		slog.Int("promoted", len(acceptedTxs)),
		slog.Uint64("stored_tip_height", stored.TipHeight),
		slog.Uint64("active_tip_height", tip.Height),
	)
	return s.flushMempoolPersistence()
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
		s.safeGo("rpc-server", func() {
			if err := s.rpcSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Error("rpc server failed", slog.String("addr", s.cfg.RPCAddr), slog.Any("error", err))
			}
		})
	}
	if s.cfg.P2PAddr != "" {
		ln, err := net.Listen("tcp", s.cfg.P2PAddr)
		if err != nil {
			return err
		}
		s.listener = ln
		s.logger.Info("p2p listener enabled", slog.String("addr", s.cfg.P2PAddr))
		s.safeGo("accept-loop", func() {
			s.acceptLoop()
		})
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
	if req, ok := msg.(p2p.TxRequestMessage); ok {
		return p.enqueueTxRequest(req)
	}
	if batch, ok := msg.(p2p.TxBatchMessage); ok {
		return p.enqueueTxBatch(batch)
	}
	envelope := outboundMessage{
		msg:        msg,
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneControl,
		class:      classifyRelayMessage(msg),
	}
	return p.enqueueDirectMessage(envelope)
}

const (
	relaySuppressionLogThreshold = 8
	relayDropWarnThreshold       = 16
	relayQueueWarnDepth          = 384
)

func (p *peerConn) enqueueDirectMessage(envelope outboundMessage) error {
	q := p.sendQ
	if p.controlQ != nil {
		q = p.controlQ
		envelope.lane = relayQueueLaneControl
	} else {
		envelope.lane = relayQueueLaneSend
	}

	// Fast path: the queue usually has room immediately, so avoid allocating a
	// timer unless we actually need to wait for backpressure to clear.
	select {
	case <-p.closed:
		return io.EOF
	case q <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
	}

	timer := time.NewTimer(controlMessageEnqueueTimeout)
	defer timer.Stop()
	select {
	case <-p.closed:
		return io.EOF
	case q <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	case <-timer.C:
		p.telemetry.noteWriterStarvation(relayQueueLaneControl)
		if p.svc != nil {
			p.svc.noteWriterStarvation(1)
			p.logRelayQueuePressure(slog.LevelWarn, "control queue enqueue timed out", relayQueueLaneControl, envelope.class, 1)
		}
		return errors.New("peer send queue saturated")
	}
}

func (p *peerConn) enqueueTxRecon(msg p2p.TxReconMessage) error {
	return p.enqueueTxReconWithPolicy(msg, true)
}

// Rebroadcast retries intentionally bypass the known-tx cache so peers that
// only saw a prior announcement get another chance to request the full tx.
func (p *peerConn) enqueueTxReconRetry(msg p2p.TxReconMessage) error {
	if len(msg.TxIDs) > 0 {
		p.telemetry.noteTxReconRetry(len(msg.TxIDs))
		if p.svc != nil {
			p.svc.noteTxReconRetry(len(msg.TxIDs))
		}
	}
	return p.enqueueTxReconWithPolicy(msg, false)
}

func (p *peerConn) enqueueTxReconWithPolicy(msg p2p.TxReconMessage, suppressKnown bool) error {
	filtered := p.filterQueuedTxIDs(msg.TxIDs, suppressKnown)
	if len(filtered) == 0 {
		return nil
	}
	immediate, armFlush := p.stagePendingRecon(filtered)
	for _, batch := range immediate {
		p.enqueueRelayRecon(batch)
	}
	if armFlush {
		if p.svc != nil {
			p.svc.safeGoDetachedWithCleanup("peer-flush-pending-recon", p.clearPendingReconFlushArm, func() {
				p.flushPendingReconAfterDelay()
			})
		} else {
			go p.flushPendingReconAfterDelay()
		}
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
		lane:       relayQueueLaneSend,
		class:      classifyRelayMessage(p2p.InvMessage{Items: items}),
		invItems:   items,
	}
	q := p.sendQ
	if priority && p.relayPriorityQ != nil {
		q = p.relayPriorityQ
		envelope.lane = relayQueueLanePriority
	}
	select {
	case <-p.closed:
		p.releaseQueuedInv(items)
		return io.EOF
	case q <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
		p.releaseQueuedInv(items)
		p.telemetry.noteDroppedInv(len(items), envelope.lane)
		if priority || len(items) >= relayDropWarnThreshold || p.queueDepths().total >= relayQueueWarnDepth {
			level := slog.LevelWarn
			if !priority {
				level = slog.LevelDebug
			}
			p.logRelayQueuePressure(level, "dropped relay inv due to saturated queue", envelope.lane, envelope.class, len(items))
		}
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
		if p.svc != nil {
			p.svc.safeGoDetachedWithCleanup("peer-flush-pending-txs", p.clearPendingTxFlushArm, func() {
				p.flushPendingTxsAfterDelay()
			})
		} else {
			go p.flushPendingTxsAfterDelay()
		}
	}
	return nil
}

func (p *peerConn) enqueueLocalTxBatch(msg p2p.TxBatchMessage) error {
	filtered := p.filterQueuedTxs(msg.Txs)
	if len(filtered) == 0 {
		return nil
	}
	batch := p2p.TxBatchMessage{Txs: filtered}
	envelope := outboundMessage{
		msg:        batch,
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneSend,
		class:      classifyRelayMessage(batch),
	}
	select {
	case <-p.closed:
		p.releaseQueuedTxs(filtered)
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
	}

	timer := time.NewTimer(p.localTxBatchEnqueueTimeout())
	defer timer.Stop()
	select {
	case <-p.closed:
		p.releaseQueuedTxs(filtered)
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	case <-timer.C:
		p.releaseQueuedTxs(filtered)
		p.telemetry.noteDroppedTxs(len(filtered), envelope.lane)
		p.logRelayQueuePressure(slog.LevelWarn, "dropped local tx batch after send queue backpressure timeout", envelope.lane, envelope.class, len(filtered))
		return errors.New("peer send queue saturated")
	}
}

func (p *peerConn) localTxBatchEnqueueTimeout() time.Duration {
	timeout := defaultLocalTxBatchEnqueueTimeout
	if p != nil && p.svc != nil && p.svc.cfg.StallTimeout > 0 {
		timeout = p.svc.cfg.StallTimeout / 3
	}
	if timeout < defaultLocalTxBatchEnqueueTimeout {
		return defaultLocalTxBatchEnqueueTimeout
	}
	if timeout > maxLocalTxBatchEnqueueTimeout {
		return maxLocalTxBatchEnqueueTimeout
	}
	return timeout
}

func (p *peerConn) enqueueTxRequest(msg p2p.TxRequestMessage) error {
	if len(msg.TxIDs) == 0 {
		return nil
	}
	immediate, armFlush := p.stagePendingTxRequests(msg.TxIDs)
	for _, batch := range immediate {
		if err := p.enqueueRelayTxRequest(batch); err != nil {
			return err
		}
	}
	if armFlush {
		if p.svc != nil {
			p.svc.safeGoDetachedWithCleanup("peer-flush-pending-txreq", p.clearPendingTxRequestFlushArm, func() {
				p.flushPendingTxRequestsAfterDelay()
			})
		} else {
			go p.flushPendingTxRequestsAfterDelay()
		}
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
		lane:       relayQueueLanePriority,
		class:      classifyRelayMessage(msg),
	}
	if p.relayPriorityQ != nil {
		select {
		case <-p.closed:
			return io.EOF
		case p.relayPriorityQ <- envelope:
			p.telemetry.noteEnqueue(p.queueDepths())
			return nil
		default:
		}
	}
	envelope.lane = relayQueueLaneSend
	select {
	case <-p.closed:
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
		p.telemetry.noteDroppedTxs(len(msg.Txs), envelope.lane)
		if len(msg.Txs) >= relayDropWarnThreshold || p.queueDepths().send >= relayQueueWarnDepth {
			p.logRelayQueuePressure(slog.LevelWarn, "dropped requested tx batch due to saturated relay queues", envelope.lane, envelope.class, len(msg.Txs))
		}
		return nil
	}
}

func (p *peerConn) enqueueFallbackTxBatch(msg p2p.TxBatchMessage) error {
	if len(msg.Txs) == 0 {
		return nil
	}
	envelope := outboundMessage{
		msg:        msg,
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneSend,
		class: relayMessageClass{
			txBatchMsgs:          1,
			txBatchItems:         len(msg.Txs),
			fallbackTxBatchMsgs:  1,
			fallbackTxBatchItems: len(msg.Txs),
		},
	}
	select {
	case <-p.closed:
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
		p.telemetry.noteDroppedTxs(len(msg.Txs), envelope.lane)
		if len(msg.Txs) >= relayDropWarnThreshold || p.queueDepths().send >= relayQueueWarnDepth {
			p.logRelayQueuePressure(slog.LevelWarn, "dropped fallback tx batch due to saturated send queue", envelope.lane, envelope.class, len(msg.Txs))
		}
		return nil
	}
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
	return p.queueDepths().total
}

func (p *peerConn) queueDepths() queueDepthSnapshot {
	depths := queueDepthSnapshot{send: len(p.sendQ)}
	if p.relayPriorityQ != nil {
		depths.priority = len(p.relayPriorityQ)
	}
	if p.controlQ != nil {
		depths.control = len(p.controlQ)
	}
	depths.total = depths.control + depths.priority + depths.send
	return depths
}

func (p *peerConn) logRelayQueuePressure(level slog.Level, msg string, lane relayQueueLane, class relayMessageClass, items int) {
	if p == nil || p.svc == nil || p.svc.logger == nil {
		return
	}
	depths := p.queueDepths()
	attrs := []slog.Attr{
		slog.String("addr", p.addr),
		slog.String("lane", lane.String()),
		slog.Int("items", items),
		slog.Int("queue_depth", depths.total),
		slog.Int("control_queue_depth", depths.control),
		slog.Int("priority_queue_depth", depths.priority),
		slog.Int("send_queue_depth", depths.send),
	}
	attrs = append(attrs, class.logAttrs()...)
	p.svc.logger.LogAttrs(context.Background(), level, msg, attrs...)
}

func (p *peerConn) maybeLogRelaySuppression(kind string, duplicateQueued int, suppressedKnown int, class relayMessageClass) {
	if p == nil || p.svc == nil || p.svc.logger == nil {
		return
	}
	total := duplicateQueued + suppressedKnown
	if total < relaySuppressionLogThreshold && class.blockInvItems == 0 && class.blockSendItems == 0 && class.blockReqItems == 0 {
		return
	}
	attrs := []slog.Attr{
		slog.String("addr", p.addr),
		slog.String("kind", kind),
		slog.Int("duplicate_queued", duplicateQueued),
		slog.Int("suppressed_known", suppressedKnown),
	}
	attrs = append(attrs, class.logAttrs()...)
	p.svc.logger.LogAttrs(context.Background(), slog.LevelDebug, "suppressed relay work before enqueue", attrs...)
}

func (p *peerConn) clearRelayState() {
	p.invMu.Lock()
	p.queuedInv = make(map[p2p.InvVector]int)
	p.invMu.Unlock()

	p.txMu.Lock()
	p.queuedTx = make(map[[32]byte]int)
	p.knownTx = make(map[[32]byte]struct{})
	p.knownTxOrder = nil
	p.knownTxNext = 0
	p.pendingTxOrder = nil
	p.pendingTxByID = nil
	p.pendingRecon = nil
	p.localRelayTxs = nil
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
	if budget := relayLaneBudget(envelope.lane); budget > 0 && time.Since(envelope.enqueuedAt) > budget {
		peer.telemetry.noteWriterStarvation(envelope.lane)
		s.noteWriterStarvation(1)
		peer.logRelayQueuePressure(slog.LevelWarn, "relay lane exceeded latency budget", envelope.lane, envelope.class, 1)
	}
	s.noteRelaySent(envelope.class)
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
		if s.cfg.StaticPeerTopology {
			return peer.send(p2p.AddrMessage{})
		}
		return peer.send(p2p.AddrMessage{Addrs: s.knownPeerAddrs()})
	case p2p.AddrMessage:
		if s.cfg.StaticPeerTopology {
			return nil
		}
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
			s.logger.Debug("applied peer headers",
				slog.String("addr", peer.addr),
				slog.Int("count", applied),
				slog.Uint64("header_height", s.headerHeight()),
				slog.String("headers", headersDebugSummary(m.Headers, 4)),
			)
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
		allowed, err := peer.allowInboundTxs(1)
		if err != nil {
			if errors.Is(err, errPeerInboundTxRateLimit) {
				s.logger.Warn("disconnecting peer for sustained inbound tx flood",
					slog.String("addr", peer.addr),
					slog.Int("tx_count", 1),
				)
				return err
			}
		}
		if !allowed {
			s.logger.Debug("dropping inbound tx due to rate limit",
				slog.String("addr", peer.addr),
			)
			return nil
		}
		peer.noteKnownTxs(m)
		admissions, errs, _, _ := s.submitDecodedTxsFrom([]types.Transaction{m.Tx}, peer)
		peer.noteUsefulTxs(countAcceptedAdmissions(admissions), time.Now())
		return s.handlePeerTxAdmissionErrors(peer, errs)
	case p2p.TxBatchMessage:
		allowed, err := peer.allowInboundTxs(len(m.Txs))
		if err != nil {
			if errors.Is(err, errPeerInboundTxRateLimit) {
				s.logger.Warn("disconnecting peer for sustained inbound tx flood",
					slog.String("addr", peer.addr),
					slog.Int("tx_count", len(m.Txs)),
				)
				return err
			}
		}
		if !allowed {
			s.logger.Debug("dropping inbound tx batch due to rate limit",
				slog.String("addr", peer.addr),
				slog.Int("tx_count", len(m.Txs)),
			)
			return nil
		}
		peer.noteKnownTxs(m)
		admissions, errs, _, _ := s.submitDecodedTxsFrom(m.Txs, peer)
		peer.noteUsefulTxs(countAcceptedAdmissions(admissions), time.Now())
		return s.handlePeerTxAdmissionErrors(peer, errs)
	case p2p.TxReconMessage:
		peer.noteKnownTxIDs(m.TxIDs)
		return s.onTxReconMessage(peer, m)
	case p2p.TxRequestMessage:
		return s.onTxRequestMessage(peer, m)
	case p2p.AvaPollMessage:
		return s.avalancheManager().onPoll(peer, m)
	case p2p.AvaVoteMessage:
		s.avalancheManager().onVote(peer, m)
		return nil
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
	view, haveView := s.chainState.sharedCommittedView()
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
		Avalanche:      s.avalancheManager().info(),
		MinerEnabled:   s.cfg.MinerEnabled,
		MinerWorkers:   s.cfg.MinerWorkers,
		GenesisFixture: s.cfg.GenesisFixture,
	}
}

func (s *Service) ChainStateInfo() ChainStateInfo {
	s.stateMu.RLock()
	view, ok := s.chainState.sharedCommittedView()
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
		UTXOChecksum:       hex.EncodeToString(view.UTXOChecksum[:]),
		UTXOCount:          view.UTXOCount,
		NextBlockSizeLimit: consensus.NextBlockSizeLimit(view.BlockSizeState, consensus.ParamsForProfile(s.cfg.Profile)),
		TipTimestamp:       view.TipHeader.Timestamp,
	}
	if entry, err := s.chainState.Store().GetBlockIndex(&view.TipHash); err == nil && entry != nil {
		info.ChainWork = hex.EncodeToString(entry.ChainWork[:])
	}
	return info
}

func (s *Service) MempoolInfo() MempoolInfo {
	stats := s.pool.Stats()
	info := MempoolInfo{
		Count:              stats.Count,
		Orphans:            stats.Orphans,
		Bytes:              stats.Bytes,
		MaxBytes:           s.cfg.MaxMempoolBytes,
		TotalFees:          stats.TotalFees,
		MedianFee:          stats.MedianFee,
		LowFee:             stats.LowFee,
		HighFee:            stats.HighFee,
		MinRelayFeePerByte: s.cfg.MinRelayFeePerByte,
		CandidateFrontier:  s.pool.SelectionCandidateCount(),
	}
	info.AvalancheConflicts, info.AvalancheFinalized = s.avalancheManager().metricsForMempool()
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
	if s.cfg.MinerPubKey != ([32]byte{}) {
		info.MinerPubKey = hex.EncodeToString(s.cfg.MinerPubKey[:])
	}
	s.stateMu.RLock()
	view, ok := s.chainState.sharedCommittedView()
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
	startedAt := time.Now()
	admission, err, orphanCount, mempoolSize := s.submitSingleDecodedTxFrom(tx, nil)
	if err != nil {
		s.logger.Warn("transaction rejected",
			slog.Any("error", err),
			slog.Duration("admit_duration", time.Since(startedAt)),
		)
		return mempool.Admission{}, err
	}
	if admission.Orphaned {
		s.logger.Debug("transaction stored as orphan",
			slog.String("txid", hex.EncodeToString(admission.TxID[:])),
			slog.Int("inputs", len(tx.Base.Inputs)),
			slog.Int("outputs", len(tx.Base.Outputs)),
			slog.Int("orphan_count", orphanCount),
			slog.Int("evicted_orphans", admission.EvictedOrphans),
			slog.Duration("admit_duration", time.Since(startedAt)),
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
		slog.Duration("admit_duration", time.Since(startedAt)),
	)
	return admission, nil
}

// SubmitTxBatch admits a local batch of already-decoded transactions through
// the same policy path used by packed RPC and peer batch relay. Results stay
// positional: a non-nil error at index i belongs to txs[i].
func (s *Service) SubmitTxBatch(txs []types.Transaction) ([]mempool.Admission, []error) {
	if len(txs) == 0 {
		return nil, nil
	}
	admissions, errs, _, _ := s.submitDecodedTxs(txs)
	return admissions, errs
}

func (s *Service) submitSingleDecodedTxFrom(tx types.Transaction, source *peerConn) (mempool.Admission, error, int, int) {
	startedAt := time.Now()
	s.releaseTxRequestsForTransactions([]types.Transaction{tx})
	admission := mempool.Admission{}
	var err error
	orphanCount := 0
	mempoolSize := 0
	defer func() {
		errorCount := 0
		if err != nil {
			errorCount = 1
		}
		s.perf.noteAdmissionDuration(time.Since(startedAt))
		attrs := []slog.Attr{
			slog.Int("tx_count", 1),
			slog.Int("accepted_txs", len(admission.Accepted)),
			slog.Int("error_count", errorCount),
			slog.Int("orphan_count", orphanCount),
			slog.Int("mempool_size", mempoolSize),
			slog.Duration("admit_duration", time.Since(startedAt)),
		}
		if source != nil {
			attrs = append(attrs, slog.String("source_addr", source.addr))
		} else {
			attrs = append(attrs, slog.String("source_addr", "local"))
		}
		s.logger.LogAttrs(context.Background(), slog.LevelDebug, "tx admission batch complete", attrs...)
	}()

	if err = s.rejectCache.lookup(tx, s.pool.Epoch(), s.chainTipHash()); err != nil {
		orphanCount = s.pool.OrphanCount()
		mempoolSize = s.pool.Count()
		return admission, err, orphanCount, mempoolSize
	}
	if err = s.avalancheManager().rejectionError(tx); err != nil {
		orphanCount = s.pool.OrphanCount()
		mempoolSize = s.pool.Count()
		s.rejectCache.remember(tx, err, s.pool.Epoch(), s.chainTipHash())
		return admission, err, orphanCount, mempoolSize
	}

	rules := consensus.DefaultConsensusRules()
	params := consensus.ParamsForProfile(s.cfg.Profile)
	admission, err = s.pool.AcceptTxWithLookupAndParams(tx, s.chainUtxoSnapshot(), params, rules)
	orphanCount = s.pool.OrphanCount()
	mempoolSize = s.pool.Count()
	if err != nil {
		s.rejectCache.remember(tx, err, s.pool.Epoch(), s.chainTipHash())
		if errors.Is(err, mempool.ErrInputAlreadySpent) {
			s.avalancheManager().trackTx(tx, false, time.Now())
		}
		return admission, err, orphanCount, mempoolSize
	}

	s.noteAcceptedAdmissions([]mempool.Admission{admission})
	if len(admission.Accepted) > 0 {
		s.invalidateBlockTemplate("tx_admission")
		now := time.Now()
		for _, accepted := range admission.Accepted {
			s.avalancheManager().trackTx(accepted.Tx, true, now)
		}
		if source == nil && !admission.Orphaned {
			s.rebroadcastMu.Lock()
			s.localRebroadcast[consensus.TxID(&tx)] = now
			s.rebroadcastMu.Unlock()
		}
		s.relayAcceptedTxs(s.peerSnapshotExcluding(source), admission.Accepted, source)
	}
	if admission.Orphaned {
		s.scheduleMempoolPersistence()
	} else if len(admission.Accepted) > 0 {
		s.scheduleMempoolPersistence()
	}
	return admission, nil, orphanCount, mempoolSize
}

func (s *Service) UTXOsByWatchItems(items []compactfilter.WatchItem) []PubKeyUTXO {
	if len(items) == 0 {
		return nil
	}
	wanted := make(map[compactfilter.WatchItem]struct{}, len(items))
	for _, item := range items {
		wanted[item] = struct{}{}
	}
	out := make([]PubKeyUTXO, 0)
	if err := s.chainState.Store().ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		item := compactFilterItemForUTXO(entry)
		if _, ok := wanted[item]; !ok {
			return nil
		}
		out = append(out, PubKeyUTXO{
			OutPoint:  outPoint,
			Value:     entry.ValueAtoms,
			Type:      item.Type,
			Payload32: item.Payload32,
			PubKey:    entry.PubKey,
		})
		return nil
	}); err != nil {
		s.logger.Warn("scan utxos by watch item failed", slog.Any("error", err))
		return nil
	}
	slices.SortFunc(out, func(a, b PubKeyUTXO) int {
		switch {
		case a.Type < b.Type:
			return -1
		case a.Type > b.Type:
			return 1
		}
		if cmp := bytes.Compare(a.Payload32[:], b.Payload32[:]); cmp != 0 {
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

func (s *Service) UTXOsByPubKeys(pubKeys [][32]byte) []PubKeyUTXO {
	items := make([]compactfilter.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, compactfilter.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return s.UTXOsByWatchItems(items)
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

func (s *Service) WalletActivityByWatchItems(items []compactfilter.WatchItem, limit int) ([]WalletActivity, error) {
	if len(items) == 0 {
		return nil, nil
	}
	s.stateMu.RLock()
	view, ok := s.chainState.sharedCommittedView()
	s.stateMu.RUnlock()
	if !ok {
		return nil, ErrNoTip
	}
	wanted := make(map[compactfilter.WatchItem]struct{}, len(items))
	for _, item := range items {
		wanted[item] = struct{}{}
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

func (s *Service) WalletActivityByPubKeys(pubKeys [][32]byte, limit int) ([]WalletActivity, error) {
	items := make([]compactfilter.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, compactfilter.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return s.WalletActivityByWatchItems(items, limit)
}

func walletActivityFromBlock(height uint64, hash [32]byte, block *types.Block, undo []storage.BlockUndoEntry, wanted map[compactfilter.WatchItem]struct{}) ([]WalletActivity, error) {
	if block == nil {
		return nil, nil
	}
	timestamp := time.Unix(int64(block.Header.Timestamp), 0).UTC()
	out := make([]WalletActivity, 0, len(block.Txs))
	undoIndex := 0
	for i, tx := range block.Txs {
		received := uint64(0)
		for _, output := range tx.Base.Outputs {
			if _, ok := wanted[compactFilterItemForOutput(output)]; ok {
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
				if _, ok := wanted[compactFilterItemForUTXO(entry.Entry)]; ok {
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

func compactFilterItemForOutput(output types.TxOutput) compactfilter.WatchItem {
	item := compactfilter.WatchItem{Type: output.Type, Payload32: output.Payload32}
	if item.Type == types.OutputXOnlyP2PK && item.Payload32 == ([32]byte{}) {
		item.Payload32 = output.PubKey
	}
	return item
}

func compactFilterItemForUTXO(entry consensus.UtxoEntry) compactfilter.WatchItem {
	item := compactfilter.WatchItem{Type: entry.Type, Payload32: entry.Payload32}
	if item.Type == types.OutputXOnlyP2PK && item.Payload32 == ([32]byte{}) {
		item.Payload32 = entry.PubKey
	}
	return item
}

// The node-side accumulator/template paths must build the exact same typed leaf
// payloads that consensus commits under utxo_root, otherwise mined blocks can
// drift from the validator's canonical UTXO root when non-x-only output
// families appear in the block.
func utxoLeafForOutput(outPoint types.OutPoint, output types.TxOutput) utreexo.UtxoLeaf {
	item := compactFilterItemForOutput(output)
	return utreexo.UtxoLeaf{
		OutPoint:   outPoint,
		Type:       item.Type,
		ValueAtoms: output.ValueAtoms,
		Payload32:  item.Payload32,
		PubKey:     output.PubKey,
	}
}

func utxoLeafForEntry(outPoint types.OutPoint, entry consensus.UtxoEntry) utreexo.UtxoLeaf {
	item := compactFilterItemForUTXO(entry)
	return utreexo.UtxoLeaf{
		OutPoint:   outPoint,
		Type:       item.Type,
		ValueAtoms: entry.ValueAtoms,
		Payload32:  item.Payload32,
		PubKey:     entry.PubKey,
	}
}

func (s *Service) submitDecodedTxs(txs []types.Transaction) ([]mempool.Admission, []error, int, int) {
	return s.submitDecodedTxsFrom(txs, nil)
}

func (s *Service) submitDecodedTxsFrom(txs []types.Transaction, source *peerConn) ([]mempool.Admission, []error, int, int) {
	startedAt := time.Now()
	s.releaseTxRequestsForTransactions(txs)
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	acceptedCount := 0
	orphanCount := 0
	mempoolSize := 0
	defer func() {
		if len(txs) == 0 {
			return
		}
		s.perf.noteAdmissionDuration(time.Since(startedAt))
		errorCount := 0
		for _, err := range errs {
			if err != nil {
				errorCount++
			}
		}
		for _, admission := range admissions {
			acceptedCount += len(admission.Accepted)
		}
		attrs := []slog.Attr{
			slog.Int("tx_count", len(txs)),
			slog.Int("accepted_txs", acceptedCount),
			slog.Int("error_count", errorCount),
			slog.Int("orphan_count", orphanCount),
			slog.Int("mempool_size", mempoolSize),
			slog.Duration("admit_duration", time.Since(startedAt)),
		}
		if source != nil {
			attrs = append(attrs, slog.String("source_addr", source.addr))
		} else {
			attrs = append(attrs, slog.String("source_addr", "local"))
		}
		s.logger.LogAttrs(context.Background(), slog.LevelDebug, "tx admission batch complete", attrs...)
	}()
	if len(txs) == 1 {
		if err := s.rejectCache.lookup(txs[0], s.pool.Epoch(), s.chainTipHash()); err != nil {
			errs[0] = err
			orphanCount = s.pool.OrphanCount()
			mempoolSize = s.pool.Count()
			return admissions, errs, orphanCount, mempoolSize
		}
		freshAdmissions, freshErrs, orphanCount, mempoolSize := s.submitFreshDecodedTxsFrom(txs, source)
		admissions[0] = freshAdmissions[0]
		errs[0] = freshErrs[0]
		s.noteRejectedTransactions(txs, errs)
		return admissions, errs, orphanCount, mempoolSize
	}
	pendingTxs, pendingIndexes := s.filterRejectCacheMisses(txs, errs)
	if len(pendingTxs) == 0 {
		orphanCount = s.pool.OrphanCount()
		mempoolSize = s.pool.Count()
		return admissions, errs, orphanCount, mempoolSize
	}
	freshAdmissions, freshErrs, orphanCount, mempoolSize := s.submitFreshDecodedTxsFrom(pendingTxs, source)
	for i, idx := range pendingIndexes {
		admissions[idx] = freshAdmissions[i]
		errs[idx] = freshErrs[i]
	}
	s.noteRejectedTransactions(pendingTxs, freshErrs)
	return admissions, errs, orphanCount, mempoolSize
}

func (s *Service) submitFreshDecodedTxsFrom(txs []types.Transaction, source *peerConn) ([]mempool.Admission, []error, int, int) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	rules := consensus.DefaultConsensusRules()
	params := consensus.ParamsForProfile(s.cfg.Profile)
	if len(txs) == 1 {
		if err := s.avalancheManager().rejectionError(txs[0]); err != nil {
			errs[0] = err
			orphanCount := s.pool.OrphanCount()
			mempoolSize := s.pool.Count()
			return admissions, errs, orphanCount, mempoolSize
		}
		admission, err := s.pool.AcceptTxWithLookupAndParams(txs[0], s.chainUtxoSnapshot(), params, rules)
		if err != nil {
			errs[0] = err
		} else {
			admissions[0] = admission
			accepted = append(accepted, admission.Accepted...)
		}
		orphanCount := s.pool.OrphanCount()
		mempoolSize := s.pool.Count()
		s.noteAcceptedAdmissions(admissions)
		if len(accepted) > 0 {
			s.invalidateBlockTemplate("tx_admission")
		}
		s.avalancheManager().noteAcceptedAdmissions(admissions)
		s.avalancheManager().noteRejectedConflicts(txs, errs)
		s.noteLocalOriginAcceptedTxs(txs, admissions, errs, source)
		s.relayAcceptedTxs(s.peerSnapshotExcluding(source), accepted, source)
		if mempoolAdmissionsChanged(admissions) {
			s.scheduleMempoolPersistence()
		}
		return admissions, errs, orphanCount, mempoolSize
	}
	if batchHasInternalDependencies(txs) {
		admissions, errs, accepted = s.submitDependentDecodedTxs(txs, rules)
		orphanCount := s.pool.OrphanCount()
		mempoolSize := s.pool.Count()
		s.noteAcceptedAdmissions(admissions)
		if len(accepted) > 0 {
			s.invalidateBlockTemplate("tx_admission")
		}
		s.avalancheManager().noteAcceptedAdmissions(admissions)
		s.avalancheManager().noteRejectedConflicts(txs, errs)
		s.noteLocalOriginAcceptedTxs(txs, admissions, errs, source)
		s.relayAcceptedTxs(s.peerSnapshotExcluding(source), accepted, source)
		if mempoolAdmissionsChanged(admissions) {
			s.scheduleMempoolPersistence()
		}
		return admissions, errs, orphanCount, mempoolSize
	}
	tipHash, chainUtxos := s.chainUtxoSnapshotWithTip()
	view := s.pool.AcquireSharedAdmissionView()
	prepared, prepareErrs := s.prepareAdmissionsParallel(txs, view, chainUtxos, tipHash, rules)
	view.Release()
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
		admission, err := s.pool.CommitPreparedWithLookupAndParams(prepared[i], chainUtxos, tipHash, params, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admissions[i] = admission
		accepted = append(accepted, admission.Accepted...)
	}
	orphanCount := s.pool.OrphanCount()
	mempoolSize := s.pool.Count()
	s.noteAcceptedAdmissions(admissions)
	if len(accepted) > 0 {
		s.invalidateBlockTemplate("tx_admission")
	}
	s.avalancheManager().noteAcceptedAdmissions(admissions)
	s.avalancheManager().noteRejectedConflicts(txs, errs)
	s.noteLocalOriginAcceptedTxs(txs, admissions, errs, source)
	s.relayAcceptedTxs(s.peerSnapshotExcluding(source), accepted, source)
	if mempoolAdmissionsChanged(admissions) {
		s.scheduleMempoolPersistence()
	}
	return admissions, errs, orphanCount, mempoolSize
}

func (s *Service) filterRejectCacheMisses(txs []types.Transaction, errs []error) ([]types.Transaction, []int) {
	if len(txs) == 0 {
		return nil, nil
	}
	poolEpoch := s.pool.Epoch()
	tipHash := s.chainTipHash()
	pending := make([]types.Transaction, 0, len(txs))
	indexes := make([]int, 0, len(txs))
	for i, tx := range txs {
		if err := s.rejectCache.lookup(tx, poolEpoch, tipHash); err != nil {
			errs[i] = err
			continue
		}
		pending = append(pending, tx)
		indexes = append(indexes, i)
	}
	return pending, indexes
}

func (s *Service) noteRejectedTransactions(txs []types.Transaction, errs []error) {
	if len(txs) == 0 || len(txs) != len(errs) {
		return
	}
	poolEpoch := s.pool.Epoch()
	tipHash := s.chainTipHash()
	for i := range txs {
		if errs[i] == nil {
			continue
		}
		s.rejectCache.remember(txs[i], errs[i], poolEpoch, tipHash)
	}
}

func (s *Service) noteLocalOriginAcceptedTxs(txs []types.Transaction, admissions []mempool.Admission, errs []error, source *peerConn) {
	if source != nil || len(txs) == 0 {
		return
	}
	now := time.Now()
	s.rebroadcastMu.Lock()
	defer s.rebroadcastMu.Unlock()
	for i := range txs {
		if errs[i] != nil || admissions[i].Orphaned {
			continue
		}
		// Track only txs explicitly submitted by the local operator path, not
		// extra txs that happened to get promoted alongside them.
		s.localRebroadcast[consensus.TxID(&txs[i])] = now
	}
}

func (s *Service) reprocessTransactions(txs []types.Transaction, templateReason string, relay bool) ([]mempool.Admission, []error, []mempool.AcceptedTx) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	if len(txs) == 0 {
		return admissions, errs, accepted
	}
	rules := consensus.DefaultConsensusRules()
	if batchHasInternalDependencies(txs) {
		admissions, errs, accepted = s.submitDependentDecodedTxs(txs, rules)
	} else {
		admissions, errs, accepted = s.retryDecodedTxSuffix(txs, rules)
	}
	s.noteRejectedTransactions(txs, errs)
	s.noteAcceptedAdmissions(admissions)
	s.avalancheManager().noteAcceptedAdmissions(admissions)
	s.avalancheManager().noteRejectedConflicts(txs, errs)
	if len(accepted) > 0 {
		s.invalidateBlockTemplate(templateReason)
		if relay {
			s.broadcastAcceptedTxsToPeers(s.peerSnapshot(), accepted)
		}
	}
	if mempoolAdmissionsChanged(admissions) {
		s.scheduleMempoolPersistence()
	}
	return admissions, errs, accepted
}

func (s *Service) submitDependentDecodedTxs(txs []types.Transaction, rules consensus.ConsensusRules) ([]mempool.Admission, []error, []mempool.AcceptedTx) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	tipHash, chainUtxos := s.chainUtxoSnapshotWithTip()
	snapshot := s.pool.AdmissionSnapshot()
	retried := false
	var fallbackTipHash [32]byte
	var fallbackChainUtxos consensus.UtxoLookup

	for i, tx := range txs {
		if err := s.avalancheManager().rejectionError(tx); err != nil {
			errs[i] = err
			continue
		}
		if currentTip := s.chainTipHash(); currentTip != tipHash {
			if retried {
				if fallbackTipHash != currentTip {
					fallbackTipHash, fallbackChainUtxos = s.chainUtxoSnapshotWithTip()
				}
				admission, err := s.pool.AcceptTxWithLookupAndParams(tx, fallbackChainUtxos, consensus.ParamsForProfile(s.cfg.Profile), rules)
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
		prepared, err := s.pool.PrepareAdmissionWithLookupAndParams(tx, snapshot, chainUtxos, consensus.ParamsForProfile(s.cfg.Profile), rules)
		if err != nil {
			errs[i] = err
			continue
		}
		prepared.PreparedTip = tipHash
		prepared.HasPreparedTip = true
		admission, err := s.pool.CommitPreparedWithLookupAndParams(prepared, chainUtxos, tipHash, consensus.ParamsForProfile(s.cfg.Profile), rules)
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
		if err := mempool.AdvanceAdmissionSnapshotWithLookup(&snapshot, chainUtxos, admission.Accepted); err != nil {
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
	view := s.pool.AcquireSharedAdmissionView()
	prepared, prepareErrs := s.prepareAdmissionsParallel(txs, view, chainUtxos, tipHash, rules)
	view.Release()
	var fallbackTipHash [32]byte
	var fallbackChainUtxos consensus.UtxoLookup
	for i, tx := range txs {
		if err := s.avalancheManager().rejectionError(tx); err != nil {
			errs[i] = err
			continue
		}
		if prepareErrs[i] != nil {
			errs[i] = prepareErrs[i]
			continue
		}
		if currentTip := s.chainTipHash(); currentTip != tipHash {
			if fallbackTipHash != currentTip {
				fallbackTipHash, fallbackChainUtxos = s.chainUtxoSnapshotWithTip()
			}
			admission, err := s.pool.AcceptTxWithLookupAndParams(tx, fallbackChainUtxos, consensus.ParamsForProfile(s.cfg.Profile), rules)
			if err != nil {
				errs[i] = err
				continue
			}
			admissions[i] = admission
			accepted = append(accepted, admission.Accepted...)
			continue
		}
		admission, err := s.pool.CommitPreparedWithLookupAndParams(prepared[i], chainUtxos, tipHash, consensus.ParamsForProfile(s.cfg.Profile), rules)
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

func (s *Service) prepareAdmissionsParallel(txs []types.Transaction, view mempool.SharedAdmissionView, chainUtxos consensus.UtxoLookup, tipHash [32]byte, rules consensus.ConsensusRules) ([]mempool.PreparedAdmission, []error) {
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
				if err := s.avalancheManager().rejectionError(txs[idx]); err != nil {
					errs[idx] = err
					continue
				}
				item, err := s.pool.PrepareAdmissionSharedWithLookupAndParams(txs[idx], view, chainUtxos, consensus.ParamsForProfile(s.cfg.Profile), rules)
				if err != nil {
					errs[idx] = err
					continue
				}
				item.PreparedTip = tipHash
				item.HasPreparedTip = true
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
	return s.minerManager().currentTemplateGeneration()
}

func (s *Service) invalidateBlockTemplate(reason string) {
	s.minerManager().invalidateBlockTemplate(reason)
}

func (s *Service) mineOneBlock() ([32]byte, error) {
	return s.minerManager().mineOneBlock()
}

func (s *Service) mineBlockTemplate(block types.Block, generation uint64) (types.Block, bool, error) {
	params := consensus.ParamsForProfile(s.cfg.Profile)
	selectionAcc, fresh, err := s.miningSelectionAccumulator(block)
	if err != nil {
		return types.Block{}, false, err
	}
	if !fresh {
		return types.Block{}, false, nil
	}
	return s.mineBlockSearchSpace(block, params, selectionAcc, func(uint64) bool {
		return s.currentTemplateGeneration() == generation
	})
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

func (s *Service) handleCommittedBranchTransition(transition committedBranchTransition, relay bool) error {
	for i := range transition.Connected {
		s.pool.RemoveConfirmed(&transition.Connected[i])
		s.avalancheManager().onBlockAccepted(&transition.Connected[i])
		s.promoteReadyOrphansAfterBlock(&transition.Connected[i])
		s.removeLocalRebroadcastForBlock(&transition.Connected[i])
	}
	if txs := transition.DisconnectedTxs; len(txs) > 0 {
		s.reprocessTransactions(txs, "reorg_reprocess", relay)
	}
	if len(transition.Connected) > 0 || len(transition.DisconnectedTxs) > 0 {
		s.scheduleMempoolPersistence()
	}
	return nil
}

func (s *Service) acceptMinedBlock(block types.Block) ([32]byte, uint64, error) {
	startedAt := time.Now()
	hash := consensus.HeaderHash(&block.Header)

	s.stateMu.Lock()
	beforeTip, ok := s.chainState.tipSnapshot()
	if !ok {
		s.stateMu.Unlock()
		return [32]byte{}, 0, ErrNoTip
	}
	if block.Header.PrevBlockHash != beforeTip.TipHash {
		s.stateMu.Unlock()
		return [32]byte{}, 0, errors.New("stale template")
	}
	summary, transition, err := s.chainState.ApplyBlockWithTransition(&block)
	if err != nil {
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
	afterTip, ok := s.chainState.tipSnapshot()
	if !ok {
		s.stateMu.Unlock()
		return [32]byte{}, 0, ErrNoTip
	}
	height := afterTip.Height
	if err := s.chainState.Store().SetHeaderHashByHeight(height, hash); err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	s.stateMu.Unlock()

	if err := s.handleCommittedBranchTransition(transition, true); err != nil {
		return [32]byte{}, 0, err
	}
	s.invalidateBlockTemplate("tip_advanced")
	s.cacheRecentBlock(block)
	s.cacheRecentHeader(block.Header)
	// Freshly mined blocks should not wait for peers to discover the header via
	// inv and then kick off headers-first recovery. Pushing the header first keeps
	// steady-state one-block catch-up responsive and avoids benchmark seeding
	// stalls on the initial funding block.
	s.broadcastHeaders([]types.BlockHeader{block.Header})
	s.broadcastMinedCompactBlock(block)
	s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	s.noteBlockAccepted()
	s.noteBlockSignatureVerification(summary)
	s.logger.Info("block accepted",
		slog.Uint64("height", height),
		slog.String("hash", hex.EncodeToString(hash[:])),
		slog.Int("txs", len(block.Txs)),
		slog.Int("sig_checks", summary.SignatureChecks),
		slog.Bool("sig_batch_fallback", summary.SignatureBatchFallback),
		slog.Duration("sig_verify_duration", summary.SignatureVerifyTime),
		slog.Int("mempool_size", s.pool.Count()),
		slog.Duration("apply_duration", time.Since(startedAt)),
	)
	s.perf.noteBlockApplyDuration(time.Since(startedAt))
	return hash, height, nil
}

func (s *Service) BuildBlockTemplate() (types.Block, error) {
	return s.minerManager().BuildBlockTemplate()
}

func (s *Service) BuildBenchmarkBlockTemplate(maxTxs int) (types.Block, error) {
	return s.minerManager().BuildBenchmarkBlockTemplate(maxTxs)
}

// Profile exposes the active chain profile to local tooling like the benchmark
// harness, which needs to size regtest-only funding batches correctly.
func (s *Service) Profile() types.ChainProfile {
	return s.cfg.Profile
}

// SubmitBenchmarkBlock accepts a prebuilt block through the local node path.
// Benchmark harnesses use this to separate block assembly/sealing from block
// application and relay timing. When SyntheticMining is enabled on the
// service, PoW checks are skipped for this acceptance path.
func (s *Service) AcceptLocalBenchmarkBlock(block types.Block) ([32]byte, uint64, error) {
	return s.acceptMinedBlock(block)
}

// SubmitBenchmarkBlock accepts a prebuilt block as if it arrived from an
// external source. Benchmark harnesses use this for follower fanout after the
// block has already been accepted on the producer node.
func (s *Service) SubmitBenchmarkBlock(block types.Block) ([32]byte, uint64, error) {
	hash := consensus.HeaderHash(&block.Header)
	applied, _, _, err := s.applyPeerBlock(&block)
	if errors.Is(err, ErrBlockHeaderNotIndexed) {
		if _, headerErr := s.applyPeerHeaders([]types.BlockHeader{block.Header}); headerErr == nil {
			applied, _, _, err = s.applyPeerBlock(&block)
		}
	}
	if err != nil {
		return [32]byte{}, 0, err
	}
	if applied {
		s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	}
	height := uint64(0)
	if tip := s.chainState.ChainState().TipHeight(); tip != nil {
		height = *tip
	}
	return hash, height, nil
}

func (s *Service) buildFundingBlock(keyHashes [][32]byte) (types.Block, []FundingOutput, error) {
	ctx, err := s.minerManager().chainSelectionSnapshot()
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
		outputs[i] = types.TxOutput{ValueAtoms: value, PubKey: keyHash}
	}
	coinbase := coinbaseTxForHeight(ctx.height+1, outputs)
	coinbaseTxID := consensus.TxID(&coinbase)
	funding := make([]FundingOutput, 0, len(outputs))
	fundingLeaves := make([]utreexo.UtxoLeaf, 0, len(outputs))
	for vout, output := range outputs {
		outPoint := types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}
		fundingLeaves = append(fundingLeaves, utxoLeafForOutput(outPoint, output))
		funding = append(funding, FundingOutput{
			OutPoint: outPoint,
			Value:    output.ValueAtoms,
			PubKey:   output.PubKey,
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
	block := types.Block{Header: header, Txs: []types.Transaction{coinbase}}
	minedBlock, ok, err := s.mineBlockSearchSpace(block, params, ctx.utxoAcc, nil)
	if err != nil {
		return types.Block{}, nil, err
	}
	if !ok {
		return types.Block{}, nil, consensus.ErrInvalidPow
	}
	return minedBlock, funding, nil
}

func (s *Service) chainUtxoSnapshot() consensus.UtxoLookup {
	return s.chainState.Store().UTXOLookupFunc()
}

func coinbaseTxForHeight(height uint64, outputs []types.TxOutput) types.Transaction {
	var extraNonce [types.CoinbaseExtraNonceLen]byte
	return types.Transaction{
		Base: types.TxBase{
			Version:            1,
			CoinbaseHeight:     &height,
			CoinbaseExtraNonce: &extraNonce,
			Outputs:            outputs,
		},
	}
}

func (s *Service) miningSelectionAccumulator(block types.Block) (*utreexo.Accumulator, bool, error) {
	s.stateMu.RLock()
	view, ok := s.chainState.sharedCommittedView()
	s.stateMu.RUnlock()
	if !ok {
		return nil, false, ErrNoTip
	}
	if block.Header.PrevBlockHash != view.TipHash {
		return nil, false, nil
	}
	if len(block.Txs) <= 1 {
		return view.UTXOAcc, true, nil
	}
	nextAcc, err := view.UTXOAcc.Apply(blockSpendOutPoints(block.Txs[1:]), blockCreatedLeaves(block.Txs[1:]))
	if err != nil {
		return nil, false, err
	}
	return nextAcc, true, nil
}

func (s *Service) mineBlockSearchSpace(block types.Block, params consensus.ChainParams, selectionAcc *utreexo.Accumulator, shouldContinue func(uint64) bool) (types.Block, bool, error) {
	mineHeader := s.mineHeaderFn
	if mineHeader == nil {
		mineHeader = consensus.MineHeaderInterruptible
	}
	if len(block.Txs) == 0 || !block.Txs[0].IsCoinbase() {
		return types.Block{}, false, errors.New("mining requires coinbase-first block")
	}
	for {
		minedHeader, ok, err := mineHeader(block.Header, params, shouldContinue)
		if err == nil && ok {
			block.Header = minedHeader
			return block, true, nil
		}
		if err != nil && !errors.Is(err, consensus.ErrMiningNonceExhausted) {
			return types.Block{}, false, err
		}
		if !ok && err == nil {
			return types.Block{}, false, nil
		}
		nextExtraNonce, wrapped := incrementCoinbaseExtraNonce(*block.Txs[0].Base.CoinbaseExtraNonce)
		if wrapped {
			return types.Block{}, false, consensus.ErrInvalidPow
		}
		block.Txs[0].Base.CoinbaseExtraNonce = &nextExtraNonce
		block.Header.Nonce = 0
		if err := rebuildCoinbaseSearchCommitments(&block, selectionAcc); err != nil {
			return types.Block{}, false, err
		}
		s.logger.Debug("mining rolled coinbase extra nonce",
			slog.String("extra_nonce", hex.EncodeToString(nextExtraNonce[:])),
			slog.Uint64("timestamp", block.Header.Timestamp),
		)
	}
}

func rebuildCoinbaseSearchCommitments(block *types.Block, selectionAcc *utreexo.Accumulator) error {
	coinbaseTxID := consensus.TxID(&block.Txs[0])
	_, _, txRoot, authRoot := consensus.BuildBlockRoots(block.Txs)
	block.Header.MerkleTxIDRoot = txRoot
	block.Header.MerkleAuthRoot = authRoot
	finalAcc, err := selectionAcc.Apply(nil, coinbaseLeaves(coinbaseTxID, block.Txs[0].Base.Outputs))
	if err != nil {
		return err
	}
	block.Header.UTXORoot = finalAcc.Root()
	return nil
}

func incrementCoinbaseExtraNonce(current [types.CoinbaseExtraNonceLen]byte) ([types.CoinbaseExtraNonceLen]byte, bool) {
	next := current
	for i := 0; i < len(next); i++ {
		next[i]++
		if next[i] != 0 {
			return next, false
		}
	}
	return next, true
}

func coinbaseLeaves(txid [32]byte, outputs []types.TxOutput) []utreexo.UtxoLeaf {
	leaves := make([]utreexo.UtxoLeaf, 0, len(outputs))
	for vout, output := range outputs {
		leaves = append(leaves, utxoLeafForOutput(types.OutPoint{TxID: txid, Vout: uint32(vout)}, output))
	}
	return leaves
}

func blockSpendOutPoints(txs []types.Transaction) []types.OutPoint {
	spent := make([]types.OutPoint, 0)
	for _, tx := range txs {
		for _, input := range tx.Base.Inputs {
			spent = append(spent, input.PrevOut)
		}
	}
	return spent
}

func blockCreatedLeaves(txs []types.Transaction) []utreexo.UtxoLeaf {
	created := make([]utreexo.UtxoLeaf, 0)
	for _, tx := range txs {
		txid := consensus.TxID(&tx)
		for vout, output := range tx.Base.Outputs {
			created = append(created, utxoLeafForOutput(types.OutPoint{TxID: txid, Vout: uint32(vout)}, output))
		}
	}
	return created
}

func (s *Service) chainUtxoSnapshotWithTip() ([32]byte, consensus.UtxoLookup) {
	tip, ok := s.chainState.tipSnapshot()
	if !ok {
		return [32]byte{}, s.chainState.Store().UTXOLookupFunc()
	}
	return tip.TipHash, s.chainState.Store().UTXOLookupFunc()
}

func (s *Service) chainTipHash() [32]byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	// Batch tip-stability checks only need the active tip identity. Avoid
	// CommittedView here because that path defensively clones the full UTXO map.
	tip, ok := s.chainState.tipSnapshot()
	if ok {
		return tip.TipHash
	}
	return [32]byte{}
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

func (p *peerConn) estimateBlockOverlap(block types.Block) blockOverlapEstimate {
	estimate := blockOverlapEstimate{}
	if len(block.Txs) <= 1 {
		return estimate
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	estimate.TotalTxs = len(block.Txs) - 1
	for _, tx := range block.Txs[1:] {
		txid := consensus.TxID(&tx)
		if _, ok := p.knownTx[txid]; ok {
			estimate.KnownTxs++
			continue
		}
		estimate.MissingTxs++
	}
	if estimate.TotalTxs > 0 {
		estimate.HitRate = float64(estimate.KnownTxs) / float64(estimate.TotalTxs)
	}
	return estimate
}

func shouldPreferGrapheneExtended(estimate blockOverlapEstimate) bool {
	if estimate.TotalTxs == 0 {
		return false
	}
	if estimate.MissingTxs >= 128 {
		return true
	}
	return estimate.HitRate < 0.65
}

func selectBlockRelayPlan(peer *peerConn, block types.Block) blockRelayPlan {
	if peer == nil || !peer.supportsCompactBlockRelay() {
		return blockRelayPlanFull
	}
	estimate := peer.estimateBlockOverlap(block)
	if peer.supportsGrapheneExtended() && shouldPreferGrapheneExtended(estimate) {
		return blockRelayPlanGrapheneExtended
	}
	return blockRelayPlanCompactFallback
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
		hash, err := s.chainState.Store().GetCanonicalHeaderHashByHeight(height)
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
	firstHash := consensus.HeaderHash(&headers[0])
	lastHash := consensus.HeaderHash(&headers[len(headers)-1])
	parentEntry, err := s.chainState.Store().GetBlockIndex(&parentHash)
	if err != nil {
		return 0, err
	}
	if parentEntry == nil {
		return 0, fmt.Errorf("missing parent entry for header parent %x", parentHash)
	}
	prevEntry := *parentEntry
	recentTimes, err := loadIndexedAncestorTimestamps(s.chainState.Store(), consensus.HeaderHash(&prevEntry.Header), 11)
	if err != nil {
		return 0, err
	}
	applied := 0
	batchEntries := make([]storage.HeaderBatchEntry, 0, len(headers))
	batchEntryByHash := make(map[[32]byte]storage.BlockIndexEntry, len(headers))
	for _, header := range headers {
		rules := consensus.DefaultConsensusRules()
		rules.SkipPow = s.headerChain.SkipPow()
		if err := consensus.ValidateHeaderWithRules(&header, consensus.PrevBlockContext{
			Height:         prevEntry.Height,
			Header:         prevEntry.Header,
			MedianTimePast: consensus.MedianTimePast(recentTimes),
			CurrentTime:    uint64(time.Now().Unix()),
		}, s.headerChain.params, rules); err != nil {
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
		recentTimes = appendRecentTime(recentTimes, header.Timestamp)
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
	oldTipHash := currentTipHash
	var activeEntries []storage.HeaderBatchEntry
	var activeForkHeight uint64
	promoted := false
	if consensus.CompareChainWork(prevEntry.ChainWork, currentTipEntry.ChainWork) > 0 {
		promotedChain := NewHeaderChain(s.headerChain.Profile())
		promotedChain.SetSkipPow(s.headerChain.SkipPow())
		if err := promotedChain.InitializeTip(prevEntry.Height, prevEntry.Header); err != nil {
			return applied, err
		}
		nextChain = promotedChain
		stored = &storage.StoredHeaderState{
			Profile:   s.headerChain.Profile(),
			Height:    prevEntry.Height,
			TipHeader: prevEntry.Header,
		}
		activeEntries, activeForkHeight, err = s.headerBranchEntriesLocked(prevEntry, batchEntryByHash)
		if err != nil {
			return applied, err
		}
		promoted = true
	}
	if err := s.chainState.Store().CommitHeaderChain(stored, batchEntries, activeForkHeight, oldTipHeight, activeEntries); err != nil {
		return 0, err
	}
	s.headerChain = nextChain
	if promoted {
		s.logger.Info("promoted higher-work header branch",
			slog.Uint64("fork_height", activeForkHeight),
			slog.Uint64("old_header_height", oldTipHeight),
			slog.Uint64("new_header_height", prevEntry.Height),
			slog.String("old_tip", shortHexBytes(oldTipHash, 16)),
			slog.String("new_tip", shortHexBytes(lastHash, 16)),
			slog.Int("branch_headers", len(activeEntries)),
			slog.String("batch_first", shortHexBytes(firstHash, 16)),
			slog.String("batch_last", shortHexBytes(lastHash, 16)),
		)
	} else {
		s.logger.Debug("stored non-winning peer header branch",
			slog.Uint64("parent_height", parentEntry.Height),
			slog.Uint64("branch_tip_height", prevEntry.Height),
			slog.Uint64("active_header_height", oldTipHeight),
			slog.String("parent", shortHexBytes(parentHash, 16)),
			slog.String("batch_first", shortHexBytes(firstHash, 16)),
			slog.String("batch_last", shortHexBytes(lastHash, 16)),
		)
	}
	return applied, nil
}

func (s *Service) headerBranchEntriesLocked(tip storage.BlockIndexEntry, batchEntries map[[32]byte]storage.BlockIndexEntry) ([]storage.HeaderBatchEntry, uint64, error) {
	entries := make([]storage.HeaderBatchEntry, 0, 8)
	cursor := tip
	for {
		activeHash, err := s.chainState.Store().GetCanonicalHeaderHashByHeight(cursor.Height)
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

func (s *Service) applyPeerBlock(block *types.Block) (bool, consensus.BlockValidationSummary, time.Duration, error) {
	startedAt := time.Now()
	hash := consensus.HeaderHash(&block.Header)
	entry, err := s.chainState.Store().GetBlockIndex(&hash)
	if err != nil {
		return false, consensus.BlockValidationSummary{}, 0, err
	}
	if entry == nil {
		return false, consensus.BlockValidationSummary{}, 0, fmt.Errorf("%w: %x", ErrBlockHeaderNotIndexed, hash)
	}
	summary, lockWait, transition, err := s.chainState.ApplyBlockWithTiming(block)
	if err != nil {
		return false, consensus.BlockValidationSummary{}, 0, err
	} else {
		s.perf.noteBlockApplyLockWaitDuration(lockWait)
	}
	if err := s.handleCommittedBranchTransition(transition, true); err != nil {
		return false, consensus.BlockValidationSummary{}, 0, err
	}
	s.cacheRecentBlock(*block)
	s.noteBlockSignatureVerification(summary)
	return len(transition.Connected) > 0 || len(transition.DisconnectedTxs) > 0, summary, time.Since(startedAt), nil
}

func (s *Service) localRebroadcastLoop() {
	ticker := time.NewTicker(localOriginRebroadcastInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.rebroadcastLocalTxs()
		}
	}
}

func (s *Service) rebroadcastLocalTxs() {
	peers := s.peerSnapshot()
	if len(peers) == 0 {
		return
	}
	now := time.Now()
	s.rebroadcastMu.Lock()
	if len(s.localRebroadcast) == 0 {
		s.rebroadcastMu.Unlock()
		return
	}
	txids := make([][32]byte, 0, len(s.localRebroadcast))
	for txid := range s.localRebroadcast {
		if !s.pool.Contains(txid) {
			delete(s.localRebroadcast, txid)
			continue
		}
		s.localRebroadcast[txid] = now
		txids = append(txids, txid)
	}
	s.rebroadcastMu.Unlock()
	if len(txids) == 0 {
		return
	}
	accepted := make([]mempool.AcceptedTx, 0, len(txids))
	for _, txid := range txids {
		accepted = append(accepted, mempool.AcceptedTx{TxID: txid})
	}
	s.broadcastAcceptedTxsToPeersRetry(peers, accepted)
}

func (s *Service) erlayReconcileLoop() {
	ticker := time.NewTicker(erlayReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runErlayReconcileRound()
		}
	}
}

func (s *Service) runErlayReconcileRound() {
	peers := s.peerSnapshot()
	if len(peers) == 0 {
		return
	}
	snapshot := s.pool.SnapshotShared()
	if len(snapshot) == 0 {
		return
	}
	for _, peer := range peers {
		if !peer.supportsErlayTxRelay() {
			continue
		}
		txids := peer.planErlayReconcileRound(snapshot, erlayReconcileBatchLimit)
		if len(txids) == 0 {
			continue
		}
		s.noteErlayRound(len(txids))
		if err := peer.enqueueTxRecon(p2p.TxReconMessage{TxIDs: txids}); err != nil && s.logger != nil {
			s.logger.Debug("erlay reconcile round enqueue failed",
				slog.String("addr", peer.addr),
				slog.Int("count", len(txids)),
				slog.Any("error", err),
			)
		}
	}
}

func (s *Service) promoteReadyOrphansAfterBlock(block *types.Block) {
	promoted, rejected := s.pool.PromoteReadyOrphansDetailedForBlockWithLookupAndParams(block, s.chainUtxoSnapshot(), consensus.ParamsForProfile(s.cfg.Profile), consensus.DefaultConsensusRules())
	if len(rejected) > 0 {
		txs := make([]types.Transaction, 0, len(rejected))
		errs := make([]error, 0, len(rejected))
		for _, reject := range rejected {
			txs = append(txs, reject.Tx)
			errs = append(errs, reject.Err)
		}
		s.noteRejectedTransactions(txs, errs)
	}
	if len(promoted) == 0 {
		return
	}
	s.notePromotedLocalRelayTxs(promoted)
	s.invalidateBlockTemplate("orphan_promotion")
	s.broadcastAcceptedTxsToPeers(s.peerSnapshot(), promoted)
	if s.logger != nil {
		s.logger.Debug("promoted ready orphans after block acceptance",
			slog.Int("promoted", len(promoted)),
			slog.Int("mempool_size", s.pool.Count()),
			slog.Int("orphan_count", s.pool.OrphanCount()),
			slog.String("txids", acceptedTxDebugSummary(promoted, 6)),
		)
	}
}

func (s *Service) notePromotedLocalRelayTxs(accepted []mempool.AcceptedTx) {
	if len(accepted) == 0 {
		return
	}
	now := time.Now()
	s.rebroadcastMu.Lock()
	defer s.rebroadcastMu.Unlock()
	for _, item := range accepted {
		s.localRebroadcast[item.TxID] = now
	}
}

func (s *Service) removeLocalRebroadcastForBlock(block *types.Block) {
	if block == nil || len(block.Txs) <= 1 {
		return
	}
	s.rebroadcastMu.Lock()
	defer s.rebroadcastMu.Unlock()
	for i := 1; i < len(block.Txs); i++ {
		delete(s.localRebroadcast, consensus.TxID(&block.Txs[i]))
	}
}

func (s *Service) acceptPeerBlockMessage(peer *peerConn, block *types.Block) error {
	hash := consensus.HeaderHash(&block.Header)
	if s.hasRejectedBlock(hash) {
		s.releaseBlockRequest(hash)
		s.peerManager().banPeerAddr(peer.addr, peerMisbehaviorBanDuration, "relayed cached invalid block")
		s.logger.Warn("peer relayed cached invalid block",
			slog.String("addr", peer.addr),
			slog.String("hash", shortHexBytes(hash, 16)),
			slog.Duration("ban", peerMisbehaviorBanDuration),
		)
		return errors.New("peer relayed cached invalid block")
	}
	applied, summary, applyDuration, err := s.applyPeerBlock(block)
	if errors.Is(err, ErrBlockHeaderNotIndexed) {
		if _, seen := s.recentHeader(hash); seen {
			if _, headerErr := s.applyPeerHeaders([]types.BlockHeader{block.Header}); headerErr == nil {
				applied, summary, applyDuration, err = s.applyPeerBlock(block)
			}
		}
	}
	if err != nil {
		return s.handlePeerBlockAcceptanceError(peer, block, err)
	}
	s.cacheRecentBlock(*block)
	s.cacheRecentHeader(block.Header)
	s.releaseBlockRequest(hash)
	s.removePendingPeerBlock(hash)
	if applied {
		peer.noteHeight(s.blockHeight())
		peer.noteUsefulBlocks(1, time.Now())
		s.invalidateBlockTemplate("peer_block")
		s.noteBlockAccepted()
		s.perf.noteBlockApplyDuration(applyDuration)
		s.logger.Info("applied peer block",
			slog.String("addr", peer.addr),
			slog.String("hash", fmt.Sprintf("%x", hash)),
			slog.Uint64("block_height", s.blockHeight()),
			slog.Int("sig_checks", summary.SignatureChecks),
			slog.Bool("sig_batch_fallback", summary.SignatureBatchFallback),
			slog.Duration("sig_verify_duration", summary.SignatureVerifyTime),
			slog.Duration("apply_duration", applyDuration),
		)
		s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	} else {
		s.perf.noteBlockApplyDuration(applyDuration)
		s.logger.Debug("validated side-branch peer block",
			slog.String("addr", peer.addr),
			slog.String("hash", fmt.Sprintf("%x", hash)),
			slog.String("parent", shortHexBytes(block.Header.PrevBlockHash, 16)),
			slog.Uint64("active_height", s.blockHeight()),
			slog.Uint64("header_height", s.headerHeight()),
			slog.Int("sig_checks", summary.SignatureChecks),
			slog.Bool("sig_batch_fallback", summary.SignatureBatchFallback),
			slog.Duration("sig_verify_duration", summary.SignatureVerifyTime),
			slog.Duration("apply_duration", applyDuration),
		)
	}
	s.drainPendingPeerBlocks(hash)
	return s.requestBlocks(peer)
}

func (s *Service) handlePeerTxAdmissionErrors(peer *peerConn, errs []error) error {
	for _, err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, consensus.ErrInvalidSignature) {
			peer.invalidTxSignatures++
			if peer.invalidTxSignatures < peerInvalidTxSignatureLimit {
				s.logger.Warn("peer submitted transaction with invalid signature",
					slog.String("addr", peer.addr),
					slog.Int("invalid_sig_count", peer.invalidTxSignatures),
					slog.Int("ban_threshold", peerInvalidTxSignatureLimit),
				)
				continue
			}
			s.peerManager().banPeerAddr(peer.addr, peerMisbehaviorBanDuration, err.Error())
			return err
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
		errors.Is(err, mempool.ErrMempoolFull) ||
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

func mempoolAdmissionsChanged(admissions []mempool.Admission) bool {
	for _, admission := range admissions {
		if admission.Orphaned || len(admission.Accepted) > 0 {
			return true
		}
	}
	return false
}

func countNonNilErrors(errs []error) int {
	total := 0
	for _, err := range errs {
		if err != nil {
			total++
		}
	}
	return total
}

func (s *Service) handlePeerBlockAcceptanceError(peer *peerConn, block *types.Block, err error) error {
	hash := consensus.HeaderHash(&block.Header)
	switch {
	case errors.Is(err, ErrBlockAlreadyKnown):
		s.releaseBlockRequest(hash)
		s.removePendingPeerBlock(hash)
		s.logger.Debug("ignoring duplicate peer block",
			slog.String("addr", peer.addr),
			slog.String("hash", shortHexBytes(hash, 16)),
			slog.Uint64("tip_height", s.blockHeight()),
			slog.Uint64("header_height", s.headerHeight()),
		)
		s.drainPendingPeerBlocks(hash)
		return s.requestBlocks(peer)
	case errors.Is(err, ErrUnknownParent):
		storeResult := s.storePendingPeerBlock(peer.addr, block)
		s.releaseBlockRequest(hash)
		s.logger.Warn("peer block arrived before parent header",
			slog.String("addr", peer.addr),
			slog.String("hash", shortHexBytes(hash, 16)),
			slog.String("parent", shortHexBytes(block.Header.PrevBlockHash, 16)),
			slog.Bool("queued", storeResult.Added),
			slog.Int("pending_blocks", storeResult.PendingCount),
			slog.Uint64("pending_bytes", storeResult.PendingBytes),
			slog.Uint64("tip_height", s.blockHeight()),
			slog.Uint64("header_height", s.headerHeight()),
			slog.String("peer_sync", s.peerSyncDebugSummary(4)),
			slog.String("inflight_blocks", s.inflightBlockDebugSummary(6)),
			slog.String("pending_detail", s.pendingPeerBlockDebugSummary(6)),
		)
		if storeResult.Evicted > 0 {
			s.logger.Warn("evicted queued peer blocks while retaining out-of-order block",
				slog.Int("evicted_blocks", storeResult.Evicted),
				slog.Int("count_limit", maxPendingPeerBlocks),
				slog.Uint64("byte_limit", maxPendingPeerBlockBytes),
				slog.Int("per_peer_limit", maxPendingPeerBlocksPerPeer),
			)
		}
		if storeResult.Dropped {
			s.logger.Warn("dropped out-of-order peer block from pending queue",
				slog.String("addr", peer.addr),
				slog.String("hash", shortHexBytes(hash, 16)),
				slog.String("reason", storeResult.DropReason),
				slog.Uint64("pending_bytes", storeResult.PendingBytes),
			)
		}
		return s.requestHeaders(peer, hash)
	case errors.Is(err, ErrParentStateUnavailable) || errors.Is(err, ErrBlockHeaderNotIndexed):
		storeResult := s.storePendingPeerBlock(peer.addr, block)
		s.releaseBlockRequest(hash)
		s.logger.Warn("peer block requires catch-up before apply",
			slog.String("addr", peer.addr),
			slog.String("hash", shortHexBytes(hash, 16)),
			slog.String("parent", shortHexBytes(block.Header.PrevBlockHash, 16)),
			slog.Bool("queued", storeResult.Added),
			slog.Int("pending_blocks", storeResult.PendingCount),
			slog.Uint64("pending_bytes", storeResult.PendingBytes),
			slog.Any("error", err),
			slog.Uint64("tip_height", s.blockHeight()),
			slog.Uint64("header_height", s.headerHeight()),
			slog.String("peer_sync", s.peerSyncDebugSummary(4)),
			slog.String("inflight_blocks", s.inflightBlockDebugSummary(6)),
			slog.String("pending_detail", s.pendingPeerBlockDebugSummary(6)),
		)
		if storeResult.Evicted > 0 {
			s.logger.Warn("evicted queued peer blocks while retaining competing-branch child",
				slog.Int("evicted_blocks", storeResult.Evicted),
				slog.Int("count_limit", maxPendingPeerBlocks),
				slog.Uint64("byte_limit", maxPendingPeerBlockBytes),
				slog.Int("per_peer_limit", maxPendingPeerBlocksPerPeer),
			)
		}
		if storeResult.Dropped {
			s.logger.Warn("dropped competing-branch child from pending queue",
				slog.String("addr", peer.addr),
				slog.String("hash", shortHexBytes(hash, 16)),
				slog.String("reason", storeResult.DropReason),
				slog.Uint64("pending_bytes", storeResult.PendingBytes),
			)
		}
		if syncErr := s.requestHeaders(peer, hash); syncErr != nil {
			return syncErr
		}
		s.drainPendingPeerBlocksIfParentActive(block.Header.PrevBlockHash)
		return s.requestBlocks(peer)
	default:
		s.releaseBlockRequest(hash)
		s.removePendingPeerBlock(hash)
		misbehavior := isPeerMisbehaviorBlockError(err)
		if misbehavior {
			s.rememberRejectedBlock(hash)
			s.peerManager().banPeerAddr(peer.addr, peerMisbehaviorBanDuration, err.Error())
		}
		s.logger.Warn("peer block apply failed",
			slog.String("addr", peer.addr),
			slog.String("hash", shortHexBytes(hash, 16)),
			slog.String("parent", shortHexBytes(block.Header.PrevBlockHash, 16)),
			slog.Any("error", err),
			slog.Bool("peer_misbehavior", misbehavior),
			slog.Uint64("tip_height", s.blockHeight()),
			slog.Uint64("header_height", s.headerHeight()),
			slog.String("peer_sync", s.peerSyncDebugSummary(4)),
			slog.String("inflight_blocks", s.inflightBlockDebugSummary(6)),
		)
		return err
	}
}

func (s *Service) storePendingPeerBlock(peerAddr string, block *types.Block) pendingPeerBlockStoreResult {
	hash := consensus.HeaderHash(&block.Header)
	parent := block.Header.PrevBlockHash
	now := time.Now()
	blockBytes := pendingPeerBlockEncodedSize(block)

	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	if s.pendingBlocks == nil {
		s.pendingBlocks = make(map[[32]byte]pendingPeerBlock)
	}
	if s.pendingBlocksByPeer == nil {
		s.pendingBlocksByPeer = make(map[string]int)
	}
	if s.pendingChildren == nil {
		s.pendingChildren = make(map[[32]byte]map[[32]byte]struct{})
	}

	result := pendingPeerBlockStoreResult{}
	if blockBytes > maxPendingPeerBlockBytes {
		result.PendingCount = len(s.pendingBlocks)
		result.PendingBytes = s.pendingBlockBytes
		result.Dropped = true
		result.DropReason = "block exceeds pending queue byte budget"
		return result
	}

	if existing, ok := s.pendingBlocks[hash]; ok {
		if existing.peerAddr == "" && peerAddr != "" {
			existing.peerAddr = peerAddr
			s.pendingBlocksByPeer[peerAddr]++
		}
		s.pendingBlockBytes -= existing.sizeBytes
		existing.block = *block
		existing.sizeBytes = blockBytes
		existing.receivedAt = now
		s.pendingBlocks[hash] = existing
		s.pendingBlockBytes += existing.sizeBytes
		for s.pendingBlockBytes > maxPendingPeerBlockBytes {
			if _, evicted := s.evictOldestPendingPeerBlockExceptLocked(hash); !evicted {
				result.Dropped = true
				result.DropReason = "pending queue could not make room for block refresh"
				break
			}
			result.Evicted++
		}
		result.PendingCount = len(s.pendingBlocks)
		result.PendingBytes = s.pendingBlockBytes
		return result
	}

	if peerAddr != "" {
		for s.pendingBlocksByPeer[peerAddr] >= maxPendingPeerBlocksPerPeer {
			if _, evicted := s.evictOldestPendingPeerBlockForPeerLocked(peerAddr); !evicted {
				break
			}
			result.Evicted++
		}
	}
	for len(s.pendingBlocks) >= maxPendingPeerBlocks {
		if _, evicted := s.evictOldestPendingPeerBlockLocked(); !evicted {
			break
		}
		result.Evicted++
	}
	for s.pendingBlockBytes+blockBytes > maxPendingPeerBlockBytes {
		if _, evicted := s.evictOldestPendingPeerBlockLocked(); !evicted {
			break
		}
		result.Evicted++
	}
	if peerAddr != "" && s.pendingBlocksByPeer[peerAddr] >= maxPendingPeerBlocksPerPeer {
		result.PendingCount = len(s.pendingBlocks)
		result.PendingBytes = s.pendingBlockBytes
		result.Dropped = true
		result.DropReason = "peer pending block quota reached"
		return result
	}
	if s.pendingBlockBytes+blockBytes > maxPendingPeerBlockBytes {
		result.PendingCount = len(s.pendingBlocks)
		result.PendingBytes = s.pendingBlockBytes
		result.Dropped = true
		result.DropReason = "pending queue byte budget exhausted"
		return result
	}

	s.pendingBlocks[hash] = pendingPeerBlock{
		block:      *block,
		peerAddr:   peerAddr,
		sizeBytes:  blockBytes,
		receivedAt: now,
	}
	if peerAddr != "" {
		s.pendingBlocksByPeer[peerAddr]++
	}
	s.pendingBlockBytes += blockBytes
	if s.pendingChildren[parent] == nil {
		s.pendingChildren[parent] = make(map[[32]byte]struct{})
	}
	s.pendingChildren[parent][hash] = struct{}{}
	s.pendingBlockFIFO = append(s.pendingBlockFIFO, hash)
	result.Added = true
	result.PendingCount = len(s.pendingBlocks)
	result.PendingBytes = s.pendingBlockBytes
	return result
}

func (s *Service) removePendingPeerBlock(hash [32]byte) {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	s.removePendingPeerBlockLocked(hash)
}

func (s *Service) removePendingPeerBlockLocked(hash [32]byte) {
	if _, ok := s.deletePendingPeerBlockLocked(hash); !ok {
		return
	}
	for i, candidate := range s.pendingBlockFIFO {
		if candidate == hash {
			s.pendingBlockFIFO = append(s.pendingBlockFIFO[:i], s.pendingBlockFIFO[i+1:]...)
			break
		}
	}
}

func (s *Service) evictOldestPendingPeerBlockLocked() ([32]byte, bool) {
	for len(s.pendingBlockFIFO) > 0 {
		hash := s.pendingBlockFIFO[0]
		s.pendingBlockFIFO = s.pendingBlockFIFO[1:]
		if _, ok := s.deletePendingPeerBlockLocked(hash); !ok {
			continue
		}
		return hash, true
	}
	return [32]byte{}, false
}

func (s *Service) evictOldestPendingPeerBlockExceptLocked(skip [32]byte) ([32]byte, bool) {
	for _, hash := range s.pendingBlockFIFO {
		if hash == skip {
			continue
		}
		if _, ok := s.deletePendingPeerBlockLocked(hash); !ok {
			continue
		}
		for i, candidate := range s.pendingBlockFIFO {
			if candidate == hash {
				s.pendingBlockFIFO = append(s.pendingBlockFIFO[:i], s.pendingBlockFIFO[i+1:]...)
				break
			}
		}
		return hash, true
	}
	return [32]byte{}, false
}

func (s *Service) evictOldestPendingPeerBlockForPeerLocked(peerAddr string) ([32]byte, bool) {
	for _, hash := range s.pendingBlockFIFO {
		entry, ok := s.pendingBlocks[hash]
		if !ok || entry.peerAddr != peerAddr {
			continue
		}
		if _, ok := s.deletePendingPeerBlockLocked(hash); !ok {
			continue
		}
		for i, candidate := range s.pendingBlockFIFO {
			if candidate == hash {
				s.pendingBlockFIFO = append(s.pendingBlockFIFO[:i], s.pendingBlockFIFO[i+1:]...)
				break
			}
		}
		return hash, true
	}
	return [32]byte{}, false
}

func (s *Service) deletePendingPeerBlockLocked(hash [32]byte) (pendingPeerBlock, bool) {
	entry, ok := s.pendingBlocks[hash]
	if !ok {
		return pendingPeerBlock{}, false
	}
	delete(s.pendingBlocks, hash)
	if s.pendingBlockBytes >= entry.sizeBytes {
		s.pendingBlockBytes -= entry.sizeBytes
	} else {
		s.pendingBlockBytes = 0
	}
	if entry.peerAddr != "" {
		s.pendingBlocksByPeer[entry.peerAddr]--
		if s.pendingBlocksByPeer[entry.peerAddr] <= 0 {
			delete(s.pendingBlocksByPeer, entry.peerAddr)
		}
	}
	parent := entry.block.Header.PrevBlockHash
	if children, ok := s.pendingChildren[parent]; ok {
		delete(children, hash)
		if len(children) == 0 {
			delete(s.pendingChildren, parent)
		}
	}
	return entry, true
}

func pendingPeerBlockEncodedSize(block *types.Block) uint64 {
	if block == nil {
		return 0
	}
	return uint64(block.EncodedLen())
}

func (s *Service) pendingPeerBlockChildren(parent [32]byte) []pendingPeerBlock {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	children := s.pendingChildren[parent]
	if len(children) == 0 {
		return nil
	}
	out := make([]pendingPeerBlock, 0, len(children))
	for hash := range children {
		entry, ok := s.pendingBlocks[hash]
		if !ok {
			continue
		}
		out = append(out, entry)
	}
	slices.SortFunc(out, func(a, b pendingPeerBlock) int {
		if !a.receivedAt.Equal(b.receivedAt) {
			if a.receivedAt.Before(b.receivedAt) {
				return -1
			}
			return 1
		}
		left := consensus.HeaderHash(&a.block.Header)
		right := consensus.HeaderHash(&b.block.Header)
		return bytes.Compare(left[:], right[:])
	})
	return out
}

func (s *Service) hasPendingPeerBlock(hash [32]byte) bool {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	_, ok := s.pendingBlocks[hash]
	return ok
}

func (s *Service) pendingPeerBlockCount() int {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	return len(s.pendingBlocks)
}

func (s *Service) pendingPeerBlockBytes() uint64 {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	return s.pendingBlockBytes
}

func (s *Service) hasRejectedBlock(hash [32]byte) bool {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	_, ok := s.rejectedBlocks[hash]
	return ok
}

func (s *Service) rememberRejectedBlock(hash [32]byte) {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	if s.rejectedBlocks == nil {
		s.rejectedBlocks = make(map[[32]byte]struct{})
	}
	if _, ok := s.rejectedBlocks[hash]; ok {
		return
	}
	s.rejectedBlocks[hash] = struct{}{}
	s.rejectedBlockOrder = append(s.rejectedBlockOrder, hash)
	if len(s.rejectedBlockOrder) <= maxRejectedBlockHashes {
		return
	}
	evicted := s.rejectedBlockOrder[0]
	s.rejectedBlockOrder = s.rejectedBlockOrder[1:]
	delete(s.rejectedBlocks, evicted)
}

func isPeerMisbehaviorBlockError(err error) bool {
	return errors.Is(err, consensus.ErrEmptyBlock) ||
		errors.Is(err, consensus.ErrFirstTxNotCoinbase) ||
		errors.Is(err, consensus.ErrTxOrderInvalid) ||
		errors.Is(err, consensus.ErrCoinbaseHasAuth) ||
		errors.Is(err, consensus.ErrCoinbaseHeightInvalid) ||
		errors.Is(err, consensus.ErrCoinbaseExtraNonce) ||
		errors.Is(err, consensus.ErrCoinbaseNoOutputs) ||
		errors.Is(err, consensus.ErrEmptyInputs) ||
		errors.Is(err, consensus.ErrEmptyOutputs) ||
		errors.Is(err, consensus.ErrAuthCountMismatch) ||
		errors.Is(err, consensus.ErrDuplicateInput) ||
		errors.Is(err, consensus.ErrMissingUTXO) ||
		errors.Is(err, consensus.ErrInvalidOutputPubKey) ||
		errors.Is(err, consensus.ErrInvalidSignature) ||
		errors.Is(err, consensus.ErrAmountOverflow) ||
		errors.Is(err, consensus.ErrInputsLessThanOutputs) ||
		errors.Is(err, consensus.ErrCoinbaseOverpay) ||
		errors.Is(err, consensus.ErrPrevHashMismatch) ||
		errors.Is(err, consensus.ErrMerkleTxIDMismatch) ||
		errors.Is(err, consensus.ErrMerkleAuthMismatch) ||
		errors.Is(err, consensus.ErrInvalidNBits) ||
		errors.Is(err, consensus.ErrInvalidCompactTarget) ||
		errors.Is(err, consensus.ErrInvalidPow) ||
		errors.Is(err, consensus.ErrBlockTooLarge) ||
		errors.Is(err, consensus.ErrUTXORootMismatch) ||
		errors.Is(err, consensus.ErrTimestampTooEarly)
}

func (s *Service) pendingPeerBlockDebugSummary(limit int) string {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	if len(s.pendingBlocks) == 0 {
		return "none"
	}
	items := make([]string, 0, len(s.pendingBlocks))
	for hash, entry := range s.pendingBlocks {
		items = append(items, fmt.Sprintf("%s<- %s@%s", shortHexBytes(hash, 8), shortHexBytes(entry.block.Header.PrevBlockHash, 8), shortPeerAddr(entry.peerAddr)))
	}
	slices.Sort(items)
	if limit > 0 && len(items) > limit {
		extra := len(items) - limit
		items = append(items[:limit], fmt.Sprintf("... +%d more", extra))
	}
	return strings.Join(items, ", ")
}

func (s *Service) drainPendingPeerBlocksIfParentActive(parentHash [32]byte) {
	if parentHash != s.chainTipHash() {
		return
	}
	s.drainPendingPeerBlocks(parentHash)
}

func (s *Service) drainPendingPeerBlocks(parentHash [32]byte) {
	queue := [][32]byte{parentHash}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		children := s.pendingPeerBlockChildren(current)
		for _, queued := range children {
			hash := consensus.HeaderHash(&queued.block.Header)
			applied, summary, applyDuration, err := s.applyPeerBlock(&queued.block)
			switch {
			case err == nil:
				s.cacheRecentBlock(queued.block)
				s.cacheRecentHeader(queued.block.Header)
				s.releaseBlockRequest(hash)
				s.removePendingPeerBlock(hash)
				if applied {
					s.invalidateBlockTemplate("peer_block")
					s.noteBlockAccepted()
					s.perf.noteBlockApplyDuration(applyDuration)
					s.logger.Info("applied queued peer block",
						slog.String("addr", queued.peerAddr),
						slog.String("hash", fmt.Sprintf("%x", hash)),
						slog.Uint64("block_height", s.blockHeight()),
						slog.Int("sig_checks", summary.SignatureChecks),
						slog.Bool("sig_batch_fallback", summary.SignatureBatchFallback),
						slog.Duration("sig_verify_duration", summary.SignatureVerifyTime),
						slog.Duration("apply_duration", applyDuration),
						slog.Int("pending_remaining", s.pendingPeerBlockCount()),
					)
					s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
				} else {
					s.perf.noteBlockApplyDuration(applyDuration)
					s.logger.Debug("validated queued side-branch peer block",
						slog.String("addr", queued.peerAddr),
						slog.String("hash", fmt.Sprintf("%x", hash)),
						slog.String("parent", shortHexBytes(queued.block.Header.PrevBlockHash, 16)),
						slog.Uint64("active_height", s.blockHeight()),
						slog.Uint64("header_height", s.headerHeight()),
						slog.Int("sig_checks", summary.SignatureChecks),
						slog.Bool("sig_batch_fallback", summary.SignatureBatchFallback),
						slog.Duration("sig_verify_duration", summary.SignatureVerifyTime),
						slog.Duration("apply_duration", applyDuration),
					)
				}
				queue = append(queue, hash)
			case errors.Is(err, ErrBlockAlreadyKnown):
				s.releaseBlockRequest(hash)
				s.removePendingPeerBlock(hash)
				queue = append(queue, hash)
			case errors.Is(err, ErrUnknownParent), errors.Is(err, ErrParentStateUnavailable), strings.Contains(err.Error(), "without indexed header"):
				s.logger.Debug("queued peer block still waiting on parent state",
					slog.String("addr", queued.peerAddr),
					slog.String("hash", shortHexBytes(hash, 16)),
					slog.String("parent", shortHexBytes(queued.block.Header.PrevBlockHash, 16)),
					slog.Any("error", err),
					slog.Int("pending_blocks", s.pendingPeerBlockCount()),
				)
			default:
				s.releaseBlockRequest(hash)
				s.removePendingPeerBlock(hash)
				s.logger.Warn("dropping queued peer block after apply failure",
					slog.String("addr", queued.peerAddr),
					slog.String("hash", shortHexBytes(hash, 16)),
					slog.String("parent", shortHexBytes(queued.block.Header.PrevBlockHash, 16)),
					slog.Any("error", err),
				)
			}
		}
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
	if s.chainState == nil {
		return 0
	}
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

func stressLaneSecretKey() [32]byte {
	return bpcrypto.TaggedHash("bpu-stress-lane-seed", []byte("regtest"))
}

func stressLaneReservePubKey() [32]byte {
	return bpcrypto.XOnlyPubKeyFromSecret(stressLaneSecretKey())
}

func signStressLaneTx(tx types.Transaction, prevValue uint64) (types.Transaction, error) {
	msg, err := consensus.SighashWithParams(&tx, 0, []consensus.UtxoEntry{{
		ValueAtoms: prevValue,
		PubKey:     stressLaneReservePubKey(),
	}}, consensus.RegtestParams())
	if err != nil {
		return types.Transaction{}, err
	}
	_, sig := bpcrypto.SignSchnorr(stressLaneSecretKey(), &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	return tx, nil
}

func buildStressLaneFanoutTx(prevOut types.OutPoint, prevValue uint64, pubKeys [][32]byte) (types.Transaction, []FundingOutput, error) {
	if len(pubKeys) == 0 {
		return types.Transaction{}, nil, errors.New("at least one pubkey is required")
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: make([]types.TxOutput, len(pubKeys)),
		},
	}
	for i, pubKey := range pubKeys {
		tx.Base.Outputs[i] = types.TxOutput{ValueAtoms: 1, PubKey: pubKey}
	}
	tx, err := signStressLaneTx(tx, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	fee := uint64(tx.EncodedLen())
	if prevValue <= fee || prevValue-fee < uint64(len(pubKeys)) {
		return types.Transaction{}, nil, consensus.ErrInputsLessThanOutputs
	}
	perOutput := (prevValue - fee) / uint64(len(pubKeys))
	remainder := (prevValue - fee) % uint64(len(pubKeys))
	for i := range tx.Base.Outputs {
		value := perOutput
		if i == len(tx.Base.Outputs)-1 {
			value += remainder
		}
		tx.Base.Outputs[i].ValueAtoms = value
	}
	tx, err = signStressLaneTx(tx, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	txid := consensus.TxID(&tx)
	outputs := make([]FundingOutput, 0, len(tx.Base.Outputs))
	for vout, output := range tx.Base.Outputs {
		outputs = append(outputs, FundingOutput{
			OutPoint: types.OutPoint{TxID: txid, Vout: uint32(vout)},
			Value:    output.ValueAtoms,
			PubKey:   output.PubKey,
		})
	}
	return tx, outputs, nil
}

func stressLaneBatchConfirmed(lookup consensus.UtxoLookup, outputs []FundingOutput) bool {
	for _, output := range outputs {
		entry, ok := lookup(output.OutPoint)
		if !ok || entry.ValueAtoms != output.Value || entry.PubKey != output.PubKey {
			return false
		}
	}
	return true
}

func (s *Service) stressLaneInfo() StressLaneInfo {
	confirmed := s.chainUtxoSnapshot()
	reserveKey := stressLaneReservePubKey()
	info := StressLaneInfo{ReservePubKey: hex.EncodeToString(reserveKey[:])}
	for _, utxo := range s.UTXOsByPubKeys([][32]byte{reserveKey}) {
		info.ReserveUTXOs++
		info.ReserveAtoms += utxo.Value
	}
	s.stressMu.Lock()
	defer s.stressMu.Unlock()
	lastPending := time.Time{}
	for txid, batch := range s.stressPending {
		if stressLaneBatchConfirmed(confirmed, batch.Outputs) {
			delete(s.stressPending, txid)
			continue
		}
		info.PendingBatches++
		info.PendingOutputs += len(batch.Outputs)
		if batch.CreatedAt.After(lastPending) {
			lastPending = batch.CreatedAt
			info.LastPendingTxID = hex.EncodeToString(txid[:])
		}
	}
	return info
}

func (s *Service) ensureStressLaneReserve() error {
	if s.stressLaneInfo().ReserveUTXOs > 0 {
		return nil
	}
	// Seed a confirmed reserve output once so live stress funding can fan out a
	// normal signed transaction and let any network miner confirm it.
	_, err := s.MineFundingOutputs([][32]byte{stressLaneReservePubKey()})
	return err
}

func (s *Service) createStressLaneBatch(keyHashes [][32]byte, reserveTopUp bool) (stressLaneBatch, StressLaneInfo, error) {
	if reserveTopUp {
		if err := s.ensureStressLaneReserve(); err != nil {
			return stressLaneBatch{}, StressLaneInfo{}, err
		}
	}
	reserveUTXOs := s.UTXOsByPubKeys([][32]byte{stressLaneReservePubKey()})
	if len(reserveUTXOs) == 0 {
		return stressLaneBatch{}, s.stressLaneInfo(), errors.New("no confirmed stress reserve is available")
	}
	best := reserveUTXOs[0]
	for _, utxo := range reserveUTXOs[1:] {
		if utxo.Value > best.Value {
			best = utxo
		}
	}
	tx, outputs, err := buildStressLaneFanoutTx(best.OutPoint, best.Value, keyHashes)
	if err != nil && reserveTopUp && errors.Is(err, consensus.ErrInputsLessThanOutputs) {
		if _, topUpErr := s.MineFundingOutputs([][32]byte{stressLaneReservePubKey()}); topUpErr != nil {
			return stressLaneBatch{}, StressLaneInfo{}, topUpErr
		}
		reserveUTXOs = s.UTXOsByPubKeys([][32]byte{stressLaneReservePubKey()})
		if len(reserveUTXOs) == 0 {
			return stressLaneBatch{}, StressLaneInfo{}, errors.New("stress reserve top-up did not publish a spendable output")
		}
		best = reserveUTXOs[0]
		for _, utxo := range reserveUTXOs[1:] {
			if utxo.Value > best.Value {
				best = utxo
			}
		}
		tx, outputs, err = buildStressLaneFanoutTx(best.OutPoint, best.Value, keyHashes)
	}
	if err != nil {
		return stressLaneBatch{}, s.stressLaneInfo(), err
	}
	if _, err := s.SubmitTx(tx); err != nil {
		return stressLaneBatch{}, s.stressLaneInfo(), err
	}
	batch := stressLaneBatch{
		TxID:      consensus.TxID(&tx),
		Outputs:   outputs,
		CreatedAt: time.Now(),
	}
	s.stressMu.Lock()
	s.stressPending[batch.TxID] = batch
	s.stressMu.Unlock()
	return batch, s.stressLaneInfo(), nil
}

func (s *Service) findActiveTxBlockHash(txid [32]byte) ([32]byte, bool, error) {
	var zero [32]byte
	s.stateMu.RLock()
	tip := s.chainState.ChainState().TipHeight()
	s.stateMu.RUnlock()
	if tip == nil {
		return zero, false, nil
	}
	for height := *tip + 1; height > 0; height-- {
		blockHeight := height - 1
		hash, err := s.chainState.Store().GetBlockHashByHeight(blockHeight)
		if err != nil {
			return zero, false, err
		}
		if hash == nil {
			continue
		}
		block, err := s.chainState.Store().GetBlock(hash)
		if err != nil {
			return zero, false, err
		}
		if block == nil {
			continue
		}
		for _, tx := range block.Txs {
			if consensus.TxID(&tx) == txid {
				return *hash, true, nil
			}
		}
	}
	return zero, false, nil
}

func (s *Service) waitForStressLaneBatch(batch stressLaneBatch, timeout time.Duration) ([]FundingOutput, StressLaneInfo, error) {
	if timeout <= 0 {
		timeout = stressLaneConfirmTimeout
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		confirmed := s.chainUtxoSnapshot()
		if stressLaneBatchConfirmed(confirmed, batch.Outputs) {
			outputs := append([]FundingOutput(nil), batch.Outputs...)
			if hash, ok, err := s.findActiveTxBlockHash(batch.TxID); err != nil {
				return nil, s.stressLaneInfo(), err
			} else if ok {
				for i := range outputs {
					outputs[i].BlockHash = hash
				}
			}
			s.stressMu.Lock()
			delete(s.stressPending, batch.TxID)
			s.stressMu.Unlock()
			info := s.stressLaneInfo()
			info.ReadyOutputs = len(outputs)
			return outputs, info, nil
		}
		select {
		case <-s.stopCh:
			return nil, s.stressLaneInfo(), io.EOF
		case <-time.After(250 * time.Millisecond):
		}
	}
	return nil, s.stressLaneInfo(), fmt.Errorf("timed out waiting for stress funding tx %x to confirm", batch.TxID)
}

func (s *Service) SeedStressLanes(keyHashes [][32]byte, reserveTopUp bool, waitForConfirmation bool) ([]FundingOutput, StressLaneInfo, [32]byte, error) {
	if !s.cfg.Profile.IsRegtestLike() {
		return nil, StressLaneInfo{}, [32]byte{}, errors.New("stress lanes are only available on regtest-style profiles")
	}
	batch, info, err := s.createStressLaneBatch(keyHashes, reserveTopUp)
	if err != nil {
		return nil, info, [32]byte{}, err
	}
	if !waitForConfirmation {
		return append([]FundingOutput(nil), batch.Outputs...), info, batch.TxID, nil
	}
	outputs, info, err := s.waitForStressLaneBatch(batch, stressLaneConfirmTimeout)
	return outputs, info, batch.TxID, err
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
	mempoolInfo := s.MempoolInfo()
	mempoolEntries := s.pool.Snapshot()
	mempoolTop := s.pool.TopByFee(8)
	performance := s.PerformanceMetrics()
	pow := s.dashboardPowSummary(blockWindow.recent)
	fees := s.dashboardFeeSummary(blockWindow.chart, mempoolInfo.Count, pow, s.cachedCandidateFeeLine(), mempoolEntries)
	mining := s.dashboardMiningSummary(blocks, pow)
	tpsChart := s.dashboardTPSChartFromBlocks(blockWindow.recent, pow, s.cachedCandidateBlockTxCount())
	mempool := s.dashboardMempoolSummary(mempoolInfo, mempoolEntries, mempoolTop, fees.Clear.Time)
	peerHosts := s.dashboardPeerHosts(now, info)
	return &publicDashboardView{
		generatedAt: now,
		nodeID:      s.nodeID,
		createdAt:   s.startedAt,
		info:        info,
		health:      health,
		system:      system,
		performance: performance,
		blocks:      blocks,
		peers:       peers,
		relay:       relay,
		peerHosts:   peerHosts,
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
		Size:        block.EncodedLen(),
		TxCount:     len(block.Txs),
		MinedByNode: blockMinedByPubKey(block, s.cfg.MinerPubKey),
	}
	fees := make([]uint64, 0, len(block.Txs))
	feeRates := make([]uint64, 0, len(block.Txs))
	undoIndex := 0
	for i, tx := range block.Txs {
		txPage := dashboardTxPage{
			BlockHeight: height,
			BlockHash:   *hash,
			Timestamp:   page.Timestamp,
			TxID:        consensus.TxID(&tx),
			Coinbase:    i == 0,
			Size:        tx.EncodedLen(),
			AuthCount:   len(tx.Auth.Entries),
		}
		for _, output := range tx.Base.Outputs {
			txPage.OutputSum += output.ValueAtoms
		}
		txPage.Outputs = make([]dashboardTxOutput, 0, len(tx.Base.Outputs))
		for idx, output := range tx.Base.Outputs {
			txPage.Outputs = append(txPage.Outputs, dashboardTxOutput{
				Index:  idx,
				Amount: output.ValueAtoms,
				PubKey: output.PubKey,
			})
		}
		if !txPage.Coinbase {
			page.TotalUserTxs++
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
				feeRates = append(feeRates, (txPage.Fee*1000)/uint64(txPage.Size))
			} else {
				feeRates = append(feeRates, 0)
			}
			page.TotalFees += txPage.Fee
			fees = append(fees, txPage.Fee)
			if txPage.Fee > 0 {
				page.PaidTxs++
			}
		}
		if len(page.PreviewTxs) < previewLimit {
			page.PreviewTxs = append(page.PreviewTxs, txPage)
		}
	}
	if len(block.Txs) > len(page.PreviewTxs) {
		page.HiddenTxCount = len(block.Txs) - len(page.PreviewTxs)
	}
	page.MedianFee, page.LowFee, page.HighFee = summarizeFeeSet(fees)
	page.MedianFeeRate, page.LowFeeRate, page.HighFeeRate = summarizeFeeSet(feeRates)
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
		firstGap := blocks[0].Timestamp.Sub(blocks[1].Timestamp)
		if firstGap > 0 {
			summary.RecentBlockGap = firstGap
		}
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
	if summary.RecentBlockGap <= 0 {
		summary.RecentBlockGap = summary.AvgBlockInterval
	}
	hashrateSpacing := summary.TargetSpacing.Seconds()
	if summary.HasObservedGap && summary.RecentBlockGap > 0 {
		hashrateSpacing = summary.RecentBlockGap.Seconds()
	}
	if hashrateSpacing > 0 {
		summary.NetworkHashrate = summary.Difficulty * float64(uint64(1)<<32) / hashrateSpacing
	}
	return summary
}

func (s *Service) dashboardFeeSummary(blocks []dashboardBlockPage, mempoolCount int, pow dashboardPowSummary, candidate dashboardCandidateFeeLine, mempoolEntries []mempool.SnapshotEntry) dashboardFeeSummary {
	out := dashboardFeeSummary{
		Recent:    make([]dashboardBlockFeeLine, 0, minInt(len(blocks), 24)),
		Candidate: candidate,
	}
	var avgTxs float64
	var txBlocks int
	median6 := make([]uint64, 0, minInt(len(blocks), 6))
	median24 := make([]uint64, 0, minInt(len(blocks), 24))
	for i, block := range blocks {
		if i < 6 {
			median6 = append(median6, block.MedianFeeRate)
		}
		if i < 24 {
			out.Recent = append(out.Recent, dashboardBlockFeeLine{
				Height:    block.Height,
				MedianFee: block.MedianFeeRate,
				LowFee:    block.LowFeeRate,
				HighFee:   block.HighFeeRate,
				PaidTxs:   block.PaidTxs,
				TotalTxs:  block.TotalUserTxs,
			})
			median24 = append(median24, block.MedianFeeRate)
			if block.PaidTxs > 0 {
				out.PaidBlocks++
			}
			out.FeePayingTxs += block.PaidTxs
			out.TotalTxs += block.TotalUserTxs
			if len(out.Recent) == 1 || block.LowFeeRate < out.RecentMin {
				out.RecentMin = block.LowFeeRate
			}
			if block.HighFeeRate > out.RecentMax {
				out.RecentMax = block.HighFeeRate
			}
		}
		if block.TxCount > 1 {
			avgTxs += float64(block.TxCount - 1)
			txBlocks++
		}
	}
	slices.Reverse(out.Recent)
	out.TotalBlocks = len(out.Recent)
	if candidate.Available {
		out.CurrentMedian = candidate.MedianFee
	} else if len(out.Recent) != 0 {
		out.CurrentMedian = out.Recent[len(out.Recent)-1].MedianFee
	}
	if len(median6) != 0 {
		median, _, _ := summarizeFeeSet(median6)
		out.Median6 = median
	}
	if len(median24) != 0 {
		median, _, _ := summarizeFeeSet(median24)
		out.Median24 = median
	}
	out.Bands = summarizeMempoolFeeBands(mempoolEntries)
	if txBlocks == 0 {
		out.Clear.Blocks = 1
		out.Clear.Time = pow.TargetSpacing
		return out
	}
	avgPerBlock := avgTxs / float64(txBlocks)
	neededBlocks := 1
	if mempoolCount > 0 {
		neededBlocks = int(math.Ceil(float64(mempoolCount) / avgPerBlock))
		if neededBlocks < 1 {
			neededBlocks = 1
		}
	}
	out.Clear.Blocks = neededBlocks
	out.Clear.Time = time.Duration(neededBlocks) * pow.AvgBlockInterval
	return out
}

func (s *Service) dashboardMiningSummary(blocks []dashboardBlockPage, pow dashboardPowSummary) dashboardMiningSummary {
	out := dashboardMiningSummary{
		Enabled: s.cfg.MinerEnabled,
		Workers: s.cfg.MinerWorkers,
	}
	if !out.Enabled {
		return out
	}
	for _, block := range blocks {
		out.RecentWindow++
		if !block.MinedByNode {
			continue
		}
		out.RecentWins++
		out.RecentHeights = append(out.RecentHeights, block.Height)
		out.RecentHashes = append(out.RecentHashes, block.Hash)
	}
	if out.RecentWindow > 0 && pow.NetworkHashrate > 0 {
		out.EstimatedHashrate = pow.NetworkHashrate * (float64(out.RecentWins) / float64(out.RecentWindow))
	}
	return out
}

func (s *Service) dashboardMempoolSummary(info MempoolInfo, entries []mempool.SnapshotEntry, top []mempool.SnapshotEntry, clearEstimate time.Duration) dashboardMempoolSummary {
	out := dashboardMempoolSummary{
		Count:         info.Count,
		Bytes:         info.Bytes,
		Orphans:       info.Orphans,
		EstimatedNext: clearEstimate,
	}
	feeRates := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.Size <= 0 {
			feeRates = append(feeRates, 0)
			continue
		}
		feeRates = append(feeRates, (entry.Fee*1000)/uint64(entry.Size))
	}
	out.MedianFee, out.LowFee, out.HighFee = summarizeFeeSet(feeRates)
	if len(top) == 0 {
		return out
	}
	out.Top = append([]mempool.SnapshotEntry(nil), top...)
	return out
}

func (s *Service) dashboardTPSChartFromBlocks(blocks []dashboardBlockPage, pow dashboardPowSummary, candidateTxCount int) dashboardTPSChart {
	chart := dashboardTPSChart{
		Label: "THROUGHPUT (BY BLOCK)",
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
	if len(ordered) > 5 {
		ordered = ordered[len(ordered)-5:]
	}
	chart.Blocks = make([]dashboardThroughputBlock, 0, len(ordered)+1)
	maxTx := 0
	for _, block := range ordered {
		txCount := maxInt(block.TxCount-1, 0)
		chart.Blocks = append(chart.Blocks, dashboardThroughputBlock{TxCount: txCount})
		chart.TotalTx += txCount
		if txCount > maxTx {
			maxTx = txCount
		}
	}
	if candidateTxCount >= 0 {
		chart.Blocks = append(chart.Blocks, dashboardThroughputBlock{TxCount: candidateTxCount, Candidate: true})
		chart.TotalTx += maxInt(candidateTxCount, 0)
		if candidateTxCount > maxTx {
			maxTx = candidateTxCount
		}
	}
	if len(chart.Blocks) == 0 {
		return chart
	}
	const maxBarWidth = 24
	for i := range chart.Blocks {
		if chart.Blocks[i].TxCount <= 0 || maxTx <= 0 {
			continue
		}
		width := int(math.Round((float64(chart.Blocks[i].TxCount) / float64(maxTx)) * maxBarWidth))
		if width <= 0 {
			width = 1
		}
		chart.Blocks[i].BarWidth = width
	}
	chart.AvgTxPerBlk = float64(chart.TotalTx) / float64(len(chart.Blocks))
	if pow.TargetSpacing > 0 {
		chart.AvgTPS = float64(chart.TotalTx) / (pow.TargetSpacing.Seconds() * float64(len(chart.Blocks)))
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
		if shouldDebugRPCFailure(req.Method, err) {
			s.logger.Debug("rpc request failed", slog.String("method", req.Method), slog.Any("error", err))
		} else {
			s.logger.Warn("rpc request failed", slog.String("method", req.Method), slog.Any("error", err))
		}
		resp.Error = err.Error()
		resp.Result = nil
	} else {
		s.logger.Debug("rpc request completed", slog.String("method", req.Method))
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func shouldDebugRPCFailure(method string, err error) bool {
	if err == nil {
		return false
	}
	return method == "submitblock" && errors.Is(err, ErrBlockAlreadyKnown)
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
	case "getavalancheinfo":
		return s.avalancheManager().info(), nil
	case "getmetrics":
		return s.PerformanceMetrics(), nil
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
	case "getblockfilter":
		var params struct {
			Hash   string  `json:"hash"`
			Height *uint64 `json:"height"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		switch {
		case params.Hash != "":
			hash, err := decodeCompactFilterHash(params.Hash)
			if err != nil {
				return nil, err
			}
			return s.CompactFilterByHash(hash)
		case params.Height != nil:
			return s.CompactFilterByHeight(*params.Height)
		default:
			return nil, fmt.Errorf("hash or height is required")
		}
	case "getfilterheaders":
		var params struct {
			StartHeight uint64 `json:"start_height"`
			Count       uint64 `json:"count"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		return s.CompactFilterHeaders(params.StartHeight, params.Count)
	case "getfiltercheckpoint":
		var params struct {
			Interval uint64 `json:"interval"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		return s.CompactFilterCheckpoint(params.Interval)
	case "getmempool":
		entries := s.pool.Snapshot()
		out := make([]string, 0, len(entries))
		for _, entry := range entries {
			txid := consensus.TxID(&entry.Tx)
			out = append(out, hex.EncodeToString(txid[:]))
		}
		return out, nil
	case "getutxosbypubkeys":
		var params struct {
			PubKeys []string `json:"pubkeys"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		pubKeys := make([][32]byte, 0, len(params.PubKeys))
		for _, raw := range params.PubKeys {
			pubKey, err := ParseMinerPubKey(raw)
			if err != nil {
				return nil, err
			}
			pubKeys = append(pubKeys, pubKey)
		}
		utxos := s.UTXOsByPubKeys(pubKeys)
		out := make([]map[string]any, 0, len(utxos))
		for _, utxo := range utxos {
			out = append(out, map[string]any{
				"txid":   hex.EncodeToString(utxo.OutPoint.TxID[:]),
				"vout":   utxo.OutPoint.Vout,
				"value":  utxo.Value,
				"pubkey": hex.EncodeToString(utxo.PubKey[:]),
			})
		}
		return map[string]any{"utxos": out, "count": len(out)}, nil
	case "getutxosbywatchitems":
		var params struct {
			WatchItems []struct {
				Type      uint64 `json:"type"`
				Payload32 string `json:"payload32"`
			} `json:"watchitems"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		items := make([]compactfilter.WatchItem, 0, len(params.WatchItems))
		for i, raw := range params.WatchItems {
			payload32, err := decodeProofHex32(raw.Payload32, fmt.Sprintf("watchitems[%d].payload32", i))
			if err != nil {
				return nil, err
			}
			items = append(items, compactfilter.WatchItem{Type: raw.Type, Payload32: payload32})
		}
		utxos := s.UTXOsByWatchItems(items)
		out := make([]map[string]any, 0, len(utxos))
		for _, utxo := range utxos {
			out = append(out, map[string]any{
				"txid":      hex.EncodeToString(utxo.OutPoint.TxID[:]),
				"vout":      utxo.OutPoint.Vout,
				"value":     utxo.Value,
				"type":      utxo.Type,
				"payload32": hex.EncodeToString(utxo.Payload32[:]),
			})
		}
		return map[string]any{"utxos": out, "count": len(out)}, nil
	case "getutxoproof":
		var params struct {
			TxID string `json:"txid"`
			Vout uint32 `json:"vout"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		txid, err := decodeProofHex32(params.TxID, "txid")
		if err != nil {
			return nil, err
		}
		proof, err := s.UTXOProof(types.OutPoint{TxID: txid, Vout: params.Vout})
		if err != nil {
			return nil, err
		}
		return EncodeRPCUTXOProof(proof), nil
	case "getutxoproofbatch":
		var params struct {
			OutPoints []struct {
				TxID string `json:"txid"`
				Vout uint32 `json:"vout"`
			} `json:"outpoints"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		outPoints := make([]types.OutPoint, 0, len(params.OutPoints))
		for i, raw := range params.OutPoints {
			txid, err := decodeProofHex32(raw.TxID, fmt.Sprintf("outpoints[%d].txid", i))
			if err != nil {
				return nil, err
			}
			outPoints = append(outPoints, types.OutPoint{TxID: txid, Vout: raw.Vout})
		}
		batch, err := s.UTXOProofBatch(outPoints)
		if err != nil {
			return nil, err
		}
		return EncodeRPCUTXOProofBatch(batch), nil
	case "verifyutxoproof":
		var params struct {
			Proof RPCAnchoredUTXOProof `json:"proof"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		proof, err := DecodeRPCUTXOProof(params.Proof)
		if err != nil {
			return nil, err
		}
		result, err := s.VerifyAnchoredUTXOProof(proof)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"valid":                result.Valid,
			"anchor_matches_local": result.AnchorMatchesLocal,
		}, nil
	case "verifyutxoproofbatch":
		var params struct {
			Proof RPCAnchoredUTXOProofBatch `json:"proof"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		batch, err := DecodeRPCUTXOProofBatch(params.Proof)
		if err != nil {
			return nil, err
		}
		result, err := s.VerifyAnchoredUTXOProofBatch(batch)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"all_valid":            result.AllValid,
			"valid_count":          result.ValidCount,
			"anchor_matches_local": result.AnchorMatchesLocal,
		}, nil
	case "getcompactstatepackage":
		var params struct {
			OutPoints []struct {
				TxID string `json:"txid"`
				Vout uint32 `json:"vout"`
			} `json:"outpoints"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		outPoints := make([]types.OutPoint, 0, len(params.OutPoints))
		for i, raw := range params.OutPoints {
			txid, err := decodeProofHex32(raw.TxID, fmt.Sprintf("outpoints[%d].txid", i))
			if err != nil {
				return nil, err
			}
			outPoints = append(outPoints, types.OutPoint{TxID: txid, Vout: raw.Vout})
		}
		pkg, err := s.CompactStatePackageForOutPoints(outPoints)
		if err != nil {
			return nil, err
		}
		return EncodeRPCCompactStatePackage(pkg), nil
	case "verifycompactstatepackage":
		var params struct {
			Package RPCCompactStatePackage `json:"package"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		pkg, err := DecodeRPCCompactStatePackage(params.Package)
		if err != nil {
			return nil, err
		}
		result, err := s.VerifyCompactStatePackage(pkg)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"all_valid":            result.AllValid,
			"valid_count":          result.ValidCount,
			"anchor_matches_local": result.AnchorMatchesLocal,
			"locality_ordered":     pkg.LocalityOrdered,
		}, nil
	case "getstresslaneinfo":
		if !s.cfg.Profile.IsRegtestLike() {
			return nil, errors.New("stress lane info is only available on regtest-style profiles")
		}
		return s.stressLaneInfo(), nil
	case "getwalletactivitybypubkeys":
		var params struct {
			PubKeys []string `json:"pubkeys"`
			Limit   int      `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		pubKeys := make([][32]byte, 0, len(params.PubKeys))
		for _, raw := range params.PubKeys {
			pubKey, err := ParseMinerPubKey(raw)
			if err != nil {
				return nil, err
			}
			pubKeys = append(pubKeys, pubKey)
		}
		activity, err := s.WalletActivityByPubKeys(pubKeys, params.Limit)
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
	case "getwalletactivitybywatchitems":
		var params struct {
			WatchItems []struct {
				Type      uint64 `json:"type"`
				Payload32 string `json:"payload32"`
			} `json:"watchitems"`
			Limit int `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		items := make([]compactfilter.WatchItem, 0, len(params.WatchItems))
		for i, raw := range params.WatchItems {
			payload32, err := decodeProofHex32(raw.Payload32, fmt.Sprintf("watchitems[%d].payload32", i))
			if err != nil {
				return nil, err
			}
			items = append(items, compactfilter.WatchItem{Type: raw.Type, Payload32: payload32})
		}
		activity, err := s.WalletActivityByWatchItems(items, params.Limit)
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
			PubKeys             []string `json:"pubkeys"`
			ReserveTopUp        *bool    `json:"reserve_top_up,omitempty"`
			WaitForConfirmation *bool    `json:"wait_for_confirmation,omitempty"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if len(params.PubKeys) == 0 {
			return nil, errors.New("pubkeys is required")
		}
		pubKeys := make([][32]byte, 0, len(params.PubKeys))
		for _, raw := range params.PubKeys {
			pubKey, err := ParseMinerPubKey(raw)
			if err != nil {
				return nil, err
			}
			pubKeys = append(pubKeys, pubKey)
		}
		reserveTopUp := true
		if params.ReserveTopUp != nil {
			reserveTopUp = *params.ReserveTopUp
		}
		waitForConfirmation := true
		if params.WaitForConfirmation != nil {
			waitForConfirmation = *params.WaitForConfirmation
		}
		outputs, info, pendingTxID, err := s.SeedStressLanes(pubKeys, reserveTopUp, waitForConfirmation)
		if err != nil {
			return nil, err
		}
		result := make([]map[string]any, 0, len(outputs))
		for _, output := range outputs {
			result = append(result, map[string]any{
				"txid":       hex.EncodeToString(output.OutPoint.TxID[:]),
				"vout":       output.OutPoint.Vout,
				"value":      output.Value,
				"pubkey":     hex.EncodeToString(output.PubKey[:]),
				"block_hash": hex.EncodeToString(output.BlockHash[:]),
			})
		}
		return map[string]any{
			"outputs":       result,
			"count":         len(result),
			"confirmed":     waitForConfirmation,
			"pending_txid":  hex.EncodeToString(pendingTxID[:]),
			"reserve_topup": reserveTopUp,
			"status":        info,
		}, nil
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
		applied, _, _, err := s.applyPeerBlock(&block)
		if errors.Is(err, ErrBlockHeaderNotIndexed) {
			if _, headerErr := s.applyPeerHeaders([]types.BlockHeader{block.Header}); headerErr == nil {
				applied, _, _, err = s.applyPeerBlock(&block)
			}
		}
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
		s.safeGoDetached("rpc-addpeer", func() {
			if err := s.ConnectPeer(params.Addr); err != nil {
				s.logger.Warn("rpc addpeer failed", slog.String("addr", params.Addr), slog.Any("error", err))
			}
		})
		return map[string]any{"addr": params.Addr}, nil
	case "stop":
		s.safeGoDetached("rpc-stop", func() {
			_ = s.Close()
		})
		return map[string]any{"stopping": true}, nil
	default:
		return nil, fmt.Errorf("unknown rpc method: %s", req.Method)
	}
}

type PeerInfo struct {
	Addr                  string `json:"addr"`
	Outbound              bool   `json:"outbound"`
	Manual                bool   `json:"manual,omitempty"`
	Height                uint64 `json:"height"`
	UserAgent             string `json:"user_agent"`
	LastProgress          int64  `json:"last_progress_unix"`
	LastUseful            int64  `json:"last_useful_unix,omitempty"`
	Protected             bool   `json:"eviction_protected,omitempty"`
	ProtectedClass        string `json:"protected_class,omitempty"`
	UsefulnessClass       string `json:"usefulness_class,omitempty"`
	UsefulnessScore       int    `json:"usefulness_score,omitempty"`
	PreferredDownload     bool   `json:"preferred_download,omitempty"`
	DownloadScore         int    `json:"download_score,omitempty"`
	HeaderStalls          int    `json:"header_stalls,omitempty"`
	BlockStalls           int    `json:"block_stalls,omitempty"`
	TxStalls              int    `json:"tx_stalls,omitempty"`
	DownloadCooldownMS    int64  `json:"download_cooldown_ms,omitempty"`
	RelayQueueDepth       int    `json:"relay_queue_depth,omitempty"`
	ControlQueueDepth     int    `json:"control_queue_depth,omitempty"`
	PriorityQueueDepth    int    `json:"priority_queue_depth,omitempty"`
	SendQueueDepth        int    `json:"send_queue_depth,omitempty"`
	PendingLocalRelayTxs  int    `json:"pending_local_relay_txs,omitempty"`
	LastRelayActivityUnix int64  `json:"last_relay_activity_unix,omitempty"`
	TxReqRecvItems        int    `json:"tx_request_received_items,omitempty"`
	TxNotFoundReceived    int    `json:"tx_not_found_received,omitempty"`
	KnownTxClears         int    `json:"known_tx_clears,omitempty"`
	WriterStarvation      int    `json:"writer_starvation_events,omitempty"`
	AvalancheSupported    bool   `json:"avalanche_supported,omitempty"`
	AvalancheWeight       uint64 `json:"avalanche_weight,omitempty"`
	AvalanchePollsSent    int    `json:"avalanche_polls_sent,omitempty"`
	AvalanchePollsRecv    int    `json:"avalanche_polls_received,omitempty"`
	AvalancheVotesSent    int    `json:"avalanche_votes_sent,omitempty"`
	AvalancheVotesRecv    int    `json:"avalanche_votes_received,omitempty"`
}

func (s *Service) PeerInfo() []PeerInfo             { return s.peerManager().PeerInfo() }
func (s *Service) RelayPeerStats() []PeerRelayStats { return s.relayManager().RelayPeerStats() }

func (s *Service) BlockTemplateStats() BlockTemplateStats {
	return s.minerManager().BlockTemplateStats()
}

func (s *Service) cachedCandidateBlockTxCount() int {
	return s.minerManager().cachedTemplateTxCount()
}

func (s *Service) cachedCandidateFeeLine() dashboardCandidateFeeLine {
	line := s.minerManager().cachedTemplateFeeLine()
	if !line.Available {
		return line
	}
	line.Height = s.Info().TipHeight + 1
	return line
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
			if s.hasRejectedBlock(item.Hash) {
				if s.logger != nil {
					s.logger.Debug("ignoring inv for recently rejected block",
						slog.String("addr", peer.addr),
						slog.String("hash", shortHexBytes(item.Hash, 16)),
					)
				}
				continue
			}
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

func (p *peerConn) allowInboundTxs(count int) (bool, error) {
	if count <= 0 {
		return true, nil
	}
	now := time.Now()
	if p.inboundTxLastRefill.IsZero() {
		p.inboundTxLastRefill = now
		p.inboundTxTokens = inboundPeerTxBurst
	}
	if elapsed := now.Sub(p.inboundTxLastRefill).Seconds(); elapsed > 0 {
		p.inboundTxTokens = min(inboundPeerTxBurst, p.inboundTxTokens+elapsed*inboundPeerTxRatePerSecond)
		p.inboundTxLastRefill = now
	}
	needed := float64(count)
	if p.inboundTxTokens >= needed {
		p.inboundTxTokens -= needed
		p.inboundTxViolations = 0
		return true, nil
	}
	p.inboundTxViolations++
	p.inboundTxTokens = 0
	if p.inboundTxViolations >= inboundPeerTxViolationLimit {
		return false, errPeerInboundTxRateLimit
	}
	return false, nil
}

func (p *peerConn) snapshotHeight() uint64 {
	if height := p.bestHeight.Load(); height > 0 {
		return height
	}
	return p.version.Height
}

func (p *peerConn) advertisesService(bit uint64) bool {
	// Tests and older in-process peers may leave Services unset; keep feature
	// paths enabled by default unless a peer explicitly advertises a bitmap.
	if p.version.Services == 0 {
		return true
	}
	return p.version.Services&bit != 0
}

func (p *peerConn) supportsErlayTxRelay() bool {
	return p.advertisesService(p2p.ServiceErlayTxRelay)
}

func (p *peerConn) supportsAvalancheOverlay() bool {
	if p.version.Services == 0 {
		return false
	}
	return p.version.Services&p2p.ServiceAvalancheOverlay != 0
}

func (p *peerConn) supportsCompactBlockRelay() bool {
	return p.advertisesService(p2p.ServiceCompactBlockRelay)
}

func (p *peerConn) supportsGrapheneExtended() bool {
	return p.advertisesService(p2p.ServiceGrapheneExtended)
}

func (p *peerConn) markGrapheneRecoveryPending(hash [32]byte) {
	p.blockRelayMu.Lock()
	defer p.blockRelayMu.Unlock()
	if p.blockRelay.pendingExtendedRecovery == nil {
		p.blockRelay.pendingExtendedRecovery = make(map[[32]byte]struct{})
	}
	p.blockRelay.pendingExtendedRecovery[hash] = struct{}{}
}

func (p *peerConn) clearGrapheneRecoveryPending(hash [32]byte) bool {
	p.blockRelayMu.Lock()
	defer p.blockRelayMu.Unlock()
	if p.blockRelay.pendingExtendedRecovery == nil {
		return false
	}
	_, ok := p.blockRelay.pendingExtendedRecovery[hash]
	delete(p.blockRelay.pendingExtendedRecovery, hash)
	return ok
}

func (p *peerConn) planErlayReconcileRound(entries []mempool.SnapshotEntry, limit int) [][32]byte {
	if len(entries) == 0 || limit <= 0 {
		return nil
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	p.erlayMu.Lock()
	defer p.erlayMu.Unlock()

	if p.knownTx == nil {
		p.knownTx = make(map[[32]byte]struct{})
	}
	if p.erlayState.cursor >= len(entries) {
		p.erlayState.cursor = 0
	}

	txids := make([][32]byte, 0, minInt(limit, len(entries)))
	start := p.erlayState.cursor
	index := start
	scanned := 0
	for scanned < len(entries) && len(txids) < limit {
		txid := entries[index].TxID
		if _, ok := p.knownTx[txid]; !ok {
			txids = append(txids, txid)
		}
		index++
		if index >= len(entries) {
			index = 0
		}
		scanned++
	}

	p.erlayState.cursor = index
	if len(txids) > 0 {
		p.erlayState.roundsStarted++
		p.erlayState.lastRoundAt = time.Now()
		p.erlayState.lastSetSize = len(txids)
	}
	return txids
}

func (p *peerConn) noteErlayRoundResult(setSize int, missing int) {
	p.erlayMu.Lock()
	defer p.erlayMu.Unlock()
	if setSize <= 0 {
		return
	}
	if missing < setSize {
		p.erlayState.roundsHit++
	}
	p.erlayState.lastSetSize = setSize
	p.erlayState.lastMissing = missing
	p.erlayState.lastRoundAt = time.Now()
}

func (p *peerConn) snapshotProgressUnix() int64 {
	if unix := p.lastProgress.Load(); unix > 0 {
		return unix
	}
	return 0
}

func (p *peerConn) bytesIn() uint64 {
	if p.traffic == nil {
		return 0
	}
	return p.traffic.rxBytes.Load()
}

func (p *peerConn) bytesOut() uint64 {
	if p.traffic == nil {
		return 0
	}
	return p.traffic.txBytes.Load()
}

func unixTimeOrZero(unix int64) time.Time {
	if unix <= 0 {
		return time.Time{}
	}
	return time.Unix(unix, 0)
}

func (p *peerConn) noteAvalanchePollSent() {
	p.avaMu.Lock()
	p.avaState.pollsSent++
	p.avaMu.Unlock()
}

func (p *peerConn) noteAvalanchePollReceived() {
	p.avaMu.Lock()
	p.avaState.pollsReceived++
	p.avaMu.Unlock()
}

func (p *peerConn) noteAvalancheVoteSent() {
	p.avaMu.Lock()
	p.avaState.votesSent++
	p.avaMu.Unlock()
}

func (p *peerConn) noteAvalancheVoteReceived() {
	p.avaMu.Lock()
	p.avaState.votesReceived++
	p.avaMu.Unlock()
}

func (p *peerConn) avalancheSnapshot() peerAvalancheState {
	p.avaMu.Lock()
	defer p.avaMu.Unlock()
	return p.avaState
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
	servedBlocks := 0
	for _, item := range msg.Items {
		switch item.Type {
		case p2p.InvTypeTx:
			requestedTxIDs = append(requestedTxIDs, item.Hash)
		case p2p.InvTypeBlock:
			if servedBlocks >= maxServedBlocksPerGetData {
				notFound = append(notFound, p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: item.Hash})
				continue
			}
			blockMsg, ok, err := s.preferredBlockRelayMessage(peer, item.Hash)
			if err != nil {
				return err
			}
			if !ok {
				notFound = append(notFound, item)
				continue
			}
			send = append(send, blockMsg)
			servedBlocks++
		case p2p.InvTypeBlockExtended:
			if servedBlocks >= maxServedBlocksPerGetData {
				notFound = append(notFound, item)
				continue
			}
			block, ok, err := s.loadBlock(item.Hash)
			if err != nil {
				return err
			}
			if !ok {
				notFound = append(notFound, item)
				continue
			}
			send = append(send, buildXThinBlockMessage(block))
			servedBlocks++
		case p2p.InvTypeBlockFull:
			if servedBlocks >= maxServedBlocksPerGetData {
				notFound = append(notFound, item)
				continue
			}
			block, ok, err := s.loadBlock(item.Hash)
			if err != nil {
				return err
			}
			if !ok {
				notFound = append(notFound, item)
				continue
			}
			send = append(send, p2p.BlockMessage{Block: block})
			servedBlocks++
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
	for start := 0; start < len(txs); start += txRelayBatchMaxItems {
		end := start + txRelayBatchMaxItems
		if end > len(txs) {
			end = len(txs)
		}
		send = append(send, p2p.TxBatchMessage{Txs: txs[start:end]})
	}
	for _, msg := range send {
		if err := peer.send(msg); err != nil {
			return err
		}
	}
	if len(notFound) == 0 {
		return nil
	}
	txNotFound := len(txIDsFromInvItems(notFound))
	if txNotFound > 0 {
		peer.telemetry.noteTxNotFoundSent(txNotFound)
		s.noteTxNotFoundSent(txNotFound)
	}
	return peer.send(p2p.NotFoundMessage{Items: notFound})
}

func (s *Service) onNotFoundMessage(peer *peerConn, msg p2p.NotFoundMessage) error {
	if txids := txIDsFromInvItems(msg.Items); len(txids) > 0 {
		peer.telemetry.noteTxNotFoundReceived(len(txids))
		s.noteTxNotFoundReceived(len(txids))
		peer.forgetKnownTxIDs(txids)
	}
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
			peer.clearGrapheneRecoveryPending(state.hash)
			return s.requestFullBlock(peer, state.hash)
		}
		if peer.clearGrapheneRecoveryPending(state.hash) {
			s.noteGrapheneExtendedRecovery()
		}
		return nil
	}
	if shouldFallbackToFullBlock(state, missing) {
		peer.deletePendingThin(state.hash)
		peer.clearGrapheneRecoveryPending(state.hash)
		return s.requestFullBlock(peer, state.hash)
	}
	peer.storePendingThin(state)
	return peer.send(p2p.GetXBlockTxMessage{BlockHash: state.hash, Indexes: missing})
}

func (s *Service) onCompactBlockMessage(peer *peerConn, msg p2p.CompactBlockMessage) error {
	s.noteCompactBlockReceived()
	hash := consensus.HeaderHash(&msg.Header)
	if ok, err := s.hasKnownBlock(hash); err != nil {
		return err
	} else if ok {
		s.releaseBlockRequest(hash)
		peer.deletePendingThin(hash)
		return nil
	}
	matches := s.pool.ShortIDMatches(func(txid [32]byte) uint64 {
		return thinBlockShortID(msg.Nonce, txid)
	}, compactShortIDSet(msg))
	state, missing := reconstructCompactBlock(msg, matches)
	if len(missing) == 0 {
		peer.deletePendingThin(state.hash)
		if err := s.acceptThinBlock(peer, state.block()); err != nil {
			s.noteCompactBlockFallback()
			s.noteGrapheneDecodeFailure()
			return s.requestGrapheneExtendedBlock(peer, state.hash)
		}
		s.noteCompactBlockRecovered()
		return nil
	}
	s.noteCompactBlockMissingTxs(len(missing))
	if shouldFallbackToFullBlock(state, missing) {
		peer.deletePendingThin(state.hash)
		s.noteCompactBlockFallback()
		s.noteGrapheneDecodeFailure()
		return s.requestGrapheneExtendedBlock(peer, state.hash)
	}
	peer.storePendingThin(state)
	s.noteCompactBlockTxRequest()
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
		peer.clearGrapheneRecoveryPending(msg.BlockHash)
		return s.requestFullBlock(peer, msg.BlockHash)
	}
	if peer.clearGrapheneRecoveryPending(msg.BlockHash) {
		s.noteGrapheneExtendedRecovery()
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
		s.noteCompactBlockFallback()
		s.noteGrapheneDecodeFailure()
		return s.requestGrapheneExtendedBlock(peer, msg.BlockHash)
	}
	for i, index := range msg.Indexes {
		if !state.fill(index, msg.Txs[i]) {
			peer.deletePendingThin(msg.BlockHash)
			s.noteCompactBlockFallback()
			s.noteGrapheneDecodeFailure()
			return s.requestGrapheneExtendedBlock(peer, msg.BlockHash)
		}
	}
	if !state.complete() {
		peer.deletePendingThin(msg.BlockHash)
		s.noteCompactBlockFallback()
		s.noteGrapheneDecodeFailure()
		return s.requestGrapheneExtendedBlock(peer, msg.BlockHash)
	}
	peer.deletePendingThin(msg.BlockHash)
	if err := s.acceptThinBlock(peer, state.block()); err != nil {
		s.noteCompactBlockFallback()
		s.noteGrapheneDecodeFailure()
		return s.requestGrapheneExtendedBlock(peer, msg.BlockHash)
	}
	s.noteCompactBlockRecovered()
	return nil
}

func (s *Service) preferredBlockRelayMessage(peer *peerConn, hash [32]byte) (p2p.Message, bool, error) {
	block, ok, err := s.loadBlock(hash)
	if err != nil || !ok {
		return nil, ok, err
	}
	plan := selectBlockRelayPlan(peer, block)
	s.noteBlockRelayPlan(plan)
	switch plan {
	case blockRelayPlanGrapheneExtended:
		return buildXThinBlockMessage(block), true, nil
	case blockRelayPlanCompactFallback:
		return buildCompactBlockMessage(block), true, nil
	default:
		return p2p.BlockMessage{Block: block}, true, nil
	}
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

func (s *Service) hasKnownBlock(hash [32]byte) (bool, error) {
	if _, ok := s.recentBlock(hash); ok {
		return true, nil
	}
	if s.chainState == nil {
		return false, nil
	}
	block, err := s.chainState.Store().GetBlock(&hash)
	if err != nil || block == nil {
		return false, err
	}
	return true, nil
}

func (s *Service) acceptThinBlock(peer *peerConn, block types.Block) error {
	return s.acceptPeerBlockMessage(peer, &block)
}

func (s *Service) requestFullBlock(peer *peerConn, hash [32]byte) error {
	if peer != nil {
		peer.clearGrapheneRecoveryPending(hash)
	}
	return peer.send(p2p.GetDataMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlockFull, Hash: hash}}})
}

func (s *Service) requestGrapheneExtendedBlock(peer *peerConn, hash [32]byte) error {
	if peer == nil || !peer.supportsGrapheneExtended() {
		return s.requestFullBlock(peer, hash)
	}
	peer.markGrapheneRecoveryPending(hash)
	return peer.send(p2p.GetDataMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlockExtended, Hash: hash}}})
}

func (s *Service) onTxReconMessage(peer *peerConn, msg p2p.TxReconMessage) error {
	if len(msg.TxIDs) == 0 {
		return nil
	}
	missing := s.pool.MissingTxIDs(msg.TxIDs)
	peer.noteErlayRoundResult(len(msg.TxIDs), len(missing))
	if s.logger != nil {
		s.logger.Debug("processed tx reconciliation announcement",
			slog.String("addr", peer.addr),
			slog.Int("announced", len(msg.TxIDs)),
			slog.Int("missing", len(missing)),
			slog.String("txids", hashesDebugSummary(msg.TxIDs, 6)),
		)
	}
	if len(missing) == 0 {
		return nil
	}
	request := s.syncManager().scheduleTxReconRequests(peer.addr, missing, len(missing))
	if len(request) == 0 {
		return nil
	}
	return peer.send(p2p.TxRequestMessage{TxIDs: request})
}

func (s *Service) onTxRequestMessage(peer *peerConn, msg p2p.TxRequestMessage) error {
	if len(msg.TxIDs) == 0 {
		return nil
	}
	peer.noteTxRequestReceived(msg.TxIDs)
	s.noteTxRequestsReceived(len(msg.TxIDs))
	txs := s.pool.TransactionsByID(msg.TxIDs)
	found := make(map[[32]byte]struct{}, len(txs))
	for _, tx := range txs {
		found[consensus.TxID(&tx)] = struct{}{}
	}
	missingItems := make([]p2p.InvVector, 0)
	for _, txid := range msg.TxIDs {
		if _, ok := found[txid]; ok {
			continue
		}
		missingItems = append(missingItems, p2p.InvVector{Type: p2p.InvTypeTx, Hash: txid})
	}
	if s.logger != nil {
		s.logger.Debug("serving tx reconciliation request",
			slog.String("addr", peer.addr),
			slog.Int("requested", len(msg.TxIDs)),
			slog.Int("found", len(txs)),
			slog.Int("missing", len(missingItems)),
			slog.String("txids", hashesDebugSummary(msg.TxIDs, 6)),
		)
	}
	for start := 0; start < len(txs); start += txRelayBatchMaxItems {
		end := start + txRelayBatchMaxItems
		if end > len(txs) {
			end = len(txs)
		}
		if err := peer.sendRequestedTxBatch(p2p.TxBatchMessage{Txs: txs[start:end]}); err != nil {
			return err
		}
	}
	if len(missingItems) > 0 {
		peer.telemetry.noteTxNotFoundSent(len(missingItems))
		s.noteTxNotFoundSent(len(missingItems))
		if err := peer.send(p2p.NotFoundMessage{Items: missingItems}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) broadcastInv(items []p2p.InvVector) { s.relayManager().broadcastInv(items) }

func (s *Service) broadcastMinedCompactBlock(block types.Block) {
	msg := buildCompactBlockMessage(block)
	compact, ok := msg.(p2p.CompactBlockMessage)
	if !ok {
		return
	}
	for _, peer := range s.peerSnapshot() {
		if !peer.supportsCompactBlockRelay() {
			continue
		}
		if err := peer.send(compact); err != nil && s.logger != nil {
			s.logger.Debug("mined compact block relay enqueue failed",
				slog.String("addr", peer.addr),
				slog.Int("short_ids", len(compact.ShortIDs)),
				slog.Any("error", err),
			)
		}
	}
}

func (s *Service) broadcastHeaders(headers []types.BlockHeader) {
	if len(headers) == 0 {
		return
	}
	for _, peer := range s.peerSnapshot() {
		if err := peer.send(p2p.HeadersMessage{Headers: headers}); err != nil && s.logger != nil {
			s.logger.Debug("header broadcast enqueue failed",
				slog.String("addr", peer.addr),
				slog.Int("count", len(headers)),
				slog.Any("error", err),
			)
		}
	}
}
func (s *Service) broadcastInvToPeers(peers []*peerConn, items []p2p.InvVector) {
	s.relayManager().broadcastInvToPeers(peers, items)
}
func (s *Service) broadcastAcceptedTxsToPeers(peers []*peerConn, accepted []mempool.AcceptedTx) {
	s.relayManager().broadcastAcceptedTxsToPeers(peers, accepted)
	erlayPeers := make([]*peerConn, 0, len(peers))
	for _, peer := range peers {
		if peer.supportsErlayTxRelay() {
			erlayPeers = append(erlayPeers, peer)
		}
	}
	s.scheduleLocalRelayFallbacks(erlayPeers, accepted)
}

func (s *Service) relayAcceptedTxs(peers []*peerConn, accepted []mempool.AcceptedTx, source *peerConn) {
	if len(peers) == 0 || len(accepted) == 0 {
		return
	}
	if !s.cfg.DandelionEnabled {
		if source == nil {
			s.broadcastLocalAcceptedTxBatches(peers, accepted)
			return
		}
		s.broadcastAcceptedTxsToPeers(peers, accepted)
		return
	}
	stemPeer := selectDandelionStemPeer(peers)
	if stemPeer == nil {
		s.broadcastAcceptedTxsToPeers(peers, accepted)
		return
	}
	// Keep the first-hop footprint to one outbound peer, then fall back to the
	// normal Erlay/legacy diffusion path after a short embargo.
	s.broadcastAcceptedTxsToPeers([]*peerConn{stemPeer}, accepted)
	sourceAddr := ""
	if source != nil {
		sourceAddr = source.addr
	}
	txids := acceptedTxIDs(accepted)
	s.safeGoDetached("dandelion-fluff-delay", func() {
		s.runDandelionFluffAfterDelay(txids, sourceAddr)
	})
}

func (s *Service) broadcastLocalAcceptedTxBatches(peers []*peerConn, accepted []mempool.AcceptedTx) {
	if len(peers) == 0 || len(accepted) == 0 {
		return
	}
	txids := acceptedTxIDs(accepted)
	txs := s.pool.TransactionsByID(txids)
	if len(txs) == 0 {
		s.broadcastAcceptedTxsToPeers(peers, accepted)
		return
	}
	// Locally submitted transactions are already past this node's policy checks.
	// Send them directly on non-Dandelion relay so nearby peers can validate and
	// mine them without a txrecon -> txreq -> txbatch round trip.
	if len(peers) == 1 {
		s.broadcastLocalAcceptedTxBatchToPeer(peers[0], txs)
		return
	}
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer *peerConn) {
			defer wg.Done()
			s.broadcastLocalAcceptedTxBatchToPeer(peer, txs)
		}(peer)
	}
	wg.Wait()
}

func (s *Service) broadcastLocalAcceptedTxBatchToPeer(peer *peerConn, txs []types.Transaction) {
	if peer == nil || len(txs) == 0 {
		return
	}
	for start := 0; start < len(txs); start += txRelayBatchMaxItems {
		end := min(len(txs), start+txRelayBatchMaxItems)
		if err := peer.enqueueLocalTxBatch(p2p.TxBatchMessage{Txs: txs[start:end]}); err != nil && s.logger != nil {
			s.logger.Debug("local tx batch relay enqueue failed",
				slog.String("addr", peer.addr),
				slog.Int("count", end-start),
				slog.Any("error", err),
			)
			return
		}
	}
}

func (s *Service) broadcastAcceptedTxsToPeersRetry(peers []*peerConn, accepted []mempool.AcceptedTx) {
	if len(peers) == 0 || len(accepted) == 0 {
		return
	}
	txids := make([][32]byte, 0, len(accepted))
	for _, item := range accepted {
		txids = append(txids, item.TxID)
	}
	erlayPeers := make([]*peerConn, 0, len(peers))
	legacyPeers := make([]*peerConn, 0, len(peers))
	for _, peer := range peers {
		if peer.supportsErlayTxRelay() {
			erlayPeers = append(erlayPeers, peer)
			continue
		}
		legacyPeers = append(legacyPeers, peer)
	}
	for _, batch := range planTxRelayRecon(erlayPeers, txids) {
		if err := batch.peer.enqueueTxReconRetry(p2p.TxReconMessage{TxIDs: batch.txids}); err != nil {
			if s.logger != nil {
				s.logger.Debug("relay retry txrecon enqueue failed",
					slog.String("addr", batch.peer.addr),
					slog.Int("count", len(batch.txids)),
					slog.Any("error", err),
				)
			}
		}
	}
	if len(legacyPeers) > 0 {
		items := make([]p2p.InvVector, 0, len(txids))
		for _, txid := range txids {
			items = append(items, p2p.InvVector{Type: p2p.InvTypeTx, Hash: txid})
		}
		s.broadcastInvToPeers(legacyPeers, items)
	}
}

const localRelayFallbackGrace = 750 * time.Millisecond

func (s *Service) scheduleLocalRelayFallbacks(peers []*peerConn, accepted []mempool.AcceptedTx) {
	if len(peers) == 0 || len(accepted) == 0 {
		return
	}
	txids := s.localOriginRelayTxIDs(accepted)
	if len(txids) == 0 {
		return
	}
	announcedAt := time.Now()
	for _, peer := range peers {
		armed := peer.armLocalRelayFallback(txids, announcedAt)
		if armed == 0 {
			continue
		}
		fallbackTxIDs := append([][32]byte(nil), txids...)
		s.safeGoDetached("local-relay-fallback", func() {
			s.runLocalRelayFallback(peer, fallbackTxIDs)
		})
	}
}

func (s *Service) localOriginRelayTxIDs(accepted []mempool.AcceptedTx) [][32]byte {
	if len(accepted) == 0 {
		return nil
	}
	s.rebroadcastMu.Lock()
	defer s.rebroadcastMu.Unlock()
	txids := make([][32]byte, 0, len(accepted))
	for _, item := range accepted {
		if _, ok := s.localRebroadcast[item.TxID]; !ok {
			continue
		}
		txids = append(txids, item.TxID)
	}
	return txids
}

func acceptedTxIDs(accepted []mempool.AcceptedTx) [][32]byte {
	if len(accepted) == 0 {
		return nil
	}
	txids := make([][32]byte, 0, len(accepted))
	for _, item := range accepted {
		txids = append(txids, item.TxID)
	}
	return txids
}

func selectDandelionStemPeer(peers []*peerConn) *peerConn {
	candidates := make([]*peerConn, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || !peer.outbound {
			continue
		}
		candidates = append(candidates, peer)
	}
	if len(candidates) == 0 {
		return nil
	}
	slices.SortFunc(candidates, func(a, b *peerConn) int {
		return strings.Compare(a.addr, b.addr)
	})
	return candidates[0]
}

func (s *Service) runDandelionFluffAfterDelay(txids [][32]byte, sourceAddr string) {
	if len(txids) == 0 {
		return
	}
	timer := time.NewTimer(dandelionFluffDelay)
	defer timer.Stop()
	select {
	case <-s.stopCh:
		return
	case <-timer.C:
	}
	due := make([]mempool.AcceptedTx, 0, len(txids))
	for _, txid := range txids {
		if !s.pool.Contains(txid) {
			continue
		}
		due = append(due, mempool.AcceptedTx{TxID: txid})
	}
	if len(due) == 0 {
		return
	}
	peers := s.peerSnapshot()
	if sourceAddr != "" {
		filtered := peers[:0]
		for _, peer := range peers {
			if peer.addr == sourceAddr {
				continue
			}
			filtered = append(filtered, peer)
		}
		peers = filtered
	}
	s.broadcastAcceptedTxsToPeers(peers, due)
}

func (s *Service) runLocalRelayFallback(peer *peerConn, txids [][32]byte) {
	timer := time.NewTimer(localRelayFallbackGrace)
	defer timer.Stop()
	select {
	case <-s.stopCh:
		return
	case <-peer.closed:
		return
	case <-timer.C:
	}
	due := peer.collectDueLocalRelayFallback(time.Now(), localRelayFallbackGrace, txids)
	if len(due) == 0 {
		return
	}
	txs := s.pool.TransactionsByID(due)
	if len(txs) == 0 {
		return
	}
	// Send fallback batches directly rather than waiting for another TxRecon
	// round so local-origin funding txs can recover from a missed request path.
	for start := 0; start < len(txs); start += txRelayBatchMaxItems {
		end := start + txRelayBatchMaxItems
		if end > len(txs) {
			end = len(txs)
		}
		batch := p2p.TxBatchMessage{Txs: txs[start:end]}
		if err := peer.enqueueFallbackTxBatch(batch); err != nil {
			return
		}
		s.noteDirectFallback(1, len(batch.Txs))
		if s.logger != nil {
			s.logger.Debug("sent direct fallback tx batch",
				slog.String("addr", peer.addr),
				slog.Int("txs", len(batch.Txs)),
				slog.String("txids", txDebugSummary(batch.Txs, 6)),
			)
		}
	}
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
	case p2p.GetDataMessage:
		for _, item := range m.Items {
			if item.Type == p2p.InvTypeBlock || item.Type == p2p.InvTypeBlockFull || item.Type == p2p.InvTypeBlockExtended {
				class.blockReqItems++
			}
		}
	case p2p.GetBlockTxMessage, p2p.GetXBlockTxMessage:
		class.blockReqItems = 1
	case p2p.BlockMessage, p2p.CompactBlockMessage, p2p.XThinBlockMessage, p2p.BlockTxMessage, p2p.XBlockTxMessage:
		class.blockSendItems = 1
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
	duplicates := 0
	for _, item := range items {
		if p.queuedInv[item] > 0 {
			duplicates++
			continue
		}
		p.queuedInv[item]++
		filtered = append(filtered, item)
	}
	if duplicates > 0 {
		p.telemetry.noteDuplicateInv(duplicates)
		if p.svc != nil {
			p.svc.noteDuplicateSuppression(duplicates)
		}
		p.maybeLogRelaySuppression("inv", duplicates, 0, classifyRelayMessage(p2p.InvMessage{Items: items}))
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
	duplicateQueued := 0
	suppressedKnown := 0
	for _, tx := range txs {
		txid := consensus.TxID(&tx)
		if p.queuedTx[txid] > 0 {
			duplicateQueued++
			continue
		}
		if _, ok := p.knownTx[txid]; ok {
			suppressedKnown++
			continue
		}
		p.queuedTx[txid]++
		filtered = append(filtered, tx)
	}
	if duplicateQueued > 0 {
		p.telemetry.noteDuplicateTx(duplicateQueued)
		if p.svc != nil {
			p.svc.noteDuplicateSuppression(duplicateQueued)
		}
	}
	if suppressedKnown > 0 {
		p.telemetry.noteKnownTxSuppressed(suppressedKnown)
		if p.svc != nil {
			p.svc.noteDuplicateSuppression(suppressedKnown)
		}
	}
	if duplicateQueued > 0 || suppressedKnown > 0 {
		p.maybeLogRelaySuppression("tx_batch", duplicateQueued, suppressedKnown, classifyRelayMessage(p2p.TxBatchMessage{Txs: txs}))
	}
	return filtered
}

func (p *peerConn) filterQueuedTxIDs(txids [][32]byte, suppressKnown bool) [][32]byte {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	filtered := make([][32]byte, 0, len(txids))
	duplicateQueued := 0
	suppressedKnownCount := 0
	for _, txid := range txids {
		if p.queuedTx[txid] > 0 {
			duplicateQueued++
			continue
		}
		if suppressKnown {
			if _, ok := p.knownTx[txid]; ok {
				suppressedKnownCount++
				continue
			}
		}
		p.queuedTx[txid]++
		filtered = append(filtered, txid)
	}
	if duplicateQueued > 0 {
		p.telemetry.noteDuplicateTx(duplicateQueued)
		if p.svc != nil {
			p.svc.noteDuplicateSuppression(duplicateQueued)
		}
	}
	if suppressedKnownCount > 0 {
		p.telemetry.noteKnownTxSuppressed(suppressedKnownCount)
		if p.svc != nil {
			p.svc.noteDuplicateSuppression(suppressedKnownCount)
		}
	}
	if duplicateQueued > 0 || suppressedKnownCount > 0 {
		p.maybeLogRelaySuppression("tx_recon", duplicateQueued, suppressedKnownCount, classifyRelayMessage(p2p.TxReconMessage{TxIDs: txids}))
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
	p.txMu.Lock()
	defer p.txMu.Unlock()
	if p.pendingTxByID == nil {
		p.pendingTxByID = make(map[[32]byte]*types.Transaction, len(txs))
	}
	for _, tx := range txs {
		txCopy := tx
		txid := consensus.TxID(&tx)
		p.pendingTxOrder = append(p.pendingTxOrder, txid)
		p.pendingTxByID[txid] = &txCopy
	}
	p.telemetry.noteCoalescedTxs(len(txs))
	ready := make([][]types.Transaction, 0, len(p.pendingTxOrder)/txRelayBatchMaxItems+1)
	for len(p.pendingTxOrder) >= txRelayBatchMaxItems {
		ready = append(ready, p.takePendingTxBatchLocked(txRelayBatchMaxItems))
	}
	armFlush := false
	if len(p.pendingTxOrder) != 0 && !p.txFlushArmed {
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
	p.telemetry.noteCoalescedRecon(len(txids))
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

func (p *peerConn) stagePendingTxRequests(txids [][32]byte) ([][][32]byte, bool) {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	if p.pendingReqSet == nil {
		p.pendingReqSet = make(map[[32]byte]struct{}, len(txids))
	}
	for _, txid := range txids {
		if _, ok := p.pendingReqSet[txid]; ok {
			continue
		}
		p.pendingReqOrder = append(p.pendingReqOrder, txid)
		p.pendingReqSet[txid] = struct{}{}
	}
	ready := make([][][32]byte, 0, len(p.pendingReqOrder)/txRelayBatchMaxItems+1)
	for len(p.pendingReqOrder) >= txRelayBatchMaxItems {
		ready = append(ready, p.takePendingTxRequestBatchLocked(txRelayBatchMaxItems))
	}
	armFlush := false
	if len(p.pendingReqOrder) != 0 && !p.reqFlushArmed {
		p.reqFlushArmed = true
		armFlush = true
	}
	return ready, armFlush
}

func (p *peerConn) flushPendingTxsAfterDelay() {
	startedAt := time.Now()
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-p.closed:
		return
	case <-timer.C:
	}
	batches := p.takePendingTxs()
	txCount := 0
	for _, batch := range batches {
		txCount += len(batch)
		p.enqueueRelayTxs(batch)
	}
	if p.svc != nil && len(batches) > 0 {
		p.svc.perf.noteRelayFlushDuration(time.Since(startedAt))
		p.svc.logger.Debug("flushed pending tx relay",
			slog.String("addr", p.addr),
			slog.Int("batches", len(batches)),
			slog.Int("tx_count", txCount),
			slog.Duration("flush_duration", time.Since(startedAt)),
		)
	}
}

func (p *peerConn) flushPendingTxRequestsAfterDelay() {
	startedAt := time.Now()
	timer := time.NewTimer(txRequestCoalesceDelay)
	defer timer.Stop()
	select {
	case <-p.closed:
		return
	case <-timer.C:
	}
	batches := p.takePendingTxRequests()
	txidCount := 0
	for _, batch := range batches {
		txidCount += len(batch)
		if err := p.enqueueRelayTxRequest(batch); err != nil {
			return
		}
	}
	if p.svc != nil && p.svc.logger != nil && len(batches) > 0 {
		p.svc.perf.noteRelayFlushDuration(time.Since(startedAt))
		p.svc.logger.Debug("flushed pending tx requests",
			slog.String("addr", p.addr),
			slog.Int("batches", len(batches)),
			slog.Int("txid_count", txidCount),
			slog.Duration("flush_duration", time.Since(startedAt)),
		)
	}
}

func (p *peerConn) flushPendingReconAfterDelay() {
	startedAt := time.Now()
	timer := time.NewTimer(erlayReconFlushDelay)
	defer timer.Stop()
	select {
	case <-p.closed:
		return
	case <-timer.C:
	}
	batches := p.takePendingRecon()
	txidCount := 0
	for _, batch := range batches {
		txidCount += len(batch)
		p.enqueueRelayRecon(batch)
	}
	if p.svc != nil && p.svc.logger != nil && len(batches) > 0 {
		p.svc.perf.noteRelayFlushDuration(time.Since(startedAt))
		p.svc.logger.Debug("flushed pending tx reconciliation",
			slog.String("addr", p.addr),
			slog.Int("batches", len(batches)),
			slog.Int("txid_count", txidCount),
			slog.Duration("flush_duration", time.Since(startedAt)),
		)
	}
}

func (p *peerConn) clearPendingTxFlushArm() {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	p.txFlushArmed = false
}

func (p *peerConn) clearPendingReconFlushArm() {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	p.reconFlushArmed = false
}

func (p *peerConn) clearPendingTxRequestFlushArm() {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	p.reqFlushArmed = false
}

func (p *peerConn) takePendingTxs() [][]types.Transaction {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	defer func() {
		p.txFlushArmed = false
	}()
	if len(p.pendingTxOrder) == 0 {
		return nil
	}
	batches := make([][]types.Transaction, 0, (len(p.pendingTxOrder)+txRelayBatchMaxItems-1)/txRelayBatchMaxItems)
	for len(p.pendingTxOrder) != 0 {
		end := txRelayBatchMaxItems
		if end > len(p.pendingTxOrder) {
			end = len(p.pendingTxOrder)
		}
		batches = append(batches, p.takePendingTxBatchLocked(end))
	}
	return batches
}

func (p *peerConn) takePendingTxRequests() [][][32]byte {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	defer func() {
		p.reqFlushArmed = false
	}()
	if len(p.pendingReqOrder) == 0 {
		return nil
	}
	batches := make([][][32]byte, 0, (len(p.pendingReqOrder)+txRelayBatchMaxItems-1)/txRelayBatchMaxItems)
	for len(p.pendingReqOrder) != 0 {
		end := txRelayBatchMaxItems
		if end > len(p.pendingReqOrder) {
			end = len(p.pendingReqOrder)
		}
		batches = append(batches, p.takePendingTxRequestBatchLocked(end))
	}
	return batches
}

func (p *peerConn) takePendingTxBatchLocked(size int) []types.Transaction {
	batch := make([]types.Transaction, 0, size)
	for _, txid := range p.pendingTxOrder[:size] {
		tx, ok := p.pendingTxByID[txid]
		if !ok {
			continue
		}
		batch = append(batch, *tx)
		delete(p.pendingTxByID, txid)
	}
	p.pendingTxOrder = p.pendingTxOrder[size:]
	if len(p.pendingTxOrder) == 0 {
		p.pendingTxOrder = nil
		p.pendingTxByID = nil
	}
	return batch
}

func (p *peerConn) takePendingTxRequestBatchLocked(size int) [][32]byte {
	batch := append([][32]byte(nil), p.pendingReqOrder[:size]...)
	for _, txid := range batch {
		delete(p.pendingReqSet, txid)
	}
	p.pendingReqOrder = p.pendingReqOrder[size:]
	if len(p.pendingReqOrder) == 0 {
		p.pendingReqOrder = nil
		p.pendingReqSet = nil
	}
	return batch
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
		lane:       relayQueueLaneSend,
		class:      classifyRelayMessage(p2p.TxBatchMessage{Txs: txs}),
	}
	select {
	case <-p.closed:
		p.releaseQueuedTxs(txs)
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
		p.releaseQueuedTxs(txs)
		p.telemetry.noteDroppedTxs(len(txs), envelope.lane)
		if len(txs) >= relayDropWarnThreshold || p.queueDepths().send >= relayQueueWarnDepth {
			p.logRelayQueuePressure(slog.LevelWarn, "dropped tx relay batch due to saturated send queue", envelope.lane, envelope.class, len(txs))
		}
		return nil
	}
}

func (p *peerConn) enqueueRelayTxRequest(txids [][32]byte) error {
	if len(txids) == 0 {
		return nil
	}
	envelope := outboundMessage{
		msg:        p2p.TxRequestMessage{TxIDs: txids},
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneControl,
		class:      classifyRelayMessage(p2p.TxRequestMessage{TxIDs: txids}),
	}
	return p.enqueueDirectMessage(envelope)
}

func (p *peerConn) enqueueRelayRecon(txids [][32]byte) error {
	if len(txids) == 0 {
		return nil
	}
	envelope := outboundMessage{
		msg:        p2p.TxReconMessage{TxIDs: txids},
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneSend,
		class:      classifyRelayMessage(p2p.TxReconMessage{TxIDs: txids}),
	}
	select {
	case <-p.closed:
		p.releaseQueuedTxIDs(txids)
		return io.EOF
	case p.sendQ <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
		p.releaseQueuedTxIDs(txids)
		p.telemetry.noteDroppedTxs(len(txids), envelope.lane)
		if len(txids) >= relayDropWarnThreshold || p.queueDepths().send >= relayQueueWarnDepth {
			p.logRelayQueuePressure(slog.LevelWarn, "dropped tx reconciliation batch due to saturated send queue", envelope.lane, envelope.class, len(txids))
		}
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

func (p *peerConn) forgetKnownTxIDs(txids [][32]byte) {
	if len(txids) == 0 {
		return
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	cleared := 0
	for _, txid := range txids {
		if _, ok := p.knownTx[txid]; ok {
			delete(p.knownTx, txid)
			cleared++
		}
	}
	if cleared > 0 {
		p.telemetry.noteKnownTxClears(cleared)
		if p.svc != nil {
			p.svc.noteKnownTxClears(cleared)
		}
	}
}

func (p *peerConn) armLocalRelayFallback(txids [][32]byte, announcedAt time.Time) int {
	if len(txids) == 0 {
		return 0
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	if p.localRelayTxs == nil {
		p.localRelayTxs = make(map[[32]byte]localRelayFallbackState, len(txids))
	}
	armed := 0
	for _, txid := range txids {
		if _, ok := p.localRelayTxs[txid]; ok {
			continue
		}
		p.localRelayTxs[txid] = localRelayFallbackState{announcedAt: announcedAt}
		armed++
	}
	return armed
}

func (p *peerConn) collectDueLocalRelayFallback(now time.Time, grace time.Duration, txids [][32]byte) [][32]byte {
	if len(txids) == 0 {
		return nil
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	due := make([][32]byte, 0, len(txids))
	for _, txid := range txids {
		state, ok := p.localRelayTxs[txid]
		if !ok {
			continue
		}
		if grace > 0 && now.Sub(state.announcedAt) < grace {
			continue
		}
		due = append(due, txid)
		delete(p.localRelayTxs, txid)
	}
	return due
}

func (p *peerConn) noteTxRequestReceived(txids [][32]byte) {
	if len(txids) == 0 {
		return
	}
	p.txMu.Lock()
	defer p.txMu.Unlock()
	if p.localRelayTxs != nil {
		for _, txid := range txids {
			delete(p.localRelayTxs, txid)
		}
	}
	p.telemetry.noteTxRequestReceived(len(txids))
}

func (p *peerConn) pendingLocalRelayCount() int {
	p.txMu.Lock()
	defer p.txMu.Unlock()
	return len(p.localRelayTxs)
}

func (p *peerConn) rememberKnownTxLocked(txid [32]byte) {
	if _, ok := p.knownTx[txid]; ok {
		return
	}
	if p.knownTx == nil {
		p.knownTx = make(map[[32]byte]struct{}, peerKnownTxLimit)
	}
	p.knownTx[txid] = struct{}{}
	if len(p.knownTxOrder) < peerKnownTxLimit {
		p.knownTxOrder = append(p.knownTxOrder, txid)
		return
	}
	evict := p.knownTxOrder[p.knownTxNext]
	delete(p.knownTx, evict)
	p.knownTxOrder[p.knownTxNext] = txid
	p.knownTxNext++
	if p.knownTxNext >= peerKnownTxLimit {
		p.knownTxNext = 0
	}
}

func txIDsFromInvItems(items []p2p.InvVector) [][32]byte {
	if len(items) == 0 {
		return nil
	}
	out := make([][32]byte, 0, len(items))
	for _, item := range items {
		if item.Type != p2p.InvTypeTx {
			continue
		}
		out = append(out, item.Hash)
	}
	return out
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

func (t *peerRelayTelemetry) noteEnqueue(depth queueDepthSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if depth.total > t.maxQueueDepth {
		t.maxQueueDepth = depth.total
	}
	if depth.control > t.maxControlQueueDepth {
		t.maxControlQueueDepth = depth.control
	}
	if depth.priority > t.maxPriorityQueueDepth {
		t.maxPriorityQueueDepth = depth.priority
	}
	if depth.send > t.maxSendQueueDepth {
		t.maxSendQueueDepth = depth.send
	}
}

func (t *peerRelayTelemetry) noteDuplicateInv(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.duplicateInv += count
}

func (t *peerRelayTelemetry) noteDuplicateTx(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.duplicateTx += count
}

func (t *peerRelayTelemetry) noteKnownTxSuppressed(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.knownTxSuppressed += count
}

func (t *peerRelayTelemetry) noteCoalescedTxs(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.coalescedTxItems += count
}

func (t *peerRelayTelemetry) noteCoalescedRecon(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.coalescedReconItems += count
}

func (t *peerRelayTelemetry) noteTxReconRetry(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.txReconRetries += count
}

func (t *peerRelayTelemetry) noteTxRequestReceived(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.txReqRecvMsgs++
	t.txReqRecvItems += count
}

func (t *peerRelayTelemetry) noteFallbackBatch(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fallbackTxBatchMsgs++
	t.fallbackTxBatchItems += count
}

func (t *peerRelayTelemetry) noteTxNotFoundSent(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.txNotFoundSent += count
}

func (t *peerRelayTelemetry) noteTxNotFoundReceived(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.txNotFoundReceived += count
}

func (t *peerRelayTelemetry) noteKnownTxClears(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.knownTxClears += count
}

func (t *peerRelayTelemetry) noteWriterStarvation(lane relayQueueLane) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writerStarvation++
	switch lane {
	case relayQueueLaneControl:
		t.controlStarvation++
	case relayQueueLanePriority:
		t.priorityStarvation++
	case relayQueueLaneSend:
		t.sendStarvation++
	}
}

func (t *peerRelayTelemetry) noteDroppedInv(count int, lane relayQueueLane) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.droppedInv += count
	switch lane {
	case relayQueueLanePriority:
		t.droppedPriorityInv += count
	case relayQueueLaneSend:
		t.droppedSendInv += count
	}
}

func (t *peerRelayTelemetry) noteDroppedTxs(count int, lane relayQueueLane) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.droppedTxs += count
	if lane == relayQueueLaneSend {
		t.droppedSendTxs += count
	}
}

func (t *peerRelayTelemetry) noteSent(envelope outboundMessage, _ int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sentMessages++
	t.txInvItems += envelope.class.txInvItems
	t.blockInvItems += envelope.class.blockInvItems
	t.blockSendItems += envelope.class.blockSendItems
	t.blockReqItems += envelope.class.blockReqItems
	t.txBatchMsgs += envelope.class.txBatchMsgs
	t.txBatchItems += envelope.class.txBatchItems
	t.fallbackTxBatchMsgs += envelope.class.fallbackTxBatchMsgs
	t.fallbackTxBatchItems += envelope.class.fallbackTxBatchItems
	t.txReconMsgs += envelope.class.txReconMsgs
	t.txReconItems += envelope.class.txReconItems
	t.txReqMsgs += envelope.class.txReqMsgs
	t.txReqItems += envelope.class.txReqItems
	if !envelope.enqueuedAt.IsZero() {
		t.lastRelayActivityUnix = time.Now().Unix()
	}
	if envelope.class.txInvItems != 0 || envelope.class.blockInvItems != 0 || envelope.class.txBatchItems != 0 || envelope.class.txReconItems != 0 || envelope.class.txReqItems != 0 {
		delay := time.Since(envelope.enqueuedAt)
		t.relaySamples = append(t.relaySamples, float64(delay.Microseconds())/1000)
	}
}

func relayLaneBudget(lane relayQueueLane) time.Duration {
	switch lane {
	case relayQueueLaneControl:
		return 100 * time.Millisecond
	case relayQueueLanePriority:
		return 200 * time.Millisecond
	case relayQueueLaneSend:
		return 500 * time.Millisecond
	default:
		return 0
	}
}

func (t *peerRelayTelemetry) snapshot(addr string, outbound bool, queueDepth queueDepthSnapshot, pendingLocalRelay int) PeerRelayStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := PeerRelayStats{
		Addr:                   addr,
		Outbound:               outbound,
		QueueDepth:             queueDepth.total,
		MaxQueueDepth:          t.maxQueueDepth,
		ControlQueueDepth:      queueDepth.control,
		PriorityQueueDepth:     queueDepth.priority,
		SendQueueDepth:         queueDepth.send,
		MaxControlQueueDepth:   t.maxControlQueueDepth,
		MaxPriorityQueueDepth:  t.maxPriorityQueueDepth,
		MaxSendQueueDepth:      t.maxSendQueueDepth,
		PendingLocalRelayTxs:   pendingLocalRelay,
		SentMessages:           t.sentMessages,
		TxInvItems:             t.txInvItems,
		BlockInvItems:          t.blockInvItems,
		BlockSendItems:         t.blockSendItems,
		BlockReqItems:          t.blockReqItems,
		TxBatchMsgs:            t.txBatchMsgs,
		TxBatchItems:           t.txBatchItems,
		TxReconMsgs:            t.txReconMsgs,
		TxReconItems:           t.txReconItems,
		TxReconRetries:         t.txReconRetries,
		TxReqMsgs:              t.txReqMsgs,
		TxReqItems:             t.txReqItems,
		TxReqRecvMsgs:          t.txReqRecvMsgs,
		TxReqRecvItems:         t.txReqRecvItems,
		FallbackTxBatchMsgs:    t.fallbackTxBatchMsgs,
		FallbackTxBatchItems:   t.fallbackTxBatchItems,
		TxNotFoundSent:         t.txNotFoundSent,
		TxNotFoundReceived:     t.txNotFoundReceived,
		KnownTxClears:          t.knownTxClears,
		DuplicateInvSuppressed: t.duplicateInv,
		DuplicateTxSuppressed:  t.duplicateTx,
		KnownTxSuppressed:      t.knownTxSuppressed,
		CoalescedTxItems:       t.coalescedTxItems,
		CoalescedReconItems:    t.coalescedReconItems,
		DroppedInv:             t.droppedInv,
		DroppedTxs:             t.droppedTxs,
		WriterStarvationEvents: t.writerStarvation,
		DroppedPriorityInv:     t.droppedPriorityInv,
		DroppedSendInv:         t.droppedSendInv,
		DroppedSendTxs:         t.droppedSendTxs,
		ControlStarvation:      t.controlStarvation,
		PriorityStarvation:     t.priorityStarvation,
		SendStarvation:         t.sendStarvation,
		LastRelayActivityUnix:  t.lastRelayActivityUnix,
		RelayEvents:            len(t.relaySamples),
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
	gapDetected := false
	blockTip := s.chainState.ChainState().TipHeight()
	headerTip := s.headerChain.TipHeight()
	if blockTip == nil || headerTip == nil {
		return out, false, nil
	}
	startHeight := *blockTip + 1
	if tipHeader := s.chainState.ChainState().TipHeader(); tipHeader != nil {
		tipHash := consensus.HeaderHash(tipHeader)
		activeTipHash, err := s.chainState.Store().GetCanonicalHeaderHashByHeight(*blockTip)
		if err != nil {
			return nil, false, err
		}
		if activeTipHash == nil {
			return out, true, nil
		}
		if *activeTipHash != tipHash {
			forkHeight, err := s.firstMissingActiveBlockHeightLocked(*blockTip, tipHash)
			if err != nil {
				return nil, false, err
			}
			startHeight = forkHeight
		}
	}
	for height := startHeight; height <= *headerTip && len(out) < limit; height++ {
		// Missing derived index entries are worth flagging for repair, but they
		// should not block steady-state sync: the canonical active header path is
		// still the source of truth for which blocks we need next.
		indexedHash, err := s.chainState.Store().GetIndexedHeaderHashByHeight(height)
		if err != nil {
			return nil, false, err
		}
		if indexedHash == nil {
			gapDetected = true
		}
		hash, err := s.chainState.Store().GetCanonicalHeaderHashByHeight(height)
		if err != nil {
			return nil, false, err
		}
		if hash == nil {
			return out, true, nil
		}
		block, err := s.chainState.Store().GetBlock(hash)
		if err != nil {
			return nil, false, err
		}
		if block == nil {
			if s.hasPendingPeerBlock(*hash) {
				continue
			}
			out = append(out, *hash)
		}
	}
	return out, gapDetected, nil
}

func (s *Service) firstMissingActiveBlockHeightLocked(blockTipHeight uint64, blockTipHash [32]byte) (uint64, error) {
	cursorHeight := blockTipHeight
	cursorHash := blockTipHash
	for {
		activeHash, err := s.chainState.Store().GetCanonicalHeaderHashByHeight(cursorHeight)
		if err != nil {
			return 0, err
		}
		if activeHash == nil {
			return 0, fmt.Errorf("missing active header hash at height %d while locating fork point", cursorHeight)
		}
		if *activeHash == cursorHash {
			return cursorHeight + 1, nil
		}
		if cursorHeight == 0 {
			return 1, nil
		}
		entry, err := s.chainState.Store().GetBlockIndex(&cursorHash)
		if err != nil {
			return 0, err
		}
		if entry == nil {
			return 0, fmt.Errorf("missing block index for active tip ancestor %x", cursorHash)
		}
		cursorHash = entry.ParentHash
		cursorHeight--
	}
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
func (s *Service) loadPersistedKnownPeers(peers map[string]storage.KnownPeerRecord) {
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

func (s *Service) isStopping() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
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
		activeHash, err := s.chainState.Store().GetCanonicalHeaderHashByHeight(entry.Height)
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
	case path == "/fees":
		return renderPublicFeePage(view), http.StatusOK
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
	case strings.HasPrefix(path, "/peer/"):
		host, err := url.PathUnescape(strings.TrimPrefix(path, "/peer/"))
		if err == nil {
			for _, peerHost := range view.peerHosts {
				if peerHost.Host == host {
					return renderPublicPeerPage(view, peerHost), http.StatusOK
				}
			}
		}
	}
	return renderPublicNotFoundPage(view), http.StatusNotFound
}

type dashboardKeyValueRow struct {
	Key   string
	Value string
}

// Render dashboard metadata with a per-section label width so each value
// starts at a stable column without hand-maintained spacing.
func renderDashboardKeyValues(rows ...dashboardKeyValueRow) string {
	if len(rows) == 0 {
		return ""
	}
	width := 0
	for _, row := range rows {
		if len(row.Key) > width {
			width = len(row.Key)
		}
	}
	var out strings.Builder
	for _, row := range rows {
		if row.Key == "" {
			continue
		}
		out.WriteString(fmt.Sprintf(" %-*s : %s\n", width, row.Key, row.Value))
	}
	return out.String()
}

func renderPublicDashboardHome(view *publicDashboardView) string {
	var body strings.Builder
	body.Grow(32768)
	body.WriteString(renderHTMLPrologue("Bitcoin Pure Monitor"))
	body.WriteString(renderDashboardBanner())
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("CHAIN OVERVIEW"))
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "Health", Value: view.health},
		dashboardKeyValueRow{Key: "Tip Height", Value: fmt.Sprintf("%d", view.info.TipHeight)},
		dashboardKeyValueRow{Key: "Tip Hash", Value: linkHash("/block/", view.info.TipHeaderHash, 14)},
		dashboardKeyValueRow{Key: "Last Block", Value: formatDashboardAgeFromTime(view.generatedAt, view.pow.LastBlockTimestamp)},
		dashboardKeyValueRow{Key: "Network Hashrate", Value: formatHumanHashRate(view.pow.NetworkHashrate)},
		dashboardKeyValueRow{Key: "Peers", Value: fmt.Sprintf("%d", len(view.peers))},
		dashboardKeyValueRow{Key: "Mempool", Value: fmt.Sprintf("%d tx", view.mempool.Count)},
		dashboardKeyValueRow{Key: "Current Orphan Blocks", Value: fmt.Sprintf("%d", view.performance.Gauges.PendingPeerBlocks)},
		dashboardKeyValueRow{Key: "Tx Relay Mode", Value: "erlay reconciliation"},
		dashboardKeyValueRow{Key: "Block Relay Mode", Value: "graphene planner"},
	))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("CANDIDATE BLOCK"))
	body.WriteString(renderCandidateBlockSection(view.template))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("BLOCK FLOW"))
	body.WriteString(renderBlockFlow(view.blocks))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader(view.tpsChart.Label))
	body.WriteString(renderTPSChart(view.tpsChart))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("FEE MARKET"))
	body.WriteString(renderFeeSection(view.fees))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("POW / DAA"))
	body.WriteString(renderPowSection(view.pow))
	body.WriteString("\n")

	if view.mining.Enabled {
		body.WriteString(renderSectionHeader("MINING"))
		body.WriteString(renderMiningSection(view.mining))
		body.WriteString("\n")
	}

	body.WriteString(renderSectionHeader("MEMPOOL"))
	body.WriteString(renderMempoolSection(view.mempool))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("PEER MESH"))
	body.WriteString(renderPeerMesh(view.peerHosts))
	body.WriteString("\n")

	body.WriteString(renderSectionHeader("PERFORMANCE"))
	body.WriteString(renderPerformanceSection(view.performance))
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
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "Hash", Value: fullHashString(block.Hash)},
		dashboardKeyValueRow{Key: "Prev", Value: shortHexBytes(block.PrevHash, 24)},
		dashboardKeyValueRow{Key: "Time", Value: block.Timestamp.Format("2006-01-02 15:04:05") + " UTC"},
		dashboardKeyValueRow{Key: "Size", Value: fmt.Sprintf("%s bytes", formatWithCommas(block.Size))},
		dashboardKeyValueRow{Key: "Tx Count", Value: fmt.Sprintf("%d", block.TxCount)},
		dashboardKeyValueRow{Key: "Fees", Value: fmt.Sprintf("median %s  low %s  high %s  total %s",
			formatAtoms(block.MedianFee), formatAtoms(block.LowFee), formatAtoms(block.HighFee), formatAtoms(block.TotalFees))},
		dashboardKeyValueRow{Key: "Pow Bits", Value: fmt.Sprintf("0x%08x", block.NBits)},
		dashboardKeyValueRow{Key: "Tx Root", Value: shortHexBytes(block.TxRoot, 24)},
		dashboardKeyValueRow{Key: "Auth Root", Value: shortHexBytes(block.AuthRoot, 24)},
		dashboardKeyValueRow{Key: "UTXO Root", Value: shortHexBytes(block.UTXORoot, 24)},
		dashboardKeyValueRow{Key: "Mined Here", Value: fmt.Sprintf("%t", block.MinedByNode)},
	))
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
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "TxID", Value: fullHashString(tx.TxID)},
		dashboardKeyValueRow{Key: "Block", Value: fmt.Sprintf("%d / %s", block.Height, shortHexBytes(block.Hash, 20))},
		dashboardKeyValueRow{Key: "Time", Value: tx.Timestamp.Format("2006-01-02 15:04:05") + " UTC"},
		dashboardKeyValueRow{Key: "Coinbase", Value: fmt.Sprintf("%t", tx.Coinbase)},
		dashboardKeyValueRow{Key: "Size", Value: fmt.Sprintf("%s bytes", formatWithCommas(tx.Size))},
		dashboardKeyValueRow{Key: "Auth Entries", Value: fmt.Sprintf("%d", tx.AuthCount)},
		dashboardKeyValueRow{Key: "Inputs", Value: fmt.Sprintf("%d (%s)", len(tx.Inputs), formatAtoms(tx.InputSum))},
		dashboardKeyValueRow{Key: "Outputs", Value: fmt.Sprintf("%d (%s)", len(tx.Outputs), formatAtoms(tx.OutputSum))},
		dashboardKeyValueRow{Key: "Fee", Value: fmt.Sprintf("%s (%s/byte)", formatAtoms(tx.Fee), formatAtoms(tx.FeeRate))},
	))
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("INPUTS"))
	body.WriteString(renderTxInputs(tx))
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("OUTPUTS"))
	body.WriteString(renderTxOutputs(tx))
	body.WriteString(renderHTMLEpilogue())
	return body.String()
}

func renderPublicPeerPage(view *publicDashboardView, host dashboardPeerHostPage) string {
	var body strings.Builder
	body.Grow(12288)
	body.WriteString(renderHTMLPrologue(fmt.Sprintf("Peer %s", host.Host)))
	body.WriteString(renderDashboardBannerCompact())
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("PEER DETAIL"))
	body.WriteString(" <a href=\"/\">[home]</a>\n")
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "peer", Value: host.Host},
		dashboardKeyValueRow{Key: "health", Value: host.Health},
		dashboardKeyValueRow{Key: "direction", Value: host.Direction},
		dashboardKeyValueRow{Key: "sockets", Value: fmt.Sprintf("%d", host.Sockets)},
		dashboardKeyValueRow{Key: "last seen", Value: formatDashboardAgeCompact(host.LastSeenUnix)},
		dashboardKeyValueRow{Key: "session age", Value: formatDashboardDuration(host.SessionAge)},
	))
	body.WriteString("\n")
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "blocks behind", Value: fmt.Sprintf("%d", host.BlocksBehind)},
		dashboardKeyValueRow{Key: "best height", Value: fmt.Sprintf("%d", host.BestHeight)},
		dashboardKeyValueRow{Key: "tip hash", Value: host.TipHash},
		dashboardKeyValueRow{Key: "last block", Value: formatDashboardAgeCompact(host.LastBlockUnix)},
	))
	body.WriteString("\n")
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "latency avg", Value: fmt.Sprintf("%.2f s", host.LatencyAvgS)},
		dashboardKeyValueRow{Key: "latency p95", Value: fmt.Sprintf("%.2f s", host.LatencyP95S)},
		dashboardKeyValueRow{Key: "outbound queue", Value: fmt.Sprintf("%d", host.OutboundQueue)},
	))
	body.WriteString("\n")
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "tx sent", Value: fmt.Sprintf("%d", host.TxSent)},
		dashboardKeyValueRow{Key: "tx requested", Value: fmt.Sprintf("%d", host.TxRequested)},
		dashboardKeyValueRow{Key: "blk sent", Value: fmt.Sprintf("%d", host.BlockSent)},
		dashboardKeyValueRow{Key: "blk requested", Value: fmt.Sprintf("%d", host.BlockRequested)},
	))
	body.WriteString("\n")
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "bytes in", Value: formatHumanBytes(host.BytesIn)},
		dashboardKeyValueRow{Key: "bytes out", Value: formatHumanBytes(host.BytesOut)},
	))
	body.WriteString("\n")
	body.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "reason", Value: host.Reason},
	))
	body.WriteString(renderHTMLEpilogue())
	return body.String()
}

func renderPublicFeePage(view *publicDashboardView) string {
	var body strings.Builder
	body.Grow(12288)
	body.WriteString(renderHTMLPrologue("Fee Market Details"))
	body.WriteString(renderDashboardBannerCompact())
	body.WriteString("\n")
	body.WriteString(renderSectionHeader("FEE MARKET DETAILS"))
	body.WriteString(" <a href=\"/\">[home]</a>\n")
	body.WriteString(renderFeeChartPage(view.fees))
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
	out.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "CPU Avg", Value: formatDashboardCPU(summary)},
		dashboardKeyValueRow{Key: "Network Up", Value: formatDashboardNetworkRate(summary.TxBytesPerSec, summary.HasNetwork)},
		dashboardKeyValueRow{Key: "Network Down", Value: formatDashboardNetworkRate(summary.RxBytesPerSec, summary.HasNetwork)},
		dashboardKeyValueRow{Key: "RAM Avg", Value: formatDashboardMemory(summary)},
		dashboardKeyValueRow{Key: "Load Avg", Value: formatDashboardLoad(summary)},
		dashboardKeyValueRow{Key: "Processes", Value: formatDashboardProcesses(summary)},
		dashboardKeyValueRow{Key: "CPU Cores", Value: formatDashboardCores(summary)},
	))
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
	const cardWidth = 17
	top := make([]string, 0, len(ordered))
	mid := make([]string, 0, len(ordered))
	hashes := make([]string, 0, len(ordered))
	cap := make([]string, 0, len(ordered))
	for i, block := range ordered {
		label := fmt.Sprintf("height %d", block.Height)
		if i == len(ordered)-1 {
			label = fmt.Sprintf("tip %d", block.Height)
		}
		hash := linkHashPadded("/block/", hex.EncodeToString(block.Hash[:]), 12, cardWidth)
		top = append(top, "."+strings.Repeat("-", cardWidth+2)+".")
		mid = append(mid, fmt.Sprintf("| %-*s |", cardWidth, label))
		hashes = append(hashes, fmt.Sprintf("| %s |", hash))
		cap = append(cap, "'"+strings.Repeat("-", cardWidth+2)+"'")
	}
	gap := "   "
	arrow := " ---> "
	if len(arrow) > len(gap) {
		gap = strings.Repeat(" ", len(arrow))
	}
	// Keep every row on the same column grid so hash-row arrows do not skew the cards.
	return " " + strings.Join(top, gap) + "\n" +
		" " + strings.Join(mid, gap) + "\n" +
		" " + strings.Join(hashes, arrow) + "\n" +
		" " + strings.Join(cap, gap) + "\n"
}

func renderTPSChart(chart dashboardTPSChart) string {
	if len(chart.Blocks) == 0 {
		return " no recent block throughput yet\n"
	}
	var out strings.Builder
	for _, block := range chart.Blocks {
		marker := " "
		if block.Candidate {
			marker = "*"
		}
		out.WriteString(fmt.Sprintf(" %3d tx%s |%s\n", block.TxCount, marker, strings.Repeat("█", block.BarWidth)))
	}
	out.WriteString("\n")
	out.WriteString(fmt.Sprintf(" %d tx total over last %d blocks (%.2f avg TPS)\n", chart.TotalTx, len(chart.Blocks), chart.AvgTPS))
	out.WriteString(fmt.Sprintf(" %.1f tx avg/block\n", chart.AvgTxPerBlk))
	for _, block := range chart.Blocks {
		if block.Candidate {
			out.WriteString(" * candidate block\n")
			break
		}
	}
	return out.String()
}

func renderFeeSection(summary dashboardFeeSummary) string {
	var out strings.Builder
	if len(summary.Recent) == 0 {
		return " no recent block fees yet\n"
	}
	out.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: fmt.Sprintf("%d-block medians", summary.TotalBlocks), Value: renderFeeSparkline(summary.Recent)},
		dashboardKeyValueRow{Key: "current / 6-block median", Value: fmt.Sprintf("%s / %s", formatAtomsPerKB(summary.CurrentMedian), formatAtomsPerKB(summary.Median6))},
		dashboardKeyValueRow{Key: "paid blocks", Value: fmt.Sprintf("%d / %d", summary.PaidBlocks, summary.TotalBlocks)},
	))
	out.WriteString("\n")
	out.WriteString(" <a href=\"/fees\">Full fee chart →</a>\n")
	return out.String()
}

func renderFeeChartPage(summary dashboardFeeSummary) string {
	var out strings.Builder
	if len(summary.Recent) == 0 {
		out.WriteString(" no recent block fees yet\n")
		return out.String()
	}
	out.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "6-block median", Value: formatAtomsPerKB(summary.Median6)},
		dashboardKeyValueRow{Key: "24-block median", Value: formatAtomsPerKB(summary.Median24)},
		dashboardKeyValueRow{Key: "recent min / max", Value: fmt.Sprintf("%s / %s atoms/kB", formatAtoms(summary.RecentMin), formatAtoms(summary.RecentMax))},
		dashboardKeyValueRow{Key: "paid blocks", Value: fmt.Sprintf("%d / %d", summary.PaidBlocks, summary.TotalBlocks)},
		dashboardKeyValueRow{Key: "fee-paying tx ratio", Value: formatDashboardPercent(summary.FeePayingTxs, summary.TotalTxs)},
		dashboardKeyValueRow{Key: "candidate block fee", Value: formatAtomsPerKB(summary.CurrentMedian)},
	))
	out.WriteString("\n")
	out.WriteString(fmt.Sprintf(" median fee by block, last %d (atoms/kB)\n\n", summary.TotalBlocks))
	out.WriteString(renderFeeHistogram(summary))
	out.WriteString("\n")
	out.WriteString(" recent blocks\n")
	out.WriteString(" height   median fee   paid tx   range\n")
	if summary.Candidate.Available {
		out.WriteString(fmt.Sprintf(" %-7s  %-11s  %-7d  %s -> %s\n",
			fmt.Sprintf("%d*", summary.Candidate.Height),
			formatAtoms(summary.Candidate.MedianFee),
			summary.Candidate.PaidTxs,
			formatAtoms(summary.Candidate.LowFee),
			formatAtoms(summary.Candidate.HighFee),
		))
	}
	remaining := 8
	if summary.Candidate.Available {
		remaining = 7
	}
	for i := len(summary.Recent) - 1; i >= 0 && remaining > 0; i-- {
		line := summary.Recent[i]
		out.WriteString(fmt.Sprintf(" %-7d  %-11s  %-7d  %s -> %s\n",
			line.Height,
			formatAtoms(line.MedianFee),
			line.PaidTxs,
			formatAtoms(line.LowFee),
			formatAtoms(line.HighFee),
		))
		remaining--
	}
	out.WriteString("\n")
	out.WriteString(" mempool by fee band\n")
	out.WriteString(fmt.Sprintf(" 1000+    : %d tx\n", summary.Bands.Above1000))
	out.WriteString(fmt.Sprintf(" 500-999  : %d tx\n", summary.Bands.Band500))
	out.WriteString(fmt.Sprintf(" 100-499  : %d tx\n", summary.Bands.Band100))
	out.WriteString(fmt.Sprintf(" 1-99     : %d tx\n", summary.Bands.Band1))
	out.WriteString(fmt.Sprintf(" 0        : %d tx\n", summary.Bands.Zero))
	if summary.Candidate.Available {
		out.WriteString("\n * current candidate block\n")
	}
	return out.String()
}

func renderPowSection(summary dashboardPowSummary) string {
	rows := []dashboardKeyValueRow{
		{Key: "Algo", Value: summary.Algorithm},
		{Key: "Target Space", Value: formatDashboardDuration(summary.TargetSpacing)},
		{Key: "Recent Gap", Value: "warming up"},
		{Key: "Difficulty", Value: fmt.Sprintf("%.4fx", summary.Difficulty)},
		{Key: "Net Hashrate", Value: formatHumanHashRate(summary.NetworkHashrate)},
		{Key: "Current Bits", Value: fmt.Sprintf("0x%08x", summary.CurrentBits)},
		{Key: "Next Bits", Value: fmt.Sprintf("0x%08x", summary.NextBits)},
	}
	if summary.HasObservedGap {
		rows[2].Value = formatDashboardDuration(summary.RecentBlockGap)
	}
	if !summary.LastBlockTimestamp.IsZero() {
		rows = append(rows, dashboardKeyValueRow{Key: "Last Block", Value: summary.LastBlockTimestamp.Format("2006-01-02 15:04:05") + " UTC"})
	}
	return renderDashboardKeyValues(rows...)
}

func renderMiningSection(summary dashboardMiningSummary) string {
	var out strings.Builder
	winShare := "warming up"
	if summary.RecentWindow > 0 {
		winShare = fmt.Sprintf("%d / %d recent blocks", summary.RecentWins, summary.RecentWindow)
	}
	out.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "Status", Value: "enabled"},
		dashboardKeyValueRow{Key: "Workers", Value: fmt.Sprintf("%d", summary.Workers)},
		dashboardKeyValueRow{Key: "Local Est", Value: formatHumanHashRate(summary.EstimatedHashrate)},
		dashboardKeyValueRow{Key: "Win Share", Value: winShare},
	))
	if len(summary.RecentHeights) == 0 {
		out.WriteString(" Recent Wins  : none in current on-screen block window\n")
		return out.String()
	}
	out.WriteString(" Recent Wins  :")
	for i := range summary.RecentHeights {
		out.WriteString(fmt.Sprintf(" %d", summary.RecentHeights[i]))
	}
	out.WriteString("\n")
	return out.String()
}

func renderPerformanceSection(summary PerformanceMetrics) string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf(" Generated    : %s UTC\n", summary.GeneratedAt.UTC().Format("2006-01-02 15:04:05")))
	out.WriteString(formatDashboardColumns("Throughput", [][2]string{
		{"admitted tx", formatWithCommas(summary.Counters.AdmittedTxs)},
		{"blocks", formatWithCommas(summary.Counters.BlocksAccepted)},
		{"orphans", formatWithCommas(summary.Counters.OrphanPromotions)},
	}))
	out.WriteString(formatDashboardColumns("Signatures", [][2]string{
		{"checks", formatWithCommas(summary.Counters.BlockSigChecks)},
		{"fallbacks", formatWithCommas(summary.Counters.BlockSigFallbacks)},
		{"mempool", formatWithCommas(summary.Gauges.MempoolTxs)},
	}))
	out.WriteString(formatDashboardColumns("Relay", [][2]string{
		{"tx items", formatWithCommas(summary.Counters.RelayedTxItems)},
		{"block items", formatWithCommas(summary.Counters.RelayedBlockItems)},
		{"stalls", formatWithCommas(summary.Counters.PeerStallEvents)},
	}))
	out.WriteString(formatDashboardColumns("Template", [][2]string{
		{"rebuilds", formatWithCommas(summary.Counters.TemplateRebuilds)},
		{"interrupts", formatWithCommas(summary.Counters.TemplateInterruptions)},
		{"frontier", formatWithCommas(summary.Gauges.CandidateFrontier)},
	}))
	out.WriteString(formatDashboardColumns("Peers", [][2]string{
		{"connected", formatWithCommas(summary.Gauges.PeerCount)},
		{"useful", formatWithCommas(summary.Gauges.UsefulPeers)},
		{"mempool", formatWithCommas(summary.Gauges.MempoolTxs)},
	}))
	out.WriteString(formatDashboardColumns("Queues", [][2]string{
		{"relay now", formatWithCommas(summary.Gauges.RelayQueueDepth)},
		{"relay peak", formatWithCommas(summary.Gauges.RelayQueueDepthPeak)},
		{"orphans", formatWithCommas(summary.Gauges.MempoolOrphans)},
	}))
	out.WriteString(formatDashboardColumns("Download", [][2]string{
		{"pending blk", formatWithCommas(summary.Gauges.PendingPeerBlocks)},
		{"inflight blk", formatWithCommas(summary.Gauges.InflightBlockReqs)},
		{"inflight tx", formatWithCommas(summary.Gauges.InflightTxReqs)},
	}))
	for _, line := range []struct {
		label string
		stats DurationHistogramSummary
	}{
		{label: " Admission", stats: summary.Latency.Admission},
		{label: " Template ", stats: summary.Latency.Template},
		{label: " Apply    ", stats: summary.Latency.BlockApply},
		{label: " Sig      ", stats: summary.Latency.BlockSigVerify},
		{label: " Relay    ", stats: summary.Latency.RelayFlush},
		{label: " Sync     ", stats: summary.Latency.SyncReq},
	} {
		out.WriteString(fmt.Sprintf(" %s : %s\n", line.label, formatLatencySummary(line.stats)))
	}
	return out.String()
}

func renderMempoolSection(summary dashboardMempoolSummary) string {
	var out strings.Builder
	out.WriteString(renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "Tx Count", Value: fmt.Sprintf("%d", summary.Count)},
		dashboardKeyValueRow{Key: "Total Size", Value: fmt.Sprintf("%s bytes", formatWithCommas(summary.Bytes))},
		dashboardKeyValueRow{Key: "Orphan Tx Count", Value: fmt.Sprintf("%d", summary.Orphans)},
		dashboardKeyValueRow{Key: "Fee Min / Median / Max (atoms/kB)", Value: fmt.Sprintf("%s / %s / %s",
			formatAtoms(summary.LowFee), formatAtoms(summary.MedianFee), formatAtoms(summary.HighFee))},
	))
	if len(summary.Top) == 0 {
		out.WriteString("\n")
		out.WriteString(" top queued tx by fee\n")
		out.WriteString(" empty\n")
		return out.String()
	}
	out.WriteString("\n")
	out.WriteString(" top queued tx by fee\n")
	for _, entry := range summary.Top {
		feeRate := uint64(0)
		if entry.Size > 0 {
			feeRate = (entry.Fee * 1000) / uint64(entry.Size)
		}
		out.WriteString(fmt.Sprintf(" %s   %s atoms/kB   %s\n",
			shortHexBytes(entry.TxID, 10), formatAtoms(feeRate), formatByteCountShort(entry.Size)))
	}
	if summary.Count > len(summary.Top) {
		out.WriteString(fmt.Sprintf(" ... %d more\n", summary.Count-len(summary.Top)))
	}
	return out.String()
}

func formatLatencySummary(summary DurationHistogramSummary) string {
	if summary.Count == 0 {
		return "no samples"
	}
	return fmt.Sprintf("count %-4d avg %6.2fms p50 %6.2fms p95 %6.2fms max %6.2fms",
		summary.Count,
		summary.AvgMS,
		summary.P50MS,
		summary.P95MS,
		summary.MaxMS,
	)
}

func formatDashboardColumns(label string, cols [][2]string) string {
	if len(cols) == 0 {
		return ""
	}
	label = strings.TrimSpace(label)
	parts := make([]string, 0, len(cols))
	for _, col := range cols {
		parts = append(parts, fmt.Sprintf("%-11s %8s", col[0], col[1]))
	}
	return fmt.Sprintf(" %-12s : %s\n", label, strings.Join(parts, "  "))
}

func renderCandidateBlockSection(stats BlockTemplateStats) string {
	return renderDashboardKeyValues(
		dashboardKeyValueRow{Key: "Status", Value: candidateBlockStatus(stats)},
		dashboardKeyValueRow{Key: "Age", Value: formatDashboardCompactDuration(time.Duration(stats.LastBuildAgeMS) * time.Millisecond)},
		dashboardKeyValueRow{Key: "Tx Candidate Txs", Value: formatWithCommas(stats.FrontierCandidates)},
		dashboardKeyValueRow{Key: "Rebuilds (1h)", Value: formatWithCommas(stats.Rebuilds)},
		dashboardKeyValueRow{Key: "Interrupts (1h)", Value: formatWithCommas(stats.Interruptions)},
		dashboardKeyValueRow{Key: "Invalidations (1h)", Value: formatWithCommas(stats.Invalidations)},
		dashboardKeyValueRow{Key: "Last Reason", Value: defaultDashboardValue(stats.LastReason, "-")},
	)
}

func candidateBlockStatus(stats BlockTemplateStats) string {
	switch strings.TrimSpace(strings.ToLower(stats.LastReason)) {
	case "interrupted":
		return "rebuilding"
	case "":
		if stats.Rebuilds == 0 && stats.CacheHits == 0 {
			return "warming up"
		}
	}
	return "ready"
}

func (s *Service) dashboardPeerHosts(now time.Time, info ServiceInfo) []dashboardPeerHostPage {
	peers := s.peerSnapshot()
	if len(peers) == 0 {
		return nil
	}
	hostSockets := make(map[string][]dashboardPeerSocketPage)
	for _, peer := range peers {
		relay := peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount())
		sync := peer.syncSnapshot()
		host := peerHostName(peer.addr)
		lag := uint64(0)
		if peerHeight := peer.snapshotHeight(); peerHeight < info.TipHeight {
			lag = info.TipHeight - peerHeight
		}
		socket := dashboardPeerSocketPage{
			Addr:           peer.addr,
			Port:           peerPort(peer.addr),
			Outbound:       peer.outbound,
			Height:         peer.snapshotHeight(),
			Lag:            lag,
			LastSeenUnix:   peer.snapshotProgressUnix(),
			LastUsefulUnix: sync.lastUsefulUnix(),
			UsefulBlocks:   sync.UsefulBlocks,
			SessionAge:     now.Sub(peer.connectedAt),
			QueueDepth:     relay.QueueDepth,
			LatencyAvgS:    relay.RelayAvgMS / 1000,
			LatencyP95S:    relay.RelayP95MS / 1000,
			TxSent:         relay.TxReconItems,
			TxRequested:    relay.TxReqItems,
			BlockSent:      relay.BlockSendItems,
			BlockRequested: relay.BlockReqItems,
			BytesIn:        peer.bytesIn(),
			BytesOut:       peer.bytesOut(),
		}
		hostSockets[host] = append(hostSockets[host], socket)
	}

	hosts := make([]dashboardPeerHostPage, 0, len(hostSockets))
	for host, sockets := range hostSockets {
		slices.SortFunc(sockets, func(a, b dashboardPeerSocketPage) int {
			if a.Outbound != b.Outbound {
				if a.Outbound {
					return -1
				}
				return 1
			}
			if a.Lag != b.Lag {
				if a.Lag < b.Lag {
					return -1
				}
				return 1
			}
			return strings.Compare(a.Addr, b.Addr)
		})
		hosts = append(hosts, summarizeDashboardPeerHost(now, host, info, sockets))
	}
	slices.SortFunc(hosts, func(a, b dashboardPeerHostPage) int {
		return strings.Compare(a.Host, b.Host)
	})
	return hosts
}

func summarizeDashboardPeerHost(now time.Time, host string, info ServiceInfo, sockets []dashboardPeerSocketPage) dashboardPeerHostPage {
	out := dashboardPeerHostPage{
		Host:        host,
		Sockets:     len(sockets),
		TipHash:     shortHexString(info.TipHeaderHash, 13),
		SocketPages: append([]dashboardPeerSocketPage(nil), sockets...),
	}
	hasInbound := false
	hasOutbound := false
	bestLag := ^uint64(0)
	worstLag := uint64(0)
	bestHeight := uint64(0)
	latestSeen := int64(0)
	latestBlock := int64(0)
	maxQueue := 0
	maxP95 := 0.0
	totalAvg := 0.0
	avgCount := 0
	txSentPrimary := pickDashboardPeerSocket(sockets, true)
	txReqPrimary := pickDashboardPeerSocket(sockets, false)
	for _, socket := range sockets {
		if socket.Outbound {
			hasOutbound = true
		} else {
			hasInbound = true
		}
		if socket.Lag < bestLag {
			bestLag = socket.Lag
		}
		if socket.Lag > worstLag {
			worstLag = socket.Lag
		}
		if socket.Height > bestHeight {
			bestHeight = socket.Height
		}
		if socket.LastSeenUnix > latestSeen {
			latestSeen = socket.LastSeenUnix
		}
		if socket.UsefulBlocks > 0 && socket.LastUsefulUnix > latestBlock {
			latestBlock = socket.LastUsefulUnix
		}
		if socket.SessionAge > out.SessionAge {
			out.SessionAge = socket.SessionAge
		}
		if socket.QueueDepth > maxQueue {
			maxQueue = socket.QueueDepth
		}
		if socket.LatencyP95S > maxP95 {
			maxP95 = socket.LatencyP95S
		}
		if socket.LatencyAvgS > 0 {
			totalAvg += socket.LatencyAvgS
			avgCount++
		}
		out.BytesIn += socket.BytesIn
		out.BytesOut += socket.BytesOut
	}
	if bestLag == ^uint64(0) {
		bestLag = 0
	}
	if avgCount > 0 {
		out.LatencyAvgS = totalAvg / float64(avgCount)
	}
	out.LatencyP95S = maxP95
	out.LastSeenUnix = latestSeen
	out.LastBlockUnix = latestBlock
	out.BestHeight = bestHeight
	out.BlocksBehind = bestLag
	out.OutboundQueue = maxQueue
	if hasInbound && hasOutbound {
		out.Direction = "in/out"
	} else if hasOutbound {
		out.Direction = "out"
	} else {
		out.Direction = "in"
	}
	if txSentPrimary != nil {
		out.TxSent = txSentPrimary.TxSent
		out.BlockSent = txSentPrimary.BlockSent
	}
	if txReqPrimary != nil {
		out.TxRequested = txReqPrimary.TxRequested
		out.BlockRequested = txReqPrimary.BlockRequested
	}
	if out.TxSent == 0 {
		for _, socket := range sockets {
			if socket.TxSent > out.TxSent {
				out.TxSent = socket.TxSent
			}
			if socket.BlockSent > out.BlockSent {
				out.BlockSent = socket.BlockSent
			}
		}
	}
	if out.TxRequested == 0 {
		for _, socket := range sockets {
			if socket.TxRequested > out.TxRequested {
				out.TxRequested = socket.TxRequested
			}
			if socket.BlockRequested > out.BlockRequested {
				out.BlockRequested = socket.BlockRequested
			}
		}
	}
	out.Health, out.Reason = classifyDashboardPeerHealth(now, latestSeen, bestLag, worstLag, maxQueue)
	return out
}

func classifyDashboardPeerHealth(now time.Time, lastSeenUnix int64, bestLag uint64, worstLag uint64, maxQueue int) (string, string) {
	const (
		busyQueueDepth = 16
		staleAfter     = 45 * time.Second
		warnAfter      = 10 * time.Second
		nearTipLag     = 2
		warnLag        = 32
		lagThreshold   = 128
	)
	if maxQueue >= busyQueueDepth {
		return "BUSY", "relay queue is overloaded"
	}
	if lastSeenUnix <= 0 || now.Sub(time.Unix(lastSeenUnix, 0)) > staleAfter {
		return "STALE", "recent peer activity is stale"
	}
	if bestLag <= nearTipLag && worstLag >= lagThreshold {
		return "MIXED", "one socket is current while another lags"
	}
	if bestLag >= lagThreshold {
		return "LAG", "materially behind tip"
	}
	if bestLag >= warnLag || worstLag >= lagThreshold || now.Sub(time.Unix(lastSeenUnix, 0)) > warnAfter {
		return "WARN", "slightly behind but active"
	}
	return "OK", "healthy and current"
}

func pickDashboardPeerSocket(sockets []dashboardPeerSocketPage, outbound bool) *dashboardPeerSocketPage {
	var best *dashboardPeerSocketPage
	for i := range sockets {
		socket := &sockets[i]
		if socket.Outbound != outbound {
			continue
		}
		if best == nil || socket.Lag < best.Lag || (socket.Lag == best.Lag && socket.LastSeenUnix > best.LastSeenUnix) {
			best = socket
		}
	}
	if best != nil {
		return best
	}
	for i := range sockets {
		socket := &sockets[i]
		if best == nil || socket.Lag < best.Lag || (socket.Lag == best.Lag && socket.LastSeenUnix > best.LastSeenUnix) {
			best = socket
		}
	}
	return best
}

func peerHostName(addr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		host = strings.TrimSpace(addr)
	}
	if parsed, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		return parsed.String()
	}
	return strings.Trim(host, "[]")
}

func peerPort(addr string) string {
	_, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return "-"
	}
	return port
}

func renderPeerMesh(hosts []dashboardPeerHostPage) string {
	if len(hosts) == 0 {
		return " no live peers\n"
	}
	var out strings.Builder
	socketCount := 0
	for _, host := range hosts {
		socketCount += host.Sockets
	}
	out.WriteString(fmt.Sprintf(" hosts=%d  sockets=%d\n\n", len(hosts), socketCount))
	out.WriteString(" peer            health  direction  latency (s) avg/p95  tx sent  tx requested  last seen\n")
	for _, host := range hosts {
		label := linkPathPadded("/peer/"+url.PathEscape(host.Host), host.Host, 15)
		out.WriteString(fmt.Sprintf(" %s  %-6s  %-9s  %5.2f / %-5.2f  %7d  %12d  %s\n",
			label,
			host.Health,
			host.Direction,
			host.LatencyAvgS,
			host.LatencyP95S,
			host.TxSent,
			host.TxRequested,
			formatDashboardAgeCompact(host.LastSeenUnix),
		))
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
		out.WriteString(fmt.Sprintf(" vout %-2d  amount %-10s  pubkey  %s\n",
			output.Index, formatAtoms(output.Amount), shortHexBytes(output.PubKey, 18)))
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

func linkPathPadded(path string, label string, width int) string {
	if width > len(label) {
		label = label + strings.Repeat(" ", width-len(label))
	}
	return fmt.Sprintf("<a href=\"%s\">%s</a>", path, label)
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

func blockMinedByPubKey(block *types.Block, pubKey [32]byte) bool {
	if len(block.Txs) == 0 || len(block.Txs[0].Base.Outputs) == 0 {
		return false
	}
	for _, output := range block.Txs[0].Base.Outputs {
		if output.PubKey == pubKey {
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

func shortUserAgent(ua string) string {
	ua = strings.TrimSpace(ua)
	if ua == "" {
		return "-"
	}
	if len(ua) <= 20 {
		return ua
	}
	return ua[:17] + "..."
}

func shortPeerAddr(addr string) string {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return "-"
	}
	if len(addr) <= 24 {
		return addr
	}
	return addr[:21] + "..."
}

func formatMaybeTimeAgo(unix int64, now time.Time) string {
	if unix <= 0 {
		return "never"
	}
	return now.Sub(time.Unix(unix, 0)).Round(time.Second).String()
}

func (s *Service) peerSyncDebugSummary(limit int) string {
	peers := s.snapshotPeers()
	if len(peers) == 0 {
		return "none"
	}
	now := time.Now()
	slices.SortFunc(peers, func(a, b *peerConn) int {
		return comparePeerAddrs(a.addr, b.addr)
	})
	var out strings.Builder
	for i, peer := range peers {
		if i >= limit {
			if out.Len() > 0 {
				out.WriteString(" | ")
			}
			out.WriteString(fmt.Sprintf("+%d more", len(peers)-limit))
			break
		}
		stats := peer.syncSnapshot()
		if i > 0 {
			out.WriteString(" | ")
		}
		out.WriteString(fmt.Sprintf("%s(out=%t h=%d last=%s hs=%d bs=%d txs=%d cool=%dms ua=%s)",
			shortPeerAddr(peer.addr),
			peer.outbound,
			peer.snapshotHeight(),
			formatMaybeTimeAgo(peer.snapshotProgressUnix(), now),
			stats.HeaderStalls,
			stats.BlockStalls,
			stats.TxStalls,
			stats.cooldownRemainingMS(now),
			shortUserAgent(peer.version.UserAgent),
		))
	}
	return out.String()
}

func (s *Service) SyncDebugSnapshot() SyncDebugSnapshot {
	return SyncDebugSnapshot{
		BlockHeight:    s.BlockHeight(),
		HeaderHeight:   s.HeaderHeight(),
		MempoolCount:   s.MempoolCount(),
		PeerSync:       s.peerSyncDebugSummary(4),
		InflightBlocks: s.inflightBlockDebugSummary(6),
		PendingBlocks:  s.pendingPeerBlockDebugSummary(6),
	}
}

func headersDebugSummary(headers []types.BlockHeader, limit int) string {
	if len(headers) == 0 {
		return "none"
	}
	hashes := make([][32]byte, 0, len(headers))
	for _, header := range headers {
		hashes = append(hashes, consensus.HeaderHash(&header))
	}
	return hashesDebugSummary(hashes, limit)
}

func hashesDebugSummary(hashes [][32]byte, limit int) string {
	if len(hashes) == 0 {
		return "none"
	}
	if limit <= 0 || limit > len(hashes) {
		limit = len(hashes)
	}
	parts := make([]string, 0, limit+1)
	for _, hash := range hashes[:limit] {
		parts = append(parts, shortHexBytes(hash, 16))
	}
	if len(hashes) > limit {
		parts = append(parts, fmt.Sprintf("+%d more", len(hashes)-limit))
	}
	return strings.Join(parts, ",")
}

func acceptedTxDebugSummary(accepted []mempool.AcceptedTx, limit int) string {
	if len(accepted) == 0 {
		return "none"
	}
	hashes := make([][32]byte, 0, len(accepted))
	for _, item := range accepted {
		hashes = append(hashes, item.TxID)
	}
	return hashesDebugSummary(hashes, limit)
}

func txDebugSummary(txs []types.Transaction, limit int) string {
	if len(txs) == 0 {
		return "none"
	}
	hashes := make([][32]byte, 0, len(txs))
	for _, tx := range txs {
		hashes = append(hashes, consensus.TxID(&tx))
	}
	return hashesDebugSummary(hashes, limit)
}

func invItemsDebugSummary(items []p2p.InvVector, filterType p2p.InvType, limit int) string {
	hashes := make([][32]byte, 0, len(items))
	for _, item := range items {
		if item.Type == filterType || (filterType == p2p.InvTypeBlockFull && item.Type == p2p.InvTypeBlock) {
			hashes = append(hashes, item.Hash)
		}
	}
	return hashesDebugSummary(hashes, limit)
}

func (s *Service) inflightBlockDebugSummary(limit int) string {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()
	if len(s.blockRequests) == 0 {
		return "none"
	}
	type item struct {
		hash [32]byte
		req  blockDownloadRequest
	}
	items := make([]item, 0, len(s.blockRequests))
	for hash, req := range s.blockRequests {
		items = append(items, item{hash: hash, req: req})
	}
	slices.SortFunc(items, func(a, b item) int {
		if a.req.requestedAt.Equal(b.req.requestedAt) {
			return bytes.Compare(a.hash[:], b.hash[:])
		}
		if a.req.requestedAt.Before(b.req.requestedAt) {
			return -1
		}
		return 1
	})
	now := time.Now()
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	parts := make([]string, 0, limit+1)
	for _, item := range items[:limit] {
		age := "pending"
		if !item.req.requestedAt.IsZero() {
			age = now.Sub(item.req.requestedAt).Round(time.Second).String()
		}
		parts = append(parts, fmt.Sprintf("%s->%s@%s(a=%d)",
			shortHexBytes(item.hash, 16),
			shortPeerAddr(item.req.peerAddr),
			age,
			item.req.attempts,
		))
	}
	if len(items) > limit {
		parts = append(parts, fmt.Sprintf("+%d more", len(items)-limit))
	}
	return strings.Join(parts, " | ")
}

func formatAtoms(value uint64) string {
	return formatUintWithCommas(value)
}

func formatAtomsPerKB(value uint64) string {
	return formatAtoms(value) + " atoms/kB"
}

func formatDashboardPercent(numerator, denominator int) string {
	if denominator <= 0 {
		return "0%"
	}
	return fmt.Sprintf("%d%%", int(math.Round((float64(numerator)/float64(denominator))*100)))
}

func formatByteCountShort(bytes int) string {
	if bytes < 0 {
		bytes = 0
	}
	return fmt.Sprintf("%s B", formatWithCommas(bytes))
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

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func formatDashboardAgeCompact(unix int64) string {
	if unix <= 0 {
		return "n/a"
	}
	return formatDashboardCompactDuration(time.Since(time.Unix(unix, 0))) + " ago"
}

func renderFeeSparkline(lines []dashboardBlockFeeLine) string {
	if len(lines) == 0 {
		return "n/a"
	}
	levels := []rune("▁▂▃▄▅▆▇█")
	maxMedian := uint64(0)
	for _, line := range lines {
		if line.MedianFee > maxMedian {
			maxMedian = line.MedianFee
		}
	}
	if maxMedian == 0 {
		return strings.Repeat(string(levels[0]), len(lines))
	}
	var out strings.Builder
	out.Grow(len(lines) * 3)
	// Compress recent fee medians into a fixed eight-level sparkline so the
	// home page keeps directional fee pressure without dumping a full table.
	for _, line := range lines {
		level := int(math.Round((float64(line.MedianFee) / float64(maxMedian)) * float64(len(levels)-1)))
		if level < 0 {
			level = 0
		}
		if level >= len(levels) {
			level = len(levels) - 1
		}
		out.WriteRune(levels[level])
	}
	return out.String()
}

func renderFeeHistogram(summary dashboardFeeSummary) string {
	if len(summary.Recent) == 0 {
		return ""
	}
	chartTop := roundUpFeeAxis(summary.RecentMax)
	cols := buildFeeHistogramColumns(summary.Recent, 0)
	var out strings.Builder
	// Keep the histogram on a strict fixed-width grid so each fee window reads
	// like an actual chart rather than a wrapped list of bars.
	for level := chartTop; level >= 0; level -= 100 {
		out.WriteString(fmt.Sprintf("%4d |%s\n", level, buildFeeHistogramColumns(summary.Recent, uint64(level))))
	}
	out.WriteString(fmt.Sprintf("     +%s\n", strings.Repeat("-", len(cols))))
	out.WriteString("      -24      -20      -16      -12      -8       -4 now\n")
	if summary.Candidate.Available {
		out.WriteString(fmt.Sprintf("      %s*\n", strings.Repeat(" ", maxInt(len(cols)-1, 0))))
	}
	return out.String()
}

func buildFeeHistogramColumns(lines []dashboardBlockFeeLine, level uint64) string {
	if len(lines) == 0 {
		return ""
	}
	cols := make([]string, 0, len(lines))
	for _, line := range lines {
		cell := " "
		if level == 0 || line.MedianFee >= level {
			cell = "█"
		}
		cols = append(cols, cell)
	}
	return strings.Join(cols, " ")
}

func roundUpFeeAxis(value uint64) int {
	if value <= 100 {
		return 100
	}
	rounded := int(((value + 99) / 100) * 100)
	if rounded < 100 {
		return 100
	}
	return rounded
}

func summarizeMempoolFeeBands(entries []mempool.SnapshotEntry) dashboardMempoolFeeBands {
	var bands dashboardMempoolFeeBands
	for _, entry := range entries {
		rate := uint64(0)
		if entry.Size > 0 {
			rate = (entry.Fee * 1000) / uint64(entry.Size)
		}
		switch {
		case rate >= 1000:
			bands.Above1000++
		case rate >= 500:
			bands.Band500++
		case rate >= 100:
			bands.Band100++
		case rate >= 1:
			bands.Band1++
		default:
			bands.Zero++
		}
	}
	return bands
}

func formatDashboardAgeFromTime(now time.Time, at time.Time) string {
	if at.IsZero() {
		return "n/a"
	}
	if now.IsZero() {
		now = time.Now()
	}
	if at.After(now) {
		at = now
	}
	return formatDashboardCompactDuration(now.Sub(at)) + " ago"
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

func formatDashboardCompactDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%02ds", int(d/time.Minute), int(d/time.Second)%60)
	}
	return fmt.Sprintf("%dh%02dm", int(d/time.Hour), int(d/time.Minute)%60)
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

func formatHumanHashRate(hashesPerSecond float64) string {
	if hashesPerSecond <= 0 {
		return "warming up"
	}
	units := []string{"H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s"}
	value := hashesPerSecond
	unit := units[0]
	for i := 1; i < len(units) && value >= 1000; i++ {
		value /= 1000
		unit = units[i]
	}
	if unit == "H/s" {
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
	if chars <= 3 {
		return raw[:chars]
	}
	return raw[:chars-3] + "..."
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
	services := p2p.ServiceNodeNetwork | p2p.ServiceErlayTxRelay | p2p.ServiceCompactBlockRelay | p2p.ServiceGrapheneExtended
	if s.avalancheManager().enabled() {
		services |= p2p.ServiceAvalancheOverlay
	}
	return p2p.VersionMessage{
		Protocol:  1,
		Services:  services,
		Height:    s.blockHeight(),
		Nonce:     randomNonce(),
		UserAgent: "bpu/go",
	}
}

func ParseMinerPubKey(raw string) ([32]byte, error) {
	var pubKey [32]byte
	if raw == "" {
		return pubKey, nil
	}
	raw = strings.TrimSpace(raw)
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return pubKey, err
	}
	if len(buf) != 32 {
		return pubKey, errors.New("miner pubkey must be 32 bytes hex")
	}
	copy(pubKey[:], buf)
	if !bpcrypto.IsValidXOnlyPubKey(&pubKey) {
		return pubKey, errors.New("miner pubkey must be a valid x-only secp256k1 public key")
	}
	return pubKey, nil
}
