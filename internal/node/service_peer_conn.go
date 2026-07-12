package node

import (
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

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
	thinWorkTokens  float64
	thinWorkRefill  time.Time
	blockRelayMu    sync.Mutex
	blockRelay      peerBlockRelayState
	blockServeMu    sync.Mutex
	blockServeBytes float64
	blockServeAt    time.Time
	blockTransferMu sync.Mutex
	blockTransfers  map[[32]byte]*incomingBlockTransfer
	erlayMu         sync.Mutex
	erlayState      peerErlayState
	telemetry       peerRelayTelemetry
	syncMu          sync.Mutex
	syncState       peerSyncState
	avaMu           sync.Mutex
	avaState        peerAvalancheState
}

type incomingBlockTransfer struct {
	header   types.BlockHeader
	total    uint64
	next     uint64
	checksum [32]byte
	file     *os.File
	updated  time.Time
}

func (p *peerConn) allowBlockServeBytes(size int, now time.Time, maxMessageBytes int) bool {
	if p == nil || size < 0 || maxMessageBytes <= 0 {
		return false
	}
	burst := float64(maxMessageBytes * 2)
	refillPerSecond := float64(maxMessageBytes) / 8
	p.blockServeMu.Lock()
	defer p.blockServeMu.Unlock()
	if p.blockServeAt.IsZero() {
		p.blockServeBytes = burst
		p.blockServeAt = now
	}
	if elapsed := now.Sub(p.blockServeAt).Seconds(); elapsed > 0 {
		p.blockServeBytes = math.Min(burst, p.blockServeBytes+elapsed*refillPerSecond)
		p.blockServeAt = now
	}
	if p.blockServeBytes < float64(size) {
		return false
	}
	p.blockServeBytes -= float64(size)
	return true
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
	blockRef   *outboundBlockReference
	enqueuedAt time.Time
	lane       relayQueueLane
	class      relayMessageClass
	invItems   []p2p.InvVector
}

type outboundBlockReference struct {
	item  p2p.InvVector
	batch *blockServeBatch
}

type blockServeBatch struct {
	mu        sync.Mutex
	remaining int64
}

func newBlockServeBatch(limit int64) *blockServeBatch {
	if limit < 0 {
		limit = 0
	}
	return &blockServeBatch{remaining: limit}
}

func (b *blockServeBatch) tryCharge(size int) bool {
	if b == nil || size < 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	charge := int64(size)
	if charge > b.remaining {
		return false
	}
	b.remaining -= charge
	return true
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

func (p *peerConn) enqueueBlockReference(item p2p.InvVector, batch *blockServeBatch) error {
	envelope := outboundMessage{
		blockRef:   &outboundBlockReference{item: item, batch: batch},
		enqueuedAt: time.Now(),
		lane:       relayQueueLanePriority,
		class:      relayMessageClass{blockSendItems: 1},
	}
	q := p.sendQ
	if p.relayPriorityQ != nil {
		q = p.relayPriorityQ
	} else {
		envelope.lane = relayQueueLaneSend
	}
	select {
	case <-p.closed:
		return io.EOF
	case q <- envelope:
		p.telemetry.noteEnqueue(p.queueDepths())
		return nil
	default:
		p.logRelayQueuePressure(slog.LevelWarn, "dropped requested block due to saturated relay queue", envelope.lane, envelope.class, 1)
		return errors.New("peer block response queue saturated")
	}
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
		p.clearIncomingBlockTransfers()
	})
}

func (p *peerConn) clearIncomingBlockTransfers() {
	p.blockTransferMu.Lock()
	defer p.blockTransferMu.Unlock()
	for hash, transfer := range p.blockTransfers {
		if transfer.file != nil {
			name := transfer.file.Name()
			_ = transfer.file.Close()
			_ = os.Remove(name)
		}
		delete(p.blockTransfers, hash)
	}
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
	for _, state := range p.pendingThin {
		if state != nil && state.releaseBudget != nil {
			state.releaseBudget()
		}
	}
	p.pendingThin = make(map[[32]byte]*pendingThinBlock)
	p.thinMu.Unlock()
}

func (s *Service) writePeerEnvelope(peer *peerConn, envelope outboundMessage) bool {
	var releaseBlockBudget func()
	if envelope.blockRef != nil {
		// Reserve the maximum source-block footprint before touching Pebble or the
		// recent-block cache. This keeps concurrent lazy loads globally bounded.
		release, ok := s.blockServeBudget.TryAcquire(s.blockServingLimit())
		if !ok {
			envelope.msg = p2p.NotFoundMessage{Items: []p2p.InvVector{envelope.blockRef.item}}
		} else {
			releaseBlockBudget = release
			msg, err := s.resolveQueuedBlockReference(peer, envelope.blockRef)
			if err != nil {
				releaseBlockBudget()
				s.logPeerWriteFailure(peer, envelope, err, "lazy block response resolution failed")
				return false
			}
			envelope.msg = msg
		}
	}
	if releaseBlockBudget != nil {
		defer releaseBlockBudget()
	}
	if s.cfg.StallTimeout > 0 {
		if err := peer.wire.SetWriteDeadline(time.Now().Add(s.cfg.StallTimeout)); err != nil {
			s.logPeerWriteFailure(peer, envelope, err, "peer write deadline setup failed")
			peer.releaseQueuedInv(envelope.invItems)
			peer.releaseRelayBatch(envelope.msg)
			peer.close()
			_ = peer.wire.Close()
			return false
		}
		defer func() {
			if err := peer.wire.SetWriteDeadline(time.Time{}); err != nil && s.logger != nil {
				s.logger.Debug("peer write deadline clear failed",
					slog.String("addr", peer.addr),
					slog.String("target_addr", peer.targetAddr),
					slog.Bool("outbound", peer.outbound),
					slog.Any("error", err),
				)
			}
		}()
	}
	if err := peer.wire.WriteMessage(envelope.msg); err != nil {
		s.logPeerWriteFailure(peer, envelope, err, "peer write failed")
		peer.releaseQueuedInv(envelope.invItems)
		peer.releaseRelayBatch(envelope.msg)
		peer.close()
		_ = peer.wire.Close()
		return false
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

func (s *Service) logPeerWriteFailure(peer *peerConn, envelope outboundMessage, err error, message string) {
	if s == nil || s.logger == nil || peer == nil {
		return
	}
	level := slog.LevelDebug
	if isTimeoutError(err) {
		level = slog.LevelWarn
	}
	depths := peer.queueDepths()
	attrs := []slog.Attr{
		slog.String("addr", peer.addr),
		slog.String("target_addr", peer.targetAddr),
		slog.Bool("outbound", peer.outbound),
		slog.String("type", fmt.Sprintf("%T", envelope.msg)),
		slog.String("lane", envelope.lane.String()),
		slog.Int("queue_depth", depths.total),
		slog.Int("control_queue_depth", depths.control),
		slog.Int("priority_queue_depth", depths.priority),
		slog.Int("send_queue_depth", depths.send),
		slog.Duration("stall_timeout", s.cfg.StallTimeout),
		slog.Any("error", err),
	}
	if !envelope.enqueuedAt.IsZero() {
		attrs = append(attrs, slog.Duration("enqueue_age", time.Since(envelope.enqueuedAt)))
	}
	attrs = append(attrs, envelope.class.logAttrs()...)
	s.logger.LogAttrs(context.Background(), level, message, attrs...)
}

func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
