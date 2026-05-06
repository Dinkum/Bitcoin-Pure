package node

import (
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

// syncManager owns sync work assignment, in-flight download tracking, and the
// watchdog that repairs or nudges stalled catch-up.
type syncManager struct {
	svc *Service

	pollMu             sync.Mutex
	lastIdleHeaderPoll time.Time
}

type syncRequestClass string

const (
	syncRequestHeaders syncRequestClass = "headers"
	syncRequestBlocks  syncRequestClass = "blocks"
	syncRequestTxs     syncRequestClass = "txs"
)

const idleHeaderPollInterval = 15 * time.Second

type peerSyncSnapshot struct {
	LastUsefulAt       time.Time
	CooldownUntil      time.Time
	UsefulHeaders      int
	UsefulBlocks       int
	UsefulTxs          int
	HeaderStalls       int
	BlockStalls        int
	TxStalls           int
	HeadersRequestedAt time.Time
}

func (s peerSyncSnapshot) lastUsefulUnix() int64 {
	if s.LastUsefulAt.IsZero() {
		return 0
	}
	return s.LastUsefulAt.Unix()
}

func (s peerSyncSnapshot) cooldownRemainingMS(now time.Time) int64 {
	if s.CooldownUntil.IsZero() || !s.CooldownUntil.After(now) {
		return 0
	}
	return s.CooldownUntil.Sub(now).Milliseconds()
}

func (s peerSyncSnapshot) downloadScore(now time.Time, localHeaderHeight uint64) int {
	score := s.UsefulBlocks*200 + s.UsefulHeaders*50 + s.UsefulTxs*5
	score -= s.HeaderStalls*160 + s.BlockStalls*200 + s.TxStalls*80
	if !s.LastUsefulAt.IsZero() {
		switch age := now.Sub(s.LastUsefulAt); {
		case age <= 15*time.Second:
			score += 120
		case age <= time.Minute:
			score += 40
		}
	}
	if !s.CooldownUntil.IsZero() && s.CooldownUntil.After(now) {
		score -= 500
	}
	return score + int(localHeaderHeight)
}

func (p *peerConn) syncSnapshot() peerSyncSnapshot {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	return peerSyncSnapshot{
		LastUsefulAt:       p.syncState.lastUsefulAt,
		CooldownUntil:      p.syncState.cooldownUntil,
		UsefulHeaders:      p.syncState.usefulHeaders,
		UsefulBlocks:       p.syncState.usefulBlocks,
		UsefulTxs:          p.syncState.usefulTxs,
		HeaderStalls:       p.syncState.headerStalls,
		BlockStalls:        p.syncState.blockStalls,
		TxStalls:           p.syncState.txStalls,
		HeadersRequestedAt: p.syncState.headersRequestedAt,
	}
}

func (p *peerConn) markHeadersRequested(at time.Time) {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	p.syncState.headersRequestedAt = at
}

func (p *peerConn) clearHeadersRequested() {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	p.syncState.headersRequestedAt = time.Time{}
}

func (p *peerConn) noteUsefulHeaders(count int, at time.Time) {
	if count <= 0 {
		p.clearHeadersRequested()
		return
	}
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	p.syncState.headersRequestedAt = time.Time{}
	p.syncState.lastUsefulAt = at
	p.syncState.cooldownUntil = time.Time{}
	p.syncState.usefulHeaders += count
	if p.syncState.headerStalls > 0 {
		p.syncState.headerStalls--
	}
}

func (p *peerConn) noteUsefulBlocks(count int, at time.Time) {
	if count <= 0 {
		return
	}
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	p.syncState.lastUsefulAt = at
	p.syncState.cooldownUntil = time.Time{}
	p.syncState.usefulBlocks += count
	if p.syncState.blockStalls > 0 {
		p.syncState.blockStalls--
	}
}

func (p *peerConn) noteUsefulTxs(count int, at time.Time) {
	if count <= 0 {
		return
	}
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	p.syncState.lastUsefulAt = at
	p.syncState.cooldownUntil = time.Time{}
	p.syncState.usefulTxs += count
	if p.syncState.txStalls > 0 {
		p.syncState.txStalls--
	}
}

func (p *peerConn) headerRequestTimedOut(now time.Time, timeout time.Duration) bool {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	return !p.syncState.headersRequestedAt.IsZero() && now.Sub(p.syncState.headersRequestedAt) >= timeout
}

func (p *peerConn) noteStall(class syncRequestClass, at time.Time) peerSyncSnapshot {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	var misses int
	switch class {
	case syncRequestHeaders:
		p.syncState.headersRequestedAt = time.Time{}
		p.syncState.headerStalls++
		misses = p.syncState.headerStalls
	case syncRequestBlocks:
		p.syncState.blockStalls++
		misses = p.syncState.blockStalls
	case syncRequestTxs:
		p.syncState.txStalls++
		// Transaction reconciliation stalls are noisy during large mempool waves.
		// Penalize the peer's download score, but do not put it into the global
		// header/block cooldown path; doing so delays urgent block catch-up behind
		// unrelated relay misses.
		misses = 0
	}
	cooldown := stallCooldownForMisses(misses)
	until := at.Add(cooldown)
	if until.After(p.syncState.cooldownUntil) {
		p.syncState.cooldownUntil = until
	}
	return peerSyncSnapshot{
		LastUsefulAt:       p.syncState.lastUsefulAt,
		CooldownUntil:      p.syncState.cooldownUntil,
		UsefulHeaders:      p.syncState.usefulHeaders,
		UsefulBlocks:       p.syncState.usefulBlocks,
		UsefulTxs:          p.syncState.usefulTxs,
		HeaderStalls:       p.syncState.headerStalls,
		BlockStalls:        p.syncState.blockStalls,
		TxStalls:           p.syncState.txStalls,
		HeadersRequestedAt: p.syncState.headersRequestedAt,
	}
}

func (p *peerConn) canServeDownloads(now time.Time) bool {
	stats := p.syncSnapshot()
	return stats.CooldownUntil.IsZero() || !stats.CooldownUntil.After(now)
}

func (m *syncManager) requestSync(peer *peerConn) {
	m.svc.logger.Debug("nudging peer sync",
		slog.String("addr", peer.addr),
		slog.Uint64("tip_height", m.svc.blockHeight()),
		slog.Uint64("header_height", m.svc.headerHeight()),
		slog.Uint64("peer_best_height", peer.snapshotHeight()),
	)
	if !m.svc.cfg.StaticPeerTopology {
		_ = peer.send(p2p.GetAddrMessage{})
	}
	_ = m.requestHeaders(peer, [32]byte{})
}

func (m *syncManager) shouldPollIdleHeaders(now time.Time) bool {
	m.pollMu.Lock()
	defer m.pollMu.Unlock()
	if !m.lastIdleHeaderPoll.IsZero() && now.Sub(m.lastIdleHeaderPoll) < idleHeaderPollInterval {
		return false
	}
	m.lastIdleHeaderPoll = now
	return true
}

func (m *syncManager) requestHeaders(peer *peerConn, stopHash [32]byte) error {
	startedAt := time.Now()
	locator := m.svc.blockLocator()
	if err := peer.send(p2p.GetHeadersMessage{Locator: locator, StopHash: stopHash}); err != nil {
		return err
	}
	peer.markHeadersRequested(time.Now())
	stop := "-"
	if stopHash != ([32]byte{}) {
		stop = shortHexBytes(stopHash, 16)
	}
	m.svc.logger.Debug("requested headers",
		slog.String("addr", peer.addr),
		slog.Int("locator_count", len(locator)),
		slog.String("locator_head", hashesDebugSummary(locator, 3)),
		slog.String("stop_hash", stop),
		slog.Uint64("tip_height", m.svc.blockHeight()),
		slog.Uint64("header_height", m.svc.headerHeight()),
		slog.Uint64("peer_best_height", peer.snapshotHeight()),
		slog.Duration("request_duration", time.Since(startedAt)),
	)
	m.svc.perf.noteSyncRequestDuration(time.Since(startedAt))
	return nil
}

func (m *syncManager) requestBlocks(peer *peerConn) error {
	startedAt := time.Now()
	hashes, gapDetected, err := m.svc.missingBlockHashesDetailed(blockRequestBatchSize)
	if err != nil {
		return err
	}
	if gapDetected {
		m.svc.logger.Debug("active header height index gap detected while requesting blocks; continuing with canonical header ancestry",
			slog.String("addr", peer.addr),
			slog.Uint64("tip_height", m.svc.blockHeight()),
			slog.Uint64("header_height", m.svc.headerHeight()),
			slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
			slog.String("inflight_blocks", m.svc.inflightBlockDebugSummary(6)),
		)
	}
	hashes = m.scheduleBlockRequests(peer.addr, hashes, blockRequestBatchSize)
	if len(hashes) == 0 {
		if m.svc.headerHeight() > m.svc.blockHeight() {
			m.svc.logger.Warn("header tip ahead but no missing block requests were generated",
				slog.String("addr", peer.addr),
				slog.Uint64("tip_height", m.svc.blockHeight()),
				slog.Uint64("header_height", m.svc.headerHeight()),
				slog.Bool("index_gap", gapDetected),
				slog.Int("inflight_blocks", m.inflightBlockRequestCount()),
				slog.String("inflight_detail", m.svc.inflightBlockDebugSummary(6)),
				slog.Int("pending_blocks", m.svc.pendingPeerBlockCount()),
				slog.String("pending_detail", m.svc.pendingPeerBlockDebugSummary(6)),
				slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
				slog.Duration("request_duration", time.Since(startedAt)),
			)
		}
		m.svc.perf.noteSyncRequestDuration(time.Since(startedAt))
		return nil
	}
	items := make([]p2p.InvVector, 0, len(hashes))
	for _, hash := range hashes {
		// Explicit sync recovery prefers full blocks. Compact/short-ID relay is
		// still useful for steady-state propagation, but catch-up should bias
		// toward deterministic delivery over relay efficiency.
		items = append(items, p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: hash})
	}
	if err := peer.send(p2p.GetDataMessage{Items: items}); err != nil {
		for _, hash := range hashes {
			m.releaseBlockRequest(hash)
		}
		return err
	}
	m.svc.logger.Debug("requesting missing blocks",
		slog.String("addr", peer.addr),
		slog.Int("count", len(hashes)),
		slog.String("first_hash", shortHexBytes(hashes[0], 16)),
		slog.String("last_hash", shortHexBytes(hashes[len(hashes)-1], 16)),
		slog.String("hashes", hashesDebugSummary(hashes, 6)),
		slog.Uint64("tip_height", m.svc.blockHeight()),
		slog.Uint64("header_height", m.svc.headerHeight()),
		slog.String("inflight_before", m.svc.inflightBlockDebugSummary(6)),
		slog.Duration("request_duration", time.Since(startedAt)),
	)
	m.svc.perf.noteSyncRequestDuration(time.Since(startedAt))
	return nil
}

func (m *syncManager) blockRequestTimeout() time.Duration {
	timeout := m.syncStallThreshold()
	if timeout < 15*time.Second {
		timeout = 15 * time.Second
	}
	return timeout
}

func (m *syncManager) txRequestTimeout() time.Duration {
	timeout := m.syncStallThreshold()
	if timeout < 10*time.Second {
		timeout = 10 * time.Second
	}
	return timeout
}

func (m *syncManager) scheduleBlockRequests(peerAddr string, hashes [][32]byte, limit int) [][32]byte {
	startedAt := time.Now()
	if len(hashes) == 0 || limit <= 0 {
		return nil
	}
	now := time.Now()
	timeout := m.blockRequestTimeout()
	selected := make([][32]byte, 0, min(limit, len(hashes)))
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	for _, hash := range hashes {
		if len(selected) >= limit {
			break
		}
		if _, rejected := m.svc.rejectedBlocks[hash]; rejected {
			continue
		}
		req, ok := m.svc.blockRequests[hash]
		if ok && now.Sub(req.requestedAt) < timeout && req.peerAddr != "" && req.peerAddr != peerAddr {
			continue
		}
		if ok && now.Sub(req.requestedAt) < timeout && req.peerAddr == peerAddr {
			continue
		}
		req.peerAddr = peerAddr
		req.requestedAt = now
		req.attempts++
		m.svc.blockRequests[hash] = req
		selected = append(selected, hash)
	}
	if len(selected) > 0 && m.svc.logger != nil {
		m.svc.logger.Debug("scheduled block requests",
			slog.String("addr", peerAddr),
			slog.Int("selected", len(selected)),
			slog.Int("candidates", len(hashes)),
			slog.Duration("request_duration", time.Since(startedAt)),
		)
	}
	if len(selected) > 0 {
		m.svc.perf.noteSyncRequestDuration(time.Since(startedAt))
	}
	return selected
}

func (m *syncManager) releaseBlockRequest(hash [32]byte) {
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	delete(m.svc.blockRequests, hash)
}

func (m *syncManager) releasePeerBlockRequests(addr string) {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return
	}
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	for hash, req := range m.svc.blockRequests {
		if req.peerAddr == addr {
			delete(m.svc.blockRequests, hash)
		}
	}
}

func (m *syncManager) scheduleTxInvRequests(peerAddr string, items []p2p.InvVector, limit int) []p2p.InvVector {
	startedAt := time.Now()
	if len(items) == 0 || limit <= 0 {
		return nil
	}
	now := time.Now()
	timeout := m.txRequestTimeout()
	selected := make([]p2p.InvVector, 0, min(limit, len(items)))
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	if m.svc.txRequests == nil {
		m.svc.txRequests = make(map[[32]byte]blockDownloadRequest)
	}
	for _, item := range items {
		if len(selected) >= limit {
			break
		}
		req, ok := m.svc.txRequests[item.Hash]
		if ok && now.Sub(req.requestedAt) < timeout && req.peerAddr != "" && req.peerAddr != peerAddr {
			continue
		}
		if ok && now.Sub(req.requestedAt) < timeout && req.peerAddr == peerAddr {
			continue
		}
		req.peerAddr = peerAddr
		req.requestedAt = now
		req.attempts++
		m.svc.txRequests[item.Hash] = req
		selected = append(selected, item)
	}
	if len(selected) > 0 && m.svc.logger != nil {
		m.svc.logger.Debug("scheduled tx inventory requests",
			slog.String("addr", peerAddr),
			slog.Int("selected", len(selected)),
			slog.Int("candidates", len(items)),
			slog.Duration("request_duration", time.Since(startedAt)),
		)
	}
	if len(selected) > 0 {
		m.svc.perf.noteSyncRequestDuration(time.Since(startedAt))
	}
	return selected
}

// scheduleTxReconRequests reuses the same in-flight request ownership tracking
// as inv/getdata so Erlay announcements do not fan out duplicate TxRequest
// traffic across multiple peers while a missing tx is already being fetched.
func (m *syncManager) scheduleTxReconRequests(peerAddr string, txids [][32]byte, limit int) [][32]byte {
	startedAt := time.Now()
	if len(txids) == 0 || limit <= 0 {
		return nil
	}
	now := time.Now()
	timeout := m.txRequestTimeout()
	// Erlay overlap happens in tight bursts, so dedupe duplicate TxRequest
	// ownership only across a short grace window instead of monopolizing the
	// tx for the full sync stall timeout.
	if timeout > 500*time.Millisecond {
		timeout = 500 * time.Millisecond
	}
	selected := make([][32]byte, 0, min(limit, len(txids)))
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	if m.svc.txRequests == nil {
		m.svc.txRequests = make(map[[32]byte]blockDownloadRequest)
	}
	for _, txid := range txids {
		if len(selected) >= limit {
			break
		}
		req, ok := m.svc.txRequests[txid]
		if ok && now.Sub(req.requestedAt) < timeout && req.peerAddr != "" && req.peerAddr != peerAddr {
			continue
		}
		if ok && now.Sub(req.requestedAt) < timeout && req.peerAddr == peerAddr {
			continue
		}
		req.peerAddr = peerAddr
		req.requestedAt = now
		req.attempts++
		m.svc.txRequests[txid] = req
		selected = append(selected, txid)
	}
	if len(selected) > 0 && m.svc.logger != nil {
		m.svc.logger.Debug("scheduled tx reconciliation requests",
			slog.String("addr", peerAddr),
			slog.Int("selected", len(selected)),
			slog.Int("candidates", len(txids)),
			slog.Duration("request_duration", time.Since(startedAt)),
		)
	}
	if len(selected) > 0 {
		m.svc.perf.noteSyncRequestDuration(time.Since(startedAt))
	}
	return selected
}

func (m *syncManager) releaseTxRequest(hash [32]byte) {
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	delete(m.svc.txRequests, hash)
}

func (m *syncManager) releaseTxRequestsForTransactions(txs []types.Transaction) {
	if len(txs) == 0 {
		return
	}
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	for _, tx := range txs {
		delete(m.svc.txRequests, consensus.TxID(&tx))
	}
}

func (m *syncManager) releasePeerTxRequests(addr string) {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return
	}
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	for hash, req := range m.svc.txRequests {
		if req.peerAddr == addr {
			delete(m.svc.txRequests, hash)
		}
	}
}

func (m *syncManager) expireStaleBlockRequests() {
	timeout := m.blockRequestTimeout()
	now := time.Now()
	expired := 0
	expiredByPeer := make(map[string]int)
	expiredHashes := make([][32]byte, 0)
	m.svc.downloadMu.Lock()
	for hash, req := range m.svc.blockRequests {
		if now.Sub(req.requestedAt) < timeout {
			continue
		}
		delete(m.svc.blockRequests, hash)
		expired++
		expiredHashes = append(expiredHashes, hash)
		if req.peerAddr != "" {
			expiredByPeer[req.peerAddr]++
		}
	}
	m.svc.downloadMu.Unlock()
	for addr, count := range expiredByPeer {
		m.recordPeerStall(addr, syncRequestBlocks, count, timeout)
	}
	if expired > 0 {
		m.svc.logger.Warn("expired stale in-flight block requests",
			slog.Int("count", expired),
			slog.Duration("timeout", timeout),
			slog.String("hashes", hashesDebugSummary(expiredHashes, 6)),
			slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
		)
	}
}

func (m *syncManager) expireStaleTxRequests() {
	timeout := m.txRequestTimeout()
	now := time.Now()
	expired := 0
	expiredByPeer := make(map[string]int)
	m.svc.downloadMu.Lock()
	for hash, req := range m.svc.txRequests {
		if now.Sub(req.requestedAt) < timeout {
			continue
		}
		delete(m.svc.txRequests, hash)
		expired++
		if req.peerAddr != "" {
			expiredByPeer[req.peerAddr]++
		}
	}
	m.svc.downloadMu.Unlock()
	for addr, count := range expiredByPeer {
		m.recordPeerStall(addr, syncRequestTxs, count, timeout)
	}
	if expired > 0 {
		m.svc.logger.Warn("expired stale in-flight tx requests",
			slog.Int("count", expired),
			slog.Duration("timeout", timeout),
		)
	}
}

func (m *syncManager) inflightBlockRequestCount() int {
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	return len(m.svc.blockRequests)
}

func (m *syncManager) inflightTxRequestCount() int {
	m.svc.downloadMu.Lock()
	defer m.svc.downloadMu.Unlock()
	return len(m.svc.txRequests)
}

func (m *syncManager) syncWatchdogLoop() {
	ticker := time.NewTicker(syncWatchdogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.svc.stopCh:
			return
		case <-ticker.C:
			m.runSyncWatchdogStep()
		}
	}
}

func (m *syncManager) runSyncWatchdogStep() {
	m.expireStaleBlockRequests()
	m.expireStaleTxRequests()
	blockHeight := m.svc.blockHeight()
	headerHeight := m.svc.headerHeight()
	if headerHeight <= blockHeight {
		peers := m.preferredDownloadPeers(2)
		if len(peers) == 0 {
			peers = m.svc.peerSnapshot()
		}
		now := time.Now()
		if len(peers) == 0 || !m.shouldPollIdleHeaders(now) {
			return
		}
		m.svc.logger.Debug("sync watchdog polling headers while tip appears current",
			slog.Uint64("tip_height", blockHeight),
			slog.Int("peers", len(peers)),
		)
		for _, peer := range peers {
			m.requestSync(peer)
		}
		return
	}

	hashes, gapDetected, err := m.svc.missingBlockHashesDetailed(128)
	if err != nil {
		m.svc.logger.Warn("sync watchdog failed to inspect missing blocks", slog.Any("error", err))
		return
	}
	if len(hashes) == 0 && m.svc.pendingPeerBlockCount() > 0 {
		if tip := m.svc.chainState.ChainState().TipHeader(); tip != nil {
			m.svc.logger.Warn("sync watchdog retrying queued competing-branch blocks",
				slog.Uint64("tip_height", blockHeight),
				slog.Uint64("header_height", headerHeight),
				slog.Int("pending_blocks", m.svc.pendingPeerBlockCount()),
				slog.String("pending_detail", m.svc.pendingPeerBlockDebugSummary(6)),
			)
			m.svc.drainPendingPeerBlocks(consensus.HeaderHash(tip))
			hashes, gapDetected, err = m.svc.missingBlockHashesDetailed(128)
			if err != nil {
				m.svc.logger.Warn("sync watchdog failed to re-scan missing blocks after queued-block drain", slog.Any("error", err))
				return
			}
		}
	}
	if gapDetected && len(hashes) == 0 {
		repaired, repairErr := m.svc.repairActiveHeightIndex()
		if repairErr != nil {
			m.svc.logger.Warn("sync watchdog failed to repair active header height index",
				slog.Any("error", repairErr),
				slog.Uint64("tip_height", blockHeight),
				slog.Uint64("header_height", headerHeight),
			)
		} else if repaired > 0 {
			m.svc.logger.Warn("sync watchdog repaired active header height index",
				slog.Int("entries", repaired),
				slog.Uint64("tip_height", blockHeight),
				slog.Uint64("header_height", headerHeight),
			)
			hashes, _, err = m.svc.missingBlockHashesDetailed(128)
			if err != nil {
				m.svc.logger.Warn("sync watchdog failed to re-scan missing blocks after repair", slog.Any("error", err))
				return
			}
		}
	} else if gapDetected {
		m.svc.logger.Debug("sync watchdog observed active header height index gap; canonical block requests already cover the active branch",
			slog.Uint64("tip_height", blockHeight),
			slog.Uint64("header_height", headerHeight),
			slog.Int("missing_blocks", len(hashes)),
		)
	}

	peers := m.svc.peerSnapshot()
	if len(peers) == 0 {
		m.svc.logger.Warn("sync watchdog found no peers while block state lags headers",
			slog.Uint64("tip_height", blockHeight),
			slog.Uint64("header_height", headerHeight),
			slog.Int("missing_blocks", len(hashes)),
			slog.String("missing_hashes", hashesDebugSummary(hashes, 6)),
			slog.String("inflight_blocks", m.svc.inflightBlockDebugSummary(6)),
			slog.Any("known_peers", m.svc.knownPeerAddrs()),
		)
		m.svc.restartKnownPeers()
		return
	}

	needsKick := len(hashes) > 0
	now := time.Now()
	stallCutoff := now.Add(-m.syncStallThreshold())
	for _, peer := range peers {
		if peer.headerRequestTimedOut(now, m.syncStallThreshold()) {
			m.recordPeerStall(peer.addr, syncRequestHeaders, 1, m.syncStallThreshold())
			needsKick = true
			continue
		}
		if progressUnix := peer.snapshotProgressUnix(); progressUnix == 0 || time.Unix(progressUnix, 0).Before(stallCutoff) {
			needsKick = true
			continue
		}
	}
	if !needsKick {
		return
	}

	preferred := m.preferredDownloadPeers(2)
	if len(preferred) > 0 {
		peers = preferred
	}

	m.svc.logger.Warn("sync watchdog nudging stalled header catch-up",
		slog.Uint64("tip_height", blockHeight),
		slog.Uint64("header_height", headerHeight),
		slog.Int("missing_blocks", len(hashes)),
		slog.Int("peers", len(peers)),
		slog.String("missing_hashes", hashesDebugSummary(hashes, 6)),
		slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
		slog.String("inflight_blocks", m.svc.inflightBlockDebugSummary(6)),
	)
	for _, peer := range peers {
		m.requestSync(peer)
		if err := m.requestBlocks(peer); err != nil {
			m.svc.logger.Warn("sync watchdog block request failed", slog.String("addr", peer.addr), slog.Any("error", err))
		}
	}
}

func (m *syncManager) syncStallThreshold() time.Duration {
	if m.svc.cfg.StallTimeout > 0 {
		return m.svc.cfg.StallTimeout * 2
	}
	return 30 * time.Second
}

func (m *syncManager) onNotFoundMessage(peer *peerConn, msg p2p.NotFoundMessage) error {
	releasedBlocks := 0
	releasedTxs := 0
	for _, item := range msg.Items {
		switch item.Type {
		case p2p.InvTypeBlock, p2p.InvTypeBlockFull:
			m.releaseBlockRequest(item.Hash)
			releasedBlocks++
		case p2p.InvTypeTx:
			m.releaseTxRequest(item.Hash)
			releasedTxs++
		}
	}
	if releasedBlocks > 0 {
		m.recordPeerStall(peer.addr, syncRequestBlocks, releasedBlocks, m.blockRequestTimeout())
		m.svc.logger.Warn("peer reported missing blocks during catch-up",
			slog.String("addr", peer.addr),
			slog.Int("released_requests", releasedBlocks),
			slog.String("items", invItemsDebugSummary(msg.Items, p2p.InvTypeBlockFull, 6)),
			slog.Uint64("tip_height", m.svc.blockHeight()),
			slog.Uint64("header_height", m.svc.headerHeight()),
			slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
		)
		return m.requestBlocks(peer)
	}
	if releasedTxs > 0 {
		m.recordPeerStall(peer.addr, syncRequestTxs, releasedTxs, m.txRequestTimeout())
		m.svc.logger.Warn("peer reported missing transactions during tx catch-up",
			slog.String("addr", peer.addr),
			slog.Int("released_requests", releasedTxs),
			slog.String("items", invItemsDebugSummary(msg.Items, p2p.InvTypeTx, 6)),
			slog.Int("inflight_txs", m.inflightTxRequestCount()),
			slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
		)
	}
	return nil
}

func (m *syncManager) preferredDownloadPeers(limit int) []*peerConn {
	if limit <= 0 {
		return nil
	}
	peers := m.svc.peerSnapshot()
	if len(peers) == 0 {
		return nil
	}
	now := time.Now()
	eligible := make([]*peerConn, 0, len(peers))
	for _, peer := range peers {
		if peer.canServeDownloads(now) {
			eligible = append(eligible, peer)
		}
	}
	if len(eligible) == 0 {
		eligible = peers
	}
	headerHeight := m.svc.headerHeight()
	slices.SortFunc(eligible, func(a, b *peerConn) int {
		scoreA := m.downloadPeerScore(a, now, headerHeight)
		scoreB := m.downloadPeerScore(b, now, headerHeight)
		if scoreA != scoreB {
			if scoreA > scoreB {
				return -1
			}
			return 1
		}
		heightA := a.snapshotHeight()
		heightB := b.snapshotHeight()
		if heightA != heightB {
			if heightA > heightB {
				return -1
			}
			return 1
		}
		return comparePeerAddrs(a.addr, b.addr)
	})
	if limit > len(eligible) {
		limit = len(eligible)
	}
	return eligible[:limit]
}

func (m *syncManager) preferredDownloadPeerAddrsLocked(now time.Time, localHeaderHeight uint64) map[string]bool {
	peers := make([]*peerConn, 0, len(m.svc.peers))
	for _, peer := range m.svc.peers {
		peers = append(peers, peer)
	}
	if len(peers) == 0 {
		return nil
	}
	eligible := make([]*peerConn, 0, len(peers))
	for _, peer := range peers {
		if peer.canServeDownloads(now) {
			eligible = append(eligible, peer)
		}
	}
	if len(eligible) == 0 {
		eligible = peers
	}
	slices.SortFunc(eligible, func(a, b *peerConn) int {
		scoreA := m.downloadPeerScore(a, now, localHeaderHeight)
		scoreB := m.downloadPeerScore(b, now, localHeaderHeight)
		if scoreA != scoreB {
			if scoreA > scoreB {
				return -1
			}
			return 1
		}
		return comparePeerAddrs(a.addr, b.addr)
	})
	preferred := make(map[string]bool, min(2, len(eligible)))
	for _, peer := range eligible[:min(2, len(eligible))] {
		preferred[peer.addr] = true
	}
	return preferred
}

func (m *syncManager) downloadPeerScore(peer *peerConn, now time.Time, localHeaderHeight uint64) int {
	score := peer.syncSnapshot().downloadScore(now, localHeaderHeight)
	heightAdv := int(peer.snapshotHeight()) - int(localHeaderHeight)
	if heightAdv > 0 {
		score += heightAdv * 20
	}
	return score
}

func (m *syncManager) recordPeerStall(addr string, class syncRequestClass, count int, timeout time.Duration) {
	if count <= 0 {
		return
	}
	peer := m.svc.peerByAddr(addr)
	if peer == nil {
		return
	}
	stats := peer.noteStall(class, time.Now())
	m.svc.logger.Warn("demoting stalled sync peer",
		slog.String("addr", peer.addr),
		slog.String("class", string(class)),
		slog.Int("stalled_requests", count),
		slog.Duration("timeout", timeout),
		slog.Int("header_stalls", stats.HeaderStalls),
		slog.Int("block_stalls", stats.BlockStalls),
		slog.Int("tx_stalls", stats.TxStalls),
		slog.Duration("cooldown", time.Until(stats.CooldownUntil)),
		slog.Uint64("peer_best_height", peer.snapshotHeight()),
		slog.Uint64("tip_height", m.svc.blockHeight()),
		slog.Uint64("header_height", m.svc.headerHeight()),
		slog.String("peer_sync", m.svc.peerSyncDebugSummary(4)),
		slog.String("inflight_blocks", m.svc.inflightBlockDebugSummary(6)),
	)
}

func stallCooldownForMisses(misses int) time.Duration {
	if misses <= 0 {
		return 0
	}
	cooldown := 5 * time.Second
	for i := 1; i < misses; i++ {
		cooldown *= 2
		if cooldown >= time.Minute {
			return time.Minute
		}
	}
	if cooldown > time.Minute {
		return time.Minute
	}
	return cooldown
}

func comparePeerAddrs(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func (m *syncManager) repairActiveHeightIndex() (int, error) {
	m.svc.stateMu.Lock()
	defer m.svc.stateMu.Unlock()
	return m.repairActiveHeightIndexLocked()
}

func (m *syncManager) repairActiveHeightIndexLocked() (int, error) {
	tipHeader := m.svc.headerChain.TipHeader()
	if tipHeader == nil {
		return 0, nil
	}
	tipHash := consensus.HeaderHash(tipHeader)
	tipEntry, err := m.svc.chainState.Store().GetBlockIndex(&tipHash)
	if err != nil {
		return 0, err
	}
	if tipEntry == nil {
		return 0, fmt.Errorf("missing active header tip entry %x", tipHash)
	}
	entries := make([]storage.BlockIndexEntry, 0, tipEntry.Height+1)
	cursor := tipEntry
	for {
		entries = append(entries, *cursor)
		if cursor.Height == 0 {
			break
		}
		parent, err := m.svc.chainState.Store().GetBlockIndex(&cursor.ParentHash)
		if err != nil {
			return 0, err
		}
		if parent == nil {
			return 0, fmt.Errorf("missing parent entry while rebuilding active header heights %x", cursor.ParentHash)
		}
		cursor = parent
	}
	slices.Reverse(entries)
	if err := m.svc.chainState.Store().RewriteActiveHeaderHeights(0, tipEntry.Height, entries); err != nil {
		return 0, err
	}
	return len(entries), nil
}
