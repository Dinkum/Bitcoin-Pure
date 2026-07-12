package node

import (
	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"bytes"
	"io"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
)

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

func (p *peerConn) storePendingThin(state *pendingThinBlock) bool {
	if p == nil || state == nil {
		return false
	}
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	p.pruneExpiredPendingThinLocked(time.Now())
	if previous := p.pendingThin[state.hash]; previous != nil {
		p.releasePendingThinLocked(previous)
		delete(p.pendingThin, state.hash)
	}
	if len(p.pendingThin) >= maxPendingThinPerPeer {
		return false
	}
	retained := pendingThinRetainedBytes(state)
	if retained == 0 || retained > uint64(^uint(0)>>1) {
		return false
	}
	var budget *p2p.PayloadBudget
	if p.svc != nil && p.svc.stopCh != nil && p.closed != nil {
		budget = p.svc.thinStateBudget
	}
	release, ok := budget.TryAcquire(int(retained))
	if !ok {
		return false
	}
	state.retainedBytes = retained
	state.expiresAt = time.Now().Add(pendingThinStateTTL)
	state.releaseBudget = release
	p.pendingThin[state.hash] = state
	if p.svc != nil {
		hash := state.hash
		expiresAt := state.expiresAt
		p.svc.safeGo("pending-thin-expiry", func() {
			p.expirePendingThinAt(hash, expiresAt)
		})
	}
	return true
}

func (p *peerConn) pendingThinState(hash [32]byte) (*pendingThinBlock, bool) {
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	p.pruneExpiredPendingThinLocked(time.Now())
	state, ok := p.pendingThin[hash]
	return state, ok
}

func (p *peerConn) deletePendingThin(hash [32]byte) {
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	if state := p.pendingThin[hash]; state != nil {
		p.releasePendingThinLocked(state)
	}
	delete(p.pendingThin, hash)
}

func (p *peerConn) pruneExpiredPendingThinLocked(now time.Time) {
	for hash, state := range p.pendingThin {
		if state == nil || state.expiresAt.IsZero() || now.Before(state.expiresAt) {
			continue
		}
		p.releasePendingThinLocked(state)
		delete(p.pendingThin, hash)
	}
}

func (p *peerConn) releasePendingThinLocked(state *pendingThinBlock) {
	if state != nil && state.releaseBudget != nil {
		state.releaseBudget()
		state.releaseBudget = nil
	}
}

func (p *peerConn) expirePendingThinAt(hash [32]byte, expiresAt time.Time) {
	wait := time.Until(expiresAt)
	if wait < 0 {
		wait = 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-p.svc.stopCh:
		return
	case <-p.closed:
		return
	case <-timer.C:
	}
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	state := p.pendingThin[hash]
	if state == nil || !state.expiresAt.Equal(expiresAt) || time.Now().Before(expiresAt) {
		return
	}
	p.releasePendingThinLocked(state)
	delete(p.pendingThin, hash)
}

func (p *peerConn) allowThinBlockWork(now time.Time) bool {
	p.thinMu.Lock()
	defer p.thinMu.Unlock()
	if p.thinWorkRefill.IsZero() {
		p.thinWorkTokens = thinWorkTokenBurst
		p.thinWorkRefill = now
	}
	elapsed := now.Sub(p.thinWorkRefill).Seconds()
	if elapsed > 0 {
		p.thinWorkTokens = math.Min(thinWorkTokenBurst, p.thinWorkTokens+elapsed*thinWorkTokensPerSecond)
		p.thinWorkRefill = now
	}
	if p.thinWorkTokens < 1 {
		return false
	}
	p.thinWorkTokens--
	return true
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
