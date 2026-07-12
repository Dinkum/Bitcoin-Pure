package node

import (
	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

type pendingThinBlock struct {
	hash          [32]byte
	header        types.BlockHeader
	txs           []types.Transaction
	filled        []bool
	expiresAt     time.Time
	retainedBytes uint64
	releaseBudget func()
}

func pendingThinRetainedBytes(state *pendingThinBlock) uint64 {
	if state == nil {
		return 0
	}
	// EncodedLen covers nested auth/output byte slices; the fixed allowance
	// covers the transaction structs, fill bitmap, and slice backing arrays.
	bytes := uint64(len(state.filled))
	for i := range state.txs {
		encoded := state.txs[i].EncodedLen()
		if encoded < 0 || bytes > ^uint64(0)-uint64(encoded)-128 {
			return ^uint64(0)
		}
		bytes += uint64(encoded) + 128
	}
	return bytes
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
