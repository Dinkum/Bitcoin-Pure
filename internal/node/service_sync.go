package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bytes"
	"fmt"
	rand "math/rand/v2"
	"slices"
	"strings"
	"time"
)

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
	maxJitter := base / 4
	if maxJitter <= 0 {
		return base
	}
	extra := time.Duration(rand.Int64N(int64(maxJitter)))
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
