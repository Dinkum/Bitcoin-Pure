package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

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

func (s *Service) requestSync(peer *peerConn) error { return s.syncManager().requestSync(peer) }
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
