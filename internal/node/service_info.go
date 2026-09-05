package node

import (
	"bitcoin-pure/internal/consensus"
	"encoding/hex"
)

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
		TxIndex:        s.chainState.Store().TxIndexStatus(),
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
