package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/dashboard"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/types"
	"encoding/hex"
	"fmt"
	"html"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type dashboardSystemStats = dashboard.SystemStats

type dashboardSystemSummary = dashboard.SystemSummary

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

func (s *Service) recordDashboardSystemSample() error {
	sample, err := dashboard.ReadSystemSample(time.Now())
	if err != nil {
		return err
	}
	s.systemStats.Record(sample)
	return nil
}

func (s *Service) buildPublicDashboardView() (*publicDashboardView, error) {
	now := time.Now()
	info := s.Info()
	peers := s.PeerInfo()
	relay := s.RelayPeerStats()
	template := s.BlockTemplateStats()
	system := s.systemStats.Summary(now, dashboardSystemWindow)
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
	template.FrontierCandidates = mempoolInfo.CandidateFrontier
	mempoolEntries := s.pool.Snapshot()
	mempoolTop := s.pool.TopByFee(8)
	performance := s.PerformanceMetrics()
	pow := s.dashboardPowSummary(blockWindow.recent)
	candidateFeeLine := s.cachedCandidateFeeLine()
	candidateTxCount := s.cachedCandidateBlockTxCount()
	if mempoolInfo.CandidateFrontier == 0 {
		candidateFeeLine = dashboardCandidateFeeLine{}
		candidateTxCount = -1
	}
	fees := s.dashboardFeeSummary(blockWindow.chart, mempoolInfo.Count, pow, candidateFeeLine, mempoolEntries)
	mining := s.dashboardMiningSummary(blocks, pow)
	tpsChart := s.dashboardTPSChartFromBlocks(blockWindow.recent, pow, candidateTxCount)
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
	case info.P2PAddr != "" && len(peers) == 0:
		return "LONELY-BUT-RUNNING"
	case info.HeaderHeight > info.TipHeight:
		return fmt.Sprintf("CATCHING-UP (%d headers ahead)", info.HeaderHeight-info.TipHeight)
	default:
		return "HEALTHY"
	}
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
		out.WriteString(fmt.Sprintf(" %3d tx%s |%s\n", block.TxCount, marker, strings.Repeat("#", block.BarWidth)))
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
	out.WriteString(" <a href=\"/fees\">Full fee chart -></a>\n")
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
	levels := []byte("._:-=+*#")
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
	out.Grow(len(lines))
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
		out.WriteByte(levels[level])
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
			cell = "#"
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
