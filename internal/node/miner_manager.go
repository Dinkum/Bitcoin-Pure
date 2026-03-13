package node

import (
	"bytes"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

// minerManager owns block-template cache state, invalidation, and worker mining.
type minerManager struct {
	svc *Service

	templateMu         sync.Mutex
	templateGeneration uint64
	template           *blockTemplateCache
	templateStats      templateBuildTelemetry
}

type blockTemplateCache struct {
	tipHash         [32]byte
	mempoolEpoch    uint64
	generation      uint64
	builtAt         time.Time
	block           types.Block
	selected        []mempool.SnapshotEntry
	selectedTxs     []types.Transaction
	selectedTxIDs   [][32]byte
	selectedAuthIDs [][32]byte
	totalFees       uint64
	usedTxBytes     int
	baseUtxos       consensus.UtxoSet
	selectionView   *consensus.UtxoOverlay
	baseAcc         *utreexo.Accumulator
	selectionAcc    *utreexo.Accumulator
}

type templateBuildTelemetry struct {
	mu                 sync.Mutex
	cacheHits          int
	rebuilds           int
	fullBuilds         int
	appendExtends      int
	noChangeRefreshes  int
	frontierCandidates int
	invalidations      int
	interruptions      int
	lastBuildAt        time.Time
	lastReason         string
}

type chainSelectionSnapshot struct {
	tipHash        [32]byte
	height         uint64
	tipHeader      types.BlockHeader
	blockSizeState consensus.BlockSizeState
	utxos          consensus.UtxoSet
	utxoAcc        *utreexo.Accumulator
}

type chainTemplateContext struct {
	tipHash        [32]byte
	height         uint64
	tipHeader      types.BlockHeader
	blockSizeState consensus.BlockSizeState
}

func (m *minerManager) currentTemplateGeneration() uint64 {
	m.templateMu.Lock()
	defer m.templateMu.Unlock()
	return m.templateGeneration
}

func (m *minerManager) invalidateBlockTemplate(reason string) {
	m.templateMu.Lock()
	m.templateGeneration++
	m.template = nil
	m.templateMu.Unlock()
	m.templateStats.noteInvalidation(reason)
}

func (m *minerManager) BuildBlockTemplate() (types.Block, error) {
	block, _, err := m.buildBlockTemplateWithGeneration()
	return block, err
}

func (m *minerManager) buildBlockTemplateWithGeneration() (types.Block, uint64, error) {
	startedAt := time.Now()
	if m.svc.cfg.MinerPubKey == ([32]byte{}) {
		return types.Block{}, 0, errors.New("miner pubkey is required for block template assembly")
	}
	ctx, err := m.chainTemplateContext()
	if err != nil {
		return types.Block{}, 0, err
	}
	mempoolEpoch := m.svc.pool.Epoch()
	if cached, generation, ok := m.cachedBlockTemplate(ctx.tipHash, mempoolEpoch); ok {
		m.templateStats.noteCacheHit(m.svc.pool.SelectionCandidateCount())
		m.svc.perf.noteTemplateDuration(time.Since(startedAt))
		m.svc.logger.Debug("template ready",
			slog.String("mode", "cached"),
			slog.Uint64("generation", generation),
			slog.Uint64("next_height", ctx.height+1),
			slog.Int("txs", len(cached.Txs)),
			slog.Duration("template_duration", time.Since(startedAt)),
		)
		return cloneBlock(cached), generation, nil
	}
	if block, generation, ok, err := m.extendBlockTemplate(ctx, mempoolEpoch); err != nil {
		return types.Block{}, 0, err
	} else if ok {
		m.templateStats.noteAppendExtend(m.svc.pool.SelectionCandidateCount())
		m.svc.noteTemplateRebuild()
		m.svc.perf.noteTemplateDuration(time.Since(startedAt))
		m.svc.logger.Debug("template ready",
			slog.String("mode", "extended"),
			slog.Uint64("generation", generation),
			slog.Uint64("next_height", ctx.height+1),
			slog.Int("txs", len(block.Txs)),
			slog.Duration("template_duration", time.Since(startedAt)),
		)
		return cloneBlock(block), generation, nil
	}
	snapshot, err := m.chainSelectionSnapshot()
	if err != nil {
		return types.Block{}, 0, err
	}
	block, selectedEntries, selectedTxs, selectedTxIDs, selectedAuthIDs, totalFees, usedTxBytes, baseUtxos, selectionView, selectionAcc, err := m.buildBlockCandidate(snapshot)
	if err != nil {
		return types.Block{}, 0, err
	}
	generation := m.storeBlockTemplate(snapshot.tipHash, mempoolEpoch, block, selectedEntries, selectedTxs, selectedTxIDs, selectedAuthIDs, totalFees, usedTxBytes, baseUtxos, selectionView, snapshot.utxoAcc, selectionAcc)
	m.templateStats.noteFullBuild(m.svc.pool.SelectionCandidateCount())
	m.svc.noteTemplateRebuild()
	m.svc.perf.noteTemplateDuration(time.Since(startedAt))
	m.svc.logger.Debug("template ready",
		slog.String("mode", "rebuilt"),
		slog.Uint64("generation", generation),
		slog.Uint64("next_height", snapshot.height+1),
		slog.Int("txs", len(block.Txs)),
		slog.Duration("template_duration", time.Since(startedAt)),
	)
	return cloneBlock(block), generation, nil
}

func (m *minerManager) minerLoop(workerID int) {
	for {
		select {
		case <-m.svc.stopCh:
			return
		default:
		}
		hash, err := m.mineOneBlock()
		if err != nil {
			if errors.Is(err, ErrNoTip) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			m.svc.logger.Warn("continuous mining failed", slog.Int("worker", workerID), slog.Any("error", err))
			select {
			case <-m.svc.stopCh:
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		m.svc.logger.Debug("miner worker found block", slog.Int("worker", workerID), slog.String("hash", hex.EncodeToString(hash[:])))
	}
}

func (m *minerManager) mineOneBlock() ([32]byte, error) {
	for {
		block, generation, err := m.buildBlockTemplateWithGeneration()
		if err != nil {
			return [32]byte{}, err
		}
		block, fresh, err := m.svc.mineBlockTemplate(block, generation)
		if err != nil {
			return [32]byte{}, err
		}
		if !fresh {
			m.templateStats.noteInterruption()
			m.svc.noteTemplateInterruption()
			continue
		}
		hash, _, err := m.svc.acceptMinedBlock(block)
		if err == nil {
			return hash, nil
		}
		if errors.Is(err, ErrNoTip) {
			return [32]byte{}, err
		}
		if strings.Contains(err.Error(), "stale template") {
			continue
		}
		return [32]byte{}, err
	}
}

func (m *minerManager) buildBlockCandidate(snapshot chainSelectionSnapshot) (types.Block, []mempool.SnapshotEntry, []types.Transaction, [][32]byte, [][32]byte, uint64, int, consensus.UtxoSet, *consensus.UtxoOverlay, *utreexo.Accumulator, error) {
	startedAt := time.Now()
	baseUtxos := snapshot.utxos
	maxTemplateBytes := int(consensus.NextBlockSizeLimit(snapshot.blockSizeState, consensus.ParamsForProfile(m.svc.cfg.Profile)))
	if maxTemplateBytes > 1024 {
		maxTemplateBytes -= 1024
	}
	selectStartedAt := time.Now()
	selectedEntries, totalFees, selectionView := m.svc.pool.SelectForBlockOverlay(baseUtxos, consensus.DefaultConsensusRules(), maxTemplateBytes)
	m.svc.perf.noteTemplateSelectDuration(time.Since(selectStartedAt))
	selectedTxs, selectedTxIDs, selectedAuthIDs, usedTxBytes := buildSelectedTemplateVectors(selectedEntries)
	selectedSpends, selectedLeaves := selectedEntryAccumulatorDeltas(selectedEntries)
	accStartedAt := time.Now()
	selectionAcc, err := snapshot.utxoAcc.Apply(selectedSpends, selectedLeaves)
	m.svc.perf.noteTemplateAccumulateDuration(time.Since(accStartedAt))
	if err != nil {
		return types.Block{}, nil, nil, nil, nil, 0, 0, nil, nil, nil, err
	}
	assembleStartedAt := time.Now()
	block, err := m.assembleBlockTemplate(chainTemplateContext{
		tipHash:        snapshot.tipHash,
		height:         snapshot.height,
		tipHeader:      snapshot.tipHeader,
		blockSizeState: snapshot.blockSizeState,
	}, selectedTxs, selectedTxIDs, selectedAuthIDs, totalFees, selectionAcc)
	m.svc.perf.noteTemplateAssembleDuration(time.Since(assembleStartedAt))
	if err != nil {
		return types.Block{}, nil, nil, nil, nil, 0, 0, nil, nil, nil, err
	}
	m.svc.logger.Debug("building block candidate",
		slog.Uint64("next_height", snapshot.height+1),
		slog.Int("selected_txs", len(selectedEntries)),
		slog.Uint64("total_fees", totalFees),
		slog.Duration("template_duration", time.Since(startedAt)),
	)
	return block, selectedEntries, selectedTxs, selectedTxIDs, selectedAuthIDs, totalFees, usedTxBytes, baseUtxos, selectionView, selectionAcc, nil
}

func (m *minerManager) assembleBlockTemplate(ctx chainTemplateContext, selectedTxs []types.Transaction, selectedTxIDs [][32]byte, selectedAuthIDs [][32]byte, totalFees uint64, selectionAcc *utreexo.Accumulator) (types.Block, error) {
	params := consensus.ParamsForProfile(m.svc.cfg.Profile)
	nextTimestamp := ctx.tipHeader.Timestamp + uint64(params.TargetSpacingSecs)
	if nextTimestamp <= ctx.tipHeader.Timestamp {
		nextTimestamp = ctx.tipHeader.Timestamp + 1
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: ctx.height, Header: ctx.tipHeader}, params)
	if err != nil {
		return types.Block{}, err
	}

	coinbase := coinbaseTxForHeight(ctx.height+1, []types.TxOutput{{
		ValueAtoms: consensus.SubsidyAtoms(ctx.height+1, params) + totalFees,
		PubKey:     m.svc.cfg.MinerPubKey,
	}})
	coinbaseTxID := consensus.TxID(&coinbase)
	coinbaseAuthID := consensus.AuthID(&coinbase)

	txs := make([]types.Transaction, 0, len(selectedTxs)+1)
	txids := make([][32]byte, 0, len(selectedTxIDs)+1)
	authids := make([][32]byte, 0, len(selectedAuthIDs)+1)
	txs = append(txs, coinbase)
	txids = append(txids, coinbaseTxID)
	authids = append(authids, coinbaseAuthID)
	txs = append(txs, selectedTxs...)
	txids = append(txids, selectedTxIDs...)
	authids = append(authids, selectedAuthIDs...)
	txRoot, authRoot := consensus.BuildBlockRootsFromIDs(txids, authids)
	finalAcc, err := selectionAcc.Apply(nil, coinbaseLeaves(coinbaseTxID, coinbase.Base.Outputs))
	if err != nil {
		return types.Block{}, err
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  ctx.tipHash,
		MerkleTxIDRoot: txRoot,
		MerkleAuthRoot: authRoot,
		UTXORoot:       finalAcc.Root(),
		Timestamp:      nextTimestamp,
		NBits:          nbits,
	}
	return types.Block{Header: header, Txs: txs}, nil
}

func (m *minerManager) chainTemplateContext() (chainTemplateContext, error) {
	m.svc.stateMu.RLock()
	defer m.svc.stateMu.RUnlock()
	view, ok := m.svc.chainState.sharedCommittedView()
	if !ok {
		return chainTemplateContext{}, ErrNoTip
	}
	return chainTemplateContext{
		tipHash:        view.TipHash,
		height:         view.Height,
		tipHeader:      view.TipHeader,
		blockSizeState: view.BlockSizeState,
	}, nil
}

func (m *minerManager) chainSelectionSnapshot() (chainSelectionSnapshot, error) {
	m.svc.stateMu.RLock()
	defer m.svc.stateMu.RUnlock()
	view, ok := m.svc.chainState.sharedCommittedView()
	if !ok {
		return chainSelectionSnapshot{}, ErrNoTip
	}
	return chainSelectionSnapshot{
		tipHash:        view.TipHash,
		height:         view.Height,
		tipHeader:      view.TipHeader,
		blockSizeState: view.BlockSizeState,
		utxos:          view.UTXOs,
		utxoAcc:        view.UTXOAcc,
	}, nil
}

func (m *minerManager) cachedBlockTemplate(tipHash [32]byte, mempoolEpoch uint64) (types.Block, uint64, bool) {
	m.templateMu.Lock()
	defer m.templateMu.Unlock()
	if m.template == nil || m.template.tipHash != tipHash || m.template.mempoolEpoch != mempoolEpoch {
		return types.Block{}, 0, false
	}
	return cloneBlock(m.template.block), m.template.generation, true
}

func (m *minerManager) extendBlockTemplate(ctx chainTemplateContext, mempoolEpoch uint64) (types.Block, uint64, bool, error) {
	m.templateMu.Lock()
	defer m.templateMu.Unlock()
	if m.template == nil || m.template.tipHash != ctx.tipHash || m.template.mempoolEpoch >= mempoolEpoch {
		return types.Block{}, 0, false, nil
	}
	maxTemplateBytes := int(consensus.NextBlockSizeLimit(ctx.blockSizeState, consensus.ParamsForProfile(m.svc.cfg.Profile)))
	if maxTemplateBytes > 1024 {
		maxTemplateBytes -= 1024
	}
	if m.template.usedTxBytes >= maxTemplateBytes {
		return types.Block{}, 0, false, nil
	}
	if !m.svc.pool.ContainsAll(m.template.selected) {
		return types.Block{}, 0, false, nil
	}

	appendSelectStartedAt := time.Now()
	added, addedFees := m.svc.pool.AppendForBlockOverlay(m.template.baseUtxos, m.template.selectionView, consensus.DefaultConsensusRules(), maxTemplateBytes, m.template.selected)
	m.svc.perf.noteTemplateSelectDuration(time.Since(appendSelectStartedAt))
	if len(added) == 0 {
		m.template.mempoolEpoch = mempoolEpoch
		assembleStartedAt := time.Now()
		block, err := m.assembleBlockTemplate(ctx, m.template.selectedTxs, m.template.selectedTxIDs, m.template.selectedAuthIDs, m.template.totalFees, m.template.selectionAcc)
		m.svc.perf.noteTemplateAssembleDuration(time.Since(assembleStartedAt))
		if err != nil {
			return types.Block{}, 0, false, err
		}
		m.template.block = cloneBlock(block)
		m.template.builtAt = time.Now()
		m.templateStats.noteNoChangeRefresh(m.svc.pool.SelectionCandidateCount())
		m.templateStats.noteBuildTime(m.template.builtAt)
		return cloneBlock(m.template.block), m.template.generation, true, nil
	}

	m.template.selected = mergeSnapshotEntriesByTxID(m.template.selected, added)
	m.template.selectedTxs, m.template.selectedTxIDs, m.template.selectedAuthIDs = mergeSelectedTemplateVectors(m.template.selectedTxs, m.template.selectedTxIDs, m.template.selectedAuthIDs, added)
	m.template.totalFees += addedFees
	for _, entry := range added {
		m.template.usedTxBytes += entry.Size
	}
	addedSpends, addedLeaves := selectedEntryAccumulatorDeltas(added)
	accStartedAt := time.Now()
	nextAcc, err := m.template.selectionAcc.Apply(addedSpends, addedLeaves)
	m.svc.perf.noteTemplateAccumulateDuration(time.Since(accStartedAt))
	if err != nil {
		return types.Block{}, 0, false, err
	}
	m.template.selectionAcc = nextAcc
	assembleStartedAt := time.Now()
	block, err := m.assembleBlockTemplate(ctx, m.template.selectedTxs, m.template.selectedTxIDs, m.template.selectedAuthIDs, m.template.totalFees, m.template.selectionAcc)
	m.svc.perf.noteTemplateAssembleDuration(time.Since(assembleStartedAt))
	if err != nil {
		return types.Block{}, 0, false, err
	}
	m.template.block = cloneBlock(block)
	m.template.mempoolEpoch = mempoolEpoch
	m.template.builtAt = time.Now()
	m.templateStats.noteAppendExtend(m.svc.pool.SelectionCandidateCount())
	m.templateStats.noteBuildTime(m.template.builtAt)
	return cloneBlock(m.template.block), m.template.generation, true, nil
}

func (m *minerManager) storeBlockTemplate(tipHash [32]byte, mempoolEpoch uint64, block types.Block, selected []mempool.SnapshotEntry, selectedTxs []types.Transaction, selectedTxIDs [][32]byte, selectedAuthIDs [][32]byte, totalFees uint64, usedTxBytes int, baseUtxos consensus.UtxoSet, selectionView *consensus.UtxoOverlay, baseAcc *utreexo.Accumulator, selectionAcc *utreexo.Accumulator) uint64 {
	m.templateMu.Lock()
	defer m.templateMu.Unlock()
	if m.templateGeneration == 0 {
		m.templateGeneration = 1
	}
	generation := m.templateGeneration
	m.template = &blockTemplateCache{
		tipHash:         tipHash,
		mempoolEpoch:    mempoolEpoch,
		generation:      generation,
		builtAt:         time.Now(),
		block:           cloneBlock(block),
		selected:        append([]mempool.SnapshotEntry(nil), selected...),
		selectedTxs:     append([]types.Transaction(nil), selectedTxs...),
		selectedTxIDs:   append([][32]byte(nil), selectedTxIDs...),
		selectedAuthIDs: append([][32]byte(nil), selectedAuthIDs...),
		totalFees:       totalFees,
		usedTxBytes:     usedTxBytes,
		baseUtxos:       baseUtxos,
		selectionView:   selectionView,
		baseAcc:         baseAcc,
		selectionAcc:    selectionAcc,
	}
	m.templateStats.noteBuildTime(m.template.builtAt)
	return generation
}

func (m *minerManager) BlockTemplateStats() BlockTemplateStats {
	return m.templateStats.snapshot()
}

func (m *minerManager) cachedTemplateTxCount() int {
	m.templateMu.Lock()
	defer m.templateMu.Unlock()
	if m.template == nil {
		return -1
	}
	return max(len(m.template.block.Txs)-1, 0)
}

func (m *minerManager) cachedTemplateFeeLine() dashboardCandidateFeeLine {
	m.templateMu.Lock()
	defer m.templateMu.Unlock()
	if m.template == nil {
		return dashboardCandidateFeeLine{}
	}
	line := dashboardCandidateFeeLine{
		Available: true,
		TotalTxs:  len(m.template.selected),
	}
	if len(m.template.selected) == 0 {
		return line
	}
	fees := make([]uint64, 0, len(m.template.selected))
	for _, entry := range m.template.selected {
		rate := uint64(0)
		if entry.Size > 0 {
			rate = (entry.Fee * 1000) / uint64(entry.Size)
		}
		fees = append(fees, rate)
		if entry.Fee > 0 {
			line.PaidTxs++
		}
	}
	line.MedianFee, line.LowFee, line.HighFee = summarizeFeeSet(fees)
	return line
}

func (t *templateBuildTelemetry) noteCacheHit(frontierCandidates int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cacheHits++
	if frontierCandidates > t.frontierCandidates {
		t.frontierCandidates = frontierCandidates
	}
}

func (t *templateBuildTelemetry) noteFullBuild(frontierCandidates int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rebuilds++
	t.fullBuilds++
	if frontierCandidates > t.frontierCandidates {
		t.frontierCandidates = frontierCandidates
	}
}

func (t *templateBuildTelemetry) noteAppendExtend(frontierCandidates int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rebuilds++
	t.appendExtends++
	if frontierCandidates > t.frontierCandidates {
		t.frontierCandidates = frontierCandidates
	}
}

func (t *templateBuildTelemetry) noteNoChangeRefresh(frontierCandidates int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.noChangeRefreshes++
	if frontierCandidates > t.frontierCandidates {
		t.frontierCandidates = frontierCandidates
	}
}

func (t *templateBuildTelemetry) noteInvalidation(reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.invalidations++
	t.lastReason = reason
}

func (t *templateBuildTelemetry) noteInterruption() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.interruptions++
	t.lastReason = "interrupted"
}

func (t *templateBuildTelemetry) noteBuildTime(at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastBuildAt = at
}

func (t *templateBuildTelemetry) snapshot() BlockTemplateStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	lastBuildAgeMS := 0
	if !t.lastBuildAt.IsZero() {
		lastBuildAgeMS = int(time.Since(t.lastBuildAt) / time.Millisecond)
	}
	return BlockTemplateStats{
		CacheHits:          t.cacheHits,
		Rebuilds:           t.rebuilds,
		FullBuilds:         t.fullBuilds,
		AppendExtends:      t.appendExtends,
		NoChangeRefreshes:  t.noChangeRefreshes,
		FrontierCandidates: t.frontierCandidates,
		Invalidations:      t.invalidations,
		Interruptions:      t.interruptions,
		LastBuildAgeMS:     lastBuildAgeMS,
		LastReason:         t.lastReason,
	}
}

func cloneBlock(block types.Block) types.Block {
	out := block
	if len(block.Txs) != 0 {
		out.Txs = append([]types.Transaction(nil), block.Txs...)
	}
	return out
}

func mergeSnapshotEntriesByTxID(left, right []mempool.SnapshotEntry) []mempool.SnapshotEntry {
	if len(left) == 0 {
		return append([]mempool.SnapshotEntry(nil), right...)
	}
	if len(right) == 0 {
		return left
	}
	merged := make([]mempool.SnapshotEntry, 0, len(left)+len(right))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		if bytes.Compare(left[i].TxID[:], right[j].TxID[:]) <= 0 {
			merged = append(merged, left[i])
			i++
			continue
		}
		merged = append(merged, right[j])
		j++
	}
	merged = append(merged, left[i:]...)
	merged = append(merged, right[j:]...)
	return merged
}

func selectedEntryAccumulatorDeltas(entries []mempool.SnapshotEntry) ([]types.OutPoint, []utreexo.UtxoLeaf) {
	totalSpent := 0
	totalCreated := 0
	for _, entry := range entries {
		totalSpent += len(entry.SpentOutPoints)
		totalCreated += len(entry.CreatedLeaves)
	}
	spent := make([]types.OutPoint, 0, totalSpent)
	created := make([]utreexo.UtxoLeaf, 0, totalCreated)
	for _, entry := range entries {
		spent = append(spent, entry.SpentOutPoints...)
		created = append(created, entry.CreatedLeaves...)
	}
	return spent, created
}

func buildSelectedTemplateVectors(entries []mempool.SnapshotEntry) ([]types.Transaction, [][32]byte, [][32]byte, int) {
	if len(entries) == 0 {
		return nil, nil, nil, 0
	}
	txs := make([]types.Transaction, 0, len(entries))
	txids := make([][32]byte, 0, len(entries))
	authids := make([][32]byte, 0, len(entries))
	usedTxBytes := 0
	for _, entry := range entries {
		txs = append(txs, entry.Tx)
		txids = append(txids, entry.TxID)
		authids = append(authids, entry.AuthID)
		usedTxBytes += entry.Size
	}
	return txs, txids, authids, usedTxBytes
}

func mergeSelectedTemplateVectors(leftTxs []types.Transaction, leftTxIDs [][32]byte, leftAuthIDs [][32]byte, right []mempool.SnapshotEntry) ([]types.Transaction, [][32]byte, [][32]byte) {
	if len(leftTxs) == 0 {
		txs, txids, authids, _ := buildSelectedTemplateVectors(right)
		return txs, txids, authids
	}
	if len(right) == 0 {
		return leftTxs, leftTxIDs, leftAuthIDs
	}
	mergedTxs := make([]types.Transaction, 0, len(leftTxs)+len(right))
	mergedTxIDs := make([][32]byte, 0, len(leftTxIDs)+len(right))
	mergedAuthIDs := make([][32]byte, 0, len(leftAuthIDs)+len(right))
	i, j := 0, 0
	for i < len(leftTxIDs) && j < len(right) {
		if bytes.Compare(leftTxIDs[i][:], right[j].TxID[:]) <= 0 {
			mergedTxs = append(mergedTxs, leftTxs[i])
			mergedTxIDs = append(mergedTxIDs, leftTxIDs[i])
			mergedAuthIDs = append(mergedAuthIDs, leftAuthIDs[i])
			i++
			continue
		}
		mergedTxs = append(mergedTxs, right[j].Tx)
		mergedTxIDs = append(mergedTxIDs, right[j].TxID)
		mergedAuthIDs = append(mergedAuthIDs, right[j].AuthID)
		j++
	}
	mergedTxs = append(mergedTxs, leftTxs[i:]...)
	mergedTxIDs = append(mergedTxIDs, leftTxIDs[i:]...)
	mergedAuthIDs = append(mergedAuthIDs, leftAuthIDs[i:]...)
	for ; j < len(right); j++ {
		mergedTxs = append(mergedTxs, right[j].Tx)
		mergedTxIDs = append(mergedTxIDs, right[j].TxID)
		mergedAuthIDs = append(mergedAuthIDs, right[j].AuthID)
	}
	return mergedTxs, mergedTxIDs, mergedAuthIDs
}
