package mempool

import (
	"bytes"
	"container/heap"
	"math/bits"
	"sort"
	"sync"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

var selectionScratchPool = sync.Pool{
	New: func() any {
		return &selectionScratch{
			included:          make(map[[32]byte]struct{}),
			claimed:           make(map[types.OutPoint]struct{}),
			seenPackageClaims: make(map[types.OutPoint]struct{}),
		}
	},
}

type packageCandidate struct {
	TxID    [32]byte
	Entries []*Entry
	Fee     uint64
	Size    int
}

// packageCandidateView freezes a frontier candidate into immutable snapshots so
// block-template assembly can run outside the mempool lock. The entire view is
// cached per mempool epoch, so repeated selections do not keep rebuilding it.
type packageCandidateView struct {
	TxID    [32]byte
	Entries []SnapshotEntry
	Fee     uint64
	Size    int
}

type selectionSnapshot struct {
	candidates       []packageCandidateView
	wakeByEntry      map[[32]byte][]int
	hasSharedEntries bool
}

type selectionCache struct {
	epoch    uint64
	frontier candidateHeap
	snapshot *selectionSnapshot
}

// selectionScratch reuses the transient working sets needed while walking the
// cached package frontier. The selection algorithm revisits many candidates per
// block build, so pooling these buffers removes a large amount of short-lived
// map and slice churn without changing ordering or policy behavior.
type selectionScratch struct {
	filtered              []SnapshotEntry
	queue                 []int
	queued                []bool
	completed             []bool
	included              map[[32]byte]struct{}
	claimed               map[types.OutPoint]struct{}
	seenPackageClaims     map[types.OutPoint]struct{}
	seenPackageClaimOrder []types.OutPoint
}

func borrowSelectionScratch() *selectionScratch {
	scratch := selectionScratchPool.Get().(*selectionScratch)
	clear(scratch.included)
	clear(scratch.claimed)
	scratch.filtered = scratch.filtered[:0]
	scratch.queue = scratch.queue[:0]
	scratch.seenPackageClaimOrder = scratch.seenPackageClaimOrder[:0]
	return scratch
}

func releaseSelectionScratch(scratch *selectionScratch) {
	if scratch == nil {
		return
	}
	scratch.resetSeenPackageClaims()
	clear(scratch.included)
	clear(scratch.claimed)
	scratch.filtered = scratch.filtered[:0]
	scratch.queue = scratch.queue[:0]
	selectionScratchPool.Put(scratch)
}

func (s *selectionScratch) prepareWakeState(candidateCount int) ([]bool, []bool, []int) {
	if cap(s.queued) < candidateCount {
		s.queued = make([]bool, candidateCount)
	} else {
		s.queued = s.queued[:candidateCount]
		clear(s.queued)
	}
	if cap(s.completed) < candidateCount {
		s.completed = make([]bool, candidateCount)
	} else {
		s.completed = s.completed[:candidateCount]
		clear(s.completed)
	}
	s.queue = s.queue[:0]
	return s.queued, s.completed, s.queue
}

func (s *selectionScratch) resetSeenPackageClaims() {
	for _, spent := range s.seenPackageClaimOrder {
		delete(s.seenPackageClaims, spent)
	}
	s.seenPackageClaimOrder = s.seenPackageClaimOrder[:0]
}

// candidateHeap keeps the package frontier incrementally ordered without
// reindexing the full tail on every insert or removal. Block assembly still
// materializes an ordered snapshot when it needs to walk the frontier end to
// end, but churn-heavy admission updates now stay O(log n).
type candidateHeap struct {
	items   []packageCandidate
	indexes map[[32]byte]int
}

// SelectForBlock preserves the caller's committed UTXO map and keeps selection
// state inside an overlay, so read-only callers do not pay for a full clone.
func (p *Pool) SelectForBlock(chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64) {
	selected, totalFees, _ := p.SelectForBlockOverlay(chainUtxos, rules, maxTxBytes)
	return selected, totalFees
}

// SelectForBlockOverlay builds block candidates against an immutable base UTXO
// map and returns the post-selection overlay so callers can keep extending the
// tentative block state without materializing the full live set.
func (p *Pool) SelectForBlockOverlay(baseUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	p.mu.Lock()
	snapshot := p.selectionSnapshotLocked()
	p.mu.Unlock()
	return p.selectForBlock(baseUtxos, rules, maxTxBytes, snapshot)
}

// SelectForBlockOverlayWithLookup builds block candidates against a generic
// committed-chain lookup and returns the post-selection overlay.
// SelectForBlockOverlayWithLookupLimit is used by benchmark-only template
// builders that need a stable transaction count per synthetic block. A zero
// maxTxs preserves normal miner selection behavior.
func (p *Pool) SelectForBlockOverlayWithLookupLimit(baseLookup consensus.UtxoLookup, context consensus.TxValidationContext, rules consensus.ConsensusRules, maxTxBytes int, maxTxs int) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	p.mu.Lock()
	snapshot := p.selectionSnapshotLocked()
	p.mu.Unlock()
	return p.selectForBlockWithLookup(baseLookup, context, rules, maxTxBytes, maxTxs, snapshot)
}

func (p *Pool) selectForBlock(baseUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int, snapshot selectionSnapshot) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	preBlockUtxos := consensus.LookupFromSet(baseUtxos)
	overlay := consensus.NewUtxoOverlay(baseUtxos)
	scratch := borrowSelectionScratch()
	defer releaseSelectionScratch(scratch)
	selected := make([]SnapshotEntry, 0, len(snapshot.candidates))
	included := scratch.included
	claimed := scratch.claimed
	usedBytes := 0
	var totalFees uint64
	p.runSelectionSnapshot(snapshot, scratch, included, func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool {
		if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
			return false
		}
		if !p.meetsRelayFloor(filteredFee, filteredSize) {
			return false
		}
		packageFees, ok := applyCandidatePackageSnapshot(preBlockUtxos, overlay, claimed, filtered, scratch, consensus.TxValidationContext{Params: consensus.MainnetParams()}, rules)
		if !ok {
			return false
		}
		usedBytes += filteredSize
		totalFees += packageFees
		for _, entry := range filtered {
			included[entry.TxID] = struct{}{}
			selected = append(selected, entry)
		}
		return true
	})

	sortSnapshotEntriesByTxID(selected)
	return selected, totalFees, overlay
}

func (p *Pool) selectForBlockWithLookup(baseLookup consensus.UtxoLookup, context consensus.TxValidationContext, rules consensus.ConsensusRules, maxTxBytes int, maxTxs int, snapshot selectionSnapshot) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	preBlockUtxos := baseLookup
	overlay := consensus.NewUtxoOverlayWithLookup(consensus.LookupWithErrFromLookup(baseLookup))
	scratch := borrowSelectionScratch()
	defer releaseSelectionScratch(scratch)
	selected := make([]SnapshotEntry, 0, len(snapshot.candidates))
	included := scratch.included
	claimed := scratch.claimed
	usedBytes := 0
	var totalFees uint64
	p.runSelectionSnapshot(snapshot, scratch, included, func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool {
		if maxTxs > 0 && len(selected)+len(filtered) > maxTxs {
			return false
		}
		if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
			return false
		}
		if !p.meetsRelayFloor(filteredFee, filteredSize) {
			return false
		}
		packageFees, ok := applyCandidatePackageSnapshot(preBlockUtxos, overlay, claimed, filtered, scratch, context, rules)
		if !ok {
			return false
		}
		usedBytes += filteredSize
		totalFees += packageFees
		for _, entry := range filtered {
			included[entry.TxID] = struct{}{}
			selected = append(selected, entry)
		}
		return true
	})

	sortSnapshotEntriesByTxID(selected)
	return selected, totalFees, overlay
}

// AppendForBlockOverlay extends a previously-built tentative block state
// without forcing the caller to materialize a full post-selection UTXO map.
func (p *Pool) AppendForBlockOverlay(preBlockUtxos consensus.UtxoSet, currentUtxos *consensus.UtxoOverlay, rules consensus.ConsensusRules, maxTxBytes int, selected []SnapshotEntry) ([]SnapshotEntry, uint64) {
	return p.AppendForBlockOverlayWithLookup(consensus.LookupFromSet(preBlockUtxos), currentUtxos, consensus.TxValidationContext{Params: consensus.MainnetParams()}, rules, maxTxBytes, selected)
}

// AppendForBlockOverlayWithLookup extends a tentative block state against an
// immutable committed-chain lookup without forcing callers to materialize that
// base view as a full map first.
func (p *Pool) AppendForBlockOverlayWithLookup(preBlockLookup consensus.UtxoLookup, currentUtxos *consensus.UtxoOverlay, context consensus.TxValidationContext, rules consensus.ConsensusRules, maxTxBytes int, selected []SnapshotEntry) ([]SnapshotEntry, uint64) {
	p.mu.Lock()
	snapshot := p.selectionSnapshotLocked()
	p.mu.Unlock()
	scratch := borrowSelectionScratch()
	defer releaseSelectionScratch(scratch)
	appendOnly := make([]SnapshotEntry, 0, len(snapshot.candidates))
	included := scratch.included
	claimed := scratch.claimed
	usedBytes := 0
	for _, entry := range selected {
		included[entry.TxID] = struct{}{}
		usedBytes += entry.Size
		for _, spent := range entry.SpentOutPoints {
			claimed[spent] = struct{}{}
		}
	}
	var totalFees uint64

	p.runSelectionSnapshot(snapshot, scratch, included, func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool {
		if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
			return false
		}
		if !p.meetsRelayFloor(filteredFee, filteredSize) {
			return false
		}
		packageFees, ok := applyCandidatePackageSnapshot(preBlockLookup, currentUtxos, claimed, filtered, scratch, context, rules)
		if !ok {
			return false
		}
		usedBytes += filteredSize
		totalFees += packageFees
		for _, entry := range filtered {
			included[entry.TxID] = struct{}{}
			appendOnly = append(appendOnly, entry)
		}
		return true
	})

	sortSnapshotEntriesByTxID(appendOnly)
	return appendOnly, totalFees
}

func (p *Pool) AppendForBlock(preBlockUtxos consensus.UtxoSet, currentUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int, selected []SnapshotEntry) ([]SnapshotEntry, uint64) {
	overlay := consensus.NewUtxoOverlay(currentUtxos)
	appendOnly, totalFees := p.AppendForBlockOverlay(preBlockUtxos, overlay, rules, maxTxBytes, selected)
	finalUtxos := overlay.Materialize()
	for outPoint := range currentUtxos {
		delete(currentUtxos, outPoint)
	}
	for outPoint, entry := range finalUtxos {
		currentUtxos[outPoint] = entry
	}
	return appendOnly, totalFees
}

func (p *Pool) SelectionCandidateCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.selection != nil {
		return p.selection.frontier.Len()
	}
	return len(p.entries)
}

func (p *Pool) cachedPackageCandidatesLocked() []packageCandidate {
	p.ensureSelectionLocked()
	return p.selection.frontier.ordered()
}

func (p *Pool) selectionSnapshotLocked() selectionSnapshot {
	if p.selection != nil && p.selection.snapshot != nil {
		return *p.selection.snapshot
	}
	live := p.cachedPackageCandidatesLocked()
	snapshot := selectionSnapshot{
		candidates:  make([]packageCandidateView, 0, len(live)),
		wakeByEntry: make(map[[32]byte][]int, len(p.entries)),
	}
	for _, candidate := range live {
		view := packageCandidateView{
			TxID:    candidate.TxID,
			Entries: make([]SnapshotEntry, 0, len(candidate.Entries)),
			Fee:     candidate.Fee,
			Size:    candidate.Size,
		}
		for _, entry := range candidate.Entries {
			view.Entries = append(view.Entries, selectionSnapshotForEntry(entry))
		}
		idx := len(snapshot.candidates)
		snapshot.candidates = append(snapshot.candidates, view)
		for _, entry := range view.Entries {
			wake := append(snapshot.wakeByEntry[entry.TxID], idx)
			snapshot.wakeByEntry[entry.TxID] = wake
			if len(wake) > 1 {
				snapshot.hasSharedEntries = true
			}
		}
	}
	if p.selection != nil {
		p.selection.snapshot = &snapshot
	}
	return snapshot
}

func (p *Pool) packageForSelection(txid [32]byte) []*Entry {
	seen := make(map[[32]byte]struct{})
	ordered := make([]*Entry, 0)
	var visit func([32]byte)
	visit = func(current [32]byte) {
		if _, ok := seen[current]; ok {
			return
		}
		entry := p.entries[current]
		if entry == nil {
			return
		}
		seen[current] = struct{}{}
		parents := txidSetKeys(entry.Parents)
		sortTxIDs(parents)
		for _, parent := range parents {
			visit(parent)
		}
		ordered = append(ordered, entry)
	}
	visit(txid)
	return ordered
}

func (p *Pool) ensureSelectionLocked() {
	if p.selection != nil {
		return
	}
	p.rebuildSelectionLocked()
}

func (p *Pool) rebuildSelectionLocked() {
	frontier := candidateHeap{indexes: make(map[[32]byte]int, len(p.entries))}
	for _, entry := range p.entries {
		candidate := p.candidateForTxLocked(entry.TxID)
		if len(candidate.Entries) == 0 {
			continue
		}
		heap.Push(&frontier, candidate)
	}
	p.selection = &selectionCache{
		epoch:    p.epoch,
		frontier: frontier,
	}
}

func (p *Pool) upsertSelectionCandidateLocked(txid [32]byte) {
	p.ensureSelectionLocked()
	p.selection.snapshot = nil
	candidate := p.candidateForTxLocked(txid)
	if len(candidate.Entries) == 0 {
		p.selection.frontier.remove(txid)
		return
	}
	p.selection.frontier.upsert(candidate)
}

func (p *Pool) removeSelectionCandidatesLocked(remove map[[32]byte]struct{}) {
	if p.selection == nil || len(remove) == 0 {
		return
	}
	p.selection.snapshot = nil
	for txid := range remove {
		p.selection.frontier.remove(txid)
	}
}

func (p *Pool) candidateForTxLocked(txid [32]byte) packageCandidate {
	entries := p.packageForSelection(txid)
	candidate := packageCandidate{
		TxID:    txid,
		Entries: entries,
	}
	for _, entry := range entries {
		candidate.Fee += entry.Fee
		candidate.Size += entry.Size
	}
	return candidate
}

func filterCandidateEntriesInto(scratch *selectionScratch, candidate packageCandidateView, included map[[32]byte]struct{}) ([]SnapshotEntry, int, uint64) {
	filtered := scratch.filtered[:0]
	size := 0
	var fee uint64
	for _, entry := range candidate.Entries {
		if _, ok := included[entry.TxID]; ok {
			continue
		}
		filtered = append(filtered, entry)
		size += entry.Size
		fee += entry.Fee
	}
	scratch.filtered = filtered
	return filtered, size, fee
}

func applyCandidatePackageSnapshot(preBlockUtxos consensus.UtxoLookup, currentUtxos *consensus.UtxoOverlay, claimed map[types.OutPoint]struct{}, entries []SnapshotEntry, scratch *selectionScratch, context consensus.TxValidationContext, _ consensus.ConsensusRules) (uint64, bool) {
	defer scratch.resetSeenPackageClaims()
	createdInPackage := make(map[types.OutPoint]struct{})
	for _, entry := range entries {
		for _, spent := range entry.SpentOutPoints {
			if _, ok := claimed[spent]; ok {
				return 0, false
			}
			if _, ok := scratch.seenPackageClaims[spent]; ok {
				return 0, false
			}
			scratch.seenPackageClaims[spent] = struct{}{}
			scratch.seenPackageClaimOrder = append(scratch.seenPackageClaimOrder, spent)
			if coin, ok := preBlockUtxos(spent); ok {
				if coin.Coinbase && (context.SpendHeight < coin.CreatedHeight || context.SpendHeight-coin.CreatedHeight < context.Params.CoinbaseMaturity) {
					return 0, false
				}
				continue
			}
			if _, ok := currentUtxos.Lookup(spent); ok {
				continue
			}
			if _, ok := createdInPackage[spent]; !ok {
				return 0, false
			}
		}
		for _, leaf := range entry.CreatedLeaves {
			createdInPackage[leaf.OutPoint] = struct{}{}
		}
	}
	// Selection runs only over mempool entries that already passed admission
	// validation. Rechecking signatures here made template build crypto-bound,
	// while full block validation still performs the authoritative verification
	// before any locally mined block can be accepted.
	var totalFees uint64
	for _, entry := range entries {
		totalFees += entry.Summary.Fee
		for _, spent := range entry.SpentOutPoints {
			claimed[spent] = struct{}{}
		}
		applyTxNoUndo(currentUtxos, entry.SpentOutPoints, entry.CreatedLeaves)
	}
	return totalFees, true
}

func applyTxNoUndo(utxos *consensus.UtxoOverlay, spent []types.OutPoint, created []utreexo.UtxoLeaf) {
	for _, outPoint := range spent {
		utxos.Spend(outPoint)
	}
	for _, leaf := range created {
		utxos.Set(leaf.OutPoint, consensus.UtxoEntryFromLeaf(leaf))
	}
}

func selectionSnapshotForEntry(entry *Entry) SnapshotEntry {
	snapshot := snapshotForEntry(entry)
	// Admission prepares these deltas once and treats them as immutable for the
	// lifetime of the entry. Selection snapshots can therefore share the backing
	// slices instead of cloning every spend/create vector on each frontier rebuild.
	snapshot.SpentOutPoints = entry.SpentOutPoints
	snapshot.CreatedLeaves = entry.CreatedLeaves
	return snapshot
}

func (p *Pool) runSelectionSnapshot(snapshot selectionSnapshot, scratch *selectionScratch, included map[[32]byte]struct{}, tryInclude func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool) {
	if len(snapshot.candidates) == 0 {
		return
	}
	if !snapshot.hasSharedEntries {
		for _, candidate := range snapshot.candidates {
			filtered, filteredSize, filteredFee := filterCandidateEntriesInto(scratch, candidate, included)
			if len(filtered) == 0 {
				continue
			}
			if !tryInclude(filtered, filteredSize, filteredFee) {
				continue
			}
		}
		return
	}
	queued, completed, queue := scratch.prepareWakeState(len(snapshot.candidates))
	for idx := range snapshot.candidates {
		queue = append(queue, idx)
		queued[idx] = true
	}
	for head := 0; head < len(queue); head++ {
		idx := queue[head]
		queued[idx] = false
		if completed[idx] {
			continue
		}
		candidate := snapshot.candidates[idx]
		filtered, filteredSize, filteredFee := filterCandidateEntriesInto(scratch, candidate, included)
		if len(filtered) == 0 {
			completed[idx] = true
			continue
		}
		if !tryInclude(filtered, filteredSize, filteredFee) {
			continue
		}
		completed[idx] = true
		for _, entry := range filtered {
			for _, wakeIdx := range snapshot.wakeByEntry[entry.TxID] {
				if completed[wakeIdx] || queued[wakeIdx] {
					continue
				}
				queue = append(queue, wakeIdx)
				queued[wakeIdx] = true
			}
		}
	}
	scratch.queue = queue
}

func candidateLess(left, right packageCandidate) bool {
	leftSize := uint64(left.Size)
	rightSize := uint64(right.Size)
	leftHi, leftLo := bits.Mul64(left.Fee, rightSize)
	rightHi, rightLo := bits.Mul64(right.Fee, leftSize)
	if leftHi != rightHi {
		return leftHi > rightHi
	}
	if leftLo != rightLo {
		return leftLo > rightLo
	}
	if left.Fee != right.Fee {
		return left.Fee > right.Fee
	}
	return bytes.Compare(left.TxID[:], right.TxID[:]) < 0
}

func (h candidateHeap) Len() int {
	return len(h.items)
}

func (h candidateHeap) Less(i, j int) bool {
	return candidateLess(h.items[i], h.items[j])
}

func (h candidateHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.indexes[h.items[i].TxID] = i
	h.indexes[h.items[j].TxID] = j
}

func (h *candidateHeap) Push(x any) {
	candidate := x.(packageCandidate)
	h.items = append(h.items, candidate)
	h.indexes[candidate.TxID] = len(h.items) - 1
}

func (h *candidateHeap) Pop() any {
	last := len(h.items) - 1
	candidate := h.items[last]
	h.items = h.items[:last]
	delete(h.indexes, candidate.TxID)
	return candidate
}

func (h *candidateHeap) ordered() []packageCandidate {
	ordered := append([]packageCandidate(nil), h.items...)
	sort.Slice(ordered, func(i, j int) bool {
		return candidateLess(ordered[i], ordered[j])
	})
	return ordered
}

func (h *candidateHeap) remove(txid [32]byte) {
	idx, ok := h.indexes[txid]
	if !ok {
		return
	}
	heap.Remove(h, idx)
}

func (h *candidateHeap) upsert(candidate packageCandidate) {
	if idx, ok := h.indexes[candidate.TxID]; ok {
		prev := h.items[idx]
		h.items[idx] = candidate
		if candidateLess(candidate, prev) || candidateLess(prev, candidate) {
			heap.Fix(h, idx)
		}
		return
	}
	heap.Push(h, candidate)
}
