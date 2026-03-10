package mempool

import (
	"bytes"
	"container/heap"
	"errors"
	"sort"
	"sync"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

var (
	ErrTxAlreadyExists    = errors.New("transaction already exists")
	ErrInputAlreadySpent  = errors.New("input already spent in mempool")
	ErrCoinbaseTx         = errors.New("coinbase transactions are not admitted to mempool")
	ErrTxTooLarge         = errors.New("transaction exceeds mempool policy size limit")
	ErrRelayFeeTooLow     = errors.New("transaction fee below relay policy floor")
	ErrTooManyAncestors   = errors.New("transaction exceeds mempool ancestor limit")
	ErrTooManyDescendants = errors.New("transaction exceeds mempool descendant limit")
)

type PoolConfig struct {
	MinRelayFeePerByte uint64
	MaxTxSize          int
	MaxAncestors       int
	MaxDescendants     int
	MaxOrphans         int
}

func DefaultConfig() PoolConfig {
	return PoolConfig{
		MinRelayFeePerByte: 1,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         128,
	}
}

type Entry struct {
	Tx              types.Transaction
	TxID            [32]byte
	Summary         consensus.TxValidationSummary
	Fee             uint64
	Size            int
	AddedAt         uint64
	Parents         map[[32]byte]struct{}
	Children        map[[32]byte]struct{}
	AncestorCount   int
	AncestorSize    int
	AncestorFees    uint64
	DescendantCount int
	DescendantSize  int
	DescendantFees  uint64
}

type SnapshotEntry struct {
	Tx              types.Transaction
	TxID            [32]byte
	Summary         consensus.TxValidationSummary
	Fee             uint64
	Size            int
	AddedAt         uint64
	AncestorCount   int
	AncestorSize    int
	AncestorFees    uint64
	DescendantCount int
	DescendantSize  int
	DescendantFees  uint64
}

type AcceptedTx struct {
	Tx      types.Transaction
	TxID    [32]byte
	Summary consensus.TxValidationSummary
	Fee     uint64
	Size    int
}

type AdmissionSnapshot struct {
	Epoch   uint64
	Entries map[[32]byte]admissionEntry
	Spent   map[types.OutPoint][32]byte
	Orphans map[[32]byte]struct{}
}

// SharedAdmissionView exposes a read-locked view of live mempool admission
// state so parallel batch preparation can share one base view without cloning
// the full pool up front. Callers must Release the view when finished.
type SharedAdmissionView struct {
	pool  *Pool
	Epoch uint64
}

type PreparedAdmission struct {
	Tx            types.Transaction
	TxID          [32]byte
	Size          int
	Summary       consensus.TxValidationSummary
	Parents       map[[32]byte]struct{}
	Missing       map[types.OutPoint]struct{}
	View          consensus.UtxoSet
	SnapshotEpoch uint64
}

type Admission struct {
	TxID           [32]byte
	Summary        consensus.TxValidationSummary
	Accepted       []AcceptedTx
	Orphaned       bool
	EvictedOrphans int
}

type Stats struct {
	Count     int
	Orphans   int
	Bytes     int
	TotalFees uint64
	MedianFee uint64
	LowFee    uint64
	HighFee   uint64
}

type Pool struct {
	mu         sync.RWMutex
	cfg        PoolConfig
	entries    map[[32]byte]*Entry
	spent      map[types.OutPoint][32]byte
	orphans    map[[32]byte]*orphanEntry
	orphanDeps map[types.OutPoint][][32]byte
	sequence   uint64
	epoch      uint64
	selection  *selectionCache
	stats      poolStats
	topByFee   []SnapshotEntry
	topDirty   bool
}

type orphanEntry struct {
	Tx      types.Transaction
	TxID    [32]byte
	Size    int
	AddedAt uint64
	Missing map[types.OutPoint]struct{}
	// MissingCount lets orphan promotion become dependency-driven: newly
	// created outputs decrement unresolved inputs directly instead of forcing a
	// full missing-set rescan on every waiting orphan.
	MissingCount int
}

type admissionEntry struct {
	Tx              types.Transaction
	TxID            [32]byte
	Parents         map[[32]byte]struct{}
	DescendantCount int
}

type packageCandidate struct {
	TxID    [32]byte
	Entries []*Entry
	Fee     uint64
	Size    int
}

type selectionCache struct {
	epoch    uint64
	frontier candidateHeap
}

// candidateHeap keeps the package frontier incrementally ordered without
// reindexing the full tail on every insert or removal. Block assembly still
// materializes an ordered snapshot when it needs to walk the frontier end to
// end, but churn-heavy admission updates now stay O(log n).
type candidateHeap struct {
	items   []packageCandidate
	indexes map[[32]byte]int
}

type poolStats struct {
	bytes     int
	totalFees uint64
	feeCounts map[uint64]int
}

const topFeeCacheLimit = 8

func New() *Pool {
	return NewWithConfig(DefaultConfig())
}

func NewWithConfig(cfg PoolConfig) *Pool {
	return &Pool{
		cfg:        cfg,
		entries:    make(map[[32]byte]*Entry),
		spent:      make(map[types.OutPoint][32]byte),
		orphans:    make(map[[32]byte]*orphanEntry),
		orphanDeps: make(map[types.OutPoint][][32]byte),
		stats:      poolStats{feeCounts: make(map[uint64]int)},
	}
}

func (p *Pool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.entries)
}

func (p *Pool) Get(txid [32]byte) *types.Transaction {
	p.mu.RLock()
	defer p.mu.RUnlock()
	entry := p.entries[txid]
	if entry == nil {
		return nil
	}
	tx := entry.Tx
	return &tx
}

func (p *Pool) Contains(txid [32]byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.entries[txid]
	return ok
}

func (p *Pool) MissingTxIDs(txids [][32]byte) [][32]byte {
	if len(txids) == 0 {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	missing := make([][32]byte, 0, len(txids))
	for _, txid := range txids {
		if _, ok := p.entries[txid]; ok {
			continue
		}
		missing = append(missing, txid)
	}
	return missing
}

func (p *Pool) TransactionsByID(txids [][32]byte) []types.Transaction {
	if len(txids) == 0 {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	txs := make([]types.Transaction, 0, len(txids))
	for _, txid := range txids {
		entry := p.entries[txid]
		if entry == nil {
			continue
		}
		txs = append(txs, entry.Tx)
	}
	return txs
}

func (p *Pool) OrphanCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.orphans)
}

func (p *Pool) Snapshot() []SnapshotEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	entries := make([]SnapshotEntry, 0, len(p.entries))
	for _, entry := range p.entries {
		entries = append(entries, SnapshotEntry{
			Tx:              entry.Tx,
			TxID:            entry.TxID,
			Summary:         entry.Summary,
			Fee:             entry.Fee,
			Size:            entry.Size,
			AddedAt:         entry.AddedAt,
			AncestorCount:   entry.AncestorCount,
			AncestorSize:    entry.AncestorSize,
			AncestorFees:    entry.AncestorFees,
			DescendantCount: entry.DescendantCount,
			DescendantSize:  entry.DescendantSize,
			DescendantFees:  entry.DescendantFees,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].TxID[:], entries[j].TxID[:]) < 0
	})
	return entries
}

func (p *Pool) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := Stats{
		Count:     len(p.entries),
		Orphans:   len(p.orphans),
		Bytes:     p.stats.bytes,
		TotalFees: p.stats.totalFees,
	}
	if out.Count == 0 {
		return out
	}
	fees := make([]uint64, 0, len(p.stats.feeCounts))
	for fee := range p.stats.feeCounts {
		fees = append(fees, fee)
	}
	sort.Slice(fees, func(i, j int) bool { return fees[i] < fees[j] })
	out.LowFee = fees[0]
	out.HighFee = fees[len(fees)-1]
	target := out.Count / 2
	seen := 0
	for _, fee := range fees {
		seen += p.stats.feeCounts[fee]
		if seen > target {
			out.MedianFee = fee
			break
		}
	}
	return out
}

func (p *Pool) TopByFee(limit int) []SnapshotEntry {
	if limit <= 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureTopByFeeLocked(limit)
	if len(p.topByFee) < limit {
		limit = len(p.topByFee)
	}
	return append([]SnapshotEntry(nil), p.topByFee[:limit]...)
}

func (p *Pool) ShortIDMatches(shortIDFn func([32]byte) uint64, wanted map[uint64]struct{}) map[uint64][]types.Transaction {
	if len(wanted) == 0 {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	matches := make(map[uint64][]types.Transaction, len(wanted))
	for _, entry := range p.entries {
		shortID := shortIDFn(entry.TxID)
		if _, ok := wanted[shortID]; !ok {
			continue
		}
		matches[shortID] = append(matches[shortID], entry.Tx)
	}
	return matches
}

func (p *Pool) AdmissionSnapshot() AdmissionSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snapshot := AdmissionSnapshot{
		Epoch:   p.epoch,
		Entries: make(map[[32]byte]admissionEntry, len(p.entries)),
		Spent:   make(map[types.OutPoint][32]byte, len(p.spent)),
		Orphans: make(map[[32]byte]struct{}, len(p.orphans)),
	}
	for txid, entry := range p.entries {
		snapshot.Entries[txid] = admissionEntry{
			Tx:              entry.Tx,
			TxID:            entry.TxID,
			Parents:         copyTxIDSet(entry.Parents),
			DescendantCount: entry.DescendantCount,
		}
	}
	for outPoint, txid := range p.spent {
		snapshot.Spent[outPoint] = txid
	}
	for txid := range p.orphans {
		snapshot.Orphans[txid] = struct{}{}
	}
	return snapshot
}

func AdvanceAdmissionSnapshot(snapshot *AdmissionSnapshot, chainUtxos consensus.UtxoSet, accepted []AcceptedTx) error {
	if snapshot == nil {
		return nil
	}
	chainLookup := consensus.LookupFromSet(chainUtxos)
	for _, entry := range accepted {
		view, parents, missing, err := resolveInputsWithView(entry.Tx, chainLookup, snapshot.Entries, snapshot.Spent)
		if err != nil {
			return err
		}
		if len(missing) != 0 {
			return ErrTxAlreadyExists
		}
		_ = view
		ancestorIDs := gatherAncestorsForView(snapshot.Entries, parents)
		for ancestorID := range ancestorIDs {
			ancestor := snapshot.Entries[ancestorID]
			ancestor.DescendantCount++
			snapshot.Entries[ancestorID] = ancestor
		}
		snapshot.Entries[entry.TxID] = admissionEntry{
			Tx:              entry.Tx,
			TxID:            entry.TxID,
			Parents:         copyTxIDSet(parents),
			DescendantCount: 1,
		}
		for _, input := range entry.Tx.Base.Inputs {
			snapshot.Spent[input.PrevOut] = entry.TxID
		}
		delete(snapshot.Orphans, entry.TxID)
	}
	return nil
}

func (p *Pool) AcquireSharedAdmissionView() SharedAdmissionView {
	p.mu.RLock()
	return SharedAdmissionView{
		pool:  p,
		Epoch: p.epoch,
	}
}

func (v SharedAdmissionView) Release() {
	if v.pool != nil {
		v.pool.mu.RUnlock()
	}
}

func (p *Pool) PrepareAdmission(tx types.Transaction, snapshot AdmissionSnapshot, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	return p.prepareAdmission(tx, snapshot.Epoch, snapshot.Entries, snapshot.Spent, snapshot.Orphans, chainUtxos, rules)
}

func (p *Pool) PrepareAdmissionShared(tx types.Transaction, view SharedAdmissionView, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	if view.pool != p {
		return PreparedAdmission{}, errors.New("shared admission view belongs to different mempool")
	}
	txid := consensus.TxID(&tx)
	if _, ok := p.entries[txid]; ok {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if _, ok := p.orphans[txid]; ok {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if len(tx.Base.Inputs) == 0 {
		return PreparedAdmission{}, ErrCoinbaseTx
	}

	size := len(tx.Encode())
	if p.cfg.MaxTxSize > 0 && size > p.cfg.MaxTxSize {
		return PreparedAdmission{}, ErrTxTooLarge
	}

	liveView, parents, missing, err := p.resolveInputsAgainstLiveView(tx, consensus.LookupFromSet(chainUtxos))
	if err != nil {
		return PreparedAdmission{}, err
	}
	prepared := PreparedAdmission{
		Tx:            tx,
		TxID:          txid,
		Size:          size,
		Parents:       parents,
		Missing:       missing,
		View:          liveView,
		SnapshotEpoch: view.Epoch,
	}
	if len(missing) != 0 {
		return prepared, nil
	}

	summary, err := consensus.ValidateTx(&tx, liveView, rules)
	if err != nil {
		return PreparedAdmission{}, err
	}
	if !p.meetsRelayFloor(summary.Fee, size) {
		return PreparedAdmission{}, ErrRelayFeeTooLow
	}

	ancestorIDs := p.gatherAncestorsLiveView(parents)
	if p.cfg.MaxAncestors > 0 && len(ancestorIDs)+1 > p.cfg.MaxAncestors {
		return PreparedAdmission{}, ErrTooManyAncestors
	}
	if p.cfg.MaxDescendants > 0 {
		for ancestorID := range ancestorIDs {
			ancestor := p.entries[ancestorID]
			if ancestor != nil && ancestor.DescendantCount+1 > p.cfg.MaxDescendants {
				return PreparedAdmission{}, ErrTooManyDescendants
			}
		}
	}

	prepared.Summary = summary
	return prepared, nil
}

func (p *Pool) prepareAdmission(tx types.Transaction, epoch uint64, entries map[[32]byte]admissionEntry, spent map[types.OutPoint][32]byte, orphans map[[32]byte]struct{}, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	txid := consensus.TxID(&tx)
	if _, ok := entries[txid]; ok {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if _, ok := orphans[txid]; ok {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if len(tx.Base.Inputs) == 0 {
		return PreparedAdmission{}, ErrCoinbaseTx
	}

	size := len(tx.Encode())
	if p.cfg.MaxTxSize > 0 && size > p.cfg.MaxTxSize {
		return PreparedAdmission{}, ErrTxTooLarge
	}

	view, parents, missing, err := resolveInputsWithView(tx, consensus.LookupFromSet(chainUtxos), entries, spent)
	if err != nil {
		return PreparedAdmission{}, err
	}
	prepared := PreparedAdmission{
		Tx:            tx,
		TxID:          txid,
		Size:          size,
		Parents:       parents,
		Missing:       missing,
		View:          view,
		SnapshotEpoch: epoch,
	}
	if len(missing) != 0 {
		return prepared, nil
	}

	summary, err := consensus.ValidateTx(&tx, view, rules)
	if err != nil {
		return PreparedAdmission{}, err
	}
	if !p.meetsRelayFloor(summary.Fee, size) {
		return PreparedAdmission{}, ErrRelayFeeTooLow
	}

	ancestorIDs := gatherAncestorsForView(entries, parents)
	if p.cfg.MaxAncestors > 0 && len(ancestorIDs)+1 > p.cfg.MaxAncestors {
		return PreparedAdmission{}, ErrTooManyAncestors
	}
	if p.cfg.MaxDescendants > 0 {
		for ancestorID := range ancestorIDs {
			ancestor := entries[ancestorID]
			if ancestor.DescendantCount+1 > p.cfg.MaxDescendants {
				return PreparedAdmission{}, ErrTooManyDescendants
			}
		}
	}

	prepared.Summary = summary
	return prepared, nil
}

func (p *Pool) CommitPrepared(prepared PreparedAdmission, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (Admission, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.entries[prepared.TxID]; ok {
		return Admission{}, ErrTxAlreadyExists
	}
	if _, ok := p.orphans[prepared.TxID]; ok {
		return Admission{}, ErrTxAlreadyExists
	}
	if len(prepared.Tx.Base.Inputs) == 0 {
		return Admission{}, ErrCoinbaseTx
	}
	if p.cfg.MaxTxSize > 0 && prepared.Size > p.cfg.MaxTxSize {
		return Admission{}, ErrTxTooLarge
	}

	view, parents, missing, err := p.resolveInputs(prepared.Tx, consensus.LookupFromSet(chainUtxos))
	if err != nil {
		return Admission{}, err
	}
	if len(missing) != 0 {
		evicted := p.storeOrphan(prepared.Tx, prepared.TxID, prepared.Size, missing)
		p.bumpEpochLocked()
		return Admission{TxID: prepared.TxID, Orphaned: true, EvictedOrphans: evicted}, nil
	}

	accepted, err := p.insertPreparedLocked(prepared, view, parents, rules)
	if err != nil {
		return Admission{}, err
	}
	admission := Admission{
		TxID:     accepted.TxID,
		Summary:  accepted.Summary,
		Accepted: []AcceptedTx{accepted},
	}
	admission.Accepted = append(admission.Accepted, p.promoteReadyOrphans(outputsForTx(prepared.TxID, prepared.Tx), consensus.LookupFromSet(chainUtxos), rules)...)
	p.bumpEpochLocked()
	return admission, nil
}

func (p *Pool) AcceptTx(tx types.Transaction, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (Admission, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	txid := consensus.TxID(&tx)
	if _, ok := p.entries[txid]; ok {
		return Admission{}, ErrTxAlreadyExists
	}
	if _, ok := p.orphans[txid]; ok {
		return Admission{}, ErrTxAlreadyExists
	}
	if len(tx.Base.Inputs) == 0 {
		return Admission{}, ErrCoinbaseTx
	}

	size := len(tx.Encode())
	if p.cfg.MaxTxSize > 0 && size > p.cfg.MaxTxSize {
		return Admission{}, ErrTxTooLarge
	}

	view, parents, missing, err := p.resolveInputs(tx, consensus.LookupFromSet(chainUtxos))
	if err != nil {
		return Admission{}, err
	}
	if len(missing) != 0 {
		evicted := p.storeOrphan(tx, txid, size, missing)
		p.bumpEpochLocked()
		return Admission{TxID: txid, Orphaned: true, EvictedOrphans: evicted}, nil
	}

	accepted, err := p.insertResolved(tx, txid, size, view, parents, rules)
	if err != nil {
		return Admission{}, err
	}

	admission := Admission{
		TxID:     accepted.TxID,
		Summary:  accepted.Summary,
		Accepted: []AcceptedTx{accepted},
	}
	admission.Accepted = append(admission.Accepted, p.promoteReadyOrphans(outputsForTx(txid, tx), consensus.LookupFromSet(chainUtxos), rules)...)
	p.bumpEpochLocked()
	return admission, nil
}

func (p *Pool) SelectForBlock(chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64) {
	tempUtxos := cloneUtxos(chainUtxos)
	return p.SelectForBlockInPlace(tempUtxos, rules, maxTxBytes)
}

// SelectForBlockOverlay builds block candidates against an immutable base UTXO
// map and returns the post-selection overlay so callers can keep extending the
// tentative block state without materializing the full live set.
func (p *Pool) SelectForBlockOverlay(baseUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.selectForBlockLocked(baseUtxos, rules, maxTxBytes)
}

// SelectForBlockFromBase builds block candidates against an immutable base UTXO
// view and returns the post-selection UTXO set without mutating the caller's
// published chain map.
func (p *Pool) SelectForBlockFromBase(baseUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64, consensus.UtxoSet) {
	selected, totalFees, overlay := p.SelectForBlockOverlay(baseUtxos, rules, maxTxBytes)
	return selected, totalFees, overlay.Materialize()
}

// SelectForBlockInPlace consumes a mutable chain view and advances it as eligible
// transactions are accepted. Eligibility is still checked against the original
// pre-block UTXO set so block assembly cannot create same-block dependency edges.
func (p *Pool) SelectForBlockInPlace(currentUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64) {
	selected, totalFees, overlay := p.SelectForBlockOverlay(currentUtxos, rules, maxTxBytes)
	finalUtxos := overlay.Materialize()
	for outPoint := range currentUtxos {
		delete(currentUtxos, outPoint)
	}
	for outPoint, entry := range finalUtxos {
		currentUtxos[outPoint] = entry
	}
	return selected, totalFees
}

func (p *Pool) selectForBlockLocked(baseUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	preBlockUtxos := consensus.LookupFromSet(baseUtxos)
	overlay := consensus.NewUtxoOverlay(baseUtxos)
	selected := make([]SnapshotEntry, 0, len(p.entries))
	included := make(map[[32]byte]struct{}, len(p.entries))
	claimed := make(map[types.OutPoint]struct{})
	usedBytes := 0
	var totalFees uint64

	pending := append([]packageCandidate(nil), p.cachedPackageCandidatesLocked()...)
	for len(pending) > 0 {
		next := make([]packageCandidate, 0, len(pending))
		progress := false
		for _, candidate := range pending {
			filtered := filterCandidateEntries(candidate.Entries, included)
			if len(filtered) == 0 {
				continue
			}
			filteredSize, filteredFee := packageStats(filtered)
			if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
				next = append(next, candidate)
				continue
			}
			if !p.meetsRelayFloor(filteredFee, filteredSize) {
				continue
			}

			packageFees, ok := applyCandidatePackage(preBlockUtxos, overlay, claimed, filtered, rules)
			if !ok {
				next = append(next, candidate)
				continue
			}

			usedBytes += filteredSize
			totalFees += packageFees
			for _, entry := range filtered {
				included[entry.TxID] = struct{}{}
				selected = append(selected, snapshotForEntry(entry))
			}
			progress = true
		}
		if !progress {
			break
		}
		pending = next
	}

	sortSnapshotEntriesByTxID(selected)
	return selected, totalFees, overlay
}

// AppendForBlockOverlay extends a previously-built tentative block state
// without forcing the caller to materialize a full post-selection UTXO map.
func (p *Pool) AppendForBlockOverlay(preBlockUtxos consensus.UtxoSet, currentUtxos *consensus.UtxoOverlay, rules consensus.ConsensusRules, maxTxBytes int, selected []SnapshotEntry) ([]SnapshotEntry, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	preBlockLookup := consensus.LookupFromSet(preBlockUtxos)
	appendOnly := make([]SnapshotEntry, 0, len(p.entries))
	included := make(map[[32]byte]struct{}, len(selected))
	claimed := claimedOutPointsForSnapshots(selected)
	usedBytes := 0
	for _, entry := range selected {
		included[entry.TxID] = struct{}{}
		usedBytes += entry.Size
	}
	var totalFees uint64

	pending := append([]packageCandidate(nil), p.cachedPackageCandidatesLocked()...)
	for len(pending) > 0 {
		next := make([]packageCandidate, 0, len(pending))
		progress := false
		for _, candidate := range pending {
			filtered := filterCandidateEntries(candidate.Entries, included)
			if len(filtered) == 0 {
				continue
			}
			filteredSize, filteredFee := packageStats(filtered)
			if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
				next = append(next, candidate)
				continue
			}
			if !p.meetsRelayFloor(filteredFee, filteredSize) {
				continue
			}

			packageFees, ok := applyCandidatePackage(preBlockLookup, currentUtxos, claimed, filtered, rules)
			if !ok {
				next = append(next, candidate)
				continue
			}

			usedBytes += filteredSize
			totalFees += packageFees
			for _, entry := range filtered {
				included[entry.TxID] = struct{}{}
				appendOnly = append(appendOnly, snapshotForEntry(entry))
			}
			progress = true
		}
		if !progress {
			break
		}
		pending = next
	}

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

func (p *Pool) ContainsAll(entries []SnapshotEntry) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, entry := range entries {
		if _, ok := p.entries[entry.TxID]; !ok {
			return false
		}
	}
	return true
}

func (p *Pool) RemoveConfirmed(block *types.Block) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if block == nil {
		return
	}
	roots := make(map[[32]byte]struct{})
	confirmedSpends := make(map[types.OutPoint]struct{})
	for i := 1; i < len(block.Txs); i++ {
		tx := block.Txs[i]
		txid := consensus.TxID(&tx)
		if _, ok := p.entries[txid]; ok {
			roots[txid] = struct{}{}
		}
		for _, input := range tx.Base.Inputs {
			confirmedSpends[input.PrevOut] = struct{}{}
		}
	}
	for txid, entry := range p.entries {
		for _, input := range entry.Tx.Base.Inputs {
			if _, ok := confirmedSpends[input.PrevOut]; ok {
				roots[txid] = struct{}{}
				break
			}
		}
	}
	p.removeRecursive(roots)
	p.bumpEpochLocked()
}

// PromoteReadyOrphansForBlock rechecks orphan transactions that were waiting on
// outputs created by the newly accepted block.
func (p *Pool) PromoteReadyOrphansForBlock(block *types.Block, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) []AcceptedTx {
	p.mu.Lock()
	defer p.mu.Unlock()
	if block == nil {
		return nil
	}
	outputs := make([]types.OutPoint, 0)
	for _, tx := range block.Txs {
		txid := consensus.TxID(&tx)
		outputs = append(outputs, outputsForTx(txid, tx)...)
	}
	if len(outputs) == 0 {
		return nil
	}
	promoted := p.promoteReadyOrphans(outputs, consensus.LookupFromSet(chainUtxos), rules)
	if len(promoted) > 0 {
		p.bumpEpochLocked()
	}
	return promoted
}

func (p *Pool) insertResolved(tx types.Transaction, txid [32]byte, size int, utxos consensus.UtxoSet, parents map[[32]byte]struct{}, rules consensus.ConsensusRules) (AcceptedTx, error) {
	summary, err := consensus.ValidateTx(&tx, utxos, rules)
	if err != nil {
		return AcceptedTx{}, err
	}
	return p.insertValidatedLocked(tx, txid, size, summary, parents)
}

func (p *Pool) insertPreparedLocked(prepared PreparedAdmission, utxos consensus.UtxoSet, parents map[[32]byte]struct{}, rules consensus.ConsensusRules) (AcceptedTx, error) {
	summary := prepared.Summary
	if len(prepared.Missing) != 0 || !sameTxIDSets(prepared.Parents, parents) || !sameUtxoSets(prepared.View, utxos) {
		validated, err := consensus.ValidateTx(&prepared.Tx, utxos, rules)
		if err != nil {
			return AcceptedTx{}, err
		}
		summary = validated
	}
	return p.insertValidatedLocked(prepared.Tx, prepared.TxID, prepared.Size, summary, parents)
}

func (p *Pool) insertValidatedLocked(tx types.Transaction, txid [32]byte, size int, summary consensus.TxValidationSummary, parents map[[32]byte]struct{}) (AcceptedTx, error) {
	if !p.meetsRelayFloor(summary.Fee, size) {
		return AcceptedTx{}, ErrRelayFeeTooLow
	}

	ancestorIDs := p.gatherAncestors(parents)
	if p.cfg.MaxAncestors > 0 && len(ancestorIDs)+1 > p.cfg.MaxAncestors {
		return AcceptedTx{}, ErrTooManyAncestors
	}
	if p.cfg.MaxDescendants > 0 {
		for ancestorID := range ancestorIDs {
			ancestor := p.entries[ancestorID]
			if ancestor != nil && ancestor.DescendantCount+1 > p.cfg.MaxDescendants {
				return AcceptedTx{}, ErrTooManyDescendants
			}
		}
	}

	ancestorSize := size
	ancestorFees := summary.Fee
	for ancestorID := range ancestorIDs {
		ancestor := p.entries[ancestorID]
		if ancestor == nil {
			continue
		}
		ancestorSize += ancestor.Size
		ancestorFees += ancestor.Fee
	}

	p.sequence++
	entry := &Entry{
		Tx:              tx,
		TxID:            txid,
		Summary:         summary,
		Fee:             summary.Fee,
		Size:            size,
		AddedAt:         p.sequence,
		Parents:         copyTxIDSet(parents),
		Children:        make(map[[32]byte]struct{}),
		AncestorCount:   len(ancestorIDs) + 1,
		AncestorSize:    ancestorSize,
		AncestorFees:    ancestorFees,
		DescendantCount: 1,
		DescendantSize:  size,
		DescendantFees:  summary.Fee,
	}
	p.entries[txid] = entry
	p.noteEntryAddedLocked(entry)
	for _, input := range tx.Base.Inputs {
		p.spent[input.PrevOut] = txid
	}
	for parentID := range parents {
		parent := p.entries[parentID]
		if parent == nil {
			continue
		}
		parent.Children[txid] = struct{}{}
	}
	for ancestorID := range ancestorIDs {
		ancestor := p.entries[ancestorID]
		if ancestor == nil {
			continue
		}
		ancestor.DescendantCount++
		ancestor.DescendantSize += size
		ancestor.DescendantFees += summary.Fee
	}
	p.upsertSelectionCandidateLocked(txid)
	return AcceptedTx{
		Tx:      tx,
		TxID:    txid,
		Summary: summary,
		Fee:     summary.Fee,
		Size:    size,
	}, nil
}

func (p *Pool) promoteReadyOrphans(outputs []types.OutPoint, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) []AcceptedTx {
	promoted := make([]AcceptedTx, 0)
	ready := make([][32]byte, 0)
	ready = p.enqueueReadyOrphansForOutputs(ready, outputs)
	for i := 0; i < len(ready); i++ {
		txid := ready[i]
		orphan := p.orphans[txid]
		if orphan == nil || orphan.MissingCount != 0 {
			continue
		}
		view, parents, missing, err := p.resolveInputs(orphan.Tx, chainLookup)
		if err != nil {
			p.deleteOrphan(txid)
			continue
		}
		if len(missing) != 0 {
			p.updateOrphanMissing(txid, missing)
			continue
		}

		p.deleteOrphan(txid)
		accepted, err := p.insertResolved(orphan.Tx, txid, orphan.Size, view, parents, rules)
		if err != nil {
			continue
		}
		promoted = append(promoted, accepted)
		ready = p.enqueueReadyOrphansForOutputs(ready, outputsForTx(txid, orphan.Tx))
	}

	return promoted
}

func (p *Pool) enqueueReadyOrphansForOutputs(ready [][32]byte, outputs []types.OutPoint) [][32]byte {
	for _, out := range outputs {
		waiting := p.orphanDeps[out]
		if len(waiting) == 0 {
			continue
		}
		delete(p.orphanDeps, out)
		for _, txid := range waiting {
			orphan := p.orphans[txid]
			if orphan == nil {
				continue
			}
			if _, ok := orphan.Missing[out]; !ok {
				continue
			}
			delete(orphan.Missing, out)
			orphan.MissingCount--
			if orphan.MissingCount == 0 {
				ready = append(ready, txid)
			}
		}
	}
	return ready
}

func (p *Pool) resolveInputs(tx types.Transaction, chainLookup consensus.UtxoLookup) (consensus.UtxoSet, map[[32]byte]struct{}, map[types.OutPoint]struct{}, error) {
	view := make(consensus.UtxoSet, len(tx.Base.Inputs))
	parents := make(map[[32]byte]struct{})
	missing := make(map[types.OutPoint]struct{})

	for _, input := range tx.Base.Inputs {
		if _, ok := p.spent[input.PrevOut]; ok {
			return nil, nil, nil, ErrInputAlreadySpent
		}
		if utxo, ok := chainLookup(input.PrevOut); ok {
			view[input.PrevOut] = utxo
			continue
		}

		parent := p.entries[input.PrevOut.TxID]
		if parent == nil {
			missing[input.PrevOut] = struct{}{}
			continue
		}
		if input.PrevOut.Vout >= uint32(len(parent.Tx.Base.Outputs)) {
			missing[input.PrevOut] = struct{}{}
			continue
		}

		output := parent.Tx.Base.Outputs[input.PrevOut.Vout]
		view[input.PrevOut] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		}
		parents[parent.TxID] = struct{}{}
	}

	return view, parents, missing, nil
}

func resolveInputsWithView(tx types.Transaction, chainLookup consensus.UtxoLookup, entries map[[32]byte]admissionEntry, spent map[types.OutPoint][32]byte) (consensus.UtxoSet, map[[32]byte]struct{}, map[types.OutPoint]struct{}, error) {
	view := make(consensus.UtxoSet, len(tx.Base.Inputs))
	parents := make(map[[32]byte]struct{})
	missing := make(map[types.OutPoint]struct{})

	for _, input := range tx.Base.Inputs {
		if _, ok := spent[input.PrevOut]; ok {
			return nil, nil, nil, ErrInputAlreadySpent
		}
		if utxo, ok := chainLookup(input.PrevOut); ok {
			view[input.PrevOut] = utxo
			continue
		}

		parent, ok := entries[input.PrevOut.TxID]
		if !ok {
			missing[input.PrevOut] = struct{}{}
			continue
		}
		if input.PrevOut.Vout >= uint32(len(parent.Tx.Base.Outputs)) {
			missing[input.PrevOut] = struct{}{}
			continue
		}

		output := parent.Tx.Base.Outputs[input.PrevOut.Vout]
		view[input.PrevOut] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		}
		parents[parent.TxID] = struct{}{}
	}

	return view, parents, missing, nil
}

func (p *Pool) resolveInputsAgainstLiveView(tx types.Transaction, chainLookup consensus.UtxoLookup) (consensus.UtxoSet, map[[32]byte]struct{}, map[types.OutPoint]struct{}, error) {
	view := make(consensus.UtxoSet, len(tx.Base.Inputs))
	parents := make(map[[32]byte]struct{})
	missing := make(map[types.OutPoint]struct{})

	for _, input := range tx.Base.Inputs {
		if _, ok := p.spent[input.PrevOut]; ok {
			return nil, nil, nil, ErrInputAlreadySpent
		}
		if utxo, ok := chainLookup(input.PrevOut); ok {
			view[input.PrevOut] = utxo
			continue
		}

		parent := p.entries[input.PrevOut.TxID]
		if parent == nil {
			missing[input.PrevOut] = struct{}{}
			continue
		}
		if input.PrevOut.Vout >= uint32(len(parent.Tx.Base.Outputs)) {
			missing[input.PrevOut] = struct{}{}
			continue
		}

		output := parent.Tx.Base.Outputs[input.PrevOut.Vout]
		view[input.PrevOut] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		}
		parents[parent.TxID] = struct{}{}
	}

	return view, parents, missing, nil
}

func (p *Pool) storeOrphan(tx types.Transaction, txid [32]byte, size int, missing map[types.OutPoint]struct{}) int {
	p.sequence++
	p.orphans[txid] = &orphanEntry{
		Tx:           tx,
		TxID:         txid,
		Size:         size,
		AddedAt:      p.sequence,
		Missing:      copyOutPointSet(missing),
		MissingCount: len(missing),
	}
	for out := range missing {
		p.orphanDeps[out] = append(p.orphanDeps[out], txid)
	}
	return p.trimOrphans()
}

func (p *Pool) trimOrphans() int {
	if p.cfg.MaxOrphans <= 0 || len(p.orphans) <= p.cfg.MaxOrphans {
		return 0
	}

	order := make([]*orphanEntry, 0, len(p.orphans))
	for _, orphan := range p.orphans {
		order = append(order, orphan)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].AddedAt != order[j].AddedAt {
			return order[i].AddedAt < order[j].AddedAt
		}
		return bytes.Compare(order[i].TxID[:], order[j].TxID[:]) < 0
	})

	evicted := 0
	for len(p.orphans) > p.cfg.MaxOrphans {
		p.deleteOrphan(order[evicted].TxID)
		evicted++
	}
	return evicted
}

func (p *Pool) deleteOrphan(txid [32]byte) {
	delete(p.orphans, txid)
}

func (p *Pool) updateOrphanMissing(txid [32]byte, missing map[types.OutPoint]struct{}) {
	orphan := p.orphans[txid]
	if orphan == nil {
		return
	}
	orphan.Missing = copyOutPointSet(missing)
	orphan.MissingCount = len(missing)
	for out := range orphan.Missing {
		p.orphanDeps[out] = append(p.orphanDeps[out], txid)
	}
}

func (p *Pool) removeRecursive(roots map[[32]byte]struct{}) {
	if len(roots) == 0 {
		return
	}
	remove := make(map[[32]byte]struct{})
	stack := make([][32]byte, 0, len(roots))
	for txid := range roots {
		if _, ok := p.entries[txid]; ok {
			stack = append(stack, txid)
		}
	}
	for len(stack) != 0 {
		txid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := remove[txid]; ok {
			continue
		}
		entry := p.entries[txid]
		if entry == nil {
			continue
		}
		remove[txid] = struct{}{}
		for child := range entry.Children {
			stack = append(stack, child)
		}
	}
	for txid := range remove {
		entry := p.entries[txid]
		if entry == nil {
			continue
		}
		for _, input := range entry.Tx.Base.Inputs {
			delete(p.spent, input.PrevOut)
		}
		for parentID := range entry.Parents {
			if _, ok := remove[parentID]; ok {
				continue
			}
			parent := p.entries[parentID]
			if parent != nil {
				delete(parent.Children, txid)
			}
		}
		for ancestorID := range p.gatherAncestors(entry.Parents) {
			if _, ok := remove[ancestorID]; ok {
				continue
			}
			ancestor := p.entries[ancestorID]
			if ancestor == nil {
				continue
			}
			ancestor.DescendantCount--
			ancestor.DescendantSize -= entry.Size
			ancestor.DescendantFees -= entry.Fee
		}
	}
	for txid := range remove {
		if entry := p.entries[txid]; entry != nil {
			p.noteEntryRemovedLocked(entry)
		}
		delete(p.entries, txid)
	}
	p.removeSelectionCandidatesLocked(remove)
}

func (p *Pool) Epoch() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.epoch
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

func (p *Pool) bumpEpochLocked() {
	p.epoch++
	if p.selection != nil {
		p.selection.epoch = p.epoch
	}
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

func (p *Pool) ensureTopByFeeLocked(limit int) {
	if !p.topDirty && len(p.topByFee) >= minInt(limit, len(p.entries)) {
		return
	}
	top := make([]SnapshotEntry, 0, minInt(limit, len(p.entries)))
	for _, entry := range p.entries {
		snapshot := snapshotForEntry(entry)
		insertAt := sort.Search(len(top), func(i int) bool {
			return topByFeeLess(snapshot, top[i])
		})
		if insertAt >= limit {
			continue
		}
		top = append(top, SnapshotEntry{})
		copy(top[insertAt+1:], top[insertAt:])
		top[insertAt] = snapshot
		if len(top) > limit {
			top = top[:limit]
		}
	}
	p.topByFee = top
	p.topDirty = false
}

func (p *Pool) noteEntryAddedLocked(entry *Entry) {
	p.stats.bytes += entry.Size
	p.stats.totalFees += entry.Fee
	p.stats.feeCounts[entry.Fee]++
	if p.topDirty {
		return
	}
	snapshot := snapshotForEntry(entry)
	insertAt := sort.Search(len(p.topByFee), func(i int) bool {
		return topByFeeLess(snapshot, p.topByFee[i])
	})
	if len(p.topByFee) >= topFeeCacheLimit && insertAt >= topFeeCacheLimit {
		return
	}
	p.topByFee = append(p.topByFee, SnapshotEntry{})
	copy(p.topByFee[insertAt+1:], p.topByFee[insertAt:])
	p.topByFee[insertAt] = snapshot
	if len(p.topByFee) > topFeeCacheLimit {
		p.topByFee = p.topByFee[:topFeeCacheLimit]
	}
}

func (p *Pool) noteEntryRemovedLocked(entry *Entry) {
	p.stats.bytes -= entry.Size
	p.stats.totalFees -= entry.Fee
	if count := p.stats.feeCounts[entry.Fee] - 1; count > 0 {
		p.stats.feeCounts[entry.Fee] = count
	} else {
		delete(p.stats.feeCounts, entry.Fee)
	}
	for i := range p.topByFee {
		if p.topByFee[i].TxID != entry.TxID {
			continue
		}
		p.topByFee = append(p.topByFee[:i], p.topByFee[i+1:]...)
		p.topDirty = true
		return
	}
}

func (p *Pool) upsertSelectionCandidateLocked(txid [32]byte) {
	p.ensureSelectionLocked()
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

func filterCandidateEntries(entries []*Entry, included map[[32]byte]struct{}) []*Entry {
	filtered := make([]*Entry, 0, len(entries))
	for _, entry := range entries {
		if _, ok := included[entry.TxID]; ok {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func packageStats(entries []*Entry) (int, uint64) {
	size := 0
	var fee uint64
	for _, entry := range entries {
		size += entry.Size
		fee += entry.Fee
	}
	return size, fee
}

type appliedTxUndo struct {
	spent   map[types.OutPoint]consensus.UtxoEntry
	created []types.OutPoint
}

func applyCandidatePackage(preBlockUtxos consensus.UtxoLookup, currentUtxos *consensus.UtxoOverlay, claimed map[types.OutPoint]struct{}, entries []*Entry, rules consensus.ConsensusRules) (uint64, bool) {
	undos := make([]appliedTxUndo, 0, len(entries))
	newlyClaimed := make([]types.OutPoint, 0, len(entries))
	var totalFees uint64
	for _, entry := range entries {
		for _, input := range entry.Tx.Base.Inputs {
			if _, ok := claimed[input.PrevOut]; ok {
				rollbackAppliedPackage(currentUtxos, undos)
				rollbackClaimedInputs(claimed, newlyClaimed)
				return 0, false
			}
		}
		summary, err := consensus.ValidateTxWithLookup(&entry.Tx, preBlockUtxos, rules)
		if err != nil {
			rollbackAppliedPackage(currentUtxos, undos)
			rollbackClaimedInputs(claimed, newlyClaimed)
			return 0, false
		}
		totalFees += summary.Fee
		for _, input := range entry.Tx.Base.Inputs {
			claimed[input.PrevOut] = struct{}{}
			newlyClaimed = append(newlyClaimed, input.PrevOut)
		}
		undos = append(undos, applyTxWithUndo(currentUtxos, entry.Tx, entry.TxID))
	}
	return totalFees, true
}

func applyTxWithUndo(utxos *consensus.UtxoOverlay, tx types.Transaction, txid [32]byte) appliedTxUndo {
	undo := appliedTxUndo{
		spent:   make(map[types.OutPoint]consensus.UtxoEntry, len(tx.Base.Inputs)),
		created: make([]types.OutPoint, 0, len(tx.Base.Outputs)),
	}
	for _, input := range tx.Base.Inputs {
		if entry, ok := utxos.Lookup(input.PrevOut); ok {
			undo.spent[input.PrevOut] = entry
		}
		utxos.Spend(input.PrevOut)
	}
	for vout, output := range tx.Base.Outputs {
		out := types.OutPoint{TxID: txid, Vout: uint32(vout)}
		utxos.Set(out, consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		})
		undo.created = append(undo.created, out)
	}
	return undo
}

func rollbackAppliedPackage(utxos *consensus.UtxoOverlay, undos []appliedTxUndo) {
	for i := len(undos) - 1; i >= 0; i-- {
		undo := undos[i]
		for _, out := range undo.created {
			utxos.Spend(out)
		}
		for out, entry := range undo.spent {
			utxos.Restore(out, entry)
		}
	}
}

func rollbackClaimedInputs(claimed map[types.OutPoint]struct{}, inputs []types.OutPoint) {
	for i := len(inputs) - 1; i >= 0; i-- {
		delete(claimed, inputs[i])
	}
}

func (p *Pool) gatherAncestors(parents map[[32]byte]struct{}) map[[32]byte]struct{} {
	ancestors := make(map[[32]byte]struct{})
	stack := txidSetKeys(parents)
	for len(stack) != 0 {
		last := len(stack) - 1
		txid := stack[last]
		stack = stack[:last]
		if _, ok := ancestors[txid]; ok {
			continue
		}
		entry := p.entries[txid]
		if entry == nil {
			continue
		}
		ancestors[txid] = struct{}{}
		for parent := range entry.Parents {
			stack = append(stack, parent)
		}
	}
	return ancestors
}

func (p *Pool) gatherAncestorsLiveView(parents map[[32]byte]struct{}) map[[32]byte]struct{} {
	ancestors := make(map[[32]byte]struct{})
	stack := txidSetKeys(parents)
	for len(stack) != 0 {
		txid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := ancestors[txid]; ok {
			continue
		}
		entry := p.entries[txid]
		if entry == nil {
			continue
		}
		ancestors[txid] = struct{}{}
		for parentID := range entry.Parents {
			stack = append(stack, parentID)
		}
	}
	return ancestors
}

func gatherAncestorsForView(entries map[[32]byte]admissionEntry, parents map[[32]byte]struct{}) map[[32]byte]struct{} {
	ancestors := make(map[[32]byte]struct{})
	stack := txidSetKeys(parents)
	for len(stack) != 0 {
		last := len(stack) - 1
		txid := stack[last]
		stack = stack[:last]
		if _, ok := ancestors[txid]; ok {
			continue
		}
		entry, ok := entries[txid]
		if !ok {
			continue
		}
		ancestors[txid] = struct{}{}
		for parent := range entry.Parents {
			stack = append(stack, parent)
		}
	}
	return ancestors
}

func (p *Pool) reindex() {
	p.spent = make(map[types.OutPoint][32]byte, len(p.entries))
	for _, entry := range p.entries {
		entry.Parents = make(map[[32]byte]struct{})
		entry.Children = make(map[[32]byte]struct{})
		entry.AncestorCount = 1
		entry.AncestorSize = entry.Size
		entry.AncestorFees = entry.Fee
		entry.DescendantCount = 1
		entry.DescendantSize = entry.Size
		entry.DescendantFees = entry.Fee
	}

	for _, entry := range p.entries {
		for _, input := range entry.Tx.Base.Inputs {
			p.spent[input.PrevOut] = entry.TxID
			parent := p.entries[input.PrevOut.TxID]
			if parent == nil {
				continue
			}
			if input.PrevOut.Vout >= uint32(len(parent.Tx.Base.Outputs)) {
				continue
			}
			entry.Parents[parent.TxID] = struct{}{}
			parent.Children[entry.TxID] = struct{}{}
		}
	}

	ancestorMemo := make(map[[32]byte]map[[32]byte]struct{}, len(p.entries))
	var collectAncestors func([32]byte) map[[32]byte]struct{}
	collectAncestors = func(txid [32]byte) map[[32]byte]struct{} {
		if cached, ok := ancestorMemo[txid]; ok {
			return cached
		}
		entry := p.entries[txid]
		ancestors := make(map[[32]byte]struct{})
		if entry != nil {
			for parent := range entry.Parents {
				ancestors[parent] = struct{}{}
				for ancestor := range collectAncestors(parent) {
					ancestors[ancestor] = struct{}{}
				}
			}
		}
		ancestorMemo[txid] = ancestors
		return ancestors
	}

	descendantMemo := make(map[[32]byte]map[[32]byte]struct{}, len(p.entries))
	var collectDescendants func([32]byte) map[[32]byte]struct{}
	collectDescendants = func(txid [32]byte) map[[32]byte]struct{} {
		if cached, ok := descendantMemo[txid]; ok {
			return cached
		}
		entry := p.entries[txid]
		descendants := make(map[[32]byte]struct{})
		if entry != nil {
			for child := range entry.Children {
				descendants[child] = struct{}{}
				for descendant := range collectDescendants(child) {
					descendants[descendant] = struct{}{}
				}
			}
		}
		descendantMemo[txid] = descendants
		return descendants
	}

	for txid, entry := range p.entries {
		for ancestor := range collectAncestors(txid) {
			parent := p.entries[ancestor]
			if parent == nil {
				continue
			}
			entry.AncestorCount++
			entry.AncestorSize += parent.Size
			entry.AncestorFees += parent.Fee
		}
		for descendant := range collectDescendants(txid) {
			child := p.entries[descendant]
			if child == nil {
				continue
			}
			entry.DescendantCount++
			entry.DescendantSize += child.Size
			entry.DescendantFees += child.Fee
		}
	}
	p.rebuildSelectionLocked()
}

func (p *Pool) meetsRelayFloor(fee uint64, size int) bool {
	if p.cfg.MinRelayFeePerByte == 0 {
		return true
	}
	if size <= 0 {
		return false
	}
	return fee >= uint64(size)*p.cfg.MinRelayFeePerByte
}

func snapshotForEntry(entry *Entry) SnapshotEntry {
	return SnapshotEntry{
		Tx:              entry.Tx,
		TxID:            entry.TxID,
		Summary:         entry.Summary,
		Fee:             entry.Fee,
		Size:            entry.Size,
		AddedAt:         entry.AddedAt,
		AncestorCount:   entry.AncestorCount,
		AncestorSize:    entry.AncestorSize,
		AncestorFees:    entry.AncestorFees,
		DescendantCount: entry.DescendantCount,
		DescendantSize:  entry.DescendantSize,
		DescendantFees:  entry.DescendantFees,
	}
}

func candidateLess(left, right packageCandidate) bool {
	leftScore := float64(left.Fee) / float64(left.Size)
	rightScore := float64(right.Fee) / float64(right.Size)
	if leftScore != rightScore {
		return leftScore > rightScore
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

func outputsForTx(txid [32]byte, tx types.Transaction) []types.OutPoint {
	outputs := make([]types.OutPoint, 0, len(tx.Base.Outputs))
	for vout := range tx.Base.Outputs {
		outputs = append(outputs, types.OutPoint{TxID: txid, Vout: uint32(vout)})
	}
	return outputs
}

func applyTx(utxos consensus.UtxoSet, tx types.Transaction, txid [32]byte) {
	for _, input := range tx.Base.Inputs {
		delete(utxos, input.PrevOut)
	}
	for vout, output := range tx.Base.Outputs {
		utxos[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		}
	}
}

func sortSnapshotEntriesByTxID(entries []SnapshotEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].TxID[:], entries[j].TxID[:]) < 0
	})
}

func cloneUtxos(utxos consensus.UtxoSet) consensus.UtxoSet {
	out := make(consensus.UtxoSet, len(utxos))
	for outPoint, entry := range utxos {
		out[outPoint] = entry
	}
	return out
}

func claimedOutPointsForSnapshots(entries []SnapshotEntry) map[types.OutPoint]struct{} {
	claimed := make(map[types.OutPoint]struct{}, len(entries))
	for _, entry := range entries {
		for _, input := range entry.Tx.Base.Inputs {
			claimed[input.PrevOut] = struct{}{}
		}
	}
	return claimed
}

func copyOutPointSet(in map[types.OutPoint]struct{}) map[types.OutPoint]struct{} {
	out := make(map[types.OutPoint]struct{}, len(in))
	for item := range in {
		out[item] = struct{}{}
	}
	return out
}

func copyTxIDSet(in map[[32]byte]struct{}) map[[32]byte]struct{} {
	out := make(map[[32]byte]struct{}, len(in))
	for item := range in {
		out[item] = struct{}{}
	}
	return out
}

func sameTxIDSets(left, right map[[32]byte]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for item := range left {
		if _, ok := right[item]; !ok {
			return false
		}
	}
	return true
}

func sameUtxoSets(left, right consensus.UtxoSet) bool {
	if len(left) != len(right) {
		return false
	}
	for outPoint, leftEntry := range left {
		rightEntry, ok := right[outPoint]
		if !ok || rightEntry != leftEntry {
			return false
		}
	}
	return true
}

func txidSetKeys(in map[[32]byte]struct{}) [][32]byte {
	out := make([][32]byte, 0, len(in))
	for txid := range in {
		out = append(out, txid)
	}
	return out
}

func sortTxIDs(ids [][32]byte) {
	sort.Slice(ids, func(i, j int) bool {
		return bytes.Compare(ids[i][:], ids[j][:]) < 0
	})
}

func topByFeeLess(left, right SnapshotEntry) bool {
	switch {
	case left.Fee > right.Fee:
		return true
	case left.Fee < right.Fee:
		return false
	case left.Size < right.Size:
		return true
	case left.Size > right.Size:
		return false
	default:
		return bytes.Compare(left.TxID[:], right.TxID[:]) < 0
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
