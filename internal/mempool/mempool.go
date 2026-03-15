package mempool

import (
	"bytes"
	"container/heap"
	"errors"
	"math/bits"
	"sort"
	"sync"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

var (
	ErrTxAlreadyExists    = errors.New("transaction already exists")
	ErrInputAlreadySpent  = errors.New("input already spent in mempool")
	ErrCoinbaseTx         = errors.New("coinbase transactions are not admitted to mempool")
	ErrTxTooLarge         = errors.New("transaction exceeds mempool policy size limit")
	ErrRelayFeeTooLow     = errors.New("transaction fee below relay policy floor")
	ErrMempoolFull        = errors.New("transaction fee too low to enter full mempool")
	ErrTooManyAncestors   = errors.New("transaction exceeds mempool ancestor limit")
	ErrTooManyDescendants = errors.New("transaction exceeds mempool descendant limit")
)

type PoolConfig struct {
	MinRelayFeePerByte uint64
	MaxTxSize          int
	MaxMempoolBytes    int
	MaxAncestors       int
	MaxDescendants     int
	MaxOrphans         int
}

func DefaultConfig() PoolConfig {
	return PoolConfig{
		MinRelayFeePerByte: 1,
		MaxTxSize:          1_000_000,
		MaxMempoolBytes:    64 << 20,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         128,
	}
}

type Entry struct {
	Tx      types.Transaction
	TxID    [32]byte
	AuthID  [32]byte
	Summary consensus.TxValidationSummary
	// SignatureChecks are prepared once at mempool admission and retained on the
	// entry for future local reuse, but template selection itself now trusts
	// admission-validated mempool entries and does not re-run them on every
	// block build.
	SignatureChecks []crypto.SchnorrBatchItem
	// SpentOutPoints and CreatedLeaves are the per-tx accumulator delta. They
	// are prepared once at admission and copied only into block-selection
	// snapshots so template assembly can reuse them without rewalking raw tx IO.
	SpentOutPoints  []types.OutPoint
	CreatedLeaves   []utreexo.UtxoLeaf
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
	AuthID          [32]byte
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
	// SignatureChecks stay empty in snapshots. Template selection trusts
	// admission-validated mempool entries and full block validation still
	// performs real signature verification before acceptance.
	SignatureChecks []crypto.SchnorrBatchItem
	SpentOutPoints  []types.OutPoint
	CreatedLeaves   []utreexo.UtxoLeaf
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
	Tx              types.Transaction
	TxID            [32]byte
	Size            int
	Summary         consensus.TxValidationSummary
	SignatureChecks []crypto.SchnorrBatchItem
	Parents         map[[32]byte]struct{}
	Missing         map[types.OutPoint]struct{}
	View            consensus.UtxoSet
	SnapshotEpoch   uint64
	PreparedTip     [32]byte
	HasPreparedTip  bool
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
	mu          sync.RWMutex
	cfg         PoolConfig
	entries     map[[32]byte]*Entry
	spent       map[types.OutPoint][32]byte
	orphans     map[[32]byte]*orphanEntry
	orphanDeps  map[types.OutPoint][][32]byte
	sequence    uint64
	epoch       uint64
	selection   *selectionCache
	stats       poolStats
	cachedStats Stats
	statsDirty  bool
	topByFee    []SnapshotEntry
	topDirty    bool
	snapshot    []SnapshotEntry
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

type evictionPackage struct {
	Root  [32]byte
	TxIDs map[[32]byte]struct{}
	Fee   uint64
	Size  int
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
		statsDirty: true,
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
	if p.snapshot != nil {
		cached := append([]SnapshotEntry(nil), p.snapshot...)
		p.mu.RUnlock()
		return cached
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snapshot == nil {
		p.snapshot = p.snapshotLocked()
	}
	return append([]SnapshotEntry(nil), p.snapshot...)
}

// SnapshotShared returns the current mempool snapshot in deterministic txid
// order without copying the cached slice again. Callers must treat the result
// as immutable.
func (p *Pool) SnapshotShared() []SnapshotEntry {
	p.mu.RLock()
	if p.snapshot != nil {
		cached := p.snapshot
		p.mu.RUnlock()
		return cached
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snapshot == nil {
		p.snapshot = p.snapshotLocked()
	}
	return p.snapshot
}

func (p *Pool) Stats() Stats {
	p.mu.RLock()
	if !p.statsDirty {
		cached := p.cachedStats
		p.mu.RUnlock()
		return cached
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statsDirty {
		p.recomputeStatsLocked()
	}
	return p.cachedStats
}

func (p *Pool) recomputeStatsLocked() {
	out := Stats{
		Count:     len(p.entries),
		Orphans:   len(p.orphans),
		Bytes:     p.stats.bytes,
		TotalFees: p.stats.totalFees,
	}
	if out.Count == 0 {
		p.cachedStats = out
		p.statsDirty = false
		return
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
	p.cachedStats = out
	p.statsDirty = false
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
	return AdvanceAdmissionSnapshotWithLookup(snapshot, consensus.LookupFromSet(chainUtxos), accepted)
}

func AdvanceAdmissionSnapshotWithLookup(snapshot *AdmissionSnapshot, chainLookup consensus.UtxoLookup, accepted []AcceptedTx) error {
	if snapshot == nil {
		return nil
	}
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
	return p.PrepareAdmissionWithLookup(tx, snapshot, consensus.LookupFromSet(chainUtxos), rules)
}

func (p *Pool) PrepareAdmissionWithLookup(tx types.Transaction, snapshot AdmissionSnapshot, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	return p.prepareAdmission(tx, snapshot.Epoch, snapshot.Entries, snapshot.Spent, snapshot.Orphans, chainLookup, rules)
}

func (p *Pool) PrepareAdmissionShared(tx types.Transaction, view SharedAdmissionView, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	return p.PrepareAdmissionSharedWithLookup(tx, view, consensus.LookupFromSet(chainUtxos), rules)
}

func (p *Pool) PrepareAdmissionSharedWithLookup(tx types.Transaction, view SharedAdmissionView, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) (PreparedAdmission, error) {
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

	liveView, parents, missing, err := p.resolveInputsAgainstLiveView(tx, chainLookup)
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

	txValidation, err := consensus.PrepareTxValidationWithLookup(&tx, consensus.LookupFromSet(liveView), rules)
	if err != nil {
		return PreparedAdmission{}, err
	}
	summary, err := consensus.ValidatePreparedTx(txValidation)
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
	prepared.SignatureChecks = append([]crypto.SchnorrBatchItem(nil), txValidation.SignatureChecks...)
	return prepared, nil
}

func (p *Pool) prepareAdmission(tx types.Transaction, epoch uint64, entries map[[32]byte]admissionEntry, spent map[types.OutPoint][32]byte, orphans map[[32]byte]struct{}, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) (PreparedAdmission, error) {
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

	view, parents, missing, err := resolveInputsWithView(tx, chainLookup, entries, spent)
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

	txValidation, err := consensus.PrepareTxValidationWithLookup(&tx, consensus.LookupFromSet(view), rules)
	if err != nil {
		return PreparedAdmission{}, err
	}
	summary, err := consensus.ValidatePreparedTx(txValidation)
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
	prepared.SignatureChecks = append([]crypto.SchnorrBatchItem(nil), txValidation.SignatureChecks...)
	return prepared, nil
}

func (p *Pool) CommitPrepared(prepared PreparedAdmission, chainUtxos consensus.UtxoSet, chainTipHash [32]byte, rules consensus.ConsensusRules) (Admission, error) {
	return p.CommitPreparedWithLookup(prepared, consensus.LookupFromSet(chainUtxos), chainTipHash, rules)
}

func (p *Pool) CommitPreparedWithLookup(prepared PreparedAdmission, chainLookup consensus.UtxoLookup, chainTipHash [32]byte, rules consensus.ConsensusRules) (Admission, error) {
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

	if prepared.SnapshotEpoch == p.epoch && prepared.HasPreparedTip && prepared.PreparedTip == chainTipHash {
		if len(prepared.Missing) != 0 {
			evicted := p.storeOrphan(prepared.Tx, prepared.TxID, prepared.Size, prepared.Missing)
			p.bumpEpochLocked()
			return Admission{TxID: prepared.TxID, Orphaned: true, EvictedOrphans: evicted}, nil
		}
		accepted, err := p.insertValidatedLocked(prepared.Tx, prepared.TxID, prepared.Size, prepared.Summary, prepared.SignatureChecks, prepared.Parents)
		if err != nil {
			return Admission{}, err
		}
		admission := Admission{
			TxID:     accepted.TxID,
			Summary:  accepted.Summary,
			Accepted: []AcceptedTx{accepted},
		}
		admission.Accepted = append(admission.Accepted, p.promoteReadyOrphans(outputsForTx(prepared.TxID, prepared.Tx), chainLookup, rules)...)
		p.bumpEpochLocked()
		return admission, nil
	}

	view, parents, missing, err := p.resolveInputs(prepared.Tx, chainLookup)
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
	admission.Accepted = append(admission.Accepted, p.promoteReadyOrphans(outputsForTx(prepared.TxID, prepared.Tx), chainLookup, rules)...)
	p.bumpEpochLocked()
	return admission, nil
}

func (p *Pool) AcceptTx(tx types.Transaction, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (Admission, error) {
	return p.AcceptTxWithLookup(tx, consensus.LookupFromSet(chainUtxos), rules)
}

func (p *Pool) AcceptTxWithLookup(tx types.Transaction, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) (Admission, error) {
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

	view, parents, missing, err := p.resolveInputs(tx, chainLookup)
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
	admission.Accepted = append(admission.Accepted, p.promoteReadyOrphans(outputsForTx(txid, tx), chainLookup, rules)...)
	p.bumpEpochLocked()
	return admission, nil
}

// SelectForBlock preserves the caller's committed UTXO map and keeps selection
// state inside an overlay, so read-only callers do not pay for a full clone.
func (p *Pool) SelectForBlock(chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64) {
	selected, totalFees, _ := p.SelectForBlockOverlay(chainUtxos, rules, maxTxBytes)
	return selected, totalFees
}

func (p *Pool) SelectForBlockWithLookup(baseLookup consensus.UtxoLookup, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64) {
	selected, totalFees, _ := p.SelectForBlockOverlayWithLookup(baseLookup, rules, maxTxBytes)
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
func (p *Pool) SelectForBlockOverlayWithLookup(baseLookup consensus.UtxoLookup, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	p.mu.Lock()
	snapshot := p.selectionSnapshotLocked()
	p.mu.Unlock()
	return p.selectForBlockWithLookup(baseLookup, rules, maxTxBytes, snapshot)
}

func (p *Pool) selectForBlock(baseUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int, snapshot selectionSnapshot) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	preBlockUtxos := consensus.LookupFromSet(baseUtxos)
	overlay := consensus.NewUtxoOverlay(baseUtxos)
	selected := make([]SnapshotEntry, 0, len(snapshot.candidates))
	included := make(map[[32]byte]struct{}, len(snapshot.candidates))
	claimed := make(map[types.OutPoint]struct{})
	usedBytes := 0
	var totalFees uint64
	p.runSelectionSnapshot(snapshot, included, func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool {
		if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
			return false
		}
		if !p.meetsRelayFloor(filteredFee, filteredSize) {
			return false
		}
		packageFees, ok := applyCandidatePackageSnapshot(preBlockUtxos, overlay, claimed, filtered, rules)
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

func (p *Pool) selectForBlockWithLookup(baseLookup consensus.UtxoLookup, rules consensus.ConsensusRules, maxTxBytes int, snapshot selectionSnapshot) ([]SnapshotEntry, uint64, *consensus.UtxoOverlay) {
	preBlockUtxos := baseLookup
	overlay := consensus.NewUtxoOverlayWithLookup(consensus.LookupWithErrFromLookup(baseLookup))
	selected := make([]SnapshotEntry, 0, len(snapshot.candidates))
	included := make(map[[32]byte]struct{}, len(snapshot.candidates))
	claimed := make(map[types.OutPoint]struct{})
	usedBytes := 0
	var totalFees uint64
	p.runSelectionSnapshot(snapshot, included, func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool {
		if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
			return false
		}
		if !p.meetsRelayFloor(filteredFee, filteredSize) {
			return false
		}
		packageFees, ok := applyCandidatePackageSnapshot(preBlockUtxos, overlay, claimed, filtered, rules)
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
	return p.AppendForBlockOverlayWithLookup(consensus.LookupFromSet(preBlockUtxos), currentUtxos, rules, maxTxBytes, selected)
}

// AppendForBlockOverlayWithLookup extends a tentative block state against an
// immutable committed-chain lookup without forcing callers to materialize that
// base view as a full map first.
func (p *Pool) AppendForBlockOverlayWithLookup(preBlockLookup consensus.UtxoLookup, currentUtxos *consensus.UtxoOverlay, rules consensus.ConsensusRules, maxTxBytes int, selected []SnapshotEntry) ([]SnapshotEntry, uint64) {
	p.mu.Lock()
	snapshot := p.selectionSnapshotLocked()
	p.mu.Unlock()
	appendOnly := make([]SnapshotEntry, 0, len(snapshot.candidates))
	included := make(map[[32]byte]struct{}, len(selected))
	claimed := claimedOutPointsForSnapshots(selected)
	usedBytes := 0
	for _, entry := range selected {
		included[entry.TxID] = struct{}{}
		usedBytes += entry.Size
	}
	var totalFees uint64

	p.runSelectionSnapshot(snapshot, included, func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool {
		if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
			return false
		}
		if !p.meetsRelayFloor(filteredFee, filteredSize) {
			return false
		}
		packageFees, ok := applyCandidatePackageSnapshot(preBlockLookup, currentUtxos, claimed, filtered, rules)
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
	return p.PromoteReadyOrphansForBlockWithLookup(block, consensus.LookupFromSet(chainUtxos), rules)
}

func (p *Pool) PromoteReadyOrphansForBlockWithLookup(block *types.Block, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) []AcceptedTx {
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
	promoted := p.promoteReadyOrphans(outputs, chainLookup, rules)
	if len(promoted) > 0 {
		p.bumpEpochLocked()
	}
	return promoted
}

func (p *Pool) insertResolved(tx types.Transaction, txid [32]byte, size int, utxos consensus.UtxoSet, parents map[[32]byte]struct{}, rules consensus.ConsensusRules) (AcceptedTx, error) {
	txValidation, err := consensus.PrepareTxValidationWithLookup(&tx, consensus.LookupFromSet(utxos), rules)
	if err != nil {
		return AcceptedTx{}, err
	}
	summary, err := consensus.ValidatePreparedTx(txValidation)
	if err != nil {
		return AcceptedTx{}, err
	}
	return p.insertValidatedLocked(tx, txid, size, summary, append([]crypto.SchnorrBatchItem(nil), txValidation.SignatureChecks...), parents)
}

func (p *Pool) insertPreparedLocked(prepared PreparedAdmission, utxos consensus.UtxoSet, parents map[[32]byte]struct{}, rules consensus.ConsensusRules) (AcceptedTx, error) {
	summary := prepared.Summary
	signatureChecks := prepared.SignatureChecks
	if len(prepared.Missing) != 0 || !sameTxIDSets(prepared.Parents, parents) || !sameUtxoSets(prepared.View, utxos) || len(signatureChecks) == 0 {
		validated, err := consensus.PrepareTxValidationWithLookup(&prepared.Tx, consensus.LookupFromSet(utxos), rules)
		if err != nil {
			return AcceptedTx{}, err
		}
		summary, err = consensus.ValidatePreparedTx(validated)
		if err != nil {
			return AcceptedTx{}, err
		}
		signatureChecks = append([]crypto.SchnorrBatchItem(nil), validated.SignatureChecks...)
	}
	return p.insertValidatedLocked(prepared.Tx, prepared.TxID, prepared.Size, summary, signatureChecks, parents)
}

func (p *Pool) insertValidatedLocked(tx types.Transaction, txid [32]byte, size int, summary consensus.TxValidationSummary, signatureChecks []crypto.SchnorrBatchItem, parents map[[32]byte]struct{}) (AcceptedTx, error) {
	if !p.meetsRelayFloor(summary.Fee, size) {
		return AcceptedTx{}, ErrRelayFeeTooLow
	}
	if err := p.ensureMempoolCapacityLocked(summary.Fee, size, parents); err != nil {
		return AcceptedTx{}, err
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
		AuthID:          consensus.AuthID(&tx),
		Summary:         summary,
		SignatureChecks: append([]crypto.SchnorrBatchItem(nil), signatureChecks...),
		SpentOutPoints:  spentOutPointsForInputs(tx.Base.Inputs),
		CreatedLeaves:   createdLeavesForOutputs(txid, tx.Base.Outputs),
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
			PubKey:     output.PubKey,
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
			PubKey:     output.PubKey,
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
			PubKey:     output.PubKey,
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

// ensureMempoolCapacityLocked keeps the admitted transaction set within the
// configured byte cap by evicting the worst-fee descendant packages first. Any
// mempool parents required by the incoming transaction are protected so package
// admission cannot evict the chain the newcomer depends on.
func (p *Pool) ensureMempoolCapacityLocked(incomingFee uint64, incomingSize int, parents map[[32]byte]struct{}) error {
	maxBytes := p.cfg.MaxMempoolBytes
	if maxBytes <= 0 {
		return nil
	}
	if incomingSize > maxBytes {
		return ErrMempoolFull
	}
	usedBytes := p.stats.bytes
	if usedBytes+incomingSize <= maxBytes {
		return nil
	}

	protected := p.gatherAncestors(parents)
	for parentID := range parents {
		protected[parentID] = struct{}{}
	}

	bytesNeeded := usedBytes + incomingSize - maxBytes
	evict, freedBytes, freedFees, ok := p.planEvictionsLocked(bytesNeeded, protected)
	if !ok {
		return ErrMempoolFull
	}
	if compareFeeRates(incomingFee, incomingSize, freedFees, freedBytes) <= 0 {
		return ErrMempoolFull
	}
	p.removeRecursive(evict)
	return nil
}

func (p *Pool) planEvictionsLocked(bytesNeeded int, protected map[[32]byte]struct{}) (map[[32]byte]struct{}, int, uint64, bool) {
	evict := make(map[[32]byte]struct{})
	freedBytes := 0
	var freedFees uint64
	for freedBytes < bytesNeeded {
		candidate, ok := p.lowestFeeEvictionPackageLocked(evict, protected)
		if !ok {
			return nil, 0, 0, false
		}
		for txid := range candidate.TxIDs {
			evict[txid] = struct{}{}
		}
		freedBytes += candidate.Size
		freedFees += candidate.Fee
	}
	return evict, freedBytes, freedFees, true
}

func (p *Pool) lowestFeeEvictionPackageLocked(excluded, protected map[[32]byte]struct{}) (evictionPackage, bool) {
	var worst evictionPackage
	found := false
	for txid := range p.entries {
		if _, skip := excluded[txid]; skip {
			continue
		}
		candidate, ok := p.evictionPackageLocked(txid, excluded, protected)
		if !ok {
			continue
		}
		if !found || evictionPackageLess(candidate, worst) {
			worst = candidate
			found = true
		}
	}
	return worst, found
}

func (p *Pool) evictionPackageLocked(root [32]byte, excluded, protected map[[32]byte]struct{}) (evictionPackage, bool) {
	if _, skip := excluded[root]; skip {
		return evictionPackage{}, false
	}
	if _, keep := protected[root]; keep {
		return evictionPackage{}, false
	}
	entry := p.entries[root]
	if entry == nil {
		return evictionPackage{}, false
	}

	pkg := evictionPackage{
		Root:  root,
		TxIDs: make(map[[32]byte]struct{}),
	}
	stack := [][32]byte{root}
	for len(stack) != 0 {
		txid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, seen := pkg.TxIDs[txid]; seen {
			continue
		}
		if _, skip := excluded[txid]; skip {
			continue
		}
		if _, keep := protected[txid]; keep {
			return evictionPackage{}, false
		}
		entry := p.entries[txid]
		if entry == nil {
			continue
		}
		pkg.TxIDs[txid] = struct{}{}
		pkg.Size += entry.Size
		pkg.Fee += entry.Fee
		for child := range entry.Children {
			stack = append(stack, child)
		}
	}
	if pkg.Size <= 0 {
		return evictionPackage{}, false
	}
	return pkg, true
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

func (p *Pool) bumpEpochLocked() {
	p.epoch++
	p.snapshot = nil
	p.statsDirty = true
	if p.selection != nil {
		p.selection.epoch = p.epoch
	}
}

func (p *Pool) snapshotLocked() []SnapshotEntry {
	entries := make([]SnapshotEntry, 0, len(p.entries))
	for _, entry := range p.entries {
		entries = append(entries, snapshotForEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].TxID[:], entries[j].TxID[:]) < 0
	})
	return entries
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

func filterCandidateEntries(candidate packageCandidateView, included map[[32]byte]struct{}) ([]SnapshotEntry, int, uint64) {
	filtered := make([]SnapshotEntry, 0, len(candidate.Entries))
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
	return filtered, size, fee
}

func applyCandidatePackageSnapshot(preBlockUtxos consensus.UtxoLookup, currentUtxos *consensus.UtxoOverlay, claimed map[types.OutPoint]struct{}, entries []SnapshotEntry, _ consensus.ConsensusRules) (uint64, bool) {
	seenPackageClaims := make(map[types.OutPoint]struct{}, len(entries))
	for _, entry := range entries {
		for _, spent := range entry.SpentOutPoints {
			if _, ok := claimed[spent]; ok {
				return 0, false
			}
			if _, ok := seenPackageClaims[spent]; ok {
				return 0, false
			}
			seenPackageClaims[spent] = struct{}{}
			if _, ok := preBlockUtxos(spent); !ok {
				return 0, false
			}
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
		utxos.Set(leaf.OutPoint, consensus.UtxoEntry{
			ValueAtoms: leaf.ValueAtoms,
			PubKey:     leaf.PubKey,
		})
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
		AuthID:          entry.AuthID,
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

func selectionSnapshotForEntry(entry *Entry) SnapshotEntry {
	snapshot := snapshotForEntry(entry)
	snapshot.SpentOutPoints = append([]types.OutPoint(nil), entry.SpentOutPoints...)
	snapshot.CreatedLeaves = append([]utreexo.UtxoLeaf(nil), entry.CreatedLeaves...)
	return snapshot
}

func (p *Pool) runSelectionSnapshot(snapshot selectionSnapshot, included map[[32]byte]struct{}, tryInclude func(filtered []SnapshotEntry, filteredSize int, filteredFee uint64) bool) {
	if len(snapshot.candidates) == 0 {
		return
	}
	if !snapshot.hasSharedEntries {
		for _, candidate := range snapshot.candidates {
			filtered, filteredSize, filteredFee := filterCandidateEntries(candidate, included)
			if len(filtered) == 0 {
				continue
			}
			if !tryInclude(filtered, filteredSize, filteredFee) {
				continue
			}
		}
		return
	}
	queued := make([]bool, len(snapshot.candidates))
	completed := make([]bool, len(snapshot.candidates))
	queue := make([]int, 0, len(snapshot.candidates))
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
		filtered, filteredSize, filteredFee := filterCandidateEntries(candidate, included)
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
			PubKey:     output.PubKey,
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
		for _, spent := range entry.SpentOutPoints {
			claimed[spent] = struct{}{}
		}
	}
	return claimed
}

func spentOutPointsForInputs(inputs []types.TxInput) []types.OutPoint {
	if len(inputs) == 0 {
		return nil
	}
	spent := make([]types.OutPoint, 0, len(inputs))
	for _, input := range inputs {
		spent = append(spent, input.PrevOut)
	}
	return spent
}

func createdLeavesForOutputs(txid [32]byte, outputs []types.TxOutput) []utreexo.UtxoLeaf {
	if len(outputs) == 0 {
		return nil
	}
	created := make([]utreexo.UtxoLeaf, 0, len(outputs))
	for vout, output := range outputs {
		created = append(created, utreexo.UtxoLeaf{
			OutPoint:   types.OutPoint{TxID: txid, Vout: uint32(vout)},
			ValueAtoms: output.ValueAtoms,
			PubKey:     output.PubKey,
		})
	}
	return created
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

func evictionPackageLess(left, right evictionPackage) bool {
	switch compareFeeRates(left.Fee, left.Size, right.Fee, right.Size) {
	case -1:
		return true
	case 1:
		return false
	}
	switch {
	case left.Fee < right.Fee:
		return true
	case left.Fee > right.Fee:
		return false
	case left.Size < right.Size:
		return true
	case left.Size > right.Size:
		return false
	default:
		return bytes.Compare(left.Root[:], right.Root[:]) < 0
	}
}

func compareFeeRates(leftFee uint64, leftSize int, rightFee uint64, rightSize int) int {
	switch {
	case leftSize <= 0 && rightSize <= 0:
		return 0
	case leftSize <= 0:
		return -1
	case rightSize <= 0:
		return 1
	}
	leftHi, leftLo := bits.Mul64(leftFee, uint64(rightSize))
	rightHi, rightLo := bits.Mul64(rightFee, uint64(leftSize))
	switch {
	case leftHi < rightHi:
		return -1
	case leftHi > rightHi:
		return 1
	case leftLo < rightLo:
		return -1
	case leftLo > rightLo:
		return 1
	default:
		return 0
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
