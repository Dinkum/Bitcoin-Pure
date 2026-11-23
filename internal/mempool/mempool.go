package mempool

import (
	"bytes"
	"errors"
	"math/bits"
	"sort"
	"sync"

	"bitcoin-pure/internal/consensus"
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
	MaxOrphanBytes     int
}

func DefaultConfig() PoolConfig {
	return PoolConfig{
		MinRelayFeePerByte: 1,
		MaxTxSize:          1_000_000,
		MaxMempoolBytes:    64 << 20,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         128,
		MaxOrphanBytes:     defaultMaxOrphanBytes,
	}
}

type Entry struct {
	Tx      types.Transaction
	TxID    [32]byte
	AuthID  [32]byte
	Summary consensus.TxValidationSummary
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
	SpentOutPoints  []types.OutPoint
	CreatedLeaves   []utreexo.UtxoLeaf
}

type AcceptedTx struct {
	Tx      types.Transaction
	TxID    [32]byte
	AuthID  [32]byte
	Summary consensus.TxValidationSummary
	Fee     uint64
	Size    int
}

type RejectedTx struct {
	Tx   types.Transaction
	TxID [32]byte
	Err  error
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
	orphanBytes int
	sequence    uint64
	epoch       uint64
	selection   *selectionCache
	stats       poolStats
	cachedStats Stats
	statsDirty  bool
	topByFee    []SnapshotEntry
	topDirty    bool
	snapshot    []SnapshotEntry
	// The bounded mutation journal lets restart persistence scale with changed
	// records. persistenceFloor forces a full reconciliation after overflow.
	persistenceSeq   uint64
	persistenceFloor uint64
	persistenceLog   []persistenceMutation
}

type poolStats struct {
	bytes     int
	totalFees uint64
	feeCounts map[uint64]int
}

const (
	topFeeCacheLimit      = 8
	defaultMaxOrphanBytes = 8 << 20
)

func New() *Pool {
	return NewWithConfig(DefaultConfig())
}

func NewWithConfig(cfg PoolConfig) *Pool {
	if cfg.MaxOrphanBytes <= 0 {
		cfg.MaxOrphanBytes = defaultMaxOrphanBytes
	}
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
	out := make([]SnapshotEntry, 0, limit)
	for _, top := range p.topByFee[:limit] {
		out = append(out, snapshotForEntry(p.entries[top.TxID]))
	}
	return out
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
	confirmed := make(map[[32]byte]struct{})
	conflicts := make(map[[32]byte]struct{})
	confirmedSpends := make(map[types.OutPoint]struct{})
	for i := 1; i < len(block.Txs); i++ {
		tx := block.Txs[i]
		txid := consensus.TxID(&tx)
		if _, ok := p.entries[txid]; ok {
			confirmed[txid] = struct{}{}
		}
		for _, input := range tx.Base.Inputs {
			confirmedSpends[input.PrevOut] = struct{}{}
		}
	}
	// p.spent is the authoritative one-spender index. Looking up the block's
	// inputs directly keeps confirmation cleanup proportional to block work
	// instead of scanning the entire mempool after every accepted block.
	for outPoint := range confirmedSpends {
		spender, ok := p.spent[outPoint]
		if !ok {
			continue
		}
		if _, isConfirmed := confirmed[spender]; !isConfirmed {
			conflicts[spender] = struct{}{}
		}
	}
	remove := p.collectRecursive(conflicts)
	for txid := range confirmed {
		remove[txid] = struct{}{}
	}
	p.removeEntriesLocked(remove)
	p.bumpEpochLocked()
}

// RemoveContextuallyInvalid revalidates against chain and in-pool inputs.
// Reinsert disconnected parents before calling this so valid packages survive.
func (p *Pool) RemoveContextuallyInvalid(chainLookup consensus.UtxoLookup, context consensus.TxValidationContext, rules consensus.ConsensusRules) []RejectedTx {
	for range 2 {
		p.mu.RLock()
		epoch := p.epoch
		txs := p.contextTransactionsLocked()
		p.mu.RUnlock()

		reasons := validatePoolContext(txs, chainLookup, context, rules)
		p.mu.Lock()
		if p.epoch == epoch {
			rejected := p.removeContextuallyInvalidLocked(reasons)
			p.mu.Unlock()
			return rejected
		}
		p.mu.Unlock()
	}
	// Keep block transitions moving even if new admissions invalidate both
	// lock-free passes.
	p.mu.Lock()
	defer p.mu.Unlock()
	reasons := validatePoolContext(p.contextTransactionsLocked(), chainLookup, context, rules)
	return p.removeContextuallyInvalidLocked(reasons)
}

func (p *Pool) contextTransactionsLocked() map[[32]byte]types.Transaction {
	txs := make(map[[32]byte]types.Transaction, len(p.entries))
	for txid, entry := range p.entries {
		txs[txid] = entry.Tx
	}
	return txs
}

func validatePoolContext(txs map[[32]byte]types.Transaction, chainLookup consensus.UtxoLookup, context consensus.TxValidationContext, rules consensus.ConsensusRules) map[[32]byte]error {
	lookup := func(out types.OutPoint) (consensus.UtxoEntry, bool) {
		if coin, ok := chainLookup(out); ok {
			return coin, true
		}
		parent, ok := txs[out.TxID]
		if !ok || uint64(out.Vout) >= uint64(len(parent.Base.Outputs)) {
			return consensus.UtxoEntry{}, false
		}
		return consensus.UtxoEntryFromOutput(parent.Base.Outputs[out.Vout]), true
	}
	reasons := make(map[[32]byte]error)
	for txid, tx := range txs {
		prepared, err := consensus.PrepareTxValidationWithLookup(&tx, lookup, context, rules)
		if err == nil {
			_, err = consensus.ValidatePreparedTx(prepared)
		}
		if err != nil {
			reasons[txid] = err
		}
	}
	return reasons
}

func (p *Pool) removeContextuallyInvalidLocked(reasons map[[32]byte]error) []RejectedTx {
	// Reaccepted parents can recreate links to children left in the pool.
	p.reindex()
	if len(reasons) == 0 {
		p.bumpEpochLocked()
		return nil
	}
	roots := make([][32]byte, 0, len(reasons))
	for root := range reasons {
		roots = append(roots, root)
	}
	remove := make(map[[32]byte]struct{})
	for _, root := range roots {
		reason := reasons[root]
		for txid := range p.collectRecursive(map[[32]byte]struct{}{root: {}}) {
			remove[txid] = struct{}{}
			if _, ok := reasons[txid]; !ok {
				reasons[txid] = reason
			}
		}
	}
	rejected := make([]RejectedTx, 0, len(remove))
	for txid := range remove {
		if entry := p.entries[txid]; entry != nil {
			rejected = append(rejected, RejectedTx{Tx: entry.Tx, TxID: txid, Err: reasons[txid]})
		}
	}
	p.removeEntriesLocked(remove)
	p.bumpEpochLocked()
	return rejected
}

func (p *Pool) removeRecursive(roots map[[32]byte]struct{}) {
	remove := p.collectRecursive(roots)
	p.removeEntriesLocked(remove)
}

func (p *Pool) collectRecursive(roots map[[32]byte]struct{}) map[[32]byte]struct{} {
	remove := make(map[[32]byte]struct{})
	if len(roots) == 0 {
		return remove
	}
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
	return remove
}

func (p *Pool) removeEntriesLocked(remove map[[32]byte]struct{}) {
	if len(remove) == 0 {
		return
	}
	relink := false
	for txid := range remove {
		entry := p.entries[txid]
		if entry == nil {
			continue
		}
		for childID := range entry.Children {
			if _, removed := remove[childID]; !removed {
				relink = true
				break
			}
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
			p.notePersistenceMutationLocked(persistenceEntryMutation, txid)
		}
		delete(p.entries, txid)
	}
	// Surviving children may lose ancestors through multiple paths. Rebuild
	// their graph and package scores when a confirmation severs those links.
	if relink {
		p.reindex()
		return
	}
	p.removeSelectionCandidatesLocked(remove)
}

func (p *Pool) Epoch() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.epoch
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

func (p *Pool) reindex() {
	p.spent = make(map[types.OutPoint][32]byte, len(p.entries))
	for _, entry := range p.entries {
		entry.Parents = nil
		entry.Children = nil
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
			if entry.Parents == nil {
				entry.Parents = make(map[[32]byte]struct{})
			}
			entry.Parents[parent.TxID] = struct{}{}
			if parent.Children == nil {
				parent.Children = make(map[[32]byte]struct{})
			}
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

func outputsForTx(txid [32]byte, tx types.Transaction) []types.OutPoint {
	outputs := make([]types.OutPoint, 0, len(tx.Base.Outputs))
	for vout := range tx.Base.Outputs {
		outputs = append(outputs, types.OutPoint{TxID: txid, Vout: uint32(vout)})
	}
	return outputs
}

func sortSnapshotEntriesByTxID(entries []SnapshotEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].TxID[:], entries[j].TxID[:]) < 0
	})
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
		outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
		created = append(created, consensus.UtxoLeafFromOutput(outPoint, output))
	}
	return created
}

func copyOutPointSet(in map[types.OutPoint]struct{}) map[types.OutPoint]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[types.OutPoint]struct{}, len(in))
	for item := range in {
		out[item] = struct{}{}
	}
	return out
}

func outPointSetKeys(in map[types.OutPoint]struct{}) []types.OutPoint {
	out := make([]types.OutPoint, 0, len(in))
	for item := range in {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if cmp := bytes.Compare(out[i].TxID[:], out[j].TxID[:]); cmp != 0 {
			return cmp < 0
		}
		return out[i].Vout < out[j].Vout
	})
	return out
}

func copyTxIDSet(in map[[32]byte]struct{}) map[[32]byte]struct{} {
	if len(in) == 0 {
		return nil
	}
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
