package mempool

import (
	"bytes"
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
		MaxAncestors:       25,
		MaxDescendants:     25,
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

type Pool struct {
	mu         sync.RWMutex
	cfg        PoolConfig
	entries    map[[32]byte]*Entry
	spent      map[types.OutPoint][32]byte
	orphans    map[[32]byte]*orphanEntry
	orphanDeps map[types.OutPoint]map[[32]byte]struct{}
	sequence   uint64
	epoch      uint64
	selection  *selectionCache
}

type orphanEntry struct {
	Tx      types.Transaction
	TxID    [32]byte
	Size    int
	AddedAt uint64
	Missing map[types.OutPoint]struct{}
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
	epoch   uint64
	ordered []packageCandidate
	indexes map[[32]byte]int
}

func New() *Pool {
	return NewWithConfig(DefaultConfig())
}

func NewWithConfig(cfg PoolConfig) *Pool {
	return &Pool{
		cfg:        cfg,
		entries:    make(map[[32]byte]*Entry),
		spent:      make(map[types.OutPoint][32]byte),
		orphans:    make(map[[32]byte]*orphanEntry),
		orphanDeps: make(map[types.OutPoint]map[[32]byte]struct{}),
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

func (p *Pool) PrepareAdmission(tx types.Transaction, snapshot AdmissionSnapshot, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	txid := consensus.TxID(&tx)
	if _, ok := snapshot.Entries[txid]; ok {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if _, ok := snapshot.Orphans[txid]; ok {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if len(tx.Base.Inputs) == 0 {
		return PreparedAdmission{}, ErrCoinbaseTx
	}

	size := len(tx.Encode())
	if p.cfg.MaxTxSize > 0 && size > p.cfg.MaxTxSize {
		return PreparedAdmission{}, ErrTxTooLarge
	}

	view, parents, missing, err := resolveInputsWithView(tx, chainUtxos, snapshot.Entries, snapshot.Spent)
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
		SnapshotEpoch: snapshot.Epoch,
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

	ancestorIDs := gatherAncestorsForView(snapshot.Entries, parents)
	if p.cfg.MaxAncestors > 0 && len(ancestorIDs)+1 > p.cfg.MaxAncestors {
		return PreparedAdmission{}, ErrTooManyAncestors
	}
	if p.cfg.MaxDescendants > 0 {
		for ancestorID := range ancestorIDs {
			ancestor := snapshot.Entries[ancestorID]
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

	view, parents, missing, err := p.resolveInputs(prepared.Tx, chainUtxos)
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
	admission.Accepted = append(admission.Accepted, p.promoteReadyOrphans(outputsForTx(prepared.TxID, prepared.Tx), chainUtxos, rules)...)
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

	view, parents, missing, err := p.resolveInputs(tx, chainUtxos)
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
	admission.Accepted = append(admission.Accepted, p.promoteReadyOrphans(outputsForTx(txid, tx), chainUtxos, rules)...)
	p.bumpEpochLocked()
	return admission, nil
}

func (p *Pool) SelectForBlock(chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int) ([]SnapshotEntry, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	selected := make([]SnapshotEntry, 0, len(p.entries))
	included := make(map[[32]byte]struct{}, len(p.entries))
	tempUtxos := cloneUtxos(chainUtxos)
	usedBytes := 0
	var totalFees uint64

	for {
		ordered := p.cachedPackageCandidatesLocked()
		if len(ordered) == 0 {
			break
		}

		progress := false
		for _, candidate := range ordered {
			filtered := filterCandidateEntries(candidate.Entries, included)
			if len(filtered) == 0 {
				continue
			}
			filteredSize, filteredFee := packageStats(filtered)
			if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
				continue
			}
			if !p.meetsRelayFloor(filteredFee, filteredSize) {
				continue
			}

			packageFees, ok := applyCandidatePackage(tempUtxos, filtered, rules)
			if !ok {
				continue
			}

			usedBytes += filteredSize
			totalFees += packageFees
			for _, entry := range filtered {
				included[entry.TxID] = struct{}{}
				selected = append(selected, snapshotForEntry(entry))
			}
			progress = true
			break
		}
		if !progress {
			break
		}
	}

	return selected, totalFees
}

func (p *Pool) AppendForBlock(currentUtxos consensus.UtxoSet, rules consensus.ConsensusRules, maxTxBytes int, selected []SnapshotEntry) ([]SnapshotEntry, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	appendOnly := make([]SnapshotEntry, 0, len(p.entries))
	included := make(map[[32]byte]struct{}, len(selected))
	usedBytes := 0
	for _, entry := range selected {
		included[entry.TxID] = struct{}{}
		usedBytes += entry.Size
	}
	var totalFees uint64

	for {
		ordered := p.cachedPackageCandidatesLocked()
		if len(ordered) == 0 {
			break
		}

		progress := false
		for _, candidate := range ordered {
			filtered := filterCandidateEntries(candidate.Entries, included)
			if len(filtered) == 0 {
				continue
			}
			filteredSize, filteredFee := packageStats(filtered)
			if maxTxBytes > 0 && usedBytes+filteredSize > maxTxBytes {
				continue
			}
			if !p.meetsRelayFloor(filteredFee, filteredSize) {
				continue
			}

			packageFees, ok := applyCandidatePackage(currentUtxos, filtered, rules)
			if !ok {
				continue
			}

			usedBytes += filteredSize
			totalFees += packageFees
			for _, entry := range filtered {
				included[entry.TxID] = struct{}{}
				appendOnly = append(appendOnly, snapshotForEntry(entry))
			}
			progress = true
			break
		}
		if !progress {
			break
		}
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

func (p *Pool) promoteReadyOrphans(outputs []types.OutPoint, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) []AcceptedTx {
	promoted := make([]AcceptedTx, 0)
	queued := append([]types.OutPoint(nil), outputs...)
	seen := make(map[[32]byte]struct{})

	for len(queued) != 0 {
		out := queued[0]
		queued = queued[1:]
		waiting := p.orphansForOutPoint(out)
		for _, txid := range waiting {
			if _, ok := seen[txid]; ok {
				continue
			}
			seen[txid] = struct{}{}

			orphan := p.orphans[txid]
			if orphan == nil {
				continue
			}
			view, parents, missing, err := p.resolveInputs(orphan.Tx, chainUtxos)
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
			queued = append(queued, outputsForTx(txid, orphan.Tx)...)
		}
	}

	return promoted
}

func (p *Pool) resolveInputs(tx types.Transaction, chainUtxos consensus.UtxoSet) (consensus.UtxoSet, map[[32]byte]struct{}, map[types.OutPoint]struct{}, error) {
	view := make(consensus.UtxoSet, len(tx.Base.Inputs))
	parents := make(map[[32]byte]struct{})
	missing := make(map[types.OutPoint]struct{})

	for _, input := range tx.Base.Inputs {
		if _, ok := p.spent[input.PrevOut]; ok {
			return nil, nil, nil, ErrInputAlreadySpent
		}
		if utxo, ok := chainUtxos[input.PrevOut]; ok {
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

func resolveInputsWithView(tx types.Transaction, chainUtxos consensus.UtxoSet, entries map[[32]byte]admissionEntry, spent map[types.OutPoint][32]byte) (consensus.UtxoSet, map[[32]byte]struct{}, map[types.OutPoint]struct{}, error) {
	view := make(consensus.UtxoSet, len(tx.Base.Inputs))
	parents := make(map[[32]byte]struct{})
	missing := make(map[types.OutPoint]struct{})

	for _, input := range tx.Base.Inputs {
		if _, ok := spent[input.PrevOut]; ok {
			return nil, nil, nil, ErrInputAlreadySpent
		}
		if utxo, ok := chainUtxos[input.PrevOut]; ok {
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

func (p *Pool) storeOrphan(tx types.Transaction, txid [32]byte, size int, missing map[types.OutPoint]struct{}) int {
	p.sequence++
	p.orphans[txid] = &orphanEntry{
		Tx:      tx,
		TxID:    txid,
		Size:    size,
		AddedAt: p.sequence,
		Missing: copyOutPointSet(missing),
	}
	for out := range missing {
		if p.orphanDeps[out] == nil {
			p.orphanDeps[out] = make(map[[32]byte]struct{})
		}
		p.orphanDeps[out][txid] = struct{}{}
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
	orphan := p.orphans[txid]
	if orphan == nil {
		return
	}
	for out := range orphan.Missing {
		waiting := p.orphanDeps[out]
		delete(waiting, txid)
		if len(waiting) == 0 {
			delete(p.orphanDeps, out)
		}
	}
	delete(p.orphans, txid)
}

func (p *Pool) updateOrphanMissing(txid [32]byte, missing map[types.OutPoint]struct{}) {
	orphan := p.orphans[txid]
	if orphan == nil {
		return
	}
	for out := range orphan.Missing {
		waiting := p.orphanDeps[out]
		delete(waiting, txid)
		if len(waiting) == 0 {
			delete(p.orphanDeps, out)
		}
	}
	orphan.Missing = copyOutPointSet(missing)
	for out := range orphan.Missing {
		if p.orphanDeps[out] == nil {
			p.orphanDeps[out] = make(map[[32]byte]struct{})
		}
		p.orphanDeps[out][txid] = struct{}{}
	}
}

func (p *Pool) orphansForOutPoint(out types.OutPoint) [][32]byte {
	waiting := p.orphanDeps[out]
	if len(waiting) == 0 {
		return nil
	}
	txids := make([][32]byte, 0, len(waiting))
	for txid := range waiting {
		txids = append(txids, txid)
	}
	sort.Slice(txids, func(i, j int) bool {
		return bytes.Compare(txids[i][:], txids[j][:]) < 0
	})
	return txids
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
		return len(p.selection.ordered)
	}
	return len(p.entries)
}

func (p *Pool) cachedPackageCandidatesLocked() []packageCandidate {
	p.ensureSelectionLocked()
	return p.selection.ordered
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
	candidates := make([]packageCandidate, 0, len(p.entries))
	for _, entry := range p.entries {
		candidate := p.candidateForTxLocked(entry.TxID)
		if len(candidate.Entries) == 0 {
			continue
		}
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidateLess(candidates[i], candidates[j])
	})
	indexes := make(map[[32]byte]int, len(candidates))
	for i, candidate := range candidates {
		indexes[candidate.TxID] = i
	}
	p.selection = &selectionCache{
		epoch:   p.epoch,
		ordered: candidates,
		indexes: indexes,
	}
}

func (p *Pool) upsertSelectionCandidateLocked(txid [32]byte) {
	p.ensureSelectionLocked()
	candidate := p.candidateForTxLocked(txid)
	if len(candidate.Entries) == 0 {
		return
	}
	if idx, ok := p.selection.indexes[txid]; ok {
		p.selection.ordered = append(p.selection.ordered[:idx], p.selection.ordered[idx+1:]...)
		delete(p.selection.indexes, txid)
		for i := idx; i < len(p.selection.ordered); i++ {
			p.selection.indexes[p.selection.ordered[i].TxID] = i
		}
	}
	insertAt := sort.Search(len(p.selection.ordered), func(i int) bool {
		return candidateLess(candidate, p.selection.ordered[i])
	})
	p.selection.ordered = append(p.selection.ordered, packageCandidate{})
	copy(p.selection.ordered[insertAt+1:], p.selection.ordered[insertAt:])
	p.selection.ordered[insertAt] = candidate
	for i := insertAt; i < len(p.selection.ordered); i++ {
		p.selection.indexes[p.selection.ordered[i].TxID] = i
	}
}

func (p *Pool) removeSelectionCandidatesLocked(remove map[[32]byte]struct{}) {
	if p.selection == nil || len(remove) == 0 {
		return
	}
	filtered := p.selection.ordered[:0]
	for _, candidate := range p.selection.ordered {
		if _, ok := remove[candidate.TxID]; ok {
			continue
		}
		filtered = append(filtered, candidate)
	}
	p.selection.ordered = filtered
	p.selection.indexes = make(map[[32]byte]int, len(p.selection.ordered))
	for i, candidate := range p.selection.ordered {
		p.selection.indexes[candidate.TxID] = i
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

func applyCandidatePackage(utxos consensus.UtxoSet, entries []*Entry, rules consensus.ConsensusRules) (uint64, bool) {
	undos := make([]appliedTxUndo, 0, len(entries))
	var totalFees uint64
	for _, entry := range entries {
		summary, err := consensus.ValidateTx(&entry.Tx, utxos, rules)
		if err != nil {
			rollbackAppliedPackage(utxos, undos)
			return 0, false
		}
		totalFees += summary.Fee
		undos = append(undos, applyTxWithUndo(utxos, entry.Tx, entry.TxID))
	}
	return totalFees, true
}

func applyTxWithUndo(utxos consensus.UtxoSet, tx types.Transaction, txid [32]byte) appliedTxUndo {
	undo := appliedTxUndo{
		spent:   make(map[types.OutPoint]consensus.UtxoEntry, len(tx.Base.Inputs)),
		created: make([]types.OutPoint, 0, len(tx.Base.Outputs)),
	}
	for _, input := range tx.Base.Inputs {
		if entry, ok := utxos[input.PrevOut]; ok {
			undo.spent[input.PrevOut] = entry
		}
		delete(utxos, input.PrevOut)
	}
	for vout, output := range tx.Base.Outputs {
		out := types.OutPoint{TxID: txid, Vout: uint32(vout)}
		utxos[out] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		}
		undo.created = append(undo.created, out)
	}
	return undo
}

func rollbackAppliedPackage(utxos consensus.UtxoSet, undos []appliedTxUndo) {
	for i := len(undos) - 1; i >= 0; i-- {
		undo := undos[i]
		for _, out := range undo.created {
			delete(utxos, out)
		}
		for out, entry := range undo.spent {
			utxos[out] = entry
		}
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

func cloneUtxos(utxos consensus.UtxoSet) consensus.UtxoSet {
	out := make(consensus.UtxoSet, len(utxos))
	for outPoint, entry := range utxos {
		out[outPoint] = entry
	}
	return out
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
