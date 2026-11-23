package mempool

import (
	"errors"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

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
	Tx             types.Transaction
	TxID           [32]byte
	Size           int
	Summary        consensus.TxValidationSummary
	verified       bool
	Parents        map[[32]byte]struct{}
	Missing        map[types.OutPoint]struct{}
	View           consensus.UtxoSet
	SnapshotEpoch  uint64
	PreparedTip    [32]byte
	HasPreparedTip bool
	context        consensus.TxValidationContext
}

type Admission struct {
	TxID           [32]byte
	Summary        consensus.TxValidationSummary
	Accepted       []AcceptedTx
	Orphaned       bool
	EvictedOrphans int
}

type admissionEntry struct {
	Tx              types.Transaction
	TxID            [32]byte
	Parents         map[[32]byte]struct{}
	DescendantCount int
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
		view, parents, missing, err := resolveInputs(entry.Tx, chainLookup, *snapshot, snapshot.Spent)
		if err != nil {
			return err
		}
		if len(missing) != 0 {
			return ErrTxAlreadyExists
		}
		_ = view
		ancestorIDs := gatherAncestorsForView(*snapshot, parents)
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

func (p *Pool) PrepareAdmissionWithLookup(tx types.Transaction, snapshot AdmissionSnapshot, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	return p.PrepareAdmissionWithLookupAndParamsAtHeight(tx, snapshot, chainLookup, consensus.MainnetParams(), 0, rules)
}

func (p *Pool) PrepareAdmissionWithLookupAndParamsAtHeight(tx types.Transaction, snapshot AdmissionSnapshot, chainLookup consensus.UtxoLookup, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	return p.prepareAdmission(tx, snapshot.Epoch, snapshot, snapshot.Spent, chainLookup, params, spendHeight, rules)
}

func (p *Pool) PrepareAdmissionSharedWithLookup(tx types.Transaction, view SharedAdmissionView, chainLookup consensus.UtxoLookup, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	return p.PrepareAdmissionSharedWithLookupAndParamsAtHeight(tx, view, chainLookup, consensus.MainnetParams(), 0, rules)
}

func (p *Pool) PrepareAdmissionSharedWithLookupAndParamsAtHeight(tx types.Transaction, view SharedAdmissionView, chainLookup consensus.UtxoLookup, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	if view.pool != p {
		return PreparedAdmission{}, errors.New("shared admission view belongs to different mempool")
	}
	return p.prepareAdmission(tx, view.Epoch, p, p.spent, chainLookup, params, spendHeight, rules)
}

type admissionSource interface {
	admissionEntry([32]byte) (admissionEntry, bool)
	hasOrphan([32]byte) bool
}

func (p *Pool) admissionEntry(id [32]byte) (admissionEntry, bool) {
	entry := p.entries[id]
	if entry == nil {
		return admissionEntry{}, false
	}
	return admissionEntry{
		Tx:              entry.Tx,
		TxID:            id,
		Parents:         entry.Parents,
		DescendantCount: entry.DescendantCount,
	}, true
}

func (p *Pool) hasOrphan(id [32]byte) bool {
	_, ok := p.orphans[id]
	return ok
}

func (s AdmissionSnapshot) admissionEntry(id [32]byte) (admissionEntry, bool) {
	entry, ok := s.Entries[id]
	return entry, ok
}

func (s AdmissionSnapshot) hasOrphan(id [32]byte) bool {
	_, ok := s.Orphans[id]
	return ok
}

func (p *Pool) prepareAdmission(tx types.Transaction, epoch uint64, entries admissionSource, spent map[types.OutPoint][32]byte, chainLookup consensus.UtxoLookup, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) (PreparedAdmission, error) {
	txid := consensus.TxID(&tx)
	if _, ok := entries.admissionEntry(txid); ok {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if entries.hasOrphan(txid) {
		return PreparedAdmission{}, ErrTxAlreadyExists
	}
	if len(tx.Base.Inputs) == 0 {
		return PreparedAdmission{}, ErrCoinbaseTx
	}

	size := tx.EncodedLen()
	if p.cfg.MaxTxSize > 0 && size > p.cfg.MaxTxSize {
		return PreparedAdmission{}, ErrTxTooLarge
	}

	view, parents, missing, err := resolveInputs(tx, chainLookup, entries, spent)
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
		context:       consensus.TxValidationContext{Params: params, SpendHeight: spendHeight},
	}
	if len(missing) != 0 {
		return prepared, nil
	}

	txValidation, err := consensus.PrepareTxValidationWithLookup(&tx, consensus.LookupFromSet(view), prepared.context, rules)
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
			ancestor, _ := entries.admissionEntry(ancestorID)
			if ancestor.DescendantCount+1 > p.cfg.MaxDescendants {
				return PreparedAdmission{}, ErrTooManyDescendants
			}
		}
	}

	prepared.Summary = summary
	prepared.verified = true
	return prepared, nil
}

func (p *Pool) CommitPrepared(prepared PreparedAdmission, chainUtxos consensus.UtxoSet, chainTipHash [32]byte, rules consensus.ConsensusRules) (Admission, error) {
	return p.CommitPreparedWithLookupAndParamsAtHeight(prepared, consensus.LookupFromSet(chainUtxos), chainTipHash, consensus.MainnetParams(), 0, rules)
}

func (p *Pool) CommitPreparedWithLookupAndParamsAtHeight(prepared PreparedAdmission, chainLookup consensus.UtxoLookup, chainTipHash [32]byte, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) (Admission, error) {
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

	context := consensus.TxValidationContext{Params: params, SpendHeight: spendHeight}
	if prepared.SnapshotEpoch == p.epoch && prepared.HasPreparedTip && prepared.PreparedTip == chainTipHash && prepared.context == context && (prepared.verified || len(prepared.Missing) != 0) {
		if len(prepared.Missing) != 0 {
			return p.admitOrphanLocked(prepared.Tx, prepared.TxID, prepared.Size, prepared.Missing)
		}
		accepted, err := p.insertValidatedLocked(prepared.Tx, prepared.TxID, prepared.Size, prepared.Summary, prepared.Parents)
		if err != nil {
			return Admission{}, err
		}
		admission := Admission{
			TxID:     accepted.TxID,
			Summary:  accepted.Summary,
			Accepted: []AcceptedTx{accepted},
		}
		promoted, _ := p.promoteReadyOrphans(outputsForTx(prepared.TxID, prepared.Tx), chainLookup, params, spendHeight, rules)
		admission.Accepted = append(admission.Accepted, promoted...)
		p.bumpEpochLocked()
		return admission, nil
	}

	view, parents, missing, err := p.resolveInputs(prepared.Tx, chainLookup)
	if err != nil {
		return Admission{}, err
	}
	if len(missing) != 0 {
		return p.admitOrphanLocked(prepared.Tx, prepared.TxID, prepared.Size, missing)
	}

	accepted, err := p.insertPreparedLocked(prepared, view, parents, params, spendHeight, rules)
	if err != nil {
		return Admission{}, err
	}
	admission := Admission{
		TxID:     accepted.TxID,
		Summary:  accepted.Summary,
		Accepted: []AcceptedTx{accepted},
	}
	promoted, _ := p.promoteReadyOrphans(outputsForTx(prepared.TxID, prepared.Tx), chainLookup, params, spendHeight, rules)
	admission.Accepted = append(admission.Accepted, promoted...)
	p.bumpEpochLocked()
	return admission, nil
}

func (p *Pool) AcceptTx(tx types.Transaction, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) (Admission, error) {
	return p.AcceptTxWithParams(tx, chainUtxos, consensus.MainnetParams(), rules)
}

func (p *Pool) AcceptTxWithParams(tx types.Transaction, chainUtxos consensus.UtxoSet, params consensus.ChainParams, rules consensus.ConsensusRules) (Admission, error) {
	return p.AcceptTxWithLookupAndParamsAtHeight(tx, consensus.LookupFromSet(chainUtxos), params, 0, rules)
}

func (p *Pool) AcceptTxWithLookupAndParamsAtHeight(tx types.Transaction, chainLookup consensus.UtxoLookup, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) (Admission, error) {
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

	size := tx.EncodedLen()
	if p.cfg.MaxTxSize > 0 && size > p.cfg.MaxTxSize {
		return Admission{}, ErrTxTooLarge
	}

	view, parents, missing, err := p.resolveInputs(tx, chainLookup)
	if err != nil {
		return Admission{}, err
	}
	if len(missing) != 0 {
		return p.admitOrphanLocked(tx, txid, size, missing)
	}

	accepted, err := p.insertResolved(tx, txid, size, view, parents, params, spendHeight, rules)
	if err != nil {
		return Admission{}, err
	}

	admission := Admission{
		TxID:     accepted.TxID,
		Summary:  accepted.Summary,
		Accepted: []AcceptedTx{accepted},
	}
	promoted, _ := p.promoteReadyOrphans(outputsForTx(txid, tx), chainLookup, params, spendHeight, rules)
	admission.Accepted = append(admission.Accepted, promoted...)
	p.bumpEpochLocked()
	return admission, nil
}

func (p *Pool) insertResolved(tx types.Transaction, txid [32]byte, size int, utxos consensus.UtxoSet, parents map[[32]byte]struct{}, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) (AcceptedTx, error) {
	txValidation, err := consensus.PrepareTxValidationWithLookup(&tx, consensus.LookupFromSet(utxos), consensus.TxValidationContext{Params: params, SpendHeight: spendHeight}, rules)
	if err != nil {
		return AcceptedTx{}, err
	}
	summary, err := consensus.ValidatePreparedTx(txValidation)
	if err != nil {
		return AcceptedTx{}, err
	}
	return p.insertValidatedLocked(tx, txid, size, summary, parents)
}

func (p *Pool) insertPreparedLocked(prepared PreparedAdmission, utxos consensus.UtxoSet, parents map[[32]byte]struct{}, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) (AcceptedTx, error) {
	summary := prepared.Summary
	context := consensus.TxValidationContext{Params: params, SpendHeight: spendHeight}
	if !prepared.verified || prepared.context != context || !sameTxIDSets(prepared.Parents, parents) || !sameUtxoSets(prepared.View, utxos) {
		validated, err := consensus.PrepareTxValidationWithLookup(&prepared.Tx, consensus.LookupFromSet(utxos), context, rules)
		if err != nil {
			return AcceptedTx{}, err
		}
		summary, err = consensus.ValidatePreparedTx(validated)
		if err != nil {
			return AcceptedTx{}, err
		}
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

	if err := p.ensureMempoolCapacityLocked(summary.Fee, size, ancestorIDs); err != nil {
		return AcceptedTx{}, err
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
	authID := consensus.AuthID(&tx)
	entry := &Entry{
		Tx:              tx,
		TxID:            txid,
		AuthID:          authID,
		Summary:         summary,
		SpentOutPoints:  spentOutPointsForInputs(tx.Base.Inputs),
		CreatedLeaves:   createdLeavesForOutputs(txid, tx.Base.Outputs),
		Fee:             summary.Fee,
		Size:            size,
		AddedAt:         p.sequence,
		Parents:         copyTxIDSet(parents),
		AncestorCount:   len(ancestorIDs) + 1,
		AncestorSize:    ancestorSize,
		AncestorFees:    ancestorFees,
		DescendantCount: 1,
		DescendantSize:  size,
		DescendantFees:  summary.Fee,
	}
	p.entries[txid] = entry
	p.notePersistenceMutationLocked(persistenceEntryMutation, txid)
	p.noteEntryAddedLocked(entry)
	for _, input := range tx.Base.Inputs {
		p.spent[input.PrevOut] = txid
	}
	for parentID := range parents {
		parent := p.entries[parentID]
		if parent == nil {
			continue
		}
		if parent.Children == nil {
			parent.Children = make(map[[32]byte]struct{})
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
		AuthID:  authID,
		Summary: summary,
		Fee:     summary.Fee,
		Size:    size,
	}, nil
}

func (p *Pool) resolveInputs(tx types.Transaction, chainLookup consensus.UtxoLookup) (consensus.UtxoSet, map[[32]byte]struct{}, map[types.OutPoint]struct{}, error) {
	return resolveInputs(tx, chainLookup, p, p.spent)
}

func resolveInputs(tx types.Transaction, chainLookup consensus.UtxoLookup, entries admissionSource, spent map[types.OutPoint][32]byte) (consensus.UtxoSet, map[[32]byte]struct{}, map[types.OutPoint]struct{}, error) {
	view := make(consensus.UtxoSet, len(tx.Base.Inputs))
	var parents map[[32]byte]struct{}
	var missing map[types.OutPoint]struct{}

	for _, input := range tx.Base.Inputs {
		if _, ok := spent[input.PrevOut]; ok {
			return nil, nil, nil, ErrInputAlreadySpent
		}
		if utxo, ok := chainLookup(input.PrevOut); ok {
			view[input.PrevOut] = utxo
			continue
		}

		parent, ok := entries.admissionEntry(input.PrevOut.TxID)
		if !ok || uint64(input.PrevOut.Vout) >= uint64(len(parent.Tx.Base.Outputs)) {
			if missing == nil {
				missing = make(map[types.OutPoint]struct{})
			}
			missing[input.PrevOut] = struct{}{}
			continue
		}
		view[input.PrevOut] = consensus.UtxoEntryFromOutput(parent.Tx.Base.Outputs[input.PrevOut.Vout])
		if parents == nil {
			parents = make(map[[32]byte]struct{})
		}
		parents[parent.TxID] = struct{}{}
	}
	return view, parents, missing, nil
}

func gatherAncestorsForView(entries admissionSource, parents map[[32]byte]struct{}) map[[32]byte]struct{} {
	ancestors := make(map[[32]byte]struct{})
	stack := txidSetKeys(parents)
	for len(stack) != 0 {
		last := len(stack) - 1
		txid := stack[last]
		stack = stack[:last]
		if _, ok := ancestors[txid]; ok {
			continue
		}
		entry, ok := entries.admissionEntry(txid)
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

func (p *Pool) meetsRelayFloor(fee uint64, size int) bool {
	if p.cfg.MinRelayFeePerByte == 0 {
		return true
	}
	if size <= 0 {
		return false
	}
	return fee/uint64(size) >= p.cfg.MinRelayFeePerByte
}
