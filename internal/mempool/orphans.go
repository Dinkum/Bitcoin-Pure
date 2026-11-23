package mempool

import (
	"bytes"
	"sort"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

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

// PromoteReadyOrphansForBlock rechecks orphan transactions that were waiting on
// outputs created by the newly accepted block.
func (p *Pool) PromoteReadyOrphansForBlock(block *types.Block, chainUtxos consensus.UtxoSet, rules consensus.ConsensusRules) []AcceptedTx {
	accepted, _ := p.PromoteReadyOrphansDetailedForBlockWithLookupAndParams(block, consensus.LookupFromSet(chainUtxos), consensus.MainnetParams(), rules)
	return accepted
}

func (p *Pool) PromoteReadyOrphansDetailedForBlockWithLookupAndParams(block *types.Block, chainLookup consensus.UtxoLookup, params consensus.ChainParams, rules consensus.ConsensusRules) ([]AcceptedTx, []RejectedTx) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if block == nil || len(p.orphans) == 0 {
		return nil, nil
	}
	outputs := make([]types.OutPoint, 0)
	for _, tx := range block.Txs {
		txid := consensus.TxID(&tx)
		outputs = append(outputs, outputsForTx(txid, tx)...)
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	spendHeight := uint64(0)
	if len(block.Txs) > 0 && block.Txs[0].Base.CoinbaseHeight != nil && *block.Txs[0].Base.CoinbaseHeight != ^uint64(0) {
		spendHeight = *block.Txs[0].Base.CoinbaseHeight + 1
	}
	promoted, rejected := p.promoteReadyOrphans(outputs, chainLookup, params, spendHeight, rules)
	if len(promoted) > 0 {
		p.bumpEpochLocked()
	}
	return promoted, rejected
}

func (p *Pool) promoteReadyOrphans(outputs []types.OutPoint, chainLookup consensus.UtxoLookup, params consensus.ChainParams, spendHeight uint64, rules consensus.ConsensusRules) ([]AcceptedTx, []RejectedTx) {
	promoted := make([]AcceptedTx, 0)
	rejected := make([]RejectedTx, 0)
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
			rejected = append(rejected, RejectedTx{Tx: orphan.Tx, TxID: txid, Err: err})
			p.deleteOrphan(txid)
			continue
		}
		if len(missing) != 0 {
			p.updateOrphanMissing(txid, missing)
			continue
		}

		p.deleteOrphan(txid)
		accepted, err := p.insertResolved(orphan.Tx, txid, orphan.Size, view, parents, params, spendHeight, rules)
		if err != nil {
			rejected = append(rejected, RejectedTx{Tx: orphan.Tx, TxID: txid, Err: err})
			continue
		}
		promoted = append(promoted, accepted)
		ready = p.enqueueReadyOrphansForOutputs(ready, outputsForTx(txid, orphan.Tx))
	}

	return promoted, rejected
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
			p.notePersistenceMutationLocked(persistenceOrphanMutation, txid)
			if orphan.MissingCount == 0 {
				ready = append(ready, txid)
			}
		}
	}
	return ready
}

func (p *Pool) admitOrphanLocked(tx types.Transaction, txid [32]byte, size int, missing map[types.OutPoint]struct{}) (Admission, error) {
	if size > p.cfg.MaxOrphanBytes {
		return Admission{}, ErrTxTooLarge
	}
	p.sequence++
	p.orphans[txid] = &orphanEntry{
		Tx:           tx,
		TxID:         txid,
		Size:         size,
		AddedAt:      p.sequence,
		Missing:      copyOutPointSet(missing),
		MissingCount: len(missing),
	}
	p.notePersistenceMutationLocked(persistenceOrphanMutation, txid)
	for out := range missing {
		p.orphanDeps[out] = append(p.orphanDeps[out], txid)
	}
	p.orphanBytes += size
	evicted := p.trimOrphans()
	p.bumpEpochLocked()
	return Admission{TxID: txid, Orphaned: true, EvictedOrphans: evicted}, nil
}

func (p *Pool) trimOrphans() int {
	if !p.orphansOverCapacity() {
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
	for p.orphansOverCapacity() {
		p.deleteOrphan(order[evicted].TxID)
		evicted++
	}
	return evicted
}

func (p *Pool) orphansOverCapacity() bool {
	return p.orphanBytes > p.cfg.MaxOrphanBytes || (p.cfg.MaxOrphans > 0 && len(p.orphans) > p.cfg.MaxOrphans)
}

func (p *Pool) deleteOrphan(txid [32]byte) {
	orphan := p.orphans[txid]
	if orphan != nil {
		p.orphanBytes -= orphan.Size
		p.removeOrphanDeps(txid, orphan.Missing)
		p.notePersistenceMutationLocked(persistenceOrphanMutation, txid)
	}
	delete(p.orphans, txid)
}

func (p *Pool) updateOrphanMissing(txid [32]byte, missing map[types.OutPoint]struct{}) {
	orphan := p.orphans[txid]
	if orphan == nil {
		return
	}
	p.removeOrphanDeps(txid, orphan.Missing)
	orphan.Missing = copyOutPointSet(missing)
	orphan.MissingCount = len(missing)
	for out := range orphan.Missing {
		p.orphanDeps[out] = append(p.orphanDeps[out], txid)
	}
	p.notePersistenceMutationLocked(persistenceOrphanMutation, txid)
}

func (p *Pool) removeOrphanDeps(txid [32]byte, missing map[types.OutPoint]struct{}) {
	for out := range missing {
		waiting := p.orphanDeps[out]
		for i, waitingTxID := range waiting {
			if waitingTxID != txid {
				continue
			}
			// Preserve waiter order so orphan promotion remains deterministic.
			waiting = append(waiting[:i], waiting[i+1:]...)
			break
		}
		if len(waiting) == 0 {
			delete(p.orphanDeps, out)
			continue
		}
		p.orphanDeps[out] = waiting
	}
}
