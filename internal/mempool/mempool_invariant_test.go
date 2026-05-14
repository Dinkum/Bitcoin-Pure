package mempool

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

type invariantCoin struct {
	value uint64
	seed  byte
}

type mempoolInvariantHarness struct {
	t                    testing.TB
	pool                 *Pool
	chain                consensus.UtxoSet
	coins                map[types.OutPoint]invariantCoin
	pendingMempoolParent []types.Transaction
	pendingBlockParent   []types.Transaction
	nextRoot             byte
	nextSeed             byte
	rules                consensus.ConsensusRules
}

func TestMempoolRandomizedInvariants(t *testing.T) {
	seeds := [][]byte{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		{2, 2, 2, 3, 8, 8, 1, 7, 5, 9, 4, 6, 0, 3, 7, 8, 9, 1},
		{7, 1, 7, 1, 7, 1, 3, 4, 5, 8, 8, 8, 6, 6, 2, 2, 9, 9},
	}
	for i, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", i), func(t *testing.T) {
			runMempoolInvariantProgram(t, seed)
		})
	}
}

func FuzzMempoolInvariantOperations(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 8, 4, 6, 7, 5, 9})
	f.Add([]byte{2, 2, 2, 3, 3, 7, 8, 8, 4, 5, 9})
	f.Add([]byte{1, 1, 1, 4, 0, 2, 6, 7, 8, 9, 3})
	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 96 {
			program = program[:96]
		}
		runMempoolInvariantProgram(t, program)
	})
}

func runMempoolInvariantProgram(t testing.TB, program []byte) {
	t.Helper()
	h := newMempoolInvariantHarness(t)
	if len(program) == 0 {
		program = []byte{0, 2, 3, 8, 4, 7, 9}
	}
	for step, op := range program {
		h.runOperation(step, op)
		h.assertPoolInvariants(fmt.Sprintf("step %d op %d", step, op%10))
		if step%5 == 0 || op%10 == 8 {
			h.assertSelectionInvariants(fmt.Sprintf("step %d selection", step))
		}
	}
	h.assertReindexStable("final reindex")
}

func newMempoolInvariantHarness(t testing.TB) *mempoolInvariantHarness {
	t.Helper()
	h := &mempoolInvariantHarness{
		t:     t,
		pool:  NewWithConfig(PoolConfig{MinRelayFeePerByte: 0, MaxTxSize: 1_000_000, MaxAncestors: 128, MaxDescendants: 128, MaxOrphans: 12}),
		chain: make(consensus.UtxoSet),
		coins: make(map[types.OutPoint]invariantCoin),
		rules: consensus.DefaultConsensusRules(),
	}
	h.nextRoot = 1
	h.nextSeed = 40
	for i := byte(1); i <= 10; i++ {
		out := types.OutPoint{TxID: [32]byte{0xa0, i}, Vout: 0}
		coin := invariantCoin{value: 100, seed: i}
		h.chain[out] = consensus.UtxoEntryFromOutput(types.NewXOnlyOutput(coin.value, signerPubKey(coin.seed)))
		h.coins[out] = coin
	}
	return h
}

func (h *mempoolInvariantHarness) runOperation(step int, op byte) {
	switch op % 10 {
	case 0, 1:
		h.addValidTx(op)
	case 2:
		h.addChildBeforeParent(op, false)
	case 3:
		h.acceptPendingMempoolParent(op)
	case 4:
		h.connectSelectedBlock()
	case 5:
		h.removeRandomEntry(op)
	case 6:
		h.addUnresolvableOrphan()
	case 7:
		h.addChildBeforeParent(op, true)
	case 8:
		h.connectPendingBlockParent(op)
	case 9:
		h.assertReindexStable(fmt.Sprintf("step %d forced reindex", step))
	}
}

func (h *mempoolInvariantHarness) addValidTx(choice byte) {
	out, coin, ok := h.pickSpendableOutput(choice, false)
	if !ok {
		return
	}
	tx := h.makeSpend(out, coin, 1)
	admission, err := h.pool.AcceptTx(tx, h.chain, h.rules)
	if err != nil {
		h.t.Fatalf("accept valid tx %x: %v", consensus.TxID(&tx), err)
	}
	if admission.Orphaned {
		h.t.Fatalf("valid tx %x was stored as orphan", consensus.TxID(&tx))
	}
}

func (h *mempoolInvariantHarness) addChildBeforeParent(choice byte, parentInBlock bool) {
	out, coin, ok := h.pickSpendableOutput(choice, parentInBlock)
	if !ok {
		return
	}
	parent := h.makeSpend(out, coin, 1)
	parentID := consensus.TxID(&parent)
	parentOut := types.OutPoint{TxID: parentID, Vout: 0}
	parentCoin := h.coins[parentOut]
	child := h.makeSpend(parentOut, parentCoin, 1)
	admission, err := h.pool.AcceptTx(child, h.chain, h.rules)
	if err != nil {
		h.t.Fatalf("store child-before-parent orphan %x: %v", consensus.TxID(&child), err)
	}
	if !admission.Orphaned {
		h.t.Fatalf("child-before-parent tx %x was accepted before parent %x", consensus.TxID(&child), parentID)
	}
	if parentInBlock {
		h.pendingBlockParent = append(h.pendingBlockParent, parent)
		return
	}
	h.pendingMempoolParent = append(h.pendingMempoolParent, parent)
}

func (h *mempoolInvariantHarness) acceptPendingMempoolParent(choice byte) {
	if len(h.pendingMempoolParent) == 0 {
		return
	}
	idx := int(choice) % len(h.pendingMempoolParent)
	parent := h.pendingMempoolParent[idx]
	h.pendingMempoolParent = append(h.pendingMempoolParent[:idx], h.pendingMempoolParent[idx+1:]...)
	_, _ = h.pool.AcceptTx(parent, h.chain, h.rules)
}

func (h *mempoolInvariantHarness) connectPendingBlockParent(choice byte) {
	if len(h.pendingBlockParent) == 0 {
		return
	}
	idx := int(choice) % len(h.pendingBlockParent)
	parent := h.pendingBlockParent[idx]
	h.pendingBlockParent = append(h.pendingBlockParent[:idx], h.pendingBlockParent[idx+1:]...)
	for _, input := range parent.Base.Inputs {
		if _, ok := h.chain[input.PrevOut]; !ok {
			return
		}
	}
	h.connectBlock([]types.Transaction{parent})
}

func (h *mempoolInvariantHarness) addUnresolvableOrphan() {
	missing := types.OutPoint{TxID: [32]byte{0xf0, h.nextRoot}, Vout: 0}
	h.nextRoot++
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: missing}},
			Outputs: []types.TxOutput{types.NewXOnlyOutput(49, signerPubKey(h.nextSignerSeed()))},
		},
		Auth: types.TxAuth{Entries: []types.TxAuthEntry{{}}},
	}
	admission, err := h.pool.AcceptTx(tx, h.chain, h.rules)
	if err != nil {
		h.t.Fatalf("store unresolvable orphan: %v", err)
	}
	if !admission.Orphaned {
		h.t.Fatalf("unresolvable tx %x was not orphaned", consensus.TxID(&tx))
	}
}

func (h *mempoolInvariantHarness) connectSelectedBlock() {
	selected, _, _ := h.pool.SelectForBlockOverlay(h.chain, h.rules, 1_000_000)
	if len(selected) == 0 {
		return
	}
	h.assertSelectedEntries("connect selected block", selected)
	txs := make([]types.Transaction, 0, len(selected))
	for _, entry := range selected {
		txs = append(txs, entry.Tx)
	}
	h.connectBlock(txs)
}

func (h *mempoolInvariantHarness) connectBlock(txs []types.Transaction) {
	if len(txs) == 0 {
		return
	}
	blockTxs := make([]types.Transaction, 0, len(txs)+1)
	blockTxs = append(blockTxs, testCoinbase(1, []types.TxOutput{{ValueAtoms: 1, PubKey: signerPubKey(250)}}))
	blockTxs = append(blockTxs, txs...)
	block := &types.Block{Txs: blockTxs}
	spentInBlock := make(map[types.OutPoint]struct{})
	for _, tx := range txs {
		for _, input := range tx.Base.Inputs {
			spentInBlock[input.PrevOut] = struct{}{}
			delete(h.chain, input.PrevOut)
		}
	}
	for _, tx := range txs {
		txid := consensus.TxID(&tx)
		for vout, output := range tx.Base.Outputs {
			out := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			if _, spent := spentInBlock[out]; spent {
				continue
			}
			h.chain[out] = consensus.UtxoEntryFromOutput(output)
		}
	}
	h.pool.RemoveConfirmed(block)
	h.pool.PromoteReadyOrphansForBlock(block, h.chain, h.rules)
}

func (h *mempoolInvariantHarness) removeRandomEntry(choice byte) {
	ids := h.sortedEntryIDs()
	if len(ids) == 0 {
		return
	}
	remove := ids[int(choice)%len(ids)]
	h.pool.mu.Lock()
	h.pool.removeRecursive(map[[32]byte]struct{}{remove: {}})
	h.pool.bumpEpochLocked()
	h.pool.mu.Unlock()
}

func (h *mempoolInvariantHarness) makeSpend(out types.OutPoint, coin invariantCoin, fee uint64) types.Transaction {
	if coin.value <= fee {
		h.t.Fatalf("coin %v value %d cannot pay fee %d", out, coin.value, fee)
	}
	recipientSeed := h.nextSignerSeed()
	tx := signedSpendTx(h.t, coin.seed, out, coin.value, recipientSeed, fee)
	txid := consensus.TxID(&tx)
	for vout, output := range tx.Base.Outputs {
		h.coins[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = invariantCoin{
			value: output.ValueAtoms,
			seed:  recipientSeed,
		}
	}
	return tx
}

func (h *mempoolInvariantHarness) nextSignerSeed() byte {
	h.nextSeed++
	if h.nextSeed == 0 {
		h.nextSeed = 1
	}
	return h.nextSeed
}

func (h *mempoolInvariantHarness) pickSpendableOutput(choice byte, chainOnly bool) (types.OutPoint, invariantCoin, bool) {
	h.pool.mu.RLock()
	defer h.pool.mu.RUnlock()
	outs := make([]types.OutPoint, 0, len(h.coins))
	for out, coin := range h.coins {
		if coin.value <= 2 {
			continue
		}
		if _, spent := h.pool.spent[out]; spent {
			continue
		}
		_, onChain := h.chain[out]
		entry := h.pool.entries[out.TxID]
		inPool := entry != nil && out.Vout < uint32(len(entry.Tx.Base.Outputs))
		if chainOnly {
			if onChain {
				outs = append(outs, out)
			}
			continue
		}
		if onChain || inPool {
			outs = append(outs, out)
		}
	}
	sort.Slice(outs, func(i, j int) bool {
		if cmp := bytes.Compare(outs[i].TxID[:], outs[j].TxID[:]); cmp != 0 {
			return cmp < 0
		}
		return outs[i].Vout < outs[j].Vout
	})
	if len(outs) == 0 {
		return types.OutPoint{}, invariantCoin{}, false
	}
	out := outs[int(choice)%len(outs)]
	return out, h.coins[out], true
}

func (h *mempoolInvariantHarness) sortedEntryIDs() [][32]byte {
	h.pool.mu.RLock()
	defer h.pool.mu.RUnlock()
	ids := make([][32]byte, 0, len(h.pool.entries))
	for txid := range h.pool.entries {
		ids = append(ids, txid)
	}
	sortTxIDs(ids)
	return ids
}

func (h *mempoolInvariantHarness) assertPoolInvariants(context string) {
	h.t.Helper()
	h.pool.mu.RLock()
	defer h.pool.mu.RUnlock()

	expectedSpent := make(map[types.OutPoint][32]byte)
	expectedParents := make(map[[32]byte]map[[32]byte]struct{}, len(h.pool.entries))
	expectedChildren := make(map[[32]byte]map[[32]byte]struct{}, len(h.pool.entries))
	for txid := range h.pool.entries {
		expectedParents[txid] = nil
		expectedChildren[txid] = nil
	}

	for txid, entry := range h.pool.entries {
		if entry == nil {
			h.t.Fatalf("%s: nil entry for %x", context, txid)
		}
		if entry.TxID != txid {
			h.t.Fatalf("%s: entry key %x mismatches entry txid %x", context, txid, entry.TxID)
		}
		if got := consensus.TxID(&entry.Tx); got != txid {
			h.t.Fatalf("%s: entry txid %x recomputes as %x", context, txid, got)
		}
		assertEntryDeltasMatchTx(h.t, context, entry)
		for _, input := range entry.Tx.Base.Inputs {
			if previous, exists := expectedSpent[input.PrevOut]; exists {
				h.t.Fatalf("%s: outpoint %v spent by both %x and %x", context, input.PrevOut, previous, txid)
			}
			expectedSpent[input.PrevOut] = txid
			parent := h.pool.entries[input.PrevOut.TxID]
			if parent != nil && input.PrevOut.Vout < uint32(len(parent.Tx.Base.Outputs)) {
				addTxIDSet(expectedParents, txid, parent.TxID)
				addTxIDSet(expectedChildren, parent.TxID, txid)
				continue
			}
			if _, ok := h.chain[input.PrevOut]; !ok {
				h.t.Fatalf("%s: accepted tx %x has dangling input %v", context, txid, input.PrevOut)
			}
		}
	}

	if !reflect.DeepEqual(h.pool.spent, expectedSpent) {
		h.t.Fatalf("%s: spent map mismatch\n got: %#v\nwant: %#v", context, h.pool.spent, expectedSpent)
	}
	for txid, entry := range h.pool.entries {
		if !sameTxIDSets(entry.Parents, expectedParents[txid]) {
			h.t.Fatalf("%s: parents for %x = %x, want %x", context, txid, txidSetKeys(entry.Parents), txidSetKeys(expectedParents[txid]))
		}
		if !sameTxIDSets(entry.Children, expectedChildren[txid]) {
			h.t.Fatalf("%s: children for %x = %x, want %x", context, txid, txidSetKeys(entry.Children), txidSetKeys(expectedChildren[txid]))
		}
	}

	for txid, entry := range h.pool.entries {
		ancestors := collectInvariantRelatives(txid, expectedParents)
		descendants := collectInvariantRelatives(txid, expectedChildren)
		wantAncestorSize, wantAncestorFees := entry.Size, entry.Fee
		for ancestor := range ancestors {
			parent := h.pool.entries[ancestor]
			wantAncestorSize += parent.Size
			wantAncestorFees += parent.Fee
		}
		wantDescendantSize, wantDescendantFees := entry.Size, entry.Fee
		for descendant := range descendants {
			child := h.pool.entries[descendant]
			wantDescendantSize += child.Size
			wantDescendantFees += child.Fee
		}
		if entry.AncestorCount != len(ancestors)+1 || entry.AncestorSize != wantAncestorSize || entry.AncestorFees != wantAncestorFees {
			h.t.Fatalf("%s: ancestor stats for %x = count/size/fees %d/%d/%d, want %d/%d/%d", context, txid, entry.AncestorCount, entry.AncestorSize, entry.AncestorFees, len(ancestors)+1, wantAncestorSize, wantAncestorFees)
		}
		if entry.DescendantCount != len(descendants)+1 || entry.DescendantSize != wantDescendantSize || entry.DescendantFees != wantDescendantFees {
			h.t.Fatalf("%s: descendant stats for %x = count/size/fees %d/%d/%d, want %d/%d/%d", context, txid, entry.DescendantCount, entry.DescendantSize, entry.DescendantFees, len(descendants)+1, wantDescendantSize, wantDescendantFees)
		}
	}

	for txid, orphan := range h.pool.orphans {
		if orphan == nil {
			h.t.Fatalf("%s: nil orphan for %x", context, txid)
		}
		if _, accepted := h.pool.entries[txid]; accepted {
			h.t.Fatalf("%s: tx %x is both accepted and orphaned", context, txid)
		}
		if orphan.MissingCount != len(orphan.Missing) {
			h.t.Fatalf("%s: orphan %x missing count = %d, want %d", context, txid, orphan.MissingCount, len(orphan.Missing))
		}
		for out := range orphan.Missing {
			if !containsTxID(h.pool.orphanDeps[out], txid) {
				h.t.Fatalf("%s: orphan %x missing outpoint %v absent from orphanDeps", context, txid, out)
			}
		}
	}
	for out, waiters := range h.pool.orphanDeps {
		seen := make(map[[32]byte]struct{}, len(waiters))
		for _, txid := range waiters {
			if _, duplicate := seen[txid]; duplicate {
				h.t.Fatalf("%s: duplicate orphan waiter %x for %v", context, txid, out)
			}
			seen[txid] = struct{}{}
			orphan := h.pool.orphans[txid]
			if orphan == nil {
				h.t.Fatalf("%s: orphanDeps references missing orphan %x for %v", context, txid, out)
			}
			if _, ok := orphan.Missing[out]; !ok {
				h.t.Fatalf("%s: orphanDeps references %x for %v, but orphan missing set does not", context, txid, out)
			}
		}
	}

	h.assertSelectionCandidatesLocked(context)
}

func (h *mempoolInvariantHarness) assertSelectionCandidatesLocked(context string) {
	candidates := h.pool.cachedPackageCandidatesLocked()
	seenCandidates := make(map[[32]byte]struct{}, len(candidates))
	for _, candidate := range candidates {
		if _, duplicate := seenCandidates[candidate.TxID]; duplicate {
			h.t.Fatalf("%s: duplicate selection candidate %x", context, candidate.TxID)
		}
		seenCandidates[candidate.TxID] = struct{}{}
		if h.pool.entries[candidate.TxID] == nil {
			h.t.Fatalf("%s: selection candidate %x missing entry", context, candidate.TxID)
		}
		packageIDs := make(map[[32]byte]struct{}, len(candidate.Entries))
		var fee uint64
		size := 0
		for _, entry := range candidate.Entries {
			if entry == nil {
				h.t.Fatalf("%s: candidate %x has nil package entry", context, candidate.TxID)
			}
			if _, duplicate := packageIDs[entry.TxID]; duplicate {
				h.t.Fatalf("%s: candidate %x contains duplicate entry %x", context, candidate.TxID, entry.TxID)
			}
			packageIDs[entry.TxID] = struct{}{}
			fee += entry.Fee
			size += entry.Size
		}
		for _, entry := range candidate.Entries {
			for parent := range entry.Parents {
				if _, ok := packageIDs[parent]; !ok {
					h.t.Fatalf("%s: candidate %x includes child %x without parent %x", context, candidate.TxID, entry.TxID, parent)
				}
			}
		}
		if candidate.Fee != fee || candidate.Size != size {
			h.t.Fatalf("%s: candidate %x fee/size = %d/%d, want %d/%d", context, candidate.TxID, candidate.Fee, candidate.Size, fee, size)
		}
	}
}

func (h *mempoolInvariantHarness) assertSelectionInvariants(context string) {
	selected, _, overlay := h.pool.SelectForBlockOverlay(h.chain, h.rules, 1_000_000)
	h.assertSelectedEntries(context, selected)
	for _, entry := range selected {
		for _, spent := range entry.SpentOutPoints {
			if _, ok := overlay.Lookup(spent); ok {
				h.t.Fatalf("%s: selected overlay still exposes spent outpoint %v", context, spent)
			}
		}
	}
}

func (h *mempoolInvariantHarness) assertSelectedEntries(context string, selected []SnapshotEntry) {
	h.t.Helper()
	selectedIDs := make(map[[32]byte]struct{}, len(selected))
	claimed := make(map[types.OutPoint][32]byte)
	for _, entry := range selected {
		if _, duplicate := selectedIDs[entry.TxID]; duplicate {
			h.t.Fatalf("%s: selected tx %x appears twice", context, entry.TxID)
		}
		selectedIDs[entry.TxID] = struct{}{}
		for _, spent := range entry.SpentOutPoints {
			if previous, exists := claimed[spent]; exists {
				h.t.Fatalf("%s: selected txs %x and %x both spend %v", context, previous, entry.TxID, spent)
			}
			claimed[spent] = entry.TxID
			if _, ok := h.chain[spent]; ok {
				continue
			}
			if _, ok := selectedIDs[spent.TxID]; ok {
				continue
			}
			if !selectionContainsTxID(selected, spent.TxID) {
				h.t.Fatalf("%s: selected child %x spends missing parent %x", context, entry.TxID, spent.TxID)
			}
		}
		assertSnapshotDeltasMatchTx(h.t, context, entry)
	}
}

func (h *mempoolInvariantHarness) assertReindexStable(context string) {
	h.t.Helper()
	h.pool.mu.Lock()
	defer h.pool.mu.Unlock()
	before := h.poolFingerprintLocked()
	h.pool.reindex()
	after := h.poolFingerprintLocked()
	if !reflect.DeepEqual(before, after) {
		h.t.Fatalf("%s: pool state changed after reindex\nbefore: %#v\nafter:  %#v", context, before, after)
	}
}

type poolInvariantFingerprint struct {
	Spent      map[types.OutPoint][32]byte
	Entries    map[[32]byte]entryInvariantFingerprint
	Orphans    map[[32]byte]orphanInvariantFingerprint
	OrphanDeps map[types.OutPoint][][32]byte
	Candidates []candidateInvariantFingerprint
}

type entryInvariantFingerprint struct {
	Parents         [][32]byte
	Children        [][32]byte
	AncestorCount   int
	AncestorSize    int
	AncestorFees    uint64
	DescendantCount int
	DescendantSize  int
	DescendantFees  uint64
}

type orphanInvariantFingerprint struct {
	Missing      []types.OutPoint
	MissingCount int
}

type candidateInvariantFingerprint struct {
	TxID    [32]byte
	Entries [][32]byte
	Fee     uint64
	Size    int
}

func (h *mempoolInvariantHarness) poolFingerprintLocked() poolInvariantFingerprint {
	fp := poolInvariantFingerprint{
		Spent:      make(map[types.OutPoint][32]byte, len(h.pool.spent)),
		Entries:    make(map[[32]byte]entryInvariantFingerprint, len(h.pool.entries)),
		Orphans:    make(map[[32]byte]orphanInvariantFingerprint, len(h.pool.orphans)),
		OrphanDeps: make(map[types.OutPoint][][32]byte, len(h.pool.orphanDeps)),
	}
	for out, txid := range h.pool.spent {
		fp.Spent[out] = txid
	}
	for txid, entry := range h.pool.entries {
		fp.Entries[txid] = entryInvariantFingerprint{
			Parents:         sortedTxIDSet(entry.Parents),
			Children:        sortedTxIDSet(entry.Children),
			AncestorCount:   entry.AncestorCount,
			AncestorSize:    entry.AncestorSize,
			AncestorFees:    entry.AncestorFees,
			DescendantCount: entry.DescendantCount,
			DescendantSize:  entry.DescendantSize,
			DescendantFees:  entry.DescendantFees,
		}
	}
	for txid, orphan := range h.pool.orphans {
		missing := outPointSetKeys(orphan.Missing)
		sortOutPoints(missing)
		fp.Orphans[txid] = orphanInvariantFingerprint{Missing: missing, MissingCount: orphan.MissingCount}
	}
	for out, waiters := range h.pool.orphanDeps {
		copied := append([][32]byte(nil), waiters...)
		sortTxIDs(copied)
		fp.OrphanDeps[out] = copied
	}
	for _, candidate := range h.pool.cachedPackageCandidatesLocked() {
		entries := make([][32]byte, 0, len(candidate.Entries))
		for _, entry := range candidate.Entries {
			entries = append(entries, entry.TxID)
		}
		fp.Candidates = append(fp.Candidates, candidateInvariantFingerprint{
			TxID:    candidate.TxID,
			Entries: entries,
			Fee:     candidate.Fee,
			Size:    candidate.Size,
		})
	}
	return fp
}

func assertEntryDeltasMatchTx(t testing.TB, context string, entry *Entry) {
	t.Helper()
	if len(entry.SpentOutPoints) != len(entry.Tx.Base.Inputs) {
		t.Fatalf("%s: entry %x spent delta len = %d, want %d", context, entry.TxID, len(entry.SpentOutPoints), len(entry.Tx.Base.Inputs))
	}
	for i, input := range entry.Tx.Base.Inputs {
		if entry.SpentOutPoints[i] != input.PrevOut {
			t.Fatalf("%s: entry %x spent delta[%d] = %v, want %v", context, entry.TxID, i, entry.SpentOutPoints[i], input.PrevOut)
		}
	}
	if len(entry.CreatedLeaves) != len(entry.Tx.Base.Outputs) {
		t.Fatalf("%s: entry %x created delta len = %d, want %d", context, entry.TxID, len(entry.CreatedLeaves), len(entry.Tx.Base.Outputs))
	}
	for i, output := range entry.Tx.Base.Outputs {
		assertLeafMatchesOutput(t, context, entry.TxID, uint32(i), entry.CreatedLeaves[i], output)
	}
}

func assertSnapshotDeltasMatchTx(t testing.TB, context string, entry SnapshotEntry) {
	t.Helper()
	if len(entry.SpentOutPoints) != len(entry.Tx.Base.Inputs) {
		t.Fatalf("%s: selected %x spent delta len = %d, want %d", context, entry.TxID, len(entry.SpentOutPoints), len(entry.Tx.Base.Inputs))
	}
	for i, input := range entry.Tx.Base.Inputs {
		if entry.SpentOutPoints[i] != input.PrevOut {
			t.Fatalf("%s: selected %x spent delta[%d] = %v, want %v", context, entry.TxID, i, entry.SpentOutPoints[i], input.PrevOut)
		}
	}
	if len(entry.CreatedLeaves) != len(entry.Tx.Base.Outputs) {
		t.Fatalf("%s: selected %x created delta len = %d, want %d", context, entry.TxID, len(entry.CreatedLeaves), len(entry.Tx.Base.Outputs))
	}
	for i, output := range entry.Tx.Base.Outputs {
		assertLeafMatchesOutput(t, context, entry.TxID, uint32(i), entry.CreatedLeaves[i], output)
	}
}

func assertLeafMatchesOutput(t testing.TB, context string, txid [32]byte, vout uint32, leaf utreexo.UtxoLeaf, output types.TxOutput) {
	t.Helper()
	wantOut := types.OutPoint{TxID: txid, Vout: vout}
	if leaf.OutPoint != wantOut {
		t.Fatalf("%s: leaf outpoint = %v, want %v", context, leaf.OutPoint, wantOut)
	}
	expected := consensus.UtxoEntryFromOutput(output)
	if leaf.Type != expected.Type || leaf.ValueAtoms != expected.ValueAtoms || leaf.Payload32 != expected.Payload32 || leaf.PubKey != expected.PubKey {
		t.Fatalf("%s: leaf for %x:%d = %+v, want normalized output %+v", context, txid, vout, leaf, expected)
	}
}

func addTxIDSet(sets map[[32]byte]map[[32]byte]struct{}, key [32]byte, value [32]byte) {
	if sets[key] == nil {
		sets[key] = make(map[[32]byte]struct{})
	}
	sets[key][value] = struct{}{}
}

func collectInvariantRelatives(txid [32]byte, graph map[[32]byte]map[[32]byte]struct{}) map[[32]byte]struct{} {
	out := make(map[[32]byte]struct{})
	stack := txidSetKeys(graph[txid])
	for len(stack) != 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if _, seen := out[current]; seen {
			continue
		}
		out[current] = struct{}{}
		for next := range graph[current] {
			stack = append(stack, next)
		}
	}
	return out
}

func containsTxID(ids [][32]byte, want [32]byte) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func selectionContainsTxID(entries []SnapshotEntry, want [32]byte) bool {
	for _, entry := range entries {
		if entry.TxID == want {
			return true
		}
	}
	return false
}

func sortedTxIDSet(in map[[32]byte]struct{}) [][32]byte {
	out := txidSetKeys(in)
	sortTxIDs(out)
	return out
}

func sortOutPoints(out []types.OutPoint) {
	sort.Slice(out, func(i, j int) bool {
		if cmp := bytes.Compare(out[i].TxID[:], out[j].TxID[:]); cmp != 0 {
			return cmp < 0
		}
		return out[i].Vout < out[j].Vout
	})
}
