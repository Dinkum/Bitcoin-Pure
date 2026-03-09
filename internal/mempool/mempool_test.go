package mempool

import (
	"bytes"
	"errors"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func signerKeyHash(seed byte) [32]byte {
	pubKey, _ := crypto.SignSchnorrForTest([32]byte{seed}, &[32]byte{})
	return crypto.KeyHash(&pubKey)
}

func testCoinbase(height uint64, outputs []types.TxOutput) types.Transaction {
	return types.Transaction{
		Base: types.TxBase{
			Version:        1,
			CoinbaseHeight: &height,
			Outputs:        outputs,
		},
	}
}

func spendTx(t *testing.T, spenderSeed byte, prevOut types.OutPoint, value uint64, recipientSeed byte, fee uint64) types.Transaction {
	t.Helper()
	if fee >= value {
		t.Fatalf("fee %d must be less than value %d", fee, value)
	}
	recipientKeyHash := signerKeyHash(recipientSeed)
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: value - fee, KeyHash: recipientKeyHash}},
		},
	}
	msg, err := consensus.Sighash(&tx, 0, []uint64{value})
	if err != nil {
		t.Fatal(err)
	}
	pubKey, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{PubKey: pubKey, Signature: sig}}}
	return tx
}

func orphanTx(t *testing.T, prevOut types.OutPoint, recipientSeed byte) types.Transaction {
	t.Helper()
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: 49, KeyHash: signerKeyHash(recipientSeed)}},
		},
		Auth: types.TxAuth{Entries: []types.TxAuthEntry{{}}},
	}
	return tx
}

func findSnapshot(t *testing.T, entries []SnapshotEntry, txid [32]byte) SnapshotEntry {
	t.Helper()
	for _, entry := range entries {
		if entry.TxID == txid {
			return entry
		}
	}
	t.Fatalf("missing txid %x", txid)
	return SnapshotEntry{}
}

func TestAcceptTxRejectsConflictingSpend(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{2}, Vout: 0}
	first := spendTx(t, 7, prevOut, 50, 8, 1)
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(7)},
	}
	if _, err := pool.AcceptTx(first, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept first tx: %v", err)
	}

	second := spendTx(t, 7, prevOut, 50, 9, 1)
	if _, err := pool.AcceptTx(second, utxos, consensus.DefaultConsensusRules()); !errors.Is(err, ErrInputAlreadySpent) {
		t.Fatalf("expected input already spent, got %v", err)
	}
}

func TestAcceptTxTracksAncestorsAndDescendants(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	childAdmission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept child: %v", err)
	}

	entries := pool.Snapshot()
	parentEntry := findSnapshot(t, entries, parentAdmission.TxID)
	childEntry := findSnapshot(t, entries, childAdmission.TxID)
	if parentEntry.DescendantCount != 2 {
		t.Fatalf("parent descendant count = %d, want 2", parentEntry.DescendantCount)
	}
	if childEntry.AncestorCount != 2 {
		t.Fatalf("child ancestor count = %d, want 2", childEntry.AncestorCount)
	}
	if childEntry.AncestorFees != 2 {
		t.Fatalf("child ancestor fees = %d, want 2", childEntry.AncestorFees)
	}
}

func TestAcceptTxRejectsDescendantLimit(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     2,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{3}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent, child, parentTxID, childTxID := makeThreeStepChain(t, prevOut)
	if _, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	if _, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}
	grandchild := spendTx(t, 3, types.OutPoint{TxID: childTxID, Vout: 0}, 48, 4, 1)
	if _, err := pool.AcceptTx(grandchild, utxos, consensus.DefaultConsensusRules()); !errors.Is(err, ErrTooManyDescendants) {
		t.Fatalf("expected descendant-limit rejection for chain rooted at %x/%x, got %v", parentTxID, childTxID, err)
	}
}

func TestAcceptTxStoresOrphanAndPromotesOnParent(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{4}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentTxID := consensus.TxID(&parent)
	child := spendTx(t, 2, types.OutPoint{TxID: parentTxID, Vout: 0}, 49, 3, 1)

	orphanAdmission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("store orphan: %v", err)
	}
	if !orphanAdmission.Orphaned {
		t.Fatalf("expected orphaned admission")
	}
	if pool.OrphanCount() != 1 {
		t.Fatalf("orphan count = %d, want 1", pool.OrphanCount())
	}

	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	if len(parentAdmission.Accepted) != 2 {
		t.Fatalf("accepted count = %d, want 2", len(parentAdmission.Accepted))
	}
	if parentAdmission.Accepted[0].TxID != parentTxID || parentAdmission.Accepted[1].TxID != consensus.TxID(&child) {
		t.Fatalf("unexpected accepted promotion order")
	}
	if pool.OrphanCount() != 0 {
		t.Fatalf("orphan count = %d, want 0", pool.OrphanCount())
	}
}

func TestAcceptTxEvictsOldestOrphan(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         1,
	})

	firstPrev := types.OutPoint{TxID: [32]byte{9}, Vout: 0}
	secondPrev := types.OutPoint{TxID: [32]byte{10}, Vout: 0}
	first, second := orphanTx(t, firstPrev, 1), orphanTx(t, secondPrev, 2)

	firstAdmission, err := pool.AcceptTx(first, consensus.UtxoSet{}, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("store first orphan: %v", err)
	}
	if !firstAdmission.Orphaned {
		t.Fatalf("expected first tx to be orphaned")
	}
	secondAdmission, err := pool.AcceptTx(second, consensus.UtxoSet{}, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("store second orphan: %v", err)
	}
	if secondAdmission.EvictedOrphans != 1 {
		t.Fatalf("evicted orphan count = %d, want 1", secondAdmission.EvictedOrphans)
	}
	if pool.OrphanCount() != 1 {
		t.Fatalf("orphan count = %d, want 1", pool.OrphanCount())
	}
	if _, ok := pool.orphans[consensus.TxID(&first)]; ok {
		t.Fatalf("expected oldest orphan to be evicted")
	}
	if _, ok := pool.orphans[consensus.TxID(&second)]; !ok {
		t.Fatalf("expected newest orphan to remain")
	}
}

func TestSelectForBlockExcludesSameBlockDescendantsAndReturnsLTOR(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{11}, Vout: 0}: {ValueAtoms: 100, KeyHash: signerKeyHash(1)},
		{TxID: [32]byte{12}, Vout: 0}: {ValueAtoms: 100, KeyHash: signerKeyHash(4)},
	}

	parent := spendTx(t, 1, types.OutPoint{TxID: [32]byte{11}, Vout: 0}, 100, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 99, 3, 20)
	childAdmission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept child: %v", err)
	}
	medium := spendTx(t, 4, types.OutPoint{TxID: [32]byte{12}, Vout: 0}, 100, 5, 10)
	mediumAdmission, err := pool.AcceptTx(medium, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept medium: %v", err)
	}

	selected, _ := pool.SelectForBlock(utxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 2 {
		t.Fatalf("selected count = %d, want 2", len(selected))
	}
	if selected[0].TxID == childAdmission.TxID || selected[1].TxID == childAdmission.TxID {
		t.Fatal("expected child spend to be excluded from same block selection")
	}
	if (selected[0].TxID != parentAdmission.TxID && selected[0].TxID != mediumAdmission.TxID) || (selected[1].TxID != parentAdmission.TxID && selected[1].TxID != mediumAdmission.TxID) {
		t.Fatalf("unexpected selected txids: got %x %x", selected[0].TxID, selected[1].TxID)
	}
	if bytes.Compare(selected[0].TxID[:], selected[1].TxID[:]) >= 0 {
		t.Fatalf("selection not in txid order: got %x before %x", selected[0].TxID, selected[1].TxID)
	}
}

func TestRemoveConfirmedEvictsConflictsAndDescendants(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{13}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}

	confirmed := spendTx(t, 1, prevOut, 50, 9, 1)
	block := &types.Block{
		Txs: []types.Transaction{
			testCoinbase(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: signerKeyHash(9)}}),
			confirmed,
		},
	}
	pool.RemoveConfirmed(block)
	if pool.Count() != 0 {
		t.Fatalf("mempool count = %d, want 0", pool.Count())
	}
}

func TestRemoveRecursiveUpdatesAncestorStatsIncrementally(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	parentPrevOut := types.OutPoint{TxID: [32]byte{14}, Vout: 0}
	unrelatedPrevOut := types.OutPoint{TxID: [32]byte{15}, Vout: 0}
	utxos := consensus.UtxoSet{
		parentPrevOut:    {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
		unrelatedPrevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(4)},
	}

	parent := spendTx(t, 1, parentPrevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	childAdmission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept child: %v", err)
	}
	unrelated := spendTx(t, 4, unrelatedPrevOut, 50, 5, 1)
	unrelatedAdmission, err := pool.AcceptTx(unrelated, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept unrelated: %v", err)
	}

	pool.removeRecursive(map[[32]byte]struct{}{childAdmission.TxID: {}})

	if pool.Count() != 2 {
		t.Fatalf("mempool count = %d, want 2", pool.Count())
	}
	entries := pool.Snapshot()
	parentEntry := findSnapshot(t, entries, parentAdmission.TxID)
	unrelatedEntry := findSnapshot(t, entries, unrelatedAdmission.TxID)
	if parentEntry.DescendantCount != 1 {
		t.Fatalf("parent descendant count = %d, want 1", parentEntry.DescendantCount)
	}
	if parentEntry.DescendantFees != parentEntry.Fee {
		t.Fatalf("parent descendant fees = %d, want %d", parentEntry.DescendantFees, parentEntry.Fee)
	}
	if unrelatedEntry.DescendantCount != 1 {
		t.Fatalf("unrelated descendant count = %d, want 1", unrelatedEntry.DescendantCount)
	}
	if pool.Get(childAdmission.TxID) != nil {
		t.Fatalf("expected removed child tx to be absent")
	}
}

func TestPrepareAdmissionAndCommitPreparedHandleSameBatchParents(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{21}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentTxID := consensus.TxID(&parent)
	child := spendTx(t, 2, types.OutPoint{TxID: parentTxID, Vout: 0}, 49, 3, 1)

	snapshot := pool.AdmissionSnapshot()
	preparedParent, err := pool.PrepareAdmission(parent, snapshot, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare parent: %v", err)
	}
	preparedChild, err := pool.PrepareAdmission(child, snapshot, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare child: %v", err)
	}
	if len(preparedChild.Missing) != 1 {
		t.Fatalf("prepared child missing count = %d, want 1", len(preparedChild.Missing))
	}

	parentAdmission, err := pool.CommitPrepared(preparedParent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("commit prepared parent: %v", err)
	}
	if len(parentAdmission.Accepted) != 1 {
		t.Fatalf("parent accepted count = %d, want 1", len(parentAdmission.Accepted))
	}
	childAdmission, err := pool.CommitPrepared(preparedChild, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("commit prepared child: %v", err)
	}
	if childAdmission.Orphaned {
		t.Fatalf("expected child to validate against live parent rather than remain orphaned")
	}
	if len(childAdmission.Accepted) != 1 || childAdmission.Accepted[0].TxID != consensus.TxID(&child) {
		t.Fatalf("unexpected child admission payload: %+v", childAdmission.Accepted)
	}
}

func TestAdvanceAdmissionSnapshotTracksSameBatchParents(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{23}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	child := spendTx(t, 2, types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}, 49, 3, 1)

	snapshot := pool.AdmissionSnapshot()
	parentPrepared, err := pool.PrepareAdmission(parent, snapshot, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare parent: %v", err)
	}
	parentAdmission, err := pool.CommitPrepared(parentPrepared, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("commit parent: %v", err)
	}
	if err := AdvanceAdmissionSnapshot(&snapshot, utxos, parentAdmission.Accepted); err != nil {
		t.Fatalf("advance snapshot: %v", err)
	}

	childPrepared, err := pool.PrepareAdmission(child, snapshot, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare child: %v", err)
	}
	if len(childPrepared.Missing) != 0 {
		t.Fatalf("expected child to resolve against advanced snapshot, missing=%d", len(childPrepared.Missing))
	}
}

func TestShortIDMatchesOnlyScansWantedIDs(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{41}, Vout: 0}: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
		{TxID: [32]byte{42}, Vout: 0}: {ValueAtoms: 50, KeyHash: signerKeyHash(2)},
	}
	first := spendTx(t, 1, types.OutPoint{TxID: [32]byte{41}, Vout: 0}, 50, 3, 1)
	second := spendTx(t, 2, types.OutPoint{TxID: [32]byte{42}, Vout: 0}, 50, 4, 1)
	firstAdmission, err := pool.AcceptTx(first, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	secondAdmission, err := pool.AcceptTx(second, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept second: %v", err)
	}

	shortIDFn := func(txid [32]byte) uint64 {
		return uint64(txid[0])<<8 | uint64(txid[1])
	}
	wantFirst := shortIDFn(firstAdmission.TxID)
	wantSecond := shortIDFn(secondAdmission.TxID)
	matches := pool.ShortIDMatches(shortIDFn, map[uint64]struct{}{
		wantFirst:  {},
		0xffff:     {},
		wantSecond: {},
	})
	if len(matches[wantFirst]) != 1 {
		t.Fatalf("first short id matches = %d, want 1", len(matches[wantFirst]))
	}
	if len(matches[wantSecond]) != 1 {
		t.Fatalf("second short id matches = %d, want 1", len(matches[wantSecond]))
	}
	if len(matches[0xffff]) != 0 {
		t.Fatalf("unexpected unmatched short id candidates: %d", len(matches[0xffff]))
	}
}

func TestBatchLookupHelpersPreserveRequestedOrderAndMisses(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{51}, Vout: 0}: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
		{TxID: [32]byte{52}, Vout: 0}: {ValueAtoms: 50, KeyHash: signerKeyHash(2)},
	}
	first := spendTx(t, 1, types.OutPoint{TxID: [32]byte{51}, Vout: 0}, 50, 3, 1)
	second := spendTx(t, 2, types.OutPoint{TxID: [32]byte{52}, Vout: 0}, 50, 4, 1)
	firstAdmission, err := pool.AcceptTx(first, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	secondAdmission, err := pool.AcceptTx(second, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept second: %v", err)
	}

	requested := [][32]byte{secondAdmission.TxID, {0xff}, firstAdmission.TxID}
	missing := pool.MissingTxIDs(requested)
	if len(missing) != 1 || missing[0] != ([32]byte{0xff}) {
		t.Fatalf("missing txids = %x, want one miss", missing)
	}

	txs := pool.TransactionsByID(requested)
	if len(txs) != 2 {
		t.Fatalf("tx count = %d, want 2", len(txs))
	}
	if consensus.TxID(&txs[0]) != secondAdmission.TxID || consensus.TxID(&txs[1]) != firstAdmission.TxID {
		t.Fatalf("returned tx order mismatch: got %x then %x", consensus.TxID(&txs[0]), consensus.TxID(&txs[1]))
	}
}

func TestSelectionCandidateCountTracksIncrementalUpdates(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{22}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	childAdmission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept child: %v", err)
	}
	if pool.SelectionCandidateCount() != 2 {
		t.Fatalf("selection candidate count = %d, want 2", pool.SelectionCandidateCount())
	}

	pool.removeRecursive(map[[32]byte]struct{}{childAdmission.TxID: {}})
	if pool.SelectionCandidateCount() != 1 {
		t.Fatalf("selection candidate count after remove = %d, want 1", pool.SelectionCandidateCount())
	}
}

func TestAppendForBlockOnlyReturnsNewSelections(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	rootPrev := types.OutPoint{TxID: [32]byte{31}, Vout: 0}
	otherPrev := types.OutPoint{TxID: [32]byte{32}, Vout: 0}
	utxos := consensus.UtxoSet{
		rootPrev:  {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
		otherPrev: {ValueAtoms: 50, KeyHash: signerKeyHash(4)},
	}

	parent := spendTx(t, 1, rootPrev, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}

	preBlockUtxos := cloneUtxos(utxos)
	currentUtxos := cloneUtxos(utxos)
	selected, _ := pool.SelectForBlock(currentUtxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 1 {
		t.Fatalf("initial selection len = %d, want 1", len(selected))
	}

	late := spendTx(t, 4, otherPrev, 50, 5, 1)
	lateAdmission, err := pool.AcceptTx(late, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept late tx: %v", err)
	}

	appended, appendedFees := pool.AppendForBlock(preBlockUtxos, currentUtxos, consensus.DefaultConsensusRules(), 1_000_000, selected)
	if len(appended) != 1 {
		t.Fatalf("appended len = %d, want 1", len(appended))
	}
	if appended[0].TxID != lateAdmission.TxID {
		t.Fatalf("appended txid = %x, want %x", appended[0].TxID, lateAdmission.TxID)
	}
	if appendedFees != lateAdmission.Summary.Fee {
		t.Fatalf("appended fees = %d, want %d", appendedFees, lateAdmission.Summary.Fee)
	}
}

func TestSelectForBlockOverlayKeepsBaseUTXOMapImmutable(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	rootPrev := types.OutPoint{TxID: [32]byte{33}, Vout: 0}
	utxos := consensus.UtxoSet{
		rootPrev: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}

	parent := spendTx(t, 1, rootPrev, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}

	selected, totalFees, overlay := pool.SelectForBlockOverlay(utxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 1 {
		t.Fatalf("selected len = %d, want 1", len(selected))
	}
	if totalFees != parentAdmission.Summary.Fee {
		t.Fatalf("selected fees = %d, want %d", totalFees, parentAdmission.Summary.Fee)
	}
	if _, ok := utxos[rootPrev]; !ok {
		t.Fatal("base utxo map was mutated during overlay selection")
	}
	parentOut := types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}
	if _, ok := overlay.Lookup(parentOut); !ok {
		t.Fatal("overlay missing selected transaction output")
	}
	if _, ok := overlay.Lookup(rootPrev); ok {
		t.Fatal("overlay still exposes spent root prevout")
	}
}

func TestAppendForBlockOverlayExtendsTentativeSelectionWithoutMaterializingBase(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	rootPrev := types.OutPoint{TxID: [32]byte{34}, Vout: 0}
	otherPrev := types.OutPoint{TxID: [32]byte{35}, Vout: 0}
	utxos := consensus.UtxoSet{
		rootPrev:  {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
		otherPrev: {ValueAtoms: 50, KeyHash: signerKeyHash(4)},
	}

	parent := spendTx(t, 1, rootPrev, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	selected, _, overlay := pool.SelectForBlockOverlay(utxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 1 {
		t.Fatalf("initial selected len = %d, want 1", len(selected))
	}

	late := spendTx(t, 4, otherPrev, 50, 5, 1)
	lateAdmission, err := pool.AcceptTx(late, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept late tx: %v", err)
	}

	appended, appendedFees := pool.AppendForBlockOverlay(utxos, overlay, consensus.DefaultConsensusRules(), 1_000_000, selected)
	if len(appended) != 1 {
		t.Fatalf("appended len = %d, want 1", len(appended))
	}
	if appended[0].TxID != lateAdmission.TxID {
		t.Fatalf("appended txid = %x, want %x", appended[0].TxID, lateAdmission.TxID)
	}
	if appendedFees != lateAdmission.Summary.Fee {
		t.Fatalf("appended fees = %d, want %d", appendedFees, lateAdmission.Summary.Fee)
	}
	if _, ok := utxos[otherPrev]; !ok {
		t.Fatal("base utxo map was mutated during overlay append")
	}
	lateOut := types.OutPoint{TxID: lateAdmission.TxID, Vout: 0}
	if _, ok := overlay.Lookup(lateOut); !ok {
		t.Fatal("overlay missing appended transaction output")
	}
	parentOut := types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}
	if _, ok := overlay.Lookup(parentOut); !ok {
		t.Fatal("overlay lost earlier tentative selection output")
	}
}

func TestSelectForBlockInPlaceAdvancesProvidedUTXOView(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	rootPrev := types.OutPoint{TxID: [32]byte{41}, Vout: 0}
	utxos := consensus.UtxoSet{
		rootPrev: {ValueAtoms: 50, KeyHash: signerKeyHash(1)},
	}
	parent := spendTx(t, 1, rootPrev, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	childTxID := consensus.TxID(&child)
	parentTxID := parentAdmission.TxID
	if _, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}

	currentUtxos := cloneUtxos(utxos)
	selected, _ := pool.SelectForBlockInPlace(currentUtxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 1 {
		t.Fatalf("selected len = %d, want 1", len(selected))
	}
	if _, ok := currentUtxos[rootPrev]; ok {
		t.Fatalf("expected original prevout to be spent from in-place view")
	}
	if _, ok := currentUtxos[types.OutPoint{TxID: childTxID, Vout: 0}]; ok {
		t.Fatalf("did not expect descendant output to exist in selected block view")
	}
	if _, ok := currentUtxos[types.OutPoint{TxID: parentTxID, Vout: 0}]; !ok {
		t.Fatalf("expected selected output to remain in in-place view")
	}
}

func makeThreeStepChain(t *testing.T, prevOut types.OutPoint) (types.Transaction, types.Transaction, [32]byte, [32]byte) {
	t.Helper()
	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentTxID := consensus.TxID(&parent)
	child := spendTx(t, 2, types.OutPoint{TxID: parentTxID, Vout: 0}, 49, 3, 1)
	childTxID := consensus.TxID(&child)
	return parent, child, parentTxID, childTxID
}
