package mempool

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"bytes"
	"strings"
	"testing"
)

func TestSelectForBlockIncludesSameBlockDescendantsAndReturnsLTOR(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{11}, Vout: 0}: {ValueAtoms: 100, PubKey: signerPubKey(1)},
		{TxID: [32]byte{12}, Vout: 0}: {ValueAtoms: 100, PubKey: signerPubKey(4)},
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
	if len(selected) != 3 {
		t.Fatalf("selected count = %d, want 3", len(selected))
	}
	for _, txid := range [][32]byte{parentAdmission.TxID, childAdmission.TxID, mediumAdmission.TxID} {
		if !containsSnapshotTxID(selected, txid) {
			t.Fatalf("selection missing txid %x: %+v", txid, selected)
		}
	}
	for i := 1; i < len(selected); i++ {
		if bytes.Compare(selected[i-1].TxID[:], selected[i].TxID[:]) >= 0 {
			t.Fatalf("selection not in txid order: got %x before %x", selected[i-1].TxID, selected[i].TxID)
		}
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
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
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
			testCoinbase(1, []types.TxOutput{{ValueAtoms: 1, PubKey: signerPubKey(9)}}),
			confirmed,
		},
	}
	pool.RemoveConfirmed(block)
	if pool.Count() != 0 {
		t.Fatalf("mempool count = %d, want 0", pool.Count())
	}
}

func TestRemoveConfirmedPreservesUnconfirmedDescendants(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{14}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
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

	block := &types.Block{
		Txs: []types.Transaction{
			testCoinbase(1, []types.TxOutput{{ValueAtoms: 1, PubKey: signerPubKey(9)}}),
			parent,
		},
	}
	pool.RemoveConfirmed(block)
	if pool.Count() != 1 {
		t.Fatalf("mempool count = %d, want 1", pool.Count())
	}
	entry := pool.entries[childAdmission.TxID]
	if entry == nil {
		t.Fatalf("child missing from mempool after parent confirmed")
	}
	if len(entry.Parents) != 0 {
		t.Fatalf("child parents = %d, want 0 after parent moved to chain", len(entry.Parents))
	}
	if entry.AncestorCount != 1 || entry.DescendantCount != 1 {
		t.Fatalf("child counts ancestor=%d descendant=%d, want 1/1", entry.AncestorCount, entry.DescendantCount)
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
		parentPrevOut:    {ValueAtoms: 50, PubKey: signerPubKey(1)},
		unrelatedPrevOut: {ValueAtoms: 50, PubKey: signerPubKey(4)},
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
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentTxID := consensus.TxID(&parent)
	child := spendTx(t, 2, types.OutPoint{TxID: parentTxID, Vout: 0}, 49, 3, 1)

	snapshot := pool.AdmissionSnapshot()
	preparedParent, err := pool.PrepareAdmissionWithLookup(parent, snapshot, consensus.LookupFromSet(utxos), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare parent: %v", err)
	}
	preparedChild, err := pool.PrepareAdmissionWithLookup(child, snapshot, consensus.LookupFromSet(utxos), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare child: %v", err)
	}
	if len(preparedChild.Missing) != 1 {
		t.Fatalf("prepared child missing count = %d, want 1", len(preparedChild.Missing))
	}

	parentAdmission, err := pool.CommitPrepared(preparedParent, utxos, [32]byte{}, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("commit prepared parent: %v", err)
	}
	if len(parentAdmission.Accepted) != 1 {
		t.Fatalf("parent accepted count = %d, want 1", len(parentAdmission.Accepted))
	}
	childAdmission, err := pool.CommitPrepared(preparedChild, utxos, [32]byte{}, consensus.DefaultConsensusRules())
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
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	child := spendTx(t, 2, types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}, 49, 3, 1)

	snapshot := pool.AdmissionSnapshot()
	parentPrepared, err := pool.PrepareAdmissionWithLookup(parent, snapshot, consensus.LookupFromSet(utxos), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare parent: %v", err)
	}
	parentAdmission, err := pool.CommitPrepared(parentPrepared, utxos, [32]byte{}, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("commit parent: %v", err)
	}
	if err := AdvanceAdmissionSnapshot(&snapshot, utxos, parentAdmission.Accepted); err != nil {
		t.Fatalf("advance snapshot: %v", err)
	}

	childPrepared, err := pool.PrepareAdmissionWithLookup(child, snapshot, consensus.LookupFromSet(utxos), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare child: %v", err)
	}
	if len(childPrepared.Missing) != 0 {
		t.Fatalf("expected child to resolve against advanced snapshot, missing=%d", len(childPrepared.Missing))
	}
}

func TestPrepareAdmissionSharedResolvesAgainstLiveParents(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{24}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}

	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	view := pool.AcquireSharedAdmissionView()
	defer view.Release()

	prepared, err := pool.PrepareAdmissionSharedWithLookup(child, view, consensus.LookupFromSet(utxos), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare child against shared view: %v", err)
	}
	if len(prepared.Missing) != 0 {
		t.Fatalf("shared view child missing count = %d, want 0", len(prepared.Missing))
	}
	if len(prepared.Parents) != 1 {
		t.Fatalf("shared view parent count = %d, want 1", len(prepared.Parents))
	}
	if _, ok := prepared.Parents[parentAdmission.TxID]; !ok {
		t.Fatalf("shared view missing live parent %x", parentAdmission.TxID)
	}
}

func TestCommitPreparedReusesPreparedStateWhenEpochAndTipMatch(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{25}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}
	tx := spendTx(t, 1, prevOut, 50, 2, 1)
	prepared, err := pool.PrepareAdmissionWithLookup(tx, pool.AdmissionSnapshot(), consensus.LookupFromSet(utxos), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare admission: %v", err)
	}
	prepared.PreparedTip = commitTipHash(1)
	prepared.HasPreparedTip = true

	// A matching prepared tip and unchanged mempool epoch should let commit reuse
	// the prepared resolution/validation result instead of touching the fallback
	// chain UTXO view again.
	admission, err := pool.CommitPrepared(prepared, consensus.UtxoSet{}, commitTipHash(1), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("commit prepared: %v", err)
	}
	if admission.Orphaned {
		t.Fatalf("expected prepared tx to remain validated rather than orphaned")
	}
	if len(admission.Accepted) != 1 || admission.Accepted[0].TxID != prepared.TxID {
		t.Fatalf("unexpected prepared admission payload: %+v", admission.Accepted)
	}
}

func TestCommitPreparedFallsBackWhenPreparedTipIsStale(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{26}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}
	tx := spendTx(t, 1, prevOut, 50, 2, 1)
	prepared, err := pool.PrepareAdmissionWithLookup(tx, pool.AdmissionSnapshot(), consensus.LookupFromSet(utxos), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("prepare admission: %v", err)
	}
	prepared.PreparedTip = commitTipHash(2)
	prepared.HasPreparedTip = true

	admission, err := pool.CommitPrepared(prepared, consensus.UtxoSet{}, commitTipHash(3), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("commit prepared fallback: %v", err)
	}
	if !admission.Orphaned {
		t.Fatalf("expected stale prepared tip to fall back to live resolution and orphan the tx")
	}
}

func TestPrepareAdmissionSharedRejectsForeignView(t *testing.T) {
	pool := NewWithConfig(DefaultConfig())
	other := NewWithConfig(DefaultConfig())
	view := other.AcquireSharedAdmissionView()
	defer view.Release()

	tx := spendTx(t, 1, types.OutPoint{TxID: [32]byte{25}, Vout: 0}, 50, 2, 1)
	_, err := pool.PrepareAdmissionSharedWithLookup(tx, view, consensus.LookupFromSet(consensus.UtxoSet{
		{TxID: [32]byte{25}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}), consensus.DefaultConsensusRules())
	if err == nil || !strings.Contains(err.Error(), "different mempool") {
		t.Fatalf("foreign shared view error = %v, want different mempool", err)
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
		rootPrev:  {ValueAtoms: 50, PubKey: signerPubKey(1)},
		otherPrev: {ValueAtoms: 50, PubKey: signerPubKey(4)},
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
	if len(selected) != 2 {
		t.Fatalf("initial selection len = %d, want 2", len(selected))
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
		rootPrev: {ValueAtoms: 50, PubKey: signerPubKey(1)},
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
	if len(selected[0].SignatureChecks) != 0 {
		t.Fatal("selection snapshots should stay lightweight and skip redundant signature rechecks")
	}
	if len(selected[0].SpentOutPoints) != len(parent.Base.Inputs) {
		t.Fatalf("selection snapshot spent delta len = %d, want %d", len(selected[0].SpentOutPoints), len(parent.Base.Inputs))
	}
	if len(selected[0].CreatedLeaves) != len(parent.Base.Outputs) {
		t.Fatalf("selection snapshot created delta len = %d, want %d", len(selected[0].CreatedLeaves), len(parent.Base.Outputs))
	}
	if snapshot := findSnapshot(t, pool.Snapshot(), parentAdmission.TxID); len(snapshot.SignatureChecks) != 0 {
		t.Fatal("generic mempool snapshots should stay lightweight and exclude selection-only signature checks")
	} else if len(snapshot.SpentOutPoints) != 0 || len(snapshot.CreatedLeaves) != 0 {
		t.Fatal("generic mempool snapshots should stay lightweight and exclude selection-only accumulator deltas")
	}
}

func TestSelectForBlockKeepsBaseUTXOMapImmutable(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	rootPrev := types.OutPoint{TxID: [32]byte{44}, Vout: 0}
	utxos := consensus.UtxoSet{
		rootPrev: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}

	parent := spendTx(t, 1, rootPrev, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}

	selected, totalFees := pool.SelectForBlock(utxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 1 {
		t.Fatalf("selected len = %d, want 1", len(selected))
	}
	if totalFees != parentAdmission.Summary.Fee {
		t.Fatalf("selected fees = %d, want %d", totalFees, parentAdmission.Summary.Fee)
	}
	if _, ok := utxos[rootPrev]; !ok {
		t.Fatal("base utxo map was mutated during selection")
	}
	if _, ok := utxos[types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}]; ok {
		t.Fatal("selection leaked created outputs into the base utxo map")
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
		rootPrev:  {ValueAtoms: 50, PubKey: signerPubKey(1)},
		otherPrev: {ValueAtoms: 50, PubKey: signerPubKey(4)},
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

func TestSelectForBlockOverlayTracksTentativeState(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	rootPrev := types.OutPoint{TxID: [32]byte{41}, Vout: 0}
	utxos := consensus.UtxoSet{
		rootPrev: {ValueAtoms: 50, PubKey: signerPubKey(1)},
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

	selected, _, overlay := pool.SelectForBlockOverlay(utxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 2 {
		t.Fatalf("selected len = %d, want 2", len(selected))
	}
	if _, ok := overlay.Lookup(rootPrev); ok {
		t.Fatalf("expected original prevout to be spent from tentative overlay")
	}
	if _, ok := overlay.Lookup(types.OutPoint{TxID: childTxID, Vout: 0}); !ok {
		t.Fatalf("expected descendant output to exist in selected block view")
	}
	if _, ok := overlay.Lookup(types.OutPoint{TxID: parentTxID, Vout: 0}); ok {
		t.Fatalf("expected same-block parent output to be spent in tentative overlay")
	}
}

func TestCandidateLessUsesExactFeerateOrdering(t *testing.T) {
	// These ratios are distinct but close enough that selection ordering should
	// stay on exact integer math rather than float rounding behavior.
	higherRate := packageCandidate{
		TxID: [32]byte{1},
		Fee:  1_000_002,
		Size: 1_000_000,
	}
	lowerRate := packageCandidate{
		TxID: [32]byte{2},
		Fee:  1_000_000,
		Size: 999_999,
	}
	if !candidateLess(higherRate, lowerRate) {
		t.Fatal("higher feerate candidate should sort first")
	}
	if candidateLess(lowerRate, higherRate) {
		t.Fatal("lower feerate candidate should sort after the higher feerate one")
	}

	tieByRateLowerFee := packageCandidate{
		TxID: [32]byte{3},
		Fee:  20,
		Size: 10,
	}
	tieByRateHigherFee := packageCandidate{
		TxID: [32]byte{4},
		Fee:  40,
		Size: 20,
	}
	if !candidateLess(tieByRateHigherFee, tieByRateLowerFee) {
		t.Fatal("equal-feerate candidates should break ties on absolute fee")
	}
}
