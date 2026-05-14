package mempool

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	crand "crypto/rand"
	"errors"
	"testing"
)

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
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(7)},
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

func TestPQParentOutputSurvivesAdmissionAndSelection(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	verificationKey, privateKey, err := crypto.GenerateMLDSA65Key(crand.Reader)
	if err != nil {
		t.Fatalf("GenerateMLDSA65Key: %v", err)
	}
	pqLock := consensus.PQLock(verificationKey)
	prevOut := types.OutPoint{TxID: [32]byte{0x31}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: consensus.UtxoEntryFromOutput(types.NewXOnlyOutput(100, signerPubKey(1))),
	}

	parent := signedXOnlySpendToOutputs(t, 1, prevOut, 100, []types.TxOutput{
		types.NewPQLockOutput(90, pqLock),
	})
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept PQ parent: %v", err)
	}
	parentOut := types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}
	child := signedPQSpendToOutputs(t, parentOut, 90, verificationKey, privateKey, []types.TxOutput{
		types.NewXOnlyOutput(80, signerPubKey(3)),
	})
	childAdmission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept PQ child: %v", err)
	}

	selected, _, overlay := pool.SelectForBlockOverlay(utxos, consensus.DefaultConsensusRules(), 1_000_000)
	if len(selected) != 2 {
		t.Fatalf("selected %d txs, want parent and child", len(selected))
	}
	if !containsSnapshotTxID(selected, parentAdmission.TxID) || !containsSnapshotTxID(selected, childAdmission.TxID) {
		t.Fatalf("selection missing parent/child: parent %x child %x selected %+v", parentAdmission.TxID, childAdmission.TxID, selected)
	}
	parentSelected := findSnapshot(t, selected, parentAdmission.TxID)
	if len(parentSelected.CreatedLeaves) != 1 {
		t.Fatalf("selected parent created leaves = %d, want 1", len(parentSelected.CreatedLeaves))
	}
	parentLeaf := parentSelected.CreatedLeaves[0]
	if parentLeaf.Type != types.OutputPQLock32 || parentLeaf.Payload32 != pqLock || parentLeaf.PubKey != ([32]byte{}) {
		t.Fatalf("PQ parent leaf lost typed payload: %+v", parentLeaf)
	}
	if _, ok := overlay.Lookup(parentOut); ok {
		t.Fatal("selected overlay retained spent PQ parent output")
	}
	childOut := types.OutPoint{TxID: childAdmission.TxID, Vout: 0}
	childEntry, ok := overlay.Lookup(childOut)
	if !ok {
		t.Fatal("selected overlay missing child output")
	}
	if childEntry.Type != types.OutputXOnlyP2PK || childEntry.Payload32 == ([32]byte{}) {
		t.Fatalf("child output not normalized in overlay: %+v", childEntry)
	}
}

func TestSnapshotSharedCachesAndInvalidatesOnEpochChange(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{41}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}

	sharedA := pool.SnapshotShared()
	sharedB := pool.SnapshotShared()
	if len(sharedA) != 1 || len(sharedB) != 1 {
		t.Fatalf("shared snapshot lens = %d/%d, want 1", len(sharedA), len(sharedB))
	}
	if &sharedA[0] != &sharedB[0] {
		t.Fatal("expected SnapshotShared to reuse the cached slice while the epoch is stable")
	}

	copied := pool.Snapshot()
	copied[0].TxID = [32]byte{99}
	sharedC := pool.SnapshotShared()
	if sharedC[0].TxID != parentAdmission.TxID {
		t.Fatal("mutating Snapshot() result should not affect cached shared snapshot")
	}

	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}

	sharedAfter := pool.SnapshotShared()
	if len(sharedAfter) != 2 {
		t.Fatalf("shared snapshot len after mutation = %d, want 2", len(sharedAfter))
	}
	if &sharedAfter[0] == &sharedA[0] {
		t.Fatal("expected epoch change to invalidate the cached shared snapshot")
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
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
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
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
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

func TestAcceptTxPromotesOrphanChainsThroughReadyQueue(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{6}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)},
	}

	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentTxID := consensus.TxID(&parent)
	child := spendTx(t, 2, types.OutPoint{TxID: parentTxID, Vout: 0}, 49, 3, 1)
	childTxID := consensus.TxID(&child)
	grandchild := spendTx(t, 3, types.OutPoint{TxID: childTxID, Vout: 0}, 48, 4, 1)

	if admission, err := pool.AcceptTx(grandchild, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("store grandchild orphan: %v", err)
	} else if !admission.Orphaned {
		t.Fatal("expected grandchild orphaned")
	}
	if admission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("store child orphan: %v", err)
	} else if !admission.Orphaned {
		t.Fatal("expected child orphaned")
	}

	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	if len(parentAdmission.Accepted) != 3 {
		t.Fatalf("accepted count = %d, want 3", len(parentAdmission.Accepted))
	}
	if parentAdmission.Accepted[0].TxID != parentTxID || parentAdmission.Accepted[1].TxID != childTxID || parentAdmission.Accepted[2].TxID != consensus.TxID(&grandchild) {
		t.Fatalf("unexpected promotion order: got %x %x %x", parentAdmission.Accepted[0].TxID, parentAdmission.Accepted[1].TxID, parentAdmission.Accepted[2].TxID)
	}
	if pool.OrphanCount() != 0 {
		t.Fatalf("orphan count = %d, want 0", pool.OrphanCount())
	}
}

func TestAcceptTxPromotesMultiInputOrphanOnlyWhenAllParentsReady(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	leftPrev := types.OutPoint{TxID: [32]byte{7}, Vout: 0}
	rightPrev := types.OutPoint{TxID: [32]byte{8}, Vout: 0}
	utxos := consensus.UtxoSet{
		leftPrev:  {ValueAtoms: 50, PubKey: signerPubKey(1)},
		rightPrev: {ValueAtoms: 50, PubKey: signerPubKey(2)},
	}

	left := spendTx(t, 1, leftPrev, 50, 3, 1)
	right := spendTx(t, 2, rightPrev, 50, 4, 1)
	join := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{
				{PrevOut: types.OutPoint{TxID: consensus.TxID(&left), Vout: 0}},
				{PrevOut: types.OutPoint{TxID: consensus.TxID(&right), Vout: 0}},
			},
			Outputs: []types.TxOutput{{ValueAtoms: 97, PubKey: signerPubKey(5)}},
		},
	}
	authEntries := make([]types.TxAuthEntry, 0, 2)
	spentCoins := []consensus.UtxoEntry{
		{ValueAtoms: 49, PubKey: signerPubKey(3)},
		{ValueAtoms: 49, PubKey: signerPubKey(4)},
	}
	for inputIndex := range join.Base.Inputs {
		msg, err := consensus.Sighash(&join, inputIndex, spentCoins)
		if err != nil {
			t.Fatalf("sighash join input %d: %v", inputIndex, err)
		}
		_, sig := crypto.SignSchnorrForTest([32]byte{byte(3 + inputIndex)}, &msg)
		authEntries = append(authEntries, types.TxAuthEntry{Signature: sig})
	}
	join.Auth = types.TxAuth{Entries: authEntries}

	if admission, err := pool.AcceptTx(join, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("store join orphan: %v", err)
	} else if !admission.Orphaned {
		t.Fatal("expected join orphaned")
	}
	if orphan := pool.orphans[consensus.TxID(&join)]; orphan == nil || orphan.MissingCount != 2 {
		t.Fatalf("join missing count = %v, want 2", orphan)
	}

	leftAdmission, err := pool.AcceptTx(left, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept left: %v", err)
	}
	if len(leftAdmission.Accepted) != 1 {
		t.Fatalf("left accepted count = %d, want 1", len(leftAdmission.Accepted))
	}
	if orphan := pool.orphans[consensus.TxID(&join)]; orphan == nil || orphan.MissingCount != 1 {
		t.Fatalf("join missing count after left = %v, want 1", orphan)
	}

	rightAdmission, err := pool.AcceptTx(right, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept right: %v", err)
	}
	if len(rightAdmission.Accepted) != 2 {
		t.Fatalf("right accepted count = %d, want 2", len(rightAdmission.Accepted))
	}
	if rightAdmission.Accepted[1].TxID != consensus.TxID(&join) {
		t.Fatalf("expected join promoted last, got %x", rightAdmission.Accepted[1].TxID)
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
	if waiting := pool.orphanDeps[firstPrev]; len(waiting) != 0 {
		t.Fatalf("evicted orphan dependency retained %d stale waiters", len(waiting))
	}
	if _, ok := pool.orphans[consensus.TxID(&second)]; !ok {
		t.Fatalf("expected newest orphan to remain")
	}
}

func TestAcceptTxEvictsLowestFeePackageWhenMempoolIsFull(t *testing.T) {
	lowPrev := types.OutPoint{TxID: [32]byte{90}, Vout: 0}
	midPrev := types.OutPoint{TxID: [32]byte{91}, Vout: 0}
	highPrev := types.OutPoint{TxID: [32]byte{92}, Vout: 0}
	low := spendTx(t, 1, lowPrev, 50, 4, 1)
	mid := spendTx(t, 2, midPrev, 50, 5, 5)
	high := spendTx(t, 3, highPrev, 50, 6, 9)
	txBytes := len(low.Encode())

	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxMempoolBytes:    txBytes * 2,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		lowPrev:  {ValueAtoms: 50, PubKey: signerPubKey(1)},
		midPrev:  {ValueAtoms: 50, PubKey: signerPubKey(2)},
		highPrev: {ValueAtoms: 50, PubKey: signerPubKey(3)},
	}

	lowAdmission, err := pool.AcceptTx(low, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept low: %v", err)
	}
	midAdmission, err := pool.AcceptTx(mid, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept mid: %v", err)
	}
	highAdmission, err := pool.AcceptTx(high, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept high: %v", err)
	}

	if pool.Count() != 2 {
		t.Fatalf("mempool count = %d, want 2", pool.Count())
	}
	if pool.Contains(lowAdmission.TxID) {
		t.Fatalf("expected low-fee tx %x to be evicted", lowAdmission.TxID)
	}
	if !pool.Contains(midAdmission.TxID) {
		t.Fatalf("expected medium-fee tx %x to remain", midAdmission.TxID)
	}
	if !pool.Contains(highAdmission.TxID) {
		t.Fatalf("expected high-fee tx %x to remain", highAdmission.TxID)
	}
	stats := pool.Stats()
	if stats.Bytes > txBytes*2 {
		t.Fatalf("mempool bytes = %d, want <= %d", stats.Bytes, txBytes*2)
	}
}

func TestAcceptTxRejectsWhenNewFeeRateDoesNotBeatFullMempool(t *testing.T) {
	firstPrev := types.OutPoint{TxID: [32]byte{93}, Vout: 0}
	secondPrev := types.OutPoint{TxID: [32]byte{94}, Vout: 0}
	latePrev := types.OutPoint{TxID: [32]byte{95}, Vout: 0}
	first := spendTx(t, 1, firstPrev, 50, 4, 3)
	second := spendTx(t, 2, secondPrev, 50, 5, 5)
	late := spendTx(t, 3, latePrev, 50, 6, 3)
	txBytes := len(first.Encode())

	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxMempoolBytes:    txBytes * 2,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		firstPrev:  {ValueAtoms: 50, PubKey: signerPubKey(1)},
		secondPrev: {ValueAtoms: 50, PubKey: signerPubKey(2)},
		latePrev:   {ValueAtoms: 50, PubKey: signerPubKey(3)},
	}

	firstAdmission, err := pool.AcceptTx(first, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	secondAdmission, err := pool.AcceptTx(second, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept second: %v", err)
	}
	if _, err := pool.AcceptTx(late, utxos, consensus.DefaultConsensusRules()); !errors.Is(err, ErrMempoolFull) {
		t.Fatalf("late err = %v, want ErrMempoolFull", err)
	}

	if pool.Count() != 2 {
		t.Fatalf("mempool count = %d, want 2", pool.Count())
	}
	if !pool.Contains(firstAdmission.TxID) || !pool.Contains(secondAdmission.TxID) {
		t.Fatal("existing transactions should remain after rejected admission")
	}
}

func TestAcceptTxProtectsRequiredParentsDuringMempoolEviction(t *testing.T) {
	parentPrev := types.OutPoint{TxID: [32]byte{96}, Vout: 0}
	unrelatedPrev := types.OutPoint{TxID: [32]byte{97}, Vout: 0}
	parent := spendTx(t, 1, parentPrev, 50, 2, 1)
	unrelated := spendTx(t, 3, unrelatedPrev, 50, 4, 1)
	txBytes := len(parent.Encode())

	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxMempoolBytes:    txBytes * 2,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		parentPrev:    {ValueAtoms: 50, PubKey: signerPubKey(1)},
		unrelatedPrev: {ValueAtoms: 50, PubKey: signerPubKey(3)},
	}

	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	unrelatedAdmission, err := pool.AcceptTx(unrelated, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept unrelated: %v", err)
	}
	child := spendTx(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 5, 9)
	childAdmission, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept child: %v", err)
	}

	if !pool.Contains(parentAdmission.TxID) {
		t.Fatalf("required parent %x was evicted", parentAdmission.TxID)
	}
	if pool.Contains(unrelatedAdmission.TxID) {
		t.Fatalf("expected unrelated tx %x to be evicted", unrelatedAdmission.TxID)
	}
	if !pool.Contains(childAdmission.TxID) {
		t.Fatalf("expected child tx %x to be admitted", childAdmission.TxID)
	}
}
