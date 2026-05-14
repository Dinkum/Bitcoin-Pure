package mempool

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"testing"
)

func TestShortIDMatchesOnlyScansWantedIDs(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{41}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(1)},
		{TxID: [32]byte{42}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(2)},
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
		{TxID: [32]byte{51}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(1)},
		{TxID: [32]byte{52}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(2)},
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
	if pool.SelectionCandidateCount() != 2 {
		t.Fatalf("selection candidate count = %d, want 2", pool.SelectionCandidateCount())
	}

	pool.removeRecursive(map[[32]byte]struct{}{childAdmission.TxID: {}})
	if pool.SelectionCandidateCount() != 1 {
		t.Fatalf("selection candidate count after remove = %d, want 1", pool.SelectionCandidateCount())
	}
}

func TestSelectionFrontierMaintainsOrderedSnapshotAcrossIncrementalUpdates(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{31}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(1)},
		{TxID: [32]byte{32}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(2)},
		{TxID: [32]byte{33}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(3)},
	}

	low := spendTx(t, 1, types.OutPoint{TxID: [32]byte{31}, Vout: 0}, 50, 4, 1)
	lowAdmission, err := pool.AcceptTx(low, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept low fee tx: %v", err)
	}
	medium := spendTx(t, 2, types.OutPoint{TxID: [32]byte{32}, Vout: 0}, 50, 5, 5)
	mediumAdmission, err := pool.AcceptTx(medium, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept medium fee tx: %v", err)
	}

	pool.mu.Lock()
	initial := pool.cachedPackageCandidatesLocked()
	pool.mu.Unlock()
	if len(initial) != 2 {
		t.Fatalf("initial candidate count = %d, want 2", len(initial))
	}
	if initial[0].TxID != mediumAdmission.TxID || initial[1].TxID != lowAdmission.TxID {
		t.Fatalf("unexpected initial frontier order: got %x then %x", initial[0].TxID, initial[1].TxID)
	}

	high := spendTx(t, 3, types.OutPoint{TxID: [32]byte{33}, Vout: 0}, 50, 6, 9)
	highAdmission, err := pool.AcceptTx(high, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept high fee tx: %v", err)
	}

	pool.mu.Lock()
	updated := pool.cachedPackageCandidatesLocked()
	pool.mu.Unlock()
	if len(updated) != 3 {
		t.Fatalf("updated candidate count = %d, want 3", len(updated))
	}
	if updated[0].TxID != highAdmission.TxID || updated[1].TxID != mediumAdmission.TxID || updated[2].TxID != lowAdmission.TxID {
		t.Fatalf("unexpected updated frontier order: got %x, %x, %x", updated[0].TxID, updated[1].TxID, updated[2].TxID)
	}

	pool.removeRecursive(map[[32]byte]struct{}{mediumAdmission.TxID: {}})

	pool.mu.Lock()
	filtered := pool.cachedPackageCandidatesLocked()
	pool.mu.Unlock()
	if len(filtered) != 2 {
		t.Fatalf("filtered candidate count = %d, want 2", len(filtered))
	}
	if filtered[0].TxID != highAdmission.TxID || filtered[1].TxID != lowAdmission.TxID {
		t.Fatalf("unexpected filtered frontier order: got %x then %x", filtered[0].TxID, filtered[1].TxID)
	}
}

func TestStatsTracksCountsFeesAndBytesIncrementally(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevA := types.OutPoint{TxID: [32]byte{61}, Vout: 0}
	prevB := types.OutPoint{TxID: [32]byte{62}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevA: {ValueAtoms: 50, PubKey: signerPubKey(1)},
		prevB: {ValueAtoms: 50, PubKey: signerPubKey(2)},
	}

	first := spendTx(t, 1, prevA, 50, 3, 5)
	second := spendTx(t, 2, prevB, 50, 4, 9)
	firstAdmission, err := pool.AcceptTx(first, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	secondAdmission, err := pool.AcceptTx(second, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept second: %v", err)
	}

	stats := pool.Stats()
	if stats.Count != 2 {
		t.Fatalf("count = %d, want 2", stats.Count)
	}
	if stats.Bytes != len(first.Encode())+len(second.Encode()) {
		t.Fatalf("bytes = %d, want %d", stats.Bytes, len(first.Encode())+len(second.Encode()))
	}
	if stats.TotalFees != firstAdmission.Summary.Fee+secondAdmission.Summary.Fee {
		t.Fatalf("total fees = %d, want %d", stats.TotalFees, firstAdmission.Summary.Fee+secondAdmission.Summary.Fee)
	}
	if stats.LowFee != firstAdmission.Summary.Fee || stats.HighFee != secondAdmission.Summary.Fee || stats.MedianFee != secondAdmission.Summary.Fee {
		t.Fatalf("unexpected fee summary: %+v", stats)
	}
	if cached := pool.Stats(); cached != stats {
		t.Fatalf("cached stats = %+v, want %+v", cached, stats)
	}

	block := &types.Block{
		Txs: []types.Transaction{
			testCoinbase(1, []types.TxOutput{{ValueAtoms: 1, PubKey: signerPubKey(9)}}),
			first,
		},
	}
	pool.RemoveConfirmed(block)
	stats = pool.Stats()
	if stats.Count != 1 || stats.TotalFees != secondAdmission.Summary.Fee || stats.LowFee != secondAdmission.Summary.Fee || stats.HighFee != secondAdmission.Summary.Fee {
		t.Fatalf("unexpected stats after removal: %+v", stats)
	}
	if cached := pool.Stats(); cached != stats {
		t.Fatalf("cached stats after removal = %+v, want %+v", cached, stats)
	}
}

func TestTopByFeeTracksBestEntriesAcrossInsertAndRemoval(t *testing.T) {
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{71}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(1)},
		{TxID: [32]byte{72}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(2)},
		{TxID: [32]byte{73}, Vout: 0}: {ValueAtoms: 50, PubKey: signerPubKey(3)},
	}

	low := spendTx(t, 1, types.OutPoint{TxID: [32]byte{71}, Vout: 0}, 50, 4, 1)
	mid := spendTx(t, 2, types.OutPoint{TxID: [32]byte{72}, Vout: 0}, 50, 5, 5)
	high := spendTx(t, 3, types.OutPoint{TxID: [32]byte{73}, Vout: 0}, 50, 6, 9)
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

	top := pool.TopByFee(2)
	if len(top) != 2 {
		t.Fatalf("top len = %d, want 2", len(top))
	}
	if top[0].TxID != highAdmission.TxID || top[1].TxID != midAdmission.TxID {
		t.Fatalf("unexpected top ordering: got %x then %x", top[0].TxID, top[1].TxID)
	}

	block := &types.Block{
		Txs: []types.Transaction{
			testCoinbase(1, []types.TxOutput{{ValueAtoms: 1, PubKey: signerPubKey(9)}}),
			high,
		},
	}
	pool.RemoveConfirmed(block)
	top = pool.TopByFee(2)
	if len(top) != 2 {
		t.Fatalf("top len after removal = %d, want 2", len(top))
	}
	if top[0].TxID != midAdmission.TxID || top[1].TxID != lowAdmission.TxID {
		t.Fatalf("unexpected top ordering after removal: got %x then %x", top[0].TxID, top[1].TxID)
	}
}
