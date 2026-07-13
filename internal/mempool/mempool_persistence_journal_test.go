package mempool

import (
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestPersistenceJournalTracksAcceptedAndRemovedEntries(t *testing.T) {
	pool := NewWithConfig(PoolConfig{MinRelayFeePerByte: 0, MaxTxSize: 1_000_000, MaxAncestors: 25, MaxDescendants: 25, MaxOrphans: 8})
	base := pool.PersistenceSnapshot()
	prevOut := types.OutPoint{TxID: [32]byte{0x41}, Vout: 0}
	tx := spendTx(t, 1, prevOut, 50, 2, 1)
	admission, err := pool.AcceptTx(tx, consensus.UtxoSet{prevOut: {ValueAtoms: 50, PubKey: signerPubKey(1)}}, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("AcceptTx: %v", err)
	}
	added, complete := pool.PersistenceChangesSince(base.Epoch)
	if !complete || added.EntryCount != 1 || len(added.EntryUpserts) != 1 || added.EntryUpserts[0].TxID != admission.TxID {
		t.Fatalf("accepted changes = %+v, complete=%v", added, complete)
	}

	block := &types.Block{Txs: []types.Transaction{
		testCoinbase(1, []types.TxOutput{{ValueAtoms: 1, PubKey: signerPubKey(9)}}),
		tx,
	}}
	pool.RemoveConfirmed(block)
	removed, complete := pool.PersistenceChangesSince(added.Epoch)
	if !complete || removed.EntryCount != 0 || len(removed.EntryDeletes) != 1 || removed.EntryDeletes[0] != admission.TxID {
		t.Fatalf("removed changes = %+v, complete=%v", removed, complete)
	}
}

func TestPersistenceJournalTracksOrphanUpdatesAndDeletes(t *testing.T) {
	pool := NewWithConfig(PoolConfig{MinRelayFeePerByte: 0, MaxTxSize: 1_000_000, MaxAncestors: 25, MaxDescendants: 25, MaxOrphans: 8})
	base := pool.PersistenceSnapshot()
	missing := types.OutPoint{TxID: [32]byte{0x51}, Vout: 0}
	orphan := spendTx(t, 3, missing, 50, 4, 1)
	admission, err := pool.AcceptTx(orphan, consensus.UtxoSet{}, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("AcceptTx orphan: %v", err)
	}
	if !admission.Orphaned {
		t.Fatal("expected orphan admission")
	}
	added, complete := pool.PersistenceChangesSince(base.Epoch)
	if !complete || added.OrphanCount != 1 || len(added.OrphanUpserts) != 1 || added.OrphanUpserts[0].TxID != admission.TxID {
		t.Fatalf("orphan changes = %+v, complete=%v", added, complete)
	}

	pool.mu.Lock()
	pool.deleteOrphan(admission.TxID)
	pool.mu.Unlock()
	removed, complete := pool.PersistenceChangesSince(added.Epoch)
	if !complete || removed.OrphanCount != 0 || len(removed.OrphanDeletes) != 1 || removed.OrphanDeletes[0] != admission.TxID {
		t.Fatalf("orphan delete changes = %+v, complete=%v", removed, complete)
	}
}

func TestPersistenceJournalOverflowRequiresFullSnapshot(t *testing.T) {
	pool := New()
	base := pool.PersistenceSnapshot()
	pool.mu.Lock()
	for i := 0; i <= persistenceJournalLimit; i++ {
		pool.notePersistenceMutationLocked(persistenceEntryMutation, [32]byte{byte(i), byte(i >> 8), byte(i >> 16)})
	}
	pool.mu.Unlock()
	if _, complete := pool.PersistenceChangesSince(base.Epoch); complete {
		t.Fatal("overflowed persistence journal unexpectedly covered the original epoch")
	}
	snapshot := pool.PersistenceSnapshot()
	if snapshot.Epoch == base.Epoch {
		t.Fatal("full snapshot did not advance to the current persistence epoch")
	}
}

func TestPersistenceAcknowledgementRetainsLaterMutations(t *testing.T) {
	pool := NewWithConfig(PoolConfig{MinRelayFeePerByte: 0, MaxTxSize: 1_000_000, MaxAncestors: 25, MaxDescendants: 25, MaxOrphans: 8})
	base := pool.PersistenceSnapshot()
	firstOut := types.OutPoint{TxID: [32]byte{0x61}, Vout: 0}
	secondOut := types.OutPoint{TxID: [32]byte{0x62}, Vout: 0}
	utxos := consensus.UtxoSet{
		firstOut:  {ValueAtoms: 50, PubKey: signerPubKey(1)},
		secondOut: {ValueAtoms: 50, PubKey: signerPubKey(3)},
	}
	if _, err := pool.AcceptTx(spendTx(t, 1, firstOut, 50, 2, 1), utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept first: %v", err)
	}
	first, complete := pool.PersistenceChangesSince(base.Epoch)
	if !complete || len(first.EntryUpserts) != 1 {
		t.Fatalf("first changes = %+v, complete=%v", first, complete)
	}
	if _, err := pool.AcceptTx(spendTx(t, 3, secondOut, 50, 4, 1), utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept second: %v", err)
	}
	pool.AcknowledgePersistence(first.Epoch)
	later, complete := pool.PersistenceChangesSince(first.Epoch)
	if !complete || len(later.EntryUpserts) != 1 || later.Epoch == first.Epoch {
		t.Fatalf("later changes = %+v, complete=%v", later, complete)
	}
}
