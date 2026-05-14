package mempool

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"testing"
)

func BenchmarkSnapshot(b *testing.B) {
	pool := benchmarkSnapshotPool(b, 2048)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := pool.Snapshot()
		if len(entries) != 2048 {
			b.Fatalf("snapshot len = %d, want 2048", len(entries))
		}
	}
}

func BenchmarkSnapshotShared(b *testing.B) {
	pool := benchmarkSnapshotPool(b, 2048)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := pool.SnapshotShared()
		if len(entries) != 2048 {
			b.Fatalf("snapshot len = %d, want 2048", len(entries))
		}
	}
}

func BenchmarkSelectionSnapshotRebuild(b *testing.B) {
	pool, _ := benchmarkSelectionPool(b, 2048)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.mu.Lock()
		if pool.selection != nil {
			pool.selection.snapshot = nil
		}
		snapshot := pool.selectionSnapshotLocked()
		pool.mu.Unlock()
		if len(snapshot.candidates) == 0 {
			b.Fatal("selection snapshot returned no candidates")
		}
	}
}

func BenchmarkSelectionWalkFromCachedSnapshot(b *testing.B) {
	pool, utxos := benchmarkSelectionPool(b, 2048)
	pool.mu.Lock()
	snapshot := pool.selectionSnapshotLocked()
	pool.mu.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected, totalFees, overlay := pool.selectForBlock(utxos, consensus.DefaultConsensusRules(), 1_000_000, snapshot)
		if len(selected) == 0 || totalFees == 0 || overlay == nil {
			b.Fatal("selection walk returned an empty candidate")
		}
	}
}

func BenchmarkStatsHot(b *testing.B) {
	pool, utxos := benchmarkSelectionPool(b, 2048)
	statsPrevOut := types.OutPoint{TxID: [32]byte{0xff, 0xff, 0xff, 0xff}, Vout: 0}
	if _, err := pool.AcceptTx(signedSpendTx(b, 201, statsPrevOut, 50, 202, 1), func() consensus.UtxoSet {
		clone := make(consensus.UtxoSet, len(utxos)+1)
		for k, v := range utxos {
			clone[k] = v
		}
		clone[statsPrevOut] = consensus.UtxoEntry{ValueAtoms: 50, PubKey: signerPubKey(201)}
		return clone
	}(), consensus.DefaultConsensusRules()); err != nil {
		b.Fatalf("accept tx for stats benchmark: %v", err)
	}
	_ = pool.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats := pool.Stats()
		if stats.Count == 0 {
			b.Fatal("stats unexpectedly empty")
		}
	}
}
