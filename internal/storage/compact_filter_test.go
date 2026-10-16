package storage

import (
	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
	"log/slog"
	"testing"
)

func openFilterTestStore(t *testing.T, path string) *ChainStore {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return &ChainStore{db: db, logger: slog.Default()}
}
func appendFilterTestBranch(t *testing.T, s *ChainStore, start uint64, parent [32]byte, count int, tag uint64) []BlockIndexEntry {
	t.Helper()
	entries := make([]BlockIndexEntry, 0, count)
	for i := 0; i < count; i++ {
		height := start + uint64(i)
		b := txIndexFixtureBlock(height, parent, tag+uint64(i), 1)
		entry := BlockIndexEntry{Height: height, Header: b.Header, ParentHash: parent, Validated: true, BlockSizeState: sampleBlockSizeState()}
		if err := s.PutValidatedBlockWithoutWalletIndex(&b, &entry, nil); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
		parent = consensus.HeaderHash(&b.Header)
	}
	return entries
}
func selectFilterTestBranch(t *testing.T, s *ChainStore, fork, oldTip uint64, entries []BlockIndexEntry) {
	t.Helper()
	if err := s.RewriteActiveHeights(fork, oldTip, entries); err != nil {
		t.Fatal(err)
	}
	last := entries[len(entries)-1]
	setTxIndexTestTip(t, s, last.Height, types.Block{Header: last.Header})
}
func finishFilterIndex(t *testing.T, s *ChainStore) {
	t.Helper()
	for i := 0; i < 100; i++ {
		progress, err := s.advanceCompactFilterIndex()
		if err != nil {
			t.Fatal(err)
		}
		if !progress {
			return
		}
	}
	t.Fatal("filter index did not converge")
}
func TestCompactFilterIndexBoundedBuildRestartAndReorg(t *testing.T) {
	path := t.TempDir()
	s := openFilterTestStore(t, path)
	entries := appendFilterTestBranch(t, s, 0, [32]byte{}, 161, 1)
	selectFilterTestBranch(t, s, 0, 0, entries)
	if _, err := s.advanceCompactFilterIndex(); err != nil {
		t.Fatal(err)
	}
	built, err := readFilterTip(s.db)
	if err != nil || built == nil || built.Height != filterIndexBatchBlocks-1 {
		t.Fatalf("unbounded first batch: %+v %v", built, err)
	}
	finishFilterIndex(t, s)
	previous, rows, err := s.CompactFilterHeaders(160, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Header != compactfilter.Header(rows[0].FilterHash, previous) {
		t.Fatal("incorrect indexed header chain")
	}
	// Removing old block manifests proves warm pages and restart use persisted
	// headers rather than loading historical blocks to rebuild their prefix.
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		if err := s.db.Delete(blockKey(hash), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openFilterTestStore(t, path)
	defer s.Close()
	gotPrev, got, err := s.CompactFilterHeaders(160, 1)
	if err != nil || gotPrev != previous || len(got) != 1 || got[0] != rows[0] {
		t.Fatalf("restart page=%v %v", got, err)
	}
	genesis := consensus.HeaderHash(&entries[0].Header)
	alternate := appendFilterTestBranch(t, s, 1, genesis, 3, 500)
	selectFilterTestBranch(t, s, 0, 160, alternate)
	finishFilterIndex(t, s)
	_, changed, err := s.CompactFilterHeaders(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range changed {
		if row.BlockHash != consensus.HeaderHash(&alternate[i].Header) {
			t.Fatal("stale canonical filter after reorg")
		}
	}
	oldHash := consensus.HeaderHash(&entries[160].Header)
	if raw, err := s.get(filterRecordKey(oldHash)); err != nil || raw == nil {
		t.Fatal("immutable branch filter was discarded")
	}
	// Returning to the indexed branch works even without any old block bodies.
	selectFilterTestBranch(t, s, 0, 3, entries)
	finishFilterIndex(t, s)
	_, reused, err := s.CompactFilterHeaders(160, 1)
	if err != nil || len(reused) != 1 || reused[0] != rows[0] {
		t.Fatalf("branch reuse failed: %v", err)
	}
}
func TestCompactFilterCheckpointResponseBoundPrecedesBackfill(t *testing.T) {
	s := openFilterTestStore(t, t.TempDir())
	defer s.Close()
	entries := appendFilterTestBranch(t, s, 0, [32]byte{}, 4, 1)
	selectFilterTestBranch(t, s, 0, 0, entries)
	if _, _, err := s.CompactFilterCheckpoints(1, 3); err == nil {
		t.Fatal("oversized checkpoints accepted")
	}
	if built, err := readFilterTip(s.db); err != nil || built != nil {
		t.Fatal("oversized request performed backfill")
	}
	finishFilterIndex(t, s)
	tip, rows, err := s.CompactFilterCheckpoints(2, 3)
	if err != nil || tip != 3 || len(rows) != 3 || rows[0].Height != 0 || rows[1].Height != 2 || rows[2].Height != 3 {
		t.Fatalf("checkpoints=%+v tip=%d err=%v", rows, tip, err)
	}
}

func TestCompactFilterIndexWaitsForImportedUndo(t *testing.T) {
	s := openFilterTestStore(t, t.TempDir())
	defer s.Close()
	genesis := txIndexFixtureBlock(0, [32]byte{}, 1, 1)
	parent := consensus.HeaderHash(&genesis.Header)
	block := txIndexFixtureBlock(1, parent, 2, 1)
	entries := []BlockIndexEntry{{Height: 0, Header: genesis.Header, Validated: true, BlockSizeState: sampleBlockSizeState()}, {Height: 1, ParentHash: parent, Header: block.Header, Validated: true, BlockSizeState: sampleBlockSizeState()}}
	if err := s.PutValidatedBlocksWithoutWalletIndex([]types.Block{genesis, block}, entries); err != nil {
		t.Fatal(err)
	}
	selectFilterTestBranch(t, s, 0, 0, entries)
	if _, err := s.advanceCompactFilterIndex(); err == nil {
		t.Fatal("imported block indexed before its undo was available")
	}
	spent := testXOnlyUTXOEntry(50, [32]byte{99})
	if err := s.PutValidatedBlockUndo(&entries[1], []BlockUndoEntry{{Entry: spent}}); err != nil {
		t.Fatal(err)
	}
	finishFilterIndex(t, s)
	previous, rows, err := s.CompactFilterHeaders(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	expected := compactfilter.Build(consensus.HeaderHash(&block.Header), &block, []compactfilter.WatchItem{{Type: types.OutputXOnlyP2PK, Payload32: spent.PubKey}})
	if rows[0].FilterHash != expected.Hash || rows[0].Header != compactfilter.Header(expected.Hash, previous) {
		t.Fatal("filter omitted imported spent outputs")
	}
}
