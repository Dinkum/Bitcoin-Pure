package storage

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"errors"
	"github.com/cockroachdb/pebble"
	"log/slog"
	"sync"
	"testing"
)

func TestConcurrentHeaderAndValidatedBlockWritesPreserveValidation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const count = 96
	blocks := make([]HeaderBatchEntry, count)
	base, _ := sampleBlockAndUTXOs()
	for i := range blocks {
		h := base.Header
		h.Nonce = uint64(i + 1)
		blocks[i] = HeaderBatchEntry{Height: uint64(i), Header: h}
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		errs <- s.CommitHeaderChain(&StoredHeaderState{TipHeader: blocks[count-1].Header, Height: count - 1}, blocks, 0, 0, nil)
	}()
	go func() {
		<-start
		for _, row := range blocks {
			b := base
			b.Header = row.Header
			entry := BlockIndexEntry{Height: row.Height, Header: b.Header, Validated: true, BlockSizeState: sampleBlockSizeState()}
			if err := s.PutValidatedBlock(&b, &entry, nil); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range blocks {
		hash := consensus.HeaderHash(&row.Header)
		entry, err := s.GetBlockIndex(&hash)
		if err != nil || entry == nil || !entry.Validated {
			t.Fatalf("validation lost at height %d: %+v, %v", row.Height, entry, err)
		}
	}
}

func TestConcurrentJournalWritersAllocateDistinctSequences(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const count = 96
	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			h := [32]byte{byte(i + 1)}
			errs <- s.SetHeaderHashByHeight(uint64(i), h)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seq, err := s.journalNextSeq()
	if err != nil || seq != count {
		t.Fatalf("journal sequence=%d err=%v; want %d distinct entries", seq, err, count)
	}
	seen := make(map[uint64]bool, count)
	for i := uint64(0); i < count; i++ {
		raw, err := s.get(journalKey(i))
		if err != nil || raw == nil {
			t.Fatalf("journal %d missing: %v", i, err)
		}
		entry, err := decodeChainJournalEntry(raw)
		if err != nil {
			t.Fatal(err)
		}
		height := entry.Pairs[0].Height
		if seen[height] {
			t.Fatalf("duplicate height %d", height)
		}
		seen[height] = true
	}
}

func TestBlockAndHeaderMetadataShareFailedCommit(t *testing.T) {
	path := t.TempDir()
	s := openFilterTestStore(t, path)
	genesis := txIndexFixtureBlock(0, [32]byte{}, 1, 1)
	entry := BlockIndexEntry{Header: genesis.Header, Validated: true, BlockSizeState: sampleBlockSizeState()}
	if err := s.PutValidatedBlock(&genesis, &entry, nil); err != nil {
		t.Fatal(err)
	}
	setTxIndexTestTip(t, s, 0, genesis)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	s = &ChainStore{db: db, logger: slog.Default()}
	defer s.Close()
	next := txIndexFixtureBlock(1, consensus.HeaderHash(&genesis.Header), 2, 1)
	nextEntry := BlockIndexEntry{Height: 1, ParentHash: next.Header.PrevBlockHash, Header: next.Header, Validated: true, BlockSizeState: sampleBlockSizeState()}
	empty, err := consensus.UtxoAccumulator(consensus.UtxoSet{})
	if err != nil {
		t.Fatal(err)
	}
	meta := &StoredChainStateMeta{Profile: types.Regtest, Height: 1, TipHeader: next.Header, BlockSizeState: sampleBlockSizeState(), UTXOAccumulator: empty, UTXOAccumulatorRoot: empty.Root()}
	update := &HeaderChainUpdate{State: &StoredHeaderState{Profile: types.Regtest, Height: 1, TipHeader: next.Header}, Entries: []HeaderBatchEntry{{Height: 1, Header: next.Header}}}
	err = s.AppendValidatedBlockMetaWithHeaderUpdate(meta, &next, &nextEntry, nil, nil, nil, update)
	if !errors.Is(err, pebble.ErrReadOnly) {
		t.Fatalf("commit failure=%v", err)
	}
	chain, err := s.LoadChainStateMeta()
	if err != nil || chain.Height != 0 {
		t.Fatalf("failed commit changed block metadata: %+v %v", chain, err)
	}
	headers, err := s.LoadHeaderState()
	if err != nil || headers.Height != 0 || headers.TipHeader != genesis.Header {
		t.Fatalf("failed commit changed header metadata: %+v %v", headers, err)
	}
	hash := consensus.HeaderHash(&next.Header)
	if row, err := s.GetBlockIndex(&hash); err != nil || row != nil {
		t.Fatal("failed commit published a block index")
	}
}
