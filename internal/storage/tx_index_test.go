package storage

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
)

// Storage fixtures intentionally omit consensus validation; node integration
// tests separately exercise valid mined blocks and the service config mapping.
func txIndexFixtureBlock(height uint64, parent [32]byte, tag uint64, count int) types.Block {
	txs := make([]types.Transaction, count)
	for i := range txs {
		txs[i] = testCoinbase(height, []types.TxOutput{types.NewXOnlyOutput(tag+uint64(i)+1, [32]byte{7})})
	}
	return testBlock(parent, height+1, consensus.RegtestParams().GenesisBits, consensus.UtxoSet{}, txs...)
}

func setTxIndexTestTip(tb testing.TB, s *ChainStore, height uint64, block types.Block) {
	tb.Helper()
	if err := s.WriteFullState(&StoredChainState{Profile: types.Regtest, Height: height, TipHeader: block.Header, BlockSizeState: sampleBlockSizeState(), UTXOs: consensus.UtxoSet{}}); err != nil {
		tb.Fatal(err)
	}
}

func storeTxIndexTestBlock(tb testing.TB, s *ChainStore, height uint64, block types.Block, active bool) {
	tb.Helper()
	var err error
	if active {
		err = s.PutBlock(height, &block)
	} else {
		err = s.PutValidatedBlockWithoutWalletIndex(&block, &BlockIndexEntry{Height: height, Header: block.Header, ParentHash: block.Header.PrevBlockHash, Validated: true, BlockSizeState: sampleBlockSizeState()}, nil)
	}
	if err != nil {
		tb.Fatal(err)
	}
}

func finishTxIndex(tb testing.TB, s *ChainStore) {
	tb.Helper()
	worker := txIndexWorker{store: s}
	for i := 0; i < 10000; i++ {
		progress, err := worker.step()
		if err != nil {
			tb.Fatal(err)
		}
		if !progress {
			if !s.TxIndexStatus().Synced {
				tb.Fatal("index did not synchronize")
			}
			return
		}
	}
	tb.Fatal("index did not converge")
}

func assertIndexedTransaction(tb testing.TB, s *ChainStore, tx types.Transaction, want *[32]byte) {
	tb.Helper()
	hash, found, err := s.FindActiveTransaction(consensus.TxID(&tx))
	if err != nil || found != (want != nil) || want != nil && hash != *want {
		tb.Fatalf("transaction status found=%v hash=%x err=%v want=%v", found, hash, err, want)
	}
}

func TestTxIndexDisabledDoesNoIndexWork(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := txIndexFixtureBlock(0, [32]byte{}, 1, 1)
	storeTxIndexTestBlock(t, s, 0, g, true)
	setTxIndexTestTip(t, s, 0, g)
	if s.txIndexNotify != nil || s.TxIndexStatus().Enabled {
		t.Fatal("index enabled by default")
	}
	if raw, err := s.get(txIndexStateKey); err != nil || raw != nil {
		t.Fatalf("disabled index checkpoint %x %v", raw, err)
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: txIndexPrefix, UpperBound: txIndexPrefixEnd})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if iter.First() {
		t.Fatal("disabled index wrote rows")
	}
}

func TestTxIndexResumesBoundedBuildAfterReopen(t *testing.T) {
	path := t.TempDir()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.txIndexEnabled = true
	g := txIndexFixtureBlock(0, [32]byte{}, 1, txIndexBatchEntries*2+3)
	gh := consensus.HeaderHash(&g.Header)
	storeTxIndexTestBlock(t, s, 0, g, true)
	setTxIndexTestTip(t, s, 0, g)
	w := txIndexWorker{store: s}
	for i := 0; i < 2; i++ {
		if _, err := w.step(); err != nil {
			t.Fatal(err)
		}
	}
	state, err := readTxIndexCheckpoint(s.db)
	if err != nil || state == nil || state.Offset != txIndexBatchEntries || !state.Building {
		t.Fatalf("checkpoint=%+v err=%v", state, err)
	}
	assertIndexedTransaction(t, s, g.Txs[len(g.Txs)-1], &gh) // Fallback during a partial build.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.txIndexEnabled = true
	w = txIndexWorker{store: s}
	if _, err := w.step(); err != nil {
		t.Fatal(err)
	}
	state, err = readTxIndexCheckpoint(s.db)
	if err != nil || state.Offset != txIndexBatchEntries*2 {
		t.Fatalf("build restarted instead of resuming: %+v %v", state, err)
	}
	finishTxIndex(t, s)
	for _, tx := range g.Txs {
		assertIndexedTransaction(t, s, tx, &gh)
	}
	assertIndexedTransaction(t, s, testCoinbase(8, []types.TxOutput{types.NewXOnlyOutput(10000, [32]byte{7})}), nil)
}

func TestTxIndexAppendReorgAndFullStateReplacement(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.txIndexEnabled = true
	g := txIndexFixtureBlock(0, [32]byte{}, 1, 1)
	gh := consensus.HeaderHash(&g.Header)
	storeTxIndexTestBlock(t, s, 0, g, true)
	setTxIndexTestTip(t, s, 0, g)
	finishTxIndex(t, s)
	a := txIndexFixtureBlock(1, gh, 10, 2)
	ah := consensus.HeaderHash(&a.Header)
	storeTxIndexTestBlock(t, s, 1, a, true)
	setTxIndexTestTip(t, s, 1, a)
	if s.TxIndexStatus().Synced {
		t.Fatal("append left old coverage authoritative")
	}
	assertIndexedTransaction(t, s, a.Txs[1], &ah)
	finishTxIndex(t, s)
	// A longer competing branch must fail the Base link check and rebuild.
	b := txIndexFixtureBlock(1, gh, 20, 2)
	bh := consensus.HeaderHash(&b.Header)
	c := txIndexFixtureBlock(2, bh, 30, 2)
	ch := consensus.HeaderHash(&c.Header)
	storeTxIndexTestBlock(t, s, 1, b, false)
	storeTxIndexTestBlock(t, s, 2, c, false)
	setTxIndexTestTip(t, s, 2, c)
	// Leave height-index entries on the old branch: snapshot fallback follows
	// canonical parent hashes and must not mistake those stale rows for history.
	assertIndexedTransaction(t, s, a.Txs[1], nil)
	assertIndexedTransaction(t, s, b.Txs[1], &bh)
	assertIndexedTransaction(t, s, c.Txs[1], &ch)
	finishTxIndex(t, s)
	assertIndexedTransaction(t, s, a.Txs[1], nil)
	assertIndexedTransaction(t, s, b.Txs[1], &bh)
	// Shorter full-state installs need no special index mutation on the writer.
	setTxIndexTestTip(t, s, 0, g)
	assertIndexedTransaction(t, s, b.Txs[1], nil)
	assertIndexedTransaction(t, s, g.Txs[0], &gh)
	finishTxIndex(t, s)
	assertIndexedTransaction(t, s, c.Txs[1], nil)
	// The newest active occurrence of a repeated txid wins, and disconnecting
	// it restores the earlier occurrence without deleting the older transaction.
	duplicate := txIndexFixtureBlock(1, gh, 50, 1)
	duplicate.Txs = append(duplicate.Txs, g.Txs[0])
	duplicate = testBlock(gh, 2, consensus.RegtestParams().GenesisBits, consensus.UtxoSet{}, duplicate.Txs...)
	dh := consensus.HeaderHash(&duplicate.Header)
	storeTxIndexTestBlock(t, s, 1, duplicate, true)
	setTxIndexTestTip(t, s, 1, duplicate)
	finishTxIndex(t, s)
	assertIndexedTransaction(t, s, g.Txs[0], &dh)
	setTxIndexTestTip(t, s, 0, g)
	finishTxIndex(t, s)
	assertIndexedTransaction(t, s, g.Txs[0], &gh)
}

func TestTxIndexIncompleteHistoryAndCorruptionUseSafeFallback(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.txIndexEnabled = true
	g := txIndexFixtureBlock(0, [32]byte{}, 1, 1)
	gh := consensus.HeaderHash(&g.Header)
	a := txIndexFixtureBlock(1, gh, 10, 1)
	ah := consensus.HeaderHash(&a.Header)
	storeTxIndexTestBlock(t, s, 0, g, true)
	storeTxIndexTestBlock(t, s, 1, a, true)
	setTxIndexTestTip(t, s, 1, a)
	if err := s.db.Delete(blockKey(gh), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	assertIndexedTransaction(t, s, a.Txs[0], &ah)
	if _, _, err := s.FindActiveTransaction([32]byte{255}); !errors.Is(err, ErrTransactionHistoryUnavailable) {
		t.Fatalf("incomplete history looked like absence: %v", err)
	}
	w := txIndexWorker{store: s}
	var buildErr error
	for i := 0; i < 5; i++ {
		if _, buildErr = w.step(); buildErr != nil {
			break
		}
	}
	if !errors.Is(buildErr, ErrTransactionHistoryUnavailable) || s.TxIndexStatus().Synced {
		t.Fatalf("incomplete rebuild=%v", buildErr)
	}
	storeTxIndexTestBlock(t, s, 0, g, false)
	finishTxIndex(t, s)
	if err := s.db.Set(txIndexStateKey, []byte("broken"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	assertIndexedTransaction(t, s, g.Txs[0], &gh)
	finishTxIndex(t, s)
	if err := s.db.Set(txIndexKey(consensus.TxID(&g.Txs[0]), 0), []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	assertIndexedTransaction(t, s, g.Txs[0], &gh)
	if !s.txIndexRebuild.Load() {
		t.Fatal("corrupt row did not request repair")
	}
	finishTxIndex(t, s)
	// Retained complete coverage remains useful if historical bodies are later
	// absent; only an incomplete index must refuse authoritative negative answers.
	if err := s.db.Delete(blockKey(gh), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	assertIndexedTransaction(t, s, g.Txs[0], &gh)
	if _, found, err := s.FindActiveTransaction([32]byte{255}); err != nil || found {
		t.Fatalf("complete index missing lookup: %v %v", found, err)
	}
}

func TestTxIndexBackgroundDisableAndReenable(t *testing.T) {
	path := t.TempDir()
	s, err := OpenWithLoggerAndOptions(path, nil, OpenOptions{TxIndexEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	g := txIndexFixtureBlock(0, [32]byte{}, 1, 1)
	gh := consensus.HeaderHash(&g.Header)
	storeTxIndexTestBlock(t, s, 0, g, true)
	setTxIndexTestTip(t, s, 0, g)
	wait := func(s *ChainStore) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if s.TxIndexStatus().Synced {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("background index did not catch up")
	}
	wait(s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a := txIndexFixtureBlock(1, gh, 10, 1)
	ah := consensus.HeaderHash(&a.Header)
	storeTxIndexTestBlock(t, s, 1, a, true)
	setTxIndexTestTip(t, s, 1, a)
	if s.TxIndexStatus().Enabled || s.txIndexNotify != nil {
		t.Fatal("disabled reopen started index")
	}
	state, err := readTxIndexCheckpoint(s.db)
	if err != nil || state.Target.Height != 0 {
		t.Fatal("disabled index was maintained")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWithLoggerAndOptions(path, nil, OpenOptions{TxIndexEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	wait(s)
	assertIndexedTransaction(t, s, a.Txs[0], &ah)
}

func TestTxIndexConcurrentReorgAndLookup(t *testing.T) {
	s, err := OpenWithLoggerAndOptions(t.TempDir(), nil, OpenOptions{TxIndexEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := txIndexFixtureBlock(0, [32]byte{}, 1, 1)
	gh := consensus.HeaderHash(&g.Header)
	storeTxIndexTestBlock(t, s, 0, g, true)
	setTxIndexTestTip(t, s, 0, g)
	a := txIndexFixtureBlock(1, gh, 10, 1)
	b := txIndexFixtureBlock(1, gh, 20, 1)
	storeTxIndexTestBlock(t, s, 1, a, false)
	storeTxIndexTestBlock(t, s, 1, b, false)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			hash, found, err := s.FindActiveTransaction(consensus.TxID(&g.Txs[0]))
			if err != nil || !found || hash != gh {
				t.Errorf("torn index/fallback view: %x %v %v", hash, found, err)
				return
			}
		}
	}()
	for i := 0; i < 30; i++ {
		block := a
		if i%2 == 0 {
			block = b
		}
		setTxIndexTestTip(t, s, 1, block)
	}
	wg.Wait()
}

func TestTxIndexRecoversAfterAbruptExit(t *testing.T) {
	path := t.TempDir()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	g := txIndexFixtureBlock(0, [32]byte{}, 1, txIndexBatchEntries*2+3)
	gh := consensus.HeaderHash(&g.Header)
	storeTxIndexTestBlock(t, s, 0, g, true)
	setTxIndexTestTip(t, s, 0, g)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestTxIndexCrashCheckpointChild$")
	cmd.Env = append(os.Environ(), "BPU_TX_INDEX_TEST_PATH="+path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, output)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.txIndexEnabled = true
	// NoSync may recover all, some, or none of the committed index batches. Every
	// recovered state must still give correct answers through index or fallback.
	assertIndexedTransaction(t, s, g.Txs[len(g.Txs)-1], &gh)
	finishTxIndex(t, s)
	for _, tx := range g.Txs {
		assertIndexedTransaction(t, s, tx, &gh)
	}
}

func TestTxIndexCrashCheckpointChild(t *testing.T) {
	path := os.Getenv("BPU_TX_INDEX_TEST_PATH")
	if path == "" {
		t.Skip("subprocess helper")
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w := txIndexWorker{store: s}
	for i := 0; i < 2; i++ {
		if _, err := w.step(); err != nil {
			t.Fatal(err)
		}
	}
	os.Exit(0) // Deliberately bypass Close and its WAL flush.
}
