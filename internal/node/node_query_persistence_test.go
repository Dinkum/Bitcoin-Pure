package node

import (
	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestMempoolPersistenceFlushesRepeatedBursts(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{Profile: types.Regtest, DBPath: t.TempDir()}, &genesis)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	matureGenesisForNodeTest(t, svc)
	svc.safeGo("test-persistence", svc.mempoolPersistLoop)
	prev := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	failures := 0
	for i := 0; i < 4; i++ {
		tx := spendTxForNodeTest(t, byte(7+i), prev, uint64(50-i), byte(8+i), 1)
		a, err := svc.SubmitTx(tx)
		if err != nil {
			t.Fatal(err)
		}
		prev = types.OutPoint{TxID: a.TxID, Vout: 0}
		deadline := time.Now().Add(2 * time.Second)
		got := 0
		for time.Now().Before(deadline) {
			stored, err := svc.chainState.Store().LoadMempoolState()
			if err != nil {
				t.Fatal(err)
			}
			if stored != nil {
				got = len(stored.Entries)
			}
			if got == i+1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Logf("burst %d persisted %d expected %d", i+1, got, i+1)
		if got != i+1 {
			failures++
		}
	}
	if failures != 0 {
		t.Fatalf("%d/4 scheduled checkpoints stale", failures)
	}
}

func TestWalletUTXORPCPropagatesIndexFailure(t *testing.T) {
	svc := newFilterQueryTestService(t, 1)
	// A derived index behind the chain must not look like an empty wallet.
	if err := svc.chainState.Store().RebuildWalletIndexes(0); err != nil {
		t.Fatal(err)
	}
	key := nodeSignerPubKey(7)
	for _, request := range []struct {
		method string
		params any
	}{
		{"getutxosbypubkeys", rpcGetUTXOsByPubKeysParams{PubKeys: []string{hex.EncodeToString(key[:])}}},
		{"getutxosbywatchitems", rpcGetUTXOsByWatchItemsParams{WatchItems: []rpcWatchItemParam{{Type: types.OutputXOnlyP2PK, Payload32: hex.EncodeToString(key[:])}}}},
	} {
		raw, err := json.Marshal(request.params)
		if err != nil {
			t.Fatal(err)
		}
		_, err = svc.dispatchRPC(rpcRequest{Method: request.method, Params: raw})
		if err == nil || !strings.Contains(err.Error(), "wallet index height") {
			t.Fatalf("%s: expected index failure, got %v", request.method, err)
		}
	}
}

func newFilterQueryTestService(tb testing.TB, height int) *Service {
	tb.Helper()
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{Profile: types.Regtest, DBPath: tb.TempDir()}, &genesis)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { svc.Close() })
	for i := 0; i < height; i++ {
		st := svc.chainState.ChainState()
		block := nextCoinbaseBlock(*st.TipHeight(), *st.TipHeader(), st.UTXOs(), byte(i%200+3), st.TipHeader().Timestamp+600)
		if _, _, err := svc.acceptMinedBlock(block); err != nil {
			tb.Fatal(err)
		}
	}
	return svc
}

func TestCompactFilterRangeAndCheckpointBoundaries(t *testing.T) {
	svc := newFilterQueryTestService(t, 4)
	for _, q := range [][2]uint64{{math.MaxUint64, 2}, {math.MaxUint64 - 1, 3}} {
		if _, err := svc.CompactFilterHeaders(q[0], q[1]); err == nil {
			t.Errorf("overflow request %v accepted", q)
		}
	}
	for start := uint64(0); start < 5; start++ {
		for count := uint64(1); count <= 5-start; count++ {
			got, err := svc.CompactFilterHeaders(start, count)
			if err != nil {
				t.Fatal(err)
			}
			want := make([]CompactFilterHeaderEntry, 0, count)
			for h := start; h < start+count; h++ {
				hash, _ := svc.chainState.Store().GetBlockHashByHeight(h)
				f, _ := svc.compactFilterForHash(*hash)
				hdr, _ := svc.referenceFilterHeaderAtHeight(h)
				want = append(want, CompactFilterHeaderEntry{Height: h, BlockHash: fmt.Sprintf("%x", *hash), FilterHash: fmt.Sprintf("%x", f.Hash), FilterHeader: fmt.Sprintf("%x", hdr)})
			}
			a, _ := json.Marshal(got.Headers)
			bb, _ := json.Marshal(want)
			if string(a) != string(bb) {
				t.Fatal("headers changed")
			}
		}
	}
	for _, interval := range []uint64{0, 1, 2, 3, 4, 9, math.MaxUint64} {
		got, err := svc.CompactFilterCheckpoint(interval)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range got.Headers {
			want, _ := svc.referenceFilterHeaderAtHeight(h.Height)
			if h.FilterHeader != fmt.Sprintf("%x", want) {
				t.Fatal("checkpoint changed")
			}
		}
	}
}

func TestMempoolPersistenceFlushesDuringSustainedNotifications(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{Profile: types.Regtest, DBPath: t.TempDir()}, &genesis)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	matureGenesisForNodeTest(t, svc)
	svc.safeGo("test-persistence", svc.mempoolPersistLoop)
	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}, 50, 8, 1)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				svc.scheduleMempoolPersistence()
			}
		}
	}()
	defer func() { close(stop); <-done }()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		stored, err := svc.chainState.Store().LoadMempoolState()
		if err != nil {
			t.Fatal(err)
		}
		if stored != nil && len(stored.Entries) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("continuous notifications prevented checkpoint for 500ms")
}

// Recompute each prefix independently to check the single-pass query results.
func (s *Service) referenceFilterHeaderAtHeight(height uint64) ([32]byte, error) {
	var prev [32]byte
	for current := uint64(0); current <= height; current++ {
		hash, err := s.chainState.Store().GetBlockHashByHeight(current)
		if err != nil {
			return [32]byte{}, err
		}
		if hash == nil {
			return [32]byte{}, fmt.Errorf("block height %d not found", current)
		}
		filter, err := s.compactFilterForHash(*hash)
		if err != nil {
			return [32]byte{}, err
		}
		prev = compactfilter.Header(filter.Hash, prev)
	}
	return prev, nil
}
