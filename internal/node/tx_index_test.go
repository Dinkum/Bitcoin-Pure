package node

import (
	"encoding/hex"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
)

func waitForTxIndex(t *testing.T, s *Service) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Info().TxIndex.Synced {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("transaction index did not catch up")
}

func TestOptionalTxIndexServiceReorgAndRestart(t *testing.T) {
	path := t.TempDir()
	g := genesisBlock()
	cfg := ServiceConfig{Profile: types.Regtest, DBPath: path, TxIndexEnabled: true}
	s, err := OpenService(cfg, &g)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	if !s.Info().TxIndex.Enabled {
		t.Fatal("service did not enable transaction index")
	}
	waitForTxIndex(t, s)
	active := NewChainState(types.Regtest)
	if _, err := active.InitializeFromGenesisBlock(&g); err != nil {
		t.Fatal(err)
	}
	a := nextCoinbaseBlock(0, g.Header, active.UTXOs(), 3, g.Header.Timestamp+600)
	if _, _, err := s.acceptMinedBlock(a); err != nil {
		t.Fatal(err)
	}
	waitForTxIndex(t, s)
	check := func(block types.Block, confirmed bool) {
		t.Helper()
		id := consensus.TxID(&block.Txs[0])
		out, err := s.rpcGetTxStatus(rpcGetTxStatusParams{TxID: hex.EncodeToString(id[:])})
		if err != nil || out.Confirmed != confirmed {
			t.Fatalf("RPC confirmation=%v err=%v want=%v", out.Confirmed, err, confirmed)
		}
		if confirmed {
			hash := consensus.HeaderHash(&block.Header)
			if out.BlockHash != hex.EncodeToString(hash[:]) {
				t.Fatal("wrong confirming block")
			}
		}
	}
	check(a, true)
	side := NewChainState(types.Regtest)
	if _, err := side.InitializeFromGenesisBlock(&g); err != nil {
		t.Fatal(err)
	}
	b := nextCoinbaseBlock(0, g.Header, side.UTXOs(), 9, g.Header.Timestamp+601)
	if _, err := side.ApplyBlock(&b); err != nil {
		t.Fatal(err)
	}
	c := nextCoinbaseBlock(1, b.Header, side.UTXOs(), 10, b.Header.Timestamp+600)
	if _, err := s.applyPeerHeaders([]types.BlockHeader{b.Header, c.Header}); err != nil {
		t.Fatal(err)
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	for _, block := range []types.Block{b, c} {
		if err := s.onPeerMessage(peer, p2p.BlockMessage{Block: block}); err != nil {
			t.Fatal(err)
		}
	}
	// Correctness is required immediately, including while the index is stale.
	check(a, false)
	check(b, true)
	check(c, true)
	waitForTxIndex(t, s)
	check(a, false)
	check(c, true)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s, err = OpenService(cfg, &g)
	if err != nil {
		t.Fatal(err)
	}
	waitForTxIndex(t, s)
	check(c, true)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	cfg.TxIndexEnabled = false
	s, err = OpenService(cfg, &g)
	if err != nil {
		t.Fatal(err)
	}
	if s.Info().TxIndex.Enabled {
		t.Fatal("disabled config still enabled transaction index")
	}
	check(c, true)
	check(a, false)
}
