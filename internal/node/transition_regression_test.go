package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func regressionService(t *testing.T) *Service {
	t.Helper()
	g := genesisBlockForPubKey(nodeSignerPubKey(7))
	s, err := OpenService(ServiceConfig{Profile: types.Regtest, DBPath: t.TempDir()}, &g)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestMinedBlockWhileHeadersAheadCommitsCoherently(t *testing.T) {
	for _, same := range []bool{true, false} {
		t.Run(map[bool]string{true: "known", false: "competing"}[same], func(t *testing.T) {
			s := regressionService(t)
			block, _, err := s.buildFundingBlock([][32]byte{nodeSignerPubKey(8)})
			if err != nil {
				t.Fatal(err)
			}
			peerBlock := block
			if !same {
				peerBlock, _, err = s.buildFundingBlock([][32]byte{nodeSignerPubKey(9)})
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.applyPeerHeaders([]types.BlockHeader{peerBlock.Header}); err != nil {
				t.Fatal(err)
			}
			hash, height, err := s.acceptMinedBlock(block)
			if err != nil || height != 1 || hash != consensus.HeaderHash(&block.Header) {
				t.Fatalf("mined result height=%d err=%v", height, err)
			}
			expected := consensus.HeaderHash(&peerBlock.Header)
			if consensus.HeaderHash(s.headerChain.TipHeader()) != expected {
				t.Fatal("best header branch replaced on a work tie")
			}
			persisted, err := s.chainState.Store().LoadHeaderState()
			if err != nil || consensus.HeaderHash(&persisted.TipHeader) != expected {
				t.Fatalf("durable header tip differs: %v", err)
			}
			active, err := s.chainState.Store().GetHeaderHashByHeight(1)
			if err != nil || active == nil || *active != expected {
				t.Fatalf("canonical header mapping differs: %v", err)
			}
			path := s.cfg.DBPath
			s.Close()
			g := genesisBlockForPubKey(nodeSignerPubKey(7))
			reopened, err := OpenService(ServiceConfig{Profile: types.Regtest, DBPath: path}, &g)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.BlockHeight() != 1 || consensus.HeaderHash(reopened.headerChain.TipHeader()) != expected {
				t.Fatal("restart changed committed branch views")
			}
		})
	}
}
func TestInvalidMinedBlockDoesNotPublishPreparedHeader(t *testing.T) {
	s := regressionService(t)
	before := consensus.HeaderHash(s.headerChain.TipHeader())
	block, _, err := s.buildFundingBlock([][32]byte{nodeSignerPubKey(8)})
	if err != nil {
		t.Fatal(err)
	}
	block.Header.UTXORoot = [32]byte{}
	if _, _, err := s.acceptMinedBlock(block); err == nil {
		t.Fatal("invalid block accepted")
	}
	if s.BlockHeight() != 0 || consensus.HeaderHash(s.headerChain.TipHeader()) != before {
		t.Fatal("failed block changed published state")
	}
	persisted, err := s.chainState.Store().LoadHeaderState()
	if err != nil || consensus.HeaderHash(&persisted.TipHeader) != before {
		t.Fatalf("failed block changed durable header state: %v", err)
	}
}
func TestReorgReconcilesChildrenOfDisconnectedParents(t *testing.T) {
	for _, conflicting := range []bool{true, false} {
		t.Run(map[bool]string{true: "conflict", false: "valid_parent"}[conflicting], func(t *testing.T) {
			s := regressionService(t)
			matureGenesisForNodeTest(t, s)
			base := detachedChainStateForNodeTest(t, s.chainState.ChainState())
			height := *base.TipHeight()
			head := *base.TipHeader()
			g := genesisBlockForPubKey(nodeSignerPubKey(7))
			out := types.OutPoint{TxID: consensus.TxID(&g.Txs[0]), Vout: 0}
			parent := spendTxForNodeTest(t, 7, out, 50, 8, 1)
			parentID := consensus.TxID(&parent)
			cb := coinbaseTxForHeight(height+1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(6)}})
			active := blockWithTxsForNodeTest(t, height, head, base.UTXOs(), []types.Transaction{cb, parent}, head.Timestamp+600)
			if _, _, err := s.acceptMinedBlock(active); err != nil {
				t.Fatal(err)
			}
			child := spendTxForNodeTest(t, 8, types.OutPoint{TxID: parentID, Vout: 0}, 49, 9, 1)
			childID := consensus.TxID(&child)
			if _, err := s.SubmitTx(child); err != nil {
				t.Fatal(err)
			}
			grandchild := spendTxForNodeTest(t, 9, types.OutPoint{TxID: childID, Vout: 0}, 48, 10, 1)
			grandchildID := consensus.TxID(&grandchild)
			if _, err := s.SubmitTx(grandchild); err != nil {
				t.Fatal(err)
			}
			txs := []types.Transaction{cb}
			if conflicting {
				txs = append(txs, spendTxForNodeTest(t, 7, out, 50, 11, 1))
			}
			alt1 := blockWithTxsForNodeTest(t, height, head, base.UTXOs(), txs, head.Timestamp+601)
			if _, err := base.ApplyBlock(&alt1); err != nil {
				t.Fatal(err)
			}
			alt2 := nextCoinbaseBlock(height+1, alt1.Header, base.UTXOs(), 5, alt1.Header.Timestamp+600)
			if _, err := s.applyPeerHeaders([]types.BlockHeader{alt1.Header, alt2.Header}); err != nil {
				t.Fatal(err)
			}
			peer := newPeerConnForTests("127.0.0.1:18444")
			for _, b := range []types.Block{alt1, alt2} {
				if err := s.onPeerMessage(peer, p2p.BlockMessage{Block: b}); err != nil {
					t.Fatal(err)
				}
			}
			for _, id := range [][32]byte{parentID, childID, grandchildID} {
				if s.pool.Contains(id) == conflicting {
					t.Fatalf("transaction %x retained=%v", id, s.pool.Contains(id))
				}
			}
			if !conflicting {
				snapshot := s.pool.AdmissionSnapshot()
				if _, ok := snapshot.Entries[childID].Parents[parentID]; !ok {
					t.Fatal("reaccepted parent dependency missing")
				}
			}
		})
	}
	if !transitionRequiresMempoolContextRecheck(committedBranchTransition{DisconnectedBlocks: 1}) {
		t.Fatal("coinbase-only reorg skipped reconciliation")
	}
}
func TestCachedTemplateRefreshAfterClockRollback(t *testing.T) {
	s := regressionService(t)
	now := uint64(time.Now().Unix())
	s.miningTimeFn = func() time.Time { return time.Unix(int64(now), 0) }
	header := types.BlockHeader{Timestamp: now + 9000, Nonce: 7}
	if err := s.refreshMiningTimestamp(&header, now-1); err != nil {
		t.Fatal(err)
	}
	if header.Timestamp != now || header.Nonce != 0 {
		t.Fatalf("cached header was not reset: %+v", header)
	}
	if _, err := s.nextMiningTimestamp(now + 9000); !errors.Is(err, consensus.ErrTimestampTooFarFuture) {
		t.Fatalf("impossible MTP accepted: %v", err)
	}
}
func TestMiningSearchRestartsAfterClockRollback(t *testing.T) {
	s := regressionService(t)
	view, _ := s.chainState.CommittedView()
	block := nextCoinbaseBlock(view.Height, view.TipHeader, s.chainState.ChainState().UTXOs(), 8, view.TipHeader.Timestamp+600)
	now := block.Header.Timestamp
	calls := 0
	s.miningTimeFn = func() time.Time { return time.Unix(int64(now), 0) }
	s.mineHeaderFn = func(header types.BlockHeader, _ consensus.ChainParams, keep func(uint64) bool) (types.BlockHeader, bool, error) {
		calls++
		now -= 9000
		if keep(0) {
			t.Fatal("future search did not stop")
		}
		return types.BlockHeader{}, false, nil
	}
	_, complete, err := s.mineBlockTemplate(block, s.currentTemplateGeneration())
	if err != nil || complete || calls != 1 {
		t.Fatalf("rollback result complete=%v calls=%d err=%v", complete, calls, err)
	}
}
func TestDashboardLinkedPathsThroughHTTPHandler(t *testing.T) {
	s := regressionService(t)
	s.publicPage = true
	view, err := s.buildPublicDashboardView()
	if err != nil {
		t.Fatal(err)
	}
	view.peerHosts = []dashboardPeerHostPage{{Host: "192.0.2.1"}}
	s.dashboard.pages = make(map[string]string)
	s.dashboard.view = view
	s.dashboard.renderedAt = time.Now()
	for _, path := range []string{"/fees", "/peer/192.0.2.1"} {
		for _, method := range []string{"GET", "HEAD"} {
			w := httptest.NewRecorder()
			s.handleHTTP(w, httptest.NewRequest(method, path, nil))
			if w.Code != 200 {
				t.Fatalf("%s %s=%d", method, path, w.Code)
			}
			if method == "HEAD" && w.Body.Len() != 0 {
				t.Fatal("HEAD included body")
			}
		}
	}
	w := httptest.NewRecorder()
	s.handleHTTP(w, httptest.NewRequest("GET", "/unknown", nil))
	if w.Code != 404 {
		t.Fatal("unknown route accepted")
	}
}

func TestChunkReassemblyReservesRawAndDecodedMemory(t *testing.T) {
	for _, extra := range []int{-1, 0} {
		t.Run(map[int]string{-1: "raw", 0: "decoded"}[extra], func(t *testing.T) {
			s := regressionService(t)
			block, _, err := s.buildFundingBlock([][32]byte{nodeSignerPubKey(8)})
			if err != nil {
				t.Fatal(err)
			}
			raw := block.Encode()
			hash := consensus.HeaderHash(&block.Header)
			s.payloadBudget = p2p.NewPayloadBudget(int64(len(raw) + extra))
			peer := newPeerConnForTests("127.0.0.1:18444")
			peer.blockTransfers = make(map[[32]byte]*incomingBlockTransfer)
			if err := s.onBlockStartMessage(peer, p2p.BlockStartMessage{Header: block.Header, TotalSize: uint64(len(raw)), Checksum: crypto.Sha256d(raw)}); err != nil {
				t.Fatal(err)
			}
			if err := s.onBlockChunkMessage(peer, p2p.BlockChunkMessage{BlockHash: hash, Data: raw}); !errors.Is(err, p2p.ErrPayloadBudget) {
				t.Fatalf("reassembly bypassed budget: %v", err)
			}
			if s.payloadBudget.Stats().Used != 0 {
				t.Fatal("failed reassembly leaked reservation")
			}
			if s.BlockHeight() != 0 {
				t.Fatal("failed reassembly committed block")
			}
		})
	}
}

func TestMinedBlockPreservesHeadersTwoBlocksAhead(t *testing.T) {
	s := regressionService(t)
	first, _, err := s.buildFundingBlock([][32]byte{nodeSignerPubKey(8)})
	if err != nil {
		t.Fatal(err)
	}
	branch := detachedChainStateForNodeTest(t, s.chainState.ChainState())
	if _, err := branch.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, branch.UTXOs(), 9, first.Header.Timestamp+600)
	if _, err := s.applyPeerHeaders([]types.BlockHeader{first.Header, second.Header}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.acceptMinedBlock(first); err != nil {
		t.Fatal(err)
	}
	tip := consensus.HeaderHash(&second.Header)
	if s.BlockHeight() != 1 || *s.headerChain.TipHeight() != 2 || consensus.HeaderHash(s.headerChain.TipHeader()) != tip {
		t.Fatal("mining replaced ahead header view")
	}
	stored, err := s.chainState.Store().LoadHeaderState()
	if err != nil || stored.Height != 2 || consensus.HeaderHash(&stored.TipHeader) != tip {
		t.Fatalf("ahead header metadata changed: %v", err)
	}
	if applied, _, _, err := s.applyPeerBlock(&second); err != nil || !applied {
		t.Fatalf("sync could not finish: applied=%v err=%v", applied, err)
	}
	if s.BlockHeight() != 2 || consensus.HeaderHash(s.headerChain.TipHeader()) != tip {
		t.Fatal("completed sync tips disagree")
	}
}

func TestRetainedBlockOwnersCountDecodedArrays(t *testing.T) {
	block := types.Block{Header: types.BlockHeader{Version: 1}, Txs: []types.Transaction{coinbaseTxForHeight(1, make([]types.TxOutput, 100))}}
	encoded := uint64(block.EncodedLen())
	retained := blockRetainedBytes(&block)
	if retained <= encoded {
		t.Fatal("decoded backing arrays omitted from retained size")
	}
	cache := recentBlockCache{maxBytes: retained*2 - 1}
	first := block
	first.Header.Nonce = 1
	second := block
	second.Header.Nonce = 2
	firstHash := consensus.HeaderHash(&first.Header)
	secondHash := consensus.HeaderHash(&second.Header)
	cacheRecentBlockLocked(&cache, firstHash, first)
	cacheRecentBlockLocked(&cache, secondHash, second)
	if cache.bytes > cache.maxBytes || len(cache.items) != 1 {
		t.Fatalf("retained cache exceeded budget: bytes=%d limit=%d count=%d", cache.bytes, cache.maxBytes, len(cache.items))
	}
	if _, ok := cache.items[firstHash]; ok {
		t.Fatal("oldest retained block was not evicted")
	}
	if _, ok := cache.items[secondHash]; !ok {
		t.Fatal("new block was not retained")
	}
	state := &pendingThinBlock{txs: block.Txs, filled: []bool{true}}
	if pendingThinRetainedBytes(state) <= encoded {
		t.Fatal("decoded reconstruction arrays omitted from retained size")
	}
}

func TestReconstructionMemoryIsReservedBeforeAllocation(t *testing.T) {
	s := regressionService(t)
	s.payloadBudget = p2p.NewPayloadBudget(1)
	if _, err := s.reserveBlockReconstruction(1); !errors.Is(err, p2p.ErrPayloadBudget) {
		t.Fatalf("reconstruction bypassed budget: %v", err)
	}
	if s.payloadBudget.Stats().Used != 0 {
		t.Fatal("failed reconstruction reservation leaked")
	}
}
func TestThinRecoveryOwnsReservationUntilHandlingFinishes(t *testing.T) {
	s := regressionService(t)
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = s
	state := &pendingThinBlock{hash: [32]byte{1}, txs: make([]types.Transaction, 1), filled: make([]bool, 1)}
	if !peer.storePendingThin(state) {
		t.Fatal("could not store reconstruction")
	}
	before := s.thinStateBudget.Stats().Used
	if before == 0 {
		t.Fatal("reconstruction not charged")
	}
	taken, ok := peer.takePendingThinState(state.hash)
	if !ok || taken != state {
		t.Fatal("recovery lost its state")
	}
	peer.deletePendingThin(state.hash)
	if s.thinStateBudget.Stats().Used != before {
		t.Fatal("detaching recovery released in-use memory")
	}
	taken.releaseBudget()
	if s.thinStateBudget.Stats().Used != 0 {
		t.Fatal("recovery reservation leaked")
	}
}
