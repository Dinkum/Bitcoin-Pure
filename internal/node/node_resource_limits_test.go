package node

import (
	"strings"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
)

func TestPendingThinStateIsCountAndByteBounded(t *testing.T) {
	svc := &Service{thinStateBudget: p2p.NewPayloadBudget(1 << 20)}
	peer := &peerConn{svc: svc, pendingThin: make(map[[32]byte]*pendingThinBlock)}
	for i := 0; i < maxPendingThinPerPeer; i++ {
		state := &pendingThinBlock{hash: [32]byte{byte(i + 1)}, txs: make([]types.Transaction, 1), filled: make([]bool, 1)}
		if !peer.storePendingThin(state) {
			t.Fatalf("store pending state %d failed", i)
		}
	}
	if peer.storePendingThin(&pendingThinBlock{hash: [32]byte{99}, txs: make([]types.Transaction, 1), filled: make([]bool, 1)}) {
		t.Fatal("stored pending state beyond per-peer limit")
	}
	peer.deletePendingThin([32]byte{1})
	if peer.storePendingThin(&pendingThinBlock{hash: [32]byte{100}, txs: make([]types.Transaction, 1), filled: make([]bool, 1)}) == false {
		t.Fatal("released pending state did not return budget")
	}
	peer.thinMu.Lock()
	peer.pendingThin[[32]byte{2}].expiresAt = time.Now().Add(-time.Second)
	peer.thinMu.Unlock()
	if _, ok := peer.pendingThinState([32]byte{2}); ok {
		t.Fatal("expired pending state remained available")
	}
}

func TestThinBlockRejectsUnknownHeaderBeforeReconstruction(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{Profile: types.Regtest, DBPath: t.TempDir()}, &genesis)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Errorf("close service: %v", err)
		}
	})
	peer := &peerConn{svc: svc, pendingThin: make(map[[32]byte]*pendingThinBlock)}
	err = svc.onCompactBlockMessage(peer, p2p.CompactBlockMessage{Header: types.BlockHeader{Version: 1}})
	if err == nil || !strings.Contains(err.Error(), "unknown header") {
		t.Fatalf("unknown compact header error = %v", err)
	}
}

func TestLazyBlockBatchEnforcesResponseByteAllowance(t *testing.T) {
	svc := &Service{
		recentHdrs: recentHeaderCache{items: make(map[[32]byte]types.BlockHeader)},
		recentBlks: recentBlockCache{items: make(map[[32]byte]types.Block)},
	}
	first := pendingPeerBlockForTest(301, 2)
	second := pendingPeerBlockForTest(302, 2)
	firstHash := consensus.HeaderHash(&first.Header)
	secondHash := consensus.HeaderHash(&second.Header)
	svc.cacheRecentBlock(first)
	svc.cacheRecentBlock(second)
	batch := newBlockServeBatch(int64(first.EncodedLen()))
	peer := &peerConn{}
	msg, err := svc.resolveQueuedBlockReference(peer, &outboundBlockReference{
		item:  p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: firstHash},
		batch: batch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := msg.(p2p.BlockMessage); !ok {
		t.Fatalf("first response type = %T, want BlockMessage", msg)
	}
	msg, err = svc.resolveQueuedBlockReference(peer, &outboundBlockReference{
		item:  p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: secondHash},
		batch: batch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := msg.(p2p.NotFoundMessage); !ok {
		t.Fatalf("over-budget response type = %T, want NotFoundMessage", msg)
	}
}

func TestPeerBlockServingUsesTokenBucket(t *testing.T) {
	peer := &peerConn{}
	now := time.Unix(100, 0)
	if !peer.allowBlockServeBytes(100, now, 100) {
		t.Fatal("first block-serving burst was not available")
	}
	if !peer.allowBlockServeBytes(100, now, 100) {
		t.Fatal("second block-serving burst was not available")
	}
	if peer.allowBlockServeBytes(1, now, 100) {
		t.Fatal("block-serving burst exceeded token budget")
	}
	if !peer.allowBlockServeBytes(12, now.Add(time.Second), 100) {
		t.Fatal("block-serving tokens did not refill")
	}
}

func TestGetDataQueuesLazyDeduplicatedBlockReferences(t *testing.T) {
	svc := &Service{
		cfg:  ServiceConfig{MaxMessageBytes: 1024},
		pool: mempool.New(),
	}
	peer := &peerConn{
		svc:            svc,
		relayPriorityQ: make(chan outboundMessage, 8),
		sendQ:          make(chan outboundMessage, 8),
		closed:         make(chan struct{}),
	}
	hash := [32]byte{7}
	err := svc.onGetDataMessage(peer, p2p.GetDataMessage{Items: []p2p.InvVector{
		{Type: p2p.InvTypeBlockFull, Hash: hash},
		{Type: p2p.InvTypeBlockFull, Hash: hash},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(peer.relayPriorityQ); got != 1 {
		t.Fatalf("queued block references = %d, want 1", got)
	}
	envelope := <-peer.relayPriorityQ
	if envelope.blockRef == nil || envelope.msg != nil {
		t.Fatalf("queued envelope = %+v, want lazy block reference", envelope)
	}
}
