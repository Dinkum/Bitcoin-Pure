package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"log/slog"
	"testing"
	"time"
)

func TestScheduleBlockRequestsReassignsExpiredInflight(t *testing.T) {
	svc := &Service{
		cfg:           ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh:        make(chan struct{}),
		blockRequests: make(map[[32]byte]blockDownloadRequest),
	}
	hashA := [32]byte{1}
	hashB := [32]byte{2}

	first := svc.scheduleBlockRequests("peer-a", [][32]byte{hashA, hashB}, 8)
	if len(first) != 2 {
		t.Fatalf("first scheduled count = %d, want 2", len(first))
	}

	second := svc.scheduleBlockRequests("peer-b", [][32]byte{hashA, hashB}, 8)
	if len(second) != 0 {
		t.Fatalf("second scheduled count = %d, want 0 while requests are still inflight", len(second))
	}

	req := svc.blockRequests[hashA]
	req.requestedAt = time.Now().Add(-svc.blockRequestTimeout() - time.Second)
	svc.blockRequests[hashA] = req

	third := svc.scheduleBlockRequests("peer-b", [][32]byte{hashA}, 8)
	if len(third) != 1 || third[0] != hashA {
		t.Fatalf("expired reassignment = %v, want [%x]", third, hashA)
	}
	if got := svc.blockRequests[hashA].peerAddr; got != "peer-b" {
		t.Fatalf("reassigned peer = %q, want peer-b", got)
	}
}

func TestScheduleTxInvRequestsReassignsExpiredInflight(t *testing.T) {
	svc := &Service{
		cfg:        ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh:     make(chan struct{}),
		txRequests: make(map[[32]byte]blockDownloadRequest),
	}
	itemA := p2p.InvVector{Type: p2p.InvTypeTx, Hash: [32]byte{1}}
	itemB := p2p.InvVector{Type: p2p.InvTypeTx, Hash: [32]byte{2}}

	first := svc.scheduleTxInvRequests("peer-a", []p2p.InvVector{itemA, itemB}, 8)
	if len(first) != 2 {
		t.Fatalf("first scheduled count = %d, want 2", len(first))
	}

	second := svc.scheduleTxInvRequests("peer-b", []p2p.InvVector{itemA, itemB}, 8)
	if len(second) != 0 {
		t.Fatalf("second scheduled count = %d, want 0 while requests are still inflight", len(second))
	}

	req := svc.txRequests[itemA.Hash]
	req.requestedAt = time.Now().Add(-svc.txRequestTimeout() - time.Second)
	svc.txRequests[itemA.Hash] = req

	third := svc.scheduleTxInvRequests("peer-b", []p2p.InvVector{itemA}, 8)
	if len(third) != 1 || third[0] != itemA {
		t.Fatalf("expired reassignment = %v, want [%+v]", third, itemA)
	}
	if got := svc.txRequests[itemA.Hash].peerAddr; got != "peer-b" {
		t.Fatalf("reassigned peer = %q, want peer-b", got)
	}
}

func TestStorePendingPeerBlockEnforcesPerPeerLimit(t *testing.T) {
	withPendingPeerBlockLimitsForTest(t, 1<<20, 2)
	svc := &Service{
		pendingBlocks:       make(map[[32]byte]pendingPeerBlock),
		pendingBlocksByPeer: make(map[string]int),
		pendingChildren:     make(map[[32]byte]map[[32]byte]struct{}),
	}
	first := pendingPeerBlockForTest(1, 2)
	second := pendingPeerBlockForTest(2, 2)
	third := pendingPeerBlockForTest(3, 2)

	if result := svc.storePendingPeerBlock("peer-a", &first); !result.Added || result.Evicted != 0 || result.Dropped {
		t.Fatalf("first store = %+v, want added without eviction", result)
	}
	if result := svc.storePendingPeerBlock("peer-a", &second); !result.Added || result.Evicted != 0 || result.Dropped {
		t.Fatalf("second store = %+v, want added without eviction", result)
	}
	result := svc.storePendingPeerBlock("peer-a", &third)
	if !result.Added || result.Evicted != 1 || result.Dropped {
		t.Fatalf("third store = %+v, want added with one eviction", result)
	}

	firstHash := consensus.HeaderHash(&first.Header)
	secondHash := consensus.HeaderHash(&second.Header)
	thirdHash := consensus.HeaderHash(&third.Header)
	if svc.hasPendingPeerBlock(firstHash) {
		t.Fatalf("oldest peer block %x should have been evicted", firstHash)
	}
	if !svc.hasPendingPeerBlock(secondHash) || !svc.hasPendingPeerBlock(thirdHash) {
		t.Fatal("newer peer blocks should remain pending")
	}
	if got := svc.pendingPeerBlockCount(); got != 2 {
		t.Fatalf("pending peer block count = %d, want 2", got)
	}
}

func TestStorePendingPeerBlockEnforcesByteBudget(t *testing.T) {
	first := pendingPeerBlockForTest(11, 4)
	second := pendingPeerBlockForTest(12, 4)
	third := pendingPeerBlockForTest(13, 4)
	byteLimit := pendingPeerBlockEncodedSize(&first) + pendingPeerBlockEncodedSize(&second) + pendingPeerBlockEncodedSize(&third) - 1
	withPendingPeerBlockLimitsForTest(t, byteLimit, 8)
	svc := &Service{
		pendingBlocks:       make(map[[32]byte]pendingPeerBlock),
		pendingBlocksByPeer: make(map[string]int),
		pendingChildren:     make(map[[32]byte]map[[32]byte]struct{}),
	}

	if result := svc.storePendingPeerBlock("peer-a", &first); !result.Added || result.Dropped {
		t.Fatalf("first store = %+v, want added", result)
	}
	if result := svc.storePendingPeerBlock("peer-b", &second); !result.Added || result.Dropped {
		t.Fatalf("second store = %+v, want added", result)
	}
	result := svc.storePendingPeerBlock("peer-c", &third)
	if !result.Added || result.Evicted != 1 || result.Dropped {
		t.Fatalf("third store = %+v, want added with one eviction", result)
	}

	firstHash := consensus.HeaderHash(&first.Header)
	secondHash := consensus.HeaderHash(&second.Header)
	thirdHash := consensus.HeaderHash(&third.Header)
	if svc.hasPendingPeerBlock(firstHash) {
		t.Fatalf("oldest block %x should have been evicted by byte budget", firstHash)
	}
	if !svc.hasPendingPeerBlock(secondHash) || !svc.hasPendingPeerBlock(thirdHash) {
		t.Fatal("newer blocks should remain pending")
	}
	if got := svc.pendingPeerBlockBytes(); got > byteLimit {
		t.Fatalf("pending peer block bytes = %d, want <= %d", got, byteLimit)
	}
}

func TestQueuedPeerBlocksDrainAfterParentArrives(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	baseState := NewChainState(types.Regtest)
	if _, err := baseState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	firstState := baseState.Clone()
	if _, err := firstState.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, firstState.UTXOs(), 4, first.Header.Timestamp+600)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{first.Header, second.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 16)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: second}); err != nil {
		t.Fatalf("onPeerMessage child before parent block: %v", err)
	}
	if got := svc.pendingPeerBlockCount(); got != 1 {
		t.Fatalf("pending queued peer blocks = %d, want 1", got)
	}
	if got := svc.blockHeight(); got != 0 {
		t.Fatalf("block height after child = %d, want 0", got)
	}

	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: first}); err != nil {
		t.Fatalf("onPeerMessage parent block: %v", err)
	}
	if got := svc.pendingPeerBlockCount(); got != 0 {
		t.Fatalf("pending queued peer blocks after parent = %d, want 0", got)
	}
	if got := svc.blockHeight(); got != 2 {
		t.Fatalf("block height after queued drain = %d, want 2", got)
	}
	if got := svc.headerHeight(); got != 2 {
		t.Fatalf("header height after queued drain = %d, want 2", got)
	}
	tip := svc.chainState.ChainState().TipHeader()
	if tip == nil {
		t.Fatal("missing tip header after queued drain")
	}
	if got, want := consensus.HeaderHash(tip), consensus.HeaderHash(&second.Header); got != want {
		t.Fatalf("tip hash = %x, want %x", got, want)
	}
}

func TestQueuedPeerBlockDrainsWhenParentIsAlreadyActive(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	baseState := NewChainState(types.Regtest)
	if _, err := baseState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	firstState := baseState.Clone()
	if _, err := firstState.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, firstState.UTXOs(), 4, first.Header.Timestamp+600)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{first.Header, second.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 16)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: first}); err != nil {
		t.Fatalf("onPeerMessage parent block: %v", err)
	}
	if got := svc.blockHeight(); got != 1 {
		t.Fatalf("block height after parent = %d, want 1", got)
	}

	parentHash := consensus.HeaderHash(&first.Header)
	svc.storePendingPeerBlock(peer.addr, &second)
	if got := svc.pendingPeerBlockCount(); got != 1 {
		t.Fatalf("pending queued peer blocks = %d, want 1", got)
	}

	svc.drainPendingPeerBlocksIfParentActive(parentHash)
	if got := svc.pendingPeerBlockCount(); got != 0 {
		t.Fatalf("pending queued peer blocks after active-parent drain = %d, want 0", got)
	}
	if got := svc.blockHeight(); got != 2 {
		t.Fatalf("block height after active-parent drain = %d, want 2", got)
	}
}

func TestQueuedPeerBlockChainDrainsAcrossReorgToHigherWorkTip(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	activeState := NewChainState(types.Regtest)
	if _, err := activeState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	active := nextCoinbaseBlock(0, genesis.Header, activeState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{active.Header}); err != nil {
		t.Fatalf("applyPeerHeaders active: %v", err)
	}
	peer := newPeerConnForTests("127.0.0.1:18445")
	peer.controlQ = make(chan outboundMessage, 16)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: active}); err != nil {
		t.Fatalf("onPeerMessage active block: %v", err)
	}

	altState := NewChainState(types.Regtest)
	if _, err := altState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	alt1 := nextCoinbaseBlock(0, genesis.Header, altState.UTXOs(), 4, genesis.Header.Timestamp+601)
	if _, err := altState.ApplyBlock(&alt1); err != nil {
		t.Fatal(err)
	}
	alt2 := nextCoinbaseBlock(1, alt1.Header, altState.UTXOs(), 5, alt1.Header.Timestamp+600)
	if _, err := altState.ApplyBlock(&alt2); err != nil {
		t.Fatal(err)
	}
	alt3 := nextCoinbaseBlock(2, alt2.Header, altState.UTXOs(), 6, alt2.Header.Timestamp+600)
	if _, err := altState.ApplyBlock(&alt3); err != nil {
		t.Fatal(err)
	}
	alt4 := nextCoinbaseBlock(3, alt3.Header, altState.UTXOs(), 7, alt3.Header.Timestamp+600)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{alt1.Header, alt2.Header, alt3.Header, alt4.Header}); err != nil {
		t.Fatalf("applyPeerHeaders competing branch: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt3}); err != nil {
		t.Fatalf("onPeerMessage alt3 before ancestors: %v", err)
	}
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt4}); err != nil {
		t.Fatalf("onPeerMessage alt4 before ancestors: %v", err)
	}
	if got := svc.pendingPeerBlockCount(); got != 2 {
		t.Fatalf("pending queued peer blocks before reorg parent arrival = %d, want 2", got)
	}

	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt1}); err != nil {
		t.Fatalf("onPeerMessage alt1: %v", err)
	}
	if got := svc.blockHeight(); got != 1 {
		t.Fatalf("block height after alt1 = %d, want 1", got)
	}
	if got := svc.pendingPeerBlockCount(); got != 2 {
		t.Fatalf("pending queued peer blocks after alt1 = %d, want 2", got)
	}

	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt2}); err != nil {
		t.Fatalf("onPeerMessage alt2: %v", err)
	}

	if got := svc.pendingPeerBlockCount(); got != 0 {
		t.Fatalf("pending queued peer blocks after queued chain drain = %d, want 0", got)
	}
	if got := svc.blockHeight(); got != 4 {
		t.Fatalf("block height after queued chain reorg = %d, want 4", got)
	}
	if got := svc.headerHeight(); got != 4 {
		t.Fatalf("header height after queued chain reorg = %d, want 4", got)
	}
	tip := svc.chainState.ChainState().TipHeader()
	if tip == nil {
		t.Fatal("missing tip header after queued chain reorg")
	}
	if got, want := consensus.HeaderHash(tip), consensus.HeaderHash(&alt4.Header); got != want {
		t.Fatalf("tip hash = %x, want %x", got, want)
	}
}

func TestCompetingBranchQueuedBlocksConvergeToHigherWorkTip(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	activeState := NewChainState(types.Regtest)
	if _, err := activeState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	active := nextCoinbaseBlock(0, genesis.Header, activeState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{active.Header}); err != nil {
		t.Fatalf("applyPeerHeaders active: %v", err)
	}
	activePeer := newPeerConnForTests("127.0.0.1:18444")
	activePeer.controlQ = make(chan outboundMessage, 16)
	if err := svc.onPeerMessage(activePeer, p2p.BlockMessage{Block: active}); err != nil {
		t.Fatalf("onPeerMessage active block: %v", err)
	}
	if got := svc.blockHeight(); got != 1 {
		t.Fatalf("active block height = %d, want 1", got)
	}

	altState := NewChainState(types.Regtest)
	if _, err := altState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	alt1 := nextCoinbaseBlock(0, genesis.Header, altState.UTXOs(), 4, genesis.Header.Timestamp+601)
	if _, err := altState.ApplyBlock(&alt1); err != nil {
		t.Fatal(err)
	}
	alt2 := nextCoinbaseBlock(1, alt1.Header, altState.UTXOs(), 5, alt1.Header.Timestamp+600)
	if _, err := altState.ApplyBlock(&alt2); err != nil {
		t.Fatal(err)
	}
	alt3 := nextCoinbaseBlock(2, alt2.Header, altState.UTXOs(), 6, alt2.Header.Timestamp+600)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{alt1.Header, alt2.Header, alt3.Header}); err != nil {
		t.Fatalf("applyPeerHeaders competing branch: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18445")
	peer.controlQ = make(chan outboundMessage, 16)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt3}); err != nil {
		t.Fatalf("onPeerMessage alt3 before ancestors: %v", err)
	}
	if got := svc.pendingPeerBlockCount(); got != 1 {
		t.Fatalf("pending queued peer blocks after alt3 = %d, want 1", got)
	}

	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt1}); err != nil {
		t.Fatalf("onPeerMessage alt1: %v", err)
	}
	if got := svc.blockHeight(); got != 1 {
		t.Fatalf("block height after alt1 = %d, want 1", got)
	}

	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: alt2}); err != nil {
		t.Fatalf("onPeerMessage alt2: %v", err)
	}

	if got := svc.pendingPeerBlockCount(); got != 0 {
		t.Fatalf("pending queued peer blocks after competing branch drain = %d, want 0", got)
	}
	if got := svc.blockHeight(); got != 3 {
		t.Fatalf("block height after competing branch catch-up = %d, want 3", got)
	}
	if got := svc.headerHeight(); got != 3 {
		t.Fatalf("header height after competing branch catch-up = %d, want 3", got)
	}
	tip := svc.chainState.ChainState().TipHeader()
	if tip == nil {
		t.Fatal("missing tip header after competing branch catch-up")
	}
	if got, want := consensus.HeaderHash(tip), consensus.HeaderHash(&alt3.Header); got != want {
		t.Fatalf("tip hash = %x, want %x", got, want)
	}
}

func TestRepairActiveHeightIndexRestoresMissingBlockRequests(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}

	genesisHash := consensus.HeaderHash(&genesis.Header)
	genesisEntry, err := svc.chainState.Store().GetBlockIndex(&genesisHash)
	if err != nil {
		t.Fatal(err)
	}
	if genesisEntry == nil {
		t.Fatal("missing genesis entry")
	}
	if err := svc.chainState.Store().RewriteActiveHeaderHeights(0, 1, []storage.BlockIndexEntry{*genesisEntry}); err != nil {
		t.Fatalf("RewriteActiveHeaderHeights: %v", err)
	}
	if err := svc.chainState.Store().WaitForDerivedIndexes(time.Second); err != nil {
		t.Fatalf("WaitForDerivedIndexes: %v", err)
	}

	hashes, gapDetected, err := svc.missingBlockHashesDetailed(8)
	if err != nil {
		t.Fatalf("missingBlockHashesDetailed: %v", err)
	}
	if !gapDetected {
		t.Fatal("expected active height gap to be detected")
	}
	if len(hashes) != 1 {
		t.Fatalf("hashes before repair = %d, want 1", len(hashes))
	}
	if hashes[0] != consensus.HeaderHash(&block.Header) {
		t.Fatalf("missing block hash before repair = %x, want %x", hashes[0], consensus.HeaderHash(&block.Header))
	}

	repaired, err := svc.repairActiveHeightIndex()
	if err != nil {
		t.Fatalf("repairActiveHeightIndex: %v", err)
	}
	if repaired == 0 {
		t.Fatal("expected repaired entries")
	}
	if err := svc.chainState.Store().WaitForDerivedIndexes(time.Second); err != nil {
		t.Fatalf("WaitForDerivedIndexes after repair: %v", err)
	}

	hashes, gapDetected, err = svc.missingBlockHashesDetailed(8)
	if err != nil {
		t.Fatalf("missingBlockHashesDetailed after repair: %v", err)
	}
	if gapDetected {
		t.Fatal("did not expect active height gap after repair")
	}
	if len(hashes) != 1 {
		t.Fatalf("hash count after repair = %d, want 1", len(hashes))
	}
	if hashes[0] != consensus.HeaderHash(&block.Header) {
		t.Fatalf("missing block hash = %x, want %x", hashes[0], consensus.HeaderHash(&block.Header))
	}
}

func TestMissingBlockHashesIncludeForkPointForPromotedBranch(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	activeState := NewChainState(types.Regtest)
	if _, err := activeState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	active1 := nextCoinbaseBlock(0, genesis.Header, activeState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{active1.Header}); err != nil {
		t.Fatalf("applyPeerHeaders active: %v", err)
	}
	if _, _, _, err := svc.applyPeerBlock(&active1); err != nil {
		t.Fatalf("applyPeerBlock active: %v", err)
	}

	branchState := NewChainState(types.Regtest)
	if _, err := branchState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	branch1 := nextCoinbaseBlock(0, genesis.Header, branchState.UTXOs(), 4, genesis.Header.Timestamp+601)
	if _, err := branchState.ApplyBlock(&branch1); err != nil {
		t.Fatal(err)
	}
	branch2 := nextCoinbaseBlock(1, branch1.Header, branchState.UTXOs(), 5, branch1.Header.Timestamp+600)
	if _, err := branchState.ApplyBlock(&branch2); err != nil {
		t.Fatal(err)
	}
	branch3 := nextCoinbaseBlock(2, branch2.Header, branchState.UTXOs(), 6, branch2.Header.Timestamp+600)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{branch1.Header, branch2.Header, branch3.Header}); err != nil {
		t.Fatalf("applyPeerHeaders branch: %v", err)
	}
	// Header promotion updates the derived active-height index asynchronously.
	// Wait for that replay so this assertion checks fork-point handling rather
	// than racing the background index repair path.
	if err := svc.chainState.Store().WaitForDerivedIndexes(time.Second); err != nil {
		t.Fatalf("WaitForDerivedIndexes branch: %v", err)
	}

	hashes, gapDetected, err := svc.missingBlockHashesDetailed(8)
	if err != nil {
		t.Fatalf("missingBlockHashesDetailed: %v", err)
	}
	if gapDetected {
		t.Fatal("did not expect active height gap")
	}
	if len(hashes) != 3 {
		t.Fatalf("missing block hash count = %d, want 3", len(hashes))
	}
	want := [][32]byte{
		consensus.HeaderHash(&branch1.Header),
		consensus.HeaderHash(&branch2.Header),
		consensus.HeaderHash(&branch3.Header),
	}
	for i := range want {
		if hashes[i] != want[i] {
			t.Fatalf("missing block hash[%d] = %x, want %x", i, hashes[i], want[i])
		}
	}
}

func TestSyncWatchdogRepairsGapAndRequestsBlocks(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		StallTimeout: time.Second,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}

	genesisHash := consensus.HeaderHash(&genesis.Header)
	genesisEntry, err := svc.chainState.Store().GetBlockIndex(&genesisHash)
	if err != nil {
		t.Fatal(err)
	}
	if genesisEntry == nil {
		t.Fatal("missing genesis entry")
	}
	if err := svc.chainState.Store().RewriteActiveHeaderHeights(0, 1, []storage.BlockIndexEntry{*genesisEntry}); err != nil {
		t.Fatalf("RewriteActiveHeaderHeights: %v", err)
	}
	if err := svc.chainState.Store().WaitForDerivedIndexes(time.Second); err != nil {
		t.Fatalf("WaitForDerivedIndexes: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 8)
	svc.peerMu.Lock()
	svc.peers[peer.addr] = peer
	svc.peerMu.Unlock()

	svc.runSyncWatchdogStep()

	gotGetData := false
	for {
		select {
		case envelope := <-peer.controlQ:
			if msg, ok := envelope.msg.(p2p.GetDataMessage); ok {
				gotGetData = true
				if len(msg.Items) != 1 || msg.Items[0].Hash != consensus.HeaderHash(&block.Header) || msg.Items[0].Type != p2p.InvTypeBlockFull {
					t.Fatalf("GetData items = %+v, want block hash %x", msg.Items, consensus.HeaderHash(&block.Header))
				}
			}
		default:
			if !gotGetData {
				t.Fatal("expected sync watchdog to request missing block")
			}
			return
		}
	}
}

func TestSyncWatchdogRotatesAwayFromTimedOutHeaderPeer(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		StallTimeout: time.Second,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}

	stalled := newPeerConnForTests("127.0.0.1:18444")
	stalled.controlQ = make(chan outboundMessage, 8)
	stalled.noteHeight(5)
	stalled.markHeadersRequested(time.Now().Add(-3 * svc.syncStallThreshold()))

	healthy := newPeerConnForTests("127.0.0.1:18445")
	healthy.controlQ = make(chan outboundMessage, 8)
	healthy.noteHeight(5)
	healthy.noteUsefulHeaders(2, time.Now())

	svc.peerMu.Lock()
	svc.peers[stalled.addr] = stalled
	svc.peers[healthy.addr] = healthy
	svc.peerMu.Unlock()

	svc.runSyncWatchdogStep()

	if got := stalled.syncSnapshot().HeaderStalls; got == 0 {
		t.Fatal("expected stalled peer header stall count to increment")
	}
	if got := stalled.syncSnapshot().cooldownRemainingMS(time.Now()); got <= 0 {
		t.Fatal("expected stalled peer cooldown")
	}
	if len(stalled.controlQ) != 0 {
		t.Fatalf("expected stalled peer to be skipped during sync rotation, queued=%d", len(stalled.controlQ))
	}
	if len(healthy.controlQ) == 0 {
		t.Fatal("expected healthy peer to receive sync work")
	}
}

func TestSyncWatchdogPollsHeadersWhenTipLooksCurrent(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 8)
	svc.peerMu.Lock()
	svc.peers[peer.addr] = peer
	svc.peerMu.Unlock()

	svc.runSyncWatchdogStep()

	gotHeaders := false
	for {
		select {
		case envelope := <-peer.controlQ:
			if _, ok := envelope.msg.(p2p.GetHeadersMessage); ok {
				gotHeaders = true
			}
		default:
			if !gotHeaders {
				t.Fatal("expected sync watchdog to poll headers while tip appears current")
			}
			return
		}
	}
}

func TestExpireStaleBlockRequestsDemotesOwningPeer(t *testing.T) {
	svc := &Service{
		cfg:           ServiceConfig{StallTimeout: time.Second},
		logger:        slog.Default(),
		peers:         make(map[string]*peerConn),
		blockRequests: make(map[[32]byte]blockDownloadRequest),
	}
	svc.syncMgr = &syncManager{svc: svc}

	stalled := newPeerConnForTests("127.0.0.1:18444")
	healthy := newPeerConnForTests("127.0.0.1:18445")
	svc.peers[stalled.addr] = stalled
	svc.peers[healthy.addr] = healthy

	hash := [32]byte{0xaa}
	svc.blockRequests[hash] = blockDownloadRequest{
		peerAddr:    stalled.addr,
		requestedAt: time.Now().Add(-2 * svc.syncManager().blockRequestTimeout()),
	}

	svc.expireStaleBlockRequests()

	if got := stalled.syncSnapshot().BlockStalls; got != 1 {
		t.Fatalf("stalled block stalls = %d, want 1", got)
	}
	if got := stalled.syncSnapshot().cooldownRemainingMS(time.Now()); got <= 0 {
		t.Fatal("expected stalled peer cooldown after expired block request")
	}
	if got := healthy.syncSnapshot().BlockStalls; got != 0 {
		t.Fatalf("healthy block stalls = %d, want 0", got)
	}
}

func TestPreferredDownloadPeersExcludeCooledPeer(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	cool := newPeerConnForTests("127.0.0.1:18444")
	cool.noteHeight(10)
	cool.noteStall(syncRequestBlocks, time.Now())

	healthy := newPeerConnForTests("127.0.0.1:18445")
	healthy.noteHeight(5)
	healthy.noteUsefulBlocks(1, time.Now())

	svc.peerMu.Lock()
	svc.peers[cool.addr] = cool
	svc.peers[healthy.addr] = healthy
	svc.peerMu.Unlock()

	preferred := svc.syncManager().preferredDownloadPeers(1)
	if len(preferred) != 1 {
		t.Fatalf("preferred peer count = %d, want 1", len(preferred))
	}
	if preferred[0].addr != healthy.addr {
		t.Fatalf("preferred peer = %s, want %s", preferred[0].addr, healthy.addr)
	}
}

func TestTxStallDoesNotCooldownBlockDownloads(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.noteStall(syncRequestTxs, time.Now())

	snapshot := peer.syncSnapshot()
	if snapshot.TxStalls != 1 {
		t.Fatalf("tx stalls = %d, want 1", snapshot.TxStalls)
	}
	if got := snapshot.cooldownRemainingMS(time.Now()); got != 0 {
		t.Fatalf("tx stall cooldown = %dms, want 0", got)
	}
	if !peer.canServeDownloads(time.Now()) {
		t.Fatal("tx-stalled peer should remain eligible for block downloads")
	}
}
