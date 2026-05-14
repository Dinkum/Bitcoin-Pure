package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestMineFundingOutputsProducesSpendableLanes(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	outputs, err := svc.MineFundingOutputs([][32]byte{nodeSignerPubKey(7), nodeSignerPubKey(8), nodeSignerPubKey(9)})
	if err != nil {
		t.Fatalf("MineFundingOutputs: %v", err)
	}
	if len(outputs) != 3 {
		t.Fatalf("outputs = %d, want 3", len(outputs))
	}
	if svc.BlockHeight() == 0 {
		t.Fatal("expected funding block to advance chain height")
	}
	utxos := svc.chainUtxoSnapshot()
	for _, output := range outputs {
		entry, ok := utxos(output.OutPoint)
		if !ok {
			t.Fatalf("missing funding outpoint %+v", output.OutPoint)
		}
		if entry.ValueAtoms != output.Value || entry.PubKey != output.PubKey {
			t.Fatalf("funding output mismatch for %+v", output.OutPoint)
		}
	}
}

func TestAcceptMinedBlockBroadcastsHeadersToPeers(t *testing.T) {
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
	svc.peerMu.Lock()
	svc.peers[peer.addr] = peer
	svc.peerMu.Unlock()

	block, _, err := svc.buildFundingBlock([][32]byte{nodeSignerPubKey(7)})
	if err != nil {
		t.Fatalf("buildFundingBlock: %v", err)
	}
	if _, _, err := svc.acceptMinedBlock(block); err != nil {
		t.Fatalf("acceptMinedBlock: %v", err)
	}

	msgs := make([]p2p.Message, 0, 1)
	timeout := time.After(time.Second)
	for len(msgs) < 1 {
		select {
		case envelope := <-peer.sendQ:
			msgs = append(msgs, envelope.msg)
		case <-timeout:
			t.Fatalf("timed out waiting for peer messages, got %d", len(msgs))
		}
	}

	headerSeen := false
	for _, msg := range msgs {
		switch typed := msg.(type) {
		case p2p.HeadersMessage:
			if len(typed.Headers) != 1 || typed.Headers[0] != block.Header {
				t.Fatalf("headers message = %+v, want block header", typed.Headers)
			}
			headerSeen = true
		}
	}
	if !headerSeen {
		t.Fatalf("expected headers message, got %T", msgs[0])
	}
}

func TestSeedStressLanesProducesConfirmedOutputs(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerPubKey:  nodeSignerPubKey(10),
		MinerEnabled: true,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	requested := [][32]byte{nodeSignerPubKey(7), nodeSignerPubKey(8), nodeSignerPubKey(9)}
	outputs, info, txid, err := svc.SeedStressLanes(requested, true, false)
	if err != nil {
		t.Fatalf("SeedStressLanes: %v", err)
	}
	if got, want := info.PendingBatches, 1; got != want {
		t.Fatalf("pending batches = %d, want %d", got, want)
	}
	if info.ReserveUTXOs == 0 {
		t.Fatal("expected a seeded reserve utxo")
	}
	if _, err := svc.MineBlocks(1); err != nil {
		t.Fatalf("MineBlocks: %v", err)
	}
	confirmed, info, err := svc.waitForStressLaneBatch(stressLaneBatch{TxID: txid, Outputs: outputs}, time.Second)
	if err != nil {
		t.Fatalf("waitForStressLaneBatch: %v", err)
	}
	if got, want := len(confirmed), len(requested); got != want {
		t.Fatalf("confirmed outputs = %d, want %d", got, want)
	}
	if info.ReadyOutputs != len(requested) {
		t.Fatalf("ready outputs = %d, want %d", info.ReadyOutputs, len(requested))
	}
	utxos := svc.chainUtxoSnapshot()
	for i, output := range confirmed {
		entry, ok := utxos(output.OutPoint)
		if !ok {
			t.Fatalf("confirmed output %d missing from utxo set", i)
		}
		if entry.ValueAtoms != output.Value || entry.PubKey != output.PubKey {
			t.Fatalf("confirmed output %d mismatch", i)
		}
		if output.BlockHash == ([32]byte{}) {
			t.Fatalf("confirmed output %d missing block hash", i)
		}
	}
	if got := svc.stressLaneInfo().PendingBatches; got != 0 {
		t.Fatalf("pending batches after confirmation = %d, want 0", got)
	}
}

func TestSeedStressLanesConfirmsViaPeerBlock(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	requested := [][32]byte{nodeSignerPubKey(7), nodeSignerPubKey(8)}
	outputs, info, txid, err := svc.SeedStressLanes(requested, true, false)
	if err != nil {
		t.Fatalf("SeedStressLanes: %v", err)
	}
	if info.PendingBatches != 1 {
		t.Fatalf("pending batches = %d, want 1", info.PendingBatches)
	}
	snapshot := svc.pool.Snapshot()
	var fanout *types.Transaction
	for _, entry := range snapshot {
		candidate := entry.Tx
		if consensus.TxID(&candidate) == txid {
			copied := candidate
			fanout = &copied
			break
		}
	}
	if fanout == nil {
		t.Fatal("expected stress funding tx in mempool snapshot")
	}
	prevHeight := svc.BlockHeight()
	prevHeader := *svc.chainState.ChainState().TipHeader()
	coinbase := coinbaseTxForHeight(prevHeight+1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(10)}})
	block := blockWithTxsForNodeTest(t, prevHeight, prevHeader, svc.chainState.ChainState().UTXOs(), []types.Transaction{coinbase, *fanout}, prevHeader.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	if err := svc.acceptPeerBlockMessage(peer, &block); err != nil {
		t.Fatalf("acceptPeerBlockMessage: %v", err)
	}
	confirmed, info, err := svc.waitForStressLaneBatch(stressLaneBatch{TxID: txid, Outputs: outputs}, time.Second)
	if err != nil {
		t.Fatalf("waitForStressLaneBatch: %v", err)
	}
	if info.ReadyOutputs != len(requested) {
		t.Fatalf("ready outputs = %d, want %d", info.ReadyOutputs, len(requested))
	}
	for i, output := range confirmed {
		if output.BlockHash != consensus.HeaderHash(&block.Header) {
			t.Fatalf("confirmed output %d block hash = %x, want %x", i, output.BlockHash, consensus.HeaderHash(&block.Header))
		}
	}
}

func TestMineBlocksProducesDistinctCoinbaseOutpoints(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerEnabled: true,
		MinerPubKey:  nodeSignerPubKey(9),
		MinerWorkers: 1,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	hashes, err := svc.MineBlocks(2)
	if err != nil {
		t.Fatalf("MineBlocks: %v", err)
	}
	if len(hashes) != 2 {
		t.Fatalf("hashes = %d, want 2", len(hashes))
	}

	utxos := svc.chainState.ChainState().UTXOs()
	seen := make(map[[32]byte]struct{})
	for outPoint := range utxos {
		if _, ok := seen[outPoint.TxID]; ok {
			continue
		}
		seen[outPoint.TxID] = struct{}{}
	}
	if len(seen) < 3 {
		t.Fatalf("distinct txids in live UTXO set = %d, want at least 3 including genesis and mined coinbases", len(seen))
	}
}

func TestMineBlockSearchSpaceRollsCoinbaseExtraNonce(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: nodeSignerPubKey(9),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	view, ok := svc.chainState.CommittedView()
	if !ok {
		t.Fatal("missing committed chain view")
	}
	block := nextCoinbaseBlock(view.Height, view.TipHeader, svc.chainState.ChainState().UTXOs(), 9, view.TipHeader.Timestamp+600)
	block.Header.Nonce = 0

	originalTxID := consensus.TxID(&block.Txs[0])
	originalTxRoot := block.Header.MerkleTxIDRoot
	originalUTXORoot := block.Header.UTXORoot
	if block.Txs[0].Base.CoinbaseExtraNonce == nil {
		t.Fatal("coinbase extra nonce missing from template")
	}
	if *block.Txs[0].Base.CoinbaseExtraNonce != ([types.CoinbaseExtraNonceLen]byte{}) {
		t.Fatal("expected initial coinbase extra nonce to start at zero")
	}

	calls := 0
	svc.mineHeaderFn = func(header types.BlockHeader, params consensus.ChainParams, shouldContinue func(uint64) bool) (types.BlockHeader, bool, error) {
		calls++
		switch calls {
		case 1:
			return types.BlockHeader{}, false, consensus.ErrMiningNonceExhausted
		case 2:
			if header.Nonce != 0 {
				t.Fatalf("rolled header nonce = %d, want 0", header.Nonce)
			}
			return mineHeaderForNodeTest(header), true, nil
		default:
			t.Fatalf("unexpected mineHeaderFn call %d", calls)
			return types.BlockHeader{}, false, nil
		}
	}

	mined, fresh, err := svc.mineBlockSearchSpace(block, consensus.RegtestParams(), view.UTXOAcc, nil)
	if err != nil {
		t.Fatalf("mineBlockSearchSpace: %v", err)
	}
	if !fresh {
		t.Fatal("expected mining to continue after extra nonce rollover")
	}
	if calls != 2 {
		t.Fatalf("mineHeaderFn calls = %d, want 2", calls)
	}
	if mined.Txs[0].Base.CoinbaseExtraNonce == nil {
		t.Fatal("mined coinbase extra nonce missing")
	}
	if *mined.Txs[0].Base.CoinbaseExtraNonce == ([types.CoinbaseExtraNonceLen]byte{}) {
		t.Fatal("expected coinbase extra nonce to roll after nonce exhaustion")
	}
	if got := consensus.TxID(&mined.Txs[0]); got == originalTxID {
		t.Fatal("expected rolled extra nonce to change coinbase txid")
	}
	if mined.Header.MerkleTxIDRoot == originalTxRoot {
		t.Fatal("expected tx root to change after extra nonce rollover")
	}
	if mined.Header.UTXORoot == originalUTXORoot {
		t.Fatal("expected utxo root to change after extra nonce rollover")
	}
}

func TestMineBlockSearchSpaceStopsOnServiceShutdown(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: nodeSignerPubKey(9),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	view, ok := svc.chainState.CommittedView()
	if !ok {
		t.Fatal("missing committed chain view")
	}
	block := nextCoinbaseBlock(view.Height, view.TipHeader, svc.chainState.ChainState().UTXOs(), 9, view.TipHeader.Timestamp+600)
	svc.mineHeaderFn = func(header types.BlockHeader, params consensus.ChainParams, shouldContinue func(uint64) bool) (types.BlockHeader, bool, error) {
		svc.stopOnce.Do(func() { close(svc.stopCh) })
		if shouldContinue(0) {
			return types.BlockHeader{}, false, errors.New("mining should stop when the service is shutting down")
		}
		return types.BlockHeader{}, false, nil
	}

	_, fresh, err := svc.mineBlockSearchSpace(block, consensus.RegtestParams(), view.UTXOAcc, nil)
	if err != nil {
		t.Fatalf("mineBlockSearchSpace: %v", err)
	}
	if fresh {
		t.Fatal("expected mining to stop without a fresh block")
	}
}

func TestBuildBlockTemplateStopsOnServiceShutdown(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: nodeSignerPubKey(9),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	svc.stopOnce.Do(func() { close(svc.stopCh) })
	_, _, err = svc.minerManager().buildBlockTemplateWithGeneration()
	if !errors.Is(err, ErrServiceStopping) {
		t.Fatalf("expected ErrServiceStopping, got %v", err)
	}
}

func TestBuildBlockTemplateRefreshesAfterTxAdmission(t *testing.T) {
	minerKey := nodeSignerPubKey(9)
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: minerKey,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	before, err := svc.BuildBlockTemplate()
	if err != nil {
		t.Fatalf("BuildBlockTemplate before tx wave: %v", err)
	}
	if len(before.Txs) != 1 {
		t.Fatalf("template tx count before wave = %d, want 1", len(before.Txs))
	}

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	after, err := svc.BuildBlockTemplate()
	if err != nil {
		t.Fatalf("BuildBlockTemplate after tx wave: %v", err)
	}
	if len(after.Txs) != 2 {
		t.Fatalf("template tx count after wave = %d, want 2", len(after.Txs))
	}
	if got := consensus.TxID(&after.Txs[1]); got != consensus.TxID(&tx) {
		t.Fatalf("template txid = %x, want %x", got, consensus.TxID(&tx))
	}
	stats := svc.BlockTemplateStats()
	if stats.Invalidations == 0 {
		t.Fatal("expected tx admission to invalidate block template")
	}
	if stats.LastReason != "tx_admission" {
		t.Fatalf("last template reason = %q, want tx_admission", stats.LastReason)
	}
}

func TestBuildBlockTemplateMaintainsLTORAcrossIncrementalAppend(t *testing.T) {
	minerKey := nodeSignerPubKey(9)
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: minerKey,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	if _, err := svc.MineBlocks(1); err != nil {
		t.Fatalf("MineBlocks: %v", err)
	}
	firstBlockHashPtr, err := svc.chainState.Store().GetBlockHashByHeight(1)
	if err != nil {
		t.Fatalf("BlockHashByHeight: %v", err)
	}
	if firstBlockHashPtr == nil {
		t.Fatal("expected block hash at height 1")
	}
	firstBlockHashArr := *firstBlockHashPtr
	firstBlock, err := svc.chainState.Store().GetBlock(&firstBlockHashArr)
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}
	if firstBlock == nil {
		t.Fatal("expected mined block at height 1")
	}
	firstCoinbaseTxID := consensus.TxID(&firstBlock.Txs[0])
	firstCoinbaseValue := firstBlock.Txs[0].Base.Outputs[0].ValueAtoms

	genesisOut := types.OutPoint{TxID: genesisTxID, Vout: 0}
	firstCoinbaseOut := types.OutPoint{TxID: firstCoinbaseTxID, Vout: 0}

	var firstTx types.Transaction
	var secondTx types.Transaction
	foundOrder := false
	for firstSeed := byte(10); firstSeed < 96 && !foundOrder; firstSeed++ {
		candidateFirst := spendTxForNodeTest(t, 7, genesisOut, 50, firstSeed, 1)
		firstTxID := consensus.TxID(&candidateFirst)
		for secondSeed := byte(96); secondSeed < 160; secondSeed++ {
			candidateSecond := spendTxForNodeTest(t, 9, firstCoinbaseOut, firstCoinbaseValue, secondSeed, 1)
			secondTxID := consensus.TxID(&candidateSecond)
			if bytes.Compare(secondTxID[:], firstTxID[:]) < 0 {
				firstTx = candidateFirst
				secondTx = candidateSecond
				foundOrder = true
				break
			}
		}
	}
	if !foundOrder {
		t.Fatal("failed to construct incremental LTOR append fixture")
	}

	if _, err := svc.SubmitTx(firstTx); err != nil {
		t.Fatalf("SubmitTx first: %v", err)
	}
	before, err := svc.BuildBlockTemplate()
	if err != nil {
		t.Fatalf("BuildBlockTemplate before append: %v", err)
	}
	if len(before.Txs) != 2 {
		t.Fatalf("template tx count before append = %d, want 2", len(before.Txs))
	}
	if got := consensus.TxID(&before.Txs[1]); got != consensus.TxID(&firstTx) {
		t.Fatalf("template txid before append = %x, want %x", got, consensus.TxID(&firstTx))
	}

	if _, err := svc.SubmitTx(secondTx); err != nil {
		t.Fatalf("SubmitTx second: %v", err)
	}
	after, err := svc.BuildBlockTemplate()
	if err != nil {
		t.Fatalf("BuildBlockTemplate after append: %v", err)
	}
	if len(after.Txs) != 3 {
		t.Fatalf("template tx count after append = %d, want 3", len(after.Txs))
	}
	firstTxID := consensus.TxID(&firstTx)
	secondTxID := consensus.TxID(&secondTx)
	if got := consensus.TxID(&after.Txs[1]); got != secondTxID {
		t.Fatalf("first non-coinbase tx after append = %x, want %x", got, secondTxID)
	}
	if got := consensus.TxID(&after.Txs[2]); got != firstTxID {
		t.Fatalf("second non-coinbase tx after append = %x, want %x", got, firstTxID)
	}
}

func TestBuildBlockTemplateUsesAtomicLTORAccumulatorDelta(t *testing.T) {
	minerKey := nodeSignerPubKey(9)
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesisOut := types.OutPoint{TxID: genesisTxID, Vout: 0}
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: minerKey,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	var parent types.Transaction
	var child types.Transaction
	var parentTxID [32]byte
	foundAtomicLTORPair := false
	for parentSeed := byte(10); parentSeed < 96 && !foundAtomicLTORPair; parentSeed++ {
		candidateParent := spendTxForNodeTest(t, 7, genesisOut, 50, parentSeed, 1)
		candidateParentTxID := consensus.TxID(&candidateParent)
		for childSeed := byte(96); childSeed < 180; childSeed++ {
			candidateChild := spendTxForNodeTest(t, parentSeed, types.OutPoint{TxID: candidateParentTxID, Vout: 0}, 49, childSeed, 1)
			candidateChildTxID := consensus.TxID(&candidateChild)
			if bytes.Compare(candidateChildTxID[:], candidateParentTxID[:]) < 0 {
				parent = candidateParent
				child = candidateChild
				parentTxID = candidateParentTxID
				foundAtomicLTORPair = true
				break
			}
		}
	}
	if !foundAtomicLTORPair {
		t.Fatal("failed to construct child-before-parent template fixture")
	}
	childTxID := consensus.TxID(&child)
	if _, err := svc.SubmitTx(parent); err != nil {
		t.Fatalf("SubmitTx parent: %v", err)
	}
	if _, err := svc.SubmitTx(child); err != nil {
		t.Fatalf("SubmitTx child: %v", err)
	}

	template, err := svc.BuildBlockTemplate()
	if err != nil {
		t.Fatalf("BuildBlockTemplate: %v", err)
	}
	if len(template.Txs) != 3 {
		t.Fatalf("template tx count = %d, want 3", len(template.Txs))
	}
	if got := consensus.TxID(&template.Txs[1]); got != childTxID {
		t.Fatalf("first non-coinbase tx = %x, want child %x", got, childTxID)
	}
	if got := consensus.TxID(&template.Txs[2]); got != parentTxID {
		t.Fatalf("second non-coinbase tx = %x, want parent %x", got, parentTxID)
	}

	rules := consensus.DefaultConsensusRules()
	rules.SkipPow = true
	state := svc.chainState.ChainState()
	applied := state.UTXOs()
	summary, overlay, _, err := consensus.ValidateAndApplyBlockOverlayWithLookup(&template, consensus.PrevBlockContext{
		Height: 0,
		Header: genesis.Header,
	}, state.BlockSizeState(), applied, consensus.LookupWithErrFromSet(applied), nil, consensus.RegtestParams(), rules)
	if err != nil {
		t.Fatalf("template failed consensus validation: %v", err)
	}
	overlay.ApplyToSet(applied)
	if got, want := summary.TotalFees, uint64(2); got != want {
		t.Fatalf("template fees = %d, want %d", got, want)
	}
	if got, want := consensus.ComputedUTXORoot(applied), template.Header.UTXORoot; got != want {
		t.Fatalf("applied root = %x, want template root %x", got, want)
	}
}

func TestSelectedEntryAccumulatorDeltasCancelInternalAtomicSpends(t *testing.T) {
	rootOut := types.OutPoint{TxID: [32]byte{0x01}, Vout: 0}
	parentOut := types.OutPoint{TxID: [32]byte{0x30}, Vout: 0}
	childOut := types.OutPoint{TxID: [32]byte{0x10}, Vout: 0}
	parentLeaf := consensus.UtxoLeafFromEntry(parentOut, consensus.UtxoEntryFromOutput(types.NewXOnlyOutput(49, nodeSignerPubKey(3))))
	childLeaf := consensus.UtxoLeafFromEntry(childOut, consensus.UtxoEntryFromOutput(types.NewXOnlyOutput(48, nodeSignerPubKey(4))))

	spent, created := selectedEntryAccumulatorDeltas([]mempool.SnapshotEntry{
		{
			TxID:           childOut.TxID,
			SpentOutPoints: []types.OutPoint{parentOut},
			CreatedLeaves:  []utreexo.UtxoLeaf{childLeaf},
		},
		{
			TxID:           parentOut.TxID,
			SpentOutPoints: []types.OutPoint{rootOut},
			CreatedLeaves:  []utreexo.UtxoLeaf{parentLeaf},
		},
	})
	if len(spent) != 1 || spent[0] != rootOut {
		t.Fatalf("spent delta = %+v, want [%+v]", spent, rootOut)
	}
	if len(created) != 1 || created[0] != childLeaf {
		t.Fatalf("created delta = %+v, want [%+v]", created, childLeaf)
	}
}

func TestMergeSelectedTemplateVectorsMaintainsAlignment(t *testing.T) {
	leftTxA := types.Transaction{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 11}}}}
	leftTxC := types.Transaction{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 33}}}}
	rightTxB := types.Transaction{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 22}}}}
	rightTxD := types.Transaction{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 44}}}}

	leftTxs := []types.Transaction{leftTxA, leftTxC}
	leftTxIDs := [][32]byte{{0x10}, {0x30}}
	leftAuthIDs := [][32]byte{{0x11}, {0x31}}
	right := []mempool.SnapshotEntry{
		{Tx: rightTxB, TxID: [32]byte{0x20}, AuthID: [32]byte{0x21}},
		{Tx: rightTxD, TxID: [32]byte{0x40}, AuthID: [32]byte{0x41}},
	}

	mergedTxs, mergedTxIDs, mergedAuthIDs := mergeSelectedTemplateVectors(leftTxs, leftTxIDs, leftAuthIDs, right)
	wantTxIDs := [][32]byte{{0x10}, {0x20}, {0x30}, {0x40}}
	wantAuthIDs := [][32]byte{{0x11}, {0x21}, {0x31}, {0x41}}
	wantValues := []uint64{11, 22, 33, 44}

	if len(mergedTxs) != len(wantTxIDs) {
		t.Fatalf("merged tx len = %d, want %d", len(mergedTxs), len(wantTxIDs))
	}
	for i := range wantTxIDs {
		if mergedTxIDs[i] != wantTxIDs[i] {
			t.Fatalf("merged txid[%d] = %x, want %x", i, mergedTxIDs[i], wantTxIDs[i])
		}
		if mergedAuthIDs[i] != wantAuthIDs[i] {
			t.Fatalf("merged authid[%d] = %x, want %x", i, mergedAuthIDs[i], wantAuthIDs[i])
		}
		if got := mergedTxs[i].Base.Outputs[0].ValueAtoms; got != wantValues[i] {
			t.Fatalf("merged tx value[%d] = %d, want %d", i, got, wantValues[i])
		}
	}
}

func TestMineOneBlockRefreshesInterruptedTemplate(t *testing.T) {
	minerKey := nodeSignerPubKey(9)
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: minerKey,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	var calls int
	svc.mineHeaderFn = func(header types.BlockHeader, params consensus.ChainParams, shouldContinue func(uint64) bool) (types.BlockHeader, bool, error) {
		calls++
		switch calls {
		case 1:
			if _, err := svc.SubmitTx(tx); err != nil {
				return types.BlockHeader{}, false, err
			}
			if shouldContinue(0) {
				return types.BlockHeader{}, false, errors.New("template should have been invalidated by tx wave")
			}
			return types.BlockHeader{}, false, nil
		case 2:
			return mineHeaderForNodeTest(header), true, nil
		default:
			return types.BlockHeader{}, false, errors.New("unexpected extra mining attempt")
		}
	}

	hash, err := svc.mineOneBlock()
	if err != nil {
		t.Fatalf("mineOneBlock: %v", err)
	}
	if calls != 2 {
		t.Fatalf("mine calls = %d, want 2", calls)
	}
	block, err := svc.chainState.Store().GetBlock(&hash)
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}
	if block == nil {
		t.Fatal("mined block missing from store")
	}
	if len(block.Txs) != 2 {
		t.Fatalf("mined block tx count = %d, want 2", len(block.Txs))
	}
	if got := consensus.TxID(&block.Txs[1]); got != consensus.TxID(&tx) {
		t.Fatalf("mined block txid = %x, want %x", got, consensus.TxID(&tx))
	}
	stats := svc.BlockTemplateStats()
	if stats.Interruptions == 0 {
		t.Fatal("expected interrupted mining telemetry")
	}
}

func TestAcceptPeerBlockInvalidatesMinerTemplate(t *testing.T) {
	minerKey := nodeSignerPubKey(9)
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile:     types.Regtest,
		DBPath:      t.TempDir(),
		MinerPubKey: minerKey,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	before, err := svc.BuildBlockTemplate()
	if err != nil {
		t.Fatalf("BuildBlockTemplate before peer block: %v", err)
	}
	genesisHash := consensus.HeaderHash(&genesis.Header)
	if before.Header.PrevBlockHash != genesisHash {
		t.Fatalf("template prev hash before peer block = %x, want %x", before.Header.PrevBlockHash, genesisHash)
	}

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	peerBlock := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 4, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{peerBlock.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 8)
	if err := svc.acceptPeerBlockMessage(peer, &peerBlock); err != nil {
		t.Fatalf("acceptPeerBlockMessage: %v", err)
	}

	after, err := svc.BuildBlockTemplate()
	if err != nil {
		t.Fatalf("BuildBlockTemplate after peer block: %v", err)
	}
	peerHash := consensus.HeaderHash(&peerBlock.Header)
	if after.Header.PrevBlockHash != peerHash {
		t.Fatalf("template prev hash after peer block = %x, want %x", after.Header.PrevBlockHash, peerHash)
	}
	stats := svc.BlockTemplateStats()
	if stats.Invalidations == 0 {
		t.Fatal("expected peer block to invalidate block template")
	}
	if stats.LastReason != "peer_block" {
		t.Fatalf("last template reason = %q, want peer_block", stats.LastReason)
	}
}

func TestTemplateTelemetryCandidateFrontierIsCurrent(t *testing.T) {
	var telemetry templateBuildTelemetry
	telemetry.noteFullBuild(30)
	if got := telemetry.snapshot().FrontierCandidates; got != 30 {
		t.Fatalf("frontier candidates after build = %d, want 30", got)
	}
	telemetry.noteNoChangeRefresh(0)
	if got := telemetry.snapshot().FrontierCandidates; got != 0 {
		t.Fatalf("frontier candidates after empty refresh = %d, want 0", got)
	}
}
