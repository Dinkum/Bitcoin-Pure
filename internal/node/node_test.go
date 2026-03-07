package node

import (
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
)

func compactTargetForTest(compact uint32) *big.Int {
	size := byte(compact >> 24)
	mantissa := compact & 0x007fffff
	target := new(big.Int).SetUint64(uint64(mantissa))
	if size <= 3 {
		target.Rsh(target, uint(8*(3-int(size))))
	} else {
		target.Lsh(target, uint(8*(int(size)-3)))
	}
	return target
}

func mineHeaderForNodeTest(header types.BlockHeader) types.BlockHeader {
	target := compactTargetForTest(header.NBits)
	for nonce := uint32(0); nonce < ^uint32(0); nonce++ {
		header.Nonce = nonce
		hash := consensus.HeaderHash(&header)
		if new(big.Int).SetBytes(hash[:]).Cmp(target) <= 0 {
			return header
		}
	}
	panic("unable to mine header")
}

func nodeSignerKeyHash(seed byte) [32]byte {
	pubKey, _ := crypto.SignSchnorrForTest([32]byte{seed}, &[32]byte{})
	return crypto.KeyHash(&pubKey)
}

func spendTxForNodeTest(t *testing.T, spenderSeed byte, prevOut types.OutPoint, value uint64, recipientSeed byte, fee uint64) types.Transaction {
	t.Helper()
	if fee >= value {
		t.Fatalf("fee %d must be less than value %d", fee, value)
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: value - fee, KeyHash: nodeSignerKeyHash(recipientSeed)}},
		},
	}
	msg, err := consensus.Sighash(&tx, 0, []uint64{value})
	if err != nil {
		t.Fatal(err)
	}
	pubKey, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{PubKey: pubKey, Signature: sig}}}
	return tx
}

func genesisBlock() types.Block {
	params := consensus.RegtestParams()
	coinbase := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{ValueAtoms: 50, KeyHash: [32]byte{7}}},
		},
	}
	txids := [][32]byte{consensus.TxID(&coinbase)}
	authids := [][32]byte{consensus.AuthID(&coinbase)}
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: txids[0], Vout: 0}: {ValueAtoms: 50, KeyHash: [32]byte{7}},
	}
	return types.Block{
		Header: types.BlockHeader{
			Version:        1,
			MerkleTxIDRoot: consensus.MerkleRoot(txids),
			MerkleAuthRoot: consensus.MerkleRoot(authids),
			UTXORoot:       consensus.ComputedUTXORoot(utxos),
			Timestamp:      params.GenesisTimestamp,
			NBits:          params.GenesisBits,
		},
		Txs: []types.Transaction{coinbase},
	}
}

func nextCoinbaseBlock(prevHeight uint64, prev types.BlockHeader, currentUTXOs consensus.UtxoSet, keyHashByte byte, timestamp uint64) types.Block {
	params := consensus.RegtestParams()
	coinbase := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: [32]byte{keyHashByte}}},
		},
	}
	txids := [][32]byte{consensus.TxID(&coinbase)}
	authids := [][32]byte{consensus.AuthID(&coinbase)}
	nextUTXOs := cloneUtxos(currentUTXOs)
	nextUTXOs[types.OutPoint{TxID: txids[0], Vout: 0}] = consensus.UtxoEntry{ValueAtoms: 1, KeyHash: [32]byte{keyHashByte}}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: prevHeight, Header: prev}, params)
	if err != nil {
		panic(err)
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  consensus.HeaderHash(&prev),
		MerkleTxIDRoot: consensus.MerkleRoot(txids),
		MerkleAuthRoot: consensus.MerkleRoot(authids),
		UTXORoot:       consensus.ComputedUTXORoot(nextUTXOs),
		Timestamp:      timestamp,
		NBits:          nbits,
	}
	return types.Block{
		Header: mineHeaderForNodeTest(header),
		Txs:    []types.Transaction{coinbase},
	}
}

func TestApplyBlockRequiresTip(t *testing.T) {
	state := NewChainState(types.Regtest)
	_, err := state.ApplyBlock(&types.Block{})
	if !errors.Is(err, ErrNoTip) {
		t.Fatalf("expected no tip error, got %v", err)
	}
}

func TestInitializeFromGenesisBlock(t *testing.T) {
	state := NewChainState(types.Regtest)
	block := genesisBlock()
	summary, err := state.InitializeFromGenesisBlock(&block)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Height != 0 || state.TipHeight() == nil || *state.TipHeight() != 0 {
		t.Fatal("genesis bootstrap mismatch")
	}
}

func TestRejectBadGenesisMerkle(t *testing.T) {
	state := NewChainState(types.Regtest)
	block := genesisBlock()
	block.Header.MerkleTxIDRoot = [32]byte{}
	_, err := state.InitializeFromGenesisBlock(&block)
	if err == nil {
		t.Fatal("expected invalid genesis")
	}
}

func TestRejectBadGenesisUTXORoot(t *testing.T) {
	state := NewChainState(types.Regtest)
	block := genesisBlock()
	block.Header.UTXORoot = [32]byte{}
	_, err := state.InitializeFromGenesisBlock(&block)
	if err == nil {
		t.Fatal("expected invalid genesis")
	}
}

func TestReplayBlocksAdvancesTip(t *testing.T) {
	state := NewChainState(types.Regtest)
	genesis := genesisBlock()
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	summary, err := state.ReplayBlocks([]types.Block{first})
	if err != nil {
		t.Fatal(err)
	}
	if summary.TipHeight != 1 {
		t.Fatalf("unexpected tip height: %d", summary.TipHeight)
	}
}

func TestPersistentRoundtrip(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	genesis := genesisBlock()
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	if err := persistent.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.ChainState().TipHeight() == nil || *reopened.ChainState().TipHeight() != 0 {
		t.Fatal("reopened tip height mismatch")
	}
}

func TestHeaderReplayAdvancesTip(t *testing.T) {
	chain := NewHeaderChain(types.Regtest)
	genesis := genesisBlock()
	if err := chain.InitializeFromGenesisHeader(genesis.Header); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, consensus.UtxoSet{}, 3, genesis.Header.Timestamp+600)
	summary, err := chain.ReplayHeaders([]types.BlockHeader{first.Header})
	if err != nil {
		t.Fatal(err)
	}
	if summary.TipHeight != 1 {
		t.Fatalf("unexpected tip height: %d", summary.TipHeight)
	}
}

func TestPersistentHeaderRoundtrip(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentHeaderChain(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	genesis := genesisBlock()
	if err := persistent.InitializeFromGenesisHeader(genesis.Header); err != nil {
		t.Fatal(err)
	}
	if err := persistent.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPersistentHeaderChain(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.HeaderChain().TipHeight() == nil || *reopened.HeaderChain().TipHeight() != 0 {
		t.Fatal("reopened header tip height mismatch")
	}
}

func TestReplayBlocksHeadersFirstInMemory(t *testing.T) {
	genesis := genesisBlock()
	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, state.UTXOs(), 4, first.Header.Timestamp+600)

	summary, err := ReplayBlocksHeadersFirst(types.Regtest, &genesis, []types.Block{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if summary.HeaderTipHeight != 2 || summary.BlockTipHeight != 2 {
		t.Fatalf("unexpected tip heights: headers=%d blocks=%d", summary.HeaderTipHeight, summary.BlockTipHeight)
	}
}

func TestReplayBlocksHeadersFirstPersistent(t *testing.T) {
	path := t.TempDir()
	genesis := genesisBlock()
	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, state.UTXOs(), 4, first.Header.Timestamp+600)

	summary, err := ReplayBlocksHeadersFirstPersistent(path, types.Regtest, &genesis, []types.Block{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if summary.HeaderTipHeight != 2 || summary.BlockTipHeight != 2 {
		t.Fatalf("unexpected persistent tip heights: headers=%d blocks=%d", summary.HeaderTipHeight, summary.BlockTipHeight)
	}
}

func TestPersistentChainStateReorgsToHigherWorkBranch(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}

	genesis := genesisBlock()
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}

	active := NewChainState(types.Regtest)
	if _, err := active.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	a1 := nextCoinbaseBlock(0, genesis.Header, active.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := active.ApplyBlock(&a1); err != nil {
		t.Fatal(err)
	}
	a2 := nextCoinbaseBlock(1, a1.Header, active.UTXOs(), 4, a1.Header.Timestamp+600)
	if _, err := active.ApplyBlock(&a2); err != nil {
		t.Fatal(err)
	}

	side := NewChainState(types.Regtest)
	if _, err := side.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	b1 := nextCoinbaseBlock(0, genesis.Header, side.UTXOs(), 9, genesis.Header.Timestamp+600)
	if _, err := side.ApplyBlock(&b1); err != nil {
		t.Fatal(err)
	}
	b2 := nextCoinbaseBlock(1, b1.Header, side.UTXOs(), 10, b1.Header.Timestamp+600)
	if _, err := side.ApplyBlock(&b2); err != nil {
		t.Fatal(err)
	}
	b3 := nextCoinbaseBlock(2, b2.Header, side.UTXOs(), 11, b2.Header.Timestamp+600)

	if _, err := persistent.ApplyBlock(&a1); err != nil {
		t.Fatal(err)
	}
	if _, err := persistent.ApplyBlock(&a2); err != nil {
		t.Fatal(err)
	}
	activeTipHash := consensus.HeaderHash(&a2.Header)
	if got := consensus.HeaderHash(persistent.ChainState().TipHeader()); got != activeTipHash {
		t.Fatalf("unexpected active tip before fork blocks: got %x want %x", got, activeTipHash)
	}

	if _, err := persistent.ApplyBlock(&b1); err != nil {
		t.Fatal(err)
	}
	if got := consensus.HeaderHash(persistent.ChainState().TipHeader()); got != activeTipHash {
		t.Fatalf("side branch should not reorg at b1: got %x want %x", got, activeTipHash)
	}
	if _, err := persistent.ApplyBlock(&b2); err != nil {
		t.Fatal(err)
	}
	if got := consensus.HeaderHash(persistent.ChainState().TipHeader()); got != activeTipHash {
		t.Fatalf("equal-work side branch should not reorg at b2: got %x want %x", got, activeTipHash)
	}
	if _, err := persistent.ApplyBlock(&b3); err != nil {
		t.Fatal(err)
	}

	sideTipHash := consensus.HeaderHash(&b3.Header)
	if persistent.ChainState().TipHeight() == nil || *persistent.ChainState().TipHeight() != 3 {
		t.Fatalf("unexpected reorged tip height: %v", persistent.ChainState().TipHeight())
	}
	if got := consensus.HeaderHash(persistent.ChainState().TipHeader()); got != sideTipHash {
		t.Fatalf("unexpected tip after reorg: got %x want %x", got, sideTipHash)
	}
	if got := len(persistent.ChainState().UTXOs()); got != 4 {
		t.Fatalf("unexpected utxo count after reorg: got %d want 4", got)
	}

	if err := persistent.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := consensus.HeaderHash(reopened.ChainState().TipHeader()); got != sideTipHash {
		t.Fatalf("unexpected reopened tip after reorg: got %x want %x", got, sideTipHash)
	}
}

func TestReconstructXThinBlockFromMempoolOverlap(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}
	parent := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	parentAdmission, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTxForNodeTest(t, 2, types.OutPoint{TxID: parentAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTx(child, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept child: %v", err)
	}

	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 99},
		Txs: []types.Transaction{
			{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}}}},
			parent,
			child,
		},
	}
	msg := buildXThinBlockMessage(block)
	thin, ok := msg.(p2p.XThinBlockMessage)
	if !ok {
		t.Fatalf("relay message type = %T, want XThinBlockMessage", msg)
	}
	state, missing := reconstructXThinBlock(thin, pool.Snapshot())
	if len(missing) != 0 {
		t.Fatalf("missing indexes = %v, want none", missing)
	}
	if !state.complete() {
		t.Fatal("expected complete thin block reconstruction")
	}
	reconstructed := state.block()
	if len(reconstructed.Txs) != len(block.Txs) {
		t.Fatalf("tx count = %d, want %d", len(reconstructed.Txs), len(block.Txs))
	}
	for i := range block.Txs {
		if consensus.TxID(&reconstructed.Txs[i]) != consensus.TxID(&block.Txs[i]) {
			t.Fatalf("tx %d mismatch", i)
		}
	}
}

func TestOnXThinBlockRequestsMissingIndexes(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{2}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}
	parent := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTxForNodeTest(t, 2, types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}, 49, 3, 1)
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 100},
		Txs: []types.Transaction{
			{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}}}},
			parent,
			child,
		},
	}
	thin := buildXThinBlockMessage(block).(p2p.XThinBlockMessage)
	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	if err := svc.onXThinBlockMessage(peer, thin); err != nil {
		t.Fatalf("onXThinBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetXBlockTxMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetXBlockTxMessage", envelope.msg)
	}
	if len(req.Indexes) != 1 || req.Indexes[0] != 2 {
		t.Fatalf("requested indexes = %v, want [2]", req.Indexes)
	}
	if _, ok := peer.pendingThinState(req.BlockHash); !ok {
		t.Fatal("expected pending thin block state")
	}
}

func TestOnXThinBlockFallsBackToFullBlockWhenOverlapIsLow(t *testing.T) {
	svc := &Service{pool: mempool.New()}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}

	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 101},
		Txs:    []types.Transaction{{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 1}}}}},
	}
	for i := 0; i < 8; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 1)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1)}},
			},
		})
	}

	thin := buildXThinBlockMessage(block).(p2p.XThinBlockMessage)
	if err := svc.onXThinBlockMessage(peer, thin); err != nil {
		t.Fatalf("onXThinBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetDataMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetDataMessage", envelope.msg)
	}
	if len(req.Items) != 1 || req.Items[0].Type != p2p.InvTypeBlockFull {
		t.Fatalf("unexpected fallback request: %+v", req.Items)
	}
}

func TestOSReleaseLooksLikeUbuntu(t *testing.T) {
	raw := strings.Join([]string{
		`NAME="Ubuntu"`,
		`ID=ubuntu`,
		`ID_LIKE=debian`,
		"",
	}, "\n")
	if !osReleaseLooksLikeUbuntu(raw) {
		t.Fatal("expected ubuntu os-release to enable dashboard")
	}
	if osReleaseLooksLikeUbuntu("ID=debian\nID_LIKE=debian\n") {
		t.Fatal("expected non-ubuntu os-release to disable dashboard")
	}
}

func TestRenderBlockFlowMarksTip(t *testing.T) {
	blocks := []dashboardBlock{
		{Height: 10, Hash: [32]byte{0xaa}},
		{Height: 11, Hash: [32]byte{0xbb}},
	}
	out := renderBlockFlow(blocks)
	if !strings.Contains(out, "tip:11") {
		t.Fatalf("expected tip marker in block flow: %q", out)
	}
	if !strings.Contains(out, "aa000000") || !strings.Contains(out, "bb000000") {
		t.Fatalf("expected short hashes in block flow: %q", out)
	}
}

func TestPeerConnCoalescesTxBatches(t *testing.T) {
	peer := &peerConn{
		sendQ:    make(chan outboundMessage, 8),
		closed:   make(chan struct{}),
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}
	first := types.Transaction{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 1}}}}
	second := types.Transaction{Base: types.TxBase{Version: 2, Outputs: []types.TxOutput{{ValueAtoms: 2}}}}

	if err := peer.enqueueTxBatch(p2p.TxBatchMessage{Txs: []types.Transaction{first}}); err != nil {
		t.Fatalf("enqueue first tx: %v", err)
	}
	if err := peer.enqueueTxBatch(p2p.TxBatchMessage{Txs: []types.Transaction{second}}); err != nil {
		t.Fatalf("enqueue second tx: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		batch, ok := envelope.msg.(p2p.TxBatchMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxBatchMessage", envelope.msg)
		}
		if len(batch.Txs) != 2 {
			t.Fatalf("coalesced batch size = %d, want 2", len(batch.Txs))
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for coalesced tx batch")
	}

	select {
	case extra := <-peer.sendQ:
		t.Fatalf("unexpected extra envelope: %#v", extra.msg)
	default:
	}
}

func TestPeerConnCoalescesTxReconAnnouncements(t *testing.T) {
	peer := &peerConn{
		sendQ:    make(chan outboundMessage, 8),
		closed:   make(chan struct{}),
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}

	if err := peer.enqueueTxRecon(p2p.TxReconMessage{TxIDs: [][32]byte{{1}}}); err != nil {
		t.Fatalf("enqueue first recon: %v", err)
	}
	if err := peer.enqueueTxRecon(p2p.TxReconMessage{TxIDs: [][32]byte{{2}}}); err != nil {
		t.Fatalf("enqueue second recon: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		recon, ok := envelope.msg.(p2p.TxReconMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
		}
		if len(recon.TxIDs) != 2 {
			t.Fatalf("coalesced recon size = %d, want 2", len(recon.TxIDs))
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for coalesced recon message")
	}
}
