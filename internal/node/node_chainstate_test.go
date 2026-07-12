package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utxochecksum"
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestAppendRecentTimeKeepsLastElevenTimestamps(t *testing.T) {
	times := make([]uint64, 0, recentTimeWindow)
	for i := uint64(1); i <= recentTimeWindow+2; i++ {
		times = appendRecentTime(times, i)
	}
	want := []uint64{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	if !slices.Equal(times, want) {
		t.Fatalf("recent times = %v, want %v", times, want)
	}
}

// merkleRootForNodeTest keeps header fixtures anchored to the spec-tagged
// tree rules instead of reusing the production consensus.MerkleRoot path.
func TestApplyBlockRequiresTip(t *testing.T) {
	state := NewChainState(types.Regtest)
	_, err := state.ApplyBlock(&types.Block{})
	if !errors.Is(err, ErrNoTip) {
		t.Fatalf("expected no tip error, got %v", err)
	}
}

func TestApplyBlockReplacesPublishedUTXOView(t *testing.T) {
	state := NewChainState(types.Regtest)
	genesis := genesisBlock()
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}

	before := state.UTXOs()
	first := nextCoinbaseBlock(0, genesis.Header, before, 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}

	firstCoinbase := consensus.TxID(&first.Txs[0])
	if _, ok := before[types.OutPoint{TxID: firstCoinbase, Vout: 0}]; ok {
		t.Fatal("published pre-block UTXO view was mutated in place")
	}
	if _, ok := state.UTXOs()[types.OutPoint{TxID: firstCoinbase, Vout: 0}]; !ok {
		t.Fatal("current chain view missing applied block coinbase output")
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

func TestCommittedViewReturnsDefensiveSnapshot(t *testing.T) {
	state := NewChainState(types.Regtest)
	block := genesisBlock()
	if _, err := state.InitializeFromGenesisBlock(&block); err != nil {
		t.Fatal(err)
	}
	view, ok := state.CommittedView()
	if !ok {
		t.Fatal("expected committed view")
	}
	if got, want := view.TipHash, consensus.HeaderHash(&block.Header); got != want {
		t.Fatalf("tip hash = %x, want %x", got, want)
	}
	if got, want := view.UTXORoot, block.Header.UTXORoot; got != want {
		t.Fatalf("utxo root = %x, want %x", got, want)
	}
	if got := view.UTXOCount; got != 1 {
		t.Fatalf("utxo count = %d, want 1", got)
	}
	if view.UTXOAcc == nil {
		t.Fatal("expected maintained accumulator")
	}
	if view.UTXOAcc == state.utxoAcc {
		t.Fatal("committed view exposed live accumulator handle")
	}

	view.UTXOCount = 0
	view.UTXOAcc = nil

	refreshed, ok := state.CommittedView()
	if !ok {
		t.Fatal("expected committed view after snapshot mutation")
	}
	if got := refreshed.UTXOCount; got != 1 {
		t.Fatalf("refreshed utxo count = %d, want 1", got)
	}
	if refreshed.UTXOAcc == nil {
		t.Fatal("snapshot mutation cleared live accumulator")
	}
}

func TestChainStateAccessorsReturnCopies(t *testing.T) {
	state := NewChainState(types.Regtest)
	block := genesisBlock()
	if _, err := state.InitializeFromGenesisBlock(&block); err != nil {
		t.Fatal(err)
	}

	height := state.TipHeight()
	if height == nil {
		t.Fatal("expected tip height")
	}
	*height = 99
	if got := *state.TipHeight(); got != 0 {
		t.Fatalf("live tip height = %d, want 0", got)
	}

	header := state.TipHeader()
	if header == nil {
		t.Fatal("expected tip header")
	}
	header.Timestamp++
	if got := state.TipHeader().Timestamp; got != block.Header.Timestamp {
		t.Fatalf("live tip timestamp = %d, want %d", got, block.Header.Timestamp)
	}

	utxos := state.UTXOs()
	for outPoint := range utxos {
		delete(utxos, outPoint)
	}
	if got := len(state.UTXOs()); got != 1 {
		t.Fatalf("live utxo count = %d, want 1", got)
	}

	acc := state.UTXOAccumulator()
	if acc == nil {
		t.Fatal("expected accumulator snapshot")
	}
	if acc == state.utxoAcc {
		t.Fatal("utxo accumulator accessor exposed live handle")
	}
	if got, want := acc.Root(), state.utxoAcc.Root(); got != want {
		t.Fatalf("snapshot accumulator root = %x, want %x", got, want)
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

func TestDisconnectBlockRestoresAccumulatorState(t *testing.T) {
	state := NewChainState(types.Regtest)
	genesis := genesisBlock()
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	genesisBlockSizeState := state.BlockSizeState()

	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	var undo []storage.BlockUndoEntry
	parentEntry := &storage.BlockIndexEntry{
		Height:         0,
		Header:         genesis.Header,
		BlockSizeState: genesisBlockSizeState,
	}

	if err := state.DisconnectBlock(&first, undo, parentEntry); err != nil {
		t.Fatalf("disconnect block: %v", err)
	}
	if state.TipHeight() == nil || *state.TipHeight() != 0 {
		t.Fatalf("tip height after disconnect = %v, want 0", state.TipHeight())
	}
	if got, want := consensus.HeaderHash(state.TipHeader()), consensus.HeaderHash(&genesis.Header); got != want {
		t.Fatalf("tip after disconnect = %x, want %x", got, want)
	}
	if got, want := state.UTXORoot(), consensus.ComputedUTXORoot(state.UTXOs()); got != want {
		t.Fatalf("utxo root after disconnect = %x, want %x", got, want)
	}
	if state.UTXOAccumulator() == nil {
		t.Fatal("expected maintained accumulator after disconnect")
	}
	if got, want := state.BlockSizeState(), parentEntry.BlockSizeState; got != want {
		t.Fatalf("block size state after disconnect = %+v, want %+v", got, want)
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

func TestBuildAccumulatorFromStoreMatchesBulkRoot(t *testing.T) {
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	utxos := make(consensus.UtxoSet, 96)
	for i := 0; i < 96; i++ {
		var txid [32]byte
		binary.LittleEndian.PutUint64(txid[:8], uint64(i+1))
		outPoint := types.OutPoint{TxID: txid, Vout: uint32(i % 3)}
		utxos[outPoint] = consensus.UtxoEntry{
			ValueAtoms: uint64(1000 + i),
			Type:       types.OutputXOnlyP2PK,
			PubKey:     nodeSignerPubKey(byte(i + 1)),
		}
	}
	genesis := genesisBlock()
	if err := store.WriteFullState(&storage.StoredChainState{
		Profile:   types.Regtest,
		Height:    0,
		TipHeader: genesis.Header,
		BlockSizeState: consensus.BlockSizeState{
			BlockSize: types.BlockHeaderEncodedLen,
			Epsilon:   16_000_000,
			Beta:      16_000_000,
		},
		UTXOs: utxos,
	}); err != nil {
		t.Fatal(err)
	}

	got, count, err := buildAccumulatorFromStore(store)
	if err != nil {
		t.Fatalf("build accumulator from store: %v", err)
	}
	want, err := consensus.UtxoAccumulator(utxos)
	if err != nil {
		t.Fatalf("bulk accumulator: %v", err)
	}
	if count != len(utxos) {
		t.Fatalf("count = %d, want %d", count, len(utxos))
	}
	if got.Root() != want.Root() {
		t.Fatalf("root = %x, want %x", got.Root(), want.Root())
	}
}

func TestPersistentRoundtripFromMeta(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	genesis := genesisBlock()
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := persistent.ApplyBlock(&block); err != nil {
		t.Fatal(err)
	}
	if err := persistent.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPersistentChainStateFromMeta(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.ChainState().TipHeight() == nil || *reopened.ChainState().TipHeight() != 1 {
		t.Fatalf("reopened tip height mismatch: %v", reopened.ChainState().TipHeight())
	}
	if got, want := reopened.ChainState().UTXORoot(), consensus.ComputedUTXORoot(reopened.ChainState().UTXOs()); got != want {
		t.Fatalf("reopened utxo root = %x, want %x", got, want)
	}
	iterated := 0
	if err := reopened.ChainState().ForEachUTXO(func(types.OutPoint, consensus.UtxoEntry) error {
		iterated++
		return nil
	}); err != nil {
		t.Fatalf("ForEachUTXO: %v", err)
	}
	if iterated != reopened.ChainState().UTXOCount() {
		t.Fatalf("iterated utxos = %d, want %d", iterated, reopened.ChainState().UTXOCount())
	}
	if reopened.ChainState().UTXOAccumulator() == nil {
		t.Fatal("expected accumulator after metadata reopen")
	}
}

func TestPersistentChainStatePreservesPQTypedUTXOAcrossReloadSpendAndReorg(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	genesis := genesisBlock()
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	maturePersistentGenesisForNodeTest(t, persistent)
	// Copy the branch point into a detached state because this fixture remains
	// in use after the persistent store is deliberately closed and reopened.
	baseHeight := *persistent.ChainState().TipHeight()
	baseHeader := *persistent.ChainState().TipHeader()
	baseBlockSizeState := persistent.ChainState().BlockSizeState()
	baseUTXOs := persistent.ChainState().UTXOs()
	verificationKey, privateKey, err := crypto.GenerateMLDSA65Key(crand.Reader)
	if err != nil {
		t.Fatalf("GenerateMLDSA65Key: %v", err)
	}
	pqLock := consensus.PQLock(verificationKey)
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesisOut := types.OutPoint{TxID: genesisTxID, Vout: 0}
	parentTx := spendTxForNodeTestToOutputs(t, 7, genesisOut, 50, []types.TxOutput{
		types.NewPQLockOutput(40, pqLock),
	})
	parentCoinbase := coinbaseTxForHeight(baseHeight+1, []types.TxOutput{types.NewXOnlyOutput(1, nodeSignerPubKey(9))})
	parentBlock := blockWithTxsForNodeTest(t, baseHeight, baseHeader, persistent.ChainState().UTXOs(), []types.Transaction{parentCoinbase, parentTx}, baseHeader.Timestamp+600)
	if _, err := persistent.ApplyBlock(&parentBlock); err != nil {
		t.Fatalf("ApplyBlock(parent): %v", err)
	}
	parentTxID := consensus.TxID(&parentTx)
	parentOut := types.OutPoint{TxID: parentTxID, Vout: 0}
	parentEntry, ok := persistent.ChainState().UTXOs()[parentOut]
	if !ok {
		t.Fatal("missing live PQ parent output")
	}
	if parentEntry.Type != types.OutputPQLock32 || parentEntry.Payload32 != pqLock {
		t.Fatalf("live PQ parent output lost type/payload: %+v", parentEntry)
	}
	if err := persistent.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPersistentChainStateFromMeta(path, types.Regtest)
	if err != nil {
		t.Fatalf("OpenPersistentChainStateFromMeta: %v", err)
	}
	reopenedEntry, ok := reopened.ChainState().UTXOs()[parentOut]
	if !ok {
		t.Fatal("reopened chain missing PQ parent output")
	}
	if reopenedEntry != parentEntry {
		t.Fatalf("reopened PQ parent output = %+v, want %+v", reopenedEntry, parentEntry)
	}
	childTx := spendPQTxForNodeTestToOutputs(t, parentOut, 40, verificationKey, privateKey, []types.TxOutput{
		types.NewXOnlyOutput(30, nodeSignerPubKey(10)),
	})
	childCoinbase := coinbaseTxForHeight(baseHeight+2, []types.TxOutput{types.NewXOnlyOutput(1, nodeSignerPubKey(11))})
	childBlock := blockWithTxsForNodeTest(t, baseHeight+1, parentBlock.Header, reopened.ChainState().UTXOs(), []types.Transaction{childCoinbase, childTx}, parentBlock.Header.Timestamp+600)
	if _, err := reopened.ApplyBlock(&childBlock); err != nil {
		t.Fatalf("ApplyBlock(child): %v", err)
	}
	if _, ok := reopened.ChainState().UTXOs()[parentOut]; ok {
		t.Fatal("spent PQ parent output remained after child block")
	}

	altState := NewChainState(types.Regtest)
	if err := altState.InitializeTip(baseHeight, baseHeader, baseBlockSizeState, baseUTXOs); err != nil {
		t.Fatal(err)
	}
	alt1 := nextCoinbaseBlock(baseHeight, baseHeader, altState.UTXOs(), 12, baseHeader.Timestamp+601)
	if _, err := altState.ApplyBlock(&alt1); err != nil {
		t.Fatal(err)
	}
	alt2 := nextCoinbaseBlock(baseHeight+1, alt1.Header, altState.UTXOs(), 13, alt1.Header.Timestamp+600)
	if _, err := altState.ApplyBlock(&alt2); err != nil {
		t.Fatal(err)
	}
	alt3 := nextCoinbaseBlock(baseHeight+2, alt2.Header, altState.UTXOs(), 14, alt2.Header.Timestamp+600)
	if _, err := reopened.ApplyBlock(&alt1); err != nil {
		t.Fatalf("ApplyBlock(alt1): %v", err)
	}
	if _, err := reopened.ApplyBlock(&alt2); err != nil {
		t.Fatalf("ApplyBlock(alt2): %v", err)
	}
	if _, err := reopened.ApplyBlock(&alt3); err != nil {
		t.Fatalf("ApplyBlock(alt3): %v", err)
	}
	if _, ok := reopened.ChainState().UTXOs()[parentOut]; ok {
		t.Fatal("reorged-out PQ parent output remained live")
	}
	if got, want := reopened.ChainState().UTXORoot(), consensus.ComputedUTXORoot(reopened.ChainState().UTXOs()); got != want {
		t.Fatalf("post-reorg root = %x, want %x", got, want)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	finalReopen, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	defer finalReopen.Close()
	if _, ok := finalReopen.ChainState().UTXOs()[parentOut]; ok {
		t.Fatal("final reopen restored reorged-out PQ parent output")
	}
}

func TestPersistentChainStateCutsOverToStoreBackedUTXOs(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer persistent.Close()

	genesis := genesisBlock()
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	if persistent.state.utxos != nil {
		t.Fatal("expected persistent chain state to drop materialized utxos after genesis bootstrap")
	}
	if persistent.state.utxoLookup == nil || persistent.state.utxoScan == nil {
		t.Fatal("expected persistent chain state to bind store-backed utxo access after genesis bootstrap")
	}

	active := NewChainState(types.Regtest)
	if _, err := active.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, active.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := persistent.ApplyBlock(&block); err != nil {
		t.Fatal(err)
	}
	if persistent.state.utxos != nil {
		t.Fatal("expected persistent chain state to stay on store-backed utxo access after block apply")
	}
	if persistent.state.utxoLookup == nil || persistent.state.utxoScan == nil {
		t.Fatal("expected persistent chain state to retain store-backed utxo access after block apply")
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

func TestHeaderReplayRejectsTimestampAtMedianTimePast(t *testing.T) {
	chain := NewHeaderChain(types.Regtest)
	genesis := genesisBlock()
	if err := chain.InitializeFromGenesisHeader(genesis.Header); err != nil {
		t.Fatal(err)
	}

	prev := genesis.Header
	for height := uint64(0); height < 11; height++ {
		header := nextCoinbaseBlock(height, prev, consensus.UtxoSet{}, byte(height+1), genesis.Header.Timestamp+height+1).Header
		if err := chain.ApplyHeader(&header); err != nil {
			t.Fatalf("apply header %d: %v", height+1, err)
		}
		prev = header
	}

	bad := nextCoinbaseBlock(11, prev, consensus.UtxoSet{}, 42, genesis.Header.Timestamp+6).Header
	err := chain.ApplyHeader(&bad)
	if !errors.Is(err, consensus.ErrTimestampTooEarly) {
		t.Fatalf("ApplyHeader error = %v, want %v", err, consensus.ErrTimestampTooEarly)
	}
}

func TestHeaderReplayRejectsTimestampBeyondLocalSystemTimeWindow(t *testing.T) {
	chain := NewHeaderChain(types.Regtest)
	genesis := genesisBlock()
	if err := chain.InitializeFromGenesisHeader(genesis.Header); err != nil {
		t.Fatal(err)
	}

	bad := nextCoinbaseBlock(0, genesis.Header, consensus.UtxoSet{}, 42, uint64(time.Now().Unix())+consensus.MaxFutureBlockTimeSeconds+1).Header
	err := chain.ApplyHeader(&bad)
	if !errors.Is(err, consensus.ErrTimestampTooFarFuture) {
		t.Fatalf("ApplyHeader error = %v, want %v", err, consensus.ErrTimestampTooFarFuture)
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
	if got, want := persistent.ChainState().UTXORoot(), consensus.ComputedUTXORoot(persistent.ChainState().UTXOs()); got != want {
		t.Fatalf("unexpected reorged utxo root: got %x want %x", got, want)
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
	if got, want := reopened.ChainState().UTXORoot(), consensus.ComputedUTXORoot(reopened.ChainState().UTXOs()); got != want {
		t.Fatalf("unexpected reopened utxo root after reorg: got %x want %x", got, want)
	}
}

func TestPersistentChainStateApplyBlockReturnsBranchTransition(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer persistent.Close()

	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	maturePersistentGenesisForNodeTest(t, persistent)
	baseHeight := *persistent.ChainState().TipHeight()
	baseHeader := *persistent.ChainState().TipHeader()
	baseBlockSizeState := persistent.ChainState().BlockSizeState()
	baseUTXOs := persistent.ChainState().UTXOs()
	newBranch := func() *ChainState {
		branch := NewChainState(types.Regtest)
		if err := branch.InitializeTip(baseHeight, baseHeader, baseBlockSizeState, baseUTXOs); err != nil {
			t.Fatal(err)
		}
		return branch
	}

	active := newBranch()
	a1 := nextCoinbaseBlock(baseHeight, baseHeader, active.UTXOs(), 3, baseHeader.Timestamp+600)
	if _, err := active.ApplyBlock(&a1); err != nil {
		t.Fatal(err)
	}
	spend := spendTxForNodeTest(t, 7, types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}, 50, 8, 1)
	a2Coinbase := coinbaseTxForHeight(baseHeight+2, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(3)}})
	a2 := blockWithTxsForNodeTest(t, baseHeight+1, a1.Header, active.UTXOs(), []types.Transaction{a2Coinbase, spend}, a1.Header.Timestamp+600)
	if _, err := active.ApplyBlock(&a2); err != nil {
		t.Fatal(err)
	}

	side := newBranch()
	b1 := nextCoinbaseBlock(baseHeight, baseHeader, side.UTXOs(), 9, baseHeader.Timestamp+601)
	if _, err := side.ApplyBlock(&b1); err != nil {
		t.Fatal(err)
	}
	b2 := nextCoinbaseBlock(baseHeight+1, b1.Header, side.UTXOs(), 10, b1.Header.Timestamp+600)
	if _, err := side.ApplyBlock(&b2); err != nil {
		t.Fatal(err)
	}
	b3 := nextCoinbaseBlock(baseHeight+2, b2.Header, side.UTXOs(), 11, b2.Header.Timestamp+600)

	_, transition, err := persistent.ApplyBlockWithTransition(&a1)
	if err != nil {
		t.Fatalf("ApplyBlockWithTransition(a1): %v", err)
	}
	if len(transition.Connected) != 1 || consensus.HeaderHash(&transition.Connected[0].Header) != consensus.HeaderHash(&a1.Header) {
		t.Fatalf("active-tip transition connected = %d blocks, want [a1]", len(transition.Connected))
	}
	if len(transition.DisconnectedTxs) != 0 {
		t.Fatalf("active-tip transition disconnected_txs = %d, want 0", len(transition.DisconnectedTxs))
	}

	if _, err := persistent.ApplyBlock(&a2); err != nil {
		t.Fatalf("ApplyBlock(a2): %v", err)
	}
	if _, err := persistent.ApplyBlock(&b1); err != nil {
		t.Fatalf("ApplyBlock(b1): %v", err)
	}
	if _, err := persistent.ApplyBlock(&b2); err != nil {
		t.Fatalf("ApplyBlock(b2): %v", err)
	}

	_, transition, err = persistent.ApplyBlockWithTransition(&b3)
	if err != nil {
		t.Fatalf("ApplyBlockWithTransition(b3): %v", err)
	}
	if len(transition.Connected) != 3 {
		t.Fatalf("reorg transition connected = %d, want 3", len(transition.Connected))
	}
	if got := consensus.HeaderHash(&transition.Connected[0].Header); got != consensus.HeaderHash(&b1.Header) {
		t.Fatalf("connected[0] = %x, want %x", got, consensus.HeaderHash(&b1.Header))
	}
	if got := consensus.HeaderHash(&transition.Connected[2].Header); got != consensus.HeaderHash(&b3.Header) {
		t.Fatalf("connected[2] = %x, want %x", got, consensus.HeaderHash(&b3.Header))
	}
	if len(transition.DisconnectedTxs) != 1 {
		t.Fatalf("reorg transition disconnected_txs = %d, want 1", len(transition.DisconnectedTxs))
	}
	if got := consensus.TxID(&transition.DisconnectedTxs[0]); got != consensus.TxID(&spend) {
		t.Fatalf("disconnected tx = %x, want %x", got, consensus.TxID(&spend))
	}
}

func TestPersistentChainStateReorgsToHigherWorkBranchFromMeta(t *testing.T) {
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
	if _, err := persistent.ApplyBlock(&b1); err != nil {
		t.Fatal(err)
	}
	if _, err := persistent.ApplyBlock(&b2); err != nil {
		t.Fatal(err)
	}
	if _, err := persistent.ApplyBlock(&b3); err != nil {
		t.Fatal(err)
	}

	sideTipHash := consensus.HeaderHash(&b3.Header)
	if err := persistent.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPersistentChainStateFromMeta(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := consensus.HeaderHash(reopened.ChainState().TipHeader()); got != sideTipHash {
		t.Fatalf("unexpected reopened tip after metadata reorg open: got %x want %x", got, sideTipHash)
	}
	if got, want := reopened.ChainState().UTXORoot(), consensus.ComputedUTXORoot(reopened.ChainState().UTXOs()); got != want {
		t.Fatalf("unexpected reopened utxo root after metadata reorg open: got %x want %x", got, want)
	}
	if got, want := reopened.ChainState().UTXOChecksum(), utxochecksum.Compute(reopened.ChainState().UTXOs()); got != want {
		t.Fatalf("unexpected reopened checksum after metadata reorg open: got %x want %x", got, want)
	}
}

func TestPersistentChainStateAcceptsBlockWithIntraBlockSpend(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer persistent.Close()

	params := consensus.RegtestParams()
	genesisCoinbase := coinbaseTxForHeight(0, []types.TxOutput{{ValueAtoms: 50, PubKey: nodeSignerPubKey(7)}})
	genesisTxID := consensus.TxID(&genesisCoinbase)
	genesisAuthID := consensus.AuthID(&genesisCoinbase)
	genesisUTXOs := consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(7)},
	}
	genesis := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			MerkleTxIDRoot: merkleRootForNodeTest([][32]byte{genesisTxID}),
			MerkleAuthRoot: merkleRootForNodeTest([][32]byte{genesisAuthID}),
			UTXORoot:       consensus.ComputedUTXORoot(genesisUTXOs),
			Timestamp:      params.GenesisTimestamp,
			NBits:          params.GenesisBits,
		},
		Txs: []types.Transaction{genesisCoinbase},
	}
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	maturePersistentGenesisForNodeTest(t, persistent)
	baseHeight := *persistent.ChainState().TipHeight()
	baseHeader := *persistent.ChainState().TipHeader()

	genesisOut := types.OutPoint{TxID: genesisTxID, Vout: 0}
	var parent types.Transaction
	var child types.Transaction
	foundLTORPair := false
	for parentSeed := byte(8); parentSeed < 64 && !foundLTORPair; parentSeed++ {
		candidateParent := spendTxForNodeTest(t, 7, genesisOut, 50, parentSeed, 1)
		candidateParentTxID := consensus.TxID(&candidateParent)
		for childSeed := byte(64); childSeed < 128; childSeed++ {
			candidateChild := spendTxForNodeTest(t, parentSeed, types.OutPoint{TxID: candidateParentTxID, Vout: 0}, 49, childSeed, 1)
			candidateChildTxID := consensus.TxID(&candidateChild)
			// LTOR can place a child before its parent because ordering is by txid,
			// not dependency. This is the persistence case that previously lost undo.
			if bytes.Compare(candidateChildTxID[:], candidateParentTxID[:]) < 0 {
				parent = candidateParent
				child = candidateChild
				foundLTORPair = true
				break
			}
		}
	}
	if !foundLTORPair {
		t.Fatal("failed to construct LTOR-compliant same-block spend fixture")
	}
	coinbase := coinbaseTxForHeight(baseHeight+1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(10)}})
	txs := []types.Transaction{coinbase, child, parent}
	txids, _, txRoot, authRoot := consensus.BuildBlockRoots(txs)
	nextUTXOs := cloneUtxos(persistent.ChainState().UTXOs())
	delete(nextUTXOs, genesisOut)
	childTxID := txids[1]
	parentTxID := txids[2]
	nextUTXOs[types.OutPoint{TxID: childTxID, Vout: 0}] = consensus.UtxoEntryFromOutputAtHeight(child.Base.Outputs[0], baseHeight+1, false)
	coinbaseTxID := txids[0]
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = consensus.UtxoEntryFromOutputAtHeight(coinbase.Base.Outputs[0], baseHeight+1, true)
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: baseHeight, Header: baseHeader}, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  consensus.HeaderHash(&baseHeader),
			MerkleTxIDRoot: txRoot,
			MerkleAuthRoot: authRoot,
			UTXORoot:       consensus.ComputedUTXORoot(nextUTXOs),
			Timestamp:      baseHeader.Timestamp + 600,
			NBits:          nbits,
		},
		Txs: txs,
	}
	block.Header = mineHeaderForNodeTest(block.Header)

	summary, err := persistent.ApplyBlock(&block)
	if err != nil {
		t.Fatalf("apply block with intra-block spend: %v", err)
	}
	if got, want := summary.TotalFees, uint64(2); got != want {
		t.Fatalf("total fees = %d, want %d", got, want)
	}
	if persistent.ChainState().TipHeight() == nil || *persistent.ChainState().TipHeight() != baseHeight+1 {
		t.Fatalf("unexpected tip after accepted block: %v", persistent.ChainState().TipHeight())
	}
	if _, ok := persistent.ChainState().UTXOs()[genesisOut]; ok {
		t.Fatal("expected genesis outpoint to be spent")
	}
	if _, ok := persistent.ChainState().UTXOs()[types.OutPoint{TxID: parentTxID, Vout: 0}]; ok {
		t.Fatal("expected parent outpoint to be spent by same-block child")
	}
	if got, want := persistent.ChainState().UTXORoot(), consensus.ComputedUTXORoot(nextUTXOs); got != want {
		t.Fatalf("utxo root after accepted block = %x, want %x", got, want)
	}
	blockHash := consensus.HeaderHash(&block.Header)
	undo, err := persistent.Store().GetUndo(&blockHash)
	if err != nil {
		t.Fatalf("read persisted undo: %v", err)
	}
	if len(undo) != 1 || undo[0].OutPoint != genesisOut {
		t.Fatalf("persisted undo = %+v, want only pre-block genesis spend", undo)
	}
}

func TestPersistentApplyBlockPreservesUTXOChecksumOnTipExtension(t *testing.T) {
	path := t.TempDir()
	genesis := genesisBlock()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatalf("OpenPersistentChainState: %v", err)
	}
	defer persistent.Close()
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatalf("InitializeFromGenesisBlock: %v", err)
	}

	baseState := NewChainState(types.Regtest)
	if _, err := baseState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := persistent.ApplyBlock(&first); err != nil {
		t.Fatalf("ApplyBlock(first): %v", err)
	}

	assertPersistentChecksumMatchesComputed(t, persistent)
}

func TestPersistentApplyBlockPreservesUTXOChecksumAcrossReorg(t *testing.T) {
	path := t.TempDir()
	genesis := genesisBlock()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatalf("OpenPersistentChainState: %v", err)
	}
	defer persistent.Close()
	if _, err := persistent.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatalf("InitializeFromGenesisBlock: %v", err)
	}

	baseState := NewChainState(types.Regtest)
	if _, err := baseState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	active1 := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := baseState.ApplyBlock(&active1); err != nil {
		t.Fatal(err)
	}
	active2 := nextCoinbaseBlock(1, active1.Header, baseState.UTXOs(), 4, active1.Header.Timestamp+600)
	if _, err := persistent.ApplyBlock(&active1); err != nil {
		t.Fatalf("ApplyBlock(active1): %v", err)
	}
	if _, err := persistent.ApplyBlock(&active2); err != nil {
		t.Fatalf("ApplyBlock(active2): %v", err)
	}

	altState := NewChainState(types.Regtest)
	if _, err := altState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	alt1 := nextCoinbaseBlock(0, genesis.Header, altState.UTXOs(), 5, genesis.Header.Timestamp+601)
	if _, err := altState.ApplyBlock(&alt1); err != nil {
		t.Fatal(err)
	}
	alt2 := nextCoinbaseBlock(1, alt1.Header, altState.UTXOs(), 6, alt1.Header.Timestamp+600)
	if _, err := altState.ApplyBlock(&alt2); err != nil {
		t.Fatal(err)
	}
	alt3 := nextCoinbaseBlock(2, alt2.Header, altState.UTXOs(), 7, alt2.Header.Timestamp+600)

	if _, err := persistent.ApplyBlock(&alt1); err != nil {
		t.Fatalf("ApplyBlock(alt1): %v", err)
	}
	if _, err := persistent.ApplyBlock(&alt2); err != nil {
		t.Fatalf("ApplyBlock(alt2): %v", err)
	}
	if _, err := persistent.ApplyBlock(&alt3); err != nil {
		t.Fatalf("ApplyBlock(alt3): %v", err)
	}

	view, ok := persistent.CommittedView()
	if !ok {
		t.Fatal("missing committed view after reorg")
	}
	if got, want := view.TipHash, consensus.HeaderHash(&alt3.Header); got != want {
		t.Fatalf("tip hash = %x, want %x", got, want)
	}
	assertPersistentChecksumMatchesComputed(t, persistent)
}
