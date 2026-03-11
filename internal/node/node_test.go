package node

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/storage"
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
	for nonce := uint64(0); ; nonce++ {
		header.Nonce = nonce
		hash := consensus.HeaderHash(&header)
		if new(big.Int).SetBytes(hash[:]).Cmp(target) <= 0 {
			return header
		}
		if nonce == math.MaxUint64 {
			break
		}
	}
	panic("unable to mine header")
}

func nodeSignerKeyHash(seed byte) [32]byte {
	pubKey, _ := crypto.SignSchnorrForTest([32]byte{seed}, &[32]byte{})
	return crypto.KeyHash(&pubKey)
}

func merkleLeafForNodeTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x00
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

func merkleNodeForNodeTest(left, right [32]byte) [32]byte {
	var buf [65]byte
	buf[0] = 0x01
	copy(buf[1:33], left[:])
	copy(buf[33:], right[:])
	return crypto.Sha256d(buf[:])
}

func merkleSoloForNodeTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x02
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

// merkleRootForNodeTest keeps header fixtures anchored to the spec-tagged
// tree rules instead of reusing the production consensus.MerkleRoot path.
func merkleRootForNodeTest(items [][32]byte) [32]byte {
	if len(items) == 0 {
		panic("merkleRootForNodeTest requires at least one item")
	}
	level := make([][32]byte, len(items))
	for i, item := range items {
		level[i] = merkleLeafForNodeTest(item)
	}
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, merkleSoloForNodeTest(level[i]))
				continue
			}
			next = append(next, merkleNodeForNodeTest(level[i], level[i+1]))
		}
		level = next
	}
	return level[0]
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
	coinbase := coinbaseTxForHeight(0, []types.TxOutput{{ValueAtoms: 50, KeyHash: [32]byte{7}}})
	txids := [][32]byte{consensus.TxID(&coinbase)}
	authids := [][32]byte{consensus.AuthID(&coinbase)}
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: txids[0], Vout: 0}: {ValueAtoms: 50, KeyHash: [32]byte{7}},
	}
	return types.Block{
		Header: types.BlockHeader{
			Version:        1,
			MerkleTxIDRoot: merkleRootForNodeTest(txids),
			MerkleAuthRoot: merkleRootForNodeTest(authids),
			UTXORoot:       consensus.ComputedUTXORoot(utxos),
			Timestamp:      params.GenesisTimestamp,
			NBits:          params.GenesisBits,
		},
		Txs: []types.Transaction{coinbase},
	}
}

func genesisBlockForKeyHash(keyHash [32]byte) types.Block {
	params := consensus.RegtestParams()
	coinbase := coinbaseTxForHeight(0, []types.TxOutput{{ValueAtoms: 50, KeyHash: keyHash}})
	txids := [][32]byte{consensus.TxID(&coinbase)}
	authids := [][32]byte{consensus.AuthID(&coinbase)}
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: txids[0], Vout: 0}: {ValueAtoms: 50, KeyHash: keyHash},
	}
	return types.Block{
		Header: types.BlockHeader{
			Version:        1,
			MerkleTxIDRoot: merkleRootForNodeTest(txids),
			MerkleAuthRoot: merkleRootForNodeTest(authids),
			UTXORoot:       consensus.ComputedUTXORoot(utxos),
			Timestamp:      params.GenesisTimestamp,
			NBits:          params.GenesisBits,
		},
		Txs: []types.Transaction{coinbase},
	}
}

func nextCoinbaseBlock(prevHeight uint64, prev types.BlockHeader, currentUTXOs consensus.UtxoSet, keyHashByte byte, timestamp uint64) types.Block {
	params := consensus.RegtestParams()
	coinbase := coinbaseTxForHeight(prevHeight+1, []types.TxOutput{{ValueAtoms: 1, KeyHash: [32]byte{keyHashByte}}})
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
		MerkleTxIDRoot: merkleRootForNodeTest(txids),
		MerkleAuthRoot: merkleRootForNodeTest(authids),
		UTXORoot:       consensus.ComputedUTXORoot(nextUTXOs),
		Timestamp:      timestamp,
		NBits:          nbits,
	}
	return types.Block{
		Header: mineHeaderForNodeTest(header),
		Txs:    []types.Transaction{coinbase},
	}
}

func blockWithTxsForNodeTest(t *testing.T, prevHeight uint64, prev types.BlockHeader, currentUTXOs consensus.UtxoSet, txs []types.Transaction, timestamp uint64) types.Block {
	t.Helper()
	params := consensus.RegtestParams()
	blockTxs := append([]types.Transaction(nil), txs...)
	if len(blockTxs) > 2 {
		sort.Slice(blockTxs[1:], func(i, j int) bool {
			left := consensus.TxID(&blockTxs[i+1])
			right := consensus.TxID(&blockTxs[j+1])
			return bytes.Compare(left[:], right[:]) < 0
		})
	}

	tempUtxos := cloneUtxos(currentUTXOs)
	claimedInputs := make(map[types.OutPoint]struct{})
	var totalFees uint64
	for i := 1; i < len(blockTxs); i++ {
		tx := &blockTxs[i]
		for _, input := range tx.Base.Inputs {
			if _, ok := claimedInputs[input.PrevOut]; ok {
				t.Fatalf("duplicate claimed input in tx %d: %v", i, input.PrevOut)
			}
			claimedInputs[input.PrevOut] = struct{}{}
		}
		summary, err := consensus.ValidateTx(tx, currentUTXOs, consensus.DefaultConsensusRules())
		if err != nil {
			t.Fatalf("validate tx %d: %v", i, err)
		}
		totalFees += summary.Fee
	}
	for spent := range claimedInputs {
		delete(tempUtxos, spent)
	}
	for i := 1; i < len(blockTxs); i++ {
		tx := &blockTxs[i]
		txid := consensus.TxID(tx)
		for vout, output := range tx.Base.Outputs {
			tempUtxos[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = consensus.UtxoEntry{
				ValueAtoms: output.ValueAtoms,
				KeyHash:    output.KeyHash,
			}
		}
	}

	coinbase := blockTxs[0]
	if len(coinbase.Base.Outputs) == 0 {
		t.Fatal("coinbase missing outputs")
	}
	coinbase.Base.Outputs[0].ValueAtoms += totalFees
	blockTxs[0] = coinbase
	coinbaseTxID := consensus.TxID(&blockTxs[0])
	for vout, output := range blockTxs[0].Base.Outputs {
		tempUtxos[types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			KeyHash:    output.KeyHash,
		}
	}

	txids := make([][32]byte, 0, len(blockTxs))
	authids := make([][32]byte, 0, len(blockTxs))
	for i := range blockTxs {
		txids = append(txids, consensus.TxID(&blockTxs[i]))
		authids = append(authids, consensus.AuthID(&blockTxs[i]))
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: prevHeight, Header: prev}, params)
	if err != nil {
		t.Fatalf("next work required: %v", err)
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  consensus.HeaderHash(&prev),
		MerkleTxIDRoot: merkleRootForNodeTest(txids),
		MerkleAuthRoot: merkleRootForNodeTest(authids),
		UTXORoot:       consensus.ComputedUTXORoot(tempUtxos),
		Timestamp:      timestamp,
		NBits:          nbits,
	}
	return types.Block{
		Header: mineHeaderForNodeTest(header),
		Txs:    blockTxs,
	}
}

func encodePackedTransactionsForNodeTest(txs []types.Transaction) string {
	if len(txs) == 0 {
		return ""
	}
	buf := make([]byte, 0)
	for _, tx := range txs {
		encoded := tx.Encode()
		size := make([]byte, 4)
		binary.LittleEndian.PutUint32(size, uint32(len(encoded)))
		buf = append(buf, size...)
		buf = append(buf, encoded...)
	}
	return base64.StdEncoding.EncodeToString(buf)
}

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
	if got := len(view.UTXOs); got != 1 {
		t.Fatalf("utxo count = %d, want 1", got)
	}
	if view.UTXOAcc == nil {
		t.Fatal("expected maintained accumulator")
	}
	if view.UTXOAcc == state.utxoAcc {
		t.Fatal("committed view exposed live accumulator handle")
	}

	for outPoint := range view.UTXOs {
		delete(view.UTXOs, outPoint)
	}
	view.UTXOAcc = nil

	refreshed, ok := state.CommittedView()
	if !ok {
		t.Fatal("expected committed view after snapshot mutation")
	}
	if got := len(refreshed.UTXOs); got != 1 {
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
	genesisUTXOs := cloneUtxos(state.UTXOs())
	genesisBlockSizeState := state.BlockSizeState()

	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	undo, err := captureUndoEntries(&first, genesisUTXOs)
	if err != nil {
		t.Fatalf("capture undo: %v", err)
	}
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

func TestPersistentChainStateAcceptsBlockWithIntraBlockSpend(t *testing.T) {
	path := t.TempDir()
	persistent, err := OpenPersistentChainState(path, types.Regtest)
	if err != nil {
		t.Fatal(err)
	}
	defer persistent.Close()

	params := consensus.RegtestParams()
	genesisCoinbase := coinbaseTxForHeight(0, []types.TxOutput{{ValueAtoms: 50, KeyHash: nodeSignerKeyHash(7)}})
	genesisTxID := consensus.TxID(&genesisCoinbase)
	genesisAuthID := consensus.AuthID(&genesisCoinbase)
	genesisUTXOs := consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(7)},
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
			if bytes.Compare(candidateParentTxID[:], candidateChildTxID[:]) < 0 {
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
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(10)}})
	txs := []types.Transaction{coinbase, parent, child}
	txids, _, txRoot, authRoot := consensus.BuildBlockRoots(txs)
	nextUTXOs := cloneUtxos(persistent.ChainState().UTXOs())
	delete(nextUTXOs, genesisOut)
	parentTxID := txids[1]
	delete(nextUTXOs, types.OutPoint{TxID: parentTxID, Vout: 0})
	childTxID := txids[2]
	nextUTXOs[types.OutPoint{TxID: childTxID, Vout: 0}] = consensus.UtxoEntry{
		ValueAtoms: child.Base.Outputs[0].ValueAtoms,
		KeyHash:    child.Base.Outputs[0].KeyHash,
	}
	coinbaseTxID := txids[0]
	nextUTXOs[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = consensus.UtxoEntry{
		ValueAtoms: coinbase.Base.Outputs[0].ValueAtoms,
		KeyHash:    coinbase.Base.Outputs[0].KeyHash,
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: 0, Header: genesis.Header}, params)
	if err != nil {
		t.Fatal(err)
	}
	block := types.Block{
		Header: types.BlockHeader{
			Version:        1,
			PrevBlockHash:  consensus.HeaderHash(&genesis.Header),
			MerkleTxIDRoot: txRoot,
			MerkleAuthRoot: authRoot,
			UTXORoot:       consensus.ComputedUTXORoot(nextUTXOs),
			Timestamp:      genesis.Header.Timestamp + 600,
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
	if persistent.ChainState().TipHeight() == nil || *persistent.ChainState().TipHeight() != 1 {
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
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}}),
			parent,
			child,
		},
	}
	msg := buildXThinBlockMessage(block)
	thin, ok := msg.(p2p.XThinBlockMessage)
	if !ok {
		t.Fatalf("relay message type = %T, want XThinBlockMessage", msg)
	}
	matches := pool.ShortIDMatches(func(txid [32]byte) uint64 {
		return thinBlockShortID(thin.Nonce, txid)
	}, xThinShortIDSet(thin))
	state, missing := reconstructXThinBlock(thin, matches)
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

func TestReconstructCompactBlockFromMempoolOverlap(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{11}, Vout: 0}
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
		Header: types.BlockHeader{Version: 1, Timestamp: 199},
		Txs: []types.Transaction{
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}}),
			parent,
			child,
		},
	}
	msg := buildCompactBlockMessage(block)
	compact, ok := msg.(p2p.CompactBlockMessage)
	if !ok {
		t.Fatalf("relay message type = %T, want CompactBlockMessage", msg)
	}
	matches := pool.ShortIDMatches(func(txid [32]byte) uint64 {
		return thinBlockShortID(compact.Nonce, txid)
	}, compactShortIDSet(compact))
	state, missing := reconstructCompactBlock(compact, matches)
	if len(missing) != 0 {
		t.Fatalf("missing indexes = %v, want none", missing)
	}
	if !state.complete() {
		t.Fatal("expected complete compact block reconstruction")
	}
	reconstructed := state.block()
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
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}}),
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
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})},
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

func TestOnCompactBlockRequestsMissingIndexes(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{12}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}
	parent := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTx(parent, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept parent: %v", err)
	}
	child := spendTxForNodeTest(t, 2, types.OutPoint{TxID: consensus.TxID(&parent), Vout: 0}, 49, 3, 1)
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 200},
		Txs: []types.Transaction{
			coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}}),
			parent,
			child,
		},
	}
	compact := buildCompactBlockMessage(block).(p2p.CompactBlockMessage)
	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	if err := svc.onCompactBlockMessage(peer, compact); err != nil {
		t.Fatalf("onCompactBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetBlockTxMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetBlockTxMessage", envelope.msg)
	}
	if len(req.Indexes) != 1 || req.Indexes[0] != 2 {
		t.Fatalf("requested indexes = %v, want [2]", req.Indexes)
	}
}

func TestOnCompactBlockFallsBackToFullBlockWhenOverlapIsLow(t *testing.T) {
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
		Header: types.BlockHeader{Version: 1, Timestamp: 201},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})},
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
	compact := buildCompactBlockMessage(block).(p2p.CompactBlockMessage)
	if err := svc.onCompactBlockMessage(peer, compact); err != nil {
		t.Fatalf("onCompactBlockMessage: %v", err)
	}
	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetDataMessage)
	if !ok {
		t.Fatalf("queued message type = %T, want GetDataMessage", envelope.msg)
	}
	if len(req.Items) != 1 || req.Items[0].Type != p2p.InvTypeBlockExtended {
		t.Fatalf("unexpected fallback request: %+v", req.Items)
	}
}

func TestOnCompactBlockFallsBackToFullBlockWithoutExtendedSupport(t *testing.T) {
	svc := &Service{pool: mempool.New()}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		version: p2p.VersionMessage{
			Services: p2p.ServiceNodeNetwork | p2p.ServiceGrapheneBlockRelay,
		},
	}
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 202},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})},
	}
	for i := 0; i < 8; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 21)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1)}},
			},
		})
	}
	compact := buildCompactBlockMessage(block).(p2p.CompactBlockMessage)
	if err := svc.onCompactBlockMessage(peer, compact); err != nil {
		t.Fatalf("onCompactBlockMessage: %v", err)
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

func TestOpenServiceDefaultsMinerWorkersWhenEnabled(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerEnabled: true,
		MinerKeyHash: nodeSignerKeyHash(42),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	wantWorkers := defaultMinerWorkers()
	if svc.cfg.MinerWorkers != wantWorkers {
		t.Fatalf("miner workers = %d, want %d", svc.cfg.MinerWorkers, wantWorkers)
	}
	if svc.cfg.MaxAncestors != 256 {
		t.Fatalf("max ancestors = %d, want 256", svc.cfg.MaxAncestors)
	}
	if svc.cfg.MaxDescendants != 256 {
		t.Fatalf("max descendants = %d, want 256", svc.cfg.MaxDescendants)
	}
	info := svc.Info()
	if !info.MinerEnabled {
		t.Fatal("expected miner to remain enabled")
	}
	if info.MinerWorkers != wantWorkers {
		t.Fatalf("service info miner workers = %d, want %d", info.MinerWorkers, wantWorkers)
	}
}

func TestOpenServiceDefaultsRPCHardening(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		RPCAddr:      "127.0.0.1:18443",
		RPCAuthToken: "test-token",
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	if svc.cfg.RPCIdleTimeout != 30*time.Second {
		t.Fatalf("rpc idle timeout = %s, want 30s", svc.cfg.RPCIdleTimeout)
	}
	if svc.cfg.RPCMaxHeaderBytes != 8<<10 {
		t.Fatalf("rpc max header bytes = %d, want 8192", svc.cfg.RPCMaxHeaderBytes)
	}
	if svc.cfg.MaxMessageBytes < int(consensus.MainnetParams().BlockSizeFloor) {
		t.Fatalf("max message bytes = %d, want at least %d", svc.cfg.MaxMessageBytes, consensus.MainnetParams().BlockSizeFloor)
	}
}

func TestOpenServiceRejectsWildcardRPCBindWithoutAuth(t *testing.T) {
	genesis := genesisBlock()
	_, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
		RPCAddr: ":18443",
	}, &genesis)
	if err == nil || !strings.Contains(err.Error(), "rpc auth token is required") {
		t.Fatalf("expected wildcard bind auth error, got %v", err)
	}
}

func TestOpenServiceRejectsMiningWithoutKeyHash(t *testing.T) {
	genesis := genesisBlock()
	_, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerEnabled: true,
	}, &genesis)
	if err == nil || !strings.Contains(err.Error(), "miner keyhash is required") {
		t.Fatalf("expected miner keyhash error, got %v", err)
	}
}

func TestApplyPeerHeadersBatchesPersistence(t *testing.T) {
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
	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, state.UTXOs(), 4, first.Header.Timestamp+600)

	applied, err := svc.applyPeerHeaders([]types.BlockHeader{first.Header, second.Header})
	if err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	stored, err := svc.chainState.Store().LoadHeaderState()
	if err != nil {
		t.Fatal(err)
	}
	if stored == nil || stored.Height != 2 || stored.TipHeader != second.Header {
		t.Fatal("stored header tip mismatch after batch")
	}
	secondHash := consensus.HeaderHash(&second.Header)
	entry, err := svc.chainState.Store().GetBlockIndex(&secondHash)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || entry.ChainWork == ([32]byte{}) {
		t.Fatal("expected batched header chainwork to persist")
	}
}

func TestSubmitPackedTxBatchRPC(t *testing.T) {
	genesis := genesisBlock()
	genesis.Txs[0].Base.Outputs[0].KeyHash = nodeSignerKeyHash(7)
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(7)},
	})

	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	params, err := json.Marshal(map[string]string{
		"packed": encodePackedTransactionsForNodeTest([]types.Transaction{tx}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "submitpackedtxbatch",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map", result)
	}
	if got := out["accepted"].(int); got != 1 {
		t.Fatalf("accepted = %d, want 1", got)
	}
}

func TestGetUTXOsByKeyHashesRPC(t *testing.T) {
	genesis := genesisBlock()
	genesis.Txs[0].Base.Outputs = []types.TxOutput{
		{ValueAtoms: 30, KeyHash: nodeSignerKeyHash(7)},
		{ValueAtoms: 20, KeyHash: nodeSignerKeyHash(8)},
	}
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 30, KeyHash: nodeSignerKeyHash(7)},
		types.OutPoint{TxID: genesisTxID, Vout: 1}: {ValueAtoms: 20, KeyHash: nodeSignerKeyHash(8)},
	})

	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	keyHash7 := nodeSignerKeyHash(7)
	params, err := json.Marshal(map[string][]string{
		"keyhashes": []string{hex.EncodeToString(keyHash7[:])},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getutxosbykeyhashes",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map", result)
	}
	utxos, ok := out["utxos"].([]map[string]any)
	if ok {
		_ = utxos
	}
	raw, err := json.Marshal(out["utxos"])
	if err != nil {
		t.Fatal(err)
	}
	var decoded []map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("utxo count = %d, want 1", len(decoded))
	}
	if got := decoded[0]["value"].(float64); got != 30 {
		t.Fatalf("value = %v, want 30", got)
	}
}

func TestSeedStressLanesRPCAndInfo(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	keyHash := nodeSignerKeyHash(7)
	params, err := json.Marshal(map[string]any{
		"keyhashes":             []string{hex.EncodeToString(keyHash[:]), hex.EncodeToString(keyHash[:])},
		"wait_for_confirmation": false,
		"reserve_top_up":        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "seedstresslanes",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC seedstresslanes: %v", err)
	}
	out, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map", result)
	}
	if got := out["count"].(int); got != 2 {
		t.Fatalf("output count = %d, want 2", got)
	}
	if got := out["confirmed"].(bool); got {
		t.Fatal("expected unconfirmed seedstresslanes result")
	}

	infoResult, err := svc.dispatchRPC(rpcRequest{Method: "getstresslaneinfo"})
	if err != nil {
		t.Fatalf("dispatchRPC getstresslaneinfo: %v", err)
	}
	info, ok := infoResult.(StressLaneInfo)
	if !ok {
		t.Fatalf("result type = %T, want StressLaneInfo", infoResult)
	}
	if info.PendingBatches != 1 {
		t.Fatalf("pending batches = %d, want 1", info.PendingBatches)
	}
	if info.ReserveUTXOs == 0 {
		t.Fatal("expected reserve utxo info")
	}
}

func TestGetChainStateRPC(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	result, err := svc.dispatchRPC(rpcRequest{Method: "getchainstate"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(ChainStateInfo)
	if !ok {
		t.Fatalf("result type = %T, want ChainStateInfo", result)
	}
	if out.TipHeight != 0 {
		t.Fatalf("tip height = %d, want 0", out.TipHeight)
	}
	if out.UTXOCount != 1 {
		t.Fatalf("utxo count = %d, want 1", out.UTXOCount)
	}
	if out.NextBlockSizeLimit == 0 {
		t.Fatal("expected next block size limit")
	}
}

func TestGetMempoolInfoRPC(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesis.Txs[0].Base.Outputs[0].ValueAtoms = 1_000
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 1_000, KeyHash: nodeSignerKeyHash(7)},
	})
	svc, err := OpenService(ServiceConfig{
		Profile:            types.Regtest,
		DBPath:             t.TempDir(),
		MinRelayFeePerByte: 1,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 1_000, 8, 200)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}
	result, err := svc.dispatchRPC(rpcRequest{Method: "getmempoolinfo"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(MempoolInfo)
	if !ok {
		t.Fatalf("result type = %T, want MempoolInfo", result)
	}
	if out.Count != 1 {
		t.Fatalf("count = %d, want 1", out.Count)
	}
	if out.Bytes <= 0 {
		t.Fatalf("bytes = %d, want > 0", out.Bytes)
	}
	if out.CandidateFrontier <= 0 {
		t.Fatalf("candidate frontier = %d, want > 0", out.CandidateFrontier)
	}
}

func TestGetMiningInfoRPC(t *testing.T) {
	genesis := genesisBlock()
	keyHash := nodeSignerKeyHash(9)
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerEnabled: true,
		MinerWorkers: 2,
		MinerKeyHash: keyHash,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	result, err := svc.dispatchRPC(rpcRequest{Method: "getmininginfo"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(MiningInfo)
	if !ok {
		t.Fatalf("result type = %T, want MiningInfo", result)
	}
	if !out.Enabled {
		t.Fatal("expected mining enabled")
	}
	if out.Workers != 2 {
		t.Fatalf("workers = %d, want 2", out.Workers)
	}
	if out.MinerKeyHash != hex.EncodeToString(keyHash[:]) {
		t.Fatalf("miner keyhash = %q", out.MinerKeyHash)
	}
	if out.CurrentBits == 0 || out.NextBits == 0 {
		t.Fatal("expected current and next bits")
	}
}

func TestGetMetricsRPC(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesis.Txs[0].Base.Outputs[0].ValueAtoms = 1_000
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 1_000, KeyHash: nodeSignerKeyHash(7)},
	})
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 1_000, 8, 200)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}
	svc.noteRelaySent(relayMessageClass{txBatchItems: 4, blockInvItems: 1})
	svc.noteBlockAccepted()
	svc.noteTemplateRebuild()
	svc.noteTemplateInterruption()
	svc.noteTxReconRetry(2)
	svc.noteDirectFallback(1, 3)
	svc.noteTxRequestsReceived(5)
	svc.noteTxNotFoundSent(2)
	svc.noteTxNotFoundReceived(1)
	svc.noteKnownTxClears(4)
	svc.noteDuplicateSuppression(6)
	svc.noteWriterStarvation(1)
	svc.perf.noteAdmissionDuration(15 * time.Millisecond)
	svc.perf.noteTemplateDuration(25 * time.Millisecond)
	svc.perf.noteBlockApplyDuration(35 * time.Millisecond)
	svc.perf.noteBlockApplyLockWaitDuration(7 * time.Millisecond)
	svc.perf.noteRelayFlushDuration(5 * time.Millisecond)
	svc.perf.noteSyncRequestDuration(8 * time.Millisecond)
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = svc
	peer.controlQ = make(chan outboundMessage, 4)
	peer.relayPriorityQ = make(chan outboundMessage, 4)
	peer.controlQ <- outboundMessage{}
	peer.relayPriorityQ <- outboundMessage{}
	peer.sendQ <- outboundMessage{}
	peer.localRelayTxs[[32]byte{9}] = localRelayFallbackState{announcedAt: time.Now()}
	svc.peers[peer.addr] = peer

	result, err := svc.dispatchRPC(rpcRequest{Method: "getmetrics"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(PerformanceMetrics)
	if !ok {
		t.Fatalf("result type = %T, want PerformanceMetrics", result)
	}
	if out.Counters.AdmittedTxs == 0 {
		t.Fatal("expected admitted tx counter")
	}
	if out.Counters.RelayedTxItems != 4 {
		t.Fatalf("relayed tx items = %d, want 4", out.Counters.RelayedTxItems)
	}
	if out.Counters.RelayedBlockItems != 1 {
		t.Fatalf("relayed block items = %d, want 1", out.Counters.RelayedBlockItems)
	}
	if out.Counters.BlocksAccepted != 1 {
		t.Fatalf("blocks accepted = %d, want 1", out.Counters.BlocksAccepted)
	}
	if out.Counters.TemplateRebuilds != 1 || out.Counters.TemplateInterruptions != 1 {
		t.Fatalf("unexpected template counters: %+v", out.Counters)
	}
	if out.Counters.TxReconRetries != 2 || out.Counters.DirectFallbackBatches != 1 || out.Counters.DirectFallbackTxs != 3 {
		t.Fatalf("unexpected relay counters: %+v", out.Counters)
	}
	if out.Counters.TxRequestsReceived != 5 || out.Counters.TxNotFoundSent != 2 || out.Counters.TxNotFoundReceived != 1 {
		t.Fatalf("unexpected tx request/notfound counters: %+v", out.Counters)
	}
	if out.Counters.KnownTxClears != 4 || out.Counters.DuplicateSuppressions != 6 || out.Counters.WriterStarvation != 1 {
		t.Fatalf("unexpected duplicate/starvation counters: %+v", out.Counters)
	}
	if out.Gauges.MempoolTxs != 1 {
		t.Fatalf("mempool txs = %d, want 1", out.Gauges.MempoolTxs)
	}
	if out.Gauges.CandidateFrontier <= 0 {
		t.Fatalf("candidate frontier = %d, want > 0", out.Gauges.CandidateFrontier)
	}
	if out.Gauges.ControlQueueDepth != 1 || out.Gauges.PriorityQueueDepth != 1 || out.Gauges.SendQueueDepth != 1 || out.Gauges.PendingLocalRelayTxs != 1 {
		t.Fatalf("unexpected relay gauges: %+v", out.Gauges)
	}
	if out.Latency.Admission.Count == 0 || out.Latency.Template.Count == 0 || out.Latency.BlockApply.Count == 0 || out.Latency.BlockApplyLockWait.Count == 0 || out.Latency.RelayFlush.Count == 0 || out.Latency.SyncReq.Count == 0 {
		t.Fatalf("expected latency samples in all metric groups: %+v", out.Latency)
	}
}

func TestGetBlockHashByHeightRPC(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	params, err := json.Marshal(map[string]any{"height": uint64(0)})
	if err != nil {
		t.Fatalf("Marshal params: %v", err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getblockhashbyheight",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", result)
	}
	if got, ok := out["height"].(uint64); !ok || got != 0 {
		t.Fatalf("height = %#v, want 0", out["height"])
	}
	hash := consensus.HeaderHash(&genesis.Header)
	want := hex.EncodeToString(hash[:])
	if got, ok := out["hash"].(string); !ok || got != want {
		t.Fatalf("hash = %#v, want %q", out["hash"], want)
	}
}

func TestGetWalletActivityByKeyHashesRPC(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	genesisTxID := consensus.TxID(&genesis.Txs[0])
	spend := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}})
	block := blockWithTxsForNodeTest(t, 0, genesis.Header, svc.chainState.ChainState().UTXOs(), []types.Transaction{coinbase, spend}, genesis.Header.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(block); err != nil {
		t.Fatalf("acceptMinedBlock: %v", err)
	}

	keyHash7 := nodeSignerKeyHash(7)
	params, err := json.Marshal(map[string]any{
		"keyhashes": []string{hex.EncodeToString(keyHash7[:])},
		"limit":     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getwalletactivitybykeyhashes",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map", result)
	}
	raw, err := json.Marshal(out["activity"])
	if err != nil {
		t.Fatal(err)
	}
	var decoded []map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("activity count = %d, want 1", len(decoded))
	}
	if got := decoded[0]["sent"].(float64); got != 50 {
		t.Fatalf("sent = %v, want 50", got)
	}
	if got := decoded[0]["fee"].(float64); got != 1 {
		t.Fatalf("fee = %v, want 1", got)
	}
}

func TestEstimateFeeRPC(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesis.Txs[0].Base.Outputs[0].ValueAtoms = 5_000
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 5_000, KeyHash: nodeSignerKeyHash(7)},
	})
	svc, err := OpenService(ServiceConfig{
		Profile:            types.Regtest,
		DBPath:             t.TempDir(),
		MinRelayFeePerByte: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	txA := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 5_000, 8, 2_000)
	if _, err := svc.SubmitTx(txA); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}
	params, err := json.Marshal(map[string]any{"target_blocks": 1})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "estimatefee",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map", result)
	}
	if got := out["fee_per_byte"].(uint64); got < 2 {
		t.Fatalf("fee_per_byte = %d, want at least 2", got)
	}
}

func TestSubmitDecodedTxsAcceptsDependentChainBatch(t *testing.T) {
	genesis := genesisBlock()
	genesis.Txs[0].Base.Outputs[0].KeyHash = nodeSignerKeyHash(7)
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(7)},
	})

	svc, err := OpenService(ServiceConfig{
		Profile:        types.Regtest,
		DBPath:         t.TempDir(),
		MaxAncestors:   256,
		MaxDescendants: 256,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	txs := make([]types.Transaction, 0, 64)
	prevOut := types.OutPoint{TxID: genesisTxID, Vout: 0}
	prevValue := uint64(50)
	currentSeed := byte(7)
	for i := 0; i < 32; i++ {
		nextSeed := currentSeed + 1
		tx := spendTxForNodeTest(t, currentSeed, prevOut, prevValue, nextSeed, 1)
		txs = append(txs, tx)
		prevOut = types.OutPoint{TxID: consensus.TxID(&tx), Vout: 0}
		prevValue = tx.Base.Outputs[0].ValueAtoms
		currentSeed = nextSeed
	}

	admissions, errs, _, mempoolSize := svc.submitDecodedTxs(txs)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("batch err at %d: %v", i, err)
		}
	}
	if mempoolSize != len(txs) {
		t.Fatalf("mempool size = %d, want %d", mempoolSize, len(txs))
	}
	for i, admission := range admissions {
		if admission.Orphaned {
			t.Fatalf("admission %d unexpectedly orphaned", i)
		}
	}
}

func TestPlanTxRelayReconReducesDenseFanoutAndDistributesTxs(t *testing.T) {
	peers := make([]*peerConn, 0, 16)
	for i := 0; i < 16; i++ {
		peers = append(peers, &peerConn{addr: fmt.Sprintf("peer-%02d:18444", i)})
	}
	txids := [][32]byte{{1}, {2}, {3}, {4}, {5}, {6}}
	batches := planTxRelayRecon(peers, txids)
	if len(batches) == 0 {
		t.Fatal("expected relay batches")
	}
	seenPeers := make(map[string]struct{})
	txFanout := make(map[[32]byte]int)
	for _, batch := range batches {
		seenPeers[batch.peer.addr] = struct{}{}
		for _, txid := range batch.txids {
			txFanout[txid]++
		}
	}
	if len(seenPeers) <= 4 {
		t.Fatalf("dense relay only used %d peers, want broader distribution", len(seenPeers))
	}
	wantFanout := txRelayFanout(len(peers))
	for _, txid := range txids {
		if got := txFanout[txid]; got != wantFanout {
			t.Fatalf("tx %x fanout = %d, want %d", txid, got, wantFanout)
		}
	}
}

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

	outputs, err := svc.MineFundingOutputs([][32]byte{nodeSignerKeyHash(7), nodeSignerKeyHash(8), nodeSignerKeyHash(9)})
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
		entry, ok := utxos[output.OutPoint]
		if !ok {
			t.Fatalf("missing funding outpoint %+v", output.OutPoint)
		}
		if entry.ValueAtoms != output.Value || entry.KeyHash != output.KeyHash {
			t.Fatalf("funding output mismatch for %+v", output.OutPoint)
		}
	}
}

func TestSeedStressLanesProducesConfirmedOutputs(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	requested := [][32]byte{nodeSignerKeyHash(7), nodeSignerKeyHash(8), nodeSignerKeyHash(9)}
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
		entry, ok := utxos[output.OutPoint]
		if !ok {
			t.Fatalf("confirmed output %d missing from utxo set", i)
		}
		if entry.ValueAtoms != output.Value || entry.KeyHash != output.KeyHash {
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

	requested := [][32]byte{nodeSignerKeyHash(7), nodeSignerKeyHash(8)}
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
	coinbase := coinbaseTxForHeight(prevHeight+1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(10)}})
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
		MinerKeyHash: nodeSignerKeyHash(9),
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

	utxos := svc.chainUtxoSnapshot()
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
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerKeyHash: nodeSignerKeyHash(9),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	view, ok := svc.chainState.CommittedView()
	if !ok {
		t.Fatal("missing committed chain view")
	}
	block := nextCoinbaseBlock(view.Height, view.TipHeader, view.UTXOs, 9, view.TipHeader.Timestamp+600)
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

func TestBuildBlockTemplateRefreshesAfterTxAdmission(t *testing.T) {
	minerKey := nodeSignerKeyHash(9)
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerKeyHash: minerKey,
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
	minerKey := nodeSignerKeyHash(9)
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerKeyHash: minerKey,
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

func TestMineOneBlockRefreshesInterruptedTemplate(t *testing.T) {
	minerKey := nodeSignerKeyHash(9)
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerKeyHash: minerKey,
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
	minerKey := nodeSignerKeyHash(9)
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerKeyHash: minerKey,
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

func TestConnectPeerReconnectsAfterDisconnect(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var (
		mu      sync.Mutex
		accepts int
	)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			accepts++
			mu.Unlock()
			go func(conn net.Conn) {
				defer conn.Close()
				wire := p2p.NewConn(conn, p2p.MagicForProfile(types.Regtest), 8<<20)
				if _, err := p2p.Handshake(wire, p2p.VersionMessage{
					Protocol:  1,
					Height:    0,
					Nonce:     1,
					UserAgent: "peer-test",
				}, 2*time.Second); err != nil {
					return
				}
			}(conn)
		}
	}()

	if err := svc.ConnectPeer(ln.Addr().String()); err != nil {
		t.Fatalf("ConnectPeer: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := accepts
		mu.Unlock()
		if got >= 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	got := accepts
	mu.Unlock()
	t.Fatalf("accepted connections = %d, want at least 2", got)
}

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

func TestOpenServiceRestoresPersistedVettedPeers(t *testing.T) {
	path := t.TempDir()
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	svc.recordKnownPeerSuccess("127.0.0.1:18444", time.Unix(1_700_000_000, 0))
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("reopen service: %v", err)
	}
	defer reopened.Close()

	addrs := reopened.knownPeerAddrs()
	if len(addrs) != 1 || addrs[0] != "127.0.0.1:18444" {
		t.Fatalf("known peers after reopen = %v, want [127.0.0.1:18444]", addrs)
	}
	loaded, err := reopened.chainState.Store().LoadKnownPeers()
	if err != nil {
		t.Fatalf("LoadKnownPeers: %v", err)
	}
	record, ok := loaded["127.0.0.1:18444"]
	if !ok {
		t.Fatal("persisted vetted peer missing after reopen")
	}
	if record.LastSuccess.IsZero() {
		t.Fatal("persisted vetted peer missing last-success metadata")
	}
}

func TestRefillOutboundPeersDialsLearnedCandidates(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:          types.Regtest,
		DBPath:           t.TempDir(),
		MaxOutboundPeers: 1,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		wire := p2p.NewConn(conn, p2p.MagicForProfile(types.Regtest), 8<<20)
		if _, err := p2p.Handshake(wire, p2p.VersionMessage{
			Protocol:  1,
			Height:    0,
			Nonce:     1,
			UserAgent: "learned-peer-test",
		}, 2*time.Second); err == nil {
			accepted <- struct{}{}
			time.Sleep(250 * time.Millisecond)
		}
	}()

	svc.rememberKnownPeers([]string{ln.Addr().String()})
	svc.refillOutboundPeers()

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for learned peer dial")
	}
	if got := svc.outboundPeerCount(); got != 1 {
		t.Fatalf("outbound peer count = %d, want 1", got)
	}
}

func TestOutboundRefillCandidatesPreferDistinctNetgroupsAndHealthyPeers(t *testing.T) {
	svc := &Service{
		cfg: ServiceConfig{P2PAddr: "127.0.0.1:18444"},
		knownPeers: map[string]storage.KnownPeerRecord{
			"10.1.1.1:18444": {
				LastSeen:    time.Unix(1_700_000_000, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_000, 0).UTC(),
			},
			"10.1.9.9:18444": {
				LastSeen:    time.Unix(1_700_000_100, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_100, 0).UTC(),
			},
			"10.2.1.1:18444": {
				LastSeen:    time.Unix(1_700_000_050, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_050, 0).UTC(),
			},
			"10.3.1.1:18444": {
				LastSeen:     time.Unix(1_700_000_200, 0).UTC(),
				LastSuccess:  time.Unix(1_700_000_200, 0).UTC(),
				LastAttempt:  time.Now().UTC(),
				FailureCount: 5,
			},
		},
		outboundPeers: make(map[string]struct{}),
		peers:         make(map[string]*peerConn),
		stopCh:        make(chan struct{}),
	}
	manager := &peerManager{svc: svc}

	addrs := manager.outboundRefillCandidates(2)
	if len(addrs) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(addrs))
	}
	if peerNetgroup(addrs[0]) == peerNetgroup(addrs[1]) {
		t.Fatalf("candidate netgroups should differ, got %v", addrs)
	}
	for _, addr := range addrs {
		if addr == "10.3.1.1:18444" {
			t.Fatalf("unhealthy peer should be deprioritized, got %v", addrs)
		}
	}
}

func TestKnownPeerAddrsSkipsSelfEquivalentAddresses(t *testing.T) {
	svc := &Service{
		cfg: ServiceConfig{
			P2PAddr: "0.0.0.0:18444",
			Peers:   []string{"127.0.0.1:18444", "10.9.0.2:18444"},
		},
		knownPeers: map[string]storage.KnownPeerRecord{
			"localhost:18444": {},
			"10.9.0.3:18444":  {},
		},
	}
	manager := &peerManager{svc: svc}

	addrs := manager.knownPeerAddrs()
	if len(addrs) != 2 {
		t.Fatalf("known peer count = %d, want 2 (%v)", len(addrs), addrs)
	}
	for _, addr := range addrs {
		if addr == "127.0.0.1:18444" || addr == "localhost:18444" || addr == "0.0.0.0:18444" {
			t.Fatalf("self-equivalent address leaked into known peers: %v", addrs)
		}
	}
}

func TestOutboundRefillCandidatesSkipSelfEquivalentAddresses(t *testing.T) {
	svc := &Service{
		cfg: ServiceConfig{P2PAddr: "0.0.0.0:18444"},
		knownPeers: map[string]storage.KnownPeerRecord{
			"127.0.0.1:18444": {
				LastSeen:    time.Unix(1_700_000_000, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_000, 0).UTC(),
			},
			"10.9.0.2:18444": {
				LastSeen:    time.Unix(1_700_000_010, 0).UTC(),
				LastSuccess: time.Unix(1_700_000_010, 0).UTC(),
			},
		},
		outboundPeers: make(map[string]struct{}),
		peers:         make(map[string]*peerConn),
		stopCh:        make(chan struct{}),
	}
	manager := &peerManager{svc: svc}

	addrs := manager.outboundRefillCandidates(2)
	if len(addrs) != 1 || addrs[0] != "10.9.0.2:18444" {
		t.Fatalf("outbound refill candidates = %v, want [10.9.0.2:18444]", addrs)
	}
}

func TestConnectPeerSkipsSelfEquivalentAddress(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
		P2PAddr: "0.0.0.0:18444",
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	if err := svc.ConnectPeer("127.0.0.1:18444"); err != nil {
		t.Fatalf("ConnectPeer: %v", err)
	}
	if got := svc.outboundPeerCount(); got != 0 {
		t.Fatalf("outbound peer count = %d, want 0", got)
	}
	if addrs := svc.knownPeerAddrs(); len(addrs) != 0 {
		t.Fatalf("known peers = %v, want none", addrs)
	}
}

func TestPeerWriteLoopSetsWriteDeadline(t *testing.T) {
	conn := &deadlineSpyConn{}
	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 1),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		wire:        p2p.NewConn(conn, p2p.MagicForProfile(types.Regtest), 8<<20),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	peer.sendQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if conn.sawNonZeroWriteDeadline() {
			close(svc.stopCh)
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(svc.stopCh)
	<-done
	t.Fatal("expected peer write loop to set a write deadline")
}

func TestPeerSendFailsFastWhenControlQueueIsSaturated(t *testing.T) {
	peer := &peerConn{
		controlQ:    make(chan outboundMessage, 1),
		sendQ:       make(chan outboundMessage, 1),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	peer.controlQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	start := time.Now()
	err := peer.send(p2p.PongMessage{Nonce: 2})
	if err == nil {
		t.Fatal("expected saturated control queue error")
	}
	if !strings.Contains(err.Error(), "peer send queue saturated") {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > controlMessageEnqueueTimeout*3 {
		t.Fatalf("send blocked too long: %s", elapsed)
	}
}

func TestPeerWriteLoopWakesForControlQueueTraffic(t *testing.T) {
	conn := &deadlineSpyConn{}
	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		wire:        p2p.NewConn(conn, p2p.MagicForProfile(types.Regtest), 1_000_000),
		controlQ:    make(chan outboundMessage, 1),
		sendQ:       make(chan outboundMessage, 1),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	time.Sleep(20 * time.Millisecond)
	peer.controlQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if conn.sawNonZeroWriteDeadline() {
			close(svc.stopCh)
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(svc.stopCh)
	<-done
	t.Fatal("expected peer write loop to wake up for control-queue traffic")
}

func TestPeerWriteLoopPrefersPriorityRelayQueue(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()

	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		wire:           p2p.NewConn(local, p2p.MagicForProfile(types.Regtest), 1_000_000),
		controlQ:       make(chan outboundMessage, 1),
		relayPriorityQ: make(chan outboundMessage, 1),
		sendQ:          make(chan outboundMessage, 1),
		closed:         make(chan struct{}),
		queuedInv:      make(map[p2p.InvVector]int),
		queuedTx:       make(map[[32]byte]int),
		knownTx:        make(map[[32]byte]struct{}),
		pendingThin:    make(map[[32]byte]*pendingThinBlock),
	}
	remoteWire := p2p.NewConn(remote, p2p.MagicForProfile(types.Regtest), 1_000_000)

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	tx := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})
	peer.sendQ <- outboundMessage{
		msg:        p2p.TxBatchMessage{Txs: []types.Transaction{tx}},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.TxBatchMessage{Txs: []types.Transaction{tx}}),
	}
	priorityItems := []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: [32]byte{9}}}
	peer.relayPriorityQ <- outboundMessage{
		msg:        p2p.InvMessage{Items: priorityItems},
		enqueuedAt: time.Now(),
		class:      classifyRelayMessage(p2p.InvMessage{Items: priorityItems}),
		invItems:   priorityItems,
	}

	msgCh := make(chan p2p.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, err := remoteWire.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		msgCh <- msg
	}()

	select {
	case err := <-errCh:
		close(svc.stopCh)
		<-done
		t.Fatalf("read message: %v", err)
	case msg := <-msgCh:
		inv, ok := msg.(p2p.InvMessage)
		if !ok {
			close(svc.stopCh)
			<-done
			t.Fatalf("first message type = %T, want InvMessage", msg)
		}
		if len(inv.Items) != 1 || inv.Items[0].Type != p2p.InvTypeBlock {
			close(svc.stopCh)
			<-done
			t.Fatalf("unexpected first inv payload: %#v", inv.Items)
		}
	case <-time.After(500 * time.Millisecond):
		close(svc.stopCh)
		<-done
		t.Fatal("timed out waiting for prioritized relay message")
	}

	close(svc.stopCh)
	<-done
}

func TestPeerWriteLoopPrefersControlThenPriorityThenSend(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()

	svc := &Service{
		cfg:    ServiceConfig{Profile: types.Regtest, StallTimeout: time.Second},
		stopCh: make(chan struct{}),
	}
	peer := &peerConn{
		wire:           p2p.NewConn(local, p2p.MagicForProfile(types.Regtest), 1_000_000),
		controlQ:       make(chan outboundMessage, 2),
		relayPriorityQ: make(chan outboundMessage, 2),
		sendQ:          make(chan outboundMessage, 2),
		closed:         make(chan struct{}),
		queuedInv:      make(map[p2p.InvVector]int),
		queuedTx:       make(map[[32]byte]int),
		knownTx:        make(map[[32]byte]struct{}),
		pendingThin:    make(map[[32]byte]*pendingThinBlock),
	}
	remoteWire := p2p.NewConn(remote, p2p.MagicForProfile(types.Regtest), 1_000_000)

	tx := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})
	peer.sendQ <- outboundMessage{
		msg:        p2p.TxBatchMessage{Txs: []types.Transaction{tx}},
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneSend,
		class:      classifyRelayMessage(p2p.TxBatchMessage{Txs: []types.Transaction{tx}}),
	}
	priorityItems := []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: [32]byte{9}}}
	peer.relayPriorityQ <- outboundMessage{
		msg:        p2p.InvMessage{Items: priorityItems},
		enqueuedAt: time.Now(),
		lane:       relayQueueLanePriority,
		class:      classifyRelayMessage(p2p.InvMessage{Items: priorityItems}),
		invItems:   priorityItems,
	}
	peer.controlQ <- outboundMessage{
		msg:        p2p.PingMessage{Nonce: 7},
		enqueuedAt: time.Now(),
		lane:       relayQueueLaneControl,
		class:      classifyRelayMessage(p2p.PingMessage{Nonce: 7}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.peerWriteLoop(peer)
	}()

	msg1, err := remoteWire.ReadMessage()
	if err != nil {
		close(svc.stopCh)
		<-done
		t.Fatalf("read first message: %v", err)
	}
	if _, ok := msg1.(p2p.PingMessage); !ok {
		close(svc.stopCh)
		<-done
		t.Fatalf("first message type = %T, want PingMessage", msg1)
	}
	msg2, err := remoteWire.ReadMessage()
	if err != nil {
		close(svc.stopCh)
		<-done
		t.Fatalf("read second message: %v", err)
	}
	if _, ok := msg2.(p2p.InvMessage); !ok {
		close(svc.stopCh)
		<-done
		t.Fatalf("second message type = %T, want InvMessage", msg2)
	}

	close(svc.stopCh)
	<-done
}

func TestPeerCloseDrainsBufferedRelayState(t *testing.T) {
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}

	inv := peer.filterQueuedInv([]p2p.InvVector{{Type: p2p.InvTypeTx, Hash: [32]byte{1}}})
	peer.sendQ <- outboundMessage{
		msg:      p2p.InvMessage{Items: inv},
		invItems: inv,
	}
	tx := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})
	filteredTxs := peer.filterQueuedTxs([]types.Transaction{tx})
	peer.sendQ <- outboundMessage{
		msg: p2p.TxBatchMessage{Txs: filteredTxs},
	}
	peer.pendingTxOrder = [][32]byte{consensus.TxID(&tx)}
	peer.pendingTxByID = map[[32]byte]types.Transaction{consensus.TxID(&tx): tx}
	peer.pendingRecon = [][32]byte{{2}}
	peer.txFlushArmed = true
	peer.reconFlushArmed = true
	peer.knownTx[[32]byte{3}] = struct{}{}
	peer.knownTxOrder = append(peer.knownTxOrder, [32]byte{3})
	peer.pendingThin[[32]byte{4}] = &pendingThinBlock{}

	peer.close()

	if len(peer.sendQ) != 0 {
		t.Fatalf("buffered send queue len = %d, want 0", len(peer.sendQ))
	}
	if len(peer.queuedInv) != 0 {
		t.Fatalf("queued inv len = %d, want 0", len(peer.queuedInv))
	}
	if len(peer.queuedTx) != 0 {
		t.Fatalf("queued tx len = %d, want 0", len(peer.queuedTx))
	}
	if len(peer.pendingTxOrder) != 0 || len(peer.pendingTxByID) != 0 || len(peer.pendingRecon) != 0 {
		t.Fatalf("expected pending relay buffers cleared")
	}
	if peer.txFlushArmed || peer.reconFlushArmed {
		t.Fatal("expected flush flags cleared")
	}
	if len(peer.knownTx) != 0 || len(peer.knownTxOrder) != 0 {
		t.Fatal("expected known tx cache cleared")
	}
	if len(peer.pendingThin) != 0 {
		t.Fatal("expected pending thin state cleared")
	}
}

func TestOnInvMessageRequestsHeadersThroughLastUnknownBlock(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	first := [32]byte{0x11}
	second := [32]byte{0x22}
	msg := p2p.InvMessage{Items: []p2p.InvVector{
		{Type: p2p.InvTypeBlock, Hash: first},
		{Type: p2p.InvTypeBlock, Hash: second},
	}}
	if err := svc.onInvMessage(peer, msg); err != nil {
		t.Fatalf("onInvMessage: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		req, ok := envelope.msg.(p2p.GetHeadersMessage)
		if !ok {
			t.Fatalf("message type = %T, want GetHeadersMessage", envelope.msg)
		}
		if req.StopHash != second {
			t.Fatalf("stop hash = %x, want %x", req.StopHash, second)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for getheaders request")
	}
}

func TestOnInvMessageRequestsFullBlockForKnownHeader(t *testing.T) {
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
	svc.cacheRecentHeader(block.Header)

	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := svc.onInvMessage(peer, p2p.InvMessage{Items: []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}}}); err != nil {
		t.Fatalf("onInvMessage: %v", err)
	}

	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.GetDataMessage)
	if !ok {
		t.Fatalf("message type = %T, want GetDataMessage", envelope.msg)
	}
	if len(req.Items) != 1 || req.Items[0].Hash != hash || req.Items[0].Type != p2p.InvTypeBlockFull {
		t.Fatalf("GetData items = %+v, want full block request for %x", req.Items, hash)
	}
}

func TestOnTxReconMessageRequestsOnlyMissingTxIDs(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{61}, Vout: 0}
	tx := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTx(tx, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept tx: %v", err)
	}

	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	missing := [32]byte{0xaa}
	if err := svc.onTxReconMessage(peer, p2p.TxReconMessage{TxIDs: [][32]byte{consensus.TxID(&tx), missing}}); err != nil {
		t.Fatalf("onTxReconMessage: %v", err)
	}

	envelope := <-peer.sendQ
	req, ok := envelope.msg.(p2p.TxRequestMessage)
	if !ok {
		t.Fatalf("message type = %T, want TxRequestMessage", envelope.msg)
	}
	if len(req.TxIDs) != 1 || req.TxIDs[0] != missing {
		t.Fatalf("requested txids = %x, want only missing tx", req.TxIDs)
	}
}

func TestRunErlayReconcileRoundQueuesUnknownMempoolTxs(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{71}, Vout: 0}
	first := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	firstAdmission, err := pool.AcceptTx(first, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}, consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept first tx: %v", err)
	}
	second := spendTxForNodeTest(t, 2, types.OutPoint{TxID: firstAdmission.TxID, Vout: 0}, 49, 3, 1)
	if _, err := pool.AcceptTx(second, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept second tx: %v", err)
	}

	svc := &Service{
		pool:  pool,
		peers: make(map[string]*peerConn),
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = svc
	peer.noteKnownTxIDs([][32]byte{firstAdmission.TxID})
	svc.peers[peer.addr] = peer

	svc.runErlayReconcileRound()

	envelope := <-peer.sendQ
	recon, ok := envelope.msg.(p2p.TxReconMessage)
	if !ok {
		t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
	}
	if len(recon.TxIDs) != 1 || recon.TxIDs[0] == firstAdmission.TxID {
		t.Fatalf("reconcile txids = %x, want only unknown tx", recon.TxIDs)
	}
}

func TestOnGetDataMessageBatchesTxLookupsAndReportsMisses(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{62}, Vout: 0}
	tx := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	if _, err := pool.AcceptTx(tx, consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept tx: %v", err)
	}

	svc := &Service{pool: pool}
	peer := &peerConn{
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
	}
	missing := [32]byte{0xbb}
	msg := p2p.GetDataMessage{Items: []p2p.InvVector{
		{Type: p2p.InvTypeTx, Hash: consensus.TxID(&tx)},
		{Type: p2p.InvTypeTx, Hash: missing},
	}}
	if err := svc.onGetDataMessage(peer, msg); err != nil {
		t.Fatalf("onGetDataMessage: %v", err)
	}

	var sawBatch bool
	var sawNotFound bool
	for i := 0; i < 2; i++ {
		envelope := <-peer.sendQ
		switch msg := envelope.msg.(type) {
		case p2p.TxBatchMessage:
			if len(msg.Txs) != 1 || consensus.TxID(&msg.Txs[0]) != consensus.TxID(&tx) {
				t.Fatalf("unexpected tx batch payload")
			}
			sawBatch = true
		case p2p.NotFoundMessage:
			if len(msg.Items) != 1 || msg.Items[0].Hash != missing {
				t.Fatalf("unexpected notfound payload: %+v", msg.Items)
			}
			sawNotFound = true
		default:
			t.Fatalf("unexpected message type %T", envelope.msg)
		}
	}
	if !sawBatch || !sawNotFound {
		t.Fatalf("expected both tx batch and notfound, sawBatch=%t sawNotFound=%t", sawBatch, sawNotFound)
	}
}

type deadlineSpyConn struct {
	mu                  sync.Mutex
	sawNonZeroWriteTime bool
}

func (c *deadlineSpyConn) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (c *deadlineSpyConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *deadlineSpyConn) Close() error                { return nil }
func (c *deadlineSpyConn) LocalAddr() net.Addr         { return deadlineSpyAddr("local") }
func (c *deadlineSpyConn) RemoteAddr() net.Addr        { return deadlineSpyAddr("remote") }
func (c *deadlineSpyConn) SetDeadline(time.Time) error { return nil }
func (c *deadlineSpyConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *deadlineSpyConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !deadline.IsZero() {
		c.sawNonZeroWriteTime = true
	}
	return nil
}

func (c *deadlineSpyConn) sawNonZeroWriteDeadline() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sawNonZeroWriteTime
}

type deadlineSpyAddr string

func (a deadlineSpyAddr) Network() string { return "tcp" }
func (a deadlineSpyAddr) String() string  { return string(a) }

func TestRenderBlockFlowMarksTip(t *testing.T) {
	blocks := []dashboardBlockPage{
		{Height: 10, Hash: [32]byte{0xaa}},
		{Height: 11, Hash: [32]byte{0xbb}},
	}
	out := renderBlockFlow(blocks)
	if !strings.Contains(out, "tip 11") {
		t.Fatalf("expected tip marker in block flow: %q", out)
	}
	if !strings.Contains(out, "height 10") {
		t.Fatalf("expected height label in block flow: %q", out)
	}
	if !strings.Contains(out, "...") {
		t.Fatalf("expected ellipsis-truncated hashes in block flow: %q", out)
	}
	if strings.Count(out, "--->") != 1 {
		t.Fatalf("expected single arrows in block flow: %q", out)
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected four block flow rows, got %d: %q", len(lines), out)
	}
	if !strings.Contains(lines[2], "--->") {
		t.Fatalf("expected hash row to include arrows: %q", out)
	}
	tagPattern := regexp.MustCompile(`<[^>]+>`)
	plainLines := make([]string, 0, len(lines))
	for _, line := range lines {
		plainLines = append(plainLines, tagPattern.ReplaceAllString(line, ""))
	}
	for i := 1; i < len(plainLines); i++ {
		if len(plainLines[i]) != len(plainLines[0]) {
			t.Fatalf("expected aligned block flow rows, got %d vs %d: %q", len(plainLines[i]), len(plainLines[0]), out)
		}
	}
}

func TestDashboardSystemSummaryComputesWindowAverages(t *testing.T) {
	now := time.Now()
	stats := dashboardSystemStats{
		samples: []dashboardSystemSample{
			{
				takenAt:       now.Add(-9 * time.Minute),
				cpuBusyTicks:  100,
				cpuTotalTicks: 200,
				rxBytes:       1_000,
				txBytes:       2_000,
				memUsedBytes:  2 * 1024 * 1024 * 1024,
				memTotalBytes: 8 * 1024 * 1024 * 1024,
				load1:         0.8,
				load5:         0.6,
				load15:        0.4,
				runningProcs:  2,
				totalProcs:    120,
				cores:         4,
			},
			{
				takenAt:       now,
				cpuBusyTicks:  180,
				cpuTotalTicks: 300,
				rxBytes:       61_000,
				txBytes:       122_000,
				memUsedBytes:  4 * 1024 * 1024 * 1024,
				memTotalBytes: 8 * 1024 * 1024 * 1024,
				load1:         1.2,
				load5:         0.9,
				load15:        0.7,
				runningProcs:  3,
				totalProcs:    128,
				cores:         4,
			},
		},
	}

	summary := stats.summary(now, 10*time.Minute)
	if !summary.HasCPU {
		t.Fatal("expected cpu summary")
	}
	if summary.CPUPercent < 79.9 || summary.CPUPercent > 80.1 {
		t.Fatalf("cpu percent = %.2f, want about 80", summary.CPUPercent)
	}
	if !summary.HasNetwork {
		t.Fatal("expected network summary")
	}
	if summary.RxBytesPerSec < 110 || summary.RxBytesPerSec > 112 {
		t.Fatalf("rx bytes/sec = %.2f, want about 111.11", summary.RxBytesPerSec)
	}
	if !summary.HasMemory {
		t.Fatal("expected memory summary")
	}
	wantMem := uint64(3 * 1024 * 1024 * 1024)
	if summary.AvgMemUsedBytes != wantMem {
		t.Fatalf("avg mem = %d, want %d", summary.AvgMemUsedBytes, wantMem)
	}
	if summary.RunningProcs != 3 || summary.TotalProcs != 128 || summary.Cores != 4 {
		t.Fatalf("unexpected process/core summary: %+v", summary)
	}
}

func TestDashboardSystemSummaryRequiresTenSecondWindowForCPUAndNetwork(t *testing.T) {
	now := time.Now()
	stats := dashboardSystemStats{
		samples: []dashboardSystemSample{
			{
				takenAt:       now.Add(-9 * time.Second),
				cpuBusyTicks:  100,
				cpuTotalTicks: 200,
				rxBytes:       1_000,
				txBytes:       2_000,
				memUsedBytes:  2 * 1024 * 1024,
				memTotalBytes: 8 * 1024 * 1024,
				load1:         0.5,
				load5:         0.4,
				load15:        0.3,
				runningProcs:  2,
				totalProcs:    100,
				cores:         2,
			},
			{
				takenAt:       now,
				cpuBusyTicks:  150,
				cpuTotalTicks: 260,
				rxBytes:       5_000,
				txBytes:       8_000,
				memUsedBytes:  3 * 1024 * 1024,
				memTotalBytes: 8 * 1024 * 1024,
				load1:         0.6,
				load5:         0.5,
				load15:        0.4,
				runningProcs:  3,
				totalProcs:    110,
				cores:         2,
			},
		},
	}

	summary := stats.summary(now, dashboardSystemWindow)
	if summary.HasCPU {
		t.Fatalf("expected cpu summary to stay warming up for windows under %s", dashboardSystemMinimumWindow)
	}
	if summary.HasNetwork {
		t.Fatalf("expected network summary to stay warming up for windows under %s", dashboardSystemMinimumWindow)
	}
}

func TestRenderDashboardSystemSectionIncludesHumanReadableStats(t *testing.T) {
	section := renderDashboardSystemSection(dashboardSystemSummary{
		Window:          10 * time.Minute,
		CPUPercent:      42.5,
		HasCPU:          true,
		RxBytesPerSec:   2 * 1024 * 1024,
		TxBytesPerSec:   512 * 1024,
		HasNetwork:      true,
		AvgMemUsedBytes: 3 * 1024 * 1024 * 1024,
		MemTotalBytes:   8 * 1024 * 1024 * 1024,
		HasMemory:       true,
		Load1:           0.42,
		Load5:           0.37,
		Load15:          0.31,
		HasLoad:         true,
		RunningProcs:    2,
		TotalProcs:      140,
		Cores:           4,
	})

	for _, want := range []string{
		"NODE SYSTEM (AVG 10M)",
		"CPU Avg      : 42.5%",
		"Network Up   : 512.0 KB/s",
		"Network Down : 2.0 MB/s",
		"42.5%",
		"2.0 MB/s",
		"512.0 KB/s",
		"3.0 GB / 8.0 GB (38%)",
		"Load Avg     : 0.42 / 0.37 / 0.31",
		"0.42 / 0.37 / 0.31",
		"Processes    : 2 run / 140 total",
		"CPU Cores    : 4 cores",
		"2 run / 140 total",
		"4 cores",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in section:\n%s", want, section)
		}
	}
}

func TestRenderMempoolSectionUsesQueuedFeeList(t *testing.T) {
	section := renderMempoolSection(dashboardMempoolSummary{
		Count:         256,
		Bytes:         2_190,
		Orphans:       3,
		MedianFee:     1_000,
		LowFee:        1_000,
		HighFee:       1_000,
		EstimatedNext: 10 * time.Minute,
		Top: []mempool.SnapshotEntry{{
			Tx:   types.Transaction{},
			TxID: [32]byte{0xaa},
			Fee:  219,
			Size: 219,
		}},
	})

	for _, want := range []string{
		"Tx Count                          : 256",
		"Total Size                        : 2,190 bytes",
		"Orphan Tx Count                   : 3",
		"Fee Min / Median / Max (atoms/kB) : 1,000 / 1,000 / 1,000",
		"top queued tx by fee",
		"aa00000...",
		"1,000 atoms/kB",
		"219 B",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in mempool section:\n%s", want, section)
		}
	}
	if strings.Contains(section, "Next Block") || strings.Contains(section, "Queue Top") {
		t.Fatalf("expected legacy mempool labels to be removed:\n%s", section)
	}
}

func TestRenderCandidateBlockSectionUsesDedicatedLabels(t *testing.T) {
	section := renderCandidateBlockSection(BlockTemplateStats{
		FrontierCandidates: 10,
		Rebuilds:           253,
		Interruptions:      157,
		Invalidations:      269,
		LastBuildAgeMS:     int((38 * time.Second).Milliseconds()),
		LastReason:         "interrupted",
	})

	for _, want := range []string{
		"Status",
		"rebuilding",
		"Age",
		"38s",
		"Tx Candidate Txs",
		"10",
		"Rebuilds (1h)",
		"253",
		"Interrupts (1h)",
		"157",
		"Invalidations (1h)",
		"269",
		"Last Reason",
		"interrupted",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in candidate block section:\n%s", want, section)
		}
	}
}

func TestRenderPerformanceSectionShowsCountersGaugesAndLatency(t *testing.T) {
	section := renderPerformanceSection(PerformanceMetrics{
		GeneratedAt: time.Unix(1_700_000_000, 0).UTC(),
		Counters: PerformanceCounters{
			AdmittedTxs:           128,
			OrphanPromotions:      7,
			RelayedTxItems:        256,
			RelayedBlockItems:     4,
			BlocksAccepted:        3,
			TemplateRebuilds:      5,
			TemplateInterruptions: 2,
			PeerStallEvents:       6,
		},
		Gauges: PerformanceGauges{
			MempoolTxs:          512,
			MempoolOrphans:      9,
			CandidateFrontier:   64,
			PeerCount:           8,
			UsefulPeers:         3,
			RelayQueueDepth:     14,
			RelayQueueDepthPeak: 29,
			PendingPeerBlocks:   2,
			InflightBlockReqs:   5,
			InflightTxReqs:      11,
		},
		Latency: PerformanceLatencyGroup{
			Admission:  DurationHistogramSummary{Count: 8, AvgMS: 12.5, P50MS: 11, P95MS: 19, MaxMS: 22},
			Template:   DurationHistogramSummary{Count: 5, AvgMS: 40, P50MS: 35, P95MS: 70, MaxMS: 72},
			BlockApply: DurationHistogramSummary{Count: 3, AvgMS: 55, P50MS: 54, P95MS: 61, MaxMS: 61},
			RelayFlush: DurationHistogramSummary{Count: 10, AvgMS: 2.5, P50MS: 2.2, P95MS: 4.8, MaxMS: 5.1},
			SyncReq:    DurationHistogramSummary{Count: 7, AvgMS: 7.5, P50MS: 7.0, P95MS: 11.2, MaxMS: 12.0},
		},
	})

	for _, want := range []string{
		"Generated    : 2023-11-14 22:13:20 UTC",
		"Throughput   : admitted tx",
		"Relay        : tx items",
		"Template     : rebuilds",
		"Peers        : connected",
		"Queues       : relay now",
		"Admission : count 8",
		"Template  : count 5",
		"Relay     : count 10",
		"Sync      : count 7",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in performance section:\n%s", want, section)
		}
	}
}

func TestRenderTPSChartUsesBlockThroughputLayout(t *testing.T) {
	section := renderTPSChart(dashboardTPSChart{
		Label: "THROUGHPUT (BY BLOCK)",
		Blocks: []dashboardThroughputBlock{
			{TxCount: 121, BarWidth: 12},
			{TxCount: 167, BarWidth: 17},
			{TxCount: 38, BarWidth: 4},
			{TxCount: 241, BarWidth: 24},
			{TxCount: 92, BarWidth: 9},
			{TxCount: 184, Candidate: true, BarWidth: 18},
		},
		TotalTx:     843,
		AvgTPS:      0.23,
		AvgTxPerBlk: 140.5,
	})

	for _, want := range []string{
		"121 tx  |",
		"167 tx  |",
		"38 tx  |",
		"241 tx  |",
		"92 tx  |",
		"184 tx* |",
		"843 tx total over last 6 blocks (0.23 avg TPS)",
		"140.5 tx avg/block",
		"* candidate block",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in throughput section:\n%s", want, section)
		}
	}
}

func TestDashboardFeeSummaryUsesRecent24Blocks(t *testing.T) {
	var svc Service
	blocks := make([]dashboardBlockPage, 0, 30)
	for i := 0; i < 30; i++ {
		blocks = append(blocks, dashboardBlockPage{
			Height:        uint64(300 - i),
			MedianFeeRate: uint64(i * 100),
			LowFeeRate:    uint64(i * 50),
			HighFeeRate:   uint64(i * 150),
			PaidTxs:       min(i, 2),
			TotalUserTxs:  3,
			TxCount:       2 + (i % 3),
		})
	}

	summary := svc.dashboardFeeSummary(blocks, 20, dashboardPowSummary{
		TargetSpacing:    10 * time.Minute,
		AvgBlockInterval: 10 * time.Minute,
	}, dashboardCandidateFeeLine{
		Height:    301,
		MedianFee: 900,
		LowFee:    100,
		HighFee:   1500,
		PaidTxs:   4,
		TotalTxs:  4,
		Available: true,
	}, []mempool.SnapshotEntry{
		{Fee: 2000, Size: 1_000},
		{Fee: 700, Size: 1_000},
		{Fee: 300, Size: 1_000},
		{Fee: 50, Size: 1_000},
		{Fee: 0, Size: 1_000},
	})

	if summary.TotalBlocks != 24 {
		t.Fatalf("expected 24 fee blocks, got %d", summary.TotalBlocks)
	}
	if summary.CurrentMedian != 900 {
		t.Fatalf("expected current median from candidate block, got %d", summary.CurrentMedian)
	}
	if summary.Median6 != 300 {
		t.Fatalf("expected 6-block median 300, got %d", summary.Median6)
	}
	if summary.Median24 != 1200 {
		t.Fatalf("expected 24-block median 1200, got %d", summary.Median24)
	}
	if summary.RecentMin != 0 || summary.RecentMax != 3450 {
		t.Fatalf("expected recent min/max 0/3450, got %d/%d", summary.RecentMin, summary.RecentMax)
	}
	if summary.PaidBlocks != 23 {
		t.Fatalf("expected 23 paid blocks, got %d", summary.PaidBlocks)
	}
	if summary.FeePayingTxs != 45 || summary.TotalTxs != 72 {
		t.Fatalf("expected fee-paying ratio inputs 45/72, got %d/%d", summary.FeePayingTxs, summary.TotalTxs)
	}
	if summary.Bands.Above1000 != 1 || summary.Bands.Band500 != 1 || summary.Bands.Band100 != 1 || summary.Bands.Band1 != 1 || summary.Bands.Zero != 1 {
		t.Fatalf("unexpected fee bands: %+v", summary.Bands)
	}
	if summary.Recent[0].Height != 277 || summary.Recent[len(summary.Recent)-1].Height != 300 {
		t.Fatalf("expected oldest-to-newest fee window, got first=%d last=%d", summary.Recent[0].Height, summary.Recent[len(summary.Recent)-1].Height)
	}
}

func TestRenderFeeSectionUsesFeeMarketLayout(t *testing.T) {
	recent := make([]dashboardBlockFeeLine, 0, 24)
	for i := 0; i < 24; i++ {
		recent = append(recent, dashboardBlockFeeLine{
			Height:    uint64(230 + i),
			MedianFee: uint64(i * 25),
			LowFee:    uint64(i * 10),
			HighFee:   uint64(i * 40),
		})
	}
	section := renderFeeSection(dashboardFeeSummary{
		Recent:        recent,
		CurrentMedian: 300,
		Median6:       200,
		PaidBlocks:    8,
		TotalBlocks:   24,
	})

	for _, want := range []string{
		"24-block medians",
		"current / 6-block median",
		"300 atoms/kB / 200 atoms/kB",
		"paid blocks",
		"8 / 24",
		"Full fee chart",
		"/fees",
		"▁",
		"█",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in fee market section:\n%s", want, section)
		}
	}
}

func TestRenderPublicDashboardPagesExposeRecentBlockAndTxLinks(t *testing.T) {
	blockHash := [32]byte{0xaa}
	txID := [32]byte{0xbb}
	generatedAt := time.Unix(1_000, 0).UTC()
	recentFees := make([]dashboardBlockFeeLine, 0, 24)
	for i := 0; i < 24; i++ {
		recentFees = append(recentFees, dashboardBlockFeeLine{
			Height:    uint64(100 + i),
			MedianFee: uint64(i * 10),
			LowFee:    uint64(i * 5),
			HighFee:   uint64(i * 15),
			PaidTxs:   i,
			TotalTxs:  i + 1,
		})
	}
	view := &publicDashboardView{
		generatedAt: generatedAt,
		nodeID:      "NODE1234",
		health:      "HEALTHY",
		info:        ServiceInfo{TipHeight: 12, TipHeaderHash: hex.EncodeToString(blockHash[:]), UTXORoot: strings.Repeat("c", 64)},
		pow: dashboardPowSummary{
			Algorithm:          "ASERT per-block",
			TargetSpacing:      10 * time.Minute,
			AvgBlockInterval:   10 * time.Minute,
			RecentBlockGap:     10 * time.Minute,
			HasObservedGap:     true,
			LastBlockTimestamp: generatedAt.Add(-38 * time.Second),
			NetworkHashrate:    12.4e12,
		},
		fees: dashboardFeeSummary{
			Recent:        recentFees,
			CurrentMedian: 300,
			Median6:       50,
			Median24:      120,
			RecentMin:     0,
			RecentMax:     345,
			PaidBlocks:    23,
			TotalBlocks:   24,
			FeePayingTxs:  100,
			TotalTxs:      150,
			Candidate: dashboardCandidateFeeLine{
				Height:    13,
				MedianFee: 300,
				LowFee:    100,
				HighFee:   600,
				PaidTxs:   14,
				TotalTxs:  14,
				Available: true,
			},
			Bands: dashboardMempoolFeeBands{
				Above1000: 2,
				Band500:   11,
				Band100:   39,
				Band1:     22,
				Zero:      8,
			},
			Clear: dashboardMempoolClearEstimate{Blocks: 1, Time: 10 * time.Minute},
		},
		mempool: dashboardMempoolSummary{Count: 10},
		performance: PerformanceMetrics{
			Gauges: PerformanceGauges{PendingPeerBlocks: 0},
		},
		tpsChart: dashboardTPSChart{
			Label: "THROUGHPUT (BY BLOCK)",
			Blocks: []dashboardThroughputBlock{
				{TxCount: 121, BarWidth: 12},
				{TxCount: 167, BarWidth: 17},
				{TxCount: 38, BarWidth: 4},
				{TxCount: 241, BarWidth: 24},
				{TxCount: 92, BarWidth: 9},
				{TxCount: 184, Candidate: true, BarWidth: 18},
			},
			TotalTx:     843,
			AvgTPS:      0.23,
			AvgTxPerBlk: 140.5,
		},
		blocks: []dashboardBlockPage{{
			Height:    12,
			Hash:      blockHash,
			Timestamp: time.Unix(100, 0).UTC(),
			PreviewTxs: []dashboardTxPage{{
				TxID:      txID,
				BlockHash: blockHash,
				Timestamp: time.Unix(100, 0).UTC(),
			}},
		}},
		peerHosts: []dashboardPeerHostPage{{
			Host:           "151.115.80.188",
			Health:         "MIXED",
			Direction:      "in/out",
			Sockets:        2,
			LastSeenUnix:   time.Now().Add(-time.Second).Unix(),
			BlocksBehind:   0,
			BestHeight:     12,
			TipHash:        "0000000d1b...",
			LatencyAvgS:    0.05,
			LatencyP95S:    0.09,
			TxSent:         5,
			TxRequested:    2,
			BlockSent:      0,
			BlockRequested: 1,
			BytesIn:        184 * 1024,
			BytesOut:       231 * 1024,
			Reason:         "healthy and current",
		}},
	}

	home, status := renderPublicDashboardPage(view, "/")
	if status != 200 || !strings.Contains(home, "/block/"+hex.EncodeToString(blockHash[:])) {
		t.Fatalf("home page missing block link:\n%s", home)
	}
	for _, want := range []string{
		"Health",
		"HEALTHY",
		"Tip Height",
		"12",
		"Last Block",
		"38s ago",
		"Network Hashrate",
		"12.4 TH/s",
		"Peers",
		"Mempool",
		"10 tx",
		"Current Orphan Blocks",
		"Tx Relay Mode",
		"erlay reconciliation",
		"Block Relay Mode",
		"graphene planner",
	} {
		if !strings.Contains(home, want) {
			t.Fatalf("home page missing %q:\n%s", want, home)
		}
	}
	if !strings.Contains(home, "/peer/151.115.80.188") {
		t.Fatalf("home page missing peer link:\n%s", home)
	}
	if !strings.Contains(home, "/fees") || !strings.Contains(home, "FEE MARKET") {
		t.Fatalf("home page missing fee market link:\n%s", home)
	}
	blockPage, status := renderPublicDashboardPage(view, "/block/"+hex.EncodeToString(blockHash[:]))
	if status != 200 || !strings.Contains(blockPage, "/tx/"+hex.EncodeToString(txID[:])) {
		t.Fatalf("block page missing tx link:\n%s", blockPage)
	}
	txPage, status := renderPublicDashboardPage(view, "/tx/"+hex.EncodeToString(txID[:]))
	if status != 200 || !strings.Contains(txPage, "TRANSACTION") {
		t.Fatalf("tx page missing transaction section:\n%s", txPage)
	}
	peerPage, status := renderPublicDashboardPage(view, "/peer/151.115.80.188")
	if status != 200 || !strings.Contains(peerPage, "PEER DETAIL") || !strings.Contains(peerPage, "bytes in") {
		t.Fatalf("peer page missing detail section:\n%s", peerPage)
	}
	feePage, status := renderPublicDashboardPage(view, "/fees")
	if status != 200 || !strings.Contains(feePage, "FEE MARKET DETAILS") || !strings.Contains(feePage, "mempool by fee band") || !strings.Contains(feePage, "candidate block fee") {
		t.Fatalf("fee page missing detail section:\n%s", feePage)
	}
}

func TestSummarizeDashboardPeerHostUsesHostLevelHealthAndTraffic(t *testing.T) {
	now := time.Unix(10_000, 0)
	info := ServiceInfo{TipHeight: 253, TipHeaderHash: strings.Repeat("a", 64)}
	host := summarizeDashboardPeerHost(now, "151.115.80.188", info, []dashboardPeerSocketPage{
		{
			Addr:         "151.115.80.188:18444",
			Outbound:     true,
			Height:       253,
			Lag:          0,
			LastSeenUnix: now.Unix(),
			SessionAge:   102 * time.Minute,
			LatencyAvgS:  0.05,
			LatencyP95S:  0.07,
			TxSent:       5,
			TxRequested:  3,
			BytesIn:      80 * 1024,
			BytesOut:     90 * 1024,
		},
		{
			Addr:           "151.115.80.188:49346",
			Outbound:       false,
			Height:         2,
			Lag:            251,
			LastSeenUnix:   now.Unix() - 1,
			SessionAge:     100 * time.Minute,
			LatencyAvgS:    0.04,
			LatencyP95S:    0.09,
			TxSent:         6,
			TxRequested:    2,
			BlockRequested: 1,
			BytesIn:        104 * 1024,
			BytesOut:       141 * 1024,
		},
	})

	if host.Health != "MIXED" {
		t.Fatalf("health = %q, want MIXED", host.Health)
	}
	if host.Direction != "in/out" {
		t.Fatalf("direction = %q, want in/out", host.Direction)
	}
	if host.BlocksBehind != 0 {
		t.Fatalf("blocks behind = %d, want 0", host.BlocksBehind)
	}
	if host.TxSent != 5 || host.TxRequested != 2 {
		t.Fatalf("traffic summary = sent %d requested %d, want 5/2", host.TxSent, host.TxRequested)
	}
	if host.LatencyAvgS < 0.044 || host.LatencyAvgS > 0.046 {
		t.Fatalf("latency avg = %.3f, want about 0.045", host.LatencyAvgS)
	}
	if host.LatencyP95S != 0.09 {
		t.Fatalf("latency p95 = %.2f, want 0.09", host.LatencyP95S)
	}
	if host.BytesIn != 184*1024 || host.BytesOut != 231*1024 {
		t.Fatalf("bytes summary = %d/%d, want %d/%d", host.BytesIn, host.BytesOut, 184*1024, 231*1024)
	}
}

func TestPeerInfoUsesObservedHeight(t *testing.T) {
	svc := &Service{
		peers:      make(map[string]*peerConn),
		knownPeers: make(map[string]storage.KnownPeerRecord),
	}
	peer := &peerConn{
		addr:           "127.0.0.1:18444",
		outbound:       true,
		connectedAt:    time.Now().Add(-2 * time.Minute),
		version:        p2p.VersionMessage{Height: 7, UserAgent: "bpu/go"},
		controlQ:       make(chan outboundMessage, 1),
		relayPriorityQ: make(chan outboundMessage, 1),
		sendQ:          make(chan outboundMessage, 1),
		localRelayTxs:  make(map[[32]byte]localRelayFallbackState),
	}
	peer.noteProgress(time.Unix(100, 0))
	peer.controlQ <- outboundMessage{}
	peer.relayPriorityQ <- outboundMessage{}
	peer.sendQ <- outboundMessage{}
	peer.localRelayTxs[[32]byte{1}] = localRelayFallbackState{announcedAt: time.Unix(101, 0)}
	peer.noteUsefulTxs(1, time.Now())
	peer.telemetry.noteTxRequestReceived(2)
	peer.telemetry.noteTxNotFoundReceived(1)
	peer.telemetry.noteKnownTxClears(3)
	svc.peers[peer.addr] = peer

	info := svc.PeerInfo()[0]
	if got := info.Height; got != 7 {
		t.Fatalf("peer height = %d, want handshake height 7", got)
	}
	if info.RelayQueueDepth != 3 || info.ControlQueueDepth != 1 || info.PriorityQueueDepth != 1 || info.SendQueueDepth != 1 {
		t.Fatalf("unexpected relay queue snapshot: %+v", info)
	}
	if info.PendingLocalRelayTxs != 1 || info.TxReqRecvItems != 2 || info.TxNotFoundReceived != 1 || info.KnownTxClears != 3 {
		t.Fatalf("unexpected peer relay details: %+v", info)
	}
	if info.UsefulnessClass == "" || info.UsefulnessScore == 0 {
		t.Fatalf("expected usefulness fields in peer info, got %+v", info)
	}

	peer.noteHeight(11)
	if got := svc.PeerInfo()[0].Height; got != 11 {
		t.Fatalf("peer height = %d, want observed height 11", got)
	}
}

func TestPeerInfoReportsProtectionClass(t *testing.T) {
	svc := &Service{
		peers: make(map[string]*peerConn),
		knownPeers: map[string]storage.KnownPeerRecord{
			"127.0.0.1:18444": {Manual: true},
		},
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.outbound = true
	peer.connectedAt = time.Now().Add(-2 * time.Minute)
	peer.noteUsefulBlocks(1, time.Now())
	svc.peers[peer.addr] = peer

	info := svc.PeerInfo()[0]
	if !info.Manual || !info.Protected {
		t.Fatalf("expected manual peer to be protected, got %+v", info)
	}
	if info.ProtectedClass != "manual" || info.UsefulnessClass != "manual" {
		t.Fatalf("unexpected protection classification: %+v", info)
	}
}

func TestOnPeerHeadersUpdatesObservedPeerHeight(t *testing.T) {
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
	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, state.UTXOs(), 4, first.Header.Timestamp+600)

	peer := &peerConn{
		addr:        "127.0.0.1:18444",
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		version:     p2p.VersionMessage{Height: 0, UserAgent: "bpu/go"},
	}

	if err := svc.onPeerMessage(peer, p2p.HeadersMessage{Headers: []types.BlockHeader{first.Header, second.Header}}); err != nil {
		t.Fatalf("onPeerMessage headers: %v", err)
	}
	if got := peer.snapshotHeight(); got != 2 {
		t.Fatalf("observed peer height = %d, want 2", got)
	}
}

func TestOnPeerHeadersAcceptsCompetingBranchFromKnownParent(t *testing.T) {
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
	mainFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if err := svc.onPeerMessage(newPeerConnForTests("127.0.0.1:18445"), p2p.HeadersMessage{Headers: []types.BlockHeader{mainFirst.Header}}); err != nil {
		t.Fatalf("seed main header: %v", err)
	}

	altFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 4, genesis.Header.Timestamp+601)
	altState := baseState.Clone()
	if _, err := altState.ApplyBlock(&altFirst); err != nil {
		t.Fatal(err)
	}
	altSecond := nextCoinbaseBlock(1, altFirst.Header, altState.UTXOs(), 5, altFirst.Header.Timestamp+600)

	peer := newPeerConnForTests("127.0.0.1:18446")
	if err := svc.onPeerMessage(peer, p2p.HeadersMessage{Headers: []types.BlockHeader{altFirst.Header, altSecond.Header}}); err != nil {
		t.Fatalf("competing headers: %v", err)
	}
	if got := peer.snapshotHeight(); got != 2 {
		t.Fatalf("observed peer height = %d, want 2", got)
	}
	if got := svc.HeaderHeight(); got != 2 {
		t.Fatalf("header height = %d, want 2", got)
	}
	if got := consensus.HeaderHash(svc.headerChain.TipHeader()); got != consensus.HeaderHash(&altSecond.Header) {
		t.Fatalf("header tip = %x, want %x", got, consensus.HeaderHash(&altSecond.Header))
	}
}

func TestApplyPeerHeadersInactiveBranchDoesNotRewriteActiveLocatorBase(t *testing.T) {
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
	mainFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if err := svc.onPeerMessage(newPeerConnForTests("127.0.0.1:18445"), p2p.HeadersMessage{Headers: []types.BlockHeader{mainFirst.Header}}); err != nil {
		t.Fatalf("seed main header: %v", err)
	}

	altFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 4, genesis.Header.Timestamp+601)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{altFirst.Header}); err != nil {
		t.Fatalf("apply competing header: %v", err)
	}

	svc.stateMu.RLock()
	height, err := svc.findLocatorHeightLocked([][32]byte{consensus.HeaderHash(&altFirst.Header), consensus.HeaderHash(&genesis.Header)})
	svc.stateMu.RUnlock()
	if err != nil {
		t.Fatalf("findLocatorHeightLocked: %v", err)
	}
	if height != 0 {
		t.Fatalf("locator height = %d, want 0 because competing header is not active", height)
	}

	hashAtHeight, err := svc.chainState.Store().GetHeaderHashByHeight(1)
	if err != nil {
		t.Fatalf("GetHeaderHashByHeight: %v", err)
	}
	mainHash := consensus.HeaderHash(&mainFirst.Header)
	if hashAtHeight == nil || *hashAtHeight != mainHash {
		t.Fatalf("active height hash = %x, want %x", hashAtHeight, mainHash)
	}
}

func TestOpenServiceRestoresPromotedHigherWorkHeaderBranch(t *testing.T) {
	path := t.TempDir()
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}

	baseState := NewChainState(types.Regtest)
	if _, err := baseState.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	mainFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{mainFirst.Header}); err != nil {
		t.Fatalf("apply main header: %v", err)
	}

	altFirst := nextCoinbaseBlock(0, genesis.Header, baseState.UTXOs(), 4, genesis.Header.Timestamp+601)
	altState := baseState.Clone()
	if _, err := altState.ApplyBlock(&altFirst); err != nil {
		t.Fatal(err)
	}
	altSecond := nextCoinbaseBlock(1, altFirst.Header, altState.UTXOs(), 5, altFirst.Header.Timestamp+600)
	if _, err := svc.applyPeerHeaders([]types.BlockHeader{altFirst.Header, altSecond.Header}); err != nil {
		t.Fatalf("apply competing headers: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  path,
	}, &genesis)
	if err != nil {
		t.Fatalf("reopen service: %v", err)
	}
	defer reopened.Close()

	if got := reopened.HeaderHeight(); got != 2 {
		t.Fatalf("reopened header height = %d, want 2", got)
	}
	if got := consensus.HeaderHash(reopened.headerChain.TipHeader()); got != consensus.HeaderHash(&altSecond.Header) {
		t.Fatalf("reopened header tip = %x, want %x", got, consensus.HeaderHash(&altSecond.Header))
	}

	altFirstHash := consensus.HeaderHash(&altFirst.Header)
	hashAtHeight, err := reopened.chainState.Store().GetHeaderHashByHeight(1)
	if err != nil {
		t.Fatalf("GetHeaderHashByHeight(1): %v", err)
	}
	if hashAtHeight == nil || *hashAtHeight != altFirstHash {
		t.Fatalf("active header hash at height 1 = %x, want %x", hashAtHeight, altFirstHash)
	}

	altSecondHash := consensus.HeaderHash(&altSecond.Header)
	hashAtHeight, err = reopened.chainState.Store().GetHeaderHashByHeight(2)
	if err != nil {
		t.Fatalf("GetHeaderHashByHeight(2): %v", err)
	}
	if hashAtHeight == nil || *hashAtHeight != altSecondHash {
		t.Fatalf("active header hash at height 2 = %x, want %x", hashAtHeight, altSecondHash)
	}
}

func TestOnPeerTxBatchIgnoresDuplicateAdmissionError(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	if err := svc.onPeerMessage(peer, p2p.TxBatchMessage{Txs: []types.Transaction{tx}}); err != nil {
		t.Fatalf("onPeerMessage duplicate batch: %v", err)
	}
}

func TestSubmitTxTracksAndRebroadcastsLocalOriginTransactions(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
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

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	svc.rebroadcastMu.Lock()
	if _, ok := svc.localRebroadcast[txid]; !ok {
		svc.rebroadcastMu.Unlock()
		t.Fatalf("local rebroadcast set missing %x", txid)
	}
	svc.rebroadcastMu.Unlock()

	select {
	case <-peer.sendQ:
	default:
	}

	svc.rebroadcastLocalTxs()

	select {
	case envelope := <-peer.sendQ:
		recon, ok := envelope.msg.(p2p.TxReconMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
		}
		if len(recon.TxIDs) != 1 || recon.TxIDs[0] != txid {
			t.Fatalf("rebroadcast txids = %x, want [%x]", recon.TxIDs, txid)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for local rebroadcast")
	}
}

func TestRebroadcastLocalTxsRetriesPeersMarkedKnown(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
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

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	select {
	case <-peer.sendQ:
	default:
	}

	peer.noteKnownTxIDs([][32]byte{txid})
	svc.rebroadcastLocalTxs()

	select {
	case envelope := <-peer.sendQ:
		recon, ok := envelope.msg.(p2p.TxReconMessage)
		if !ok {
			t.Fatalf("message type = %T, want TxReconMessage", envelope.msg)
		}
		if len(recon.TxIDs) != 1 || recon.TxIDs[0] != txid {
			t.Fatalf("rebroadcast txids = %x, want [%x]", recon.TxIDs, txid)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for retry rebroadcast")
	}
}

func TestLocalVersionAdvertisesErlayAndGrapheneServices(t *testing.T) {
	svc := &Service{}
	version := svc.localVersion()
	for _, want := range []uint64{
		p2p.ServiceNodeNetwork,
		p2p.ServiceErlayTxRelay,
		p2p.ServiceGrapheneBlockRelay,
		p2p.ServiceGrapheneExtended,
	} {
		if version.Services&want == 0 {
			t.Fatalf("services bitmap %b missing capability %b", version.Services, want)
		}
	}
}

func TestSelectBlockRelayPlanChoosesExtendedForPoorOverlap(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")
	block := types.Block{
		Header: types.BlockHeader{Version: 1, Timestamp: 203},
		Txs:    []types.Transaction{coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})},
	}
	for i := 0; i < 16; i++ {
		block.Txs = append(block.Txs, types.Transaction{
			Base: types.TxBase{
				Version: 1,
				Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(i + 41)}, Vout: 0}}},
				Outputs: []types.TxOutput{{ValueAtoms: uint64(i + 1)}},
			},
		})
	}
	if plan := selectBlockRelayPlan(peer, block); plan != blockRelayPlanGrapheneExtended {
		t.Fatalf("relay plan = %d, want extended", plan)
	}
	for _, tx := range block.Txs[1:] {
		peer.noteKnownTxIDs([][32]byte{consensus.TxID(&tx)})
	}
	if plan := selectBlockRelayPlan(peer, block); plan != blockRelayPlanGrapheneP1 {
		t.Fatalf("relay plan = %d, want protocol 1", plan)
	}
}

func TestPeerOriginTransactionsAreNotTrackedForLocalRebroadcast(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	peer := newPeerConnForTests("127.0.0.1:18444")
	admissions, errs, _, _ := svc.submitDecodedTxsFrom([]types.Transaction{tx}, peer)
	if errs[0] != nil {
		t.Fatalf("submitDecodedTxsFrom: %v", errs[0])
	}
	if admissions[0].Orphaned {
		t.Fatal("peer-origin tx unexpectedly orphaned")
	}

	svc.rebroadcastMu.Lock()
	defer svc.rebroadcastMu.Unlock()
	if len(svc.localRebroadcast) != 0 {
		t.Fatalf("local rebroadcast set size = %d, want 0", len(svc.localRebroadcast))
	}
}

func TestApplyPeerBlockRemovesConfirmedLocalRebroadcastTransactions(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	prevOut := types.OutPoint{TxID: consensus.TxID(&genesis.Txs[0]), Vout: 0}
	tx := spendTxForNodeTest(t, 7, prevOut, 50, 8, 1)
	txid := consensus.TxID(&tx)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	block := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 9, genesis.Header.Timestamp+600)
	block.Txs = append(block.Txs, tx)
	block.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{
		consensus.TxID(&block.Txs[0]),
		txid,
	})
	block.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{
		consensus.AuthID(&block.Txs[0]),
		consensus.AuthID(&tx),
	})
	utxos := make(consensus.UtxoSet, len(state.UTXOs()))
	for outPoint, entry := range state.UTXOs() {
		utxos[outPoint] = entry
	}
	delete(utxos, prevOut)
	utxos[types.OutPoint{TxID: txid, Vout: 0}] = consensus.UtxoEntry{
		ValueAtoms: tx.Base.Outputs[0].ValueAtoms,
		KeyHash:    tx.Base.Outputs[0].KeyHash,
	}
	coinbaseTxID := consensus.TxID(&block.Txs[0])
	utxos[types.OutPoint{TxID: coinbaseTxID, Vout: 0}] = consensus.UtxoEntry{
		ValueAtoms: block.Txs[0].Base.Outputs[0].ValueAtoms,
		KeyHash:    block.Txs[0].Base.Outputs[0].KeyHash,
	}
	block.Header.UTXORoot = consensus.ComputedUTXORoot(utxos)
	block.Header = mineHeaderForNodeTest(block.Header)

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if applied, _, err := svc.applyPeerBlock(&block); err != nil || !applied {
		t.Fatalf("applyPeerBlock = (%v, %v), want (true, nil)", applied, err)
	}

	svc.rebroadcastMu.Lock()
	defer svc.rebroadcastMu.Unlock()
	if _, ok := svc.localRebroadcast[txid]; ok {
		t.Fatalf("confirmed tx %x still tracked for rebroadcast", txid)
	}
}

func TestApplyPeerBlockPromotesReadyOrphans(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	genesisTxID := consensus.TxID(&genesis.Txs[0])
	chainUTXOs := consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {
			ValueAtoms: 50,
			KeyHash:    nodeSignerKeyHash(7),
		},
	}
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, KeyHash: nodeSignerKeyHash(9)}})
	block := blockWithTxsForNodeTest(t, 0, genesis.Header, chainUTXOs, []types.Transaction{coinbase}, genesis.Header.Timestamp+1)
	orphanTx := spendTxForNodeTest(t, 9, types.OutPoint{TxID: consensus.TxID(&coinbase), Vout: 0}, 1, 8, 0)
	peer := newPeerConnForTests("127.0.0.1:18444")

	if err := svc.onPeerMessage(peer, p2p.TxBatchMessage{Txs: []types.Transaction{orphanTx}}); err != nil {
		t.Fatalf("onPeerMessage orphan batch: %v", err)
	}
	if got := svc.pool.Count(); got != 0 {
		t.Fatalf("mempool count before parent block = %d, want 0", got)
	}
	if got := svc.pool.OrphanCount(); got != 1 {
		t.Fatalf("orphan count before parent block = %d, want 1", got)
	}

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if err := svc.acceptPeerBlockMessage(peer, &block); err != nil {
		t.Fatalf("acceptPeerBlockMessage: %v", err)
	}
	if got := svc.pool.Count(); got != 1 {
		t.Fatalf("mempool count after parent block = %d, want 1", got)
	}
	if got := svc.pool.OrphanCount(); got != 0 {
		t.Fatalf("orphan count after parent block = %d, want 0", got)
	}
	if !svc.pool.Contains(consensus.TxID(&orphanTx)) {
		t.Fatal("promoted orphan tx missing from mempool")
	}
}

func TestApplyPeerBlockDoesNotRequireServiceStateMu(t *testing.T) {
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

	done := make(chan error, 1)
	svc.stateMu.RLock()
	go func() {
		applied, _, err := svc.applyPeerBlock(&block)
		if err != nil {
			done <- err
			return
		}
		if !applied {
			done <- fmt.Errorf("peer block did not become active")
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		svc.stateMu.RUnlock()
		if err != nil {
			t.Fatalf("applyPeerBlock under stateMu reader: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		svc.stateMu.RUnlock()
		t.Fatal("applyPeerBlock blocked on service stateMu")
	}
}

func TestOnPeerBlockMessageIgnoresKnownBlock(t *testing.T) {
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
	if applied, _, err := svc.applyPeerBlock(&block); err != nil || !applied {
		t.Fatalf("applyPeerBlock = (%v, %v), want (true, nil)", applied, err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 4)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: block}); err != nil {
		t.Fatalf("onPeerMessage known block: %v", err)
	}
}

func TestOnPeerBlockMessageRequestsHeadersWhenParentUnknown(t *testing.T) {
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

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.controlQ = make(chan outboundMessage, 4)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: block}); err != nil {
		t.Fatalf("onPeerMessage child without header: %v", err)
	}

	select {
	case envelope := <-peer.controlQ:
		req, ok := envelope.msg.(p2p.GetHeadersMessage)
		if !ok {
			t.Fatalf("message type = %T, want GetHeadersMessage", envelope.msg)
		}
		if req.StopHash != consensus.HeaderHash(&block.Header) {
			t.Fatalf("stop hash = %x, want %x", req.StopHash, consensus.HeaderHash(&block.Header))
		}
	default:
		t.Fatal("expected catch-up headers request")
	}
}

func TestOnPeerBlockMessageRequestsCatchUpForUnavailableParentState(t *testing.T) {
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
	peer.controlQ = make(chan outboundMessage, 8)
	if err := svc.onPeerMessage(peer, p2p.BlockMessage{Block: second}); err != nil {
		t.Fatalf("onPeerMessage child before parent block: %v", err)
	}

	var sawHeaders bool
	var sawGetData bool
	for i := 0; i < 2; i++ {
		select {
		case envelope := <-peer.controlQ:
			switch msg := envelope.msg.(type) {
			case p2p.GetHeadersMessage:
				sawHeaders = true
				if msg.StopHash != consensus.HeaderHash(&second.Header) {
					t.Fatalf("stop hash = %x, want %x", msg.StopHash, consensus.HeaderHash(&second.Header))
				}
			case p2p.GetDataMessage:
				sawGetData = true
				if len(msg.Items) == 0 {
					t.Fatal("expected missing block requests")
				}
			default:
				t.Fatalf("unexpected message type %T", envelope.msg)
			}
		default:
		}
	}
	if !sawHeaders {
		t.Fatal("expected catch-up GetHeaders request")
	}
	if !sawGetData {
		t.Fatal("expected catch-up GetData request")
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
	if len(hashes) != 0 {
		t.Fatalf("hashes before repair = %d, want 0", len(hashes))
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
	if _, _, err := svc.applyPeerBlock(&active1); err != nil {
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
		requestedAt: time.Now().Add(-11 * time.Second),
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

func newPeerConnForTests(addr string) *peerConn {
	return &peerConn{
		addr:          addr,
		sendQ:         make(chan outboundMessage, 4),
		closed:        make(chan struct{}),
		queuedInv:     make(map[p2p.InvVector]int),
		queuedTx:      make(map[[32]byte]int),
		knownTx:       make(map[[32]byte]struct{}),
		localRelayTxs: make(map[[32]byte]localRelayFallbackState),
		pendingThin:   make(map[[32]byte]*pendingThinBlock),
		version: p2p.VersionMessage{
			Height:    0,
			Services:  p2p.ServiceNodeNetwork | p2p.ServiceErlayTxRelay | p2p.ServiceGrapheneBlockRelay | p2p.ServiceGrapheneExtended | p2p.ServiceAvalancheOverlay,
			UserAgent: "bpu/go",
		},
	}
}

func TestPeerConnCoalescesTxBatches(t *testing.T) {
	peer := &peerConn{
		sendQ:    make(chan outboundMessage, 8),
		closed:   make(chan struct{}),
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}
	first := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})
	second := coinbaseTxForHeight(2, []types.TxOutput{{ValueAtoms: 2}})

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

func TestAvalanchePollRespondsWithPreferredTx(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	preferred := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	admissions, errs, _, _ := svc.submitDecodedTxs([]types.Transaction{preferred})
	if errs[0] != nil || len(admissions[0].Accepted) != 1 {
		t.Fatalf("submit preferred = (%+v, %v), want accepted", admissions[0], errs[0])
	}
	conflict := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 9, 1)
	_, errs, _, _ = svc.submitDecodedTxs([]types.Transaction{conflict})
	if !errors.Is(errs[0], mempool.ErrInputAlreadySpent) {
		t.Fatalf("conflict err = %v, want ErrInputAlreadySpent", errs[0])
	}

	peer := newPeerConnForTests("127.0.0.1:20001")
	if err := svc.onPeerMessage(peer, p2p.AvaPollMessage{
		PollID: 9,
		Items:  []types.OutPoint{{TxID: genesisTxID, Vout: 0}},
	}); err != nil {
		t.Fatalf("onPeerMessage poll: %v", err)
	}

	select {
	case envelope := <-peer.sendQ:
		vote, ok := envelope.msg.(p2p.AvaVoteMessage)
		if !ok {
			t.Fatalf("message type = %T, want AvaVoteMessage", envelope.msg)
		}
		if vote.PollID != 9 || len(vote.Votes) != 1 || !vote.Votes[0].HasOpinion {
			t.Fatalf("unexpected vote payload: %+v", vote)
		}
		if want := consensus.TxID(&preferred); vote.Votes[0].TxID != want {
			t.Fatalf("vote txid = %x, want %x", vote.Votes[0].TxID, want)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for avalanche vote")
	}
}

func TestAvalancheFinalizesAndRejectsConflicts(t *testing.T) {
	genesis := genesisBlockForKeyHash(nodeSignerKeyHash(7))
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	svc, err := OpenService(ServiceConfig{
		Profile:                   types.Regtest,
		DBPath:                    t.TempDir(),
		AvalancheKSample:          3,
		AvalancheBeta:             2,
		AvalanchePollInterval:     50 * time.Millisecond,
		AvalancheAlphaNumerator:   1,
		AvalancheAlphaDenominator: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	preferred := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	conflict := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 9, 1)
	if _, err := svc.SubmitTx(preferred); err != nil {
		t.Fatalf("SubmitTx preferred: %v", err)
	}
	_, errs, _, _ := svc.submitDecodedTxs([]types.Transaction{conflict})
	if !errors.Is(errs[0], mempool.ErrInputAlreadySpent) {
		t.Fatalf("conflict err = %v, want ErrInputAlreadySpent", errs[0])
	}

	peers := []*peerConn{
		newPeerConnForTests("127.0.0.1:21001"),
		newPeerConnForTests("127.0.0.1:21002"),
		newPeerConnForTests("127.0.0.1:21003"),
	}
	svc.peerMu.Lock()
	for _, peer := range peers {
		peer.svc = svc
		svc.peers[peer.addr] = peer
	}
	svc.peerMu.Unlock()

	want := consensus.TxID(&preferred)
	for round := 0; round < 2; round++ {
		svc.avalancheManager().runPollStep(time.Now())
		for _, peer := range peers {
			select {
			case envelope := <-peer.sendQ:
				poll, ok := envelope.msg.(p2p.AvaPollMessage)
				if !ok {
					t.Fatalf("message type = %T, want AvaPollMessage", envelope.msg)
				}
				votes := make([]p2p.AvaVote, len(poll.Items))
				for i := range votes {
					votes[i] = p2p.AvaVote{HasOpinion: true, TxID: want}
				}
				svc.onPeerMessage(peer, p2p.AvaVoteMessage{PollID: poll.PollID, Votes: votes})
			case <-time.After(100 * time.Millisecond):
				t.Fatalf("timed out waiting for avalanche poll in round %d", round)
			}
		}
	}

	if err := svc.avalancheManager().rejectionError(conflict); !errors.Is(err, ErrAvalancheFinalConflict) {
		t.Fatalf("rejection err = %v, want ErrAvalancheFinalConflict", err)
	}
	info := svc.avalancheManager().info()
	if info.FinalizedConflictSets != 1 {
		t.Fatalf("finalized conflict sets = %d, want 1", info.FinalizedConflictSets)
	}
	if info.VoteResponses != 6 {
		t.Fatalf("vote responses = %d, want 6", info.VoteResponses)
	}
}

func TestPeerConnPendingTxStoreMaterializesRelayBatchOnce(t *testing.T) {
	peer := &peerConn{
		queuedTx: make(map[[32]byte]int),
		knownTx:  make(map[[32]byte]struct{}),
	}
	first := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1}})
	second := coinbaseTxForHeight(2, []types.TxOutput{{ValueAtoms: 2}})

	ready, armFlush := peer.stagePendingTxs([]types.Transaction{first, second})
	if len(ready) != 0 {
		t.Fatalf("ready batches = %d, want 0", len(ready))
	}
	if !armFlush {
		t.Fatal("expected flush timer to arm for partial relay batch")
	}
	if len(peer.pendingTxOrder) != 2 || len(peer.pendingTxByID) != 2 {
		t.Fatalf("pending tx store = (%d order, %d payloads), want 2/2", len(peer.pendingTxOrder), len(peer.pendingTxByID))
	}

	batches := peer.takePendingTxs()
	if len(batches) != 1 {
		t.Fatalf("batch count = %d, want 1", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Fatalf("batch tx count = %d, want 2", len(batches[0]))
	}
	if consensus.TxID(&batches[0][0]) != consensus.TxID(&first) || consensus.TxID(&batches[0][1]) != consensus.TxID(&second) {
		t.Fatal("materialized relay batch lost tx order")
	}
	if len(peer.pendingTxOrder) != 0 || len(peer.pendingTxByID) != 0 {
		t.Fatal("expected pending tx store cleared after materialization")
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

func TestEmitThroughputSummaryLogsExpectedFields(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := consensus.UtxoSet{
		{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 50, KeyHash: nodeSignerKeyHash(1)},
	}
	tx := spendTxForNodeTest(t, 1, types.OutPoint{TxID: [32]byte{1}, Vout: 0}, 50, 2, 1)
	if _, err := pool.AcceptTx(tx, utxos, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("accept tx: %v", err)
	}

	startedAt := time.Unix(1_700_000_000, 0).UTC()
	peerA := newPeerConnForTests("127.0.0.1:18444")
	peerA.sendQ <- outboundMessage{}
	peerA.sendQ <- outboundMessage{}
	peerA.telemetry.noteEnqueue(queueDepthSnapshot{total: 2, send: 2})
	peerA.syncState.lastUsefulAt = startedAt.Add(30 * time.Second)
	peerB := newPeerConnForTests("127.0.0.1:18445")
	peerB.telemetry.noteEnqueue(queueDepthSnapshot{total: 1, send: 1})

	svc := &Service{
		cfg: ServiceConfig{
			StallTimeout:              15 * time.Second,
			ThroughputSummaryInterval: time.Minute,
		},
		logger:        logger,
		pool:          pool,
		peers:         map[string]*peerConn{peerA.addr: peerA, peerB.addr: peerB},
		blockRequests: map[[32]byte]blockDownloadRequest{{1}: {}, {2}: {}},
		txRequests:    map[[32]byte]blockDownloadRequest{{3}: {}, {4}: {}, {5}: {}},
		pendingBlocks: map[[32]byte]pendingPeerBlock{{9}: {}},
		startedAt:     startedAt,
		stopCh:        make(chan struct{}),
	}
	peerA.svc = svc
	peerB.svc = svc

	svc.noteAcceptedAdmissions([]mempool.Admission{{
		Accepted: []mempool.AcceptedTx{{TxID: [32]byte{6}}, {TxID: [32]byte{7}}},
	}})
	svc.noteRelaySent(relayMessageClass{txBatchItems: 6, blockInvItems: 1})
	svc.noteBlockAccepted()
	svc.noteTemplateRebuild()
	svc.noteTemplateInterruption()

	svc.emitThroughputSummary(startedAt.Add(time.Minute))

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("unmarshal summary log: %v", err)
	}
	if got := entry["msg"]; got != "throughput summary" {
		t.Fatalf("msg = %v, want throughput summary", got)
	}
	if got := int(entry["admitted_txs"].(float64)); got != 2 {
		t.Fatalf("admitted_txs = %d, want 2", got)
	}
	if got := entry["admitted_txs_per_sec"].(float64); got < 0.033 || got > 0.034 {
		t.Fatalf("admitted_txs_per_sec = %.6f, want about 0.0333", got)
	}
	if got := int(entry["relayed_tx_items"].(float64)); got != 6 {
		t.Fatalf("relayed_tx_items = %d, want 6", got)
	}
	if got := int(entry["relayed_block_items"].(float64)); got != 1 {
		t.Fatalf("relayed_block_items = %d, want 1", got)
	}
	if got := int(entry["blocks_accepted"].(float64)); got != 1 {
		t.Fatalf("blocks_accepted = %d, want 1", got)
	}
	if got := int(entry["template_rebuilds"].(float64)); got != 1 {
		t.Fatalf("template_rebuilds = %d, want 1", got)
	}
	if got := int(entry["template_interruptions"].(float64)); got != 1 {
		t.Fatalf("template_interruptions = %d, want 1", got)
	}
	if got := int(entry["orphan_promotions"].(float64)); got != 1 {
		t.Fatalf("orphan_promotions = %d, want 1", got)
	}
	if got := int(entry["mempool_txs"].(float64)); got != 1 {
		t.Fatalf("mempool_txs = %d, want 1", got)
	}
	if got := int(entry["candidate_frontier"].(float64)); got != 1 {
		t.Fatalf("candidate_frontier = %d, want 1", got)
	}
	if got := int(entry["peer_count"].(float64)); got != 2 {
		t.Fatalf("peer_count = %d, want 2", got)
	}
	if got := int(entry["useful_peers"].(float64)); got != 1 {
		t.Fatalf("useful_peers = %d, want 1", got)
	}
	if got := int(entry["relay_queue_depth"].(float64)); got != 2 {
		t.Fatalf("relay_queue_depth = %d, want 2", got)
	}
	if got := int(entry["relay_queue_depth_peak"].(float64)); got != 2 {
		t.Fatalf("relay_queue_depth_peak = %d, want 2", got)
	}
	if got := int(entry["pending_peer_blocks"].(float64)); got != 1 {
		t.Fatalf("pending_peer_blocks = %d, want 1", got)
	}
	if got := int(entry["inflight_block_requests"].(float64)); got != 2 {
		t.Fatalf("inflight_block_requests = %d, want 2", got)
	}
	if got := int(entry["inflight_tx_requests"].(float64)); got != 3 {
		t.Fatalf("inflight_tx_requests = %d, want 3", got)
	}
}

func TestEnqueuePriorityInvLogsSaturatedQueueAndTracksLaneDrops(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "debug"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = &Service{logger: logger}
	peer.relayPriorityQ = make(chan outboundMessage, 1)
	peer.relayPriorityQ <- outboundMessage{msg: p2p.PingMessage{Nonce: 1}}

	items := []p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: [32]byte{9}}}
	if err := peer.enqueueInvItems(items, true); err != nil {
		t.Fatalf("enqueueInvItems: %v", err)
	}

	stats := peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount())
	if stats.DroppedInv != 1 || stats.DroppedPriorityInv != 1 {
		t.Fatalf("unexpected dropped inv stats: %+v", stats)
	}
	logged := buf.String()
	if !strings.Contains(logged, "dropped relay inv due to saturated queue") || !strings.Contains(logged, "\"lane\":\"priority\"") {
		t.Fatalf("expected saturated priority queue log, got %s", logged)
	}
}

func TestFilterQueuedTxIDsLogsSuppressionBurst(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "debug"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = &Service{logger: logger}
	known := [32]byte{1}
	peer.knownTx[known] = struct{}{}

	txids := make([][32]byte, 0, 10)
	for i := 0; i < 10; i++ {
		txids = append(txids, known)
	}
	filtered := peer.filterQueuedTxIDs(txids, true)
	if len(filtered) != 0 {
		t.Fatalf("filtered txids = %d, want 0", len(filtered))
	}
	if !strings.Contains(buf.String(), "suppressed relay work before enqueue") || !strings.Contains(buf.String(), "\"kind\":\"tx_recon\"") {
		t.Fatalf("expected suppression log, got %s", buf.String())
	}
}

func TestRelayPeerStatsExposeLanePressureCounters(t *testing.T) {
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.telemetry.noteDroppedInv(2, relayQueueLanePriority)
	peer.telemetry.noteDroppedInv(3, relayQueueLaneSend)
	peer.telemetry.noteDroppedTxs(4, relayQueueLaneSend)
	peer.telemetry.noteWriterStarvation(relayQueueLaneControl)
	peer.telemetry.noteWriterStarvation(relayQueueLanePriority)
	peer.telemetry.noteWriterStarvation(relayQueueLaneSend)

	stats := peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount())
	if stats.DroppedPriorityInv != 2 || stats.DroppedSendInv != 3 || stats.DroppedSendTxs != 4 {
		t.Fatalf("unexpected lane drop stats: %+v", stats)
	}
	if stats.ControlStarvation != 1 || stats.PriorityStarvation != 1 || stats.SendStarvation != 1 {
		t.Fatalf("unexpected lane starvation stats: %+v", stats)
	}
}

func TestEnsureInboundCapacityEvictsLowValuePeerAndLogs(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	now := time.Now()
	goodA := newPeerConnForTests("127.0.0.1:18444")
	goodA.connectedAt = now.Add(-2 * time.Minute)
	goodA.noteHeight(25)
	goodA.noteUsefulBlocks(1, now)

	goodB := newPeerConnForTests("127.0.0.1:18445")
	goodB.connectedAt = now.Add(-2 * time.Minute)
	goodB.noteHeight(24)
	goodB.noteUsefulHeaders(1, now)

	low := newPeerConnForTests("127.0.0.1:18446")
	low.connectedAt = now.Add(-10 * time.Minute)

	svc := &Service{
		cfg:    ServiceConfig{MaxInboundPeers: 3},
		logger: logger,
		peers: map[string]*peerConn{
			goodA.addr: goodA,
			goodB.addr: goodB,
			low.addr:   low,
		},
		stopCh: make(chan struct{}),
	}

	if ok := svc.peerManager().ensureInboundCapacity("127.0.0.1:19000"); !ok {
		t.Fatal("expected inbound capacity manager to evict low-value peer")
	}
	if _, ok := svc.peers[low.addr]; ok {
		t.Fatalf("expected low-value peer %s to be evicted", low.addr)
	}
	logged := buf.String()
	if !strings.Contains(logged, "evicting low-value inbound peer to admit candidate") {
		t.Fatalf("expected inbound eviction log, got %s", logged)
	}
	if !strings.Contains(logged, low.addr) || !strings.Contains(logged, "127.0.0.1:19000") {
		t.Fatalf("expected candidate and victim in inbound eviction log, got %s", logged)
	}
}

func TestEnsureInboundCapacityRejectsWhenPeersProtected(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	now := time.Now()
	peerA := newPeerConnForTests("127.0.0.1:18444")
	peerA.connectedAt = now.Add(-2 * time.Minute)
	peerA.noteHeight(25)
	peerA.noteUsefulBlocks(1, now)

	peerB := newPeerConnForTests("127.0.0.1:18445")
	peerB.connectedAt = now.Add(-2 * time.Minute)
	peerB.noteHeight(24)
	peerB.noteUsefulHeaders(1, now)

	svc := &Service{
		cfg:    ServiceConfig{MaxInboundPeers: 2},
		logger: logger,
		peers: map[string]*peerConn{
			peerA.addr: peerA,
			peerB.addr: peerB,
		},
		stopCh: make(chan struct{}),
	}

	if ok := svc.peerManager().ensureInboundCapacity("127.0.0.1:19001"); ok {
		t.Fatal("expected inbound capacity manager to reject candidate when peers are protected")
	}
	if !strings.Contains(buf.String(), "all inbound slots currently protected") {
		t.Fatalf("expected protected inbound rejection log, got %s", buf.String())
	}
}

func TestReserveOutboundTargetReplacesLowValuePeerAndLogs(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.NewLogger(&buf, logging.Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	now := time.Now().UTC()
	manual := newPeerConnForTests("127.0.0.1:18444")
	manual.outbound = true
	manual.connectedAt = now.Add(-3 * time.Minute)
	manual.noteHeight(50)
	manual.noteUsefulBlocks(1, now)

	low := newPeerConnForTests("127.0.0.1:18445")
	low.outbound = true
	low.connectedAt = now.Add(-10 * time.Minute)

	svc := &Service{
		cfg:    ServiceConfig{MaxOutboundPeers: 2},
		logger: logger,
		peers: map[string]*peerConn{
			manual.addr: manual,
			low.addr:    low,
		},
		outboundPeers: map[string]struct{}{
			manual.addr: {},
			low.addr:    {},
		},
		knownPeers: map[string]storage.KnownPeerRecord{
			manual.addr:       {Manual: true, LastSeen: now, LastSuccess: now},
			low.addr:          {LastSeen: now.Add(-6 * time.Hour), FailureCount: 2},
			"127.0.0.1:18446": {LastSeen: now, LastSuccess: now},
		},
		stopCh: make(chan struct{}),
	}

	reservation, reserved, err := svc.peerManager().reserveOutboundTarget("127.0.0.1:18446", false)
	if err != nil {
		t.Fatalf("reserve outbound target: %v", err)
	}
	if !reserved {
		t.Fatal("expected outbound candidate to reserve a replacement slot")
	}
	if reservation.evictedAddr != low.addr {
		t.Fatalf("evicted addr = %s, want %s", reservation.evictedAddr, low.addr)
	}
	if _, ok := svc.outboundPeers[low.addr]; ok {
		t.Fatalf("expected low-value outbound target %s to be removed", low.addr)
	}
	if _, ok := svc.outboundPeers["127.0.0.1:18446"]; !ok {
		t.Fatal("expected replacement outbound target to be installed")
	}
	logged := buf.String()
	if !strings.Contains(logged, "evicting lower-value outbound target to make room for candidate") {
		t.Fatalf("expected outbound replacement log, got %s", logged)
	}
	if !strings.Contains(logged, low.addr) || !strings.Contains(logged, "127.0.0.1:18446") {
		t.Fatalf("expected candidate and victim in outbound replacement log, got %s", logged)
	}
}

func TestRefillOutboundPeersRebalancesLowValueLearnedPeer(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:          types.Regtest,
		DBPath:           t.TempDir(),
		MaxOutboundPeers: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	now := time.Now().UTC()
	manual := newPeerConnForTests("127.0.0.1:18444")
	manual.outbound = true
	manual.connectedAt = now.Add(-3 * time.Minute)
	manual.noteHeight(50)
	manual.noteUsefulBlocks(1, now)

	low := newPeerConnForTests("127.0.0.1:18445")
	low.outbound = true
	low.connectedAt = now.Add(-10 * time.Minute)

	svc.peerMu.Lock()
	svc.peers = map[string]*peerConn{
		manual.addr: manual,
		low.addr:    low,
	}
	svc.outboundPeers = map[string]struct{}{
		manual.addr: {},
		low.addr:    {},
	}
	svc.knownPeers = map[string]storage.KnownPeerRecord{
		manual.addr:       {Manual: true, LastSeen: now, LastSuccess: now},
		low.addr:          {LastSeen: now.Add(-6 * time.Hour), FailureCount: 2},
		"127.0.0.1:18446": {LastSeen: now, LastSuccess: now},
	}
	svc.peerMu.Unlock()

	svc.peerManager().refillOutboundPeers()

	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if _, ok := svc.outboundPeers[low.addr]; ok {
		t.Fatalf("expected rebalance to remove low-value outbound target %s", low.addr)
	}
	if _, ok := svc.outboundPeers["127.0.0.1:18446"]; !ok {
		t.Fatal("expected rebalance to install the better learned outbound target")
	}
}
