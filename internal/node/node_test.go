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
	"math/big"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
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
	coinbase := coinbaseTxForHeight(0, []types.TxOutput{{ValueAtoms: 50, KeyHash: [32]byte{7}}})
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
		MerkleTxIDRoot: consensus.MerkleRoot(txids),
		MerkleAuthRoot: consensus.MerkleRoot(authids),
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

func TestCommittedViewReusesPublishedChainState(t *testing.T) {
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

func TestPersistentChainStateRejectsBlockWithIntraBlockSpend(t *testing.T) {
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
			MerkleTxIDRoot: consensus.MerkleRoot([][32]byte{genesisTxID}),
			MerkleAuthRoot: consensus.MerkleRoot([][32]byte{genesisAuthID}),
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
	nextUTXOs[types.OutPoint{TxID: parentTxID, Vout: 0}] = consensus.UtxoEntry{
		ValueAtoms: parent.Base.Outputs[0].ValueAtoms,
		KeyHash:    parent.Base.Outputs[0].KeyHash,
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

	if _, err := persistent.ApplyBlock(&block); !errors.Is(err, consensus.ErrMissingUTXO) {
		t.Fatalf("apply block with intra-block spend error = %v, want %v", err, consensus.ErrMissingUTXO)
	}
	if persistent.ChainState().TipHeight() == nil || *persistent.ChainState().TipHeight() != 0 {
		t.Fatalf("unexpected tip after rejected block: %v", persistent.ChainState().TipHeight())
	}
	if _, ok := persistent.ChainState().UTXOs()[genesisOut]; !ok {
		t.Fatal("expected genesis outpoint to remain after rejection")
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
	genesis.Header.MerkleTxIDRoot = consensus.MerkleRoot([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = consensus.MerkleRoot([][32]byte{consensus.AuthID(&genesis.Txs[0])})
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
	genesis.Header.MerkleTxIDRoot = consensus.MerkleRoot([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = consensus.MerkleRoot([][32]byte{consensus.AuthID(&genesis.Txs[0])})
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
	genesis.Header.MerkleTxIDRoot = consensus.MerkleRoot([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = consensus.MerkleRoot([][32]byte{consensus.AuthID(&genesis.Txs[0])})
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
	genesis.Header.MerkleTxIDRoot = consensus.MerkleRoot([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = consensus.MerkleRoot([][32]byte{consensus.AuthID(&genesis.Txs[0])})
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
	genesis.Header.MerkleTxIDRoot = consensus.MerkleRoot([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = consensus.MerkleRoot([][32]byte{consensus.AuthID(&genesis.Txs[0])})
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
	svc.mineHeaderFn = func(header types.BlockHeader, params consensus.ChainParams, shouldContinue func(uint32) bool) (types.BlockHeader, bool, error) {
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
	if _, ok := loaded["127.0.0.1:18444"]; !ok {
		t.Fatal("persisted vetted peer missing after reopen")
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
	peer.pendingTxs = []types.Transaction{tx}
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
	if len(peer.pendingTxs) != 0 || len(peer.pendingRecon) != 0 {
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
	if !strings.Contains(out, "tip:11") {
		t.Fatalf("expected tip marker in block flow: %q", out)
	}
	if !strings.Contains(out, "aa0000000000") || !strings.Contains(out, "bb0000000000") {
		t.Fatalf("expected short hashes in block flow: %q", out)
	}
	if strings.Count(out, "--->") != 1 {
		t.Fatalf("expected single arrows in block flow: %q", out)
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
		"Network Avg  : up 512.0 KB/s",
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

func TestRenderMempoolSectionUsesStackedMetrics(t *testing.T) {
	section := renderMempoolSection(dashboardMempoolSummary{
		Count:         256,
		Orphans:       3,
		MedianFee:     179,
		LowFee:        120,
		HighFee:       240,
		EstimatedNext: 10 * time.Minute,
		Top: []mempool.SnapshotEntry{{
			Tx:   types.Transaction{},
			TxID: [32]byte{0xaa},
			Fee:  240,
			Size: 180,
		}},
	})

	for _, want := range []string{
		"Tx Count     : 256",
		"Orphans      : 3",
		"Median Fee   : 179",
		"Fee Low / Hi : 120 / 240",
		"Next Block   : est 00h 10m 00s",
		"Queue Top    :",
		"aa00000000",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected %q in mempool section:\n%s", want, section)
		}
	}
}

func TestRenderPublicDashboardPagesExposeRecentBlockAndTxLinks(t *testing.T) {
	blockHash := [32]byte{0xaa}
	txID := [32]byte{0xbb}
	view := &publicDashboardView{
		nodeID:  "NODE1234",
		info:    ServiceInfo{TipHeight: 12, TipHeaderHash: hex.EncodeToString(blockHash[:]), UTXORoot: strings.Repeat("c", 64)},
		pow:     dashboardPowSummary{Algorithm: "ASERT per-block", TargetSpacing: 10 * time.Minute, AvgBlockInterval: 10 * time.Minute, HasObservedGap: true},
		fees:    dashboardFeeSummary{Clear: dashboardMempoolClearEstimate{Blocks: 1, Time: 10 * time.Minute}},
		mempool: dashboardMempoolSummary{},
		tpsChart: dashboardTPSChart{
			Label:     "TPS last 10m",
			Buckets:   []float64{1, 2, 3},
			BucketEnd: []time.Time{time.Now(), time.Now(), time.Now()},
			MaxTPS:    3,
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
	}

	home, status := renderPublicDashboardPage(view, "/")
	if status != 200 || !strings.Contains(home, "/block/"+hex.EncodeToString(blockHash[:])) {
		t.Fatalf("home page missing block link:\n%s", home)
	}
	blockPage, status := renderPublicDashboardPage(view, "/block/"+hex.EncodeToString(blockHash[:]))
	if status != 200 || !strings.Contains(blockPage, "/tx/"+hex.EncodeToString(txID[:])) {
		t.Fatalf("block page missing tx link:\n%s", blockPage)
	}
	txPage, status := renderPublicDashboardPage(view, "/tx/"+hex.EncodeToString(txID[:]))
	if status != 200 || !strings.Contains(txPage, "TRANSACTION") {
		t.Fatalf("tx page missing transaction section:\n%s", txPage)
	}
}

func TestPeerInfoUsesObservedHeight(t *testing.T) {
	svc := &Service{
		peers: make(map[string]*peerConn),
	}
	peer := &peerConn{
		addr:     "127.0.0.1:18444",
		outbound: true,
		version:  p2p.VersionMessage{Height: 7, UserAgent: "bpu/go"},
	}
	peer.noteProgress(time.Unix(100, 0))
	svc.peers[peer.addr] = peer

	if got := svc.PeerInfo()[0].Height; got != 7 {
		t.Fatalf("peer height = %d, want handshake height 7", got)
	}

	peer.noteHeight(11)
	if got := svc.PeerInfo()[0].Height; got != 11 {
		t.Fatalf("peer height = %d, want observed height 11", got)
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

	hashAtHeight, err := svc.chainState.Store().GetBlockHashByHeight(1)
	if err != nil {
		t.Fatalf("GetBlockHashByHeight: %v", err)
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
	hashAtHeight, err := reopened.chainState.Store().GetBlockHashByHeight(1)
	if err != nil {
		t.Fatalf("GetBlockHashByHeight(1): %v", err)
	}
	if hashAtHeight == nil || *hashAtHeight != altFirstHash {
		t.Fatalf("active header hash at height 1 = %x, want %x", hashAtHeight, altFirstHash)
	}

	altSecondHash := consensus.HeaderHash(&altSecond.Header)
	hashAtHeight, err = reopened.chainState.Store().GetBlockHashByHeight(2)
	if err != nil {
		t.Fatalf("GetBlockHashByHeight(2): %v", err)
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
	if applied, err := svc.applyPeerBlock(&block); err != nil || !applied {
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
	if err := svc.chainState.Store().RewriteActiveHeights(0, 1, []storage.BlockIndexEntry{*genesisEntry}); err != nil {
		t.Fatalf("RewriteActiveHeights: %v", err)
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
	if err := svc.chainState.Store().RewriteActiveHeights(0, 1, []storage.BlockIndexEntry{*genesisEntry}); err != nil {
		t.Fatalf("RewriteActiveHeights: %v", err)
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
		addr:        addr,
		sendQ:       make(chan outboundMessage, 4),
		closed:      make(chan struct{}),
		queuedInv:   make(map[p2p.InvVector]int),
		queuedTx:    make(map[[32]byte]int),
		knownTx:     make(map[[32]byte]struct{}),
		pendingThin: make(map[[32]byte]*pendingThinBlock),
		version:     p2p.VersionMessage{Height: 0, UserAgent: "bpu/go"},
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
