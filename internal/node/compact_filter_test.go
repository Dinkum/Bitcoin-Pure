package node

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestGetBlockFilterRPCIncludesCreatedAndSpentPubKeys(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
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
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	block := blockWithTxsForNodeTest(t, 0, genesis.Header, svc.chainState.ChainState().UTXOs(), []types.Transaction{coinbase, spend}, genesis.Header.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(block); err != nil {
		t.Fatalf("acceptMinedBlock: %v", err)
	}
	blockHash := consensus.HeaderHash(&block.Header)
	params, err := json.Marshal(map[string]any{"hash": hex.EncodeToString(blockHash[:])})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{Method: "getblockfilter", Params: params})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	info, ok := result.(CompactFilterInfo)
	if !ok {
		t.Fatalf("result type = %T, want CompactFilterInfo", result)
	}
	filterBytes, err := hex.DecodeString(info.FilterHex)
	if err != nil {
		t.Fatalf("DecodeString(filter): %v", err)
	}
	for _, pubKey := range [][32]byte{nodeSignerPubKey(7), nodeSignerPubKey(8), nodeSignerPubKey(9)} {
		matched, err := compactfilter.Match(blockHash, filterBytes, pubKey)
		if err != nil {
			t.Fatalf("Match(%x): %v", pubKey, err)
		}
		if !matched {
			t.Fatalf("expected filter to match pubkey %x", pubKey)
		}
	}
}

func TestGetFilterHeadersRPCChainsCompactFilters(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	view, ok := svc.chainState.CommittedView()
	if !ok {
		t.Fatal("expected committed view")
	}
	first := nextCoinbaseBlock(view.Height, view.TipHeader, view.UTXOs, 3, view.TipHeader.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(first); err != nil {
		t.Fatalf("acceptMinedBlock(first): %v", err)
	}
	view, ok = svc.chainState.CommittedView()
	if !ok {
		t.Fatal("expected committed view after first block")
	}
	second := nextCoinbaseBlock(view.Height, view.TipHeader, view.UTXOs, 4, view.TipHeader.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(second); err != nil {
		t.Fatalf("acceptMinedBlock(second): %v", err)
	}

	params, err := json.Marshal(map[string]any{"start_height": uint64(1), "count": uint64(2)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{Method: "getfilterheaders", Params: params})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	info, ok := result.(CompactFilterHeadersInfo)
	if !ok {
		t.Fatalf("result type = %T, want CompactFilterHeadersInfo", result)
	}
	if len(info.Headers) != 2 {
		t.Fatalf("header count = %d, want 2", len(info.Headers))
	}
	prevHeader, err := hex.DecodeString(info.PreviousFilterHeader)
	if err != nil {
		t.Fatalf("DecodeString(previous): %v", err)
	}
	if len(prevHeader) != 32 {
		t.Fatalf("previous filter header len = %d, want 32", len(prevHeader))
	}
	firstHash := consensus.HeaderHash(&first.Header)
	firstFilter, err := svc.compactFilterForHash(firstHash)
	if err != nil {
		t.Fatalf("compactFilterForHash(first): %v", err)
	}
	var previous [32]byte
	copy(previous[:], prevHeader)
	expectedFirstHeader := compactfilter.Header(firstFilter.Hash, previous)
	if info.Headers[0].FilterHeader != hex.EncodeToString(expectedFirstHeader[:]) {
		t.Fatalf("first filter header = %s, want %x", info.Headers[0].FilterHeader, expectedFirstHeader)
	}
}

func TestGetFilterCheckpointRPC(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	view, _ := svc.chainState.CommittedView()
	first := nextCoinbaseBlock(view.Height, view.TipHeader, view.UTXOs, 3, view.TipHeader.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(first); err != nil {
		t.Fatalf("acceptMinedBlock(first): %v", err)
	}
	view, _ = svc.chainState.CommittedView()
	second := nextCoinbaseBlock(view.Height, view.TipHeader, view.UTXOs, 4, view.TipHeader.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(second); err != nil {
		t.Fatalf("acceptMinedBlock(second): %v", err)
	}

	params, err := json.Marshal(map[string]any{"interval": uint64(1)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{Method: "getfiltercheckpoint", Params: params})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	info, ok := result.(CompactFilterCheckpointInfo)
	if !ok {
		t.Fatalf("result type = %T, want CompactFilterCheckpointInfo", result)
	}
	if len(info.Headers) != 3 {
		t.Fatalf("checkpoint count = %d, want 3", len(info.Headers))
	}
	if info.Headers[2].Height != 2 {
		t.Fatalf("tip checkpoint height = %d, want 2", info.Headers[2].Height)
	}
}
