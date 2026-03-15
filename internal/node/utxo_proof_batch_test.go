package node

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestGetUTXOProofBatchRPC(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	txid := consensus.TxID(&genesis.Txs[0])
	var syntheticTxID [32]byte
	syntheticTxID[0] = 9
	params, err := json.Marshal(map[string]any{
		"outpoints": []map[string]any{
			{"txid": hex.EncodeToString(txid[:]), "vout": uint32(0)},
			{"txid": hex.EncodeToString(syntheticTxID[:]), "vout": uint32(1)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getutxoproofbatch",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	raw, ok := result.(RPCAnchoredUTXOProofBatch)
	if !ok {
		t.Fatalf("result type = %T, want RPCAnchoredUTXOProofBatch", result)
	}
	if len(raw.Proofs) != 2 {
		t.Fatalf("proof count = %d, want 2", len(raw.Proofs))
	}
	batch, err := DecodeRPCUTXOProofBatch(raw)
	if err != nil {
		t.Fatalf("DecodeRPCUTXOProofBatch: %v", err)
	}
	verification := VerifyAnchoredUTXOProofBatch(batch)
	if !verification.AllValid || verification.ValidCount != 2 {
		t.Fatalf("batch verification = %+v, want all proofs valid", verification)
	}
}

func TestCompactStatePackageRPCUsesLocalityOrder(t *testing.T) {
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
	first := nextCoinbaseBlock(view.Height, view.TipHeader, svc.chainState.ChainState().UTXOs(), 3, view.TipHeader.Timestamp+1)
	if _, err := svc.chainState.ApplyBlock(&first); err != nil {
		t.Fatalf("ApplyBlock(first): %v", err)
	}
	view, ok = svc.chainState.CommittedView()
	if !ok {
		t.Fatal("expected committed view after first block")
	}
	second := nextCoinbaseBlock(view.Height, view.TipHeader, svc.chainState.ChainState().UTXOs(), 4, view.TipHeader.Timestamp+1)
	if _, err := svc.chainState.ApplyBlock(&second); err != nil {
		t.Fatalf("ApplyBlock(second): %v", err)
	}

	firstOutPoint := types.OutPoint{TxID: consensus.TxID(&first.Txs[0]), Vout: 0}
	secondOutPoint := types.OutPoint{TxID: consensus.TxID(&second.Txs[0]), Vout: 0}
	params, err := json.Marshal(map[string]any{
		"outpoints": []map[string]any{
			{"txid": hex.EncodeToString(secondOutPoint.TxID[:]), "vout": secondOutPoint.Vout},
			{"txid": hex.EncodeToString(firstOutPoint.TxID[:]), "vout": firstOutPoint.Vout},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getcompactstatepackage",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	raw, ok := result.(RPCCompactStatePackage)
	if !ok {
		t.Fatalf("result type = %T, want RPCCompactStatePackage", result)
	}
	if !raw.LocalityOrdered {
		t.Fatal("expected compact state package to report locality ordering")
	}
	pkg, err := DecodeRPCCompactStatePackage(raw)
	if err != nil {
		t.Fatalf("DecodeRPCCompactStatePackage: %v", err)
	}
	if len(pkg.Proofs) != 2 {
		t.Fatalf("proof count = %d, want 2", len(pkg.Proofs))
	}
	if pkg.Proofs[0].OutPoint != firstOutPoint || pkg.Proofs[1].OutPoint != secondOutPoint {
		t.Fatalf("unexpected compact package proof order: %+v", pkg.Proofs)
	}
	verification, err := svc.VerifyCompactStatePackage(pkg)
	if err != nil {
		t.Fatalf("VerifyCompactStatePackage: %v", err)
	}
	if !verification.AllValid || verification.ValidCount != 2 || !verification.AnchorMatchesLocal {
		t.Fatalf("compact package verification = %+v, want all proofs valid and locally anchored", verification)
	}
}
