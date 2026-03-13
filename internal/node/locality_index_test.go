package node

import (
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestLocalityOrderedUTXOsAndProofBatchPlanning(t *testing.T) {
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
	first := nextCoinbaseBlock(view.Height, view.TipHeader, view.UTXOs, 3, view.TipHeader.Timestamp+1)
	if _, err := svc.chainState.ApplyBlock(&first); err != nil {
		t.Fatalf("ApplyBlock(first): %v", err)
	}
	view, ok = svc.chainState.CommittedView()
	if !ok {
		t.Fatal("expected committed view after first block")
	}
	second := nextCoinbaseBlock(view.Height, view.TipHeader, view.UTXOs, 4, view.TipHeader.Timestamp+1)
	if _, err := svc.chainState.ApplyBlock(&second); err != nil {
		t.Fatalf("ApplyBlock(second): %v", err)
	}

	ordered, err := svc.LocalityOrderedUTXOs(0)
	if err != nil {
		t.Fatalf("LocalityOrderedUTXOs: %v", err)
	}
	if len(ordered) < 3 {
		t.Fatalf("expected at least 3 locality-ordered utxos, got %d", len(ordered))
	}
	lastTwo := ordered[len(ordered)-2:]
	firstOutPoint := types.OutPoint{TxID: consensus.TxID(&first.Txs[0]), Vout: 0}
	secondOutPoint := types.OutPoint{TxID: consensus.TxID(&second.Txs[0]), Vout: 0}
	if lastTwo[0].OutPoint != firstOutPoint || lastTwo[1].OutPoint != secondOutPoint {
		t.Fatalf("expected recent coinbases to cluster at the end of locality order, got %+v", lastTwo)
	}

	planned, err := svc.PlanUTXOProofBatch([]types.OutPoint{secondOutPoint, firstOutPoint})
	if err != nil {
		t.Fatalf("PlanUTXOProofBatch: %v", err)
	}
	if len(planned) != 2 || planned[0] != firstOutPoint || planned[1] != secondOutPoint {
		t.Fatalf("unexpected proof batch order: %+v", planned)
	}
}
