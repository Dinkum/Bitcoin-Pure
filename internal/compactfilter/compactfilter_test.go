package compactfilter

import (
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

func TestBuildAndMatch(t *testing.T) {
	block := &types.Block{
		Txs: []types.Transaction{
			{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 1, PubKey: [32]byte{7}}}}},
			{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 2, PubKey: [32]byte{8}}}}},
		},
	}
	undo := []storage.BlockUndoEntry{
		{Entry: consensus.UtxoEntry{ValueAtoms: 3, PubKey: [32]byte{9}}},
	}
	blockHash := [32]byte{4}
	filter := Build(blockHash, block, undo)
	for _, pubKey := range [][32]byte{{7}, {8}, {9}} {
		ok, err := Match(blockHash, filter.Encoded, pubKey)
		if err != nil {
			t.Fatalf("Match(%x): %v", pubKey, err)
		}
		if !ok {
			t.Fatalf("expected pubkey %x to match", pubKey)
		}
	}
	ok, err := Match(blockHash, filter.Encoded, [32]byte{42})
	if err != nil {
		t.Fatalf("Match(miss): %v", err)
	}
	if ok {
		t.Fatal("unexpected filter match for absent pubkey")
	}
}

func TestHeaderChainsDeterministically(t *testing.T) {
	firstHash := [32]byte{1}
	secondHash := [32]byte{2}
	first := Header(firstHash, [32]byte{})
	second := Header(secondHash, first)
	if second == first {
		t.Fatal("expected distinct chained filter headers")
	}
}
