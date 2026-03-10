package types

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func testCoinbaseHeight(height uint64) *uint64 {
	return &height
}

func sampleTx() Transaction {
	return Transaction{
		Base: TxBase{
			Version: 1,
			Inputs: []TxInput{{
				PrevOut: OutPoint{
					TxID: [32]byte{1},
					Vout: 0,
				},
			}},
			Outputs: []TxOutput{{
				ValueAtoms: 42,
				KeyHash:    [32]byte{2},
			}},
		},
		Auth: TxAuth{
			Entries: []TxAuthEntry{{
				PubKey:    [32]byte{3},
				Signature: [64]byte{4},
			}},
		},
	}
}

func TestTransactionRoundtrip(t *testing.T) {
	tx := sampleTx()
	got, err := DecodeTransactionWithLimits(tx.Encode(), DefaultCodecLimits())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, tx) {
		t.Fatalf("transaction mismatch")
	}
}

func TestBlockRoundtrip(t *testing.T) {
	block := Block{
		Header: BlockHeader{
			Version:        1,
			MerkleTxIDRoot: [32]byte{9},
			MerkleAuthRoot: [32]byte{8},
			UTXORoot:       [32]byte{7},
			Timestamp:      1,
			NBits:          0x1d00ffff,
		},
		Txs: []Transaction{{
			Base: TxBase{
				Version:        1,
				CoinbaseHeight: testCoinbaseHeight(0),
				Outputs: []TxOutput{{
					ValueAtoms: 50,
					KeyHash:    [32]byte{5},
				}},
			},
		}},
	}
	got, err := DecodeBlockWithLimits(block.Encode(), DefaultCodecLimits())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(got.Encode(), block.Encode()) {
		t.Fatalf("block mismatch")
	}
}

func TestCoinbaseTransactionRoundtripPreservesHeight(t *testing.T) {
	tx := Transaction{
		Base: TxBase{
			Version:        7,
			CoinbaseHeight: testCoinbaseHeight(123),
			Inputs:         []TxInput{},
			Outputs: []TxOutput{{
				ValueAtoms: 50,
				KeyHash:    [32]byte{9},
			}},
		},
		Auth: TxAuth{Entries: []TxAuthEntry{}},
	}
	got, err := DecodeTransactionWithLimits(tx.Encode(), DefaultCodecLimits())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, tx) {
		t.Fatalf("coinbase transaction mismatch")
	}
}

func TestRejectsTrailingBytes(t *testing.T) {
	encoded := append(sampleTx().Encode(), 0)
	_, err := DecodeTransactionWithLimits(encoded, DefaultCodecLimits())
	if !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("expected trailing bytes error, got %v", err)
	}
}

func TestParseChainProfileAcceptsRegtestMediumAndHard(t *testing.T) {
	profile, err := ParseChainProfile("regtest_medium")
	if err != nil {
		t.Fatalf("ParseChainProfile(regtest_medium): %v", err)
	}
	if profile != RegtestMedium {
		t.Fatalf("profile = %q, want %q", profile, RegtestMedium)
	}
	if !profile.IsRegtestLike() {
		t.Fatal("regtest_medium should be treated as regtest-like")
	}

	profile, err = ParseChainProfile("regtest_hard")
	if err != nil {
		t.Fatalf("ParseChainProfile(regtest_hard): %v", err)
	}
	if profile != RegtestHard {
		t.Fatalf("profile = %q, want %q", profile, RegtestHard)
	}
	if !profile.IsRegtestLike() {
		t.Fatal("regtest_hard should be treated as regtest-like")
	}
}
