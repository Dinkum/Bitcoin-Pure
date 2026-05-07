package types

import (
	"bytes"
	"testing"
)

func fuzzCodecLimits() CodecLimits {
	return CodecLimits{
		MaxInputs:      16,
		MaxOutputs:     16,
		MaxTxsPerBlock: 16,
		MaxBlockBytes:  4096,
	}
}

func FuzzDecodeTransactionCanonicalRoundTrip(f *testing.F) {
	f.Add(sampleTx().Encode())
	f.Add(Transaction{
		Base: TxBase{
			Version:            1,
			CoinbaseHeight:     testCoinbaseHeight(1),
			CoinbaseExtraNonce: testCoinbaseExtraNonce(7),
			Outputs: []TxOutput{{
				ValueAtoms: 50,
				PubKey:     [32]byte{5},
			}},
		},
	}.Encode())
	f.Add([]byte{0xfd, 0xfc, 0x00})

	limits := fuzzCodecLimits()
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > limits.MaxBlockBytes {
			t.Skip()
		}
		tx, err := DecodeTransactionWithLimits(raw, limits)
		if err != nil {
			return
		}
		encoded := tx.Encode()
		if !bytes.Equal(encoded, raw) {
			t.Fatalf("accepted non-canonical transaction encoding: raw=%x encoded=%x tx=%#v", raw, encoded, tx)
		}
		roundTrip, err := DecodeTransactionWithLimits(encoded, limits)
		if err != nil {
			t.Fatalf("redecode canonical transaction: %v", err)
		}
		if !bytes.Equal(roundTrip.Encode(), encoded) {
			t.Fatal("transaction canonical encoding was not stable")
		}
	})
}

func FuzzDecodeBlockCanonicalRoundTrip(f *testing.F) {
	coinbase := Transaction{
		Base: TxBase{
			Version:            1,
			CoinbaseHeight:     testCoinbaseHeight(2),
			CoinbaseExtraNonce: testCoinbaseExtraNonce(9),
			Outputs: []TxOutput{{
				ValueAtoms: 50,
				PubKey:     [32]byte{5},
			}},
		},
	}
	f.Add(Block{
		Header: BlockHeader{
			Version:        1,
			MerkleTxIDRoot: [32]byte{9},
			MerkleAuthRoot: [32]byte{8},
			UTXORoot:       [32]byte{7},
			Timestamp:      1,
			NBits:          0x1d00ffff,
			Nonce:          7,
		},
		Txs: []Transaction{coinbase},
	}.Encode())
	f.Add(append(BlockHeader{Version: 1}.Encode(), 0xfd, 0xfc, 0x00))

	limits := fuzzCodecLimits()
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > limits.MaxBlockBytes {
			t.Skip()
		}
		block, err := DecodeBlockWithLimits(raw, limits)
		if err != nil {
			return
		}
		encoded := block.Encode()
		if !bytes.Equal(encoded, raw) {
			t.Fatalf("accepted non-canonical block encoding: raw=%x encoded=%x block=%#v", raw, encoded, block)
		}
		roundTrip, err := DecodeBlockWithLimits(encoded, limits)
		if err != nil {
			t.Fatalf("redecode canonical block: %v", err)
		}
		if !bytes.Equal(roundTrip.Encode(), encoded) {
			t.Fatal("block canonical encoding was not stable")
		}
	})
}
