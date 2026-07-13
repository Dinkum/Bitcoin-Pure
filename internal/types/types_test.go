package types

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

type shortBlockWriter struct {
	buf bytes.Buffer
	max int
}

func (w *shortBlockWriter) Write(p []byte) (int, error) {
	if len(p) > w.max {
		p = p[:w.max]
	}
	return w.buf.Write(p)
}

type zeroBlockWriter struct{}

func (zeroBlockWriter) Write([]byte) (int, error) { return 0, nil }

func testCoinbaseHeight(height uint64) *uint64 {
	return &height
}

func testCoinbaseExtraNonce(seed byte) *[CoinbaseExtraNonceLen]byte {
	var extra [CoinbaseExtraNonceLen]byte
	extra[0] = seed
	return &extra
}

func TestBlockWriteToMatchesCanonicalEncoding(t *testing.T) {
	block := Block{
		Header: BlockHeader{
			Version:        3,
			PrevBlockHash:  [32]byte{1},
			MerkleTxIDRoot: [32]byte{2},
			MerkleAuthRoot: [32]byte{3},
			UTXORoot:       [32]byte{4},
			Timestamp:      5,
			NBits:          6,
			Nonce:          7,
		},
		Txs: []Transaction{
			{
				Base: TxBase{
					Version:            1,
					CoinbaseHeight:     testCoinbaseHeight(9),
					CoinbaseExtraNonce: testCoinbaseExtraNonce(10),
					Outputs:            []TxOutput{NewXOnlyOutput(11, [32]byte{12})},
				},
			},
			{
				Base: TxBase{
					Version: 2,
					Inputs: []TxInput{
						{PrevOut: OutPoint{TxID: [32]byte{13}, Vout: 14}},
						{PrevOut: OutPoint{TxID: [32]byte{20}, Vout: 21}},
					},
					Outputs: []TxOutput{NewPQLockOutput(15, [32]byte{16})},
				},
				Auth: TxAuth{Entries: []TxAuthEntry{
					{AuthPayload: []byte{17, 18, 19}},
					{Signature: [64]byte{22}},
				}},
			},
		},
	}
	w := &shortBlockWriter{max: 3}
	written, err := block.WriteTo(w)
	if err != nil {
		t.Fatal(err)
	}
	want := block.Encode()
	if written != int64(len(want)) {
		t.Fatalf("WriteTo bytes = %d, want %d", written, len(want))
	}
	if !bytes.Equal(w.buf.Bytes(), want) {
		t.Fatal("streamed block encoding differs from canonical Encode")
	}
	if _, err := block.WriteTo(zeroBlockWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero-progress writer error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestDecodeBlockOwnedBufferBorrowsAuthorizationPayload(t *testing.T) {
	block := Block{
		Header: BlockHeader{Version: 1},
		Txs: []Transaction{{
			Base: TxBase{
				Version: 1,
				Inputs:  []TxInput{{PrevOut: OutPoint{TxID: [32]byte{1}}}},
				Outputs: []TxOutput{NewXOnlyOutput(2, [32]byte{3})},
			},
			Auth: TxAuth{Entries: []TxAuthEntry{{AuthPayload: []byte{4, 5, 6}}}},
		}},
	}
	encoded := block.Encode()
	decoded, err := DecodeBlockOwnedBuffer(encoded, uint64(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	payload := decoded.Txs[0].Auth.Entries[0].AuthPayload
	if len(payload) != 3 {
		t.Fatalf("decoded payload length = %d, want 3", len(payload))
	}
	payload[0] = 99
	if encoded[len(encoded)-len(payload)] != 99 {
		t.Fatal("owned-buffer decoder copied the authorization payload")
	}
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
				PubKey:     [32]byte{2},
			}},
		},
		Auth: TxAuth{
			Entries: []TxAuthEntry{{
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

func TestEncodedLenMatchesCanonicalEncoding(t *testing.T) {
	coinbase := Transaction{
		Base: TxBase{
			Version:            1,
			CoinbaseHeight:     testCoinbaseHeight(253),
			CoinbaseExtraNonce: testCoinbaseExtraNonce(3),
			Outputs: []TxOutput{{
				ValueAtoms: 50,
				PubKey:     [32]byte{5},
			}},
		},
	}
	tx := sampleTx()
	pqTx := sampleTx()
	pqTx.Base.Outputs = []TxOutput{{
		Type:       OutputPQLock32,
		ValueAtoms: 7,
		Payload32:  [32]byte{8},
	}}
	pqTx.Auth.Entries = []TxAuthEntry{{AuthPayload: bytes.Repeat([]byte{9}, 128)}}
	block := Block{
		Header: BlockHeader{Version: 1},
		Txs:    []Transaction{coinbase, tx, pqTx},
	}
	cases := []Transaction{coinbase, tx, pqTx}
	for i, item := range cases {
		if got, want := item.EncodedLen(), len(item.Encode()); got != want {
			t.Fatalf("tx %d encoded len = %d, want %d", i, got, want)
		}
	}
	if got, want := block.EncodedLen(), len(block.Encode()); got != want {
		t.Fatalf("block encoded len = %d, want %d", got, want)
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
			Nonce:          1<<40 + 7,
		},
		Txs: []Transaction{{
			Base: TxBase{
				Version:            1,
				CoinbaseHeight:     testCoinbaseHeight(0),
				CoinbaseExtraNonce: testCoinbaseExtraNonce(1),
				Outputs: []TxOutput{{
					ValueAtoms: 50,
					PubKey:     [32]byte{5},
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

func TestDecodeBlockDoesNotImposeIndependentOutputCountLimit(t *testing.T) {
	outputs := make([]TxOutput, 100_001)
	for i := range outputs {
		outputs[i] = NewXOnlyOutput(1, [32]byte{byte(i)})
	}
	block := Block{Txs: []Transaction{{Base: TxBase{
		Version:            1,
		CoinbaseHeight:     testCoinbaseHeight(1),
		CoinbaseExtraNonce: testCoinbaseExtraNonce(1),
		Outputs:            outputs,
	}}}}
	encoded := block.Encode()
	decoded, err := DecodeBlockWithBudget(encoded, uint64(len(encoded)))
	if err != nil {
		t.Fatalf("decode 100001-output block: %v", err)
	}
	if got := len(decoded.Txs[0].Base.Outputs); got != len(outputs) {
		t.Fatalf("decoded outputs = %d, want %d", got, len(outputs))
	}
}

func TestDecodeBlockRejectsOversizeBeforeDecode(t *testing.T) {
	block := Block{
		Header: BlockHeader{Version: 1},
		Txs: []Transaction{{
			Base: TxBase{
				Version:            1,
				CoinbaseHeight:     testCoinbaseHeight(0),
				CoinbaseExtraNonce: testCoinbaseExtraNonce(1),
				Outputs: []TxOutput{{
					ValueAtoms: 50,
					PubKey:     [32]byte{5},
				}},
			},
		}},
	}
	raw := block.Encode()
	limits := DefaultCodecLimits()
	limits.MaxBlockBytes = len(raw) - 1
	var limitErr LimitExceededError
	if _, err := DecodeBlockWithLimits(raw, limits); !errors.As(err, &limitErr) {
		t.Fatalf("decode oversize block error = %v, want LimitExceededError", err)
	}
}

func TestCoinbaseTransactionRoundtripPreservesHeight(t *testing.T) {
	tx := Transaction{
		Base: TxBase{
			Version:            7,
			CoinbaseHeight:     testCoinbaseHeight(123),
			CoinbaseExtraNonce: testCoinbaseExtraNonce(9),
			Inputs:             []TxInput{},
			Outputs: []TxOutput{{
				ValueAtoms: 50,
				PubKey:     [32]byte{9},
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

func TestBlockHeaderRoundtripPreservesUint64Nonce(t *testing.T) {
	header := BlockHeader{
		Version:        1,
		PrevBlockHash:  [32]byte{1},
		MerkleTxIDRoot: [32]byte{2},
		MerkleAuthRoot: [32]byte{3},
		UTXORoot:       [32]byte{4},
		Timestamp:      5,
		NBits:          0x1d00ffff,
		Nonce:          1<<48 + 9,
	}
	got, err := DecodeBlockHeader(header.Encode())
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if !reflect.DeepEqual(got, header) {
		t.Fatalf("header mismatch")
	}
	if len(header.Encode()) != BlockHeaderEncodedLen {
		t.Fatalf("header length = %d, want %d", len(header.Encode()), BlockHeaderEncodedLen)
	}
}

func TestDecodeTransactionPreservesZeroLength64AuthPayload(t *testing.T) {
	tx := sampleTx()
	tx.Auth.Entries[0] = TxAuthEntry{AuthPayload: make([]byte, 64)}
	encoded := tx.Encode()
	got, err := DecodeTransactionWithLimits(encoded, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(got.Encode(), encoded) {
		t.Fatal("zero-filled 64-byte auth payload was not preserved")
	}
}

func TestDecodeTransactionRejectsCoinbaseMissingExtraNonce(t *testing.T) {
	raw := []byte{
		0x01, 0x00, 0x00, 0x00, // version
		0x00, // input_count
		0x01, // coinbase_height
		0x01, // output_count
	}
	raw = append(raw, make([]byte, 40)...)
	raw = append(raw, 0x00) // auth_count
	if _, err := DecodeTransactionWithLimits(raw, DefaultCodecLimits()); err == nil {
		t.Fatal("expected missing coinbase extra nonce to fail decoding")
	}
}

func TestRejectsTrailingBytes(t *testing.T) {
	encoded := append(sampleTx().Encode(), 0)
	_, err := DecodeTransactionWithLimits(encoded, DefaultCodecLimits())
	if !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("expected trailing bytes error, got %v", err)
	}
}

func TestParseChainProfileAcceptsRegtestMediumHardAndBenchNet(t *testing.T) {
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

	profile, err = ParseChainProfile("benchnet")
	if err != nil {
		t.Fatalf("ParseChainProfile(benchnet): %v", err)
	}
	if profile != BenchNet {
		t.Fatalf("profile = %q, want %q", profile, BenchNet)
	}
	if !profile.IsRegtestLike() {
		t.Fatal("benchnet should be treated as regtest-like")
	}
}
