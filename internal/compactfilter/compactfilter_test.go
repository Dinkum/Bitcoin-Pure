package compactfilter

import (
	"encoding/binary"
	"strings"
	"testing"

	"bitcoin-pure/internal/types"
)

func TestBuildAndMatch(t *testing.T) {
	block := &types.Block{
		Txs: []types.Transaction{
			{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 1, PubKey: [32]byte{7}}}}},
			{Base: types.TxBase{Outputs: []types.TxOutput{{ValueAtoms: 2, PubKey: [32]byte{8}}}}},
		},
	}
	spent := []WatchItem{
		{Type: types.OutputXOnlyP2PK, Payload32: [32]byte{9}},
	}
	blockHash := [32]byte{4}
	filter := Build(blockHash, block, spent)
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

func TestDecodeFingerprintsRejectsImpossibleCount(t *testing.T) {
	if _, err := decodeFingerprints([]byte{2, 0}); err == nil || !strings.Contains(err.Error(), "count") {
		t.Fatalf("decodeFingerprints err = %v, want count error", err)
	}
}

func TestDecodeFingerprintsRejectsMissingCount(t *testing.T) {
	if _, err := decodeFingerprints(nil); err == nil || !strings.Contains(err.Error(), "missing compact filter count") {
		t.Fatalf("decodeFingerprints err = %v, want missing count error", err)
	}
}

func TestDecodeFingerprintsRejectsOverlongCount(t *testing.T) {
	if _, err := decodeFingerprints([]byte{0x80, 0x00}); err == nil || !strings.Contains(err.Error(), "count") {
		t.Fatalf("decodeFingerprints err = %v, want count error", err)
	}
}

func TestDecodeFingerprintsRejectsOverlongDelta(t *testing.T) {
	if _, err := decodeFingerprints([]byte{1, 0x80, 0x00}); err == nil || !strings.Contains(err.Error(), "delta") {
		t.Fatalf("decodeFingerprints err = %v, want delta error", err)
	}
}

func TestDecodeFingerprintsRejectsDeltaOverflow(t *testing.T) {
	var encoded []byte
	encoded = binary.AppendUvarint(encoded, 2)
	encoded = binary.AppendUvarint(encoded, ^uint64(0))
	encoded = binary.AppendUvarint(encoded, 1)
	if _, err := decodeFingerprints(encoded); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("decodeFingerprints err = %v, want overflow error", err)
	}
}
