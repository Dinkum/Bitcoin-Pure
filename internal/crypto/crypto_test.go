package crypto

import "testing"

func TestTaggedHashIsStable(t *testing.T) {
	h := TaggedHash("BPU/Test", []byte("abc"))
	if h != TaggedHash("BPU/Test", []byte("abc")) {
		t.Fatal("tagged hash mismatch")
	}
	if h == TaggedHash("BPU/Test", []byte("abd")) {
		t.Fatal("tagged hash should differ")
	}
}

func TestSchnorrRoundtrip(t *testing.T) {
	msg := Sha256([]byte("hello"))
	pubKey, sig := RandomSignSchnorrForTest(&msg)
	if !VerifySchnorrXOnly(&pubKey, &sig, &msg) {
		t.Fatal("signature verification failed")
	}
}

func TestSchnorrBatchRoundtrip(t *testing.T) {
	items := make([]SchnorrBatchItem, 0, 8)
	for i := 0; i < 8; i++ {
		msg := Sha256([]byte{byte(i), byte(i + 1), byte(i + 2)})
		pubKey, sig := RandomSignSchnorrForTest(&msg)
		items = append(items, SchnorrBatchItem{
			PubKey:    pubKey,
			Signature: sig,
			Msg:       msg,
		})
	}
	if !VerifySchnorrBatchXOnly(items) {
		t.Fatal("batch verification failed")
	}
	if !VerifySchnorrBatchXOnlyWithFallback(items) {
		t.Fatal("batch verification with fallback failed")
	}
	result := VerifySchnorrBatchXOnlyResult(items)
	if !result.Valid {
		t.Fatal("batch verification result unexpectedly invalid")
	}
	if result.Fallback {
		t.Fatal("valid batch unexpectedly reported fallback")
	}
}

func TestSchnorrBatchRejectsTamperedSignature(t *testing.T) {
	items := make([]SchnorrBatchItem, 0, 4)
	for i := 0; i < 4; i++ {
		msg := Sha256([]byte{byte(10 + i), byte(20 + i)})
		pubKey, sig := RandomSignSchnorrForTest(&msg)
		items = append(items, SchnorrBatchItem{
			PubKey:    pubKey,
			Signature: sig,
			Msg:       msg,
		})
	}
	items[2].Signature[17] ^= 0x01
	if VerifySchnorrBatchXOnlyWithFallback(items) {
		t.Fatal("tampered batch unexpectedly verified")
	}
	result := VerifySchnorrBatchXOnlyResult(items)
	if result.Valid {
		t.Fatal("tampered batch unexpectedly verified via result helper")
	}
	if !result.Fallback {
		t.Fatal("tampered batch should report fallback attempt")
	}
}
