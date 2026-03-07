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
