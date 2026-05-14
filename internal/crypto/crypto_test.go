package crypto

import (
	"strings"
	"testing"
)

func TestHash32HexRoundTripUsesCanonicalByteOrder(t *testing.T) {
	raw := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	hash, err := ParseHash32Hex(raw)
	if err != nil {
		t.Fatalf("ParseHash32Hex: %v", err)
	}
	if hash.String() != raw {
		t.Fatalf("String = %s, want %s", hash.String(), raw)
	}
	if hash.Array()[0] != 0x00 || hash.Array()[31] != 0x1f {
		t.Fatalf("hash byte order changed: %x", hash.Array())
	}
	bytesCopy := hash.Bytes()
	bytesCopy[0] = 0xff
	if hash.Array()[0] != 0x00 {
		t.Fatal("Bytes returned mutable backing storage")
	}
}

func TestParseHash32HexRejectsInvalidInput(t *testing.T) {
	for _, raw := range []string{
		"",
		strings.Repeat("00", 31),
		strings.Repeat("00", 33),
		strings.Repeat("zz", 32),
	} {
		if _, err := ParseHash32Hex(raw); err == nil {
			t.Fatalf("ParseHash32Hex(%q) succeeded", raw)
		}
	}
}

func TestTaggedHashIsStable(t *testing.T) {
	h := TaggedHash("BPU/Test", []byte("abc"))
	if h != TaggedHash("BPU/Test", []byte("abc")) {
		t.Fatal("tagged hash mismatch")
	}
	if h == TaggedHash("BPU/Test", []byte("abd")) {
		t.Fatal("tagged hash should differ")
	}
}

func TestPQLockMatchesSpecTagAndPayload(t *testing.T) {
	verificationKey := make([]byte, MLDSA65VerificationKeySize)
	for i := range verificationKey {
		verificationKey[i] = byte(i)
	}

	want := TaggedHash("BPU/PQLOCK/MLDSA65/v1", verificationKey)
	if got := PQLock(verificationKey); got != want {
		t.Fatalf("PQLock = %x, want %x", got, want)
	}

	legacyPayload := append([]byte{0x01}, verificationKey...)
	if got := PQLock(verificationKey); got == TaggedHash("BPU/PQLOCK/v1", legacyPayload) {
		t.Fatal("PQLock unexpectedly matches legacy algID-prefixed derivation")
	}
}

func TestBIP340ChallengeHashUsesNativeTaggedHash(t *testing.T) {
	r := arrayOf(0x01)
	pubKey := arrayOf(0x02)
	msg := arrayOf(0x03)
	payload := append(append(append([]byte(nil), r[:]...), pubKey[:]...), msg[:]...)
	want := TaggedHash(BIP340ChallengeTag, payload)
	if got := BIP340ChallengeHash(r, pubKey, msg); got != want {
		t.Fatalf("BIP340ChallengeHash = %x, want %x", got, want)
	}
}

func TestSchnorrRoundtrip(t *testing.T) {
	msg := Sha256([]byte("hello"))
	pubKey, sig := RandomSignSchnorrForTest(&msg)
	if !VerifySchnorrXOnly(&pubKey, &sig, &msg) {
		t.Fatal("signature verification failed")
	}
	if !VerifyXOnlySchnorr(pubKey, msg, sig) {
		t.Fatal("value wrapper signature verification failed")
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

func TestSchnorrExactItemsRejectsTamperedSignature(t *testing.T) {
	items := make([]SchnorrBatchItem, 0, 4)
	for i := 0; i < 4; i++ {
		msg := Sha256([]byte{byte(30 + i), byte(40 + i)})
		pubKey, sig := RandomSignSchnorrForTest(&msg)
		items = append(items, SchnorrBatchItem{
			PubKey:    pubKey,
			Signature: sig,
			Msg:       msg,
		})
	}
	if !VerifySchnorrXOnlyItems(items) {
		t.Fatal("valid exact signature set unexpectedly failed")
	}
	items[1].Signature[9] ^= 0x01
	if VerifySchnorrXOnlyItems(items) {
		t.Fatal("tampered exact signature set unexpectedly verified")
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

func BenchmarkTaggedHash(b *testing.B) {
	payload := []byte("benchmark payload for tagged hashing")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = TaggedHash("BPU/mainnet/c1/SigHashV1", payload)
	}
}

func arrayOf(value byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = value
	}
	return out
}
