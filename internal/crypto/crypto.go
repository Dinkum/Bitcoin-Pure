package crypto

import (
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	secp "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

const (
	BIP340ChallengeTag = "BIP0340/challenge"
)

var ErrInvalidHash32Hex = errors.New("invalid 32-byte hash hex")

// Hash32 is BPU's native protocol identity value. It deliberately keeps string
// and byte forms in the same canonical order instead of inheriting display
// conventions from Bitcoin libraries.
type Hash32 [32]byte

type SchnorrBatchItem struct {
	PubKey    [32]byte
	Signature [64]byte
	Msg       [32]byte
}

type SchnorrBatchResult struct {
	Valid    bool
	Fallback bool
}

var taggedHashCache sync.Map

func Hash32FromArray(raw [32]byte) Hash32 {
	return Hash32(raw)
}

func ParseHash32Hex(raw string) (Hash32, error) {
	if len(raw) != 64 {
		return Hash32{}, ErrInvalidHash32Hex
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return Hash32{}, ErrInvalidHash32Hex
	}
	var out Hash32
	copy(out[:], decoded)
	return out, nil
}

func (h Hash32) Array() [32]byte {
	return [32]byte(h)
}

func (h Hash32) Bytes() []byte {
	out := make([]byte, len(h))
	copy(out, h[:])
	return out
}

func (h Hash32) String() string {
	return hex.EncodeToString(h[:])
}

func Sha256(buf []byte) [32]byte {
	return sha256.Sum256(buf)
}

// Hash256 returns the consensus double-SHA-256 identity hash used for txids,
// authids, block hashes, and other BPU fixed-width commitments.
func Hash256(buf []byte) Hash32 {
	first := Sha256(buf)
	return Hash32(Sha256(first[:]))
}

func Sha256d(buf []byte) [32]byte {
	return Hash256(buf).Array()
}

func TaggedHash32(tag string, payload []byte) Hash32 {
	// Tags are low-cardinality protocol constants, so caching their SHA-256
	// avoids repeating identical work on every tagged hash invocation.
	tagHash := cachedTagHash(tag)
	h := sha256.New()
	h.Write(tagHash[:])
	h.Write(tagHash[:])
	h.Write(payload)
	var out Hash32
	h.Sum(out[:0])
	return out
}

func TaggedHash(tag string, payload []byte) [32]byte {
	return TaggedHash32(tag, payload).Array()
}

func BIP340ChallengeHash(r32 [32]byte, pubKey32 [32]byte, msg32 [32]byte) [32]byte {
	payload := make([]byte, 0, 96)
	payload = append(payload, r32[:]...)
	payload = append(payload, pubKey32[:]...)
	payload = append(payload, msg32[:]...)
	return TaggedHash(BIP340ChallengeTag, payload)
}

func cachedTagHash(tag string) [32]byte {
	if cached, ok := taggedHashCache.Load(tag); ok {
		return cached.([32]byte)
	}
	hash := Sha256([]byte(tag))
	cached, _ := taggedHashCache.LoadOrStore(tag, hash)
	return cached.([32]byte)
}

func IsValidXOnlyPubKey(pubKey *[32]byte) bool {
	_, err := schnorr.ParsePubKey(pubKey[:])
	return err == nil
}

func VerifySchnorrXOnly(pubKey *[32]byte, sig *[64]byte, msg *[32]byte) bool {
	parsedPubKey, err := schnorr.ParsePubKey(pubKey[:])
	if err != nil {
		return false
	}
	parsedSig, err := schnorr.ParseSignature(sig[:])
	if err != nil {
		return false
	}
	return parsedSig.Verify(msg[:], parsedPubKey)
}

func VerifyXOnlySchnorr(pubKey [32]byte, msg [32]byte, sig [64]byte) bool {
	return VerifySchnorrXOnly(&pubKey, &sig, &msg)
}

// VerifySchnorrXOnlyItems verifies each signature independently. This is the
// deterministic verifier consensus code should use for acceptance decisions.
func VerifySchnorrXOnlyItems(items []SchnorrBatchItem) bool {
	for i := range items {
		item := items[i]
		if !VerifySchnorrXOnly(&item.PubKey, &item.Signature, &item.Msg) {
			return false
		}
	}
	return true
}

// VerifySchnorrBatchXOnly performs BIP340-style batch verification across a set
// of signatures. This is a probabilistic accelerator and therefore should only
// be used on non-consensus paths.
func VerifySchnorrBatchXOnly(items []SchnorrBatchItem) bool {
	switch len(items) {
	case 0:
		return true
	case 1:
		item := items[0]
		return VerifySchnorrXOnly(&item.PubKey, &item.Signature, &item.Msg)
	}

	scalars, ok := newBatchScalarSource()
	if !ok {
		return false
	}

	var lhs secp.ModNScalar
	var rhs secp.JacobianPoint
	for i := range items {
		item := items[i]
		pubKey, err := schnorr.ParsePubKey(item.PubKey[:])
		if err != nil {
			return false
		}
		var p secp.JacobianPoint
		pubKey.AsJacobian(&p)

		var rX secp.FieldVal
		if overflow := rX.SetByteSlice(item.Signature[:32]); overflow {
			return false
		}
		var s secp.ModNScalar
		if overflow := s.SetByteSlice(item.Signature[32:]); overflow {
			return false
		}
		var rY secp.FieldVal
		if !secp.DecompressY(&rX, false, &rY) {
			return false
		}
		var r secp.JacobianPoint
		r.X.Set(&rX)
		r.Y.Set(&rY)
		r.Z.SetInt(1)

		var challengeR [32]byte
		copy(challengeR[:], item.Signature[:32])
		commitment := BIP340ChallengeHash(challengeR, item.PubKey, item.Msg)
		var e secp.ModNScalar
		e.SetBytes(&commitment)

		coeff, ok := scalars.scalar(i)
		if !ok {
			return false
		}

		var weightedS secp.ModNScalar
		weightedS.Set(&coeff).Mul(&s)
		lhs.Add(&weightedS)

		var weightedR secp.JacobianPoint
		secp.ScalarMultNonConst(&coeff, &r, &weightedR)

		var weightedE secp.ModNScalar
		weightedE.Set(&coeff).Mul(&e)
		var weightedEP secp.JacobianPoint
		secp.ScalarMultNonConst(&weightedE, &p, &weightedEP)

		var weightedTerm secp.JacobianPoint
		secp.AddNonConst(&weightedR, &weightedEP, &weightedTerm)
		secp.AddNonConst(&rhs, &weightedTerm, &rhs)
	}

	var lhsPoint secp.JacobianPoint
	secp.ScalarBaseMultNonConst(&lhs, &lhsPoint)
	lhsPoint.ToAffine()
	rhs.ToAffine()
	if lhsPoint.Z.IsZero() || rhs.Z.IsZero() {
		return lhsPoint.Z.IsZero() && rhs.Z.IsZero()
	}
	return lhsPoint.X.Equals(&rhs.X) && lhsPoint.Y.Equals(&rhs.Y)
}

// VerifySchnorrBatchXOnlyWithFallback preserves exact behavior by retrying the
// same set with independent verification when the probabilistic batch pass does
// not clear.
func VerifySchnorrBatchXOnlyWithFallback(items []SchnorrBatchItem) bool {
	return VerifySchnorrBatchXOnlyResult(items).Valid
}

// VerifySchnorrBatchXOnlyResult exposes whether batch verification needed the
// exact fallback pass, which helps benchmark and node diagnostics without
// changing acceptance behavior.
func VerifySchnorrBatchXOnlyResult(items []SchnorrBatchItem) SchnorrBatchResult {
	if VerifySchnorrBatchXOnly(items) {
		return SchnorrBatchResult{Valid: true}
	}
	for i := range items {
		item := items[i]
		if !VerifySchnorrXOnly(&item.PubKey, &item.Signature, &item.Msg) {
			return SchnorrBatchResult{Fallback: true}
		}
	}
	return SchnorrBatchResult{Valid: true, Fallback: true}
}

func XOnlyPubKeyFromSecret(secretKey [32]byte) [32]byte {
	privKey, _ := btcec.PrivKeyFromBytes(secretKey[:])
	pubKey := schnorr.SerializePubKey(privKey.PubKey())
	var xonly [32]byte
	copy(xonly[:], pubKey)
	return xonly
}

func SignSchnorr(secretKey [32]byte, msg *[32]byte) ([32]byte, [64]byte) {
	privKey, _ := btcec.PrivKeyFromBytes(secretKey[:])
	return signWithPrivKey(privKey, msg)
}

func SignXOnlySchnorr(secretKey [32]byte, msg [32]byte) ([64]byte, error) {
	privKey, _ := btcec.PrivKeyFromBytes(secretKey[:])
	sig, err := schnorr.Sign(privKey, msg[:])
	if err != nil {
		return [64]byte{}, err
	}
	var encodedSig [64]byte
	copy(encodedSig[:], sig.Serialize())
	return encodedSig, nil
}

func GenerateXOnlySchnorrKey() ([32]byte, [32]byte, error) {
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	pubKey := schnorr.SerializePubKey(privKey.PubKey())
	var xonly [32]byte
	copy(xonly[:], pubKey)
	var secret [32]byte
	copy(secret[:], privKey.Serialize())
	return secret, xonly, nil
}

func SignSchnorrForTest(secretKey [32]byte, msg *[32]byte) ([32]byte, [64]byte) {
	return SignSchnorr(secretKey, msg)
}

func RandomSignSchnorrForTest(msg *[32]byte) ([32]byte, [64]byte) {
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(err)
	}
	return signWithPrivKey(privKey, msg)
}

func signWithPrivKey(privKey *btcec.PrivateKey, msg *[32]byte) ([32]byte, [64]byte) {
	pubKey := schnorr.SerializePubKey(privKey.PubKey())
	sig, err := schnorr.Sign(privKey, msg[:])
	if err != nil {
		panic(err)
	}
	var xonly [32]byte
	var encodedSig [64]byte
	copy(xonly[:], pubKey)
	copy(encodedSig[:], sig.Serialize())
	return xonly, encodedSig
}

type batchScalarSource struct {
	seed [32]byte
}

func newBatchScalarSource() (batchScalarSource, bool) {
	var source batchScalarSource
	if _, err := crand.Read(source.seed[:]); err != nil {
		return batchScalarSource{}, false
	}
	return source, true
}

func (s batchScalarSource) scalar(index int) (secp.ModNScalar, bool) {
	var coeff secp.ModNScalar
	if index == 0 {
		coeff.SetInt(1)
		return coeff, true
	}

	// Batch verification only needs independent non-zero coefficients. Expand one
	// cryptographic seed into per-item candidates to avoid a syscall per item.
	var material [40]byte
	copy(material[:32], s.seed[:])
	binary.LittleEndian.PutUint32(material[32:36], uint32(index))
	for attempts := 0; attempts < 8; attempts++ {
		binary.LittleEndian.PutUint32(material[36:], uint32(attempts))
		candidate := Sha256(material[:])
		coeff.SetBytes(&candidate)
		if !coeff.IsZero() {
			return coeff, true
		}
	}
	return secp.ModNScalar{}, false
}
