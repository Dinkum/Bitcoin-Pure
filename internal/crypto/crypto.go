package crypto

import (
	crand "crypto/rand"
	"crypto/sha256"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	secp "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

type Hash32 = [32]byte

type SchnorrBatchItem struct {
	PubKey    [32]byte
	Signature [64]byte
	Msg       [32]byte
}

type SchnorrBatchResult struct {
	Valid    bool
	Fallback bool
}

func Sha256(buf []byte) Hash32 {
	return sha256.Sum256(buf)
}

func Sha256d(buf []byte) Hash32 {
	first := Sha256(buf)
	return Sha256(first[:])
}

func TaggedHash(tag string, payload []byte) Hash32 {
	tagHash := Sha256([]byte(tag))
	h := sha256.New()
	h.Write(tagHash[:])
	h.Write(tagHash[:])
	h.Write(payload)
	var out Hash32
	copy(out[:], h.Sum(nil))
	return out
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

		commitment := chainhash.TaggedHash(
			chainhash.TagBIP0340Challenge,
			item.Signature[:32],
			item.PubKey[:],
			item.Msg[:],
		)
		var e secp.ModNScalar
		e.SetBytes((*[32]byte)(commitment))

		coeff, ok := randomBatchScalar(i)
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

func randomBatchScalar(index int) (secp.ModNScalar, bool) {
	var coeff secp.ModNScalar
	if index == 0 {
		coeff.SetInt(1)
		return coeff, true
	}
	for attempts := 0; attempts < 8; attempts++ {
		var buf [32]byte
		if _, err := crand.Read(buf[:]); err != nil {
			return secp.ModNScalar{}, false
		}
		coeff.SetBytes(&buf)
		if !coeff.IsZero() {
			return coeff, true
		}
	}
	return secp.ModNScalar{}, false
}
