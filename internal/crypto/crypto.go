package crypto

import (
	"crypto/sha256"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

type Hash32 = [32]byte

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

func KeyHash(pubKey *[32]byte) Hash32 {
	return Sha256(pubKey[:])
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
