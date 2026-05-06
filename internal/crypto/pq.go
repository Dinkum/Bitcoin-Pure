package crypto

import (
	"errors"
	"io"

	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
)

const PQLockTag = "BPU/PQLOCK/MLDSA65/v1"

const (
	MLDSA65VerificationKeySize = mldsa65.PublicKeySize
	MLDSA65PrivateKeySize      = mldsa65.PrivateKeySize
	MLDSA65SignatureSize       = mldsa65.SignatureSize
)

var (
	ErrInvalidMLDSA65VerificationKey = errors.New("invalid ML-DSA-65 verification key")
	ErrInvalidMLDSA65PrivateKey      = errors.New("invalid ML-DSA-65 private key")
)

// PQLock derives the 32-byte consensus lock committed by PQ outputs.
func PQLock(verificationKey []byte) [32]byte {
	return TaggedHash(PQLockTag, verificationKey)
}

func GenerateMLDSA65Key(rand io.Reader) ([]byte, []byte, error) {
	pubKey, privKey, err := mldsa65.GenerateKey(rand)
	if err != nil {
		return nil, nil, err
	}
	return pubKey.Bytes(), privKey.Bytes(), nil
}

func SignMLDSA65(privateKey []byte, msg []byte) ([]byte, error) {
	if len(privateKey) != MLDSA65PrivateKeySize {
		return nil, ErrInvalidMLDSA65PrivateKey
	}
	var parsed mldsa65.PrivateKey
	if err := parsed.UnmarshalBinary(privateKey); err != nil {
		return nil, err
	}
	signature := make([]byte, MLDSA65SignatureSize)
	if err := mldsa65.SignTo(&parsed, msg, nil, false, signature); err != nil {
		return nil, err
	}
	return signature, nil
}

func VerifyMLDSA65(verificationKey []byte, signature []byte, msg []byte) bool {
	if len(verificationKey) != MLDSA65VerificationKeySize || len(signature) != MLDSA65SignatureSize {
		return false
	}
	var parsed mldsa65.PublicKey
	if err := parsed.UnmarshalBinary(verificationKey); err != nil {
		return false
	}
	return mldsa65.Verify(&parsed, msg, nil, signature)
}
