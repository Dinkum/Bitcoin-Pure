package utxochecksum

import (
	"encoding/binary"
	"math/big"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

const leafTag = "BPU/UTXOChecksumLeafV1"

var (
	// Use the secp256k1 field prime as a convenient 256-bit prime modulus for a
	// non-consensus commutative set checksum. This keeps the digest compact while
	// still supporting exact modular inverses for removals.
	modulus    = mustParseBigInt("fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f")
	modulusM1  = new(big.Int).Sub(new(big.Int).Set(modulus), big.NewInt(1))
	emptyValue = big.NewInt(1)
)

// Compute returns an order-independent UTXO checksum for the provided set.
func Compute(utxos consensus.UtxoSet) [32]byte {
	acc := newAccumulator()
	for outPoint, entry := range utxos {
		acc.add(outPoint, entry)
	}
	return acc.digest()
}

// ComputeFromStore computes the order-independent checksum by scanning a
// store-backed UTXO source without materializing the full set in the caller.
func ComputeFromStore(store interface {
	ForEachUTXO(func(types.OutPoint, consensus.UtxoEntry) error) error
}) ([32]byte, error) {
	acc := newAccumulator()
	if err := store.ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		acc.add(outPoint, entry)
		return nil
	}); err != nil {
		return [32]byte{}, err
	}
	return acc.digest(), nil
}

// ApplyDelta incrementally updates a checksum by removing spent entries and
// adding created entries. Callers should only remove entries that existed in
// the pre-update committed set.
func ApplyDelta(current [32]byte, spent map[types.OutPoint]consensus.UtxoEntry, created map[types.OutPoint]consensus.UtxoEntry) [32]byte {
	acc := accumulatorFromDigest(current)
	for outPoint, entry := range spent {
		acc.remove(outPoint, entry)
	}
	for outPoint, entry := range created {
		acc.add(outPoint, entry)
	}
	return acc.digest()
}

type accumulator struct {
	value *big.Int
}

func newAccumulator() accumulator {
	return accumulator{value: new(big.Int).Set(emptyValue)}
}

func accumulatorFromDigest(digest [32]byte) accumulator {
	value := new(big.Int).SetBytes(digest[:])
	if value.Sign() == 0 {
		value.Set(emptyValue)
	}
	value.Mod(value, modulus)
	if value.Sign() == 0 {
		value.Set(emptyValue)
	}
	return accumulator{value: value}
}

func (a accumulator) add(outPoint types.OutPoint, entry consensus.UtxoEntry) {
	a.value.Mul(a.value, leafElement(outPoint, entry))
	a.value.Mod(a.value, modulus)
}

func (a accumulator) remove(outPoint types.OutPoint, entry consensus.UtxoEntry) {
	inverse := new(big.Int).ModInverse(leafElement(outPoint, entry), modulus)
	if inverse == nil {
		panic("utxo checksum leaf element has no inverse")
	}
	a.value.Mul(a.value, inverse)
	a.value.Mod(a.value, modulus)
}

func (a accumulator) digest() [32]byte {
	var out [32]byte
	buf := a.value.Bytes()
	copy(out[len(out)-len(buf):], buf)
	return out
}

func leafElement(outPoint types.OutPoint, entry consensus.UtxoEntry) *big.Int {
	payload := encodeLeaf(outPoint, entry)
	hash := crypto.TaggedHash(leafTag, payload)
	value := new(big.Int).SetBytes(hash[:])
	value.Mod(value, modulusM1)
	value.Add(value, big.NewInt(1))
	return value
}

func encodeLeaf(outPoint types.OutPoint, entry consensus.UtxoEntry) []byte {
	buf := make([]byte, 0, 32+4+9+8+32)
	buf = append(buf, outPoint.TxID[:]...)
	var vout [4]byte
	binary.LittleEndian.PutUint32(vout[:], outPoint.Vout)
	buf = append(buf, vout[:]...)
	buf = appendCanonicalVarInt(buf, entry.Type)
	var value [8]byte
	binary.LittleEndian.PutUint64(value[:], entry.ValueAtoms)
	buf = append(buf, value[:]...)
	payload32 := entry.Payload32
	if payload32 == ([32]byte{}) && entry.Type == types.OutputXOnlyP2PK {
		payload32 = entry.PubKey
	}
	buf = append(buf, payload32[:]...)
	return buf
}

func mustParseBigInt(hex string) *big.Int {
	v, ok := new(big.Int).SetString(hex, 16)
	if !ok {
		panic("invalid big integer constant")
	}
	return v
}

func appendCanonicalVarInt(dst []byte, v uint64) []byte {
	switch {
	case v <= 0xfc:
		return append(dst, byte(v))
	case v <= 0xffff:
		return append(dst, 0xfd, byte(v), byte(v>>8))
	case v <= 0xffff_ffff:
		return append(dst, 0xfe, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	default:
		return append(dst, 0xff,
			byte(v),
			byte(v>>8),
			byte(v>>16),
			byte(v>>24),
			byte(v>>32),
			byte(v>>40),
			byte(v>>48),
			byte(v>>56),
		)
	}
}
