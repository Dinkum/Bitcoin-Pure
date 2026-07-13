package utxochecksum

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	secp "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

const leafTag = "BPU/UTXOChecksumLeafV1"

var (
	leafTagHash = sha256.Sum256([]byte(leafTag))
	// leafReductionThreshold is p-1 for the secp256k1 field prime p. The leaf
	// mapping is SHA256(tag||tag||payload) mod (p-1) + 1.
	leafReductionThreshold = [32]byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xfc, 0x2e,
	}
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
	if len(spent) != 0 {
		// Invert the product once instead of performing one expensive field
		// inversion per spent coin. Multiplication is commutative, so this is
		// exactly equivalent to multiplying each individual inverse.
		var product secp.FieldVal
		product.SetInt(1)
		for outPoint, entry := range spent {
			leaf := leafElement(outPoint, entry)
			product.Mul(&leaf)
		}
		product.Inverse()
		acc.value.Mul(&product)
	}
	for outPoint, entry := range created {
		acc.add(outPoint, entry)
	}
	return acc.digest()
}

type accumulator struct {
	value secp.FieldVal
}

func newAccumulator() accumulator {
	var acc accumulator
	acc.value.SetInt(1)
	return acc
}

func accumulatorFromDigest(digest [32]byte) accumulator {
	var acc accumulator
	if digest == ([32]byte{}) {
		acc.value.SetInt(1)
		return acc
	}
	if overflow := acc.value.SetByteSlice(digest[:]); overflow {
		acc.value.Normalize()
		if acc.value.IsZero() {
			acc.value.SetInt(1)
		}
	}
	return acc
}

func (a *accumulator) add(outPoint types.OutPoint, entry consensus.UtxoEntry) {
	leaf := leafElement(outPoint, entry)
	a.value.Mul(&leaf)
}

func (a *accumulator) digest() [32]byte {
	var out [32]byte
	a.value.Normalize().PutBytes(&out)
	return out
}

func leafElement(outPoint types.OutPoint, entry consensus.UtxoEntry) secp.FieldVal {
	// The largest canonical leaf payload is 85 bytes. Keeping both the tagged
	// prefix and payload in this fixed array lets sha256.Sum256 stay allocation
	// free on the block-application hot path.
	var tagged [64 + 32 + 4 + 9 + 8 + 32]byte
	copy(tagged[:32], leafTagHash[:])
	copy(tagged[32:64], leafTagHash[:])
	offset := 64
	copy(tagged[offset:offset+32], outPoint.TxID[:])
	offset += 32
	binary.LittleEndian.PutUint32(tagged[offset:offset+4], outPoint.Vout)
	offset += 4
	offset += putCanonicalVarInt(tagged[offset:], entry.Type)
	binary.LittleEndian.PutUint64(tagged[offset:offset+8], entry.ValueAtoms)
	offset += 8
	payload32 := entry.Payload32
	if payload32 == ([32]byte{}) && entry.Type == types.OutputXOnlyP2PK {
		payload32 = entry.PubKey
	}
	copy(tagged[offset:offset+32], payload32[:])
	offset += 32
	hash := sha256.Sum256(tagged[:offset])
	return fieldElementFromHash(hash)
}

func fieldElementFromHash(hash [32]byte) secp.FieldVal {
	var value secp.FieldVal
	if bytes.Compare(hash[:], leafReductionThreshold[:]) >= 0 {
		// Both values share the first 24 all-ones bytes in this extremely rare
		// branch, so the exact hash-(p-1)+1 result fits in 64 bits.
		reduced := binary.BigEndian.Uint64(hash[24:]) - binary.BigEndian.Uint64(leafReductionThreshold[24:]) + 1
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], reduced)
		value.SetByteSlice(encoded[:])
		return value
	}
	value.SetByteSlice(hash[:])
	value.AddInt(1)
	return value
}

func putCanonicalVarInt(dst []byte, value uint64) int {
	switch {
	case value < 0xfd:
		dst[0] = byte(value)
		return 1
	case value <= 0xffff:
		dst[0] = 0xfd
		binary.LittleEndian.PutUint16(dst[1:3], uint16(value))
		return 3
	case value <= 0xffff_ffff:
		dst[0] = 0xfe
		binary.LittleEndian.PutUint32(dst[1:5], uint32(value))
		return 5
	default:
		dst[0] = 0xff
		binary.LittleEndian.PutUint64(dst[1:9], value)
		return 9
	}
}
