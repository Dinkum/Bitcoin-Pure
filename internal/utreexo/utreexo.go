package utreexo

import (
	"cmp"
	"slices"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

const (
	UTXOLeafTag = "BPU/UtxoLeafV1"
	UTXORootTag = "BPU/UtxoRootV1"
)

type UtxoLeaf struct {
	OutPoint   types.OutPoint
	ValueAtoms uint64
	KeyHash    [32]byte
}

func writeVarInt(out *[]byte, v uint64) {
	switch {
	case v <= 0xfc:
		*out = append(*out, byte(v))
	case v <= 0xffff:
		*out = append(*out, 0xfd, byte(v), byte(v>>8))
	case v <= 0xffff_ffff:
		*out = append(*out, 0xfe, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	default:
		*out = append(*out, 0xff,
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

func hashPair(left, right *[32]byte) [32]byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, left[:]...)
	buf = append(buf, right[:]...)
	return crypto.Sha256d(buf)
}

func LeafHash(leaf UtxoLeaf) [32]byte {
	buf := make([]byte, 0, 76)
	buf = append(buf, leaf.OutPoint.TxID[:]...)
	buf = append(buf,
		byte(leaf.OutPoint.Vout),
		byte(leaf.OutPoint.Vout>>8),
		byte(leaf.OutPoint.Vout>>16),
		byte(leaf.OutPoint.Vout>>24),
	)
	buf = append(buf,
		byte(leaf.ValueAtoms),
		byte(leaf.ValueAtoms>>8),
		byte(leaf.ValueAtoms>>16),
		byte(leaf.ValueAtoms>>24),
		byte(leaf.ValueAtoms>>32),
		byte(leaf.ValueAtoms>>40),
		byte(leaf.ValueAtoms>>48),
		byte(leaf.ValueAtoms>>56),
	)
	buf = append(buf, leaf.KeyHash[:]...)
	return crypto.TaggedHash(UTXOLeafTag, buf)
}

func ForestPeaks(leaves []UtxoLeaf) [][32]byte {
	sorted := append([]UtxoLeaf(nil), leaves...)
	slices.SortFunc(sorted, func(a, b UtxoLeaf) int {
		if n := slices.Compare(a.OutPoint.TxID[:], b.OutPoint.TxID[:]); n != 0 {
			return n
		}
		return cmp.Compare(a.OutPoint.Vout, b.OutPoint.Vout)
	})

	var peaks []*[32]byte
	for _, leaf := range sorted {
		carry := LeafHash(leaf)
		level := 0
		for {
			if level == len(peaks) {
				peaks = append(peaks, nil)
			}
			if peaks[level] == nil {
				copyCarry := carry
				peaks[level] = &copyCarry
				break
			}
			next := hashPair(peaks[level], &carry)
			peaks[level] = nil
			carry = next
			level++
		}
	}

	out := make([][32]byte, 0, len(peaks))
	for _, peak := range peaks {
		if peak != nil {
			out = append(out, *peak)
		}
	}
	return out
}

func UtxoRoot(leaves []UtxoLeaf) [32]byte {
	peaks := ForestPeaks(leaves)
	buf := make([]byte, 0, 1+len(peaks)*32)
	writeVarInt(&buf, uint64(len(peaks)))
	for _, peak := range peaks {
		buf = append(buf, peak[:]...)
	}
	return crypto.TaggedHash(UTXORootTag, buf)
}
