package utxochecksum

import (
	"encoding/binary"
	"math/big"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func TestComputeIsOrderIndependent(t *testing.T) {
	left := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
	}
	right := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
	}
	if got, want := Compute(left), Compute(right); got != want {
		t.Fatalf("checksum mismatch: got %x want %x", got, want)
	}
}

func TestApplyDeltaMatchesFullRecompute(t *testing.T) {
	base := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
	}
	spent := map[types.OutPoint]consensus.UtxoEntry{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
	}
	created := map[types.OutPoint]consensus.UtxoEntry{
		types.OutPoint{TxID: [32]byte{9}, Vout: 2}: {ValueAtoms: 99, PubKey: [32]byte{8}},
	}
	next := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
		types.OutPoint{TxID: [32]byte{9}, Vout: 2}: {ValueAtoms: 99, PubKey: [32]byte{8}},
	}
	if got, want := ApplyDelta(Compute(base), spent, created), Compute(next); got != want {
		t.Fatalf("delta checksum mismatch: got %x want %x", got, want)
	}
}

type fakeUTXOStore struct {
	utxos consensus.UtxoSet
}

func (f fakeUTXOStore) ForEachUTXO(fn func(types.OutPoint, consensus.UtxoEntry) error) error {
	for outPoint, entry := range f.utxos {
		if err := fn(outPoint, entry); err != nil {
			return err
		}
	}
	return nil
}

func TestComputeFromStoreMatchesCompute(t *testing.T) {
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: [32]byte{1}, Vout: 0}: {ValueAtoms: 10, PubKey: [32]byte{2}},
		types.OutPoint{TxID: [32]byte{3}, Vout: 1}: {ValueAtoms: 20, PubKey: [32]byte{4}},
	}
	got, err := ComputeFromStore(fakeUTXOStore{utxos: utxos})
	if err != nil {
		t.Fatalf("ComputeFromStore: %v", err)
	}
	if want := Compute(utxos); got != want {
		t.Fatalf("store checksum mismatch: got %x want %x", got, want)
	}
}

func TestFixedFieldMatchesLegacyChecksum(t *testing.T) {
	base := make(consensus.UtxoSet, 4096)
	spent := make(map[types.OutPoint]consensus.UtxoEntry, 1024)
	created := make(map[types.OutPoint]consensus.UtxoEntry, 1024)
	for i := 0; i < 4096; i++ {
		outPoint, entry := checksumFixtureCoin(uint64(i))
		base[outPoint] = entry
		if i < 1024 {
			spent[outPoint] = entry
			createdOutPoint, createdEntry := checksumFixtureCoin(uint64(i + 10_000))
			created[createdOutPoint] = createdEntry
		}
	}
	legacyBase := legacyCompute(base)
	if got := Compute(base); got != legacyBase {
		t.Fatalf("fixed-field checksum = %x, legacy = %x", got, legacyBase)
	}
	if got, want := ApplyDelta(legacyBase, spent, created), legacyApplyDelta(legacyBase, spent, created); got != want {
		t.Fatalf("fixed-field delta = %x, legacy = %x", got, want)
	}
}

func TestFieldElementHashReductionBoundaries(t *testing.T) {
	modulus, ok := new(big.Int).SetString("fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f", 16)
	if !ok {
		t.Fatal("parse legacy modulus")
	}
	modulusM1 := new(big.Int).Sub(new(big.Int).Set(modulus), big.NewInt(1))
	threshold := new(big.Int).SetBytes(leafReductionThreshold[:])
	vectors := []*big.Int{
		big.NewInt(0),
		new(big.Int).Sub(new(big.Int).Set(threshold), big.NewInt(1)),
		new(big.Int).Set(threshold),
		new(big.Int).Add(new(big.Int).Set(threshold), big.NewInt(1)),
		new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)),
	}
	for _, vector := range vectors {
		var hash [32]byte
		vector.FillBytes(hash[:])
		field := fieldElementFromHash(hash)
		var got [32]byte
		field.Normalize().PutBytes(&got)
		wantInt := new(big.Int).Mod(new(big.Int).Set(vector), modulusM1)
		wantInt.Add(wantInt, big.NewInt(1))
		var want [32]byte
		wantInt.FillBytes(want[:])
		if got != want {
			t.Fatalf("hash %x reduced to %x, want %x", hash, got, want)
		}
	}
}

func TestLeafElementMatchesLegacyAcrossCanonicalVarIntWidths(t *testing.T) {
	outPoint := types.OutPoint{TxID: [32]byte{1, 2, 3}, Vout: 7}
	for _, outputType := range []uint64{0, 0xfc, 0xfd, 0xffff, 0x1_0000, 0xffff_ffff, 0x1_0000_0000, ^uint64(0)} {
		entry := consensus.UtxoEntry{Type: outputType, ValueAtoms: 42, Payload32: [32]byte{9, 8, 7}}
		field := leafElement(outPoint, entry)
		var got [32]byte
		field.Normalize().PutBytes(&got)
		var want [32]byte
		legacyLeafElement(outPoint, entry).FillBytes(want[:])
		if got != want {
			t.Fatalf("type %d leaf element = %x, legacy = %x", outputType, got, want)
		}
	}
}

func checksumFixtureCoin(index uint64) (types.OutPoint, consensus.UtxoEntry) {
	var txid [32]byte
	binary.LittleEndian.PutUint64(txid[:8], index+1)
	binary.LittleEndian.PutUint64(txid[8:16], (index+1)*0x9e3779b97f4a7c15)
	entry := consensus.UtxoEntry{
		Type:       index % 5,
		ValueAtoms: index + 1,
		Payload32:  txid,
	}
	return types.OutPoint{TxID: txid, Vout: uint32(index % 4)}, entry
}

func legacyCompute(utxos consensus.UtxoSet) [32]byte {
	value := big.NewInt(1)
	modulus := legacyChecksumModulus()
	for outPoint, entry := range utxos {
		value.Mul(value, legacyLeafElement(outPoint, entry))
		value.Mod(value, modulus)
	}
	return bigIntDigest(value)
}

func legacyApplyDelta(current [32]byte, spent map[types.OutPoint]consensus.UtxoEntry, created map[types.OutPoint]consensus.UtxoEntry) [32]byte {
	modulus := legacyChecksumModulus()
	value := new(big.Int).SetBytes(current[:])
	for outPoint, entry := range spent {
		value.Mul(value, new(big.Int).ModInverse(legacyLeafElement(outPoint, entry), modulus))
		value.Mod(value, modulus)
	}
	for outPoint, entry := range created {
		value.Mul(value, legacyLeafElement(outPoint, entry))
		value.Mod(value, modulus)
	}
	return bigIntDigest(value)
}

func legacyLeafElement(outPoint types.OutPoint, entry consensus.UtxoEntry) *big.Int {
	payload := make([]byte, 0, 32+4+9+8+32)
	payload = append(payload, outPoint.TxID[:]...)
	var vout [4]byte
	binary.LittleEndian.PutUint32(vout[:], outPoint.Vout)
	payload = append(payload, vout[:]...)
	payload = types.AppendCanonicalVarInt(payload, entry.Type)
	var value [8]byte
	binary.LittleEndian.PutUint64(value[:], entry.ValueAtoms)
	payload = append(payload, value[:]...)
	payload32 := entry.Payload32
	if payload32 == ([32]byte{}) && entry.Type == types.OutputXOnlyP2PK {
		payload32 = entry.PubKey
	}
	payload = append(payload, payload32[:]...)
	hash := crypto.TaggedHash(leafTag, payload)
	modulusM1 := new(big.Int).Sub(legacyChecksumModulus(), big.NewInt(1))
	valueInt := new(big.Int).SetBytes(hash[:])
	valueInt.Mod(valueInt, modulusM1)
	return valueInt.Add(valueInt, big.NewInt(1))
}

func legacyChecksumModulus() *big.Int {
	modulus, ok := new(big.Int).SetString("fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f", 16)
	if !ok {
		panic("invalid test checksum modulus")
	}
	return modulus
}

func bigIntDigest(value *big.Int) [32]byte {
	var out [32]byte
	value.FillBytes(out[:])
	return out
}
