package utreexo

import (
	"encoding/binary"
	"testing"

	"bitcoin-pure/internal/types"
)

func benchmarkAccumulatorLeaf(index uint64) UtxoLeaf {
	var txid [32]byte
	binary.LittleEndian.PutUint64(txid[:8], index*0x9e3779b97f4a7c15)
	binary.LittleEndian.PutUint64(txid[8:16], index^0xd1b54a32d192ed03)
	binary.LittleEndian.PutUint64(txid[16:24], index*0x94d049bb133111eb)
	binary.LittleEndian.PutUint64(txid[24:32], index^0x8538ecb5bd456ea3)
	return UtxoLeaf{
		OutPoint:   types.OutPoint{TxID: txid, Vout: uint32(index)},
		Type:       types.OutputXOnlyP2PK,
		ValueAtoms: index + 1,
		Payload32:  txid,
	}
}

func BenchmarkAccumulatorBuild10K(b *testing.B) {
	leaves := make([]UtxoLeaf, 10_000)
	for i := range leaves {
		leaves[i] = benchmarkAccumulatorLeaf(uint64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := NewAccumulatorFromLeaves(leaves); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulatorSteadyCycle measures a 10,000-leaf committed state with
// one 1,024-spend/1,024-create transition.
func BenchmarkAccumulatorSteadyCycle(b *testing.B) {
	const (
		resident = 10_000
		batch    = 1_024
	)
	leaves := make([]UtxoLeaf, resident)
	for i := range leaves {
		leaves[i] = benchmarkAccumulatorLeaf(uint64(i))
	}
	base, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		b.Fatal(err)
	}
	spent := make([]types.OutPoint, batch)
	created := make([]UtxoLeaf, batch)
	for i := range spent {
		spent[i] = leaves[i].OutPoint
		created[i] = benchmarkAccumulatorLeaf(uint64(resident + i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := base.Apply(spent, created); err != nil {
			b.Fatal(err)
		}
	}
}
