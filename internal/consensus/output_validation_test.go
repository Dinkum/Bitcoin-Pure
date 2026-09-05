package consensus

import (
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"
)

func newOutputValidationCache() func(types.TxOutput) error {
	var cache outputKeyCache
	return func(output types.TxOutput) error { return validateOutputPayloadWithKeyCache(output, &cache) }
}
func TestOutputKeyCacheMatchesUncachedValidation(t *testing.T) {
	validate := newOutputValidationCache()
	var sk [32]byte
	sk[31] = 1
	key := crypto.XOnlyPubKeyFromSecret(sk)
	cases := []types.TxOutput{types.NewXOnlyOutput(1, key), types.NewXOnlyOutput(0, key), {Type: 99, ValueAtoms: 1}, {Type: types.OutputPQLock32, ValueAtoms: 1}, {ValueAtoms: 1, Payload32: key, PubKey: [32]byte{1}}}
	for i := 0; i < 1024; i++ {
		var invalid [32]byte
		binary.BigEndian.PutUint32(invalid[28:], uint32(i))
		cases = append(cases, types.NewXOnlyOutput(1, invalid))
	}
	for repeat := 0; repeat < 2; repeat++ {
		for i, out := range cases {
			a, b := validateOutputPayload(out), validate(out)
			if (a == nil) != (b == nil) || a != nil && a.Error() != b.Error() {
				t.Fatalf("case %d error mismatch", i)
			}
		}
	}
}

func TestOutputKeyCacheBoundsAndCollisions(t *testing.T) {
	var c outputKeyCache
	if unsafe.Sizeof(c) != 8448 {
		t.Fatalf("cache size %d", unsafe.Sizeof(c))
	}
	var valid []types.TxOutput
	for n := uint64(1); len(valid) < 1024; n++ {
		var key [32]byte
		binary.BigEndian.PutUint64(key[24:], n)
		out := types.NewXOnlyOutput(1, key)
		if validateOutputPayload(out) == nil {
			valid = append(valid, out)
		}
	}
	// Every key lands in slot zero. Alternate different valid keys, malformed
	// aliases, zero amounts, invalid curve points and PQ outputs after a hit.
	cases := append([]types.TxOutput{}, valid...)
	for _, out := range valid {
		cases = append(cases, out)
		bad := out
		bad.ValueAtoms = 0
		cases = append(cases, bad)
		bad = out
		bad.PubKey = [32]byte{1}
		cases = append(cases, bad)
		cases = append(cases, types.NewXOnlyOutput(1, [32]byte{}), types.NewPQLockOutput(1, out.Payload32))
	}
	for _, out := range cases {
		a, b := validateOutputPayload(out), validateOutputPayloadWithKeyCache(out, &c)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("mismatch %v %v", a, b)
		}
	}
	var fresh outputKeyCache
	for _, used := range fresh.used {
		if used {
			t.Fatal("cross-block state")
		}
	}
	t.Logf("%d checks, 1024 valid colliding keys, %d fixed bytes", len(cases), unsafe.Sizeof(c))
}
func FuzzOutputKeyCacheEquivalence(f *testing.F) {
	key := consensusTestPubKey(7)
	seed := make([]byte, 66)
	copy(seed[:32], key[:])
	copy(seed[32:64], key[:])
	f.Add(seed)
	f.Add(make([]byte, 66))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) != 66 {
			return
		}
		var a, b [32]byte
		copy(a[:], data[:32])
		copy(b[:], data[32:64])
		var c outputKeyCache
		// Warm, collide, repeat, then vary the fields that must be checked on hits.
		outputs := []types.TxOutput{types.NewXOnlyOutput(1, a), types.NewXOnlyOutput(1, b), types.NewXOnlyOutput(1, a), types.NewXOnlyOutput(1, b)}
		out := types.NewXOnlyOutput(uint64(data[64]&1), a)
		out.Type = uint64(data[65])
		out.PubKey = b
		outputs = append(outputs, out, out)
		for _, out := range outputs {
			want, got := validateOutputPayload(out), validateOutputPayloadWithKeyCache(out, &c)
			if fmt.Sprint(want) != fmt.Sprint(got) {
				t.Fatalf("error differs %v / %v", want, got)
			}
		}
	})
}
