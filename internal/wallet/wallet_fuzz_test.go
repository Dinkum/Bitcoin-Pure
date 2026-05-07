package wallet

import "testing"

func FuzzParseAmountRoundTrip(f *testing.F) {
	for _, seed := range []string{
		"1",
		"1 BPU",
		"1.25",
		"0.000000001 BPU",
		"42 atoms",
		"18446744073.709551615",
		"",
		"-1",
		"0.0000000001",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		atoms, err := ParseAmount(raw)
		if err != nil {
			return
		}
		if atoms == 0 {
			t.Fatalf("ParseAmount(%q) returned zero without error", raw)
		}
		roundTrip, err := ParseAmount(FormatAmount(atoms))
		if err != nil {
			t.Fatalf("ParseAmount(FormatAmount(%d)): %v", atoms, err)
		}
		if roundTrip != atoms {
			t.Fatalf("amount round trip = %d, want %d", roundTrip, atoms)
		}
	})
}

func FuzzParseAddressRoundTrip(f *testing.F) {
	var pubKey [32]byte
	for i := range pubKey {
		pubKey[i] = byte(i)
	}
	f.Add(EncodeAddress(pubKey))
	f.Add("bpu:")
	f.Add("BPU:" + EncodeAddress(pubKey)[4:])
	f.Add("not-an-address")

	f.Fuzz(func(t *testing.T, raw string) {
		payload, err := ParseAddress(raw)
		if err != nil {
			return
		}
		roundTrip, err := ParseAddress(EncodeAddress(payload))
		if err != nil {
			t.Fatalf("ParseAddress(EncodeAddress(...)): %v", err)
		}
		if roundTrip != payload {
			t.Fatal("address payload round trip mismatch")
		}
	})
}
