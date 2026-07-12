package consensus

import (
	"bitcoin-pure/internal/types"
	"errors"
	"testing"
)

func TestSubsidyAtomsMatchesDecadeHalvingSchedule(t *testing.T) {
	params := MainnetParams()
	if params.HalvingInterval != 525_600 {
		t.Fatalf("halving interval = %d, want 525600", params.HalvingInterval)
	}
	if got := SubsidyAtoms(0, params); got != 5_000*atomsPerBPU {
		t.Fatalf("genesis subsidy = %d, want %d", got, uint64(5_000*atomsPerBPU))
	}
	if got := SubsidyAtoms(params.HalvingInterval, params); got != 2_500*atomsPerBPU {
		t.Fatalf("first halving subsidy = %d, want %d", got, uint64(2_500*atomsPerBPU))
	}
	if got := SubsidyAtoms(params.HalvingInterval*12, params); got != 1_220_703_125 {
		t.Fatalf("twelfth halving subsidy = %d, want 1220703125", got)
	}
	if got := SubsidyAtoms(params.HalvingInterval*13, params); got != atomsPerBPU {
		t.Fatalf("tail-emission subsidy = %d, want %d", got, uint64(atomsPerBPU))
	}
	if got := SubsidyAtoms(params.HalvingInterval*80, params); got != atomsPerBPU {
		t.Fatalf("far-future subsidy = %d, want %d", got, uint64(atomsPerBPU))
	}
}

func TestMineHeaderInterruptibleStopsWhenTemplateTurnsStale(t *testing.T) {
	params := RegtestParams()
	header := types.BlockHeader{NBits: params.GenesisBits}
	mined, ok, err := MineHeaderInterruptible(header, params, func(uint64) bool { return false })
	if err != nil {
		t.Fatalf("MineHeaderInterruptible: %v", err)
	}
	if ok {
		t.Fatal("expected interruptible mining to stop before finding work")
	}
	if mined != (types.BlockHeader{}) {
		t.Fatalf("interrupted header = %+v, want zero header", mined)
	}
}

func TestValidateHeaderAcceptsValidRegtestHeader(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	nbits, err := NextWorkRequired(PrevBlockContext{Height: 0, Header: prev}, params)
	if err != nil {
		t.Fatal(err)
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     prev.Timestamp + 600,
		NBits:         nbits,
	}
	header = mineHeaderForTest(header)
	if err := ValidateHeader(&header, PrevBlockContext{Height: 0, Header: prev}, params); err != nil {
		t.Fatalf("validate header: %v", err)
	}
}

func TestValidateHeaderRejectsPrevHashMismatch(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: [32]byte{9},
		Timestamp:     prev.Timestamp + 600,
		NBits:         params.GenesisBits,
	}
	header = mineHeaderForTest(header)
	err := ValidateHeader(&header, PrevBlockContext{Height: 0, Header: prev}, params)
	if !errors.Is(err, ErrPrevHashMismatch) {
		t.Fatalf("expected prev hash mismatch, got %v", err)
	}
}

func TestValidateHeaderRejectsTimestampAtOrBelowMedianTimePast(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     100,
		NBits:         params.GenesisBits,
	}
	err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: 100,
	}, params)
	if !errors.Is(err, ErrTimestampTooEarly) {
		t.Fatalf("expected timestamp-too-early error, got %v", err)
	}
}

func TestValidateHeaderAcceptsTimestampAboveMedianTimePast(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     101,
		NBits:         params.GenesisBits,
	}
	header = mineHeaderForTest(header)
	if err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: 100,
		CurrentTime:    101,
	}, params); err != nil {
		t.Fatalf("ValidateHeader: %v", err)
	}
}

func TestValidateHeaderRejectsTimestampBeyondLocalSystemTimeWindow(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	currentTime := params.GenesisTimestamp + 100
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     currentTime + MaxFutureBlockTimeSeconds + 1,
		NBits:         params.GenesisBits,
	}
	err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: params.GenesisTimestamp,
		CurrentTime:    currentTime,
	}, params)
	if !errors.Is(err, ErrTimestampTooFarFuture) {
		t.Fatalf("expected timestamp-too-far-future error, got %v", err)
	}
}

func TestValidateHeaderAcceptsTimestampAtLocalSystemTimeWindowBoundary(t *testing.T) {
	params := RegtestParams()
	prev := types.BlockHeader{
		Version:   1,
		Timestamp: params.GenesisTimestamp,
		NBits:     params.GenesisBits,
	}
	currentTime := params.GenesisTimestamp + 100
	header := types.BlockHeader{
		Version:       1,
		PrevBlockHash: HeaderHash(&prev),
		Timestamp:     currentTime + MaxFutureBlockTimeSeconds,
		NBits:         params.GenesisBits,
	}
	header = mineHeaderForTest(header)
	if err := ValidateHeader(&header, PrevBlockContext{
		Height:         0,
		Header:         prev,
		MedianTimePast: params.GenesisTimestamp,
		CurrentTime:    currentTime,
	}, params); err != nil {
		t.Fatalf("ValidateHeader: %v", err)
	}
}

func TestNextWorkRequiredASERTOnScheduleMatchesGenesisBits(t *testing.T) {
	params := RegtestParams()
	prev := PrevBlockContext{
		Height: 0,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp,
			NBits:     params.GenesisBits,
		},
	}
	got, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	if got != params.GenesisBits {
		t.Fatalf("expected genesis bits 0x%08x, got 0x%08x", params.GenesisBits, got)
	}
}

func TestNextWorkRequiredASERTUsesParentTimestampNotCandidateTimestamp(t *testing.T) {
	params := RegtestParams()
	prev := PrevBlockContext{
		Height: 1,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp + 3600,
			NBits:     params.GenesisBits,
		},
	}
	gotA, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	if gotA != gotB {
		t.Fatalf("expected parent-timestamp anchored bits to be stable: %08x vs %08x", gotA, gotB)
	}
}

func TestNextWorkRequiredASERTLateParentEasesDifficulty(t *testing.T) {
	params := RegtestParams()
	prev := PrevBlockContext{
		Height: 10,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp + 10*600 + 3600,
			NBits:     params.GenesisBits,
		},
	}
	got, err := NextWorkRequired(prev, params)
	if err != nil {
		t.Fatal(err)
	}
	genesisTarget, err := compactToTarget(params.GenesisBits)
	if err != nil {
		t.Fatal(err)
	}
	gotTarget, err := compactToTarget(got)
	if err != nil {
		t.Fatal(err)
	}
	if gotTarget.Cmp(genesisTarget) < 0 {
		t.Fatalf("expected easier or equal target than genesis, got genesis=%s current=%s", genesisTarget.String(), gotTarget.String())
	}
}

func TestNextWorkRequiredASERTMatchesReferenceCases(t *testing.T) {
	params := RegtestParams()
	anchor := GenesisAsertAnchor(params)
	cases := []struct {
		name      string
		height    uint64
		timestamp uint64
	}{
		{name: "block1 on schedule", height: 0, timestamp: params.GenesisTimestamp},
		{name: "block1 early", height: 0, timestamp: params.GenesisTimestamp - 300},
		{name: "block1 late", height: 0, timestamp: params.GenesisTimestamp + 3600},
		{name: "several blocks on schedule", height: 143, timestamp: params.GenesisTimestamp + 143*600},
		{name: "large positive delta", height: 500, timestamp: params.GenesisTimestamp + 500*600 + 14*86400},
		{name: "large negative delta", height: 500, timestamp: params.GenesisTimestamp + 500*600 - 12*3600},
		{name: "overflow saturates", height: 20_000, timestamp: params.GenesisTimestamp + 20_000*600 + 10*365*86400},
		{name: "underflow clamps to one", height: 20_000, timestamp: params.GenesisTimestamp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prev := PrevBlockContext{
				Height: tc.height,
				Header: types.BlockHeader{
					Timestamp: tc.timestamp,
					NBits:     params.GenesisBits,
				},
			}
			got, err := NextWorkRequiredASERT(anchor, prev, params)
			if err != nil {
				t.Fatal(err)
			}
			want := referenceAsertBits(t, anchor, prev, params)
			if got != want {
				t.Fatalf("bits = 0x%08x, want 0x%08x", got, want)
			}
		})
	}
}

func BenchmarkNextWorkRequiredASERT(b *testing.B) {
	params := MainnetParams()
	anchor := GenesisAsertAnchor(params)
	prev := PrevBlockContext{
		Height: 50_000,
		Header: types.BlockHeader{
			Timestamp: params.GenesisTimestamp + uint64(params.TargetSpacingSecs*50_000+17),
			NBits:     params.GenesisBits,
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := NextWorkRequiredASERT(anchor, prev, params); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMineHeaderInterruptible4096(b *testing.B) {
	params := RegtestParams()
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  [32]byte{1},
		MerkleTxIDRoot: [32]byte{2},
		MerkleAuthRoot: [32]byte{3},
		UTXORoot:       [32]byte{4},
		Timestamp:      params.GenesisTimestamp + uint64(params.TargetSpacingSecs),
		NBits:          0x1b0404cb,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := MineHeaderInterruptible(header, params, func(nonce uint64) bool { return nonce < 4096 })
		if err != nil {
			b.Fatal(err)
		}
		if ok {
			b.Fatal("expected benchmark header to stop before finding pow")
		}
	}
}

func TestNextWorkRequiredBitcoinLegacyRetargetHelperClampsToPowLimit(t *testing.T) {
	params := RegtestParams()
	first := &types.BlockHeader{Timestamp: params.GenesisTimestamp, NBits: params.GenesisBits}
	prev := &types.BlockHeader{Timestamp: params.GenesisTimestamp + uint64(params.TargetSpacingSecs*2016*10), NBits: params.GenesisBits}
	got, err := NextWorkRequiredBitcoinLegacy(first, prev, params)
	if err != nil {
		t.Fatal(err)
	}
	if got != params.PowLimitBits {
		t.Fatalf("expected pow limit bits 0x%08x, got 0x%08x", params.PowLimitBits, got)
	}
}

func TestNewBlockSizeStateUsesABLAFloors(t *testing.T) {
	params := MainnetParams()
	state := NewBlockSizeState(params)
	if state.BlockSize != 0 {
		t.Fatalf("block size = %d, want 0", state.BlockSize)
	}
	if state.Epsilon != 16_000_000 {
		t.Fatalf("epsilon = %d, want 16000000", state.Epsilon)
	}
	if state.Beta != 16_000_000 {
		t.Fatalf("beta = %d, want 16000000", state.Beta)
	}
	if state.Limit() != params.BlockSizeFloor {
		t.Fatalf("limit = %d, want %d", state.Limit(), params.BlockSizeFloor)
	}
}

func TestAdvanceBlockSizeStateABLAPositiveBranch(t *testing.T) {
	params := MainnetParams()
	prev := BlockSizeState{
		BlockSize: params.BlockSizeFloor,
		Epsilon:   16_000_000,
		Beta:      16_000_000,
	}
	next := AdvanceBlockSizeState(prev, 1_000, params)
	if next.BlockSize != 1_000 {
		t.Fatalf("next block size = %d, want 1000", next.BlockSize)
	}
	if next.Epsilon != 16_000_210 {
		t.Fatalf("epsilon = %d, want 16000210", next.Epsilon)
	}
	if next.Beta != 16_001_679 {
		t.Fatalf("beta = %d, want 16001679", next.Beta)
	}
	if got := NextBlockSizeLimit(prev, params); got != 32_001_889 {
		t.Fatalf("next limit = %d, want 32001889", got)
	}
}

func TestAdvanceBlockSizeStateABLANegativeBranchClampsToFloor(t *testing.T) {
	params := MainnetParams()
	prev := BlockSizeState{
		BlockSize: 0,
		Epsilon:   16_000_000,
		Beta:      16_000_000,
	}
	next := AdvanceBlockSizeState(prev, 512, params)
	if next.BlockSize != 512 {
		t.Fatalf("next block size = %d, want 512", next.BlockSize)
	}
	if next.Epsilon != 16_000_000 {
		t.Fatalf("epsilon = %d, want floor 16000000", next.Epsilon)
	}
	if next.Beta != 16_000_000 {
		t.Fatalf("beta = %d, want floor 16000000", next.Beta)
	}
	if got := NextBlockSizeLimit(prev, params); got != params.BlockSizeFloor {
		t.Fatalf("next limit = %d, want %d", got, params.BlockSizeFloor)
	}
}
