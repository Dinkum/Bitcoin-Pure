package utreexo

import (
	"bytes"
	"errors"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"testing"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func testLeaf(txidByte byte, vout uint32, value uint64, keyHashByte byte) UtxoLeaf {
	return UtxoLeaf{
		OutPoint: types.OutPoint{
			TxID: [32]byte{txidByte},
			Vout: vout,
		},
		ValueAtoms: value,
		PubKey:     [32]byte{keyHashByte},
	}
}

func TestDeterministicUnderPermutation(t *testing.T) {
	a := testLeaf(1, 0, 10, 11)
	b := testLeaf(2, 1, 20, 22)
	c := testLeaf(3, 2, 30, 33)
	if UtxoRoot([]UtxoLeaf{a, b, c}) != UtxoRoot([]UtxoLeaf{c, a, b}) {
		t.Fatal("utxo root should be stable under permutation")
	}
}

func TestRootChangesWithData(t *testing.T) {
	a := testLeaf(1, 0, 10, 11)
	b := testLeaf(2, 1, 20, 22)
	if UtxoRoot([]UtxoLeaf{a, b}) == UtxoRoot([]UtxoLeaf{a, testLeaf(2, 1, 21, 22)}) {
		t.Fatal("utxo root should change")
	}
}

func TestEmptySetRootIsStable(t *testing.T) {
	if UtxoRoot(nil) != UtxoRoot(nil) {
		t.Fatal("empty root should be stable")
	}
	if got := UtxoRoot(nil); got != crypto.TaggedHash(UTXORootTag, nil) {
		t.Fatalf("empty root = %x, want %x", got, crypto.TaggedHash(UTXORootTag, nil))
	}
}

func TestSingleLeafRootWrapsLeafHash(t *testing.T) {
	leaf := testLeaf(9, 3, 90, 99)
	got := UtxoRoot([]UtxoLeaf{leaf})
	wantLeaf := LeafHash(leaf)
	want := crypto.TaggedHash(UTXORootTag, wantLeaf[:])
	if got != want {
		t.Fatalf("single leaf root = %x, want %x", got, want)
	}
}

func TestBranchHashDependsOnTrieSplit(t *testing.T) {
	left := testLeaf(0x10, 0, 10, 11)
	right := testLeaf(0x90, 1, 20, 22)
	root := UtxoRoot([]UtxoLeaf{left, right})
	leftHash := LeafHash(left)
	rightHash := LeafHash(right)
	branch := BranchHash(leftHash, rightHash)
	want := crypto.TaggedHash(UTXORootTag, branch[:])
	if root != want {
		t.Fatalf("two-leaf root = %x, want %x", root, want)
	}
}

func TestTrieUsesLexicalBitOrder(t *testing.T) {
	a := testLeaf(0x7f, 0, 10, 1)
	b := testLeaf(0x80, 0, 20, 2)
	rootAB := UtxoRoot([]UtxoLeaf{a, b})
	rootBA := UtxoRoot([]UtxoLeaf{b, a})
	if rootAB != rootBA {
		t.Fatal("bitwise trie ordering should remain permutation-invariant")
	}
	aHash := LeafHash(a)
	bHash := LeafHash(b)
	if bytes.Equal(aHash[:], bHash[:]) {
		t.Fatal("fixture leaves should not collide")
	}
}

func TestAccumulatorMatchesBulkRoot(t *testing.T) {
	leaves := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
		testLeaf(3, 2, 30, 33),
		testLeaf(4, 3, 40, 44),
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	if got, want := acc.Root(), UtxoRoot(leaves); got != want {
		t.Fatalf("accumulator root = %x, want %x", got, want)
	}
}

func TestCompressedAccumulatorMaterializesOnlyLeavesAndBranches(t *testing.T) {
	const leafCount = 10_000
	leaves := make([]UtxoLeaf, leafCount)
	for i := range leaves {
		leaves[i] = testLeaf(byte(i), uint32(i), uint64(i+1), byte(i>>8))
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	records := AccumulatorNodeRecords(acc)
	if got, want := len(records), 2*leafCount-1; got != want {
		t.Fatalf("material records = %d, want %d", got, want)
	}
	for _, record := range records {
		switch {
		case record.Leaf != nil:
			if record.Path.Depth != keyBits || record.LeftPath != nil || record.RightPath != nil {
				t.Fatalf("invalid material leaf at depth %d", record.Path.Depth)
			}
		default:
			if record.LeftPath == nil || record.RightPath == nil {
				t.Fatalf("unary material branch at depth %d", record.Path.Depth)
			}
			if err := validateChildPath(record.Path, *record.LeftPath, false); err != nil {
				t.Fatal(err)
			}
			if err := validateChildPath(record.Path, *record.RightPath, true); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCompressedAccumulatorMatchesLegacyBitTrieRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for size := 1; size <= 256; size *= 2 {
		leaves := make([]UtxoLeaf, 0, size)
		seen := make(map[types.OutPoint]struct{}, size)
		for len(leaves) < size {
			var outPoint types.OutPoint
			_, _ = rng.Read(outPoint.TxID[:])
			outPoint.Vout = rng.Uint32()
			if _, exists := seen[outPoint]; exists {
				continue
			}
			seen[outPoint] = struct{}{}
			leaves = append(leaves, UtxoLeaf{OutPoint: outPoint, ValueAtoms: rng.Uint64(), PubKey: [32]byte{byte(len(leaves))}})
		}
		acc, err := NewAccumulatorFromLeaves(leaves)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if got, want := acc.Root(), legacyBitTrieRoot(leaves); got != want {
			t.Fatalf("size %d compressed root = %x, legacy root = %x", size, got, want)
		}
	}
}

func legacyBitTrieRoot(leaves []UtxoLeaf) [32]byte {
	if len(leaves) == 0 {
		return crypto.TaggedHash(UTXORootTag, nil)
	}
	sorted := sortedKeyedLeaves(leaves)
	hash := legacyBitTrieHash(sorted, 0)
	return crypto.TaggedHash(UTXORootTag, hash[:])
}

func legacyBitTrieHash(leaves []keyedLeaf, depth int) [32]byte {
	if len(leaves) == 1 {
		return leaves[0].hash
	}
	split := splitAtBit(leaves, depth)
	if split == 0 || split == len(leaves) {
		return legacyBitTrieHash(leaves, depth+1)
	}
	left := legacyBitTrieHash(leaves[:split], depth+1)
	right := legacyBitTrieHash(leaves[split:], depth+1)
	return BranchHash(left, right)
}

func TestCompressedAccumulatorRecordReductionAt10K(t *testing.T) {
	leaves := make([]UtxoLeaf, 10_000)
	for i := range leaves {
		leaves[i] = benchmarkAccumulatorLeaf(uint64(i))
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatal(err)
	}
	compressed := len(AccumulatorNodeRecords(acc))
	legacy := legacyBitTrieRecordCount(sortedKeyedLeaves(leaves), 0)
	if ratio := float64(legacy) / float64(compressed); ratio < 20 {
		t.Fatalf("material-record reduction = %.2fx (%d to %d), want >=20x", ratio, legacy, compressed)
	} else {
		t.Logf("material records: legacy=%d compressed=%d reduction=%.2fx", legacy, compressed, ratio)
	}

	spent := make([]types.OutPoint, 1_024)
	created := make([]UtxoLeaf, 1_024)
	for i := range spent {
		spent[i] = leaves[i].OutPoint
		created[i] = benchmarkAccumulatorLeaf(uint64(len(leaves) + i))
	}
	next, err := acc.Apply(spent, created)
	if err != nil {
		t.Fatal(err)
	}
	delta := AccumulatorNodeDeltaBetween(acc, next)
	compressedOps := len(delta.Upserts) + len(delta.Deletes)
	afterLeaves := append(append([]UtxoLeaf(nil), leaves[len(spent):]...), created...)
	legacyOps := legacyBitTrieDeltaOperations(sortedKeyedLeaves(leaves), sortedKeyedLeaves(afterLeaves), 0)
	if ratio := float64(legacyOps) / float64(compressedOps); ratio < 50 {
		t.Fatalf("durable operation reduction = %.2fx (%d to %d), want >=50x", ratio, legacyOps, compressedOps)
	} else {
		t.Logf("1024-cycle durable operations: legacy=%d compressed=%d reduction=%.2fx", legacyOps, compressedOps, ratio)
	}
}

func legacyBitTrieRecordCount(leaves []keyedLeaf, depth int) int {
	if len(leaves) == 1 {
		return keyBits - depth + 1
	}
	split := splitAtBit(leaves, depth)
	if split == 0 || split == len(leaves) {
		return 1 + legacyBitTrieRecordCount(leaves, depth+1)
	}
	return 1 + legacyBitTrieRecordCount(leaves[:split], depth+1) + legacyBitTrieRecordCount(leaves[split:], depth+1)
}

func legacyBitTrieDeltaOperations(before, after []keyedLeaf, depth int) int {
	if keyedLeavesEqual(before, after) {
		return 0
	}
	if len(before) == 0 {
		return legacyBitTrieRecordCount(after, depth)
	}
	if len(after) == 0 {
		return legacyBitTrieRecordCount(before, depth)
	}
	operations := 1 // The post-transition node is upserted at this depth.
	if depth == keyBits {
		return operations
	}
	beforeSplit := splitAtBit(before, depth)
	afterSplit := splitAtBit(after, depth)
	operations += legacyBitTrieDeltaOperations(before[:beforeSplit], after[:afterSplit], depth+1)
	operations += legacyBitTrieDeltaOperations(before[beforeSplit:], after[afterSplit:], depth+1)
	return operations
}

func keyedLeavesEqual(left, right []keyedLeaf) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].key != right[i].key || left[i].hash != right[i].hash {
			return false
		}
	}
	return true
}

func TestAccumulatorNodeRecordsRoundTripAndDelta(t *testing.T) {
	leaves := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
		testLeaf(3, 2, 30, 33),
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	rebuilt, err := NewAccumulatorFromNodeRecords(AccumulatorNodeRecords(acc))
	if err != nil {
		t.Fatalf("NewAccumulatorFromNodeRecords: %v", err)
	}
	if rebuilt.Count() != acc.Count() || rebuilt.Root() != acc.Root() {
		t.Fatalf("rebuilt accumulator = count %d root %x, want count %d root %x", rebuilt.Count(), rebuilt.Root(), acc.Count(), acc.Root())
	}

	nextLeaf := testLeaf(4, 3, 40, 44)
	next, err := acc.Apply([]types.OutPoint{leaves[0].OutPoint}, []UtxoLeaf{nextLeaf})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	delta := AccumulatorNodeDeltaBetween(acc, next)
	if len(delta.Upserts) == 0 || len(delta.Deletes) == 0 {
		t.Fatalf("delta should contain upserts and deletes: %+v", delta)
	}
	nextRebuilt, err := NewAccumulatorFromNodeRecords(AccumulatorNodeRecords(next))
	if err != nil {
		t.Fatalf("rebuild next: %v", err)
	}
	if nextRebuilt.Count() != next.Count() || nextRebuilt.Root() != next.Root() {
		t.Fatalf("rebuilt next accumulator = count %d root %x, want count %d root %x", nextRebuilt.Count(), nextRebuilt.Root(), next.Count(), next.Root())
	}
}

func TestForEachAccumulatorNodeRecordCoverageAndError(t *testing.T) {
	leaves := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
		testLeaf(3, 2, 30, 33),
		testLeaf(4, 3, 40, 44),
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[AccumulatorNodePath]AccumulatorNodeRecord)
	for _, record := range AccumulatorNodeRecords(acc) {
		want[record.Path] = record
	}
	visited := 0
	err = ForEachAccumulatorNodeRecord(acc, func(record AccumulatorNodeRecord) error {
		expected, ok := want[record.Path]
		if !ok {
			return errors.New("visitor returned an unexpected path")
		}
		if expected.Hash != record.Hash || expected.Count != record.Count || (expected.Leaf == nil) != (record.Leaf == nil) {
			return errors.New("visitor record differs from compatibility record")
		}
		delete(want, record.Path)
		visited++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if visited != MaterialNodeCount(acc) || len(want) != 0 {
		t.Fatalf("visitor covered %d records with %d missing, want %d", visited, len(want), MaterialNodeCount(acc))
	}

	stop := errors.New("stop traversal")
	calls := 0
	err = ForEachAccumulatorNodeRecord(acc, func(AccumulatorNodeRecord) error {
		calls++
		if calls == 3 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || calls != 3 {
		t.Fatalf("early stop = err %v calls %d, want sentinel after 3", err, calls)
	}

	noOp := func(AccumulatorNodeRecord) error { return nil }
	if allocs := testing.AllocsPerRun(100, func() {
		if err := ForEachAccumulatorNodeRecord(acc, noOp); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("streaming traversal allocated %.2f objects/run", allocs)
	}
}

func TestCompressedAccumulatorDeltaRoundTrip(t *testing.T) {
	leaves := make([]UtxoLeaf, 4_096)
	for i := range leaves {
		leaves[i] = testLeaf(byte(i), uint32(i), uint64(i+1), byte(i>>8))
	}
	before, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatal(err)
	}
	spent := make([]types.OutPoint, 128)
	created := make([]UtxoLeaf, 128)
	for i := range spent {
		spent[i] = leaves[i*2].OutPoint
		created[i] = testLeaf(byte(i), uint32(50_000+i), uint64(i+1), byte(i+19))
	}
	after, err := before.Apply(spent, created)
	if err != nil {
		t.Fatal(err)
	}
	delta := AccumulatorNodeDeltaBetween(before, after)
	records := make(map[AccumulatorNodePath]AccumulatorNodeRecord)
	for _, record := range AccumulatorNodeRecords(before) {
		records[record.Path] = record
	}
	for _, path := range delta.Deletes {
		delete(records, path)
	}
	for _, record := range delta.Upserts {
		records[record.Path] = record
	}
	updated := make([]AccumulatorNodeRecord, 0, len(records))
	for _, record := range records {
		updated = append(updated, record)
	}
	rebuilt, err := NewAccumulatorFromNodeRecords(updated)
	if err != nil {
		t.Fatalf("rebuild from applied delta: %v", err)
	}
	if rebuilt.Count() != after.Count() || rebuilt.Root() != after.Root() {
		t.Fatalf("delta state count/root mismatch")
	}
	if operations := len(delta.Deletes) + len(delta.Upserts); operations >= 20_000 {
		t.Fatalf("compressed delta used %d record operations", operations)
	}
}

func TestAccumulatorDeltaRandomizedExactRecordParity(t *testing.T) {
	// These directed transitions cover root wrapping/collapse, replacement at
	// the same outpoint, complete removal, and rebuilding from an empty root.
	a := testLeaf(0x40, 0, 10, 1)
	b := testLeaf(0x60, 0, 20, 2)
	c := testLeaf(0xc0, 0, 30, 3)
	empty := NewAccumulator()
	two, err := NewAccumulatorFromLeaves([]UtxoLeaf{a, b})
	if err != nil {
		t.Fatal(err)
	}
	assertExactAccumulatorDelta(t, empty, two)
	wrapped, err := two.Apply(nil, []UtxoLeaf{c})
	if err != nil {
		t.Fatal(err)
	}
	assertExactAccumulatorDelta(t, two, wrapped)
	collapsed, err := wrapped.Apply([]types.OutPoint{c.OutPoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertExactAccumulatorDelta(t, wrapped, collapsed)
	recreatedLeaf := a
	recreatedLeaf.ValueAtoms++
	recreated, err := collapsed.Apply([]types.OutPoint{a.OutPoint}, []UtxoLeaf{recreatedLeaf})
	if err != nil {
		t.Fatal(err)
	}
	assertExactAccumulatorDelta(t, collapsed, recreated)
	backToEmpty, err := recreated.Apply([]types.OutPoint{a.OutPoint, b.OutPoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertExactAccumulatorDelta(t, recreated, backToEmpty)
	assertExactAccumulatorDelta(t, backToEmpty, wrapped)

	rng := rand.New(rand.NewSource(0x5eed))
	state := make(map[types.OutPoint]UtxoLeaf)
	current := NewAccumulator()
	nextID := uint64(1)
	for round := 0; round < 200; round++ {
		outPoints := sortedStateOutPoints(state)
		spendCount := 0
		if len(outPoints) > 0 {
			spendCount = rng.Intn(min(len(outPoints), 8) + 1)
		}
		if round%41 == 40 {
			spendCount = len(outPoints)
		}
		rng.Shuffle(len(outPoints), func(i, j int) { outPoints[i], outPoints[j] = outPoints[j], outPoints[i] })
		spent := append([]types.OutPoint(nil), outPoints[:spendCount]...)
		created := make([]UtxoLeaf, 0, 8)
		for i, outPoint := range spent {
			if (round+i)%3 == 0 {
				leaf := state[outPoint]
				leaf.ValueAtoms += uint64(round + i + 1)
				created = append(created, leaf)
			}
		}
		newCount := rng.Intn(9)
		if len(state) == 0 {
			newCount = 1 + rng.Intn(8)
		}
		for i := 0; i < newCount; i++ {
			leaf := randomizedDeltaLeaf(rng, nextID)
			nextID++
			created = append(created, leaf)
		}

		next, err := current.Apply(spent, created)
		if err != nil {
			t.Fatalf("round %d Apply: %v", round, err)
		}
		assertExactAccumulatorDelta(t, current, next)
		for _, outPoint := range spent {
			delete(state, outPoint)
		}
		for _, leaf := range created {
			state[leaf.OutPoint] = leaf
		}
		current = next
	}
}

func assertExactAccumulatorDelta(t *testing.T, before, after *Accumulator) {
	t.Helper()
	records := make(map[AccumulatorNodePath]AccumulatorNodeRecord)
	for _, record := range AccumulatorNodeRecords(before) {
		records[record.Path] = record
	}
	delta := AccumulatorNodeDeltaBetween(before, after)
	for _, path := range delta.Deletes {
		delete(records, path)
	}
	for _, record := range delta.Upserts {
		records[record.Path] = record
	}
	want := make(map[AccumulatorNodePath]AccumulatorNodeRecord)
	for _, record := range AccumulatorNodeRecords(after) {
		want[record.Path] = record
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("delta record set differs from post-state: got %d records, want %d", len(records), len(want))
	}
	wantRoot, hasRoot := AccumulatorRootPath(after)
	if hasRoot != (delta.RootPath != nil) || hasRoot && *delta.RootPath != wantRoot {
		t.Fatalf("delta root path = %v, want %v (present %t)", delta.RootPath, wantRoot, hasRoot)
	}
	flat := make([]AccumulatorNodeRecord, 0, len(records))
	for _, record := range records {
		flat = append(flat, record)
	}
	rebuilt, err := NewAccumulatorFromNodeRecords(flat)
	if err != nil {
		t.Fatalf("rebuild exact delta records: %v", err)
	}
	if rebuilt.Count() != after.Count() || rebuilt.Root() != after.Root() {
		t.Fatalf("rebuilt exact delta state differs from post-state")
	}
}

func sortedStateOutPoints(state map[types.OutPoint]UtxoLeaf) []types.OutPoint {
	out := make([]types.OutPoint, 0, len(state))
	for outPoint := range state {
		out = append(out, outPoint)
	}
	sort.Slice(out, func(i, j int) bool {
		left, right := leafKey(out[i]), leafKey(out[j])
		return bytes.Compare(left[:], right[:]) < 0
	})
	return out
}

func randomizedDeltaLeaf(rng *rand.Rand, id uint64) UtxoLeaf {
	var txid [32]byte
	_, _ = rng.Read(txid[:])
	// Mix a monotonic identity into the random key to make accidental duplicate
	// generation impossible while retaining varied Patricia prefixes.
	for i := 0; i < 8; i++ {
		txid[24+i] ^= byte(id >> (8 * i))
	}
	return UtxoLeaf{
		OutPoint:   types.OutPoint{TxID: txid, Vout: uint32(id)},
		ValueAtoms: id + 1,
		PubKey:     [32]byte{byte(id), byte(id >> 8)},
	}
}

func TestCompressedAccumulatorRejectsMalformedChildPath(t *testing.T) {
	acc, err := NewAccumulatorFromLeaves([]UtxoLeaf{testLeaf(1, 0, 1, 1), testLeaf(2, 0, 2, 2)})
	if err != nil {
		t.Fatal(err)
	}
	records := AccumulatorNodeRecords(acc)
	if records[0].LeftPath == nil {
		t.Fatal("fixture root is not a branch")
	}
	bad := *records[0].LeftPath
	byteIndex := records[0].Path.Depth / 8
	bitOffset := 7 - (records[0].Path.Depth % 8)
	bad.Key[byteIndex] |= 1 << bitOffset
	records[0].LeftPath = &bad
	if _, err := NewAccumulatorFromNodeRecords(records); err == nil {
		t.Fatal("expected malformed child direction to fail")
	}
}

func TestBulkAccumulatorMatchesIncrementalBuilder(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("parallel accumulator build needs multiple workers to exercise")
	}
	leaves := make([]UtxoLeaf, 0, 2048)
	for i := 0; i < 2048; i++ {
		leaves = append(leaves, testLeaf(byte(i), uint32(i>>8), uint64(i+1), byte(i+17)))
	}
	bulk, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	incremental := NewAccumulator()
	for _, leaf := range leaves {
		incremental, err = incremental.Add(leaf)
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if got, want := bulk.Root(), incremental.Root(); got != want {
		t.Fatalf("bulk accumulator root = %x, want %x", got, want)
	}
}

func TestAccumulatorApplyMatchesBulkRootAfterUpdates(t *testing.T) {
	original := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
		testLeaf(3, 2, 30, 33),
	}
	acc, err := NewAccumulatorFromLeaves(original)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	created := []UtxoLeaf{
		testLeaf(9, 0, 90, 99),
		testLeaf(10, 1, 100, 100),
	}
	next, err := acc.Apply(
		[]types.OutPoint{original[0].OutPoint, original[2].OutPoint},
		created,
	)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	wantLeaves := []UtxoLeaf{original[1], created[0], created[1]}
	if got, want := next.Root(), UtxoRoot(wantLeaves); got != want {
		t.Fatalf("updated accumulator root = %x, want %x", got, want)
	}
}

func TestAccumulatorApplyMatchesSequentialUpdatesOnLargeBatch(t *testing.T) {
	original := make([]UtxoLeaf, 0, 512)
	for i := 0; i < 512; i++ {
		original = append(original, testLeaf(byte(i), uint32(i), uint64(i+1), byte(i+17)))
	}
	acc, err := NewAccumulatorFromLeaves(original)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	spent := make([]types.OutPoint, 0, 128)
	created := make([]UtxoLeaf, 0, 128)
	for i := 0; i < 128; i++ {
		spent = append(spent, original[i*2].OutPoint)
		created = append(created, testLeaf(byte(i), uint32(10_000+i), uint64(1_000+i), byte(i+91)))
	}
	batched, err := acc.Apply(spent, created)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	sequential := acc
	for _, outPoint := range spent {
		sequential, err = sequential.Delete(outPoint)
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	for _, leaf := range created {
		sequential, err = sequential.Add(leaf)
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if got, want := batched.Root(), sequential.Root(); got != want {
		t.Fatalf("batched root = %x, want %x", got, want)
	}
	if batched.Count() != sequential.Count() {
		t.Fatalf("batched count = %d, want %d", batched.Count(), sequential.Count())
	}
}

func TestAccumulatorApplyRejectsDuplicateSpentOutPoint(t *testing.T) {
	acc, err := NewAccumulatorFromLeaves([]UtxoLeaf{testLeaf(1, 0, 10, 11)})
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	_, err = acc.Apply(
		[]types.OutPoint{
			{TxID: [32]byte{1}, Vout: 0},
			{TxID: [32]byte{1}, Vout: 0},
		},
		nil,
	)
	if err == nil {
		t.Fatal("expected duplicate spent outpoint error")
	}
}

func TestAccumulatorRejectsMissingDelete(t *testing.T) {
	acc, err := NewAccumulatorFromLeaves([]UtxoLeaf{testLeaf(1, 0, 10, 11)})
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	if _, err := acc.Delete(types.OutPoint{TxID: [32]byte{9}, Vout: 0}); err == nil {
		t.Fatal("expected missing delete error")
	}
}

func TestAccumulatorRejectsDuplicateOutPointInBulkBuild(t *testing.T) {
	_, err := NewAccumulatorFromLeaves([]UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(1, 0, 20, 22),
	})
	if err == nil {
		t.Fatal("expected duplicate outpoint error")
	}
}

func TestAccumulatorProofVerifiesMembership(t *testing.T) {
	leaves := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
		testLeaf(3, 2, 30, 33),
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	proof, err := acc.Prove(leaves[1].OutPoint)
	if err != nil {
		t.Fatalf("Prove: %v", err)
	}
	if !proof.Exists {
		t.Fatal("expected membership proof")
	}
	if proof.ValueAtoms != leaves[1].ValueAtoms || proof.PubKey != leaves[1].PubKey {
		t.Fatal("membership proof leaf data mismatch")
	}
	if !VerifyProof(acc.Root(), proof) {
		t.Fatal("expected membership proof to verify")
	}
}

func TestAccumulatorProofVerifiesExclusion(t *testing.T) {
	leaves := []UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
	}
	acc, err := NewAccumulatorFromLeaves(leaves)
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	proof, err := acc.Prove(types.OutPoint{TxID: [32]byte{9}, Vout: 0})
	if err != nil {
		t.Fatalf("Prove: %v", err)
	}
	if proof.Exists {
		t.Fatal("expected exclusion proof")
	}
	if !VerifyProof(acc.Root(), proof) {
		t.Fatal("expected exclusion proof to verify")
	}
}

func TestAccumulatorProofRejectsTampering(t *testing.T) {
	acc, err := NewAccumulatorFromLeaves([]UtxoLeaf{
		testLeaf(1, 0, 10, 11),
		testLeaf(2, 1, 20, 22),
	})
	if err != nil {
		t.Fatalf("NewAccumulatorFromLeaves: %v", err)
	}
	proof, err := acc.Prove(types.OutPoint{TxID: [32]byte{1}, Vout: 0})
	if err != nil {
		t.Fatalf("Prove: %v", err)
	}
	proof.ValueAtoms++
	if VerifyProof(acc.Root(), proof) {
		t.Fatal("expected tampered proof to fail")
	}
}

func TestExclusionProofCannotReuseExistingLeafHash(t *testing.T) {
	leaf := UtxoLeaf{
		OutPoint:   types.OutPoint{TxID: [32]byte{42}, Vout: 7},
		Type:       types.OutputXOnlyP2PK,
		ValueAtoms: 50,
		Payload32:  [32]byte{9},
	}
	acc, err := NewAccumulatorFromLeaves([]UtxoLeaf{leaf})
	if err != nil {
		t.Fatal(err)
	}
	forged := OutPointProof{
		Version:  ProofVersion,
		OutPoint: leaf.OutPoint,
		Steps:    make([]ProofStep, keyBits),
		Terminal: &leaf,
	}
	if VerifyProof(acc.Root(), forged) {
		t.Fatal("forged exclusion proof for an existing outpoint verified")
	}
}
