package utreexo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"sync"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

const (
	UTXOLeafTag   = "BPU/UtxoLeafV1"
	UTXOBranchTag = "BPU/UtxoBranchV1"
	UTXORootTag   = "BPU/UtxoRootV1"
	outPointBytes = 36
	keyBits       = outPointBytes * 8

	parallelLeafHashThreshold  = 512
	parallelTreeBuildThreshold = 256
)

const (
	OutPointKeyBytes = outPointBytes
	KeyBits          = keyBits
)

type UtxoLeaf struct {
	OutPoint   types.OutPoint
	Type       uint64
	ValueAtoms uint64
	Payload32  [32]byte
	PubKey     [32]byte
}

type keyedLeaf struct {
	key  [outPointBytes]byte
	hash [32]byte
	utxo UtxoLeaf
}

type Accumulator struct {
	root  *accNode
	count int
}

type accNode struct {
	path  AccumulatorNodePath
	left  *accNode
	right *accNode
	leaf  *keyedLeaf
	hash  [32]byte
	count int
}

type OutPointProof struct {
	Version    uint8
	OutPoint   types.OutPoint
	Exists     bool
	Type       uint64
	ValueAtoms uint64
	Payload32  [32]byte
	PubKey     [32]byte
	Steps      []ProofStep
	Terminal   *UtxoLeaf
}

const ProofVersion uint8 = 2

type ProofStep struct {
	HasSibling  bool
	SiblingHash [32]byte
}

type AccumulatorNodePath struct {
	Depth int
	Key   [OutPointKeyBytes]byte
}

type AccumulatorNodeRecord struct {
	Path      AccumulatorNodePath
	Hash      [32]byte
	Count     int
	Leaf      *UtxoLeaf
	LeftPath  *AccumulatorNodePath
	RightPath *AccumulatorNodePath
}

type AccumulatorNodeDelta struct {
	Upserts  []AccumulatorNodeRecord
	Deletes  []AccumulatorNodePath
	RootPath *AccumulatorNodePath
}

func LeafHash(leaf UtxoLeaf) [32]byte {
	var scratch [outPointBytes + 9 + 8 + 32]byte
	buf := scratch[:0]
	key := leafKey(leaf.OutPoint)
	buf = append(buf, key[:]...)
	buf = types.AppendCanonicalVarInt(buf, leaf.Type)
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
	payload32 := leaf.Payload32
	if payload32 == ([32]byte{}) && leaf.Type == types.OutputXOnlyP2PK {
		payload32 = leaf.PubKey
	}
	buf = append(buf, payload32[:]...)
	return crypto.TaggedHash(UTXOLeafTag, buf)
}

func BranchHash(left, right [32]byte) [32]byte {
	var scratch [64]byte
	buf := scratch[:0]
	buf = append(buf, left[:]...)
	buf = append(buf, right[:]...)
	return crypto.TaggedHash(UTXOBranchTag, buf)
}

func UtxoRoot(leaves []UtxoLeaf) [32]byte {
	if len(leaves) == 0 {
		return crypto.TaggedHash(UTXORootTag, nil)
	}
	sorted := sortedKeyedLeaves(leaves)
	if err := ensureUniqueSortedLeaves(sorted); err != nil {
		panic(err.Error())
	}
	root := buildAccumulatorTree(sorted, 0, parallelBuildBudget())
	return crypto.TaggedHash(UTXORootTag, root.hash[:])
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

func NewAccumulatorFromLeaves(leaves []UtxoLeaf) (*Accumulator, error) {
	if len(leaves) == 0 {
		return NewAccumulator(), nil
	}
	sorted := sortedKeyedLeaves(leaves)
	if err := ensureUniqueSortedLeaves(sorted); err != nil {
		return nil, err
	}
	return &Accumulator{
		root:  buildAccumulatorTree(sorted, 0, parallelBuildBudget()),
		count: len(sorted),
	}, nil
}

func NewAccumulatorFromNodeRecords(records []AccumulatorNodeRecord) (*Accumulator, error) {
	nodes := make(map[AccumulatorNodePath]AccumulatorNodeRecord, len(records))
	for _, record := range records {
		if record.Path.Depth < 0 || record.Path.Depth > keyBits {
			return nil, fmt.Errorf("invalid accumulator node depth %d", record.Path.Depth)
		}
		record.Path.Key = maskPathKey(record.Path.Key, record.Path.Depth)
		if _, exists := nodes[record.Path]; exists {
			return nil, fmt.Errorf("duplicate accumulator node at depth %d", record.Path.Depth)
		}
		nodes[record.Path] = record
	}
	if len(nodes) == 0 {
		return NewAccumulator(), nil
	}
	childPaths := make(map[AccumulatorNodePath]struct{}, len(nodes))
	for _, record := range nodes {
		if record.LeftPath != nil {
			childPaths[normalizedPath(*record.LeftPath)] = struct{}{}
		}
		if record.RightPath != nil {
			childPaths[normalizedPath(*record.RightPath)] = struct{}{}
		}
	}
	var rootPath AccumulatorNodePath
	rootCount := 0
	for path := range nodes {
		if _, child := childPaths[path]; !child {
			rootPath = path
			rootCount++
		}
	}
	if rootCount != 1 {
		return nil, fmt.Errorf("accumulator records contain %d roots", rootCount)
	}
	visited := make(map[AccumulatorNodePath]struct{}, len(nodes))
	root, err := rebuildNodeFromRecords(nodes, rootPath, visited)
	if err != nil {
		return nil, err
	}
	if len(visited) != len(nodes) {
		return nil, fmt.Errorf("accumulator records contain %d unreachable nodes", len(nodes)-len(visited))
	}
	return &Accumulator{root: root, count: root.count}, nil
}

func (a *Accumulator) Root() [32]byte {
	if a == nil || a.root == nil {
		return crypto.TaggedHash(UTXORootTag, nil)
	}
	return crypto.TaggedHash(UTXORootTag, a.root.hash[:])
}

func (a *Accumulator) Count() int {
	if a == nil {
		return 0
	}
	return a.count
}

// Clone returns a distinct accumulator handle for callers that need snapshot
// semantics. The trie itself is structurally persistent, so sharing the root is
// safe and avoids rebuilding the committed state.
func (a *Accumulator) Clone() *Accumulator {
	if a == nil {
		return nil
	}
	return &Accumulator{
		root:  a.root,
		count: a.count,
	}
}

func (a *Accumulator) Add(leaf UtxoLeaf) (*Accumulator, error) {
	if a == nil {
		a = NewAccumulator()
	}
	keyed := keyedLeaf{
		key:  leafKey(leaf.OutPoint),
		hash: LeafHash(leaf),
		utxo: leaf,
	}
	root, err := insertLeaf(a.root, keyed)
	if err != nil {
		return nil, err
	}
	return &Accumulator{root: root, count: a.count + 1}, nil
}

func (a *Accumulator) Delete(outPoint types.OutPoint) (*Accumulator, error) {
	if a == nil || a.root == nil {
		return nil, fmt.Errorf("missing accumulator leaf %x:%d", outPoint.TxID, outPoint.Vout)
	}
	root, deleted, err := deleteLeaf(a.root, leafKey(outPoint))
	if err != nil {
		return nil, err
	}
	if !deleted {
		return nil, fmt.Errorf("missing accumulator leaf %x:%d", outPoint.TxID, outPoint.Vout)
	}
	return &Accumulator{root: root, count: a.count - 1}, nil
}

// Apply batches one template-sized accumulator transition. Mining and block
// assembly build large spend/create sets, so replaying Delete/Add one leaf at a
// time wastes trie walks and hash rebuilds on the same prefixes.
func (a *Accumulator) Apply(spent []types.OutPoint, created []UtxoLeaf) (*Accumulator, error) {
	if a == nil {
		a = NewAccumulator()
	}
	if len(spent) == 0 && len(created) == 0 {
		return a, nil
	}
	sortedSpent, err := sortedSpentKeys(spent)
	if err != nil {
		return nil, err
	}
	sortedCreated := sortedKeyedLeaves(created)
	if err := ensureUniqueSortedLeaves(sortedCreated); err != nil {
		return nil, err
	}
	root := a.root
	for _, key := range sortedSpent {
		var deleted bool
		root, deleted, err = deleteLeaf(root, key)
		if err != nil {
			return nil, err
		}
		if !deleted {
			outPoint := outPointFromKey(key)
			return nil, fmt.Errorf("missing accumulator leaf %x:%d", outPoint.TxID, outPoint.Vout)
		}
	}
	for _, leaf := range sortedCreated {
		root, err = insertLeaf(root, leaf)
		if err != nil {
			return nil, err
		}
	}
	if root == nil {
		return NewAccumulator(), nil
	}
	return &Accumulator{root: root, count: root.count}, nil
}

func AccumulatorNodeRecords(acc *Accumulator) []AccumulatorNodeRecord {
	if acc == nil || acc.root == nil {
		return nil
	}
	records := make([]AccumulatorNodeRecord, 0, MaterialNodeCount(acc))
	_ = ForEachAccumulatorNodeRecord(acc, func(record AccumulatorNodeRecord) error {
		records = append(records, cloneNodeRecord(record))
		return nil
	})
	return records
}

// MaterialNodeCount returns the number of records in the compressed trie
// without traversing or allocating. Every non-empty binary Patricia trie has
// one leaf per UTXO and exactly one fewer genuine two-child branch.
func MaterialNodeCount(acc *Accumulator) int {
	if acc == nil || acc.count == 0 {
		return 0
	}
	return 2*acc.count - 1
}

// ForEachAccumulatorNodeRecord visits material records in stable root-left-
// right order without building an O(UTXO-count) slice. Pointer fields in the
// callback record are read-only views valid for that callback; callers that
// retain or mutate records must copy them. Traversal stops at the first error.
func ForEachAccumulatorNodeRecord(acc *Accumulator, visit func(AccumulatorNodeRecord) error) error {
	if visit == nil {
		return fmt.Errorf("accumulator node visitor is required")
	}
	if acc == nil || acc.root == nil {
		return nil
	}
	return forEachAccumulatorNodeRecord(acc.root, visit)
}

func forEachAccumulatorNodeRecord(node *accNode, visit func(AccumulatorNodeRecord) error) error {
	if node == nil {
		return nil
	}
	if err := visit(nodeRecordView(node)); err != nil {
		return err
	}
	if node.leaf != nil {
		return nil
	}
	if err := forEachAccumulatorNodeRecord(node.left, visit); err != nil {
		return err
	}
	return forEachAccumulatorNodeRecord(node.right, visit)
}

// AccumulatorRootPath returns the exact path of the compressed root record.
func AccumulatorRootPath(acc *Accumulator) (AccumulatorNodePath, bool) {
	if acc == nil || acc.root == nil {
		return AccumulatorNodePath{}, false
	}
	return acc.root.path, true
}

func AccumulatorNodeDeltaBetween(before *Accumulator, after *Accumulator) AccumulatorNodeDelta {
	var delta AccumulatorNodeDelta
	diffAccumulatorNodes(rootOf(before), rootOf(after), &delta)
	if path, ok := AccumulatorRootPath(after); ok {
		delta.RootPath = &path
	}
	return delta
}

func OutPointKey(outPoint types.OutPoint) [OutPointKeyBytes]byte {
	return leafKey(outPoint)
}

func OutPointFromKey(key [OutPointKeyBytes]byte) types.OutPoint {
	return outPointFromKey(key)
}

// Prove returns a single-outpoint Merklix proof over the current accumulator
// root. Membership proofs carry the queried leaf; exclusion proofs carry a
// committed terminal leaf whose full path proves the query branch is absent.
func (a *Accumulator) Prove(outPoint types.OutPoint) (OutPointProof, error) {
	proof := OutPointProof{Version: ProofVersion, OutPoint: outPoint}
	if a == nil || a.root == nil {
		return proof, nil
	}
	key := leafKey(outPoint)
	steps := make([]ProofStep, keyBits)
	node := a.root
	for node.leaf == nil {
		depth := node.path.Depth
		if bitSet(key, depth) {
			steps[depth] = ProofStep{HasSibling: true, SiblingHash: node.left.hash}
			node = node.right
		} else {
			steps[depth] = ProofStep{HasSibling: true, SiblingHash: node.right.hash}
			node = node.left
		}
	}
	if node.leaf.key != key {
		terminalCopy := node.leaf.utxo
		proof.Terminal = &terminalCopy
		proof.Steps = steps
		return proof, nil
	}
	proof.Exists = true
	proof.Type = node.leaf.utxo.Type
	proof.ValueAtoms = node.leaf.utxo.ValueAtoms
	proof.Payload32 = node.leaf.utxo.Payload32
	proof.PubKey = node.leaf.utxo.PubKey
	proof.Steps = steps
	return proof, nil
}

// VerifyProof checks a membership or exclusion proof against a committed
// `utxo_root` without requiring access to the full UTXO set.
func VerifyProof(root [32]byte, proof OutPointProof) bool {
	if proof.Version != ProofVersion || len(proof.Steps) > keyBits {
		return false
	}
	if len(proof.Steps) == 0 {
		return !proof.Exists && proof.Terminal == nil && root == crypto.TaggedHash(UTXORootTag, nil)
	}
	if proof.Exists {
		if proof.Terminal != nil {
			return false
		}
		return verifyMembershipProof(root, UtxoLeaf{
			OutPoint:   proof.OutPoint,
			Type:       proof.Type,
			ValueAtoms: proof.ValueAtoms,
			Payload32:  proof.Payload32,
			PubKey:     proof.PubKey,
		}, proof.Steps)
	}
	if proof.Terminal == nil || proof.Terminal.OutPoint == proof.OutPoint || len(proof.Steps) != keyBits {
		return false
	}
	queryKey := leafKey(proof.OutPoint)
	terminalKey := leafKey(proof.Terminal.OutPoint)
	divergence := firstDifferentBit(queryKey, terminalKey)
	if divergence < 0 || proof.Steps[divergence].HasSibling {
		return false
	}
	return verifyMembershipProof(root, *proof.Terminal, proof.Steps)
}

func verifyMembershipProof(root [32]byte, leaf UtxoLeaf, steps []ProofStep) bool {
	if len(steps) != keyBits {
		return false
	}
	key := leafKey(leaf.OutPoint)
	current := LeafHash(leaf)
	for bitIndex := len(steps) - 1; bitIndex >= 0; bitIndex-- {
		step := steps[bitIndex]
		if !step.HasSibling {
			if step.SiblingHash != ([32]byte{}) {
				return false
			}
			continue
		}
		if bitSet(key, bitIndex) {
			current = BranchHash(step.SiblingHash, current)
		} else {
			current = BranchHash(current, step.SiblingHash)
		}
	}
	return root == crypto.TaggedHash(UTXORootTag, current[:])
}

func firstDifferentBit(left, right [outPointBytes]byte) int {
	return firstDifferentBitFrom(left, right, 0)
}

func firstDifferentBitFrom(left, right [outPointBytes]byte, start int) int {
	for bitIndex := start; bitIndex < keyBits; bitIndex++ {
		if bitSet(left, bitIndex) != bitSet(right, bitIndex) {
			return bitIndex
		}
	}
	return -1
}

func normalizedPath(path AccumulatorNodePath) AccumulatorNodePath {
	path.Key = maskPathKey(path.Key, path.Depth)
	return path
}

func pathContains(parent, child AccumulatorNodePath) bool {
	if parent.Depth > child.Depth {
		return false
	}
	return maskPathKey(child.Key, parent.Depth) == parent.Key
}

func validateChildPath(parent, child AccumulatorNodePath, right bool) error {
	if child.Depth <= parent.Depth || child.Depth > keyBits || !pathContains(parent, child) {
		return fmt.Errorf("invalid accumulator child path at depth %d from parent depth %d", child.Depth, parent.Depth)
	}
	if bitSet(child.Key, parent.Depth) != right {
		return fmt.Errorf("accumulator child direction mismatch at depth %d", parent.Depth)
	}
	return nil
}

func rebuildNodeFromRecords(records map[AccumulatorNodePath]AccumulatorNodeRecord, path AccumulatorNodePath, visited map[AccumulatorNodePath]struct{}) (*accNode, error) {
	path = normalizedPath(path)
	if _, ok := visited[path]; ok {
		return nil, fmt.Errorf("accumulator node at depth %d has multiple parents", path.Depth)
	}
	record, ok := records[path]
	if !ok {
		return nil, fmt.Errorf("missing accumulator node at depth %d", path.Depth)
	}
	visited[path] = struct{}{}
	if record.Leaf != nil {
		if path.Depth != keyBits || record.LeftPath != nil || record.RightPath != nil {
			return nil, fmt.Errorf("invalid accumulator leaf shape at depth %d", path.Depth)
		}
		keyed := keyedLeaf{
			key:  leafKey(record.Leaf.OutPoint),
			hash: LeafHash(*record.Leaf),
			utxo: *record.Leaf,
		}
		if keyed.key != path.Key {
			return nil, fmt.Errorf("accumulator leaf key mismatch at depth %d", path.Depth)
		}
		if keyed.hash != record.Hash || record.Count != 1 {
			return nil, fmt.Errorf("accumulator leaf hash/count mismatch at depth %d", path.Depth)
		}
		leafCopy := keyed
		return &accNode{path: path, leaf: &leafCopy, hash: record.Hash, count: 1}, nil
	}
	if path.Depth >= keyBits || record.LeftPath == nil || record.RightPath == nil {
		return nil, fmt.Errorf("internal accumulator node at leaf depth")
	}
	leftPath := normalizedPath(*record.LeftPath)
	rightPath := normalizedPath(*record.RightPath)
	if err := validateChildPath(path, leftPath, false); err != nil {
		return nil, err
	}
	if err := validateChildPath(path, rightPath, true); err != nil {
		return nil, err
	}
	left, err := rebuildNodeFromRecords(records, leftPath, visited)
	if err != nil {
		return nil, err
	}
	right, err := rebuildNodeFromRecords(records, rightPath, visited)
	if err != nil {
		return nil, err
	}
	node := makeBranch(path.Depth, path.Key, left, right)
	if node.hash != record.Hash || node.count != record.Count {
		return nil, fmt.Errorf("accumulator node hash/count mismatch at depth %d", path.Depth)
	}
	return node, nil
}

func diffAccumulatorNodes(before, after *accNode, delta *AccumulatorNodeDelta) {
	if before == after {
		return
	}
	if before != nil && after == nil {
		collectNodeDeletes(before, delta)
		return
	}
	if before == nil {
		collectNodeUpserts(after, delta)
		return
	}
	if before.path == after.path {
		delta.Upserts = append(delta.Upserts, nodeRecord(after))
		if before.leaf == nil && after.leaf == nil {
			diffAccumulatorNodes(before.left, after.left, delta)
			diffAccumulatorNodes(before.right, after.right, delta)
		}
		return
	}
	// Persistent insertions wrap an existing subtree, while deletions collapse
	// to one. Align that shared child so deltas stay proportional to the change.
	if after.leaf == nil && pathContains(after.path, before.path) {
		delta.Upserts = append(delta.Upserts, nodeRecord(after))
		if bitSet(before.path.Key, after.path.Depth) {
			diffAccumulatorNodes(nil, after.left, delta)
			diffAccumulatorNodes(before, after.right, delta)
		} else {
			diffAccumulatorNodes(before, after.left, delta)
			diffAccumulatorNodes(nil, after.right, delta)
		}
		return
	}
	if before.leaf == nil && pathContains(before.path, after.path) {
		delta.Deletes = append(delta.Deletes, before.path)
		if bitSet(after.path.Key, before.path.Depth) {
			diffAccumulatorNodes(before.left, nil, delta)
			diffAccumulatorNodes(before.right, after, delta)
		} else {
			diffAccumulatorNodes(before.left, after, delta)
			diffAccumulatorNodes(before.right, nil, delta)
		}
		return
	}
	collectNodeDeletes(before, delta)
	collectNodeUpserts(after, delta)
}

func collectNodeUpserts(node *accNode, delta *AccumulatorNodeDelta) {
	if node == nil {
		return
	}
	delta.Upserts = append(delta.Upserts, nodeRecord(node))
	if node.leaf != nil {
		return
	}
	collectNodeUpserts(node.left, delta)
	collectNodeUpserts(node.right, delta)
}

func collectNodeDeletes(node *accNode, delta *AccumulatorNodeDelta) {
	if node == nil {
		return
	}
	delta.Deletes = append(delta.Deletes, node.path)
	if node.leaf != nil {
		return
	}
	collectNodeDeletes(node.left, delta)
	collectNodeDeletes(node.right, delta)
}

func nodeRecord(node *accNode) AccumulatorNodeRecord {
	record := AccumulatorNodeRecord{Path: node.path, Hash: node.hash, Count: node.count}
	if node.leaf != nil {
		leaf := node.leaf.utxo
		record.Leaf = &leaf
	} else {
		left, right := node.left.path, node.right.path
		record.LeftPath, record.RightPath = &left, &right
	}
	return record
}

func nodeRecordView(node *accNode) AccumulatorNodeRecord {
	record := AccumulatorNodeRecord{Path: node.path, Hash: node.hash, Count: node.count}
	if node.leaf != nil {
		record.Leaf = &node.leaf.utxo
	} else {
		record.LeftPath = &node.left.path
		record.RightPath = &node.right.path
	}
	return record
}

func cloneNodeRecord(record AccumulatorNodeRecord) AccumulatorNodeRecord {
	if record.Leaf != nil {
		leaf := *record.Leaf
		record.Leaf = &leaf
	}
	if record.LeftPath != nil {
		left := *record.LeftPath
		record.LeftPath = &left
	}
	if record.RightPath != nil {
		right := *record.RightPath
		record.RightPath = &right
	}
	return record
}

func rootOf(acc *Accumulator) *accNode {
	if acc == nil {
		return nil
	}
	return acc.root
}

func maskPathKey(key [OutPointKeyBytes]byte, depth int) [OutPointKeyBytes]byte {
	if depth <= 0 {
		return [OutPointKeyBytes]byte{}
	}
	if depth >= keyBits {
		return key
	}
	fullBytes := depth / 8
	remainingBits := depth % 8
	for i := fullBytes + 1; i < len(key); i++ {
		key[i] = 0
	}
	if remainingBits == 0 {
		for i := fullBytes; i < len(key); i++ {
			key[i] = 0
		}
		return key
	}
	mask := byte(0xff << (8 - remainingBits))
	key[fullBytes] &= mask
	return key
}

func sortedKeyedLeaves(leaves []UtxoLeaf) []keyedLeaf {
	sorted := make([]keyedLeaf, len(leaves))
	workers := runtime.GOMAXPROCS(0)
	if workers <= 1 || len(leaves) < parallelLeafHashThreshold {
		for i, leaf := range leaves {
			sorted[i] = keyedLeaf{
				key:  leafKey(leaf.OutPoint),
				hash: LeafHash(leaf),
				utxo: leaf,
			}
		}
	} else {
		if workers > len(leaves) {
			workers = len(leaves)
		}
		chunkSize := (len(leaves) + workers - 1) / workers
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			start := worker * chunkSize
			if start >= len(leaves) {
				break
			}
			end := start + chunkSize
			if end > len(leaves) {
				end = len(leaves)
			}
			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				for i := start; i < end; i++ {
					sorted[i] = keyedLeaf{
						key:  leafKey(leaves[i].OutPoint),
						hash: LeafHash(leaves[i]),
						utxo: leaves[i],
					}
				}
			}(start, end)
		}
		wg.Wait()
	}
	slices.SortFunc(sorted, func(a, b keyedLeaf) int {
		return bytes.Compare(a.key[:], b.key[:])
	})
	return sorted
}

func ensureUniqueSortedLeaves(sorted []keyedLeaf) error {
	for i := 1; i < len(sorted); i++ {
		if sorted[i-1].key == sorted[i].key {
			return fmt.Errorf("duplicate outpoint in UTXO commitment")
		}
	}
	return nil
}

// The accumulator materializes only leaves and genuine two-child branches.
// Skipped unary depths are commitment-transparent and are represented by the
// exact depth and prefix stored on the next material node.
func buildAccumulatorTree(leaves []keyedLeaf, bitIndex int, budget int) *accNode {
	if len(leaves) == 1 {
		return makeLeaf(leaves[0])
	}
	depth := firstDifferentBitFrom(leaves[0].key, leaves[len(leaves)-1].key, bitIndex)
	if depth < 0 {
		panic("duplicate outpoint in UTXO commitment")
	}
	split := splitAtBit(leaves, depth)
	leftBudget, rightBudget := splitParallelBudget(budget)
	if budget > 0 && len(leaves) >= parallelTreeBuildThreshold {
		var left *accNode
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			left = buildAccumulatorTree(leaves[:split], depth+1, leftBudget)
		}()
		right := buildAccumulatorTree(leaves[split:], depth+1, rightBudget)
		wg.Wait()
		return makeBranch(depth, leaves[0].key, left, right)
	}
	left := buildAccumulatorTree(leaves[:split], depth+1, 0)
	right := buildAccumulatorTree(leaves[split:], depth+1, 0)
	return makeBranch(depth, leaves[0].key, left, right)
}

func parallelBuildBudget() int {
	workers := runtime.GOMAXPROCS(0)
	if workers <= 1 {
		return 0
	}
	return workers - 1
}

func splitParallelBudget(budget int) (int, int) {
	if budget <= 1 {
		return 0, 0
	}
	left := (budget - 1) / 2
	right := budget - 1 - left
	return left, right
}

func insertLeaf(node *accNode, leaf keyedLeaf) (*accNode, error) {
	if node == nil {
		return makeLeaf(leaf), nil
	}
	divergence := firstDifferentBitFrom(node.path.Key, leaf.key, 0)
	if divergence < 0 {
		return nil, fmt.Errorf("duplicate outpoint in UTXO commitment")
	}
	if divergence < node.path.Depth {
		newLeaf := makeLeaf(leaf)
		if bitSet(leaf.key, divergence) {
			return makeBranch(divergence, leaf.key, node, newLeaf), nil
		}
		return makeBranch(divergence, leaf.key, newLeaf, node), nil
	}
	if node.leaf != nil {
		return nil, fmt.Errorf("conflicting accumulator leaf at identical key depth")
	}
	if bitSet(leaf.key, node.path.Depth) {
		right, err := insertLeaf(node.right, leaf)
		if err != nil {
			return nil, err
		}
		return makeBranch(node.path.Depth, node.path.Key, node.left, right), nil
	}
	left, err := insertLeaf(node.left, leaf)
	if err != nil {
		return nil, err
	}
	return makeBranch(node.path.Depth, node.path.Key, left, node.right), nil
}

func deleteLeaf(node *accNode, key [outPointBytes]byte) (*accNode, bool, error) {
	if node == nil || !pathContains(node.path, AccumulatorNodePath{Depth: keyBits, Key: key}) {
		return nil, false, nil
	}
	if node.leaf != nil {
		return nil, true, nil
	}
	if bitSet(key, node.path.Depth) {
		right, deleted, err := deleteLeaf(node.right, key)
		if err != nil || !deleted {
			return nil, deleted, err
		}
		if right == nil {
			return node.left, true, nil
		}
		return makeBranch(node.path.Depth, node.path.Key, node.left, right), true, nil
	}
	left, deleted, err := deleteLeaf(node.left, key)
	if err != nil || !deleted {
		return nil, deleted, err
	}
	if left == nil {
		return node.right, true, nil
	}
	return makeBranch(node.path.Depth, node.path.Key, left, node.right), true, nil
}

func makeLeaf(leaf keyedLeaf) *accNode {
	leafCopy := leaf
	return &accNode{path: AccumulatorNodePath{Depth: keyBits, Key: leaf.key}, leaf: &leafCopy, hash: leaf.hash, count: 1}
}

func makeBranch(depth int, prefix [outPointBytes]byte, left, right *accNode) *accNode {
	if left == nil || right == nil {
		panic("compressed accumulator branch requires two children")
	}
	return &accNode{
		path:  AccumulatorNodePath{Depth: depth, Key: maskPathKey(prefix, depth)},
		left:  left,
		right: right,
		hash:  BranchHash(left.hash, right.hash),
		count: left.count + right.count,
	}
}

func splitAtBit(leaves []keyedLeaf, bitIndex int) int {
	for i, leaf := range leaves {
		if bitSet(leaf.key, bitIndex) {
			return i
		}
	}
	return len(leaves)
}

func sortedSpentKeys(spent []types.OutPoint) ([][outPointBytes]byte, error) {
	if len(spent) == 0 {
		return nil, nil
	}
	keys := make([][outPointBytes]byte, len(spent))
	for i, outPoint := range spent {
		keys[i] = leafKey(outPoint)
	}
	slices.SortFunc(keys, func(a, b [outPointBytes]byte) int {
		return bytes.Compare(a[:], b[:])
	})
	for i := 1; i < len(keys); i++ {
		if keys[i-1] == keys[i] {
			outPoint := outPointFromKey(keys[i])
			return nil, fmt.Errorf("duplicate accumulator spend %x:%d", outPoint.TxID, outPoint.Vout)
		}
	}
	return keys, nil
}

func outPointFromKey(key [outPointBytes]byte) types.OutPoint {
	var outPoint types.OutPoint
	copy(outPoint.TxID[:], key[:32])
	outPoint.Vout = binary.LittleEndian.Uint32(key[32:])
	return outPoint
}

func bitSet(key [outPointBytes]byte, bitIndex int) bool {
	byteIndex := bitIndex / 8
	bitOffset := 7 - (bitIndex % 8)
	return ((key[byteIndex] >> bitOffset) & 1) == 1
}

func leafKey(outPoint types.OutPoint) [outPointBytes]byte {
	var key [outPointBytes]byte
	copy(key[:32], outPoint.TxID[:])
	binary.LittleEndian.PutUint32(key[32:], outPoint.Vout)
	return key
}
