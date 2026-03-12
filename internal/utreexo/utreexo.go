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

type UtxoLeaf struct {
	OutPoint   types.OutPoint
	ValueAtoms uint64
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
	left  *accNode
	right *accNode
	leaf  *keyedLeaf
	hash  [32]byte
	count int
}

type OutPointProof struct {
	OutPoint   types.OutPoint
	Exists     bool
	ValueAtoms uint64
	PubKey     [32]byte
	Steps      []ProofStep
}

type ProofStep struct {
	HasSibling  bool
	SiblingHash [32]byte
}

func LeafHash(leaf UtxoLeaf) [32]byte {
	buf := make([]byte, 0, outPointBytes+8+32)
	key := leafKey(leaf.OutPoint)
	buf = append(buf, key[:]...)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, leaf.ValueAtoms)
	buf = append(buf, value...)
	buf = append(buf, leaf.PubKey[:]...)
	return crypto.TaggedHash(UTXOLeafTag, buf)
}

func BranchHash(left, right [32]byte) [32]byte {
	buf := make([]byte, 0, 64)
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
	root, err := insertLeaf(a.root, keyed, 0)
	if err != nil {
		return nil, err
	}
	return &Accumulator{root: root, count: a.count + 1}, nil
}

func (a *Accumulator) Delete(outPoint types.OutPoint) (*Accumulator, error) {
	if a == nil || a.root == nil {
		return nil, fmt.Errorf("missing accumulator leaf %x:%d", outPoint.TxID, outPoint.Vout)
	}
	root, deleted, err := deleteLeaf(a.root, leafKey(outPoint), 0)
	if err != nil {
		return nil, err
	}
	if !deleted {
		return nil, fmt.Errorf("missing accumulator leaf %x:%d", outPoint.TxID, outPoint.Vout)
	}
	return &Accumulator{root: root, count: a.count - 1}, nil
}

func (a *Accumulator) Apply(spent []types.OutPoint, created []UtxoLeaf) (*Accumulator, error) {
	if a == nil {
		a = NewAccumulator()
	}
	next := a
	var err error
	for _, outPoint := range spent {
		next, err = next.Delete(outPoint)
		if err != nil {
			return nil, err
		}
	}
	for _, leaf := range created {
		next, err = next.Add(leaf)
		if err != nil {
			return nil, err
		}
	}
	return next, nil
}

// Prove returns a single-outpoint Merklix proof over the current accumulator
// root. Exclusion proofs terminate at the first missing branch on the queried
// path; membership proofs carry the committed leaf payload.
func (a *Accumulator) Prove(outPoint types.OutPoint) (OutPointProof, error) {
	proof := OutPointProof{OutPoint: outPoint}
	if a == nil || a.root == nil {
		return proof, nil
	}
	key := leafKey(outPoint)
	steps := make([]ProofStep, 0, keyBits)
	node := a.root
	for bitIndex := 0; bitIndex < keyBits; bitIndex++ {
		if node == nil {
			return OutPointProof{}, fmt.Errorf("invalid accumulator state while proving %x:%d", outPoint.TxID, outPoint.Vout)
		}
		queryBit := bitSet(key, bitIndex)
		var next, sibling *accNode
		if queryBit {
			next = node.right
			sibling = node.left
		} else {
			next = node.left
			sibling = node.right
		}
		step := ProofStep{}
		if sibling != nil {
			step.HasSibling = true
			step.SiblingHash = sibling.hash
		}
		steps = append(steps, step)
		if next == nil {
			proof.Steps = steps
			return proof, nil
		}
		node = next
	}
	if node.leaf == nil || node.leaf.key != key {
		return OutPointProof{}, fmt.Errorf("invalid accumulator leaf for %x:%d", outPoint.TxID, outPoint.Vout)
	}
	proof.Exists = true
	proof.ValueAtoms = node.leaf.utxo.ValueAtoms
	proof.PubKey = node.leaf.utxo.PubKey
	proof.Steps = steps
	return proof, nil
}

// VerifyProof checks a membership or exclusion proof against a committed
// `utxo_root` without requiring access to the full UTXO set.
func VerifyProof(root [32]byte, proof OutPointProof) bool {
	if len(proof.Steps) == 0 {
		return !proof.Exists && root == crypto.TaggedHash(UTXORootTag, nil)
	}
	key := leafKey(proof.OutPoint)
	last := len(proof.Steps) - 1
	var current [32]byte
	if proof.Exists {
		current = LeafHash(UtxoLeaf{
			OutPoint:   proof.OutPoint,
			ValueAtoms: proof.ValueAtoms,
			PubKey:     proof.PubKey,
		})
	} else {
		bottom := proof.Steps[last]
		if !bottom.HasSibling {
			return false
		}
		current = bottom.SiblingHash
		last--
	}
	for bitIndex := last; bitIndex >= 0; bitIndex-- {
		step := proof.Steps[bitIndex]
		if !step.HasSibling {
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

// The accumulator keeps an explicit bit-by-bit trie so later Add/Delete calls
// can walk it deterministically, but unary nodes reuse their child hash so the
// committed root matches the compressed Merklix form.
func buildAccumulatorTree(leaves []keyedLeaf, bitIndex int, budget int) *accNode {
	if len(leaves) == 1 {
		return buildLeafPath(leaves[0], bitIndex)
	}
	if bitIndex >= outPointBytes*8 {
		panic("duplicate outpoint in UTXO commitment")
	}

	split := splitAtBit(leaves, bitIndex)
	switch {
	case split == 0:
		return makeAccNode(nil, buildAccumulatorTree(leaves, bitIndex+1, budget))
	case split == len(leaves):
		return makeAccNode(buildAccumulatorTree(leaves, bitIndex+1, budget), nil)
	default:
		leftBudget, rightBudget := splitParallelBudget(budget)
		if budget > 0 && len(leaves) >= parallelTreeBuildThreshold {
			var left *accNode
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				left = buildAccumulatorTree(leaves[:split], bitIndex+1, leftBudget)
			}()
			right := buildAccumulatorTree(leaves[split:], bitIndex+1, rightBudget)
			wg.Wait()
			return makeAccNode(left, right)
		}
		left := buildAccumulatorTree(leaves[:split], bitIndex+1, 0)
		right := buildAccumulatorTree(leaves[split:], bitIndex+1, 0)
		return makeAccNode(left, right)
	}
}

func buildLeafPath(leaf keyedLeaf, bitIndex int) *accNode {
	if bitIndex == keyBits {
		leafCopy := leaf
		return &accNode{
			leaf:  &leafCopy,
			hash:  leafCopy.hash,
			count: 1,
		}
	}
	if bitSet(leaf.key, bitIndex) {
		return makeAccNode(nil, buildLeafPath(leaf, bitIndex+1))
	}
	return makeAccNode(buildLeafPath(leaf, bitIndex+1), nil)
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

func insertLeaf(node *accNode, leaf keyedLeaf, bitIndex int) (*accNode, error) {
	if bitIndex == keyBits {
		if node != nil && node.leaf != nil {
			if node.leaf.key == leaf.key {
				return nil, fmt.Errorf("duplicate outpoint in UTXO commitment")
			}
			return nil, fmt.Errorf("conflicting accumulator leaf at identical key depth")
		}
		leafCopy := leaf
		return &accNode{
			leaf:  &leafCopy,
			hash:  leaf.hash,
			count: 1,
		}, nil
	}
	var left, right *accNode
	if node != nil {
		left = node.left
		right = node.right
	}
	if bitSet(leaf.key, bitIndex) {
		nextRight, err := insertLeaf(right, leaf, bitIndex+1)
		if err != nil {
			return nil, err
		}
		right = nextRight
	} else {
		nextLeft, err := insertLeaf(left, leaf, bitIndex+1)
		if err != nil {
			return nil, err
		}
		left = nextLeft
	}
	return makeAccNode(left, right), nil
}

func deleteLeaf(node *accNode, key [outPointBytes]byte, bitIndex int) (*accNode, bool, error) {
	if node == nil {
		return nil, false, nil
	}
	if bitIndex == keyBits {
		if node.leaf == nil || node.leaf.key != key {
			return nil, false, nil
		}
		return nil, true, nil
	}
	left := node.left
	right := node.right
	var deleted bool
	var err error
	if bitSet(key, bitIndex) {
		right, deleted, err = deleteLeaf(right, key, bitIndex+1)
	} else {
		left, deleted, err = deleteLeaf(left, key, bitIndex+1)
	}
	if err != nil || !deleted {
		return nil, deleted, err
	}
	return makeAccNode(left, right), true, nil
}

func makeAccNode(left, right *accNode) *accNode {
	switch {
	case left == nil && right == nil:
		return nil
	case left == nil:
		return &accNode{
			right: right,
			hash:  right.hash,
			count: right.count,
		}
	case right == nil:
		return &accNode{
			left:  left,
			hash:  left.hash,
			count: left.count,
		}
	default:
		return &accNode{
			left:  left,
			right: right,
			hash:  BranchHash(left.hash, right.hash),
			count: left.count + right.count,
		}
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
