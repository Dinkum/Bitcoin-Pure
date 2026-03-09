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
	KeyHash    [32]byte
}

type keyedLeaf struct {
	key  [outPointBytes]byte
	hash [32]byte
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

func LeafHash(leaf UtxoLeaf) [32]byte {
	buf := make([]byte, 0, outPointBytes+8+32)
	key := leafKey(leaf.OutPoint)
	buf = append(buf, key[:]...)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, leaf.ValueAtoms)
	buf = append(buf, value...)
	buf = append(buf, leaf.KeyHash[:]...)
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

func (a *Accumulator) Add(leaf UtxoLeaf) (*Accumulator, error) {
	if a == nil {
		a = NewAccumulator()
	}
	keyed := keyedLeaf{
		key:  leafKey(leaf.OutPoint),
		hash: LeafHash(leaf),
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

func sortedKeyedLeaves(leaves []UtxoLeaf) []keyedLeaf {
	sorted := make([]keyedLeaf, len(leaves))
	workers := runtime.GOMAXPROCS(0)
	if workers <= 1 || len(leaves) < parallelLeafHashThreshold {
		for i, leaf := range leaves {
			sorted[i] = keyedLeaf{
				key:  leafKey(leaf.OutPoint),
				hash: LeafHash(leaf),
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
