package mempool

import (
	"bytes"
	"container/heap"
)

type evictionCandidate struct {
	Root [32]byte
	Fee  uint64
	Size int
}

// ensureMempoolCapacityLocked keeps the admitted transaction set within the
// configured byte cap by evicting the worst-fee descendant packages first. Any
// mempool parents required by the incoming transaction are protected so package
// admission cannot evict the chain the newcomer depends on.
func (p *Pool) ensureMempoolCapacityLocked(incomingFee uint64, incomingSize int, protected map[[32]byte]struct{}) error {
	maxBytes := p.cfg.MaxMempoolBytes
	if maxBytes <= 0 {
		return nil
	}
	if incomingSize > maxBytes {
		return ErrMempoolFull
	}
	usedBytes := p.stats.bytes
	if usedBytes+incomingSize <= maxBytes {
		return nil
	}

	bytesNeeded := usedBytes + incomingSize - maxBytes
	evict, freedBytes, freedFees, ok := p.planEvictionsLocked(bytesNeeded, protected)
	if !ok {
		return ErrMempoolFull
	}
	if compareFeeRates(incomingFee, incomingSize, freedFees, freedBytes) <= 0 {
		return ErrMempoolFull
	}
	p.removeEntriesLocked(evict)
	return nil
}

func (p *Pool) planEvictionsLocked(bytesNeeded int, protected map[[32]byte]struct{}) (map[[32]byte]struct{}, int, uint64, bool) {
	queue := evictionQueue{
		candidates: make([]evictionCandidate, 0, len(p.entries)),
		indexes:    make(map[[32]byte]int, len(p.entries)),
	}
	// The protected set contains every ancestor of every required parent.
	// Any package containing a protected entry therefore has a protected root.
	for id, entry := range p.entries {
		if _, keep := protected[id]; keep {
			continue
		}
		queue.indexes[id] = len(queue.candidates)
		queue.candidates = append(queue.candidates, evictionCandidate{
			Root: id,
			Fee:  entry.DescendantFees,
			Size: entry.DescendantSize,
		})
	}
	heap.Init(&queue)

	evict := make(map[[32]byte]struct{})
	freedBytes := 0
	var freedFees uint64
	for freedBytes < bytesNeeded {
		if queue.Len() == 0 {
			return nil, 0, 0, false
		}
		root := queue.candidates[0].Root
		removed := p.collectEvictionPackageLocked(root, evict)
		for _, entry := range removed {
			heap.Remove(&queue, queue.indexes[entry.TxID])
			freedBytes += entry.Size
			freedFees += entry.Fee
		}
		// Any remaining package can lose bytes only through a removed
		// descendant. Update only those ancestors before choosing again.
		for _, entry := range removed {
			for ancestor := range p.gatherAncestors(entry.Parents) {
				index, ok := queue.indexes[ancestor]
				if !ok {
					continue
				}
				queue.candidates[index].Size -= entry.Size
				queue.candidates[index].Fee -= entry.Fee
				heap.Fix(&queue, index)
			}
		}
	}
	return evict, freedBytes, freedFees, true
}

func (p *Pool) collectEvictionPackageLocked(root [32]byte, evict map[[32]byte]struct{}) []*Entry {
	var removed []*Entry
	stack := [][32]byte{root}
	for len(stack) != 0 {
		index := len(stack) - 1
		id := stack[index]
		stack = stack[:index]
		if _, exists := evict[id]; exists {
			continue
		}
		entry := p.entries[id]
		evict[id] = struct{}{}
		removed = append(removed, entry)
		for child := range entry.Children {
			stack = append(stack, child)
		}
	}
	return removed
}

type evictionQueue struct {
	candidates []evictionCandidate
	indexes    map[[32]byte]int
}

func (q evictionQueue) Len() int { return len(q.candidates) }

func (q evictionQueue) Less(i, j int) bool {
	return evictionCandidateLess(q.candidates[i], q.candidates[j])
}

func (q evictionQueue) Swap(i, j int) {
	q.candidates[i], q.candidates[j] = q.candidates[j], q.candidates[i]
	q.indexes[q.candidates[i].Root] = i
	q.indexes[q.candidates[j].Root] = j
}

func (q *evictionQueue) Push(raw any) {
	candidate := raw.(evictionCandidate)
	q.indexes[candidate.Root] = len(q.candidates)
	q.candidates = append(q.candidates, candidate)
}

func (q *evictionQueue) Pop() any {
	last := len(q.candidates) - 1
	candidate := q.candidates[last]
	q.candidates = q.candidates[:last]
	delete(q.indexes, candidate.Root)
	return candidate
}

func evictionCandidateLess(left, right evictionCandidate) bool {
	switch compareFeeRates(left.Fee, left.Size, right.Fee, right.Size) {
	case -1:
		return true
	case 1:
		return false
	}
	switch {
	case left.Fee < right.Fee:
		return true
	case left.Fee > right.Fee:
		return false
	case left.Size < right.Size:
		return true
	case left.Size > right.Size:
		return false
	default:
		return bytes.Compare(left.Root[:], right.Root[:]) < 0
	}
}
