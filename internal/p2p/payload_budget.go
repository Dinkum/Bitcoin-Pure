package p2p

import "sync"

// PayloadBudget bounds the aggregate payload buffers held by all connections
// that share it. Admission is deliberately non-blocking: under pressure, a
// peer is disconnected instead of reserving an unbounded queue of readers.
type PayloadBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
	peak  int64
}

type PayloadBudgetStats struct {
	Limit int64
	Used  int64
	Peak  int64
}

func NewPayloadBudget(limit int64) *PayloadBudget {
	if limit < 0 {
		limit = 0
	}
	return &PayloadBudget{limit: limit}
}

func (b *PayloadBudget) TryAcquire(size int) (func(), bool) {
	if b == nil || size == 0 {
		return func() {}, true
	}
	if size < 0 {
		return nil, false
	}
	requested := int64(size)
	b.mu.Lock()
	if b.limit > 0 && (requested > b.limit || b.used > b.limit-requested) {
		b.mu.Unlock()
		return nil, false
	}
	b.used += requested
	if b.used > b.peak {
		b.peak = b.used
	}
	b.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.used -= requested
			b.mu.Unlock()
		})
	}, true
}

func (b *PayloadBudget) Stats() PayloadBudgetStats {
	if b == nil {
		return PayloadBudgetStats{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return PayloadBudgetStats{Limit: b.limit, Used: b.used, Peak: b.peak}
}
