package p2p

import "sync"

// PayloadBudget bounds payload buffers and decoded storage held by connections
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

// PayloadReservation follows a received message through decoding and handling.
// Reserve must precede allocation; Release is safe on every error path.
type PayloadReservation struct {
	budget   *PayloadBudget
	used     int64
	released bool
}

func (b *PayloadBudget) NewReservation() *PayloadReservation { return &PayloadReservation{budget: b} }

func (r *PayloadReservation) Reserve(size int) error {
	if size < 0 {
		return ErrPayloadBudget
	}
	b := r.budget
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	requested := int64(size)
	if r.released || (b.limit > 0 && (requested > b.limit || b.used > b.limit-requested)) {
		return ErrPayloadBudget
	}
	b.used += requested
	r.used += requested
	if b.used > b.peak {
		b.peak = b.used
	}
	return nil
}

func (r *PayloadReservation) Release() {
	if r == nil || r.budget == nil {
		return
	}
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	b.used -= r.used
	r.used = 0
}

func (b *PayloadBudget) TryAcquire(size int) (func(), bool) {
	r := b.NewReservation()
	if err := r.Reserve(size); err != nil {
		return nil, false
	}
	return r.Release, true
}

func (b *PayloadBudget) Stats() PayloadBudgetStats {
	if b == nil {
		return PayloadBudgetStats{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return PayloadBudgetStats{Limit: b.limit, Used: b.used, Peak: b.peak}
}
