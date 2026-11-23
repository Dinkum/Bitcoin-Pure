package wallet

import (
	"container/heap"
	"errors"
	"fmt"

	"bitcoin-pure/internal/types"
)

// BuildFanout plans and signs a sequence against one snapshot of the wallet.
// Selected inputs leave the heap, and a new change output can fund later sends.
func (s *Store) BuildFanout(name string, destinations []string, count int, amount, fee, feeRate uint64, utxos []SpendableUTXO) ([]SendPlan, error) {
	if len(destinations) == 0 || count <= 0 {
		return nil, errors.New("fanout needs at least one destination and a positive count")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, _, ok := s.findWallet(name)
	if !ok {
		return nil, ErrWalletNotFound
	}
	coins, err := spendableCoins(*entry, utxos)
	if err != nil {
		return nil, err
	}
	// A sorted slice already satisfies the heap invariant.
	available := coinHeap(coins)
	changeIndex := nextAddressIndex(*entry)
	plans := make([]SendPlan, 0, count)
	for i := range count {
		to := destinations[i%len(destinations)]
		plan, err := s.planSend(entry.Name, changeIndex, available.take, to, amount, fee, feeRate)
		if err != nil {
			return nil, fmt.Errorf("fanout plan %d/%d: %w", i+1, count, err)
		}
		plans = append(plans, plan)
		if plan.ChangeAddress != nil {
			heap.Push(&available, SelectedInput{
				OutPoint: types.OutPoint{TxID: plan.TransactionID, Vout: 1},
				Value:    plan.Change,
				Address:  *plan.ChangeAddress,
			})
			changeIndex++
		}
	}
	return plans, nil
}

type coinHeap []SelectedInput

func (h coinHeap) Len() int { return len(h) }

func (h coinHeap) Less(i, j int) bool { return compareCoins(h[i], h[j]) < 0 }

func (h coinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *coinHeap) Push(coin any) { *h = append(*h, coin.(SelectedInput)) }

func (h *coinHeap) Pop() any {
	last := len(*h) - 1
	coin := (*h)[last]
	(*h)[last] = SelectedInput{}
	*h = (*h)[:last]
	return coin
}

func (h *coinHeap) take(yield func(SelectedInput) bool) {
	for h.Len() > 0 {
		coin := heap.Pop(h).(SelectedInput)
		if !yield(coin) {
			return
		}
	}
}
