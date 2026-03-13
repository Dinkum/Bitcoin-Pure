package node

import (
	"slices"

	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

// LocalityOrderedUTXOs exposes the non-consensus locality index directly for
// snapshot packing or proof-serving paths that want recently-created coins
// clustered together.
func (s *Service) LocalityOrderedUTXOs(limit int) ([]storage.LocalityIndexedUTXO, error) {
	return s.chainState.Store().LoadLocalityOrderedUTXOs(limit)
}

// PlanUTXOProofBatch orders requested outpoints by locality sequence so future
// batched proof-serving paths can consume nearby leaves together.
func (s *Service) PlanUTXOProofBatch(outPoints []types.OutPoint) ([]types.OutPoint, error) {
	planned := append([]types.OutPoint(nil), outPoints...)
	type localityRank struct {
		seq uint64
		ok  bool
	}
	ranks := make(map[types.OutPoint]localityRank, len(planned))
	for _, outPoint := range planned {
		seq, ok, err := s.chainState.Store().LocalitySequence(outPoint)
		if err != nil {
			return nil, err
		}
		ranks[outPoint] = localityRank{seq: seq, ok: ok}
	}
	slices.SortFunc(planned, func(a, b types.OutPoint) int {
		rankA := ranks[a]
		rankB := ranks[b]
		switch {
		case rankA.ok && rankB.ok && rankA.seq < rankB.seq:
			return -1
		case rankA.ok && rankB.ok && rankA.seq > rankB.seq:
			return 1
		case rankA.ok && !rankB.ok:
			return -1
		case !rankA.ok && rankB.ok:
			return 1
		default:
			return compareSnapshotOutPoints(a, b)
		}
	})
	return planned, nil
}
