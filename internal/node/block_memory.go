package node

import (
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"unsafe"
)

// Include the wire buffer because owned-buffer block decoding borrows auth
// bytes from it. Counting decoded arrays as well keeps retained owners bounded
// even for blocks with many small inputs or outputs.
func blockRetainedBytes(block *types.Block) uint64 {
	if block == nil {
		return 0
	}
	bytes := uint64(block.EncodedLen()) + uint64(cap(block.Txs))*uint64(unsafe.Sizeof(types.Transaction{}))
	for i := range block.Txs {
		bytes += transactionArrayBytes(&block.Txs[i])
	}
	return bytes
}
func transactionArrayBytes(tx *types.Transaction) uint64 {
	bytes := uint64(cap(tx.Base.Inputs))*uint64(unsafe.Sizeof(types.TxInput{})) +
		uint64(cap(tx.Base.Outputs))*uint64(unsafe.Sizeof(types.TxOutput{})) +
		uint64(cap(tx.Auth.Entries))*uint64(unsafe.Sizeof(types.TxAuthEntry{}))
	if tx.Base.CoinbaseHeight != nil {
		bytes += 8
	}
	if tx.Base.CoinbaseExtraNonce != nil {
		bytes += types.CoinbaseExtraNonceLen
	}
	return bytes
}

func (s *Service) reserveBlockReconstruction(count int) (*p2p.PayloadReservation, error) {
	// Alongside transaction slots and the fill bitmap, account for missing-index
	// growth and the temporary short-ID lookup maps before reconstruction starts.
	const perSlot = int(unsafe.Sizeof(types.Transaction{})) + 1 + 8 + 64
	if count < 0 || count > int(^uint(0)>>1)/perSlot {
		return nil, p2p.ErrPayloadBudget
	}
	reservation := s.payloadBudget.NewReservation()
	if err := reservation.Reserve(count * perSlot); err != nil {
		return nil, err
	}
	return reservation, nil
}
