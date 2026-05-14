package node

import (
	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"
)

type FundingOutput struct {
	OutPoint  types.OutPoint
	Value     uint64
	PubKey    [32]byte
	BlockHash [32]byte
}

type StressLaneInfo struct {
	ReservePubKey   string `json:"reserve_pubkey"`
	ReserveUTXOs    int    `json:"reserve_utxos"`
	ReserveAtoms    uint64 `json:"reserve_atoms"`
	PendingBatches  int    `json:"pending_batches"`
	PendingOutputs  int    `json:"pending_outputs"`
	ReadyOutputs    int    `json:"ready_outputs"`
	LastPendingTxID string `json:"last_pending_txid,omitempty"`
}

type stressLaneBatch struct {
	TxID      [32]byte
	Outputs   []FundingOutput
	CreatedAt time.Time
}

func stressLaneSecretKey() [32]byte {
	return bpcrypto.TaggedHash("bpu-stress-lane-seed", []byte("regtest"))
}

func stressLaneReservePubKey() [32]byte {
	return bpcrypto.XOnlyPubKeyFromSecret(stressLaneSecretKey())
}

func signStressLaneTx(tx types.Transaction, prevValue uint64) (types.Transaction, error) {
	msg, err := consensus.SighashWithParams(&tx, 0, []consensus.UtxoEntry{
		consensus.UtxoEntryFromOutput(types.NewXOnlyOutput(prevValue, stressLaneReservePubKey())),
	}, consensus.RegtestParams())
	if err != nil {
		return types.Transaction{}, err
	}
	_, sig := bpcrypto.SignSchnorr(stressLaneSecretKey(), &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	return tx, nil
}

func buildStressLaneFanoutTx(prevOut types.OutPoint, prevValue uint64, pubKeys [][32]byte) (types.Transaction, []FundingOutput, error) {
	if len(pubKeys) == 0 {
		return types.Transaction{}, nil, errors.New("at least one pubkey is required")
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: make([]types.TxOutput, len(pubKeys)),
		},
	}
	for i, pubKey := range pubKeys {
		tx.Base.Outputs[i] = types.TxOutput{ValueAtoms: 1, PubKey: pubKey}
	}
	tx, err := signStressLaneTx(tx, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	fee := uint64(tx.EncodedLen())
	if prevValue <= fee || prevValue-fee < uint64(len(pubKeys)) {
		return types.Transaction{}, nil, consensus.ErrInputsLessThanOutputs
	}
	perOutput := (prevValue - fee) / uint64(len(pubKeys))
	remainder := (prevValue - fee) % uint64(len(pubKeys))
	for i := range tx.Base.Outputs {
		value := perOutput
		if i == len(tx.Base.Outputs)-1 {
			value += remainder
		}
		tx.Base.Outputs[i].ValueAtoms = value
	}
	tx, err = signStressLaneTx(tx, prevValue)
	if err != nil {
		return types.Transaction{}, nil, err
	}
	txid := consensus.TxID(&tx)
	outputs := make([]FundingOutput, 0, len(tx.Base.Outputs))
	for vout, output := range tx.Base.Outputs {
		outputs = append(outputs, FundingOutput{
			OutPoint: types.OutPoint{TxID: txid, Vout: uint32(vout)},
			Value:    output.ValueAtoms,
			PubKey:   output.PubKey,
		})
	}
	return tx, outputs, nil
}

func stressLaneBatchConfirmed(lookup consensus.UtxoLookup, outputs []FundingOutput) bool {
	for _, output := range outputs {
		entry, ok := lookup(output.OutPoint)
		if !ok || entry.ValueAtoms != output.Value || entry.PubKey != output.PubKey {
			return false
		}
	}
	return true
}

func (s *Service) stressLaneInfo() StressLaneInfo {
	confirmed := s.chainUtxoSnapshot()
	reserveKey := stressLaneReservePubKey()
	info := StressLaneInfo{ReservePubKey: hex.EncodeToString(reserveKey[:])}
	for _, utxo := range s.UTXOsByPubKeys([][32]byte{reserveKey}) {
		info.ReserveUTXOs++
		info.ReserveAtoms += utxo.Value
	}
	s.stressMu.Lock()
	defer s.stressMu.Unlock()
	lastPending := time.Time{}
	for txid, batch := range s.stressPending {
		if stressLaneBatchConfirmed(confirmed, batch.Outputs) {
			delete(s.stressPending, txid)
			continue
		}
		info.PendingBatches++
		info.PendingOutputs += len(batch.Outputs)
		if batch.CreatedAt.After(lastPending) {
			lastPending = batch.CreatedAt
			info.LastPendingTxID = hex.EncodeToString(txid[:])
		}
	}
	return info
}

func (s *Service) ensureStressLaneReserve() error {
	if s.stressLaneInfo().ReserveUTXOs > 0 {
		return nil
	}
	// Seed a confirmed reserve output once so live stress funding can fan out a
	// normal signed transaction and let any network miner confirm it.
	_, err := s.MineFundingOutputs([][32]byte{stressLaneReservePubKey()})
	return err
}

func (s *Service) createStressLaneBatch(keyHashes [][32]byte, reserveTopUp bool) (stressLaneBatch, StressLaneInfo, error) {
	if reserveTopUp {
		if err := s.ensureStressLaneReserve(); err != nil {
			return stressLaneBatch{}, StressLaneInfo{}, err
		}
	}
	reserveUTXOs := s.UTXOsByPubKeys([][32]byte{stressLaneReservePubKey()})
	if len(reserveUTXOs) == 0 {
		return stressLaneBatch{}, s.stressLaneInfo(), errors.New("no confirmed stress reserve is available")
	}
	best := reserveUTXOs[0]
	for _, utxo := range reserveUTXOs[1:] {
		if utxo.Value > best.Value {
			best = utxo
		}
	}
	tx, outputs, err := buildStressLaneFanoutTx(best.OutPoint, best.Value, keyHashes)
	if err != nil && reserveTopUp && errors.Is(err, consensus.ErrInputsLessThanOutputs) {
		if _, topUpErr := s.MineFundingOutputs([][32]byte{stressLaneReservePubKey()}); topUpErr != nil {
			return stressLaneBatch{}, StressLaneInfo{}, topUpErr
		}
		reserveUTXOs = s.UTXOsByPubKeys([][32]byte{stressLaneReservePubKey()})
		if len(reserveUTXOs) == 0 {
			return stressLaneBatch{}, StressLaneInfo{}, errors.New("stress reserve top-up did not publish a spendable output")
		}
		best = reserveUTXOs[0]
		for _, utxo := range reserveUTXOs[1:] {
			if utxo.Value > best.Value {
				best = utxo
			}
		}
		tx, outputs, err = buildStressLaneFanoutTx(best.OutPoint, best.Value, keyHashes)
	}
	if err != nil {
		return stressLaneBatch{}, s.stressLaneInfo(), err
	}
	if _, err := s.SubmitTx(tx); err != nil {
		return stressLaneBatch{}, s.stressLaneInfo(), err
	}
	batch := stressLaneBatch{
		TxID:      consensus.TxID(&tx),
		Outputs:   outputs,
		CreatedAt: time.Now(),
	}
	s.stressMu.Lock()
	s.stressPending[batch.TxID] = batch
	s.stressMu.Unlock()
	return batch, s.stressLaneInfo(), nil
}

func (s *Service) findActiveTxBlockHash(txid [32]byte) ([32]byte, bool, error) {
	var zero [32]byte
	s.stateMu.RLock()
	tip := s.chainState.ChainState().TipHeight()
	s.stateMu.RUnlock()
	if tip == nil {
		return zero, false, nil
	}
	for height := *tip + 1; height > 0; height-- {
		blockHeight := height - 1
		hash, err := s.chainState.Store().GetBlockHashByHeight(blockHeight)
		if err != nil {
			return zero, false, err
		}
		if hash == nil {
			continue
		}
		block, err := s.chainState.Store().GetBlock(hash)
		if err != nil {
			return zero, false, err
		}
		if block == nil {
			continue
		}
		for _, tx := range block.Txs {
			if consensus.TxID(&tx) == txid {
				return *hash, true, nil
			}
		}
	}
	return zero, false, nil
}

func (s *Service) waitForStressLaneBatch(batch stressLaneBatch, timeout time.Duration) ([]FundingOutput, StressLaneInfo, error) {
	if timeout <= 0 {
		timeout = stressLaneConfirmTimeout
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		confirmed := s.chainUtxoSnapshot()
		if stressLaneBatchConfirmed(confirmed, batch.Outputs) {
			outputs := append([]FundingOutput(nil), batch.Outputs...)
			if hash, ok, err := s.findActiveTxBlockHash(batch.TxID); err != nil {
				return nil, s.stressLaneInfo(), err
			} else if ok {
				for i := range outputs {
					outputs[i].BlockHash = hash
				}
			}
			s.stressMu.Lock()
			delete(s.stressPending, batch.TxID)
			s.stressMu.Unlock()
			info := s.stressLaneInfo()
			info.ReadyOutputs = len(outputs)
			return outputs, info, nil
		}
		select {
		case <-s.stopCh:
			return nil, s.stressLaneInfo(), io.EOF
		case <-time.After(250 * time.Millisecond):
		}
	}
	return nil, s.stressLaneInfo(), fmt.Errorf("timed out waiting for stress funding tx %x to confirm", batch.TxID)
}

func (s *Service) SeedStressLanes(keyHashes [][32]byte, reserveTopUp bool, waitForConfirmation bool) ([]FundingOutput, StressLaneInfo, [32]byte, error) {
	if !s.cfg.Profile.IsRegtestLike() {
		return nil, StressLaneInfo{}, [32]byte{}, errors.New("stress lanes are only available on regtest-style profiles")
	}
	batch, info, err := s.createStressLaneBatch(keyHashes, reserveTopUp)
	if err != nil {
		return nil, info, [32]byte{}, err
	}
	if !waitForConfirmation {
		return append([]FundingOutput(nil), batch.Outputs...), info, batch.TxID, nil
	}
	outputs, info, err := s.waitForStressLaneBatch(batch, stressLaneConfirmTimeout)
	return outputs, info, batch.TxID, err
}
