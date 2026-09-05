package node

import (
	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"time"
)

func (s *Service) SubmitTx(tx types.Transaction) (mempool.Admission, error) {
	startedAt := time.Now()
	admission, err, orphanCount, mempoolSize := s.submitSingleDecodedTxFrom(tx, nil)
	if err != nil {
		s.logger.Warn("transaction rejected",
			slog.Any("error", err),
			slog.Duration("admit_duration", time.Since(startedAt)),
		)
		return mempool.Admission{}, err
	}
	if admission.Orphaned {
		s.logger.Debug("transaction stored as orphan",
			slog.String("txid", hex.EncodeToString(admission.TxID[:])),
			slog.Int("inputs", len(tx.Base.Inputs)),
			slog.Int("outputs", len(tx.Base.Outputs)),
			slog.Int("orphan_count", orphanCount),
			slog.Int("evicted_orphans", admission.EvictedOrphans),
			slog.Duration("admit_duration", time.Since(startedAt)),
		)
		return admission, nil
	}
	s.logger.Debug("transaction accepted",
		slog.String("txid", hex.EncodeToString(admission.TxID[:])),
		slog.Int("inputs", len(tx.Base.Inputs)),
		slog.Int("outputs", len(tx.Base.Outputs)),
		slog.Uint64("fee", admission.Summary.Fee),
		slog.Int("accepted_txs", len(admission.Accepted)),
		slog.Int("mempool_size", mempoolSize),
		slog.Duration("admit_duration", time.Since(startedAt)),
	)
	return admission, nil
}

// SubmitTxBatch admits a local batch of already-decoded transactions through
// the same policy path used by packed RPC and peer batch relay. Results stay
// positional: a non-nil error at index i belongs to txs[i].
func (s *Service) SubmitTxBatch(txs []types.Transaction) ([]mempool.Admission, []error) {
	if len(txs) == 0 {
		return nil, nil
	}
	admissions, errs, _, _ := s.submitDecodedTxs(txs)
	return admissions, errs
}

func (s *Service) submitSingleDecodedTxFrom(tx types.Transaction, source *peerConn) (mempool.Admission, error, int, int) {
	startedAt := time.Now()
	s.releaseTxRequestsForTransactions([]types.Transaction{tx})
	admission := mempool.Admission{}
	var err error
	orphanCount := 0
	mempoolSize := 0
	defer func() {
		errorCount := 0
		if err != nil {
			errorCount = 1
		}
		s.perf.noteAdmissionDuration(time.Since(startedAt))
		attrs := []slog.Attr{
			slog.Int("tx_count", 1),
			slog.Int("accepted_txs", len(admission.Accepted)),
			slog.Int("error_count", errorCount),
			slog.Int("orphan_count", orphanCount),
			slog.Int("mempool_size", mempoolSize),
			slog.Duration("admit_duration", time.Since(startedAt)),
		}
		if source != nil {
			attrs = append(attrs, slog.String("source_addr", source.addr))
		} else {
			attrs = append(attrs, slog.String("source_addr", "local"))
		}
		s.logger.LogAttrs(context.Background(), slog.LevelDebug, "tx admission batch complete", attrs...)
	}()

	if err = s.rejectCache.lookup(tx, s.pool.Epoch(), s.chainTipHash()); err != nil {
		orphanCount = s.pool.OrphanCount()
		mempoolSize = s.pool.Count()
		return admission, err, orphanCount, mempoolSize
	}
	if err = s.avalancheManager().rejectionError(tx); err != nil {
		orphanCount = s.pool.OrphanCount()
		mempoolSize = s.pool.Count()
		s.rejectCache.remember(tx, err, s.pool.Epoch(), s.chainTipHash())
		return admission, err, orphanCount, mempoolSize
	}

	rules := consensus.DefaultConsensusRules()
	params := consensus.ParamsForProfile(s.cfg.Profile)
	_, spendHeight, chainLookup := s.chainUtxoSnapshotWithTip()
	admission, err = s.pool.AcceptTxWithLookupAndParamsAtHeight(tx, chainLookup, params, spendHeight, rules)
	orphanCount = s.pool.OrphanCount()
	mempoolSize = s.pool.Count()
	if err != nil {
		s.rejectCache.remember(tx, err, s.pool.Epoch(), s.chainTipHash())
		if errors.Is(err, mempool.ErrInputAlreadySpent) {
			s.avalancheManager().trackTx(tx, false, time.Now())
		}
		return admission, err, orphanCount, mempoolSize
	}

	s.noteAcceptedAdmissions([]mempool.Admission{admission})
	if len(admission.Accepted) > 0 {
		s.invalidateBlockTemplate("tx_admission")
		now := time.Now()
		for _, accepted := range admission.Accepted {
			s.avalancheManager().trackTx(accepted.Tx, true, now)
		}
		if source == nil && !admission.Orphaned {
			s.rebroadcastMu.Lock()
			s.localRebroadcast[consensus.TxID(&tx)] = now
			s.rebroadcastMu.Unlock()
		}
		s.relayAcceptedTxs(s.peerSnapshotExcluding(source), admission.Accepted, source)
	}
	if admission.Orphaned {
		s.scheduleMempoolPersistence()
	} else if len(admission.Accepted) > 0 {
		s.scheduleMempoolPersistence()
	}
	return admission, nil, orphanCount, mempoolSize
}

// Best-effort view for the diagnostic stress surface; RPC callers use the
// error-returning query so an unavailable index cannot become a zero balance.
func (s *Service) UTXOsByWatchItems(items []compactfilter.WatchItem) []PubKeyUTXO {
	out, err := s.walletUTXOsByWatchItems(items)
	if err != nil {
		s.logger.Warn("load wallet utxos by watch item failed", slog.Any("error", err))
	}
	return out
}

func (s *Service) walletUTXOsByWatchItems(items []compactfilter.WatchItem) ([]PubKeyUTXO, error) {
	if len(items) == 0 {
		return nil, nil
	}
	indexed, err := s.chainState.Store().WalletUTXOsByWatchItems(storageWatchItems(items))
	if err != nil {
		return nil, err
	}
	s.stateMu.RLock()
	view, ok := s.chainState.sharedCommittedView()
	s.stateMu.RUnlock()
	if !ok {
		return nil, ErrNoTip
	}
	params := consensus.ParamsForProfile(s.cfg.Profile)
	out := make([]PubKeyUTXO, 0, len(indexed))
	for _, indexedUTXO := range indexed {
		item := compactFilterItemForUTXO(indexedUTXO.Entry)
		confirmations := view.Height - indexedUTXO.Height + 1
		out = append(out, PubKeyUTXO{
			OutPoint:      indexedUTXO.OutPoint,
			Value:         indexedUTXO.Entry.ValueAtoms,
			Type:          item.Type,
			Payload32:     item.Payload32,
			PubKey:        indexedUTXO.Entry.PubKey,
			Height:        indexedUTXO.Height,
			Confirmations: confirmations,
			Coinbase:      indexedUTXO.Coinbase,
			Mature:        !indexedUTXO.Coinbase || confirmations >= params.CoinbaseMaturity,
		})
	}
	slices.SortFunc(out, func(a, b PubKeyUTXO) int {
		switch {
		case a.Type < b.Type:
			return -1
		case a.Type > b.Type:
			return 1
		}
		if cmp := bytes.Compare(a.Payload32[:], b.Payload32[:]); cmp != 0 {
			return cmp
		}
		if cmp := bytes.Compare(a.OutPoint.TxID[:], b.OutPoint.TxID[:]); cmp != 0 {
			return cmp
		}
		switch {
		case a.OutPoint.Vout < b.OutPoint.Vout:
			return -1
		case a.OutPoint.Vout > b.OutPoint.Vout:
			return 1
		default:
			return 0
		}
	})
	return out, nil
}

func (s *Service) annotateWalletUTXOOrigins(utxos []PubKeyUTXO) error {
	if len(utxos) == 0 {
		return nil
	}
	s.stateMu.RLock()
	view, ok := s.chainState.sharedCommittedView()
	s.stateMu.RUnlock()
	if !ok {
		return ErrNoTip
	}
	remaining := make(map[types.OutPoint]int, len(utxos))
	for i := range utxos {
		remaining[utxos[i].OutPoint] = i
	}
	params := consensus.ParamsForProfile(s.cfg.Profile)
	for height := view.Height + 1; height > 0 && len(remaining) > 0; height-- {
		blockHeight := height - 1
		hash, err := s.chainState.Store().GetBlockHashByHeight(blockHeight)
		if err != nil {
			return err
		}
		if hash == nil {
			continue
		}
		block, err := s.chainState.Store().GetBlock(hash)
		if err != nil {
			return err
		}
		if block == nil {
			continue
		}
		for txIndex := range block.Txs {
			txid := consensus.TxID(&block.Txs[txIndex])
			for vout := range block.Txs[txIndex].Base.Outputs {
				outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
				utxoIndex, ok := remaining[outPoint]
				if !ok {
					continue
				}
				confirmations := view.Height - blockHeight + 1
				utxos[utxoIndex].Height = blockHeight
				utxos[utxoIndex].Confirmations = confirmations
				utxos[utxoIndex].Coinbase = txIndex == 0
				utxos[utxoIndex].Mature = txIndex != 0 || confirmations >= params.CoinbaseMaturity
				delete(remaining, outPoint)
			}
		}
	}
	// If an output was found in the UTXO set but its creating block could not be
	// located, keep it unavailable rather than letting the wallet spend a coin
	// whose maturity status is unknown.
	for _, utxoIndex := range remaining {
		utxos[utxoIndex].Mature = false
	}
	return nil
}

func (s *Service) UTXOsByPubKeys(pubKeys [][32]byte) []PubKeyUTXO {
	items := make([]compactfilter.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, compactfilter.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return s.UTXOsByWatchItems(items)
}

func (s *Service) EstimateFeeRate(targetBlocks int) uint64 {
	if targetBlocks <= 0 {
		targetBlocks = 1
	}
	minRelay := s.cfg.MinRelayFeePerByte
	if minRelay == 0 {
		minRelay = 1
	}
	// The shared snapshot is immutable; only the separate rates slice is sorted.
	entries := s.pool.SnapshotShared()
	if len(entries) == 0 {
		return minRelay
	}
	rates := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.Size <= 0 {
			continue
		}
		rate := entry.Fee / uint64(entry.Size)
		if entry.Fee%uint64(entry.Size) != 0 {
			rate++
		}
		if rate < minRelay {
			rate = minRelay
		}
		rates = append(rates, rate)
	}
	if len(rates) == 0 {
		return minRelay
	}
	slices.Sort(rates)
	percentile := 75
	switch {
	case targetBlocks <= 1:
		percentile = 75
	case targetBlocks == 2:
		percentile = 60
	case targetBlocks == 3:
		percentile = 50
	default:
		percentile = 35
	}
	index := (len(rates) - 1) * percentile / 100
	if rates[index] < minRelay {
		return minRelay
	}
	return rates[index]
}

func (s *Service) WalletActivityByWatchItems(items []compactfilter.WatchItem, limit int) ([]WalletActivity, error) {
	if len(items) == 0 {
		return nil, nil
	}
	records, err := s.chainState.Store().WalletActivityByWatchItems(storageWatchItems(items), limit)
	if err != nil {
		return nil, err
	}
	out := make([]WalletActivity, 0, len(records))
	for _, record := range records {
		out = append(out, WalletActivity{
			TxID:      record.TxID,
			BlockHash: record.BlockHash,
			Height:    record.Height,
			Timestamp: time.Unix(int64(record.Timestamp), 0).UTC(),
			Coinbase:  record.Coinbase,
			Received:  record.Received,
			Sent:      record.Sent,
			Fee:       record.Fee,
			Net:       int64(record.Received) - int64(record.Sent),
		})
	}
	return out, nil
}

func (s *Service) WalletActivityByPubKeys(pubKeys [][32]byte, limit int) ([]WalletActivity, error) {
	items := make([]compactfilter.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, compactfilter.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return s.WalletActivityByWatchItems(items, limit)
}

func validateWalletActivityLimit(limit int) error {
	switch {
	case limit <= 0:
		return errors.New("wallet activity limit must be positive")
	case limit > maxWalletActivityLimit:
		return fmt.Errorf("wallet activity limit must be <= %d", maxWalletActivityLimit)
	default:
		return nil
	}
}

func walletActivityFromBlock(height uint64, hash [32]byte, block *types.Block, undo []storage.BlockUndoEntry, wanted map[compactfilter.WatchItem]struct{}) ([]WalletActivity, error) {
	if block == nil {
		return nil, nil
	}
	resolvedInputs, err := consensus.ResolveBlockInputEntries(block, undo)
	if err != nil {
		return nil, fmt.Errorf("resolve wallet activity block %x inputs: %w", hash, err)
	}
	timestamp := time.Unix(int64(block.Header.Timestamp), 0).UTC()
	out := make([]WalletActivity, 0, len(block.Txs))
	for i, tx := range block.Txs {
		received := uint64(0)
		for _, output := range tx.Base.Outputs {
			if _, ok := wanted[compactFilterItemForOutput(output)]; ok {
				received += output.ValueAtoms
			}
		}
		sent := uint64(0)
		inputSum := uint64(0)
		if i > 0 {
			for _, input := range tx.Base.Inputs {
				entry, ok := resolvedInputs[input.PrevOut]
				if !ok {
					return nil, fmt.Errorf("missing resolved wallet activity input for block %x: %v", hash, input.PrevOut)
				}
				inputSum += entry.ValueAtoms
				if _, ok := wanted[compactFilterItemForUTXO(entry)]; ok {
					sent += entry.ValueAtoms
				}
			}
		}
		if received == 0 && sent == 0 {
			continue
		}
		outputSum := uint64(0)
		for _, output := range tx.Base.Outputs {
			outputSum += output.ValueAtoms
		}
		fee := uint64(0)
		if i > 0 {
			fee = inputSum - outputSum
		}
		out = append(out, WalletActivity{
			TxID:      consensus.TxID(&tx),
			BlockHash: hash,
			Height:    height,
			Timestamp: timestamp,
			Coinbase:  i == 0,
			Received:  received,
			Sent:      sent,
			Fee:       fee,
			Net:       int64(received) - int64(sent),
		})
	}
	return out, nil
}

func compactFilterItemForOutput(output types.TxOutput) compactfilter.WatchItem {
	return compactfilter.WatchItemForOutput(output)
}

func compactFilterItemForUTXO(entry consensus.UtxoEntry) compactfilter.WatchItem {
	item := compactfilter.WatchItem{Type: entry.Type, Payload32: entry.Payload32}
	if item.Type == types.OutputXOnlyP2PK && item.Payload32 == ([32]byte{}) {
		item.Payload32 = entry.PubKey
	}
	return item
}

func compactFilterItemsForUndo(undo []storage.BlockUndoEntry) []compactfilter.WatchItem {
	if len(undo) == 0 {
		return nil
	}
	items := make([]compactfilter.WatchItem, 0, len(undo))
	for _, spent := range undo {
		items = append(items, compactFilterItemForUTXO(spent.Entry))
	}
	return items
}

func storageWatchItems(items []compactfilter.WatchItem) []storage.WalletWatchItem {
	out := make([]storage.WalletWatchItem, 0, len(items))
	for _, item := range items {
		out = append(out, storage.WalletWatchItem{Type: item.Type, Payload32: item.Payload32})
	}
	return out
}

func utxoLeafForOutput(outPoint types.OutPoint, output types.TxOutput) utreexo.UtxoLeaf {
	return consensus.UtxoLeafFromOutput(outPoint, output)
}

func utxoLeafForEntry(outPoint types.OutPoint, entry consensus.UtxoEntry) utreexo.UtxoLeaf {
	return consensus.UtxoLeafFromEntry(outPoint, entry)
}

func (s *Service) submitDecodedTxs(txs []types.Transaction) ([]mempool.Admission, []error, int, int) {
	return s.submitDecodedTxsFrom(txs, nil)
}

func (s *Service) submitDecodedTxsFrom(txs []types.Transaction, source *peerConn) ([]mempool.Admission, []error, int, int) {
	startedAt := time.Now()
	s.releaseTxRequestsForTransactions(txs)
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	acceptedCount := 0
	orphanCount := 0
	mempoolSize := 0
	defer func() {
		if len(txs) == 0 {
			return
		}
		s.perf.noteAdmissionDuration(time.Since(startedAt))
		errorCount := 0
		for _, err := range errs {
			if err != nil {
				errorCount++
			}
		}
		for _, admission := range admissions {
			acceptedCount += len(admission.Accepted)
		}
		attrs := []slog.Attr{
			slog.Int("tx_count", len(txs)),
			slog.Int("accepted_txs", acceptedCount),
			slog.Int("error_count", errorCount),
			slog.Int("orphan_count", orphanCount),
			slog.Int("mempool_size", mempoolSize),
			slog.Duration("admit_duration", time.Since(startedAt)),
		}
		if source != nil {
			attrs = append(attrs, slog.String("source_addr", source.addr))
		} else {
			attrs = append(attrs, slog.String("source_addr", "local"))
		}
		s.logger.LogAttrs(context.Background(), slog.LevelDebug, "tx admission batch complete", attrs...)
	}()
	if len(txs) == 1 {
		if err := s.rejectCache.lookup(txs[0], s.pool.Epoch(), s.chainTipHash()); err != nil {
			errs[0] = err
			orphanCount = s.pool.OrphanCount()
			mempoolSize = s.pool.Count()
			return admissions, errs, orphanCount, mempoolSize
		}
		freshAdmissions, freshErrs, orphanCount, mempoolSize := s.submitFreshDecodedTxsFrom(txs, source)
		admissions[0] = freshAdmissions[0]
		errs[0] = freshErrs[0]
		s.noteRejectedTransactions(txs, errs)
		return admissions, errs, orphanCount, mempoolSize
	}
	pendingTxs, pendingIndexes := s.filterRejectCacheMisses(txs, errs)
	if len(pendingTxs) == 0 {
		orphanCount = s.pool.OrphanCount()
		mempoolSize = s.pool.Count()
		return admissions, errs, orphanCount, mempoolSize
	}
	freshAdmissions, freshErrs, orphanCount, mempoolSize := s.submitFreshDecodedTxsFrom(pendingTxs, source)
	for i, idx := range pendingIndexes {
		admissions[idx] = freshAdmissions[i]
		errs[idx] = freshErrs[i]
	}
	s.noteRejectedTransactions(pendingTxs, freshErrs)
	return admissions, errs, orphanCount, mempoolSize
}

func (s *Service) submitFreshDecodedTxsFrom(txs []types.Transaction, source *peerConn) ([]mempool.Admission, []error, int, int) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	rules := consensus.DefaultConsensusRules()
	params := consensus.ParamsForProfile(s.cfg.Profile)
	if len(txs) == 1 {
		if err := s.avalancheManager().rejectionError(txs[0]); err != nil {
			errs[0] = err
			orphanCount := s.pool.OrphanCount()
			mempoolSize := s.pool.Count()
			return admissions, errs, orphanCount, mempoolSize
		}
		_, spendHeight, chainLookup := s.chainUtxoSnapshotWithTip()
		admission, err := s.pool.AcceptTxWithLookupAndParamsAtHeight(txs[0], chainLookup, params, spendHeight, rules)
		if err != nil {
			errs[0] = err
		} else {
			admissions[0] = admission
			accepted = append(accepted, admission.Accepted...)
		}
		orphanCount := s.pool.OrphanCount()
		mempoolSize := s.pool.Count()
		s.noteAcceptedAdmissions(admissions)
		if len(accepted) > 0 {
			s.invalidateBlockTemplate("tx_admission")
		}
		s.avalancheManager().noteAcceptedAdmissions(admissions)
		s.avalancheManager().noteRejectedConflicts(txs, errs)
		s.noteLocalOriginAcceptedTxs(txs, admissions, errs, source)
		s.relayAcceptedTxs(s.peerSnapshotExcluding(source), accepted, source)
		if mempoolAdmissionsChanged(admissions) {
			s.scheduleMempoolPersistence()
		}
		return admissions, errs, orphanCount, mempoolSize
	}
	if batchHasInternalDependencies(txs) {
		admissions, errs, accepted = s.submitDependentDecodedTxs(txs, rules)
		orphanCount := s.pool.OrphanCount()
		mempoolSize := s.pool.Count()
		s.noteAcceptedAdmissions(admissions)
		if len(accepted) > 0 {
			s.invalidateBlockTemplate("tx_admission")
		}
		s.avalancheManager().noteAcceptedAdmissions(admissions)
		s.avalancheManager().noteRejectedConflicts(txs, errs)
		s.noteLocalOriginAcceptedTxs(txs, admissions, errs, source)
		s.relayAcceptedTxs(s.peerSnapshotExcluding(source), accepted, source)
		if mempoolAdmissionsChanged(admissions) {
			s.scheduleMempoolPersistence()
		}
		return admissions, errs, orphanCount, mempoolSize
	}
	tipHash, spendHeight, chainUtxos := s.chainUtxoSnapshotWithTip()
	view := s.pool.AcquireSharedAdmissionView()
	prepared, prepareErrs := s.prepareAdmissionsParallel(txs, view, chainUtxos, tipHash, spendHeight, rules)
	view.Release()
	for i := range txs {
		if prepareErrs[i] != nil {
			errs[i] = prepareErrs[i]
			continue
		}
		if s.chainTipHash() != tipHash {
			retryAdmissions, retryErrs, retryAccepted := s.retryDecodedTxSuffix(txs[i:], rules)
			copy(admissions[i:], retryAdmissions)
			copy(errs[i:], retryErrs)
			accepted = append(accepted, retryAccepted...)
			break
		}
		admission, err := s.pool.CommitPreparedWithLookupAndParamsAtHeight(prepared[i], chainUtxos, tipHash, params, spendHeight, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admissions[i] = admission
		accepted = append(accepted, admission.Accepted...)
	}
	orphanCount := s.pool.OrphanCount()
	mempoolSize := s.pool.Count()
	s.noteAcceptedAdmissions(admissions)
	if len(accepted) > 0 {
		s.invalidateBlockTemplate("tx_admission")
	}
	s.avalancheManager().noteAcceptedAdmissions(admissions)
	s.avalancheManager().noteRejectedConflicts(txs, errs)
	s.noteLocalOriginAcceptedTxs(txs, admissions, errs, source)
	s.relayAcceptedTxs(s.peerSnapshotExcluding(source), accepted, source)
	if mempoolAdmissionsChanged(admissions) {
		s.scheduleMempoolPersistence()
	}
	return admissions, errs, orphanCount, mempoolSize
}

func (s *Service) filterRejectCacheMisses(txs []types.Transaction, errs []error) ([]types.Transaction, []int) {
	if len(txs) == 0 {
		return nil, nil
	}
	poolEpoch := s.pool.Epoch()
	tipHash := s.chainTipHash()
	pending := make([]types.Transaction, 0, len(txs))
	indexes := make([]int, 0, len(txs))
	for i, tx := range txs {
		if err := s.rejectCache.lookup(tx, poolEpoch, tipHash); err != nil {
			errs[i] = err
			continue
		}
		pending = append(pending, tx)
		indexes = append(indexes, i)
	}
	return pending, indexes
}

func (s *Service) noteRejectedTransactions(txs []types.Transaction, errs []error) {
	if len(txs) == 0 || len(txs) != len(errs) {
		return
	}
	poolEpoch := s.pool.Epoch()
	tipHash := s.chainTipHash()
	for i := range txs {
		if errs[i] == nil {
			continue
		}
		s.rejectCache.remember(txs[i], errs[i], poolEpoch, tipHash)
	}
}

func (s *Service) noteLocalOriginAcceptedTxs(txs []types.Transaction, admissions []mempool.Admission, errs []error, source *peerConn) {
	if source != nil || len(txs) == 0 {
		return
	}
	now := time.Now()
	s.rebroadcastMu.Lock()
	defer s.rebroadcastMu.Unlock()
	for i := range txs {
		if errs[i] != nil || admissions[i].Orphaned {
			continue
		}
		// Track only txs explicitly submitted by the local operator path, not
		// extra txs that happened to get promoted alongside them.
		s.localRebroadcast[consensus.TxID(&txs[i])] = now
	}
}

func (s *Service) reprocessTransactions(txs []types.Transaction, templateReason string, relay bool) ([]mempool.Admission, []error, []mempool.AcceptedTx) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	if len(txs) == 0 {
		return admissions, errs, accepted
	}
	rules := consensus.DefaultConsensusRules()
	if batchHasInternalDependencies(txs) {
		admissions, errs, accepted = s.submitDependentDecodedTxs(txs, rules)
	} else {
		admissions, errs, accepted = s.retryDecodedTxSuffix(txs, rules)
	}
	s.noteRejectedTransactions(txs, errs)
	s.noteAcceptedAdmissions(admissions)
	s.avalancheManager().noteAcceptedAdmissions(admissions)
	s.avalancheManager().noteRejectedConflicts(txs, errs)
	if len(accepted) > 0 {
		s.invalidateBlockTemplate(templateReason)
		if relay {
			s.broadcastAcceptedTxsToPeers(s.peerSnapshot(), accepted)
		}
	}
	if mempoolAdmissionsChanged(admissions) {
		s.scheduleMempoolPersistence()
	}
	return admissions, errs, accepted
}

func (s *Service) submitDependentDecodedTxs(txs []types.Transaction, rules consensus.ConsensusRules) ([]mempool.Admission, []error, []mempool.AcceptedTx) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	tipHash, spendHeight, chainUtxos := s.chainUtxoSnapshotWithTip()
	snapshot := s.pool.AdmissionSnapshot()
	retried := false
	var fallbackTipHash [32]byte
	var fallbackSpendHeight uint64
	var fallbackChainUtxos consensus.UtxoLookup

	for i, tx := range txs {
		if err := s.avalancheManager().rejectionError(tx); err != nil {
			errs[i] = err
			continue
		}
		if currentTip := s.chainTipHash(); currentTip != tipHash {
			if retried {
				if fallbackTipHash != currentTip {
					fallbackTipHash, fallbackSpendHeight, fallbackChainUtxos = s.chainUtxoSnapshotWithTip()
				}
				admission, err := s.pool.AcceptTxWithLookupAndParamsAtHeight(tx, fallbackChainUtxos, consensus.ParamsForProfile(s.cfg.Profile), fallbackSpendHeight, rules)
				if err != nil {
					errs[i] = err
					continue
				}
				admissions[i] = admission
				accepted = append(accepted, admission.Accepted...)
				continue
			}
			tipHash, spendHeight, chainUtxos = s.chainUtxoSnapshotWithTip()
			snapshot = s.pool.AdmissionSnapshot()
			retried = true
		}
		prepared, err := s.pool.PrepareAdmissionWithLookupAndParamsAtHeight(tx, snapshot, chainUtxos, consensus.ParamsForProfile(s.cfg.Profile), spendHeight, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		prepared.PreparedTip = tipHash
		prepared.HasPreparedTip = true
		admission, err := s.pool.CommitPreparedWithLookupAndParamsAtHeight(prepared, chainUtxos, tipHash, consensus.ParamsForProfile(s.cfg.Profile), spendHeight, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admissions[i] = admission
		accepted = append(accepted, admission.Accepted...)
		if admission.Orphaned {
			snapshot.Orphans[prepared.TxID] = struct{}{}
			continue
		}
		if err := mempool.AdvanceAdmissionSnapshotWithLookup(&snapshot, chainUtxos, admission.Accepted); err != nil {
			snapshot = s.pool.AdmissionSnapshot()
		}
	}
	return admissions, errs, accepted
}

func batchHasInternalDependencies(txs []types.Transaction) bool {
	if len(txs) < 2 {
		return false
	}
	batchIDs := make(map[[32]byte]struct{}, len(txs))
	for i := range txs {
		batchIDs[consensus.TxID(&txs[i])] = struct{}{}
	}
	for i := range txs {
		for _, input := range txs[i].Base.Inputs {
			if _, ok := batchIDs[input.PrevOut.TxID]; ok {
				return true
			}
		}
	}
	return false
}

func (s *Service) retryDecodedTxSuffix(txs []types.Transaction, rules consensus.ConsensusRules) ([]mempool.Admission, []error, []mempool.AcceptedTx) {
	admissions := make([]mempool.Admission, len(txs))
	errs := make([]error, len(txs))
	accepted := make([]mempool.AcceptedTx, 0, len(txs))
	if len(txs) == 0 {
		return admissions, errs, accepted
	}
	tipHash, spendHeight, chainUtxos := s.chainUtxoSnapshotWithTip()
	view := s.pool.AcquireSharedAdmissionView()
	prepared, prepareErrs := s.prepareAdmissionsParallel(txs, view, chainUtxos, tipHash, spendHeight, rules)
	view.Release()
	var fallbackTipHash [32]byte
	var fallbackSpendHeight uint64
	var fallbackChainUtxos consensus.UtxoLookup
	for i, tx := range txs {
		if err := s.avalancheManager().rejectionError(tx); err != nil {
			errs[i] = err
			continue
		}
		if prepareErrs[i] != nil {
			errs[i] = prepareErrs[i]
			continue
		}
		if currentTip := s.chainTipHash(); currentTip != tipHash {
			if fallbackTipHash != currentTip {
				fallbackTipHash, fallbackSpendHeight, fallbackChainUtxos = s.chainUtxoSnapshotWithTip()
			}
			admission, err := s.pool.AcceptTxWithLookupAndParamsAtHeight(tx, fallbackChainUtxos, consensus.ParamsForProfile(s.cfg.Profile), fallbackSpendHeight, rules)
			if err != nil {
				errs[i] = err
				continue
			}
			admissions[i] = admission
			accepted = append(accepted, admission.Accepted...)
			continue
		}
		admission, err := s.pool.CommitPreparedWithLookupAndParamsAtHeight(prepared[i], chainUtxos, tipHash, consensus.ParamsForProfile(s.cfg.Profile), spendHeight, rules)
		if err != nil {
			errs[i] = err
			continue
		}
		admissions[i] = admission
		accepted = append(accepted, admission.Accepted...)
	}
	return admissions, errs, accepted
}

func decodePackedTransactions(encoded string) ([]types.Transaction, error) {
	if encoded == "" {
		return nil, nil
	}
	buf, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	txs := make([]types.Transaction, 0)
	limits := types.DefaultCodecLimits()
	for len(buf) > 0 {
		if len(buf) < 4 {
			return nil, errors.New("truncated packed transaction length")
		}
		size := binary.LittleEndian.Uint32(buf[:4])
		buf = buf[4:]
		if size == 0 {
			return nil, errors.New("packed transaction length must be non-zero")
		}
		if uint32(len(buf)) < size {
			return nil, errors.New("truncated packed transaction payload")
		}
		tx, err := types.DecodeTransactionWithLimits(buf[:size], limits)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
		buf = buf[size:]
	}
	return txs, nil
}

func (s *Service) prepareAdmissionsParallel(txs []types.Transaction, view mempool.SharedAdmissionView, chainUtxos consensus.UtxoLookup, tipHash [32]byte, spendHeight uint64, rules consensus.ConsensusRules) ([]mempool.PreparedAdmission, []error) {
	prepared := make([]mempool.PreparedAdmission, len(txs))
	errs := make([]error, len(txs))
	if len(txs) == 0 {
		return prepared, errs
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(txs) {
		workers = len(txs)
	}
	indexCh := make(chan int, len(txs))
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range indexCh {
				if err := s.avalancheManager().rejectionError(txs[idx]); err != nil {
					errs[idx] = err
					continue
				}
				item, err := s.pool.PrepareAdmissionSharedWithLookupAndParamsAtHeight(txs[idx], view, chainUtxos, consensus.ParamsForProfile(s.cfg.Profile), spendHeight, rules)
				if err != nil {
					errs[idx] = err
					continue
				}
				item.PreparedTip = tipHash
				item.HasPreparedTip = true
				prepared[idx] = item
			}
		}()
	}
	for i := range txs {
		indexCh <- i
	}
	close(indexCh)
	wg.Wait()
	return prepared, errs
}
