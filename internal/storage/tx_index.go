package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
)

const (
	txIndexVersion      = 1
	txIndexBatchEntries = 512
	txIndexBatchTxBytes = 8 << 20
)

var (
	txIndexPrefix    = []byte("tx_index/")
	txIndexPrefixEnd = prefixUpperBound(txIndexPrefix)
	txIndexStateKey  = []byte("meta/tx_index")
	// Incomplete history cannot establish that a transaction never confirmed.
	ErrTransactionHistoryUnavailable = errors.New("transaction status requires unavailable historical blocks")
)

type txIndexTip struct {
	Height uint64
	Hash   [32]byte
}

// A checkpoint and its rows share one WAL batch. Building checkpoints are never
// authoritative for queries, including after restart. Base is a previously
// complete prefix; a new tail must link to it before that prefix can be reused.
type txIndexCheckpoint struct {
	Version  int
	Target   txIndexTip
	Base     *txIndexTip `json:",omitempty"`
	Next     txIndexTip
	Offset   uint64
	Building bool
}

// TxIndexStatus describes acceleration availability, not consensus state.
type TxIndexStatus struct {
	Enabled       bool    `json:"enabled"`
	Synced        bool    `json:"synced"`
	IndexedHeight *uint64 `json:"indexed_height,omitempty"`
}

func txIndexKey(txid [32]byte, height uint64) []byte {
	key := make([]byte, len(txIndexPrefix)+32+8)
	copy(key, txIndexPrefix)
	copy(key[len(txIndexPrefix):], txid[:])
	binary.BigEndian.PutUint64(key[len(key)-8:], height)
	return key
}

func readTxIndexTip(reader valueReader) (*txIndexTip, error) {
	heightBytes, err := readValue(reader, metaTipHeightKey)
	if err != nil {
		return nil, err
	}
	headerBytes, err := readValue(reader, metaTipHeaderKey)
	if err != nil {
		return nil, err
	}
	if heightBytes == nil && headerBytes == nil {
		return nil, nil
	}
	height, err := decodeU64(heightBytes)
	if err != nil {
		return nil, err
	}
	header, err := types.DecodeBlockHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	return &txIndexTip{Height: height, Hash: consensus.HeaderHash(&header)}, nil
}

func readTxIndexCheckpoint(reader valueReader) (*txIndexCheckpoint, error) {
	raw, err := readValue(reader, txIndexStateKey)
	if err != nil || raw == nil {
		return nil, err
	}
	var state txIndexCheckpoint
	if len(raw) > 4096 {
		return nil, errors.New("oversized transaction index checkpoint")
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	if state.Version != txIndexVersion || state.Next.Height > state.Target.Height ||
		(!state.Building && (state.Offset != 0 || state.Base != nil)) ||
		(state.Base != nil && state.Base.Height >= state.Next.Height) {
		return nil, errors.New("invalid transaction index checkpoint")
	}
	return &state, nil
}

func writeTxIndexCheckpoint(batch *pebble.Batch, state txIndexCheckpoint) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return batch.Set(txIndexStateKey, raw, nil)
}

func (s *ChainStore) resetTxIndex(tip txIndexTip) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(txIndexPrefix, txIndexPrefixEnd, nil); err != nil {
		return err
	}
	if err := writeTxIndexCheckpoint(batch, txIndexCheckpoint{Version: txIndexVersion, Target: tip, Next: tip, Building: true}); err != nil {
		return err
	}
	// This is derived data. Lost unsynced work is replayed from the last recovered
	// atomic checkpoint; it never changes the durability of canonical chain writes.
	return batch.Commit(pebble.NoSync)
}

func (s *ChainStore) TxIndexStatus() TxIndexStatus {
	status := TxIndexStatus{Enabled: s.txIndexEnabled}
	if !status.Enabled {
		return status
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	tip, err := readTxIndexTip(snap)
	if err != nil || tip == nil {
		return status
	}
	state, err := readTxIndexCheckpoint(snap)
	if err != nil || state == nil {
		return status
	}
	if !state.Building {
		h := state.Target.Height
		status.IndexedHeight = &h
		status.Synced = state.Target == *tip
	} else if state.Base != nil {
		h := state.Base.Height
		status.IndexedHeight = &h
	}
	return status
}

// FindActiveTransaction reads index coverage and the canonical tip from the
// same snapshot. A lagging/reorged/rebuilding index cannot answer negatively or
// positively; the fallback follows the actual parent chain in that snapshot.
func (s *ChainStore) FindActiveTransaction(txid [32]byte) ([32]byte, bool, error) {
	var zero [32]byte
	snap := s.db.NewSnapshot()
	defer snap.Close()
	tip, err := readTxIndexTip(snap)
	if err != nil || tip == nil {
		return zero, false, err
	}
	if s.txIndexEnabled {
		state, indexErr := readTxIndexCheckpoint(snap)
		if indexErr == nil && state != nil && !state.Building && state.Target == *tip {
			hash, found, indexErr := lookupIndexedTransaction(snap, txid, tip.Height)
			if indexErr == nil {
				return hash, found, nil
			}
			s.txIndexRebuild.Store(true)
			s.notifyDerivedReplay()
		}
	}
	return scanActiveTransaction(snap, *tip, txid)
}

func lookupIndexedTransaction(snap *pebble.Snapshot, txid [32]byte, tipHeight uint64) ([32]byte, bool, error) {
	var hash [32]byte
	prefix := txIndexKey(txid, 0)[:len(txIndexPrefix)+32]
	iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return hash, false, err
	}
	defer iter.Close()
	if !iter.Last() {
		return hash, false, iter.Error()
	}
	if len(iter.Key()) != len(prefix)+8 || len(iter.Value()) != 32 || binary.BigEndian.Uint64(iter.Key()[len(prefix):]) > tipHeight {
		return hash, false, errors.New("invalid active transaction index row")
	}
	copy(hash[:], iter.Value())
	return hash, true, nil
}

func scanActiveTransaction(reader valueReader, tip txIndexTip, txid [32]byte) ([32]byte, bool, error) {
	var zero [32]byte
	hash := tip.Hash
	missing := false
	for height := tip.Height; ; height-- {
		block, err := getBlockFrom(reader, &hash)
		if err != nil {
			return zero, false, err
		}
		var parent [32]byte
		if block != nil {
			for i := range block.Txs {
				if consensus.TxID(&block.Txs[i]) == txid {
					return hash, true, nil
				}
			}
			parent = block.Header.PrevBlockHash
		} else {
			missing = true
			index, err := getBlockIndexFrom(reader, &hash)
			if err != nil {
				return zero, false, err
			}
			if index == nil {
				return zero, false, ErrTransactionHistoryUnavailable
			}
			parent = index.Header.PrevBlockHash
		}
		if height == 0 {
			break
		}
		hash = parent
	}
	if missing {
		return zero, false, ErrTransactionHistoryUnavailable
	}
	return zero, false, nil
}

// The worker retains at most one decoded block. Row batches are bounded even
// when a block contains many transactions, and the persisted offset permits
// restart partway through a block without rebuilding the whole chain.
type txIndexWorker struct {
	store     *ChainStore
	block     *types.Block
	blockHash [32]byte
}

func (s *ChainStore) txIndexLoop() {
	worker := txIndexWorker{store: s}
	var lastError string
	var lastErrorAt time.Time
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		progress, err := worker.step()
		delay := time.Second
		if err != nil {
			// Missing snapshot history can persist for a long time. Keep the
			// condition visible without emitting the same warning every second.
			if err.Error() != lastError || time.Since(lastErrorAt) >= time.Minute {
				s.logger.Warn("transaction index unavailable; status queries use chain history", slog.Any("error", err))
				lastError, lastErrorAt = err.Error(), time.Now()
			}
		} else {
			lastError = ""
		}
		if progress && err == nil {
			delay = time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.stopCh:
			timer.Stop()
			return
		case <-s.txIndexNotify:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (w *txIndexWorker) step() (bool, error) {
	s := w.store
	snap := s.db.NewSnapshot()
	tip, err := readTxIndexTip(snap)
	if err != nil || tip == nil {
		snap.Close()
		return false, err
	}
	state, stateErr := readTxIndexCheckpoint(snap)
	snap.Close()
	if stateErr != nil {
		// Invalid/versioned checkpoints are disposable. If reads themselves are
		// failing, reset also fails; no canonical data is changed in either case.
		s.logger.Warn("rebuilding unreadable transaction index", slog.Any("error", stateErr))
	}
	if state == nil || stateErr != nil || s.txIndexRebuild.Swap(false) {
		w.block = nil
		return true, s.resetTxIndex(*tip)
	}
	if !state.Building {
		if state.Target == *tip {
			w.block = nil
			return false, nil
		}
		if tip.Height <= state.Target.Height {
			w.block = nil
			return true, s.resetTxIndex(*tip)
		}
		base := state.Target
		*state = txIndexCheckpoint{Version: txIndexVersion, Target: *tip, Base: &base, Next: *tip, Building: true}
		batch := s.db.NewBatch()
		defer batch.Close()
		if err := writeTxIndexCheckpoint(batch, *state); err != nil {
			return false, err
		}
		return true, batch.Commit(pebble.NoSync)
	}
	// A same-height branch replacement or shorter installed state cannot extend
	// this build. Longer branches are checked when their tail reaches Base.
	if tip.Height < state.Target.Height || (tip.Height == state.Target.Height && tip.Hash != state.Target.Hash) {
		w.block = nil
		return true, s.resetTxIndex(*tip)
	}
	if w.block == nil || w.blockHash != state.Next.Hash {
		block, err := s.GetBlock(&state.Next.Hash)
		if err != nil {
			return false, err
		}
		if block == nil {
			return false, ErrTransactionHistoryUnavailable
		}
		w.block = block
		w.blockHash = state.Next.Hash
	}
	block := w.block
	if len(block.Txs) == 0 {
		return false, errors.New("cannot index an empty historical block")
	}
	if state.Offset > uint64(len(block.Txs)) {
		w.block = nil
		return true, s.resetTxIndex(*tip)
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	count, txBytes := 0, 0
	for state.Offset < uint64(len(block.Txs)) && count < txIndexBatchEntries && (count == 0 || txBytes < txIndexBatchTxBytes) {
		select {
		case <-s.stopCh:
			return false, nil
		default:
		}
		tx := &block.Txs[state.Offset]
		if err := batch.Set(txIndexKey(consensus.TxID(tx), state.Next.Height), state.Next.Hash[:], nil); err != nil {
			return false, err
		}
		state.Offset++
		count++
		txBytes += tx.EncodedLen()
	}
	if state.Offset == uint64(len(block.Txs)) {
		switch {
		case state.Base != nil && state.Next.Height == state.Base.Height+1:
			if block.Header.PrevBlockHash != state.Base.Hash {
				w.block = nil
				return true, s.resetTxIndex(*tip)
			}
			state.Building = false
		case state.Base == nil && state.Next.Height == 0:
			state.Building = false
		default:
			if state.Next.Height == 0 {
				return false, errors.New("transaction index reached genesis before its base")
			}
			state.Next = txIndexTip{Height: state.Next.Height - 1, Hash: block.Header.PrevBlockHash}
		}
		state.Offset = 0
		w.block = nil
		if !state.Building {
			state.Base = nil
			state.Next = state.Target
		}
	}
	if err := writeTxIndexCheckpoint(batch, *state); err != nil {
		return false, err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return false, err
	}
	if !state.Building {
		s.logger.Info("transaction index caught up", slog.Uint64("height", state.Target.Height))
	}
	return true, nil
}
