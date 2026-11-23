package storage

import (
	"encoding/json"
	"errors"
	"log/slog"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
)

// StoredMempoolState keeps restart-only tx relay state out of consensus tables.
// The encoded transactions are replayed through normal mempool policy when the
// persisted tip no longer matches the committed chain tip.
type StoredMempoolState struct {
	Version   uint32               `json:"version"`
	Profile   types.ChainProfile   `json:"profile"`
	TipHeight uint64               `json:"tip_height"`
	TipHash   [32]byte             `json:"tip_hash"`
	Entries   []StoredMempoolEntry `json:"entries,omitempty"`
	Orphans   []StoredMempoolEntry `json:"orphans,omitempty"`
}

// StoredMempoolStateMeta is the durable checkpoint header for restart-only
// mempool persistence. The actual accepted/orphan records live under per-tx
// keys so flushes can upsert or delete only what changed.
type StoredMempoolStateMeta struct {
	Version   uint32             `json:"version"`
	Profile   types.ChainProfile `json:"profile"`
	TipHeight uint64             `json:"tip_height"`
	TipHash   [32]byte           `json:"tip_hash"`
}

type StoredMempoolEntry struct {
	Tx      []byte                        `json:"tx"`
	Summary consensus.TxValidationSummary `json:"summary"`
	AddedAt uint64                        `json:"added_at"`
	Missing []types.OutPoint              `json:"missing,omitempty"`
}

type StoredMempoolDeltaEntry struct {
	TxID  [32]byte
	Entry StoredMempoolEntry
}

// StoredMempoolStateDelta applies one atomically visible checkpoint update: a
// new tip header plus any accepted/orphan record upserts and deletes needed to
// bring the stored restart cache in sync with the live mempool.
type StoredMempoolStateDelta struct {
	Meta          StoredMempoolStateMeta
	EntryUpserts  []StoredMempoolDeltaEntry
	EntryDeletes  [][32]byte
	OrphanUpserts []StoredMempoolDeltaEntry
	OrphanDeletes [][32]byte
}

func (s *ChainStore) LoadMempoolState() (*StoredMempoolState, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	buf, err := readValue(snap, metaMempoolStateKey)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	var meta StoredMempoolStateMeta
	if err := json.Unmarshal(buf, &meta); err != nil {
		return nil, err
	}
	// Legacy restart checkpoints stored the full mempool in one JSON blob. Keep
	// decoding that format so reopening an older local data dir does not discard
	// restart state just because the storage layout changed.
	if meta.Version < 2 {
		var legacy StoredMempoolState
		if err := json.Unmarshal(buf, &legacy); err != nil {
			return nil, err
		}
		return &legacy, nil
	}
	entries, err := loadMempoolEntries(snap, mempoolEntryPrefix, mempoolEntryPrefixEnd)
	if err != nil {
		return nil, err
	}
	orphans, err := loadMempoolEntries(snap, mempoolOrphanPrefix, mempoolOrphanPrefixEnd)
	if err != nil {
		return nil, err
	}
	return &StoredMempoolState{
		Version:   meta.Version,
		Profile:   meta.Profile,
		TipHeight: meta.TipHeight,
		TipHash:   meta.TipHash,
		Entries:   entries,
		Orphans:   orphans,
	}, nil
}

func (s *ChainStore) WriteMempoolState(state *StoredMempoolState) error {
	if state == nil {
		return errors.New("mempool state is required")
	}
	version := state.Version
	if version < 2 {
		version = 2
	}
	delta := StoredMempoolStateDelta{
		Meta: StoredMempoolStateMeta{
			// Version 2 switches the on-disk layout to metadata plus per-tx keys.
			Version:   version,
			Profile:   state.Profile,
			TipHeight: state.TipHeight,
			TipHash:   state.TipHash,
		},
		EntryUpserts:  make([]StoredMempoolDeltaEntry, 0, len(state.Entries)),
		OrphanUpserts: make([]StoredMempoolDeltaEntry, 0, len(state.Orphans)),
	}
	for _, entry := range state.Entries {
		tx, err := types.DecodeTransactionWithLimits(entry.Tx, types.DefaultCodecLimits())
		if err != nil {
			return err
		}
		delta.EntryUpserts = append(delta.EntryUpserts, StoredMempoolDeltaEntry{
			TxID:  consensus.TxID(&tx),
			Entry: entry,
		})
	}
	for _, orphan := range state.Orphans {
		tx, err := types.DecodeTransactionWithLimits(orphan.Tx, types.DefaultCodecLimits())
		if err != nil {
			return err
		}
		delta.OrphanUpserts = append(delta.OrphanUpserts, StoredMempoolDeltaEntry{
			TxID:  consensus.TxID(&tx),
			Entry: orphan,
		})
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.clearMempoolStateWithBatch(batch); err != nil {
		return err
	}
	if err := applyMempoolStateDeltaWithBatch(batch, delta); err != nil {
		return err
	}
	if err := batch.Commit(bestEffortWriteOptions); err != nil {
		return err
	}
	s.logger.Debug("wrote full mempool state",
		slog.Uint64("tip_height", delta.Meta.TipHeight),
		slog.Int("entries", len(delta.EntryUpserts)),
		slog.Int("orphans", len(delta.OrphanUpserts)),
	)
	return nil
}

func (s *ChainStore) ApplyMempoolStateDelta(delta StoredMempoolStateDelta) error {
	if delta.Meta.Profile == "" {
		return errors.New("mempool state profile is required")
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := applyMempoolStateDeltaWithBatch(batch, delta); err != nil {
		return err
	}
	if err := batch.Commit(bestEffortWriteOptions); err != nil {
		return err
	}
	s.logger.Debug("applied mempool state delta",
		slog.Uint64("tip_height", delta.Meta.TipHeight),
		slog.Int("entry_upserts", len(delta.EntryUpserts)),
		slog.Int("entry_deletes", len(delta.EntryDeletes)),
		slog.Int("orphan_upserts", len(delta.OrphanUpserts)),
		slog.Int("orphan_deletes", len(delta.OrphanDeletes)),
	)
	return nil
}

func (s *ChainStore) ClearMempoolState() error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.clearMempoolStateWithBatch(batch); err != nil {
		return err
	}
	if err := batch.Commit(bestEffortWriteOptions); err != nil {
		return err
	}
	s.logger.Debug("cleared mempool state")
	return nil
}

func (s *ChainStore) clearMempoolStateWithBatch(batch *pebble.Batch) error {
	if err := batch.Delete(metaMempoolStateKey, nil); err != nil {
		return err
	}
	if err := batch.DeleteRange(mempoolEntryPrefix, mempoolEntryPrefixEnd, nil); err != nil {
		return err
	}
	return batch.DeleteRange(mempoolOrphanPrefix, mempoolOrphanPrefixEnd, nil)
}

func loadMempoolEntries(reader storeReader, lower, upper []byte) ([]StoredMempoolEntry, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	entries := make([]StoredMempoolEntry, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		var entry StoredMempoolEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

func applyMempoolStateDeltaWithBatch(batch *pebble.Batch, delta StoredMempoolStateDelta) error {
	if batch == nil {
		return errors.New("mempool delta batch is required")
	}
	encoded, err := json.Marshal(delta.Meta)
	if err != nil {
		return err
	}
	if err := batch.Set(metaMempoolStateKey, encoded, bestEffortWriteOptions); err != nil {
		return err
	}
	for _, entry := range delta.EntryUpserts {
		if err := writeMempoolBatchEntry(batch, mempoolEntryPrefix, entry); err != nil {
			return err
		}
	}
	for _, txid := range delta.EntryDeletes {
		if err := batch.Delete(mempoolEntryKey(mempoolEntryPrefix, txid), bestEffortWriteOptions); err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
	}
	for _, orphan := range delta.OrphanUpserts {
		if err := writeMempoolBatchEntry(batch, mempoolOrphanPrefix, orphan); err != nil {
			return err
		}
	}
	for _, txid := range delta.OrphanDeletes {
		if err := batch.Delete(mempoolEntryKey(mempoolOrphanPrefix, txid), bestEffortWriteOptions); err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
	}
	return nil
}

func writeMempoolBatchEntry(batch *pebble.Batch, prefix []byte, entry StoredMempoolDeltaEntry) error {
	encoded, err := json.Marshal(entry.Entry)
	if err != nil {
		return err
	}
	return batch.Set(mempoolEntryKey(prefix, entry.TxID), encoded, bestEffortWriteOptions)
}

func mempoolEntryKey(prefix []byte, txid [32]byte) []byte {
	key := make([]byte, len(prefix)+len(txid))
	copy(key, prefix)
	copy(key[len(prefix):], txid[:])
	return key
}
