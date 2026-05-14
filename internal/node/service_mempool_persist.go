package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"log/slog"
	"slices"
	"time"
)

type persistedMempoolState struct {
	Valid   bool
	Meta    storage.StoredMempoolStateMeta
	Entries map[[32]byte]persistedMempoolEntryFingerprint
	Orphans map[[32]byte]persistedMempoolOrphanFingerprint
}

type livePersistedMempoolSnapshot struct {
	State   persistedMempoolState
	Entries map[[32]byte]mempool.PersistedEntry
	Orphans map[[32]byte]mempool.PersistedOrphan
}

type persistedMempoolEntryFingerprint struct {
	AuthID  [32]byte
	Summary consensus.TxValidationSummary
	AddedAt uint64
}

type persistedMempoolOrphanFingerprint struct {
	AuthID  [32]byte
	AddedAt uint64
	Missing []types.OutPoint
}

func (s *Service) scheduleMempoolPersistence() {
	if s == nil || s.mempoolPersistCh == nil {
		return
	}
	select {
	case s.mempoolPersistCh <- struct{}{}:
	default:
	}
}

func (s *Service) mempoolPersistLoop() {
	const persistDebounce = 25 * time.Millisecond
	var timer *time.Timer
	var timerCh <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.mempoolPersistCh:
			if timer == nil {
				timer = time.NewTimer(persistDebounce)
				timerCh = timer.C
				continue
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(persistDebounce)
		case <-timerCh:
			timerCh = nil
			if err := s.flushMempoolPersistence(); err != nil && s.logger != nil {
				s.logger.Warn("mempool persistence flush failed", slog.Any("error", err))
			}
		}
	}
}

func (s *Service) flushMempoolPersistence() error {
	if s == nil || s.chainState == nil || s.chainState.Store() == nil {
		return nil
	}
	s.mempoolPersistMu.Lock()
	defer s.mempoolPersistMu.Unlock()

	snapshot, ok := s.buildPersistableMempoolSnapshot()
	if !ok {
		if !s.mempoolPersistState.Valid {
			return nil
		}
		if err := s.chainState.Store().ClearMempoolState(); err != nil {
			return err
		}
		s.mempoolPersistState = persistedMempoolState{}
		return nil
	}
	delta, changed := buildStoredMempoolDelta(snapshot, s.mempoolPersistState)
	if !changed {
		return nil
	}
	if err := s.chainState.Store().ApplyMempoolStateDelta(delta); err != nil {
		return err
	}
	s.mempoolPersistState = snapshot.State
	return nil
}

func (s *Service) buildPersistableMempoolSnapshot() (livePersistedMempoolSnapshot, bool) {
	if s == nil || s.pool == nil || s.chainState == nil {
		return livePersistedMempoolSnapshot{}, false
	}
	tip, ok := s.chainState.tipSnapshot()
	if !ok {
		return livePersistedMempoolSnapshot{}, false
	}
	entries := s.pool.PersistedEntries()
	orphans := s.pool.PersistedOrphans()
	if len(entries) == 0 && len(orphans) == 0 {
		return livePersistedMempoolSnapshot{}, false
	}
	snapshot := livePersistedMempoolSnapshot{
		State: persistedMempoolState{
			Valid: true,
			Meta: storage.StoredMempoolStateMeta{
				Version:   2,
				Profile:   s.cfg.Profile,
				TipHeight: tip.Height,
				TipHash:   tip.TipHash,
			},
			Entries: make(map[[32]byte]persistedMempoolEntryFingerprint, len(entries)),
			Orphans: make(map[[32]byte]persistedMempoolOrphanFingerprint, len(orphans)),
		},
		Entries: make(map[[32]byte]mempool.PersistedEntry, len(entries)),
		Orphans: make(map[[32]byte]mempool.PersistedOrphan, len(orphans)),
	}
	for _, entry := range entries {
		snapshot.State.Entries[entry.TxID] = persistedMempoolEntryFingerprint{
			AuthID:  entry.AuthID,
			Summary: entry.Summary,
			AddedAt: entry.AddedAt,
		}
		snapshot.Entries[entry.TxID] = entry
	}
	for _, orphan := range orphans {
		snapshot.State.Orphans[orphan.TxID] = persistedMempoolOrphanFingerprint{
			AuthID:  orphan.AuthID,
			AddedAt: orphan.AddedAt,
			Missing: append([]types.OutPoint(nil), orphan.Missing...),
		}
		snapshot.Orphans[orphan.TxID] = orphan
	}
	return snapshot, true
}

func buildStoredMempoolDelta(current livePersistedMempoolSnapshot, previous persistedMempoolState) (storage.StoredMempoolStateDelta, bool) {
	delta := storage.StoredMempoolStateDelta{
		Meta: current.State.Meta,
	}
	changed := !previous.Valid || previous.Meta != current.State.Meta

	for txid, entry := range current.Entries {
		if fingerprint, ok := previous.Entries[txid]; ok && samePersistedEntryFingerprint(fingerprint, entry) {
			continue
		}
		delta.EntryUpserts = append(delta.EntryUpserts, storage.StoredMempoolDeltaEntry{
			TxID: txid,
			Entry: storage.StoredMempoolEntry{
				Tx:      entry.Tx.Encode(),
				Summary: entry.Summary,
				AddedAt: entry.AddedAt,
			},
		})
		changed = true
	}
	for txid := range previous.Entries {
		if _, ok := current.State.Entries[txid]; ok {
			continue
		}
		delta.EntryDeletes = append(delta.EntryDeletes, txid)
		changed = true
	}
	for txid, orphan := range current.Orphans {
		if fingerprint, ok := previous.Orphans[txid]; ok && samePersistedOrphanFingerprint(fingerprint, orphan) {
			continue
		}
		delta.OrphanUpserts = append(delta.OrphanUpserts, storage.StoredMempoolDeltaEntry{
			TxID: txid,
			Entry: storage.StoredMempoolEntry{
				Tx:      orphan.Tx.Encode(),
				AddedAt: orphan.AddedAt,
				Missing: append([]types.OutPoint(nil), orphan.Missing...),
			},
		})
		changed = true
	}
	for txid := range previous.Orphans {
		if _, ok := current.State.Orphans[txid]; ok {
			continue
		}
		delta.OrphanDeletes = append(delta.OrphanDeletes, txid)
		changed = true
	}
	return delta, changed
}

func samePersistedEntryFingerprint(fingerprint persistedMempoolEntryFingerprint, entry mempool.PersistedEntry) bool {
	return fingerprint.AuthID == entry.AuthID &&
		fingerprint.Summary == entry.Summary &&
		fingerprint.AddedAt == entry.AddedAt
}

func samePersistedOrphanFingerprint(fingerprint persistedMempoolOrphanFingerprint, orphan mempool.PersistedOrphan) bool {
	return fingerprint.AuthID == orphan.AuthID &&
		fingerprint.AddedAt == orphan.AddedAt &&
		slices.Equal(fingerprint.Missing, orphan.Missing)
}

func (s *Service) reloadPersistedMempool() error {
	stored, err := s.chainState.Store().LoadMempoolState()
	if err != nil {
		return err
	}
	if stored == nil {
		return nil
	}
	if stored.Profile != s.cfg.Profile {
		s.logger.Warn("discarding persisted mempool for different profile",
			slog.String("stored_profile", stored.Profile.String()),
			slog.String("active_profile", s.cfg.Profile.String()),
		)
		return s.chainState.Store().ClearMempoolState()
	}
	accepted := make([]mempool.PersistedEntry, 0, len(stored.Entries))
	for i, entry := range stored.Entries {
		tx, err := types.DecodeTransactionWithLimits(entry.Tx, types.DefaultCodecLimits())
		if err != nil {
			s.logger.Warn("discarding persisted mempool after decode failure",
				slog.Int("entry_index", i),
				slog.Any("error", err),
			)
			_ = s.chainState.Store().ClearMempoolState()
			return nil
		}
		txid := consensus.TxID(&tx)
		accepted = append(accepted, mempool.PersistedEntry{
			TxID:    txid,
			AuthID:  consensus.AuthID(&tx),
			Tx:      tx,
			Summary: entry.Summary,
			AddedAt: entry.AddedAt,
		})
	}
	orphans := make([]mempool.PersistedOrphan, 0, len(stored.Orphans))
	for i, entry := range stored.Orphans {
		tx, err := types.DecodeTransactionWithLimits(entry.Tx, types.DefaultCodecLimits())
		if err != nil {
			s.logger.Warn("discarding persisted orphan after decode failure",
				slog.Int("orphan_index", i),
				slog.Any("error", err),
			)
			_ = s.chainState.Store().ClearMempoolState()
			return nil
		}
		txid := consensus.TxID(&tx)
		orphans = append(orphans, mempool.PersistedOrphan{
			TxID:    txid,
			AuthID:  consensus.AuthID(&tx),
			Tx:      tx,
			AddedAt: entry.AddedAt,
			Missing: append([]types.OutPoint(nil), entry.Missing...),
		})
	}
	s.mempoolPersistMu.Lock()
	s.mempoolPersistState = persistedStateFromStoredMempool(stored, accepted, orphans)
	s.mempoolPersistMu.Unlock()
	tip, ok := s.chainState.tipSnapshot()
	if !ok {
		return nil
	}
	if stored.TipHeight == tip.Height && stored.TipHash == tip.TipHash {
		if err := s.pool.RestorePersistedState(accepted, orphans); err != nil {
			return err
		}
		s.logger.Info("restored mempool from persisted state",
			slog.Int("entries", len(accepted)),
			slog.Int("orphans", len(orphans)),
			slog.Uint64("tip_height", tip.Height),
		)
		return s.flushMempoolPersistence()
	}
	reprocess := make([]types.Transaction, 0, len(accepted)+len(orphans))
	for _, entry := range accepted {
		reprocess = append(reprocess, entry.Tx)
	}
	for _, orphan := range orphans {
		reprocess = append(reprocess, orphan.Tx)
	}
	admissions, errs, acceptedTxs := s.reprocessTransactions(reprocess, "mempool_reload", false)
	s.logger.Info("reprocessed persisted mempool against current tip",
		slog.Int("submitted", len(reprocess)),
		slog.Int("accepted", countAcceptedAdmissions(admissions)),
		slog.Int("rejected", countNonNilErrors(errs)),
		slog.Int("promoted", len(acceptedTxs)),
		slog.Uint64("stored_tip_height", stored.TipHeight),
		slog.Uint64("active_tip_height", tip.Height),
	)
	return s.flushMempoolPersistence()
}

func persistedStateFromStoredMempool(stored *storage.StoredMempoolState, entries []mempool.PersistedEntry, orphans []mempool.PersistedOrphan) persistedMempoolState {
	if stored == nil {
		return persistedMempoolState{}
	}
	state := persistedMempoolState{
		Valid: true,
		Meta: storage.StoredMempoolStateMeta{
			Version:   stored.Version,
			Profile:   stored.Profile,
			TipHeight: stored.TipHeight,
			TipHash:   stored.TipHash,
		},
		Entries: make(map[[32]byte]persistedMempoolEntryFingerprint, len(entries)),
		Orphans: make(map[[32]byte]persistedMempoolOrphanFingerprint, len(orphans)),
	}
	if state.Meta.Version < 2 {
		state.Meta.Version = 2
		// Legacy checkpoints stored every tx inside meta/mempool_state. Treat
		// the v2 per-tx keyspace as empty until the next flush rewrites all
		// still-live entries or clears the legacy blob entirely.
		return state
	}
	for _, entry := range entries {
		state.Entries[entry.TxID] = persistedMempoolEntryFingerprint{
			AuthID:  entry.AuthID,
			Summary: entry.Summary,
			AddedAt: entry.AddedAt,
		}
	}
	for _, orphan := range orphans {
		state.Orphans[orphan.TxID] = persistedMempoolOrphanFingerprint{
			AuthID:  orphan.AuthID,
			AddedAt: orphan.AddedAt,
			Missing: append([]types.OutPoint(nil), orphan.Missing...),
		}
	}
	return state
}
