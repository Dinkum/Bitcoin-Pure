package mempool

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

// PersistedEntry is the restart-only subset needed to rebuild package state
// without paying full validation cost when the persisted tip still matches the
// committed chain tip.
type PersistedEntry struct {
	TxID    [32]byte
	AuthID  [32]byte
	Tx      types.Transaction
	Summary consensus.TxValidationSummary
	AddedAt uint64
}

type PersistedOrphan struct {
	TxID    [32]byte
	AuthID  [32]byte
	Tx      types.Transaction
	AddedAt uint64
	Missing []types.OutPoint
}

// PersistenceSnapshot is one lock-consistent view of restart-only mempool
// records. Epoch identifies the exact mutation boundary represented by it.
type PersistenceSnapshot struct {
	Epoch   uint64
	Entries []PersistedEntry
	Orphans []PersistedOrphan
}

// PersistenceChanges is a coalesced view of records touched after an earlier
// snapshot. A txid appears as its current record or as a delete, never both.
type PersistenceChanges struct {
	Epoch         uint64
	EntryCount    int
	OrphanCount   int
	EntryUpserts  []PersistedEntry
	EntryDeletes  [][32]byte
	OrphanUpserts []PersistedOrphan
	OrphanDeletes [][32]byte
}

type persistenceMutationKind uint8

const (
	persistenceEntryMutation persistenceMutationKind = iota + 1
	persistenceOrphanMutation
	persistenceJournalLimit = 65_536
)

type persistenceMutation struct {
	Seq  uint64
	Kind persistenceMutationKind
	TxID [32]byte
}

// PersistenceSnapshot captures accepted and orphan records under one read lock
// so a durable checkpoint can be tied to an exact mutation epoch.
func (p *Pool) PersistenceSnapshot() PersistenceSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snapshot := PersistenceSnapshot{
		Epoch:   p.persistenceSeq,
		Entries: make([]PersistedEntry, 0, len(p.entries)),
		Orphans: make([]PersistedOrphan, 0, len(p.orphans)),
	}
	for _, entry := range p.entries {
		snapshot.Entries = append(snapshot.Entries, persistedEntryForEntry(entry))
	}
	for _, orphan := range p.orphans {
		snapshot.Orphans = append(snapshot.Orphans, persistedOrphanForOrphan(orphan))
	}
	sort.Slice(snapshot.Entries, func(i, j int) bool {
		return bytes.Compare(snapshot.Entries[i].TxID[:], snapshot.Entries[j].TxID[:]) < 0
	})
	sort.Slice(snapshot.Orphans, func(i, j int) bool {
		return bytes.Compare(snapshot.Orphans[i].TxID[:], snapshot.Orphans[j].TxID[:]) < 0
	})
	return snapshot
}

// PersistenceChangesSince returns only records touched after epoch. The false
// result means the bounded journal no longer covers that epoch and the caller
// must use PersistenceSnapshot instead.
func (p *Pool) PersistenceChangesSince(epoch uint64) (PersistenceChanges, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	changes := PersistenceChanges{
		Epoch:       p.persistenceSeq,
		EntryCount:  len(p.entries),
		OrphanCount: len(p.orphans),
	}
	if epoch > p.persistenceSeq || epoch < p.persistenceFloor {
		return changes, false
	}
	dirtyEntries := make(map[[32]byte]struct{})
	dirtyOrphans := make(map[[32]byte]struct{})
	for _, mutation := range p.persistenceLog {
		if mutation.Seq <= epoch {
			continue
		}
		switch mutation.Kind {
		case persistenceEntryMutation:
			dirtyEntries[mutation.TxID] = struct{}{}
		case persistenceOrphanMutation:
			dirtyOrphans[mutation.TxID] = struct{}{}
		}
	}
	entryIDs := txidSetKeys(dirtyEntries)
	orphanIDs := txidSetKeys(dirtyOrphans)
	sortTxIDs(entryIDs)
	sortTxIDs(orphanIDs)
	for _, txid := range entryIDs {
		if entry := p.entries[txid]; entry != nil {
			changes.EntryUpserts = append(changes.EntryUpserts, persistedEntryForEntry(entry))
		} else {
			changes.EntryDeletes = append(changes.EntryDeletes, txid)
		}
	}
	for _, txid := range orphanIDs {
		if orphan := p.orphans[txid]; orphan != nil {
			changes.OrphanUpserts = append(changes.OrphanUpserts, persistedOrphanForOrphan(orphan))
		} else {
			changes.OrphanDeletes = append(changes.OrphanDeletes, txid)
		}
	}
	return changes, true
}

// AcknowledgePersistence releases journal entries at or below an epoch that a
// caller has durably committed. Mutations that raced after the snapshot remain
// available for the next flush.
func (p *Pool) AcknowledgePersistence(epoch uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch > p.persistenceSeq || epoch < p.persistenceFloor {
		return
	}
	firstPending := 0
	for firstPending < len(p.persistenceLog) && p.persistenceLog[firstPending].Seq <= epoch {
		firstPending++
	}
	copy(p.persistenceLog, p.persistenceLog[firstPending:])
	p.persistenceLog = p.persistenceLog[:len(p.persistenceLog)-firstPending]
	p.persistenceFloor = epoch
}

func persistedEntryForEntry(entry *Entry) PersistedEntry {
	return PersistedEntry{
		TxID:    entry.TxID,
		AuthID:  entry.AuthID,
		Tx:      entry.Tx,
		Summary: entry.Summary,
		AddedAt: entry.AddedAt,
	}
}

func persistedOrphanForOrphan(orphan *orphanEntry) PersistedOrphan {
	return PersistedOrphan{
		TxID:    orphan.TxID,
		AuthID:  consensus.AuthID(&orphan.Tx),
		Tx:      orphan.Tx,
		AddedAt: orphan.AddedAt,
		Missing: outPointSetKeys(orphan.Missing),
	}
}

func (p *Pool) notePersistenceMutationLocked(kind persistenceMutationKind, txid [32]byte) {
	p.persistenceSeq++
	if len(p.persistenceLog) >= persistenceJournalLimit {
		// Dropping the covered prefix is safe because readers behind this floor
		// receive false and rebuild one full, authoritative snapshot.
		p.persistenceLog = p.persistenceLog[:0]
		p.persistenceFloor = p.persistenceSeq - 1
	}
	p.persistenceLog = append(p.persistenceLog, persistenceMutation{Seq: p.persistenceSeq, Kind: kind, TxID: txid})
}

func (p *Pool) resetPersistenceJournalLocked() {
	p.persistenceSeq++
	p.persistenceFloor = p.persistenceSeq
	p.persistenceLog = p.persistenceLog[:0]
}

func (p *Pool) RestorePersistedState(entries []PersistedEntry, orphans []PersistedOrphan) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.entries) != 0 || len(p.orphans) != 0 {
		return errors.New("mempool restore requires an empty pool")
	}
	p.sequence = 0
	p.epoch = 0
	p.selection = nil
	p.snapshot = nil
	p.topByFee = nil
	p.topDirty = true
	p.stats = poolStats{feeCounts: make(map[uint64]int)}
	p.cachedStats = Stats{}
	p.statsDirty = true
	p.entries = make(map[[32]byte]*Entry, len(entries))
	p.spent = make(map[types.OutPoint][32]byte)
	p.orphans = make(map[[32]byte]*orphanEntry, len(orphans))
	p.orphanDeps = make(map[types.OutPoint][][32]byte)

	sortedEntries := append([]PersistedEntry(nil), entries...)
	sort.Slice(sortedEntries, func(i, j int) bool {
		if sortedEntries[i].AddedAt != sortedEntries[j].AddedAt {
			return sortedEntries[i].AddedAt < sortedEntries[j].AddedAt
		}
		left, right := sortedEntries[i].TxID, sortedEntries[j].TxID
		return bytes.Compare(left[:], right[:]) < 0
	})
	restoredEntries := make(map[[32]byte]*Entry, len(sortedEntries))
	claimedInputs := make(map[types.OutPoint][32]byte)
	for _, persisted := range sortedEntries {
		txid := consensus.TxID(&persisted.Tx)
		if txid != persisted.TxID {
			return fmt.Errorf("persisted mempool txid mismatch: stored=%x computed=%x", persisted.TxID, txid)
		}
		authid := consensus.AuthID(&persisted.Tx)
		if authid != persisted.AuthID {
			return fmt.Errorf("persisted mempool authid mismatch for %x", txid)
		}
		if persisted.Tx.IsCoinbase() {
			return fmt.Errorf("persisted mempool entry is coinbase: %x", txid)
		}
		if _, exists := restoredEntries[txid]; exists {
			return fmt.Errorf("duplicate persisted mempool entry: %x", txid)
		}
		spent := spentOutPointsForInputs(persisted.Tx.Base.Inputs)
		for _, outPoint := range spent {
			if conflict, exists := claimedInputs[outPoint]; exists {
				return fmt.Errorf("persisted mempool input conflict: %v spent by %x and %x", outPoint, conflict, txid)
			}
			claimedInputs[outPoint] = txid
		}
		size := persisted.Tx.EncodedLen()
		entry := &Entry{
			Tx:             persisted.Tx,
			TxID:           txid,
			AuthID:         authid,
			Summary:        persisted.Summary,
			SpentOutPoints: spent,
			CreatedLeaves:  createdLeavesForOutputs(txid, persisted.Tx.Base.Outputs),
			Fee:            persisted.Summary.Fee,
			Size:           size,
			AddedAt:        persisted.AddedAt,
			Parents:        make(map[[32]byte]struct{}),
			Children:       make(map[[32]byte]struct{}),
		}
		restoredEntries[txid] = entry
	}

	sortedOrphans := append([]PersistedOrphan(nil), orphans...)
	sort.Slice(sortedOrphans, func(i, j int) bool {
		if sortedOrphans[i].AddedAt != sortedOrphans[j].AddedAt {
			return sortedOrphans[i].AddedAt < sortedOrphans[j].AddedAt
		}
		left, right := sortedOrphans[i].TxID, sortedOrphans[j].TxID
		return bytes.Compare(left[:], right[:]) < 0
	})
	restoredOrphans := make(map[[32]byte]*orphanEntry, len(sortedOrphans))
	restoredOrphanDeps := make(map[types.OutPoint][][32]byte)
	for _, persisted := range sortedOrphans {
		txid := consensus.TxID(&persisted.Tx)
		if txid != persisted.TxID {
			return fmt.Errorf("persisted orphan txid mismatch: stored=%x computed=%x", persisted.TxID, txid)
		}
		if authid := consensus.AuthID(&persisted.Tx); authid != persisted.AuthID {
			return fmt.Errorf("persisted orphan authid mismatch for %x", txid)
		}
		if persisted.Tx.IsCoinbase() {
			return fmt.Errorf("persisted orphan is coinbase: %x", txid)
		}
		if _, exists := restoredEntries[txid]; exists {
			return fmt.Errorf("persisted orphan duplicates accepted tx: %x", txid)
		}
		if _, exists := restoredOrphans[txid]; exists {
			return fmt.Errorf("duplicate persisted orphan: %x", txid)
		}
		missing := make(map[types.OutPoint]struct{}, len(persisted.Missing))
		for _, outPoint := range persisted.Missing {
			missing[outPoint] = struct{}{}
		}
		restoredOrphans[txid] = &orphanEntry{
			Tx:           persisted.Tx,
			TxID:         txid,
			Size:         persisted.Tx.EncodedLen(),
			AddedAt:      persisted.AddedAt,
			Missing:      missing,
			MissingCount: len(missing),
		}
		for outPoint := range missing {
			restoredOrphanDeps[outPoint] = append(restoredOrphanDeps[outPoint], txid)
		}
	}
	p.entries = restoredEntries
	p.orphans = restoredOrphans
	p.orphanDeps = restoredOrphanDeps
	for _, entry := range p.entries {
		p.noteEntryAddedLocked(entry)
		if entry.AddedAt > p.sequence {
			p.sequence = entry.AddedAt
		}
	}
	for _, orphan := range p.orphans {
		p.orphanBytes += orphan.Size
		if orphan.AddedAt > p.sequence {
			p.sequence = orphan.AddedAt
		}
	}
	p.reindex()
	p.topByFee = nil
	p.topDirty = true
	p.snapshot = nil
	p.statsDirty = true
	p.resetPersistenceJournalLocked()
	p.trimOrphans()
	return nil
}
