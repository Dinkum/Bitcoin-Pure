package storage

import (
	"bitcoin-pure/internal/consensus"
	"github.com/cockroachdb/pebble"
)

// HeaderChainUpdate is prepared before block persistence. A non-nil update
// with no State preserves the best header branch during headers-first sync.
type HeaderChainUpdate struct {
	State        *StoredHeaderState
	ForkHeight   uint64
	OldTipHeight uint64
	Entries      []HeaderBatchEntry
}

func (s *ChainStore) applyHeaderUpdateBatch(batch *pebble.Batch, update *HeaderChainUpdate) error {
	if update == nil || update.State == nil {
		return nil
	}
	if err := writeHeaderMeta(batch, update.State); err != nil {
		return err
	}
	pairs := make([]chainJournalHeightHash, 0, len(update.Entries))
	for _, entry := range update.Entries {
		pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: consensus.HeaderHash(&entry.Header)})
	}
	return s.appendJournalEntryBatch(batch, chainJournalEntry{Kind: journalRewriteHeaderHeights, ForkHeight: update.ForkHeight, OldTipHeight: update.OldTipHeight, Pairs: pairs})
}
