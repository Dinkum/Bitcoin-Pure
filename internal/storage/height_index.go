package storage

import (
	"encoding/binary"
	"errors"
	"fmt"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
)

type chainJournalKind uint8

const (
	journalSetBlockHeight chainJournalKind = 1 + iota
	journalRewriteBlockHeights
	journalSetHeaderHeight
	journalRewriteHeaderHeights
)

type chainJournalHeightHash struct {
	Height uint64
	Hash   [32]byte
}

type chainJournalEntry struct {
	Kind         chainJournalKind
	ForkHeight   uint64
	OldTipHeight uint64
	Pairs        []chainJournalHeightHash
}

func (s *ChainStore) RewriteActiveHeights(forkHeight uint64, oldTipHeight uint64, entries []BlockIndexEntry) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	pairs := make([]chainJournalHeightHash, 0, len(entries))
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: hash})
	}
	if err := applyJournalEntryBatch(batch, chainJournalEntry{
		Kind:         journalRewriteBlockHeights,
		ForkHeight:   forkHeight,
		OldTipHeight: oldTipHeight,
		Pairs:        pairs,
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	return nil
}

func (s *ChainStore) RewriteActiveHeaderHeights(forkHeight uint64, oldTipHeight uint64, entries []BlockIndexEntry) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	pairs := make([]chainJournalHeightHash, 0, len(entries))
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: hash})
	}
	if err := applyJournalEntryBatch(batch, chainJournalEntry{
		Kind:         journalRewriteHeaderHeights,
		ForkHeight:   forkHeight,
		OldTipHeight: oldTipHeight,
		Pairs:        pairs,
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	return nil
}

func (s *ChainStore) SetHeaderHashByHeight(height uint64, hash [32]byte) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := applyJournalEntryBatch(batch, chainJournalEntry{
		Kind:  journalSetHeaderHeight,
		Pairs: []chainJournalHeightHash{{Height: height, Hash: hash}},
	}); err != nil {
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		return err
	}
	s.notifyBackgroundIndexes()
	return nil
}

func (s *ChainStore) GetBlockHashByHeight(height uint64) (*[32]byte, error) {
	buf, err := s.get(heightIndexKey(height))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return s.derivedHashByHeight(height, false)
	}
	if len(buf) != 32 {
		return nil, errors.New("invalid height index encoding")
	}
	var hash [32]byte
	copy(hash[:], buf)
	return &hash, nil
}

func (s *ChainStore) GetHeaderHashByHeight(height uint64) (*[32]byte, error) {
	buf, err := s.get(headerHeightIndexKey(height))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return s.derivedHashByHeight(height, true)
	}
	if len(buf) != 32 {
		return nil, errors.New("invalid header height index encoding")
	}
	var hash [32]byte
	copy(hash[:], buf)
	return &hash, nil
}

func (s *ChainStore) GetIndexedHeaderHashByHeight(height uint64) (*[32]byte, error) {
	buf, err := s.get(headerHeightIndexKey(height))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	if len(buf) != 32 {
		return nil, errors.New("invalid header height index encoding")
	}
	var hash [32]byte
	copy(hash[:], buf)
	return &hash, nil
}

func (s *ChainStore) GetCanonicalHeaderHashByHeight(height uint64) (*[32]byte, error) {
	return s.derivedHashByHeight(height, true)
}

func (s *ChainStore) derivedHashByHeight(height uint64, header bool) (*[32]byte, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	tipHeightKey := metaTipHeightKey
	tipHeaderKey := metaTipHeaderKey
	if header {
		tipHeightKey = metaHeaderTipHeightKey
		tipHeaderKey = metaHeaderTipHeaderKey
	}
	heightBytes, err := readValue(snap, tipHeightKey)
	if err != nil {
		return nil, err
	}
	headerBytes, err := readValue(snap, tipHeaderKey)
	if err != nil {
		return nil, err
	}
	if (heightBytes == nil || headerBytes == nil) && header {
		heightBytes, err = readValue(snap, metaTipHeightKey)
		if err != nil {
			return nil, err
		}
		headerBytes, err = readValue(snap, metaTipHeaderKey)
		if err != nil {
			return nil, err
		}
	}
	if heightBytes == nil || headerBytes == nil {
		return nil, nil
	}
	tipHeight, err := decodeU64(heightBytes)
	if err != nil {
		return nil, err
	}
	if height > tipHeight {
		return nil, nil
	}
	tipHeader, err := types.DecodeBlockHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	hash := consensus.HeaderHash(&tipHeader)
	if height == tipHeight {
		return &hash, nil
	}
	cursorHash := hash
	cursorHeight := tipHeight
	for cursorHeight > height {
		entry, err := getBlockIndexFrom(snap, &cursorHash)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, fmt.Errorf("missing block index for derived height lookup %x", cursorHash)
		}
		cursorHash = entry.ParentHash
		cursorHeight--
	}
	return &cursorHash, nil
}

func writeHeightIndexesBatch(batch *pebble.Batch, entries ...chainJournalEntry) error {
	for _, entry := range entries {
		if err := applyJournalEntryBatch(batch, entry); err != nil {
			return err
		}
	}
	return nil
}

// recoverLegacyHeightJournal runs before the store opens to readers or
// accepts commits. Finish any old deferred writes in sequence order, then
// retire the log. Normal writers update the height indexes in the same
// durable batch as the canonical tip.
func (s *ChainStore) recoverLegacyHeightJournal() error {
	nextBytes, err := s.get(metaJournalNextSeqKey)
	if err != nil || nextBytes == nil {
		return err
	}
	nextSeq, err := decodeU64(nextBytes)
	if err != nil {
		return err
	}
	derivedBytes, err := s.get(metaDerivedJournalSeqKey)
	if err != nil {
		return err
	}
	var derivedSeq uint64
	if derivedBytes != nil {
		derivedSeq, err = decodeU64(derivedBytes)
		if err != nil {
			return err
		}
	}
	if derivedSeq > nextSeq {
		return errors.New("height journal checkpoint exceeds end of log")
	}
	const replayEntriesPerBatch = 512
	for derivedSeq < nextSeq {
		end := derivedSeq + min(nextSeq-derivedSeq, replayEntriesPerBatch)
		if err := s.replayLegacyHeightJournalRange(derivedSeq, end); err != nil {
			return err
		}
		derivedSeq = end
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(journalPrefix, prefixUpperBound(journalPrefix), nil); err != nil {
		return err
	}
	if err := batch.Delete(metaJournalNextSeqKey, nil); err != nil {
		return err
	}
	if err := batch.Delete(metaDerivedJournalSeqKey, nil); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func (s *ChainStore) replayLegacyHeightJournalRange(first, end uint64) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	for seq := first; seq < end; seq++ {
		raw, err := s.get(journalKey(seq))
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		entry, err := decodeChainJournalEntry(raw)
		if err != nil {
			return err
		}
		if err := applyJournalEntryBatch(batch, entry); err != nil {
			return err
		}
	}
	if err := batch.Set(metaDerivedJournalSeqKey, encodeU64(end), nil); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func journalPairsFromEntries(entries []BlockIndexEntry) []chainJournalHeightHash {
	pairs := make([]chainJournalHeightHash, 0, len(entries))
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		pairs = append(pairs, chainJournalHeightHash{Height: entry.Height, Hash: hash})
	}
	return pairs
}

func journalKey(seq uint64) []byte {
	return append(append([]byte(nil), journalPrefix...), encodeU64(seq)...)
}

func heightIndexKey(height uint64) []byte {
	return append(append([]byte(nil), heightIndexPrefix...), encodeU64(height)...)
}

func headerHeightIndexKey(height uint64) []byte {
	return append(append([]byte(nil), headerHeightIndexPrefix...), encodeU64(height)...)
}

func encodeChainJournalEntry(entry chainJournalEntry) []byte {
	buf := make([]byte, 1+8+8+4)
	buf[0] = byte(entry.Kind)
	binary.LittleEndian.PutUint64(buf[1:9], entry.ForkHeight)
	binary.LittleEndian.PutUint64(buf[9:17], entry.OldTipHeight)
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(entry.Pairs)))
	for _, pair := range entry.Pairs {
		rawHeight := make([]byte, 8)
		binary.LittleEndian.PutUint64(rawHeight, pair.Height)
		buf = append(buf, rawHeight...)
		buf = append(buf, pair.Hash[:]...)
	}
	return buf
}

func decodeChainJournalEntry(buf []byte) (chainJournalEntry, error) {
	if len(buf) < 21 {
		return chainJournalEntry{}, errors.New("invalid chain journal entry encoding")
	}
	entry := chainJournalEntry{
		Kind:         chainJournalKind(buf[0]),
		ForkHeight:   binary.LittleEndian.Uint64(buf[1:9]),
		OldTipHeight: binary.LittleEndian.Uint64(buf[9:17]),
	}
	count := binary.LittleEndian.Uint32(buf[17:21])
	buf = buf[21:]
	if len(buf) != int(count)*(8+32) {
		return chainJournalEntry{}, errors.New("invalid chain journal height/hash payload")
	}
	entry.Pairs = make([]chainJournalHeightHash, 0, count)
	for i := uint32(0); i < count; i++ {
		pair := chainJournalHeightHash{Height: binary.LittleEndian.Uint64(buf[:8])}
		copy(pair.Hash[:], buf[8:40])
		entry.Pairs = append(entry.Pairs, pair)
		buf = buf[40:]
	}
	return entry, nil
}

func applyJournalEntryBatch(batch *pebble.Batch, entry chainJournalEntry) error {
	switch entry.Kind {
	case journalSetBlockHeight:
		for _, pair := range entry.Pairs {
			if err := batch.Set(heightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	case journalRewriteBlockHeights:
		for height := entry.ForkHeight; height < entry.OldTipHeight; {
			height++
			if err := batch.Delete(heightIndexKey(height), nil); err != nil {
				return err
			}
		}
		for _, pair := range entry.Pairs {
			if err := batch.Set(heightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	case journalSetHeaderHeight:
		for _, pair := range entry.Pairs {
			if err := batch.Set(headerHeightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	case journalRewriteHeaderHeights:
		for height := entry.ForkHeight; height < entry.OldTipHeight; {
			height++
			if err := batch.Delete(headerHeightIndexKey(height), nil); err != nil {
				return err
			}
		}
		for _, pair := range entry.Pairs {
			if err := batch.Set(headerHeightIndexKey(pair.Height), pair.Hash[:], nil); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown chain journal kind %d", entry.Kind)
	}
	return nil
}
