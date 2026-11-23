package storage

import (
	"bytes"
	"errors"
	"slices"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
)

type LocalityIndexedUTXO struct {
	Sequence uint64
	OutPoint types.OutPoint
	Entry    consensus.UtxoEntry
}

func (s *ChainStore) LoadLocalityOrderedUTXOs(limit int) ([]LocalityIndexedUTXO, error) {
	items := make([]LocalityIndexedUTXO, 0)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: localitySeqPrefix,
		UpperBound: localitySeqPrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		seq, err := decodeLocalitySeqFromKey(iter.Key())
		if err != nil {
			return nil, err
		}
		outPoint, entry, err := decodeLocalitySeqValue(iter.Value())
		if err != nil {
			return nil, err
		}
		items = append(items, LocalityIndexedUTXO{Sequence: seq, OutPoint: outPoint, Entry: entry})
		if limit > 0 && len(items) == limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *ChainStore) LocalitySequence(outPoint types.OutPoint) (uint64, bool, error) {
	return s.localitySeqForOutPoint(outPoint)
}

func rebuildLocalityIndexBatch(batch *pebble.Batch, utxos consensus.UtxoSet) error {
	if err := batch.DeleteRange(localitySeqPrefix, localitySeqPrefixEnd, nil); err != nil {
		return err
	}
	if err := batch.DeleteRange(localityMetaPrefix, localityMetaPrefixEnd, nil); err != nil {
		return err
	}
	ordered := make([]types.OutPoint, 0, len(utxos))
	for outPoint := range utxos {
		ordered = append(ordered, outPoint)
	}
	sortOutPointsCanonical(ordered)
	for seq, outPoint := range ordered {
		if err := batch.Set(localitySeqKey(uint64(seq)), encodeLocalitySeqValue(outPoint, utxos[outPoint]), nil); err != nil {
			return err
		}
		if err := batch.Set(localityMetaKey(outPoint), encodeU64(uint64(seq)), nil); err != nil {
			return err
		}
	}
	return batch.Set(metaLocalityNextSeqKey, encodeU64(uint64(len(ordered))), nil)
}

func (s *ChainStore) applyLocalityDeltaBatch(batch *pebble.Batch, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	nextSeq, err := s.localityNextSeq()
	if err != nil {
		return err
	}
	for _, outPoint := range spent {
		seq, ok, err := s.localitySeqForOutPoint(outPoint)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := batch.Delete(localitySeqKey(seq), nil); err != nil {
			return err
		}
		if err := batch.Delete(localityMetaKey(outPoint), nil); err != nil {
			return err
		}
	}
	orderedCreated := make([]types.OutPoint, 0, len(created))
	for outPoint := range created {
		orderedCreated = append(orderedCreated, outPoint)
	}
	sortOutPointsCanonical(orderedCreated)
	for _, outPoint := range orderedCreated {
		if oldSeq, ok, err := s.localitySeqForOutPoint(outPoint); err != nil {
			return err
		} else if ok {
			if err := batch.Delete(localitySeqKey(oldSeq), nil); err != nil {
				return err
			}
			if err := batch.Delete(localityMetaKey(outPoint), nil); err != nil {
				return err
			}
		}
		if err := batch.Set(localitySeqKey(nextSeq), encodeLocalitySeqValue(outPoint, created[outPoint]), nil); err != nil {
			return err
		}
		if err := batch.Set(localityMetaKey(outPoint), encodeU64(nextSeq), nil); err != nil {
			return err
		}
		nextSeq++
	}
	return batch.Set(metaLocalityNextSeqKey, encodeU64(nextSeq), nil)
}

func (s *ChainStore) applyLocalityRewriteBatch(batch *pebble.Batch, previous consensus.UtxoSet, next consensus.UtxoSet) error {
	spent := make([]types.OutPoint, 0)
	created := make(map[types.OutPoint]consensus.UtxoEntry)
	for outPoint := range previous {
		if _, ok := next[outPoint]; !ok {
			spent = append(spent, outPoint)
		}
	}
	for outPoint, entry := range next {
		if _, ok := previous[outPoint]; !ok {
			created[outPoint] = entry
		}
	}
	return s.applyLocalityDeltaBatch(batch, spent, created)
}

func (s *ChainStore) localityNextSeq() (uint64, error) {
	buf, err := s.get(metaLocalityNextSeqKey)
	if err != nil {
		return 0, err
	}
	if buf == nil {
		return 0, nil
	}
	return decodeU64(buf)
}

func (s *ChainStore) localitySeqForOutPoint(outPoint types.OutPoint) (uint64, bool, error) {
	buf, err := s.get(localityMetaKey(outPoint))
	if err != nil {
		return 0, false, err
	}
	if buf == nil {
		return 0, false, nil
	}
	seq, err := decodeU64(buf)
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

func sortOutPointsCanonical(outPoints []types.OutPoint) {
	slices.SortFunc(outPoints, func(a, b types.OutPoint) int {
		return compareOutPoints(a, b)
	})
}

func compareOutPoints(a, b types.OutPoint) int {
	switch cmp := bytes.Compare(a.TxID[:], b.TxID[:]); {
	case cmp < 0:
		return -1
	case cmp > 0:
		return 1
	case a.Vout < b.Vout:
		return -1
	case a.Vout > b.Vout:
		return 1
	default:
		return 0
	}
}

func localitySeqKey(seq uint64) []byte {
	return append(append([]byte(nil), localitySeqPrefix...), encodeU64BE(seq)...)
}

func localityMetaKey(outPoint types.OutPoint) []byte {
	buf := append([]byte(nil), localityMetaPrefix...)
	outPoint.Encode(&buf)
	return buf
}

func encodeLocalitySeqValue(outPoint types.OutPoint, entry consensus.UtxoEntry) []byte {
	buf := make([]byte, 0, 36+49)
	outPoint.Encode(&buf)
	return append(buf, encodeUTXOEntry(entry)...)
}

func decodeLocalitySeqValue(buf []byte) (types.OutPoint, consensus.UtxoEntry, error) {
	if len(buf) <= 36 {
		return types.OutPoint{}, consensus.UtxoEntry{}, errors.New("invalid locality sequence value")
	}
	outPoint, err := decodeOutPoint(buf[:36])
	if err != nil {
		return types.OutPoint{}, consensus.UtxoEntry{}, err
	}
	entry, err := decodeUTXOEntry(buf[36:])
	return outPoint, entry, err
}

func decodeLocalitySeqFromKey(key []byte) (uint64, error) {
	if len(key) != len(localitySeqPrefix)+8 {
		return 0, errors.New("invalid locality sequence key")
	}
	return decodeU64BE(key[len(localitySeqPrefix):])
}

// migrateLocalityIndex keeps sequence assignments and changes the sort order
// of their stored keys. Legacy rows stay available until the new namespace has
// been fully written, so an interrupted open can restart from the same source.
func (s *ChainStore) migrateLocalityIndex() error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: legacyLocalitySeqPrefix,
		UpperBound: legacyLocalitySeqPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	if !iter.First() {
		return iter.Error()
	}
	if err := s.db.DeleteRange(localitySeqPrefix, localitySeqPrefixEnd, bestEffortWriteOptions); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	const localityMigrationBatchRows = 10_000
	for ; iter.Valid(); iter.Next() {
		seq, err := decodeU64(iter.Key()[len(legacyLocalitySeqPrefix):])
		if err != nil {
			return err
		}
		value := iter.Value()
		if len(value) == 36 {
			outPoint, err := decodeOutPoint(value)
			if err != nil {
				return err
			}
			entry, ok, err := s.GetUTXO(outPoint)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			value = encodeLocalitySeqValue(outPoint, entry)
		} else if _, _, err := decodeLocalitySeqValue(value); err != nil {
			return err
		}
		if err := batch.Set(localitySeqKey(seq), value, nil); err != nil {
			return err
		}
		if batch.Count() >= localityMigrationBatchRows {
			if err := batch.Commit(bestEffortWriteOptions); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if err := batch.DeleteRange(legacyLocalitySeqPrefix, legacyLocalitySeqPrefixEnd, nil); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}
