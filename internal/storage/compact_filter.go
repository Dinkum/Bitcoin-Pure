package storage

import (
	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/types"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"time"
)

const MaxCompactFilterHeaders = 2000

const filterIndexBatchBlocks = 128
const filterIndexBatchBytes = 8 << 20

var filterRecordPrefix = []byte("filter_v1/")
var filterHeightPrefix = []byte("filter_height_v1/")
var filterTipKey = []byte("meta/filter_tip_v1")
var ErrFilterIndexCatchingUp = errors.New("compact filter index is catching up; retry the request")

type CompactFilterHeaderRecord struct {
	Height     uint64
	BlockHash  [32]byte
	FilterHash [32]byte
	Header     [32]byte
}

func filterRecordKey(hash [32]byte) []byte {
	return append(append([]byte(nil), filterRecordPrefix...), hash[:]...)
}
func filterHeightKey(height uint64) []byte {
	return append(append([]byte(nil), filterHeightPrefix...), encodeU64(height)...)
}

func readFilterRecord(reader valueReader, hash [32]byte) (*CompactFilterHeaderRecord, error) {
	raw, err := readValue(reader, filterRecordKey(hash))
	if err != nil || raw == nil {
		return nil, err
	}
	if len(raw) != 64 {
		return nil, errors.New("invalid compact filter index record")
	}
	row := &CompactFilterHeaderRecord{BlockHash: hash}
	copy(row.FilterHash[:], raw[:32])
	copy(row.Header[:], raw[32:])
	return row, nil
}
func readFilterTip(reader valueReader) (*txIndexTip, error) {
	raw, err := readValue(reader, filterTipKey)
	if err != nil || raw == nil {
		return nil, err
	}
	if len(raw) != 40 {
		return nil, errors.New("invalid compact filter index checkpoint")
	}
	height, err := decodeU64(raw[:8])
	if err != nil {
		return nil, err
	}
	tip := &txIndexTip{Height: height}
	copy(tip.Hash[:], raw[8:])
	return tip, nil
}
func writeFilterTip(batch *pebble.Batch, tip *txIndexTip) error {
	if tip == nil {
		return batch.Delete(filterTipKey, nil)
	}
	return batch.Set(filterTipKey, append(encodeU64(tip.Height), tip.Hash[:]...), nil)
}
func canonicalFilterHash(reader valueReader, height uint64) ([32]byte, error) {
	var hash [32]byte
	raw, err := readValue(reader, heightIndexKey(height))
	if err != nil {
		return hash, err
	}
	if len(raw) != 32 {
		return hash, ErrFilterIndexCatchingUp
	}
	copy(hash[:], raw)
	return hash, nil
}
func filterChainTip(reader valueReader) (*txIndexTip, error) {
	tip, err := readTxIndexTip(reader)
	if err != nil || tip == nil {
		return tip, err
	}
	hash, err := canonicalFilterHash(reader, tip.Height)
	if err != nil {
		return nil, err
	}
	if hash != tip.Hash {
		return nil, ErrFilterIndexCatchingUp
	}
	return tip, nil
}

// Immutable block-hash records survive reorgs. Only height selection and the
// coverage checkpoint follow the canonical chain, in bounded atomic batches.
func (s *ChainStore) advanceCompactFilterIndex() (bool, error) {
	s.filterMu.Lock()
	defer s.filterMu.Unlock()
	snap := s.db.NewSnapshot()
	defer snap.Close()
	tip, err := filterChainTip(snap)
	if err != nil || tip == nil {
		return false, err
	}
	built, err := readFilterTip(snap)
	if err != nil {
		return false, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	steps := 0
	for built != nil {
		if built.Height <= tip.Height {
			hash, err := canonicalFilterHash(snap, built.Height)
			if err != nil {
				return false, err
			}
			if hash == built.Hash {
				break
			}
		}
		if built.Height == 0 {
			built = nil
		} else {
			entry, err := getBlockIndexFrom(snap, &built.Hash)
			if err != nil {
				return false, err
			}
			if entry == nil {
				return false, errors.New("missing compact filter checkpoint ancestor")
			}
			built = &txIndexTip{Height: built.Height - 1, Hash: entry.ParentHash}
		}
		steps++
		if steps == filterIndexBatchBlocks {
			if err := writeFilterTip(batch, built); err != nil {
				return false, err
			}
			return true, batch.Commit(pebble.NoSync)
		}
	}
	if built != nil && *built == *tip {
		return false, nil
	}
	var previous [32]byte
	height := uint64(0)
	if built != nil {
		row, err := readFilterRecord(snap, built.Hash)
		if err != nil {
			return false, err
		}
		if row == nil {
			return false, errors.New("missing compact filter checkpoint record")
		}
		previous = row.Header
		height = built.Height + 1
	}
	bytes := 0
	for ; steps < filterIndexBatchBlocks && height <= tip.Height; steps++ {
		hash, err := canonicalFilterHash(snap, height)
		if err != nil {
			return false, err
		}
		row, err := readFilterRecord(snap, hash)
		if err != nil {
			return false, err
		}
		if row == nil || row.Header != compactfilter.Header(row.FilterHash, previous) {
			block, err := getBlockFrom(snap, &hash)
			if err != nil {
				return false, err
			}
			if block == nil {
				return false, fmt.Errorf("compact filter history unavailable at height %d", height)
			}
			raw, err := readValue(snap, blockUndoKey(hash))
			if err != nil {
				return false, err
			}
			if height > 0 && raw == nil {
				return false, fmt.Errorf("compact filter undo unavailable at height %d", height)
			}
			var undo []BlockUndoEntry
			if raw != nil {
				undo, err = decodeBlockUndo(raw)
				if err != nil {
					return false, err
				}
			}
			items := make([]compactfilter.WatchItem, 0, len(undo))
			for _, spent := range undo {
				item := compactfilter.WatchItem{Type: spent.Entry.Type, Payload32: spent.Entry.Payload32}
				if item.Type == types.OutputXOnlyP2PK && item.Payload32 == ([32]byte{}) {
					item.Payload32 = spent.Entry.PubKey
				}
				items = append(items, item)
			}
			filter := compactfilter.Build(hash, block, items)
			row = &CompactFilterHeaderRecord{BlockHash: hash, FilterHash: filter.Hash, Header: compactfilter.Header(filter.Hash, previous)}
			encoded := append(append(make([]byte, 0, 64), row.FilterHash[:]...), row.Header[:]...)
			if err := batch.Set(filterRecordKey(hash), encoded, nil); err != nil {
				return false, err
			}
			bytes += block.EncodedLen() + len(raw)
		}
		if err := batch.Set(filterHeightKey(height), hash[:], nil); err != nil {
			return false, err
		}
		built = &txIndexTip{Height: height, Hash: hash}
		previous = row.Header
		if height == tip.Height || bytes >= filterIndexBatchBytes {
			break
		}
		height++
	}
	if err := writeFilterTip(batch, built); err != nil {
		return false, err
	}
	return true, batch.Commit(pebble.NoSync)
}

func (s *ChainStore) compactFilterIndexLoop() {
	var lastError string
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		progress, err := s.advanceCompactFilterIndex()
		if err != nil && !errors.Is(err, ErrFilterIndexCatchingUp) && err.Error() != lastError {
			s.logger.Warn("compact filter index unavailable", "error", err)
			lastError = err.Error()
		}
		if err == nil {
			lastError = ""
		}
		if progress && err == nil {
			continue
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-s.stopCh:
			timer.Stop()
			return
		case <-s.filterNotify:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func filterSnapshotReady(snap *pebble.Snapshot, stop uint64) (bool, error) {
	tip, err := filterChainTip(snap)
	if err != nil {
		return false, err
	}
	if tip == nil || stop > tip.Height {
		return false, fmt.Errorf("block height %d not found", stop)
	}
	built, err := readFilterTip(snap)
	if err != nil {
		return false, err
	}
	if built == nil || built.Height < stop || built.Height > tip.Height {
		return false, nil
	}
	hash, err := canonicalFilterHash(snap, built.Height)
	if err != nil {
		return false, err
	}
	return hash == built.Hash, nil
}
func (s *ChainStore) compactFilterSnapshot(stop uint64) (*pebble.Snapshot, error) {
	snap := s.db.NewSnapshot()
	ready, err := filterSnapshotReady(snap, stop)
	if err != nil {
		snap.Close()
		return nil, err
	}
	if ready {
		return snap, nil
	}
	snap.Close()
	// A request can help one bounded batch; it cannot force a genesis-to-tip scan.
	if _, err := s.advanceCompactFilterIndex(); err != nil {
		return nil, err
	}
	snap = s.db.NewSnapshot()
	ready, err = filterSnapshotReady(snap, stop)
	if err != nil || !ready {
		snap.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrFilterIndexCatchingUp
	}
	return snap, nil
}
func filterRowAt(snap *pebble.Snapshot, height uint64) (CompactFilterHeaderRecord, error) {
	var zero CompactFilterHeaderRecord
	raw, err := readValue(snap, filterHeightKey(height))
	if err != nil {
		return zero, err
	}
	if len(raw) != 32 {
		return zero, ErrFilterIndexCatchingUp
	}
	var hash [32]byte
	copy(hash[:], raw)
	canonical, err := canonicalFilterHash(snap, height)
	if err != nil {
		return zero, err
	}
	if hash != canonical {
		return zero, ErrFilterIndexCatchingUp
	}
	row, err := readFilterRecord(snap, hash)
	if err != nil {
		return zero, err
	}
	if row == nil {
		return zero, ErrFilterIndexCatchingUp
	}
	row.Height = height
	return *row, nil
}
func (s *ChainStore) CompactFilterHeaders(start, count uint64) ([32]byte, []CompactFilterHeaderRecord, error) {
	var previous [32]byte
	if count == 0 {
		return previous, nil, nil
	}
	if count > MaxCompactFilterHeaders || start > ^uint64(0)-(count-1) {
		return previous, nil, errors.New("invalid compact filter header range")
	}
	stop := start + count - 1
	snap, err := s.compactFilterSnapshot(stop)
	if err != nil {
		return previous, nil, err
	}
	defer snap.Close()
	if start > 0 {
		row, err := filterRowAt(snap, start-1)
		if err != nil {
			return previous, nil, err
		}
		previous = row.Header
	}
	rows := make([]CompactFilterHeaderRecord, 0, count)
	for height := start; ; height++ {
		row, err := filterRowAt(snap, height)
		if err != nil {
			return previous, nil, err
		}
		rows = append(rows, row)
		if height == stop {
			break
		}
	}
	return previous, rows, nil
}
func (s *ChainStore) CompactFilterCheckpoints(interval uint64, maxEntries int) (uint64, []CompactFilterHeaderRecord, error) {
	if interval == 0 || maxEntries <= 0 {
		return 0, nil, errors.New("invalid compact filter checkpoint request")
	}
	snap := s.db.NewSnapshot()
	tip, err := filterChainTip(snap)
	snap.Close()
	if err != nil || tip == nil {
		return 0, nil, err
	}
	if tip.Height/interval >= uint64(maxEntries) || (tip.Height%interval != 0 && tip.Height/interval+1 >= uint64(maxEntries)) {
		return 0, nil, fmt.Errorf("checkpoint response exceeds max %d; increase interval", maxEntries)
	}
	snap, err = s.compactFilterSnapshot(tip.Height)
	if err != nil {
		return 0, nil, err
	}
	defer snap.Close()
	// The selected tip and every returned row belong to this same snapshot.
	tip, err = filterChainTip(snap)
	if err != nil || tip == nil {
		return 0, nil, err
	}
	if tip.Height/interval >= uint64(maxEntries) || (tip.Height%interval != 0 && tip.Height/interval+1 >= uint64(maxEntries)) {
		return 0, nil, fmt.Errorf("checkpoint response exceeds max %d; increase interval", maxEntries)
	}
	ready, err := filterSnapshotReady(snap, tip.Height)
	if err != nil {
		return 0, nil, err
	}
	if !ready {
		return 0, nil, ErrFilterIndexCatchingUp
	}
	rows := make([]CompactFilterHeaderRecord, 0, tip.Height/interval+2)
	for height := uint64(0); ; {
		row, err := filterRowAt(snap, height)
		if err != nil {
			return 0, nil, err
		}
		rows = append(rows, row)
		if tip.Height-height < interval {
			break
		}
		height += interval
	}
	if rows[len(rows)-1].Height != tip.Height {
		row, err := filterRowAt(snap, tip.Height)
		if err != nil {
			return 0, nil, err
		}
		rows = append(rows, row)
	}
	return tip.Height, rows, nil
}
