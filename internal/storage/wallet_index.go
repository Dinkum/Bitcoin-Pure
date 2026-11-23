package storage

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
)

type WalletWatchItem struct {
	Type      uint64
	Payload32 [32]byte
}

type WalletIndexedUTXO struct {
	OutPoint types.OutPoint
	Entry    consensus.UtxoEntry
	Height   uint64
	Coinbase bool
}

type WalletActivityRecord struct {
	TxID      [32]byte
	BlockHash [32]byte
	Height    uint64
	Timestamp uint64
	Coinbase  bool
	Received  uint64
	Sent      uint64
	Fee       uint64
}

func (s *ChainStore) removeLegacyWalletOrigins() error {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: walletOriginPrefix, UpperBound: walletOriginPrefixEnd})
	if err != nil {
		return err
	}
	found := iter.First()
	err = iter.Close()
	if err != nil || !found {
		return err
	}
	return s.db.DeleteRange(walletOriginPrefix, walletOriginPrefixEnd, consensusCriticalWriteOptions)
}

func (s *ChainStore) WalletUTXOsByWatchItems(items []WalletWatchItem) ([]WalletIndexedUTXO, error) {
	if len(items) == 0 {
		return nil, nil
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	if err := requireWalletIndexReady(snap); err != nil {
		return nil, err
	}
	out := make([]WalletIndexedUTXO, 0)
	for _, item := range uniqueWalletWatchItems(items) {
		prefix := walletUTXOItemPrefix(item)
		iter, err := snap.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			outPoint, err := decodeOutPoint(iter.Key()[len(prefix):])
			if err != nil {
				iter.Close()
				return nil, err
			}
			record, err := decodeWalletIndexedUTXO(iter.Value())
			if err != nil {
				iter.Close()
				return nil, err
			}
			record.OutPoint = outPoint
			out = append(out, record)
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return nil, err
		}
		iter.Close()
	}
	slices.SortFunc(out, compareWalletIndexedUTXOs)
	return out, nil
}

func (s *ChainStore) WalletActivityByWatchItems(items []WalletWatchItem, limit int) ([]WalletActivityRecord, error) {
	if len(items) == 0 {
		return nil, nil
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	if err := requireWalletIndexReady(snap); err != nil {
		return nil, err
	}
	uniqueItems := uniqueWalletWatchItems(items)
	cursors := make([]*walletActivityCursor, 0, len(uniqueItems))
	defer func() {
		for _, cursor := range cursors {
			cursor.iter.Close()
		}
	}()
	for _, item := range uniqueItems {
		prefix := walletActivityItemWatchPrefix(item)
		iter, err := snap.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		cursor := &walletActivityCursor{iter: iter}
		if err := cursor.first(); err != nil {
			iter.Close()
			return nil, err
		}
		if cursor.valid {
			cursors = append(cursors, cursor)
		} else {
			iter.Close()
		}
	}

	activityHeap := walletActivityHeap(cursors)
	heap.Init(&activityHeap)
	out := make([]WalletActivityRecord, 0)
	for activityHeap.Len() > 0 && (limit <= 0 || len(out) < limit) {
		cursor := heap.Pop(&activityHeap).(*walletActivityCursor)
		record := cursor.record
		if err := cursor.next(); err != nil {
			return nil, err
		}
		if cursor.valid {
			heap.Push(&activityHeap, cursor)
		}
		for activityHeap.Len() > 0 {
			next := activityHeap[0]
			if next.record.Height != record.Height || next.record.TxID != record.TxID {
				break
			}
			next = heap.Pop(&activityHeap).(*walletActivityCursor)
			record.Received += next.record.Received
			record.Sent += next.record.Sent
			if next.record.Fee > record.Fee {
				record.Fee = next.record.Fee
			}
			if err := next.next(); err != nil {
				return nil, err
			}
			if next.valid {
				heap.Push(&activityHeap, next)
			}
		}
		out = append(out, record)
	}
	return out, nil
}

func (s *ChainStore) WalletIndexHeight() (*uint64, error) {
	return walletIndexHeight(s.db)
}

func walletIndexHeight(reader valueReader) (*uint64, error) {
	buf, err := readValue(reader, metaWalletIndexHeightKey)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	height, err := decodeU64(buf)
	if err != nil {
		return nil, err
	}
	return &height, nil
}

func requireWalletIndexReady(reader valueReader) error {
	height, err := walletIndexHeight(reader)
	if err != nil {
		return err
	}
	if height == nil {
		return errors.New("wallet index is not ready")
	}
	chainMeta, _, err := loadChainStateMeta(reader)
	if err != nil {
		return err
	}
	if chainMeta == nil {
		return errors.New("chain state is not ready")
	}
	if *height != chainMeta.Height {
		return fmt.Errorf("wallet index height %d does not match chain height %d", *height, chainMeta.Height)
	}
	return nil
}

func uniqueWalletWatchItems(items []WalletWatchItem) []WalletWatchItem {
	if len(items) < 2 {
		return items
	}
	seen := make(map[WalletWatchItem]struct{}, len(items))
	out := make([]WalletWatchItem, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

type walletActivityCursor struct {
	iter   *pebble.Iterator
	record WalletActivityRecord
	valid  bool
}

func (c *walletActivityCursor) first() error {
	c.valid = c.iter.First()
	if !c.valid {
		return c.iter.Error()
	}
	return c.decode()
}

func (c *walletActivityCursor) next() error {
	c.valid = c.iter.Next()
	if !c.valid {
		return c.iter.Error()
	}
	return c.decode()
}

func (c *walletActivityCursor) decode() error {
	record, err := decodeWalletActivityRecord(c.iter.Value())
	if err != nil {
		return err
	}
	c.record = record
	return nil
}

type walletActivityHeap []*walletActivityCursor

func (h walletActivityHeap) Len() int { return len(h) }

func (h walletActivityHeap) Less(i, j int) bool {
	return compareWalletActivityRecords(h[i].record, h[j].record) < 0
}

func (h walletActivityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *walletActivityHeap) Push(x any) {
	*h = append(*h, x.(*walletActivityCursor))
}

func (h *walletActivityHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (s *ChainStore) RebuildWalletIndexes(tipHeight uint64) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.rebuildWalletIndexesLocked(tipHeight)
}

func (s *ChainStore) RebuildWalletIndexesAtCurrentTip() (uint64, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	meta, err := s.LoadChainStateMeta()
	if err != nil {
		return 0, err
	}
	if meta == nil {
		return 0, errors.New("chain state is not ready")
	}
	return meta.Height, s.rebuildWalletIndexesLocked(meta.Height)
}

func (s *ChainStore) rebuildWalletIndexesLocked(tipHeight uint64) error {
	batch := s.db.NewBatch()
	if err := s.deleteWalletIndexesBatch(batch); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	batch = s.db.NewBatch()
	for height := uint64(0); height <= tipHeight; height++ {
		hash, err := s.GetBlockHashByHeight(height)
		if err != nil {
			batch.Close()
			return err
		}
		if hash == nil {
			batch.Close()
			return fmt.Errorf("missing active block hash at height %d during wallet index rebuild", height)
		}
		block, err := s.GetBlock(hash)
		if err != nil {
			batch.Close()
			return err
		}
		if block == nil {
			batch.Close()
			return fmt.Errorf("missing active block %x at height %d during wallet index rebuild", *hash, height)
		}
		undo, err := s.GetUndo(hash)
		if err != nil {
			batch.Close()
			return err
		}
		if err := putWalletActivityForBlockBatch(batch, height, *hash, block, undo); err != nil {
			batch.Close()
			return err
		}
		if height%1000 == 999 {
			if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
				batch.Close()
				return err
			}
			batch.Close()
			batch = s.db.NewBatch()
		}
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	batch = s.db.NewBatch()
	writtenUTXOs := 0
	if err := s.ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		if err := putWalletUTXO(batch, outPoint, entry); err != nil {
			return err
		}
		writtenUTXOs++
		if writtenUTXOs%10_000 != 0 {
			return nil
		}
		if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
			return err
		}
		batch.Close()
		batch = s.db.NewBatch()
		return nil
	}); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Set(metaWalletIndexHeightKey, encodeU64(tipHeight), nil); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
		batch.Close()
		return err
	}
	batch.Close()
	s.logger.Info("rebuilt wallet indexes", slog.Uint64("height", tipHeight))
	return nil
}

func (s *ChainStore) walletIndexReadyForAppend(height uint64) (bool, error) {
	indexHeight, err := s.WalletIndexHeight()
	if err != nil {
		return false, err
	}
	if indexHeight == nil {
		return height == 0, nil
	}
	if height == 0 {
		return *indexHeight == 0, nil
	}
	return *indexHeight == height-1, nil
}

func (s *ChainStore) walletIndexReadyForReorg(oldTipHeight uint64) (bool, error) {
	indexHeight, err := s.WalletIndexHeight()
	if err != nil {
		return false, err
	}
	return indexHeight != nil && *indexHeight == oldTipHeight, nil
}

func applyWalletActiveBlockBatch(batch *pebble.Batch, height uint64, hash [32]byte, block *types.Block, undo []BlockUndoEntry, created map[types.OutPoint]consensus.UtxoEntry) error {
	if err := putWalletActivityForBlockBatch(batch, height, hash, block, undo); err != nil {
		return err
	}
	// Undo already contains the pre-block UTXOs needed to locate wallet rows.
	for _, spent := range undo {
		if err := deleteWalletUTXO(batch, spent.OutPoint, spent.Entry); err != nil {
			return err
		}
	}
	for outPoint, entry := range created {
		if err := putWalletUTXO(batch, outPoint, entry); err != nil {
			return err
		}
	}
	return batch.Set(metaWalletIndexHeightKey, encodeU64(height), nil)
}

func (s *ChainStore) applyWalletReorgBatch(batch *pebble.Batch, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry, forkHeight uint64, oldTipHeight uint64, activeEntries []BlockIndexEntry) error {
	if err := s.deleteWalletActivityHeightRangeBatch(batch, forkHeight+1, oldTipHeight); err != nil {
		return err
	}
	for _, entry := range activeEntries {
		hash := consensus.HeaderHash(&entry.Header)
		block, err := s.GetBlock(&hash)
		if err != nil {
			return err
		}
		if block == nil {
			return fmt.Errorf("missing active block for wallet reorg index %x", hash)
		}
		undo, err := s.GetUndo(&hash)
		if err != nil {
			return err
		}
		if err := putWalletActivityForBlockBatch(batch, entry.Height, hash, block, undo); err != nil {
			return err
		}
	}
	for _, outPoint := range spent {
		entry, ok, err := s.GetUTXO(outPoint)
		if err != nil {
			return err
		}
		if ok {
			if err := deleteWalletUTXO(batch, outPoint, entry); err != nil {
				return err
			}
		}
	}
	for outPoint, entry := range created {
		if err := putWalletUTXO(batch, outPoint, entry); err != nil {
			return err
		}
	}
	if len(activeEntries) == 0 {
		return batch.Delete(metaWalletIndexHeightKey, nil)
	}
	return batch.Set(metaWalletIndexHeightKey, encodeU64(activeEntries[len(activeEntries)-1].Height), nil)
}

func putWalletUTXO(batch *pebble.Batch, outPoint types.OutPoint, entry consensus.UtxoEntry) error {
	record := WalletIndexedUTXO{Entry: entry, Height: entry.CreatedHeight, Coinbase: entry.Coinbase}
	return batch.Set(walletUTXOKey(walletWatchItemForEntry(entry), outPoint), encodeWalletIndexedUTXO(record), nil)
}

func deleteWalletUTXO(batch *pebble.Batch, outPoint types.OutPoint, entry consensus.UtxoEntry) error {
	return batch.Delete(walletUTXOKey(walletWatchItemForEntry(entry), outPoint), nil)
}

func putWalletActivityForBlockBatch(batch *pebble.Batch, height uint64, hash [32]byte, block *types.Block, undo []BlockUndoEntry) error {
	if block == nil {
		return nil
	}
	resolvedInputs, err := consensus.ResolveBlockInputEntries(block, undo)
	if err != nil {
		return fmt.Errorf("resolve wallet activity inputs at height %d: %w", height, err)
	}
	timestamp := block.Header.Timestamp
	for txIndex, tx := range block.Txs {
		type itemDelta struct {
			received uint64
			sent     uint64
		}
		deltas := make(map[WalletWatchItem]itemDelta)
		for _, output := range tx.Base.Outputs {
			item := walletWatchItemForOutput(output)
			delta := deltas[item]
			delta.received += output.ValueAtoms
			deltas[item] = delta
		}
		inputSum := uint64(0)
		if txIndex > 0 {
			for _, input := range tx.Base.Inputs {
				entry, ok := resolvedInputs[input.PrevOut]
				if !ok {
					return fmt.Errorf("missing resolved wallet activity input at height %d: %v", height, input.PrevOut)
				}
				inputSum += entry.ValueAtoms
				item := walletWatchItemForEntry(entry)
				delta := deltas[item]
				delta.sent += entry.ValueAtoms
				deltas[item] = delta
			}
		}
		txid := consensus.TxID(&tx)
		if len(deltas) == 0 {
			continue
		}
		outputSum := uint64(0)
		for _, output := range tx.Base.Outputs {
			outputSum += output.ValueAtoms
		}
		fee := uint64(0)
		if txIndex > 0 && inputSum >= outputSum {
			fee = inputSum - outputSum
		}
		for item, delta := range deltas {
			if delta.received == 0 && delta.sent == 0 {
				continue
			}
			itemFee := uint64(0)
			if delta.sent > 0 {
				itemFee = fee
			}
			record := WalletActivityRecord{
				TxID:      txid,
				BlockHash: hash,
				Height:    height,
				Timestamp: timestamp,
				Coinbase:  txIndex == 0,
				Received:  delta.received,
				Sent:      delta.sent,
				Fee:       itemFee,
			}
			encoded := encodeWalletActivityRecord(record)
			if err := batch.Set(walletActivityItemKey(item, height, txid), encoded, nil); err != nil {
				return err
			}
			if err := batch.Set(walletActivityHeightKey(height, item, txid), nil, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *ChainStore) deleteWalletActivityHeightRangeBatch(batch *pebble.Batch, firstHeight uint64, lastHeight uint64) error {
	if firstHeight > lastHeight {
		return nil
	}
	lower := walletActivityHeightPrefixForHeight(firstHeight)
	var upper []byte
	if lastHeight == ^uint64(0) {
		upper = walletActHtPrefixEnd
	} else {
		upper = walletActivityHeightPrefixForHeight(lastHeight + 1)
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) < len(walletActivityHtPrefix)+8 {
			return errors.New("invalid wallet activity height key")
		}
		height, err := decodeU64BE(iter.Key()[len(walletActivityHtPrefix) : len(walletActivityHtPrefix)+8])
		if err != nil {
			return err
		}
		item, txid, err := decodeWalletActivityHeightKey(iter.Key(), height)
		if err != nil {
			return err
		}
		if err := batch.Delete(walletActivityItemKey(item, height, txid), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return batch.DeleteRange(lower, upper, nil)
}

func (s *ChainStore) deleteWalletIndexesBatch(batch *pebble.Batch) error {
	for _, bounds := range []struct {
		lower []byte
		upper []byte
	}{
		{walletUTXOPrefix, walletUTXOPrefixEnd},
		{walletActivityItemPrefix, walletActItemPrefixEnd},
		{walletActivityHtPrefix, walletActHtPrefixEnd},
	} {
		if err := batch.DeleteRange(bounds.lower, bounds.upper, nil); err != nil {
			return err
		}
	}
	return batch.Delete(metaWalletIndexHeightKey, nil)
}

func invalidateWalletIndexesBatch(batch *pebble.Batch) error {
	return batch.Delete(metaWalletIndexHeightKey, nil)
}

func walletUTXOItemPrefix(item WalletWatchItem) []byte {
	buf := append([]byte(nil), walletUTXOPrefix...)
	encodeWalletWatchItem(&buf, item)
	return buf
}

func walletUTXOKey(item WalletWatchItem, outPoint types.OutPoint) []byte {
	buf := walletUTXOItemPrefix(item)
	outPoint.Encode(&buf)
	return buf
}

func walletActivityItemWatchPrefix(item WalletWatchItem) []byte {
	buf := append([]byte(nil), walletActivityItemPrefix...)
	encodeWalletWatchItem(&buf, item)
	return buf
}

func walletActivityItemKey(item WalletWatchItem, height uint64, txid [32]byte) []byte {
	buf := walletActivityItemWatchPrefix(item)
	buf = append(buf, encodeU64BE(^height)...)
	buf = append(buf, txid[:]...)
	return buf
}

func walletActivityHeightPrefixForHeight(height uint64) []byte {
	return append(append([]byte(nil), walletActivityHtPrefix...), encodeU64BE(height)...)
}

func walletActivityHeightKey(height uint64, item WalletWatchItem, txid [32]byte) []byte {
	buf := walletActivityHeightPrefixForHeight(height)
	encodeWalletWatchItem(&buf, item)
	buf = append(buf, txid[:]...)
	return buf
}

func encodeWalletWatchItem(buf *[]byte, item WalletWatchItem) {
	*buf = append(*buf, encodeU64(item.Type)...)
	*buf = append(*buf, item.Payload32[:]...)
}

func decodeWalletWatchItem(buf []byte) (WalletWatchItem, error) {
	if len(buf) != 40 {
		return WalletWatchItem{}, errors.New("invalid wallet watch item encoding")
	}
	itemType, err := decodeU64(buf[:8])
	if err != nil {
		return WalletWatchItem{}, err
	}
	var payload [32]byte
	copy(payload[:], buf[8:40])
	return WalletWatchItem{Type: itemType, Payload32: payload}, nil
}

func encodeWalletIndexedUTXO(record WalletIndexedUTXO) []byte {
	buf := make([]byte, 0, 8+1+49)
	buf = append(buf, encodeU64(record.Height)...)
	if record.Coinbase {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return append(buf, encodeUTXOEntry(record.Entry)...)
}

func decodeWalletIndexedUTXO(buf []byte) (WalletIndexedUTXO, error) {
	if len(buf) < 10 {
		return WalletIndexedUTXO{}, errors.New("invalid wallet UTXO encoding")
	}
	height, err := decodeU64(buf[:8])
	if err != nil {
		return WalletIndexedUTXO{}, err
	}
	coinbase, err := decodeBoolByte(buf[8])
	if err != nil {
		return WalletIndexedUTXO{}, err
	}
	entry, err := decodeUTXOEntry(buf[9:])
	if err != nil {
		return WalletIndexedUTXO{}, err
	}
	return WalletIndexedUTXO{Height: height, Coinbase: coinbase, Entry: entry}, nil
}

func encodeWalletActivityRecord(record WalletActivityRecord) []byte {
	buf := make([]byte, 0, 105)
	buf = append(buf, record.TxID[:]...)
	buf = append(buf, record.BlockHash[:]...)
	buf = append(buf, encodeU64(record.Height)...)
	buf = append(buf, encodeU64(record.Timestamp)...)
	if record.Coinbase {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = append(buf, encodeU64(record.Received)...)
	buf = append(buf, encodeU64(record.Sent)...)
	buf = append(buf, encodeU64(record.Fee)...)
	return buf
}

func decodeWalletActivityRecord(buf []byte) (WalletActivityRecord, error) {
	if len(buf) != 105 {
		return WalletActivityRecord{}, errors.New("invalid wallet activity encoding")
	}
	var record WalletActivityRecord
	copy(record.TxID[:], buf[:32])
	copy(record.BlockHash[:], buf[32:64])
	var err error
	if record.Height, err = decodeU64(buf[64:72]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Timestamp, err = decodeU64(buf[72:80]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Coinbase, err = decodeBoolByte(buf[80]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Received, err = decodeU64(buf[81:89]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Sent, err = decodeU64(buf[89:97]); err != nil {
		return WalletActivityRecord{}, err
	}
	if record.Fee, err = decodeU64(buf[97:105]); err != nil {
		return WalletActivityRecord{}, err
	}
	return record, nil
}

func decodeWalletActivityHeightKey(key []byte, height uint64) (WalletWatchItem, [32]byte, error) {
	prefixLen := len(walletActivityHtPrefix) + 8
	if len(key) != prefixLen+40+32 {
		return WalletWatchItem{}, [32]byte{}, errors.New("invalid wallet activity height key")
	}
	gotHeight, err := decodeU64BE(key[len(walletActivityHtPrefix):prefixLen])
	if err != nil {
		return WalletWatchItem{}, [32]byte{}, err
	}
	if gotHeight != height {
		return WalletWatchItem{}, [32]byte{}, errors.New("wallet activity height key mismatch")
	}
	item, err := decodeWalletWatchItem(key[prefixLen : prefixLen+40])
	if err != nil {
		return WalletWatchItem{}, [32]byte{}, err
	}
	var txid [32]byte
	copy(txid[:], key[prefixLen+40:])
	return item, txid, nil
}

func walletWatchItemForOutput(output types.TxOutput) WalletWatchItem {
	return WalletWatchItem{Type: output.Type, Payload32: output.CanonicalPayload32()}
}

func walletWatchItemForEntry(entry consensus.UtxoEntry) WalletWatchItem {
	item := WalletWatchItem{Type: entry.Type, Payload32: entry.Payload32}
	if item.Type == types.OutputXOnlyP2PK && item.Payload32 == ([32]byte{}) {
		item.Payload32 = entry.PubKey
	}
	return item
}

func compareWalletIndexedUTXOs(a, b WalletIndexedUTXO) int {
	if a.Entry.Type < b.Entry.Type {
		return -1
	}
	if a.Entry.Type > b.Entry.Type {
		return 1
	}
	aItem := walletWatchItemForEntry(a.Entry)
	bItem := walletWatchItemForEntry(b.Entry)
	if cmp := bytes.Compare(aItem.Payload32[:], bItem.Payload32[:]); cmp != 0 {
		return cmp
	}
	return compareOutPoints(a.OutPoint, b.OutPoint)
}

func compareWalletActivityRecords(a, b WalletActivityRecord) int {
	switch {
	case a.Height > b.Height:
		return -1
	case a.Height < b.Height:
		return 1
	}
	if cmp := bytes.Compare(a.TxID[:], b.TxID[:]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(a.BlockHash[:], b.BlockHash[:])
}
