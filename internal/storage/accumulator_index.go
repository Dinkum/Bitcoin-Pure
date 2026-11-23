package storage

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"math"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"github.com/cockroachdb/pebble"
)

// migrateUTXOAccumulatorIndex rebuilds the v1 derived index from the canonical
// UTXO keyspace into a separate, bounded-write staging namespace. Only the
// final synced metadata batch activates v2, so an interrupted migration leaves
// the complete v1 index visible rather than a partially written v2 index.
func (s *ChainStore) migrateUTXOAccumulatorIndex() error {
	versionBytes, err := s.get(metaUTXOAccumulatorVersionKey)
	if err != nil || versionBytes == nil {
		return err
	}
	version, err := decodeU64(versionBytes)
	if err != nil {
		return err
	}
	switch version {
	case utxoAccumulatorIndexVersion:
		if _, ok, err := s.LoadUTXOAccumulator(); err != nil {
			return fmt.Errorf("validate utxo accumulator index: %w", err)
		} else if !ok {
			return errors.New("utxo accumulator v2 metadata is present without an index")
		}
		s.cleanupLegacyUTXOAccumulatorIndex()
		return nil
	case utxoAccumulatorIndexV1:
	default:
		return fmt.Errorf("unsupported utxo accumulator index version %d", version)
	}

	meta, err := s.LoadChainStateMeta()
	if err != nil {
		return fmt.Errorf("load chain metadata for utxo accumulator migration: %w", err)
	}
	if meta == nil {
		return errors.New("cannot migrate utxo accumulator index without chain metadata")
	}
	acc := utreexo.NewAccumulator()
	if err := s.ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		next, err := acc.Add(consensus.UtxoLeafFromEntry(outPoint, entry))
		if err != nil {
			return err
		}
		acc = next
		return nil
	}); err != nil {
		return fmt.Errorf("scan canonical utxos for accumulator migration: %w", err)
	}
	if acc.Count() != meta.UTXOCount {
		return fmt.Errorf("refusing utxo accumulator migration: rebuilt count=%d meta=%d", acc.Count(), meta.UTXOCount)
	}
	root := acc.Root()
	if root != meta.TipHeader.UTXORoot {
		return fmt.Errorf("refusing utxo accumulator migration: rebuilt root=%x header=%x", root, meta.TipHeader.UTXORoot)
	}
	if meta.UTXOAccumulatorRoot != ([32]byte{}) && root != meta.UTXOAccumulatorRoot {
		return fmt.Errorf("refusing utxo accumulator migration: rebuilt root=%x meta=%x", root, meta.UTXOAccumulatorRoot)
	}

	// v2 lives in a separate namespace. Populate it in bounded synced chunks
	// while v1 remains active, then make the completed generation visible with
	// one metadata batch. A restart after interrupted staging simply discards
	// the inactive v2 namespace and starts it again.
	clearBatch := s.db.NewBatch()
	if err := clearBatch.DeleteRange(utxoAccumulatorNodePrefix, utxoAccumulatorNodePrefixEnd, nil); err != nil {
		clearBatch.Close()
		return err
	}
	if err := clearBatch.Commit(consensusCriticalWriteOptions); err != nil {
		clearBatch.Close()
		return fmt.Errorf("clear incomplete utxo accumulator staging index: %w", err)
	}
	clearBatch.Close()

	if err := s.stageAccumulatorIndex(acc); err != nil {
		return err
	}
	if err := validateAccumulatorIndexRecords(s.db, acc); err != nil {
		return fmt.Errorf("validate staged utxo accumulator index: %w", err)
	}

	activation := s.db.NewBatch()
	if rootPath, ok := utreexo.AccumulatorRootPath(acc); ok {
		if err := activation.Set(metaUTXOAccumulatorRootPathKey, encodeAccumulatorPath(rootPath), nil); err != nil {
			activation.Close()
			return err
		}
	} else if err := activation.Delete(metaUTXOAccumulatorRootPathKey, nil); err != nil {
		activation.Close()
		return err
	}
	if err := activation.Set(metaUTXOAccumulatorVersionKey, encodeU64(utxoAccumulatorIndexVersion), nil); err != nil {
		activation.Close()
		return err
	}
	if err := activation.Commit(consensusCriticalWriteOptions); err != nil {
		activation.Close()
		return fmt.Errorf("activate utxo accumulator index migration: %w", err)
	}
	activation.Close()

	s.cleanupLegacyUTXOAccumulatorIndex()
	s.logger.Info("migrated utxo accumulator index", slog.Uint64("from_version", version), slog.Uint64("to_version", utxoAccumulatorIndexVersion), slog.Int("utxo_count", acc.Count()))
	return nil
}

func (s *ChainStore) stageAccumulatorIndex(acc *utreexo.Accumulator) error {
	const migrationRecordBatchSize = 4096
	batch := s.db.NewBatch()
	defer func() { batch.Close() }()
	batchCount := 0
	flush := func() error {
		if batchCount == 0 {
			return nil
		}
		if err := batch.Commit(consensusCriticalWriteOptions); err != nil {
			return err
		}
		batch.Close()
		batch = s.db.NewBatch()
		batchCount = 0
		return nil
	}
	err := utreexo.ForEachAccumulatorNodeRecord(acc, func(record utreexo.AccumulatorNodeRecord) error {
		if err := batch.Set(accumulatorNodeKey(record.Path), encodeAccumulatorNodeValue(record), nil); err != nil {
			return err
		}
		batchCount++
		if batchCount == migrationRecordBatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("stage utxo accumulator index records: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("stage utxo accumulator index records: %w", err)
	}
	return nil
}

func validateAccumulatorIndexRecords(reader storeReader, acc *utreexo.Accumulator) error {
	expectedCount := 0
	if err := utreexo.ForEachAccumulatorNodeRecord(acc, func(record utreexo.AccumulatorNodeRecord) error {
		expectedCount++
		value, closer, err := reader.Get(accumulatorNodeKey(record.Path))
		if err != nil {
			return err
		}
		matches := bytes.Equal(value, encodeAccumulatorNodeValue(record))
		closer.Close()
		if !matches {
			return fmt.Errorf("staged accumulator record mismatch at depth %d", record.Path.Depth)
		}
		return nil
	}); err != nil {
		return err
	}
	actualCount := 0
	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: utxoAccumulatorNodePrefix, UpperBound: utxoAccumulatorNodePrefixEnd})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		actualCount++
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if actualCount != expectedCount {
		return fmt.Errorf("staged accumulator record count=%d, want %d", actualCount, expectedCount)
	}
	return nil
}

func (s *ChainStore) cleanupLegacyUTXOAccumulatorIndex() {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: legacyUTXOAccumulatorNodePrefix, UpperBound: legacyUTXOAccumulatorNodePrefixEnd})
	if err != nil {
		s.logger.Warn("failed to inspect legacy utxo accumulator index", slog.String("error", err.Error()))
		return
	}
	hasLegacy := iter.First()
	iterErr := iter.Error()
	iter.Close()
	if iterErr != nil {
		s.logger.Warn("failed to inspect legacy utxo accumulator index", slog.String("error", iterErr.Error()))
		return
	}
	if !hasLegacy {
		return
	}
	cleanup := s.db.NewBatch()
	defer cleanup.Close()
	if err := cleanup.DeleteRange(legacyUTXOAccumulatorNodePrefix, legacyUTXOAccumulatorNodePrefixEnd, nil); err != nil {
		s.logger.Warn("failed to schedule legacy utxo accumulator cleanup", slog.String("error", err.Error()))
		return
	}
	if err := cleanup.Commit(bestEffortWriteOptions); err != nil {
		s.logger.Warn("failed to clean legacy utxo accumulator index", slog.String("error", err.Error()))
	}
}

func (s *ChainStore) LoadUTXOAccumulator() (*utreexo.Accumulator, bool, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	versionBytes, err := readValue(snap, metaUTXOAccumulatorVersionKey)
	if err != nil {
		return nil, false, err
	}
	if versionBytes == nil {
		return nil, false, nil
	}
	version, err := decodeU64(versionBytes)
	if err != nil {
		return nil, false, err
	}
	if version != utxoAccumulatorIndexVersion {
		return nil, false, fmt.Errorf("unsupported utxo accumulator index version %d", version)
	}
	rootPathBytes, err := readValue(snap, metaUTXOAccumulatorRootPathKey)
	if err != nil {
		return nil, false, err
	}
	meta, _, err := loadChainStateMeta(snap)
	if err != nil {
		return nil, false, err
	}
	if meta == nil {
		return nil, false, nil
	}
	acc, err := loadAccumulatorRecords(snap)
	if err != nil {
		return nil, false, err
	}
	if acc.Count() != meta.UTXOCount {
		return nil, false, fmt.Errorf("utxo accumulator count mismatch: index=%d meta=%d", acc.Count(), meta.UTXOCount)
	}
	if acc.Root() != meta.TipHeader.UTXORoot {
		return nil, false, fmt.Errorf("utxo accumulator root mismatch: index=%x header=%x", acc.Root(), meta.TipHeader.UTXORoot)
	}
	if meta.UTXOAccumulatorRoot != ([32]byte{}) && acc.Root() != meta.UTXOAccumulatorRoot {
		return nil, false, fmt.Errorf("utxo accumulator root mismatch: index=%x meta=%x", acc.Root(), meta.UTXOAccumulatorRoot)
	}
	rootPath, hasRoot := utreexo.AccumulatorRootPath(acc)
	if !hasRoot {
		if len(rootPathBytes) != 0 {
			return nil, false, errors.New("utxo accumulator root path present for empty index")
		}
	} else {
		persistedRootPath, err := decodeAccumulatorPath(rootPathBytes)
		if err != nil {
			return nil, false, fmt.Errorf("invalid utxo accumulator root path: %w", err)
		}
		if persistedRootPath != rootPath {
			return nil, false, errors.New("utxo accumulator root path mismatch")
		}
	}
	return acc, true, nil
}

func (s *ChainStore) UTXOAccumulatorProof(outPoint types.OutPoint) (utreexo.OutPointProof, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	return utxoAccumulatorProof(snap, outPoint)
}

func utxoAccumulatorProof(reader valueReader, outPoint types.OutPoint) (utreexo.OutPointProof, error) {
	proof := utreexo.OutPointProof{Version: utreexo.ProofVersion, OutPoint: outPoint}
	meta, _, err := loadChainStateMeta(reader)
	if err != nil {
		return proof, err
	}
	if meta == nil || meta.UTXOCount == 0 {
		return proof, nil
	}
	rootPathBytes, err := readValue(reader, metaUTXOAccumulatorRootPathKey)
	if err != nil {
		return proof, err
	}
	path, err := decodeAccumulatorPath(rootPathBytes)
	if err != nil {
		return proof, fmt.Errorf("invalid utxo accumulator root path: %w", err)
	}
	node, ok, err := readAccumulatorNode(reader, path)
	if err != nil {
		return proof, err
	}
	if !ok {
		return proof, errors.New("missing utxo accumulator root node")
	}
	queryKey := utreexo.OutPointKey(outPoint)
	if !accumulatorPathMatchesKey(path, queryKey) {
		terminal, err := firstAccumulatorLeaf(reader, path)
		if err != nil {
			return proof, err
		}
		membership, err := utxoAccumulatorProof(reader, terminal.OutPoint)
		if err != nil {
			return proof, err
		}
		terminalCopy := *terminal
		proof.Terminal = &terminalCopy
		proof.Steps = membership.Steps
		return proof, nil
	}
	steps := make([]utreexo.ProofStep, utreexo.KeyBits)
	for node.Leaf == nil {
		if node.LeftPath == nil || node.RightPath == nil || !validAccumulatorChildren(path, *node.LeftPath, *node.RightPath) {
			return proof, errors.New("invalid accumulator branch record")
		}
		queryRight := accumulatorBitSet(queryKey, path.Depth)
		nextPath := node.LeftPath
		siblingPath := node.RightPath
		if queryRight {
			nextPath, siblingPath = node.RightPath, node.LeftPath
		}
		sibling, siblingOK, err := readAccumulatorNode(reader, *siblingPath)
		if err != nil {
			return proof, err
		}
		if !siblingOK {
			return proof, errors.New("missing accumulator sibling node")
		}
		steps[path.Depth] = utreexo.ProofStep{HasSibling: true, SiblingHash: sibling.Hash}
		next, nextOK, err := readAccumulatorNode(reader, *nextPath)
		if err != nil {
			return proof, err
		}
		if !nextOK {
			return proof, errors.New("missing accumulator child node")
		}
		if !accumulatorPathMatchesKey(*nextPath, queryKey) {
			terminal, err := firstAccumulatorLeaf(reader, *nextPath)
			if err != nil {
				return proof, err
			}
			membership, err := utxoAccumulatorProof(reader, terminal.OutPoint)
			if err != nil {
				return proof, err
			}
			if !membership.Exists {
				return proof, errors.New("accumulator exclusion witness is not a member")
			}
			terminalCopy := *terminal
			proof.Terminal = &terminalCopy
			proof.Steps = membership.Steps
			return proof, nil
		}
		path = *nextPath
		node = next
	}
	if node.Leaf == nil {
		return proof, errors.New("invalid accumulator proof leaf")
	}
	if node.Leaf.OutPoint != outPoint {
		return proof, fmt.Errorf("accumulator proof leaf mismatch: got %x:%d", node.Leaf.OutPoint.TxID, node.Leaf.OutPoint.Vout)
	}
	proof.Exists = true
	proof.Type = node.Leaf.Type
	proof.ValueAtoms = node.Leaf.ValueAtoms
	proof.Payload32 = node.Leaf.Payload32
	proof.PubKey = node.Leaf.PubKey
	proof.Steps = steps
	return proof, nil
}

func firstAccumulatorLeaf(reader valueReader, path utreexo.AccumulatorNodePath) (*utreexo.UtxoLeaf, error) {
	for {
		node, ok, err := readAccumulatorNode(reader, path)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("missing accumulator exclusion witness node")
		}
		if node.Leaf != nil {
			leaf := *node.Leaf
			return &leaf, nil
		}
		if node.LeftPath == nil || node.RightPath == nil || !validAccumulatorChildren(path, *node.LeftPath, *node.RightPath) {
			return nil, errors.New("invalid accumulator branch record")
		}
		path = *node.LeftPath
	}
}

func (s *ChainStore) WriteUTXOAccumulator(acc *utreexo.Accumulator) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	meta, _, err := loadChainStateMeta(s.db)
	if err != nil {
		return err
	}
	if meta == nil || acc.Root() != meta.TipHeader.UTXORoot {
		return errors.New("accumulator does not match the persisted chain tip")
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := replaceAccumulatorIndexBatch(batch, acc); err != nil {
		return err
	}
	count := 0
	if acc != nil {
		count = acc.Count()
	}
	if err := batch.Set(metaUTXOCountKey, encodeU64(uint64(count)), nil); err != nil {
		return err
	}
	return batch.Commit(consensusCriticalWriteOptions)
}

func readAccumulatorNode(reader valueReader, path utreexo.AccumulatorNodePath) (utreexo.AccumulatorNodeRecord, bool, error) {
	key := accumulatorNodeKey(path)
	val, closer, err := reader.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return utreexo.AccumulatorNodeRecord{}, false, nil
	}
	if err != nil {
		return utreexo.AccumulatorNodeRecord{}, false, err
	}
	defer closer.Close()
	record, err := decodeAccumulatorNodeValue(path, val)
	if err != nil {
		return utreexo.AccumulatorNodeRecord{}, false, err
	}
	return record, true, nil
}

func applyAccumulatorStateBatch(batch *pebble.Batch, state *StoredChainStateMeta) error {
	if state == nil {
		return nil
	}
	switch {
	case state.UTXOAccumulatorDelta != nil:
		return applyAccumulatorDeltaBatch(batch, *state.UTXOAccumulatorDelta)
	case state.UTXOAccumulator != nil:
		return replaceAccumulatorIndexBatch(batch, state.UTXOAccumulator)
	default:
		return nil
	}
}

func (s *ChainStore) populateAccumulatorDeltaFromUTXODelta(state *StoredChainStateMeta, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	if state == nil || state.UTXOAccumulator != nil || state.UTXOAccumulatorDelta != nil {
		return nil
	}
	current, ok, err := s.LoadUTXOAccumulator()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	createdLeaves := make([]utreexo.UtxoLeaf, 0, len(created))
	for outPoint, entry := range created {
		createdLeaves = append(createdLeaves, consensus.UtxoLeafFromEntry(outPoint, entry))
	}
	next, err := current.Apply(spent, createdLeaves)
	if err != nil {
		return err
	}
	delta := utreexo.AccumulatorNodeDeltaBetween(current, next)
	state.UTXOAccumulatorDelta = &delta
	state.UTXOAccumulatorRoot = next.Root()
	state.UTXOCount = next.Count()
	return nil
}

func replaceAccumulatorIndexBatch(batch *pebble.Batch, acc *utreexo.Accumulator) error {
	if err := batch.DeleteRange(utxoAccumulatorNodePrefix, utxoAccumulatorNodePrefixEnd, nil); err != nil {
		return err
	}
	if err := utreexo.ForEachAccumulatorNodeRecord(acc, func(record utreexo.AccumulatorNodeRecord) error {
		return batch.Set(accumulatorNodeKey(record.Path), encodeAccumulatorNodeValue(record), nil)
	}); err != nil {
		return err
	}
	if err := batch.Set(metaUTXOAccumulatorVersionKey, encodeU64(utxoAccumulatorIndexVersion), nil); err != nil {
		return err
	}
	if rootPath, ok := utreexo.AccumulatorRootPath(acc); ok {
		if err := batch.Set(metaUTXOAccumulatorRootPathKey, encodeAccumulatorPath(rootPath), nil); err != nil {
			return err
		}
	} else if err := batch.Delete(metaUTXOAccumulatorRootPathKey, nil); err != nil {
		return err
	}
	if acc == nil {
		root := utreexo.NewAccumulator().Root()
		return batch.Set(metaUTXOAccumulatorRootKey, root[:], nil)
	}
	root := acc.Root()
	return batch.Set(metaUTXOAccumulatorRootKey, root[:], nil)
}

func applyAccumulatorDeltaBatch(batch *pebble.Batch, delta utreexo.AccumulatorNodeDelta) error {
	for _, path := range delta.Deletes {
		if !validAccumulatorPathDepth(path) {
			return errors.New("invalid accumulator delete path depth")
		}
		if err := batch.Delete(accumulatorNodeKey(path), nil); err != nil {
			return err
		}
	}
	for _, record := range delta.Upserts {
		if !validAccumulatorPathDepth(record.Path) {
			return errors.New("invalid accumulator upsert path depth")
		}
		if record.Leaf == nil && (record.LeftPath == nil || record.RightPath == nil || !validAccumulatorChildren(record.Path, *record.LeftPath, *record.RightPath)) {
			return errors.New("invalid accumulator upsert child paths")
		}
		if err := batch.Set(accumulatorNodeKey(record.Path), encodeAccumulatorNodeValue(record), nil); err != nil {
			return err
		}
	}
	if delta.RootPath != nil {
		if !validAccumulatorPathDepth(*delta.RootPath) {
			return errors.New("invalid accumulator delta root path depth")
		}
		if err := batch.Set(metaUTXOAccumulatorRootPathKey, encodeAccumulatorPath(*delta.RootPath), nil); err != nil {
			return err
		}
	} else if err := batch.Delete(metaUTXOAccumulatorRootPathKey, nil); err != nil {
		return err
	}
	return batch.Set(metaUTXOAccumulatorVersionKey, encodeU64(utxoAccumulatorIndexVersion), nil)
}

func validAccumulatorPathDepth(path utreexo.AccumulatorNodePath) bool {
	return path.Depth >= 0 && path.Depth <= utreexo.KeyBits
}

func accumulatorNodeKey(path utreexo.AccumulatorNodePath) []byte {
	if path.Depth < 0 || path.Depth > utreexo.KeyBits {
		panic("invalid accumulator node path depth")
	}
	keyLen := (path.Depth + 7) / 8
	buf := make([]byte, 0, len(utxoAccumulatorNodePrefix)+2+keyLen)
	buf = append(buf, utxoAccumulatorNodePrefix...)
	buf = append(buf, byte(path.Depth>>8), byte(path.Depth))
	buf = append(buf, path.Key[:keyLen]...)
	return buf
}

func accumulatorBitSet(key [utreexo.OutPointKeyBytes]byte, depth int) bool {
	byteIndex := depth / 8
	bitOffset := 7 - (depth % 8)
	return ((key[byteIndex] >> bitOffset) & 1) == 1
}

func decodeAccumulatorNodeKey(key []byte) (utreexo.AccumulatorNodePath, error) {
	if len(key) < len(utxoAccumulatorNodePrefix)+2 || !bytes.HasPrefix(key, utxoAccumulatorNodePrefix) {
		return utreexo.AccumulatorNodePath{}, errors.New("invalid accumulator node key")
	}
	raw := key[len(utxoAccumulatorNodePrefix):]
	depth := int(raw[0])<<8 | int(raw[1])
	if depth < 0 || depth > utreexo.KeyBits {
		return utreexo.AccumulatorNodePath{}, errors.New("invalid accumulator node depth")
	}
	keyLen := (depth + 7) / 8
	if len(raw) != 2+keyLen {
		return utreexo.AccumulatorNodePath{}, errors.New("invalid accumulator node path length")
	}
	if depth%8 != 0 && keyLen > 0 {
		unusedMask := byte((1 << (8 - depth%8)) - 1)
		if raw[2+keyLen-1]&unusedMask != 0 {
			return utreexo.AccumulatorNodePath{}, errors.New("non-canonical accumulator node path key")
		}
	}
	var pathKey [utreexo.OutPointKeyBytes]byte
	copy(pathKey[:], raw[2:])
	return utreexo.AccumulatorNodePath{Depth: depth, Key: pathKey}, nil
}

func encodeAccumulatorNodeValue(record utreexo.AccumulatorNodeRecord) []byte {
	buf := make([]byte, 0, 1+8+32+2*(2+utreexo.OutPointKeyBytes)+36+49)
	if record.Leaf != nil {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = append(buf, encodeU64(uint64(record.Count))...)
	buf = append(buf, record.Hash[:]...)
	if record.Leaf != nil {
		record.Leaf.OutPoint.Encode(&buf)
		entry := consensus.UtxoEntryFromLeaf(*record.Leaf)
		buf = append(buf, encodeCommittedCoin(entry)...)
	} else {
		if record.LeftPath == nil || record.RightPath == nil {
			panic("internal accumulator record is missing child paths")
		}
		buf = append(buf, encodeAccumulatorPath(*record.LeftPath)...)
		buf = append(buf, encodeAccumulatorPath(*record.RightPath)...)
	}
	return buf
}

func decodeAccumulatorNodeValue(path utreexo.AccumulatorNodePath, buf []byte) (utreexo.AccumulatorNodeRecord, error) {
	if len(buf) < 41 {
		return utreexo.AccumulatorNodeRecord{}, errors.New("invalid accumulator node value")
	}
	kind := buf[0]
	count, err := decodeU64(buf[1:9])
	if err != nil {
		return utreexo.AccumulatorNodeRecord{}, err
	}
	if count == 0 || count > uint64(math.MaxInt) {
		return utreexo.AccumulatorNodeRecord{}, errors.New("invalid accumulator node count")
	}
	record := utreexo.AccumulatorNodeRecord{
		Path:  path,
		Count: int(count),
	}
	copy(record.Hash[:], buf[9:41])
	remaining := buf[41:]
	switch kind {
	case 0:
		left, consumed, err := decodeAccumulatorPathPrefix(remaining)
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, fmt.Errorf("invalid left accumulator child path: %w", err)
		}
		right, rightConsumed, err := decodeAccumulatorPathPrefix(remaining[consumed:])
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, fmt.Errorf("invalid right accumulator child path: %w", err)
		}
		if count < 2 || consumed+rightConsumed != len(remaining) || !validAccumulatorChildren(path, left, right) {
			return utreexo.AccumulatorNodeRecord{}, errors.New("invalid internal accumulator node payload")
		}
		record.LeftPath = &left
		record.RightPath = &right
	case 1:
		if path.Depth != utreexo.KeyBits || count != 1 || len(remaining) < 36 {
			return utreexo.AccumulatorNodeRecord{}, errors.New("invalid leaf accumulator node payload")
		}
		outPoint, err := decodeOutPoint(remaining[:36])
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, err
		}
		if utreexo.OutPointKey(outPoint) != path.Key {
			return utreexo.AccumulatorNodeRecord{}, errors.New("accumulator leaf does not match its path")
		}
		entry, err := decodeCommittedCoin(remaining[36:])
		if err != nil {
			return utreexo.AccumulatorNodeRecord{}, err
		}
		leaf := consensus.UtxoLeafFromEntry(outPoint, entry)
		record.Leaf = &leaf
	default:
		return utreexo.AccumulatorNodeRecord{}, errors.New("invalid accumulator node kind")
	}
	return record, nil
}

func encodeAccumulatorPath(path utreexo.AccumulatorNodePath) []byte {
	if path.Depth < 0 || path.Depth > utreexo.KeyBits {
		panic("invalid accumulator path depth")
	}
	keyLen := (path.Depth + 7) / 8
	buf := make([]byte, 2+keyLen)
	buf[0] = byte(path.Depth >> 8)
	buf[1] = byte(path.Depth)
	copy(buf[2:], path.Key[:keyLen])
	return buf
}

func decodeAccumulatorPath(buf []byte) (utreexo.AccumulatorNodePath, error) {
	path, consumed, err := decodeAccumulatorPathPrefix(buf)
	if err != nil {
		return utreexo.AccumulatorNodePath{}, err
	}
	if consumed != len(buf) {
		return utreexo.AccumulatorNodePath{}, errors.New("trailing accumulator path bytes")
	}
	return path, nil
}

func decodeAccumulatorPathPrefix(buf []byte) (utreexo.AccumulatorNodePath, int, error) {
	if len(buf) < 2 {
		return utreexo.AccumulatorNodePath{}, 0, errors.New("truncated accumulator path")
	}
	depth := int(buf[0])<<8 | int(buf[1])
	if depth < 0 || depth > utreexo.KeyBits {
		return utreexo.AccumulatorNodePath{}, 0, errors.New("invalid accumulator path depth")
	}
	keyLen := (depth + 7) / 8
	if len(buf) < 2+keyLen {
		return utreexo.AccumulatorNodePath{}, 0, errors.New("truncated accumulator path key")
	}
	var key [utreexo.OutPointKeyBytes]byte
	copy(key[:], buf[2:2+keyLen])
	if depth%8 != 0 && keyLen > 0 {
		unusedMask := byte((1 << (8 - depth%8)) - 1)
		if key[keyLen-1]&unusedMask != 0 {
			return utreexo.AccumulatorNodePath{}, 0, errors.New("non-canonical accumulator path key")
		}
	}
	return utreexo.AccumulatorNodePath{Depth: depth, Key: key}, 2 + keyLen, nil
}

func accumulatorPathMatchesKey(path utreexo.AccumulatorNodePath, key [utreexo.OutPointKeyBytes]byte) bool {
	fullBytes := path.Depth / 8
	if !bytes.Equal(path.Key[:fullBytes], key[:fullBytes]) {
		return false
	}
	remaining := path.Depth % 8
	if remaining == 0 {
		return true
	}
	mask := byte(0xff << (8 - remaining))
	return path.Key[fullBytes]&mask == key[fullBytes]&mask
}

func validAccumulatorChildren(parent, left, right utreexo.AccumulatorNodePath) bool {
	if parent.Depth < 0 || parent.Depth >= utreexo.KeyBits ||
		left.Depth <= parent.Depth || left.Depth > utreexo.KeyBits ||
		right.Depth <= parent.Depth || right.Depth > utreexo.KeyBits {
		return false
	}
	if !accumulatorPathMatchesKey(parent, left.Key) || !accumulatorPathMatchesKey(parent, right.Key) {
		return false
	}
	return !accumulatorBitSet(left.Key, parent.Depth) && accumulatorBitSet(right.Key, parent.Depth)
}
