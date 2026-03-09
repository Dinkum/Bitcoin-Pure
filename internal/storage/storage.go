package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/types"
	"github.com/cockroachdb/pebble"
)

var (
	metaProfileKey         = []byte("meta/profile")
	metaHeaderTipHeightKey = []byte("meta/header_tip_height")
	metaHeaderTipHeaderKey = []byte("meta/header_tip_header")
	metaTipHeightKey       = []byte("meta/tip_height")
	metaTipHeaderKey       = []byte("meta/tip_header")
	metaBlockSizeStateKey  = []byte("meta/block_size_state")
	blockPrefix            = []byte("blocks/")
	blockIndexPrefix       = []byte("block_index/")
	blockUndoPrefix        = []byte("block_undo/")
	heightIndexPrefix      = []byte("height_index/")
	knownPeerPrefix        = []byte("known_peer/")
	utxoPrefix             = []byte("utxo/")
)

var (
	knownPeerPrefixEnd = prefixUpperBound(knownPeerPrefix)
	utxoPrefixEnd      = prefixUpperBound(utxoPrefix)
)

type StoredChainState struct {
	Profile        types.ChainProfile
	Height         uint64
	TipHeader      types.BlockHeader
	BlockSizeState consensus.BlockSizeState
	UTXOs          consensus.UtxoSet
}

type StoredHeaderState struct {
	Profile   types.ChainProfile
	Height    uint64
	TipHeader types.BlockHeader
}

type BlockIndexEntry struct {
	Height         uint64
	ParentHash     [32]byte
	Header         types.BlockHeader
	ChainWork      [32]byte
	Validated      bool
	BlockSizeState consensus.BlockSizeState
}

type BlockUndoEntry struct {
	OutPoint types.OutPoint
	Entry    consensus.UtxoEntry
}

type HeaderBatchEntry struct {
	Height    uint64
	Header    types.BlockHeader
	ChainWork [32]byte
}

type ChainStore struct {
	db *pebble.DB
}

type noopLogger struct{}

func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Fatalf(string, ...interface{}) {}

func Open(path string) (*ChainStore, error) {
	logging.Component("storage").Info("opening pebble chain store", slog.String("path", path))
	db, err := pebble.Open(filepath.Clean(path), &pebble.Options{
		Logger: noopLogger{},
	})
	if err != nil {
		return nil, err
	}
	return &ChainStore{db: db}, nil
}

func (s *ChainStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	logging.Component("storage").Info("closing pebble chain store")
	return s.db.Close()
}

func (s *ChainStore) LoadChainState() (*StoredChainState, error) {
	profileBytes, err := s.get(metaProfileKey)
	if err != nil {
		return nil, err
	}
	if profileBytes == nil {
		return nil, nil
	}
	heightBytes, err := s.get(metaTipHeightKey)
	if err != nil {
		return nil, err
	}
	headerBytes, err := s.get(metaTipHeaderKey)
	if err != nil {
		return nil, err
	}
	blockSizeBytes, err := s.get(metaBlockSizeStateKey)
	if err != nil {
		return nil, err
	}
	if heightBytes == nil && headerBytes == nil && blockSizeBytes == nil {
		return nil, nil
	}
	if heightBytes == nil || headerBytes == nil || blockSizeBytes == nil {
		return nil, errors.New("invalid data: missing chain metadata")
	}

	profile, err := types.ParseChainProfile(string(profileBytes))
	if err != nil {
		return nil, err
	}
	height, err := decodeU64(heightBytes)
	if err != nil {
		return nil, err
	}
	header, err := types.DecodeBlockHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	blockSizeState, err := decodeBlockSizeState(blockSizeBytes)
	if err != nil {
		return nil, err
	}

	utxos := make(consensus.UtxoSet)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: utxoPrefix,
		UpperBound: utxoPrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		outpoint, err := decodeOutPoint(iter.Key()[len(utxoPrefix):])
		if err != nil {
			return nil, err
		}
		entry, err := decodeUTXOEntry(iter.Value())
		if err != nil {
			return nil, err
		}
		utxos[outpoint] = entry
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return &StoredChainState{
		Profile:        profile,
		Height:         height,
		TipHeader:      header,
		BlockSizeState: blockSizeState,
		UTXOs:          utxos,
	}, nil
}

func (s *ChainStore) LoadHeaderState() (*StoredHeaderState, error) {
	profileBytes, err := s.get(metaProfileKey)
	if err != nil {
		return nil, err
	}
	if profileBytes == nil {
		return nil, nil
	}

	heightBytes, err := s.get(metaHeaderTipHeightKey)
	if err != nil {
		return nil, err
	}
	headerBytes, err := s.get(metaHeaderTipHeaderKey)
	if err != nil {
		return nil, err
	}
	if heightBytes == nil && headerBytes == nil {
		heightBytes, err = s.get(metaTipHeightKey)
		if err != nil {
			return nil, err
		}
		headerBytes, err = s.get(metaTipHeaderKey)
		if err != nil {
			return nil, err
		}
	}
	if heightBytes == nil || headerBytes == nil {
		return nil, nil
	}

	profile, err := types.ParseChainProfile(string(profileBytes))
	if err != nil {
		return nil, err
	}
	height, err := decodeU64(heightBytes)
	if err != nil {
		return nil, err
	}
	header, err := types.DecodeBlockHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	return &StoredHeaderState{
		Profile:   profile,
		Height:    height,
		TipHeader: header,
	}, nil
}

func (s *ChainStore) LoadKnownPeers() (map[string]time.Time, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: knownPeerPrefix,
		UpperBound: knownPeerPrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	peers := make(map[string]time.Time)
	for iter.First(); iter.Valid(); iter.Next() {
		addr := string(iter.Key()[len(knownPeerPrefix):])
		if addr == "" {
			continue
		}
		lastSeenUnix, err := decodeI64(iter.Value())
		if err != nil {
			return nil, err
		}
		peers[addr] = time.Unix(0, lastSeenUnix).UTC()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return peers, nil
}

func (s *ChainStore) WriteKnownPeers(peers map[string]time.Time) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: knownPeerPrefix,
		UpperBound: knownPeerPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for addr, lastSeen := range peers {
		if addr == "" {
			continue
		}
		if err := batch.Set(knownPeerKey(addr), encodeI64(lastSeen.UTC().UnixNano()), nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	logging.Component("storage").Info("wrote known peers", slog.Int("count", len(peers)))
	return nil
}

func (s *ChainStore) WriteFullState(state *StoredChainState) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMeta(batch, state); err != nil {
		return err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: utxoPrefix,
		UpperBound: utxoPrefixEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(cloneBytes(iter.Key()), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for outPoint, entry := range state.UTXOs {
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(entry), nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	logging.Component("storage").Info("wrote full chain state",
		slog.Uint64("height", state.Height),
		slog.Int("utxo_count", len(state.UTXOs)),
	)
	return nil
}

func (s *ChainStore) RewriteFullStateDelta(previous *StoredChainState, next *StoredChainState) error {
	if next == nil {
		return errors.New("next chain state is required")
	}
	if previous == nil {
		return s.WriteFullState(next)
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMeta(batch, next); err != nil {
		return err
	}

	deleted := 0
	written := 0
	for outPoint, previousEntry := range previous.UTXOs {
		nextEntry, ok := next.UTXOs[outPoint]
		if !ok {
			if err := batch.Delete(utxoKey(outPoint), nil); err != nil {
				return err
			}
			deleted++
			continue
		}
		if nextEntry == previousEntry {
			continue
		}
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(nextEntry), nil); err != nil {
			return err
		}
		written++
	}
	for outPoint, nextEntry := range next.UTXOs {
		if previousEntry, ok := previous.UTXOs[outPoint]; ok && previousEntry == nextEntry {
			continue
		}
		if _, ok := previous.UTXOs[outPoint]; ok {
			continue
		}
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(nextEntry), nil); err != nil {
			return err
		}
		written++
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	logging.Component("storage").Info("rewrote chain state via utxo delta",
		slog.Uint64("height", next.Height),
		slog.Int("deleted_utxos", deleted),
		slog.Int("written_utxos", written),
		slog.Int("final_utxo_count", len(next.UTXOs)),
	)
	return nil
}

func (s *ChainStore) WriteHeaderState(state *StoredHeaderState) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeHeaderMeta(batch, state); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	logging.Component("storage").Info("wrote header state", slog.Uint64("height", state.Height))
	return nil
}

func (s *ChainStore) CommitHeaderChain(state *StoredHeaderState, entries []HeaderBatchEntry, forkHeight uint64, oldTipHeight uint64, activeEntries []HeaderBatchEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeHeaderMeta(batch, state); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := putHeaderBatch(batch, BlockIndexEntry{
			Height:     entry.Height,
			ParentHash: entry.Header.PrevBlockHash,
			Header:     entry.Header,
			ChainWork:  entry.ChainWork,
		}, false); err != nil {
			return err
		}
	}
	if len(activeEntries) != 0 {
		for height := forkHeight + 1; height <= oldTipHeight; height++ {
			if err := batch.Delete(heightIndexKey(height), nil); err != nil {
				return err
			}
			if height == ^uint64(0) {
				break
			}
		}
		for _, entry := range activeEntries {
			hash := consensus.HeaderHash(&entry.Header)
			if err := batch.Set(heightIndexKey(entry.Height), hash[:], nil); err != nil {
				return err
			}
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	logging.Component("storage").Info("wrote header batch",
		slog.Uint64("height", state.Height),
		slog.Int("count", len(entries)),
	)
	return nil
}

func (s *ChainStore) PutBlock(height uint64, block *types.Block) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	entry, err := s.buildLinearIndexEntry(height, &block.Header, false, consensus.BlockSizeState{})
	if err != nil {
		return err
	}
	if err := putBlockBatch(batch, block, entry, true); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	logging.Component("storage").Info("stored block",
		slog.Uint64("height", height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
	)
	return nil
}

func (s *ChainStore) PutHeader(height uint64, header *types.BlockHeader) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	entry, err := s.buildLinearIndexEntry(height, header, false, consensus.BlockSizeState{})
	if err != nil {
		return err
	}
	if err := putHeaderBatch(batch, entry, true); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	hash := consensus.HeaderHash(header)
	logging.Component("storage").Info("stored header",
		slog.Uint64("height", height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
	)
	return nil
}

func (s *ChainStore) AppendBlock(state *StoredChainState, block *types.Block, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	entry, err := s.buildLinearIndexEntry(state.Height, &block.Header, true, state.BlockSizeState)
	if err != nil {
		return err
	}
	return s.AppendValidatedBlock(state, block, &entry, nil, spent, created)
}

func (s *ChainStore) AppendValidatedBlock(state *StoredChainState, block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry, spent []types.OutPoint, created map[types.OutPoint]consensus.UtxoEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := writeMeta(batch, state); err != nil {
		return err
	}
	if err := putBlockBatch(batch, block, *entry, true); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	for _, outPoint := range spent {
		if err := batch.Delete(utxoKey(outPoint), nil); err != nil {
			return err
		}
	}
	for outPoint, entry := range created {
		if err := batch.Set(utxoKey(outPoint), encodeUTXOEntry(entry), nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	logging.Component("storage").Info("appended block delta",
		slog.Uint64("height", state.Height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
		slog.Int("spent_utxos", len(spent)),
		slog.Int("created_utxos", len(created)),
	)
	return nil
}

func (s *ChainStore) PutValidatedBlock(block *types.Block, entry *BlockIndexEntry, undo []BlockUndoEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := putBlockBatch(batch, block, *entry, false); err != nil {
		return err
	}
	hash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockUndoKey(hash), encodeBlockUndo(undo), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *ChainStore) RewriteActiveHeights(forkHeight uint64, oldTipHeight uint64, entries []BlockIndexEntry) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	for height := forkHeight + 1; height <= oldTipHeight; height++ {
		if err := batch.Delete(heightIndexKey(height), nil); err != nil {
			return err
		}
		if height == ^uint64(0) {
			break
		}
	}
	for _, entry := range entries {
		hash := consensus.HeaderHash(&entry.Header)
		if err := batch.Set(heightIndexKey(entry.Height), hash[:], nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.NoSync)
}

func (s *ChainStore) GetBlock(blockHash *[32]byte) (*types.Block, error) {
	buf, err := s.get(blockKey(*blockHash))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	block, err := types.DecodeBlockWithLimits(buf, types.DefaultCodecLimits())
	if err != nil {
		return nil, err
	}
	return &block, nil
}

func (s *ChainStore) GetUndo(blockHash *[32]byte) ([]BlockUndoEntry, error) {
	buf, err := s.get(blockUndoKey(*blockHash))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return decodeBlockUndo(buf)
}

func (s *ChainStore) GetBlockHashByHeight(height uint64) (*[32]byte, error) {
	buf, err := s.get(heightIndexKey(height))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	if len(buf) != 32 {
		return nil, errors.New("invalid height index encoding")
	}
	var hash [32]byte
	copy(hash[:], buf)
	return &hash, nil
}

func (s *ChainStore) GetBlockIndex(blockHash *[32]byte) (*BlockIndexEntry, error) {
	buf, err := s.get(blockIndexKey(*blockHash))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return decodeBlockIndexEntry(buf)
}

func (s *ChainStore) GetBlockIndexByHeight(height uint64) (*BlockIndexEntry, error) {
	hash, err := s.GetBlockHashByHeight(height)
	if err != nil {
		return nil, err
	}
	if hash == nil {
		return nil, nil
	}
	return s.GetBlockIndex(hash)
}

func (s *ChainStore) GetBlockByHeight(height uint64) (*types.Block, error) {
	hash, err := s.GetBlockHashByHeight(height)
	if err != nil {
		return nil, err
	}
	if hash == nil {
		return nil, nil
	}
	return s.GetBlock(hash)
}

func (s *ChainStore) get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return cloneBytes(value), nil
}

func writeMeta(batch *pebble.Batch, state *StoredChainState) error {
	if err := writeHeaderMeta(batch, &StoredHeaderState{
		Profile:   state.Profile,
		Height:    state.Height,
		TipHeader: state.TipHeader,
	}); err != nil {
		return err
	}
	if err := batch.Set(metaTipHeightKey, encodeU64(state.Height), nil); err != nil {
		return err
	}
	if err := batch.Set(metaTipHeaderKey, state.TipHeader.Encode(), nil); err != nil {
		return err
	}
	return batch.Set(metaBlockSizeStateKey, encodeBlockSizeState(state.BlockSizeState), nil)
}

func putBlockBatch(batch *pebble.Batch, block *types.Block, entry BlockIndexEntry, active bool) error {
	blockHash := consensus.HeaderHash(&block.Header)
	if err := batch.Set(blockKey(blockHash), block.Encode(), nil); err != nil {
		return err
	}
	return putHeaderBatch(batch, entry, active)
}

func writeHeaderMeta(batch *pebble.Batch, state *StoredHeaderState) error {
	if err := batch.Set(metaProfileKey, []byte(state.Profile.String()), nil); err != nil {
		return err
	}
	if err := batch.Set(metaHeaderTipHeightKey, encodeU64(state.Height), nil); err != nil {
		return err
	}
	return batch.Set(metaHeaderTipHeaderKey, state.TipHeader.Encode(), nil)
}

func putHeaderBatch(batch *pebble.Batch, entry BlockIndexEntry, active bool) error {
	blockHash := consensus.HeaderHash(&entry.Header)
	if active {
		if err := batch.Set(heightIndexKey(entry.Height), blockHash[:], nil); err != nil {
			return err
		}
	}
	return batch.Set(blockIndexKey(blockHash), encodeBlockIndexEntry(entry), nil)
}

func cloneBytes(buf []byte) []byte {
	return append([]byte(nil), buf...)
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] == 0xff {
			continue
		}
		out[i]++
		return out[:i+1]
	}
	return nil
}

func encodeU64(v uint64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, v)
	return out
}

func encodeI64(v int64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(v))
	return out
}

func decodeU64(buf []byte) (uint64, error) {
	if len(buf) != 8 {
		return 0, errors.New("invalid u64 encoding")
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func decodeI64(buf []byte) (int64, error) {
	if len(buf) != 8 {
		return 0, errors.New("invalid i64 encoding")
	}
	return int64(binary.LittleEndian.Uint64(buf)), nil
}

func blockKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockPrefix...), hash[:]...)
}

func blockIndexKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockIndexPrefix...), hash[:]...)
}

func blockUndoKey(hash [32]byte) []byte {
	return append(append([]byte(nil), blockUndoPrefix...), hash[:]...)
}

func heightIndexKey(height uint64) []byte {
	return append(append([]byte(nil), heightIndexPrefix...), encodeU64(height)...)
}

func knownPeerKey(addr string) []byte {
	return append(append([]byte(nil), knownPeerPrefix...), []byte(addr)...)
}

func utxoKey(outPoint types.OutPoint) []byte {
	buf := append([]byte(nil), utxoPrefix...)
	outPoint.Encode(&buf)
	return buf
}

func decodeOutPoint(buf []byte) (types.OutPoint, error) {
	if len(buf) != 36 {
		return types.OutPoint{}, errors.New("invalid outpoint encoding")
	}
	var outPoint types.OutPoint
	copy(outPoint.TxID[:], buf[:32])
	outPoint.Vout = binary.LittleEndian.Uint32(buf[32:36])
	return outPoint, nil
}

func encodeUTXOEntry(entry consensus.UtxoEntry) []byte {
	buf := make([]byte, 8, 40)
	binary.LittleEndian.PutUint64(buf, entry.ValueAtoms)
	buf = append(buf, entry.KeyHash[:]...)
	return buf
}

func decodeUTXOEntry(buf []byte) (consensus.UtxoEntry, error) {
	if len(buf) != 40 {
		return consensus.UtxoEntry{}, errors.New("invalid utxo entry encoding")
	}
	var keyHash [32]byte
	copy(keyHash[:], buf[8:])
	return consensus.UtxoEntry{
		ValueAtoms: binary.LittleEndian.Uint64(buf[:8]),
		KeyHash:    keyHash,
	}, nil
}

func encodeBlockIndexEntry(entry BlockIndexEntry) []byte {
	blockSizeState := encodeBlockSizeState(entry.BlockSizeState)
	buf := make([]byte, 8, 8+32+148+32+1+4+len(blockSizeState))
	binary.LittleEndian.PutUint64(buf, entry.Height)
	buf = append(buf, entry.ParentHash[:]...)
	buf = append(buf, entry.Header.Encode()...)
	buf = append(buf, entry.ChainWork[:]...)
	if entry.Validated {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = append(buf, consensus.EncodeLenPrefixed(blockSizeState)...)
	return buf
}

func decodeBlockIndexEntry(buf []byte) (*BlockIndexEntry, error) {
	if len(buf) < 8+32+148+32+1+4 {
		return nil, errors.New("invalid block index entry encoding")
	}
	height := binary.LittleEndian.Uint64(buf[:8])
	var parentHash [32]byte
	copy(parentHash[:], buf[8:40])
	header, err := types.DecodeBlockHeader(buf[40 : 40+148])
	if err != nil {
		return nil, err
	}
	var chainWork [32]byte
	copy(chainWork[:], buf[188:220])
	validated := buf[220] != 0
	stateBytes, remaining, err := consensus.DecodeLenPrefixed(buf[221:])
	if err != nil {
		return nil, err
	}
	if len(remaining) != 0 {
		return nil, errors.New("unexpected trailing block index entry data")
	}
	blockSizeState, err := decodeBlockSizeState(stateBytes)
	if err != nil {
		return nil, err
	}
	return &BlockIndexEntry{
		Height:         height,
		ParentHash:     parentHash,
		Header:         header,
		ChainWork:      chainWork,
		Validated:      validated,
		BlockSizeState: blockSizeState,
	}, nil
}

func encodeBlockUndo(entries []BlockUndoEntry) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(entries)))
	for _, entry := range entries {
		entry.OutPoint.Encode(&buf)
		buf = append(buf, encodeUTXOEntry(entry.Entry)...)
	}
	return buf
}

func decodeBlockUndo(buf []byte) ([]BlockUndoEntry, error) {
	if len(buf) < 8 {
		return nil, errors.New("invalid block undo encoding")
	}
	count := binary.LittleEndian.Uint64(buf[:8])
	buf = buf[8:]
	entries := make([]BlockUndoEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(buf) < 36+40 {
			return nil, errors.New("truncated block undo entry")
		}
		outPoint, err := decodeOutPoint(buf[:36])
		if err != nil {
			return nil, err
		}
		entry, err := decodeUTXOEntry(buf[36 : 36+40])
		if err != nil {
			return nil, err
		}
		entries = append(entries, BlockUndoEntry{OutPoint: outPoint, Entry: entry})
		buf = buf[36+40:]
	}
	if len(buf) != 0 {
		return nil, errors.New("unexpected trailing block undo data")
	}
	return entries, nil
}

func (s *ChainStore) buildLinearIndexEntry(height uint64, header *types.BlockHeader, validated bool, blockSizeState consensus.BlockSizeState) (BlockIndexEntry, error) {
	work, err := consensus.BlockWork(header.NBits)
	if err != nil {
		return BlockIndexEntry{}, err
	}
	chainWork := work
	if height > 0 {
		parentEntry, err := s.GetBlockIndex(&header.PrevBlockHash)
		if err != nil {
			return BlockIndexEntry{}, err
		}
		if parentEntry == nil {
			return BlockIndexEntry{}, fmt.Errorf("missing parent block index for %x", header.PrevBlockHash)
		}
		chainWork = consensus.AddChainWork(parentEntry.ChainWork, work)
	}
	return BlockIndexEntry{
		Height:         height,
		ParentHash:     header.PrevBlockHash,
		Header:         *header,
		ChainWork:      chainWork,
		Validated:      validated,
		BlockSizeState: blockSizeState,
	}, nil
}

func encodeBlockSizeState(state consensus.BlockSizeState) []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[:8], state.BlockSize)
	binary.LittleEndian.PutUint64(buf[8:16], state.Epsilon)
	binary.LittleEndian.PutUint64(buf[16:24], state.Beta)
	return buf
}

func decodeBlockSizeState(buf []byte) (consensus.BlockSizeState, error) {
	switch len(buf) {
	case 24:
		if looksLikeLegacyBlockSizeState(buf) {
			return decodeLegacyBlockSizeState(buf)
		}
		return consensus.BlockSizeState{
			BlockSize: binary.LittleEndian.Uint64(buf[:8]),
			Epsilon:   binary.LittleEndian.Uint64(buf[8:16]),
			Beta:      binary.LittleEndian.Uint64(buf[16:24]),
		}, nil
	default:
		if len(buf) < 24 {
			return consensus.BlockSizeState{}, errors.New("invalid block size state encoding")
		}
		return decodeLegacyBlockSizeState(buf)
	}
}

func looksLikeLegacyBlockSizeState(buf []byte) bool {
	// The old encoding was {limit, ewma, recent_count}. A 24-byte payload with a
	// very small third word is almost certainly that legacy form because modern
	// ABLA state keeps Beta at or above the multi-megabyte floor, not a tiny
	// recent-block counter.
	return binary.LittleEndian.Uint64(buf[16:24]) <= 65_536
}

func decodeLegacyBlockSizeState(buf []byte) (consensus.BlockSizeState, error) {
	count := binary.LittleEndian.Uint64(buf[16:24])
	expected := 24 + int(count)*8
	if len(buf) != expected {
		return consensus.BlockSizeState{}, fmt.Errorf("invalid block size state length: %d", len(buf))
	}
	limit := binary.LittleEndian.Uint64(buf[:8])
	ewma := binary.LittleEndian.Uint64(buf[8:16])
	blockSize := ewma
	if count > 0 {
		start := 24 + (int(count)-1)*8
		blockSize = binary.LittleEndian.Uint64(buf[start : start+8])
	}
	epsilon := limit / 2
	beta := limit - epsilon
	// Legacy nodes stored only the current limit, EWMA, and recent block sizes.
	// Seed the newer ABLA state with the same total limit and the latest observed
	// block size (or EWMA when history is empty) so reopen/migration preserves the
	// operator-visible ceiling instead of failing on decode.
	return consensus.BlockSizeState{
		BlockSize: blockSize,
		Epsilon:   epsilon,
		Beta:      beta,
	}, nil
}
