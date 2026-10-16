package node

import (
	"encoding/hex"
	"fmt"

	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/storage"
)

const (
	maxFilterHeadersPerRPC  = storage.MaxCompactFilterHeaders
	defaultFilterCheckpoint = 1000
)

type CompactFilterInfo struct {
	FilterType string `json:"filter_type"`
	Height     uint64 `json:"height"`
	BlockHash  string `json:"block_hash"`
	Entries    int    `json:"entries"`
	FilterHex  string `json:"filter_hex"`
	FilterHash string `json:"filter_hash"`
}

type CompactFilterHeaderEntry struct {
	Height       uint64 `json:"height"`
	BlockHash    string `json:"block_hash"`
	FilterHash   string `json:"filter_hash"`
	FilterHeader string `json:"filter_header"`
}

type CompactFilterHeadersInfo struct {
	FilterType           string                     `json:"filter_type"`
	StartHeight          uint64                     `json:"start_height"`
	Count                uint64                     `json:"count"`
	PreviousFilterHeader string                     `json:"previous_filter_header"`
	Headers              []CompactFilterHeaderEntry `json:"headers"`
}

type CompactFilterCheckpointEntry struct {
	Height       uint64 `json:"height"`
	BlockHash    string `json:"block_hash"`
	FilterHeader string `json:"filter_header"`
}

type CompactFilterCheckpointInfo struct {
	FilterType string                         `json:"filter_type"`
	Interval   uint64                         `json:"interval"`
	TipHeight  uint64                         `json:"tip_height"`
	Headers    []CompactFilterCheckpointEntry `json:"headers"`
}

func (s *Service) CompactFilterByHeight(height uint64) (CompactFilterInfo, error) {
	blockHash, err := s.chainState.Store().GetBlockHashByHeight(height)
	if err != nil {
		return CompactFilterInfo{}, err
	}
	if blockHash == nil {
		return CompactFilterInfo{}, fmt.Errorf("block height %d not found", height)
	}
	return s.CompactFilterByHash(*blockHash)
}

func (s *Service) CompactFilterByHash(blockHash [32]byte) (CompactFilterInfo, error) {
	block, err := s.chainState.Store().GetBlock(&blockHash)
	if err != nil {
		return CompactFilterInfo{}, err
	}
	if block == nil {
		return CompactFilterInfo{}, fmt.Errorf("block %x not found", blockHash)
	}
	undo, err := s.chainState.Store().GetUndo(&blockHash)
	if err != nil {
		return CompactFilterInfo{}, err
	}
	entry, err := s.chainState.Store().GetBlockIndex(&blockHash)
	if err != nil {
		return CompactFilterInfo{}, err
	}
	if entry == nil {
		return CompactFilterInfo{}, fmt.Errorf("block index %x not found", blockHash)
	}
	filter := compactfilter.Build(blockHash, block, compactFilterItemsForUndo(undo))
	return CompactFilterInfo{
		FilterType: compactfilter.Type(),
		Height:     entry.Height,
		BlockHash:  hex.EncodeToString(blockHash[:]),
		Entries:    filter.Entries,
		FilterHex:  hex.EncodeToString(filter.Encoded),
		FilterHash: hex.EncodeToString(filter.Hash[:]),
	}, nil
}

func (s *Service) CompactFilterHeaders(startHeight uint64, count uint64) (CompactFilterHeadersInfo, error) {
	if count > maxFilterHeadersPerRPC {
		return CompactFilterHeadersInfo{}, fmt.Errorf("count %d exceeds max %d", count, maxFilterHeadersPerRPC)
	}
	if count == 0 {
		return CompactFilterHeadersInfo{
			FilterType:  compactfilter.Type(),
			StartHeight: startHeight,
			Count:       0,
		}, nil
	}
	if startHeight > ^uint64(0)-(count-1) {
		return CompactFilterHeadersInfo{}, fmt.Errorf("filter height range overflows")
	}
	previous, rows, err := s.chainState.Store().CompactFilterHeaders(startHeight, count)
	if err != nil {
		return CompactFilterHeadersInfo{}, err
	}
	headers := make([]CompactFilterHeaderEntry, 0, len(rows))
	for _, row := range rows {
		headers = append(headers, CompactFilterHeaderEntry{Height: row.Height, BlockHash: hex.EncodeToString(row.BlockHash[:]), FilterHash: hex.EncodeToString(row.FilterHash[:]), FilterHeader: hex.EncodeToString(row.Header[:])})
	}
	return CompactFilterHeadersInfo{FilterType: compactfilter.Type(), StartHeight: startHeight, Count: count, PreviousFilterHeader: hex.EncodeToString(previous[:]), Headers: headers}, nil
}

func (s *Service) CompactFilterCheckpoint(interval uint64) (CompactFilterCheckpointInfo, error) {
	if interval == 0 {
		interval = defaultFilterCheckpoint
	}
	tip, rows, err := s.chainState.Store().CompactFilterCheckpoints(interval, maxFilterHeadersPerRPC)
	if err != nil {
		return CompactFilterCheckpointInfo{}, err
	}
	headers := make([]CompactFilterCheckpointEntry, 0, len(rows))
	for _, row := range rows {
		headers = append(headers, CompactFilterCheckpointEntry{Height: row.Height, BlockHash: hex.EncodeToString(row.BlockHash[:]), FilterHeader: hex.EncodeToString(row.Header[:])})
	}
	return CompactFilterCheckpointInfo{FilterType: compactfilter.Type(), Interval: interval, TipHeight: tip, Headers: headers}, nil
}

func (s *Service) compactFilterForHash(blockHash [32]byte) (compactfilter.Filter, error) {
	block, err := s.chainState.Store().GetBlock(&blockHash)
	if err != nil {
		return compactfilter.Filter{}, err
	}
	if block == nil {
		return compactfilter.Filter{}, fmt.Errorf("block %x not found", blockHash)
	}
	undo, err := s.chainState.Store().GetUndo(&blockHash)
	if err != nil {
		return compactfilter.Filter{}, err
	}
	return compactfilter.Build(blockHash, block, compactFilterItemsForUndo(undo)), nil
}

func decodeCompactFilterHash(raw string) ([32]byte, error) {
	var out [32]byte
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return out, err
	}
	if len(buf) != len(out) {
		return out, fmt.Errorf("expected %d-byte hash hex", len(out))
	}
	copy(out[:], buf)
	return out, nil
}
