package node

import (
	"encoding/hex"
	"fmt"

	"bitcoin-pure/internal/compactfilter"
)

const (
	maxFilterHeadersPerRPC  = 2000
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
	stopHeight := startHeight + count - 1
	var prevHeader [32]byte
	var previous [32]byte
	headers := make([]CompactFilterHeaderEntry, 0, count)
	for height := uint64(0); ; height++ {
		hash, err := s.chainState.Store().GetBlockHashByHeight(height)
		if err != nil {
			return CompactFilterHeadersInfo{}, err
		}
		if hash == nil {
			return CompactFilterHeadersInfo{}, fmt.Errorf("block height %d not found", height)
		}
		filter, err := s.compactFilterForHash(*hash)
		if err != nil {
			return CompactFilterHeadersInfo{}, err
		}
		header := compactfilter.Header(filter.Hash, prevHeader)
		if height == startHeight {
			previous = prevHeader
		}
		if height >= startHeight {
			headers = append(headers, CompactFilterHeaderEntry{
				Height:       height,
				BlockHash:    hex.EncodeToString(hash[:]),
				FilterHash:   hex.EncodeToString(filter.Hash[:]),
				FilterHeader: hex.EncodeToString(header[:]),
			})
		}
		prevHeader = header
		if height == stopHeight {
			break
		}
	}
	return CompactFilterHeadersInfo{
		FilterType:           compactfilter.Type(),
		StartHeight:          startHeight,
		Count:                count,
		PreviousFilterHeader: hex.EncodeToString(previous[:]),
		Headers:              headers,
	}, nil
}

func (s *Service) CompactFilterCheckpoint(interval uint64) (CompactFilterCheckpointInfo, error) {
	if interval == 0 {
		interval = defaultFilterCheckpoint
	}
	view, ok := s.chainState.sharedCommittedView()
	if !ok {
		return CompactFilterCheckpointInfo{FilterType: compactfilter.Type(), Interval: interval}, nil
	}
	// Build the header chain once and emit only the requested checkpoints.
	headers := make([]CompactFilterCheckpointEntry, 0)
	var previous [32]byte
	for height := uint64(0); ; height++ {
		hash, err := s.chainState.Store().GetBlockHashByHeight(height)
		if err != nil {
			return CompactFilterCheckpointInfo{}, err
		}
		if hash == nil {
			return CompactFilterCheckpointInfo{}, fmt.Errorf("block height %d not found", height)
		}
		filter, err := s.compactFilterForHash(*hash)
		if err != nil {
			return CompactFilterCheckpointInfo{}, err
		}
		header := compactfilter.Header(filter.Hash, previous)
		if height%interval == 0 || height == view.Height {
			headers = append(headers, CompactFilterCheckpointEntry{
				Height:       height,
				BlockHash:    hex.EncodeToString(hash[:]),
				FilterHeader: hex.EncodeToString(header[:]),
			})
		}
		previous = header
		if height == view.Height {
			break
		}
	}
	return CompactFilterCheckpointInfo{
		FilterType: compactfilter.Type(),
		Interval:   interval,
		TipHeight:  view.Height,
		Headers:    headers,
	}, nil
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
