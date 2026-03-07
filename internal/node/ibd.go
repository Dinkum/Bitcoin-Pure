package node

import (
	"errors"
	"fmt"
	"log/slog"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/types"
)

type HeadersFirstIBDSummary struct {
	HeaderTipHeight uint64
	BlockTipHeight  uint64
	TipHeaderHash   [32]byte
	UTXORoot        [32]byte
	UTXOCount       int
	BlockSizeLimit  uint64
}

func ReplayBlocksHeadersFirst(profile types.ChainProfile, genesis *types.Block, blocks []types.Block) (HeadersFirstIBDSummary, error) {
	logger := logging.Component("ibd")
	logger.Info("starting in-memory headers-first replay",
		slog.String("profile", profile.String()),
		slog.Int("blocks", len(blocks)),
	)
	headerChain := NewHeaderChain(profile)
	if err := headerChain.InitializeFromGenesisHeader(genesis.Header); err != nil {
		return HeadersFirstIBDSummary{}, err
	}
	headers := make([]types.BlockHeader, 0, len(blocks))
	for i := range blocks {
		headers = append(headers, blocks[i].Header)
	}
	headerSummary, err := headerChain.ReplayHeaders(headers)
	if err != nil {
		return HeadersFirstIBDSummary{}, err
	}

	chainState := NewChainState(profile)
	if _, err := chainState.InitializeFromGenesisBlock(genesis); err != nil {
		return HeadersFirstIBDSummary{}, err
	}
	blockSummary, err := chainState.ReplayBlocks(blocks)
	if err != nil {
		return HeadersFirstIBDSummary{}, err
	}

	summary := HeadersFirstIBDSummary{
		HeaderTipHeight: headerSummary.TipHeight,
		BlockTipHeight:  blockSummary.TipHeight,
		TipHeaderHash:   blockSummary.TipHeaderHash,
		UTXORoot:        blockSummary.UTXORoot,
		UTXOCount:       blockSummary.UTXOCount,
		BlockSizeLimit:  blockSummary.BlockSizeLimit,
	}
	logger.Info("completed in-memory headers-first replay",
		slog.Uint64("header_tip_height", summary.HeaderTipHeight),
		slog.Uint64("block_tip_height", summary.BlockTipHeight),
		slog.Int("utxo_count", summary.UTXOCount),
	)
	return summary, nil
}

func ReplayBlocksHeadersFirstPersistent(path string, profile types.ChainProfile, genesis *types.Block, blocks []types.Block) (HeadersFirstIBDSummary, error) {
	logger := logging.Component("ibd")
	logger.Info("starting persistent headers-first replay",
		slog.String("path", path),
		slog.String("profile", profile.String()),
		slog.Int("blocks", len(blocks)),
	)
	headerChain, err := OpenPersistentHeaderChain(path, profile)
	if err != nil {
		return HeadersFirstIBDSummary{}, err
	}
	if headerChain.HeaderChain().TipHeight() == nil {
		if err := headerChain.InitializeFromGenesisHeader(genesis.Header); err != nil {
			headerChain.Close()
			return HeadersFirstIBDSummary{}, err
		}
	}
	headers := make([]types.BlockHeader, 0, len(blocks))
	for i := range blocks {
		headers = append(headers, blocks[i].Header)
	}
	headerSummary, err := headerChain.ReplayHeaders(headers)
	if err != nil {
		headerChain.Close()
		return HeadersFirstIBDSummary{}, err
	}
	if err := headerChain.Close(); err != nil {
		return HeadersFirstIBDSummary{}, err
	}

	chainState, err := OpenPersistentChainState(path, profile)
	if err != nil {
		return HeadersFirstIBDSummary{}, err
	}
	defer chainState.Close()
	if chainState.ChainState().TipHeight() == nil {
		if _, err := chainState.InitializeFromGenesisBlock(genesis); err != nil {
			return HeadersFirstIBDSummary{}, err
		}
	}

	for i := range blocks {
		hash := consensus.HeaderHash(&blocks[i].Header)
		entry, err := chainState.Store().GetBlockIndex(&hash)
		if err != nil {
			return HeadersFirstIBDSummary{}, err
		}
		if entry == nil {
			return HeadersFirstIBDSummary{}, fmt.Errorf("missing indexed header for block %x", hash)
		}
		expectedHeight := *chainState.ChainState().TipHeight() + 1
		if entry.Height != expectedHeight {
			return HeadersFirstIBDSummary{}, fmt.Errorf("unexpected indexed header height: expected %d, got %d", expectedHeight, entry.Height)
		}
		if entry.Header != blocks[i].Header {
			return HeadersFirstIBDSummary{}, errors.New("indexed header mismatch")
		}
		if _, err := chainState.ApplyBlock(&blocks[i]); err != nil {
			return HeadersFirstIBDSummary{}, err
		}
	}

	tipHeight := chainState.ChainState().TipHeight()
	tipHeader := chainState.ChainState().TipHeader()
	if tipHeight == nil || tipHeader == nil {
		return HeadersFirstIBDSummary{}, ErrNoTip
	}

	summary := HeadersFirstIBDSummary{
		HeaderTipHeight: headerSummary.TipHeight,
		BlockTipHeight:  *tipHeight,
		TipHeaderHash:   consensus.HeaderHash(tipHeader),
		UTXORoot:        chainState.ChainState().UTXORoot(),
		UTXOCount:       len(chainState.ChainState().UTXOs()),
		BlockSizeLimit:  consensus.NextBlockSizeLimit(chainState.ChainState().BlockSizeState(), consensus.ParamsForProfile(profile)),
	}
	logger.Info("completed persistent headers-first replay",
		slog.Uint64("header_tip_height", summary.HeaderTipHeight),
		slog.Uint64("block_tip_height", summary.BlockTipHeight),
		slog.Int("utxo_count", summary.UTXOCount),
	)
	return summary, nil
}
