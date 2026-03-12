package node

import (
	"fmt"
	"log/slog"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

type HeaderReplaySummary struct {
	TipHeight     uint64
	TipHeaderHash [32]byte
}

type HeaderChain struct {
	params      consensus.ChainParams
	height      *uint64
	tipHeader   *types.BlockHeader
	recentTimes []uint64
}

type PersistentHeaderChain struct {
	chain *HeaderChain
	store *storage.ChainStore
}

func NewHeaderChain(profile types.ChainProfile) *HeaderChain {
	return &HeaderChain{
		params: consensus.ParamsForProfile(profile),
	}
}

func HeaderChainFromStoredState(stored *storage.StoredHeaderState) (*HeaderChain, error) {
	chain := NewHeaderChain(stored.Profile)
	if err := chain.InitializeTip(stored.Height, stored.TipHeader); err != nil {
		return nil, err
	}
	return chain, nil
}

func (c *HeaderChain) InitializeTip(height uint64, header types.BlockHeader) error {
	if c.tipHeader != nil {
		return ErrTipAlreadyInitialized
	}
	c.height = &height
	c.tipHeader = &header
	c.recentTimes = []uint64{header.Timestamp}
	return nil
}

func (c *HeaderChain) InitializeFromGenesisHeader(header types.BlockHeader) error {
	if err := c.InitializeTip(0, header); err != nil {
		return err
	}
	logging.Component("headers").Info("initialized header chain from genesis",
		slog.String("profile", c.params.Profile.String()),
		slog.String("hash", fmt.Sprintf("%x", consensus.HeaderHash(&header))),
	)
	return nil
}

func (c *HeaderChain) ApplyHeader(header *types.BlockHeader) error {
	if c.height == nil || c.tipHeader == nil {
		return ErrNoTip
	}
	if err := consensus.ValidateHeader(header, consensus.PrevBlockContext{
		Height:         *c.height,
		Header:         *c.tipHeader,
		MedianTimePast: consensus.MedianTimePast(c.recentTimes),
	}, c.params); err != nil {
		return err
	}
	height := *c.height + 1
	c.height = &height
	tip := *header
	c.tipHeader = &tip
	c.recentTimes = appendRecentTime(c.recentTimes, header.Timestamp)
	logging.Component("headers").Info("validated header",
		slog.Uint64("height", height),
		slog.String("hash", fmt.Sprintf("%x", consensus.HeaderHash(header))),
	)
	return nil
}

func (c *HeaderChain) ReplayHeaders(headers []types.BlockHeader) (HeaderReplaySummary, error) {
	for i := range headers {
		if err := c.ApplyHeader(&headers[i]); err != nil {
			return HeaderReplaySummary{}, err
		}
	}
	if c.height == nil || c.tipHeader == nil {
		return HeaderReplaySummary{}, ErrNoTip
	}
	return HeaderReplaySummary{
		TipHeight:     *c.height,
		TipHeaderHash: consensus.HeaderHash(c.tipHeader),
	}, nil
}

func (c *HeaderChain) TipHeight() *uint64 {
	return c.height
}

func (c *HeaderChain) TipHeader() *types.BlockHeader {
	return c.tipHeader
}

func (c *HeaderChain) Profile() types.ChainProfile {
	return c.params.Profile
}

func (c *HeaderChain) StoredState() (*storage.StoredHeaderState, error) {
	if c.height == nil || c.tipHeader == nil {
		return nil, ErrNoTip
	}
	return &storage.StoredHeaderState{
		Profile:   c.Profile(),
		Height:    *c.height,
		TipHeader: *c.tipHeader,
	}, nil
}

func OpenPersistentHeaderChain(path string, profile types.ChainProfile) (*PersistentHeaderChain, error) {
	logger := logging.Component("headers")
	logger.Info("opening persistent header chain", slog.String("path", path), slog.String("profile", profile.String()))
	store, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	stored, err := store.LoadHeaderState()
	if err != nil {
		store.Close()
		return nil, err
	}
	var chain *HeaderChain
	if stored != nil {
		if stored.Profile != profile {
			store.Close()
			return nil, fmt.Errorf("stored profile mismatch: expected %s, got %s", profile, stored.Profile)
		}
		chain, err = HeaderChainFromStoredState(stored)
		if err != nil {
			store.Close()
			return nil, err
		}
		recentTimes, err := loadIndexedAncestorTimestamps(store, consensus.HeaderHash(&stored.TipHeader), 11)
		if err != nil {
			store.Close()
			return nil, err
		}
		chain.recentTimes = recentTimes
		logger.Info("loaded persisted header chain", slog.Uint64("height", stored.Height))
	} else {
		chain = NewHeaderChain(profile)
		logger.Info("no persisted header chain found", slog.String("path", path))
	}
	return &PersistentHeaderChain{chain: chain, store: store}, nil
}

func (p *PersistentHeaderChain) Close() error {
	if p == nil || p.store == nil {
		return nil
	}
	logging.Component("headers").Info("closing persistent header chain")
	return p.store.Close()
}

func (p *PersistentHeaderChain) InitializeFromGenesisHeader(header types.BlockHeader) error {
	if err := p.chain.InitializeFromGenesisHeader(header); err != nil {
		return err
	}
	stored, err := p.chain.StoredState()
	if err != nil {
		return err
	}
	if err := p.store.WriteHeaderState(stored); err != nil {
		return err
	}
	logging.Component("headers").Info("persisted genesis header",
		slog.String("hash", fmt.Sprintf("%x", consensus.HeaderHash(&header))),
	)
	return p.store.PutHeader(0, &header)
}

func (p *PersistentHeaderChain) ApplyHeader(header *types.BlockHeader) error {
	if err := p.chain.ApplyHeader(header); err != nil {
		return err
	}
	stored, err := p.chain.StoredState()
	if err != nil {
		return err
	}
	if err := p.store.WriteHeaderState(stored); err != nil {
		return err
	}
	logging.Component("headers").Info("persisted header",
		slog.Uint64("height", stored.Height),
		slog.String("hash", fmt.Sprintf("%x", consensus.HeaderHash(header))),
	)
	return p.store.PutHeader(stored.Height, header)
}

func (p *PersistentHeaderChain) ReplayHeaders(headers []types.BlockHeader) (HeaderReplaySummary, error) {
	for i := range headers {
		if err := p.ApplyHeader(&headers[i]); err != nil {
			return HeaderReplaySummary{}, err
		}
	}
	return p.chain.ReplayHeaders(nil)
}

func (p *PersistentHeaderChain) HeaderChain() *HeaderChain {
	return p.chain
}

func (p *PersistentHeaderChain) Store() *storage.ChainStore {
	return p.store
}
