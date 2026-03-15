package node

import (
	"fmt"
	"log/slog"
	"time"

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
	logger      *slog.Logger
	height      *uint64
	tipHeader   *types.BlockHeader
	recentTimes []uint64
	skipPow     bool
}

type PersistentHeaderChain struct {
	chain  *HeaderChain
	store  *storage.ChainStore
	logger *slog.Logger
}

func NewHeaderChain(profile types.ChainProfile) *HeaderChain {
	return NewHeaderChainWithLogger(profile, logging.Component("headers"))
}

func NewHeaderChainWithLogger(profile types.ChainProfile, logger *slog.Logger) *HeaderChain {
	if logger == nil {
		logger = logging.Component("headers")
	}
	return &HeaderChain{
		params: consensus.ParamsForProfile(profile),
		logger: logger,
	}
}

func HeaderChainFromStoredState(stored *storage.StoredHeaderState) (*HeaderChain, error) {
	chain := NewHeaderChain(stored.Profile)
	if err := chain.InitializeTip(stored.Height, stored.TipHeader); err != nil {
		return nil, err
	}
	return chain, nil
}

func (c *HeaderChain) WithLogger(logger *slog.Logger) *HeaderChain {
	if logger != nil {
		c.logger = logger
	}
	return c
}

func (c *HeaderChain) SetSkipPow(skip bool) {
	c.skipPow = skip
}

func (c *HeaderChain) SkipPow() bool {
	return c.skipPow
}

func (c *HeaderChain) InitializeTip(height uint64, header types.BlockHeader) error {
	if c.tipHeader != nil {
		return ErrTipAlreadyInitialized
	}
	c.height = &height
	c.tipHeader = &header
	c.recentTimes = make([]uint64, 1, recentTimeWindow)
	c.recentTimes[0] = header.Timestamp
	return nil
}

func (c *HeaderChain) InitializeFromGenesisHeader(header types.BlockHeader) error {
	if err := c.InitializeTip(0, header); err != nil {
		return err
	}
	c.logger.Info("initialized header chain from genesis",
		slog.String("profile", c.params.Profile.String()),
		slog.String("hash", fmt.Sprintf("%x", consensus.HeaderHash(&header))),
	)
	return nil
}

func (c *HeaderChain) ApplyHeader(header *types.BlockHeader) error {
	if c.height == nil || c.tipHeader == nil {
		return ErrNoTip
	}
	rules := consensus.DefaultConsensusRules()
	rules.SkipPow = c.skipPow
	if err := consensus.ValidateHeaderWithRules(header, consensus.PrevBlockContext{
		Height:         *c.height,
		Header:         *c.tipHeader,
		MedianTimePast: consensus.MedianTimePast(c.recentTimes),
		CurrentTime:    uint64(time.Now().Unix()),
	}, c.params, rules); err != nil {
		return err
	}
	height := *c.height + 1
	c.height = &height
	tip := *header
	c.tipHeader = &tip
	c.recentTimes = appendRecentTime(c.recentTimes, header.Timestamp)
	c.logger.Info("validated header",
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
	return OpenPersistentHeaderChainWithLogger(path, profile, nil)
}

func OpenPersistentHeaderChainWithLogger(path string, profile types.ChainProfile, logger *slog.Logger) (*PersistentHeaderChain, error) {
	if logger == nil {
		logger = logging.Component("headers")
	}
	headerLogger := logging.ComponentWith(logger, "headers")
	headerLogger.Info("opening persistent header chain", slog.String("path", path), slog.String("profile", profile.String()))
	store, err := storage.OpenWithLogger(path, logging.ComponentWith(logger, "storage"))
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
		chain.WithLogger(headerLogger)
		recentTimes, err := loadIndexedAncestorTimestamps(store, consensus.HeaderHash(&stored.TipHeader), 11)
		if err != nil {
			store.Close()
			return nil, err
		}
		chain.recentTimes = recentTimes
		headerLogger.Info("loaded persisted header chain", slog.Uint64("height", stored.Height))
	} else {
		chain = NewHeaderChainWithLogger(profile, headerLogger)
		headerLogger.Info("no persisted header chain found", slog.String("path", path))
	}
	return &PersistentHeaderChain{chain: chain, store: store, logger: headerLogger}, nil
}

func (p *PersistentHeaderChain) Close() error {
	if p == nil || p.store == nil {
		return nil
	}
	p.logger.Info("closing persistent header chain")
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
	p.logger.Info("persisted genesis header",
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
	p.logger.Info("persisted header",
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
