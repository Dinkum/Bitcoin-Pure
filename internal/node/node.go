package node

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"bitcoin-pure/internal/utxochecksum"
)

var (
	ErrNoTip                 = errors.New("chain tip is not initialized")
	ErrTipAlreadyInitialized = errors.New("tip already initialized")
)

type GenesisBootstrapSummary struct {
	Height              uint64
	HeaderHash          [32]byte
	CoinbaseTxID        [32]byte
	PostGenesisUTXORoot [32]byte
	UTXOChecksum        [32]byte
	UTXOCount           int
	BlockSizeLimit      uint64
}

type ChainReplaySummary struct {
	TipHeight      uint64
	TipHeaderHash  [32]byte
	UTXORoot       [32]byte
	UTXOChecksum   [32]byte
	UTXOCount      int
	BlockSizeLimit uint64
}

type CommittedChainView struct {
	Height         uint64
	TipHeader      types.BlockHeader
	TipHash        [32]byte
	BlockSizeState consensus.BlockSizeState
	UTXOCount      int
	UTXOAcc        *utreexo.Accumulator
	UTXORoot       [32]byte
	UTXOChecksum   [32]byte
}

// sharedCommittedChainView is an internal-only snapshot for hot paths such as
// block template assembly. It carries only immutable tip metadata plus the
// maintained accumulator/count scalars; callers that need actual UTXO reads
// fetch through the store-backed lookup helpers instead of borrowing the
// compatibility map directly.
type sharedCommittedChainView struct {
	Height         uint64
	TipHeader      types.BlockHeader
	TipHash        [32]byte
	BlockSizeState consensus.BlockSizeState
	UTXOCount      int
	UTXOAcc        *utreexo.Accumulator
	UTXORoot       [32]byte
	UTXOChecksum   [32]byte
}

type chainTipSnapshot struct {
	Height  uint64
	TipHash [32]byte
}

type ChainState struct {
	params         consensus.ChainParams
	rules          consensus.ConsensusRules
	logger         *slog.Logger
	height         *uint64
	tipHeader      *types.BlockHeader
	recentTimes    []uint64
	blockSizeState consensus.BlockSizeState
	utxos          consensus.UtxoSet
	utxoLookup     consensus.UtxoLookupWithErr
	utxoScan       func(func(types.OutPoint, consensus.UtxoEntry) error) error
	utxoCount      int
	utxoAcc        *utreexo.Accumulator
	utxoChecksum   [32]byte
}

type PersistentChainState struct {
	mu     sync.RWMutex
	logger *slog.Logger
	state  *ChainState
	store  *storage.ChainStore
}

type appliedBlockDetail struct {
	summary     consensus.BlockValidationSummary
	overlay     *consensus.UtxoOverlay
	createdUTXO map[types.OutPoint]consensus.UtxoEntry
}

func NewChainState(profile types.ChainProfile) *ChainState {
	params := consensus.ParamsForProfile(profile)
	return &ChainState{
		params:         params,
		rules:          consensus.DefaultConsensusRules(),
		logger:         logging.Component("chain"),
		blockSizeState: consensus.NewBlockSizeState(params),
		utxos:          make(consensus.UtxoSet),
		utxoLookup:     consensus.LookupWithErrFromSet(consensus.UtxoSet{}),
	}
}

func (c *ChainState) WithRules(rules consensus.ConsensusRules) *ChainState {
	c.rules = rules
	return c
}

func (c *ChainState) WithLogger(logger *slog.Logger) *ChainState {
	if logger != nil {
		c.logger = logger
	}
	return c
}

func ChainStateFromStoredState(stored *storage.StoredChainState) (*ChainState, error) {
	state := NewChainState(stored.Profile)
	if err := state.InitializeTip(stored.Height, stored.TipHeader, stored.BlockSizeState, stored.UTXOs); err != nil {
		return nil, err
	}
	if stored.UTXOChecksum != ([32]byte{}) {
		state.utxoChecksum = stored.UTXOChecksum
	}
	return state, nil
}

func chainStateFromStoredMeta(stored *storage.StoredChainStateMeta, utxos consensus.UtxoSet, lookup consensus.UtxoLookupWithErr, scan func(func(types.OutPoint, consensus.UtxoEntry) error) error, utxoCount int, acc *utreexo.Accumulator) (*ChainState, error) {
	if stored == nil {
		return nil, nil
	}
	if acc == nil && utxos != nil {
		var err error
		acc, err = consensus.UtxoAccumulator(utxos)
		if err != nil {
			return nil, err
		}
	}
	state := NewChainState(stored.Profile)
	height := stored.Height
	tipHeader := stored.TipHeader
	state.height = &height
	state.tipHeader = &tipHeader
	state.recentTimes = make([]uint64, 1, recentTimeWindow)
	state.recentTimes[0] = stored.TipHeader.Timestamp
	state.blockSizeState = stored.BlockSizeState
	if utxos != nil {
		state.replaceMaterializedUTXOs(cloneUtxos(utxos))
	} else {
		state.utxos = nil
		state.utxoCount = utxoCount
	}
	if lookup != nil {
		state.utxoLookup = lookup
	}
	state.utxoScan = scan
	state.utxoAcc = acc
	if stored.UTXOChecksum != ([32]byte{}) {
		state.utxoChecksum = stored.UTXOChecksum
	} else if state.utxos != nil {
		state.utxoChecksum = utxochecksum.Compute(state.utxos)
	}
	return state, nil
}

func buildAccumulatorFromStore(store *storage.ChainStore) (*utreexo.Accumulator, int, error) {
	leaves := make([]utreexo.UtxoLeaf, 0)
	utxoCount := 0
	if err := store.ForEachUTXO(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		leaves = append(leaves, utreexo.UtxoLeaf{
			OutPoint:   outPoint,
			ValueAtoms: entry.ValueAtoms,
			PubKey:     entry.PubKey,
		})
		utxoCount++
		return nil
	}); err != nil {
		return nil, 0, err
	}
	acc, err := utreexo.NewAccumulatorFromLeaves(leaves)
	if err != nil {
		return nil, 0, err
	}
	return acc, utxoCount, nil
}

func (c *ChainState) InitializeTip(height uint64, header types.BlockHeader, blockSizeState consensus.BlockSizeState, utxos consensus.UtxoSet) error {
	if c.tipHeader != nil {
		return ErrTipAlreadyInitialized
	}
	c.height = &height
	c.tipHeader = &header
	c.recentTimes = make([]uint64, 1, recentTimeWindow)
	c.recentTimes[0] = header.Timestamp
	c.blockSizeState = blockSizeState
	c.replaceMaterializedUTXOs(cloneUtxos(utxos))
	acc, err := consensus.UtxoAccumulator(c.utxos)
	if err != nil {
		return err
	}
	c.utxoAcc = acc
	c.utxoChecksum = utxochecksum.Compute(c.utxos)
	return nil
}

func (c *ChainState) InitializeFromGenesisBlock(genesis *types.Block) (GenesisBootstrapSummary, error) {
	if len(genesis.Txs) == 0 {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: empty block")
	}
	if len(genesis.Txs) != 1 {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: bootstrap path currently requires coinbase-only genesis")
	}
	coinbase := &genesis.Txs[0]
	if !coinbase.IsCoinbase() {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: first transaction must be coinbase")
	}
	if len(coinbase.Auth.Entries) != 0 {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: coinbase must not have auth")
	}
	if coinbase.Base.CoinbaseHeight == nil || *coinbase.Base.CoinbaseHeight != 0 {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: coinbase height must be 0")
	}
	if coinbase.Base.CoinbaseExtraNonce == nil {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: coinbase extra nonce must be present")
	}
	if len(coinbase.Base.Outputs) == 0 {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: coinbase has no outputs")
	}

	txids := [][32]byte{consensus.TxID(coinbase)}
	if consensus.MerkleRoot(txids) != genesis.Header.MerkleTxIDRoot {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: merkle_txid_root mismatch")
	}
	authids := [][32]byte{consensus.AuthID(coinbase)}
	if consensus.MerkleRoot(authids) != genesis.Header.MerkleAuthRoot {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: merkle_auth_root mismatch")
	}

	coinbaseTxID := txids[0]
	utxos := make(consensus.UtxoSet, len(coinbase.Base.Outputs))
	for vout, output := range coinbase.Base.Outputs {
		utxos[types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}] = consensus.UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			PubKey:     output.PubKey,
		}
	}
	seededBlockSizeState := consensus.NewBlockSizeState(c.params)
	seededBlockSizeState.BlockSize = uint64(len(genesis.Encode()))
	acc, err := consensus.UtxoAccumulator(utxos)
	if err != nil {
		return GenesisBootstrapSummary{}, err
	}
	postGenesisUTXORoot := acc.Root()
	if genesis.Header.UTXORoot != postGenesisUTXORoot {
		return GenesisBootstrapSummary{}, errors.New("invalid genesis block: utxo_root mismatch")
	}
	if err := c.InitializeTip(0, genesis.Header, seededBlockSizeState, utxos); err != nil {
		return GenesisBootstrapSummary{}, err
	}
	summary := GenesisBootstrapSummary{
		Height:              0,
		HeaderHash:          consensus.HeaderHash(&genesis.Header),
		CoinbaseTxID:        coinbaseTxID,
		PostGenesisUTXORoot: postGenesisUTXORoot,
		UTXOChecksum:        utxochecksum.Compute(utxos),
		UTXOCount:           c.utxoCount,
		BlockSizeLimit:      consensus.NextBlockSizeLimit(c.blockSizeState, c.params),
	}
	c.utxoChecksum = summary.UTXOChecksum
	c.logger.Info("initialized chain from genesis",
		slog.String("profile", c.params.Profile.String()),
		slog.String("header_hash", fmt.Sprintf("%x", summary.HeaderHash)),
		slog.String("coinbase_txid", fmt.Sprintf("%x", summary.CoinbaseTxID)),
		slog.String("utxo_checksum", fmt.Sprintf("%x", summary.UTXOChecksum)),
		slog.Int("utxo_count", summary.UTXOCount),
		slog.Uint64("block_size_limit", summary.BlockSizeLimit),
	)
	return summary, nil
}

func (c *ChainState) ApplyBlock(block *types.Block) (consensus.BlockValidationSummary, error) {
	detail, err := c.applyBlockDetailed(block)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	hash := consensus.HeaderHash(&block.Header)
	c.logger.Info("validated block",
		slog.Uint64("height", detail.summary.Height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
		slog.Int("txs", len(block.Txs)),
		slog.Int("utxo_count", c.utxoCount),
		slog.Uint64("next_block_size_limit", consensus.NextBlockSizeLimit(detail.summary.NextBlockSizeState, c.params)),
	)
	return detail.summary, nil
}

func (c *ChainState) applyBlockDetailed(block *types.Block) (appliedBlockDetail, error) {
	if c.height == nil || c.tipHeader == nil {
		return appliedBlockDetail{}, ErrNoTip
	}
	// The published chain UTXO map is immutable. Validation runs against that
	// shared base view and only swaps in a freshly materialized post-block map
	// once the block is fully validated.
	summary, overlay, nextAcc, err := consensus.ValidateAndApplyBlockOverlayWithLookup(
		block,
		c.prevBlockContext(),
		c.blockSizeState,
		c.utxos,
		c.utxoLookup,
		c.utxoAcc,
		c.params,
		c.rules,
	)
	if err != nil {
		return appliedBlockDetail{}, err
	}
	createdUTXO := overlay.CreatedEntriesClone()
	spentUTXO, err := blockSpentCommittedUTXOs(c.utxoLookup, block)
	if err != nil {
		return appliedBlockDetail{}, err
	}
	height := summary.Height
	c.height = &height
	tip := block.Header
	c.tipHeader = &tip
	c.recentTimes = appendRecentTime(c.recentTimes, block.Header.Timestamp)
	c.blockSizeState = summary.NextBlockSizeState
	if c.utxos != nil {
		c.replaceMaterializedUTXOs(overlay.Materialize())
	} else {
		c.utxoCount += len(createdUTXO) - len(spentUTXO)
	}
	c.utxoAcc = nextAcc
	c.utxoChecksum = utxochecksum.ApplyDelta(c.utxoChecksum, spentUTXO, createdUTXO)
	return appliedBlockDetail{summary: summary, overlay: overlay, createdUTXO: createdUTXO}, nil
}

func (c *ChainState) ReplayBlocks(blocks []types.Block) (ChainReplaySummary, error) {
	for i := range blocks {
		if _, err := c.ApplyBlock(&blocks[i]); err != nil {
			return ChainReplaySummary{}, err
		}
	}
	if c.height == nil || c.tipHeader == nil {
		return ChainReplaySummary{}, ErrNoTip
	}
	return ChainReplaySummary{
		TipHeight:      *c.height,
		TipHeaderHash:  consensus.HeaderHash(c.tipHeader),
		UTXORoot:       c.UTXORoot(),
		UTXOChecksum:   c.UTXOChecksum(),
		UTXOCount:      c.utxoCount,
		BlockSizeLimit: consensus.NextBlockSizeLimit(c.blockSizeState, c.params),
	}, nil
}

func (c *ChainState) TipHeight() *uint64 {
	return cloneUint64Ptr(c.height)
}

func (c *ChainState) Profile() types.ChainProfile {
	return c.params.Profile
}

func (c *ChainState) TipHeader() *types.BlockHeader {
	return cloneBlockHeaderPtr(c.tipHeader)
}

func (c *ChainState) BlockSizeState() consensus.BlockSizeState {
	return c.blockSizeState
}

func (c *ChainState) UTXOs() consensus.UtxoSet {
	if c.utxos == nil {
		utxos, err := c.materializeUTXOs()
		if err != nil {
			c.logger.Warn("materialize committed utxos failed", slog.Any("error", err))
			return nil
		}
		return utxos
	}
	return cloneUtxos(c.utxos)
}

func (c *ChainState) ForEachUTXO(fn func(types.OutPoint, consensus.UtxoEntry) error) error {
	if fn == nil {
		return nil
	}
	if c.utxos != nil {
		for outPoint, entry := range c.utxos {
			if err := fn(outPoint, entry); err != nil {
				return err
			}
		}
		return nil
	}
	if c.utxoScan == nil {
		return fmt.Errorf("utxo iteration unavailable")
	}
	return c.utxoScan(fn)
}

func (c *ChainState) UTXOCount() int {
	return c.utxoCount
}

func (c *ChainState) UTXORoot() [32]byte {
	if c.utxoAcc == nil {
		return consensus.ComputedUTXORoot(c.UTXOs())
	}
	return c.utxoAcc.Root()
}

func (c *ChainState) UTXOChecksum() [32]byte {
	if c.utxoChecksum == ([32]byte{}) {
		return utxochecksum.Compute(c.UTXOs())
	}
	return c.utxoChecksum
}

func (c *ChainState) CommittedView() (CommittedChainView, bool) {
	if c.height == nil || c.tipHeader == nil {
		return CommittedChainView{}, false
	}
	view := CommittedChainView{
		Height:         *c.height,
		TipHeader:      *c.tipHeader,
		TipHash:        consensus.HeaderHash(c.tipHeader),
		BlockSizeState: c.blockSizeState,
		UTXOCount:      c.utxoCount,
		UTXOAcc:        c.utxoAcc.Clone(),
		UTXORoot:       c.UTXORoot(),
		UTXOChecksum:   c.UTXOChecksum(),
	}
	return view, true
}

func (c *ChainState) sharedCommittedView() (sharedCommittedChainView, bool) {
	if c.height == nil || c.tipHeader == nil {
		return sharedCommittedChainView{}, false
	}
	return sharedCommittedChainView{
		Height:         *c.height,
		TipHeader:      *c.tipHeader,
		TipHash:        consensus.HeaderHash(c.tipHeader),
		BlockSizeState: c.blockSizeState,
		UTXOCount:      c.utxoCount,
		UTXOAcc:        c.utxoAcc,
		UTXORoot:       c.UTXORoot(),
		UTXOChecksum:   c.UTXOChecksum(),
	}, true
}

func (c *ChainState) tipSnapshot() (chainTipSnapshot, bool) {
	if c.height == nil || c.tipHeader == nil {
		return chainTipSnapshot{}, false
	}
	return chainTipSnapshot{
		Height:  *c.height,
		TipHash: consensus.HeaderHash(c.tipHeader),
	}, true
}

func (c *ChainState) UTXOAccumulator() *utreexo.Accumulator {
	return c.utxoAcc.Clone()
}

func (c *ChainState) StoredState() (*storage.StoredChainState, error) {
	if c.height == nil || c.tipHeader == nil {
		return nil, ErrNoTip
	}
	utxos, err := c.materializeUTXOs()
	if err != nil {
		return nil, err
	}
	return &storage.StoredChainState{
		Profile:        c.Profile(),
		Height:         *c.height,
		TipHeader:      *c.tipHeader,
		BlockSizeState: c.blockSizeState,
		UTXOChecksum:   c.UTXOChecksum(),
		UTXOs:          utxos,
	}, nil
}

func (c *ChainState) StoredStateMeta() (*storage.StoredChainState, error) {
	if c.height == nil || c.tipHeader == nil {
		return nil, ErrNoTip
	}
	return &storage.StoredChainState{
		Profile:        c.Profile(),
		Height:         *c.height,
		TipHeader:      *c.tipHeader,
		BlockSizeState: c.blockSizeState,
		UTXOChecksum:   c.UTXOChecksum(),
	}, nil
}

func blockSpentCommittedUTXOs(lookup consensus.UtxoLookupWithErr, block *types.Block) (map[types.OutPoint]consensus.UtxoEntry, error) {
	spent := make(map[types.OutPoint]consensus.UtxoEntry)
	for i := 1; i < len(block.Txs); i++ {
		for _, input := range block.Txs[i].Base.Inputs {
			entry, ok, err := lookup(input.PrevOut)
			if err != nil {
				return nil, fmt.Errorf("spent utxo lookup failed: %w", err)
			}
			if !ok {
				continue
			}
			spent[input.PrevOut] = entry
		}
	}
	return spent, nil
}

func OpenPersistentChainState(path string, profile types.ChainProfile) (*PersistentChainState, error) {
	return OpenPersistentChainStateWithRulesAndLogger(path, profile, consensus.DefaultConsensusRules(), slog.Default())
}

func OpenPersistentChainStateFromMeta(path string, profile types.ChainProfile) (*PersistentChainState, error) {
	return OpenPersistentChainStateFromMetaWithRulesAndLogger(path, profile, consensus.DefaultConsensusRules(), slog.Default())
}

func OpenPersistentChainStateWithRules(path string, profile types.ChainProfile, rules consensus.ConsensusRules) (*PersistentChainState, error) {
	return OpenPersistentChainStateWithRulesAndLogger(path, profile, rules, slog.Default())
}

func OpenPersistentChainStateFromMetaWithRules(path string, profile types.ChainProfile, rules consensus.ConsensusRules) (*PersistentChainState, error) {
	return OpenPersistentChainStateFromMetaWithRulesAndLogger(path, profile, rules, slog.Default())
}

func OpenPersistentChainStateWithRulesAndLogger(path string, profile types.ChainProfile, rules consensus.ConsensusRules, logger *slog.Logger) (*PersistentChainState, error) {
	return openPersistentChainStateFromMeta(path, profile, rules, logger, storage.OpenOptions{})
}

func OpenPersistentChainStateFromMetaWithRulesAndLogger(path string, profile types.ChainProfile, rules consensus.ConsensusRules, logger *slog.Logger) (*PersistentChainState, error) {
	return openPersistentChainStateFromMeta(path, profile, rules, logger, storage.OpenOptions{})
}

func openPersistentChainState(path string, profile types.ChainProfile, rules consensus.ConsensusRules, logger *slog.Logger, storeOptions storage.OpenOptions) (*PersistentChainState, error) {
	if logger == nil {
		logger = slog.Default()
	}
	chainLogger := logging.ComponentWith(logger, "chain")
	chainLogger.Info("opening persistent chain state", slog.String("path", path), slog.String("profile", profile.String()))
	store, err := storage.OpenWithLoggerAndOptions(path, logging.ComponentWith(logger, "storage"), storeOptions)
	if err != nil {
		return nil, err
	}
	stored, err := store.LoadChainState()
	if err != nil {
		store.Close()
		return nil, err
	}
	var state *ChainState
	if stored != nil {
		if stored.Profile != profile {
			store.Close()
			return nil, fmt.Errorf("stored profile mismatch: expected %s, got %s", profile, stored.Profile)
		}
		state, err = ChainStateFromStoredState(stored)
		if err != nil {
			store.Close()
			return nil, err
		}
		state.WithLogger(chainLogger)
		recentTimes, err := loadIndexedAncestorTimestamps(store, consensus.HeaderHash(&stored.TipHeader), 11)
		if err != nil {
			store.Close()
			return nil, err
		}
		state.recentTimes = recentTimes
		state.WithRules(rules)
		chainLogger.Info("loaded persisted chain state",
			slog.Uint64("height", stored.Height),
			slog.Int("utxo_count", len(stored.UTXOs)),
			slog.Uint64("block_size_limit", consensus.NextBlockSizeLimit(stored.BlockSizeState, state.params)),
		)
	} else {
		state = NewChainState(profile).WithLogger(chainLogger).WithRules(rules)
		chainLogger.Info("no persisted chain state found", slog.String("path", path))
	}
	return &PersistentChainState{logger: chainLogger, state: state, store: store}, nil
}

func openPersistentChainStateFromMeta(path string, profile types.ChainProfile, rules consensus.ConsensusRules, logger *slog.Logger, storeOptions storage.OpenOptions) (*PersistentChainState, error) {
	if logger == nil {
		logger = slog.Default()
	}
	chainLogger := logging.ComponentWith(logger, "chain")
	chainLogger.Info("opening persistent chain state from metadata", slog.String("path", path), slog.String("profile", profile.String()))
	store, err := storage.OpenWithLoggerAndOptions(path, logging.ComponentWith(logger, "storage"), storeOptions)
	if err != nil {
		return nil, err
	}
	stored, err := store.LoadChainStateMeta()
	if err != nil {
		store.Close()
		return nil, err
	}
	var state *ChainState
	if stored != nil {
		if stored.Profile != profile {
			store.Close()
			return nil, fmt.Errorf("stored profile mismatch: expected %s, got %s", profile, stored.Profile)
		}
		acc, utxoCount, err := buildAccumulatorFromStore(store)
		if err != nil {
			store.Close()
			return nil, err
		}
		state, err = chainStateFromStoredMeta(stored, nil, store.UTXOLookupWithErr(), store.ForEachUTXO, utxoCount, acc)
		if err != nil {
			store.Close()
			return nil, err
		}
		state.WithLogger(chainLogger)
		recentTimes, err := loadIndexedAncestorTimestamps(store, consensus.HeaderHash(&stored.TipHeader), 11)
		if err != nil {
			store.Close()
			return nil, err
		}
		state.recentTimes = recentTimes
		state.WithRules(rules)
		chainLogger.Info("loaded persisted chain state from metadata",
			slog.Uint64("height", stored.Height),
			slog.Int("utxo_count", utxoCount),
			slog.Uint64("block_size_limit", consensus.NextBlockSizeLimit(stored.BlockSizeState, state.params)),
		)
	} else {
		state = NewChainState(profile).WithLogger(chainLogger).WithRules(rules)
		chainLogger.Info("no persisted chain state found", slog.String("path", path))
	}
	return &PersistentChainState{logger: chainLogger, state: state, store: store}, nil
}

func (p *PersistentChainState) Close() error {
	if p == nil || p.store == nil {
		return nil
	}
	p.logger.Info("closing persistent chain state")
	return p.store.Close()
}

func (p *PersistentChainState) InitializeFromGenesisBlock(genesis *types.Block) (GenesisBootstrapSummary, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	summary, err := p.state.InitializeFromGenesisBlock(genesis)
	if err != nil {
		return GenesisBootstrapSummary{}, err
	}
	stored, err := p.state.StoredStateMeta()
	if err != nil {
		return GenesisBootstrapSummary{}, err
	}
	stored.UTXOs = p.state.UTXOs()
	if err := p.store.WriteFullState(stored); err != nil {
		return GenesisBootstrapSummary{}, err
	}
	work, err := consensus.BlockWork(genesis.Header.NBits)
	if err != nil {
		return GenesisBootstrapSummary{}, err
	}
	entry := &storage.BlockIndexEntry{
		Height:         0,
		ParentHash:     genesis.Header.PrevBlockHash,
		Header:         genesis.Header,
		ChainWork:      work,
		Validated:      true,
		BlockSizeState: stored.BlockSizeState,
	}
	if err := p.store.PutValidatedBlock(genesis, entry, nil); err != nil {
		return GenesisBootstrapSummary{}, err
	}
	if err := p.store.RewriteActiveHeights(0, 0, []storage.BlockIndexEntry{*entry}); err != nil {
		return GenesisBootstrapSummary{}, err
	}
	p.logger.Info("persisted genesis block",
		slog.String("hash", fmt.Sprintf("%x", summary.HeaderHash)),
		slog.Int("utxo_count", summary.UTXOCount),
	)
	return summary, nil
}

func (p *PersistentChainState) ReplayBlocks(blocks []types.Block) (ChainReplaySummary, error) {
	for i := range blocks {
		if _, err := p.ApplyBlock(&blocks[i]); err != nil {
			return ChainReplaySummary{}, err
		}
	}
	return p.state.ReplayBlocks(nil)
}

func (p *PersistentChainState) ChainState() *ChainState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.state == nil {
		return nil
	}
	return p.state.Clone()
}

func (p *PersistentChainState) CommittedView() (CommittedChainView, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p == nil || p.state == nil {
		return CommittedChainView{}, false
	}
	return p.state.CommittedView()
}

func (p *PersistentChainState) sharedCommittedView() (sharedCommittedChainView, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p == nil || p.state == nil {
		return sharedCommittedChainView{}, false
	}
	return p.state.sharedCommittedView()
}

func (p *PersistentChainState) tipSnapshot() (chainTipSnapshot, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p == nil || p.state == nil {
		return chainTipSnapshot{}, false
	}
	return p.state.tipSnapshot()
}

func (p *PersistentChainState) Store() *storage.ChainStore {
	return p.store
}

func spentOutPoints(block types.Block) []types.OutPoint {
	spent := make([]types.OutPoint, 0)
	for _, tx := range block.Txs[1:] {
		for _, input := range tx.Base.Inputs {
			spent = append(spent, input.PrevOut)
		}
	}
	return spent
}

func createdUTXOs(block types.Block) map[types.OutPoint]consensus.UtxoEntry {
	created := make(map[types.OutPoint]consensus.UtxoEntry)
	for _, tx := range block.Txs {
		txHash := consensus.TxID(&tx)
		for vout, output := range tx.Base.Outputs {
			created[types.OutPoint{TxID: txHash, Vout: uint32(vout)}] = consensus.UtxoEntry{
				ValueAtoms: output.ValueAtoms,
				PubKey:     output.PubKey,
			}
		}
	}
	return created
}

func cloneUtxos(utxos consensus.UtxoSet) consensus.UtxoSet {
	out := make(consensus.UtxoSet, len(utxos))
	for k, v := range utxos {
		out[k] = v
	}
	return out
}

func (c *ChainState) replaceMaterializedUTXOs(utxos consensus.UtxoSet) {
	// Keep the compatibility map, lookup adapter, and scalar count in lockstep
	// while the on-disk cutover is still removing map-based callers.
	c.utxos = utxos
	if c.utxos != nil {
		c.utxoLookup = consensus.LookupWithErrFromSet(c.utxos)
	}
	c.utxoCount = len(c.utxos)
}

func (c *ChainState) materializeUTXOs() (consensus.UtxoSet, error) {
	if c.utxos != nil {
		return cloneUtxos(c.utxos), nil
	}
	if c.utxoScan == nil {
		return nil, fmt.Errorf("utxo materialization unavailable")
	}
	utxos := make(consensus.UtxoSet, c.utxoCount)
	if err := c.utxoScan(func(outPoint types.OutPoint, entry consensus.UtxoEntry) error {
		utxos[outPoint] = entry
		return nil
	}); err != nil {
		return nil, err
	}
	return utxos, nil
}

func cloneUint64Ptr(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func cloneBlockHeaderPtr(header *types.BlockHeader) *types.BlockHeader {
	if header == nil {
		return nil
	}
	out := *header
	return &out
}

func (c *ChainState) prevBlockContext() consensus.PrevBlockContext {
	return consensus.PrevBlockContext{
		Height:         *c.height,
		Header:         *c.tipHeader,
		MedianTimePast: consensus.MedianTimePast(c.recentTimes),
		CurrentTime:    uint64(time.Now().Unix()),
	}
}

const recentTimeWindow = 11

func appendRecentTime(times []uint64, timestamp uint64) []uint64 {
	if len(times) < recentTimeWindow {
		return append(times, timestamp)
	}
	copy(times, times[1:])
	times[len(times)-1] = timestamp
	return times
}

func loadIndexedAncestorTimestamps(store *storage.ChainStore, tipHash [32]byte, limit int) ([]uint64, error) {
	if limit <= 0 {
		return nil, nil
	}
	times := make([]uint64, 0, limit)
	cursorHash := tipHash
	for len(times) < limit {
		entry, err := store.GetBlockIndex(&cursorHash)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, fmt.Errorf("missing block index for timestamp window %x", cursorHash)
		}
		times = append(times, entry.Header.Timestamp)
		if entry.Height == 0 {
			break
		}
		cursorHash = entry.ParentHash
	}
	slices.Reverse(times)
	return times, nil
}
