package node

import (
	"errors"
	"fmt"
	"log/slog"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
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
	UTXOCount           int
	BlockSizeLimit      uint64
}

type ChainReplaySummary struct {
	TipHeight      uint64
	TipHeaderHash  [32]byte
	UTXORoot       [32]byte
	UTXOCount      int
	BlockSizeLimit uint64
}

type CommittedChainView struct {
	Height         uint64
	TipHeader      types.BlockHeader
	TipHash        [32]byte
	BlockSizeState consensus.BlockSizeState
	UTXOs          consensus.UtxoSet
	UTXOAcc        *utreexo.Accumulator
	UTXORoot       [32]byte
}

type ChainState struct {
	params         consensus.ChainParams
	rules          consensus.ConsensusRules
	height         *uint64
	tipHeader      *types.BlockHeader
	blockSizeState consensus.BlockSizeState
	utxos          consensus.UtxoSet
	utxoAcc        *utreexo.Accumulator
}

type PersistentChainState struct {
	state *ChainState
	store *storage.ChainStore
}

func NewChainState(profile types.ChainProfile) *ChainState {
	params := consensus.ParamsForProfile(profile)
	return &ChainState{
		params:         params,
		rules:          consensus.DefaultConsensusRules(),
		blockSizeState: consensus.NewBlockSizeState(params),
		utxos:          make(consensus.UtxoSet),
	}
}

func (c *ChainState) WithRules(rules consensus.ConsensusRules) *ChainState {
	c.rules = rules
	return c
}

func ChainStateFromStoredState(stored *storage.StoredChainState) (*ChainState, error) {
	state := NewChainState(stored.Profile)
	if err := state.InitializeTip(stored.Height, stored.TipHeader, stored.BlockSizeState, stored.UTXOs); err != nil {
		return nil, err
	}
	return state, nil
}

func (c *ChainState) InitializeTip(height uint64, header types.BlockHeader, blockSizeState consensus.BlockSizeState, utxos consensus.UtxoSet) error {
	if c.tipHeader != nil {
		return ErrTipAlreadyInitialized
	}
	c.height = &height
	c.tipHeader = &header
	c.blockSizeState = blockSizeState
	c.utxos = cloneUtxos(utxos)
	acc, err := consensus.UtxoAccumulator(c.utxos)
	if err != nil {
		return err
	}
	c.utxoAcc = acc
	return nil
}

func (c *ChainState) InitializeFromGenesisBlock(genesis *types.Block) (GenesisBootstrapSummary, error) {
	logger := logging.Component("chain")
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
			KeyHash:    output.KeyHash,
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
		UTXOCount:           len(c.utxos),
		BlockSizeLimit:      consensus.NextBlockSizeLimit(c.blockSizeState, c.params),
	}
	logger.Info("initialized chain from genesis",
		slog.String("profile", c.params.Profile.String()),
		slog.String("header_hash", fmt.Sprintf("%x", summary.HeaderHash)),
		slog.String("coinbase_txid", fmt.Sprintf("%x", summary.CoinbaseTxID)),
		slog.Int("utxo_count", summary.UTXOCount),
		slog.Uint64("block_size_limit", summary.BlockSizeLimit),
	)
	return summary, nil
}

func (c *ChainState) ApplyBlock(block *types.Block) (consensus.BlockValidationSummary, error) {
	logger := logging.Component("chain")
	if c.height == nil || c.tipHeader == nil {
		return consensus.BlockValidationSummary{}, ErrNoTip
	}
	// The published chain UTXO map is immutable. Validation runs against that
	// shared base view and only swaps in a freshly materialized post-block map
	// once the block is fully validated.
	summary, finalUtxos, nextAcc, err := consensus.ValidateAndApplyBlockViewWithAccumulator(
		block,
		consensus.PrevBlockContext{Height: *c.height, Header: *c.tipHeader},
		c.blockSizeState,
		c.utxos,
		c.utxoAcc,
		c.params,
		c.rules,
	)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	height := summary.Height
	c.height = &height
	tip := block.Header
	c.tipHeader = &tip
	c.blockSizeState = summary.NextBlockSizeState
	c.utxos = finalUtxos
	c.utxoAcc = nextAcc
	hash := consensus.HeaderHash(&block.Header)
	logger.Info("validated block",
		slog.Uint64("height", summary.Height),
		slog.String("hash", fmt.Sprintf("%x", hash)),
		slog.Int("txs", len(block.Txs)),
		slog.Int("utxo_count", len(c.utxos)),
		slog.Uint64("next_block_size_limit", consensus.NextBlockSizeLimit(summary.NextBlockSizeState, c.params)),
	)
	return summary, nil
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
		UTXOCount:      len(c.utxos),
		BlockSizeLimit: consensus.NextBlockSizeLimit(c.blockSizeState, c.params),
	}, nil
}

func (c *ChainState) TipHeight() *uint64 {
	return c.height
}

func (c *ChainState) Profile() types.ChainProfile {
	return c.params.Profile
}

func (c *ChainState) TipHeader() *types.BlockHeader {
	return c.tipHeader
}

func (c *ChainState) BlockSizeState() consensus.BlockSizeState {
	return c.blockSizeState
}

func (c *ChainState) UTXOs() consensus.UtxoSet {
	return c.utxos
}

func (c *ChainState) UTXORoot() [32]byte {
	if c.utxoAcc == nil {
		return consensus.ComputedUTXORoot(c.utxos)
	}
	return c.utxoAcc.Root()
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
		UTXOs:          c.utxos,
		UTXOAcc:        c.utxoAcc,
		UTXORoot:       c.UTXORoot(),
	}
	return view, true
}

func (c *ChainState) UTXOAccumulator() *utreexo.Accumulator {
	return c.utxoAcc
}

func (c *ChainState) StoredState() (*storage.StoredChainState, error) {
	if c.height == nil || c.tipHeader == nil {
		return nil, ErrNoTip
	}
	return &storage.StoredChainState{
		Profile:        c.Profile(),
		Height:         *c.height,
		TipHeader:      *c.tipHeader,
		BlockSizeState: c.blockSizeState,
		UTXOs:          cloneUtxos(c.utxos),
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
	}, nil
}

func OpenPersistentChainState(path string, profile types.ChainProfile) (*PersistentChainState, error) {
	return OpenPersistentChainStateWithRules(path, profile, consensus.DefaultConsensusRules())
}

func OpenPersistentChainStateWithRules(path string, profile types.ChainProfile, rules consensus.ConsensusRules) (*PersistentChainState, error) {
	logger := logging.Component("chain")
	logger.Info("opening persistent chain state", slog.String("path", path), slog.String("profile", profile.String()))
	store, err := storage.Open(path)
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
		state.WithRules(rules)
		logger.Info("loaded persisted chain state",
			slog.Uint64("height", stored.Height),
			slog.Int("utxo_count", len(stored.UTXOs)),
			slog.Uint64("block_size_limit", consensus.NextBlockSizeLimit(stored.BlockSizeState, state.params)),
		)
	} else {
		state = NewChainState(profile).WithRules(rules)
		logger.Info("no persisted chain state found", slog.String("path", path))
	}
	return &PersistentChainState{state: state, store: store}, nil
}

func (p *PersistentChainState) Close() error {
	if p == nil || p.store == nil {
		return nil
	}
	logging.Component("chain").Info("closing persistent chain state")
	return p.store.Close()
}

func (p *PersistentChainState) InitializeFromGenesisBlock(genesis *types.Block) (GenesisBootstrapSummary, error) {
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
	logging.Component("chain").Info("persisted genesis block",
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
	return p.state
}

func (p *PersistentChainState) CommittedView() (CommittedChainView, bool) {
	if p == nil || p.state == nil {
		return CommittedChainView{}, false
	}
	return p.state.CommittedView()
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
				KeyHash:    output.KeyHash,
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
