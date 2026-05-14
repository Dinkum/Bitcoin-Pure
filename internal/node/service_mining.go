package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"
)

func (s *Service) MineBlocks(count int) ([]string, error) {
	hashes := make([]string, 0, count)
	for len(hashes) < count {
		hash, err := s.mineOneBlock()
		if err != nil {
			return hashes, err
		}
		hashes = append(hashes, hex.EncodeToString(hash[:]))
	}
	return hashes, nil
}

func (s *Service) currentTemplateGeneration() uint64 {
	return s.minerManager().currentTemplateGeneration()
}

func (s *Service) invalidateBlockTemplate(reason string) {
	s.minerManager().invalidateBlockTemplate(reason)
}

func (s *Service) mineOneBlock() ([32]byte, error) {
	return s.minerManager().mineOneBlock()
}

func (s *Service) mineBlockTemplate(block types.Block, generation uint64) (types.Block, bool, error) {
	params := consensus.ParamsForProfile(s.cfg.Profile)
	selectionAcc, fresh, err := s.miningSelectionAccumulator(block)
	if err != nil {
		return types.Block{}, false, err
	}
	if !fresh {
		return types.Block{}, false, nil
	}
	return s.mineBlockSearchSpace(block, params, selectionAcc, func(uint64) bool {
		return s.currentTemplateGeneration() == generation
	})
}

func (s *Service) MineFundingOutputs(keyHashes [][32]byte) ([]FundingOutput, error) {
	if !s.cfg.Profile.IsRegtestLike() {
		return nil, errors.New("funding outputs are only available on regtest-style profiles")
	}
	if len(keyHashes) == 0 {
		return nil, nil
	}
	for {
		block, outputs, err := s.buildFundingBlock(keyHashes)
		if err != nil {
			return nil, err
		}
		hash, _, err := s.acceptMinedBlock(block)
		if err == nil {
			for i := range outputs {
				outputs[i].BlockHash = hash
			}
			return outputs, nil
		}
		if strings.Contains(err.Error(), "stale template") {
			continue
		}
		return nil, err
	}
}

func (s *Service) handleCommittedBranchTransition(transition committedBranchTransition, relay bool) error {
	for i := range transition.Connected {
		s.pool.RemoveConfirmed(&transition.Connected[i])
		s.avalancheManager().onBlockAccepted(&transition.Connected[i])
		s.promoteReadyOrphansAfterBlock(&transition.Connected[i])
		s.removeLocalRebroadcastForBlock(&transition.Connected[i])
	}
	if txs := transition.DisconnectedTxs; len(txs) > 0 {
		s.reprocessTransactions(txs, "reorg_reprocess", relay)
	}
	if len(transition.Connected) > 0 || len(transition.DisconnectedTxs) > 0 {
		s.scheduleMempoolPersistence()
	}
	return nil
}

func (s *Service) acceptMinedBlock(block types.Block) ([32]byte, uint64, error) {
	startedAt := time.Now()
	hash := consensus.HeaderHash(&block.Header)

	s.stateMu.Lock()
	beforeTip, ok := s.chainState.tipSnapshot()
	if !ok {
		s.stateMu.Unlock()
		return [32]byte{}, 0, ErrNoTip
	}
	if block.Header.PrevBlockHash != beforeTip.TipHash {
		s.stateMu.Unlock()
		return [32]byte{}, 0, errors.New("stale template")
	}
	summary, transition, err := s.chainState.ApplyBlockWithTransition(&block)
	if err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	if err := s.headerChain.ApplyHeader(&block.Header); err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	stored, err := s.headerChain.StoredState()
	if err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	if err := s.chainState.Store().WriteHeaderState(stored); err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	afterTip, ok := s.chainState.tipSnapshot()
	if !ok {
		s.stateMu.Unlock()
		return [32]byte{}, 0, ErrNoTip
	}
	height := afterTip.Height
	if err := s.chainState.Store().SetHeaderHashByHeight(height, hash); err != nil {
		s.stateMu.Unlock()
		return [32]byte{}, 0, err
	}
	s.stateMu.Unlock()

	if err := s.handleCommittedBranchTransition(transition, true); err != nil {
		return [32]byte{}, 0, err
	}
	s.invalidateBlockTemplate("tip_advanced")
	s.cacheRecentBlock(block)
	s.cacheRecentHeader(block.Header)
	// Freshly mined blocks should not wait for peers to discover the header via
	// inv and then kick off headers-first recovery. Pushing the header first keeps
	// steady-state one-block catch-up responsive and avoids benchmark seeding
	// stalls on the initial funding block.
	s.broadcastHeaders([]types.BlockHeader{block.Header})
	s.broadcastMinedCompactBlock(block)
	s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	s.noteBlockAccepted()
	s.noteBlockSignatureVerification(summary)
	s.logger.Info("block accepted",
		slog.Uint64("height", height),
		slog.String("hash", hex.EncodeToString(hash[:])),
		slog.Int("txs", len(block.Txs)),
		slog.Int("sig_checks", summary.SignatureChecks),
		slog.Bool("sig_batch_fallback", summary.SignatureBatchFallback),
		slog.Duration("sig_verify_duration", summary.SignatureVerifyTime),
		slog.Int("mempool_size", s.pool.Count()),
		slog.Duration("apply_duration", time.Since(startedAt)),
	)
	s.perf.noteBlockApplyDuration(time.Since(startedAt))
	return hash, height, nil
}

func (s *Service) BuildBlockTemplate() (types.Block, error) {
	return s.minerManager().BuildBlockTemplate()
}

func (s *Service) BuildBenchmarkBlockTemplate(maxTxs int) (types.Block, error) {
	return s.minerManager().BuildBenchmarkBlockTemplate(maxTxs)
}

// Profile exposes the active chain profile to local tooling like the benchmark
// harness, which needs to size regtest-only funding batches correctly.
func (s *Service) Profile() types.ChainProfile {
	return s.cfg.Profile
}

// SubmitBenchmarkBlock accepts a prebuilt block through the local node path.
// Benchmark harnesses use this to separate block assembly/sealing from block
// application and relay timing. When SyntheticMining is enabled on the
// service, PoW checks are skipped for this acceptance path.
func (s *Service) AcceptLocalBenchmarkBlock(block types.Block) ([32]byte, uint64, error) {
	return s.acceptMinedBlock(block)
}

// SubmitBenchmarkBlock accepts a prebuilt block as if it arrived from an
// external source. Benchmark harnesses use this for follower fanout after the
// block has already been accepted on the producer node.
func (s *Service) SubmitBenchmarkBlock(block types.Block) ([32]byte, uint64, error) {
	hash := consensus.HeaderHash(&block.Header)
	applied, _, _, err := s.applyPeerBlock(&block)
	if errors.Is(err, ErrBlockHeaderNotIndexed) {
		if _, headerErr := s.applyPeerHeaders([]types.BlockHeader{block.Header}); headerErr == nil {
			applied, _, _, err = s.applyPeerBlock(&block)
		}
	}
	if err != nil {
		return [32]byte{}, 0, err
	}
	if applied {
		s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	}
	height := uint64(0)
	if tip := s.chainState.ChainState().TipHeight(); tip != nil {
		height = *tip
	}
	return hash, height, nil
}

func (s *Service) buildFundingBlock(keyHashes [][32]byte) (types.Block, []FundingOutput, error) {
	ctx, err := s.minerManager().chainSelectionSnapshot()
	if err != nil {
		return types.Block{}, nil, err
	}
	if len(keyHashes) == 0 {
		return types.Block{}, nil, errors.New("at least one key hash is required")
	}
	params := consensus.ParamsForProfile(s.cfg.Profile)
	nextTimestamp := ctx.tipHeader.Timestamp + uint64(params.TargetSpacingSecs)
	if nextTimestamp <= ctx.tipHeader.Timestamp {
		nextTimestamp = ctx.tipHeader.Timestamp + 1
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: ctx.height, Header: ctx.tipHeader}, params)
	if err != nil {
		return types.Block{}, nil, err
	}
	subsidy := consensus.SubsidyAtoms(ctx.height+1, params)
	if subsidy < uint64(len(keyHashes)) {
		return types.Block{}, nil, consensus.ErrInputsLessThanOutputs
	}
	outputs := make([]types.TxOutput, len(keyHashes))
	perOutput := subsidy / uint64(len(keyHashes))
	remainder := subsidy % uint64(len(keyHashes))
	for i, keyHash := range keyHashes {
		value := perOutput
		if i == len(keyHashes)-1 {
			value += remainder
		}
		outputs[i] = types.TxOutput{ValueAtoms: value, PubKey: keyHash}
	}
	coinbase := coinbaseTxForHeight(ctx.height+1, outputs)
	coinbaseTxID := consensus.TxID(&coinbase)
	funding := make([]FundingOutput, 0, len(outputs))
	fundingLeaves := make([]utreexo.UtxoLeaf, 0, len(outputs))
	for vout, output := range outputs {
		outPoint := types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}
		fundingLeaves = append(fundingLeaves, utxoLeafForOutput(outPoint, output))
		funding = append(funding, FundingOutput{
			OutPoint: outPoint,
			Value:    output.ValueAtoms,
			PubKey:   output.PubKey,
		})
	}
	finalAcc, err := ctx.utxoAcc.Apply(nil, fundingLeaves)
	if err != nil {
		return types.Block{}, nil, err
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  ctx.tipHash,
		MerkleTxIDRoot: consensus.MerkleRoot([][32]byte{coinbaseTxID}),
		MerkleAuthRoot: consensus.MerkleRoot([][32]byte{consensus.AuthID(&coinbase)}),
		UTXORoot:       finalAcc.Root(),
		Timestamp:      nextTimestamp,
		NBits:          nbits,
	}
	block := types.Block{Header: header, Txs: []types.Transaction{coinbase}}
	minedBlock, ok, err := s.mineBlockSearchSpace(block, params, ctx.utxoAcc, nil)
	if err != nil {
		return types.Block{}, nil, err
	}
	if !ok {
		return types.Block{}, nil, consensus.ErrInvalidPow
	}
	return minedBlock, funding, nil
}

func (s *Service) chainUtxoSnapshot() consensus.UtxoLookup {
	return s.chainState.Store().UTXOLookupFunc()
}

func coinbaseTxForHeight(height uint64, outputs []types.TxOutput) types.Transaction {
	var extraNonce [types.CoinbaseExtraNonceLen]byte
	return types.Transaction{
		Base: types.TxBase{
			Version:            1,
			CoinbaseHeight:     &height,
			CoinbaseExtraNonce: &extraNonce,
			Outputs:            outputs,
		},
	}
}

func (s *Service) miningSelectionAccumulator(block types.Block) (*utreexo.Accumulator, bool, error) {
	s.stateMu.RLock()
	view, ok := s.chainState.sharedCommittedView()
	s.stateMu.RUnlock()
	if !ok {
		return nil, false, ErrNoTip
	}
	if block.Header.PrevBlockHash != view.TipHash {
		return nil, false, nil
	}
	if len(block.Txs) <= 1 {
		return view.UTXOAcc, true, nil
	}
	nextAcc, err := view.UTXOAcc.Apply(blockSpendOutPoints(block.Txs[1:]), blockCreatedLeaves(block.Txs[1:]))
	if err != nil {
		return nil, false, err
	}
	return nextAcc, true, nil
}

func (s *Service) mineBlockSearchSpace(block types.Block, params consensus.ChainParams, selectionAcc *utreexo.Accumulator, shouldContinue func(uint64) bool) (types.Block, bool, error) {
	mineHeader := s.mineHeaderFn
	if mineHeader == nil {
		mineHeader = consensus.MineHeaderInterruptible
	}
	if len(block.Txs) == 0 || !block.Txs[0].IsCoinbase() {
		return types.Block{}, false, errors.New("mining requires coinbase-first block")
	}
	continueMining := func(nonce uint64) bool {
		select {
		case <-s.stopCh:
			return false
		default:
		}
		return shouldContinue == nil || shouldContinue(nonce)
	}
	for {
		minedHeader, ok, err := mineHeader(block.Header, params, continueMining)
		if err == nil && ok {
			block.Header = minedHeader
			return block, true, nil
		}
		if err != nil && !errors.Is(err, consensus.ErrMiningNonceExhausted) {
			return types.Block{}, false, err
		}
		if !ok && err == nil {
			return types.Block{}, false, nil
		}
		if !continueMining(0) {
			return types.Block{}, false, nil
		}
		nextExtraNonce, wrapped := incrementCoinbaseExtraNonce(*block.Txs[0].Base.CoinbaseExtraNonce)
		if wrapped {
			return types.Block{}, false, consensus.ErrInvalidPow
		}
		block.Txs[0].Base.CoinbaseExtraNonce = &nextExtraNonce
		block.Header.Nonce = 0
		if err := rebuildCoinbaseSearchCommitments(&block, selectionAcc); err != nil {
			return types.Block{}, false, err
		}
		s.logger.Debug("mining rolled coinbase extra nonce",
			slog.String("extra_nonce", hex.EncodeToString(nextExtraNonce[:])),
			slog.Uint64("timestamp", block.Header.Timestamp),
		)
	}
}

func rebuildCoinbaseSearchCommitments(block *types.Block, selectionAcc *utreexo.Accumulator) error {
	coinbaseTxID := consensus.TxID(&block.Txs[0])
	_, _, txRoot, authRoot := consensus.BuildBlockRoots(block.Txs)
	block.Header.MerkleTxIDRoot = txRoot
	block.Header.MerkleAuthRoot = authRoot
	finalAcc, err := selectionAcc.Apply(nil, coinbaseLeaves(coinbaseTxID, block.Txs[0].Base.Outputs))
	if err != nil {
		return err
	}
	block.Header.UTXORoot = finalAcc.Root()
	return nil
}

func incrementCoinbaseExtraNonce(current [types.CoinbaseExtraNonceLen]byte) ([types.CoinbaseExtraNonceLen]byte, bool) {
	next := current
	for i := 0; i < len(next); i++ {
		next[i]++
		if next[i] != 0 {
			return next, false
		}
	}
	return next, true
}

func coinbaseLeaves(txid [32]byte, outputs []types.TxOutput) []utreexo.UtxoLeaf {
	leaves := make([]utreexo.UtxoLeaf, 0, len(outputs))
	for vout, output := range outputs {
		leaves = append(leaves, utxoLeafForOutput(types.OutPoint{TxID: txid, Vout: uint32(vout)}, output))
	}
	return leaves
}

func blockSpendOutPoints(txs []types.Transaction) []types.OutPoint {
	spent := make([]types.OutPoint, 0)
	for _, tx := range txs {
		for _, input := range tx.Base.Inputs {
			spent = append(spent, input.PrevOut)
		}
	}
	return spent
}

func blockCreatedLeaves(txs []types.Transaction) []utreexo.UtxoLeaf {
	created := make([]utreexo.UtxoLeaf, 0)
	for _, tx := range txs {
		txid := consensus.TxID(&tx)
		for vout, output := range tx.Base.Outputs {
			created = append(created, utxoLeafForOutput(types.OutPoint{TxID: txid, Vout: uint32(vout)}, output))
		}
	}
	return created
}

func (s *Service) chainUtxoSnapshotWithTip() ([32]byte, consensus.UtxoLookup) {
	tip, ok := s.chainState.tipSnapshot()
	if !ok {
		return [32]byte{}, s.chainState.Store().UTXOLookupFunc()
	}
	return tip.TipHash, s.chainState.Store().UTXOLookupFunc()
}

func (s *Service) chainTipHash() [32]byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	// Batch tip-stability checks only need the active tip identity. Avoid
	// CommittedView here because that path defensively clones the full UTXO map.
	tip, ok := s.chainState.tipSnapshot()
	if ok {
		return tip.TipHash
	}
	return [32]byte{}
}
