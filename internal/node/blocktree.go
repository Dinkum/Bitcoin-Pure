package node

import (
	"fmt"
	"slices"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
	"bitcoin-pure/internal/utxochecksum"
)

var (
	ErrBlockAlreadyKnown      = fmt.Errorf("block already known")
	ErrUnknownParent          = fmt.Errorf("unknown parent block")
	ErrParentStateUnavailable = fmt.Errorf("parent block state unavailable")
)

type branchStep struct {
	block types.Block
	entry storage.BlockIndexEntry
}

// committedBranchTransition is the runtime-facing branch delta published by a
// successful active-tip advance or winner-branch reorg. Connected blocks are in
// commit order. Disconnected transactions exclude coinbase and are ordered by
// block height from fork+1 upward so mempool reprocessing sees the same
// dependency ordering as the previous storage-reread path.
type committedBranchTransition struct {
	Connected       []types.Block
	DisconnectedTxs []types.Transaction
}

func (c *ChainState) Clone() *ChainState {
	out := NewChainState(c.params.Profile).WithRules(c.rules).WithLogger(c.logger)
	if c.height != nil && c.tipHeader != nil {
		height := *c.height
		header := *c.tipHeader
		out.height = &height
		out.tipHeader = &header
	}
	out.recentTimes = append([]uint64(nil), c.recentTimes...)
	out.blockSizeState = c.blockSizeState
	// The published UTXO map is immutable; branch evaluation can share it and
	// let subsequent apply/disconnect steps materialize fresh maps only when the
	// branch actually mutates state.
	out.utxos = c.utxos
	out.utxosShared = c.utxos != nil
	if c.utxos != nil {
		c.utxosShared = true
	}
	out.utxoLookup = c.utxoLookup
	out.utxoScan = c.utxoScan
	out.utxoCount = c.utxoCount
	out.utxoAcc = c.utxoAcc
	// Branch evaluation persists StoredStateMeta snapshots. Carry the committed
	// checksum forward so append/reorg paths keep delta updates anchored to the
	// real pre-state instead of recomputing from the zero value.
	out.utxoChecksum = c.utxoChecksum
	return out
}

func (c *ChainState) DisconnectBlock(block *types.Block, undo []storage.BlockUndoEntry, parent *storage.BlockIndexEntry) error {
	if c.height == nil || c.tipHeader == nil {
		return ErrNoTip
	}
	baseUtxos, err := c.materializeUTXOs()
	if err != nil {
		return err
	}
	workingUtxos := consensus.NewUtxoOverlay(baseUtxos)
	nextAcc, err := disconnectBlockOverlay(workingUtxos, c.utxoAcc, block, undo)
	if err != nil {
		return err
	}

	height := parent.Height
	header := parent.Header
	c.height = &height
	c.tipHeader = &header
	if len(c.recentTimes) > 0 {
		c.recentTimes = append([]uint64(nil), c.recentTimes[:len(c.recentTimes)-1]...)
	}
	c.blockSizeState = parent.BlockSizeState
	c.replaceMaterializedUTXOs(workingUtxos.Materialize())
	c.utxoAcc = nextAcc
	// Disconnects are reorg-only and off the hot path, so recompute directly to
	// guarantee checksum/state parity after undo application.
	c.utxoChecksum = utxochecksum.Compute(c.utxos)
	return nil
}

func (p *PersistentChainState) ApplyBlock(block *types.Block) (consensus.BlockValidationSummary, error) {
	summary, _, _, err := p.ApplyBlockWithTiming(block)
	return summary, err
}

func (p *PersistentChainState) ApplyBlockWithTransition(block *types.Block) (consensus.BlockValidationSummary, committedBranchTransition, error) {
	summary, _, transition, err := p.ApplyBlockWithTiming(block)
	return summary, transition, err
}

func (p *PersistentChainState) ApplyBlockWithTiming(block *types.Block) (consensus.BlockValidationSummary, time.Duration, committedBranchTransition, error) {
	if summary, wait, transition, committed, err := p.tryApplyActiveTipExtension(block); committed || err != nil {
		return summary, wait, transition, err
	}
	waitStarted := time.Now()
	p.mu.Lock()
	wait := time.Since(waitStarted)
	defer p.mu.Unlock()
	summary, transition, err := p.applyBlockLocked(block)
	return summary, wait, transition, err
}

func (p *PersistentChainState) tryApplyActiveTipExtension(block *types.Block) (consensus.BlockValidationSummary, time.Duration, committedBranchTransition, bool, error) {
	p.mu.RLock()
	if p.state == nil || p.state.height == nil || p.state.tipHeader == nil {
		p.mu.RUnlock()
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, false, nil
	}
	snapshot := p.state.Clone()
	tipHash := consensus.HeaderHash(snapshot.tipHeader)
	p.mu.RUnlock()

	if block.Header.PrevBlockHash != tipHash {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, false, nil
	}

	blockHash := consensus.HeaderHash(&block.Header)
	existing, err := p.store.GetBlockIndex(&blockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, err
	}
	if existing != nil && existing.Validated {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, ErrBlockAlreadyKnown
	}
	parentEntry, err := p.store.GetBlockIndex(&tipHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, err
	}
	if parentEntry == nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, ErrUnknownParent
	}
	if !parentEntry.Validated {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, ErrParentStateUnavailable
	}

	// Validate against an immutable snapshot without holding the write lock. If
	// the tip moves before commit, we discard this work and fall back to the
	// general locked path.
	undo, err := captureUndoEntries(block, snapshot.utxoLookup)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, err
	}
	detail, err := snapshot.applyBlockDetailed(block)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, err
	}
	nextStateMeta, err := snapshot.StoredStateMeta()
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, err
	}
	work, err := consensus.BlockWork(block.Header.NBits)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, true, err
	}
	entry := storage.BlockIndexEntry{
		Height:         detail.summary.Height,
		ParentHash:     block.Header.PrevBlockHash,
		Header:         block.Header,
		ChainWork:      consensus.AddChainWork(parentEntry.ChainWork, work),
		Validated:      true,
		BlockSizeState: snapshot.BlockSizeState(),
	}
	spent, created := activeTipDelta(undo, detail.createdUTXO)

	waitStarted := time.Now()
	p.mu.Lock()
	wait := time.Since(waitStarted)
	defer p.mu.Unlock()

	if p.state == nil || p.state.height == nil || p.state.tipHeader == nil {
		return consensus.BlockValidationSummary{}, wait, committedBranchTransition{}, false, nil
	}
	if consensus.HeaderHash(p.state.tipHeader) != tipHash {
		return consensus.BlockValidationSummary{}, wait, committedBranchTransition{}, false, nil
	}
	existing, err = p.store.GetBlockIndex(&blockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, wait, committedBranchTransition{}, true, err
	}
	if existing != nil && existing.Validated {
		return consensus.BlockValidationSummary{}, wait, committedBranchTransition{}, true, ErrBlockAlreadyKnown
	}
	if err := p.store.AppendValidatedBlock(nextStateMeta, block, &entry, undo, spent, created); err != nil {
		return consensus.BlockValidationSummary{}, wait, committedBranchTransition{}, true, err
	}
	snapshot.bindCommittedUTXOBackend(p.store.UTXOLookupWithErr(), p.store.ForEachUTXO, snapshot.UTXOCount())
	p.state = snapshot
	return detail.summary, wait, committedBranchTransition{Connected: []types.Block{*block}}, true, nil
}

func (p *PersistentChainState) applyBlockLocked(block *types.Block) (consensus.BlockValidationSummary, committedBranchTransition, error) {
	if p.state.height == nil || p.state.tipHeader == nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, ErrNoTip
	}

	blockHash := consensus.HeaderHash(&block.Header)
	existing, err := p.store.GetBlockIndex(&blockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
	}
	if existing != nil && existing.Validated {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, ErrBlockAlreadyKnown
	}

	parentEntry, err := p.store.GetBlockIndex(&block.Header.PrevBlockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
	}
	if parentEntry == nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, ErrUnknownParent
	}
	if !parentEntry.Validated {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, ErrParentStateUnavailable
	}

	steps, forkHeight, err := p.branchSteps(parentEntry, block, existing)
	if err != nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
	}
	if err := p.rejectDeepReorgWhileFastSyncPending(forkHeight); err != nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
	}
	tempState, reorgOverlay, connectedEntries, undoByHash, createdByHash, transition, summary, err := p.evaluateBranch(steps, forkHeight)
	if err != nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
	}

	bestTipHash := consensus.HeaderHash(p.state.TipHeader())
	bestTipEntry, err := p.store.GetBlockIndex(&bestTipHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
	}
	if bestTipEntry == nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, fmt.Errorf("missing best-tip entry for %x", bestTipHash)
	}
	newTipEntry := connectedEntries[len(connectedEntries)-1]
	connectedEntryIndex := indexBranchEntries(connectedEntries)
	if consensus.CompareChainWork(newTipEntry.ChainWork, bestTipEntry.ChainWork) <= 0 {
		for _, step := range steps {
			hash := consensus.HeaderHash(&step.block.Header)
			entryIndex, ok := connectedEntryIndex[hash]
			if !ok {
				return consensus.BlockValidationSummary{}, committedBranchTransition{}, fmt.Errorf("missing connected entry for block %x", hash)
			}
			entry := &connectedEntries[entryIndex]
			if err := p.store.PutValidatedBlock(&step.block, entry, undoByHash[hash]); err != nil {
				return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
			}
		}
		return summary, committedBranchTransition{}, nil
	}

	nextStateMeta, err := tempState.StoredStateMeta()
	if err != nil {
		return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
	}
	isActiveTipExtension := forkHeight == oldTipHeightForState(p.state) && len(steps) == 1 && parentEntry.Height == bestTipEntry.Height &&
		consensus.HeaderHash(&parentEntry.Header) == bestTipHash
	oldTipHeight := *p.state.TipHeight()
	if isActiveTipExtension {
		undo := undoByHash[blockHash]
		spent, created := activeTipDelta(undo, createdByHash[blockHash])
		if err := p.store.AppendValidatedBlock(nextStateMeta, block, &newTipEntry, undo, spent, created); err != nil {
			return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
		}
	} else {
		for _, step := range steps {
			hash := consensus.HeaderHash(&step.block.Header)
			entryIndex, ok := connectedEntryIndex[hash]
			if !ok {
				return consensus.BlockValidationSummary{}, committedBranchTransition{}, fmt.Errorf("missing connected entry for block %x", hash)
			}
			entry := &connectedEntries[entryIndex]
			if err := p.store.PutValidatedBlock(&step.block, entry, undoByHash[hash]); err != nil {
				return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
			}
		}
		if err := p.store.CommitReorgDelta(
			chainStateMeta(nextStateMeta),
			reorgOverlay.SpentOutPoints(),
			reorgOverlay.CreatedEntriesClone(),
			forkHeight,
			oldTipHeight,
			connectedEntries,
		); err != nil {
			return consensus.BlockValidationSummary{}, committedBranchTransition{}, err
		}
	}
	tempState.bindCommittedUTXOBackend(p.store.UTXOLookupWithErr(), p.store.ForEachUTXO, tempState.UTXOCount())
	p.state = tempState
	return summary, transition, nil
}

func (p *PersistentChainState) rejectDeepReorgWhileFastSyncPending(forkHeight uint64) error {
	fastSyncState, err := p.store.LoadFastSyncState()
	if err != nil {
		return err
	}
	if fastSyncState != nil && forkHeight < fastSyncState.SnapshotHeight {
		return fmt.Errorf("%w: fork height %d is below imported snapshot height %d", ErrHistoricalSnapshotVerificationPending, forkHeight, fastSyncState.SnapshotHeight)
	}
	return nil
}

func oldTipHeightForState(state *ChainState) uint64 {
	if state == nil || state.TipHeight() == nil {
		return 0
	}
	return *state.TipHeight()
}

func (p *PersistentChainState) branchSteps(parentEntry *storage.BlockIndexEntry, block *types.Block, existing *storage.BlockIndexEntry) ([]branchStep, uint64, error) {
	currentEntry, err := p.currentEntryForBlock(parentEntry, block, existing)
	if err != nil {
		return nil, 0, err
	}

	steps := []branchStep{{block: *block, entry: currentEntry}}
	cursor := parentEntry
	for {
		activeHash, err := p.store.GetBlockHashByHeight(cursor.Height)
		if err != nil {
			return nil, 0, err
		}
		cursorHash := consensus.HeaderHash(&cursor.Header)
		if activeHash != nil && *activeHash == cursorHash {
			reverseBranchSteps(steps)
			return steps, cursor.Height, nil
		}
		rawBlock, err := p.store.GetBlock(&cursorHash)
		if err != nil {
			return nil, 0, err
		}
		if rawBlock == nil {
			return nil, 0, fmt.Errorf("missing raw block for side-chain ancestor %x", cursorHash)
		}
		steps = append(steps, branchStep{block: *rawBlock, entry: *cursor})

		parent, err := p.store.GetBlockIndex(&cursor.ParentHash)
		if err != nil {
			return nil, 0, err
		}
		if parent == nil {
			return nil, 0, fmt.Errorf("missing parent index for side-chain ancestor %x", cursorHash)
		}
		cursor = parent
	}
}

func (p *PersistentChainState) currentEntryForBlock(parentEntry *storage.BlockIndexEntry, block *types.Block, existing *storage.BlockIndexEntry) (storage.BlockIndexEntry, error) {
	if existing != nil {
		if existing.Header != block.Header {
			return storage.BlockIndexEntry{}, fmt.Errorf("stored header mismatch for block %x", consensus.HeaderHash(&block.Header))
		}
		return storage.BlockIndexEntry{
			Height:         existing.Height,
			ParentHash:     existing.ParentHash,
			Header:         existing.Header,
			ChainWork:      existing.ChainWork,
			Validated:      true,
			BlockSizeState: existing.BlockSizeState,
		}, nil
	}

	work, err := consensus.BlockWork(block.Header.NBits)
	if err != nil {
		return storage.BlockIndexEntry{}, err
	}
	return storage.BlockIndexEntry{
		Height:         parentEntry.Height + 1,
		ParentHash:     block.Header.PrevBlockHash,
		Header:         block.Header,
		ChainWork:      consensus.AddChainWork(parentEntry.ChainWork, work),
		Validated:      true,
		BlockSizeState: consensus.BlockSizeState{},
	}, nil
}

func (p *PersistentChainState) evaluateBranch(steps []branchStep, forkHeight uint64) (*ChainState, *consensus.UtxoOverlay, []storage.BlockIndexEntry, map[[32]byte][]storage.BlockUndoEntry, map[[32]byte]map[types.OutPoint]consensus.UtxoEntry, committedBranchTransition, consensus.BlockValidationSummary, error) {
	tempState := p.state.Clone()
	reorgOverlay, disconnectedBlocks, err := p.disconnectToHeight(tempState, forkHeight)
	if err != nil {
		return nil, nil, nil, nil, nil, committedBranchTransition{}, consensus.BlockValidationSummary{}, err
	}

	entries := make([]storage.BlockIndexEntry, 0, len(steps))
	undoByHash := make(map[[32]byte][]storage.BlockUndoEntry, len(steps))
	createdByHash := make(map[[32]byte]map[types.OutPoint]consensus.UtxoEntry, len(steps))
	connectedBlocks := make([]types.Block, 0, len(steps))
	var summary consensus.BlockValidationSummary
	for _, step := range steps {
		undo, err := captureUndoEntries(&step.block, tempState.utxoLookup)
		if err != nil {
			return nil, nil, nil, nil, nil, committedBranchTransition{}, consensus.BlockValidationSummary{}, err
		}
		detail, err := tempState.applyBlockDetailed(&step.block)
		if err != nil {
			return nil, nil, nil, nil, nil, committedBranchTransition{}, consensus.BlockValidationSummary{}, err
		}
		mergeOverlayDelta(reorgOverlay, detail.overlay)
		summary = detail.summary
		entry := step.entry
		entry.Height = summary.Height
		entry.Validated = true
		entry.BlockSizeState = tempState.BlockSizeState()
		hash := consensus.HeaderHash(&step.block.Header)
		entries = append(entries, entry)
		connectedBlocks = append(connectedBlocks, step.block)
		undoByHash[hash] = undo
		createdByHash[hash] = detail.createdUTXO
	}
	return tempState, reorgOverlay, entries, undoByHash, createdByHash, committedBranchTransition{
		Connected:       connectedBlocks,
		DisconnectedTxs: disconnectedBranchTransactions(disconnectedBlocks),
	}, summary, nil
}

func (p *PersistentChainState) disconnectToHeight(state *ChainState, targetHeight uint64) (*consensus.UtxoOverlay, []types.Block, error) {
	baseUtxos, err := state.materializeUTXOs()
	if err != nil {
		return nil, nil, err
	}
	workingUtxos := consensus.NewUtxoOverlay(baseUtxos)
	workingAcc := state.utxoAcc
	if state.utxoChecksum == ([32]byte{}) {
		state.utxoChecksum = utxochecksum.Compute(baseUtxos)
	}
	if workingAcc == nil {
		workingAcc, err = consensus.UtxoAccumulator(baseUtxos)
		if err != nil {
			return nil, nil, err
		}
	}
	disconnected := make([]types.Block, 0)
	for state.TipHeight() != nil && *state.TipHeight() > targetHeight {
		height := *state.TipHeight()
		hash, err := p.store.GetBlockHashByHeight(height)
		if err != nil {
			return nil, nil, err
		}
		if hash == nil {
			return nil, nil, fmt.Errorf("missing active hash at height %d", height)
		}
		block, err := p.store.GetBlock(hash)
		if err != nil {
			return nil, nil, err
		}
		if block == nil {
			return nil, nil, fmt.Errorf("missing active block %x", *hash)
		}
		disconnected = append(disconnected, *block)
		undo, err := p.store.GetUndo(hash)
		if err != nil {
			return nil, nil, err
		}
		parentEntry, err := p.store.GetBlockIndex(&block.Header.PrevBlockHash)
		if err != nil {
			return nil, nil, err
		}
		if parentEntry == nil {
			return nil, nil, fmt.Errorf("missing parent entry for active block %x", *hash)
		}
		checksumSpent := survivingBlockOutputs(workingUtxos.Lookup, block)
		checksumRestored := undoEntriesToUtxoMap(undo)
		state.utxoChecksum = utxochecksum.ApplyDelta(state.utxoChecksum, checksumSpent, checksumRestored)
		workingAcc, err = disconnectBlockOverlay(workingUtxos, workingAcc, block, undo)
		if err != nil {
			return nil, nil, err
		}
		parentHeight := parentEntry.Height
		parentHeader := parentEntry.Header
		state.height = &parentHeight
		state.tipHeader = &parentHeader
		state.blockSizeState = parentEntry.BlockSizeState
	}
	state.replaceMaterializedUTXOs(workingUtxos.Materialize())
	state.utxoAcc = workingAcc
	if state.tipHeader != nil {
		recentTimes, err := loadIndexedAncestorTimestamps(p.store, consensus.HeaderHash(state.tipHeader), 11)
		if err != nil {
			return nil, nil, err
		}
		state.recentTimes = recentTimes
	}
	slices.Reverse(disconnected)
	return workingUtxos, disconnected, nil
}

func disconnectedBranchTransactions(blocks []types.Block) []types.Transaction {
	total := 0
	for _, block := range blocks {
		if len(block.Txs) > 1 {
			total += len(block.Txs) - 1
		}
	}
	if total == 0 {
		return nil
	}
	txs := make([]types.Transaction, 0, total)
	for _, block := range blocks {
		txs = append(txs, block.Txs[1:]...)
	}
	return txs
}

func disconnectBlockOverlay(currentUtxos *consensus.UtxoOverlay, currentAcc *utreexo.Accumulator, block *types.Block, undo []storage.BlockUndoEntry) (*utreexo.Accumulator, error) {
	if currentUtxos == nil {
		return nil, fmt.Errorf("missing rollback utxo overlay")
	}
	if currentAcc == nil {
		return nil, fmt.Errorf("missing rollback utxo accumulator")
	}

	spentOutputs := make([]types.OutPoint, 0)
	for _, tx := range block.Txs {
		txid := consensus.TxID(&tx)
		for vout := range tx.Base.Outputs {
			outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			if _, ok := currentUtxos.Lookup(outPoint); ok {
				currentUtxos.Spend(outPoint)
				spentOutputs = append(spentOutputs, outPoint)
			}
		}
	}

	undoIndex := 0
	restoredLeaves := make([]utreexo.UtxoLeaf, 0, len(undo))
	// Outputs created earlier in the same block are removed above and should not be
	// restored from undo; only spends that reached into the pre-block UTXO set
	// consume undo entries.
	intraBlockOutputs := make(map[types.OutPoint]struct{})
	for i := 1; i < len(block.Txs); i++ {
		for _, input := range block.Txs[i].Base.Inputs {
			if _, ok := intraBlockOutputs[input.PrevOut]; ok {
				continue
			}
			if undoIndex >= len(undo) {
				return nil, fmt.Errorf("block undo mismatch: missing undo entry for input %v", input.PrevOut)
			}
			entry := undo[undoIndex]
			undoIndex++
			if entry.OutPoint != input.PrevOut {
				return nil, fmt.Errorf("undo outpoint mismatch for input %v", input.PrevOut)
			}
			currentUtxos.Restore(input.PrevOut, entry.Entry)
			restoredLeaves = append(restoredLeaves, utxoLeafForEntry(entry.OutPoint, entry.Entry))
		}
		txid := consensus.TxID(&block.Txs[i])
		for vout := range block.Txs[i].Base.Outputs {
			intraBlockOutputs[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = struct{}{}
		}
	}
	if undoIndex != len(undo) {
		return nil, fmt.Errorf("block undo mismatch: unused undo entries %d", len(undo)-undoIndex)
	}

	return currentAcc.Apply(spentOutputs, restoredLeaves)
}

func captureUndoEntries(block *types.Block, lookup consensus.UtxoLookupWithErr) ([]storage.BlockUndoEntry, error) {
	if block == nil || len(block.Txs) <= 1 {
		return nil, nil
	}
	inputCap := max(0, len(block.Txs)-1)
	outputCap := len(block.Txs)
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		if len(tx.Base.Inputs) > 1 {
			inputCap += len(tx.Base.Inputs) - 1
		}
		if len(tx.Base.Outputs) > 1 {
			outputCap += len(tx.Base.Outputs) - 1
		}
	}
	undo := make([]storage.BlockUndoEntry, 0, inputCap)
	spent := make(map[types.OutPoint]struct{}, inputCap)
	created := make(map[types.OutPoint]consensus.UtxoEntry, outputCap)
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		txid := consensus.TxID(tx)
		for _, input := range block.Txs[i].Base.Inputs {
			if _, duplicate := spent[input.PrevOut]; duplicate {
				return nil, fmt.Errorf("missing utxo for undo capture: %v", input.PrevOut)
			}
			spent[input.PrevOut] = struct{}{}
			// Only spends that reach into the pre-block UTXO set need an undo record.
			// Same-block dependency edges are rewound by deleting this block's outputs.
			if entry, existed, err := lookup(input.PrevOut); err != nil {
				return nil, fmt.Errorf("undo capture lookup failed: %w", err)
			} else if existed {
				undo = append(undo, storage.BlockUndoEntry{OutPoint: input.PrevOut, Entry: entry})
				continue
			}
			if _, ok := created[input.PrevOut]; !ok {
				return nil, fmt.Errorf("missing utxo for undo capture: %v", input.PrevOut)
			}
			delete(created, input.PrevOut)
		}
		for vout, output := range tx.Base.Outputs {
			created[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = consensus.UtxoEntry{
				Type:       output.Type,
				ValueAtoms: output.ValueAtoms,
				Payload32:  output.Payload32,
				PubKey:     output.PubKey,
			}
		}
	}
	return undo, nil
}

func activeTipDelta(undo []storage.BlockUndoEntry, created map[types.OutPoint]consensus.UtxoEntry) ([]types.OutPoint, map[types.OutPoint]consensus.UtxoEntry) {
	spent := make([]types.OutPoint, 0, len(undo))
	for _, entry := range undo {
		spent = append(spent, entry.OutPoint)
	}
	if len(created) == 0 {
		return spent, nil
	}
	return spent, created
}

func chainStateMeta(state *storage.StoredChainState) *storage.StoredChainStateMeta {
	if state == nil {
		return nil
	}
	return &storage.StoredChainStateMeta{
		Profile:        state.Profile,
		Height:         state.Height,
		TipHeader:      state.TipHeader,
		BlockSizeState: state.BlockSizeState,
		UTXOChecksum:   state.UTXOChecksum,
	}
}

func mergeOverlayDelta(target *consensus.UtxoOverlay, delta *consensus.UtxoOverlay) {
	if target == nil || delta == nil {
		return
	}
	for _, outPoint := range delta.SpentOutPoints() {
		target.Spend(outPoint)
	}
	for outPoint, entry := range delta.CreatedEntries() {
		target.Set(outPoint, entry)
	}
}

func survivingBlockOutputs(lookup consensus.UtxoLookup, block *types.Block) map[types.OutPoint]consensus.UtxoEntry {
	spent := make(map[types.OutPoint]consensus.UtxoEntry)
	for _, tx := range block.Txs {
		txid := consensus.TxID(&tx)
		for vout := range tx.Base.Outputs {
			outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			entry, ok := lookup(outPoint)
			if !ok {
				continue
			}
			spent[outPoint] = entry
		}
	}
	return spent
}

func undoEntriesToUtxoMap(undo []storage.BlockUndoEntry) map[types.OutPoint]consensus.UtxoEntry {
	if len(undo) == 0 {
		return nil
	}
	created := make(map[types.OutPoint]consensus.UtxoEntry, len(undo))
	for _, entry := range undo {
		created[entry.OutPoint] = entry.Entry
	}
	return created
}

func reverseBranchSteps(steps []branchStep) {
	for left, right := 0, len(steps)-1; left < right; left, right = left+1, right-1 {
		steps[left], steps[right] = steps[right], steps[left]
	}
}

func indexBranchEntries(entries []storage.BlockIndexEntry) map[[32]byte]int {
	index := make(map[[32]byte]int, len(entries))
	for i := range entries {
		index[consensus.HeaderHash(&entries[i].Header)] = i
	}
	return index
}
