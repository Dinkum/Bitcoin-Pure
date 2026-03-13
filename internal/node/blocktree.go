package node

import (
	"fmt"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
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
	out.utxoAcc = c.utxoAcc
	return out
}

func (c *ChainState) DisconnectBlock(block *types.Block, undo []storage.BlockUndoEntry, parent *storage.BlockIndexEntry) error {
	if c.height == nil || c.tipHeader == nil {
		return ErrNoTip
	}
	workingUtxos := consensus.NewUtxoOverlay(c.utxos)
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
	c.utxos = workingUtxos.Materialize()
	c.utxoAcc = nextAcc
	return nil
}

func (p *PersistentChainState) ApplyBlock(block *types.Block) (consensus.BlockValidationSummary, error) {
	summary, _, err := p.ApplyBlockWithTiming(block)
	return summary, err
}

func (p *PersistentChainState) ApplyBlockWithTiming(block *types.Block) (consensus.BlockValidationSummary, time.Duration, error) {
	if summary, wait, committed, err := p.tryApplyActiveTipExtension(block); committed || err != nil {
		return summary, wait, err
	}
	waitStarted := time.Now()
	p.mu.Lock()
	wait := time.Since(waitStarted)
	defer p.mu.Unlock()
	summary, err := p.applyBlockLocked(block)
	return summary, wait, err
}

func (p *PersistentChainState) tryApplyActiveTipExtension(block *types.Block) (consensus.BlockValidationSummary, time.Duration, bool, error) {
	p.mu.RLock()
	if p.state == nil || p.state.height == nil || p.state.tipHeader == nil {
		p.mu.RUnlock()
		return consensus.BlockValidationSummary{}, 0, false, nil
	}
	snapshot := p.state.Clone()
	tipHash := consensus.HeaderHash(snapshot.tipHeader)
	p.mu.RUnlock()

	if block.Header.PrevBlockHash != tipHash {
		return consensus.BlockValidationSummary{}, 0, false, nil
	}

	blockHash := consensus.HeaderHash(&block.Header)
	existing, err := p.store.GetBlockIndex(&blockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, true, err
	}
	if existing != nil && existing.Validated {
		return consensus.BlockValidationSummary{}, 0, true, ErrBlockAlreadyKnown
	}
	parentEntry, err := p.store.GetBlockIndex(&tipHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, true, err
	}
	if parentEntry == nil {
		return consensus.BlockValidationSummary{}, 0, true, ErrUnknownParent
	}
	if !parentEntry.Validated {
		return consensus.BlockValidationSummary{}, 0, true, ErrParentStateUnavailable
	}

	// Validate against an immutable snapshot without holding the write lock. If
	// the tip moves before commit, we discard this work and fall back to the
	// general locked path.
	undo, err := captureUndoEntries(block, snapshot.utxos)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, true, err
	}
	detail, err := snapshot.applyBlockDetailed(block)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, true, err
	}
	nextStateMeta, err := snapshot.StoredStateMeta()
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, true, err
	}
	work, err := consensus.BlockWork(block.Header.NBits)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, true, err
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
		return consensus.BlockValidationSummary{}, wait, false, nil
	}
	if consensus.HeaderHash(p.state.tipHeader) != tipHash {
		return consensus.BlockValidationSummary{}, wait, false, nil
	}
	existing, err = p.store.GetBlockIndex(&blockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, wait, true, err
	}
	if existing != nil && existing.Validated {
		return consensus.BlockValidationSummary{}, wait, true, ErrBlockAlreadyKnown
	}
	if err := p.store.AppendValidatedBlock(nextStateMeta, block, &entry, undo, spent, created); err != nil {
		return consensus.BlockValidationSummary{}, wait, true, err
	}
	p.state = snapshot
	return detail.summary, wait, true, nil
}

func (p *PersistentChainState) applyBlockLocked(block *types.Block) (consensus.BlockValidationSummary, error) {
	if p.state.height == nil || p.state.tipHeader == nil {
		return consensus.BlockValidationSummary{}, ErrNoTip
	}

	blockHash := consensus.HeaderHash(&block.Header)
	existing, err := p.store.GetBlockIndex(&blockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	if existing != nil && existing.Validated {
		return consensus.BlockValidationSummary{}, ErrBlockAlreadyKnown
	}

	parentEntry, err := p.store.GetBlockIndex(&block.Header.PrevBlockHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	if parentEntry == nil {
		return consensus.BlockValidationSummary{}, ErrUnknownParent
	}
	if !parentEntry.Validated {
		return consensus.BlockValidationSummary{}, ErrParentStateUnavailable
	}

	steps, forkHeight, err := p.branchSteps(parentEntry, block, existing)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	if err := p.rejectDeepReorgWhileFastSyncPending(forkHeight); err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	tempState, connectedEntries, undoByHash, createdByHash, summary, err := p.evaluateBranch(steps, forkHeight)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}

	bestTipHash := consensus.HeaderHash(p.state.TipHeader())
	bestTipEntry, err := p.store.GetBlockIndex(&bestTipHash)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	if bestTipEntry == nil {
		return consensus.BlockValidationSummary{}, fmt.Errorf("missing best-tip entry for %x", bestTipHash)
	}
	newTipEntry := connectedEntries[len(connectedEntries)-1]
	if consensus.CompareChainWork(newTipEntry.ChainWork, bestTipEntry.ChainWork) <= 0 {
		for _, step := range steps {
			hash := consensus.HeaderHash(&step.block.Header)
			entry := findBranchEntry(connectedEntries, hash)
			if entry == nil {
				return consensus.BlockValidationSummary{}, fmt.Errorf("missing connected entry for block %x", hash)
			}
			if err := p.store.PutValidatedBlock(&step.block, entry, undoByHash[hash]); err != nil {
				return consensus.BlockValidationSummary{}, err
			}
		}
		return summary, nil
	}

	nextStateMeta, err := tempState.StoredStateMeta()
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	isActiveTipExtension := forkHeight == oldTipHeightForState(p.state) && len(steps) == 1 && parentEntry.Height == bestTipEntry.Height &&
		consensus.HeaderHash(&parentEntry.Header) == bestTipHash
	oldTipHeight := *p.state.TipHeight()
	if isActiveTipExtension {
		undo := undoByHash[blockHash]
		spent, created := activeTipDelta(undo, createdByHash[blockHash])
		if err := p.store.AppendValidatedBlock(nextStateMeta, block, &newTipEntry, undo, spent, created); err != nil {
			return consensus.BlockValidationSummary{}, err
		}
	} else {
		for _, step := range steps {
			hash := consensus.HeaderHash(&step.block.Header)
			entry := findBranchEntry(connectedEntries, hash)
			if entry == nil {
				return consensus.BlockValidationSummary{}, fmt.Errorf("missing connected entry for block %x", hash)
			}
			if err := p.store.PutValidatedBlock(&step.block, entry, undoByHash[hash]); err != nil {
				return consensus.BlockValidationSummary{}, err
			}
		}
		currentStateMeta, err := p.state.StoredStateMeta()
		if err != nil {
			return consensus.BlockValidationSummary{}, err
		}
		currentStateMeta.UTXOs = p.state.UTXOs()
		nextStateMeta.UTXOs = tempState.UTXOs()
		if err := p.store.RewriteFullStateDelta(currentStateMeta, nextStateMeta); err != nil {
			return consensus.BlockValidationSummary{}, err
		}
		if err := p.store.RewriteActiveHeights(forkHeight, oldTipHeight, connectedEntries); err != nil {
			return consensus.BlockValidationSummary{}, err
		}
	}
	p.state = tempState
	return summary, nil
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

func (p *PersistentChainState) evaluateBranch(steps []branchStep, forkHeight uint64) (*ChainState, []storage.BlockIndexEntry, map[[32]byte][]storage.BlockUndoEntry, map[[32]byte]map[types.OutPoint]consensus.UtxoEntry, consensus.BlockValidationSummary, error) {
	tempState := p.state.Clone()
	if err := p.disconnectToHeight(tempState, forkHeight); err != nil {
		return nil, nil, nil, nil, consensus.BlockValidationSummary{}, err
	}

	entries := make([]storage.BlockIndexEntry, 0, len(steps))
	undoByHash := make(map[[32]byte][]storage.BlockUndoEntry, len(steps))
	createdByHash := make(map[[32]byte]map[types.OutPoint]consensus.UtxoEntry, len(steps))
	var summary consensus.BlockValidationSummary
	for _, step := range steps {
		undo, err := captureUndoEntries(&step.block, tempState.UTXOs())
		if err != nil {
			return nil, nil, nil, nil, consensus.BlockValidationSummary{}, err
		}
		detail, err := tempState.applyBlockDetailed(&step.block)
		if err != nil {
			return nil, nil, nil, nil, consensus.BlockValidationSummary{}, err
		}
		summary = detail.summary
		entry := step.entry
		entry.Height = summary.Height
		entry.Validated = true
		entry.BlockSizeState = tempState.BlockSizeState()
		hash := consensus.HeaderHash(&step.block.Header)
		entries = append(entries, entry)
		undoByHash[hash] = undo
		createdByHash[hash] = detail.createdUTXO
	}
	return tempState, entries, undoByHash, createdByHash, summary, nil
}

func (p *PersistentChainState) disconnectToHeight(state *ChainState, targetHeight uint64) error {
	workingUtxos := consensus.NewUtxoOverlay(state.utxos)
	workingAcc := state.utxoAcc
	if workingAcc == nil {
		var err error
		workingAcc, err = consensus.UtxoAccumulator(state.utxos)
		if err != nil {
			return err
		}
	}
	for state.TipHeight() != nil && *state.TipHeight() > targetHeight {
		height := *state.TipHeight()
		hash, err := p.store.GetBlockHashByHeight(height)
		if err != nil {
			return err
		}
		if hash == nil {
			return fmt.Errorf("missing active hash at height %d", height)
		}
		block, err := p.store.GetBlock(hash)
		if err != nil {
			return err
		}
		if block == nil {
			return fmt.Errorf("missing active block %x", *hash)
		}
		undo, err := p.store.GetUndo(hash)
		if err != nil {
			return err
		}
		parentEntry, err := p.store.GetBlockIndex(&block.Header.PrevBlockHash)
		if err != nil {
			return err
		}
		if parentEntry == nil {
			return fmt.Errorf("missing parent entry for active block %x", *hash)
		}
		workingAcc, err = disconnectBlockOverlay(workingUtxos, workingAcc, block, undo)
		if err != nil {
			return err
		}
		parentHeight := parentEntry.Height
		parentHeader := parentEntry.Header
		state.height = &parentHeight
		state.tipHeader = &parentHeader
		state.blockSizeState = parentEntry.BlockSizeState
	}
	state.utxos = workingUtxos.Materialize()
	state.utxoAcc = workingAcc
	if state.tipHeader != nil {
		recentTimes, err := loadIndexedAncestorTimestamps(p.store, consensus.HeaderHash(state.tipHeader), 11)
		if err != nil {
			return err
		}
		state.recentTimes = recentTimes
	}
	return nil
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
			restoredLeaves = append(restoredLeaves, utreexo.UtxoLeaf{
				OutPoint:   entry.OutPoint,
				ValueAtoms: entry.Entry.ValueAtoms,
				PubKey:     entry.Entry.PubKey,
			})
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

func captureUndoEntries(block *types.Block, utxos consensus.UtxoSet) ([]storage.BlockUndoEntry, error) {
	undo := make([]storage.BlockUndoEntry, 0)
	preBlock := consensus.LookupFromSet(utxos)
	tempUtxos := consensus.NewUtxoOverlay(utxos)
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		txid := consensus.TxID(tx)
		for _, input := range block.Txs[i].Base.Inputs {
			entry, ok := tempUtxos.Lookup(input.PrevOut)
			if !ok {
				return nil, fmt.Errorf("missing utxo for undo capture: %v", input.PrevOut)
			}
			// Only spends that reach into the pre-block UTXO set need an undo record.
			// Same-block dependency edges are rewound by deleting this block's outputs.
			if _, existed := preBlock(input.PrevOut); existed {
				undo = append(undo, storage.BlockUndoEntry{OutPoint: input.PrevOut, Entry: entry})
			}
			tempUtxos.Spend(input.PrevOut)
		}
		for vout, output := range tx.Base.Outputs {
			tempUtxos.Set(types.OutPoint{TxID: txid, Vout: uint32(vout)}, consensus.UtxoEntry{
				ValueAtoms: output.ValueAtoms,
				PubKey:     output.PubKey,
			})
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

func reverseBranchSteps(steps []branchStep) {
	for left, right := 0, len(steps)-1; left < right; left, right = left+1, right-1 {
		steps[left], steps[right] = steps[right], steps[left]
	}
}

func findBranchEntry(entries []storage.BlockIndexEntry, hash [32]byte) *storage.BlockIndexEntry {
	for i := range entries {
		if consensus.HeaderHash(&entries[i].Header) == hash {
			return &entries[i]
		}
	}
	return nil
}
