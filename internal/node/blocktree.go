package node

import (
	"fmt"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
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
	out := NewChainState(c.params.Profile).WithRules(c.rules)
	if c.height != nil && c.tipHeader != nil {
		height := *c.height
		header := *c.tipHeader
		out.height = &height
		out.tipHeader = &header
	}
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
	for _, tx := range block.Txs {
		txid := consensus.TxID(&tx)
		for vout := range tx.Base.Outputs {
			workingUtxos.Spend(types.OutPoint{TxID: txid, Vout: uint32(vout)})
		}
	}

	undoIndex := 0
	// Outputs created earlier in the same block are deleted above and should not be
	// restored from undo; only spends that reached into the pre-block UTXO set
	// consume undo entries.
	intraBlockOutputs := make(map[types.OutPoint]struct{})
	for i := 1; i < len(block.Txs); i++ {
		for _, input := range block.Txs[i].Base.Inputs {
			if _, ok := intraBlockOutputs[input.PrevOut]; ok {
				continue
			}
			if undoIndex >= len(undo) {
				return fmt.Errorf("block undo mismatch: missing undo entry for input %v", input.PrevOut)
			}
			entry := undo[undoIndex]
			undoIndex++
			if entry.OutPoint != input.PrevOut {
				return fmt.Errorf("undo outpoint mismatch for input %v", input.PrevOut)
			}
			workingUtxos.Restore(input.PrevOut, entry.Entry)
		}
		txid := consensus.TxID(&block.Txs[i])
		for vout := range block.Txs[i].Base.Outputs {
			intraBlockOutputs[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = struct{}{}
		}
	}
	if undoIndex != len(undo) {
		return fmt.Errorf("block undo mismatch: unused undo entries %d", len(undo)-undoIndex)
	}

	height := parent.Height
	header := parent.Header
	c.height = &height
	c.tipHeader = &header
	c.blockSizeState = parent.BlockSizeState
	c.utxos = workingUtxos.Materialize()
	acc, err := consensus.UtxoAccumulator(c.utxos)
	if err != nil {
		return err
	}
	c.utxoAcc = acc
	return nil
}

func (p *PersistentChainState) ApplyBlock(block *types.Block) (consensus.BlockValidationSummary, error) {
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
	tempState, connectedEntries, undoByHash, summary, err := p.evaluateBranch(steps, forkHeight)
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}

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
		return summary, nil
	}

	storedState, err := tempState.StoredState()
	if err != nil {
		return consensus.BlockValidationSummary{}, err
	}
	isActiveTipExtension := forkHeight == oldTipHeightForState(p.state) && len(steps) == 1 && parentEntry.Height == bestTipEntry.Height &&
		consensus.HeaderHash(&parentEntry.Header) == bestTipHash
	oldTipHeight := *p.state.TipHeight()
	if isActiveTipExtension {
		undo := undoByHash[blockHash]
		spent, created := activeTipDelta(block, storedState.UTXOs, undo)
		if err := p.store.AppendValidatedBlock(storedState, block, &newTipEntry, undo, spent, created); err != nil {
			return consensus.BlockValidationSummary{}, err
		}
	} else {
		if err := p.store.WriteFullState(storedState); err != nil {
			return consensus.BlockValidationSummary{}, err
		}
		if err := p.store.RewriteActiveHeights(forkHeight, oldTipHeight, connectedEntries); err != nil {
			return consensus.BlockValidationSummary{}, err
		}
	}
	p.state = tempState
	return summary, nil
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

func (p *PersistentChainState) evaluateBranch(steps []branchStep, forkHeight uint64) (*ChainState, []storage.BlockIndexEntry, map[[32]byte][]storage.BlockUndoEntry, consensus.BlockValidationSummary, error) {
	tempState := p.state.Clone()
	if err := p.disconnectToHeight(tempState, forkHeight); err != nil {
		return nil, nil, nil, consensus.BlockValidationSummary{}, err
	}

	entries := make([]storage.BlockIndexEntry, 0, len(steps))
	undoByHash := make(map[[32]byte][]storage.BlockUndoEntry, len(steps))
	var summary consensus.BlockValidationSummary
	for _, step := range steps {
		undo, err := captureUndoEntries(&step.block, tempState.UTXOs())
		if err != nil {
			return nil, nil, nil, consensus.BlockValidationSummary{}, err
		}
		summary, err = tempState.ApplyBlock(&step.block)
		if err != nil {
			return nil, nil, nil, consensus.BlockValidationSummary{}, err
		}
		entry := step.entry
		entry.Height = summary.Height
		entry.Validated = true
		entry.BlockSizeState = tempState.BlockSizeState()
		hash := consensus.HeaderHash(&step.block.Header)
		entries = append(entries, entry)
		undoByHash[hash] = undo
	}
	return tempState, entries, undoByHash, summary, nil
}

func (p *PersistentChainState) disconnectToHeight(state *ChainState, targetHeight uint64) error {
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
		if err := state.DisconnectBlock(block, undo, parentEntry); err != nil {
			return err
		}
	}
	return nil
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
				KeyHash:    output.KeyHash,
			})
		}
	}
	return undo, nil
}

func activeTipDelta(block *types.Block, finalUTXOs consensus.UtxoSet, undo []storage.BlockUndoEntry) ([]types.OutPoint, map[types.OutPoint]consensus.UtxoEntry) {
	spent := make([]types.OutPoint, 0, len(undo))
	for _, entry := range undo {
		spent = append(spent, entry.OutPoint)
	}
	created := make(map[types.OutPoint]consensus.UtxoEntry)
	for _, tx := range block.Txs {
		txid := consensus.TxID(&tx)
		for vout := range tx.Base.Outputs {
			outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			entry, ok := finalUTXOs[outPoint]
			if ok {
				created[outPoint] = entry
			}
		}
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
