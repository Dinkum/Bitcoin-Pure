package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"fmt"
	"time"
)

// stateMu protects a locally mined header decision through its block commit.
// Peer blocks already have indexed headers and preserve that selected branch.
func (s *Service) applyBlockAndHeaderLocked(block *types.Block) (consensus.BlockValidationSummary, time.Duration, committedBranchTransition, error) {
	next, update, err := s.prepareBlockHeaderUpdateLocked(block)
	if err != nil {
		return consensus.BlockValidationSummary{}, 0, committedBranchTransition{}, err
	}
	summary, wait, transition, err := s.chainState.applyBlockWithHeaderUpdate(block, update)
	if err != nil {
		return summary, wait, transition, err
	}
	s.headerChain = next
	return summary, wait, transition, nil
}

func (s *Service) prepareBlockHeaderUpdateLocked(block *types.Block) (*HeaderChain, *storage.HeaderChainUpdate, error) {
	if s.headerChain.TipHeader() == nil {
		return nil, nil, ErrNoTip
	}
	store := s.chainState.Store()
	parent, err := store.GetBlockIndex(&block.Header.PrevBlockHash)
	if err != nil {
		return nil, nil, err
	}
	if parent == nil {
		return nil, nil, ErrUnknownParent
	}
	work, err := consensus.BlockWork(block.Header.NBits)
	if err != nil {
		return nil, nil, err
	}
	entry := storage.BlockIndexEntry{Height: parent.Height + 1, Header: block.Header, ParentHash: block.Header.PrevBlockHash, ChainWork: consensus.AddChainWork(parent.ChainWork, work)}
	currentHash := consensus.HeaderHash(s.headerChain.TipHeader())
	current, err := store.GetBlockIndex(&currentHash)
	if err != nil {
		return nil, nil, err
	}
	if current == nil {
		return nil, nil, fmt.Errorf("missing header tip %x", currentHash)
	}
	update := &storage.HeaderChainUpdate{}
	if consensus.CompareChainWork(entry.ChainWork, current.ChainWork) <= 0 {
		return s.headerChain, update, nil
	}
	hash := consensus.HeaderHash(&block.Header)
	entries, fork, err := s.headerBranchEntriesLocked(entry, map[[32]byte]storage.BlockIndexEntry{hash: entry})
	if err != nil {
		return nil, nil, err
	}
	next := NewHeaderChain(s.headerChain.Profile()).WithLogger(s.headerChain.logger)
	next.SetSkipPow(s.headerChain.SkipPow())
	if err := next.InitializeTip(entry.Height, block.Header); err != nil {
		return nil, nil, err
	}
	times, err := loadIndexedAncestorTimestamps(store, block.Header.PrevBlockHash, 10)
	if err != nil {
		return nil, nil, err
	}
	next.recentTimes = appendRecentTime(times, block.Header.Timestamp)
	update.State = &storage.StoredHeaderState{Profile: next.Profile(), Height: entry.Height, TipHeader: block.Header}
	update.Entries = entries
	update.ForkHeight = fork
	update.OldTipHeight = current.Height
	return next, update, nil
}
