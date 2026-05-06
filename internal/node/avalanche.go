package node

import (
	"encoding/hex"
	"errors"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
)

const (
	avalancheConflictTTL      = 10 * time.Minute
	avalanchePruneInterval    = 30 * time.Second
	avalancheMaxPollOutpoints = 64
)

var ErrAvalancheFinalConflict = errors.New("transaction conflicts with avalanche-finalized preference")

type AvalancheInfo struct {
	Enabled                bool   `json:"enabled"`
	Mode                   string `json:"mode"`
	Weighting              string `json:"weighting"`
	KSample                int    `json:"k_sample"`
	AlphaNumerator         int    `json:"alpha_numerator"`
	AlphaDenominator       int    `json:"alpha_denominator"`
	Beta                   int    `json:"beta"`
	PollIntervalMS         int64  `json:"poll_interval_ms"`
	TrackedConflictSets    int    `json:"tracked_conflict_sets"`
	FinalizedConflictSets  int    `json:"finalized_conflict_sets"`
	TrackedTransactions    int    `json:"tracked_transactions"`
	InFlightPolls          int    `json:"inflight_polls"`
	PollsStarted           uint64 `json:"polls_started"`
	PollsFinalized         uint64 `json:"polls_finalized"`
	VoteResponses          uint64 `json:"vote_responses"`
	RejectedFinalConflicts uint64 `json:"rejected_final_conflicts"`
}

type avalancheManager struct {
	svc *Service

	mu        sync.Mutex
	rng       *rand.Rand
	conflicts map[types.OutPoint]*avalancheConflictSet
	txInputs  map[[32]byte][]types.OutPoint
	polls     map[uint64]*avalanchePollState
	stats     avalancheStats
	nextPrune time.Time
}

type avalancheConflictSet struct {
	members     map[[32]byte]struct{}
	pref        [32]byte
	hasPref     bool
	counter     map[[32]byte]int
	final       map[[32]byte]bool
	lastSuccess bool
	updatedAt   time.Time
}

type avalanchePollState struct {
	items     []types.OutPoint
	peers     map[string]uint64
	responses map[string][]p2p.AvaVote
	createdAt time.Time
}

type avalancheStats struct {
	pollsStarted           uint64
	pollsFinalized         uint64
	voteResponses          uint64
	rejectedFinalConflicts uint64
}

func newAvalancheManager(svc *Service) *avalancheManager {
	return &avalancheManager{
		svc:       svc,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		conflicts: make(map[types.OutPoint]*avalancheConflictSet),
		txInputs:  make(map[[32]byte][]types.OutPoint),
		polls:     make(map[uint64]*avalanchePollState),
	}
}

func avalancheEnabled(mode string) bool {
	return strings.TrimSpace(strings.ToLower(mode)) != "off"
}

func avalancheModeString(mode string) string {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		return "on"
	}
	return mode
}

func (m *avalancheManager) enabled() bool {
	return m != nil && avalancheEnabled(m.svc.cfg.AvalancheMode)
}

func (m *avalancheManager) info() AvalancheInfo {
	if m == nil {
		return AvalancheInfo{Enabled: false, Mode: "off", Weighting: "disabled"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	finalized := 0
	for _, set := range m.conflicts {
		if set.hasAnyFinal() {
			finalized++
		}
	}
	return AvalancheInfo{
		Enabled:                m.enabled(),
		Mode:                   avalancheModeString(m.svc.cfg.AvalancheMode),
		Weighting:              "equal_peer_weight_initial",
		KSample:                m.svc.cfg.AvalancheKSample,
		AlphaNumerator:         m.svc.cfg.AvalancheAlphaNumerator,
		AlphaDenominator:       m.svc.cfg.AvalancheAlphaDenominator,
		Beta:                   m.svc.cfg.AvalancheBeta,
		PollIntervalMS:         m.svc.cfg.AvalanchePollInterval.Milliseconds(),
		TrackedConflictSets:    len(m.conflicts),
		FinalizedConflictSets:  finalized,
		TrackedTransactions:    len(m.txInputs),
		InFlightPolls:          len(m.polls),
		PollsStarted:           m.stats.pollsStarted,
		PollsFinalized:         m.stats.pollsFinalized,
		VoteResponses:          m.stats.voteResponses,
		RejectedFinalConflicts: m.stats.rejectedFinalConflicts,
	}
}

func (m *avalancheManager) rejectionError(tx types.Transaction) error {
	if !m.enabled() {
		return nil
	}
	txid := consensus.TxID(&tx)
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredIfDueLocked(now)
	for _, input := range tx.Base.Inputs {
		set := m.conflicts[input.PrevOut]
		if set == nil || now.Sub(set.updatedAt) > avalancheConflictTTL {
			continue
		}
		for winner, final := range set.final {
			if !final || winner == txid {
				continue
			}
			m.stats.rejectedFinalConflicts++
			return ErrAvalancheFinalConflict
		}
	}
	return nil
}

func (m *avalancheManager) noteAcceptedAdmissions(admissions []mempool.Admission) {
	if !m.enabled() {
		return
	}
	now := time.Now()
	for _, admission := range admissions {
		if len(admission.Accepted) == 0 {
			continue
		}
		for _, accepted := range admission.Accepted {
			m.trackTx(accepted.Tx, true, now)
		}
	}
}

func (m *avalancheManager) noteRejectedConflicts(txs []types.Transaction, errs []error) {
	if !m.enabled() {
		return
	}
	now := time.Now()
	for i, err := range errs {
		if !errors.Is(err, mempool.ErrInputAlreadySpent) {
			continue
		}
		m.trackTx(txs[i], false, now)
	}
}

func (m *avalancheManager) onBlockAccepted(block *types.Block) {
	if !m.enabled() || block == nil {
		return
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 1; i < len(block.Txs); i++ {
		tx := block.Txs[i]
		txid := consensus.TxID(&tx)
		inputs := make([]types.OutPoint, 0, len(tx.Base.Inputs))
		for _, input := range tx.Base.Inputs {
			inputs = append(inputs, input.PrevOut)
			set := m.ensureConflictSetLocked(input.PrevOut, now)
			set.members = map[[32]byte]struct{}{txid: {}}
			set.pref = txid
			set.hasPref = true
			set.counter = map[[32]byte]int{txid: 0}
			set.final = make(map[[32]byte]bool)
			set.lastSuccess = false
			set.updatedAt = now
		}
		m.txInputs[txid] = inputs
	}
	m.pruneExpiredIfDueLocked(now)
}

func (m *avalancheManager) pollLoop() {
	if !m.enabled() {
		return
	}
	ticker := time.NewTicker(m.svc.cfg.AvalanchePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.svc.stopCh:
			return
		case <-ticker.C:
			m.runPollStep(time.Now())
		}
	}
}

func (m *avalancheManager) runPollStep(now time.Time) {
	if !m.enabled() {
		return
	}
	m.finalizeExpiredPolls(now)
	pollID, items, peers := m.preparePollRound(now)
	if len(items) == 0 || len(peers) == 0 {
		return
	}
	msg := p2p.AvaPollMessage{PollID: pollID, Items: items}
	sentPeers := make(map[string]uint64, len(peers))
	for _, peer := range peers {
		if err := peer.send(msg); err != nil {
			continue
		}
		peer.noteAvalanchePollSent()
		sentPeers[peer.addr] = 1
	}
	if len(sentPeers) == 0 {
		return
	}
	m.mu.Lock()
	m.polls[pollID] = &avalanchePollState{
		items:     items,
		peers:     sentPeers,
		responses: make(map[string][]p2p.AvaVote, len(sentPeers)),
		createdAt: now,
	}
	m.stats.pollsStarted++
	m.mu.Unlock()
}

func (m *avalancheManager) preparePollRound(now time.Time) (uint64, []types.OutPoint, []*peerConn) {
	peers := m.samplePeers()
	if len(peers) == 0 {
		return 0, nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredIfDueLocked(now)
	items := make([]types.OutPoint, 0, minInt(len(m.conflicts), avalancheMaxPollOutpoints))
	for outpoint, set := range m.conflicts {
		if now.Sub(set.updatedAt) > avalancheConflictTTL {
			continue
		}
		if set.hasAnyFinal() {
			continue
		}
		items = append(items, outpoint)
		if len(items) >= avalancheMaxPollOutpoints {
			break
		}
	}
	if len(items) == 0 {
		return 0, nil, nil
	}
	slices.SortFunc(items, func(a, b types.OutPoint) int {
		if cmp := strings.Compare(hex.EncodeToString(a.TxID[:]), hex.EncodeToString(b.TxID[:])); cmp != 0 {
			return cmp
		}
		switch {
		case a.Vout < b.Vout:
			return -1
		case a.Vout > b.Vout:
			return 1
		default:
			return 0
		}
	})
	return randomNonce(), items, peers
}

func (m *avalancheManager) samplePeers() []*peerConn {
	peers := m.svc.peerSnapshot()
	candidates := make([]*peerConn, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || !peer.supportsAvalancheOverlay() {
			continue
		}
		candidates = append(candidates, peer)
	}
	if len(candidates) <= m.svc.cfg.AvalancheKSample {
		slices.SortFunc(candidates, func(a, b *peerConn) int { return strings.Compare(a.addr, b.addr) })
		return candidates
	}
	m.mu.Lock()
	m.rng.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	m.mu.Unlock()
	candidates = candidates[:m.svc.cfg.AvalancheKSample]
	slices.SortFunc(candidates, func(a, b *peerConn) int { return strings.Compare(a.addr, b.addr) })
	return candidates
}

func (m *avalancheManager) onPoll(peer *peerConn, msg p2p.AvaPollMessage) error {
	if !m.enabled() {
		return nil
	}
	peer.noteAvalanchePollReceived()
	votes := make([]p2p.AvaVote, 0, len(msg.Items))
	m.mu.Lock()
	for _, item := range msg.Items {
		vote := p2p.AvaVote{}
		if set := m.conflicts[item]; set != nil && set.hasPref {
			vote.HasOpinion = true
			vote.TxID = set.pref
		}
		votes = append(votes, vote)
	}
	m.mu.Unlock()
	peer.noteAvalancheVoteSent()
	return peer.send(p2p.AvaVoteMessage{PollID: msg.PollID, Votes: votes})
}

func (m *avalancheManager) onVote(peer *peerConn, msg p2p.AvaVoteMessage) {
	if !m.enabled() {
		return
	}
	peer.noteAvalancheVoteReceived()
	now := time.Now()
	m.mu.Lock()
	state := m.polls[msg.PollID]
	if state == nil {
		m.mu.Unlock()
		return
	}
	if _, ok := state.peers[peer.addr]; !ok || len(msg.Votes) != len(state.items) {
		m.mu.Unlock()
		return
	}
	if _, seen := state.responses[peer.addr]; seen {
		m.mu.Unlock()
		return
	}
	state.responses[peer.addr] = append([]p2p.AvaVote(nil), msg.Votes...)
	m.stats.voteResponses++
	ready := len(state.responses) >= len(state.peers)
	if !ready {
		m.mu.Unlock()
		return
	}
	delete(m.polls, msg.PollID)
	m.applyPollResultsLocked(state, now)
	m.stats.pollsFinalized++
	m.mu.Unlock()
}

func (m *avalancheManager) finalizeExpiredPolls(now time.Time) {
	if !m.enabled() {
		return
	}
	timeout := m.svc.cfg.AvalanchePollInterval
	m.mu.Lock()
	defer m.mu.Unlock()
	for pollID, state := range m.polls {
		if now.Sub(state.createdAt) < timeout {
			continue
		}
		delete(m.polls, pollID)
		if len(state.responses) == 0 {
			continue
		}
		m.applyPollResultsLocked(state, now)
		m.stats.pollsFinalized++
	}
}

func (m *avalancheManager) applyPollResultsLocked(state *avalanchePollState, now time.Time) {
	totalWeight := uint64(0)
	for _, weight := range state.peers {
		totalWeight += weight
	}
	if totalWeight == 0 {
		return
	}
	for itemIdx, outpoint := range state.items {
		set := m.conflicts[outpoint]
		if set == nil {
			continue
		}
		weights := make(map[[32]byte]uint64)
		for _, votes := range state.responses {
			if itemIdx >= len(votes) || !votes[itemIdx].HasOpinion {
				continue
			}
			txid := votes[itemIdx].TxID
			if _, ok := set.members[txid]; !ok {
				continue
			}
			weights[txid]++
		}
		winner, successful := m.pickWinningColor(set, weights, totalWeight)
		if !successful {
			set.lastSuccess = false
			continue
		}
		if set.hasPref && set.pref == winner {
			if set.lastSuccess {
				set.counter[winner]++
			} else {
				set.counter[winner] = 1
			}
		} else {
			set.pref = winner
			set.hasPref = true
			set.counter[winner] = 1
		}
		set.lastSuccess = true
		set.updatedAt = now
		if set.counter[winner] >= m.svc.cfg.AvalancheBeta {
			set.final[winner] = true
		}
	}
}

func (m *avalancheManager) pickWinningColor(set *avalancheConflictSet, weights map[[32]byte]uint64, totalWeight uint64) ([32]byte, bool) {
	var winner [32]byte
	if len(weights) == 0 {
		return winner, false
	}
	maxWeight := uint64(0)
	tied := make([][32]byte, 0, len(weights))
	for txid, weight := range weights {
		switch {
		case weight > maxWeight:
			maxWeight = weight
			tied = tied[:0]
			tied = append(tied, txid)
			winner = txid
		case weight == maxWeight:
			tied = append(tied, txid)
		}
	}
	if len(tied) > 1 {
		if set.hasPref {
			for _, txid := range tied {
				if txid == set.pref {
					winner = txid
					goto threshold
				}
			}
		}
		return [32]byte{}, false
	}
threshold:
	if maxWeight*uint64(m.svc.cfg.AvalancheAlphaDenominator) < uint64(m.svc.cfg.AvalancheAlphaNumerator)*totalWeight {
		return [32]byte{}, false
	}
	return winner, true
}

func (m *avalancheManager) trackTx(tx types.Transaction, preferred bool, now time.Time) {
	if !m.enabled() || len(tx.Base.Inputs) == 0 {
		return
	}
	txid := consensus.TxID(&tx)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredIfDueLocked(now)
	inputs := make([]types.OutPoint, 0, len(tx.Base.Inputs))
	for _, input := range tx.Base.Inputs {
		inputs = append(inputs, input.PrevOut)
		set := m.ensureConflictSetLocked(input.PrevOut, now)
		set.members[txid] = struct{}{}
		if !set.hasPref || preferred {
			set.pref = txid
			set.hasPref = true
		}
		set.updatedAt = now
	}
	m.txInputs[txid] = inputs
}

func (m *avalancheManager) ensureConflictSetLocked(outpoint types.OutPoint, now time.Time) *avalancheConflictSet {
	set := m.conflicts[outpoint]
	if set != nil {
		return set
	}
	set = &avalancheConflictSet{
		members:   make(map[[32]byte]struct{}),
		counter:   make(map[[32]byte]int),
		final:     make(map[[32]byte]bool),
		updatedAt: now,
	}
	m.conflicts[outpoint] = set
	return set
}

func (m *avalancheManager) pruneExpiredLocked(now time.Time) {
	for outpoint, set := range m.conflicts {
		if now.Sub(set.updatedAt) <= avalancheConflictTTL {
			continue
		}
		delete(m.conflicts, outpoint)
	}
}

func (m *avalancheManager) pruneExpiredIfDueLocked(now time.Time) {
	if !m.nextPrune.IsZero() && now.Before(m.nextPrune) {
		return
	}
	m.pruneExpiredLocked(now)
	m.nextPrune = now.Add(avalanchePruneInterval)
}

func (s *avalancheConflictSet) hasAnyFinal() bool {
	for _, final := range s.final {
		if final {
			return true
		}
	}
	return false
}

func (m *avalancheManager) metricsForMempool() (int, int) {
	if m == nil || !m.enabled() {
		return 0, 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	finalized := 0
	for _, set := range m.conflicts {
		if set.hasAnyFinal() {
			finalized++
		}
	}
	return len(m.conflicts), finalized
}

func (m *avalancheManager) notePowOverride() {
	if m == nil || !m.enabled() {
		return
	}
	m.svc.logger.Warn("avalanche state overridden by selected pow chain")
}

func (m *avalancheManager) logConfig() {
	if m == nil || !m.enabled() {
		return
	}
	m.svc.logger.Info("avalanche overlay enabled",
		slog.String("weighting", "equal_peer_weight_initial"),
		slog.Int("k_sample", m.svc.cfg.AvalancheKSample),
		slog.Int("alpha_numerator", m.svc.cfg.AvalancheAlphaNumerator),
		slog.Int("alpha_denominator", m.svc.cfg.AvalancheAlphaDenominator),
		slog.Int("beta", m.svc.cfg.AvalancheBeta),
		slog.Duration("poll_interval", m.svc.cfg.AvalanchePollInterval),
	)
}
