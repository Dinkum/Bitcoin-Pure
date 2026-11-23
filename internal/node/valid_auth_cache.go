package node

import (
	"sync"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/types"
)

const validAuthCacheCapacity = 262_144

type validAuthCacheKey struct {
	txid    [32]byte
	authid  [32]byte
	profile types.ChainProfile
	lineage string
}

type validAuthCache struct {
	mu    sync.RWMutex
	max   int
	next  int
	order []validAuthCacheKey
	items map[validAuthCacheKey]struct{}
}

func newValidAuthCache(max int) *validAuthCache {
	if max <= 0 {
		max = validAuthCacheCapacity
	}
	return &validAuthCache{
		max: max,
	}
}

func (c *validAuthCache) remember(txid, authid [32]byte, params consensus.ChainParams) {
	if c == nil || txid == ([32]byte{}) || authid == ([32]byte{}) {
		return
	}
	key := validAuthCacheKey{txid: txid, authid: authid, profile: params.Profile, lineage: params.ChainLineage}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items == nil {
		c.items = make(map[validAuthCacheKey]struct{})
	}
	if _, ok := c.items[key]; ok {
		return
	}
	if len(c.order) < c.max {
		c.items[key] = struct{}{}
		c.order = append(c.order, key)
		return
	}
	if len(c.order) > 0 {
		evicted := c.order[c.next]
		delete(c.items, evicted)
		c.order[c.next] = key
		c.next = (c.next + 1) % c.max
	}
	c.items[key] = struct{}{}
}

func (c *validAuthCache) contains(txid, authid [32]byte, params consensus.ChainParams) bool {
	if c == nil || txid == ([32]byte{}) || authid == ([32]byte{}) {
		return false
	}
	key := validAuthCacheKey{txid: txid, authid: authid, profile: params.Profile, lineage: params.ChainLineage}
	c.mu.RLock()
	_, ok := c.items[key]
	c.mu.RUnlock()
	return ok
}

func (s *Service) rememberValidAuthAdmissions(admissions []mempool.Admission) {
	if s == nil || s.validAuth == nil || len(admissions) == 0 {
		return
	}
	params := consensus.ParamsForProfile(s.cfg.Profile)
	for _, admission := range admissions {
		for _, accepted := range admission.Accepted {
			authID := accepted.AuthID
			if authID == ([32]byte{}) && len(accepted.Tx.Auth.Entries) > 0 {
				authID = consensus.AuthID(&accepted.Tx)
			}
			s.validAuth.remember(accepted.TxID, authID, params)
		}
	}
}

func (s *Service) hasValidTxAuth(txid, authid [32]byte, params consensus.ChainParams) bool {
	if s == nil || s.validAuth == nil {
		return false
	}
	return s.validAuth.contains(txid, authid, params)
}
