package node

import (
	"sync"

	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/mempool"
)

const validAuthCacheCapacity = 262_144

type validAuthCacheKey struct {
	txid     [32]byte
	authid   [32]byte
	paramTag [32]byte
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
		max:   max,
		order: make([]validAuthCacheKey, 0, max),
		items: make(map[validAuthCacheKey]struct{}, max),
	}
}

func validAuthParamTag(params consensus.ChainParams) [32]byte {
	return bpcrypto.Sha256d([]byte(params.SighashTag()))
}

func (c *validAuthCache) remember(txid, authid [32]byte, params consensus.ChainParams) {
	if c == nil || txid == ([32]byte{}) || authid == ([32]byte{}) {
		return
	}
	key := validAuthCacheKey{txid: txid, authid: authid, paramTag: validAuthParamTag(params)}
	c.mu.Lock()
	defer c.mu.Unlock()
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
	key := validAuthCacheKey{txid: txid, authid: authid, paramTag: validAuthParamTag(params)}
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
