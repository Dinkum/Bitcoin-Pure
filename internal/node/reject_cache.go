package node

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/types"
)

const (
	txRejectCacheCapacity     = 4096
	txRejectCachePermanentTTL = 10 * time.Minute
	txRejectCacheTemporaryTTL = 30 * time.Second
)

type txRejectClass uint8

const (
	txRejectClassNone txRejectClass = iota
	txRejectClassPermanent
	txRejectClassTemporary
)

type txRejectCacheRecord struct {
	relayID   [32]byte
	txID      [32]byte
	class     txRejectClass
	err       error
	expiresAt time.Time
	poolEpoch uint64
	tipHash   [32]byte
}

type txRejectCacheEntry struct {
	key    [32]byte
	record txRejectCacheRecord
}

type txRejectCacheStats struct {
	Entries int
	Hits    uint64
	Misses  uint64
}

type txRejectCache struct {
	mu           sync.Mutex
	capacity     int
	permanentTTL time.Duration
	temporaryTTL time.Duration
	now          func() time.Time
	entries      map[[32]byte]*list.Element
	order        *list.List
	hits         uint64
	misses       uint64
}

func newTxRejectCache(capacity int, permanentTTL, temporaryTTL time.Duration) *txRejectCache {
	if capacity <= 0 {
		capacity = txRejectCacheCapacity
	}
	if permanentTTL <= 0 {
		permanentTTL = txRejectCachePermanentTTL
	}
	if temporaryTTL <= 0 {
		temporaryTTL = txRejectCacheTemporaryTTL
	}
	return &txRejectCache{
		capacity:     capacity,
		permanentTTL: permanentTTL,
		temporaryTTL: temporaryTTL,
		now:          time.Now,
		entries:      make(map[[32]byte]*list.Element, capacity),
		order:        list.New(),
	}
}

func (c *txRejectCache) lookup(tx types.Transaction, poolEpoch uint64, tipHash [32]byte) error {
	if c == nil {
		return nil
	}
	key := relayIDForTx(tx)
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()
	elem := c.entries[key]
	if elem == nil {
		c.misses++
		return nil
	}
	entry := elem.Value.(*txRejectCacheEntry)
	if !entry.record.valid(now, poolEpoch, tipHash) {
		c.removeElementLocked(elem)
		c.misses++
		return nil
	}
	c.order.MoveToBack(elem)
	c.hits++
	return entry.record.err
}

func (c *txRejectCache) remember(tx types.Transaction, err error, poolEpoch uint64, tipHash [32]byte) {
	if c == nil || err == nil {
		return
	}
	class := classifyTxReject(err)
	if class == txRejectClassNone {
		return
	}
	key := relayIDForTx(tx)
	record := txRejectCacheRecord{
		relayID:   key,
		txID:      consensus.TxID(&tx),
		class:     class,
		err:       err,
		expiresAt: c.expiryForClass(class),
		poolEpoch: poolEpoch,
		tipHash:   tipHash,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.entries[key]; elem != nil {
		entry := elem.Value.(*txRejectCacheEntry)
		entry.record = record
		c.order.MoveToBack(elem)
		return
	}
	for len(c.entries) >= c.capacity {
		c.evictOldestLocked()
	}
	elem := c.order.PushBack(&txRejectCacheEntry{key: key, record: record})
	c.entries[key] = elem
}

func (c *txRejectCache) snapshot() txRejectCacheStats {
	if c == nil {
		return txRejectCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return txRejectCacheStats{
		Entries: len(c.entries),
		Hits:    c.hits,
		Misses:  c.misses,
	}
}

func (c *txRejectCache) expiryForClass(class txRejectClass) time.Time {
	ttl := c.permanentTTL
	if class == txRejectClassTemporary {
		ttl = c.temporaryTTL
	}
	return c.now().Add(ttl)
}

func (r txRejectCacheRecord) valid(now time.Time, poolEpoch uint64, tipHash [32]byte) bool {
	if now.After(r.expiresAt) {
		return false
	}
	if r.class != txRejectClassTemporary {
		return true
	}
	return r.poolEpoch == poolEpoch && r.tipHash == tipHash
}

func (c *txRejectCache) evictOldestLocked() {
	elem := c.order.Front()
	if elem == nil {
		return
	}
	c.removeElementLocked(elem)
}

func (c *txRejectCache) removeElementLocked(elem *list.Element) {
	if elem == nil {
		return
	}
	entry := elem.Value.(*txRejectCacheEntry)
	delete(c.entries, entry.key)
	c.order.Remove(elem)
}

func relayIDForTx(tx types.Transaction) [32]byte {
	txid := consensus.TxID(&tx)
	authid := consensus.AuthID(&tx)
	var payload [64]byte
	copy(payload[:32], txid[:])
	copy(payload[32:], authid[:])
	// Relay-side caches need the full transaction identity, not just txid.
	return crypto.TaggedHash("BPU/RelayIDV1", payload[:])
}

func classifyTxReject(err error) txRejectClass {
	switch {
	case err == nil:
		return txRejectClassNone
	case errors.Is(err, mempool.ErrRelayFeeTooLow),
		errors.Is(err, mempool.ErrMempoolFull),
		errors.Is(err, mempool.ErrTooManyAncestors),
		errors.Is(err, mempool.ErrTooManyDescendants),
		errors.Is(err, mempool.ErrInputAlreadySpent),
		errors.Is(err, ErrAvalancheFinalConflict):
		return txRejectClassTemporary
	case errors.Is(err, mempool.ErrTxAlreadyExists):
		return txRejectClassNone
	case errors.Is(err, consensus.ErrMissingUTXO):
		return txRejectClassNone
	default:
		return txRejectClassPermanent
	}
}
