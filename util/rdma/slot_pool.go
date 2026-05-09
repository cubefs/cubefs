// SlotPool: slot-level borrowing on top of one or more RDMAConns.
//
// This file is build-tag-free. Slot accounting (round-robin allocation,
// blocking on full, dirty exclusion) is pure Go and unit-testable on any
// platform. The CGO/RDMA-tagged pool.go (or stub.go) wires the connection
// factory to the real Dial function or returns ErrNotSupported.
//
// Design (P1 of docs/plan/rdma-optimization-spec.md):
//
//   SlotPool
//     ├── RDMAConn A: slot[0..N-1]
//     └── RDMAConn B: slot[0..N-1]
//
// Each conn has per-slot inUse and dirty bitmaps. AcquireSlot rotates
// round-robin within a conn; if all slots in all conns are inUse and
// maxConns is reached, the call blocks on a per-pool sync.Cond until a
// ReleaseSlot Broadcast wakes it. forceClose marks the released slot dirty
// (excluded from future rotation); when every slot of a conn is dirty and
// no goroutine still holds an inUse slot, the conn is closed and removed
// from the pool so it does not occupy a maxConns budget forever.

package rdma

import (
	"errors"
	"fmt"
	"sync"
)

// ErrSlotPoolClosed is returned by AcquireSlot after Close has been called.
var ErrSlotPoolClosed = errors.New("rdma: slot pool closed")

// SlotHandle represents a borrowed slot on a specific connection. Returned
// by AcquireSlot; must be returned via ReleaseSlot.
type SlotHandle struct {
	Conn    *RDMAConn
	SlotIdx int

	// pool is the originating pool; used by ReleaseSlot. Hidden from
	// external users so they cannot accidentally release into a foreign
	// pool.
	pool *singleSlotPool
}

// connSlots tracks slot availability for one RDMAConn.
type connSlots struct {
	conn   *RDMAConn
	inUse  []bool // someone is currently holding the slot
	dirty  []bool // slot was forceClose'd; never rotates back into use
	nextRR int    // next round-robin index to consider
}

func newConnSlots(conn *RDMAConn, numSlots int) *connSlots {
	return &connSlots{
		conn:  conn,
		inUse: make([]bool, numSlots),
		dirty: make([]bool, numSlots),
	}
}

// tryAcquire returns the next free slot index or -1 if none. Caller must
// hold the pool mutex.
func (cs *connSlots) tryAcquire() int {
	n := len(cs.inUse)
	for i := 0; i < n; i++ {
		idx := (cs.nextRR + i) % n
		if !cs.inUse[idx] && !cs.dirty[idx] {
			cs.inUse[idx] = true
			cs.nextRR = (idx + 1) % n
			return idx
		}
	}
	return -1
}

// allDeadAndIdle reports whether every slot is dirty AND no slot is
// currently in use. A connSlots in this state is safe to close: no future
// AcquireSlot can ever return one of its slots, and no goroutine still
// owns an in-flight request on it.
func (cs *connSlots) allDeadAndIdle() bool {
	for i := range cs.dirty {
		if cs.inUse[i] || !cs.dirty[i] {
			return false
		}
	}
	return true
}

// dialFunc constructs an RDMAConn for the given target. Real builds wire
// this to Dial; tests inject a fake.
type dialFunc func(addr string, cfg RDMAConnConfig) (*RDMAConn, error)

// singleSlotPool owns the slot bookkeeping for one remote address.
type singleSlotPool struct {
	addr     string
	cfg      RDMAConnConfig
	maxConns int
	dial     dialFunc

	mu     sync.Mutex
	cond   *sync.Cond
	conns  []*connSlots
	closed bool
}

func newSingleSlotPool(addr string, cfg RDMAConnConfig, maxConns int, dial dialFunc) *singleSlotPool {
	p := &singleSlotPool{
		addr:     addr,
		cfg:      cfg,
		maxConns: maxConns,
		dial:     dial,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// acquire blocks until a slot is available, then returns a handle.
func (p *singleSlotPool) acquire() (*SlotHandle, error) {
	p.mu.Lock()
	for {
		if p.closed {
			p.mu.Unlock()
			return nil, ErrSlotPoolClosed
		}

		// Try to find a free slot among existing conns.
		for _, cs := range p.conns {
			if idx := cs.tryAcquire(); idx >= 0 {
				h := &SlotHandle{Conn: cs.conn, SlotIdx: idx, pool: p}
				p.mu.Unlock()
				return h, nil
			}
		}

		// No free slot. If we still have headroom under maxConns, dial a
		// new connection (outside the lock so a slow Dial doesn't stall
		// other waiters).
		if len(p.conns) < p.maxConns {
			cfg := p.cfg
			addr := p.addr
			dial := p.dial
			p.mu.Unlock()

			conn, err := dial(addr, cfg)

			p.mu.Lock()
			if err != nil {
				p.mu.Unlock()
				return nil, fmt.Errorf("rdma: dial %s: %w", addr, err)
			}
			if p.closed {
				// Pool closed while we were dialing.
				p.mu.Unlock()
				conn.Close()
				return nil, ErrSlotPoolClosed
			}
			p.conns = append(p.conns, newConnSlots(conn, p.cfg.NumSlots))
			continue // retry the allocation loop with the new conn
		}

		// All conns full and maxConns reached. Block until a slot is
		// released (or the pool closes).
		p.cond.Wait()
	}
}

// release returns h to the pool. forceClose=true marks the slot dirty
// (excluded from future allocation) without disturbing other slots on the
// same connection. When every slot of a connection becomes dirty and no
// in-use slot remains, the connection is closed and removed.
func (p *singleSlotPool) release(h *SlotHandle, forceClose bool) {
	p.mu.Lock()
	var toClose *RDMAConn
	for i, cs := range p.conns {
		if cs.conn != h.Conn {
			continue
		}
		if h.SlotIdx >= 0 && h.SlotIdx < len(cs.inUse) {
			cs.inUse[h.SlotIdx] = false
			if forceClose {
				cs.dirty[h.SlotIdx] = true
			}
		}
		if cs.allDeadAndIdle() {
			toClose = cs.conn
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
		}
		break
	}
	p.cond.Broadcast()
	p.mu.Unlock()
	if toClose != nil {
		// Close outside the lock; conn.Close waits on its drainer.
		toClose.Close()
	}
}

// closeAll shuts down every conn in this pool. Pending acquirers are
// woken with ErrSlotPoolClosed.
func (p *singleSlotPool) closeAll() {
	p.mu.Lock()
	p.closed = true
	conns := p.conns
	p.conns = nil
	p.cond.Broadcast()
	p.mu.Unlock()
	for _, cs := range conns {
		cs.conn.Close()
	}
}

// activeSlots returns the number of currently-borrowed slots across all
// conns in this single-target pool. O(numConns × numSlots); used by
// metrics and tests, not on the hot path.
func (p *singleSlotPool) activeSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, cs := range p.conns {
		for _, u := range cs.inUse {
			if u {
				n++
			}
		}
	}
	return n
}

// RDMAConnPool manages slot-level borrowing across multiple remote
// addresses. The name is retained for caller import compatibility but the
// API is now slot-based: the legacy GetConnect / PutConnect have been
// removed in favour of AcquireSlot / ReleaseSlot.
type RDMAConnPool struct {
	mu    sync.RWMutex
	pools map[string]*singleSlotPool
	cfg   RDMAPoolConfig
	dial  dialFunc
}

// newPool constructs a pool with the given Dial function. Build-tagged
// wrappers in pool.go (rdma) and stub.go (non-rdma) call this.
func newPool(cfg RDMAPoolConfig, dial dialFunc) (*RDMAConnPool, error) {
	if cfg.NumSlots <= 0 || cfg.NumSlots > maxSlots {
		return nil, fmt.Errorf("rdma: NumSlots %d out of range [1,%d]", cfg.NumSlots, maxSlots)
	}
	if err := ValidateSlotSize(cfg.SlotSize); err != nil {
		return nil, err
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 4
	}
	return &RDMAConnPool{
		pools: make(map[string]*singleSlotPool),
		cfg:   cfg,
		dial:  dial,
	}, nil
}

// AcquireSlot returns a borrowed slot to addr, blocking until one is
// available. The returned handle MUST be returned via ReleaseSlot or it
// will leak both the slot and any flow-control credit attached to it.
func (p *RDMAConnPool) AcquireSlot(addr string) (*SlotHandle, error) {
	p.mu.RLock()
	sp := p.pools[addr]
	p.mu.RUnlock()

	if sp == nil {
		p.mu.Lock()
		sp = p.pools[addr]
		if sp == nil {
			connCfg := RDMAConnConfig{
				NumSlots:      p.cfg.NumSlots,
				SlotSize:      p.cfg.SlotSize,
				CreditAckMode: p.cfg.CreditAckMode,
				Poll:          p.cfg.Poll,
			}
			sp = newSingleSlotPool(addr, connCfg, p.cfg.MaxConns, p.dial)
			p.pools[addr] = sp
		}
		p.mu.Unlock()
	}
	return sp.acquire()
}

// ReleaseSlot returns h to its originating pool. forceClose=true marks the
// slot as permanently unusable (the operation that ran on it experienced
// a fault we cannot recover from); other slots on the same connection
// remain usable.
func (p *RDMAConnPool) ReleaseSlot(h *SlotHandle, forceClose bool) {
	if h == nil || h.pool == nil {
		return
	}
	h.pool.release(h, forceClose)
}

// Close closes all connections in the pool. Pending AcquireSlot calls are
// woken with ErrSlotPoolClosed.
func (p *RDMAConnPool) Close() {
	p.mu.Lock()
	pools := p.pools
	p.pools = nil
	p.mu.Unlock()
	for _, sp := range pools {
		sp.closeAll()
	}
}

// ActiveSlots returns the total number of borrowed slots across every
// remote address. Used by metrics.
func (p *RDMAConnPool) ActiveSlots() int {
	p.mu.RLock()
	pools := make([]*singleSlotPool, 0, len(p.pools))
	for _, sp := range p.pools {
		pools = append(pools, sp)
	}
	p.mu.RUnlock()
	n := 0
	for _, sp := range pools {
		n += sp.activeSlots()
	}
	return n
}
