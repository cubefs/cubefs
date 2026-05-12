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
	"hash/fnv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util"
)

// ErrSlotPoolClosed is returned by AcquireSlot after Close has been called.
var ErrSlotPoolClosed = errors.New("rdma: slot pool closed")

// fnvHash32 hashes s with FNV-1a 32-bit. Used to pin (PartitionID,
// ExtentID)-style routing keys to a stable conn index in singleSlotPool
// so the same extent always lands on the same QP.
func fnvHash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

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
// hold the pool mutex. Returns -1 also when the underlying connection has
// been marked closed (drainer-detected fault, externally closed) so the
// pool stops handing out slots on a broken QP.
func (cs *connSlots) tryAcquire() int {
	if cs.conn != nil && cs.conn.IsClosed() {
		return -1
	}
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

// allDeadAndIdle reports whether the connSlots is safe to remove:
// either every slot is dirty AND no slot is currently in use, OR the
// underlying conn has been marked closed (e.g. drainer-detected fault)
// and no slot is currently in use. In both cases no future AcquireSlot
// can ever return one of its slots, and no goroutine still owns an
// in-flight request on it.
func (cs *connSlots) allDeadAndIdle() bool {
	for i := range cs.inUse {
		if cs.inUse[i] {
			return false
		}
	}
	if cs.conn != nil && cs.conn.IsClosed() {
		return true
	}
	for i := range cs.dirty {
		if !cs.dirty[i] {
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

	mu   sync.Mutex
	cond *sync.Cond
	// conns is a fixed-size positional array of length maxConns. Index i
	// is the "stable hash slot i": when a caller passes a hashKey to
	// acquire, the conn at i = hash(key) % maxConns is returned
	// deterministically. nil means no conn dialed for that index yet.
	//
	// Stable indexing matters because a leader→follower path that fans
	// the same extent's append writes across multiple conns destroys the
	// strict offset ordering datanode/storage/extent.go requires; routing
	// each (Partition,Extent) to a fixed conn keeps server-side dispatch
	// linearisable. Empty hashKey falls back to round-robin scan over all
	// indices for callers that don't care about ordering.
	conns []*connSlots
	// dialing[i] = true means a single-flight dial is in flight for
	// conns[i]. Replaces the previous integer counter; per-index gating
	// is required because hash routing pins a specific index.
	dialing []bool
	closed  bool
}

func newSingleSlotPool(addr string, cfg RDMAConnConfig, maxConns int, dial dialFunc) *singleSlotPool {
	p := &singleSlotPool{
		addr:     addr,
		cfg:      cfg,
		maxConns: maxConns,
		dial:     dial,
		conns:    make([]*connSlots, maxConns),
		dialing:  make([]bool, maxConns),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// acquire blocks until a slot is available, then returns a handle.
// Records slot_wait_seconds when it had to block (skipped on the fast
// path so the histogram isn't scrubbed with zero observations).
//
// hashKey, when non-empty, pins the call to a deterministic conn index
// (hash(key) % maxConns); same key → same conn → same QP → server
// processes serially → strict ordering preserved. hashKey == "" falls
// back to round-robin across all conns for callers that don't care
// about ordering.
func (p *singleSlotPool) acquire(hashKey string) (*SlotHandle, error) {
	start := time.Now()
	blocked := false

	// indices defines the candidate conn slots for this call.
	var targetIdx int = -1
	if hashKey != "" {
		targetIdx = int(fnvHash32(hashKey)) % p.maxConns
	}

	p.mu.Lock()
	for {
		if p.closed {
			p.mu.Unlock()
			return nil, ErrSlotPoolClosed
		}

		// Try to find a free slot among existing conns. With hash routing
		// (targetIdx>=0) only that one conn is a candidate; otherwise scan
		// all of them (legacy round-robin).
		if targetIdx >= 0 {
			if cs := p.conns[targetIdx]; cs != nil {
				if idx := cs.tryAcquire(); idx >= 0 {
					h := &SlotHandle{Conn: cs.conn, SlotIdx: idx, pool: p}
					active := p.activeSlotsLocked()
					p.mu.Unlock()
					if blocked {
						metricsObserveSlotWait(p.cfg.Role, p.addr, time.Since(start))
					}
					metricsSetActiveSlots(p.cfg.Role, p.addr, active)
					return h, nil
				}
			}
		} else {
			for _, cs := range p.conns {
				if cs == nil {
					continue
				}
				if idx := cs.tryAcquire(); idx >= 0 {
					h := &SlotHandle{Conn: cs.conn, SlotIdx: idx, pool: p}
					active := p.activeSlotsLocked()
					p.mu.Unlock()
					if blocked {
						metricsObserveSlotWait(p.cfg.Role, p.addr, time.Since(start))
					}
					metricsSetActiveSlots(p.cfg.Role, p.addr, active)
					return h, nil
				}
			}
		}

		// Dial a missing conn if one is needed. Hash-routed callers can
		// only ever dial their specific index; round-robin callers pick
		// the first nil index. Single-flight via dialing[i] so concurrent
		// acquirers cooperate on the maxConns cap.
		dialIdx := -1
		if targetIdx >= 0 {
			if p.conns[targetIdx] == nil && !p.dialing[targetIdx] {
				dialIdx = targetIdx
			}
		} else {
			for i := range p.conns {
				if p.conns[i] == nil && !p.dialing[i] {
					dialIdx = i
					break
				}
			}
		}
		if dialIdx >= 0 {
			p.dialing[dialIdx] = true
			cfg := p.cfg
			addr := p.addr
			dial := p.dial
			p.mu.Unlock()

			conn, err := dial(addr, cfg)

			p.mu.Lock()
			p.dialing[dialIdx] = false
			if err != nil {
				// Wake other waiters parked on cond — they may now want
				// to attempt the dial themselves (the dialing flag has
				// cleared).
				p.cond.Broadcast()
				p.mu.Unlock()
				return nil, fmt.Errorf("rdma: dial %s: %w", addr, err)
			}
			if p.closed {
				// Pool closed while we were dialing.
				p.cond.Broadcast()
				p.mu.Unlock()
				conn.Close()
				return nil, ErrSlotPoolClosed
			}
			p.conns[dialIdx] = newConnSlots(conn, p.cfg.NumSlots)
			// New conn brings numSlots free slots; wake other waiters
			// so they can claim slots from the new conn rather than
			// staying parked until the next ReleaseSlot.
			p.cond.Broadcast()
			continue // retry the allocation loop with the new conn
		}

		// Either: target conn is dialed but full, or someone else is
		// dialing it. Block until a slot is released or a dial completes.
		blocked = true
		p.cond.Wait()
	}
}

// release returns h to the pool. forceClose=true marks the slot dirty
// (excluded from future allocation) without disturbing other slots on the
// same connection. When every slot of a connection becomes dirty and no
// in-use slot remains, the connection is closed and the entry is set to
// nil so a future acquire can re-dial it (preserving the stable hash
// index — slice splicing would shift remaining indices and break
// hash-routed callers' ordering guarantees).
func (p *singleSlotPool) release(h *SlotHandle, forceClose bool) {
	p.mu.Lock()
	var toClose *RDMAConn
	for i, cs := range p.conns {
		if cs == nil || cs.conn != h.Conn {
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
			p.conns[i] = nil
		}
		break
	}
	active := p.activeSlotsLocked()
	p.cond.Broadcast()
	p.mu.Unlock()
	metricsSetActiveSlots(p.cfg.Role, p.addr, active)
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
	conns := make([]*connSlots, 0, len(p.conns))
	for i, cs := range p.conns {
		if cs != nil {
			conns = append(conns, cs)
			p.conns[i] = nil
		}
	}
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
	return p.activeSlotsLocked()
}

// activeSlotsLocked is the lock-held variant used inside acquire/release
// so the caller can update metrics with the latest count without dropping
// the mutex twice.
func (p *singleSlotPool) activeSlotsLocked() int {
	n := 0
	for _, cs := range p.conns {
		if cs == nil {
			continue
		}
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
//
// AcquireSlot uses round-robin slot selection. Callers that require
// strict per-stream ordering (datanode→follower replication, SDK
// per-extent appends) MUST call AcquireSlotForKey instead with a stable
// (PartitionID, ExtentID)-style key — otherwise concurrent slots
// dispatch their packets across multiple QPs and the server-side
// extent.go append-offset check will reject out-of-order writes with
// OpTryOtherExtent.
func (p *RDMAConnPool) AcquireSlot(addr string) (*SlotHandle, error) {
	return p.AcquireSlotForKey(addr, "")
}

// ConnForKey returns a healthy RDMAConn for the given (addr, key)
// without holding any slot. Used by operations that need the conn's
// QP but not the slot accounting — currently only the one-sided
// RDMA Read fast path (Sprint A.6).
//
// Implementation note: this acquires a slot and immediately releases
// it. The brief slot use is the price for reusing AcquireSlotForKey's
// dial-on-demand machinery without duplicating it. Conns outlive
// slots — the released slot returns to the free list while the
// caller continues to hold the conn pointer; another caller may
// concurrently use the same conn for sends or reads on its QP.
func (p *RDMAConnPool) ConnForKey(addr, key string) (*RDMAConn, error) {
	handle, err := p.AcquireSlotForKey(addr, key)
	if err != nil {
		return nil, err
	}
	conn := handle.Conn
	p.ReleaseSlot(handle, false)
	return conn, nil
}

// ConnIfReady returns an already-established RDMAConn for addr if one
// exists, or (nil, false) if no conn has been dialed yet (or all are
// closed). It NEVER dials a new conn — designed for the one-sided
// RDMA Read fast path, which must not pay dial latency on the read
// hot path and must not contend for the per-peer connection budget
// that the two-sided path manages.
//
// Rationale (added after a production incident where Phase A had a
// 100% failure rate and dropped read throughput from 1.2 GB/s to
// 50 MB/s): ConnForKey with key="" goes through round-robin which,
// when all existing conns are slot-saturated, falls into the
// dial-missing-conn branch. In a read-heavy ObjectNode workload the
// only conn that was ever established was conn 0; every Phase A
// attempt then tried to dial conn 1, which the server rejected
// (RDMA_CM_EVENT_REJECTED — likely per-peer QP cap). Phase A is
// best-effort acceleration, so the right answer is "if I can't piggy-
// back on a ready conn, fall back to two-sided silently."
//
// The returned conn is shared with the two-sided path; callers must
// only use it for QP-level operations (post_send / post_recv) that do
// not need slot bookkeeping — exactly what PostRDMAReadAndWait does.
func (p *RDMAConnPool) ConnIfReady(addr string) (*RDMAConn, bool) {
	p.mu.RLock()
	sp := p.pools[addr]
	p.mu.RUnlock()
	if sp == nil {
		return nil, false
	}
	return sp.anyAliveConn()
}

// anyAliveConn returns the first non-nil, non-closed conn in the
// sub-pool. The "first" rule keeps callers stable (always picks conn 0
// when present) which matches the two-sided path's natural conn-0
// preference for read traffic; a load-balanced variant can be layered
// on top later if Phase A becomes the dominant traffic source.
func (p *singleSlotPool) anyAliveConn() (*RDMAConn, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cs := range p.conns {
		if cs == nil || cs.conn == nil {
			continue
		}
		if cs.conn.IsClosed() {
			continue
		}
		return cs.conn, true
	}
	return nil, false
}

// AcquireSlotForKey is like AcquireSlot but pins the call to a
// deterministic conn (hash(key) % maxConns) within the per-addr
// sub-pool, so all calls with the same key share a QP and observe FIFO
// ordering at the server side.
func (p *RDMAConnPool) AcquireSlotForKey(addr, key string) (*SlotHandle, error) {
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
				Role:          p.cfg.Role,
			}
			sp = newSingleSlotPool(addr, connCfg, p.cfg.MaxConns, p.dial)
			p.pools[addr] = sp
		}
		p.mu.Unlock()
	}
	return sp.acquire(key)
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

// MinPayloadBytes returns the configured small-packet RDMA-skip threshold.
// Callers (SDK send paths) consult this before invoking AcquireSlot for
// payloads that may be too small to benefit from RDMA (P6).
func (p *RDMAConnPool) MinPayloadBytes() int {
	if p == nil {
		return 0
	}
	return p.cfg.MinPayloadBytes
}

// MaxPayloadBytes returns the largest data size that is guaranteed to
// fit in one slot, accounting for slot-header and worst-case packet
// header overhead. Callers should compare their data length (Size) plus
// ArgLen against this value; if it exceeds, the RDMA path must be
// skipped and the request routed over TCP. Returns 0 if the pool is nil
// or slotSize is unset.
func (p *RDMAConnPool) MaxPayloadBytes() int {
	if p == nil || p.cfg.SlotSize <= 0 {
		return 0
	}
	// Match SerializePacket's worst-case overhead: slot-header (16) +
	// packet-header (PacketHeaderSize) + optional version-trailer
	// (VerSeq + ProtoVer). Constants live in util to stay build-tag-free.
	const slotHeaderSize = 16
	overhead := slotHeaderSize + util.PacketHeaderSize +
		util.PacketVerSeqFiledLen + util.PacketProtoVerFiledLen
	if p.cfg.SlotSize <= overhead {
		return 0
	}
	return p.cfg.SlotSize - overhead
}

// Role returns the metric role label associated with the pool. Useful
// for callers that want to record fallback metrics with the same label
// the pool would have used.
func (p *RDMAConnPool) Role() string {
	if p == nil {
		return ""
	}
	return p.cfg.Role
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
