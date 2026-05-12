//go:build linux && rdma

package rdma

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// ReadOnlyConnPool is a dedicated connection pool for Phase A one-sided
// RDMA Read traffic. Each (post-shift) RDMA address gets ONE conn,
// distinct from the two-sided slot pool's conns.
//
// Why a separate pool: RC QPs go to ERR state on any WR completion
// error (RDMA spec, not a bug we can fix). One-sided reads can fail
// for legitimate reasons that don't reflect a broken QP — a server's
// MR was dereg'd because an extent was deleted, a stale lease's rkey,
// a transient read on a chunk past EOF. Sharing a QP with two-sided
// traffic means any of those Phase A failures kills the QP and
// poisons the two-sided send/recv path. Production: a single Phase A
// status=12 WC drove every DP into "maybe readonly" within seconds.
// Isolating Phase A into its own conn keeps the failure blast radius
// inside Phase A.
//
// Design:
//   - Conn per addr, lazy-dialed on first ConnIfReady.
//   - If a dialed conn observes a WR fault (markFault), the next
//     ConnIfReady call detects IsClosed() and re-dials.
//   - We pay one extra QP per (SDK, DataNode) pair — cheap compared
//     to the safety win.
//   - The pool DOES NOT participate in slot accounting. Phase A's
//     QP is used exclusively for RDMA_READ WRs, which post directly
//     against the QP (see read_waiter.go's PostRDMAReadAndWait).
//
// Concurrency:
//   - mu protects conns map and the per-addr dialing flag.
//   - During a dial, the caller holds the addr-specific dialing
//     guard but releases the map mutex so other addrs can proceed.
//   - On failed dial we don't keep retrying immediately — caller
//     sees (nil, false) and silently falls back to two-sided. The
//     two-sided path may then dial successfully (it has its own
//     conn pool), and the next Phase A attempt will try ours again.

// ErrReadOnlyPoolClosed is returned by ConnIfReady after Close.
var ErrReadOnlyPoolClosed = errors.New("rdma: ReadOnlyConnPool closed")

// readOnlyConnState wraps the conn + dial bookkeeping for a single addr.
type readOnlyConnState struct {
	mu       sync.Mutex
	conn     *RDMAConn
	dialing  bool
	lastErr  error
	lastDial time.Time
}

// ReadOnlyConnPool is goroutine-safe.
type ReadOnlyConnPool struct {
	cfg RDMAPoolConfig

	mu     sync.RWMutex
	conns  map[string]*readOnlyConnState
	closed bool

	// dialBackoff suppresses repeated dial attempts to a target that
	// just failed to dial — prevents Phase A from hammering an
	// unreachable peer in the read hot path. 1s is enough to skip
	// the immediate burst of in-flight requests; longer would mask
	// legitimate recovery.
	dialBackoff time.Duration
}

// NewReadOnlyConnPool constructs a pool. The cfg fields that matter for
// the dial: Device, Port (passed through to verbs), NumSlots/SlotSize
// (these end up unused for Phase A but the conn constructor still
// allocates them — accepted as a "complete RDMAConn" cost in exchange
// for not forking Dial). RDMAPortShift is honoured by ConnIfReady's
// translation so callers can pass TCP addresses, same UX as the
// two-sided pool.
func NewReadOnlyConnPool(cfg RDMAPoolConfig) (*ReadOnlyConnPool, error) {
	if cfg.NumSlots <= 0 || cfg.SlotSize <= 0 {
		return nil, fmt.Errorf("rdma: ReadOnlyConnPool: NumSlots/SlotSize must be > 0")
	}
	return &ReadOnlyConnPool{
		cfg:         cfg,
		conns:       make(map[string]*readOnlyConnState),
		dialBackoff: time.Second,
	}, nil
}

// ConnIfReady returns the live Phase A conn for addr, or (nil, false)
// if no conn is currently usable. Lazy-dials on first miss; re-dials
// when the cached conn has been markFault'd by its drainer.
//
// addr here is the caller's view (typically a TCP listen address).
// We apply RDMAPortShift internally to keep parity with the two-sided
// pool's keying — callers pass the same string they pass to
// rdmaConnPool.ConnIfReady / AcquireSlotForKey.
//
// Returns (nil, false) on:
//   - pool closed
//   - first attempt to addr is dialing concurrently
//   - last dial to addr failed within dialBackoff window
//   - dial fails
//
// The caller (Phase A entry) treats all of these the same way:
// fall through to the two-sided path silently. ConnIfReady never
// produces a hard error to the caller.
func (p *ReadOnlyConnPool) ConnIfReady(callerAddr string) (*RDMAConn, bool) {
	target := callerAddr
	if p.cfg.RDMAPortShift != 0 {
		target = util.ShiftAddrPort(callerAddr, p.cfg.RDMAPortShift)
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, false
	}
	st, ok := p.conns[target]
	p.mu.RUnlock()

	if !ok {
		// First touch for this addr — create the state entry.
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, false
		}
		st, ok = p.conns[target]
		if !ok {
			st = &readOnlyConnState{}
			p.conns[target] = st
		}
		p.mu.Unlock()
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	// Fast path: alive conn cached.
	if st.conn != nil && !st.conn.IsClosed() {
		return st.conn, true
	}

	// Tear down a faulted/closed conn so the next dial gets a clean
	// state. Conn.Close is idempotent on already-closed conns.
	if st.conn != nil {
		_ = st.conn.Close()
		st.conn = nil
		log.LogWarnf("rdma readonly pool: conn to %s was closed/faulted — will redial", target)
	}

	// Don't redial faster than dialBackoff. Phase A is called
	// in-band on every read; without this gate, a peer that
	// goes down briefly would cost every concurrent reader one
	// dial round-trip before they all give up. With it, only the
	// first request after the backoff window tries.
	if !st.lastDial.IsZero() && time.Since(st.lastDial) < p.dialBackoff && st.lastErr != nil {
		return nil, false
	}

	// Concurrent guard so N parallel readers don't all dial at once.
	if st.dialing {
		return nil, false
	}
	st.dialing = true
	cfg := readOnlyConnCfg(p.cfg)
	st.mu.Unlock()

	conn, err := Dial(target, cfg)

	st.mu.Lock()
	st.dialing = false
	st.lastDial = time.Now()
	st.lastErr = err
	if err != nil {
		log.LogWarnf("rdma readonly pool: dial %s FAILED: %v — Phase A disabled for this addr until %s",
			target, err, st.lastDial.Add(p.dialBackoff).Format(time.RFC3339))
		return nil, false
	}
	st.conn = conn
	log.LogInfof("rdma readonly pool: dial %s OK — Phase A conn ready", target)
	return conn, true
}

// Close tears down every cached conn. Subsequent ConnIfReady calls
// return (nil, false). Safe to call multiple times.
func (p *ReadOnlyConnPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	toClose := make([]*RDMAConn, 0, len(p.conns))
	for _, st := range p.conns {
		st.mu.Lock()
		if st.conn != nil {
			toClose = append(toClose, st.conn)
			st.conn = nil
		}
		st.mu.Unlock()
	}
	p.mu.Unlock()
	for _, c := range toClose {
		_ = c.Close()
	}
}

// readOnlyConnStats is exposed for the Phase A stats logger so the
// next 60s line shows how many Phase A conns are alive across all
// DataNodes — a quick health signal independent of the success/fail
// counters.
type readOnlyConnStats struct {
	Tracked int
	Alive   int
	Faulted int
}

func (p *ReadOnlyConnPool) Stats() readOnlyConnStats {
	var s readOnlyConnStats
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, st := range p.conns {
		st.mu.Lock()
		s.Tracked++
		switch {
		case st.conn == nil:
			s.Faulted++
		case st.conn.IsClosed():
			s.Faulted++
		default:
			s.Alive++
		}
		st.mu.Unlock()
	}
	return s
}

// readOnlyDialCount exposed for tests — used nowhere in prod.
var readOnlyDialCount int64

func incReadOnlyDialCount() { atomic.AddInt64(&readOnlyDialCount, 1) }

// readOnlyConnCfg derives the per-conn config from the pool config.
// We override Role to a dedicated label so the existing prometheus
// metrics ({role, addr}) separate Phase A traffic from two-sided
// without operators needing a new dashboard.
func readOnlyConnCfg(p RDMAPoolConfig) RDMAConnConfig {
	return RDMAConnConfig{
		NumSlots:      p.NumSlots,
		SlotSize:      p.SlotSize,
		CreditAckMode: p.CreditAckMode,
		Poll:          p.Poll,
		Role:          p.Role + "_phasea",
	}
}
