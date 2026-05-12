// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rdma

import (
	"errors"
	"fmt"
	"sync"
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
//
// Build-tag-free so it can be unit-tested on darwin (the cgo Dial is
// injected via dialFunc — production wiring passes the real verbs
// Dial in pool.go's NewReadOnlyConnPool wrapper; tests pass mocks).

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
	cfg  RDMAPoolConfig
	dial dialFunc

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

// newReadOnlyConnPool builds a pool with the supplied dial function.
// Production code uses NewReadOnlyConnPool in pool.go (rdma build)
// or pool_stub.go (non-rdma) which inject the real / stub dial; tests
// inject mocks directly via this entry point.
func newReadOnlyConnPool(cfg RDMAPoolConfig, dial dialFunc) (*ReadOnlyConnPool, error) {
	if cfg.NumSlots <= 0 || cfg.SlotSize <= 0 {
		return nil, fmt.Errorf("rdma: ReadOnlyConnPool: NumSlots/SlotSize must be > 0")
	}
	if dial == nil {
		return nil, errors.New("rdma: ReadOnlyConnPool: nil dial")
	}
	return &ReadOnlyConnPool{
		cfg:         cfg,
		dial:        dial,
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

	// Fast path: alive conn cached.
	if st.conn != nil && !st.conn.IsClosed() {
		conn := st.conn
		st.mu.Unlock()
		return conn, true
	}

	// Tear down a faulted/closed conn so the next dial gets a clean
	// state. Conn.Close is idempotent on already-closed conns.
	if st.conn != nil {
		old := st.conn
		st.conn = nil
		log.LogWarnf("rdma readonly pool: conn to %s was closed/faulted — will redial", target)
		// Close outside the lock to avoid blocking other goroutines
		// during teardown (Close may take milliseconds).
		st.mu.Unlock()
		_ = old.Close()
		st.mu.Lock()
	}

	// Don't redial faster than dialBackoff. Phase A is called
	// in-band on every read; without this gate, a peer that
	// goes down briefly would cost every concurrent reader one
	// dial round-trip before they all give up. With it, only the
	// first request after the backoff window tries.
	if !st.lastDial.IsZero() && time.Since(st.lastDial) < p.dialBackoff && st.lastErr != nil {
		st.mu.Unlock()
		return nil, false
	}

	// Concurrent guard so N parallel readers don't all dial at once.
	if st.dialing {
		st.mu.Unlock()
		return nil, false
	}
	st.dialing = true
	cfg := readOnlyConnCfg(p.cfg)
	st.mu.Unlock()

	conn, err := p.dial(target, cfg)

	st.mu.Lock()
	st.dialing = false
	st.lastDial = time.Now()
	st.lastErr = err
	// Pool may have been Closed while we were dialing; if so, throw
	// away the new conn and report miss.
	if p.isClosed() {
		st.mu.Unlock()
		if err == nil && conn != nil {
			_ = conn.Close()
		}
		return nil, false
	}
	if err != nil {
		st.mu.Unlock()
		log.LogWarnf("rdma readonly pool: dial %s FAILED: %v — Phase A disabled for this addr until %s",
			target, err, st.lastDial.Add(p.dialBackoff).Format(time.RFC3339))
		return nil, false
	}
	st.conn = conn
	st.mu.Unlock()
	log.LogInfof("rdma readonly pool: dial %s OK — Phase A conn ready", target)
	return conn, true
}

// isClosed is a small helper to read p.closed under the rwmutex.
// Used by ConnIfReady's post-dial check.
func (p *ReadOnlyConnPool) isClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
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

// ReadOnlyConnStats is a snapshot of the pool's per-addr conn health,
// surfaced by the 60s Phase A stats logger so operators can confirm
// that Phase A's QPs are alive without having to read the per-fault
// WARN log.
type ReadOnlyConnStats struct {
	Tracked int // addrs we've ever tried to dial
	Alive   int // addrs with an open, non-faulted conn right now
	Faulted int // addrs whose conn was markFault'd or never dialed successfully
}

func (p *ReadOnlyConnPool) Stats() ReadOnlyConnStats {
	var s ReadOnlyConnStats
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
