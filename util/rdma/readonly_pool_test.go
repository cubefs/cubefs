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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests exercise ReadOnlyConnPool's lifecycle without touching
// real RDMA hardware: dialFunc is injected, RDMAConn is the stub form
// (non-rdma build), and IsClosed / Close are the stub's tiny flag impl.
// Real RDMA dial behaviour is verified by integration tests on linux+rdma.

// fakeDialCtl drives a controllable dialFunc that tests pass to
// newReadOnlyConnPool. Counters / failure injection / synchronisation
// hooks are atomic so the concurrent tests don't need locks.
type fakeDialCtl struct {
	dialCount int64
	failNext  int32 // when >0, the next dial returns failErr; decremented
	failErr   error
	// dialDelay holds the dial goroutine for at least this long so
	// the concurrent-dial test can observe the dialing flag.
	dialDelay time.Duration
	// onDial is invoked at the start of each dial (after the count
	// is incremented) — tests use it as a synchronization barrier.
	onDial func(addr string)
}

func (f *fakeDialCtl) dial(addr string, _ RDMAConnConfig) (*RDMAConn, error) {
	atomic.AddInt64(&f.dialCount, 1)
	if f.onDial != nil {
		f.onDial(addr)
	}
	if f.dialDelay > 0 {
		time.Sleep(f.dialDelay)
	}
	if atomic.LoadInt32(&f.failNext) > 0 {
		atomic.AddInt32(&f.failNext, -1)
		return nil, f.failErr
	}
	return &RDMAConn{}, nil
}

func newTestReadOnlyPool(t *testing.T, ctl *fakeDialCtl) *ReadOnlyConnPool {
	t.Helper()
	p, err := newReadOnlyConnPool(RDMAPoolConfig{
		NumSlots: 4, SlotSize: 64 * 1024,
	}, ctl.dial)
	if err != nil {
		t.Fatalf("newReadOnlyConnPool: %v", err)
	}
	// Short backoff so the redial path doesn't burn test wall time.
	p.dialBackoff = 50 * time.Millisecond
	return p
}

func TestReadOnlyConnPool_Ctor(t *testing.T) {
	if _, err := newReadOnlyConnPool(RDMAPoolConfig{NumSlots: 0, SlotSize: 1}, func(string, RDMAConnConfig) (*RDMAConn, error) { return nil, nil }); err == nil {
		t.Error("zero NumSlots should be rejected")
	}
	if _, err := newReadOnlyConnPool(RDMAPoolConfig{NumSlots: 1, SlotSize: 0}, func(string, RDMAConnConfig) (*RDMAConn, error) { return nil, nil }); err == nil {
		t.Error("zero SlotSize should be rejected")
	}
	if _, err := newReadOnlyConnPool(RDMAPoolConfig{NumSlots: 1, SlotSize: 1}, nil); err == nil {
		t.Error("nil dial should be rejected")
	}
}

func TestReadOnlyConnPool_LazyDial(t *testing.T) {
	ctl := &fakeDialCtl{}
	p := newTestReadOnlyPool(t, ctl)
	defer p.Close()

	c, ok := p.ConnIfReady("dn1:17310")
	if !ok || c == nil {
		t.Fatalf("first ConnIfReady: ok=%v c=%v", ok, c)
	}
	if got := atomic.LoadInt64(&ctl.dialCount); got != 1 {
		t.Errorf("dialCount after first ConnIfReady: got %d want 1", got)
	}

	// Second call hits the cached conn; no new dial.
	c2, ok := p.ConnIfReady("dn1:17310")
	if !ok || c2 != c {
		t.Errorf("second ConnIfReady should return cached conn: c=%v c2=%v ok=%v", c, c2, ok)
	}
	if got := atomic.LoadInt64(&ctl.dialCount); got != 1 {
		t.Errorf("dialCount after cache hit: got %d want 1", got)
	}
}

func TestReadOnlyConnPool_DistinctAddrs(t *testing.T) {
	ctl := &fakeDialCtl{}
	p := newTestReadOnlyPool(t, ctl)
	defer p.Close()

	a, _ := p.ConnIfReady("dn1:17310")
	b, _ := p.ConnIfReady("dn2:17310")
	if a == nil || b == nil || a == b {
		t.Errorf("distinct addrs should get distinct conns: a=%v b=%v", a, b)
	}
	if got := atomic.LoadInt64(&ctl.dialCount); got != 2 {
		t.Errorf("dialCount: got %d want 2", got)
	}
}

func TestReadOnlyConnPool_PortShift(t *testing.T) {
	// ConnIfReady should apply RDMAPortShift before dialing, mirroring
	// the keying the two-sided pool already uses. The test verifies
	// the dial was called with the SHIFTED address.
	var dialed string
	ctl := &fakeDialCtl{
		onDial: func(addr string) { dialed = addr },
	}
	p, err := newReadOnlyConnPool(RDMAPoolConfig{
		NumSlots: 4, SlotSize: 64 * 1024, RDMAPortShift: 40,
	}, ctl.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if _, ok := p.ConnIfReady("dn1:17310"); !ok {
		t.Fatal("ConnIfReady failed")
	}
	if dialed != "dn1:17350" {
		t.Errorf("dialed addr: got %q want %q", dialed, "dn1:17350")
	}
}

func TestReadOnlyConnPool_DialFailureBackoff(t *testing.T) {
	ctl := &fakeDialCtl{
		failNext: 100, // fail for the duration of the test
		failErr:  errors.New("simulated unreachable"),
	}
	p := newTestReadOnlyPool(t, ctl) // dialBackoff = 50ms
	defer p.Close()

	// First attempt dials and fails.
	if _, ok := p.ConnIfReady("dn1:17310"); ok {
		t.Fatal("ConnIfReady should fail when dial fails")
	}
	if got := atomic.LoadInt64(&ctl.dialCount); got != 1 {
		t.Errorf("dialCount after first failure: got %d want 1", got)
	}

	// Second attempt within backoff window must NOT dial.
	if _, ok := p.ConnIfReady("dn1:17310"); ok {
		t.Fatal("ConnIfReady should still fail")
	}
	if got := atomic.LoadInt64(&ctl.dialCount); got != 1 {
		t.Errorf("dialCount within backoff window: got %d want 1 (no redial)", got)
	}

	// Wait past backoff window — next call should retry dial.
	time.Sleep(p.dialBackoff + 20*time.Millisecond)
	if _, ok := p.ConnIfReady("dn1:17310"); ok {
		t.Fatal("ConnIfReady should still fail (dial still fails)")
	}
	if got := atomic.LoadInt64(&ctl.dialCount); got != 2 {
		t.Errorf("dialCount after backoff expiry: got %d want 2", got)
	}
}

func TestReadOnlyConnPool_RedialAfterFault(t *testing.T) {
	ctl := &fakeDialCtl{}
	p := newTestReadOnlyPool(t, ctl)
	defer p.Close()

	c1, _ := p.ConnIfReady("dn1:17310")
	if c1 == nil {
		t.Fatal("first dial")
	}
	// Simulate a drainer markFault by closing the conn directly.
	_ = c1.Close()

	// Next ConnIfReady should detect closed and dial a fresh conn.
	// Wait past backoff so the redial path runs (no lastErr so it'd
	// run anyway, but be explicit).
	time.Sleep(p.dialBackoff + 20*time.Millisecond)
	c2, ok := p.ConnIfReady("dn1:17310")
	if !ok || c2 == nil {
		t.Fatalf("redial failed: ok=%v c2=%v", ok, c2)
	}
	if c2 == c1 {
		t.Errorf("expected fresh conn, got the same faulted one")
	}
	if got := atomic.LoadInt64(&ctl.dialCount); got != 2 {
		t.Errorf("dialCount after redial: got %d want 2", got)
	}
}

func TestReadOnlyConnPool_ConcurrentDialSingleflight(t *testing.T) {
	// N parallel ConnIfReady on a cold addr must converge to ONE
	// dial — losers see (nil, false) and the winner stores the conn.
	// dialDelay lets all callers race the dialing flag.
	ctl := &fakeDialCtl{
		dialDelay: 50 * time.Millisecond,
	}
	p := newTestReadOnlyPool(t, ctl)
	defer p.Close()

	const N = 16
	var wg sync.WaitGroup
	var okCount int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := p.ConnIfReady("dn1:17310"); ok {
				atomic.AddInt64(&okCount, 1)
			}
		}()
	}
	wg.Wait()

	// Exactly one dial.
	if got := atomic.LoadInt64(&ctl.dialCount); got != 1 {
		t.Errorf("dialCount: got %d want 1 (concurrent dialing should be singleflighted)", got)
	}
	// At least one ConnIfReady must succeed (the winner). Losers'
	// behaviour is "not ok" — they fall back silently. After the
	// dial completes, subsequent ConnIfReady should all succeed.
	if okCount < 1 {
		t.Errorf("at least the winner should succeed: okCount=%d", okCount)
	}
	if _, ok := p.ConnIfReady("dn1:17310"); !ok {
		t.Error("post-dial ConnIfReady should succeed")
	}
}

func TestReadOnlyConnPool_Close(t *testing.T) {
	ctl := &fakeDialCtl{}
	p := newTestReadOnlyPool(t, ctl)

	c, _ := p.ConnIfReady("dn1:17310")
	if c == nil {
		t.Fatal("setup dial")
	}
	if c.IsClosed() {
		t.Fatal("conn should be open before pool Close")
	}

	p.Close()

	if !c.IsClosed() {
		t.Error("pool Close should close cached conns")
	}
	if _, ok := p.ConnIfReady("dn1:17310"); ok {
		t.Error("ConnIfReady after Close should return false")
	}
	// Idempotent.
	p.Close()
}

func TestReadOnlyConnPool_CloseDuringDial(t *testing.T) {
	// Close() may race a dial in flight. The post-dial isClosed()
	// check should detect this and discard the freshly-dialed conn
	// instead of stashing it.
	dialReleased := make(chan struct{})
	dialStarted := make(chan struct{})
	ctl := &fakeDialCtl{
		onDial: func(_ string) {
			close(dialStarted)
			<-dialReleased
		},
	}
	p := newTestReadOnlyPool(t, ctl)

	dialResult := make(chan bool, 1)
	go func() {
		_, ok := p.ConnIfReady("dn1:17310")
		dialResult <- ok
	}()
	<-dialStarted

	// Close pool while dial is in flight.
	p.Close()

	// Let the dial return successfully — but it should be discarded.
	close(dialReleased)

	select {
	case ok := <-dialResult:
		if ok {
			t.Error("ConnIfReady should report miss when pool was closed mid-dial")
		}
	case <-time.After(time.Second):
		t.Fatal("ConnIfReady didn't return within 1s after Close+dial release")
	}
}

func TestReadOnlyConnPool_Stats(t *testing.T) {
	ctl := &fakeDialCtl{}
	p := newTestReadOnlyPool(t, ctl)
	defer p.Close()

	// Initially empty.
	if s := p.Stats(); s.Tracked != 0 || s.Alive != 0 || s.Faulted != 0 {
		t.Errorf("initial Stats: %+v", s)
	}

	// One healthy conn.
	c1, _ := p.ConnIfReady("dn1:17310")
	if s := p.Stats(); s.Tracked != 1 || s.Alive != 1 || s.Faulted != 0 {
		t.Errorf("after first dial Stats: %+v", s)
	}

	// Second addr also healthy.
	_, _ = p.ConnIfReady("dn2:17310")
	if s := p.Stats(); s.Tracked != 2 || s.Alive != 2 || s.Faulted != 0 {
		t.Errorf("after second dial Stats: %+v", s)
	}

	// Fault the first conn — Stats should reflect Faulted=1 until
	// the next ConnIfReady call kicks off a redial.
	_ = c1.Close()
	if s := p.Stats(); s.Tracked != 2 || s.Alive != 1 || s.Faulted != 1 {
		t.Errorf("after first conn fault Stats: %+v", s)
	}
}
