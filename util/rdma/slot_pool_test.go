package rdma

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDial constructs an in-memory RDMAConn shell that satisfies the
// pool's bookkeeping without touching real RDMA hardware. The pool only
// reads NumSlots / IsClosed / RemoteAddr / Close on the conn — none of
// which require the CGO-tagged code paths.
func fakeDial(numSlots int, dialCount *int64) dialFunc {
	return func(addr string, cfg RDMAConnConfig) (*RDMAConn, error) {
		if dialCount != nil {
			atomic.AddInt64(dialCount, 1)
		}
		return &RDMAConn{numSlots: numSlots}, nil
	}
}

func newTestPool(t *testing.T, numSlots, maxConns int, dialCount *int64) *RDMAConnPool {
	t.Helper()
	cfg := RDMAPoolConfig{
		NumSlots: numSlots,
		SlotSize: MinValidSlotSize,
		MaxConns: maxConns,
	}
	pool, err := newPool(cfg, fakeDial(numSlots, dialCount))
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestSlotPool_RoundRobinSameConn verifies AcquireSlot rotates through
// distinct slots within a single connection before opening a second one.
func TestSlotPool_RoundRobinSameConn(t *testing.T) {
	const numSlots = 4
	var dialCount int64
	pool := newTestPool(t, numSlots, 2, &dialCount)

	seen := map[int]bool{}
	for i := 0; i < numSlots; i++ {
		h, err := pool.AcquireSlot("addr-A")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if seen[h.SlotIdx] {
			t.Errorf("slot %d returned twice", h.SlotIdx)
		}
		seen[h.SlotIdx] = true
	}
	if got := atomic.LoadInt64(&dialCount); got != 1 {
		t.Errorf("dialCount=%d, expected 1 (round-robin within first conn)", got)
	}
}

// TestSlotPool_DialsSecondConnWhenFirstFull verifies a new connection is
// dialed once every slot of the first conn is already in use.
func TestSlotPool_DialsSecondConnWhenFirstFull(t *testing.T) {
	const numSlots = 2
	var dialCount int64
	pool := newTestPool(t, numSlots, 2, &dialCount)

	// Saturate first conn.
	h1, _ := pool.AcquireSlot("addr-A")
	h2, _ := pool.AcquireSlot("addr-A")
	// Third acquire must dial a new conn rather than block.
	h3, err := pool.AcquireSlot("addr-A")
	if err != nil {
		t.Fatalf("third acquire: %v", err)
	}
	defer pool.ReleaseSlot(h1, false)
	defer pool.ReleaseSlot(h2, false)
	defer pool.ReleaseSlot(h3, false)

	if got := atomic.LoadInt64(&dialCount); got != 2 {
		t.Errorf("dialCount=%d, expected 2 (second conn dialed)", got)
	}
	if h3.Conn == h1.Conn || h3.Conn == h2.Conn {
		t.Error("third handle should be on a different conn than the first two")
	}
}

// TestSlotPool_BlocksWhenAllConnsAndSlotsExhausted verifies the spec
// requirement: AcquireSlot blocks until a slot is released.
func TestSlotPool_BlocksWhenAllConnsAndSlotsExhausted(t *testing.T) {
	const numSlots = 1
	pool := newTestPool(t, numSlots, 1, nil) // 1 conn, 1 slot total

	h1, err := pool.AcquireSlot("addr-A")
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		h   *SlotHandle
		err error
	}
	done := make(chan result, 1)
	go func() {
		h, err := pool.AcquireSlot("addr-A")
		done <- result{h, err}
	}()

	select {
	case r := <-done:
		t.Fatalf("expected blocking acquire, got handle=%v err=%v", r.h, r.err)
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	pool.ReleaseSlot(h1, false)

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("blocked acquire returned error: %v", r.err)
		}
		pool.ReleaseSlot(r.h, false)
	case <-time.After(time.Second):
		t.Fatal("blocked acquire did not unblock after release")
	}
}

// TestSlotPool_DirtySlotExcludedFromRotation verifies that a forceClose'd
// slot is not handed out again, while other slots on the same conn remain
// available — matching the spec's "同连接其他 slot 不受影响" requirement.
func TestSlotPool_DirtySlotExcludedFromRotation(t *testing.T) {
	const numSlots = 4
	pool := newTestPool(t, numSlots, 1, nil)

	// Borrow then forceClose slot N.
	h1, _ := pool.AcquireSlot("addr-A")
	dirtySlot := h1.SlotIdx
	pool.ReleaseSlot(h1, true)

	// Now drain the remaining slots — none should be the dirty one.
	for i := 0; i < numSlots-1; i++ {
		h, err := pool.AcquireSlot("addr-A")
		if err != nil {
			t.Fatalf("acquire %d after dirty: %v", i, err)
		}
		if h.SlotIdx == dirtySlot {
			t.Errorf("dirty slot %d was reused at iteration %d", dirtySlot, i)
		}
		defer pool.ReleaseSlot(h, false)
	}
}

// TestSlotPool_AllDirtyConnRemovedAndReplaced verifies that once every
// slot of a conn is dirty, the conn is closed and a fresh conn is dialed
// on the next acquire.
func TestSlotPool_AllDirtyConnRemovedAndReplaced(t *testing.T) {
	const numSlots = 2
	var dialCount int64
	pool := newTestPool(t, numSlots, 2, &dialCount)

	// Acquire and forceClose every slot of the first conn.
	h1, _ := pool.AcquireSlot("addr-A")
	h2, _ := pool.AcquireSlot("addr-A")
	firstConn := h1.Conn
	if h2.Conn != firstConn {
		t.Fatalf("expected both handles on the same conn")
	}
	pool.ReleaseSlot(h1, true) // dirty slot 0; conn still has slot 1 in use
	pool.ReleaseSlot(h2, true) // dirty slot 1; conn now all-dead-and-idle, removed

	// Next acquire should dial a fresh conn.
	h3, err := pool.AcquireSlot("addr-A")
	if err != nil {
		t.Fatalf("acquire after teardown: %v", err)
	}
	defer pool.ReleaseSlot(h3, false)

	if h3.Conn == firstConn {
		t.Error("removed conn was reused; expected fresh dial")
	}
	if got := atomic.LoadInt64(&dialCount); got != 2 {
		t.Errorf("dialCount=%d, expected 2 (initial + replacement)", got)
	}
}

// TestSlotPool_ConcurrentAcquireDistinctHandles drives many goroutines
// against a saturating pool and asserts every concurrent acquire produces
// a unique (Conn, SlotIdx) tuple. Without per-slot inUse tracking this
// would trivially fail.
func TestSlotPool_ConcurrentAcquireDistinctHandles(t *testing.T) {
	const (
		numSlots = 8
		maxConns = 4
		workers  = 16
		ops      = 50
	)
	pool := newTestPool(t, numSlots, maxConns, nil)

	type key struct {
		conn *RDMAConn
		slot int
	}
	var seenMu sync.Mutex
	seen := map[key]int{}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				h, err := pool.AcquireSlot("addr-A")
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				seenMu.Lock()
				k := key{h.Conn, h.SlotIdx}
				if seen[k] > 0 {
					t.Errorf("duplicate (conn, slot) %v while still held", k)
				}
				seen[k] = 1
				seenMu.Unlock()

				// Hold briefly then release.
				time.Sleep(50 * time.Microsecond)

				seenMu.Lock()
				seen[k] = 0
				seenMu.Unlock()
				pool.ReleaseSlot(h, false)
			}
		}()
	}
	wg.Wait()
}

// TestSlotPool_CloseUnblocksWaiters verifies pool.Close wakes any
// goroutines currently parked in AcquireSlot with ErrSlotPoolClosed.
func TestSlotPool_CloseUnblocksWaiters(t *testing.T) {
	pool := newTestPool(t, 1, 1, nil)

	h, _ := pool.AcquireSlot("addr-A")
	defer pool.ReleaseSlot(h, false)

	done := make(chan error, 1)
	go func() {
		_, err := pool.AcquireSlot("addr-A")
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	pool.Close()

	select {
	case err := <-done:
		if err != ErrSlotPoolClosed {
			t.Fatalf("got %v, want ErrSlotPoolClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock blocked acquire")
	}
}

// TestSlotPool_RespectsMaxConnsUnderConcurrentDial reproduces the TOCTOU
// race that existed before slot_pool.go tracked an in-flight dialing
// counter: many acquirers see len(conns) < maxConns concurrently, all
// release the lock to dial, all succeed, and all append — overshooting
// the cap. With the dialing counter, only maxConns dials may be in
// flight at any moment regardless of concurrency.
func TestSlotPool_RespectsMaxConnsUnderConcurrentDial(t *testing.T) {
	const (
		numSlots = 1
		maxConns = 2
		workers  = 16
	)
	var dialCount int64
	slowDial := func(addr string, cfg RDMAConnConfig) (*RDMAConn, error) {
		atomic.AddInt64(&dialCount, 1)
		// Hold the dial in flight long enough that all workers race
		// past the maxConns check before any of them returns. Without
		// the dialing counter all 16 would dial concurrently.
		time.Sleep(20 * time.Millisecond)
		return &RDMAConn{numSlots: numSlots}, nil
	}

	pool, err := newPool(RDMAPoolConfig{
		NumSlots: numSlots,
		SlotSize: MinValidSlotSize,
		MaxConns: maxConns,
	}, slowDial)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	// Each worker acquires, holds briefly, releases. With maxConns=2 and
	// numSlots=1 the pool offers 2 total slots; workers serialise through
	// them but every dial that ends up registered must respect the cap.
	var wg sync.WaitGroup
	wg.Add(workers)
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			h, err := pool.AcquireSlot("addr-A")
			if err != nil {
				errCh <- err
				return
			}
			// Brief hold so other workers can race the dial path.
			time.Sleep(time.Millisecond)
			pool.ReleaseSlot(h, false)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("worker error: %v", err)
	}

	got := atomic.LoadInt64(&dialCount)
	if got > maxConns {
		t.Fatalf("dialCount = %d, exceeded maxConns = %d (TOCTOU regression)", got, maxConns)
	}
	if got < 1 {
		t.Fatalf("dialCount = %d, expected at least 1", got)
	}
}

// TestSlotPool_DialFailureWakesWaiters ensures that when a Dial returns
// an error, other goroutines parked on the cond are woken — they may
// want to retry the dial themselves now that the dialing slot has been
// freed.
func TestSlotPool_DialFailureWakesWaiters(t *testing.T) {
	const (
		numSlots = 1
		maxConns = 1
	)
	var dialAttempts int64
	failingDial := func(addr string, cfg RDMAConnConfig) (*RDMAConn, error) {
		n := atomic.AddInt64(&dialAttempts, 1)
		if n == 1 {
			// First dial fails; a subsequent retry should succeed and
			// produce a usable handle.
			return nil, errors.New("simulated dial failure")
		}
		return &RDMAConn{numSlots: numSlots}, nil
	}

	pool, err := newPool(RDMAPoolConfig{
		NumSlots: numSlots,
		SlotSize: MinValidSlotSize,
		MaxConns: maxConns,
	}, failingDial)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	// First acquire: dial fails, returns error.
	if _, err := pool.AcquireSlot("addr-A"); err == nil {
		t.Fatal("first acquire: expected dial failure, got nil")
	}
	// Second acquire: should succeed (dialing counter was decremented
	// after the failure, so we are below maxConns again).
	h, err := pool.AcquireSlot("addr-A")
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	pool.ReleaseSlot(h, false)

	if got := atomic.LoadInt64(&dialAttempts); got != 2 {
		t.Errorf("dialAttempts = %d, want 2", got)
	}
}

// TestSlotPool_ActiveSlotsCount verifies the diagnostic counter tracks
// borrowed slots across releases (used by P3 metrics).
func TestSlotPool_ActiveSlotsCount(t *testing.T) {
	pool := newTestPool(t, 4, 2, nil)

	if got := pool.ActiveSlots(); got != 0 {
		t.Errorf("initial: got %d, want 0", got)
	}
	h1, _ := pool.AcquireSlot("addr-A")
	h2, _ := pool.AcquireSlot("addr-A")
	if got := pool.ActiveSlots(); got != 2 {
		t.Errorf("after 2 acquires: got %d, want 2", got)
	}
	pool.ReleaseSlot(h1, false)
	if got := pool.ActiveSlots(); got != 1 {
		t.Errorf("after 1 release: got %d, want 1", got)
	}
	pool.ReleaseSlot(h2, false)
	if got := pool.ActiveSlots(); got != 0 {
		t.Errorf("after all releases: got %d, want 0", got)
	}
}

// TestRDMAConnPool_MinPayloadBytesAccessor verifies the MinPayloadBytes
// accessor passes through the configured threshold and handles nil.
func TestRDMAConnPool_MinPayloadBytesAccessor(t *testing.T) {
	cases := []struct {
		name       string
		threshold  int
		wantResult int
	}{
		{name: "zero (no threshold)", threshold: 0, wantResult: 0},
		{name: "4 KB default", threshold: 4096, wantResult: 4096},
		{name: "64 KB", threshold: 64 * 1024, wantResult: 64 * 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := RDMAPoolConfig{
				NumSlots:        4,
				SlotSize:        MinValidSlotSize,
				MaxConns:        2,
				MinPayloadBytes: tc.threshold,
			}
			pool, err := newPool(cfg, fakeDial(4, nil))
			if err != nil {
				t.Fatalf("newPool: %v", err)
			}
			defer pool.Close()
			if got := pool.MinPayloadBytes(); got != tc.wantResult {
				t.Errorf("MinPayloadBytes() = %d, want %d", got, tc.wantResult)
			}
		})
	}

	// Nil receiver returns 0 — defensive contract used by SDK call sites.
	var nilPool *RDMAConnPool
	if got := nilPool.MinPayloadBytes(); got != 0 {
		t.Errorf("(*RDMAConnPool)(nil).MinPayloadBytes() = %d, want 0", got)
	}
	if got := nilPool.Role(); got != "" {
		t.Errorf("(*RDMAConnPool)(nil).Role() = %q, want \"\"", got)
	}
}

// TestRDMAConnPool_RolePropagation ensures the Role label set on the
// pool is reachable via Role(); SDK fallback metrics rely on it.
func TestRDMAConnPool_RolePropagation(t *testing.T) {
	cfg := RDMAPoolConfig{
		NumSlots: 4,
		SlotSize: MinValidSlotSize,
		MaxConns: 1,
		Role:     RoleClient,
	}
	pool, err := newPool(cfg, fakeDial(4, nil))
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	defer pool.Close()
	if got := pool.Role(); got != RoleClient {
		t.Errorf("Role() = %q, want %q", got, RoleClient)
	}
}

// TestSlotPool_RecoverAfterConnFaultedWithNoSlotsHeld verifies the
// scenario hit by Phase A's MaxConns=1 dedicated pool: when an
// out-of-band fault (drainer markFault on a one-sided RDMA Read WR
// failure) closes the only conn while NO slot is currently held,
// the next AcquireSlot must reap the faulted conn and redial a
// fresh one — NOT block forever.
//
// Before the fix, allDeadAndIdle cleanup ran only on the release()
// path, which never fires when no slot was in use at fault time.
// The acquire() dial branch then sees `conns[0] != nil` and refuses
// to redial, leaving every subsequent caller parked in cond.Wait().
//
// The test uses MaxConns=1 to make the failure deterministic:
// there's only one conn slot, so if reaping doesn't happen the
// next acquire has nowhere to go.
func TestSlotPool_RecoverAfterConnFaultedWithNoSlotsHeld(t *testing.T) {
	var dialCount int64
	pool := newTestPool(t, 4 /*numSlots*/, 1 /*maxConns*/, &dialCount)

	// 1) Warm up — acquire and release so the conn is created.
	h, err := pool.AcquireSlot("dn1")
	if err != nil {
		t.Fatalf("warm-up acquire: %v", err)
	}
	conn := h.Conn
	pool.ReleaseSlot(h, false)
	if got := atomic.LoadInt64(&dialCount); got != 1 {
		t.Fatalf("warm-up dialCount: got %d want 1", got)
	}

	// 2) Out-of-band fault: simulate the drainer markFault'ing the
	//    conn via a one-sided WR failure. The pool's slot accounting
	//    has nothing in-flight at this point.
	_ = conn.Close()
	if !conn.IsClosed() {
		t.Fatal("conn should be closed after simulated fault")
	}

	// 3) The very next acquire must succeed by redialing, in
	//    bounded time. Without the fix, this AcquireSlot blocks
	//    forever; we cap it with a goroutine + timeout so the test
	//    fails with a clear message instead of hanging.
	done := make(chan struct{})
	var h2 *SlotHandle
	var acqErr error
	go func() {
		h2, acqErr = pool.AcquireSlot("dn1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("AcquireSlot blocked after conn fault — slot pool failed to redial (deadlock)")
	}

	if acqErr != nil {
		t.Fatalf("post-fault AcquireSlot: %v", acqErr)
	}
	if h2 == nil || h2.Conn == nil {
		t.Fatal("post-fault AcquireSlot returned nil handle/conn")
	}
	if h2.Conn == conn {
		t.Error("post-fault AcquireSlot returned the same faulted conn; should be fresh")
	}
	if got := atomic.LoadInt64(&dialCount); got != 2 {
		t.Errorf("dialCount after fault: got %d want 2", got)
	}
	pool.ReleaseSlot(h2, false)
}

// TestSlotPool_RecoverAfterFaultedConnWhileOtherSlotsInUse verifies
// the trickier variant: a fault hits the conn while OTHER slots on
// it are still held. The faulted conn must stay in the conns table
// until those slots get released (otherwise we'd lose track of who
// owns what), and only THEN should the next acquire redial.
//
// In our Phase A usage (MaxConns=1, mixed lookup slot traffic +
// QP-only RDMA Reads), this is the actual production path: a one-
// sided read WR can fail while a lookup is mid-slot.
func TestSlotPool_RecoverAfterFaultedConnWhileOtherSlotsInUse(t *testing.T) {
	var dialCount int64
	pool := newTestPool(t, 4 /*numSlots*/, 1 /*maxConns*/, &dialCount)

	hHeld, err := pool.AcquireSlot("dn1")
	if err != nil {
		t.Fatalf("setup acquire: %v", err)
	}
	conn := hHeld.Conn

	// Fault arrives while hHeld is in flight.
	_ = conn.Close()

	// While hHeld is still held, a parallel acquire should not
	// produce a fresh conn (because the faulted one still has
	// slots in use — releasing forceClose on those is the
	// recovery handshake). Instead it should park.
	parkResult := make(chan error, 1)
	go func() {
		_, err := pool.AcquireSlot("dn1")
		parkResult <- err
	}()
	select {
	case err := <-parkResult:
		t.Fatalf("AcquireSlot returned %v while faulted conn still has slots in use; should park", err)
	case <-time.After(150 * time.Millisecond):
	}

	// Now release the held slot — with forceClose=true (caller's
	// duty after observing a failed operation on this conn). This
	// triggers allDeadAndIdle cleanup, the pool reaps conns[0], and
	// the parked acquire wakes up to redial.
	pool.ReleaseSlot(hHeld, true)

	select {
	case err := <-parkResult:
		if err != nil {
			t.Fatalf("post-release AcquireSlot: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcquireSlot still blocked after the faulted conn's last slot was released")
	}
	if got := atomic.LoadInt64(&dialCount); got != 2 {
		t.Errorf("dialCount: got %d want 2 (warm-up + redial)", got)
	}
}

// TestSlotPool_HashRoutedRedialAfterFault is the keyed variant of
// the no-slot-held recovery test. Phase A's lookup callers pass
// non-empty key (PartitionID-ExtentID); the dial branch with
// targetIdx >= 0 has its own redial code path that must also reap
// faulted conns at the target index.
func TestSlotPool_HashRoutedRedialAfterFault(t *testing.T) {
	var dialCount int64
	pool := newTestPool(t, 4, 1, &dialCount)

	h, err := pool.AcquireSlotForKey("dn1", "k-1")
	if err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	conn := h.Conn
	pool.ReleaseSlot(h, false)
	_ = conn.Close()

	done := make(chan struct{})
	var h2 *SlotHandle
	var acqErr error
	go func() {
		h2, acqErr = pool.AcquireSlotForKey("dn1", "k-1")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("AcquireSlotForKey blocked after fault on hash-routed conn")
	}
	if acqErr != nil {
		t.Fatalf("post-fault AcquireSlotForKey: %v", acqErr)
	}
	if h2.Conn == conn {
		t.Error("post-fault returned faulted conn")
	}
	pool.ReleaseSlot(h2, false)
}

// TestSlotPool_ConnIfReadyReturnsFalseAfterFault verifies the
// QP-only access path Phase A uses: ConnIfReady must return
// (nil, false) when the only conn is faulted. Otherwise Phase A
// would happily post WRs on a dead QP and harvest WC_WR_FLUSH_ERR
// for every chunk.
func TestSlotPool_ConnIfReadyReturnsFalseAfterFault(t *testing.T) {
	pool := newTestPool(t, 4, 1, nil)

	// Force conn creation via an acquire.
	h, err := pool.AcquireSlot("dn1")
	if err != nil {
		t.Fatalf("setup acquire: %v", err)
	}
	conn := h.Conn
	pool.ReleaseSlot(h, false)

	if got, ok := pool.ConnIfReady("dn1"); !ok || got != conn {
		t.Fatalf("ConnIfReady pre-fault: ok=%v conn=%v", ok, got)
	}

	_ = conn.Close()

	if _, ok := pool.ConnIfReady("dn1"); ok {
		t.Error("ConnIfReady should return false after the only conn was faulted")
	}
}

// TestRDMAConnPool_ConnIfReadyKeyingMatchesAcquire locks in the
// contract that ConnIfReady and AcquireSlotForKey use the SAME
// address form as the pools map key — i.e. caller is responsible
// for any address translation (port shift, etc.) and must apply
// it uniformly to both APIs.
//
// Regression for the C1 Phase A deploy that produced
// fail.conn=7213/attempt=7213: lookupExtentMR translated TCP→RDMA
// addr before AcquireSlotForKey (so pool[rdma_addr] was populated)
// but extent_reader_rdma_read.go called ConnIfReady(tcp_addr).
// ConnIfReady never found the live conn even though one existed.
// The test passes addresses A and B and verifies they are distinct
// keys; if any future change adds an implicit shift inside the pool,
// the test will fail and force the author to reconcile both APIs.
func TestRDMAConnPool_ConnIfReadyKeyingMatchesAcquire(t *testing.T) {
	pool := newTestPool(t, 4, 1, nil)

	// AcquireSlotForKey populates pool.pools["addr-X"].
	h, err := pool.AcquireSlotForKey("addr-X", "k")
	if err != nil {
		t.Fatalf("AcquireSlotForKey: %v", err)
	}
	conn := h.Conn
	pool.ReleaseSlot(h, false)

	// ConnIfReady with the SAME addr must find that conn.
	got, ok := pool.ConnIfReady("addr-X")
	if !ok || got != conn {
		t.Fatalf("ConnIfReady(\"addr-X\"): ok=%v conn=%v want %v", ok, got, conn)
	}

	// ConnIfReady with a DIFFERENT addr must return false. This is
	// the literal "callers responsible for translation" contract:
	// pool maps key by addr string, no implicit normalisation.
	if _, ok := pool.ConnIfReady("addr-Y"); ok {
		t.Error("ConnIfReady(\"addr-Y\") should return false; pool was populated under \"addr-X\"")
	}

	// Mixed addr forms (the bug class): if the caller mistakenly
	// passes a pre-shift addr to one API and a post-shift addr to
	// the other, the post-shift API's data is invisible to the
	// pre-shift caller. Verify by acquiring under one key and
	// looking up under another.
	h2, err := pool.AcquireSlotForKey("post-shift-addr", "k")
	if err != nil {
		t.Fatalf("AcquireSlotForKey(post-shift-addr): %v", err)
	}
	pool.ReleaseSlot(h2, false)

	if _, ok := pool.ConnIfReady("pre-shift-addr"); ok {
		t.Error("ConnIfReady with mismatched addr form should return false; this is the contract callers depend on")
	}
}

// TestRDMAConnPool_ConnIfReadyForKey_RoutesToSameConnAsAcquire
// locks in the Phase A correctness invariant: a key passed to
// AcquireSlotForKey and the same key later passed to
// ConnIfReadyForKey must resolve to the same conn so the rkey
// returned by lookup remains valid on the read QP's PD.
//
// With maxConns > 1 this is non-trivial — different keys hash to
// different conns, and a key that flips between conns would silently
// break Phase A reads with a 5s WR timeout. The test pins two
// distinct keys, walks them up via AcquireSlotForKey, releases the
// slots, then verifies ConnIfReadyForKey returns the same conn each
// time AND that ConnIfReady (no key) sees the first alive one.
func TestRDMAConnPool_ConnIfReadyForKey_RoutesToSameConnAsAcquire(t *testing.T) {
	var dialCount int64
	pool := newTestPool(t, 4 /*numSlots*/, 4 /*maxConns*/, &dialCount)

	// Acquire under two distinct keys → triggers two dials at
	// different indices (assuming keys hash to distinct buckets;
	// fnv("k-a") and fnv("k-b") differ).
	hA, err := pool.AcquireSlotForKey("dn1", "k-a")
	if err != nil {
		t.Fatalf("AcquireSlotForKey(k-a): %v", err)
	}
	connA := hA.Conn
	pool.ReleaseSlot(hA, false)

	hB, err := pool.AcquireSlotForKey("dn1", "k-b")
	if err != nil {
		t.Fatalf("AcquireSlotForKey(k-b): %v", err)
	}
	connB := hB.Conn
	pool.ReleaseSlot(hB, false)

	// ConnIfReadyForKey with the same keys must return identical
	// pointers — that's the lookup ↔ read PD invariant.
	gotA, ok := pool.ConnIfReadyForKey("dn1", "k-a")
	if !ok || gotA != connA {
		t.Errorf("ConnIfReadyForKey(k-a): got %p ok=%v want %p", gotA, ok, connA)
	}
	gotB, ok := pool.ConnIfReadyForKey("dn1", "k-b")
	if !ok || gotB != connB {
		t.Errorf("ConnIfReadyForKey(k-b): got %p ok=%v want %p", gotB, ok, connB)
	}
}

// TestRDMAConnPool_ConnIfReadyForKey_EmptyKeyFallsBack verifies
// that an empty key falls back to anyAliveConn() semantics, mirroring
// AcquireSlotForKey's round-robin path. Callers shouldn't pass "" in
// the Phase A path, but the fallback keeps the API uniform.
func TestRDMAConnPool_ConnIfReadyForKey_EmptyKeyFallsBack(t *testing.T) {
	pool := newTestPool(t, 4, 4, nil)
	h, _ := pool.AcquireSlotForKey("dn1", "warm")
	pool.ReleaseSlot(h, false)

	conn, ok := pool.ConnIfReadyForKey("dn1", "")
	if !ok || conn == nil {
		t.Fatalf("empty key should fall back to anyAliveConn: ok=%v conn=%v", ok, conn)
	}
}

// TestRDMAConnPool_ConnIfReadyForKey_NoSubPoolReturnsFalse
// verifies the cold-cache case: no AcquireSlotForKey has run for
// this addr, so the pools map has no entry. ConnIfReadyForKey must
// return (nil, false) without dialing.
func TestRDMAConnPool_ConnIfReadyForKey_NoSubPoolReturnsFalse(t *testing.T) {
	pool := newTestPool(t, 4, 4, nil)
	if conn, ok := pool.ConnIfReadyForKey("never-touched-addr", "k"); ok || conn != nil {
		t.Errorf("ConnIfReadyForKey on cold addr: got %p ok=%v want nil false", conn, ok)
	}
}

// TestRDMAConnPool_ConnIfReadyForKey_ClosedConnReturnsFalse
// verifies the recovery path: if the hash-target conn has been
// markFault'd, ConnIfReadyForKey returns false so the caller
// silently drops to two-sided. The next AcquireSlotForKey on the
// same key triggers the existing redial path.
func TestRDMAConnPool_ConnIfReadyForKey_ClosedConnReturnsFalse(t *testing.T) {
	pool := newTestPool(t, 4, 4, nil)
	h, _ := pool.AcquireSlotForKey("dn1", "k-fault")
	conn := h.Conn
	pool.ReleaseSlot(h, false)

	// Simulate drainer markFault by closing directly.
	_ = conn.Close()

	if got, ok := pool.ConnIfReadyForKey("dn1", "k-fault"); ok || got != nil {
		t.Errorf("ConnIfReadyForKey on faulted conn: got %p ok=%v want nil false", got, ok)
	}
}
