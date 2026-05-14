// Copyright 2026 The CubeFS Authors.
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

package master

import (
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// -----------------------------------------------------------------------
// Test scaffolding
// -----------------------------------------------------------------------

// stubSource is an in-memory syncDispatcherSource for unit tests.
type stubSource struct {
	mu    sync.Mutex
	nodes map[string]*SyncNode
}

func newStubSource() *stubSource {
	return &stubSource{nodes: make(map[string]*SyncNode)}
}

func (s *stubSource) set(addr string, sn *SyncNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[addr] = sn
}

func (s *stubSource) rangeSyncNodes(visit func(addr string, sn *SyncNode) bool) {
	s.mu.Lock()
	// snapshot keys so we can release the lock; SyncNode has its own
	// RLock for the runtime fields.
	addrs := make([]string, 0, len(s.nodes))
	nodes := make([]*SyncNode, 0, len(s.nodes))
	for k, v := range s.nodes {
		addrs = append(addrs, k)
		nodes = append(nodes, v)
	}
	s.mu.Unlock()
	for i, addr := range addrs {
		if !visit(addr, nodes[i]) {
			return
		}
	}
}

// makeNode returns a *SyncNode with the fields the dispatcher reads
// already populated. ReportTime defaults to now; pass overrides via
// the opts function.
func makeNode(addr string, opts func(*SyncNode)) *SyncNode {
	sn := newSyncNode(addr, "test-cluster")
	sn.ReportTime = time.Now()
	sn.IsActive = true
	sn.BoltDBHealthy = true
	if opts != nil {
		opts(sn)
	}
	return sn
}

// fixedNow returns a fixed-time function for deterministic tests.
func fixedNow(t time.Time) func() time.Time { return func() time.Time { return t } }

// -----------------------------------------------------------------------
// computeLoadScore tests
// -----------------------------------------------------------------------

func TestDispatcher_ComputeLoadScore_HealthyNode(t *testing.T) {
	now := time.Now()
	sn := makeNode("10.0.0.1:17030", func(n *SyncNode) {
		n.ReportTime = now
		n.RunningTasks = 4 // 4/8 = 0.5
		n.CPUPercent = 50  // 0.5
	})
	got := computeLoadScore(sn, now)
	// 0.4*0.5 + 0.3*0 + 0.2*0.5 + 0.1*0 = 0.30
	want := 0.30
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("LoadScore = %v, want %v", got, want)
	}
}

func TestDispatcher_ComputeLoadScore_StaleNode(t *testing.T) {
	now := time.Now()
	sn := makeNode("10.0.0.1:17030", func(n *SyncNode) {
		n.ReportTime = now.Add(-2 * dispatcherStaleness)
	})
	got := computeLoadScore(sn, now)
	if !math.IsInf(got, 1) {
		t.Fatalf("stale node should score +Inf, got %v", got)
	}
}

func TestDispatcher_ComputeLoadScore_UnhealthyBolt(t *testing.T) {
	now := time.Now()
	sn := makeNode("10.0.0.1:17030", func(n *SyncNode) {
		n.BoltDBHealthy = false
	})
	got := computeLoadScore(sn, now)
	if !math.IsInf(got, 1) {
		t.Fatalf("bolt-unhealthy node should score +Inf, got %v", got)
	}
}

func TestDispatcher_ComputeLoadScore_NilNode(t *testing.T) {
	got := computeLoadScore(nil, time.Now())
	if !math.IsInf(got, 1) {
		t.Fatalf("nil node should score +Inf, got %v", got)
	}
}

func TestDispatcher_ComputeLoadScore_InactiveNode(t *testing.T) {
	now := time.Now()
	sn := makeNode("10.0.0.1:17030", func(n *SyncNode) {
		n.IsActive = false
	})
	got := computeLoadScore(sn, now)
	if !math.IsInf(got, 1) {
		t.Fatalf("inactive node should score +Inf, got %v", got)
	}
}

// -----------------------------------------------------------------------
// Candidates tests
// -----------------------------------------------------------------------

func TestDispatcher_Candidates_SortedByScoreAsc(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("a:1", makeNode("a:1", func(n *SyncNode) { n.RunningTasks = 6; n.ReportTime = now })) // score 0.30
	src.set("b:2", makeNode("b:2", func(n *SyncNode) { n.RunningTasks = 1; n.ReportTime = now })) // score 0.05
	src.set("c:3", makeNode("c:3", func(n *SyncNode) { n.RunningTasks = 4; n.ReportTime = now })) // score 0.20

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	got := d.Candidates(dispatcherStaleness)
	want := []string{"b:2", "c:3", "a:1"}
	if !equalStrings(got, want) {
		t.Fatalf("Candidates = %v, want %v", got, want)
	}
}

func TestDispatcher_Candidates_TieBreakByLastDispatch(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	// Two nodes with identical load (running=1, cpu=0 → score 0.05).
	src.set("a:1", makeNode("a:1", func(n *SyncNode) { n.RunningTasks = 1; n.ReportTime = now }))
	src.set("b:2", makeNode("b:2", func(n *SyncNode) { n.RunningTasks = 1; n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)
	// Simulate that "a:1" was dispatched-to most recently → "b:2" should
	// win the tiebreak.
	d.lastDispatchAt["a:1"] = now
	// b:2 has zero-valued time, which Before(now) → true → b first.

	got := d.Candidates(dispatcherStaleness)
	if len(got) != 2 {
		t.Fatalf("Candidates len = %d, want 2 (%v)", len(got), got)
	}
	if got[0] != "b:2" {
		t.Fatalf("tiebreak winner = %v, want b:2 (full=%v)", got[0], got)
	}
}

func TestDispatcher_Candidates_SkipsStaleAndUnhealthy(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("alive:1", makeNode("alive:1", func(n *SyncNode) { n.ReportTime = now }))
	src.set("stale:2", makeNode("stale:2", func(n *SyncNode) { n.ReportTime = now.Add(-1 * time.Hour) }))
	src.set("bolt:3", makeNode("bolt:3", func(n *SyncNode) { n.ReportTime = now; n.BoltDBHealthy = false }))
	src.set("inactive:4", makeNode("inactive:4", func(n *SyncNode) { n.ReportTime = now; n.IsActive = false }))
	src.set("saturated:5", makeNode("saturated:5", func(n *SyncNode) {
		n.ReportTime = now
		n.RunningTasks = int64(defaultMaxConcurrentTasks)
	}))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)
	got := d.Candidates(dispatcherStaleness)
	if !equalStrings(got, []string{"alive:1"}) {
		t.Fatalf("Candidates = %v, want [alive:1]", got)
	}
}

// -----------------------------------------------------------------------
// Dispatch tests
// -----------------------------------------------------------------------

func TestDispatcher_Dispatch_PicksLowestLoad(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("hot:1", makeNode("hot:1", func(n *SyncNode) { n.RunningTasks = 6; n.ReportTime = now }))
	src.set("cold:2", makeNode("cold:2", func(n *SyncNode) { n.RunningTasks = 1; n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)
	called := ""
	addr, err := d.Dispatch("task-1", func(a string) error {
		called = a
		return nil
	}, 3)
	if err != nil {
		t.Fatalf("Dispatch err = %v", err)
	}
	if addr != "cold:2" || called != "cold:2" {
		t.Fatalf("Dispatch addr = %q called = %q, want cold:2/cold:2", addr, called)
	}
	if owner := d.OwnerOf("task-1"); owner != "cold:2" {
		t.Fatalf("OwnerOf(task-1) = %q, want cold:2", owner)
	}
	if owned := d.TasksOwnedBy("cold:2"); len(owned) != 1 || owned[0] != "task-1" {
		t.Fatalf("TasksOwnedBy(cold:2) = %v, want [task-1]", owned)
	}
}

func TestDispatcher_Dispatch_FallsBackOnNack(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("first:1", makeNode("first:1", func(n *SyncNode) { n.RunningTasks = 1; n.ReportTime = now }))   // 0.05
	src.set("second:2", makeNode("second:2", func(n *SyncNode) { n.RunningTasks = 3; n.ReportTime = now })) // 0.15
	src.set("third:3", makeNode("third:3", func(n *SyncNode) { n.RunningTasks = 5; n.ReportTime = now }))   // 0.25

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	attempts := []string{}
	addr, err := d.Dispatch("task-x", func(a string) error {
		attempts = append(attempts, a)
		if a == "first:1" || a == "second:2" {
			return errors.New("nack")
		}
		return nil
	}, 3)
	if err != nil {
		t.Fatalf("Dispatch err = %v", err)
	}
	if addr != "third:3" {
		t.Fatalf("Dispatch addr = %q, want third:3", addr)
	}
	if !equalStrings(attempts, []string{"first:1", "second:2", "third:3"}) {
		t.Fatalf("attempts = %v, want [first:1 second:2 third:3]", attempts)
	}
}

func TestDispatcher_Dispatch_AllNackReturnsError(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("a:1", makeNode("a:1", func(n *SyncNode) { n.ReportTime = now }))
	src.set("b:2", makeNode("b:2", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	var calls int32
	_, err := d.Dispatch("task-x", func(a string) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("always-nack")
	}, 3)
	if err == nil {
		t.Fatalf("Dispatch should fail when every candidate nacks")
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("call count = %d, want 2 (one per candidate)", atomic.LoadInt32(&calls))
	}
	if d.OwnerOf("task-x") != "" {
		t.Fatalf("failed dispatch must not record ownership")
	}
}

func TestDispatcher_Dispatch_RespectsMaxRetries(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	// 5 healthy nodes, all will nack.
	for _, addr := range []string{"a:1", "b:2", "c:3", "d:4", "e:5"} {
		addr := addr
		src.set(addr, makeNode(addr, func(n *SyncNode) { n.ReportTime = now }))
	}

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	var calls int32
	_, err := d.Dispatch("task-x", func(a string) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("nack")
	}, 3)
	if err == nil {
		t.Fatalf("Dispatch should fail")
	}
	// maxRetries=3 → up to 4 attempts (initial + 3 fallbacks)
	if got := atomic.LoadInt32(&calls); got != 4 {
		t.Fatalf("call count = %d, want 4 (initial + 3 retries)", got)
	}
}

func TestDispatcher_Dispatch_EmptyFleet(t *testing.T) {
	d := newSyncDispatcherFromSource(newStubSource())
	d.now = fixedNow(time.Now())
	_, err := d.Dispatch("task-x", func(a string) error { return nil }, 3)
	if !errors.Is(err, ErrNoCandidates) {
		t.Fatalf("err = %v, want ErrNoCandidates", err)
	}
}

func TestDispatcher_Dispatch_EmptyTaskID(t *testing.T) {
	src := newStubSource()
	src.set("a:1", makeNode("a:1", nil))
	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(time.Now())
	_, err := d.Dispatch("", func(a string) error { return nil }, 3)
	if err == nil {
		t.Fatalf("Dispatch with empty taskID should error")
	}
}

// -----------------------------------------------------------------------
// Release / ownership tests
// -----------------------------------------------------------------------

func TestDispatcher_Release_DropsOwnership(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("a:1", makeNode("a:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	if _, err := d.Dispatch("t1", func(a string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if d.OwnerOf("t1") != "a:1" {
		t.Fatalf("pre-release: OwnerOf(t1) = %q", d.OwnerOf("t1"))
	}
	d.Release("t1")
	if d.OwnerOf("t1") != "" {
		t.Fatalf("post-release: OwnerOf(t1) = %q, want \"\"", d.OwnerOf("t1"))
	}
	if owned := d.TasksOwnedBy("a:1"); len(owned) != 0 {
		t.Fatalf("post-release: TasksOwnedBy = %v, want empty", owned)
	}
	// Release of unknown taskID is a no-op.
	d.Release("ghost")
}

func TestDispatcher_HandleNodeDeath_ReleasesAllOwned(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	for _, tid := range []string{"t1", "t2", "t3"} {
		if _, err := d.Dispatch(tid, func(a string) error { return nil }, 3); err != nil {
			t.Fatalf("dispatch %s: %v", tid, err)
		}
	}
	if owned := d.TasksOwnedBy("dying:1"); len(owned) != 3 {
		t.Fatalf("pre-death: TasksOwnedBy = %v, want 3", owned)
	}
	d.handleNodeDeath("dying:1")
	for _, tid := range []string{"t1", "t2", "t3"} {
		if d.OwnerOf(tid) != "" {
			t.Fatalf("post-death: OwnerOf(%s) = %q, want \"\"", tid, d.OwnerOf(tid))
		}
	}
	if owned := d.TasksOwnedBy("dying:1"); len(owned) != 0 {
		t.Fatalf("post-death: TasksOwnedBy = %v, want empty", owned)
	}
}

// -----------------------------------------------------------------------
// P1-4: failover hook plumbing
// -----------------------------------------------------------------------

// TestDispatcher_HandleNodeDeath_NoHookIsNoop verifies that handleNodeDeath
// without a hook just releases ownership — no panic, no work, callers
// see an empty owned set.
func TestDispatcher_HandleNodeDeath_NoHookIsNoop(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)
	if _, err := d.Dispatch("t1", func(a string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// No hook installed — must not panic.
	d.handleNodeDeath("dying:1")

	if d.OwnerOf("t1") != "" {
		t.Fatalf("OwnerOf(t1) post-death = %q, want empty", d.OwnerOf("t1"))
	}
	if owned := d.TasksOwnedBy("dying:1"); len(owned) != 0 {
		t.Fatalf("TasksOwnedBy = %v, want empty", owned)
	}
}

// TestDispatcher_HandleNodeDeath_HookCalledOncePerTask installs a counter
// and confirms every previously-owned task triggers the hook exactly once.
func TestDispatcher_HandleNodeDeath_HookCalledOncePerTask(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	for _, tid := range []string{"t1", "t2", "t3"} {
		if _, err := d.Dispatch(tid, func(a string) error { return nil }, 3); err != nil {
			t.Fatalf("dispatch %s: %v", tid, err)
		}
	}

	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	d.WithFailoverHook(func(taskID string) error {
		mu.Lock()
		seen[taskID]++
		mu.Unlock()
		return nil
	})

	d.handleNodeDeath("dying:1")

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("hook calls seen for %d task(s), want 3 (got %v)", len(seen), seen)
	}
	for _, tid := range []string{"t1", "t2", "t3"} {
		if seen[tid] != 1 {
			t.Fatalf("hook for %q called %d times, want 1", tid, seen[tid])
		}
	}
}

// TestDispatcher_HandleNodeDeath_HookErrorsDoNotAbortLoop installs a hook
// that returns an error for the first task it sees; the dispatcher must
// still call it for the remaining tasks.
func TestDispatcher_HandleNodeDeath_HookErrorsDoNotAbortLoop(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	tasks := []string{"t1", "t2", "t3", "t4"}
	for _, tid := range tasks {
		if _, err := d.Dispatch(tid, func(a string) error { return nil }, 3); err != nil {
			t.Fatalf("dispatch %s: %v", tid, err)
		}
	}

	var (
		mu    sync.Mutex
		calls int
	)
	d.WithFailoverHook(func(taskID string) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return errors.New("simulated redispatch failure")
	})

	d.handleNodeDeath("dying:1")

	mu.Lock()
	defer mu.Unlock()
	if calls != len(tasks) {
		t.Fatalf("hook calls = %d, want %d (loop aborted prematurely)", calls, len(tasks))
	}
}

// TestDispatcher_WithFailoverHook_Replaces verifies a second install
// overrides the first — only the most recent hook fires on subsequent
// deaths.
func TestDispatcher_WithFailoverHook_Replaces(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)
	if _, err := d.Dispatch("t1", func(a string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	var firstCalls, secondCalls int32
	d.WithFailoverHook(func(_ string) error { atomic.AddInt32(&firstCalls, 1); return nil })
	d.WithFailoverHook(func(_ string) error { atomic.AddInt32(&secondCalls, 1); return nil })

	d.handleNodeDeath("dying:1")
	if atomic.LoadInt32(&firstCalls) != 0 {
		t.Fatalf("first hook should have been replaced, calls = %d", atomic.LoadInt32(&firstCalls))
	}
	if atomic.LoadInt32(&secondCalls) != 1 {
		t.Fatalf("second hook calls = %d, want 1", atomic.LoadInt32(&secondCalls))
	}
}

// -----------------------------------------------------------------------
// P1-2 acceptance: distribution std-dev ≤ 30% of mean across 3 nodes
// for 10 sequential dispatches.
// -----------------------------------------------------------------------

func TestDispatcher_Distribution_AcrossThreeNodes(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	// Three identical nodes — tiebreak should round-robin.
	src.set("a:1", makeNode("a:1", func(n *SyncNode) { n.ReportTime = now; n.RunningTasks = 0 }))
	src.set("b:2", makeNode("b:2", func(n *SyncNode) { n.ReportTime = now; n.RunningTasks = 0 }))
	src.set("c:3", makeNode("c:3", func(n *SyncNode) { n.ReportTime = now; n.RunningTasks = 0 }))

	clock := now
	d := newSyncDispatcherFromSource(src)
	d.now = func() time.Time { return clock }

	counts := map[string]int{}
	for i := 0; i < 10; i++ {
		clock = clock.Add(time.Millisecond)
		addr, err := d.Dispatch(taskName(i), func(a string) error { return nil }, 3)
		if err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
		counts[addr]++
		// Simulate running-task accounting bumping for the chosen node.
		// This is what closes the loop in production — heartbeats update
		// running counts. We mutate the stub directly.
		src.mu.Lock()
		sn := src.nodes[addr]
		src.mu.Unlock()
		sn.Lock()
		sn.RunningTasks++
		sn.Unlock()
	}
	// Compute std-dev / mean of counts.
	values := []float64{
		float64(counts["a:1"]),
		float64(counts["b:2"]),
		float64(counts["c:3"]),
	}
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))
	sq := 0.0
	for _, v := range values {
		sq += (v - mean) * (v - mean)
	}
	std := math.Sqrt(sq / float64(len(values)))
	if mean == 0 {
		t.Fatalf("mean = 0, counts = %v", counts)
	}
	if std/mean > 0.30 {
		t.Fatalf("std/mean = %.2f > 0.30 (counts=%v)", std/mean, counts)
	}
}

// -----------------------------------------------------------------------
// Concurrency: Dispatch + Release under -race
// -----------------------------------------------------------------------

func TestDispatcher_Concurrent_DispatchRelease(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	for _, addr := range []string{"a:1", "b:2", "c:3", "d:4"} {
		addr := addr
		src.set(addr, makeNode(addr, func(n *SyncNode) { n.ReportTime = now }))
	}

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	var wg sync.WaitGroup
	const N = 200
	for i := 0; i < N; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = d.Dispatch(taskName(i), func(a string) error { return nil }, 3)
		}()
		go func() {
			defer wg.Done()
			d.Release(taskName(i))
		}()
	}
	wg.Wait()
}

// -----------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func taskName(i int) string {
	return "task-" + itoa(i)
}

// itoa avoids strconv import noise in tests.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}

// keep sort imported (defensive against go vet drift)
var _ = sort.Strings
