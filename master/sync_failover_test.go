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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

// -----------------------------------------------------------------------
// Test scaffolding
// -----------------------------------------------------------------------

// stubFailoverCluster is an in-memory syncFailoverCluster for unit
// tests. It records every sendRunTask call so the test can assert
// "task X was sent to node Y" without spinning up real TaskManagers.
type stubFailoverCluster struct {
	mu       sync.Mutex
	live     map[string]struct{}   // addrs the stub considers registered
	rejectIn map[string]error      // addr → error to return from sendRunTask
	sent     []stubFailoverSendRec // chronological log of sendRunTask calls
}

type stubFailoverSendRec struct {
	Addr   string
	OpCode uint8
}

func newStubFailoverCluster(addrs ...string) *stubFailoverCluster {
	s := &stubFailoverCluster{
		live:     make(map[string]struct{}),
		rejectIn: make(map[string]error),
	}
	for _, a := range addrs {
		s.live[a] = struct{}{}
	}
	return s
}

func (s *stubFailoverCluster) sendRunTask(addr string, task *proto.AdminTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, bad := s.rejectIn[addr]; bad {
		return err
	}
	if _, ok := s.live[addr]; !ok {
		return fmt.Errorf("stub: syncnode %s not registered", addr)
	}
	s.sent = append(s.sent, stubFailoverSendRec{Addr: addr, OpCode: task.OpCode})
	return nil
}

func (s *stubFailoverCluster) sentCopy() []stubFailoverSendRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]stubFailoverSendRec, len(s.sent))
	copy(out, s.sent)
	return out
}

// runTaskPayload builds the AdminTask that production stores via
// SyncFailover.Remember.
func runTaskPayload(taskID, addr string) *proto.AdminTask {
	t := proto.NewAdminTask(proto.OpSyncNodeRunTask, addr, map[string]string{
		"ruleID": taskID,
	})
	t.ID = taskID
	return t
}

// -----------------------------------------------------------------------
// Constructor wiring
// -----------------------------------------------------------------------

// TestFailover_NewSyncFailoverInstallsHook ensures the constructor wires
// f.redispatch as the dispatcher's failover hook — Death must trigger it.
func TestFailover_NewSyncFailoverInstallsHook(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("a:1", makeNode("a:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("a:1")
	f := newSyncFailoverFromSource(cluster, d)

	// Dispatch then immediately kill the only node — no payload remembered,
	// so redispatch should record "no saved payload" but still be invoked
	// (which is the proof the hook is wired).
	if _, err := d.Dispatch("t1", func(string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	d.handleNodeDeath("a:1")

	recent := f.Recent(0)
	if len(recent) != 1 {
		t.Fatalf("Recent() len = %d, want 1 (records = %+v)", len(recent), recent)
	}
	if recent[0].TaskID != "t1" {
		t.Fatalf("Recent()[0].TaskID = %q, want t1", recent[0].TaskID)
	}
	if recent[0].Err == "" {
		t.Fatalf("Recent()[0].Err should mention missing payload, got empty")
	}
}

// -----------------------------------------------------------------------
// Remember + redispatch happy path
// -----------------------------------------------------------------------

// TestFailover_RedispatchPicksNewNode walks the full happy path: two
// candidate nodes, dispatch to one, kill it, remember-saved payload
// re-dispatches to the other.
func TestFailover_RedispatchPicksNewNode(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now; n.RunningTasks = 0 }))
	src.set("backup:2", makeNode("backup:2", func(n *SyncNode) { n.ReportTime = now; n.RunningTasks = 0 }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("dying:1", "backup:2")
	f := newSyncFailoverFromSource(cluster, d)

	// Force initial dispatch onto dying:1 by saying it was dispatched-to
	// later (so tie-break favors backup… wait, that's the wrong direction).
	// We need dying:1 to be chosen first. Make backup:2 carry more load.
	src.mu.Lock()
	src.nodes["backup:2"].RunningTasks = 4 // higher load
	src.mu.Unlock()

	addr, err := d.Dispatch("t1", func(string) error { return nil }, 3)
	if err != nil {
		t.Fatalf("initial dispatch: %v", err)
	}
	if addr != "dying:1" {
		t.Fatalf("initial owner = %q, want dying:1 (verify test fixture)", addr)
	}
	f.Remember("t1", runTaskPayload("t1", "dying:1"))

	// Now simulate dying:1 going dark from the dispatcher's POV.
	src.mu.Lock()
	src.nodes["dying:1"].IsActive = false
	src.mu.Unlock()

	d.handleNodeDeath("dying:1")

	sent := cluster.sentCopy()
	if len(sent) != 1 || sent[0].Addr != "backup:2" {
		t.Fatalf("sendRunTask log = %+v, want [{backup:2, OpSyncNodeRunTask}]", sent)
	}
	if sent[0].OpCode != proto.OpSyncNodeRunTask {
		t.Fatalf("OpCode = %v, want OpSyncNodeRunTask", sent[0].OpCode)
	}

	recent := f.Recent(0)
	if len(recent) != 1 {
		t.Fatalf("Recent() len = %d, want 1", len(recent))
	}
	if recent[0].TaskID != "t1" || recent[0].ToAddr != "backup:2" || recent[0].Err != "" {
		t.Fatalf("Recent()[0] = %+v, want {TaskID:t1, ToAddr:backup:2, Err:\"\"}", recent[0])
	}
	if d.OwnerOf("t1") != "backup:2" {
		t.Fatalf("OwnerOf(t1) post-failover = %q, want backup:2", d.OwnerOf("t1"))
	}
}

// -----------------------------------------------------------------------
// Dead-letter path
// -----------------------------------------------------------------------

// TestFailover_RedispatchNoCandidates_DeadLetters confirms that when the
// only remaining node is unhealthy, the orphan lands in DeadLetter() and
// Recent() carries the error.
func TestFailover_RedispatchNoCandidates_DeadLetters(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("solo:1", makeNode("solo:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("solo:1")
	f := newSyncFailoverFromSource(cluster, d)

	if _, err := d.Dispatch("t1", func(string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	f.Remember("t1", runTaskPayload("t1", "solo:1"))

	// The only node dies; no candidates remain for re-dispatch.
	src.mu.Lock()
	src.nodes["solo:1"].IsActive = false
	src.mu.Unlock()

	d.handleNodeDeath("solo:1")

	dead := f.DeadLetter()
	if len(dead) != 1 {
		t.Fatalf("DeadLetter() = %+v, want exactly 1 entry", dead)
	}
	if _, ok := dead["t1"]; !ok {
		t.Fatalf("DeadLetter() missing t1: %+v", dead)
	}
	recent := f.Recent(0)
	if len(recent) != 1 || recent[0].Err == "" {
		t.Fatalf("Recent() = %+v, want one entry with non-empty Err", recent)
	}
	if recent[0].ToAddr != "" {
		t.Fatalf("Recent()[0].ToAddr = %q, want empty (failed redispatch)", recent[0].ToAddr)
	}
}

// TestFailover_RedispatchSendNack_DeadLetters confirms that if every live
// candidate's sendRunTask returns an error, the task is dead-lettered.
func TestFailover_RedispatchSendNack_DeadLetters(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))
	src.set("backup:2", makeNode("backup:2", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("dying:1", "backup:2")
	cluster.rejectIn["backup:2"] = errors.New("simulated send failure")
	f := newSyncFailoverFromSource(cluster, d)

	if _, err := d.Dispatch("t1", func(string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	f.Remember("t1", runTaskPayload("t1", "dying:1"))

	src.mu.Lock()
	src.nodes["dying:1"].IsActive = false
	src.mu.Unlock()

	d.handleNodeDeath("dying:1")

	dead := f.DeadLetter()
	if _, ok := dead["t1"]; !ok {
		t.Fatalf("DeadLetter() missing t1: %+v", dead)
	}
}

// -----------------------------------------------------------------------
// Forget + missing-payload path
// -----------------------------------------------------------------------

// TestFailover_ForgetMakesRedispatchNoop verifies a Forget()'d task does
// not get re-sent on owner death; instead it records a benign "no saved
// payload" entry.
func TestFailover_ForgetMakesRedispatchNoop(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))
	src.set("backup:2", makeNode("backup:2", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("dying:1", "backup:2")
	f := newSyncFailoverFromSource(cluster, d)

	if _, err := d.Dispatch("t1", func(string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	f.Remember("t1", runTaskPayload("t1", "dying:1"))
	f.Forget("t1")

	d.handleNodeDeath(d.OwnerOf("t1"))

	if got := cluster.sentCopy(); len(got) != 0 {
		t.Fatalf("Forget should suppress redispatch, got sends = %+v", got)
	}
	recent := f.Recent(0)
	if len(recent) != 1 || recent[0].Err == "" {
		t.Fatalf("Recent() = %+v, want one no-payload entry", recent)
	}
}

// TestFailover_RememberClearsDeadLetter exercises the recovery path:
// after a dead-letter, a fresh Remember should drop the dead-letter
// flag so DeadLetter() reflects current reality.
func TestFailover_RememberClearsDeadLetter(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("solo:1", makeNode("solo:1", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("solo:1")
	f := newSyncFailoverFromSource(cluster, d)

	if _, err := d.Dispatch("t1", func(string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	f.Remember("t1", runTaskPayload("t1", "solo:1"))
	src.mu.Lock()
	src.nodes["solo:1"].IsActive = false
	src.mu.Unlock()
	d.handleNodeDeath("solo:1")
	if _, ok := f.DeadLetter()["t1"]; !ok {
		t.Fatalf("setup: expected t1 in dead-letter")
	}

	// Operator re-dispatches manually; new Remember must clear the flag.
	f.Remember("t1", runTaskPayload("t1", "solo:1"))
	if _, ok := f.DeadLetter()["t1"]; ok {
		t.Fatalf("Remember should clear dead-letter; still present: %+v", f.DeadLetter())
	}
}

// -----------------------------------------------------------------------
// Recent() bounding
// -----------------------------------------------------------------------

// TestFailover_RecentCapsRecords ensures Recent(N) returns at most N
// entries (most recent first when ordered) and respects the internal
// failoverHistoryCap.
func TestFailover_RecentCapsRecords(t *testing.T) {
	cluster := newStubFailoverCluster()
	d := newSyncDispatcherFromSource(newStubSource())
	d.now = fixedNow(time.Now())
	f := newSyncFailoverFromSource(cluster, d)

	for i := 0; i < failoverHistoryCap+10; i++ {
		f.appendHistory(FailoverRecord{TaskID: fmt.Sprintf("t-%d", i)})
	}

	all := f.Recent(0)
	if len(all) != failoverHistoryCap {
		t.Fatalf("Recent(0) len = %d, want %d (cap)", len(all), failoverHistoryCap)
	}
	last5 := f.Recent(5)
	if len(last5) != 5 {
		t.Fatalf("Recent(5) len = %d, want 5", len(last5))
	}
	// Ring drops oldest; the most-recent entry must be t-{cap+9}.
	wantLast := fmt.Sprintf("t-%d", failoverHistoryCap+9)
	if last5[len(last5)-1].TaskID != wantLast {
		t.Fatalf("last record TaskID = %q, want %q", last5[len(last5)-1].TaskID, wantLast)
	}
}

// -----------------------------------------------------------------------
// handleNodeDeath fan-out: 3 owned tasks → 3 history entries
// -----------------------------------------------------------------------

func TestFailover_HandleNodeDeathProducesOneRecordPerTask(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))
	src.set("backup:2", makeNode("backup:2", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("dying:1", "backup:2")
	f := newSyncFailoverFromSource(cluster, d)

	// Steer dispatches to dying:1 by making backup:2 look loaded.
	src.mu.Lock()
	src.nodes["backup:2"].RunningTasks = 6
	src.mu.Unlock()

	tasks := []string{"t1", "t2", "t3"}
	for _, tid := range tasks {
		addr, err := d.Dispatch(tid, func(string) error { return nil }, 3)
		if err != nil {
			t.Fatalf("dispatch %s: %v", tid, err)
		}
		if addr != "dying:1" {
			t.Fatalf("%s landed on %s, want dying:1", tid, addr)
		}
		f.Remember(tid, runTaskPayload(tid, "dying:1"))
	}

	// Open the door for backup to win redispatch.
	src.mu.Lock()
	src.nodes["dying:1"].IsActive = false
	src.nodes["backup:2"].RunningTasks = 0
	src.mu.Unlock()

	d.handleNodeDeath("dying:1")

	recent := f.Recent(0)
	if len(recent) != len(tasks) {
		t.Fatalf("Recent() len = %d, want %d", len(recent), len(tasks))
	}
	seenTaskIDs := map[string]bool{}
	for _, r := range recent {
		if r.ToAddr != "backup:2" || r.Err != "" {
			t.Fatalf("record %+v: want ToAddr=backup:2 + empty Err", r)
		}
		seenTaskIDs[r.TaskID] = true
	}
	for _, tid := range tasks {
		if !seenTaskIDs[tid] {
			t.Fatalf("task %s missing from history: %+v", tid, recent)
		}
	}
	sent := cluster.sentCopy()
	if len(sent) != len(tasks) {
		t.Fatalf("sendRunTask called %d time(s), want %d", len(sent), len(tasks))
	}
}

// -----------------------------------------------------------------------
// Forget on unknown taskID is a no-op
// -----------------------------------------------------------------------

func TestFailover_ForgetUnknownIsNoop(t *testing.T) {
	cluster := newStubFailoverCluster()
	d := newSyncDispatcherFromSource(newStubSource())
	d.now = fixedNow(time.Now())
	f := newSyncFailoverFromSource(cluster, d)
	f.Forget("never-existed") // must not panic
}

// -----------------------------------------------------------------------
// Remember rejects empty + nil inputs
// -----------------------------------------------------------------------

func TestFailover_RememberIgnoresEmptyAndNil(t *testing.T) {
	cluster := newStubFailoverCluster()
	d := newSyncDispatcherFromSource(newStubSource())
	d.now = fixedNow(time.Now())
	f := newSyncFailoverFromSource(cluster, d)

	f.Remember("", runTaskPayload("x", "a:1"))
	f.Remember("t1", nil)

	if len(f.payloads) != 0 {
		t.Fatalf("payloads should remain empty, got %+v", f.payloads)
	}
}

// -----------------------------------------------------------------------
// Concurrent Remember + redispatch under -race
// -----------------------------------------------------------------------

func TestFailover_ConcurrentRememberAndDeath(t *testing.T) {
	now := time.Now()
	src := newStubSource()
	src.set("dying:1", makeNode("dying:1", func(n *SyncNode) { n.ReportTime = now }))
	src.set("backup:2", makeNode("backup:2", func(n *SyncNode) { n.ReportTime = now }))

	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)

	cluster := newStubFailoverCluster("dying:1", "backup:2")
	f := newSyncFailoverFromSource(cluster, d)

	var wg sync.WaitGroup
	const N = 200
	var deaths int32
	for i := 0; i < N; i++ {
		i := i
		wg.Add(3)
		go func() {
			defer wg.Done()
			f.Remember(fmt.Sprintf("t-%d", i), runTaskPayload(fmt.Sprintf("t-%d", i), "dying:1"))
		}()
		go func() {
			defer wg.Done()
			f.Forget(fmt.Sprintf("t-%d", i))
		}()
		go func() {
			defer wg.Done()
			d.handleNodeDeath("dying:1")
			atomic.AddInt32(&deaths, 1)
		}()
	}
	wg.Wait()
	// Sanity: Recent() must not have grown beyond cap.
	if got := len(f.Recent(0)); got > failoverHistoryCap {
		t.Fatalf("Recent() len = %d > cap %d", got, failoverHistoryCap)
	}
}
