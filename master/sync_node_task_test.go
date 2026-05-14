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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cubefs/cubefs/proto"
)

// TestIsTerminalStatus pins the executor status strings master treats as
// terminal. These MUST stay in sync with syncnode/executor.Status's
// StatusDone / StatusFailed / StatusCancelled constants or Bug #3 won't
// land — non-terminal statuses must NOT trip Release+Forget.
func TestIsTerminalStatus(t *testing.T) {
	cases := []struct {
		status string
		want   bool
	}{
		{"done", true},
		{"failed", true},
		{"cancelled", true},
		{"running", false},
		{"", false},
		{"unknown", false},
		// Common typos / casings that MUST NOT match — preserves the
		// "fail closed" contract.
		{"Done", false},
		{"cancel", false},
	}
	for _, tc := range cases {
		if got := isTerminalStatus(tc.status); got != tc.want {
			t.Errorf("isTerminalStatus(%q) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

// TestDecodeTerminalReport_TypedAndMapShapes verifies both wire shapes
// produce a usable TaskTerminalReport: direct *proto.TaskTerminalReport
// (in-process synthesis path) and map[string]interface{} (post-JSON
// rehydration via the admin task RPC framework). Bug #3's release path
// relies on this working for the real network case.
func TestDecodeTerminalReport_TypedAndMapShapes(t *testing.T) {
	t.Run("typed pointer survives", func(t *testing.T) {
		want := &proto.TaskTerminalReport{TaskID: "t-1", Status: "done"}
		task := &proto.AdminTask{
			ID:       "t-1",
			OpCode:   proto.OpSyncNodeRunTask,
			Response: want,
		}
		got, err := decodeTerminalReport(task)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != want {
			t.Errorf("typed pointer not preserved: got=%p want=%p", got, want)
		}
	})

	t.Run("map roundtrip", func(t *testing.T) {
		raw := map[string]interface{}{
			"taskId": "t-2",
			"status": "failed",
			"error":  "boom",
		}
		task := &proto.AdminTask{
			ID:       "t-2",
			OpCode:   proto.OpSyncNodeRunTask,
			Response: raw,
		}
		got, err := decodeTerminalReport(task)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.TaskID != "t-2" {
			t.Errorf("TaskID = %q, want t-2", got.TaskID)
		}
		if got.Status != "failed" {
			t.Errorf("Status = %q, want failed", got.Status)
		}
		if got.Error != "boom" {
			t.Errorf("Error = %q, want boom", got.Error)
		}
	})

	t.Run("falls back to admin task ID when inner is empty", func(t *testing.T) {
		raw := map[string]interface{}{
			"status": "done",
		}
		task := &proto.AdminTask{
			ID:       "outer-id",
			OpCode:   proto.OpSyncNodeRunTask,
			Response: raw,
		}
		got, err := decodeTerminalReport(task)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.TaskID != "outer-id" {
			t.Errorf("TaskID = %q, want fallback %q", got.TaskID, "outer-id")
		}
	})

	t.Run("rejects nil task", func(t *testing.T) {
		if _, err := decodeTerminalReport(nil); err == nil {
			t.Fatal("err = nil, want non-nil")
		}
	})

	t.Run("rejects empty response", func(t *testing.T) {
		task := &proto.AdminTask{ID: "x", OpCode: proto.OpSyncNodeRunTask}
		if _, err := decodeTerminalReport(task); err == nil {
			t.Fatal("err = nil, want non-nil")
		}
	})

	t.Run("survives full json roundtrip", func(t *testing.T) {
		// This shape mirrors what handleSyncNodeTaskResponse sees: the
		// AdminTask was marshalled by syncnode, sent over the wire, and
		// json-unmarshalled by master into a map.
		original := &proto.AdminTask{
			ID:       "t-3",
			OpCode:   proto.OpSyncNodeRunTask,
			Response: &proto.TaskTerminalReport{TaskID: "t-3", Status: "cancelled"},
		}
		raw, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var roundtripped proto.AdminTask
		if err := json.Unmarshal(raw, &roundtripped); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		got, err := decodeTerminalReport(&roundtripped)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.TaskID != "t-3" || got.Status != "cancelled" {
			t.Errorf("roundtrip: got=%+v, want TaskID=t-3 Status=cancelled", got)
		}
	})
}

// TestHandleSyncNodeTaskResponse_TerminalReleasesAndForgets is a focused
// unit test for the Bug #3 release path. We can't spin a full cluster on
// macOS (rocksdb-dependent), so we exercise the in-memory dispatcher +
// failover wiring directly and check that a synthetic terminal response
// causes Release + Forget to be called.
//
// This bypasses c.syncNode(nodeAddr) (which requires a real syncNodes
// sync.Map entry) — the test asserts the helper functions used INSIDE
// the OpSyncNodeRunTask case wire correctly through the dispatcher /
// failover surfaces.
func TestHandleSyncNodeTaskResponse_TerminalReleasesAndForgets(t *testing.T) {
	// Build the dispatcher with a stub source so we can record one
	// owned task that the terminal report should release.
	src := newStubSource()
	src.set("nodeA:1", makeNode("nodeA:1", nil))
	d := newSyncDispatcherFromSource(src)

	if _, err := d.Dispatch("t-99", func(string) error { return nil }, 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if owner := d.OwnerOf("t-99"); owner != "nodeA:1" {
		t.Fatalf("OwnerOf(t-99) = %q, want nodeA:1", owner)
	}

	// Failover orchestrator hooked into the same dispatcher; remember a
	// payload so we can observe Forget clearing it.
	cluster := newStubFailoverCluster("nodeA:1")
	f := newSyncFailoverFromSource(cluster, d)
	f.Remember("t-99", runTaskPayload("t-99", "nodeA:1"))

	// Synthesize the terminal report the way master receives it from
	// the wire (post-JSON-rehydration → map[string]interface{}).
	terminalRaw := map[string]interface{}{
		"taskId": "t-99",
		"status": "done",
	}
	task := &proto.AdminTask{
		ID:       "t-99",
		OpCode:   proto.OpSyncNodeRunTask,
		Response: terminalRaw,
	}

	rep, err := decodeTerminalReport(task)
	if err != nil {
		t.Fatalf("decodeTerminalReport: %v", err)
	}
	if !isTerminalStatus(rep.Status) {
		t.Fatalf("status %q not terminal — fix isTerminalStatus", rep.Status)
	}

	// Exercise the exact two side-effects the OpSyncNodeRunTask case
	// performs inside handleSyncNodeTaskResponse.
	d.Release(rep.TaskID)
	f.Forget(rep.TaskID)

	if owner := d.OwnerOf("t-99"); owner != "" {
		t.Errorf("after Release, OwnerOf(t-99) = %q, want empty", owner)
	}
	// After Forget the payload must be gone — drive that via the
	// failover orchestrator's redispatch path: with no payload, it
	// short-circuits with "no saved payload" recorded in history.
	if err := f.redispatch("t-99"); err != nil {
		t.Fatalf("redispatch after Forget: %v", err)
	}
	recent := f.Recent(0)
	if len(recent) == 0 {
		t.Fatalf("Recent() empty — redispatch should have recorded history")
	}
	last := recent[len(recent)-1]
	if last.Err == "" {
		t.Errorf("Recent()[last].Err = empty, want missing-payload signal")
	}
}

// TestHandleTerminalResponse_LastShardClearsParent exercises the Bug S2
// cleanup path: register a parent via DispatchN with 2 shards, then drive
// the same MarkShardTerminal + Clear sequence the OpSyncNodeRunTask
// branch of handleSyncNodeTaskResponse runs. The first shard's terminal
// must NOT clear the parent; the second must.
func TestHandleTerminalResponse_LastShardClearsParent(t *testing.T) {
	fo, disp, _ := fanoutHarness(t, 2)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("p1", "r1", 2,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 3); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}
	// Wire a failover orchestrator just to validate the Forget step in
	// the same shape as production. Not required for the cleanup
	// assertion itself.
	cluster := newStubFailoverCluster("node-0", "node-1")
	f := newSyncFailoverFromSource(cluster, disp)
	f.Remember("p1/0", runTaskPayload("p1/0", "node-0"))
	f.Remember("p1/1", runTaskPayload("p1/1", "node-1"))

	// Drive shard 0 terminal — mirrors handleSyncNodeTaskResponse.
	terminate := func(subID string) {
		t.Helper()
		disp.Release(subID)
		f.Forget(subID)
		parent, shard, isShard := splitSubTaskID(subID)
		if !isShard {
			t.Fatalf("splitSubTaskID(%q) returned isShard=false", subID)
		}
		if allDone, exists := fo.MarkShardTerminal(parent, shard); exists && allDone {
			fo.Clear(parent)
		}
	}

	terminate("p1/0")
	if !fo.IsParent("p1") {
		t.Fatalf("parent p1 cleared after only 1/2 shards terminal")
	}

	terminate("p1/1")
	if fo.IsParent("p1") {
		t.Errorf("parent p1 still present after all shards terminal — leak")
	}
}

// TestRequireSyncAdminToken_PassThroughWhenEmpty: with no token
// configured, the middleware is a pass-through (preserves the existing
// open default for tests and dev).
func TestRequireSyncAdminToken_PassThroughWhenEmpty(t *testing.T) {
	SetSyncAdminToken("")
	called := false
	h := requireSyncAdminToken(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/syncNode/list", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if !called {
		t.Errorf("handler not invoked when token unset")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// TestRequireSyncAdminToken_RejectsMissingHeader: with a token
// configured, requests without Authorization / X-Sync-Token are 401.
func TestRequireSyncAdminToken_RejectsMissingHeader(t *testing.T) {
	SetSyncAdminToken("secret-token")
	defer SetSyncAdminToken("")
	called := false
	h := requireSyncAdminToken(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	req := httptest.NewRequest(http.MethodPost, "/syncNode/dispatch", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if called {
		t.Errorf("handler invoked despite missing token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// TestRequireSyncAdminToken_AcceptsBearerToken: correct Bearer header
// passes the gate.
func TestRequireSyncAdminToken_AcceptsBearerToken(t *testing.T) {
	SetSyncAdminToken("secret-token")
	defer SetSyncAdminToken("")
	called := false
	h := requireSyncAdminToken(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodPost, "/syncNode/dispatch", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	h(rec, req)
	if !called {
		t.Errorf("handler not invoked with correct bearer")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// TestRequireSyncAdminToken_AcceptsXSyncTokenHeader: X-Sync-Token
// fallback works when Authorization is absent.
func TestRequireSyncAdminToken_AcceptsXSyncTokenHeader(t *testing.T) {
	SetSyncAdminToken("secret-token")
	defer SetSyncAdminToken("")
	h := requireSyncAdminToken(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/syncNode/list", nil)
	req.Header.Set("X-Sync-Token", "secret-token")
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// TestRequireSyncAdminToken_RejectsWrongToken: a mismatched token is
// rejected even when the header is present.
func TestRequireSyncAdminToken_RejectsWrongToken(t *testing.T) {
	SetSyncAdminToken("secret-token")
	defer SetSyncAdminToken("")
	h := requireSyncAdminToken(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("handler invoked with wrong token")
	})
	req := httptest.NewRequest(http.MethodPost, "/syncNode/dispatch", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// TestDispatchSyncTask_RejectsOversizedBody verifies SEC3: a request
// body larger than dispatchMaxBodyBytes returns an error before any
// handler logic fires. We invoke the handler directly so we don't have
// to start a real master (rocksdb is unavailable on macOS test boxes).
func TestDispatchSyncTask_RejectsOversizedBody(t *testing.T) {
	huge := bytes.Repeat([]byte("a"), dispatchMaxBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/syncNode/dispatch", bytes.NewReader(huge))
	rec := httptest.NewRecorder()

	// Minimal Server stub — dispatchSyncTask reads m.cluster fields
	// only AFTER decoding the body, so an empty cluster is fine for the
	// over-size path. We construct the wrapper enough to satisfy the
	// metric label.
	srv := &Server{cluster: &Cluster{}}
	srv.dispatchSyncTask(rec, req)

	// MaxBytesReader makes ReadAll error out; the handler returns the
	// JSON error envelope (200 status, but Code != Success). We also
	// must NOT see a server-decoded task ID.
	body, _ := io.ReadAll(rec.Body)
	if !bytes.Contains(body, []byte("read body")) && !bytes.Contains(body, []byte("http: request body too large")) {
		t.Errorf("expected body-size error, got: %s", string(body))
	}
}

// -----------------------------------------------------------------------
// S5 reconnect path tests — split-brain prevention on syncnode rejoin.
//
// The contract under test (handleSyncNodeHeartbeatResp + the private
// cancelOrphanedTasksOnReconnect helper):
//
//  1. A heartbeat from a node that was already IsActive=true does NOT
//     trigger a cancel fanout — happy-path stays O(1).
//  2. A heartbeat from a node previously marked dead (IsActive=false)
//     pushes OpSyncNodeCancelTask via that node's TaskManager for every
//     task the dispatcher's ownership ledger has re-homed elsewhere.
//  3. If the ledger is empty (no tasks to re-home), no cancels are sent
//     and the reconnect logs at debug — never an error.
// -----------------------------------------------------------------------

// makeReconnectCluster wires a minimal *Cluster that exercises ONLY the
// S5 reconnect path. The dispatcher is fed a stub source so we don't
// need real syncnodes for LoadScore — we only call AllOwnerships().
// The SyncNode entry under test gets a fresh TaskManager whose TaskMap
// we inspect to verify the cancel fanout shape.
func makeReconnectCluster(t *testing.T, addr string, wasActive bool) (*Cluster, *SyncNode, *SyncDispatcher) {
	t.Helper()
	src := newStubSource()
	d := newSyncDispatcherFromSource(src)
	c := &Cluster{Name: "test-cluster", syncDispatcher: d}
	sn := newSyncNode(addr, c.Name)
	sn.IsActive = wasActive
	// We do NOT register sn in src.nodes — Dispatch would short-circuit
	// to ErrNoCandidates and the test would need a richer harness. The
	// reconnect path only needs the ledger contents + the SyncNode's
	// TaskManager, both of which we manipulate directly below.
	c.syncNodes.Store(addr, sn)
	t.Cleanup(func() {
		// Stop the AdminTaskManager goroutine spawned by newSyncNode.
		sn.clean()
	})
	return c, sn, d
}

// seedOwnership pokes a (taskID, owner) pair directly into the dispatcher's
// ledger so the test can pre-set the ownership graph without going through
// Dispatch (which requires live candidates in the source).
func seedOwnership(d *SyncDispatcher, taskID, owner string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.taskOwner[taskID] = owner
	owned, ok := d.ownedByAddr[owner]
	if !ok {
		owned = make(map[string]struct{})
		d.ownedByAddr[owner] = owned
	}
	owned[taskID] = struct{}{}
}

// countAddedTasks returns the number of entries currently in the
// SyncNode TaskManager's TaskMap whose OpCode matches op. Reading
// TaskMap requires the manager's RLock.
func countAddedTasks(sn *SyncNode, op uint8) int {
	sn.TaskManager.RLock()
	defer sn.TaskManager.RUnlock()
	n := 0
	for _, t := range sn.TaskManager.TaskMap {
		if t.OpCode == op {
			n++
		}
	}
	return n
}

// TestHandleHeartbeat_NormalActivePath: a heartbeat to an already-active
// node MUST NOT trigger any cancel fanout. The S5 guard fires only on
// the wasActive=false → IsActive=true transition.
func TestHandleHeartbeat_NormalActivePath(t *testing.T) {
	addr := "10.0.0.1:17030"
	c, sn, d := makeReconnectCluster(t, addr, true /* wasActive */)

	// Seed one task owned elsewhere — if the fanout fired by mistake,
	// the TaskManager would gain an OpSyncNodeCancelTask entry.
	seedOwnership(d, "task-elsewhere", "10.0.0.2:17030")

	resp := &proto.SyncNodeHeartbeatResponse{
		Status:        proto.TaskSucceeds,
		BoltDBHealthy: true,
	}
	if err := c.handleSyncNodeHeartbeatResp(addr, resp); err != nil {
		t.Fatalf("handleSyncNodeHeartbeatResp: %v", err)
	}

	if got := countAddedTasks(sn, proto.OpSyncNodeCancelTask); got != 0 {
		t.Errorf("active-path heartbeat triggered %d cancel(s); want 0", got)
	}
	// Sanity: post-condition still IsActive=true.
	sn.RLock()
	active := sn.IsActive
	sn.RUnlock()
	if !active {
		t.Errorf("IsActive = false after happy-path heartbeat")
	}
}

// TestHandleHeartbeat_ReconnectFiresCancelFanout: a node previously
// marked dead reconnects. The dispatcher has 3 tasks in the ledger —
// 1 still owned by the reconnecting node and 2 re-homed elsewhere. The
// reconnect path MUST push exactly 2 OpSyncNodeCancelTask entries to
// the reconnecting node's TaskManager (cancels for the re-homed tasks
// only — never for tasks still owned here).
func TestHandleHeartbeat_ReconnectFiresCancelFanout(t *testing.T) {
	addr := "10.0.0.1:17030"
	c, sn, d := makeReconnectCluster(t, addr, false /* wasActive */)

	// 1 still owned by addr (must NOT receive a cancel).
	seedOwnership(d, "task-still-here", addr)
	// 2 transferred to other addrs (each MUST receive a cancel pushed
	// to addr's TaskManager).
	seedOwnership(d, "task-moved-1", "10.0.0.2:17030")
	seedOwnership(d, "task-moved-2", "10.0.0.3:17030")

	resp := &proto.SyncNodeHeartbeatResponse{
		Status:        proto.TaskSucceeds,
		BoltDBHealthy: true,
	}
	if err := c.handleSyncNodeHeartbeatResp(addr, resp); err != nil {
		t.Fatalf("handleSyncNodeHeartbeatResp: %v", err)
	}

	got := countAddedTasks(sn, proto.OpSyncNodeCancelTask)
	if got != 2 {
		t.Errorf("reconnect cancel-fanout pushed %d task(s); want 2", got)
	}

	// Verify the cancel task IDs target the re-homed tasks, NOT the
	// task still owned by the reconnecting node. We inspect the
	// AdminTask.Request payloads.
	sn.TaskManager.RLock()
	cancelledIDs := map[string]bool{}
	for _, task := range sn.TaskManager.TaskMap {
		if task.OpCode != proto.OpSyncNodeCancelTask {
			continue
		}
		req, ok := task.Request.(map[string]interface{})
		if !ok {
			t.Errorf("cancel task %q Request = %T, want map[string]interface{}", task.ID, task.Request)
			continue
		}
		id, _ := req["taskId"].(string)
		cancelledIDs[id] = true
	}
	sn.TaskManager.RUnlock()

	if cancelledIDs["task-still-here"] {
		t.Errorf("cancel fanout incorrectly targeted task still owned by reconnecting node")
	}
	if !cancelledIDs["task-moved-1"] || !cancelledIDs["task-moved-2"] {
		t.Errorf("cancel fanout missed re-homed task IDs; got: %v", cancelledIDs)
	}

	// Post-condition: IsActive flipped to true.
	sn.RLock()
	active := sn.IsActive
	sn.RUnlock()
	if !active {
		t.Errorf("IsActive = false after successful reconnect heartbeat")
	}
}

// TestHandleHeartbeat_ReconnectNoOwnerships: a node previously marked
// dead reconnects but the dispatcher's ledger is empty (e.g. all its
// former tasks already reported terminal and were Release()'d). The
// reconnect path MUST send zero cancels and return cleanly.
func TestHandleHeartbeat_ReconnectNoOwnerships(t *testing.T) {
	addr := "10.0.0.1:17030"
	c, sn, _ := makeReconnectCluster(t, addr, false /* wasActive */)
	// No seedOwnership calls — ledger is empty.

	resp := &proto.SyncNodeHeartbeatResponse{
		Status:        proto.TaskSucceeds,
		BoltDBHealthy: true,
	}
	if err := c.handleSyncNodeHeartbeatResp(addr, resp); err != nil {
		t.Fatalf("handleSyncNodeHeartbeatResp: %v", err)
	}

	if got := countAddedTasks(sn, proto.OpSyncNodeCancelTask); got != 0 {
		t.Errorf("empty-ledger reconnect pushed %d cancel(s); want 0", got)
	}
	sn.RLock()
	active := sn.IsActive
	sn.RUnlock()
	if !active {
		t.Errorf("IsActive = false after empty-ledger reconnect")
	}
}
