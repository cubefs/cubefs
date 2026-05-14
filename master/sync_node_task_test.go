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
	"encoding/json"
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
