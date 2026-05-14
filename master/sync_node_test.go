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
	"time"

	"github.com/cubefs/cubefs/proto"
)

// These tests cover the SyncNode primitives in isolation. They do NOT
// spin up a real master/raft cluster — integration coverage (HTTP route,
// leader-switch, in-flight heartbeat scheduling) lives in the mocktest
// harness used by the rest of the package.

func TestSyncNode_NewActiveByDefault(t *testing.T) {
	sn := newSyncNode("10.0.0.1:17030", "test-cluster")
	if sn.Addr != "10.0.0.1:17030" {
		t.Fatalf("Addr = %q, want %q", sn.Addr, "10.0.0.1:17030")
	}
	if !sn.IsActive {
		t.Fatalf("freshly created SyncNode should be active")
	}
	if sn.ReportTime.IsZero() {
		t.Fatalf("ReportTime should be initialized")
	}
	if sn.TaskManager == nil {
		t.Fatalf("TaskManager should be initialized")
	}
}

func TestSyncNode_CheckLivenessKeepsActiveBeforeTimeout(t *testing.T) {
	sn := newSyncNode("10.0.0.1:17030", "test-cluster")
	sn.ReportTime = time.Now()
	sn.checkLiveness()
	if !sn.IsActive {
		t.Fatalf("SyncNode should stay active when within timeout")
	}
}

func TestSyncNode_CheckLivenessFlipsInactiveAfterTimeout(t *testing.T) {
	sn := newSyncNode("10.0.0.1:17030", "test-cluster")
	// Push ReportTime past the timeout window. defaultNodeTimeOutSec is
	// noHeartBeatTimes * defaultIntervalToCheckHeartbeat (= a small
	// multiple of seconds); subtract twice that to be safe.
	sn.ReportTime = time.Now().Add(-time.Duration(defaultNodeTimeOutSec*2) * time.Second)
	sn.checkLiveness()
	if sn.IsActive {
		t.Fatalf("SyncNode should flip inactive after timeout")
	}
}

func TestSyncNode_CreateHeartbeatTaskCarriesLeaderAddr(t *testing.T) {
	sn := newSyncNode("10.0.0.1:17030", "test-cluster")
	masterAddr := "10.0.0.99:17010"
	task := sn.createHeartbeatTask(masterAddr)

	if task == nil {
		t.Fatalf("createHeartbeatTask returned nil")
	}
	if task.OpCode != proto.OpSyncNodeHeartbeat {
		t.Fatalf("OpCode = 0x%x, want 0x%x", task.OpCode, proto.OpSyncNodeHeartbeat)
	}
	if task.OperatorAddr != sn.Addr {
		t.Fatalf("OperatorAddr = %q, want %q", task.OperatorAddr, sn.Addr)
	}

	req, ok := task.Request.(*proto.SyncNodeHeartbeatRequest)
	if !ok {
		t.Fatalf("Request not *SyncNodeHeartbeatRequest, got %T", task.Request)
	}
	if req.LeaderAddr != masterAddr {
		t.Fatalf("LeaderAddr = %q, want %q", req.LeaderAddr, masterAddr)
	}
	if req.Addr != sn.Addr {
		t.Fatalf("Addr = %q, want %q", req.Addr, sn.Addr)
	}
}

func TestSyncNode_HeartbeatTaskRoundTripsThroughJSON(t *testing.T) {
	sn := newSyncNode("10.0.0.1:17030", "test-cluster")
	task := sn.createHeartbeatTask("10.0.0.99:17010")

	// Encode the request body the way admin-task transport would.
	body, err := json.Marshal(task.Request)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	got := &proto.SyncNodeHeartbeatRequest{}
	if err = json.Unmarshal(body, got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.LeaderAddr != "10.0.0.99:17010" || got.Addr != sn.Addr {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestSyncNode_HeartbeatTaskIsHeartbeatOpcode(t *testing.T) {
	sn := newSyncNode("10.0.0.1:17030", "test-cluster")
	task := sn.createHeartbeatTask("10.0.0.99:17010")
	if !task.IsHeartbeatTask() {
		t.Fatalf("OpSyncNodeHeartbeat should be classified as a heartbeat task")
	}
}

func TestSyncNode_DecodeTaskResponse(t *testing.T) {
	// Simulate the wire shape: master receives task.Response as a generic
	// map[string]interface{} after JSON decode of the AdminTask envelope.
	// decodeTaskResponse should lift it to the concrete typed pointer.
	rawResp := map[string]interface{}{
		"status":         uint8(proto.TaskSucceeds),
		"nodeId":         uint64(42),
		"addr":           "10.0.0.1:17030",
		"runningTasks":   int64(7),
		"queuedTasks":    int64(3),
		"scheduledRules": 12,
		"boltDBHealthy":  true,
		"version":        "1.2.3",
	}
	task := &proto.AdminTask{
		OpCode:   proto.OpSyncNodeHeartbeat,
		Response: rawResp,
	}
	out := &proto.SyncNodeHeartbeatResponse{}
	if err := decodeTaskResponse(task, out); err != nil {
		t.Fatalf("decodeTaskResponse failed: %v", err)
	}
	if out.NodeID != 42 || out.Addr != "10.0.0.1:17030" {
		t.Fatalf("identity mismatch: %+v", out)
	}
	if out.RunningTasks != 7 || out.QueuedTasks != 3 || out.ScheduledRules != 12 {
		t.Fatalf("counts mismatch: %+v", out)
	}
	if !out.BoltDBHealthy {
		t.Fatalf("BoltDBHealthy should be true")
	}
	if out.NodeVersion != "1.2.3" {
		t.Fatalf("NodeVersion = %q, want %q", out.NodeVersion, "1.2.3")
	}
	// task.Response should now be the typed pointer.
	if _, ok := task.Response.(*proto.SyncNodeHeartbeatResponse); !ok {
		t.Fatalf("task.Response not lifted to *SyncNodeHeartbeatResponse, got %T", task.Response)
	}
}

func TestSyncNode_NewValueOnlyPersistsIdentity(t *testing.T) {
	sn := newSyncNode("10.0.0.1:17030", "test-cluster")
	sn.ID = 99
	sn.RunningTasks = 100 // runtime field must NOT leak into raft record
	v := newSyncNodeValue(sn)
	if v.ID != 99 || v.Addr != "10.0.0.1:17030" {
		t.Fatalf("syncNodeValue identity mismatch: %+v", v)
	}
	// snv is the raft-replicated identity record; ensure it has no extra
	// runtime fields baked in by checking JSON shape.
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]interface{}
	if err = json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := decoded["RunningTasks"]; present {
		t.Fatalf("syncNodeValue must not persist runtime field RunningTasks: %v", decoded)
	}
	if len(decoded) != 2 {
		t.Fatalf("syncNodeValue should persist exactly ID + Addr, got: %v", decoded)
	}
}

func TestSyncNode_NotFoundErrorMessage(t *testing.T) {
	err := syncNodeNotFound("10.0.0.1:17030")
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if got := err.Error(); got == "" {
		t.Fatalf("expected non-empty error message")
	}
}
