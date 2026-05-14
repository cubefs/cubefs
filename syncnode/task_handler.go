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

package syncnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/tasks"
	"github.com/cubefs/cubefs/util/log"
)

// runnerAPI is the slim contract TaskHandler needs from tasks.Runner. Defined
// at the consumer side so unit tests can swap in a stub without dragging the
// full executor / backend stack into the handler suite.
type runnerAPI interface {
	Trigger(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error)
	Cancel(ctx context.Context, taskID string) error
}

// subTaskRunner is the optional P1-7 fan-out hook. Production *tasks.Runner
// satisfies it; the test stub implements only runnerAPI. handleRunTask does
// a type assertion at dispatch time so the legacy path keeps working when
// the embedded runner doesn't support sub-tasks (the request is then
// rejected with a clear error).
type subTaskRunner interface {
	TriggerSubTask(ctx context.Context, ruleID, parentTaskID string,
		shardIndex, shardTotal int, wait bool) (*tasks.Record, error)
}

// masterResponder is the slim contract TaskHandler needs from SyncMasterClient
// to push task lifecycle envelopes back to master. Tests inject a stub.
type masterResponder interface {
	ResponseTask(task *proto.AdminTask) error
}

// RunTaskRequest is the master-pushed instruction inside the AdminTask.Request
// field of OpSyncNodeRunTask packets. The master dispatcher fills these when
// it picks this syncnode as the owner.
//
// SubTask is the P1-7 fan-out hook; P1-3 only requires RuleID to make
// end-to-end master-driven execution work.
type RunTaskRequest struct {
	TaskID   string                 `json:"taskId,omitempty"`
	RuleID   string                 `json:"ruleId"`
	Type     string                 `json:"type,omitempty"`
	SubTask  *RunSubTaskInfo        `json:"subTask,omitempty"`
	Override map[string]interface{} `json:"override,omitempty"`
}

// RunSubTaskInfo describes one shard of a fan-out task. P1-7 fills it; for
// P1-3 we just plumb it through without acting on it.
//
// TODO(P1-7): when Runner.TriggerSubTask lands, dispatch on SubTask != nil and
// forward ShardIndex / ShardTotal so workers can scope their work range.
type RunSubTaskInfo struct {
	ParentTaskID string `json:"parentTaskId"`
	ShardIndex   int    `json:"shardIndex"`
	ShardTotal   int    `json:"shardTotal"`
}

// CancelTaskRequest is the master-pushed cancel directive carried inside the
// AdminTask.Request field of OpSyncNodeCancelTask packets.
type CancelTaskRequest struct {
	TaskID string `json:"taskId"`
}

// TaskHandler accepts OpSyncNodeRunTask / OpSyncNodeCancelTask packets from
// master, dispatches them to the local Runner, and (eventually) reports the
// resulting lifecycle back to master via ResponseSyncNodeTask.
//
// Constructed in doStart after the runner + master client are built. One
// instance per SyncNode. Safe for concurrent use; HandlePacket is goroutine-
// safe because the underlying Runner is.
type TaskHandler struct {
	runner       runnerAPI
	masterClient masterResponder

	// Counters exposed via the existing /admin/syncnode/stat surface in a
	// future patch; reading them in tests is enough for now.
	runTaskTotal    atomic.Uint64
	cancelTaskTotal atomic.Uint64
}

// NewTaskHandler constructs a TaskHandler. runner must be non-nil; mc may be
// nil if the master client has not been built yet (HandleConn / HandlePacket
// tolerate a nil responder and skip the lifecycle push-back).
func NewTaskHandler(runner *tasks.Runner, mc *SyncMasterClient) *TaskHandler {
	return newTaskHandler(runner, mc)
}

// newTaskHandler is the test-friendly constructor accepting the narrow
// interfaces; production callers should use NewTaskHandler.
func newTaskHandler(runner runnerAPI, mc masterResponder) *TaskHandler {
	if runner == nil {
		// Programmer error — fail loud so a misconfigured server doesn't
		// silently accept packets and drop them.
		panic("syncnode: TaskHandler requires a non-nil runner")
	}
	// Normalise an explicitly-typed nil masterResponder into an untyped nil
	// so the `if h.masterClient == nil` checks downstream behave as expected.
	if mc == nil || isNilResponder(mc) {
		mc = nil
	}
	return &TaskHandler{runner: runner, masterClient: mc}
}

// isNilResponder guards against the "typed-nil interface" trap: callers may
// pass a *SyncMasterClient that is itself nil but stored in an interface
// value (non-nil interface, nil underlying). Treat that as no responder.
func isNilResponder(mc masterResponder) bool {
	switch v := mc.(type) {
	case *SyncMasterClient:
		return v == nil
	default:
		return false
	}
}

// RunTaskTotal returns the lifetime count of accepted OpSyncNodeRunTask
// packets (including ones that failed validation — the counter advances on
// receipt, not on success).
func (h *TaskHandler) RunTaskTotal() uint64 { return h.runTaskTotal.Load() }

// CancelTaskTotal returns the lifetime count of OpSyncNodeCancelTask packets.
func (h *TaskHandler) CancelTaskTotal() uint64 { return h.cancelTaskTotal.Load() }

// HandleConn drains one connection: reads packets in a loop, dispatches each,
// and writes the ack/nack back. Returns when the conn closes, an unrecoverable
// read error occurs, or ctx is cancelled.
//
// The connection is always closed before HandleConn returns.
func (h *TaskHandler) HandleConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetNoDelay(true)
	}
	remote := conn.RemoteAddr().String()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		p := &proto.Packet{}
		if err := p.ReadFromConnWithVer(conn, proto.NoReadDeadlineTime); err != nil {
			if !errors.Is(err, io.EOF) {
				log.LogWarnf("task_handler: read from %s: %v", remote, err)
			}
			return
		}
		reply := h.HandlePacket(ctx, p)
		if err := reply.WriteToConn(conn); err != nil {
			log.LogWarnf("task_handler: write reply to %s: %v", remote, err)
			return
		}
	}
}

// HandlePacket processes one packet and returns the ack/nack to send back.
// Never returns nil. Exposed for unit tests so the per-opcode logic can be
// driven without a real socket.
func (h *TaskHandler) HandlePacket(ctx context.Context, p *proto.Packet) *proto.Packet {
	switch p.Opcode {
	case proto.OpSyncNodeRunTask:
		return h.handleRunTask(ctx, p)
	case proto.OpSyncNodeCancelTask:
		return h.handleCancelTask(ctx, p)
	default:
		return errorReply(p, fmt.Errorf("unknown opcode 0x%X", p.Opcode))
	}
}

// handleRunTask is the OpSyncNodeRunTask path. It decodes the AdminTask
// envelope, pulls the RunTaskRequest out, and triggers the rule asynchronously
// via Runner.Trigger(wait=false). On any decode / validation / trigger failure
// we nack with the error message so the master surfaces it.
func (h *TaskHandler) handleRunTask(ctx context.Context, p *proto.Packet) *proto.Packet {
	h.runTaskTotal.Add(1)

	req, err := decodeRunTaskRequest(p.Data)
	if err != nil {
		return errorReply(p, fmt.Errorf("decode run task: %w", err))
	}
	if req.RuleID == "" {
		return errorReply(p, errors.New("missing ruleID in RunTaskRequest"))
	}

	// Use a fresh background context: the inbound packet's ctx represents
	// "stop reading more packets on this connection" — it must NOT cancel
	// the spawned task. Cancellation is explicit via OpSyncNodeCancelTask.
	//
	// P1-7: when SubTask is set the master has split the rule into a
	// shard fan-out; route through TriggerSubTask so the executor.Task
	// gets the ShardIndex / ShardTotal that scope the producer loop. We
	// access TriggerSubTask via the optional subTaskRunner interface so
	// the legacy stub-only runnerAPI keeps compiling.
	if req.SubTask != nil {
		sr, ok := h.runner.(subTaskRunner)
		if !ok {
			return errorReply(p, fmt.Errorf(
				"sub-task dispatch unsupported by runner: rule=%q parent=%q shard=%d/%d",
				req.RuleID, req.SubTask.ParentTaskID, req.SubTask.ShardIndex, req.SubTask.ShardTotal))
		}
		if _, err := sr.TriggerSubTask(context.Background(),
			req.RuleID, req.SubTask.ParentTaskID,
			req.SubTask.ShardIndex, req.SubTask.ShardTotal, false); err != nil {
			return errorReply(p, fmt.Errorf("trigger sub-task %q shard %d/%d: %w",
				req.RuleID, req.SubTask.ShardIndex, req.SubTask.ShardTotal, err))
		}
		return okReply(p)
	}

	if _, err := h.runner.Trigger(context.Background(), req.RuleID, false); err != nil {
		return errorReply(p, fmt.Errorf("trigger rule %q: %w", req.RuleID, err))
	}
	return okReply(p)
}

// handleCancelTask is the OpSyncNodeCancelTask path. Runner.Cancel is a no-op
// for unknown task IDs (it returns ErrTaskNotFound which we surface verbatim
// to master so cancel-for-already-finished-task can be distinguished from a
// pure dispatch error).
func (h *TaskHandler) handleCancelTask(ctx context.Context, p *proto.Packet) *proto.Packet {
	h.cancelTaskTotal.Add(1)

	req, err := decodeCancelRequest(p.Data)
	if err != nil {
		return errorReply(p, fmt.Errorf("decode cancel task: %w", err))
	}
	if req.TaskID == "" {
		return errorReply(p, errors.New("missing taskID in CancelTaskRequest"))
	}
	if err := h.runner.Cancel(ctx, req.TaskID); err != nil {
		return errorReply(p, fmt.Errorf("cancel %q: %w", req.TaskID, err))
	}
	return okReply(p)
}

// decodeRunTaskRequest unwraps the AdminTask envelope and pulls a typed
// RunTaskRequest out of its Request field. The wire format goes through two
// hops of JSON encoding (Packet.Data → AdminTask → Request), and the inner
// Request lands as a map[string]interface{} after the first Unmarshal. We
// re-marshal that map and decode into the typed struct rather than asserting
// on field names — keeps decode invariants in one place.
func decodeRunTaskRequest(data []byte) (*RunTaskRequest, error) {
	task, err := decodeAdminTask(data)
	if err != nil {
		return nil, err
	}
	var req RunTaskRequest
	if err := remarshalInto(task.Request, &req); err != nil {
		return nil, fmt.Errorf("decode request payload: %w", err)
	}
	return &req, nil
}

// decodeCancelRequest mirrors decodeRunTaskRequest for OpSyncNodeCancelTask.
func decodeCancelRequest(data []byte) (*CancelTaskRequest, error) {
	task, err := decodeAdminTask(data)
	if err != nil {
		return nil, err
	}
	var req CancelTaskRequest
	if err := remarshalInto(task.Request, &req); err != nil {
		return nil, fmt.Errorf("decode request payload: %w", err)
	}
	return &req, nil
}

// decodeAdminTask is the shared first hop: Packet.Data → AdminTask. Rejects
// empty input early so callers can distinguish "no body" from "bad body".
func decodeAdminTask(data []byte) (*proto.AdminTask, error) {
	if len(data) == 0 {
		return nil, errors.New("empty packet body")
	}
	var task proto.AdminTask
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("admin task: %w", err)
	}
	return &task, nil
}

// remarshalInto round-trips an interface{} value through JSON into a typed
// destination. Tolerates a nil source (treats it as an empty object). Used to
// peel the AdminTask.Request field out of its map[string]interface{} shape.
func remarshalInto(src interface{}, dst interface{}) error {
	if src == nil {
		// Empty request body — leave dst at its zero value.
		return nil
	}
	raw, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

// okReply builds a fresh ack packet matching the request's opcode + ReqID.
// We construct a new packet rather than mutating the inbound one because the
// caller may want to inspect the original after we reply (currently no such
// caller exists, but the cost is one allocation and the clarity is worth it).
func okReply(req *proto.Packet) *proto.Packet {
	reply := &proto.Packet{
		Magic:      proto.ProtoMagic,
		Opcode:     req.Opcode,
		ReqID:      req.ReqID,
		ResultCode: proto.OpOk,
	}
	return reply
}

// errorReply builds a fresh nack packet carrying err.Error() as the body so
// the master surfaces the failure reason to the operator.
func errorReply(req *proto.Packet, err error) *proto.Packet {
	msg := []byte(err.Error())
	return &proto.Packet{
		Magic:      proto.ProtoMagic,
		Opcode:     req.Opcode,
		ReqID:      req.ReqID,
		ResultCode: proto.OpErr,
		Size:       uint32(len(msg)),
		Data:       msg,
	}
}
