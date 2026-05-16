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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/tasks"
	"github.com/cubefs/cubefs/util/log"
)

// runnerAPI is the slim contract TaskHandler needs from tasks.Runner. Defined
// at the consumer side so unit tests can swap in a stub without dragging the
// full executor / backend stack into the handler suite.
type runnerAPI interface {
	Trigger(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error)
	TriggerWithID(ctx context.Context, ruleID, taskID string, wait bool) (*tasks.Record, error)
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

// ruleAwareRunner is the P2-6 fan-out hook that consumes the master-
// shipped rule snapshot directly, bypassing the local rule store. The
// production *tasks.Runner satisfies it via TriggerWithRule; tests can
// stub via runnerAPI alone if they don't exercise this path.
type ruleAwareRunner interface {
	TriggerWithRule(ctx context.Context, rule *rules.Rule, taskID string,
		shardIndex, shardTotal int, prefixes []string, wait bool) (*tasks.Record, error)
}

// masterResponder is the slim contract TaskHandler needs from SyncMasterClient
// to push task lifecycle envelopes back to master. Tests inject a stub.
type masterResponder interface {
	ResponseTask(task *proto.AdminTask) error
	// LocalServerAddr returns the canonical "ip:port" this syncnode is reachable
	// at from master's perspective. Used as OperatorAddr when building terminal
	// push-back AdminTask envelopes so master can look up the sender.
	LocalServerAddr() string
}

// RunTaskRequest is the master-pushed instruction inside the AdminTask.Request
// field of OpSyncNodeRunTask packets. The master dispatcher fills these when
// it picks this syncnode as the owner.
//
// As of P2 the master ships the full rule snapshot in the Rule field so
// syncnode no longer needs a local rule store; Phase 6 cuts the local
// lookup path. SubTask is the P1-7 fan-out hook; SubTask.Prefixes (P2-5)
// is the prefix-mode shard descriptor.
type RunTaskRequest struct {
	TaskID   string                 `json:"taskId,omitempty"`
	RuleID   string                 `json:"ruleId"`
	Type     string                 `json:"type,omitempty"`
	Rule     *proto.SyncRule        `json:"rule,omitempty"`
	SubTask  *RunSubTaskInfo        `json:"subTask,omitempty"`
	Override map[string]interface{} `json:"override,omitempty"`
}

// RunSubTaskInfo describes one shard of a fan-out task. ShardIndex /
// ShardTotal cover hash-mode dispatch (P1-7); Prefixes (P2-5) carries
// the literal prefix list a prefix-mode shard owns. Both modes route
// through the same SubTask field — readers should treat a non-empty
// Prefixes as a signal to ignore ShardIndex/Total for filtering.
type RunSubTaskInfo struct {
	ParentTaskID string   `json:"parentTaskId"`
	ShardIndex   int      `json:"shardIndex"`
	ShardTotal   int      `json:"shardTotal"`
	Prefixes     []string `json:"prefixes,omitempty"`
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

	// bb is the backend builder used by handleListPrefixes (P2-5
	// auto-prefix probe). nil = list-prefixes packets return an error
	// (test-friendly default; production wires this via
	// WithBackendBuilder from server.go).
	bb *backendBuilder

	// SEC2: per-read deadline applied inside HandleConn. Stored as
	// time.Duration so callers can wire it from SyncConfig.TCP. Zero
	// or negative falls back to DefaultTCPReadIdleTimeout seconds at
	// read time (see resolvedReadIdleTimeout). Replaces the previous
	// proto.NoReadDeadlineTime read which let one idle conn pin one
	// goroutine + one FD indefinitely.
	readIdleTimeout time.Duration

	// Counters exposed via the existing /admin/syncnode/stat surface in a
	// future patch; reading them in tests is enough for now.
	runTaskTotal      atomic.Uint64
	cancelTaskTotal   atomic.Uint64
	listPrefixesTotal atomic.Uint64
}

// TaskHandlerOption configures optional TaskHandler behaviour. See
// WithReadIdleTimeout.
type TaskHandlerOption func(*TaskHandler)

// WithReadIdleTimeout sets the per-packet read deadline applied inside
// HandleConn. d <= 0 falls back to DefaultTCPReadIdleTimeout seconds.
func WithReadIdleTimeout(d time.Duration) TaskHandlerOption {
	return func(h *TaskHandler) { h.readIdleTimeout = d }
}

// withBackendBuilder wires the backend builder used by the auto-prefix
// probe handler. Unexported because production callers go through
// server.go construction; tests can drive HandlePacket without it (the
// list-prefixes path returns an error when bb is nil).
func withBackendBuilder(bb *backendBuilder) TaskHandlerOption {
	return func(h *TaskHandler) { h.bb = bb }
}

// NewTaskHandler constructs a TaskHandler. runner must be non-nil; mc may be
// nil if the master client has not been built yet (HandleConn / HandlePacket
// tolerate a nil responder and skip the lifecycle push-back).
func NewTaskHandler(runner *tasks.Runner, mc *SyncMasterClient, opts ...TaskHandlerOption) *TaskHandler {
	h := newTaskHandler(runner, mc)
	for _, opt := range opts {
		opt(h)
	}
	return h
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

// pushFailedTerminal sends a synthetic "failed" terminal report to master for
// taskID. Called when handleRunTask cannot even start the executor (e.g. a
// backend build failure) so master never receives the normal onTerminal
// push-back — without this the ledger entry stays "running" forever.
//
// Best-effort: if masterClient is nil or the HTTP call fails we only log.
func (h *TaskHandler) pushFailedTerminal(taskID, errMsg string) {
	if h.masterClient == nil || taskID == "" {
		return
	}
	report := &proto.TaskTerminalReport{
		TaskID: taskID,
		Status: "failed",
		Error:  errMsg,
	}
	t := proto.NewAdminTaskEx(proto.OpSyncNodeRunTask, h.masterClient.LocalServerAddr(), nil, taskID)
	t.Response = report
	if err := h.masterClient.ResponseTask(t); err != nil {
		log.LogWarnf("syncnode: pushFailedTerminal %q: %v", taskID, err)
	}
}

// RunTaskTotal returns the lifetime count of accepted OpSyncNodeRunTask
// packets (including ones that failed validation — the counter advances on
// receipt, not on success).
func (h *TaskHandler) RunTaskTotal() uint64 { return h.runTaskTotal.Load() }

// CancelTaskTotal returns the lifetime count of OpSyncNodeCancelTask packets.
func (h *TaskHandler) CancelTaskTotal() uint64 { return h.cancelTaskTotal.Load() }

// resolvedReadIdleTimeoutSec returns the configured per-read deadline in
// SECONDS for proto.ReadFromConnWithVer (which takes seconds, not a
// time.Duration). A zero or negative h.readIdleTimeout falls back to
// DefaultTCPReadIdleTimeout. Anything sub-second is clamped to 1s — the
// proto API has 1s granularity and we don't want a fractional config
// value to silently turn into "no deadline".
func (h *TaskHandler) resolvedReadIdleTimeoutSec() int {
	if h.readIdleTimeout <= 0 {
		return DefaultTCPReadIdleTimeout
	}
	sec := int(h.readIdleTimeout / time.Second)
	if sec < 1 {
		return 1
	}
	return sec
}

// HandleConn drains one connection: reads packets in a loop, dispatches each,
// and writes the ack/nack back. Returns when the conn closes, an unrecoverable
// read error occurs, or ctx is cancelled.
//
// SEC2: each packet read is bounded by readIdleTimeout (default 60s). An
// idle conn that doesn't send a packet within the window is torn down so
// the per-conn goroutine + FD don't leak under a flood scenario. Previous
// code used proto.NoReadDeadlineTime which let any client pin resources
// indefinitely.
//
// The connection is always closed before HandleConn returns.
func (h *TaskHandler) HandleConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetNoDelay(true)
	}
	remote := conn.RemoteAddr().String()
	timeoutSec := h.resolvedReadIdleTimeoutSec()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		p := &proto.Packet{}
		if err := p.ReadFromConnWithVer(conn, timeoutSec); err != nil {
			if isTimeoutErr(err) {
				log.LogDebugf("task_handler: idle read timeout from %s after %ds — closing", remote, timeoutSec)
				return
			}
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

// isTimeoutErr is the net.Error-style timeout check used by HandleConn to
// distinguish "read deadline expired" from a real read failure. The proto
// read path wraps the underlying net.OpError; net.Error.Timeout walks
// through the wrap chain so a direct type assertion on the underlying
// error works.
func isTimeoutErr(err error) bool {
	var nerr net.Error
	if errors.As(err, &nerr) {
		return nerr.Timeout()
	}
	return false
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
	case proto.OpSyncNodeListPrefixes:
		return h.handleListPrefixes(ctx, p)
	default:
		return errorReply(p, fmt.Errorf("unknown opcode 0x%X", p.Opcode))
	}
}

// handleRunTask is the OpSyncNodeRunTask path. It decodes the AdminTask
// envelope, pulls the RunTaskRequest out, and triggers the rule asynchronously
// via Runner.Trigger(wait=false). On any decode / validation / trigger failure
// we nack with the error message so the master surfaces it.
//
// P2-6 fast path: when req.Rule != nil the master shipped the full rule
// snapshot — go straight to TriggerWithRule, skipping the local rule
// store lookup entirely. This is the only path master uses post-cutover;
// the legacy RuleID-only path is kept for backward compat with older
// callers but is no longer exercised by the master scheduler.
func (h *TaskHandler) handleRunTask(ctx context.Context, p *proto.Packet) *proto.Packet {
	h.runTaskTotal.Add(1)

	req, err := decodeRunTaskRequest(p.Data)
	if err != nil {
		return errorReply(p, fmt.Errorf("decode run task: %w", err))
	}
	if req.RuleID == "" && req.Rule == nil {
		return errorReply(p, errors.New("missing ruleID and rule snapshot in RunTaskRequest"))
	}

	// P2-6 fast path: master shipped the rule snapshot. Use it directly.
	if req.Rule != nil {
		rar, ok := h.runner.(ruleAwareRunner)
		if !ok {
			return errorReply(p, fmt.Errorf(
				"rule-aware dispatch unsupported by runner: rule=%q", req.Rule.ID()))
		}
		var shardIdx, shardTotal int
		var prefixes []string
		if req.SubTask != nil {
			shardIdx = req.SubTask.ShardIndex
			shardTotal = req.SubTask.ShardTotal
			prefixes = req.SubTask.Prefixes
		}
		if _, err := rar.TriggerWithRule(context.Background(), req.Rule, req.TaskID,
			shardIdx, shardTotal, prefixes, false); err != nil {
			log.LogWarnf("syncnode: TriggerWithRule rule=%q task=%q: %v", req.Rule.ID(), req.TaskID, err)
			h.pushFailedTerminal(req.TaskID, err.Error())
			return errorReply(p, fmt.Errorf("trigger rule %q (snapshot path): %w", req.Rule.ID(), err))
		}
		return okReply(p)
	}

	// Legacy path: master sent only RuleID; syncnode looks up by ID
	// in its local store. Retained for pre-P2 callers + tests that
	// don't yet build a rule snapshot.
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
			shardTaskID := fmt.Sprintf("%s/%d", req.SubTask.ParentTaskID, req.SubTask.ShardIndex)
			log.LogWarnf("syncnode: TriggerSubTask rule=%q shard=%d/%d task=%q: %v",
				req.RuleID, req.SubTask.ShardIndex, req.SubTask.ShardTotal, shardTaskID, err)
			h.pushFailedTerminal(shardTaskID, err.Error())
			return errorReply(p, fmt.Errorf("trigger sub-task %q shard %d/%d: %w",
				req.RuleID, req.SubTask.ShardIndex, req.SubTask.ShardTotal, err))
		}
		return okReply(p)
	}

	// Single-shard path. When the master provided a TaskID, honour it via
	// TriggerWithID so the local Record key is identical to the master's
	// taskOwner ledger entry — otherwise cancel / forget / failover all
	// silently no-op because the local IDs would diverge. An empty TaskID
	// (older pre-fix masters) falls back to the local idFactory.
	if req.TaskID != "" {
		if _, err := h.runner.TriggerWithID(context.Background(), req.RuleID, req.TaskID, false); err != nil {
			log.LogWarnf("syncnode: TriggerWithID rule=%q task=%q: %v", req.RuleID, req.TaskID, err)
			h.pushFailedTerminal(req.TaskID, err.Error())
			return errorReply(p, fmt.Errorf("trigger rule %q taskID %q: %w", req.RuleID, req.TaskID, err))
		}
		return okReply(p)
	}
	if _, err := h.runner.Trigger(context.Background(), req.RuleID, false); err != nil {
		log.LogWarnf("syncnode: Trigger rule=%q: %v", req.RuleID, err)
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

// handleListPrefixes is the OpSyncNodeListPrefixes path (Phase P2-5
// auto-prefix probe). The master asks this syncnode to enumerate the
// top-level prefixes under a given (endpoint, prefix, delimiter) tuple;
// the result drives master's prefix-bucket fan-out.
//
// The handler is intentionally lightweight: it builds (or reuses from
// the pool) a backend, calls List(prefix, recursive=false), collects
// the CommonPrefixes (Entry.IsDir==true), and replies. Does NOT count
// toward concurrentTasks — listing one directory is a sub-second
// operation and shouldn't block real task dispatch.
func (h *TaskHandler) handleListPrefixes(ctx context.Context, p *proto.Packet) *proto.Packet {
	h.listPrefixesTotal.Add(1)
	if h.bb == nil {
		return errorReply(p, errors.New("backend builder not wired"))
	}
	req, err := decodeListPrefixesRequest(p.Data)
	if err != nil {
		return errorReply(p, fmt.Errorf("decode list-prefixes request: %w", err))
	}
	ep := req.Endpoint
	be, err := h.bb.Build(ctx, &ep)
	if err != nil {
		return errorReply(p, fmt.Errorf("build backend: %w", err))
	}
	maxPrefixes := req.MaxPrefixes
	if maxPrefixes <= 0 {
		maxPrefixes = proto.SyncListPrefixesMaxDefault
	}
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ch, err := be.List(listCtx, req.Prefix, false)
	if err != nil {
		return errorReply(p, fmt.Errorf("backend list: %w", err))
	}
	prefixes := make([]string, 0, 16)
	for entry := range ch {
		if entry.Err != nil {
			return errorReply(p, fmt.Errorf("list err: %w", entry.Err))
		}
		if !entry.IsDir {
			continue
		}
		prefixes = append(prefixes, entry.Key)
		if len(prefixes) >= maxPrefixes {
			break
		}
	}
	reply := &proto.SyncListPrefixesReply{Prefixes: prefixes}
	body, err := json.Marshal(reply)
	if err != nil {
		return errorReply(p, fmt.Errorf("marshal reply: %w", err))
	}
	log.LogInfof("handleListPrefixes: kind=%s prefix=%q found=%d", ep.Kind, req.Prefix, len(prefixes))
	return &proto.Packet{
		Magic:      proto.ProtoMagic,
		Opcode:     p.Opcode,
		ReqID:      p.ReqID,
		ResultCode: proto.OpOk,
		Size:       uint32(len(body)),
		Data:       body,
	}
}

// decodeListPrefixesRequest unwraps the AdminTask envelope and pulls a
// typed SyncListPrefixesRequest out. The wire format goes through two
// JSON layers: the outer AdminTask carries Request as interface{}, and
// the inner payload is the request struct. We round-trip rather than
// type-assert because the inner shape arrives as map[string]interface{}
// after the outer Unmarshal.
func decodeListPrefixesRequest(data []byte) (*proto.SyncListPrefixesRequest, error) {
	var envelope struct {
		Request json.RawMessage `json:"Request"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if len(envelope.Request) == 0 {
		return nil, errors.New("missing Request body")
	}
	var req proto.SyncListPrefixesRequest
	if err := json.Unmarshal(envelope.Request, &req); err != nil {
		return nil, fmt.Errorf("decode request body: %w", err)
	}
	return &req, nil
}
