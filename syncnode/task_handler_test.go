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
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/tasks"
)

// TestMain initialises the proto buffer pool. proto.Packet.WriteToConn /
// ReadFromConnWithVer reach into a package-global pool (Buffers); the
// production path calls InitBufferPool from cmd.Init, but unit tests must do
// it themselves or hit a nil-deref the first time a packet is written.
func TestMain(m *testing.M) {
	proto.InitBufferPool(32 * 1024)
	os.Exit(m.Run())
}

// stubRunner is a hand-rolled stand-in for *tasks.Runner. Records every
// Trigger / Cancel call so tests can assert on call ordering and arguments.
type stubRunner struct {
	mu sync.Mutex

	triggerCalls       []triggerCall
	triggerWithIDCalls []triggerWithIDCall
	cancelCalls        []cancelCall

	// triggerErr is returned by Trigger / TriggerWithID when set; nil →
	// success.
	triggerErr error
	// cancelErr is returned by Cancel when set; nil → success.
	cancelErr error

	// triggerCount / cancelCount are atomic so concurrent tests can poll
	// without grabbing mu.
	triggerCount       atomic.Uint64
	triggerWithIDCount atomic.Uint64
	cancelCount        atomic.Uint64
}

type triggerCall struct {
	ruleID string
	wait   bool
}

type triggerWithIDCall struct {
	ruleID string
	taskID string
	wait   bool
}

type cancelCall struct {
	taskID string
}

func (s *stubRunner) Trigger(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error) {
	s.mu.Lock()
	s.triggerCalls = append(s.triggerCalls, triggerCall{ruleID: ruleID, wait: wait})
	err := s.triggerErr
	s.mu.Unlock()
	s.triggerCount.Add(1)
	if err != nil {
		return nil, err
	}
	return &tasks.Record{TaskID: "t-stub-" + ruleID, RuleID: ruleID}, nil
}

func (s *stubRunner) TriggerWithID(ctx context.Context, ruleID, taskID string, wait bool) (*tasks.Record, error) {
	s.mu.Lock()
	s.triggerWithIDCalls = append(s.triggerWithIDCalls, triggerWithIDCall{ruleID: ruleID, taskID: taskID, wait: wait})
	err := s.triggerErr
	s.mu.Unlock()
	s.triggerWithIDCount.Add(1)
	if err != nil {
		return nil, err
	}
	id := taskID
	if id == "" {
		id = "t-stub-" + ruleID
	}
	return &tasks.Record{TaskID: id, RuleID: ruleID}, nil
}

func (s *stubRunner) Cancel(ctx context.Context, taskID string) error {
	s.mu.Lock()
	s.cancelCalls = append(s.cancelCalls, cancelCall{taskID: taskID})
	err := s.cancelErr
	s.mu.Unlock()
	s.cancelCount.Add(1)
	return err
}

// stubResponder satisfies masterResponder for tests that need to verify the
// lifecycle push-back. P1-3 itself doesn't exercise this surface, but keeping
// it here avoids a future test churn.
type stubResponder struct {
	mu    sync.Mutex
	calls []*proto.AdminTask
	err   error
}

func (s *stubResponder) ResponseTask(task *proto.AdminTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, task)
	return s.err
}

// newRunPacket builds an OpSyncNodeRunTask packet whose body is a JSON-encoded
// AdminTask with the given RunTaskRequest. Helper for the dispatch tests.
func newRunPacket(t *testing.T, req RunTaskRequest, reqID int64) *proto.Packet {
	t.Helper()
	task := &proto.AdminTask{
		OpCode:  proto.OpSyncNodeRunTask,
		Request: req,
	}
	body, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal admin task: %v", err)
	}
	return &proto.Packet{
		Magic:  proto.ProtoMagic,
		Opcode: proto.OpSyncNodeRunTask,
		ReqID:  reqID,
		Size:   uint32(len(body)),
		Data:   body,
	}
}

// newCancelPacket mirrors newRunPacket for OpSyncNodeCancelTask.
func newCancelPacket(t *testing.T, req CancelTaskRequest, reqID int64) *proto.Packet {
	t.Helper()
	task := &proto.AdminTask{
		OpCode:  proto.OpSyncNodeCancelTask,
		Request: req,
	}
	body, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal admin task: %v", err)
	}
	return &proto.Packet{
		Magic:  proto.ProtoMagic,
		Opcode: proto.OpSyncNodeCancelTask,
		ReqID:  reqID,
		Size:   uint32(len(body)),
		Data:   body,
	}
}

func TestNewTaskHandler_PanicsOnNilRunner(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil runner")
		}
	}()
	_ = newTaskHandler(nil, nil)
}

func TestNewTaskHandler_TypedNilMasterClientIsNormalised(t *testing.T) {
	// Pass a typed-nil *SyncMasterClient; the handler must treat it as
	// "no responder" rather than tripping a nil-deref later.
	var mc *SyncMasterClient
	h := NewTaskHandler(&tasks.Runner{}, mc)
	if h.masterClient != nil {
		t.Fatalf("expected masterClient to be normalised to nil, got %T", h.masterClient)
	}
}

func TestHandlePacket_UnknownOpcode(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := &proto.Packet{Opcode: 0xAB, ReqID: 7}

	reply := h.HandlePacket(context.Background(), p)

	if reply == nil {
		t.Fatal("reply is nil")
	}
	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr (%x)", reply.ResultCode, proto.OpErr)
	}
	if reply.ReqID != p.ReqID {
		t.Fatalf("ReqID = %d, want %d", reply.ReqID, p.ReqID)
	}
	if !contains(string(reply.Data), "unknown opcode") {
		t.Fatalf("reply body = %q, want substring %q", reply.Data, "unknown opcode")
	}
	if runner.triggerCount.Load() != 0 || runner.cancelCount.Load() != 0 {
		t.Fatal("unknown opcode must not reach the runner")
	}
}

func TestHandleRunTask_HappyPath(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := newRunPacket(t, RunTaskRequest{RuleID: "rule-a"}, 100)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpOk {
		t.Fatalf("ResultCode = %x, want OpOk; body=%q", reply.ResultCode, reply.Data)
	}
	if reply.ReqID != 100 {
		t.Fatalf("ReqID = %d, want 100", reply.ReqID)
	}
	if got := runner.triggerCount.Load(); got != 1 {
		t.Fatalf("trigger count = %d, want 1", got)
	}
	if h.RunTaskTotal() != 1 {
		t.Fatalf("RunTaskTotal = %d, want 1", h.RunTaskTotal())
	}
	runner.mu.Lock()
	gotCall := runner.triggerCalls[0]
	runner.mu.Unlock()
	if gotCall.ruleID != "rule-a" || gotCall.wait {
		t.Fatalf("trigger call = %+v, want {ruleID=rule-a, wait=false}", gotCall)
	}
}

// TestHandleRunTask_WithTaskID confirms that a master-supplied TaskID is
// routed through TriggerWithID so the local Record key matches master's
// taskOwner ledger entry. This is the wire fix for the "Cancel(t-1)
// silently no-ops" bug.
func TestHandleRunTask_WithTaskID(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := newRunPacket(t, RunTaskRequest{RuleID: "rule-a", TaskID: "t-master-7"}, 101)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpOk {
		t.Fatalf("ResultCode = %x, want OpOk; body=%q", reply.ResultCode, reply.Data)
	}
	if runner.triggerWithIDCount.Load() != 1 {
		t.Fatalf("triggerWithID count = %d, want 1", runner.triggerWithIDCount.Load())
	}
	if runner.triggerCount.Load() != 0 {
		t.Fatalf("trigger count = %d, want 0 (TaskID present, should route through TriggerWithID)", runner.triggerCount.Load())
	}
	runner.mu.Lock()
	got := runner.triggerWithIDCalls[0]
	runner.mu.Unlock()
	if got.ruleID != "rule-a" || got.taskID != "t-master-7" || got.wait {
		t.Fatalf("triggerWithID call = %+v, want {ruleID=rule-a, taskID=t-master-7, wait=false}", got)
	}
}

func TestHandleRunTask_MissingRuleID(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := newRunPacket(t, RunTaskRequest{RuleID: ""}, 1)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr", reply.ResultCode)
	}
	if !contains(string(reply.Data), "missing ruleID") {
		t.Fatalf("reply body = %q, want substring %q", reply.Data, "missing ruleID")
	}
	if runner.triggerCount.Load() != 0 {
		t.Fatal("runner.Trigger must not be called when ruleID is empty")
	}
	if h.RunTaskTotal() != 1 {
		t.Fatalf("RunTaskTotal = %d, want 1 (counter advances on receipt)", h.RunTaskTotal())
	}
}

func TestHandleRunTask_RunnerError(t *testing.T) {
	runner := &stubRunner{triggerErr: errors.New("rule not found: rule-x")}
	h := newTaskHandler(runner, nil)
	p := newRunPacket(t, RunTaskRequest{RuleID: "rule-x"}, 5)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr", reply.ResultCode)
	}
	if !contains(string(reply.Data), "rule not found") {
		t.Fatalf("reply body = %q, want runner error to be surfaced", reply.Data)
	}
}

func TestHandleRunTask_BadJSON(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := &proto.Packet{
		Opcode: proto.OpSyncNodeRunTask,
		ReqID:  1,
		Data:   []byte("not json"),
		Size:   8,
	}

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr", reply.ResultCode)
	}
	if !contains(string(reply.Data), "decode") {
		t.Fatalf("reply body = %q, want substring %q", reply.Data, "decode")
	}
}

func TestHandleRunTask_EmptyBody(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := &proto.Packet{Opcode: proto.OpSyncNodeRunTask, ReqID: 1}

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr", reply.ResultCode)
	}
	if !contains(string(reply.Data), "empty") {
		t.Fatalf("reply body = %q, want substring %q", reply.Data, "empty")
	}
}

func TestHandleCancelTask_HappyPath(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := newCancelPacket(t, CancelTaskRequest{TaskID: "task-42"}, 11)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpOk {
		t.Fatalf("ResultCode = %x, want OpOk; body=%q", reply.ResultCode, reply.Data)
	}
	if h.CancelTaskTotal() != 1 {
		t.Fatalf("CancelTaskTotal = %d, want 1", h.CancelTaskTotal())
	}
	runner.mu.Lock()
	gotCall := runner.cancelCalls[0]
	runner.mu.Unlock()
	if gotCall.taskID != "task-42" {
		t.Fatalf("cancel call = %+v, want taskID=task-42", gotCall)
	}
}

func TestHandleCancelTask_MissingTaskID(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := newCancelPacket(t, CancelTaskRequest{TaskID: ""}, 1)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr", reply.ResultCode)
	}
	if !contains(string(reply.Data), "missing taskID") {
		t.Fatalf("reply body = %q, want substring %q", reply.Data, "missing taskID")
	}
	if runner.cancelCount.Load() != 0 {
		t.Fatal("runner.Cancel must not be called when taskID is empty")
	}
}

func TestHandleCancelTask_RunnerError(t *testing.T) {
	runner := &stubRunner{cancelErr: errors.New("task not found")}
	h := newTaskHandler(runner, nil)
	p := newCancelPacket(t, CancelTaskRequest{TaskID: "task-missing"}, 1)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr", reply.ResultCode)
	}
	if !contains(string(reply.Data), "task not found") {
		t.Fatalf("reply body = %q, want runner error surfaced", reply.Data)
	}
}

func TestHandleCancelTask_BadJSON(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := &proto.Packet{
		Opcode: proto.OpSyncNodeCancelTask,
		ReqID:  1,
		Data:   []byte("{bad"),
		Size:   4,
	}
	reply := h.HandlePacket(context.Background(), p)
	if reply.ResultCode != proto.OpErr {
		t.Fatalf("ResultCode = %x, want OpErr", reply.ResultCode)
	}
}

func TestHandlePacket_PreservesReqID(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)
	p := newRunPacket(t, RunTaskRequest{RuleID: "rule-z"}, 12345)

	reply := h.HandlePacket(context.Background(), p)

	if reply.ReqID != 12345 {
		t.Fatalf("ReqID = %d, want 12345", reply.ReqID)
	}
	if reply.Opcode != proto.OpSyncNodeRunTask {
		t.Fatalf("Opcode = %x, want %x", reply.Opcode, proto.OpSyncNodeRunTask)
	}
	if reply.Magic != proto.ProtoMagic {
		t.Fatalf("Magic = %x, want %x", reply.Magic, proto.ProtoMagic)
	}
}

// TestHandleConn_Sequence wires a localhost TCP socket pair to the handler
// and sends a RunTask + a CancelTask back-to-back, asserting that both
// replies come back in order with the right ResultCode and that the runner
// saw both calls.
//
// Uses a real TCP loopback (not net.Pipe) because proto.Packet.WriteToConn
// calls SetWriteDeadline which net.Pipe does not honour reliably.
func TestHandleConn_Sequence(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)

	server, client := newLoopbackPair(t)
	defer func() { _ = client.Close() }()

	done := make(chan struct{})
	go func() {
		h.HandleConn(context.Background(), server)
		close(done)
	}()

	// Send RunTask.
	runPkt := newRunPacket(t, RunTaskRequest{RuleID: "rule-seq"}, 1)
	if err := runPkt.WriteToConn(client); err != nil {
		t.Fatalf("write run packet: %v", err)
	}
	reply1 := readReplyWithTimeout(t, client, 2*time.Second)
	if reply1.ResultCode != proto.OpOk || reply1.ReqID != 1 {
		t.Fatalf("run reply: code=%x reqID=%d, want OpOk + 1", reply1.ResultCode, reply1.ReqID)
	}

	// Send CancelTask.
	cancelPkt := newCancelPacket(t, CancelTaskRequest{TaskID: "task-seq"}, 2)
	if err := cancelPkt.WriteToConn(client); err != nil {
		t.Fatalf("write cancel packet: %v", err)
	}
	reply2 := readReplyWithTimeout(t, client, 2*time.Second)
	if reply2.ResultCode != proto.OpOk || reply2.ReqID != 2 {
		t.Fatalf("cancel reply: code=%x reqID=%d, want OpOk + 2", reply2.ResultCode, reply2.ReqID)
	}

	// Closing the client end signals EOF to the handler; HandleConn should
	// then return cleanly.
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit after client close")
	}

	if runner.triggerCount.Load() != 1 {
		t.Fatalf("trigger count = %d, want 1", runner.triggerCount.Load())
	}
	if runner.cancelCount.Load() != 1 {
		t.Fatalf("cancel count = %d, want 1", runner.cancelCount.Load())
	}
}

// TestHandleConn_ContextCancel verifies that HandleConn returns once ctx is
// cancelled, even if no further packets arrive on the wire.
func TestHandleConn_ContextCancel(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)

	server, client := newLoopbackPair(t)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.HandleConn(ctx, server)
		close(done)
	}()

	// Send one packet so the loop is past its first iteration.
	runPkt := newRunPacket(t, RunTaskRequest{RuleID: "rule-cancel"}, 1)
	if err := runPkt.WriteToConn(client); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = readReplyWithTimeout(t, client, 2*time.Second)

	// Cancel the context, then close the client end so the blocked read
	// inside HandleConn returns. (ReadFromConnWithVer is a plain blocking
	// read; ctx is checked between packets.)
	cancel()
	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not exit on ctx cancel + conn close")
	}
}

// TestHandlePacket_Concurrent fires many RunTask packets in parallel goroutines
// and asserts both the counter and the stub's call total. Run with -race to
// flush out any forgotten synchronisation.
func TestHandlePacket_Concurrent(t *testing.T) {
	runner := &stubRunner{}
	h := newTaskHandler(runner, nil)

	const N = 32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			p := newRunPacket(t, RunTaskRequest{RuleID: "rule-c"}, int64(i))
			reply := h.HandlePacket(context.Background(), p)
			if reply.ResultCode != proto.OpOk {
				t.Errorf("reply %d code = %x, want OpOk", i, reply.ResultCode)
			}
		}(i)
	}
	wg.Wait()

	if h.RunTaskTotal() != N {
		t.Fatalf("RunTaskTotal = %d, want %d", h.RunTaskTotal(), N)
	}
	if runner.triggerCount.Load() != N {
		t.Fatalf("trigger count = %d, want %d", runner.triggerCount.Load(), N)
	}
}

// TestRunTaskRequest_SubTaskRoundTrip exercises the JSON shape of the
// RunTaskRequest including the optional SubTask field. Confirms that the
// P1-7 fan-out plumbing decodes cleanly even though P1-3 ignores it.
func TestRunTaskRequest_SubTaskRoundTrip(t *testing.T) {
	in := RunTaskRequest{
		RuleID:  "rule-shard",
		Type:    "sync",
		SubTask: &RunSubTaskInfo{ParentTaskID: "parent-1", ShardIndex: 3, ShardTotal: 8},
	}
	task := &proto.AdminTask{Request: in}
	body, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := decodeRunTaskRequest(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.RuleID != in.RuleID || got.Type != in.Type {
		t.Fatalf("scalar fields lost: got=%+v in=%+v", got, in)
	}
	if got.SubTask == nil {
		t.Fatal("SubTask was dropped during round-trip")
	}
	if got.SubTask.ParentTaskID != "parent-1" || got.SubTask.ShardIndex != 3 || got.SubTask.ShardTotal != 8 {
		t.Fatalf("SubTask = %+v", got.SubTask)
	}
}

// readReplyWithTimeout drains one packet from conn or fails the test. Used by
// the loopback-based tests above.
func readReplyWithTimeout(t *testing.T, conn net.Conn, d time.Duration) *proto.Packet {
	t.Helper()
	type res struct {
		p   *proto.Packet
		err error
	}
	ch := make(chan res, 1)
	go func() {
		p := &proto.Packet{}
		err := p.ReadFromConnWithVer(conn, proto.NoReadDeadlineTime)
		ch <- res{p: p, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("read reply: %v", r.err)
		}
		return r.p
	case <-time.After(d):
		t.Fatal("timed out waiting for reply")
		return nil
	}
}

// newLoopbackPair returns a connected pair of *net.TCPConn (server, client)
// on a free localhost port. Used instead of net.Pipe because
// proto.Packet.WriteToConn calls SetWriteDeadline which net.Pipe rejects.
func newLoopbackPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	accepted := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, aerr := l.Accept()
		if aerr != nil {
			errCh <- aerr
			return
		}
		accepted <- c
	}()

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case s := <-accepted:
		t.Cleanup(func() { _ = s.Close(); _ = c.Close() })
		return s, c
	case err := <-errCh:
		t.Fatalf("accept: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out establishing loopback pair")
	}
	return nil, nil
}

func contains(haystack, needle string) bool {
	return indexOf(haystack, needle) >= 0
}

// indexOf is a tiny strings.Contains substitute kept inline so the test file
// doesn't take a strings import just for one call (and stays consistent with
// the std-libs already in scope).
func indexOf(haystack, needle string) int {
	if len(needle) == 0 {
		return 0
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
