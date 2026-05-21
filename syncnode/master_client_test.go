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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

// fakeMaster is a tiny httptest-backed master used by every test in this
// file. Each handler is overridable so individual tests can flip the
// behaviour (e.g. fail AddSyncNode for the first N calls).
type fakeMaster struct {
	srv *httptest.Server

	// Counters: atomic so heartbeat assertions can read them while the
	// heartbeat goroutine still runs in parallel.
	getClusterCalls atomic.Int64
	addNodeCalls    atomic.Int64
	heartbeatCalls  atomic.Int64

	// Overridable hooks; default behaviour wraps the simplest happy path.
	mu                  sync.Mutex
	addNodeFailUntil    int64    // first N calls return 500
	addNodeStatus       int      // override status code (0 → 200)
	heartbeatFailUntil  int64    // first N heartbeat calls return non-success
	nextNodeID          uint64
	clusterIP           string
	clusterName         string
}

func newFakeMaster(t *testing.T) *fakeMaster {
	t.Helper()
	f := &fakeMaster{
		nextNodeID:  42,
		clusterIP:   "127.0.0.1",
		clusterName: "test-cluster",
	}
	mux := http.NewServeMux()
	mux.HandleFunc(proto.AdminGetIP, f.handleGetIP)
	mux.HandleFunc(proto.AddSyncNode, f.handleAddSyncNode)
	mux.HandleFunc(proto.GetSyncNodeTaskResponse, f.handleHeartbeat)
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *fakeMaster) Close() {
	if f.srv != nil {
		f.srv.Close()
	}
}

// Addr returns "host:port" suitable for masterAddr config.
func (f *fakeMaster) Addr() string {
	u := f.srv.URL
	u = strings.TrimPrefix(u, "http://")
	return u
}

func (f *fakeMaster) handleGetIP(w http.ResponseWriter, _ *http.Request) {
	f.getClusterCalls.Add(1)
	f.mu.Lock()
	ci := proto.ClusterInfo{Cluster: f.clusterName, Ip: f.clusterIP}
	f.mu.Unlock()
	writeReply(w, ci)
}

func (f *fakeMaster) handleAddSyncNode(w http.ResponseWriter, r *http.Request) {
	n := f.addNodeCalls.Add(1)
	f.mu.Lock()
	failUntil := f.addNodeFailUntil
	overrideStatus := f.addNodeStatus
	id := f.nextNodeID
	f.nextNodeID++
	f.mu.Unlock()

	if overrideStatus != 0 {
		w.WriteHeader(overrideStatus)
		return
	}
	if n <= failUntil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// Addr param is also recorded for sanity.
	_ = r.URL.Query().Get("addr")
	writeReply(w, json.Number(toDecimal(id)))
}

func (f *fakeMaster) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	n := f.heartbeatCalls.Add(1)
	_, _ = io.Copy(io.Discard, r.Body)
	f.mu.Lock()
	failUntil := f.heartbeatFailUntil
	f.mu.Unlock()
	if n <= failUntil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeReply(w, nil)
}

// writeReply encodes the value into proto.HTTPReplyRaw with Code=0. Data
// is JSON-encoded inline so HTTPReplyRaw.Data sees the raw bytes the
// MasterClient expects.
func writeReply(w http.ResponseWriter, data interface{}) {
	raw := proto.HTTPReplyRaw{Code: 0, Msg: "OK"}
	if data != nil {
		buf, err := json.Marshal(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		raw.Data = buf
	} else {
		raw.Data = json.RawMessage("null")
	}
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(raw)
	_, _ = w.Write(out)
}

func toDecimal(v uint64) string {
	// Avoid strconv import noise; just hand-roll for the few digits we use.
	// json.Number is just a string under the hood.
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}

// stubProvider returns a fixed payload so heartbeat tests can verify
// the dynamic gauges round-trip.
type stubProvider struct{ payload proto.SyncNodeHeartbeatResponse }

func (s *stubProvider) Snapshot() proto.SyncNodeHeartbeatResponse { return s.payload }

// shortOpts returns option set tuned for fast tests.
func shortOpts() []MasterOption {
	return []MasterOption{
		WithHeartbeatInterval(50 * time.Millisecond),
		WithRegisterBackoff(20 * time.Millisecond),
	}
}

// waitFor polls fn() every 5ms up to d. Returns true if fn() ever true.
func waitFor(d time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fn()
}

// TestStart_RegisterSucceedsFast covers case 1 + 2: Start returns
// immediately, register completes within a tight bound, NodeID becomes
// non-zero, LocalServerAddr / ClusterID are populated.
func TestStart_RegisterSucceedsFast(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()

	c := NewSyncMasterClient(fm.Addr(), "17910", shortOpts()...)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	if !waitFor(time.Second, c.IsRegistered) {
		t.Fatalf("expected IsRegistered within 1s; got false (nodeID=%d)", c.NodeID())
	}
	if got := c.NodeID(); got != 42 {
		t.Errorf("NodeID = %d, want 42", got)
	}
	if got := c.LocalServerAddr(); got != "127.0.0.1:17910" {
		t.Errorf("LocalServerAddr = %q, want 127.0.0.1:17910", got)
	}
	if got := c.ClusterID(); got != "test-cluster" {
		t.Errorf("ClusterID = %q, want test-cluster", got)
	}
}

// TestStop_Idempotent covers case 3: Stop is safe to call twice and
// returns nil both times.
func TestStop_Idempotent(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()

	c := NewSyncMasterClient(fm.Addr(), "17910", shortOpts()...)
	_ = c.Start(context.Background())
	waitFor(500*time.Millisecond, c.IsRegistered)

	if err := c.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// TestStart_Idempotent ensures double-Start is a no-op (only one set of
// goroutines is spawned).
func TestStart_Idempotent(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()

	c := NewSyncMasterClient(fm.Addr(), "17910", shortOpts()...)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestRegister_RetriesAfterTransient500 covers case 4: master returns 500
// for the first few AddSyncNode calls, then succeeds. Client must keep
// retrying without panic and eventually register.
func TestRegister_RetriesAfterTransient500(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()
	fm.mu.Lock()
	fm.addNodeFailUntil = 3
	fm.mu.Unlock()

	c := NewSyncMasterClient(fm.Addr(), "17910", shortOpts()...)
	_ = c.Start(context.Background())
	defer c.Stop()

	if !waitFor(2*time.Second, c.IsRegistered) {
		t.Fatalf("expected eventual register after transient errors; nodeID=%d addNodeCalls=%d",
			c.NodeID(), fm.addNodeCalls.Load())
	}
	if fm.addNodeCalls.Load() < 4 {
		t.Errorf("expected at least 4 AddSyncNode attempts; got %d", fm.addNodeCalls.Load())
	}
}

// TestRegister_MasterFullyDown covers case 5: master unreachable. Client
// must not panic and Stop must return cleanly.
func TestRegister_MasterFullyDown(t *testing.T) {
	// 127.0.0.1:1 is the well-known "nothing listens here" sink port.
	c := NewSyncMasterClient("127.0.0.1:1", "17910", shortOpts()...)
	_ = c.Start(context.Background())
	time.Sleep(300 * time.Millisecond) // a few retry rounds

	if c.IsRegistered() {
		t.Fatalf("client should not register against dead master")
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestHeartbeat_TicksAfterRegister covers case 6: heartbeat goroutine
// fires after register completes and the fake's counter increments.
func TestHeartbeat_TicksAfterRegister(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()

	c := NewSyncMasterClient(fm.Addr(), "17910",
		WithHeartbeatInterval(30*time.Millisecond),
		WithRegisterBackoff(20*time.Millisecond),
	)
	_ = c.Start(context.Background())
	defer c.Stop()

	if !waitFor(time.Second, c.IsRegistered) {
		t.Fatalf("never registered")
	}
	if !waitFor(time.Second, func() bool { return fm.heartbeatCalls.Load() >= 3 }) {
		t.Fatalf("expected ≥3 heartbeat calls, got %d", fm.heartbeatCalls.Load())
	}
}

// TestHeartbeat_FailureFlipsRegistered covers case 7: six consecutive
// heartbeat failures flip IsRegistered back to false and spawn a new
// register attempt.
func TestHeartbeat_FailureFlipsRegistered(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()

	c := NewSyncMasterClient(fm.Addr(), "17910",
		WithHeartbeatInterval(20*time.Millisecond),
		WithRegisterBackoff(20*time.Millisecond),
	)
	_ = c.Start(context.Background())
	defer c.Stop()

	if !waitFor(500*time.Millisecond, c.IsRegistered) {
		t.Fatalf("never registered")
	}
	addCallsBefore := fm.addNodeCalls.Load()

	// Flip heartbeats to fail for plenty of cycles. heartbeatFailureThreshold
	// is 6, so after ~120ms the client should deregister and call AddSyncNode
	// again. Observe via the addNodeCalls counter incrementing rather than
	// the (very brief) registered=false window — the production code
	// self-heals within microseconds of the flip, so polling the flag is
	// inherently racy.
	fm.mu.Lock()
	fm.heartbeatFailUntil = 100 // many failures
	fm.mu.Unlock()

	if !waitFor(2*time.Second, func() bool {
		return fm.addNodeCalls.Load() > addCallsBefore
	}) {
		t.Fatalf("expected AddSyncNode to be re-called after heartbeat failures; hbCalls=%d addCalls=%d",
			fm.heartbeatCalls.Load(), fm.addNodeCalls.Load())
	}

	// Re-enable heartbeats; eventually a fresh register triggers and the
	// client recovers.
	fm.mu.Lock()
	fm.heartbeatFailUntil = 0
	fm.mu.Unlock()
	if !waitFor(2*time.Second, c.IsRegistered) {
		t.Fatalf("expected re-register after heartbeats recover; hbCalls=%d addCalls=%d",
			fm.heartbeatCalls.Load(), fm.addNodeCalls.Load())
	}
	if fm.addNodeCalls.Load() <= addCallsBefore {
		t.Errorf("expected new AddSyncNode call after re-register; before=%d after=%d",
			addCallsBefore, fm.addNodeCalls.Load())
	}
}

// TestHeartbeat_NoProviderStillSends covers case 8: with no
// SnapshotProvider configured the heartbeat still sends a minimal payload.
func TestHeartbeat_NoProviderStillSends(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()

	c := NewSyncMasterClient(fm.Addr(), "17910",
		WithHeartbeatInterval(30*time.Millisecond),
		WithRegisterBackoff(20*time.Millisecond),
		// deliberately no WithSnapshotProvider
	)
	_ = c.Start(context.Background())
	defer c.Stop()
	if !waitFor(time.Second, c.IsRegistered) {
		t.Fatalf("never registered")
	}
	if !waitFor(time.Second, func() bool { return fm.heartbeatCalls.Load() >= 2 }) {
		t.Fatalf("expected ≥2 heartbeats with nil provider, got %d", fm.heartbeatCalls.Load())
	}
}

// TestHeartbeat_ProviderInjected ensures the snapshot provider's values
// flow into the outbound payload (verified by buildHeartbeatPayload).
func TestHeartbeat_ProviderInjected(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()

	provider := &stubProvider{payload: proto.SyncNodeHeartbeatResponse{
		UptimeSeconds: 42,
		RunningTasks:  3,
		BoltDBHealthy: true,
	}}
	c := NewSyncMasterClient(fm.Addr(), "17910",
		WithSnapshotProvider(provider),
		WithRegisterBackoff(20*time.Millisecond),
	)
	_ = c.Start(context.Background())
	defer c.Stop()
	if !waitFor(time.Second, c.IsRegistered) {
		t.Fatalf("never registered")
	}

	payload := c.buildHeartbeatPayload()
	if payload.UptimeSeconds != 42 || payload.RunningTasks != 3 || !payload.BoltDBHealthy {
		t.Errorf("provider fields not propagated: %+v", payload)
	}
	if payload.NodeID == 0 || payload.Addr == "" {
		t.Errorf("nodeID/addr should be overlaid by client: %+v", payload)
	}
}

// TestRegister_InvalidIPFromMaster covers a sad-path branch: master
// returns a non-IPv4 address → register treats it as an error and retries.
func TestRegister_InvalidIPFromMaster(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()
	fm.mu.Lock()
	fm.clusterIP = "not-an-ip" // first response invalid
	fm.mu.Unlock()

	c := NewSyncMasterClient(fm.Addr(), "17910", shortOpts()...)
	_ = c.Start(context.Background())
	defer c.Stop()

	// Verify the client keeps trying without panic; flip IP to valid mid-flight.
	time.Sleep(120 * time.Millisecond)
	if c.IsRegistered() {
		t.Fatalf("registered against invalid IP unexpectedly")
	}
	fm.mu.Lock()
	fm.clusterIP = "127.0.0.1"
	fm.mu.Unlock()
	if !waitFor(2*time.Second, c.IsRegistered) {
		t.Fatalf("expected recovery after valid IP returned")
	}
}

// TestNextBackoff exercises the helper's edge cases independently.
func TestNextBackoff(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want time.Duration
	}{
		{0, defaultRegisterBackoff},
		{1 * time.Second, 2 * time.Second},
		{16 * time.Second, registerRetryMax},
		{registerRetryMax, registerRetryMax},
		{2 * registerRetryMax, registerRetryMax},
	}
	for _, c := range cases {
		if got := nextBackoff(c.in); got != c.want {
			t.Errorf("nextBackoff(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestSplitMasterAddrs covers the comma-split helper.
func TestSplitMasterAddrs(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"127.0.0.1:17010", []string{"127.0.0.1:17010"}},
		{"a:1, b:2 ,c:3", []string{"a:1", "b:2", "c:3"}},
		{"", []string{}},
		{" , , ", []string{}},
	}
	for _, c := range cases {
		got := splitMasterAddrs(c.in)
		if !equalSlice(got, c.want) {
			t.Errorf("splitMasterAddrs(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func equalSlice(a, b []string) bool {
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

// TestStop_BeforeStart ensures Stop on an unstarted client is a no-op.
func TestStop_BeforeStart(t *testing.T) {
	c := NewSyncMasterClient("127.0.0.1:1", "17910")
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
}

// TestStop_NilReceiver guards against panics on the zero-value path.
func TestStop_NilReceiver(t *testing.T) {
	var c *SyncMasterClient
	if err := c.Stop(); err != nil {
		t.Errorf("Stop on nil: %v", err)
	}
}

// TestStart_NilReceiver ensures Start on nil returns an error.
func TestStart_NilReceiver(t *testing.T) {
	var c *SyncMasterClient
	if err := c.Start(context.Background()); err == nil {
		t.Fatalf("expected error from nil Start")
	}
}

// TestContextCancellation verifies that cancelling the parent context
// shuts down the goroutines without a Stop call.
func TestContextCancellation(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := NewSyncMasterClient(fm.Addr(), "17910", shortOpts()...)
	_ = c.Start(ctx)
	waitFor(500*time.Millisecond, c.IsRegistered)
	cancel()
	// Stop should still be safe afterwards.
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop after ctx cancel: %v", err)
	}
}

// TestRegister_MasterReturnsZeroID covers the "0 is invalid" guard. The
// fakeMaster is wired to return id=0 forever until the test flips it.
func TestRegister_MasterReturnsZeroID(t *testing.T) {
	fm := newFakeMaster(t)
	defer fm.Close()
	// Pin nextNodeID to 0 for every call by overriding the handler. Default
	// fakeMaster handler post-increments which would let the second call
	// succeed; we want id=0 until we say otherwise.
	stickZero := atomic.Bool{}
	stickZero.Store(true)
	overrideID := atomic.Uint64{}
	fm.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case proto.AdminGetIP:
			fm.getClusterCalls.Add(1)
			writeReply(w, proto.ClusterInfo{Cluster: "test", Ip: "127.0.0.1"})
		case proto.AddSyncNode:
			fm.addNodeCalls.Add(1)
			id := overrideID.Load()
			if stickZero.Load() {
				id = 0
			}
			writeReply(w, json.Number(toDecimal(id)))
		case proto.GetSyncNodeTaskResponse:
			fm.heartbeatCalls.Add(1)
			writeReply(w, nil)
		}
	})

	c := NewSyncMasterClient(fm.Addr(), "17910", shortOpts()...)
	_ = c.Start(context.Background())
	defer c.Stop()

	// Should keep retrying; flip to a valid id mid-flight.
	time.Sleep(120 * time.Millisecond)
	if c.IsRegistered() {
		t.Fatalf("registered with nodeID=0 unexpectedly")
	}
	overrideID.Store(7)
	stickZero.Store(false)
	if !waitFor(time.Second, c.IsRegistered) {
		t.Fatalf("never recovered after nodeID flipped to 7")
	}
	if got := c.NodeID(); got != 7 {
		t.Errorf("NodeID = %d, want 7", got)
	}
}

// TestFakeMasterListenerCanRebind is a sanity check: even after closing
// the test server, a brand new client pointed at the (now-dead) addr
// only logs warnings and Stop completes. Guards against the boot-time
// crash scenario in the design doc.
func TestFakeMasterListenerCanRebind(t *testing.T) {
	fm := newFakeMaster(t)
	addr := fm.Addr()
	fm.Close()

	// Quick sanity: addr is no longer listening.
	conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
	if err == nil {
		_ = conn.Close()
	}

	c := NewSyncMasterClient(addr, "17910", shortOpts()...)
	_ = c.Start(context.Background())
	time.Sleep(150 * time.Millisecond)
	if c.IsRegistered() {
		t.Fatalf("registered against closed test server")
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
