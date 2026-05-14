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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/syncnode/ratelimit"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// Defaults applied when MasterOption-less or zero-valued options are used.
// The register loop uses exponential backoff capped at registerRetryMax;
// the heartbeat goroutine ticks every defaultHeartbeatInterval.
const (
	defaultHeartbeatInterval = 10 * time.Second
	defaultRegisterBackoff   = 2 * time.Second
	registerRetryMax         = 30 * time.Second

	// heartbeatFailureThreshold is the number of consecutive heartbeat
	// failures that flips the client back to "unregistered" and re-arms a
	// fresh register attempt. 6 * 10s = ~60s of master silence.
	heartbeatFailureThreshold = 6
)

// HeartbeatSnapshotProvider is the small read-only interface SyncNode
// satisfies so the master client can populate runtime gauges in each
// heartbeat without taking a hard dependency on the SyncNode type.
//
// Snapshot fills the dynamic gauge fields; the master client overwrites
// Status / Result / NodeID / Addr / NodeVersion before sending so impls
// can leave those zero.
type HeartbeatSnapshotProvider interface {
	Snapshot() proto.SyncNodeHeartbeatResponse
}

// SyncMasterClient bundles the master.MasterClient + the register /
// heartbeat goroutines. One instance per SyncNode. Constructed in doStart;
// Start spawns goroutines, Stop signals them to exit.
type SyncMasterClient struct {
	mc *master.MasterClient

	// Static config recorded at construction.
	listenPort string

	// Tunables (defaults from constants above; tests inject shorter values).
	heartbeatInterval time.Duration
	registerBackoff   time.Duration

	// Optional runtime stats injector. nil → minimal heartbeat payload.
	snapshotProvider HeartbeatSnapshotProvider

	// Optional rate-limit registry. When non-nil, every heartbeat reply
	// carrying RuleQuotas / BackendQuotas drives SetRuleLimit /
	// SetBackendLimit on the registry (§12.4 / P1-8 + P1-9). nil → reply
	// quotas are ignored (used by older tests that don't care about
	// rate-limit plumbing).
	rateLimits *ratelimit.Registry

	// Identity, lazily filled by register() on first success. Accessed via
	// atomic / mutex to allow safe concurrent reads from handlers.
	nodeID     atomic.Uint64
	registered atomic.Bool

	idMu            sync.RWMutex
	localServerAddr string
	clusterID       string

	// Lifecycle.
	stopCh chan struct{}
	wg     sync.WaitGroup
	once   sync.Once // guards Start so a double-call is a no-op
	closed atomic.Bool
}

// MasterOption follows the functional-options convention used elsewhere in
// CubeFS (`util/exporter`, `sdk/data/stream/...`).
type MasterOption func(*SyncMasterClient)

// WithHeartbeatInterval overrides the default 10s tick. Useful in tests.
func WithHeartbeatInterval(d time.Duration) MasterOption {
	return func(c *SyncMasterClient) {
		if d > 0 {
			c.heartbeatInterval = d
		}
	}
}

// WithRegisterBackoff overrides the initial register retry interval. The
// backoff still ramps up via nextBackoff() / registerRetryMax.
func WithRegisterBackoff(d time.Duration) MasterOption {
	return func(c *SyncMasterClient) {
		if d > 0 {
			c.registerBackoff = d
		}
	}
}

// WithSnapshotProvider injects the runtime-stats source consulted on every
// heartbeat. Optional — if absent the heartbeat carries the minimum
// payload (NodeID + Addr).
func WithSnapshotProvider(p HeartbeatSnapshotProvider) MasterOption {
	return func(c *SyncMasterClient) { c.snapshotProvider = p }
}

// WithRateLimitRegistry installs the bandwidth-limit registry the client
// updates from heartbeat replies. Master computes per-node rule / backend
// quotas (sync_quota.go) and pushes them down via the reply; the client
// applies them through SetRuleLimit / SetBackendLimit on the registry
// (§12.4 / P1-8 + P1-9). Optional — without it the reply is parsed but
// no buckets are retuned.
func WithRateLimitRegistry(reg *ratelimit.Registry) MasterOption {
	return func(c *SyncMasterClient) { c.rateLimits = reg }
}

// NewSyncMasterClient constructs the client. masterAddr is the comma-
// separated multi-master string from sync.json; listenPort is the
// syncnode's TCP listen port (what master persists as the syncnode's
// address). Does NOT touch the network — Start does that.
func NewSyncMasterClient(masterAddr string, listenPort string, opts ...MasterOption) *SyncMasterClient {
	hosts := splitMasterAddrs(masterAddr)
	c := &SyncMasterClient{
		mc:                master.NewMasterClient(hosts, false),
		listenPort:        listenPort,
		heartbeatInterval: defaultHeartbeatInterval,
		registerBackoff:   defaultRegisterBackoff,
		stopCh:            make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start spawns the register + heartbeat goroutines and returns immediately.
// The first register attempt happens in the background so a missing master
// does not block syncnode boot. Idempotent.
func (c *SyncMasterClient) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("syncnode master client: nil receiver")
	}
	started := false
	c.once.Do(func() {
		c.wg.Add(2)
		go c.registerLoop(ctx)
		go c.heartbeatLoop(ctx)
		started = true
	})
	if !started {
		log.LogInfof("syncnode master client: Start called twice; ignoring")
	}
	return nil
}

// Stop signals the goroutines to exit and blocks until they do. Safe to
// call multiple times; subsequent calls are no-ops.
func (c *SyncMasterClient) Stop() error {
	if c == nil {
		return nil
	}
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.stopCh)
	c.wg.Wait()
	return nil
}

// NodeID returns the master-allocated node id. Zero until first register
// succeeds — pair with IsRegistered for the boolean view.
func (c *SyncMasterClient) NodeID() uint64 { return c.nodeID.Load() }

// IsRegistered reports whether the most recent register attempt succeeded
// (and the subsequent heartbeats have not all failed).
func (c *SyncMasterClient) IsRegistered() bool { return c.registered.Load() }

// LocalServerAddr returns the canonical "ip:port" the master sees us at.
// Empty until first register succeeds.
func (c *SyncMasterClient) LocalServerAddr() string {
	c.idMu.RLock()
	defer c.idMu.RUnlock()
	return c.localServerAddr
}

// ClusterID returns the cluster name discovered via GetClusterInfo. Empty
// until first register succeeds.
func (c *SyncMasterClient) ClusterID() string {
	c.idMu.RLock()
	defer c.idMu.RUnlock()
	return c.clusterID
}

// registerLoop drives one-shot register() attempts with exponential
// backoff. Exits on stopCh / ctx.Done(). Re-armed on heartbeat-failure
// threshold by heartbeatLoop.
func (c *SyncMasterClient) registerLoop(ctx context.Context) {
	defer c.wg.Done()
	backoff := c.registerBackoff
	if backoff <= 0 {
		backoff = defaultRegisterBackoff
	}
	for {
		// Cooperative cancellation point.
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		if err := c.register(); err != nil {
			log.LogWarnf("syncnode register: %v (retry in %v)", err, backoff)
			if !sleepCtx(ctx, c.stopCh, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		log.LogInfof("syncnode registered: nodeID=%d addr=%s cluster=%s",
			c.NodeID(), c.LocalServerAddr(), c.ClusterID())
		return
	}
}

// register performs one attempt. Returns error on any step failure so the
// caller can decide whether to retry.
func (c *SyncMasterClient) register() error {
	ci, err := c.mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return fmt.Errorf("get cluster info: %w", err)
	}
	if ci == nil {
		return errors.New("get cluster info: nil response")
	}
	if !util.IsIPV4(ci.Ip) {
		return fmt.Errorf("invalid local ip %q from master", ci.Ip)
	}
	addr := fmt.Sprintf("%s:%s", ci.Ip, c.listenPort)

	id, err := c.mc.NodeAPI().AddSyncNode(addr)
	if err != nil {
		return fmt.Errorf("add sync node: %w", err)
	}
	if id == 0 {
		return errors.New("add sync node: master returned nodeID=0")
	}

	c.nodeID.Store(id)
	c.idMu.Lock()
	c.localServerAddr = addr
	c.clusterID = ci.Cluster
	c.idMu.Unlock()
	c.registered.Store(true)
	return nil
}

// heartbeatLoop ticks every heartbeatInterval. When registered, it sends a
// heartbeat; on heartbeatFailureThreshold consecutive failures it flips
// registered back to false and re-arms a fresh register goroutine.
func (c *SyncMasterClient) heartbeatLoop(ctx context.Context) {
	defer c.wg.Done()
	interval := c.heartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	consecutiveFailures := 0
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if !c.IsRegistered() {
			// First register hasn't completed yet — wait quietly.
			continue
		}
		if err := c.sendHeartbeat(ctx); err != nil {
			consecutiveFailures++
			log.LogWarnf("syncnode heartbeat: %v (consecutive=%d)", err, consecutiveFailures)
			if consecutiveFailures >= heartbeatFailureThreshold {
				log.LogWarnf("syncnode lost master after %d failed heartbeats; re-registering",
					consecutiveFailures)
				c.registered.Store(false)
				c.wg.Add(1)
				go c.registerLoop(ctx)
				consecutiveFailures = 0
			}
			continue
		}
		consecutiveFailures = 0
	}
}

// sendHeartbeat pushes a heartbeat envelope to the master via the
// ResponseSyncNodeTask path. The current SDK call discards the master's
// reply body; once the SDK exposes a typed-reply variant we'll decode
// SyncNodeHeartbeatResponse here and feed any RuleQuotas / BackendQuotas
// into applyHeartbeatReply. Until then this is a single push.
//
// TODO(P1-8+9 wiring): swap ResponseSyncNodeTask for a variant that
// returns the decoded *proto.SyncNodeHeartbeatResponse, then call
// c.applyHeartbeatReply on it. Master's outgoing-task injection (in
// cluster.go) is wired conditionally on the same TODO.
func (c *SyncMasterClient) sendHeartbeat(_ context.Context) error {
	resp := c.buildHeartbeatPayload()
	task := proto.NewAdminTask(proto.OpSyncNodeHeartbeat, c.LocalServerAddr(), nil)
	task.Response = &resp
	if err := c.mc.NodeAPI().ResponseSyncNodeTask(task); err != nil {
		return fmt.Errorf("response sync node task: %w", err)
	}
	return nil
}

// applyHeartbeatReply pushes the master-computed quota maps from a
// heartbeat reply into the local rate-limit registry. A nil registry or
// nil reply is a no-op so older callers / tests that don't care about
// rate-limit plumbing keep working. Empty / missing entries on either map
// are tolerated; a zero MB/s value removes the bucket (back to unlimited)
// — see Registry.SetRuleLimit / SetBackendLimit.
//
// Exported for testability; the production caller is sendHeartbeat once
// the SDK reply-decode path lands (see the TODO there).
func (c *SyncMasterClient) applyHeartbeatReply(reply *proto.SyncNodeHeartbeatResponse) {
	if c == nil || c.rateLimits == nil || reply == nil {
		return
	}
	for ruleID, mbps := range reply.RuleQuotas {
		c.rateLimits.SetRuleLimit(ruleID, int(mbps))
	}
	for keyStr, mbps := range reply.BackendQuotas {
		k, ok := ratelimit.ParseBackendKey(keyStr)
		if !ok {
			log.LogWarnf("syncnode heartbeat: malformed backend key %q in reply", keyStr)
			continue
		}
		c.rateLimits.SetBackendLimit(k, int(mbps))
	}
}

// buildHeartbeatPayload assembles the SyncNodeHeartbeatResponse for the
// outbound heartbeat. With no SnapshotProvider it carries only NodeID +
// Addr; with one it carries the full runtime gauge set.
func (c *SyncMasterClient) buildHeartbeatPayload() proto.SyncNodeHeartbeatResponse {
	var resp proto.SyncNodeHeartbeatResponse
	if c.snapshotProvider != nil {
		resp = c.snapshotProvider.Snapshot()
	}
	resp.Status = proto.TaskSucceeds
	resp.NodeID = c.NodeID()
	resp.Addr = c.LocalServerAddr()
	if resp.NodeVersion == "" {
		resp.NodeVersion = BuildVersion
	}
	return resp
}

// nextBackoff doubles the current interval up to registerRetryMax.
func nextBackoff(cur time.Duration) time.Duration {
	if cur <= 0 {
		return defaultRegisterBackoff
	}
	next := cur * 2
	if next > registerRetryMax {
		next = registerRetryMax
	}
	return next
}

// sleepCtx blocks for d or until stopCh / ctx is signalled. Returns true
// if d elapsed; false if cancelled.
func sleepCtx(ctx context.Context, stopCh <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-stopCh:
		return false
	case <-ctx.Done():
		return false
	}
}

// ResponseTask posts a task lifecycle update back to master via
// ResponseSyncNodeTask. Thin wrapper around the SDK call so callers (in
// particular task_handler.go's lifecycle push-back path) don't reach into the
// embedded master.MasterClient directly.
func (c *SyncMasterClient) ResponseTask(task *proto.AdminTask) error {
	if c == nil || c.mc == nil {
		return errors.New("syncnode master client: not initialised")
	}
	if err := c.mc.NodeAPI().ResponseSyncNodeTask(task); err != nil {
		return fmt.Errorf("response sync node task: %w", err)
	}
	return nil
}

// splitMasterAddrs parses the comma-separated masterAddr config field
// (mirrors NewMasterClientFromString but tolerates whitespace).
func splitMasterAddrs(s string) []string {
	out := make([]string, 0, 4)
	for _, part := range strings.Split(s, ",") {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}
