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

package ratelimit

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// BackendKey identifies one (kind, endpoint, region) triple — the same
// shape as backend.PoolKey. Keeping it as a value type means it can be
// used as a map key directly. Defining it locally (instead of importing
// backend) keeps the dependency graph one-way: executor depends on
// ratelimit, ratelimit does not depend on backend.
type BackendKey struct {
	Kind     string
	Endpoint string
	Region   string
}

// String formats the key for diagnostics.
func (k BackendKey) String() string {
	return fmt.Sprintf("%s|%s|%s", k.Kind, k.Endpoint, k.Region)
}

// ParseBackendKey is the inverse of BackendKey.String(). Returns ok=false
// on a malformed input (anything other than exactly three "|"-separated
// fields). Master ships keys in this format via the heartbeat-reply
// BackendQuotas map; the syncnode master client uses ParseBackendKey to
// decode them before pushing into the Registry (§12.4 / P1-9).
func ParseBackendKey(s string) (BackendKey, bool) {
	parts := strings.SplitN(s, "|", 3)
	if len(parts) != 3 {
		return BackendKey{}, false
	}
	if parts[0] == "" {
		return BackendKey{}, false
	}
	return BackendKey{Kind: parts[0], Endpoint: parts[1], Region: parts[2]}, true
}

// Registry holds the node-level (layer 3) bucket and per-backend (layer 4,
// node-local) buckets. One Registry is created per syncnode process and
// injected into the executor via WithRateLimitRegistry. Per-task (layer 1)
// buckets are constructed on demand at transfer start, not stored here.
//
// P1-8 added per-rule (layer 2) buckets keyed by rule.ID. Master computes
// the per-node share of the cluster-wide rule cap and ships it to each
// syncnode via the heartbeat reply; the syncnode calls SetRuleLimit to
// install / retune the bucket. RuleBucket returns nil when no quota is
// configured so callers skip the layer (preserves the unthrottled fast
// path).
//
// Registry is safe for concurrent use.
type Registry struct {
	mu sync.Mutex

	// nodeBucket is never nil — NewRegistry always installs one. A zero
	// rate leaves it unlimited.
	nodeBucket *Bucket

	// backend holds layer-4 buckets keyed by (kind, endpoint, region).
	// Absent keys mean "no per-backend cap"; callers should treat a nil
	// return from BackendBucket as "skip this layer".
	backend map[BackendKey]*Bucket

	// rule holds layer-2 buckets keyed by rule.ID. Absent keys mean "no
	// per-rule cap"; callers should treat a nil return from RuleBucket
	// as "skip this layer".
	rule map[string]*Bucket

	// totalBytes counts bytes successfully transferred through the registry.
	// ObserveBytes increments it; TotalBytesObserved reads it. The snapshot
	// cache differentiates two readings to derive the egress MB/s gauge.
	totalBytes atomic.Int64
}

// ObserveBytes records n bytes of successfully transferred data. Called from
// the executor after every file transfer completes. Safe for concurrent use.
func (r *Registry) ObserveBytes(n int64) {
	if n > 0 {
		r.totalBytes.Add(n)
	}
}

// TotalBytesObserved returns the cumulative byte count since the Registry was
// created. Callers compute an egress rate by differencing two readings taken
// a known interval apart.
func (r *Registry) TotalBytesObserved() int64 {
	return r.totalBytes.Load()
}

// NewRegistry returns a Registry with the node-level bucket pre-installed.
// nodeMBps <= 0 leaves the node bucket unlimited.
func NewRegistry(nodeMBps int) *Registry {
	return &Registry{
		nodeBucket: NewBucket(nodeMBps),
		backend:    make(map[BackendKey]*Bucket),
		rule:       make(map[string]*Bucket),
	}
}

// NodeBucket returns the layer-3 bucket. Never nil.
func (r *Registry) NodeBucket() *Bucket {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nodeBucket
}

// SetNodeLimit retunes the layer-3 bucket. mbps <= 0 disables limiting.
// Useful for dynamic reconfiguration (P2-M). Accepts int because the
// node-wide cap is operator-configured (whole MB/s); the dynamic per-rule
// / per-backend setters take float64 (see SetRuleLimit / SetBackendLimit).
func (r *Registry) SetNodeLimit(mbps int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeBucket.SetLimit(float64(mbps))
}

// SetBackendLimit installs or updates the per-backend bucket. mbps <= 0
// removes the entry — subsequent BackendBucket calls return nil and
// callers skip the layer entirely (equivalent to unlimited for that key).
// Existing Bucket instances are reused on update so in-flight transfers
// retune dynamically rather than holding stale buckets.
//
// mbps is float64 because master computes per-node shares as
// cluster_cap / N where N is the active-node count, which is commonly
// fractional (SEC5).
func (r *Registry) SetBackendLimit(k BackendKey, mbps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if mbps <= 0 {
		delete(r.backend, k)
		return
	}
	if b, ok := r.backend[k]; ok {
		b.SetLimit(mbps)
		return
	}
	b := &Bucket{}
	b.setLimitLocked(mbps)
	r.backend[k] = b
}

// BackendBucket returns the layer-4 bucket for k, or nil if no limit is
// configured. Callers treat nil as "skip this layer".
func (r *Registry) BackendBucket(k BackendKey) *Bucket {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.backend[k]
}

// Snapshot returns a copy of the current per-backend configuration for
// diagnostics / tests. The returned map is independent of the Registry's
// internal state.
func (r *Registry) Snapshot() map[BackendKey]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[BackendKey]float64, len(r.backend))
	for k, b := range r.backend {
		out[k] = b.Mbps()
	}
	return out
}

// SetRuleLimit installs or updates the per-rule (layer 2) bucket. mbps <= 0
// removes the entry — subsequent RuleBucket calls return nil and callers
// skip the layer entirely (equivalent to unlimited for that rule).
// Existing Bucket instances are reused on update so in-flight transfers
// retune dynamically rather than holding stale buckets. Same shape as
// SetBackendLimit so master's quota-update path is symmetric across the
// two layers (§12.4 / P1-8).
//
// mbps is float64 because master computes per-node shares as
// cluster_cap / N where N is the active-node count, which is commonly
// fractional (SEC5).
func (r *Registry) SetRuleLimit(ruleID string, mbps float64) {
	if ruleID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if mbps <= 0 {
		delete(r.rule, ruleID)
		return
	}
	if b, ok := r.rule[ruleID]; ok {
		b.SetLimit(mbps)
		return
	}
	b := &Bucket{}
	b.setLimitLocked(mbps)
	r.rule[ruleID] = b
}

// RuleBucket returns the layer-2 bucket for ruleID, or nil if no limit is
// configured. Callers treat nil as "skip this layer".
func (r *Registry) RuleBucket(ruleID string) *Bucket {
	if ruleID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rule[ruleID]
}

// RuleSnapshot returns a copy of the current per-rule configuration for
// diagnostics / tests. The returned map is independent of the Registry's
// internal state.
func (r *Registry) RuleSnapshot() map[string]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]float64, len(r.rule))
	for k, b := range r.rule {
		out[k] = b.Mbps()
	}
	return out
}
