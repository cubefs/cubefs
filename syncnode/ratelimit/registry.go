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
	"sync"
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

// Registry holds the node-level (layer 3) bucket and per-backend (layer 4,
// node-local) buckets. One Registry is created per syncnode process and
// injected into the executor via WithRateLimitRegistry. Per-task (layer 1)
// buckets are constructed on demand at transfer start, not stored here.
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
}

// NewRegistry returns a Registry with the node-level bucket pre-installed.
// nodeMBps <= 0 leaves the node bucket unlimited.
func NewRegistry(nodeMBps int) *Registry {
	return &Registry{
		nodeBucket: NewBucket(nodeMBps),
		backend:    make(map[BackendKey]*Bucket),
	}
}

// NodeBucket returns the layer-3 bucket. Never nil.
func (r *Registry) NodeBucket() *Bucket {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nodeBucket
}

// SetNodeLimit retunes the layer-3 bucket. mbps <= 0 disables limiting.
// Useful for dynamic reconfiguration (P2-M).
func (r *Registry) SetNodeLimit(mbps int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeBucket.SetLimit(mbps)
}

// SetBackendLimit installs or updates the per-backend bucket. mbps <= 0
// removes the entry — subsequent BackendBucket calls return nil and
// callers skip the layer entirely (equivalent to unlimited for that key).
// Existing Bucket instances are reused on update so in-flight transfers
// retune dynamically rather than holding stale buckets.
func (r *Registry) SetBackendLimit(k BackendKey, mbps int) {
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
	r.backend[k] = NewBucket(mbps)
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
func (r *Registry) Snapshot() map[BackendKey]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[BackendKey]int, len(r.backend))
	for k, b := range r.backend {
		out[k] = b.Mbps()
	}
	return out
}
