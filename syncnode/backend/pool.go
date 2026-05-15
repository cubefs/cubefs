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

package backend

import (
	"fmt"
	"sync"
)

// PoolKey identifies a unique Backend client instance. The triple
// (Kind, Endpoint, Region) defines the "where to connect"; the
// Bucket field disambiguates per-bucket (s3) / per-volume (cfs)
// instances because the concrete Backend wrappers bake the bucket /
// volume into their state at construction time.
//
// Without Bucket in the key, two rules pointing at the same s3
// endpoint+region but different buckets would share one *s3.Backend
// whose bucket was set from whichever rule constructed it first —
// the second rule's reads/writes would silently hit the wrong
// bucket. Same hazard for cfs volume. See pool_test.go.
//
// CredKey disambiguates backends that share the same endpoint/bucket
// but use different credentials (e.g. two rules with different inline
// AK/SK injected by the dashboard). For env-var credentials it holds
// the AccessKeyEnv name; for inline credentials it holds the AK value.
// For backends without per-bucket identity (e.g. local), both Bucket
// and CredKey are "".
type PoolKey struct {
	Kind     string
	Endpoint string // S3 endpoint URL; empty for cfs/local
	Region   string // S3 region; empty for cfs/local
	Bucket   string // S3 bucket OR cfs volume; empty for local
	CredKey  string // credential discriminator; empty for cfs/local
}

func (k PoolKey) String() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s", k.Kind, k.Endpoint, k.Region, k.Bucket, k.CredKey)
}

// Pool caches Backend instances by PoolKey. Concurrent callers requesting
// the same key share the same Backend. Close() releases all instances.
//
// Concurrency: callers may call Acquire from many goroutines. Construction
// is single-flight per key — if 100 goroutines race to acquire the same
// PoolKey at the same time, the constructor runs exactly once.
type Pool struct {
	mu      sync.Mutex
	clients map[PoolKey]*pooled
	closed  bool
}

type pooled struct {
	once    sync.Once
	backend Backend
	err     error
	cfg     interface{} // remembered for re-construction if needed (unused for now)
}

// NewPool returns a fresh Pool. Each syncnode process has exactly one
// (passed around via SyncNode.backendPool).
func NewPool() *Pool {
	return &Pool{clients: make(map[PoolKey]*pooled)}
}

// Acquire returns the cached Backend for key, constructing it on first use.
// cfg must match the per-backend Config type registered for key.Kind; it's
// only used on the first Acquire and ignored on subsequent calls (because
// the cached client is shared).
func (p *Pool) Acquire(key PoolKey, cfg interface{}) (Backend, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("backend pool: closed")
	}
	entry, ok := p.clients[key]
	if !ok {
		entry = &pooled{cfg: cfg}
		p.clients[key] = entry
	}
	p.mu.Unlock()

	entry.once.Do(func() {
		b, err := New(key.Kind, cfg)
		if err != nil {
			entry.err = fmt.Errorf("construct %s: %w", key, err)
			return
		}
		entry.backend = b
	})
	if entry.err != nil {
		// Allow the next Acquire to retry: drop the failed entry from the
		// map so a follow-up call constructs fresh. Otherwise a transient
		// failure permanently breaks the pool slot.
		p.mu.Lock()
		if p.clients[key] == entry {
			delete(p.clients, key)
		}
		p.mu.Unlock()
		return nil, entry.err
	}
	return entry.backend, nil
}

// Close releases all cached backends. After Close, Acquire returns error.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	entries := p.clients
	p.clients = nil
	p.mu.Unlock()

	var firstErr error
	for _, e := range entries {
		if e.backend != nil {
			if err := e.backend.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Size returns the current number of cached backends. For metrics / tests.
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.clients)
}
