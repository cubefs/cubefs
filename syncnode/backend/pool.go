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

// PoolKey identifies a logical Backend "client" — the same (kind, endpoint,
// region) triple maps to the same shared instance so we don't waste
// HTTP/2 connection pools or credential refreshers across rules.
//
// For backends without endpoint/region (e.g. cfs), Endpoint / Region are
// empty strings — every cfs vol may share a single client (further
// scoping by vol happens inside the cfs adapter).
type PoolKey struct {
	Kind     string
	Endpoint string // S3 endpoint URL; empty for cfs/local
	Region   string // S3 region; empty for cfs/local
}

func (k PoolKey) String() string {
	return fmt.Sprintf("%s|%s|%s", k.Kind, k.Endpoint, k.Region)
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
