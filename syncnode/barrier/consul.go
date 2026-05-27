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

// Package barrier implements the cross-shard "stage barrier" used by the
// bench executor (S1.6). Each syncnode running one shard of a bench task
// announces its readiness for a named stage via Consul KV; the call
// returns once every peer has done the same, or the configured timeout
// has elapsed.
//
// Design choice: Consul over a master-side RPC.
//   - syncnodes already have a Consul client available (every other CubeFS
//     role registers there). Avoids inventing a new master protocol.
//   - Consul sessions give automatic key-release on crash: if a shard
//     dies mid-barrier its ready key is dropped within the session TTL,
//     so subsequent retries see a clean slate.
//   - KV blocking queries (WaitIndex) give us a long-poll without a busy
//     loop.
//
// Failure modes:
//   - Consul unreachable at Ready(): returns the error; caller (executor)
//     logs and proceeds without waiting. The stage still runs.
//   - Timeout reached before all peers ready: returns ErrBarrierTimeout.
//     Same caller policy applies.
//   - Context cancelled (task cancelled / process shutting down):
//     returns ctx.Err(); session is released in the defer.
//
// All ready keys for a (taskID, stage) live under a deterministic KV
// prefix so the master / operators can inspect barrier progress with a
// single `consul kv get -recurse`.
package barrier

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
	consulapi "github.com/hashicorp/consul/api"
)

// ErrBarrierTimeout is returned by Ready when the configured wait window
// elapses before all expected shards register. The caller decides whether
// to abort or continue the stage; the executor logs and continues.
var ErrBarrierTimeout = errors.New("barrier: timeout waiting for peers")

// Barrier coordinates "wait until N shards are ready" semantics across
// syncnodes. Implementations must be safe for concurrent Ready calls
// (different stages may run their barriers in parallel) and must release
// their underlying resources on Close.
type Barrier interface {
	// Ready announces that shardID is ready for (taskID, stage) and
	// blocks until at least expectShards shards have done the same, or
	// timeout elapses, or ctx is cancelled.
	//
	// Returns nil on success, ErrBarrierTimeout on timeout, ctx.Err() on
	// cancellation, or a wrapped error on transport failure.
	Ready(ctx context.Context, taskID, stage, shardID string, expectShards int, timeout time.Duration) error
	// Close releases any persistent resources. Idempotent.
	Close() error
}

// keyPrefix returns the KV prefix under which all ready keys for one
// (taskID, stage) tuple live. Centralised so all readers / writers agree.
func keyPrefix(taskID, stage string) string {
	return fmt.Sprintf("cubefs/bench/%s/barrier/%s/", taskID, stage)
}

// keyForShard composes the KV key one shard registers under.
func keyForShard(taskID, stage, shardID string) string {
	return keyPrefix(taskID, stage) + shardID
}

// consulBarrier is the production implementation. One *consulapi.Client
// per syncnode; reused across many Ready calls.
type consulBarrier struct {
	client *consulapi.Client
	addr   string
}

// NewConsulBarrier builds a Barrier backed by the Consul HTTP API at
// addr. Address forms accepted: "host:port", "http://host:port",
// "https://host:port". An empty addr falls back to consulapi defaults
// (CONSUL_HTTP_ADDR env / 127.0.0.1:8500).
func NewConsulBarrier(addr string) (Barrier, error) {
	cfg := consulapi.DefaultConfig()
	if addr != "" {
		cfg.Address = stripScheme(addr)
		if hasHTTPSScheme(addr) {
			cfg.Scheme = "https"
		}
	}
	c, err := consulapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("consul barrier: new client: %w", err)
	}
	// Best-effort sanity check — if Consul is down right now we still
	// return a usable client (Ready() reports the error per-call), but
	// a leader-known response on startup is a strong signal the wiring
	// is correct.
	if _, err := c.Status().Leader(); err != nil {
		log.LogWarnf("barrier: consul leader probe failed at %q: %v (Ready calls will fail until consul recovers)", addr, err)
	}
	return &consulBarrier{client: c, addr: addr}, nil
}

func stripScheme(addr string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(addr) > len(p) && addr[:len(p)] == p {
			return addr[len(p):]
		}
	}
	return addr
}

func hasHTTPSScheme(addr string) bool {
	const p = "https://"
	return len(addr) >= len(p) && addr[:len(p)] == p
}

// Ready implements Barrier.
//
// Sequence:
//  1. Create a Consul session (TTL 30s, Behavior=release) so a shard
//     crash auto-releases its key.
//  2. Renew the session in a background goroutine until ctx done or
//     Ready returns.
//  3. Acquire the per-shard KV key under that session.
//  4. Poll the prefix with a blocking query until len >= expectShards
//     OR the timeout fires.
//  5. Destroy the session in defer (Release the key + drop session).
func (b *consulBarrier) Ready(ctx context.Context, taskID, stage, shardID string, expectShards int, timeout time.Duration) error {
	if b == nil || b.client == nil {
		return errors.New("consul barrier: nil client")
	}
	if expectShards <= 1 {
		// Solo shard — barrier is a no-op but still publish ready so
		// operators inspecting Consul see the stage start.
		_, err := b.client.KV().Put(&consulapi.KVPair{
			Key:   keyForShard(taskID, stage, shardID),
			Value: []byte("ready"),
		}, nil)
		if err != nil {
			return fmt.Errorf("consul barrier: solo write: %w", err)
		}
		return nil
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	// Wrap ctx with the barrier-level timeout so the blocking query
	// returns promptly when either deadline fires.
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 1) create session
	sess := &consulapi.SessionEntry{
		Name:      fmt.Sprintf("cubefs-bench/%s/%s/%s", taskID, stage, shardID),
		TTL:       "30s",
		Behavior:  consulapi.SessionBehaviorRelease,
		LockDelay: 0,
	}
	sessID, _, err := b.client.Session().Create(sess, nil)
	if err != nil {
		return fmt.Errorf("consul barrier: create session: %w", err)
	}
	// 2) auto-renew session in background until Ready exits.
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		// RenewPeriodic returns on doneCh close; we close renewDoneCh
		// in the defer below so it stops in lockstep with this call.
		_ = b.client.Session().RenewPeriodic("15s", sessID, nil, waitCtx.Done())
	}()
	// Defer: always destroy the session (releases the KV lock).
	defer func() {
		// Best-effort destroy; ignore errors — caller already returning.
		_, _ = b.client.Session().Destroy(sessID, nil)
		<-renewDone
	}()

	// 3) acquire our key under the session
	pair := &consulapi.KVPair{
		Key:     keyForShard(taskID, stage, shardID),
		Value:   []byte("ready"),
		Session: sessID,
	}
	ok, _, err := b.client.KV().Acquire(pair, nil)
	if err != nil {
		return fmt.Errorf("consul barrier: acquire %s: %w", pair.Key, err)
	}
	if !ok {
		// Another shard already holds this exact key (re-dispatch of the
		// same shard ID). Treat as success — the original holder will
		// publish ready on our behalf. Don't block the cluster.
		log.LogWarnf("barrier: key %s already held; treating as ready", pair.Key)
	}

	// 4) blocking-query loop on the prefix list.
	prefix := keyPrefix(taskID, stage)
	var lastIdx uint64
	for {
		// Bound each blocking call so a stuck Consul doesn't pin us
		// past the timeout. consul/api's QueryOptions.WaitTime is
		// honoured as a hard cap on the long-poll window.
		queryOpts := &consulapi.QueryOptions{
			WaitIndex: lastIdx,
			WaitTime:  5 * time.Second,
		}
		pairs, meta, err := b.client.KV().List(prefix, queryOpts.WithContext(waitCtx))
		if err != nil {
			// ctx-derived deadline error is the canonical "timeout"
			// signal — we map it to ErrBarrierTimeout below.
			if waitCtx.Err() != nil {
				break
			}
			return fmt.Errorf("consul barrier: list %s: %w", prefix, err)
		}
		if meta != nil {
			lastIdx = meta.LastIndex
		}
		if countReady(pairs) >= expectShards {
			return nil
		}
		// Loop continues; the next List call will block again on
		// WaitIndex change.
		if waitCtx.Err() != nil {
			break
		}
	}

	// 5) timeout / cancel branch
	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}
	return ErrBarrierTimeout
}

func countReady(pairs consulapi.KVPairs) int {
	n := 0
	for _, p := range pairs {
		if p == nil {
			continue
		}
		// Only count keys actually held by a live session — Behavior=release
		// keeps the key around with Session="" after release, which would
		// inflate the count.
		if p.Session == "" {
			continue
		}
		n++
	}
	return n
}

// Close releases resources held by the barrier. The consul/api client
// owns an http.Client that's safe to GC; we keep Close idempotent so
// callers don't have to track lifecycle.
func (b *consulBarrier) Close() error {
	// Nothing persistent to release. Sessions are scoped per Ready call.
	return nil
}

// MemBarrier is an in-memory Barrier intended for unit tests AND for the
// degraded production path where Consul cannot be reached at startup
// (single-shard fallback). It is process-local: peers running in
// different processes will NOT see each other.
//
// Construct with NewMemBarrier(expectShards). Ready returns immediately
// once expectShards distinct shardIDs have called Ready for the same
// (taskID, stage); concurrent waiters all unblock.
type MemBarrier struct {
	mu      sync.Mutex
	waiters map[string]*memBarrierState
	defaultExpect int
}

type memBarrierState struct {
	shards map[string]struct{}
	signal chan struct{}
}

// NewMemBarrier returns a process-local barrier. defaultExpect is used
// by callers that don't supply expectShards (e.g. degraded mode where
// only one shard exists); Ready uses the per-call expectShards when
// non-zero.
func NewMemBarrier(defaultExpect int) *MemBarrier {
	if defaultExpect < 1 {
		defaultExpect = 1
	}
	return &MemBarrier{
		waiters:       make(map[string]*memBarrierState),
		defaultExpect: defaultExpect,
	}
}

// Ready implements Barrier on top of in-memory state.
func (m *MemBarrier) Ready(ctx context.Context, taskID, stage, shardID string, expectShards int, timeout time.Duration) error {
	if expectShards <= 0 {
		expectShards = m.defaultExpect
	}
	key := taskID + "|" + stage
	m.mu.Lock()
	st, ok := m.waiters[key]
	if !ok {
		st = &memBarrierState{
			shards: make(map[string]struct{}),
			signal: make(chan struct{}),
		}
		m.waiters[key] = st
	}
	st.shards[shardID] = struct{}{}
	reached := len(st.shards) >= expectShards
	sig := st.signal
	if reached {
		// Close exactly once — recreating the channel here so a follow-up
		// stage on the same key starts fresh. The next Ready call will
		// allocate a new state (we delete the key below) so this guard
		// against double-close is just defensive.
		select {
		case <-sig:
			// already closed
		default:
			close(sig)
		}
		// Remove the entry so the next call for a fresh (taskID, stage)
		// starts a new waiter set. This matches the Consul implementation
		// where session destroy releases the keys.
		delete(m.waiters, key)
	}
	m.mu.Unlock()

	if reached {
		return nil
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-sig:
		return nil
	case <-waitCtx.Done():
		// On timeout, also remove our shardID so a retry can re-register
		// cleanly. Other peers still waiting will time out independently.
		m.mu.Lock()
		if st2, ok := m.waiters[key]; ok {
			delete(st2.shards, shardID)
		}
		m.mu.Unlock()
		if ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
		return ErrBarrierTimeout
	}
}

// Close implements Barrier. No persistent resources.
func (m *MemBarrier) Close() error { return nil }
