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
	"math"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/tasks"
)

// TestSnapshot_ZeroSyncNode covers the safe-defaults branch when the
// SyncNode is partially constructed (executor/scheduler/boltDB still nil).
// Snapshot must not panic and must return reasonable zero values for the
// gauges that depend on uninitialised subsystems.
func TestSnapshot_ZeroSyncNode(t *testing.T) {
	s := &SyncNode{}
	got := s.Snapshot()
	if got.RunningTasks != 0 {
		t.Errorf("RunningTasks = %d, want 0 on bare SyncNode", got.RunningTasks)
	}
	if got.ScheduledRules != 0 {
		t.Errorf("ScheduledRules = %d, want 0 on bare SyncNode", got.ScheduledRules)
	}
	if got.BoltDBHealthy {
		t.Errorf("BoltDBHealthy = true on bare SyncNode; want false")
	}
	// UptimeSeconds is populated from package-level startedAt; we only
	// assert non-negative since the test process startedAt is set in metrics.
	if got.UptimeSeconds < 0 {
		t.Errorf("UptimeSeconds = %d, want ≥ 0", got.UptimeSeconds)
	}
	// Load-score inputs default to zero on a bare node (no cfg, no
	// taskStore). These must not panic and must report zero.
	if got.MaxConcurrentTasks != 0 {
		t.Errorf("MaxConcurrentTasks = %d, want 0 on bare SyncNode", got.MaxConcurrentTasks)
	}
	if got.BandwidthMBpsLimit != 0 {
		t.Errorf("BandwidthMBpsLimit = %f, want 0 on bare SyncNode", got.BandwidthMBpsLimit)
	}
	if got.LastTaskFailureRate != 0 {
		t.Errorf("LastTaskFailureRate = %f, want 0 on bare SyncNode", got.LastTaskFailureRate)
	}
}

// TestSnapshot_LoadScoreInputsFromConfig verifies the concurrency caps
// are read from s.cfg.Concurrency on the heartbeat hot path.
func TestSnapshot_LoadScoreInputsFromConfig(t *testing.T) {
	s := &SyncNode{
		cfg: &SyncConfig{
			Concurrency: ConcurrencyConfig{
				MaxConcurrentTasks: 16,
				BandwidthLimitMBps: 250,
			},
		},
	}
	got := s.Snapshot()
	if got.MaxConcurrentTasks != 16 {
		t.Errorf("MaxConcurrentTasks = %d, want 16", got.MaxConcurrentTasks)
	}
	if got.BandwidthMBpsLimit != 250 {
		t.Errorf("BandwidthMBpsLimit = %f, want 250", got.BandwidthMBpsLimit)
	}
}

// TestComputeRecentFailureRate_NilStore covers the early-return path
// before the taskStore is wired up (startup race).
func TestComputeRecentFailureRate_NilStore(t *testing.T) {
	s := &SyncNode{}
	if rate := s.computeRecentFailureRate(5 * time.Minute); rate != 0 {
		t.Errorf("rate = %v, want 0 when taskStore is nil", rate)
	}
}

// TestComputeRecentFailureRate covers the four shapes the dispatcher
// cares about: empty store, all-success, mixed, and window-cutoff.
func TestComputeRecentFailureRate(t *testing.T) {
	tests := []struct {
		name    string
		records []*tasks.Record
		window  time.Duration
		want    float64
	}{
		{
			name:    "empty store yields zero",
			records: nil,
			window:  5 * time.Minute,
			want:    0,
		},
		{
			name: "all-passing yields zero",
			records: []*tasks.Record{
				doneRecord("t1", executor.StatusDone, -1*time.Minute),
				doneRecord("t2", executor.StatusDone, -2*time.Minute),
				doneRecord("t3", executor.StatusDone, -3*time.Minute),
			},
			window: 5 * time.Minute,
			want:   0,
		},
		{
			name: "mixed inside window yields proportion",
			records: []*tasks.Record{
				doneRecord("t1", executor.StatusDone, -30*time.Second),
				doneRecord("t2", executor.StatusFailed, -1*time.Minute),
				doneRecord("t3", executor.StatusDone, -2*time.Minute),
				doneRecord("t4", executor.StatusFailed, -3*time.Minute),
				doneRecord("t5", executor.StatusFailed, -4*time.Minute),
			},
			window: 5 * time.Minute,
			want:   3.0 / 5.0,
		},
		{
			name: "records older than window are ignored",
			records: []*tasks.Record{
				doneRecord("t1", executor.StatusDone, -1*time.Minute),
				doneRecord("t2", executor.StatusFailed, -10*time.Minute), // outside window
			},
			window: 5 * time.Minute,
			want:   0, // only success inside window → 0/1 = 0
		},
		{
			name: "in-flight records (zero DoneAt) are ignored",
			records: []*tasks.Record{
				{TaskID: "running1", Status: executor.StatusRunning},
				doneRecord("t1", executor.StatusFailed, -1*time.Minute),
			},
			window: 5 * time.Minute,
			want:   1.0, // 1 failed / 1 terminal inside window
		},
		{
			name: "cancelled records count as non-failed",
			records: []*tasks.Record{
				doneRecord("t1", executor.StatusCancelled, -1*time.Minute),
				doneRecord("t2", executor.StatusFailed, -2*time.Minute),
			},
			window: 5 * time.Minute,
			want:   0.5,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := tasks.NewMemoryStore()
			ctx := context.Background()
			for _, r := range tc.records {
				if err := store.Put(ctx, r); err != nil {
					t.Fatalf("seed Put: %v", err)
				}
			}
			s := &SyncNode{taskStore: store}
			got := s.computeRecentFailureRate(tc.window)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("rate = %v, want %v", got, tc.want)
			}
		})
	}
}

// doneRecord builds a terminal *tasks.Record with DoneAt = now+offset
// (use a negative offset for past records). Helper for table-driven tests.
func doneRecord(id string, status executor.Status, offset time.Duration) *tasks.Record {
	return &tasks.Record{
		TaskID:    id,
		Status:    status,
		StartedAt: time.Now().Add(offset - 1*time.Second),
		DoneAt:    time.Now().Add(offset),
	}
}

// TestSnapshot_UsesCachedValues verifies Snapshot() serves the cached
// failure rate + rules vector instead of doing a synchronous BoltDB scan.
// Install a pre-populated snapshotCache directly on a bare *SyncNode and
// confirm the returned response mirrors it exactly.
func TestSnapshot_UsesCachedValues(t *testing.T) {
	cache := &snapshotCache{}
	cache.failureRate.Store(float64(0.42))
	want := []proto.SyncRuleAdvert{
		{ID: "rule-a", AggregateBandwidthLimitMBps: 100},
		{ID: "rule-b", AggregateBandwidthLimitMBps: 0},
	}
	cache.rules.Store(&want)

	s := &SyncNode{snapshotCache: cache}
	got := s.Snapshot()

	if math.Abs(got.LastTaskFailureRate-0.42) > 1e-9 {
		t.Fatalf("LastTaskFailureRate = %v, want 0.42 (from cache)", got.LastTaskFailureRate)
	}
	if len(got.Rules) != len(want) {
		t.Fatalf("Rules length = %d, want %d (from cache)", len(got.Rules), len(want))
	}
	for i := range want {
		if got.Rules[i].ID != want[i].ID {
			t.Errorf("Rules[%d].ID = %q, want %q", i, got.Rules[i].ID, want[i].ID)
		}
		if got.Rules[i].AggregateBandwidthLimitMBps != want[i].AggregateBandwidthLimitMBps {
			t.Errorf("Rules[%d].Cap = %d, want %d",
				i, got.Rules[i].AggregateBandwidthLimitMBps, want[i].AggregateBandwidthLimitMBps)
		}
	}
}

// TestSnapshot_CacheMissReturnsZero confirms that pre-cache / bare-
// constructed *SyncNode (snapshotCache == nil) returns zero failure rate
// and nil Rules without panicking. Preserves the safe-default semantics
// the prior synchronous code path provided when stores were absent.
func TestSnapshot_CacheMissReturnsZero(t *testing.T) {
	s := &SyncNode{}
	got := s.Snapshot()
	if got.LastTaskFailureRate != 0 {
		t.Errorf("LastTaskFailureRate = %v, want 0 when cache is nil", got.LastTaskFailureRate)
	}
	if got.Rules != nil {
		t.Errorf("Rules = %v, want nil when cache is nil", got.Rules)
	}
}

// TestRefreshSnapshotCache_WritesValues verifies the refresh helper does
// the I/O against the underlying stores and lands the result in the
// cache atomic. Confirms the "cache writer" half of the pattern (the
// "cache reader" half is covered by TestSnapshot_UsesCachedValues).
func TestRefreshSnapshotCache_WritesValues(t *testing.T) {
	store := tasks.NewMemoryStore()
	ctx := context.Background()
	if err := store.Put(ctx, doneRecord("t1", executor.StatusFailed, -1*time.Minute)); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	if err := store.Put(ctx, doneRecord("t2", executor.StatusDone, -2*time.Minute)); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	s := &SyncNode{taskStore: store, snapshotCache: &snapshotCache{}}
	s.refreshSnapshotCache()

	resp := s.Snapshot()
	if math.Abs(resp.LastTaskFailureRate-0.5) > 1e-9 {
		t.Fatalf("after refresh LastTaskFailureRate = %v, want 0.5 (1 failed / 2 total)",
			resp.LastTaskFailureRate)
	}
	if s.snapshotCache.updatedAt.Load() == 0 {
		t.Error("updatedAt should be non-zero after refresh")
	}
}
