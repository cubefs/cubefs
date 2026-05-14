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
	"testing"
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
}
