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
	"time"

	"github.com/cubefs/cubefs/proto"
)

// Snapshot satisfies HeartbeatSnapshotProvider. The MasterClient calls it
// once per heartbeat tick — the implementation MUST be cheap (no I/O on
// the hot path). Status / Result / NodeID / Addr / NodeVersion are filled
// by the client; we only fill the dynamic gauges.
func (s *SyncNode) Snapshot() proto.SyncNodeHeartbeatResponse {
	resp := proto.SyncNodeHeartbeatResponse{
		UptimeSeconds:  int64(time.Since(startedAt).Seconds()),
		ReloadFailures: reloadFailuresTotal.Load(),
	}
	if s.executor != nil {
		resp.RunningTasks = int64(s.executor.RunningCount())
	}
	if s.scheduler != nil {
		resp.ScheduledRules = s.scheduler.RegisteredCount()
	}
	if s.boltDB != nil {
		resp.BoltDBHealthy = s.boltDB.Health() == nil
	}
	return resp
}
