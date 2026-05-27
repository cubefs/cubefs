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

package master

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

// orphanShardSilenceThreshold is how long a Running task may go without a
// heartbeat-driven UpdatedAt refresh before the orphan scan considers it
// dead. Syncnode heartbeats fire every 10s and carry per-task progress on
// each tick, so 90s = 3 × interval. Comfortably survives one missed tick
// + cluster-side retry jitter, but small enough that a wedged shard
// doesn't keep the dashboard "running" forever after the owner pod
// disappears.
const orphanShardSilenceThreshold = 90 * time.Second

// checkOrphanShards walks the two task ledgers (SyncTaskLedger,
// BenchTaskLedger) and marks each Running record as failed when the
// owning syncnode is no longer alive OR the record has been silent for
// longer than orphanShardSilenceThreshold. Called from the heartbeat
// tick (checkSyncNodeHeartbeat) so the scan runs at the same cadence as
// node liveness flips. Per the design (docs/plan/syncnode/orphan-reclaim.md),
// orphan tasks are NOT redispatched — failing them releases dashboard
// state so the operator can retry. Lock ordering: we hold no SyncNode
// lock across the ledger Fail calls; the only RW state touched is the
// per-ledger mutex.
func (c *Cluster) checkOrphanShards() {
	if c == nil {
		return
	}
	now := time.Now()

	// SyncTask side: iterate over Running records (List with status filter).
	if c.syncTaskLedger != nil {
		for _, rec := range c.syncTaskLedger.List(SyncTaskStatusRunning, "", "") {
			if rec == nil || rec.Owner == "" {
				continue
			}
			reason := c.classifyOrphan(rec.Owner, rec.UpdatedAt, rec.StartedAt, now)
			if reason == "" {
				continue
			}
			if c.syncTaskLedger.Fail(rec.TaskID, reason) {
				log.LogWarnf("checkOrphanShards: sync task %q on owner %q marked failed: %s",
					rec.TaskID, rec.Owner, reason)
			}
		}
	}

	// BenchTask side: iterate over Running records. Shards roll up into
	// their parent via CompleteShardAndAggregate after Fail so the parent
	// reaches its terminal aggregate state.
	if c.benchTaskLedger != nil {
		for _, rec := range c.benchTaskLedger.List("", string(BenchTaskStatusRunning)) {
			if rec == nil || rec.Owner == "" {
				// Parent fan-out records have empty Owner and do not run on
				// a single node — they are rolled up by shard aggregation.
				continue
			}
			updatedAt := time.UnixMilli(rec.UpdatedAt)
			startedAt := time.UnixMilli(rec.CreatedAt)
			reason := c.classifyOrphan(rec.Owner, updatedAt, startedAt, now)
			if reason == "" {
				continue
			}
			if !c.benchTaskLedger.Fail(rec.TaskID, reason) {
				continue
			}
			log.LogWarnf("checkOrphanShards: bench task %q on owner %q marked failed: %s",
				rec.TaskID, rec.Owner, reason)
			if rec.ParentTaskID != "" {
				// Roll the shard's terminal state up into the parent. The
				// aggregator picks up Status from the freshly-failed shard
				// record and folds it into ShardsDone / parent Status.
				c.benchTaskLedger.CompleteShardAndAggregate(rec.TaskID)
			}
		}
	}
}

// classifyOrphan returns a non-empty reason string when (owner, updatedAt)
// indicates the task should be considered orphaned. Empty return → "still
// healthy, do not touch". The owner is orphaned when:
//   - the addr is no longer present in c.syncNodes (operator decommissioned
//     the node), OR
//   - the syncnode entry exists but IsActive=false (heartbeat timed out;
//     checkSyncNodeHeartbeat just flipped it), OR
//   - UpdatedAt has been silent for longer than orphanShardSilenceThreshold
//     even though the owner still appears active (covers wedged executors
//     that stopped reporting progress).
//
// updatedAt zero is treated as startedAt for the silence math (a record
// that was inserted but never updated still counts as "silent since insert").
func (c *Cluster) classifyOrphan(owner string, updatedAt, startedAt, now time.Time) string {
	if owner == "" {
		return ""
	}
	silenceRef := updatedAt
	if silenceRef.IsZero() {
		silenceRef = startedAt
	}
	silence := time.Duration(0)
	if !silenceRef.IsZero() {
		silence = now.Sub(silenceRef)
	}
	v, ok := c.syncNodes.Load(owner)
	if !ok {
		return fmt.Sprintf("owner %q no longer registered (silent for %s)", owner, silence.Truncate(time.Second))
	}
	sn, ok := v.(*SyncNode)
	if !ok || sn == nil {
		return fmt.Sprintf("owner %q registry entry malformed", owner)
	}
	sn.RLock()
	active := sn.IsActive
	sn.RUnlock()
	if !active {
		return fmt.Sprintf("owner %q inactive (silent for %s)", owner, silence.Truncate(time.Second))
	}
	if silence > orphanShardSilenceThreshold {
		return fmt.Sprintf("owner %q still active but task silent for %s (> %s threshold)",
			owner, silence.Truncate(time.Second), orphanShardSilenceThreshold)
	}
	return ""
}
