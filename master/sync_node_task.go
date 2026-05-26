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
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// handleSyncNodeTaskResponse dispatches a syncnode admin-task response. It
// mirrors Cluster.handleLcNodeTaskResponse — locate the SyncNode by addr,
// drop the in-flight task from its sender, and update runtime state from
// the heartbeat snapshot. Other opcodes are reserved for B-3/B-4.
//
// Local response decode is used (instead of operate_util.unmarshalTaskResponse)
// to keep this change scoped to master/sync_node_*.go.
func (c *Cluster) handleSyncNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("sn action[handleSyncNodeTaskResponse] receive addr[%v] task response, but task is nil", nodeAddr)
		return
	}
	log.LogInfof("sn action[handleSyncNodeTaskResponse] receive addr[%v] task: %v", nodeAddr, task.ToString())

	sn, err := c.syncNode(nodeAddr)
	if err != nil {
		log.LogWarnf("sn handleSyncNodeTaskResponse: %v, err: %v", task.ToString(), err)
		return
	}
	sn.TaskManager.DelTask(task)

	switch task.OpCode {
	case proto.OpSyncNodeHeartbeat:
		resp := &proto.SyncNodeHeartbeatResponse{}
		if err = decodeTaskResponse(task, resp); err != nil {
			log.LogWarnf("sn handleSyncNodeTaskResponse decode failed: %v, err: %v", task.ToString(), err)
			return
		}
		if err = c.handleSyncNodeHeartbeatResp(nodeAddr, resp); err != nil {
			log.LogWarnf("sn handleSyncNodeHeartbeatResp failed: %v, err: %v", task.ToString(), err)
		}
	case proto.OpSyncNodeRunTask:
		// Terminal-status push-back from the syncnode (Bug #3 fix). The
		// run completed (done / failed / cancelled) on the owner; clear
		// the dispatcher's ownership entry + the failover orchestrator's
		// saved payload so the in-memory maps don't grow unbounded.
		rep, derr := decodeTerminalReport(task)
		if derr != nil {
			log.LogWarnf("sn handleSyncNodeTaskResponse decode terminal: %v, err: %v", task.ToString(), derr)
			return
		}
		if !isTerminalStatus(rep.Status) {
			// Non-terminal status (e.g. progress beacon repurposing the
			// same opcode in the future) — skip the release path.
			log.LogDebugf("sn handleSyncNodeTaskResponse: non-terminal status %q for task %q, skipping release",
				rep.Status, rep.TaskID)
			return
		}
		if c.syncDispatcher != nil {
			c.syncDispatcher.Release(rep.TaskID)
		}
		if c.syncFailover != nil {
			c.syncFailover.Forget(rep.TaskID)
		}
		// Eagerly decrement RunningTasks so the next Dispatch/Candidates call
		// sees a fresh count without waiting for the next heartbeat cycle.
		// The heartbeat will reset it to the authoritative value shortly after.
		sn.Lock()
		if sn.RunningTasks > 0 {
			sn.RunningTasks--
		}
		sn.Unlock()
		// Map executor status ("done"/"failed"/"cancelled") to ledger status.
		var masterStatus SyncTaskStatus
		switch rep.Status {
		case "done":
			masterStatus = SyncTaskStatusSucceeded
		case "failed":
			masterStatus = SyncTaskStatusFailed
		case "cancelled":
			masterStatus = SyncTaskStatusCancelled
		default:
			masterStatus = SyncTaskStatus(rep.Status)
		}
		prog := SyncTaskProgress{
			FilesTotal:   rep.Progress.FilesTotal,
			FilesDone:    rep.Progress.FilesDone,
			FilesSkipped: rep.Progress.FilesSkipped,
			FilesFailed:  rep.Progress.FilesFailed,
			BytesTotal:   rep.Progress.BytesTotal,
			BytesDone:    rep.Progress.BytesDone,
			BytesSkipped: rep.Progress.BytesSkipped,
		}
		// Write this task's (shard or single) terminal record BEFORE checking
		// fan-out completion, so aggregateFanoutShards sees all shard records.
		c.recordTaskTerminal(rep.TaskID, masterStatus, rep.Error, prog)
		log.LogInfof("sn task %s terminal on %s: status=%s err=%s",
			rep.TaskID, nodeAddr, rep.Status, rep.Error)
		// Update the bench task ledger with the terminal status so records
		// don't stay "running" forever. Handles three cases:
		//   1. Task completed with a bench result → Complete (stores metrics).
		//   2. Task cancelled without a result    → Cancel.
		//   3. Task failed without a result       → Fail (e.g. executor crash).
		// After updating the shard, CompleteShardAndAggregate checks whether
		// all sibling shards are done and rolls up the parent record if so.
		if c.benchTaskLedger != nil {
			if rep.BenchResult != nil {
				if raw, merr := json.Marshal(rep.BenchResult); merr == nil {
					var shardResult spec.BenchShardResult
					if merr = json.Unmarshal(raw, &shardResult); merr == nil {
						c.benchTaskLedger.Complete(rep.TaskID, shardResult)
					} else {
						log.LogWarnf("sn benchTaskLedger: unmarshal bench result for task %s: %v", rep.TaskID, merr)
						c.benchTaskLedger.Fail(rep.TaskID, "result decode error: "+merr.Error())
					}
				} else {
					log.LogWarnf("sn benchTaskLedger: marshal bench result for task %s: %v", rep.TaskID, merr)
					c.benchTaskLedger.Fail(rep.TaskID, "result marshal error: "+merr.Error())
				}
			} else {
				// Terminal without a bench result — executor failure or cancel.
				if rep.Status == "cancelled" {
					c.benchTaskLedger.Cancel(rep.TaskID)
				} else {
					errMsg := rep.Error
					if errMsg == "" {
						errMsg = "task ended without a bench result"
					}
					c.benchTaskLedger.Fail(rep.TaskID, errMsg)
				}
			}
			// For fan-out bench tasks, aggregate the shard into the parent.
			// No-op when the task ID is not a known bench shard record.
			parentID, allDone := c.benchTaskLedger.CompleteShardAndAggregate(rep.TaskID)
			if allDone {
				log.LogInfof("sn bench shard aggregate: parent %s fully done", parentID)
			}
			// SLA evaluation: run once the task reaches a terminal state.
			// Two cases:
			//   1. Single-shard task (parentID == "") — evaluate against
			//      this shard's BenchResult.Stages.
			//   2. Fan-out parent — only when allDone, aggregate every
			//      shard's stages worst-case before evaluating.
			// Tasks that never produced a BenchResult (executor crash,
			// cancel) have no metrics to score, so we still record a
			// failing SLAResult when the rule has SLA configured so the
			// dashboard surfaces "SLA could not be evaluated" rather than
			// hiding the field entirely.
			if parentID == "" {
				c.evaluateBenchSLAForSingle(rep.TaskID)
			} else if allDone {
				c.evaluateBenchSLAForParent(parentID)
			}
		}
		// Fan-out: when the terminal report carries a shard sub-task ID
		// ("<parent>/<N>"), mark it done and — once every shard reports —
		// aggregate into the parent record and release in-memory state.
		if c.syncFanout != nil {
			if parentID, shardIdx, isShard := splitSubTaskID(rep.TaskID); isShard {
				if allDone, exists := c.syncFanout.MarkShardTerminal(parentID, shardIdx); exists && allDone {
					parentStatus, parentProg := c.aggregateFanoutShards(parentID)
					c.recordTaskTerminal(parentID, parentStatus, "", parentProg)
					c.syncFanout.Clear(parentID)
					log.LogInfof("sn syncFanout: parent %s complete; status=%s", parentID, parentStatus)
					c.updateRuleLastRun(parentID, parentStatus, "")
				}
			} else {
				// Single (non-fan-out) task — write back rule's last run status.
				c.updateRuleLastRun(rep.TaskID, masterStatus, rep.Error)
			}
		} else {
			c.updateRuleLastRun(rep.TaskID, masterStatus, rep.Error)
		}
	default:
		log.LogInfof("sn handleSyncNodeTaskResponse: unknown opcode %v, ignored", task.OpCode)
	}
}

// handleSyncNodeHeartbeatResp updates the SyncNode runtime snapshot. Same
// shape as Cluster.handleLcNodeHeartbeatResp.
//
// S5 fix: when a node previously marked inactive (because checkSyncNodeHeartbeat
// observed a stale ReportTime and handleNodeDeath redispatched its tasks
// elsewhere) heartbeats again, naively setting IsActive=true leaves the
// reconnecting node running tasks it no longer owns — the dispatcher has
// re-homed them. The same task then runs on TWO nodes in parallel. We
// detect the wasActive=false → IsActive=true transition under SyncNode.Lock,
// capture the addr, release the lock, then push OpSyncNodeCancelTask for
// any task the dispatcher no longer maps to this addr. The cancel-fanout
// runs O(N) over the ownership ledger ONLY on the rare reconnect path; the
// happy heartbeat path stays O(1).
func (c *Cluster) handleSyncNodeHeartbeatResp(nodeAddr string, resp *proto.SyncNodeHeartbeatResponse) (err error) {
	log.LogDebugf("action[handleSyncNodeHeartbeatResp] clusterID[%v] receive syncNode[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleSyncNodeHeartbeatResp] clusterID[%v] syncNode[%v] heartbeat task failed, err[%v]",
			c.Name, nodeAddr, resp.Result))
		return
	}

	sn, err := c.syncNode(nodeAddr)
	if err != nil {
		log.LogErrorf("action[handleSyncNodeHeartbeatResp], syncNode[%v], heartbeat error: %v", nodeAddr, err.Error())
		return
	}
	sn.Lock()
	wasActive := sn.IsActive
	sn.IsActive = true
	sn.ReportTime = time.Now()
	sn.Version = resp.NodeVersion
	sn.UptimeSeconds = resp.UptimeSeconds
	sn.RunningTasks = resp.RunningTasks
	sn.QueuedTasks = resp.QueuedTasks
	sn.ScheduledRules = resp.ScheduledRules
	sn.BoltDBHealthy = resp.BoltDBHealthy
	sn.BandwidthMBps = resp.BandwidthMBps
	sn.CPUPercent = resp.CPUPercent
	sn.MemPercent = resp.MemPercent
	sn.MemTotalMB = resp.MemTotalMB
	sn.CPUCores = resp.CPUCores
	sn.ReloadFailures = resp.ReloadFailures
	sn.MaxConcurrentTasks = resp.MaxConcurrentTasks
	sn.BandwidthMBpsLimit = resp.BandwidthMBpsLimit
	sn.LastTaskFailureRate = resp.LastTaskFailureRate
	sn.MountPoints = resp.MountPoints
	addr := sn.Addr
	sn.Unlock()

	// S5: reconnect path. Master had marked this node dead and reassigned
	// its tasks. Send Cancel for anything we no longer own here so the
	// reconnecting node stops the orphaned local runs. Runs OUTSIDE the
	// SyncNode lock to respect the canonical lock ordering documented at
	// checkSyncNodeHeartbeat (SyncNode → SyncDispatcher).
	if !wasActive && c.syncDispatcher != nil {
		c.cancelOrphanedTasksOnReconnect(addr)
	}

	// FIX #4: ingest the syncnode's advertised per-rule
	// AggregateBandwidthLimitMBps caps so the SyncQuotaCalculator has
	// authoritative cluster ceilings. Operators set the same cap on every
	// node's sync.json via SIGHUP reload; master takes the most-recent
	// non-zero value as the truth. A zero value clears the cap (no cluster
	// limit on that rule).
	if c.syncQuota != nil {
		for _, ad := range resp.Rules {
			if ad.ID == "" {
				continue
			}
			c.syncQuota.SetRuleLimit(ad.ID, float64(ad.AggregateBandwidthLimitMBps))
		}
	}

	// Update in-flight task progress from heartbeat reports so the ledger
	// shows live progress without waiting for task terminal.
	if len(resp.TaskReports) > 0 && c.syncTaskLedger != nil {
		for _, report := range resp.TaskReports {
			if report.TaskID == "" {
				continue
			}
			c.syncTaskLedger.UpdateProgress(report.TaskID, SyncTaskProgress{
				FilesTotal:           report.Progress.FilesTotal,
				FilesDone:            report.Progress.FilesDone,
				FilesSkipped:         report.Progress.FilesSkipped,
				FilesFailed:          report.Progress.FilesFailed,
				BytesTotal:           report.Progress.BytesTotal,
				BytesDone:            report.Progress.BytesDone,
				BytesSkipped:         report.Progress.BytesSkipped,
				ThroughputMBps:       report.Progress.ThroughputMBps,
				CurrentBandwidthMBps: report.Progress.CurrentBandwidthMBps,
				SkippedSamples:       report.Progress.SkippedSamples,
			})
		}
	}

	// Heartbeat ledger reconcile (defect 2): cross-check ledger Running
	// records owned by this addr against the authoritative list the
	// syncnode just reported. Records that claim Owner=addr but are
	// absent from RunningTaskIDs are ledger drift — the syncnode crashed-
	// restarted, the task vanished, or master+syncnode disagree for some
	// other reason — and must be marked failed so the dashboard stops
	// showing them as running. orphan_scan also catches this case via the
	// 90s silence threshold; heartbeat reconcile narrows the detection
	// window to ~heartbeatReconcileGrace.
	c.reconcileLedgerFromHeartbeat(addr, resp.RunningTaskIDs)

	log.LogInfof("action[handleSyncNodeHeartbeatResp], syncNode[%v], running[%v], queued[%v], rules[%v], bolt[%v]",
		nodeAddr, resp.RunningTasks, resp.QueuedTasks, resp.ScheduledRules, resp.BoltDBHealthy)
	return
}

// heartbeatReconcileGrace is the minimum age (StartedAt → now) of a
// Running record before reconcileLedgerFromHeartbeat will fail it for
// being absent from the syncnode's reported RunningTaskIDs. The grace
// window absorbs the dispatch → queue → executor-pickup race: a task
// that was just dispatched has Owner=addr in the ledger but may not yet
// have surfaced in executor.RunningSnapshots() on the syncnode side.
// 30s is comfortably larger than any reasonable queue wait + executor
// startup latency, and far shorter than orphanShardSilenceThreshold (90s)
// — so reconcile catches drift faster than the orphan scan when the
// owner is still actively heartbeating.
const heartbeatReconcileGrace = 30 * time.Second

// reconcileLedgerFromHeartbeat enforces the invariant "every Running
// ledger record with Owner=addr must appear in addr's most-recent
// RunningTaskIDs heartbeat". Violations are marked failed. Empty input
// (syncnode reports no running tasks) means the syncnode is idle —
// every Running record on addr is reconcilable. Grace window applies
// per-record via StartedAt.
//
// Lock ordering: holds no SyncNode lock across ledger Fail calls; the
// only RW state touched is the per-ledger mutex (matches orphan_scan.go).
func (c *Cluster) reconcileLedgerFromHeartbeat(addr string, reportedIDs []string) {
	if c == nil || addr == "" {
		return
	}
	reported := make(map[string]struct{}, len(reportedIDs))
	for _, id := range reportedIDs {
		if id == "" {
			continue
		}
		reported[id] = struct{}{}
	}
	now := time.Now()

	// SyncTask side: scan Running records owned by addr.
	if c.syncTaskLedger != nil {
		for _, rec := range c.syncTaskLedger.ListByOwner(addr, SyncTaskStatusRunning) {
			if rec == nil {
				continue
			}
			if _, present := reported[rec.TaskID]; present {
				continue
			}
			if !rec.StartedAt.IsZero() && now.Sub(rec.StartedAt) < heartbeatReconcileGrace {
				continue
			}
			reason := fmt.Sprintf("owner %q heartbeat did not report task as running (StartedAt=%s)",
				addr, rec.StartedAt.Format(time.RFC3339))
			if c.syncTaskLedger.Fail(rec.TaskID, reason) {
				log.LogWarnf("reconcileLedgerFromHeartbeat: sync task %q on owner %q marked failed: %s",
					rec.TaskID, addr, reason)
			}
		}
	}

	// BenchTask side: BenchTaskLedger has no ListByOwner; iterate the
	// Running set and filter on Owner manually. Skip parent fan-out
	// records (Owner == "") — they roll up via shard aggregation only.
	if c.benchTaskLedger != nil {
		for _, rec := range c.benchTaskLedger.List("", string(BenchTaskStatusRunning)) {
			if rec == nil || rec.Owner != addr {
				continue
			}
			if _, present := reported[rec.TaskID]; present {
				continue
			}
			startedAt := time.UnixMilli(rec.CreatedAt)
			if !startedAt.IsZero() && now.Sub(startedAt) < heartbeatReconcileGrace {
				continue
			}
			reason := fmt.Sprintf("owner %q heartbeat did not report task as running (CreatedAt=%s)",
				addr, startedAt.Format(time.RFC3339))
			if !c.benchTaskLedger.Fail(rec.TaskID, reason) {
				continue
			}
			log.LogWarnf("reconcileLedgerFromHeartbeat: bench task %q on owner %q marked failed: %s",
				rec.TaskID, addr, reason)
			if rec.ParentTaskID != "" {
				c.benchTaskLedger.CompleteShardAndAggregate(rec.TaskID)
			}
		}
	}
}

// cancelOrphanedTasksOnReconnect detects tasks that the dispatcher's
// ownership ledger no longer associates with addr and pushes
// OpSyncNodeCancelTask to that node so it stops the local run. The
// dispatcher tracks owners; tasks whose owner is no longer addr (the
// dispatcher re-homed them to some other syncnode while addr was marked
// dead) are "orphans" from master's view.
//
// Cost: O(N) over the entire ownership ledger. Called only on the rare
// reconnect transition (wasActive=false → IsActive=true); never on the
// happy heartbeat path.
//
// Limitation: master cannot enumerate exactly which tasks the syncnode
// is actually running locally — the heartbeat snapshot reports a
// RunningTasks COUNT, not a set of IDs. As a conservative fallback we
// send Cancel for every taskID whose OwnerOf != addr (i.e. previously
// owned here, now owned elsewhere). The syncnode's handleCancelTask is
// a no-op for unknown taskIDs (Runner.Cancel doesn't error for missing
// IDs), so the over-broadcast is safe.
//
// Payload shape: we use a map[string]interface{}{"taskId": <id>} literal
// because syncnode.CancelTaskRequest lives in package syncnode and master
// must not depend on it. syncnode/task_handler.go:decodeCancelRequest
// round-trips the request body through JSON into the typed struct, so
// the map literal decodes identically to a typed CancelTaskRequest.
func (c *Cluster) cancelOrphanedTasksOnReconnect(addr string) {
	if c.syncDispatcher == nil {
		return
	}
	snI, ok := c.syncNodes.Load(addr)
	if !ok {
		return
	}
	sn, ok := snI.(*SyncNode)
	if !ok || sn == nil || sn.TaskManager == nil {
		return
	}
	ownerships := c.syncDispatcher.AllOwnerships()
	if len(ownerships) == 0 {
		log.LogDebugf("syncnode reconnect: %s rejoined with empty ownership ledger; no orphan-cancels needed", addr)
		return
	}
	cancelled := 0
	for taskID, owner := range ownerships {
		if owner == addr {
			continue
		}
		req := map[string]interface{}{"taskId": taskID}
		task := proto.NewAdminTaskEx(proto.OpSyncNodeCancelTask, addr, req, taskID)
		sn.TaskManager.AddTask(task)
		cancelled++
	}
	if cancelled > 0 {
		log.LogWarnf("syncnode reconnect: pushed %d orphan-cancel(s) to %s", cancelled, addr)
	} else {
		log.LogDebugf("syncnode reconnect: %s rejoined with no orphaned tasks; ledger had %d owned elsewhere", addr, 0)
	}
}

// decodeTaskResponse round-trips task.Response into the supplied typed
// pointer. The admin-task RPC framework already decoded the wire bytes
// into a generic map[string]interface{}; remarshal+unmarshal lifts it to
// the concrete type without touching operate_util.unmarshalTaskResponse.
func decodeTaskResponse(task *proto.AdminTask, out interface{}) error {
	bytes, err := json.Marshal(task.Response)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(bytes, out); err != nil {
		return err
	}
	task.Response = out
	return nil
}

// decodeTerminalReport peels a TaskTerminalReport out of an
// OpSyncNodeRunTask response envelope. The wire shape may arrive as a
// typed *proto.TaskTerminalReport (direct decode) OR as
// map[string]interface{} (post-RPC framework rehydration); we handle both
// via the shared decodeTaskResponse round-trip.
func decodeTerminalReport(task *proto.AdminTask) (*proto.TaskTerminalReport, error) {
	if task == nil {
		return nil, fmt.Errorf("nil admin task")
	}
	if task.Response == nil {
		return nil, fmt.Errorf("admin task carries no Response payload")
	}
	if rep, ok := task.Response.(*proto.TaskTerminalReport); ok && rep != nil {
		return rep, nil
	}
	rep := &proto.TaskTerminalReport{}
	if err := decodeTaskResponse(task, rep); err != nil {
		return nil, err
	}
	// Falls back to the task ID when the inner field is empty — the
	// outer AdminTask.ID is set by the syncnode push-back path to the
	// same value, so this preserves the dispatcher Release key even if
	// a downstream message-shape change leaves rep.TaskID blank.
	if rep.TaskID == "" {
		rep.TaskID = task.ID
	}
	return rep, nil
}

// isTerminalStatus reports whether status string is one of the executor's
// terminal sentinel values ("done" / "failed" / "cancelled"). Mirrors
// syncnode/executor.Status string constants without taking a build-graph
// dependency on the syncnode package from master.
func isTerminalStatus(status string) bool {
	switch status {
	case "done", "failed", "cancelled":
		return true
	default:
		return false
	}
}

// aggregateFanoutShards iterates every shard record of parentID in the task
// ledger, aggregates progress, and derives the parent's terminal status
// (failed if any shard failed, cancelled if any shard cancelled, succeeded
// otherwise). Called after all shards report terminal so every shard record
// is guaranteed to be written before we query.
func (c *Cluster) aggregateFanoutShards(parentID string) (SyncTaskStatus, SyncTaskProgress) {
	parent := c.syncTaskLedger.Get(parentID)
	if parent == nil {
		return SyncTaskStatusSucceeded, SyncTaskProgress{}
	}
	var prog SyncTaskProgress
	anyFailed, anyCancelled := false, false
	for i := 0; i < parent.ShardTotal; i++ {
		subID := fmt.Sprintf("%s/%d", parentID, i)
		rec := c.syncTaskLedger.Get(subID)
		if rec == nil {
			continue
		}
		switch rec.Status {
		case SyncTaskStatusFailed:
			anyFailed = true
		case SyncTaskStatusCancelled:
			anyCancelled = true
		}
		prog.FilesTotal += rec.Progress.FilesTotal
		prog.FilesDone += rec.Progress.FilesDone
		prog.FilesSkipped += rec.Progress.FilesSkipped
		prog.FilesFailed += rec.Progress.FilesFailed
		prog.BytesTotal += rec.Progress.BytesTotal
		prog.BytesDone += rec.Progress.BytesDone
		prog.BytesSkipped += rec.Progress.BytesSkipped
	}
	status := SyncTaskStatusSucceeded
	if anyFailed {
		status = SyncTaskStatusFailed
	} else if anyCancelled {
		status = SyncTaskStatusCancelled
	}
	return status, prog
}

// updateRuleLastRun writes back the terminal status of a root task (single or
// fan-out parent) to the rule cache and persists via raft. rootTaskID is the
// parent/single task ID (format "<ruleID>/<nanoseconds>"); ruleID is derived
// by stripping the trailing timestamp suffix. No-op when the rule is evicted.
func (c *Cluster) updateRuleLastRun(rootTaskID string, status SyncTaskStatus, errMsg string) {
	if c == nil || c.syncRuleCache == nil {
		return
	}
	ruleID := stripShardSuffix(rootTaskID)
	if ruleID == rootTaskID || ruleID == "" {
		return
	}
	rule := c.syncRuleCache.Get(ruleID)
	if rule == nil {
		return
	}
	updated := *rule
	updated.LastRunAt = time.Now()
	switch status {
	case SyncTaskStatusSucceeded:
		updated.LastRunStatus = "succeeded"
		updated.LastRunError = ""
	case SyncTaskStatusFailed:
		updated.LastRunStatus = "failed"
		updated.LastRunError = errMsg
	case SyncTaskStatusCancelled:
		updated.LastRunStatus = "cancelled"
		updated.LastRunError = ""
	default:
		updated.LastRunStatus = string(status)
	}
	c.syncRuleCache.Put(&updated)
	if rerr := c.syncUpdateSyncRule(&updated); rerr != nil {
		log.LogWarnf("updateRuleLastRun: raft persist rule=%q: %v", ruleID, rerr)
	}
}
