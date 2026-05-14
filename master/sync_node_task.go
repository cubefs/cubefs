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
		// Bug S2 fix: when the terminal report is for a fan-out sub-task
		// ("<parent>/<shard>"), mark the shard terminal on the fanout and
		// clear the parent record once every shard has reported. Without
		// this, SyncFanout.parents grows monotonically for the lifetime
		// of the master process.
		if c.syncFanout != nil {
			if parentID, shardIdx, isShard := splitSubTaskID(rep.TaskID); isShard {
				if allDone, exists := c.syncFanout.MarkShardTerminal(parentID, shardIdx); exists && allDone {
					c.syncFanout.Clear(parentID)
					log.LogInfof("sn syncFanout: parent %s complete; cleared after %d/%d shards terminal",
						parentID, shardIdx+1, shardIdx+1)
				}
			}
		}
		// Update the task ledger so /syncTask/get returns the terminal status.
		// Executor uses "done"/"failed"/"cancelled"; master ledger uses
		// "succeeded"/"failed"/"cancelled" — map accordingly.
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
		// TaskTerminalReport carries no progress; preserve the last snapshot.
		var prog SyncTaskProgress
		if existing := c.syncTaskLedger.Get(rep.TaskID); existing != nil {
			prog = existing.Progress
		}
		c.recordTaskTerminal(rep.TaskID, masterStatus, rep.Error, prog)
		log.LogInfof("sn task %s terminal on %s: status=%s err=%s",
			rep.TaskID, nodeAddr, rep.Status, rep.Error)
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
	sn.ReloadFailures = resp.ReloadFailures
	sn.MaxConcurrentTasks = resp.MaxConcurrentTasks
	sn.BandwidthMBpsLimit = resp.BandwidthMBpsLimit
	sn.LastTaskFailureRate = resp.LastTaskFailureRate
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

	log.LogInfof("action[handleSyncNodeHeartbeatResp], syncNode[%v], running[%v], queued[%v], rules[%v], bolt[%v]",
		nodeAddr, resp.RunningTasks, resp.QueuedTasks, resp.ScheduledRules, resp.BoltDBHealthy)
	return
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
		task := proto.NewAdminTask(proto.OpSyncNodeCancelTask, addr, req)
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
