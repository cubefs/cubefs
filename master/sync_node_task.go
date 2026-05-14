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
	default:
		// TODO(B-2): OpSyncNodeRunTask / OpSyncNodeCancelTask handled in
		// later phases — log+ignore here keeps the wire path open.
		log.LogInfof("sn handleSyncNodeTaskResponse: unknown opcode %v, ignored", task.OpCode)
	}
}

// handleSyncNodeHeartbeatResp updates the SyncNode runtime snapshot. Same
// shape as Cluster.handleLcNodeHeartbeatResp.
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
	sn.Unlock()

	log.LogInfof("action[handleSyncNodeHeartbeatResp], syncNode[%v], running[%v], queued[%v], rules[%v], bolt[%v]",
		nodeAddr, resp.RunningTasks, resp.QueuedTasks, resp.ScheduledRules, resp.BoltDBHealthy)
	return
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
