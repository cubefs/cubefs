// Copyright 2023 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) handleLcNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("lc action[handleLcNodeTaskResponse] receive addr[%v] task response, but task is nil", nodeAddr)
		return
	}
	log.LogInfof("lc action[handleLcNodeTaskResponse] receive addr[%v] task: %v", nodeAddr, task.ToString())
	var (
		err    error
		lcNode *LcNode
	)

	if lcNode, err = c.lcNode(nodeAddr); err != nil {
		goto errHandler
	}
	lcNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpLcNodeHeartbeat:
		response := task.Response.(*proto.LcNodeHeartbeatResponse)
		err = c.handleLcNodeHeartbeatResp(task.OperatorAddr, response)
	case proto.OpLcNodeScan:
		response := task.Response.(*proto.LcNodeRuleTaskResponse)
		err = c.handleLcNodeLcScanResp(task.OperatorAddr, response)
	case proto.OpLcNodeSnapshotVerDel:
		response := task.Response.(*proto.SnapshotVerDelTaskResponse)
		err = c.handleLcNodeSnapshotScanResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("lc unknown operate code %v", task.OpCode))
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogWarnf("lc handleLcNodeTaskResponse failed, task: %v, err: %v", task.ToString(), err)
	return
}

func (c *Cluster) handleLcNodeHeartbeatResp(nodeAddr string, resp *proto.LcNodeHeartbeatResponse) (err error) {
	var lcNode *LcNode

	log.LogDebugf("action[handleLcNodeHeartbeatResp] clusterID[%v] receive lcNode[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleLcNodeHeartbeatResp] clusterID[%v] lcNode[%v] heartbeat task failed, err[%v]",
			c.Name, nodeAddr, resp.Result))
		return
	}

	if lcNode, err = c.lcNode(nodeAddr); err != nil {
		log.LogErrorf("action[handleLcNodeHeartbeatResp], lcNode[%v], heartbeat error: %v", nodeAddr, err.Error())
		return
	}
	lcNode.Lock()
	lcNode.IsActive = true
	lcNode.ReportTime = time.Now()
	lcNode.Unlock()

	// update lcNodeStatus
	log.LogInfof("action[handleLcNodeHeartbeatResp], lcNode[%v], LcScanningTasks[%v], SnapshotScanningTasks[%v]", nodeAddr, len(resp.LcScanningTasks), len(resp.SnapshotScanningTasks))
	c.lcMgr.lcNodeStatus.UpdateNode(nodeAddr, len(resp.LcScanningTasks))
	c.snapshotMgr.lcNodeStatus.UpdateNode(nodeAddr, len(resp.SnapshotScanningTasks))

	// handle LcScanningTasks
	for _, taskRsp := range resp.LcScanningTasks {
		c.lcMgr.lcRuleTaskStatus.Lock()

		// avoid updating TaskResults incorrectly when received handleLcNodeLcScanResp first and then handleLcNodeHeartbeatResp
		if c.lcMgr.lcRuleTaskStatus.Results[taskRsp.ID] != nil && c.lcMgr.lcRuleTaskStatus.Results[taskRsp.ID].Done {
			log.LogInfof("action[handleLcNodeHeartbeatResp], lcNode[%v] task[%v] already done", nodeAddr, taskRsp.ID)
		} else {
			t := time.Now()
			taskRsp.UpdateTime = &t
			c.lcMgr.lcRuleTaskStatus.Results[taskRsp.ID] = taskRsp
		}

		c.lcMgr.lcRuleTaskStatus.Unlock()
		log.LogDebugf("action[handleLcNodeHeartbeatResp], lcNode[%v] taskRsp: %v", nodeAddr, taskRsp)
	}
	if len(resp.LcScanningTasks) < resp.LcTaskCountLimit {
		log.LogInfof("action[handleLcNodeHeartbeatResp], notify idle lcNode[%v], now LcScanningTasks[%v]", nodeAddr, len(resp.LcScanningTasks))
		c.lcMgr.notifyIdleLcNode()
	}

	// handle SnapshotScanningTasks
	for _, taskRsp := range resp.SnapshotScanningTasks {
		c.snapshotMgr.lcSnapshotTaskStatus.Lock()

		// avoid updating TaskResults incorrectly when received handleLcNodeLcScanResp first and then handleLcNodeHeartbeatResp
		if c.snapshotMgr.lcSnapshotTaskStatus.TaskResults[taskRsp.ID] != nil && c.snapshotMgr.lcSnapshotTaskStatus.TaskResults[taskRsp.ID].Done {
			log.LogInfof("action[handleLcNodeHeartbeatResp], lcNode[%v] snapshot task[%v] already done", nodeAddr, taskRsp.ID)
		} else {
			t := time.Now()
			taskRsp.UpdateTime = &t
			c.snapshotMgr.lcSnapshotTaskStatus.TaskResults[taskRsp.ID] = taskRsp
		}

		c.snapshotMgr.lcSnapshotTaskStatus.Unlock()
		log.LogDebugf("action[handleLcNodeHeartbeatResp], lcNode[%v] snapshot taskRsp: %v", nodeAddr, taskRsp)
	}
	if len(resp.SnapshotScanningTasks) < resp.LcTaskCountLimit {
		n := resp.LcTaskCountLimit - len(resp.SnapshotScanningTasks)
		log.LogInfof("action[handleLcNodeHeartbeatResp], notify idle lcNode[%v], now SnapshotScanningTasks[%v], notify times[%v]", nodeAddr, len(resp.SnapshotScanningTasks), n)
		for i := 0; i < n; i++ {
			c.snapshotMgr.notifyIdleLcNode()
		}
	}

	log.LogInfof("action[handleLcNodeHeartbeatResp], lcNode[%v], heartbeat success", nodeAddr)
	return
}

func (c *Cluster) handleLcNodeLcScanResp(nodeAddr string, resp *proto.LcNodeRuleTaskResponse) (err error) {
	log.LogDebugf("action[handleLcNodeLcScanResp] lcNode[%v] task[%v] Enter", nodeAddr, resp.ID)
	defer func() {
		log.LogDebugf("action[handleLcNodeLcScanResp] lcNode[%v] task[%v] Exit", nodeAddr, resp.ID)
	}()

	switch resp.Status {
	case proto.TaskFailed:
		log.LogWarnf("action[handleLcNodeLcScanResp] scanning failed, resp(%v), no redo", resp)
		return
	case proto.TaskSucceeds:
		c.lcMgr.lcRuleTaskStatus.AddResult(resp)
		log.LogInfof("action[handleLcNodeLcScanResp] scanning completed, resp(%v)", resp)
		return
	default:
		log.LogInfof("action[handleLcNodeLcScanResp] scanning received, resp(%v)", resp)
	}

	return
}

func (c *Cluster) handleLcNodeSnapshotScanResp(nodeAddr string, resp *proto.SnapshotVerDelTaskResponse) (err error) {
	log.LogDebugf("action[handleLcNodeSnapshotScanResp] lcNode[%v] task[%v] Enter", nodeAddr, resp.ID)
	defer func() {
		log.LogDebugf("action[handleLcNodeSnapshotScanResp] lcNode[%v] task[%v] Exit", nodeAddr, resp.ID)
	}()

	switch resp.Status {
	case proto.TaskFailed:
		c.snapshotMgr.lcSnapshotTaskStatus.RedoTask(resp.SnapshotVerDelTask)
		log.LogErrorf("action[handleLcNodeSnapshotScanResp] scanning failed, resp(%v), redo", resp)
		return
	case proto.TaskSucceeds:
		// 1.mark done for VersionMgr
		var vol *Vol
		vol, err = c.getVol(resp.VolName)
		if err != nil {
			log.LogErrorf("action[handleLcNodeSnapshotScanResp] snapshot task(%v) scanning completed by %v, results(%v), volume(%v) is not found",
				resp.ID, nodeAddr, resp, resp.VolName)
		} else {
			_ = vol.VersionMgr.DelVer(resp.VerSeq)
		}

		// 2. mark done for snapshotMgr
		c.snapshotMgr.lcSnapshotTaskStatus.AddResult(resp)
		log.LogInfof("action[handleLcNodeSnapshotScanResp] scanning completed, resp(%v)", resp)
		return
	default:
		log.LogInfof("action[handleLcNodeSnapshotScanResp] scanning received, resp(%v)", resp)
	}

	return
}
