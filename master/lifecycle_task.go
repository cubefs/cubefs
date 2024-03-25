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
	"context"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func (c *Cluster) handleLcNodeTaskResponse(ctx context.Context, nodeAddr string, task *proto.AdminTask) {
	span := proto.SpanFromContext(ctx)
	if task == nil {
		span.Infof("lc action[handleLcNodeTaskResponse] receive addr[%v] task response, but task is nil", nodeAddr)
		return
	}
	span.Infof("lc action[handleLcNodeTaskResponse] receive addr[%v] task: %v", nodeAddr, task.ToString())
	var (
		err    error
		lcNode *LcNode
	)

	if lcNode, err = c.lcNode(nodeAddr); err != nil {
		goto errHandler
	}
	lcNode.TaskManager.DelTask(ctx, task)
	if err = unmarshalTaskResponse(ctx, task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpLcNodeHeartbeat:
		response := task.Response.(*proto.LcNodeHeartbeatResponse)
		err = c.handleLcNodeHeartbeatResp(ctx, task.OperatorAddr, response)
	case proto.OpLcNodeScan:
		response := task.Response.(*proto.LcNodeRuleTaskResponse)
		err = c.handleLcNodeLcScanResp(ctx, task.OperatorAddr, response)
	case proto.OpLcNodeSnapshotVerDel:
		response := task.Response.(*proto.SnapshotVerDelTaskResponse)
		err = c.handleLcNodeSnapshotScanResp(ctx, task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("lc unknown operate code %v", task.OpCode))
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	span.Warnf("lc handleLcNodeTaskResponse failed, task: %v, err: %v", task.ToString(), err)
}

func (c *Cluster) handleLcNodeHeartbeatResp(ctx context.Context, nodeAddr string, resp *proto.LcNodeHeartbeatResponse) (err error) {
	var lcNode *LcNode
	span := proto.SpanFromContext(ctx)
	span.Debugf("action[handleLcNodeHeartbeatResp] clusterID[%v] receive lcNode[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status != proto.TaskSucceeds {
		Warn(ctx, c.Name, fmt.Sprintf("action[handleLcNodeHeartbeatResp] clusterID[%v] lcNode[%v] heartbeat task failed, err[%v]",
			c.Name, nodeAddr, resp.Result))
		return
	}

	if lcNode, err = c.lcNode(nodeAddr); err != nil {
		span.Errorf("action[handleLcNodeHeartbeatResp], lcNode[%v], heartbeat error: %v", nodeAddr, err.Error())
		return
	}
	lcNode.Lock()
	lcNode.IsActive = true
	lcNode.ReportTime = time.Now()
	lcNode.Unlock()

	// update lcNodeStatus
	span.Infof("action[handleLcNodeHeartbeatResp], lcNode[%v], LcScanningTasks[%v], SnapshotScanningTasks[%v]", nodeAddr, len(resp.LcScanningTasks), len(resp.SnapshotScanningTasks))
	c.lcMgr.lcNodeStatus.UpdateNode(nodeAddr, len(resp.LcScanningTasks))
	c.snapshotMgr.lcNodeStatus.UpdateNode(nodeAddr, len(resp.SnapshotScanningTasks))

	// handle LcScanningTasks
	for _, taskRsp := range resp.LcScanningTasks {
		c.lcMgr.lcRuleTaskStatus.Lock()

		// avoid updating TaskResults incorrectly when received handleLcNodeLcScanResp first and then handleLcNodeHeartbeatResp
		if c.lcMgr.lcRuleTaskStatus.Results[taskRsp.ID] != nil && c.lcMgr.lcRuleTaskStatus.Results[taskRsp.ID].Done {
			span.Infof("action[handleLcNodeHeartbeatResp], lcNode[%v] task[%v] already done", nodeAddr, taskRsp.ID)
		} else {
			t := time.Now()
			taskRsp.UpdateTime = &t
			c.lcMgr.lcRuleTaskStatus.Results[taskRsp.ID] = taskRsp
		}

		c.lcMgr.lcRuleTaskStatus.Unlock()
		span.Debugf("action[handleLcNodeHeartbeatResp], lcNode[%v] taskRsp: %v", nodeAddr, taskRsp)
	}
	if len(resp.LcScanningTasks) < resp.LcTaskCountLimit {
		span.Infof("action[handleLcNodeHeartbeatResp], notify idle lcNode[%v], now LcScanningTasks[%v]", nodeAddr, len(resp.LcScanningTasks))
		c.lcMgr.notifyIdleLcNode(ctx)
	}

	// handle SnapshotScanningTasks
	for _, taskRsp := range resp.SnapshotScanningTasks {
		c.snapshotMgr.lcSnapshotTaskStatus.Lock()

		// avoid updating TaskResults incorrectly when received handleLcNodeLcScanResp first and then handleLcNodeHeartbeatResp
		if c.snapshotMgr.lcSnapshotTaskStatus.TaskResults[taskRsp.ID] != nil && c.snapshotMgr.lcSnapshotTaskStatus.TaskResults[taskRsp.ID].Done {
			span.Infof("action[handleLcNodeHeartbeatResp], lcNode[%v] snapshot task[%v] already done", nodeAddr, taskRsp.ID)
		} else {
			t := time.Now()
			taskRsp.UpdateTime = &t
			c.snapshotMgr.lcSnapshotTaskStatus.TaskResults[taskRsp.ID] = taskRsp
		}

		c.snapshotMgr.lcSnapshotTaskStatus.Unlock()
		span.Debugf("action[handleLcNodeHeartbeatResp], lcNode[%v] snapshot taskRsp: %v", nodeAddr, taskRsp)
	}
	if len(resp.SnapshotScanningTasks) < resp.LcTaskCountLimit {
		n := resp.LcTaskCountLimit - len(resp.SnapshotScanningTasks)
		span.Infof("action[handleLcNodeHeartbeatResp], notify idle lcNode[%v], now SnapshotScanningTasks[%v], notify times[%v]", nodeAddr, len(resp.SnapshotScanningTasks), n)
		for i := 0; i < n; i++ {
			c.snapshotMgr.notifyIdleLcNode(ctx)
		}
	}

	span.Infof("action[handleLcNodeHeartbeatResp], lcNode[%v], heartbeat success", nodeAddr)
	return
}

func (c *Cluster) handleLcNodeLcScanResp(ctx context.Context, nodeAddr string, resp *proto.LcNodeRuleTaskResponse) (err error) {
	span := proto.SpanFromContext(ctx)
	span.Debugf("action[handleLcNodeLcScanResp] lcNode[%v] task[%v] Enter", nodeAddr, resp.ID)
	defer func() {
		span.Debugf("action[handleLcNodeLcScanResp] lcNode[%v] task[%v] Exit", nodeAddr, resp.ID)
	}()

	switch resp.Status {
	case proto.TaskFailed:
		span.Warnf("action[handleLcNodeLcScanResp] scanning failed, resp(%v), no redo", resp)
		return
	case proto.TaskSucceeds:
		c.lcMgr.lcRuleTaskStatus.AddResult(resp)
		span.Infof("action[handleLcNodeLcScanResp] scanning completed, resp(%v)", resp)
		return
	default:
		span.Infof("action[handleLcNodeLcScanResp] scanning received, resp(%v)", resp)
	}

	return
}

func (c *Cluster) handleLcNodeSnapshotScanResp(ctx context.Context, nodeAddr string, resp *proto.SnapshotVerDelTaskResponse) (err error) {
	span := proto.SpanFromContext(ctx)
	span.Debugf("action[handleLcNodeSnapshotScanResp] lcNode[%v] task[%v] Enter", nodeAddr, resp.ID)
	defer func() {
		span.Debugf("action[handleLcNodeSnapshotScanResp] lcNode[%v] task[%v] Exit", nodeAddr, resp.ID)
	}()

	switch resp.Status {
	case proto.TaskFailed:
		c.snapshotMgr.lcSnapshotTaskStatus.RedoTask(resp.SnapshotVerDelTask)
		span.Errorf("action[handleLcNodeSnapshotScanResp] scanning failed, resp(%v), redo", resp)
		return
	case proto.TaskSucceeds:
		// 1.mark done for VersionMgr
		var vol *Vol
		vol, err = c.getVol(resp.VolName)
		if err != nil {
			span.Errorf("action[handleLcNodeSnapshotScanResp] snapshot task(%v) scanning completed by %v, results(%v), volume(%v) is not found",
				resp.ID, nodeAddr, resp, resp.VolName)
		} else {
			_ = vol.VersionMgr.DelVer(ctx, resp.VerSeq)
		}

		// 2. mark done for snapshotMgr
		c.snapshotMgr.lcSnapshotTaskStatus.AddResult(resp)
		span.Infof("action[handleLcNodeSnapshotScanResp] scanning completed, resp(%v)", resp)
		return
	default:
		span.Infof("action[handleLcNodeSnapshotScanResp] scanning received, resp(%v)", resp)
	}

	return
}
