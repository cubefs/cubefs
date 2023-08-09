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
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) handleLcNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("lc action[handleLcNodeTaskResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	log.LogInfof("lc action[handleLcNodeTaskResponse] receive addr[%v] task:%v", nodeAddr, task.ToString())
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
		bytes, err := json.Marshal(task.Request)
		if err != nil {
			log.LogErrorf("lc action[handleLcNodeTaskResponse] Marshal task request failed!")
			return
		}
		var request interface{}
		request = &proto.LcNodeRuleTaskRequest{}
		if err = json.Unmarshal(bytes, request); err != nil {
			log.LogErrorf("lc action[handleLcNodeTaskResponse] Unmarshal task request failed!")
			return
		}
		task.Request = request

		req := task.Request.(*proto.LcNodeRuleTaskRequest)
		response := task.Response.(*proto.LcNodeRuleTaskResponse)
		err = c.handleLcNodeLcScanResp(req, response)
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
	log.LogErrorf("lc handleLcNodeTaskResponse failed, task: %v, err: %v", task.ToString(), err)
	return
}

func (c *Cluster) handleLcNodeLcScanResp(req *proto.LcNodeRuleTaskRequest, resp *proto.LcNodeRuleTaskResponse) (err error) {
	log.LogDebugf("action[handleLcNodeLcScanResp] Rule(%v) in task(%v) Enter", req.Task.Rule.ID, req.Task.Id)
	defer func() {
		log.LogDebugf("action[handleLcNodeLcScanResp] Rule(%v) in task(%v) Exit", req.Task.Rule.ID, req.Task.Id)
	}()

	c.lcMgr.lcNodeStatus.releaseNode(req.LcNodeAddr)

	if resp.Status == proto.TaskFailed {
		c.lcMgr.lcRuleTaskStatus.redoTask(req.Task)
	} else {
		c.lcMgr.lcRuleTaskStatus.finishTask(req.Task, resp)
	}

	log.LogInfof("action[handleLcNodeLcScanResp] task(%v) scanning completed, current rule(%v), vol(%v), results(%v).",
		req.Task.Id, req.Task.Rule.ID, req.Task.VolName, resp)
	return
}

func (c *Cluster) handleLcNodeHeartbeatResp(nodeAddr string, resp *proto.LcNodeHeartbeatResponse) (err error) {
	var (
		lcNode *LcNode
		logMsg string
	)

	log.LogDebugf("action[handleLcNodeHeartbeatResp] clusterID[%v] receive lcNode[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleLcNodeHeartbeatResp] clusterID[%v] lcNode[%v] heartbeat task failed, err[%v]",
			c.Name, nodeAddr, resp.Result))
		return
	}

	if lcNode, err = c.lcNode(nodeAddr); err != nil {
		goto errHandler
	}
	lcNode.Lock()
	lcNode.IsActive = true
	lcNode.ReportTime = time.Now()
	lcNode.Unlock()

	//handle LcScanningTasks
	if len(resp.LcScanningTasks) != 0 {
		for _, taskRsp := range resp.LcScanningTasks {
			c.lcMgr.lcRuleTaskStatus.Lock()
			c.lcMgr.lcRuleTaskStatus.Results[taskRsp.ID] = taskRsp
			c.lcMgr.lcRuleTaskStatus.Unlock()
			log.LogDebugf("action[handleLcNodeHeartbeatResp], lcNode(%v) scanning LcScanningTasks rsp(%v)", nodeAddr, taskRsp)
		}
	} else {
		log.LogDebugf("action[handleLcNodeHeartbeatResp], lcNode[%v] is idle for LcScanningTasks", nodeAddr)
		task := c.lcMgr.lcNodeStatus.releaseNode(nodeAddr)
		if task != nil {
			c.lcMgr.lcRuleTaskStatus.deleteScanningTask(task.(*proto.RuleTask))
		}
		c.lcMgr.notifyIdleLcNode()
	}

	//handle SnapshotScanningTasks
	if len(resp.SnapshotScanningTasks) != 0 {
		for _, taskRsp := range resp.SnapshotScanningTasks {
			c.snapshotMgr.volVerInfos.Lock()
			c.snapshotMgr.volVerInfos.TaskResults[taskRsp.ID] = taskRsp

			if pInfo, ok := c.snapshotMgr.volVerInfos.ProcessingVerInfos[taskRsp.ID]; ok {
				pInfo.updateTime = time.Now()
				log.LogDebugf("action[handleLcNodeHeartbeatResp], snapshot scan taskid(%v) update time",
					taskRsp.ID)
			} else {
				//in case of master restart or leader changing, resume processing status
				if vol, err := c.getVol(taskRsp.VolName); err == nil {
					volVerInfoList := vol.VersionMgr.getVersionList()
					for _, volVerInfo := range volVerInfoList.VerList {
						if volVerInfo.Ver == taskRsp.VerSeq && volVerInfo.Status == proto.VersionDeleting {
							pInfo := &ProcessingVerInfo{
								LcVerInfo: LcVerInfo{
									VerInfo: proto.VerInfo{
										VolName: taskRsp.VolName,
										VerSeq:  taskRsp.VerSeq,
									},
									dTime: time.Unix(volVerInfo.DelTime, 0),
								},
								updateTime: time.Now(),
							}
							c.snapshotMgr.volVerInfos.ProcessingVerInfos[taskRsp.ID] = pInfo
							log.LogInfof("action[handleLcNodeHeartbeatResp], task (%v)  processing status is resumed",
								taskRsp.ID)
						}
					}
				} else {
					log.LogErrorf("action[handleLcNodeHeartbeatResp], snapshot scan vol(%v) not found", taskRsp.VolName)
				}
			}
			c.snapshotMgr.volVerInfos.Unlock()
			log.LogDebugf("action[handleLcNodeHeartbeatResp], SnapshotScanningTasks rsp(%v)", taskRsp)
		}
	} else {
		log.LogDebugf("action[handleLcNodeHeartbeatResp], lcNode[%v] is idle for SnapshotScanningTasks", nodeAddr)
		task := c.snapshotMgr.lcNodeStatus.releaseNode(nodeAddr)
		if task != nil {
			c.snapshotMgr.volVerInfos.RemoveProcessingVerInfo(task.(string))
		}
		c.snapshotMgr.notifyIdleLcNode()
	}

	logMsg = fmt.Sprintf("action[handleLcNodeHeartbeatResp], lcNode:%v, ReportTime:%v success", lcNode.Addr, lcNode.ReportTime.Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

func (c *Cluster) handleLcNodeSnapshotScanResp(nodeAddr string, resp *proto.SnapshotVerDelTaskResponse) (err error) {
	log.LogDebugf("action[handleLcNodeSnapshotScanResp] lcnode(%v), resp id(%v) Enter", nodeAddr, resp.ID)
	defer func() {
		log.LogDebugf("action[handleLcNodeSnapshotScanResp] lcnode(%v), resp id(%v) Exit", nodeAddr, resp.ID)
	}()

	c.snapshotMgr.lcNodeStatus.releaseNode(nodeAddr)

	if resp.Status == proto.TaskFailed {
		c.snapshotMgr.volVerInfos.RedoProcessingVerInfo(resp.ID)
	} else {
		//1.mark done for VersionMgr
		var vol *Vol
		vol, err = c.getVol(resp.VolName)
		if err != nil {
			log.LogErrorf("action[handleLcNodeSnapshotScanResp] snapshot task(%v) scanning completed by %v, results(%v), volume(%v) is not found",
				resp.ID, nodeAddr, resp, resp.VolName)
			return
		} else {
			_ = vol.VersionMgr.DelVer(resp.VerSeq)
		}

		//2. mark done for snapshotMgr
		c.snapshotMgr.volVerInfos.RemoveProcessingVerInfo(resp.ID)
	}

	log.LogInfof("action[handleLcNodeSnapshotScanResp] scanning completed, resp(%v)", resp)
	return
}
