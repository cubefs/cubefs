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
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/remotecache/flashgroupmanager"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

func (c *Cluster) addFlashNodeHeartbeatTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.peekFlashNode(t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		node.TaskManager.AddTask(t)
	}
}

func (c *Cluster) syncFlashNodeSetIOLimitTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.peekFlashNode(t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		if _, err = node.SyncSendAdminTask(t); err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
	}
}

func (c *Cluster) handleManualTaskProcessing(flashNode *flashgroupmanager.FlashNode, resp *proto.FlashNodeHeartbeatResponse) {
	for _, taskRsp := range resp.ManualScanningTasks {
		manualTask, ok := c.flashManMgr.LoadManualTaskById(taskRsp.ID)
		if !ok {
			continue
		}
		log.LogDebugf("action[handleFlashNodeHeartbeatResp], get manger rlock for task[%v] and  taskRsp[%v]", manualTask, taskRsp)
		// avoid updating TaskResults incorrectly when received handleFlashNodeHeartbeatResp first and then handleFlashNodeHeartbeatResp
		manualTask.Lock()
		if proto.ManualTaskDone(manualTask.Status) {
			log.LogInfof("action[handleFlashNodeHeartbeatResp], flashNode[%v] task[%v] already done", flashNode.Addr, taskRsp.ID)
		} else {
			manualTask.SetResponse(taskRsp)
		}
		manualTask.Unlock()
		log.LogDebugf("action[handleFlashNodeHeartbeatResp], flashNode[%v] taskRsp: %v", flashNode.Addr, taskRsp)
	}

	c.flashManMgr.flashNodeTaskStatus.mu.Lock()
	c.flashManMgr.flashNodeTaskStatus.WorkingCount[flashNode.Addr] = len(resp.ManualScanningTasks)
	c.flashManMgr.flashNodeTaskStatus.mu.Unlock()
	log.LogInfof("action[handleFlashNodeHeartbeatResp], flashNode[%v], heartbeat success", flashNode.Addr)
}

func (c *Cluster) checkFlashNodeHeartbeat() {
	tasks := c.flashNodeTopo.CreateFlashNodeHeartBeatTasks(c.masterAddr(), c.cfg.flashNodeHandleReadTimeout,
		c.cfg.flashNodeReadDataNodeTimeout, c.cfg.flashHotKeyMissCount, c.cfg.flashReadFlowLimit, c.cfg.flashWriteFlowLimit)
	c.addFlashNodeHeartbeatTasks(tasks)
}

func (m *Server) addFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr common.String
		zoneName common.String
		version  common.String
		id       uint64
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeAdd))
	defer func() {
		doStatAndMetric(proto.FlashNodeAdd, metric, err, nil)
	}()
	if err = parseArgs(r, argParserNodeAddr(&nodeAddr),
		zoneName.ZoneName().OmitEmpty().OnValue(func() error {
			if zoneName.V == "" {
				zoneName.V = DefaultZoneName
			}
			return nil
		}),
		version.Key("version").OmitEmpty(),
	); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if id, err = m.cluster.addFlashNode(nodeAddr.V, zoneName.V, version.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (c *Cluster) addFlashNode(nodeAddr, zoneName, version string) (id uint64, err error) {
	return c.flashNodeTopo.AddFlashNode(c.Name, nodeAddr, zoneName, version,
		c.idAlloc.allocateCommonID, c.syncAddFlashNode)
}

func (m *Server) listFlashNodes(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeList))
	defer func() {
		doStatAndMetric(proto.FlashNodeList, metric, nil, nil)
	}()
	showAll := true
	active := false
	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, exists := r.Form["active"]; exists {
		showAll = false
		activeReq, _ := strconv.ParseInt(r.FormValue("active"), 10, 64)
		if activeReq == -1 {
			showAll = true
		} else if activeReq == 1 {
			active = true
		}
	}
	zoneFlashNodes := m.cluster.flashNodeTopo.ListFlashNodes(showAll, active)
	sendOkReply(w, r, newSuccessHTTPReply(zoneFlashNodes))
}

func (m *Server) getFlashNode(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeGet))
	defer func() {
		doStatAndMetric(proto.FlashNodeGet, metric, err, nil)
	}()
	var nodeAddr common.String
	if err = parseArgs(r, argParserNodeAddr(&nodeAddr)); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var flashNode *flashgroupmanager.FlashNode
	if flashNode, err = m.cluster.peekFlashNode(nodeAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashNode.GetFlashNodeViewInfo()))
}

func (m *Server) removeFlashNode(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeRemove))
	defer func() {
		doStatAndMetric(proto.FlashNodeRemove, metric, err, nil)
	}()
	var offLineAddr common.String
	if err = parseArgs(r, argParserNodeAddr(&offLineAddr)); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var node *flashgroupmanager.FlashNode
	if node, err = m.cluster.flashNodeTopo.PeekFlashNode(offLineAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if node.FlashGroupID != flashgroupmanager.UnusedFlashNodeFlashGroupID {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("to delete a flashnode, it needs to be removed from the flashgroup first")))
		return
	}

	if err = m.cluster.removeFlashNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("delete flash node [%v] successfully", offLineAddr)))
}

func (m *Server) removeAllInactiveFlashNodes(w http.ResponseWriter, r *http.Request) {
	var err error
	removeAddresses := []string{}
	removeNodes := m.cluster.flashNodeTopo.GetAllInactiveFlashNodes()
	for _, node := range removeNodes {
		if err = m.cluster.removeFlashNode(node); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		removeAddresses = append(removeAddresses, node.Addr)
	}
	sendOkReply(w, r, newSuccessHTTPReply(removeAddresses))
}

func (c *Cluster) removeFlashNode(flashNode *flashgroupmanager.FlashNode) (err error) {
	return c.flashNodeTopo.RemoveFlashNode(c.Name, flashNode, c.syncDeleteFlashNode)
}

func (m *Server) setFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  common.String
		enable    bool
		workRole  string
		flashNode *flashgroupmanager.FlashNode
		err       error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeSet))
	defer func() {
		doStatAndMetric(proto.FlashNodeSet, metric, err, nil)
	}()
	if err = parseArgs(r, argParserNodeAddr(&nodeAddr)); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if flashNode, err = m.cluster.flashNodeTopo.PeekFlashNode(nodeAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if _, exists := r.Form["enable"]; exists {
		enable, err = strconv.ParseBool(r.FormValue("enable"))
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
		if err = m.cluster.updateFlashNode(flashNode, enable); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if _, exists := r.Form["workRole"]; exists {
		workRole = r.FormValue("workRole")
		if err = m.cluster.updateFlashNodeWorkRole(flashNode, workRole); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply("set flashNode success"))
}

func (m *Server) createFlashNodeManualTask(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		err   error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.CreateFlashNodeManualTask))
	defer func() {
		doStatAndMetric(proto.CreateFlashNodeManualTask, metric, err, nil)
	}()
	if bytes, err = io.ReadAll(r.Body); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	req := proto.FlashManualTask{}
	if err = json.Unmarshal(bytes, &req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var vol *Vol
	if vol, err = m.cluster.getVol(req.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	if !vol.remoteCacheEnable {
		err = fmt.Errorf("distribute cache of vol[%v] unavailable", vol.Name)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInvalidCfg, Msg: err.Error()})
		return
	}
	if m.cluster.flashNodeTopo == nil || !m.cluster.flashNodeTopo.CheckForActiveNode() {
		err = fmt.Errorf("no available distributed cache nodes")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInvalidCfg, Msg: err.Error()})
		return
	}
	start := time.Now()
	req.StartTime = &start
	req.UpdateTime = &start
	if req.Id == "" {
		req.Id = uuid.New().String()
	}
	m.cluster.flashManMgr.mu.Lock()
	if err = checkManualConfig(&req, vol, m.cluster.flashManMgr); err != nil {
		m.cluster.flashManMgr.mu.Unlock()
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInvalidCfg, Msg: err.Error()})
		return
	}
	err = m.cluster.syncAddFlashManualTask(&req)
	if err != nil {
		m.cluster.flashManMgr.mu.Unlock()
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	m.cluster.flashManMgr.flashManualTasks.Store(req.Id, &req)
	m.cluster.flashManMgr.mu.Unlock()
	log.LogInfof("action[setFlashNodeManualTask],clusterID[%v] vol:%v", m.cluster.Name, req.VolName)
	sendOkReply(w, r, newSuccessHTTPReply("set flashNode manual task success"))
}

func (m *Server) flashManualTask(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminFlashManualTask))
	defer func() {
		doStatAndMetric(proto.AdminFlashManualTask, metric, err, nil)
	}()
	if m.cluster.partition == nil || !m.cluster.partition.IsRaftLeader() {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "not leader"})
		return
	}
	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	opCode := r.FormValue("op")
	tId := r.FormValue("tid")
	switch opCode {
	case "info":
		vol := r.FormValue("vol")
		rsp := m.cluster.flashManMgr.findMatchTasks(vol, tId)
		sendOkReply(w, r, newSuccessHTTPReply(rsp))
	case "set":
		limit := r.FormValue("total_limit")
		if limit != "" {
			if totalLimit, err := strconv.ParseInt(limit, 10, 32); err != nil {
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
				return
			} else {
				m.cluster.flashManMgr.taskTotalLimit = int(totalLimit)
			}
		}
		sendOkReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("total limit %v", limit)})
	case "stop":
		err = m.cluster.flashManMgr.dispatchTaskOp(tId, opCode)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		} else {
			sendOkReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("tid(%v), op(%v), send to flashnode", tId, opCode)})
		}
	case "pause":
		err = m.cluster.flashManMgr.dispatchTaskOp(tId, opCode)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		} else {
			manualTask, ok := m.cluster.flashManMgr.LoadManualTaskById(tId)
			if ok {
				manualTask.Lock()
				manualTask.Status = int(proto.Flash_Task_Pause)
				if e := m.cluster.syncAddFlashManualTask(manualTask); e != nil {
					log.LogWarnf("action[pause] syncAddFlashManualTask %v err(%v)", manualTask, e)
				}
				manualTask.Unlock()
			}
			sendOkReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("tid(%v), op(%v), send to flashnode", tId, opCode)})
		}
	case "resume":
		err = m.cluster.flashManMgr.dispatchTaskOp(tId, opCode)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		} else {
			manualTask, ok := m.cluster.flashManMgr.LoadManualTaskById(tId)
			if ok {
				manualTask.Lock()
				manualTask.Status = int(proto.Flash_Task_Running)
				if e := m.cluster.syncAddFlashManualTask(manualTask); e != nil {
					log.LogWarnf("action[resume] syncAddFlashManualTask %v err(%v)", manualTask, e)
				}
				manualTask.Unlock()
			}
			sendOkReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("tid(%v), op(%v), send to flashnode", tId, opCode)})
		}
	case "delete":
		manualTask, ok := m.cluster.flashManMgr.LoadManualTaskById(tId)
		if !ok {
			log.LogWarnf("action[delete] %v does not exsit", tId)
		} else {
			manualTask.Lock()
			if proto.ManualTaskIsRunning(manualTask.Status) || manualTask.Status == int(proto.Flash_Task_Pause) {
				manualTask.Unlock()
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("tid(%v), invaild status[%v] can not delete", tId, manualTask.Status)})
				return
			}
			if err = m.cluster.syncDeleteFlashManualTask(manualTask); err != nil {
				log.LogWarnf("action[delete] syncDeleteFlashManualTask %v err(%v)", manualTask.Id, err)
			}
			manualTask.Unlock()
			m.cluster.flashManMgr.flashManualTasks.Delete(tId)
		}
		sendOkReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("tid(%v), delete success", tId)})
	default:
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "invalid op"})
	}
}

func (m *Server) handleFlashNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		tr  *proto.AdminTask
		err error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetFlashNodeTaskResponse))
	defer func() {
		doStatAndMetric(proto.GetFlashNodeTaskResponse, metric, err, nil)
	}()

	tr, err = parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleFlashNodeTaskResponse(tr.OperatorAddr, tr)
}

func (c *Cluster) handleFlashNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("flash action[handleFlashNodeTaskResponse] receive addr[%v] task response, but task is nil", nodeAddr)
		return
	}
	log.LogInfof("flash action[handleFlashNodeTaskResponse] receive addr[%v] task: %v", nodeAddr, task.ToString())
	var (
		err       error
		flashNode *flashgroupmanager.FlashNode
	)

	if flashNode, err = c.peekFlashNode(nodeAddr); err != nil {
		goto errHandler
	}
	flashNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpFlashNodeScan:
		response := task.Response.(*proto.FlashNodeManualTaskResponse)
		err = c.handleFlashNodeScanResp(task.OperatorAddr, response)
	case proto.OpFlashNodeHeartbeat:
		response := task.Response.(*proto.FlashNodeHeartbeatResponse)
		err = c.handleFlashNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("flash unknown operate code %v", task.OpCode))
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogWarnf("flash handleFlashNodeTaskResponse failed, task: %v, err: %v", task.ToString(), err)
}

func (c *Cluster) handleFlashNodeHeartbeatResp(nodeAddr string, resp *proto.FlashNodeHeartbeatResponse) (err error) {
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleFlashNodeHeartbeatResp] clusterID[%v] flashNode[%v] heartbeat task failed, err[%v]",
			c.Name, nodeAddr, resp.Result))
		return
	}
	var node *flashgroupmanager.FlashNode
	if node, err = c.peekFlashNode(nodeAddr); err != nil {
		log.LogErrorf("action[handleFlashNodeHeartbeatResp], flashNode[%v], heartbeat error: %v", nodeAddr, err.Error())
		return
	}
	node.SetActive()
	node.UpdateFlashNodeStatHeartbeat(resp)
	c.handleManualTaskProcessing(node, resp)
	return
}

func (c *Cluster) updateFlashNode(flashNode *flashgroupmanager.FlashNode, enable bool) (err error) {
	return c.flashNodeTopo.UpdateFlashNode(flashNode, enable, c.syncUpdateFlashNode)
}

func (c *Cluster) updateFlashNodeWorkRole(flashNode *flashgroupmanager.FlashNode, workRole string) error {
	flashNode.Lock()
	defer flashNode.Unlock()
	flashNode.WorkRole = workRole
	if err := c.syncUpdateFlashNode(flashNode); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) syncAddFlashNode(flashNode *flashgroupmanager.FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncAddFlashNode, flashNode)
}

func (c *Cluster) syncUpdateFlashNode(flashNode *flashgroupmanager.FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncUpdateFlashNode, flashNode)
}

func (c *Cluster) syncDeleteFlashNode(flashNode *flashgroupmanager.FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncDeleteFlashNode, flashNode)
}

func (c *Cluster) syncPutFlashNodeInfo(opType uint32, flashNode *flashgroupmanager.FlashNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashNodePrefix + strconv.FormatUint(flashNode.ID, 10) + keySeparator + flashNode.Addr
	metadata.V, err = json.Marshal(flashNode.FlashNodeValue)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncAddFlashManualTask(flt *proto.FlashManualTask) (err error) {
	if flt == nil {
		return fmt.Errorf("flashManualTask is nil on syncAddFlashManualTask")
	}
	return c.syncPutFlashManualTaskInfo(opSyncAddFlashManualTask, flt)
}

func (c *Cluster) syncDeleteFlashManualTask(flt *proto.FlashManualTask) (err error) {
	return c.syncPutFlashManualTaskInfo(opSyncDeleteFlashManualTask, flt)
}

func (c *Cluster) syncPutFlashManualTaskInfo(opType uint32, flt *proto.FlashManualTask) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashManualTaskPrefix + flt.Id
	metadata.V, err = json.Marshal(flt)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) peekFlashNode(addr string) (flashNode *flashgroupmanager.FlashNode, err error) {
	return c.flashNodeTopo.PeekFlashNode(addr)
}

func argParserNodeAddr(nodeAddr *common.String) *common.Argument {
	return nodeAddr.Addr().OnValue(func() error {
		if ipAddr, ok := util.ParseAddrToIpAddr(nodeAddr.V); ok {
			nodeAddr.V = ipAddr
			return nil
		}
		return unmatchedKey(new(common.String).Addr().Key())
	})
}

func (m *Server) setFlashNodeReadIOLimits(w http.ResponseWriter, r *http.Request) {
	var (
		flow       common.Int
		iocc       common.Int
		factor     common.Int
		readFlow   int64
		readIocc   int64
		readFactor int64
		err        error
	)

	if err = parseArgs(r, flow.Flow().OmitEmpty().OnEmpty(func() error {
		readFlow = -1
		return nil
	}).OnValue(func() error {
		readFlow = flow.V
		return nil
	}),
		iocc.Iocc().OmitEmpty().OnEmpty(func() error {
			readIocc = -1
			return nil
		}).OnValue(func() error {
			readIocc = iocc.V
			return nil
		}),
		factor.Factor().OmitEmpty().OnEmpty(func() error {
			readFactor = -1
			return nil
		}).OnValue(func() error {
			readFactor = factor.V
			return nil
		})); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogDebugf("action[setFlashNodeReadIOLimits],flow[%v] iocc[%v] factor [%v]",
		readFlow, readIocc, readFactor)
	tasks := make([]*proto.AdminTask, 0)
	flashNodes := m.cluster.flashNodeTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetIOLimitsTask(int(readFlow), int(readIocc), int(readFactor), proto.OpFlashNodeSetReadIOLimits)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeSetIOLimitTasks(tasks)
	sendOkReply(w, r, newSuccessHTTPReply("set ReadIOLimits for FlashNode is submit,check it later."))
}

func (m *Server) setFlashNodeWriteIOLimits(w http.ResponseWriter, r *http.Request) {
	var (
		flow        common.Int
		iocc        common.Int
		factor      common.Int
		writeFlow   int64
		writeIocc   int64
		writeFactor int64
		err         error
	)

	if err = parseArgs(r, flow.Flow().OmitEmpty().OnEmpty(func() error {
		writeFlow = -1
		return nil
	}).OnValue(func() error {
		writeFlow = flow.V
		return nil
	}),
		iocc.Iocc().OmitEmpty().OnEmpty(func() error {
			writeIocc = -1
			return nil
		}).OnValue(func() error {
			writeIocc = iocc.V
			return nil
		}),
		factor.Factor().OmitEmpty().OnEmpty(func() error {
			writeFactor = -1
			return nil
		}).OnValue(func() error {
			writeFactor = factor.V
			return nil
		})); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogDebugf("action[setFlashNodeWriteIOLimits],flow[%v] iocc[%v] factor [%v]",
		writeFlow, writeIocc, writeFactor)
	tasks := make([]*proto.AdminTask, 0)
	flashNodes := m.cluster.flashNodeTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetIOLimitsTask(int(writeFlow), int(writeIocc), int(writeFactor), proto.OpFlashNodeSetWriteIOLimits)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeSetIOLimitTasks(tasks)
	sendOkReply(w, r, newSuccessHTTPReply("set WriteIOLimits for FlashNode is submit,check it later."))
}
