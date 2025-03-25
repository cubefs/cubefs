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
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/httpclient"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

const (
	_defaultNodeTimeoutDuration = defaultNodeTimeOutSec * time.Second
)

type flashNodeValue struct {
	// immutable
	ID       uint64
	Addr     string
	ZoneName string
	Version  string
	// mutable
	FlashGroupID   uint64 // 0: have not allocated to flash group
	IsEnable       bool
	TaskCountLimit int
}

type FlashNode struct {
	TaskManager *AdminTaskManager

	sync.RWMutex
	flashNodeValue
	DiskStat      []*proto.FlashNodeDiskCacheStat
	ReportTime    time.Time
	IsActive      bool
	LimiterStatus *proto.FlashNodeLimiterStatusInfo
	WorkRole      string
}

func newFlashNode(addr, zoneName, clusterID, version string, isEnable bool) *FlashNode {
	node := new(FlashNode)
	node.Addr = addr
	node.ZoneName = zoneName
	node.Version = version
	node.IsEnable = isEnable
	node.TaskManager = newAdminTaskManager(addr, clusterID)
	return node
}

func (flashNode *FlashNode) clean() {
	flashNode.TaskManager.exitCh <- struct{}{}
}

func (flashNode *FlashNode) setActive() {
	flashNode.Lock()
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	flashNode.Unlock()
}

func (flashNode *FlashNode) isWriteable() (ok bool) {
	flashNode.RLock()
	if flashNode.FlashGroupID == unusedFlashNodeFlashGroupID &&
		time.Since(flashNode.ReportTime) < _defaultNodeTimeoutDuration {
		ok = true
	}
	flashNode.RUnlock()
	return
}

func (flashNode *FlashNode) isActiveAndEnable() (ok bool) {
	flashNode.RLock()
	ok = flashNode.IsActive && flashNode.IsEnable
	flashNode.RUnlock()
	return
}

func (flashNode *FlashNode) getFlashNodeViewInfo() (info *proto.FlashNodeViewInfo) {
	flashNode.RLock()
	info = &proto.FlashNodeViewInfo{
		ID:            flashNode.ID,
		Addr:          flashNode.Addr,
		ReportTime:    flashNode.ReportTime,
		IsActive:      flashNode.IsActive,
		Version:       flashNode.Version,
		ZoneName:      flashNode.ZoneName,
		FlashGroupID:  flashNode.FlashGroupID,
		IsEnable:      flashNode.IsEnable,
		DiskStat:      flashNode.DiskStat,
		LimiterStatus: flashNode.LimiterStatus,
	}
	flashNode.RUnlock()
	return
}

func (flashNode *FlashNode) updateFlashNodeStatHeartbeat(resp *proto.FlashNodeHeartbeatResponse) {
	log.LogInfof("updateFlashNodeStatHeartbeat, flashNode:%v, resp[%v], time:%v", flashNode.Addr, resp, time.Now().Format("2006-01-02 15:04:05"))
	flashNode.Lock()
	flashNode.DiskStat = resp.Stat
	flashNode.LimiterStatus = resp.LimiterStatus
	flashNode.TaskCountLimit = resp.FlashNodeTaskCountLimit
	flashNode.Unlock()
}

// TODO: sync with proto.FlashNodeHeartbeatResponse.
func (c *Cluster) syncFlashNodeHeartbeatTasks(tasks []*proto.AdminTask) {
	var packet *proto.Packet
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.peekFlashNode(t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		if packet, err = node.TaskManager.syncSendAdminTask(t); err != nil {
			log.LogError(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		node.setActive()

		resp := &proto.FlashNodeHeartbeatResponse{}
		err = json.Unmarshal(packet.Data, resp)
		if err != nil {
			log.LogErrorf("Failed to unmarshal response: %v", err)
			continue
		}
		node.updateFlashNodeStatHeartbeat(resp)
		c.handleFlashNodeHeartbeatResp(node, resp)
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
		if _, err = node.TaskManager.syncSendAdminTask(t); err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
	}
}

func (c *Cluster) handleFlashNodeHeartbeatResp(flashNode *FlashNode, resp *proto.FlashNodeHeartbeatResponse) {
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
	tasks := make([]*proto.AdminTask, 0)
	c.flashNodeTopo.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.checkLiveliness()
		task := node.createHeartbeatTask(c.masterAddr(), c.cfg.flashNodeHandleReadTimeout, c.cfg.flashNodeReadDataNodeTimeout)
		tasks = append(tasks, task)
		return true
	})
	go c.syncFlashNodeHeartbeatTasks(tasks)
}

func (flashNode *FlashNode) checkLiveliness() {
	flashNode.Lock()
	if time.Since(flashNode.ReportTime) > _defaultNodeTimeoutDuration {
		flashNode.IsActive = false
	}
	flashNode.Unlock()
}

func (flashNode *FlashNode) createHeartbeatTask(masterAddr string, flashNodeHandleReadTimeout int, flashNodeReadDataNodeTimeout int) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	request.FlashNodeHandleReadTimeout = flashNodeHandleReadTimeout
	request.FlashNodeReadDataNodeTimeout = flashNodeReadDataNodeTimeout

	task = proto.NewAdminTask(proto.OpFlashNodeHeartbeat, flashNode.Addr, request)
	return
}

func (flashNode *FlashNode) createSetIOLimitsTask(flow, iocc, factor int, opCode uint8) (task *proto.AdminTask) {
	request := &proto.FlashNodeSetIOLimitsRequest{
		Flow:   flow,
		Iocc:   iocc,
		Factor: factor,
	}
	task = proto.NewAdminTask(opCode, flashNode.Addr, request)
	return
}

func (flashNode *FlashNode) createFnScanTask(masterAddr string, manualTask *proto.FlashManualTask) (task *proto.AdminTask) {
	request := &proto.FlashNodeManualTaskRequest{
		MasterAddr: masterAddr,
		FnNodeAddr: flashNode.Addr,
		Task:       manualTask,
	}
	task = proto.NewAdminTaskEx(proto.OpFlashNodeScan, flashNode.Addr, request, manualTask.Id)
	return
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
	c.flashNodeTopo.mu.Lock()
	defer func() {
		c.flashNodeTopo.mu.Unlock()
		if err != nil {
			log.LogErrorf("action[addFlashNode],clusterID[%v] Addr:%v err:%v ", c.Name, nodeAddr, err.Error())
		}
	}()

	var flashNode *FlashNode
	flashNode, err = c.peekFlashNode(nodeAddr)
	if err == nil {
		return flashNode.ID, nil
	}
	flashNode = newFlashNode(nodeAddr, zoneName, c.Name, version, true)
	_, err = c.flashNodeTopo.getZone(zoneName)
	if err != nil {
		c.flashNodeTopo.putZoneIfAbsent(newFlashNodeZone(zoneName))
	}
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		return
	}
	flashNode.ID = id
	if err = c.syncAddFlashNode(flashNode); err != nil {
		return
	}
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	if err = c.flashNodeTopo.putFlashNode(flashNode); err != nil {
		return
	}
	log.LogInfof("action[addFlashNode],clusterID[%v] Addr:%v ZoneName:%v success", c.Name, nodeAddr, zoneName)
	return
}

func (m *Server) listFlashNodes(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeList))
	defer func() {
		doStatAndMetric(proto.FlashNodeList, metric, nil, nil)
	}()
	zoneFlashNodes := make(map[string][]*proto.FlashNodeViewInfo)
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
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if showAll || flashNode.isActiveAndEnable() == active {
			zoneFlashNodes[flashNode.ZoneName] = append(zoneFlashNodes[flashNode.ZoneName], flashNode.getFlashNodeViewInfo())
		}
		return true
	})
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
	var flashNode *FlashNode
	if flashNode, err = m.cluster.peekFlashNode(nodeAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(flashNode.getFlashNodeViewInfo()))
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
	var node *FlashNode
	if node, err = m.cluster.peekFlashNode(offLineAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if node.FlashGroupID != unusedFlashNodeFlashGroupID {
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
	var (
		err         error
		removeNodes []*FlashNode
	)
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if !flashNode.isActiveAndEnable() && flashNode.FlashGroupID == unusedFlashNodeFlashGroupID {
			removeNodes = append(removeNodes, flashNode)
		}
		return true
	})
	for _, node := range removeNodes {
		if err = m.cluster.removeFlashNode(node); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply("remove all inactive flash nodes successfully"))
}

func (c *Cluster) removeFlashNode(flashNode *FlashNode) (err error) {
	log.LogWarnf("action[removeFlashNode], ZoneName[%s] Node[%s] offline", flashNode.ZoneName, flashNode.Addr)
	var flashGroupID uint64
	if flashGroupID, err = c.deleteFlashNode(flashNode); err != nil {
		return
	}
	if flashGroupID != unusedFlashNodeFlashGroupID {
		var flashGroup *FlashGroup
		if flashGroup, err = c.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
			return
		}
		flashGroup.removeFlashNode(flashNode.Addr)
		c.flashNodeTopo.updateClientCache()
	}

	go func() {
		time.Sleep(time.Duration(defaultWaitClientUpdateFgTimeSec) * time.Second)
		arr := strings.SplitN(flashNode.Addr, ":", 2)
		p, _ := strconv.ParseUint(arr[1], 10, 64)
		addr := fmt.Sprintf("%s:%d", arr[0], p+1)
		if err = httpclient.New().Addr(addr).FlashNode().EvictAll(); err != nil {
			log.LogErrorf("flashNode[%v] evict all failed, err:%v", flashNode.Addr, err)
			return
		}
	}()

	log.LogInfof("action[removeFlashNode], clusterID[%s] node[%s] flashGroupID[%d] offline success",
		c.Name, flashNode.Addr, flashGroupID)
	return
}

func (c *Cluster) deleteFlashNode(flashNode *FlashNode) (oldFlashGroupID uint64, err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	oldFlashGroupID = flashNode.FlashGroupID
	flashNode.FlashGroupID = unusedFlashNodeFlashGroupID
	if err = c.syncDeleteFlashNode(flashNode); err != nil {
		log.LogErrorf("action[deleteFlashNode],clusterID[%v] node[%v] offline failed,err[%v]",
			c.Name, flashNode.Addr, err)
		flashNode.FlashGroupID = oldFlashGroupID
		return
	}
	c.delFlashNodeFromCache(flashNode)
	return
}

func (c *Cluster) delFlashNodeFromCache(flashNode *FlashNode) {
	c.flashNodeTopo.deleteFlashNode(flashNode)
	go flashNode.clean()
}

func (m *Server) setFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  common.String
		enable    bool
		workRole  string
		flashNode *FlashNode
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
	if flashNode, err = m.cluster.peekFlashNode(nodeAddr.V); err != nil {
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
	if m.cluster.flashNodeTopo == nil || !m.cluster.flashNodeTopo.checkForActiveNode() {
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

func (c *Cluster) updateFlashNode(flashNode *FlashNode, enable bool) (err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.IsEnable != enable {
		oldState := flashNode.IsEnable
		flashNode.IsEnable = enable
		if err = c.syncUpdateFlashNode(flashNode); err != nil {
			flashNode.IsEnable = oldState
			return
		}
		if flashNode.FlashGroupID != unusedFlashNodeFlashGroupID {
			c.flashNodeTopo.updateClientCache()
		}
	}
	return
}

func (c *Cluster) updateFlashNodeWorkRole(flashNode *FlashNode, workRole string) error {
	flashNode.Lock()
	defer flashNode.Unlock()
	flashNode.WorkRole = workRole
	if err := c.syncUpdateFlashNode(flashNode); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) syncAddFlashNode(flashNode *FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncAddFlashNode, flashNode)
}

func (c *Cluster) syncUpdateFlashNode(flashNode *FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncUpdateFlashNode, flashNode)
}

func (c *Cluster) syncDeleteFlashNode(flashNode *FlashNode) (err error) {
	return c.syncPutFlashNodeInfo(opSyncDeleteFlashNode, flashNode)
}

func (c *Cluster) syncPutFlashNodeInfo(opType uint32, flashNode *FlashNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashNodePrefix + strconv.FormatUint(flashNode.ID, 10) + keySeparator + flashNode.Addr
	metadata.V, err = json.Marshal(flashNode.flashNodeValue)
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

func (c *Cluster) peekFlashNode(addr string) (flashNode *FlashNode, err error) {
	value, ok := c.flashNodeTopo.flashNodeMap.Load(addr)
	if !ok {
		err = errors.Trace(notFoundMsg(fmt.Sprintf("flashnode[%v]", addr)), "")
		return
	}
	flashNode = value.(*FlashNode)
	return
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
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if flashNode.isActiveAndEnable() {
			task := flashNode.createSetIOLimitsTask(int(readFlow), int(readIocc), int(readFactor), proto.OpFlashNodeSetReadIOLimits)
			tasks = append(tasks, task)
		}
		return true
	})
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
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if flashNode.isActiveAndEnable() {
			task := flashNode.createSetIOLimitsTask(int(writeFlow), int(writeIocc), int(writeFactor), proto.OpFlashNodeSetWriteIOLimits)
			tasks = append(tasks, task)
		}
		return true
	})
	go m.cluster.syncFlashNodeSetIOLimitTasks(tasks)
	sendOkReply(w, r, newSuccessHTTPReply("set WriteIOLimits for FlashNode is submit,check it later."))
}
