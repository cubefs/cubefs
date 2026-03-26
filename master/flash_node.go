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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
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

func (c *Cluster) addFlashNodeHeartbeatTasks(topoName string, tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.peekFlashNode(topoName, t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		node.TaskManager.AddTask(t)
	}
}

func (c *Cluster) syncFlashNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.peekFlashNode(t.TopoName, t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		if _, err = node.SyncSendAdminTask(t); err != nil {
			log.LogWarn(fmt.Sprintf("action[syncFlashNodeTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
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

// getRemoteCacheDisableTTLMap returns a map of volume -> remoteCacheDisableTTL for all volumes
func (c *Cluster) getRemoteCacheDisableTTLMap() map[string]bool {
	remoteCacheDisableTTLMap := make(map[string]bool)
	c.volMutex.RLock()
	for name, vol := range c.vols {
		if vol.remoteCacheEnable && vol.remoteCacheDisableTTL {
			remoteCacheDisableTTLMap[name] = true
		}
	}
	c.volMutex.RUnlock()
	return remoteCacheDisableTTLMap
}

func (m *Server) getRemoteCacheDisableTTLMap(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetRemoteCacheDisableTTLMap))
	defer func() {
		doStatAndMetric(proto.AdminGetRemoteCacheDisableTTLMap, metric, err, nil)
	}()
	remoteCacheDisableTTLMap := m.cluster.getRemoteCacheDisableTTLMap()
	sendOkReply(w, r, newSuccessHTTPReply(remoteCacheDisableTTLMap))
}

func (m *Server) setFlashTopoVolReadFlow(w http.ResponseWriter, r *http.Request) {
	var (
		flow      common.Int
		err       error
		flashTopo *flashgroupmanager.FlashNodeTopology
	)

	volName := r.FormValue(volNameKey)
	if volName == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "missing volName"})
		return
	}
	if err = parseArgs(r, flow.Key(remoteCacheReadFlow).OnValue(func() error { return nil })); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if flow.V < 0 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "freadFlow must be >= 0"})
		return
	}

	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}

	_, err = m.cluster.getVol(volName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	flashTopo.SetRemoteCacheReadFlow(volName, flow.V)
	if err = m.cluster.syncUpdateFlashTopo(flashTopo); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	tasks := make([]*proto.AdminTask, 0)
	flashNodes := flashTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetVolReadIOLimitsTask(volName, flow.V)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeTasks(tasks)

	msg := fmt.Sprintf("set vol(%s) freadFlow to %d for topo(%s) and submitted to flashnodes", volName, flow.V, topoName)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) setFlashTopoVolWriteFlow(w http.ResponseWriter, r *http.Request) {
	var (
		flow      common.Int
		err       error
		flashTopo *flashgroupmanager.FlashNodeTopology
	)

	volName := r.FormValue(volNameKey)
	if volName == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "missing volName"})
		return
	}
	if err = parseArgs(r, flow.Key(remoteCacheWriteFlow).OnValue(func() error { return nil })); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if flow.V < 0 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "fwriteFlow must be >= 0"})
		return
	}

	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}

	_, err = m.cluster.getVol(volName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	flashTopo.SetRemoteCacheWriteFlow(volName, flow.V)
	if err = m.cluster.syncUpdateFlashTopo(flashTopo); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	tasks := make([]*proto.AdminTask, 0)
	flashNodes := flashTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetVolWriteIOLimitsTask(volName, flow.V)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeTasks(tasks)

	msg := fmt.Sprintf("set vol(%s) fwriteFlow to %d for topo(%s) and submitted to flashnodes", volName, flow.V, topoName)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (c *Cluster) checkFlashNodeHeartbeat() {
	// Collect remoteCacheDisableTTL for all volumes
	remoteCacheDisableTTLMap := c.getRemoteCacheDisableTTLMap()

	c.flashNodeTopo.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}
		topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
		if !ok {
			return true
		}
		tasks := topo.CreateFlashNodeHeartBeatTasks(
			c.masterAddr(),
			c.cfg.flashNodeHandleReadTimeout,
			c.cfg.flashNodeReadDataNodeTimeout,
			c.cfg.flashHotKeyMissCount,
			c.cfg.flashReadFlowLimit,
			c.cfg.flashWriteFlowLimit,
			c.cfg.flashKeyFlowLimit,
			remoteCacheDisableTTLMap,
			topo.GetRemoteCacheReadFlowMap(),
			topo.GetRemoteCacheWriteFlowMap(),
		)
		c.addFlashNodeHeartbeatTasks(topo.Name, tasks)
		return true
	})
}

func (m *Server) addFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr common.String
		zoneName common.String
		version  common.String
		nodeID   common.Uint
		id       uint64
		topoName string
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
		nodeID.Key("id").OmitEmpty(),
	); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	region := r.FormValue(regionKey)
	if region == "" {
		region = proto.DefaultRegionName
	}
	// all flashnode is added to idle topo by default
	if id, topoName, err = m.cluster.addFlashNode(proto.IdleTopoName, nodeAddr.V, zoneName.V, version.V, region, nodeID.V); err != nil {
		log.LogWarnf("addFlashNode: fn[%v] topo %v nodeID %v region %v failed:err %v", nodeAddr.V, topoName, nodeID.V, region, err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if detail, _ := strconv.ParseBool(r.FormValue("detail")); detail {
		sendOkReply(w, r, newSuccessHTTPReply(&proto.FlashNodeRegisterResponse{
			NodeID:   id,
			TopoName: topoName,
		}))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (c *Cluster) addFlashNode(topoName, nodeAddr, zoneName, version, region string, id uint64) (nodeID uint64, assignedTopoName string, err error) {
	// check if flash node is registered before
	var (
		flashTopo *flashgroupmanager.FlashNodeTopology
		flashNode *flashgroupmanager.FlashNode
	)
	assignedTopoName = topoName
	c.flashNodeTopo.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}
		topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
		if !ok {
			return true
		}
		if id == 0 { // flash node is upgraded from v3.5.3 before
			flashNode, _ = topo.PeekFlashNode(nodeAddr)
		} else {
			flashNode, _ = topo.PeekFlashNodeById(id)
		}
		if flashNode != nil {
			if flashNode.Region != region {
				err = fmt.Errorf("region is conflict: [%v]  previously registered[%v]", region, flashNode.Region)
			}
			assignedTopoName = topo.Name
			return false
		}
		return true
	})

	if err != nil {
		log.LogWarnf("addFlashNode fn[%v]:err %v", id, err.Error())
		return
	}
	flashTopo, err = c.PeekFlashTopo(assignedTopoName)
	if err != nil {
		return
	}
	nodeID, err = flashTopo.AddFlashNode(c.Name, nodeAddr, zoneName, version, region, id,
		c.idAlloc.allocateCommonID, c.syncAddFlashNode, c.syncMoveFlashNode)
	return
}

func (m *Server) listFlashNodes(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeList))
	defer func() {
		doStatAndMetric(proto.FlashNodeList, metric, nil, nil)
	}()
	showAll := true
	active := false
	showAllTopo := false
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
	// whether to show all topologies
	if v := r.FormValue("showAllTopo"); v != "" {
		if b, e := strconv.ParseBool(v); e == nil {
			showAllTopo = b
		}
	}
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	var (
		flashTopo *flashgroupmanager.FlashNodeTopology
		err       error
	)
	log.LogDebugf("listFlashNodes: showAllTopo %v, showAll %v active %v", showAllTopo, showAll, active)
	if showAllTopo {
		// aggregate nodes from all topologies
		all := make(map[string][]*proto.FlashNodeViewInfo)
		m.cluster.flashNodeTopo.Range(func(_, value interface{}) bool {
			if value == nil {
				return true
			}
			topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
			if !ok {
				return true
			}
			log.LogDebugf("listFlashNodes: ListFlashNodes topo %v %v, showAll %v active %v", topo.Name, showAllTopo, showAll, active)
			mset := topo.ListFlashNodes(showAll, active)
			for zone, nodes := range mset {
				all[zone] = append(all[zone], nodes...)
			}
			return true
		})
		sendOkReply(w, r, newSuccessHTTPReply(all))
		return
	}
	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	zoneFlashNodes := flashTopo.ListFlashNodes(showAll, active)
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
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	var flashNode *flashgroupmanager.FlashNode
	if flashNode, err = m.cluster.peekFlashNode(topoName, nodeAddr.V); err != nil {
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
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	var flashTopo *flashgroupmanager.FlashNodeTopology
	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}

	var node *flashgroupmanager.FlashNode
	if node, err = flashTopo.PeekFlashNode(offLineAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrFlashNodeNotExists))
		return
	}

	if node.FlashGroupID != flashgroupmanager.UnusedFlashNodeFlashGroupID {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("to delete a flashnode, it needs to be removed from the flashgroup first")))
		return
	}

	if err = m.cluster.removeFlashNode(flashTopo, node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("delete flash node [%v] successfully", offLineAddr)))
}

func (m *Server) removeAllInactiveFlashNodes(w http.ResponseWriter, r *http.Request) {
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	var (
		err       error
		flashTopo *flashgroupmanager.FlashNodeTopology
	)
	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}
	removeAddresses := []string{}
	removeNodes := flashTopo.GetAllInactiveFlashNodes()
	for _, node := range removeNodes {
		if err = m.cluster.removeFlashNode(flashTopo, node); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		removeAddresses = append(removeAddresses, node.Addr)
	}
	sendOkReply(w, r, newSuccessHTTPReply(removeAddresses))
}

func (c *Cluster) removeFlashNode(flashTopo *flashgroupmanager.FlashNodeTopology, flashNode *flashgroupmanager.FlashNode) (err error) {
	return flashTopo.RemoveFlashNode(c.Name, flashNode, c.syncDeleteFlashNode)
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
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	flashTopo, err := m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}

	if flashNode, err = flashTopo.PeekFlashNode(nodeAddr.V); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if _, exists := r.Form["enable"]; exists {
		enable, err = strconv.ParseBool(r.FormValue("enable"))
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
		if err = m.cluster.updateFlashNode(flashTopo, flashNode, enable); err != nil {
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
		bytes     []byte
		err       error
		flashTopo *flashgroupmanager.FlashNodeTopology
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
	if req.ManualTaskStatistics == nil {
		req.ManualTaskStatistics = &proto.ManualTaskStatistics{}
	}
	if req.ManualTaskConfig.PrepareLimitPerSecond == 0 {
		req.ManualTaskConfig.PrepareLimitPerSecond = 10000
	}
	if req.ManualTaskConfig.TaskTimeoutMinutes == 0 {
		req.ManualTaskConfig.TaskTimeoutMinutes = 20
	}
	if req.ManualTaskConfig.RetryCount == 0 {
		req.ManualTaskConfig.RetryCount = 3
	}
	// Validate file size limits
	if req.ManualTaskConfig.MinFileSizeLimit > req.ManualTaskConfig.MaxFileSizeLimit {
		err = fmt.Errorf("MinFileSizeLimit(%d) cannot be greater than MaxFileSizeLimit(%d)",
			req.ManualTaskConfig.MinFileSizeLimit, req.ManualTaskConfig.MaxFileSizeLimit)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	var vol *Vol
	if vol, err = m.cluster.getVol(req.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	// Backward Compatibility
	if req.TopoName == "" {
		req.TopoName = proto.DefaultTopoName
	}

	flashTopo, err = m.cluster.PeekFlashTopo(req.TopoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if !flashTopo.CheckForActiveNode() {
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
		mTasks := m.cluster.flashManMgr.findMatchTasks(vol, tId)
		for _, rsp := range mTasks {
			if rsp == nil || rsp.ManualTaskStatistics == nil {
				continue
			}
			if rsp.Status == int(proto.Flash_Task_End) {
				stats := rsp.ManualTaskStatistics
				if stats.TotalExtentKeyNum > 0 {
					numerator := stats.SuccessFlashKeyNum + stats.SkipFlashKeyNum
					denominator := stats.TotalExtentKeyNum
					stats.CompletionRate = fmt.Sprintf("%d%%", (numerator*100)/denominator)
				} else {
					numerator := stats.TotalFileCachedNum + stats.TotalDirScannedNum
					denominator := numerator + stats.ErrorCacheNum + stats.ErrorReadDirNum
					if denominator == 0 {
						stats.CompletionRate = "0%"
					} else {
						stats.CompletionRate = fmt.Sprintf("%d%%", (numerator*100)/denominator)
					}
				}
			} else if rsp.ManualTaskConfig.PrintProgress && rsp.ManualTaskStatistics.TotalEntryNum > 0 {
				stats := rsp.ManualTaskStatistics
				numerator := stats.TotalFileCachedNum + stats.TotalDirScannedNum
				percent := (numerator * 100) / rsp.ManualTaskStatistics.TotalEntryNum
				if percent > 99 {
					percent = 99
				}
				if percent < 0 {
					percent = 0
				}
				stats.CompletionRate = fmt.Sprintf("%d%%", percent)
			}
			if !rsp.ManualTaskConfig.PrintProgress {
				continue
			}
			if rsp.Done {
				rsp.ManualTaskStatistics.LoadProgress = "100%"
				continue
			}
			stats := rsp.ManualTaskStatistics
			if stats.TotalEntryNum <= 0 {
				stats.LoadProgress = "0%"
				continue
			}

			doneEntries := stats.TotalFileScannedNum + stats.TotalDirScannedNum
			percent := (doneEntries * 100) / stats.TotalEntryNum
			if percent > 99 {
				percent = 99
			}
			if percent < 0 {
				percent = 0
			}
			stats.LoadProgress = fmt.Sprintf("%d%%", percent)
		}

		sendOkReply(w, r, newSuccessHTTPReply(mTasks))
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
	// Backward-compatible with older FlashNode versions.
	if task.TopoName == "" {
		task.TopoName = proto.DefaultTopoName
	}
	if flashNode, err = c.peekFlashNode(task.TopoName, nodeAddr); err != nil {
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
		if response.TopoName == "" {
			response.TopoName = proto.DefaultTopoName
		}
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
	log.LogErrorf("flash handleFlashNodeTaskResponse failed, task: %v, err: %v", task.ToString(), err)
}

func (c *Cluster) handleFlashNodeHeartbeatResp(nodeAddr string, resp *proto.FlashNodeHeartbeatResponse) (err error) {
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleFlashNodeHeartbeatResp] clusterID[%v] flashNode[%v] heartbeat task failed, err[%v]",
			c.Name, nodeAddr, resp.Result))
		return
	}
	var (
		node *flashgroupmanager.FlashNode
		topo *flashgroupmanager.FlashNodeTopology
	)
	if topo, err = c.PeekFlashTopo(resp.TopoName); err != nil {
		log.LogErrorf("action[handleFlashNodeHeartbeatResp], topo[%v] not found", resp.TopoName)
		return
	}
	if node, err = topo.PeekFlashNode(nodeAddr); err != nil {
		log.LogErrorf("action[handleFlashNodeHeartbeatResp], flashNode[%v], heartbeat error: %v", nodeAddr, err.Error())
		return
	}
	node.SetActive()
	if err = node.UpdateZoneName(topo, resp.ZoneName, c.syncUpdateFlashNode); err != nil {
		log.LogErrorf("action[handleFlashNodeHeartbeatResp], flashNode[%v], update zone name %v failed: %v", nodeAddr, resp.ZoneName, err.Error())
		return
	}
	node.UpdateFlashNodeStatHeartbeat(resp)
	c.handleManualTaskProcessing(node, resp)
	return
}

func (c *Cluster) updateFlashNode(topo *flashgroupmanager.FlashNodeTopology, flashNode *flashgroupmanager.FlashNode, enable bool) (err error) {
	if _, err = topo.PeekFlashNode(flashNode.Addr); err == nil {
		err = topo.UpdateFlashNode(flashNode, enable, c.syncUpdateFlashNode)
		return
	}
	return
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

func (c *Cluster) syncMoveFlashNode(oldAddr string, newValue *flashgroupmanager.FlashNodeValue) (err error) {
	if newValue == nil {
		return fmt.Errorf("syncMoveFlashNode: newValue is nil")
	}
	oldKey := flashNodePrefix + strconv.FormatUint(newValue.ID, 10) + keySeparator + oldAddr
	newKey := flashNodePrefix + strconv.FormatUint(newValue.ID, 10) + keySeparator + newValue.Addr
	newV, err := json.Marshal(*newValue)
	if err != nil {
		return errors.New(err.Error())
	}
	mv := &moveKeyValueCmd{NewK: newKey, NewV: newV}
	mvBytes, err := json.Marshal(mv)
	if err != nil {
		return errors.New(err.Error())
	}
	metadata := &RaftCmd{Op: opSyncMoveFlashNode, K: oldKey, V: mvBytes}
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

func (c *Cluster) peekFlashNode(topoName, addr string) (flashNode *flashgroupmanager.FlashNode, err error) {
	var flashTopo *flashgroupmanager.FlashNodeTopology
	flashTopo, err = c.PeekFlashTopo(topoName)
	if err != nil {
		return
	}
	return flashTopo.PeekFlashNode(addr)
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
		flashTopo  *flashgroupmanager.FlashNodeTopology
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
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}

	log.LogDebugf("action[setFlashNodeReadIOLimits],flow[%v] iocc[%v] factor [%v]",
		readFlow, readIocc, readFactor)
	tasks := make([]*proto.AdminTask, 0)
	flashNodes := flashTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetIOLimitsTask(int(readFlow), int(readIocc), int(readFactor), proto.OpFlashNodeSetReadIOLimits)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeTasks(tasks)
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
		flashTopo   *flashgroupmanager.FlashNodeTopology
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
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}
	log.LogDebugf("action[setFlashNodeWriteIOLimits],flow[%v] iocc[%v] factor [%v]",
		writeFlow, writeIocc, writeFactor)
	tasks := make([]*proto.AdminTask, 0)
	flashNodes := flashTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		task := flashNode.CreateSetIOLimitsTask(int(writeFlow), int(writeIocc), int(writeFactor), proto.OpFlashNodeSetWriteIOLimits)
		tasks = append(tasks, task)
	}
	go m.cluster.syncFlashNodeTasks(tasks)
	sendOkReply(w, r, newSuccessHTTPReply("set WriteIOLimits for FlashNode is submit,check it later."))
}

func (m *Server) setFlashNodePreheatIOLimits(w http.ResponseWriter, r *http.Request) {
	var (
		flow        common.Int
		preheatFlow int64
		err         error
		flashTopo   *flashgroupmanager.FlashNodeTopology
	)

	if err = parseArgs(r, flow.Flow().OmitEmpty().OnEmpty(func() error {
		preheatFlow = -1
		return nil
	}).OnValue(func() error {
		preheatFlow = flow.V
		return nil
	})); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// Backward Compatibility
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	addr := r.FormValue("addr")

	flashTopo, err = m.cluster.PeekFlashTopo(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)))
		return
	}
	log.LogDebugf("action[setFlashNodePreheatIOLimits],flow[%v] addr[%v]", preheatFlow, addr)
	tasks := make([]*proto.AdminTask, 0)
	flashNodes := flashTopo.GetAllActiveFlashNodes()
	for _, flashNode := range flashNodes {
		if addr != "" && flashNode.Addr != addr {
			continue
		}
		task := flashNode.CreateSetIOLimitsTask(int(preheatFlow), -1, -1, proto.OpFlashNodeSetPreheatIOLimits)
		tasks = append(tasks, task)
	}
	if len(tasks) == 0 {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("no flashnode found to set preheat IO limits")))
		return
	}
	go m.cluster.syncFlashNodeTasks(tasks)
	sendOkReply(w, r, newSuccessHTTPReply("set PreheatIOLimits for FlashNode is submit,check it later."))
}

func (m *Server) queryCacheVols(w http.ResponseWriter, r *http.Request) {
	topoName := r.FormValue(nameKey)
	if topoName == "" {
		topoName = proto.DefaultTopoName
	}
	result, err := m.cluster.queryCacheVols(topoName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(result))
}

func (c *Cluster) queryCacheVols(topoName string) (map[string]int64, error) {
	flashTopo, err := c.PeekFlashTopo(topoName)
	if err != nil {
		return nil, err
	}
	// forbid operations on markDeleted topology
	if flashTopo.IsMarkDelete() {
		return nil, fmt.Errorf("topo[%v] is markDeleted, operation not allowed", topoName)
	}

	flashNodes := flashTopo.GetAllActiveFlashNodes()
	if len(flashNodes) == 0 {
		log.LogWarnf("action[queryCacheVols] no available flash nodes ")
		return make(map[string]int64), nil
	}

	// Create tasks for all flashnodes
	tasks := make([]*proto.AdminTask, 0, len(flashNodes))
	for _, flashNode := range flashNodes {
		request := &proto.FlashNodeCacheVolsRequest{}
		task := proto.NewAdminTask(proto.OpFlashNodeCacheVols, flashNode.Addr, request)
		task.TopoName = topoName
		tasks = append(tasks, task)
	}

	// Send tasks concurrently and collect responses
	resultMap := make(map[string]int64)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, task := range tasks {
		wg.Add(1)
		go func(t *proto.AdminTask) {
			defer wg.Done()
			flashNode, err := c.peekFlashNode(topoName, t.OperatorAddr)
			if err != nil {
				log.LogWarnf("action[queryCacheVols] peekFlashNode %v failed: %v", t.OperatorAddr, err)
				return
			}
			packet, err := flashNode.SyncSendAdminTask(t)
			if err != nil {
				log.LogWarnf("action[queryCacheVols] SyncSendAdminTask to %v failed: %v", t.OperatorAddr, err)
				return
			}
			if packet.ResultCode != proto.OpOk {
				log.LogWarnf("action[queryCacheVols] task to %v failed with code %v", t.OperatorAddr, packet.ResultCode)
				return
			}
			// Unmarshal AdminTask from packet.Data, then parse Response field separately
			var adminTaskMap map[string]interface{}
			decode := json.NewDecoder(bytes.NewBuffer(packet.Data))
			decode.UseNumber()
			if err = decode.Decode(&adminTaskMap); err != nil {
				log.LogWarnf("action[queryCacheVols] decode AdminTask from %v failed: %v", t.OperatorAddr, err)
				return
			}
			// Parse Response field as FlashNodeCacheVolsResponse
			responseData, ok := adminTaskMap["Response"]
			if !ok {
				log.LogWarnf("action[queryCacheVols] Response field not found from %v", t.OperatorAddr)
				return
			}
			responseBytes, err := json.Marshal(responseData)
			if err != nil {
				log.LogWarnf("action[queryCacheVols] marshal Response from %v failed: %v", t.OperatorAddr, err)
				return
			}
			response := &proto.FlashNodeCacheVolsResponse{}
			if err = json.Unmarshal(responseBytes, response); err != nil {
				log.LogWarnf("action[queryCacheVols] unmarshal FlashNodeCacheVolsResponse from %v failed: %v", t.OperatorAddr, err)
				return
			}
			log.LogDebugf("action[queryCacheVols] response %v from %v ", response, t.OperatorAddr)
			mu.Lock()
			for vol, cacheSize := range response.VolCacheSizeMap {
				resultMap[vol] += cacheSize
			}
			mu.Unlock()
		}(task)
	}

	wg.Wait()
	return resultMap, nil
}
