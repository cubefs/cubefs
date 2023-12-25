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
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
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
	FlashGroupID uint64 // 0: have not allocated to flash group
	IsEnable     bool
}

type FlashNode struct {
	TaskManager *AdminTaskManager

	sync.RWMutex
	flashNodeValue
	ReportTime time.Time
	IsActive   bool
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
		ID:           flashNode.ID,
		Addr:         flashNode.Addr,
		ReportTime:   flashNode.ReportTime,
		IsActive:     flashNode.IsActive,
		Version:      flashNode.Version,
		ZoneName:     flashNode.ZoneName,
		FlashGroupID: flashNode.FlashGroupID,
		IsEnable:     flashNode.IsEnable,
	}
	flashNode.RUnlock()
	return
}

// TODO: sync with proto.FlashNodeHeartbeatResponse.
func (c *Cluster) syncFlashNodeHeartbeatTasks(tasks []*proto.AdminTask) {
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
			log.LogError(fmt.Sprintf("action[syncFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		node.setActive()
	}
}

func (c *Cluster) checkFlashNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.flashNodeTopo.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.checkLiveliness()
		task := node.createHeartbeatTask(c.masterAddr())
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

func (flashNode *FlashNode) createHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpFlashNodeHeartbeat, flashNode.Addr, request)
	return
}

func (m *Server) addFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		zoneName string
		version  string
		id       uint64
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeAdd))
	defer func() {
		doStatAndMetric(proto.FlashNodeAdd, metric, err, nil)
	}()
	if nodeAddr, zoneName, version, err = parseRequestForAddFlashNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if id, err = m.cluster.addFlashNode(nodeAddr, zoneName, version); err != nil {
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
	listAll := extractListAll(r)
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if listAll || flashNode.isActiveAndEnable() {
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
	var nodeAddr string
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var flashNode *FlashNode
	if flashNode, err = m.cluster.peekFlashNode(nodeAddr); err != nil {
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
	var offLineAddr string
	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var node *FlashNode
	if node, err = m.cluster.peekFlashNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.removeFlashNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("delete flash node [%v] successfully", offLineAddr)))
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
		err = c.selectFlashNodesFromZoneAddToFlashGroup(flashNode.ZoneName, 1, []string{flashNode.Addr}, flashGroup)
		c.flashNodeTopo.updateClientCache()
		if err != nil {
			return
		}
	}
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
		nodeAddr  string
		state     bool
		flashNode *FlashNode
		err       error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.FlashNodeSet))
	defer func() {
		doStatAndMetric(proto.FlashNodeSet, metric, err, nil)
	}()
	if nodeAddr, state, err = parseAndExtractFlashNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if flashNode, err = m.cluster.peekFlashNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.updateFlashNode(flashNode, state); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply("set flashNode success"))
}

func (c *Cluster) updateFlashNode(flashNode *FlashNode, state bool) (err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.IsEnable != state {
		oldState := flashNode.IsEnable
		flashNode.IsEnable = state
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

func (c *Cluster) peekFlashNode(addr string) (flashNode *FlashNode, err error) {
	value, ok := c.flashNodeTopo.flashNodeMap.Load(addr)
	if !ok {
		err = errors.Trace(flashNodeNotFound(addr), "%v not found", addr)
		return
	}
	flashNode = value.(*FlashNode)
	return
}

func flashNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("flashnode[%v]", addr))
}

func parseRequestForAddFlashNode(r *http.Request) (nodeAddr, zoneName, version string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	version = r.FormValue(versionKey)
	return
}

func parseAndExtractFlashNode(r *http.Request) (nodeAddr string, state bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	state, err = strconv.ParseBool(r.FormValue(stateKey))
	return
}

func extractListAll(r *http.Request) (all bool) {
	all, _ = strconv.ParseBool(r.FormValue("all"))
	return
}
