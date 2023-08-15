// Copyright 2018 The CubeFS Authors.
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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type flashNodeValue struct {
	ID           uint64
	Addr         string
	ZoneName     string
	Version      string
	FlashGroupID uint64 // 0: have not allocated to flash group
	IsEnable     bool
}

type FlashNode struct {
	flashNodeValue
	ReportTime  time.Time
	IsActive    bool
	TaskManager *AdminTaskManager
	sync.RWMutex
}

func newFlashNode(addr, zoneName, clusterID, version string, isEnable bool) *FlashNode {
	node := new(FlashNode)
	node.Addr = addr
	node.ZoneName = zoneName
	node.Version = version
	node.IsEnable = isEnable
	node.TaskManager = newAdminTaskManager(addr, zoneName, clusterID)
	return node
}

func (flashNode *FlashNode) isFlashNodeUnused() bool {
	return flashNode.FlashGroupID == unusedFlashNodeFlashGroupID
}

func (flashNode *FlashNode) clean() {
	flashNode.TaskManager.exitCh <- struct{}{}
}

func (flashNode *FlashNode) setNodeActive() {
	flashNode.Lock()
	defer flashNode.Unlock()
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
}
func (flashNode *FlashNode) isWriteAble() (ok bool) {
	flashNode.RLock()
	defer flashNode.RUnlock()
	if flashNode.isFlashNodeUnused() && time.Since(flashNode.ReportTime) < time.Second*time.Duration(defaultNodeTimeOutSec) {
		ok = true
	}
	return
}

func (flashNode *FlashNode) getFlashNodeViewInfo() (info *proto.FlashNodeViewInfo) {
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
	return
}

func (c *Cluster) syncProcessCheckFlashNodeHeartbeatTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		node, err := c.flashNode(t.OperatorAddr)
		if err != nil {
			log.LogWarn(fmt.Sprintf("action[syncProcessCheckFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		if _, err = node.TaskManager.syncSendAdminTask(t); err != nil {
			log.LogError(fmt.Sprintf("action[syncProcessCheckFlashNodeHeartbeatTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
			continue
		}
		node.ReportTime = time.Now()
		node.IsActive = true
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
	go c.syncProcessCheckFlashNodeHeartbeatTasks(tasks)
}
func (flashNode *FlashNode) checkLiveliness() {
	flashNode.Lock()
	defer flashNode.Unlock()
	if time.Since(flashNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		flashNode.IsActive = false
	}
}
func (flashNode *FlashNode) createHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpFlashNodeHeartbeat, flashNode.Addr, request)
	return
}
func (c *Cluster) addFlashNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.flashNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
		} else {
			node.TaskManager.AddTask(t)
		}
	}
}
func (flashNode *FlashNode) updateMetric(resp *proto.FlashNodeHeartbeatResponse) {
	flashNode.Lock()
	defer flashNode.Unlock()
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	flashNode.Version = resp.Version
	flashNode.ZoneName = resp.ZoneName
}

func (m *Server) addFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		zoneName string
		version  string
		id       uint64
		err      error
	)
	metrics := exporter.NewModuleTP(proto.AddFlashNodeUmpKey)
	defer func() { metrics.Set(err) }()
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
	c.flashNodeTopo.flashNodesMutex.Lock()
	defer c.flashNodeTopo.flashNodesMutex.Unlock()
	var flashNode *FlashNode
	flashNode, err = c.flashNode(nodeAddr)
	if err == nil {
		return flashNode.ID, nil
	}

	flashNode = newFlashNode(nodeAddr, zoneName, c.Name, version, defaultFlashNodeOnlineState)
	_, err = c.flashNodeTopo.getZone(zoneName)
	if err != nil {
		c.flashNodeTopo.putZoneIfAbsent(newFlashNodeZone(zoneName))
	}
	// allocate flashNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	flashNode.ID = id
	flashNode.ZoneName = zoneName
	if err = c.syncAddFlashNode(flashNode); err != nil {
		goto errHandler
	}
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	c.flashNodeTopo.putFlashNode(flashNode)
	log.LogInfof("action[addFlashNode],clusterID[%v] Addr:%v ZoneName:%v success",
		c.Name, nodeAddr, zoneName)
	return
errHandler:
	log.LogErrorf("action[addFlashNode],clusterID[%v] Addr:%v err:%v ", c.Name, nodeAddr, err.Error())
	return
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
	if version = r.FormValue(versionKey); version == "" {
		version = defaultMetaNodeVersion
	}
	return
}

func (m *Server) getAllFlashNodes(w http.ResponseWriter, r *http.Request) {
	metrics := exporter.NewModuleTP(proto.GetAllFlashNodesUmpKey)
	defer func() { metrics.Set(nil) }()
	zoneFlashNodes := make(map[string][]*proto.FlashNodeViewInfo)
	getAllFlashNodes := extractGetAllFlashNodes(r)
	m.cluster.flashNodeTopo.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if getAllFlashNodes {
			zoneFlashNodes[flashNode.ZoneName] = append(zoneFlashNodes[flashNode.ZoneName], flashNode.getFlashNodeViewInfo())
			return true
		}

		if flashNode.isActiveAndEnable() {
			zoneFlashNodes[flashNode.ZoneName] = append(zoneFlashNodes[flashNode.ZoneName], flashNode.getFlashNodeViewInfo())
		}

		return true
	})
	sendOkReply(w, r, newSuccessHTTPReply(zoneFlashNodes))
}

func (m *Server) getFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr      string
		flashNode     *FlashNode
		flashNodeInfo *proto.FlashNodeViewInfo
		err           error
	)
	metrics := exporter.NewModuleTP(proto.GetFlashNodeUmpKey)
	defer func() { metrics.Set(err) }()
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if flashNode, err = m.cluster.flashNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	flashNodeInfo = flashNode.getFlashNodeViewInfo()
	sendOkReply(w, r, newSuccessHTTPReply(flashNodeInfo))
}

func (m *Server) decommissionFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		node        *FlashNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	metrics := exporter.NewModuleTP(proto.DecommissionFlashNodeUmpKey)
	defer func() { metrics.Set(err) }()
	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.flashNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.decommissionFlashNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission flash node [%v] successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (c *Cluster) decommissionFlashNode(flashNode *FlashNode) (err error) {
	msg := fmt.Sprintf("action[decommissionFlashNode], Node[%v] FlashGroupID:%v ZoneName:%v OffLine", flashNode.Addr, flashNode.FlashGroupID, flashNode.ZoneName)
	log.LogWarn(msg)
	flashGroupID := flashNode.FlashGroupID
	if err = c.deleteFlashNode(flashNode); err != nil {
		return
	}
	if flashGroupID != unusedFlashNodeFlashGroupID {
		var flashGroup *FlashGroup
		if flashGroup, err = c.flashNodeTopo.getFlashGroup(flashGroupID); err != nil {
			return
		}
		flashGroup.removeFlashNode(flashNode.Addr)
		err = c.selectFlashNodesFromZoneAddToFlashGroup(flashNode.ZoneName, 1, []string{flashNode.Addr}, flashGroup)
		if err != nil {
			return
		}
	}
	msg = fmt.Sprintf("action[decommissionFlashNode],clusterID[%v] node[%v] flashGroupID[%v] offLine success",
		c.Name, flashNode.Addr, flashGroupID)
	Warn(c.Name, msg)
	return
}
func (c *Cluster) deleteFlashNode(flashNode *FlashNode) (err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	oldFlashGroupID := flashNode.FlashGroupID
	flashNode.FlashGroupID = unusedFlashNodeFlashGroupID
	if err = c.syncDeleteFlashNode(flashNode); err != nil {
		msg := fmt.Sprintf("action[deleteFlashNode],clusterID[%v] node[%v] offline failed,err[%v]",
			c.Name, flashNode.Addr, err)
		Warn(c.Name, msg)
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

func (c *Cluster) flashNode(addr string) (flashNode *FlashNode, err error) {
	value, ok := c.flashNodeTopo.flashNodeMap.Load(addr)
	if !ok {
		err = errors.Trace(flashNodeNotFound(addr), "%v not found", addr)
		return
	}
	flashNode = value.(*FlashNode)
	return
}
func flashNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("flash node[%v]", addr))
}

func (c *Cluster) loadFlashGroups() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashGroupPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashGroups],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		var fgv flashGroupValue
		if err = json.Unmarshal(value, &fgv); err != nil {
			err = fmt.Errorf("action[loadFlashGroups],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashGroup := newFlashGroup(fgv.ID, fgv.Slots, fgv.Status)
		c.flashNodeTopo.flashGroupMap.Store(flashGroup.ID, flashGroup)
		for _, slot := range flashGroup.Slots {
			c.flashNodeTopo.slotsMap[slot] = flashGroup.ID
		}
		log.LogInfof("action[loadFlashGroups],flashGroup[%v]", flashGroup.ID)
	}
	return
}

func (c *Cluster) loadFlashNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashNodes],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		fnv := &flashNodeValue{}
		if err = json.Unmarshal(value, fnv); err != nil {
			err = fmt.Errorf("action[loadFlashNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashNode := newFlashNode(fnv.Addr, fnv.ZoneName, c.Name, fnv.Version, fnv.IsEnable)
		flashNode.ID = fnv.ID
		flashNode.FlashGroupID = fnv.FlashGroupID

		if !flashNode.isFlashNodeUnused() {
			if flashGroup, err1 := c.flashNodeTopo.getFlashGroup(flashNode.FlashGroupID); err1 == nil {
				flashGroup.putFlashNode(flashNode)
			} else {
				log.LogErrorf("action[loadFlashNodes]fnv:%v err:%v", *fnv, err1.Error())
			}
		}

		_, err = c.flashNodeTopo.getZone(flashNode.ZoneName)
		if err != nil {
			c.flashNodeTopo.putZoneIfAbsent(newFlashNodeZone(flashNode.ZoneName))
		}
		c.flashNodeTopo.putFlashNode(flashNode)
		log.LogInfof("action[loadFlashNodes],flashNode[%v],FlashGroupID[%v]", flashNode.Addr, flashNode.FlashGroupID)
	}
	return
}

func (m *Server) setFlashNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  string
		state     bool
		flashNode *FlashNode
		err       error
	)
	metrics := exporter.NewModuleTP(proto.SetFlashNodeUmpKey)
	defer func() { metrics.Set(err) }()
	if nodeAddr, state, err = parseAndExtractFlashNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if flashNode, err = m.cluster.flashNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if flashNode.IsEnable != state {
		oldState := flashNode.IsEnable
		flashNode.IsEnable = state
		if err = m.cluster.syncUpdateFlashNode(flashNode); err != nil {
			flashNode.IsEnable = oldState
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply("set flashNode success"))
}

func (flashNode *FlashNode) isActiveAndEnable() bool {
	if flashNode.IsActive && flashNode.IsEnable {
		return true
	}
	return false
}
