// Copyright 2018 The Chubao Authors.
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
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

type ECNode struct {
	DataNode
	MaxDiskAvailSpace     uint64
	EcPartitionReports []*proto.EcPartitionReport
}

type EcNodeValue struct {
	ID              uint64
	Addr            string
	HttpPort        string
	ZoneName        string
	Version         string
	EcScrubPeriod   uint8 //day
	EcMaxScrubDpNum uint8 //max handle scrub dp num same time
	EcMaxRecoverDp  uint8 //max concurrent recover dp num
}

func newEcNode(addr, httpPort, clusterID, zoneName, version string) *ECNode {
	node := new(ECNode)
	node.Addr = addr
	node.Carry = rand.Float64()
	node.HttpPort = httpPort
	node.ZoneName = zoneName
	node.Version  = version
	node.TaskManager = newAdminTaskManager(addr, zoneName, clusterID)
	return node
}

func newEcNodeValue(node *ECNode) *EcNodeValue {
	return &EcNodeValue{
		ID:       node.ID,
		Addr:     node.Addr,
		HttpPort: node.HttpPort,
		ZoneName: node.ZoneName,
		Version:  node.Version,
	}
}

func (ecnode *ECNode) createHeartbeatTask(masterAddr, targetAddr string, opType uint8) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(opType, targetAddr, request)
	return
}

func (ecNode *ECNode) updateMetric(resp *proto.EcNodeHeartbeatResponse) {
	ecNode.Lock()
	defer ecNode.Unlock()
	ecNode.Total = resp.Total
	ecNode.Used = resp.Used
	ecNode.AvailableSpace = resp.Available
	ecNode.MaxDiskAvailSpace = resp.MaxCapacity
	ecNode.DataPartitionCount = resp.CreatedPartitionCnt
	ecNode.EcPartitionReports = resp.PartitionReports
	ecNode.HttpPort = resp.HttpPort
	ecNode.ZoneName = resp.CellName
	ecNode.BadDisks = resp.BadDisks
	if ecNode.Total == 0 {
		ecNode.UsageRatio = 0.0
	} else {
		ecNode.UsageRatio = (float64)(ecNode.Used) / (float64)(ecNode.Total)
	}
	ecNode.ReportTime = time.Now()
	ecNode.isActive = true
	ecNode.Version = resp.Version
}

func (m *Server) getEcNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr   string
		node       *ECNode
		ecNodeInfo *proto.EcNodeInfo
		err        error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.ecNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrCodecNodeNotExists))
		return
	}

	node.PersistenceDataPartitions = m.cluster.getAllEcPartitionIDByEcnode(nodeAddr)
	ecNodeInfo = &proto.EcNodeInfo{
		Total:                     node.Total,
		Used:                      node.Used,
		AvailableSpace:            node.AvailableSpace,
		MaxDiskAvailSpace:         node.MaxDiskAvailSpace,
		ID:                        node.ID,
		ZoneName:                  node.ZoneName,
		Addr:                      node.Addr,
		HttpPort:                  node.HttpPort,
		ReportTime:                node.ReportTime,
		IsActive:                  node.isActive,
		UsageRatio:                node.UsageRatio,
		SelectedTimes:             node.SelectedTimes,
		Carry:                     node.Carry,
		DataPartitionReports:      node.DataPartitionReports,
		DataPartitionCount:        node.DataPartitionCount,
		PersistenceDataPartitions: node.PersistenceDataPartitions,
		BadDisks:                  node.BadDisks,
		ToBeOffline:               node.ToBeOffline,
		ToBeMigrated:              node.ToBeMigrated,
		Version:                   node.Version,
	}

	sendOkReply(w, r, newSuccessHTTPReply(ecNodeInfo))
}

func (m *Server) addEcNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		httpPort string
		zoneName string
		version  string
		id       uint64
		err      error
	)

	if nodeAddr, httpPort, zoneName, version, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if id, err = m.cluster.addEcNode(nodeAddr, httpPort, zoneName, version); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(id))
}

// Decommission a ec node
func (m *Server) decommissionEcNode(w http.ResponseWriter, r *http.Request) {
	var (
		node        *ECNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.ecNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.decommissionEcNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission ec node [%v] successfully", offLineAddr)
	log.LogInfof(rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Decommission a disk. This will decommission all the data partitions on this disk.
func (m *Server) decommissionEcDisk(w http.ResponseWriter, r *http.Request) {
	var (
		node                  *ECNode
		offLineAddr, diskPath string
		err                   error
		badPartitionIds       []uint64
		badPartitions         []*EcDataPartition
	)

	if offLineAddr, diskPath, err = parseRequestToDecommissionNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.ecNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	badPartitions = node.badPartitions(diskPath, m.cluster)
	if len(badPartitions) == 0 {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("node[%v] no partitions on disk[%v],successfully", node.Addr, diskPath)))
		return
	}
	for _, bdp := range badPartitions {
		badPartitionIds = append(badPartitionIds, bdp.PartitionID)
	}

	if err = m.cluster.decommissionEcDisk(node, diskPath, badPartitions); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("node[%v] disk[%v], badPartitionIds[%v] has offline", node.Addr, diskPath, badPartitionIds)))
}

func (m *Server) handleEcNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleEcNodeTaskResponse(tr.OperatorAddr, tr)
}

func (ecNode *ECNode) checkLiveness() {
	ecNode.Lock()
	defer ecNode.Unlock()
	if time.Since(ecNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		ecNode.isActive = false
	}
	return
}

func (c *Cluster) checkEcNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.ecNodes.Range(func(addr, ecNode interface{}) bool {
		node := ecNode.(*ECNode)
		node.checkLiveness()
		task := node.createHeartbeatTask(c.masterAddr(), node.Addr, proto.OpEcNodeHeartbeat)
		tasks = append(tasks, task)
		return true
	})
	c.addEcNodeTasks(tasks)
}

func (c *Cluster) allEcNodes() (ecNodes []proto.NodeView) {
	ecNodes = make([]proto.NodeView, 0)
	c.ecNodes.Range(func(key, value interface{}) bool {
		ecNode, ok := value.(*ECNode)
		if !ok {
			return true
		}
		ecNodes = append(ecNodes, proto.NodeView{Addr: ecNode.Addr, Status: ecNode.isActive, ID: ecNode.ID, IsWritable: ecNode.isWriteAble(), Version: ecNode.Version})
		return true
	})
	return
}

func (c *Cluster) addEcNode(nodeAddr, httpPort, zoneName, version string) (id uint64, err error) {
	c.enMutex.Lock()
	defer c.enMutex.Unlock()
	var ecNode *ECNode
	if node, ok := c.ecNodes.Load(nodeAddr); ok {
		ecNode = node.(*ECNode)
		return ecNode.ID, nil
	}

	ecNode = newEcNode(nodeAddr, httpPort, c.Name, zoneName, version)
	_, err = c.t.getZone(zoneName)
	if err != nil {
		c.t.putZoneIfAbsent(newZone(zoneName))
	}
	// allocate ecNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	ecNode.ID = id
	ecNode.ZoneName = zoneName
	if err = c.syncAddEcNode(ecNode); err != nil {
		goto errHandler
	}
	c.t.putEcNode(ecNode)
	c.ecNodes.Store(nodeAddr, ecNode)
	log.LogInfof("action[addEcNode],clusterID[%v] ecNodeAddr:%v success",
		c.Name, nodeAddr)
	return
errHandler:
	log.LogErrorf("action[addEcNode],clusterID[%v] ecNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	return
}
func (ecNode *ECNode) badPartitions(diskPath string, c *Cluster) (partitions []*EcDataPartition) {
	partitions = make([]*EcDataPartition, 0)
	vols := c.copyVols()
	if len(vols) == 0 {
		return partitions
	}
	for _, vol := range vols {
		dps := vol.ecDataPartitions.checkBadDiskEcDataPartitions(diskPath, ecNode.Addr)
		partitions = append(partitions, dps...)
	}
	return
}

func (c *Cluster) decommissionEcNode(ecNode *ECNode) (err error) {
	msg := fmt.Sprintf("action[decommissionEcNode], Node[%v] OffLine", ecNode.Addr)
	log.LogWarn(msg)
	var wg sync.WaitGroup
	ecNode.ToBeOffline = true
	partitions := c.getAllEcDataPartitionByEcNode(ecNode.Addr)
	errChannel := make(chan error, len(partitions))
	defer func() {
		if err != nil {
			ecNode.ToBeOffline = false
		}
		close(errChannel)
	}()
	for _, ecdp := range partitions {
		wg.Add(1)
		go func(ecdp *EcDataPartition) {
			defer wg.Done()
			if err1 := c.decommissionEcDataPartition(ecNode.Addr, ecdp, ""); err1 != nil {
				errChannel <- err1
			}
		}(ecdp)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		return
	default:
	}
	if err = c.syncDeleteEcNode(ecNode); err != nil {
		return
	}
	c.delEcNodeFromCache(ecNode)
	go ecNode.clean()
	log.LogInfof("action[decommissionEcNode], Node[%v] OffLine success", ecNode.Addr)
	return
}

func (c *Cluster) delEcNodeFromCache(ecNode *ECNode) {
	c.ecNodes.Delete(ecNode.Addr)
	c.t.deleteEcNode(ecNode)
	go ecNode.clean()
}

func (c *Cluster) adjustEcNode(ecNode *ECNode) {
	c.enMutex.Lock()
	defer c.enMutex.Unlock()
	var err error
	defer func() {
		if err != nil {
			log.LogError(errors.Stack(err))
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(ecNode.ZoneName)
	if err != nil {
		zone = newZone(ecNode.ZoneName)
		c.t.putZone(zone)
	}
	if err = c.syncUpdateEcNode(ecNode); err != nil {
		return
	}
	err = c.t.putEcNode(ecNode)
	return
}

func (c *Cluster) loadEcNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(ecNodePrefix))
	if err != nil {
		return err
	}

	for _, value := range result {
		ecnv := &EcNodeValue{}
		if err = json.Unmarshal(value, ecnv); err != nil {
			return
		}
		if ecnv.ZoneName == "" {
			ecnv.ZoneName = DefaultZoneName
		}
		ecNode := newEcNode(ecnv.Addr, ecnv.HttpPort, c.Name, ecnv.ZoneName, ecnv.Version)
		zone, err := c.t.getZone(ecnv.ZoneName)
		if err != nil {
			zone = newZone(ecnv.ZoneName)
			c.t.putZoneIfAbsent(zone)
		}
		ecNode.ID = ecnv.ID
		c.ecNodes.Store(ecNode.Addr, ecNode)
		log.LogInfof("action[loadEcNodes],EcNode[%v],ZoneName[%v]", ecNode.Addr, ecNode.ZoneName)
	}
	return
}

func (c *Cluster) syncAddEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncAddEcNode, ecNode)
}

func (c *Cluster) syncDeleteEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncDeleteEcNode, ecNode)
}

func (c *Cluster) syncUpdateEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncUpdateEcNode, ecNode)
}

func (c *Cluster) handleEcNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[handleEcNodeTaskResponse] receive Task response:%v from %v", task.ID, nodeAddr))
	var (
		ecNode *ECNode
	)

	if ecNode, err = c.ecNode(nodeAddr); err != nil {
		goto errHandler
	}
	ecNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpEcNodeHeartbeat:
		response := task.Response.(*proto.EcNodeHeartbeatResponse)
		err = c.dealEcNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		log.LogErrorf("unknown operate code %v", task.OpCode)
	}

	if err == nil {
		log.LogInfof("process task:%v status:%v success", task.ID, task.Status)
	}
	return
errHandler:
	log.LogError(fmt.Sprintf("action[handleEcNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealEcNodeHeartbeatResp(nodeAddr string, resp *proto.EcNodeHeartbeatResponse) (err error) {
	var (
		ecNode *ECNode
	)
	log.LogInfof("action[dealEcNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFailed {
		return
	}

	if ecNode, err = c.ecNode(nodeAddr); err != nil {
		goto errHandler
	}
	if resp.CellName == "" {
		resp.CellName = DefaultZoneName
	}
	if ecNode.ZoneName != resp.CellName {
		c.t.deleteEcNode(ecNode)
		ecNode.ZoneName = resp.CellName
		c.adjustEcNode(ecNode)
	}

	ecNode.updateMetric(resp)
	if err = c.t.putEcNode(ecNode); err != nil {
		log.LogErrorf("action[dealEcNodeHeartbeatResp] ecNode[%v],zone[%v], err[%v]", ecNode.Addr, ecNode.ZoneName, err)
	}
	c.updateEcNode(ecNode, resp.PartitionReports)
	log.LogInfof("action[dealEcNodeHeartbeatResp],ecNode:%v ReportTime:%v  success", ecNode.Addr, time.Now().Unix())
	return
errHandler:
	log.LogErrorf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	return
}

func (ecNode *ECNode) isWriteAble() (ok bool) {
	ecNode.RLock()
	defer ecNode.RUnlock()

	if ecNode.isActive == true && ecNode.AvailableSpace > 100*util.GB && ecNode.ToBeOffline == false && ecNode.ToBeMigrated == false{
		ok = true
	}

	return
}

func (ecNode *ECNode) canCreateEcPartition(needAllocSpace uint64) (ok bool) {
	ecNode.RLock()
	defer ecNode.RUnlock()
	if ecNode.isActive == true && needAllocSpace < ecNode.MaxDiskAvailSpace {
		ok = true
	}
	return
}

func (ecNode *ECNode) isAvailCarryNode() (ok bool) {
	ecNode.RLock()
	defer ecNode.RUnlock()

	return ecNode.Carry >= 1
}

// SetCarry implements "SetCarry" in the Node interface
func (ecNode *ECNode) SetCarry(carry float64, storeMode proto.StoreMode) {
	ecNode.Lock()
	defer ecNode.Unlock()
	ecNode.Carry = carry
}

// SelectNodeForWrite implements "SelectNodeForWrite" in the Node interface
func (ecNode *ECNode) SelectNodeForWrite(storeMode proto.StoreMode) {
	ecNode.Lock()
	defer ecNode.Unlock()
	ecNode.UsageRatio = float64(ecNode.Used) / float64(ecNode.Total)
	ecNode.SelectedTimes++
	ecNode.Carry = ecNode.Carry - 1.0
}

func (ecNode *ECNode) GetID() uint64 {
	ecNode.RLock()
	defer ecNode.RUnlock()
	return ecNode.ID
}

func (ecNode *ECNode) GetAddr() string {
	ecNode.RLock()
	defer ecNode.RUnlock()
	return ecNode.Addr
}

func extractEcScrubEnable(r *http.Request) (ecScrubEnable bool, err error) {
	var value string
	if value = r.FormValue(ecScrubEnableKey); value == "" {
		ecScrubEnable = false
		return
	}
	if ecScrubEnable, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func (m *Server) updateEcClusterInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err                error
		msg                string
		ecScrubEnable      bool
		ecMaxScrubExtents  int
		ecScrubPeriod      int
		maxCodecConcurrent int
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	var value string
	if value = r.FormValue(ecScrubEnableKey); value == "" {
		ecScrubEnable = m.cluster.EcScrubEnable
	}else {
		if ecScrubEnable, err = strconv.ParseBool(value); err != nil {
			return
		}
	}


	if ecMaxScrubExtentsStr := r.FormValue(ecMaxScrubExtentsKey); ecMaxScrubExtentsStr != "" {
		if ecMaxScrubExtents, err = strconv.Atoi(ecMaxScrubExtentsStr); err != nil {
			err = unmatchedKey(ecMaxScrubExtentsKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		ecMaxScrubExtents = int(m.cluster.EcMaxScrubExtents)
	}

	if maxCodecConcurrentStr := r.FormValue(maxCodecConcurrentKey); maxCodecConcurrentStr != "" {
		if maxCodecConcurrent, err = strconv.Atoi(maxCodecConcurrentStr); err != nil {
			err = unmatchedKey(maxCodecConcurrentKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		maxCodecConcurrent = m.cluster.MaxCodecConcurrent
	}

	if ecScrubPeriodStr := r.FormValue(ecScrubPeriodKey); ecScrubPeriodStr != "" {
		if ecScrubPeriod, err = strconv.Atoi(ecScrubPeriodStr); err != nil {
			err = unmatchedKey(ecScrubPeriodKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		ecScrubPeriod = int(m.cluster.EcScrubPeriod)
	}

	if err = m.cluster.updateEcClusterInfo(ecScrubEnable, ecMaxScrubExtents, ecScrubPeriod, maxCodecConcurrent); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update Ec scrub successfully\n")
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) getEcScrubInfo(w http.ResponseWriter, r *http.Request) {
	scrubInfo := &proto.UpdateEcScrubInfoRequest{
		ScrubEnable:     m.cluster.EcScrubEnable,
		MaxScrubExtents: m.cluster.EcMaxScrubExtents,
		ScrubPeriod:     m.cluster.EcScrubPeriod,
	}
	ecScrubPeriod := int64(m.cluster.EcScrubPeriod) * defaultEcScrubPeriodTime
	if time.Now().Unix()-m.cluster.EcStartScrubTime >= ecScrubPeriod && m.cluster.EcScrubEnable {
		m.cluster.EcStartScrubTime = time.Now().Unix()
		log.LogDebugf("getEcScrubInfo updateTime")
		if err := m.cluster.syncPutCluster(); err != nil {
			log.LogErrorf("persist cluster EcStartScrubTime err[%v]", err)
		}
	}
	scrubInfo.StartScrubTime = m.cluster.EcStartScrubTime
	sendOkReply(w, r, newSuccessHTTPReply(scrubInfo))
}

func extractEcEnable(r *http.Request, vol *Vol) (enableEc bool, err error) {
	var value string
	if value = r.FormValue(ecEnableKey); value == "" {
		enableEc = vol.EcEnable
		return
	}
	if enableEc, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}
func (m *Server) updateVolEcInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err           error
		name          string
		msg           string
		ecEnable      bool
		ecSaveTime    int64
		ecWaitTime    int64
		ecTimeOut     int64
		ecRetryWait   int64
		ecMaxUnitSize uint64
		vol           *Vol
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	if ecEnable, err = extractEcEnable(r, vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if ecMaxUnitSizeStr := r.FormValue(ecMaxUnitSizeKey); ecMaxUnitSizeStr != "" {
		if ecMaxUnitSize, err = strconv.ParseUint(ecMaxUnitSizeStr, 10, 64); err != nil {
			err = unmatchedKey(ecMaxUnitSizeKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		ecMaxUnitSize = vol.EcMaxUnitSize
	}

	if ecSaveTimeStr := r.FormValue(ecSaveTimeKey); ecSaveTimeStr != "" {
		if ecSaveTime, err = strconv.ParseInt(ecSaveTimeStr, 10, 64); err != nil {
			err = unmatchedKey(ecSaveTimeKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		ecSaveTime = vol.EcMigrationSaveTime
	}

	if ecWaitTimeStr := r.FormValue(ecWaitTimeKey); ecWaitTimeStr != "" {
		if ecWaitTime, err = strconv.ParseInt(ecWaitTimeStr, 10, 64); err != nil {
			err = unmatchedKey(ecWaitTimeKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		ecWaitTime = vol.EcMigrationWaitTime
	}

	if ecTimeOutStr := r.FormValue(ecTimeOutKey); ecTimeOutStr != "" {
		if ecTimeOut, err = strconv.ParseInt(ecTimeOutStr, 10, 64); err != nil {
			err = unmatchedKey(ecTimeOutKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		ecTimeOut = vol.EcMigrationTimeOut
	}

	if ecRetryWaitStr := r.FormValue(ecRetryWaitKey); ecRetryWaitStr != "" {
		if ecRetryWait, err = strconv.ParseInt(ecRetryWaitStr, 10, 64); err != nil {
			err = unmatchedKey(ecRetryWaitKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}else {
		ecRetryWait = vol.EcMigrationRetryWait
	}

	if err = m.cluster.updateVolEcInfo(name, ecEnable, defaultEcDataNum, defaultEcParityNum, ecSaveTime, ecWaitTime, ecTimeOut, ecRetryWait, uint64(ecMaxUnitSize)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) GetCanMigrateDp(w http.ResponseWriter, r *http.Request) {
	canOperDps := m.cluster.getCanOperDataPartitions(false)
	sendOkReply(w, r, newSuccessHTTPReply(canOperDps))
}

func (m *Server) GetCanDelDp(w http.ResponseWriter, r *http.Request) {
	canOperDps := m.cluster.getCanOperDataPartitions(true)
	sendOkReply(w, r, newSuccessHTTPReply(canOperDps))
}

func (m *Server) DelDpAlreadyEc(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ID  uint64
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if ID, err = extractPartitionID(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.delDpAlreadyEc(ID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg := fmt.Sprintf("dp partitionID :%v  delete successfully", ID)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) DpMigrateEcById(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		id  uint64
		test bool
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if id, err = extractPartitionID(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if test, err = extractTest(r); err != nil {
		test = false
	}

	if err = m.cluster.ecMigrateById(id, test); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg := fmt.Sprintf("dp partitionID :%v  migrate ec successfully", id)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func parseRequestToRemoveEcReplica(r *http.Request) (partitionID uint64, nodeAddr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func parseRequestToAddEcReplica(r *http.Request) (partitionID uint64, nodeAddr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func (m *Server) deleteEcDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		addr        string
		partitionID uint64
		err         error
		ep          *EcDataPartition
	)

	if partitionID, addr, err = parseRequestToRemoveEcReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogDebugf("deleteEcDataReplica addr(%v) partitionID(%v)", addr, partitionID)
	if ep, err = m.cluster.getEcPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeEcPartitionNotExists, Msg: err.Error()})
		return
	}

	if err = m.cluster.removeEcDataReplica(ep, addr, true); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("ec partitionID :%v  del replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) addEcDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		addr        string
		partitionID uint64
		err         error
		ep          *EcDataPartition
	)

	if partitionID, addr, err = parseRequestToAddEcReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if ep, err = m.cluster.getEcPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	if len(ep.Hosts) >= int(ep.DataUnitsNum+ep.ParityUnitsNum) {
		sendErrReply(w, r, newErrHTTPReply(errors.NewErrorf("addEcDataReplica fail: len(Hosts)(%v) >= shardsNum(%v)",
			len(ep.Hosts), ep.DataUnitsNum+ep.ParityUnitsNum)))
		return
	}
	newhosts := make([]string, 0, len(ep.Hosts)+1)
	idx := -1
	for i, val := range ep.hostsIdx {
		if val {
			continue
		}
		idx = i
		break
	}
	if idx == -1 {
		sendErrReply(w, r, newErrHTTPReply(errors.NewErrorf("addEcDataReplica fail: not find host pos hostsIdx(%v)", ep.hostsIdx)))
		return
	}
	find := false
	for i, host := range ep.Hosts {
		if i == idx {
			find = true
			newhosts = append(newhosts, addr)
		}
		newhosts = append(newhosts, host)
	}

	if !find {//if idx is replicaNum-1, can't find need append handle
		newhosts = append(newhosts, addr)
	}

	if err = m.cluster.addEcReplica(ep, idx, newhosts); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if ep, err = m.cluster.getEcPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	if _, err := m.cluster.syncChangeEcPartitionMembers(ep, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	ep.Lock()
	ep.Status = proto.ReadOnly
	ep.isRecover = true
	ep.PanicHosts = append(ep.PanicHosts, addr)
	m.cluster.syncUpdateEcDataPartition(ep)
	ep.Unlock()
	m.cluster.putBadEcPartitionIDs(nil, addr, ep.PartitionID)
	rstMsg = fmt.Sprintf("ec partitionID :%v  add replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) GetAllTaskStatus(w http.ResponseWriter, r *http.Request) {
	if taskView, err := m.cluster.getAllTaskStatus(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	} else {
		sendOkReply(w, r, newSuccessHTTPReply(taskView))
	}
}

func (m *Server) DpStopMigrating(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ID  uint64
		dp  *DataPartition
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if ID, err = extractPartitionID(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	log.LogInfof("DpStopMigrating partitionId(%v)", ID)
	if _, err = m.cluster.getEcPartitionByID(ID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrEcPartitionNotExists))
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(ID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	if dp.EcMigrateStatus != proto.Migrating {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotMigrating))
		return
	}

	if err = m.cluster.dealStopMigratingTaskById(ID); err != nil {
		log.LogErrorf("dealStopMigratingTaskById err(%v)", err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg := fmt.Sprintf("dp partitionID :%v  stop Migrating successfully", ID)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) DNStopMigrating(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		addr string
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	log.LogInfof("DNStopMigrating addr(%v)", addr)

	if err = m.cluster.dealStopMigratingTaskByNode(addr); err != nil {
		log.LogErrorf("dealStopMigratingTaskByNode err(%v)", err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg := fmt.Sprintf("dataNode :%v  stop Migrating successfully", addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (c *Cluster) setEcNodeToOfflineState(startID, endID uint64, state bool, zoneName string) {
	c.ecNodes.Range(func(key, value interface{}) bool {
		node, ok := value.(*ECNode)
		if !ok {
			return true
		}
		if node.ID < startID || node.ID > endID {
			return true
		}
		if node.ZoneName != zoneName {
			return true
		}
		node.Lock()
		node.ToBeMigrated = state
		node.Unlock()
		return true
	})
}

func (c *Cluster) updateEcClusterInfo(ecScrubEnable bool, ecMaxScrubExtents, ecScrubPeriod, maxCodecConcurrent int) (err error) {
	var (
		oldEcScrubEnbale     bool
		oldEcMaxScrubExtents uint8
		oldEcScrubPeriod     uint32
		oldStartScrubTime    int64
		oldMaxCodecConcurrent int
	)
	oldEcScrubEnbale = c.EcScrubEnable
	oldEcMaxScrubExtents = c.EcMaxScrubExtents
	oldEcScrubPeriod = c.EcScrubPeriod
	oldStartScrubTime = c.EcStartScrubTime
	oldMaxCodecConcurrent = c.MaxCodecConcurrent

	c.EcScrubEnable = ecScrubEnable
	c.EcMaxScrubExtents = uint8(ecMaxScrubExtents)
	c.EcScrubPeriod = uint32(ecScrubPeriod)
	c.MaxCodecConcurrent = maxCodecConcurrent
	if !oldEcScrubEnbale && ecScrubEnable {
		c.EcStartScrubTime = time.Now().Unix()
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[updateEcScrubInfo] err[%v]", err)
		c.EcScrubEnable = oldEcScrubEnbale
		c.EcMaxScrubExtents = oldEcMaxScrubExtents
		c.EcScrubPeriod = oldEcScrubPeriod
		c.EcStartScrubTime = oldStartScrubTime
		c.MaxCodecConcurrent = oldMaxCodecConcurrent
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) updateVolEcInfo(name string, ecEnable bool, ecDataNum, ecParityNum uint8, ecSaveTime, ecWaitTime, ecTimeOut, ecRetryWait int64, ecMaxUnitSize uint64) (err error) {
	var (
		vol                *Vol
		oldEcEnable        bool
		oldEcDataNum       uint8
		oldEcParityNum     uint8
		oldEcSaveTime      int64
		oldEcWaitTime      int64
		oldEcTimeOut       int64
		oldEcRetryWait     int64
		oldEcMaxUnitSize   uint64
	)
	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[updateVolEcInfo] err[%v]", err)
		err = proto.ErrVolNotExists
		goto errHandler
	}
	vol.Lock()
	defer vol.Unlock()
	oldEcEnable = vol.EcEnable
	oldEcDataNum = vol.EcDataNum
	oldEcParityNum = vol.EcParityNum
	oldEcSaveTime = vol.EcMigrationSaveTime
	oldEcWaitTime = vol.EcMigrationWaitTime
	oldEcTimeOut  = vol.EcMigrationTimeOut
	oldEcRetryWait = vol.EcMigrationRetryWait
	oldEcMaxUnitSize = vol.EcMaxUnitSize

	if ecSaveTime < defaultMinEcTime {
		ecSaveTime = defaultMinEcTime
	}//避免客户端还未拉取最新试图，dp已经被删除
	if ecWaitTime < defaultMinEcTime {
		ecWaitTime = defaultMinEcTime
	}//避免dp刚变为只读客户端还未拉取试图就被迁移，造成迁移过程中客户端仍可以对dp进行覆盖写导致迁移数据和dp数据不一致
	if ecRetryWait < defaultMinEcTime {
		ecRetryWait = defaultMinEcTime
	}//避免codec还未上报完成状态再次进行失败重传
	vol.EcEnable = ecEnable
	vol.EcDataNum = ecDataNum
	vol.EcParityNum = ecParityNum
	vol.EcMigrationSaveTime = ecSaveTime
	vol.EcMigrationWaitTime = ecWaitTime
	vol.EcMigrationTimeOut = ecTimeOut
	vol.EcMigrationRetryWait = ecRetryWait
	vol.EcMaxUnitSize = ecMaxUnitSize

	if err = c.syncUpdateVol(vol); err != nil {
		vol.EcEnable = oldEcEnable
		vol.EcDataNum = oldEcDataNum
		vol.EcParityNum = oldEcParityNum
		vol.EcMigrationWaitTime = oldEcWaitTime
		vol.EcMigrationTimeOut = oldEcTimeOut
		vol.EcMigrationSaveTime = oldEcSaveTime
		vol.EcMigrationRetryWait = oldEcRetryWait
		vol.EcMaxUnitSize = oldEcMaxUnitSize
		log.LogErrorf("action[updateVol] vol[%v] err[%v]", name, err)
		err = proto.ErrPersistenceByRaft
		goto errHandler
	}
	return
errHandler:
	err = fmt.Errorf("action[updateVolEcInfo], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

// if oper is true, return can delete dp; if oper is false, return can migrate dp
func (c *Cluster) getCanOperDataPartitions(oper bool) []*proto.DataPartitionResponse {
	dpCanOper := make([]*proto.DataPartitionResponse, 0)
	vols := c.allVols()
	for _, vol := range vols {
		dataPartitions := vol.getCanOperDataPartitions(oper)
		dpCanOper = append(dpCanOper, dataPartitions...)
	}

	return dpCanOper
}

func (c *Cluster) ecNode(addr string) (ecNode *ECNode, err error) {
	value, ok := c.ecNodes.Load(addr)
	if !ok {
		err = errors.Trace(dataNodeNotFound(addr), "%v not found", addr)
		return
	}
	ecNode = value.(*ECNode)
	return
}

func (c *Cluster) chooseTargetEcNodes(excludeZone string, excludeHosts []string, replicaNum int, zoneNum int, needAllocSpace uint64) (hosts []string, err error) {
	var (
		zones          []*Zone
		hasAllocateNum int
	)
	excludeZones := make([]string, 0)
	zones = make([]*Zone, 0)

	if excludeZone != "" {
		excludeZones = append(excludeZones, excludeZone)
	}
	zones, err = c.t.allocZonesForEcNode(zoneNum, replicaNum, excludeZones)
	if err != nil {
		return
	}
	for hasAllocateNum < replicaNum {
		for _, zone := range zones {
			selectedHosts, _, e := zone.getAvailEcNodeHosts(excludeHosts, 1, needAllocSpace)
			if e != nil {
				return nil, errors.NewError(e)
			}
			hosts = append(hosts, selectedHosts...)
			excludeHosts = append(excludeHosts, selectedHosts...)
			hasAllocateNum = hasAllocateNum + 1
			if hasAllocateNum == replicaNum {
				break
			}
		}
	}

	log.LogInfof("action[chooseTargetEcNodes] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNum, len(zones), hosts)
	if len(hosts) != replicaNum {
		log.LogErrorf("action[chooseTargetEcNodes] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNum, len(zones), hosts)
		return nil, errors.Trace(proto.ErrNoDataNodeToCreateDataPartition, "hosts len[%v],replicaNum[%v],zoneNum[%v],selectedZones[%v]",
			len(hosts), replicaNum, zoneNum, len(zones))
	}
	return
}

func (c *Cluster) getAllEcDataPartitionByEcNode(addr string) (partitions []*EcDataPartition) {
	partitions = make([]*EcDataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, ecdp := range vol.ecDataPartitions.partitions {
			for _, host := range ecdp.Hosts {
				if host == addr {
					partitions = append(partitions, ecdp)
					break
				}
			}
		}
	}
	return
}

func (c *Cluster) getAllEcPartitionIDByEcnode(addr string) (partitionIDs []uint64) {
	partitionIDs = make([]uint64, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, ecdp := range vol.ecDataPartitions.partitions {
			for _, host := range ecdp.Hosts {
				if host == addr {
					partitionIDs = append(partitionIDs, ecdp.PartitionID)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) checkCorruptEcPartitions() (inactiveEcNodes []string, corruptEcPartitions []*EcDataPartition, err error) {
	partitionMap := make(map[uint64]uint8)
	c.ecNodes.Range(func(addr, node interface{}) bool {
		ecNode := node.(*ECNode)
		if !ecNode.isActive {
			inactiveEcNodes = append(inactiveEcNodes, ecNode.Addr)
		}
		return true
	})
	for _, addr := range inactiveEcNodes {
		partitions := c.getAllEcDataPartitionByEcNode(addr)
		for _, partition := range partitions {
			partitionMap[partition.PartitionID] = partitionMap[partition.PartitionID] + 1
		}
	}

	for partitionID, badNum := range partitionMap {
		var partition *EcDataPartition
		if partition, err = c.getEcPartitionByID(partitionID); err != nil {
			return
		}
		if badNum > partition.ParityUnitsNum/2 && badNum != partition.ParityUnitsNum {
			corruptEcPartitions = append(corruptEcPartitions, partition)
		}
	}
	log.LogInfof("clusterID[%v] inactiveEcNodes:%v  corruptEcPartitions count:[%v]",
		c.Name, inactiveEcNodes, len(corruptEcPartitions))
	return
}

func (c *Cluster) checkLackReplicaEcPartitions() (lackReplicaEcPartitions []*EcDataPartition, err error) {
	vols := c.copyVols()
	for _, vol := range vols {
		var eps *EcDataPartitionCache
		eps = vol.ecDataPartitions
		for _, ep := range eps.partitions {
			if !proto.IsEcFinished(ep.EcMigrateStatus){
				continue
			}
			if ep.ReplicaNum > uint8(len(ep.Hosts)) {
				lackReplicaEcPartitions = append(lackReplicaEcPartitions, ep)
			}
		}
	}
	log.LogInfof("clusterID[%v] lackReplicaEcPartitions count:[%v]", c.Name, len(lackReplicaEcPartitions))
	return
}

func (c *Cluster) putBadEcPartitionIDs(replica *EcReplica, addr string, partitionId uint64) {
	var key string
	newBadEcPartitionIDs := make([]uint64, 0)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}
	c.badEcPartitionsMutex.Lock()
	defer c.badEcPartitionsMutex.Unlock()
	badEcPartitionIDs, ok := c.BadEcPartitionIds.Load(key)
	if ok {
		newBadEcPartitionIDs = badEcPartitionIDs.([]uint64)
	}
	newBadEcPartitionIDs = append(newBadEcPartitionIDs, partitionId)
	c.BadEcPartitionIds.Store(key, newBadEcPartitionIDs)
}

func (c *Cluster) isEcRecovering(ecdp *EcDataPartition, addr string) (isRecover bool) {
	var key string
	ecdp.RLock()
	defer ecdp.RUnlock()
	replica, _ := ecdp.getEcReplica(addr)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}
	var badEcPartitionIDs []uint64
	badEcPartitions, ok := c.BadEcPartitionIds.Load(key)
	if ok {
		badEcPartitionIDs = badEcPartitions.([]uint64)
	}
	for _, id := range badEcPartitionIDs {
		if id == ecdp.PartitionID {
			isRecover = true
		}
	}
	return
}

func (c *Cluster) clearCodecNodes() {
	c.codecNodes.Range(func(key, value interface{}) bool {
		codecNode := value.(*CodecNode)
		c.codecNodes.Delete(key)
		codecNode.clean()
		return true
	})
}

func (c *Cluster) clearEcNodes() {
	c.ecNodes.Range(func(key, value interface{}) bool {
		ecNode := value.(*ECNode)
		c.ecNodes.Delete(key)
		ecNode.clean()
		return true
	})
}

func (c *Cluster) updateEcNodeStatInfo() {
	var (
		total uint64
		used  uint64
		totalNodes         int
		writableNodes      int
		highUsedRatioNodes int
	)
	c.ecNodes.Range(func(addr, node interface{}) bool {
		ecNode := node.(*ECNode)
		total = total + ecNode.Total
		used = used + ecNode.Used
		totalNodes++
		if ecNode.isActive && ecNode.isWriteAble() {
			writableNodes++
		}
		if ecNode.UsageRatio >= defaultHighUsedRatioDataNodesThreshold {
			highUsedRatioNodes++
		}
		return true
	})
	if total <= 0 {
		return
	}
	usedRate := float64(used) / float64(total)
	if usedRate > spaceAvailableRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add ecNode",
			c.Name, usedRate, used, total))
	}
	c.ecNodeStatInfo.TotalGB = total / util.GB
	usedGB := used / util.GB
	c.ecNodeStatInfo.IncreasedGB = int64(usedGB) - int64(c.ecNodeStatInfo.UsedGB)
	c.ecNodeStatInfo.UsedGB = usedGB
	c.ecNodeStatInfo.UsedRatio = strconv.FormatFloat(usedRate, 'f', 3, 32)
	c.ecNodeStatInfo.TotalNodes = totalNodes
	c.ecNodeStatInfo.WritableNodes = writableNodes
	c.ecNodeStatInfo.HighUsedRatioNodes = highUsedRatioNodes
}

func (c *Cluster) addEcNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.ecNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
		} else {
			node.TaskManager.AddTask(t)
		}
	}
}

func (c *Cluster) updateEcNode(ecNode *ECNode, partitions []*proto.EcPartitionReport) {
	for _, vr := range partitions {
		if vr == nil {
			continue
		}
		vol, err := c.getVol(vr.VolName)
		if err != nil {
			continue
		}
		if vol.Status == markDelete {
			continue
		}
		if ecdp, err := vol.ecDataPartitions.get(vr.PartitionID); err == nil {
			ecdp.updateMetric(vr, ecNode, c)
		}
		if dp, err := vol.dataPartitions.get(vr.PartitionID); err == nil {
			dp.checkAndRemoveMissReplica(ecNode.Addr)
		}
	}
	return
}

func (c *Cluster) checkEcDiskRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkEcDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkEcDiskRecoveryProgress occurred panic")
		}
	}()
	c.BadEcPartitionIds.Range(func(key, value interface{}) bool {
		badEcPartitionIds := value.([]uint64)
		newBadEcDpIds := make([]uint64, 0)
		for _, partitionID := range badEcPartitionIds {
			ep, err := c.getEcPartitionByID(partitionID)
			if err != nil {
				continue
			}
			if !ep.isRecover {
				continue
			}

			if ep.isEcFilesCatchUp() && ep.isEcDataCatchUp() && len(ep.ecReplicas) >= int(ep.DataUnitsNum + ep.ParityUnitsNum) {
				ep.RLock()
				ep.isRecover = false
				ep.PanicHosts = make([]string, 0)
				c.syncUpdateEcDataPartition(ep)
				ep.RUnlock()
				Warn(c.Name, fmt.Sprintf("action[checkEcDiskRecoveryProgress] clusterID[%v],partitionID[%v] has recovered success", c.Name, partitionID))
			} else {
				newBadEcDpIds = append(newBadEcDpIds, partitionID)
			}
		}
		c.badEcPartitionsMutex.Lock()
		defer c.badEcPartitionsMutex.Unlock()

		newValue, _ := c.BadEcPartitionIds.Load(key)
		newBadEcPartitionIds := newValue.([]uint64)
		if len(newBadEcPartitionIds) != len(badEcPartitionIds) {//avoid new insert value deleted
			log.LogInfof("new value insert, wait next schedule handle, newBadEcPartitionIds(%v) badEcPartitionIds(%v)\n",
				newBadEcPartitionIds, badEcPartitionIds)
			return true
		}
		if len(newBadEcDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress] clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.BadEcPartitionIds.Delete(key)
		} else {
			c.BadEcPartitionIds.Store(key, newBadEcDpIds)
		}
		return true
	})

}

func (c *Cluster) decommissionEcDisk(ecNode *ECNode, badDiskPath string, badPartitions []*EcDataPartition) (err error) {
	msg := fmt.Sprintf("action[decommissionDisk], Node[%v] OffLine,disk[%v]", ecNode.Addr, badDiskPath)
	log.LogWarn(msg)
	var wg sync.WaitGroup
	errChannel := make(chan error, len(badPartitions))
	defer func() {
		close(errChannel)
	}()
	for _, ecdp := range badPartitions {
		wg.Add(1)
		go func(ecdp *EcDataPartition) {
			defer wg.Done()
			if err1 := c.decommissionEcDataPartition(ecNode.Addr, ecdp, diskOfflineErr); err1 != nil {
				errChannel <- err1
			}
		}(ecdp)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		return
	default:
	}
	msg = fmt.Sprintf("action[decommissionDisk],clusterID[%v] Node[%v] disk[%v] OffLine success",
		c.Name, ecNode.Addr, badDiskPath)
	Warn(c.Name, msg)
	return
}

func getEcNodeMaxTotal(ecNodes *sync.Map) (maxTotal uint64) {
	ecNodes.Range(func(key, value interface{}) bool {
		ecNode := value.(*ECNode)
		if ecNode.Total > maxTotal {
			maxTotal = ecNode.Total
		}
		return true
	})

	return
}

func getAvailCarryEcNodeTab(maxTotal uint64, excludeHosts []string, ecNodes *sync.Map, needAllocSpace uint64) (nodeTabs SortedWeightedNodes, availCount int) {
	nodeTabs = make(SortedWeightedNodes, 0)
	ecNodes.Range(func(key, value interface{}) bool {
		ecNode := value.(*ECNode)
		if contains(excludeHosts, ecNode.Addr) == true {
			return true
		}
		if ecNode.isWriteAble() == false {
			return true
		}

		if ecNode.canCreateEcPartition(needAllocSpace) == false {
			return true
		}

		if ecNode.isAvailCarryNode() == true {
			availCount++
		}
		nt := new(weightedNode)
		nt.Carry = ecNode.Carry
		if ecNode.AvailableSpace < 0 {
			nt.Weight = 0.0
		} else {
			nt.Weight = float64(ecNode.AvailableSpace) / float64(maxTotal)
		}
		nt.Ptr = ecNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}
