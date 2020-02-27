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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type ECNode struct {
	ID          uint64
	Addr        string
	ReportTime  time.Time
	isActive    bool
	TaskManager *AdminTaskManager
	sync.RWMutex
	//todo add field
}

type EcNodeValue struct {
	ID   uint64
	Addr string
}

func newEcNodeValue(node *ECNode) *EcNodeValue {
	return &EcNodeValue{
		ID:   node.ID,
		Addr: node.Addr,
	}
}

func newEcNode(addr, clusterID string) *ECNode {
	node := new(ECNode)
	node.Addr = addr
	node.TaskManager = newAdminTaskManager(addr, clusterID)
	return node
}

func (ecNode *ECNode) updateMetric(resp *proto.EcNodeHeartbeatResponse) {
	ecNode.Lock()
	defer ecNode.Unlock()
	ecNode.ReportTime = time.Now()
	ecNode.isActive = true
}

func (ecNode *ECNode) clean() {
	ecNode.TaskManager.exitCh <- struct{}{}
}

func (c *Cluster) checkEcNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.ecNodes.Range(func(addr, ecNode interface{}) bool {
		node := ecNode.(*ECNode)
		task := createHeartbeatTask(c.masterAddr(), node.Addr, proto.OpEcNodeHeartbeat)
		tasks = append(tasks, task)
		return true
	})
	c.addEcNodeTasks(tasks)
}

func (m *Server) addEcNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if id, err = m.cluster.addEcNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
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
	ecNodeInfo = &proto.EcNodeInfo{
		ID:         node.ID,
		Addr:       node.Addr,
		ReportTime: node.ReportTime,
		IsActive:   node.isActive,
	}

	sendOkReply(w, r, newSuccessHTTPReply(ecNodeInfo))
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
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
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
		err := fmt.Errorf("unknown operate code %v", task.OpCode)
		log.LogError(err)
	}

	if err != nil {
		log.LogError(fmt.Sprintf("process task[%v] failed", task.ToString()))
	} else {
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
		logMsg string
	)
	log.LogInfof("action[dealEcNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealEcNodeHeartbeatResp],clusterID[%v] nodeAddr %v heartbeat failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if ecNode, err = c.ecNode(nodeAddr); err != nil {
		goto errHandler
	}

	ecNode.updateMetric(resp)
	//c.updateEcNode(ecNode, resp.MetaPartitionReports)
	//ecNode.metaPartitionInfos = nil
	logMsg = fmt.Sprintf("action[dealEcNodeHeartbeatResp],metaNode:%v ReportTime:%v  success", ecNode.Addr, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) addEcNode(nodeAddr string) (id uint64, err error) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	var ecNode *ECNode
	if node, ok := c.ecNodes.Load(nodeAddr); ok {
		ecNode = node.(*ECNode)
		return ecNode.ID, nil
	}

	ecNode = newEcNode(nodeAddr, c.Name)
	// allocate ecNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	ecNode.ID = id
	if err = c.syncAddEcNode(ecNode); err != nil {
		goto errHandler
	}
	c.ecNodes.Store(nodeAddr, ecNode)
	log.LogInfof("action[addEcNode],clusterID[%v] ecNodeAddr:%v success",
		c.Name, nodeAddr)
	return
errHandler:
	err = fmt.Errorf("action[addEcNode],clusterID[%v] ecNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

// key=#dn#id#Addr,value = json.Marshal(dnv)
func (c *Cluster) syncAddEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncPut, ecNode)
}

func (c *Cluster) syncDeleteEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncDelete, ecNode)
}

func (c *Cluster) syncPutEcNodeInfo(opType uint32, ecNode *ECNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ecNodePrefix + strconv.FormatUint(ecNode.ID, 10) + keySeparator + ecNode.Addr
	dnv := newEcNodeValue(ecNode)
	metadata.V, err = json.Marshal(dnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) ecNode(addr string) (ecNode *ECNode, err error) {
	value, ok := c.ecNodes.Load(addr)
	if !ok {
		err = errors.Trace(ecNodeNotFound(addr), "%v not found", addr)
		return
	}
	ecNode = value.(*ECNode)
	return
}

func (c *Cluster) decommissionEcNode(ecNode *ECNode) (err error) {
	msg := fmt.Sprintf("action[decommissionEcNode], Node[%v] OffLine", ecNode.Addr)
	log.LogWarn(msg)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if err = c.decommissionEcDataPartition(ecNode.Addr, dp); err != nil {
				return
			}
		}
	}
	if err = c.syncDeleteEcNode(ecNode); err != nil {
		msg = fmt.Sprintf("action[decommissionEcNode],clusterID[%v] node[%v] offline failed,err[%v]",
			c.Name, ecNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.ecNodes.Delete(ecNode.Addr)
	go ecNode.clean()
	msg = fmt.Sprintf("action[decommissionEcNode],clusterID[%v] node[%v] offLine success",
		c.Name, ecNode.Addr)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) decommissionEcDataPartition(offlineAddr string, dp *DataPartition) (err error) {
	//todo ec partition use raft or not? cross cell or not ?
	var (
		targetHosts []string
		newAddr     string
		msg         string
		dataNode    *DataNode
		cell        *Cell
		replica     *DataReplica
		ns          *nodeSet
	)
	dp.RLock()
	if ok := dp.hasEcHost(offlineAddr); !ok {
		dp.RUnlock()
		return
	}
	replica, _ = dp.getReplica(offlineAddr)
	dp.RUnlock()
	if err = c.validateDecommissionDataPartition(dp, offlineAddr); err != nil {
		goto errHandler
	}

	if dataNode, err = c.dataNode(offlineAddr); err != nil {
		goto errHandler
	}

	if dataNode.CellName == "" {
		err = fmt.Errorf("dataNode[%v] cell is nil", dataNode.Addr)
		goto errHandler
	}
	if cell, err = c.t.getCell(dataNode); err != nil {
		goto errHandler
	}
	if targetHosts, _, err = cell.getAvailDataNodeHosts(dp.Hosts, 1); err != nil {
		if ns, err = c.t.getNodeSet(dataNode.NodeSetID); err != nil {
			goto errHandler
		}
		// select data nodes from the other cell in same node set
		if targetHosts, _, err = ns.getAvailDataNodeHosts(cell, dp.Hosts, 1); err != nil {
			// select data nodes from the other node set
			if targetHosts, _, err = c.chooseTargetDataNodes(ns, cell, dp.Hosts, 1); err != nil {
				goto errHandler
			}
		}
	}
	if err = c.removeDataReplica(dp, offlineAddr, false); err != nil {
		goto errHandler
	}
	newAddr = targetHosts[0]
	if err = c.addDataReplica(dp, newAddr); err != nil {
		goto errHandler
	}
	dp.Status = proto.ReadOnly
	dp.isRecover = true
	c.putBadDataPartitionIDs(replica, offlineAddr, dp.PartitionID)
	log.LogWarnf("clusterID[%v] partitionID:%v  on Node:%v offline success,newHost[%v],PersistenceHosts:[%v]",
		c.Name, dp.PartitionID, offlineAddr, newAddr, dp.Hosts)
	return
errHandler:
	msg = fmt.Sprintf("decommissionEcDataPartition clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, offlineAddr, newAddr, err, dp.Hosts)
	if err != nil {
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) loadEcNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(ecNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadEcNodes],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		ecnv := &EcNodeValue{}
		if err = json.Unmarshal(value, ecnv); err != nil {
			err = fmt.Errorf("action[loadEcNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		ecNode := newEcNode(ecnv.Addr, c.Name)
		ecNode.ID = ecnv.ID
		c.ecNodes.Store(ecNode.Addr, ecNode)
		log.LogInfof("action[loadEcNodes],EcNode[%v]", ecNode.Addr)
	}
	return
}
