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
	"runtime"
	"sync"
	"time"
)

func (c *Cluster) addDataNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.dataNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err))
		} else {
			node.TaskManager.AddTask(t)
		}
	}
}

func (c *Cluster) addMetaNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.metaNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
		} else {
			node.Sender.AddTask(t)
		}
	}
}

func (c *Cluster) waitForResponseToLoadDataPartition(partitions []*DataPartition) {

	var wg sync.WaitGroup
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					const size = runtimeStackBufSize
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					log.LogError(fmt.Sprintf("doLoadDataPartition panic %v: %s\n", err, buf))
				}
			}()
			c.doLoadDataPartition(dp)
		}(dp)
	}
	wg.Wait()
}

func (c *Cluster) loadDataPartition(dp *DataPartition) {
	go func() {
		c.doLoadDataPartition(dp)
	}()
}

// taking the given mata partition offline.
// 1. checking if the meta partition can be offline.
// There are two cases where the partition is not allowed to be offline:
// (1) the replica is not in the latest host list
// (2) there are too few replicas
// 2. choosing a new available meta node
// 3. persistent the new host list
// 4. generating an async task to delete the replica
// 5. asynchronously create a new data partition
func (c *Cluster) decommissionMetaPartition(nodeAddr string, mp *MetaPartition) (err error) {
	var (
		vol         *Vol
		t           *proto.AdminTask
		tasks       []*proto.AdminTask
		newHosts    []string
		onlineAddrs []string
		newPeers    []proto.Peer
		removePeer  proto.Peer
		metaNode    *MetaNode
		ns          *nodeSet
	)
	log.LogWarnf("action[decommissionMetaPartition],volName[%v],nodeAddr[%v],partitionID[%v]", mp.volName, nodeAddr, mp.PartitionID)
	if !contains(mp.Hosts, nodeAddr) {
		return
	}
	if vol, err = c.getVol(mp.volName); err != nil {
		goto errHandler
	}
	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	if ns, err = c.t.getNodeSet(metaNode.NodeSetID); err != nil {
		goto errHandler
	}
	mp.Lock()
	defer mp.Unlock()
	if err = mp.canBeOffline(nodeAddr, int(vol.mpReplicaNum)); err != nil {
		goto errHandler
	}
	if newHosts, newPeers, err = ns.getAvailMetaNodeHosts(mp.Hosts, 1); err != nil {
		// choose a meta node in the node set
		if newHosts, newPeers, err = c.chooseTargetMetaHosts(1); err != nil {
			goto errHandler
		}
	}

	onlineAddrs = make([]string, len(newHosts))
	copy(onlineAddrs, newHosts)
	for _, host := range mp.Hosts {
		if host == nodeAddr {
			removePeer = proto.Peer{ID: metaNode.ID, Addr: nodeAddr}
		} else {
			var mn *MetaNode
			if mn, err = c.metaNode(host); err != nil {
				goto errHandler
			}
			newPeers = append(newPeers, proto.Peer{ID: mn.ID, Addr: host})
			newHosts = append(newHosts, host)
		}
	}
	tasks = mp.buildNewMetaPartitionTasks(onlineAddrs, newPeers, mp.volName)
	if t, err = mp.createTaskToDecommissionReplica(mp.volName, removePeer, newPeers[0]); err != nil {
		goto errHandler
	}
	tasks = append(tasks, t)
	if err = mp.persistToRocksDB(newHosts, newPeers, mp.volName, c); err != nil {
		goto errHandler
	}
	mp.removeReplicaByAddr(nodeAddr)
	mp.removeMissingReplica(nodeAddr)
	c.addMetaNodeTasks(tasks)
	Warn(c.Name, fmt.Sprintf("clusterID[%v] meta partition[%v] offline addr[%v] success,new addr[%v]",
		c.Name, mp.PartitionID, nodeAddr, newPeers[0].Addr))
	return

errHandler:
	log.LogError(fmt.Sprintf("action[decommissionMetaPartition],volName: %v,partitionID: %v,err: %v",
		mp.volName, mp.PartitionID, errors.Stack(err)))
	Warn(c.Name, fmt.Sprintf("clusterID[%v] meta partition[%v] offline addr[%v] failed,err:%v",
		c.Name, mp.PartitionID, nodeAddr, err))
	return
}

func (c *Cluster) loadMetaPartitionAndCheckResponse(mp *MetaPartition) {
	go func() {
		c.doLoadMetaPartition(mp)
	}()
}

func (c *Cluster) doLoadMetaPartition(mp *MetaPartition) {
	var wg sync.WaitGroup
	log.LogInfof("action[doLoadMetaPartition],vol[%v],mpID[%v] begin", mp.volName, mp.PartitionID)
	mp.RLock()
	hosts := make([]string, len(mp.Hosts))
	copy(hosts, mp.Hosts)
	mp.RUnlock()
	errChannel := make(chan error, len(hosts))
	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			mr, err := mp.getMetaReplica(host)
			if err != nil {
				errChannel <- err
				return
			}
			task := mr.createTaskToLoadMetaPartition(mp.PartitionID)
			conn, err := mr.metaNode.Sender.connPool.GetConnect(mr.Addr)
			if err != nil {
				errChannel <- err
				return
			}
			response, err := mr.metaNode.Sender.syncSendAdminTask(task, conn)
			if err != nil {
				errChannel <- err
				return
			}
			loadResponse := &proto.MetaPartitionLoadResponse{}
			if err = json.Unmarshal(response, loadResponse); err != nil {
				errChannel <- err
				return
			}
			loadResponse.Addr = host
			mp.addOrReplaceLoadResponse(loadResponse)
			mr.metaNode.Sender.connPool.PutConnect(conn, false)
		}(host)
	}
	wg.Wait()
	select {
	case err := <-errChannel:
		msg := fmt.Sprintf("action[doLoadMetaPartition] vol[%v],mpID[%v],err[%v]", mp.volName, mp.PartitionID, err.Error())
		Warn(c.Name, msg)
		return
	default:
	}
	mp.checkSnapshot(c.Name)
	log.LogInfof("action[doLoadMetaPartition],vol[%v],mpID[%v] success", mp.volName, mp.PartitionID)
}

func (c *Cluster) doLoadDataPartition(dp *DataPartition) {
	log.LogInfo(fmt.Sprintf("action[doLoadDataPartition],partitionID:%v", dp.PartitionID))
	if !dp.needsToCompareCRC() {
		log.LogInfo(fmt.Sprintf("action[doLoadDataPartition],partitionID:%v isRecover[%v] don't need compare", dp.PartitionID, dp.isRecover))
		return
	}
	loadTasks := dp.createLoadTasks()
	c.addDataNodeTasks(loadTasks)
	for i := 0; i < timeToWaitForResponse; i++ {
		if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) {
			log.LogWarnf("action[checkLoadResponse]  all replica has responded,partitionID:%v ", dp.PartitionID)
			break
		}
		time.Sleep(time.Second)
	}

	if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) == false {
		return
	}

	dp.getFileCount()
	dp.validateCRC(c.Name)
	dp.setToNormal()
}

func (c *Cluster) handleMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[handleMetaNodeTaskResponse] receive Task response:%v from %v", task.ToString(), nodeAddr))
	var (
		metaNode *MetaNode
	)

	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	metaNode.Sender.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpMetaNodeHeartbeat:
		response := task.Response.(*proto.MetaNodeHeartbeatResponse)
		err = c.dealMetaNodeHeartbeatResp(task.OperatorAddr, response)
	case proto.OpCreateMetaPartition:
		response := task.Response.(*proto.CreateMetaPartitionResponse)
		err = c.dealCreateMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpDeleteMetaPartition:
		response := task.Response.(*proto.DeleteMetaPartitionResponse)
		err = c.dealDeleteMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpUpdateMetaPartition:
		response := task.Response.(*proto.UpdateMetaPartitionResponse)
		err = c.dealUpdateMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpLoadMetaPartition:
		response := task.Response.(*proto.LoadMetaPartitionMetricResponse)
		err = c.dealLoadMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpDecommissionMetaPartition:
		response := task.Response.(*proto.MetaPartitionDecommissionResponse)
		err = c.dealOfflineMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpDecommissionDataPartition:
		response := task.Response.(*proto.DataPartitionDecommissionResponse)
		err = c.dealOfflineDataPartitionResp(task.OperatorAddr, response)
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
	log.LogError(fmt.Sprintf("action[handleMetaNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealOfflineDataPartitionResp(nodeAddr string, resp *proto.DataPartitionDecommissionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealOfflineDataPartitionResp],clusterID[%v] nodeAddr %v "+
			"offline meta partition[%v] failed,err %v",
			c.Name, nodeAddr, resp.PartitionId, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	return
}

func (c *Cluster) dealOfflineMetaPartitionResp(nodeAddr string, resp *proto.MetaPartitionDecommissionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealOfflineMetaPartitionResp],clusterID[%v] nodeAddr %v "+
			"offline meta partition[%v] failed,err %v",
			c.Name, nodeAddr, resp.PartitionID, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	return
}

func (c *Cluster) dealLoadMetaPartitionResp(nodeAddr string, resp *proto.LoadMetaPartitionMetricResponse) (err error) {
	return
}

func (c *Cluster) dealUpdateMetaPartitionResp(nodeAddr string, resp *proto.UpdateMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealUpdateMetaPartitionResp],clusterID[%v] nodeAddr %v update meta partition failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) dealDeleteMetaPartitionResp(nodeAddr string, resp *proto.DeleteMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealDeleteMetaPartitionResp],clusterID[%v] nodeAddr %v "+
			"delete meta partition failed,err %v", c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	var mr *MetaReplica
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errHandler
	}
	mp.Lock()
	defer mp.Unlock()
	if mr, err = mp.getMetaReplica(nodeAddr); err != nil {
		goto errHandler
	}
	mp.removeReplica(mr)
	return

errHandler:
	log.LogError(fmt.Sprintf("dealDeleteMetaPartitionResp %v", err))
	return
}

func (c *Cluster) dealCreateMetaPartitionResp(nodeAddr string, resp *proto.CreateMetaPartitionResponse) (err error) {
	log.LogInfof("action[dealCreateMetaPartitionResp] receive resp from nodeAddr[%v] pid[%v]", nodeAddr, resp.PartitionID)
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealCreateMetaPartitionResp],clusterID[%v] nodeAddr %v create meta partition failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	var (
		metaNode *MetaNode
		vol      *Vol
		mp       *MetaPartition
		mr       *MetaReplica
	)
	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	if vol, err = c.getVol(resp.VolName); err != nil {
		goto errHandler
	}

	if mp, err = vol.metaPartition(resp.PartitionID); err != nil {
		goto errHandler
	}
	mp.Lock()
	defer mp.Unlock()
	mr = newMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
	mp.addReplica(mr)
	mp.removeMissingReplica(mr.Addr)
	log.LogInfof("action[dealCreateMetaPartitionResp] process resp from nodeAddr[%v] pid[%v] success", nodeAddr, resp.PartitionID)
	return
errHandler:
	log.LogErrorf(fmt.Sprintf("action[dealCreateMetaPartitionResp] %v", errors.Stack(err)))
	return
}

func (c *Cluster) dealMetaNodeHeartbeatResp(nodeAddr string, resp *proto.MetaNodeHeartbeatResponse) (err error) {
	var (
		metaNode *MetaNode
		logMsg   string
	)
	log.LogInfof("action[dealMetaNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealMetaNodeHeartbeatResp],clusterID[%v] nodeAddr %v heartbeat failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}

	metaNode.updateMetric(resp, c.cfg.MetaNodeThreshold)
	metaNode.setNodeActive()

	if err = c.t.putMetaNode(metaNode); err != nil {
		log.LogErrorf("action[dealMetaNodeHeartbeatResp],metaNode[%v] error[%v]", metaNode.Addr, err)
	}
	c.updateMetaNode(metaNode, resp.MetaPartitionReports, metaNode.reachesThreshold())
	metaNode.metaPartitionInfos = nil
	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeatResp],metaNode:%v ReportTime:%v  success", metaNode.Addr, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) handleDataNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("action[handleDataNodeTaskResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	log.LogDebugf("action[handleDataNodeTaskResponse] receive addr[%v] task response:%v", nodeAddr, task.ToString())
	var (
		err      error
		dataNode *DataNode
	)

	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	dataNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpDeleteDataPartition:
		response := task.Response.(*proto.DeleteDataPartitionResponse)
		err = c.dealDeleteDataPartitionResponse(task.OperatorAddr, response)
	case proto.OpLoadDataPartition:
		response := task.Response.(*proto.LoadDataPartitionResponse)
		err = c.handleResponseToLoadDataPartition(task.OperatorAddr, response)
	case proto.OpDataNodeHeartbeat:
		response := task.Response.(*proto.DataNodeHeartbeatResponse)
		err = c.handleDataNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("unknown operate code %v", task.OpCode))
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogErrorf("process task[%v] failed,err:%v", task.ToString(), err)
	return
}

func (c *Cluster) dealDeleteDataPartitionResponse(nodeAddr string, resp *proto.DeleteDataPartitionResponse) (err error) {
	var (
		dp *DataPartition
	)
	if resp.Status == proto.TaskSucceeds {
		if dp, err = c.getDataPartitionByID(resp.PartitionId); err != nil {
			return
		}
		dp.Lock()
		defer dp.Unlock()
		dp.removeReplicaByAddr(nodeAddr)

	} else {
		Warn(c.Name, fmt.Sprintf("clusterID[%v] delete data partition[%v] failed,err[%v]", c.Name, nodeAddr, resp.Result))
	}

	return
}

func (c *Cluster) handleResponseToLoadDataPartition(nodeAddr string, resp *proto.LoadDataPartitionResponse) (err error) {
	var dataNode *DataNode
	dp, err := c.getDataPartitionByID(resp.PartitionId)
	if err != nil || resp.Status == proto.TaskFailed || resp.PartitionSnapshot == nil {
		return
	}
	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		return
	}
	dp.loadFile(dataNode, resp)

	return
}

func (c *Cluster) handleDataNodeHeartbeatResp(nodeAddr string, resp *proto.DataNodeHeartbeatResponse) (err error) {

	var (
		dataNode *DataNode
		logMsg   string
	)
	log.LogInfof("action[handleDataNodeHeartbeatResp] clusterID[%v] receive dataNode[%v] heartbeat, ", c.Name, nodeAddr)
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleDataNodeHeartbeatResp] clusterID[%v] dataNode[%v] heartbeat task failed",
			c.Name, nodeAddr))
		return
	}

	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}

	if dataNode.RackName != "" && dataNode.RackName != resp.RackName {
		Warn(c.Name, fmt.Sprintf("ClusterID[%s] DataNode[%v] rack from [%v] to [%v]!",
			c.Name, nodeAddr, dataNode.RackName, resp.RackName))
		dataNode.RackName = resp.RackName
		c.t.replaceDataNode(dataNode)
	}

	dataNode.updateNodeMetric(resp)

	if err = c.t.putDataNode(dataNode); err != nil {
		log.LogErrorf("action[handleDataNodeHeartbeatResp] dataNode[%v] err[%v]", dataNode.Addr, err)
	}
	c.updateDataNode(dataNode, resp.PartitionReports)
	logMsg = fmt.Sprintf("action[handleDataNodeHeartbeatResp],dataNode:%v ReportTime:%v  success", dataNode.Addr, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

/*if node report data partition infos,so range data partition infos,then update data partition info*/
func (c *Cluster) updateDataNode(dataNode *DataNode, dps []*proto.PartitionReport) {
	for _, vr := range dps {
		if vr == nil {
			continue
		}
		if dp, err := c.getDataPartitionByID(vr.PartitionID); err == nil {
			dp.updateMetric(vr, dataNode,c)
		}
	}
}

func (c *Cluster) updateMetaNode(metaNode *MetaNode, metaPartitions []*proto.MetaPartitionReport, threshold bool) {
	for _, mr := range metaPartitions {
		if mr == nil {
			continue
		}
		mp, err := c.getMetaPartitionByID(mr.PartitionID)
		if err != nil {
			log.LogError(fmt.Sprintf("action[updateMetaNode],err:%v", err))
			err = nil
			continue
		}
		mp.updateMetaPartition(mr, metaNode)
		c.updateInodeIDUpperBound(mp, mr, threshold, metaNode)
	}
}

func (c *Cluster) updateInodeIDUpperBound(mp *MetaPartition, mr *proto.MetaPartitionReport, threshold bool, metaNode *MetaNode) {
	if !threshold {
		return
	}
	mp.Lock()
	defer mp.Unlock()
	if _, err := mp.getMetaReplicaLeader(); err != nil {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] id[%v] no leader", mp.volName, mp.PartitionID)
		return
	}
	var (
		vol *Vol
		err error
	)
	if vol, err = c.getVol(mp.volName); err != nil {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] not found", mp.volName)
		return
	}
	maxPartitionID := vol.maxPartitionID()
	if mp.PartitionID < maxPartitionID {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] id[%v] less than maxId[%v]", mp.volName, mp.PartitionID, maxPartitionID)
		return
	}

	if mp.Start != mr.Start {
		Warn(c.Name, fmt.Sprintf("mpid[%v],start[%v],mrStart[%v],addr[%v]", mp.PartitionID, mp.Start, mr.Start, metaNode.Addr))
	}

	hasEnough := c.hasEnoughWritableMetaHosts(int(vol.mpReplicaNum), metaNode.NodeSetID)
	if mp.End == defaultMaxMetaPartitionInodeID && hasEnough {
		var end uint64
		if mr.MaxInodeID <= 0 {
			end = mr.Start + defaultMetaPartitionInodeIDStep
		} else {
			end = mr.MaxInodeID + defaultMetaPartitionInodeIDStep
		}
		log.LogWarnf("mpId[%v],start[%v],end[%v],addr[%v],used[%v]", mp.PartitionID, mp.Start, mp.End, metaNode.Addr, metaNode.Used)
		mp.updateInodeIDRange(c, end)
	}
}
