// Copyright 2018 The Containerfs Authors.
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
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"runtime"
	"sync"
	"time"
	"github.com/juju/errors"
)

func (c *Cluster) putDataNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.getDataNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err))
		} else {
			node.Sender.PutTask(t)
		}
	}
}

func (c *Cluster) putMetaNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.getMetaNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
		} else {
			node.Sender.PutTask(t)
		}
	}
}

func (c *Cluster) waitLoadDataPartitionResponse(partitions []*DataPartition) {

	var wg sync.WaitGroup
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					const size = RuntimeStackBufSize
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					log.LogError(fmt.Sprintf("processLoadDataPartition panic %v: %s\n", err, buf))
				}
			}()
			c.processLoadDataPartition(dp)
		}(dp)
	}
	wg.Wait()
}

func (c *Cluster) loadDataPartitionAndCheckResponse(dp *DataPartition) {
	go func() {
		c.processLoadDataPartition(dp)
	}()
}

func (c *Cluster) metaPartitionOffline(volName, nodeAddr string, partitionID uint64) (err error) {
	var (
		vol         *Vol
		mp          *MetaPartition
		t           *proto.AdminTask
		tasks       []*proto.AdminTask
		newHosts    []string
		onlineAddrs []string
		newPeers    []proto.Peer
		removePeer  proto.Peer
		metaNode    *MetaNode
		ns          *NodeSet
	)
	log.LogWarnf("action[metaPartitionOffline],volName[%v],nodeAddr[%v],partitionID[%v]", volName, nodeAddr, partitionID)
	if vol, err = c.getVol(volName); err != nil {
		goto errDeal
	}
	if mp, err = vol.getMetaPartition(partitionID); err != nil {
		goto errDeal
	}
	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if ns, err = c.t.getNodeSet(metaNode.NodeSetId); err != nil {
		goto errDeal
	}
	mp.Lock()
	defer mp.Unlock()
	if !contains(mp.PersistenceHosts, nodeAddr) {
		return
	}

	if err = mp.canOffline(nodeAddr, int(vol.mpReplicaNum)); err != nil {
		goto errDeal
	}
	if newHosts, newPeers, err = ns.getAvailMetaNodeHosts(mp.PersistenceHosts, 1); err != nil {
		//select metaNode of other nodeSet
		if newHosts, newPeers, err = c.ChooseTargetMetaHosts(1); err != nil {
			goto errDeal
		}
	}

	onlineAddrs = make([]string, len(newHosts))
	copy(onlineAddrs, newHosts)
	for _, mr := range mp.Replicas {
		if mr.Addr == nodeAddr {
			removePeer = proto.Peer{ID: mr.nodeId, Addr: mr.Addr}
		} else {
			newPeers = append(newPeers, proto.Peer{ID: mr.nodeId, Addr: mr.Addr})
			newHosts = append(newHosts, mr.Addr)
		}
	}

	tasks = mp.generateCreateMetaPartitionTasks(onlineAddrs, newPeers, volName)
	if t, err = mp.generateOfflineTask(volName, removePeer, newPeers[0]); err != nil {
		goto errDeal
	}
	tasks = append(tasks, t)
	if err = mp.updateInfoToStore(newHosts, newPeers, volName, c); err != nil {
		goto errDeal
	}
	mp.removeReplicaByAddr(nodeAddr)
	mp.checkAndRemoveMissMetaReplica(nodeAddr)
	c.putMetaNodeTasks(tasks)
	Warn(c.Name, fmt.Sprintf("clusterID[%v] meta partition[%v] offline addr[%v] success,new addr[%v]",
		c.Name, partitionID, nodeAddr, newPeers[0].Addr))
	return
errDeal:
	log.LogError(fmt.Sprintf("action[metaPartitionOffline],volName: %v,partitionID: %v,err: %v",
		volName, partitionID, errors.ErrorStack(err)))
	Warn(c.Name, fmt.Sprintf("clusterID[%v] meta partition[%v] offline addr[%v] failed,err:%v",
		c.Name, partitionID, nodeAddr, err))
	return
}

func (c *Cluster) loadMetaPartitionAndCheckResponse(mp *MetaPartition) {
	go func() {
		c.processLoadMetaPartition(mp)
	}()
}

func (c *Cluster) processLoadMetaPartition(mp *MetaPartition) {

}

func (c *Cluster) processLoadDataPartition(dp *DataPartition) {
	log.LogInfo(fmt.Sprintf("action[processLoadDataPartition],partitionID:%v", dp.PartitionID))
	loadTasks := dp.generateLoadTasks()
	c.putDataNodeTasks(loadTasks)
	for i := 0; i < LoadDataPartitionWaitTime; i++ {
		if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) {
			log.LogWarnf("action[%v] trigger all replication,partitionID:%v ", "loadDataPartitionAndCheckResponse", dp.PartitionID)
			break
		}
		time.Sleep(time.Second)
	}
	// response is time out
	if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) == false {
		return
	}
	dp.getFileCount()
	dp.checkFile(c.Name)
	dp.setToNormal()
}

func (c *Cluster) dealMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[dealMetaNodeTaskResponse] receive Task response:%v from %v", task.ToString(), nodeAddr))
	var (
		metaNode *MetaNode
	)

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	metaNode.Sender.DelTask(task)
	if err = UnmarshalTaskResponse(task); err != nil {
		goto errDeal
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
	case proto.OpOfflineMetaPartition:
		response := task.Response.(*proto.MetaPartitionOfflineResponse)
		err = c.dealOfflineMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpOfflineDataPartition:
		response := task.Response.(*proto.DataPartitionOfflineResponse)
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
errDeal:
	log.LogError(fmt.Sprintf("action[dealMetaNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealOfflineDataPartitionResp(nodeAddr string, resp *proto.DataPartitionOfflineResponse) (err error) {
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealOfflineDataPartitionResp],clusterID[%v] nodeAddr %v "+
			"offline meta partition[%v] failed,err %v",
			c.Name, nodeAddr, resp.PartitionId, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	return
}

func (c *Cluster) dealOfflineMetaPartitionResp(nodeAddr string, resp *proto.MetaPartitionOfflineResponse) (err error) {
	if resp.Status == proto.TaskFail {
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
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealUpdateMetaPartitionResp],clusterID[%v] nodeAddr %v update meta partition failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) dealDeleteMetaPartitionResp(nodeAddr string, resp *proto.DeleteMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealDeleteMetaPartitionResp],clusterID[%v] nodeAddr %v "+
			"delete meta partition failed,err %v", c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	var mr *MetaReplica
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errDeal
	}
	mp.Lock()
	defer mp.Unlock()
	if mr, err = mp.getMetaReplica(nodeAddr); err != nil {
		goto errDeal
	}
	mp.removeReplica(mr)
	return

errDeal:
	log.LogError(fmt.Sprintf("dealDeleteMetaPartitionResp %v", err))
	return
}

func (c *Cluster) dealCreateMetaPartitionResp(nodeAddr string, resp *proto.CreateMetaPartitionResponse) (err error) {
	log.LogInfof("action[dealCreateMetaPartitionResp] receive resp from nodeAddr[%v] pid[%v]", nodeAddr, resp.PartitionID)
	if resp.Status == proto.TaskFail {
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
	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if vol, err = c.getVol(resp.VolName); err != nil {
		goto errDeal
	}

	if mp, err = vol.getMetaPartition(resp.PartitionID); err != nil {
		goto errDeal
	}
	mp.Lock()
	defer mp.Unlock()
	mr = NewMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
	mp.addReplica(mr)
	mp.checkAndRemoveMissMetaReplica(mr.Addr)
	log.LogInfof("action[dealCreateMetaPartitionResp] process resp from nodeAddr[%v] pid[%v] success", nodeAddr, resp.PartitionID)
	return
errDeal:
	log.LogErrorf(fmt.Sprintf("action[dealCreateMetaPartitionResp] %v", errors.ErrorStack(err)))
	return
}

func (c *Cluster) dealMetaNodeHeartbeatResp(nodeAddr string, resp *proto.MetaNodeHeartbeatResponse) (err error) {
	var (
		metaNode *MetaNode
		logMsg   string
	)
	log.LogInfof("action[dealMetaNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealMetaNodeHeartbeatResp],clusterID[%v] nodeAddr %v heartbeat failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}

	metaNode.updateMetric(resp, c.cfg.MetaNodeThreshold)
	metaNode.setNodeAlive()
	c.t.putMetaNode(metaNode)
	c.UpdateMetaNode(metaNode, resp.MetaPartitionInfo, metaNode.isArriveThreshold())
	metaNode.metaPartitionInfos = nil
	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeatResp],metaNode:%v ReportTime:%v  success", metaNode.Addr, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errDeal:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.ErrorStack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) dealDataNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("action[dealDataNodeTaskResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	log.LogDebugf("action[dealDataNodeTaskResponse] receive addr[%v] task response:%v", nodeAddr, task.ToString())
	var (
		err      error
		dataNode *DataNode
	)

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	dataNode.Sender.DelTask(task)
	if err = UnmarshalTaskResponse(task); err != nil {
		goto errDeal
	}

	switch task.OpCode {
	case proto.OpDeleteDataPartition:
		response := task.Response.(*proto.DeleteDataPartitionResponse)
		err = c.dealDeleteDataPartitionResponse(task.OperatorAddr, response)
	case proto.OpLoadDataPartition:
		response := task.Response.(*proto.LoadDataPartitionResponse)
		err = c.dealLoadDataPartitionResponse(task.OperatorAddr, response)
	case proto.OpDataNodeHeartbeat:
		response := task.Response.(*proto.DataNodeHeartBeatResponse)
		err = c.dealDataNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("unknown operate code %v", task.OpCode))
		goto errDeal
	}

	if err != nil {
		goto errDeal
	}
	return

errDeal:
	log.LogErrorf("process task[%v] failed,err:%v", task.ToString(), err)
	return
}

func (c *Cluster) dealDeleteDataPartitionResponse(nodeAddr string, resp *proto.DeleteDataPartitionResponse) (err error) {
	var (
		dp *DataPartition
	)
	if resp.Status == proto.TaskSuccess {
		if dp, err = c.getDataPartitionByID(resp.PartitionId); err != nil {
			return
		}
		dp.Lock()
		defer dp.Unlock()
		dp.offLineInMem(nodeAddr)

	} else {
		Warn(c.Name, fmt.Sprintf("clusterID[%v] delete data partition[%v] failed,err[%v]", c.Name, nodeAddr, resp.Result))
	}

	return
}

func (c *Cluster) dealLoadDataPartitionResponse(nodeAddr string, resp *proto.LoadDataPartitionResponse) (err error) {
	var dataNode *DataNode
	dp, err := c.getDataPartitionByID(resp.PartitionId)
	if err != nil || resp.Status == proto.TaskFail || resp.PartitionSnapshot == nil {
		return
	}
	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		return
	}
	dp.LoadFile(dataNode, resp)

	return
}

func (c *Cluster) dealDataNodeHeartbeatResp(nodeAddr string, resp *proto.DataNodeHeartBeatResponse) (err error) {

	var (
		dataNode *DataNode
		logMsg   string
	)
	log.LogInfof("action[dealDataNodeHeartbeatResp] clusterID[%v] receive dataNode[%v] heartbeat, ", c.Name, nodeAddr)
	if resp.Status != proto.TaskSuccess {
		Warn(c.Name, fmt.Sprintf("action[dealDataNodeHeartbeatResp] clusterID[%v] dataNode[%v] heartbeat task failed",
			c.Name, nodeAddr))
		return
	}

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}

	if dataNode.RackName != "" && dataNode.RackName != resp.RackName {
		Warn(c.Name, fmt.Sprintf("ClusterID[%s] DataNode[%v] rack from [%v] to [%v]!",
			c.Name, nodeAddr, dataNode.RackName, resp.RackName))
		dataNode.RackName = resp.RackName
		c.t.replaceDataNode(dataNode)
	}

	dataNode.UpdateNodeMetric(resp)
	c.t.PutDataNode(dataNode)
	c.UpdateDataNode(dataNode, resp.PartitionInfo)
	dataNode.dataPartitionInfos = nil
	logMsg = fmt.Sprintf("action[dealDataNodeHeartbeatResp],dataNode:%v ReportTime:%v  success", dataNode.Addr, time.Now().Unix())
	log.LogInfof(logMsg)

	return
errDeal:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

/*if node report data partition infos,so range data partition infos,then update data partition info*/
func (c *Cluster) UpdateDataNode(dataNode *DataNode, dps []*proto.PartitionReport) {
	for _, vr := range dps {
		if vr == nil {
			continue
		}
		if dp, err := c.getDataPartitionByID(vr.PartitionID); err == nil {
			dp.UpdateMetric(vr, dataNode)
		}
	}
}

func (c *Cluster) UpdateMetaNode(metaNode *MetaNode, metaPartitions []*proto.MetaPartitionReport, threshold bool) {
	for _, mr := range metaPartitions {
		if mr == nil {
			continue
		}
		mp, err := c.getMetaPartitionByID(mr.PartitionID)
		if err != nil {
			log.LogError(fmt.Sprintf("action[UpdateMetaNode],err:%v", err))
			err = nil
			continue
		}
		mp.UpdateMetaPartition(mr, metaNode)
		c.updateEnd(mp, mr, threshold, metaNode)
	}
}

func (c *Cluster) updateEnd(mp *MetaPartition, mr *proto.MetaPartitionReport, threshold bool, metaNode *MetaNode) {
	if !threshold {
		return
	}
	mp.Lock()
	defer mp.Unlock()
	if _, err := mp.getLeaderMetaReplica(); err != nil {
		log.LogWarnf("action[updateEnd] vol[%v] id[%v] no leader", mp.volName, mp.PartitionID)
		return
	}
	var (
		vol *Vol
		err error
	)
	if vol, err = c.getVol(mp.volName); err != nil {
		log.LogWarnf("action[updateEnd] vol[%v] not found", mp.volName)
		return
	}
	maxPartitionID := vol.getMaxPartitionID()
	if mp.PartitionID < maxPartitionID {
		log.LogWarnf("action[updateEnd] vol[%v] id[%v] less than maxId[%v]", mp.volName, mp.PartitionID, maxPartitionID)
		return
	}

	if mp.Start != mr.Start {
		Warn(c.Name, fmt.Sprintf("mpid[%v],start[%v],mrStart[%v],addr[%v]", mp.PartitionID, mp.Start, mr.Start, metaNode.Addr))
	}

	hasEnough := c.hasEnoughWritableMetaHosts(int(vol.mpReplicaNum), metaNode.NodeSetId)
	if mp.End == DefaultMaxMetaPartitionInodeID && hasEnough {
		var end uint64
		if mr.MaxInodeID <= 0 {
			end = mr.Start + DefaultMetaPartitionInodeIDStep
		} else {
			end = mr.MaxInodeID + DefaultMetaPartitionInodeIDStep
		}
		log.LogWarnf("mpId[%v],start[%v],end[%v],addr[%v],used[%v]", mp.PartitionID, mp.Start, mp.End, metaNode.Addr, metaNode.Used)
		mp.UpdateEnd(c, end)
	}
}
