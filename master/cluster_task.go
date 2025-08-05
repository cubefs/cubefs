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
	"runtime"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) addDataNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		c.addDataNodeTask(t)
	}
}

func (c *Cluster) addDataNodeTask(task *proto.AdminTask) {
	if task == nil {
		return
	}
	if node, err := c.dataNode(task.OperatorAddr); err != nil {
		log.LogWarnf("action[putTasks],nodeAddr:%s,taskID:%s,err:%v", task.OperatorAddr, task.ID, err)
	} else {
		node.TaskManager.AddTask(task)
	}
}

func (c *Cluster) addMetaNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.metaNode(t.OperatorAddr); err != nil {
			log.LogWarnf("action[putTasks],nodeAddr:%s,taskID:%s,err:%v", t.OperatorAddr, t.ID, err)
		} else {
			node.Sender.AddTask(t)
		}
	}
}

func (c *Cluster) addLcNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.lcNode(t.OperatorAddr); err != nil {
			log.LogWarnf("action[putTasks],nodeAddr:%s,taskID:%s,err:%v", t.OperatorAddr, t.ID, err)
		} else {
			node.TaskManager.AddTask(t)
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
					log.LogErrorf("doLoadDataPartition panic %v: %s", err, buf)
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

func (c *Cluster) migrateMetaPartition(srcAddr, targetAddr string, mp *MetaPartition) (err error) {
	var (
		newPeers        []proto.Peer
		metaNode        *MetaNode
		zone            *Zone
		ns              *nodeSet
		excludeNodeSets []uint64
		finalHosts      []string
		oldHosts        []string
		zones           []string
	)

	log.LogWarnf("action[migrateMetaPartition],volName[%v], migrate from src[%s] to target[%s],partitionID[%v] begin",
		mp.volName, srcAddr, targetAddr, mp.PartitionID)

	mp.RLock()
	if !contains(mp.Hosts, srcAddr) {
		mp.RUnlock()
		log.LogErrorf("action[migrateMetaPartition],volName[%v], src[%s] not exist, partitionID[%v]",
			mp.volName, srcAddr, mp.PartitionID)
		return fmt.Errorf("migrateMetaPartition src [%s] is not exist in mp(%d)", srcAddr, mp.PartitionID)
	}
	oldHosts = mp.Hosts
	mp.RUnlock()

	if err = c.validateDecommissionMetaPartition(mp, srcAddr, false); err != nil {
		goto errHandler
	}

	if metaNode, err = c.metaNode(srcAddr); err != nil {
		goto errHandler
	}

	if zone, err = c.t.getZone(metaNode.ZoneName); err != nil {
		goto errHandler
	}

	if ns, err = zone.getNodeSet(metaNode.NodeSetID); err != nil {
		goto errHandler
	}

	if targetAddr != "" {
		newPeers = []proto.Peer{{
			Addr: targetAddr,
		}}
	} else if _, newPeers, err = ns.getAvailMetaNodeHosts(oldHosts, 1); err != nil {
		if _, ok := c.vols[mp.volName]; !ok {
			log.LogWarnf("[migrateMetaPartition] clusterID[%v] partitionID:%v  on node:[%v]",
				c.Name, mp.PartitionID, mp.Hosts)
			return
		}
		if c.isFaultDomain(c.vols[mp.volName]) {
			log.LogWarnf("[migrateMetaPartition] clusterID[%v] partitionID:%v  on node:[%v]",
				c.Name, mp.PartitionID, mp.Hosts)
			return
		}
		// choose a meta node in other node set in the same zone
		excludeNodeSets = append(excludeNodeSets, ns.ID)
		if _, newPeers, err = zone.getAvailNodeHosts(TypeMetaPartition, excludeNodeSets, oldHosts, 1); err != nil {
			zones = mp.getLiveZones(srcAddr)
			var excludeZone []string
			if len(zones) == 0 {
				excludeZone = append(excludeZone, zone.name)
			} else {
				excludeZone = append(excludeZone, zones[0])
			}
			// choose a meta node in other zone
			if _, newPeers, err = c.getHostFromNormalZone(TypeMetaPartition, excludeZone, excludeNodeSets, oldHosts, 1, 1, "", proto.MediaType_Unspecified); err != nil {
				goto errHandler
			}
		}
	}

	finalHosts = append(oldHosts, newPeers[0].Addr) // add new one
	for i, host := range finalHosts {
		if host == srcAddr {
			finalHosts = append(finalHosts[:i], finalHosts[i+1:]...) // remove old one
			break
		}
	}
	if err = c.checkMultipleReplicasOnSameMachine(finalHosts); err != nil {
		return err
	}

	if err = c.deleteMetaReplica(mp, srcAddr, false, false); err != nil {
		goto errHandler
	}

	if err = c.addMetaReplica(mp, newPeers[0].Addr); err != nil {
		goto errHandler
	}

	mp.IsRecover = true
	c.putBadMetaPartitions(srcAddr, mp.PartitionID)

	mp.RLock()
	c.syncUpdateMetaPartition(mp)
	mp.RUnlock()

	Warn(c.Name, fmt.Sprintf("action[migrateMetaPartition] clusterID[%v] vol[%v] meta partition[%v] "+
		"migrate addr[%v] success,new addr[%v]", c.Name, mp.volName, mp.PartitionID, srcAddr, newPeers[0].Addr))
	return

errHandler:
	msg := fmt.Sprintf("action[migrateMetaPartition],volName: %v,partitionID: %v,err: %v", mp.volName, mp.PartitionID, errors.Stack(err))
	log.LogError(msg)
	Warn(c.Name, msg)

	if err != nil {
		err = fmt.Errorf("action[migrateMetaPartition] vol[%v],partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
	}
	return
}

// taking the given mata partition offline.
// 1. checking if the meta partition can be offline.
// There are two cases where the partition is not allowed to be offline:
// (1) the replica is not in the latest host list
// (2) there are too few replicas
// 2. choosing a new available meta node
// 3. synchronized decommission meta partition
// 4. synchronized create a new meta partition
// 5. persistent the new host list
func (c *Cluster) decommissionMetaPartition(nodeAddr string, mp *MetaPartition) (err error) {
	if c.ForbidMpDecommission {
		err = fmt.Errorf("cluster mataPartition decommission switch is disabled")
		return
	}
	return c.migrateMetaPartition(nodeAddr, "", mp)
}

func (c *Cluster) validateDecommissionMetaPartition(mp *MetaPartition, nodeAddr string, forceDel bool) (err error) {
	mp.RLock()
	defer mp.RUnlock()

	var vol *Vol
	if vol, err = c.getVol(mp.volName); err != nil {
		return
	}

	if err = mp.canBeOffline(nodeAddr, int(vol.mpReplicaNum)); err != nil {
		return
	}

	if forceDel {
		log.LogWarnf("action[validateDecommissionMetaPartition] mp relica be force delete without check missing and recovery status")
		return
	}

	if err = mp.hasMissingOneReplica(nodeAddr, int(vol.mpReplicaNum)); err != nil {
		return
	}

	if mp.IsRecover && !mp.activeMaxInodeSimilar() {
		err = fmt.Errorf("vol[%v],meta partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, mp.PartitionID, nodeAddr)
		return
	}
	return
}

func (c *Cluster) checkInactiveMetaNodes() (inactiveMetaNodes []string, err error) {
	inactiveMetaNodes = make([]string, 0)

	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		if !metaNode.IsActive {
			inactiveMetaNodes = append(inactiveMetaNodes, metaNode.Addr)
		}
		return true
	})

	log.LogInfof("clusterID[%v] inactiveMetaNodes:%v", c.Name, inactiveMetaNodes)
	return
}

type VolNameSet map[string]struct{}

func (c *Cluster) checkReplicaMetaPartitions() (
	lackReplicaMetaPartitions []*MetaPartition, noLeaderMetaPartitions []*MetaPartition,
	unavailableReplicaMPs []*MetaPartition, excessReplicaMetaPartitions, inodeCountNotEqualMPs, maxInodeNotEqualMPs, dentryCountNotEqualMPs []*MetaPartition, err error,
) {
	lackReplicaMetaPartitions = make([]*MetaPartition, 0)
	noLeaderMetaPartitions = make([]*MetaPartition, 0)
	excessReplicaMetaPartitions = make([]*MetaPartition, 0)
	inodeCountNotEqualMPs = make([]*MetaPartition, 0)
	maxInodeNotEqualMPs = make([]*MetaPartition, 0)
	dentryCountNotEqualMPs = make([]*MetaPartition, 0)

	markDeleteVolNames := make(VolNameSet)

	vols := c.copyVols()
	for _, vol := range vols {
		if vol.Status == proto.VolStatusMarkDelete {
			markDeleteVolNames[vol.Name] = struct{}{}
			continue
		}

		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if uint8(len(mp.Hosts)) < mp.ReplicaNum || uint8(len(mp.getActiveAddrs())) < mp.ReplicaNum {
				lackReplicaMetaPartitions = append(lackReplicaMetaPartitions, mp)
			}

			if !mp.isLeaderExist() && (time.Now().Unix()-mp.LeaderReportTime > c.cfg.MpNoLeaderReportIntervalSec) {
				noLeaderMetaPartitions = append(noLeaderMetaPartitions, mp)
			}

			if uint8(len(mp.Hosts)) > mp.ReplicaNum || uint8(len(mp.Replicas)) > mp.ReplicaNum {
				excessReplicaMetaPartitions = append(excessReplicaMetaPartitions, mp)
			}

			for _, replica := range mp.Replicas {
				if replica.Status == proto.Unavailable {
					unavailableReplicaMPs = append(unavailableReplicaMPs, mp)
					break
				}
			}
		}
		vol.mpsLock.RUnlock()
	}
	c.inodeCountNotEqualMP.Range(func(key, value interface{}) bool {
		mp := value.(*MetaPartition)
		if _, ok := markDeleteVolNames[mp.volName]; !ok {
			inodeCountNotEqualMPs = append(inodeCountNotEqualMPs, mp)
		}
		return true
	})
	c.maxInodeNotEqualMP.Range(func(key, value interface{}) bool {
		mp := value.(*MetaPartition)
		if _, ok := markDeleteVolNames[mp.volName]; !ok {
			maxInodeNotEqualMPs = append(maxInodeNotEqualMPs, mp)
		}
		return true
	})
	c.dentryCountNotEqualMP.Range(func(key, value interface{}) bool {
		mp := value.(*MetaPartition)
		if _, ok := markDeleteVolNames[mp.volName]; !ok {
			dentryCountNotEqualMPs = append(dentryCountNotEqualMPs, mp)
		}
		return true
	})
	log.LogInfof("clusterID[%v], lackReplicaMetaPartitions count:[%v], noLeaderMetaPartitions count[%v]"+
		"unavailableReplicaMPs count:[%v], excessReplicaMp count:[%v]",
		c.Name, len(lackReplicaMetaPartitions), len(noLeaderMetaPartitions),
		len(unavailableReplicaMPs), len(excessReplicaMetaPartitions))
	return
}

func (c *Cluster) deleteMetaReplica(partition *MetaPartition, addr string, validate bool, forceDel bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[deleteMetaReplica],vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
		}
	}()

	if validate {
		if err = c.validateDecommissionMetaPartition(partition, addr, forceDel); err != nil {
			return
		}
	}

	metaNode, err := c.metaNode(addr)
	if err != nil {
		return
	}

	removePeer := proto.Peer{ID: metaNode.ID, Addr: addr, HeartbeatPort: metaNode.HeartbeatPort, ReplicaPort: metaNode.ReplicaPort}
	if err = c.removeMetaPartitionRaftMember(partition, removePeer); err != nil {
		return
	}

	if err = c.deleteMetaPartition(partition, metaNode); err != nil {
		return
	}
	return
}

func (c *Cluster) deleteMetaPartition(partition *MetaPartition, removeMetaNode *MetaNode) (err error) {
	partition.Lock()
	mr, err := partition.getMetaReplica(removeMetaNode.Addr)
	if err != nil {
		partition.Unlock()
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v], err[%v]", partition.volName, partition.PartitionID, err)
		return nil
	}
	task := mr.createTaskToDeleteReplica(partition.PartitionID)
	partition.removeReplicaByAddr(removeMetaNode.Addr)
	partition.removeMissingReplica(removeMetaNode.Addr)
	partition.Unlock()
	_, err = removeMetaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
	}
	return nil
}

func (c *Cluster) removeMetaPartitionRaftMember(partition *MetaPartition, removePeer proto.Peer) (err error) {
	partition.offlineMutex.Lock()
	defer partition.offlineMutex.Unlock()
	defer func() {
		if err1 := c.updateMetaPartitionOfflinePeerIDWithLock(partition, 0); err1 != nil {
			err = errors.Trace(err, "updateMetaPartitionOfflinePeerIDWithLock failed, err[%v]", err1)
		}
	}()
	if err = c.updateMetaPartitionOfflinePeerIDWithLock(partition, removePeer.ID); err != nil {
		return
	}
	mr, err := partition.getMetaReplicaLeader()
	if err != nil {
		return
	}
	t, err := partition.createTaskToRemoveRaftMember(removePeer)
	if err != nil {
		return
	}
	var leaderMetaNode *MetaNode
	leaderMetaNode = mr.metaNode
	if leaderMetaNode == nil {
		leaderMetaNode, err = c.metaNode(mr.Addr)
		if err != nil {
			return
		}
	}
	if _, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		return
	}
	newHosts := make([]string, 0, len(partition.Hosts)-1)
	newPeers := make([]proto.Peer, 0, len(partition.Hosts)-1)
	for _, host := range partition.Hosts {
		if host == removePeer.Addr {
			continue
		}
		newHosts = append(newHosts, host)
	}
	for _, peer := range partition.Peers {
		if peer.Addr == removePeer.Addr && peer.ID == removePeer.ID {
			continue
		}
		newPeers = append(newPeers, peer)
	}
	if err = partition.persistToRocksDB("removeMetaPartitionRaftMember", partition.volName, newHosts, newPeers, c); err != nil {
		return
	}
	if mr.Addr != removePeer.Addr {
		return
	}
	metaNode, err := c.metaNode(partition.Hosts[0])
	if err != nil {
		return
	}
	if err = partition.tryToChangeLeader(c, metaNode); err != nil {
		return
	}
	return
}

func (c *Cluster) updateMetaPartitionOfflinePeerIDWithLock(mp *MetaPartition, peerID uint64) (err error) {
	mp.Lock()
	defer mp.Unlock()
	mp.OfflinePeerID = peerID
	if err = mp.persistToRocksDB("updateMetaPartitionOfflinePeerIDWithLock", mp.volName, mp.Hosts, mp.Peers, c); err != nil {
		return
	}
	return
}

func (c *Cluster) addMetaReplica(partition *MetaPartition, addr string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addMetaReplica],vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
		}
	}()
	partition.Lock()
	defer partition.Unlock()
	if contains(partition.Hosts, addr) {
		err = fmt.Errorf("vol[%v],mp[%v] has contains host[%v]", partition.volName, partition.PartitionID, addr)
		return
	}
	metaNode, err := c.metaNode(addr)
	if err != nil {
		return
	}
	addPeer := proto.Peer{ID: metaNode.ID, Addr: addr, HeartbeatPort: metaNode.HeartbeatPort, ReplicaPort: metaNode.ReplicaPort}
	if err = c.addMetaPartitionRaftMember(partition, addPeer); err != nil {
		return
	}
	newHosts := append(partition.Hosts, addPeer.Addr)
	newPeers := append(partition.Peers, addPeer)
	if err = partition.persistToRocksDB("addMetaReplica", partition.volName, newHosts, newPeers, c); err != nil {
		return
	}
	if err = c.createMetaReplica(partition, addPeer); err != nil {
		return
	}
	if err = partition.afterCreation(addPeer.Addr, c); err != nil {
		return
	}
	return
}

func (c *Cluster) createMetaReplica(partition *MetaPartition, addPeer proto.Peer) (err error) {
	task, err := partition.createTaskToCreateReplica(addPeer.Addr)
	if err != nil {
		return
	}
	metaNode, err := c.metaNode(addPeer.Addr)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (c *Cluster) buildAddMetaPartitionRaftMemberTaskAndSyncSend(mp *MetaPartition, addPeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}

		if err != nil {
			log.LogErrorf("action[addMetaRaftMemberAndSend],vol[%v],meta partition[%v],resultCode[%v],err[%v]",
				mp.volName, mp.PartitionID, resultCode, err)
		} else {
			log.LogWarnf("action[addMetaRaftMemberAndSend],vol[%v],meta partition[%v],resultCode[%v]",
				mp.volName, mp.PartitionID, resultCode)
		}
	}()

	t, err := mp.createTaskToAddRaftMember(addPeer, leaderAddr)
	if err != nil {
		return
	}
	leaderMetaNode, err := c.metaNode(leaderAddr)
	if err != nil {
		return
	}
	if resp, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		return
	}
	return
}

func (c *Cluster) addMetaPartitionRaftMember(partition *MetaPartition, addPeer proto.Peer) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)
	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderMr, err := partition.getMetaReplicaLeader()
	if err == nil {
		leaderAddr = leaderMr.Addr
		if contains(partition.Hosts, leaderAddr) {
			candidateAddrs = append(candidateAddrs, leaderAddr)
		} else {
			leaderAddr = ""
		}
	}
	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	// send task to leader addr first,if need to retry,then send to other addr
	for index, host := range candidateAddrs {
		// wait for a new leader
		if leaderAddr == "" && len(candidateAddrs) < int(partition.ReplicaNum) {
			time.Sleep(retrySendSyncTaskInternal)
		}
		_, err = c.buildAddMetaPartitionRaftMemberTaskAndSyncSend(partition, addPeer, host)
		if err == nil {
			break
		}
		if index < len(candidateAddrs)-1 {
			time.Sleep(retrySendSyncTaskInternal)
		}
	}
	return
}

func (c *Cluster) loadMetaPartitionAndCheckResponse(mp *MetaPartition) {
	go func() {
		c.doLoadMetaPartition(mp)
	}()
}

func (c *Cluster) doLoadMetaPartition(mp *MetaPartition) {
	var wg sync.WaitGroup
	mp.Lock()
	hosts := make([]string, len(mp.Hosts))
	copy(hosts, mp.Hosts)
	mp.LoadResponse = make([]*proto.MetaPartitionLoadResponse, 0)
	mp.Unlock()
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
			response, err := mr.metaNode.Sender.syncSendAdminTask(task)
			if err != nil {
				errChannel <- err
				return
			}
			loadResponse := &proto.MetaPartitionLoadResponse{}
			if err = json.Unmarshal(response.Data, loadResponse); err != nil {
				errChannel <- err
				return
			}
			loadResponse.Addr = host
			mp.addOrReplaceLoadResponse(loadResponse)
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
	mp.checkSnapshot(c)
}

func (c *Cluster) doLoadDataPartition(dp *DataPartition) {
	log.LogInfof("action[doLoadDataPartition],partitionID:%d", dp.PartitionID)
	if !dp.needsToCompareCRC() {
		log.LogInfof("action[doLoadDataPartition],partitionID:%d isRecover[%t] don't need compare", dp.PartitionID, dp.isRecover)
		return
	}
	dp.resetFilesWithMissingReplica()
	loadTasks := dp.createLoadTasks()
	c.addDataNodeTasks(loadTasks)
	success := false
	for i := 0; i < timeToWaitForResponse; i++ {
		if dp.checkLoadResponse(c.getDataPartitionTimeoutSec()) {
			success = true
			break
		}
		time.Sleep(time.Second)
	}

	if !success {
		return
	}

	dp.getFileCount()
	if proto.IsNormalDp(dp.PartitionType) {
		dp.validateCRC(c.Name)
		dp.checkReplicaSize(c.Name, c.cfg.diffReplicaSpaceUsage)
	}

	dp.setToNormal()
}

func (c *Cluster) handleMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf("action[handleMetaNodeTaskResponse] receive Task response:%s from %s now:%d", task.IdString(), nodeAddr, time.Now().Unix())
	var metaNode *MetaNode

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
	case proto.OpDeleteMetaPartition:
		response := task.Response.(*proto.DeleteMetaPartitionResponse)
		err = c.dealDeleteMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpUpdateMetaPartition:
		response := task.Response.(*proto.UpdateMetaPartitionResponse)
		err = c.dealUpdateMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpVersionOperation:
		response := task.Response.(*proto.MultiVersionOpResponse)
		err = c.dealOpMetaNodeMultiVerResp(task.OperatorAddr, response)
	default:
		err := fmt.Errorf("unknown operate code %v", task.OpCode)
		log.LogError(err)
	}

	if err != nil {
		log.LogErrorf("process task[%s] failed", task.ToString())
	} else {
		log.LogInfof("[handleMetaNodeTaskResponse] process task:%v status:%v success", task.IdString(), task.Status)
	}
	return
errHandler:
	log.LogErrorf("action[handleMetaNodeTaskResponse],nodeAddr %s,taskId %s,err %v",
		nodeAddr, task.IdString(), err)
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

func (c *Cluster) dealOpMetaNodeMultiVerResp(nodeAddr string, resp *proto.MultiVersionOpResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealOpMetaNodeMultiVerResp],clusterID[%v] volume [%v] nodeAddr %v operate meta partition snapshot version,err %v",
			c.Name, resp.VolumeID, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	var vol *Vol
	if vol, err = c.getVol(resp.VolumeID); err != nil {
		return
	}
	vol.VersionMgr.handleTaskRsp(resp, TypeMetaPartition)
	return
}

func (c *Cluster) dealOpDataNodeMultiVerResp(nodeAddr string, resp *proto.MultiVersionOpResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealOpMetaNodeMultiVerResp],clusterID[%v] volume [%v] nodeAddr %v operate meta partition snapshot version,err %v",
			c.Name, resp.VolumeID, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	var vol *Vol
	if vol, err = c.getVol(resp.VolumeID); err != nil {
		return
	}
	vol.VersionMgr.handleTaskRsp(resp, TypeDataPartition)
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
	log.LogErrorf("dealDeleteMetaPartitionResp %v", err)
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

	if metaNode.ToBeOffline {
		log.LogInfof("action[dealMetaNodeHeartbeatResp] dataNode is toBeOffline, addr[%s]", nodeAddr)
		return
	}

	if resp.ZoneName == "" {
		resp.ZoneName = DefaultZoneName
	}

	if metaNode.ZoneName != resp.ZoneName {
		c.t.deleteMetaNode(metaNode)
		oldZoneName := metaNode.ZoneName
		metaNode.ZoneName = resp.ZoneName
		c.adjustMetaNode(metaNode)
		log.LogWarnf("metaNode zone changed from [%v] to [%v]", oldZoneName, resp.ZoneName)
	}

	metaNode.ReceivedForbidWriteOpOfProtoVer0 = resp.ReceivedForbidWriteOpOfProtoVer0
	if metaNode.ReceivedForbidWriteOpOfProtoVer0 != c.cfg.forbidWriteOpOfProtoVer0 {
		log.LogWarnf("[dealMetaNodeHeartbeatResp] metaNode[%v] ReceivedForbidWriteOpOfProtoVer0(%v) is different from master forbidWriteOpOfProtoVer0(%v)",
			metaNode.Addr, metaNode.ReceivedForbidWriteOpOfProtoVer0, c.cfg.forbidWriteOpOfProtoVer0)
	}

	// change cpu util and io used
	metaNode.CpuUtil.Store(resp.CpuUtil)
	metaNode.updateMetric(resp, c.cfg.MetaNodeThreshold)
	metaNode.setNodeActive()

	if err = c.t.putMetaNode(metaNode); err != nil {
		log.LogErrorf("action[dealMetaNodeHeartbeatResp],metaNode[%v] error[%v]", metaNode.Addr, err)
	}
	c.updateMetaNode(metaNode, resp.MetaPartitionReports, metaNode.reachesThreshold())
	// todo remove, this no need set metaNode.metaPartitionInfos = nil
	// metaNode.metaPartitionInfos = nil
	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeatResp],metaNode:%v,zone[%v], ReportTime:%v  success", metaNode.Addr, metaNode.ZoneName, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) adjustMetaNode(metaNode *MetaNode) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	oldNodeSetID := metaNode.NodeSetID
	var err error
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[adjustMetaNode],clusterID[%v] addr:%v,zone[%v] err:%v ", c.Name, metaNode.Addr, metaNode.ZoneName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(metaNode.ZoneName)
	if err != nil {
		zone = newZone(metaNode.ZoneName, proto.MediaType_Unspecified)
		c.t.putZone(zone)
	}
	c.nsMutex.Lock()
	ns := zone.getAvailNodeSetForMetaNode()
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			c.nsMutex.Unlock()
			return
		}
	}
	c.nsMutex.Unlock()

	metaNode.NodeSetID = ns.ID
	if err = c.syncUpdateMetaNode(metaNode); err != nil {
		metaNode.NodeSetID = oldNodeSetID
		return
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		return
	}
	err = c.t.putMetaNode(metaNode)
}

func (c *Cluster) handleDataNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("action[handleDataNodeTaskResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("action[handleDataNodeTaskResponse] receive addr[%v] task response:%v", nodeAddr, task.ToString())
	}
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
		err = c.handleDataNodeHeartbeatResp(task.OperatorAddr, response, task.RequestID)
	case proto.OpVersionOperation:
		response := task.Response.(*proto.MultiVersionOpResponse)
		err = c.dealOpDataNodeMultiVerResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf("unknown operate code %d", task.OpCode)
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogErrorf("process task[%v] failed,err:%v", task.ToString(), err)
}

func (c *Cluster) dealDeleteDataPartitionResponse(nodeAddr string, resp *proto.DeleteDataPartitionResponse) (err error) {
	var dp *DataPartition
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
	if resp.Status == proto.TaskFailed || resp.PartitionSnapshot == nil {
		return
	}
	var (
		dataNode *DataNode
		dp       *DataPartition
		vol      *Vol
	)
	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		return
	}
	if resp.VolName != "" {
		vol, err = c.getVol(resp.VolName)
		if err != nil {
			return
		}
		dp, err = vol.getDataPartitionByID(resp.PartitionId)
	} else {
		dp, err = c.getDataPartitionByID(resp.PartitionId)
	}
	if err != nil {
		return
	}
	dp.loadFile(dataNode, resp)

	return
}

func (c *Cluster) handleDataNodeHeartbeatResp(nodeAddr string, resp *proto.DataNodeHeartbeatResponse, reqId string) (err error) {
	var (
		dataNode *DataNode
		logMsg   string
	)
	log.LogInfof("action[handleDataNodeHeartbeatResp] clusterID[%v] receive dataNode[%v] heartbeat %v ",
		c.Name, nodeAddr, reqId)
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleDataNodeHeartbeatResp] clusterID[%v] dataNode[%v] heartbeat task failed",
			c.Name, nodeAddr))
		return
	}

	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	if dataNode.ToBeOffline {
		log.LogInfof("action[handleDataNodeHeartbeatResp] dataNode is toBeOffline, addr[%s]", nodeAddr)
		// return
	}
	if resp.ZoneName == "" {
		resp.ZoneName = DefaultZoneName
	}
	if dataNode.ZoneName != resp.ZoneName {
		c.t.deleteDataNode(dataNode)
		oldZoneName := dataNode.ZoneName
		dataNode.ZoneName = resp.ZoneName
		c.dnMutex.Lock()
		c.adjustDataNode(dataNode)
		c.dnMutex.Unlock()
		log.LogWarnf("dataNode [%v] zone changed from [%v] to [%v]", dataNode.Addr, oldZoneName, resp.ZoneName)
	}
	// change cpu util and io used
	dataNode.CpuUtil.Store(resp.CpuUtil)
	dataNode.SetIoUtils(resp.IoUtils)

	dataNode.updateNodeMetric(c, resp)

	if err = c.t.putDataNode(dataNode); err != nil {
		log.LogErrorf("action[handleDataNodeHeartbeatResp] dataNode[%v],zone[%v],node set[%v], err[%v]", dataNode.Addr, dataNode.ZoneName, dataNode.NodeSetID, err)
	}
	c.updateDataNode(dataNode, resp.PartitionReports)

	dataNode.ReceivedForbidWriteOpOfProtoVer0 = resp.ReceivedForbidWriteOpOfProtoVer0
	if dataNode.ReceivedForbidWriteOpOfProtoVer0 != c.cfg.forbidWriteOpOfProtoVer0 {
		log.LogWarnf("[handleDataNodeHeartbeatResp] dataNode[%v] receivedForbiddenWriteOpVerBitmask(%v) is different from master forbidWriteOpOfProtoVer0(%v)",
			dataNode.Addr, dataNode.ReceivedForbidWriteOpOfProtoVer0, c.cfg.forbidWriteOpOfProtoVer0)
	}

	logMsg = fmt.Sprintf("action[handleDataNodeHeartbeatResp],dataNode:%v,zone[%v], ReportTime:%v  success", dataNode.Addr, dataNode.ZoneName, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

func (c *Cluster) adjustDataNode(dataNode *DataNode) {
	oldNodeSetID := dataNode.NodeSetID
	var err error
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[adjustDataNode],clusterID[%v] dataNodeAddr:%v,zone[%v] err:%v ", c.Name, dataNode.Addr, dataNode.ZoneName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(dataNode.ZoneName)
	if err != nil {
		zone = newZone(dataNode.ZoneName, dataNode.MediaType)
		c.t.putZone(zone)
	}

	c.nsMutex.Lock()
	ns := zone.getAvailNodeSetForDataNode()
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			c.nsMutex.Unlock()
			return
		}
	}
	c.nsMutex.Unlock()

	if _, err = c.checkSetZoneMediaTypePersist(zone, dataNode.MediaType); err != nil {
		return
	}

	dataNode.NodeSetID = ns.ID
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		dataNode.NodeSetID = oldNodeSetID
		return
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		return
	}
	err = c.t.putDataNode(dataNode)
}

/*if node report data partition infos,so range data partition infos,then update data partition info*/
func (c *Cluster) updateDataNode(dataNode *DataNode, dps []*proto.DataPartitionReport) {
	for _, vr := range dps {
		if vr == nil {
			continue
		}
		if vr.VolName != "" {
			vol, err := c.getVol(vr.VolName)
			if err != nil {
				continue
			}
			//if vol.Status == proto.VolStatusMarkDelete {
			//	continue
			//}
			if dp, err := vol.getDataPartitionByID(vr.PartitionID); err == nil {
				dp.updateMetric(vr, dataNode, c)
			}
		} else {
			if dp, err := c.getDataPartitionByID(vr.PartitionID); err == nil {
				dp.updateMetric(vr, dataNode, c)
			}
		}
	}
}

func (c *Cluster) updateMetaNode(metaNode *MetaNode, metaPartitions []*proto.MetaPartitionReport, threshold bool) {
	var (
		vol *Vol
		err error
	)
	for _, mr := range metaPartitions {
		if mr == nil {
			continue
		}
		var mp *MetaPartition
		if mr.VolName != "" {

			vol, err = c.getVol(mr.VolName)
			if err != nil {
				continue
			}

			//if vol.Status == proto.VolStatusMarkDelete {
			//	continue
			//}

			mp, err = vol.metaPartition(mr.PartitionID)
			if err != nil {
				continue
			}

		} else {
			mp, err = c.getMetaPartitionByID(mr.PartitionID)
			if err != nil {
				continue
			}
		}

		// send latest end to replica metanode, including updating the end after MaxMP split when the old MaxMP is unavailable
		if mr.End != mp.End {
			mp.addUpdateMetaReplicaTask(c)
		}

		mp.updateMetaPartition(mr, metaNode, c)
		vol.uidSpaceManager.pushUidMsg(mr)
		vol.quotaManager.quotaUpdate(mr)
		c.updateInodeIDUpperBound(mp, mr, threshold, metaNode)
	}
}

func (c *Cluster) updateInodeIDUpperBound(mp *MetaPartition, mr *proto.MetaPartitionReport, hasArriveThreshold bool, metaNode *MetaNode) (err error) {
	if !hasArriveThreshold {
		return
	}
	var vol *Vol
	if vol, err = c.getVol(mp.volName); err != nil {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] not found", mp.volName)
		return
	}

	maxPartitionID := vol.maxMetaPartitionID()
	if mr.PartitionID < maxPartitionID {
		return
	}
	var end uint64
	metaPartitionInodeIdStep := gConfig.MetaPartitionInodeIdStep
	if mr.MaxInodeID <= 0 {
		end = mr.Start + metaPartitionInodeIdStep
	} else {
		end = mr.MaxInodeID + metaPartitionInodeIdStep
	}
	log.LogWarnf("mpId[%v],start[%v],end[%v],addr[%v],used[%v]", mp.PartitionID, mp.Start, mp.End, metaNode.Addr, metaNode.Used)
	if c.cfg.DisableAutoCreate {
		log.LogWarnf("updateInodeIDUpperBound: disable auto create meta partition, mp %d", mp.PartitionID)
		return
	}
	if err = vol.splitMetaPartition(c, mp, end, metaPartitionInodeIdStep, false); err != nil {
		log.LogErrorf("mpId[%v], splitMetaPartition err %v", mp.PartitionID, err)
	}
	return
}
