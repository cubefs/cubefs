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
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"strings"
	"sync"
	"time"
)

// DataPartition represents the structure of storing the file contents.
type DataPartition struct {
	PartitionID             uint64
	LastLoadedTime          int64
	ReplicaNum              uint8
	Status                  int8
	isRecover               bool
	Replicas                []*DataReplica
	Hosts                   []string // host addresses
	Peers                   []proto.Peer
	sync.RWMutex
	total                   uint64
	used                    uint64
	MissingNodes            map[string]int64 // key: address of the missing node, value: when the node is missing
	VolName                 string
	VolID                   uint64
	modifyTime              int64
	createTime              int64
	FileInCoreMap           map[string]*FileInCore
	FilesWithMissingReplica map[string]int64 // key: file name, value: last time when a missing replica is found
}

func newDataPartition(ID uint64, replicaNum uint8, volName string, volID uint64) (partition *DataPartition) {
	partition = new(DataPartition)
	partition.ReplicaNum = replicaNum
	partition.PartitionID = ID
	partition.Hosts = make([]string, 0)
	partition.Peers = make([]proto.Peer, 0)
	partition.Replicas = make([]*DataReplica, 0)
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.FilesWithMissingReplica = make(map[string]int64)
	partition.MissingNodes = make(map[string]int64)

	partition.Status = proto.ReadOnly
	partition.VolName = volName
	partition.VolID = volID
	partition.modifyTime = time.Now().Unix()
	partition.createTime = time.Now().Unix()
	return
}

func (partition *DataPartition) addReplica(replica *DataReplica) {
	for _, r := range partition.Replicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	partition.Replicas = append(partition.Replicas, replica)
}

func (partition *DataPartition) createTaskToCreateDataPartition(addr string, dataPartitionSize uint64) (task *proto.AdminTask) {

	task = proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateDataPartitionRequest(
		partition.VolName, partition.PartitionID, partition.Peers, int(dataPartitionSize)))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToDeleteDataPartition(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpDeleteDataPartition, addr, newDeleteDataPartitionRequest(partition.PartitionID))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToDecommissionDataPartition(removePeer proto.Peer, addPeer proto.Peer) (task *proto.AdminTask, err error) {
	leaderAddr := partition.getLeaderAddr()
	if leaderAddr == "" {
		err = proto.ErrNoLeader
		return
	}
	task = proto.NewAdminTask(proto.OpDecommissionDataPartition, leaderAddr, newOfflineDataPartitionRequest(partition.PartitionID, removePeer, addPeer))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) resetTaskID(t *proto.AdminTask) {
	t.ID = fmt.Sprintf("%v_DataPartitionID[%v]", t.ID, partition.PartitionID)
}

// Check if there is a replica missing or not.
func (partition *DataPartition) hasMissingOneReplica(replicaNum int) (err error) {
	hostNum := len(partition.Replicas)
	if hostNum <= replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneReplica", partition.PartitionID, proto.ErrHasOneMissingReplica))
		err = proto.ErrHasOneMissingReplica
	}
	return
}

func (partition *DataPartition) canBeOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],partitionID:%v  RocksDBHost:%v  offLine:%v ",
		partition.PartitionID, partition.Hosts, offlineAddr)
	liveReplicas := partition.liveReplicas(defaultDataPartitionTimeOutSec)

	otherLiveReplicas := make([]*DataReplica, 0)
	for i := 0; i < len(liveReplicas); i++ {
		replica := partition.Replicas[i]
		if replica.Addr != offlineAddr {
			otherLiveReplicas = append(otherLiveReplicas, replica)
		}
	}

	if len(otherLiveReplicas) < int(partition.ReplicaNum/2) {
		msg = fmt.Sprintf(msg+" err:%v  liveReplicas:%v ", proto.ErrCannotBeOffLine, len(liveReplicas))
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}

	return
}

func (partition *DataPartition) logDecommissionedDataPartition(addr string) (msg string) {
	msg = fmt.Sprintf("action[logDecommissionedDataPartition],data partition:%v  offlineaddr:%v  ",
		partition.PartitionID, addr)
	replicas := partition.availableDataReplicas()
	for i := 0; i < len(replicas); i++ {
		replica := replicas[i]
		msg += fmt.Sprintf(" addr:%v  dataReplicaStatus:%v  FileCount :%v ", replica.Addr,
			replica.Status, replica.FileCount)
	}
	log.LogWarn(msg)

	return
}

// get all the valid replicas of the given data partition
func (partition *DataPartition) availableDataReplicas() (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]

		// the node reports heartbeat normally and the node is available
		if replica.isLocationAvailable() == true && partition.hasHost(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

// Remove the replica address from the memory.
func (partition *DataPartition) removeReplicaByAddr(addr string) {
	delIndex := -1
	var replica *DataReplica
	for i := 0; i < len(partition.Replicas); i++ {
		replica = partition.Replicas[i]
		if replica.Addr == addr {
			delIndex = i
			break
		}
	}

	msg := fmt.Sprintf("action[removeReplicaByAddr],data partition:%v  on Node:%v  OffLine,the node is in replicas:%v", partition.PartitionID, addr, replica != nil)
	log.LogDebug(msg)
	if delIndex == -1 {
		return
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.deleteReplicaByIndex(delIndex)
	partition.modifyTime = time.Now().Unix()

	return
}

func (partition *DataPartition) deleteReplicaByIndex(index int) {
	var replicaAddrs []string
	for _, replica := range partition.Replicas {
		replicaAddrs = append(replicaAddrs, replica.Addr)
	}
	msg := fmt.Sprintf("deleteReplicaByIndex replica:%v  index:%v  locations :%v ", partition.PartitionID, index, replicaAddrs)
	log.LogInfo(msg)
	replicasAfter := partition.Replicas[index+1:]
	partition.Replicas = partition.Replicas[:index]
	partition.Replicas = append(partition.Replicas, replicasAfter...)
}

func (partition *DataPartition) createLoadTasks() (tasks []*proto.AdminTask) {

	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.Hosts {
		replica, err := partition.getReplica(addr)
		if err != nil || replica.isLive(defaultDataPartitionTimeOutSec) == false {
			continue
		}
		replica.HasLoadResponse = false
		tasks = append(tasks, partition.createLoadTask(addr))
	}
	partition.LastLoadedTime = time.Now().Unix()
	return
}

func (partition *DataPartition) createLoadTask(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpLoadDataPartition, addr, newLoadDataPartitionMetricRequest(partition.PartitionID))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) getReplica(addr string) (replica *DataReplica, err error) {
	for index := 0; index < len(partition.Replicas); index++ {
		replica = partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogErrorf("action[getReplica],partitionID:%v,locations:%v,err:%v",
		partition.PartitionID, addr, dataReplicaNotFound(addr))
	return nil, errors.Trace(dataReplicaNotFound(addr), "%v not found", addr)
}

func (partition *DataPartition) convertToDataPartitionResponse() (dpr *proto.DataPartitionResponse) {
	dpr = new(proto.DataPartitionResponse)
	partition.Lock()
	defer partition.Unlock()
	dpr.PartitionID = partition.PartitionID
	dpr.Status = partition.Status
	dpr.ReplicaNum = partition.ReplicaNum
	dpr.Hosts = make([]string, len(partition.Hosts))
	copy(dpr.Hosts, partition.Hosts)
	dpr.LeaderAddr = partition.getLeaderAddr()
	return
}

func (partition *DataPartition) getLeaderAddr() (leaderAddr string) {
	for _, replica := range partition.Replicas {
		if replica.IsLeader {
			return replica.Addr
		}
	}
	return
}

func (partition *DataPartition) checkLoadResponse(timeOutSec int64) (isResponse bool) {
	partition.RLock()
	defer partition.RUnlock()
	for _, addr := range partition.Hosts {
		replica, err := partition.getReplica(addr)
		if err != nil {
			return
		}
		timePassed := time.Now().Unix() - partition.LastLoadedTime
		if replica.HasLoadResponse == false && timePassed > timeToWaitForResponse {
			msg := fmt.Sprintf("action[checkLoadResponse], partitionID:%v on Node:%v no response, spent time %v s",
				partition.PartitionID, addr, timePassed)
			log.LogWarn(msg)
			return
		}
		if replica.isLive(timeOutSec) == false || replica.HasLoadResponse == false {
			return
		}
	}
	isResponse = true

	return
}

func (partition *DataPartition) getReplicaByIndex(index uint8) (replica *DataReplica) {
	return partition.Replicas[int(index)]
}

func (partition *DataPartition) getFileCount() {
	var msg string
	filesToBeDeleted := make([]string, 0)
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		replica.FileCount = 0
	}
	for _, fc := range partition.FileInCoreMap {
		if len(fc.MetadataArray) == 0 {
			filesToBeDeleted = append(filesToBeDeleted, fc.Name)
		}
		for _, vfNode := range fc.MetadataArray {
			replica := partition.getReplicaByIndex(vfNode.locIndex)
			replica.FileCount++
		}
	}

	for _, vfName := range filesToBeDeleted {
		delete(partition.FileInCoreMap, vfName)
	}

	for _, replica := range partition.Replicas {
		msg = fmt.Sprintf(getFileCountOnDataReplica + "partitionID:%v  replicaAddr:%v  FileCount:%v  "+
			"NodeIsActive:%v  replicaIsActive:%v  .replicaStatusOnNode:%v ", partition.PartitionID, replica.Addr, replica.FileCount,
			replica.getReplicaNode().isActive, replica.isActive(defaultDataPartitionTimeOutSec), replica.Status)
		log.LogInfo(msg)
	}

}

// Release the memory occupied by the data partition.
func (partition *DataPartition) releaseDataPartition() {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasFromHosts(defaultDataPartitionTimeOutSec)
	for _, replica := range liveReplicas {
		replica.HasLoadResponse = false
	}
	for name, fc := range partition.FileInCoreMap {
		fc.MetadataArray = nil
		delete(partition.FileInCoreMap, name)
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	for name, fileMissReplicaTime := range partition.FilesWithMissingReplica {
		if time.Now().Unix()-fileMissReplicaTime > 2*intervalToLoadDataPartition {
			delete(partition.FilesWithMissingReplica, name)
		}
	}

}

func (partition *DataPartition) hasReplica(host string) (replica *DataReplica, ok bool) {
	// using loop instead of map to save the memory
	for _, replica = range partition.Replicas {
		if replica.Addr == host {
			ok = true
			break
		}
	}
	return
}

func (partition *DataPartition) checkReplicaNum(c *Cluster, volName string) {
	partition.RLock()
	defer partition.RUnlock()
	if int(partition.ReplicaNum) != len(partition.Hosts) {
		msg := fmt.Sprintf("FIX DataPartition replicaNum,clusterID[%v] volName[%v] partitionID:%v orgReplicaNum:%v",
			c.Name, volName, partition.PartitionID, partition.ReplicaNum)
		Warn(c.Name, msg)
	}
}

func (partition *DataPartition) hostsToString() (hosts string) {
	return strings.Join(partition.Hosts, underlineSeparator)
}

func (partition *DataPartition) setToNormal() {
	partition.Lock()
	defer partition.Unlock()
	partition.isRecover = false
}

func (partition *DataPartition) setStatus(status int8) {
	partition.Lock()
	defer partition.Unlock()
	partition.Status = status
}

func (partition *DataPartition) hasHost(addr string) (ok bool) {
	for _, host := range partition.Hosts {
		if host == addr {
			ok = true
			break
		}
	}
	return
}

func (partition *DataPartition) liveReplicas(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if replica.isLive(timeOutSec) == true && partition.hasHost(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

// get all the live replicas from the persistent hosts
func (partition *DataPartition) getLiveReplicasFromHosts(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, host := range partition.Hosts {
		replica, ok := partition.hasReplica(host)
		if !ok {
			continue
		}
		if replica.isLive(timeOutSec) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

func (partition *DataPartition) checkAndRemoveMissReplica(addr string) {
	if _, ok := partition.MissingNodes[addr]; ok {
		delete(partition.MissingNodes, addr)
	}
}

func (partition *DataPartition) loadFile(dataNode *DataNode, resp *proto.LoadDataPartitionResponse) {
	partition.Lock()
	defer partition.Unlock()

	index, err := partition.getReplicaIndex(dataNode.Addr)
	if err != nil {
		msg := fmt.Sprintf("loadFile partitionID:%v  on Node:%v  don't report :%v ", partition.PartitionID, dataNode.Addr, err)
		log.LogWarn(msg)
		return
	}
	replica := partition.Replicas[index]
	for _, dpf := range resp.PartitionSnapshot {
		if dpf == nil {
			continue
		}
		fc, ok := partition.FileInCoreMap[dpf.Name]
		if !ok {
			fc = newFileInCore(dpf.Name)
			partition.FileInCoreMap[dpf.Name] = fc
		}
		fc.updateFileInCore(partition.PartitionID, dpf, replica, index)
	}
	replica.HasLoadResponse = true
}

func (partition *DataPartition) getReplicaIndex(addr string) (index int, err error) {
	for index = 0; index < len(partition.Replicas); index++ {
		replica := partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogErrorf("action[getReplicaIndex],partitionID:%v,location:%v,err:%v",
		partition.PartitionID, addr, dataReplicaNotFound(addr))
	return -1, errors.Trace(dataReplicaNotFound(addr), "%v not found ", addr)
}

func (partition *DataPartition) updateForOffline(offlineAddr, newAddr, volName string, newPeers []proto.Peer, c *Cluster) (err error) {
	orgHosts := make([]string, len(partition.Hosts))
	copy(orgHosts, partition.Hosts)
	oldPeers := make([]proto.Peer, len(partition.Peers))
	copy(oldPeers, partition.Peers)
	newHosts := make([]string, 0)
	for index, addr := range partition.Hosts {
		if addr == offlineAddr {
			after := partition.Hosts[index+1:]
			newHosts = partition.Hosts[:index]
			newHosts = append(newHosts, after...)
			break
		}
	}
	newHosts = append(newHosts, newAddr)
	partition.Hosts = newHosts
	partition.Peers = newPeers
	if err = c.syncUpdateDataPartition(partition); err != nil {
		partition.Hosts = orgHosts
		partition.Peers = oldPeers
		return errors.Trace(err, "update partition[%v] failed", partition.PartitionID)
	}
	msg := fmt.Sprintf("action[updateForOffline]  partitionID:%v offlineAddr:%v newAddr:%v"+
		"oldHosts:%v newHosts:%v,oldPees[%v],newPeers[%v]",
		partition.PartitionID, offlineAddr, newAddr, orgHosts, partition.Hosts, oldPeers, newPeers)
	log.LogInfo(msg)
	return
}

func (partition *DataPartition) updateMetric(vr *proto.PartitionReport, dataNode *DataNode, c *Cluster) {

	if !partition.hasHost(dataNode.Addr) {
		return
	}
	partition.Lock()
	defer partition.Unlock()
	replica, err := partition.getReplica(dataNode.Addr)
	if err != nil {
		replica = newDataReplica(dataNode)
		partition.addReplica(replica)
	}
	partition.total = vr.Total
	partition.used = vr.Used
	replica.Status = int8(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	replica.FileCount = uint32(vr.ExtentCount)
	replica.setAlive()
	replica.IsLeader = vr.IsLeader
	replica.NeedsToCompare = vr.NeedCompare
	if replica.DiskPath != vr.DiskPath && vr.DiskPath != "" {
		replica.DiskPath = vr.DiskPath
		c.syncUpdateDataPartition(partition)
	}
	partition.checkAndRemoveMissReplica(dataNode.Addr)
}

func (partition *DataPartition) getMaxUsedSpace() uint64 {
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		if replica.Used > partition.used {
			partition.used = replica.Used
		}
	}
	return partition.used
}

func (partition *DataPartition) afterCreation(nodeAddr, diskPath string, c *Cluster) (err error) {
	dataNode, err := c.dataNode(nodeAddr)
	if err != nil {
		return err
	}
	replica := newDataReplica(dataNode)
	replica.Status = proto.ReadWrite
	replica.DiskPath = diskPath
	partition.addReplica(replica)
	partition.checkAndRemoveMissReplica(replica.Addr)
	return
}

// Check if the replica's size is aligned or not.
func (partition *DataPartition) isReplicaSizeAligned() bool {
	if len(partition.Replicas) == 0 {
		return true
	}
	used := partition.Replicas[0].Used

	var diff float64
	for _, replica := range partition.Replicas {
		tempDiff := math.Abs(float64(replica.Used) - float64(used))
		if tempDiff > diff {
			diff = tempDiff
		}
	}
	if diff < float64(util.GB) {
		return true
	}
	return false
}

// Check if it makes sense to compare the CRC.
// Note that if loading the data into a data node is not finished, then there is no need to check the CRC.
func (partition *DataPartition) needsToCompareCRC() (needCompare bool) {
	partition.Lock()
	defer partition.Unlock()
	if partition.isRecover {
		return false
	}
	needCompare = true
	for _, replica := range partition.Replicas {
		if !replica.NeedsToCompare {
			needCompare = false
			break
		}
	}
	return
}

func (partition *DataPartition) containsBadDisk(diskPath string, nodeAddr string) bool {
	partition.RLock()
	defer partition.RUnlock()
	for _, replica := range partition.Replicas {
		if nodeAddr == replica.Addr && diskPath == replica.DiskPath {
			return true
		}
	}
	return false
}
