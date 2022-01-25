// Copyright 2018 The Cubefs Authors.
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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

// DataPartition represents the structure of storing the file contents.
type DataPartition struct {
	PartitionID    uint64
	PartitionType  int
	PartitionTTL   int64
	LastLoadedTime int64
	ReplicaNum     uint8
	Status         int8
	isRecover      bool
	Replicas       []*DataReplica
	Hosts          []string // host addresses
	Peers          []proto.Peer
	offlineMutex   sync.RWMutex
	sync.RWMutex
	total                   uint64
	used                    uint64
	MissingNodes            map[string]int64 // key: address of the missing node, value: when the node is missing
	VolName                 string
	VolID                   uint64
	modifyTime              int64
	createTime              int64
	lastWarnTime            int64
	OfflinePeerID           uint64
	FileInCoreMap           map[string]*FileInCore
	FilesWithMissingReplica map[string]int64 // key: file name, value: last time when a missing replica is found
}

type DataPartitionPreLoad struct {
	PreloadCacheTTL      uint64
	preloadCacheCapacity int
	preloadReplicaNum    int
	preloadZoneName      string
}

func (d *DataPartitionPreLoad) toString() string {
	return fmt.Sprintf("PreloadCacheTTL[%d]_preloadCacheCapacity[%d]_preloadReplicaNum[%d]_preloadZoneName[%s]",
		d.PreloadCacheTTL, d.preloadCacheCapacity, d.preloadReplicaNum, d.preloadZoneName)
}

func newDataPartition(ID uint64, replicaNum uint8, volName string, volID uint64, partitionType int, partitionTTL int64) (partition *DataPartition) {
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
	partition.PartitionType = partitionType
	partition.PartitionTTL = partitionTTL

	partition.modifyTime = time.Now().Unix()
	partition.createTime = time.Now().Unix()
	partition.lastWarnTime = time.Now().Unix()
	return
}

func (partition *DataPartition) resetFilesWithMissingReplica() {
	partition.Lock()
	defer partition.Unlock()
	partition.FilesWithMissingReplica = make(map[string]int64)
}

func (partition *DataPartition) dataNodeStartTime() int64 {
	partition.Lock()
	defer partition.Unlock()
	startTime := int64(0)
	for _, replica := range partition.Replicas {
		if startTime < replica.dataNode.StartTime {
			startTime = replica.dataNode.StartTime
		}
	}

	return startTime
}

func (partition *DataPartition) addReplica(replica *DataReplica) {
	for _, r := range partition.Replicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	partition.Replicas = append(partition.Replicas, replica)
}

func (partition *DataPartition) tryToChangeLeader(c *Cluster, dataNode *DataNode) (err error) {
	task, err := partition.createTaskToTryToChangeLeader(dataNode.Addr)
	if err != nil {
		return
	}
	if _, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (partition *DataPartition) prepareAddRaftMember(addPeer proto.Peer) (leaderAddr string, candidateAddrs []string, err error) {
	partition.RLock()
	defer partition.RUnlock()
	if contains(partition.Hosts, addPeer.Addr) {
		err = fmt.Errorf("vol[%v],data partition[%v] has contains host[%v]", partition.VolName, partition.PartitionID, addPeer.Addr)
		return
	}
	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderAddr = partition.getLeaderAddr()
	if leaderAddr != "" && contains(partition.Hosts, leaderAddr) {
		candidateAddrs = append(candidateAddrs, leaderAddr)
	} else {
		leaderAddr = ""
	}
	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	return
}

func (partition *DataPartition) createTaskToTryToChangeLeader(addr string) (task *proto.AdminTask, err error) {
	task = proto.NewAdminTask(proto.OpDataPartitionTryToLeader, addr, nil)
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToAddRaftMember(addPeer proto.Peer, leaderAddr string) (task *proto.AdminTask, err error) {
	task = proto.NewAdminTask(proto.OpAddDataPartitionRaftMember, leaderAddr, newAddDataPartitionRaftMemberRequest(partition.PartitionID, addPeer))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToRemoveRaftMember(removePeer proto.Peer) (task *proto.AdminTask, err error) {
	leaderAddr := partition.getLeaderAddr()
	if leaderAddr == "" {
		err = proto.ErrNoLeader
		return
	}
	task = proto.NewAdminTask(proto.OpRemoveDataPartitionRaftMember, leaderAddr, newRemoveDataPartitionRaftMemberRequest(partition.PartitionID, removePeer))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToCreateDataPartition(addr string, dataPartitionSize uint64,
	peers []proto.Peer, hosts []string, createType int, partitionType int) (task *proto.AdminTask) {

	task = proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateDataPartitionRequest(
		partition.VolName, partition.PartitionID, peers, int(dataPartitionSize), hosts, createType, partitionType))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToDeleteDataPartition(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpDeleteDataPartition, addr, newDeleteDataPartitionRequest(partition.PartitionID))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) resetTaskID(t *proto.AdminTask) {
	t.ID = fmt.Sprintf("%v_DataPartitionID[%v]", t.ID, partition.PartitionID)
	t.PartitionID = partition.PartitionID
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
		replica := liveReplicas[i]
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
	dpr.PartitionType = partition.PartitionType
	dpr.PartitionTTL = partition.PartitionTTL
	dpr.Status = partition.Status
	dpr.ReplicaNum = partition.ReplicaNum
	dpr.Hosts = make([]string, len(partition.Hosts))
	copy(dpr.Hosts, partition.Hosts)
	dpr.LeaderAddr = partition.getLeaderAddr()
	dpr.IsRecover = partition.isRecover

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

func (partition *DataPartition) getLeaderAddrWithLock() (leaderAddr string) {
	partition.RLock()
	defer partition.RUnlock()
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

func (partition *DataPartition) checkReplicaNum(c *Cluster, vol *Vol) {
	partition.RLock()
	defer partition.RUnlock()

	if int(partition.ReplicaNum) != len(partition.Hosts) {
		msg := fmt.Sprintf("FIX DataPartition replicaNum,clusterID[%v] volName[%v] partitionID:%v orgReplicaNum:%v",
			c.Name, vol.Name, partition.PartitionID, partition.ReplicaNum)
		Warn(c.Name, msg)
	}

	if vol.dpReplicaNum != partition.ReplicaNum && !vol.NeedToLowerReplica {
		vol.NeedToLowerReplica = true
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

// get all the live replicas from the persistent hosts
func (partition *DataPartition) getLiveReplicas(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, replica := range partition.Replicas {
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
	replica.Used = resp.Used
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

func (partition *DataPartition) update(action, volName string, newPeers []proto.Peer, newHosts []string, c *Cluster) (err error) {
	orgHosts := make([]string, len(partition.Hosts))
	copy(orgHosts, partition.Hosts)
	oldPeers := make([]proto.Peer, len(partition.Peers))
	copy(oldPeers, partition.Peers)
	partition.Hosts = newHosts
	partition.Peers = newPeers
	if err = c.syncUpdateDataPartition(partition); err != nil {
		partition.Hosts = orgHosts
		partition.Peers = oldPeers
		return errors.Trace(err, "action[%v] update partition[%v] vol[%v] failed", action, partition.PartitionID, volName)
	}
	msg := fmt.Sprintf("action[%v] success,vol[%v] partitionID:%v "+
		"oldHosts:%v newHosts:%v,oldPees[%v],newPeers[%v]",
		action, volName, partition.PartitionID, orgHosts, partition.Hosts, oldPeers, partition.Peers)
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
	replica.Status = int8(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	partition.setMaxUsed()
	replica.FileCount = uint32(vr.ExtentCount)
	replica.setAlive()
	replica.IsLeader = vr.IsLeader
	replica.NeedsToCompare = vr.NeedCompare
	if replica.DiskPath != vr.DiskPath && vr.DiskPath != "" {
		oldDiskPath := replica.DiskPath
		replica.DiskPath = vr.DiskPath
		err = c.syncUpdateDataPartition(partition)
		if err != nil {
			replica.DiskPath = oldDiskPath
		}
	}
	partition.checkAndRemoveMissReplica(dataNode.Addr)
}

func (partition *DataPartition) setMaxUsed() {
	var maxUsed uint64
	for _, r := range partition.Replicas {
		if r.Used > maxUsed {
			maxUsed = r.Used
		}
	}
	partition.used = maxUsed
}

func (partition *DataPartition) getMaxUsedSpace() uint64 {
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
	replica.ReportTime = time.Now().Unix()
	replica.Total = util.DefaultDataPartitionSize
	partition.addReplica(replica)
	partition.checkAndRemoveMissReplica(replica.Addr)
	return
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

func (partition *DataPartition) getMinus() (minus float64) {
	partition.RLock()
	defer partition.RUnlock()
	used := partition.Replicas[0].Used
	for _, replica := range partition.Replicas {
		if math.Abs(float64(replica.Used)-float64(used)) > minus {
			minus = math.Abs(float64(replica.Used) - float64(used))
		}
	}
	return minus
}

func (partition *DataPartition) getToBeDecommissionHost(replicaNum int) (host string) {
	partition.RLock()
	defer partition.RUnlock()
	hostLen := len(partition.Hosts)
	if hostLen <= 1 || hostLen <= replicaNum {
		return
	}
	host = partition.Hosts[hostLen-1]
	return
}

func (partition *DataPartition) removeOneReplicaByHost(c *Cluster, host string) (err error) {
	if err = c.removeDataReplica(partition, host, false); err != nil {
		return
	}

	partition.RLock()
	defer partition.RUnlock()

	oldReplicaNum := partition.ReplicaNum
	partition.ReplicaNum = partition.ReplicaNum - 1

	if err = c.syncUpdateDataPartition(partition); err != nil {
		partition.ReplicaNum = oldReplicaNum
	}

	return
}

func (partition *DataPartition) getLiveZones(offlineAddr string) (zones []string) {
	partition.RLock()
	defer partition.RUnlock()
	for _, replica := range partition.Replicas {
		if replica.dataNode == nil {
			continue
		}
		if replica.dataNode.Addr == offlineAddr {
			continue
		}
		zones = append(zones, replica.dataNode.ZoneName)
	}
	return
}

func (partition *DataPartition) buildDpInfo(c *Cluster) *proto.DataPartitionInfo {
	partition.RLock()
	defer partition.RUnlock()

	var replicas = make([]*proto.DataReplica, len(partition.Replicas))
	for i, replica := range partition.Replicas {
		replicas[i] = &replica.DataReplica
	}

	var fileInCoreMap = make(map[string]*proto.FileInCore)
	for k, v := range partition.FileInCoreMap {
		fileInCoreMap[k] = v.clone()
	}

	zones := make([]string, len(partition.Hosts))
	for idx, host := range partition.Hosts {
		dataNode, err := c.dataNode(host)
		if err == nil {
			zones[idx] = dataNode.ZoneName
		}
	}

	return &proto.DataPartitionInfo{
		PartitionID:             partition.PartitionID,
		PartitionTTL:            partition.PartitionTTL,
		PartitionType:           partition.PartitionType,
		LastLoadedTime:          partition.LastLoadedTime,
		ReplicaNum:              partition.ReplicaNum,
		Status:                  partition.Status,
		Replicas:                replicas,
		Hosts:                   partition.Hosts,
		Peers:                   partition.Peers,
		Zones:                   zones,
		MissingNodes:            partition.MissingNodes,
		VolName:                 partition.VolName,
		VolID:                   partition.VolID,
		FileInCoreMap:           fileInCoreMap,
		OfflinePeerID:           partition.OfflinePeerID,
		FilesWithMissingReplica: partition.FilesWithMissingReplica,
	}
}
