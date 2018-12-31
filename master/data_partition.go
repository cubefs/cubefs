// Copyright 2018 The CFS Authors.
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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"math"
	"strings"
	"sync"
	"time"
)

// DataPartition is the unit of storing the file contents and the replication is performed in terms of the partitions
type DataPartition struct {
	PartitionID      uint64
	LastLoadTime     int64
	ReplicaNum       uint8
	Status           int8
	isRecover        bool
	Replicas         []*DataReplica

	// TODO what are PersistenceHosts?  持久化的host列表   直接改为 hosts
	PersistenceHosts []string
	Peers            []proto.Peer
	sync.RWMutex
	total         uint64
	used          uint64
	MissNodes     map[string]int64
	VolName       string
	VolID         uint64
	modifyTime    int64
	createTime    int64
	RandomWrite   bool
	FileInCoreMap map[string]*FileInCore
}

func newDataPartition(ID uint64, replicaNum uint8, volName string, volID uint64, randomWrite bool) (partition *DataPartition) {
	partition = new(DataPartition)
	partition.ReplicaNum = replicaNum
	partition.PartitionID = ID
	partition.PersistenceHosts = make([]string, 0)
	partition.Peers = make([]proto.Peer, 0)
	partition.Replicas = make([]*DataReplica, 0)
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.MissNodes = make(map[string]int64)
	partition.Status = proto.ReadOnly
	partition.VolName = volName
	partition.VolID = volID
	partition.modifyTime = time.Now().Unix()
	partition.createTime = time.Now().Unix()
	partition.RandomWrite = randomWrite
	return
}

func (partition *DataPartition) addMember(replica *DataReplica) {
	for _, r := range partition.Replicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	partition.Replicas = append(partition.Replicas, replica)
}

func (partition *DataPartition) generateCreateTasks(dataPartitionSize uint64) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for _, addr := range partition.PersistenceHosts {
		tasks = append(tasks, partition.generateCreateTask(addr, dataPartitionSize))
	}
	return
}

func (partition *DataPartition) generateCreateTask(addr string, dataPartitionSize uint64) (task *proto.AdminTask) {

	task = proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateDataPartitionRequest(
		partition.VolName, partition.PartitionID, partition.RandomWrite, partition.Peers, int(dataPartitionSize)))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) generateDeleteTask(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpDeleteDataPartition, addr, newDeleteDataPartitionRequest(partition.PartitionID))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) generateOfflineTask(removePeer proto.Peer, addPeer proto.Peer) (task *proto.AdminTask, err error) {
	if !partition.RandomWrite {
		return partition.generateDeleteTask(removePeer.Addr), nil
	}
	leaderAddr := partition.getLeaderAddr()
	if leaderAddr == "" {
		err = errNoLeader
		return
	}
	task = proto.NewAdminTask(proto.OpOfflineDataPartition, leaderAddr, newOfflineDataPartitionRequest(partition.PartitionID, removePeer, addPeer))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) resetTaskID(t *proto.AdminTask) {
	t.ID = fmt.Sprintf("%v_DataPartitionID[%v]", t.ID, partition.PartitionID)
}

// TODO what does "hasMissOne" mean?
// 三个副本 已经有一个副本丢失
// hasMissingOneReplica
func (partition *DataPartition) hasMissOne(replicaNum int) (err error) {
	hostNum := len(partition.PersistenceHosts)
	if hostNum <= replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissOne", partition.PartitionID, errDataReplicaHasMissOne))
		err = errDataReplicaHasMissOne
	}
	return
}

// TODO liveReplicas and otherLiveReplicas can be put into the same loop
func (partition *DataPartition) canBeOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],partitionID:%v  RocksDBHost:%v  offLine:%v ",
		partition.PartitionID, partition.PersistenceHosts, offlineAddr)
	liveReplicas := partition.getLiveReplicas(defaultDataPartitionTimeOutSec)
	otherLiveReplicas := partition.removeOfflineAddr(liveReplicas, offlineAddr)
	if len(otherLiveReplicas) < int(partition.ReplicaNum/2) {
		msg = fmt.Sprintf(msg+" err:%v  liveReplicas:%v ", errCannotOffLine, len(liveReplicas))
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}

	return
}

// TODO rephrase the name
func (partition *DataPartition) removeOfflineAddr(liveReplicas []*DataReplica, offlineAddr string) (otherLiveReplicas []*DataReplica) {
	otherLiveReplicas = make([]*DataReplica, 0)
	for i := 0; i < len(liveReplicas); i++ {
		replica := partition.Replicas[i]
		if replica.Addr != offlineAddr {
			otherLiveReplicas = append(otherLiveReplicas, replica)
		}
	}
	return
}

func (partition *DataPartition) generatorOffLineLog(offlineAddr string) (msg string) {
	msg = fmt.Sprintf("action[generatorOffLineLog],data partition:%v  offlineaddr:%v  ",
		partition.PartitionID, offlineAddr)
	replicas := partition.getAvailableDataReplicas()
	for i := 0; i < len(replicas); i++ {
		replica := replicas[i]
		msg += fmt.Sprintf(" addr:%v  dataReplicaStatus:%v  FileCount :%v ", replica.Addr,
			replica.Status, replica.FileCount)
	}
	log.LogWarn(msg)

	return
}

// get all the valid replicas of the given data partition
func (partition *DataPartition) getAvailableDataReplicas() (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]

		// the node reports heartbeat normally and the node is available
		if replica.isLocationAvailable() == true && partition.isInPersistenceHosts(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

// TODO what does offLineInMem mean?
// 从 memory 中移除 指定地址的副本  removeReplicaByAddr()
func (partition *DataPartition) offLineInMem(addr string) {
	delIndex := -1
	var replica *DataReplica
	for i := 0; i < len(partition.Replicas); i++ {
		replica = partition.Replicas[i]
		if replica.Addr == addr {
			delIndex = i
			break
		}
	}

	msg := fmt.Sprintf("action[offLineInMem],data partition:%v  on Node:%v  OffLine,the node is in replicas:%v", partition.PartitionID, addr, replica != nil)
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
	for _, addr := range partition.PersistenceHosts {
		replica, err := partition.getReplica(addr)
		if err != nil || replica.isLive(defaultDataPartitionTimeOutSec) == false {
			continue
		}
		replica.LoadPartitionIsResponse = false
		tasks = append(tasks, partition.createLoadTask(addr))
	}
	partition.LastLoadTime = time.Now().Unix()
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
	return nil, errors.Annotatef(dataReplicaNotFound(addr), "%v not found", addr)
}

func (partition *DataPartition) convertToDataPartitionResponse() (dpr *DataPartitionResponse) {
	dpr = new(DataPartitionResponse)
	partition.Lock()
	defer partition.Unlock()
	dpr.PartitionID = partition.PartitionID
	dpr.Status = partition.Status
	dpr.ReplicaNum = partition.ReplicaNum
	dpr.Hosts = make([]string, len(partition.PersistenceHosts))
	copy(dpr.Hosts, partition.PersistenceHosts)
	dpr.RandomWrite = partition.RandomWrite
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
	for _, addr := range partition.PersistenceHosts {
		replica, err := partition.getReplica(addr)
		if err != nil {
			return
		}
		timePassed := time.Now().Unix() - partition.LastLoadTime
		if replica.LoadPartitionIsResponse == false && timePassed > loadDataPartitionWaitTime {
			msg := fmt.Sprintf("action[checkLoadResponse], partitionID:%v on Node:%v no response, spent time %v s",
				partition.PartitionID, addr, timePassed)
			log.LogWarn(msg)
			return
		}
		if replica.isLive(timeOutSec) == false || replica.LoadPartitionIsResponse == false {
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
	needDelFiles := make([]string, 0)
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		replica.FileCount = 0
	}
	for _, fc := range partition.FileInCoreMap {
		if len(fc.Metas) == 0 {
			needDelFiles = append(needDelFiles, fc.Name)
		}
		for _, vfNode := range fc.Metas {
			replica := partition.getReplicaByIndex(vfNode.locIndex)
			replica.FileCount++
		}

	}

	for _, vfName := range needDelFiles {
		delete(partition.FileInCoreMap, vfName)
	}

	for _, replica := range partition.Replicas {
		msg = fmt.Sprintf(getDataReplicaFileCountInfo+"partitionID:%v  replicaAddr:%v  FileCount:%v  "+
			"NodeIsActive:%v  replicaIsActive:%v  .replicaStatusOnNode:%v ", partition.PartitionID, replica.Addr, replica.FileCount,
			replica.getReplicaNode().isActive, replica.isActive(defaultDataPartitionTimeOutSec), replica.Status)
		log.LogInfo(msg)
	}

}

func (partition *DataPartition) releaseDataPartition() {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasByPersistenceHosts(defaultDataPartitionTimeOutSec)
	for _, replica := range liveReplicas {
		replica.LoadPartitionIsResponse = false
	}
	for name, fc := range partition.FileInCoreMap {
		fc.Metas = nil
		delete(partition.FileInCoreMap, name)
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)

}

// TODO why use loop here? why not just map? explain why we use loop instead of map
func (partition *DataPartition) isInReplicas(host string) (replica *DataReplica, ok bool) {
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
	if int(partition.ReplicaNum) != len(partition.PersistenceHosts) {
		msg := fmt.Sprintf("FIX DataPartition replicaNum,clusterID[%v] volName[%v] partitionID:%v orgReplicaNum:%v",
			c.Name, volName, partition.PartitionID, partition.ReplicaNum)
		Warn(c.Name, msg)
	}
}

func (partition *DataPartition) hostsToString() (hosts string) {
	return strings.Join(partition.PersistenceHosts, underlineSeparator)
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

func (partition *DataPartition) isInPersistenceHosts(addr string) (ok bool) {
	for _, host := range partition.PersistenceHosts {
		if host == addr {
			ok = true
			break
		}
	}
	return
}

func (partition *DataPartition) getLiveReplicas(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if replica.isLive(timeOutSec) == true && partition.isInPersistenceHosts(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

// get all the live replicas from the persistent hosts
func (partition *DataPartition) getLiveReplicasByPersistenceHosts(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, host := range partition.PersistenceHosts {
		replica, ok := partition.isInReplicas(host)
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
	if _, ok := partition.MissNodes[addr]; ok {
		delete(partition.MissNodes, addr)
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
	replica.LoadPartitionIsResponse = true
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
	return -1, errors.Annotatef(dataReplicaNotFound(addr), "%v not found ", addr)
}

func (partition *DataPartition) updateForOffline(offlineAddr, newAddr, volName string, newPeers []proto.Peer, c *Cluster) (err error) {
	orgHosts := make([]string, len(partition.PersistenceHosts))
	copy(orgHosts, partition.PersistenceHosts)
	oldPeers := make([]proto.Peer, len(partition.Peers))
	copy(oldPeers, partition.Peers)
	newHosts := make([]string, 0)
	for index, addr := range partition.PersistenceHosts {
		if addr == offlineAddr {
			after := partition.PersistenceHosts[index+1:]
			newHosts = partition.PersistenceHosts[:index]
			newHosts = append(newHosts, after...)
			break
		}
	}
	newHosts = append(newHosts, newAddr)
	partition.PersistenceHosts = newHosts
	partition.Peers = newPeers
	if err = c.syncUpdateDataPartition(partition); err != nil {
		partition.PersistenceHosts = orgHosts
		partition.Peers = oldPeers
		return errors.Annotatef(err, "update partition[%v] failed", partition.PartitionID)
	}
	msg := fmt.Sprintf("action[updateForOffline]  partitionID:%v offlineAddr:%v newAddr:%v"+
		"oldHosts:%v newHosts:%v,oldPees[%v],newPeers[%v]",
		partition.PartitionID, offlineAddr, newAddr, orgHosts, partition.PersistenceHosts, oldPeers, newPeers)
	log.LogInfo(msg)
	return
}

func (partition *DataPartition) updateMetric(vr *proto.PartitionReport, dataNode *DataNode) {

	if !partition.isInPersistenceHosts(dataNode.Addr) {
		return
	}
	partition.Lock()
	defer partition.Unlock()
	replica, err := partition.getReplica(dataNode.Addr)
	if err != nil {
		replica = newDataReplica(dataNode)
		partition.addMember(replica)
	}
	partition.total = vr.Total
	partition.used = vr.Used
	replica.Status = int8(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	replica.FileCount = uint32(vr.ExtentCount)
	replica.setAlive()
	replica.IsLeader = vr.IsLeader
	replica.NeedCompare = vr.NeedCompare
	partition.checkAndRemoveMissReplica(dataNode.Addr)
}

func (partition *DataPartition) toJSON() (body []byte, err error) {
	partition.RLock()
	defer partition.RUnlock()
	return json.Marshal(partition)
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

// TODO what does createDataPartitionSuccessTriggerOperator mean?
// the caller must add lock
// 创建data partition 成功后的操作
// postprocessingXXX
func (partition *DataPartition) createDataPartitionSuccessTriggerOperator(nodeAddr string, c *Cluster) (err error) {
	dataNode, err := c.getDataNode(nodeAddr)
	if err != nil {
		return err
	}
	replica := newDataReplica(dataNode)
	replica.Status = proto.ReadWrite
	partition.addMember(replica)
	partition.checkAndRemoveMissReplica(replica.Addr)
	return
}

// the caller add lock
func (partition *DataPartition) isReplicaSizeAligned() bool {
	if len(partition.Replicas) == 0 {
		return true
	}
	used := partition.Replicas[0].Used

	// TODO wha does minus mean?
	var minus float64  // TODO delta
	for _, replica := range partition.Replicas {

		// TODO we should use a variable to buffer the result of math.Abs(float64(replica.Used)-float64(used))
		if math.Abs(float64(replica.Used)-float64(used)) > minus {
			minus = math.Abs(float64(replica.Used) - float64(used))
		}
	}
	if minus < float64(util.GB) {
		return true
	}
	return false
}

// TODO data integrity check?
// 需要比较 有的datanode 的数据没有完全加载的话比较数据的话没有意义
// needsCompareCRC
func (partition *DataPartition) needsCompare() (needCompare bool) {
	partition.Lock()
	defer partition.Unlock()
	needCompare = true
	for _, replica := range partition.Replicas {
		if !replica.NeedCompare {
			needCompare = false
			break
		}
	}
	return
}
