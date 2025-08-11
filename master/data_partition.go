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
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/strutil"
)

// DataPartition represents the structure of storing the file contents.
type DataPartition struct {
	PartitionID      uint64
	PartitionType    int
	LastLoadedTime   int64
	ReplicaNum       uint8
	Status           int8
	isRecover        bool
	Replicas         []*DataReplica
	LeaderReportTime int64
	Hosts            []string // host addresses
	Peers            []proto.Peer
	offlineMutex     sync.RWMutex
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

	RdOnly                            bool
	addReplicaMutex                   sync.RWMutex
	DecommissionDiskRetryMap          map[string]int
	DecommissionRetry                 int
	DecommissionStatus                uint32
	DecommissionSrcAddr               string
	DecommissionDstAddr               string
	DecommissionRaftForce             bool
	DecommissionSrcDiskPath           string
	DecommissionTerm                  uint64
	DecommissionDstAddrSpecify        bool // if DecommissionDstAddrSpecify is true, donot rollback when add replica fail
	DecommissionNeedRollback          bool
	DecommissionNeedRollbackTimes     uint32
	DecommissionErrorMessage          string
	DecommissionFirstHostDiskTokenKey string
	DecommissionWeight                int
	SpecialReplicaDecommissionStop    chan bool // used for stop
	SpecialReplicaDecommissionStep    uint32
	IsDiscard                         bool
	VerSeq                            uint64
	RecoverStartTime                  time.Time
	RecoverUpdateTime                 time.Time
	RecoverLastConsumeTime            time.Duration
	DecommissionRetryTime             time.Time
	RepairBlockSize                   uint64
	DecommissionType                  uint32
	RestoreReplica                    uint32
	MediaType                         uint32
	ForbidWriteOpOfProtoVer0          bool
}

func newDataPartition(ID uint64, replicaNum uint8, volName string, volID uint64,
	partitionType int, mediaType uint32,
) (partition *DataPartition) {
	partition = new(DataPartition)
	partition.ReplicaNum = replicaNum
	partition.PartitionID = ID
	partition.Hosts = make([]string, 0)
	partition.Peers = make([]proto.Peer, 0)
	partition.Replicas = make([]*DataReplica, 0)
	partition.FileInCoreMap = make(map[string]*FileInCore)
	partition.FilesWithMissingReplica = make(map[string]int64)
	partition.MissingNodes = make(map[string]int64)
	partition.DecommissionDiskRetryMap = make(map[string]int)

	partition.Status = proto.ReadOnly
	partition.VolName = volName
	partition.VolID = volID
	partition.PartitionType = partitionType

	now := time.Now().Unix()
	partition.modifyTime = now
	partition.createTime = now
	partition.lastWarnTime = now
	partition.SpecialReplicaDecommissionStop = make(chan bool, 1024)
	partition.DecommissionStatus = DecommissionInitial
	partition.SpecialReplicaDecommissionStep = SpecialDecommissionInitial
	partition.DecommissionDstAddrSpecify = false
	partition.LeaderReportTime = now
	partition.RepairBlockSize = util.DefaultDataPartitionSize
	partition.RestoreReplica = RestoreReplicaMetaStop
	partition.MediaType = mediaType
	return
}

func (partition *DataPartition) setReadWrite() {
	partition.Status = proto.ReadWrite
	for _, replica := range partition.Replicas {
		replica.ReadOnlyReasons = 0
		replica.Status = proto.ReadWrite
	}
}

func (partition *DataPartition) allUnavailable() bool {
	for _, r := range partition.Replicas {
		if !r.isUnavailable() && r.isActive(defaultDataPartitionTimeOutSec) {
			return false
		}
	}
	return true
}

func (partition *DataPartition) isSpecialReplicaCnt() bool {
	return partition.ReplicaNum == 1 || partition.ReplicaNum == 2
}

func (partition *DataPartition) resetFilesWithMissingReplica() {
	partition.Lock()
	defer partition.Unlock()
	partition.FilesWithMissingReplica = make(map[string]int64)
}

func (partition *DataPartition) addReplica(replica *DataReplica) {
	for _, r := range partition.Replicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	partition.Replicas = append(partition.Replicas, replica)
}

func (partition *DataPartition) tryToChangeLeaderByHost(host string) (err error) {
	var dataNode *DataNode
	for _, r := range partition.Replicas {
		if host == r.Addr {
			dataNode = r.dataNode
			break
		}
	}
	if dataNode == nil {
		return fmt.Errorf("host not found[%v]", host)
	}
	task, err := partition.createTaskToTryToChangeLeader(host)
	if err != nil {
		return
	}
	if _, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return
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

func (partition *DataPartition) createTaskToSetRepairingStatus(addr string, repairingStatus bool) (task *proto.AdminTask, err error) {
	task = proto.NewAdminTask(proto.OpSetRepairingStatus, addr, newSetRepairingStatusRequest(partition.PartitionID, repairingStatus))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToAddRaftMember(addPeer proto.Peer, leaderAddr string, repairingStatus bool) (task *proto.AdminTask, err error) {
	task = proto.NewAdminTask(proto.OpAddDataPartitionRaftMember, leaderAddr, newAddDataPartitionRaftMemberRequest(partition.PartitionID, addPeer, repairingStatus))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToRemoveRaftMember(c *Cluster, removePeer proto.Peer, repairingStatus bool, force bool, autoRemove bool) (err error) {
	doWork := func(leaderAddr string, flag bool) error {
		log.LogInfof("action[createTaskToRemoveRaftMember] vol[%v],data partition[%v] removePeer %v leaderAddr %v autoRemove %v",
			partition.VolName, partition.PartitionID, removePeer, leaderAddr, flag)
		req := newRemoveDataPartitionRaftMemberRequest(partition.PartitionID, removePeer, repairingStatus)
		req.Force = force
		req.AutoRemove = autoRemove

		task := proto.NewAdminTask(proto.OpRemoveDataPartitionRaftMember, leaderAddr, req)
		partition.resetTaskID(task)

		leaderDataNode, err := c.dataNode(leaderAddr)
		if err != nil {
			log.LogErrorf("action[createTaskToRemoveRaftMember] vol[%v],data partition[%v],err[%v]", partition.VolName, partition.PartitionID, err)
			return err
		}
		if _, err = leaderDataNode.TaskManager.syncSendAdminTask(task); err != nil {
			log.LogErrorf("action[createTaskToRemoveRaftMember] vol[%v],data partition[%v],err[%v]", partition.VolName, partition.PartitionID, err)
			return err
		}
		log.LogWarnf("action[createTaskToRemoveRaftMember] remove peer[%v] finished, vol[%v],data partition[%v],leader addr[%v]", removePeer, partition.VolName, partition.PartitionID, leaderAddr)
		return nil
	}

	leaderAddr := partition.getLeaderAddr()
	log.LogInfof("action[createTaskToRemoveRaftMember] vol[%v],data partition[%v] removePeer %v leaderAddr %v autoRemove %v",
		partition.VolName, partition.PartitionID, removePeer, leaderAddr, autoRemove)
	if leaderAddr == "" {
		if force {
			for _, replica := range partition.Replicas {
				if replica.Addr == removePeer.Addr {
					continue
				}
				leaderAddr = replica.Addr
				err = doWork(leaderAddr, autoRemove)
				if err != nil {
					continue
				}
				return nil
			}

			return fmt.Errorf("createTaskToRemoveRaftMember: force delte raft memeber failed, dp %d, err %v", partition.PartitionID, err)
		} else {
			err = proto.ErrNoLeader
			return
		}
	} else {
		return doWork(leaderAddr, autoRemove)
	}
}

func (partition *DataPartition) createTaskToCreateDataPartition(addr string, dataPartitionSize uint64,
	peers []proto.Peer, hosts []string, createType int, partitionType int, decommissionedDisks []string) (task *proto.AdminTask,
) {
	leaderSize := 0
	if createType == proto.DecommissionedCreateDataPartition {
		if len(partition.Replicas) == 0 {
			log.LogInfof("action[createTaskToCreateDataPartition] cannot create replica for dp %v with empty replicas",
				partition.decommissionInfo())
			return
		}
		partition.RLock()
		leaderSize = int(partition.Replicas[0].Used)
		partition.RUnlock()
	}

	task = proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateDataPartitionRequest(
		partition.VolName, partition.PartitionID, int(partition.ReplicaNum),
		peers, int(dataPartitionSize), leaderSize, hosts, createType,
		partitionType, decommissionedDisks, partition.VerSeq))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) createTaskToDeleteDataPartition(addr string, raftForceDel bool) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpDeleteDataPartition, addr,
		newDeleteDataPartitionRequest(partition.PartitionID, partition.DecommissionType, raftForceDel))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) resetTaskID(t *proto.AdminTask) {
	t.ID = fmt.Sprintf("%v_DataPartitionID[%v]", t.ID, partition.PartitionID)
	t.PartitionID = partition.PartitionID
}

func (partition *DataPartition) isLastReplicas(host string) error {
	if len(partition.Replicas) == 1 && partition.Replicas[0].Addr == host {
		return fmt.Errorf("partition %v has only one replica %v", partition.PartitionID, host)
	}
	if len(partition.Hosts) == 1 && partition.Hosts[0] == host {
		return fmt.Errorf("partition %v has only one host %v", partition.PartitionID, host)
	}

	normalHosts := make([]string, 0, len(partition.Replicas))
	for _, r := range partition.Replicas {
		if r.isNormal(partition.PartitionID, defaultDataPartitionTimeOutSec) {
			normalHosts = append(normalHosts, r.Addr)
		}
	}

	if len(normalHosts) == 1 && normalHosts[0] == host {
		return fmt.Errorf("partition %v only has one normal host, %v", partition.PartitionID, host)
	}

	return nil
}

// Check if there is a replica missing or not.
func (partition *DataPartition) hasMissingOneReplica(addr string, replicaNum int) (err error) {
	hostNum := len(partition.Replicas)

	inReplicas := false
	for _, rep := range partition.Replicas {
		if addr == rep.Addr {
			inReplicas = true
		}
	}

	if hostNum <= replicaNum-1 && inReplicas {
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

	if partition.ReplicaNum >= 3 && len(otherLiveReplicas) < int(partition.ReplicaNum/2+1) {
		var lives []string
		for _, replica := range otherLiveReplicas {
			lives = append(lives, replica.Addr)
		}
		msg = fmt.Sprintf(msg+" err:%v  liveReplicas len:%v [%v] not satisify qurom %d ",
			proto.ErrCannotBeOffLine, len(otherLiveReplicas), lives, int(partition.ReplicaNum/2+1))
		log.LogError(msg)
		err = fmt.Errorf(msg)
		return
	}

	if len(liveReplicas) == 0 {
		msg = fmt.Sprintf(msg+" err:%v  replicaNum:%v liveReplicas is 0 ", proto.ErrCannotBeOffLine, partition.ReplicaNum)
		log.LogError(msg)
		err = fmt.Errorf(msg)
		return
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

	msg := fmt.Sprintf("action[removeReplicaByAddr],data partition:%v  on node:%v  OffLine,the node is in replicas:%v", partition.PartitionID, addr, replica != nil)
	log.LogDebug(msg)
	if delIndex == -1 {
		return
	}
	partition.FileInCoreMap = make(map[string]*FileInCore)
	partition.deleteReplicaByIndex(delIndex)
	partition.modifyTime = time.Now().Unix()
}

func (partition *DataPartition) deleteReplicaByIndex(index int) {
	var replicaAddrs []string
	for _, replica := range partition.Replicas {
		replicaAddrs = append(replicaAddrs, replica.Addr)
	}
	msg := fmt.Sprintf("deleteReplicaByIndex dp %v  index:%v  locations :%v ", partition.PartitionID, index, replicaAddrs)
	log.LogInfo(msg)
	replicasAfter := partition.Replicas[index+1:]
	tmp := partition.Replicas[:index]
	tmp = append(tmp, replicasAfter...)
	partition.Replicas = tmp
}

func (partition *DataPartition) createLoadTasks() (tasks []*proto.AdminTask) {
	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.Hosts {
		replica, err := partition.getReplica(addr)
		if err != nil || !replica.isLive(partition.PartitionID, defaultDataPartitionTimeOutSec) {
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
	if !partition.IsDiscard {
		log.LogWarnf("action[getReplica],partitionID:%v,locations:%v,err:%v",
			partition.PartitionID, addr, dataReplicaNotFound(addr))
	}
	return nil, errors.Trace(dataReplicaNotFound(addr), "%v not found", addr)
}

func (partition *DataPartition) convertToDataPartitionResponse() (dpr *proto.DataPartitionResponse) {
	dpr = new(proto.DataPartitionResponse)
	partition.Lock()
	defer partition.Unlock()

	dpr.PartitionID = partition.PartitionID
	dpr.PartitionType = partition.PartitionType
	dpr.Status = partition.Status
	dpr.ReplicaNum = partition.ReplicaNum
	dpr.Hosts = make([]string, len(partition.Hosts))
	copy(dpr.Hosts, partition.Hosts)
	dpr.LeaderAddr = partition.getLeaderAddr()
	dpr.IsRecover = partition.isRecover
	dpr.IsDiscard = partition.IsDiscard
	dpr.MediaType = partition.MediaType
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
			log.LogInfof("action[checkLoadResponse] partitionID:%v getReplica addr %v error %v", partition.PartitionID, addr, err)
			return
		}
		timePassed := time.Now().Unix() - partition.LastLoadedTime
		if !replica.HasLoadResponse && timePassed > timeToWaitForResponse {
			msg := fmt.Sprintf("action[checkLoadResponse], partitionID:%v on node:%v no response, spent time %v s",
				partition.PartitionID, addr, timePassed)
			log.LogWarn(msg)
			return
		}
		if !replica.isLive(partition.PartitionID, timeOutSec) || !replica.HasLoadResponse {
			log.LogInfof("action[checkLoadResponse] partitionID:%v getReplica addr %v replica.isLive(timeOutSec) %v",
				partition.PartitionID, addr, replica.isLive(partition.PartitionID, timeOutSec))
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
	partition.FileInCoreMap = make(map[string]*FileInCore)
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
		if partition.isSpecialReplicaCnt() {
			// only reduce replica num when rollback failed or no decommission happen
			if partition.IsRollbackFailed() || partition.IsDecommissionInitial() {
				log.LogInfof("action[checkReplicaNum] volume %v partition %v need to lower replica", partition.VolName, partition.PartitionID)
				vol.NeedToLowerReplica = true
				return
			}
			return
		} else {
			// add replica success but del replica failed
			log.LogInfof("action[checkReplicaNum] volume %v partition %v replica num abnormal %v [%v]",
				partition.VolName, partition.PartitionID, partition.ReplicaNum, partition.Hosts)
			vol.NeedToLowerReplica = true
		}
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
		if replica.isLive(partition.PartitionID, timeOutSec) && partition.hasHost(replica.Addr) {
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
		if replica.isLive(partition.PartitionID, timeOutSec) {
			replicas = append(replicas, replica)
		} else {
			// msg := fmt.Sprintf("dp %v replica addr %v is unavailable, datanode active %v replica status %v and is active %v",
			//	partition.PartitionID, replica.Addr, replica.dataNode.isActive, replica.Status, replica.isActive(timeOutSec))
			replica.Status = proto.Unavailable
			log.LogDebugf("action[getLiveReplicasFromHosts] vol %v dp %v replica %v is unavailable",
				partition.VolName, partition.PartitionID, replica.Addr)
			// auditlog.LogMasterOp("DataPartitionReplicaStatus", msg, nil)
		}
	}

	return
}

// get all the live replicas from the persistent hosts
func (partition *DataPartition) getLiveReplicas(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, replica := range partition.Replicas {
		if replica.isLive(partition.PartitionID, timeOutSec) {
			replicas = append(replicas, replica)
		} else {
			replica.Status = proto.Unavailable
			log.LogWarnf("action[getLiveReplicas] vol %v dp %v replica %v is unavailable",
				partition.VolName, partition.PartitionID, replica.Addr)
		}
	}

	return
}

func (partition *DataPartition) checkAndRemoveMissReplica(addr string) {
	delete(partition.MissingNodes, addr)
}

func (partition *DataPartition) loadFile(dataNode *DataNode, resp *proto.LoadDataPartitionResponse) {
	partition.Lock()
	defer partition.Unlock()

	index, err := partition.getReplicaIndex(dataNode.Addr)
	if err != nil {
		msg := fmt.Sprintf("loadFile partitionID:%v  on node:%v  don't report :%v ", partition.PartitionID, dataNode.Addr, err)
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
		log.LogInfof("updateFileInCore partition %v", partition.PartitionID)
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
	if len(newHosts) == 0 {
		log.LogErrorf("update. action[%v] update partition[%v] vol[%v] old host[%v]", action, partition.PartitionID, volName, partition.Hosts)
		return
	}
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
	log.LogWarnf(msg)
	return
}

func (partition *DataPartition) updateMetric(vr *proto.DataPartitionReport, dataNode *DataNode, c *Cluster) {
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
	replica.ForbidWriteOpOfProtoVer0 = vr.ForbidWriteOpOfProtoVer0
	replica.ReadOnlyReasons = vr.ReadOnlyReasons
	replica.IsMissingTinyExtent = vr.IsMissingTinyExtent
	replica.IsRepairing = vr.IsRepairing
	partition.setForbidWriteOpOfProtoVer0()
	if replica.IsLeader {
		partition.LeaderReportTime = time.Now().Unix()
	}
	replica.NeedsToCompare = vr.NeedCompare
	// if repair progress is forward,update RecoverUpdateTime
	if vr.DecommissionRepairProgress > replica.DecommissionRepairProgress {
		partition.RecoverUpdateTime = time.Now()
	}
	replica.DecommissionRepairProgress = vr.DecommissionRepairProgress
	replica.LocalPeers = vr.LocalPeers
	replica.TriggerDiskError = vr.TriggerDiskError
	if replica.DiskPath != vr.DiskPath && vr.DiskPath != "" {
		oldDiskPath := replica.DiskPath
		replica.DiskPath = vr.DiskPath
		err = c.syncUpdateDataPartition(partition)
		if err != nil {
			replica.DiskPath = oldDiskPath
		}
	}

	if c.RaftPartitionCanUsingDifferentPortEnabled() {
		// update old partition peers, add raft ports
		localPeers := make(map[string]proto.Peer)
		for _, peer := range vr.LocalPeers {
			if len(peer.ReplicaPort) == 0 || len(peer.HeartbeatPort) == 0 {
				peer.ReplicaPort = dataNode.ReplicaPort
				peer.HeartbeatPort = dataNode.HeartbeatPort
			}
			localPeers[peer.Addr] = peer
		}

		needUpdate := false
		for i, peer := range partition.Peers {
			if len(peer.ReplicaPort) == 0 || len(peer.HeartbeatPort) == 0 {
				if localPeer, exist := localPeers[peer.Addr]; exist {
					partition.Peers[i].ReplicaPort = localPeer.ReplicaPort
					partition.Peers[i].HeartbeatPort = localPeer.HeartbeatPort
					needUpdate = true
				}
			}
		}
		if needUpdate {
			c.syncUpdateDataPartition(partition)
		}
	}

	partition.checkAndRemoveMissReplica(dataNode.Addr)

	if replica.dataNode.RdOnly {
		replica.ReadOnlyReasons |= proto.DataNodeRdOnly
		if replica.Status == proto.ReadWrite {
			replica.Status = proto.ReadOnly
		}
	}

	if partition.RdOnly {
		replica.ReadOnlyReasons |= proto.PartitionRdOnly
		if replica.Status == proto.ReadWrite {
			replica.Status = proto.ReadOnly
		}
	}
}

func (partition *DataPartition) setMaxUsed() {
	var maxUsed uint64
	for _, r := range partition.Replicas {
		if r.Used > maxUsed {
			maxUsed = r.Used
		}
	}
	partition.used = maxUsed
	if log.EnableDebug() {
		log.LogDebugf("[setMaxUsed] vol(%v) dp(%v) set max used size(%v)", partition.VolName, partition.PartitionID, strutil.FormatSize(maxUsed))
	}
}

func (partition *DataPartition) setForbidWriteOpOfProtoVer0() {
	for _, r := range partition.Replicas {
		if !r.isActive(defaultDataPartitionTimeOutSec) {
			continue
		}
		if !r.ForbidWriteOpOfProtoVer0 {
			partition.ForbidWriteOpOfProtoVer0 = false
			return
		}
	}
	partition.ForbidWriteOpOfProtoVer0 = true
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
	// Special replica dp need to check live replica num immediately.
	// At this time, heartbeat messages may not have been received
	if partition.IsDecommissionRunning() || partition.isSpecialReplicaCnt() {
		replica.Status = proto.Recovering
	} else {
		replica.Status = proto.Unavailable
	}
	replica.DiskPath = diskPath
	replica.ReportTime = time.Now().Unix()
	replica.Total = util.DefaultDataPartitionSize
	partition.addReplica(replica)
	partition.checkAndRemoveMissReplica(replica.Addr)
	log.LogInfof("action[afterCreation] dp %v add new replica %v ", partition.PartitionID, dataNode.Addr)
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

func (partition *DataPartition) getReplicaDisk(nodeAddr string) string {
	partition.RLock()
	defer partition.RUnlock()
	for _, replica := range partition.Replicas {
		if nodeAddr == replica.Addr {
			return replica.DiskPath
		}
	}
	return ""
}

func (partition *DataPartition) activeUsedSimilar() bool {
	liveReplicas := partition.liveReplicas(defaultDataPartitionTimeOutSec)
	used := liveReplicas[0].Used
	minus := float64(0)

	for _, replica := range liveReplicas {
		if math.Abs(float64(replica.Used)-float64(used)) > minus {
			minus = math.Abs(float64(replica.Used) - float64(used))
		}
	}

	return minus < util.GB
}

// func (partition *DataPartition) getNodeSets() (nodeSets []uint64) {
// 	partition.RLock()
// 	defer partition.RUnlock()
// 	nodeSetMap := map[uint64]struct{}{}
// 	for _, replica := range partition.Replicas {
// 		if replica.dataNode == nil {
// 			continue
// 		}
// 		nodeSetMap[replica.dataNode.NodeSetID] = struct{}{}
// 	}
// 	for nodeSet := range nodeSetMap {
// 		nodeSets = append(nodeSets, nodeSet)
// 	}
// 	return
// }

// nolint: staticcheck
// func (partition *DataPartition) getZones() (zones []string) {
// 	partition.RLock()
// 	defer partition.RUnlock()
// 	zoneMap := map[string]struct{}{}
// 	for _, replica := range partition.Replicas {
// 		if replica.dataNode == nil {
// 			continue
// 		}
// 		zoneMap[replica.dataNode.ZoneName] = struct{}{}
// 	}
// 	for zone := range zoneMap {
// 		zones = append(zones, zone)
// 	}
// 	return
// }

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

	replicas := make([]*proto.DataReplica, len(partition.Replicas))
	for i, replica := range partition.Replicas {
		dataReplica := replica.DataReplica
		dataReplica.DomainAddr = replica.dataNode.DomainAddr
		replicas[i] = &dataReplica
	}

	fileInCoreMap := make(map[string]*proto.FileInCore)
	for k, v := range partition.FileInCoreMap {
		fileInCoreMap[k] = v.clone()
	}

	filesMissReplicas := make(map[string]int64, len(partition.FilesWithMissingReplica))
	for k, v := range partition.FilesWithMissingReplica {
		filesMissReplicas[k] = v
	}

	missNodes := map[string]int64{}
	for k, v := range partition.MissingNodes {
		missNodes[k] = v
	}

	zones := make([]string, len(partition.Hosts))
	nodeSets := make([]uint64, len(partition.Hosts))
	for idx, host := range partition.Hosts {
		dataNode, err := c.dataNode(host)
		if err == nil {
			zones[idx] = dataNode.ZoneName
			nodeSets[idx] = dataNode.NodeSetID
		}
	}

	forbidden := true
	vol, err := c.getVol(partition.VolName)
	if err == nil {
		forbidden = vol.Forbidden
	} else {
		log.LogErrorf("action[buildDpInfo]failed to get volume %v, err %v", partition.VolName, err)
	}

	return &proto.DataPartitionInfo{
		PartitionID:              partition.PartitionID,
		PartitionType:            partition.PartitionType,
		LastLoadedTime:           partition.LastLoadedTime,
		ReplicaNum:               partition.ReplicaNum,
		Status:                   partition.Status,
		Replicas:                 replicas,
		Hosts:                    partition.Hosts,
		Peers:                    partition.Peers,
		Zones:                    zones,
		NodeSets:                 nodeSets,
		MissingNodes:             missNodes,
		VolName:                  partition.VolName,
		VolID:                    partition.VolID,
		FileInCoreMap:            fileInCoreMap,
		OfflinePeerID:            partition.OfflinePeerID,
		IsRecover:                partition.isRecover,
		FilesWithMissingReplica:  filesMissReplicas,
		IsDiscard:                partition.IsDiscard,
		SingleDecommissionStatus: partition.GetSpecialReplicaDecommissionStep(),
		Forbidden:                forbidden,
		MediaType:                partition.MediaType,
		ForbidWriteOpOfProtoVer0: partition.ForbidWriteOpOfProtoVer0,
	}
}

const (
	DecommissionInitial uint32 = iota
	markDecommission
	DecommissionPause // can only stop markDecommission
	DecommissionPrepare
	DecommissionRunning
	DecommissionSuccess
	DecommissionFail
	DecommissionCancel
)

const (
	RestoreReplicaMetaStop uint32 = iota
	RestoreReplicaMetaRunning
	RestoreReplicaMetaForbidden
)

const (
	SpecialDecommissionInitial uint32 = iota
	SpecialDecommissionEnter
	SpecialDecommissionWaitAddRes
	SpecialDecommissionWaitAddResFin
	SpecialDecommissionRemoveOld
)

const InvalidDecommissionDpCnt = -1

const (
	defaultDecommissionParallelLimit              = 10
	defaultDecommissionRetryLimit                 = 5
	defaultDecommissionRetryInternal              = 10 * time.Minute
	defaultDecommissionRollbackLimit              = 3
	defaultSetRestoreReplicaStatusLimit           = 300
	defaultDecommissionFirstHostDiskParallelLimit = 10
	defaultDecommissionFirstHostParallelLimit     = 0
)

func GetDecommissionStatusMessage(status uint32) string {
	switch status {
	case DecommissionInitial:
		return "Initial"
	case markDecommission:
		return "Marked"
	case DecommissionPause:
		return "Paused"
	case DecommissionRunning:
		return "Running"
	case DecommissionSuccess:
		return "Success"
	case DecommissionFail:
		return "Failed"
	case DecommissionPrepare:
		return "DecommissionPrepare"
	case DecommissionCancel:
		return "DecommissionCancel"
	default:
		return fmt.Sprintf("Unkown:%v", status)
	}
}

func GetDecommissionTypeMessage(status uint32) string {
	switch status {
	case AutoDecommission:
		return "AutoDecommission"
	case ManualDecommission:
		return "ManualDecommission"
	case QueryDecommission:
		return "QueryDecommission"
	case AutoAddReplica:
		return "AutoAddReplica"
	case InitialDecommission:
		return "InitialDecommission"
	case ManualAddReplica:
		return "ManualAddReplica"
	default:
		return fmt.Sprintf("Unkown:%v", status)
	}
}

func GetRestoreReplicaMessage(status uint32) string {
	switch status {
	case RestoreReplicaMetaStop:
		return "RestoreReplicaMetaStop"
	case RestoreReplicaMetaRunning:
		return "RestoreReplicaMetaRunning"
	case RestoreReplicaMetaForbidden:
		return "RestoreReplicaMetaForbidden"
	default:
		return fmt.Sprintf("Unkown:%v", status)
	}
}

func GetSpecialDecommissionStatusMessage(status uint32) string {
	switch status {
	case SpecialDecommissionInitial:
		return "SpecialDecommissionInitial"
	case SpecialDecommissionEnter:
		return "SpecialDecommissionEnter"
	case SpecialDecommissionWaitAddRes:
		return "SpecialDecommissionWaitAddRes"
	case SpecialDecommissionWaitAddResFin:
		return "SpecialDecommissionWaitAddResFin"
	case SpecialDecommissionRemoveOld:
		return "SpecialDecommissionRemoveOld"
	default:
		return fmt.Sprintf("Unkown:%v", status)
	}
}

func (partition *DataPartition) ReleaseDecommissionFirstHostToken(c *Cluster) {
	key := partition.DecommissionFirstHostDiskTokenKey
	defer func() {
		log.LogInfof("action[ReleaseDecommissionFirstHostToken] dp(%v) release first host token(%v) success", partition.PartitionID, partition.DecommissionFirstHostDiskTokenKey)
		partition.DecommissionFirstHostDiskTokenKey = ""
	}()
	keySlice := strings.Split(key, "_")
	if len(keySlice) != 2 {
		return
	}
	addr := keySlice[0]
	diskPath := keySlice[1]
	value, ok := c.DataNodeToDecommissionRepairDpMap.Load(addr)
	if !ok {
		return
	}
	dataNodeToRepairDpInfo := value.(*DataNodeToDecommissionRepairDpInfo)
	dataNodeToRepairDpInfo.mu.Lock()
	defer dataNodeToRepairDpInfo.mu.Unlock()
	diskToRepairDpInfo, found := dataNodeToRepairDpInfo.DiskToDecommissionRepairDpMap[diskPath]
	if !found {
		return
	}

	if _, isExist := diskToRepairDpInfo.RepairingDps[partition.PartitionID]; !isExist {
		return
	}
	delete(diskToRepairDpInfo.RepairingDps, partition.PartitionID)
	if len(diskToRepairDpInfo.RepairingDps) == 0 {
		delete(dataNodeToRepairDpInfo.DiskToDecommissionRepairDpMap, diskPath)
	} else {
		atomic.StoreUint64(&diskToRepairDpInfo.CurParallel, uint64(len(diskToRepairDpInfo.RepairingDps)))
		dataNodeToRepairDpInfo.DiskToDecommissionRepairDpMap[diskPath] = diskToRepairDpInfo
	}

	dataNodeParallel := uint64(0)
	for _, diskInfo := range dataNodeToRepairDpInfo.DiskToDecommissionRepairDpMap {
		dataNodeParallel += atomic.LoadUint64(&diskInfo.CurParallel)
	}
	atomic.StoreUint64(&dataNodeToRepairDpInfo.CurParallel, dataNodeParallel)
}

func (partition *DataPartition) AcquireDecommissionFirstHostToken(c *Cluster) bool {
	var (
		ok                     bool
		found                  bool
		err                    error
		firstHost              string
		key                    string
		value                  interface{}
		firstReplica           *DataReplica
		dataNode               *DataNode
		dataNodeToRepairDpInfo *DataNodeToDecommissionRepairDpInfo
		diskToRepairDpInfo     *DiskToDecommissionRepairDpInfo
		dataNodeParallel       uint64
	)

	for _, host := range partition.Hosts {
		// for AutoAddReplica , firstHost does not need to consider the decommission source address since only adding and not deleting replica
		// for raftForce specialReplica dp, firstHost does not need to consider the decommission source address since adding before deleting replica
		// for three replicas dp or non-raftForce specialReplica dp, need to find the first host other than the decommission source address since deleting before adding replica
		if partition.DecommissionType == AutoAddReplica || (partition.isSpecialReplicaCnt() && !partition.DecommissionRaftForce) ||
			((partition.ReplicaNum == 3 || partition.isSpecialReplicaCnt() && partition.DecommissionRaftForce) && host != partition.DecommissionSrcAddr) {
			firstReplica, ok = partition.hasReplica(host)
			firstHost = host
			break
		}
	}
	if !ok {
		err = fmt.Errorf("dp(%v) can not find first host(%v) replica", partition.PartitionID, firstHost)
		log.LogWarnf("action[AcquireDecommissionFirstHostToken] failed, err(%v)", err.Error())
		goto errHandle
	}

	value, _ = c.DataNodeToDecommissionRepairDpMap.LoadOrStore(firstReplica.Addr, &DataNodeToDecommissionRepairDpInfo{
		mu:                            sync.Mutex{},
		CurParallel:                   0,
		Addr:                          firstReplica.Addr,
		DiskToDecommissionRepairDpMap: make(map[string]*DiskToDecommissionRepairDpInfo),
	})
	dataNodeToRepairDpInfo = value.(*DataNodeToDecommissionRepairDpInfo)
	dataNode, err = c.dataNode(firstReplica.Addr)
	if err != nil {
		log.LogErrorf("action[AcquireDecommissionFirstHostToken] failed, dp(%v) err(%v)", partition.PartitionID, err.Error())
		goto errHandle
	}
	if atomic.LoadUint64(&dataNode.DecommissionFirstHostParallelLimit) != 0 &&
		atomic.LoadUint64(&dataNodeToRepairDpInfo.CurParallel) >= atomic.LoadUint64(&dataNode.DecommissionFirstHostParallelLimit) {
		log.LogInfof("action[AcquireDecommissionFirstHostToken] dp(%v) acquire failed, datanode(%v) decommissionFirstHostParallelLimit has been reached", partition.PartitionID, dataNode.Addr)
		return false
	}
	dataNodeToRepairDpInfo.mu.Lock()
	defer dataNodeToRepairDpInfo.mu.Unlock()
	diskToRepairDpInfo, found = dataNodeToRepairDpInfo.DiskToDecommissionRepairDpMap[firstReplica.DiskPath]
	if !found {
		diskToRepairDpInfo = &DiskToDecommissionRepairDpInfo{
			CurParallel:  0,
			DiskPath:     firstReplica.DiskPath,
			RepairingDps: make(map[uint64]struct{}),
		}
	}

	if atomic.LoadUint64(&c.DecommissionFirstHostDiskParallelLimit) != 0 &&
		atomic.LoadUint64(&diskToRepairDpInfo.CurParallel) >= atomic.LoadUint64(&c.DecommissionFirstHostDiskParallelLimit) {
		log.LogInfof("action[AcquireDecommissionFirstHostToken] dp(%v) acquire failed, decommissionFirstHostDiskParallelLimit has been reached", partition.PartitionID)
		return false
	}

	diskToRepairDpInfo.RepairingDps[partition.PartitionID] = struct{}{}
	atomic.StoreUint64(&diskToRepairDpInfo.CurParallel, uint64(len(diskToRepairDpInfo.RepairingDps)))
	dataNodeToRepairDpInfo.DiskToDecommissionRepairDpMap[firstReplica.DiskPath] = diskToRepairDpInfo
	for _, diskInfo := range dataNodeToRepairDpInfo.DiskToDecommissionRepairDpMap {
		dataNodeParallel += atomic.LoadUint64(&diskInfo.CurParallel)
	}
	atomic.StoreUint64(&dataNodeToRepairDpInfo.CurParallel, dataNodeParallel)
	c.DataNodeToDecommissionRepairDpMap.Store(firstReplica.Addr, dataNodeToRepairDpInfo)
	key = fmt.Sprintf("%v_%v", firstReplica.Addr, firstReplica.DiskPath)
	partition.DecommissionFirstHostDiskTokenKey = key
	log.LogInfof("action[AcquireDecommissionFirstHostToken] dp(%v) acquire first host token(%v) success", partition.PartitionID, partition.DecommissionFirstHostDiskTokenKey)
	return true
errHandle:
	partition.markRollbackFailed(false)
	partition.DecommissionErrorMessage = err.Error()
	log.LogWarnf("action[AcquireDecommissionFirstHostToken] clusterID[%v] vol[%v] partitionID[%v]"+
		" retry [%v] status [%v] DecommissionDstAddrSpecify [%v] DecommissionDstAddr [%v] failed",
		c.Name, partition.VolName, partition.PartitionID, partition.DecommissionRetry, partition.GetDecommissionStatus(),
		partition.DecommissionDstAddrSpecify, partition.DecommissionDstAddr)
	return false
}

func isReplicasContainsHost(replicas []*DataReplica, host string) bool {
	for _, replica := range replicas {
		if replica.Addr == host {
			return true
		}
	}
	return false
}

func (partition *DataPartition) MarkDecommissionStatus(srcAddr, dstAddr, srcDisk string, raftForce bool, term uint64,
	migrateType uint32, weight int, c *Cluster, ns *nodeSet,
) (err error) {
	defer func() {
		if err != nil {
			msg := fmt.Sprintf("dp(%v) mark decommission status failed", partition.decommissionInfo())
			auditlog.LogMasterOp("DataPartitionDecommission", msg, err)
		}
	}()

	if dstAddr != "" {
		if err = c.checkDataNodeAddrMediaTypeForMigrate(srcAddr, dstAddr); err != nil {
			log.LogErrorf("[MarkDecommissionStatus] check mediaType err: %v", err.Error())
			return
		}
	}

	var status uint32
	// if mark discard, decommission it directly to delete replica
	if partition.IsDiscard {
		goto directly
	}
	if partition.isPerformingDecommission(c) {
		log.LogWarnf("action[MarkDecommissionStatus] dp(%v) is performing decommission",
			partition.PartitionID)
		return proto.ErrPerformingDecommission
	}
	// set DecommissionType first for recovering replica meta
	partition.DecommissionType = migrateType
	if err = partition.tryRecoverReplicaMeta(c, migrateType); err != nil {
		log.LogWarnf("action[MarkDecommissionStatus] dp[%v]tryRecoverReplicaMeta failed:%v",
			partition.PartitionID, err)
		return err
	}
	status = partition.GetDecommissionStatus()
	if err = partition.canMarkDecommission(status, c); err != nil {
		log.LogWarnf("action[MarkDecommissionStatus] dp[%v] cannot make decommission:%v",
			partition.PartitionID, err)
		return errors.NewErrorf("dp[%v] cannot make decommission err:%v",
			partition.PartitionID, err)
	}
	// if decommission the other replica of this dp, the status of decommission would be overwritten, so save it's error msg
	// to last decommission failed disk
	if status == DecommissionFail && partition.hasHost(partition.DecommissionSrcAddr) && srcAddr != partition.DecommissionSrcAddr {
		key := fmt.Sprintf("%s_%s", partition.DecommissionSrcAddr, partition.DecommissionSrcDiskPath)
		if value, ok := c.DecommissionDisks.Load(key); ok {
			disk := value.(*DecommissionDisk)
			if !disk.residualDecommissionDpsHas(partition.PartitionID) {
				disk.residualDecommissionDpsSave(partition.PartitionID, partition.DecommissionErrorMessage, c)
			}
		}

	}
	// for auto decommission, need raftForce to delete src if no leader
	if migrateType == AutoDecommission {
		log.LogDebugf("action[MarkDecommissionStatus] dp[%v] lostLeader %v leader %v interval %v",
			partition.PartitionID, partition.lostLeader(c), partition.getLeaderAddr(), time.Now().Unix()-partition.LeaderReportTime)
		if partition.lostLeader(c) {
			// auto add replica may be skipped, so check with ReplicaNum or Peers
			diskErrReplicaNum := partition.getReplicaDiskErrorNum()
			if diskErrReplicaNum == partition.ReplicaNum || diskErrReplicaNum == uint8(len(partition.Peers)) {
				log.LogWarnf("action[MarkDecommissionStatus] dp[%v] all live replica is unavaliable,"+
					" cannot handle in auto decommission mode", partition.decommissionInfo())
				return proto.ErrAllReplicaUnavailable
			}

			if partition.ReplicaNum == 3 && len(partition.Hosts) == 3 {
				diskErrReplicas := partition.getAllDiskErrorReplica()
				if isReplicasContainsHost(diskErrReplicas, partition.Hosts[0]) && isReplicasContainsHost(diskErrReplicas, partition.Hosts[1]) {
					if _, ok := partition.hasReplica(partition.Hosts[2]); ok {
						// raftForce delete host0 and host1
						toDeleteHosts := partition.Hosts[:2]
						for _, toDeleteHost := range toDeleteHosts {
							if err = c.removeDataReplica(partition, toDeleteHost, false, true); err != nil {
								log.LogWarnf("action[MarkDecommissionStatus] dp[%v] replicaNum[%v] remove first data replica[%v] failed, err: %v",
									partition.PartitionID, partition.ReplicaNum, toDeleteHosts, err)
								msg := fmt.Sprintf("dp(%v) replicaNum(%v) mark decommission found host(%v) unavailable, raftForce delete it",
									partition.decommissionInfo(), partition.ReplicaNum, toDeleteHost)
								auditlog.LogMasterOp("DataPartitionDecommission", msg, err)
								return
							}
						}
						// decommission success, reset status
						partition.ResetDecommissionStatus()
						partition.setRestoreReplicaStop()
						msg := fmt.Sprintf("dp(%v) replicaNum(%v) mark decommission found host0(%v) and host1(%v) unavailable, raftForce delete them",
							partition.decommissionInfo(), partition.ReplicaNum, toDeleteHosts[0], toDeleteHosts[1])
						auditlog.LogMasterOp("DataPartitionDecommission", msg, nil)
						return
					} else {
						log.LogWarnf("action[MarkDecommissionStatus] dp[%v] all live replica is unavaliable,"+
							" cannot handle in auto decommission mode", partition.decommissionInfo())
						return proto.ErrAllReplicaUnavailable
					}
				}
			}

			raftForce = true
			diskErrReplica := partition.getDiskErrorReplica()
			if diskErrReplica != nil {
				log.LogWarnf("action[MarkDecommissionStatus] dp[%v] has disk error replica %v:%v",
					partition.PartitionID, diskErrReplica.Addr, diskErrReplica.DiskPath)
				// decommission disk err replica first
				if diskErrReplica.Addr != srcAddr {
					srcAddr = diskErrReplica.Addr
					srcDisk = diskErrReplica.DiskPath
					log.LogWarnf("action[MarkDecommissionStatus] dp[%v] decommission bad replica %v_%v first, expect"+
						"to decommission %v",
						partition.PartitionID, diskErrReplica.Addr, diskErrReplica.DiskPath, srcAddr)
					err = proto.ErrDecommissionDiskErrDPFirst
				}
				// in the case of autoDecommission and dp no leader :
				// 1. for three replicas dp with two diskErr replicas and one normal replica should be set to the highest priority.
				// 2. for three replicas dp with one replica missing, one diskErr replica and one normal replica should be set to the highest priority.
				// 3. for three replicas dp with one diskErr replica and two normal replicas should be set to the high priority.
				// 4. for two replicas dp with one diskErr replica should be set to high priority.
				if partition.ReplicaNum == 3 {
					if (diskErrReplicaNum == 2 && len(partition.Hosts) == 3) || (diskErrReplicaNum == 1 && len(partition.Hosts) == 2) {
						weight = highestPriorityDecommissionWeight
					} else {
						weight = highPriorityDecommissionWeight
					}
				} else if partition.ReplicaNum == 2 {
					weight = highPriorityDecommissionWeight
				}
			}
		} else {
			// for special dp , if no replica is disk err, leader should not be none, so decommission the replica it hoped
			// in the case of autoDecommission and dp has leader :
			// 1. for three replicas dp with one diskErr replica and two normal replicas should be set to the high priority.
			// 2. for three replicas dp with three normal replica(may be unmarked replicas on bad disks) should keep the medium priority of the incoming weight parameter.
			// 3. for two replicas dp with two normal replica(may be unmarked replicas on bad disks) should keep the medium priority of the incoming weight parameter.
			// 4. for one replica dp with one normal replica(may be unmarked replicas on bad disks) should keep the medium priority of the incoming weight parameter.
			if partition.ReplicaNum == 3 && partition.getReplicaDiskErrorNum() == 1 {
				diskErrReplica := partition.getDiskErrorReplica()
				if diskErrReplica != nil {
					// decommission disk err replica first
					if diskErrReplica.Addr != srcAddr {
						srcAddr = diskErrReplica.Addr
						srcDisk = diskErrReplica.DiskPath
						log.LogWarnf("action[MarkDecommissionStatus] dp[%v] decommission bad replica %v_%v first",
							partition.PartitionID, diskErrReplica.Addr, diskErrReplica.DiskPath)
						err = proto.ErrDecommissionDiskErrDPFirst
					}
					weight = highPriorityDecommissionWeight
				}
			}
		}
	} else {
		if partition.lostLeader(c) {
			// auto add replica may be skipped, so check with ReplicaNum or Peers
			diskErrReplicaNum := partition.getReplicaDiskErrorNum()
			if diskErrReplicaNum == partition.ReplicaNum || diskErrReplicaNum == uint8(len(partition.Peers)) {
				log.LogWarnf("action[MarkDecommissionStatus] dp[%v] all replica is unavaliable, cannot handle in manual decommission mode",
					partition.PartitionID)
				return proto.ErrAllReplicaUnavailable
			}
		}

		if migrateType == ManualDecommission && partition.ReplicaNum == 3 && len(partition.Hosts) >= 2 {
			diskErrReplicas := partition.getAllDiskErrorReplica()
			if raftForce {
				if (isReplicasContainsHost(diskErrReplicas, partition.Hosts[0]) && isReplicasContainsHost(diskErrReplicas, partition.Hosts[1])) ||
					(isReplicasContainsHost(diskErrReplicas, partition.Hosts[0]) && srcAddr != partition.Hosts[0]) ||
					(isReplicasContainsHost(diskErrReplicas, partition.Hosts[1]) && srcAddr == partition.Hosts[0]) {
					// mark decommission failed
					log.LogWarnf("action[MarkDecommissionStatus] dp[%v] replicaNum[%v] raftForce[%v] host0 other than the srcAddr is unavaliable, cannot handle in manual decommission mode",
						partition.PartitionID, partition.ReplicaNum, raftForce)
					return proto.ErrFirstHostUnavailable
				}
			}
		}

		if migrateType == ManualDecommission && partition.ReplicaNum == 2 && len(partition.Hosts) == 2 {
			diskErrReplicas := partition.getAllDiskErrorReplica()
			if raftForce {
				if (isReplicasContainsHost(diskErrReplicas, partition.Hosts[0]) && srcAddr == partition.Hosts[1]) ||
					(isReplicasContainsHost(diskErrReplicas, partition.Hosts[1]) && srcAddr == partition.Hosts[0]) {
					// mark decommission failed
					log.LogWarnf("action[MarkDecommissionStatus] dp[%v] replicaNum[%v] raftForce(%v) host0 other than the srcAddr is unavaliable, cannot handle in manual decommission mode",
						partition.PartitionID, partition.ReplicaNum, raftForce)
					return proto.ErrFirstHostUnavailable
				}
			} else {
				if isReplicasContainsHost(diskErrReplicas, partition.Hosts[0]) {
					// mark decommission failed
					log.LogWarnf("action[MarkDecommissionStatus] dp[%v] replicaNum[%v] host0[%v] is unavaliable, cannot handle in manual decommission mode",
						partition.PartitionID, partition.ReplicaNum, partition.Hosts[0])
					return proto.ErrFirstHostUnavailable
				}
			}
		}
		// in the case of manualDecommission :
		// 1. for all replicaNum dp should keep the priority(specified when executing the decommission) of the incoming weight parameter.
		// in the case of autoAddReplica :
		// 2. for all replicaNum dp should keep the high priority of the incoming weight parameter.
	}
directly:
	waitTimes := 0
	if partition.IsDecommissionPaused() {
		if !partition.pauseReplicaRepair(partition.DecommissionDstAddr, false, c) {
			log.LogWarnf("action[MarkDecommissionStatus] dp [%d] recover from stop failed", partition.PartitionID)
			return errors.NewErrorf("action[MarkDecommissionStatus] dp [%d] recover from stop failed", partition.PartitionID)
		}
		// forbidden dp to restore meta for replica
		// if migrateType is AutoAddReplica, RestoreReplica is already RestoreReplicaMetaRunning, so do not need to check
		// this flag again

		for {
			if !partition.setRestoreReplicaForbidden() && migrateType != AutoAddReplica {
				waitTimes++
				if waitTimes >= defaultSetRestoreReplicaStatusLimit {
					return errors.NewErrorf("set RestoreReplicaMetaForbidden timeout")
				}
				// maybe other replica is decommissioning and that replica is added into decommission list
				// recently
				if c.processDataPartitionDecommission(partition.PartitionID) {
					return errors.NewErrorf("dp[%v] %v", partition.PartitionID, proto.ErrPerformingDecommission.Error())
				}
				// wait for checkReplicaMeta ended
				log.LogWarnf("action[MarkDecommissionStatus] dp [%d]wait for setting restore replica forbidden",
					partition.PartitionID)
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
		partition.DecommissionRetry = 0
		partition.SetDecommissionStatus(markDecommission)
		// update decommissionTerm for next time query
		partition.DecommissionTerm = term
		partition.DecommissionWeight = weight
		partition.DecommissionRaftForce = raftForce
		partition.DecommissionType = migrateType
		return
	}
	// forbidden dp to restore meta for replica
	for {
		if !partition.setRestoreReplicaForbidden() && migrateType != AutoAddReplica {
			waitTimes++
			if waitTimes >= defaultSetRestoreReplicaStatusLimit {
				return errors.NewErrorf("set RestoreReplicaMetaForbidden timeout")
			}
			// maybe other replica is decommissioning and that replica is added into decommission list
			// recently
			if c.processDataPartitionDecommission(partition.PartitionID) {
				return errors.NewErrorf("dp[%v] %v", partition.PartitionID, proto.ErrPerformingDecommission.Error())
			}
			log.LogWarnf("action[MarkDecommissionStatus] dp [%d]wait for setting restore replica forbidden",
				partition.PartitionID)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	// initial or failed restart
	partition.ResetDecommissionStatus()
	partition.DecommissionType = migrateType
	partition.SetDecommissionStatus(markDecommission)
	partition.DecommissionSrcAddr = srcAddr
	partition.DecommissionDstAddr = dstAddr
	partition.DecommissionSrcDiskPath = srcDisk
	partition.DecommissionRaftForce = raftForce
	partition.DecommissionTerm = term
	partition.DecommissionWeight = weight
	partition.DecommissionErrorMessage = ""
	// reset special replicas decommission status
	partition.isRecover = false
	partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	if partition.DecommissionSrcDiskPath == "" {
		partition.RLock()
		replica, _ := partition.getReplica(srcAddr)
		partition.RUnlock()
		if replica != nil {
			partition.DecommissionSrcDiskPath = replica.DiskPath
		}
	}
	if dstAddr != "" {
		partition.DecommissionDstAddrSpecify = true
	}
	log.LogDebugf("action[MarkDecommissionStatus] dp[%v]", partition.decommissionInfo())
	return
}

func (partition *DataPartition) SetDecommissionStatus(status uint32) {
	log.LogDebugf("[SetDecommissionStatus] set dp(%v) decommission status to status(%v)", partition.PartitionID, status)
	atomic.StoreUint32(&partition.DecommissionStatus, status)
}

func (partition *DataPartition) SetSpecialReplicaDecommissionStep(step uint32) {
	atomic.StoreUint32(&partition.SpecialReplicaDecommissionStep, step)
}

func (partition *DataPartition) GetDecommissionStatus() uint32 {
	return atomic.LoadUint32(&partition.DecommissionStatus)
}

func (partition *DataPartition) GetSpecialReplicaDecommissionStep() uint32 {
	return atomic.LoadUint32(&partition.SpecialReplicaDecommissionStep)
}

func (partition *DataPartition) IsDecommissionSuccess() bool {
	return partition.GetDecommissionStatus() == DecommissionSuccess
}

func (partition *DataPartition) IsDecommissionFailed() bool {
	return partition.GetDecommissionStatus() == DecommissionFail
}

func (partition *DataPartition) IsDecommissionRunning() bool {
	return partition.GetDecommissionStatus() == DecommissionRunning
}

func (partition *DataPartition) IsDecommissionPrepare() bool {
	return partition.GetDecommissionStatus() == DecommissionPrepare
}

func (partition *DataPartition) IsDecommissionPaused() bool {
	return partition.GetDecommissionStatus() == DecommissionPause
}

func (partition *DataPartition) IsDecommissionInitial() bool {
	return partition.GetDecommissionStatus() == DecommissionInitial
}

func (partition *DataPartition) IsMarkDecommission() bool {
	return partition.GetDecommissionStatus() == markDecommission
}

func (partition *DataPartition) IsDoingDecommission() bool {
	decommStatus := partition.GetDecommissionStatus()

	return (decommStatus > DecommissionInitial && decommStatus < DecommissionSuccess)
}

func (partition *DataPartition) TryToDecommission(c *Cluster) bool {
	if !partition.IsMarkDecommission() {
		log.LogWarnf("action[TryToDecommission] failed dp[%v] status expected markDecommission[%v]",
			partition.decommissionInfo(), atomic.LoadUint32(&partition.DecommissionStatus))
		return false
	}

	log.LogDebugf("action[TryToDecommission] dp[%v]", partition.decommissionInfo())

	return partition.Decommission(c)
}

func (partition *DataPartition) Decommission(c *Cluster) bool {
	var (
		msg                  string
		err                  error
		srcAddr              = partition.DecommissionSrcAddr
		targetAddr           = partition.DecommissionDstAddr
		srcReplica           *DataReplica
		resetDecommissionDst = true
		begin                = time.Now()
		finalHosts           = make([]string, len(partition.Hosts))
	)

	if partition.GetDecommissionStatus() == DecommissionInitial {
		log.LogWarnf("action[decommissionDataPartition] dp [%v] may be cancel", partition.decommissionInfo())
		partition.DecommissionErrorMessage = "cancel decommission"
		partition.markRollbackFailed(false)
		return false
	}
	if !c.AutoDecommissionDiskIsEnabled() && partition.DecommissionType == AutoDecommission {
		log.LogWarnf("action[decommissionDataPartition] dp [%v] decommission is disable", partition.decommissionInfo())
		partition.DecommissionErrorMessage = "disable auto " +
			" decommission"
		partition.markRollbackFailed(false)
		return false
	}

	partition.RLock()
	copy(finalHosts, partition.Hosts)
	finalHosts = append(finalHosts, targetAddr) // add new one
	partition.RUnlock()
	for i, host := range finalHosts {
		if host == srcAddr {
			finalHosts = append(finalHosts[:i], finalHosts[i+1:]...) // remove old one
			break
		}
	}
	if err = c.checkMultipleReplicasOnSameMachine(finalHosts); err != nil {
		goto errHandler
	}

	if partition.ReplicaNum == 1 && partition.DecommissionRaftForce {
		log.LogWarnf("action[decommissionDataPartition] dp [%v] single replica does not support raftForce deletion", partition.decommissionInfo())
		partition.DecommissionErrorMessage = "single replica does not support raftForce deletion"
		partition.markRollbackFailed(false)
		return false
	}

	partition.SetDecommissionStatus(DecommissionPrepare)
	err = c.syncUpdateDataPartition(partition)
	if err != nil {
		log.LogWarnf("action[decommissionDataPartition] dp [%v] update to prepare failed", partition.PartitionID)
		goto errHandler
	}
	log.LogInfof("action[decommissionDataPartition] dp[%v] start decommission ", partition.decommissionInfo())
	// NOTE: delete if not normal data partition or dp is discard
	if partition.IsDiscard || !proto.IsNormalDp(partition.PartitionType) {
		// if _, ok := c.vols[partition.VolName]; !ok {
		// 	log.LogWarnf("action[decommissionDataPartition]vol [%v] for dp [%v] is deleted ", partition.VolName,
		// 		partition.PartitionID)
		// } else {
		// 	log.LogWarnf("[decommissionDataPartition] delete dp(%v) discard(%v)", partition.PartitionID, partition.IsDiscard)
		// 	vol.deleteDataPartition(c, partition)
		// }
		partition.SetDecommissionStatus(DecommissionSuccess)
		log.LogWarnf("action[decommissionDataPartition] skip dp(%v) discard(%v)", partition.PartitionID, partition.IsDiscard)
		return true
	}
	defer func() {
		c.syncUpdateDataPartition(partition)
	}()

	// if decommission src for dp is not reset and decommission dst is already repaired
	srcReplica, _ = partition.getReplica(partition.DecommissionSrcAddr)

	if len(partition.Replicas) == int(partition.ReplicaNum) && srcReplica == nil {
		partition.SetDecommissionStatus(DecommissionSuccess)
		log.LogWarnf("action[decommissionDataPartition]dp(%v) status(%v) is already decommissioned",
			partition.PartitionID, partition.Status)
		return true
	}
	// do not check condition for decommission if set DecommissionRaftForce
	// do not check condition for decommission for AutoAddReplica
	if !partition.DecommissionRaftForce && partition.DecommissionType != AutoAddReplica {
		if err = c.validateDecommissionDataPartition(partition, srcAddr); err != nil {
			goto errHandler
		}
	}

	// in the raftForce case, need to check if all replicas except the decommission src addr are diskErr replicas
	if partition.DecommissionRaftForce && (partition.DecommissionType == AutoDecommission || partition.DecommissionType == ManualDecommission) {
		if partition.isReplicaAllDiskErrorExceptSrcAddr() {
			msg = fmt.Sprintf("dp(%v) all replicas except decommission src addr(%v) are diskErr replicas", partition.PartitionID, partition.DecommissionSrcAddr)
			log.LogWarnf("action[decommissionDataPartition] %s", msg)
			auditlog.LogMasterOp("DataPartitionDecommission", msg, nil)
			partition.DecommissionErrorMessage = msg
			partition.markRollbackFailed(false)
			return false
		}
	}

	// if master change and recover SpecialDecommission, do not need to check dataNode size,
	// it is checked before
	if !(partition.isSpecialReplicaCnt() && partition.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddRes) {
		err = c.updateDataNodeSize(targetAddr, partition)
		if err != nil {
			log.LogWarnf("action[decommissionDataPartition] target addr can't be writable, add %s %s", targetAddr, err.Error())
			goto errHandler
		}
	}
	defer func() {
		if err != nil {
			c.returnDataSize(targetAddr, partition)
		}
	}()
	// if single/two replica without raftforce
	if partition.isSpecialReplicaCnt() && !partition.DecommissionRaftForce {
		if partition.GetSpecialReplicaDecommissionStep() == SpecialDecommissionInitial {
			partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionEnter)
		}
		if err = c.decommissionSingleDp(partition, targetAddr, srcAddr); err != nil {
			// when dp retry decommission, step into SpecialDecommissionWaitAddResFin above
			// do not reset decommission dst when master leader changed
			if partition.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddResFin ||
				strings.Contains(err.Error(), "master leader changed") ||
				strings.Contains(err.Error(), "old replica unavailable") {
				resetDecommissionDst = false
			}
			goto errHandler
		}
	} else {
		if err = c.removeDataReplica(partition, srcAddr, false, partition.DecommissionRaftForce); err != nil {
			goto errHandler
		}
		if err = c.addDataReplica(partition, targetAddr, true, false); err != nil {
			goto errHandler
		}
		newReplica, _ := partition.getReplica(targetAddr)
		newReplica.Status = proto.Recovering // in case heartbeat response is not arrived
		partition.isRecover = true
		partition.Status = proto.ReadOnly
		partition.SetDecommissionStatus(DecommissionRunning)
		partition.RecoverUpdateTime = time.Now()
		partition.RecoverStartTime = time.Now()
		c.putBadDataPartitionIDsByDiskPath(partition.DecommissionSrcDiskPath, partition.DecommissionSrcAddr, partition.PartitionID)
	}
	// only stop 3-replica,need to release token
	if partition.IsDecommissionPaused() {
		log.LogInfof("action[decommissionDataPartition]clusterID[%v] partitionID:%v decommission paused", c.Name, partition.PartitionID)
		if !partition.pauseReplicaRepair(partition.DecommissionDstAddr, true, c) {
			log.LogWarnf("action[decommissionDataPartition]clusterID[%v] partitionID:%v  paused failed", c.Name, partition.PartitionID)
		}
		return true
	} else {
		msg := fmt.Sprintf("clusterID[%v] info[%v] offline success: consume[%v]seconds",
			c.Name, partition.decommissionInfo(), time.Since(begin).Seconds())
		log.LogInfof("action[decommissionDataPartition] %v", msg)
		auditlog.LogMasterOp("DataPartitionDecommission", msg, nil)
		return true
	}

errHandler:
	// special replica num receive stop signal,donot reset  SingleDecommissionStatus for decommission again
	if partition.GetDecommissionStatus() == DecommissionPause {
		log.LogWarnf("action[decommissionDataPartition] partitionID:%v is stopped", partition.PartitionID)
		return true
	}
	partition.DecommissionRetry++
	partition.DecommissionRetryTime = time.Now()
	// if need rollback, set to fail
	// do not reset DecommissionDstAddr outside the rollback operation, as it may cause rollback failure
	if partition.DecommissionNeedRollback {
		partition.SetDecommissionStatus(DecommissionFail)
	} else {
		// The maximum number of retries for the DP error has been reached,
		// and a rollback is still required, even if the rollback conditions have not been triggered.
		if partition.DecommissionRetry >= defaultDecommissionRetryLimit {
			partition.markRollbackFailed(true)
		} else {
			// remove dp from BadDataPartitionIDs, preventing errors caused by disk manager not finding the replica
			err := c.removeDPFromBadDataPartitionIDs(partition.DecommissionSrcAddr, partition.DecommissionSrcDiskPath, partition.PartitionID)
			if err != nil {
				log.LogWarnf("action[decommissionDataPartition] del dp[%v] from bad dataPartitionIDs failed:%v", partition.PartitionID, err)
			}
			partition.ReleaseDecommissionToken(c)
			partition.ReleaseDecommissionFirstHostToken(c)
			// choose other node to create data partition when retry decommission if not specify dst
			if resetDecommissionDst && !partition.DecommissionDstAddrSpecify {
				partition.DecommissionDstAddr = ""
				log.LogWarnf("action[decommissionDataPartition] partitionID:%v reset DecommissionDstAddr", partition.PartitionID)
			}
			partition.SetDecommissionStatus(markDecommission)
		}
	}
	msg = fmt.Sprintf("clusterID[%v] info[%v] offline failed:%v consume[%v]seconds",
		c.Name, partition.decommissionInfo(), err.Error(), time.Since(begin).Seconds())
	log.LogWarnf("action[decommissionDataPartition] %s", msg)
	auditlog.LogMasterOp("DataPartitionDecommission", msg, err)
	partition.DecommissionErrorMessage = err.Error()
	return false
}

func (partition *DataPartition) PauseDecommission(c *Cluster) bool {
	status := partition.GetDecommissionStatus()
	// support retry pause if pause failed last time
	if status == DecommissionInitial || status == DecommissionSuccess ||
		status == DecommissionFail {
		log.LogWarnf("action[PauseDecommission] dp[%v] cannot be stopped status[%v]", partition.PartitionID, status)
		return true
	}
	defer c.syncUpdateDataPartition(partition)
	log.LogDebugf("action[PauseDecommission] dp[%v] status %v set to stop ",
		partition.PartitionID, partition.GetDecommissionStatus())

	if status == markDecommission {
		partition.SetDecommissionStatus(DecommissionPause)
		return true
	}
	if partition.isSpecialReplicaCnt() {
		log.LogDebugf("action[PauseDecommission]special replica dp[%v] status[%v]",
			partition.PartitionID, partition.GetSpecialReplicaDecommissionStep())
		partition.SpecialReplicaDecommissionStop <- false
		// if special replica is repairing, stop the process
		if partition.GetSpecialReplicaDecommissionStep() == SpecialDecommissionWaitAddRes {
			if !partition.pauseReplicaRepair(partition.DecommissionDstAddr, true, c) {
				return false
			}
		}
	} else {
		if partition.IsDecommissionRunning() {
			if !partition.pauseReplicaRepair(partition.DecommissionDstAddr, true, c) {
				return false
			}
			log.LogDebugf("action[PauseDecommission] dp[%v] status [%v] send stop signal ",
				partition.PartitionID, partition.GetDecommissionStatus())
		}
	}
	partition.SetDecommissionStatus(DecommissionPause)
	partition.isRecover = false
	return true
}

func (partition *DataPartition) ResetDecommissionStatus() {
	partition.DecommissionDstAddr = ""
	partition.DecommissionSrcAddr = ""
	partition.DecommissionRetry = 0
	partition.DecommissionRaftForce = false
	partition.DecommissionSrcDiskPath = ""
	partition.isRecover = false
	partition.DecommissionTerm = 0
	partition.DecommissionWeight = 0
	partition.DecommissionDstAddrSpecify = false
	partition.DecommissionNeedRollback = false
	atomic.StoreUint32(&partition.DecommissionNeedRollbackTimes, 0)
	partition.SetDecommissionStatus(DecommissionInitial)
	partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	partition.DecommissionErrorMessage = ""
	partition.DecommissionType = InitialDecommission
	partition.RecoverStartTime = time.Time{}
	partition.RecoverUpdateTime = time.Time{}
	partition.DecommissionRetryTime = time.Time{}
}

func (partition *DataPartition) resetRestoreMeta(expected uint32) (ok bool) {
	ok = atomic.CompareAndSwapUint32(&partition.RestoreReplica, expected, RestoreReplicaMetaStop)
	return
}

func (partition *DataPartition) rollback(c *Cluster) {
	var err error
	defer func() {
		c.syncUpdateDataPartition(partition)
		auditlog.LogMasterOp("DataPartitionDecommissionRollback",
			fmt.Sprintf("dp %v rollback end", partition.decommissionInfo()), err)
	}()
	auditlog.LogMasterOp("DataPartitionDecommissionRollback",
		fmt.Sprintf("dp %v rollback start", partition.decommissionInfo()), nil)
	// delete it from BadDataPartitionIds
	err = c.removeDPFromBadDataPartitionIDs(partition.DecommissionSrcAddr, partition.DecommissionSrcDiskPath, partition.PartitionID)
	if err != nil {
		log.LogWarnf("action[rollback]dp[%v] rollback to del from bad dataPartitionIDs failed:%v", partition.PartitionID, err)
	}
	err = partition.removeReplicaByForce(c, partition.DecommissionDstAddr, true, false)
	if err != nil {
		// keep decommission status to failed for rollback
		log.LogWarnf("action[rollback]dp[%v] rollback to del replica[%v] failed:%v",
			partition.PartitionID, partition.DecommissionDstAddr, err.Error())
		partition.DecommissionErrorMessage = fmt.Sprintf("rollback failed:%v", err.Error())
		return
	}
	// err = partition.restoreReplicaMeta(c)
	// if err != nil {
	//	return
	// }
	// release token first
	partition.ReleaseDecommissionToken(c)
	partition.ReleaseDecommissionFirstHostToken(c)
	// reset status if rollback success
	partition.DecommissionRetry = 0
	partition.DecommissionRetryTime = time.Time{}
	partition.isRecover = false
	partition.DecommissionNeedRollback = false
	partition.DecommissionErrorMessage = ""
	partition.SetDecommissionStatus(markDecommission)
	partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	// specify dst addr do not need rollback
	// keep DecommissionSrcAddr to prevent allocate DecommissionSrcAddr data node during acquire token
	if !partition.DecommissionDstAddrSpecify {
		partition.DecommissionDstAddr = ""
	}
	log.LogWarnf("action[rollback]dp[%v] rollback success", partition.PartitionID)
}

func (partition *DataPartition) addToDecommissionList(c *Cluster) {
	if partition.DecommissionSrcAddr == "" {
		return
	}
	var (
		dataNode *DataNode
		zone     *Zone
		ns       *nodeSet
		err      error
	)
	if dataNode, err = c.dataNode(partition.DecommissionSrcAddr); err != nil {
		log.LogWarnf("action[addToDecommissionList]find dp[%v] src decommission dataNode [%v] failed[%v]",
			partition.PartitionID, partition.DecommissionSrcAddr, err.Error())
		return
	}

	if dataNode.ZoneName == "" {
		log.LogWarnf("action[addToDecommissionList]dataNode[%v] zone is nil", dataNode.Addr)
		return
	}

	if zone, err = c.t.getZone(dataNode.ZoneName); err != nil {
		log.LogWarnf("action[addToDecommissionList]dataNode[%v] zone is nil:%v", dataNode.Addr, err.Error())
		return
	}

	if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
		log.LogWarnf("action[addToDecommissionList]dataNode[%v] nodeSet is nil:%v", dataNode.Addr, err.Error())
		return
	}
	log.LogInfof("action[addToDecommissionList]ready to add dp[%v] decommission src[%v] Disk[%v] dst[%v] status[%v] specialStep[%v],"+
		" RollbackTimes(%v) isRecover(%v) host[%v] to  decommission list[%v]",
		partition.PartitionID, partition.DecommissionSrcAddr, partition.DecommissionSrcDiskPath,
		partition.DecommissionDstAddr, partition.GetDecommissionStatus(), partition.GetSpecialReplicaDecommissionStep(),
		partition.DecommissionNeedRollbackTimes, partition.isRecover, partition.Hosts, ns.ID)
	ns.AddToDecommissionDataPartitionList(partition, c)
}

func (partition *DataPartition) checkConsumeToken() bool {
	return partition.IsDecommissionRunning() || partition.IsDecommissionSuccess() || partition.IsDecommissionFailed()
}

// only mark stop status or initial
func (partition *DataPartition) canMarkDecommission(status uint32, c *Cluster) error {
	// dp may not be reset decommission status from last decommission
	// if partition.DecommissionTerm != term {
	//	return true
	// }
	// make sure dp release the token
	rollbackTimes := atomic.LoadUint32(&partition.DecommissionNeedRollbackTimes)
	if c.processDataPartitionDecommission(partition.PartitionID) {
		return errors.NewErrorf("dp[%v] %v", partition.PartitionID, proto.ErrPerformingDecommission.Error())
	}
	if status == DecommissionInitial ||
		status == DecommissionPause {
		return nil
	}
	// do not need to check rollback times, when reach max, dp is removed for decommission list
	if status == DecommissionFail {
		return nil
	}
	return errors.NewErrorf("dp[%v]cannot mark decommission: status %v rollbackTimes %v",
		partition.PartitionID, status, rollbackTimes)
}

func (partition *DataPartition) canAddToDecommissionList() bool {
	status := partition.GetDecommissionStatus()
	if status == DecommissionInitial ||
		status == DecommissionPause {
		return false
	}
	return true
}

func (partition *DataPartition) tryRollback(c *Cluster) bool {
	if !partition.needRollback(c) {
		return false
	}
	atomic.AddUint32(&partition.DecommissionNeedRollbackTimes, 1)
	partition.rollback(c)
	return true
}

func (partition *DataPartition) IsRollbackFailed() bool {
	return partition.IsDecommissionFailed() &&
		atomic.LoadUint32(&partition.DecommissionNeedRollbackTimes) >= defaultDecommissionRollbackLimit
}

func (partition *DataPartition) pauseReplicaRepair(replicaAddr string, stop bool, c *Cluster) bool {
	index := partition.findReplica(replicaAddr)
	if index == -1 {
		log.LogWarnf("action[pauseReplicaRepair]dp[%v] can't find replica %v", partition.PartitionID, replicaAddr)
		// maybe paused from rollback[mark]
		return true
	}
	const RetryMax = 5
	var (
		dataNode *DataNode
		err      error
		retry    = 0
	)

	for retry <= RetryMax {
		if dataNode, err = c.dataNode(replicaAddr); err != nil {
			retry++
			time.Sleep(time.Second)
			log.LogWarnf("action[pauseReplicaRepair]dp[%v] can't find dataNode %v", partition.PartitionID, partition.DecommissionSrcAddr)
			continue
		}
		task := partition.createTaskToStopDataPartitionRepair(replicaAddr, stop)
		packet, err := dataNode.TaskManager.syncSendAdminTask(task)
		if err != nil {
			retry++
			time.Sleep(time.Second)
			log.LogWarnf("action[pauseReplicaRepair]dp[%v] send stop task failed %v", partition.PartitionID, err.Error())
			continue
		}
		if !stop {
			partition.RecoverUpdateTime = time.Now().Add(-partition.RecoverLastConsumeTime)
			partition.RecoverLastConsumeTime = time.Duration(0)
			log.LogDebugf("action[pauseReplicaRepair]dp[%v] replica %v RecoverUpdateTime sub %v seconds",
				partition.PartitionID, replicaAddr, partition.RecoverLastConsumeTime.Seconds())
		} else {
			partition.RecoverLastConsumeTime = time.Since(partition.RecoverUpdateTime)
			log.LogDebugf("action[pauseReplicaRepair]dp[%v] replica %v already recover %v seconds",
				partition.PartitionID, replicaAddr, partition.RecoverLastConsumeTime.Seconds())
		}
		log.LogDebugf("action[pauseReplicaRepair]dp[%v] send stop to  replica %v packet %v", partition.PartitionID, replicaAddr, packet)
		return true
	}
	return false
}

func (partition *DataPartition) findReplica(replicaAddr string) int {
	partition.Lock()
	defer partition.Unlock()
	var (
		replica *DataReplica
		index   = -1
	)

	for i := 0; i < len(partition.Replicas); i++ {
		replica = partition.Replicas[i]
		if replica.Addr == replicaAddr {
			index = i
			break
		}
	}
	return index
}

func (partition *DataPartition) createTaskToStopDataPartitionRepair(addr string, stop bool) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpStopDataPartitionRepair, addr, newStopDataPartitionRepairRequest(partition.PartitionID, stop))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) TryAcquireDecommissionToken(c *Cluster) bool {
	var (
		zone            *Zone
		ns              *nodeSet
		err             error
		targetHosts     []string
		excludeNodeSets []uint64
		zones           []string
		result          = false
	)
	defer c.syncUpdateDataPartition(partition)
	begin := time.Now()
	defer func() {
		log.LogDebugf("action[TryAcquireDecommissionToken] dp %v get token to %v consume(%v) err(%v) result(%v)",
			partition.decommissionInfo(), partition.DecommissionDstAddr, time.Since(begin).String(), err, result)
	}()

	// the first time for dst addr not specify
	if !partition.DecommissionDstAddrSpecify && partition.DecommissionDstAddr == "" {
		// try to find available data node in src nodeset
		ns, zone, err = getTargetNodeset(partition.DecommissionSrcAddr, c)
		if err != nil {
			log.LogWarnf("action[TryAcquireDecommissionToken] dp %v find src nodeset failed:%v",
				partition.PartitionID, err.Error())
			goto errHandler
		}
		if partition.isSpecialReplicaCnt() && ns.HasDecommissionToken(partition.PartitionID) {
			log.LogDebugf("action[TryAcquireDecommissionToken]dp %v has token when reloading meta from nodeset %v",
				partition.PartitionID, ns.ID)
			result = true
			return true
		}

		excludeHosts := partition.Hosts
		// if dp rollback success, DecommissionSrcAddr is not contained in dp.hosts, so we must prevent
		// to create new replica on DecommissionSrcAddr, eg 3 replica dp recover failed, but dp hosts do
		// not contain DecommissionSrcAddr when completing rolling back
		if partition.DecommissionSrcAddr != "" && !partition.hasHost(partition.DecommissionSrcAddr) {
			excludeHosts = append(excludeHosts, partition.DecommissionSrcAddr)
		}
		log.LogDebugf("action[TryAcquireDecommissionToken]dp %v excludeHosts %v",
			partition.PartitionID, excludeHosts)
		// data nodes in a nodeset has the same mediaType
		targetHosts, _, err = ns.getAvailDataNodeHosts(excludeHosts, 1)
		if err != nil {
			log.LogWarnf("action[TryAcquireDecommissionToken] dp %v choose from src nodeset failed:%v",
				partition.PartitionID, err.Error())
			if _, ok := c.vols[partition.VolName]; !ok {
				log.LogWarnf("action[TryAcquireDecommissionToken] dp %v cannot find vol:%v",
					partition.PartitionID, err.Error())
				goto errHandler
			}

			if c.isFaultDomain(c.vols[partition.VolName]) {
				log.LogWarnf("action[TryAcquireDecommissionToken] dp %v is fault domain",
					partition.PartitionID)
				goto errHandler
			}
			excludeNodeSets = append(excludeNodeSets, ns.ID)
			// data nodes in a zone has the same mediaType
			if targetHosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, excludeNodeSets, excludeHosts, 1); err != nil {
				log.LogWarnf("action[TryAcquireDecommissionToken] dp %v choose from other nodeset failed:%v",
					partition.PartitionID, err.Error())
				// select data nodes from the other zone
				zones = partition.getLiveZones(partition.DecommissionSrcAddr)
				if targetHosts, _, err = c.getHostFromNormalZone(TypeDataPartition, zones, excludeNodeSets, excludeHosts, 1, 1, "", partition.MediaType); err != nil {
					log.LogWarnf("action[TryAcquireDecommissionToken] dp %v choose from other zone failed:%v",
						partition.PartitionID, err.Error())
					goto errHandler
				}
			}
			// get nodeset for target host
			newAddr := targetHosts[0]
			ns, _, err = getTargetNodeset(newAddr, c)
			if err != nil {
				log.LogWarnf("action[TryAcquireDecommissionToken] dp %v find new nodeset failed:%v",
					partition.PartitionID, err.Error())
				goto errHandler
			}
		}
		// only persist DecommissionDstAddr when get token
		if ns.AcquireDecommissionToken(partition.PartitionID) {
			partition.DecommissionDstAddr = targetHosts[0]
			log.LogDebugf("action[TryAcquireDecommissionToken] dp %v get token from %v nodeset %v success",
				partition.PartitionID, partition.DecommissionDstAddr, ns.ID)
			result = true
			return true
		} else {
			log.LogDebugf("action[TryAcquireDecommissionToken] dp %v: nodeset %v token is empty",
				partition.PartitionID, ns.ID)
			return false
		}
	} else {
		ns, _, err = getTargetNodeset(partition.DecommissionDstAddr, c)
		if err != nil {
			log.LogWarnf("action[TryAcquireDecommissionToken]dp %v find src nodeset failed:%v",
				partition.PartitionID, err.Error())
			goto errHandler
		}
		if partition.isSpecialReplicaCnt() && ns.HasDecommissionToken(partition.PartitionID) {
			log.LogDebugf("action[TryAcquireDecommissionToken]dp %v has token when reloading meta from nodeset[%v]",
				partition.PartitionID, ns.ID)
			return true
		}
		if ns.AcquireDecommissionToken(partition.PartitionID) {
			log.LogDebugf("action[TryAcquireDecommissionToken]dp %v get token from %v nodeset %v success",
				partition.PartitionID, partition.DecommissionDstAddr, ns.ID)
			return true
		} else {
			log.LogDebugf("action[TryAcquireDecommissionToken] dp %v: nodeset %v token is empty",
				partition.PartitionID, ns.ID)
			return false
		}
	}
errHandler:
	partition.DecommissionRetry++
	partition.DecommissionRetryTime = time.Now()
	if partition.DecommissionRetry >= defaultDecommissionRetryLimit {
		partition.markRollbackFailed(false)
	}
	partition.DecommissionErrorMessage = err.Error()
	log.LogWarnf("action[TryAcquireDecommissionToken] clusterID[%v] vol[%v] partitionID[%v]"+
		" retry [%v] status [%v] DecommissionDstAddrSpecify [%v] DecommissionDstAddr [%v] failed",
		c.Name, partition.VolName, partition.PartitionID, partition.DecommissionRetry, partition.GetDecommissionStatus(),
		partition.DecommissionDstAddrSpecify, partition.DecommissionDstAddr)
	return false
}

func (partition *DataPartition) ReleaseDecommissionToken(c *Cluster) {
	if partition.DecommissionDstAddr == "" {
		return
	}
	if ns, _, err := getTargetNodeset(partition.DecommissionDstAddr, c); err != nil {
		log.LogWarnf("action[ReleaseDecommissionToken]should never happen dp %v:%v", partition.PartitionID, err.Error())
		return
	} else {
		ns.ReleaseDecommissionToken(partition.PartitionID)
	}
}

// func (partition *DataPartition) ShouldReleaseDecommissionTokenByStop(c *Cluster) {
//	if partition.DecommissionDstAddr == "" && !partition.DecommissionDstAddrSpecify {
//		return
//	}
//	index := partition.findReplica(partition.DecommissionDstAddr)
//	if index == -1 {
//		log.LogWarnf("action[ShouldReleaseDecommissionTokenByStop]dp[%v] has not added replica %v",
//			partition.PartitionID, partition.DecommissionDstAddr)
//	}
//	partition.ReleaseDecommissionToken(c)
// }

func getTargetNodeset(addr string, c *Cluster) (ns *nodeSet, zone *Zone, err error) {
	var dataNode *DataNode
	dataNode, err = c.dataNode(addr)
	if err != nil {
		log.LogWarnf("action[getTargetNodeset] find src %v data node failed:%v", addr, err.Error())
		return nil, nil, err
	}
	zone, err = c.t.getZone(dataNode.ZoneName)
	if err != nil {
		log.LogWarnf("action[getTargetNodeset] find src %v zone failed:%v", addr, err.Error())
		return nil, nil, err
	}
	ns, err = zone.getNodeSet(dataNode.NodeSetID)
	if err != nil {
		log.LogWarnf("action[getTargetNodeset] find src %v nodeset failed:%v", addr, err.Error())
		return nil, nil, err
	}
	return ns, zone, nil
}

func (partition *DataPartition) needRollback(c *Cluster) bool {
	log.LogDebugf("action[needRollback]dp[%v]", partition.decommissionInfo())
	// failed by error except add replica or create dp or repair dp
	if !partition.DecommissionNeedRollback {
		return false
	}

	if atomic.LoadUint32(&partition.DecommissionNeedRollbackTimes) >= defaultDecommissionRollbackLimit {
		log.LogDebugf("action[needRollback]try delete dp[%v] replica %v DecommissionNeedRollbackTimes[%v]",
			partition.PartitionID, partition.DecommissionDstAddr, atomic.LoadUint32(&partition.DecommissionNeedRollbackTimes))
		// delete it from BadDataPartitionIds
		err := c.removeDPFromBadDataPartitionIDs(partition.DecommissionSrcAddr, partition.DecommissionSrcDiskPath, partition.PartitionID)
		if err != nil {
			log.LogWarnf("action[rollback]dp[%v] rollback to del from bad dataPartitionIDs failed:%v", partition.PartitionID, err)
		}
		partition.DecommissionNeedRollback = false
		removeAddr := partition.DecommissionDstAddr
		// when special replica partition enter SpecialDecommissionWaitAddResFin, new replica is recoverd, so only
		// need to delete DecommissionSrcAddr
		if partition.isSpecialReplicaCnt() && partition.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddResFin {
			removeAddr = partition.DecommissionSrcAddr
		}
		err = partition.removeReplicaByForce(c, removeAddr, true, false)
		if err != nil {
			log.LogWarnf("action[needRollback]dp[%v] remove decommission dst replica %v failed: %v",
				partition.PartitionID, removeAddr, err)
		}
		c.syncUpdateDataPartition(partition)
		auditlog.LogMasterOp("DataPartitionDecommissionRollback",
			fmt.Sprintf("dp %v rollback reach max times", partition.decommissionInfo()), err)
		return false
	}
	return true
}

func (partition *DataPartition) markRollbackFailed(needRollback bool) {
	partition.SetDecommissionStatus(DecommissionFail)
	partition.DecommissionNeedRollbackTimes = defaultDecommissionRollbackLimit
	partition.DecommissionNeedRollback = needRollback
}

func (partition *DataPartition) removeReplicaByForce(c *Cluster, peerAddr string, enableSetRepairingStatus bool, repairingStatus bool) error {
	// del new add replica,may timeout, try rollback next time
	force := partition.DecommissionRaftForce
	// if single dp add raft member success but add a replica fails, use force to delete raft member
	// to avoid no leader
	if partition.ReplicaNum == 1 && partition.lostLeader(c) {
		force = true
	}
	log.LogInfof("action[removeReplicaByForce]dp[%v] rollback to del peer %v force %v", partition.PartitionID, peerAddr, force)
	err := c.removeDataReplica(partition, peerAddr, false, force)
	if err != nil {
		log.LogWarnf("action[removeReplicaByForce]dp[%v] rollback to del peer %v force %v failed:%v, delete peer on master"+
			"", partition.PartitionID, peerAddr, force, err)
		// to ensure hosts for master is always correct
		// redundant replica can be deleted by checkReplicaMeta
		partition.removeHostByForce(c, peerAddr)
		return err
	}
	return nil
}

func (partition *DataPartition) checkReplicaMetaEqualToMaster(replicaPeers []proto.Peer) bool {
	// Check peer length
	if len(partition.Peers) != len(replicaPeers) {
		return false
	}

	// Check nodeID and Addr for each peer
	for _, replicaPeer := range replicaPeers {
		found := false
		for _, basePeer := range partition.Peers {
			if replicaPeer.Addr == basePeer.Addr {
				if replicaPeer.ID != basePeer.ID {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func (partition *DataPartition) recoverDataReplicaMeta(replicaAddr string, c *Cluster) error {
	var (
		dataNode *DataNode
		err      error
	)
	if dataNode, err = c.dataNode(replicaAddr); err != nil {
		log.LogWarnf("action[recoverDataReplicaMeta]dp(%v)  can't find dataNode %v", partition.PartitionID, replicaAddr)
		return err
	}
	task := partition.createTaskToRecoverDataReplicaMeta(replicaAddr, partition.Peers, partition.Hosts)
	packet, err := dataNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogWarnf("action[recoverDataReplicaMeta]dp(%v), addr:%s, syncSendAdminTask to replica failed %v",
			partition.PartitionID, replicaAddr, err)
		return err
	}
	log.LogDebugf("action[recoverDataReplicaMeta]dp(%v) send packet(%v)task to recover replica %v meta success",
		partition.PartitionID, packet, replicaAddr)
	return nil
}

func (partition *DataPartition) createTaskToRecoverDataReplicaMeta(addr string, peers []proto.Peer, hosts []string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpRecoverDataReplicaMeta, addr, newRecoverDataReplicaMetaRequest(partition.PartitionID, peers, hosts))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) getReplicaDiskErrorNum() uint8 {
	partition.RLock()
	defer partition.RUnlock()
	var count uint8 = 0
	for _, replica := range partition.Replicas {
		if replica.TriggerDiskError {
			count++
		}
	}
	return count
}

func (partition *DataPartition) isReplicaAllDiskErrorExceptSrcAddr() bool {
	partition.RLock()
	defer partition.RUnlock()
	ok := true
	for _, replica := range partition.Replicas {
		if replica.Addr != partition.DecommissionSrcAddr && !replica.TriggerDiskError {
			ok = false
		}
	}
	return ok
}

func (partition *DataPartition) getDiskErrorReplica() *DataReplica {
	partition.RLock()
	defer partition.RUnlock()
	for _, replica := range partition.Replicas {
		if replica.TriggerDiskError {
			return replica
		}
	}
	return nil
}

func (partition *DataPartition) getAllDiskErrorReplica() []*DataReplica {
	partition.RLock()
	defer partition.RUnlock()
	diskErrReplicas := make([]*DataReplica, 0)
	for _, replica := range partition.Replicas {
		if replica.TriggerDiskError {
			diskErrReplicas = append(diskErrReplicas, replica)
		}
	}
	return diskErrReplicas
}

func (partition *DataPartition) checkReplicaMeta(c *Cluster) (err error) {
	var auditMsg string

	if partition.isPerformingDecommission(c) {
		log.LogDebugf("action[checkReplicaMeta]dp(%v) is performing decommission, skip it",
			partition.PartitionID)
		return proto.ErrPerformingDecommission
	}
	if !partition.needReplicaMetaRestore(c) {
		log.LogDebugf("action[checkReplicaMeta]dp(%v) do not need to restore meta",
			partition.PartitionID)
		return nil
	}
	if !partition.setRestoreReplicaRunning() {
		log.LogDebugf("action[checkReplicaMeta]dp(%v) set RestoreReplicaMetaRunning failed",
			partition.PartitionID)
		return proto.ErrPerformingRestoreReplica
	}

	err = c.syncUpdateDataPartition(partition)
	if err != nil {
		partition.setRestoreReplicaStatus(RestoreReplicaMetaStop)
		return
	}
	defer func() {
		partition.setRestoreReplicaStatus(RestoreReplicaMetaStop)
		// if update error, wait for next time, do not block decommission
		c.syncUpdateDataPartition(partition)
	}()
	log.LogDebugf("action[checkReplicaMeta]dp %v", partition.decommissionInfo())

	// for special replica, if remove old replica failed, then trigger error that has to
	// reset decommission dst during retry, updateDataNodeSize failed e.g.Then the other
	// new replica is added success and old replica is removed.
	if len(partition.Replicas) == len(partition.Hosts) && len(partition.Hosts) == len(partition.Peers) &&
		len(partition.Replicas) > int(partition.ReplicaNum) {
		if partition.GetDecommissionStatus() == DecommissionInitial {
			hostLen := len(partition.Hosts)
			removeReplica := partition.Hosts[hostLen-1]
			err = c.removeDataReplica(partition, removeReplica, false, false)
			auditMsg = fmt.Sprintf("dp(%v) remove excessive peer %v ", partition.decommissionInfo(), removeReplica)
			log.LogDebugf("action[checkReplicaMeta]%v, err %v", auditMsg, err)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			if err != nil {
				return
			}
		} else if partition.GetDecommissionStatus() == DecommissionFail {
			var removeReplica string
			if partition.DecommissionSrcAddr != "" {
				removeReplica = partition.DecommissionSrcAddr
			} else {
				hostLen := len(partition.Hosts)
				removeReplica = partition.Hosts[hostLen-1]
			}
			err = c.removeDataReplica(partition, removeReplica, false, false)
			auditMsg = fmt.Sprintf("dp(%v) remove excessive peer %v ", partition.decommissionInfo(), removeReplica)
			log.LogDebugf("action[checkReplicaMeta]%v, err %v", auditMsg, err)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			if err != nil {
				return
			}
			partition.ResetDecommissionStatus()
		}
	}

	// if len(partition.Peers) == int(partition.ReplicaNum) && len(partition.Peers) > len(partition.Replicas) {
	//	for _, peer := range partition.Peers {
	//		found := false
	//		for _, replica := range partition.Replicas {
	//			if replica.Addr == peer.Addr {
	//				found = true
	//			}
	//		}
	//		if !found {
	//			redundantPeers = append(redundantPeers, peer)
	//		}
	//	}
	//	// remove from hosts and peers only
	//	for _, peer := range redundantPeers {
	//		err = c.removeHostMember(partition, peer)
	//		auditMsg = fmt.Sprintf("dp(%v) remove unloaded peer %v for master", partition.PartitionID, peer)
	//		log.LogDebugf("action[checkReplicaMeta]%v: err %v", auditMsg, err)
	//		auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
	//		if err != nil {
	//			return
	//		}
	//	}
	// }
	// find redundant peers from replica meta
	force := false
	replicasToDelete := make([]proto.Peer, 0)
	for _, replica := range partition.Replicas {
		// new created replica, no heart beat report, skip
		if len(replica.LocalPeers) == 0 {
			continue
		}
		// do not delete new replica add by manual
		if partition.DecommissionType == ManualAddReplica {
			continue
		}
		redundantPeers := findPeersToDeleteByConfig(replica.LocalPeers, partition.Peers)
		for _, peer := range redundantPeers {
			replicasToDelete = append(replicasToDelete, peer)
			// use raftForce to delete redundant peer when dp is leaderless. This progress maybe keep executing util
			// wal logs with member change be truncated
			if partition.lostLeader(c) {
				force = true
			}
			// remove raft member
			err = partition.createTaskToRemoveRaftMember(c, peer, false, force, true)
			auditMsg = fmt.Sprintf("dp(%v) remove redundant peer %v force %v:to replica %v: LocalPeers%v",
				partition.decommissionInfo(), peer, force, replica.Addr, replica.LocalPeers)
			log.LogDebugf("action[checkReplicaMeta]%v, err %v", auditMsg, err)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			if err != nil {
				return nil
			}
		}
	}
	// redundant peer may add into replicas of master by heartbeat, during adding raft member
	// otherwise, master will delete valid peers by out date config of replica
	for _, peer := range replicasToDelete {
		partition.removeReplicaByAddr(peer.Addr)
		var dataNode *DataNode
		dataNode, err = c.dataNode(peer.Addr)
		auditMsg = fmt.Sprintf("dp(%v) cannot found datanode for replica %v to delete",
			partition.decommissionInfo(), peer.Addr)
		if err != nil {
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			return nil
		}
		err = c.deleteDataReplica(partition, dataNode, true)
		auditMsg = fmt.Sprintf("dp(%v) remove redundant replica on %v for master,by replicasToDelete ",
			partition.decommissionInfo(), peer.Addr)
		auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		if err != nil {
			return nil
		}
	}
	// find redundant peers from master
	for _, replica := range partition.Replicas {
		// new created replica, no heart beat report, skip
		if len(replica.LocalPeers) == 0 {
			continue
		}
		redundantPeers := findPeersToDeleteByConfig(partition.Peers, replica.LocalPeers)
		for _, peer := range redundantPeers {
			err = c.removeHostMember(partition, peer)
			auditMsg = fmt.Sprintf("dp(%v) remove redundant peer %v for master,base on replica %v,localPeers(%v) ",
				partition.decommissionInfo(), peer, replica.Addr, replica.LocalPeers)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			if err != nil {
				return nil
			}
			// redundant peers on master may exist on dataNode, and the redundant replica will be
			// added into partition.Replicas again by hear beat.
			var dataNode *DataNode
			dataNode, err = c.dataNode(peer.Addr)
			auditMsg = fmt.Sprintf("dp(%v) cannot found datanode for replica  %v ,base on replica %v,localPeers(%v) ",
				partition.decommissionInfo(), peer.Addr, replica.Addr, replica.LocalPeers)
			if err != nil {
				auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
				return nil
			}
			err = c.deleteDataReplica(partition, dataNode, true)
			auditMsg = fmt.Sprintf("dp(%v) remove redundant replica on %v for master,base on replica %v,localPeers(%v) ",
				partition.decommissionInfo(), peer.Addr, replica.Addr, replica.LocalPeers)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			if err != nil {
				return nil
			}
		}
	}

	// find missing replica, add new replica
	if partition.ReplicaNum > uint8(len(partition.Hosts)) {
		if partition.ReplicaNum == 1 {
			err = errors.NewErrorf("can handle 1-replica")
			auditMsg = fmt.Sprintf("dp(%v) ReplicaNum %v hostsNum %v auto add replica",
				partition.PartitionID, partition.ReplicaNum, len(partition.Hosts))
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			return
		}
		// may be one replica is unavailable
		if partition.lostLeader(c) {
			auditMsg = fmt.Sprintf("dp(%v) lost leader skip auto add replica", partition.PartitionID)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, nil)
			return
		}
		addr := partition.Hosts[0]
		var node *DataNode
		node, err = c.dataNode(addr)
		if err != nil {
			log.LogWarnf("action[checkReplicaMeta]dp(%v) cannot find node %v",
				partition.PartitionID, addr)
			return nil
		}
		err = c.markDecommissionDataPartition(partition, node, false, AutoAddReplica, highPriorityDecommissionWeight)
		auditMsg = fmt.Sprintf("dp(%v) ReplicaNum %v hostsNum %v auto add replica",
			partition.PartitionID, partition.ReplicaNum, len(partition.Hosts))
		log.LogDebugf("action[checkReplicaMeta]%v: err %v", auditMsg, err)
		auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		if err != nil {
			return nil
		} else {
			return proto.ErrWaitForAutoAddReplica
		}
	}
	return
	// find redundant replica
	// if partition.ReplicaNum < uint8(len(partition.Hosts)) {
	//	host := partition.getToBeDecommissionHost(int(partition.ReplicaNum))
	//	if host != "" {
	//		err = partition.removeOneReplicaByHost(c, host)
	//	}
	//	auditMsg = fmt.Sprintf("dp(%v) ReplicaNum %v hostsNum %v auto delete replica: %v",
	//		partition.PartitionID, partition.ReplicaNum, len(partition.Hosts), host)
	//	log.LogDebugf("action[checkReplicaMeta]%v: err %v", auditMsg, err)
	//	auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
	// }
}

func findPeersToDeleteByConfig(toCompare, basePeers []proto.Peer) []proto.Peer {
	var redundantPeers []proto.Peer
	for _, peer := range toCompare {
		found := false
		for _, base := range basePeers {
			if base.Addr == peer.Addr {
				found = true
			}
		}
		if !found {
			redundantPeers = append(redundantPeers, peer)
		}
	}
	return redundantPeers
}

func (partition *DataPartition) lostLeader(c *Cluster) bool {
	return partition.getLeaderAddr() == "" && (time.Now().Unix()-partition.LeaderReportTime > c.cfg.DpNoLeaderReportIntervalSec)
}

func (partition *DataPartition) decommissionInfo() string {
	var replicas []string
	for _, replica := range partition.Replicas {
		replicas = append(replicas, replica.Addr)
	}

	return fmt.Sprintf("vol(%v)_dp(%v)_replicaNum(%v)_src(%v)_dst(%v)_hosts(%v)_diskRetryMap(%v)_retry(%v)_isRecover(%v)_status(%v)_specialStatus(%v)"+
		"_needRollback(%v)_rollbackTimes(%v)_force(%v)_type(%v)_RestoreReplica(%v)_errMsg(%v)_discard(%v)_term(%v)_weight(%v)_firstHostDiskTokenKey(%v)_replica(%v)_recoverStartTime(%v)_addr(%p)",
		partition.VolName, partition.PartitionID, partition.ReplicaNum, partition.DecommissionSrcAddr, partition.DecommissionDstAddr,
		partition.Hosts, partition.DecommissionDiskRetryMap, partition.DecommissionRetry, partition.isRecover, GetDecommissionStatusMessage(partition.GetDecommissionStatus()),
		GetSpecialDecommissionStatusMessage(partition.GetSpecialReplicaDecommissionStep()), partition.DecommissionNeedRollback,
		partition.DecommissionNeedRollbackTimes, partition.DecommissionRaftForce, GetDecommissionTypeMessage(partition.DecommissionType),
		GetRestoreReplicaMessage(partition.RestoreReplica), partition.DecommissionErrorMessage, partition.IsDiscard,
		partition.DecommissionTerm, partition.DecommissionWeight, partition.DecommissionFirstHostDiskTokenKey, replicas, partition.RecoverStartTime.Format("2006-01-02 15:04:05"), partition)
}

func (partition *DataPartition) isPerformingDecommission(c *Cluster) bool {
	// it should be decommission initial
	if partition.DecommissionSrcAddr == "" {
		return false
	}
	// do not perform restore replica if ns not found
	dataNode, err := c.dataNode(partition.DecommissionSrcAddr)
	if err != nil {
		log.LogWarnf("action[checkReplicaMeta]dp(%v) cannot find src dataNode %v: %v",
			partition.PartitionID, partition.DecommissionSrcAddr, err)
		return false
	}
	zone, err := c.t.getZone(dataNode.ZoneName)
	if err != nil {
		log.LogWarnf("action[checkReplicaMeta]dp(%v) find zone for addr %v: %v",
			partition.PartitionID, partition.DecommissionSrcAddr, err)
		return false
	}
	ns, err := zone.getNodeSet(dataNode.NodeSetID)
	if err != nil {
		log.LogWarnf("action[checkReplicaMeta]dp(%v) find nodeset for addr %v: %v",
			partition.PartitionID, partition.DecommissionSrcAddr, err)
		return false
	}
	return ns.processDataPartitionDecommission(partition.PartitionID)
}

func (partition *DataPartition) setRestoreReplicaStatus(status uint32) {
	atomic.StoreUint32(&partition.RestoreReplica, status)
}

func (partition *DataPartition) removeHostByForce(c *Cluster, peerAddr string) {
	dataNode, err := c.dataNode(peerAddr)
	if err != nil {
		log.LogWarnf("action[removeHostByForce]dp %v find dataNode %v failed:%v",
			partition.PartitionID, peerAddr, err)
		return
	}
	removePeer := proto.Peer{ID: dataNode.ID, Addr: peerAddr, HeartbeatPort: dataNode.HeartbeatPort, ReplicaPort: dataNode.ReplicaPort}
	if err = c.removeHostMember(partition, removePeer); err != nil {
		log.LogWarnf("action[removeHostByForce]dp %v remove host %v failed:%v",
			partition.PartitionID, peerAddr, err)
		return
	}
	if err = c.deleteDataReplica(partition, dataNode, false); err != nil {
		return
	}
	// data replica would be mark expired when dataNode reboot
	leaderAddr := partition.getLeaderAddrWithLock()
	if leaderAddr != peerAddr {
		return
	}
	if dataNode, err = c.dataNode(partition.Hosts[0]); err != nil {
		log.LogWarnf("action[removeHostByForce]dp %v find dataNode %v failed:%v",
			partition.PartitionID, partition.Hosts[0], err)
		return
	}
	if err = partition.tryToChangeLeader(c, dataNode); err != nil {
		log.LogWarnf("action[removeHostByForce]dp %v tryToChangeLeader %v failed:%v",
			partition.PartitionID, partition.Hosts[0], err)
		return
	}
}

func (partition *DataPartition) resetForManualAddReplica() {
	partition.DecommissionDstAddr = ""
	partition.DecommissionType = InitialDecommission
	partition.isRecover = false
	partition.SetDecommissionStatus(DecommissionInitial)
	partition.setRestoreReplicaStop()
}

func (partition *DataPartition) setRestoreReplicaRunning() bool {
	return atomic.CompareAndSwapUint32(&partition.RestoreReplica, RestoreReplicaMetaStop, RestoreReplicaMetaRunning)
}

func (partition *DataPartition) setRestoreReplicaForbidden() bool {
	return atomic.CompareAndSwapUint32(&partition.RestoreReplica, RestoreReplicaMetaStop, RestoreReplicaMetaForbidden)
}

func (partition *DataPartition) setRestoreReplicaStop() bool {
	return atomic.CompareAndSwapUint32(&partition.RestoreReplica, RestoreReplicaMetaForbidden, RestoreReplicaMetaStop)
}

func (partition *DataPartition) tryRecoverReplicaMeta(c *Cluster, migrateType uint32) error {
	// AutoAddReplica do not need to check meta for replica again, only have to check
	// dp is performing decommission
	if migrateType == AutoAddReplica {
		return nil
	}
	waitTimes := 0
	for {
		err := partition.checkReplicaMeta(c)
		if err != nil {
			if err == proto.ErrPerformingRestoreReplica {
				waitTimes++
				if waitTimes > defaultSetRestoreReplicaStatusLimit {
					return errors.NewErrorf("set restore replica status timeout:5min")
				}
				// maybe other replica is decommissioning
				if c.processDataPartitionDecommission(partition.PartitionID) {
					return errors.NewErrorf("dp[%v] %v", partition.PartitionID, proto.ErrPerformingDecommission.Error())
				}
				log.LogDebugf("action[tryRecoverReplicaMeta]dp(%v) wait for checking replica",
					partition.PartitionID)
				time.Sleep(time.Second)
				continue
			}
			return errors.NewErrorf("restore replica meta failed:%v", err.Error())
		}
		return nil
	}
}

func (partition *DataPartition) createTaskToRecoverBackupDataPartitionReplica(addr, disk string) (task *proto.AdminTask,
) {
	task = proto.NewAdminTask(proto.OpRecoverBackupDataReplica, addr, newRecoverBackupDataPartitionReplicaRequest(
		partition.PartitionID, disk))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) needReplicaMetaRestore(c *Cluster) bool {
	partition.RLock()
	defer partition.RUnlock()
	if len(partition.Replicas) == len(partition.Hosts) && len(partition.Hosts) == len(partition.Peers) &&
		len(partition.Replicas) > int(partition.ReplicaNum) && (partition.GetDecommissionStatus() == DecommissionInitial || partition.GetDecommissionStatus() == DecommissionFail) {
		return true
	}

	for _, replica := range partition.Replicas {
		if len(replica.LocalPeers) == 0 {
			continue
		}

		if partition.DecommissionType == ManualAddReplica {
			continue
		}

		redundantPeers := findPeersToDeleteByConfig(replica.LocalPeers, partition.Peers)
		if len(redundantPeers) != 0 {
			return true
		}
	}

	for _, replica := range partition.Replicas {
		if len(replica.LocalPeers) == 0 {
			continue
		}
		redundantPeers := findPeersToDeleteByConfig(partition.Peers, replica.LocalPeers)
		if len(redundantPeers) != 0 {
			return true
		}
	}

	if partition.ReplicaNum > uint8(len(partition.Hosts)) {
		if partition.ReplicaNum == 1 {
			err := errors.NewErrorf("can handle 1-replica")
			auditMsg := fmt.Sprintf("dp(%v) ReplicaNum %v hostsNum %v auto add replica",
				partition.PartitionID, partition.ReplicaNum, len(partition.Hosts))
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
			return false
		}
		// may be one replica is unavailable
		if partition.lostLeader(c) {
			auditMsg := fmt.Sprintf("dp(%v) lost leader skip auto add replica", partition.PartitionID)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, nil)
			return false
		}
		return true
	}
	return false
}
