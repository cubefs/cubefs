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
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
)

// DataPartition represents the structure of storing the file contents.
type DataPartition struct {
	PartitionID      uint64
	PartitionType    int
	PartitionTTL     int64
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

	RdOnly                         bool
	addReplicaMutex                sync.RWMutex
	DecommissionRetry              int
	DecommissionStatus             uint32
	DecommissionSrcAddr            string
	DecommissionDstAddr            string
	DecommissionRaftForce          bool
	DecommissionSrcDiskPath        string
	DecommissionTerm               uint64
	DecommissionDstAddrSpecify     bool // if DecommissionDstAddrSpecify is true, donot rollback when add replica fail
	DecommissionNeedRollback       bool
	DecommissionNeedRollbackTimes  int
	SpecialReplicaDecommissionStop chan bool // used for stop
	SpecialReplicaDecommissionStep uint32
	IsDiscard                      bool
	VerSeq                         uint64
	RecoverStartTime               time.Time
	RecoverLastConsumeTime         time.Duration
	DecommissionWaitTimes          int
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

	now := time.Now().Unix()
	partition.modifyTime = now
	partition.createTime = now
	partition.lastWarnTime = now
	partition.SpecialReplicaDecommissionStop = make(chan bool, 1024)
	partition.DecommissionStatus = DecommissionInitial
	partition.SpecialReplicaDecommissionStep = SpecialDecommissionInitial
	partition.DecommissionDstAddrSpecify = false
	partition.LeaderReportTime = now
	return
}

func (partition *DataPartition) setReadWrite() {
	partition.Status = proto.ReadWrite
	for _, replica := range partition.Replicas {
		replica.Status = proto.ReadWrite
	}
}

func (partition *DataPartition) isSpecialReplicaCnt() bool {
	return partition.ReplicaNum == 1 || partition.ReplicaNum == 2
}

func (partition *DataPartition) isSingleReplica() bool {
	return partition.ReplicaNum == 1
}

func (partition *DataPartition) isTwoReplica() bool {
	return partition.ReplicaNum == 2
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

func (partition *DataPartition) tryToChangeLeaderByHost(ctx context.Context, host string) (err error) {
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
	if _, err = dataNode.TaskManager.syncSendAdminTask(ctx, task); err != nil {
		return
	}
	return
}

func (partition *DataPartition) tryToChangeLeader(ctx context.Context, c *Cluster, dataNode *DataNode) (err error) {
	task, err := partition.createTaskToTryToChangeLeader(dataNode.Addr)
	if err != nil {
		return
	}
	if _, err = dataNode.TaskManager.syncSendAdminTask(ctx, task); err != nil {
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

func (partition *DataPartition) createTaskToRemoveRaftMember(ctx context.Context, c *Cluster, removePeer proto.Peer, force bool) (err error) {
	span := proto.SpanFromContext(ctx)
	doWork := func(leaderAddr string) error {
		span.Infof("action[createTaskToRemoveRaftMember] vol[%v],data partition[%v] removePeer %v leaderAddr %v", partition.VolName, partition.PartitionID, removePeer, leaderAddr)
		req := newRemoveDataPartitionRaftMemberRequest(partition.PartitionID, removePeer)
		req.Force = force

		task := proto.NewAdminTask(proto.OpRemoveDataPartitionRaftMember, leaderAddr, req)
		partition.resetTaskID(task)

		leaderDataNode, err := c.dataNode(leaderAddr)
		if err != nil {
			span.Errorf("action[createTaskToRemoveRaftMember] vol[%v],data partition[%v],err[%v]", partition.VolName, partition.PartitionID, err)
			return err
		}
		if _, err = leaderDataNode.TaskManager.syncSendAdminTask(ctx, task); err != nil {
			span.Errorf("action[createTaskToRemoveRaftMember] vol[%v],data partition[%v],err[%v]", partition.VolName, partition.PartitionID, err)
			return err
		}
		return nil
	}

	leaderAddr := partition.getLeaderAddr()
	if leaderAddr == "" {
		if force {
			for _, replica := range partition.Replicas {
				if replica.Addr != removePeer.Addr {
					leaderAddr = replica.Addr
				}
				doWork(leaderAddr)
			}
		} else {
			err = proto.ErrNoLeader
			return
		}
	} else {
		return doWork(leaderAddr)
	}
	return
}

func (partition *DataPartition) createTaskToCreateDataPartition(addr string, dataPartitionSize uint64,
	peers []proto.Peer, hosts []string, createType int, partitionType int, decommissionedDisks []string) (task *proto.AdminTask,
) {
	leaderSize := 0
	if createType == proto.DecommissionedCreateDataPartition {
		leaderSize = int(partition.Replicas[0].Used)
	}

	task = proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateDataPartitionRequest(
		partition.VolName, partition.PartitionID, int(partition.ReplicaNum),
		peers, int(dataPartitionSize), leaderSize, hosts, createType,
		partitionType, decommissionedDisks, partition.VerSeq))
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
func (partition *DataPartition) hasMissingOneReplica(ctx context.Context, addr string, replicaNum int) (err error) {
	hostNum := len(partition.Replicas)

	inReplicas := false
	for _, rep := range partition.Replicas {
		if addr == rep.Addr {
			inReplicas = true
		}
	}
	span := proto.SpanFromContext(ctx)
	if hostNum <= replicaNum-1 && inReplicas {
		span.Error(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneReplica", partition.PartitionID, proto.ErrHasOneMissingReplica))
		err = proto.ErrHasOneMissingReplica
	}
	return
}

func (partition *DataPartition) canBeOffLine(ctx context.Context, offlineAddr string) (err error) {
	span := proto.SpanFromContext(ctx)
	msg := fmt.Sprintf("action[canOffLine],partitionID:%v  RocksDBHost:%v  offLine:%v ",
		partition.PartitionID, partition.Hosts, offlineAddr)
	liveReplicas := partition.liveReplicas(ctx, defaultDataPartitionTimeOutSec)
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
			proto.ErrCannotBeOffLine, len(liveReplicas), lives, int(partition.ReplicaNum/2+1))
		span.Error(msg)
		err = fmt.Errorf(msg)
		return
	}

	if len(liveReplicas) == 0 {
		msg = fmt.Sprintf(msg+" err:%v  replicaNum:%v liveReplicas is 0 ", proto.ErrCannotBeOffLine, partition.ReplicaNum)
		span.Error(msg)
		err = fmt.Errorf(msg)
		return
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
func (partition *DataPartition) removeReplicaByAddr(ctx context.Context, addr string) {
	span := proto.SpanFromContext(ctx)
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
	span.Debug(msg)
	if delIndex == -1 {
		return
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.deleteReplicaByIndex(ctx, delIndex)
	partition.modifyTime = time.Now().Unix()

	return
}

func (partition *DataPartition) deleteReplicaByIndex(ctx context.Context, index int) {
	var replicaAddrs []string
	span := proto.SpanFromContext(ctx)

	for _, replica := range partition.Replicas {
		replicaAddrs = append(replicaAddrs, replica.Addr)
	}
	msg := fmt.Sprintf("deleteReplicaByIndex dp %v  index:%v  locations :%v ", partition.PartitionID, index, replicaAddrs)
	span.Info(msg)
	replicasAfter := partition.Replicas[index+1:]
	partition.Replicas = partition.Replicas[:index]
	partition.Replicas = append(partition.Replicas, replicasAfter...)
}

func (partition *DataPartition) createLoadTasks(ctx context.Context) (tasks []*proto.AdminTask) {
	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.Hosts {
		replica, err := partition.getReplica(ctx, addr)
		if err != nil || replica.isLive(ctx, defaultDataPartitionTimeOutSec) == false {
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

func (partition *DataPartition) getReplica(ctx context.Context, addr string) (replica *DataReplica, err error) {
	for index := 0; index < len(partition.Replicas); index++ {
		replica = partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	span := proto.SpanFromContext(ctx)

	span.Errorf("action[getReplica],partitionID:%v,locations:%v,err:%v",
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
	dpr.IsDiscard = partition.IsDiscard

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

func (partition *DataPartition) checkLoadResponse(ctx context.Context, timeOutSec int64) (isResponse bool) {
	partition.RLock()
	defer partition.RUnlock()
	span := proto.SpanFromContext(ctx)

	for _, addr := range partition.Hosts {
		replica, err := partition.getReplica(ctx, addr)
		if err != nil {
			span.Infof("action[checkLoadResponse] partitionID:%v getReplica addr %v error %v", partition.PartitionID, addr, err)
			return
		}
		timePassed := time.Now().Unix() - partition.LastLoadedTime
		if replica.HasLoadResponse == false && timePassed > timeToWaitForResponse {
			msg := fmt.Sprintf("action[checkLoadResponse], partitionID:%v on node:%v no response, spent time %v s",
				partition.PartitionID, addr, timePassed)
			span.Warn(msg)
			return
		}
		if replica.isLive(ctx, timeOutSec) == false || replica.HasLoadResponse == false {
			span.Infof("action[checkLoadResponse] partitionID:%v getReplica addr %v replica.isLive(timeOutSec) %v", partition.PartitionID, addr, replica.isLive(ctx, timeOutSec))
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
func (partition *DataPartition) releaseDataPartition(ctx context.Context) {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasFromHosts(ctx, defaultDataPartitionTimeOutSec)
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

func (partition *DataPartition) checkReplicaNum(ctx context.Context, c *Cluster, vol *Vol) {
	partition.RLock()
	defer partition.RUnlock()
	span := proto.SpanFromContext(ctx)

	if int(partition.ReplicaNum) != len(partition.Hosts) {
		msg := fmt.Sprintf("FIX DataPartition replicaNum,clusterID[%v] volName[%v] partitionID:%v orgReplicaNum:%v",
			c.Name, vol.Name, partition.PartitionID, partition.ReplicaNum)
		Warn(ctx, c.Name, msg)
		if partition.isSpecialReplicaCnt() && partition.IsDecommissionFailed() { // case restart and no message left,delete the lasted replica be added
			span.Infof("action[checkReplicaNum] volume %v partition %v need to lower replica", partition.VolName, partition.PartitionID)
			vol.NeedToLowerReplica = true
			return
		}
	}

	if vol.dpReplicaNum != partition.ReplicaNum && !vol.NeedToLowerReplica {
		span.Debugf("action[checkReplicaNum] volume %v partiton %v replicanum abnornal %v %v",
			partition.VolName, partition.PartitionID, vol.dpReplicaNum, partition.ReplicaNum)
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

func (partition *DataPartition) hasHost(addr string) (ok bool) {
	for _, host := range partition.Hosts {
		if host == addr {
			ok = true
			break
		}
	}
	return
}

func (partition *DataPartition) liveReplicas(ctx context.Context, timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if replica.isLive(ctx, timeOutSec) && partition.hasHost(replica.Addr) {
			replicas = append(replicas, replica)
		}
	}

	return
}

// get all the live replicas from the persistent hosts
func (partition *DataPartition) getLiveReplicasFromHosts(ctx context.Context, timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, host := range partition.Hosts {
		replica, ok := partition.hasReplica(host)
		if !ok {
			continue
		}
		if replica.isLive(ctx, timeOutSec) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

// get all the live replicas from the persistent hosts
func (partition *DataPartition) getLiveReplicas(ctx context.Context, timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, replica := range partition.Replicas {
		if replica.isLive(ctx, timeOutSec) == true {
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

func (partition *DataPartition) loadFile(ctx context.Context, dataNode *DataNode, resp *proto.LoadDataPartitionResponse) {
	partition.Lock()
	defer partition.Unlock()
	span := proto.SpanFromContext(ctx)
	index, err := partition.getReplicaIndex(ctx, dataNode.Addr)
	if err != nil {
		msg := fmt.Sprintf("loadFile partitionID:%v  on node:%v  don't report :%v ", partition.PartitionID, dataNode.Addr, err)
		span.Warn(msg)
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
		span.Infof("updateFileInCore partition %v", partition.PartitionID)
		fc.updateFileInCore(partition.PartitionID, dpf, replica, index)
	}
	replica.HasLoadResponse = true
	replica.Used = resp.Used
}

func (partition *DataPartition) getReplicaIndex(ctx context.Context, addr string) (index int, err error) {
	for index = 0; index < len(partition.Replicas); index++ {
		replica := partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	span := proto.SpanFromContext(ctx)

	span.Errorf("action[getReplicaIndex],partitionID:%v,location:%v,err:%v",
		partition.PartitionID, addr, dataReplicaNotFound(addr))
	return -1, errors.Trace(dataReplicaNotFound(addr), "%v not found ", addr)
}

func (partition *DataPartition) update(ctx context.Context, action, volName string, newPeers []proto.Peer, newHosts []string, c *Cluster) (err error) {
	span := proto.SpanFromContext(ctx)

	if len(newHosts) == 0 {
		span.Errorf("update. action[%v] update partition[%v] vol[%v] old host[%v]", action, partition.PartitionID, volName, partition.Hosts)
		return
	}
	orgHosts := make([]string, len(partition.Hosts))
	copy(orgHosts, partition.Hosts)
	oldPeers := make([]proto.Peer, len(partition.Peers))
	copy(oldPeers, partition.Peers)
	partition.Hosts = newHosts
	partition.Peers = newPeers
	if err = c.syncUpdateDataPartition(ctx, partition); err != nil {
		partition.Hosts = orgHosts
		partition.Peers = oldPeers
		return errors.Trace(err, "action[%v] update partition[%v] vol[%v] failed", action, partition.PartitionID, volName)
	}
	msg := fmt.Sprintf("action[%v] success,vol[%v] partitionID:%v "+
		"oldHosts:%v newHosts:%v,oldPees[%v],newPeers[%v]",
		action, volName, partition.PartitionID, orgHosts, partition.Hosts, oldPeers, partition.Peers)
	span.Warnf(msg)
	return
}

func (partition *DataPartition) updateMetric(ctx context.Context, vr *proto.DataPartitionReport, dataNode *DataNode, c *Cluster) {
	if !partition.hasHost(dataNode.Addr) {
		return
	}
	partition.Lock()
	defer partition.Unlock()

	replica, err := partition.getReplica(ctx, dataNode.Addr)
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
	if replica.IsLeader {
		partition.LeaderReportTime = time.Now().Unix()
	}
	replica.NeedsToCompare = vr.NeedCompare
	replica.DecommissionRepairProgress = vr.DecommissionRepairProgress
	if replica.DiskPath != vr.DiskPath && vr.DiskPath != "" {
		oldDiskPath := replica.DiskPath
		replica.DiskPath = vr.DiskPath
		err = c.syncUpdateDataPartition(ctx, partition)
		if err != nil {
			replica.DiskPath = oldDiskPath
		}
	}
	partition.checkAndRemoveMissReplica(dataNode.Addr)

	if replica.Status == proto.ReadWrite && (partition.RdOnly || replica.dataNode.RdOnly) {
		replica.Status = int8(proto.ReadOnly)
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
}

func (partition *DataPartition) getMaxUsedSpace() uint64 {
	return partition.used
}

func (partition *DataPartition) afterCreation(ctx context.Context, nodeAddr, diskPath string, c *Cluster) (err error) {
	dataNode, err := c.dataNode(nodeAddr)
	if err != nil {
		return err
	}
	span := proto.SpanFromContext(ctx)

	replica := newDataReplica(dataNode)
	if partition.IsDecommissionRunning() {
		replica.Status = proto.Recovering
	} else {
		replica.Status = proto.Unavailable
	}
	replica.DiskPath = diskPath
	replica.ReportTime = time.Now().Unix()
	replica.Total = util.DefaultDataPartitionSize
	partition.addReplica(replica)
	partition.checkAndRemoveMissReplica(replica.Addr)
	span.Infof("action[afterCreation] dp %v add new replica %v ", partition.PartitionID, dataNode.Addr)
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

func (partition *DataPartition) activeUsedSimilar(ctx context.Context) bool {
	partition.RLock()
	defer partition.RUnlock()
	liveReplicas := partition.liveReplicas(ctx, defaultDataPartitionTimeOutSec)
	used := liveReplicas[0].Used
	minus := float64(0)

	for _, replica := range liveReplicas {
		if math.Abs(float64(replica.Used)-float64(used)) > minus {
			minus = math.Abs(float64(replica.Used) - float64(used))
		}
	}

	return minus < util.GB
}

func (partition *DataPartition) getToBeDecommissionHost(ctx context.Context, replicaNum int) (host string) {
	partition.RLock()
	defer partition.RUnlock()
	span := proto.SpanFromContext(ctx)

	// if new replica is added success when failed(rollback failed with delete new replica timeout eg)
	if partition.isSpecialReplicaCnt() &&
		partition.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddRes &&
		partition.IsDecommissionFailed() {
		span.Infof("action[getToBeDecommissionHost] get single replica partition %v need to decommission %v",
			partition.PartitionID, partition.DecommissionDstAddr)
		host = partition.DecommissionDstAddr
		return
	}
	hostLen := len(partition.Hosts)
	if hostLen <= 1 || hostLen <= replicaNum {
		return
	}
	host = partition.Hosts[hostLen-1]
	return
}

func (partition *DataPartition) removeOneReplicaByHost(ctx context.Context, c *Cluster, host string, isReplicaNormal bool) (err error) {
	if err = c.removeDataReplica(ctx, partition, host, false, false); err != nil {
		return
	}

	partition.RLock()
	defer partition.RUnlock()

	//if partition.isSpecialReplicaCnt() && isReplicaNormal {
	//	partition.SingleDecommissionStatus = 0
	//	partition.SingleDecommissionAddr = ""
	//	return
	//}
	oldReplicaNum := partition.ReplicaNum
	partition.ReplicaNum = partition.ReplicaNum - 1

	if err = c.syncUpdateDataPartition(ctx, partition); err != nil {
		partition.ReplicaNum = oldReplicaNum
	}

	return
}

func (partition *DataPartition) getNodeSets() (nodeSets []uint64) {
	partition.RLock()
	defer partition.RUnlock()
	nodeSetMap := map[uint64]struct{}{}
	for _, replica := range partition.Replicas {
		if replica.dataNode == nil {
			continue
		}
		nodeSetMap[replica.dataNode.NodeSetID] = struct{}{}
	}
	for nodeSet := range nodeSetMap {
		nodeSets = append(nodeSets, nodeSet)
	}
	return
}

func (partition *DataPartition) getZones() (zones []string) {
	partition.RLock()
	defer partition.RUnlock()
	zoneMap := map[string]struct{}{}
	for _, replica := range partition.Replicas {
		if replica.dataNode == nil {
			continue
		}
		zoneMap[replica.dataNode.ZoneName] = struct{}{}
	}
	for zone := range zoneMap {
		zones = append(zones, zone)
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

func (partition *DataPartition) buildDpInfo(ctx context.Context, c *Cluster) *proto.DataPartitionInfo {
	partition.RLock()
	defer partition.RUnlock()
	span := proto.SpanFromContext(ctx)

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
		span.Errorf("action[buildDpInfo]failed to get volume %v, err %v", partition.VolName, err)
	}

	return &proto.DataPartitionInfo{
		PartitionID:              partition.PartitionID,
		PartitionTTL:             partition.PartitionTTL,
		PartitionType:            partition.PartitionType,
		LastLoadedTime:           partition.LastLoadedTime,
		ReplicaNum:               partition.ReplicaNum,
		Status:                   partition.Status,
		Replicas:                 replicas,
		Hosts:                    partition.Hosts,
		Peers:                    partition.Peers,
		Zones:                    zones,
		NodeSets:                 nodeSets,
		MissingNodes:             partition.MissingNodes,
		VolName:                  partition.VolName,
		VolID:                    partition.VolID,
		FileInCoreMap:            fileInCoreMap,
		OfflinePeerID:            partition.OfflinePeerID,
		IsRecover:                partition.isRecover,
		FilesWithMissingReplica:  partition.FilesWithMissingReplica,
		IsDiscard:                partition.IsDiscard,
		SingleDecommissionStatus: partition.GetSpecialReplicaDecommissionStep(),
		Forbidden:                forbidden,
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
	defaultDecommissionParallelLimit      = 10
	defaultDecommissionRetryLimit         = 5
	defaultDecommissionRollbackLimit      = 3
	defaultDecommissionDiskParallelFactor = 0
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
	default:
		return "Unknown"
	}
}

func (partition *DataPartition) MarkDecommissionStatus(ctx context.Context, srcAddr, dstAddr, srcDisk string, raftForce bool, term uint64, c *Cluster) bool {
	span := proto.SpanFromContext(ctx)

	if !partition.canMarkDecommission(term) {
		span.Warnf("action[MarkDecommissionStatus] dp[%v] cannot make decommission:status[%v]",
			partition.PartitionID, partition.GetDecommissionStatus())
		return false
	}

	if partition.IsDecommissionPaused() {
		if !partition.pauseReplicaRepair(ctx, partition.DecommissionDstAddr, false, c) {
			span.Warnf("action[MarkDecommissionStatus] dp [%d] recover from stop failed", partition.PartitionID)
			return false
		}
		partition.SetDecommissionStatus(markDecommission)
		// update decommissionTerm for next time query
		partition.DecommissionTerm = term
		return true
	}
	// initial or failed restart
	partition.ResetDecommissionStatus()
	partition.SetDecommissionStatus(markDecommission)
	partition.DecommissionSrcAddr = srcAddr
	partition.DecommissionDstAddr = dstAddr
	partition.DecommissionSrcDiskPath = srcDisk
	partition.DecommissionRaftForce = raftForce
	partition.DecommissionTerm = term
	// reset special replicas decommission status
	partition.isRecover = false
	partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	if partition.DecommissionSrcDiskPath == "" {
		partition.RLock()
		replica, _ := partition.getReplica(ctx, srcAddr)
		partition.RUnlock()
		if replica != nil {
			partition.DecommissionSrcDiskPath = replica.DiskPath
		}
	}
	if dstAddr != "" {
		partition.DecommissionDstAddrSpecify = true
	}
	span.Debugf("action[MarkDecommissionStatus] dp[%v] SrcAddr %v, dstAddr %v, diskPath %v, raftForce %v term %v",
		partition.PartitionID, partition.DecommissionSrcAddr, partition.DecommissionDstAddr,
		partition.DecommissionSrcDiskPath, partition.DecommissionRaftForce, partition.DecommissionTerm)
	return true
}

func (partition *DataPartition) SetDecommissionStatus(status uint32) {
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

func (partition *DataPartition) TryToDecommission(ctx context.Context, c *Cluster) bool {
	span := proto.SpanFromContext(ctx)

	if !partition.IsMarkDecommission() {
		span.Warnf("action[TryToDecommission] failed dp[%v] status expected markDecommission[%v]",
			partition.PartitionID, atomic.LoadUint32(&partition.DecommissionStatus))
		return false
	}

	span.Debugf("action[TryToDecommission] dp[%v]", partition.PartitionID)

	return partition.Decommission(ctx, c)
}

func (partition *DataPartition) Decommission(ctx context.Context, c *Cluster) bool {
	var (
		msg        string
		err        error
		srcAddr    = partition.DecommissionSrcAddr
		targetAddr = partition.DecommissionDstAddr
	)
	span := proto.SpanFromContext(ctx)

	defer func() {
		c.syncUpdateDataPartition(ctx, partition)
	}()
	span.Infof("action[decommissionDataPartition] dp[%v] from node[%v] to node[%v], raftForce[%v] SingleDecommissionStatus[%v]",
		partition.PartitionID, srcAddr, targetAddr, partition.DecommissionRaftForce, partition.GetSpecialReplicaDecommissionStep())
	begin := time.Now()
	partition.SetDecommissionStatus(DecommissionPrepare)
	err = c.syncUpdateDataPartition(ctx, partition)
	if err != nil {
		span.Warnf("action[decommissionDataPartition] dp [%v] update to prepare failed", partition.PartitionID)
		goto errHandler
	}

	// delete if not normal data partition
	if !proto.IsNormalDp(partition.PartitionType) {
		c.vols[partition.VolName].deleteDataPartition(ctx, c, partition)
		partition.SetDecommissionStatus(DecommissionSuccess)
		span.Warnf("action[decommissionDataPartition]delete dp directly[%v]", partition.PartitionID)
		return true
	}

	if err = c.validateDecommissionDataPartition(ctx, partition, srcAddr); err != nil {
		goto errHandler
	}

	err = c.updateDataNodeSize(targetAddr, partition)
	if err != nil {
		span.Warnf("action[decommissionDataPartition] target addr can't be writable, add %s %s", targetAddr, err.Error())
		goto errHandler
	}
	defer func() {
		if err != nil {
			c.returnDataSize(ctx, targetAddr, partition)
		}
	}()
	// if single/two replica without raftforce
	if partition.isSpecialReplicaCnt() && !partition.DecommissionRaftForce {
		if partition.GetSpecialReplicaDecommissionStep() == SpecialDecommissionInitial {
			partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionEnter)
		}
		if err = c.decommissionSingleDp(ctx, partition, targetAddr, srcAddr); err != nil {
			goto errHandler
		}
	} else {
		if err = c.removeDataReplica(ctx, partition, srcAddr, false, partition.DecommissionRaftForce); err != nil {
			goto errHandler
		}
		if err = c.addDataReplica(ctx, partition, targetAddr); err != nil {
			goto errHandler
		}
		newReplica, _ := partition.getReplica(ctx, targetAddr)
		newReplica.Status = proto.Recovering // in case heartbeat response is not arrived
		partition.isRecover = true
		partition.Status = proto.ReadOnly
		partition.SetDecommissionStatus(DecommissionRunning)
		partition.RecoverStartTime = time.Now()
		c.putBadDataPartitionIDsByDiskPath(partition.DecommissionSrcDiskPath, partition.DecommissionSrcAddr, partition.PartitionID)
	}
	// only stop 3-replica,need to release token
	if partition.IsDecommissionPaused() {
		span.Infof("action[decommissionDataPartition]clusterID[%v] partitionID:%v decommission paused", c.Name, partition.PartitionID)
		if !partition.pauseReplicaRepair(ctx, partition.DecommissionDstAddr, true, c) {
			span.Warnf("action[decommissionDataPartition]clusterID[%v] partitionID:%v  paused failed", c.Name, partition.PartitionID)
		}
		return true
	} else {
		span.Infof("action[decommissionDataPartition]clusterID[%v] partitionID:%v "+
			"on node:%v offline success,newHost[%v],PersistenceHosts:[%v], SingleDecommissionStatus[%v]prepare consume[%v]seconds",
			c.Name, partition.PartitionID, srcAddr, targetAddr, partition.Hosts, partition.GetSpecialReplicaDecommissionStep(), time.Since(begin).Seconds())
		return true
	}

errHandler:
	// special replica num receive stop signal,donot reset  SingleDecommissionStatus for decommission again
	if partition.GetDecommissionStatus() == DecommissionPause {
		span.Warnf("action[decommissionDataPartition] partitionID:%v is stopped", partition.PartitionID)
		return true
	}

	partition.DecommissionRetry++
	if partition.DecommissionRetry >= defaultDecommissionRetryLimit {
		partition.SetDecommissionStatus(DecommissionFail)
	} else {
		partition.SetDecommissionStatus(markDecommission) // retry again
		partition.DecommissionWaitTimes = 0
	}

	// if need rollback, set to fail,reset DecommissionDstAddr
	if partition.DecommissionNeedRollback {
		partition.SetDecommissionStatus(DecommissionFail)
	}
	msg = fmt.Sprintf("clusterID[%v] vol[%v] partitionID[%v]  on Node:%v  "+
		"to newHost:%v Err:%v, PersistenceHosts:%v ,retry %v,status %v, isRecover %v SingleDecommissionStatus[%v]"+
		" DecommissionNeedRollback[%v]",
		c.Name, partition.VolName, partition.PartitionID, srcAddr, targetAddr, err.Error(),
		partition.Hosts, partition.DecommissionRetry, partition.GetDecommissionStatus(),
		partition.isRecover, partition.GetSpecialReplicaDecommissionStep(), partition.DecommissionNeedRollback)
	Warn(ctx, c.Name, msg)
	span.Warnf("action[decommissionDataPartition] %s", msg)
	return false
}

func (partition *DataPartition) PauseDecommission(ctx context.Context, c *Cluster) bool {
	span := proto.SpanFromContext(ctx)

	status := partition.GetDecommissionStatus()
	// support retry pause if pause failed last time
	if status == DecommissionInitial || status == DecommissionSuccess ||
		status == DecommissionFail {
		span.Warnf("action[PauseDecommission] dp[%v] cannot be stopped status[%v]", partition.PartitionID, status)
		return true
	}
	defer c.syncUpdateDataPartition(ctx, partition)
	span.Debugf("action[PauseDecommission] dp[%v] status %v set to stop ",
		partition.PartitionID, partition.GetDecommissionStatus())

	if status == markDecommission {
		partition.SetDecommissionStatus(DecommissionPause)
		return true
	}
	if partition.isSpecialReplicaCnt() {
		span.Debugf("action[PauseDecommission]special replica dp[%v] status[%v]",
			partition.PartitionID, partition.GetSpecialReplicaDecommissionStep())
		partition.SpecialReplicaDecommissionStop <- false
		// if special replica is repairing, stop the process
		if partition.GetSpecialReplicaDecommissionStep() == SpecialDecommissionWaitAddRes {
			if !partition.pauseReplicaRepair(ctx, partition.DecommissionDstAddr, true, c) {
				return false
			}
		}
	} else {
		if partition.IsDecommissionRunning() {
			if !partition.pauseReplicaRepair(ctx, partition.DecommissionDstAddr, true, c) {
				return false
			}
			span.Debugf("action[PauseDecommission] dp[%v] status [%v] send stop signal ",
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
	partition.DecommissionDstAddrSpecify = false
	partition.DecommissionNeedRollback = false
	partition.DecommissionNeedRollbackTimes = 0
	partition.SetDecommissionStatus(DecommissionInitial)
	partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	partition.DecommissionWaitTimes = 0
}

func (partition *DataPartition) rollback(ctx context.Context, c *Cluster) {
	span := proto.SpanFromContext(ctx)

	// del new add replica,may timeout, try rollback next time
	err := c.removeDataReplica(ctx, partition, partition.DecommissionDstAddr, false, false)
	if err != nil {
		// keep decommission status to failed for rollback
		span.Warnf("action[rollback]dp[%v] rollback to del replica[%v] failed:%v",
			partition.PartitionID, partition.DecommissionDstAddr, err.Error())
		return
	}
	err = partition.restoreReplicaMeta(ctx, c)
	if err != nil {
		return
	}
	// release token first
	partition.ReleaseDecommissionToken(ctx, c)
	// reset status if rollback success
	partition.DecommissionDstAddr = ""
	partition.DecommissionRetry = 0
	partition.isRecover = false
	partition.DecommissionNeedRollback = false
	partition.DecommissionWaitTimes = 0
	partition.SetDecommissionStatus(markDecommission)
	partition.SetSpecialReplicaDecommissionStep(SpecialDecommissionInitial)
	c.syncUpdateDataPartition(ctx, partition)
	span.Warnf("action[rollback]dp[%v] rollback success", partition.PartitionID)
	return
}

func (partition *DataPartition) addToDecommissionList(ctx context.Context, c *Cluster) {
	if partition.DecommissionSrcAddr == "" {
		return
	}
	span := proto.SpanFromContext(ctx)

	var (
		dataNode *DataNode
		zone     *Zone
		ns       *nodeSet
		err      error
	)
	if dataNode, err = c.dataNode(partition.DecommissionSrcAddr); err != nil {
		span.Warnf("action[addToDecommissionList]find dp[%v] src decommission dataNode [%v] failed[%v]",
			partition.PartitionID, partition.DecommissionSrcAddr, err.Error())
		return
	}

	if dataNode.ZoneName == "" {
		span.Warnf("action[addToDecommissionList]dataNode[%v] zone is nil", dataNode.Addr)
		return
	}

	if zone, err = c.t.getZone(dataNode.ZoneName); err != nil {
		span.Warnf("action[addToDecommissionList]dataNode[%v] zone is nil:%v", dataNode.Addr, err.Error())
		return
	}

	if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
		span.Warnf("action[addToDecommissionList]dataNode[%v] nodeSet is nil:%v", dataNode.Addr, err.Error())
		return
	}
	ns.AddToDecommissionDataPartitionList(ctx, partition, c)
	span.Debugf("action[addToDecommissionList]dp[%v] decommission src[%v] Disk[%v] dst[%v] status[%v] specialStep[%v],"+
		" add to  decommission list[%v] ",
		partition.PartitionID, partition.DecommissionSrcAddr, partition.DecommissionSrcDiskPath,
		partition.DecommissionDstAddr, partition.GetDecommissionStatus(), partition.GetSpecialReplicaDecommissionStep(), ns.ID)
}

func (partition *DataPartition) checkConsumeToken() bool {
	if partition.GetDecommissionStatus() == DecommissionRunning {
		return true
	}
	return false
}

// only mark stop status or initial
func (partition *DataPartition) canMarkDecommission(term uint64) bool {
	// dp may not be reset decommission status from last decommission
	if partition.DecommissionTerm != term {
		return true
	}
	status := partition.GetDecommissionStatus()
	if status == DecommissionInitial ||
		status == DecommissionPause ||
		status == DecommissionFail {
		return true
	}
	return false
}

func (partition *DataPartition) canAddToDecommissionList() bool {
	status := partition.GetDecommissionStatus()
	if status == DecommissionInitial ||
		status == DecommissionPause ||
		status == DecommissionSuccess ||
		status == DecommissionFail {
		return false
	}
	return true
}

func (partition *DataPartition) tryRollback(ctx context.Context, c *Cluster) bool {
	if !partition.needRollback(ctx, c) {
		return false
	}
	partition.DecommissionNeedRollbackTimes++
	partition.rollback(ctx, c)
	return true
}

func (partition *DataPartition) pauseReplicaRepair(ctx context.Context, replicaAddr string, stop bool, c *Cluster) bool {
	index := partition.findReplica(replicaAddr)
	span := proto.SpanFromContext(ctx)

	if index == -1 {
		span.Warnf("action[pauseReplicaRepair]dp[%v] can't find replica %v", partition.PartitionID, replicaAddr)
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
			span.Warnf("action[pauseReplicaRepair]dp[%v] can't find dataNode %v", partition.PartitionID, partition.DecommissionSrcAddr)
			continue
		}
		task := partition.createTaskToStopDataPartitionRepair(replicaAddr, stop)
		packet, err := dataNode.TaskManager.syncSendAdminTask(ctx, task)
		if err != nil {
			retry++
			time.Sleep(time.Second)
			span.Warnf("action[pauseReplicaRepair]dp[%v] send stop task failed %v", partition.PartitionID, err.Error())
			continue
		}
		if !stop {
			partition.RecoverStartTime = time.Now().Add(-partition.RecoverLastConsumeTime)
			partition.RecoverLastConsumeTime = time.Duration(0)
			span.Debugf("action[pauseReplicaRepair]dp[%v] replica %v RecoverStartTime sub %v seconds",
				partition.PartitionID, replicaAddr, partition.RecoverLastConsumeTime.Seconds())
		} else {
			partition.RecoverLastConsumeTime = time.Now().Sub(partition.RecoverStartTime)
			span.Debugf("action[pauseReplicaRepair]dp[%v] replica %v already recover %v seconds",
				partition.PartitionID, replicaAddr, partition.RecoverLastConsumeTime.Seconds())
		}
		span.Debugf("action[pauseReplicaRepair]dp[%v] send stop to  replica %v packet %v", partition.PartitionID, replicaAddr, packet)
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

func (partition *DataPartition) TryAcquireDecommissionToken(ctx context.Context, c *Cluster) bool {
	var (
		zone            *Zone
		ns              *nodeSet
		err             error
		targetHosts     []string
		excludeNodeSets []uint64
		zones           []string
	)
	span := proto.SpanFromContext(ctx)

	const MaxRetryDecommissionWait = 60
	defer c.syncUpdateDataPartition(ctx, partition)

	if partition.DecommissionRetry > 0 {
		partition.DecommissionWaitTimes++
		if partition.DecommissionWaitTimes < MaxRetryDecommissionWait {
			// log.Debugf("action[TryAcquireDecommissionToken] dp %v wait %v", partition.PartitionID, partition.DecommissionWaitTimes)
			return false
		} else {
			partition.DecommissionWaitTimes = 0
		}
	}

	// the first time for dst addr not specify
	if !partition.DecommissionDstAddrSpecify && partition.DecommissionDstAddr == "" {
		// try to find available data node in src nodeset
		ns, zone, err = getTargetNodeset(ctx, partition.DecommissionSrcAddr, c)
		if err != nil {
			span.Warnf("action[TryAcquireDecommissionToken] dp %v find src nodeset failed:%v",
				partition.PartitionID, err.Error())
			goto errHandler
		}
		targetHosts, _, err = ns.getAvailDataNodeHosts(ctx, partition.Hosts, 1)
		if err != nil {
			span.Warnf("action[TryAcquireDecommissionToken] dp %v choose from src nodeset failed:%v",
				partition.PartitionID, err.Error())
			if _, ok := c.vols[partition.VolName]; !ok {
				span.Warnf("action[TryAcquireDecommissionToken] dp %v cannot find vol:%v",
					partition.PartitionID, err.Error())
				goto errHandler
			}

			if c.isFaultDomain(ctx, c.vols[partition.VolName]) {
				span.Warnf("action[TryAcquireDecommissionToken] dp %v is fault domain",
					partition.PartitionID)
				goto errHandler
			}
			excludeNodeSets = append(excludeNodeSets, ns.ID)
			if targetHosts, _, err = zone.getAvailNodeHosts(ctx, TypeDataPartition, excludeNodeSets, partition.Hosts, 1); err != nil {
				// select data nodes from the other zone
				zones = partition.getLiveZones(partition.DecommissionSrcAddr)
				var excludeZone []string
				if len(zones) == 0 {
					excludeZone = append(excludeZone, zone.name)
				} else {
					excludeZone = append(excludeZone, zones[0])
				}
				if targetHosts, _, err = c.getHostFromNormalZone(ctx, TypeDataPartition, excludeZone, excludeNodeSets, partition.Hosts, 1, 1, ""); err != nil {
					span.Warnf("action[TryAcquireDecommissionToken] dp %v getHostFromNormalZone failed:%v",
						partition.PartitionID, err.Error())
					goto errHandler
				}
			}
			// get nodeset for target host
			newAddr := targetHosts[0]
			ns, zone, err = getTargetNodeset(ctx, newAddr, c)
			if err != nil {
				span.Warnf("action[TryAcquireDecommissionToken] dp %v find new nodeset failed:%v",
					partition.PartitionID, err.Error())
				goto errHandler
			}
		}
		// only persist DecommissionDstAddr when get token
		if ns.AcquireDecommissionToken(partition.PartitionID) {
			partition.DecommissionDstAddr = targetHosts[0]
			span.Debugf("action[TryAcquireDecommissionToken] dp %v get token from %v nodeset %v success",
				partition.PartitionID, partition.DecommissionDstAddr, ns.ID)
			return true
		} else {
			span.Debugf("action[TryAcquireDecommissionToken] dp %v: nodeset %v token is empty",
				partition.PartitionID, ns.ID)
			return false
		}
	} else {
		ns, zone, err = getTargetNodeset(ctx, partition.DecommissionDstAddr, c)
		if err != nil {
			span.Warnf("action[TryAcquireDecommissionToken]dp %v find src nodeset failed:%v",
				partition.PartitionID, err.Error())
			goto errHandler
		}
		if ns.AcquireDecommissionToken(partition.PartitionID) {
			span.Debugf("action[TryAcquireDecommissionToken]dp %v get token from %v nodeset %v success",
				partition.PartitionID, partition.DecommissionDstAddr, ns.ID)
			return true
		} else {
			return false
		}
	}
errHandler:
	partition.DecommissionRetry++
	if partition.DecommissionRetry >= defaultDecommissionRetryLimit {
		partition.SetDecommissionStatus(DecommissionFail)
	} else {
		partition.DecommissionWaitTimes = 0
	}
	span.Warnf("action[TryAcquireDecommissionToken] clusterID[%v] vol[%v] partitionID[%v]"+
		" retry [%v] status [%v] DecommissionDstAddrSpecify [%v] DecommissionDstAddr [%v] failed",
		c.Name, partition.VolName, partition.PartitionID, partition.DecommissionRetry, partition.GetDecommissionStatus(),
		partition.DecommissionDstAddrSpecify, partition.DecommissionDstAddr)
	return false
}

func (partition *DataPartition) ReleaseDecommissionToken(ctx context.Context, c *Cluster) {
	if partition.DecommissionDstAddr == "" {
		return
	}
	span := proto.SpanFromContext(ctx)

	if ns, _, err := getTargetNodeset(ctx, partition.DecommissionDstAddr, c); err != nil {
		span.Warnf("action[ReleaseDecommissionToken]should never happen dp %v:%v", partition.PartitionID, err.Error())
		return
	} else {
		ns.ReleaseDecommissionToken(partition.PartitionID)
	}
}

//func (partition *DataPartition) ShouldReleaseDecommissionTokenByStop(c *Cluster) {
//	if partition.DecommissionDstAddr == "" && !partition.DecommissionDstAddrSpecify {
//		return
//	}
//	index := partition.findReplica(partition.DecommissionDstAddr)
//	if index == -1 {
//		span.Warnf("action[ShouldReleaseDecommissionTokenByStop]dp[%v] has not added replica %v",
//			partition.PartitionID, partition.DecommissionDstAddr)
//	}
//	partition.ReleaseDecommissionToken(c)
//}

func (partition *DataPartition) restoreReplicaMeta(ctx context.Context, c *Cluster) (err error) {
	//dst has
	//dstDataNode, err := c.dataNode(partition.DecommissionDstAddr)
	//if err != nil {
	//	span.Warnf("action[restoreReplicaMeta]partition %v find dst %v data node failed:%v",
	//		partition.PartitionID, partition.DecommissionDstAddr, err.Error())
	//	return
	//}
	//removePeer := proto.Peer{ID: dstDataNode.ID, Addr: partition.DecommissionDstAddr}
	//if err = c.removeHostMember(partition, removePeer); err != nil {
	//	span.Warnf("action[restoreReplicaMeta]partition %v metadata  removeReplica %v failed:%v",
	//		partition.PartitionID, partition.DecommissionDstAddr, err.Error())
	//	return
	//}
	span := proto.SpanFromContext(ctx)

	srcDataNode, err := c.dataNode(partition.DecommissionSrcAddr)
	if err != nil {
		span.Warnf("action[restoreReplicaMeta]partition %v find src %v data node failed:%v",
			partition.PartitionID, partition.DecommissionSrcAddr, err.Error())
		return
	}
	addPeer := proto.Peer{ID: srcDataNode.ID, Addr: partition.DecommissionSrcAddr}
	if err = c.addDataPartitionRaftMember(ctx, partition, addPeer); err != nil {
		span.Warnf("action[restoreReplicaMeta]partition %v metadata addReplica %v failed:%v",
			partition.PartitionID, partition.DecommissionSrcAddr, err.Error())
		return
	}
	span.Debugf("action[restoreReplicaMeta]partition %v meta data has restored:hosts [%v] peers[%v]",
		partition.PartitionID, partition.Hosts, partition.Peers)
	return
}

func getTargetNodeset(ctx context.Context, addr string, c *Cluster) (ns *nodeSet, zone *Zone, err error) {
	span := proto.SpanFromContext(ctx)

	var dataNode *DataNode
	dataNode, err = c.dataNode(addr)
	if err != nil {
		span.Warnf("action[getTargetNodeset] find src %v data node failed:%v", addr, err.Error())
		return nil, nil, err
	}
	zone, err = c.t.getZone(dataNode.ZoneName)
	if err != nil {
		span.Warnf("action[getTargetNodeset] find src %v zone failed:%v", addr, err.Error())
		return nil, nil, err
	}
	ns, err = zone.getNodeSet(dataNode.NodeSetID)
	if err != nil {
		span.Warnf("action[getTargetNodeset] find src %v nodeset failed:%v", addr, err.Error())
		return nil, nil, err
	}
	return ns, zone, nil
}

func (partition *DataPartition) needRollback(ctx context.Context, c *Cluster) bool {
	span := proto.SpanFromContext(ctx)

	span.Debugf("action[needRollback]dp[%v]DecommissionNeedRollbackTimes[%v]", partition.PartitionID, partition.DecommissionNeedRollbackTimes)
	// failed by error except add replica or create dp or repair dp
	if !partition.DecommissionNeedRollback {
		return false
	}
	// specify dst addr do not need rollback
	if partition.DecommissionDstAddrSpecify {
		span.Warnf("action[needRollback]dp[%v] do not rollback for DecommissionDstAddrSpecify", partition.PartitionID)
		return false
	}
	if partition.DecommissionNeedRollbackTimes >= defaultDecommissionRollbackLimit {
		span.Debugf("action[needRollback]try add restore replica, dp[%v]DecommissionNeedRollbackTimes[%v]",
			partition.PartitionID, partition.DecommissionNeedRollbackTimes)
		partition.DecommissionNeedRollback = false
		err := c.addDataReplica(ctx, partition, partition.DecommissionSrcAddr)
		if err != nil {
			span.Warnf("action[needRollback]dp[%v] recover decommission src replica %v failed: %v",
				partition.PartitionID, partition.DecommissionSrcAddr, err)
		}
		err = c.removeDataReplica(ctx, partition, partition.DecommissionDstAddr, false, false)
		if err != nil {
			span.Warnf("action[needRollback]dp[%v] remove decommission dst replica %v failed: %v",
				partition.PartitionID, partition.DecommissionDstAddr, err)
		}
		return false
	}
	return true
}

func (partition *DataPartition) restoreReplica(ctx context.Context, c *Cluster) {
	var err error
	span := proto.SpanFromContext(ctx)

	err = c.removeDataReplica(ctx, partition, partition.DecommissionDstAddr, false, false)
	if err != nil {
		span.Warnf("action[restoreReplica]dp[%v] rollback to del replica[%v] failed:%v",
			partition.PartitionID, partition.DecommissionDstAddr, err.Error())
	} else {
		span.Debugf("action[restoreReplica]dp[%v] rollback to del replica[%v] success",
			partition.PartitionID, partition.DecommissionDstAddr)
	}

	err = c.addDataReplica(ctx, partition, partition.DecommissionSrcAddr)
	if err != nil {
		span.Warnf("action[restoreReplica]dp[%v] recover decommission src replica failed", partition.PartitionID)
	} else {
		span.Debugf("action[restoreReplica]dp[%v] rollback to add replica[%v] success",
			partition.PartitionID, partition.DecommissionSrcAddr)
	}
}
