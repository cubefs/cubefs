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
	stringutil "github.com/cubefs/cubefs/util/string"
	"sync"

	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// MetaReplica defines the replica of a meta partition
type MetaReplica struct {
	Addr              string
	start             uint64 // lower bound of the inode id
	end               uint64 // upper bound of the inode id
	nodeID            uint64
	MaxInodeID        uint64
	InodeCount        uint64
	DentryCount       uint64
	DelInoCnt         uint64
	MaxExistIno       uint64
	ReportTime        int64
	Status            int8 // unavailable, readOnly, readWrite
	IsLeader          bool
	IsLearner         bool
	IsRecover         bool
	StoreMode         proto.StoreMode
	ApplyId           uint64
	AllocatorInUseCnt uint64
	metaNode          *MetaNode
	createTime        int64
}

// MetaPartition defines the structure of a meta partition
type MetaPartition struct {
	PartitionID          uint64
	Start                uint64
	End                  uint64
	MaxInodeID           uint64
	InodeCount           uint64
	DentryCount          uint64
	DelInodeCount        uint64
	MaxExistIno          uint64
	InoAllocatorInuseCnt uint64
	Replicas             []*MetaReplica
	ReplicaNum           uint8
	LearnerNum           uint8
	Status               int8
	IsRecover            bool
	volID                uint64
	volName              string
	Hosts                []string
	Peers                []proto.Peer
	Learners             []proto.Learner
	MissNodes            map[string]int64 `graphql:"-"`
	OfflinePeerID        uint64
	modifyTime           int64
	lastOfflineTime      int64
	PanicHosts           []string //PanicHosts records the hosts discard by reset peer action.
	LoadResponse         []*proto.MetaPartitionLoadResponse
	offlineMutex         sync.RWMutex
	sync.RWMutex
}

func newMetaReplica(start, end uint64, metaNode *MetaNode) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeID: metaNode.ID, Addr: metaNode.Addr}
	mr.metaNode = metaNode
	mr.ReportTime = time.Now().Unix()
	mr.createTime = time.Now().Unix()
	return
}

func newMetaPartition(partitionID, start, end uint64, replicaNum, learnerNum uint8, volName string, volID uint64) (mp *MetaPartition) {
	mp = &MetaPartition{PartitionID: partitionID, Start: start, End: end, volName: volName, volID: volID}
	mp.ReplicaNum = replicaNum
	mp.LearnerNum = learnerNum
	mp.Replicas = make([]*MetaReplica, 0)
	mp.Status = proto.Unavailable
	mp.MissNodes = make(map[string]int64, 0)
	mp.Peers = make([]proto.Peer, 0)
	mp.Learners = make([]proto.Learner, 0)
	mp.Hosts = make([]string, 0)
	mp.PanicHosts = make([]string, 0)
	mp.LoadResponse = make([]*proto.MetaPartitionLoadResponse, 0)
	return
}

func (mp *MetaPartition) setPeers(peers []proto.Peer) {
	mp.Peers = peers
}

func (mp *MetaPartition) setLearners(learners []proto.Learner) {
	mp.Learners = learners
}

func (mp *MetaPartition) setHosts(hosts []string) {
	mp.Hosts = hosts
}

func (mp *MetaPartition) hostsToString() (hosts string) {
	return strings.Join(mp.Hosts, underlineSeparator)
}

func (mp *MetaPartition) addReplica(mr *MetaReplica) {
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			return
		}
	}
	mp.Replicas = append(mp.Replicas, mr)
	return
}

func (mp *MetaPartition) removeReplica(mr *MetaReplica) {
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			continue
		}
		newReplicas = append(newReplicas, m)
	}
	mp.Replicas = newReplicas
	return
}

func (mp *MetaPartition) removeReplicaByAddr(addr string) {
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == addr {
			continue
		}
		newReplicas = append(newReplicas, m)
	}
	mp.Replicas = newReplicas
	return
}

func (mp *MetaPartition) updateInodeIDRangeForAllReplicas() {
	for _, mr := range mp.Replicas {
		mr.end = mp.End
	}
}

//canSplit caller must be add lock
func (mp *MetaPartition) canSplit(end uint64) (err error) {
	if end < mp.Start {
		err = fmt.Errorf("end[%v] less than mp.start[%v]", end, mp.Start)
		return
	}
	// overflow
	if end > (defaultMaxMetaPartitionInodeID - proto.DefaultMetaPartitionInodeIDStep) {
		msg := fmt.Sprintf("action[updateInodeIDRange] vol[%v] partitionID[%v] nextStart[%v] "+
			"to prevent overflow ,not update end", mp.volName, mp.PartitionID, end)
		log.LogWarn(msg)
		err = fmt.Errorf(msg)
		return
	}
	if end <= mp.MaxInodeID {
		err = fmt.Errorf("next meta partition start must be larger than %v", mp.MaxInodeID)
		return
	}
	if _, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] id[%v] no leader", mp.volName, mp.PartitionID)
		return
	}
	return
}

func (mp *MetaPartition) addUpdateMetaReplicaTask(c *Cluster) (err error) {

	tasks := make([]*proto.AdminTask, 0)
	t := mp.createTaskToUpdateMetaReplica(c.Name, mp.PartitionID, mp.End)
	//if no leader,don't update end
	if t == nil {
		err = proto.ErrNoLeader
		return
	}
	tasks = append(tasks, t)
	c.addMetaNodeTasks(tasks)
	log.LogWarnf("action[addUpdateMetaReplicaTask] partitionID[%v] end[%v] success", mp.PartitionID, mp.End)
	return
}

func (mp *MetaPartition) checkEnd(c *Cluster, maxPartitionID uint64) {

	if mp.PartitionID != maxPartitionID {
		return
	}
	vol, err := c.getVol(mp.volName)
	if err != nil {
		log.LogWarnf("action[checkEnd] vol[%v] not exist", mp.volName)
		return
	}
	vol.createMpMutex.RLock()
	defer vol.createMpMutex.RUnlock()
	curMaxPartitionID := vol.maxPartitionID()
	if mp.PartitionID != curMaxPartitionID {
		log.LogWarnf("action[checkEnd] partition[%v] not max partition[%v]", mp.PartitionID, curMaxPartitionID)
		return
	}
	mp.Lock()
	defer mp.Unlock()
	if _, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogWarnf("action[checkEnd] partition[%v] no leader", mp.PartitionID)
		return
	}
	if mp.End != defaultMaxMetaPartitionInodeID {
		oldEnd := mp.End
		mp.End = defaultMaxMetaPartitionInodeID
		if err := c.syncUpdateMetaPartition(mp); err != nil {
			mp.End = oldEnd
			log.LogErrorf("action[checkEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
			return
		}
		if err = mp.addUpdateMetaReplicaTask(c); err != nil {
			mp.End = oldEnd
		}
	}
	log.LogDebugf("action[checkEnd] partitionID[%v] end[%v]", mp.PartitionID, mp.End)
}

// hosts with lock
func (mp *MetaPartition) hosts() []string {
	mp.RLock()
	defer mp.RUnlock()
	hosts := make([]string, len(mp.Hosts))
	copy(hosts, mp.Hosts)
	return hosts
}

func (mp *MetaPartition) getMetaReplica(addr string) (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.Addr == addr {
			return
		}
	}
	return nil, metaReplicaNotFound(addr)
}

func (mp *MetaPartition) removeMissingReplica(addr string) {
	if _, ok := mp.MissNodes[addr]; ok {
		delete(mp.MissNodes, addr)
	}
}

func (mp *MetaPartition) checkLeader() {
	mp.Lock()
	defer mp.Unlock()
	for _, mr := range mp.Replicas {
		if !mr.isActive() {
			mr.IsLeader = false
		}
	}
	return
}

func (mp *MetaPartition) checkStatus(clusterID string, writeLog bool, replicaNum int, maxPartitionID uint64) (doSplit bool) {
	mp.Lock()
	defer mp.Unlock()
	liveReplicas := mp.getLiveReplicas()
	if len(liveReplicas) <= replicaNum/2 {
		mp.Status = proto.Unavailable
	} else {
		mr, err := mp.getMetaReplicaLeader()
		if err != nil {
			mp.Status = proto.Unavailable
		}
		mp.Status = mr.Status
		for _, replica := range liveReplicas {
			if replica.Status == proto.ReadOnly {
				mp.Status = proto.ReadOnly
			}
			if mr.metaNode == nil {
				continue
			}
			if !mr.metaNode.reachesThreshold() {
				continue
			}
			if mp.PartitionID == maxPartitionID {
				doSplit = true
			} else {
				mp.Status = proto.ReadOnly
			}
		}
	}

	if mp.PartitionID == maxPartitionID && mp.Status == proto.ReadOnly {
		mp.Status = proto.ReadWrite
	}
	log.LogDebugf("action[checkStatus], maxPartitionId:%v, mp id:%v, status:%v", maxPartitionID, mp.PartitionID, mp.Status)
	if writeLog && len(liveReplicas) != int(mp.ReplicaNum) {
		allLiveReplicas := mp.getAllLiveReplicas()
		inactiveAddrs := mp.getInactiveAddrsFromLiveReplicas(allLiveReplicas)
		msg := fmt.Sprintf("action[checkMPStatus],id:%v,status:%v,replicaNum:%v, replicas:%v learnerNum:%v,replicas:%v,persistenceHosts:%v inactiveAddrs:%v",
			mp.PartitionID, mp.Status, mp.ReplicaNum, len(mp.Replicas), mp.LearnerNum, len(allLiveReplicas), mp.Hosts, inactiveAddrs)
		log.LogInfo(msg)
		Warn(clusterID, msg)
	}
	return
}

func (mp *MetaPartition) getMetaReplicaLeader() (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.IsLeader && mr.isActive() {
			return
		}
	}
	err = proto.ErrNoLeader
	return
}

func (mp *MetaPartition) checkReplicaNum(c *Cluster, volName string, replicaNum uint8) {
	mp.RLock()
	defer mp.RUnlock()
	if mp.ReplicaNum != replicaNum {
		msg := fmt.Sprintf("FIX MetaPartition replicaNum clusterID[%v] vol[%v] replica num[%v],current num[%v]",
			c.Name, volName, replicaNum, mp.ReplicaNum)
		Warn(c.Name, msg)
	}
}

func (mp *MetaPartition) removeIllegalReplica() (excessAddr string, err error) {
	mp.Lock()
	defer mp.Unlock()
	for _, mr := range mp.Replicas {
		if !contains(mp.Hosts, mr.Addr) {
			excessAddr = mr.Addr
			err = proto.ErrIllegalMetaReplica
			break
		}
	}
	return
}

func (mp *MetaPartition) missingReplicaAddrs() (lackAddrs []string) {
	mp.RLock()
	defer mp.RUnlock()
	var liveReplicas []string
	for _, mr := range mp.Replicas {
		liveReplicas = append(liveReplicas, mr.Addr)
	}
	for _, host := range mp.Hosts {
		if !contains(liveReplicas, host) {
			lackAddrs = append(lackAddrs, host)
			break
		}
	}
	return
}

func (mp *MetaPartition) updateMetaPartition(mgr *proto.MetaPartitionReport, metaNode *MetaNode) {

	if !contains(mp.Hosts, metaNode.Addr) {
		return
	}
	mp.Lock()
	defer mp.Unlock()
	mr, err := mp.getMetaReplica(metaNode.Addr)
	if err != nil {
		mr = newMetaReplica(mp.Start, mp.End, metaNode)
		mp.addReplica(mr)
	}
	mr.updateMetric(mgr)
	mp.setMaxInodeID()
	mp.setInodeCount()
	mp.setDentryCount()
	mp.setDeletedInodeCount()
	mp.setMaxExistIno()
	mp.setInoAllocatorUsedCount()
	mp.removeMissingReplica(metaNode.Addr)
}

func (mp *MetaPartition) canBeOffline(nodeAddr string, replicaNum int) (err error) {
	liveReplicas := mp.getLiveReplicas()
	if len(liveReplicas) < int(mp.ReplicaNum/2+1) {
		err = proto.ErrNoEnoughReplica
		return
	}
	liveAddrs := mp.getLiveReplicasAddr(liveReplicas)
	if len(liveReplicas) == (replicaNum/2+1) && contains(liveAddrs, nodeAddr) {
		err = fmt.Errorf("live replicas num will be less than majority after offline nodeAddr: %v", nodeAddr)
		return
	}
	return
}

// Check if there is a replica missing or not.
func (mp *MetaPartition) hasMissingOneReplica(offlineAddr string, replicaNum int) (err error) {
	learnerHosts := mp.getLearnerHosts()
	curHostCount := len(mp.Hosts)
	for _, host := range mp.Hosts {
		if contains(learnerHosts, host) {
			curHostCount = curHostCount - 1
			continue
		}
		if host == offlineAddr {
			curHostCount = curHostCount - 1
		}
	}
	curReplicaCount := len(mp.Replicas)
	for _, r := range mp.Replicas {
		if r.IsLearner {
			curReplicaCount = curReplicaCount - 1
			continue
		}
		if r.Addr == offlineAddr {
			curReplicaCount = curReplicaCount - 1
		}
	}
	if curHostCount < replicaNum-1 || curReplicaCount < replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneReplica", mp.PartitionID, proto.ErrHasOneMissingReplica))
		err = proto.ErrHasOneMissingReplica
	}
	return
}

func (mp *MetaPartition) getLiveReplicasAddr(liveReplicas []*MetaReplica) (addrs []string) {
	addrs = make([]string, 0)
	for _, mr := range liveReplicas {
		addrs = append(addrs, mr.Addr)
	}
	return
}

func (mp *MetaPartition) getInactiveAddrsFromLiveReplicas(liveReplicas []*MetaReplica) (inactiveAddrs []string) {
	inactiveAddrs = make([]string, 0)
	liveReplicasAddr := mp.getLiveReplicasAddr(liveReplicas)
	for _, host := range mp.Hosts {
		if !contains(liveReplicasAddr, host) {
			inactiveAddrs = append(inactiveAddrs, host)
		}
	}
	return
}

// include norma and learner peer
func (mp *MetaPartition) getAllLiveReplicas() (allLiveReplicas []*MetaReplica) {
	allLiveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			allLiveReplicas = append(allLiveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) getLiveReplicas() (liveReplicas []*MetaReplica) {
	learnerHosts := mp.getLearnerHosts()
	liveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.IsLearner {
			continue
		}
		if contains(learnerHosts, mr.Addr) {
			continue
		}
		if mr.isActive() {
			liveReplicas = append(liveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) updateMetaPartitionReplicaEnd(c *Cluster) {
	mp.RLock()
	defer mp.RUnlock()
	mp.addUpdateMetaReplicaTask(c)
}

func (mp *MetaPartition) persistToRocksDB(action, volName string, newHosts []string, newPeers []proto.Peer, newLearners []proto.Learner, c *Cluster) (err error) {
	oldHosts := make([]string, len(mp.Hosts))
	copy(oldHosts, mp.Hosts)
	oldPeers := make([]proto.Peer, len(mp.Peers))
	copy(oldPeers, mp.Peers)
	oldLearners := make([]proto.Learner, len(mp.Learners))
	copy(oldLearners, mp.Learners)
	mp.Hosts = newHosts
	mp.Peers = newPeers
	mp.Learners = newLearners
	if err = c.syncUpdateMetaPartition(mp); err != nil {
		mp.Hosts = oldHosts
		mp.Peers = oldPeers
		mp.Learners = oldLearners
		log.LogWarnf("action[%v_persist] failed,vol[%v] partitionID:%v  old hosts:%v new hosts:%v  oldPeers:%v newPeers:%v  oldLearners:%v newLearners:%v",
			action, volName, mp.PartitionID, mp.Hosts, newHosts, mp.Peers, newPeers, mp.Learners, newLearners)
		return
	}
	log.LogWarnf("action[%v_persist] success,vol[%v] partitionID:%v  old hosts:%v  new hosts:%v oldPeers:%v  newPeers:%v  oldLearners:%v newLearners:%v",
		action, volName, mp.PartitionID, oldHosts, mp.Hosts, oldPeers, mp.Peers, oldLearners, mp.Learners)
	return
}

func (mp *MetaPartition) getActiveAddrs() (liveAddrs []string) {
	liveAddrs = make([]string, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveAddrs = append(liveAddrs, mr.Addr)
		}
	}
	return liveAddrs
}

func (mp *MetaPartition) isMissingReplica(addr string) bool {
	return !contains(mp.getActiveAddrs(), addr)
}

func (mp *MetaPartition) shouldReportMissingReplica(addr string, interval int64) (isWarn bool) {
	lastWarningTime, ok := mp.MissNodes[addr]
	if !ok {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	} else if (time.Now().Unix() - lastWarningTime) > interval {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	}
	return false
}

func (mp *MetaPartition) reportMissingReplicas(clusterID, leaderAddr string, seconds, interval int64) {
	mp.Lock()
	defer mp.Unlock()
	for _, replica := range mp.Replicas {
		// reduce the alarm frequency
		if contains(mp.Hosts, replica.Addr) && replica.isMissing() && mp.shouldReportMissingReplica(replica.Addr, interval) {
			metaNode := replica.metaNode
			var (
				lastReportTime time.Time
			)
			isActive := true
			if metaNode != nil {
				lastReportTime = metaNode.ReportTime
				isActive = metaNode.IsActive
			}
			msg := fmt.Sprintf("action[reportMissingReplicas], clusterID[%v] volName[%v] partition:%v  on Node:%v  "+
				"miss time > :%v  vlocLastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v",
				clusterID, mp.volName, mp.PartitionID, replica.Addr, seconds, replica.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
			msg = fmt.Sprintf("decommissionMetaPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, mp.PartitionID, replica.Addr)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range mp.Hosts {
		if mp.isMissingReplica(addr) && mp.shouldReportMissingReplica(addr, interval) {
			msg := fmt.Sprintf("action[reportMissingReplicas],clusterID[%v] volName[%v] partition:%v  on Node:%v  "+
				"miss time  > %v ",
				clusterID, mp.volName, mp.PartitionID, addr, defaultMetaPartitionTimeOutSec)
			Warn(clusterID, msg)
			msg = fmt.Sprintf("decommissionMetaPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, mp.PartitionID, addr)
			Warn(clusterID, msg)
		}
	}
}

func (mp *MetaPartition) replicaCreationTasks(c *Cluster, volName string) {
	var msg string
	mp.offlineMutex.Lock()
	defer mp.offlineMutex.Unlock()
	if addr, err := mp.removeIllegalReplica(); err != nil {
		msg = fmt.Sprintf("action[%v],clusterID[%v] metaPartition:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			deleteIllegalReplicaErr, c.Name, mp.PartitionID, addr, err.Error(), mp.Hosts)
		log.LogWarn(msg)
		c.deleteMetaReplica(mp, addr, true, false)
	}
	if addrs := mp.missingReplicaAddrs(); addrs != nil {
		msg = fmt.Sprintf("action[missingReplicaAddrs],clusterID[%v] metaPartition:%v  lack replication"+
			" on :%v Hosts:%v",
			c.Name, mp.PartitionID, addrs, mp.Hosts)
		Warn(c.Name, msg)
	}

	return
}

func (mp *MetaPartition) buildNewMetaPartitionTasks(specifyAddrs []string, peers []proto.Peer, volName string,
	storeMode proto.StoreMode, trashDays uint32) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	hosts := make([]string, 0)
	req := &proto.CreateMetaPartitionRequest{
		Start:        mp.Start,
		End:          mp.End,
		PartitionID:  mp.PartitionID,
		Members:      peers,
		VolName:      volName,
		Learners:     mp.Learners,
		StoreMode:    storeMode,
		TrashDays:    trashDays,
		CreationType: proto.NormalCreateMetaPartition,
	}
	if specifyAddrs == nil {
		hosts = mp.Hosts
	} else {
		hosts = specifyAddrs
	}

	for _, addr := range hosts {
		t := proto.NewAdminTask(proto.OpCreateMetaPartition, addr, req)
		resetMetaPartitionTaskID(t, mp.PartitionID)
		tasks = append(tasks, t)
	}
	return
}

func (mp *MetaPartition) tryToChangeLeader(c *Cluster, metaNode *MetaNode) (err error) {
	task, err := mp.createTaskToTryToChangeLeader(metaNode.Addr)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (mp *MetaPartition) createTaskToTryToChangeLeader(addr string) (task *proto.AdminTask, err error) {
	task = proto.NewAdminTask(proto.OpMetaPartitionTryToLeader, addr, nil)
	resetMetaPartitionTaskID(task, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToCreateReplica(host string, storeMode proto.StoreMode) (t *proto.AdminTask, err error) {
	req := &proto.CreateMetaPartitionRequest{
		Start:        mp.Start,
		End:          mp.End,
		PartitionID:  mp.PartitionID,
		Members:      mp.Peers,
		VolName:      mp.volName,
		Learners:     mp.Learners,
		StoreMode:    storeMode,
		CreationType: proto.DecommissionedCreateMetaPartition,
	}
	t = proto.NewAdminTask(proto.OpCreateMetaPartition, host, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToAddRaftMember(addPeer proto.Peer, leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.AddMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpAddMetaPartitionRaftMember, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToAddRaftLearner(addLearner proto.Learner, leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.AddMetaPartitionRaftLearnerRequest{PartitionId: mp.PartitionID, AddLearner: addLearner}
	t = proto.NewAdminTask(proto.OpAddMetaPartitionRaftLearner, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToPromoteRaftLearner(promoteLearner proto.Learner, leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.PromoteMetaPartitionRaftLearnerRequest{PartitionId: mp.PartitionID, PromoteLearner: promoteLearner}
	t = proto.NewAdminTask(proto.OpPromoteMetaPartitionRaftLearner, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToRemoveRaftMember(removePeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return nil, errors.NewError(err)
	}
	req := &proto.RemoveMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, RemovePeer: removePeer, RaftOnly: false}
	t = proto.NewAdminTask(proto.OpRemoveMetaPartitionRaftMember, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToRemoveRaftOnly(removePeer proto.Peer) (t *proto.AdminTask) {
	req := &proto.RemoveMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, RemovePeer: removePeer, RaftOnly: true}
	t = proto.NewAdminTask(proto.OpRemoveMetaPartitionRaftMember, "", req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToResetRaftMembers(newPeers []proto.Peer, address string) (t *proto.AdminTask, err error) {
	req := &proto.ResetMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, NewPeers: newPeers}
	t = proto.NewAdminTask(proto.OpResetMetaPartitionRaftMember, address, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToDecommissionReplica(volName string, removePeer proto.Peer, addPeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return nil, errors.NewError(err)
	}
	req := &proto.MetaPartitionDecommissionRequest{PartitionID: mp.PartitionID, VolName: volName, RemovePeer: removePeer, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpDecommissionMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func resetMetaPartitionTaskID(t *proto.AdminTask, partitionID uint64) {
	t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, partitionID)
	t.PartitionID = partitionID
}

func (mp *MetaPartition) createTaskToUpdateMetaReplica(clusterID string, partitionID uint64, end uint64) (t *proto.AdminTask) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		msg := fmt.Sprintf("action[createTaskToUpdateMetaReplica] clusterID[%v] meta partition %v no leader",
			clusterID, mp.PartitionID)
		Warn(clusterID, msg)
		return
	}
	req := &proto.UpdateMetaPartitionRequest{PartitionID: partitionID, End: end, VolName: mp.volName}
	t = proto.NewAdminTask(proto.OpUpdateMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mr *MetaReplica) createTaskToDeleteReplica(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaPartitionRequest{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpDeleteMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mr *MetaReplica) createTaskToLoadMetaPartition(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.MetaPartitionLoadRequest{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpLoadMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}
func (mr *MetaReplica) isMissing() (miss bool) {
	return time.Now().Unix()-mr.ReportTime > defaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) isActive() (active bool) {
	active = mr.metaNode.IsActive && time.Now().Unix()-mr.ReportTime < defaultMetaPartitionTimeOutSec
	if time.Now().Unix()-mr.createTime > defaultMetaReplicaCheckStatusSec {
		active = active && mr.Status != proto.Unavailable
	}

	if !active {
		log.LogInfof("metaNode.IsActive:%v mr.Status:%v diffReportTime:%v",
			mr.metaNode.IsActive, mr.Status, time.Now().Unix()-mr.ReportTime)
	}

	return
}

func (mr *MetaReplica) setLastReportTime() {
	mr.ReportTime = time.Now().Unix()
}

func (mr *MetaReplica) updateMetric(mgr *proto.MetaPartitionReport) {
	mr.Status = (int8)(mgr.Status)
	mr.IsLeader = mgr.IsLeader
	mr.MaxInodeID = mgr.MaxInodeID
	mr.InodeCount = mgr.InodeCnt
	mr.DentryCount = mgr.DentryCnt
	mr.DelInoCnt = mgr.DelInodeCnt
	mr.MaxExistIno = mgr.ExistMaxInodeID
	mr.IsLearner = mgr.IsLearner
	mr.IsRecover = mgr.IsRecover
	if mgr.StoreMode == 0 {
		mgr.StoreMode = proto.StoreModeMem
	}
	mr.StoreMode = mgr.StoreMode
	mr.ApplyId = mgr.ApplyId
	mr.AllocatorInUseCnt = mgr.AllocatorInUseCnt
	mr.setLastReportTime()
}

func (mp *MetaPartition) afterCreation(nodeAddr string, c *Cluster, storeMode proto.StoreMode) (err error) {
	metaNode, err := c.metaNode(nodeAddr)
	if err != nil {
		return err
	}
	mr := newMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
	mr.ReportTime = time.Now().Unix()
	mr.StoreMode = storeMode
	mr.IsRecover = true
	mp.addReplica(mr)
	mp.removeMissingReplica(mr.Addr)
	return
}

func (mp *MetaPartition) addOrReplaceLoadResponse(response *proto.MetaPartitionLoadResponse) {
	mp.Lock()
	defer mp.Unlock()
	loadResponse := make([]*proto.MetaPartitionLoadResponse, 0)
	for _, lr := range mp.LoadResponse {
		if lr.Addr == response.Addr {
			continue
		}
		loadResponse = append(loadResponse, lr)
	}
	loadResponse = append(loadResponse, response)
	mp.LoadResponse = loadResponse
}

func (mp *MetaPartition) getMinusOfMaxInodeID() (minus float64, normalReplicaCount int) {
	mp.RLock()
	defer mp.RUnlock()
	var sentry uint64
	for index, replica := range mp.Replicas {
		if replica.IsLearner {
			continue
		}
		normalReplicaCount++
		if index == 0 || sentry == 0 {
			sentry = replica.MaxInodeID
			continue
		}
		diff := math.Abs(float64(replica.MaxInodeID) - float64(sentry))
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) getNormalReplicas() (normalReplicas []*MetaReplica) {
	normalReplicas = make([]*MetaReplica, 0)
	for _, replica := range mp.Replicas {
		if replica.IsLearner {
			continue
		}
		normalReplicas = append(normalReplicas, replica)
	}
	return
}

func (mp *MetaPartition) getPercentMinusOfInodeCount() (minus float64) {
	mp.RLock()
	defer mp.RUnlock()
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.InodeCount)
			continue
		}
		diff := math.Abs(float64(replica.InodeCount) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	minus = minus / sentry
	return
}

func (mp *MetaPartition) getMinusOfInodeCount() (minus float64) {
	mp.RLock()
	defer mp.RUnlock()
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.InodeCount)
			continue
		}
		diff := math.Abs(float64(replica.InodeCount) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) getMinusOfDentryCount() (minus float64) {
	mp.RLock()
	defer mp.RUnlock()
	if len(mp.Replicas) == 0 {
		return 1
	}
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.DentryCount)
			continue
		}
		diff := math.Abs(float64(replica.DentryCount) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) getMinusOfApplyID() (minus float64) {
	mp.RLock()
	defer mp.RUnlock()
	if len(mp.LoadResponse) == 0 {
		return 1
	}
	var sentry float64
	for index, resp := range mp.LoadResponse {
		if index == 0 {
			sentry = float64(resp.ApplyID)
			continue
		}
		diff := math.Abs(float64(resp.ApplyID) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) setMaxInodeID() {
	var maxUsed uint64
	for _, r := range mp.Replicas {
		if r.MaxInodeID > maxUsed {
			maxUsed = r.MaxInodeID
		}
	}
	mp.MaxInodeID = maxUsed
}

func (mp *MetaPartition) setInodeCount() {
	var inodeCount uint64
	for _, r := range mp.Replicas {
		if r.InodeCount > inodeCount {
			inodeCount = r.InodeCount
		}
	}
	mp.InodeCount = inodeCount
}

func (mp *MetaPartition) setDentryCount() {
	var dentryCount uint64
	for _, r := range mp.Replicas {
		if r.DentryCount > dentryCount {
			dentryCount = r.DentryCount
		}
	}
	mp.DentryCount = dentryCount
}

func (mp *MetaPartition) setMaxExistIno() {
	var maxExistIno uint64
	for _, r := range mp.Replicas {
		if r.MaxExistIno > maxExistIno {
			maxExistIno = r.MaxExistIno
		}
	}
	mp.MaxExistIno = maxExistIno
}

func (mp *MetaPartition) setDeletedInodeCount() {
	var deletedInodeCount uint64
	for _, r := range mp.Replicas {
		if r.DelInoCnt > deletedInodeCount {
			deletedInodeCount = r.DelInoCnt
		}
	}
	mp.DelInodeCount = deletedInodeCount
}

func (mp *MetaPartition) setInoAllocatorUsedCount() {
	allocatorUsedCount := uint64(0)
	for _, r := range mp.Replicas {
		if r.AllocatorInUseCnt > allocatorUsedCount {
			allocatorUsedCount = r.AllocatorInUseCnt
		}
	}
	mp.InoAllocatorInuseCnt = allocatorUsedCount
}

func (mp *MetaPartition) getAllNodeSets() (nodeSets []uint64) {
	mp.RLock()
	defer mp.RUnlock()
	nodeSets = make([]uint64, 0)
	for _, mr := range mp.Replicas {
		if mr.metaNode == nil {
			continue
		}
		if !containsID(nodeSets, mr.metaNode.NodeSetID) {
			nodeSets = append(nodeSets, mr.metaNode.NodeSetID)
		}
	}
	return
}

func (mp *MetaPartition) getLiveZones(offlineAddr string) (zones []string) {
	mp.RLock()
	defer mp.RUnlock()
	for _, mr := range mp.Replicas {
		if mr.metaNode == nil {
			continue
		}
		if mr.Addr == offlineAddr {
			continue
		}
		zones = append(zones, mr.metaNode.ZoneName)
	}
	return
}

func (mp *MetaPartition) isLatestReplica(addr string) (ok bool) {
	hostsLen := len(mp.Hosts)
	if hostsLen <= 1 {
		return
	}
	latestAddr := mp.Hosts[hostsLen-1]
	return latestAddr == addr
}

func (mp *MetaPartition) isLearnerReplica(addr string) (ok bool) {
	for _, learner := range mp.Learners {
		if learner.Addr == addr {
			return true
		}
	}
	return
}

func (mp *MetaPartition) RepairZone(vol *Vol, c *Cluster) (err error) {
	var (
		zoneList             []string
		masterRegionZoneName []string
		isNeedRebalance      bool
	)
	isCrossRegionHATypeQuorumVol := IsCrossRegionHATypeQuorum(vol.CrossRegionHAType)
	if isCrossRegionHATypeQuorumVol {
		if masterRegionZoneName, _, err = c.getMasterAndSlaveRegionZoneName(vol.zoneName); err != nil {
			return
		}
	}
	mp.RLock()
	defer mp.RUnlock()
	var isValidZone bool
	if isValidZone, err = c.isValidZone(vol.zoneName); err != nil {
		return
	}
	if !isValidZone {
		log.LogDebugf("action[RepairZone], vol[%v], zoneName[%v], mpReplicaNum[%v] can not be automatically repaired", vol.Name, vol.zoneName, vol.mpReplicaNum)
		return
	}
	zoneList = strings.Split(vol.zoneName, ",")
	if isCrossRegionHATypeQuorumVol {
		zoneList = masterRegionZoneName
	}
	normalReplicas := mp.getNormalReplicas()
	if len(normalReplicas) != int(vol.mpReplicaNum) {
		log.LogDebugf("action[RepairZone], meta replica normalReplicas[%v] not equal to mpReplicaNum[%v]", len(normalReplicas), vol.mpReplicaNum)
		return
	}
	if mp.IsRecover {
		log.LogDebugf("action[RepairZone], meta partition[%v] is recovering", mp.PartitionID)
		return
	}

	var mpInRecover uint64
	mpInRecover = uint64(c.metaPartitionInRecovering())
	if int32(mpInRecover) > c.cfg.MetaPartitionsRecoverPoolSize {
		log.LogDebugf("action[repairMetaPartition] clusterID[%v]Recover pool is full, recover partition[%v], pool size[%v]", c.Name, mpInRecover, c.cfg.MetaPartitionsRecoverPoolSize)
		return
	}
	rps := mp.getLiveReplicas()
	if len(rps) < int(vol.mpReplicaNum) {
		log.LogDebugf("action[RepairZone], vol[%v], zoneName[%v], live Replicas [%v] less than mpReplicaNum[%v], can not be automatically repaired", vol.Name, vol.zoneName, len(rps), vol.mpReplicaNum)
		return
	}

	if isNeedRebalance, err = mp.needToRebalanceZone(c, zoneList, vol.CrossRegionHAType); err != nil {
		return
	}
	if !isNeedRebalance {
		return
	}

	if err = c.sendRepairMetaPartitionTask(mp, BalanceMetaZone); err != nil {
		log.LogErrorf("action[RepairZone] clusterID[%v] vol[%v] meta partition[%v] err[%v]", c.Name, vol.Name, mp.PartitionID, err)
		return
	}
	return
}

var getTargetAddressForRepairMetaZone = func(c *Cluster, nodeAddr string, mp *MetaPartition, oldHosts []string,
	excludeNodeSets []uint64, zoneName string, dstStoreMode proto.StoreMode) (oldAddr, addAddr string, err error) {
	var (
		offlineZoneName     string
		targetZoneName      string
		addrInTargetZone    string
		targetZone          *Zone
		nodesetInTargetZone *nodeSet
		targetHosts         []string
		vol                 *Vol
	)
	if vol, err = c.getVol(mp.volName); err != nil {
		return
	}
	if offlineZoneName, targetZoneName, err = mp.getOfflineAndTargetZone(c, zoneName, IsCrossRegionHATypeQuorum(vol.CrossRegionHAType)); err != nil {
		return
	}
	if offlineZoneName == "" || targetZoneName == "" {
		return
	}
	if targetZone, err = c.t.getZone(targetZoneName); err != nil {
		return
	}
	if oldAddr, err = mp.getAddressByZoneName(c, offlineZoneName); err != nil {
		return
	}
	if oldAddr == "" {
		err = fmt.Errorf("can not find address to decommission")
		return
	}
	if err = c.validateDecommissionMetaPartition(mp, oldAddr); err != nil {
		return
	}
	if addrInTargetZone, err = mp.getAddressByZoneName(c, targetZone.name); err != nil {
		return
	}
	//if there is no replica in target zone, choose random nodeset in target zone
	if addrInTargetZone == "" {
		if targetHosts, _, err = targetZone.getAvailMetaNodeHosts(nil, mp.Hosts, 1, dstStoreMode); err != nil {
			return
		}
		if len(targetHosts) == 0 {
			err = fmt.Errorf("no available space to find a target address")
			return
		}
		addAddr = targetHosts[0]
		return
	}
	var targetNode *MetaNode
	//if there is a replica in target zone, choose the same nodeset with this replica
	if targetNode, err = c.metaNode(addrInTargetZone); err != nil {
		err = fmt.Errorf("action[getTargetAddressForRepairMetaZone] partitionID[%v], addr[%v] metaNode not exist", mp.PartitionID, addrInTargetZone)
		return
	}
	if nodesetInTargetZone, err = targetZone.getNodeSet(targetNode.NodeSetID); err != nil {
		return
	}
	if targetHosts, _, err = nodesetInTargetZone.getAvailMetaNodeHosts(mp.Hosts, 1, dstStoreMode); err != nil {
		// select meta nodes from the other node set in same zone
		excludeNodeSets = append(excludeNodeSets, nodesetInTargetZone.ID)
		if targetHosts, _, err = targetZone.getAvailMetaNodeHosts(excludeNodeSets, mp.Hosts, 1, dstStoreMode); err != nil {
			return
		}
	}
	if len(targetHosts) == 0 {
		err = fmt.Errorf("no available space to find a target address")
		return
	}
	addAddr = targetHosts[0]
	log.LogInfof("action[getTargetAddressForRepairMetaZone],meta partitionID:%v,zone name:[%v],old address:[%v], new address:[%v]",
		mp.PartitionID, zoneName, oldAddr, addAddr)
	return
}

//check if the meta partition needs to rebalance zone
func (mp *MetaPartition) needToRebalanceZone(c *Cluster, zoneList []string, volCrossRegionHAType proto.CrossRegionHAType) (isNeed bool, err error) {
	var curZoneMap map[string]uint8
	var curZoneList []string
	curZoneMap = make(map[string]uint8, 0)
	curZoneList = make([]string, 0)
	if IsCrossRegionHATypeQuorum(volCrossRegionHAType) {
		if curZoneMap, err = mp.getMetaMasterRegionZoneMap(c); err != nil {
			return
		}
	} else {
		if curZoneMap, err = mp.getMetaZoneMap(c); err != nil {
			return
		}
	}
	for k := range curZoneMap {
		curZoneList = append(curZoneList, k)
	}

	log.LogInfof("action[needToRebalanceZone],meta partitionID:%v,zone name:%v,current zones[%v]",
		mp.PartitionID, zoneList, curZoneList)
	if len(curZoneMap) == len(zoneList) {
		isNeed = false
		for _, zone := range zoneList {
			if _, ok := curZoneMap[zone]; !ok {
				isNeed = true
			}
		}
		return
	}
	isNeed = true
	return
}

func (mp *MetaPartition) getOfflineAndTargetZone(c *Cluster, volZoneName string, isCrossRegionHATypeQuorumVol bool) (offlineZone, targetZone string, err error) {
	zoneList := strings.Split(volZoneName, ",")
	if isCrossRegionHATypeQuorumVol {
		if zoneList, _, err = c.getMasterAndSlaveRegionZoneName(volZoneName); err != nil {
			return
		}
	}
	switch len(zoneList) {
	case 1:
		zoneList = append(make([]string, 0), zoneList[0], zoneList[0], zoneList[0])
	case 2:
		switch mp.PartitionID % 2 {
		case 0:
			zoneList = append(make([]string, 0), zoneList[0], zoneList[0], zoneList[1])
		default:
			zoneList = append(make([]string, 0), zoneList[1], zoneList[1], zoneList[0])
		}
		log.LogInfof("action[getSourceAndTargetZone],data partitionID:%v,zone name:[%v],chosen zoneList:%v",
			mp.PartitionID, volZoneName, zoneList)
	case 3:
		log.LogInfof("action[getSourceAndTargetZone],data partitionID:%v,zone name:[%v],chosen zoneList:%v",
			mp.PartitionID, volZoneName, zoneList)
	default:
		err = fmt.Errorf("partition zone num must be 1, 2 or 3")
		return
	}
	var currentZoneList []string
	if isCrossRegionHATypeQuorumVol {
		if currentZoneList, err = mp.getMasterRegionZoneList(c); err != nil {
			return
		}
	} else {
		if currentZoneList, err = mp.getZoneList(c); err != nil {
			return
		}
	}
	intersect := stringutil.Intersect(zoneList, currentZoneList)
	projectiveToZoneList := stringutil.Projective(zoneList, intersect)
	projectiveToCurZoneList := stringutil.Projective(currentZoneList, intersect)
	log.LogInfof("Current replica zoneList:%v, volume zoneName:%v ", currentZoneList, zoneList)
	if len(projectiveToZoneList) == 0 || len(projectiveToCurZoneList) == 0 {
		err = fmt.Errorf("action[getSourceAndTargetZone], Current replica zoneList:%v is consistent with the volume zoneName:%v, do not need to balance ", currentZoneList, zoneList)
		return
	}
	offlineZone = projectiveToCurZoneList[0]
	targetZone = projectiveToZoneList[0]
	return
}

func (mp *MetaPartition) getAddressByZoneName(c *Cluster, zone string) (addr string, err error) {
	for _, host := range mp.Hosts {
		var metaNode *MetaNode
		var z *Zone
		if metaNode, err = c.metaNode(host); err != nil {
			return
		}
		if z, err = c.t.getZoneByMetaNode(metaNode); err != nil {
			return
		}
		if zone == z.name {
			addr = host
		}
	}
	return
}

func (mp *MetaPartition) getZoneList(c *Cluster) (zoneList []string, err error) {
	zoneList = make([]string, 0)
	for _, host := range mp.Hosts {
		var metaNode *MetaNode
		var zone *Zone
		if metaNode, err = c.metaNode(host); err != nil {
			return
		}
		if zone, err = c.t.getZoneByMetaNode(metaNode); err != nil {
			return
		}
		zoneList = append(zoneList, zone.name)
	}
	return
}

func (mp *MetaPartition) getMetaZoneMap(c *Cluster) (curZonesMap map[string]uint8, err error) {
	curZonesMap = make(map[string]uint8, 0)
	for _, host := range mp.Hosts {
		var metaNode *MetaNode
		var zone *Zone
		if metaNode, err = c.metaNode(host); err != nil {
			return
		}
		if zone, err = c.t.getZoneByMetaNode(metaNode); err != nil {
			return
		}
		if _, ok := curZonesMap[zone.name]; !ok {
			curZonesMap[zone.name] = 1
		} else {
			curZonesMap[zone.name] = curZonesMap[zone.name] + 1
		}
	}
	return
}

// getNewHostsWithAddedPeer is used in add new replica which is covered with partition Lock, so need not use this lock again
func (mp *MetaPartition) getNewHostsWithAddedPeer(c *Cluster, addPeerAddr string) (newHosts []string, addPeerRegionType proto.RegionType, err error) {
	newHosts = make([]string, 0, len(mp.Hosts)+1)
	addPeerRegionType, err = c.getMetaNodeRegionType(addPeerAddr)
	if err != nil {
		return
	}
	masterRegionHosts, slaveRegionHosts, err := c.getMasterAndSlaveRegionAddrsFromMetaNodeAddrs(mp.Hosts)
	if err != nil {
		return
	}

	newHosts = append(newHosts, masterRegionHosts...)
	switch addPeerRegionType {
	case proto.MasterRegion:
		newHosts = append(newHosts, addPeerAddr)
		newHosts = append(newHosts, slaveRegionHosts...)
	case proto.SlaveRegion:
		newHosts = append(newHosts, slaveRegionHosts...)
		newHosts = append(newHosts, addPeerAddr)
	default:
		err = fmt.Errorf("action[getNewHostsWithAddedPeer] addr:%v, region type:%v is wrong", addPeerAddr, addPeerRegionType)
		return
	}
	if len(newHosts) != len(mp.Hosts)+1 {
		err = fmt.Errorf("action[getNewHostsWithAddedPeer] newHosts:%v count is not equal to %d, masterRegionHosts:%v slaveRegionHosts:%v",
			newHosts, len(mp.Hosts)+1, masterRegionHosts, slaveRegionHosts)
		return
	}
	return
}

func (mp *MetaPartition) getMetaMasterRegionZoneMap(c *Cluster) (curMasterRegionZonesMap map[string]uint8, err error) {
	curMasterRegionZonesMap = make(map[string]uint8, 0)
	for _, host := range mp.Hosts {
		var metaNode *MetaNode
		var region *Region
		if metaNode, err = c.metaNode(host); err != nil {
			return
		}
		if region, err = c.getRegionOfZoneName(metaNode.ZoneName); err != nil {
			return
		}
		if region.isMasterRegion() {
			curMasterRegionZonesMap[metaNode.ZoneName]++
		}
	}
	return
}

// get master region zone list of hosts, there may be duplicate zones
func (mp *MetaPartition) getMasterRegionZoneList(c *Cluster) (masterRegionZoneList []string, err error) {
	masterRegionZoneList = make([]string, 0)
	for _, host := range mp.Hosts {
		var metaNode *MetaNode
		var region *Region
		if metaNode, err = c.metaNode(host); err != nil {
			return
		}
		if region, err = c.getRegionOfZoneName(metaNode.ZoneName); err != nil {
			return
		}
		if region.isMasterRegion() {
			masterRegionZoneList = append(masterRegionZoneList, metaNode.ZoneName)
		}
	}
	return
}

func (mp *MetaPartition) canAddReplicaForCrossRegionQuorumVol(replicaNum int) bool {
	diff, _ := mp.getMinusOfMaxInodeID()
	if len(mp.Hosts) < replicaNum && diff < defaultMinusOfMaxInodeID {
		return true
	}
	return false
}

func (mp *MetaPartition) getLearnerHosts() (learnerHosts []string) {
	learnerHosts = make([]string, 0)
	for _, learner := range mp.Learners {
		learnerHosts = append(learnerHosts, learner.Addr)
	}
	return
}

func (mp *MetaPartition) allReplicaHasRecovered() bool {
	mp.RLock()
	defer mp.RUnlock()

	for _, mpReplica := range mp.Replicas {
		if mpReplica.IsRecover {
			return false
		}
	}
	return true
}
