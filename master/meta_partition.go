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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// MetaReplica defines the replica of a meta partition
type MetaReplica struct {
	Addr                      string
	start                     uint64 // lower bound of the inode id
	end                       uint64 // upper bound of the inode id
	dataSize                  uint64
	nodeID                    uint64
	MaxInodeID                uint64
	InodeCount                uint64
	DentryCount               uint64
	TxCnt                     uint64
	TxRbInoCnt                uint64
	TxRbDenCnt                uint64
	FreeListLen               uint64
	ReportTime                int64
	Status                    int8 // unavailable, readOnly, readWrite
	IsLeader                  bool
	ForbidWriteOpOfProtoVer0  bool
	StatByStorageClass        []*proto.StatOfStorageClass
	StatByMigrateStorageClass []*proto.StatOfStorageClass
	metaNode                  *MetaNode
}

// MetaPartition defines the structure of a meta partition
type MetaPartition struct {
	PartitionID               uint64
	Start                     uint64
	End                       uint64
	MaxInodeID                uint64
	InodeCount                uint64
	DentryCount               uint64
	FreeListLen               uint64
	TxCnt                     uint64
	TxRbInoCnt                uint64
	TxRbDenCnt                uint64
	Replicas                  []*MetaReplica
	LeaderReportTime          int64
	ReplicaNum                uint8
	Status                    int8
	IsRecover                 bool
	IsFreeze                  bool
	volID                     uint64
	volName                   string
	Hosts                     []string
	Peers                     []proto.Peer
	OfflinePeerID             uint64
	MissNodes                 map[string]int64
	LoadResponse              []*proto.MetaPartitionLoadResponse
	offlineMutex              sync.RWMutex
	uidInfo                   []*proto.UidReportSpaceInfo
	EqualCheckPass            bool
	VerSeq                    uint64
	heartBeatDone             bool
	ForbidWriteOpOfProtoVer0  bool
	StatByStorageClass        []*proto.StatOfStorageClass
	StatByMigrateStorageClass []*proto.StatOfStorageClass
	sync.RWMutex
}

func newMetaReplica(start, end uint64, metaNode *MetaNode) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeID: metaNode.ID, Addr: metaNode.Addr}
	mr.metaNode = metaNode
	mr.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
	mr.StatByMigrateStorageClass = make([]*proto.StatOfStorageClass, 0)
	mr.ReportTime = time.Now().Unix()
	return
}

func newMetaPartition(partitionID, start, end uint64, replicaNum uint8, volName string, volID uint64, verSeq uint64) (mp *MetaPartition) {
	mp = &MetaPartition{PartitionID: partitionID, Start: start, End: end, volName: volName, volID: volID}
	mp.ReplicaNum = replicaNum
	mp.Replicas = make([]*MetaReplica, 0)
	mp.LeaderReportTime = time.Now().Unix()
	mp.Status = proto.Unavailable
	mp.MissNodes = make(map[string]int64)
	mp.Peers = make([]proto.Peer, 0)
	mp.Hosts = make([]string, 0)
	mp.VerSeq = verSeq
	mp.LoadResponse = make([]*proto.MetaPartitionLoadResponse, 0)
	mp.EqualCheckPass = true
	mp.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
	return
}

func (mp *MetaPartition) setPeers(peers []proto.Peer) {
	mp.Peers = peers
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
}

func (mp *MetaPartition) updateInodeIDRangeForAllReplicas() {
	for _, mr := range mp.Replicas {
		mr.end = mp.End
	}
}

// canSplit caller must be add lock
func (mp *MetaPartition) canSplit(end uint64, metaPartitionInodeIdStep uint64, ignoreNoLeader bool) (err error) {
	if end < mp.Start {
		err = fmt.Errorf("end[%v] less than mp.start[%v]", end, mp.Start)
		return
	}
	// overflow
	if end > (defaultMaxMetaPartitionInodeID - metaPartitionInodeIdStep) {
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

	if ignoreNoLeader {
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
	// if no leader,don't update end
	if t == nil {
		err = proto.ErrNoLeader
		return
	}
	tasks = append(tasks, t)
	c.addMetaNodeTasks(tasks)
	log.LogWarnf("action[addUpdateMetaReplicaTask] partitionID[%v] end[%v] success", mp.PartitionID, mp.End)
	return
}

func (mp *MetaPartition) dataSize() uint64 {
	maxSize := uint64(0)
	for _, mr := range mp.Replicas {
		if maxSize < mr.dataSize {
			maxSize = mr.dataSize
		}
	}

	return maxSize
}

func (mp *MetaPartition) checkEnd(c *Cluster, maxPartitionID uint64) {
	if mp.PartitionID < maxPartitionID {
		return
	}
	vol, err := c.getVol(mp.volName)
	if err != nil {
		log.LogWarnf("action[checkEnd] vol[%v] not exist", mp.volName)
		return
	}

	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()

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

func (mp *MetaPartition) getMetaReplica(addr string) (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.Addr == addr {
			return
		}
	}
	return nil, metaReplicaNotFound(addr)
}

func (mp *MetaPartition) removeMissingReplica(addr string) {
	delete(mp.MissNodes, addr)
}

func (mp *MetaPartition) isLeaderExist() bool {
	mp.RLock()
	defer mp.RUnlock()
	for _, mr := range mp.Replicas {
		if mr.IsLeader {
			return true
		}
	}
	return false
}

func (mp *MetaPartition) checkLeader(clusterID string) {
	mp.Lock()
	defer mp.Unlock()
	for _, mr := range mp.Replicas {
		if !mr.isActive() {
			mr.IsLeader = false
		}
	}

	var report bool
	if _, err := mp.getMetaReplicaLeader(); err != nil {
		report = true
	}
	if WarnMetrics != nil {
		WarnMetrics.WarnMpNoLeader(clusterID, mp.PartitionID, mp.ReplicaNum, report)
	}
}

func (mp *MetaPartition) checkStatus(clusterID string, writeLog bool, replicaNum int, maxPartitionID uint64, metaPartitionInodeIdStep uint64, forbiddenVol bool) (doSplit bool) {
	if mp.IsFreeze {
		return
	}

	mp.Lock()
	defer mp.Unlock()

	mp.checkReplicas()
	liveReplicas := mp.getLiveReplicas()

	if len(liveReplicas) <= replicaNum/2 {
		mp.Status = proto.Unavailable
	} else {
		mr, err := mp.getMetaReplicaLeader()
		if err != nil {
			mp.Status = proto.Unavailable
			log.LogErrorf("[checkStatus] mp %v getMetaReplicaLeader err:%v", mp.PartitionID, err)
		}
		if mr.Status == proto.Unavailable || !forbiddenVol {
			mp.Status = mr.Status
		} else {
			mp.Status = proto.ReadOnly
		}

		for _, replica := range liveReplicas {
			if replica.Status == proto.ReadOnly {
				mp.Status = proto.ReadOnly
			}

			if mr.metaNode == nil {
				continue
			}

			if !mr.metaNode.reachesThreshold() && mp.InodeCount < metaPartitionInodeIdStep {
				continue
			}

			if mp.PartitionID == maxPartitionID {
				log.LogInfof("split[checkStatus] need split,id:%v,status:%v,replicaNum:%v,InodeCount:%v", mp.PartitionID, mp.Status, mp.ReplicaNum, mp.InodeCount)
				doSplit = true
			} else {
				if mr.metaNode.reachesThreshold() || mp.End-mp.MaxInodeID > 2*metaPartitionInodeIdStep {
					log.LogInfof("split[checkStatus],change state,id:%v,status:%v,replicaNum:%v,replicas:%v,persistenceHosts:%v, inodeCount:%v, MaxInodeID:%v, start:%v, end:%v",
						mp.PartitionID, mp.Status, mp.ReplicaNum, len(liveReplicas), mp.Hosts, mp.InodeCount, mp.MaxInodeID, mp.Start, mp.End)
					mp.Status = proto.ReadOnly
				}
			}
		}
	}

	if mp.PartitionID >= maxPartitionID && mp.Status == proto.ReadOnly && !forbiddenVol {
		mp.Status = proto.ReadWrite
	}

	if writeLog && len(liveReplicas) != int(mp.ReplicaNum) {
		msg := fmt.Sprintf("action[checkMPStatus],id:%v,status:%v,replicaNum:%v,replicas:%v,persistenceHosts:%v",
			mp.PartitionID, mp.Status, mp.ReplicaNum, len(liveReplicas), mp.Hosts)
		log.LogInfo(msg)
		Warn(clusterID, msg)
	}

	return
}

func (mp *MetaPartition) getMetaReplicaLeader() (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.IsLeader {
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

func (mp *MetaPartition) removeIllegalReplica() (excessAddr string, t *proto.AdminTask, err error) {
	mp.RLock()
	defer mp.RUnlock()
	for _, mr := range mp.Replicas {
		if !contains(mp.Hosts, mr.Addr) {
			t = mr.createTaskToDeleteReplica(mp.PartitionID)
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

func (mp *MetaPartition) updateMetaPartition(mgr *proto.MetaPartitionReport, metaNode *MetaNode, c *Cluster) {
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
	if mr.IsLeader {
		mp.LeaderReportTime = time.Now().Unix()
	}
	mp.setMaxInodeID()
	mp.setInodeCount()
	mp.setDentryCount()
	mp.setFreeListLen()
	mp.SetTxCnt()
	mp.removeMissingReplica(metaNode.Addr)
	mp.setUidInfo(mgr)
	mp.setStatByStorageClass()
	mp.setHeartBeatDone()
	mp.SetForbidWriteOpOfProtoVer0()

	if c.RaftPartitionCanUsingDifferentPortEnabled() {
		// update old partition peers, add raft ports
		localPeers := map[string]proto.Peer{}
		for _, peer := range mgr.LocalPeers {
			if len(peer.ReplicaPort) == 0 || len(peer.HeartbeatPort) == 0 {
				peer.ReplicaPort = metaNode.ReplicaPort
				peer.HeartbeatPort = metaNode.HeartbeatPort
			}
			localPeers[peer.Addr] = peer
		}
		needUpdate := false
		for i, peer := range mp.Peers {
			if len(peer.ReplicaPort) == 0 || len(peer.HeartbeatPort) == 0 {
				if localPeer, exist := localPeers[peer.Addr]; exist {
					mp.Peers[i].ReplicaPort = localPeer.ReplicaPort
					mp.Peers[i].HeartbeatPort = localPeer.HeartbeatPort
					needUpdate = true
				}
			}
		}
		if needUpdate {
			c.syncUpdateMetaPartition(mp)
		}
	}
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

// Check if there is a replica missing or not, exclude addr
func (mp *MetaPartition) hasMissingOneReplica(addr string, replicaNum int) (err error) {
	inReplicas := false
	for _, rep := range mp.Replicas {
		if rep.Addr == addr {
			inReplicas = true
			break
		}
	}

	hostNum := len(mp.Replicas)
	if hostNum <= replicaNum-1 && inReplicas {
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

func (mp *MetaPartition) getLiveReplicas() (liveReplicas []*MetaReplica) {
	liveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveReplicas = append(liveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) checkReplicas() {
	for _, mr := range mp.Replicas {
		if !mr.isActive() {
			mr.Status = proto.Unavailable
			mr.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
			mr.StatByMigrateStorageClass = make([]*proto.StatOfStorageClass, 0)
		}
	}
}

func (mp *MetaPartition) persistToRocksDB(action, volName string, newHosts []string, newPeers []proto.Peer, c *Cluster) (err error) {
	oldHosts := make([]string, len(mp.Hosts))
	copy(oldHosts, mp.Hosts)
	oldPeers := make([]proto.Peer, len(mp.Peers))
	copy(oldPeers, mp.Peers)
	mp.Hosts = newHosts
	mp.Peers = newPeers
	if err = c.syncUpdateMetaPartition(mp); err != nil {
		mp.Hosts = oldHosts
		mp.Peers = oldPeers
		log.LogWarnf("action[%v_persist] failed,vol[%v] partitionID:%v  old hosts:%v new hosts:%v oldPeers:%v  newPeers:%v",
			action, volName, mp.PartitionID, mp.Hosts, newHosts, mp.Peers, newPeers)
		return
	}
	log.LogWarnf("action[%v_persist] success,vol[%v] partitionID:%v  old hosts:%v  new hosts:%v oldPeers:%v  newPeers:%v ",
		action, volName, mp.PartitionID, oldHosts, mp.Hosts, oldPeers, mp.Peers)
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
	return isWarn
	// return false
}

func (mp *MetaPartition) reportMissingReplicas(clusterID, leaderAddr string, seconds, interval int64) {
	mp.Lock()
	defer mp.Unlock()
	for _, replica := range mp.Replicas {
		// reduce the alarm frequency
		if contains(mp.Hosts, replica.Addr) && replica.isMissing() {
			if mp.shouldReportMissingReplica(replica.Addr, interval) {
				metaNode := replica.metaNode
				var lastReportTime time.Time
				isActive := true
				if metaNode != nil {
					lastReportTime = metaNode.ReportTime
					isActive = metaNode.IsActive
				}
				msg := fmt.Sprintf("action[reportMissingReplicas], clusterID[%v] volName[%v] partition:%v  on node:%v  "+
					"miss time > :%v  vlocLastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v",
					clusterID, mp.volName, mp.PartitionID, replica.Addr, seconds, replica.ReportTime, lastReportTime, isActive)
				Warn(clusterID, msg)
				if WarnMetrics != nil {
					WarnMetrics.WarnMissingMp(clusterID, replica.Addr, mp.PartitionID, true)
				}
			}
		} else {
			if WarnMetrics != nil {
				WarnMetrics.WarnMissingMp(clusterID, replica.Addr, mp.PartitionID, false)
			}
		}
	}
	if WarnMetrics != nil {
		WarnMetrics.CleanObsoleteMpMissing(clusterID, mp)
	}
	for _, addr := range mp.Hosts {
		if mp.isMissingReplica(addr) && mp.shouldReportMissingReplica(addr, interval) {
			msg := fmt.Sprintf("action[reportMissingReplicas],clusterID[%v] volName[%v] partition:%v  on node:%v  "+
				"miss time  > %v ",
				clusterID, mp.volName, mp.PartitionID, addr, defaultMetaPartitionTimeOutSec)
			Warn(clusterID, msg)
			msg = fmt.Sprintf("decommissionMetaPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, mp.PartitionID, addr)
			Warn(clusterID, msg)
		}
	}
}

func (mp *MetaPartition) replicaCreationTasks(clusterID, volName string) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if addr, _, err := mp.removeIllegalReplica(); err != nil {
		msg = fmt.Sprintf("action[%v],clusterID[%v] metaPartition:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			deleteIllegalReplicaErr, clusterID, mp.PartitionID, addr, err.Error(), mp.Hosts)
		log.LogWarn(msg)
	}
	if addrs := mp.missingReplicaAddrs(); addrs != nil {
		msg = fmt.Sprintf("action[missingReplicaAddrs],clusterID[%v] metaPartition:%v  lack replication"+
			" on :%v Hosts:%v",
			clusterID, mp.PartitionID, addrs, mp.Hosts)
		Warn(clusterID, msg)
	}

	return
}

func (mp *MetaPartition) buildNewMetaPartitionTasks(specifyAddrs []string, peers []proto.Peer, volName string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	var hosts []string

	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     peers,
		VolName:     volName,
		VerSeq:      mp.VerSeq,
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

func (mp *MetaPartition) tryToChangeLeaderByHost(host string) (err error) {
	var metaNode *MetaNode
	for _, r := range mp.Replicas {
		if host == r.Addr {
			metaNode = r.metaNode
			break
		}
	}
	if metaNode == nil {
		return fmt.Errorf("host not found[%v]", host)
	}
	task, err := mp.createTaskToTryToChangeLeader(host)
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

func (mp *MetaPartition) createTaskToCreateReplica(host string) (t *proto.AdminTask, err error) {
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     mp.Peers,
		VolName:     mp.volName,
		VerSeq:      mp.VerSeq,
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

func (mp *MetaPartition) createTaskToRemoveRaftMember(removePeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return nil, errors.NewError(err)
	}
	req := &proto.RemoveMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, RemovePeer: removePeer}
	t = proto.NewAdminTask(proto.OpRemoveMetaPartitionRaftMember, mr.Addr, req)
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
	return mr.metaNode.IsActive && mr.Status != proto.Unavailable &&
		time.Now().Unix()-mr.ReportTime < defaultMetaPartitionTimeOutSec
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
	mr.TxCnt = mgr.TxCnt
	mr.TxRbInoCnt = mgr.TxRbInoCnt
	mr.TxRbDenCnt = mgr.TxRbDenCnt
	mr.FreeListLen = mgr.FreeListLen
	mr.dataSize = mgr.Size
	mr.ForbidWriteOpOfProtoVer0 = mgr.ForbidWriteOpOfProtoVer0

	if mgr.StatByStorageClass != nil {
		mr.StatByStorageClass = mgr.StatByStorageClass
	} else if len(mr.StatByStorageClass) != 0 {
		// handle compatibility, report from old version metanode has no filed StatByStorageClass
		mr.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
	}

	if mgr.StatByMigrateStorageClass != nil {
		mr.StatByMigrateStorageClass = mgr.StatByMigrateStorageClass
	} else if len(mr.StatByMigrateStorageClass) != 0 {
		mr.StatByMigrateStorageClass = make([]*proto.StatOfStorageClass, 0)
	}

	mr.setLastReportTime()

	if mr.metaNode.RdOnly && mr.Status == proto.ReadWrite {
		mr.Status = proto.ReadOnly
	}
}

func (mr *MetaReplica) createTaskToFreezeReplica(partitionID uint64, freeze bool) (t *proto.AdminTask) {
	req := &proto.FreezeMetaPartitionRequest{
		PartitionID: partitionID,
		Freeze:      freeze,
	}
	t = proto.NewAdminTask(proto.OpFreezeEmptyMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mr *MetaReplica) createTaskToBackupReplica(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.BackupMetaPartitionRequest{
		PartitionID: partitionID,
	}
	t = proto.NewAdminTask(proto.OpBackupEmptyMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mp *MetaPartition) afterCreation(nodeAddr string, c *Cluster) (err error) {
	metaNode, err := c.metaNode(nodeAddr)
	if err != nil {
		return err
	}
	mr := newMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
	mr.ReportTime = time.Now().Unix()
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

func (mp *MetaPartition) getMinusOfMaxInodeID() (minus float64) {
	mp.RLock()
	defer mp.RUnlock()
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.MaxInodeID)
			continue
		}
		diff := math.Abs(float64(replica.MaxInodeID) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) activeMaxInodeSimilar() bool {
	minus := float64(0)
	var sentry float64
	replicas := mp.getLiveReplicas()
	for index, replica := range replicas {
		if index == 0 {
			sentry = float64(replica.MaxInodeID)
			continue
		}
		diff := math.Abs(float64(replica.MaxInodeID) - sentry)
		if diff > minus {
			minus = diff
		}
	}

	return minus < defaultMinusOfMaxInodeID
}

func (mp *MetaPartition) setUidInfo(mgr *proto.MetaPartitionReport) {
	if !mgr.IsLeader {
		return
	}

	mp.uidInfo = mgr.UidInfo
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

// Caller should call mp.lock and mp.unlock when use it.
func (mp *MetaPartition) setHeartBeatDone() {
	if len(mp.Replicas) == int(mp.ReplicaNum) {
		mp.heartBeatDone = true
	}
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

func (mp *MetaPartition) SetForbidWriteOpOfProtoVer0() {
	for _, r := range mp.Replicas {
		if !r.isActive() {
			continue
		}
		if !r.ForbidWriteOpOfProtoVer0 {
			mp.ForbidWriteOpOfProtoVer0 = false
			return
		}
	}
	mp.ForbidWriteOpOfProtoVer0 = true
}

func (mp *MetaPartition) setFreeListLen() {
	var freeListLen uint64
	for _, r := range mp.Replicas {
		if r.FreeListLen > freeListLen {
			freeListLen = r.FreeListLen
		}
	}
	mp.FreeListLen = freeListLen
}

func (mp *MetaPartition) SetTxCnt() {
	var txCnt, rbInoCnt, rbDenCnt uint64
	for _, r := range mp.Replicas {
		if r.TxCnt > txCnt {
			txCnt = r.TxCnt
		}
		if r.TxRbInoCnt > rbInoCnt {
			rbInoCnt = r.TxRbInoCnt
		}
		if r.TxRbDenCnt > rbDenCnt {
			rbDenCnt = r.TxRbDenCnt
		}
	}
	mp.TxCnt, mp.TxRbInoCnt, mp.TxRbDenCnt = txCnt, rbInoCnt, rbDenCnt
}

func (mp *MetaPartition) setStatByStorageClass() {
	var mpNormalStat *proto.StatOfStorageClass
	var mpMigrateStat *proto.StatOfStorageClass
	var ok bool
	statNormalStorageClassMap := make(map[uint32]*proto.StatOfStorageClass)
	statMigrateStorageClassMap := make(map[uint32]*proto.StatOfStorageClass)

	for _, r := range mp.Replicas {
		if r.StatByStorageClass == nil {
			continue
		}

		for _, rStat := range r.StatByStorageClass {
			if mpNormalStat, ok = statNormalStorageClassMap[rStat.StorageClass]; !ok {
				mpNormalStat = proto.NewStatOfStorageClass(rStat.StorageClass)
				statNormalStorageClassMap[rStat.StorageClass] = mpNormalStat
			}

			if rStat.InodeCount > mpNormalStat.InodeCount {
				mpNormalStat.InodeCount = rStat.InodeCount
			}

			if rStat.UsedSizeBytes > mpNormalStat.UsedSizeBytes {
				mpNormalStat.UsedSizeBytes = rStat.UsedSizeBytes
			}
		}

		for _, rMigrateStat := range r.StatByMigrateStorageClass {
			if mpMigrateStat, ok = statMigrateStorageClassMap[rMigrateStat.StorageClass]; !ok {
				mpMigrateStat = proto.NewStatOfStorageClass(rMigrateStat.StorageClass)
				statMigrateStorageClassMap[rMigrateStat.StorageClass] = mpMigrateStat
			}

			if rMigrateStat.InodeCount > mpMigrateStat.InodeCount {
				mpMigrateStat.InodeCount = rMigrateStat.InodeCount
			}

			if rMigrateStat.UsedSizeBytes > mpMigrateStat.UsedSizeBytes {
				mpMigrateStat.UsedSizeBytes = rMigrateStat.UsedSizeBytes
			}
		}
	}

	normalToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, mpStat := range statNormalStorageClassMap {
		normalToSlice = append(normalToSlice, mpStat)
	}
	mp.StatByStorageClass = normalToSlice

	migrateToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, mpStat := range statMigrateStorageClassMap {
		migrateToSlice = append(migrateToSlice, mpStat)
	}
	mp.StatByMigrateStorageClass = migrateToSlice
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

func (mp *MetaPartition) IsEmptyToBeClean() bool {
	if mp.InodeCount != 0 || mp.DentryCount != 0 || mp.End == defaultMaxMetaPartitionInodeID {
		return false
	}

	return true
}
