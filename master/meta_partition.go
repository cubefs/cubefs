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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
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
	LocalPeers                []proto.Peer
	ForbidWriteOpOfProtoVer0  bool
	StatByStorageClass        []*proto.StatOfStorageClass
	StatByMigrateStorageClass []*proto.StatOfStorageClass
	StatByPool                []*proto.StatOfStorageClass
	StatByMigratePool         []*proto.StatOfStorageClass
	metaNode                  *MetaNode
	ReadOnlyReasons           uint32
	StoreMode                 proto.StoreMode
	IsLearner                 bool
	LeaseApplyTime            int64
}

// MetaPartition defines the structure of a meta partition
type MetaPartition struct {
	PartitionID      uint64
	Start            uint64
	End              uint64
	MaxInodeID       uint64
	InodeCount       uint64
	DentryCount      uint64
	FreeListLen      uint64
	TxCnt            uint64
	TxRbInoCnt       uint64
	TxRbDenCnt       uint64
	Replicas         []*MetaReplica
	LeaderReportTime int64
	ReplicaNum       uint8
	Status           int8
	// IsRecover                 atomicutil.Bool
	Freeze                    int8
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
	StatByPool                []*proto.StatOfStorageClass
	StatByMigratePool         []*proto.StatOfStorageClass
	sync.RWMutex

	LastDelReplicaTime int64
	RestoreReplicaMeta uint32
	// DecommissionType   uint32
	Region string // Region name for this meta partition

	proto.RecoverPair
	RecoverLearners []*proto.RecoverPair
}

func (mp *MetaPartition) newMetaReplica(start, end uint64, metaNode *MetaNode) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeID: metaNode.ID, Addr: metaNode.Addr}
	mr.metaNode = metaNode
	mr.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
	mr.StatByMigrateStorageClass = make([]*proto.StatOfStorageClass, 0)
	mr.StatByPool = make([]*proto.StatOfStorageClass, 0)
	mr.StatByMigratePool = make([]*proto.StatOfStorageClass, 0)
	mr.LocalPeers = make([]proto.Peer, 0)
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
	mp.StatByPool = make([]*proto.StatOfStorageClass, 0)
	mp.StatByMigratePool = make([]*proto.StatOfStorageClass, 0)
	mp.Region = proto.DefaultRegion // Default region
	mp.RecoverPair = proto.RecoverPair{}
	return
}

func (mp *MetaPartition) CheckLastDelReplicaTime() bool {
	return mp.GetLastDelTime()+mpReplicaDelInterval < time.Now().Unix()
}

func (mp *MetaPartition) GetLastDelTime() int64 {
	return mp.LastDelReplicaTime
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

	curMaxPartitionID := vol.maxMetaPartitionID()
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

func (mp *MetaPartition) checkLeader(clusterID string, timeOutSec int64) {
	mp.Lock()
	defer mp.Unlock()
	for _, mr := range mp.Replicas {
		if !mr.isActive(timeOutSec) {
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

func (mp *MetaPartition) checkStatus(clusterID string, writeLog bool, replicaNum int, maxPartitionID uint64, metaPartitionInodeIdStep uint64, forbiddenVol bool, timeOutSec int64) (doSplit bool) {
	if mp.IsMetaPartitionFreezed() {
		return
	}

	mp.Lock()
	defer mp.Unlock()

	mp.checkReplicas(timeOutSec)
	liveReplicas := mp.getLiveReplicas(timeOutSec)

	log.LogDebugf("checkStatus: start check mp %d", mp.PartitionID)

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

			if replica.metaNode == nil {
				continue
			}

			if replica.StoreMode == proto.StoreModeRocksDb {
				if !replica.metaNode.reachesRocksdbDisksThreshold() && mp.InodeCount < metaPartitionInodeIdStep {
					continue
				}
			} else {
				if !replica.metaNode.reachesThreshold() && mp.InodeCount < metaPartitionInodeIdStep {
					continue
				}
			}

			if mp.PartitionID == maxPartitionID {
				log.LogInfof("split[checkStatus] need split,id:%v,status:%v,replicaNum:%v,InodeCount:%v", mp.PartitionID, mp.Status, mp.ReplicaNum, mp.InodeCount)
				doSplit = true
			} else {
				if mp.CheckMetaNodeReachScheduled(replica.metaNode, replica.StoreMode, metaPartitionInodeIdStep) {
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
			t = mr.createTaskToDeleteReplica(mp.PartitionID, false)
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
		mr = mp.newMetaReplica(mp.Start, mp.End, metaNode)
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
	liveReplicas := mp.getLiveReplicas(defaultMetaPartitionTimeOutSec)
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

			if rep.IsLearner {
				return nil
			}
			break
		}
	}

	hostNum := 0
	for _, replica := range mp.Replicas {
		if !replica.IsLearner {
			hostNum++
		}
	}
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

// Get live replicas, exclude learner replicas
func (mp *MetaPartition) getLiveReplicas(timeOutSec int64) (liveReplicas []*MetaReplica) {
	liveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive(timeOutSec) && !mr.IsLearner && contains(mp.Hosts, mr.Addr) {
			liveReplicas = append(liveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) checkReplicas(timeOutSec int64) {
	for _, mr := range mp.Replicas {
		if !mr.isActive(timeOutSec) {
			mr.Status = proto.Unavailable
			mr.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
			mr.StatByMigrateStorageClass = make([]*proto.StatOfStorageClass, 0)
			mr.StatByPool = make([]*proto.StatOfStorageClass, 0)
			mr.StatByMigratePool = make([]*proto.StatOfStorageClass, 0)
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

func (mp *MetaPartition) getActiveAddrs(timeOutSec int64) (liveAddrs []string) {
	liveAddrs = make([]string, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive(timeOutSec) {
			liveAddrs = append(liveAddrs, mr.Addr)
		}
	}
	return liveAddrs
}

func (mp *MetaPartition) isMissingReplica(addr string, timeOutSec int64) bool {
	exist := false
	for _, replica := range mp.Replicas {
		if replica.Addr == addr {
			exist = true
			if replica.isMissing(timeOutSec) {
				return true
			}
		}
	}
	return !exist
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

func (mp *MetaPartition) reportMissingReplicas(clusterID, leaderAddr string, timeOutSec int64, interval int64) {
	mp.Lock()
	defer mp.Unlock()
	for _, replica := range mp.Replicas {
		// reduce the alarm frequency
		if contains(mp.Hosts, replica.Addr) && replica.isMissing(timeOutSec) {
			if mp.shouldReportMissingReplica(replica.Addr, interval) {
				metaNode := replica.metaNode
				var lastReportTime time.Time
				isActive := true
				if metaNode != nil {
					lastReportTime = metaNode.ReportTime
					isActive = metaNode.IsActive
				}
				msg := fmt.Sprintf("action[reportMissingReplicas], clusterID[%v] volName[%v] partition:%v  on node:%v  "+
					"miss time > %v  replicaLastRepostTime:%v   nodeLastReportTime:%v  nodeisActive:%v",
					clusterID, mp.volName, mp.PartitionID, replica.Addr, timeOutSec, replica.ReportTime, lastReportTime, isActive)
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
		if mp.isMissingReplica(addr, timeOutSec) && mp.shouldReportMissingReplica(addr, interval) {
			msg := fmt.Sprintf("action[reportMissingReplicas],clusterID[%v] volName[%v] partition:%v  on node:%v  "+
				"miss time > %v",
				clusterID, mp.volName, mp.PartitionID, addr, timeOutSec)
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

func (mp *MetaPartition) buildNewMetaPartitionTasks(specifyAddrs []string, peers []proto.Peer, volName string, storeMode proto.StoreMode) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	var hosts []string

	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     peers,
		VolName:     volName,
		VerSeq:      mp.VerSeq,
		StoreMode:   storeMode,
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

	log.LogWarnf("action[tryToChangeLeader] vol[%v] mp[%v] try to change leader to %v success", mp.volName, mp.PartitionID, metaNode.Addr)
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

func (mp *MetaPartition) createTaskToCreateReplica(host string, storeMode proto.StoreMode) (t *proto.AdminTask, err error) {
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     mp.Peers,
		VolName:     mp.volName,
		VerSeq:      mp.VerSeq,
		StoreMode:   storeMode,
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

func (mp *MetaPartition) createTaskToAddRaftLearner(addPeer proto.Peer, leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: mp.PartitionID,
		AddPeer:     addPeer,
		OpType:      proto.OpTypeAddLearner,
	}
	t = proto.NewAdminTask(proto.OpAddMetaPartitionRaftMember, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	log.LogWarnf("action[createTaskToAddRaftLearner] task created,vol[%v],meta partition[%v],peer[%v:%v],taskID[%v]",
		mp.volName, mp.PartitionID, addPeer.ID, addPeer.Addr, t.ID)
	return
}

func (mp *MetaPartition) createTaskToPromoteLearner(promotePeer proto.Peer, leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: mp.PartitionID,
		AddPeer:     promotePeer,
		OpType:      proto.OpTypePromoteLearner,
	}
	t = proto.NewAdminTask(proto.OpAddMetaPartitionRaftMember, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	log.LogWarnf("action[createTaskToPromoteLearner] task created,vol[%v],meta partition[%v],peer[%v:%v],taskID[%v]",
		mp.volName, mp.PartitionID, promotePeer.ID, promotePeer.Addr, t.ID)
	return
}

func (mp *MetaPartition) createTaskToRemoveRaftMember(removePeer proto.Peer, force bool, autoRemove bool) (t *proto.AdminTask, err error) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil && !force {
		return nil, errors.NewError(err)
	}
	var leaderAddr string
	if mr != nil {
		leaderAddr = mr.Addr
	}
	req := &proto.RemoveMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, RemovePeer: removePeer, Force: force, AutoRemove: autoRemove}
	t = proto.NewAdminTask(proto.OpRemoveMetaPartitionRaftMember, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	err = nil
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

func (mr *MetaReplica) createTaskToDeleteReplica(partitionID uint64, raftForceDel bool) (t *proto.AdminTask) {
	req := &proto.DeleteMetaPartitionRequest{PartitionID: partitionID, Force: raftForceDel}
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

func (mr *MetaReplica) isMissing(timeOutSec int64) (miss bool) {
	return time.Now().Unix()-mr.ReportTime > timeOutSec
}

func (mr *MetaReplica) isActive(timeOutSec int64) (active bool) {
	return mr.metaNode.IsActive && mr.Status != proto.Unavailable &&
		time.Now().Unix()-mr.ReportTime < timeOutSec
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
	mr.ReadOnlyReasons = mgr.ReadOnlyReasons
	mr.LocalPeers = mgr.LocalPeers

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

	if mgr.StatByPool != nil {
		mr.StatByPool = mgr.StatByPool
	} else if len(mr.StatByPool) != 0 {
		mr.StatByPool = make([]*proto.StatOfStorageClass, 0)
	}

	if mgr.StatByMigratePool != nil {
		mr.StatByMigratePool = mgr.StatByMigratePool
	} else if len(mr.StatByMigratePool) != 0 {
		mr.StatByMigratePool = make([]*proto.StatOfStorageClass, 0)
	}

	mr.setLastReportTime()

	if mgr.IsLearner != mr.IsLearner {
		mr.IsLearner = mgr.IsLearner
		log.LogWarnf("action[updateMetric] mp [%v] meta replica[%v] is learner[%v]", mgr.PartitionID, mr.Addr, mr.IsLearner)
	}

	mr.LeaseApplyTime = mgr.LeaseApplyTime

	if mgr.StoreMode == proto.StoreModeMem && mr.metaNode.RdOnly {
		mr.ReadOnlyReasons |= proto.MetaNodeReadOnly
		if mr.Status == proto.ReadWrite {
			mr.Status = proto.ReadOnly
		}
	} else if mgr.StoreMode == proto.StoreModeRocksDb && mr.metaNode.RocksdbRdOnly {
		mr.ReadOnlyReasons |= proto.RocksdbReadOnly
		if mr.Status == proto.ReadWrite {
			mr.Status = proto.ReadOnly
		}
	}
	mr.StoreMode = mgr.StoreMode
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

func (mp *MetaPartition) afterCreation(nodeAddr string, c *Cluster, storeMode proto.StoreMode) (err error) {
	metaNode, err := c.metaNode(nodeAddr)
	if err != nil {
		return err
	}
	mr := mp.newMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
	mr.ReportTime = time.Now().Unix()
	mr.StoreMode = storeMode
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
	replicas := mp.getLiveReplicas(defaultMetaPartitionTimeOutSec)
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
		if !r.isActive(defaultMetaPartitionTimeOutSec) {
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
	statByPoolMap := make(map[uint8]*proto.StatOfStorageClass)
	statByMigratePoolMap := make(map[uint8]*proto.StatOfStorageClass)
	var statPool *proto.StatOfStorageClass
	var statMigratePool *proto.StatOfStorageClass

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

		// stat pool
		for _, rStat := range r.StatByPool {
			if statPool, ok = statByPoolMap[rStat.PoolId]; !ok {
				statPool = proto.NewStatOfStorageClassByPool(rStat.PoolId)
				statByPoolMap[rStat.PoolId] = statPool
			}
			if rStat.InodeCount > statPool.InodeCount {
				statPool.InodeCount = rStat.InodeCount
			}
			if rStat.UsedSizeBytes > statPool.UsedSizeBytes {
				statPool.UsedSizeBytes = rStat.UsedSizeBytes
			}
		}

		for _, rStat := range r.StatByMigratePool {
			if statMigratePool, ok = statByMigratePoolMap[rStat.PoolId]; !ok {
				statMigratePool = proto.NewStatOfStorageClassByPool(rStat.PoolId)
				statByMigratePoolMap[rStat.PoolId] = statMigratePool
			}
			if rStat.UsedSizeBytes > statMigratePool.UsedSizeBytes {
				statMigratePool.UsedSizeBytes = rStat.UsedSizeBytes
			}
			if rStat.InodeCount > statMigratePool.InodeCount {
				statMigratePool.InodeCount = rStat.InodeCount
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

	poolToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat := range statByPoolMap {
		poolToSlice = append(poolToSlice, stat)
	}
	mp.StatByPool = poolToSlice

	migratePoolToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat := range statByMigratePoolMap {
		migratePoolToSlice = append(migratePoolToSlice, stat)
	}
	mp.StatByMigratePool = migratePoolToSlice
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

	mp.RLock()
	defer mp.RUnlock()
	for _, replica := range mp.Replicas {
		if replica.StoreMode == proto.StoreModeRocksDb {
			return false
		}
	}

	return true
}

func (mr *MetaReplica) createTaskToGetRaftStatus(partitionID uint64, replicaNum int) (t *proto.AdminTask) {
	req := &proto.IsRaftStatusOKRequest{
		PartitionID: partitionID,
		ReplicaNum:  replicaNum,
	}
	t = proto.NewAdminTask(proto.OpIsRaftStatusOk, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mp *MetaPartition) IsMetaPartitionFreezed() bool {
	return mp.Freeze != proto.FreezeMetaPartitionInit
}

func (mp *MetaPartition) GetMetaReplicaStoreMode(addr string) (mode proto.StoreMode, err error) {
	mode = proto.StoreModeMax
	for _, replica := range mp.Replicas {
		if replica.Addr == addr {
			mode = replica.StoreMode
			break
		}
	}
	if mode == proto.StoreModeMax {
		err = fmt.Errorf("get store mode failed, addr: %s", addr)
	}

	return
}

func (mp *MetaPartition) CheckMetaNodeReachScheduled(metaNode *MetaNode, storeMode proto.StoreMode, metaPartitionInodeIdStep uint64) bool {
	if mp.End-mp.MaxInodeID > 2*metaPartitionInodeIdStep {
		return true
	}

	if storeMode == proto.StoreModeRocksDb {
		return metaNode.reachesRocksdbDisksThreshold()
	}

	return metaNode.reachesThreshold()
}

// checkIntersection checks if master peers and replica peers have intersection
func (mp *MetaPartition) checkIntersection(c *Cluster) error {
	mp.RLock()
	defer mp.RUnlock()

	replicaPeers := make([][]proto.Peer, 0, len(mp.Replicas))
	for _, replica := range mp.Replicas {
		replicaPeers = append(replicaPeers, replica.LocalPeers)
	}

	if isInterSectionBetweenMasterAndReplicasEmptySet(mp.Peers, replicaPeers) {
		log.LogErrorf("action[checkIntersection]mp(%v) interSection between master and replicas is the empty set", mp.PartitionID)
		c.NoSamePeerMps.Store(mp.PartitionID, struct{}{})
		return proto.ErrDpNoSamePeer
	}
	return nil
}

// removeExcessiveReplicas removes auto-promotable learners and excessive voters
func (mp *MetaPartition) removeExcessiveReplicas(c *Cluster) (err error) {
	mp.RLock()
	peers := append([]proto.Peer(nil), mp.Peers...)
	mp.RUnlock()

	var (
		removedAddrs []string
		auditMsg     string
	)

	defer func() {
		if err != nil {
			auditMsg = fmt.Sprintf("mp(%v) remove excessive replicas failed, err %v", mp.PartitionID, err)
			log.LogErrorf("action[removeExcessiveReplicas] %v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		}
		for _, addr := range removedAddrs {
			auditMsg = fmt.Sprintf("mp(%v) remove excessive replica %v", mp.PartitionID, addr)
			log.LogDebugf("action[removeExcessiveReplicas]%v, err %v", auditMsg, err)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		}
	}()

	nonLearnerPeers := make([]proto.Peer, 0)

	// 1) Remove auto-promotable learners
	for _, p := range peers {
		if p.Type == raftProto.PeerLearner && !p.ManualPromote {
			if err = c.deleteMetaReplica(mp, p.Addr, false, false); err != nil {
				return err
			}
			removedAddrs = append(removedAddrs, p.Addr)
		} else if p.Type != raftProto.PeerLearner {
			nonLearnerPeers = append(nonLearnerPeers, p)
		}
	}

	// 2) Remove extra voter when voter count exceeds ReplicaNum
	if len(nonLearnerPeers) > int(mp.ReplicaNum) {
		var (
			removeAddr string
			leaderAddr = mp.getLeaderAddr()
		)

		if mp.RecoverSrc != "" {
			removeAddr = mp.RecoverSrc
		} else {
			removeAddr = nonLearnerPeers[len(nonLearnerPeers)-1].Addr
		}
		if removeAddr == leaderAddr {
			for i := len(nonLearnerPeers) - 1; i >= 0; i-- {
				if nonLearnerPeers[i].Addr != leaderAddr {
					removeAddr = nonLearnerPeers[i].Addr
					break
				}
			}
		}
		if err = c.deleteMetaReplica(mp, removeAddr, false, false); err != nil {
			return err
		}
		removedAddrs = append(removedAddrs, removeAddr)
	}
	return nil
}

// removeRedundantPeersFromReplicaMeta removes redundant peers reported by replicas
func (mp *MetaPartition) removeRedundantPeersFromReplicaMeta(c *Cluster) (err error) {
	type replicaView struct {
		addr       string
		localPeers []proto.Peer
	}

	mp.RLock()
	replicas := make([]replicaView, 0, len(mp.Replicas))
	for _, r := range mp.Replicas {
		replicas = append(replicas, replicaView{
			addr:       r.Addr,
			localPeers: append([]proto.Peer(nil), r.LocalPeers...),
		})
	}
	peers := append([]proto.Peer(nil), mp.Peers...)
	mp.RUnlock()

	var (
		removedPeers []proto.Peer
		auditMsg     string
	)

	defer func() {
		if err != nil {
			auditMsg = fmt.Sprintf("mp(%v) remove redundant peer from replica meta failed, err %v", mp.PartitionID, err)
			log.LogErrorf("action[removeRedundantPeersFromReplicaMeta] %v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		}
		for _, peer := range removedPeers {
			auditMsg = fmt.Sprintf("mp(%v) remove redundant peer %v from replica meta", mp.PartitionID, peer.Addr)
			log.LogDebugf("action[removeRedundantPeersFromReplicaMeta]%v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, nil)
		}
	}()

	force := false
	replicasToDelete := make([]proto.Peer, 0)

	for _, replica := range replicas {
		if len(replica.localPeers) == 0 {
			continue
		}
		if mp.lostLeader(c) {
			return
		}
		redundantPeers := findPeersToDeleteByConfig(replica.localPeers, peers)
		for _, peer := range redundantPeers {
			replicasToDelete = append(replicasToDelete, peer)
			if err = c.removeMetaPartitionRaftMember(mp, peer, force, true); err != nil {
				return
			}
		}
	}

	for _, peer := range replicasToDelete {
		metaNode, err := c.metaNode(peer.Addr)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				continue
			}
			return err
		}
		if err = c.deleteMetaPartition(mp, metaNode, true); err != nil {
			return err
		}
		removedPeers = append(removedPeers, peer)
	}
	return nil
}

// removeRedundantPeersFromMaster removes redundant peers from master peers
func (mp *MetaPartition) removeRedundantPeersFromMaster(c *Cluster) (err error) {
	mp.RLock()
	leader, err := mp.getMetaReplicaLeader()
	if err != nil {
		mp.RUnlock()
		return err
	}
	localPeers := append([]proto.Peer(nil), leader.LocalPeers...)
	peers := append([]proto.Peer(nil), mp.Peers...)
	liveReplicas := mp.getLiveReplicas(defaultMetaPartitionTimeOutSec)
	liveAddrs := mp.getLiveReplicasAddr(liveReplicas)
	mp.RUnlock()

	var (
		removedPeers []proto.Peer
		auditMsg     string
	)

	nonLearnerNum := len(liveAddrs)

	defer func() {
		if err != nil {
			auditMsg = fmt.Sprintf("mp(%v) remove redundant peer from master failed, err %v", mp.PartitionID, err)
			log.LogErrorf("action[removeRedundantPeersFromMaster] %v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		}
		for _, peer := range removedPeers {
			auditMsg = fmt.Sprintf("mp(%v) remove redundant peer %v from master", mp.PartitionID, peer.Addr)
			log.LogDebugf("action[removeRedundantPeersFromMaster]%v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, nil)
		}
	}()

	if len(localPeers) == 0 {
		return nil
	}

	redundantPeers := findPeersToDeleteByConfig(peers, localPeers)
	for _, peer := range redundantPeers {
		if contains(liveAddrs, peer.Addr) && peer.Type == raftProto.PeerNormal && nonLearnerNum <= int(mp.ReplicaNum/2+1) {
			continue
		}
		if peer.Type == raftProto.PeerLearner && peer.ManualPromote {
			continue
		}
		if err = c.removeMetaHostMember(mp, peer); err != nil {
			return err
		}
		metaNode, err := c.metaNode(peer.Addr)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				continue
			}
			return err
		}
		if err = c.deleteMetaPartition(mp, metaNode, true); err != nil {
			return err
		}
		removedPeers = append(removedPeers, peer)
		if contains(liveAddrs, peer.Addr) && peer.Type == raftProto.PeerNormal {
			nonLearnerNum--
		}
	}
	return nil
}

// Automatically add a replica when non-learner count < ReplicaNum
func (mp *MetaPartition) autoAddReplica(c *Cluster) (err error) {
	// Auto add replica only works in learner mode
	if !c.EnableMpDecommissionByLearner {
		log.LogDebugf("action[autoAddReplica]mp(%v) skip: learner mode is not enabled", mp.PartitionID)
		return nil
	}

	// Limit the number of autoAddReplica
	if c.CheckMPDecommissionLimit(proto.AutoAddReplica) != nil {
		return errors.NewErrorf("autoAddReplica throttled: meta partition decommission limit reached for type AutoAddReplica")
	}

	mp.RLock()
	peers := append([]proto.Peer(nil), mp.Peers...)
	mp.RUnlock()

	nonLearnerCount := 0
	for _, p := range peers {
		if p.Type != raftProto.PeerLearner {
			nonLearnerCount++
		}
	}

	if mp.ReplicaNum <= uint8(nonLearnerCount) {
		return nil
	}

	var (
		auditMsg  string
		addedAddr string
	)

	defer func() {
		if err != nil {
			auditMsg = fmt.Sprintf("mp(%v) auto add replica failed, err %v", mp.PartitionID, err)
			log.LogErrorf("action[addMissingReplicas] %v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		} else {
			auditMsg = fmt.Sprintf("mp(%v) auto add replica %v", mp.PartitionID, addedAddr)
			log.LogDebugf("action[addMissingReplicas]%v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, nil)
		}
		if !mp.IsRecover.Load() {
			mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)
		}
		mp.RLock()
		c.syncUpdateMetaPartition(mp)
		mp.RUnlock()
	}()

	if mp.lostLeader(c) {
		return errors.NewErrorf("mp(%v) lost leader skip auto add replica", mp.PartitionID)
	}

	vol, err := c.getVol(mp.volName)
	if err != nil {
		return err
	}

	selectPeers, storeMode, err := c.selectTargetMetaPeer(mp, "", "", vol.DefaultStoreMode, mp.Region)
	if err != nil {
		return err
	}

	if len(selectPeers) == 0 {
		return errors.NewErrorf("mp(%v) selectTargetMetaPeer returns empty peers", mp.PartitionID)
	}

	// Add replica using learner mode
	mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)
	if err = c.addMetaReplicaLearner(mp, selectPeers[0].Addr, storeMode, "", false, proto.AutoAddReplica); err != nil {
		return err
	}

	return nil
}

func (mp *MetaPartition) checkReplicaMeta(c *Cluster) (err error) {
	if err = mp.checkIntersection(c); err != nil {
		return err
	}

	if _, ok := c.NoSamePeerMps.Load(mp.PartitionID); ok {
		c.NoSamePeerMps.Delete(mp.PartitionID)
	}

	if !mp.needReplicaMetaRestore(c) {
		log.LogDebugf("action[checkReplicaMeta]mp(%v) do not need to restore meta", mp.PartitionID)
		return nil
	}

	if !mp.setRestoreReplicaRunning() {
		log.LogDebugf("action[checkReplicaMeta]mp(%v) set RestoreReplicaMetaRunning failed", mp.PartitionID)
		return proto.ErrPerformingRestoreReplica
	}

	mp.RLock()
	err = c.syncUpdateMetaPartition(mp)
	mp.RUnlock()
	if err != nil {
		mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)
		return
	}

	defer func() {
		if !mp.IsRecover.Load() {
			mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)
		}
		mp.RLock()
		c.syncUpdateMetaPartition(mp)
		mp.RUnlock()
	}()

	// stage1: remove redundant peers from replica meta
	if err = mp.removeRedundantPeersFromReplicaMeta(c); err != nil {
		return err
	}

	// stage2: remove redundant peers from master
	if err = mp.removeRedundantPeersFromMaster(c); err != nil {
		return err
	}

	// stage3: remove excessive replicas
	if err = mp.removeExcessiveReplicas(c); err != nil {
		return err
	}

	// stage3b: remove manual learners that violate volume MP region policy for mp.Region
	if err = mp.removeManualLearnersViolatingMpRegionPolicy(c); err != nil {
		return err
	}

	// stage4: add missing replicas
	if err = mp.autoAddReplica(c); err != nil {
		return err
	}

	return nil
}

// volMpPolicyForSourceRegion returns the VolMpPolicy for meta partitions whose data source region is
// sourceRegion. When the volume has no mpPolicy, an empty map, or no entry for sourceRegion, returns an
// empty policy (Learner may be nil).
func volMpPolicyForSourceRegion(vol *Vol, sourceRegion string) *proto.VolMpPolicy {
	if vol == nil {
		return &proto.VolMpPolicy{}
	}
	var mpPolicy *proto.VolMpPolicy
	vol.volLock.RLock()
	if vol.mpPolicy == nil || len(vol.mpPolicy) == 0 {
		mpPolicy = &proto.VolMpPolicy{}
	} else {
		mpPolicy = vol.mpPolicy[sourceRegion]
	}
	vol.volLock.RUnlock()
	if mpPolicy == nil {
		return &proto.VolMpPolicy{}
	}
	return mpPolicy
}

// removeManualLearnersViolatingMpRegionPolicy removes manual-promote raft learners whose metanode region
// is not listed in policy.Learner for mp.Region (same effective policy as manualLearnerOutsideMpRegionPolicy).
func (mp *MetaPartition) removeManualLearnersViolatingMpRegionPolicy(c *Cluster) (err error) {
	if c == nil {
		return nil
	}
	vol, err := c.getVol(mp.volName)
	if err != nil || vol == nil {
		return nil
	}
	policy := volMpPolicyForSourceRegion(vol, mp.Region)

	mp.RLock()
	peers := append([]proto.Peer(nil), mp.Peers...)
	mp.RUnlock()

	var (
		removedAddrs []string
		auditMsg     string
	)

	defer func() {
		if err != nil {
			auditMsg = fmt.Sprintf("mp(%v) remove policy-violating manual learners failed, err %v", mp.PartitionID, err)
			log.LogErrorf("action[removeManualLearnersViolatingMpRegionPolicy] %v", auditMsg)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		}
		for _, addr := range removedAddrs {
			auditMsg = fmt.Sprintf("mp(%v) removed policy-violating manual learner %v", mp.PartitionID, addr)
			log.LogDebugf("action[removeManualLearnersViolatingMpRegionPolicy]%v, err %v", auditMsg, err)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, err)
		}
	}()

	for _, p := range peers {
		if p.Type != raftProto.PeerLearner || !p.ManualPromote {
			continue
		}
		dstRegion := c.getRegionFromMetaNodeAddr(p.Addr)
		if _, allowed := policy.Learner[dstRegion]; allowed {
			continue
		}
		if err = c.deleteMetaReplica(mp, p.Addr, false, false); err != nil {
			return err
		}
		removedAddrs = append(removedAddrs, p.Addr)
	}

	mp.RLock()
	peersAfter := append([]proto.Peer(nil), mp.Peers...)
	mp.RUnlock()
	for _, addr := range mp.addrsToTrimForMpRegionPolicyLearners(c, policy, peersAfter) {
		if err = c.deleteMetaReplica(mp, addr, false, false); err != nil {
			return err
		}
		removedAddrs = append(removedAddrs, addr)
	}
	return nil
}

// isManualLearnerPeerMigrating reports whether peerAddr matches RecoverDst on the partition
// (embedded RecoverPair or RecoverLearners).
func (mp *MetaPartition) isManualLearnerPeerMigrating(peerAddr string) bool {
	match := func(rp *proto.RecoverPair) bool {
		if rp == nil || rp.RecoverDst != peerAddr {
			return false
		}
		if rp.RecoverSrc == peerAddr || rp.RecoverDst == peerAddr {
			return true
		}
		return false
	}
	if match(&mp.RecoverPair) {
		return true
	}
	for _, rp := range mp.RecoverLearners {
		if match(rp) {
			return true
		}
	}
	return false
}

// addrsToTrimForMpRegionPolicyLearners returns manual learner addresses to delete so that each policy
// target region has at most one learner. When multiple learners map to the same target region,
// prefer removing StoreMode-mismatched replicas first (when replica mode is known), then surplus peers.
func (mp *MetaPartition) addrsToTrimForMpRegionPolicyLearners(c *Cluster, policy *proto.VolMpPolicy, peers []proto.Peer) []string {
	if policy == nil || len(policy.Learner) == 0 || c == nil {
		return nil
	}
	type ranked struct {
		addr   string
		modeOk bool
	}
	var out []string
	for targetRegion, lp := range policy.Learner {
		if lp == nil {
			continue
		}
		var addrs []string
		for _, p := range peers {
			if p.Type != raftProto.PeerLearner || !p.ManualPromote {
				continue
			}

			if c.getRegionFromMetaNodeAddr(p.Addr) != targetRegion {
				continue
			}

			addrs = append(addrs, p.Addr)
		}
		if len(addrs) <= 1 {
			continue
		}

		rankedList := make([]ranked, 0, len(addrs))
		for _, addr := range addrs {
			mode, err := mp.GetMetaReplicaStoreMode(addr)
			modeOk := err == nil && mode == lp.Mode
			rankedList = append(rankedList, ranked{addr: addr, modeOk: modeOk})
		}

		sort.SliceStable(rankedList, func(i, j int) bool {
			if rankedList[i].modeOk != rankedList[j].modeOk {
				return !rankedList[i].modeOk && rankedList[j].modeOk
			}
			return rankedList[i].addr < rankedList[j].addr
		})

		for i := 0; i < len(rankedList)-1; i++ {
			out = append(out, rankedList[i].addr)
		}
	}
	return out
}

// manualLearnerOutsideMpRegionPolicy returns true if any manual-promote raft learner is not allowed
// by the effective policy for mp.Region. Effective policy is empty (no Learner entries) when the volume
// has no mpPolicy, an empty mpPolicy map, or no entry for mp.Region; then any manual learner triggers restore.
// Otherwise each manual learner's metanode region must appear in policy.Learner.
// It also returns true when more than one manual learner maps to the same policy.Learner target region.
// If any manual learner matches isManualLearnerPeerMigrating, this returns false (no violation) without
// finishing duplicate detection.
func (mp *MetaPartition) manualLearnerOutsideMpRegionPolicy(c *Cluster) bool {
	if c == nil {
		return false
	}
	vol, err := c.getVol(mp.volName)
	if err != nil || vol == nil {
		return false
	}
	policy := volMpPolicyForSourceRegion(vol, mp.Region)

	for _, p := range mp.Peers {
		if p.Type != raftProto.PeerLearner || !p.ManualPromote {
			continue
		}

		dstRegion := c.getRegionFromMetaNodeAddr(p.Addr)
		if _, allowed := policy.Learner[dstRegion]; !allowed {
			log.LogInfof("manualLearnerOutsideMpRegionPolicy: mp(%v) manual learner %v not allowed by policy %v", mp.PartitionID, p.Addr, policy)
			return true
		}
	}

	for targetRegion, lp := range policy.Learner {
		if lp == nil {
			continue
		}
		var addrs []string
		for _, p := range mp.Peers {
			if p.Type != raftProto.PeerLearner || !p.ManualPromote {
				continue
			}
			if mp.isManualLearnerPeerMigrating(p.Addr) {
				return false
			}

			if c.getRegionFromMetaNodeAddr(p.Addr) != targetRegion {
				continue
			}

			addrs = append(addrs, p.Addr)
		}
		if len(addrs) > 1 {
			log.LogInfof("manualLearnerOutsideMpRegionPolicy: mp(%v) manual learner %v has multiple learners for target region %v", mp.PartitionID, addrs, targetRegion)
			return true
		}
	}
	return false
}

func (mp *MetaPartition) needReplicaMetaRestore(c *Cluster) bool {
	mp.RLock()
	defer mp.RUnlock()

	// stage1: remove excessive replicas (non-learner count should not exceed ReplicaNum; auto learners should be cleaned)
	nonLearnerCnt := 0
	hasAutoLearner := false
	for _, p := range mp.Peers {
		if p.Type != raftProto.PeerLearner {
			nonLearnerCnt++
		} else if !p.ManualPromote {
			hasAutoLearner = true
		}
	}
	if nonLearnerCnt > int(mp.ReplicaNum) || hasAutoLearner {
		return true
	}

	// stage1b: manual learners not allowed by volume MP region policy for this partition's region
	if mp.manualLearnerOutsideMpRegionPolicy(c) {
		return true
	}

	// stage2: redundant peers reported by replicas
	for _, replica := range mp.Replicas {
		if len(replica.LocalPeers) == 0 {
			continue
		}

		redundantPeers := findPeersToDeleteByConfig(replica.LocalPeers, mp.Peers)
		if len(redundantPeers) != 0 {
			return true
		}
	}

	// stage3: redundant peers from master perspective vs leader view
	leader, err := mp.getMetaReplicaLeader()
	if err == nil && len(leader.LocalPeers) != 0 {
		redundantPeers := findPeersToDeleteByConfig(mp.Peers, leader.LocalPeers)
		if len(redundantPeers) != 0 {
			return true
		}
	}

	// stage4: add missing replicas (non-learner count below ReplicaNum)
	if nonLearnerCnt < int(mp.ReplicaNum) {
		if mp.lostLeader(c) {
			auditMsg := fmt.Sprintf("mp(%v) lost leader skip auto add replica", mp.PartitionID)
			auditlog.LogMasterOp("RestoreReplicaMeta", auditMsg, nil)
			return false
		}
		return true
	}

	return false
}

func (mp *MetaPartition) hasPeersRecovering() bool {
	if mp.IsRecover.Load() {
		return true
	}
	for _, learner := range mp.RecoverLearners {
		if learner.RecoverState != proto.RecoverStateFailed {
			return true
		}
	}
	return false
}

func (mp *MetaPartition) setRestoreReplicaRunning() bool {
	return atomic.CompareAndSwapUint32(&mp.RestoreReplicaMeta, RestoreReplicaMetaStop, RestoreReplicaMetaRunning)
}

func (mp *MetaPartition) setRestoreReplicaForbidden() bool {
	return atomic.CompareAndSwapUint32(&mp.RestoreReplicaMeta, RestoreReplicaMetaStop, RestoreReplicaMetaForbidden)
}

func (mp *MetaPartition) setRestoreReplicaStatus(status uint32) {
	if status == RestoreReplicaMetaStop && mp.hasPeersRecovering() {
		log.LogWarnf("setRestoreReplicaStatus: mp(%v) is recovering or has learners, skip set status %v, recover %v, len(recoverLearners) %v",
			mp.PartitionID, status, mp.IsRecover.Load(), len(mp.RecoverLearners))
		return
	}
	atomic.StoreUint32(&mp.RestoreReplicaMeta, status)
}

func (mp *MetaPartition) lostLeader(c *Cluster) bool {
	return mp.getLeaderAddr() == "" && (time.Now().Unix()-mp.LeaderReportTime > c.cfg.MpNoLeaderReportIntervalSec)
}

func (mp *MetaPartition) getLeaderAddr() string {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return ""
	}
	return mr.Addr
}
