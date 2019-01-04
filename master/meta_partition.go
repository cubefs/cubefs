// Copyright 2018 The Container File System Authors.
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
	"sync"

	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"strings"
	"time"
)

// MetaReplica defines the replica of a meta partition
type MetaReplica struct {
	Addr       string
	start      uint64  // lower bound of the inode id
	end        uint64  // upper bound of the inode id
	nodeID     uint64
	ReportTime int64
	Status     int8    // unavailable, readOnly, readWrite
	IsLeader   bool
	metaNode   *MetaNode
}

// MetaPartition defines the structure of a meta partition
type MetaPartition struct {
	PartitionID  uint64
	Start        uint64
	End          uint64
	MaxNodeID    uint64
	Replicas     []*MetaReplica
	ReplicaNum   uint8 // TODO is this necessary ?
	Status       int8
	volID        uint64
	volName      string
	Hosts        []string
	Peers        []proto.Peer
	MissNodes    map[string]int64
	LoadResponse []*proto.MetaPartitionLoadResponse
	sync.RWMutex
}

func newMetaReplica(start, end uint64, metaNode *MetaNode) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeID: metaNode.ID, Addr: metaNode.Addr}
	mr.metaNode = metaNode
	mr.ReportTime = time.Now().Unix()
	return
}

func newMetaPartition(partitionID, start, end uint64, replicaNum uint8, volName string, volID uint64) (mp *MetaPartition) {
	mp = &MetaPartition{PartitionID: partitionID, Start: start, End: end, volName: volName, volID: volID}
	mp.ReplicaNum = replicaNum
	mp.Replicas = make([]*MetaReplica, 0)
	mp.Status = proto.Unavaliable
	mp.MissNodes = make(map[string]int64, 0)
	mp.Peers = make([]proto.Peer, 0)
	mp.Hosts = make([]string, 0)
	mp.LoadResponse = make([]*proto.MetaPartitionLoadResponse, 0)
	return
}

func (mp *MetaPartition) toJSON() (body []byte, err error) {
	mp.RLock()
	defer mp.RUnlock()
	return json.Marshal(mp)
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

func (mp *MetaPartition) updateInodeIDUpperBoundForAllReplicas() {
	for _, mr := range mp.Replicas {
		mr.end = mp.End
	}
}

func (mp *MetaPartition) updateInodeIDUpperBound(c *Cluster, end uint64) {
	// overflow
	if end > (defaultMaxMetaPartitionInodeID - defaultMetaPartitionInodeIDStep) {
		log.LogWarnf("action[updateInodeIDUpperBound] clusterID[%v] partitionID[%v] nextStart[%v] "+
			"to prevent overflow ,not update end", c.Name, mp.PartitionID, end)
		return
	}
	var err error
	tasks := make([]*proto.AdminTask, 0)
	oldEnd := mp.End
	mp.End = end
	t := mp.createTaskToUpdateMetaReplica(c.Name, mp.PartitionID, end)

	// if there is no leader, we don't update the end
	if t == nil {
		mp.End = oldEnd
		return
	}
	if err = c.syncUpdateMetaPartition(mp); err != nil {
		mp.End = oldEnd
		goto errHandler
	}
	mp.updateInodeIDUpperBoundForAllReplicas()
	tasks = append(tasks, t)
	c.addMetaNodeTasks(tasks)
	if err = c.createMetaPartition(mp.volName, mp.End+1, defaultMaxMetaPartitionInodeID); err != nil {
		Warn(c.Name, fmt.Sprintf("action[updateInodeIDUpperBound] clusterID[%v] partitionID[%v] create meta partition err[%v]",
			c.Name, mp.PartitionID, err))
		goto errHandler
	}
	log.LogWarnf("action[updateInodeIDUpperBound] partitionID[%v] end[%v] success", mp.PartitionID, mp.End)
	return
errHandler:
	log.LogErrorf("action[updateInodeIDUpperBound] partitionID[%v] err[%v]", mp.PartitionID, err)
	return
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
	mp.Lock()
	defer mp.Unlock()
	curMaxPartitionID := vol.maxPartitionID()
	if mp.PartitionID != curMaxPartitionID {
		log.LogWarnf("action[checkEnd] partition[%v] not max partition[%v]", mp.PartitionID, curMaxPartitionID)
		return
	}
	if mp.End != defaultMaxMetaPartitionInodeID {
		oldEnd := mp.End
		mp.End = defaultMaxMetaPartitionInodeID
		if err := c.syncUpdateMetaPartition(mp); err != nil {
			mp.End = oldEnd
			log.LogErrorf("action[checkEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
		}
	}
	log.LogWarnf("action[checkEnd] partitionID[%v] end[%v]", mp.PartitionID, mp.End)
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

func (mp *MetaPartition) checkReplicaLeader() {
	mp.Lock()
	defer mp.Unlock()
	for _, mr := range mp.Replicas {
		if !mr.isActive() {
			mr.IsLeader = false
		}
	}
	return
}

func (mp *MetaPartition) checkStatus(writeLog bool, replicaNum int) {
	mp.Lock()
	defer mp.Unlock()
	liveReplicas := mp.getLiveReplica()
	if len(liveReplicas) <= replicaNum/2 {
		mp.Status = proto.Unavaliable
	} else {
		mr, err := mp.getLeaderMetaReplica()
		if err != nil {
			mp.Status = proto.Unavaliable
		}
		mp.Status = mr.Status
	}

	if writeLog {
		log.LogInfof("action[checkMPStatus],id:%v,status:%v,replicaNum:%v,liveReplicas:%v persistenceHosts:%v",
			mp.PartitionID, mp.Status, mp.ReplicaNum, len(liveReplicas), mp.Hosts)
	}
}

func (mp *MetaPartition) getLeaderMetaReplica() (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.IsLeader {
			return
		}
	}
	err = noLeaderErr
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
			err = illegalMetaReplicaErr
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
	mp.MaxNodeID = mgr.MaxInodeID
	mr.updateMetric(mgr)
	mp.removeMissingReplica(metaNode.Addr)
}

func (mp *MetaPartition) canBeOffline(nodeAddr string, replicaNum int) (err error) {
	liveReplicas := mp.getLiveReplica()
	if !mp.hasMajorityReplicas(len(liveReplicas), replicaNum) {
		err = noEnoughReplicaErr
		return
	}
	liveAddrs := mp.getLiveReplicasAddr(liveReplicas)
	if len(liveReplicas) == (replicaNum/2+1) && contains(liveAddrs, nodeAddr) {
		err = fmt.Errorf("live replicas num will be less than majority after offline nodeAddr: %v", nodeAddr)
		return
	}
	return
}

// TODO inline?
func (mp *MetaPartition) hasMajorityReplicas(liveReplicas int, replicaNum int) bool {
	return liveReplicas >= int(mp.ReplicaNum/2+1)
}

func (mp *MetaPartition) getLiveReplicasAddr(liveReplicas []*MetaReplica) (addrs []string) {
	addrs = make([]string, 0)
	for _, mr := range liveReplicas {
		addrs = append(addrs, mr.Addr)
	}
	return
}
func (mp *MetaPartition) getLiveReplica() (liveReplicas []*MetaReplica) {
	liveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveReplicas = append(liveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) persistToRocksDB(newHosts []string, newPeers []proto.Peer, volName string, c *Cluster) (err error) {
	oldHosts := make([]string, len(mp.Hosts))
	copy(oldHosts, mp.Hosts)
	oldPeers := make([]proto.Peer, len(mp.Peers))
	copy(oldPeers, mp.Peers)
	mp.Hosts = newHosts
	mp.Peers = newPeers
	if err = c.syncUpdateMetaPartition(mp); err != nil {
		mp.Hosts = oldHosts
		mp.Peers = oldPeers
		log.LogWarnf("action[persistToRocksDB] failed,partitionID:%v  old hosts:%v new hosts:%v oldPeers:%v  newPeers:%v",
			mp.PartitionID, mp.Hosts, newHosts, mp.Peers, newPeers)
		return
	}
	log.LogWarnf("action[persistToRocksDB] success,partitionID:%v  old hosts:%v  new hosts:%v oldPeers:%v  newPeers:%v ",
		mp.PartitionID, oldHosts, mp.Hosts, oldPeers, mp.Peers)
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

// TODO is this wrapper necessary?
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

func (mp *MetaPartition) reportMissingReplicas(clusterID string, partitionMissSec, warnInterval int64) {
	mp.Lock()
	defer mp.Unlock()
	for _, replica := range mp.Replicas {
		// reduce the alarm  frequency
		if contains(mp.Hosts, replica.Addr) && replica.isMissing() && mp.shouldReportMissingReplica(replica.Addr, warnInterval) {
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
				clusterID, mp.volName, mp.PartitionID, replica.Addr, partitionMissSec, replica.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range mp.Hosts {
		if mp.isMissingReplica(addr) && mp.shouldReportMissingReplica(addr, warnInterval) {
			msg := fmt.Sprintf("action[reportMissingReplicas],clusterID[%v] volName[%v] partition:%v  on Node:%v  "+
				"miss time  > %v ",
				clusterID, mp.volName, mp.PartitionID, addr, defaultMetaPartitionTimeOutSec)
			Warn(clusterID, msg)
		}
	}
}

func (mp *MetaPartition) replicaCreationTasks(clusterID, volName string) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if addr, task, err := mp.removeIllegalReplica(); err != nil {
		msg = fmt.Sprintf("action[%v],clusterID[%v] metaPartition:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			deleteIllegalReplicaErr, clusterID, mp.PartitionID, addr, err.Error(), mp.Hosts)
		log.LogWarn(msg)
		tasks = append(tasks, task)
	}
	if addrs := mp.missingReplicaAddrs(); addrs != nil {
		msg = fmt.Sprintf("action[missingReplicaAddrs],clusterID[%v] metaPartition:%v  lack replication"+
			" on :%v Hosts:%v",
			clusterID, mp.PartitionID, addrs, mp.Hosts)
		log.LogWarn(msg)
		tasks = append(tasks, mp.createTaskToAddMissingMetaReplica(addrs, volName)...)
	}

	return
}

func (mp *MetaPartition) buildNewMetaPartitionTasks(specifyAddrs []string, peers []proto.Peer, volName string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	hosts := make([]string, 0)
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     peers,
		VolName:     volName,
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

func (mp *MetaPartition) createTaskToAddMissingMetaReplica(addrs []string, volName string) (tasks []*proto.AdminTask) {
	return mp.buildNewMetaPartitionTasks(addrs, mp.Peers, volName)
}

func (mp *MetaPartition) generateOfflineTask(volName string, removePeer proto.Peer, addPeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		return nil, errors.Trace(err)
	}
	req := &proto.MetaPartitionOfflineRequest{PartitionID: mp.PartitionID, VolName: volName, RemovePeer: removePeer, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpOfflineMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func resetMetaPartitionTaskID(t *proto.AdminTask, partitionID uint64) {
	t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, partitionID)
}

func (mp *MetaPartition) createTaskToUpdateMetaReplica(clusterID string, partitionID uint64, end uint64) (t *proto.AdminTask) {
	mr, err := mp.getLeaderMetaReplica()
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
	return time.Now().Unix() - mr.ReportTime > defaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) isActive() (active bool) {
	return mr.metaNode.IsActive && mr.Status != proto.Unavaliable &&
		time.Now().Unix()-mr.ReportTime < defaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) setLastReportTime() {
	mr.ReportTime = time.Now().Unix()
}

func (mr *MetaReplica) updateMetric(mgr *proto.MetaPartitionReport) {
	mr.Status = (int8)(mgr.Status)
	mr.IsLeader = mgr.IsLeader
	mr.setLastReportTime()
}

func (mp *MetaPartition) updateMetricByRaft(mpv *MetaPartitionValue) {
	mp.Start = mpv.Start
	mp.End = mpv.End
	mp.Peers = mpv.Peers
	mp.Hosts = strings.Split(mpv.Hosts, underlineSeparator)

}

func (mp *MetaPartition) postProcessingPartitionCreation(nodeAddr string, c *Cluster) (err error) {
	metaNode, err := c.metaNode(nodeAddr)
	if err != nil {
		return err
	}
	mr := newMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
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
