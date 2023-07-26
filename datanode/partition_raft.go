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

package datanode

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/async"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/holder"
	"github.com/cubefs/cubefs/util/log"
	raftproto "github.com/tiglabs/raft/proto"
)

type extentAction struct {
	extentID uint64
	offset   int64
	size     int64
}

func (a *extentAction) Overlap(o holder.Action) bool {
	other, is := o.(*extentAction)
	return is &&
		a.extentID == other.extentID &&
		a.offset < other.offset+other.size &&
		other.offset < a.offset+a.size
}

type dataPartitionCfg struct {
	VolName       string              `json:"vol_name"`
	ClusterID     string              `json:"cluster_id"`
	PartitionID   uint64              `json:"partition_id"`
	PartitionSize int                 `json:"partition_size"`
	Peers         []proto.Peer        `json:"peers"`
	ReplicaNum    int                 `json:"replica_num"`
	Hosts         []string            `json:"hosts"`
	Learners      []proto.Learner     `json:"learners"`
	NodeID        uint64              `json:"-"`
	RaftStore     raftstore.RaftStore `json:"-"`
	CreationType  int                 `json:"-"`

	VolHAType proto.CrossRegionHAType `json:"vol_ha_type"`

	Mode proto.ConsistencyMode `json:"-"`
}

func (dp *DataPartition) raftPort() (heartbeat, replica int, err error) {
	raftConfig := dp.config.RaftStore.RaftConfig()
	heartbeatAddrSplits := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicaAddrSplits := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrSplits) != 2 {
		err = errors.New("illegal heartbeat address")
		return
	}
	if len(replicaAddrSplits) != 2 {
		err = errors.New("illegal replica address")
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrSplits[1])
	if err != nil {
		return
	}
	replica, err = strconv.Atoi(replicaAddrSplits[1])
	if err != nil {
		return
	}
	return
}

// startRaft start raft instance when data partition start or restore.
func (dp *DataPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
		learners      []raftproto.Learner
	)
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("startRaft(%v)  Raft Panic(%v)", dp.partitionID, r)
			panic(mesg)
		}
	}()

	if heartbeatPort, replicaPort, err = dp.raftPort(); err != nil {
		return
	}
	for _, peer := range dp.config.Peers {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := raftstore.PeerAddress{
			Peer: raftproto.Peer{
				ID: peer.ID,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicaPort:   replicaPort,
		}
		peers = append(peers, rp)
	}
	for _, learner := range dp.config.Learners {
		addLearner := raftproto.Learner{
			ID:         learner.ID,
			PromConfig: &raftproto.PromoteConfig{AutoPromote: learner.PmConfig.AutoProm, PromThreshold: learner.PmConfig.PromThreshold},
		}
		learners = append(learners, addLearner)
	}
	log.LogDebugf("start partition(%v) raft peers: %s path: %s",
		dp.partitionID, peers, dp.path)

	var getStartIndex raftstore.GetStartIndexFunc = func(firstIndex, lastIndex uint64) (startIndex uint64) {
		// Compute index for raft recover
		var applied = dp.applyStatus.Applied()
		defer func() {
			log.LogWarnf("partition(%v) computed start index [startIndex: %v, applied: %v, firstIndex: %v, lastIndex: %v]",
				dp.partitionID, startIndex, applied, firstIndex, lastIndex)
		}()
		if applied >= firstIndex && applied-firstIndex > RaftLogRecoverInAdvance {
			startIndex = applied - RaftLogRecoverInAdvance
			return
		}
		startIndex = firstIndex
		return
	}

	var maxCommitID uint64
	if dp.isNeedFaultCheck() {
		if maxCommitID, err = dp.getMaxCommitID(context.Background()); err != nil {
			return
		}
	}

	var fsm = &raftstore.FunctionalPartitionFsm{
		ApplyFunc:              dp.handleRaftApply,
		ApplyMemberChangeFunc:  dp.handleRaftApplyMemberChange,
		SnapshotFunc:           dp.handleRaftSnapshot,
		AskRollbackFunc:        dp.handleRaftAskRollback,
		ApplySnapshotFunc:      dp.handleRaftApplySnapshot,
		HandleFatalEventFunc:   dp.handleRaftFatalEvent,
		HandleLeaderChangeFunc: dp.handleRaftLeaderChange,
	}

	pc := &raftstore.PartitionConfig{
		ID:                 dp.partitionID,
		Peers:              peers,
		Learners:           learners,
		SM:                 fsm,
		WalPath:            dp.path,
		StartCommit:        maxCommitID,
		GetStartIndex:      getStartIndex,
		WALContinuityCheck: dp.isNeedFaultCheck(),
		WALContinuityFix:   dp.isNeedFaultCheck(),
		Mode:               dp.config.Mode,
		StorageListener: raftstore.NewStorageListenerBuilder().
			ListenStoredEntry(dp.listenStoredRaftLogEntry).
			Build(),
	}
	dp.raftPartition = dp.config.RaftStore.CreatePartition(pc)

	if err = dp.raftPartition.Start(); err != nil {
		return
	}
	go dp.startRaftWorker()
	return
}

func (dp *DataPartition) getMaxCommitID(ctx context.Context) (maxID uint64, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("getMaxCommitID, partition:%v, error:%v", dp.partitionID, err)
		}
	}()
	minReply := dp.getServerFaultCheckQuorum()
	if minReply == 0 {
		log.LogDebugf("start partition(%v), skip get max commit id", dp.partitionID)
		return
	}
	allRemoteCommitID, replyNum := dp.getRemoteReplicaCommitID(ctx)
	if replyNum < minReply {
		err = fmt.Errorf("reply num(%d) not enough to minReply(%d)", replyNum, minReply)
		return
	}
	maxID, _ = dp.findMaxID(allRemoteCommitID)
	log.LogInfof("start partition(%v), maxCommitID(%v)", dp.partitionID, maxID)
	return
}

func (dp *DataPartition) stopRaft() {
	if dp.raftPartition != nil {
		log.LogErrorf("[FATAL] stop raft partition(%v)", dp.partitionID)
		_ = dp.raftPartition.Stop()
	}
	return
}

func (dp *DataPartition) CanRemoveRaftMember(peer proto.Peer) error {
	for _, learner := range dp.config.Learners {
		if learner.ID == peer.ID && learner.Addr == peer.Addr {
			return nil
		}
	}
	downReplicas := dp.config.RaftStore.RaftServer().GetDownReplicas(dp.partitionID)
	hasExsit := false
	for _, p := range dp.config.Peers {
		if p.ID == peer.ID {
			hasExsit = true
			break
		}
	}
	if !hasExsit {
		return nil
	}

	hasDownReplicasExcludePeer := make([]uint64, 0)
	for _, nodeID := range downReplicas {
		if nodeID.NodeID == peer.ID {
			continue
		}
		hasDownReplicasExcludePeer = append(hasDownReplicasExcludePeer, nodeID.NodeID)
	}

	sumReplicas := len(dp.config.Peers) - len(dp.config.Learners)
	if sumReplicas%2 == 1 {
		if sumReplicas-len(hasDownReplicasExcludePeer) > (sumReplicas/2 + 1) {
			return nil
		}
	} else {
		if sumReplicas-len(hasDownReplicasExcludePeer) >= (sumReplicas/2 + 1) {
			return nil
		}
	}

	return fmt.Errorf("hasDownReplicasExcludePeer(%v) too much,so donnot offline(%v)", downReplicas, peer)
}

// startRaftWorker starts the task schedule as follows:
// 1. write the raft applied id into disk.
// 2. collect the applied ids from raft members.
// 3. based on the minimum applied id to cutoff and delete the saved raft log in order to free the disk space.
func (dp *DataPartition) startRaftWorker() {
	raftLogTruncateScheduleTimer := time.NewTimer(time.Second * 1)

	log.LogDebugf("[startSchedule] hello DataPartition schedule")

	for {
		select {
		case <-dp.stopC:
			log.LogDebugf("[startSchedule] stop partition(%v)", dp.partitionID)
			raftLogTruncateScheduleTimer.Stop()
			return

		case extentID := <-dp.stopRaftC:
			dp.stopRaft()
			log.LogErrorf("action[ExtentRepair] stop raft partition(%v)_%v", dp.partitionID, extentID)

		case <-raftLogTruncateScheduleTimer.C:
			dp.scheduleRaftWALTruncate(context.Background())
			raftLogTruncateScheduleTimer.Reset(time.Minute * 1)

		}
	}
}

// startRaftAfterRepair starts the raft after repairing a partition.
// It can only happens after all the extent files are repaired by the leader.
// When the repair is finished, the local dp.partitionSize is same as the leader's dp.partitionSize.
// The repair task can be done in statusUpdateScheduler->runRepair.
func (dp *DataPartition) startRaftAfterRepair() {
	var (
		initPartitionSize, initMaxExtentID uint64
		err                                error
	)
	timer := time.NewTimer(0)
	if !dp.isLeader {
		dp.TinyDeleteRecover = true
	}
	for {
		select {
		case <-timer.C:
			err = nil
			if dp.isLeader { // primary does not need to wait repair
				dp.DataPartitionCreateType = proto.NormalCreateDataPartition
				if err = dp.persist(nil); err != nil {
					log.LogErrorf("Partition(%v) persist metadata failed and try after 5s: %v", dp.partitionID, err)
					timer.Reset(5 * time.Second)
					continue
				}
				if err := dp.startRaft(); err != nil {
					log.LogErrorf("PartitionID(%v) leader start raft err(%v).", dp.partitionID, err)
					timer.Reset(5 * time.Second)
					continue
				}
				log.LogDebugf("PartitionID(%v) leader started.", dp.partitionID)
				return
			}
			ctx := context.Background()
			// wait for dp.replicas to be updated
			relicas := dp.getReplicaClone()
			if len(relicas) == 0 {
				log.LogErrorf("action[startRaftAfterRepair] partition(%v) replicas is nil.", dp.partitionID)
				timer.Reset(5 * time.Second)
				continue
			}
			if initMaxExtentID == 0 || initPartitionSize == 0 {
				initMaxExtentID, initPartitionSize, err = dp.getLeaderMaxExtentIDAndPartitionSize(ctx)
				if err != nil {
					log.LogErrorf("PartitionID(%v) get MaxExtentID  err(%v)", dp.partitionID, err)
					timer.Reset(5 * time.Second)
					continue
				}
			}

			if dp.isPartitionSizeRecover(ctx, initMaxExtentID, initPartitionSize) {
				timer.Reset(5 * time.Second)
				continue
			}

			if dp.isTinyDeleteRecordRecover() {
				timer.Reset(5 * time.Second)
				continue
			}

			// start raft
			dp.DataPartitionCreateType = proto.NormalCreateDataPartition
			if err = dp.persist(nil); err != nil {
				log.LogErrorf("Partition(%v) persist metadata failed and try after 5s: %v", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			if err := dp.startRaft(); err != nil {
				log.LogErrorf("PartitionID(%v) start raft err(%v). Retry after 5s.", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			log.LogInfof("PartitionID(%v) raft started.", dp.partitionID)
			return
		case <-dp.stopC:
			timer.Stop()
			return
		}
	}
}

func (dp *DataPartition) isTinyDeleteRecordRecover() bool {
	if !dp.TinyDeleteRecover {
		return false
	}
	localTinyDeleteFileSize, err := dp.extentStore.LoadTinyDeleteFileOffset()
	if err != nil {
		log.LogErrorf("PartitionID(%v) tiny extent delete record wait snapshot recover, err:%v", dp.partitionID, err)
		return true
	}
	log.LogErrorf("PartitionID(%v) tiny extent delete record wait snapshot recover, local(%v)", dp.partitionID, localTinyDeleteFileSize)
	return true
}

func (dp *DataPartition) isPartitionSizeRecover(ctx context.Context, initMaxExtentID, initPartitionSize uint64) bool {
	// get the partition size from the primary and compare it with the loparal one
	currLeaderPartitionSize, err := dp.getLeaderPartitionSize(ctx, initMaxExtentID)
	if err != nil {
		log.LogErrorf("PartitionID(%v) get leader size err(%v)", dp.partitionID, err)
		return true
	}
	if currLeaderPartitionSize < initPartitionSize {
		initPartitionSize = currLeaderPartitionSize
	}
	localSize := dp.extentStore.StoreSizeExtentID(initMaxExtentID)

	log.LogInfof("startRaftAfterRepair PartitionID(%v) initMaxExtentID(%v) initPartitionSize(%v) currLeaderPartitionSize(%v)"+
		"localSize(%v)", dp.partitionID, initMaxExtentID, initPartitionSize, currLeaderPartitionSize, localSize)

	if initPartitionSize > localSize {
		log.LogErrorf("PartitionID(%v) partition data  wait snapshot recover, leader(%v) local(%v)", dp.partitionID, initPartitionSize, localSize)
		return true
	}
	return false
}

// startRaftAsync dp instance can start without raft, this enables remote request to get the dp basic info
func (dp *DataPartition) startRaftAsync() {
	var err error
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err = dp.startRaft(); err != nil {
				log.LogErrorf("partition(%v) start raft failed: %v", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			return
		case <-dp.stopC:
			return
		}
	}
}

// Add a raft node.
func (dp *DataPartition) addRaftNode(req *proto.AddDataPartitionRaftMemberRequest, index uint64) (isUpdated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = dp.raftPort(); err != nil {
		return
	}

	found := false
	for _, peer := range dp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			found = true
			break
		}
	}
	isUpdated = !found
	if !isUpdated {
		return
	}
	data, _ := json.Marshal(req)
	log.LogInfof("addRaftNode: remove self: partitionID(%v) nodeID(%v) index(%v) data(%v) ",
		req.PartitionId, dp.config.NodeID, index, string(data))
	dp.config.Peers = append(dp.config.Peers, req.AddPeer)
	dp.config.Hosts = append(dp.config.Hosts, req.AddPeer.Addr)
	dp.replicasLock.Lock()
	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	dp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicaPort)
	return
}

// Delete a raft node.
func (dp *DataPartition) removeRaftNode(req *proto.RemoveDataPartitionRaftMemberRequest, index uint64) (isUpdated bool, err error) {
	canRemoveSelf := true
	if dp.config.NodeID == req.RemovePeer.ID {
		if canRemoveSelf, err = dp.canRemoveSelf(); err != nil {
			return
		}
	}

	peerIndex := -1
	data, _ := json.Marshal(req)
	isUpdated = false
	log.LogInfof("Start RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, string(data))
	for i, peer := range dp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			peerIndex = i
			isUpdated = true
			break
		}
	}
	if !isUpdated {
		log.LogInfof("NoUpdate RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
			req.PartitionId, dp.config.NodeID, string(data))
		return
	}
	hostIndex := -1
	for index, host := range dp.config.Hosts {
		if host == req.RemovePeer.Addr {
			hostIndex = index
			break
		}
	}
	if hostIndex != -1 {
		dp.config.Hosts = append(dp.config.Hosts[:hostIndex], dp.config.Hosts[hostIndex+1:]...)
	}

	dp.replicasLock.Lock()
	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()

	dp.config.Peers = append(dp.config.Peers[:peerIndex], dp.config.Peers[peerIndex+1:]...)
	learnerIndex := -1
	for i, learner := range dp.config.Learners {
		if learner.ID == req.RemovePeer.ID && learner.Addr == req.RemovePeer.Addr {
			learnerIndex = i
			break
		}
	}
	if learnerIndex != -1 {
		dp.config.Learners = append(dp.config.Learners[:learnerIndex], dp.config.Learners[learnerIndex+1:]...)
	}
	if dp.config.NodeID == req.RemovePeer.ID && canRemoveSelf {
		if req.ReserveResource {
			dp.Disk().space.DeletePartitionFromCache(dp.partitionID)
		} else {
			dp.raftPartition.Expired()
			dp.Disk().space.ExpiredPartition(dp.partitionID)
		}
		isUpdated = false
	}
	log.LogInfof("Fininsh RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, string(data))

	return
}

// Reset a raft node.
func (dp *DataPartition) resetRaftNode(req *proto.ResetDataPartitionRaftMemberRequest) (isUpdated bool, err error) {
	var (
		newHostIndexes    []int
		newPeerIndexes    []int
		newLearnerIndexes []int
		newHosts          []string
		newPeers          []proto.Peer
		newLearners       []proto.Learner
	)
	data, _ := json.Marshal(req)
	isUpdated = true
	log.LogInfof("Start ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, string(data))

	if len(req.NewPeers) >= len(dp.config.Peers) {
		log.LogInfof("NoUpdate ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
			req.PartitionId, dp.config.NodeID, string(data))
		return
	}
	for _, peer := range req.NewPeers {
		flag := false
		for index, p := range dp.config.Peers {
			if peer.ID == p.ID {
				flag = true
				newPeerIndexes = append(newPeerIndexes, index)
				break
			}
		}
		if !flag {
			isUpdated = false
			log.LogInfof("ResetRaftNode must be old node, PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
				req.PartitionId, dp.config.NodeID, string(data))
			return
		}
	}
	for _, peer := range req.NewPeers {
		flag := false
		for index, host := range dp.config.Hosts {
			if peer.Addr == host {
				flag = true
				newHostIndexes = append(newHostIndexes, index)
				break
			}
		}
		if !flag {
			isUpdated = false
			log.LogInfof("ResetRaftNode must be old node, PartitionID(%v) nodeID(%v) OldHosts(%v)  do RaftLog(%v) ",
				req.PartitionId, dp.config.NodeID, dp.config.Hosts, string(data))
			return
		}
	}
	for _, peer := range req.NewPeers {
		for index, l := range dp.config.Learners {
			if peer.ID == l.ID {
				newLearnerIndexes = append(newLearnerIndexes, index)
				break
			}
		}
	}
	newHosts = make([]string, len(newHostIndexes))
	newPeers = make([]proto.Peer, len(newPeerIndexes))
	newLearners = make([]proto.Learner, len(newLearnerIndexes))
	dp.replicasLock.Lock()
	sort.Ints(newHostIndexes)
	for i, index := range newHostIndexes {
		newHosts[i] = dp.config.Hosts[index]
	}
	dp.config.Hosts = newHosts

	sort.Ints(newPeerIndexes)
	for i, index := range newPeerIndexes {
		newPeers[i] = dp.config.Peers[index]
	}
	dp.config.Peers = newPeers

	sort.Ints(newLearnerIndexes)
	for i, index := range newLearnerIndexes {
		newLearners[i] = dp.config.Learners[index]
	}
	dp.config.Learners = newLearners

	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()
	log.LogInfof("Finish ResetRaftNode  PartitionID(%v) nodeID(%v) newHosts(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, newHosts, string(data))
	return
}

// Add a raft learner.
func (dp *DataPartition) addRaftLearner(req *proto.AddDataPartitionRaftLearnerRequest, index uint64) (isUpdated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = dp.raftPort(); err != nil {
		return
	}

	addPeer := false
	for _, peer := range dp.config.Peers {
		if peer.ID == req.AddLearner.ID {
			addPeer = true
			break
		}
	}
	if !addPeer {
		peer := proto.Peer{ID: req.AddLearner.ID, Addr: req.AddLearner.Addr}
		dp.config.Peers = append(dp.config.Peers, peer)
		dp.config.Hosts = append(dp.config.Hosts, peer.Addr)
	}

	addLearner := false
	for _, learner := range dp.config.Learners {
		if learner.ID == req.AddLearner.ID {
			addLearner = true
			break
		}
	}
	if !addLearner {
		dp.config.Learners = append(dp.config.Learners, req.AddLearner)
	}
	isUpdated = !addPeer || !addLearner
	if !isUpdated {
		return
	}
	log.LogInfof("addRaftLearner: partitionID(%v) nodeID(%v) index(%v) data(%v) ",
		req.PartitionId, dp.config.NodeID, index, req)
	dp.replicasLock.Lock()
	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()
	addr := strings.Split(req.AddLearner.Addr, ":")[0]
	dp.config.RaftStore.AddNodeWithPort(req.AddLearner.ID, addr, heartbeatPort, replicaPort)
	return
}

// Promote a raft learner.
func (dp *DataPartition) promoteRaftLearner(req *proto.PromoteDataPartitionRaftLearnerRequest, index uint64) (isUpdated bool, err error) {
	var promoteIndex int
	for i, learner := range dp.config.Learners {
		if learner.ID == req.PromoteLearner.ID {
			isUpdated = true
			promoteIndex = i
			break
		}
	}
	if isUpdated {
		dp.config.Learners = append(dp.config.Learners[:promoteIndex], dp.config.Learners[promoteIndex+1:]...)
		log.LogInfof("promoteRaftLearner: partitionID(%v) nodeID(%v) index(%v) data(%v), new learners(%v) ",
			req.PartitionId, dp.config.NodeID, index, req, dp.config.Learners)
	}
	return
}

// Update a raft node.
func (dp *DataPartition) updateRaftNode(req *proto.DataPartitionDecommissionRequest, index uint64) (updated bool, err error) {
	log.LogDebugf("[updateRaftNode]: not support.")
	return
}

// LoadAppliedID loads the applied IDs to the memory.
func (dp *DataPartition) LoadAppliedID() (applied uint64, err error) {
	filename := path.Join(dp.Path(), ApplyIndexFile)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadApplyIndex] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyIndex]: ApplyIndex is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &applied); err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

func (dp *DataPartition) AdvanceNextTruncate(id uint64) {
	if snap, success := dp.applyStatus.AdvanceNextTruncate(id); success {
		if log.IsInfoEnabled() {
			log.LogInfof("partition[%v] advance truncate ID [last: %v, next: %v]", dp.partitionID, snap.LastTruncate(), snap.NextTruncate())
		}
	}
}

func (dp *DataPartition) GetAppliedID() (id uint64) {
	return dp.applyStatus.Applied()
}

func (dp *DataPartition) GetPersistedAppliedID() (id uint64) {
	return dp.persistedApplied
}

func (dp *DataPartition) SetConsistencyMode(mode proto.ConsistencyMode) {
	if current := dp.config.Mode; current != mode {
		dp.config.Mode = mode
		if dp.raftPartition != nil {
			dp.raftPartition.SetConsistencyMode(mode)
		}
		if log.IsInfoEnabled() {
			log.LogInfof("SetConsistencyMode: Partition(%v) consistency mode changes [%v -> %v]", dp.partitionID, current, mode)
		}
		if err := dp.persistMetaDataOnly(); err != nil {
			log.LogErrorf("SetConsistencyMode: Partition(%v) persist metadata failed: %v", dp.partitionID, err)
		}
	}
}

func (dp *DataPartition) GetConsistencyMode() proto.ConsistencyMode {
	if dp.raftPartition != nil {
		return dp.raftPartition.GetConsistencyMode()
	}
	return dp.config.Mode
}

func (s *DataNode) parseRaftConfig(cfg *config.Config) (err error) {
	s.raftDir = cfg.GetString(ConfigKeyRaftDir)
	if s.raftDir == "" {
		return fmt.Errorf("bad raftDir config")
	}
	s.raftHeartbeat = cfg.GetString(ConfigKeyRaftHeartbeat)
	s.raftReplica = cfg.GetString(ConfigKeyRaftReplica)
	log.LogDebugf("[parseRaftConfig] load raftDir(%v).", s.raftDir)
	log.LogDebugf("[parseRaftConfig] load raftHearbeat(%v).", s.raftHeartbeat)
	log.LogDebugf("[parseRaftConfig] load raftReplica(%v).", s.raftReplica)
	return
}

func (s *DataNode) startRaftServer(cfg *config.Config) (err error) {
	log.LogInfo("Start: startRaftServer")

	s.parseRaftConfig(cfg)

	constCfg := config.ConstConfig{
		Listen:           s.port,
		RaftHeartbetPort: s.raftHeartbeat,
		RaftReplicaPort:  s.raftReplica,
	}
	var ok = false
	if ok, err = config.CheckOrStoreConstCfg(s.raftDir, config.DefaultConstConfigFile, &constCfg); !ok {
		log.LogErrorf("constCfg check failed %v %v %v %v", s.raftDir, config.DefaultConstConfigFile, constCfg, err)
		return fmt.Errorf("constCfg check failed %v %v %v %v", s.raftDir, config.DefaultConstConfigFile, constCfg, err)
	}

	if _, err = os.Stat(s.raftDir); err != nil {
		if err = os.MkdirAll(s.raftDir, 0755); err != nil {
			err = errors.NewErrorf("create raft server dir: %s", err.Error())
			log.LogErrorf("action[startRaftServer] cannot start raft server err(%v)", err)
			return
		}
	}

	heartbeatPort, err := strconv.Atoi(s.raftHeartbeat)
	if err != nil {
		err = errors.NewErrorf("Raft heartbeat port configuration error: %s", err.Error())
		return
	}
	replicatePort, err := strconv.Atoi(s.raftReplica)
	if err != nil {
		err = errors.NewErrorf("Raft replica port configuration error: %s", err.Error())
		return
	}

	raftConf := &raftstore.Config{
		NodeID:            s.nodeID,
		RaftPath:          s.raftDir,
		TickInterval:      s.tickInterval,
		IPAddr:            LocalIP,
		HeartbeatPort:     heartbeatPort,
		ReplicaPort:       replicatePort,
		NumOfLogsToRetain: DefaultRaftLogsToRetain,
	}
	s.raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		err = errors.NewErrorf("new raftStore: %s", err.Error())
		log.LogErrorf("action[startRaftServer] cannot start raft server err(%v)", err)
	}

	return
}

func (s *DataNode) stopRaftServer() {
	if s.raftStore != nil {
		s.raftStore.Stop()
	}
}

// NewPacketToBroadcastMinAppliedID returns a new packet to broadcast the min applied ID.
func NewPacketToBroadcastMinAppliedID(ctx context.Context, partitionID uint64, minAppliedID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpBroadcastMinAppliedID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data[0:8], minAppliedID)
	p.Size = uint32(len(p.Data))
	p.SetCtx(ctx)
	return
}

// NewPacketToGetAppliedID returns a new packet to get the applied ID.
func NewPacketToGetAppliedID(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetAppliedId
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}

// NewPacketToGetPersistedAppliedID returns a new packet to get the applied ID.
func NewPacketToGetPersistedAppliedID(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetPersistedAppliedId
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}

// NewPacketToGetPartitionSize returns a new packet to get the partition size.
func NewPacketToGetPartitionSize(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetPartitionSize
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}

// NewPacketToGetMaxExtentIDAndPartitionSIze returns a new packet to get the partition size.
func NewPacketToGetMaxExtentIDAndPartitionSIze(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetMaxExtentIDAndPartitionSize
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}

func (dp *DataPartition) findMinID(allIDs map[string]uint64) (minID uint64, host string) {
	minID = math.MaxUint64
	for k, v := range allIDs {
		if v < minID {
			minID = v
			host = k
		}
	}
	return minID, host
}

func (dp *DataPartition) findMaxID(allIDs map[string]uint64) (maxID uint64, host string) {
	for k, v := range allIDs {
		if v > maxID {
			maxID = v
			host = k
		}
	}
	return maxID, host
}

// Get the partition size from the leader.
func (dp *DataPartition) getLeaderPartitionSize(ctx context.Context, maxExtentID uint64) (size uint64, err error) {
	var (
		conn *net.TCPConn
	)

	p := NewPacketToGetPartitionSize(ctx, dp.partitionID)
	p.ExtentID = maxExtentID
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		err = errors.Trace(err, " partition(%v) get LeaderHost failed ", dp.partitionID)
		return
	}
	target := replicas[0]
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Trace(err, " partition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "partition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	err = p.ReadFromConn(conn, 60)
	if err != nil {
		err = errors.Trace(err, "partition(%v) read from host(%v)", dp.partitionID, target)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.Trace(err, "partition(%v) result code not ok(%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	size = binary.BigEndian.Uint64(p.Data[0:8])
	log.LogInfof("partition(%v) MaxExtentID(%v) size(%v)", dp.partitionID, maxExtentID, size)

	return
}

// Get the MaxExtentID partition  from the leader.
func (dp *DataPartition) getLeaderMaxExtentIDAndPartitionSize(ctx context.Context) (maxExtentID, PartitionSize uint64, err error) {
	var (
		conn *net.TCPConn
	)

	p := NewPacketToGetMaxExtentIDAndPartitionSIze(ctx, dp.partitionID)
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		err = errors.Trace(err, " partition(%v) get Leader failed ", dp.partitionID)
		return
	}
	target := replicas[0]
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Trace(err, " partition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "partition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	err = p.ReadFromConn(conn, 60)
	if err != nil {
		err = errors.Trace(err, "partition(%v) read from host(%v)", dp.partitionID, target)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.Trace(err, "partition(%v) result code not ok(%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	maxExtentID = binary.BigEndian.Uint64(p.Data[0:8])
	PartitionSize = binary.BigEndian.Uint64(p.Data[8:16])

	log.LogInfof("partition(%v) maxExtentID(%v) PartitionSize(%v) on leader", dp.partitionID, maxExtentID, PartitionSize)

	return
}

func (dp *DataPartition) broadcastRaftWALNextTruncateID(ctx context.Context, nextTruncateID uint64) (err error) {
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		err = errors.Trace(err, " partition(%v) get replicas failed,replicas is nil. ", dp.partitionID)
		log.LogErrorf(err.Error())
		return
	}
	var wg = new(sync.WaitGroup)
	for i := 0; i < len(replicas); i++ {
		target := replicas[i]
		if dp.IsLocalAddress(target) {
			dp.AdvanceNextTruncate(nextTruncateID)
			continue
		}
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			p := NewPacketToBroadcastMinAppliedID(ctx, dp.partitionID, nextTruncateID)
			var conn *net.TCPConn
			conn, err = gConnPool.GetConnect(target)
			if err != nil {
				return
			}
			defer gConnPool.PutConnect(conn, true)
			err = p.WriteToConn(conn, proto.WriteDeadlineTime)
			if err != nil {
				return
			}
			err = p.ReadFromConn(conn, 60)
			if err != nil {
				return
			}
			gConnPool.PutConnect(conn, true)
			if log.IsInfoEnabled() {
				log.LogInfof("partition[%v] broadcast raft WAL next truncate ID [%v] to replica[%v]", dp.partitionID, nextTruncateID, target)
			}
		}(target)
	}
	wg.Wait()
	return
}

// Get all replica commit ids
func (dp *DataPartition) getRemoteReplicaCommitID(ctx context.Context) (commitIDMap map[string]uint64, replyNum uint8) {
	hosts := dp.getReplicaClone()
	if len(hosts) == 0 {
		log.LogErrorf("action[getRemoteReplicaCommitID] partition(%v) replicas is nil.", dp.partitionID)
		return
	}
	commitIDMap = make(map[string]uint64, len(hosts))
	errSlice := make(map[string]error)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)
	for _, host := range hosts {
		if dp.IsLocalAddress(host) {
			continue
		}
		wg.Add(1)
		go func(curAddr string) {
			var commitID uint64
			var err error
			commitID, err = dp.getRemoteCommitID(curAddr)
			if commitID == 0 {
				log.LogDebugf("action[getRemoteReplicaCommitID] partition(%v) replicaHost(%v) commitID=0",
					dp.partitionID, curAddr)
			}
			ok := false
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				log.LogErrorf("action[getRemoteReplicaCommitID] partition(%v) failed, err:%v", dp.partitionID, err)
				errSlice[curAddr] = err
			} else {
				commitIDMap[curAddr] = commitID
				ok = true
			}
			log.LogDebugf("action[getRemoteReplicaCommitID]: get commit id[%v] ok[%v] from host[%v], pid[%v]", commitID, ok, curAddr, dp.partitionID)
			wg.Done()
		}(host)
	}
	wg.Wait()
	replyNum = uint8(len(hosts) - 1 - len(errSlice))
	log.LogDebugf("action[getRemoteReplicaCommitID]: get commit id from hosts[%v], pid[%v]", hosts, dp.partitionID)
	return
}

// Get all replica applied ids
func (dp *DataPartition) getPersistedAppliedIDFromReplicas(ctx context.Context, hosts []string, timeoutNs, readTimeoutNs int64) (appliedIDs map[string]uint64) {
	if len(hosts) == 0 {
		log.LogErrorf("action[getAllReplicaAppliedID] partition(%v) replicas is nil.", dp.partitionID)
		return
	}
	appliedIDs = make(map[string]uint64)
	var futures = make(map[string]*async.Future) // host -> future
	for _, host := range hosts {
		if dp.IsLocalAddress(host) {
			appliedIDs[host] = dp.persistedApplied
			continue
		}
		var future = async.NewFuture()
		go func(future *async.Future, curAddr string) {
			var appliedID uint64
			var err error
			appliedID, err = dp.getRemotePersistedAppliedID(ctx, curAddr, timeoutNs, readTimeoutNs)
			if err == nil {
				future.Respond(appliedID, nil)
				return
			}
			if !strings.Contains(err.Error(), repl.ErrorUnknownOp.Error()) {
				future.Respond(nil, err)
				return
			}
			// TODO: 以下为版本过度而设计的兼容逻辑，由于获取PersistedAppliedID为新增接口，为避免在灰度过程中无法正常进行WAL的Truncate调度。 请在下一个版本移除。
			if log.IsWarnEnabled() {
				log.LogWarnf("partition[%v] get persisted applied ID failed cause remote [%v] respond result code [%v], try to get current applied ID.",
					dp.partitionID, curAddr, repl.ErrorUnknownOp.Error())
			}
			appliedID, err = dp.getRemoteAppliedID(ctx, curAddr, timeoutNs, readTimeoutNs)
			if err != nil {
				future.Respond(nil, err)
				return
			}
			future.Respond(appliedID, nil)
		}(future, host)
		futures[host] = future
	}

	for host, future := range futures {
		resp, err := future.Response()
		if err != nil {
			log.LogWarnf("partition[%v] get applied ID from remote[%v] failed: %v", dp.partitionID, host, err)
			continue
		}
		applied := resp.(uint64)
		appliedIDs[host] = applied
		if log.IsDebugEnabled() {
			log.LogDebugf("partition[%v]: get applied ID [%v] from remote[%v] ", dp.partitionID, host, applied)
		}
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("partition[%v] get applied ID from all replications, hosts[%v], respond[%v]", dp.partitionID, hosts, appliedIDs)
	}
	return
}

// Get target members' commit id
func (dp *DataPartition) getRemoteCommitID(target string) (commitID uint64, err error) {
	if dp.disk == nil || dp.disk.space == nil || dp.disk.space.dataNode == nil {
		err = fmt.Errorf("action[getRemoteCommitID] data node[%v] not ready", target)
		return
	}
	profPort := dp.disk.space.dataNode.httpPort
	httpAddr := fmt.Sprintf("%v:%v", strings.Split(target, ":")[0], profPort)
	dataClient := data.NewDataHttpClient(httpAddr, false)
	var hardState proto.HardState
	hardState, err = dataClient.GetPartitionRaftHardState(dp.partitionID)
	if err != nil {
		err = fmt.Errorf("action[getRemoteCommitID] datanode[%v] get partition failed, err:%v", target, err)
		return
	}
	commitID = hardState.Commit
	log.LogDebugf("[getRemoteCommitID] partition(%v) remoteCommitID(%v)", dp.partitionID, commitID)
	return
}

func (dp *DataPartition) getLocalAppliedID() {

}

// Get target members' applied id
func (dp *DataPartition) getRemoteAppliedID(ctx context.Context, target string, timeoutNs, readTimeoutNs int64) (appliedID uint64, err error) {
	p := NewPacketToGetAppliedID(ctx, dp.partitionID)
	if err = dp.sendTcpPacket(target, p, timeoutNs, readTimeoutNs); err != nil {
		return
	}
	appliedID = binary.BigEndian.Uint64(p.Data[0:8])
	log.LogDebugf("[getRemoteAppliedID] partition(%v) remoteAppliedID(%v)", dp.partitionID, appliedID)
	return
}

// Get target members' persisted applied id
func (dp *DataPartition) getRemotePersistedAppliedID(ctx context.Context, target string, timeoutNs, readTimeoutNs int64) (appliedID uint64, err error) {
	p := NewPacketToGetPersistedAppliedID(ctx, dp.partitionID)
	if err = dp.sendTcpPacket(target, p, timeoutNs, readTimeoutNs); err != nil {
		return
	}
	appliedID = binary.BigEndian.Uint64(p.Data[0:8])
	log.LogDebugf("[getRemotePersistedAppliedID] partition(%v) remoteAppliedID(%v)", dp.partitionID, appliedID)
	return
}

func (dp *DataPartition) sendTcpPacket(target string, p *repl.Packet, timeout, readTimeout int64) (err error) {
	var conn *net.TCPConn
	start := time.Now().UnixNano()
	defer func() {
		if err != nil {
			err = fmt.Errorf(p.LogMessage(p.GetOpMsg(), target, start, err))
			log.LogErrorf(err.Error())
		}
	}()

	conn, err = gConnPool.GetConnect(target)
	if err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConnNs(conn, timeout) // write command to the remote host
	if err != nil {
		return
	}
	err = p.ReadFromConnNs(conn, readTimeout)
	if err != nil {
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("partition(%v) result code not ok(%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	log.LogDebugf("[sendTcpPacket] partition(%v)", dp.partitionID)
	return
}

// Get all members' applied ids and find the minimum one
func (dp *DataPartition) scheduleRaftWALTruncate(ctx context.Context) {
	var minPersistedAppliedID uint64

	if !dp.IsRaftStarted() {
		return
	}

	// Get the applied id by the leader
	_, isLeader := dp.IsRaftLeader()
	if !isLeader {
		return
	}

	var replicas = dp.getReplicaClone()

	// Check replicas number
	var replicaNum = dp.config.ReplicaNum
	if len(replicas) != replicaNum {
		if log.IsDebugEnabled() {
			log.LogDebugf("partition[%v] skips schedule raft WAL truncate cause replicas number illegal",
				dp.partitionID)
		}
		return
	}
	var raftStatus = dp.raftPartition.Status()
	if len(raftStatus.Replicas) != replicaNum {
		if log.IsDebugEnabled() {
			log.LogDebugf("partition[%v] skips schedule raft WAL truncate cause replicas number illegal",
				dp.partitionID)
		}
		return
	}
	for id, replica := range raftStatus.Replicas {
		if strings.Contains(replica.State, "ReplicaStateSnapshot") {
			if log.IsDebugEnabled() {
				log.LogDebugf("partition[%v] skips schedule raft WAL truncate cause found replica [%v] in snapshot state",
					dp.partitionID, id)
			}
			return
		}
	}

	var persistedAppliedIDs = dp.getPersistedAppliedIDFromReplicas(ctx, replicas, proto.WriteDeadlineTime*1e9, 60*1e9)
	if len(persistedAppliedIDs) == 0 {
		log.LogDebugf("[scheduleRaftWALTruncate] PartitionID(%v) Get appliedId failed!", dp.partitionID)
		return
	}
	if len(persistedAppliedIDs) == len(replicas) { // update dp.minPersistedAppliedID when every member had replied
		minPersistedAppliedID, _ = dp.findMinID(persistedAppliedIDs)
		if log.IsInfoEnabled() {
			log.LogInfof("partition[%v] computed min persisted applied ID [%v] from replication group [%v]",
				dp.partitionID, minPersistedAppliedID, persistedAppliedIDs)
		}
		dp.broadcastRaftWALNextTruncateID(ctx, minPersistedAppliedID)
	}
	return
}

func (dp *DataPartition) isNeedFaultCheck() bool {
	return dp.needServerFaultCheck
}

func (dp *DataPartition) setNeedFaultCheck(need bool) {
	dp.needServerFaultCheck = need
}

func (dp *DataPartition) getServerFaultCheckQuorum() uint8 {
	switch dp.serverFaultCheckLevel {
	case CheckAllCommitID:
		return uint8(len(dp.replicas)) - 1
	case CheckQuorumCommitID:
		return uint8(len(dp.replicas) / 2)
	default:
		return 0
	}
}

func (dp *DataPartition) listenStoredRaftLogEntry(entry *raftproto.Entry) {
	var command []byte = nil
	switch entry.Type {
	case raftproto.EntryNormal:
		if len(entry.Data) > 0 {
			command = entry.Data
		}

	case raftproto.EntryRollback:
		rollback := new(raftproto.Rollback)
		rollback.Decode(entry.Data)
		if len(rollback.Data) > 0 {
			command = rollback.Data
		}

	default:
	}

	if command != nil && len(command) > 0 {
		if opItem, err := UnmarshalRandWriteRaftLog(entry.Data); err == nil && opItem.opcode == proto.OpRandomWrite {
			dp.actionHolder.Register(entry.Index, &extentAction{
				extentID: opItem.extentID,
				offset:   opItem.offset,
				size:     opItem.size,
			})
		}
	}
}

func (dp *DataPartition) checkAndWaitForPendingActionApplied(extentID uint64, offset, size int64) (err error) {
	var ctx, _ = context.WithTimeout(context.Background(), time.Second*3)
	err = dp.actionHolder.Wait(ctx, &extentAction{
		extentID: extentID,
		offset:   offset,
		size:     size,
	})
	return
}
