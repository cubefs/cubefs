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
	"encoding/binary"
	"encoding/json"
	"fmt"
	syslog "log"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/datanode/storage"

	"github.com/cubefs/cubefs/datanode/repl"
	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type dataPartitionCfg struct {
	VolName                  string              `json:"vol_name"`
	ClusterID                string              `json:"cluster_id"`
	PartitionID              uint64              `json:"partition_id"`
	PartitionSize            int                 `json:"partition_size"`
	PartitionType            int                 `json:"partition_type"`
	Peers                    []proto.Peer        `json:"peers"`
	Hosts                    []string            `json:"hosts"`
	NodeID                   uint64              `json:"-"`
	RaftStore                raftstore.RaftStore `json:"-"`
	ReplicaNum               int
	VerSeq                   uint64 `json:"ver_seq"`
	CreateType               int
	Forbidden                bool
	DpRepairBlockSize        uint64
	IsEnableSnapshot         bool
	ForbidWriteOpOfProtoVer0 bool
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

// StartRaft start raft instance when data partition start or restore.
func (dp *DataPartition) StartRaft(isLoad bool) (err error) {
	begin := time.Now()
	defer func() {
		log.LogInfof("[StartRaft] load dp(%v) start raft using time(%v), slow(%v)", dp.info(), time.Since(begin), time.Since(begin) > 1*time.Second)
	}()

	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
	)
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("StartRaft(%v)  Raft Panic (%v)", dp.partitionID, r)
			log.LogError(mesg)
			if isLoad {
				err = errors.New(mesg)
			} else {
				log.LogFlush()
				panic(mesg)
			}
		}
	}()

	if heartbeatPort, replicaPort, err = dp.raftPort(); err != nil {
		return
	}
	for _, peer := range dp.config.Peers {
		if dp.dataNode.raftPartitionCanUsingDifferentPort {
			if peerHeartbeatPort, perr := strconv.Atoi(peer.HeartbeatPort); perr == nil {
				heartbeatPort = peerHeartbeatPort
			}
			if peerReplicaPort, perr := strconv.Atoi(peer.ReplicaPort); perr == nil {
				replicaPort = peerReplicaPort
			}
		}

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
	log.LogDebugf("start partition(%v) raft peers: %s path: %s applyid:%v",
		dp.partitionID, peers, dp.path, dp.appliedID)
	pc := &raftstore.PartitionConfig{
		ID:      uint64(dp.partitionID),
		Applied: dp.appliedID,
		Peers:   peers,
		SM:      dp,
		WalPath: dp.path,
	}

	dp.raftPartition, err = dp.config.RaftStore.CreatePartition(pc)
	if err == nil {
		dp.ForceSetRaftRunning()
		dp.ForceSetDataPartitionToFinishLoad()
	}
	return
}

func (dp *DataPartition) raftStopped() bool {
	return atomic.LoadInt32(&dp.raftStatus) == RaftStatusStopped
}

func (dp *DataPartition) stopRaft() {
	begin := time.Now()
	defer func() {
		log.LogInfof("[stopRaft] dp(%v) stop raft using time(%v)", dp.info(), time.Since(begin))
	}()
	if atomic.CompareAndSwapInt32(&dp.raftStatus, RaftStatusRunning, RaftStatusStopped) {
		msg := fmt.Sprintf("stop raft partition(%v)", dp.info())
		log.LogWarnf("%v", msg)
		auditlog.LogDataNodeOp("DataPartitionStopRaft", msg, nil)
		dp.raftPartition.Stop()
	}
}

func (dp *DataPartition) CanRemoveRaftMember(peer proto.Peer, force bool) error {
	downReplicas := dp.config.RaftStore.RaftServer().GetDownReplicas(dp.partitionID)
	hasExsit := false
	for _, p := range dp.config.Peers {
		if p.ID == peer.ID {
			hasExsit = true
			break
		}
	}
	if !hasExsit {
		log.LogInfof("action[CanRemoveRaftMember] replicaNum %v peers %v, peer %v not found", dp.replicaNum, len(dp.config.Peers), peer)
		return nil
	}

	hasDownReplicasExcludePeer := make([]uint64, 0)
	for _, nodeID := range downReplicas {
		if nodeID.NodeID == peer.ID {
			continue
		}
		// check nodeID is valid
		hasDownReplicasExcludePeer = append(hasDownReplicasExcludePeer, nodeID.NodeID)
	}

	log.LogInfof("action[CanRemoveRaftMember] dp %v replicaNum %v peers %v", dp.partitionID, dp.replicaNum, len(dp.config.Peers))
	if force {
		return nil
	}

	sumReplicas := len(dp.config.Peers)
	if sumReplicas%2 == 1 {
		if sumReplicas-len(hasDownReplicasExcludePeer) > (sumReplicas/2 + 1) {
			return nil
		}
	} else {
		if sumReplicas-len(hasDownReplicasExcludePeer) >= (sumReplicas/2 + 1) {
			return nil
		}
	}

	return fmt.Errorf("hasDownReplicasExcludePeer(%v) too much,so donnot offline (%v)", downReplicas, peer)
}

// StartRaftLoggingSchedule starts the task schedule as follows:
// 1. write the raft applied id into disk.
// 2. collect the applied ids from raft members.
// 3. based on the minimum applied id to cutoff and delete the saved raft log in order to free the disk space.
func (dp *DataPartition) StartRaftLoggingSchedule() {
	getAppliedIDTimer := time.NewTimer(time.Second * 1)
	truncateRaftLogTimer := time.NewTimer(time.Minute * 10)
	storeAppliedIDTimer := time.NewTimer(time.Second * 10)

	log.LogDebugf("[startSchedule] hello DataPartition schedule")

	for {
		select {
		case <-dp.stopC:
			log.LogDebugf("[startSchedule] stop partition(%v)", dp.partitionID)
			getAppliedIDTimer.Stop()
			truncateRaftLogTimer.Stop()
			storeAppliedIDTimer.Stop()
			return

		case extentID := <-dp.stopRaftC:
			dp.stopRaft()
			log.LogErrorf("action[ExtentRepair] stop raft partition(%v)_%v", dp.partitionID, extentID)

		case <-getAppliedIDTimer.C:
			if !dp.raftStopped() {
				dp.updateMaxMinAppliedID()
			}
			getAppliedIDTimer.Reset(time.Minute * 1)

		case <-truncateRaftLogTimer.C:
			if dp.raftStopped() {
				break
			}
			log.LogDebugf("[StartRaftLoggingSchedule] partition [%v] minAppliedID %v lastTruncateID %v",
				dp.partitionID, dp.minAppliedID, dp.lastTruncateID)
			if dp.minAppliedID > dp.lastTruncateID { // Has changed
				appliedID := atomic.LoadUint64(&dp.appliedID)
				if err := dp.storeAppliedID(appliedID); err != nil {
					log.LogErrorf("[StartRaftLoggingSchedule] partition [%v] persist applied ID [%v] during scheduled truncate raft log failed: %v", dp.partitionID, appliedID, err)
					truncateRaftLogTimer.Reset(time.Minute)
					dp.checkIsDiskError(err, WriteFlag)
					continue
				}
				truncIndex := dp.minAppliedID
				if truncIndex > appliedID {
					truncIndex = appliedID
				}
				dp.raftPartition.Truncate(truncIndex)
				dp.lastTruncateID = truncIndex
				if err := dp.PersistMetadata(); err != nil {
					log.LogErrorf("[StartRaftLoggingSchedule] partition [%v] persist metadata during scheduled truncate raft log failed: %v", dp.partitionID, err)
					truncateRaftLogTimer.Reset(time.Minute)
					continue
				}
				log.LogInfof("[StartRaftLoggingSchedule] partition [%v] scheduled truncate raft log"+
					" [minAppliedID %v, applied: %v, truncated: %v]", dp.partitionID, dp.minAppliedID, appliedID, dp.minAppliedID)
			}
			truncateRaftLogTimer.Reset(time.Minute)

		case <-storeAppliedIDTimer.C:
			appliedID := atomic.LoadUint64(&dp.appliedID)
			log.LogDebugf("[StartRaftLoggingSchedule] partition [%v] persist applied id(%v)", dp.partitionID, appliedID)
			if err := dp.storeAppliedID(appliedID); err != nil {
				log.LogErrorf("partition [%v] scheduled persist applied ID [%v] failed: %v", dp.partitionID, appliedID, err)
				dp.checkIsDiskError(err, WriteFlag)
			}
			storeAppliedIDTimer.Reset(time.Second * 10)
		case req := <-dp.PersistApplyIdChan:
			appliedID := atomic.LoadUint64(&dp.appliedID)
			log.LogDebugf("[StartRaftLoggingSchedule] partition [%v] persist applied id(%v) immediately", dp.partitionID, appliedID)
			if err := dp.storeAppliedID(appliedID); err != nil {
				log.LogErrorf("partition [%v] scheduled persist applied ID [%v] immediately failed: %v", dp.partitionID, appliedID, err)
				dp.checkIsDiskError(err, WriteFlag)
			}
			req.done <- struct{}{}
		}
	}
}

func compareExtentsBySize(toCompareExtents, baseExtents []*storage.ExtentInfo) bool {
	for _, extent := range toCompareExtents {
		found := false
		for _, base := range baseExtents {
			if base.Size == extent.Size {
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

// StartRaftAfterRepair starts the raft after repairing a partition.
// It can only happens after all the extent files are repaired by the leader.
// When the repair is finished, the local dp.partitionSize is same as the leader's dp.partitionSize.
// The repair task can be done in statusUpdateScheduler->LaunchRepair.
func (dp *DataPartition) StartRaftAfterRepair(isLoad bool) {
	log.LogDebugf("StartRaftAfterRepair enter")
	var (
		initPartitionSize, initMaxExtentID uint64
		currLeaderPartitionSize            uint64
		currLeaderRealUsedSize             uint64
		currLeaderPartitionNormalExtents   []*storage.ExtentInfo
		localPartitionNormalExtents        []*storage.ExtentInfo
		err                                error
	)
	timer := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-timer.C:
			err = nil
			if dp.isLeader { // primary does not need to wait repair
				if err := dp.StartRaft(isLoad); err != nil {
					log.LogErrorf("PartitionID(%v) leader start raft err(%v).", dp.partitionID, err)
					continue
				}
				log.LogDebugf("PartitionID(%v) leader started.", dp.partitionID)
				return
			}

			if dp.stopRecover && dp.isDecommissionRecovering() {
				log.LogDebugf("action[StartRaftAfterRepair] PartitionID(%v) receive stop signal.", dp.partitionID)
				continue
			}

			// wait for dp.replicas to be updated
			if dp.getReplicaLen() == 0 {
				continue
			}
			if initMaxExtentID == 0 || initPartitionSize == 0 {
				initMaxExtentID, initPartitionSize, err = dp.getLeaderMaxExtentIDAndPartitionSize()
			}

			if err != nil {
				log.LogErrorf("action[StartRaftAfterRepair] PartitionID(%v) get MaxExtentID  err(%v)", dp.partitionID, err)
				continue
			}

			// get the partition size from the primary and compare it with the loparal one
			currLeaderPartitionSize, currLeaderRealUsedSize, err = dp.getLeaderPartitionSize(initMaxExtentID)
			if err != nil {
				log.LogErrorf("action[StartRaftAfterRepair] PartitionID(%v) get leader size err(%v)", dp.partitionID, err)
				continue
			}

			dp.leaderSize = int(currLeaderRealUsedSize)

			if currLeaderPartitionSize < initPartitionSize {
				initPartitionSize = currLeaderPartitionSize
			}
			localSize := dp.extentStore.StoreSizeExtentID(initMaxExtentID)
			if initPartitionSize == 0 {
				dp.decommissionRepairProgress = 0
			} else {
				dp.decommissionRepairProgress = float64(localSize) / float64(initPartitionSize)
			}
			log.LogInfof("action[StartRaftAfterRepair] PartitionID(%v) initMaxExtentID(%v) initPartitionSize(%v) currLeaderPartitionSize(%v)"+
				"localSize(%v)decommissionRepairProgress(%v)", dp.partitionID, initMaxExtentID, initPartitionSize, currLeaderPartitionSize, localSize, dp.decommissionRepairProgress)

			if initPartitionSize > localSize {
				log.LogWarnf("action[StartRaftAfterRepair] PartitionID(%v) leader size(%v) local size(%v) wait snapshot recover", dp.partitionID, initPartitionSize, localSize)
				continue
			}

			currLeaderPartitionNormalExtents, err = dp.getLeaderPartitionNormalExtentInfo()
			if err != nil {
				log.LogErrorf("action[StartRaftAfterRepair] PartitionID(%v) get leader normal extents err(%v)", dp.partitionID, err)
				continue
			}
			localPartitionNormalExtents, _, err = dp.getLocalExtentInfo(proto.NormalExtentType, nil)
			if err != nil {
				log.LogErrorf("action[StartRaftAfterRepair] PartitionID(%v) get local normal extents err(%v)", dp.partitionID, err)
				continue
			}

			if !compareExtentsBySize(currLeaderPartitionNormalExtents, localPartitionNormalExtents) {
				log.LogWarnf("action[StartRaftAfterRepair] PartitionID(%v) leader normal extent incomplete match local normal extent, wait snapshot recover", dp.partitionID)
				continue
			}

			if err := dp.StartRaft(isLoad); err != nil {
				log.LogErrorf("action[StartRaftAfterRepair] dp(%v) start raft err(%v)", dp.info(), err)
				dp.DataPartitionCreateType = proto.NormalCreateDataPartition
				dp.decommissionRepairProgress = float64(1)
				dp.PersistMetadata()
				return
			}
			// if dataNode recv message for deleting replica before startRaft, raft for this replica will keep running without
			// root path of dp
			if dp.dataNode.space.Partition(dp.partitionID) == nil {
				log.LogWarnf("action[StartRaftAfterRepair] PartitionID(%v) is deleted, stop raft! ", dp.info())
				dp.stopRaft()
				return
			}

			// start raft
			dp.DataPartitionCreateType = proto.NormalCreateDataPartition
			log.LogInfof("action[StartRaftAfterRepair] PartitionID(%v) change to NormalCreateDataPartition",
				dp.partitionID)
			dp.decommissionRepairProgress = float64(1)
			dp.PersistMetadata()
			log.LogInfof("action[StartRaftAfterRepair] PartitionID(%v) raft started!", dp.partitionID)
			return
		case <-dp.stopC:
			log.LogDebugf("action[StartRaftAfterRepair] PartitionID(%v) receive dp stop signal!!.", dp.partitionID)
			timer.Stop()
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
	if dp.dataNode.raftPartitionCanUsingDifferentPort {
		if peerHeartbeatPort, perr := strconv.Atoi(req.AddPeer.HeartbeatPort); perr == nil {
			heartbeatPort = peerHeartbeatPort
		}
		if peerReplicaPort, perr := strconv.Atoi(req.AddPeer.ReplicaPort); perr == nil {
			replicaPort = peerReplicaPort
		}
	}

	if req.EnableSetRepairingStatus {
		dp.isRepairing = req.RepairingStatus
		log.LogWarnf("action[addRaftNode] %v set repairingStatus %v", dp.partitionID, req.RepairingStatus)
	}

	log.LogInfof("action[addRaftNode] add raft node peer [%v]", req.AddPeer)
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
	log.LogInfof("addRaftNode: partitionID(%v) nodeID(%v) index(%v) data(%v) ",
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
	//var canRemoveSelf bool
	//if canRemoveSelf, err = dp.canRemoveSelf(); err != nil {
	//	return
	//}
	peerIndex := -1
	data, _ := json.Marshal(req)
	isUpdated = false
	log.LogInfof("Start RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, dp.config.NodeID, string(data))
	for i, peer := range dp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			peerIndex = i
			isUpdated = true
			break
		}
	}
	if !isUpdated {
		log.LogInfof("NoUpdate RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
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
	dp.config.Peers = append(dp.config.Peers[:peerIndex], dp.config.Peers[peerIndex+1:]...)
	// if ApplyMemberChange is timeout, master will keep old hosts. when meta node executing
	// delete inode operation, will produce amount of dp not found error messages
	// if dp.config.NodeID == req.RemovePeer.ID && !dp.isLoadingDataPartition && canRemoveSelf {
	//	dp.raftPartition.Delete()
	//	dp.Disk().space.DeletePartition(dp.partitionID)
	//	isUpdated = false
	//}
	// update dp replicas after removing a raft node
	if isUpdated {
		dp.replicasLock.Lock()
		dp.replicas = make([]string, len(dp.config.Hosts))
		copy(dp.replicas, dp.config.Hosts)
		dp.replicasLock.Unlock()
	}
	log.LogInfof("Finish RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, dp.config.NodeID, string(data))

	return
}

func (dp *DataPartition) storeAppliedID(applyIndex uint64) (err error) {
	filename := path.Join(dp.Path(), TempApplyIndexFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		fp.Close()
		os.Remove(filename)
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", applyIndex)); err != nil {
		return
	}
	fp.Sync()
	err = os.Rename(filename, path.Join(dp.Path(), ApplyIndexFile))
	return
}

// LoadAppliedID loads the applied IDs to the memory.
func (dp *DataPartition) LoadAppliedID() (err error) {
	begin := time.Now()
	defer func() {
		log.LogInfof("[LoadAppliedID] load dp(%v) load applied id(%v) using time(%v)", dp.partitionID,
			dp.appliedID, time.Since(begin))
	}()
	filename := path.Join(dp.Path(), ApplyIndexFile)
	if _, err = os.Stat(filename); err != nil {
		return
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		err = errors.NewErrorf("[loadApplyIndex] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyIndex]: ApplyIndex is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &dp.appliedID); err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	dp.extentStore.ApplyId = dp.appliedID
	return
}

func (dp *DataPartition) SetMinAppliedID(id uint64) {
	dp.minAppliedID = id
}

func (dp *DataPartition) GetAppliedID() (id uint64) {
	return dp.appliedID
}

func (s *DataNode) parseRaftConfig(cfg *config.Config) (err error) {
	s.raftDir = cfg.GetString(ConfigKeyRaftDir)
	if s.raftDir == "" {
		return fmt.Errorf("bad raftDir config")
	}
	s.tickInterval = int(cfg.GetFloat(CfgTickInterval))
	s.raftHeartbeat = cfg.GetString(ConfigKeyRaftHeartbeat)
	s.raftReplica = cfg.GetString(ConfigKeyRaftReplica)
	s.raftRecvBufSize = int(cfg.GetInt(CfgRaftRecvBufSize))
	log.LogDebugf("[parseRaftConfig] load raftDir(%v).", s.raftDir)
	log.LogDebugf("[parseRaftConfig] load raftHearbeat(%v).", s.raftHeartbeat)
	log.LogDebugf("[parseRaftConfig] load raftReplica(%v).", s.raftReplica)
	return
}

func (s *DataNode) startRaftServer(cfg *config.Config) (err error) {
	log.LogInfo("Start: startRaftServer")

	if s.clusterUuidEnable {
		if err = config.CheckOrStoreClusterUuid(s.raftDir, s.clusterUuid, false); err != nil {
			log.LogErrorf("CheckOrStoreClusterUuid failed: %v", err)
			return fmt.Errorf("CheckOrStoreClusterUuid failed: %v", err)
		}
	}

	constCfg := config.ConstConfig{
		Listen:           s.port,
		RaftHeartbetPort: s.raftHeartbeat,
		RaftReplicaPort:  s.raftReplica,
	}
	ok := false
	if ok, err = config.CheckOrStoreConstCfg(s.raftDir, config.DefaultConstConfigFile, &constCfg); !ok {
		log.LogErrorf("constCfg check failed %v %v %v %v", s.raftDir, config.DefaultConstConfigFile, constCfg, err)
		return fmt.Errorf("constCfg check failed %v %v %v %v", s.raftDir, config.DefaultConstConfigFile, constCfg, err)
	}

	if _, err = os.Stat(s.raftDir); err != nil {
		if err = os.MkdirAll(s.raftDir, 0o755); err != nil {
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
		IPAddr:            LocalIP,
		HeartbeatPort:     heartbeatPort,
		ReplicaPort:       replicatePort,
		NumOfLogsToRetain: DefaultRaftLogsToRetain,
		TickInterval:      s.tickInterval,
		RecvBufSize:       s.raftRecvBufSize,
	}
	s.raftStore, err = raftstore.NewRaftStore(raftConf, cfg)
	if err != nil {
		err = errors.NewErrorf("new raftStore: %s", err.Error())
		log.LogErrorf("action[startRaftServer] cannot start raft server err(%v)", err)
	}

	return
}

func (s *DataNode) stopRaftServer() {
	if s.raftStore != nil {
		begin := time.Now()
		defer func() {
			msg := fmt.Sprintf("[stopRaftServer] stop raft server using time(%v)", time.Since(begin))
			log.LogInfo(msg)
			syslog.Print(msg)
		}()
		s.raftStore.Stop()
	}
}

// NewPacketToBroadcastMinAppliedID returns a new packet to broadcast the min applied ID.
func NewPacketToBroadcastMinAppliedID(partitionID uint64, minAppliedID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpBroadcastMinAppliedID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, minAppliedID)
	p.Size = uint32(len(p.Data))
	return
}

// NewPacketToGetAppliedID returns a new packet to get the applied ID.
func NewPacketToGetAppliedID(partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetAppliedId
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	return
}

// NewPacketToGetPartitionSize returns a new packet to get the partition size.
func NewPacketToGetPartitionSize(partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetPartitionSize
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	return
}

// NewPacketToGetPartitionSize returns a new packet to get the partition size.
func NewPacketToGetMaxExtentIDAndPartitionSIze(partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetMaxExtentIDAndPartitionSize
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	return
}

func (dp *DataPartition) findMinAppliedID(allAppliedIDs []uint64) (minAppliedID uint64, index int) {
	index = 0
	minAppliedID = allAppliedIDs[0]
	for i := 1; i < len(allAppliedIDs); i++ {
		if allAppliedIDs[i] < minAppliedID {
			minAppliedID = allAppliedIDs[i]
			index = i
		}
	}
	return minAppliedID, index
}

func (dp *DataPartition) findMaxAppliedID(allAppliedIDs []uint64) (maxAppliedID uint64, index int) {
	for i := 0; i < len(allAppliedIDs); i++ {
		if allAppliedIDs[i] > maxAppliedID {
			maxAppliedID = allAppliedIDs[i]
			index = i
		}
	}
	return maxAppliedID, index
}

func (dp *DataPartition) getLeaderPartitionNormalExtentInfo() (extentInfos []*storage.ExtentInfo, err error) {
	target := dp.getReplicaAddr(0)
	return dp.getRemoteExtentInfo(proto.NormalExtentType, nil, target)
}

// Get the partition size from the leader.
func (dp *DataPartition) getLeaderPartitionSize(maxExtentID uint64) (logicSize, realUsedSize uint64, err error) {
	var conn *net.TCPConn

	p := NewPacketToGetPartitionSize(dp.partitionID)
	p.ExtentID = maxExtentID
	target := dp.getReplicaAddr(0)
	conn, err = gConnPool.GetConnect(target) // get remote connect
	if err != nil {
		err = errors.Trace(err, " partition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	defer func() {
		gConnPool.PutConnect(conn, err != nil)
	}()
	err = p.WriteToConn(conn) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "partition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	err = p.ReadFromConnWithVer(conn, 60)
	if err != nil {
		err = errors.Trace(err, "partition(%v) read from host(%v)", dp.partitionID, target)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.Trace(err, "partition(%v) result code not ok (%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}

	logicSize = binary.BigEndian.Uint64(p.Data[:8])
	if len(p.Data) > 8 {
		realUsedSize = binary.BigEndian.Uint64(p.Data[8:])
	}

	log.LogInfof("partition(%v) MaxExtentID(%v) size(%v), usedSize (%v)", dp.partitionID, maxExtentID, logicSize, realUsedSize)

	return
}

func (dp *DataPartition) getMaxExtentIDAndPartitionSize(target string) (maxExtentID, PartitionSize uint64, err error) {
	var conn *net.TCPConn
	p := NewPacketToGetMaxExtentIDAndPartitionSIze(dp.partitionID)

	conn, err = gConnPool.GetConnect(target) // get remote connect
	if err != nil {
		err = errors.Trace(err, " partition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	defer func() {
		gConnPool.PutConnect(conn, err != nil)
	}()
	err = p.WriteToConn(conn) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "partition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	err = p.ReadFromConnWithVer(conn, 60)
	if err != nil {
		err = errors.Trace(err, "partition(%v) read from host(%v)", dp.partitionID, target)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.Trace(err, "partition(%v) result code not ok (%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	maxExtentID = binary.BigEndian.Uint64(p.Data[0:8])
	PartitionSize = binary.BigEndian.Uint64(p.Data[8:16])

	log.LogInfof("partition(%v) maxExtentID(%v) PartitionSize(%v) on leader", dp.partitionID, maxExtentID, PartitionSize)
	return
}

// Get the MaxExtentID partition  from the leader.
func (dp *DataPartition) getLeaderMaxExtentIDAndPartitionSize() (maxExtentID, PartitionSize uint64, err error) {
	target := dp.getReplicaAddr(0)
	return dp.getMaxExtentIDAndPartitionSize(target)
}

// Get the MaxExtentID partition  from the leader.
func (dp *DataPartition) getMemberExtentIDAndPartitionSize() (maxExtentID, PartitionSize uint64, err error) {
	target := dp.getReplicaAddr(1)
	return dp.getMaxExtentIDAndPartitionSize(target)
}

func (dp *DataPartition) broadcastMinAppliedID(minAppliedID uint64) (err error) {
	allReplica := dp.getReplicaCopy()

	for i := 0; i < len(allReplica); i++ {
		targetReplica := allReplica[i]

		replicaHostParts := strings.Split(targetReplica, ":")
		replicaHost := strings.TrimSpace(replicaHostParts[0])
		if LocalIP == replicaHost {
			log.LogDebugf("[broadcastMinAppliedID] partition(%v) local no send msg. localIP(%v) replicaHost(%v) appliedId(%v)",
				dp.partitionID, LocalIP, replicaHost, dp.appliedID)
			dp.minAppliedID = minAppliedID
			continue
		}

		p := NewPacketToBroadcastMinAppliedID(dp.partitionID, minAppliedID)
		var conn *net.TCPConn
		conn, err = gConnPool.GetConnect(targetReplica)
		if err != nil {
			return
		}
		err = p.WriteToConn(conn)
		if err != nil {
			gConnPool.PutConnect(conn, true)
			return
		}
		err = p.ReadFromConnWithVer(conn, 60)
		if err != nil {
			gConnPool.PutConnect(conn, true)
			return
		}
		gConnPool.PutConnect(conn, false)
		log.LogDebugf("[broadcastMinAppliedID] partition(%v) minAppliedID(%v) has sent to replica(%v)",
			dp.partitionID, minAppliedID, targetReplica)
	}

	return
}

// Get all replica applied ids
func (dp *DataPartition) getAllReplicaAppliedID() (allAppliedID []uint64, replyNum uint8) {
	replicas := dp.getReplicaCopy()
	replicaLen := len(replicas)
	allAppliedID = make([]uint64, replicaLen)
	for i := 0; i < replicaLen; i++ {
		p := NewPacketToGetAppliedID(dp.partitionID)
		replicaHostParts := strings.Split(replicas[i], ":")
		replicaHost := strings.TrimSpace(replicaHostParts[0])
		if LocalIP == replicaHost {
			log.LogDebugf("partition(%v) local no send msg. localIP(%v) replicaHost(%v) appliedId(%v)",
				dp.partitionID, LocalIP, replicaHost, dp.appliedID)
			allAppliedID[i] = dp.appliedID
			replyNum++
			continue
		}
		target := replicas[i]
		appliedID, err := dp.getRemoteAppliedID(target, p)
		if err != nil {
			log.LogErrorf("partition(%v) getRemoteAppliedID from replica(%v) Failed(%v).", dp.partitionID, target, err)
			continue
		}
		if appliedID == 0 {
			log.LogDebugf("[getAllReplicaAppliedID] partition(%v) local appliedID(%v) replicaHost(%v) appliedID=0",
				dp.partitionID, dp.appliedID, replicaHost)
		}
		allAppliedID[i] = appliedID
		replyNum++
	}

	return
}

// Get target members' applied id
func (dp *DataPartition) getRemoteAppliedID(target string, p *repl.Packet) (appliedID uint64, err error) {
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
	defer func() {
		gConnPool.PutConnect(conn, err != nil)
	}()
	err = p.WriteToConn(conn) // write command to the remote host
	if err != nil {
		return
	}
	err = p.ReadFromConnWithVer(conn, 60)
	if err != nil {
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("partition(%v) result code not ok (%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	appliedID = binary.BigEndian.Uint64(p.Data)

	log.LogDebugf("[getRemoteAppliedID] partition(%v) remoteAppliedID(%v)", dp.partitionID, appliedID)

	return
}

// Get all members' applied ids and find the minimum one
func (dp *DataPartition) updateMaxMinAppliedID() {
	var (
		minAppliedID uint64
		maxAppliedID uint64
	)

	// Get the applied id by the leader
	_, isLeader := dp.IsRaftLeader()
	if !isLeader {
		return
	}

	// if leader has not applied the raft, no need to get others
	if dp.appliedID == 0 {
		return
	}

	allAppliedID, replyNum := dp.getAllReplicaAppliedID()
	if replyNum == 0 {
		log.LogDebugf("[updateMaxMinAppliedID] PartitionID(%v) Get appliedId failed!", dp.partitionID)
		return
	}
	if replyNum == uint8(len(allAppliedID)) { // update dp.minAppliedID when every member had replied
		minAppliedID, _ = dp.findMinAppliedID(allAppliedID)
		log.LogDebugf("[updateMaxMinAppliedID] PartitionID(%v) localID(%v) OK! oldMinID(%v) newMinID(%v) allAppliedID(%v)",
			dp.partitionID, dp.appliedID, dp.minAppliedID, minAppliedID, allAppliedID)
		dp.broadcastMinAppliedID(minAppliedID)
	}

	maxAppliedID, _ = dp.findMaxAppliedID(allAppliedID)
	log.LogDebugf("[updateMaxMinAppliedID] PartitionID(%v) localID(%v) OK! oldMaxID(%v) newMaxID(%v)",
		dp.partitionID, dp.appliedID, dp.maxAppliedID, maxAppliedID)
	dp.maxAppliedID = maxAppliedID
}
