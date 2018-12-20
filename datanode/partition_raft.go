// Copyright 2018 The ChuBao Authors.
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
	"io/ioutil"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	raftproto "github.com/tiglabs/raft/proto"
)

type dataPartitionCfg struct {
	VolName       string              `json:"vol_name"`
	ClusterID     string              `json:"cluster_id"`
	PartitionID   uint64              `json:"partition_id"`
	PartitionSize int                 `json:"partition_size"`
	Peers         []proto.Peer        `json:"peers"`
	RandomWrite   bool                `json:"random_write"`
	NodeID        uint64              `json:"-"`
	RaftStore     raftstore.RaftStore `json:"-"`
}

func (dp *DataPartition) getRaftPort() (heartbeat, replicate int, err error) {
	raftConfig := dp.config.RaftStore.RaftConfig()
	heartbeatAddrParts := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicateAddrParts := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrParts) != 2 {
		err = errors.New("illegal heartbeat address")
		return
	}
	if len(replicateAddrParts) != 2 {
		err = errors.New("illegal replicate address")
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrParts[1])
	if err != nil {
		return
	}
	replicate, err = strconv.Atoi(replicateAddrParts[1])
	if err != nil {
		return
	}
	return
}

// StartRaft start raft instance when data partition start or restore.
func (dp *DataPartition) StartRaft() (err error) {
	var (
		heartbeatPort int
		replicatePort int
		peers         []raftstore.PeerAddress
	)

	if heartbeatPort, replicatePort, err = dp.getRaftPort(); err != nil {
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
			ReplicatePort: replicatePort,
		}
		peers = append(peers, rp)
	}
	log.LogDebugf("start partition=%v raft peers: %s path: %s",
		dp.partitionID, peers, dp.path)
	pc := &raftstore.PartitionConfig{
		ID:      uint64(dp.partitionID),
		Applied: dp.applyID,
		Peers:   peers,
		SM:      dp,
		WalPath: dp.path,
	}

	dp.raftPartition, err = dp.config.RaftStore.CreatePartition(pc)

	return
}

func (dp *DataPartition) stopRaft() {
	if dp.raftPartition != nil {
		log.LogErrorf("[FATAL] stop raft partition=%v", dp.partitionID)
		dp.raftPartition.Stop()
		dp.raftPartition = nil
	}
	return
}

// StartSchedule task when data partition start or restore
// 1. Store the raft applied id to "APPLY" file per 5 minutes.
// 2. GetConnect the min applied id from all members and truncate raft log file per 10 minutes.
// 3. Update partition status when apply failed and stop raft. Need manual intervention.
func (dp *DataPartition) StartSchedule() {
	var isRunning bool
	truncRaftlogTimer := time.NewTimer(time.Minute * 10)
	storeAppliedTimer := time.NewTimer(time.Minute * 5)

	log.LogDebugf("[startSchedule] hello DataPartition schedule")

	dumpFunc := func(applyIndex uint64) {
		log.LogDebugf("[startSchedule] partitionID=%d: applyID=%d", dp.config.PartitionID, applyIndex)
		if err := dp.storeApplyIndex(applyIndex); err != nil {
			//retry
			dp.storeC <- applyIndex
			err = errors.Errorf("[startSchedule]: dump partition=%d: %v", dp.config.PartitionID, err.Error())
			log.LogErrorf(err.Error())
		}
		isRunning = false
	}

	go func(stopC chan bool) {
		var indexes []uint64
		readyChan := make(chan struct{}, 1)
		for {
			if len(indexes) > 0 {
				if isRunning == false {
					isRunning = true
					readyChan <- struct{}{}
				}
			}
			select {
			case <-stopC:
				log.LogDebugf("[startSchedule] stop partition=%v", dp.partitionID)
				truncRaftlogTimer.Stop()
				storeAppliedTimer.Stop()
				return

			case <-readyChan:
				for _, idx := range indexes {
					log.LogDebugf("[startSchedule] ready partition=%v: applyID=%d", dp.config.PartitionID, idx)
					go dumpFunc(idx)
				}
				indexes = nil

			case applyID := <-dp.storeC:
				indexes = append(indexes, applyID)
				log.LogDebugf("[startSchedule] store apply id partitionID=%d: applyID=%d",
					dp.config.PartitionID, applyID)

			case extentID := <-dp.repairC:
				dp.disk.Status = proto.Unavaliable
				dp.stopRaft()
				log.LogErrorf("action[ExtentRepair] stop raft partition=%v_%v", dp.partitionID, extentID)

			case <-truncRaftlogTimer.C:
				if dp.raftPartition == nil {
					break
				}
				dp.getMinAppliedID()
				if dp.minAppliedID > dp.lastTruncateID { // Has changed
					go dp.raftPartition.Truncate(dp.minAppliedID)
					dp.lastTruncateID = dp.minAppliedID
				}
				truncRaftlogTimer.Reset(time.Minute * 10)

			case <-storeAppliedTimer.C:
				dp.storeC <- dp.applyID
				storeAppliedTimer.Reset(time.Minute * 5)
			}
		}
	}(dp.stopC)
}

// WaitingRepairedAndStartRaft Backup start raft must after all the extent files were repaired by primary.
// Repair finished - Local's dp.partitionSize is same to primary's dp.partitionSize.
// The repair task be done in statusUpdateScheduler->LaunchRepair.
// This method just be called when create partitions.
func (dp *DataPartition) WaitingRepairedAndStartRaft() {
	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			if dp.isLeader {
				// Primary needn't waiting extent repair.
				if err := dp.StartRaft(); err != nil {
					log.LogErrorf("partitionID[%v] leader start raft err[%v].", dp.partitionID, err)
					timer.Reset(5 * time.Second)
					continue
				}
				log.LogDebugf("partitionID[%v] leader started.", dp.partitionID)
				return
			}
			// Wait the dp.replicaHosts updated.
			if len(dp.replicaHosts) == 0 {
				timer.Reset(5 * time.Second)
				continue
			}
			// Backup get the dp.partitionSize from primary and compare with local.
			partitionSize, err := dp.getPartitionSize()
			if err != nil {
				log.LogErrorf("partitionID[%v] get leader size err[%v]", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			if int(partitionSize) != dp.partitionSize {
				log.LogErrorf("partitionID[%v] leader size[%v] local size[%v]", dp.partitionID, partitionSize, dp.partitionSize)
				timer.Reset(5 * time.Second)
				continue
			}
			// Start raft
			if err := dp.StartRaft(); err != nil {
				log.LogErrorf("partitionID[%v] start raft err[%v]. Retry after 20s.", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			log.LogDebugf("partitionID[%v] raft started.", dp.partitionID)
			return
		case <-dp.stopC:
			timer.Stop()
			return
		}
	}
}

func (dp *DataPartition) confAddNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicatePort int
	)
	if heartbeatPort, replicatePort, err = dp.getRaftPort(); err != nil {
		return
	}

	findAddPeer := false
	for _, peer := range dp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			findAddPeer = true
			break
		}
	}
	updated = !findAddPeer
	if !updated {
		return
	}
	dp.config.Peers = append(dp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	dp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicatePort)
	return
}

func (dp *DataPartition) confRemoveNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	if dp.raftPartition == nil {
		err = fmt.Errorf("%s partitionID=%v applyid=%v", RaftIsNotStart, dp.partitionID, index)
		return
	}
	peerIndex := -1
	for i, peer := range dp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		return
	}
	if req.RemovePeer.ID == dp.config.NodeID {
		go func(index uint64) {
			for {
				time.Sleep(time.Millisecond)
				if dp.raftPartition.AppliedIndex() < index {
					continue
				}
				if dp.raftPartition != nil {
					dp.raftPartition.Delete()
				}
				dp.Disk().space.DeletePartition(dp.partitionID)
				log.LogDebugf("[confRemoveNode]: remove self end.")
				return
			}
		}(index)
		updated = false
		log.LogDebugf("[confRemoveNode]: begin remove self.")
		return
	}
	dp.config.Peers = append(dp.config.Peers[:peerIndex], dp.config.Peers[peerIndex+1:]...)
	log.LogDebugf("[confRemoveNode]: remove peer.")
	return
}

func (dp *DataPartition) confUpdateNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	log.LogDebugf("[confUpdateNode]: not support.")
	return
}

func (dp *DataPartition) storeApplyIndex(applyIndex uint64) (err error) {
	filename := path.Join(dp.Path(), TempApplyIndexFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", applyIndex)); err != nil {
		return
	}
	err = os.Rename(filename, path.Join(dp.Path(), ApplyIndexFile))
	return
}

func (dp *DataPartition) LoadApplyIndex() (err error) {
	filename := path.Join(dp.Path(), ApplyIndexFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.Errorf("[loadApplyIndex] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.Errorf("[loadApplyIndex]: ApplyIndex is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &dp.applyID); err != nil {
		err = errors.Errorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

func (dp *DataPartition) SetMinAppliedID(id uint64) {
	dp.minAppliedID = id
}

func (dp *DataPartition) GetAppliedID() (id uint64) {
	return dp.applyID
}

func (s *DataNode) parseRaftConfig(cfg *config.Config) (err error) {
	s.raftDir = cfg.GetString(ConfigKeyRaftDir)
	if s.raftDir == "" {
		s.raftDir = DefaultRaftDir
	}
	s.raftHeartbeat = cfg.GetString(ConfigKeyRaftHeartbeat)
	s.raftReplicate = cfg.GetString(ConfigKeyRaftReplicate)

	log.LogDebugf("[parseRaftConfig] load raftDir[%v].", s.raftDir)
	log.LogDebugf("[parseRaftConfig] load raftHearbeat[%v].", s.raftHeartbeat)
	log.LogDebugf("[parseRaftConfig] load raftReplicate[%v].", s.raftReplicate)
	return
}

// Start raft server when DataNode server be started.
func (s *DataNode) startRaftServer(cfg *config.Config) (err error) {
	log.LogInfo("Start: startRaftServer")

	s.parseRaftConfig(cfg)

	if _, err = os.Stat(s.raftDir); err != nil {
		if err = os.MkdirAll(s.raftDir, 0755); err != nil {
			err = errors.Errorf("create raft server dir: %s", err.Error())
			log.LogErrorf("action[startRaftServer] cannot start raft server err[%v]", err)
			return
		}
	}

	heartbeatPort, _ := strconv.Atoi(s.raftHeartbeat)
	replicatePort, _ := strconv.Atoi(s.raftReplicate)

	raftConf := &raftstore.Config{
		NodeID:        s.nodeID,
		WalPath:       s.raftDir,
		IPAddr:        s.localIP,
		HeartbeatPort: heartbeatPort,
		ReplicatePort: replicatePort,
		RetainLogs:    dpRetainRaftLogs,
	}
	s.raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		err = errors.Errorf("new raftStore: %s", err.Error())
		log.LogErrorf("action[startRaftServer] cannot start raft server err[%v]", err)
	}

	return
}

func (s *DataNode) stopRaftServer() {
	if s.raftStore != nil {
		s.raftStore.Stop()
	}
}

// ExtentRepair Create task to repair extent files
func (dp *DataPartition) ExtentRepair(extentFiles []*storage.ExtentInfo) {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[ExtentRepair] partition=%v start.", dp.partitionID)

	mf := NewDataPartitionRepairTask(extentFiles)

	for i := 0; i < len(extentFiles); i++ {
		extentFile := extentFiles[i]
		addFile := &storage.ExtentInfo{Source: extentFile.Source, FileID: extentFile.FileID, Size: extentFile.Size, Inode: extentFile.Inode}
		mf.AddExtentsTasks = append(mf.AddExtentsTasks, addFile)
		log.LogDebugf("action[ExtentRepair] partition=%v extent [%v_%v] addFile[%v].",
			dp.partitionID, dp.partitionID, extentFile.FileID, addFile)
	}

	dp.MergeExtentStoreRepair(mf)

	finishTime := time.Now().UnixNano()
	log.LogInfof("action[ExtentRepair] partition=%v finish cost[%vms].",
		dp.partitionID, (finishTime-startTime)/int64(time.Millisecond))
}

// GetConnect all extents information
func (dp *DataPartition) getExtentInfo(targetAddr string) (extentFiles []*storage.ExtentInfo, err error) {
	// get remote extents meta by opGetAllWaterMarker cmd
	p := repl.NewGetAllWaterMarker(dp.partitionID, proto.NormalExtentMode)
	var conn *net.TCPConn
	target := targetAddr
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Annotatef(err, "getExtentInfo  partition=%v get host[%v] connect", dp.partitionID, target)
		return
	}
	err = p.WriteToConn(conn) //write command to remote host
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getExtentInfo partition=%v write to host[%v]", dp.partitionID, target)
		return
	}
	err = p.ReadFromConn(conn, 60) //read it response
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getExtentInfo partition=%v read from host[%v]", dp.partitionID, target)
		return
	}
	fileInfos := make([]*storage.ExtentInfo, 0)
	err = json.Unmarshal(p.Data[:p.Size], &fileInfos)
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getExtentInfo partition=%v unmarshal json[%v]", dp.partitionID, string(p.Data[:p.Size]))
		return
	}

	extentFiles = make([]*storage.ExtentInfo, 0)
	for _, fileInfo := range fileInfos {
		extentFiles = append(extentFiles, fileInfo)
	}

	gConnPool.PutConnect(conn, true)

	return
}

func NewGetAppliedID(partitionID uint64, minAppliedID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetAppliedId
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GeneratorRequestID()
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, minAppliedID)
	p.Size = uint32(len(p.Data))
	return
}

func NewGetPartitionSize(partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetPartitionSize
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GeneratorRequestID()
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

// GetConnect partition size from leader
func (dp *DataPartition) getPartitionSize() (partitionSize uint64, err error) {
	var (
		conn *net.TCPConn
	)

	p := NewGetPartitionSize(dp.partitionID)

	target := dp.replicaHosts[0]
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Annotatef(err, " partition=%v get host[%v] connect", dp.partitionID, target)
		return
	}

	err = p.WriteToConn(conn) //write command to remote host
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "partition=%v write to host[%v]", dp.partitionID, target)
		return
	}
	err = p.ReadFromConn(conn, 60) //read it response
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "partition=%v read from host[%v]", dp.partitionID, target)
		return
	}

	partitionSize = binary.BigEndian.Uint64(p.Data)

	log.LogDebugf("partition=%v partitionSize=%v", dp.partitionID, partitionSize)

	gConnPool.PutConnect(conn, true)

	return
}

// GetConnect all member's applied id
func (dp *DataPartition) getAllAppliedID(setMinAppliedID bool) (allAppliedID []uint64, err error) {
	var minAppliedID uint64
	allAppliedID = make([]uint64, len(dp.replicaHosts))
	if setMinAppliedID == true {
		minAppliedID = dp.minAppliedID
	} else {
		minAppliedID = 0
	}
	p := NewGetAppliedID(dp.partitionID, minAppliedID)

	for i := 0; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		replicaHostParts := strings.Split(dp.replicaHosts[i], ":")
		replicaHost := strings.TrimSpace(replicaHostParts[0])
		if LocalIP == replicaHost {
			log.LogDebugf("partition=%v local no need send msg. localIP[%v] replicaHost[%v] appliedId[%v]",
				dp.partitionID, LocalIP, replicaHost, dp.applyID)
			allAppliedID[i] = dp.applyID
			continue
		}

		target := dp.replicaHosts[i]
		conn, err = gConnPool.GetConnect(target) //get remote connect
		if err != nil {
			err = errors.Annotatef(err, " partition=%v get host[%v] connect", dp.partitionID, target)
			return
		}

		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.PutConnect(conn, true)
			err = errors.Annotatef(err, "partition=%v write to host[%v]", dp.partitionID, target)
			return
		}
		err = p.ReadFromConn(conn, 60) //read it response
		if err != nil {
			gConnPool.PutConnect(conn, true)
			err = errors.Annotatef(err, "partition=%v read from host[%v]", dp.partitionID, target)
			return
		}

		remoteAppliedID := binary.BigEndian.Uint64(p.Data)

		log.LogDebugf("partition=%v remoteAppliedID=%v", dp.partitionID, remoteAppliedID)

		allAppliedID[i] = remoteAppliedID

		gConnPool.PutConnect(conn, true)
	}

	return
}

// GetConnect all member's applied id and find the mini one
func (dp *DataPartition) getMinAppliedID() {
	var (
		minAppliedID uint64
		err          error
	)

	//get applied id only by leader
	leaderAddr, isLeader := dp.IsRaftLeader()
	if !isLeader || leaderAddr == "" {
		log.LogDebugf("[getMinAppliedId] partition=%v notRaftLeader leaderAddr[%v] localIP[%v]",
			dp.partitionID, leaderAddr, LocalIP)
		return
	}

	//if leader has not applied, no need to get others
	if dp.applyID == 0 {
		log.LogDebugf("[getMinAppliedID] partition=%v leader no apply. commit=%v", dp.partitionID, dp.raftPartition.CommittedIndex())
		return
	}

	defer func() {
		if err == nil {
			log.LogDebugf("[getMinAppliedID] partition=%v success oldAppId=%v newAppId=%v localAppId=%v",
				dp.partitionID, dp.minAppliedID, minAppliedID, dp.applyID)
			//success maybe update the minAppliedID
			dp.minAppliedID = minAppliedID
		} else {
			//do nothing
			log.LogErrorf("[getMinAppliedID] partition=%v newAppId=%v localAppId=%v err %v",
				dp.partitionID, minAppliedID, dp.applyID, err)
		}
	}()

	allAppliedID, err := dp.getAllAppliedID(true)
	if err != nil {
		log.LogErrorf("[getMinAppliedID] partition=%v newAppId=%v localAppId=%v err %v",
			dp.partitionID, minAppliedID, dp.applyID, err)
		return
	}

	minAppliedID, _ = dp.findMinAppliedID(allAppliedID)
	return
}

// GetConnect all member's applied id and find the max one
func (dp *DataPartition) getMaxAppliedID() (index int, err error) {
	var allAppliedID []uint64
	allAppliedID, err = dp.getAllAppliedID(false)
	if err != nil {
		log.LogErrorf("[getAllAppliedID] partition=%v localAppId=%v err %v", dp.partitionID, dp.applyID, err)
		return
	}

	_, index = dp.findMaxAppliedID(allAppliedID)

	return
}
