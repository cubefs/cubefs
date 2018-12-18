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
	ClusterId     string              `json:"cluster_id"`
	PartitionId   uint64              `json:"partition_id"`
	PartitionSize int                 `json:"partition_size"`
	Peers         []proto.Peer        `json:"peers"`
	RandomWrite   bool                `json:"random_write"`
	NodeId        uint64              `json:"-"`
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

/*
 Start raft instance when data partition start or restore.
*/
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
		dp.partitionId, peers, dp.path)
	pc := &raftstore.PartitionConfig{
		ID:      uint64(dp.partitionId),
		Applied: dp.applyId,
		Peers:   peers,
		SM:      dp,
		WalPath: dp.path,
	}

	dp.raftPartition, err = dp.config.RaftStore.CreatePartition(pc)

	return
}

func (dp *DataPartition) stopRaft() {
	if dp.raftPartition != nil {
		log.LogErrorf("[FATAL] stop raft partition=%v", dp.partitionId)
		dp.raftPartition.Stop()
		dp.raftPartition = nil
	}
	return
}

/*
 Start schedule task when data partition start or restore
 1. Store the raft applied id to "APPLY" file per 5 minutes.
 2. GetConnect the min applied id from all members and truncate raft log file per 10 minutes.
 3. Update partition status when apply failed and stop raft. Need manual intervention.
*/
func (dp *DataPartition) StartSchedule() {
	var isRunning bool
	truncRaftlogTimer := time.NewTimer(time.Minute * 10)
	storeAppliedTimer := time.NewTimer(time.Minute * 5)

	log.LogDebugf("[startSchedule] hello DataPartition schedule")

	dumpFunc := func(applyIndex uint64) {
		log.LogDebugf("[startSchedule] partitionId=%d: applyID=%d", dp.config.PartitionId, applyIndex)
		if err := dp.storeApplyIndex(applyIndex); err != nil {
			//retry
			dp.storeC <- applyIndex
			err = errors.Errorf("[startSchedule]: dump partition=%d: %v", dp.config.PartitionId, err.Error())
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
				log.LogDebugf("[startSchedule] stop partition=%v", dp.partitionId)
				truncRaftlogTimer.Stop()
				storeAppliedTimer.Stop()
				return

			case <-readyChan:
				for _, idx := range indexes {
					log.LogDebugf("[startSchedule] ready partition=%v: applyID=%d", dp.config.PartitionId, idx)
					go dumpFunc(idx)
				}
				indexes = nil

			case applyId := <-dp.storeC:
				indexes = append(indexes, applyId)
				log.LogDebugf("[startSchedule] store apply id partitionId=%d: applyID=%d",
					dp.config.PartitionId, applyId)

			case extentId := <-dp.repairC:
				dp.disk.Status = proto.Unavaliable
				dp.stopRaft()
				log.LogErrorf("action[ExtentRepair] stop raft partition=%v_%v", dp.partitionId, extentId)

			case <-truncRaftlogTimer.C:
				if dp.raftPartition == nil {
					break
				}
				dp.getMinAppliedId()
				if dp.minAppliedId > dp.lastTruncateId { // Has changed
					go dp.raftPartition.Truncate(dp.minAppliedId)
					dp.lastTruncateId = dp.minAppliedId
				}
				truncRaftlogTimer.Reset(time.Minute * 10)

			case <-storeAppliedTimer.C:
				dp.storeC <- dp.applyId
				storeAppliedTimer.Reset(time.Minute * 5)
			}
		}
	}(dp.stopC)
}

// Backup start raft must after all the extent files were repaired by primary.
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
					log.LogErrorf("partitionId[%v] leader start raft err[%v].", dp.partitionId, err)
					timer.Reset(5 * time.Second)
					continue
				}
				log.LogDebugf("partitionId[%v] leader started.", dp.partitionId)
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
				log.LogErrorf("partitionId[%v] get leader size err[%v]", dp.partitionId, err)
				timer.Reset(5 * time.Second)
				continue
			}
			if int(partitionSize) != dp.partitionSize {
				log.LogErrorf("partitionId[%v] leader size[%v] local size[%v]", dp.partitionId, partitionSize, dp.partitionSize)
				timer.Reset(5 * time.Second)
				continue
			}
			// Start raft
			if err := dp.StartRaft(); err != nil {
				log.LogErrorf("partitionId[%v] start raft err[%v]. Retry after 20s.", dp.partitionId, err)
				timer.Reset(5 * time.Second)
				continue
			}
			log.LogDebugf("partitionId[%v] raft started.", dp.partitionId)
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
		err = fmt.Errorf("%s partitionId=%v applyid=%v", RaftIsNotStart, dp.partitionId, index)
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
	if req.RemovePeer.ID == dp.config.NodeId {
		go func(index uint64) {
			for {
				time.Sleep(time.Millisecond)
				if dp.raftPartition.AppliedIndex() < index {
					continue
				}
				if dp.raftPartition != nil {
					dp.raftPartition.Delete()
				}
				dp.Disk().space.DeletePartition(dp.partitionId)
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
	if _, err = fmt.Sscanf(string(data), "%d", &dp.applyId); err != nil {
		err = errors.Errorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

func (dp *DataPartition) SetMinAppliedId(id uint64) {
	dp.minAppliedId = id
}

func (dp *DataPartition) GetAppliedId() (id uint64) {
	return dp.applyId
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
		NodeID:        s.nodeId,
		WalPath:       s.raftDir,
		IpAddr:        s.localIp,
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

// Create task to repair extent files
func (dp *DataPartition) ExtentRepair(extentFiles []*storage.FileInfo) {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[ExtentRepair] partition=%v start.", dp.partitionId)

	mf := NewDataPartitionRepairTask(extentFiles)

	for i := 0; i < len(extentFiles); i++ {
		extentFile := extentFiles[i]
		addFile := &storage.FileInfo{Source: extentFile.Source, FileId: extentFile.FileId, Size: extentFile.Size, Inode: extentFile.Inode}
		mf.AddExtentsTasks = append(mf.AddExtentsTasks, addFile)
		log.LogDebugf("action[ExtentRepair] partition=%v extent [%v_%v] addFile[%v].",
			dp.partitionId, dp.partitionId, extentFile.FileId, addFile)
	}

	dp.MergeExtentStoreRepair(mf)

	finishTime := time.Now().UnixNano()
	log.LogInfof("action[ExtentRepair] partition=%v finish cost[%vms].",
		dp.partitionId, (finishTime-startTime)/int64(time.Millisecond))
}

// GetConnect all extents information
func (dp *DataPartition) getExtentInfo(targetAddr string) (extentFiles []*storage.FileInfo, err error) {
	// get remote extents meta by opGetAllWaterMarker cmd
	p := repl.NewGetAllWaterMarker(dp.partitionId, proto.NormalExtentMode)
	var conn *net.TCPConn
	target := targetAddr
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Annotatef(err, "getExtentInfo  partition=%v get host[%v] connect", dp.partitionId, target)
		return
	}
	err = p.WriteToConn(conn) //write command to remote host
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getExtentInfo partition=%v write to host[%v]", dp.partitionId, target)
		return
	}
	err = p.ReadFromConn(conn, 60) //read it response
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getExtentInfo partition=%v read from host[%v]", dp.partitionId, target)
		return
	}
	fileInfos := make([]*storage.FileInfo, 0)
	err = json.Unmarshal(p.Data[:p.Size], &fileInfos)
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getExtentInfo partition=%v unmarshal json[%v]", dp.partitionId, string(p.Data[:p.Size]))
		return
	}

	extentFiles = make([]*storage.FileInfo, 0)
	for _, fileInfo := range fileInfos {
		extentFiles = append(extentFiles, fileInfo)
	}

	gConnPool.PutConnect(conn, true)

	return
}

func NewGetAppliedId(partitionId uint64, minAppliedId uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetAppliedId
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GeneratorRequestID()
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, minAppliedId)
	p.Size = uint32(len(p.Data))
	return
}

func NewGetPartitionSize(partitionId uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetPartitionSize
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GeneratorRequestID()
	return
}

func (dp *DataPartition) findMinAppliedId(allAppliedIds []uint64) (minAppliedId uint64, index int) {
	index = 0
	minAppliedId = allAppliedIds[0]
	for i := 1; i < len(allAppliedIds); i++ {
		if allAppliedIds[i] < minAppliedId {
			minAppliedId = allAppliedIds[i]
			index = i
		}
	}
	return minAppliedId, index
}

func (dp *DataPartition) findMaxAppliedId(allAppliedIds []uint64) (maxAppliedId uint64, index int) {
	for i := 0; i < len(allAppliedIds); i++ {
		if allAppliedIds[i] > maxAppliedId {
			maxAppliedId = allAppliedIds[i]
			index = i
		}
	}
	return maxAppliedId, index
}

// GetConnect partition size from leader
func (dp *DataPartition) getPartitionSize() (partitionSize uint64, err error) {
	var (
		conn *net.TCPConn
	)

	p := NewGetPartitionSize(dp.partitionId)

	target := dp.replicaHosts[0]
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Annotatef(err, " partition=%v get host[%v] connect", dp.partitionId, target)
		return
	}

	err = p.WriteToConn(conn) //write command to remote host
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "partition=%v write to host[%v]", dp.partitionId, target)
		return
	}
	err = p.ReadFromConn(conn, 60) //read it response
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "partition=%v read from host[%v]", dp.partitionId, target)
		return
	}

	partitionSize = binary.BigEndian.Uint64(p.Data)

	log.LogDebugf("partition=%v partitionSize=%v", dp.partitionId, partitionSize)

	gConnPool.PutConnect(conn, true)

	return
}

// GetConnect all member's applied id
func (dp *DataPartition) getAllAppliedId(setMinAppliedId bool) (allAppliedId []uint64, err error) {
	var minAppliedId uint64
	allAppliedId = make([]uint64, len(dp.replicaHosts))
	if setMinAppliedId == true {
		minAppliedId = dp.minAppliedId
	} else {
		minAppliedId = 0
	}
	p := NewGetAppliedId(dp.partitionId, minAppliedId)

	for i := 0; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		replicaHostParts := strings.Split(dp.replicaHosts[i], ":")
		replicaHost := strings.TrimSpace(replicaHostParts[0])
		if LocalIP == replicaHost {
			log.LogDebugf("partition=%v local no need send msg. localIp[%v] replicaHost[%v] appliedId[%v]",
				dp.partitionId, LocalIP, replicaHost, dp.applyId)
			allAppliedId[i] = dp.applyId
			continue
		}

		target := dp.replicaHosts[i]
		conn, err = gConnPool.GetConnect(target) //get remote connect
		if err != nil {
			err = errors.Annotatef(err, " partition=%v get host[%v] connect", dp.partitionId, target)
			return
		}

		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.PutConnect(conn, true)
			err = errors.Annotatef(err, "partition=%v write to host[%v]", dp.partitionId, target)
			return
		}
		err = p.ReadFromConn(conn, 60) //read it response
		if err != nil {
			gConnPool.PutConnect(conn, true)
			err = errors.Annotatef(err, "partition=%v read from host[%v]", dp.partitionId, target)
			return
		}

		remoteAppliedId := binary.BigEndian.Uint64(p.Data)

		log.LogDebugf("partition=%v remoteAppliedId=%v", dp.partitionId, remoteAppliedId)

		allAppliedId[i] = remoteAppliedId

		gConnPool.PutConnect(conn, true)
	}

	return
}

// GetConnect all member's applied id and find the mini one
func (dp *DataPartition) getMinAppliedId() {
	var (
		minAppliedId uint64
		err          error
	)

	//get applied id only by leader
	leaderAddr, isLeader := dp.IsRaftLeader()
	if !isLeader || leaderAddr == "" {
		log.LogDebugf("[getMinAppliedId] partition=%v notRaftLeader leaderAddr[%v] localIp[%v]",
			dp.partitionId, leaderAddr, LocalIP)
		return
	}

	//if leader has not applied, no need to get others
	if dp.applyId == 0 {
		log.LogDebugf("[getMinAppliedId] partition=%v leader no apply. commit=%v", dp.partitionId, dp.raftPartition.CommittedIndex())
		return
	}

	defer func() {
		if err == nil {
			log.LogDebugf("[getMinAppliedId] partition=%v success oldAppId=%v newAppId=%v localAppId=%v",
				dp.partitionId, dp.minAppliedId, minAppliedId, dp.applyId)
			//success maybe update the minAppliedId
			dp.minAppliedId = minAppliedId
		} else {
			//do nothing
			log.LogErrorf("[getMinAppliedId] partition=%v newAppId=%v localAppId=%v err %v",
				dp.partitionId, minAppliedId, dp.applyId, err)
		}
	}()

	allAppliedId, err := dp.getAllAppliedId(true)
	if err != nil {
		log.LogErrorf("[getMinAppliedId] partition=%v newAppId=%v localAppId=%v err %v",
			dp.partitionId, minAppliedId, dp.applyId, err)
		return
	}

	minAppliedId, _ = dp.findMinAppliedId(allAppliedId)
	return
}

// GetConnect all member's applied id and find the max one
func (dp *DataPartition) getMaxAppliedId() (index int, err error) {
	var allAppliedId []uint64
	allAppliedId, err = dp.getAllAppliedId(false)
	if err != nil {
		log.LogErrorf("[getAllAppliedId] partition=%v localAppId=%v err %v", dp.partitionId, dp.applyId, err)
		return
	}

	_, index = dp.findMaxAppliedId(allAppliedId)

	return
}
