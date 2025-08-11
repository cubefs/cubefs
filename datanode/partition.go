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
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/datanode/storage"
	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/strutil"
)

const (
	DataPartitionPrefix           = "datapartition"
	DataPartitionMetadataFileName = "META"
	TempMetadataFileName          = ".meta"
	ApplyIndexFile                = "APPLY"
	TempApplyIndexFile            = ".apply"
	TimeLayout                    = "2006-01-02 15:04:05"
	DpStatusFile                  = ".dpStatus"
)

const (
	RaftStatusStopped = 0
	RaftStatusRunning = 1
)

const (
	DpCheckBaseInterval = 7200
	DpCheckRandomRange  = 1800
	DpMinCheckInterval  = DpCheckBaseInterval - DpCheckRandomRange
	DpMaxCheckInterval  = DpCheckBaseInterval + DpCheckRandomRange
)

type DataPartitionMetadata struct {
	VolumeID                string
	PartitionID             uint64
	PartitionSize           int
	PartitionType           int
	CreateTime              string
	Peers                   []proto.Peer
	Hosts                   []string
	DataPartitionCreateType int
	LastTruncateID          uint64
	ReplicaNum              int
	StopRecover             bool
	VerList                 []*proto.VolVersionInfo
	ApplyID                 uint64
	DiskErrCnt              uint64
	IsRepairing             bool
}

func (md *DataPartitionMetadata) Validate() (err error) {
	md.VolumeID = strings.TrimSpace(md.VolumeID)
	if len(md.VolumeID) == 0 || md.PartitionID == 0 || md.PartitionSize == 0 {
		err = errors.New("illegal data partition metadata")
		return
	}
	return
}

// MetaMultiSnapshotInfo
type MetaMultiSnapshotInfo struct {
	VerSeq uint64
	Status int8
	Ctime  time.Time
}

type DataPartition struct {
	clusterID       string
	volumeID        string
	partitionID     uint64
	partitionStatus int
	partitionSize   int
	partitionType   int
	replicaNum      int
	replicas        []string // addresses of the replicas
	replicasLock    sync.RWMutex
	disk            *Disk
	dataNode        *DataNode
	isLeader        bool
	isRaftLeader    bool
	path            string
	used            int
	leaderSize      int
	extentStore     *storage.ExtentStore
	raftPartition   raftstore.Partition
	config          *dataPartitionCfg
	appliedID       uint64 // apply id used in Raft
	lastTruncateID  uint64 // truncate id used in Raft
	metaAppliedID   uint64 // apply id while do meta persist
	minAppliedID    uint64
	maxAppliedID    uint64

	stopOnce  sync.Once
	stopRaftC chan uint64
	storeC    chan uint64
	stopC     chan bool

	raftStatus int32

	intervalToUpdateReplicas      int64 // interval to ask the master for updating the replica information
	snapshot                      []*proto.File
	snapshotMutex                 sync.RWMutex
	intervalToUpdatePartitionSize time.Time
	loadExtentHeaderStatus        int
	DataPartitionCreateType       int
	isLoadingDataPartition        int32
	persistMetaMutex              sync.RWMutex

	// snapshot
	// verSeqPrepare              uint64
	// verSeqCommitStatus         int8
	verSeq                     uint64
	volVersionInfoList         *proto.VolVersionInfoList
	decommissionRepairProgress float64 // record repair progress for decommission datapartition
	stopRecover                bool
	recoverErrCnt              uint64 // donot reset, if reach max err cnt, delete this dp

	diskErrCnt         uint64 // number of disk io errors while reading or writing
	responseStatus     uint32
	PersistApplyIdChan chan PersistApplyIdRequest

	readOnlyReasons uint32
	isRepairing     bool
}

type PersistApplyIdRequest struct {
	done chan struct{}
}

const (
	responseInitial uint32 = iota
	responseWait
)

func (dp *DataPartition) IsForbidden() bool {
	return dp.config.Forbidden
}

func (dp *DataPartition) SetForbidden(status bool) {
	dp.config.Forbidden = status
}

func (dp *DataPartition) IsForbidWriteOpOfProtoVer0() bool {
	return dp.config.ForbidWriteOpOfProtoVer0
}

func (dp *DataPartition) SetForbidWriteOpOfProtoVer0(status bool) {
	dp.config.ForbidWriteOpOfProtoVer0 = status
}

func (dp *DataPartition) GetRepairBlockSize() (size uint64) {
	size = dp.config.DpRepairBlockSize
	if size == 0 {
		size = proto.DefaultDpRepairBlockSize
	}
	return
}

func (dp *DataPartition) SetRepairBlockSize(size uint64) {
	dp.config.DpRepairBlockSize = size
}

func CreateDataPartition(dpCfg *dataPartitionCfg, disk *Disk, request *proto.CreateDataPartitionRequest) (dp *DataPartition, err error) {
	if dp, err = newDataPartition(dpCfg, disk, true); err != nil {
		return
	}
	disk.space.partitionMutex.Lock()
	disk.space.partitions[dp.partitionID] = dp
	disk.space.partitionMutex.Unlock()
	dp.ForceLoadHeader()
	if request.CreateType == proto.NormalCreateDataPartition {
		err = dp.StartRaft(false)
	} else {
		// init leaderSize to partitionSize
		disk.updateDisk(uint64(request.LeaderSize))
		// ensure heartbeat report  Recovering
		dp.partitionStatus = proto.Recovering
		dp.isRepairing = true
		dp.leaderSize = request.LeaderSize
		go dp.StartRaftAfterRepair(false)
	}
	if err != nil {
		return nil, err
	}

	// persist file metadata
	go dp.StartRaftLoggingSchedule()
	dp.DataPartitionCreateType = request.CreateType
	dp.replicaNum = request.ReplicaNum
	err = dp.PersistMetadata()
	disk.AddSize(uint64(dp.Size()))
	return
}

func (dp *DataPartition) IsEqualCreateDataPartitionRequest(request *proto.CreateDataPartitionRequest) (err error) {
	if len(dp.config.Peers) != len(request.Members) {
		return fmt.Errorf("exist partition(%v)  peers len(%v) members len(%v)",
			dp.partitionID, len(dp.config.Peers), len(request.Members))
	}
	for index, host := range dp.config.Hosts {
		requestHost := request.Hosts[index]
		if host != requestHost {
			return fmt.Errorf("exist partition(%v) index(%v) requestHost(%v) persistHost(%v)",
				dp.partitionID, index, requestHost, host)
		}
	}
	for index, peer := range dp.config.Peers {
		requestPeer := request.Members[index]
		if requestPeer.ID != peer.ID || requestPeer.Addr != peer.Addr {
			return fmt.Errorf("exist partition(%v) index(%v) requestPeer(%v) persistPeers(%v)",
				dp.partitionID, index, requestPeer, peer)
		}
	}
	if dp.config.VolName != request.VolumeId {
		return fmt.Errorf("exist partition Partition(%v)  requestVolName(%v) persistVolName(%v)",
			dp.partitionID, request.VolumeId, dp.config.VolName)
	}

	return
}

func (dp *DataPartition) ForceSetDataPartitionToLoading() {
	atomic.StoreInt32(&dp.isLoadingDataPartition, 1)
}

func (dp *DataPartition) ForceSetDataPartitionToFinishLoad() {
	atomic.StoreInt32(&dp.isLoadingDataPartition, 2)
}

func (dp *DataPartition) IsDataPartitionLoadFin() bool {
	return atomic.LoadInt32(&dp.isLoadingDataPartition) == 2
}

func (dp *DataPartition) IsDataPartitionLoading() bool {
	return atomic.LoadInt32(&dp.isLoadingDataPartition) == 1
}

func (dp *DataPartition) ForceSetRaftRunning() {
	atomic.StoreInt32(&dp.raftStatus, RaftStatusRunning)
}

// LoadDataPartition loads and returns a partition instance based on the specified directory.
// It reads the partition metadata file stored under the specified directory
// and creates the partition instance.
func LoadDataPartition(partitionDir string, disk *Disk) (dp *DataPartition, err error) {
	var metaFileData []byte
	if metaFileData, err = ioutil.ReadFile(path.Join(partitionDir, DataPartitionMetadataFileName)); err != nil {
		return
	}
	meta := &DataPartitionMetadata{}
	if err = json.Unmarshal(metaFileData, meta); err != nil {
		return
	}

	// compat old persisted data partitions, add raft port info
	lackRaftPort := false
	if disk.dataNode.raftPartitionCanUsingDifferentPort {
		for i, peer := range meta.Peers {
			if len(peer.ReplicaPort) == 0 || len(peer.HeartbeatPort) == 0 {
				peer.ReplicaPort = disk.dataNode.raftReplica
				peer.HeartbeatPort = disk.dataNode.raftHeartbeat
				meta.Peers[i] = peer
				lackRaftPort = true
			}
		}
	}

	if err = meta.Validate(); err != nil {
		return
	}

	dpCfg := &dataPartitionCfg{
		VolName:          meta.VolumeID,
		PartitionSize:    meta.PartitionSize,
		PartitionType:    meta.PartitionType,
		PartitionID:      meta.PartitionID,
		ReplicaNum:       meta.ReplicaNum,
		Peers:            meta.Peers,
		Hosts:            meta.Hosts,
		RaftStore:        disk.space.GetRaftStore(),
		NodeID:           disk.space.GetNodeID(),
		ClusterID:        disk.space.GetClusterID(),
		IsEnableSnapshot: disk.space.dataNode.clusterEnableSnapshot,
	}
	if dp, err = newDataPartition(dpCfg, disk, false); err != nil {
		return
	}
	dp.stopRecover = meta.StopRecover
	dp.metaAppliedID = meta.ApplyID
	dp.isRepairing = meta.IsRepairing
	dp.computeUsage()
	dp.ForceSetDataPartitionToLoading()
	disk.space.AttachPartition(dp)
	if err = dp.LoadAppliedID(); err != nil {
		log.LogErrorf("action[LoadDataPartition] load apply id failed %v", err)
		dp.checkIsDiskError(err, ReadFlag)
		disk.space.DetachDataPartition(dp.partitionID)
		return
	}
	log.LogInfof("Action(LoadDataPartition) PartitionID(%v) meta(%v) stopRecover(%v) from disk(%v)",
		dp.partitionID, meta, meta.StopRecover, disk.Path)
	dp.DataPartitionCreateType = meta.DataPartitionCreateType
	dp.lastTruncateID = meta.LastTruncateID
	go dp.StartRaftLoggingSchedule()
	if meta.DataPartitionCreateType == proto.NormalCreateDataPartition {
		err = dp.StartRaft(true)
		func() {
			begin := time.Now()
			defer func() {
				log.LogInfof("[LoadDataPartition] load dp(%v) flush extent using time(%v)", dp.partitionID, time.Since(begin))
			}()
			dp.extentStore.Flush()
		}()
	} else {
		log.LogInfof("[LoadDataPartition] dp(%v) skip disk limit, need repair", dp.partitionID)
		// init leaderSize to partitionSize
		dp.leaderSize = dp.partitionSize
		dp.partitionStatus = proto.Recovering
		go dp.StartRaftAfterRepair(true)
	}
	if err != nil {
		log.LogErrorf("PartitionID(%v) start raft err(%v)..", dp.info(), err)
		dp.checkIsDiskError(err, ReadFlag)
		disk.space.DetachDataPartition(dp.partitionID)
		return
	}

	disk.AddSize(uint64(dp.Size()))
	dp.ForceLoadHeader()
	// if dp trigger disk error before, add it to diskErrPartitionSet
	dp.diskErrCnt = meta.DiskErrCnt
	if meta.DiskErrCnt > 0 {
		dp.stopRaft()
		disk.AddDiskErrPartition(dp.partitionID)
		diskErrPartitionCnt := disk.GetDiskErrPartitionCount()
		if diskErrPartitionCnt >= disk.dataNode.diskUnavailablePartitionErrorCount {
			msg := fmt.Sprintf("set disk unavailable for too many disk error, "+
				"disk path(%v), ip(%v), diskErrPartitionCnt(%v) threshold(%v)",
				disk.Path, LocalIP, diskErrPartitionCnt, disk.dataNode.diskUnavailablePartitionErrorCount)
			exporter.Warning(msg)
			log.LogWarnf(msg)
			disk.doDiskError()
		}
	}

	if disk.dataNode.raftPartitionCanUsingDifferentPort && lackRaftPort {
		// persist dp meta with raft port
		if err = dp.PersistMetadata(); err != nil {
			log.LogErrorf("persist dp(%v) meta with raft port error(%v)", dp.partitionID, err)
			return
		}
	}

	return
}

func newDataPartition(dpCfg *dataPartitionCfg, disk *Disk, isCreate bool) (dp *DataPartition, err error) {
	partitionID := dpCfg.PartitionID
	begin := time.Now()
	defer func() {
		log.LogInfof("[newDataPartition] load dp(%v) new data partition using time(%v)", partitionID, time.Since(begin))
	}()

	var dataPath string

	if proto.IsNormalDp(dpCfg.PartitionType) {
		dataPath = path.Join(disk.Path, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionID, dpCfg.PartitionSize))
	} else {
		return nil, fmt.Errorf("newDataPartition fail, dataPartitionCfg(%v)", dpCfg)
	}

	partition := &DataPartition{
		volumeID:                dpCfg.VolName,
		clusterID:               dpCfg.ClusterID,
		partitionID:             partitionID,
		replicaNum:              dpCfg.ReplicaNum,
		disk:                    disk,
		dataNode:                disk.dataNode,
		path:                    dataPath,
		partitionSize:           dpCfg.PartitionSize,
		partitionType:           dpCfg.PartitionType,
		replicas:                make([]string, 0),
		stopC:                   make(chan bool),
		stopRaftC:               make(chan uint64),
		storeC:                  make(chan uint64, 128),
		snapshot:                make([]*proto.File, 0),
		partitionStatus:         proto.ReadWrite,
		config:                  dpCfg,
		raftStatus:              RaftStatusStopped,
		verSeq:                  dpCfg.VerSeq,
		DataPartitionCreateType: dpCfg.CreateType,
		volVersionInfoList:      &proto.VolVersionInfoList{},
		responseStatus:          responseInitial,
		PersistApplyIdChan:      make(chan PersistApplyIdRequest),
	}

	if partition.dataNode.raftPartitionCanUsingDifferentPort {
		// during upgrade process, create partition request may lack raft ports info
		defaultHeartbeatPort, defaultReplicaPort, err := partition.raftPort()
		if err == nil {
			for i := range partition.config.Peers {
				if len(partition.config.Peers[i].ReplicaPort) == 0 || len(partition.config.Peers[i].HeartbeatPort) == 0 {
					partition.config.Peers[i].ReplicaPort = strconv.FormatInt(int64(defaultReplicaPort), 10)
					partition.config.Peers[i].HeartbeatPort = strconv.FormatInt(int64(defaultHeartbeatPort), 10)
				}
			}
		}
	}

	atomic.StoreUint64(&partition.recoverErrCnt, 0)
	log.LogInfof("action[newDataPartition] dp %v replica num %v", partitionID, dpCfg.ReplicaNum)

	VolsForbidWriteOpOfProtoVer0 := disk.dataNode.VolsForbidWriteOpOfProtoVer0
	if _, ok := VolsForbidWriteOpOfProtoVer0[partition.volumeID]; ok {
		partition.SetForbidWriteOpOfProtoVer0(true)
	} else {
		partition.SetForbidWriteOpOfProtoVer0(false)
	}
	log.LogInfof("[newDataPartition] vol(%v) dp(%v) IsForbidWriteOpOfProtoVer0: %v",
		dpCfg.VolName, partitionID, partition.IsForbidWriteOpOfProtoVer0())

	partition.replicasInit()
	partition.extentStore, err = storage.NewExtentStore(partition.path, dpCfg.PartitionID, dpCfg.PartitionSize,
		partition.partitionType, disk.dataNode.cacheCap, isCreate)
	if err != nil {
		log.LogWarnf("action[newDataPartition] dp %v NewExtentStore failed %v", partitionID, err.Error())
		return
	}
	partition.extentStore.IsEnableSnapshot = dpCfg.IsEnableSnapshot
	// store applyid
	if isCreate {
		log.LogInfof("action[newDataPartition] init apply id when create dp directly. dp %d", partitionID)
		if err = partition.storeAppliedID(partition.appliedID); err != nil {
			log.LogErrorf("action[newDataPartition] dp %v initial Apply [%v] failed: %v",
				partition.partitionID, partition.appliedID, err)
			partition.checkIsDiskError(err, WriteFlag)
			return
		}
	}

	if !fileutil.ExistDir(partition.path) {
		log.LogWarnf("action[newDataPartition] dp(%v) dir(%v) not exists", dp.partitionID, dp.Path())
		return
	}
	statusPath := path.Join(partition.path, DpStatusFile)
	fp, err := os.OpenFile(statusPath, os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		dp.checkIsDiskError(err, ReadFlag)
		return
	}
	defer fp.Close()

	disk.AttachDataPartition(partition)
	dp = partition

	go partition.statusUpdateScheduler()
	if isCreate && dpCfg.IsEnableSnapshot {
		if err = dp.getVerListFromMaster(); err != nil {
			log.LogErrorf("action[newDataPartition] vol %v dp %v loadFromMaster verList failed err %v", dp.volumeID, dp.partitionID, err)
			return
		}
	}

	log.LogInfof("action[newDataPartition] dp %v replica num %v CreateType %v create success",
		dp.partitionID, dpCfg.ReplicaNum, dp.DataPartitionCreateType)
	return
}

func (partition *DataPartition) HandleSetRepairingStatusOp(req *proto.SetDataPartitionRepairingStatusRequest) (err error) {
	var (
		reqData []byte
		pItem   *RaftCmdItem
	)
	if reqData, err = json.Marshal(req); err != nil {
		return
	}
	pItem = &RaftCmdItem{
		Op: uint32(proto.OpSetRepairingStatus),
		K:  []byte("setRepairingStatus"),
		V:  reqData,
	}
	data, _ := MarshalRaftCmd(pItem)
	_, err = partition.Submit(data)
	return
}

func (partition *DataPartition) fsmSetRepairingStatusOp(opItem *RaftCmdItem) (err error) {
	req := new(proto.SetDataPartitionRepairingStatusRequest)
	if err = json.Unmarshal(opItem.V, req); err != nil {
		log.LogErrorf("action[fsmSetRepairingStatusOp] dp[%v] op item %v", partition.partitionID, opItem)
		return
	}

	oldStatus := partition.isRepairing
	partition.isRepairing = req.RepairingStatus
	if err = partition.PersistMetadata(); err != nil {
		log.LogErrorf("action[fsmSetRepairingStatusOp] persist dp %v metadata failed, err: %v", partition.partitionID, err)
		partition.isRepairing = oldStatus
		return
	}
	log.LogWarnf("action[fsmSetRepairingStatusOp] %v set repairingStatus %v success", partition.partitionID, req.RepairingStatus)
	return
}

func (partition *DataPartition) HandleVersionOp(req *proto.MultiVersionOpRequest) (err error) {
	var (
		verData []byte
		pItem   *RaftCmdItem
	)
	if verData, err = json.Marshal(req); err != nil {
		return
	}
	pItem = &RaftCmdItem{
		Op: uint32(proto.OpVersionOp),
		K:  []byte("version"),
		V:  verData,
	}
	data, _ := MarshalRaftCmd(pItem)
	_, err = partition.Submit(data)
	return
}

func (partition *DataPartition) fsmVersionOp(opItem *RaftCmdItem) (err error) {
	req := new(proto.MultiVersionOpRequest)
	if err = json.Unmarshal(opItem.V, req); err != nil {
		log.LogErrorf("action[fsmVersionOp] dp[%v] op item %v", partition.partitionID, opItem)
		return
	}
	if len(req.VolVerList) == 0 {
		return
	}
	lastSeq := req.VolVerList[len(req.VolVerList)-1].Ver
	partition.volVersionInfoList.RWLock.Lock()
	if len(partition.volVersionInfoList.VerList) == 0 {
		partition.volVersionInfoList.VerList = make([]*proto.VolVersionInfo, len(req.VolVerList))
		copy(partition.volVersionInfoList.VerList, req.VolVerList)
		partition.verSeq = lastSeq
		log.LogInfof("action[fsmVersionOp] dp %v seq %v updateVerList reqeust ver %v verlist  %v  dp verlist nil and set",
			partition.partitionID, partition.verSeq, lastSeq, req.VolVerList)
		partition.volVersionInfoList.RWLock.Unlock()
		return
	}

	lastVerInfo := partition.volVersionInfoList.GetLastVolVerInfo()
	log.LogInfof("action[fsmVersionOp] dp %v seq %v lastVerList seq %v req seq %v op %v",
		partition.partitionID, partition.verSeq, lastVerInfo.Ver, lastSeq, req.Op)

	if lastVerInfo.Ver >= lastSeq {
		if lastVerInfo.Ver == lastSeq {
			if req.Op == proto.CreateVersionCommit {
				lastVerInfo.Status = proto.VersionNormal
			}
		}
		partition.volVersionInfoList.RWLock.Unlock()
		return
	}

	var status uint8 = proto.VersionPrepare
	if req.Op == proto.CreateVersionCommit {
		status = proto.VersionNormal
	}
	partition.volVersionInfoList.VerList = append(partition.volVersionInfoList.VerList, &proto.VolVersionInfo{
		Status: status,
		Ver:    lastSeq,
	})

	partition.verSeq = lastSeq

	err = partition.PersistMetadata()
	log.LogInfof("action[fsmVersionOp] dp %v seq %v updateVerList reqeust add new seq %v verlist (%v) err (%v)",
		partition.partitionID, partition.verSeq, lastSeq, partition.volVersionInfoList, err)

	partition.volVersionInfoList.RWLock.Unlock()
	return
}

func (dp *DataPartition) getVerListFromMaster() (err error) {
	var verList *proto.VolVersionInfoList
	verList, err = MasterClient.AdminAPI().GetVerList(dp.volumeID)
	if err != nil {
		log.LogErrorf("action[onStart] GetVerList err[%v]", err)
		return
	}

	for _, info := range verList.VerList {
		if info.Status != proto.VersionNormal {
			continue
		}
		dp.volVersionInfoList.VerList = append(dp.volVersionInfoList.VerList, info)
	}

	log.LogDebugf("action[onStart] dp %v verList %v", dp.partitionID, dp.volVersionInfoList.VerList)
	dp.verSeq = dp.volVersionInfoList.GetLastVer()
	return
}

func (dp *DataPartition) replicasInit() {
	replicas := make([]string, 0)
	if dp.config.Hosts == nil {
		return
	}
	replicas = append(replicas, dp.config.Hosts...)
	dp.replicasLock.Lock()
	dp.replicas = replicas
	dp.replicasLock.Unlock()
	if dp.config.Hosts != nil && len(dp.config.Hosts) >= 1 {
		leaderAddr := strings.Split(dp.config.Hosts[0], ":")
		if len(leaderAddr) == 2 && strings.TrimSpace(leaderAddr[0]) == LocalIP {
			dp.isLeader = true
		}
	}
}

func (dp *DataPartition) GetExtentCount() int {
	return dp.extentStore.GetExtentCount()
}

func (dp *DataPartition) Path() string {
	return dp.path
}

// IsRaftLeader tells if the given address belongs to the raft leader.
func (dp *DataPartition) IsRaftLeader() (addr string, ok bool) {
	if dp.raftStopped() {
		return
	}
	leaderID, _ := dp.raftPartition.LeaderTerm()
	if leaderID == 0 {
		return
	}
	ok = leaderID == dp.config.NodeID
	for _, peer := range dp.config.Peers {
		if leaderID == peer.ID {
			addr = peer.Addr
			return
		}
	}
	return
}

func (dp *DataPartition) getConfigHosts() []string {
	return dp.config.Hosts
}

func (dp *DataPartition) Replicas() []string {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	return dp.replicas
}

func (dp *DataPartition) getReplicaCopy() []string {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()

	tmpCopy := make([]string, len(dp.replicas))
	copy(tmpCopy, dp.replicas)

	return tmpCopy
}

func (dp *DataPartition) getReplicaAddr(index int) string {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	return dp.replicas[index]
}

func (dp *DataPartition) getReplicaLen() int {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	return len(dp.replicas)
}

func (dp *DataPartition) IsExistReplica(addr string) bool {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	for _, host := range dp.replicas {
		if host == addr {
			return true
		}
	}
	return false
}

func (dp *DataPartition) IsExistPeer(peer proto.Peer) bool {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	for _, localPeer := range dp.config.Peers {
		if peer.Addr == localPeer.Addr {
			return true
		}
	}
	return false
}

func (dp *DataPartition) IsExistReplicaWithNodeId(addr string, nodeID uint64) bool {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	for _, peer := range dp.config.Peers {
		if peer.Addr == addr && peer.ID == nodeID {
			return true
		}
	}
	return false
}

func (dp *DataPartition) ReloadSnapshot() {
	files, err := dp.extentStore.SnapShot()
	if err != nil {
		log.LogErrorf("ReloadSnapshot err %v", err)
		return
	}

	dp.snapshotMutex.Lock()
	for _, f := range dp.snapshot {
		storage.PutSnapShotFileToPool(f)
	}
	dp.snapshot = files
	dp.snapshotMutex.Unlock()
}

// Snapshot returns the snapshot of the data partition.
func (dp *DataPartition) SnapShot() (files []*proto.File) {
	dp.snapshotMutex.RLock()
	defer dp.snapshotMutex.RUnlock()

	return dp.snapshot
}

// Stop close the store and the raft store.
func (dp *DataPartition) Stop() {
	begin := time.Now()
	defer func() {
		msg := fmt.Sprintf("[Stop] stop dp(%v) using time(%v), slow(%v)", dp.info(), time.Since(begin),
			time.Since(begin) > 100*time.Millisecond)
		log.LogInfo(msg)
		auditlog.LogDataNodeOp("DataPartitionStop", msg, nil)
	}()
	dp.stopOnce.Do(func() {
		log.LogInfof("action[Stop]:dp(%v) stop once", dp.info())
		if dp.stopC != nil {
			close(dp.stopC)
		}
		// Close the store and raftstore.
		dp.stopRaft()
		dp.extentStore.Close()
		applyId := atomic.LoadUint64(&dp.appliedID)
		log.LogInfof("action[Stop]:dp(%v) store applyId %v", dp.info(), applyId)
		err := dp.storeAppliedID(applyId)
		if err != nil {
			log.LogErrorf("action[Stop]: failed to store applied index dp %v, err %v", dp.info(), err)
			dp.checkIsDiskError(err, WriteFlag)
		}
	})
}

// Disk returns the disk instance.
func (dp *DataPartition) Disk() *Disk {
	return dp.disk
}

// func (dp *DataPartition) IsRejectWrite() bool {
// 	return dp.Disk().RejectWrite
// }

// Status returns the partition status.
func (dp *DataPartition) Status() int {
	return dp.partitionStatus
}

// Size returns the partition size.
func (dp *DataPartition) Size() int {
	return dp.partitionSize
}

// Used returns the used space.
func (dp *DataPartition) Used() int {
	return dp.used
}

// Available returns the available space.
func (dp *DataPartition) Available() int {
	return dp.partitionSize - dp.used
}

func (dp *DataPartition) ReadOnlyReasons() uint32 {
	return dp.readOnlyReasons
}

func (dp *DataPartition) ForceLoadHeader() {
	dp.loadExtentHeaderStatus = FinishLoadDataPartitionExtentHeader
}

func (dp *DataPartition) RemoveAll(force bool) (err error) {
	dp.persistMetaMutex.Lock()
	defer dp.persistMetaMutex.Unlock()
	if force {
		originalPath := dp.Path()
		parent := path.Dir(originalPath)
		fileName := path.Base(originalPath)
		newFilename := BackupPartitionPrefix + fileName
		newPath := fmt.Sprintf("%v-%v", path.Join(parent, newFilename), time.Now().Format("20060102150405"))
		//_, err = os.Stat(newPath)
		//if err == nil {
		//	newPathWithTimestamp := fmt.Sprintf("%v-%v", newPath, time.Now().Format("20060102150405"))
		//	err = os.Rename(newPath, newPathWithTimestamp)
		//	if err != nil {
		//		log.LogWarnf("action[Stop]:dp(%v) rename dir from %v to %v,err %v", dp.info(), newPath, newPathWithTimestamp, err)
		//		return err
		//	}
		//}
		err = os.Rename(originalPath, newPath)
		if err == nil {
			dp.path = newPath
			dp.disk.AddBackupPartitionDir(dp.partitionID)
		}
		log.LogInfof("action[Stop]:dp(%v) rename dir from %v to %v,err %v", dp.info(), originalPath, newPath, err)
	} else {
		err = os.RemoveAll(dp.Path())
		log.LogInfof("action[Stop]:dp(%v) remove %v,err %v", dp.info(), dp.Path(), err)
	}
	return err
}

// PersistMetadata persists the file metadata on the disk.
func (dp *DataPartition) PersistMetadata() (err error) {
	dp.persistMetaMutex.Lock()
	defer dp.persistMetaMutex.Unlock()

	if !fileutil.ExistDir(dp.Path()) {
		log.LogWarnf("[PersistMetadata] dp(%v) persist metadata, but dp dir(%v) has been removed", dp.partitionID, dp.Path())
		return
	}

	var (
		metadataFile *os.File
		metaData     []byte
	)
	fileName := path.Join(dp.Path(), TempMetadataFileName)
	if metadataFile, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0o666); err != nil {
		return
	}
	defer func() {
		metadataFile.Sync()
		metadataFile.Close()
		os.Remove(fileName)
	}()

	md := &DataPartitionMetadata{
		VolumeID:                dp.config.VolName,
		PartitionID:             dp.config.PartitionID,
		ReplicaNum:              dp.config.ReplicaNum,
		PartitionSize:           dp.config.PartitionSize,
		PartitionType:           dp.config.PartitionType,
		Peers:                   dp.config.Peers,
		Hosts:                   dp.config.Hosts,
		DataPartitionCreateType: dp.DataPartitionCreateType,
		CreateTime:              time.Now().Format(TimeLayout),
		LastTruncateID:          dp.lastTruncateID,
		StopRecover:             dp.stopRecover,
		VerList:                 dp.volVersionInfoList.VerList,
		ApplyID:                 dp.appliedID,
		DiskErrCnt:              atomic.LoadUint64(&dp.diskErrCnt),
		IsRepairing:             dp.isRepairing,
	}

	if metaData, err = json.Marshal(md); err != nil {
		return
	}
	// persist meta can be failed with  io error
	if _, err = metadataFile.Write(metaData); err != nil {
		return
	}
	dp.metaAppliedID = dp.appliedID
	log.LogWarnf("PersistMetadata DataPartition(%v) data(%v)", dp.partitionID, string(metaData))
	err = os.Rename(fileName, path.Join(dp.Path(), DataPartitionMetadataFileName))
	return
}

func (dp *DataPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(time.Minute)
	snapshotTicker := time.NewTicker(time.Minute * 5)
	peersTicker := time.NewTicker(10 * time.Second)

	genCheckInterval := func() time.Duration {
		interval := DpMinCheckInterval + rand.Intn(DpMaxCheckInterval-DpMinCheckInterval+1)
		return time.Duration(interval) * time.Second
	}
	dpCheckTimer := time.NewTimer(genCheckInterval())

	var index int
	for {
		select {
		case <-ticker.C:
			dp.statusUpdate()

			index++
			if index >= math.MaxUint32 {
				index = 0
			}

			if index%2 == 0 {
				dp.LaunchRepair(proto.TinyExtentType)
			} else {
				dp.LaunchRepair(proto.NormalExtentType)
			}
		case <-snapshotTicker.C:
			dp.ReloadSnapshot()
		case <-peersTicker.C:
			dp.validatePeers()
		case <-dpCheckTimer.C:
			dp.checkAvailable()
			dpCheckTimer.Reset(genCheckInterval())
		case <-dp.stopC:
			ticker.Stop()
			snapshotTicker.Stop()
			peersTicker.Stop()
			dpCheckTimer.Stop()
			return
		}
	}
}

func (dp *DataPartition) statusUpdate() {
	status := proto.ReadWrite
	dp.computeUsage()

	dp.readOnlyReasons = proto.ReasonNone

	if dp.used >= dp.partitionSize {
		status = proto.ReadOnly
		dp.readOnlyReasons |= proto.DpOverCapacity
	}
	if dp.isNormalType() && dp.extentStore.GetExtentCount() >= storage.MaxExtentCount {
		status = proto.ReadOnly
		dp.readOnlyReasons |= proto.DpExtentLimit
	}
	if dp.IsBaseFileIDException() {
		status = proto.ReadOnly
		dp.readOnlyReasons |= proto.DpBaseFileException
	}
	if dp.disk.Status == proto.ReadOnly {
		status = proto.ReadOnly
		dp.readOnlyReasons |= proto.DiskReadOnly
	}
	if dp.isNormalType() && dp.raftStatus == RaftStatusStopped {
		// dp is still recovering
		if dp.DataPartitionCreateType == proto.DecommissionedCreateDataPartition {
			status = proto.Recovering
		} else {
			status = proto.Unavailable
		}
	}

	if dp.getDiskErrCnt() > 0 {
		status = proto.Unavailable
	}

	log.LogInfof("action[statusUpdate] dp %v raft status %v dp.status %v, status %v, readOnlyReasons_mask[0x%04x]. disk status %v canWrite(%v)",
		dp.info(), dp.raftStatus, dp.Status(), status, dp.readOnlyReasons, float64(dp.disk.Status), dp.disk.CanWrite())
	// dp.partitionStatus = int(math.Min(float64(status), float64(dp.disk.Status)))
	dp.partitionStatus = status
}

func (dp *DataPartition) computeUsage() {
	if dp.intervalToUpdatePartitionSize.Unix() != 0 &&
		time.Since(dp.intervalToUpdatePartitionSize) < IntervalToUpdatePartitionSize {
		log.LogDebugf("[computeUsage] dp(%v) skip size update", dp.partitionID)
		return
	}
	dp.used = int(dp.ExtentStore().GetStoreUsedSize())
	if log.EnableDebug() {
		log.LogDebugf("[computeUsage] dp(%v) update size(%v)", dp.partitionID, strutil.FormatSize(uint64(dp.used)))
	}
	dp.intervalToUpdatePartitionSize = time.Now()
}

func (dp *DataPartition) ExtentStore() *storage.ExtentStore {
	return dp.extentStore
}

func (dp *DataPartition) checkIsDiskError(err error, rwFlag uint8) {
	if err == nil {
		return
	}
	log.LogWarnf("checkIsDiskError: disk path %v, error: %v, partition:%v, rwFlag:%v",
		dp.Path(), err.Error(), dp.partitionID, rwFlag)
	if !IsDiskErr(err.Error()) {
		return
	}

	log.LogWarnf("[checkIsDiskError] disk(%v) dp(%v) meet io error", dp.Path(), dp.partitionID)
	dp.stopRaft()
	dp.incDiskErrCnt()
	dp.disk.triggerDiskError(rwFlag, dp.partitionID)

	// must after change disk.status
	dp.statusUpdate()
}

func newRaftApplyError(err error) error {
	return errors.NewErrorf("[Custom Error]: unhandled raft apply error, err(%s)", err)
}

func isRaftApplyError(errMsg string) bool {
	return strings.Contains(errMsg, "[Custom Error]: unhandled raft apply error")
}

// String returns the string format of the data partition information.
func (dp *DataPartition) String() (m string) {
	return fmt.Sprintf(DataPartitionPrefix+"_%v_%v", dp.partitionID, dp.partitionSize)
}

// LaunchRepair launches the repair of extents.
func (dp *DataPartition) LaunchRepair(extentType uint8) {
	if dp.partitionStatus == proto.Unavailable {
		return
	}
	if err := dp.updateReplicas(false); err != nil {
		if strings.Contains(err.Error(), proto.ErrDataPartitionNotExists.Error()) {
			log.LogWarnf("action[LaunchRepair] partition(%v) err(%v).", dp.partitionID, err)
			return
		}
		log.LogErrorf("action[LaunchRepair] partition(%v) err(%v).", dp.partitionID, err)
		return
	}
	if !dp.isLeader {
		return
	}
	if dp.extentStore.BrokenTinyExtentCnt() == 0 {
		dp.extentStore.MoveAllToBrokenTinyExtentC(MinTinyExtentsToRepair)
	}
	dp.repair(extentType)
}

func (dp *DataPartition) updateReplicas(isForce bool) (err error) {
	if !isForce && time.Now().Unix()-dp.intervalToUpdateReplicas <= IntervalToUpdateReplica {
		return
	}
	dp.isLeader = false
	isLeader, replicas, _, err := dp.fetchReplicasFromMaster()
	if err != nil {
		return
	}
	dp.replicasLock.Lock()
	defer dp.replicasLock.Unlock()
	if !dp.compareReplicas(dp.replicas, replicas) {
		log.LogInfof("action[updateReplicas] partition(%v) replicas changed from (%v) to (%v).",
			dp.partitionID, dp.replicas, replicas)
	}
	// only update isLeader, dp.replica can only be updated by member change. remove redundant triggered by master
	// would be failed for not found error
	dp.isLeader = isLeader
	// dp.replicas = replicas
	dp.intervalToUpdateReplicas = time.Now().Unix()
	log.LogInfof(fmt.Sprintf("ActionUpdateReplicationHosts partiton(%v), force(%v) isLeader(%v)",
		dp.partitionID, isForce, isLeader))

	return
}

// Compare the fetched replica with the local one.
func (dp *DataPartition) compareReplicas(v1, v2 []string) (equals bool) {
	if len(v1) == len(v2) {
		for i := 0; i < len(v1); i++ {
			if v1[i] != v2[i] {
				return false
			}
		}
		return true
	}
	return false
}

type ReplicaInfo struct {
	Addr string
	Disk string
}

// Fetch the replica information from the master.
func (dp *DataPartition) fetchReplicasFromMaster() (isLeader bool, replicas []string, infos []ReplicaInfo, err error) {
	var partition *proto.DataPartitionInfo
	retry := 0
	for {
		if partition, err = MasterClient.AdminAPI().GetDataPartition(dp.volumeID, dp.partitionID); err != nil {
			retry++
			if retry > 5 {
				isLeader = false
				return
			}
		} else {
			break
		}
		time.Sleep(10 * time.Second)
	}

	replicas = append(replicas, partition.Hosts...)
	for _, replica := range partition.Replicas {
		infos = append(infos, ReplicaInfo{Addr: replica.Addr, Disk: replica.DiskPath})
	}
	if partition.Hosts != nil && len(partition.Hosts) >= 1 {
		leaderAddr := strings.Split(partition.Hosts[0], ":")
		if len(leaderAddr) == 2 && strings.TrimSpace(leaderAddr[0]) == LocalIP {
			isLeader = true
		}
	}
	return
}

func (dp *DataPartition) Load() (response *proto.LoadDataPartitionResponse) {
	response = &proto.LoadDataPartitionResponse{}
	response.PartitionId = uint64(dp.partitionID)
	response.PartitionStatus = dp.partitionStatus
	response.Used = uint64(dp.Used())
	var err error

	if dp.loadExtentHeaderStatus != FinishLoadDataPartitionExtentHeader {
		response.PartitionSnapshot = make([]*proto.File, 0)
	} else {
		response.PartitionSnapshot = dp.SnapShot()
	}
	if err != nil {
		response.Status = proto.TaskFailed
		response.Result = err.Error()
		return
	}
	return
}

// DoExtentStoreRepair performs the repairs of the extent store.
// 1. when the extent size is smaller than the max size on the record, start to repair the missing part.
// 2. if the extent does not even exist, create the extent first, and then repair.
func (dp *DataPartition) DoExtentStoreRepair(repairTask *DataPartitionRepairTask) {
	if dp.stopRecover && dp.isDecommissionRecovering() {
		log.LogWarnf("DoExtentStoreRepair %v receive stop signal", dp.partitionID)
		return
	}
	store := dp.extentStore
	log.LogDebugf("DoExtentStoreRepair dp %v len extents to created %v type %v",
		dp.partitionID, len(repairTask.ExtentsToBeCreated), repairTask.TaskType)
	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		log.LogDebugf("DoExtentStoreRepair.dp %v len extentInfo %v", dp.partitionID, extentInfo)
		if storage.IsTinyExtent(extentInfo.FileID) {
			continue
		}
		if store.HasExtent(uint64(extentInfo.FileID)) {
			log.LogWarnf("DoExtentStoreRepair dp %v, extent(%v) is exist", dp.partitionID, extentInfo.FileID)
			continue
		}
		if !AutoRepairStatus {
			log.LogWarnf("DoExtentStoreRepair dp %v, AutoRepairStatus is False,so cannot Create extent(%v)",
				dp.partitionID, extentInfo.FileID)
			continue
		}

		var err error
		dp.disk.diskLimit(OpAsyncWrite, 0, func() {
			err = store.Create(extentInfo.FileID)
		})
		if err != nil {
			log.LogWarnf("DoExtentStoreRepair dp %v extent %v failed, err:%v",
				dp.partitionID, extentInfo.FileID, err.Error())
			continue
		}
	}

	var (
		wg           *sync.WaitGroup
		recoverIndex int
	)
	wg = new(sync.WaitGroup)
	log.LogDebugf("DoExtentStoreRepair dp %v len extents to repair %v type %v",
		dp.partitionID, len(repairTask.ExtentsToBeRepaired), repairTask.TaskType)
	log.LogInfof("[DoExtentStoreRepair] dp(%v) start repair extents len(%v)", dp.partitionID, len(repairTask.extents))
	for _, extentInfo := range repairTask.ExtentsToBeRepaired {
		log.LogDebugf("[DoExtentStoreRepair] dp(%v) repiar extent(%v)", dp.partitionID, extentInfo)
		if dp.dataNode.space.Partition(dp.partitionID) == nil {
			log.LogWarnf("DoExtentStoreRepair dp %v is detached, quit repair",
				dp.partitionID)
		}
		if dp.stopRecover && dp.isDecommissionRecovering() {
			log.LogWarnf("DoExtentStoreRepair %v receive stop signal", dp.partitionID)
			return
		}
		if !store.HasExtent(uint64(extentInfo.FileID)) {
			continue
		}
		wg.Add(1)

		// repair the extents
		go dp.doStreamExtentFixRepair(wg, extentInfo)
		recoverIndex++

		if recoverIndex%NumOfFilesToRecoverInParallel == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
	dp.doStreamFixTinyDeleteRecord(repairTask)
}

func (dp *DataPartition) pushSyncDeleteRecordFromLeaderMesg() bool {
	select {
	case dp.Disk().syncTinyDeleteRecordFromLeaderOnEveryDisk <- true:
		return true
	default:
		return false
	}
}

func (dp *DataPartition) consumeTinyDeleteRecordFromLeaderMesg() {
	select {
	case <-dp.Disk().syncTinyDeleteRecordFromLeaderOnEveryDisk:
		return
	default:
		return
	}
}

func (dp *DataPartition) doStreamFixTinyDeleteRecord(repairTask *DataPartitionRepairTask) {
	var (
		localTinyDeleteFileSize int64
		err                     error
		conn                    net.Conn
	)
	if !dp.pushSyncDeleteRecordFromLeaderMesg() {
		return
	}

	defer func() {
		dp.consumeTinyDeleteRecordFromLeaderMesg()
		if err != nil {
			log.LogWarnf("doStreamFixTinyDeleteRecord: occured error, dp %d, err %s", dp.partitionID, err.Error())
		}
	}()
	if localTinyDeleteFileSize, err = dp.extentStore.LoadTinyDeleteFileOffset(); err != nil {
		return
	}

	log.LogInfof(ActionSyncTinyDeleteRecord+" start PartitionID(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) leaderAddr(%v)",
		dp.partitionID, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteRecordFileSize, repairTask.LeaderAddr)

	if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteRecordFileSize {
		return
	}

	if repairTask.LeaderTinyDeleteRecordFileSize-localTinyDeleteFileSize < MinTinyExtentDeleteRecordSyncSize {
		return
	}

	defer func() {
		log.LogInfof(ActionSyncTinyDeleteRecord+" end PartitionID(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) leaderAddr(%v) err(%v)",
			dp.partitionID, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteRecordFileSize, repairTask.LeaderAddr, err)
	}()

	p := repl.NewPacketToReadTinyDeleteRecord(dp.partitionID, localTinyDeleteFileSize)
	if conn, err = dp.getRepairConn(repairTask.LeaderAddr); err != nil {
		return
	}
	defer func() {
		if dp.enableSmux() {
			dp.putRepairConn(conn, true)
		} else {
			dp.putRepairConn(conn, err != nil)
		}
	}()

	if err = p.WriteToConn(conn); err != nil {
		err = fmt.Errorf("write failed, remote %s, err %s", repairTask.LeaderAddr, err.Error())
		return
	}
	store := dp.extentStore
	start := time.Now().Unix()
	reqId := p.ReqID
	oldFileSize := localTinyDeleteFileSize
	for localTinyDeleteFileSize < repairTask.LeaderTinyDeleteRecordFileSize {
		if dp.stopRecover && dp.isDecommissionRecovering() {
			log.LogWarnf("doStreamFixTinyDeleteRecord %v receive stop signal", dp.partitionID)
			return
		}
		if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteRecordFileSize {
			return
		}
		if err = p.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			err = fmt.Errorf("read failed, remote %s, err %s", conn.RemoteAddr().String(), err.Error())
			return
		}
		if p.IsErrPacket() {
			logContent := fmt.Sprintf("action[doStreamFixTinyDeleteRecord] %v.",
				p.LogMessage(p.GetOpMsg(), conn.RemoteAddr().String(), start, fmt.Errorf(string(p.Data[:p.Size]))))
			err = fmt.Errorf(logContent)
			return
		}

		if p.ReqID != reqId {
			pStr := fmt.Sprintf("ext_%d_dp_%d_size_%d_req_%d_start_%d_dt_%d_oldReq_%d_oldSize_%d_nowSize_%d",
				p.ExtentID, p.PartitionID, p.Size, p.ReqID, p.StartT, len(p.Data), reqId, oldFileSize, localTinyDeleteFileSize)
			err = fmt.Errorf("action[doStreamFixTinyDeleteRecord] %s, remote %s, info %s. recive error pkt",
				p.String(), conn.RemoteAddr().String(), pStr)
			return
		}

		if p.CRC != crc32.ChecksumIEEE(p.Data[:p.Size]) {
			err = fmt.Errorf("crc not match")
			return
		}
		if p.Size%storage.DeleteTinyRecordSize != 0 {
			err = fmt.Errorf("unavali size")
			return
		}
		var index int
		for (index+1)*storage.DeleteTinyRecordSize <= int(p.Size) {
			record := p.Data[index*storage.DeleteTinyRecordSize : (index+1)*storage.DeleteTinyRecordSize]
			extentID, offset, size := storage.UnMarshalTinyExtent(record)
			localTinyDeleteFileSize += storage.DeleteTinyRecordSize
			index++
			if !storage.IsTinyExtent(extentID) {
				continue
			}
			DeleteLimiterWait()
			// log.LogInfof("doStreamFixTinyDeleteRecord Delete PartitionID(%v)_Extent(%v)_Offset(%v)_Size(%v)", dp.partitionID, extentID, offset, size)
			// store.MarkDelete(extentID, int64(offset), int64(size))
			store.RecordTinyDelete(extentID, int64(offset), int64(size))
			if store.IsClosed() {
				log.LogWarnf("dp %v in doStreamFixTinyDeleteRecord exit due to store is closed", dp.partitionID)
				return
			}
		}
	}
}

// ChangeRaftMember is a wrapper function of changing the raft member.
func (dp *DataPartition) ChangeRaftMember(changeType raftProto.ConfChangeType, peer raftProto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = dp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

func (dp *DataPartition) canRemoveSelf() (canRemove bool, err error) {
	var partition *proto.DataPartitionInfo
	retry := 0
	for {
		if partition, err = MasterClient.AdminAPI().GetDataPartition(dp.volumeID, dp.partitionID); err != nil {
			log.LogErrorf("action[canRemoveSelf] err[%v]", err)
			retry++
			if retry > 60 {
				return
			}
		} else {
			break
		}
		time.Sleep(10 * time.Second)
	}

	canRemove = false
	var existInPeers bool
	for _, peer := range partition.Peers {
		if dp.config.NodeID == peer.ID {
			existInPeers = true
		}
	}
	if !existInPeers {
		canRemove = true
		return
	}
	if dp.config.NodeID == partition.OfflinePeerID {
		canRemove = true
		return
	}
	return
}

func (dp *DataPartition) getRepairConn(target string) (net.Conn, error) {
	return dp.dataNode.getRepairConnFunc(target)
}

func (dp *DataPartition) enableSmux() bool {
	if dp.dataNode == nil {
		return false
	}
	return dp.dataNode.enableSmuxConnPool
}

func (dp *DataPartition) putRepairConn(conn net.Conn, forceClose bool) {
	log.LogDebugf("action[putRepairConn], forceClose: %v", forceClose)
	dp.dataNode.putRepairConnFunc(conn, forceClose)
}

func (dp *DataPartition) IsBaseFileIDException() bool {
	extID, _ := dp.extentStore.GetPersistenceBaseExtentID()
	return extID > storage.MaxExtentID
}

func (dp *DataPartition) isNormalType() bool {
	return proto.IsNormalDp(dp.partitionType)
}

func (dp *DataPartition) StopDecommissionRecover(stop bool) {
	// only work for decommission repair
	if !dp.isDecommissionRecovering() {
		log.LogWarnf("[StopDecommissionRecover]  dp(%d) is not in recovering status: type %d status %d",
			dp.partitionID, dp.partitionType, dp.Status())
		return
	}
	// for check timeout
	dp.stopRecover = stop
	dp.PersistMetadata()
}

func (dp *DataPartition) isDecommissionRecovering() bool {
	// decommission recover failed or success will set to normal
	return dp.DataPartitionCreateType == proto.DecommissionedCreateDataPartition
}

func (dp *DataPartition) incDiskErrCnt() {
	diskErrCnt := atomic.AddUint64(&dp.diskErrCnt, 1)
	err := dp.PersistMetadata()
	log.LogWarnf("[incDiskErrCnt]: dp(%v) disk err count:%v, err %v", dp.partitionID, diskErrCnt, err)
}

func (dp *DataPartition) getDiskErrCnt() uint64 {
	return atomic.LoadUint64(&dp.diskErrCnt)
}

func (dp *DataPartition) reload(s *SpaceManager) error {
	disk := dp.disk
	rootDir := dp.path
	log.LogDebugf("data partition rootDir %v", rootDir)
	s.DetachDataPartition(dp.partitionID)
	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
	log.LogDebugf("data partition %v is detached", dp.partitionID)
	_, err := LoadDataPartition(rootDir, disk)
	if err != nil {
		return err
	}
	return nil
}

func (dp *DataPartition) resetDiskErrCnt() {
	atomic.StoreUint64(&dp.diskErrCnt, 0)
}

func (dp *DataPartition) hasNodeIDConflict(addr string, nodeID uint64) error {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	for _, peer := range dp.config.Peers {
		if peer.Addr == addr && peer.ID != nodeID {
			return errors.NewErrorf(fmt.Sprintf("local nodeID for %v is %v(expected:%v)", peer.Addr, peer.ID, nodeID))
		}
	}
	return nil
}

func (dp *DataPartition) info() string {
	diskPath := ""
	if dp.disk != nil {
		diskPath = dp.disk.Path
	}
	return fmt.Sprintf("id(%v)_disk(%v)_type(%v)", dp.partitionID, diskPath, dp.partitionType)
}

func (dp *DataPartition) validatePeers() {
	dataNodes := dp.dataNode.space.getDataNodeIDs()
	for _, peer := range dp.config.Peers {
		for _, dn := range dataNodes {
			if dn.Addr == peer.Addr && dn.ID != peer.ID {
				log.LogWarnf("dp %v find expired peer %v(expected %v_%v)", dp.info(), peer, dn.ID, dn.Addr)
				newReq := &proto.RemoveDataPartitionRaftMemberRequest{
					PartitionId: dp.partitionID,
					Force:       true,
					RemovePeer:  peer,
				}
				reqData, err := json.Marshal(newReq)
				if err != nil {
					log.LogWarnf("dp %v marshal newReq %v failed %v", dp.info(), newReq, err)
					continue
				}
				cc := &raftProto.ConfChange{
					Type: raftProto.ConfRemoveNode,
					Peer: raftProto.Peer{
						ID: peer.ID,
					},
					Context: reqData,
				}
				dp.dataNode.space.raftStore.RaftServer().RemoveRaftForce(dp.partitionID, cc)
				dp.ApplyMemberChange(cc, 0)
				dp.PersistMetadata()
				log.LogWarnf("dp %v remove expired peer %v", dp.info(), peer)
			}
		}
	}
}

func (dp *DataPartition) checkAvailable() (err error) {
	path := path.Join(dp.path, DpStatusFile)
	fp, err := os.OpenFile(path, os.O_TRUNC|os.O_RDWR, 0o755)
	if err != nil {
		dp.checkIsDiskError(err, ReadFlag)
		return
	}
	defer fp.Close()
	data := []byte(DpStatusFile)
	_, err = fp.WriteAt(data, 0)
	if err != nil {
		dp.checkIsDiskError(err, WriteFlag)
		return
	}
	if err = fp.Sync(); err != nil {
		dp.checkIsDiskError(err, WriteFlag)
		return
	}
	if _, err = fp.ReadAt(data, 0); err != nil {
		dp.checkIsDiskError(err, ReadFlag)
		if dp.partitionStatus == proto.Unavailable {
			log.LogErrorf("[checkAvailable] dp %v is unavailable", dp.partitionID)
		}
	}
	return
}

func (dp *DataPartition) GetExtentCountWithoutLock() int {
	return dp.extentStore.GetExtentCountWithoutLock()
}

func (dp *DataPartition) setChangeMemberWaiting() bool {
	return atomic.CompareAndSwapUint32(&dp.responseStatus, responseInitial, responseWait)
}

func (dp *DataPartition) setRestoreReplicaFinish() bool {
	return atomic.CompareAndSwapUint32(&dp.responseStatus, responseWait, responseInitial)
}

func (dp *DataPartition) EvictExtentCache() {
	err := dp.extentStore.DoExtentCacheTtl(dp.dataNode.ExtentCacheTtlByMin)
	if err != nil {
		log.LogWarnf("[EvictExtentCache] DoExtentCacheTtl err: %s", err.Error())
	}
}
