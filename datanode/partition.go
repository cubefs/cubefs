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

package datanode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/master"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
	"sort"
	"syscall"
)

const (
	DataPartitionPrefix       = "datapartition"
	DataPartitionMetaFileName = "META"
	TempMetaFileName          = ".meta"
	ApplyIndexFile            = "APPLY"
	TempApplyIndexFile        = ".apply"
	TimeLayout                = "2006-01-02 15:04:05"
)

var (
	AdminGetDataPartition = master.AdminGetDataPartition
)

// TODO it seems that this is the metadata of a data partition. if my understanding is correct, we should name it as "DPMetadata" or simply "MetadataArray"
type DPMetadata struct { // DataPartitionMetadata
	VolumeID      string
	PartitionID   uint64
	PartitionSize int
	CreateTime    string
	RandomWrite   bool  // isRandomWrite
	Peers         []proto.Peer
}

type sortPeers []proto.Peer

// TODO is it the right place for sortPeers?
func (sp sortPeers) Len() int {
	return len(sp)
}

func (sp sortPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

func (md *DPMetadata) Validate() (err error) {
	md.VolumeID = strings.TrimSpace(md.VolumeID)
	if len(md.VolumeID) == 0 || md.PartitionID == 0 || md.PartitionSize == 0 {
		err = errors.New("illegal data partition metadata")
		return
	}
	return
}

type DataPartition struct {
	clusterID       string
	volumeID        string
	partitionID     uint64
	partitionStatus int
	partitionSize   int
	replicas        []string // addresses of the replicas
	disk            *Disk
	isLeader        bool
	isRaftLeader    bool
	path            string
	used            int
	extentStore     *storage.ExtentStore
	raftPartition   raftstore.Partition
	config          *dataPartitionCfg
	applyID         uint64 // TODO what is applyID?   raft使用的， 在raft里面已经用到的applidID  Raft 里面的
	lastTruncateID  uint64 // TODO what is lastTruncateID?    Raft 里面的
	minAppliedID    uint64 // TODO what is appliedID?  Raft 里面的
	maxAppliedID    uint64 // Raft 里面的
	repairC         chan uint64
	storeC          chan uint64
	stopC           chan bool

	runtimeMetrics          *DataPartitionMetrics
	updateReplicationTime   int64 // TODO what is updateReplicationTime? timestamp?  每隔多少分钟去请求master当前partition的复制组的关系
	isFirstFixTinyExtents   bool // TODO what is isFirstFixTinyExtents?  第一次启动的时候必须把所有的tinyextent 修复 shouldRepairAllExtents
	snapshot                []*proto.File
	snapshotLock            sync.RWMutex // TODO should we call it snapshotMutex?
	intervalToUpdatePartitionSize int64
}

func CreateDataPartition(dpCfg *dataPartitionCfg, disk *Disk) (dp *DataPartition, err error) {

	if dp, err = newDataPartition(dpCfg, disk); err != nil {
		return
	}

	// Start raft for random write
	if dpCfg.RandomWrite {
		go dp.StartSchedule()
		go dp.StartRaftAfterRepair()
	}

	// Store meta information into meta file.
	err = dp.PersistMetadata()
	return
}

// LoadDataPartition loads and returns a partition instance based on the specified directory.
// It reads the partition metadata file stored under the specified directory
// and creates the partition instance.
func LoadDataPartition(partitionDir string, disk *Disk) (dp *DataPartition, err error) {
	var (
		metaFileData []byte
	)
	if metaFileData, err = ioutil.ReadFile(path.Join(partitionDir, DataPartitionMetaFileName)); err != nil {
		return
	}
	meta := &DPMetadata{}
	if err = json.Unmarshal(metaFileData, meta); err != nil {
		return
	}
	if err = meta.Validate(); err != nil {
		return
	}

	dpCfg := &dataPartitionCfg{
		VolName:       meta.VolumeID,
		PartitionSize: meta.PartitionSize,
		PartitionID:   meta.PartitionID,
		RandomWrite:   meta.RandomWrite,
		Peers:         meta.Peers,
		RaftStore:     disk.space.GetRaftStore(),
		NodeID:        disk.space.GetNodeID(),
		ClusterID:     disk.space.GetClusterID(),
	}
	if dp, err = newDataPartition(dpCfg, disk); err != nil {
		return
	}

	if dpCfg.RandomWrite {
		if err = dp.LoadApplyIndex(); err != nil {
			log.LogErrorf("action[loadApplyIndex] %v", err)
		}

		if err = dp.StartRaft(); err != nil {
			return
		}

		go dp.StartSchedule()
	}
	return
}

func newDataPartition(dpCfg *dataPartitionCfg, disk *Disk) (dp *DataPartition, err error) {
	partitionID := dpCfg.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionID, dpCfg.PartitionSize))
	partition := &DataPartition{
		volumeID:        dpCfg.VolName,
		clusterID:       dpCfg.ClusterID,
		partitionID:     partitionID,
		disk:            disk,
		path:            dataPath,
		partitionSize:   dpCfg.PartitionSize,
		replicas:    make([]string, 0),
		stopC:           make(chan bool, 0),
		repairC:         make(chan uint64, 0),
		storeC:          make(chan uint64, 128),
		snapshot:        make([]*proto.File, 0),
		partitionStatus: proto.ReadWrite,
		runtimeMetrics:  NewDataPartitionMetrics(),
		config:          dpCfg,
	}
	partition.extentStore, err = storage.NewExtentStore(partition.path, dpCfg.PartitionID, dpCfg.PartitionSize)
	if err != nil {
		return
	}

	// TODO change the name
	partition.isFirstFixTinyExtents = true
	disk.AttachDataPartition(partition)
	dp = partition
	go partition.statusUpdateScheduler()
	return
}


func (dp *DataPartition) ID() uint64 {
	return dp.partitionID
}

func (dp *DataPartition) GetExtentCount() int {
	dp.snapshotLock.RLock()
	defer dp.snapshotLock.RUnlock()
	return len(dp.snapshot)
}

// TODO is this necessary?
func (dp *DataPartition) Path() string {
	return dp.path
}

// IsRaftLeader tells if the given address belongs to the raft leader.
func (dp *DataPartition) IsRaftLeader() (addr string, ok bool) {
	if dp.raftPartition == nil {
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

// TODO is it necessary?
func (dp *DataPartition) Replicas() []string {
	return dp.replicas
}

func (dp *DataPartition) ReloadSnapshot() {
	files, err := dp.extentStore.SnapShot()
	if err != nil {
		return
	}
	dp.snapshotLock.Lock()
	dp.snapshot = files
	dp.snapshotLock.Unlock()
}

// Snapshot returns the snapshot of the data partition.
func (dp *DataPartition) SnapShot() (files []*proto.File) {
	dp.snapshotLock.RLock()
	defer dp.snapshotLock.RUnlock()

	return dp.snapshot
}

// Stop close the store and the raft store.
func (dp *DataPartition) Stop() {
	if dp.stopC != nil {
		close(dp.stopC)
	}
	// Close the store and raftstore.
	dp.extentStore.Close()
	dp.stopRaft()
}

// TODO is it necessary?
func (dp *DataPartition) FlushDelete() (err error) {
	err = dp.extentStore.FlushDelete()
	return
}

// TODO is it necessary?
func (dp *DataPartition) Disk() *Disk {
	return dp.disk
}

// TODO is it necessary?
func (dp *DataPartition) Status() int {
	return dp.partitionStatus
}

// TODO is it necessary?
func (dp *DataPartition) Size() int {
	return dp.partitionSize
}

// TODO is it necessary?
func (dp *DataPartition) Used() int {
	return dp.used
}

// TODO is it necessary?
func (dp *DataPartition) Available() int {
	return dp.partitionSize - dp.used
}

// TODO what does this mean? remove
func (dp *DataPartition) ChangeStatus(status int) {
	switch status {
	case proto.ReadOnly, proto.ReadWrite, proto.Unavaliable:
		dp.partitionStatus = status
	}
}


// PersistMetadata persists the file metadata on the disk.
func (dp *DataPartition) PersistMetadata() (err error) {
	var (
		metaFile *os.File
		metaData []byte
	)
	tempFileName := path.Join(dp.Path(), TempMetaFileName)
	if metaFile, err = os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	defer func() {
		metaFile.Sync()
		metaFile.Close()
		os.Remove(tempFileName)
	}()

	sp := sortPeers(dp.config.Peers)
	sort.Sort(sp)

	md := &DPMetadata{
		VolumeID:      dp.config.VolName,
		PartitionID:   dp.config.PartitionID,
		PartitionSize: dp.config.PartitionSize,
		Peers:         dp.config.Peers,
		RandomWrite:   dp.config.RandomWrite,
		CreateTime:    time.Now().Format(TimeLayout),
	}
	if metaData, err = json.Marshal(md); err != nil {
		return
	}
	if _, err = metaFile.Write(metaData); err != nil {
		return
	}

	err = os.Rename(tempFileName, path.Join(dp.Path(), DataPartitionMetaFileName))
	return
}

func (dp *DataPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(10 * time.Second)
	metricTicker := time.NewTicker(5 * time.Second)
	var index int
	for {
		select {
		case <-ticker.C:
			index++
			dp.statusUpdate()
			if index >= math.MaxUint32 {
				index = 0
			}
			if index%2 == 0 {
				dp.LaunchRepair(proto.TinyExtentType)
			} else {
				dp.LaunchRepair(proto.NormalExtentType)
			}
			dp.ReloadSnapshot()
		case <-dp.stopC:
			ticker.Stop()
			metricTicker.Stop()
			return
		case <-metricTicker.C:
			dp.runtimeMetrics.recomputeLatency()
		}
	}
}

func (dp *DataPartition) statusUpdate() {
	status := proto.ReadWrite
	dp.computeUsage()

	// TODO why not combine these two conditions together? 放在一块可以
	if dp.used >= dp.partitionSize {
		status = proto.ReadOnly
	}
	if dp.extentStore.ExtentCount() >= MaxActiveExtents {
		status = proto.ReadOnly
	}

	// TODO what does the compare mean here?
	// explain
	dp.partitionStatus = int(math.Min(float64(status), float64(dp.disk.Status)))
}

// TODO why this name is capitalized? 改小写
func ParseFileName(filename string) (extentID uint64, isExtent bool) {
	if isExtent = storage.RegexpExtentFile.MatchString(filename); !isExtent {
		return
	}
	var (
		err error
	)
	if extentID, err = strconv.ParseUint(filename, 10, 64); err != nil {
		isExtent = false
		return
	}
	isExtent = true
	return
}

func (dp *DataPartition) actualSize(path string, finfo os.FileInfo) (size int64) {
	name := finfo.Name()
	extentID, isExtent := ParseFileName(name)
	if !isExtent {
		return finfo.Size()
	}
	if storage.IsTinyExtent(extentID) {
		stat := new(syscall.Stat_t)
		err := syscall.Stat(fmt.Sprintf("%v/%v", path, finfo.Name()), stat)
		if err != nil {
			return finfo.Size()
		}
		return stat.Blocks * DiskSectorSize
	}

	return finfo.Size()
}

func (dp *DataPartition) computeUsage() {
	var (
		used  int64
		files []os.FileInfo
		err   error
	)
	if time.Now().Unix() - dp.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize {
		return
	}
	if files, err = ioutil.ReadDir(dp.path); err != nil {
		return
	}
	for _, file := range files {
		used += dp.actualSize(dp.path, file)
	}
	dp.used = int(used)
	dp.intervalToUpdatePartitionSize = time.Now().Unix()
}

// TODO why this wrapper?
func (dp *DataPartition) ExtentStore() *storage.ExtentStore {
	return dp.extentStore
}

// String returns the string format of the data partition information.
func (dp *DataPartition) String() (m string) {
	return fmt.Sprintf(DataPartitionPrefix+"_%v_%v", dp.partitionID, dp.partitionSize)
}

// LaunchRepair launches the repair of extens
// TODO needs some explanations here
//
func (dp *DataPartition) LaunchRepair(extentType uint8) {
	if dp.partitionStatus == proto.Unavaliable {
		return
	}
	if err := dp.updateReplicas(); err != nil {
		log.LogErrorf("action[LaunchRepair] err(%v).", err)
		return
	}
	if !dp.isLeader {
		return
	}
	if dp.extentStore.BadTinyExtentCnt() == 0 {
		dp.extentStore.MoveAllToBadTinyExtentC(MinFixTinyExtents)
	}
	dp.repair(extentType)
}

func (dp *DataPartition) updateReplicas() (err error) {
	if time.Now().Unix() - dp.updateReplicationTime <= IntervalToUpdateReplica {
		return
	}
	dp.isLeader = false
	isLeader, replicas, err := dp.fetchReplicasFromMaster()
	if err != nil {
		return
	}
	if !dp.compareReplicas(dp.replicas, replicas) {
		log.LogInfof("action[updateReplicas] partition(%v) replicas changed from (%v) to (%v).",
			dp.partitionID, dp.replicas, replicas)
	}
	dp.isLeader = isLeader
	dp.replicas = replicas
	dp.updateReplicationTime = time.Now().Unix()
	log.LogInfof(fmt.Sprintf("ActionUpdateReplicationHosts partiton[%v]", dp.partitionID))

	return
}

// Compare the fetched replica with the local one.
func (dp *DataPartition) compareReplicas(v1, v2 []string) (equals bool) {
	equals = true
	if len(v1) == len(v2) {
		for i := 0; i < len(v1); i++ {
			if v1[i] != v2[i] {
				equals = false
				return
			}
		}
		equals = true
		return
	}
	equals = false
	return
}

// Fetch the replica information from the master.
func (dp *DataPartition) fetchReplicasFromMaster() (isLeader bool, replicas []string, err error) {

	// TODO why capitalized var name?
	var (
		HostBuf []byte
	)
	params := make(map[string]string)
	params["id"] = strconv.Itoa(int(dp.partitionID))
	if HostBuf, err = MasterHelper.Request("GET", AdminGetDataPartition, params, nil); err != nil {
		isLeader = false
		return
	}
	response := &master.DataPartition{}
	replicas = make([]string, 0)
	if err = json.Unmarshal(HostBuf, &response); err != nil {
		isLeader = false
		replicas = nil
		return
	}
	for _, host := range response.Hosts {
		replicas = append(replicas, host)
	}
	if response.Hosts != nil && len(response.Hosts) >= 1 {
		leaderAddr := strings.Split(response.Hosts[0], ":")
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
	response.PartitionSnapshot = dp.SnapShot()
	return
}

// TODO it seems that there is no usage of this function
func (dp *DataPartition) GetAllExtentsMeta() (files []*storage.ExtentInfo, err error) {
	files, err = dp.extentStore.GetAllWatermarks(storage.NormalExtentFilter())
	if err != nil {
		return nil, err
	}

	return
}

// TODO needs some explanation here
/*
 成员收到extent修复通知
 1. Extent size小于最大Size，从extent未开始修复，补齐缺失部分内容
 2. Extent不存在，先创建Extent，然后修复整个extent文件
*/
// DoExtentStoreRepair performs the repairs of the extent store.
// 修复长度加上
func (dp *DataPartition) DoExtentStoreRepair(repairTask *DataPartitionRepairTask) {
	store := dp.extentStore
	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		if storage.IsTinyExtent(extentInfo.FileID) {
			continue
		}
		if store.HasExtent(uint64(extentInfo.FileID)) {
			info := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentInfo.FileID, Size: extentInfo.Size}
			repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
			continue
		}
		err := store.Create(uint64(extentInfo.FileID), extentInfo.Inode)
		if err != nil {
			continue
		}
		info := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentInfo.FileID, Size: extentInfo.Size}
		repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
	}
	var (
		wg           *sync.WaitGroup
		recoverIndex int
	)
	wg = new(sync.WaitGroup)
	for _, extentInfo := range repairTask.ExtentsToBeRepaired {

		// TODO why we do this check again?
		if !store.HasExtent(uint64(extentInfo.FileID)) {
			continue
		}
		wg.Add(1)

		// repair the extents
		go dp.doStreamExtentFixRepair(wg, extentInfo)
		recoverIndex++

		// TODO should the following line be wrapped ?
		if recoverIndex % NumOfFilesToRecoverInParallel == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
}

// TODO is this function ever used ?
func (dp *DataPartition) AddWriteMetrics(latency uint64) {
	dp.runtimeMetrics.UpdateWriteMetrics(latency)
}

// TODO is this function ever used ?
func (dp *DataPartition) AddReadMetrics(latency uint64) {
	dp.runtimeMetrics.UpdateReadMetrics(latency)
}

// TODO why do we need this wrapper?
// ChangeRaftMember is a wrapper function of changing the raft member.
func (dp *DataPartition) ChangeRaftMember(changeType raftProto.ConfChangeType, peer raftProto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = dp.raftPartition.ChangeMember(changeType, peer, context)
	return
}
