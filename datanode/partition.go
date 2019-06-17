// Copyright 2018 The Chubao Authors.
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

	"github.com/chubaofs/chubaofs/master"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
	"hash/crc32"
	"net"
	"sort"
	"syscall"
)

const (
	DataPartitionPrefix           = "datapartition"
	DataPartitionMetadataFileName = "META"
	TempMetadataFileName          = ".meta"
	ApplyIndexFile                = "APPLY"
	TempApplyIndexFile            = ".apply"
	TimeLayout                    = "2006-01-02 15:04:05"
)

type DataPartitionMetadata struct {
	VolumeID      string
	PartitionID   uint64
	PartitionSize int
	CreateTime    string
	Peers         []proto.Peer
}

type sortedPeers []proto.Peer

func (sp sortedPeers) Len() int {
	return len(sp)
}

func (sp sortedPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortedPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

func (md *DataPartitionMetadata) Validate() (err error) {
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
	appliedID       uint64 // apply id used in Raft
	lastTruncateID  uint64 // truncate id used in Raft
	minAppliedID    uint64
	maxAppliedID    uint64
	stopRaftC       chan uint64
	storeC          chan uint64
	stopC           chan bool

	runtimeMetrics                *DataPartitionMetrics
	intervalToUpdateReplicas      int64 // interval to ask the master for updating the replica information
	snapshot                      []*proto.File
	snapshotMutex                 sync.RWMutex
	intervalToUpdatePartitionSize int64
	loadExtentHeaderStatus        int

	FullSyncTinyDeleteTime int64
}

func CreateDataPartition(dpCfg *dataPartitionCfg, disk *Disk) (dp *DataPartition, err error) {

	if dp, err = newDataPartition(dpCfg, disk); err != nil {
		return
	}

	go dp.StartRaftLoggingSchedule()
	go dp.StartRaftAfterRepair()
	go dp.ForceLoadHeader()

	// persist file metadata
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
	if metaFileData, err = ioutil.ReadFile(path.Join(partitionDir, DataPartitionMetadataFileName)); err != nil {
		return
	}
	meta := &DataPartitionMetadata{}
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
		Peers:         meta.Peers,
		RaftStore:     disk.space.GetRaftStore(),
		NodeID:        disk.space.GetNodeID(),
		ClusterID:     disk.space.GetClusterID(),
	}
	if dp, err = newDataPartition(dpCfg, disk); err != nil {
		return
	}

	if err = dp.LoadAppliedID(); err != nil {
		log.LogErrorf("action[loadApplyIndex] %v", err)
	}

	if err = dp.StartRaft(); err != nil {
		return
	}

	go dp.StartRaftLoggingSchedule()
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
		replicas:        make([]string, 0),
		stopC:           make(chan bool, 0),
		stopRaftC:       make(chan uint64, 0),
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

	disk.AttachDataPartition(partition)
	dp = partition
	go partition.statusUpdateScheduler()
	return
}

func (dp *DataPartition) GetExtentCount() int {
	return dp.extentStore.GetExtentCount()
}

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

func (dp *DataPartition) Replicas() []string {
	return dp.replicas
}

func (dp *DataPartition) ReloadSnapshot() {
	files, err := dp.extentStore.SnapShot()
	if err != nil {
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
	if dp.stopC != nil {
		close(dp.stopC)
	}
	// Close the store and raftstore.
	dp.extentStore.Close()
	dp.stopRaft()
}

// Disk returns the disk instance.
func (dp *DataPartition) Disk() *Disk {
	return dp.disk
}

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

func (dp *DataPartition) ForceLoadHeader() {
	dp.loadExtentHeaderStatus = FinishLoadDataPartitionExtentHeader
}

// PersistMetadata persists the file metadata on the disk.
func (dp *DataPartition) PersistMetadata() (err error) {
	var (
		metadataFile *os.File
		metaData     []byte
	)
	fileName := path.Join(dp.Path(), TempMetadataFileName)
	if metadataFile, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	defer func() {
		metadataFile.Sync()
		metadataFile.Close()
		os.Remove(fileName)
	}()

	sp := sortedPeers(dp.config.Peers)
	sort.Sort(sp)

	md := &DataPartitionMetadata{
		VolumeID:      dp.config.VolName,
		PartitionID:   dp.config.PartitionID,
		PartitionSize: dp.config.PartitionSize,
		Peers:         dp.config.Peers,
		CreateTime:    time.Now().Format(TimeLayout),
	}
	if metaData, err = json.Marshal(md); err != nil {
		return
	}
	if _, err = metadataFile.Write(metaData); err != nil {
		return
	}

	err = os.Rename(fileName, path.Join(dp.Path(), DataPartitionMetadataFileName))
	return
}
func (dp *DataPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(time.Minute)
	snapshotTicker := time.NewTicker(time.Minute * 5)
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
		case <-snapshotTicker.C:
			dp.ReloadSnapshot()
		case <-dp.stopC:
			ticker.Stop()
			snapshotTicker.Stop()
			return
		}
	}
}

func (dp *DataPartition) statusUpdate() {
	status := proto.ReadWrite
	dp.computeUsage()

	if dp.used >= dp.partitionSize {
		status = proto.ReadOnly
	}
	if dp.extentStore.GetExtentCount() >= storage.MaxExtentCount {
		status = proto.ReadOnly
	}

	dp.partitionStatus = int(math.Min(float64(status), float64(dp.disk.Status)))
}

func parseFileName(filename string) (extentID uint64, isExtent bool) {
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
	extentID, isExtent := parseFileName(name)
	if !isExtent {
		return 0
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
	if time.Now().Unix()-dp.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize {
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

func (dp *DataPartition) ExtentStore() *storage.ExtentStore {
	return dp.extentStore
}

func (dp *DataPartition) checkIsDiskError(err error) {
	if err == nil {
		return
	}
	if IsDiskErr(err.Error()) {
		mesg := fmt.Sprintf("disk path %v error on %v", dp.Path, LocalIP)
		exporter.NewAlarm(mesg)
		log.LogErrorf(mesg)
		dp.stopRaft()
		dp.disk.incReadErrCnt()
		dp.disk.incWriteErrCnt()
		dp.disk.Status = proto.Unavailable
		dp.statusUpdate()
		dp.disk.ForceExitRaftStore()
	}
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
	if err := dp.updateReplicas(); err != nil {
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

func (dp *DataPartition) updateReplicas() (err error) {
	if time.Now().Unix()-dp.intervalToUpdateReplicas <= IntervalToUpdateReplica {
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
	dp.intervalToUpdateReplicas = time.Now().Unix()
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

	var (
		bufs []byte
	)
	params := make(map[string]string)
	params["id"] = strconv.Itoa(int(dp.partitionID))
	if bufs, err = MasterHelper.Request("GET", proto.AdminGetDataPartition, params, nil); err != nil {
		isLeader = false
		return
	}
	response := &master.DataPartition{}
	replicas = make([]string, 0)
	if err = json.Unmarshal(bufs, &response); err != nil {
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
	store := dp.extentStore
	if len(repairTask.ExtentsToBeCreated) > 0 {
		allAppliedIDs := dp.getOtherAppliedID()
		if len(allAppliedIDs) > 0 {
			minAppliedID := allAppliedIDs[0]
			for i := 1; i < len(allAppliedIDs); i++ {
				if allAppliedIDs[i] < minAppliedID {
					minAppliedID = allAppliedIDs[i]
				}
			}
			if minAppliedID > 0 {
				dp.appliedID = minAppliedID
			}
		}
	}

	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		if storage.IsTinyExtent(extentInfo.FileID) {
			continue
		}
		if store.HasExtent(uint64(extentInfo.FileID)) {
			info := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentInfo.FileID, Size: extentInfo.Size}
			repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
			continue
		}
		err := store.Create(uint64(extentInfo.FileID))
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
	dp.doStreamFixTinyDeleteRecord(repairTask, time.Now().Unix()-dp.FullSyncTinyDeleteTime > MaxFullSyncTinyDeleteTime)
}

func (dp *DataPartition) doStreamFixTinyDeleteRecord(repairTask *DataPartitionRepairTask, isFullSync bool) {
	var (
		localTinyDeleteFileSize int64
		err                     error
		conn                    *net.TCPConn
	)
	if !isFullSync {
		localTinyDeleteFileSize = dp.extentStore.LoadTinyDeleteFileOffset()
		dp.FullSyncTinyDeleteTime = time.Now().Unix()
	}

	log.LogInfof(ActionSyncTinyDeleteRecord+" start partitionId(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) leaderAddr(%v)",
		dp.partitionID, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteFileSize, repairTask.LeaderAddr)
	if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteFileSize {
		return
	}
	defer func() {
		log.LogInfof(ActionSyncTinyDeleteRecord+" end partitionId(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) leaderAddr(%v) err(%v)",
			dp.partitionID, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteFileSize, repairTask.LeaderAddr, err)
	}()

	p := repl.NewPacketToTinyDeleteRecord(dp.partitionID, localTinyDeleteFileSize)
	if conn, err = gConnPool.GetConnect(repairTask.LeaderAddr); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn); err != nil {
		return
	}
	store := dp.extentStore
	start := time.Now().Unix()
	for localTinyDeleteFileSize < repairTask.LeaderTinyDeleteFileSize {
		if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteFileSize {
			return
		}
		if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			return
		}
		if p.IsErrPacket() {
			logContent := fmt.Sprintf("action[doStreamFixTinyDeleteRecord] %v.",
				p.LogMessage(p.GetOpMsg(), conn.RemoteAddr().String(), start, fmt.Errorf(string(p.Data[:p.Size]))))
			err = fmt.Errorf(logContent)
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
			store.MarkDelete(extentID, int64(offset), int64(size), localTinyDeleteFileSize)
			if !isFullSync {
				log.LogWarnf(fmt.Sprintf(ActionSyncTinyDeleteRecord+" extentID_(%v)_extentOffset(%v)_size(%v)", extentID, offset, size))
			}

		}
	}
}

// ChangeRaftMember is a wrapper function of changing the raft member.
func (dp *DataPartition) ChangeRaftMember(changeType raftProto.ConfChangeType, peer raftProto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = dp.raftPartition.ChangeMember(changeType, peer, context)
	return
}
