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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"hash/crc32"
	"net"
	"syscall"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	raftProto "github.com/tiglabs/raft/proto"
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
	VolumeID                string
	PartitionID             uint64
	PartitionSize           int
	CreateTime              string
	Peers                   []proto.Peer
	Hosts                   []string
	Learners                []proto.Learner
	DataPartitionCreateType int
	LastTruncateID          uint64
	LastUpdateTime          int64
	VolumeHAType            proto.CrossRegionHAType

	// 该BOOL值表示Partition是否已经就绪，该值默认值为false，
	// 新创建的DP成员为默认值，表示未完成第一次Raft恢复，Raft未就绪。
	// 当第一次快照或者有应用日志行为时，该值被置为true并需要持久化该信息。
	// 当发生快照应用(Apply Snapshot)行为时，该值为true。该DP需要关闭并进行报警。
	IsCatchUp bool
}

func (md *DataPartitionMetadata) Equals(other *DataPartitionMetadata) bool {
	return (md == nil && other == nil) ||
		(md != nil && other != nil && md.VolumeID == other.VolumeID &&
			md.PartitionID == other.PartitionID &&
			md.PartitionSize == other.PartitionSize &&
			md.CreateTime == other.CreateTime &&
			reflect.DeepEqual(md.Peers, other.Peers) &&
			reflect.DeepEqual(md.Hosts, other.Hosts) &&
			reflect.DeepEqual(md.Learners, other.Learners) &&
			md.DataPartitionCreateType == other.DataPartitionCreateType &&
			md.LastTruncateID == other.LastTruncateID &&
			md.LastUpdateTime == other.LastUpdateTime &&
			md.VolumeHAType == other.VolumeHAType) &&
			md.IsCatchUp == other.IsCatchUp
}

func (md *DataPartitionMetadata) Validate() (err error) {
	md.VolumeID = strings.TrimSpace(md.VolumeID)
	if len(md.VolumeID) == 0 || md.PartitionID == 0 || md.PartitionSize == 0 {
		err = errors.New("illegal data partition metadata")
		return
	}
	return
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

type WALApplyStatus struct {
	applied      uint64
	lastTruncate uint64
	nextTruncate uint64

	mu sync.RWMutex
}

func (s *WALApplyStatus) Init(applied, lastTruncate uint64) (success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if applied == 0 || (applied != 0 && applied >= lastTruncate) {
		s.applied, s.lastTruncate = applied, lastTruncate
		success = true
	}
	return
}

func (s *WALApplyStatus) AdvanceApplied(id uint64) (snap WALApplyStatus, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applied < id && s.lastTruncate <= id {
		s.applied = id
		success = true
	}
	snap = WALApplyStatus{
		applied:      s.applied,
		lastTruncate: s.lastTruncate,
		nextTruncate: s.nextTruncate,
	}
	return
}

func (s *WALApplyStatus) Applied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

func (s *WALApplyStatus) AdvanceNextTruncate(id uint64) (snap WALApplyStatus, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastTruncate < id && s.nextTruncate < id && id <= s.applied {
		if s.nextTruncate != 0 && s.nextTruncate > s.lastTruncate {
			s.lastTruncate = s.nextTruncate
		}
		s.nextTruncate = id
		success = true
	}
	snap = WALApplyStatus{
		applied:      s.applied,
		lastTruncate: s.lastTruncate,
		nextTruncate: s.nextTruncate,
	}
	return
}

func (s *WALApplyStatus) NextTruncate() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextTruncate
}

func (s *WALApplyStatus) LastTruncate() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastTruncate
}

func (s *WALApplyStatus) Snap() *WALApplyStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &WALApplyStatus{
		applied:      s.applied,
		lastTruncate: s.lastTruncate,
		nextTruncate: s.nextTruncate,
	}
}

func NewWALApplyStatus() *WALApplyStatus {
	return &WALApplyStatus{}
}

type DataPartition struct {
	clusterID       string
	volumeID        string
	partitionID     uint64
	partitionStatus int
	partitionSize   int
	replicas        []string // addresses of the replicas
	replicasLock    sync.RWMutex
	disk            *Disk
	isLeader        bool
	isRaftLeader    bool
	path            string
	used            int
	extentStore     *storage.ExtentStore
	raftPartition   raftstore.Partition
	config          *dataPartitionCfg

	isCatchUp    bool
	applyStatus  *WALApplyStatus
	minAppliedID uint64
	maxAppliedID uint64

	repairC         chan struct{}
	fetchVolHATypeC chan struct{}

	stopOnce  sync.Once
	stopRaftC chan uint64
	storeC    chan uint64
	stopC     chan bool

	intervalToUpdateReplicas      int64 // interval to ask the master for updating the replica information
	snapshot                      []*proto.File
	snapshotMutex                 sync.RWMutex
	intervalToUpdatePartitionSize int64
	loadExtentHeaderStatus        int
	FullSyncTinyDeleteTime        int64
	lastSyncTinyDeleteTime        int64
	lastUpdateTime                int64
	DataPartitionCreateType       int

	monitorData []*statistics.MonitorData

	persistSync chan struct{}

	inRepairExtents  map[uint64]struct{}
	inRepairExtentMu sync.Mutex

	persistedApplied  uint64
	persistedMetadata *DataPartitionMetadata
}

func CreateDataPartition(dpCfg *dataPartitionCfg, disk *Disk, request *proto.CreateDataPartitionRequest) (dp *DataPartition, err error) {

	if dp, err = newDataPartition(dpCfg, disk, true); err != nil {
		return
	}
	dp.ForceLoadHeader()

	// persist file metadata
	dp.DataPartitionCreateType = request.CreateType
	dp.lastUpdateTime = time.Now().Unix()
	err = dp.PersistMetaDataOnly()
	disk.AddSize(uint64(dp.Size()))
	return
}

func (dp *DataPartition) IsEquareCreateDataPartitionRequst(request *proto.CreateDataPartitionRequest) (err error) {
	if len(dp.config.Peers) != len(request.Members) {
		return fmt.Errorf("Exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", dp.partitionID, dp.config.Peers, request.Members)
	}
	for index, host := range dp.config.Hosts {
		requestHost := request.Hosts[index]
		if host != requestHost {
			return fmt.Errorf("Exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", dp.partitionID, dp.config.Hosts, request.Hosts)
		}
	}
	for index, peer := range dp.config.Peers {
		requestPeer := request.Members[index]
		if requestPeer.ID != peer.ID || requestPeer.Addr != peer.Addr {
			return fmt.Errorf("Exist unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", dp.partitionID, dp.config.Peers, request.Members)
		}
	}
	for index, learner := range dp.config.Learners {
		requestLearner := request.Learners[index]
		if requestLearner.ID != learner.ID || requestLearner.Addr != learner.Addr {
			return fmt.Errorf("Exist unavali Partition(%v) partitionLearners(%v) requestLearners(%v)", dp.partitionID, dp.config.Learners, request.Learners)
		}
	}
	if dp.config.VolName != request.VolumeId {
		return fmt.Errorf("Exist unavali Partition(%v) VolName(%v) requestVolName(%v)", dp.partitionID, dp.config.VolName, request.VolumeId)
	}
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
		Hosts:         meta.Hosts,
		Learners:      meta.Learners,
		RaftStore:     disk.space.GetRaftStore(),
		NodeID:        disk.space.GetNodeID(),
		ClusterID:     disk.space.GetClusterID(),
		CreationType:  meta.DataPartitionCreateType,

		VolHAType: meta.VolumeHAType,
	}
	if dp, err = newDataPartition(dpCfg, disk, false); err != nil {
		return
	}
	dp.lastUpdateTime = meta.LastUpdateTime
	// dp.PersistMetadata()
	disk.space.AttachPartition(dp)

	var appliedID uint64
	if appliedID, err = dp.LoadAppliedID(); err != nil {
		log.LogErrorf("action[loadApplyIndex] %v", err)
	}
	log.LogInfof("Action(LoadDataPartition) PartitionID(%v) meta(%v)", dp.partitionID, meta)
	dp.DataPartitionCreateType = meta.DataPartitionCreateType
	dp.isCatchUp = meta.IsCatchUp

	if !dp.applyStatus.Init(appliedID, meta.LastTruncateID) {
		err = fmt.Errorf("action[loadApplyIndex] illegal metadata, appliedID %v, lastTruncateID %v", appliedID, meta.LastTruncateID)
		return
	}

	disk.AddSize(uint64(dp.Size()))
	dp.ForceLoadHeader()
	if (len(dp.config.Hosts) > 3 && dp.config.VolHAType == proto.DefaultCrossRegionHAType) ||
		(len(dp.config.Hosts) <= 3 && dp.config.VolHAType == proto.CrossRegionHATypeQuorum) {
		dp.ProposeFetchVolHAType()
	}

	dp.persistedApplied = appliedID
	dp.persistedMetadata = meta
	return
}

const (
	DelayFullSyncTinyDeleteTimeRandom = 6 * 60 * 60
)

func newDataPartition(dpCfg *dataPartitionCfg, disk *Disk, isCreatePartition bool) (dp *DataPartition, err error) {
	partitionID := dpCfg.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionID, dpCfg.PartitionSize))
	partition := &DataPartition{
		volumeID:                dpCfg.VolName,
		clusterID:               dpCfg.ClusterID,
		partitionID:             partitionID,
		disk:                    disk,
		path:                    dataPath,
		partitionSize:           dpCfg.PartitionSize,
		replicas:                make([]string, 0),
		repairC:                 make(chan struct{}, 1),
		fetchVolHATypeC:         make(chan struct{}, 1),
		stopC:                   make(chan bool, 0),
		stopRaftC:               make(chan uint64, 0),
		storeC:                  make(chan uint64, 128),
		snapshot:                make([]*proto.File, 0),
		partitionStatus:         proto.ReadWrite,
		config:                  dpCfg,
		DataPartitionCreateType: dpCfg.CreationType,
		monitorData:             statistics.InitMonitorData(statistics.ModelDataNode),
		persistSync:             make(chan struct{}, 1),
		inRepairExtents:         make(map[uint64]struct{}),
		applyStatus:             NewWALApplyStatus(),
	}
	partition.replicasInit()

	var cacheListener storage.CacheListener = func(event storage.CacheEvent, e *storage.Extent) {
		switch event {
		case storage.CacheEvent_Add:
			disk.IncreaseFDCount()
		case storage.CacheEvent_Evict:
			disk.DecreaseFDCount()
		}
	}

	partition.extentStore, err = storage.NewExtentStore(partition.path, dpCfg.PartitionID, dpCfg.PartitionSize, CacheCapacityPerPartition, cacheListener, isCreatePartition)
	if err != nil {
		return
	}
	rand.Seed(time.Now().UnixNano())
	partition.FullSyncTinyDeleteTime = time.Now().Unix() + rand.Int63n(3600*24)
	partition.lastSyncTinyDeleteTime = partition.FullSyncTinyDeleteTime
	// Attach data partition to disk mapping
	disk.AttachDataPartition(partition)
	dp = partition
	return
}

func (dp *DataPartition) Start() (err error) {
	go dp.statusUpdateScheduler(context.Background())
	if dp.DataPartitionCreateType == proto.DecommissionedCreateDataPartition {
		go dp.startRaftAfterRepair()
		return
	}
	if err = dp.startRaft(); err != nil {
		log.LogErrorf("partition(%v) start raft failed: %v", dp.partitionID, err)
	}
	return
}

func (dp *DataPartition) replicasInit() {
	replicas := make([]string, 0)
	if dp.config.Hosts == nil {
		return
	}
	for _, host := range dp.config.Hosts {
		replicas = append(replicas, host)
	}
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

func (dp *DataPartition) IsRandomWriteDisabled() (disabled bool) {
	disabled = dp.config.VolHAType == proto.CrossRegionHATypeQuorum
	return
}

func (dp *DataPartition) IsRaftLearner() bool {
	for _, learner := range dp.config.Learners {
		if learner.ID == dp.config.NodeID {
			return true
		}
	}
	return false
}

func (dp *DataPartition) getReplicaClone() (newReplicas []string) {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	newReplicas = make([]string, len(dp.replicas))
	copy(newReplicas, dp.replicas)
	return
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

func (dp *DataPartition) IsExistLearner(tarLearner proto.Learner) bool {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	for _, learner := range dp.config.Learners {
		if learner.Addr == tarLearner.Addr && learner.ID == tarLearner.ID {
			return true
		}
	}
	return false
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
	dp.stopOnce.Do(func() {
		if dp.stopC != nil {
			close(dp.stopC)
		}
		// Close the store and raftstore.
		dp.extentStore.Close()
		dp.stopRaft()
		if err := dp.Persist(nil); err != nil {
			log.LogErrorf("persist partition [%v] failed when stop: %v", dp.partitionID, err)
		}
	})
	return
}

func (dp *DataPartition) Delete() {
	if dp == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("DataPartition(%v) Delete panic(%v)", dp.partitionID, r)
			log.LogWarnf(mesg)
		}
	}()
	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
	if dp.raftPartition != nil {
		_ = dp.raftPartition.Delete()
	} else {
		log.LogWarnf("action[Delete] raft instance not ready! dp:%v", dp.config.PartitionID)
	}
	_ = os.RemoveAll(dp.Path())
}

func (dp *DataPartition) Expired() {
	if dp == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("DataPartition(%v) Expired panic(%v)", dp.partitionID, r)
			log.LogWarnf(mesg)
		}
	}()

	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
	if dp.raftPartition != nil {
		_ = dp.raftPartition.Expired()
	} else {
		log.LogWarnf("action[ExpiredPartition] raft instance not ready! dp:%v", dp.config.PartitionID)
	}
	var currentPath = path.Clean(dp.path)
	var newPath = path.Join(path.Dir(currentPath),
		ExpiredPartitionPrefix+path.Base(currentPath)+"_"+strconv.FormatInt(time.Now().Unix(), 10))
	if err := os.Rename(currentPath, newPath); err != nil {
		log.LogErrorf("ExpiredPartition: mark expired partition fail: volume(%v) partitionID(%v) path(%v) newPath(%v) err(%v)",
			dp.volumeID,
			dp.partitionID,
			dp.path,
			newPath,
			err)
		return
	}
	log.LogInfof("ExpiredPartition: mark expired partition: volume(%v) partitionID(%v) path(%v) newPath(%v)",
		dp.volumeID,
		dp.partitionID,
		dp.path,
		newPath)
}

// Disk returns the disk instance.
func (dp *DataPartition) Disk() *Disk {
	return dp.disk
}

func (dp *DataPartition) IsRejectWrite() bool {
	return dp.Disk().RejectWrite
}

const (
	MinDiskSpace = 10 * 1024 * 1024 * 1024
)

func (dp *DataPartition) IsRejectRandomWrite() bool {
	return dp.Disk().Available < MinDiskSpace
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

func (dp *DataPartition) Repair() {
	select {
	case dp.repairC <- struct{}{}:
	default:
	}
}

func (dp *DataPartition) ProposeFetchVolHAType() {
	select {
	case dp.fetchVolHATypeC <- struct{}{}:
	default:
	}
}

func (dp *DataPartition) statusUpdateScheduler(ctx context.Context) {
	repairTimer := time.NewTimer(time.Minute)
	validateCRCTimer := time.NewTimer(DefaultIntervalDataPartitionValidateCRC)
	retryFetchVolHATypeTimer := time.NewTimer(0)
	retryFetchVolHATypeTimer.Stop()
	persistDpLastUpdateTimer := time.NewTimer(time.Hour) //for persist dp lastUpdateTime
	var index int
	for {

		select {
		case <-dp.stopC:
			repairTimer.Stop()
			validateCRCTimer.Stop()
			return

		case <-dp.repairC:
			repairTimer.Stop()
			log.LogDebugf("partition(%v) execute manual data repair for all extent", dp.partitionID)
			dp.ExtentStore().MoveAllToBrokenTinyExtentC(storage.TinyExtentCount)
			dp.runRepair(ctx, proto.TinyExtentType, false)
			dp.runRepair(ctx, proto.NormalExtentType, false)
			repairTimer.Reset(time.Minute)
		case <-repairTimer.C:
			index++
			dp.statusUpdate()
			if index >= math.MaxUint32 {
				index = 0
			}
			if index%2 == 0 {
				dp.runRepair(ctx, proto.TinyExtentType, true)
			} else {
				dp.runRepair(ctx, proto.NormalExtentType, true)
			}
			repairTimer.Reset(time.Minute)
		case <-validateCRCTimer.C:
			dp.runValidateCRC(ctx)
			validateCRCTimer.Reset(DefaultIntervalDataPartitionValidateCRC)
		case <-dp.fetchVolHATypeC:
			if err := dp.fetchVolHATypeFromMaster(); err != nil {
				retryFetchVolHATypeTimer.Reset(time.Minute)
			}
		case <-retryFetchVolHATypeTimer.C:
			if err := dp.fetchVolHATypeFromMaster(); err != nil {
				retryFetchVolHATypeTimer.Reset(time.Minute)
			}
		case <-persistDpLastUpdateTimer.C:
			_ = dp.PersistMetaDataOnly()
			persistDpLastUpdateTimer.Reset(time.Hour)
		}
	}
}

func (dp *DataPartition) fetchVolHATypeFromMaster() (err error) {
	var simpleVolView *proto.SimpleVolView
	if simpleVolView, err = MasterClient.AdminAPI().GetVolumeSimpleInfo(dp.volumeID); err != nil {
		return
	}
	if dp.config.VolHAType != simpleVolView.CrossRegionHAType {
		dp.config.VolHAType = simpleVolView.CrossRegionHAType
		if err = dp.PersistMetaDataOnly(); err != nil {
			return
		}
	}
	return
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
	if dp.Status() == proto.Unavailable {
		status = proto.Unavailable
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
	if time.Now().Unix()-dp.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize {
		return
	}
	dp.used = int(dp.ExtentStore().GetStoreUsedSize())
	dp.intervalToUpdatePartitionSize = time.Now().Unix()
}

func (dp *DataPartition) ExtentStore() *storage.ExtentStore {
	return dp.extentStore
}

func (dp *DataPartition) checkIsDiskError(err error) (diskError bool) {
	if err == nil {
		return
	}
	if IsDiskErr(err.Error()) {
		mesg := fmt.Sprintf("disk path %v error on %v", dp.Path(), LocalIP)
		exporter.Warning(mesg)
		log.LogErrorf(mesg)
		dp.stopRaft()
		dp.disk.incReadErrCnt()
		dp.disk.incWriteErrCnt()
		dp.disk.Status = proto.Unavailable
		dp.statusUpdate()
		dp.disk.ForceExitRaftStore()
		diskError = true
	}
	return
}

// String returns the string format of the data partition information.
func (dp *DataPartition) String() (m string) {
	return fmt.Sprintf(DataPartitionPrefix+"_%v_%v", dp.partitionID, dp.partitionSize)
}

// runRepair launches the repair of extents.
func (dp *DataPartition) runRepair(ctx context.Context, extentType uint8, fetchReplicas bool) {

	if dp.partitionStatus == proto.Unavailable {
		return
	}
	if fetchReplicas {
		if err := dp.updateReplicas(false); err != nil {
			log.LogErrorf("action[runRepair] partition(%v) err(%v).", dp.partitionID, err)
			return
		}
	}

	if !dp.isLeader {
		return
	}
	if dp.extentStore.BrokenTinyExtentCnt() == 0 {
		dp.extentStore.MoveAllToBrokenTinyExtentC(MinTinyExtentsToRepair)
	}
	dp.repair(ctx, extentType)
}

func (dp *DataPartition) updateReplicas(isForce bool) (err error) {
	if !isForce && time.Now().Unix()-dp.intervalToUpdateReplicas <= IntervalToUpdateReplica {
		return
	}
	dp.isLeader = false
	isLeader, replicas, err := dp.fetchReplicasFromMaster()
	if err != nil {
		return
	}
	dp.replicasLock.Lock()
	defer dp.replicasLock.Unlock()
	if !dp.compareReplicas(dp.replicas, replicas) {
		log.LogInfof("action[updateReplicas] partition(%v) replicas changed from(%v) to(%v).",
			dp.partitionID, dp.replicas, replicas)
	}
	dp.isLeader = isLeader
	dp.replicas = replicas
	dp.intervalToUpdateReplicas = time.Now().Unix()
	log.LogInfof(fmt.Sprintf("ActionUpdateReplicationHosts partiton(%v)", dp.partitionID))

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

	var partition *proto.DataPartitionInfo
	if partition, err = MasterClient.AdminAPI().GetDataPartition(dp.volumeID, dp.partitionID); err != nil {
		isLeader = false
		return
	}
	for _, host := range partition.Hosts {
		replicas = append(replicas, host)
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

// DoExtentStoreRepairOnFollowerDisk performs the repairs of the extent store.
// 1. when the extent size is smaller than the max size on the record, start to repair the missing part.
// 2. if the extent does not even exist, create the extent first, and then repair.
func (dp *DataPartition) DoExtentStoreRepairOnFollowerDisk(repairTask *DataPartitionRepairTask) {
	store := dp.extentStore
	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		if storage.IsTinyExtent(extentInfo[storage.FileID]) {
			continue
		}

		if !storage.IsTinyExtent(extentInfo[storage.FileID]) && !dp.ExtentStore().IsFinishLoad() {
			continue
		}
		if store.HasExtent(uint64(extentInfo[storage.FileID])) {
			//info := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentInfo.FileID, Size: extentInfo.Size} todo
			info := storage.ExtentInfoBlock{storage.FileID: extentInfo[storage.FileID], storage.Size: extentInfo[storage.Size]}
			repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
			continue
		}
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v)", extentInfo.String())
			continue
		}
		err := store.Create(uint64(extentInfo[storage.FileID]), true)
		if err != nil {
			continue
		}
		dp.lastUpdateTime = time.Now().Unix()
		//info := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentInfo.FileID, Size: extentInfo.Size}
		info := storage.ExtentInfoBlock{storage.FileID: extentInfo[storage.FileID], storage.Size: extentInfo[storage.Size]}
		repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
	}
	var (
		wg           *sync.WaitGroup
		recoverIndex int
	)
	wg = new(sync.WaitGroup)
	for _, extentInfo := range repairTask.ExtentsToBeRepaired {

		if !store.HasExtent(extentInfo[storage.FileID]) {
			continue
		}
		wg.Add(1)
		source := repairTask.ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
		// repair the extents
		go dp.doStreamExtentFixRepairOnFollowerDisk(context.Background(), wg, extentInfo, source)
		recoverIndex++

		if recoverIndex%NumOfFilesToRecoverInParallel == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
	dp.doStreamFixTinyDeleteRecord(context.Background(), repairTask, time.Now().Unix()-dp.FullSyncTinyDeleteTime > MaxFullSyncTinyDeleteTime)
}

type TinyDeleteRecord struct {
	extentID uint64
	offset   uint64
	size     uint64
}

type TinyDeleteRecordArr []TinyDeleteRecord

func (dp *DataPartition) doStreamFixTinyDeleteRecord(ctx context.Context, repairTask *DataPartitionRepairTask, isFullSync bool) {
	var (
		localTinyDeleteFileSize int64
		err                     error
		conn                    *net.TCPConn
		isRealSync              bool
	)

	if !dp.Disk().canFinTinyDeleteRecord() {
		return
	}
	defer func() {
		dp.Disk().finishFixTinyDeleteRecord()
	}()
	log.LogInfof(ActionSyncTinyDeleteRecord+" start PartitionID(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) "+
		"leaderAddr(%v) ,lastSyncTinyDeleteTime(%v) currentTime(%v) fullSyncTinyDeleteTime(%v) isFullSync(%v)",
		dp.partitionID, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteRecordFileSize, repairTask.LeaderAddr,
		dp.lastSyncTinyDeleteTime, time.Now().Unix(), dp.FullSyncTinyDeleteTime, isFullSync)

	defer func() {
		log.LogInfof(ActionSyncTinyDeleteRecord+" end PartitionID(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) leaderAddr(%v) "+
			"err(%v), lastSyncTinyDeleteTime(%v) currentTime(%v) fullSyncTinyDeleteTime(%v) isFullSync(%v) isRealSync(%v)\",",
			dp.partitionID, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteRecordFileSize, repairTask.LeaderAddr, err,
			dp.lastSyncTinyDeleteTime, time.Now().Unix(), dp.FullSyncTinyDeleteTime, isFullSync, isRealSync)
	}()
	if !isFullSync {
		if time.Now().Unix()-dp.lastSyncTinyDeleteTime < MinSyncTinyDeleteTime {
			return
		}
		if localTinyDeleteFileSize, err = dp.extentStore.LoadTinyDeleteFileOffset(); err != nil {
			return
		}

	} else {
		dp.FullSyncTinyDeleteTime = time.Now().Unix()
	}

	if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteRecordFileSize {
		return
	}

	if !isFullSync && repairTask.LeaderTinyDeleteRecordFileSize-localTinyDeleteFileSize < MinTinyExtentDeleteRecordSyncSize {
		return
	}
	isRealSync = true
	dp.lastSyncTinyDeleteTime = time.Now().Unix()
	p := repl.NewPacketToReadTinyDeleteRecord(ctx, dp.partitionID, localTinyDeleteFileSize)
	if conn, err = gConnPool.GetConnect(repairTask.LeaderAddr); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}
	store := dp.extentStore
	start := time.Now().Unix()
	for localTinyDeleteFileSize < repairTask.LeaderTinyDeleteRecordFileSize {
		if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteRecordFileSize {
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
		var allTinyDeleteRecordsArr [storage.TinyExtentCount + 1]TinyDeleteRecordArr
		for currTinyExtentID := storage.TinyExtentStartID; currTinyExtentID < storage.TinyExtentStartID+storage.TinyExtentCount; currTinyExtentID++ {
			allTinyDeleteRecordsArr[currTinyExtentID] = make([]TinyDeleteRecord, 0)
		}

		for (index+1)*storage.DeleteTinyRecordSize <= int(p.Size) {
			record := p.Data[index*storage.DeleteTinyRecordSize : (index+1)*storage.DeleteTinyRecordSize]
			extentID, offset, size := storage.UnMarshalTinyExtent(record)
			localTinyDeleteFileSize += storage.DeleteTinyRecordSize
			index++
			if !storage.IsTinyExtent(extentID) {
				continue
			}
			DeleteLimiterWait()
			dr := TinyDeleteRecord{
				extentID: extentID,
				offset:   offset,
				size:     size,
			}
			allTinyDeleteRecordsArr[extentID] = append(allTinyDeleteRecordsArr[extentID], dr)
		}
		for currTinyExtentID := storage.TinyExtentStartID; currTinyExtentID < storage.TinyExtentStartID+storage.TinyExtentCount; currTinyExtentID++ {
			currentDeleteRecords := allTinyDeleteRecordsArr[currTinyExtentID]
			for _, dr := range currentDeleteRecords {
				if dr.extentID != uint64(currTinyExtentID) {
					continue
				}
				if !storage.IsTinyExtent(dr.extentID) {
					continue
				}
				log.LogInfof("doStreamFixTinyDeleteRecord Delete PartitionID(%v)_Extent(%v)_Offset(%v)_Size(%v)", dp.partitionID, dr.extentID, dr.offset, dr.size)
				store.MarkDelete(dr.extentID, int64(dr.offset), int64(dr.size))
			}
		}
	}
}

// ChangeRaftMember is a wrapper function of changing the raft member.
func (dp *DataPartition) ChangeRaftMember(changeType raftProto.ConfChangeType, peer raftProto.Peer, context []byte) (resp interface{}, err error) {
	log.LogErrorf("[DataPartition->ChangeRaftMember] [partitionID: %v] start [changeType: %v, peer: %v]", dp.partitionID, changeType, peer)
	defer func() {
		log.LogErrorf("[DataPartition->ChangeRaftMember] [partitionID: %v] finish [changeType: %v, peer: %v]", dp.partitionID, changeType, peer)
	}()
	resp, err = dp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

//
func (dp *DataPartition) canRemoveSelf() (canRemove bool, err error) {
	var partition *proto.DataPartitionInfo
	if partition, err = MasterClient.AdminAPI().GetDataPartition(dp.volumeID, dp.partitionID); err != nil {
		log.LogErrorf("action[canRemoveSelf] err(%v)", err)
		return
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

func (dp *DataPartition) SyncReplicaHosts(replicas []string) {
	if len(replicas) == 0 {
		return
	}
	dp.isLeader = false
	var leader bool // Whether current instance is the leader member.
	if len(replicas) >= 1 {
		leaderAddr := replicas[0]
		leaderAddrParts := strings.Split(leaderAddr, ":")
		if len(leaderAddrParts) == 2 && strings.TrimSpace(leaderAddrParts[0]) == LocalIP {
			leader = true
		}
	}
	dp.replicasLock.Lock()
	dp.isLeader = leader
	dp.replicas = replicas
	dp.intervalToUpdateReplicas = time.Now().Unix()
	dp.replicasLock.Unlock()
	log.LogInfof("partition(%v) synchronized replica hosts from master [replicas:(%v), leader: %v]",
		dp.partitionID, strings.Join(replicas, ","), leader)
	if leader {
		dp.Repair()
	}
}

// ResetRaftMember is a wrapper function of changing the raft member.
func (dp *DataPartition) ResetRaftMember(peers []raftProto.Peer, context []byte) (err error) {
	if dp.raftPartition == nil {
		return fmt.Errorf("raft instance not ready")
	}
	err = dp.raftPartition.ResetMember(peers, context)
	return
}

func (dp *DataPartition) EvictExpiredFileDescriptor() {
	dp.extentStore.EvictExpiredCache()
}

func (dp *DataPartition) ForceEvictFileDescriptor(ratio storage.Ratio) {
	dp.extentStore.ForceEvictCache(ratio)
}
