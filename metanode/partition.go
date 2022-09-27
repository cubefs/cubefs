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

package metanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	raftproto "github.com/tiglabs/raft/proto"
)

var (
	ErrIllegalHeartbeatAddress = errors.New("illegal heartbeat address")
	ErrIllegalReplicateAddress = errors.New("illegal replicate address")
)

const (
	MetaPartitionMarshVersion1 = uint32(0)
	MetaPartitionMarshVersion2 = uint32(1)
)

// Errors
var (
	ErrInodeIDOutOfRange = errors.New("inode ID out of range")
)

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

// MetaPartitionConfig is used to create a meta partition.
type MetaPartitionConfig struct {
	// Identity for raftStore group. RaftStore nodes in the same raftStore group must have the same groupID.
	PartitionId        uint64              `json:"partition_id"`
	VolName            string              `json:"vol_name"`
	Start              uint64              `json:"start"`    // Minimal Inode ID of this range. (Required during initialization)
	End                uint64              `json:"end"`      // Maximal Inode ID of this range. (Required during initialization)
	Peers              []proto.Peer        `json:"peers"`    // Peers information of the raftStore
	Learners           []proto.Learner     `json:"learners"` // Learners information of the raftStore
	TrashRemainingDays int32               `json:"-"`
	Cursor             uint64              `json:"-"` // Cursor ID of the inode that have been assigned
	NodeId             uint64              `json:"-"`
	RootDir            string              `json:"-"`
	RocksDBDir         string              `json:"rocksDb_dir"`
	BeforeStart        func()              `json:"-"`
	AfterStart         func()              `json:"-"`
	BeforeStop         func()              `json:"-"`
	AfterStop          func()              `json:"-"`
	RaftStore          raftstore.RaftStore `json:"-"`
	ConnPool           *util.ConnectPool   `json:"-"`
	StoreMode          proto.StoreMode     `json:"store_mode"`
	CreationType       int                 `json:"creation_type"`
	sync.Mutex
}

func (c *MetaPartitionConfig) checkMeta() (err error) {
	if c.PartitionId <= 0 {
		err = errors.NewErrorf("[checkMeta]: partition id at least 1, "+
			"now partition id is: %d", c.PartitionId)
		return
	}
	if c.Start < 0 {
		err = errors.NewErrorf("[checkMeta]: start at least 0")
		return
	}
	if c.End <= c.Start {
		err = errors.NewErrorf("[checkMeta]: end=%v, "+
			"start=%v; end <= start", c.End, c.Start)
		return
	}
	if len(c.Peers) <= 0 {
		err = errors.NewErrorf("[checkMeta]: must have peers, now peers is 0")
		return
	}
	return
}

func (c *MetaPartitionConfig) sortPeers() {
	sp := sortedPeers(c.Peers)
	sort.Sort(sp)
}

// OpInode defines the interface for the inode operations.
type OpInode interface {
	CreateInode(req *CreateInoReq, p *Packet) (err error)
	UnlinkInode(req *UnlinkInoReq, p *Packet) (err error)
	UnlinkInodeBatch(req *BatchUnlinkInoReq, p *Packet) (err error)
	InodeGet(req *InodeGetReq, p *Packet, version uint8) (err error)
	InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error)
	CreateInodeLink(req *LinkInodeReq, p *Packet) (err error)
	EvictInode(req *EvictInodeReq, p *Packet) (err error)
	EvictInodeBatch(req *BatchEvictInodeReq, p *Packet) (err error)
	SetAttr(reqData []byte, p *Packet) (err error)
	GetInodeTree() InodeTree
	DeleteInode(req *proto.DeleteInodeRequest, p *Packet) (err error)
	DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet) (err error)
	GetCompactInodeInfo(req *proto.GetCmpInodesRequest, p *Packet) (err error)
	MergeExtents(req *proto.InodeMergeExtentsRequest, p *Packet) (err error)
}

type OpDeletedInode interface {
	GetDeletedInode(req *GetDeletedInodeReq, p *Packet) (err error)
	BatchGetDeletedInode(req *BatchGetDeletedInodeReq, p *Packet) (err error)
	RecoverDeletedInode(req *proto.RecoverDeletedInodeRequest, p *Packet) (err error)
	BatchRecoverDeletedInode(req *proto.BatchRecoverDeletedInodeRequest, p *Packet) (err error)
	CleanDeletedInode(req *proto.CleanDeletedInodeRequest, p *Packet) (err error)
	BatchCleanDeletedInode(req *proto.BatchCleanDeletedInodeRequest, p *Packet) (err error)
	CleanExpiredDeletedINode() (err error)
	StatDeletedFileInfo(p *Packet) (err error)
}

type OpExtend interface {
	SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error)
	GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error)
	BatchGetXAttr(req *proto.BatchGetXAttrRequest, p *Packet) (err error)
	RemoveXAttr(req *proto.RemoveXAttrRequest, p *Packet) (err error)
	ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error)
}

// OpDentry defines the interface for the dentry operations.
type OpDentry interface {
	CreateDentry(req *CreateDentryReq, p *Packet) (err error)
	DeleteDentry(req *DeleteDentryReq, p *Packet) (err error)
	DeleteDentryBatch(req *BatchDeleteDentryReq, p *Packet) (err error)
	UpdateDentry(req *UpdateDentryReq, p *Packet) (err error)
	ReadDir(req *ReadDirReq, p *Packet) (err error)
	Lookup(req *LookupReq, p *Packet) (err error)
	GetDentryTree() DentryTree
}

type OpDeletedDentry interface {
	RecoverDeletedDentry(req *RecoverDeletedDentryReq, p *Packet) (err error)
	BatchRecoverDeletedDentry(req *BatchRecoverDeletedDentryReq, p *Packet) (err error)
	CleanDeletedDentry(req *CleanDeletedDentryReq, p *Packet) (err error)
	BatchCleanDeletedDentry(req *BatchCleanDeletedDentryReq, p *Packet) (err error)
	CleanExpiredDeletedDentry() (err error)
	LookupDeleted(req *LookupDeletedDentryReq, p *Packet) (err error)
	ReadDeletedDir(req *ReadDeletedDirReq, p *Packet) (err error)
}

// OpExtent defines the interface for the extent operations.
type OpExtent interface {
	ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error)
	ExtentInsert(req *proto.InsertExtentKeyRequest, p *Packet) (err error)
	ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ExtentsTruncate(req *ExtentsTruncateReq, p *Packet) (err error)
	BatchExtentAppend(req *proto.AppendExtentKeysRequest, p *Packet) (err error)
}

type OpMultipart interface {
	GetMultipart(req *proto.GetMultipartRequest, p *Packet) (err error)
	CreateMultipart(req *proto.CreateMultipartRequest, p *Packet) (err error)
	AppendMultipart(req *proto.AddMultipartPartRequest, p *Packet) (err error)
	RemoveMultipart(req *proto.RemoveMultipartRequest, p *Packet) (err error)
	ListMultipart(req *proto.ListMultipartRequest, p *Packet) (err error)
}

// OpMeta defines the interface for the metadata operations.
type OpMeta interface {
	OpInode
	OpDentry
	OpExtent
	OpPartition
	OpExtend
	OpMultipart
	OpDeletedInode
	OpDeletedDentry
}

// OpPartition defines the interface for the partition operations.
type OpPartition interface {
	IsLeader() (leaderAddr string, isLeader bool)
	IsLearner() bool
	HasAlivePeer() (bool, int)
	GetCursor() uint64
	GetAppliedID() uint64
	GetBaseConfig() MetaPartitionConfig
	ResponseLoadMetaPartition(p *Packet) (err error)
	PersistMetadata() (err error)
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
	ResetMember(peers []raftproto.Peer, context []byte) (err error)
	RemoveMemberOnlyRaft(peerID uint64) (err error)
	ResetMemberInter(peers []uint64) (err error)
	ApplyResetMember(req *proto.ResetMetaPartitionRaftMemberRequest) (updated bool, err error)
	Reset() (err error)
	Expired() error
	UpdatePartition(ctx context.Context, req *UpdatePartitionReq, resp *UpdatePartitionResp) (err error)
	DeleteRaft() error
	ExpiredRaft() error
	IsExistPeer(peer proto.Peer) bool
	IsExistLearner(learner proto.Learner) bool
	TryToLeader(groupID uint64) error
	CanRemoveRaftMember(peer proto.Peer) error
	IsEquareCreateMetaPartitionRequst(request *proto.CreateMetaPartitionRequest) (err error)
	GetSnapShot() (snap Snapshot)
	ReleaseSnapShot(snap Snapshot)
	IsRaftHang() bool
	FreeInode(val []byte) error
}

// MetaPartition defines the interface for the meta partition operations.
type MetaPartition interface {
	Start() error
	Stop()
	OpMeta
	LoadSnapshot(path string) error
	SumMonitorData(reportTime int64) []*statistics.MonitorData
}

// metaPartition manages the range of the inode IDs.
// When a new inode is requested, it allocates a new inode id for this inode if possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config                      *MetaPartitionConfig
	size                        uint64 // For partition all file size
	applyID                     uint64 // Inode/Dentry max applyID, this index will be update after restoring from the dumped data.
	dentryTree                  DentryTree
	inodeTree                   InodeTree     // btree for inodes
	extendTree                  ExtendTree    // btree for inode extend (XAttr) management
	multipartTree               MultipartTree // collection for multipart management
	raftPartition               raftstore.Partition
	stopC                       chan bool
	storeChan                   chan *storeMsg
	state                       uint32
	delInodeFp                  *os.File
	freeList                    *freeList // free inode list
	extDelCh                    chan []proto.ExtentKey
	extReset                    chan struct{}
	vol                         *Vol
	manager                     *metadataManager
	monitorData                 []*statistics.MonitorData
	marshalVersion              uint32
	dentryDeletedTree           DeletedDentryTree
	inodeDeletedTree            DeletedInodeTree
	trashExpiresFirstUpdateTime time.Time
	extDelCursor                chan uint64
	db                          *RocksDbInfo
	addBatchKey                 []byte
	delBatchKey                 []byte
	lastSubmit                  int64
	waitPersistCommitCnt        uint64
	deleteEKRecordCount         uint64
	delEKFd                     *os.File
	inodeDelEkRecordCount       uint64
	inodeDelEkFd                *os.File
}

// Start starts a meta partition.
func (mp *metaPartition) Start() (err error) {
	if atomic.CompareAndSwapUint32(&mp.state, common.StateStandby, common.StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = common.StateStandby
			} else {
				newState = common.StateRunning
			}
			atomic.StoreUint32(&mp.state, newState)
		}()
		if mp.config.BeforeStart != nil {
			mp.config.BeforeStart()
		}
		if err = mp.onStart(); err != nil {
			err = errors.NewErrorf("[Start]->%s", err.Error())
			return
		}
		if mp.config.AfterStart != nil {
			mp.config.AfterStart()
		}
	}
	return
}

// Stop stops a meta partition.
func (mp *metaPartition) Stop() {
	if atomic.CompareAndSwapUint32(&mp.state, common.StateRunning, common.StateShutdown) {
		defer atomic.StoreUint32(&mp.state, common.StateStopped)
		if mp.config.BeforeStop != nil {
			mp.config.BeforeStop()
		}
		mp.onStop()
		if mp.config.AfterStop != nil {
			mp.config.AfterStop()
			log.LogDebugf("[AfterStop]: partition id=%d execute ok.",
				mp.config.PartitionId)
		}
	}
}

func (mp *metaPartition) onStart() (err error) {
	defer func() {
		if err == nil {
			return
		}
		mp.onStop()
	}()
	mp.startSchedule(mp.applyID)
	if err = mp.startFreeList(); err != nil {
		err = errors.NewErrorf("[onStart] start free list id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	if err = mp.startRaft(); err != nil {
		err = errors.NewErrorf("[onStart]start raft id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	if mp.config.CreationType == proto.DecommissionedCreateMetaPartition {
		go mp.checkRecoverAfterStart()
	}
	mp.startCleanTrashScheduler()
	mp.startUpdateTrashDaysScheduler()
	return
}

func (mp *metaPartition) onStop() {
	mp.stopRaft()
	mp.stop()
	mp.db.CloseDb()
	mp.inodeTree.Release()
}

func (mp *metaPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
		learners      []raftproto.Learner
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}
	for _, peer := range mp.config.Peers {
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
	log.LogDebugf("start partition id=%d raft peers: %s",
		mp.config.PartitionId, peers)

	for _, learner := range mp.config.Learners {
		rl := raftproto.Learner{ID: learner.ID, PromConfig: &raftproto.PromoteConfig{AutoPromote: learner.PmConfig.AutoProm, PromThreshold: learner.PmConfig.PromThreshold}}
		learners = append(learners, rl)
	}
	pc := &raftstore.PartitionConfig{
		ID:       mp.config.PartitionId,
		Applied:  mp.applyID,
		Peers:    peers,
		Learners: learners,
		SM:       mp,
	}
	mp.raftPartition = mp.config.RaftStore.CreatePartition(pc)
	err = mp.raftPartition.Start()
	return
}

func (mp *metaPartition) stopRaft() {
	if mp.raftPartition != nil {
		// TODO Unhandled errors
		mp.raftPartition.Stop()
	}
	return
}

func (mp *metaPartition) getRaftPort() (heartbeat, replica int, err error) {
	raftConfig := mp.config.RaftStore.RaftConfig()
	heartbeatAddrSplits := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicaAddrSplits := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrSplits) != 2 {
		err = ErrIllegalHeartbeatAddress
		return
	}
	if len(replicaAddrSplits) != 2 {
		err = ErrIllegalReplicateAddress
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

func (mp *metaPartition) selectRocksDBDir() (err error) {
	var (
		dir         string
		partitionId = strconv.FormatUint(mp.config.PartitionId, 10)
	)

	//clean
	for _, dir = range mp.manager.rocksDBDirs {
		rocksdbDir := path.Join(dir, partitionPrefix+partitionId)
		if _, err = os.Stat(rocksdbDir); err != nil {
			if os.IsNotExist(err) {
				continue
			} else {
				return
			}
		} else {
			if err = os.RemoveAll(path.Join(rocksdbDir, "db")); err != nil {
				return
			}
		}
	}

	dir, err = util.SelectDisk(mp.manager.rocksDBDirs)
	if err != nil {
		log.LogErrorf("selectRocksDBDir, mp[%v] select failed(%v), so set root dir(%s) as rocksdb dir",
			mp.config.PartitionId, err, mp.manager.rootDir)
		mp.config.RocksDBDir = mp.manager.rootDir
		err = nil
		return
	}
	mp.config.RocksDBDir = dir
	return
}

func (mp *metaPartition) getRocksDbRootDir() string {
	partitionId := strconv.FormatUint(mp.config.PartitionId, 10)
	return path.Join(mp.config.RocksDBDir, partitionPrefix+partitionId, "db")
}

func NewMetaPartition(conf *MetaPartitionConfig, manager *metadataManager) *metaPartition {
	mp := &metaPartition{
		config:         conf,
		stopC:          make(chan bool),
		storeChan:      make(chan *storeMsg, 100),
		freeList:       newFreeList(),
		extDelCh:       make(chan []proto.ExtentKey, 10000),
		extReset:       make(chan struct{}),
		vol:            NewVol(),
		manager:        manager,
		monitorData:    statistics.InitMonitorData(statistics.ModelMetaNode),
		marshalVersion: MetaPartitionMarshVersion2,
		extDelCursor:   make(chan uint64, 1),
		db:             NewRocksDb(),
	}
	return mp
}

// NewMetaPartition creates a new meta partition with the specified configuration.
func CreateMetaPartition(conf *MetaPartitionConfig, manager *metadataManager) (MetaPartition, error) {
	var (
		mp  *metaPartition
		err error
	)

	mp = NewMetaPartition(conf, manager)
	if err = mp.selectRocksDBDir(); err != nil {
		return nil, err
	}

	if mp.HasMemStore() {
		mp.initMemoryTree()
		mp.cleanRocksDbTreeResource()
	}

	if err = mp.db.OpenDb(mp.getRocksDbRootDir()); err != nil {
		return nil, err
	}

	if mp.HasRocksDBStore() {
		if err = mp.initRocksDBTree(); err != nil {
			err = errors.NewErrorf("[CreateMetaPartition] Init rocksDB tree failed :%v", err.Error())
			return nil, err
		}
		mp.cleanMemoryTreeResource()
	}

	if err = mp.persistMetadata(); err != nil {
		return nil, err
	}
	return mp, nil
}

func LoadMetaPartition(conf *MetaPartitionConfig, manager *metadataManager) (MetaPartition, error) {
	var (
		mp  *metaPartition
		err error
	)

	mp = NewMetaPartition(conf, manager)
	if mp.config.RocksDBDir == "" {
		mp.config.RocksDBDir = mp.config.RootDir
	}

	if err = mp.load(context.Background()); err != nil {
		return nil, err
	}
	return mp, nil
}

func (mp *metaPartition) cleanMemoryTreeResource() {
	if mp.HasRocksDBStore() {
		log.LogWarnf("mp[%v] remove mem dir,but has rocksdb store mode", mp.config.PartitionId)
	}
	os.RemoveAll(mp.config.RootDir + snapshotDir)
	os.RemoveAll(mp.config.RootDir + snapshotDirTmp)
	os.RemoveAll(mp.config.RootDir + snapshotBackup)
}

func (mp *metaPartition) initMemoryTree() {
	mp.dentryTree = &DentryBTree{NewBtree()}
	mp.inodeTree = &InodeBTree{NewBtree()}
	mp.extendTree = &ExtendBTree{NewBtree()}
	mp.multipartTree = &MultipartBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	return
}

func (mp *metaPartition) cleanRocksDbTreeResource() {
	if mp.HasMemStore() {
		log.LogWarnf("mp[%v] remove rocks dir,but has memory store mode", mp.config.PartitionId)
	}
	os.RemoveAll(mp.getRocksDbRootDir())
}

func (mp *metaPartition) initRocksDBTree() (err error) {
	var tree *RocksTree

	if tree, err = DefaultRocksTree(mp.db); err != nil {
		log.LogErrorf("[initRocksDBTree] default rocks tree dir: %v, id: %v error %v ", mp.config.RocksDBDir, mp.config.PartitionId, err)
		return
	}
	if mp.inodeTree, err = NewInodeRocks(tree); err != nil {
		return
	}
	if mp.dentryTree, err = NewDentryRocks(tree); err != nil {
		return
	}
	if mp.extendTree, err = NewExtendRocks(tree); err != nil {
		return
	}
	if mp.multipartTree, err = NewMultipartRocks(tree); err != nil {
		return
	}
	if mp.dentryDeletedTree, err = NewDeletedDentryRocks(tree); err != nil {
		return
	}
	if mp.inodeDeletedTree, err = NewDeletedInodeRocks(tree); err != nil {
		return
	}
	return
}

// IsLeader returns the raft leader address and if the current meta partition is the leader.
func (mp *metaPartition) IsLeader() (leaderAddr string, ok bool) {
	if mp.raftPartition == nil {
		return
	}
	leaderID, _ := mp.raftPartition.LeaderTerm()
	if leaderID == 0 {
		return
	}
	ok = leaderID == mp.config.NodeId
	for _, peer := range mp.config.Peers {
		if leaderID == peer.ID {
			leaderAddr = peer.Addr
			return
		}
	}
	return
}

func (mp *metaPartition) calcMPStatus() (int, error) {
	total := configTotalMem
	used, err := util.GetProcessMemory(os.Getpid())
	if err != nil {
		return proto.Unavailable, err
	}

	addr, _ := mp.IsLeader()
	if addr == "" {
		return proto.Unavailable, nil
	}
	if mp.config.Cursor >= mp.config.End {
		return proto.ReadOnly, nil
	}
	if used > uint64(float64(total)*MaxUsedMemFactor) {
		return proto.ReadOnly, nil
	}

	return proto.ReadWrite, nil
}

func (mp *metaPartition) IsLearner() bool {
	for _, learner := range mp.config.Learners {
		if mp.config.NodeId == learner.ID {
			return true
		}
	}
	return false
}

func (mp *metaPartition) GetPeers() (peers []string) {
	peers = make([]string, 0)
	for _, peer := range mp.config.Peers {
		if mp.config.NodeId == peer.ID {
			continue
		}
		peers = append(peers, peer.Addr)
	}
	return
}

func (mp *metaPartition) HasAlivePeer() (bool, int) {
	//todo can not check isolated island
	aliveNum := 0
	for _, peer := range mp.config.Peers {
		if mp.config.NodeId == peer.ID {
			continue
		}
		connect, err := net.DialTimeout("tcp", peer.Addr, time.Duration(1)*time.Second)
		if err == nil {
			connect.Close()
			aliveNum++
		}
	}
	return aliveNum != 0, aliveNum
}

// GetCursor returns the cursor stored in the config.
func (mp *metaPartition) GetCursor() uint64 {
	return atomic.LoadUint64(&mp.config.Cursor)
}

// GetAppliedID returns applied ID of raft
func (mp *metaPartition) GetAppliedID() uint64 {
	return atomic.LoadUint64(&mp.applyID)
}

// PersistMetadata is the wrapper of persistMetadata.
func (mp *metaPartition) PersistMetadata() (err error) {
	mp.config.sortPeers()
	err = mp.persistMetadata()
	return
}

func (mp *metaPartition) loadMetaDataToMemory(ctx context.Context) (err error) {
	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	if err = mp.loadInode(ctx, snapshotPath); err != nil {
		return
	}
	if err = mp.loadDentry(snapshotPath); err != nil {
		return
	}
	if err = mp.loadExtend(snapshotPath); err != nil {
		return
	}
	if err = mp.loadMultipart(snapshotPath); err != nil {
		return
	}
	if err = mp.loadDeletedDentry(snapshotPath); err != nil {
		return
	}
	if err = mp.loadDeletedInode(ctx, snapshotPath); err != nil {
		return
	}
	return
}

func (mp *metaPartition) loadMetaInRocksDB() (err error) {
	return
}

func (mp *metaPartition) loadApplyIDAndCursorFromSnapshot() (applyID, cursor uint64, err error) {
	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	filename := path.Join(snapshotPath, applyIDFile)
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
		err = errors.NewErrorf("[loadApplyIDAndCursorFromSnapshot] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyIDAndCursorFromSnapshot]: ApplyID is empty")
		return
	}
	if strings.Contains(string(data), "|") {
		_, err = fmt.Sscanf(string(data), "%d|%d", &applyID, &cursor)
	} else {
		_, err = fmt.Sscanf(string(data), "%d", &applyID)
	}
	if err != nil {
		err = errors.NewErrorf("[loadApplyIDAndCursorFromSnapshot] ReadApplyID: %s", err.Error())
		return
	}

	log.LogInfof("loadApplyIDAndCursorFromSnapshot: load complete: partitionID(%v) volume(%v) applyID(%v) cursor(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, applyID, cursor, filename)
	return
}

func (mp *metaPartition) loadApplyIDAndCursor() (err error) {
	var (
		applyIDInSnapshot uint64 = ^uint64(0)
		applyIDInRocksDB  uint64 = ^uint64(0)
		cursorInSnapshot  uint64
		cursorInRocksDB   uint64
		maxInode          uint64
	)
	if mp.HasMemStore() {
		if applyIDInSnapshot, cursorInSnapshot, err = mp.loadApplyIDAndCursorFromSnapshot(); err != nil {
			return
		}

		atomic.StoreUint64(&mp.applyID, applyIDInSnapshot)

		if cursorInSnapshot > atomic.LoadUint64(&mp.config.Cursor) {
			atomic.StoreUint64(&mp.config.Cursor, cursorInSnapshot)
		}
	}

	if mp.HasRocksDBStore() {
		applyIDInRocksDB = mp.inodeTree.GetApplyID()
		atomic.StoreUint64(&mp.applyID, applyIDInRocksDB)

		cursorInRocksDB = mp.inodeTree.GetCursor()
		if maxInode, err = mp.inodeTree.GetMaxInode(); err != nil {
			return
		}

		if maxInode > atomic.LoadUint64(&mp.config.Cursor) {
			atomic.StoreUint64(&mp.config.Cursor, maxInode)
		}
		if cursorInRocksDB > atomic.LoadUint64(&mp.config.Cursor) {
			atomic.StoreUint64(&mp.config.Cursor, cursorInRocksDB)
		}
	}
	log.LogDebugf("mp[%v] applyID:%v, cursor:%v", mp.config.PartitionId, mp.applyID, mp.config.Cursor)

	return
}

func (mp *metaPartition) LoadSnapshot(snapshotPath string) (err error) {
	if err = mp.loadInode(context.Background(), snapshotPath); err != nil {
		return
	}
	if err = mp.loadDentry(snapshotPath); err != nil {
		return
	}
	if err = mp.loadExtend(snapshotPath); err != nil {
		return
	}
	if err = mp.loadMultipart(snapshotPath); err != nil {
		return
	}

	if err = mp.loadDeletedInode(context.Background(), snapshotPath); err != nil {
		return
	}

	if err = mp.loadDeletedDentry(snapshotPath); err != nil {
		return
	}

	err = mp.loadApplyID(snapshotPath)
	return
}

func (mp *metaPartition) load(ctx context.Context) (err error) {
	if err = mp.loadMetadata(); err != nil {
		return
	}

	if err = mp.db.OpenDb(mp.getRocksDbRootDir()); err != nil {
		return
	}

	if mp.HasMemStore() {
		mp.initMemoryTree()
		if err = mp.loadMetaDataToMemory(ctx); err != nil {
			return
		}
		//mp.cleanRocksDbTreeResource()
	}

	if mp.HasRocksDBStore() {
		if err = mp.initRocksDBTree(); err != nil {
			return
		}
		if err = mp.loadMetaInRocksDB(); err != nil {
			return
		}
		mp.cleanMemoryTreeResource()
	}

	err = mp.loadApplyIDAndCursor()
	mp.persistMetadata()
	return
}

func (mp *metaPartition) store(sm *storeMsg) (err error) {
	if mp.HasRocksDBStore() {
		//after reviewed, there no need to execute flush
		return nil
	}
	tmpDir := path.Join(mp.config.RootDir, snapshotDirTmp)
	if _, err = os.Stat(tmpDir); err == nil {
		// TODO Unhandled errors
		os.RemoveAll(tmpDir)
	}
	err = nil
	if err = os.MkdirAll(tmpDir, 0775); err != nil {
		return
	}

	defer func() {
		if err != nil {
			// TODO Unhandled errors
			os.RemoveAll(tmpDir)
		}
	}()
	var crcBuffer = bytes.NewBuffer(make([]byte, 0, 16))
	var storeFuncs = []func(dir string, sm *storeMsg) (uint32, error){
		mp.storeInode,
		mp.storeDentry,
		mp.storeExtend,
		mp.storeMultipart,
		mp.storeDeletedDentry,
		mp.storeDeletedInode,
	}
	for _, storeFunc := range storeFuncs {
		var crc uint32
		if crc, err = storeFunc(tmpDir, sm); err != nil {
			return
		}
		if crcBuffer.Len() != 0 {
			crcBuffer.WriteString(" ")
		}
		crcBuffer.WriteString(fmt.Sprintf("%d", crc))
	}
	if err = mp.storeApplyID(tmpDir, sm); err != nil {
		return
	}
	// write crc to file
	if err = ioutil.WriteFile(path.Join(tmpDir, SnapshotSign), crcBuffer.Bytes(), 0775); err != nil {
		return
	}
	snapshotDir := path.Join(mp.config.RootDir, snapshotDir)
	// check snapshot backup
	backupDir := path.Join(mp.config.RootDir, snapshotBackup)
	if _, err = os.Stat(backupDir); err == nil {
		if err = os.RemoveAll(backupDir); err != nil {
			return
		}
	}
	err = nil

	// rename snapshot
	if _, err = os.Stat(snapshotDir); err == nil {
		if err = os.Rename(snapshotDir, backupDir); err != nil {
			return
		}
	}
	err = nil

	if err = os.Rename(tmpDir, snapshotDir); err != nil {
		_ = os.Rename(backupDir, snapshotDir)
		return
	}
	err = os.RemoveAll(backupDir)
	return
}

// UpdatePeers updates the peers.
func (mp *metaPartition) UpdatePeers(peers []proto.Peer) {
	mp.config.Peers = peers
}

// DeleteRaft deletes the raft partition.
func (mp *metaPartition) DeleteRaft() (err error) {
	err = mp.raftPartition.Delete()
	return
}

// ExpiredRaft deletes the raft partition.
func (mp *metaPartition) ExpiredRaft() (err error) {
	err = mp.raftPartition.Expired()
	return
}

// Return a new inode ID and update the offset.
func (mp *metaPartition) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := atomic.LoadUint64(&mp.config.Cursor)
		end := mp.config.End
		if cur >= end {
			return 0, ErrInodeIDOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mp.config.Cursor, cur, newId) {
			return newId, nil
		}
	}
}

// Return a new inode ID and update the offset.
func (mp *metaPartition) isInoOutOfRange(inodeId uint64) (outOfRange bool, err error) {
	end := atomic.LoadUint64(&mp.config.End)
	if inodeId > end || inodeId < mp.config.Start {
		outOfRange = true
		err = fmt.Errorf("ino[%d] is out of range[%d, %d]", inodeId, mp.config.Start, end)
	}
	return
}

// ChangeMember changes the raft member with the specified one.
func (mp *metaPartition) ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = mp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

// ResetMebmer reset the raft members with new peers, be carefull !
func (mp *metaPartition) ResetMember(peers []raftproto.Peer, context []byte) (err error) {
	err = mp.raftPartition.ResetMember(peers, context)
	return
}

// RemoveMemberOnlyRaft remove the raft member, be carefull ! only execute after reset to add raft log
func (mp *metaPartition) RemoveMemberOnlyRaft(peerID uint64) (err error) {

	var reqData []byte
	req := &proto.RemoveMetaPartitionRaftMemberRequest{}
	req.PartitionId = mp.config.PartitionId
	req.RemovePeer.ID = peerID
	req.RaftOnly = true

	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		return
	}
	if req.RemovePeer.ID == 0 {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali RemovePeerID %v", req.RemovePeer.ID))
		return
	}
	if leaderAddr, ok := mp.IsLeader(); !ok {
		err = fmt.Errorf("replica is not mp[%d] leader[%s]", mp.config.PartitionId, leaderAddr)
	}
	_, err = mp.ChangeMember(raftproto.ConfRemoveNode,
		raftproto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		return err
	}
	return
}

// ResetMebmer reset the raft members with new peers, be carefull ! only execute when no leader, otherwise failed
func (mp *metaPartition) ResetMemberInter(peerIDs []uint64) (err error) {
	var (
		reqData []byte
		updated bool
	)

	leaderAddr, _ := mp.IsLeader()
	if leaderAddr != "" {
		err = fmt.Errorf("mp[%d] is noraml, leader is %s, can not reset member", mp.config.PartitionId, leaderAddr)
		return
	}
	req := &proto.ResetMetaPartitionRaftMemberRequest{NewPeers: make([]proto.Peer, 0)}

	hasMySelf := false
	for _, peerId := range peerIDs {
		if peerId == mp.config.NodeId {
			hasMySelf = true
		}
	}

	if !hasMySelf {
		err = fmt.Errorf("mp[%d] reset peers[%v] that does not have local node[%d]",
			mp.config.PartitionId, peerIDs, mp.config.NodeId)
		return
	}

	for _, peerID := range peerIDs {
		findFLag := false
		for _, peer := range mp.config.Peers {
			if peer.ID == peerID {
				findFLag = true
				req.NewPeers = append(req.NewPeers, peer)
				break
			}
		}
		if findFLag == false {
			//todo need return?
			err = fmt.Errorf("mp[%d] can not find peer[%d]", mp.config.PartitionId, peerID)
			return
		}
	}

	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opResetMetaPartitionMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		return
	}
	var peers []raftproto.Peer
	for _, peer := range peerIDs {
		peers = append(peers, raftproto.Peer{ID: peer})
	}
	err = mp.ResetMember(peers, reqData)
	if err != nil {
		return err
	}
	updated, err = mp.ApplyResetMember(req)
	if err != nil {
		return err
	}
	if updated {
		if err = mp.PersistMetadata(); err != nil {
			log.LogErrorf("action[opResetMetaPartitionMember] err[%v].", err)
		}
	}
	return
}

// GetBaseConfig returns the configuration stored in the meta partition. TODO remove? no usage?
func (mp *metaPartition) GetBaseConfig() MetaPartitionConfig {
	return *mp.config
}

// UpdatePartition updates the meta partition. TODO remove? no usage?
func (mp *metaPartition) UpdatePartition(ctx context.Context, req *UpdatePartitionReq,
	resp *UpdatePartitionResp) (err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	r, err := mp.submit(ctx, opFSMUpdatePartition, "", reqData)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	if status := r.(uint8); status != proto.OpOk {
		resp.Status = proto.TaskFailed
		p := NewPacket(ctx)
		p.ResultCode = status
		err = errors.NewErrorf("[UpdatePartition]: %s", p.GetResultMsg())
		resp.Result = p.GetResultMsg()
	}
	resp.Status = proto.TaskSucceeds
	return
}

func (mp *metaPartition) DecommissionPartition(ctx context.Context, req []byte) (err error) {
	_, err = mp.submit(ctx, opFSMDecommissionPartition, "", req)
	return
}

func (mp *metaPartition) IsExistPeer(peer proto.Peer) bool {
	for _, hasExsitPeer := range mp.config.Peers {
		if hasExsitPeer.Addr == peer.Addr && hasExsitPeer.ID == peer.ID {
			return true
		}
	}
	return false
}

func (mp *metaPartition) IsExistLearner(learner proto.Learner) bool {
	var existPeer bool
	for _, hasExistPeer := range mp.config.Peers {
		if hasExistPeer.Addr == learner.Addr && hasExistPeer.ID == learner.ID {
			existPeer = true
		}
	}
	var existLearner bool
	for _, hasExistLearner := range mp.config.Learners {
		if hasExistLearner.Addr == learner.Addr && hasExistLearner.ID == learner.ID {
			existLearner = true
		}
	}
	return existPeer && existLearner
}

func (mp *metaPartition) TryToLeader(groupID uint64) error {
	return mp.raftPartition.TryToLeader(groupID)
}

// ResponseLoadMetaPartition loads the snapshot signature. TODO remove? no usage?
func (mp *metaPartition) ResponseLoadMetaPartition(p *Packet) (err error) {
	resp := &proto.MetaPartitionLoadResponse{
		PartitionID: mp.config.PartitionId,
		DoCompare:   true,
	}
	resp.MaxInode = mp.GetCursor()
	resp.InodeCount = mp.inodeTree.Count()
	resp.DentryCount = mp.dentryTree.Count()
	resp.ApplyID = mp.applyID
	if err != nil {
		err = errors.Trace(err,
			"[ResponseLoadMetaPartition] check snapshot")
		return
	}

	data, err := json.Marshal(resp)
	if err != nil {
		err = errors.Trace(err, "[ResponseLoadMetaPartition] marshal")
		return
	}
	p.PacketOkWithBody(data)
	return
}

// MarshalJSON is the wrapper of json.Marshal.
func (mp *metaPartition) MarshalJSON() ([]byte, error) {
	return json.Marshal(mp.config)
}

// TODO remove? no usage?
// Reset resets the meta partition.
func (mp *metaPartition) Reset() (err error) {
	mp.inodeTree.Release()
	mp.dentryTree.Release()
	mp.extendTree.Release()
	mp.multipartTree.Release()
	mp.inodeDeletedTree.Release()
	mp.dentryDeletedTree.Release()
	mp.config.Cursor = 0
	mp.applyID = 0
	mp.db.CloseDb()
	mp.db.ReleaseRocksDb()

	// remove files
	filenames := []string{applyIDFile, dentryFile, inodeFile, extendFile, multipartFile}
	for _, filename := range filenames {
		filepath := path.Join(mp.config.RootDir, filename)
		if err = os.Remove(filepath); err != nil {
			return
		}
	}
	mp.cleanRocksDbTreeResource()
	mp.cleanMemoryTreeResource()

	return
}

func (mp *metaPartition) Expired() (err error) {
	mp.stop()

	mp.inodeTree.Release()
	mp.dentryTree.Release()
	mp.extendTree.Release()
	mp.multipartTree.Release()
	mp.dentryDeletedTree.Release()
	mp.inodeDeletedTree.Release()
	mp.config.Cursor = 0
	mp.applyID = 0
	mp.db.CloseDb()
	mp.db.ReleaseRocksDb()

	currentPath := path.Clean(mp.config.RootDir)

	var newPath = path.Join(path.Dir(currentPath),
		ExpiredPartitionPrefix+path.Base(currentPath)+"_"+strconv.FormatInt(time.Now().Unix(), 10))

	if err := os.Rename(currentPath, newPath); err != nil {
		log.LogErrorf("ExpiredPartition: mark expired partition fail: partitionID(%v) path(%v) newPath(%v) err(%v)", mp.config.PartitionId, currentPath, newPath, err)
		return err
	}
	log.LogInfof("ExpiredPartition: mark expired partition: partitionID(%v) path(%v) newPath(%v)",
		mp.config.PartitionId, currentPath, newPath)
	return nil
}

//
func (mp *metaPartition) canRemoveSelf() (canRemove bool, err error) {
	var partition *proto.MetaPartitionInfo
	if partition, err = masterClient.ClientAPI().GetMetaPartition(mp.config.PartitionId); err != nil {
		log.LogErrorf("action[canRemoveSelf] err[%v]", err)
		return
	}
	canRemove = false
	var existInPeers bool
	for _, peer := range partition.Peers {
		if mp.config.NodeId == peer.ID {
			existInPeers = true
		}
	}
	if !existInPeers {
		canRemove = true
		return
	}
	if mp.config.NodeId == partition.OfflinePeerID {
		canRemove = true
		return
	}
	return
}

func (mp *metaPartition) SumMonitorData(reportTime int64) []*statistics.MonitorData {
	dataList := make([]*statistics.MonitorData, 0)
	totalCount := uint64(0)
	for i := 0; i < len(mp.monitorData); i++ {
		if atomic.LoadUint64(&mp.monitorData[i].Count) == 0 {
			continue
		}
		data := &statistics.MonitorData{
			VolName:     mp.config.VolName,
			PartitionID: mp.config.PartitionId,
			Action:      i,
			ActionStr:   statistics.ActionMetaMap[i],
			Size:        atomic.SwapUint64(&mp.monitorData[i].Size, 0),
			Count:       atomic.SwapUint64(&mp.monitorData[i].Count, 0),
			ReportTime:  reportTime,
		}
		dataList = append(dataList, data)
		totalCount += data.Count
	}
	if totalCount > 0 {
		totalData := &statistics.MonitorData{
			VolName:     mp.config.VolName,
			PartitionID: mp.config.PartitionId,
			Count:       totalCount,
			ReportTime:  reportTime,
			IsTotal:     true,
		}
		dataList = append(dataList, totalData)
	}
	return dataList
}

func (mp *metaPartition) isTrashEnable() bool {
	return mp.config.TrashRemainingDays > 0
}

func (mp *metaPartition) isTrashDisable() bool {
	return mp.config.TrashRemainingDays <= 0
}


func (mp *metaPartition) HasMemStore() bool {
	if (mp.config.StoreMode & proto.StoreModeMem) != 0 {
		return true
	}

	return false
}

func (mp *metaPartition) HasRocksDBStore() bool {
	if (mp.config.StoreMode & proto.StoreModeRocksDb) != 0 {
		return true
	}

	return false
}

func (mp *metaPartition) GetSnapShot() Snapshot {
	return NewSnapshot(mp)
}

func (mp *metaPartition) ReleaseSnapShot(snap Snapshot) {
	snap.Close()
}

func (mp *metaPartition) IsRaftHang() bool {
	last := atomic.LoadInt64(&mp.lastSubmit)
	if last == 0 || time.Now().Unix()-last < RaftHangTimeOut {
		return false
	}
	return true
}

func (mp *metaPartition) tryToGiveUpLeader() {
	log.LogInfof("[tryToGiveUpLeader] mp(%v) id(%v)", mp.config.PartitionId, mp.config.NodeId)
	if _, ok := mp.IsLeader(); !ok {
		log.LogInfof("[tryToGiveUpLeader] mp(%v) id(%v) is not leader", mp.config.PartitionId, mp.config.NodeId)
		return
	}

	for _, peer := range mp.config.Peers {
		if peer.ID == mp.config.NodeId {
			continue
		}
		if err := mp.sendTryToLeaderReqToFollower(peer.Addr); err != nil {
			continue
		}
		break
	}
}

func (mp *metaPartition) sendTryToLeaderReqToFollower(addr string) error {
	log.LogInfof("[sendTryToLeaderReqToFollower] mp(%v) id(%v) send tryToLeaderRequest to %s", mp.config.PartitionId, mp.config.NodeId, addr)
	conn, err := mp.config.ConnPool.GetConnect(addr)
	if err != nil {
		log.LogErrorf("[sendTryToLeaderReqToFollower] get conn failed, mpID(%v), follower addr(%s), err(%v)",
			mp.config.PartitionId, addr, err)
		return err
	}
	defer mp.config.ConnPool.PutConnect(conn, true)
	packet := NewPacketToChangeLeader(context.Background(), mp.config.PartitionId)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		log.LogErrorf("[sendTryToLeaderReqToFollower] writeToConn failed, mpID(%v), follower addr(%s), err(%v)",
			mp.config.PartitionId, addr, err)
		return err
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		log.LogErrorf("[sendTryToLeaderReqToFollower] readFromConn failed, mpID(%v), follower addr(%s), err(%v)",
			mp.config.PartitionId, addr, err)
		return err
	}
	if packet.ResultCode != proto.OpOk {
		log.LogErrorf("[sendTryToLeaderReqToFollower] result code mismatch, mpID(%v), follower addr(%s), resultCode(%v)",
			mp.config.PartitionId, addr, packet.ResultCode)
		return fmt.Errorf("error code:0x%X", packet.ResultCode)
	}
	return nil
}

func (mp *metaPartition) FreeInode(val []byte) (err error) {
	var dbWriteHandle interface{}
	dbWriteHandle, err = mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("[FreeInode] create batch write handle failed:%v", err)
		return
	}
	defer func() {
		_ = mp.inodeTree.ReleaseBatchWriteHandle(dbWriteHandle)
	}()

	if err = mp.internalClean(dbWriteHandle, val); err != nil {
		_ = mp.inodeTree.ReleaseBatchWriteHandle(dbWriteHandle)
		log.LogErrorf("[FreeInode] internal clean inode failed:%v", err)
		return
	}

	if err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, false); err != nil {
		log.LogErrorf("[FreeInode] commit batch write handle failed:%v", err)
	}
	return
}

func (mp *metaPartition) checkRecoverAfterStart() {
	timer := time.NewTimer(0)
	for {
		select {
		case <- timer.C:
			leaderAddr, ok := mp.IsLeader()
			if ok {
				log.LogInfof("CheckRecoverAfterStart mp[%v] is leader, skip check recover", mp.config.PartitionId)
				return
			}

			if leaderAddr == "" {
				log.LogErrorf("CheckRecoverAfterStart mp[%v] no leader", mp.config.PartitionId)
				timer.Reset(time.Second * 5)
				continue
			}

			applyID, err := mp.GetLeaderRaftApplyID(leaderAddr)
			if err != nil {
				log.LogErrorf("CheckRecoverAfterStart mp[%v] get leader raft apply id failed:%v",
					mp.config.PartitionId, err)
				timer.Reset(time.Second * 5)
				continue
			}

			if mp.applyID < applyID {
				log.LogErrorf("CheckRecoverAfterStart mp[%v] apply id(leader:%v, current node:%v)",
					mp.config.PartitionId, applyID, mp.applyID)
				timer.Reset(time.Second * 5)
				continue
			}

			mp.config.CreationType = proto.NormalCreateMetaPartition
			if err = mp.persistMetadata(); err != nil {
				log.LogErrorf("CheckRecoverAfterStart mp[%v] persist meta data failed:%v", mp.config.PartitionId, err)
				timer.Reset(time.Second * 5)
				continue
			}
			log.LogInfof("CheckRecoverAfterStart mp[%v] recover finish, applyID(leader:%v, current node:%v)",
				mp.config.PartitionId, applyID, mp.applyID)
			return
		case <- mp.stopC:
			timer.Stop()
			return

		}
	}
}

func (mp *metaPartition) GetLeaderRaftApplyID(target string) (applyID uint64, err error) {
	var conn *net.TCPConn
	defer func() {
		if err != nil {
			mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		} else {
			mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
		}
	}()
	conn, err = mp.config.ConnPool.GetConnect(target)
	if err != nil {
		log.LogErrorf("GetLeaderRaftApplyID mp[%v] get connect failed:%v", mp.config.PartitionId, err)
		return
	}
	packet := NewPacketToGetApplyID(context.Background(), mp.config.PartitionId)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		log.LogErrorf("GetLeaderRaftApplyID mp[%v] write to connection failed:%v", mp.config.PartitionId, err)
		return
	}

	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		log.LogErrorf("GetLeaderRaftApplyID mp[%v] read from connection failed:%v", mp.config.PartitionId, err)
		return
	}

	if packet.ResultCode != proto.OpOk {
		log.LogErrorf("GetLeaderRaftApplyID mp[%v] resultCode:0x%x", mp.config.PartitionId, packet.ResultCode)
		err = fmt.Errorf("get raft apply id failed with code 0x%x", packet.ResultCode)
		return
	}

	applyID = binary.BigEndian.Uint64(packet.Data[:8])
	return
}
