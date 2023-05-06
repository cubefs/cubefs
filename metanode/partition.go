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

package metanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"

	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/hashicorp/consul/api"
)

var (
	ErrIllegalHeartbeatAddress = errors.New("illegal heartbeat address")
	ErrIllegalReplicateAddress = errors.New("illegal replicate address")
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
	PartitionId   uint64              `json:"partition_id"`
	VolName       string              `json:"vol_name"`
	Start         uint64              `json:"start"` // Minimal Inode ID of this range. (Required during initialization)
	End           uint64              `json:"end"`   // Maximal Inode ID of this range. (Required during initialization)
	PartitionType int                 `json:"partition_type"`
	Peers         []proto.Peer        `json:"peers"` // Peers information of the raftStore
	Cursor        uint64              `json:"-"`     // Cursor ID of the inode that have been assigned
	NodeId        uint64              `json:"-"`
	RootDir       string              `json:"-"`
	BeforeStart   func()              `json:"-"`
	AfterStart    func()              `json:"-"`
	BeforeStop    func()              `json:"-"`
	AfterStop     func()              `json:"-"`
	RaftStore     raftstore.RaftStore `json:"-"`
	ConnPool      *util.ConnectPool   `json:"-"`
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
	InodeGet(req *InodeGetReq, p *Packet) (err error)
	InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error)
	CreateInodeLink(req *LinkInodeReq, p *Packet) (err error)
	EvictInode(req *EvictInodeReq, p *Packet) (err error)
	EvictInodeBatch(req *BatchEvictInodeReq, p *Packet) (err error)
	SetAttr(reqData []byte, p *Packet) (err error)
	GetInodeTree() *BTree
	GetInodeTreeLen() int
	DeleteInode(req *proto.DeleteInodeRequest, p *Packet) (err error)
	DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet) (err error)
	ClearInodeCache(req *proto.ClearInodeCacheRequest, p *Packet) (err error)
}

type OpExtend interface {
	SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error)
	BatchSetXAttr(req *proto.BatchSetXAttrRequest, p *Packet) (err error)
	GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error)
	GetAllXAttr(req *proto.GetAllXAttrRequest, p *Packet) (err error)
	BatchGetXAttr(req *proto.BatchGetXAttrRequest, p *Packet) (err error)
	RemoveXAttr(req *proto.RemoveXAttrRequest, p *Packet) (err error)
	ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error)
	UpdateXAttr(req *proto.UpdateXAttrRequest, p *Packet) (err error)
}

// OpDentry defines the interface for the dentry operations.
type OpDentry interface {
	CreateDentry(req *CreateDentryReq, p *Packet) (err error)
	DeleteDentry(req *DeleteDentryReq, p *Packet) (err error)
	DeleteDentryBatch(req *BatchDeleteDentryReq, p *Packet) (err error)
	UpdateDentry(req *UpdateDentryReq, p *Packet) (err error)
	ReadDir(req *ReadDirReq, p *Packet) (err error)
	ReadDirLimit(req *ReadDirLimitReq, p *Packet) (err error)
	ReadDirOnly(req *ReadDirOnlyReq, p *Packet) (err error)
	Lookup(req *LookupReq, p *Packet) (err error)
	GetDentryTree() *BTree
	GetDentryTreeLen() int
}

// OpExtent defines the interface for the extent operations.
type OpExtent interface {
	ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error)
	ExtentAppendWithCheck(req *proto.AppendExtentKeyWithCheckRequest, p *Packet) (err error)
	BatchObjExtentAppend(req *proto.AppendObjExtentKeysRequest, p *Packet) (err error)
	ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ObjExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ExtentsTruncate(req *ExtentsTruncateReq, p *Packet) (err error)
	BatchExtentAppend(req *proto.AppendExtentKeysRequest, p *Packet) (err error)
	// ExtentsDelete(req *proto.DelExtentKeyRequest, p *Packet) (err error)
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
}

// OpPartition defines the interface for the partition operations.
type OpPartition interface {
	IsLeader() (leaderAddr string, isLeader bool)
	IsFollowerRead() bool
	SetFollowerRead(bool)
	GetCursor() uint64
	GetBaseConfig() MetaPartitionConfig
	ResponseLoadMetaPartition(p *Packet) (err error)
	PersistMetadata() (err error)
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
	Reset() (err error)
	UpdatePartition(req *UpdatePartitionReq, resp *UpdatePartitionResp) (err error)
	DeleteRaft() error
	IsExsitPeer(peer proto.Peer) bool
	TryToLeader(groupID uint64) error
	CanRemoveRaftMember(peer proto.Peer) error
	IsEquareCreateMetaPartitionRequst(request *proto.CreateMetaPartitionRequest) (err error)
}

// MetaPartition defines the interface for the meta partition operations.
type MetaPartition interface {
	Start(isCreate bool) error
	Stop()
	DataSize() uint64
	GetFreeListLen() int
	OpMeta
	LoadSnapshot(path string) error
	ForceSetMetaPartitionToLoadding()
	ForceSetMetaPartitionToFininshLoad()
}

// metaPartition manages the range of the inode IDs.
// When a new inode is requested, it allocates a new inode id for this inode if possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config                 *MetaPartitionConfig
	size                   uint64 // For partition all file size
	applyID                uint64 // Inode/Dentry max applyID, this index will be update after restoring from the dumped data.
	dentryTree             *BTree
	inodeTree              *BTree // btree for inodes
	extendTree             *BTree // btree for inode extend (XAttr) management
	multipartTree          *BTree // collection for multipart management
	raftPartition          raftstore.Partition
	stopC                  chan bool
	storeChan              chan *storeMsg
	state                  uint32
	delInodeFp             *os.File
	freeList               *freeList // free inode list
	extDelCh               chan []proto.ExtentKey
	extReset               chan struct{}
	vol                    *Vol
	manager                *metadataManager
	isLoadingMetaPartition bool
	summaryLock            sync.Mutex
	ebsClient              *blobstore.BlobStoreClient
	volType                int
	isFollowerRead         bool
	xattrLock              sync.Mutex
	fileRange              []int64
}

func (mp *metaPartition) updateSize() {
	timer := time.NewTicker(time.Minute * 2)
	go func() {
		for {
			select {
			case <-timer.C:
				size := uint64(0)

				mp.inodeTree.GetTree().Ascend(func(item BtreeItem) bool {
					inode := item.(*Inode)
					size += inode.Size
					return true
				})

				mp.size = size
				log.LogDebugf("[updateSize] update mp(%d) size(%d) success", mp.config.PartitionId, size)
			case <-mp.stopC:
				log.LogDebugf("[updateSize] stop update mp(%d) size", mp.config.PartitionId)
				return
			}
		}
	}()
}

func (mp *metaPartition) ForceSetMetaPartitionToLoadding() {
	mp.isLoadingMetaPartition = true
}

func (mp *metaPartition) ForceSetMetaPartitionToFininshLoad() {
	mp.isLoadingMetaPartition = false
}

func (mp *metaPartition) DataSize() uint64 {
	return mp.size
}

func (mp *metaPartition) GetFreeListLen() int {
	return mp.freeList.Len()
}

// Start starts a meta partition.
func (mp *metaPartition) Start(isCreate bool) (err error) {
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
		if err = mp.onStart(isCreate); err != nil {
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

func (mp *metaPartition) onStart(isCreate bool) (err error) {
	defer func() {
		if err == nil {
			return
		}
		mp.onStop()
	}()
	if err = mp.load(isCreate); err != nil {
		err = errors.NewErrorf("[onStart] load partition id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	mp.startScheduleTask()
	if err = mp.startFreeList(); err != nil {
		err = errors.NewErrorf("[onStart] start free list id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	if err = mp.startRaft(); err != nil {
		err = errors.NewErrorf("[onStart] start raft id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}

	// set EBS Client
	clusterInfo, cerr := masterClient.AdminAPI().GetClusterInfo()
	if cerr != nil {
		err = cerr
		log.LogErrorf("action[onStart] GetClusterInfo err[%v]", err)
		return
	}
	volumeInfo, verr := masterClient.AdminAPI().GetVolumeSimpleInfo(mp.config.VolName)
	if verr != nil {
		err = verr
		log.LogErrorf("action[onStart] GetVolumeSimpleInfo err[%v]", err)
		return
	}
	mp.volType = volumeInfo.VolType
	var ebsClient *blobstore.BlobStoreClient
	if clusterInfo.EbsAddr != "" && proto.IsCold(mp.volType) {
		ebsClient, err = blobstore.NewEbsClient(
			access.Config{
				ConnMode: access.NoLimitConnMode,
				Consul: api.Config{
					Address: clusterInfo.EbsAddr,
				},
				MaxSizePutOnce: int64(volumeInfo.ObjBlockSize),
				Logger:         &access.Logger{Filename: path.Join(log.LogDir, "ebs.log")},
			},
		)

		if err != nil {
			log.LogErrorf("action[onStart] err[%v]", err)
			return
		}
		if ebsClient == nil {
			err = errors.NewErrorf("[onStart] ebsClient is nil")
			return
		}
		mp.ebsClient = ebsClient
	}
	mp.updateSize()

	// do cache TTL die out process
	if err = mp.cacheTTLWork(); err != nil {
		err = errors.NewErrorf("[onStart] start CacheTTLWork id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	return
}

func (mp *metaPartition) startScheduleTask() {
	mp.startSchedule(mp.applyID)
	mp.startFileStats()
}

func (mp *metaPartition) onStop() {
	mp.stopRaft()
	mp.stop()
	if mp.delInodeFp != nil {
		mp.delInodeFp.Sync()
		mp.delInodeFp.Close()
	}
}

func (mp *metaPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
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
	pc := &raftstore.PartitionConfig{
		ID:      mp.config.PartitionId,
		Applied: mp.applyID,
		Peers:   peers,
		SM:      mp,
	}
	mp.raftPartition, err = mp.config.RaftStore.CreatePartition(pc)
	if err == nil {
		mp.ForceSetMetaPartitionToFininshLoad()
	}
	return
}

func (mp *metaPartition) stopRaft() {
	if mp.raftPartition != nil {
		// TODO Unhandled errors
		//mp.raftPartition.Stop()
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

// NewMetaPartition creates a new meta partition with the specified configuration.
func NewMetaPartition(conf *MetaPartitionConfig, manager *metadataManager) MetaPartition {
	mp := &metaPartition{
		config:        conf,
		dentryTree:    NewBtree(),
		inodeTree:     NewBtree(),
		extendTree:    NewBtree(),
		multipartTree: NewBtree(),
		stopC:         make(chan bool),
		storeChan:     make(chan *storeMsg, 100),
		freeList:      newFreeList(),
		extDelCh:      make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:      make(chan struct{}),
		vol:           NewVol(),
		manager:       manager,
	}
	return mp
}

// IsLeader returns the raft leader address and if the current meta partition is the leader.
func (mp *metaPartition) SetFollowerRead(fRead bool) {
	if mp.raftPartition == nil {
		return
	}
	mp.isFollowerRead = fRead
	return
}

// IsLeader returns the raft leader address and if the current meta partition is the leader.
func (mp *metaPartition) IsFollowerRead() (ok bool) {
	if mp.raftPartition == nil {
		return
	}
	return mp.isFollowerRead
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

// GetCursor returns the cursor stored in the config.
func (mp *metaPartition) GetCursor() uint64 {
	return atomic.LoadUint64(&mp.config.Cursor)
}

// PersistMetadata is the wrapper of persistMetadata.
func (mp *metaPartition) PersistMetadata() (err error) {
	mp.config.sortPeers()
	err = mp.persistMetadata()
	return
}

func (mp *metaPartition) LoadSnapshot(snapshotPath string) (err error) {
	if err = mp.loadInode(snapshotPath); err != nil {
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
	err = mp.loadApplyID(snapshotPath)
	return
}

func (mp *metaPartition) load(isCreate bool) (err error) {
	if err = mp.loadMetadata(); err != nil {
		return
	}

	// create new metaPartition, no need to load snapshot
	if isCreate {
		return
	}

	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	var loadFuncs = []func(rootDir string) error{
		mp.loadInode,
		mp.loadDentry,
		mp.loadExtend,
		mp.loadMultipart,
		mp.loadApplyID,
	}

	errs := make([]error, 0)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(loadFuncs))
	for _, f := range loadFuncs {
		loadFunc := f
		go func() {
			defer wg.Done()
			if e := loadFunc(snapshotPath); e != nil {
				mutex.Lock()
				errs = append(errs, e)
				mutex.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		err = errs[0]
	}
	return
}

func (mp *metaPartition) store(sm *storeMsg) (err error) {
	log.LogWarnf("metaPartition store apply %v", sm.applyIndex)
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
	log.LogWarnf("metaPartition store apply %v", sm.applyIndex)
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

// ChangeMember changes the raft member with the specified one.
func (mp *metaPartition) ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = mp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

// GetBaseConfig returns the configuration stored in the meta partition. TODO remove? no usage?
func (mp *metaPartition) GetBaseConfig() MetaPartitionConfig {
	return *mp.config
}

// UpdatePartition updates the meta partition. TODO remove? no usage?
func (mp *metaPartition) UpdatePartition(req *UpdatePartitionReq,
	resp *UpdatePartitionResp) (err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	r, err := mp.submit(opFSMUpdatePartition, reqData)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	if status := r.(uint8); status != proto.OpOk {
		resp.Status = proto.TaskFailed
		p := &Packet{}
		p.ResultCode = status
		err = errors.NewErrorf("[UpdatePartition]: %s", p.GetResultMsg())
		resp.Result = p.GetResultMsg()
	}
	resp.Status = proto.TaskSucceeds
	return
}

func (mp *metaPartition) DecommissionPartition(req []byte) (err error) {
	_, err = mp.submit(opFSMDecommissionPartition, req)
	return
}

func (mp *metaPartition) IsExsitPeer(peer proto.Peer) bool {
	for _, hasExsitPeer := range mp.config.Peers {
		if hasExsitPeer.Addr == peer.Addr && hasExsitPeer.ID == peer.ID {
			return true
		}
	}
	return false
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
	resp.InodeCount = uint64(mp.GetInodeTreeLen())
	resp.DentryCount = uint64(mp.GetDentryTreeLen())
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
	mp.inodeTree.Reset()
	mp.dentryTree.Reset()
	mp.config.Cursor = 0
	mp.applyID = 0

	// remove files
	filenames := []string{applyIDFile, dentryFile, inodeFile, extendFile, multipartFile}
	for _, filename := range filenames {
		filepath := path.Join(mp.config.RootDir, filename)
		if err = os.Remove(filepath); err != nil {
			return
		}
	}

	return
}

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

// cacheTTLWork only happen in datalake situation
func (mp *metaPartition) cacheTTLWork() (err error) {
	// check volume type, only Cold volume will do the cache ttl.
	volView, mcErr := masterClient.ClientAPI().GetVolumeWithoutAuthKey(mp.config.VolName)
	if mcErr != nil {
		err = fmt.Errorf("cacheTTLWork: can't get volume info: partitoinID(%v) volume(%v)",
			mp.config.PartitionId, mp.config.VolName)
		return
	}
	if volView.VolType != proto.VolumeTypeCold {
		return
	}
	// do cache ttl work
	go mp.doCacheTTL(volView.CacheTTL)
	return
}

func (mp *metaPartition) doCacheTTL(cacheTTL int) (err error) {
	// first sleep a rand time, range [0, 1200s(20m)],
	// make sure all mps is not doing scan work at the same time.
	rand.Seed(time.Now().Unix())
	time.Sleep(time.Duration(rand.Intn(1200)))

	ttl := time.NewTicker(time.Duration(util.OneDaySec()) * time.Second)
	for {
		select {
		case <-ttl.C:
			log.LogDebugf("[doCacheTTL] begin cache ttl, mp[%v] cacheTTL[%v]", mp.config.PartitionId, cacheTTL)
			// only leader can do TTL work
			if _, ok := mp.IsLeader(); !ok {
				log.LogDebugf("[doCacheTTL] partitionId=%d is not leader, skip", mp.config.PartitionId)
				continue
			}

			// get the last cacheTTL
			volView, mcErr := masterClient.ClientAPI().GetVolumeWithoutAuthKey(mp.config.VolName)
			if mcErr != nil {
				err = fmt.Errorf("[doCacheTTL]: can't get volume info: partitoinID(%v) volume(%v)",
					mp.config.PartitionId, mp.config.VolName)
				return
			}
			cacheTTL = volView.CacheTTL

			mp.InodeTTLScan(cacheTTL)

		case <-mp.stopC:
			log.LogWarnf("[doCacheTTL] stoped, mp(%d)", mp.config.PartitionId)
			return
		}
	}
}

func (mp *metaPartition) InodeTTLScan(cacheTTL int) {
	curTime := Now.GetCurrentTimeUnix()
	// begin
	count := 0
	needSleep := false
	mp.inodeTree.GetTree().Ascend(func(i BtreeItem) bool {
		inode := i.(*Inode)
		// dir type just skip
		if proto.IsDir(inode.Type) {
			return true
		}
		inode.RLock()
		// eks is empty just skip
		if len(inode.Extents.eks) == 0 || inode.ShouldDelete() {
			inode.RUnlock()
			return true
		}

		if (curTime - inode.AccessTime) > int64(cacheTTL)*util.OneDaySec() {
			log.LogDebugf("[InodeTTLScan] mp[%v] do inode ttl delete[%v]", mp.config.PartitionId, inode.Inode)
			count++
			// make request
			p := &Packet{}
			req := &proto.EmptyExtentKeyRequest{
				Inode: inode.Inode,
			}
			ino := NewInode(req.Inode, 0)
			curTime = Now.GetCurrentTimeUnix()
			if inode.ModifyTime < curTime {
				ino.ModifyTime = curTime
			}

			mp.ExtentsEmpty(req, p, ino)
			// check empty result.
			// if result is OpAgain, means the extDelCh maybe full,
			// so let it sleep 1s.
			if p.ResultCode == proto.OpAgain {
				needSleep = true
			}
		}
		inode.RUnlock()
		// every 1000 inode sleep 1s
		if count > 1000 || needSleep {
			count %= 1000
			needSleep = false
			time.Sleep(time.Second)
		}
		return true
	})
}
