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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/cmd/common"
	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

var (
	ErrIllegalHeartbeatAddress = errors.New("illegal heartbeat address")
	ErrIllegalReplicateAddress = errors.New("illegal replicate address")
	ErrSnapshotCrcMismatch     = errors.New("snapshot crc not match")
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
	TxCreateInode(req *proto.TxCreateInodeRequest, p *Packet) (err error)
	TxUnlinkInode(req *proto.TxUnlinkInodeRequest, p *Packet) (err error)
	TxCreateInodeLink(req *proto.TxLinkInodeRequest, p *Packet) (err error)
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
	TxCreateDentry(req *proto.TxCreateDentryRequest, p *Packet) (err error)
	TxDeleteDentry(req *proto.TxDeleteDentryRequest, p *Packet) (err error)
	TxUpdateDentry(req *proto.TxUpdateDentryRequest, p *Packet) (err error)
}

type OpTransaction interface {
	TxCommit(req *proto.TxApplyRequest, p *Packet) (err error)
	TxInodeCommit(req *proto.TxInodeApplyRequest, p *Packet) (err error)
	TxDentryCommit(req *proto.TxDentryApplyRequest, p *Packet) (err error)
	TxRollback(req *proto.TxApplyRequest, p *Packet) (err error)
	TxInodeRollback(req *proto.TxInodeApplyRequest, p *Packet) (err error)
	TxDentryRollback(req *proto.TxDentryApplyRequest, p *Packet) (err error)
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
	GetUidInfo() (info []*proto.UidReportSpaceInfo)
	SetUidLimit(info []*proto.UidSpaceInfo)
}

// OpMeta defines the interface for the metadata operations.
type OpMeta interface {
	OpInode
	OpDentry
	OpExtent
	OpPartition
	OpExtend
	OpMultipart
	OpTransaction
	OpQuota
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

type UidManager struct {
	accumDelta        *sync.Map
	accumBase         *sync.Map
	accumRebuildDelta *sync.Map // snapshot redoLog
	accumRebuildBase  *sync.Map // snapshot mirror
	uidAcl            *sync.Map
	lastUpdateTime    time.Time
	enable            bool
	rbuilding         bool
	volName           string
	acLock            sync.RWMutex
	mpID              uint64
}

func NewUidMgr(volName string, mpID uint64) (mgr *UidManager) {
	mgr = &UidManager{
		volName:           volName,
		mpID:              mpID,
		accumDelta:        new(sync.Map),
		accumBase:         new(sync.Map),
		accumRebuildDelta: new(sync.Map),
		accumRebuildBase:  new(sync.Map),
		uidAcl:            new(sync.Map),
	}
	var uid uint32
	mgr.uidAcl.Store(uid, false)
	log.LogDebugf("NewUidMgr init")
	return
}

func (uMgr *UidManager) addUidSpace(uid uint32, inode uint64, eks []proto.ExtentKey) (status uint8) {
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()

	status = proto.OpOk
	if uMgr.getUidAcl(uid) {
		log.LogWarnf("addUidSpace.vol %v mp[%v] uid %v be set full", uMgr.mpID, uMgr.volName, uid)
		return proto.OpNoSpaceErr
	}
	if eks == nil {
		return
	}
	var size int64
	for _, ek := range eks {
		size += int64(ek.Size)
	}
	log.LogDebugf("addUidSpace. mp[%v] uid %v accumDelta size %v", uMgr.mpID, uid, size)
	if val, ok := uMgr.accumDelta.Load(uid); ok {
		size += val.(int64)
	}
	uMgr.accumDelta.Store(uid, size)
	log.LogDebugf("addUidSpace. mp[%v] uid %v accumDelta Store size %v", uMgr.mpID, uid, size)

	if uMgr.rbuilding {
		if val, ok := uMgr.accumRebuildDelta.Load(uid); ok {
			size += val.(int64)
		}
		log.LogDebugf("addUidSpace. mp[%v] rbuilding uid %v accumDelta Store size %v", uMgr.mpID, uid, size)
		uMgr.accumRebuildDelta.Store(uid, size)
	}
	return
}

func (uMgr *UidManager) doMinusUidSpace(uid uint32, inode uint64, size uint64) {
	log.LogDebugf("doMinusUidSpace. mp[%v] inode %v uid %v size %v", uMgr.mpID, inode, uid, size)
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()

	doWork := func(delta *sync.Map) {
		var rsvSize int64
		if val, ok := delta.Load(uid); ok {
			delta.Store(uid, val.(int64)-int64(size))
			log.LogDebugf("doMinusUidSpace. mp[%v] uid %v accumDelta now size %v", uMgr.mpID, uid, rsvSize)
		} else {
			rsvSize -= int64(size)
			log.LogDebugf("doMinusUidSpace. mp[%v] uid %v accumDelta now size %v", uMgr.mpID, uid, rsvSize)
			delta.Store(uid, rsvSize)
		}
	}
	doWork(uMgr.accumDelta)
	if uMgr.rbuilding {
		log.LogDebugf("doMinusUidSpace.mp[%v] rbuilding inode %v uid %v size %v", uMgr.mpID, inode, uid, size)
		doWork(uMgr.accumRebuildDelta)
	}
}

func (uMgr *UidManager) minusUidSpace(uid uint32, inode uint64, eks []proto.ExtentKey) {
	var size uint64
	for _, ek := range eks {
		size += uint64(ek.Size)
	}
	uMgr.doMinusUidSpace(uid, inode, size)
}

func (uMgr *UidManager) getUidAcl(uid uint32) (enable bool) {
	if val, ok := uMgr.uidAcl.Load(uid); ok {
		enable = val.(bool)
	}
	return
}

func (uMgr *UidManager) setUidAcl(info []*proto.UidSpaceInfo) {
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()

	uMgr.uidAcl = new(sync.Map)
	for _, uidInfo := range info {
		if uidInfo.VolName != uMgr.volName {
			continue
		}
		// log.LogDebugf("setUidAcl.vol %v uid %v be set enable %v", uMgr.volName, uidInfo.Uid, uidInfo.Limited)
		uMgr.uidAcl.Store(uidInfo.Uid, uidInfo.Limited)
	}
}

func (uMgr *UidManager) getAllUidSpace() (rsp []*proto.UidReportSpaceInfo) {
	uMgr.acLock.RLock()
	defer uMgr.acLock.RUnlock()

	var ok bool

	uMgr.accumDelta.Range(func(key, value interface{}) bool {
		var size int64
		size += value.(int64)
		if baseInfo, ok := uMgr.accumBase.Load(key.(uint32)); ok {
			size += baseInfo.(int64)
			if size < 0 {
				log.LogErrorf("getAllUidSpace. mp[%v] uid %v size small than 0 %v, old %v, new %v", uMgr.mpID, key.(uint32), size, value.(int64), baseInfo.(int64))
				return false
			}
		}
		uMgr.accumBase.Store(key.(uint32), size)
		return true
	})

	uMgr.accumDelta = new(sync.Map)

	uMgr.accumBase.Range(func(key, value interface{}) bool {
		var size int64
		if size, ok = value.(int64); !ok {
			log.LogErrorf("getAllUidSpace. mp[%v] accumBase key %v size type %v", uMgr.mpID, reflect.TypeOf(key), reflect.TypeOf(value))
			return false
		}
		rsp = append(rsp, &proto.UidReportSpaceInfo{
			Uid:  key.(uint32),
			Size: uint64(size),
		})
		// log.LogDebugf("getAllUidSpace. mp[%v] accumBase uid %v size %v", uMgr.mpID, key.(uint32), size)
		return true
	})

	return
}

func (uMgr *UidManager) accumRebuildStart() {
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()
	log.LogDebugf("accumRebuildStart vol [%v] mp[%v]", uMgr.volName, uMgr.mpID)
	uMgr.rbuilding = true
}

func (uMgr *UidManager) accumRebuildFin() {
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()
	log.LogDebugf("accumRebuildFin rebuild vol %v, mp:[%v],%v:%v", uMgr.volName, uMgr.mpID, uMgr.accumRebuildBase, uMgr.accumRebuildDelta)
	uMgr.accumBase = uMgr.accumRebuildBase
	uMgr.accumDelta = uMgr.accumRebuildDelta
	uMgr.accumRebuildBase = new(sync.Map)
	uMgr.accumRebuildDelta = new(sync.Map)
	uMgr.rbuilding = false
}

func (uMgr *UidManager) accumInoUidSize(ino *Inode, accum *sync.Map) {
	size := ino.GetSpaceSize()
	if val, ok := accum.Load(ino.Uid); ok {
		size += uint64(val.(int64))
	}
	accum.Store(ino.Uid, int64(size))
}

type OpQuota interface {
	setQuotaHbInfo(infos []*proto.QuotaHeartBeatInfo)
	getQuotaReportInfos() (infos []*proto.QuotaReportInfo)
	batchSetInodeQuota(req *proto.BatchSetMetaserverQuotaReuqest,
		resp *proto.BatchSetMetaserverQuotaResponse) (err error)
	batchDeleteInodeQuota(req *proto.BatchDeleteMetaserverQuotaReuqest,
		resp *proto.BatchDeleteMetaserverQuotaResponse) (err error)
	getInodeQuota(inode uint64, p *Packet) (err error)
}

// metaPartition manages the range of the inode IDs.
// When a new inode is requested, it allocates a new inode id for this inode if possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config                 *MetaPartitionConfig
	size                   uint64                // For partition all file size
	applyID                uint64                // Inode/Dentry max applyID, this index will be update after restoring from the dumped data.
	dentryTree             *BTree                // btree for dentries
	inodeTree              *BTree                // btree for inodes
	extendTree             *BTree                // btree for inode extend (XAttr) management
	multipartTree          *BTree                // collection for multipart management
	txProcessor            *TransactionProcessor // transction processor
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
	uidManager             *UidManager
	xattrLock              sync.Mutex
	fileRange              []int64
	mqMgr                  *MetaQuotaManager
}

func (mp *metaPartition) acucumRebuildStart() {
	mp.uidManager.accumRebuildStart()
}
func (mp *metaPartition) acucumRebuildFin() {
	mp.uidManager.accumRebuildFin()
}
func (mp *metaPartition) acucumUidSizeByStore(ino *Inode) {
	mp.uidManager.accumInoUidSize(ino, mp.uidManager.accumRebuildBase)
}

func (mp *metaPartition) acucumUidSizeByLoad(ino *Inode) {
	mp.uidManager.accumInoUidSize(ino, mp.uidManager.accumBase)
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

	// set EBS Client
	if clusterInfo, err = masterClient.AdminAPI().GetClusterInfo(); err != nil {
		log.LogErrorf("action[onStart] GetClusterInfo err[%v]", err)
		return
	}

	var volumeInfo *proto.SimpleVolView
	if volumeInfo, err = masterClient.AdminAPI().GetVolumeSimpleInfo(mp.config.VolName); err != nil {
		log.LogErrorf("action[onStart] GetVolumeSimpleInfo err[%v]", err)
		return
	}

	mp.volType = volumeInfo.VolType
	var ebsClient *blobstore.BlobStoreClient
	if clusterInfo.EbsAddr != "" && proto.IsCold(mp.volType) {
		ebsClient, err = blobstore.NewEbsClient(
			access.Config{
				ConnMode: access.NoLimitConnMode,
				Consul: access.ConsulConfig{
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

	if err = mp.startRaft(); err != nil {
		err = errors.NewErrorf("[onStart] start raft id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
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
	mp.txProcessor = NewTransactionProcessor(mp)
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

func (mp *metaPartition) parseCrcFromFile() ([]uint32, error) {
	data, err := ioutil.ReadFile(path.Join(path.Join(mp.config.RootDir, snapshotDir), SnapshotSign))
	if err != nil {
		return nil, err
	}
	raw := string(data)
	crcStrs := strings.Split(raw, " ")

	crcs := make([]uint32, 0, len(crcStrs))
	for _, crcStr := range crcStrs {
		crc, err := strconv.ParseUint(crcStr, 10, 32)
		if err != nil {
			return nil, err
		}
		crcs = append(crcs, uint32(crc))
	}

	return crcs, nil
}

const CRC_NUM_BEFORE_CUBEFS_V3_2_2 int = 4
const CRC_NUM_SINCE_CUBEFS_V3_2_2 int = 7

func (mp *metaPartition) LoadSnapshot(snapshotPath string) (err error) {
	crcs, err := mp.parseCrcFromFile()
	if err != nil {
		return err
	}

	var loadFuncs = []func(rootDir string, crc uint32) error{
		mp.loadInode,
		mp.loadDentry,
		mp.loadExtend,
		mp.loadMultipart,
	}

	var needLoadTxStuff bool
	//handle compatibility in upgrade scenarios
	if len(crcs) == CRC_NUM_BEFORE_CUBEFS_V3_2_2 {
		needLoadTxStuff = false
	} else if len(crcs) == CRC_NUM_SINCE_CUBEFS_V3_2_2 {
		needLoadTxStuff = true
		loadFuncs = append(loadFuncs, mp.loadTxInfo)
		loadFuncs = append(loadFuncs, mp.loadTxRbInode)
		loadFuncs = append(loadFuncs, mp.loadTxRbDentry)
	} else {
		log.LogErrorf("action[LoadSnapshot] crc array length %d not match", len(crcs))
		return ErrSnapshotCrcMismatch
	}

	errs := make([]error, len(loadFuncs))
	var wg sync.WaitGroup
	wg.Add(len(loadFuncs))
	for idx, f := range loadFuncs {
		loadFunc := f
		i := idx
		go func() {
			defer wg.Done()
			if i == 2 { //loadExtend must be executed after loadInode
				return
			}
			errs[i] = loadFunc(snapshotPath, crcs[i])
		}()
	}

	wg.Wait()

	for _, err = range errs {
		if err != nil {
			return
		}
	}

	if err = mp.loadExtend(snapshotPath, crcs[2]); err != nil {
		return
	}

	if needLoadTxStuff {
		if err = mp.loadTxID(snapshotPath); err != nil {
			return
		}
	}

	return mp.loadApplyID(snapshotPath)
}

func (mp *metaPartition) load(isCreate bool) (err error) {
	if err = mp.loadMetadata(); err != nil {
		return
	}

	// 1. create new metaPartition, no need to load snapshot
	// 2. store the snapshot files for new mp, because
	// mp.load() will check all the snapshot files when mn startup
	if isCreate {
		if err = mp.storeSnapshotFiles(); err != nil {
			err = errors.NewErrorf("[onStart] storeSnapshotFiles for partition id=%d: %s",
				mp.config.PartitionId, err.Error())
		}
		return
	}

	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	if _, err = os.Stat(snapshotPath); err != nil {
		log.LogErrorf("load snapshot failed, err: %s", err.Error())
		return nil

	}
	return mp.LoadSnapshot(snapshotPath)
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
		mp.storeTxInfo,
		mp.storeTxRbInode,
		mp.storeTxRbDentry,
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
	if err = mp.storeTxID(tmpDir, sm); err != nil {
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
	mp.txProcessor.Reset()

	// remove files
	filenames := []string{applyIDFile, dentryFile, inodeFile, extendFile, multipartFile, txInfoFile, txRbInodeFile, txRbDentryFile, TxIDFile}
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

func (mp *metaPartition) initTxInfo(txInfo *proto.TransactionInfo) {
	txInfo.TxID = mp.txProcessor.txManager.nextTxID()
	txInfo.TmID = int64(mp.config.PartitionId)
	txInfo.CreateTime = time.Now().UnixNano()
	txInfo.State = proto.TxStatePreCommit
}

func (mp *metaPartition) storeSnapshotFiles() (err error) {
	msg := &storeMsg{
		applyIndex:     mp.applyID,
		txId:           mp.txProcessor.txManager.txIdAlloc.getTransactionID(),
		inodeTree:      NewBtree(),
		dentryTree:     NewBtree(),
		extendTree:     NewBtree(),
		multipartTree:  NewBtree(),
		txTree:         NewBtree(),
		txRbInodeTree:  NewBtree(),
		txRbDentryTree: NewBtree(),
	}

	return mp.store(msg)
}
