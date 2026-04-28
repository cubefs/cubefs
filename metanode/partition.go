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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime/debug"
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
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

// NOTE: if the operation is invoked by local machine
// the remote addr is "127.0.0.1"
const localAddrForAudit = "127.0.0.1"

var (
	ErrIllegalHeartbeatAddress = errors.New("illegal heartbeat address")
	ErrIllegalReplicateAddress = errors.New("illegal replicate address")
	ErrSnapshotCrcMismatch     = errors.New("snapshot crc not match")
)

const (
	NotStoreMsgFlag = iota
	NeedStoreMsgFlag
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

// MetaMultiSnapshotInfo
type MetaMultiSnapshotInfo struct {
	VerSeq uint64
	Status int8
	Ctime  time.Time
}

// MetaPartitionConfig is used to create a meta partition.
type MetaPartitionConfig struct {
	// Identity for raftStore group. RaftStore nodes in the same raftStore group must have the same groupID.
	PartitionId              uint64              `json:"partition_id"`
	VolName                  string              `json:"vol_name"`
	Start                    uint64              `json:"start"` // Minimal Inode ID of this range. (Required during initialization)
	End                      uint64              `json:"end"`   // Maximal Inode ID of this range. (Required during initialization)
	PartitionType            int                 `json:"partition_type"`
	Peers                    []proto.Peer        `json:"peers"` // Peers information of the raftStore
	Cursor                   uint64              `json:"-"`     // Cursor ID of the inode that have been assigned
	UniqId                   uint64              `json:"-"`
	NodeId                   uint64              `json:"-"`
	RootDir                  string              `json:"-"`
	VerSeq                   uint64              `json:"ver_seq"`
	BeforeStart              func()              `json:"-"`
	AfterStart               func()              `json:"-"`
	BeforeStop               func()              `json:"-"`
	AfterStop                func()              `json:"-"`
	RaftStore                raftstore.RaftStore `json:"-"`
	ConnPool                 *util.ConnectPool   `json:"-"`
	Forbidden                bool                `json:"-"`
	ForbidWriteOpOfProtoVer0 bool                `json:"ForbidWriteOpOfProtoVer0"`
	Freeze                   bool                `json:"freeze"`

	StoreMode            proto.StoreMode `json:"store_mode"`
	RocksDBDir           string          `json:"rocksDb_dir"`
	RocksWalFileSize     uint64          `json:"rocks_wal_file_size"`
	RocksWalMemSize      uint64          `json:"rocks_wal_mem_size"`
	RocksLogFileSize     uint64          `json:"rocks_log_file_size"`
	RocksLogReversedTime uint64          `json:"rocks_log_reversed"`
	RocksLogReVersedCnt  uint64          `json:"rocks_log_re_versed_cnt"`
	RocksWalTTL          uint64          `json:"rocks_wal_ttl"`
}

func (c *MetaPartitionConfig) IsLearner() bool {
	for _, peer := range c.Peers {
		if peer.Type == raftproto.PeerLearner && peer.ID == c.NodeId {
			return true
		}
	}
	return false
}

func (c *MetaPartitionConfig) peersStr() string {
	peersStr := ""
	for _, peer := range c.Peers {
		peersStr += fmt.Sprintf("[%v:%v:%v:%v:%v],", peer.ID, peer.Type, peer.Addr, peer.HeartbeatPort, peer.ReplicaPort)
	}
	return peersStr
}

func (c *MetaPartitionConfig) checkMeta() (err error) {
	if c.PartitionId <= 0 {
		err = errors.NewErrorf("[checkMeta]: partition id at least 1, "+
			"now partition id is: %d", c.PartitionId)
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
	CreateInode(req *CreateInoReq, p *Packet, remoteAddr string) (err error)
	UnlinkInode(req *UnlinkInoReq, p *Packet, remoteAddr string) (err error)
	UnlinkInodeBatch(req *BatchUnlinkInoReq, p *Packet, remoteAddr string) (err error)
	InodeGet(req *InodeGetReq, p *Packet) (err error)
	InodeGetSplitEk(req *InodeGetSplitReq, p *Packet) (err error)
	InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error)
	CreateInodeLink(req *LinkInodeReq, p *Packet, remoteAddr string) (err error)
	EvictInode(req *EvictInodeReq, p *Packet, remoteAddr string) (err error)
	EvictInodeBatch(req *BatchEvictInodeReq, p *Packet, remoteAddr string) (err error)
	SetAttr(req *SetattrRequest, reqData []byte, p *Packet) (err error)
	GetInodeTreeLen() int
	DeleteInode(req *proto.DeleteInodeRequest, p *Packet, remoteAddr string) (err error)
	DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet, remoteAddr string) (err error)
	TxCreateInode(req *proto.TxCreateInodeRequest, p *Packet, remoteAddr string) (err error)
	TxUnlinkInode(req *proto.TxUnlinkInodeRequest, p *Packet, remoteAddr string) (err error)
	TxCreateInodeLink(req *proto.TxLinkInodeRequest, p *Packet, remoteAddr string) (err error)
	QuotaCreateInode(req *proto.QuotaCreateInodeRequest, p *Packet, remoteAddr string) (err error)
	InodeGetAccessTime(req *InodeGetReq, p *Packet) (err error)
	RenewalForbiddenMigration(req *proto.RenewalForbiddenMigrationRequest, p *Packet, remoteAddr string) (err error)
	UpdateExtentKeyAfterMigration(req *proto.UpdateExtentKeyAfterMigrationRequest, p *Packet, remoteAddr string) (err error)
	InodeGetWithEk(req *InodeGetReq, p *Packet) (err error)
	SetCreateTime(req *SetCreateTimeRequest, reqData []byte, p *Packet) (err error) // for debugging
	DeleteMigrationExtentKey(req *proto.DeleteMigrationExtentKeyRequest, p *Packet, remoteAddr string) (err error)
	UpdateInodeMeta(req *proto.UpdateInodeMetaRequest, p *Packet) (err error)
	ScanInodeByPool(req *proto.ScanInodeByPoolRequest, resp *proto.ScanInodeByPoolResponse) (err error)
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
	LockDir(req *proto.LockDirRequest, p *Packet) (err error)
}

// OpDentry defines the interface for the dentry operations.
type OpDentry interface {
	CreateDentry(req *CreateDentryReq, p *Packet, remoteAddr string) (err error)
	DeleteDentry(req *DeleteDentryReq, p *Packet, remoteAddr string) (err error)
	DeleteDentryBatch(req *BatchDeleteDentryReq, p *Packet, remoteAddr string) (err error)
	UpdateDentry(req *UpdateDentryReq, p *Packet, remoteAddr string) (err error)
	ReadDir(req *ReadDirReq, p *Packet) (err error)
	ReadDirLimit(req *ReadDirLimitReq, p *Packet) (err error)
	ReadDirOnly(req *ReadDirOnlyReq, p *Packet) (err error)
	Lookup(req *LookupReq, p *Packet) (err error)
	GetDentryTreeLen() int
	TxCreateDentry(req *proto.TxCreateDentryRequest, p *Packet, remoteAddr string) (err error)
	TxDeleteDentry(req *proto.TxDeleteDentryRequest, p *Packet, remoteAddr string) (err error)
	TxUpdateDentry(req *proto.TxUpdateDentryRequest, p *Packet, remoteAddr string) (err error)
	QuotaCreateDentry(req *proto.QuotaCreateDentryRequest, p *Packet, remoteAddr string) (err error)
}

type OpTransaction interface {
	TxCreate(req *proto.TxCreateRequest, p *Packet) (err error)
	TxCommitRM(req *proto.TxApplyRMRequest, p *Packet) error
	TxRollbackRM(req *proto.TxApplyRMRequest, p *Packet) error
	TxCommit(req *proto.TxApplyRequest, p *Packet, remoteAddr string) (err error)
	TxRollback(req *proto.TxApplyRequest, p *Packet, remoteAddr string) (err error)
	TxGetInfo(req *proto.TxGetInfoRequest, p *Packet) (err error)
	TxGetCnt() (uint64, uint64, uint64, error)
}

// OpExtent defines the interface for the extent operations.
type OpExtent interface {
	ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error)
	ExtentAppendWithCheck(req *proto.AppendExtentKeyWithCheckRequest, p *Packet, remoteAddr string) (err error)
	BatchObjExtentAppend(req *proto.AppendObjExtentKeysRequest, p *Packet) (err error)
	ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ObjExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ExtentsTruncate(req *ExtentsTruncateReq, p *Packet, remoteAddr string) (err error)
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
	SetTxInfo(info []*proto.TxInfo)
	GetExpiredMultipart(req *proto.GetExpiredMultipartRequest, p *Packet) (err error)
}

// MultiVersion operation from master or client
type OpMultiVersion interface {
	GetVerSeq() uint64
	GetVerList() []*proto.VolVersionInfo
	GetAllVerList() []*proto.VolVersionInfo
	HandleVersionOp(op uint8, verSeq uint64, verList []*proto.VolVersionInfo, sync bool) (err error)
	fsmVersionOp(reqData []byte) (err error)
	GetAllVersionInfo(req *proto.MultiVersionOpRequest, p *Packet) (err error)
	GetSpecVersionInfo(req *proto.MultiVersionOpRequest, p *Packet) (err error)
	GetExtentByVer(ino *Inode, req *proto.GetExtentsRequest, rsp *proto.GetExtentsResponse)
	checkVerList(info *proto.VolVersionInfoList, sync bool) (needUpdate bool, err error)
	checkByMasterVerlist(mpVerList *proto.VolVersionInfoList, masterVerList *proto.VolVersionInfoList) (err error)
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
	OpMultiVersion
}

// OpPartition defines the interface for the partition operations.
type OpPartition interface {
	GetVolName() (volName string)
	IsLeader() (leaderAddr string, isLeader bool)
	LeaderTerm() (leaderID, term uint64)
	GetCursor() uint64
	GetAppliedID() uint64
	GetUniqId() uint64
	IsFollowerRead() bool
	EnableLearnerRead() bool
	SetFollowerRead(bool)
	GetBaseConfig() MetaPartitionConfig
	ResponseLoadMetaPartition(p *Packet) (err error)
	PersistMetadata() (err error)
	RenameStaleMetadata() (err error)
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
	Reset() (err error)
	UpdatePartition(req *UpdatePartitionReq, resp *UpdatePartitionResp) (err error)
	DeleteRaft() error
	IsExsitPeer(peer proto.Peer) bool
	TryToLeader(groupID uint64) error
	CanRemoveRaftMember(peer proto.Peer) error
	IsEquareCreateMetaPartitionRequst(request *proto.CreateMetaPartitionRequest) (err error)
	GetUniqID(p *Packet, num uint32) (err error)
	CloseAndBackupRaft() error
	IsSnapshoting() bool
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
	IsForbidden() bool
	SetForbidden(status bool)
	IsForbidWriteOpOfProtoVer0() bool
	SetForbidWriteOpOfProtoVer0(status bool)
	IsEnableAuditLog() bool
	SetEnableAuditLog(status bool)
	UpdateVolumeView(dataView *proto.DataPartitionsView, volumeView *proto.SimpleVolView)
	GetStatByStorageClass() []*proto.StatOfStorageClass
	GetMigrateStatByStorageClass() []*proto.StatOfStorageClass
	GetStatByPool() []*proto.StatOfStorageClass
	GetMigrateStatByPool() []*proto.StatOfStorageClass
	SetFreeze(req *proto.FreezeMetaPartitionRequest) (err error)
	GetSnapShot() (Snapshot, error)
	ReleaseSnapShot(snap Snapshot)
	GetStoreMode() proto.StoreMode
	GetApplyID() uint64
	CalcMetaPartitionMd5Sum() error
	GetLeaseApplyTime() int64
}

type UidManager struct {
	accumBase        map[uint32]int64
	accumRebuildBase map[uint32]int64 // snapshot mirror
	uidAcl           *sync.Map
	rbuilding        bool
	volName          string
	acLock           sync.RWMutex
	mpID             uint64
}

func NewUidMgr(volName string, mpID uint64) (mgr *UidManager) {
	mgr = &UidManager{
		volName:          volName,
		mpID:             mpID,
		accumBase:        map[uint32]int64{},
		accumRebuildBase: map[uint32]int64{},
		uidAcl:           new(sync.Map),
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
		log.LogWarnf("addUidSpace.volname [%v] mp[%v] uid %v be set full", uMgr.mpID, uMgr.volName, uid)
		return proto.OpNoSpaceErr
	}

	return
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
		// log.LogDebugf("setUidAcl.volname [%v] uid %v be set enable %v", uMgr.volName, uidInfo.Uid, uidInfo.Limited)
		uMgr.uidAcl.Store(uidInfo.Uid, uidInfo.Limited)
	}
}

func (uMgr *UidManager) getAllUidSpace() (rsp []*proto.UidReportSpaceInfo) {
	uMgr.acLock.RLock()
	defer uMgr.acLock.RUnlock()

	for uid, size := range uMgr.accumBase {
		rsp = append(rsp, &proto.UidReportSpaceInfo{
			Uid:  uid,
			Size: uint64(size),
		})
	}

	return
}

func (uMgr *UidManager) accumRebuildStart() bool {
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()
	log.LogDebugf("accumRebuildStart vol [%v] mp[%v] rbuildbySnapshot [%v]", uMgr.volName, uMgr.mpID, uMgr.rbuilding)
	if uMgr.rbuilding {
		return false
	}
	uMgr.rbuilding = true
	return true
}

func (uMgr *UidManager) accumRebuildFin(rebuild bool) {
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()
	log.LogDebugf("accumRebuildFin rebuild volname [%v], mp:[%v],%v, rebuild:[%v]", uMgr.volName, uMgr.mpID,
		uMgr.accumRebuildBase, rebuild)
	uMgr.rbuilding = false
	if !rebuild {
		uMgr.accumRebuildBase = map[uint32]int64{}
		return
	}
	uMgr.accumBase = uMgr.accumRebuildBase
	uMgr.accumRebuildBase = map[uint32]int64{}
}

func (uMgr *UidManager) accumInoUidSize(ino *Inode, accum map[uint32]int64) {
	uMgr.acLock.Lock()
	defer uMgr.acLock.Unlock()

	size := ino.GetSpaceSize()
	if val, ok := accum[ino.Uid]; ok {
		size += uint64(val)
	}
	accum[ino.Uid] = int64(size)
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

type BlobStoreClientWrapper struct {
	blobClientLock    sync.Mutex
	blobClient        *blobstore.BlobStoreClient
	cfg               *access.Config
	lastTryCreateTime int64
}

func NewBlobStoreClientWrapper(cfg access.Config) (blobWrapper *BlobStoreClientWrapper, err error) {
	blobWrapper = &BlobStoreClientWrapper{
		blobClient:        nil,
		cfg:               &cfg,
		lastTryCreateTime: 0,
	}

	if _, _, err = blobWrapper.getBlobStoreClient(); err != nil {
		return blobWrapper, err
	}

	return blobWrapper, nil
}

// will create blobstore client if not created or creation failed earlier
func (ew *BlobStoreClientWrapper) getBlobStoreClient() (blobClient *blobstore.BlobStoreClient, create bool, err error) {
	ew.blobClientLock.Lock()
	defer ew.blobClientLock.Unlock()

	create = false
	if ew.blobClient != nil {
		return ew.blobClient, create, nil
	}

	if ew.cfg.Consul.Address == "" {
		// TODO:tangjingyu get blobstore addr from master
		err = errors.New("addr is empty, can not create blobstore client")
		return nil, create, err
	}

	if time.Now().Unix()-ew.lastTryCreateTime < DefaultCreateBlobClientIntervalSec {
		err = fmt.Errorf("addr(%v) create blobstore client failed, wait a while to try create again", ew.cfg.Consul.Address)
		return nil, create, err
	}

	blobClient, err = blobstore.NewEbsClient(*(ew.cfg))
	if err != nil {
		err = fmt.Errorf("addr(%v) create blobstore client err: %v", ew.cfg.Consul.Address, err.Error())
		ew.lastTryCreateTime = time.Now().Unix()
		return nil, create, err
	} else if blobClient == nil {
		err = fmt.Errorf("addr(%v) create blobstore client is nil", ew.cfg.Consul.Address)
		ew.lastTryCreateTime = time.Now().Unix()
		return nil, create, err
	}

	log.LogDebugf("[getBlobStoreClient] addr(%v) create blobstore client success", gClusterInfo.EbsAddr)
	ew.blobClient = blobClient
	ew.lastTryCreateTime = 0
	create = true
	return ew.blobClient, create, nil
}

// metaPartition manages the range of the inode IDs.
// When a new inode is requested, it allocates a new inode id for this inode if possible.
// States:
//
//	+-----+             +-------+
//	| New | → Restore → | Ready |
//	+-----+             +-------+
type metaPartition struct {
	config                    *MetaPartitionConfig
	size                      uint64                // For partition all file size
	applyID                   uint64                // Inode/Dentry max applyID, this index will be update after restoring from the dumped data.
	storedApplyId             uint64                // update after store snapshot to disk
	dentryTree                DentryTree            // btree for dentries
	inodeTree                 InodeTree             // btree for inodes
	extendTree                ExtendTree            // btree for inode extend (XAttr) management
	multipartTree             MultipartTree         // collection for multipart management
	txProcessor               *TransactionProcessor // transction processor
	raftPartition             raftstore.Partition
	stopC                     chan bool
	storeChan                 chan *storeMsg
	state                     uint32
	delInodeFp                *os.File
	freeList                  *freeList // free inode list
	freeHybridList            *freeList // to store inode delay to delete migration keys
	extDelCh                  chan []proto.ExtentKey
	extReset                  chan struct{}
	vol                       *Vol
	manager                   *metadataManager
	isLoadingMetaPartition    bool
	blobClientWrapper         *BlobStoreClientWrapper
	volType                   int // kept in hybrid cloud for compatibility
	isFollowerRead            bool
	uidManager                *UidManager
	xattrLock                 sync.Mutex
	fileRange                 []int64
	mqMgr                     *MetaQuotaManager
	nonIdempotent             sync.Mutex
	uniqChecker               *uniqChecker
	verSeq                    uint64
	multiVersionList          *proto.VolVersionInfoList
	verUpdateChan             chan []byte
	enableAuditLog            bool
	recycleInodeDelFileFlag   atomicutil.Flag
	enablePersistAccessTime   bool
	accessTimeValidInterval   uint64
	statByStorageClass        []*proto.StatOfStorageClass
	statByMigrateStorageClass []*proto.StatOfStorageClass
	statByPool                []*proto.StatOfStorageClass
	statByMigratePool         []*proto.StatOfStorageClass
	syncAtimeCh               chan uint64

	storeMsgFlag   int32 // 0: not store msg, 1: store msg
	leaseApplyTime int64

	rocksdbManager  RocksdbManager
	db              *RocksdbOperator
	multiVerApplyId uint64
	Md5ApplyId      uint64
	Md5Sum          string
}

func (mp *metaPartition) SetNeedStoreMsgFlag(flag int) {
	atomic.StoreInt32(&mp.storeMsgFlag, int32(flag))
}

func (mp *metaPartition) needStoreMsg() bool {
	return atomic.LoadInt32(&mp.storeMsgFlag) == NeedStoreMsgFlag
}

// IsLeader returns the raft leader address and if the current meta partition is the leader.
func (mp *metaPartition) SetFollowerRead(fRead bool) {
	if mp.raftPartition == nil {
		return
	}
	mp.isFollowerRead = fRead
}

func (mp *metaPartition) EnableLearnerRead() bool {
	if mp.raftPartition.IsRestoring() {
		log.LogDebugf("enable learner read failed, partition(%v) is restoring", mp.config.PartitionId)
		return false
	}

	threshold := FollowerReadLeaseTime()
	if math.Abs(float64(timeutil.GetCurrentTimeUnix()-mp.leaseApplyTime)) > float64(threshold) {
		log.LogWarnf("enable learner read failed, partition(%v) lease time is too long, leaseTime(%v), currentTime(%v), threshold(%v)",
			mp.config.PartitionId, mp.leaseApplyTime, timeutil.GetCurrentTimeUnix(), threshold)
		return false
	}

	return true
}

// IsLeader returns the raft leader address and if the current meta partition is the leader.
func (mp *metaPartition) IsFollowerRead() (ok bool) {
	if mp.raftPartition == nil {
		return false
	}

	if !mp.isFollowerRead {
		return false
	}

	if mp.raftPartition.IsRestoring() {
		return false
	}

	return true
}

func (mp *metaPartition) IsForbidden() bool {
	return mp.config.Forbidden || mp.config.Freeze
}

func (mp *metaPartition) SetForbidden(status bool) {
	mp.config.Forbidden = status
}

func (mp *metaPartition) GetVolStorageClass() uint32 {
	return mp.vol.GetVolView().VolStorageClass
}

func (mp *metaPartition) IsForbidWriteOpOfProtoVer0() bool {
	return mp.config.ForbidWriteOpOfProtoVer0
}

func (mp *metaPartition) SetForbidWriteOpOfProtoVer0(status bool) {
	mp.config.ForbidWriteOpOfProtoVer0 = status
}

func (mp *metaPartition) IsEnableAuditLog() bool {
	return mp.enableAuditLog
}

func (mp *metaPartition) SetEnableAuditLog(status bool) {
	mp.enableAuditLog = status
}

func (mp *metaPartition) raftClosed() bool {
	if mp.raftPartition == nil {
		return false
	}

	return mp.raftPartition.Closed()
}

func (mp *metaPartition) acucumRebuildStart() bool {
	return mp.uidManager.accumRebuildStart()
}

func (mp *metaPartition) acucumRebuildFin(rebuild bool) {
	mp.uidManager.accumRebuildFin(rebuild)
}

func (mp *metaPartition) acucumUidSizeByStore(ino *Inode) {
	mp.uidManager.accumInoUidSize(ino, mp.uidManager.accumRebuildBase)
}

func (mp *metaPartition) acucumUidSizeByLoad(ino *Inode) {
	mp.uidManager.accumInoUidSize(ino, mp.uidManager.accumBase)
}

func (mp *metaPartition) GetVerList() []*proto.VolVersionInfo {
	mp.multiVersionList.RWLock.RLock()
	defer mp.multiVersionList.RWLock.RUnlock()

	verList := make([]*proto.VolVersionInfo, len(mp.multiVersionList.VerList))
	copy(verList, mp.multiVersionList.VerList)

	return verList
}

// include TemporaryVerMap or else cann't recycle temporary version after restart
func (mp *metaPartition) GetAllVerList() (verList []*proto.VolVersionInfo) {
	mp.multiVersionList.RWLock.RLock()
	defer mp.multiVersionList.RWLock.RUnlock()

	verList = make([]*proto.VolVersionInfo, len(mp.multiVersionList.VerList))
	copy(verList, mp.multiVersionList.VerList)

	for _, verInfo := range mp.multiVersionList.TemporaryVerMap {
		verList = append(verList, verInfo)
	}
	sort.SliceStable(verList, func(i, j int) bool {
		return verList[i].Ver < verList[j].Ver
	})
	return
}

func (mp *metaPartition) updateSize() {
	lastApplyId := uint64(0)

	go func() {
		timer := time.NewTicker(time.Minute * 2)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if lastApplyId == mp.GetAppliedID() && lastApplyId != 0 {
					continue
				}
				lastApplyId = mp.GetAppliedID()
				mp.updateSizeLoopFunc()
			case <-mp.stopC:
				log.LogDebugf("[updateSize] stop update mp[%v] size, inodeCount(%d), dentryCount(%d)",
					mp.config.PartitionId, mp.inodeTree.Len(), mp.dentryTree.Len())
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

func (mp *metaPartition) versionInit(isCreate bool) (err error) {
	if !isCreate {
		return
	}
	var verList *proto.VolVersionInfoList
	verList, err = masterClient.AdminAPI().GetVerList(mp.config.VolName)
	if err != nil {
		log.LogErrorf("action[onStart] GetVerList err[%v]", err)
		return
	}

	for _, info := range verList.VerList {
		if info.Status != proto.VersionNormal {
			continue
		}
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, info)
	}

	log.LogDebugf("action[onStart] mp[%v] verList %v", mp.config.PartitionId, mp.multiVersionList.VerList)
	vlen := len(mp.multiVersionList.VerList)
	if vlen > 0 {
		mp.verSeq = mp.multiVersionList.VerList[vlen-1].Ver
	}

	return
}

func (mp *metaPartition) onStart(isCreate bool) (err error) {
	defer func() {
		if err == nil {
			return
		}
		mp.onStop()
	}()

	if mp.manager.metaNode.clusterEnableSnapshot {
		if err = mp.versionInit(isCreate); err != nil {
			return
		}
	}
	if err = mp.load(isCreate); err != nil {
		err = errors.NewErrorf("[onStart] load partition id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	mp.startScheduleTask()

	retryCnt := 0
	for ; retryCnt < 200; retryCnt++ {
		if err = mp.manager.forceUpdateVolumeView(mp); err != nil {
			log.LogWarnf("[onStart] vol(%v) mpId(%d) retryCnt(%v), GetVolumeSimpleInfo err[%v]",
				mp.config.VolName, mp.config.PartitionId, retryCnt, err)
			if strings.Compare(err.Error(), proto.ErrVolNotExists.Error()) == 0 {
				return
			}
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	if err != nil {
		log.LogErrorf("[onStart] vol(%v) mpId(%d), after retryCnt(%v) failed to GetVolumeSimpleInfo: %v",
			mp.config.VolName, mp.config.PartitionId, retryCnt, err)
		return
	}

	log.LogWarnf("[onStart] vol(%v) mpId(%d) retryCnt(%v), GetVolumeSimpleInfo succ",
		mp.config.VolName, mp.config.PartitionId, retryCnt)

	volInfo := mp.vol.GetVolView()
	mp.vol.volDeleteLockTime = volInfo.DeleteLockTime
	if mp.manager.metaNode.clusterEnableSnapshot {
		go mp.runVersionOp()
	}

	// Start timestamp notification goroutine
	go mp.startNotifyTimestamp()

	mp.volType = volInfo.VolType
	if !proto.IsValidStorageClass(volInfo.VolStorageClass) {
		err = errors.NewErrorf("[onStart] vol(%v) mpId(%d), get from master invalid volStorageClass(%v)",
			mp.config.VolName, mp.config.PartitionId, volInfo.VolStorageClass)
		return
	}

	log.LogInfof("[onStart] vol(%v) mpId(%v), from master VolStorageClass(%v)",
		mp.config.VolName, mp.config.PartitionId, proto.StorageClassString(mp.GetVolStorageClass()))

	if proto.IsCold(mp.volType) || proto.IsVolSupportStorageClass(volInfo.AllowedStorageClass, proto.StorageClass_BlobStore) {
		mp.blobClientWrapper, err = NewBlobStoreClientWrapper(access.Config{
			ConnMode: access.NoLimitConnMode,
			Consul: access.ConsulConfig{
				Address: gClusterInfo.EbsAddr, // gClusterInfo is fetched from master in register procedure
			},
			MaxSizePutOnce: int64(volInfo.ObjBlockSize),
			Logger: &access.Logger{
				Filename: path.Join(log.LogDir, "ebs.log"),
			},
			LogLevel: log.GetBlobLogLevel(),
		})

		if err != nil {
			log.LogWarnf("action[onStart] mp(%v) blobStoreAddr(%v), create blobstore client err[%v], but still start mp and will try create blobstore later",
				mp.config.PartitionId, gClusterInfo.EbsAddr, err)
			// not return err here, blobstore client may be created latter
		} else {
			log.LogWarnf("action[onStart] mp(%v) blobStoreAddr(%v), create blobstore client success",
				mp.config.PartitionId, gClusterInfo.EbsAddr)
		}
	}

	if err = mp.startFreeList(); err != nil {
		err = errors.NewErrorf("[onStart] start free list id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}

	go mp.startCheckerEvict()

	log.LogWarnf("[before raft] get mp[%v] applied(%d),inodeCount(%d),dentryCount(%d)", mp.config.PartitionId, mp.applyID, mp.inodeTree.Len(), mp.dentryTree.Len())

	if err = mp.startRaft(isCreate); err != nil {
		err = errors.NewErrorf("[onStart] start raft id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	log.LogWarnf("[after raft] get mp[%v] applied(%d),inodeCount(%d),dentryCount(%d)", mp.config.PartitionId, mp.applyID, mp.inodeTree.Len(), mp.dentryTree.Len())

	if mp.HasRocksDBStore() {
		go mp.ScanRocksdb()
	}

	mp.updateSize()

	if proto.IsHot(mp.volType) && mp.manager.metaNode.clusterEnableSnapshot {
		log.LogInfof("hot vol not need cacheTTL")
		go mp.multiVersionTTLWork(time.Minute)
		return
	}
	return
}

func (mp *metaPartition) startScheduleTask() {
	applyId := mp.applyID
	if mp.inodeTree.GetStoreMode() == proto.StoreModeRocksDb {
		var err error
		applyId, err = mp.inodeTree.GetApplyIdFromDisk()
		if err != nil {
			log.LogErrorf("[startScheduleTask] mp(%d) get applyId err: %s", mp.config.PartitionId, err.Error())
			applyId = 0
		}
	}
	mp.startSchedule(applyId)
}

func (mp *metaPartition) onStop() {
	mp.stopRaft()
	mp.stop()
	if mp.delInodeFp != nil {
		mp.delInodeFp.Sync()
		mp.delInodeFp.Close()
	}
}

func (mp *metaPartition) startRaft(isCreate bool) (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
	)

	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}
	for _, peer := range mp.config.Peers {
		if mp.manager.metaNode.raftPartitionCanUsingDifferentPort {
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
				ID:   peer.ID,
				Type: peer.Type,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicaPort:   replicaPort,
		}
		peers = append(peers, rp)
	}
	log.LogInfof("start partition id=%d,applyID:%v raft peers: %s",
		mp.config.PartitionId, mp.applyID, peers)
	pc := &raftstore.PartitionConfig{
		ID:       mp.config.PartitionId,
		Applied:  mp.applyID,
		Peers:    peers,
		SM:       mp,
		IsCreate: isCreate,
	}
	mp.raftPartition, err = mp.config.RaftStore.CreatePartition(pc)
	if err == nil {
		mp.ForceSetMetaPartitionToFininshLoad()
	}
	return
}

func (mp *metaPartition) stopRaft() {
	if mp.raftPartition != nil {
		mp.raftPartition.Stop()
	}
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
		config:         conf,
		stopC:          make(chan bool),
		storeChan:      make(chan *storeMsg, 100),
		freeList:       newFreeList(),
		freeHybridList: newFreeList(),
		extDelCh:       make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		syncAtimeCh:    make(chan uint64, defaultSyncInodeAtimeCnt),
		extReset:       make(chan struct{}),
		vol:            NewVol(),
		manager:        manager,
		uniqChecker:    newUniqChecker(),
		verSeq:         conf.VerSeq,
		multiVersionList: &proto.VolVersionInfoList{
			TemporaryVerMap: make(map[uint64]*proto.VolVersionInfo),
		},
		enableAuditLog: true,
	}
	if manager != nil {
		mp.rocksdbManager = manager.rocksdbManager
	}

	if mp.manager != nil && mp.manager.metaNode.raftPartitionCanUsingDifferentPort {
		// during upgrade process, create partition request may lack raft ports info
		defaultHeartbeatPort, defaultReplicaPort, err := mp.getRaftPort()
		if err == nil {
			for i := range mp.config.Peers {
				if len(mp.config.Peers[i].ReplicaPort) == 0 || len(mp.config.Peers[i].HeartbeatPort) == 0 {
					mp.config.Peers[i].ReplicaPort = strconv.FormatInt(int64(defaultReplicaPort), 10)
					mp.config.Peers[i].HeartbeatPort = strconv.FormatInt(int64(defaultHeartbeatPort), 10)
				}
			}
		}
	}

	if manager != nil {
		mp.config.ForbidWriteOpOfProtoVer0 = manager.isVolForbidWriteOpOfProtoVer0(mp.config.VolName)
	}

	go mp.batchSyncInodeAtime()

	return mp
}

// startNotifyTimestamp starts a goroutine to periodically send timestamp notifications to followers
func (mp *metaPartition) startNotifyTimestamp() {
	log.LogInfof("[startNotifyTimestamp] mp(%v) start timestamp notification", mp.config.PartitionId)
	interval := time.Duration(FollowerReadLeaseTime()) * 1000 * time.Millisecond / 3
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-mp.stopC:
			log.LogInfof("[startNotifyTimestamp] mp(%v) stop timestamp notification", mp.config.PartitionId)
			return
		case <-timer.C:

			interval = time.Duration(FollowerReadLeaseTime()) * 1000 * time.Millisecond / 3
			timer.Reset(interval)

			// Only leader sends timestamp notifications
			if _, ok := mp.IsLeader(); !ok {
				continue
			}

			// Get current timestamp
			timestamp := time.Now().Unix()

			// Encode timestamp as uint64 bytes
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(timestamp))
			// Submit through raft
			if _, err := mp.submit(opFSMNotifyTimestamp, buf); err != nil {
				log.LogWarnf("[startNotifyTimestamp] mp(%v) failed to submit timestamp notification: %v, interval: %v",
					mp.config.PartitionId, err, interval)
			} else {
				log.LogDebugf("[startNotifyTimestamp] mp(%v) sent timestamp notification: %v, interval: %v",
					mp.config.PartitionId, timestamp, interval)
			}

		}
	}
}

func (mp *metaPartition) GetVerSeq() uint64 {
	return atomic.LoadUint64(&mp.verSeq)
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

func (mp *metaPartition) LeaderTerm() (leaderID, term uint64) {
	if mp.raftPartition == nil {
		return
	}
	return mp.raftPartition.LeaderTerm()
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

// GetAppliedID returns applied ID of raft
func (mp *metaPartition) GetAppliedID() uint64 {
	return atomic.LoadUint64(&mp.applyID)
}

// GetUniqId returns the uniqid stored in the config.
func (mp *metaPartition) GetUniqId() uint64 {
	return atomic.LoadUint64(&mp.config.UniqId)
}

// PersistMetadata is the wrapper of persistMetadata.
func (mp *metaPartition) PersistMetadata() (err error) {
	mp.config.sortPeers()
	err = mp.persistMetadata()
	return
}

// Backup partition to partition.old
func (mp *metaPartition) RenameStaleMetadata() (err error) {
	err = mp.renameStaleMetadata()
	return
}

func (mp *metaPartition) parseCrcFromFile() ([]uint32, error) {
	data, err := os.ReadFile(path.Join(path.Join(mp.config.RootDir, snapshotDir), SnapshotSign))
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

const (
	CRC_COUNT_BASIC      int = 4
	CRC_COUNT_TX_STUFF   int = 7
	CRC_COUNT_UINQ_STUFF int = 8
	CRC_COUNT_MULTI_VER  int = 9
)

func (mp *metaPartition) LoadSnapshot(snapshotPath string) (err error) {
	crcs, err := mp.parseCrcFromFile()
	if err != nil {
		return err
	}

	loadFuncs := []func(rootDir string, crc uint32) error{
		mp.loadInode,
		mp.loadDentry,
		nil, // loading quota info from extend requires mp.loadInode() has been completed, so skip mp.loadExtend() here
		mp.loadMultipart,
	}

	crc_count := len(crcs)
	if crc_count != CRC_COUNT_BASIC && crc_count != CRC_COUNT_TX_STUFF && crc_count != CRC_COUNT_UINQ_STUFF && crc_count != CRC_COUNT_MULTI_VER {
		log.LogErrorf("action[LoadSnapshot] crc array length %d not match", len(crcs))
		return ErrSnapshotCrcMismatch
	}

	// handle compatibility in upgrade scenarios
	needLoadTxStuff := false
	needLoadUniqStuff := false
	needStoreMultiVer := false
	if crc_count >= CRC_COUNT_TX_STUFF {
		needLoadTxStuff = true
		loadFuncs = append(loadFuncs, mp.loadTxInfo)
		loadFuncs = append(loadFuncs, mp.loadTxRbInode)
		loadFuncs = append(loadFuncs, mp.loadTxRbDentry)
	}
	if crc_count >= CRC_COUNT_UINQ_STUFF {
		needLoadUniqStuff = true
		loadFuncs = append(loadFuncs, mp.loadUniqChecker)
	}

	if crc_count == CRC_COUNT_MULTI_VER {
		if err = mp.loadMultiVer(snapshotPath, crcs[CRC_COUNT_MULTI_VER-1]); err != nil {
			return
		}
	} else {
		needStoreMultiVer = true
	}

	errs := make([]error, len(loadFuncs))
	var wg sync.WaitGroup
	wg.Add(len(loadFuncs))
	for idx, f := range loadFuncs {
		loadFunc := f
		if f == nil {
			wg.Done()
			continue
		}

		i := idx
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.LogWarnf("action[LoadSnapshot] recovered when load partition partition: %v, failed: %v, i %d, stack %s",
						mp.config.PartitionId, r, i, debug.Stack())

					errs[i] = errors.NewErrorf("%v, i %d, stack %s", r, i, debug.Stack())
				}

				wg.Done()
			}()

			errs[i] = loadFunc(snapshotPath, crcs[i])
		}()
	}

	wg.Wait()
	log.LogDebugf("[load meta finish] get mp[%v] inodeCount(%d),dentryCount(%d)", mp.config.PartitionId, mp.inodeTree.Len(), mp.dentryTree.Len())
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

	if needLoadUniqStuff {
		if err = mp.loadUniqID(snapshotPath); err != nil {
			return
		}
	}

	if err = mp.loadApplyID(snapshotPath); err != nil {
		return
	}

	if needStoreMultiVer {
		_, _ = mp.storeMultiVersion(snapshotPath, &storeMsg{
			multiVerList: mp.multiVersionList.VerList,
			applyIndex:   mp.GetApplyID(),
		})
	}
	return
}

func (mp *metaPartition) load(isCreate bool) (err error) {
	if err = mp.loadMetadata(); err != nil {
		return
	}

	if err = mp.initObjects(isCreate); err != nil {
		log.LogErrorf("[load] failed to init objects, mp(%v) err(%v)", mp.config.PartitionId, err)
		return
	}

	// 1. create new metaPartition, no need to load snapshot
	// 2. store the snapshot files for new mp, because
	// mp.load() will check all the snapshot files when mn startup
	// NOTE: only memory store need to create snapshot files
	if isCreate {
		if mp.HasRocksDBStore() {
			if err = mp.persistMetadata(); err != nil {
				log.LogErrorf("[load] mp(%v) failed to persist rocksdb dir to metadata file, err(%v)", mp.config.PartitionId, err)
				return
			}
			return
		}
		if err = mp.storeSnapshotFiles(); err != nil {
			err = errors.NewErrorf("[onStart] storeSnapshotFiles for partition id=%d: %s",
				mp.config.PartitionId, err.Error())
		}
		return
	}

	if mp.HasRocksDBStore() {
		err = mp.LoadDataFromRocksDb()
		if err != nil {
			log.LogErrorf("[load] failed to load rocksdb data, mp(%v) err(%v)", mp.config.PartitionId, err)
			return err
		}
		err = mp.loadRocksdbFile()
		if err != nil {
			log.LogErrorf("[load] failed to load data, mp(%v) err(%v)", mp.config.PartitionId, err)
			return err
		}
		if err = mp.loadRocksdbApplyID(); err != nil {
			log.LogErrorf("[load] failed to load apply id, mp(%v) err(%v)", mp.config.PartitionId, err)
			return
		}
		if log.EnableDebug() {
			log.LogDebugf("[load] mp(%v) is create(%v)", mp.config.PartitionId, isCreate)
			log.LogDebugf("[load] mp(%v) apply id(%v)", mp.config.PartitionId, mp.applyID)
			log.LogDebugf("[load] mp(%v) inode real len(%v)", mp.config.PartitionId, mp.inodeTree.RealCount())
			log.LogDebugf("[load] mp(%v) dentry real len(%v)", mp.config.PartitionId, mp.dentryTree.RealCount())
			log.LogDebugf("[load] mp(%v) extentd real len(%v)", mp.config.PartitionId, mp.extendTree.RealCount())
			log.LogDebugf("[load] mp(%v) multipart real len(%v)", mp.config.PartitionId, mp.multipartTree.RealCount())
			txTree := mp.txProcessor.txManager.txTree
			txRbInoTree := mp.txProcessor.txResource.txRbInodeTree
			txRbDenTree := mp.txProcessor.txResource.txRbDentryTree
			log.LogDebugf("[load] mp(%v) tx real len(%v)", mp.config.PartitionId, txTree.RealCount())
			log.LogDebugf("[load] mp(%v) tx rb inode real len(%v)", mp.config.PartitionId, txRbInoTree.RealCount())
			log.LogDebugf("[load] mp(%v) tx rb dentry real len(%v)", mp.config.PartitionId, txRbDenTree.RealCount())
		}
		return
	}

	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	if _, err = os.Stat(snapshotPath); err != nil {
		log.LogErrorf("load snapshot failed, err: %s", err.Error())
		return nil

	}

	err = mp.LoadSnapshot(snapshotPath)
	if err != nil {
		return err
	}
	return nil
}

func (mp *metaPartition) doFileStats(thresholds []uint64) {
	fileRange := make([]int64, len(thresholds)+1)
	snap, err := mp.GetSnapShot()
	if err != nil {
		log.LogErrorf("mp[%d] create snap shot failed", mp.config.PartitionId)
		return
	}
	defer func() {
		mp.ReleaseSnapShot(snap)
		if err != nil {
			log.LogErrorf("mp[%d] range inode failed", mp.config.PartitionId)
		}
	}()

	err = snap.Range(InodeType, func(i interface{}) bool {
		ino := i.(*Inode)
		if proto.IsRegular(ino.Type) && ino.NLink > 0 {
			index := calculateFileRangeIndex(ino.Size, thresholds)
			if index >= 0 && index < len(fileRange) {
				fileRange[index]++
			}
		}
		return true
	})
	mp.fileRange = fileRange
}

func (mp *metaPartition) store(sm *storeMsg) (err error) {
	log.LogWarnf("metaPartition %d storeMode(%d) store applyIndex %v", mp.config.PartitionId, mp.config.StoreMode, sm.applyIndex)
	defer func() {
		log.LogWarnf("metaPartition %d storeMode(%d) store applyIndex %v finish", mp.config.PartitionId, mp.config.StoreMode, sm.applyIndex)
	}()

	if mp.inodeTree.GetStoreMode() == proto.StoreModeRocksDb {
		err = mp.storeRocksdb(sm)
		if err != nil {
			log.LogErrorf("storeRocksdb err: %s", err.Error())
			return err
		}
		return nil
	}

	tmpDir := path.Join(mp.config.RootDir, snapshotDirTmp)
	if _, err = os.Stat(tmpDir); err == nil {
		// TODO Unhandled errors
		os.RemoveAll(tmpDir)
	}
	if err = os.MkdirAll(tmpDir, 0o775); err != nil {
		return
	}

	defer func() {
		if err != nil {
			// TODO Unhandled errors
			os.RemoveAll(tmpDir)
		}
	}()
	crcBuffer := bytes.NewBuffer(make([]byte, 0, 16))
	storeFuncs := []func(dir string, sm *storeMsg) (uint32, error){
		mp.storeInode,
		mp.storeDentry,
		mp.storeExtend,
		mp.storeMultipart,
		mp.storeTxInfo,
		mp.storeTxRbInode,
		mp.storeTxRbDentry,
		mp.storeUniqChecker,
		mp.storeMultiVersion,
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
	log.LogWarnf("metaPartition %d store applyIndex %v", mp.config.PartitionId, sm.applyIndex)
	if err = mp.storeApplyID(tmpDir, sm); err != nil {
		return
	}
	if err = mp.storeTxID(tmpDir, sm); err != nil {
		return
	}
	if err = mp.storeUniqID(tmpDir, sm); err != nil {
		return
	}

	// write crc to file
	if err = fileutil.WriteFileWithSync(path.Join(tmpDir, SnapshotSign), crcBuffer.Bytes(), 0o775); err != nil {
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
	if err != nil {
		return
	}

	mp.storedApplyId = sm.applyIndex
	return
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
			log.LogWarnf("nextInodeID: can't create inode again, cur %d, end %d", cur, end)
			return 0, ErrInodeIDOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mp.config.Cursor, cur, newId) {
			return newId, nil
		}
		mp.inodeTree.SetCursor(newId)
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
	resp *UpdatePartitionResp,
) (err error) {
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
	resp.ApplyID = mp.getApplyID()
	resp.CommittedID = mp.getCommittedID()
	resp.Md5ApplyId = mp.Md5ApplyId
	resp.Md5Sum = mp.Md5Sum
	resp.RocksdbDir = mp.config.RocksDBDir

	resp.RaftInfo.DownReplicas = mp.config.RaftStore.RaftServer().GetDownReplicas(mp.config.PartitionId)
	resp.RaftInfo.PendingPeers = mp.config.RaftStore.RaftServer().GetPendingReplica(mp.config.PartitionId)
	if rStatus := mp.config.RaftStore.RaftStatus(mp.config.PartitionId); rStatus != nil {
		resp.RaftInfo.RaftStatus = *rStatus
	}
	resp.RaftInfo.Hosts = mp.config.Peers

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

// Reset resets the meta partition.
func (mp *metaPartition) Reset() (err error) {
	log.LogWarnf("[Reset] reset mp(%v) storeMode(%d)", mp.config.PartitionId, mp.config.StoreMode)

	if mp.inodeTree.GetStoreMode() == proto.StoreModeRocksDb {
		// Add to async cleaner instead of direct Clear
		err = mp.manager.rocksdbCleaner.AddTask(mp)
		if err != nil {
			log.LogErrorf("[Reset] add task to cleaner failed, err: %v", err)
			return err
		}
		mp.rocksdbManager.CloseRocksdb(mp.db)
		mp.db = nil
	}

	mp.txProcessor.Reset()
	log.LogWarnf("[Reset] clear and reset mp(%v) storeMode(%d)", mp.config.PartitionId, mp.config.StoreMode)

	// remove files
	filenames := []string{applyIDFile, dentryFile, inodeFile, extendFile, multipartFile, verdataFile, txInfoFile, txRbInodeFile, txRbDentryFile, TxIDFile}
	for _, filename := range filenames {
		filepath := path.Join(mp.config.RootDir, filename)
		if err = os.Remove(filepath); err != nil && !os.IsNotExist(err) {
			return
		}
		err = nil
	}

	if mp.config.RocksDBDir != "" && mp.rocksdbManager != nil {
		err = mp.rocksdbManager.DetachPartition(mp.config.RocksDBDir)
		if err != nil {
			log.LogWarnf("[opDeleteMetaPartition] failed to detach partition from rocksdb manager, err(%v)", err)
			err = nil
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
func (mp *metaPartition) multiVersionTTLWork(dur time.Duration) {
	// do cache ttl work
	// first sleep a rand time, range [0, 1200s(20m)],
	// make sure all mps is not doing scan work at the same time.
	rand.Seed(time.Now().Unix())
	time.Sleep(time.Duration(rand.Intn(60)))
	log.LogDebugf("[multiVersionTTLWork] start, mp[%v]", mp.config.PartitionId)
	ttl := time.NewTicker(dur)
	snapQueue := make(chan interface{}, 5)
	for {
		select {
		case <-ttl.C:
			log.LogDebugf("[multiVersionTTLWork] begin cache ttl, mp[%v]", mp.config.PartitionId)
			mp.multiVersionList.RWLock.RLock()
			volVersionInfoList := &proto.VolVersionInfoList{
				TemporaryVerMap: make(map[uint64]*proto.VolVersionInfo),
			}
			copy(volVersionInfoList.VerList, mp.multiVersionList.VerList)
			for key, value := range mp.multiVersionList.TemporaryVerMap {
				copiedValue := *value
				volVersionInfoList.TemporaryVerMap[key] = &copiedValue
			}

			mp.multiVersionList.RWLock.RUnlock()
			for _, version := range volVersionInfoList.TemporaryVerMap {
				if version.Status == proto.VersionDeleting {
					continue
				}
				snapQueue <- nil
				version.Status = proto.VersionDeleting
				go func(verSeq uint64) {
					mp.delPartitionVersion(verSeq)
					mp.multiVersionList.RWLock.Lock()
					delete(mp.multiVersionList.TemporaryVerMap, verSeq)
					mp.multiVersionList.RWLock.Unlock()
					<-snapQueue
				}(version.Ver)
			}

		case <-mp.stopC:
			log.LogWarnf("[multiVersionTTLWork] stoped, mp[%v]", mp.config.PartitionId)
			return
		}
	}
}

func (mp *metaPartition) delPartitionVersion(verSeq uint64) {
	var wg sync.WaitGroup
	wg.Add(3)
	reqVerSeq := verSeq
	if reqVerSeq == 0 {
		reqVerSeq = math.MaxUint64
	}

	log.LogInfof("action[delPartitionVersion] mp[%v] verseq [%v]:%v", mp.config.PartitionId, verSeq, reqVerSeq)
	go mp.delPartitionInodesVersion(reqVerSeq, &wg)
	go mp.delPartitionExtendsVersion(reqVerSeq, &wg)
	go mp.delPartitionDentriesVersion(reqVerSeq, &wg)
	wg.Wait()
}

func (mp *metaPartition) delPartitionDentriesVersion(verSeq uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	// begin
	count := 0
	needSleep := false

	snap, err := mp.GetSnapShot()
	if err != nil {
		log.LogErrorf("mp[%d] create snap shot failed", mp.config.PartitionId)
		return
	}
	defer func() {
		if snap != nil {
			mp.ReleaseSnapShot(snap)
		}
		if err != nil {
			log.LogErrorf("mp[%d] range inode failed", mp.config.PartitionId)
		}
	}()

	err = snap.Range(DentryType, func(i interface{}) bool {
		if _, ok := mp.IsLeader(); !ok {
			return false
		}
		den := i.(*Dentry)
		// dir type just skip

		p := &Packet{}
		req := &proto.DeleteDentryRequest{
			VolName:     mp.config.VolName,
			ParentID:    den.ParentId,
			PartitionID: mp.config.PartitionId,
			Name:        den.Name,
			Verseq:      verSeq,
		}
		mp.DeleteDentry(req, p, localAddrForAudit)
		// check empty result.
		// if result is OpAgain, means the extDelCh maybe full,
		// so let it sleep 1s.
		if p.ResultCode == proto.OpAgain {
			needSleep = true
		}

		// every 1000 inode sleep 1s
		count++
		if count >= 1000 || needSleep {
			count %= 1000
			needSleep = false
			time.Sleep(time.Second)
		}
		return true
	})
}

func (mp *metaPartition) delPartitionExtendsVersion(verSeq uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	// begin
	count := 0
	needSleep := false

	snap, err := mp.GetSnapShot()
	if err != nil {
		log.LogErrorf("mp[%d] create snap shot failed", mp.config.PartitionId)
		return
	}
	defer func() {
		if snap != nil {
			mp.ReleaseSnapShot(snap)
		}
		if err != nil {
			log.LogErrorf("mp[%d] range inode failed", mp.config.PartitionId)
		}
	}()

	err = snap.Range(ExtendType, func(treeItem interface{}) bool {
		if _, ok := mp.IsLeader(); !ok {
			return false
		}
		e := treeItem.(*Extend)

		p := &Packet{}
		req := &proto.RemoveXAttrRequest{
			VolName:     mp.config.VolName,
			PartitionId: mp.config.PartitionId,
			Inode:       e.inode,
			VerSeq:      verSeq,
		}
		mp.RemoveXAttr(req, p)
		// check empty result.
		// if result is OpAgain, means the extDelCh maybe full,
		// so let it sleep 1s.
		if p.ResultCode == proto.OpAgain {
			needSleep = true
		}

		// every 1000 inode sleep 1s
		count++
		if count >= 1000 || needSleep {
			count %= 1000
			needSleep = false
			time.Sleep(time.Second)
		}
		return true
	})
}

func (mp *metaPartition) delPartitionInodesVersion(verSeq uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	// begin
	count := 0
	needSleep := false

	snap, err := mp.GetSnapShot()
	if err != nil {
		log.LogErrorf("mp[%d] create snap shot failed", mp.config.PartitionId)
		return
	}
	defer func() {
		mp.ReleaseSnapShot(snap)
		if err != nil {
			log.LogErrorf("mp[%d] range inode failed", mp.config.PartitionId)
		}
	}()

	err = snap.Range(InodeType, func(i interface{}) bool {
		if _, ok := mp.IsLeader(); !ok {
			return false
		}
		inode := i.(*Inode)
		// dir type just skip
		if proto.IsDir(inode.Type) {
			return true
		}

		inode.RLock()
		// eks is empty just skip
		if ok, _ := inode.ShouldDelVer(verSeq, mp.verSeq); !ok {
			inode.RUnlock()
			return true
		}

		p := &Packet{}
		req := &proto.UnlinkInodeRequest{
			Inode:  inode.Inode,
			VerSeq: verSeq,
		}
		inode.RUnlock()

		mp.UnlinkInode(req, p, localAddrForAudit)
		// check empty result.
		// if result is OpAgain, means the extDelCh maybe full,
		// so let it sleep 1s.
		if p.ResultCode == proto.OpAgain {
			needSleep = true
		}

		// every 1000 inode sleep 1s
		count++
		if count >= 1000 || needSleep {
			count %= 1000
			needSleep = false
			time.Sleep(time.Second)
		}
		return true
	})
}

func (mp *metaPartition) initTxInfo(txInfo *proto.TransactionInfo) error {
	txInfo.TxID = mp.txProcessor.txManager.nextTxID()

	txInfo.CreateTime = time.Now().Unix()
	txInfo.State = proto.TxStatePreCommit

	if mp.txProcessor.txManager.opLimiter.Allow() {
		return nil
	}

	return fmt.Errorf("tx create is limited")
}

func (mp *metaPartition) storeSnapshotFiles() (err error) {
	snap, err := mp.GetSnapShot()
	if err != nil {
		return
	}
	defer snap.Close()
	msg := &storeMsg{
		snap:         snap,
		applyIndex:   mp.GetAppliedID(),
		uniqId:       mp.GetUniqId(),
		uniqChecker:  newUniqChecker(),
		multiVerList: mp.multiVersionList.VerList,
	}

	return mp.store(msg)
}

func (mp *metaPartition) startCheckerEvict() {
	timer := time.NewTimer(opCheckerInterval)
	for {
		select {
		case <-timer.C:
			if _, ok := mp.IsLeader(); ok {
				left, evict, err := mp.uniqCheckerEvict()
				if evict != 0 {
					log.LogInfof("[uniqChecker] after doEvict partition-%d, left:%d, evict:%d, err:%v", mp.config.PartitionId, left, evict, err)
				} else {
					log.LogDebugf("[uniqChecker] after doEvict partition-%d, left:%d, evict:%d, err:%v", mp.config.PartitionId, left, evict, err)
				}
			}
			timer.Reset(opCheckerInterval)
		case <-mp.stopC:
			return
		}
	}
}

func (mp *metaPartition) GetVolName() (volName string) {
	return mp.config.VolName
}

func (mp *metaPartition) GetAccessTimeValidInterval() time.Duration {
	interval := atomic.LoadUint64(&mp.accessTimeValidInterval)
	if interval == 0 {
		return proto.DefaultAccessTimeValidInterval
	}
	return time.Duration(interval)
}

func (mp *metaPartition) GetStatByStorageClass() []*proto.StatOfStorageClass {
	return mp.statByStorageClass
}

func (mp *metaPartition) GetMigrateStatByStorageClass() []*proto.StatOfStorageClass {
	return mp.statByMigrateStorageClass
}

func (mp *metaPartition) GetStatByPool() []*proto.StatOfStorageClass {
	return mp.statByPool
}

func (mp *metaPartition) GetMigrateStatByPool() []*proto.StatOfStorageClass {
	return mp.statByMigratePool
}

func (mp *metaPartition) CloseAndBackupRaft() (err error) {
	err = mp.raftPartition.CloseAndBackup()
	return
}

func (mp *metaPartition) SetFreeze(req *proto.FreezeMetaPartitionRequest) (err error) {
	reqData, err := json.Marshal(*req)
	if err != nil {
		return
	}
	r, err := mp.submit(opFSMSetFreeze, reqData)
	if err != nil {
		return
	}
	if status := r.(uint8); status != proto.OpOk {
		p := &Packet{}
		p.ResultCode = status
		err = errors.NewErrorf("[SetFreeze]: %s", p.GetResultMsg())
		return
	}

	return nil
}

func (mp *metaPartition) getFileRange() []int64 {
	fileRangeCopy := make([]int64, len(mp.fileRange))
	copy(fileRangeCopy, mp.fileRange)
	return fileRangeCopy
}

func (mp *metaPartition) HasRocksDBStore() bool {
	return mp.config.StoreMode&proto.StoreModeRocksDb != 0
}

func (mp *metaPartition) GetSnapShot() (Snapshot, error) {
	return NewSnapshot(mp)
}

func (mp *metaPartition) ReleaseSnapShot(snap Snapshot) {
	snap.Close()
}

func (mp *metaPartition) initMemoryTree() {
	mp.dentryTree = &DentryBTree{NewBtree()}
	mp.inodeTree = &InodeBTree{NewBtree()}
	mp.extendTree = &ExtendBTree{NewBtree()}
	mp.multipartTree = &MultipartBTree{NewBtree()}
}

func (mp *metaPartition) initRocksDBTree() (err error) {
	var tree *RocksTree

	if tree, err = DefaultRocksTree(mp.db, mp.config.PartitionId); err != nil {
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
	return
}

func (mp *metaPartition) selectRocksDBDir() (err error) {
	dir, err := mp.manager.rocksdbManager.SelectRocksdbDisk(mp.manager.rocksDBDiskUsageThreshold)
	if err != nil {
		log.LogErrorf("[selectRocksDBDir] mp(%v) select failed(%v)",
			mp.config.PartitionId, err)
		return
	}
	mp.config.RocksDBDir = dir
	return
}

func (mp *metaPartition) LoadDataFromRocksDb() (err error) {
	// NOTE: load tx id
	txId := mp.inodeTree.GetTxId()
	if txId > mp.txProcessor.txManager.txIdAlloc.getTransactionID() {
		mp.txProcessor.txManager.txIdAlloc.setTransactionID(txId)
	}
	uniqId := mp.inodeTree.GetUniqID()
	if uniqId > mp.GetUniqId() {
		atomic.StoreUint64(&mp.config.UniqId, uniqId)
	}
	log.LogDebugf("[LoadDataFromRocksDb] mp(%v) load tx id(%v) uniqid(%d)", mp.config.PartitionId, mp.txProcessor.txManager.txIdAlloc.getTransactionID(), uniqId)
	return
}

func (mp *metaPartition) Clear() (err error) {
	log.LogDebugf("[Clear] mp(%v) clear data", mp.config.PartitionId)

	if !mp.HasRocksDBStore() {
		return nil
	}

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to open write handle, err(%v)", mp.config.PartitionId, err)
		return
	}
	if err = mp.inodeTree.Clear(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to clear inode tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	if err = mp.dentryTree.Clear(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to clear dentry tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	if err = mp.extendTree.Clear(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to clear extend tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	if err = mp.multipartTree.Clear(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to clear multipart tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	txTree := mp.txProcessor.txManager.txTree
	txRbInoTree := mp.txProcessor.txResource.txRbInodeTree
	txRbDenTree := mp.txProcessor.txResource.txRbDentryTree
	if err = txTree.Clear(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to clear transaction tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	if err = txRbInoTree.Clear(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to clear transaction rollback inode tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	if err = txRbDenTree.Clear(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to clear transaction rollback dentry tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	// NOTE: delete metadata
	if err = mp.inodeTree.DeleteMetadata(handle); err != nil {
		log.LogErrorf("[Clear] mp(%v) failed to delete metadata, err(%v)", mp.config.PartitionId, err)
		return
	}
	err = mp.inodeTree.CommitAndReleaseBatchWriteForClear(handle)
	if err != nil {
		return
	}

	mp.applyID = 0
	mp.txProcessor.txManager.txIdAlloc.setTransactionID(0)
	mp.config.Cursor = 0
	mp.config.UniqId = 0
	return
}

func (mp *metaPartition) updateSizeLoopFunc() error {
	migrateSize := uint64(0)
	migrateInodeCnt := uint32(0)

	statStorageClassMap := make(map[uint32]*proto.StatOfStorageClass)
	var statStorageClass *proto.StatOfStorageClass
	statByMigStorageClassMap := make(map[uint32]*proto.StatOfStorageClass)
	var statMigStorageClass *proto.StatOfStorageClass
	statByPoolMap := make(map[uint8]*proto.StatOfStorageClass)
	var statPool *proto.StatOfStorageClass
	statByMigPoolMap := make(map[uint8]*proto.StatOfStorageClass)
	var statMigPool *proto.StatOfStorageClass
	var ok bool

	uidRebuild := mp.acucumRebuildStart()
	thresholds, _, enable := mp.manager.GetFileStatsConfig()
	fileRange := make([]int64, len(thresholds)+1)

	snap, err := mp.GetSnapShot()
	if err != nil {
		log.LogErrorf("mp[%d] create snap shot failed", mp.config.PartitionId)
		return err
	}
	defer func() {
		if snap != nil {
			mp.ReleaseSnapShot(snap)
		}
		if err != nil {
			log.LogErrorf("mp[%d] range inode failed", mp.config.PartitionId)
		}
	}()

	err = snap.RangeReuseInode(func(inode *Inode) bool {
		// to update metaPartition size
		migrateInodeCnt++
		migrateSize += inode.Size

		// stat normal Extents
		if statStorageClass, ok = statStorageClassMap[inode.StorageClass]; !ok {
			statStorageClass = proto.NewStatOfStorageClass(inode.StorageClass)
			statStorageClassMap[inode.StorageClass] = statStorageClass
		}
		statStorageClass.InodeCount++
		statStorageClass.UsedSizeBytes += inode.Size

		// stat pool
		if statPool, ok = statByPoolMap[inode.PoolId]; !ok {
			statPool = proto.NewStatOfStorageClassByPool(inode.PoolId)
			statByPoolMap[inode.PoolId] = statPool
		}
		statPool.InodeCount++
		statPool.UsedSizeBytes += inode.Size

		// stat migration Extents
		if inode.HybridCloudExtentsMigration == nil ||
			inode.HybridCloudExtentsMigration.sortedEks == nil ||
			!proto.IsValidStorageClass(inode.HybridCloudExtentsMigration.storageClass) {
			return true
		}

		migrateStorageClass := inode.HybridCloudExtentsMigration.storageClass
		if statMigStorageClass, ok = statByMigStorageClassMap[migrateStorageClass]; !ok {
			statMigStorageClass = proto.NewStatOfStorageClass(migrateStorageClass)
			statByMigStorageClassMap[migrateStorageClass] = statMigStorageClass
		}
		statMigStorageClass.InodeCount++
		statMigStorageClass.UsedSizeBytes += inode.Size

		// stat migration pool
		if statMigPool, ok = statByMigPoolMap[inode.HybridCloudExtentsMigration.poolId]; !ok {
			statMigPool = proto.NewStatOfStorageClassByPool(inode.HybridCloudExtentsMigration.poolId)
			statByMigPoolMap[inode.HybridCloudExtentsMigration.poolId] = statMigPool
		}
		statMigPool.InodeCount++
		statMigPool.UsedSizeBytes += inode.Size

		if uidRebuild {
			mp.acucumUidSizeByStore(inode)
		}
		if enable && proto.IsRegular(inode.Type) && inode.NLink > 0 {
			index := calculateFileRangeIndex(inode.Size, thresholds)
			if index >= 0 && index < len(fileRange) {
				fileRange[index]++
			}
		}

		return true
	})

	log.LogDebugf("[Trace updateSizeLoopFunc] metanode vol(%s) mp(%v) migrateSize(%v) mpSize(%v) migrateInodeCnt(%v)",
		mp.config.VolName, mp.config.PartitionId, migrateSize, mp.size, migrateInodeCnt)
	mp.fileRange = fileRange
	mp.acucumRebuildFin(uidRebuild)
	mp.size = migrateSize

	normalToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat := range statStorageClassMap {
		normalToSlice = append(normalToSlice, stat)
	}
	mp.statByStorageClass = normalToSlice

	migrateToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, migStat := range statByMigStorageClassMap {
		migrateToSlice = append(migrateToSlice, migStat)
	}
	mp.statByMigrateStorageClass = migrateToSlice

	poolToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat := range statByPoolMap {
		poolToSlice = append(poolToSlice, stat)
	}
	mp.statByPool = poolToSlice

	migratePoolToSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat := range statByMigPoolMap {
		migratePoolToSlice = append(migratePoolToSlice, stat)
	}
	mp.statByMigratePool = migratePoolToSlice

	log.LogDebugf("[updateSize] update mp(%d) size(%d) success, inodeCount(%d), dentryCount(%d), "+
		"migrateInodeCount(%v) migrateSize(%v)",
		mp.config.PartitionId, mp.size, mp.inodeTree.Len(), mp.dentryTree.Len(),
		migrateInodeCnt, migrateSize)

	return nil
}

func (mp *metaPartition) initObjects(isCreate bool) (err error) {
	mp.uidManager = NewUidMgr(mp.config.VolName, mp.config.PartitionId)
	mp.mqMgr = NewQuotaManager(mp.config.VolName, mp.config.PartitionId)

	if mp.HasRocksDBStore() {
		mp.db, err = mp.rocksdbManager.OpenRocksdb(mp.config.RocksDBDir, mp.config.PartitionId)
		if err != nil {
			log.LogErrorf("[initObjects] mp(%v) failed to open rocksdb, err(%v)", mp.config.PartitionId, err)
			return
		}
		if err = mp.initRocksDBTree(); err != nil {
			return
		}
	} else {
		mp.initMemoryTree()
	}

	mp.txProcessor = NewTransactionProcessor(mp)
	return
}

func (mp *metaPartition) GetStoreMode() proto.StoreMode {
	return mp.inodeTree.GetStoreMode()
}

func (mp *metaPartition) storeRocksdb(sm *storeMsg) (err error) {
	err = mp.storeRocksdbExtent(sm)
	if err != nil {
		log.LogErrorf("storeRocksdb rocksdb extent failed, err: %s", err.Error())
		return err
	}

	err = mp.storeRocksdbFile(sm)
	if err != nil {
		log.LogErrorf("storeRocksdb rocksdb file failed, err: %s", err.Error())
		return err
	}

	uniqid := mp.GetUniqId()
	if uniqid > mp.inodeTree.GetUniqID() {
		mp.inodeTree.SetUniqID(uniqid)
		err = mp.inodeTree.PersistBaseInfo()
		if err != nil {
			log.LogErrorf("storeRocksdb persist base info failed, err: %s", err.Error())
			return err
		}
	}

	mp.storedApplyId = sm.applyIndex
	return nil
}

func (mp *metaPartition) ScanRocksdb() error {
	maxInode, err := mp.loadRocksdbInode()
	if err != nil {
		log.LogErrorf("mp[%d] scan inode failed: %s", mp.config.PartitionId, err.Error())
		return err
	}

	if maxInode > atomic.LoadUint64(&mp.config.Cursor) {
		log.LogWarnf("ScanRocksdb mp[%d] set cursor from (%v) to (%v)", mp.config.PartitionId, atomic.LoadUint64(&mp.config.Cursor), maxInode)
		atomic.StoreUint64(&mp.config.Cursor, maxInode)
	}

	err = mp.loadRocksdbExtent()
	if err != nil {
		log.LogErrorf("[load] failed to load rocksdb extent, mp(%v) err: %s", mp.config.PartitionId, err.Error())
		return err
	}

	return nil
}

func (mp *metaPartition) IsSnapshoting() bool {
	isRestoring := mp.raftPartition.IsRestoring()
	if isRestoring {
		log.LogWarnf("IsSnapshoting mp[%d] isRestoring: %v", mp.config.PartitionId, isRestoring)
	}
	return isRestoring
}

func (mp *metaPartition) GetApplyID() uint64 {
	return atomic.LoadUint64(&mp.applyID)
}

func (mp *metaPartition) CalcMetaPartitionMd5Sum() error {
	_, err := mp.submit(opFSMCalcMetaPartitionMd5Sum, nil)
	if err != nil {
		return err
	}

	return nil
}

func (mp *metaPartition) GetLeaseApplyTime() int64 {
	return mp.leaseApplyTime
}
