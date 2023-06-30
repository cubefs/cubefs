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
	"fmt"
	"github.com/cubefs/cubefs/util/unit"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
)

// Type alias.
type (
	// Master -> MetaNode  create metaPartition request
	CreateMetaRangeReq = proto.CreateMetaPartitionRequest
	// MetaNode -> Master create metaPartition response
	CreateMetaRangeResp = proto.CreateMetaPartitionResponse
	// Client -> MetaNode create Inode request
	CreateInoReq = proto.CreateInodeRequest
	// MetaNode -> Client create Inode response
	CreateInoResp = proto.CreateInodeResponse
	// Client -> MetaNode create Link Request
	LinkInodeReq = proto.LinkInodeRequest
	// MetaNode -> Client create Link Response
	LinkInodeResp = proto.LinkInodeResponse
	// Client -> MetaNode delete Inode request struct
	UnlinkInoReq = proto.UnlinkInodeRequest
	// Client -> MetaNode delete Inode request struct
	BatchUnlinkInoReq = proto.BatchUnlinkInodeRequest
	// MetaNode -> Client delete Inode response
	UnlinkInoResp = proto.UnlinkInodeResponse
	// MetaNode -> Client delete batch Inode response
	BatchUnlinkInoResp = proto.BatchUnlinkInodeResponse
	// Client -> MetaNode create Dentry request struct
	CreateDentryReq = proto.CreateDentryRequest
	// Client -> MetaNode delete Dentry request
	DeleteDentryReq = proto.DeleteDentryRequest
	// Client -> MetaNode delete Dentry request
	BatchDeleteDentryReq = proto.BatchDeleteDentryRequest
	// MetaNode -> Client delete Dentry response
	DeleteDentryResp = proto.DeleteDentryResponse
	// MetaNode -> Client batch delete Dentry response
	BatchDeleteDentryResp = proto.BatchDeleteDentryResponse
	// Client -> MetaNode updateDentry request
	UpdateDentryReq = proto.UpdateDentryRequest
	// MetaNode -> Client updateDentry response
	UpdateDentryResp = proto.UpdateDentryResponse
	// Client -> MetaNode read dir request
	ReadDirReq = proto.ReadDirRequest
	// MetaNode -> Client read dir response
	ReadDirResp = proto.ReadDirResponse
	// MetaNode -> Client lookup
	LookupReq = proto.LookupRequest

	// Client -> MetaNode lookup
	LookupResp = proto.LookupResponse
	// Client -> MetaNode
	InodeGetReq = proto.InodeGetRequest
	// Client -> MetaNode
	InodeGetReqBatch = proto.BatchInodeGetRequest
	// Master -> MetaNode
	UpdatePartitionReq = proto.UpdateMetaPartitionRequest
	// MetaNode -> Master
	UpdatePartitionResp = proto.UpdateMetaPartitionResponse
	// Client -> MetaNode
	ExtentsTruncateReq = proto.TruncateRequest

	// Client -> MetaNode
	EvictInodeReq = proto.EvictInodeRequest
	// Client -> MetaNode
	BatchEvictInodeReq = proto.BatchEvictInodeRequest
	// Client -> MetaNode
	SetattrRequest = proto.SetAttrRequest
	// Client -> MetaNode
	GetAppliedIDReq   = proto.GetAppliedIDRequest
	GetSnapshotCrcReq = proto.GetSnapshotCrcRequest

	// Client -> MetaNode lookup
	LookupDeletedDentryReq = proto.LookupDeletedDentryRequest
	// Client -> MetaNode recover a deleted Dentry request
	RecoverDeletedDentryReq      = proto.RecoverDeletedDentryRequest
	BatchRecoverDeletedDentryReq = proto.BatchRecoverDeletedDentryRequest
	RecoverDeletedInodeReq       = proto.RecoverDeletedInodeRequest
	BatchRecoverDeletedInodeReq  = proto.BatchRecoverDeletedInodeRequest
	CleanDeletedDentryReq        = proto.CleanDeletedDentryRequest
	BatchCleanDeletedDentryReq   = proto.BatchCleanDeletedDentryRequest
	CleanDeletedInodeReq         = proto.CleanDeletedInodeRequest
	BatchCleanDeletedInodeReq    = proto.BatchCleanDeletedInodeRequest
	BatchCleanDeletedInodeResp   = proto.BatchCleanDeletedInodeResponse
	GetDeletedInodeReq           = proto.GetDeletedInodeRequest
	GetDeletedInodeResp          = proto.GetDeletedInodeResponse
	BatchGetDeletedInodeReq      = proto.BatchGetDeletedInodeRequest
	BatchGetDeletedInodeResp     = proto.BatchGetDeletedInodeResponse
	ReadDeletedDirReq            = proto.ReadDeletedDirRequest
	ReadDeletedDirResp           = proto.ReadDeletedDirResponse
	CleanExpiredInodeReq         = proto.CleanExpiredInodeRequest
	CleanExpiredDentryReq        = proto.CleanExpiredDentryRequest
	StatDeletedFileReq           = proto.StatDeletedFileInfoRequest
	StatDeletedFileResp          = proto.StatDeletedFileInfoResponse
)

const (
	opFSMCreateInode uint32 = iota
	opFSMUnlinkInode
	opFSMCreateDentry
	opFSMDeleteDentry
	opFSMDeletePartition
	opFSMUpdatePartition
	opFSMDecommissionPartition
	opFSMExtentsAdd
	opFSMStoreTick
	startStoreTick
	stopStoreTick
	opFSMUpdateDentry
	opFSMExtentTruncate
	opFSMCreateLinkInode
	opFSMEvictInode
	opFSMInternalDeleteInode
	opFSMSetAttr
	opFSMInternalDelExtentFile
	opFSMInternalDelExtentCursor
	opExtentFileSnapshot
	opFSMSetXAttr
	opFSMRemoveXAttr
	opFSMCreateMultipart
	opFSMRemoveMultipart
	opFSMAppendMultipart
	opFSMSyncCursor

	//supplement action
	opFSMInternalDeleteInodeBatch
	opFSMDeleteDentryBatch
	opFSMUnlinkInodeBatch
	opFSMEvictInodeBatch

	opFSMCursorReset

	opFSMExtentsInsert
	// snapshotBatchCreate
	opFSMBatchCreate
	opFSMSnapShotCrc

	opFSMCreateDeletedInode
	opFSMCreateDeletedDentry
	opFSMRecoverDeletedDentry
	opFSMBatchRecoverDeletedDentry
	opFSMRecoverDeletedInode
	opFSMBatchRecoverDeletedInode
	opFSMCleanDeletedDentry
	opFSMBatchCleanDeletedDentry
	opFSMCleanDeletedInode
	opFSMBatchCleanDeletedInode
	opFSMInternalCleanDeletedInode
	opFSMCleanExpiredDentry
	opFSMCleanExpiredInode
	opFSMExtentDelSync
	opSnapSyncExtent
	opFSMExtentMerge
	resetStoreTick
	opFSMExtentDelSyncV2
	opFSMMetaAddVirtualMP //deprecated
	opFSMSynVirtualMPs //deprecated

	opFSMSyncMetaConf

	opFSMMetaRaftAddVirtualMP //deprecated
)

var (
	exporterKey string
)

var (
	ErrNoLeader    = errors.New("no leader")
	ErrNotALeader  = errors.New("not a leader")
	ErrSnapShotEOF = errors.New("snapshot eof")
)

// Default configuration
const (
	defaultMetadataDir                    = "metadataDir"
	defaultRaftDir                        = "raftDir"
	defaultAuthTimeout                    = 5 // seconds
	defaultMaxMetaPartitionInodeID uint64 = 1<<63 - 1
	defaultDiskReservedSpace              = 5 * unit.GB
	metaDataFlockFile                     = ".MetaDataFlock"
)

// Configuration keys
const (
	cfgLocalIP           = "localIP"
	cfgListen            = "listen"
	cfgMetadataDir       = "metadataDir"
	cfgRaftDir           = "raftDir"
	cfgMasterAddrs       = "masterAddrs" // will be deprecated
	cfgRaftHeartbeatPort = "raftHeartbeatPort"
	cfgRaftReplicaPort   = "raftReplicaPort"
	cfgDeleteBatchCount  = "deleteBatchCount"
	cfgTotalMem          = "totalMem"
	cfgZoneName          = "zoneName"
	cfgTickIntervalMs    = "tickIntervalMs"
	cfgRocksDirs         = "rocksDirs"
	cfgProfPort          = "prof"
	cfgDiskReservedSpace = "diskReservedSpace"

	metaNodeDeleteBatchCountKey = "batchCount"
	trashEnableKey              = "trashEnable"
)

const (
	// interval of persisting in-memory data
	intervalToPersistData           = time.Minute * 5
	intervalToSyncCursor            = time.Minute * 1
	intervalToUpdateAllVolsConf     = time.Minute * 5
	intervalToUpdateVolTrashExpires = time.Minute * 1
	defSyncRocksDbWalLog            = time.Minute * 30
	intervalSyncRocksDbWalLog       = time.Second * 2
	defMinSyncRockDbWalLog          = time.Minute * 2
	defRaftLogCap                   = 4
	defRaftLogSize                  = unit.MB * 8
	defIntervalToCleanTrash         = time.Minute * 5

	intervalFailOverLeader          = time.Second
	defTryFailOverCnt               = 3
	defParallelFailOverCnt          = 10

	defCleanTrashItemMaxTotalCountEachTime    = 100 * 10000 //100W
	defCleanTrashItemMaxDurationEachTime      = 5           //min

	defBitMapAllocatorMaxUsedFactorForAvailable = 0.9
	defBitMapAllocatorMinFreeFactorForAvailable = 0.4

	defDumpSnapPreAllocatedMemSize = 4 * unit.MB
	Uint32Size                     = int(unsafe.Sizeof(uint32(0)))
	Uint64Size                     = int(unsafe.Sizeof(uint64(0)))
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

const (
	mpResetInoLimited = 1000
	mpResetInoStep    = 1000
)

const (
	RocksDBVersion         = proto.RocksDBVersion
	Version3_3_0           = proto.Version_3_3_0
	BitMapAllocatorVersion = proto.BitMapAllocator
	Version4_0_0           = proto.Version_4_0_0
	MetaNodeLatestVersion  = proto.BaseVersion
)

const (
	maximumApplyIdDifference = 1000
)

const (
	RaftHangTimeOut          = 60
	ProxyTryToLeaderRetryCnt = 1
)

type CursorResetMode int

const (
	SubCursor CursorResetMode = iota
	AddCursor
	InValidCursorType
)

func (mode CursorResetMode) String() string {
	switch mode {
	case SubCursor:
		return "sub"
	case AddCursor:
		return "add"
	default:
		return "unknown"
	}
}

func ParseCursorResetMode(typeStr string) (CursorResetMode, error) {
	switch typeStr {
	case "0", "sub", "Sub", "SUB":
		return SubCursor, nil
	case "1", "add", "Add", "ADD":
		return AddCursor, nil
	default:
		return InValidCursorType, fmt.Errorf("error cursor reset mode:%s", typeStr)
	}
}

type SnapshotVersion byte

const (
	BaseSnapshotV = iota
	BatchSnapshotV1 //rocksdb
	BatchSnapshotV2 //mp reuse, bitmap allocator
	LatestSnapV = BatchSnapshotV2 //change with max snap version
)

const (
	mpStopChOpenState = iota
	mpStopChStoppedState
)

type MetaRaftApplyResult struct {
	Status uint8
	Msg    string
}
