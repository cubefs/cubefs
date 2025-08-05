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
	"time"

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
	ReadDirReq      = proto.ReadDirRequest
	ReadDirOnlyReq  = proto.ReadDirOnlyRequest
	ReadDirLimitReq = proto.ReadDirLimitRequest
	// MetaNode -> Client read dir response
	ReadDirResp      = proto.ReadDirResponse
	ReadDirOnlyResp  = proto.ReadDirOnlyResponse
	ReadDirLimitResp = proto.ReadDirLimitResponse

	// MetaNode -> Client lookup
	LookupReq = proto.LookupRequest
	// Client -> MetaNode lookup
	LookupResp = proto.LookupResponse
	// Client -> MetaNode
	InodeGetReq = proto.InodeGetRequest
	// Tool -> MetaNode
	InodeGetSplitReq = proto.InodeGetSplitRequest
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
	GetUniqIDResp = proto.GetUniqIDResponse

	// Client -> MetaNode
	RenewalForbiddenMigrationRequest = proto.RenewalForbiddenMigrationRequest

	// Client -> MetaNode
	UpdateExtentKeyAfterMigrationRequest = proto.UpdateExtentKeyAfterMigrationRequest

	// Client -> MetaNode, used for debugging
	SetCreateTimeRequest = proto.SetCreateTimeRequest

	DeleteMigrationExtentKeyRequest = proto.DeleteMigrationExtentKeyRequest
	// Client -> MetaNode
	UpdateInodeMetaRequest = proto.UpdateInodeMetaRequest
	// Master -> MetaNode
	SetFreezeReq = proto.FreezeMetaPartitionRequest
)

// op code should be fixed, order change will cause raft fsm log apply fail
const (
	opFSMCreateInode             = 0
	opFSMUnlinkInode             = 1
	opFSMCreateDentry            = 2
	opFSMDeleteDentry            = 3
	opFSMDeletePartition         = 4
	opFSMUpdatePartition         = 5
	opFSMDecommissionPartition   = 6
	opFSMExtentsAdd              = 7
	opFSMStoreTick               = 8
	startStoreTick               = 9
	stopStoreTick                = 10
	opFSMUpdateDentry            = 11
	opFSMExtentTruncate          = 12
	opFSMCreateLinkInode         = 13
	opFSMEvictInode              = 14
	opFSMInternalDeleteInode     = 15
	opFSMSetAttr                 = 16
	opFSMInternalDelExtentFile   = 17
	opFSMInternalDelExtentCursor = 18
	opExtentFileSnapshot         = 19
	opFSMSetXAttr                = 20
	opFSMRemoveXAttr             = 21
	opFSMCreateMultipart         = 22
	opFSMRemoveMultipart         = 23
	opFSMAppendMultipart         = 24
	opFSMSyncCursor              = 25

	// supplement action
	opFSMInternalDeleteInodeBatch = 26
	opFSMDeleteDentryBatch        = 27
	opFSMUnlinkInodeBatch         = 28
	opFSMEvictInodeBatch          = 29

	opFSMExtentsAddWithCheck = 30

	opFSMUpdateSummaryInfo = 31
	opFSMUpdateXAttr       = 32
	opFSMObjExtentsAdd     = 33
	// opFSMExtentsDel
	opFSMExtentsEmpty = 34

	opFSMClearInodeCache = 35
	opFSMSentToChan      = 36

	// transaction
	opFSMSyncTxID           = 37
	opFSMTxCreateInode      = 38
	opFSMTxCreateInodeQuota = 39
	opFSMTxCreateDentry     = 40
	opFSMTxSetState         = 41
	opFSMTxCommit           = 42
	opFSMTxCommitRM         = 43
	opFSMTxRollbackRM       = 44
	opFSMTxRollback         = 45
	opFSMTxInit             = 46
	opFSMTxDelete           = 47
	opFSMTxDeleteDentry     = 48
	opFSMTxUnlinkInode      = 49
	opFSMTxUpdateDentry     = 50
	opFSMTxCreateLinkInode  = 51
	// transaction snapshot
	opFSMTxSnapshot         = 52
	opFSMTxRbInodeSnapshot  = 53
	opFSMTxRbDentrySnapshot = 54

	// quota
	opFSMCreateInodeQuota      = 55
	opFSMSetInodeQuotaBatch    = 56
	opFSMDeleteInodeQuotaBatch = 57

	opFSMSnapFormatVersion = 58
	opFSMApplyId           = 59
	opFSMTxId              = 60
	opFSMCursor            = 61

	// uniq checker
	opFSMUniqID              = 62
	opFSMUniqIDSnap          = 63
	opFSMUniqCheckerSnap     = 64
	opFSMUniqCheckerEvict    = 65
	opFSMUnlinkInodeOnce     = 66
	opFSMCreateLinkInodeOnce = 67
	// dir lock
	opFSMLockDir = 68

	opFSMSyncInodeAccessTime = 69
	opFSMUpdateInodeMeta     = 70

	opFSMVerListSnapShot   = 73
	opFSMVersionOp         = 74
	opFSMExtentSplit       = 75
	opFSMSentToChanWithVer = 76

	// hybrid cloud
	opFSMRenewalForbiddenMigration                = 87
	opFSMUpdateExtentKeyAfterMigration            = 88
	opFSMInternalBatchFreeInodeMigrationExtentKey = 89
	opFSMSetInodeCreateTime                       = 90 // for debug
	opFSMSetMigrationExtentKeyDeleteImmediately   = 91

	// freeze meta partition
	opFSMSetFreeze = 92
)

// new inode opCode
const (
	opFSMBatchSyncInodeATime = 11000
)

var (
	ErrNoLeader   = errors.New("no leader")
	ErrNotALeader = errors.New("not a leader")
)

// Default configuration
const (
	defaultMetadataDir = "metadataDir"
	defaultRaftDir     = "raftDir"
)

// Configuration keys
const (
	cfgLocalIP                   = "localIP"
	cfgMetadataDir               = "metadataDir"
	cfgRaftDir                   = "raftDir"
	cfgRaftHeartbeatPort         = "raftHeartbeatPort"
	cfgRaftReplicaPort           = "raftReplicaPort"
	cfgDeleteBatchCount          = "deleteBatchCount"
	cfgTotalMem                  = "totalMem"
	cfgMemRatio                  = "memRatio"
	cfgZoneName                  = "zoneName"
	cfgTickInterval              = "tickInterval"
	cfgRaftRecvBufSize           = "raftRecvBufSize"
	cfgSmuxPortShift             = "smuxPortShift"             // int
	cfgSmuxMaxConn               = "smuxMaxConn"               // int
	cfgSmuxStreamPerConn         = "smuxStreamPerConn"         // int
	cfgSmuxMaxBuffer             = "smuxMaxBuffer"             // int
	cfgRetainLogs                = "retainLogs"                // string, raft RetainLogs
	cfgRaftSyncSnapFormatVersion = "raftSyncSnapFormatVersion" // int, format version of snapshot that raft leader sent to follower
	cfgServiceIDKey              = "serviceIDKey"
	cfgEnableGcTimer             = "enableGcTimer" // bool
	CfgGcRecyclePercent          = "gcRecyclePercent"

	metaNodeDeleteBatchCountKey = "batchCount"
	configNameResolveInterval   = "nameResolveInterval" // int
)

const (
	// interval of persisting in-memory data
	intervalToPersistData = time.Minute * 5
	intervalToSyncCursor  = time.Minute * 1

	defaultDelExtentsCnt               = 100000
	defaultMaxQuotaGoroutine           = 5
	defaultQuotaSwitch                 = true
	DefaultNameResolveInterval         = 1 // minutes
	DefaultRaftNumOfLogsToRetain       = 20000 * 2
	DefaultCreateBlobClientIntervalSec = 30
	defaultSyncInodeAtimeCnt           = 102400
	RaftCommitDiffMax                  = 100
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

// TODO: to remove unused by golangci
var (
	_ = opFSMDeletePartition
	_ = opFSMUpdateSummaryInfo
	_ = (*Dentry).getLastestVer
	_ = (*Inode).isEkInRefMap
	_ = (*metaPartition).decommissionPartition
	_ = (*metaPartition).getDentryTree
	_ = (*metaPartition).internalHasInode
	_ = (*metaPartition).fsmDelVerExtents
	_ = (*TransactionResource).copyGetTxRbInode
)

const DelMetaPartitionHdr = "del_partition_"
