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

// ============================================================================
// Type Aliases - Request/Response Structures
// ============================================================================

// Master to MetaNode communication
type (
	CreateMetaRangeReq  = proto.CreateMetaPartitionRequest
	CreateMetaRangeResp = proto.CreateMetaPartitionResponse
	UpdatePartitionReq  = proto.UpdateMetaPartitionRequest
	UpdatePartitionResp = proto.UpdateMetaPartitionResponse
	SetFreezeReq        = proto.FreezeMetaPartitionRequest
)

// Client to MetaNode communication - Inode operations
type (
	CreateInoReq           = proto.CreateInodeRequest
	CreateInoResp          = proto.CreateInodeResponse
	LinkInodeReq           = proto.LinkInodeRequest
	LinkInodeResp          = proto.LinkInodeResponse
	UnlinkInoReq           = proto.UnlinkInodeRequest
	UnlinkInoResp          = proto.UnlinkInodeResponse
	BatchUnlinkInoReq      = proto.BatchUnlinkInodeRequest
	BatchUnlinkInoResp     = proto.BatchUnlinkInodeResponse
	EvictInodeReq          = proto.EvictInodeRequest
	BatchEvictInodeReq     = proto.BatchEvictInodeRequest
	SetattrRequest         = proto.SetAttrRequest
	InodeGetReq            = proto.InodeGetRequest
	InodeGetSplitReq       = proto.InodeGetSplitRequest
	InodeGetReqBatch       = proto.BatchInodeGetRequest
	GetUniqIDResp          = proto.GetUniqIDResponse
	UpdateInodeMetaRequest = proto.UpdateInodeMetaRequest
)

// Client to MetaNode communication - Dentry operations
type (
	CreateDentryReq       = proto.CreateDentryRequest
	DeleteDentryReq       = proto.DeleteDentryRequest
	DeleteDentryResp      = proto.DeleteDentryResponse
	BatchDeleteDentryReq  = proto.BatchDeleteDentryRequest
	BatchDeleteDentryResp = proto.BatchDeleteDentryResponse
	UpdateDentryReq       = proto.UpdateDentryRequest
	UpdateDentryResp      = proto.UpdateDentryResponse
)

// Client to MetaNode communication - Directory operations
type (
	ReadDirReq       = proto.ReadDirRequest
	ReadDirOnlyReq   = proto.ReadDirOnlyRequest
	ReadDirLimitReq  = proto.ReadDirLimitRequest
	ReadDirResp      = proto.ReadDirResponse
	ReadDirOnlyResp  = proto.ReadDirOnlyResponse
	ReadDirLimitResp = proto.ReadDirLimitResponse
)

// Client to MetaNode communication - Lookup operations
type (
	LookupReq  = proto.LookupRequest
	LookupResp = proto.LookupResponse
)

// Client to MetaNode communication - Extent operations
type (
	ExtentsTruncateReq = proto.TruncateRequest
)

// Hybrid cloud and migration operations
type (
	RenewalForbiddenMigrationRequest     = proto.RenewalForbiddenMigrationRequest
	UpdateExtentKeyAfterMigrationRequest = proto.UpdateExtentKeyAfterMigrationRequest
	DeleteMigrationExtentKeyRequest      = proto.DeleteMigrationExtentKeyRequest
)

// Debug and utility operations
type (
	SetCreateTimeRequest = proto.SetCreateTimeRequest
)

// ============================================================================
// FSM Operation Codes
// ============================================================================

// op code should be fixed, order change will cause raft fsm log apply fail
const (
	// Basic operations
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

	// Batch operations
	opFSMInternalDeleteInodeBatch = 26
	opFSMDeleteDentryBatch        = 27
	opFSMUnlinkInodeBatch         = 28
	opFSMEvictInodeBatch          = 29

	// Extended operations
	opFSMExtentsAddWithCheck = 30
	opFSMUpdateSummaryInfo   = 31
	opFSMUpdateXAttr         = 32
	opFSMObjExtentsAdd       = 33
	opFSMSentToChan          = 36

	// Transaction operations
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

	// Transaction snapshot operations
	opFSMTxSnapshot         = 52
	opFSMTxRbInodeSnapshot  = 53
	opFSMTxRbDentrySnapshot = 54

	// Quota operations
	opFSMCreateInodeQuota      = 55
	opFSMSetInodeQuotaBatch    = 56
	opFSMDeleteInodeQuotaBatch = 57

	// Snapshot and version operations
	opFSMSnapFormatVersion = 58
	opFSMApplyId           = 59
	opFSMTxId              = 60
	opFSMCursor            = 61

	// Uniq checker operations
	opFSMUniqID              = 62
	opFSMUniqIDSnap          = 63
	opFSMUniqCheckerSnap     = 64
	opFSMUniqCheckerEvict    = 65
	opFSMUnlinkInodeOnce     = 66
	opFSMCreateLinkInodeOnce = 67

	// Directory lock operations
	opFSMLockDir = 68

	// Inode access time and metadata operations
	opFSMSyncInodeAccessTime = 69
	opFSMUpdateInodeMeta     = 70

	// Version list and extent operations
	opFSMVerListSnapShot   = 73
	opFSMVersionOp         = 74
	opFSMExtentSplit       = 75
	opFSMSentToChanWithVer = 76

	// Hybrid cloud operations
	opFSMRenewalForbiddenMigration                = 87
	opFSMUpdateExtentKeyAfterMigration            = 88
	opFSMInternalBatchFreeInodeMigrationExtentKey = 89
	opFSMSetInodeCreateTime                       = 90 // for debug
	opFSMSetMigrationExtentKeyDeleteImmediately   = 91

	// Freeze meta partition operations
	opFSMSetFreeze = 92

	// calc meta partition md5 sum
	opFSMCalcMetaPartitionMd5Sum = 93
)

// New inode operation codes
const (
	opFSMBatchSyncInodeATime = 11000
)

// ============================================================================
// Error Definitions
// ============================================================================

var (
	ErrNoLeader   = errors.New("no leader")
	ErrNotALeader = errors.New("not a leader")
)

// ============================================================================
// Default Configuration
// ============================================================================

const (
	defaultMetadataDir = "metadataDir"
	defaultRaftDir     = "raftDir"
	defaultRocksdMode  = "disk"
)

// ============================================================================
// Configuration Keys
// ============================================================================

const (
	// Basic configuration
	cfgLocalIP           = "localIP"
	cfgMetadataDir       = "metadataDir"
	cfgRaftDir           = "raftDir"
	cfgRaftHeartbeatPort = "raftHeartbeatPort"
	cfgRaftReplicaPort   = "raftReplicaPort"
	cfgZoneName          = "zoneName"
	cfgRack              = "rack"

	// Performance configuration
	cfgDeleteBatchCount = "deleteBatchCount"
	cfgTotalMem         = "totalMem"
	cfgMemRatio         = "memRatio"
	cfgTickInterval     = "tickInterval"
	cfgRaftRecvBufSize  = "raftRecvBufSize"
	cfgReadDirIops      = "readDirIops"

	// SMux configuration
	cfgSmuxPortShift     = "smuxPortShift"
	cfgSmuxMaxConn       = "smuxMaxConn"
	cfgSmuxStreamPerConn = "smuxStreamPerConn"
	cfgSmuxMaxBuffer     = "smuxMaxBuffer"

	// Raft configuration
	cfgRetainLogs                = "retainLogs"
	cfgRaftSyncSnapFormatVersion = "raftSyncSnapFormatVersion"

	// Service configuration
	cfgServiceIDKey = "serviceIDKey"

	// GC configuration
	cfgEnableGcTimer          = "enableGcTimer"
	CfgGcRecyclePercent       = "gcRecyclePercent"
	configNameResolveInterval = "nameResolveInterval"

	// QoS configuration
	cfsQosEnable = "qosEnable"

	// RocksDB configuration
	cfgRocksDirs                    = "rocksDirs"
	cfgDiskReservedSpace            = "diskReservedSpace"
	cfgRocksdbWriteBufferSize       = "rocksdbWriteBufferSize"
	cfgRocksdbWriteBufferNum        = "rocksdbWriteBufferNum"
	cfgRocksdbBlockCacheSize        = "rocksdbBlockCacheSize"
	cfgRocksdbMinWriteBufferToMerge = "rocksdbMinWriteBufferToMerge"
	cfgRocksdbMaxSubCompactions     = "rocksdbMaxSubCompactions"
	cfgRocksdbMode                  = "rocksdbMode"
	cfgRocksdbEnableStats           = "rocksdbEnableStats"
	cfgRocksdbKeyNumMax             = "rocksdbKeyNumMax"
	CfgRocksDBDiskUsageThreshold    = "rocksDBDiskUsageThreshold"

	// MetaNode specific configuration
	metaNodeDeleteBatchCountKey = "batchCount"

	// NOTE: metanode rocksdb config
	cfgRocksdbBytesPerSync             = "rocksdbBytesPerSync"             // uint64
	cfgRocksdbParallelism              = "rocksdbParallelism"              // int
	cfgRocksdbMaxBackgroundCompactions = "rocksdbMaxBackgroundCompactions" // int
	cfgRocksdbMaxBackgroundFlushes     = "rocksdbMaxBackgroundFlushes"     // int
	cfgRocksdbSoftCompactionLimit      = "rocksdbSoftCompactionLimit"      // int64
	cfgRocksdbHardCompactionLimit      = "rocksdbHardCompactionLimit"      // int64
	cfsRocksdbPeriodicCompactSecond    = "rocksdbPeriodicCompactSecond"
)

const (
	// Time intervals
	intervalToPersistData = time.Minute * 5
	intervalToSyncCursor  = time.Minute * 1

	// Default values
	defaultDelExtentsCnt               = 100000
	defaultMaxQuotaGoroutine           = 5
	defaultQuotaSwitch                 = true
	DefaultNameResolveInterval         = 1 // minutes
	DefaultRaftNumOfLogsToRetain       = 20000 * 2
	DefaultCreateBlobClientIntervalSec = 30
	defaultSyncInodeAtimeCnt           = 102400
	RaftCommitDiffMax                  = 100
	DefaultGOGCValue                   = 100
	defaultDiskReservedSpace           = 5 * GB
	AccessTimeOffset                   = 52
	defaultRocksdbKeyNumMax            = 2000000000
	defaultPeriodicCompactSec          = 86400
)

// ============================================================================
// Size Constants
// ============================================================================

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

// ============================================================================
// Unused Variables (for linter compliance)
// ============================================================================

// TODO: Remove unused variables by golangci
var (
	_ = opFSMDeletePartition
	_ = opFSMUpdateSummaryInfo
	_ = (*Dentry).getLastestVer
	_ = (*Inode).isEkInRefMap
	_ = (*metaPartition).decommissionPartition
	_ = (*metaPartition).getDentryTree
	_ = (*metaPartition).internalHasInode
	_ = (*TransactionResource).copyGetTxRbInode
)

// ============================================================================
// Partition Constants
// ============================================================================

const DelMetaPartitionHdr = "del_partition_"
