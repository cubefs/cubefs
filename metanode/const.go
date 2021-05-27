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
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
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
	ReadDirLimitReq = proto.ReadDirLimitRequest
	// MetaNode -> Client read dir response
	ReadDirResp      = proto.ReadDirResponse
	ReadDirLimitResp = proto.ReadDirLimitResponse
	ReadDirOnlyReq   = proto.ReadDirOnlyRequest
	// MetaNode -> Client read dir response
	ReadDirOnlyResp = proto.ReadDirOnlyResponse
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

	opFSMExtentsAddWithCheck
	opFSMUpdateXAttr
)

var (
	exporterKey string
)

var (
	ErrNoLeader   = errors.New("no leader")
	ErrNotALeader = errors.New("not a leader")
)

// Default configuration
const (
	defaultMetadataDir = "metadataDir"
	defaultRaftDir     = "raftDir"
	defaultAuthTimeout = 5 // seconds
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
	cfgTickInterval      = "tickInterval"
	cfgSmuxPortShift     = "smuxPortShift"     //int
	cfgSmuxMaxConn       = "smuxMaxConn"       //int
	cfgSmuxStreamPerConn = "smuxStreamPerConn" //int
	cfgSmuxMaxBuffer     = "smuxMaxBuffer"     //int

	metaNodeDeleteBatchCountKey = "batchCount"
)

const (
	// interval of persisting in-memory data
	intervalToPersistData = time.Minute * 5
	intervalToSyncCursor  = time.Minute * 1
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)
