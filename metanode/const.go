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
	"github.com/chubaofs/cfs/proto"
	"github.com/chubaofs/cfs/util/errors"
	"time"
)

const (
	StateStandby uint32 = iota
	StateStart
	StateRunning
	StateShutdown
	StateStopped
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
	// MetaNode -> Client delete Inode response
	UnlinkInoResp = proto.UnlinkInodeResponse
	// Client -> MetaNode create Dentry request struct
	CreateDentryReq = proto.CreateDentryRequest
	// Client -> MetaNode delete Dentry request
	DeleteDentryReq = proto.DeleteDentryRequest
	// MetaNode -> Client delete Dentry response
	DeleteDentryResp = proto.DeleteDentryResponse
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
	// Client -> MetaNOde
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
	cfgMasterAddrs       = "masterAddrs"
	cfgRaftHeartbeatPort = "raftHeartbeatPort"
	cfgRaftReplicaPort   = "raftReplicaPort"
	cfgTotalMem          = "totalMem"
)

const (
	// interval of persisting in-memory data
	intervalToPersistData = time.Minute * 5
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)
