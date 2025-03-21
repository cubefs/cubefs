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

package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/log"
)

var (
	GRequestID = time.Now().Unix()<<32 + 1
	Buffers    *buf.BufferPool
)

// GenerateRequestID generates the request ID.
func GenerateRequestID() int64 {
	return atomic.AddInt64(&GRequestID, 1)
}

const (
	AddrSplit        = "/"
	FollowerReadFlag = 'F'
)

// Operations
const (
	ProtoMagic           uint8 = 0xFF
	OpInitResultCode     uint8 = 0x00
	OpCreateExtent       uint8 = 0x01
	OpMarkDelete         uint8 = 0x02
	OpWrite              uint8 = 0x03
	OpRead               uint8 = 0x04
	OpStreamRead         uint8 = 0x05
	OpStreamFollowerRead uint8 = 0x06
	OpGetAllWatermarks   uint8 = 0x07

	OpNotifyReplicasToRepair         uint8 = 0x08
	OpExtentRepairRead               uint8 = 0x09
	OpBroadcastMinAppliedID          uint8 = 0x0A
	OpRandomWrite                    uint8 = 0x0F
	OpGetAppliedId                   uint8 = 0x10
	OpGetPartitionSize               uint8 = 0x11
	OpSyncRandomWrite                uint8 = 0x12
	OpSyncWrite                      uint8 = 0x13
	OpReadTinyDeleteRecord           uint8 = 0x14
	OpTinyExtentRepairRead           uint8 = 0x15
	OpGetMaxExtentIDAndPartitionSize uint8 = 0x16
	OpSnapshotExtentRepairRead       uint8 = 0x17
	OpSnapshotExtentRepairRsp        uint8 = 0x18
	// 0x19 is occupied by OpMetaUpdateExtentKeyAfterMigration

	// Operations: Client -> MetaNode.
	OpMetaCreateInode   uint8 = 0x20
	OpMetaUnlinkInode   uint8 = 0x21
	OpMetaCreateDentry  uint8 = 0x22
	OpMetaDeleteDentry  uint8 = 0x23
	OpMetaOpen          uint8 = 0x24
	OpMetaLookup        uint8 = 0x25
	OpMetaReadDir       uint8 = 0x26
	OpMetaInodeGet      uint8 = 0x27
	OpMetaBatchInodeGet uint8 = 0x28
	OpMetaExtentsAdd    uint8 = 0x29
	OpMetaExtentsDel    uint8 = 0x2A
	OpMetaExtentsList   uint8 = 0x2B
	OpMetaUpdateDentry  uint8 = 0x2C
	OpMetaTruncate      uint8 = 0x2D
	OpMetaLinkInode     uint8 = 0x2E
	OpMetaEvictInode    uint8 = 0x2F
	OpMetaSetattr       uint8 = 0x30
	OpMetaReleaseOpen   uint8 = 0x31

	// Operations: MetaNode Leader -> MetaNode Follower
	OpMetaFreeInodesOnRaftFollower uint8 = 0x32
	OpMetaDeleteInode              uint8 = 0x33 // delete specified inode immediately and do not remove data.
	OpMetaBatchExtentsAdd          uint8 = 0x34 // for extents batch attachment
	OpMetaSetXAttr                 uint8 = 0x35
	OpMetaGetXAttr                 uint8 = 0x36
	OpMetaRemoveXAttr              uint8 = 0x37
	OpMetaListXAttr                uint8 = 0x38
	OpMetaBatchGetXAttr            uint8 = 0x39
	OpMetaExtentAddWithCheck       uint8 = 0x3A // Append extent key with discard extents check
	OpMetaReadDirLimit             uint8 = 0x3D
	OpMetaLockDir                  uint8 = 0x3E

	// Operations: Master -> MetaNode
	OpCreateMetaPartition           uint8 = 0x40
	OpMetaNodeHeartbeat             uint8 = 0x41
	OpDeleteMetaPartition           uint8 = 0x42
	OpUpdateMetaPartition           uint8 = 0x43
	OpLoadMetaPartition             uint8 = 0x44
	OpDecommissionMetaPartition     uint8 = 0x45
	OpAddMetaPartitionRaftMember    uint8 = 0x46
	OpRemoveMetaPartitionRaftMember uint8 = 0x47
	OpMetaPartitionTryToLeader      uint8 = 0x48

	// Quota
	OpMetaBatchSetInodeQuota    uint8 = 0x50
	OpMetaBatchDeleteInodeQuota uint8 = 0x51
	OpMetaGetInodeQuota         uint8 = 0x52
	OpQuotaCreateInode          uint8 = 0x53
	OpQuotaCreateDentry         uint8 = 0x54

	// Operations: Master -> LcNode
	OpLcNodeHeartbeat      uint8 = 0x55
	OpLcNodeScan           uint8 = 0x56
	OpLcNodeSnapshotVerDel uint8 = 0x5B

	// backUp
	OpBatchLockNormalExtent   uint8 = 0x57
	OpBatchUnlockNormalExtent uint8 = 0x58
	OpBackupRead              uint8 = 0x59
	OpBackupWrite             uint8 = 0x5A // used by fsck only

	// Operations: Master -> DataNode
	OpCreateDataPartition           uint8 = 0x60
	OpDeleteDataPartition           uint8 = 0x61
	OpLoadDataPartition             uint8 = 0x62
	OpDataNodeHeartbeat             uint8 = 0x63
	OpReplicateFile                 uint8 = 0x64
	OpDeleteFile                    uint8 = 0x65
	OpDecommissionDataPartition     uint8 = 0x66
	OpAddDataPartitionRaftMember    uint8 = 0x67
	OpRemoveDataPartitionRaftMember uint8 = 0x68
	OpDataPartitionTryToLeader      uint8 = 0x69
	OpQos                           uint8 = 0x6A
	OpStopDataPartitionRepair       uint8 = 0x6B
	OpRecoverDataReplicaMeta        uint8 = 0x6C
	OpRecoverBackupDataReplica      uint8 = 0x6D
	OpRecoverBadDisk                uint8 = 0x6E
	OpQueryBadDiskRecoverProgress   uint8 = 0x6F
	OpDeleteBackupDirectories       uint8 = 0x80

	// Operations: MultipartInfo
	OpCreateMultipart  uint8 = 0x70
	OpGetMultipart     uint8 = 0x71
	OpAddMultipartPart uint8 = 0x72
	OpRemoveMultipart  uint8 = 0x73
	OpListMultiparts   uint8 = 0x74

	OpBatchDeleteExtent   uint8 = 0x75 // SDK to MetaNode
	OpGcBatchDeleteExtent uint8 = 0x76 // SDK to MetaNode
	OpGetExpiredMultipart uint8 = 0x77

	// Operations: MetaNode Leader -> MetaNode Follower
	OpMetaBatchDeleteInode  uint8 = 0x90
	OpMetaBatchDeleteDentry uint8 = 0x91
	OpMetaBatchUnlinkInode  uint8 = 0x92
	OpMetaBatchEvictInode   uint8 = 0x93

	// Transaction Operations: Client -> MetaNode.
	OpMetaTxCreate       uint8 = 0xA0
	OpMetaTxCreateInode  uint8 = 0xA1
	OpMetaTxUnlinkInode  uint8 = 0xA2
	OpMetaTxCreateDentry uint8 = 0xA3
	OpTxCommit           uint8 = 0xA4
	OpTxRollback         uint8 = 0xA5
	OpTxCommitRM         uint8 = 0xA6
	OpTxRollbackRM       uint8 = 0xA7
	OpMetaTxDeleteDentry uint8 = 0xA8
	OpMetaTxUpdateDentry uint8 = 0xA9
	OpMetaTxLinkInode    uint8 = 0xAA
	OpMetaTxGet          uint8 = 0xAB

	// Operations: Client -> MetaNode.
	OpMetaGetUniqID    uint8 = 0xAC
	OpMetaGetAppliedID uint8 = 0xAD

	// Multi version snapshot
	OpRandomWriteAppend     uint8 = 0xB1
	OpSyncRandomWriteAppend uint8 = 0xB2
	OpRandomWriteVer        uint8 = 0xB3
	OpSyncRandomWriteVer    uint8 = 0xB4
	OpSyncRandomWriteVerRsp uint8 = 0xB5
	OpTryWriteAppend        uint8 = 0xB6
	OpSyncTryWriteAppend    uint8 = 0xB7
	OpVersionOp             uint8 = 0xB8

	// Commons
	OpNoSpaceErr uint8 = 0xEE
	OpForbidErr  uint8 = 0xEF
	OpDirQuota   uint8 = 0xF1

	// Commons

	OpConflictExtentsErr uint8 = 0xF2
	OpIntraGroupNetErr   uint8 = 0xF3
	OpArgMismatchErr     uint8 = 0xF4
	OpNotExistErr        uint8 = 0xF5
	OpDiskNoSpaceErr     uint8 = 0xF6
	OpDiskErr            uint8 = 0xF7
	OpErr                uint8 = 0xF8
	OpAgain              uint8 = 0xF9
	OpExistErr           uint8 = 0xFA
	OpInodeFullErr       uint8 = 0xFB
	OpTryOtherAddr       uint8 = 0xFC
	OpNotPerm            uint8 = 0xFD
	OpNotEmpty           uint8 = 0xFE
	OpOk                 uint8 = 0xF0
	OpAgainVerionList    uint8 = 0xEF

	OpPing                  uint8 = 0xFF
	OpMetaUpdateXAttr       uint8 = 0x3B
	OpMetaReadDirOnly       uint8 = 0x3C
	OpUploadPartConflictErr uint8 = 0x3D

	// ebs obj meta
	OpMetaObjExtentAdd       uint8 = 0xDD
	OpMetaObjExtentsList     uint8 = 0xDE
	OpMetaExtentsEmpty       uint8 = 0xDF
	OpMetaBatchObjExtentsAdd uint8 = 0xD0
	OpMetaClearInodeCache    uint8 = 0xD1

	OpMetaBatchSetXAttr uint8 = 0xD2
	OpMetaGetAllXAttr   uint8 = 0xD3

	// transaction error

	OpTxInodeInfoNotExistErr  uint8 = 0xE0
	OpTxConflictErr           uint8 = 0xE1
	OpTxDentryInfoNotExistErr uint8 = 0xE2
	OpTxRbInodeNotExistErr    uint8 = 0xE3
	OpTxRbDentryNotExistErr   uint8 = 0xE4
	OpTxInfoNotExistErr       uint8 = 0xE5
	OpTxInternalErr           uint8 = 0xE6
	OpTxCommitItemErr         uint8 = 0xE7
	OpTxRollbackItemErr       uint8 = 0xE8
	OpTxRollbackUnknownRbType uint8 = 0xE9
	OpTxTimeoutErr            uint8 = 0xEA
	OpTxSetStateErr           uint8 = 0xEB
	OpTxCommitErr             uint8 = 0xEC
	OpTxRollbackErr           uint8 = 0xED
	OpTxUnknownOp             uint8 = 0xEE
	// multiVersion to dp/mp
	OpVersionOperation uint8 = 0xD5
	OpSplitMarkDelete  uint8 = 0xD6
	OpTryOtherExtent   uint8 = 0xD7

	// io speed limit
	OpLimitedIoErr          uint8 = 0xB1
	OpReadRepairExtentAgain uint8 = 0xEF
	OpStoreClosed           uint8 = 0xB2

	// get access time
	OpMetaInodeAccessTimeGet uint8 = 0xB2

	OpReachMaxExtentsErr uint8 = 0xB3

	// hybirdCloud
	OpMismatchStorageClass              uint8 = 0x82
	OpMetaRenewalForbiddenMigration     uint8 = 0x83
	OpMetaUpdateExtentKeyAfterMigration uint8 = 0x84
	OpDeleteMigrationExtentKey          uint8 = 0x85
	OpLeaseOccupiedByOthers             uint8 = 0x86
	OpLeaseGenerationNotMatch           uint8 = 0x87
	OpWriteOpOfProtoVerForbidden        uint8 = 0x88
)

const (
	WriteDeadlineTime = 5
	ReadDeadlineTime  = 5

	SyncSendTaskDeadlineTime                  = 30
	NoReadDeadlineTime                        = -1
	BatchDeleteExtentReadDeadLineTime         = 120
	GetAllWatermarksDeadLineTime              = 60
	DefaultClusterLoadFactor          float64 = 10
	MultiVersionFlag                          = 0x80
	VersionListFlag                           = 0x40
	PacketProtocolVersionFlag                 = 0x10
)

// multi version operation
const (
	CreateVersion        = 1
	DeleteVersion        = 2
	CreateVersionPrepare = 3
	CreateVersionCommit  = 4
	SyncBatchVersionList = 5
)

// stage of version building
const (
	VersionInit            = 0
	VersionWorking         = 1
	VersionWorkingTimeOut  = 2
	VersionWorkingAbnormal = 3
	VersionWorkingFinished = 4
)

// status of version
const (
	VersionNormal         = 1
	VersionDeleted        = 2
	VersionDeleting       = 3
	VersionDeleteAbnormal = 4
	VersionPrepare        = 5
)

const (
	TinyExtentType   = 0
	NormalExtentType = 1
)

const (
	NormalCreateDataPartition         = 0
	DecommissionedCreateDataPartition = 1
)

const (
	PacketProtoVersion0 = 0 // before v3.5.0
	PacketProtoVersion1 = 1 // from v3.5.0
)

// Packet defines the packet structure.
type Packet struct {
	Magic              uint8
	ExtentType         uint8 // the highest bit be set while rsp to client if version not consistent then Verseq be valid
	Opcode             uint8
	ResultCode         uint8
	RemainingFollowers uint8
	CRC                uint32
	Size               uint32
	ArgLen             uint32
	KernelOffset       uint64
	PartitionID        uint64
	ExtentID           uint64
	ExtentOffset       int64
	ReqID              int64
	Arg                []byte // for create or append ops, the data contains the address
	Data               []byte
	StartT             int64
	mesg               string
	HasPrepare         bool
	VerSeq             uint64 // only used in mod request to datanode

	// protocol version of packet
	// version-0: before v3.4
	// version-1: from v3.4
	ProtoVersion uint32

	VerList  []*VolVersionInfo
	noPrefix bool
}

func IsTinyExtentType(extentType uint8) bool {
	return extentType&NormalExtentType != NormalExtentType
}

func IsNormalExtentType(extentType uint8) bool {
	return extentType&NormalExtentType == NormalExtentType
}

func GetMsgByCode(code uint8) string {
	pkt := NewPacket()
	pkt.ResultCode = code
	return pkt.GetResultMsg()
}

// NewPacket returns a new packet.
func NewPacket() *Packet {
	p := new(Packet)
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()
	return p
}

// NewPacketReqID returns a new packet with ReqID assigned.
func NewPacketReqID() *Packet {
	p := NewPacket()
	p.ReqID = GenerateRequestID()
	return p
}

func (p *Packet) IsRandomWrite() bool {
	switch p.Opcode {
	case OpRandomWrite,
		OpSyncRandomWrite,
		OpSyncRandomWriteVer,
		OpRandomWriteVer:
		return true
	default:
		return false
	}
}

func (p *Packet) GetCopy() *Packet {
	newPacket := NewPacket()
	newPacket.ReqID = p.ReqID
	newPacket.Opcode = p.Opcode
	newPacket.PartitionID = p.PartitionID

	newPacket.Data = make([]byte, p.Size)
	copy(newPacket.Data[:p.Size], p.Data)

	newPacket.Size = p.Size
	return newPacket
}

func (p *Packet) String() string {
	return fmt.Sprintf("ReqID(%v)Op(%v)PartitionID(%v)ResultCode(%v)ExID(%v)ExtOffset(%v)KernelOff(%v)Type(%v)VerSeq(%v)ProtoVer(%v)Size(%v)FollowerRead(%v)",
		p.ReqID, p.GetOpMsg(), p.PartitionID, p.GetResultMsg(), p.ExtentID, p.ExtentOffset, p.KernelOffset, p.ExtentType, p.VerSeq, p.ProtoVersion, p.Size, p.IsFollowerReadMetaPkt())
}

func (p *Packet) IsFollowerReadMetaPkt() bool {
	if p.ArgLen == 1 && p.Arg[0] == FollowerReadFlag {
		return true
	}
	return false
}

// GetStoreType returns the store type.
func (p *Packet) GetStoreType() (m string) {
	if IsNormalExtentType(p.ExtentType) {
		return "NormalExtent"
	} else if IsTinyExtentType(p.ExtentType) {
		return "TinyExtent"
	} else {
		return "Unknown"
	}
}

func (p *Packet) GetOpMsgWithReqAndResult() (m string) {
	return fmt.Sprintf("Req(%v)_(%v)_Result(%v)", p.ReqID, p.GetOpMsg(), p.GetResultMsg())
}

// GetOpMsg returns the operation type.
func (p *Packet) GetOpMsg() (m string) {
	switch p.Opcode {
	case OpCreateExtent:
		m = "OpCreateExtent"
	case OpMarkDelete:
		m = "OpMarkDelete"
	case OpSplitMarkDelete:
		m = "OpMarkDelete"
	case OpWrite:
		m = "OpWrite"
	case OpTryWriteAppend:
		m = "OpTryWriteAppend"
	case OpRandomWrite:
		m = "OpRandomWrite"
	case OpRandomWriteAppend:
		m = "OpRandomWriteAppend"
	case OpRandomWriteVer:
		m = "OpRandomWriteVer"
	case OpRead:
		m = "Read"
	case OpStreamRead:
		m = "OpStreamRead"
	case OpStreamFollowerRead:
		m = "OpStreamFollowerRead"
	case OpGetAllWatermarks:
		m = "OpGetAllWatermarks"
	case OpNotifyReplicasToRepair:
		m = "OpNotifyReplicasToRepair"
	case OpExtentRepairRead:
		m = "OpExtentRepairRead"
	case OpConflictExtentsErr:
		m = "ConflictExtentsErr"
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr"
	case OpMetaCreateInode:
		m = "OpMetaCreateInode"
	case OpQuotaCreateInode:
		m = "OpQuotaCreateInode"
	case OpMetaUnlinkInode:
		m = "OpMetaUnlinkInode"
	case OpMetaBatchUnlinkInode:
		m = "OpMetaBatchUnlinkInode"
	case OpMetaCreateDentry:
		m = "OpMetaCreateDentry"
	case OpQuotaCreateDentry:
		m = "OpQuotaCreateDentry"
	case OpMetaDeleteDentry:
		m = "OpMetaDeleteDentry"
	case OpMetaBatchDeleteDentry:
		m = "OpMetaBatchDeleteDentry"
	case OpMetaOpen:
		m = "OpMetaOpen"
	case OpMetaReleaseOpen:
		m = "OpMetaReleaseOpen"
	case OpMetaLookup:
		m = "OpMetaLookup"
	case OpMetaReadDir:
		m = "OpMetaReadDir"
	case OpMetaReadDirLimit:
		m = "OpMetaReadDirLimit"
	case OpMetaLockDir:
		m = "OpMetaLockDir"
	case OpMetaInodeGet:
		m = "OpMetaInodeGet"
	case OpMetaBatchInodeGet:
		m = "OpMetaBatchInodeGet"
	case OpMetaExtentsAdd:
		m = "OpMetaExtentsAdd"
	case OpMetaExtentAddWithCheck:
		m = "OpMetaExtentAddWithCheck"
	case OpMetaObjExtentAdd:
		m = "OpMetaObjExtentAdd"
	case OpMetaExtentsDel:
		m = "OpMetaExtentsDel"
	case OpMetaExtentsList:
		m = "OpMetaExtentsList"
	case OpMetaObjExtentsList:
		m = "OpMetaObjExtentsList"
	case OpMetaUpdateDentry:
		m = "OpMetaUpdateDentry"
	case OpMetaTruncate:
		m = "OpMetaTruncate"
	case OpMetaLinkInode:
		m = "OpMetaLinkInode"
	case OpMetaEvictInode:
		m = "OpMetaEvictInode"
	case OpMetaBatchEvictInode:
		m = "OpMetaBatchEvictInode"
	case OpMetaSetattr:
		m = "OpMetaSetattr"
	case OpCreateMetaPartition:
		m = "OpCreateMetaPartition"
	case OpMetaNodeHeartbeat:
		m = "OpMetaNodeHeartbeat"
	case OpDeleteMetaPartition:
		m = "OpDeleteMetaPartition"
	case OpUpdateMetaPartition:
		m = "OpUpdateMetaPartition"
	case OpLoadMetaPartition:
		m = "OpLoadMetaPartition"
	case OpDecommissionMetaPartition:
		m = "OpDecommissionMetaPartition"
	case OpCreateDataPartition:
		m = "OpCreateDataPartition"
	case OpDeleteDataPartition:
		m = "OpDeleteDataPartition"
	case OpLoadDataPartition:
		m = "OpLoadDataPartition"
	case OpDecommissionDataPartition:
		m = "OpDecommissionDataPartition"
	case OpDataNodeHeartbeat:
		m = "OpDataNodeHeartbeat"
	case OpReplicateFile:
		m = "OpReplicateFile"
	case OpDeleteFile:
		m = "OpDeleteFile"
	case OpGetAppliedId:
		m = "OpGetAppliedId"
	case OpGetPartitionSize:
		m = "OpGetPartitionSize"
	case OpSyncWrite:
		m = "OpSyncWrite"
	case OpSyncTryWriteAppend:
		m = "OpSyncTryWriteAppend"
	case OpSyncRandomWrite:
		m = "OpSyncRandomWrite"
	case OpSyncRandomWriteVer:
		m = "OpSyncRandomWriteVer"
	case OpSyncRandomWriteAppend:
		m = "OpSyncRandomWriteAppend"
	case OpReadTinyDeleteRecord:
		m = "OpReadTinyDeleteRecord"
	case OpPing:
		m = "OpPing"
	case OpTinyExtentRepairRead:
		m = "OpTinyExtentRepairRead"
	case OpSnapshotExtentRepairRead:
		m = "OpSnapshotExtentRepairRead"
	case OpGetMaxExtentIDAndPartitionSize:
		m = "OpGetMaxExtentIDAndPartitionSize"
	case OpBroadcastMinAppliedID:
		m = "OpBroadcastMinAppliedID"
	case OpRemoveDataPartitionRaftMember:
		m = "OpRemoveDataPartitionRaftMember"
	case OpAddDataPartitionRaftMember:
		m = "OpAddDataPartitionRaftMember"
	case OpAddMetaPartitionRaftMember:
		m = "OpAddMetaPartitionRaftMember"
	case OpRemoveMetaPartitionRaftMember:
		m = "OpRemoveMetaPartitionRaftMember"
	case OpMetaPartitionTryToLeader:
		m = "OpMetaPartitionTryToLeader"
	case OpDataPartitionTryToLeader:
		m = "OpDataPartitionTryToLeader"
	case OpMetaDeleteInode:
		m = "OpMetaDeleteInode"
	case OpMetaBatchDeleteInode:
		m = "OpMetaBatchDeleteInode"
	case OpMetaBatchExtentsAdd:
		m = "OpMetaBatchExtentsAdd"
	case OpMetaBatchObjExtentsAdd:
		m = "OpMetaBatchObjExtentsAdd"
	case OpMetaSetXAttr:
		m = "OpMetaSetXAttr"
	case OpMetaGetXAttr:
		m = "OpMetaGetXAttr"
	case OpMetaRemoveXAttr:
		m = "OpMetaRemoveXAttr"
	case OpMetaListXAttr:
		m = "OpMetaListXAttr"
	case OpMetaBatchGetXAttr:
		m = "OpMetaBatchGetXAttr"
	case OpMetaUpdateXAttr:
		m = "OpMetaUpdateXAttr"
	case OpCreateMultipart:
		m = "OpCreateMultipart"
	case OpGetMultipart:
		m = "OpGetMultipart"
	case OpAddMultipartPart:
		m = "OpAddMultipartPart"
	case OpRemoveMultipart:
		m = "OpRemoveMultipart"
	case OpListMultiparts:
		m = "OpListMultiparts"
	case OpBatchDeleteExtent:
		m = "OpBatchDeleteExtent"
	case OpGcBatchDeleteExtent:
		m = "OpGcBatchDeleteExtent"
	case OpMetaClearInodeCache:
		m = "OpMetaClearInodeCache"
	case OpMetaTxCreateInode:
		m = "OpMetaTxCreateInode"
	case OpMetaTxCreateDentry:
		m = "OpMetaTxCreateDentry"
	case OpTxCommit:
		m = "OpTxCommit"
	case OpMetaTxCreate:
		m = "OpMetaTxCreate"
	case OpTxRollback:
		m = "OpTxRollback"
	case OpTxCommitRM:
		m = "OpTxCommitRM"
	case OpTxRollbackRM:
		m = "OpTxRollbackRM"
	case OpMetaTxDeleteDentry:
		m = "OpMetaTxDeleteDentry"
	case OpMetaTxUnlinkInode:
		m = "OpMetaTxUnlinkInode"
	case OpMetaTxUpdateDentry:
		m = "OpMetaTxUpdateDentry"
	case OpMetaTxLinkInode:
		m = "OpMetaTxLinkInode"
	case OpMetaTxGet:
		m = "OpMetaTxGet"
	case OpMetaGetAppliedID:
		m = "OpMetaGetAppliedId"
	case OpMetaBatchSetInodeQuota:
		m = "OpMetaBatchSetInodeQuota"
	case OpMetaBatchDeleteInodeQuota:
		m = "OpMetaBatchDeleteInodeQuota"
	case OpMetaGetInodeQuota:
		m = "OpMetaGetInodeQuota"
	case OpStopDataPartitionRepair:
		m = "OpStopDataPartitionRepair"
	case OpLcNodeHeartbeat:
		m = "OpLcNodeHeartbeat"
	case OpLcNodeScan:
		m = "OpLcNodeScan"
	case OpLcNodeSnapshotVerDel:
		m = "OpLcNodeSnapshotVerDel"
	case OpMetaReadDirOnly:
		m = "OpMetaReadDirOnly"
	case OpBackupRead:
		m = "OpBackupRead"
	case OpBatchLockNormalExtent:
		m = "OpBatchLockNormalExtent"
	case OpBatchUnlockNormalExtent:
		m = "OpBatchUnlockNormalExtent"
	case OpMetaRenewalForbiddenMigration:
		m = "OpMetaRenewalForbiddenMigration"
	case OpMetaUpdateExtentKeyAfterMigration:
		m = "OpMetaUpdateExtentKeyAfterMigration"
	case OpDeleteMigrationExtentKey:
		m = "OpDeleteMigrationExtentKey"
	default:
		m = fmt.Sprintf("op:%v not found", p.Opcode)
	}
	return
}

func GetStatusStr(status uint8) string {
	pkt := &Packet{}
	pkt.ResultCode = status
	return pkt.GetResultMsg()
}

// GetResultMsg returns the result message.
func (p *Packet) GetResultMsg() (m string) {
	if p == nil {
		return ""
	}

	switch p.ResultCode {
	case OpConflictExtentsErr:
		m = "ConflictExtentsErr"
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr"
	case OpDiskNoSpaceErr:
		m = "DiskNoSpaceErr"
	case OpDiskErr:
		m = "DiskErr"
	case OpErr:
		m = "Err: " + string(p.Data)
	case OpAgain:
		m = "Again: " + string(p.Data)
	case OpOk:
		m = "Ok"
	case OpExistErr:
		m = "ExistErr"
	case OpInodeFullErr:
		m = "InodeFullErr"
	case OpArgMismatchErr:
		m = "ArgUnmatchErr"
	case OpNotExistErr:
		m = "NotExistErr"
	case OpTryOtherAddr:
		m = "TryOtherAddr"
	case OpNotPerm:
		m = "NotPerm"
	case OpNotEmpty:
		m = "DirNotEmpty"
	case OpDirQuota:
		m = "OpDirQuota"
	case OpNoSpaceErr:
		m = "NoSpaceErr"
	case OpTxInodeInfoNotExistErr:
		m = "OpTxInodeInfoNotExistErr"
	case OpTxConflictErr:
		m = "TransactionConflict"
	case OpTxDentryInfoNotExistErr:
		m = "OpTxDentryInfoNotExistErr"
	case OpTxRbInodeNotExistErr:
		m = "OpTxRbInodeNotExistEr"
	case OpTxRbDentryNotExistErr:
		m = "OpTxRbDentryNotExistEr"
	case OpTxInfoNotExistErr:
		m = "OpTxInfoNotExistErr"
	case OpTxInternalErr:
		m = "OpTxInternalErr"
	case OpTxCommitItemErr:
		m = "OpTxCommitItemErr"
	case OpTxRollbackItemErr:
		m = "OpTxRollbackItemErr"
	case OpTxRollbackUnknownRbType:
		m = "OpTxRollbackUnknownRbType"
	case OpTxTimeoutErr:
		m = "OpTxTimeoutErr"
	case OpTxSetStateErr:
		m = "OpTxSetStateErr"
	case OpTxCommitErr:
		m = "OpTxCommitErr"
	case OpTxRollbackErr:
		m = "OpTxRollbackErr"
	case OpUploadPartConflictErr:
		m = "OpUploadPartConflictErr"
	case OpForbidErr:
		m = "OpForbidErr"
	case OpLimitedIoErr:
		m = "OpLimitedIoErr"
	case OpStoreClosed:
		return "OpStoreClosed"
	case OpReachMaxExtentsErr:
		return "OpReachMaxExtentsErr"
	case OpMismatchStorageClass:
		m = "OpMismatchStorageClass:" + string(p.Data)
	case OpLeaseOccupiedByOthers:
		m = "LeaseOccupiedByOthers"
	case OpLeaseGenerationNotMatch:
		m = "OpLeaseGenerationNotMatch"
	case OpWriteOpOfProtoVerForbidden:
		m = "OpWriteOpOfProtoVerForbidden"
	default:
		return fmt.Sprintf("Unknown ResultCode(%v)", p.ResultCode)
	}
	return
}

func (p *Packet) GetReqID() int64 {
	return p.ReqID
}

// MarshalHeader marshals the packet header.
func (p *Packet) MarshalHeader(out []byte) {
	out[0] = p.Magic
	out[1] = p.ExtentType
	out[2] = p.Opcode
	out[3] = p.ResultCode
	out[4] = p.RemainingFollowers
	binary.BigEndian.PutUint32(out[5:9], p.CRC)
	binary.BigEndian.PutUint32(out[9:13], p.Size)
	binary.BigEndian.PutUint32(out[13:17], p.ArgLen)
	binary.BigEndian.PutUint64(out[17:25], p.PartitionID)
	binary.BigEndian.PutUint64(out[25:33], p.ExtentID)
	binary.BigEndian.PutUint64(out[33:41], uint64(p.ExtentOffset))
	binary.BigEndian.PutUint64(out[41:49], uint64(p.ReqID))
	binary.BigEndian.PutUint64(out[49:util.PacketHeaderSize], p.KernelOffset)

	curSize := util.PacketHeaderSize
	if p.ExtentType&PacketProtocolVersionFlag > 0 {
		binary.BigEndian.PutUint64(out[curSize:curSize+util.PacketVerSeqFiledLen], p.VerSeq)
		curSize = curSize + util.PacketVerSeqFiledLen

		p.ProtoVersion = PacketProtoVersion1
		binary.BigEndian.PutUint32(out[curSize:curSize+util.PacketProtoVerFiledLen], p.ProtoVersion)
	} else if p.Opcode == OpRandomWriteVer || p.ExtentType&MultiVersionFlag > 0 {
		binary.BigEndian.PutUint64(out[curSize:curSize+util.PacketVerSeqFiledLen], p.VerSeq)
	}
}

func (p *Packet) IsVersionList() bool {
	return p.ExtentType&VersionListFlag == VersionListFlag
}

// UnmarshalHeader unmarshals the packet header.
func (p *Packet) UnmarshalHeader(in []byte) error {
	p.Magic = in[0]
	if p.Magic != ProtoMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Magic)))
	}

	p.ExtentType = in[1]
	p.Opcode = in[2]
	p.ResultCode = in[3]
	p.RemainingFollowers = in[4]
	p.CRC = binary.BigEndian.Uint32(in[5:9])
	p.Size = binary.BigEndian.Uint32(in[9:13])
	p.ArgLen = binary.BigEndian.Uint32(in[13:17])
	p.PartitionID = binary.BigEndian.Uint64(in[17:25])
	p.ExtentID = binary.BigEndian.Uint64(in[25:33])
	p.ExtentOffset = int64(binary.BigEndian.Uint64(in[33:41]))
	p.ReqID = int64(binary.BigEndian.Uint64(in[41:49]))
	p.KernelOffset = binary.BigEndian.Uint64(in[49:util.PacketHeaderSize])

	// header opcode OpRandomWriteVer should not unmarshal here due to header size is const
	// the ver param should read at the higher level directly
	// if p.Opcode ==OpRandomWriteVer {

	return nil
}

func (p *Packet) TryReadExtraFieldsFromConn(c net.Conn) (err error) {
	if p.ExtentType&PacketProtocolVersionFlag > 0 {
		verSeq := make([]byte, 8)
		if _, err = io.ReadFull(c, verSeq); err != nil {
			return
		}
		p.VerSeq = binary.BigEndian.Uint64(verSeq)

		protoVer := make([]byte, 4)
		if _, err = io.ReadFull(c, protoVer); err != nil {
			return
		}
		p.ProtoVersion = binary.BigEndian.Uint32(protoVer)
	} else if p.ExtentType&MultiVersionFlag > 0 {
		ver := make([]byte, 8)
		if _, err = io.ReadFull(c, ver); err != nil {
			return
		}
		p.VerSeq = binary.BigEndian.Uint64(ver)
	}

	return
}

const verInfoCnt = 17

func (p *Packet) MarshalVersionSlice() (data []byte, err error) {
	items := p.VerList
	cnt := len(items)
	buff := bytes.NewBuffer(make([]byte, 0, 2*cnt*verInfoCnt))
	if err := binary.Write(buff, binary.BigEndian, uint16(cnt)); err != nil {
		return nil, err
	}

	for _, v := range items {
		if err := binary.Write(buff, binary.BigEndian, v.Ver); err != nil {
			return nil, err
		}
		if err := binary.Write(buff, binary.BigEndian, v.DelTime); err != nil {
			return nil, err
		}
		if err := binary.Write(buff, binary.BigEndian, v.Status); err != nil {
			return nil, err
		}
	}

	return buff.Bytes(), nil
}

func (p *Packet) UnmarshalVersionSlice(cnt int, d []byte) error {
	items := make([]*VolVersionInfo, 0)
	buf := bytes.NewBuffer(d)
	var err error

	for idx := 0; idx < cnt; idx++ {
		e := &VolVersionInfo{}
		err = binary.Read(buf, binary.BigEndian, &e.Ver)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.BigEndian, &e.DelTime)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.BigEndian, &e.Status)
		if err != nil {
			return err
		}
		items = append(items, e)
	}
	p.VerList = items
	return nil
}

// MarshalData marshals the packet data.
func (p *Packet) MarshalData(v interface{}) error {
	data, err := json.Marshal(v)
	if err == nil {
		p.Data = data
		p.Size = uint32(len(p.Data))
	}
	return err
}

// UnmarshalData unmarshals the packet data.
func (p *Packet) UnmarshalData(v interface{}) error {
	return json.Unmarshal(p.Data, v)
}

// WriteToNoDeadLineConn writes through the connection without deadline.
func (p *Packet) WriteToNoDeadLineConn(c net.Conn) (err error) {
	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.ArgLen)]); err == nil {
			if p.Data != nil {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func (p *Packet) CalcPacketHeaderSize() (headerSize int) {
	headerSize = util.PacketHeaderSize

	if p.ExtentType&PacketProtocolVersionFlag > 0 {
		headerSize = util.PacketHeaderProtoVerSize
	} else if p.Opcode == OpRandomWriteVer || p.ExtentType&MultiVersionFlag > 0 {
		headerSize = util.PacketHeaderVerSize
	}

	return
}

// WriteToConn writes through the given connection.
func (p *Packet) WriteToConn(c net.Conn) (err error) {
	headSize := p.CalcPacketHeaderSize()
	header, err := Buffers.Get(headSize)
	if err != nil {
		header = make([]byte, headSize)
	}
	// log.LogErrorf("action[WriteToConn] buffer get nil,opcode %v head len [%v]", p.Opcode, len(header))
	defer Buffers.Put(header)
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))
	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		// write dir version info.
		if p.IsVersionList() {
			d, err1 := p.MarshalVersionSlice()
			if err1 != nil {
				log.LogErrorf("MarshalVersionSlice: marshal version ifo failed, err %s", err1.Error())
				return err1
			}

			_, err = c.Write(d)
			if err != nil {
				return err
			}
		}
		if _, err = c.Write(p.Arg[:int(p.ArgLen)]); err == nil {
			if p.Data != nil && p.Size != 0 {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

// ReadFull is a wrapper function of io.ReadFull.
func ReadFull(c net.Conn, buf *[]byte, readSize int) (err error) {
	*buf = make([]byte, readSize)
	_, err = io.ReadFull(c, (*buf)[:readSize])
	return
}

func (p *Packet) IsWriteOperation() bool {
	return p.Opcode == OpWrite || p.Opcode == OpSyncWrite
}

func (p *Packet) IsReadOperation() bool {
	return p.Opcode == OpStreamRead || p.Opcode == OpRead ||
		p.Opcode == OpExtentRepairRead || p.Opcode == OpReadTinyDeleteRecord ||
		p.Opcode == OpTinyExtentRepairRead || p.Opcode == OpStreamFollowerRead ||
		p.Opcode == OpSnapshotExtentRepairRead
}

func (p *Packet) IsReadMetaPkt() bool {
	if p.Opcode == OpMetaLookup || p.Opcode == OpMetaInodeGet || p.Opcode == OpMetaBatchInodeGet ||
		p.Opcode == OpMetaReadDir || p.Opcode == OpMetaExtentsList || p.Opcode == OpGetMultipart ||
		p.Opcode == OpMetaGetXAttr || p.Opcode == OpMetaListXAttr || p.Opcode == OpListMultiparts ||
		p.Opcode == OpMetaBatchGetXAttr || p.Opcode == OpMetaObjExtentsList || p.Opcode == OpMetaReadDirLimit || p.Opcode == OpMetaGetInodeQuota {
		return true
	}
	return false
}

// ReadFromConn reads the data from the given connection.
// Recognize the version bit and parse out version,
// to avoid version field rsp back , the rsp of random write from datanode with replace OpRandomWriteVer to OpRandomWriteVerRsp
func (p *Packet) ReadFromConnWithVer(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(timeoutSec)))
	} else {
		c.SetReadDeadline(time.Time{})
	}

	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)
	var n int
	if n, err = io.ReadFull(c, header); err != nil {
		return
	}
	if n != util.PacketHeaderSize {
		return syscall.EBADMSG
	}

	if err = p.UnmarshalHeader(header); err != nil {
		err = fmt.Errorf("packet from remote(%v) UnmarshalHeader err: %v",
			c.RemoteAddr(), err)
		log.LogWarnf("[ReadFromConnWithVer] %v", err)
		return
	}

	if err = p.TryReadExtraFieldsFromConn(c); err != nil {
		err = fmt.Errorf("reqId(%v) opCode(%v) protoVer(%v) remote(%v), read extra fileds err: %v",
			p.GetReqID(), p.GetOpMsg(), p.ProtoVersion, c.RemoteAddr(), err)
		log.LogWarnf("[ReadFromConnWithVer] %v", err)
		return
	}

	if p.IsVersionList() {
		cntByte := make([]byte, 2)
		if _, err = io.ReadFull(c, cntByte); err != nil {
			return err
		}
		cnt := binary.BigEndian.Uint16(cntByte)
		log.LogDebugf("action[ReadFromConnWithVer] op %s verseq %v, extType %d, cnt %d",
			p.GetOpMsg(), p.VerSeq, p.ExtentType, cnt)
		verData := make([]byte, cnt*verInfoCnt)
		if _, err = io.ReadFull(c, verData); err != nil {
			err = fmt.Errorf("reqId(%v) opCode(%v) protoVer(%v) remote(%v), read ver slice from conn failed: %v",
				p.GetReqID(), p.GetOpMsg(), p.ProtoVersion, c.RemoteAddr(), err)
			log.LogWarnf("[ReadFromConnWithVer] %v", err)
			return err
		}

		err = p.UnmarshalVersionSlice(int(cnt), verData)
		if err != nil {
			err = fmt.Errorf("reqId(%v) opCode(%v) protoVer(%v) remote(%v), unmarshal ver slice failed: %v",
				p.GetReqID(), p.GetOpMsg(), p.ProtoVersion, c.RemoteAddr(), err)
			log.LogWarnf("[ReadFromConnWithVer] %v", err)
			return err
		}
	}

	if p.ArgLen > 0 {
		p.Arg = make([]byte, int(p.ArgLen))
		if _, err = io.ReadFull(c, p.Arg[:int(p.ArgLen)]); err != nil {
			err = fmt.Errorf("reqId(%v) opCode(%v) protoVer(%v) remote(%v), read arg failed: %v",
				p.GetReqID(), p.GetOpMsg(), p.ProtoVersion, c.RemoteAddr(), err)
			log.LogWarnf("[ReadFromConnWithVer] %v", err)
			return err
		}
	}

	size := p.Size
	if p.IsReadOperation() && p.ResultCode == OpInitResultCode {
		size = 0
	}

	if p.IsWriteOperation() && size == util.BlockSize {
		p.Data, _ = Buffers.Get(int(size))
	} else {
		p.Data = make([]byte, size)
	}

	if n, err = io.ReadFull(c, p.Data[:size]); err != nil {
		err = fmt.Errorf("reqId(%v) opCode(%v) protoVer(%v) remote(%v), read data failed: %v",
			p.GetReqID(), p.GetOpMsg(), p.ProtoVersion, c.RemoteAddr(), err)
		log.LogWarnf("[ReadFromConnWithVer] %v", err)
		return err
	}
	if n != int(size) {
		return syscall.EBADMSG
	}
	return nil
}

// ReadFromConn reads the data from the given connection.
func (p *Packet) ReadFromConn(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(timeoutSec)))
	} else {
		c.SetReadDeadline(time.Time{})
	}
	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)
	var n int
	if n, err = io.ReadFull(c, header); err != nil {
		return
	}
	if n != util.PacketHeaderSize {
		return syscall.EBADMSG
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if err = p.TryReadExtraFieldsFromConn(c); err != nil {
		return
	}

	if p.ArgLen > 0 {
		p.Arg = make([]byte, int(p.ArgLen))
		if _, err = io.ReadFull(c, p.Arg[:int(p.ArgLen)]); err != nil {
			return err
		}
	}

	size := p.Size
	if (p.Opcode == OpRead || p.Opcode == OpStreamRead || p.Opcode == OpExtentRepairRead || p.Opcode == OpStreamFollowerRead ||
		p.Opcode == OpBackupRead) && p.ResultCode == OpInitResultCode {
		size = 0
	}
	p.Data = make([]byte, size)
	if n, err = io.ReadFull(c, p.Data[:size]); err != nil {
		return err
	}
	if n != int(size) {
		return syscall.EBADMSG
	}
	return nil
}

// PacketOkReply sets the result code as OpOk, and sets the body as empty.
func (p *Packet) PacketOkReply() {
	p.ResultCode = OpOk
	p.Size = 0
	p.Data = nil
	p.ArgLen = 0
}

// PacketOkWithBody sets the result code as OpOk, and sets the body with the give data.
func (p *Packet) PacketOkWithBody(reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = OpOk
	p.ArgLen = 0
}

// attention use for tmp byte arr, eg: json marshal data
func (p *Packet) PacketOkWithByte(reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = reply
	p.ResultCode = OpOk
	p.ArgLen = 0
}

// PacketErrorWithBody sets the packet with error code whose body is filled with the given data.
func (p *Packet) PacketErrorWithBody(code uint8, reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = code
	p.ArgLen = 0
}

func (p *Packet) SetPacketHasPrepare() {
	p.setPacketPrefix()
	p.HasPrepare = true
}

func (p *Packet) SetPacketRePrepare() {
	p.HasPrepare = false
}

func (p *Packet) AddMesgLog(m string) {
	p.mesg += m
}

// GetUniqueLogId returns the unique log ID.
func (p *Packet) GetUniqueLogId() (m string) {
	defer func() {
		m = m + fmt.Sprintf("_ResultMesg(%v)", p.GetResultMsg())
	}()
	if p.HasPrepare {
		m = p.GetMsg()
		return
	}
	m = fmt.Sprintf("Req(%v)_Partition(%v)_", p.ReqID, p.PartitionID)
	if p.Opcode == OpSplitMarkDelete || (IsTinyExtentType(p.ExtentType) && p.Opcode == OpMarkDelete) && len(p.Data) > 0 {
		ext := new(TinyExtentDeleteRecord)
		err := json.Unmarshal(p.Data, ext)
		if err == nil {
			m += fmt.Sprintf("Extent(%v)_ExtentOffset(%v)_Size(%v)_Opcode(%v)",
				ext.ExtentId, ext.ExtentOffset, ext.Size, p.GetOpMsg())
			return m
		}
	} else if p.Opcode == OpReadTinyDeleteRecord || p.Opcode == OpNotifyReplicasToRepair || p.Opcode == OpDataNodeHeartbeat ||
		p.Opcode == OpLoadDataPartition || p.Opcode == OpBatchDeleteExtent || p.Opcode == OpGcBatchDeleteExtent {
		p.mesg += fmt.Sprintf("Opcode(%v)", p.GetOpMsg())
		return
	} else if p.Opcode == OpBroadcastMinAppliedID || p.Opcode == OpGetAppliedId {
		if p.Size > 0 {
			applyID := binary.BigEndian.Uint64(p.Data)
			m += fmt.Sprintf("Opcode(%v)_AppliedID(%v)", p.GetOpMsg(), applyID)
		} else {
			m += fmt.Sprintf("Opcode(%v)", p.GetOpMsg())
		}
		return m
	}
	m = fmt.Sprintf("Req(%v)_Partition(%v)_Extent(%v)_ExtentOffset(%v)_KernelOffset(%v)_"+
		"Size(%v)_Opcode(%v)_CRC(%v)",
		p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset,
		p.KernelOffset, p.Size, p.GetOpMsg(), p.CRC)

	return
}

func (p *Packet) GetMsg() string {
	if p.noPrefix {
		p.mesg = fmt.Sprintf("Req(%v)_Partition(%v)_Extent(%v)_ExtentOffset(%v)_KernelOffset(%v)_"+
			"Size(%v)_Opcode(%v)_CRC(%v), m(%s)",
			p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset,
			p.KernelOffset, p.Size, p.GetOpMsg(), p.CRC, p.mesg)
		return p.mesg
	}
	return p.mesg
}

func (p *Packet) setPacketPrefix() {
	if !log.EnableDebug() && p.IsReadOperation() {
		p.noPrefix = true
		return
	}

	p.mesg = fmt.Sprintf("Req(%v)_Partition(%v)_", p.ReqID, p.PartitionID)
	if (p.Opcode == OpSplitMarkDelete || (IsTinyExtentType(p.ExtentType) && p.Opcode == OpMarkDelete)) && len(p.Data) > 0 {
		ext := new(TinyExtentDeleteRecord)
		err := json.Unmarshal(p.Data, ext)
		if err == nil {
			p.mesg += fmt.Sprintf("Extent(%v)_ExtentOffset(%v)_Size(%v)_Opcode(%v)",
				ext.ExtentId, ext.ExtentOffset, ext.Size, p.GetOpMsg())
			return
		}
	} else if p.Opcode == OpReadTinyDeleteRecord || p.Opcode == OpNotifyReplicasToRepair || p.Opcode == OpDataNodeHeartbeat ||
		p.Opcode == OpLoadDataPartition || p.Opcode == OpBatchDeleteExtent || p.Opcode == OpGcBatchDeleteExtent {
		p.mesg += fmt.Sprintf("Opcode(%v)", p.GetOpMsg())
		return
	} else if p.Opcode == OpBroadcastMinAppliedID || p.Opcode == OpGetAppliedId {
		if p.Size > 0 {
			applyID := binary.BigEndian.Uint64(p.Data)
			p.mesg += fmt.Sprintf("Opcode(%v)_AppliedID(%v)", p.GetOpMsg(), applyID)
		} else {
			p.mesg += fmt.Sprintf("Opcode(%v)", p.GetOpMsg())
		}
		return
	}
	p.mesg = fmt.Sprintf("Req(%v)_Partition(%v)_Extent(%v)_ExtentOffset(%v)_KernelOffset(%v)_"+
		"Size(%v)_Opcode(%v)_CRC(%v)",
		p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset,
		p.KernelOffset, p.Size, p.GetOpMsg(), p.CRC)
}

// IsForwardPkt returns if the packet is the forward packet (a packet that will be forwarded to the followers).
func (p *Packet) IsForwardPkt() bool {
	return p.RemainingFollowers > 0
}

// LogMessage logs the given message.
func (p *Packet) LogMessage(action, remote string, start int64, err error) (m string) {
	if err == nil {
		m = fmt.Sprintf("id[%v] isPrimaryBackReplLeader[%v] remote[%v] "+
			" cost[%v] ", p.GetUniqueLogId(), p.IsForwardPkt(), remote, (time.Now().UnixNano()-start)/1e6)
	} else {
		m = fmt.Sprintf("id[%v] isPrimaryBackReplLeader[%v] remote[%v]"+
			", err[%v]", p.GetUniqueLogId(), p.IsForwardPkt(), remote, err.Error())
	}

	return
}

// ShallRetry returns if we should retry the packet.
func (p *Packet) ShouldRetryWithVersionList() bool {
	return p.ResultCode == OpAgainVerionList
}

// ShallRetry returns if we should retry the packet.
func (p *Packet) ShouldRetry() bool {
	return p.ResultCode == OpAgain || p.ResultCode == OpErr
}

func (p *Packet) IsBatchDeleteExtents() bool {
	return p.Opcode == OpBatchDeleteExtent || p.Opcode == OpGcBatchDeleteExtent
}

func InitBufferPool(bufLimit int64) {
	buf.NormalBuffersTotalLimit = bufLimit
	buf.HeadBuffersTotalLimit = bufLimit
	buf.HeadVerBuffersTotalLimit = bufLimit
	buf.HeadProtoVerBuffersTotalLimit = bufLimit

	Buffers = buf.NewBufferPool()
}

func InitBufferPoolEx(bufLimit int64, chanSize int) {
	if chanSize <= 0 {
		buf.HeaderBufferPoolSize = buf.DefaultBufChanSize
	} else if chanSize < buf.MinBufferChanSize {
		buf.HeaderBufferPoolSize = buf.MinBufferChanSize
	} else {
		buf.HeaderBufferPoolSize = chanSize
	}

	if bufLimit > 0 {
		buf.TinyBuffersTotalLimit = bufLimit / 8
	}

	InitBufferPool(bufLimit)
}

func (p *Packet) IsBatchLockNormalExtents() bool {
	return p.Opcode == OpBatchLockNormalExtent
}

func (p *Packet) IsBatchUnlockNormalExtents() bool {
	return p.Opcode == OpBatchUnlockNormalExtent
}
