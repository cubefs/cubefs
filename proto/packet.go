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

package proto

import (
	"context"
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

	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/buf"
)

var (
	GRequestID = int64(1)
	Buffers    = buf.NewBufferPool()
)

// GenerateRequestID generates the request ID.
func GenerateRequestID() int64 {
	return atomic.AddInt64(&GRequestID, 1)
}

const (
	AddrSplit = "/"
)

const (
	OpInodeGetVersion1   uint8 = 1
	OpInodeGetVersion2   uint8 = 2
	OpInodeGetCurVersion uint8 = OpInodeGetVersion2
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
	OpGetAllWatermarksV2             uint8 = 0x17
	OpGetAllExtentInfo               uint8 = 0x18
	OpTinyExtentAvaliRead            uint8 = 0x19

	// Operations client-->datanode
	OpRandomWriteV3     uint8 = 0x50
	OpSyncRandomWriteV3 uint8 = 0x51

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

	OpMetaRecoverDeletedDentry      uint8 = 0x80
	OpMetaRecoverDeletedInode       uint8 = 0x81
	OpMetaCleanDeletedDentry        uint8 = 0x82
	OpMetaCleanDeletedInode         uint8 = 0x83
	OpMetaCleanExpiredDentry        uint8 = 0x84
	OpMetaCleanExpiredInode         uint8 = 0x85
	OpMetaLookupForDeleted          uint8 = 0x86
	OpMetaGetDeletedInode           uint8 = 0x87
	OpMetaBatchGetDeletedInode      uint8 = 0x88
	OpMetaReadDeletedDir            uint8 = 0x89
	OpMetaStatDeletedFileInfo       uint8 = 0x8A
	OpMetaBatchRecoverDeletedDentry uint8 = 0x8B
	OpMetaBatchRecoverDeletedInode  uint8 = 0x8C
	OpMetaBatchCleanDeletedDentry   uint8 = 0x8D
	OpMetaBatchCleanDeletedInode    uint8 = 0x8E

	//Operations: MetaNode Leader -> MetaNode Follower
	OpMetaFreeInodesOnRaftFollower uint8 = 0x32

	OpMetaDeleteInode        uint8 = 0x33 // delete specified inode immediately and do not remove data.
	OpMetaBatchExtentsAdd    uint8 = 0x34 // for extents batch attachment
	OpMetaSetXAttr           uint8 = 0x35
	OpMetaGetXAttr           uint8 = 0x36
	OpMetaRemoveXAttr        uint8 = 0x37
	OpMetaListXAttr          uint8 = 0x38
	OpMetaBatchGetXAttr      uint8 = 0x39
	OpMetaGetAppliedID       uint8 = 0x3A
	OpMetaExtentsInsert      uint8 = 0x3B
	OpMetaInodeGetV2         uint8 = 0x3C //new op code, old(get) compatible the old client
	OpGetMetaNodeVersionInfo uint8 = 0x3D

	// Operations: Master -> MetaNode
	OpCreateMetaPartition             uint8 = 0x40
	OpMetaNodeHeartbeat               uint8 = 0x41
	OpDeleteMetaPartition             uint8 = 0x42
	OpUpdateMetaPartition             uint8 = 0x43
	OpLoadMetaPartition               uint8 = 0x44
	OpDecommissionMetaPartition       uint8 = 0x45
	OpAddMetaPartitionRaftMember      uint8 = 0x46
	OpRemoveMetaPartitionRaftMember   uint8 = 0x47
	OpMetaPartitionTryToLeader        uint8 = 0x48
	OpResetMetaPartitionRaftMember    uint8 = 0x4B
	OpAddMetaPartitionRaftLearner     uint8 = 0x4C
	OpPromoteMetaPartitionRaftLearner uint8 = 0x4D

	// Operations: Master -> DataNode
	OpCreateDataPartition             uint8 = 0x60
	OpDeleteDataPartition             uint8 = 0x61
	OpLoadDataPartition               uint8 = 0x62
	OpDataNodeHeartbeat               uint8 = 0x63
	OpReplicateFile                   uint8 = 0x64
	OpDeleteFile                      uint8 = 0x65
	OpDecommissionDataPartition       uint8 = 0x66
	OpAddDataPartitionRaftMember      uint8 = 0x67
	OpRemoveDataPartitionRaftMember   uint8 = 0x68
	OpDataPartitionTryToLeader        uint8 = 0x69
	OpSyncDataPartitionReplicas       uint8 = 0x6A
	OpAddDataPartitionRaftLearner     uint8 = 0x6B
	OpPromoteDataPartitionRaftLearner uint8 = 0x6C
	OpResetDataPartitionRaftMember    uint8 = 0x6D

	// Operations: MultipartInfo
	OpCreateMultipart  uint8 = 0x70
	OpGetMultipart     uint8 = 0x71
	OpAddMultipartPart uint8 = 0x72
	OpRemoveMultipart  uint8 = 0x73
	OpListMultiparts   uint8 = 0x74

	OpBatchDeleteExtent uint8 = 0x75 // SDK to MetaNode

	//Operations: MetaNode Leader -> MetaNode Follower
	OpMetaBatchDeleteInode  uint8 = 0x90
	OpMetaBatchDeleteDentry uint8 = 0x91
	OpMetaBatchUnlinkInode  uint8 = 0x92
	OpMetaBatchEvictInode   uint8 = 0x93

	//inode reset
	OpMetaCursorReset   uint8 = 0x94
	OpMetaGetCmpInode   uint8 = 0x95
	OpMetaInodeMergeEks uint8 = 0x96

	// Commons
	OpInodeOutOfRange  uint8 = 0xF2
	OpIntraGroupNetErr uint8 = 0xF3
	OpArgMismatchErr   uint8 = 0xF4
	OpNotExistErr      uint8 = 0xF5
	OpDiskNoSpaceErr   uint8 = 0xF6
	OpDiskErr          uint8 = 0xF7
	OpErr              uint8 = 0xF8
	OpAgain            uint8 = 0xF9
	OpExistErr         uint8 = 0xFA
	OpInodeFullErr     uint8 = 0xFB
	OpTryOtherAddr     uint8 = 0xFC
	OpNotPerm          uint8 = 0xFD
	OpNotEmtpy         uint8 = 0xFE
	OpDisabled         uint8 = 0xFF
	OpOk               uint8 = 0xF0

	OpPing uint8 = 0xFF
)

const (
	WriteDeadlineTime            = 5
	ReadDeadlineTime             = 5
	SyncSendTaskDeadlineTime     = 20
	NoReadDeadlineTime           = -1
	MaxWaitFollowerRepairTime    = 60 * 5
	GetAllWatermarksDeadLineTime = 60
	MaxPacketProcessTime         = 5
	MinReadDeadlineTime          = 1
)

const (
	TinyExtentType   = 0
	NormalExtentType = 1
	AllExtentType    = 2
)

const (
	CheckPreExtentExist = 1
)

const (
	NormalCreateDataPartition         = 0
	DecommissionedCreateDataPartition = 1
)

const FollowerReadFlag = 'F'

var GReadOps = []uint8{OpMetaLookup, OpMetaReadDir, OpMetaInodeGet, OpMetaBatchInodeGet, OpMetaExtentsList, OpMetaGetXAttr,
	OpMetaListXAttr, OpMetaBatchGetXAttr, OpMetaGetAppliedID, OpGetMultipart, OpListMultiparts}

// Packet defines the packet structure.
type Packet struct {
	Magic              uint8
	ExtentType         uint8
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
	SendT              int64
	WaitT              int64
	RecvT              int64
	mesg               string
	HasPrepare         bool
	PoolFlag           int64
	ctx                context.Context
}

// NewPacket returns a new packet.
func NewPacket(ctx context.Context) *Packet {
	p := new(Packet)
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()
	p.SetCtx(ctx)

	return p
}

// NewPacketReqID returns a new packet with ReqID assigned.
func NewPacketReqID(ctx context.Context) *Packet {
	p := NewPacket(ctx)
	p.ReqID = GenerateRequestID()
	return p
}

func (p *Packet) Ctx() context.Context {
	return p.ctx
}

func (p *Packet) SetCtx(ctx context.Context) {
	p.ctx = ctx
}

func (p *Packet) String() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("Req(%v)Op(%v)PartitionID(%v)ResultCode(%v)", p.ReqID, p.GetOpMsg(), p.PartitionID, p.GetResultMsg())
}

func (p *Packet) IsReadOp() bool {
	for _, opCode := range GReadOps {
		if p.Opcode == opCode {
			return true
		}
	}
	return false
}

// GetStoreType returns the store type.
func (p *Packet) GetStoreType() (m string) {
	switch p.ExtentType {
	case TinyExtentType:
		m = "TinyExtent"
	case NormalExtentType:
		m = "NormalExtent"
	default:
		m = "Unknown"
	}
	return
}

func (p *Packet) GetOpMsgWithReqAndResult() (m string) {
	return fmt.Sprintf("Req(%v)_(%v)_Result(%v)_Body(%v)", p.ReqID, p.GetOpMsg(), p.GetResultMsg(), string(p.Data[0:p.Size]))
}

// GetOpMsg returns the operation type.
func (p *Packet) GetOpMsg() (m string) {
	switch p.Opcode {
	case OpCreateExtent:
		m = "OpCreateExtent"
	case OpMarkDelete:
		m = "OpMarkDelete"
	case OpWrite:
		m = "OpWrite"
	case OpRandomWrite:
		m = "OpRandomWrite"
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
	case OpInodeOutOfRange:
		m = "InodeOutOfRange"
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr"
	case OpMetaCreateInode:
		m = "OpMetaCreateInode"
	case OpMetaUnlinkInode:
		m = "OpMetaUnlinkInode"
	case OpMetaBatchUnlinkInode:
		m = "OpMetaBatchUnlinkInode"
	case OpMetaCreateDentry:
		m = "OpMetaCreateDentry"
	case OpMetaDeleteDentry:
		m = "OpMetaDeleteDentry"
	case OpMetaOpen:
		m = "OpMetaOpen"
	case OpMetaReleaseOpen:
		m = "OpMetaReleaseOpen"
	case OpMetaLookup:
		m = "OpMetaLookup"
	case OpMetaReadDir:
		m = "OpMetaReadDir"
	case OpMetaInodeGet:
		m = "OpMetaInodeGet"
	case OpMetaInodeGetV2:
		m = "OpMetaInodeGetV2"
	case OpMetaBatchInodeGet:
		m = "OpMetaBatchInodeGet"
	case OpMetaExtentsAdd:
		m = "OpMetaExtentsAdd"
	case OpMetaExtentsInsert:
		m = "OpMetaExtentsInsert"
	case OpMetaExtentsDel:
		m = "OpMetaExtentsDel"
	case OpMetaExtentsList:
		m = "OpMetaExtentsList"
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
	case OpSyncRandomWrite:
		m = "OpSyncRandomWrite"
	case OpReadTinyDeleteRecord:
		m = "OpReadTinyDeleteRecord"
	case OpPing:
		m = "OpPing"
	case OpTinyExtentRepairRead:
		m = "OpTinyExtentRepairRead"
	case OpGetMaxExtentIDAndPartitionSize:
		m = "OpGetMaxExtentIDAndPartitionSize"
	case OpBroadcastMinAppliedID:
		m = "OpBroadcastMinAppliedID"
	case OpRemoveDataPartitionRaftMember:
		m = "OpRemoveDataPartitionRaftMember"
	case OpAddDataPartitionRaftMember:
		m = "OpAddDataPartitionRaftMember"
	case OpAddDataPartitionRaftLearner:
		m = "OpAddDataPartitionRaftLearner"
	case OpPromoteDataPartitionRaftLearner:
		m = "OpPromoteDataPartitionRaftLearner"
	case OpResetDataPartitionRaftMember:
		m = "OpResetDataPartitionRaftMember"
	case OpAddMetaPartitionRaftMember:
		m = "OpAddMetaPartitionRaftMember"
	case OpRemoveMetaPartitionRaftMember:
		m = "OpRemoveMetaPartitionRaftMember"
	case OpAddMetaPartitionRaftLearner:
		m = "OpAddMetaPartitionRaftLearner"
	case OpPromoteMetaPartitionRaftLearner:
		m = "OpPromoteMetaPartitionRaftLearner"
	case OpResetMetaPartitionRaftMember:
		m = "OpResetMetaPartitionRaftMember"
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
	case OpMetaCursorReset:
		m = "OpMetaCursorReset"
	case OpSyncDataPartitionReplicas:
		m = "OpSyncDataPartitionReplicas"
	case OpMetaGetAppliedID:
		m = "OpMetaGetAppliedID"
	case OpRandomWriteV3:
		m = "OpRandomWriteV3"
	case OpSyncRandomWriteV3:
		m = "OpSyncRandomWriteV3"
	case OpMetaRecoverDeletedDentry:
		m = "OpMetaRecoverDeletedDentry"
	case OpMetaBatchRecoverDeletedDentry:
		m = "OpMetaBatchRecoverDeletedDentry"
	case OpMetaRecoverDeletedInode:
		m = "OpMetaRecoverDeletedInode"
	case OpMetaBatchRecoverDeletedInode:
		m = "OpMetaBatchRecoverDeletedInode"
	case OpMetaCleanExpiredInode:
		m = "OpMetaCleanExpiredInode"
	case OpMetaCleanExpiredDentry:
		m = "OpMetaCleanExpiredDentry"
	case OpMetaCleanDeletedInode:
		m = "OpMetaCleanDeletedInode"
	case OpMetaCleanDeletedDentry:
		m = "OpMetaCleanDeletedDentry"
	case OpMetaGetDeletedInode:
		m = "OpMetaGetDeletedInode"
	case OpMetaBatchGetDeletedInode:
		m = "OpMetaBatchGetDeletedInode"
	case OpMetaLookupForDeleted:
		m = "OpMetaLookupForDeleted"
	case OpMetaReadDeletedDir:
		m = "OpMetaReadDeletedDir"
	case OpMetaStatDeletedFileInfo:
		m = "OpMetaStatDeletedFileInfo"
	case OpMetaGetCmpInode:
		m = "OpMetaGetCmpInode"
	case OpMetaInodeMergeEks:
		m = "OpMetaInodeMergeEks"
	}
	return
}

// GetResultMsg returns the result message.
func (p *Packet) GetResultMsg() (m string) {
	if p == nil {
		return ""
	}

	switch p.ResultCode {
	case OpInodeOutOfRange:
		m = "Inode Out of Range :" + p.GetRespData()
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr: " + p.GetRespData()
	case OpDiskNoSpaceErr:
		m = "DiskNoSpaceErr: " + p.GetRespData()
	case OpDiskErr:
		m = "DiskErr: " + p.GetRespData()
	case OpErr:
		m = "Err: " + p.GetRespData()
	case OpAgain:
		m = "Again: " + p.GetRespData()
	case OpOk:
		m = "Ok"
	case OpExistErr:
		m = "ExistErr: " + p.GetRespData()
	case OpInodeFullErr:
		m = "InodeFullErr: " + p.GetRespData()
	case OpArgMismatchErr:
		m = "ArgUnmatchErr: " + p.GetRespData()
	case OpNotExistErr:
		m = "NotExistErr: " + p.GetRespData()
	case OpTryOtherAddr:
		m = "TryOtherAddr: " + p.GetRespData()
	case OpNotPerm:
		m = "NotPerm: " + p.GetRespData()
	case OpNotEmtpy:
		m = "DirNotEmpty: " + p.GetRespData()
	case OpDisabled:
		m = "Disabled: " + p.GetRespData()
	default:
		return fmt.Sprintf("Unknown ResultCode(%v)", p.ResultCode)
	}
	return
}

func (p *Packet) GetReqID() int64 {
	return p.ReqID
}

func (p *Packet) GetRespData() (msg string) {
	if len(p.Data) > 0 && p.Size < uint32(len(p.Data)) {
		msgLen := util.Min(int(p.Size), 512)
		msg = string(p.Data[:msgLen])
	}
	return msg
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
	return
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

// WriteToConn writes through the given connection.
func (p *Packet) WriteToConn(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec > 0 {
		c.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(timeoutSec)))
	}

	return p.writeToConn(c)
}

// WriteToConn writes through the given connection.
func (p *Packet) WriteToConnNs(c net.Conn, timeoutNs int64) (err error) {
	if timeoutNs > 0 {
		c.SetWriteDeadline(time.Now().Add(time.Nanosecond * time.Duration(timeoutNs)))
	}

	return p.writeToConn(c)
}

func (p *Packet) writeToConn(c net.Conn) (err error) {
	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)
	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
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

// ReadFromConn reads the data from the given connection.
func (p *Packet) ReadFromConn(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(timeoutSec)))
	} else {
		c.SetReadDeadline(time.Time{})
	}
	return p.readFromConn(c)
}

func (p *Packet) ReadFromConnNs(c net.Conn, timeoutNs int64) (err error) {
	if timeoutNs != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Nanosecond * time.Duration(timeoutNs)))
	} else {
		c.SetReadDeadline(time.Time{})
	}
	return p.readFromConn(c)
}

func (p *Packet) readFromConn(c net.Conn) (err error) {
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

	if p.ArgLen > 0 {
		p.Arg = make([]byte, int(p.ArgLen))
		if _, err = io.ReadFull(c, p.Arg[:int(p.ArgLen)]); err != nil {
			return err
		}
	}

	if p.Size < 0 {
		return syscall.EBADMSG
	}
	size := p.Size
	if (p.Opcode == OpRead || p.Opcode == OpStreamRead || p.Opcode == OpExtentRepairRead || p.Opcode == OpStreamFollowerRead) && p.ResultCode == OpInitResultCode {
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

// PacketErrorWithBody sets the packet with error code whose body is filled with the given data.
func (p *Packet) PacketErrorWithBody(code uint8, reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = code
	p.ArgLen = 0
}

func (p *Packet) SetPacketHasPrepare(remoteAddr string) {
	m := p.getPacketCommonLog(remoteAddr)
	p.mesg = m
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
		if p.PoolFlag==0 {
			m = m + fmt.Sprintf("_ResultMesg(%v)", p.GetResultMsg())
		}else {
			m = m + fmt.Sprintf("_ResultMesg(%v)_PoolFLag(%v)", p.GetResultMsg(),p.PoolFlag)
		}
	}()
	if p.HasPrepare {
		m = p.mesg
		return
	}
	m = p.getPacketCommonLog("unknowAddr")
	return
}

func (p *Packet) getPacketCommonLog(remoteAddr string) (m string) {
	m = fmt.Sprintf("RemoteAddr(%v)_Req(%v)_Partition(%v)_", remoteAddr, p.ReqID, p.PartitionID)
	if p.ExtentType == TinyExtentType && p.Opcode == OpMarkDelete && len(p.Data) > 0 {
		ext := new(TinyExtentDeleteRecord)
		err := json.Unmarshal(p.Data, ext)
		if err == nil {
			m += fmt.Sprintf("Extent(%v)_ExtentOffset(%v)_Size(%v)_Opcode(%v)",
				ext.ExtentId, ext.ExtentOffset, ext.Size, p.GetOpMsg())
			return
		}
	} else if p.Opcode == OpReadTinyDeleteRecord || p.Opcode == OpNotifyReplicasToRepair || p.Opcode == OpDataNodeHeartbeat ||
		p.Opcode == OpLoadDataPartition || p.Opcode == OpBatchDeleteExtent {
		p.mesg += fmt.Sprintf("Opcode(%v)", p.GetOpMsg())
		return
	} else if p.Opcode == OpBroadcastMinAppliedID || p.Opcode == OpGetAppliedId {
		if p.Size > 0 {
			applyID := binary.BigEndian.Uint64(p.Data)
			m += fmt.Sprintf("Opcode(%v)_AppliedID(%v)", p.GetOpMsg(), applyID)
		} else {
			m += fmt.Sprintf("Opcode(%v)", p.GetOpMsg())
		}
		return
	} else if p.Opcode == OpGetAllWatermarks || p.Opcode == OpGetAllWatermarksV2 {
		m += fmt.Sprintf("Opcode(%v)_RepairExtentType(%v)_", p.GetOpMsg(), p.ExtentType)
		return
	}
	m = fmt.Sprintf("Req(%v)_Partition(%v)_Extent(%v)_ExtentOffset(%v)_KernelOffset(%v)_"+
		"Size(%v)_Opcode(%v)_CRC(%v)",
		p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset,
		p.KernelOffset, p.Size, p.GetOpMsg(), p.CRC)
	return
}

// IsForwardPkt returns if the packet is the forward packet (a packet that will be forwarded to the followers).
func (p *Packet) IsForwardPkt() bool {
	return p.RemainingFollowers > 0
}

// LogMessage logs the given message.
func (p *Packet) LogMessage(action, remote string, start int64, err error) (m string) {
	if err == nil {
		m = fmt.Sprintf("action[%v] id[%v] isPrimaryBackReplLeader[%v] remote[%v] "+
			" cost[%v]ms ", action, p.GetUniqueLogId(), p.IsForwardPkt(), remote, (time.Now().UnixNano()-start)/1e6)

	} else {
		m = fmt.Sprintf("action[%v] id[%v] isPrimaryBackReplLeader[%v] remote[%v]"+
			", err[%v]", action, p.GetUniqueLogId(), p.IsForwardPkt(), remote, err.Error())
	}

	return
}

// ShallRetry returns if we should retry the packet.
// As meta can not reentrant the unlink op, so unlink can not retry.
// Meta can not reentran ops [create dentry\ update dentry\ create indoe\ link inode\ unlink inode\]
func (p *Packet) ShouldRetry() bool {
	return p.Opcode != OpMetaUnlinkInode && (p.ResultCode == OpAgain || p.ResultCode == OpErr)
}

func (p *Packet) IsReadMetaPkt() bool {
	if p.Opcode == OpMetaLookup || p.Opcode == OpMetaInodeGet || p.Opcode == OpMetaInodeGetV2 || p.Opcode == OpMetaBatchInodeGet ||
		p.Opcode == OpMetaReadDir || p.Opcode == OpMetaExtentsList || p.Opcode == OpGetMultipart ||
		p.Opcode == OpMetaGetXAttr || p.Opcode == OpMetaListXAttr || p.Opcode == OpListMultiparts ||
		p.Opcode == OpMetaBatchGetXAttr || p.Opcode == OpMetaLookupForDeleted || p.Opcode == OpMetaGetDeletedInode ||
		p.Opcode == OpMetaBatchGetDeletedInode || p.Opcode == OpMetaReadDeletedDir {
		return true
	}
	return false
}

func (p *Packet) IsRandomWrite() bool {
	if p.Opcode == OpRandomWriteV3 || p.Opcode == OpSyncRandomWriteV3 || p.Opcode == OpRandomWrite || p.Opcode == OpSyncRandomWrite {
		return true
	}
	return false
}

func (p *Packet) IsFollowerReadMetaPkt() bool {
	if p.ArgLen == 1 && p.Arg[0] == FollowerReadFlag {
		return true
	}
	return false
}

func NewPacketToGetAllExtentInfo(ctx context.Context, partitionID uint64) (p *Packet) {
	p = new(Packet)
	p.Opcode = OpGetAllExtentInfo
	p.PartitionID = partitionID
	p.Magic = ProtoMagic
	p.ReqID = GenerateRequestID()
	p.SetCtx(ctx)
	return
}
