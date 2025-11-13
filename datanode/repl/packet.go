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

package repl

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

var (
	ErrBadNodes       = errors.New("BadNodesErr")
	ErrArgLenMismatch = errors.New("ArgLenMismatchErr")
)

type PacketInterface interface {
	IsErrPacket() bool
	WriteToConn(c net.Conn) (err error)
	ReadFromConnWithVer(c net.Conn, timeoutSec int) (err error)
	GetUniqueLogId() (m string)
	GetReqID() int64
	GetPartitionID() uint64
	GetExtentID() uint64
	GetSize() uint32
	GetCRC() uint32
	GetArg() []byte
	GetArgLen() uint32
	GetData() []byte
	GetResultCode() uint8
	GetExtentOffset() int64
	GetStartT() int64
	SetSize(size uint32)
	GetOpcode() uint8
	SetResultCode(uint8)
	SetCRC(crc uint32)
	SetExtentOffset(int64)
	GetOpMsg() (m string)
	ShallDegrade() bool
	SetStartT(StartT int64)
	SetData(data []byte)
	SetOpCode(uint8)
	LogMessage(action, remote string, start int64, err error) (m string)
	GetNoPrefixMsg() string
	PackErrorBody(action, msg string)
	PacketOkReply()
	SetArglen(len uint32)
	SetArg(data []byte)
}

type (
	NewPacketFunc func() (p PacketInterface)
	Packet        struct {
		proto.Packet
		followersAddrs  []string
		followerPackets []*FollowerPacket
		IsReleased      int32 // TODO what is released?
		Object          interface{}
		TpObject        *exporter.TimePointCount
		NeedReply       bool
		OrgBuffer       []byte

		// used locally
		shallDegrade bool
		AfterPre     bool
	}
)

type FollowerPacket struct {
	proto.Packet
	respCh chan error
}

func NewFollowerPacket() *FollowerPacket {
	fp := &FollowerPacket{
		respCh: make(chan error, 1),
		Packet: proto.Packet{
			StartT: time.Now().UnixNano(),
		},
	}
	return fp
}

func (p *FollowerPacket) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	errorBody := action + "_" + msg
	p.Size = uint32(len(errorBody))
	p.Data = []byte(errorBody)
}

func (p *FollowerPacket) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk && p.ResultCode != proto.OpInitResultCode
}

func (p *FollowerPacket) identificationErrorResultCode(errLog string, errMsg string) {
	if strings.Contains(errMsg, storage.ErrParameterMismatch.Error()) ||
		strings.Contains(errMsg, ErrorUnknownOp.Error()) {
		p.ResultCode = proto.OpArgMismatchErr
	} else if strings.Contains(errMsg, proto.ErrDataPartitionNotExists.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, storage.ErrExtentNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrExtentHasBeenDeleted.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.ErrNoSpace.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrLimitedIo.Error()) {
		p.ResultCode = proto.OpLimitedIoErr
	} else if strings.Contains(errMsg, storage.ErrTinyRecover.Error()) {
		p.ResultCode = proto.OpTinyRecoverErr
	} else if strings.Contains(errMsg, storage.ErrDpDecommissionRepair.Error()) {
		p.ResultCode = proto.OpDpDecommissionRepairErr
	} else if strings.Contains(errMsg, storage.ErrDpRepair.Error()) {
		p.ResultCode = proto.OpDpRepairErr
	} else if strings.Contains(errMsg, storage.ErrTryAgain.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, raft.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, raft.ErrStopped.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, storage.ErrStoreAlreadyClosed.Error()) {
		p.ResultCode = proto.OpStoreClosed
	} else if strings.Contains(errMsg, storage.ErrReachMaxExtentsCount.Error()) {
		p.ResultCode = proto.OpReachMaxExtentsErr
	} else if strings.Contains(errLog, ActionReceiveFromFollower) || strings.Contains(errLog, ActionSendToFollowers) ||
		strings.Contains(errLog, ConnIsNullErr) {
		p.ResultCode = proto.OpIntraGroupNetErr
		log.LogErrorf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
	} else if strings.Contains(errMsg, storage.ErrClusterForbidWriteOpOfProtoVer.Error()) {
		p.ResultCode = proto.OpWriteOpOfProtoVerForbidden
	} else {
		log.LogErrorf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
		p.ResultCode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) AfterTp() (ok bool) {
	if p.TpObject != nil {
		p.TpObject.Set(nil)
	}

	return
}

func (p *Packet) clean() {
	if p.Data == nil && p.OrgBuffer == nil {
		return
	}
	p.Object = nil
	p.TpObject = nil
	p.Data = nil
	p.Arg = nil
	if p.OrgBuffer != nil && len(p.OrgBuffer) == util.BlockSize && p.IsNormalWriteOperation() {
		proto.Buffers.Put(p.OrgBuffer)
		p.OrgBuffer = nil
	}
}

func copyPacket(src *Packet, dst *FollowerPacket) {
	dst.Magic = src.Magic
	dst.ExtentType = src.ExtentType
	dst.Opcode = src.Opcode
	dst.ResultCode = src.ResultCode
	dst.CRC = src.CRC
	dst.Size = src.Size
	dst.KernelOffset = src.KernelOffset
	dst.PartitionID = src.PartitionID
	dst.ExtentID = src.ExtentID
	dst.ExtentOffset = src.ExtentOffset
	dst.ReqID = src.ReqID
	dst.Data = src.OrgBuffer
}

func (p *Packet) BeforeTp(clusterID string) (ok bool) {
	if p.IsForwardPkt() && !p.IsRandomWrite() {
		p.TpObject = exporter.NewTPCnt(fmt.Sprintf("PrimaryBackUp_%v", p.GetOpMsg()))
	} else if p.IsRandomWrite() {
		p.TpObject = exporter.NewTPCnt(fmt.Sprintf("Raft_%v", p.GetOpMsg()))
	}

	return
}

func (p *Packet) resolveFollowersAddr() (err error) {
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionPreparePkt, err.Error())
		}
	}()
	if len(p.Arg) < int(p.ArgLen) {
		err = ErrArgLenMismatch
		return
	}
	str := string(p.Arg[:p.ArgLen])
	followerAddrs := strings.SplitN(str, proto.AddrSplit, -1)
	followerNum := len(followerAddrs) - 1
	p.OrgBuffer = p.Data
	if followerNum > 0 {
		p.followersAddrs = followerAddrs[:followerNum]
		log.LogInfof("action[resolveFollowersAddr] %v", p.followersAddrs)
	}
	p.followerPackets = make([]*FollowerPacket, followerNum)

	return
}

func NewPacketEx() PacketInterface {
	return &Packet{
		Packet: proto.Packet{
			Magic:  proto.ProtoMagic,
			StartT: time.Now().UnixNano(),
		},
		NeedReply: true,
	}
}

func NewPacket() *Packet {
	return &Packet{
		Packet: proto.Packet{
			Magic:  proto.ProtoMagic,
			StartT: time.Now().UnixNano(),
		},
		NeedReply: true,
	}
}

func NewPacketToGetAllWatermarks(partitionID uint64, extentType uint8) *Packet {
	return &Packet{
		Packet: proto.Packet{
			Opcode:      proto.OpGetAllWatermarks,
			PartitionID: partitionID,
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			ExtentType:  extentType,
			Data:        []byte{ByteMarker},
		},
	}
}

func NewPacketToReadTinyDeleteRecord(partitionID uint64, offset int64) *Packet {
	return &Packet{
		Packet: proto.Packet{
			Opcode:       proto.OpReadTinyDeleteRecord,
			PartitionID:  partitionID,
			Magic:        proto.ProtoMagic,
			ReqID:        proto.GenerateRequestID(),
			ExtentOffset: offset,
		},
	}
}

func NewReadTinyDeleteRecordResponsePacket(requestID int64, partitionID uint64) *Packet {
	return &Packet{
		Packet: proto.Packet{
			PartitionID: partitionID,
			Magic:       proto.ProtoMagic,
			Opcode:      proto.OpOk,
			ReqID:       requestID,
			ExtentType:  proto.NormalExtentType,
		},
	}
}

type (
	MakeStreamReadResponsePacket func(requestID int64, partitionID uint64, extentID uint64) (p PacketInterface)
	MakeExtentRepairReadPacket   func(partitionID uint64, extentID uint64, offset, size int) (p PacketInterface)
)

func NewExtentRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) PacketInterface {
	return &Packet{
		Packet: proto.Packet{
			ExtentID:     extentID,
			PartitionID:  partitionID,
			Magic:        proto.ProtoMagic,
			ExtentOffset: int64(offset),
			Size:         uint32(size),
			Opcode:       proto.OpExtentRepairRead,
			ExtentType:   proto.NormalExtentType,
			ReqID:        proto.GenerateRequestID(),
		},
	}
}

func NewTinyExtentRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) PacketInterface {
	return &Packet{
		Packet: proto.Packet{
			ExtentID:     extentID,
			PartitionID:  partitionID,
			Magic:        proto.ProtoMagic,
			ExtentOffset: int64(offset),
			Size:         uint32(size),
			Opcode:       proto.OpTinyExtentRepairRead,
			ExtentType:   proto.TinyExtentType,
			ReqID:        proto.GenerateRequestID(),
		},
	}
}

func NewTinyExtentStreamReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) *Packet {
	return &Packet{
		Packet: proto.Packet{
			ExtentID:    extentID,
			PartitionID: partitionID,
			Magic:       proto.ProtoMagic,
			Opcode:      proto.OpTinyExtentRepairRead,
			ReqID:       requestID,
			ExtentType:  proto.TinyExtentType,
			StartT:      time.Now().UnixNano(),
		},
	}
}

func NewNormalExtentWithHoleRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) PacketInterface {
	return &Packet{
		Packet: proto.Packet{
			ExtentID:     extentID,
			PartitionID:  partitionID,
			Magic:        proto.ProtoMagic,
			ExtentOffset: int64(offset),
			Size:         uint32(size),
			Opcode:       proto.OpSnapshotExtentRepairRead,
			ExtentType:   proto.TinyExtentType,
			ReqID:        proto.GenerateRequestID(),
		},
	}
}

func NewNormalExtentWithHoleStreamReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) *Packet {
	return &Packet{
		Packet: proto.Packet{
			ExtentID:    extentID,
			PartitionID: partitionID,
			Magic:       proto.ProtoMagic,
			Opcode:      proto.OpSnapshotExtentRepairRsp,
			ReqID:       requestID,
			ExtentType:  proto.NormalExtentType,
			StartT:      time.Now().UnixNano(),
		},
	}
}

func NewStreamReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) PacketInterface {
	return &Packet{
		Packet: proto.Packet{
			ExtentID:    extentID,
			PartitionID: partitionID,
			Magic:       proto.ProtoMagic,
			Opcode:      proto.OpOk,
			ReqID:       requestID,
			ExtentType:  proto.NormalExtentType,
		},
	}
}

func NewPacketToNotifyExtentRepair(partitionID uint64) *Packet {
	return &Packet{
		Packet: proto.Packet{
			Opcode:      proto.OpNotifyReplicasToRepair,
			PartitionID: partitionID,
			Magic:       proto.ProtoMagic,
			ExtentType:  proto.NormalExtentType,
			ReqID:       proto.GenerateRequestID(),
		},
	}
}

func (p *Packet) SetResultCode(code uint8) {
	p.ResultCode = code
}

func (p *Packet) SetCRC(crc uint32) {
	p.CRC = crc
}

func (p *Packet) SetExtentOffset(offset int64) {
	p.ExtentOffset = offset
}

func (p *Packet) GetStartT() int64 {
	return p.StartT
}

func (p *Packet) GetPartitionID() uint64 {
	return p.PartitionID
}

func (p *Packet) GetExtentID() uint64 {
	return p.ExtentID
}

func (p *Packet) GetSize() uint32 {
	return p.Size
}

func (p *Packet) SetSize(size uint32) {
	p.Size = size
}

func (p *Packet) SetOpCode(op uint8) {
	p.Opcode = op
}

func (p *Packet) GetOpcode() uint8 {
	return p.Opcode
}

func (p *Packet) GetArg() []byte {
	return p.Arg
}

func (p *Packet) GetCRC() uint32 {
	return p.CRC
}

func (p *Packet) GetArgLen() uint32 {
	return p.ArgLen
}

func (p *Packet) GetData() []byte {
	return p.Data
}

func (p *Packet) GetResultCode() uint8 {
	return p.ResultCode
}

func (p *Packet) GetExtentOffset() int64 {
	return p.ExtentOffset
}

func (p *Packet) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk && p.ResultCode != proto.OpInitResultCode
}

func (p *Packet) IsWriteOpOfPacketProtoVerForbidden() bool {
	return p.ResultCode == proto.OpWriteOpOfProtoVerForbidden
}

var ErrorUnknownOp = errors.New("unknown opcode")

func (p *Packet) identificationErrorResultCode(errLog string, errMsg string) {
	log.LogDebugf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
	if strings.Contains(errMsg, storage.ErrParameterMismatch.Error()) ||
		strings.Contains(errMsg, ErrorUnknownOp.Error()) {
		p.ResultCode = proto.OpArgMismatchErr
	} else if strings.Contains(errMsg, proto.ErrDataPartitionNotExists.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, storage.ErrExtentNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrExtentHasBeenDeleted.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.ErrNoSpace.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrBrokenDisk.Error()) {
		p.ResultCode = proto.OpDiskErr
	} else if strings.Contains(errMsg, "GetAvailableTinyExtent") {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrLimitedIo.Error()) {
		p.ResultCode = proto.OpLimitedIoErr
	} else if strings.Contains(errMsg, storage.ErrTinyRecover.Error()) {
		p.ResultCode = proto.OpTinyRecoverErr
	} else if strings.Contains(errMsg, storage.ErrDpDecommissionRepair.Error()) {
		p.ResultCode = proto.OpDpDecommissionRepairErr
	} else if strings.Contains(errMsg, storage.ErrDpRepair.Error()) {
		p.ResultCode = proto.OpDpRepairErr
	} else if strings.Contains(errMsg, storage.ErrTryAgain.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, raft.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, raft.ErrStopped.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, storage.ErrVerNotConsistent.Error()) {
		p.ResultCode = proto.ErrCodeVersionOpError
		// log.LogDebugf("action[identificationErrorResultCode] not change ver erro code, (%v)", string(debug.Stack()))
	} else if strings.Contains(errMsg, storage.ErrNoDiskReadRepairExtentToken.Error()) {
		p.ResultCode = proto.OpReadRepairExtentAgain
	} else if strings.Contains(errMsg, storage.ErrStoreAlreadyClosed.Error()) {
		p.ResultCode = proto.OpStoreClosed
	} else if strings.Contains(errMsg, storage.ErrReachMaxExtentsCount.Error()) {
		p.ResultCode = proto.OpReachMaxExtentsErr
	} else if strings.Contains(errMsg, storage.ErrClusterForbidWriteOpOfProtoVer.Error()) {
		p.ResultCode = proto.OpWriteOpOfProtoVerForbidden
	} else if strings.Contains(errMsg, storage.ErrVolForbidWriteOpOfProtoVer.Error()) {
		p.ResultCode = proto.OpWriteOpOfProtoVerForbidden
	} else {
		if p.Opcode == proto.OpReadTinyDeleteRecord ||
			(p.Opcode == proto.OpStreamFollowerRead && strings.Contains(errMsg, "timeout")) {
			log.LogWarnf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
		} else {
			log.LogErrorf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
		}
		p.ResultCode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	errorBody := action + "_" + msg
	p.Size = uint32(len(errorBody))
	p.Data = []byte(errorBody)
}

func (p *Packet) ReadFull(c net.Conn, opcode uint8, readSize int) error {
	if p.IsNormalWriteOperation() && readSize == util.BlockSize {
		p.Data, _ = proto.Buffers.Get(readSize)
	} else {
		p.Data = make([]byte, readSize)
	}
	_, err := io.ReadFull(c, p.Data[:readSize])
	return err
}

func (p *Packet) IsMasterCommand() bool {
	switch p.Opcode {
	case
		proto.OpDataNodeHeartbeat,
		proto.OpVersionOperation,
		proto.OpLoadDataPartition,
		proto.OpCreateDataPartition,
		proto.OpDeleteDataPartition,
		proto.OpDecommissionDataPartition,
		proto.OpAddDataPartitionRaftMember,
		proto.OpRemoveDataPartitionRaftMember,
		proto.OpDataPartitionTryToLeader,
		proto.OpRecoverBackupDataReplica,
		proto.OpRecoverBadDisk,
		proto.OpQueryBadDiskRecoverProgress,
		proto.OpDeleteBackupDirectories,
		proto.OpDeleteLostDisk,
		proto.OpReloadDisk:
		return true
	default:
		return false
	}
}

// op need to be processed by dp raft leader.
func (p *Packet) IsUrgentLeaderReq() bool {
	switch p.Opcode {
	case
		proto.OpRandomWrite,
		proto.OpRandomWriteAppend,
		proto.OpRandomWriteVer,
		proto.OpSyncRandomWrite,
		proto.OpStreamRead,
		proto.OpRead:
		return true
	default:
		return false
	}
}

func (p *Packet) IsForwardPacket() bool {
	return p.RemainingFollowers > 0 && !p.isSpecialReplicaCntPacket()
}

func (p *Packet) isSpecialReplicaCntPacket() bool {
	return p.RemainingFollowers == 127
}

func (p *Packet) IsLeaderPacket() bool {
	isLeaderOp := p.IsNormalWriteOperation() || p.IsCreateExtentOperation() || p.IsMarkDeleteExtentOperation()
	return (p.IsForwardPkt() || p.isSpecialReplicaCntPacket()) && isLeaderOp
}

func (p *Packet) IsTinyExtentType() bool {
	return p.ExtentType == proto.TinyExtentType
}

func (p *Packet) IsNormalWriteOperation() bool {
	return p.Opcode == proto.OpWrite || p.Opcode == proto.OpSyncWrite
}

func (p *Packet) IsSnapshotModWriteAppendOperation() bool {
	return p.Opcode == proto.OpRandomWriteAppend || p.Opcode == proto.OpSyncRandomWriteAppend
}

func (p *Packet) IsCreateExtentOperation() bool {
	return p.Opcode == proto.OpCreateExtent
}

func (p *Packet) IsMarkDeleteExtentOperation() bool {
	return p.Opcode == proto.OpMarkDelete || p.Opcode == proto.OpSplitMarkDelete
}

func (p *Packet) IsMarkSplitExtentOperation() bool {
	return p.Opcode == proto.OpSplitMarkDelete
}

func (p *Packet) IsBatchDeleteExtents() bool {
	return p.Opcode == proto.OpBatchDeleteExtent || p.Opcode == proto.OpGcBatchDeleteExtent
}

func (p *Packet) IsBroadcastMinAppliedID() bool {
	return p.Opcode == proto.OpBroadcastMinAppliedID
}

func (p *Packet) IsReadOperation() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpRead ||
		p.Opcode == proto.OpExtentRepairRead || p.Opcode == proto.OpReadTinyDeleteRecord ||
		p.Opcode == proto.OpTinyExtentRepairRead || p.Opcode == proto.OpStreamFollowerRead ||
		p.Opcode == proto.OpBackupRead
}

func (p *Packet) IsRandomWrite() bool {
	return p.Opcode == proto.OpRandomWrite || p.Opcode == proto.OpSyncRandomWrite ||
		p.Opcode == proto.OpRandomWriteVer || p.Opcode == proto.OpSyncRandomWriteVer
}

func (p *Packet) IsSyncWrite() bool {
	return p.Opcode == proto.OpSyncWrite || p.Opcode == proto.OpSyncRandomWrite
}

func (p *Packet) SetDegrade() {
	p.shallDegrade = true
}

func (p *Packet) UnsetDegrade() {
	p.shallDegrade = false
}

func (p *Packet) ShallDegrade() bool {
	return p.shallDegrade
}

func (p *Packet) SetStartT(StartT int64) {
	p.StartT = StartT
}

func (p *Packet) SetData(data []byte) {
	p.Data = data
}

func (p *Packet) SetArglen(len uint32) {
	p.ArgLen = len
}

func (p *Packet) SetArg(data []byte) {
	p.Arg = data
}
