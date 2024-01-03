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

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
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

func NewFollowerPacket() (fp *FollowerPacket) {
	fp = new(FollowerPacket)
	fp.respCh = make(chan error, 1)
	fp.StartT = time.Now().UnixNano()
	return fp
}

func (p *FollowerPacket) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}

func (p *FollowerPacket) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk && p.ResultCode != proto.OpInitResultCode
}

func (p *FollowerPacket) identificationErrorResultCode(errLog string, errMsg string) {
	if strings.Contains(errLog, ActionReceiveFromFollower) || strings.Contains(errLog, ActionSendToFollowers) ||
		strings.Contains(errLog, ConnIsNullErr) {
		p.ResultCode = proto.OpIntraGroupNetErr
		log.LogErrorf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
	} else if strings.Contains(errMsg, storage.ParameterMismatchError.Error()) ||
		strings.Contains(errMsg, ErrorUnknownOp.Error()) {
		p.ResultCode = proto.OpArgMismatchErr
	} else if strings.Contains(errMsg, proto.ErrDataPartitionNotExists.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, storage.ExtentNotFoundError.Error()) ||
		strings.Contains(errMsg, storage.ExtentHasBeenDeletedError.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.NoSpaceError.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.TryAgainError.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, raft.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, raft.ErrStopped.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
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
	str := string(p.Arg[:int(p.ArgLen)])
	followerAddrs := strings.SplitN(str, proto.AddrSplit, -1)
	followerNum := uint8(len(followerAddrs) - 1)
	p.followersAddrs = make([]string, followerNum)
	p.followerPackets = make([]*FollowerPacket, followerNum)
	p.OrgBuffer = p.Data
	if followerNum > 0 {
		p.followersAddrs = followerAddrs[:int(followerNum)]
		log.LogInfof("action[resolveFollowersAddr] %v", p.followersAddrs)
	}
	if p.RemainingFollowers < 0 {
		err = ErrBadNodes
		return
	}

	return
}

func NewPacketEx() (p PacketInterface) {
	pr := new(Packet)
	pr.Magic = proto.ProtoMagic
	pr.StartT = time.Now().UnixNano()
	pr.NeedReply = true
	return pr
}

func NewPacket() (p *Packet) {
	p = new(Packet)
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	p.NeedReply = true
	return
}

func NewPacketToGetAllWatermarks(partitionID uint64, extentType uint8) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAllWatermarks
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentType = extentType

	return
}

func NewPacketToReadTinyDeleteRecord(partitionID uint64, offset int64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpReadTinyDeleteRecord
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentOffset = offset

	return
}

func NewReadTinyDeleteRecordResponsePacket(requestID int64, partitionID uint64) (p *Packet) {
	p = new(Packet)
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpOk
	p.ReqID = requestID
	p.ExtentType = proto.NormalExtentType

	return
}

type (
	MakeStreamReadResponsePacket func(requestID int64, partitionID uint64, extentID uint64) (p PacketInterface)
	MakeExtentRepairReadPacket   func(partitionID uint64, extentID uint64, offset, size int) (p PacketInterface)
)

func NewExtentRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) (p PacketInterface) {
	pr := new(Packet)
	pr.ExtentID = extentID
	pr.PartitionID = partitionID
	pr.Magic = proto.ProtoMagic
	pr.ExtentOffset = int64(offset)
	pr.Size = uint32(size)
	pr.Opcode = proto.OpExtentRepairRead
	pr.ExtentType = proto.NormalExtentType
	pr.ReqID = proto.GenerateRequestID()

	return pr
}

func NewTinyExtentRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) (p PacketInterface) {
	pr := new(Packet)
	pr.ExtentID = extentID
	pr.PartitionID = partitionID
	pr.Magic = proto.ProtoMagic
	pr.ExtentOffset = int64(offset)
	pr.Size = uint32(size)
	pr.Opcode = proto.OpTinyExtentRepairRead
	pr.ExtentType = proto.TinyExtentType
	pr.ReqID = proto.GenerateRequestID()

	return pr
}

func NewTinyExtentStreamReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpTinyExtentRepairRead
	p.ReqID = requestID
	p.ExtentType = proto.TinyExtentType
	p.StartT = time.Now().UnixNano()

	return
}

func NewNormalExtentWithHoleRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) (p PacketInterface) {
	pr := new(Packet)
	pr.ExtentID = extentID
	pr.PartitionID = partitionID
	pr.Magic = proto.ProtoMagic
	pr.ExtentOffset = int64(offset)
	pr.Size = uint32(size)
	pr.Opcode = proto.OpSnapshotExtentRepairRead
	pr.ExtentType = proto.TinyExtentType
	pr.ReqID = proto.GenerateRequestID()
	p = pr

	return
}

func NewNormalExtentWithHoleStreamReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpSnapshotExtentRepairRsp
	p.ReqID = requestID
	p.ExtentType = proto.NormalExtentType
	p.StartT = time.Now().UnixNano()

	return
}

func NewStreamReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) (p PacketInterface) {
	pr := new(Packet)
	pr.ExtentID = extentID
	pr.PartitionID = partitionID
	pr.Magic = proto.ProtoMagic
	pr.Opcode = proto.OpOk
	pr.ReqID = requestID
	pr.ExtentType = proto.NormalExtentType
	return pr
}

func NewPacketToNotifyExtentRepair(partitionID uint64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpNotifyReplicasToRepair
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()

	return
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

func (p *Packet) getErrMessage() (m string) {
	return fmt.Sprintf("req(%v) err(%v)", p.GetUniqueLogId(), string(p.Data[:p.Size]))
}

var ErrorUnknownOp = errors.New("unknown opcode")

func (p *Packet) identificationErrorResultCode(errLog string, errMsg string) {
	log.LogDebugf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
	if strings.Contains(errLog, ActionReceiveFromFollower) || strings.Contains(errLog, ActionSendToFollowers) ||
		strings.Contains(errLog, ConnIsNullErr) {
		p.ResultCode = proto.OpIntraGroupNetErr
	} else if strings.Contains(errMsg, storage.ParameterMismatchError.Error()) ||
		strings.Contains(errMsg, ErrorUnknownOp.Error()) {
		p.ResultCode = proto.OpArgMismatchErr
	} else if strings.Contains(errMsg, proto.ErrDataPartitionNotExists.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, storage.ExtentNotFoundError.Error()) ||
		strings.Contains(errMsg, storage.ExtentHasBeenDeletedError.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.NoSpaceError.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.BrokenDiskError.Error()) {
		p.ResultCode = proto.OpDiskErr
	} else if strings.Contains(errMsg, storage.TryAgainError.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, raft.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, raft.ErrStopped.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else if strings.Contains(errMsg, storage.VerNotConsistentError.Error()) {
		p.ResultCode = proto.ErrCodeVersionOpError
		// log.LogDebugf("action[identificationErrorResultCode] not change ver erro code, (%v)", string(debug.Stack()))
	} else if strings.Contains(errMsg, storage.NoDiskReadRepairExtentTokenError.Error()) {
		p.ResultCode = proto.OpReadRepairExtentAgain
	} else if strings.Contains(errMsg, storage.ForbiddenDataPartitionError.Error()) {
		p.ResultCode = proto.OpForbidErr
	} else {
		log.LogErrorf("action[identificationErrorResultCode] error %v, errmsg %v", errLog, errMsg)
		p.ResultCode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}

func (p *Packet) ReadFull(c net.Conn, opcode uint8, readSize int) (err error) {
	if p.IsNormalWriteOperation() && readSize == util.BlockSize {
		p.Data, _ = proto.Buffers.Get(readSize)
	} else {
		p.Data = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, p.Data[:readSize])
	return
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
		proto.OpDataPartitionTryToLeader:
		return true
	default:
		return false
	}
}

func (p *Packet) IsForwardPacket() bool {
	r := p.RemainingFollowers > 0 && !p.isSpecialReplicaCntPacket()
	return r
}

func (p *Packet) isSpecialReplicaCntPacket() bool {
	r := p.RemainingFollowers == 127
	return r
}

// A leader packet is the packet send to the leader and does not require packet forwarding.
func (p *Packet) IsLeaderPacket() (ok bool) {
	if (p.IsForwardPkt() || p.isSpecialReplicaCntPacket()) &&
		(p.IsNormalWriteOperation() || p.IsCreateExtentOperation() || p.IsMarkDeleteExtentOperation()) {
		ok = true
	}

	return
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
	return p.Opcode == proto.OpBatchDeleteExtent
}

func (p *Packet) IsBroadcastMinAppliedID() bool {
	return p.Opcode == proto.OpBroadcastMinAppliedID
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
