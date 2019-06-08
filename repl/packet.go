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

package repl

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/tiglabs/raft"
)

var (
	ErrBadNodes       = errors.New("BadNodesErr")
	ErrArgLenMismatch = errors.New("ArgLenMismatchErr")
)

type Packet struct {
	proto.Packet
	followersAddrs  []string
	followerPackets []*FollowerPacket
	IsReleased      int32 // TODO what is released?
	Object          interface{}
	TpObject        *exporter.TimePointCount
	NeedReply       bool
	OrgBuffer       []byte
}

type FollowerPacket struct {
	proto.Packet
	respCh chan error
}

func NewFollowerPacket() (fp *FollowerPacket) {
	fp = new(FollowerPacket)
	fp.respCh = make(chan error, 1)
	return fp
}

func (p *FollowerPacket) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}

func (p *FollowerPacket) identificationErrorResultCode(errLog string, errMsg string) {
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
	} else if strings.Contains(errMsg, storage.TryAgainError.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, raft.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else {
		p.ResultCode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) AfterTp() (ok bool) {
	p.TpObject.Set(nil)

	return
}

func (p *Packet) clean() {
	if p.Data == nil {
		return
	}
	p.Object = nil
	p.TpObject = nil
	p.Data = nil
	p.Arg = nil
	if p.OrgBuffer != nil && len(p.OrgBuffer) == util.BlockSize && p.IsWriteOperation() {
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
	p.TpObject = exporter.NewTPCnt(p.GetOpMsg())
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
	}
	if p.RemainingFollowers < 0 {
		err = ErrBadNodes
		return
	}

	return
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

func NewPacketToTinyDeleteRecord(partitionID uint64, offset int64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpReadTinyDelete
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentOffset = offset

	return
}

func NewTinyDeleteRecordResponsePacket(requestID int64, partitionID uint64) (p *Packet) {
	p = new(Packet)
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpOk
	p.ReqID = requestID
	p.ExtentType = proto.NormalExtentType

	return
}

func NewExtentRepairReadPacket(partitionID uint64, extentID uint64, offset, size int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpExtentRepairRead
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()

	return
}

func NewStreamReadResponsePacket(requestID int64, partitionID uint64, extentID uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpOk
	p.ReqID = requestID
	p.ExtentType = proto.NormalExtentType

	return
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

func (p *Packet) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk
}

func (p *Packet) getErrMessage() (m string) {
	return fmt.Sprintf("req(%v) err(%v)", p.GetUniqueLogId(), string(p.Data[:p.Size]))
}

var (
	ErrorUnknownOp = errors.New("unknown opcode")
)

func (p *Packet) identificationErrorResultCode(errLog string, errMsg string) {
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
	} else if strings.Contains(errMsg, storage.TryAgainError.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, raft.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpTryOtherAddr
	} else {
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
	if p.IsWriteOperation() && readSize == util.BlockSize {
		p.Data, _ = proto.Buffers.Get(readSize)
	} else {
		p.Data = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, p.Data[:readSize])
	return
}

func (p *Packet) isReadOperation() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpRead || p.Opcode == proto.OpExtentRepairRead
}

func (p *Packet) ReadFromConnFromCli(c net.Conn, deadlineTime time.Duration) (err error) {
	if deadlineTime != proto.NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(deadlineTime * time.Second))
	} else {
		c.SetReadDeadline(time.Time{})
	}
	header, err := proto.Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer proto.Buffers.Put(header)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.ArgLen > 0 {
		if err = proto.ReadFull(c, &p.Arg, int(p.ArgLen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	if p.IsReadExtentOperation() && p.ResultCode == proto.OpInitResultCode {
		size = 0
	}
	return p.ReadFull(c, p.Opcode, int(size))
}

// A leader packet is the packet send to the leader and does not require packet forwarding.
func (p *Packet) IsLeaderPacket() (ok bool) {
	if p.IsForwardPkt() && p.IsWriteOperation() || p.IsCreateExtentOperation() || p.IsMarkDeleteExtentOperation() {
		ok = true
	}

	return
}

func (p *Packet) IsWriteOperation() bool {
	return p.Opcode == proto.OpWrite || p.Opcode == proto.OpSyncWrite
}

func (p *Packet) IsCreateExtentOperation() bool {
	return p.Opcode == proto.OpCreateExtent
}

func (p *Packet) IsMarkDeleteExtentOperation() bool {
	return p.Opcode == proto.OpMarkDelete
}

func (p *Packet) IsTinyExtentType() bool {
	return p.ExtentType == proto.TinyExtentType
}

func (p *Packet) IsReadExtentOperation() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpExtentRepairRead || p.Opcode == proto.OpRead || p.Opcode == proto.OpReadTinyDelete
}

func (p *Packet) IsRandomWriteOption() bool {
	return p.Opcode == proto.OpRandomWrite || p.Opcode == proto.OpSyncRandomWrite
}

func (p *Packet) IsMasterCommand() bool {
	switch p.Opcode {
	case
		proto.OpDataNodeHeartbeat,
		proto.OpLoadDataPartition,
		proto.OpCreateDataPartition,
		proto.OpDeleteDataPartition,
		proto.OpDecommissionDataPartition:
		return true
	}
	return false
}

func (p *Packet) IsForwardPacket() bool {
	r := p.RemainingFollowers > 0
	return r
}
