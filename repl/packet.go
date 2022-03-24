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
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/util/tracing"

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
	followersAddrs    []string
	followerPackets   []*FollowerPacket
	IsReleased        int32 // TODO what is released?
	Object            interface{}
	TpObject          *exporter.TimePointCount
	NeedReply         bool
	OrgBuffer         []byte
	OrgSize           int32
	useBufferPoolFlag int64
	quorum            int
	refCnt            int32
	errorCh           chan error
	IsFromPool        bool
}

type FollowerPacket struct {
	proto.Packet
	errorCh             chan error
	refCnt              *int32
	isUseBufferFromPool bool
}

func NewFollowerPacket(ctx context.Context, parent *Packet) (fp *FollowerPacket) {
	fp = new(FollowerPacket)
	fp.errorCh = parent.errorCh
	fp.StartT = time.Now().UnixNano()
	fp.SetCtx(ctx)
	return fp
}

func (p *FollowerPacket) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}

func (p *FollowerPacket) DecRefCnt() {
	if p.isUseBufferFromPool && atomic.LoadInt32(p.refCnt) > 0 {
		atomic.AddInt32(p.refCnt, -1)
	}
}

func (p *FollowerPacket) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk && p.ResultCode != proto.OpInitResultCode
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
	if p.TpObject != nil {
		p.TpObject.Set(nil)
	}

	return
}

const (
	PacketUseBufferPool   = 1
	PacketNoUseBufferPool = 0
)

func (p *Packet) canPutToBufferPool() (can bool) {
	if p.isUseBufferPool() && atomic.LoadInt32(&p.refCnt) == 0 {
		return true
	}
	return
}

func (p *Packet) clean() (isReturnToPool bool) {
	if p.isUseBufferPool() && p.canPutToBufferPool() {
		if len(p.followerPackets) != 0 {
			for i := 0; i < len(p.followerPackets); i++ {
				if p.followerPackets[i] != nil {
					p.followerPackets[i].Data = nil
				}
			}
		}
		proto.Buffers.Put(p.OrgBuffer)
		isReturnToPool = true
		p.Object = nil
		p.TpObject = nil
		p.Data = nil
		p.Arg = nil
		p.followerPackets = nil
		p.OrgBuffer = nil
	}
	atomic.StoreInt64(&p.useBufferPoolFlag, PacketNoUseBufferPool)
	return
}

func (p *Packet) forceClean() (isReturnToPool bool) {
	if p.isUseBufferPool() {
		if len(p.followerPackets) != 0 {
			for i := 0; i < len(p.followerPackets); i++ {
				if p.followerPackets[i] != nil {
					p.followerPackets[i].Data = nil
				}
			}
		}
		proto.Buffers.Put(p.OrgBuffer)
		isReturnToPool = true
		p.Object = nil
		p.TpObject = nil
		p.Data = nil
		p.Arg = nil
		p.followerPackets = nil
		p.OrgBuffer = nil
	}
	atomic.StoreInt64(&p.useBufferPoolFlag, PacketNoUseBufferPool)
	return
}

func (p *Packet) addRefCnt() {
	if p.isUseBufferPool() {
		atomic.AddInt32(&p.refCnt, 1)
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
	dst.refCnt = &src.refCnt
	if src.isUseBufferPool() {
		dst.isUseBufferFromPool = true
	}
}

func copyFollowerPacket(src *FollowerPacket, dst *FollowerPacket) {
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
}

func copyReplPacket(src *Packet, dst *Packet) {
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
}

func (p *Packet) BeforeTp(clusterID string) (ok bool) {
	if p.IsForwardPkt() && !p.IsRandomWrite() {
		p.TpObject = exporter.NewTPCnt(fmt.Sprintf("PrimaryBackUp_%v", p.GetOpMsg()))
	} else if p.IsRandomWrite() {
		p.TpObject = exporter.NewTPCnt(fmt.Sprintf("Raft_%v", p.GetOpMsg()))
	}

	return
}

func (p *Packet) DecRefCnt() {
	if atomic.LoadInt64(&p.useBufferPoolFlag) == PacketUseBufferPool {
		if atomic.LoadInt32(&p.refCnt) > 0 {
			atomic.AddInt32(&p.refCnt, -1)
		}
	}
}

func (p *Packet) isUseBufferPool() bool {
	return atomic.LoadInt64(&p.useBufferPoolFlag) == PacketUseBufferPool
}

func (p *Packet) resolveFollowersAddr(remoteAddr string) (err error) {
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionPreparePkt, err.Error())
			log.LogErrorf("action[%v]  packet(%v) from remote(%v) error(%v)",
				ActionPreparePkt, p.GetUniqueLogId(), remoteAddr, err.Error())
		}
	}()
	if len(p.Arg) < int(p.ArgLen) {
		err = ErrArgLenMismatch
		return
	}
	p.followersAddrs, p.quorum = DecodeReplPacketArg(p.Arg[:int(p.ArgLen)])
	p.followerPackets = make([]*FollowerPacket, len(p.followersAddrs))
	if p.RemainingFollowers < 0 {
		err = ErrBadNodes
		return
	}
	if p.isUseBufferPool() {
		p.addRefCnt()
	}

	return
}

const (
	PacketPoolCnt = 64
)

var (
	PacketPool [PacketPoolCnt]*sync.Pool
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < PacketPoolCnt; i++ {
		PacketPool[i] = &sync.Pool{New: func() interface{} {
			return new(Packet)
		}}
	}
}

func PutPacketToPool(p *Packet) {
	if !p.IsFromPool {
		return
	}
	p.Size = 0
	p.Data = nil
	p.Opcode = 0
	p.PartitionID = 0
	p.ExtentID = 0
	p.ExtentOffset = 0
	p.Magic = proto.ProtoMagic
	p.ExtentType = 0
	p.ResultCode = 0
	p.refCnt = 0
	p.RemainingFollowers = 0
	p.CRC = 0
	p.ArgLen = 0
	p.KernelOffset = 0
	p.SetCtx(nil)
	p.ReqID = 0
	p.Arg = nil
	p.Data = nil
	p.HasPrepare = false
	p.StartT = time.Now().UnixNano()
	p.WaitT = time.Now().UnixNano()
	p.SendT = time.Now().UnixNano()
	p.RecvT = time.Now().UnixNano()
	index := rand.Intn(PacketPoolCnt)
	PacketPool[index].Put(p)
}

func GetPacketFromPool() (p *Packet) {
	index := rand.Intn(PacketPoolCnt)
	p = PacketPool[index].Get().(*Packet)
	p.IsFromPool = true
	p.StartT = time.Now().UnixNano()
	p.NeedReply = true
	return
}

func NewPacket(ctx context.Context) (p *Packet) {
	p = new(Packet)
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	p.NeedReply = true
	p.SetCtx(ctx)
	return
}

func NewPacketFromPool(ctx context.Context) (p *Packet) {
	p = GetPacketFromPool()
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	p.NeedReply = true
	p.SetCtx(ctx)
	return
}

func NewPacketToGetAllWatermarks(ctx context.Context, partitionID uint64, extentType uint8) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAllWatermarks
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentType = extentType
	p.SetCtx(ctx)

	return
}

func NewPacketToGetAllWatermarksV2(ctx context.Context, partitionID uint64, extentType uint8) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAllWatermarksV2
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentType = extentType
	p.SetCtx(ctx)
	return
}

func NewPacketToReadTinyDeleteRecord(ctx context.Context, partitionID uint64, offset int64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpReadTinyDeleteRecord
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentOffset = offset
	p.SetCtx(ctx)

	return
}

func NewReadTinyDeleteRecordResponsePacket(ctx context.Context, requestID int64, partitionID uint64) (p *Packet) {
	p = new(Packet)
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpOk
	p.ReqID = requestID
	p.ExtentType = proto.NormalExtentType
	p.SetCtx(ctx)

	return
}

func NewExtentRepairReadPacket(ctx context.Context, partitionID uint64, extentID uint64, offset, size int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpExtentRepairRead
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)

	return
}

func NewTinyExtentRepairReadPacket(ctx context.Context, partitionID uint64, extentID uint64, offset, size int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpTinyExtentRepairRead
	p.ExtentType = proto.TinyExtentType
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)

	return
}

func NewTinyExtentStreamReadResponsePacket(ctx context.Context, requestID int64, partitionID uint64, extentID uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpTinyExtentRepairRead
	p.ReqID = requestID
	p.ExtentType = proto.TinyExtentType
	p.StartT = time.Now().UnixNano()
	p.SetCtx(ctx)

	return
}

func NewStreamReadResponsePacket(ctx context.Context, requestID int64, partitionID uint64, extentID uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpOk
	p.ReqID = requestID
	p.ExtentType = proto.NormalExtentType
	p.SetCtx(ctx)

	return
}

func NewPacketToNotifyExtentRepair(ctx context.Context, partitionID uint64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpNotifyReplicasToRepair
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)

	return
}

func (p *Packet) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk && p.ResultCode != proto.OpInitResultCode
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
	} else if strings.Contains(errMsg, proto.ErrOperationDisabled.Error()) {
		p.ResultCode = proto.OpDisabled
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

func (p *Packet) ReadFromConnFromCli(c net.Conn, deadlineSonds int64) (isUseBufferPool bool, err error) {
	if deadlineSonds != proto.NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Duration(deadlineSonds) * time.Second))
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

	var tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("repl.Packet.ReadFromConn[Decode Packet]")
	defer func() {
		tracer.SetTag("conn.remote", c.RemoteAddr().String())
		tracer.SetTag("conn.local", c.LocalAddr().String())
		tracer.SetTag("ReqID", p.GetReqID())
		tracer.SetTag("ReqOp", p.GetOpMsg())
		tracer.SetTag("Arg", string(p.Arg))
		tracer.SetTag("ret.err", err)
		tracer.Finish()
	}()
	p.SetCtx(tracer.Context())

	if p.ArgLen > 0 {
		if err = proto.ReadFull(c, &p.Arg, int(p.ArgLen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	return p.allocateBufferFromPoolForReadConnnectBody(c)
}

func (p *Packet) allocateBufferFromPoolForReadConnnectBody(c net.Conn) (isUseBufferPool bool, err error) {
	readSize := p.Size
	if p.IsReadOperation() && p.ResultCode == proto.OpInitResultCode {
		readSize = 0
		return
	}
	p.OrgSize = int32(readSize)
	if p.IsWriteOperation() && readSize <= util.BlockSize {
		p.Data, _ = proto.Buffers.Get(util.BlockSize)
		atomic.StoreInt64(&p.useBufferPoolFlag, PacketUseBufferPool)
		_, err = io.ReadFull(c, p.Data[:readSize])
		if err != nil {
			proto.Buffers.Put(p.Data)
			return
		}
		isUseBufferPool = true
	} else if p.IsRandomWriteV3() {
		needDataSize := uint32(readSize) + proto.RandomWriteRaftLogV3HeaderSize
		if needDataSize <= util.BlockSize {
			p.Data, _ = proto.Buffers.Get(util.BlockSize)
			atomic.StoreInt64(&p.useBufferPoolFlag, PacketUseBufferPool)
			isUseBufferPool = true
		} else {
			p.Data = make([]byte, uint32(readSize)+proto.RandomWriteRaftLogV3HeaderSize)
		}
		_, err = io.ReadFull(c, p.Data[proto.RandomWriteRaftLogV3HeaderSize:p.Size+proto.RandomWriteRaftLogV3HeaderSize])
	} else {
		p.Data = make([]byte, readSize)
		_, err = io.ReadFull(c, p.Data[:readSize])
	}
	return
}

func (p *Packet) IsMasterCommand() bool {
	switch p.Opcode {
	case
		proto.OpDataNodeHeartbeat,
		proto.OpLoadDataPartition,
		proto.OpCreateDataPartition,
		proto.OpDeleteDataPartition,
		proto.OpDecommissionDataPartition,
		proto.OpAddDataPartitionRaftMember,
		proto.OpRemoveDataPartitionRaftMember,
		proto.OpDataPartitionTryToLeader,
		proto.OpSyncDataPartitionReplicas,
		proto.OpAddDataPartitionRaftLearner,
		proto.OpPromoteDataPartitionRaftLearner:
		return true
	}
	return false
}

// A leader packet is the packet send to the leader and does not require packet forwarding.
func (p *Packet) IsLeaderPacket() (ok bool) {
	if p.RemainingFollowers > 0 && (p.IsWriteOperation() || p.IsCreateExtentOperation() || p.IsMarkDeleteExtentOperation()) {
		ok = true
	}

	return
}

func (p *Packet) IsTinyExtentType() bool {
	return p.ExtentType == proto.TinyExtentType
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

func (p *Packet) IsBroadcastMinAppliedID() bool {
	return p.Opcode == proto.OpBroadcastMinAppliedID
}

func (p *Packet) IsReadOperation() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpRead ||
		p.Opcode == proto.OpExtentRepairRead || p.Opcode == proto.OpReadTinyDeleteRecord ||
		p.Opcode == proto.OpTinyExtentRepairRead || p.Opcode == proto.OpStreamFollowerRead ||
		p.Opcode == proto.OpTinyExtentAvaliRead
}

func (p *Packet) IsRandomWrite() bool {
	return p.Opcode == proto.OpRandomWrite || p.Opcode == proto.OpSyncRandomWrite ||
		p.Opcode == proto.OpRandomWriteV3 || p.Opcode == proto.OpSyncRandomWriteV3
}

func (p *Packet) IsRandomWriteV3() bool {
	return p.Opcode == proto.OpRandomWriteV3 || p.Opcode == proto.OpSyncRandomWriteV3
}

func (p *Packet) IsSyncWrite() bool {
	return p.Opcode == proto.OpSyncWrite || p.Opcode == proto.OpSyncRandomWrite
}
