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
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/unit"
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
	TpObject          exporter.TP
	NeedReply         bool
	OrgBuffer         []byte
	OrgSize           int32
	useDataPoolFlag   int64
	usePacketPoolFlag int64
	quorum            int
	dataPoolRefCnt    int32
	packetPoolRefCnt  int32
	errorCh           chan error
	mesg              string
	replSource        string

	elapseStart time.Time // 用于记录Packet完整处理链路的耗时
}

type FollowerPacket struct {
	proto.Packet
	errorCh        chan error
	dataPoolRefCnt *int32
	isUseDataPool  bool
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

func (p *FollowerPacket) DecDataPoolRefCnt() {
	if p.isUseDataPool && atomic.LoadInt32(p.dataPoolRefCnt) > 0 {
		atomic.AddInt32(p.dataPoolRefCnt, -1)
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
	} else if strings.Contains(errMsg, proto.ExtentNotFoundError.Error()) ||
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
	PacketUseDataPool     = 1
	PacketNoUseDataPool   = 0
	PacketUsePacketPool   = 2
	PacketNoUsePacketPool = 0
)

func (p *Packet) ResetElapse() {
	p.elapseStart = time.Now()
}

func (p *Packet) Elapsed() time.Duration {
	return time.Now().Sub(p.elapseStart)
}

func (p *Packet) canPutToDataPool() (can bool) {
	if p.isUseDataPool() && atomic.LoadInt32(&p.dataPoolRefCnt) == 0 {
		return true
	}
	return
}

func (p *Packet) canPutToPacketPool() (can bool) {
	if p.isUsePacketPool() && atomic.LoadInt32(&p.packetPoolRefCnt) == 0 {
		return true
	}
	return
}

func (p *Packet) cleanDataPoolFlag(srcFun string) (isReturnToPool bool) {
	if p.isUseDataPool() && p.canPutToDataPool() {
		atomic.StoreInt64(&p.useDataPoolFlag, PacketNoUseDataPool)
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
		p.dataPoolRefCnt = 0
		p.Arg = nil
		p.followerPackets = nil
		p.OrgBuffer = nil
	}
	return
}

func (p *Packet) forceCleanDataPoolFlag(srcFun string) (isReturnToPool bool) {
	if p.isUseDataPool() {
		atomic.StoreInt64(&p.useDataPoolFlag, PacketNoUseDataPool)
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
		p.dataPoolRefCnt = 0
		p.Arg = nil
		p.followerPackets = nil
		p.OrgBuffer = nil
	}
	return
}

func (p *Packet) cleanPacketPoolFlag(srcFun string) (isReturnToPool bool) {
	if p.isUsePacketPool() && p.canPutToPacketPool() {
		PutPacketToPool(p)
		isReturnToPool = true
	}
	return
}

func (p *Packet) forceCleanPacketPoolFlag(srcFun string) (isReturnToPool bool) {
	if p.isUsePacketPool() {
		PutPacketToPool(p)
		isReturnToPool = true
	}
	return
}

func (p *Packet) addDataPoolRefCnt() {
	if p.isUseDataPool() {
		atomic.AddInt32(&p.dataPoolRefCnt, 1)
	}
}

func (p *Packet) addPacketPoolRefCnt() {
	if p.isUsePacketPool() {
		atomic.AddInt32(&p.packetPoolRefCnt, 1)
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
	dst.dataPoolRefCnt = &src.dataPoolRefCnt
	if src.isUseDataPool() {
		dst.isUseDataPool = true
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
		p.TpObject = exporter.NewModuleTP(fmt.Sprintf("PrimaryBackUp_%v", p.GetOpMsg()))
	} else if p.IsRandomWrite() {
		p.TpObject = exporter.NewModuleTP(fmt.Sprintf("Raft_%v", p.GetOpMsg()))
	}

	return
}

func (p *Packet) DecDataPoolRefCnt() {
	if p.isUseDataPool() {
		if atomic.LoadInt32(&p.dataPoolRefCnt) > 0 {
			atomic.AddInt32(&p.dataPoolRefCnt, -1)
		}
	}
}

func (p *Packet) DecPacketPoolRefCnt() {
	if p.isUsePacketPool() {
		if atomic.LoadInt32(&p.packetPoolRefCnt) > 0 {
			atomic.AddInt32(&p.packetPoolRefCnt, -1)
		}
	}
}

func (p *Packet) isUseDataPool() bool {
	return atomic.LoadInt64(&p.useDataPoolFlag) == PacketUseDataPool
}

func (p *Packet) isUsePacketPool() bool {
	return atomic.LoadInt64(&p.usePacketPoolFlag) == PacketUsePacketPool
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
	atomic.StoreInt64(&p.usePacketPoolFlag, PacketNoUsePacketPool)
	if len(p.followerPackets) != 0 {
		for i := 0; i < len(p.followerPackets); i++ {
			if p.followerPackets[i] != nil {
				p.followerPackets[i].Data = nil
			}
		}
	}
	p.HasPrepare = false
	p.Size = 0
	p.Data = nil
	p.Opcode = 0
	p.PartitionID = 0
	p.ExtentID = 0
	p.ExtentOffset = 0
	p.Magic = proto.ProtoMagic
	p.ExtentType = 0
	p.ResultCode = 0
	p.dataPoolRefCnt = 0
	p.packetPoolRefCnt = 0
	p.RemainingFollowers = 0
	p.CRC = 0
	p.ArgLen = 0
	p.OrgBuffer = nil
	p.KernelOffset = 0
	p.SetCtx(nil)
	p.Arg = nil
	p.OrgSize = 0
	p.followersAddrs = nil
	p.IsReleased = 0
	p.mesg = ""
	p.Object = nil
	p.NeedReply = true
	p.OrgSize = 0
	p.quorum = 0
	p.TpObject = nil
	p.errorCh = nil
	p.Data = nil
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
	p.StartT = time.Now().UnixNano()
	p.usePacketPoolFlag = PacketUsePacketPool
	if p.PoolFlag == 0 {
		p.PoolFlag = proto.GenerateRequestID()
	}
	p.NeedReply = true
	return
}

func (p *Packet) GetFollowers() []string {
	return p.followersAddrs
}

func NewPacket(ctx context.Context) (p *Packet) {
	p = new(Packet)
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

func NewExtentRepairReadPacket(ctx context.Context, partitionID uint64, extentID uint64, offset, size int, force bool) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	if force {
		p.Arg = []byte{1}
		p.ArgLen = uint32(len(p.Arg))
	}
	p.Size = uint32(size)
	p.Opcode = proto.OpExtentRepairRead
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)

	return
}

func NewTinyExtentRepairReadPacket(ctx context.Context, partitionID uint64, extentID uint64, offset, size int, force bool) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	if force {
		p.Arg = []byte{1}
		p.ArgLen = uint32(len(p.Arg))
	}
	p.Size = uint32(size)
	p.Opcode = proto.OpTinyExtentRepairRead
	p.ExtentType = proto.TinyExtentType
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)

	return
}

func NewPacketToReadEcTinyDeleteRecord(ctx context.Context, partitionID uint64, offset int64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpEcTinyDelInfoRead
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentOffset = offset
	p.SetCtx(ctx)

	return
}

func NewExtentStripeRead(partitionID, extentID, offset, size uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpEcRead
	p.ReqID = proto.GenerateRequestID()
	p.StartT = time.Now().UnixNano()

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
	} else if strings.Contains(errMsg, proto.ExtentNotFoundError.Error()) ||
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
	p.ArgLen = 0
}

func (p *Packet) ReadFromConnFromCli(c net.Conn, deadlineSonds int64) (isUseBufferPool bool, err error) {
	if deadlineSonds != proto.NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Duration(deadlineSonds) * time.Second))
	} else {
		c.SetReadDeadline(time.Time{})
	}
	header, err := proto.Buffers.Get(unit.PacketHeaderSize)
	if err != nil {
		header = make([]byte, unit.PacketHeaderSize)
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
	return p.allocateBufferFromPoolForReadConnnectBody(c)
}

func (p *Packet) allocateBufferFromPoolForReadConnnectBody(c net.Conn) (isUseBufferPool bool, err error) {
	readSize := p.Size
	if p.IsReadOperation() && p.ResultCode == proto.OpInitResultCode {
		readSize = 0
		return
	}
	p.OrgSize = int32(readSize)
	if p.IsWriteOperation() && readSize <= unit.BlockSize {
		p.Data, _ = proto.Buffers.Get(unit.BlockSize)
		_, err = io.ReadFull(c, p.Data[:readSize])
		atomic.StoreInt64(&p.useDataPoolFlag, PacketUseDataPool)
		isUseBufferPool = true
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
		proto.OpPromoteDataPartitionRaftLearner,
		proto.OpEcNodeHeartbeat,
		proto.OpCreateEcDataPartition,
		proto.OpChangeEcPartitionMembers,
		proto.OpDeleteEcDataPartition:
		return true
	}
	return false
}

func (p *Packet) IsForwardPacket() bool {
	r := p.RemainingFollowers > 0
	return r
}

// A leader packet is the packet send to the leader and does not require packet forwarding.
func (p *Packet) IsLeaderPacket() (ok bool) {
	return p.RemainingFollowers > 0
}

func (p *Packet) IsTinyExtentType() bool {
	return p.ExtentType == proto.TinyExtentType
}

func (p *Packet) IsWriteOperation() bool {
	return p.Opcode == proto.OpWrite || p.Opcode == proto.OpSyncWrite ||
		p.Opcode == proto.OpEcWrite || p.Opcode == proto.OpSyncEcWrite
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
		p.Opcode == proto.OpTinyExtentAvaliRead || p.Opcode == proto.OpEcTinyRepairRead
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
