// Copyright 2018 The Containerfs Authors.
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

package datanode

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"time"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/ump"
)

var (
	ErrBadNodes           = errors.New("BadNodesErr")
	ErrArgLenMismatch     = errors.New("ArgLenMismatchErr")
	ErrAddrsNodesMismatch = errors.New("AddrsNodesMismatchErr")
)

const (
	HasReturnToStore = 1
)

type Packet struct {
	proto.Packet
	replicateConns            []*net.TCPConn
	replicateAddrs            []string
	isRelaseTinyExtentToStore int32
	partition                 DataPartition
	replicateNum              uint8
	tpObject                  *ump.TpObject
}

func (p *Packet) afterTp() (ok bool) {
	var err error
	if p.isErrPack() {
		err = fmt.Errorf(p.GetOpMsg()+" failed because(%v)", string(p.Data[:p.Size]))
	}
	ump.AfterTP(p.tpObject, err)

	return
}

func (p *Packet) beforeTp(clusterId string) (ok bool) {
	umpKey := fmt.Sprintf("%s_datanode_stream%v", clusterId, p.GetOpMsg())
	p.tpObject = ump.BeforeTP(umpKey)
	return
}

const (
	ForceCloseConnect = true
	NoCloseConnect    = false
)

func (p *Packet) resolveReplicateAddrs() (err error) {
	if len(p.Arg) < int(p.Arglen) {
		return ErrArgLenMismatch
	}
	str := string(p.Arg[:int(p.Arglen)])
	replicateAddrs := strings.SplitN(str, proto.AddrSplit, -1)
	p.replicateNum = uint8(len(replicateAddrs) - 1)
	p.replicateAddrs = make([]string, p.replicateNum)
	p.replicateConns = make([]*net.TCPConn, p.replicateNum)
	if p.replicateNum > 0 {
		p.replicateAddrs = replicateAddrs[:int(p.replicateNum)]
	}
	if p.RemainReplicates < 0 {
		err = ErrBadNodes
		return
	}

	return
}

func (p *Packet) forceDestoryAllConnect() {
	for i := 0; i < len(p.replicateConns); i++ {
		gConnPool.PutConnect(p.replicateConns[i], ForceCloseConnect)
	}
}

func (p *Packet) forceDestoryCheckUsedClosedConnect(err error) {
	for i := 0; i < len(p.replicateConns); i++ {
		gConnPool.CheckErrorForceClose(p.replicateConns[i], p.replicateAddrs[i], err)
	}
}

func (p *Packet) PutConnectsToPool() {
	for i := 0; i < len(p.replicateConns); i++ {
		gConnPool.PutConnect(p.replicateConns[i], NoCloseConnect)
	}
}

func NewPacket() (p *Packet) {
	p = new(Packet)
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	return
}

func (p *Packet) IsMasterCommand() bool {
	switch p.Opcode {
	case
		proto.OpDataNodeHeartbeat,
		proto.OpLoadDataPartition,
		proto.OpCreateDataPartition,
		proto.OpDeleteDataPartition,
		proto.OpOfflineDataPartition:
		return true
	}
	return false
}

func (p *Packet) isForwardPacket() bool {
	r := p.RemainReplicates > 0
	return r
}

func (p *Packet) checkCrc() (err error) {
	if !p.isWriteOperation() {
		return
	}

	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc == p.CRC {
		return
	}
	return storage.ErrPkgCrcMismatch
}

func NewGetAllWaterMarker(partitionId uint32, extentType uint8) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAllWaterMark
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GeneratorRequestID()
	p.StoreMode = extentType

	return
}

func NewExtentRepairReadPacket(partitionId uint32, extentId uint64, offset, size int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentId
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpExtentRepairRead
	p.StoreMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()

	return
}

func NewStreamReadResponsePacket(requestId int64, partitionId uint32, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentId
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpOk
	p.ReqID = requestId
	p.StoreMode = proto.NormalExtentMode

	return
}

func NewNotifyExtentRepair(partitionId uint32) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpNotifyExtentRepair
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.StoreMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()

	return
}

func (p *Packet) isWriteOperation() bool {
	return p.Opcode == proto.OpWrite
}

func (p *Packet) isCreateExtentOperation() bool {
	return p.Opcode == proto.OpCreateExtent
}

func (p *Packet) isMarkDeleteExtentOperation() bool {
	return p.Opcode == proto.OpMarkDelete
}

func (p *Packet) isReadOperation() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpRead || p.Opcode == proto.OpExtentRepairRead
}

func (p *Packet) isLeaderPacket() (ok bool) {
	if p.replicateNum == p.RemainReplicates && (p.isWriteOperation() || p.isCreateExtentOperation() || p.isMarkDeleteExtentOperation()) {
		ok = true
	}

	return
}

func (p *Packet) isErrPack() bool {
	return p.ResultCode != proto.OpOk
}

func (p *Packet) getErrMessage() (m string) {
	return fmt.Sprintf("req(%v) err(%v)", p.GetUniqueLogId(), string(p.Data[:p.Size]))
}

func (p *Packet) identificationErrorResultCode(errLog string, errMsg string) {
	if strings.Contains(errLog, ActionReceiveFromNext) || strings.Contains(errLog, ActionSendToNext) ||
		strings.Contains(errLog, ConnIsNullErr) || strings.Contains(errLog, ActionCheckAndAddInfos) {
		p.ResultCode = proto.OpIntraGroupNetErr
		return
	}

	if strings.Contains(errMsg, storage.ErrorParamMismatch.Error()) ||
		strings.Contains(errMsg, ErrorUnknownOp.Error()) {
		p.ResultCode = proto.OpArgMismatchErr
	} else if strings.Contains(errMsg, storage.ErrorExtentNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrorExtentHasDelete.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.ErrSyscallNoSpace.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrorAgain.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, storage.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpNotLeaderErr
	} else if strings.Contains(errMsg, storage.ErrorExtentNotFound.Error()) {
		if p.Opcode != proto.OpWrite {
			p.ResultCode = proto.OpNotExistErr
		} else {
			p.ResultCode = proto.OpIntraGroupNetErr
		}
	} else {
		p.ResultCode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	if p.ResultCode == proto.OpDiskNoSpaceErr || p.ResultCode == proto.OpDiskErr {
		p.ResultCode = proto.OpIntraGroupNetErr
	}
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}

func (p *Packet) ReadFull(c net.Conn, readSize int) (err error) {
	if p.Opcode == proto.OpWrite && readSize == util.BlockSize {
		p.Data, _ = proto.Buffers.Get(util.BlockSize)
	} else {
		p.Data = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, p.Data[:readSize])
	return
}

func (p *Packet) ReadFromConnFromCli(c net.Conn, deadlineTime time.Duration) (err error) {
	if deadlineTime != proto.NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(deadlineTime * time.Second))
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

	if p.Arglen > 0 {
		if err = proto.ReadFull(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	if p.isReadOperation() && p.ResultCode == proto.OpInitResultCode {
		size = 0
	}
	return p.ReadFull(c, int(size))
}
