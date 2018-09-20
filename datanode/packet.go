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

type Packet struct {
	proto.Packet
	NextConn      *net.TCPConn
	NextAddr      string
	IsReturn      bool
	DataPartition DataPartition
	goals         uint8
	addrs         []string
	tpObject      *ump.TpObject
	useConnectMap bool
}

func (p *Packet) afterTp() (ok bool) {
	var err error
	if p.IsErrPack() {
		err = fmt.Errorf(p.GetOpMsg()+" failed because[%v]", string(p.Data[:p.Size]))
	}
	ump.AfterTP(p.tpObject, err)

	return
}

func (p *Packet) beforeTp(clusterId string) (ok bool) {
	umpKey := fmt.Sprintf("%s_datanode_stream%v", clusterId, p.GetOpMsg())
	p.tpObject = ump.BeforeTP(umpKey)
	return
}

func (p *Packet) UnmarshalAddrs() (addrs []string, err error) {
	if len(p.Arg) < int(p.Arglen) {
		return nil, ErrArgLenMismatch
	}
	str := string(p.Arg[:int(p.Arglen)])
	goalAddrs := strings.SplitN(str, proto.AddrSplit, -1)
	p.goals = uint8(len(goalAddrs) - 1)
	if p.goals > 0 {
		addrs = goalAddrs[:int(p.goals)]
	}
	if p.Nodes < 0 {
		err = ErrBadNodes
		return
	}
	copy(p.addrs, addrs)

	return
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
		proto.OpDeleteDataPartition:
		return true
	}
	return false
}

func (p *Packet) GetNextAddr(addrs []string) error {
	sub := p.goals - p.Nodes
	if sub < 0 || sub > p.goals || (sub == p.goals && p.Nodes != 0) {
		return ErrAddrsNodesMismatch
	}
	if sub == p.goals && p.Nodes == 0 {
		return nil
	}

	p.NextAddr = fmt.Sprint(addrs[sub])

	return nil
}

func (p *Packet) IsTransitPkg() bool {
	r := p.Nodes > 0
	return r
}

func (p *Packet) CheckCrc() (err error) {
	if !p.IsWriteOperation() {
		return
	}

	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc == p.Crc {
		return
	}
	return storage.ErrPkgCrcMismatch
}

func NewExtentStoreGetAllWaterMarker(partitionId uint32) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpExtentStoreGetAllWaterMark
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GetReqID()

	return
}

func NewBlobStoreGetAllWaterMarker(partitionId uint32) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpBlobStoreGetAllWaterMark
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.StoreMode = proto.BlobStoreMode
	p.ReqID = proto.GetReqID()

	return
}

func NewStreamReadPacket(partitionId uint32, extentId, offset, size int) (p *Packet) {
	p = new(Packet)
	p.FileID = uint64(extentId)
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.Offset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpStreamRead
	p.StoreMode = proto.ExtentStoreMode
	p.ReqID = proto.GetReqID()

	return
}

func NewStreamBlobFileRepairReadPacket(partitionId uint32, blobfileId int) (p *Packet) {
	p = new(Packet)
	p.FileID = uint64(blobfileId)
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBlobFileRepairRead
	p.StoreMode = proto.BlobStoreMode
	p.ReqID = proto.GetReqID()

	return
}

func NewNotifyRepair(partitionId uint32) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpNotifyRepair
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GetReqID()

	return
}

func (p *Packet) IsTailNode() (ok bool) {
	if p.Nodes == 0 && (p.IsWriteOperation() || p.Opcode == proto.OpCreateFile ||
		(p.Opcode == proto.OpMarkDelete && p.StoreMode == proto.BlobStoreMode)) {
		return true
	}

	return
}

func (p *Packet) IsWriteOperation() bool {
	return p.Opcode == proto.OpWrite
}

func (p *Packet) IsCreateFileOperation() bool {
	return p.Opcode == proto.OpCreateFile
}

func (p *Packet) IsMarkDeleteOperation() bool {
	return p.Opcode == proto.OpMarkDelete
}

func (p *Packet) IsExtentWritePacket() bool {
	return p.StoreMode == proto.ExtentStoreMode && p.IsWriteOperation()
}

func (p *Packet) IsReadOperation() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpRead
}

func (p *Packet) IsMarkDeleteReq() bool {
	return p.Opcode == proto.OpMarkDelete
}

func (p *Packet) isHeadNode() (ok bool) {
	if p.goals == p.Nodes && (p.IsWriteOperation() || p.IsCreateFileOperation() || p.IsMarkDeleteOperation()) {
		ok = true
	}

	return
}

func (p *Packet) CopyFrom(src *Packet) {
	p.ResultCode = src.ResultCode
	p.Opcode = src.Opcode
	p.Size = src.Size
	p.Data = src.Data
}

func (p *Packet) IsErrPack() bool {
	return p.ResultCode != proto.OpOk
}

func (p *Packet) getErr() (m string) {
	return fmt.Sprintf("req[%v] err[%v]", p.GetUniqueLogId(), string(p.Data[:p.Size]))
}

func (p *Packet) ClassifyErrorOp(errLog string, errMsg string) {
	if strings.Contains(errLog, ActionReceiveFromNext) || strings.Contains(errLog, ActionSendToNext) ||
		strings.Contains(errLog, ConnIsNullErr) || strings.Contains(errLog, ActionCheckAndAddInfos) {
		p.ResultCode = proto.OpIntraGroupNetErr
		return
	}

	if strings.Contains(errMsg, storage.ErrorParamMismatch.Error()) ||
		strings.Contains(errMsg, ErrorUnknownOp.Error()) {
		p.ResultCode = proto.OpArgMismatchErr
	} else if strings.Contains(errMsg, storage.ErrorObjNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrorHasDelete.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.ErrSyscallNoSpace.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrorAgain.Error()) {
		p.ResultCode = proto.OpIntraGroupNetErr
	} else if strings.Contains(errMsg, storage.ErrorFileNotFound.Error()) {
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
	p.ClassifyErrorOp(action, msg)
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
	if (p.Opcode == proto.OpRead || p.Opcode == proto.OpStreamRead) && p.ResultCode == proto.OpInitResultCode {
		size = 0
	}
	return p.ReadFull(c, int(size))
}
