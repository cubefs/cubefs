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

package stream

import (
	"encoding/binary"
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util"
	"hash/crc32"
	"io"
	"net"
	"time"
)

type Packet struct {
	proto.Packet
	inode        uint64
	kernelOffset int
	status       int
	errCount     int
}

func (p *Packet) String() string {
	return fmt.Sprintf("ReqID(%v) Op(%v) Inode(%v) FileOffset(%v) Size(%v) PartitionID(%v) ExtentID(%v) ExtentOffset(%v) CRC(%v) ResultCode(%v)", p.ReqID, p.GetOpMsg(), p.inode, p.kernelOffset, p.Size, p.PartitionID, p.ExtentID, p.ExtentOffset, p.CRC, p.GetResultMesg())
}

func NewPacket(inode uint64, offset int) *Packet {
	p := new(Packet)
	p.ReqID = proto.GeneratorRequestID()
	p.Magic = proto.ProtoMagic
	p.StoreMode = proto.NormalExtentMode
	p.Opcode = proto.OpWrite
	p.inode = inode
	p.kernelOffset = offset
	p.Data, _ = proto.Buffers.Get(util.BlockSize)
	return p
}

func NewWritePacket(dp *wrapper.DataPartition, extentId uint64, offset int, inode uint64, kernelOffset int, isRandom bool) (p *Packet) {
	p = new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.Data = make([]byte, 0)
	p.StoreMode = proto.NormalExtentMode
	p.ExtentID = extentId
	p.ExtentOffset = int64(offset)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.RemainReplicates = uint8(len(dp.Hosts) - 1)
	p.ReqID = proto.GeneratorRequestID()
	if isRandom {
		p.Opcode = proto.OpRandomWrite
	} else {
		p.Opcode = proto.OpWrite
	}
	p.inode = inode
	p.kernelOffset = kernelOffset
	p.Data, _ = proto.Buffers.Get(util.BlockSize)

	return
}

func NewReadPacket(key *proto.ExtentKey, offset, size int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = key.ExtentId
	p.PartitionID = key.PartitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpRead
	p.StoreMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()
	p.RemainReplicates = 0

	return
}

func NewStreamReadPacket(key *proto.ExtentKey, offset, size int, inode uint64, fileOffset int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = key.ExtentId
	p.PartitionID = key.PartitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpStreamRead
	p.StoreMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()
	p.RemainReplicates = 0
	p.inode = inode
	p.kernelOffset = fileOffset

	return
}

func NewCreateExtentPacket(dp *wrapper.DataPartition, inodeId uint64) (p *Packet) {
	p = new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.Data = make([]byte, 0)
	p.StoreMode = proto.NormalExtentMode
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.RemainReplicates = uint8(len(dp.Hosts) - 1)
	p.ReqID = proto.GeneratorRequestID()
	p.Opcode = proto.OpCreateExtent

	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, inodeId)
	p.Size = uint32(len(p.Data))

	return p
}

func NewDeleteExtentPacket(dp *wrapper.DataPartition, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpMarkDelete
	p.StoreMode = proto.NormalExtentMode
	p.PartitionID = dp.PartitionID
	p.ExtentID = extentId
	p.ReqID = proto.GeneratorRequestID()
	p.RemainReplicates = uint8(len(dp.Hosts) - 1)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	return p
}

func NewReply(reqId int64, partition uint32, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.ReqID = reqId
	p.PartitionID = partition
	p.ExtentID = extentId
	p.Magic = proto.ProtoMagic
	p.StoreMode = proto.NormalExtentMode

	return
}

func (p *Packet) IsEqualWriteReply(q *Packet) bool {
	if p.ReqID == q.ReqID && p.PartitionID == q.PartitionID {
		return true
	}
	return false
}

func (p *Packet) IsEqualReadReply(q *Packet) bool {
	if p.ReqID == q.ReqID && p.PartitionID == q.PartitionID && p.ExtentID == q.ExtentID && p.ExtentOffset == q.ExtentOffset && p.Size == q.Size {
		return true
	}
	return false
}

func (p *Packet) IsEqualStreamReadReply(q *Packet) bool {
	if p.ReqID == q.ReqID && p.PartitionID == q.PartitionID && p.ExtentID == q.ExtentID {
		return true
	}

	return false
}

func (p *Packet) fill(data []byte, size int) (canWrite int) {
	if p.Size+uint32(size) > util.BlockSize {
		return
	}
	blockSpace := util.BlockSize
	remain := int(blockSpace) - int(p.Size)
	canWrite = util.Min(remain, size)
	if canWrite <= 0 {
		return
	}
	copy(p.Data[p.Size:p.Size+uint32(canWrite)], data[:canWrite])
	p.Size += uint32(canWrite)

	return
}

func (p *Packet) isFullPacket() bool {
	return p.Size-util.BlockSize == 0
}

func (p *Packet) getPacketLength() int {
	return int(p.Size)
}

func (p *Packet) writeTo(conn net.Conn) (err error) {
	p.CRC = crc32.ChecksumIEEE(p.Data[:p.Size])
	err = p.WriteToConn(conn)

	return
}

func ReadFull(c net.Conn, buf *[]byte, readSize int) (err error) {
	if *buf == nil || readSize != util.BlockSize {
		*buf = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, (*buf)[:readSize])
	return
}

func (p *Packet) ReadFromConnStream(c net.Conn, deadlineTime time.Duration) (err error) {
	if deadlineTime != proto.NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(deadlineTime * time.Second))
	}
	header, _ := proto.Buffers.Get(util.PacketHeaderSize)
	defer proto.Buffers.Put(header)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.Arglen > 0 {
		if err = ReadFull(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	_, err = io.ReadFull(c, p.Data[:size])
	return
}
