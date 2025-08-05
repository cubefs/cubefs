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

package stream

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
)

// Packet defines a wrapper of the packet in proto.
type Packet struct {
	proto.Packet
	inode    uint64
	errCount int
}

// String returns the string format of the packet.
func (p *Packet) String() string {
	return fmt.Sprintf("ReqID(%v)Op(%v)Inode(%v)FileOffset(%v)Size(%v)PartitionID(%v)ExtentID(%v)ExtentOffset(%v)CRC(%v)ResultCode(%v:%v)Seq(%v), errCnt(%d)",
		p.ReqID, p.GetOpMsg(), p.inode, p.KernelOffset, p.Size, p.PartitionID, p.ExtentID, p.ExtentOffset, p.CRC, p.ResultCode, p.GetResultMsg(), p.VerSeq, p.errCount)
}

func NewWriteTinyDirectly(inode uint64, dpID uint64, offset int, dp *wrapper.DataPartition) *Packet {
	reqPacket := NewWritePacket(inode, offset, proto.TinyExtentType)
	reqPacket.PartitionID = dpID
	reqPacket.RemainingFollowers = uint8(len(dp.Hosts) - 1)
	reqPacket.Arg = ([]byte)(dp.GetAllAddrs())
	reqPacket.ArgLen = uint32(len(reqPacket.Arg))
	if len(dp.Hosts) == 1 {
		reqPacket.RemainingFollowers = 127
	}
	return reqPacket
}

// NewWritePacket returns a new write packet.
func NewWritePacket(inode uint64, fileOffset, storeMode int) *Packet {
	p := new(Packet)
	p.ReqID = proto.GenerateRequestID()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpWrite
	p.inode = inode
	p.KernelOffset = uint64(fileOffset)
	if storeMode == proto.TinyExtentType {
		p.Data, _ = proto.Buffers.Get(util.DefaultTinySizeLimit)
	} else {
		p.Data, _ = proto.Buffers.Get(util.BlockSize)
	}
	return p
}

// NewOverwritePacket returns a new overwrite packet.
func NewOverwriteByAppendPacket(dp *wrapper.DataPartition, extentID uint64, extentOffset int,
	inode uint64, fileOffset int, direct bool, op uint8,
) *Packet {
	p := new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.ExtentID = extentID
	p.ExtentOffset = int64(extentOffset)
	p.ReqID = proto.GenerateRequestID()
	p.Arg = nil
	p.ArgLen = 0
	p.RemainingFollowers = 0
	p.Opcode = op

	if direct {
		if op == proto.OpRandomWriteAppend {
			p.Opcode = proto.OpSyncRandomWriteAppend
		} else if op == proto.OpTryWriteAppend {
			p.Opcode = proto.OpSyncTryWriteAppend
		}
	}

	p.inode = inode
	p.KernelOffset = uint64(fileOffset)
	p.Data, _ = proto.Buffers.Get(util.BlockSize)
	return p
}

// NewOverwritePacket returns a new overwrite packet.
func NewOverwritePacket(dp *wrapper.DataPartition, extentID uint64, extentOffset int, inode uint64, fileOffset int, isSnap bool) *Packet {
	p := new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.ExtentID = extentID
	p.ExtentOffset = int64(extentOffset)
	p.ReqID = proto.GenerateRequestID()
	p.Arg = nil
	p.ArgLen = 0
	p.RemainingFollowers = 0
	if isSnap {
		p.Opcode = proto.OpRandomWriteVer // proto.OpRandomWrite
	} else {
		p.Opcode = proto.OpRandomWrite // proto.OpRandomWrite
	}
	p.inode = inode
	p.KernelOffset = uint64(fileOffset)
	p.Data, _ = proto.Buffers.Get(util.BlockSize)
	return p
}

// NewReadPacket returns a new read packet.
func NewReadPacket(key *proto.ExtentKey, extentOffset, size int, inode uint64, fileOffset int, followerRead bool) *Packet {
	p := new(Packet)
	p.ExtentID = key.ExtentId
	p.PartitionID = key.PartitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(extentOffset)
	p.Size = uint32(size)
	if followerRead {
		p.Opcode = proto.OpStreamFollowerRead
	} else {
		p.Opcode = proto.OpStreamRead
	}
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = 0
	p.inode = inode
	p.KernelOffset = uint64(fileOffset)
	return p
}

// NewCreateExtentPacket returns a new packet to create extent.
func NewCreateExtentPacket(dp *wrapper.DataPartition, inode uint64) *Packet {
	p := new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.ExtentType |= proto.PacketProtocolVersionFlag
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.ArgLen = uint32(len(p.Arg))
	p.RemainingFollowers = uint8(len(dp.Hosts) - 1)
	if len(dp.Hosts) == 1 {
		p.RemainingFollowers = 127
	}
	p.ReqID = proto.GenerateRequestID()
	p.Opcode = proto.OpCreateExtent
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, inode)
	p.Size = uint32(len(p.Data))
	return p
}

// NewFlashCachePacket returns a new packet of flash cache.
func NewFlashCachePacket(inode uint64, opcode uint8) *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.inode = inode
	p.Opcode = opcode
	return p
}

func NewFlashCacheReply() *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	return p
}

// NewReply returns a new reply packet. TODO rename to NewReplyPacket?
func NewReply(reqID int64, partitionID uint64, extentID uint64) *Packet {
	p := new(Packet)
	p.ReqID = reqID
	p.PartitionID = partitionID
	p.ExtentID = extentID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	return p
}

func (p *Packet) isValidWriteReply(q *Packet) bool {
	if p.ReqID == q.ReqID && p.PartitionID == q.PartitionID {
		return true
	}
	return false
}

func (p *Packet) isValidReadReply(q *Packet) bool {
	if p.ReqID == q.ReqID && p.PartitionID == q.PartitionID && p.ExtentID == q.ExtentID {
		return true
	}
	return false
}

func (p *Packet) writeToConn(conn net.Conn) error {
	p.CRC = crc32.ChecksumIEEE(p.Data[:p.Size])
	return p.WriteToConn(conn)
}

func (p *Packet) readFromConn(c net.Conn, deadlineTime time.Duration) (err error) {
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

	if err = p.TryReadExtraFieldsFromConn(c); err != nil {
		return
	}

	if p.ArgLen > 0 {
		if err = readToBuffer(c, &p.Arg, int(p.ArgLen)); err != nil {
			return
		}
	}

	size := int(p.Size)
	if size > len(p.Data) {
		size = len(p.Data)
	}

	_, err = io.ReadFull(c, p.Data[:size])
	return
}

func readToBuffer(c net.Conn, buf *[]byte, readSize int) (err error) {
	if *buf == nil || readSize != util.BlockSize {
		*buf = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, (*buf)[:readSize])
	return
}
