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
	inode      uint64
	fileOffset int
	errCount   int
}

func (p *Packet) String() string {
	return fmt.Sprintf("ReqID(%v)Op(%v)Inode(%v)FileOffset(%v)Size(%v)PartitionID(%v)ExtentID(%v)ExtentOffset(%v)CRC(%v)ResultCode(%v)", p.ReqID, p.GetOpMsg(), p.inode, p.fileOffset, p.Size, p.PartitionID, p.ExtentID, p.ExtentOffset, p.CRC, p.GetResultMesg())
}

func NewWritePacket(inode uint64, fileOffset, storeMode int) *Packet {
	p := new(Packet)
	p.ReqID = proto.GeneratorRequestID()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpWrite
	p.inode = inode
	p.fileOffset = fileOffset
	if storeMode == proto.TinyExtentMode {
		p.Data, _ = proto.Buffers.Get(util.DefaultTinySizeLimit)
	} else {
		p.Data, _ = proto.Buffers.Get(util.BlockSize)
	}
	return p
}

func NewOverwritePacket(dp *wrapper.DataPartition, extentID uint64, extentOffset int, inode uint64, fileOffset int) *Packet {
	p := new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.Data = make([]byte, 0)
	p.ExtentMode = proto.NormalExtentMode
	p.ExtentID = extentID
	p.ExtentOffset = int64(extentOffset)
	p.ReqID = proto.GeneratorRequestID()
	p.Arg = nil
	p.Arglen = 0
	p.RemainFollowers = 0
	p.Opcode = proto.OpRandomWrite
	p.inode = inode
	p.fileOffset = fileOffset
	p.Data, _ = proto.Buffers.Get(util.BlockSize)
	return p
}

func NewReadPacket(key *proto.ExtentKey, extentOffset, size int, inode uint64, fileOffset int) *Packet {
	p := new(Packet)
	p.ExtentID = key.ExtentId
	p.PartitionID = key.PartitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(extentOffset)
	p.Size = uint32(size)
	p.Opcode = proto.OpStreamRead
	p.ExtentMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()
	p.RemainFollowers = 0
	p.inode = inode
	p.fileOffset = fileOffset
	return p
}

func NewCreateExtentPacket(dp *wrapper.DataPartition, inode uint64) *Packet {
	p := new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.Data = make([]byte, 0)
	p.ExtentMode = proto.NormalExtentMode
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.RemainFollowers = uint8(len(dp.Hosts) - 1)
	p.ReqID = proto.GeneratorRequestID()
	p.Opcode = proto.OpCreateExtent
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, inode)
	p.Size = uint32(len(p.Data))
	return p
}

func NewReply(reqID int64, partitionID uint64, extentID uint64) *Packet {
	p := new(Packet)
	p.ReqID = reqID
	p.PartitionID = partitionID
	p.ExtentID = extentID
	p.Magic = proto.ProtoMagic
	p.ExtentMode = proto.NormalExtentMode
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

	if p.Arglen > 0 {
		if err = readToBuffer(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
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
