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

package data

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
)

// Packet defines a wrapper of the packet in proto.
type Packet struct {
	proto.Packet
	inode    uint64
	errCount int
}

func EncodeReplPacketArg(followers []string, quorum int) []byte {
	return []byte(strings.Join(followers, "/") + "/" + strconv.Itoa(quorum))
}

func (p *Packet) SetupReplArg(allHost []string, quorum int) {
	var followers []string
	if len(allHost) > 1 {
		followers = allHost[1:]
	}
	p.Arg = EncodeReplPacketArg(followers, quorum)
	p.ArgLen = uint32(len(p.Arg))
}

// String returns the string format of the packet.
func (p *Packet) String() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("ResultCode(%v)ReqID(%v)Op(%v)Inode(%v)FileOffset(%v)Size(%v)PartitionID(%v)ExtentID(%v)ExtentOffset(%v)CRC(%v)",
		p.GetResultMsg(), p.ReqID, p.GetOpMsg(), p.inode, p.KernelOffset, p.Size, p.PartitionID, p.ExtentID, p.ExtentOffset, p.CRC)
}

// NewWritePacket returns a new write packet.
func NewWritePacket(ctx context.Context, inode uint64, fileOffset uint64, storeMode int, blksize int) *Packet {
	p := new(Packet)
	p.ReqID = proto.GenerateRequestID()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpWrite
	p.ExtentType = uint8(storeMode)
	p.inode = inode
	p.KernelOffset = fileOffset
	var err error
	if p.Data, err = proto.Buffers.Get(blksize); err != nil {
		p.Data = make([]byte, blksize)
	}
	p.SetCtx(ctx)
	return p
}

// NewWritePacket returns a new write packet.
func NewROWPacket(ctx context.Context, dp *DataPartition, quorum int, inode uint64, extID int, fileOffset uint64, extentOffset, size int) *Packet {
	p := new(Packet)
	p.ReqID = proto.GenerateRequestID()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpWrite
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dp.PartitionID
	p.ExtentID = uint64(extID)
	p.RemainingFollowers = uint8(len(dp.Hosts) - 1)
	p.KernelOffset = uint64(fileOffset)
	p.ExtentOffset = int64(extentOffset)
	p.Size = uint32(size)
	p.inode = inode

	p.SetCtx(ctx)
	p.SetupReplArg(dp.GetAllHosts(), quorum)
	return p
}

const (
	OverWritePoolCnt = 64
)

var (
	OverWritePacketPools [OverWritePoolCnt]*sync.Pool
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for index := 0; index < OverWritePoolCnt; index++ {
		OverWritePacketPools[index] = &sync.Pool{
			New: func() interface{} {
				return new(Packet)
			},
		}
	}
}

func GetOverWritePacketFromPool() (p *Packet) {
	index := rand.Intn(OverWritePoolCnt)
	p = OverWritePacketPools[index].Get().(*Packet)
	return p
}

func PutOverWritePacketToPool(p *Packet) {
	p.Data = nil
	p.Size = 0
	p.PartitionID = 0
	p.ExtentID = 0
	p.ExtentOffset = 0
	p.Arg = nil
	p.ArgLen = 0
	p.RemainingFollowers = 0
	p.ReqID = 0
	p.inode = 0
	p.SetCtx(nil)
	index := rand.Intn(OverWritePoolCnt)
	OverWritePacketPools[index].Put(p)
}

// NewOverwritePacket returns a new overwrite packet.
func NewOverwritePacket(ctx context.Context, dp *DataPartition, extentID uint64, extentOffset int, inode uint64, fileOffset uint64) *Packet {
	p := GetOverWritePacketFromPool()
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.ExtentID = extentID
	p.ExtentOffset = int64(extentOffset)
	p.ReqID = proto.GenerateRequestID()
	p.Arg = nil
	p.ArgLen = 0
	p.RemainingFollowers = 0
	p.Opcode = proto.OpRandomWrite
	p.HasPrepare = false
	p.StartT, p.RecvT, p.WaitT, p.SendT = time.Now().UnixNano(), time.Now().UnixNano(), time.Now().UnixNano(), time.Now().UnixNano()
	p.ReqID = proto.GenerateRequestID()
	p.inode = inode
	p.KernelOffset = uint64(fileOffset)
	p.SetCtx(ctx)
	return p
}

// NewReadPacket returns a new read packet.
func NewReadPacket(ctx context.Context, key *proto.ExtentKey, extentOffset, size int, inode uint64, fileOffset uint64, followerRead bool) *Packet {
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
	p.SetCtx(ctx)
	return p
}

// NewCreateExtentPacket returns a new packet to create extent.
func NewCreateExtentPacket(ctx context.Context, dp *DataPartition, quorum int, inode uint64) *Packet {
	p := new(Packet)
	p.PartitionID = dp.PartitionID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.RemainingFollowers = uint8(len(dp.Hosts) - 1)
	p.ReqID = proto.GenerateRequestID()
	p.Opcode = proto.OpCreateExtent
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, inode)
	p.Size = uint32(len(p.Data))
	p.SetCtx(ctx)
	p.SetupReplArg(dp.GetAllHosts(), quorum)
	return p
}

// NewPacketToGetAppliedID returns a new packet to get the applied ID.
func NewPacketToGetDpAppliedID(ctx context.Context, partitionID uint64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAppliedId
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.Arg = nil
	p.ArgLen = 0
	p.RemainingFollowers = 0
	p.SetCtx(ctx)
	return
}

// NewReply returns a new reply packet. TODO rename to NewReplyPacket?
func NewReply(ctx context.Context, reqID int64, partitionID uint64, extentID uint64) *Packet {
	p := new(Packet)
	p.ReqID = reqID
	p.PartitionID = partitionID
	p.ExtentID = extentID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.SetCtx(ctx)
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

func (p *Packet) writeToConn(conn net.Conn, timeoutNs int64) error {
	p.CRC = crc32.ChecksumIEEE(p.Data[:p.Size])
	return p.WriteToConnNs(conn, timeoutNs)
}

func (p *Packet) readFromConn(c net.Conn, deadlineTimeNs int64) (err error) {
	if deadlineTimeNs != proto.NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Duration(deadlineTimeNs) * time.Nanosecond))
	}
	header, _ := proto.Buffers.Get(util.PacketHeaderSize)
	defer proto.Buffers.Put(header)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.ArgLen > 0 {
		if err = readToBuffer(c, &p.Arg, int(p.ArgLen)); err != nil {
			return
		}
	}

	if p.ResultCode != proto.OpOk || int(p.Size) != len(p.Data) {
		// if read fails in datanode, p->Data would be error msg, p->Size would be the size of the msg
		msg := make([]byte, p.Size)
		_, readErr := io.ReadFull(c, msg)
		err = errors.New(fmt.Sprintf("readFromConn: ResultCode(%v) Size(%v) expectedSize(%v) msg(%v) readErr(%v)", p.GetResultMsg(), p.Size, len(p.Data), string(msg), readErr))
		return
	}

	_, err = io.ReadFull(c, p.Data)
	return
}

func (p *Packet) Copy() *Packet {
	packet := new(Packet)
	packet.ReqID = p.ReqID
	packet.PartitionID = p.PartitionID
	packet.Magic = p.Magic
	packet.ExtentType = p.ExtentType
	packet.ExtentID = p.ExtentID
	packet.ExtentOffset = p.ExtentOffset
	packet.Arg = p.Arg
	packet.ArgLen = p.ArgLen
	packet.RemainingFollowers = p.RemainingFollowers
	packet.Opcode = p.Opcode
	packet.inode = p.inode
	packet.KernelOffset = p.KernelOffset
	packet.Size = p.Size
	var err error
	if packet.Data, err = proto.Buffers.Get(len(p.Data)); err != nil {
		packet.Data = make([]byte, len(p.Data))
	}
	packet.SetCtx(p.Ctx())
	packet.errCount = p.errCount
	return packet
}

func readToBuffer(c net.Conn, buf *[]byte, readSize int) (err error) {
	if *buf == nil || readSize != util.BlockSize {
		*buf = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, (*buf)[:readSize])
	return
}

func NewTinyExtentReadPacket(ctx context.Context, partitionID uint64, extentID uint64, offset, size int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpTinyExtentAvaliRead
	p.ExtentType = proto.TinyExtentType
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)

	return
}
