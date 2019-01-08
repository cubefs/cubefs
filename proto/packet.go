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

package proto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/buf"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	GRequestID = int64(1)
	Buffers    = buf.NewBufferPool()
)

func GeneratorRequestID() int64 {
	return atomic.AddInt64(&GRequestID, 1)
}

const (
	AddrSplit = "/"
)

//operations
const (
	ProtoMagic         uint8 = 0xFF
	OpInitResultCode   uint8 = 0x00
	OpCreateExtent     uint8 = 0x01
	OpMarkDelete       uint8 = 0x02
	OpWrite            uint8 = 0x03
	OpRead             uint8 = 0x04
	OpStreamRead       uint8 = 0x05
	OpGetAllWatermarks uint8 = 0x07

	OpNotifyExtentRepair      uint8 = 0x08
	OpExtentRepairRead        uint8 = 0x09
	OpBlobFileRepairRead      uint8 = 0x0A
	OpFlowInfo                uint8 = 0x0B
	OpSyncDelNeedle           uint8 = 0x0C // TODO remove?
	OpNotifyCompactBlobFile   uint8 = 0x0D
	OpGetDataPartitionMetrics uint8 = 0x0E
	OpRandomWrite             uint8 = 0x0F
	OpGetAppliedId            uint8 = 0x10
	OpGetPartitionSize        uint8 = 0x11

	// Operations: Client -> MetaNode.
	OpMetaCreateInode   uint8 = 0x20
	OpMetaDeleteInode   uint8 = 0x21
	OpMetaCreateDentry  uint8 = 0x22
	OpMetaDeleteDentry  uint8 = 0x23
	OpMetaOpen          uint8 = 0x24
	OpMetaLookup        uint8 = 0x25
	OpMetaReadDir       uint8 = 0x26
	OpMetaInodeGet      uint8 = 0x27
	OpMetaBatchInodeGet uint8 = 0x28
	OpMetaExtentsAdd    uint8 = 0x29
	OpMetaExtentsDel    uint8 = 0x2A
	OpMetaExtentsList   uint8 = 0x2B
	OpMetaUpdateDentry  uint8 = 0x2C
	OpMetaTruncate      uint8 = 0x2D
	OpMetaLinkInode     uint8 = 0x2E
	OpMetaEvictInode    uint8 = 0x2F
	OpMetaSetattr       uint8 = 0x30
	OpMetaReleaseOpen   uint8 = 0x31

	// Operations: Master -> MetaNode
	OpCreateMetaPartition       uint8 = 0x40
	OpMetaNodeHeartbeat         uint8 = 0x41
	OpDeleteMetaPartition       uint8 = 0x42
	OpUpdateMetaPartition       uint8 = 0x43
	OpLoadMetaPartition         uint8 = 0x44
	OpDecommissionMetaPartition uint8 = 0x45

	// Operations: Master -> DataNode
	OpCreateDataPartition       uint8 = 0x60
	OpDeleteDataPartition       uint8 = 0x61
	OpLoadDataPartition         uint8 = 0x62
	OpDataNodeHeartbeat         uint8 = 0x63
	OpReplicateFile             uint8 = 0x64
	OpDeleteFile                uint8 = 0x65
	OpDecommissionDataPartition uint8 = 0x66

	// Commons
	OpIntraGroupNetErr uint8 = 0xF3
	OpArgMismatchErr   uint8 = 0xF4
	OpNotExistErr      uint8 = 0xF5
	OpDiskNoSpaceErr   uint8 = 0xF6
	OpDiskErr          uint8 = 0xF7
	OpErr              uint8 = 0xF8
	OpAgain            uint8 = 0xF9
	OpExistErr         uint8 = 0xFA
	OpInodeFullErr     uint8 = 0xFB
	OpNotLeaderErr     uint8 = 0xFC
	OpNotPerm          uint8 = 0xFD
	OpOk               uint8 = 0xF0

	// For connection diagnosis
	OpPing uint8 = 0xFF
)

const (
	WriteDeadlineTime        = 5
	ReadDeadlineTime         = 5
	SyncSendTaskDeadlineTime = 20
	NoReadDeadlineTime       = -1
)

// TODO what is TinyExtentType and what is NormalExtentType
const (
	TinyExtentType   = 0
	NormalExtentType = 1
)

type Packet struct {
	Magic              uint8
	ExtentType         uint8
	Opcode             uint8
	ResultCode         uint8
	RemainingFollowers uint8
	CRC                uint32
	Size               uint32
	ArgLen             uint32
	KernelOffset       uint64
	PartitionID        uint64
	ExtentID           uint64
	ExtentOffset       int64
	ReqID              int64
	Arg                []byte //if create or append ops, data contains addrs
	Data               []byte
	StartT             int64
}

func NewPacket() *Packet {
	p := new(Packet)
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()

	return p
}

func (p *Packet) GetStoreModeMsg() (m string) {
	switch p.ExtentType {
	case TinyExtentType:
		m = "TinyExtent"
	case NormalExtentType:
		m = "NormalExtent"
	default:
		m = "Unknown"
	}
	return
}

func (p *Packet) GetOpMsg() (m string) {
	switch p.Opcode {
	case OpCreateExtent:
		m = "CreateFile"
	case OpMarkDelete:
		m = "MarkDelete"
	case OpWrite:
		m = "Write"
	case OpRandomWrite:
		m = "IsRandomWrite"
	case OpRead:
		m = "Read"
	case OpStreamRead:
		m = "StreamRead"
	case OpGetAllWatermarks:
		m = "GetAllWatermarks"
	case OpNotifyExtentRepair:
		m = "NotifyReplicasToRepair"
	case OpBlobFileRepairRead:
		m = "BlobFileRepairRead"
	case OpNotifyCompactBlobFile:
		m = "NotifyCompactBlobFile"
	case OpExtentRepairRead:
		m = "ExtentRepairRead"
	case OpFlowInfo:
		m = "FlowInfo"
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr"
	case OpMetaCreateInode:
		m = "OpMetaCreateInode"
	case OpMetaDeleteInode:
		m = "OpMetaDeleteInode"
	case OpMetaCreateDentry:
		m = "OpMetaCreateDentry"
	case OpMetaDeleteDentry:
		m = "OpMetaDeleteDentry"
	case OpMetaOpen:
		m = "OpMetaOpen"
	case OpMetaReleaseOpen:
		m = "OpMetaReleaseOpen"
	case OpMetaLookup:
		m = "OpMetaLookup"
	case OpMetaReadDir:
		m = "OpMetaReadDir"
	case OpMetaInodeGet:
		m = "OpMetaInodeGet"
	case OpMetaBatchInodeGet:
		m = "OpMetaBatchInodeGet"
	case OpMetaExtentsAdd:
		m = "OpMetaExtentsAdd"
	case OpMetaExtentsDel:
		m = "OpMetaExtentsDel"
	case OpMetaExtentsList:
		m = "OpMetaExtentsList"
	case OpMetaUpdateDentry:
		m = "OpMetaUpdateDentry"
	case OpMetaTruncate:
		m = "OpMetaTruncate"
	case OpMetaLinkInode:
		m = "OpMetaLinkInode"
	case OpMetaEvictInode:
		m = "OpMetaEvictInode"
	case OpMetaSetattr:
		m = "OpMetaSetattr"
	case OpCreateMetaPartition:
		m = "OpCreateMetaPartition"
	case OpMetaNodeHeartbeat:
		m = "OpMetaNodeHeartbeat"
	case OpDeleteMetaPartition:
		m = "OpDeleteMetaPartition"
	case OpUpdateMetaPartition:
		m = "OpUpdateMetaPartition"
	case OpLoadMetaPartition:
		m = "OpLoadMetaPartition"
	case OpDecommissionMetaPartition:
		m = "OpDecommissionMetaPartition"
	case OpCreateDataPartition:
		m = "OpCreateDataPartition"
	case OpDeleteDataPartition:
		m = "OpDeleteDataPartition"
	case OpLoadDataPartition:
		m = "OpLoadDataPartition"
	case OpDecommissionDataPartition:
		m = "OpDecommissionDataPartition"
	case OpDataNodeHeartbeat:
		m = "OpDataNodeHeartbeat"
	case OpReplicateFile:
		m = "OpReplicateFile"
	case OpDeleteFile:
		m = "OpDeleteFile"
	case OpPing:
		m = "OpPing"
	case OpGetDataPartitionMetrics:
		m = "OpGetDataPartitionMetrics"
	case OpGetAppliedId:
		m = "OpGetAppliedId"
	case OpGetPartitionSize:
		m = "OpGetPartitionSize"
	}
	return
}

func (p *Packet) GetResultMesg() (m string) {
	if p == nil {
		return ""
	}

	switch p.ResultCode {
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr"
	case OpDiskNoSpaceErr:
		m = "DiskNoSpaceErr"
	case OpDiskErr:
		m = "DiskErr"
	case OpErr:
		m = "Err"
	case OpAgain:
		m = "Again"
	case OpOk:
		m = "Ok"
	case OpSyncDelNeedle:
		m = "OpSyncHasDelNeedle"
	case OpExistErr:
		m = "ExistErr"
	case OpInodeFullErr:
		m = "InodeFullErr"
	case OpArgMismatchErr:
		m = "ArgUnmatchErr"
	case OpNotExistErr:
		m = "NotExistErr"
	case OpNotLeaderErr:
		m = "NotLeaderErr"
	case OpNotPerm:
		m = "NotPerm"
	default:
		return fmt.Sprintf("Unknown ResultCode(%v)", p.ResultCode)
	}
	return
}

func (p *Packet) MarshalHeader(out []byte) {
	out[0] = p.Magic
	out[1] = p.ExtentType
	out[2] = p.Opcode
	out[3] = p.ResultCode
	out[4] = p.RemainingFollowers
	binary.BigEndian.PutUint32(out[5:9], p.CRC)
	binary.BigEndian.PutUint32(out[9:13], p.Size)
	binary.BigEndian.PutUint32(out[13:17], p.ArgLen)
	binary.BigEndian.PutUint64(out[17:25], p.PartitionID)
	binary.BigEndian.PutUint64(out[25:33], p.ExtentID)
	binary.BigEndian.PutUint64(out[33:41], uint64(p.ExtentOffset))
	binary.BigEndian.PutUint64(out[41:49], uint64(p.ReqID))
	binary.BigEndian.PutUint64(out[49:util.PacketHeaderSize], p.KernelOffset)
	return
}

func (p *Packet) UnmarshalHeader(in []byte) error {
	p.Magic = in[0]
	if p.Magic != ProtoMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Magic)))
	}

	p.ExtentType = in[1]
	p.Opcode = in[2]
	p.ResultCode = in[3]
	p.RemainingFollowers = in[4]
	p.CRC = binary.BigEndian.Uint32(in[5:9])
	p.Size = binary.BigEndian.Uint32(in[9:13])
	p.ArgLen = binary.BigEndian.Uint32(in[13:17])
	p.PartitionID = binary.BigEndian.Uint64(in[17:25])
	p.ExtentID = binary.BigEndian.Uint64(in[25:33])
	p.ExtentOffset = int64(binary.BigEndian.Uint64(in[33:41]))
	p.ReqID = int64(binary.BigEndian.Uint64(in[41:49]))
	p.KernelOffset = binary.BigEndian.Uint64(in[49:util.PacketHeaderSize])

	return nil
}

func (p *Packet) MarshalData(v interface{}) error {
	data, err := json.Marshal(v)
	if err == nil {
		p.Data = data
		p.Size = uint32(len(p.Data))
	}
	return err
}

func (p *Packet) UnmarshalData(v interface{}) error {
	return json.Unmarshal(p.Data, v)
}

func (p *Packet) WriteToNoDeadLineConn(c net.Conn) (err error) {
	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.ArgLen)]); err == nil {
			if p.Data != nil {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func (p *Packet) WriteToConn(c net.Conn) (err error) {
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))
	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.ArgLen)]); err == nil {
			if p.Data != nil && p.Size != 0 {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func ReadFull(c net.Conn, buf *[]byte, readSize int) (err error) {
	*buf = make([]byte, readSize)
	_, err = io.ReadFull(c, (*buf)[:readSize])
	return
}

func (p *Packet) ReadFromConn(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(timeoutSec)))
	}
	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.ArgLen > 0 {
		p.Arg = make([]byte, int(p.ArgLen))
		if _, err = io.ReadFull(c, p.Arg[:int(p.ArgLen)]); err != nil {
			return err
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	if (p.Opcode == OpRead || p.Opcode == OpStreamRead || p.Opcode == OpExtentRepairRead) && p.ResultCode == OpInitResultCode {
		size = 0
	}
	p.Data = make([]byte, size)
	_, err = io.ReadFull(c, p.Data[:size])
	return err
}

func (p *Packet) PackOkReply() {
	p.ResultCode = OpOk
	p.Size = 0
	p.ArgLen = 0
}

func (p *Packet) PackOkWithBody(reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = OpOk
	p.ArgLen = 0
}

func (p *Packet) PackErrorWithBody(code uint8, reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = code
	p.ArgLen = 0
}

func (p *Packet) GetUniqueLogId() (m string) {
	var (
		offset uint64
		size   uint32
	)
	if p.ExtentType == TinyExtentType && p.Opcode == OpMarkDelete && len(p.Data) > 0 {
		ext := new(ExtentKey)
		err := json.Unmarshal(p.Data, ext)
		if err == nil {
			offset = ext.ExtentOffset
			size = ext.Size
		}
	} else {
		offset = uint64(p.ExtentOffset)
		size = p.Size
	}

	m = fmt.Sprintf("Req%v_Partition%v_Extent%v_ExtentOffset%v_KernelOffset%v_Size%v_StoreMode%v_Opcode%v_ResultCode%v",
		p.ReqID, p.PartitionID, p.ExtentID, offset,
		p.KernelOffset, size, p.GetStoreModeMsg(), p.GetOpMsg(), p.GetResultMesg())
	return
}

func (p *Packet) IsForwardPkt() bool {
	return p.RemainingFollowers > 0
}

func (p *Packet) LogMessage(action, remote string, start int64, err error) (m string) {
	if err == nil {
		m = fmt.Sprintf("id[%v] op[%v] remote[%v] "+
			" cost[%v] transite[%v] nodes[%v]",
			p.GetUniqueLogId(), action, remote,
			(time.Now().UnixNano()-start)/1e6, p.IsForwardPkt(), p.RemainingFollowers)

	} else {
		m = fmt.Sprintf("id[%v] op[%v] remote[%v]"+
			", err[%v] transite[%v] nodes[%v]", p.GetUniqueLogId(), action,
			remote, err.Error(), p.IsForwardPkt(), p.RemainingFollowers)
	}

	return
}

func (p *Packet) ShallRetry() bool {
	return p.ResultCode == OpAgain || p.ResultCode == OpErr
}
