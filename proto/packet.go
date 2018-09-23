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
	ReqIDGlobal = int64(1)
	Buffers     = buf.NewBufferPool()
)

func GetReqID() int64 {
	return atomic.AddInt64(&ReqIDGlobal, 1)
}

const (
	AddrSplit       = "/"
	ExtentPartition = "extent"
	BlobPartition   = "blob"
)

//operations
const (
	ProtoMagic                   uint8 = 0xFF
	OpInitResultCode             uint8 = 0x00
	OpCreateFile                 uint8 = 0x01
	OpMarkDelete                 uint8 = 0x02
	OpWrite                      uint8 = 0x03
	OpRead                       uint8 = 0x04
	OpStreamRead                 uint8 = 0x05
	OpGetWatermark               uint8 = 0x06
	OpExtentStoreGetAllWaterMark uint8 = 0x07

	OpNotifyExtentRepair       uint8 = 0x08
	OpERepairRead              uint8 = 0x09
	OpBlobFileRepairRead       uint8 = 0x0A
	OpFlowInfo                 uint8 = 0x0B
	OpSyncDelNeedle            uint8 = 0x0C
	OpNotifyCompactBlobFile    uint8 = 0x0D
	OpGetDataPartitionMetrics  uint8 = 0x0E
	OpBlobStoreGetAllWaterMark uint8 = 0x0F
	OpNotifyBlobRepair         uint8 = 0x10

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

	// Operations: Master -> MetaNode
	OpCreateMetaPartition  uint8 = 0x40
	OpMetaNodeHeartbeat    uint8 = 0x41
	OpDeleteMetaPartition  uint8 = 0x42
	OpUpdateMetaPartition  uint8 = 0x43
	OpLoadMetaPartition    uint8 = 0x44
	OpOfflineMetaPartition uint8 = 0x45

	// Operations: Master -> DataNode
	OpCreateDataPartition uint8 = 0x60
	OpDeleteDataPartition uint8 = 0x61
	OpLoadDataPartition   uint8 = 0x62
	OpDataNodeHeartbeat   uint8 = 0x63
	OpReplicateFile       uint8 = 0x64
	OpDeleteFile          uint8 = 0x65

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
	OpOk               uint8 = 0xF0

	// For connection diagnosis
	OpPing uint8 = 0xFF
)

const (
	WriteDeadlineTime      = 5
	ReadDeadlineTime       = 5
	NoReadDeadlineTime     = -1
	StreamReadDeadLineTime = 2
)

const (
	BlobStoreMode   = 0
	ExtentStoreMode = 1
)

type Packet struct {
	Magic       uint8
	StoreMode   uint8
	Opcode      uint8
	ResultCode  uint8
	Nodes       uint8
	Crc         uint32
	Size        uint32
	Arglen      uint32
	PartitionID uint32
	FileID      uint64
	Offset      int64
	ReqID       int64
	Arg         []byte //if create or append ops, data contains addrs
	Data        []byte
	StartT      int64
}

func NewPacket() *Packet {
	p := new(Packet)
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()

	return p
}

func (p *Packet) GetStoreModeMsg() (m string) {
	switch p.StoreMode {
	case BlobStoreMode:
		m = "Blob"
	case ExtentStoreMode:
		m = "Extent"
	default:
		m = "Unknown"
	}
	return
}

func (p *Packet) GetOpMsg() (m string) {
	switch p.Opcode {
	case OpCreateFile:
		m = "CreateFile"
	case OpMarkDelete:
		m = "MarkDelete"
	case OpWrite:
		m = "Write"
	case OpRead:
		m = "Read"
	case OpStreamRead:
		m = "StreamRead"
	case OpGetWatermark:
		m = "GetWatermark"
	case OpExtentStoreGetAllWaterMark:
		m = "GetAllWatermark"
	case OpNotifyExtentRepair:
		m = "NotifyExtentRepair"
	case OpBlobFileRepairRead:
		m = "BlobFileRepairRead"
	case OpNotifyCompactBlobFile:
		m = "NotifyCompactBlobFile"
	case OpERepairRead:
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
	case OpOfflineMetaPartition:
		m = "OpOfflineMetaPartition"
	case OpCreateDataPartition:
		m = "OpCreateDataPartion"
	case OpDeleteDataPartition:
		m = "OpDeleteDataPartion"
	case OpLoadDataPartition:
		m = "OpLoadDataPartion"
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
	case OpBlobStoreGetAllWaterMark:
		m="OpBlobStoreGetAllWaterMark"
	case OpNotifyBlobRepair:
		m="OpNotifyBlobRepair"

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
	default:
		return fmt.Sprintf("Unknown ResultCode(%v)", p.ResultCode)
	}
	return
}

func (p *Packet) GetData() string {
	if p == nil || p.Data == nil {
		return ""
	}
	return string(p.Data)
}

func (p *Packet) MarshalHeader(out []byte) {
	out[0] = p.Magic
	out[1] = p.StoreMode
	out[2] = p.Opcode
	out[3] = p.ResultCode
	out[4] = p.Nodes
	binary.BigEndian.PutUint32(out[5:9], p.Crc)
	binary.BigEndian.PutUint32(out[9:13], p.Size)
	binary.BigEndian.PutUint32(out[13:17], p.Arglen)
	binary.BigEndian.PutUint32(out[17:21], p.PartitionID)
	binary.BigEndian.PutUint64(out[21:29], p.FileID)
	binary.BigEndian.PutUint64(out[29:37], uint64(p.Offset))
	binary.BigEndian.PutUint64(out[37:util.PacketHeaderSize], uint64(p.ReqID))
	return
}

func (p *Packet) UnmarshalHeader(in []byte) error {
	p.Magic = in[0]
	if p.Magic != ProtoMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Magic)))
	}

	p.StoreMode = in[1]
	p.Opcode = in[2]
	p.ResultCode = in[3]
	p.Nodes = in[4]
	p.Crc = binary.BigEndian.Uint32(in[5:9])
	p.Size = binary.BigEndian.Uint32(in[9:13])
	p.Arglen = binary.BigEndian.Uint32(in[13:17])
	p.PartitionID = binary.BigEndian.Uint32(in[17:21])
	p.FileID = binary.BigEndian.Uint64(in[21:29])
	p.Offset = int64(binary.BigEndian.Uint64(in[29:37]))
	p.ReqID = int64(binary.BigEndian.Uint64(in[37:util.PacketHeaderSize]))

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
		if _, err = c.Write(p.Arg[:int(p.Arglen)]); err == nil {
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
		if _, err = c.Write(p.Arg[:int(p.Arglen)]); err == nil {
			if p.Data != nil && p.Size != 0 {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func (p *Packet) WriteHeaderToConn(c net.Conn) (err error) {
	header, err := Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer Buffers.Put(header)
	p.MarshalHeader(header)
	_, err = c.Write(header)

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

	if p.Arglen > 0 {
		if err = ReadFull(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	if (p.Opcode == OpRead || p.Opcode == OpStreamRead) && p.ResultCode == OpInitResultCode {
		size = 0
	}
	return ReadFull(c, &p.Data, int(size))
}

func (p *Packet) PackOkReply() {
	p.ResultCode = OpOk
	p.Size = 0
	p.Arglen = 0
}

func (p *Packet) PackOkReadReply() {
	p.ResultCode = OpOk
	p.Arglen = 0
}

func (p *Packet) PackOkGetWatermarkReply(size int64) {
	p.Offset = size
	p.Size = 0
	p.ResultCode = OpOk
	p.Arglen = 0
}

func (p *Packet) IsOkReply() bool {
	return p.ResultCode == OpOk
}

func (p *Packet) PackOkWithBody(reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = OpOk
	p.Arglen = 0
}

func (p *Packet) PackErrorWithBody(errCode uint8, reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = errCode
	p.Arglen = 0
}

func (p *Packet) GetUniqueLogId() (m string) {
	m = fmt.Sprintf("%v_%v_%v_%v_%v_%v_%v_%v", p.ReqID, p.PartitionID, p.FileID,
		p.Offset, p.Size, p.GetStoreModeMsg(), p.GetOpMsg(), p.GetResultMesg())

	return
}

func (p *Packet) IsTransitPkg() bool {
	return p.Nodes > 0
}

func (p *Packet) ActionMsg(action, remote string, start int64, err error) (m string) {
	if err == nil {
		m = fmt.Sprintf("id[%v] op[%v] remote[%v] "+
			" cost[%v] transite[%v] nodes[%v]",
			p.GetUniqueLogId(), action, remote,
			(time.Now().UnixNano()-start)/1e6, p.IsTransitPkg(), p.Nodes)

	} else {
		m = fmt.Sprintf("id[%v] op[%v] remote[%v]"+
			", err[%v] transite[%v] nodes[%v]", p.GetUniqueLogId(), action,
			remote, err.Error(), p.IsTransitPkg(), p.Nodes)
	}

	return
}

func (p *Packet) ShallRetry() bool {
	return p.ResultCode == OpAgain || p.ResultCode == OpErr
}

func NewPingPacket() *Packet {
	return &Packet{
		Magic:  ProtoMagic,
		Opcode: OpPing,
		ReqID:  GetReqID(),
	}
}
