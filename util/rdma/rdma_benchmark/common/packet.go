package common

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"rdma_test/rdma"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
	PB

	OpWrite uint8 = 0x03

	OpOk uint8 = 0xF0

	ProtoMagic uint8 = 0xFF

	TinyExtentType   = 0
	NormalExtentType = 1

	BlockCount         = 1024
	BlockSize          = 65536 * 2
	ReadBlockSize      = BlockSize
	PerBlockCrcSize    = 4
	ExtentSize         = BlockCount * BlockSize
	PacketHeaderSize   = 57
	BlockHeaderSize    = 4096
	SyscallTryMaxTimes = 3

	DefaultTinySizeLimit = 1 * MB
)

var (
	GRequestID = int64(1)
	Buffers    *BufferPool
)

func InitBufferPool(bufLimit int64) {
	NormalBuffersTotalLimit = bufLimit
	HeadBuffersTotalLimit = bufLimit
	Buffers = NewBufferPool()
}

func GenerateRequestID() int64 {
	return atomic.AddInt64(&GRequestID, 1)
}

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
	Arg                []byte // for create or append ops, the data contains the address
	Data               []byte
	RdmaBuffer         []byte
	StartT             int64
	mesg               string
	HasPrepare         bool
}

func NewWritePacket(storeMode int, c net.Conn, size uint32) *Packet {
	p := new(Packet)
	p.ReqID = GenerateRequestID()
	p.Magic = ProtoMagic
	p.Opcode = OpWrite
	if storeMode == TinyExtentType {
		if GParam.Protocol == "rdma" {
			dataBuffer, _ := rdma.GetDataBuffer(DefaultTinySizeLimit + 105)
			p.Arg = dataBuffer[65:105]
			p.Data = dataBuffer[105:]
			p.RdmaBuffer = dataBuffer
		} else {
			p.Arg = make([]byte, 40)
			p.Data, _ = Buffers.Get(DefaultTinySizeLimit)
		}
	} else {
		if GParam.Protocol == "rdma" {
			dataBuffer, _ := rdma.GetDataBuffer(size + 105)
			p.Arg = dataBuffer[65:105]
			p.Data = dataBuffer[105:]
			p.RdmaBuffer = dataBuffer
		} else {
			p.Arg = make([]byte, 40)
			p.Data, _ = Buffers.Get(int(size))
		}
	}
	return p
}

func NewReply(reqID int64) *Packet {
	p := new(Packet)
	p.ReqID = reqID
	p.Magic = ProtoMagic
	p.ExtentType = NormalExtentType
	return p
}

func NewPacket() (p *Packet) {
	p = new(Packet)
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()
	return
}

func (p *Packet) PacketOkReply() {
	p.ResultCode = OpOk
	p.Size = 0
	p.Data = nil
	p.ArgLen = 0
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
	binary.BigEndian.PutUint64(out[49:PacketHeaderSize], p.KernelOffset)
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
	p.KernelOffset = binary.BigEndian.Uint64(in[49:PacketHeaderSize])

	return nil
}

func (p *Packet) WriteToRDMAConn(conn *rdma.Connection) (err error) {

	offset := 0

	p.MarshalHeader(p.RdmaBuffer[0:PacketHeaderSize])
	offset += PacketHeaderSize + 8

	offset += 40

	if _, err = conn.WriteExternalBuffer(p.RdmaBuffer, int(105+p.Size)); err != nil {
		return
	}
	return
}

func (p *Packet) WriteToConn(c net.Conn) (err error) {
	header, err := Buffers.Get(PacketHeaderSize)
	if err != nil {
		header = make([]byte, PacketHeaderSize)
	}
	defer Buffers.Put(header)
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
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

func (p *Packet) RecvRespFromRDMAConn(c *rdma.Connection, timeoutSec int) (err error) {

	var dataBuffer []byte
	var offset uint32 = 0

	if dataBuffer, err = c.GetRecvMsgBuffer(); err != nil {
		return
	}
	p.RdmaBuffer = dataBuffer
	if err = p.UnmarshalHeader(dataBuffer[0:PacketHeaderSize]); err != nil {
		c.ReleaseConnRxDataBuffer(dataBuffer)
		return
	}

	offset += PacketHeaderSize + 8

	if p.ArgLen > 0 {
		p.Arg = dataBuffer[65 : 65+p.ArgLen]
	}
	offset += 40

	if p.Size < 0 {
		c.ReleaseConnRxDataBuffer(dataBuffer)
		return syscall.EBADMSG
	}
	size := p.Size
	p.Data = dataBuffer[105 : 105+int(size)]
	return
}

func (p *Packet) ReadFromConn(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec != -1 {
		c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(timeoutSec)))
	} else {
		c.SetReadDeadline(time.Time{})
	}
	header, err := Buffers.Get(PacketHeaderSize)
	if err != nil {
		header = make([]byte, PacketHeaderSize)
	}
	defer Buffers.Put(header)
	var n int
	if n, err = io.ReadFull(c, header); err != nil {
		return
	}
	if n != PacketHeaderSize {
		return syscall.EBADMSG
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
		return syscall.EBADMSG
	}
	size := p.Size
	p.Data = make([]byte, size)
	if n, err = io.ReadFull(c, p.Data[:size]); err != nil {
		return err
	}
	if n != int(size) {
		return syscall.EBADMSG
	}
	return nil
}

func (p *Packet) ReadFromRDMAConnFromCli(conn *rdma.Connection, deadlineTime time.Duration) (err error) {

	var dataBuffer []byte
	var offset int

	if dataBuffer, err = conn.GetRecvMsgBuffer(); err != nil {
		return
	}

	p.RdmaBuffer = dataBuffer
	if err = p.UnmarshalHeader(dataBuffer[0:PacketHeaderSize]); err != nil {
		conn.ReleaseConnRxDataBuffer(dataBuffer)
		return
	}
	offset += PacketHeaderSize + 8 //rdma
	if p.ArgLen > 0 {
		p.Arg = dataBuffer[65 : 65+p.ArgLen]
	}
	offset += 40
	if p.Size < 0 {
		conn.ReleaseConnRxDataBuffer(dataBuffer)
		return
	}
	size := p.Size

	p.Data = dataBuffer[105 : 105+int(size)]
	return
}

func ReadFull(c net.Conn, buf *[]byte, readSize int) (err error) {
	*buf = make([]byte, readSize)
	_, err = io.ReadFull(c, (*buf)[:readSize])
	return
}

func (p *Packet) ReadFull(c net.Conn, opcode uint8, readSize int) (err error) {
	if readSize == BlockSize {
		p.Data, _ = Buffers.Get(readSize)
	} else {
		p.Data = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, p.Data[:readSize])
	return
}

func (p *Packet) ReadFromTCPConnFromCli(c net.Conn, deadlineTime time.Duration) (err error) {
	var header []byte
	header, err = Buffers.Get(PacketHeaderSize)
	if err != nil {
		header = make([]byte, PacketHeaderSize)
	}
	defer Buffers.Put(header)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}
	if p.ArgLen > 0 {
		if err = ReadFull(c, &p.Arg, int(p.ArgLen)); err != nil {
			return
		}
	}
	if p.Size < 0 {
		return
	}
	size := p.Size
	return p.ReadFull(c, p.Opcode, int(size))
}

func (p *Packet) SendRespToRDMAConn(conn *rdma.Connection) (err error) {
	var dataBuffer []byte
	var offset uint32 = 0

	if dataBuffer, err = conn.GetConnTxDataBuffer(145); err != nil {
		return
	}

	defer func() {
		if dataBuffer != nil {
			if err = conn.ReleaseConnTxDataBuffer(dataBuffer); err != nil {
			}
		}
	}()

	p.MarshalHeader(dataBuffer[0:PacketHeaderSize])
	offset += PacketHeaderSize + 8
	if p.ArgLen != 0 {
		copy(dataBuffer[65:105], p.Arg)
	}
	offset += 40

	if p.Data != nil && p.Size != 0 {
		copy(dataBuffer[105:105+int(p.Size)], p.Data)
	}

	if _, err = conn.WriteBuffer(dataBuffer, int(105+p.Size)); err != nil {
		return
	}
	return
}
