// Copyright 2022 The CubeFS Authors.
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

package bcache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/buf"
)

const (
	OpBlockCachePut uint8 = 0xB1
	OpBlockCacheGet uint8 = 0xB2
	OpBlockCacheDel uint8 = 0xB3
)

const (
	CacheMagic uint8 = 0xFF
)

const (
	PacketHeaderSize = 11
)

var Buffers *buf.BufferPool

type PutCacheRequest struct {
	CacheKey string `json:"key"`
	Data     []byte `json:"data"`
	VolName  string `json:"volName"`
}

func (req *PutCacheRequest) Marshal() (result []byte, err error) {
	buff := bytespool.AllocWithZeroLengthBuffer(4 + len(req.CacheKey) + 4 + len(req.Data) + 4 + len(req.VolName))
	// cache key
	err = binary.Write(buff, binary.BigEndian, uint32(len(req.CacheKey)))
	if err != nil {
		return
	}
	_, err = buff.Write([]byte(req.CacheKey))
	if err != nil {
		return
	}

	// data
	err = binary.Write(buff, binary.BigEndian, uint32(len(req.Data)))
	if err != nil {
		return
	}
	_, err = buff.Write(req.Data)
	if err != nil {
		return
	}

	// vol
	err = binary.Write(buff, binary.BigEndian, uint32(len(req.VolName)))
	if err != nil {
		return
	}
	_, err = buff.Write([]byte(req.VolName))
	if err != nil {
		return
	}
	return buff.Bytes(), nil
}

func (req *PutCacheRequest) UnmarshalValue(data []byte) (err error) {
	reader := bytes.NewReader(data)
	var keyLen uint32
	err = binary.Read(reader, binary.BigEndian, &keyLen)
	if err != nil {
		return
	}

	keyBytes := bytespool.Alloc(int(keyLen))
	defer bytespool.Free(keyBytes)
	_, err = reader.Read(keyBytes)
	if err != nil {
		return err
	}
	req.CacheKey = string(keyBytes)

	var dataLen uint32
	err = binary.Read(reader, binary.BigEndian, &dataLen)
	if err != nil {
		return err
	}
	dataBytes := bytespool.Alloc(int(dataLen))
	defer bytespool.Free(dataBytes)
	err = binary.Read(reader, binary.BigEndian, &dataBytes)
	if err != nil {
		return err
	}
	req.Data = dataBytes

	var volNameLen uint32
	err = binary.Read(reader, binary.BigEndian, &volNameLen)
	if err != nil {
		return err
	}
	volNameBytes := bytespool.Alloc(int(volNameLen))
	defer bytespool.Free(volNameBytes)
	_, err = reader.Read(volNameBytes)
	if err != nil {
		return err
	}
	req.VolName = string(volNameBytes)

	return nil
}

type GetCacheRequest struct {
	CacheKey string `json:"key"`
	Offset   uint64 `json:"offset"`
	Size     uint32 `json:"size"`
}

func (req *GetCacheRequest) Marshal() (result []byte, err error) {
	buff := bytespool.AllocWithZeroLengthBuffer(4 + len(req.CacheKey) + 8 + 4)
	// cache key
	err = binary.Write(buff, binary.BigEndian, uint32(len(req.CacheKey)))
	if err != nil {
		return
	}
	_, err = buff.Write([]byte(req.CacheKey))
	if err != nil {
		return
	}

	err = binary.Write(buff, binary.BigEndian, &req.Offset)
	if err != nil {
		return
	}
	err = binary.Write(buff, binary.BigEndian, &req.Size)
	if err != nil {
		return
	}

	return buff.Bytes(), nil
}

func (req *GetCacheRequest) UnmarshalValue(data []byte) (err error) {
	reader := bytes.NewReader(data)
	var keyLen uint32
	err = binary.Read(reader, binary.BigEndian, &keyLen)
	if err != nil {
		return
	}

	keyBytes := bytespool.Alloc(int(keyLen))
	defer bytespool.Free(keyBytes)
	_, err = reader.Read(keyBytes)
	if err != nil {
		return err
	}
	req.CacheKey = string(keyBytes)

	err = binary.Read(reader, binary.BigEndian, &req.Offset)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &req.Size)
	if err != nil {
		return err
	}
	return nil
}

type GetCachePathResponse struct {
	CachePath string `json:"path"`
}

func (req *GetCachePathResponse) Marshal() (result []byte, err error) {
	buff := bytespool.AllocWithZeroLengthBuffer(4 + len(req.CachePath))
	// cache key
	err = binary.Write(buff, binary.BigEndian, uint32(len(req.CachePath)))
	if err != nil {
		return
	}
	_, err = buff.Write([]byte(req.CachePath))
	if err != nil {
		return
	}

	return buff.Bytes(), nil
}

func (req *GetCachePathResponse) UnmarshalValue(data []byte) (err error) {
	reader := bytes.NewReader(data)
	var keyLen uint32
	err = binary.Read(reader, binary.BigEndian, &keyLen)
	if err != nil {
		return
	}

	keyBytes := bytespool.Alloc(int(keyLen))
	defer bytespool.Free(keyBytes)
	_, err = reader.Read(keyBytes)
	if err != nil {
		return err
	}
	req.CachePath = string(keyBytes)
	return nil
}

type DelCacheRequest struct {
	CacheKey string `json:"key"`
}

func (req *DelCacheRequest) Marshal() (result []byte, err error) {
	buff := bytespool.AllocWithZeroLengthBuffer(4 + len(req.CacheKey))
	// cache key
	err = binary.Write(buff, binary.BigEndian, uint32(len(req.CacheKey)))
	if err != nil {
		return
	}
	_, err = buff.Write([]byte(req.CacheKey))
	if err != nil {
		return
	}

	return buff.Bytes(), nil
}

func (req *DelCacheRequest) UnmarshalValue(data []byte) (err error) {
	reader := bytes.NewReader(data)
	var keyLen uint32
	err = binary.Read(reader, binary.BigEndian, &keyLen)
	if err != nil {
		return
	}

	keyBytes := bytespool.Alloc(int(keyLen))
	defer bytespool.Free(keyBytes)
	_, err = reader.Read(keyBytes)
	if err != nil {
		return err
	}
	req.CacheKey = string(keyBytes)
	return nil
}

type BlockCachePacket struct {
	Magic      uint8
	Opcode     uint8
	ResultCode uint8 // 3
	CRC        uint32
	Size       uint32 // Data's size ; header size: 3 + 8 = 11
	Data       []byte
	StartT     int64
}

func NewBlockCachePacket() *BlockCachePacket {
	p := new(BlockCachePacket)
	p.Magic = CacheMagic
	p.StartT = time.Now().UnixNano()
	return p
}

func (p *BlockCachePacket) String() string {
	return fmt.Sprintf("OpMsg(%v)", p.GetOpMsg())
}

func (p *BlockCachePacket) GetOpMsg() (m string) {
	switch p.Opcode {
	case OpBlockCachePut:
		m = "OpBlockCachePut"
	case OpBlockCacheGet:
		m = "OpBlockCacheGet"
	case OpBlockCacheDel:
		m = "OpBlockCacheDel"
	default:
		// do nothing
	}
	return
}

func (p *BlockCachePacket) GetResultMsg() (m string) {
	if p == nil {
		return ""
	}
	switch p.ResultCode {
	case proto.OpErr:
		m = "Err: " + string(p.Data)
	case proto.OpOk:
		m = "Ok"
	case proto.OpNotExistErr:
		m = "NotExistErr"
	default:
		return fmt.Sprintf("Unknown ResultCode(%v)", p.ResultCode)
	}
	return
}

func (p *BlockCachePacket) MarshalHeader(out []byte) {
	out[0] = p.Magic
	out[1] = p.Opcode
	out[2] = p.ResultCode
	binary.BigEndian.PutUint32(out[3:7], p.CRC)
	binary.BigEndian.PutUint32(out[7:11], p.Size)
}

func (p *BlockCachePacket) UnMarshalHeader(in []byte) error {
	p.Magic = in[0]
	if p.Magic != CacheMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Magic)))
	}
	p.Opcode = in[1]
	p.ResultCode = in[2]
	p.CRC = binary.BigEndian.Uint32(in[3:7])
	p.Size = binary.BigEndian.Uint32(in[7:11])
	return nil
}

func (p *BlockCachePacket) MarshalData(v interface{}) error {
	data, err := json.Marshal(v)
	if err == nil {
		p.Data = data
		p.Size = uint32(len(p.Data))
	}
	return err
}

func (p *BlockCachePacket) UnmarshalData(v interface{}) error {
	return json.Unmarshal(p.Data, v)
}

func (p *BlockCachePacket) WriteToConn(c net.Conn) (err error) {
	header, err := Buffers.Get(PacketHeaderSize)
	if err != nil {
		header = make([]byte, PacketHeaderSize)
	}
	defer Buffers.Put(header)
	c.SetWriteDeadline(time.Now().Add(proto.WriteDeadlineTime * time.Second))
	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if p.Data != nil {
			_, err = c.Write(p.Data[:p.Size])
		}
	}
	return
}

func (p *BlockCachePacket) ReadFromConn(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec != proto.NoReadDeadlineTime {
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
	if err = p.UnMarshalHeader(header); err != nil {
		return
	}
	size := p.Size
	//if p.Opcode == OpBlockCachePut || p.Opcode == OpBlockCacheDel {
	//	size = 0
	//}
	p.Data = make([]byte, size)
	if n, err = io.ReadFull(c, p.Data[:size]); err != nil {
		return err
	}
	if n != int(size) {
		return syscall.EBADMSG
	}
	return nil
}

func (p *BlockCachePacket) PacketOkReplay() {
	p.ResultCode = proto.OpOk
	p.Size = 0
	p.Data = nil
}

func (p *BlockCachePacket) PacketOkWithBody(reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = proto.OpOk
}

func (p *BlockCachePacket) PacketErrorWithBody(code uint8, reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = code
}
