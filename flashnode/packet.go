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

package flashnode

import (
	"context"
	"io"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

type Packet struct {
	proto.Packet
}

func (p *Packet) ReadFromReader(reader io.Reader) (err error) {
	header, err := proto.Buffers.Get(unit.PacketHeaderSize)
	if err != nil {
		header = make([]byte, unit.PacketHeaderSize)
	}
	defer proto.Buffers.Put(header)
	var n int
	if n, err = io.ReadFull(reader, header); err != nil {
		return
	}
	if n != unit.PacketHeaderSize {
		return syscall.EBADMSG
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.ArgLen > 0 {
		p.Arg = make([]byte, int(p.ArgLen))
		if _, err = io.ReadFull(reader, p.Arg[:int(p.ArgLen)]); err != nil {
			return err
		}
	}

	if p.Size < 0 {
		return syscall.EBADMSG
	}
	size := p.Size
	if (p.Opcode == proto.OpRead || p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpExtentRepairRead || p.Opcode == proto.OpStreamFollowerRead) && p.ResultCode == proto.OpInitResultCode {
		size = 0
	}
	var buffErr error
	if p.Data, buffErr = proto.Buffers.Get(int(size)); buffErr != nil {
		p.Data = make([]byte, size)
	}
	if n, err = io.ReadFull(reader, p.Data[:size]); err != nil {
		return err
	}
	if n != int(size) {
		return syscall.EBADMSG
	}
	return nil
}

func NewPacket(ctx context.Context) *Packet {
	p := &Packet{}
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	p.SetCtx(ctx)
	return p
}

func NewCacheReply(ctx context.Context) *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	return p
}

func NewPreparePacket() *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.Opcode = proto.OpCachePrepare
	return p
}

func MarshalCachePrepareRequestToPacket(req *proto.CachePrepareRequest) (packet *Packet, err error) {
	packet = NewPreparePacket()
	if err = packet.MarshalDataPb(req); err != nil {
		log.LogWarnf("MarshalCachePrepareRequestToPacket: failed to MarshalDataPb (%+v). err(%v)", packet, err)
		return
	}
	return packet, err
}

func UnMarshalPacketToCachePrepare(p *Packet) (req *proto.CachePrepareRequest, err error) {
	req = &proto.CachePrepareRequest{}
	if err = p.UnmarshalDataPb(req); err != nil {
		err = errors.NewErrorf("UnmarshalDataPb data length[%v] err[%v]", len(p.Data), err)
		return
	}
	return
}

func UnMarshalPacketToCacheRead(p *Packet) (req *proto.CacheReadRequest, err error) {
	req = &proto.CacheReadRequest{}
	if err = p.UnmarshalDataPb(req); err != nil {
		err = errors.NewErrorf("UnmarshalDataPb data length[%v] err[%v]", len(p.Data), err)
		return
	}
	return
}
