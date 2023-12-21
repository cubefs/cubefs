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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cubefs/cubefs/flashnode/cachengine"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func (f *FlashNode) preHandle(conn net.Conn, p *proto.Packet) error {
	if p.Opcode == proto.OpCacheRead && !f.readLimiter.Allow() {
		metric := exporter.NewTPCnt("NodeReqLimit")
		metric.Set(nil)
		err := errors.NewErrorf("%s", "flashnode read request was been limited")
		log.LogWarnf("action[preHandle] %s, remote address:%s", err.Error(), conn.RemoteAddr())
		return err
	}
	return nil
}

func (f *FlashNode) handlePacket(conn net.Conn, p *proto.Packet) (err error) {
	metric := exporter.NewTPCnt(p.GetOpMsg())
	defer func() { metric.Set(err) }()

	switch p.Opcode {
	case proto.OpFlashNodeHeartbeat:
		err = f.opFlashNodeHeartbeat(conn, p)
	case proto.OpCacheRead:
		err = f.opCacheRead(conn, p)
	case proto.OpCachePrepare:
		err = f.opPrepare(conn, p)
	default:
		err = fmt.Errorf("unknown Opcode:%d", p.Opcode)
	}
	if err != nil {
		err = errors.NewErrorf("%s [%s] req: %d - %s", conn.RemoteAddr().String(),
			p.GetOpMsg(), p.GetReqID(), err.Error())
	}
	return
}

func (f *FlashNode) opFlashNodeHeartbeat(conn net.Conn, p *proto.Packet) (err error) {
	p.PacketOkReply()
	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("ack master response: %s", err.Error())
		return err
	}
	log.LogInfof("[opMasterHeartbeat] master:%s", conn.RemoteAddr().String())
	return
}

func (f *FlashNode) opCacheRead(conn net.Conn, p *proto.Packet) (err error) {
	var volume string
	defer func() {
		if err != nil {
			logContent := fmt.Sprintf("action[opCacheRead] volume:[%v], logMsg:%v.", volume,
				p.LogMessage(p.GetOpMsg(), conn.RemoteAddr().String(), p.StartT, err))
			log.LogError(logContent)
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
			p.WriteToConn(conn)
		}
	}()

	ctx, ctxCancel := context.WithTimeout(context.Background(), proto.ReadCacheTimeout*time.Second*2)
	defer ctxCancel()

	req := new(proto.CacheReadRequest)
	if err = p.UnmarshalDataPb(req); err != nil {
		return
	}
	volume = req.CacheRequest.Volume

	cr := req.CacheRequest
	block, err := f.cacheEngine.GetCacheBlockForRead(volume, cr.Inode, cr.FixedFileOffset, cr.Version, req.Size_)
	if err != nil {
		if block, err = f.cacheEngine.CreateBlock(cr); err != nil {
			return err
		}
		go block.InitOnce(f.cacheEngine, cr.Sources)
	}
	err = f.doStreamReadRequest(ctx, conn, req, p, block)
	return
}

func (f *FlashNode) doStreamReadRequest(ctx context.Context, conn net.Conn, req *proto.CacheReadRequest, p *proto.Packet, block *cachengine.CacheBlock) (err error) {
	const action = "action[doStreamReadRequest]"
	needReplySize := uint32(req.Size_)
	offset := int64(req.Offset)
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s cache block(%v) err:%v", action, block.String(), err)
		}
	}()

	for needReplySize > 0 {
		err = nil
		reply := proto.NewPacket()
		reply.ReqID = p.ReqID
		reply.StartT = p.StartT

		currReadSize := uint32(util.Min(int(needReplySize), util.ReadBlockSize))

		var bufOnce sync.Once
		buf, bufErr := proto.Buffers.Get(util.ReadBlockSize)
		bufRelease := func() {
			bufOnce.Do(func() {
				if bufErr == nil {
					proto.Buffers.Put(reply.Data[:util.ReadBlockSize])
				}
			})
		}
		if bufErr != nil {
			buf = make([]byte, currReadSize)
		}
		reply.Data = buf[:currReadSize]

		reply.ExtentOffset = offset
		p.Size = currReadSize
		p.ExtentOffset = offset

		reply.CRC, err = block.Read(ctx, reply.Data[:], offset, int64(currReadSize))
		if err != nil {
			bufRelease()
			return
		}
		p.CRC = reply.CRC

		reply.Size = currReadSize
		reply.ResultCode = proto.OpOk
		reply.Opcode = p.Opcode
		p.ResultCode = proto.OpOk
		if err = reply.WriteToConn(conn); err != nil {
			bufRelease()
			log.LogErrorf("%s volume:[%v] %v.", action, req.CacheRequest.Volume,
				reply.LogMessage(reply.GetOpMsg(), conn.RemoteAddr().String(), reply.StartT, err))
			return
		}

		needReplySize -= currReadSize
		offset += int64(currReadSize)
		bufRelease()
		log.LogInfof("%s ReqID[%v] volume:[%v] reply[%v] block[%v] .", action, p.ReqID, req.CacheRequest.Volume,
			reply.LogMessage(reply.GetOpMsg(), conn.RemoteAddr().String(), reply.StartT, err), block.String())
	}
	p.PacketOkReply()
	return
}

func (f *FlashNode) opPrepare(conn net.Conn, p *proto.Packet) (err error) {
	action := "action[opPrepare]"
	var volume string
	defer func() {
		if err != nil {
			log.LogErrorf("%s volume:[%v] %v.", action, volume,
				p.LogMessage(p.GetOpMsg(), conn.RemoteAddr().String(), p.StartT, err))
		}
	}()

	req := new(proto.CachePrepareRequest)
	if err = p.UnmarshalDataPb(req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		if e := p.WriteToConn(conn); e != nil {
			log.LogErrorf("%s write to conn %v.", action, e)
		}
		return
	}
	volume = req.CacheRequest.Volume

	p.PacketOkReply()
	if e := p.WriteToConn(conn); e != nil {
		log.LogErrorf("%s write to conn volume:%s %v.", action, volume, e)
	}

	if err = f.cacheEngine.PrepareCache(p.ReqID, req.CacheRequest); err != nil {
		return err
	}
	if len(req.FlashNodes) > 0 {
		f.dispatchRequestToFollowers(req)
	}
	return
}

func (f *FlashNode) dispatchRequestToFollowers(request *proto.CachePrepareRequest) {
	req := &proto.CachePrepareRequest{
		CacheRequest: request.CacheRequest,
		FlashNodes:   make([]string, 0),
	}
	wg := sync.WaitGroup{}
	for idx, addr := range request.FlashNodes {
		if addr == f.localAddr {
			continue
		}
		wg.Add(1)
		log.LogDebugf("dispatchRequestToFollowers: try to prepare on addr:%s (%d/%d)", addr, idx, len(request.FlashNodes))
		go func(addr string) {
			defer wg.Done()
			if err := f.sendPrepareRequest(addr, req); err != nil {
				log.LogErrorf("dispatchRequestToFollowers: failed to distribute request to addr(%v) err(%v)", addr, err)
			}
		}(addr)
	}
	wg.Wait()
}

func (f *FlashNode) sendPrepareRequest(addr string, req *proto.CachePrepareRequest) (err error) {
	action := "action[sendPrepareRequest]"
	conn, err := f.connPool.GetConnect(addr)
	if err != nil {
		return err
	}
	defer func() {
		f.connPool.PutConnect(conn, err != nil)
	}()
	log.LogDebugf("%s to addr:%s request:%v", action, addr, req)

	followerPacket := proto.NewPacketReqID()
	followerPacket.Opcode = proto.OpCachePrepare
	if err = followerPacket.MarshalDataPb(req); err != nil {
		log.LogWarnf("%s failed to MarshalDataPb (%+v) err(%v)", action, followerPacket, err)
		return err
	}
	if err = followerPacket.WriteToNoDeadLineConn(conn); err != nil {
		log.LogWarnf("%s failed to write to addr(%s) err(%v)", action, addr, err)
		return err
	}
	reply := proto.NewPacket()
	if err = reply.ReadFromConn(conn, 30); err != nil {
		log.LogWarnf("%s read reply(%v) from addr(%s) err(%v)", action, reply, addr, err)
		return err
	}
	if reply.ResultCode != proto.OpOk {
		log.LogWarnf("%s reply(%v) from addr(%s) ResultCode(%d)", action, reply, addr, reply.ResultCode)
		return fmt.Errorf("ResultCode(%v)", reply.ResultCode)
	}
	return nil
}
