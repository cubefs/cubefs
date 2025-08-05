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
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/flashnode/cachengine"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func (f *FlashNode) preHandle(conn net.Conn, p *Packet) error {
	if p.Opcode == proto.OpCachePrepare || p.Opcode == proto.OpCacheRead {
		if f.cacheEngine == nil {
			return errors.New("cache engine not started")
		}
	}
	// request rate limit for entire flash node
	if !(p.Opcode == proto.OpCacheRead && f.nodeLimit != 0) {
		return nil
	}
	if !f.nodeLimiter.Allow() {
		err := errors.NewErrorf("flashnode request is limited(%d)", f.nodeLimit)
		metric := exporter.NewTPCnt("NodeReqLimit")
		log.LogWarnf("action[preHandle] %s, remote address:%s", err.Error(), conn.RemoteAddr())
		metric.Set(nil)
		return err
	}
	return nil
}

// handlePacket handles the tcp packet operations.
func (f *FlashNode) handlePacket(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	metric := exporter.NewTPCnt(p.GetOpMsg())
	defer func() {
		metric.Set(err)
	}()
	switch p.Opcode {
	case proto.OpFlashNodeHeartbeat:
		err = f.opFlashNodeHeartbeat(conn, p, remoteAddr)
	case proto.OpCacheRead:
		err = f.opCacheRead(conn, p, remoteAddr)
	case proto.OpCachePrepare:
		err = f.opPrepare(conn, p, remoteAddr)
	default:
		err = fmt.Errorf("unknown Opcode:%d", p.Opcode)
	}
	if err != nil {
		err = errors.NewErrorf("%s [%s] req: %d - %s", remoteAddr, p.GetOpMsg(), p.GetReqID(), err.Error())
	}
	return
}

func (f *FlashNode) opFlashNodeHeartbeat(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	p.PacketOkReply()
	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("ack master response: %s", err.Error())
		return err
	}
	log.LogInfof("%s [opMasterHeartbeat] ", remoteAddr)
	return
}

func (f *FlashNode) opCacheRead(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	var (
		block  *cachengine.CacheBlock
		req    *proto.CacheReadRequest
		volume string
	)

	defer func() {
		if err != nil {
			logContent := fmt.Sprintf("action[opCacheRead] volume:[%v], logMsg:%v.", volume,
				p.LogMessage(p.GetOpMsg(), remoteAddr, p.StartT, err))
			log.LogErrorf(logContent)
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
			_ = respondToClient(conn, p)
		}
	}()
	ctx, ctxCancel := context.WithTimeout(context.Background(), proto.ReadCacheTimeout*time.Second*2)
	defer ctxCancel()
	if req, err = UnMarshalPacketToCacheRead(p); err != nil {
		return
	}
	volume = req.CacheRequest.Volume
	if !f.volLimitAllow(volume) {
		err = errors.NewErrorf("volume(%s) request is limited(%d)", volume, f.volLimitMap[volume])
		log.LogWarnf("action[preHandle] %s, remote address:%s", err.Error(), remoteAddr)
		metric := exporter.NewTPCnt("VolReqLimit")
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		_ = respondToClient(conn, p)
		err = nil
		metric.Set(nil)
		return
	}
	if block, err = f.cacheEngine.GetCacheBlockForRead(volume, req.CacheRequest.Inode, req.CacheRequest.FixedFileOffset, req.CacheRequest.Version, req.Size_); err != nil {
		if block, err = f.cacheEngine.CreateBlock(req.CacheRequest); err != nil {
			return err
		}
		go block.InitOnce(f.cacheEngine, req.CacheRequest.Sources)
	}
	if err = f.doStreamReadRequest(ctx, conn, req, p, block); err != nil {
		return
	}
	return
}

func (f *FlashNode) doStreamReadRequest(ctx context.Context, conn net.Conn, req *proto.CacheReadRequest, p *Packet, block *cachengine.CacheBlock) (err error) {
	needReplySize := uint32(req.Size_)
	offset := int64(req.Offset)
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[doStreamReadRequest] cache block(%v) err:%v", block.String(), err)
		}
	}()
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		reply := NewCacheReply()
		reply.ReqID = p.ReqID
		reply.StartT = p.StartT
		currReadSize := uint32(util.Min(int(needReplySize), util.ReadBlockSize))
		if currReadSize == util.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(util.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}

		reply.ExtentOffset = offset
		p.Size = currReadSize
		p.ExtentOffset = offset

		err = func() error {
			var storeErr error
			reply.CRC, storeErr = block.Read(ctx, reply.Data[0:currReadSize], offset, int64(currReadSize))
			return storeErr
		}()
		p.CRC = reply.CRC
		if err != nil {
			if currReadSize == util.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		reply.Size = currReadSize
		reply.ResultCode = proto.OpOk
		reply.Opcode = p.Opcode
		p.ResultCode = proto.OpOk

		err = func() error {
			return reply.WriteToConn(conn)
		}()
		if err != nil {
			if currReadSize == util.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			logContent := fmt.Sprintf("action[doStreamReadRequest] volume:[%v] %v.", req.CacheRequest.Volume,
				reply.LogMessage(reply.GetOpMsg(), conn.RemoteAddr().String(), reply.StartT, err))
			log.LogErrorf(logContent)
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		if currReadSize == util.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
		logContent := fmt.Sprintf("action[doStreamReadRequest] ReqID[%v] volume:[%v] reply[%v] block[%v] .", p.ReqID, req.CacheRequest.Volume,
			reply.LogMessage(reply.GetOpMsg(), conn.RemoteAddr().String(), reply.StartT, err), block.String())
		log.LogReadf(logContent)
	}
	p.PacketOkReply()
	return
}

func (f *FlashNode) opPrepare(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	var req *proto.CachePrepareRequest
	var volume string
	defer func() {
		if err != nil {
			logContent := fmt.Sprintf("action[opPrepare] volume:[%v] %v.", volume, p.LogMessage(p.GetOpMsg(), remoteAddr, p.StartT, err))
			log.LogErrorf(logContent)
		}
	}()
	reqID := p.ReqID
	if req, err = UnMarshalPacketToCachePrepare(p); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		_ = respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	_ = respondToClient(conn, p)
	volume = req.CacheRequest.Volume
	if err = f.cacheEngine.PrepareCache(reqID, req.CacheRequest); err != nil {
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
	for _, n := range request.FlashNodes {
		if strings.Split(n, ":")[0] == f.localAddr {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := f.sendPrepareRequest(req, addr); err != nil {
				log.LogErrorf("dispatchRequestToFollowers: failed to distribute request to addr(%v) err(%v)", addr, err)
			}
		}(n)
	}
	wg.Wait()
}

func (f *FlashNode) sendPrepareRequest(req *proto.CachePrepareRequest, target string) (err error) {
	var conn *net.TCPConn
	conn, err = f.connPool.GetConnect(target)
	if err != nil {
		return err
	}
	defer func() {
		f.connPool.PutConnect(conn, err != nil)
	}()

	// TODO: xxx
	// log.LogDebugf("action[sendPrepareRequest] request:%v", req)
	// followerPacket, err := MarshalCachePrepareRequestToPacket(req)
	// if err != nil {
	// 	return err
	// }
	// if err = followerPacket.WriteToConnNs(conn, CacheReqWriteTimeoutMilliSec*1e6); err != nil {
	// 	log.LogWarnf("action[sendPrepareRequest]: failed to write to addr(%v) err(%v)", target, err)
	// 	return
	// }
	// replyPacket := common.NewCacheReply(followerPacket.Ctx())
	// // replyPacket = repl.NewReplProtocol(followerPacket.Ctx())
	// if err = replyPacket.ReadFromConnNs(conn, CacheReqReadTimeoutMilliSec*1e6); err != nil {
	// 	log.LogWarnf("action[sendPrepareRequest]: failed to ReadFromConn, replyPacket(%v), fg host(%v), err(%v)", replyPacket, target, err)
	// 	return
	// }
	// if replyPacket.ResultCode != proto.OpOk {
	// 	log.LogWarnf("action[sendPrepareRequest]: ResultCode NOK, replyPacket(%v), fg host(%v), ResultCode(%v)", replyPacket, target, replyPacket.ResultCode)
	// 	err = fmt.Errorf("ResultCode NOK (%v)", replyPacket.ResultCode)
	// 	return
	// }
	return
}

// Reply data through tcp connection to the client.
func respondToClient(conn net.Conn, p *Packet) (err error) {
	// Handle panic
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("respondToClient: panic occurred: %v\n%v", r, string(debug.Stack()))
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()

	// process data and send reply though specified tcp connection.
	err = p.WriteToConn(conn)
	if err != nil {
		log.LogErrorf("response to client[%s], request[%s], response packet[%s]",
			err.Error(), p.GetOpMsg(), p.GetResultMsg())
	}
	return
}
