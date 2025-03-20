// Copyright 2023 The CubeFS Authors.
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
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/cubefs/cubefs/util/stat"
)

func (f *FlashNode) preHandle(conn net.Conn, p *proto.Packet) error {
	if (p.Opcode == proto.OpFlashNodeCacheRead || p.Opcode == proto.OpFlashNodeCachePrepare) && !f.readLimiter.Allow() {
		metric := exporter.NewTPCnt("NodeReqLimit")
		metric.Set(nil)
		err := errors.NewErrorf("%s", "flashnode read request was been limited")
		log.LogWarnf("action[preHandle] %s, remote address:%s", err.Error(), conn.RemoteAddr())
		return err
	}
	return nil
}

func (f *FlashNode) handlePacket(conn net.Conn, p *proto.Packet) (err error) {
	switch p.Opcode {
	case proto.OpFlashNodeHeartbeat:
		err = f.opFlashNodeHeartbeat(conn, p)
	case proto.OpFlashNodeCachePrepare:
		err = f.opCachePrepare(conn, p)
	case proto.OpFlashNodeCacheRead:
		err = f.opCacheRead(conn, p)
	case proto.OpFlashNodeSetReadIOLimits:
		err = f.opSetReadIOLimits(conn, p)
	case proto.OpFlashNodeSetWriteIOLimits:
		err = f.opSetWriteIOLimits(conn, p)
	default:
		err = fmt.Errorf("unknown Opcode:%d", p.Opcode)
	}

	return
}

func (f *FlashNode) SetTimeout(handleReadTimeout int, readDataNodeTimeout int) {
	if f.handleReadTimeout != handleReadTimeout && handleReadTimeout > 0 {
		log.LogInfof("FlashNode set handleReadTimeout from %d to %d", f.handleReadTimeout, handleReadTimeout)
		f.handleReadTimeout = handleReadTimeout
		f.limitWrite.ResetIOEx(f.diskWriteIocc*len(f.disks), f.diskWriteIoFactorFlow, f.handleReadTimeout)
		f.limitWrite.ResetFlow(f.diskWriteFlow)
	}
	f.cacheEngine.SetReadDataNodeTimeout(readDataNodeTimeout)
}

func (f *FlashNode) opFlashNodeHeartbeat(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data
	req := &proto.HeartBeatRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}

	decode := json.NewDecoder(bytes.NewBuffer(data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err == nil {
		f.SetTimeout(req.FlashNodeHandleReadTimeout, req.FlashNodeReadDataNodeTimeout)
	} else {
		log.LogErrorf("decode HeartBeatRequest error: %s", err.Error())
	}

	resp := &proto.FlashNodeHeartbeatResponse{}
	resp.Stat = make([]*proto.FlashNodeDiskCacheStat, 0)
	for _, cacheStat := range f.cacheEngine.GetHeartBeatCacheStat() {
		stat := &proto.FlashNodeDiskCacheStat{
			DataPath:  cacheStat.DataPath,
			Medium:    cacheStat.Medium,
			Total:     cacheStat.Total,
			MaxAlloc:  cacheStat.MaxAlloc,
			HasAlloc:  cacheStat.HasAlloc,
			FreeSpace: cacheStat.MaxAlloc - cacheStat.HasAlloc,
			HitRate:   cacheStat.HitRate,
			Evicts:    cacheStat.Evicts,
			ReadRps:   f.readRps,
			KeyNum:    cacheStat.Num,
			Status:    cacheStat.Status,
		}
		resp.Stat = append(resp.Stat, stat)
	}
	writeStatus := proto.FlashNodeLimiterStatus{Status: f.limitWrite.Status(true), DiskNum: len(f.disks), ReadTimeoutSec: f.handleReadTimeout}
	readStatus := proto.FlashNodeLimiterStatus{Status: f.limitRead.Status(true), DiskNum: len(f.disks), ReadTimeoutSec: f.handleReadTimeout}
	resp.LimiterStatus = &proto.FlashNodeLimiterStatusInfo{WriteStatus: writeStatus, ReadStatus: readStatus}
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)

	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("ack master response: %s", err.Error())
		return err
	}
	log.LogInfof("[opMasterHeartbeat] master:%s handleReadTimeout(%v) readDataNodeTimeout(%v)",
		conn.RemoteAddr().String(), req.FlashNodeHandleReadTimeout, req.FlashNodeReadDataNodeTimeout)
	return
}

func (f *FlashNode) opCacheRead(conn net.Conn, p *proto.Packet) (err error) {
	var volume string
	bgTime := stat.BeginStat()
	metricOpCacheRead := exporter.NewTPCnt("opCacheRead")
	defer func() {
		metricOpCacheRead.SetWithLabels(err, map[string]string{exporter.Vol: volume})
		stat.EndStat("FlashNode:opCacheRead", err, bgTime, 1)
	}()

	defer func() {
		if err != nil {
			log.LogWarnf("action[opCacheRead] volume:[%s], logMsg:%s", volume,
				p.LogMessage(p.GetOpMsg(), conn.RemoteAddr().String(), p.StartT, err))
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
			if e := p.WriteToConn(conn); e != nil {
				log.LogErrorf("action[opCacheRead] write to conn %v", e)
			}
		}
	}()

	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Duration(f.handleReadTimeout)*time.Second)
	defer ctxCancel()

	req := new(proto.CacheReadRequest)
	if err = p.UnmarshalDataPb(req); err != nil {
		return
	}
	if req.CacheRequest == nil {
		err = fmt.Errorf("no cache read request")
		return
	}
	volume = req.CacheRequest.Volume

	cr := req.CacheRequest
	block, err := f.cacheEngine.GetCacheBlockForRead(volume, cr.Inode, cr.FixedFileOffset, cr.Version, req.Size_)
	if err != nil {
		log.LogInfof("opCacheRead: GetCacheBlockForRead failed, req(%v) err(%v)", req, err)
		hitRateMap := f.cacheEngine.GetHitRate()
		for dataPath, hitRate := range hitRateMap {
			if hitRate < f.lowerHitRate {
				log.LogWarnf("opCacheRead: flashnode %v dataPath(%v) is lower hitrate %v", f.localAddr, dataPath, hitRate)
				errMetric := exporter.NewCounter("lowerHitRate")
				errMetric.AddWithLabels(1, map[string]string{exporter.FlashNode: f.localAddr, exporter.Disk: dataPath, exporter.Err: "LowerHitRate"})
			}
		}
		bgTime2 := stat.BeginStat()
		missCacheMetric := exporter.NewTPCnt("MissCacheRead")
		// try to cache more miss data, but reply to client more quickly
		if writable := f.limitWrite.TryRunWithContext(ctx, int(req.Size_), func() {
			if block2, err := f.cacheEngine.CreateBlock(cr, conn.RemoteAddr().String(), false); err != nil {
				log.LogWarnf("opCacheRead: CreateBlock failed, req(%v) err(%v)", req, err)
				return
			} else {
				block2.InitOnceForCacheRead(f.cacheEngine, cr.Sources)
			}
		}); !writable {
			err = fmt.Errorf("create block cache limited")
			stat.EndStat("MissCacheReadLimit", err, bgTime2, 1)
			missCacheMetric.SetWithLabels(err, map[string]string{exporter.Vol: volume})
			return
		}
		select {
		case <-ctx.Done():
			stat.EndStat("MissCacheReadCancel", ctx.Err(), bgTime2, 1)
			missCacheMetric.SetWithLabels(err, map[string]string{exporter.Vol: volume})
			return ctx.Err()
		default:
			block, err = f.cacheEngine.GetCacheBlockForRead(volume, cr.Inode, cr.FixedFileOffset, cr.Version, req.Size_)
		}
		stat.EndStat("MissCacheRead", err, bgTime2, 1)
		missCacheMetric.SetWithLabels(err, map[string]string{exporter.Vol: volume})
		if err != nil {
			return err
		}
	}
	bgTime2 := stat.BeginStat()
	hitCacheMetric := exporter.NewTPCnt("HitCacheRead")
	// reply to client as quick as possible if hit cache
	err2 := f.limitRead.RunNoWait(int(req.Size_), false, func() {
		err = f.doStreamReadRequest(ctx, conn, req, p, block)
	})
	if err2 != nil {
		err = err2
		stat.EndStat("HitCacheRead", err, bgTime2, 1)
		hitCacheMetric.SetWithLabels(err, map[string]string{exporter.Vol: volume})
		return
	}
	stat.EndStat("HitCacheRead", err, bgTime2, 1)
	hitCacheMetric.SetWithLabels(err, map[string]string{exporter.Vol: volume})
	return
}

func (f *FlashNode) doStreamReadRequest(ctx context.Context, conn net.Conn, req *proto.CacheReadRequest, p *proto.Packet, block *cachengine.CacheBlock) (err error) {
	const action = "action[doStreamReadRequest]"
	needReplySize := uint32(req.Size_)
	offset := int64(req.Offset)
	defer func() {
		if err != nil {
			log.LogWarnf("%s cache block(%v) err:%v", action, block.String(), err)
		} else {
			f.metrics.updateReadCountMetric(block.GetRootPath())
			f.metrics.updateReadBytesMetric(req.Size_, block.GetRootPath())
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

		bgTime := stat.BeginStat()
		metric := exporter.NewTPCnt("HitCacheRead_ReplyToClient")
		if err = reply.WriteToConn(conn); err != nil {
			metric.SetWithLabels(err, map[string]string{exporter.Vol: req.CacheRequest.Volume, exporter.Client: conn.RemoteAddr().String()})
			bufRelease()
			log.LogErrorf("%s volume:[%s] %s", action, req.CacheRequest.Volume,
				reply.LogMessage(reply.GetOpMsg(), conn.RemoteAddr().String(), reply.StartT, err))
			return
		}
		stat.EndStat("HitCacheRead:ReplyToClient", err, bgTime, 1)
		metric.SetWithLabels(err, map[string]string{exporter.Vol: req.CacheRequest.Volume, exporter.Client: conn.RemoteAddr().String()})
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		bufRelease()
		if log.EnableInfo() {
			log.LogInfof("%s ReqID[%d] volume:[%s] reply[%s] block[%s]", action, p.ReqID, req.CacheRequest.Volume,
				reply.LogMessage(reply.GetOpMsg(), conn.RemoteAddr().String(), reply.StartT, err), block.String())
		}
	}
	p.PacketOkReply()
	return
}

func (f *FlashNode) opCachePrepare(conn net.Conn, p *proto.Packet) (err error) {
	action := "action[opCachePrepare]"
	var volume string
	defer func() {
		if err != nil {
			log.LogErrorf("%s volume:[%s] %s", action, volume,
				p.LogMessage(p.GetOpMsg(), conn.RemoteAddr().String(), p.StartT, err))
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
			if e := p.WriteToConn(conn); e != nil {
				log.LogErrorf("%s write to conn %v", action, e)
			}
		}
	}()

	req := new(proto.CachePrepareRequest)
	if err = p.UnmarshalDataPb(req); err != nil {
		return
	}
	if req.CacheRequest == nil {
		err = fmt.Errorf("no cache prepare request")
		return
	}
	volume = req.CacheRequest.Volume

	if err = f.cacheEngine.PrepareCache(p.ReqID, req.CacheRequest, conn.RemoteAddr().String()); err != nil {
		log.LogErrorf("%s prepare %v", action, err)
		return
	}

	p.PacketOkReply()
	if e := p.WriteToConn(conn); e != nil {
		log.LogErrorf("%s write to conn volume:%s %v", action, volume, e)
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
	followerPacket.Opcode = proto.OpFlashNodeCachePrepare
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

func (f *FlashNode) opSetReadIOLimits(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data
	req := &proto.FlashNodeSetIOLimitsRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	update := false
	decode := json.NewDecoder(bytes.NewBuffer(data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err == nil {
		log.LogDebugf("opSetReadIOLimits  req: %v", req)
		if req.Iocc != -1 {
			f.diskReadIocc = req.Iocc
			update = true
		}
		if req.Flow != -1 {
			f.diskReadFlow = req.Flow
			update = true
		}
		if req.Factor != -1 {
			f.diskReadIoFactorFlow = req.Factor
			update = true
		}
		if update {
			f.limitRead.ResetIOEx(f.diskReadIocc*len(f.disks), f.diskReadIoFactorFlow, f.handleReadTimeout)
			f.limitRead.ResetFlow(f.diskReadFlow)
		}
	} else {
		log.LogErrorf("decode FlashNodeSetIOLimitsRequest error: %s", err.Error())
	}
	p.PacketOkReply()
	return
}

func (f *FlashNode) opSetWriteIOLimits(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data
	req := &proto.FlashNodeSetIOLimitsRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	update := false
	decode := json.NewDecoder(bytes.NewBuffer(data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err == nil {
		log.LogDebugf("opSetReadIOLimits  req: %v", req)
		if req.Iocc != -1 {
			f.diskWriteIocc = req.Iocc
			update = true
		}
		if req.Flow != -1 {
			f.diskWriteFlow = req.Flow
			update = true
		}
		if req.Factor != -1 {
			f.diskWriteIoFactorFlow = req.Factor
			update = true
		}
		if update {
			f.limitWrite.ResetIOEx(f.diskWriteIocc*len(f.disks), f.diskWriteIoFactorFlow, f.handleReadTimeout)
			f.limitWrite.ResetFlow(f.diskWriteFlow)
		}
	} else {
		log.LogErrorf("decode opSetWriteIOLimits error: %s", err.Error())
	}
	p.PacketOkReply()
	return
}
