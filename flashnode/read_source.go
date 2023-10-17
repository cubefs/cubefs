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
	"github.com/cubefs/cubefs/util/exporter"
	"hash/crc32"
	"net"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

const (
	ExtentReadMaxRetry      = 3
	ExtentReadTimeoutSec    = 3
	ExtentReadSleepInterval = 100 * time.Millisecond
	IdleConnTimeoutData     = 30 * time.Second
	ConnectTimeoutData      = 500 * time.Millisecond
)

type blockWriteHandle struct {
	writer func(data []byte, off, size int64) error
	offset int64
}

var extentReaderConnPool *connpool.ConnectPool

type ReadSource struct{}

func NewReadSource() *ReadSource {
	extentReaderConnPool = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, IdleConnTimeoutData, ConnectTimeoutData)
	return &ReadSource{}
}

func (reader *ReadSource) ReadExtentData(source *proto.DataSource, writer func(data []byte, off, size int64) error) (readBytes int, err error) {
	wh := &blockWriteHandle{
		writer: writer,
		offset: int64(source.CacheBlockOffset()),
	}
	ek := &proto.ExtentKey{PartitionId: source.PartitionID, ExtentId: source.ExtentID}
	req := newStreamFollowerReadPacket(context.Background(), ek, int(source.ExtentOffset), int(source.Size_), source.FileOffset)
	return reader.extentReadWithRetry(req, source.Hosts, wh)
}

func (reader *ReadSource) extentReadWithRetry(req *proto.Packet, hosts []string, w *blockWriteHandle) (readBytes int, err error) {
	errMap := make(map[string]error)
	startTime := time.Now()
	retryCount := 0
	for i := 0; i < ExtentReadMaxRetry; i++ {
		retryCount++
		for _, addr := range hosts {
			if log.IsDebugEnabled() {
				log.LogDebugf("extentReadWithRetry: try addr(%v) req(%v)", addr, req)
			}
			if addr == "" {
				err = errors.New(fmt.Sprintf("extentReadWithRetry: failed, current address is null, req(%v)", req))
				return
			}
			readBytes, err = reader.sendReadCmdToDataPartition(addr, req, w)
			if err == nil {
				return
			}
			errMap[addr] = err
			if log.IsWarnEnabled() {
				log.LogWarnf("extentReadWithRetry: try addr(%v) failed! req(%v) err(%v)", addr, req, err)
			}
			if strings.Contains(err.Error(), proto.ErrTmpfsNoSpace.Error()) {
				break
			}
		}
		if time.Since(startTime) > time.Duration(ExtentReadTimeoutSec*int64(time.Second)) {
			log.LogWarnf("extentReadWithRetry: retry timeout req(%v) time(%v)", req, time.Since(startTime))
			break
		}
		log.LogWarnf("extentReadWithRetry: errMap(%v), req(%v), try the next round", errMap, req)
		time.Sleep(ExtentReadSleepInterval)
	}
	err = errors.New(fmt.Sprintf("FollowerRead: failed %v times, req(%v) errMap(%v)", ExtentReadMaxRetry, req, errMap))
	return
}

func (reader *ReadSource) sendReadCmdToDataPartition(addr string, req *proto.Packet, w *blockWriteHandle) (readBytes int, err error) {
	var conn *net.TCPConn
	defer func() {
		extentReaderConnPool.PutConnectWithErr(conn, err)
	}()
	if conn, err = extentReaderConnPool.GetConnect(addr); err != nil {
		log.LogWarnf("sendToDataPartition: get connection to curr addr failed, addr(%v) req(%v) err(%v)", addr, req, err)
		return
	}
	req.SendT = time.Now().UnixNano()
	if err = req.WriteToConnNs(conn, ExtentReadTimeoutSec*int64(time.Second)); err != nil {
		log.LogWarnf("sendReadCmdToDataPartition: failed to write to addr(%v) err(%v)", addr, err)
		return
	}
	if readBytes, err = reader.getReadReply(conn, req, w); err != nil {
		log.LogWarnf("sendReadCmdToDataPartition: getReply error and RETURN, addr(%v) req(%v) err(%v)", addr, req, err)
		return
	}
	return
}

func (reader *ReadSource) getReadReply(conn *net.TCPConn, req *proto.Packet, w *blockWriteHandle) (readBytes int, err error) {
	readBytes = 0
	tmpData := make([]byte, req.Size)
	offset := w.offset
	for readBytes < int(req.Size) {
		replyPacket := newReplyPacket(req.Ctx(), req.ReqID, req.PartitionID, req.ExtentID)
		bufSize := unit.Min(unit.ReadBlockSize, int(req.Size)-readBytes)
		replyPacket.Data = tmpData[readBytes : readBytes+bufSize]
		e := replyPacket.ReadFromConn(conn, ExtentReadTimeoutSec)
		if e != nil {
			log.LogWarnf("getReadReply: failed to read from connect, req(%v) readBytes(%v) err(%v)", req, readBytes, e)
			return readBytes, e
		}

		e = checkReadReplyValid(req, replyPacket)
		if e != nil {
			return readBytes, e
		}
		if w.writer == nil {
			return readBytes, fmt.Errorf("write handle is nil")
		}
		e = w.writer(replyPacket.Data[:bufSize], offset, int64(bufSize))
		if e != nil {
			return readBytes, e
		}
		if replyPacket.Size != uint32(bufSize) {
			exporter.WarningCritical(fmt.Sprintf("action[getReadReply] reply size not valid, ReqID(%v) PartitionID(%v) Extent(%v) buffSize(%v) ReplySize(%v)", req.ReqID, req.PartitionID, req.ExtentID, bufSize, replyPacket.Size))
		}
		offset += int64(replyPacket.Size)
		readBytes += int(replyPacket.Size)
	}
	return readBytes, nil
}

func checkReadReplyValid(request *proto.Packet, reply *proto.Packet) (err error) {
	if reply.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("checkReadReplyValid: ResultCode(%v) NOK", reply.GetResultMsg()))
		return
	}
	if !isValidReadReply(request, reply) {
		err = errors.New(fmt.Sprintf("checkReadReplyValid: inconsistent req and reply, req(%v) reply(%v)", request, reply))
		return
	}
	expectCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
	if reply.CRC != expectCrc {
		err = errors.New(fmt.Sprintf("checkReadReplyValid: inconsistent CRC, expectCRC(%v) replyCRC(%v)", expectCrc, reply.CRC))
		return
	}
	return nil
}

func isValidReadReply(p *proto.Packet, q *proto.Packet) bool {
	if p.ReqID == q.ReqID && p.PartitionID == q.PartitionID && p.ExtentID == q.ExtentID {
		return true
	}
	return false
}

// newStreamFollowerReadPacket returns a new read packet.
func newStreamFollowerReadPacket(ctx context.Context, key *proto.ExtentKey, extentOffset, size int, fileOffset uint64) *proto.Packet {
	p := new(proto.Packet)
	p.ExtentID = key.ExtentId
	p.PartitionID = key.PartitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(extentOffset)
	p.Size = uint32(size)
	p.Opcode = proto.OpStreamFollowerRead
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = 0
	p.KernelOffset = fileOffset
	p.SetCtx(ctx)
	return p
}

// newReplyPacket returns a new reply packet.
func newReplyPacket(ctx context.Context, reqID int64, partitionID uint64, extentID uint64) *proto.Packet {
	p := new(proto.Packet)
	p.ReqID = reqID
	p.PartitionID = partitionID
	p.ExtentID = extentID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	p.SetCtx(ctx)
	return p
}
