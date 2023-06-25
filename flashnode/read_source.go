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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"hash/crc32"
	"net"
	"strings"
	"time"
)

const (
	ExtentReadMaxRetry      = 3
	ExtentReadTimeoutSec    = 3
	ExtentReadSleepInterval = 1
	IdleConnTimeoutData     = 30
	ConnectTimeoutDataMs    = 500
)

var extentReaderConnPool *connpool.ConnectPool

type ReadSource struct {
}

func NewReadSource() *ReadSource {
	extentReaderConnPool = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, IdleConnTimeoutData, ConnectTimeoutDataMs*int64(time.Millisecond))
	return &ReadSource{}
}

func (reader *ReadSource) ReadExtentData(source *proto.DataSource, afterReadFunc func([]byte, int64) error) (readBytes int, err error) {
	var reqPacket *proto.Packet
	reqPacket = newReadPacket(context.Background(), &proto.ExtentKey{
		PartitionId: source.PartitionID,
		ExtentId:    source.ExtentID,
	}, int(source.ExtentOffset), int(source.Size_), source.FileOffset, true)

	readBytes, err = extentReadWithRetry(reqPacket, source.Hosts, afterReadFunc)
	if err != nil {
		log.LogErrorf("read error: err(%v)", err)
		return readBytes, err
	}
	return
}

func extentReadWithRetry(reqPacket *proto.Packet, hosts []string, afterReadFunc func([]byte, int64) error) (readBytes int, err error) {
	errMap := make(map[string]error)
	startTime := time.Now()
	retryCount := 0
	for i := 0; i < ExtentReadMaxRetry; i++ {
		retryCount++
		for _, addr := range hosts {
			log.LogWarnf("extentReadWithRetry: try addr(%v) reqPacket(%v)", addr, reqPacket)
			readBytes, err = sendReadCmdToDataPartition(addr, reqPacket, afterReadFunc)
			if err == nil {
				return
			}
			errMap[addr] = err
			log.LogWarnf("extentReadWithRetry: try addr(%v) failed! reqPacket(%v) err(%v)", addr, reqPacket, err)
			if strings.Contains(err.Error(), proto.ErrTmpfsNoSpace.Error()) {
				break
			}
		}
		if time.Since(startTime) > time.Duration(ExtentReadTimeoutSec*int64(time.Second)) {
			log.LogWarnf("extentReadWithRetry: retry timeout req(%v) time(%v)", reqPacket, time.Since(startTime))
			break
		}
		log.LogWarnf("extentReadWithRetry: errMap(%v), reqPacket(%v), try the next round", errMap, reqPacket)
		time.Sleep(ExtentReadSleepInterval)
	}
	err = errors.New(fmt.Sprintf("FollowerRead: failed %v times, reqPacket(%v) errMap(%v)", ExtentReadMaxRetry, reqPacket, errMap))
	return
}

func sendReadCmdToDataPartition(addr string, reqPacket *proto.Packet, afterReadFunc func([]byte, int64) error) (readBytes int, err error) {
	if addr == "" {
		err = errors.New(fmt.Sprintf("sendReadCmdToDataPartition: failed, current address is null, reqPacket(%v)", reqPacket))
		return
	}
	var conn *net.TCPConn
	defer func() {
		extentReaderConnPool.PutConnectWithErr(conn, err)
	}()
	if conn, err = extentReaderConnPool.GetConnect(addr); err != nil {
		log.LogWarnf("sendToDataPartition: get connection to curr addr failed, addr(%v) reqPacket(%v) err(%v)", addr, reqPacket, err)
		return
	}
	if err = sendToDataPartition(conn, reqPacket, addr); err != nil {
		log.LogWarnf("sendReadCmdToDataPartition: send to curr addr failed, addr(%v) reqPacket(%v) err(%v)", addr, reqPacket, err)
		return
	}
	if readBytes, err = getReadReply(conn, reqPacket, afterReadFunc); err != nil {
		log.LogWarnf("sendReadCmdToDataPartition: getReply error and RETURN, addr(%v) reqPacket(%v) err(%v)", addr, reqPacket, err)
		return
	}
	return
}

func sendToDataPartition(conn *net.TCPConn, req *proto.Packet, addr string) (err error) {
	if log.IsDebugEnabled() {
		log.LogDebugf("sendToDataPartition: send to addr(%v)", addr)
	}
	if req.Opcode == proto.OpStreamFollowerRead {
		req.SendT = time.Now().UnixNano()
	}
	if err = req.WriteToConnNs(conn, ExtentReadTimeoutSec*int64(time.Second)); err != nil {
		log.LogWarnf("sendToDataPartition: failed to write to addr(%v) err(%v)", addr, err)
		return
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("sendToDataPartition exit: send to addr(%v) successfully", addr)
	}
	return
}

func getReadReply(conn *net.TCPConn, reqPacket *proto.Packet, afterReadFunc func([]byte, int64) error) (readBytes int, err error) {
	readBytes = 0
	tmpData := make([]byte, reqPacket.Size)
	for readBytes < int(reqPacket.Size) {
		replyPacket := newReplyPacket(reqPacket.Ctx(), reqPacket.ReqID, reqPacket.PartitionID, reqPacket.ExtentID)
		bufSize := unit.Min(unit.ReadBlockSize, int(reqPacket.Size)-readBytes)
		replyPacket.Data = tmpData[readBytes : readBytes+bufSize]
		e := replyPacket.ReadFromConn(conn, ExtentReadTimeoutSec)
		if e != nil {
			log.LogWarnf("getReadReply: failed to read from connect, req(%v) readBytes(%v) err(%v)", reqPacket, readBytes, e)
			// Upon receiving TryOtherAddrError, other hosts will be retried.
			return readBytes, e
		}

		e = checkReadReplyValid(reqPacket, replyPacket)
		if e != nil {
			// Don't change the error message, since the caller will
			// check if it is NotLeaderErr.
			return readBytes, e
		}
		if afterReadFunc != nil {
			e = afterReadFunc(replyPacket.Data[:bufSize], int64(bufSize))
			if e != nil {
				return readBytes, e
			}
		}
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

// newReadPacket returns a new read packet.
func newReadPacket(ctx context.Context, key *proto.ExtentKey, extentOffset, size int, fileOffset uint64, followerRead bool) *proto.Packet {
	p := new(proto.Packet)
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
