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

package data

import (
	"fmt"
	"hash/crc32"
	"net"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

const (
	StreamSendReadMaxRetry      = 3
	StreamSendOverWriteMaxRetry = 1
	StreamSendOverWriteTimeout  = 2 * time.Second

	StreamSendSleepInterval = 100 * time.Millisecond
	StreamSendTimeout       = 2 * time.Minute

	StreamReadConsistenceRetry   = 50
	StreamReadConsistenceTimeout = 1 * time.Minute

	IdleConnTimeoutData  = 30
	ConnectTimeoutDataMs = 500
	ReadTimeoutData      = 3
	WriteTimeoutData     = 3

	HostErrAccessTimeout = 300 // second

	StreamRetryTimeout = 10 * time.Minute
)

type GetReplyFunc func(conn *net.TCPConn) (err error, again bool)

// StreamConn defines the struct of the stream connection.
type StreamConn struct {
	dp       *DataPartition
	currAddr string
}

var (
	StreamConnPoolInitOnce sync.Once
	StreamConnPool         *connpool.ConnectPool
)

// NewStreamConn returns a new stream connection.
func NewStreamConn(dp *DataPartition, follower bool) *StreamConn {
	if !follower {
		return &StreamConn{
			dp:       dp,
			currAddr: dp.GetLeaderAddr(),
		}
	}

	if dp.ClientWrapper.CrossRegionHATypeQuorum() {
		return &StreamConn{
			dp:       dp,
			currAddr: dp.getNearestCrossRegionHost(),
		}
	}

	if dp.ClientWrapper.NearRead() {
		return &StreamConn{
			dp:       dp,
			currAddr: dp.getNearestHost(),
		}
	}

	return &StreamConn{
		dp:       dp,
		currAddr: dp.getFollowerReadHost(),
	}
}

func NewStreamConnWithAddr(dp *DataPartition, addr string) *StreamConn {
	return &StreamConn{
		dp:       dp,
		currAddr: addr,
	}
}

// String returns the string format of the stream connection.
func (sc *StreamConn) String() string {
	if sc == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Partition(%v) CurrentAddr(%v) Hosts(%v)", sc.dp.PartitionID, sc.currAddr, sc.dp.Hosts)
}

func (sc *StreamConn) sendToDataPartition(req *common.Packet) (conn *net.TCPConn, err error) {
	if log.IsDebugEnabled() {
		log.LogDebugf("sendToDataPartition: send to addr(%v), reqPacket(%v)", sc.currAddr, req)
	}
	if conn, err = StreamConnPool.GetConnect(sc.currAddr); err != nil {
		log.LogWarnf("sendToDataPartition: get connection to curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
		return
	}
	if req.Opcode == proto.OpStreamFollowerRead {
		req.SendT = time.Now().UnixNano()
	}
	if err = req.WriteToConnNs(conn, sc.dp.ClientWrapper.connConfig.WriteTimeoutNs); err != nil {
		log.LogWarnf("sendToDataPartition: failed to write to addr(%v) err(%v)", sc.currAddr, err)
		return
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("sendToDataPartition exit: send to addr(%v) reqPacket(%v) successfully", sc.currAddr, req)
	}
	return
}

func (sc *StreamConn) getReadReply(conn *net.TCPConn, reqPacket *common.Packet, req *ExtentRequest) (readBytes int, reply *common.Packet, tryOther bool, err error) {
	readBytes = 0
	for readBytes < int(reqPacket.Size) {
		replyPacket := common.NewReply(reqPacket.Ctx(), reqPacket.ReqID, reqPacket.PartitionID, reqPacket.ExtentID)
		bufSize := unit.Min(unit.ReadBlockSize, int(reqPacket.Size)-readBytes)
		replyPacket.Data = req.Data[readBytes : readBytes+bufSize]
		e := replyPacket.ReadFromConn(conn, sc.dp.ClientWrapper.connConfig.ReadTimeoutNs)
		if e != nil {
			log.LogWarnf("getReadReply: failed to read from connect, req(%v) readBytes(%v) err(%v)", reqPacket, readBytes, e)
			// Upon receiving TryOtherAddrError, other hosts will be retried.
			return readBytes, replyPacket, true, e
		}
		//log.LogDebugf("ExtentReader Read: ResultCode(%v) req(%v) reply(%v) readBytes(%v)", replyPacket.GetResultMsg(), reqPacket, replyPacket, readBytes)

		e = common.CheckReadReplyValid(reqPacket, replyPacket)
		if e != nil {
			// Dont change the error message, since the caller will
			// check if it is NotLeaderErr.
			return readBytes, replyPacket, false, e
		}

		readBytes += int(replyPacket.Size)
	}
	return readBytes, nil, false, nil
}

func checkReadReplyValid(request *common.Packet, reply *common.Packet) (err error) {
	if reply.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("checkReadReplyValid: ResultCode(%v) NOK", reply.GetResultMsg()))
		return
	}
	if !request.IsValidReadReply(reply) {
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
