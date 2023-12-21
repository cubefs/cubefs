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
	"fmt"
	"hash/crc32"
	"net"
	"strings"
	"time"

	"github.com/cubefs/cubefs/flashnode/cachengine"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

var _ cachengine.ReadExtentData = ReadExtentData

var extentReaderConnPool *util.ConnectPool

func initExtentConnPool() {
	extentReaderConnPool = util.NewConnectPoolWithTimeoutAndCap(0, 10, _connPoolIdleTimeout, 1)
}

func ReadExtentData(source *proto.DataSource, afterReadFunc cachengine.ReadExtentAfter) (readBytes int, err error) {
	reqPacket := newReadPacket(&proto.ExtentKey{
		PartitionId: source.PartitionID,
		ExtentId:    source.ExtentID,
	}, int(source.ExtentOffset), int(source.Size_), source.FileOffset, true)

	readBytes, err = extentReadWithRetry(reqPacket, source.Hosts, afterReadFunc)
	if err != nil {
		log.LogErrorf("read extent err(%v)", err)
	}
	return
}

func extentReadWithRetry(reqPacket *proto.Packet, hosts []string, afterReadFunc cachengine.ReadExtentAfter) (readBytes int, err error) {
	errMap := make(map[string]error)
	startTime := time.Now()
	for try := range [_extentReadMaxRetry]struct{}{} {
		try++
		for _, addr := range hosts {
			if addr == "" {
				continue
			}
			log.LogDebugf("extentReadWithRetry: try(%d) addr(%s) reqPacket(%v)", try, addr, reqPacket)
			if readBytes, err = readFromDataPartition(addr, reqPacket, afterReadFunc); err == nil {
				return
			}
			errMap[addr] = err
			log.LogWarnf("extentReadWithRetry: try(%d) addr(%s) reqPacket(%v) err(%v)", try, addr, reqPacket, err)
			if strings.Contains(err.Error(), proto.ErrTmpfsNoSpace.Error()) {
				break
			}
		}
		if time.Since(startTime) > _extentReadTimeoutSec*time.Second {
			log.LogWarnf("extentReadWithRetry: retry timeout req(%v) time(%v)", reqPacket, time.Since(startTime))
			break
		}
		log.LogWarnf("extentReadWithRetry: errMap(%v) reqPacket(%v) try the next round", errMap, reqPacket)
		time.Sleep(_extentReadInterval)
	}
	err = errors.NewErrorf("FollowerRead: tried %d times reqPacket(%v) errMap(%v)", _extentReadMaxRetry, reqPacket, errMap)
	return
}

func readFromDataPartition(addr string, reqPacket *proto.Packet, afterReadFunc cachengine.ReadExtentAfter) (readBytes int, err error) {
	var conn *net.TCPConn
	var why string
	defer func() {
		extentReaderConnPool.PutConnect(conn, err != nil)
		if err != nil {
			log.LogWarnf("readFromDataPartition: %s addr(%s) reqPacket(%v) err(%v)", why, addr, reqPacket, err)
		}
	}()
	if conn, err = extentReaderConnPool.GetConnect(addr); err != nil {
		why = "get connection"
		return
	}

	log.LogDebugf("readFromDataPartition: addr(%s) reqPacket(%v)", addr, reqPacket)
	if reqPacket.Opcode == proto.OpStreamFollowerRead {
		reqPacket.StartT = time.Now().UnixNano()
	}
	if err = reqPacket.WriteToConn(conn); err != nil {
		why = "set to connection"
		return
	}
	if readBytes, err = getReadReply(conn, reqPacket, afterReadFunc); err != nil {
		why = "get reply"
		return
	}
	return
}

func getReadReply(conn *net.TCPConn, reqPacket *proto.Packet, afterReadFunc cachengine.ReadExtentAfter) (readBytes int, err error) {
	buf, bufErr := proto.Buffers.Get(util.ReadBlockSize)
	defer func() {
		if bufErr == nil {
			proto.Buffers.Put(buf[:util.ReadBlockSize])
		}
		if err != nil {
			log.LogWarnf("getReadReply: req(%v) readBytes(%v) err(%v)", reqPacket, readBytes, err)
		}
	}()
	if bufErr != nil {
		buf = make([]byte, reqPacket.Size)
	}

	for readBytes < int(reqPacket.Size) {
		reply := newReplyPacket(reqPacket.ReqID, reqPacket.PartitionID, reqPacket.ExtentID)
		bufSize := util.Min(util.ReadBlockSize, int(reqPacket.Size)-readBytes)
		reply.Data = buf[readBytes : readBytes+bufSize]
		if err = reply.ReadFromConn(conn, _extentReadTimeoutSec); err != nil {
			return
		}

		if err = checkReadReplyValid(reqPacket, reply); err != nil {
			return
		}
		if afterReadFunc != nil {
			if err = afterReadFunc(reply.Data[:bufSize], int64(bufSize)); err != nil {
				return
			}
		}
		if reply.Size != uint32(bufSize) {
			exporter.Warning(fmt.Sprintf("action[getReadReply] reply size not valid, "+
				"ReqID(%v) PartitionID(%v) Extent(%v) buffSize(%v) ReplySize(%v)",
				reqPacket.ReqID, reqPacket.PartitionID, reqPacket.ExtentID, bufSize, reply.Size))
		}
		readBytes += int(reply.Size)
	}
	return readBytes, nil
}

func checkReadReplyValid(request *proto.Packet, reply *proto.Packet) (err error) {
	if reply.ResultCode != proto.OpOk {
		return errors.NewErrorf("ResultCode(%v) NOTOK", reply.GetResultMsg())
	}
	if !isValidReadReply(request, reply) {
		return errors.NewErrorf("inconsistent req and reply, req(%v) reply(%v)", request, reply)
	}
	expectCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
	if reply.CRC != expectCrc {
		return errors.NewErrorf("inconsistent CRC, expectCRC(%v) replyCRC(%v)", expectCrc, reply.CRC)
	}
	return nil
}

func isValidReadReply(p *proto.Packet, q *proto.Packet) bool {
	return p.ReqID == q.ReqID && p.PartitionID == q.PartitionID && p.ExtentID == q.ExtentID
}

func newReadPacket(key *proto.ExtentKey, extentOffset, size int, fileOffset uint64, followerRead bool) *proto.Packet {
	p := new(proto.Packet)
	p.ExtentID = key.ExtentId
	p.PartitionID = key.PartitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentOffset = int64(extentOffset)
	p.Size = uint32(size)
	p.ExtentType = proto.NormalExtentType
	p.RemainingFollowers = 0
	p.KernelOffset = fileOffset
	p.Opcode = proto.OpStreamRead
	if followerRead {
		p.Opcode = proto.OpStreamFollowerRead
	}
	return p
}

func newReplyPacket(reqID int64, partitionID uint64, extentID uint64) *proto.Packet {
	p := new(proto.Packet)
	p.ReqID = reqID
	p.PartitionID = partitionID
	p.ExtentID = extentID
	p.Magic = proto.ProtoMagic
	p.ExtentType = proto.NormalExtentType
	return p
}
