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

package stream

import (
	"fmt"
	"hash/crc32"
	"net"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

// ExtentReader defines the struct of the extent reader.
type ExtentReader struct {
	inode        uint64
	key          *proto.ExtentKey
	dp           *wrapper.DataPartition
	followerRead bool
	retryRead    bool

	maxRetryTimeout time.Duration
}

// NewExtentReader returns a new extent reader.
func NewExtentReader(inode uint64, key *proto.ExtentKey, dp *wrapper.DataPartition, followerRead bool, retryRead bool) *ExtentReader {
	return &ExtentReader{
		inode:        inode,
		key:          key,
		dp:           dp,
		followerRead: followerRead,
		retryRead:    retryRead,
	}
}

// String returns the string format of the extent reader.
func (reader *ExtentReader) String() (m string) {
	return fmt.Sprintf("inode (%v) extentKey(%v)", reader.inode,
		reader.key.Marshal())
}

// Read reads the extent request.
func (reader *ExtentReader) Read(req *ExtentRequest) (readBytes int, err error) {
	offset := req.FileOffset - int(reader.key.FileOffset) + int(reader.key.ExtentOffset)
	size := req.Size

	reqPacket := NewReadPacket(reader.key, offset, size, reader.inode, req.FileOffset, reader.followerRead)
	sc := NewStreamConn(reader.dp, reader.followerRead, reader.maxRetryTimeout)

	log.LogDebugf("ExtentReader Read enter: size(%v) req(%v) reqPacket(%v)", size, req, reqPacket)

	// P4b: try RDMA per-chunk before falling back to the TCP streaming
	// path. RDMA only fires the leader (or the configured follower) for
	// this attempt; on any failure we fall through to the TCP path
	// below, which has its own host-iteration retry.
	if rdmaConnPool != nil && reader.dp != nil && len(reader.dp.Hosts) > 0 {
		rdmaAddr := reader.dp.Hosts[0]
		if n, rerr := reader.readViaRDMA(rdmaAddr, reqPacket, req, offset, size); rerr == nil {
			readBytes = n
			return
		} else {
			log.LogWarnf("ExtentReader Read: RDMA failed addr(%v) req(%v) err(%v), falling back to TCP",
				rdmaAddr, reqPacket, rerr)
		}
	}

	err = sc.Send(&reader.retryRead, reqPacket, func(conn *net.TCPConn) (error, bool) {
		bgTime := stat.BeginStat()
		defer func() {
			addr := conn.RemoteAddr().String()
			parts := strings.Split(addr, ":")
			if len(parts) > 0 {
				stat.EndStat(fmt.Sprintf("dataNode:%v", parts[0]), err, bgTime, 1)
			}
			stat.EndStat("dataNode", err, bgTime, 1)
		}()
		readBytes = 0
		for readBytes < size {
			replyPacket := NewReply(reqPacket.ReqID, reader.dp.PartitionID, reqPacket.ExtentID)
			bufSize := util.Min(util.ReadBlockSize, size-readBytes)
			replyPacket.Data = req.Data[readBytes : readBytes+bufSize]
			e := replyPacket.readFromConn(conn, proto.ReadDeadlineTime)

			if e != nil {
				if sc.dp.ClientWrapper.FollowerRead() && sc.dp.ClientWrapper.NearRead() && sc.dp.MediaType == proto.MediaType_HDD && strings.Contains(e.Error(), "timeout") {
					sc.dp.ClientWrapper.AddReadFailedHosts(sc.dp.PartitionID, conn.RemoteAddr().String())
				}
				log.LogWarnf("Extent Reader Read: failed to read from connect, ino(%v) req(%v) readBytes(%v) err(%v)", reader.inode, reqPacket, readBytes, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}

			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpLimitedIoErr {
				// NOTE: use special errors to retry
				return LimitedIoError, true
			}

			e = reader.checkStreamReply(reqPacket, replyPacket)
			if e != nil {
				log.LogWarnf("checkStreamReply failed:(%v) reply msg:(%v)", e, replyPacket.GetResultMsg())
				// Dont change the error message, since the caller will
				// check if it is NotLeaderErr.
				return e, false
			}

			readBytes += int(replyPacket.Size)
		}
		return nil, false
	})

	if err != nil {
		// if cold vol and cach is invaild
		if !reader.retryRead && (err == TryOtherAddrError || strings.Contains(err.Error(), "ExistErr")) {
			log.LogWarnf("Extent Reader Read: err(%v) req(%v) reqPacket(%v)", err, req, reqPacket)
		} else {
			log.LogErrorf("Extent Reader Read: err(%v) req(%v) reqPacket(%v)", err, req, reqPacket)
		}
	}

	log.LogDebugf("ExtentReader Read exit: req(%v) reqPacket(%v) readBytes(%v) err(%v)", req, reqPacket, readBytes, err)
	return
}

// readViaRDMA tries to satisfy req entirely over RDMA, chunked at
// util.ReadBlockSize. Returns the number of bytes filled into req.Data
// (== size on success) and an error if any chunk failed; the caller
// falls back to the TCP path, which restarts the read from the
// beginning. We do NOT support partial RDMA + TCP completion because
// the TCP path's getReply expects a single ReqID per StreamConn session
// and re-issuing only the failed tail under a new ReqID would race with
// the server's replication semantics.
func (reader *ExtentReader) readViaRDMA(addr string, reqPacket *Packet, req *ExtentRequest, offset, size int) (int, error) {
	readBytes := 0
	for readBytes < size {
		bufSize := util.Min(util.ReadBlockSize, size-readBytes)
		chunkReq := NewReadPacket(reader.key, offset+readBytes, bufSize,
			reader.inode, req.FileOffset+readBytes, reader.followerRead)
		// Inherit ReqID from the outer reqPacket so server-side audit
		// logs correlate; chunk index is implicit in offset.
		chunkReq.ReqID = reqPacket.ReqID

		resp, rerr := recvPacketViaRDMA(addr, chunkReq)
		if rerr != nil {
			return readBytes, rerr
		}
		if int(resp.Size) != bufSize {
			return readBytes, fmt.Errorf("rdma read: chunk size %d != requested %d", resp.Size, bufSize)
		}
		copy(req.Data[readBytes:readBytes+bufSize], resp.Data[:resp.Size])
		readBytes += bufSize
	}
	return readBytes, nil
}

func (reader *ExtentReader) checkStreamReply(request *Packet, reply *Packet) (err error) {
	if reply.ResultCode == proto.OpTryOtherAddr {
		return TryOtherAddrError
	}

	// if follower read is enabled, try other hosts when triggering OpNotExistErr
	// if reply.ResultCode == proto.OpNotExistErr {
	// 	return ExtentNotFoundError
	// }

	if reply.ResultCode != proto.OpOk {
		if request.Opcode == proto.OpStreamFollowerRead && reply.ResultCode != proto.OpForbidErr {
			log.LogWarnf("checkStreamReply: ResultCode(%v) NOK, OpStreamFollowerRead return TryOtherAddrError, "+
				"req(%v) reply(%v)", reply.GetResultMsg(), request, reply)
			return TryOtherAddrError
		}
		err = errors.New(fmt.Sprintf("checkStreamReply: ResultCode(%v) NOK", reply.GetResultMsg()))
		return
	}
	if !request.isValidReadReply(reply) {
		err = errors.New(fmt.Sprintf("checkStreamReply: inconsistent req and reply, req(%v) reply(%v)", request, reply))
		return
	}
	expectCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
	if reply.CRC != expectCrc {
		err = errors.New(fmt.Sprintf("checkStreamReply: inconsistent CRC, expectCRC(%v) replyCRC(%v), relpy(%v)", expectCrc, reply.CRC, reply.GetNoPrefixMsg()))
		return
	}
	return nil
}
