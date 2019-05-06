// Copyright 2018 The Chubao Authors.
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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/wrapper"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"hash/crc32"
	"net"
)

// ExtentReader defines the struct of the extent reader.
type ExtentReader struct {
	inode uint64
	key   *proto.ExtentKey
	dp    *wrapper.DataPartition
}

// NewExtentReader returns a new extent reader.
func NewExtentReader(inode uint64, key *proto.ExtentKey, dp *wrapper.DataPartition) *ExtentReader {
	return &ExtentReader{
		inode: inode,
		key:   key,
		dp:    dp,
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

	reqPacket := NewReadPacket(reader.key, offset, size, reader.inode, req.FileOffset)
	sc := NewStreamConn(reader.dp)

	log.LogDebugf("ExtentReader Read enter: size(%v) req(%v) reqPacket(%v)", size, req, reqPacket)

	err = sc.Send(reqPacket, func(conn *net.TCPConn) (error, bool) {
		readBytes = 0
		for readBytes < size {
			replyPacket := NewReply(reqPacket.ReqID, reader.dp.PartitionID, reqPacket.ExtentID)
			bufSize := util.Min(util.ReadBlockSize, size-readBytes)
			replyPacket.Data = req.Data[readBytes : readBytes+bufSize]
			e := replyPacket.readFromConn(conn, proto.ReadDeadlineTime)
			if e != nil {
				log.LogWarnf("Extent Reader Read: failed to read from connect, ino(%v) req(%v) readBytes(%v) err(%v)", reader.inode, reqPacket, readBytes, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}

			//log.LogDebugf("ExtentReader Read: ResultCode(%v) req(%v) reply(%v) readBytes(%v)", replyPacket.GetResultMsg(), reqPacket, replyPacket, readBytes)

			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			e = reader.checkStreamReply(reqPacket, replyPacket)
			if e != nil {
				// Dont change the error message, since the caller will
				// check if it is NotLeaderErr.
				return e, false
			}

			readBytes += int(replyPacket.Size)
		}
		return nil, false
	})

	if err != nil {
		log.LogErrorf("Extent Reader Read: err(%v) req(%v) reqPacket(%v)", err, req, reqPacket)
	}

	log.LogDebugf("ExtentReader Read exit: req(%v) reqPacket(%v) readBytes(%v) err(%v)", req, reqPacket, readBytes, err)
	return
}

func (reader *ExtentReader) checkStreamReply(request *Packet, reply *Packet) (err error) {
	if reply.ResultCode == proto.OpTryOtherAddr {
		return TryOtherAddrError
	}

	if reply.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("checkStreamReply: ResultCode(%v) NOK", reply.GetResultMsg()))
		return
	}
	if !request.isValidReadReply(reply) {
		err = errors.New(fmt.Sprintf("checkStreamReply: inconsistent req and reply, req(%v) reply(%v)", request, reply))
		return
	}
	expectCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
	if reply.CRC != expectCrc {
		err = errors.New(fmt.Sprintf("checkStreamReply: inconsistent CRC, expectCRC(%v) replyCRC(%v)", expectCrc, reply.CRC))
		return
	}
	return nil
}
