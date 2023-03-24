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
	"context"
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/log"
)

// ExtentReader defines the struct of the extent reader.
type ExtentReader struct {
	inode        uint64
	key          *proto.ExtentKey
	dp           *DataPartition
	followerRead bool
}

// NewExtentReader returns a new extent reader.
func NewExtentReader(inode uint64, key *proto.ExtentKey, dp *DataPartition, followerRead bool) *ExtentReader {
	return &ExtentReader{
		inode:        inode,
		key:          key,
		dp:           dp,
		followerRead: followerRead,
	}
}

// String returns the string format of the extent reader.
func (er *ExtentReader) String() (m string) {
	if er == nil {
		return ""
	}
	return fmt.Sprintf("inode (%v) extentKey(%v)", er.inode,
		er.key.Marshal())
}

func (er *ExtentReader) EcTinyExtentRead(ctx context.Context, req *ExtentRequest) (readBytes int, err error) {
	offset := int(req.FileOffset) - int(er.key.FileOffset) + int(er.key.ExtentOffset)
	size := req.Size

	reqPacket := common.NewTinyExtentReadPacket(ctx, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, offset, req.Size)

	log.LogDebugf("grep : size(%v) req(%v) reqPacket(%v)", size, req, reqPacket)

	readBytes, err = er.read(er.dp, reqPacket, req, er.followerRead)

	if err != nil {
		log.LogWarnf("ExtentReader EcTinyExtentRead: err(%v) req(%v) reqPacket(%v)", err, req, reqPacket)
	}

	log.LogDebugf("ExtentReader EcTinyExtentRead exit: req(%v) reqPacket(%v) readBytes(%v) err(%v)", req, reqPacket, readBytes, err)
	return
}

// Read reads the extent request.
func (er *ExtentReader) Read(ctx context.Context, req *ExtentRequest) (readBytes int, err error) {
	offset := int(req.FileOffset - er.key.FileOffset + er.key.ExtentOffset)
	size := req.Size

	reqPacket := common.NewReadPacket(ctx, er.key, offset, size, er.inode, req.FileOffset, er.followerRead)

	log.LogDebugf("ExtentReader Read enter: size(%v) req(%v) reqPacket(%v)", size, req, reqPacket)

	readBytes, err = er.read(er.dp, reqPacket, req, er.followerRead)

	if err != nil {
		log.LogWarnf("Extent Reader Read: err(%v) req(%v) reqPacket(%v)", err, req, reqPacket)
	}

	log.LogDebugf("ExtentReader Read exit: req(%v) reqPacket(%v) readBytes(%v) err(%v)", req, reqPacket, readBytes, err)
	return
}

func (er *ExtentReader) read(dp *DataPartition, reqPacket *common.Packet, req *ExtentRequest, followerRead bool) (readBytes int, err error) {
	var sc *StreamConn
	if dp.canEcRead() {
		reqPacket.Opcode = proto.OpStreamEcRead
		if sc, readBytes, err = dp.EcRead(reqPacket, req); err == nil {
			return
		}
		log.LogWarnf("read error: read EC failed, read data from replicate, err(%v)", err)
		errMsg := fmt.Sprintf("read EC failed inode(%v) req(%v)", er.inode, req)
		common.HandleUmpAlarm(dp.ClientWrapper.clusterName, dp.ClientWrapper.volName, "ecRead", errMsg)
	}
	if !followerRead {
		sc, readBytes, err = dp.LeaderRead(reqPacket, req)
		if err != nil {
			log.LogWarnf("read error: read leader failed, err(%v)", err)
			readBytes, err = dp.ReadConsistentFromHosts(sc, reqPacket, req)
		}
	} else {
		sc, readBytes, err = dp.FollowerRead(reqPacket, req)
	}
	if err != nil {
		log.LogWarnf("read error: err(%v), followerRead(%v)", err, followerRead)
		return readBytes, err
	}
	return
}
