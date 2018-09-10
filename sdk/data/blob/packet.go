// Copyright 2018 The Containerfs Authors.
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

package blob

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"

	"github.com/tiglabs/containerfs/proto"
)

const (
	NumKeySegments = 9
)

func NewWritePacket(dp *DataPartition, data []byte) *proto.Packet {
	pkt := proto.NewPacket()

	pkt.StoreMode = proto.TinyStoreMode
	pkt.Opcode = proto.OpWrite
	pkt.ReqID = proto.GetReqID()

	pkt.PartitionID = uint32(dp.PartitionID)

	pkt.Nodes = uint8(len(dp.Hosts) - 1)
	pkt.Arg = ([]byte)(dp.GetFollowAddrs())
	pkt.Arglen = uint32(len(pkt.Arg))

	pkt.Size = uint32(len(data))
	pkt.Data = data
	pkt.Crc = crc32.ChecksumIEEE(data)

	return pkt
}

func NewReadPacket(partitionID uint32, fileID uint64, objID int64, size uint32) *proto.Packet {
	pkt := proto.NewPacket()

	pkt.PartitionID = partitionID
	pkt.FileID = fileID
	pkt.Offset = objID
	pkt.Size = size

	pkt.StoreMode = proto.TinyStoreMode
	pkt.ReqID = proto.GetReqID()
	pkt.Opcode = proto.OpRead
	pkt.Nodes = 0

	return pkt
}

func NewDeletePacket(dp *DataPartition, fileID uint64, objID int64) *proto.Packet {
	pkt := proto.NewPacket()

	pkt.StoreMode = proto.TinyStoreMode
	pkt.ReqID = proto.GetReqID()
	pkt.Opcode = proto.OpMarkDelete

	pkt.PartitionID = uint32(dp.PartitionID)
	pkt.FileID = fileID
	pkt.Offset = objID

	pkt.Nodes = uint8(len(dp.Hosts) - 1)
	pkt.Arg = ([]byte)(dp.GetFollowAddrs())
	pkt.Arglen = uint32(len(pkt.Arg))

	return pkt
}

func ParsePacket(pkt *proto.Packet) (partitionID uint32, fileID uint64, objID int64, size uint32) {
	return pkt.PartitionID, pkt.FileID, pkt.Offset, pkt.Size
}

func GenKey(clusterName, volName string, partitionID uint32, fileID uint64, objID int64, size uint32) string {
	interKey := fmt.Sprintf("%v/%v/%v/%v/%v", partitionID, fileID, objID, size, time.Now().UnixNano())
	checkSum := crc32.ChecksumIEEE([]byte(interKey))
	key := fmt.Sprintf("%v/%v/%v/%v/%v", clusterName, volName, proto.TinyStoreMode, interKey, checkSum)
	return key
}

func ParseKey(key string) (clusterName, volName string, partitionID uint32, fileID uint64, objID int64, size uint32, err error) {
	segs := strings.Split(key, "/")
	if len(segs) != NumKeySegments {
		err = errors.New(fmt.Sprintf("ParseKey: num key(%v)", key))
		return
	}

	clusterName = segs[0]
	if strings.Compare(clusterName, "") != 0 {
		err = errors.New(fmt.Sprintf("ParseKey: cluster key(%v)", key))
		return
	}

	volName = segs[1]
	if strings.Compare(volName, "") != 0 {
		err = errors.New(fmt.Sprintf("ParseKey: volname key(%v)", key))
		return
	}

	val, err := strconv.ParseUint(segs[2], 10, 8)
	if int(val) != proto.TinyStoreMode {
		err = errors.New(fmt.Sprintf("ParseKey: store mode key(%v)", key))
		return
	}

	val, err = strconv.ParseUint(segs[3], 10, 32)
	if err != nil {
		err = errors.New(fmt.Sprintf("ParseKey: partition key(%v)", key))
		return
	}
	partitionID = uint32(val)

	val, err = strconv.ParseUint(segs[4], 10, 64)
	if err != nil {
		err = errors.New(fmt.Sprintf("ParseKey: file key(%v)", key))
		return
	}
	fileID = uint64(val)

	val, err = strconv.ParseUint(segs[5], 10, 64)
	if err != nil {
		err = errors.New(fmt.Sprintf("ParseKey: object key(%v)", key))
		return
	}
	objID = int64(val)

	val, err = strconv.ParseUint(segs[6], 10, 32)
	if err != nil {
		err = errors.New(fmt.Sprintf("ParseKey: size key(%v)", key))
		return
	}
	size = uint32(val)

	//TODO: checksum
	return
}
