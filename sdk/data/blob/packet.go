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
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
)

const (
	NumKeySegments = 10
)

func NewBlobWritePacket(dp *wrapper.DataPartition, data []byte) *proto.Packet {
	p := proto.NewPacket()

	p.StoreMode = proto.BlobStoreMode
	p.Opcode = proto.OpWrite
	p.ReqID = proto.GetReqID()
	p.PartitionID = uint32(dp.PartitionID)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.Nodes = uint8(len(dp.Hosts) - 1)
	p.Size = uint32(len(data))
	p.Data = data
	p.Crc = crc32.ChecksumIEEE(data)

	return p
}

func NewBlobReadPacket(partitionID uint32, fileID uint64, objID int64, size uint32) *proto.Packet {
	p := proto.NewPacket()
	p.PartitionID = partitionID
	p.FileID = fileID
	p.Offset = objID
	p.Size = size
	p.StoreMode = proto.BlobStoreMode
	p.ReqID = proto.GetReqID()
	p.Opcode = proto.OpRead
	p.Nodes = 0

	return p
}

func NewBlobDeletePacket(dp *wrapper.DataPartition, fileID uint64, objID int64) *proto.Packet {
	p := proto.NewPacket()
	p.StoreMode = proto.BlobStoreMode
	p.ReqID = proto.GetReqID()
	p.Opcode = proto.OpMarkDelete
	p.PartitionID = uint32(dp.PartitionID)
	p.FileID = fileID
	p.Offset = objID
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.Nodes = uint8(len(dp.Hosts) - 1)

	return p
}

func ParsePacket(p *proto.Packet) (partitionID uint32, fileID uint64, objID int64, crc uint32) {
	return p.PartitionID, p.FileID, p.Offset, p.Crc
}

func GenKey(clusterName, volName string, partitionID uint32, fileID uint64, objID int64, size, crc uint32) string {
	interKey := fmt.Sprintf("%v/%v/%v/%v/%v/%v", partitionID, fileID, objID, size, time.Now().UnixNano(), crc)
	checkSum := crc32.ChecksumIEEE([]byte(interKey))
	key := fmt.Sprintf("%v/%v/%v/%v/%v", clusterName, volName, proto.BlobStoreMode, interKey, checkSum)
	return key
}

func ParseKey(key string) (clusterName, volName string, partitionID uint32, fileID uint64, objID int64, size, crc uint32, err error) {
	segs := strings.Split(key, "/")
	if len(segs) != NumKeySegments {
		err = errors.New(fmt.Sprintf("ParseKey: num key(%v)", key))
		return
	}

	clusterName = segs[0]
	if strings.Compare(clusterName, "") == 0 {
		err = errors.New(fmt.Sprintf("ParseKey: cluster key(%v)", key))
		return
	}

	volName = segs[1]
	if strings.Compare(volName, "") == 0 {
		err = errors.New(fmt.Sprintf("ParseKey: volname key(%v)", key))
		return
	}

	val, err := strconv.ParseUint(segs[2], 10, 8)
	if int(val) != proto.BlobStoreMode {
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

	val, err = strconv.ParseUint(segs[8], 10, 32)
	if err != nil {
		err = errors.New(fmt.Sprintf("ParseKey: crc key(%v)", key))
		return
	}
	crc = uint32(val)

	//TODO: checksum
	return
}
