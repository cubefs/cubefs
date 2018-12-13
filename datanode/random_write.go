// Copyright 2018 The ChuBao Authors.
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

package datanode

import (
	_ "net/http/pprof"

	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
	"strings"
)

type RndWrtCmdItem struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

type rndWrtOpItem struct {
	extentId uint64
	offset   int64
	size     int64
	data     []byte
	crc      uint32
}

// Marshal random write value to binary data.
// Binary frame structure:
//  +------+----+------+------+------+------+------+
//  | Item | extentId | offset | size | crc | data |
//  +------+----+------+------+------+------+------+
//  | byte |     8    |    8   |  8   |  4  | size |
//  +------+----+------+------+------+------+------+
func rndWrtDataMarshal(extentId uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(8 + 8*2 + 4 + int(size))
	if err = binary.Write(buff, binary.BigEndian, extentId); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, offset); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, size); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, crc); err != nil {
		return
	}
	if _, err = buff.Write(data); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func rndWrtDataUnmarshal(raw []byte) (result *rndWrtOpItem, err error) {
	var opItem rndWrtOpItem

	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &opItem.extentId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.offset); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.crc); err != nil {
		return
	}
	opItem.data = make([]byte, opItem.size)
	if _, err = buff.Read(opItem.data); err != nil {
		return
	}

	result = &opItem
	return
}

func (rndWrtItem *RndWrtCmdItem) rndWrtCmdMarshalJson() (cmd []byte, err error) {
	return json.Marshal(rndWrtItem)
}

func (rndWrtItem *RndWrtCmdItem) rndWrtCmdUnmarshal(cmd []byte) (err error) {
	return json.Unmarshal(cmd, rndWrtItem)
}

// Submit the propose to raft.
func (dp *dataPartition) RandomWriteSubmit(pkg *Packet) (err error) {
	val, err := rndWrtDataMarshal(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data, pkg.CRC)
	if err != nil {
		return
	}
	resp, err := dp.Put(opRandomWrite, val)
	if err != nil {
		return
	}

	pkg.ResultCode = resp.(uint8)
	if pkg.ResultCode == proto.OpOk && !dp.hadRandomWrite {
		dp.hadRandomWrite = true
	}
	log.LogDebugf("[randomWrite] SubmitRaft: req_%v_%v_%v_%v_%v response = %v.",
		dp.partitionId, pkg.ExtentID, pkg.ExtentOffset, pkg.Size, pkg.CRC, pkg.GetResultMesg())
	return
}

func (dp *dataPartition) checkWriteErrs(errMsg string) (ignore bool) {
	// file has deleted when raft log apply
	if strings.Contains(errMsg, storage.ErrorHasDelete.Error()) {
		return true
	}
	return false
}

func (dp *dataPartition) addDiskErrs(err error, flag uint8) {
	if err == nil {
		return
	}

	d := dp.Disk()
	if d == nil {
		return
	}
	if !IsDiskErr(err.Error()) {
		return
	}
	if flag == WriteFlag {
		d.addWriteErr()
	}
}

func (dp *dataPartition) RandomPartitionReadCheck(request *Packet, connect net.Conn) (err error) {
	if !dp.config.RandomWrite || request.Opcode == proto.OpExtentRepairRead || !dp.hadRandomWrite {
		return
	}
	_, ok := dp.IsRaftLeader()
	if !ok {
		err = storage.ErrNotLeader
		log.LogErrorf("[readCheck] read ErrorNotLeader partition=%v", dp.partitionId)
		return
	}

	return
}

type ItemIterator struct {
	applyID uint64
}

func NewItemIterator(applyID uint64) *ItemIterator {
	si := new(ItemIterator)
	si.applyID = applyID
	return si
}

func (si *ItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

func (si *ItemIterator) Close() {
	return
}

func (si *ItemIterator) Next() (data []byte, err error) {
	appIdBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(appIdBuf, si.applyID)
	data = appIdBuf[:]
	return
}
