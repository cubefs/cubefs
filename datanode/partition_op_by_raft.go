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

package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"

	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft"
	"strings"
	"sync/atomic"
)

type RaftCmdItem struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

type rndWrtOpItem struct {
	extentID uint64
	offset   int64
	size     int64
	data     []byte
	crc      uint32
}

// Marshal random write value to binary data.
// Binary frame structure:
//  +------+----+------+------+------+------+------+
//  | Item | extentID | offset | size | crc | data |
//  +------+----+------+------+------+------+------+
//  | byte |     8    |    8   |  8   |  4  | size |
//  +------+----+------+------+------+------+------+
func rndWrtDataMarshal(extentID uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(8 + 8*2 + 4 + int(size))
	if err = binary.Write(buff, binary.BigEndian, extentID); err != nil {
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
	if err = binary.Read(buff, binary.BigEndian, &opItem.extentID); err != nil {
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

func (raftOpItem *RaftCmdItem) raftCmdMarshalJSON() (cmd []byte, err error) {
	return json.Marshal(raftOpItem)
}

func (raftOpItem *RaftCmdItem) raftCmdUnmarshal(cmd []byte) (err error) {
	return json.Unmarshal(cmd, raftOpItem)
}

// RandomWriteSubmit submits the proposal to raft.
func (dp *DataPartition) RandomWriteSubmit(pkg *repl.Packet) (err error) {
	val, err := rndWrtDataMarshal(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data, pkg.CRC)
	if err != nil {
		return
	}
	var (
		resp interface{}
	)
	if pkg.Opcode == proto.OpRandomWrite {
		resp, err = dp.Put(opRandomWrite, val)
	} else {
		resp, err = dp.Put(opRandomSyncWrite, val)
	}
	if err != nil {
		return
	}

	pkg.ResultCode = resp.(uint8)

	log.LogDebugf("[RandomWrite] SubmitRaft: %v", pkg.GetUniqueLogId())
	return
}

func (dp *DataPartition) checkWriteErrs(errMsg string) (ignore bool) {
	// file has been deleted when applying the raft log
	if strings.Contains(errMsg, storage.ExtentHasBeenDeletedError.Error()) || strings.Contains(errMsg, storage.ExtentNotFoundError.Error()) {
		return true
	}
	return false
}

func (dp *DataPartition) addDiskErrs(err error, flag uint8) {
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
		d.incWriteErrCnt()
	}
}

// CheckLeader checks if itself is the leader during read
func (dp *DataPartition) CheckLeader(request *repl.Packet, connect net.Conn) (err error) {
	//  and use another getRaftLeaderAddr() to return the actual address
	_, ok := dp.IsRaftLeader()
	if !ok {
		err = raft.ErrNotLeader
		logContent := fmt.Sprintf("action[ReadCheck] %v.", request.LogMessage(request.GetOpMsg(), connect.RemoteAddr().String(), request.StartT, err))
		log.LogWarnf(logContent)
		return
	}

	return
}

type ItemIterator struct {
	applyID uint64
}

// NewItemIterator creates a new item iterator.
func NewItemIterator(applyID uint64) *ItemIterator {

	si := new(ItemIterator)
	si.applyID = applyID
	return si
}

// ApplyIndex returns the appliedID
func (si *ItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

// Close Closes the iterator.
func (si *ItemIterator) Close() {
	return
}

// Next returns the next item in the iterator.
func (si *ItemIterator) Next() (data []byte, err error) {
	appIDBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(appIDBuf, si.applyID)
	data = appIDBuf[:]
	return
}

// ApplyRandomWrite random write apply
func (dp *DataPartition) ApplyRandomWrite(msg *RaftCmdItem, raftApplyID uint64) (extentID uint64, err error) {
	opItem := &rndWrtOpItem{}
	defer func() {
		if err != nil {
			atomic.StoreUint64(&dp.disk.WriteErrCnt, uint64(dp.disk.MaxErrCnt))
			err = storage.BrokenExtentError
		}
	}()
	if opItem, err = rndWrtDataUnmarshal(msg.V); err != nil {
		log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v) unmarshal failed", raftApplyID, dp.partitionID, err)
		return
	}
	extentID = opItem.extentID
	log.LogDebugf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v)",
		raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size)
	for i := 0; i < maxRetryCounts; i++ {
		err = dp.ExtentStore().Write(opItem.extentID, opItem.offset, opItem.size, opItem.data, opItem.crc, NotUpdateSize, msg.Op == opRandomSyncWrite)
		dp.checkIsDiskError(err)
		if err != nil {
			if dp.checkWriteErrs(err.Error()) {
				log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) ignore error[%v]",
					raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err)
				err = nil
			}
		}
		dp.addDiskErrs(err, WriteFlag)
		if err == nil {
			break
		}
		log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) apply err[%v] retry[%v]",
			raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err, i)
	}

	return
}
