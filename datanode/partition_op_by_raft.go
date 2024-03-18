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

package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

type RaftCmdItem struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

type rndWrtOpItem struct {
	opcode   uint8
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

const (
	BinaryMarshalMagicVersion = 0xFF
)

func MarshalRandWriteRaftLog(opcode uint8, extentID uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(8 + 8*2 + 4 + int(size) + 4 + 4)
	if err = binary.Write(buff, binary.BigEndian, uint32(BinaryMarshalMagicVersion)); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, opcode); err != nil {
		return
	}
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

// RandomWriteSubmit submits the proposal to raft.
func UnmarshalRandWriteRaftLog(raw []byte) (opItem *rndWrtOpItem, err error) {
	opItem = new(rndWrtOpItem)
	buff := bytes.NewBuffer(raw)
	var version uint32
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &opItem.opcode); err != nil {
		return
	}
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

	return
}

func MarshalRaftCmd(raftOpItem *RaftCmdItem) (raw []byte, err error) {
	if raw, err = json.Marshal(raftOpItem); err != nil {
		return
	}
	return
}

func UnmarshalRaftCmd(raw []byte) (raftOpItem *RaftCmdItem, err error) {
	raftOpItem = new(RaftCmdItem)
	defer func() {
		log.LogDebugf("Unmarsh use oldVersion,result %v", err)
	}()
	if err = json.Unmarshal(raw, raftOpItem); err != nil {
		return
	}
	return
}

func UnmarshalOldVersionRaftLog(raw []byte) (opItem *rndWrtOpItem, err error) {
	raftOpItem := new(RaftCmdItem)
	defer func() {
		log.LogDebugf("Unmarsh use oldVersion,result %v", err)
	}()
	if err = json.Unmarshal(raw, raftOpItem); err != nil {
		return
	}
	opItem, err = UnmarshalOldVersionRandWriteOpItem(raftOpItem.V)
	if err != nil {
		return
	}
	opItem.opcode = uint8(raftOpItem.Op)
	return
}

func UnmarshalOldVersionRandWriteOpItem(raw []byte) (result *rndWrtOpItem, err error) {
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
	// do nothing
}

// Next returns the next item in the iterator.
func (si *ItemIterator) Next() (data []byte, err error) {
	// appIDBuf := make([]byte, 8)
	// binary.BigEndian.PutUint64(appIDBuf, si.applyID)
	// data = appIDBuf[:]
	err = io.EOF
	return
}

// ApplyRandomWrite random write apply
func (dp *DataPartition) ApplyRandomWrite(command []byte, raftApplyID uint64) (respStatus interface{}, err error) {
	opItem := &rndWrtOpItem{}
	respStatus = proto.OpOk
	defer func() {
		if err == nil {
			dp.uploadApplyID(raftApplyID)
			log.LogDebugf("action[ApplyRandomWrite] dp(%v) raftApplyID(%v) success!", dp.partitionID, raftApplyID)
		} else {
			if respStatus == proto.OpExistErr { // for tryAppendWrite
				err = nil
				log.LogDebugf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) apply err(%v) retry[20]",
					raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err)
				return
			}
			err = fmt.Errorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) apply err(%v) retry[20]",
				raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err)
			log.LogErrorf("action[ApplyRandomWrite] Partition(%v) failed err %v", dp.partitionID, err)
			exporter.Warning(err.Error())
			if respStatus == proto.OpOk {
				respStatus = proto.OpDiskErr
			}
			panic(newRaftApplyError(err))
		}
	}()

	if opItem, err = UnmarshalRandWriteRaftLog(command); err != nil {
		log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v) unmarshal failed(%v)", raftApplyID, dp.partitionID, err)
		return
	}
	log.LogDebugf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v)",
		raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size)

	for i := 0; i < 20; i++ {
		dp.disk.allocCheckLimit(proto.FlowWriteType, uint32(opItem.size))
		dp.disk.allocCheckLimit(proto.IopsWriteType, 1)

		var syncWrite bool
		writeType := storage.RandomWriteType
		if opItem.opcode == proto.OpRandomWrite || opItem.opcode == proto.OpSyncRandomWrite {
			if dp.verSeq > 0 {
				err = storage.VerNotConsistentError
				log.LogErrorf("action[ApplyRandomWrite] volume [%v] dp [%v] %v,client need update to newest version!", dp.volumeID, dp.partitionID, err)
				return
			}
		} else if opItem.opcode == proto.OpRandomWriteAppend || opItem.opcode == proto.OpSyncRandomWriteAppend {
			writeType = storage.AppendRandomWriteType
		} else if opItem.opcode == proto.OpTryWriteAppend || opItem.opcode == proto.OpSyncTryWriteAppend {
			writeType = storage.AppendWriteType
		}

		if opItem.opcode == proto.OpSyncRandomWriteAppend || opItem.opcode == proto.OpSyncRandomWrite || opItem.opcode == proto.OpSyncRandomWriteVer {
			syncWrite = true
		}

		dp.disk.limitWrite.Run(int(opItem.size), func() {
			respStatus, err = dp.ExtentStore().Write(opItem.extentID, opItem.offset, opItem.size, opItem.data, opItem.crc, writeType, syncWrite, false)
		})
		if err == nil {
			break
		}
		if IsDiskErr(err.Error()) {
			panic(newRaftApplyError(err))
		}
		if strings.Contains(err.Error(), storage.ExtentNotFoundError.Error()) {
			err = nil
			return
		}
		if (opItem.opcode == proto.OpTryWriteAppend || opItem.opcode == proto.OpSyncTryWriteAppend) && respStatus == proto.OpTryOtherExtent {
			err = nil
			return
		}
		log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) apply err(%v) retry(%v)",
			raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err, i)
	}

	return
}

// RandomWriteSubmit submits the proposal to raft.
func (dp *DataPartition) RandomWriteSubmit(pkg *repl.Packet) (err error) {
	val, err := MarshalRandWriteRaftLog(pkg.Opcode, pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data, pkg.CRC)
	if err != nil {
		log.LogErrorf("action[RandomWriteSubmit] [%v] marshal error %v", dp.partitionID, err)
		return
	}
	pkg.ResultCode, err = dp.Submit(val)
	return
}

func (dp *DataPartition) Submit(val []byte) (retCode uint8, err error) {
	var resp interface{}
	resp, err = dp.Put(nil, val)
	retCode, _ = resp.(uint8)
	if err != nil {
		log.LogErrorf("action[RandomWriteSubmit] submit err %v", err)
		return
	}
	return
}

func (dp *DataPartition) CheckWriteVer(p *repl.Packet) (err error) {
	log.LogDebugf("action[CheckWriteVer] packet %v dpseq %v ", p, dp.verSeq)
	if atomic.LoadUint64(&dp.verSeq) == p.VerSeq {
		return
	}

	if p.Opcode == proto.OpSyncRandomWrite || p.Opcode == proto.OpRandomWrite {
		err = fmt.Errorf("volume enable mulit version")
		log.LogErrorf("action[CheckWriteVer] error %v", err)
		return
	}
	if p.VerSeq < dp.verSeq {
		p.ExtentType |= proto.MultiVersionFlag
		p.ExtentType |= proto.VersionListFlag

		if p.Opcode == proto.OpRandomWriteVer || p.Opcode == proto.OpSyncRandomWriteVer {
			err = storage.VerNotConsistentError
			log.LogDebugf("action[CheckWriteVer] dp %v client verSeq[%v] small than dataPartiton ver[%v]",
				dp.config.PartitionID, p.VerSeq, dp.verSeq)
		}

		p.VerSeq = dp.verSeq
		dp.volVersionInfoList.RWLock.RLock()
		p.VerList = make([]*proto.VolVersionInfo, len(dp.volVersionInfoList.VerList))
		copy(p.VerList, dp.volVersionInfoList.VerList)
		dp.volVersionInfoList.RWLock.RUnlock()
		log.LogInfof("action[CheckWriteVer] partitionId %v reqId %v verList %v seq %v dpVerList %v",
			p.PartitionID, p.ReqID, p.VerList, p.VerSeq, dp.volVersionInfoList.VerList)
		return
	} else if p.VerSeq > dp.verSeq {
		log.LogWarnf("action[CheckWriteVer] partitionId %v reqId %v verList (%v) seq %v old one(%v)",
			p.PartitionID, p.ReqID, p.VerList, p.VerSeq, dp.volVersionInfoList.VerList)
		dp.verSeq = p.VerSeq
		dp.volVersionInfoList.RWLock.Lock()
		dp.volVersionInfoList.VerList = make([]*proto.VolVersionInfo, len(p.VerList))
		copy(dp.volVersionInfoList.VerList, p.VerList)
		dp.volVersionInfoList.RWLock.Unlock()
	}
	return
}
