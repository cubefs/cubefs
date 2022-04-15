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
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"

	"github.com/tiglabs/raft"
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
	magic    int
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
	RandomWriteRaftLogMagicVersionV3 = 0xF3
	MaxRandomWriteOpItemPoolSize=32
)

func MarshalRandWriteRaftLogV3(opcode uint8, extentID uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	if len(data)<proto.RandomWriteRaftLogV3HeaderSize{
		return nil,fmt.Errorf("data too low for MarshalRandWriteRaftLogV3(%v)",len(data))
	}
	var index int
	binary.BigEndian.PutUint32(data[index:index+4],uint32(RandomWriteRaftLogMagicVersionV3))
	index+=4
	data[index]=opcode
	index+=1
	binary.BigEndian.PutUint64(data[index:index+8],extentID)
	index+=8
	binary.BigEndian.PutUint64(data[index:index+8],uint64(offset))
	index+=8
	binary.BigEndian.PutUint64(data[index:index+8],uint64(size))
	index+=8
	binary.BigEndian.PutUint32(data[index:index+4],uint32(crc))
	index+=4
	result=data
	return
}


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

var (
	RandomWriteOpItemPool [MaxRandomWriteOpItemPoolSize]*sync.Pool
)

func init(){
	rand.Seed(time.Now().UnixNano())
	for i:=0;i<MaxRandomWriteOpItemPoolSize;i++{
		RandomWriteOpItemPool[i]=&sync.Pool{
			New: func() interface{} {
				return new(rndWrtOpItem)
			},
		}
	}
}

func GetRandomWriteOpItem()(item *rndWrtOpItem) {
	magic:=rand.Intn(MaxRandomWriteOpItemPoolSize)
	item=RandomWriteOpItemPool[magic].Get().(*rndWrtOpItem)
	item.magic=magic
	item.size=0
	item.crc=0
	item.offset=0
	item.extentID=0
	item.opcode=0
	item.data=nil
	return
}

func PutRandomWriteOpItem(item *rndWrtOpItem){
	if item==nil || item.magic==0 {
		return
	}
	item.size=0
	item.crc=0
	item.offset=0
	item.extentID=0
	item.opcode=0
	item.data=nil
	RandomWriteOpItemPool[item.magic].Put(item)
}


// RandomWriteSubmit submits the proposal to raft.
func UnmarshalRandWriteRaftLog(raw []byte) (opItem *rndWrtOpItem, err error) {
	opItem = GetRandomWriteOpItem()
	var index int
	version:=binary.BigEndian.Uint32(raw[index:index+4])
	index+=4
	if version==RandomWriteRaftLogMagicVersionV3{
		return BinaryUnmarshalRandWriteRaftLogV3(raw)
	}
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		return
	}

	if version != BinaryMarshalMagicVersion {
		opItem, err = UnmarshalOldVersionRaftLog(raw)
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


// RandomWriteSubmit submits the proposal to raft.
func BinaryUnmarshalRandWriteRaftLogV3(raw []byte) (opItem *rndWrtOpItem, err error) {
	opItem = GetRandomWriteOpItem()
	var index int
	if len(raw)<proto.RandomWriteRaftLogV3HeaderSize{
		err=fmt.Errorf("unavali RandomWriteRaftlog Header, raw len(%v)",len(raw))
	}
	version:=binary.BigEndian.Uint32(raw[index:index+4])
	index+=4
	if version != RandomWriteRaftLogMagicVersionV3 {
		return nil,fmt.Errorf("unavali raftLogVersion %v",RandomWriteRaftLogMagicVersionV3)
	}
	opItem.opcode=raw[index]
	index+=1
	opItem.extentID=binary.BigEndian.Uint64(raw[index:index+8])
	index+=8
	opItem.offset=int64(binary.BigEndian.Uint64(raw[index:index+8]))
	index+=8
	opItem.size=int64(binary.BigEndian.Uint64(raw[index:index+8]))
	index+=8
	opItem.crc=binary.BigEndian.Uint32(raw[index:index+4])
	index+=4
	if opItem.size+int64(index)!=int64(len(raw)){
		err=fmt.Errorf("unavali RandomWriteRaftlog body, raw len(%v), has unmarshal(%v) opItemSize(%v)",len(raw),index,opItem.size )
	}
	opItem.data = raw[index:int64(index)+opItem.size]

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

func (dp *DataPartition) checkWriteErrs(errMsg string) (ignore bool) {
	// file has been deleted when applying the raft log
	if strings.Contains(errMsg, storage.ExtentHasBeenDeletedError.Error()) || strings.Contains(errMsg, storage.ExtentNotFoundError.Error()) {
		return true
	}
	return false
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
func (dp *DataPartition) ApplyRandomWrite(opItem *rndWrtOpItem, raftApplyID uint64) (resp interface{}, err error) {
	start := time.Now().UnixMicro()
	defer func() {
		if err == nil {
			resp = proto.OpOk
			if log.IsDebugEnabled() {
				log.LogWritef("[ApplyRandomWrite] "+
					"ApplyID(%v) Partition(%v)_Extent(%v)_"+
					"ExtentOffset(%v)_Size(%v)_CRC(%v) cost(%v)us",
					raftApplyID, dp.partitionID, opItem.extentID,
					opItem.offset, opItem.size, opItem.crc, time.Now().UnixMicro()-start)
			}
		} else {
			msg := fmt.Sprintf("[ApplyRandomWrite] "+
				"ApplyID(%v) Partition(%v)_Extent(%v)_"+
				"ExtentOffset(%v)_Size(%v)_CRC(%v)  Failed Result(%v) cost(%v)us",
				raftApplyID, dp.partitionID, opItem.extentID,
				opItem.offset, opItem.size, opItem.crc, err.Error(), time.Now().UnixMicro()-start)
			exporter.Warning(msg)
			resp = proto.OpDiskErr
			log.LogErrorf(msg)
		}
	}()
	for i := 0; i < 20; i++ {
		err = func() error {
			var storeErr error
			tp := exporter.NewTPCnt("ApplyRandomWrite_StoreWrite")
			defer func() {
				tp.Set(storeErr)
			}()
			storeErr = dp.ExtentStore().Write(nil, opItem.extentID, opItem.offset, opItem.size, opItem.data, opItem.crc, storage.RandomWriteType, opItem.opcode == proto.OpSyncRandomWrite)
			return storeErr
		}()
		if dp.checkIsDiskError(err) {
			return
		}
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "illegal") {
			err = nil
			break
		}
		if strings.Contains(err.Error(), storage.ExtentNotFoundError.Error()) {
			err = nil
			return
		}
		log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) apply err(%v) retry(%v)", raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err, i)
	}
	dp.monitorData[statistics.ActionOverWrite].UpdateData(uint64(opItem.size))

	return
}

// RandomWriteSubmit submits the proposal to raft.
func (dp *DataPartition) RandomWriteSubmit(pkg *repl.Packet) (err error) {
	err = dp.ExtentStore().CheckIsAvaliRandomWrite(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size))
	if err != nil {
		return err
	}

	val, err := MarshalRandWriteRaftLog(pkg.Opcode, pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data[:pkg.Size], pkg.CRC)
	if err != nil {
		return
	}
	var (
		resp interface{}
	)
	if resp, err = dp.Put(pkg.Ctx(), nil, val); err != nil {
		return
	}

	pkg.ResultCode = resp.(uint8)

	log.LogDebugf("[RandomWrite] SubmitRaft: %v", pkg.GetUniqueLogId())

	return
}


// RandomWriteSubmit submits the proposal to raft.
func (dp *DataPartition) RandomWriteSubmitV3(pkg *repl.Packet) (err error) {
	err = dp.ExtentStore().CheckIsAvaliRandomWrite(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size))
	if err != nil {
		return err
	}
	//if len(pkg.Data)<int(pkg.Size)+proto.RandomWriteRaftLogV3HeaderSize{
	//	err=fmt.Errorf("unavali len(pkg.Data)(%v) ,pkg.Size(%v)," +
	//		"RandomWriteRaftLogV3HeaderSize(%v)",len(pkg.Data),pkg.Size,proto.RandomWriteRaftLogV3HeaderSize)
	//	return
	//}
	val, err := MarshalRandWriteRaftLogV3(pkg.Opcode, pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data[0:int(pkg.Size)+proto.RandomWriteRaftLogV3HeaderSize], pkg.CRC)
	if err != nil {
		return
	}
	var (
		resp interface{}
	)
	if resp, err = dp.Put(pkg.Ctx(), nil, val); err != nil {
		return
	}
	pkg.ResultCode = resp.(uint8)
	if log.IsDebugEnabled() {
		log.LogDebugf("[RandomWrite] SubmitRaft: %v", pkg.GetUniqueLogId())
	}

	return
}
