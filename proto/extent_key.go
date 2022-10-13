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

package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

var (
	ExtentKeyHeader       = []byte("EKV2")
	ExtentKeyHeaderV3     = []byte("EKV3")
	ExtentKeyHeaderSize   = len(ExtentKeyHeader)
	ExtentLength          = 40
	ExtentKeyChecksumSize = 4
	ExtentVerFieldSize    = 9 // ver(8) and isSplit(1)
	ExtentV2Length        = ExtentKeyHeaderSize + ExtentLength + ExtentKeyChecksumSize
	ExtentV3Length        = ExtentKeyHeaderSize + ExtentLength + ExtentKeyChecksumSize + ExtentVerFieldSize
	InvalidKey            = errors.New("invalid key error")
	InvalidKeyHeader      = errors.New("invalid extent v2 key header error")
	InvalidKeyCheckSum    = errors.New("invalid extent v2 key checksum error")
)

// ExtentKey defines the extent key struct.
type ExtentKey struct {
	FileOffset   uint64 // offset in file
	PartitionId  uint64
	ExtentId     uint64
	ExtentOffset uint64 // offset in extent like tiny extent offset large than 0,normal is 0
	Size         uint32 // extent real size?
	CRC          uint32
	//snapshot
	VerSeq  uint64
	ModGen  uint64
	IsSplit bool
}

func (k *ExtentKey) GenerateId() uint64 {
	if k.PartitionId > math.MaxUint32 || k.ExtentId > math.MaxUint32 {
		log.LogFatal("ext %v abnormal", k)
	}
	return (k.PartitionId << 32) | k.ExtentId
}

func ParseFromId(sID uint64) (dpID uint64, extID uint64) {
	dpID = sID >> 32
	extID = sID & math.MaxUint32
	return
}

func MergeSplitKey(inodeID uint64, ekRefMap *sync.Map, sMap *sync.Map) (err error) {
	if ekRefMap == nil || sMap == nil {
		log.LogErrorf("MergeSplitKey. inodeID %v should not use nil role", inodeID)
		return
	}
	sMap.Range(func(id, value interface{}) bool {
		dpID, extID := ParseFromId(id.(uint64))
		nVal := uint32(0)
		val, ok := ekRefMap.Load(id)
		if ok {
			nVal = val.(uint32)
		}
		log.LogDebugf("UnmarshalInodeValue inode %v get splitID %v dp id %v extentid %v, refCnt %v, add %v",
			inodeID, id.(uint64), dpID, extID, value.(uint32), nVal)

		ekRefMap.Store(id, nVal+value.(uint32))
		return true
	})
	return
}

func (k *ExtentKey) IsEqual(rightKey *ExtentKey) bool {
	//	return false
	return k.PartitionId == rightKey.PartitionId &&
		k.ExtentId == rightKey.ExtentId &&
		k.VerSeq == rightKey.VerSeq &&
		k.ExtentOffset == rightKey.ExtentOffset &&
		k.FileOffset == rightKey.FileOffset
}

func (k *ExtentKey) IsSequence(rightKey *ExtentKey) bool {
	//	return false
	return k.PartitionId == rightKey.PartitionId &&
		k.ExtentId == rightKey.ExtentId &&
		k.VerSeq == rightKey.VerSeq &&
		k.ExtentOffset+uint64(k.Size) == rightKey.ExtentOffset &&
		k.FileOffset+uint64(k.Size) == rightKey.FileOffset
}

func (k *ExtentKey) IsFileInSequence(rightKey *ExtentKey) bool {
	//	return false
	return k.PartitionId == rightKey.PartitionId &&
		k.ExtentId == rightKey.ExtentId &&
		k.ExtentOffset+uint64(k.Size) == rightKey.ExtentOffset
}

// String returns the string format of the extentKey.
func (k ExtentKey) String() string {
	return fmt.Sprintf("ExtentKey{FileOffset(%v),VerSeq(%v) Partition(%v),ExtentID(%v),ExtentOffset(%v),isSplit(%v),Size(%v),CRC(%v)}",
		k.FileOffset, k.VerSeq, k.PartitionId, k.ExtentId, k.ExtentOffset, k.IsSplit, k.Size, k.CRC)
}

// Less defines the less comparator.
func (k *ExtentKey) Less(than btree.Item) bool {
	that := than.(*ExtentKey)
	return k.FileOffset < that.FileOffset
}

// Marshal marshals the extent key.
func (k *ExtentKey) Copy() btree.Item {
	return k
}

func (k *ExtentKey) Marshal() (m string) {
	return fmt.Sprintf("%v_%v_%v_%v_%v_%v", k.FileOffset, k.PartitionId, k.ExtentId, k.ExtentOffset, k.Size, k.CRC)
}

func (k *ExtentKey) MarshalBinaryExt(data []byte) {
	binary.BigEndian.PutUint64(data[0:], k.FileOffset)
	binary.BigEndian.PutUint64(data[8:], k.PartitionId)
	binary.BigEndian.PutUint64(data[16:], k.ExtentId)
	binary.BigEndian.PutUint64(data[24:], k.ExtentOffset)
	binary.BigEndian.PutUint32(data[32:], k.Size)
	binary.BigEndian.PutUint32(data[36:], k.CRC)
}

// MarshalBinary marshals the binary format of the extent key.
func (k *ExtentKey) MarshalBinary(v3 bool) ([]byte, error) {
	log.LogDebugf("MarshalBinary ek %v", k)
	extLen := ExtentLength
	if v3 {
		extLen += ExtentVerFieldSize
	}

	buf := bytes.NewBuffer(make([]byte, 0, ExtentLength))
	if err := binary.Write(buf, binary.BigEndian, k.FileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.PartitionId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.ExtentId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.ExtentOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Size); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.CRC); err != nil {
		return nil, err
	}
	if v3 {
		if err := binary.Write(buf, binary.BigEndian, k.VerSeq); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, k.IsSplit); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary unmarshals the binary format of the extent key.
func (k *ExtentKey) UnmarshalBinary(buf *bytes.Buffer, v3 bool) (err error) {
	if err = binary.Read(buf, binary.BigEndian, &k.FileOffset); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.PartitionId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentOffset); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Size); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.CRC); err != nil {
		return
	}
	if v3 {
		if err = binary.Read(buf, binary.BigEndian, &k.VerSeq); err != nil {
			return
		}
		if err = binary.Read(buf, binary.BigEndian, &k.IsSplit); err != nil {
			return
		}
	}

	return
}

func (k *ExtentKey) CheckSum(v3 bool) uint32 {
	sign := crc32.NewIEEE()
	buf, err := k.MarshalBinary(v3)
	if err != nil {
		log.LogErrorf("[ExtentKey] extentKey %v CRC32 error: %v", k, err)
		return 0
	}
	sign.Write(buf)

	return sign.Sum32()
}

// marshal extentkey to []bytes with v2 of magic head
func (k *ExtentKey) MarshalBinaryWithCheckSum(v3 bool) ([]byte, error) {
	extLen := ExtentV2Length
	flag := ExtentKeyHeader
	if v3 {
		extLen = ExtentV3Length
		flag = ExtentKeyHeaderV3
	}
	buf := bytes.NewBuffer(make([]byte, 0, extLen))
	if err := binary.Write(buf, binary.BigEndian, flag); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.FileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.PartitionId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.ExtentId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.ExtentOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Size); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.CRC); err != nil {
		return nil, err
	}
	if v3 {
		if err := binary.Write(buf, binary.BigEndian, k.VerSeq); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, k.IsSplit); err != nil {
			return nil, err
		}
	}
	if err := binary.Write(buf, binary.BigEndian, k.CheckSum(v3)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// unmarshal extentkey from bytes.Buffer with checksum
func (k *ExtentKey) UnmarshalBinaryWithCheckSum(buf *bytes.Buffer) (err error) {
	var (
		checksum uint32
		v3       bool
	)
	magic := make([]byte, ExtentKeyHeaderSize)

	if err = binary.Read(buf, binary.BigEndian, magic); err != nil {
		return
	}
	log.LogDebugf("action[UnmarshalBinaryWithCheckSum] header magic %v", string(magic))
	if r := bytes.Compare(magic, ExtentKeyHeader); r != 0 {
		if r = bytes.Compare(magic, ExtentKeyHeaderV3); r != 0 {
			log.LogErrorf("action[UnmarshalBinaryWithCheckSum] err header magic %v", string(magic))
			err = InvalidKeyHeader
			return
		}
		v3 = true
	}
	if err = binary.Read(buf, binary.BigEndian, &k.FileOffset); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.PartitionId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentOffset); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Size); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.CRC); err != nil {
		return
	}

	if v3 {
		if err = binary.Read(buf, binary.BigEndian, &k.VerSeq); err != nil {
			return
		}
		if err = binary.Read(buf, binary.BigEndian, &k.IsSplit); err != nil {
			return
		}
	}

	if err = binary.Read(buf, binary.BigEndian, &checksum); err != nil {
		return
	}

	if k.CheckSum(v3) != checksum {
		err = InvalidKeyCheckSum
		return
	}

	return
}

// TODO remove
func (k *ExtentKey) UnMarshal(m string) (err error) {
	_, err = fmt.Sscanf(m, "%v_%v_%v_%v_%v_%v", &k.FileOffset, &k.PartitionId, &k.ExtentId, &k.ExtentOffset, &k.Size, &k.CRC)
	return
}

// TODO remove
func (k *ExtentKey) GetExtentKey() (m string) {
	return fmt.Sprintf("%v_%v_%v_%v_%v", k.PartitionId, k.FileOffset, k.ExtentId, k.ExtentOffset, k.Size)
}

type TinyExtentDeleteRecord struct {
	FileOffset   uint64
	PartitionId  uint64
	ExtentId     uint64
	ExtentOffset uint64
	Size         uint32
	CRC          uint32
}
