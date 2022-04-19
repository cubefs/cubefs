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

package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/btree"
)

var (
	ExtentLength      = 40
	ExtentDbKeyLength = 24
	InvalidKey        = errors.New("invalid key error")
)

// ExtentKey defines the extent key struct.
type ExtentKey struct {
	FileOffset   uint64
	PartitionId  uint64
	ExtentId     uint64
	ExtentOffset uint64
	Size         uint32
	CRC          uint32
}

// String returns the string format of the extentKey.
func (k ExtentKey) String() string {
	return fmt.Sprintf("ExtentKey{FileOffset(%v),Partition(%v),ExtentID(%v),ExtentOffset(%v),Size(%v),CRC(%v)}", k.FileOffset, k.PartitionId, k.ExtentId, k.ExtentOffset, k.Size, k.CRC)
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

// MarshalDbKey marshals the binary format of the extent for db key.
//pid(8) + extent Id(8) + extent offset(4) + extent size(4)
func (k *ExtentKey) MarshalDbKey() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, ExtentLength))
	if err := binary.Write(buf, binary.BigEndian, k.PartitionId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.ExtentId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, (uint32)(k.ExtentOffset)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Size); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//pid(8) + extent Id(8) + extent offset(4) + extent size(4)
func (k *ExtentKey) UnmarshalDbKey(buffer []byte) (err error) {
	var ekOffset uint32
	buf := bytes.NewBuffer(buffer)
	if err = binary.Read(buf, binary.BigEndian, &k.PartitionId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &ekOffset); err != nil {
		return
	}
	k.ExtentOffset = uint64(ekOffset)
	if err = binary.Read(buf, binary.BigEndian, &k.Size); err != nil {
		return
	}
	return nil
}

//pid(8) + extent Id(8) + extent offset(4) + extent size(4)
func (k *ExtentKey) UnmarshalDbKeyByBuffer(buf *bytes.Buffer) (err error) {
	var ekOffset uint32
	if err = binary.Read(buf, binary.BigEndian, &k.PartitionId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &ekOffset); err != nil {
		return
	}
	k.ExtentOffset = uint64(ekOffset)
	if err = binary.Read(buf, binary.BigEndian, &k.Size); err != nil {
		return
	}
	return nil
}

// MarshalBinary marshals the binary format of the extent key.
func (k *ExtentKey) MarshalBinary() ([]byte, error) {
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
	return buf.Bytes(), nil
}

func (k *ExtentKey) MarshalBinaryV2() ([]byte, error) {
	data := make([]byte, 0, ExtentLength)
	binary.BigEndian.PutUint64(data[0:8], k.FileOffset)
	binary.BigEndian.PutUint64(data[8:16], k.PartitionId)
	binary.BigEndian.PutUint64(data[16:24], k.ExtentId)
	binary.BigEndian.PutUint64(data[24:32], k.ExtentOffset)
	binary.BigEndian.PutUint32(data[32:36], k.Size)
	binary.BigEndian.PutUint32(data[36:40], k.CRC)
	return data, nil
}

func (k *ExtentKey) EncodeBinary(data []byte) {
	binary.BigEndian.PutUint64(data[0:8], k.FileOffset)
	binary.BigEndian.PutUint64(data[8:16], k.PartitionId)
	binary.BigEndian.PutUint64(data[16:24], k.ExtentId)
	binary.BigEndian.PutUint64(data[24:32], k.ExtentOffset)
	binary.BigEndian.PutUint32(data[32:36], k.Size)
	binary.BigEndian.PutUint32(data[36:40], k.CRC)
	return
}

// UnmarshalBinary unmarshals the binary format of the extent key.
func (k *ExtentKey) UnmarshalBinary(buf *bytes.Buffer) (err error) {
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
	return
}

func (k *ExtentKey) UnmarshalBinaryV2(data []byte) (err error) {
	if len(data) < ExtentLength {
		return fmt.Errorf("ekdata buff err, need at least %d, but buff len:%d", ExtentLength, len(data))
	}
	k.FileOffset = binary.BigEndian.Uint64(data[0:8])
	k.PartitionId = binary.BigEndian.Uint64(data[8:16])
	k.ExtentId = binary.BigEndian.Uint64(data[16:24])
	k.ExtentOffset = binary.BigEndian.Uint64(data[24:32])
	k.Size = binary.BigEndian.Uint32(data[32:36])
	k.CRC = binary.BigEndian.Uint32(data[36:40])

	return nil
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

// This method determines whether there is an overlapping relationship
// between the two specified EK expression ranges based on following
// comparison: upper1 >= lower2 && upper2 >= lower1 .
func (k *ExtentKey) Overlap(o *ExtentKey) bool {
	return k.FileOffset+uint64(k.Size) >= o.FileOffset && o.FileOffset+uint64(o.Size) >= k.FileOffset
}

func (k *ExtentKey) Equal(k1 *ExtentKey) bool {
	return k.FileOffset == k1.FileOffset &&
		k.PartitionId == k1.PartitionId &&
		k.ExtentId == k1.ExtentId &&
		k.ExtentOffset == k1.ExtentOffset &&
		k.Size == k1.Size
}

type TinyExtentDeleteRecord struct {
	FileOffset   uint64
	PartitionId  uint64
	ExtentId     uint64
	ExtentOffset uint64
	Size         uint32
	CRC          uint32
}

const (
	ExtentKeyPoolCnt = 32
)

var (
	extentPool [ExtentKeyPoolCnt]*sync.Pool
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for index := 0; index < ExtentKeyPoolCnt; index++ {
		extentPool[index] = &sync.Pool{
			New: func() interface{} {
				return new(ExtentKey)
			},
		}
	}
}

func GetExtentKeyFromPool() *ExtentKey {
	ek := extentPool[rand.Intn(ExtentKeyPoolCnt)].Get().(*ExtentKey)
	ek.Size = 0
	ek.ExtentOffset = 0
	ek.ExtentId = 0
	ek.CRC = 0
	ek.FileOffset = 0
	ek.PartitionId = 0
	return ek
}

func PutExtentKeyToPool(ek *ExtentKey) {
	if ek != nil {
		extentPool[rand.Intn(ExtentKeyPoolCnt)].Put(ek)
	}
}
