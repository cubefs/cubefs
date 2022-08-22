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

	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

var (
	ExtentKeyHeader       = []byte("EKV2")
	ExtentKeyHeaderSize   = len(ExtentKeyHeader)
	ExtentLength          = 40
	ExtentKeyChecksumSize = 4
	ExtentV2Length        = ExtentKeyHeaderSize + ExtentLength + ExtentKeyChecksumSize
	InvalidKey            = errors.New("invalid key error")
	InvalidKeyHeader      = errors.New("invalid extent v2 key header error")
	InvalidKeyCheckSum    = errors.New("invalid extent v2 key checksum error")
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

func (k *ExtentKey) CheckSum() uint32 {
	sign := crc32.NewIEEE()
	buf, err := k.MarshalBinary()
	if err != nil {
		log.LogErrorf("[ExtentKey] extentKey %v CRC32 error: %v", k, err)
		return 0
	}
	sign.Write(buf)

	return sign.Sum32()
}

// marshal extentkey to []bytes with v2 of magic head
func (k *ExtentKey) MarshalBinaryWithCheckSum() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, ExtentV2Length))
	if err := binary.Write(buf, binary.BigEndian, ExtentKeyHeader); err != nil {
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
	if err := binary.Write(buf, binary.BigEndian, k.CheckSum()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// unmarshal extentkey from bytes.Buffer with checksum
func (k *ExtentKey) UnmarshalBinaryWithCheckSum(buf *bytes.Buffer) (err error) {
	var checksum uint32
	magic := make([]byte, ExtentKeyHeaderSize)
	if err = binary.Read(buf, binary.BigEndian, magic); err != nil {
		return
	}
	if r := bytes.Compare(magic, ExtentKeyHeader); r != 0 {
		err = InvalidKeyHeader
		return
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
	if err = binary.Read(buf, binary.BigEndian, &checksum); err != nil {
		return
	}
	if k.CheckSum() != checksum {
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
