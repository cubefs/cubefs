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

package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var InvalidKey = errors.New("invalid key error")

type ExtentKey struct {
	PartitionId uint32
	ExtentId    uint64
	Size        uint32
	Crc         uint32
}

func (ek ExtentKey) String() string {
	return fmt.Sprintf("ExtentKey{Partition(%v),ExtentID(%v),Size(%v),CRC(%v)}", ek.PartitionId, ek.ExtentId, ek.Size, ek.Crc)
}

func (ek *ExtentKey) Equal(k ExtentKey) bool {
	return ek.PartitionId == k.PartitionId && ek.ExtentId == k.ExtentId
}

func (ek *ExtentKey) FullEqual(k ExtentKey) bool {
	return ek.PartitionId == k.PartitionId && ek.ExtentId == k.ExtentId && ek.Size == k.Size
}

func (k *ExtentKey) Marshal() (m string) {
	return fmt.Sprintf("%v_%v_%v_%v", k.PartitionId, k.ExtentId, k.Size, k.Crc)
}

func (k *ExtentKey) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buf, binary.BigEndian, k.PartitionId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.ExtentId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Size); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Crc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (k *ExtentKey) UnmarshalBinary(buf *bytes.Buffer) (err error) {
	if err = binary.Read(buf, binary.BigEndian, &k.PartitionId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Size); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Crc); err != nil {
		return
	}
	return
}

func (k *ExtentKey) GetExtentKey() (m string) {
	return fmt.Sprintf("%v_%v", k.PartitionId, k.ExtentId)
}

func (k *ExtentKey) UnMarshal(m string) (err error) {
	var (
		size uint64
		crc  uint64
	)
	err = InvalidKey
	keyArr := strings.Split(m, "_")
	size, err = strconv.ParseUint(keyArr[2], 10, 64)
	if err != nil {
		return
	}
	crc, err = strconv.ParseUint(keyArr[3], 10, 64)
	if err != nil {
		return
	}
	vId, _ := strconv.ParseUint(keyArr[0], 10, 32)
	k.ExtentId, _ = strconv.ParseUint(keyArr[1], 10, 64)
	k.PartitionId = uint32(vId)
	k.Size = uint32(size)
	k.Crc = uint32(crc)

	return nil
}
