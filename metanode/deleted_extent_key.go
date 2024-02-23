// Copyright 2023 The CubeFS Authors.
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

package metanode

import (
	"bytes"
	"encoding/binary"

	"github.com/cubefs/cubefs/proto"
)

type DeletedExtentKey struct {
	proto.ExtentKey
	Inode           uint64
	DeletedExtentId uint64
}

func (dek *DeletedExtentKey) Less(than BtreeItem) (ok bool) {
	// NOTE: partition id
	other := than.(*DeletedExtentKey)
	if dek.PartitionId != other.PartitionId {
		return dek.PartitionId < other.PartitionId
	}
	// NOTE: inode
	if dek.Inode != other.Inode {
		return dek.Inode < other.Inode
	}
	// NOTE: extent id
	if dek.ExtentId != other.ExtentId {
		return dek.ExtentId < other.ExtentId
	}
	// NOTE: unique id
	if dek.DeletedExtentId != other.DeletedExtentId {
		return dek.DeletedExtentId < other.DeletedExtentId
	}
	return
}

func (dek *DeletedExtentKey) Marshal() (v []byte, err error) {
	v, err = dek.ExtentKey.MarshalBinary(true)
	if err != nil {
		return
	}
	buff := bytes.NewBuffer(v)
	err = binary.Write(buff, binary.BigEndian, dek.Inode)
	if err != nil {
		return
	}
	err = binary.Write(buff, binary.BigEndian, dek.DeletedExtentId)
	if err != nil {
		return
	}
	v = buff.Bytes()
	return
}

func (dek *DeletedExtentKey) Unmarshal(v []byte) (err error) {
	buf := bytes.NewBuffer(v)
	err = dek.ExtentKey.UnmarshalBinary(buf, true)
	if err != nil {
		return
	}
	err = binary.Read(buf, binary.BigEndian, &dek.Inode)
	if err != nil {
		return
	}
	err = binary.Read(buf, binary.BigEndian, &dek.DeletedExtentId)
	if err != nil {
		return
	}
	return
}

func NewDeletedExtentKey(ek *proto.ExtentKey, ino, deletedExtentId uint64) (dek *DeletedExtentKey) {
	return &DeletedExtentKey{
		ExtentKey:       *ek,
		Inode:           ino,
		DeletedExtentId: deletedExtentId,
	}
}

func NewDeletedExtentKeyPrefix(partitionId uint64, ino uint64) (dek *DeletedExtentKey) {
	return &DeletedExtentKey{
		ExtentKey: proto.ExtentKey{
			PartitionId: partitionId,
		},
		Inode:           ino,
		DeletedExtentId: 0,
	}
}
