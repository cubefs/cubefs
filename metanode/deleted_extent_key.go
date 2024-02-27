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
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
)

type DeletedExtentKey struct {
	ExtentKey       proto.ExtentKey
	Inode           uint64
	DeletedExtentId uint64
}

var _ BtreeItem = &DeletedExtentKey{}

func (dek *DeletedExtentKey) Less(than BtreeItem) (ok bool) {
	// NOTE: partition id
	other := than.(*DeletedExtentKey)
	// NOTE: inode
	if dek.Inode != other.Inode {
		return dek.Inode < other.Inode
	}
	// NOTE: partition
	if dek.ExtentKey.PartitionId != other.ExtentKey.PartitionId {
		return dek.ExtentKey.PartitionId < other.ExtentKey.PartitionId
	}
	// NOTE: extent id
	if dek.ExtentKey.ExtentId != other.ExtentKey.ExtentId {
		return dek.ExtentKey.ExtentId < other.ExtentKey.ExtentId
	}
	// NOTE: extent offset
	if dek.ExtentKey.ExtentOffset != other.ExtentKey.ExtentOffset {
		return dek.ExtentKey.ExtentOffset < other.ExtentKey.ExtentOffset
	}
	// NOTE: deleted extent id
	return dek.DeletedExtentId < other.DeletedExtentId
}

func (dek *DeletedExtentKey) Copy() (other BtreeItem) {
	other = &DeletedExtentKey{
		ExtentKey:       dek.ExtentKey,
		Inode:           dek.Inode,
		DeletedExtentId: dek.DeletedExtentId,
	}
	return other
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
	err = dek.UnmarshalWithBuffer(buf)
	return
}

func (dek *DeletedExtentKey) UnmarshalWithBuffer(buf *bytes.Buffer) (err error) {
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

func NewDeletedExtentKeyPrefix(ino uint64) (dek *DeletedExtentKey) {
	return &DeletedExtentKey{
		ExtentKey:       proto.ExtentKey{},
		Inode:           ino,
		DeletedExtentId: 0,
	}
}

func (mp *metaPartition) AllocDeletedExtentId() (id uint64) {
	id = atomic.AddUint64(&mp.deletedExtentId, 1)
	mp.deletedExtentsTree.SetDeletedExtentId(id)
	return
}

func (mp *metaPartition) GetDeletedExtentId() (id uint64) {
	id = atomic.LoadUint64(&mp.deletedExtentId)
	return
}

func (mp *metaPartition) SetDeletedExtentId(id uint64) {
	// NOTE: dek id is increase only
	now := atomic.LoadUint64(&mp.deletedExtentId)
	for now < id {
		if atomic.CompareAndSwapUint64(&mp.deletedExtentId, now, id) {
			return
		}
		now = atomic.LoadUint64(&mp.deletedExtentId)
	}
}

type DeletedObjExtentKey struct {
	ObjExtentKey    proto.ObjExtentKey
	Inode           uint64
	DeletedExtentId uint64
}

var _ BtreeItem = &DeletedObjExtentKey{}

func (doek *DeletedObjExtentKey) Less(than BtreeItem) (ok bool) {
	other := than.(*DeletedObjExtentKey)
	// NOTE: inode
	if doek.Inode != other.Inode {
		return doek.Inode < other.Inode
	}
	// NOTE: cid
	if doek.ObjExtentKey.Cid != other.ObjExtentKey.Cid {
		return doek.ObjExtentKey.Cid < other.ObjExtentKey.Cid
	}
	// NOTE: file offset
	if doek.ObjExtentKey.FileOffset != other.ObjExtentKey.FileOffset {
		return doek.ObjExtentKey.FileOffset < other.ObjExtentKey.FileOffset
	}
	return doek.DeletedExtentId < other.DeletedExtentId
}

func (doek *DeletedObjExtentKey) Copy() (other BtreeItem) {
	other = &DeletedObjExtentKey{
		ObjExtentKey:    doek.ObjExtentKey,
		Inode:           doek.Inode,
		DeletedExtentId: doek.DeletedExtentId,
	}
	return
}

func (doek *DeletedObjExtentKey) Marshal() (v []byte, err error) {
	buff := bytes.NewBuffer([]byte{})
	if v, err = doek.ObjExtentKey.MarshalBinary(); err != nil {
		return
	}
	_, err = buff.Write(v)
	if err != nil {
		return
	}
	err = binary.Write(buff, binary.BigEndian, doek.Inode)
	if err != nil {
		return
	}
	err = binary.Write(buff, binary.BigEndian, doek.DeletedExtentId)
	if err != nil {
		return
	}
	v = buff.Bytes()
	return
}

func (doek *DeletedObjExtentKey) Unmarshal(v []byte) (err error) {
	buf := bytes.NewBuffer(v)
	doek.UnmarshalWithBuffer(buf)
	return
}

func (doek *DeletedObjExtentKey) UnmarshalWithBuffer(buf *bytes.Buffer) (err error) {
	err = doek.ObjExtentKey.UnmarshalBinary(buf)
	if err != nil {
		return
	}
	err = binary.Read(buf, binary.BigEndian, &doek.Inode)
	if err != nil {
		return
	}
	err = binary.Read(buf, binary.BigEndian, &doek.DeletedExtentId)
	if err != nil {
		return
	}
	return
}

func NewDeletedObjExtentKey(oek *proto.ObjExtentKey, ino, deletedExtentId uint64) (doek *DeletedObjExtentKey) {
	doek = &DeletedObjExtentKey{
		ObjExtentKey:    *oek,
		Inode:           ino,
		DeletedExtentId: deletedExtentId,
	}
	return
}

func NewDeletedObjExtentKeyPrefix(ino uint64) (doek *DeletedObjExtentKey) {
	doek = &DeletedObjExtentKey{
		ObjExtentKey: proto.ObjExtentKey{},
		Inode:        ino,
	}
	return
}
