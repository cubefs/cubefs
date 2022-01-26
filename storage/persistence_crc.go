// Copyright 2018 The Cubefs Authors.
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

package storage

import (
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

type BlockCrc struct {
	BlockNo int
	Crc     uint32
}
type BlockCrcArr []*BlockCrc

const (
	BaseExtentIDOffset = 0
)

func (arr BlockCrcArr) Len() int           { return len(arr) }
func (arr BlockCrcArr) Less(i, j int) bool { return arr[i].BlockNo < arr[j].BlockNo }
func (arr BlockCrcArr) Swap(i, j int)      { arr[i], arr[j] = arr[j], arr[i] }

type UpdateCrcFunc func(e *Extent, blockNo int, crc uint32) (err error)
type GetExtentCrcFunc func(extentID uint64) (crc uint32, err error)

func (s *ExtentStore) PersistenceBlockCrc(e *Extent, blockNo int, blockCrc uint32) (err error) {
	if !proto.IsNormalDp(s.partitionType) {
		return
	}

	startIdx := blockNo * util.PerBlockCrcSize
	endIdx := startIdx + util.PerBlockCrcSize
	binary.BigEndian.PutUint32(e.header[startIdx:endIdx], blockCrc)
	verifyStart := startIdx + int(util.BlockHeaderSize*e.extentID)
	if _, err = s.verifyExtentFp.WriteAt(e.header[startIdx:endIdx], int64(verifyStart)); err != nil {
		return
	}

	return
}

func (s *ExtentStore) DeleteBlockCrc(extentID uint64) (err error) {
	if !proto.IsNormalDp(s.partitionType) {
		return
	}

	err = fallocate(int(s.verifyExtentFp.Fd()), FallocFLPunchHole|FallocFLKeepSize,
		int64(util.BlockHeaderSize*extentID), util.BlockHeaderSize)

	return
}

func (s *ExtentStore) PersistenceBaseExtentID(extentID uint64) (err error) {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, extentID)
	_, err = s.metadataFp.WriteAt(value, BaseExtentIDOffset)
	return
}

func (s *ExtentStore) GetPreAllocSpaceExtentIDOnVerfiyFile() (extentID uint64) {
	value := make([]byte, 8)
	_, err := s.metadataFp.ReadAt(value, 8)
	if err != nil {
		return
	}
	extentID = binary.BigEndian.Uint64(value)
	return
}

func (s *ExtentStore) PreAllocSpaceOnVerfiyFile(currExtentID uint64) {
	if !proto.IsNormalDp(s.partitionType) {
		return
	}

	if currExtentID > atomic.LoadUint64(&s.hasAllocSpaceExtentIDOnVerfiyFile) {
		prevAllocSpaceExtentID := int64(atomic.LoadUint64(&s.hasAllocSpaceExtentIDOnVerfiyFile))
		endAllocSpaceExtentID := int64(prevAllocSpaceExtentID + 1000)
		size := int64(1000 * util.BlockHeaderSize)
		err := fallocate(int(s.verifyExtentFp.Fd()), 1, prevAllocSpaceExtentID*util.BlockHeaderSize, size)
		if err != nil {
			return
		}
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(endAllocSpaceExtentID))
		if _, err = s.metadataFp.WriteAt(data, 8); err != nil {
			return
		}
		atomic.StoreUint64(&s.hasAllocSpaceExtentIDOnVerfiyFile, uint64(endAllocSpaceExtentID))
		log.LogInfof("Action(PreAllocSpaceOnVerfiyFile) PartitionID(%v) currentExtent(%v)"+
			"PrevAllocSpaceExtentIDOnVerifyFile(%v) EndAllocSpaceExtentIDOnVerifyFile(%v)"+
			" has allocSpaceOnVerifyFile to (%v)", s.partitionID, currExtentID, prevAllocSpaceExtentID, endAllocSpaceExtentID,
			prevAllocSpaceExtentID*util.BlockHeaderSize+size)
	}

	return
}

func (s *ExtentStore) GetPersistenceBaseExtentID() (extentID uint64, err error) {
	data := make([]byte, 8)
	_, err = s.metadataFp.ReadAt(data, 0)
	if err != nil {
		return
	}
	extentID = binary.BigEndian.Uint64(data)
	return
}

func (s *ExtentStore) PersistenceHasDeleteExtent(extentID uint64) (err error) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, extentID)
	if _, err = s.normalExtentDeleteFp.Write(data); err != nil {
		return
	}
	return
}

func (s *ExtentStore) GetHasDeleteExtent() (extentDes []ExtentDeleted, err error) {
	data := make([]byte, 8)
	offset := int64(0)
	for {
		_, err = s.normalExtentDeleteFp.ReadAt(data, offset)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}

		extent := ExtentDeleted{}
		extent.ExtentID = binary.BigEndian.Uint64(data)
		extentDes = append(extentDes, extent)
		offset += 8
	}

	return
}
