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

package storage

import (
	"encoding/binary"
	"io"
	"os"
	"path"
	"strconv"
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
	log.LogDebugf("PersistenceBlockCrc. extent id %v blockNo %v blockCrc %v data path %v", e.extentID, blockNo, blockCrc, s.dataPath)
	if !proto.IsNormalDp(s.partitionType) {
		return
	}

	if blockNo >= len(e.header)/util.PerBlockCrcSize {
		exp := make([]byte, util.BlockHeaderSize*(1+(blockNo*util.PerBlockCrcSize-len(e.header))/util.BlockHeaderSize))
		e.header = append(e.header, exp...)
	}
	startIdx := blockNo * util.PerBlockCrcSize % util.BlockHeaderSize
	endIdx := startIdx + util.PerBlockCrcSize%util.BlockHeaderSize
	fIdx := blockNo * util.PerBlockCrcSize / util.BlockHeaderSize
	log.LogDebugf("PersistenceBlockCrc. idx %v startIdx %v endIdx %v", fIdx, startIdx, endIdx)
	fp := s.verifyExtentFp
	if fIdx > 0 {
		gap := fIdx - len(s.verifyExtentFpAppend)
		log.LogDebugf("PersistenceBlockCrc. idx %v gap %v", fIdx, gap)
		if gap > 0 {
			appendFpArr := make([]*os.File, fIdx-len(s.verifyExtentFpAppend))
			s.verifyExtentFpAppend = append(s.verifyExtentFpAppend, appendFpArr...)

			for i := gap; i > 0; i-- {
				suffix := fIdx - i
				dataPath := path.Join(s.dataPath, ExtCrcHeaderFileName+"_"+strconv.Itoa(suffix))
				log.LogDebugf("PersistenceBlockCrc. idx %v try create path %v", fIdx-1, dataPath)
				if fp, err = os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0666); err != nil {
					log.LogDebugf("PersistenceBlockCrc. idx %v try create path %v err %v", fIdx, dataPath, err)
					return
				}
				log.LogDebugf("PersistenceBlockCrc. idx %v try create path %v success", fIdx, dataPath)
				s.verifyExtentFpAppend[suffix] = fp
				s.PreAllocSpaceOnVerfiyFileForAppend(suffix)
			}
		}
		if s.verifyExtentFpAppend[fIdx-1] == nil {
			dataPath := path.Join(s.dataPath, ExtCrcHeaderFileName+"_"+strconv.Itoa(fIdx-1))
			if fp, err = os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0666); err != nil {
				return
			}
			s.verifyExtentFpAppend[fIdx-1] = fp
		}
		fp = s.verifyExtentFpAppend[fIdx-1]
	}

	binary.BigEndian.PutUint32(e.header[startIdx:endIdx], blockCrc)
	verifyStart := startIdx + int(util.BlockHeaderSize*e.extentID)
	log.LogDebugf("PersistenceBlockCrc. dp %v write at start %v end %v name %v", s.partitionID, startIdx, endIdx, fp.Name())
	if _, err = fp.WriteAt(e.header[startIdx:endIdx], int64(verifyStart)); err != nil {
		return
	}

	return
}

func (s *ExtentStore) DeleteBlockCrc(extentID uint64) (err error) {
	if !proto.IsNormalDp(s.partitionType) {
		return
	}

	if err = fallocate(int(s.verifyExtentFp.Fd()), util.FallocFLPunchHole|util.FallocFLKeepSize,
		int64(util.BlockHeaderSize*extentID), util.BlockHeaderSize); err != nil {
		return
	}

	for idx, fp := range s.verifyExtentFpAppend {
		if fp == nil {
			log.LogErrorf("DeleteBlockCrc. idx %v append fp is nil", idx)
			return
		}
		log.LogDebugf("DeleteBlockCrc. dp %v idx %v extentID %v offset %v", s.partitionID, idx, extentID, int64(util.BlockHeaderSize*extentID))
		if err = fallocate(int(fp.Fd()), util.FallocFLPunchHole|util.FallocFLKeepSize,
			int64(util.BlockHeaderSize*extentID), util.BlockHeaderSize); err != nil {
			return
		}
	}

	return
}

func (s *ExtentStore) PersistenceBaseExtentID(extentID uint64) (err error) {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, extentID)
	_, err = s.metadataFp.WriteAt(value, BaseExtentIDOffset)
	return
}

func (s *ExtentStore) GetPreAllocSpaceExtentIDOnVerifyFile() (extentID uint64) {
	value := make([]byte, 8)
	_, err := s.metadataFp.ReadAt(value, 8)
	if err != nil {
		return
	}
	extentID = binary.BigEndian.Uint64(value)
	return
}

func (s *ExtentStore) PreAllocSpaceOnVerfiyFileForAppend(idx int) {
	if !proto.IsNormalDp(s.partitionType) {
		return
	}
	log.LogDebugf("PreAllocSpaceOnVerfiyFileForAppend. idx %v end %v", idx, len(s.verifyExtentFpAppend))
	if idx >= len(s.verifyExtentFpAppend) {
		log.LogErrorf("PreAllocSpaceOnVerfiyFileForAppend. idx %v end %v", idx, len(s.verifyExtentFpAppend))
		return
	}
	prevAllocSpaceExtentID := int64(atomic.LoadUint64(&s.hasAllocSpaceExtentIDOnVerfiyFile))

	log.LogDebugf("PreAllocSpaceOnVerfiyFileForAppend. idx %v size %v", idx, prevAllocSpaceExtentID*util.BlockHeaderSize)
	err := fallocate(int(s.verifyExtentFpAppend[idx].Fd()), 1, 0, prevAllocSpaceExtentID*util.BlockHeaderSize)
	if err != nil {
		log.LogErrorf("PreAllocSpaceOnVerfiyFileForAppend. idx %v size %v err %v", idx, prevAllocSpaceExtentID*util.BlockHeaderSize, err)
		return
	}
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

		for id, fp := range s.verifyExtentFpAppend {
			stat, _ := fp.Stat()
			log.LogDebugf("PreAllocSpaceOnVerfiyFile. id %v name %v size %v", id, fp.Name(), stat.Size())
			err = fallocate(int(fp.Fd()), 1, prevAllocSpaceExtentID*util.BlockHeaderSize, size)
			if err != nil {
				log.LogErrorf("PreAllocSpaceOnVerfiyFile. id %v name %v err %v", id, fp.Name(), err)
				return
			}
		}

		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(endAllocSpaceExtentID))
		if _, err = s.metadataFp.WriteAt(data, 8); err != nil {
			return
		}
		atomic.StoreUint64(&s.hasAllocSpaceExtentIDOnVerfiyFile, uint64(endAllocSpaceExtentID))
		log.LogInfof("Action(PreAllocSpaceOnVerifyFile) PartitionID(%v) currentExtent(%v)"+
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

}
