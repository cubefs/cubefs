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
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

const (
	ExtentOpenOpt  = os.O_CREATE | os.O_RDWR | os.O_EXCL
	ExtentHasClose = -1
	SEEK_DATA      = 3
	SEEK_HOLE      = 4
)

// Extent is an implementation of Extent for local regular extent file data management.
// This extent implementation manages all header info and data body in one single entry file.
// Header of extent include inode value of this extent block and Crc blocks of data blocks.
type Extent struct {
	file       *os.File
	filePath   string
	extentID   uint64
	modifyTime int64
	dataSize   int64
	modified   int32
	header     []byte
	sync.Mutex

	ioInterceptor IOInterceptor
}

// NewExtent create and returns a new extent instance.
func NewExtent(name string, extentID uint64, ioi IOInterceptor) *Extent {
	e := new(Extent)
	e.extentID = extentID
	e.filePath = name
	e.ioInterceptor = ioi
	return e
}

// Close this extent and release FD.
func (e *Extent) Close(sync bool) (err error) {
	if sync {
		if atomic.CompareAndSwapInt32(&e.modified, 1, 0) {
			if err = e.file.Sync(); err != nil {
				return
			}
		}
	}
	if err = e.file.Close(); err != nil {
		return
	}
	return
}

func (e *Extent) Exist() (exist bool) {
	_, err := os.Stat(e.filePath)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// InitToFS init extent data info filesystem. If entry file exist and overwrite is true,
// this operation will clear all data of exist entry file and initialize extent header data.
func (e *Extent) InitToFS() (err error) {
	if e.file, err = os.OpenFile(e.filePath, ExtentOpenOpt, 0666); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			e.file.Close()
		}
	}()
	if proto.IsTinyExtent(e.extentID) {
		e.dataSize = 0
		return
	}
	atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
	e.dataSize = 0
	return
}

// RestoreFromFS restores the entity data and status from the file stored on the filesystem.
func (e *Extent) RestoreFromFS() (err error) {
	if e.file, err = os.OpenFile(e.filePath, os.O_RDWR, 0666); err != nil {
		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			err = proto.ExtentNotFoundError
		}
		return err
	}
	var (
		info os.FileInfo
	)
	if info, err = e.file.Stat(); err != nil {
		err = fmt.Errorf("stat file %v: %v", e.file.Name(), err)
		return
	}
	if proto.IsTinyExtent(e.extentID) {
		watermark := info.Size()
		if watermark%PageSize != 0 {
			watermark = watermark + (PageSize - watermark%PageSize)
		}
		e.dataSize = watermark
		return
	}
	e.dataSize = info.Size()
	atomic.StoreInt64(&e.modifyTime, info.ModTime().Unix())
	return
}

// Size returns length of the extent (not including the header).
func (e *Extent) Size() (size int64) {
	return e.dataSize
}

// ModifyTime returns the time when this extent was modified recently.
func (e *Extent) ModifyTime() int64 {
	return atomic.LoadInt64(&e.modifyTime)
}

func IsRandomWrite(writeType int) bool {
	return writeType == RandomWriteType
}

func IsAppendWrite(writeType int) bool {
	return writeType == AppendWriteType
}

// WriteTiny performs write on a tiny extent.
func (e *Extent) WriteTiny(data []byte, offset, size int64, crc uint32, writeType int, isSync bool) (err error) {
	e.Lock()
	defer e.Unlock()
	index := offset + size

	if IsAppendWrite(writeType) && offset != e.dataSize {
		return ParameterMismatchError
	}

	var doIO = func() {
		_, err = e.file.WriteAt(data[:size], int64(offset))
	}
	e.ioInterceptor.intercept(IOWrite, doIO)
	if err != nil {
		return
	}
	if isSync {
		if err = e.file.Sync(); err != nil {
			return
		}
	}

	if !IsAppendWrite(writeType) {
		return
	}
	if index%PageSize != 0 {
		index = index + (PageSize - index%PageSize)
	}
	e.dataSize = index

	return
}

// Write writes data to an extent.
func (e *Extent) Write(data []byte, offset, size int64, crc uint32, writeType int, isSync bool, crcFunc UpdateCrcFunc, ei *ExtentInfoBlock) (err error) {
	defer func() {
		if err == nil {
			atomic.StoreInt32(&e.modified, 1)
		}
	}()
	if proto.IsTinyExtent(e.extentID) {
		err = e.WriteTiny(data, offset, size, crc, writeType, isSync)
		return
	}

	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if err = e.checkWriteParameter(offset, size, writeType); err != nil {
		return
	}
	var doIO = func() {
		_, err = e.file.WriteAt(data[:size], int64(offset))
	}
	e.ioInterceptor.intercept(IOWrite, doIO)
	if err != nil {
		return
	}
	blockNo := offset / unit.BlockSize
	offsetInBlock := offset % unit.BlockSize
	defer func() {
		if IsAppendWrite(writeType) {
			atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
			e.dataSize = int64(math.Max(float64(e.dataSize), float64(offset+size)))
		}
		if IsRandomWrite(writeType) && offset+size > e.dataSize {
			e.dataSize = int64(math.Max(float64(e.dataSize), float64(offset+size)))
		}
	}()
	if isSync {
		if err = e.file.Sync(); err != nil {
			return
		}
	}
	if offsetInBlock == 0 && size == unit.BlockSize {
		err = crcFunc(e, int(blockNo), crc)
		return
	}
	if offsetInBlock+size <= unit.BlockSize {
		err = crcFunc(e, int(blockNo), 0)
		return
	}
	if err = crcFunc(e, int(blockNo), 0); err == nil {
		err = crcFunc(e, int(blockNo+1), 0)
	}

	return
}

// Read reads data from an extent.
func (e *Extent) Read(data []byte, offset, size int64, isRepairRead bool) (crc uint32, err error) {
	if proto.IsTinyExtent(e.extentID) {
		return e.ReadTiny(data, offset, size, isRepairRead)
	}
	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	var doIO = func() {
		_, err = e.file.ReadAt(data[:size], offset)
	}
	e.ioInterceptor.intercept(IORead, doIO)
	if err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data)
	return
}

// ReadTiny read data from a tiny extent.
func (e *Extent) ReadTiny(data []byte, offset, size int64, isRepairRead bool) (crc uint32, err error) {
	var doIO = func() {
		_, err = e.file.ReadAt(data[:size], offset)
	}
	e.ioInterceptor.intercept(IORead, doIO)
	if isRepairRead && err == io.EOF {
		err = nil
	}
	crc = crc32.ChecksumIEEE(data[:size])

	return
}

func (e *Extent) checkOffsetAndSize(offset, size int64) error {
	if offset+size > unit.BlockSize*unit.BlockCount {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= unit.BlockCount*unit.BlockSize || size == 0 {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	//if size > unit.BlockSize {
	//	return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	//}
	return nil
}

const (
	IllegalOverWriteError = "illegal overwrite"
)

func (e *Extent) checkWriteParameter(offset, size int64, writeType int) error {
	if IsAppendWrite(writeType) && offset != e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("illegal append: offset=%v size=%v extentsize=%v", offset, size, e.dataSize))
	}
	if IsRandomWrite(writeType) && offset > e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("%v: offset=%v size=%v extentsize=%v", IllegalOverWriteError, offset, size, e.dataSize))
	}
	return nil
}

// Flush synchronizes data to the disk.
func (e *Extent) Flush() (err error) {
	if atomic.CompareAndSwapInt32(&e.modified, 1, 0) {
		err = e.file.Sync()
	}
	return
}

func (e *Extent) autoComputeExtentCrc(crcFunc UpdateCrcFunc) (crc uint32, err error) {
	var blockCnt int
	blockCnt = int(e.Size() / unit.BlockSize)
	if e.Size()%unit.BlockSize != 0 {
		blockCnt += 1
	}
	crcData := make([]byte, blockCnt*unit.PerBlockCrcSize)
	for blockNo := 0; blockNo < blockCnt; blockNo++ {
		blockCrc := binary.BigEndian.Uint32(e.header[blockNo*unit.PerBlockCrcSize : (blockNo+1)*unit.PerBlockCrcSize])
		if blockCrc != 0 {
			binary.BigEndian.PutUint32(crcData[blockNo*unit.PerBlockCrcSize:(blockNo+1)*unit.PerBlockCrcSize], blockCrc)
			continue
		}
		bdata := make([]byte, unit.BlockSize)
		offset := int64(blockNo * unit.BlockSize)
		var readN int
		readN, err = e.file.ReadAt(bdata[:unit.BlockSize], offset)
		if readN == 0 && err != nil {
			break
		}
		blockCrc = crc32.ChecksumIEEE(bdata[:readN])
		err = crcFunc(e, blockNo, blockCrc)
		if err != nil {
			return
		}
		binary.BigEndian.PutUint32(crcData[blockNo*unit.PerBlockCrcSize:(blockNo+1)*unit.PerBlockCrcSize], blockCrc)
	}
	crc = crc32.ChecksumIEEE(crcData)

	return
}

const (
	PageSize          = 4 * unit.KB
	FallocFLKeepSize  = 1
	FallocFLPunchHole = 2
)

// DeleteTiny deletes a tiny extent.
func (e *Extent) DeleteTiny(offset, size int64) (hasDelete bool, err error) {
	if int(offset)%PageSize != 0 {
		return false, ParameterMismatchError
	}

	if int(size)%PageSize != 0 {
		size += int64(PageSize - int(size)%PageSize)
	}
	if int(size)%PageSize != 0 {
		return false, ParameterMismatchError
	}

	newOffset, err := e.file.Seek(offset, SEEK_DATA)
	if err != nil {
		if strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			return true, nil
		}
		return false, err
	}
	if newOffset-offset >= size {
		hasDelete = true
		return true, nil
	}
	err = fallocate(int(e.file.Fd()), FallocFLPunchHole|FallocFLKeepSize, offset, size)
	return
}

func (e *Extent) getRealBlockCnt() (blockNum int64) {
	stat := new(syscall.Stat_t)
	syscall.Stat(e.filePath, stat)
	return stat.Blocks
}

func (e *Extent) TinyExtentRecover(data []byte, offset, size int64, crc uint32, isEmptyPacket bool) (err error) {
	e.Lock()
	defer e.Unlock()
	if !proto.IsTinyExtent(e.extentID) {
		return ParameterMismatchError
	}
	if offset%PageSize != 0 || offset != e.dataSize {
		return fmt.Errorf("error empty packet on (%v) offset(%v) size(%v)"+
			" isEmptyPacket(%v)  e.dataSize(%v)", e.file.Name(), offset, size, isEmptyPacket, e.dataSize)
	}
	log.LogDebugf("before file (%v) getRealBlockNo (%v) isEmptyPacket(%v)"+
		"offset(%v) size(%v) e.datasize(%v)", e.filePath, e.getRealBlockCnt(), isEmptyPacket, offset, size, e.dataSize)
	if isEmptyPacket {
		finfo, err := e.file.Stat()
		if err != nil {
			return err
		}
		if offset < finfo.Size() {
			return fmt.Errorf("error empty packet on (%v) offset(%v) size(%v)"+
				" isEmptyPacket(%v) filesize(%v) e.dataSize(%v)", e.file.Name(), offset, size, isEmptyPacket, finfo.Size(), e.dataSize)
		}
		if err = syscall.Ftruncate(int(e.file.Fd()), offset+size); err != nil {
			return err
		}
		err = fallocate(int(e.file.Fd()), FallocFLPunchHole|FallocFLKeepSize, offset, size)
	} else {
		_, err = e.file.WriteAt(data[:size], int64(offset))
	}
	if err != nil {
		return
	}
	watermark := offset + size
	if watermark%PageSize != 0 {
		watermark = watermark + (PageSize - watermark%PageSize)
	}
	e.dataSize = watermark
	log.LogDebugf("after file (%v) getRealBlockNo (%v) isEmptyPacket(%v)"+
		"offset(%v) size(%v) e.datasize(%v)", e.filePath, e.getRealBlockCnt(), isEmptyPacket, offset, size, e.dataSize)

	return
}

func (e *Extent) tinyExtentAvaliOffset(offset int64) (newOffset, newEnd int64, err error) {
	e.Lock()
	defer e.Unlock()
	newOffset, err = e.file.Seek(int64(offset), SEEK_DATA)
	if err != nil {
		return
	}
	newEnd, err = e.file.Seek(int64(newOffset), SEEK_HOLE)
	if err != nil {
		return
	}
	//if newOffset-offset > unit.BlockSize {
	//	newOffset = offset + unit.BlockSize
	//}
	//if newEnd-newOffset > unit.BlockSize {
	//	newEnd = newOffset + unit.BlockSize
	//}
	if newEnd < newOffset {
		err = fmt.Errorf("unavali TinyExtentAvaliOffset on SEEK_DATA or SEEK_HOLE   (%v) offset(%v) "+
			"newEnd(%v) newOffset(%v)", e.extentID, offset, newEnd, newOffset)
	}
	return
}

func (e *Extent) tinyExtentAvaliAndHoleOffset(offset int64) (dataOffset, holeOffset int64, err error) {
	e.Lock()
	defer e.Unlock()
	dataOffset, err = e.file.Seek(offset, SEEK_DATA)
	if err != nil {
		return
	}
	holeOffset, err = e.file.Seek(offset, SEEK_HOLE)
	if err != nil {
		return
	}
	return
}
