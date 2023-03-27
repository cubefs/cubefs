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

package ecstorage

import (
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/unit"
)

const (
	ExtentOpenOpt  = os.O_CREATE | os.O_RDWR | os.O_EXCL
	ExtentHasClose = -1
	SEEK_DATA      = 3
	SEEK_HOLE      = 4
)

type ExtentInfo struct {
	FileID           uint64 `json:"fileId"`
	Size             uint64 `json:"size"`
	Crc              uint32 `json:"Crc"`
	IsDeleted        bool   `json:"deleted"`
	ModifyTime       int64  `json:"modTime"`
	Source           string `json:"src"`
	OriginExtentSize uint64 `json:"origin_extent_size"`
}

type CrcInfo struct {
	blockCrc  uint32
	blockSize uint32
}

func (ei *ExtentInfo) String() (m string) {
	source := ei.Source
	if source == "" {
		source = "none"
	}
	return fmt.Sprintf("%v_%v_%v_%v", ei.FileID, ei.Size, ei.IsDeleted, source)
}

// Extent is an implementation of Extent for local regular extent file data management.
// This extent implementation manages all header info and data body in one single entry file.
// Header of extent include inode value of this extent block and Crc blocks of data blocks.
type Extent struct {
	file       *os.File
	filePath   string
	extentID   uint64
	modifyTime int64
	dataSize   int64
	hasClose   int32
	sync.Mutex
}

// WriteEcTiny performs write on a tiny extent.
func (e *Extent) EcWriteTiny(data []byte, offset, size int64, writeType int, isSync bool) (err error) {
	e.Lock()
	defer e.Unlock()
	index := offset + size

	if IsAppendWrite(writeType) && offset != e.dataSize {
		return ParameterMismatchError
	}

	if _, err = e.file.WriteAt(data[:size], offset); err != nil {
		return
	}
	if isSync {
		if err = e.file.Sync(); err != nil {
			return
		}
	}
	atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
	if !IsAppendWrite(writeType) {
		return
	}
	if index%PageSize != 0 {
		index = index + (PageSize - index%PageSize)
	}
	e.dataSize = index
	return
}

// EcWrite writes data to an extent.
func (e *Extent) EcWrite(data []byte, offset, size int64, writeType int, isSync bool) (err error) {
	if IsTinyExtent(e.extentID) {
		err = e.EcWriteTiny(data, offset, size, writeType, isSync)
		return
	}

	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if err = e.checkEcWriteParameter(offset, size, writeType); err != nil {
		return
	}

	if _, err = e.file.WriteAt(data[:size], offset); err != nil {
		return
	}
	defer func() {
		if IsAppendWrite(writeType) {
			atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
			e.dataSize = int64(math.Max(float64(e.dataSize), float64(offset+size)))
		}
	}()
	if isSync {
		if err = e.file.Sync(); err != nil {
			return
		}
	}
	return
}

func (e *Extent) EcRead(data []byte, size, offset int64, isRepairRead bool) (dataSize int, crc uint32, err error) {
	if IsTinyExtent(e.extentID) {
		return e.EcReadTiny(data, size, offset, isRepairRead)
	}
	dataSize, err = e.file.ReadAt(data[:size], offset)
	if err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data[:size])
	return
}

func (e *Extent) ecTinyExtentAvaliOffset(offset int64) (newOffset, newEnd int64, err error) {
	e.Lock()
	defer e.Unlock()
	newOffset, err = e.file.Seek(offset, SEEK_DATA)
	if err != nil {
		return
	}
	newEnd, err = e.file.Seek(newOffset, SEEK_HOLE)
	if err != nil {
		return
	}
	if newOffset-offset > unit.EcBlockSize {
		newOffset = offset + unit.EcBlockSize
	}
	if newEnd-newOffset > unit.EcBlockSize {
		newEnd = newOffset + unit.EcBlockSize
	}
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

// ReadTiny read data from a tiny extent.
func (e *Extent) EcReadTiny(data []byte, size, offset int64, isRepairRead bool) (dataSize int, crc uint32, err error) {

	var (
		newOffset   int64
		newEnd      int64
		successSize int
	)
	needReadSize := size
	if !isRepairRead { //normalRead读取有效数据
		dataSize, err = e.file.ReadAt(data[:size], offset)
		crc = crc32.ChecksumIEEE(data[:dataSize])
		return
	}

	defer func() {
		crc = crc32.ChecksumIEEE(data[:size])
	}()

	for { //repair scrub degrade read need fill 0 to hole size
		if needReadSize <= 0 {
			break
		}
		newOffset, newEnd, err = e.ecTinyExtentAvaliOffset(offset)
		if err != nil {
			errStr := fmt.Sprintf("%v", err)
			if strings.Contains(errStr, "no such device or address") || err == io.EOF {
				err = nil
			}
			return
		}
		if newOffset > offset {
			holeSize := newOffset - offset
			needReadSize -= holeSize
			dataSize += int(holeSize)
			offset += holeSize
			continue
		}

		currReadSize := unit.Min(int(newEnd-offset), int(needReadSize))
		successSize, err = e.file.ReadAt(data[dataSize:dataSize+currReadSize], offset)
		if err != nil {
			dataSize += successSize
			err = errors.NewErrorf("read err[%v] needReadSize[%v] successSize[%v]", err, currReadSize, successSize)
			return
		}
		needReadSize -= int64(successSize)
		offset += int64(successSize)
		dataSize += successSize
	}
	return
}

func (e *Extent) checkEcWriteParameter(offset, size int64, writeType int) error {
	if IsAppendWrite(writeType) && offset != e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("illegal append: offset=%v size=%v extentsize=%v", offset, size, e.dataSize))
	}

	if IsRandomWrite(writeType) && offset+size > e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("illegal overwrite: offset=%v size=%v extentsize=%v", offset, size, e.dataSize))
	}
	return nil
}

// NewExtentInCore create and returns a new extent instance.
func NewExtentInCore(name string, extentID uint64) *Extent {
	e := new(Extent)
	e.extentID = extentID
	e.filePath = name

	return e
}

func (e *Extent) HasClosed() bool {
	return atomic.LoadInt32(&e.hasClose) == ExtentHasClose
}

// Close this extent and release FD.
func (e *Extent) Close() (err error) {
	if e.HasClosed() {
		return
	}
	if err = e.file.Close(); err != nil {
		return
	}
	return
}

func (e *Extent) Exist() (exsit bool) {
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
			os.Remove(e.filePath)
		}
	}()

	if IsTinyExtent(e.extentID) {
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
			err = ExtentNotFoundError
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
	if IsTinyExtent(e.extentID) {
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

// RestoreFromFS restores the entity data and status from the file stored on the filesystem.
func (e *Extent) getStatFromFS(info os.FileInfo) (err error) {
	if IsTinyExtent(e.extentID) {
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

	if _, err = e.file.WriteAt(data[:size], int64(offset)); err != nil {
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

func (e *Extent) checkOffsetAndSize(offset, size int64) error {
	if offset+size > unit.BlockSize*unit.BlockCount {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= unit.BlockCount*unit.BlockSize || size == 0 {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	return nil
}

func (e *Extent) checkWriteParameter(offset, size int64, writeType int) error {
	if IsAppendWrite(writeType) && offset != e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("illegal append: offset=%v size=%v extentsize=%v", offset, size, e.dataSize))
	}
	if IsRandomWrite(writeType) && offset+size > e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("illegal overwrite: offset=%v size=%v extentsize=%v", offset, size, e.dataSize))
	}
	return nil
}

// Flush synchronizes data to the disk.
func (e *Extent) Flush() (err error) {
	err = e.file.Sync()
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
	if !IsTinyExtent(e.extentID) {
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
	if newOffset-offset > unit.BlockSize {
		newOffset = offset + unit.BlockSize
	}
	if newEnd-newOffset > unit.BlockSize {
		newEnd = newOffset + unit.BlockSize
	}
	if newEnd < newOffset {
		err = fmt.Errorf("unavali TinyExtentAvaliOffset on SEEK_DATA or SEEK_HOLE   (%v) offset(%v) "+
			"newEnd(%v) newOffset(%v)", e.extentID, offset, newEnd, newOffset)
	}
	return
}
