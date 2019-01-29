// Copyright 2018 The Container File System Authors.
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
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/buf"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	ExtentOpenOpt          = os.O_CREATE | os.O_RDWR | os.O_EXCL
	ExtentOpenOptOverwrite = os.O_CREATE | os.O_RDWR
)

type ExtentInfo struct {
	FileID     uint64    `json:"fileId"`
	Inode      uint64    `json:"ino"`
	Size       uint64    `json:"size"`
	Crc        uint32    `json:"crc"`
	IsDeleted  bool      `json:"deleted"`
	ModifyTime time.Time `json:"modTime"`
	Source     string    `json:"src"`
}

func (ei *ExtentInfo) FromExtent(extent *Extent, isDirtyBlock bool) {
	if extent != nil {
		ei.FileID = extent.ID()
		ei.Inode = extent.Ino()
		ei.Size = uint64(extent.Size())
		if !IsTinyExtent(ei.FileID) {
			if isDirtyBlock {
				ei.Crc = 0
			} else {
				ei.Crc = extent.HeaderChecksum()
			}
			ei.IsDeleted = extent.HasBeenMarkedAsDeleted()
			ei.ModifyTime = extent.ModifyTime()
		}
	}
}

func (ei *ExtentInfo) String() (m string) {
	source := ei.Source
	if source == "" {
		source = "none"
	}
	return fmt.Sprintf("%v_%v_%v_%v_%v_%v", ei.FileID, ei.Inode, ei.Size, ei.Crc, ei.IsDeleted, source)
}

// Extent is an implementation of Extent for local regular extent file data management.
// This extent implementation manages all header info and data body in one single entry file.
// Header of extent include inode value of this extent block and crc blocks of data blocks.
type Extent struct {
	file       *os.File
	filePath   string
	extentID   uint64
	lock       sync.RWMutex
	header     []byte
	modifyTime time.Time
	dataSize   int64
}

// NewExtentInCore create and returns a new extent instance.
func NewExtentInCore(name string, extentID uint64) *Extent {
	e := new(Extent)
	e.extentID = extentID
	e.filePath = name
	e.header = make([]byte, util.BlockHeaderSize)

	return e
}

// Close this extent and release FD.
func (e *Extent) Close() (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if err = e.file.Close(); err != nil {
		return
	}
	return
}

// Ino returns this inode ID of this extent block belong to.
func (e *Extent) Ino() (ino uint64) {
	ino = binary.BigEndian.Uint64(e.header[:util.BlockHeaderInoSize])
	return
}

// ID returns the identity value (extentID) of this extent entity.
func (e *Extent) ID() uint64 {
	return e.extentID
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
func (e *Extent) InitToFS(ino uint64) (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
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
	var (
		fileInfo os.FileInfo
	)
	if fileInfo, err = e.file.Stat(); err != nil {
		return err
	}
	e.modifyTime = fileInfo.ModTime()
	e.dataSize = 0
	return
}

// RestoreFromFS restores the entity data and status from the file stored on the filesystem.
func (e *Extent) RestoreFromFS() (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
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
	e.modifyTime = info.ModTime()
	return
}

// MarkDelete marks the extent as deleted.
func (e *Extent) MarkDelete() (err error) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	e.header[util.MarkDeleteIndex] = util.MarkDelete
	if _, err = e.file.WriteAt(e.header, 0); err != nil {
		return
	}
	e.modifyTime = time.Now()
	return
}

// HasBeenMarkedAsDeleted returns if the extent has been marked as deleted.
func (e *Extent) HasBeenMarkedAsDeleted() bool {
	if IsTinyExtent(e.extentID) {
		return false
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.header[util.MarkDeleteIndex] == util.MarkDelete
}

// Size returns length of the extent (not including the header).
func (e *Extent) Size() (size int64) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	size = e.dataSize
	return
}

// ModifyTime returns the time when this extent was modified recently.
func (e *Extent) ModifyTime() time.Time {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.modifyTime
}

// WriteTiny performs write on a tiny extent.
func (e *Extent) WriteTiny(data []byte, offset, size int64, crc uint32, isUpdateSize bool) (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	index := offset + size
	if index >= math.MaxUint32 {
		return ExtentIsFullError
	}

	if _, err = e.file.WriteAt(data[:size], int64(offset)); err != nil {
		return
	}
	if !isUpdateSize {
		return
	}
	if index%PageSize != 0 {
		index = index + (PageSize - index%PageSize)
	}
	e.dataSize = index

	return
}

type UpdateCrcFunc func(updateExtentID uint64, updateblockNo int, updateCrc uint32, updateE *Extent, isDirtyBlock bool) error

// Write writes data to an extent.
func (e *Extent) Write(data []byte, offset, size int64, crc uint32, crcFunc UpdateCrcFunc, isUpdateSize bool) (isDirtyBlock bool, err error) {
	if IsTinyExtent(e.extentID) {
		err = e.WriteTiny(data, offset, size, crc, isUpdateSize)
		return
	}

	e.lock.RLock()
	defer e.lock.RUnlock()
	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if _, err = e.file.WriteAt(data[:size], int64(offset)); err != nil {
		return
	}
	blockNo := offset / util.BlockSize
	offsetInBlock := offset % util.BlockSize
	e.dataSize = int64(math.Max(float64(e.dataSize), float64(offset+size)))
	e.modifyTime = time.Now()
	if offsetInBlock == 0 && size == util.BlockSize {
		err = crcFunc(e.extentID, int(blockNo), crc, e, isDirtyBlock)
		return
	} else {
		isDirtyBlock = true
		err = crcFunc(e.extentID, int(blockNo), crc, e, isDirtyBlock)
	}

	return
}

// Read reads data from an extent.
func (e *Extent) Read(data []byte, offset, size int64, isRepairRead bool) (crc uint32, err error) {
	if IsTinyExtent(e.extentID) {
		return e.ReadTiny(data, offset, size, isRepairRead)
	}
	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	if _, err = e.file.ReadAt(data[:size], offset); err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data)
	return
}

// ReadTiny read data from a tiny extent.
func (e *Extent) ReadTiny(data []byte, offset, size int64, isRepairRead bool) (crc uint32, err error) {
	_, err = e.file.ReadAt(data[:size], offset)
	if isRepairRead && err == io.EOF {
		err = nil
	}
	crc = crc32.ChecksumIEEE(data[:size])

	return
}

func (e *Extent) checkOffsetAndSize(offset, size int64) error {
	if offset+size > util.BlockSize*util.BlockCount {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= util.BlockCount*util.BlockSize || size == 0 {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}

	if size > util.BlockSize {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	return nil
}

// Flush synchronizes data to the disk.
func (e *Extent) Flush() (err error) {
	err = e.file.Sync()
	return
}

// HeaderChecksum returns the crc checksum of the extent header, which includes the inode data and the block crc.
func (e *Extent) HeaderChecksum() (crc uint32) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	blockNum := e.dataSize / util.BlockSize
	if e.dataSize%util.BlockSize != 0 {
		blockNum = blockNum + 1
	}
	crc = crc32.ChecksumIEEE(e.header[util.BlockHeaderCrcIndex : util.BlockHeaderCrcIndex+blockNum*util.PerBlockCrcSize])
	return
}

func (e *Extent) checkDirtyBlock() (dirtyBlocks []int) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	dirtyBlocks = make([]int, 0)
	for index := 0; index < util.BlockCount; index++ {
		if e.header[index*util.PerBlockCrcSize] == util.DirtyBlock {
			dirtyBlocks = append(dirtyBlocks, index)
		}
	}
	return
}

func (e *Extent) autoFixDirtyBlock(crcFunc UpdateCrcFunc) {
	dirtyBlocks := e.checkDirtyBlock()
	if len(dirtyBlocks) == 0 {
		return
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	for _, dirtyBlockNo := range dirtyBlocks {
		data := make([]byte, util.BlockSize)
		offset := int64(dirtyBlockNo * util.BlockSize)
		readN,err:= e.file.ReadAt(data[:util.BlockSize], offset)
		if err!=io.EOF{
			continue
		}
		crc := crc32.ChecksumIEEE(data[:readN])
		if err = crcFunc(e.extentID, dirtyBlockNo, crc, e, false); err != nil {
			continue
		}
	}
}

const (
	PageSize          = 4 * util.KB
	FallocFLKeepSize  = 1
	FallocFLPunchHole = 2
)

// DeleteTiny deletes a tiny extent.
func (e *Extent) DeleteTiny(offset, size int64) (err error) {
	if int(offset)%PageSize != 0 {
		return ParameterMismatchError
	}

	if int(size)%PageSize != 0 {
		size += int64(PageSize - int(size)%PageSize)
	}
	if int(size)%PageSize != 0 {
		return ParameterMismatchError
	}
	err = syscall.Fallocate(int(e.file.Fd()), FallocFLPunchHole|FallocFLKeepSize, offset, size)

	return
}
