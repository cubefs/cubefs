// Copyright 2018 The Containerfs Authors.
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
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/buf"
)

const (
	ExtentOpenOpt          = os.O_CREATE | os.O_RDWR | os.O_EXCL
	ExtentOpenOptOverwrite = os.O_CREATE | os.O_RDWR
)

var (
	ErrBrokenExtentFile = errors.New("broken extent file error")
)

type ExtentInfo struct {
	FileID  uint64    `json:"fileId"`
	Inode   uint64    `json:"ino"`
	Size    uint64    `json:"size"`
	Crc     uint32    `json:"crc"`
	Deleted bool      `json:"deleted"`
	ModTime time.Time `json:"modTime"`
	Source  string    `json:"src"`
}

func (ei *ExtentInfo) FromExtent(extent *Extent) {
	if extent != nil {
		ei.FileID = extent.ID()
		ei.Inode = extent.Ino()
		ei.Size = uint64(extent.Size())
		if !IsTinyExtent(ei.FileID) {
			ei.Crc = extent.HeaderChecksum()
			ei.Deleted = extent.IsMarkDelete()
			ei.ModTime = extent.ModTime()
		}
	}
}

func (ei *ExtentInfo) String() (m string) {
	source := ei.Source
	if source == "" {
		source = "none"
	}
	return fmt.Sprintf("%v_%v_%v_%v_%v_%v", ei.FileID, ei.Inode, ei.Size, ei.Crc, ei.Deleted, source)
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
	closeC     chan bool
	closed     bool
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
	e.closed = true
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
func (e *Extent) InitToFS(ino uint64, overwrite bool) (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	opt := ExtentOpenOpt
	if overwrite {
		opt = ExtentOpenOptOverwrite
	}

	if e.file, err = os.OpenFile(e.filePath, opt, 0666); err != nil {
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
	if err = e.file.Truncate(util.BlockHeaderSize); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(e.header[:8], ino)
	if _, err = e.file.WriteAt(e.header[:8], 0); err != nil {
		return err
	}
	emptyCrc := crc32.ChecksumIEEE(make([]byte, util.BlockSize))
	for blockNo := 0; blockNo < util.BlockCount; blockNo++ {
		if err = e.updateBlockCrc(blockNo, emptyCrc); err != nil {
			return err
		}
	}
	if err = e.file.Sync(); err != nil {
		return err
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

// RestoreFromFS restore entity data and status from entry file stored in filesystem.
func (e *Extent) RestoreFromFS(loadHeader bool) (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.file, err = os.OpenFile(e.filePath, os.O_RDWR, 0666); err != nil {
		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			err = ErrorExtentNotFound
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
		e.dataSize = info.Size()
		return
	}
	if info.Size() < util.BlockHeaderSize {
		err = ErrBrokenExtentFile
		return
	}
	if loadHeader {
		if _, err = e.file.ReadAt(e.header, 0); err != nil {
			err = fmt.Errorf("read file %v offset %v: %v", e.file.Name(), 0, err)
			return
		}
	}

	e.dataSize = info.Size() - util.BlockHeaderSize
	e.modifyTime = info.ModTime()
	return
}

// MarkDelete mark this extent as deleted.
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

// IsMarkDelete test this extent if has been marked as delete.
func (e *Extent) IsMarkDelete() bool {
	if IsTinyExtent(e.extentID) {
		return false
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.header[util.MarkDeleteIndex] == util.MarkDelete
}

// Size returns length of extent data exclude header.
func (e *Extent) Size() (size int64) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	size = e.dataSize
	return
}

// ModTime returns the time when this extent was last modified.
func (e *Extent) ModTime() time.Time {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.modifyTime
}

func (e *Extent) WriteTiny(data []byte, offset, size int64, crc uint32) (err error) {
	if offset+size >= math.MaxUint32 {
		return ErrorExtentHasFull
	}
	if _, err = e.file.WriteAt(data[:size], int64(offset)); err != nil {
		return
	}
	e.dataSize = offset + size
	watermark := e.dataSize
	if watermark%PageSize != 0 {
		watermark = watermark + (PageSize - watermark%PageSize)
	}
	e.dataSize = watermark

	return
}

func (e *Extent) TinyRecover(data []byte, offset, size int64, crc uint32) (err error) {
	if !IsTinyExtent(e.extentID) {
		return ErrorUnavaliExtent
	}
	if offset+size >= math.MaxUint32 {
		return ErrorExtentHasFull
	}
	if _, err = e.file.WriteAt(data[:size], int64(offset)); err != nil {
		return
	}
	e.dataSize = offset + size

	return
}

// Write data to extent.
func (e *Extent) Write(data []byte, offset, size int64, crc uint32) (err error) {
	if IsTinyExtent(e.extentID) {
		return e.WriteTiny(data, offset, size, crc)
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	var (
		writeSize int
	)
	if writeSize, err = e.file.WriteAt(data[:size], int64(offset+util.BlockHeaderSize)); err != nil {
		return
	}
	blockNo := offset / util.BlockSize
	offsetInBlock := offset % util.BlockSize
	e.dataSize = int64(math.Max(float64(e.dataSize), float64(offset+size)))
	e.modifyTime = time.Now()
	if offsetInBlock == 0 && size == util.BlockSize {
		return e.updateBlockCrc(int(blockNo), crc)
	}

	// Prepare read buffer for block data
	var (
		blockBuffer []byte
		poolErr     error
	)
	if blockBuffer, poolErr = buf.Buffers.Get(util.BlockSize); poolErr != nil {
		blockBuffer = make([]byte, util.BlockSize)
	}
	defer buf.Buffers.Put(blockBuffer)

	remainCheckByteCnt := offsetInBlock + int64(writeSize)
	for {
		if remainCheckByteCnt <= 0 {
			break
		}
		readN, readErr := e.file.ReadAt(blockBuffer, int64(blockNo*util.BlockSize+util.BlockHeaderSize))
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			err = readErr
			return
		}
		if readN == 0 {
			break
		}
		crc = crc32.ChecksumIEEE(blockBuffer[:readN])
		if err = e.updateBlockCrc(int(blockNo), crc); err != nil {
			return
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF || readN < util.BlockSize {
			break
		}
		remainCheckByteCnt -= int64(readN)
		blockNo++
	}
	return
}

// Read data from extent.
func (e *Extent) Read(data []byte, offset, size int64) (crc uint32, err error) {
	if IsTinyExtent(e.extentID) {
		return e.ReadTiny(data, offset, size)
	}
	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	if _, err = e.file.ReadAt(data[:size], offset+util.BlockHeaderSize); err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data)
	return
}

func (e *Extent) ReadTiny(data []byte, offset, size int64) (crc uint32, err error) {
	if _, err = e.file.ReadAt(data[:size], offset); err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data[:size])

	return
}

func (e *Extent) updateBlockCrc(blockNo int, crc uint32) (err error) {
	startIdx := util.BlockHeaderCrcIndex + blockNo*util.PerBlockCrcSize
	endIdx := startIdx + util.PerBlockCrcSize
	binary.BigEndian.PutUint32(e.header[startIdx:endIdx], crc)
	if _, err = e.file.WriteAt(e.header[startIdx:endIdx], int64(startIdx)); err != nil {
		return
	}
	e.modifyTime = time.Now()

	return
}

func (e *Extent) checkOffsetAndSize(offset, size int64) error {
	if offset+size > util.BlockSize*util.BlockCount {
		return NewParamMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= util.BlockCount*util.BlockSize || size == 0 {
		return NewParamMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}

	if size > util.BlockSize {
		return NewParamMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	return nil
}

// Flush synchronize data to disk immediately.
func (e *Extent) Flush() (err error) {
	err = e.file.Sync()
	return
}

// HeaderChecksum returns crc checksum value of extent header data
// include inode data and block crc.
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

const (
	PageSize          = 4 * util.KB
	FallocFLKeepSize  = 1
	FallocFLPunchHole = 2
)

func (e *Extent) DeleteTiny(offset, size int64) (err error) {
	if int(offset)%PageSize != 0 {
		return ErrorParamMismatch
	}

	if int(size)%PageSize != 0 {
		size += int64(PageSize - int(size)%PageSize)
	}
	if int(size)%PageSize != 0 {
		return ErrorParamMismatch
	}
	err = syscall.Fallocate(int(e.file.Fd()), FallocFLPunchHole|FallocFLKeepSize, offset, size)

	return
}
