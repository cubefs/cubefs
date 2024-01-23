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
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"golang.org/x/time/rate"
)

const (
	ExtentOpenOpt  = os.O_CREATE | os.O_RDWR | os.O_EXCL
	ExtentHasClose = -1
	SEEK_DATA      = 3
	SEEK_HOLE      = 4
)

type HeaderHandler interface {
	Load(b []byte, off, size int64) error
	Save(b []byte, off, size int64) error
}

type funcHeaderHandler struct {
	load func([]byte, int64, int64) error
	save func([]byte, int64, int64) error
}

func (f *funcHeaderHandler) Load(b []byte, off, size int64) error {
	if f != nil && f.load != nil {
		return f.load(b, off, size)
	}
	return nil
}

func (f *funcHeaderHandler) Save(b []byte, off, size int64) error {
	if f != nil && f.save != nil {
		return f.save(b, off, size)
	}
	return nil
}

func NewFuncHeaderHandler(load, save func([]byte, int64, int64) error) HeaderHandler {
	return &funcHeaderHandler{
		load: load,
		save: save,
	}
}

type Fingerprint []uint32

func (f *Fingerprint) Append(crc uint32) (index int) {
	index = len(*f)
	*f = append(*f, crc)
	return index
}

func (f *Fingerprint) Equals(other Fingerprint) bool {
	return reflect.DeepEqual(*f, other)
}

func (f *Fingerprint) Get(index int) (crc uint32, exists bool) {
	if index < len(*f) {
		crc = (*f)[index]
		exists = true
	}
	return
}

func (f *Fingerprint) FirstConflict(other Fingerprint) int {
	for i := 0; i < len(*f) && i < len(other); i++ {
		if (*f)[i] != other[i] {
			return i
		}
	}
	return 0
}

func (f *Fingerprint) Len() int {
	return len(*f)
}

func (f *Fingerprint) Empty() bool {
	return len(*f) == 0
}

func (f *Fingerprint) Sum() uint32 {
	var hash = crc32.NewIEEE()
	var b = make([]byte, 4)
	for i := 0; i < len(*f); i++ {
		binary.BigEndian.PutUint32(b, (*f)[i])
		_, _ = hash.Write(b)
	}
	return hash.Sum32()
}

func (f *Fingerprint) EncodeBinary() []byte {
	var b = make([]byte, 4*len(*f))
	for i := 0; i < len(*f); i++ {
		var offset = i * 4
		binary.BigEndian.PutUint32(b[offset:offset+4], (*f)[i])
	}
	return b
}

func (f *Fingerprint) DecodeBinary(b []byte) {
	for i := 0; i < len(b)/4; i++ {
		f.Append(binary.BigEndian.Uint32(b[i*4 : (i+1)*4]))
	}
}

func (f *Fingerprint) Reset() {
	*f = (*f)[:0]
}

func (f *Fingerprint) String() string {
	return fmt.Sprintf("%v:%v", f.Len(), f.Sum())
}

func NewFingerprint(cap int) Fingerprint {
	return make([]uint32, 0, cap)
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
	modified   int32
	modifies   int64
	header     []byte
	sync.Mutex

	headerHandler HeaderHandler
	interceptors  IOInterceptors
}

// CreateExtent create an new Extent to disk.
func CreateExtent(name string, extentID uint64, handler HeaderHandler, interceptors IOInterceptors) (*Extent, error) {
	var e = &Extent{
		extentID:      extentID,
		filePath:      name,
		headerHandler: handler,
		interceptors:  interceptors,
	}
	if err := e.create(); err != nil {
		return nil, err
	}
	return e, nil
}

// OpenExtent open an exists Extent from disk.
func OpenExtent(name string, extentID uint64, handler HeaderHandler, interceptors IOInterceptors) (*Extent, error) {
	var e = &Extent{
		extentID:      extentID,
		filePath:      name,
		headerHandler: handler,
		interceptors:  interceptors,
	}
	if err := e.open(); err != nil {
		return nil, err
	}
	return e, nil
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

// create init extent data info filesystem. If entry file exist and overwrite is true,
// this operation will clear all data of exist entry file and initialize extent header data.
func (e *Extent) create() (err error) {
	var interceptor = e.interceptors.Get(IOCreate)
	var ctx context.Context
	if ctx, err = interceptor.Before(); err != nil {
		return
	}
	e.file, err = os.OpenFile(e.filePath, ExtentOpenOpt, 0666)
	interceptor.After(ctx, 0, err)
	if err != nil {
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
	} else {
		e.header = make([]byte, unit.BlockHeaderSize)
	}
	atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
	e.dataSize = 0
	return
}

// open restores the entity data and status from the file stored on the filesystem.
func (e *Extent) open() (err error) {
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
	} else {
		e.header = make([]byte, unit.BlockHeaderSize)
		if err = e.headerHandler.Load(e.header, 0, unit.BlockHeaderSize); err != nil {
			return
		}
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

func (e *Extent) Modified() bool {
	return atomic.LoadInt32(&e.modified) != 0
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

	if err = e.checkTinyWriteParameter(offset, size, writeType); err != nil {
		return
	}

	if IsAppendWrite(writeType) && offset != e.dataSize {
		return ParameterMismatchError
	}
	var interceptor = e.interceptors.Get(IOWrite)
	var ctx context.Context
	if ctx, err = interceptor.Before(); err != nil {
		return
	}
	var n int
	n, err = e.file.WriteAt(data[:size], int64(offset))
	interceptor.After(ctx, int64(n), err)
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
func (e *Extent) Write(data []byte, offset, size int64, crc uint32, writeType int, isSync bool) (err error) {
	defer func() {
		if err == nil {
			atomic.StoreInt32(&e.modified, 1)
			atomic.AddInt64(&e.modifies, size)
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
	var interceptor = e.interceptors.Get(IOWrite)
	var ctx context.Context
	if ctx, err = interceptor.Before(); err != nil {
		return
	}
	var n int
	n, err = e.file.WriteAt(data[:size], int64(offset))
	interceptor.After(ctx, int64(n), err)
	if err != nil {
		return
	}
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

	var (
		blkNo     = int(offset / unit.BlockSize)
		blkOffset = offset % unit.BlockSize
	)
	if blkOffset == 0 && size == unit.BlockSize {
		// 当本次写入的数据位置完全对齐一个完整的Block(128KB), 更新该Block的CRC记录信息
		e.setBlkCRC(blkNo, crc)
		err = e.saveHeader(blkNo)
		return
	}

	// 将数据变更涉及Block的CRC信息置0以触发重新计算
	for cursor := blkNo; cursor <= int(offset+size-1)/unit.BlockSize; cursor++ {
		e.setBlkCRC(cursor, 0)
		if err = e.saveHeader(cursor); err != nil {
			return
		}
	}
	return
}

// Read reads data from an extent.
func (e *Extent) Read(data []byte, offset, size int64, isRepairRead bool) (crc uint32, err error) {
	if proto.IsTinyExtent(e.extentID) {
		return e.readTiny(data, offset, size, isRepairRead)
	}
	if err = e.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	var interceptor = e.interceptors.Get(IORead)
	var ctx context.Context
	if ctx, err = interceptor.Before(); err != nil {
		return
	}
	var n int
	n, err = e.file.ReadAt(data[:size], offset)
	interceptor.After(ctx, int64(n), err)
	if err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data)

	return
}

// readTiny read data from a tiny extent.
func (e *Extent) readTiny(data []byte, offset, size int64, isRepairRead bool) (crc uint32, err error) {
	var interceptor = e.interceptors.Get(IORead)
	var ctx context.Context
	if ctx, err = interceptor.Before(); err != nil {
		return
	}
	var n int
	n, err = e.file.ReadAt(data[:size], offset)
	interceptor.After(ctx, int64(n), err)
	if isRepairRead && err == io.EOF {
		err = nil
	}
	crc = crc32.ChecksumIEEE(data[:size])

	return
}

func (e *Extent) Fingerprint(offset, size int64, strict bool) (fingerprint Fingerprint, err error) {
	fingerprint = NewFingerprint(int(size) / unit.BlockSize)
	var blkCRC uint32
	var updatedBlkNos []int
	var buf = make([]byte, unit.BlockSize)
	for cursor := offset; cursor < offset+size; {
		if !proto.IsTinyExtent(e.extentID) && cursor%unit.BlockSize == 0 && cursor+unit.BlockSize < offset+size {
			var blkNo = int(cursor) / unit.BlockSize
			var updated bool
			if blkCRC, updated, err = e.computeBlkCRC(blkNo, strict); err != nil {
				return
			}
			if updated {
				updatedBlkNos = append(updatedBlkNos, blkNo)
			}
			cursor += unit.BlockSize
			fingerprint.Append(blkCRC)
			continue
		}
		var readSize = int64(math.Min(float64(offset+size-cursor), float64(unit.BlockSize)))
		if _, err = e.file.ReadAt(buf[:readSize], cursor); err != nil {
			return
		}
		blkCRC = crc32.ChecksumIEEE(buf[:readSize])
		fingerprint.Append(blkCRC)
		cursor += readSize
	}

	switch {
	case len(updatedBlkNos) == 0:
	case len(updatedBlkNos) <= 4:
		err = e.saveHeader(updatedBlkNos...)
	default:
		err = e.saveHeader()
	}
	return
}

func (e *Extent) saveHeader(blkNos ...int) (err error) {
	if len(blkNos) == 0 {
		err = e.headerHandler.Save(e.header[:unit.BlockHeaderSize], 0, unit.BlockHeaderSize)
		return
	}
	for _, blkNo := range blkNos {
		var hOff = int64(blkNo * unit.PerBlockCrcSize)
		if err = e.headerHandler.Save(e.header[hOff:hOff+unit.PerBlockCrcSize], hOff, unit.PerBlockCrcSize); err != nil {
			return
		}
	}
	return
}

func (e *Extent) setBlkCRC(blockNo int, crc uint32) {
	var hOffset = blockNo * unit.PerBlockCrcSize
	if len(e.header) >= hOffset+unit.PerBlockCrcSize {
		binary.BigEndian.PutUint32(e.header[hOffset:hOffset+unit.PerBlockCrcSize], crc)
	}
	return
}

func (e *Extent) getBlkCRC(blockNo int) uint32 {
	var hOffset = blockNo * unit.PerBlockCrcSize
	if len(e.header) >= hOffset+unit.PerBlockCrcSize {
		return binary.BigEndian.Uint32(e.header[hOffset : hOffset+unit.PerBlockCrcSize])
	}
	return 0
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

func (e *Extent) checkTinyWriteParameter(offset, size int64, writeType int) error {
	if IsAppendWrite(writeType) && offset != e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("illegal append: offset=%v size=%v extentsize=%v", offset, size, e.dataSize))
	}
	if IsRandomWrite(writeType) && offset+size > e.dataSize {
		return NewParameterMismatchErr(fmt.Sprintf("%v: offset=%v size=%v extentsize=%v", IllegalOverWriteError, offset, size, e.dataSize))
	}
	return nil
}

// Flush synchronizes data to the disk.
func (e *Extent) Flush(limiter *rate.Limiter) (err error) {
	if atomic.CompareAndSwapInt32(&e.modified, 1, 0) {
		var modifies = int(atomic.LoadInt64(&e.modifies))
		if limiter != nil {
			var burst = limiter.Burst()
			if modifies > burst {
				modifies = burst
			}
			_ = limiter.WaitN(context.Background(), modifies)
		}
		var interceptor = e.interceptors.Get(IOSync)
		var ctx context.Context
		if ctx, err = interceptor.Before(); err != nil {
			return
		}
		if err = e.file.Sync(); err != nil {
			return
		}
		interceptor.After(ctx, 0, err)
		atomic.StoreInt64(&e.modifies, 0)
	}
	return
}

func (e *Extent) autoComputeExtentCrc() (crc uint32, err error) {
	var blockCnt int
	blockCnt = int(e.Size() / unit.BlockSize)
	if e.Size()%unit.BlockSize != 0 {
		blockCnt += 1
	}
	crcData := make([]byte, blockCnt*unit.PerBlockCrcSize)
	var updatedBlkNos []int
	for blockNo := 0; blockNo < blockCnt; blockNo++ {
		var (
			blockCrc uint32
			updated  bool
		)
		if blockCrc, updated, err = e.computeBlkCRC(blockNo, false); err != nil {
			break
		}
		if updated {
			updatedBlkNos = append(updatedBlkNos, blockNo)
		}
		binary.BigEndian.PutUint32(crcData[blockNo*unit.PerBlockCrcSize:(blockNo+1)*unit.PerBlockCrcSize], blockCrc)
	}
	crc = crc32.ChecksumIEEE(crcData)

	switch {
	case len(updatedBlkNos) == 0:
	case len(updatedBlkNos) <= 4:
		err = e.saveHeader(updatedBlkNos...)
	default:
		err = e.saveHeader()
	}
	return
}

func (e *Extent) computeBlkCRC(blkNo int, force bool) (crc uint32, updated bool, err error) {
	if crc = e.getBlkCRC(blkNo); crc != 0 && !force {
		return
	}
	var (
		blkdata = make([]byte, unit.BlockSize)
		offset  = int64(blkNo * unit.BlockSize)
		readN   int
	)
	if readN, err = e.file.ReadAt(blkdata[:unit.BlockSize], offset); readN == 0 && err != nil {
		return
	}
	err = nil
	crc = crc32.ChecksumIEEE(blkdata[:readN])
	e.setBlkCRC(blkNo, crc)
	updated = true
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
	var interceptor = e.interceptors.Get(IOPunch)
	var ctx context.Context
	if ctx, err = interceptor.Before(); err != nil {
		return
	}
	err = fallocate(int(e.file.Fd()), FallocFLPunchHole|FallocFLKeepSize, offset, size)
	interceptor.After(ctx, 0, err)
	return
}

func (e *Extent) getRealBlockCnt() (blockNum int64) {
	stat := new(syscall.Stat_t)
	syscall.Stat(e.filePath, stat)
	return stat.Blocks
}

func (e *Extent) TinyExtentRecover(data []byte, offset, size int64, isEmptyPacket bool) (err error) {
	e.Lock()
	defer e.Unlock()
	if !proto.IsTinyExtent(e.extentID) {
		return ParameterMismatchError
	}
	if offset%PageSize != 0 || offset != e.dataSize {
		return fmt.Errorf("error empty packet on (%v) offset(%v) size(%v)"+
			" isEmptyPacket(%v)  e.dataSize(%v)", e.file.Name(), offset, size, isEmptyPacket, e.dataSize)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("before file (%v) getRealBlockNo (%v) isEmptyPacket(%v)"+
			"offset(%v) size(%v) e.datasize(%v)", e.filePath, e.getRealBlockCnt(), isEmptyPacket, offset, size, e.dataSize)
	}
	if isEmptyPacket {
		var finfo os.FileInfo
		finfo, err = e.file.Stat()
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
		var interceptor = e.interceptors.Get(IOPunch)
		var ctx context.Context
		if ctx, err = interceptor.Before(); err != nil {
			return
		}
		err = fallocate(int(e.file.Fd()), FallocFLPunchHole|FallocFLKeepSize, offset, size)
		interceptor.After(ctx, 0, err)
	} else {
		var interceptor = e.interceptors.Get(IOWrite)
		var ctx context.Context
		if ctx, err = interceptor.Before(); err != nil {
			return
		}
		var n int
		n, err = e.file.WriteAt(data[:size], int64(offset))
		interceptor.After(ctx, int64(n), err)
	}
	if err != nil {
		return
	}
	watermark := offset + size
	if watermark%PageSize != 0 {
		watermark = watermark + (PageSize - watermark%PageSize)
	}
	e.dataSize = watermark
	if log.IsDebugEnabled() {
		log.LogDebugf("after file (%v) getRealBlockNo (%v) isEmptyPacket(%v)"+
			"offset(%v) size(%v) e.datasize(%v)", e.filePath, e.getRealBlockCnt(), isEmptyPacket, offset, size, e.dataSize)
	}

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
