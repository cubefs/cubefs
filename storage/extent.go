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
	"bytes"
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
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ExtentOpenOpt  = os.O_CREATE | os.O_RDWR | os.O_EXCL
	ExtentHasClose = -1
	SEEK_DATA      = 3
	SEEK_HOLE      = 4
)

const (
	ExtentMaxSize = 1024 * 1024 * 1024 * 1024 * 4 // 4TB
	pageSize      = 4096
	alignSize     = 4096
)

func alignment(block []byte, AlignSize int) int {
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(AlignSize-1))
}

// alignedBlock returns []byte of size BlockSize aligned to a multiple
// of AlignSize in memory (must be power of two)
func alignedBlock(blkSize int, block []byte) []byte {
	a := alignment(block, alignSize)
	offset := 0
	if a != 0 {
		offset = alignSize - a
	}
	block = block[offset : offset+blkSize]
	// Can't check alignment of a zero sized block
	if blkSize != 0 {
		a = alignment(block, alignSize)
		if a != 0 {
			log.LogFatal("Failed to align block")
		}
	}
	return block
}

type WriteParam struct {
	ExtentID                                uint64
	Offset                                  int64
	Size                                    int64
	Data                                    []byte
	Crc                                     uint32
	WriteType                               int
	IsSync, IsHole, IsRepair, IsBackupWrite bool
}

func (wparam *WriteParam) String() (m string) {
	return fmt.Sprintf("ExtentID(%v)Offset(%v)Size(%v)Crc(%v)WriteType(%v)IsSync(%v)IsHole(%v)IsRepair(%v)IsBackupWrite(%v)",
		wparam.ExtentID, wparam.Offset, wparam.Size, wparam.Crc, wparam.WriteType, wparam.IsSync, wparam.IsHole, wparam.IsRepair, wparam.IsBackupWrite)
}

type ExtentInfo struct {
	FileID              uint64 `json:"fileId"`
	Size                uint64 `json:"size"`
	Crc                 uint32 `json:"Crc"`
	IsDeleted           bool   `json:"deleted"`
	ModifyTime          int64  `json:"modTime"` // random write not update modify time
	AccessTime          int64  `json:"accessTime"`
	Source              string `json:"src"`
	SnapshotDataOff     uint64 `json:"snapSize"`
	SnapPreAllocDataOff uint64 `json:"snapPreAllocSize"`
	ApplyID             uint64 `json:"applyID"`
	ApplySize           int64  `json:"ApplySize"`
}

func (ei *ExtentInfo) TotalSize() uint64 {
	if ei.SnapshotDataOff > util.ExtentSize {
		return ei.Size + (ei.SnapshotDataOff - util.ExtentSize)
	}
	return ei.Size
}

func (ei *ExtentInfo) String() (m string) {
	source := ei.Source
	if source == "" {
		source = "none"
	}
	return fmt.Sprintf("FileID(%v)_Size(%v)_IsDeleted(%v)_Source(%v)_MT(%d)_AT(%d)_CRC(%d)", ei.FileID, ei.Size, ei.IsDeleted, source, ei.ModifyTime, ei.AccessTime, ei.Crc)
}

func (ei *ExtentInfo) MarshalBinaryWithBuffer(buff *bytes.Buffer) (err error) {
	if err = binary.Write(buff, binary.BigEndian, ei.FileID); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, ei.Size); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, ei.IsDeleted); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, ei.ModifyTime); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, ei.AccessTime); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, ei.SnapPreAllocDataOff); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, ei.SnapshotDataOff); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, ei.ApplyID); err != nil {
		return
	}
	return
}

func (ei *ExtentInfo) MarshalBinary() (v []byte, err error) {
	buff := bytes.NewBuffer([]byte{})
	if err = ei.MarshalBinaryWithBuffer(buff); err != nil {
		return
	}
	v = buff.Bytes()
	return
}

func (ei *ExtentInfo) UnmarshalBinaryWithBuffer(buff *bytes.Buffer) (err error) {
	if err = binary.Read(buff, binary.BigEndian, &ei.FileID); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &ei.Size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &ei.IsDeleted); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &ei.ModifyTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &ei.AccessTime); err != nil {
		return
	}

	if buff.Len() > 0 {
		if err = binary.Read(buff, binary.BigEndian, &ei.SnapPreAllocDataOff); err != nil {
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &ei.SnapshotDataOff); err != nil {
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &ei.ApplyID); err != nil {
			return
		}
	}
	return
}

func (ei *ExtentInfo) UnmarshalBinary(v []byte) (err error) {
	err = ei.UnmarshalBinaryWithBuffer(bytes.NewBuffer(v))
	return
}

// SortedExtentInfos defines an array sorted by AccessTime
type SortedExtentInfos []*ExtentInfo

func (extInfos SortedExtentInfos) Len() int {
	return len(extInfos)
}

func (extInfos SortedExtentInfos) Less(i, j int) bool {
	return extInfos[i].AccessTime < extInfos[j].AccessTime
}

func (extInfos SortedExtentInfos) Swap(i, j int) {
	extInfos[i], extInfos[j] = extInfos[j], extInfos[i]
}

// Extent is an implementation of Extent for local regular extent file data management.
// This extent implementation manages all header info and data body in one single entry file.
// Header of extent include inode value of this extent block and Crc blocks of data blocks.
type Extent struct {
	file            *os.File
	readFile        *os.File
	filePath        string
	extentID        uint64
	modifyTime      int64
	accessTime      int64
	dataSize        int64
	hasClose        int32
	header          []byte
	snapshotDataOff uint64
	dirty           atomicutil.Bool
	sync.Mutex
}

// NewExtentInCore create and returns a new extent instance.
func NewExtentInCore(name string, extentID uint64) *Extent {
	e := new(Extent)
	e.extentID = extentID
	e.filePath = name
	e.snapshotDataOff = util.ExtentSize
	e.dirty.Store(false)
	return e
}

func (e *Extent) String() string {
	return fmt.Sprintf("%v_%v_%v", e.filePath, e.dataSize, e.snapshotDataOff)
}

func (e *Extent) GetSize() (int64, uint64) {
	return e.dataSize, e.snapshotDataOff
}

func (e *Extent) HasClosed() bool {
	return atomic.LoadInt32(&e.hasClose) == ExtentHasClose
}

func (e *Extent) setClosed() {
	atomic.StoreInt32(&e.hasClose, ExtentHasClose)
}

// Close this extent and release FD.
func (e *Extent) Close() (err error) {
	if e.HasClosed() {
		return
	}
	// NOTE: see https://yarchive.net/comp/linux/close_return_value.html
	// close will always tear down fd
	e.setClosed()
	if err = e.file.Close(); err != nil {
		return
	}

	if err = e.closeReadFile(); err != nil {
		return
	}
	return
}

func (e *Extent) Exist() (exsit bool) {
	_, err := os.Stat(e.filePath)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func (e *Extent) GetFile() *os.File {
	return e.file
}

// InitToFS init extent data info filesystem. If entry file exist and overwrite is true,
// this operation will clear all data of exist entry file and initialize extent header data.
func (e *Extent) InitToFS() (err error) {
	if e.file, err = os.OpenFile(e.filePath, ExtentOpenOpt, 0o666); err != nil {
		return err
	}
	if IsTinyExtent(e.extentID) {
		e.dataSize = 0
		return
	}
	atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
	atomic.StoreInt64(&e.accessTime, time.Now().Unix())
	e.dataSize = 0
	return
}

func (e *Extent) InitReadFile() (err error) {
	if e.readFile != nil {
		return
	}

	e.Lock()
	defer e.Unlock()

	if e.readFile != nil {
		return
	}

	if e.readFile, err = os.OpenFile(e.filePath, os.O_RDONLY|syscall.O_DIRECT, 0o666); err != nil {
		e.readFile = nil
		return err
	}

	return nil
}

func (e *Extent) closeReadFile() (err error) {
	if e.readFile == nil {
		return nil
	}

	if err = e.readFile.Close(); err != nil {
		return
	}
	return nil
}

func (e *Extent) GetDataSize(statSize int64) (dataSize int64) {
	var (
		dataStart int64
		holStart  int64
		curOff    int64
		err       error
	)

	for {
		// curOff if the hold start and the data end
		curOff, err = e.file.Seek(holStart, SEEK_DATA)
		if err != nil || curOff >= util.ExtentSize || (holStart > 0 && holStart == curOff) {
			log.LogDebugf("GetDataSize statSize %v curOff %v dataStart %v holStart %v, err %v,path %v", statSize, curOff, dataStart, holStart, err, e.filePath)
			break
		}
		log.LogDebugf("GetDataSize statSize %v curOff %v dataStart %v holStart %v, err %v,path %v", statSize, curOff, dataStart, holStart, err, e.filePath)
		dataStart = curOff

		curOff, err = e.file.Seek(dataStart, SEEK_HOLE)
		if err != nil || curOff >= util.ExtentSize || dataStart == curOff {
			log.LogDebugf("GetDataSize statSize %v curOff %v dataStart %v holStart %v, err %v,path %v", statSize, curOff, dataStart, holStart, err, e.filePath)
			break
		}
		log.LogDebugf("GetDataSize statSize %v curOff %v dataStart %v holStart %v, err %v,path %v", statSize, curOff, dataStart, holStart, err, e.filePath)
		holStart = curOff
	}
	log.LogDebugf("GetDataSize statSize %v curOff %v dataStart %v holStart %v, err %v,path %v", statSize, curOff, dataStart, holStart, err, e.filePath)
	if holStart == 0 {
		if statSize > util.ExtentSize {
			return util.ExtentSize
		}
		return statSize
	}
	return holStart
}

// RestoreFromFS restores the entity data and status from the file stored on the filesystem.
func (e *Extent) RestoreFromFS() (err error) {
	if e.file, err = os.OpenFile(e.filePath, os.O_RDWR, 0o666); err != nil {
		if os.IsNotExist(err) {
			err = ExtentNotFoundError
		}
		return err
	}

	var info os.FileInfo
	if info, err = e.file.Stat(); err != nil {
		err = fmt.Errorf("stat file %v: %v", e.file.Name(), err)
		return
	}

	if IsTinyExtent(e.extentID) {
		watermark := info.Size()
		if watermark%util.PageSize != 0 {
			watermark = watermark + (util.PageSize - watermark%util.PageSize)
		}
		e.dataSize = watermark
		return
	}

	e.dataSize = e.GetDataSize(info.Size())
	e.snapshotDataOff = util.ExtentSize
	if !IsTinyExtent(e.extentID) {
		if info.Size() > util.ExtentSize {
			e.snapshotDataOff = uint64(info.Size())
		}
	}

	atomic.StoreInt64(&e.modifyTime, info.ModTime().Unix())

	ts := info.Sys().(*syscall.Stat_t)
	atomic.StoreInt64(&e.accessTime, time.Unix(int64(ts.Atim.Sec), int64(ts.Atim.Nsec)).Unix())
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

func IsAppendRandomWrite(writeType int) bool {
	return writeType == AppendRandomWriteType
}

// WriteTiny performs write on a tiny extent.
func (e *Extent) WriteTiny(param *WriteParam) (err error) {
	e.Lock()
	defer e.Unlock()
	index := param.Offset + param.Size
	if index >= ExtentMaxSize {
		return ExtentIsFullError
	}

	if IsAppendWrite(param.WriteType) && param.Offset != e.dataSize {
		return ParameterMismatchError
	}

	if _, err = e.file.WriteAt(param.Data[:param.Size], int64(param.Offset)); err != nil {
		return
	}
	if param.IsSync {
		if err = e.file.Sync(); err != nil {
			return
		}
	}

	if !IsAppendWrite(param.WriteType) {
		return
	}
	if index%util.PageSize != 0 {
		index = index + (util.PageSize - index%util.PageSize)
	}
	e.dataSize = index

	return
}

// Write writes data to an extent.
func (e *Extent) Write(param *WriteParam, crcFunc UpdateCrcFunc) (status uint8, err error) {
	defer func() {
		e.dirty.Store(!param.IsSync)
	}()

	if logger.IsEnableDebug() {
		log.LogDebugf("action[Extent.Write] path %v write param(%v)", e.filePath, param)
	}
	defer func() {
		e.dirty.Store(!param.IsSync)
	}()

	status = proto.OpOk
	if IsTinyExtent(e.extentID) {
		err = e.WriteTiny(param)
		return
	}

	if err = e.checkWriteOffsetAndSize(param); err != nil {
		log.LogErrorf("action[Extent.Write] checkWriteOffsetAndSize write param(%v) err %v", param, err)
		err = newParameterError("extent current size=%d write param(%v)", e.dataSize, param)
		log.LogErrorf("action[Extent.Write] newParameterError path %v write param(%v) err %v", e.filePath, param, err)
		status = proto.OpTryOtherExtent
		return
	}

	// Check if extent file size matches the write offset just in case
	// multiple clients are writing concurrently.
	e.Lock()
	defer e.Unlock()

	if IsAppendWrite(param.WriteType) && e.dataSize != param.Offset && !param.IsRepair {
		err = newParameterError("extent current size=%d write param(%v)", e.dataSize, param)
		log.LogInfof("action[Extent.Write] newParameterError path %v write param(%v) err %v", e.filePath, param, err)
		status = proto.OpTryOtherExtent
		return
	}
	if IsAppendRandomWrite(param.WriteType) {
		if e.snapshotDataOff <= util.ExtentSize {
			log.LogInfof("action[Extent.Write] truncate extent %v write param(%v) truncate err %v", e, param, err)
			if err = e.file.Truncate(util.ExtentSize); err != nil {
				log.LogErrorf("action[Extent.Write] path %v write param(%v) truncate err %v", e.filePath, param, err)
				return
			}
		}
	}
	if param.IsHole {
		if err = e.repairPunchHole(param.Offset, param.Size); err != nil {
			return
		}
	} else {
		if _, err = e.file.WriteAt(param.Data[:param.Size], int64(param.Offset)); err != nil {
			log.LogErrorf("action[Extent.Write] path %v  write param(%v) err %v", e.filePath, param, err)
			return
		}
	}
	defer func() {
		if logger.IsEnableDebug() {
			log.LogDebugf("action[Extent.Write]  write param(%v),eInfo %v,err %v, status %v", param, e, err, status)
		}
		if IsAppendWrite(param.WriteType) {
			atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
			e.dataSize = int64(math.Max(float64(e.dataSize), float64(param.Offset+param.Size)))
			log.LogDebugf("action[Extent.Write] eInfo %v write param(%v)", e, param)
		} else if IsAppendRandomWrite(param.WriteType) {
			atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
			e.snapshotDataOff = uint64(math.Max(float64(e.snapshotDataOff), float64(param.Offset+param.Size)))
		}
		if logger.IsEnableDebug() {
			log.LogDebugf("action[Extent.Write] write param(%v) dataSize %v snapshotDataOff %v", param, e.dataSize, e.snapshotDataOff)
		}
	}()

	if param.IsSync {
		if err = e.file.Sync(); err != nil {
			log.LogDebugf("action[Extent.Write] write param(%v) err %v", param, err)
			return
		}
	}

	// NOTE: compute crc
	beginOffset := param.Offset
	endOffset := param.Offset + param.Size
	for beginOffset != endOffset {
		// NOTE: take a block
		blockNo := beginOffset / util.BlockSize
		offsetInBlock := beginOffset % util.BlockSize
		remainSizeInBlock := util.BlockSize - offsetInBlock

		sizeInBlock := endOffset - beginOffset
		if sizeInBlock > remainSizeInBlock {
			sizeInBlock = remainSizeInBlock
		}

		// NOTE: aliagn, compute crc
		if offsetInBlock == 0 && sizeInBlock == util.BlockSize {
			err = crcFunc(e, int(blockNo), param.Crc)
			log.LogDebugf("action[Extent.Write] write param(%v) err %v crcOffset %v", param, err, beginOffset)
			beginOffset += sizeInBlock
			continue
		}
		// NOTE: not aliagn
		err = crcFunc(e, int(blockNo), 0)
		log.LogDebugf("action[Extent.Write]  write param(%v) err %v crcOffset %v", param, err, beginOffset)
		beginOffset += sizeInBlock
	}
	return
}

// Read reads data from an extent.
func (e *Extent) Read(data []byte, offset, size int64, isRepairRead, directRead bool) (crc uint32, err error) {
	if IsTinyExtent(e.extentID) {
		return e.ReadTiny(data, offset, size, isRepairRead)
	}

	if err = e.checkReadOffsetAndSize(offset, size); err != nil {
		log.LogErrorf("action[Extent.Read]extent %v offset %d size %d err %v", e.extentID, offset, size, err)
		return
	}

	var rSize int
	if size < util.BlockSize && directRead {
		err = e.ReadAligned(data, offset, size)
	} else if rSize, err = e.file.ReadAt(data[:size], offset); err != nil {
		log.LogErrorf("action[Extent.Read]extent %v offset %v size %v err %v realsize %v", e.extentID, offset, size, err, rSize)
		return
	}
	crc = crc32.ChecksumIEEE(data)
	return
}

func (e *Extent) ReadAligned(data []byte, offset, size int64) error {
	err := e.InitReadFile()
	if err != nil {
		log.LogErrorf("ReadAligned: init read only file failed, path %s, err %s", e.filePath, err.Error())
		return err
	}

	start := offset / pageSize * pageSize
	end := (offset + size + pageSize - 1) / pageSize * pageSize

	newSize := end - start

	block := bytespool.Alloc(int(newSize) + alignSize)
	defer bytespool.Free(block)

	newData := alignedBlock(int(newSize), block)

	n, err := e.readFile.ReadAt(newData, start)
	if err != nil && err != io.EOF {
		return err
	}

	newEnd := offset - start + size
	if n < int(newEnd) {
		return fmt.Errorf("read data size %d less than req, off %d, start %d, size %d",
			n, offset, start, size)
	}

	copy(data, newData[offset-start:offset-start+size])

	return nil
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

func (e *Extent) checkReadOffsetAndSize(offset, size int64) error {
	if (e.snapshotDataOff == util.ExtentSize && offset > e.Size()) ||
		(e.snapshotDataOff > util.ExtentSize && uint64(offset) > e.snapshotDataOff) {
		return newParameterError("offset=%d size=%d snapshotDataOff=%d e.Size=%v", offset, size,
			e.snapshotDataOff, e.Size())
	}
	return nil
}

func (e *Extent) checkWriteOffsetAndSize(param *WriteParam) error {
	err := newParameterError("writeType=%d offset=%d size=%d", param.WriteType, param.Offset, param.Size)
	if IsAppendWrite(param.WriteType) {
		if param.Size == 0 ||
			param.Offset+param.Size > util.ExtentSize ||
			param.Offset >= util.ExtentSize {
			return err
		}
		if !param.IsRepair && param.Size > util.BlockSize {
			return err
		}
	} else if IsAppendRandomWrite(param.WriteType) {
		log.LogDebugf("action[checkOffsetAndSize] offset %v size %v", param.Offset, param.Size)
		if param.Offset < util.ExtentSize || param.Size == 0 {
			return err
		}
	}
	return nil
}

// Flush synchronizes data to the disk.
func (e *Extent) Flush() (err error) {
	if e.HasClosed() || !e.dirty.CompareAndSwap(true, false) {
		return
	}
	err = e.file.Sync()
	return
}

func (e *Extent) GetCrc(blockNo int64) uint32 {
	if int64(len(e.header)) < (blockNo+1)*util.PerBlockCrcSize {
		return 0
	}
	return binary.BigEndian.Uint32(e.header[blockNo*util.PerBlockCrcSize : (blockNo+1)*util.PerBlockCrcSize])
}

func (e *Extent) autoComputeExtentCrc(extSize int64, crcFunc UpdateCrcFunc) (crc uint32, err error) {
	var blockCnt int
	blockCnt = int(extSize / util.BlockSize)
	if extSize%util.BlockSize != 0 {
		blockCnt += 1
	}
	log.LogDebugf("autoComputeExtentCrc. path %v extent %v extent size %v,blockCnt %v", e.filePath, e.extentID, extSize, blockCnt)
	crcData := make([]byte, blockCnt*util.PerBlockCrcSize)
	for blockNo := 0; blockNo < blockCnt; blockNo++ {
		blockCrc := binary.BigEndian.Uint32(e.header[blockNo*util.PerBlockCrcSize : (blockNo+1)*util.PerBlockCrcSize])
		if blockCrc != 0 {
			binary.BigEndian.PutUint32(crcData[blockNo*util.PerBlockCrcSize:(blockNo+1)*util.PerBlockCrcSize], blockCrc)
			continue
		}
		bdata := make([]byte, util.BlockSize)
		offset := int64(blockNo * util.BlockSize)
		readN, err := e.file.ReadAt(bdata[:util.BlockSize], offset)
		if readN == 0 && err != nil {
			log.LogErrorf("autoComputeExtentCrc. path %v extent %v blockNo %v, readN %v err %v", e.filePath, e.extentID, blockNo, readN, err)
			break
		}
		blockCrc = crc32.ChecksumIEEE(bdata[:readN])
		err = crcFunc(e, blockNo, blockCrc)
		if err != nil {
			log.LogErrorf("autoComputeExtentCrc. path %v extent %v blockNo %v, err %v", e.filePath, e.extentID, blockNo, err)
			return 0, nil
		}
		log.LogDebugf("autoComputeExtentCrc. path %v extent %v blockCrc %v,blockNo %v", e.filePath, e.extentID, blockCrc, blockNo)
		binary.BigEndian.PutUint32(crcData[blockNo*util.PerBlockCrcSize:(blockNo+1)*util.PerBlockCrcSize], blockCrc)
	}
	crc = crc32.ChecksumIEEE(crcData)
	log.LogDebugf("autoComputeExtentCrc. path %v extent %v crc %v", e.filePath, e.extentID, crc)
	return crc, err
}

// DeleteTiny deletes a tiny extent.
func (e *Extent) punchDelete(offset, size int64) (hasDelete bool, err error) {
	log.LogDebugf("punchDelete extent %v offset %v, size %v", e, offset, size)
	if int(offset)%util.PageSize != 0 {
		return false, ParameterMismatchError
	}
	if int(size)%util.PageSize != 0 {
		size += int64(util.PageSize - int(size)%util.PageSize)
	}

	newOffset, err := e.file.Seek(offset, SEEK_DATA)
	if err != nil {
		if strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			return true, nil
		}
		return false, err
	}
	if newOffset-offset >= size {
		return true, nil
	}
	log.LogDebugf("punchDelete offset %v size %v", offset, size)
	err = fallocate(int(e.file.Fd()), util.FallocFLPunchHole|util.FallocFLKeepSize, offset, size)
	return
}

func (e *Extent) getRealBlockCnt() (blockNum int64) {
	stat := new(syscall.Stat_t)
	syscall.Stat(e.filePath, stat)
	return stat.Blocks
}

func (e *Extent) repairPunchHole(offset, size int64) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("error empty packet on (%v) offset(%v) size(%v)"+
				" e.dataSize(%v) err (%v)", e.file.Name(), offset, size, e.dataSize, err)
		}
	}()
	if offset%util.PageSize != 0 {
		err = fmt.Errorf("offset invalid")
		return
	}
	if IsTinyExtent(e.extentID) {
		if offset != e.dataSize {
			err = fmt.Errorf("tiny offset not euqal")
			return
		}
	} else {
		if offset < util.ExtentSize {
			if offset != e.dataSize {
				err = fmt.Errorf("normal offset not euqal")
				return
			}
		} else {
			if offset != int64(e.snapshotDataOff) {
				err = fmt.Errorf("normal offset not euqal")
				return
			}
		}
	}
	log.LogDebugf("before file (%v) getRealBlockNo (%v) "+
		"offset(%v) size(%v) e.datasize(%v)", e.filePath, e.getRealBlockCnt(), offset, size, e.dataSize)

	var finfo os.FileInfo
	finfo, err = e.file.Stat()
	if err != nil {
		return err
	}
	if offset < finfo.Size() {
		return fmt.Errorf("error empty packet on (%v) offset(%v) size(%v)"+
			" filesize(%v) e.dataSize(%v)", e.file.Name(), offset, size, finfo.Size(), e.dataSize)
	}
	if err = syscall.Ftruncate(int(e.file.Fd()), offset+size); err != nil {
		return err
	}
	err = fallocate(int(e.file.Fd()), util.FallocFLPunchHole|util.FallocFLKeepSize, offset, size)
	return
}

func (e *Extent) TinyExtentRecover(data []byte, offset, size int64, crc uint32, isEmptyPacket bool) (err error) {
	e.Lock()
	defer e.Unlock()
	if !IsTinyExtent(e.extentID) {
		return ParameterMismatchError
	}
	if isEmptyPacket {
		err = e.repairPunchHole(offset, size)
	} else {
		_, err = e.file.WriteAt(data[:size], int64(offset))
	}
	if err != nil {
		return
	}
	watermark := offset + size
	if watermark%util.PageSize != 0 {
		watermark = watermark + (util.PageSize - watermark%util.PageSize)
	}
	e.dataSize = watermark
	log.LogDebugf("after file (%v) getRealBlockNo (%v) isEmptyPacket(%v)"+
		"offset(%v) size(%v) e.datasize(%v)", e.filePath, e.getRealBlockCnt(), isEmptyPacket, offset, size, e.dataSize)

	return
}

func (e *Extent) getExtentWithHoleAvailableOffset(offset int64) (newOffset, newEnd int64, err error) {
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
	if newOffset-offset > util.BlockSize {
		newOffset = offset + util.BlockSize
	}
	if newEnd-newOffset > util.BlockSize {
		newEnd = newOffset + util.BlockSize
	}
	if newEnd < newOffset {
		err = fmt.Errorf("unavali ExtentAvaliOffset on SEEK_DATA or SEEK_HOLE   (%v) offset(%v) "+
			"newEnd(%v) newOffset(%v)", e.extentID, offset, newEnd, newOffset)
	}
	return
}
