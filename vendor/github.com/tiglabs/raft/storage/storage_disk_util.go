// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/tiglabs/raft/proto"
)

const (
	Log_Magic             = 0x1EEE // uint32
	Log_MaxEntryNum       = 8192
	Log_MetaHeadSize      = (4 + 4 + 8 + 4 + 4) // magic + crc + shard id + cap + entry num
	Log_FileMetaEntrySize = (8 + 8 + 8)         // index + term + offset
	Log_FileMetaSize      = (Log_MetaHeadSize + Log_MaxEntryNum*Log_FileMetaEntrySize)
	Log_EntryHeadSize     = (4 + 4 + 1 + 8 + 8)     // crc + data size + type + term + index
	Log_HsHeadSize        = (4 + 4 + 8 + 8 + 4 + 4) // magic + crc + shard id + seq id + data size + data size

	Log_EntryCacheNum = 128
	Log_FileCacheNum  = 5
	Log_HardStateNum  = 2
)

var (
	entryHeadPool    = NewBlockBuffer(Log_EntryHeadSize)
	metaBufferPool   = NewBlockBuffer(Log_FileMetaSize)
	hsHeadBufferPool = NewBlockBuffer(Log_HsHeadSize)

	Err_BufferSizeNotEnough   = errors.New("BufferSizeNotEnough")
	Err_LogFileFull           = errors.New("LogFileFull")
	Err_LogFileEmpty          = errors.New("LogFileEmpty")
	Err_LogEmpty              = errors.New("LogFileEmpty")
	Err_LogDirNotExist        = errors.New("Err_LogDirNotExist")
	Err_LogDirCannotAccess    = errors.New("Err_LogDirCannotAccess")
	Err_InvalidFilePos        = errors.New("InvalidFilePos")
	Err_IndexOutOfRange       = errors.New("IndexOutOfRange")
	Err_IndexOutOfFileRange   = errors.New("IndexOutOfFileRange")
	Err_LastIndexNotMatch     = errors.New("LastIndexNotMatch")
	Err_FileLastIndexNotMatch = errors.New("FileLastIndexNotMatch")
	Err_LogFileIncomplete     = errors.New("LogFileIncomplete")
	Err_LossLogFileInMiddle   = errors.New("LossLogFileInMiddle")
	Err_NilInput              = errors.New("NilInput")
	Err_EmptyHardState        = errors.New("EmptyHardState")
	Err_EmptySnapshotMeta     = errors.New("EmptySnapshotMeta")
	Err_NoHardStateFile       = errors.New("NoHardStateFile")
	Err_CrcNotMatch           = errors.New("CrcNotMatch")
	Err_BadMeta               = errors.New("BadMeta")
	Err_FirstIndexNotMatch    = errors.New("FirstIndexNotMatch")
	Err_BadMagic              = errors.New("BadMagic")
	Err_FileNameNotMatch      = errors.New("FileNameNotMatch")
)

type BlockBuffer struct {
	bufPool   *sync.Pool
	blockSize int
}

func NewBlockBuffer(blockSize int) *BlockBuffer {
	bb := &BlockBuffer{
		blockSize: blockSize,
		bufPool:   new(sync.Pool),
	}
	bb.bufPool.New = func() interface{} {
		return make([]byte, bb.blockSize)
	}
	return bb
}
func (bb *BlockBuffer) GetBuffer() []byte {
	return bb.bufPool.Get().([]byte)
}
func (bb *BlockBuffer) PutBuffer(b []byte) {
	if cap(b) >= bb.blockSize {
		bb.bufPool.Put(b[:bb.blockSize])
	}
}
func (bb *BlockBuffer) BlockSize() uint32 {
	return uint32(bb.blockSize)
}

func fileSize(f *os.File) (int64, error) {
	fi, e := f.Stat()
	if e != nil {
		return 0, e
	}
	return fi.Size(), nil
}
func getLogFilePos(index uint64) int {
	return int((index - 1) / Log_MaxEntryNum)
}
func getLogFileFirstIndex(pos int) uint64 {
	return uint64(pos*Log_MaxEntryNum + 1)
}
func getLogFileLastIndex(pos int) uint64 {
	return uint64(pos+1) * Log_MaxEntryNum
}
func LogFileName(shardId, firstIndex, lastIndex uint64) string {
	return fmt.Sprintf("log_%d.%010x-%010x", shardId, firstIndex, lastIndex)
}
func LogFileNameByPos(shardId uint64, pos int) string {
	return LogFileName(shardId, getLogFileFirstIndex(pos), getLogFileLastIndex(pos))
}
func LogFileNameByIndex(shardId, index uint64) string {
	pos := getLogFilePos(index)
	return LogFileName(shardId, getLogFileFirstIndex(pos), getLogFileLastIndex(pos))
}
func ParseLogFileName(fileName string) (shardId, firstIndex, lastIndex uint64, e error) {
	if _, e = fmt.Sscanf(fileName, "log_%d.%010x-%010x", &shardId, &firstIndex, &lastIndex); e != nil {
		return shardId, firstIndex, lastIndex, e
	}
	if fileName != LogFileName(shardId, firstIndex, lastIndex) {
		return shardId, firstIndex, lastIndex, Err_FileNameNotMatch
	}
	return shardId, firstIndex, lastIndex, nil
}
func CurLogFileName(shardId uint64) string {
	return fmt.Sprintf("log_%d.current", shardId)
}
func LogFilePrefix(shardId uint64) string {
	return fmt.Sprintf("log_%d.", shardId)
}
func HsFileName(shardId, seqId uint64) string {
	return fmt.Sprintf("hs_%d.%x", shardId, seqId%Log_HardStateNum)
}

func CutEntriesMaxSize(entries []*proto.Entry, maxSize uint64) []*proto.Entry {
	if len(entries) <= 1 || maxSize == math.MaxUint32 {
		return entries
	}
	size := entries[0].Size()
	for i := 1; i < len(entries); i++ {
		size += entries[i].Size()
		if size > maxSize {
			return entries[:i]
		}
	}
	return entries
}
