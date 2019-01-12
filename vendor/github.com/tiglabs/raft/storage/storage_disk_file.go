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
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/tiglabs/raft/proto"
)

type LogFile struct {
	// meta
	shardId   uint64
	cap       uint32
	entryNum  int
	entryMeta []EntryMeta
	ranges    [2]uint64

	f         *os.File
	offset    int64
	path      string
	finalPath string
}

func NewLogFile(dir string, shardId uint64, entries []*proto.Entry) (file *LogFile, n int, e error) {
	if len(entries) == 0 {
		return nil, 0, Err_NilInput
	}
	pos := getLogFilePos(entries[0].Index)
	file = new(LogFile)
	file.shardId = shardId
	file.cap = Log_MaxEntryNum
	file.path = dir + "/" + CurLogFileName(shardId)
	file.ranges[0] = getLogFileFirstIndex(pos)
	file.ranges[1] = getLogFileLastIndex(pos)
	file.finalPath = dir + "/" + LogFileName(shardId, file.ranges[0], file.ranges[1])
	if file.f, e = os.OpenFile(file.path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0664); e != nil {
		return nil, 0, e
	}
	file.entryMeta = make([]EntryMeta, Log_MaxEntryNum)
	file.offset = Log_FileMetaSize
	if _, e = file.flushMeta(); e != nil {
		goto errAndNewFileClose
	}
	if n, e = file.writeEntries(entries); e != nil {
		goto errAndNewFileClose
	}
	return file, n, nil
errAndNewFileClose:
	file.f.Close()
	return nil, n, e
}
func LoadLastLogFile(dir string, shardId uint64) (file *LogFile, e error) {
	path := dir + CurLogFileName(shardId)
	file = new(LogFile)
	if file.f, e = os.OpenFile(path, os.O_RDWR, 0664); e != nil {
		return nil, e
	}
	file.entryMeta = make([]EntryMeta, Log_MaxEntryNum)
	file.path = path
	cleanFunc := func(e error) (*LogFile, error) {
		file.Close()
		return nil, e
	}

	mBuf := metaBufferPool.GetBuffer()
	defer metaBufferPool.PutBuffer(mBuf)
	if _, e = file.f.ReadAt(mBuf[:Log_MetaHeadSize], 0); e != nil {
		return cleanFunc(e)
	}
	if e = file.loadLogFileMetaHead(mBuf); e != nil {
		return cleanFunc(e)
	}

	file.offset = Log_FileMetaSize
	file.entryNum = 0
	buf := mBuf
	entry := new(proto.Entry)
	for file.entryNum < Log_MaxEntryNum {
		var n int
		n, e = readEntry(entry, mBuf, buf, file.f, file.offset)
		if e == io.EOF || e == Err_CrcNotMatch {
			e = nil
			break
		}
		if e != nil {
			return cleanFunc(e)
		}
		file.entryMeta[file.entryNum].Index = entry.Index
		file.entryMeta[file.entryNum].Term = entry.Term
		file.entryMeta[file.entryNum].Offset = file.offset
		file.offset += int64(n)
		file.entryNum++

		if len(entry.Data) > len(buf) {
			buf = entry.Data
		}
	}
	if e = file.f.Truncate(file.offset); e != nil {
		return cleanFunc(e)
	}
	return file, nil
}
func LoadLogFile(path string) (file *LogFile, e error) {
	file = new(LogFile)
	if file.f, e = os.OpenFile(path, os.O_RDONLY, 0664); e != nil {
		return nil, e
	}
	file.entryMeta = make([]EntryMeta, Log_MaxEntryNum)
	file.path = path

	mBuf := metaBufferPool.GetBuffer()
	defer metaBufferPool.PutBuffer(mBuf)
	if _, e = file.f.ReadAt(mBuf, 0); e != nil {
		goto errAndLoadFileClose
	}
	if e = file.loadLogFileMeta(mBuf); e != nil {
		goto errAndLoadFileClose
	}

	if file.entryNum == 0 { // 新架构下这边load的应该都是有东西的，不一定满(没从理论firstIndex开始写)
		file.offset = Log_FileMetaSize
		return nil, Err_LogFileEmpty
	}
	file.offset, e = fileSize(file.f)
	if e != nil {
		goto errAndLoadFileClose
	}
	if file.entryMeta[file.entryNum-1].Offset >= file.offset {
		e = Err_LogFileIncomplete
		goto errAndLoadFileClose
	}
	return file, nil

errAndLoadFileClose:
	file.f.Close()
	return nil, e
}
func (file *LogFile) Close() {
	if file.f != nil {
		file.f.Close()
		file.f = nil
	}
}
func (file *LogFile) Delete() (e error) {
	if file.f != nil {
		file.f.Close()
		file.f = nil
	}
	if e = os.Remove(file.path); e == os.ErrNotExist {
		e = nil
	}
	return
}
func (file *LogFile) Status() bool {
	return file.f != nil
}
func (file *LogFile) cutEnds(leaveLastIndex uint64) (e error) {
	if file.entryNum == 0 {
		return nil
	}
	if leaveLastIndex < file.entryMeta[0].Index {
		return nil
	}
	pos := int(leaveLastIndex - file.entryMeta[0].Index)
	if pos+1 < file.entryNum {
		file.offset = file.entryMeta[pos+1].Offset
		e = file.f.Truncate(file.offset)
		file.entryNum = pos + 1
	}
	return e
}
func (file *LogFile) writeEntries(entries []*proto.Entry) (n int, e error) {
	if len(entries) == 0 {
		return
	}
	if file.entryNum > 0 && file.entryMeta[file.entryNum-1].Index+1 != entries[0].Index {
		return 0, Err_FileLastIndexNotMatch
	}
	cco := crc32.NewIEEE()
	head := entryHeadPool.GetBuffer()
	defer entryHeadPool.PutBuffer(head)

	for i := 0; i < len(entries) && entries[i].Index <= file.ranges[1] && file.entryNum < Log_MaxEntryNum; i++ {
		entry := entries[i]
		binary.LittleEndian.PutUint32(head[4:], uint32(len(entry.Data))) // data size
		head[8] = byte(entry.Type)                                       // type
		binary.LittleEndian.PutUint64(head[9:], entry.Term)              // term
		binary.LittleEndian.PutUint64(head[17:], entry.Index)            // index
		cco.Reset()
		cco.Write(head[4:])
		cco.Write(entry.Data)
		binary.LittleEndian.PutUint32(head[0:], cco.Sum32()) // crc

		if _, e = file.f.WriteAt(head, file.offset); e == nil {
			_, e = file.f.WriteAt(entry.Data, file.offset+Log_EntryHeadSize)
		}
		if e != nil {
			return n, e
		}
		metaInts := &file.entryMeta[file.entryNum]
		metaInts.Index, metaInts.Term, metaInts.Offset = entry.Index, entry.Term, file.offset // 假定进来的都是连号的
		file.offset += int64(Log_EntryHeadSize + len(entry.Data))
		file.entryNum++
		n++
	}
	return n, nil
}
func (file *LogFile) readEntries(lo, hi uint64, maxSize uint64) ([]*proto.Entry, uint64, bool, error) {
	isFull := false
	poss, e := file.getEntriesPos(lo, hi)
	if e != nil {
		return nil, 0, isFull, e
	}

	entries := make([]*proto.Entry, hi-lo+1)
	size := uint64(0)
	head := entryHeadPool.GetBuffer()
	defer entryHeadPool.PutBuffer(head)

	for i, pos := range poss {
		entry := new(proto.Entry)
		if _, e = readEntry(entry, head, nil, file.f, pos.Offset); e != nil {
			return entries[:i], size, isFull, e
		}
		size += entry.Size()
		entries[i] = entry
		if maxSize != math.MaxUint32 {
			if size > maxSize {
				return entries[:i], size - entry.Size(), true, nil
			}
			if size == maxSize {
				return entries[:i+1], size, true, nil
			}
		}
	}
	return entries, size, isFull, nil
}
func (file *LogFile) readEntry(index uint64) ([]*proto.Entry, error) {
	pos, e := file.getEntriesPos(index, index)
	if e != nil {
		return nil, e
	}
	head := entryHeadPool.GetBuffer()
	defer entryHeadPool.PutBuffer(head)
	entry := new(proto.Entry)
	if _, e = readEntry(entry, head, nil, file.f, pos[0].Offset); e != nil {
		return nil, e
	}
	return []*proto.Entry{entry}, nil
}
func readEntry(entry *proto.Entry, headBuf, dataBuf []byte, r io.ReaderAt, pos int64) (int, error) {
	if entry == nil {
		return 0, Err_NilInput
	}
	if len(headBuf) < Log_EntryHeadSize {
		headBuf = entryHeadPool.GetBuffer()
		defer entryHeadPool.PutBuffer(headBuf)
	}
	if n, e := r.ReadAt(headBuf[:Log_EntryHeadSize], pos); e != nil {
		return n, e
	}
	crc := binary.LittleEndian.Uint32(headBuf)
	length := int(binary.LittleEndian.Uint32(headBuf[4:]))
	entry.Type = proto.EntryType(headBuf[8])
	entry.Term = binary.LittleEndian.Uint64(headBuf[9:])
	entry.Index = binary.LittleEndian.Uint64(headBuf[17:])
	if len(dataBuf) >= length {
		entry.Data = dataBuf[:length]
	} else {
		entry.Data = make([]byte, length)
	}
	if n, e := r.ReadAt(entry.Data, pos+Log_EntryHeadSize); e != nil {
		return Log_EntryHeadSize + n, e
	}
	cco := crc32.NewIEEE()
	cco.Write(headBuf[4:])
	cco.Write(entry.Data)
	if cco.Sum32() != crc {
		return Log_EntryHeadSize + length, Err_CrcNotMatch
	}
	return Log_EntryHeadSize + length, nil
}

type EntryMeta struct {
	Index  uint64
	Term   uint64
	Offset int64
}

func (em *EntryMeta) Clone() *EntryMeta {
	return &EntryMeta{
		Index:  em.Index,
		Term:   em.Term,
		Offset: em.Offset,
	}
}
