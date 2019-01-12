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
	"container/list"
	"math"
	"os"
	"sync"

	"github.com/tiglabs/raft/proto"
)

func (ds *DiskRotateStorage) getEntries(lo, hi uint64, maxSize uint64) (entries []*proto.Entry, isCompact bool, e error) { // [lo, hi]
	if ds.curLogFile == nil {
		return nil, false, Err_LogEmpty
	}
	isCompact = lo < ds.curLogFile.getFirstIndex()
	entries = ds.entryCache.getEntries(lo, hi, maxSize)
	if entries != nil {
		return entries, isCompact, nil
	}
	if !isCompact {
		entries, _, _, e := ds.curLogFile.readEntries(lo, hi, maxSize)
		if e != nil {
			return nil, isCompact, e
		}
		return entries, isCompact, nil
	}
	// 打开文件
	loPos := getLogFilePos(lo)
	hiPos := getLogFilePos(hi)
	fileCloseNum := int(hiPos - loPos + 1)
	files := make([]*LogFile, fileCloseNum)
	if hi >= ds.curLogFile.getFirstIndex() {
		hiPos--
		fileCloseNum--
		files[fileCloseNum] = ds.curLogFile
	}
	cleanFunc := func(entries []*proto.Entry, e error) ([]*proto.Entry, bool, error) {
		for i := 0; i < fileCloseNum; i++ {
			if files[i] != nil {
				files[i].Close()
			}
		}
		return entries, isCompact, e
	}
	for i := 0; i < fileCloseNum; i++ {
		pos := loPos + i
		path := ds.dir + LogFileName(ds.shardId, getLogFileFirstIndex(pos), getLogFileLastIndex(pos))
		files[i], e = LoadLogFile(path)
		if e != nil {
			return cleanFunc(entries, e)
		}
	}
	// 读文件
	size := uint64(0)
	sizeLimit := maxSize
	for _, loadFile := range files {
		var (
			es        []*proto.Entry
			localSize uint64
			isFull    bool
		)
		start, end := loadFile.getFirstIndex(), loadFile.getLastIndex()
		if start < lo {
			start = lo
		}
		if end > hi {
			end = hi
		}
		if sizeLimit != math.MaxUint32 {
			sizeLimit = maxSize - size
		}
		es, localSize, isFull, e = loadFile.readEntries(start, end, sizeLimit)
		if e != nil {
			return cleanFunc(entries, e)
		}
		if len(es) > 0 {
			entries = append(entries, es...)
			size += localSize
		}
		if isFull {
			break
		}
	}
	if len(entries) == 0 { // 因为大小限制一个都没出来
		entries, e = files[0].readEntry(lo)
		if e != nil {
			return cleanFunc(entries, e)
		}
	}
	return entries, isCompact, e
}
func (ds *DiskRotateStorage) getEntry(index uint64) (entries []*proto.Entry, isCompact bool, e error) {
	if ds.curLogFile == nil {
		return nil, false, Err_LogEmpty
	}
	isCompact = index < ds.curLogFile.getFirstIndex()
	if entry := ds.entryCache.getEntry(index); entry != nil {
		return []*proto.Entry{entry}, isCompact, nil
	}
	logFile := ds.curLogFile
	if isCompact {
		path := ds.dir + LogFileNameByIndex(ds.shardId, index)
		logFile, e = LoadLogFile(path)
		if e != nil {
			return
		}
		defer logFile.Close()
	}
	entries, _, _, e = logFile.readEntries(index, index, math.MaxUint32)
	return entries, isCompact, e
}
func (ds *DiskRotateStorage) getEntryMeta(index uint64) (entryMeta *EntryMeta, isCompact bool, e error) {
	if ds.curLogFile != nil {
		if firstIndex := ds.curLogFile.getFirstIndex(); index >= firstIndex {
			entryMeta, e = ds.curLogFile.getEntryMetaObj(int(index - firstIndex))
			return
		}
	}
	isCompact = true
	var logFile *LogFile
	path := ds.dir + LogFileNameByIndex(ds.shardId, index)
	if logFile, e = LoadLogFile(path); e != nil {
		return
	}
	defer logFile.Close()
	entryMeta, e = logFile.getEntryMetaObj(int(index - logFile.getFirstIndex()))
	return
}
func (ds *DiskRotateStorage) putEntries(entries []*proto.Entry) (e error) {
	var (
		n  int
		fn int
	)
	if ds.curLogFile == nil {
		if ds.curLogFile, n, e = NewLogFile(ds.dir, ds.shardId, entries); e != nil {
			return e
		}
	} else {
		n, e = ds.curLogFile.writeEntries(entries)
	}
	for n < len(entries) {
		if e = ds.rotateFile(); e != nil {
			return e
		}
		if ds.curLogFile, fn, e = NewLogFile(ds.dir, ds.shardId, entries[n:]); e != nil {
			return e
		}
		n += fn
	}
	return nil
}
func (ds *DiskRotateStorage) putEntry(entry *proto.Entry) (e error) {
	var n int
	if ds.curLogFile == nil {
		if ds.curLogFile, _, e = NewLogFile(ds.dir, ds.shardId, []*proto.Entry{entry}); e != nil {
			return e
		}
	}
	n, e = ds.curLogFile.writeEntries([]*proto.Entry{entry})
	if e != nil {
		return e
	}
	if n == 0 {
		if e = ds.rotateFile(); e != nil {
			return e
		}
		if ds.curLogFile, n, e = NewLogFile(ds.dir, ds.shardId, []*proto.Entry{entry}); e != nil {
			return e
		}
	}
	return nil
}
func (ds *DiskRotateStorage) rotateFile() (e error) {
	if _, e = ds.curLogFile.flushMeta(); e != nil {
		return e
	}
	ds.curLogFile.Close()
	e = os.Rename(ds.curLogFile.path, ds.curLogFile.finalPath)
	ds.curLogFile = nil
	return e
}
func (ds *DiskRotateStorage) getLastIndex() (uint64, error) {
	if ds.curLogFile == nil {
		return 0, Err_LogEmpty
	}
	lastIndex := ds.curLogFile.getLastIndex()
	if lastIndex == 0 {
		return 0, Err_LogFileEmpty
	}
	return lastIndex, nil
}
func (ds *DiskRotateStorage) getFirstIndex() (uint64, error) {
	snap, e := ds.hardStateFile.getSnapshotMeta()
	if e != nil {
		if ds.curLogFile != nil && ds.curLogFile.getFirstIndex() > 0 {
			return ds.curLogFile.getFirstIndex(), nil
		}
		return 0, e
	}
	return snap.Index, nil
}
func (ds *DiskRotateStorage) cutEnds(lastIndex uint64) error {
	if ds.curLogFile == nil { // 没文件，不用砍了
		return nil
	}
	nowLastIndex, e := ds.getLastIndex()
	if e != nil {
		return e
	}
	if lastIndex == nowLastIndex {
		return nil
	}
	firstIndex, e := ds.getFirstIndex()
	if e != nil {
		return e
	}
	// 清理缓存
	ds.entryCache.cutEnds(lastIndex)
	// 全清
	if lastIndex < firstIndex {
		start := getLogFilePos(firstIndex)
		end := getLogFilePos(ds.curLogFile.getFirstIndex())
		for i := start; i < end; i++ {
			os.Remove(ds.dir + LogFileNameByPos(ds.shardId, i))
		}
		ds.curLogFile.Delete()
		ds.curLogFile = nil
		return nil
	}
	// 跨文件
	if lastIndex >= firstIndex && lastIndex < ds.curLogFile.getFirstIndex() {
		start := getLogFilePos(lastIndex)
		end := getLogFilePos(ds.curLogFile.getFirstIndex())
		for i := start + 1; i < end; i++ {
			os.Remove(ds.dir + LogFileNameByPos(ds.shardId, i))
		}
		ds.curLogFile.Close()
		ds.curLogFile = nil
		if e = os.Rename(ds.dir+LogFileNameByPos(ds.shardId, start), CurLogFileName(ds.shardId)); e == nil {
			ds.curLogFile, e = LoadLastLogFile(ds.dir, ds.shardId)
		}
		if e != nil {
			return e
		}
	}
	// last file 内部
	if e = ds.curLogFile.cutEnds(lastIndex); e != nil {
		return e
	}
	return nil
}

type LogEntryCache struct {
	maxSize int

	cache  []*proto.Entry
	lo, hi int // 无限增长，除非被截短, {lo, hi)
}

func NewLogEntryCache(maxSize int) *LogEntryCache {
	return &LogEntryCache{
		maxSize: maxSize,
		cache:   make([]*proto.Entry, maxSize),
	}
}
func (cache *LogEntryCache) getLastEntry() *proto.Entry {
	return cache.cache[(cache.hi+cache.maxSize-1)%cache.maxSize]
}
func (cache *LogEntryCache) getFirstEntry() *proto.Entry {
	return cache.cache[(cache.lo)%cache.maxSize]
}
func (cache *LogEntryCache) getLastIndex() uint64 {
	if entry := cache.getLastEntry(); entry != nil {
		return entry.Index
	}
	return 0 // 意思是整个是空的
}
func (cache *LogEntryCache) getFirstIndex() uint64 {
	if entry := cache.getFirstEntry(); entry != nil {
		return entry.Index
	}
	return 0 // 意思是整个是空的
}
func (cache *LogEntryCache) write(entries []*proto.Entry) {
	if l := len(entries); l > cache.maxSize {
		entries = entries[l-cache.maxSize:]
	}
	for _, entry := range entries {
		cache.cache[cache.hi%cache.maxSize] = CloneEntry(entry)
		cache.hi++
		if cache.hi-cache.lo >= cache.maxSize {
			cache.lo++
		}
	}
}
func (cache *LogEntryCache) getEntry(index uint64) *proto.Entry {
	if firstEntry := cache.getFirstEntry(); firstEntry != nil {
		if entry := cache.cache[(cache.lo+int(index-firstEntry.Index))%cache.maxSize]; entry != nil && entry.Index == index {
			return entry
		}
	}
	return nil // 空的
}
func (cache *LogEntryCache) getEntries(lo, hi uint64, maxSize uint64) []*proto.Entry { // [lo, hi] 只返回整个在界内的
	if cache.lo == cache.hi { // 空的
		return nil
	}
	firstIndex, lastIndex := cache.getFirstIndex(), cache.getLastIndex()
	if lo < firstIndex || lastIndex < hi { // 在界外
		return nil
	}
	size, pos := int(hi-lo+1), cache.lo+int(lo-firstIndex)
	re := make([]*proto.Entry, size)
	for i := range re {
		re[i] = cache.cache[(pos+i)%cache.maxSize]
		if re[i] == nil || re[i].Index != lo+uint64(i) {
			return nil
		}
	}
	return CutEntriesMaxSize(re, maxSize)
}
func (cache *LogEntryCache) getCap() int {
	return cache.maxSize
}
func (cache *LogEntryCache) cutEnds(leaveIndex uint64) {
	// 空的
	if cache.hi == cache.lo {
		return
	}
	// 部分
	firstIndex := cache.getFirstIndex()
	lastIndex := cache.getLastIndex()
	if firstIndex <= leaveIndex && leaveIndex <= lastIndex {
		for cache.hi = cache.hi - 1; cache.cache[cache.hi%cache.maxSize].Index > leaveIndex; cache.hi-- {
			cache.cache[cache.hi%cache.maxSize] = nil
		}
		return
	}
	// 全部
	if leaveIndex < firstIndex {
		cache.clear()
	}
	// 不需要
	return
}
func (cache *LogEntryCache) clear() {
	for i := cache.lo; i < cache.hi; i++ {
		cache.cache[i%cache.maxSize] = nil
	}
	cache.lo, cache.hi = 0, 0
}

type LogFileCache struct {
	fileList *list.List
	fileMap  map[uint64]*list.Element
	lock     sync.RWMutex
	maxSize  int
}

func CloneEntry(org *proto.Entry) *proto.Entry {
	return &proto.Entry{
		Type:  org.Type,
		Term:  org.Term,
		Index: org.Index,
		Data:  org.Data,
	}
}
