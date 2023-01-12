// Copyright 2018 The tiglabs raft Authors.
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
package wal

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/tiglabs/raft/logger"

	"github.com/tiglabs/raft/proto"
)

type logEntryStorage struct {
	s *Storage

	dir         string
	filesize    int
	syncRotate  bool
	logfiles    []logFileName // 所有日志文件的名字
	last        *logEntryFile
	nextFileSeq uint64

	cache *logFileCache

	rotates sync.Map
}

func openLogStorage(dir string, s *Storage) (*logEntryStorage, error) {
	ls := &logEntryStorage{
		s:           s,
		dir:         dir,
		filesize:    s.c.GetFileSize(),
		nextFileSeq: 1,
		syncRotate:  s.c.GetSyncRotate(),
	}

	// cache
	ls.cache = newLogFileCache(s.c.GetFileCacheCapacity(),
		func(name logFileName) (*logEntryFile, error) {
			return openLogEntryFile(ls.dir, name, false)
		})

	// open
	if err := ls.open(); err != nil {
		return nil, err
	}

	return ls, nil
}

func (ls *logEntryStorage) open() error {
	names, err := listLogEntryFiles(ls.dir)
	if err != nil {
		return err
	}

	// 没有历史文件，创建第一个起始index为0的文件
	if len(names) == 0 {
		f, err := ls.createNew(1)
		if err != nil {
			return err
		}
		ls.logfiles = append(ls.logfiles, f.Name())
		ls.last = f
		return nil
	}

	nlen := len(names)
	ls.nextFileSeq = names[nlen-1].seq + 1 // next设为历史文件中seq最大的加1
	ls.logfiles = append(ls.logfiles, names...)
	f, err := openLogEntryFile(ls.dir, ls.logfiles[nlen-1], true) // 打开最后一个文件
	if err != nil {
		return err
	}
	ls.last = f
	return nil
}

func (ls *logEntryStorage) ContinuityCheck() (lci uint64, pass bool, err error) {
	sort.Sort(nameSlice(ls.logfiles))
	var (
		lf                            *logEntryFile
		prevLastIndex, currFirstIndex uint64
	)
	for i := 1; i < len(ls.logfiles); i++ {
		if lf, err = ls.get(ls.logfiles[i-1]); err != nil {
			return
		}
		prevLastIndex = lf.LastIndex()
		if lf, err = ls.get(ls.logfiles[i]); err != nil {
			return
		}
		currFirstIndex = lf.FirstIndex()
		if prevLastIndex+1 != currFirstIndex {
			lci = prevLastIndex
			return
		}
	}
	lci = ls.last.LastIndex()
	pass = true
	return
}

func (ls *logEntryStorage) SetFileSize(filesize int) {
	if ls.filesize != filesize {
		ls.filesize = filesize
	}
}

func (ls *logEntryStorage) GetFileSize() int {
	return ls.filesize
}

func (ls *logEntryStorage) SetFileCacheCapacity(capacity int) {
	if ls != nil && ls.cache != nil {
		ls.cache.SetCapacity(capacity)
	}
}

func (ls *logEntryStorage) GetFileCacheCapacity() int {
	if ls != nil && ls.cache != nil {
		return ls.cache.GetCapacity()
	}
	return 0
}

func (ls *logEntryStorage) Term(i uint64) (term uint64, isCompact bool, err error) {
	lf, err := ls.locateFile(i)
	if err != nil {
		return
	}
	term, err = lf.Term(i)
	return
}

func (ls *logEntryStorage) LastIndex() uint64 {
	// 最后一个日志文件里没有东西
	if ls.last.Len() == 0 {
		if len(ls.logfiles) > 1 { // 拿上一个文件的lastIndex
			return ls.last.name.index - 1
		}
		return 0
	}
	return ls.last.LastIndex()
}

func (ls *logEntryStorage) Entries(lo, hi uint64, maxSize uint64) (entries []*proto.Entry, isCompact bool, err error) {
	if lo > ls.LastIndex() {
		err = fmt.Errorf("entries's hi(%d) is out of bound lastindex(%d)", hi, ls.LastIndex())
		return
	}

	si := ls.locate(lo)
	lfs := ls.logfiles[si:]
	var ent *proto.Entry
	var lf *logEntryFile
	i := lo
	var size uint64
	// 读取历史文件里的日志
	for _, fn := range lfs {
		if fn.index >= hi {
			return
		}

		lf, err = ls.get(fn)
		if err != nil {
			return
		}
		for i <= lf.LastIndex() {
			ent, err = lf.Get(i)
			if err != nil {
				return
			}
			size += ent.Size()
			if i >= hi || size > maxSize {
				return
			}
			entries = append(entries, ent)
			i++
		}
	}

	return
}

func (ls *logEntryStorage) SaveEntries(ents []*proto.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	if err := ls.truncateBack(ents[0].Index); err != nil {
		return err
	}

	for _, ent := range ents {
		if err := ls.saveEntry(nil, ent); err != nil {
			return err
		}
	}

	// flush应用层内存中的，写入file
	if err := ls.last.Flush(nil); err != nil {
		return err
	}

	return nil
}

func (ls *logEntryStorage) Sync() error {
	return ls.last.Sync()
}

// TruncateFront 从前面截断，用于删除旧数据, 只有整个文件的数据都是旧的时才删除
func (ls *logEntryStorage) TruncateFront(index uint64) error {
	truncFIndex := -1
	for i := 0; i < len(ls.logfiles)-1; i++ {
		if ls.logfiles[i+1].index-1 <= index {
			truncFIndex = i
		} else {
			break
		}
	}

	var deletions []logFileName
	if truncFIndex >= 0 {
		deletions = make([]logFileName, truncFIndex+1)
		copy(deletions, ls.logfiles[:truncFIndex+1])
		ls.logfiles = ls.logfiles[truncFIndex+1:]
	}

	var remove = func(names []logFileName) {
		for _, name := range names {
			_ = ls.cache.Delete(name, true)
		}
		go func(names []logFileName) {
			for _, name := range names {
				if err := ls.remove(name); err != nil {
					logger.Warn("remove log file [%v] fail: %v", name.String(), err)
				}
			}
		}(names)
	}

	if len(deletions) > 0 {
		remove(deletions)
	}

	return nil
}

// TruncateAll 清空
func (ls *logEntryStorage) TruncateAll() error {
	ls.cache.Clean(true)
	for _, f := range ls.logfiles {
		if err := ls.remove(f); err != nil {
			return err
		}
	}
	ls.nextFileSeq = 1
	ls.logfiles = nil

	lf, err := ls.createNew(1)
	if err != nil {
		return err
	}
	ls.last = lf
	ls.logfiles = append(ls.logfiles, lf.Name())

	return nil
}

// truncateBack 从后面截断，用于删除冲突日志
func (ls *logEntryStorage) truncateBack(index uint64) error {

	if ls.LastIndex() < index {
		return nil
	}

	if ls.logfiles[0].index >= index {
		return ls.TruncateAll()
	}

	idx := ls.locate(index)
	if idx == len(ls.logfiles)-1 { // 冲突位置在最后一个文件
		if err := ls.last.Truncate(index); err != nil {
			return err
		}
	} else {
		for i := idx + 1; i < len(ls.logfiles); i++ {
			if err := ls.remove(ls.logfiles[i]); err != nil {
				return err
			}
			_ = ls.cache.Delete(ls.logfiles[i], true)
			var lfn = ls.logfiles[i]
			logger.Warn("storage[%v] remove log file [name: %v] cause truncate back to [index: %v]", ls.dir, lfn.String(), index)
		}

		n := ls.logfiles[idx]
		lf, err := ls.get(n)
		if err != nil {
			return err
		}

		if ls.isAsyncRotateEnabled() {
			ls.checkAndWaitRotate(lf.Name())
		}

		_ = ls.cache.Delete(n, false)

		ls.last = lf
		if err := ls.last.OpenWrite(); err != nil {
			return err
		}
		if err := ls.last.Truncate(index); err != nil {
			return err
		}

		ls.logfiles = ls.logfiles[:idx+1]
		ls.nextFileSeq = n.seq + 1
	}
	return nil
}

func (ls *logEntryStorage) createNew(index uint64) (*logEntryFile, error) {
	name := logFileName{seq: ls.nextFileSeq, index: index}
	f, err := createLogEntryFile(ls.dir, name)
	if err != nil {
		return nil, err
	}

	ls.nextFileSeq++

	return f, nil
}

func (ls *logEntryStorage) get(name logFileName) (*logEntryFile, error) {
	if name.seq == ls.last.Seq() {
		return ls.last, nil
	}
	return ls.cache.Get(name)
}

func (ls *logEntryStorage) remove(name logFileName) error {
	return os.Remove(path.Join(ls.dir, name.String()))
}

// 写满了，新建一个新文件
func (ls *logEntryStorage) rotate(ctx context.Context) error {

	prevLast := ls.last.LastIndex()

	prev := ls.last

	var err error

	if err = prev.Flush(ctx); err != nil {
		return err
	}

	var lf *logEntryFile
	lf, err = ls.createNew(prevLast + 1)
	if err != nil {
		return err
	}

	ls.last = lf
	ls.logfiles = append(ls.logfiles, lf.Name())

	if err = ls.cache.Put(prev.Name(), prev); err != nil {
		return err
	}

	if !ls.isAsyncRotateEnabled() {
		if err := prev.FinishWrite(ctx); err != nil {
			return err
		}
		return nil
	}

	prev.IncreaseRef()
	ls.registerRotate(prev.Name())
	go func(lf *logEntryFile) {
		defer ls.revokeRotate(lf.Name())
		_ = lf.FinishWrite(ctx)
		_ = lf.Release()
	}(prev)

	return nil
}

func (ls *logEntryStorage) size() int {
	return len(ls.logfiles)
}

func (ls *logEntryStorage) locate(logindex uint64) int {
	fi := sort.Search(len(ls.logfiles), func(i int) bool {
		var nextIndex uint64
		if i == len(ls.logfiles)-1 {
			nextIndex = math.MaxUint64
		} else {
			nextIndex = ls.logfiles[i+1].index
		}
		return logindex < nextIndex
	})
	return fi
}

func (ls *logEntryStorage) locateFile(logindex uint64) (*logEntryFile, error) {
	i := ls.locate(logindex)
	if i >= len(ls.logfiles) {
		panic("could not find log file")
	}
	return ls.get(ls.logfiles[i])
}

func (ls *logEntryStorage) saveEntry(ctx context.Context, ent *proto.Entry) error {

	// 检查日志是否连续
	prevIndex := ls.LastIndex()
	if prevIndex != 0 {
		if prevIndex+1 != ent.Index {
			return fmt.Errorf("append discontinuous log. prev index: %d, current: %d", prevIndex, ent.Index)
		}
	}

	// 当期文件是否已经写满
	woffset := ls.last.WriteOffset()
	if uint64(woffset)+uint64(recordSize(ent)) > uint64(ls.filesize) {
		if err := ls.rotate(ctx); err != nil {
			return err
		}
	}

	if err := ls.last.Save(ctx, ent); err != nil {
		return err
	}

	return nil
}

func (ls *logEntryStorage) Close() {
	if ls.isAsyncRotateEnabled() {
		// Wait for async rotate process finished.
		ls.checkAndWaitAllRotate()
	}

	if err := ls.cache.Close(); err != nil {
		logger.Warn("close log file cache error: %v", err)
	}

	if err := ls.last.Release(); err != nil {
		var name = ls.last.Name()
		logger.Warn("close log file %s error: %v", name.String(), err)
	}
}

func (ls *logEntryStorage) isAsyncRotateEnabled() bool {
	return !ls.syncRotate
}

func (ls *logEntryStorage) registerRotate(name logFileName) {
	var rotateWG = new(sync.WaitGroup)
	rotateWG.Add(1)
	for {
		value, loaded := ls.rotates.LoadOrStore(name, rotateWG)
		if !loaded {
			return
		}
		if wg, is := value.(*sync.WaitGroup); is {
			wg.Wait()
		}
	}
}

func (ls *logEntryStorage) revokeRotate(name logFileName) {
	if value, loaded := ls.rotates.LoadAndDelete(name); loaded {
		if wg, is := value.(*sync.WaitGroup); is {
			wg.Done()
		}
	}
}

func (ls *logEntryStorage) checkAndWaitRotate(name logFileName) {
	if value, loaded := ls.rotates.Load(name); loaded {
		if wg, is := value.(*sync.WaitGroup); is {
			wg.Wait()
		}
	}
}

func (ls *logEntryStorage) checkAndWaitAllRotate() {
	ls.rotates.Range(func(key, value interface{}) bool {
		if wg, is := value.(*sync.WaitGroup); is {
			wg.Wait()
		}
		return true
	})
}
