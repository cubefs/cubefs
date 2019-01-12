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
	"io/ioutil"
	"os"
	"strings"

	"github.com/tiglabs/raft/proto"
)

type DiskRotateStorage struct {
	dir     string
	shardId uint64

	entryCache *LogEntryCache
	curLogFile *LogFile

	hardStateFile *HardStateFile
}

func NewDiskRotateStorage(dir string, shardId uint64) (*DiskRotateStorage, error) {
	if !strings.HasSuffix(dir, "/") {
		dir = dir + "/"
	}
	if fi, e := os.Stat(dir); e != nil {
		return nil, e
	} else if !fi.IsDir() {
		return nil, Err_LogDirNotExist
	}
	// hard state
	hsf, e := LoadHardStateFile(dir, shardId)
	if e != nil && e != Err_NoHardStateFile {
		return nil, e
	}
	logFile, e := LoadLastLogFile(dir, shardId)
	if e != nil && !strings.Contains(e.Error(), "no such file or directory") {
		return nil, e
	}
	// log file
	ds := &DiskRotateStorage{
		dir:           dir,
		shardId:       shardId,
		entryCache:    NewLogEntryCache(Log_EntryCacheNum),
		curLogFile:    logFile,
		hardStateFile: hsf,
	}
	return ds, nil
}

func (ds *DiskRotateStorage) StoreEntries(entries []*proto.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	if nowLastIndex, e := ds.getLastIndex(); e != nil && ds.curLogFile != nil {
		return e
	} else if nowLastIndex+1 < entries[0].Index {
		return Err_LastIndexNotMatch
	}
	e := ds.cutEnds(entries[0].Index - 1)
	if e != nil {
		return e
	}
	if e = ds.putEntries(entries); e != nil {
		return e
	}
	ds.entryCache.write(entries)
	return nil
}
func (ds *DiskRotateStorage) Truncate(index uint64) error {
	pos := getLogFilePos(index)
	for i := 0; i < pos; i++ {
		os.RemoveAll(ds.dir + LogFileNameByPos(ds.shardId, i))
	}
	return nil
}
func (ds *DiskRotateStorage) Entries(lo, hi uint64, maxSize uint64) (entries []*proto.Entry, isCompact bool, e error) {
	if lo >= hi {
		return nil, false, Err_IndexOutOfRange
	} else if lo == hi-1 {
		return ds.getEntry(lo)
	}
	return ds.getEntries(lo, hi-1, maxSize)
}
func (ds *DiskRotateStorage) Term(index uint64) (term uint64, isCompact bool, e error) {
	var em *EntryMeta
	em, isCompact, e = ds.getEntryMeta(index)
	if e != nil {
		return
	}
	term = em.Term
	return
}
func (ds *DiskRotateStorage) FirstIndex() (uint64, error) {
	sm, e := ds.hardStateFile.getSnapshotMeta()
	if e != nil {
		return 0, e
	}
	return sm.Index, nil
}
func (ds *DiskRotateStorage) LastIndex() (uint64, error) {
	return ds.getLastIndex()
}
func (ds *DiskRotateStorage) InitialState() (hs proto.HardState, e error) {
	if ds.hardStateFile == nil {
		return
	}
	hsp, e := ds.hardStateFile.getHardState()
	if e != nil {
		return hs, e
	}
	copyHardState(hsp, &hs)
	return hs, nil
}
func (ds *DiskRotateStorage) StoreHardState(hs proto.HardState) (e error) {
	if ds.hardStateFile == nil {
		ds.hardStateFile, e = NewHardStateFile(ds.dir, ds.shardId, &hs, nil)
		return e
	}
	return ds.hardStateFile.write(&hs, nil)
}
func (ds *DiskRotateStorage) ApplySnapshot(meta proto.SnapshotMeta) (e error) {
	if ds.hardStateFile == nil {
		ds.hardStateFile, e = NewHardStateFile(ds.dir, ds.shardId, nil, &meta)
		return e
	}
	return ds.hardStateFile.write(nil, &meta)
}
func (ds *DiskRotateStorage) Clear() error { // 废
	ds.entryCache.clear()
	fis, e := ioutil.ReadDir(ds.dir)
	if e != nil {
		return e
	}
	if ds.curLogFile != nil {
		ds.curLogFile.Close()
	}
	logPrefix := LogFilePrefix(ds.shardId)
	for _, fi := range fis {
		if !fi.IsDir() && strings.HasPrefix(fi.Name(), logPrefix) {
			os.RemoveAll(ds.dir + fi.Name())
		}
	}
	return nil
}
func (ds *DiskRotateStorage) Close() {
	ds.entryCache.clear()
	if ds.curLogFile != nil {
		ds.curLogFile.Close()
	}
	return
}
