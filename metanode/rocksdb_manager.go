// Copyright 2024 The CubeFS Authors.
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

package metanode

import (
	"errors"
	"sync"

	"github.com/cubefs/cubefs/util/diskmon"
	"github.com/cubefs/cubefs/util/log"
)

var (
	ErrUnregisteredRocksdbPath = errors.New("rocksdb path unregister")
	ErrRocksdbPathRegistered   = errors.New("rocksdb path already registered")
	ErrRocksdbOpened           = errors.New("rocksdb stil in use")
)

type RocksdbManager interface {
	Register(dbPath string) (err error)

	Unregister(dbPath string) (err error)

	OpenRocksdb(dbPath string) (db *RocksdbOperator, err error)

	CloseRocksdb(db *RocksdbOperator)

	SelectRocksdbDisk(usableFactor float64) (disk string, err error)

	AttachPartition(dbPath string) (err error)

	DetachPartition(dbPath string) (err error)

	GetPartitionCount(dbPath string) (count int, err error)
}

type RocksdbHandle struct {
	db         *RocksdbOperator
	rc         uint64
	partitions int
}

type rocksdbManager struct {
	writeBufferSize int
	writeBufferNum  int
	blockCacheSize  uint64
	mutex           sync.Mutex
	dbs             map[string]*RocksdbHandle
}

func (r *rocksdbManager) Register(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	_, ok := r.dbs[dbPath]
	if ok {
		err = ErrRocksdbPathRegistered
		return
	}
	r.dbs[dbPath] = &RocksdbHandle{
		db: NewRocksdb(),
		rc: 0,
	}
	return
}

func (r *rocksdbManager) Unregister(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	if handle.rc != 0 {
		err = ErrRocksdbOpened
		return
	}
	delete(r.dbs, dbPath)
	return
}

func (r *rocksdbManager) OpenRocksdb(dbPath string) (db *RocksdbOperator, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	handle.rc += 1
	if handle.rc == 1 {
		err = handle.db.OpenDb(dbPath, r.writeBufferSize, r.writeBufferNum, r.blockCacheSize, 0, 0, 0)
		if err != nil {
			handle.rc -= 1
			return
		}
	}
	db = handle.db
	return
}

func (r *rocksdbManager) CloseRocksdb(db *RocksdbOperator) {
	dbPath := db.dir
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		return
	}
	handle.rc -= 1
	if handle.rc == 0 {
		err := handle.db.CloseDb()
		if err != nil {
			log.LogErrorf("[CloseRocksdb] failed to close rocksdb(%v) err(%v)", dbPath, err)
		}
	}
}

func (r *rocksdbManager) SelectRocksdbDisk(usableFactor float64) (disk string, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	stats := make([]diskmon.DiskStat, 0)
	for dir, handle := range r.dbs {
		var stat diskmon.DiskStat
		stat, err = diskmon.NewDiskStat(dir)
		if err != nil {
			log.LogErrorf("[SelectRocksdbDisk] failed to select rocksdb disk, err(%v)", err)
			return
		}
		stat.PartitionCount = handle.partitions
		stats = append(stats, stat)
	}
	d, err := diskmon.SelectDisk(stats, usableFactor)
	if err != nil {
		log.LogErrorf("[SelectRocksdbDisk] failed to select rocksdb disk, err(%v)", err)
		return
	}
	disk = d.Path
	handle := r.dbs[disk]
	handle.partitions += 1
	return
}

func (r *rocksdbManager) AttachPartition(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	handle.partitions += 1
	return
}

func (r *rocksdbManager) DetachPartition(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	if handle.partitions != 0 {
		handle.partitions -= 1
	}
	return
}

func (r *rocksdbManager) GetPartitionCount(dbPath string) (count int, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	count = handle.partitions
	return
}

var _ RocksdbManager = &rocksdbManager{}

func NewRocksdbManager(writeBufferSize int, writeBufferNum int, blockCacheSize uint64) (p RocksdbManager) {
	p = &rocksdbManager{
		writeBufferSize: writeBufferSize,
		writeBufferNum:  writeBufferNum,
		blockCacheSize:  blockCacheSize,
		dbs:             make(map[string]*RocksdbHandle),
	}
	return
}
