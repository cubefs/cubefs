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
	"fmt"
	"path"
	"strings"
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

	OpenRocksdb(dbPath string, metaPartitionId uint64) (db *RocksdbOperator, err error)

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

type PerDiskRocksdbManager struct {
	writeBufferSize     int
	writeBufferNum      int
	minWriteBuffToMerge int
	blockCacheSize      uint64
	mutex               sync.Mutex
	dbs                 map[string]*RocksdbHandle
}

func (r *PerDiskRocksdbManager) Register(dbPath string) (err error) {
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

func (r *PerDiskRocksdbManager) Unregister(dbPath string) (err error) {
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

func (r *PerDiskRocksdbManager) OpenRocksdb(dbPath string, metaPartitionId uint64) (db *RocksdbOperator, err error) {
	log.LogDebugf("[OpenRocksdb] open rocksdb(%v) for mp(%v)", dbPath, metaPartitionId)

	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	handle.rc += 1
	if handle.rc == 1 {
		err = handle.db.OpenDb(dbPath, r.writeBufferSize, r.writeBufferNum, r.minWriteBuffToMerge, r.blockCacheSize, 0, 0, 0)
		if err != nil {
			handle.rc -= 1
			return
		}
	}
	db = handle.db
	return
}

func (r *PerDiskRocksdbManager) CloseRocksdb(db *RocksdbOperator) {
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

func (r *PerDiskRocksdbManager) SelectRocksdbDisk(usableFactor float64) (disk string, err error) {
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

func (r *PerDiskRocksdbManager) AttachPartition(dbPath string) (err error) {
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

func (r *PerDiskRocksdbManager) DetachPartition(dbPath string) (err error) {
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

func (r *PerDiskRocksdbManager) GetPartitionCount(dbPath string) (count int, err error) {
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

var _ RocksdbManager = &PerDiskRocksdbManager{}

type PerPartitionRocksdbManager struct {
	writeBufferSize     int
	writeBufferNum      int
	minWriteBuffToMerge int
	blockCacheSize      uint64
	mutex               sync.Mutex
	partitionCnt        map[string]int
	dbs                 map[string]interface{}
}

func (r *PerPartitionRocksdbManager) AttachPartition(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	cnt, ok := r.partitionCnt[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	cnt += 1
	r.partitionCnt[dbPath] = cnt
	return
}

func (r *PerPartitionRocksdbManager) DetachPartition(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	cnt, ok := r.partitionCnt[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	if cnt != 0 {
		cnt -= 1
		r.partitionCnt[dbPath] = cnt
	}
	return
}

func (r *PerPartitionRocksdbManager) GetPartitionCount(dbPath string) (count int, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	count, ok := r.partitionCnt[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	return
}

func (r *PerPartitionRocksdbManager) OpenRocksdb(dbPath string, metaPartitionId uint64) (db *RocksdbOperator, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	_, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}

	mpPath := fmt.Sprintf("metaPartition_%v", metaPartitionId)
	perPartitionDbDir := path.Join(dbPath, mpPath)
	db = NewRocksdb()
	err = db.OpenDb(perPartitionDbDir, r.writeBufferSize, r.writeBufferNum, r.minWriteBuffToMerge, r.blockCacheSize, 0, 0, 0)
	return
}

func (r *PerPartitionRocksdbManager) CloseRocksdb(db *RocksdbOperator) {
	err := db.CloseDb()
	if err != nil {
		log.LogErrorf("[CloseRocksdb] failed to close rocksdb(%v), err(%v)", db.dir, err)
	}
}

func (r *PerPartitionRocksdbManager) Register(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	_, ok := r.dbs[dbPath]
	if ok {
		err = ErrRocksdbPathRegistered
		return
	}
	r.dbs[dbPath] = 1
	r.partitionCnt[dbPath] = 0
	return
}

func (r *PerPartitionRocksdbManager) Unregister(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	_, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	delete(r.dbs, dbPath)
	delete(r.partitionCnt, dbPath)
	return
}

func (r *PerPartitionRocksdbManager) SelectRocksdbDisk(usableFactor float64) (disk string, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	stats := make([]diskmon.DiskStat, 0)
	for dir := range r.dbs {
		var stat diskmon.DiskStat
		stat, err = diskmon.NewDiskStat(dir)
		if err != nil {
			log.LogErrorf("[SelectRocksdbDisk] failed to select rocksdb disk, err(%v)", err)
			return
		}
		stat.PartitionCount = r.partitionCnt[dir]
		stats = append(stats, stat)
	}
	d, err := diskmon.SelectDisk(stats, usableFactor)
	if err != nil {
		log.LogErrorf("[SelectRocksdbDisk] failed to select rocksdb disk, err(%v)", err)
		return
	}
	disk = d.Path
	r.partitionCnt[disk] += 1
	return
}

var _ RocksdbManager = &PerPartitionRocksdbManager{}

func NewPerDiskRocksdbManager(writeBufferSize int, writeBufferNum int, minWriteBuffToMerge int, blockCacheSize uint64) (p RocksdbManager) {
	p = &PerDiskRocksdbManager{
		writeBufferSize:     writeBufferSize,
		writeBufferNum:      writeBufferNum,
		minWriteBuffToMerge: minWriteBuffToMerge,
		blockCacheSize:      blockCacheSize,
		dbs:                 make(map[string]*RocksdbHandle),
	}
	return
}

func NewPerPartitionRocksdbManager(writeBufferSize int, writeBufferNum int, minWriteBuffToMerge int, blockCacheSize uint64) (p RocksdbManager) {
	p = &PerPartitionRocksdbManager{
		writeBufferSize:     writeBufferSize,
		writeBufferNum:      writeBufferNum,
		minWriteBuffToMerge: minWriteBuffToMerge,
		blockCacheSize:      blockCacheSize,
		dbs:                 make(map[string]interface{}),
		partitionCnt:        make(map[string]int),
	}
	return
}

type RocksdbMode int

const (
	PerDiskRocksdbMode      RocksdbMode = 0
	PerPartitionRocksdbMode RocksdbMode = iota
)

const DefaultRocksdbMode = PerDiskRocksdbMode

func ParseRocksdbMode(option string) (mode RocksdbMode) {
	option = strings.ToLower(option)
	configMap := map[string]RocksdbMode{
		"disk":      PerDiskRocksdbMode,
		"partition": PerPartitionRocksdbMode,
	}
	mode, ok := configMap[option]
	if ok {
		return
	}
	mode = DefaultRocksdbMode
	return
}
