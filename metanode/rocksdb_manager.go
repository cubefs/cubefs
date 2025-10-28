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
	ErrRocksdbNoResource       = errors.New("rocksdb no resource")
)

type RocksdbManagerConfig struct {
	WriteBufferSize          int    `json:"writeBufferSize"`
	WriteBufferNum           int    `json:"writeBufferNum"`
	MinWriteBuffToMerge      int    `json:"minWriteBuffToMerge"`
	MaxSubCompactions        int    `json:"maxSubCompactions"`
	BlockCacheSize           uint64 `json:"blockCacheSize"`
	EnableStats              bool   `json:"enableStats"`
	BytesPerSync             uint64 `json:"bytesPerSync"`
	Parallelism              int    `json:"parallelism"`
	MaxBackgroundCompactions int    `json:"maxBackgroundCompactions"`
	MaxBackgroundFlushes     int    `json:"maxBackgroundFlushes"`
	SoftCompactionLimit      uint64 `json:"softCompactionLimit"`
	HardCompactionLimit      uint64 `json:"hardCompactionLimit"`
	PeriodicCompactSec       uint64 `json:"periodicCompactionSecond"`
}

type RocksdbManager interface {
	Register(dbPath string) (err error)
	Unregister(dbPath string) (err error)
	OpenRocksdb(dbPath string, metaPartitionId uint64) (db *RocksdbOperator, err error)
	CloseRocksdb(db *RocksdbOperator)
	SelectRocksdbDisk(usableFactor float64) (disk string, err error)
	AttachPartition(dbPath string) (err error)
	DetachPartition(dbPath string) (err error)
	GetPartitionCount(dbPath string) (count int, err error)
	UpdateConfig(dbPath string, config map[string]string) error
	GetConfig(dbPath string) (map[string]string, error)
	SetForbidden(dbPath string, forbidden bool) error
}

type RocksdbHandle struct {
	db         *RocksdbOperator
	rc         uint64
	partitions int
	Forbidden  bool
}

type PerDiskRocksdbManager struct {
	writeBufferSize          int
	writeBufferNum           int
	minWriteBuffToMerge      int
	maxSubCompactions        int
	blockCacheSize           uint64
	enableStats              bool
	bytesPerSync             uint64
	parallelism              int
	maxBackgroundCompactions int
	maxBackgroundFlushes     int
	softCompactionLimit      uint64
	hardCompactionLimit      uint64
	periodicCompactSec       uint64
	mutex                    sync.Mutex
	dbs                      map[string]*RocksdbHandle
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
		opts := &RocksDBOptions{
			Dir:                      dbPath,
			WriteBufferSize:          r.writeBufferSize,
			WriteBufferNum:           r.writeBufferNum,
			MinWriteBuffToMerge:      r.minWriteBuffToMerge,
			MaxSubCompactions:        r.maxSubCompactions,
			BlockCacheSize:           r.blockCacheSize,
			EnableStats:              r.enableStats,
			BytesPerSync:             r.bytesPerSync,
			Parallelism:              r.parallelism,
			MaxBackgroundCompactions: r.maxBackgroundCompactions,
			MaxBackgroundFlushes:     r.maxBackgroundFlushes,
			SoftCompactionLimit:      r.softCompactionLimit,
			HardCompactionLimit:      r.hardCompactionLimit,
			PeriodicCompactSec:       r.periodicCompactSec,
		}
		err = handle.db.OpenDb(opts)
		if err != nil {
			handle.rc -= 1
			return
		}
	}
	db = handle.db
	return
}

func (r *PerDiskRocksdbManager) CloseRocksdb(db *RocksdbOperator) {
	if db == nil {
		return
	}
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
		if handle.Forbidden {
			continue
		}
		var stat diskmon.DiskStat
		stat, err = diskmon.NewDiskStat(dir)
		if err != nil {
			log.LogErrorf("[SelectRocksdbDisk] failed to select rocksdb disk, err(%v)", err)
			return
		}
		stat.PartitionCount = handle.partitions
		stats = append(stats, stat)
	}
	if len(stats) == 0 {
		err = ErrRocksdbNoResource
		return
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
	count = int(handle.rc)
	return
}

func (r *PerDiskRocksdbManager) UpdateConfig(dbPath string, config map[string]string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	handle, ok := r.dbs[dbPath]
	if !ok {
		return ErrUnregisteredRocksdbPath
	}

	if handle.db == nil {
		return ErrRocksdbAccess
	}

	err := handle.db.SetOptions(config)
	if err != nil {
		log.LogErrorf("[UpdateConfig] failed to set rocksdb options, err(%v)", err)
		return err
	}

	return nil
}

func (r *PerDiskRocksdbManager) GetConfig(dbPath string) (map[string]string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	handle, ok := r.dbs[dbPath]
	if !ok {
		return nil, ErrUnregisteredRocksdbPath
	}

	return handle.db.GetOptions(), nil
}

func (r *PerDiskRocksdbManager) SetForbidden(dbPath string, forbidden bool) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	handle, ok := r.dbs[dbPath]
	if !ok {
		return ErrUnregisteredRocksdbPath
	}
	oldValue := handle.Forbidden
	if oldValue == forbidden {
		return nil
	}
	handle.Forbidden = forbidden
	log.LogInfof("[SetForbidden] set rocksdb(%v) forbidden from (%v) to (%v)", dbPath, oldValue, forbidden)
	return nil
}

var _ RocksdbManager = &PerDiskRocksdbManager{}

type RocksdbDirInfo struct {
	Forbidden bool
}

type PerPartitionRocksdbManager struct {
	writeBufferSize          int
	writeBufferNum           int
	minWriteBuffToMerge      int
	maxSubCompactions        int
	blockCacheSize           uint64
	enableStats              bool
	bytesPerSync             uint64
	parallelism              int
	maxBackgroundCompactions int
	maxBackgroundFlushes     int
	softCompactionLimit      uint64
	hardCompactionLimit      uint64
	periodicCompactSec       uint64
	mutex                    sync.Mutex
	partitionCnt             map[string]int
	dbs                      map[string]*RocksdbDirInfo
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
	opts := &RocksDBOptions{
		Dir:                      perPartitionDbDir,
		WriteBufferSize:          r.writeBufferSize,
		WriteBufferNum:           r.writeBufferNum,
		MinWriteBuffToMerge:      r.minWriteBuffToMerge,
		MaxSubCompactions:        r.maxSubCompactions,
		BlockCacheSize:           r.blockCacheSize,
		EnableStats:              r.enableStats,
		BytesPerSync:             r.bytesPerSync,
		Parallelism:              r.parallelism,
		MaxBackgroundCompactions: r.maxBackgroundCompactions,
		MaxBackgroundFlushes:     r.maxBackgroundFlushes,
		SoftCompactionLimit:      r.softCompactionLimit,
		HardCompactionLimit:      r.hardCompactionLimit,
		PeriodicCompactSec:       r.periodicCompactSec,
	}
	err = db.OpenDb(opts)
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
	r.dbs[dbPath] = &RocksdbDirInfo{
		Forbidden: false,
	}
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
		if r.dbs[dir].Forbidden {
			continue
		}
		var stat diskmon.DiskStat
		stat, err = diskmon.NewDiskStat(dir)
		if err != nil {
			log.LogErrorf("[SelectRocksdbDisk] failed to select rocksdb disk, err(%v)", err)
			return
		}
		stat.PartitionCount = r.partitionCnt[dir]
		stats = append(stats, stat)
	}
	if len(stats) == 0 {
		err = ErrRocksdbNoResource
		return
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

func (r *PerPartitionRocksdbManager) UpdateConfig(dbPath string, config map[string]string) error {
	return fmt.Errorf("partition rocksdb manager does not support update config")
}

func (r *PerPartitionRocksdbManager) GetConfig(dbPath string) (map[string]string, error) {
	return nil, fmt.Errorf("partition rocksdb manager does not support get config")
}

func (r *PerPartitionRocksdbManager) SetForbidden(dbPath string, forbidden bool) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	handle, ok := r.dbs[dbPath]
	if !ok {
		return ErrUnregisteredRocksdbPath
	}
	oldValue := handle.Forbidden
	if oldValue == forbidden {
		return nil
	}
	handle.Forbidden = forbidden
	log.LogInfof("[SetForbidden] set rocksdb(%v) forbidden from (%v) to (%v)", dbPath, oldValue, forbidden)
	return nil
}

var _ RocksdbManager = &PerPartitionRocksdbManager{}

func NewPerDiskRocksdbManager(config *RocksdbManagerConfig) (p RocksdbManager) {
	p = &PerDiskRocksdbManager{
		writeBufferSize:          config.WriteBufferSize,
		writeBufferNum:           config.WriteBufferNum,
		minWriteBuffToMerge:      config.MinWriteBuffToMerge,
		maxSubCompactions:        config.MaxSubCompactions,
		blockCacheSize:           config.BlockCacheSize,
		enableStats:              config.EnableStats,
		bytesPerSync:             config.BytesPerSync,
		parallelism:              config.Parallelism,
		maxBackgroundCompactions: config.MaxBackgroundCompactions,
		maxBackgroundFlushes:     config.MaxBackgroundFlushes,
		softCompactionLimit:      config.SoftCompactionLimit,
		hardCompactionLimit:      config.HardCompactionLimit,
		periodicCompactSec:       config.PeriodicCompactSec,
		dbs:                      make(map[string]*RocksdbHandle),
	}
	return
}

func NewPerPartitionRocksdbManager(config *RocksdbManagerConfig) (p RocksdbManager) {
	p = &PerPartitionRocksdbManager{
		writeBufferSize:          config.WriteBufferSize,
		writeBufferNum:           config.WriteBufferNum,
		minWriteBuffToMerge:      config.MinWriteBuffToMerge,
		maxSubCompactions:        config.MaxSubCompactions,
		blockCacheSize:           config.BlockCacheSize,
		enableStats:              config.EnableStats,
		bytesPerSync:             config.BytesPerSync,
		parallelism:              config.Parallelism,
		maxBackgroundCompactions: config.MaxBackgroundCompactions,
		maxBackgroundFlushes:     config.MaxBackgroundFlushes,
		softCompactionLimit:      config.SoftCompactionLimit,
		hardCompactionLimit:      config.HardCompactionLimit,
		periodicCompactSec:       config.PeriodicCompactSec,
		dbs:                      make(map[string]*RocksdbDirInfo),
		partitionCnt:             make(map[string]int),
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
