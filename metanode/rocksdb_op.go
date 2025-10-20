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
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tecbot/gorocksdb"
)

const (
	DefaultCacheSize                = 256 * util.MB
	DefaultWriteBuffSize            = 256 * util.MB
	DefaultWriteBuffNum             = 4
	DefaultMinWriteBuffToMerge      = 4
	DefaultMaxSubCompaction         = 4
	DefaultRetryCount               = 3
	DefaultMaxLogFileSize           = 1 * util.MB
	DefaultLogFileRollTime          = 3 * 24 * time.Hour // NOTE: 3 day
	DefaultKeepLogFileNum           = 3
	ReadTierAll                     = 0
	ReadTierBlockCache              = 1
	ReadTierPersisted               = 2
	ReadTierMemtable                = 3
	DefaultBytesPerSync             = 1 * util.MB
	DefaultParallelism              = 4
	DefaultMaxBackgroundCompactions = 4
	DefaultMaxBackgroundFlushes     = 4
	DefaultSoftCompactionLimit      = 512 * util.GB
	DefaultHardCompactionLimit      = 2 * util.TB
	DefaultBlockSize                = 16 * util.KB
)

var (
	ErrRocksdbAccess             = errors.New("access rocksdb error")
	ErrRocksdbOperation          = errors.New("rocksdb operation error")
	ErrInvalidRocksdbWriteHandle = errors.New("invalid rocksdb write batch")
	ErrInvalidRocksdbTableType   = errors.New("invalid rocksdb table type")
	ErrInvalidRocksdbSnapshot    = errors.New("invalid rocksdb snapshot")
)

type TableType byte

const (
	BaseInfoTable TableType = iota
	DentryTable
	InodeTable
	ExtendTable
	MultipartTable
	TransactionTable
	TransactionRollbackInodeTable
	TransactionRollbackDentryTable
	DeletedExtentsTable
	DeletedObjExtentsTable
	MaxTable
)

const (
	FlushInterval = 30 * time.Second
)

func getTableTypeKey(treeType TreeType) TableType {
	switch treeType {
	case InodeType:
		return InodeTable
	case DentryType:
		return DentryTable
	case MultipartType:
		return MultipartTable
	case ExtendType:
		return ExtendTable
	case TransactionType:
		return TransactionTable
	case TransactionRollbackInodeType:
		return TransactionRollbackInodeTable
	case TransactionRollbackDentryType:
		return TransactionRollbackDentryTable
	case DeletedExtentsType:
		return DeletedExtentsTable
	case DeletedObjExtentsType:
		return DeletedObjExtentsTable
	default:
	}
	panic(ErrInvalidRocksdbTableType)
}

const (
	dbInitSt uint32 = iota
	dbOpenningSt
	dbOpenedSt
	dbClosingSt
	dbClosedSt
)

func isRetryError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "Try again") {
		return true
	}
	return false
}

type RocksdbOperator struct {
	dir   string
	db    *gorocksdb.DB
	mutex sync.RWMutex
	state uint32

	readOption  *gorocksdb.ReadOptions
	writeOption *gorocksdb.WriteOptions
	openOption  *gorocksdb.Options
	cache       *gorocksdb.Cache
	tableOption *gorocksdb.BlockBasedTableOptions

	readDiskOption *gorocksdb.ReadOptions
	config         map[string]string

	isFlushing    bool
	lastFlushTime time.Time
	flushMutex    sync.Mutex
}

func NewRocksdb() (operator *RocksdbOperator) {
	operator = &RocksdbOperator{
		state:         dbInitSt,
		config:        make(map[string]string),
		isFlushing:    false,
		lastFlushTime: time.Now(),
	}
	return
}

func (db *RocksdbOperator) CloseDb() (err error) {
	log.LogDebugf("close RocksDB, Path(%s), State(%v)", db.dir, atomic.LoadUint32(&db.state))

	if ok := atomic.CompareAndSwapUint32(&db.state, dbOpenedSt, dbClosingSt); !ok {
		if atomic.LoadUint32(&db.state) == dbClosedSt {
			// already closed
			return nil
		}
		return fmt.Errorf("db state error, cur: %v, to:%v", db.state, dbClosingSt)
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()
	defer atomic.CompareAndSwapUint32(&db.state, dbClosingSt, dbClosedSt)

	db.db.Close()
	db.readOption.Destroy()
	db.writeOption.Destroy()
	db.openOption.Destroy()
	db.cache.Destroy()
	db.tableOption.Destroy()
	db.readDiskOption.Destroy()

	db.db = nil
	db.readOption = nil
	db.writeOption = nil
	db.openOption = nil
	db.tableOption = nil
	db.cache = nil
	db.readDiskOption = nil
	return
}

func (db *RocksdbOperator) GetStatistics() string {
	if db.openOption != nil {
		return db.openOption.GetStatisticsString()
	}
	return ""
}

type RocksDBOptions struct {
	Dir                      string
	WriteBufferSize          int
	WriteBufferNum           int
	MinWriteBuffToMerge      int
	MaxSubCompactions        int
	BlockCacheSize           uint64
	MaxLogFileSize           int
	LogFileTimeToRoll        time.Duration
	KeepLogFileNum           int
	EnableStats              bool
	BytesPerSync             uint64
	Parallelism              int
	MaxBackgroundCompactions int
	MaxBackgroundFlushes     int
	SoftCompactionLimit      uint64
	HardCompactionLimit      uint64
	PeriodicCompactSec       uint64
}

func (dbInfo *RocksdbOperator) newRocksdbOptions(opts *RocksDBOptions) (
	dbOpts *gorocksdb.Options,
	cache *gorocksdb.Cache,
	tableOpts *gorocksdb.BlockBasedTableOptions,
) {
	dbOpts = gorocksdb.NewDefaultOptions()

	// NOTE: check and set default options
	if opts.WriteBufferSize == 0 {
		opts.WriteBufferSize = DefaultWriteBuffSize
	}
	if opts.WriteBufferNum == 0 {
		opts.WriteBufferNum = DefaultWriteBuffNum
	}
	if opts.MinWriteBuffToMerge == 0 {
		opts.MinWriteBuffToMerge = DefaultMinWriteBuffToMerge
	}
	if opts.MaxSubCompactions == 0 {
		opts.MaxSubCompactions = DefaultMaxSubCompaction
	}
	if opts.MaxLogFileSize == 0 {
		opts.MaxLogFileSize = DefaultMaxLogFileSize
	}
	if opts.LogFileTimeToRoll == 0 {
		opts.LogFileTimeToRoll = DefaultLogFileRollTime
	}
	if opts.KeepLogFileNum == 0 {
		opts.KeepLogFileNum = DefaultKeepLogFileNum
	}
	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = DefaultCacheSize
	}
	if opts.BytesPerSync == 0 {
		opts.BytesPerSync = DefaultBytesPerSync
	}
	if opts.Parallelism == 0 {
		opts.Parallelism = DefaultParallelism
	}
	if opts.MaxBackgroundCompactions == 0 {
		opts.MaxBackgroundCompactions = DefaultMaxBackgroundCompactions
	}
	if opts.MaxBackgroundFlushes == 0 {
		opts.MaxBackgroundFlushes = DefaultMaxBackgroundFlushes
	}
	if opts.SoftCompactionLimit == 0 {
		opts.SoftCompactionLimit = DefaultSoftCompactionLimit
	}
	if opts.HardCompactionLimit == 0 {
		opts.HardCompactionLimit = DefaultHardCompactionLimit
	}

	// NOTE: main options
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetWriteBufferSize(opts.WriteBufferSize)
	dbOpts.SetMaxWriteBufferNumber(opts.WriteBufferNum)
	dbOpts.SetCompression(gorocksdb.NoCompression)
	dbOpts.SetMinWriteBufferNumberToMerge(opts.MinWriteBuffToMerge)
	dbOpts.SetLevelCompactionDynamicLevelBytes(true)
	dbOpts.SetTargetFileSizeMultiplier(2)
	tableOpts = gorocksdb.NewDefaultBlockBasedTableOptions()
	cache = gorocksdb.NewLRUCache(opts.BlockCacheSize)
	tableOpts.SetBlockCache(cache)
	tableOpts.SetCacheIndexAndFilterBlocks(true)
	tableOpts.SetPinL0FilterAndIndexBlocksInCache(true)
	tableOpts.SetFilterPolicy(gorocksdb.NewBloomFilter(10))
	tableOpts.SetBlockSize(DefaultBlockSize)
	// from SetFormatVersion comments, it's better to use 3.
	tableOpts.SetFormatVersion(3)
	dbOpts.SetBlockBasedTableFactory(tableOpts)

	// NOTE: rocksdb log file options
	dbOpts.SetMaxLogFileSize(opts.MaxLogFileSize)
	dbOpts.SetLogFileTimeToRoll(int(opts.LogFileTimeToRoll.Seconds()))
	dbOpts.SetKeepLogFileNum(opts.KeepLogFileNum)

	if opts.EnableStats {
		dbOpts.EnableStatistics()
	}
	dbOpts.SetMaxSubCompactions(opts.MaxSubCompactions)
	dbOpts.SetBytesPerSync(opts.BytesPerSync)
	dbOpts.IncreaseParallelism(opts.Parallelism)
	dbOpts.SetMaxBackgroundCompactions(opts.MaxBackgroundCompactions)
	dbOpts.SetMaxBackgroundFlushes(opts.MaxBackgroundFlushes)
	dbOpts.SetSoftPendingCompactionBytesLimit(opts.SoftCompactionLimit)
	dbOpts.SetHardPendingCompactionBytesLimit(opts.HardCompactionLimit)
	return
}

func (dbInfo *RocksdbOperator) doOpen(opts *RocksDBOptions) (err error) {
	var stat fs.FileInfo

	stat, err = os.Stat(opts.Dir)
	if err == nil && !stat.IsDir() {
		log.LogErrorf("interOpenDb path:[%s] is not dir", opts.Dir)
		return fmt.Errorf("path:[%s] is not dir", opts.Dir)
	}

	if err != nil && !os.IsNotExist(err) {
		log.LogErrorf("interOpenDb stat error: dir: %v, err: %v", opts.Dir, err)
		return err
	}

	// NOTE: mkdir all  will return nil when path exist and path is dir
	if err = os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.LogErrorf("interOpenDb mkdir error: dir: %v, err: %v", opts.Dir, err)
		return err
	}

	log.LogInfof("[doOpen] rocksdb dir(%v)", opts.Dir)
	dbInfo.openOption, dbInfo.cache, dbInfo.tableOption = dbInfo.newRocksdbOptions(opts)

	dbInfo.db, err = gorocksdb.OpenDb(dbInfo.openOption, opts.Dir)

	if err != nil {
		log.LogErrorf("interOpenDb open db err:%v", err)
		if dbInfo.openOption != nil {
			dbInfo.openOption.Destroy()
			dbInfo.openOption = nil
		}
		if dbInfo.cache != nil {
			dbInfo.cache.Destroy()
			dbInfo.cache = nil
		}
		if dbInfo.tableOption != nil {
			dbInfo.tableOption.Destroy()
			dbInfo.tableOption = nil
		}
		return ErrRocksdbOperation
	}
	dbInfo.dir = opts.Dir
	dbInfo.readOption = gorocksdb.NewDefaultReadOptions()
	dbInfo.writeOption = gorocksdb.NewDefaultWriteOptions()
	// NOTE: we use raft wal, enable rocksdb wal is unnecessary
	dbInfo.writeOption.DisableWAL(true)
	dbInfo.readDiskOption = gorocksdb.NewDefaultReadOptions()
	dbInfo.readDiskOption.SetReadTier(ReadTierPersisted)
	dbInfo.readDiskOption.SetFillCache(false)
	dbInfo.setOptToConfig(opts)
	if opts.PeriodicCompactSec > 0 {
		err = dbInfo.db.SetOptions([]string{"periodic_compaction_seconds"}, []string{strconv.FormatUint(opts.PeriodicCompactSec, 10)})
		if err != nil {
			err = fmt.Errorf("set option [periodic_compaction_seconds=%d] failed: %v", opts.PeriodicCompactSec, err)
			log.LogErrorf(err.Error())
			return err
		}
	}

	return nil
}

func NewDefaultRocksDBOptions(dir string) *RocksDBOptions {
	return &RocksDBOptions{
		Dir:                 dir,
		WriteBufferSize:     DefaultWriteBuffSize,
		WriteBufferNum:      DefaultWriteBuffNum,
		MinWriteBuffToMerge: DefaultMinWriteBuffToMerge,
		MaxSubCompactions:   DefaultMaxSubCompaction,
		BlockCacheSize:      DefaultCacheSize,
		MaxLogFileSize:      DefaultMaxLogFileSize,
		LogFileTimeToRoll:   DefaultLogFileRollTime,
		KeepLogFileNum:      DefaultKeepLogFileNum,
		SoftCompactionLimit: DefaultSoftCompactionLimit,
		HardCompactionLimit: DefaultHardCompactionLimit,
	}
}

func (dbInfo *RocksdbOperator) OpenDb(opts *RocksDBOptions) (err error) {
	ok := atomic.CompareAndSwapUint32(&dbInfo.state, dbInitSt, dbOpenningSt)
	ok = ok || atomic.CompareAndSwapUint32(&dbInfo.state, dbClosedSt, dbOpenningSt)
	if !ok {
		if atomic.LoadUint32(&dbInfo.state) == dbOpenedSt {
			// already opened
			return nil
		}
		return fmt.Errorf("db state error, cur: %v, to:%v", dbInfo.state, dbOpenningSt)
	}

	dbInfo.mutex.Lock()
	defer func() {
		if err == nil {
			atomic.CompareAndSwapUint32(&dbInfo.state, dbOpenningSt, dbOpenedSt)
		} else {
			log.LogErrorf("OpenDb failed, dir:%s error:%v", opts.Dir, err)
			atomic.CompareAndSwapUint32(&dbInfo.state, dbOpenningSt, dbInitSt)
		}
		dbInfo.mutex.Unlock()
	}()

	return dbInfo.doOpen(opts)
}

func (dbInfo *RocksdbOperator) ReOpenDb(opts *RocksDBOptions) (err error) {
	if ok := atomic.CompareAndSwapUint32(&dbInfo.state, dbClosedSt, dbOpenningSt); !ok {
		if atomic.LoadUint32(&dbInfo.state) == dbOpenedSt {
			// already opened
			return nil
		}
		return fmt.Errorf("db state error, cur: %v, to:%v", dbInfo.state, dbOpenningSt)
	}

	dbInfo.mutex.Lock()
	defer func() {
		if err == nil {
			atomic.CompareAndSwapUint32(&dbInfo.state, dbOpenningSt, dbOpenedSt)
		} else {
			atomic.CompareAndSwapUint32(&dbInfo.state, dbOpenningSt, dbClosedSt)
		}
		dbInfo.mutex.Unlock()
	}()

	if dbInfo == nil || (dbInfo.dir != "" && dbInfo.dir != opts.Dir) {
		return fmt.Errorf("rocks db dir changed, need new db instance")
	}

	return dbInfo.doOpen(opts)
}

func genRocksDBReadOption(snap *gorocksdb.Snapshot) (ro *gorocksdb.ReadOptions) {
	ro = gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetSnapshot(snap)
	return
}

func (dbInfo *RocksdbOperator) iterator(ro *gorocksdb.ReadOptions) *gorocksdb.Iterator {
	return dbInfo.db.NewIterator(ro)
}

func (dbInfo *RocksdbOperator) rangeWithIter(it *gorocksdb.Iterator, start []byte, end []byte, cb func(k, v []byte) (bool, error)) error {
	it.Seek(start)
	for ; it.ValidForPrefix(start); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		if bytes.Compare(end, key) < 0 {
			break
		}
		if hasNext, err := cb(key, value); err != nil {
			log.LogErrorf("[RocksDB Op] RangeWithIter key: %v value: %v err: %v", key, value, err)
			return err
		} else if !hasNext {
			return nil
		}
	}
	return nil
}

func (dbInfo *RocksdbOperator) rangeWithIterByPrefix(it *gorocksdb.Iterator, prefix, start, end []byte, cb func(k, v []byte) (bool, error)) error {
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		if bytes.Compare(key, start) < 0 {
			continue
		}
		if bytes.Compare(end, key) < 0 {
			break
		}
		if hasNext, err := cb(key, value); err != nil {
			log.LogErrorf("[RocksTree] RangeWithIter key: %v value: %v err: %v", key, value, err)
			return err
		} else if !hasNext {
			return nil
		}
	}
	return nil
}

func (dbInfo *RocksdbOperator) descRangeWithIter(it *gorocksdb.Iterator, start []byte, end []byte, cb func(k, v []byte) (bool, error)) error {
	it.SeekForPrev(end)
	for ; it.ValidForPrefix(start); it.Prev() {
		key := it.Key().Data()
		value := it.Value().Data()
		if bytes.Compare(key, start) < 0 {
			break
		}
		if hasNext, err := cb(key, value); err != nil {
			log.LogErrorf("[RocksDB Op] RangeWithIter key: %v value: %v err: %v", key, value, err)
			return err
		} else if !hasNext {
			return nil
		}
	}
	return nil
}

func (dbInfo *RocksdbOperator) accessDb() error {
	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		log.LogErrorf("[RocksDB Op] can not access db, db is not opened. Cur state:%v", dbInfo.state)
		return ErrRocksdbAccess
	}

	dbInfo.mutex.RLock()
	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		dbInfo.mutex.RUnlock()
		log.LogErrorf("[RocksDB Op] can not access db, db is not opened. Cur state:%v", dbInfo.state)
		return ErrRocksdbAccess
	}
	return nil
}

func (dbInfo *RocksdbOperator) releaseDb() {
	dbInfo.mutex.RUnlock()
}

// NOTE: hold the lock while using snapshot
func (dbInfo *RocksdbOperator) OpenSnap() *gorocksdb.Snapshot {
	if err := dbInfo.accessDb(); err != nil {
		log.LogErrorf("[RocksDB Op] OpenSnap failed:%v", err)
		return nil
	}

	snap := dbInfo.db.NewSnapshot()
	if snap == nil {
		dbInfo.releaseDb()
	}
	return snap
}

func (dbInfo *RocksdbOperator) ReleaseSnap(snap *gorocksdb.Snapshot) {
	if snap == nil {
		return
	}
	defer dbInfo.releaseDb()

	dbInfo.db.ReleaseSnapshot(snap)
}

func (dbInfo *RocksdbOperator) RangeWithSnap(start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte) (bool, error)) (err error) {
	if snap == nil {
		return ErrInvalidRocksdbSnapshot
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	ro := genRocksDBReadOption(snap)
	it := dbInfo.iterator(ro)
	defer func() {
		it.Close()
		ro.Destroy()
	}()
	return dbInfo.rangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksdbOperator) GetBytesWithSnap(snap *gorocksdb.Snapshot, key []byte) (value []byte, err error) {
	if snap == nil {
		err = ErrInvalidRocksdbSnapshot
		return
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	ro := genRocksDBReadOption(snap)
	defer ro.Destroy()
	for index := 0; index < DefaultRetryCount; {
		value, err = dbInfo.db.GetBytes(ro, key)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("[RocksDB Op] GetBytes failed, error(%v)", err)
			break
		}
		log.LogErrorf("[RocksDB Op] GetBytes failed with retry error(%v), continue", err)
		index++
	}
	if err != nil {
		log.LogErrorf("[RocksDB Op] GetBytes err:%v", err)
		err = ErrRocksdbOperation
		return
	}
	return
}

func (dbInfo *RocksdbOperator) RangeWithSnapByPrefix(prefix, start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte) (bool, error)) (err error) {
	if snap == nil {
		return ErrInvalidRocksdbSnapshot
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	ro := genRocksDBReadOption(snap)
	it := dbInfo.iterator(ro)
	defer func() {
		it.Close()
		ro.Destroy()
	}()
	return dbInfo.rangeWithIterByPrefix(it, prefix, start, end, cb)
}

func (dbInfo *RocksdbOperator) DescRangeWithSnap(start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte) (bool, error)) (err error) {
	if snap == nil {
		return ErrInvalidRocksdbSnapshot
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	ro := genRocksDBReadOption(snap)
	it := dbInfo.iterator(ro)
	defer func() {
		it.Close()
		ro.Destroy()
	}()
	return dbInfo.descRangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksdbOperator) Range(start, end []byte, cb func(k, v []byte) (bool, error)) (err error) {
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	snapshot := dbInfo.db.NewSnapshot()
	ro := genRocksDBReadOption(snapshot)
	it := dbInfo.iterator(ro)
	defer func() {
		it.Close()
		ro.Destroy()
		dbInfo.db.ReleaseSnapshot(snapshot)
	}()
	return dbInfo.rangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksdbOperator) DescRange(start, end []byte, cb func(k, v []byte) (bool, error)) (err error) {
	if err = dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()

	snapshot := dbInfo.db.NewSnapshot()
	ro := genRocksDBReadOption(snapshot)
	it := dbInfo.iterator(ro)
	defer func() {
		it.Close()
		ro.Destroy()
		dbInfo.db.ReleaseSnapshot(snapshot)
	}()
	return dbInfo.descRangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksdbOperator) GetBytes(key []byte) (bytes []byte, err error) {
	if err = dbInfo.accessDb(); err != nil {
		log.LogErrorf("[RocksDB Op] GetBytes failed, error:%v", err)
		return
	}
	defer dbInfo.releaseDb()
	for index := 0; index < DefaultRetryCount; {
		bytes, err = dbInfo.db.GetBytes(dbInfo.readOption, key)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("[RocksDB Op] GetBytes failed, error(%v)", err)
			break
		}
		log.LogErrorf("[RocksDB Op] GetBytes failed with retry error(%v), continue", err)
		index++
	}
	if err != nil {
		log.LogErrorf("[RocksDB Op] GetBytes err:%v", err)
		err = ErrRocksdbOperation
		return
	}
	return
}

func (dbInfo *RocksdbOperator) HasKey(key []byte) (bool, error) {
	bs, err := dbInfo.GetBytes(key)
	if err != nil {
		return false, err
	}
	return len(bs) > 0, nil
}

func (dbInfo *RocksdbOperator) Put(key, value []byte) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] Put failed, error:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()
	for index := 0; index < DefaultRetryCount; {
		err = dbInfo.db.Put(dbInfo.writeOption, key, value)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("[RocksDB Op] Put failed, error(%v)", err)
			break
		}
		log.LogErrorf("[RocksDB Op] Put failed with retry error(%v), continue", err)
		index++
	}
	if err != nil {
		log.LogErrorf("[RocksDB Op] Put err:%v", err)
		err = ErrRocksdbOperation
		return
	}
	return
}

func (dbInfo *RocksdbOperator) Del(key []byte) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] Del failed, error:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()
	for index := 0; index < DefaultRetryCount; {
		err = dbInfo.db.Delete(dbInfo.writeOption, key)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("[RocksDB Op] Del failed, error(%v)", err)
			break
		}
		log.LogErrorf("[RocksDB Op] Del failed with retry error(%v), continue", err)
		index++
	}
	if err != nil {
		log.LogErrorf("[RocksDB Op] Del err:%v", err)
		err = ErrRocksdbOperation
		return
	}
	return
}

func (dbInfo *RocksdbOperator) CreateBatchHandler() (interface{}, error) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] CreateBatchHandler failed, error:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return nil, err
	}
	defer dbInfo.releaseDb()
	batch := gorocksdb.NewWriteBatch()
	return batch, nil
}

func (dbInfo *RocksdbOperator) AddItemToBatch(handle interface{}, key, value []byte) (err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return ErrInvalidRocksdbWriteHandle
	}

	batch.Put(key, value)
	return nil
}

func (dbInfo *RocksdbOperator) DelItemToBatch(handle interface{}, key []byte) (err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return ErrInvalidRocksdbWriteHandle
	}

	batch.Delete(key)
	return nil
}

func (dbInfo *RocksdbOperator) DelRangeToBatch(handle interface{}, start []byte, end []byte) (err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return ErrInvalidRocksdbWriteHandle
	}

	batch.DeleteRange(start, end)
	return nil
}

func (dbInfo *RocksdbOperator) CommitBatchAndRelease(handle interface{}) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] CommitBatchAndRelease failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = ErrInvalidRocksdbWriteHandle
		return
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	for index := 0; index < DefaultRetryCount; {
		err = dbInfo.db.Write(dbInfo.writeOption, batch)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("[RocksDB Op] CommitBatchAndRelease write failed, error(%v)", err)
			break
		}
		log.LogErrorf("[RocksDB Op] CommitBatchAndRelease write failed with retry error(%v), continue", err)
		index++
	}
	batch.Destroy()
	if err != nil {
		log.LogErrorf("[RocksDB Op] CommitBatchAndRelease write failed:%v", err)
		err = ErrRocksdbOperation
		return
	}
	return
}

func (dbInfo *RocksdbOperator) HandleBatchCount(handle interface{}) (count int, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] CommitBatchAndRelease failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = ErrInvalidRocksdbWriteHandle
		return
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	count = batch.Count()
	return
}

func (dbInfo *RocksdbOperator) CommitBatch(handle interface{}) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] CommitBatch failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = ErrInvalidRocksdbWriteHandle
		return
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	for index := 0; index < DefaultRetryCount; {
		err = dbInfo.db.Write(dbInfo.writeOption, batch)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("[RocksDB Op] CommitBatch write failed, error(%v)", err)
			break
		}
		log.LogErrorf("[RocksDB Op] CommitBatch write failed with retry error(%v), continue", err)
		index++
	}
	if err != nil {
		log.LogErrorf("[RocksDB Op] CommitBatch write failed, error(%v)", err)
		err = ErrRocksdbOperation
		return
	}
	return
}

func (dbInfo *RocksdbOperator) ReleaseBatchHandle(handle interface{}) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] ReleaseBatchHandle failed, err:%v", err)
		}
	}()

	if handle == nil {
		return
	}

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = ErrInvalidRocksdbWriteHandle
		return
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	batch.Destroy()
	return
}

func (dbInfo *RocksdbOperator) ClearBatchWriteHandle(handle interface{}) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] ClearBatchWriteHandle failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = ErrInvalidRocksdbWriteHandle
		return
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	batch.Clear()
	return
}

func (dbInfo *RocksdbOperator) CompactRange(start, end []byte) (err error) {
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	dbInfo.db.CompactRange(gorocksdb.Range{
		Start: start,
		Limit: end,
	})
	return
}

func (dbInfo *RocksdbOperator) Flush(block bool) (err error) {
	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		log.LogErrorf("[RocksDB Op] can not access db, db is not opened. Cur state:%v", dbInfo.state)
		return ErrRocksdbAccess
	}

	dbInfo.flushMutex.Lock()
	if dbInfo.isFlushing || (time.Since(dbInfo.lastFlushTime) < FlushInterval) {
		dbInfo.flushMutex.Unlock()
		return nil
	}

	dbInfo.isFlushing = true
	dbInfo.flushMutex.Unlock()

	opts := gorocksdb.NewDefaultFlushOptions()
	opts.SetWait(block)
	defer func() {
		opts.Destroy()
		dbInfo.flushMutex.Lock()
		dbInfo.isFlushing = false
		if err == nil {
			dbInfo.lastFlushTime = time.Now()
		}
		dbInfo.flushMutex.Unlock()
	}()

	err = dbInfo.db.Flush(opts)
	if err != nil {
		return
	}

	return
}

func (dbInfo *RocksdbOperator) GetBytesFromDisk(key []byte) (bytes []byte, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] GetBytes failed, error:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	for index := 0; index < DefaultRetryCount; {
		bytes, err = dbInfo.db.GetBytes(dbInfo.readDiskOption, key)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("[RocksDB Op] GetBytes failed, error(%v)", err)
			break
		}
		log.LogErrorf("[RocksDB Op] GetBytes failed with retry error(%v), continue", err)
		index++
	}
	if err != nil {
		log.LogErrorf("[RocksDB Op] GetBytes err:%v", err)
		err = ErrRocksdbOperation
		return
	}
	return
}

func (dbInfo *RocksdbOperator) SetOptions(config map[string]string) error {
	if dbInfo.db == nil {
		return ErrRocksdbAccess
	}

	dbInfo.mutex.Lock()
	defer dbInfo.mutex.Unlock()

	for key, val := range config {
		err := dbInfo.db.SetOptions([]string{key}, []string{val})
		if err != nil {
			err = fmt.Errorf("set option [%s=%s] failed: %v", key, val, err)
			log.LogErrorf(err.Error())
			return err
		}
		dbInfo.config[key] = val
	}

	return nil
}

func (dbInfo *RocksdbOperator) GetOptions() map[string]string {
	dbInfo.mutex.RLock()
	defer dbInfo.mutex.RUnlock()

	ret := make(map[string]string)
	for key, val := range dbInfo.config {
		ret[key] = val
	}
	return ret
}

func (dbInfo *RocksdbOperator) setOptToConfig(opts *RocksDBOptions) {
	dbInfo.config["write_buffer_size"] = strconv.Itoa(opts.WriteBufferSize)
	dbInfo.config["max_write_buffer_number"] = strconv.Itoa(opts.WriteBufferNum)
	dbInfo.config["min_write_buffer_number_to_merge"] = strconv.Itoa(opts.MinWriteBuffToMerge)
	dbInfo.config["max_subcompactions"] = strconv.Itoa(opts.MaxSubCompactions)
	dbInfo.config["bytes_per_sync"] = strconv.FormatUint(opts.BytesPerSync, 10)
	dbInfo.config["max_background_compactions"] = strconv.Itoa(opts.MaxBackgroundCompactions)
	dbInfo.config["max_background_flushes"] = strconv.Itoa(opts.MaxBackgroundFlushes)
	dbInfo.config["periodic_compaction_seconds"] = strconv.FormatUint(opts.PeriodicCompactSec, 10)
}

func (dbInfo *RocksdbOperator) GetApproximateSizes(start, end []byte) (size uint64, err error) {
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	sizelist := dbInfo.db.GetApproximateSizes([]gorocksdb.Range{
		{
			Start: start,
			Limit: end,
		},
	})
	return sizelist[0], nil
}

func (dbInfo *RocksdbOperator) GetLevelNum() (int, error) {
	if err := dbInfo.accessDb(); err != nil {
		return 0, err
	}
	defer dbInfo.releaseDb()

	liveFiles := dbInfo.db.GetLiveFilesMetaData()
	levelNum := 0
	for _, file := range liveFiles {
		if file.Level > levelNum {
			levelNum = file.Level
		}
	}

	return levelNum, nil
}

func (dbInfo *RocksdbOperator) GetLevelNumMap() (map[string]int, error) {
	if err := dbInfo.accessDb(); err != nil {
		return nil, err
	}
	defer dbInfo.releaseDb()

	liveFiles := dbInfo.db.GetLiveFilesMetaData()
	ret := make(map[string]int)
	for _, file := range liveFiles {
		ret[file.Name] = file.Level
	}

	return ret, nil
}

func (dbInfo *RocksdbOperator) GetProperty(property string) (string, error) {
	if err := dbInfo.accessDb(); err != nil {
		return "", err
	}
	defer dbInfo.releaseDb()

	return dbInfo.db.GetProperty(property), nil
}
