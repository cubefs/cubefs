package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/tecbot/gorocksdb"
	"io/fs"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	DefLRUCacheSize       = 256 * unit.MB
	DefWriteBuffSize      = 4 * unit.MB
	DefRetryCount         = 3
	DefMaxWriteBatchCount = 1000
	DefWalSizeLimitMB     = 5
	DefWalMemSizeLimitMB  = 2
	DefMaxLogFileSize     = 1 //MB
	DefLogFileRollTimeDay = 3 // 3 day
	DefLogReservedCnt     = 3
	DefWalTTL             = 60 //second
)

var (
	accessDBError = fmt.Errorf("access rocksdb error")
	rocksDBError  = fmt.Errorf("rocksdb operation error")
)

type TableType byte

const (
	BaseInfoTable TableType = iota
	DentryTable
	InodeTable
	ExtendTable
	MultipartTable
	ExtentDelTableV1
	DelDentryTable
	DelInodeTable
	ExtentDelTable
	ReqRecordsTable
)

const (
	SyncWalTable = 255
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
	case DelDentryType:
		return DelDentryTable
	case DelInodeType:
		return DelInodeTable
	default:
	}
	panic("error tree type")
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

func recoverRocksDBPanic() {
	if err := recover(); err != nil {
		log.LogCriticalf("recover panic, stack: %v", string(debug.Stack()))
		exporter.WarningRocksdbError("recover rocksdb panic")
		return
	}
	return
}

type RocksDbInfo struct {
	dir            string
	defReadOption  *gorocksdb.ReadOptions
	defWriteOption *gorocksdb.WriteOptions
	defFlushOption *gorocksdb.FlushOptions
	defSyncOption  *gorocksdb.WriteOptions
	db             *gorocksdb.DB
	mutex          sync.RWMutex
	state          uint32
	syncCnt        uint64
	SyncFlag       bool
}

func NewRocksDb() (dbInfo *RocksDbInfo) {
	dbInfo = &RocksDbInfo{
		state: dbInitSt,
	}
	return
}

func (dbInfo *RocksDbInfo) ReleaseRocksDb() error {
	dbInfo.mutex.Lock()
	defer dbInfo.mutex.Unlock()
	if dbInfo.db != nil || atomic.LoadUint32(&dbInfo.state) != dbClosedSt {
		return fmt.Errorf("rocks db is using, can not release")
	}

	return os.RemoveAll(dbInfo.dir)
}

func (dbInfo *RocksDbInfo) CloseDb() (err error) {
	log.LogDebugf("close RocksDB, Path(%s), State(%v)", dbInfo.dir, atomic.LoadUint32(&dbInfo.state))

	if ok := atomic.CompareAndSwapUint32(&dbInfo.state, dbOpenedSt, dbClosingSt); !ok {
		if atomic.LoadUint32(&dbInfo.state) == dbClosedSt {
			//already closed
			return nil
		}
		return fmt.Errorf("db state error, cur: %v, to:%v", dbInfo.state, dbClosingSt)
	}

	dbInfo.mutex.Lock()
	defer func() {
		if err == nil {
			atomic.CompareAndSwapUint32(&dbInfo.state, dbClosingSt, dbClosedSt)
		} else {
			atomic.CompareAndSwapUint32(&dbInfo.state, dbClosingSt, dbOpenedSt)
		}
		dbInfo.mutex.Unlock()
	}()

	dbInfo.db.Close()
	dbInfo.defReadOption.Destroy()
	dbInfo.defWriteOption.Destroy()
	dbInfo.defFlushOption.Destroy()

	dbInfo.db = nil
	dbInfo.defReadOption = nil
	dbInfo.defWriteOption = nil
	dbInfo.defFlushOption = nil
	return
}
func (dbInfo *RocksDbInfo) CommitEmptyRecordToSyncWal(flushWal bool) {
	defer recoverRocksDBPanic()
	var err error
	dbInfo.syncCnt += 1
	log.LogWarnf("db[%v] start sync, flush wal flag:%v", dbInfo.dir, flushWal)
	if err = dbInfo.accessDb(); err != nil {
		log.LogErrorf("db[%v] sync finished; failed:%v", dbInfo.dir, err)
		return
	}
	defer dbInfo.releaseDb()
	key := make([]byte, 1)
	value := make([]byte, 8)
	key[0] = SyncWalTable
	binary.BigEndian.PutUint64(value, dbInfo.syncCnt)
	dbInfo.defSyncOption.SetSync(dbInfo.SyncFlag)
	dbInfo.db.Put(dbInfo.defSyncOption, key, value)

	if flushWal {
		dbInfo.defFlushOption.SetWait(dbInfo.SyncFlag)
		err = dbInfo.db.Flush(dbInfo.defFlushOption)
	}

	if err != nil {
		log.LogErrorf("db[%v] sync(sync:%v-flush:%v) finished; failed:%v", dbInfo.dir, dbInfo.SyncFlag, flushWal, err)
		return
	}
	log.LogWarnf("db[%v] sync(sync:%v-flush:%v) finished; success", dbInfo.dir, dbInfo.SyncFlag, flushWal)
	return
}

func (dbInfo *RocksDbInfo) interOpenDb(dir string, walFileSize, walMemSize, logFileSize, logReversed, logReversedCnt, walTTL uint64) (err error) {
	var stat fs.FileInfo

	stat, err = os.Stat(dir)
	if err == nil && !stat.IsDir() {
		log.LogErrorf("interOpenDb path:[%s] is not dir", dir)
		return fmt.Errorf("path:[%s] is not dir", dir)
	}

	if err != nil && !os.IsNotExist(err) {
		log.LogErrorf("interOpenDb stat error: dir: %v, err: %v", dir, err)
		return err
	}

	//mkdir all  will return nil when path exist and path is dir
	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		log.LogErrorf("interOpenDb mkdir error: dir: %v, err: %v", dir, err)
		return err
	}

	log.LogInfof("rocks db dir:[%s]", dir)

	//adjust param
	if walFileSize == 0 {
		walFileSize = DefWalSizeLimitMB
	}
	if walMemSize == 0 {
		walMemSize = DefWalMemSizeLimitMB
	}
	if logFileSize == 0 {
		logFileSize = DefMaxLogFileSize
	}
	if logReversed == 0 {
		logReversed = DefLogFileRollTimeDay
	}
	if logReversedCnt == 0 {
		logReversedCnt = DefLogReservedCnt
	}
	if walTTL == 0 {
		walTTL = DefWalTTL
	}

	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(DefLRUCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(DefWriteBuffSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)

	opts.SetWalSizeLimitMb(walFileSize)
	opts.SetMaxTotalWalSize(walMemSize * unit.MB)
	opts.SetMaxLogFileSize(int(logFileSize * unit.MB))
	opts.SetLogFileTimeToRoll(int(logReversed * 60 * 60 * 24))
	opts.SetKeepLogFileNum(int(logReversedCnt))
	opts.SetWALTtlSeconds(walTTL)
	//opts.SetParanoidChecks(true)
	for index := 0; index < DefRetryCount; {
		dbInfo.db, err = gorocksdb.OpenDb(opts, dir)
		if err == nil {
			break
		}
		if !isRetryError(err) {
			log.LogErrorf("interOpenDb open db err:%v", err)
			break
		}
		log.LogErrorf("interOpenDb open db with retry error:%v", err)
		index++
	}
	if err != nil {
		log.LogErrorf("interOpenDb open db err:%v", err)
		return rocksDBError
	}
	dbInfo.dir = dir
	dbInfo.defReadOption = gorocksdb.NewDefaultReadOptions()
	dbInfo.defWriteOption = gorocksdb.NewDefaultWriteOptions()
	dbInfo.defFlushOption = gorocksdb.NewDefaultFlushOptions()
	dbInfo.defSyncOption = gorocksdb.NewDefaultWriteOptions()
	dbInfo.SyncFlag = true
	dbInfo.defSyncOption.SetSync(dbInfo.SyncFlag)

	//dbInfo.defWriteOption.DisableWAL(true)
	return nil
}

func (dbInfo *RocksDbInfo) OpenDb(dir string, walFileSize, walMemSize, logFileSize, logReversed, logReversedCnt, walTTL uint64) (err error) {
	ok := atomic.CompareAndSwapUint32(&dbInfo.state, dbInitSt, dbOpenningSt)
	ok = ok || atomic.CompareAndSwapUint32(&dbInfo.state, dbClosedSt, dbOpenningSt)
	if !ok {
		if atomic.LoadUint32(&dbInfo.state) == dbOpenedSt {
			//already opened
			return nil
		}
		return fmt.Errorf("db state error, cur: %v, to:%v", dbInfo.state, dbOpenningSt)
	}

	dbInfo.mutex.Lock()
	defer func() {
		if err == nil {
			atomic.CompareAndSwapUint32(&dbInfo.state, dbOpenningSt, dbOpenedSt)
		} else {
			log.LogErrorf("OpenDb failed, dir:%s error:%v", dir, err)
			atomic.CompareAndSwapUint32(&dbInfo.state, dbOpenningSt, dbInitSt)
		}
		dbInfo.mutex.Unlock()
	}()

	return dbInfo.interOpenDb(dir, walFileSize, walMemSize, logFileSize, logReversed, logReversedCnt, walTTL)
}

func (dbInfo *RocksDbInfo) ReOpenDb(dir string, walFileSize, walMemSize, logFileSize, logReversed, logReversedCnt, walTTL uint64) (err error) {
	if ok := atomic.CompareAndSwapUint32(&dbInfo.state, dbClosedSt, dbOpenningSt); !ok {
		if atomic.LoadUint32(&dbInfo.state) == dbOpenedSt {
			//already opened
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

	if dbInfo == nil || (dbInfo.dir != "" && dbInfo.dir != dir) {
		return fmt.Errorf("rocks db dir changed, need new db instance")
	}

	return dbInfo.interOpenDb(dir, walFileSize, walMemSize, logFileSize, logReversed, logReversedCnt, walTTL)

}

func (dbInfo *RocksDbInfo) iterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetSnapshot(snapshot)
	return dbInfo.db.NewIterator(ro)
}

func (dbInfo *RocksDbInfo) rangeWithIter(it *gorocksdb.Iterator, start []byte, end []byte, cb func(k, v []byte) (bool, error)) error {
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

func (dbInfo *RocksDbInfo) rangeWithIterByPrefix(it *gorocksdb.Iterator, prefix, start, end []byte, cb func(k, v []byte) (bool, error)) error {
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

func (dbInfo *RocksDbInfo) descRangeWithIter(it *gorocksdb.Iterator, start []byte, end []byte, cb func(k, v []byte) (bool, error)) error {
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

func (dbInfo *RocksDbInfo) accessDb() error {
	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		log.LogErrorf("[RocksDB Op] can not access db, db is not opened. Cur state:%v", dbInfo.state)
		return accessDBError
	}

	dbInfo.mutex.RLock()
	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		dbInfo.mutex.RUnlock()
		log.LogErrorf("[RocksDB Op] can not access db, db is not opened. Cur state:%v", dbInfo.state)
		return accessDBError
	}
	return nil
}

func (dbInfo *RocksDbInfo) releaseDb() {
	dbInfo.mutex.RUnlock()
	return
}

func (dbInfo *RocksDbInfo) OpenSnap() *gorocksdb.Snapshot {
	if err := dbInfo.accessDb(); err != nil {
		log.LogErrorf("[RocksDB Op] OpenSnap failed:%v", err)
		return nil
	}
	defer dbInfo.releaseDb()

	return dbInfo.db.NewSnapshot()
}

func (dbInfo *RocksDbInfo) ReleaseSnap(snap *gorocksdb.Snapshot) {
	defer recoverRocksDBPanic()

	if snap == nil {
		return
	}
	if err := dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	dbInfo.db.ReleaseSnapshot(snap)
	return
}

func (dbInfo *RocksDbInfo) RangeWithSnap(start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte) (bool, error)) (err error) {
	defer recoverRocksDBPanic()
	if snap == nil {
		return fmt.Errorf("invalid snapshot")
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	it := dbInfo.iterator(snap)
	defer func() {
		it.Close()
	}()
	return dbInfo.rangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksDbInfo) RangeWithSnapByPrefix(prefix, start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte) (bool, error)) ( err error) {
	defer recoverRocksDBPanic()
	if snap == nil {
		return fmt.Errorf("invalid snapshot")
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	it := dbInfo.iterator(snap)
	defer func() {
		it.Close()
	}()
	return dbInfo.rangeWithIterByPrefix(it, prefix, start, end, cb)
}

func (dbInfo *RocksDbInfo) DescRangeWithSnap(start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte) (bool, error)) (err error) {
	defer recoverRocksDBPanic()
	if snap == nil {
		return fmt.Errorf("invalid snapshot")
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	it := dbInfo.iterator(snap)
	defer func() {
		it.Close()
	}()
	return dbInfo.descRangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksDbInfo) Range(start, end []byte, cb func(k, v []byte) (bool, error)) (err error) {
	defer recoverRocksDBPanic()
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	snapshot := dbInfo.db.NewSnapshot()
	it := dbInfo.iterator(snapshot)
	defer func() {
		it.Close()
		dbInfo.db.ReleaseSnapshot(snapshot)
	}()
	return dbInfo.rangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksDbInfo) DescRange(start, end []byte, cb func(k, v []byte) (bool, error)) (err error) {
	defer recoverRocksDBPanic()
	if err = dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()

	snapshot := dbInfo.db.NewSnapshot()
	it := dbInfo.iterator(snapshot)
	defer func() {
		it.Close()
		dbInfo.db.ReleaseSnapshot(snapshot)
	}()
	return dbInfo.descRangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksDbInfo) GetBytes(key []byte) (bytes []byte, err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] GetBytes failed, error:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	for index := 0; index < DefRetryCount; {
		bytes, err = dbInfo.db.GetBytes(dbInfo.defReadOption, key)
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
		err = rocksDBError
		return
	}
	return
}

func (dbInfo *RocksDbInfo) HasKey(key []byte) (bool, error) {
	bs, err := dbInfo.GetBytes(key)
	if err != nil {
		return false, err
	}
	return len(bs) > 0, nil
}

func (dbInfo *RocksDbInfo) Put(key, value []byte) (err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] Put failed, error:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()
	for index := 0; index < DefRetryCount; {
		err = dbInfo.db.Put(dbInfo.defWriteOption, key, value)
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
		err = rocksDBError
		return
	}
	return
}

func (dbInfo *RocksDbInfo) Del(key []byte) (err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] Del failed, error:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()
	for index := 0; index < DefRetryCount; {
		err = dbInfo.db.Delete(dbInfo.defWriteOption, key)
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
		err = rocksDBError
		return
	}
	return
}

func (dbInfo *RocksDbInfo) CreateBatchHandler() (interface{}, error) {
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

func (dbInfo *RocksDbInfo) AddItemToBatch(handle interface{}, key, value []byte) (err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	batch.Put(key, value)
	return nil
}

func (dbInfo *RocksDbInfo) DelItemToBatch(handle interface{}, key []byte) (err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	batch.Delete(key)
	return nil
}

func (dbInfo *RocksDbInfo) CommitBatchAndRelease(handle interface{}) (err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] CommitBatchAndRelease failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = fmt.Errorf("handle is invalid, not write batch")
		return
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	for index := 0; index < DefRetryCount; {
		err = dbInfo.db.Write(dbInfo.defWriteOption, batch)
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
	if err != nil {
		log.LogErrorf("[RocksDB Op] CommitBatchAndRelease write failed:%v", err)
		err = rocksDBError
		return
	}
	batch.Destroy()
	return
}

func (dbInfo *RocksDbInfo) HandleBatchCount(handle interface{}) (count int, err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] CommitBatchAndRelease failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = fmt.Errorf("handle is invalid, not write batch")
		return
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	count = batch.Count()
	return
}

func (dbInfo *RocksDbInfo) CommitBatch(handle interface{}) (err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] CommitBatch failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = fmt.Errorf("handle is invalid, not write batch")
		return
	}

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	for index := 0; index < DefRetryCount; {
		err = dbInfo.db.Write(dbInfo.defWriteOption, batch)
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
		err = rocksDBError
		return
	}
	return
}

func (dbInfo *RocksDbInfo) ReleaseBatchHandle(handle interface{}) (err error) {
	defer recoverRocksDBPanic()
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
		err = fmt.Errorf("handle is invalid, not write batch")
		return
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	batch.Destroy()
	return
}

func (dbInfo *RocksDbInfo) ClearBatchWriteHandle(handle interface{}) (err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] ClearBatchWriteHandle failed, err:%v", err)
		}
	}()

	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		err = fmt.Errorf("handle is invalid, not write batch")
		return
	}
	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()
	batch.Clear()
	return
}

func (dbInfo *RocksDbInfo) Flush() (err error) {
	defer recoverRocksDBPanic()
	defer func() {
		if err != nil {
			log.LogErrorf("[RocksDB Op] ClearBatchWriteHandle failed, err:%v", err)
		}
	}()

	if err = dbInfo.accessDb(); err != nil {
		return
	}
	defer dbInfo.releaseDb()

	return dbInfo.db.Flush(dbInfo.defFlushOption)
}
