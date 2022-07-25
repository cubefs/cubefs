package metanode

import (
	"bytes"
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tecbot/gorocksdb"
	"io/fs"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

const(
	DefLRUCacheSize       = 256 * util.MB
	DefWriteBuffSize      = 4 * util.MB
	DefRetryCount         = 3
	DefMaxWriteBatchCount = 1000
)

var (
	accessDBError = fmt.Errorf("access rocksdb error")
	rocksDBError  = fmt.Errorf("rocksdb operation error")
)

type  TableType byte

const (
	BaseInfoTable TableType = iota
	DentryTable
	InodeTable
	ExtendTable
	MultipartTable
	ExtentDelTable
	DelDentryTable
	DelInodeTable
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

type RocksDbInfo struct {
	dir            string
	defReadOption  *gorocksdb.ReadOptions
	defWriteOption *gorocksdb.WriteOptions
	defFlushOption *gorocksdb.FlushOptions
	db             *gorocksdb.DB
	mutex		   sync.RWMutex
	wait 		   sync.WaitGroup
	state  		   uint32
}

func NewRocksDb() (dbInfo *RocksDbInfo){
	dbInfo = &RocksDbInfo{
		state: dbInitSt,
	}
	return
}

func (dbInfo *RocksDbInfo) ReleaseRocksDb() (error){
	dbInfo.mutex.Lock()
	defer dbInfo.mutex.Unlock()
	if dbInfo.db != nil || atomic.LoadUint32(&dbInfo.state) != dbClosedSt{
		return fmt.Errorf("rocks db is using, can not release")
	}

	return os.RemoveAll(dbInfo.dir)
}

func (dbInfo *RocksDbInfo) CloseDb() (err error){

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

	//wait batch or snap release
	dbInfo.wait.Wait()

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

func (dbInfo *RocksDbInfo) interOpenDb(dir string) (err error) {
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

	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(DefLRUCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(DefWriteBuffSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)
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
	dbInfo.defReadOption  = gorocksdb.NewDefaultReadOptions()
	dbInfo.defWriteOption = gorocksdb.NewDefaultWriteOptions()
	dbInfo.defFlushOption = gorocksdb.NewDefaultFlushOptions()
	//dbInfo.defWriteOption.DisableWAL(true)
	return nil
}

func (dbInfo *RocksDbInfo) OpenDb(dir string) (err error){
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

	return dbInfo.interOpenDb(dir)
}

func (dbInfo *RocksDbInfo) ReOpenDb(dir string) (err error){
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

	return dbInfo.interOpenDb(dir)

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

func (dbInfo *RocksDbInfo)accessDb() error {
	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		log.LogErrorf("[RocksDB Op] can not access db, db is not opened. Cur state:%v", dbInfo.state)
		return accessDBError
	}

	dbInfo.mutex.RLock()

	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		log.LogErrorf("[RocksDB Op] can not access db, db is not opened. Cur state:%v", dbInfo.state)
		return accessDBError
	}

	return nil
}

func (dbInfo *RocksDbInfo)releaseDb() {
	dbInfo.mutex.RUnlock()
	return
}

func (dbInfo *RocksDbInfo)OpenSnap() *gorocksdb.Snapshot {
	if err := dbInfo.accessDb(); err != nil {
		log.LogErrorf("[RocksDB Op] OpenSnap failed:%v", err)
		return nil
	}
	defer dbInfo.releaseDb()

	dbInfo.wait.Add(1)
	return dbInfo.db.NewSnapshot()
}

func (dbInfo *RocksDbInfo)ReleaseSnap(snap *gorocksdb.Snapshot) {
	if snap == nil {
		return
	}

	dbInfo.wait.Done()
	dbInfo.db.ReleaseSnapshot(snap)
	return
}

func (dbInfo *RocksDbInfo)RangeWithSnap(start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte)(bool, error)) error  {
	if snap == nil {
		return fmt.Errorf("invalid snapshot")
	}

	it := dbInfo.iterator(snap)
	defer func() {
		it.Close()
	}()
	return dbInfo.rangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksDbInfo)DescRangeWithSnap(start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte)(bool, error)) error  {
	if snap == nil {
		return fmt.Errorf("invalid snapshot")
	}

	it := dbInfo.iterator(snap)
	defer func() {
		it.Close()
	}()
	return dbInfo.descRangeWithIter(it, start, end, cb)
}

func (dbInfo *RocksDbInfo) Range(start, end []byte, cb func(k, v []byte) (bool, error)) error {
	if err := dbInfo.accessDb(); err != nil {
		return err
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

func (dbInfo *RocksDbInfo) DescRange(start, end []byte, cb func(k, v []byte) (bool, error)) error {
	if err := dbInfo.accessDb(); err != nil {
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
	dbInfo.wait.Add(1)
	batch := gorocksdb.NewWriteBatch()
	return batch, nil
}

func (dbInfo *RocksDbInfo) AddItemToBatch(handle interface{}, key, value []byte) (err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}
	batch.Put(key, value)
	return nil
}

func (dbInfo *RocksDbInfo) DelItemToBatch(handle interface{}, key []byte) (err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}
	batch.Delete(key)
	return nil
}

func (dbInfo *RocksDbInfo) CommitBatchAndRelease(handle interface{}) (err error) {
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
	dbInfo.wait.Done()
	return
}

func (dbInfo *RocksDbInfo) HandleBatchCount(handle interface{}) (count int, err error) {
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
	count = batch.Count()
	return
}

func (dbInfo *RocksDbInfo) CommitBatch(handle interface{}) (err error) {
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

func (dbInfo *RocksDbInfo) ReleaseBatchHandle(handle interface{})(err error) {
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

	batch.Destroy()
	dbInfo.wait.Done()
	return
}

func (dbInfo *RocksDbInfo) ClearBatchWriteHandle(handle interface{}) (err error) {
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
	batch.Clear()
	return
}

func (dbInfo *RocksDbInfo) Flush() error {
	return dbInfo.db.Flush(dbInfo.defFlushOption)
}
