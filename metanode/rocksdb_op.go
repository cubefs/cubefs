package metanode

import (
	"bytes"
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tecbot/gorocksdb"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
)

const(
	DefLRUCacheSize  = 256 * util.MB
	DefWriteBuffSize = 4 * util.MB
)
type  TableType byte

const (
	BaseInfoTable TableType = iota
	DentryTable
	InodeTable
	ExtendTable
	MultipartTable
	ExtentDelTable
	InodeDelTable
)

const (
	dbInitSt uint32 = iota
	dbOpenningSt
	dbOpenedSt
	dbClosingSt
	dbClosedSt
)

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
		return fmt.Errorf("path:[%s] is not dir", dir)
	}

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	//mkdir all  will return nil when path exist and path is dir
	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		log.LogInfof("NewRocksTree mkidr error: dir: %v, err: %v", dir, err)
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
	dbInfo.db, err = gorocksdb.OpenDb(opts, dir)
	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return err
	}
	dbInfo.dir = dir
	dbInfo.defReadOption  = gorocksdb.NewDefaultReadOptions()
	dbInfo.defWriteOption = gorocksdb.NewDefaultWriteOptions()
	dbInfo.defFlushOption = gorocksdb.NewDefaultFlushOptions()
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
			log.LogErrorf("[RocksTree] RangeWithIter key: %v value: %v err: %v", key, value, err)
			return err
		} else if !hasNext {
			return nil
		}
	}
	return nil
}

func (dbInfo *RocksDbInfo)accessDb() error {
	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		return fmt.Errorf("can not access db, db is not opened. Cur state:%v", dbInfo.state)
	}

	dbInfo.mutex.RLock()

	if atomic.LoadUint32(&dbInfo.state) != dbOpenedSt {
		return fmt.Errorf("can not access db, db is not opened. Cur state:%v", dbInfo.state)
	}

	return nil
}

func (dbInfo *RocksDbInfo)releaseDb() {
	dbInfo.mutex.RUnlock()
	return
}

func (dbInfo *RocksDbInfo)OpenSnap()*gorocksdb.Snapshot {
	if err := dbInfo.accessDb(); err != nil {
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

// Has checks if the key exists in the btree.
func (dbInfo *RocksDbInfo) GetBytes(key []byte) ([]byte, error) {
	if err := dbInfo.accessDb(); err != nil {
		return nil, err
	}
	defer dbInfo.releaseDb()
	return dbInfo.db.GetBytes(dbInfo.defReadOption, key)
}

// Has checks if the key exists in the btree.
func (dbInfo *RocksDbInfo) HasKey(key []byte) (bool, error) {
	if err := dbInfo.accessDb(); err != nil {
		return false, err
	}
	defer dbInfo.releaseDb()

	bs, err := dbInfo.GetBytes(key)
	if err != nil {
		return false, err
	}
	return len(bs) > 0, nil
}

// Has checks if the key exists in the btree.
func (dbInfo *RocksDbInfo) Put(key, value []byte) error {
	if err := dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()
	return dbInfo.db.Put(dbInfo.defWriteOption, key, value)
}

func (dbInfo *RocksDbInfo) Del(key []byte) error {
	if err := dbInfo.accessDb(); err != nil {
		return err
	}
	defer dbInfo.releaseDb()
	return dbInfo.db.Delete(dbInfo.defWriteOption, key)
}

func (dbInfo *RocksDbInfo) CreateBatchHandler() (interface{}, error) {
	if err := dbInfo.accessDb(); err != nil {
		return nil, err
	}
	defer dbInfo.releaseDb()
	dbInfo.wait.Add(1)
	batch := gorocksdb.NewWriteBatch()
	return batch, nil
}

func (dbInfo *RocksDbInfo) AddItemToBatch(handle interface{}, key, value []byte)(err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}
	batch.Put(key, value)
	return nil
}

func (dbInfo *RocksDbInfo) DelItemToBatch(handle interface{}, key []byte)(err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}
	batch.Delete(key)
	return nil
}

func (dbInfo *RocksDbInfo) CommitBatchAndRelease(handle interface{})(err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}

	err = dbInfo.db.Write(dbInfo.defWriteOption, batch)
	batch.Destroy()
	dbInfo.wait.Done()
	return
}

func (dbInfo *RocksDbInfo) ReleaseBatchHandle(handle interface{})(err error) {
	batch, ok := handle.(*gorocksdb.WriteBatch)
	if !ok {
		return fmt.Errorf("handle is invalid, not write batch")
	}

	batch.Destroy()
	dbInfo.wait.Done()
	return
}
