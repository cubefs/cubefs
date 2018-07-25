package raftstore

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/gorocksdb"
)

//#cgo CFLAGS:-I/usr/local/include
//#cgo LDFLAGS:-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
import "C"

type RocksDBStore struct {
	dir string
	db  *gorocksdb.DB
}

func NewRocksDBStore(dir string) (store *RocksDBStore) {
	store = &RocksDBStore{dir: dir}
	err := store.Open()
	if err != nil {
		panic(fmt.Sprintf("Failed to Open rocksDB! err:%v", err.Error()))
	}
	return store
}

func (rs *RocksDBStore) Open() error {
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, rs.dir)
	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return err
	}
	rs.db = db

	return nil

}

func (rs *RocksDBStore) Del(key interface{}) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	defer wb.Clear()
	slice, err := rs.db.Get(ro, []byte(key.(string)))
	if err != nil {
		return
	}
	result = slice.Data()
	err = rs.db.Delete(wo, []byte(key.(string)))
	return
}

func (rs *RocksDBStore) Put(key, value interface{}) (result interface{}, err error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wb.Put([]byte(key.(string)), value.([]byte))
	if err := rs.db.Write(wo, wb); err != nil {
		return nil, err
	}
	result = value
	return result, nil
}

func (rs *RocksDBStore) Get(key interface{}) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	return rs.db.GetBytes(ro, []byte(key.(string)))
}

func (rs *RocksDBStore) DeleteKeyAndPutIndex(key string, cmdMap map[string][]byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	defer wb.Clear()
	wb.Delete([]byte(key))
	for key, value := range cmdMap {
		wb.Put([]byte(key), value)
	}

	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[deleteFromRocksDB],err:%v", err)
		return err
	}
	return nil
}

func (rs *RocksDBStore) BatchPut(cmdMap map[string][]byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	for key, value := range cmdMap {
		wb.Put([]byte(key), value)
	}
	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[batchPutToRocksDB],err:%v", err)
		return err
	}
	return nil
}

func (rs *RocksDBStore) RocksDBSnapshot() *gorocksdb.Snapshot {
	return rs.db.NewSnapshot()
}

func (rs *RocksDBStore) ReleaseSnapshot(snapshot *gorocksdb.Snapshot) {
	rs.db.ReleaseSnapshot(snapshot)
}

func (rs *RocksDBStore) Iterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetSnapshot(snapshot)

	return rs.db.NewIterator(ro)
}
