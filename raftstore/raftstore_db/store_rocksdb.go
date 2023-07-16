// Copyright 2018 The CubeFS Authors.
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

package raftstore_db

import (
	"fmt"

	"github.com/tecbot/gorocksdb"
	"os"
)

// RocksDBStore is a wrapper of the gorocksdb.DB
type RocksDBStore struct {
	dir string
	db  *gorocksdb.DB
}

// NewRocksDBStore returns a new RocksDB instance.
func NewRocksDBStore(dir string, lruCacheSize, writeBufferSize int) (store *RocksDBStore, err error) {
	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		return
	}
	store = &RocksDBStore{dir: dir}
	if err = store.Open(lruCacheSize, writeBufferSize); err != nil {
		return
	}
	return
}

// Open opens the RocksDB instance.
func (rs *RocksDBStore) Open(lruCacheSize, writeBufferSize int) error {
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(lruCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(writeBufferSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)
	db, err := gorocksdb.OpenDb(opts, rs.dir)
	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return err
	}
	rs.db = db
	return nil

}

// Del deletes a key-value pair.
func (rs *RocksDBStore) Del(key interface{}, isSync bool) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(isSync)
	defer func() {
		wo.Destroy()
		ro.Destroy()
		wb.Destroy()
	}()
	slice, err := rs.db.Get(ro, []byte(key.(string)))
	if err != nil {
		return
	}
	result = slice.Data()
	err = rs.db.Delete(wo, []byte(key.(string)))
	return
}

// Put adds a new key-value pair to the RocksDB.
func (rs *RocksDBStore) Put(key, value interface{}, isSync bool) (result interface{}, err error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(isSync)
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Put([]byte(key.(string)), value.([]byte))
	if err := rs.db.Write(wo, wb); err != nil {
		return nil, err
	}

	result = value
	return result, nil
}

func (rs *RocksDBStore) Flush() (err error) {
	fo := gorocksdb.NewDefaultFlushOptions()
	return rs.db.Flush(fo)
}

// Get returns the value based on the given key.
func (rs *RocksDBStore) Get(key interface{}) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	return rs.db.GetBytes(ro, []byte(key.(string)))
}

// DeleteKeyAndPutIndex deletes the key-value pair based on the given key and put other keys in the cmdMap to RocksDB.
// TODO explain
func (rs *RocksDBStore) DeleteKeyAndPutIndex(key string, cmdMap map[string][]byte, isSync bool) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	wb := gorocksdb.NewWriteBatch()
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Delete([]byte(key))
	for otherKey, value := range cmdMap {
		if otherKey == key {
			continue
		}
		wb.Put([]byte(otherKey), value)
	}

	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[deleteFromRocksDB],err:%v", err)
		return err
	}
	return nil
}

// Put adds a new key-value pair to the RocksDB.
func (rs *RocksDBStore) Replace(key string, value interface{}, isSync bool) (result interface{}, err error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(isSync)
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Delete([]byte(key))
	wb.Put([]byte(key), value.([]byte))
	if err := rs.db.Write(wo, wb); err != nil {
		return nil, err
	}
	result = value
	return result, nil
}

// BatchPut puts the key-value pairs in batch.
func (rs *RocksDBStore) BatchPut(cmdMap map[string][]byte, isSync bool) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	wb := gorocksdb.NewWriteBatch()
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	for key, value := range cmdMap {
		wb.Put([]byte(key), value)
	}
	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[batchPutToRocksDB],err:%v", err)
		return err
	}
	return nil
}

// SeekForPrefix seeks for the place where the prefix is located in the snapshots.
func (rs *RocksDBStore) SeekForPrefix(prefix []byte) (result map[string][]byte, err error) {
	result = make(map[string][]byte)
	snapshot := rs.RocksDBSnapshot()
	it := rs.Iterator(snapshot)
	defer func() {
		it.Close()
		rs.ReleaseSnapshot(snapshot)
	}()
	it.Seek(prefix)
	for ; it.ValidForPrefix(prefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		valueByte := make([]byte, len(value))
		copy(valueByte, value)
		result[string(key)] = valueByte
		it.Key().Free()
		it.Value().Free()
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// RocksDBSnapshot returns the RocksDB snapshot.
func (rs *RocksDBStore) RocksDBSnapshot() *gorocksdb.Snapshot {
	return rs.db.NewSnapshot()
}

// ReleaseSnapshot releases the snapshot and its resources.
func (rs *RocksDBStore) ReleaseSnapshot(snapshot *gorocksdb.Snapshot) {
	rs.db.ReleaseSnapshot(snapshot)
}

// Iterator returns the iterator of the snapshot.
func (rs *RocksDBStore) Iterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetSnapshot(snapshot)

	return rs.db.NewIterator(ro)
}
