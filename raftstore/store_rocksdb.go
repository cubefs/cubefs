// Copyright 2018 The Containerfs Authors.
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

package raftstore

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
)

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
	opts.SetCompression(gorocksdb.NoCompression)
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
	wo.SetSync(true)
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
	wo.SetSync(true)
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
	wo.SetSync(true)
	wb := gorocksdb.NewWriteBatch()
	defer wb.Clear()
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

func (rs *RocksDBStore) BatchPut(cmdMap map[string][]byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
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
