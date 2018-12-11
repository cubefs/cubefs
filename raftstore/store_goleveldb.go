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
	"github.com/tiglabs/containerfs/third_party/goleveldb/leveldb"
	lerrors "github.com/tiglabs/containerfs/third_party/goleveldb/leveldb/errors"
	"github.com/tiglabs/containerfs/third_party/goleveldb/leveldb/iterator"
	"github.com/tiglabs/containerfs/third_party/goleveldb/leveldb/opt"
)

type LevelDBStore struct {
	dir string
	db  *leveldb.DB
}

func NewLevelDBStore(dir string) (store *LevelDBStore) {
	store = &LevelDBStore{dir: dir}
	err := store.Open()
	if err != nil {
		panic(fmt.Sprintf("Failed to Open goleveldb! err:%v", err.Error()))
	}
	return store
}

func (rs *LevelDBStore) Open() error {
	defaultOptions := &opt.Options{
		Compression:        opt.NoCompression,
		BlockCacheCapacity: 2 << 30,
	}
	db, err := leveldb.OpenFile(rs.dir, defaultOptions)
	if err != nil {
		err = fmt.Errorf("action[openGoleveldb],err:%v", err)
		return err
	}
	rs.db = db

	return nil

}

func (rs *LevelDBStore) Del(key interface{}) (result interface{}, err error) {
	ro := &opt.ReadOptions{DontFillCache: true}
	wo := &opt.WriteOptions{Sync: true}
	result, err = rs.db.Get([]byte(key.(string)), ro)
	if err != nil && err != lerrors.ErrNotFound {
		return
	}
	err = rs.db.Delete([]byte(key.(string)), wo)
	return
}

func (rs *LevelDBStore) Put(key, value interface{}) (result interface{}, err error) {
	wo := &opt.WriteOptions{Sync: true}
	if err := rs.db.Put([]byte(key.(string)), value.([]byte), wo); err != nil {
		return nil, err
	}
	result = value
	return result, nil
}

func (rs *LevelDBStore) Get(key interface{}) (result interface{}, err error) {
	ro := &opt.ReadOptions{DontFillCache: true}
	value, err := rs.db.Get([]byte(key.(string)), ro)
	if err != nil && err != lerrors.ErrNotFound {
		return
	}
	return value, nil
}

func (rs *LevelDBStore) DeleteKeyAndPutIndex(key string, cmdMap map[string][]byte) error {
	wo := &opt.WriteOptions{Sync: true}
	wb := new(leveldb.Batch)
	defer wb.Reset()
	wb.Delete([]byte(key))
	for otherKey, value := range cmdMap {
		if otherKey == key {
			continue
		}
		wb.Put([]byte(otherKey), value)
	}

	if err := rs.db.Write(wb, wo); err != nil {
		err = fmt.Errorf("action[deleteFromGoleveldb],err:%v", err)
		return err
	}
	return nil
}

func (rs *LevelDBStore) BatchPut(cmdMap map[string][]byte) error {
	wo := &opt.WriteOptions{Sync: true}
	wb := new(leveldb.Batch)
	defer wb.Reset()
	for key, value := range cmdMap {
		wb.Put([]byte(key), value)
	}
	if err := rs.db.Write(wb, wo); err != nil {
		err = fmt.Errorf("action[batchPutToGoleveldb],err:%v", err)
		return err
	}
	return nil
}

func (rs *LevelDBStore) Snapshot() (*leveldb.Snapshot, error) {
	return rs.db.GetSnapshot()
}

func (rs *LevelDBStore) ReleaseSnapshot(snapshot *leveldb.Snapshot) {
	snapshot.Release()
}

func (rs *LevelDBStore) Iterator(snapshot *leveldb.Snapshot) iterator.Iterator {
	ro := &opt.ReadOptions{DontFillCache: true}
	return snapshot.NewIterator(nil, ro)
}
