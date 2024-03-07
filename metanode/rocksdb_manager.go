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
	"sync"
)

var (
	ErrUnregisteredRocksdbPath = errors.New("rocksdb path unregister")
	ErrRocksdbPathRegistered   = errors.New("rocksdb path already registered")
	ErrRocksdbOpened           = errors.New("rocksdb stil in use")
)

type RocksdbManager interface {
	Register(dbPath string) (err error)

	Unregister(dbPath string) (err error)

	OpenRocksdb(dbPath string) (db *RocksDbInfo, err error)

	CloseRocksdb(db *RocksDbInfo)
}

type RocksdbHandle struct {
	db *RocksDbInfo
	rc uint64
}

type rocksdbManager struct {
	mutex sync.Mutex
	dbs   map[string]*RocksdbHandle
}

func (r *rocksdbManager) Register(dbPath string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	_, ok := r.dbs[dbPath]
	if ok {
		err = ErrRocksdbPathRegistered
		return
	}
	r.dbs[dbPath] = &RocksdbHandle{
		db: NewRocksDb(),
		rc: 0,
	}
	return
}

func (r *rocksdbManager) Unregister(dbPath string) (err error) {
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

func (r *rocksdbManager) OpenRocksdb(dbPath string) (db *RocksDbInfo, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		err = ErrUnregisteredRocksdbPath
		return
	}
	handle.rc += 1
	if handle.rc == 1 {
		err = handle.db.OpenDb(dbPath, 0, 0, 0, 0, 0, 0)
		if err != nil {
			handle.rc -= 1
			return
		}
	}
	db = handle.db
	return
}

func (r *rocksdbManager) CloseRocksdb(db *RocksDbInfo) {
	dbPath := db.dir
	r.mutex.Lock()
	defer r.mutex.Unlock()
	handle, ok := r.dbs[dbPath]
	if !ok {
		return
	}
	handle.rc -= 1
	if handle.rc == 0 {
		handle.db.CloseDb()
	}
}

var _ RocksdbManager = &rocksdbManager{}

func NewRocksdbManager() (p RocksdbManager) {
	p = &rocksdbManager{
		dbs: make(map[string]*RocksdbHandle),
	}
	return
}
