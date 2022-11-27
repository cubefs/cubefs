// Copyright 2022 The CubeFS Authors.
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

package kvstore

import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"

	rdb "github.com/tecbot/gorocksdb"
)

var (
	ErrInvalidCFName = errors.New("invalid column family name")
	ErrNotFound      = os.ErrNotExist
)

type KVStore interface {
	KVStorage
	GetDB() *rdb.DB
	Table(name string) KVTable
	Close() error
}

type Iterator interface {
	SeekToFirst()
	SeekToLast()
	Seek([]byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
	Key() *rdb.Slice
	Next()
	Value() *rdb.Slice
	// destroy iterator and read option
	Close()
	Err() error
}

type WriteBatch struct {
	*rdb.WriteBatch
}

type Snapshot rdb.Snapshot

type ReadOptions struct {
	*rdb.ReadOptions
}

type WriteOptions struct {
	*rdb.WriteOptions
}

type Range rdb.Range

type KV struct {
	Key   []byte
	Value []byte
}

type instance struct {
	db *rdb.DB
	ro *rdb.ReadOptions
	wo *rdb.WriteOptions
	fo *rdb.FlushOptions

	cfTables map[string]*table
	opt      *rdb.Options
	sync     bool

	lock sync.RWMutex
	once sync.Once
}

type iterator struct {
	ro   *rdb.ReadOptions
	once sync.Once
	*rdb.Iterator
}

func (i *iterator) Close() {
	i.once.Do(func() {
		i.Iterator.Close()
		i.ro.Destroy()
	})
}

func OpenDBWithCF(path string, isSync bool, cfnames []string, dbOpts ...DbOptions) (KVStore, error) {
	if path == "" {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}

	err := os.MkdirAll(path, 0o755)
	if err != nil {
		panic(err)
	}

	dbOpt := &RocksDBOption{}
	dbOpt.applyOpts(dbOpts)

	if len(cfnames) == 0 {
		return nil, ErrInvalidCFName
	}
	cfnames = append([]string{"default"}, cfnames...)

	opts := genRocksdbOpts(dbOpt)

	cfopts := make([]*rdb.Options, len(cfnames))
	for i := 0; i < len(cfnames); i++ {
		cfopts[i] = opts
	}

	db, cfs, err := rdb.OpenDbColumnFamilies(opts, path, cfnames, cfopts)
	if err != nil {
		opts.Destroy()
		if strings.HasSuffix(err.Error(), "does not exist (create_if_missing is false)") {
			err = ErrNotFound
		}
		return nil, err
	}

	ro := rdb.NewDefaultReadOptions()
	ro.SetVerifyChecksums(true)
	wo := rdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	fo := rdb.NewDefaultFlushOptions()

	cfTables := make(map[string]*table)
	ins := &instance{
		db:       db,
		ro:       ro,
		wo:       wo,
		fo:       fo,
		cfTables: cfTables,
		opt:      opts,
		sync:     isSync,
		lock:     sync.RWMutex{},
		once:     sync.Once{},
	}

	for i := range cfs {
		cfTables[cfnames[i]] = &table{
			name: cfnames[i],
			cf:   cfs[i],
			ins:  ins,
			db:   ins.db,
			ro:   ro,
			wo:   wo,
			fo:   fo,
		}
	}

	return ins, nil
}

func OpenDB(path string, isSync bool, dbOpts ...DbOptions) (KVStore, error) {
	if path == "" {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		panic(err)
	}
	dbOpt := &RocksDBOption{}
	dbOpt.applyOpts(dbOpts)

	opts := genRocksdbOpts(dbOpt)
	db, err := rdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		if strings.HasSuffix(err.Error(), "does not exist (create_if_missing is false)") {
			err = ErrNotFound
		}
		return nil, err
	}

	ro := rdb.NewDefaultReadOptions()
	ro.SetVerifyChecksums(true)
	wo := rdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	fo := rdb.NewDefaultFlushOptions()

	return &instance{
		db:       db,
		ro:       ro,
		wo:       wo,
		fo:       fo,
		cfTables: make(map[string]*table),
		opt:      opts,
		sync:     isSync,
		lock:     sync.RWMutex{},
		once:     sync.Once{},
	}, nil
}

func (s *instance) Table(name string) KVTable {
	s.lock.RLock()
	if t, ok := s.cfTables[name]; ok {
		s.lock.RUnlock()
		return t
	}
	s.lock.RUnlock()
	return nil
}

func (s *instance) Name() string {
	return s.db.Name()
}

func (s *instance) GetDB() *rdb.DB {
	return s.db
}

func (s *instance) Get(key []byte, opts ...OpOption) (data []byte, err error) {
	data, err = s.db.GetBytes(s.ro, key)
	if err == nil && data == nil {
		err = ErrNotFound
	}
	return
}

func (s *instance) Put(kv KV, opts ...OpOption) error {
	return s.db.Put(s.wo, kv.Key, kv.Value)
}

func (s *instance) Delete(key []byte, opts ...OpOption) (err error) {
	return s.db.Delete(s.wo, key)
}

func (s *instance) DeleteBatch(keys [][]byte, safe bool) (err error) {
	batch := rdb.NewWriteBatch()
	defer batch.Destroy()
	for _, key := range keys {
		if safe {
			_, err := s.Get(key)
			if err != nil {
				return err
			}
		}
		batch.Delete(key)
	}
	if batch.Count() > 0 {
		err = s.db.Write(s.wo, batch)
	}
	return
}

func (s *instance) WriteBatch(kvs []KV, safe bool) (err error) {
	batch := rdb.NewWriteBatch()
	defer batch.Destroy()
	for _, kv := range kvs {
		key := kv.Key
		if safe {
			v, err := s.db.GetBytes(s.ro, key)
			if err != nil {
				return err
			}
			if v != nil &&
				!bytes.Equal(kv.Value, v) {
				msg := fmt.Sprintf("table(%v): conflict data at row %v\n old buf: %v, new buf: %v",
					s.db.Name(), kv.Key, v, kv.Value)
				return errors.New(msg)
			}
		}
		batch.Put(key, kv.Value)
	}
	if batch.Count() > 0 {
		err = s.db.Write(s.wo, batch)
	}
	return
}

func (s *instance) NewSnapshot() *Snapshot {
	return (*Snapshot)(s.db.NewSnapshot())
}

func (s *instance) ReleaseSnapshot(snapshot *Snapshot) {
	s.db.ReleaseSnapshot((*rdb.Snapshot)(snapshot))
}

func (s *instance) NewIterator(snapshot *Snapshot, opts ...OpOption) Iterator {
	var op Op

	op.applyOpts(opts)
	ro := op.Ro

	if ro == nil {
		ro = NewReadOptions()
	}
	if snapshot != nil {
		ro.SetSnapshot((*rdb.Snapshot)(snapshot))
	}

	return &iterator{ro: ro.ReadOptions, Iterator: s.db.NewIterator(ro.ReadOptions), once: sync.Once{}}
}

func (s *instance) Flush() (err error) {
	return s.db.Flush(s.fo)
}

func (s *instance) Close() error {
	s.once.Do(func() {
		s.ro.Destroy()
		s.wo.Destroy()
		s.lock.RLock()
		for i := range s.cfTables {
			s.cfTables[i].cf.Destroy()
		}

		s.db.Close()
		s.lock.RUnlock()
	})

	return nil
}

func (s *instance) NewWriteBatch() *WriteBatch {
	return &WriteBatch{rdb.NewWriteBatch()}
}

func (s *instance) DoBatch(batch *WriteBatch) error {
	if batch.Count() > 0 {
		return s.db.Write(s.wo, batch.WriteBatch)
	}
	return nil
}

func NewReadOptions() *ReadOptions {
	return &ReadOptions{rdb.NewDefaultReadOptions()}
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{rdb.NewDefaultWriteOptions()}
}
