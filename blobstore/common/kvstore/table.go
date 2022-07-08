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

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	rdb "github.com/tecbot/gorocksdb"
)

type KVTable interface {
	KVStorage
	GetDB() *rdb.DB
	GetCf() *rdb.ColumnFamilyHandle
}

type table struct {
	name string
	ins  *instance
	db   *rdb.DB
	ro   *rdb.ReadOptions
	wo   *rdb.WriteOptions
	fo   *rdb.FlushOptions
	cf   *rdb.ColumnFamilyHandle
}

/* table function list */
func (t *table) Get(key []byte, opts ...OpOption) (data []byte, err error) {
	cValue, err := t.db.GetCF(t.ro, t.cf, key)
	if err != nil {
		return nil, err
	}
	defer cValue.Free()
	if err == nil && cValue.Size() == 0 {
		err = ErrNotFound
		return
	}
	data = make([]byte, cValue.Size())
	copy(data, cValue.Data())
	return
}

func (t *table) Put(kv KV, opts ...OpOption) (err error) {
	return t.db.PutCF(t.wo, t.cf, kv.Key, kv.Value)
}

func (t *table) Delete(key []byte, opts ...OpOption) (err error) {
	return t.db.DeleteCF(t.wo, t.cf, key)
}

func (t *table) Name() string {
	return t.name
}

func (t *table) Flush() error {
	return t.db.Flush(t.fo)
}

func (t *table) DeleteBatch(keys [][]byte, safe bool) (err error) {
	batch := rdb.NewWriteBatch()
	defer batch.Destroy()
	for _, key := range keys {
		if safe {
			_, err := t.Get(key)
			if err != nil {
				return err
			}
		}
		batch.DeleteCF(t.cf, key)
	}
	if batch.Count() > 0 {
		err = t.db.Write(t.wo, batch)
	}
	return
}

func (t *table) WriteBatch(kvs []KV, safe bool) (err error) {
	batch := rdb.NewWriteBatch()
	defer batch.Destroy()
	for _, kv := range kvs {
		key := kv.Key
		if safe {
			v, err := t.Get(key)
			if err != nil {
				return err
			}
			if v != nil &&
				!bytes.Equal(kv.Value, v) {
				msg := fmt.Sprintf("table(%v): conflict data at row %v\n old buf: %v, new buf: %v",
					t.db.Name(), kv.Key, v, kv.Value)
				return errors.New(msg)
			}
		}
		batch.PutCF(t.cf, kv.Key, kv.Value)
	}
	if batch.Count() > 0 {
		err = t.db.Write(t.wo, batch)
	}
	return
}

func (t *table) NewSnapshot() *Snapshot {
	return (*Snapshot)(t.db.NewSnapshot())
}

func (s *table) ReleaseSnapshot(snapshot *Snapshot) {
	s.db.ReleaseSnapshot((*rdb.Snapshot)(snapshot))
}

func (t *table) NewIterator(snapshot *Snapshot, opts ...OpOption) Iterator {
	var op Op

	op.applyOpts(opts)
	ro := op.Ro

	if ro == nil {
		ro = NewReadOptions()
	}

	if snapshot != nil {
		ro.SetSnapshot((*rdb.Snapshot)(snapshot))
	}

	return &iterator{ro: ro.ReadOptions, Iterator: t.db.NewIteratorCF(ro.ReadOptions, t.cf), once: sync.Once{}}
}

func (t *table) GetDB() *rdb.DB {
	return t.db
}

func (t *table) GetCf() *rdb.ColumnFamilyHandle {
	return t.cf
}

func (t *table) NewWriteBatch() *WriteBatch {
	return &WriteBatch{rdb.NewWriteBatch()}
}

func (t *table) DoBatch(batch *WriteBatch) error {
	if batch.Count() > 0 {
		return t.db.Write(t.wo, batch.WriteBatch)
	}
	return nil
}
