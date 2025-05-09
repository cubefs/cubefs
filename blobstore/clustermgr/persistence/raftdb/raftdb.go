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

package raftdb

import "github.com/cubefs/cubefs/blobstore/common/kvstore"

type RaftDB struct {
	kvs kvstore.KVStore
}

func OpenRaftDB(path string, dbOpts ...kvstore.DbOptions) (*RaftDB, error) {
	db, err := kvstore.OpenDB(path, dbOpts...)
	if err != nil {
		return nil, err
	}

	return &RaftDB{kvs: db}, nil
}

func (r *RaftDB) Close() error {
	return r.kvs.Close()
}

func (r *RaftDB) Put(key, value []byte) error {
	return r.kvs.Put(kvstore.KV{Key: key, Value: value})
}

func (r *RaftDB) PutKVs(keys, values [][]byte) error {
	kvs := make([]kvstore.KV, 0, len(keys))
	for i := range keys {
		kvs = append(kvs, kvstore.KV{Key: keys[i], Value: values[i]})
	}
	return r.kvs.WriteBatch(kvs, false)
}

// Get don't return error if not found key
func (r *RaftDB) Get(key []byte) ([]byte, error) {
	value, err := r.kvs.Get(key)
	if err == kvstore.ErrNotFound {
		err = nil
	}
	return value, err
}
