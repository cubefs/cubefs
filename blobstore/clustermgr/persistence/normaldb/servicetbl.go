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

package normaldb

import (
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
)

type ServiceTable struct {
	tbl kvstore.KVTable
}

func OpenServiceTable(db *NormalDB) *ServiceTable {
	return &ServiceTable{db.Table(serviceCF)}
}

func (s *ServiceTable) Get(sname string) ([][]byte, error) {
	iter := s.tbl.NewIterator(nil)
	defer func() {
		iter.Close()
	}()

	keyPrefix := []byte(sname)
	var values [][]byte
	for iter.Seek(keyPrefix); iter.ValidForPrefix(keyPrefix); iter.Next() {
		if err := iter.Err(); err != nil {
			return nil, err
		}
		val := make([]byte, iter.Value().Size())
		copy(val, iter.Value().Data())
		values = append(values, val)
		iter.Key().Free()
		iter.Value().Free()
	}
	return values, nil
}

func (s *ServiceTable) Range(f func(key, val []byte) bool) error {
	iter := s.tbl.NewIterator(nil)
	defer func() {
		iter.Close()
	}()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := iter.Err(); err != nil {
			return err
		}
		key := make([]byte, iter.Key().Size())
		copy(key, iter.Key().Data())
		val := make([]byte, iter.Value().Size())
		copy(val, iter.Value().Data())
		iter.Key().Free()
		iter.Value().Free()
		if !f(key, val) {
			break
		}
	}
	return nil
}

func (s *ServiceTable) Put(sname, host string, data []byte) (err error) {
	key := []byte(sname + "/" + host)
	return s.tbl.Put(kvstore.KV{Key: key, Value: data})
}

func (s *ServiceTable) Delete(sname, host string) (err error) {
	key := []byte(sname + "/" + host)
	return s.tbl.Delete(key)
}
