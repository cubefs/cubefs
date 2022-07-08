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
	"encoding/binary"
	"errors"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
)

type ScopeTable struct {
	tbl kvstore.KVTable
}

func OpenScopeTable(db kvstore.KVStore) (*ScopeTable, error) {
	if db == nil {
		return nil, errors.New("OpenScopeTable failed: db is nil")
	}
	return &ScopeTable{db.Table(scopeCF)}, nil
}

func (s *ScopeTable) Load() (map[string]uint64, error) {
	iter := s.tbl.NewIterator(nil)
	defer iter.Close()

	ret := make(map[string]uint64)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		if iter.Key().Size() > 0 {
			ret[string(iter.Key().Data())] = binary.BigEndian.Uint64(iter.Value().Data())
			iter.Key().Free()
			iter.Value().Free()
		}
	}
	return ret, nil
}

func (s *ScopeTable) Put(name string, commit uint64) error {
	key := []byte(name)
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, commit)
	err := s.tbl.Put(kvstore.KV{Key: key, Value: v})
	if err != nil {
		return err
	}
	return nil
}

func (s *ScopeTable) Get(name string) (uint64, error) {
	key := []byte(name)
	v, err := s.tbl.Get(key)
	if err != nil {
		return 0, err
	}
	current := binary.BigEndian.Uint64(v)
	return current, nil
}
