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

package kvdb

import (
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type KvTable struct {
	tbl kvstore.KVTable
}

func OpenKvTable(db kvstore.KVStore) (*KvTable, error) {
	if db == nil {
		return nil, errors.New("taskTable  failed: db is nil")
	}
	return &KvTable{db.Table(kvCF)}, nil
}

func (t *KvTable) Set(key []byte, value []byte) error {
	kv := kvstore.KV{
		Key:   key,
		Value: value,
	}
	err := t.tbl.Put(kv)
	return err
}

func (t *KvTable) Get(key []byte) ([]byte, error) {
	return t.tbl.Get(key)
}

func (t *KvTable) Delete(key []byte) error {
	return t.tbl.Delete(key)
}

func (t *KvTable) List(args *clustermgr.ListKvOpts) ([]*clustermgr.KeyValue, error) {
	s := t.tbl.NewSnapshot()
	defer t.tbl.ReleaseSnapshot(s)
	iter := t.tbl.NewIterator(s)
	defer iter.Close()

	seekKey := args.Prefix
	if args.Marker != "" {
		seekKey = args.Marker
	}
	iter.Seek([]byte(seekKey))
	if args.Marker != "" && iter.Valid() {
		iter.Next()
	}

	ret := make([]*clustermgr.KeyValue, 0, 1000)
	count := args.Count
	for ; count > 0 && iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return nil, errors.Info(iter.Err(), "task table iterate failed")
		}
		if iter.Key().Size() != 0 {
			// eg prefix= "balance-4-100-", the balance-5-1 task will be iterate
			// so iter must ValidForPrefix() return true
			if !iter.ValidForPrefix([]byte(args.Prefix)) {
				iter.Key().Free()
				iter.Value().Free()
				break
			}
			k := string(iter.Key().Data())
			val := make([]byte, len(iter.Value().Data()))
			copy(val, iter.Value().Data())
			ret = append(ret, &clustermgr.KeyValue{Key: k, Value: val})
			count--
			iter.Key().Free()
			iter.Value().Free()
		}

	}
	return ret, nil
}
