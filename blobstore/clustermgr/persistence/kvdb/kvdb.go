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

import "github.com/cubefs/cubefs/blobstore/common/kvstore"

var (
	kvCF  = "keyValue"
	kvCFs = []string{
		kvCF,
	}
)

type KvDB struct {
	kvstore.KVStore
}

func Open(path string, isSync bool, dbOpts ...kvstore.DbOptions) (*KvDB, error) {
	db, err := kvstore.OpenDBWithCF(path, isSync, kvCFs, dbOpts...)
	if err != nil {
		return nil, err
	}

	return &KvDB{db}, nil
}

func (t *KvDB) Close() error {
	t.KVStore.Close()
	return nil
}

func (t *KvDB) GetAllCfNames() []string {
	return kvCFs
}
