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

package catalogdb

import (
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
)

var (
	routeCF                = "route"
	spaceCF                = "space"
	shardCF                = "shard"
	shardUnitCF            = "shard_unit"
	transitedShardCF       = "transited_shard"
	transitedShardUnitCF   = "transited_shard_unit"
	shardUnitDiskIDIndexCF = "shardUnit_DiskID"
	catalogCFs             = []string{
		routeCF,
		spaceCF,
		shardCF,
		shardUnitCF,
		transitedShardCF,
		transitedShardUnitCF,
		shardUnitDiskIDIndexCF,
	}
)

type CatalogDB struct {
	kvstore.KVStore
}

func Open(path string, dbOpts ...kvstore.DbOptions) (*CatalogDB, error) {
	db, err := kvstore.OpenDBWithCF(path, catalogCFs, dbOpts...)
	if err != nil {
		return nil, err
	}

	return &CatalogDB{db}, nil
}

func (t *CatalogDB) Close() error {
	t.KVStore.Close()
	return nil
}

func (t *CatalogDB) GetAllCfNames() []string {
	return catalogCFs
}
