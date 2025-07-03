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

import "github.com/cubefs/cubefs/blobstore/common/kvstore"

var (
	scopeCF            = "scope"
	diskCF             = "disk"
	nodeCF             = "node"
	configCF           = "config"
	diskDropCF         = "disk_drop"
	nodeDropCF         = "node_drop"
	serviceCF          = "service"
	diskStatusIndexCF  = "disk-status"
	diskHostIndexCF    = "disk-host"
	diskIDCIndexCF     = "disk-idc"
	diskIDCRackIndexCF = "disk-idc-rack"

	shardNodeDiskCF             = "sn-disk"
	shardNodeCF                 = "shard-node"
	shardNodeDiskStatusIndexCF  = "sn-disk-status"
	shardNodeDiskHostIndexCF    = "sn-disk-host"
	shardNodeDiskIDCIndexCF     = "sn-disk-idc"
	shardNodeDiskIDCRackIndexCF = "sn-disk-idc-rack"
	shardNodeDiskDropCF         = "sn-disk-drop"

	normalDBCfs = []string{
		scopeCF,
		diskCF,
		nodeCF,
		diskDropCF,
		nodeDropCF,
		configCF,
		serviceCF,
		diskStatusIndexCF,
		diskHostIndexCF,
		diskIDCIndexCF,
		diskIDCRackIndexCF,

		shardNodeDiskCF,
		shardNodeCF,
		shardNodeDiskStatusIndexCF,
		shardNodeDiskHostIndexCF,
		shardNodeDiskIDCIndexCF,
		shardNodeDiskIDCRackIndexCF,
		shardNodeDiskDropCF,
	}
)

type NormalDB struct {
	kvstore.KVStore
}

func OpenNormalDB(path string, dbOpts ...kvstore.DbOptions) (*NormalDB, error) {
	db, err := kvstore.OpenDBWithCF(path, normalDBCfs, dbOpts...)
	if err != nil {
		return nil, err
	}

	return &NormalDB{db}, nil
}

func (n *NormalDB) Close() error {
	n.KVStore.Close()
	return nil
}

func (n *NormalDB) GetAllCfNames() []string {
	return normalDBCfs
}
