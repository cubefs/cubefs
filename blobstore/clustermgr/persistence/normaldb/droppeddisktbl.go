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
	"errors"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

var uselessVal = []byte("1")

type DroppedDiskTable struct {
	tbl kvstore.KVTable
}

func OpenBlobNodeDroppedDiskTable(db kvstore.KVStore) (*DroppedDiskTable, error) {
	if db == nil {
		return nil, errors.New("OpenScopeTable failed: db is nil")
	}
	return &DroppedDiskTable{db.Table(diskDropCF)}, nil
}

func OpenShardNodeDroppedDiskTable(db kvstore.KVStore) (*DroppedDiskTable, error) {
	if db == nil {
		return nil, errors.New("OpenScopeTable failed: db is nil")
	}
	return &DroppedDiskTable{db.Table(shardNodeDiskDropCF)}, nil
}

// GetAllDroppingDisk return all drop disk in memory
func (d *DroppedDiskTable) GetAllDroppingDisk() ([]proto.DiskID, error) {
	iter := d.tbl.NewIterator(nil)
	defer iter.Close()
	ret := make([]proto.DiskID, 0)
	var diskID proto.DiskID
	iter.SeekToFirst()
	for iter.Valid() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		ret = append(ret, diskID.Decode(iter.Key().Data()))
		iter.Key().Free()
		iter.Value().Free()
		iter.Next()
	}
	return ret, nil
}

// AddDroppingDisk add a dropping disk
func (d *DroppedDiskTable) AddDroppingDisk(diskID proto.DiskID) error {
	key := diskID.Encode()
	return d.tbl.Put(kvstore.KV{Key: key, Value: uselessVal})
}

// DroppedDisk finish dropping in a disk
func (d *DroppedDiskTable) DroppedDisk(diskID proto.DiskID) error {
	key := diskID.Encode()
	return d.tbl.Delete(key)
}

// IsDroppingDisk find a dropping disk if exist
func (d *DroppedDiskTable) IsDroppingDisk(diskID proto.DiskID) (exist bool, err error) {
	key := diskID.Encode()
	_, err = d.tbl.Get(key)
	if errors.Is(err, kvstore.ErrNotFound) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	exist = true
	return
}
