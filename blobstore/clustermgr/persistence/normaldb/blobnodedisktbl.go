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

package normaldb

import (
	"encoding/json"
	"strings"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type BlobNodeDiskInfoRecord struct {
	DiskInfoRecord
	MaxChunkCnt  int64 `json:"max_chunk_cnt"`
	FreeChunkCnt int64 `json:"free_chunk_cnt"`
	UsedChunkCnt int64 `json:"used_chunk_cnt"`
	Size         int64 `json:"size"`
	Used         int64 `json:"used"`
	Free         int64 `json:"free"`
}

func OpenBlobNodeDiskTable(db kvstore.KVStore, ensureIndex bool) (*BlobNodeDiskTable, error) {
	if db == nil {
		return nil, errors.New("OpenBlobNodeDiskTable failed: db is nil")
	}
	table := &BlobNodeDiskTable{
		diskTable: &diskTable{
			tbl: db.Table(diskCF),
			indexes: map[string]indexItem{
				diskStatusIndex:  {indexNames: []string{diskStatusIndex}, tbl: db.Table(diskStatusIndexCF)},
				diskHostIndex:    {indexNames: []string{diskHostIndex}, tbl: db.Table(diskHostIndexCF)},
				diskIDCIndex:     {indexNames: []string{diskIDCIndex}, tbl: db.Table(diskIDCIndexCF)},
				diskIDCRACKIndex: {indexNames: strings.Split(diskIDCRACKIndex, "-"), tbl: db.Table(diskIDCRackIndexCF)},
			},
		},
	}
	table.diskTable.rd = table

	// ensure index
	if ensureIndex {
		list, err := table.GetAllDisks()
		if err != nil {
			return nil, errors.Info(err, "get all disk failed").Detail(err)
		}
		for i := range list {
			if err = table.AddDisk(list[i]); err != nil {
				return nil, errors.Info(err, "add disk failed").Detail(err)
			}
		}
	}

	return table, nil
}

type BlobNodeDiskTable struct {
	diskTable *diskTable
}

func (b *BlobNodeDiskTable) GetDisk(diskID proto.DiskID) (info *BlobNodeDiskInfoRecord, err error) {
	ret, err := b.diskTable.GetDisk(diskID)
	if err != nil {
		return nil, err
	}
	return ret.(*BlobNodeDiskInfoRecord), nil
}

func (b *BlobNodeDiskTable) GetAllDisks() ([]*BlobNodeDiskInfoRecord, error) {
	ret := make([]*BlobNodeDiskInfoRecord, 0)
	err := b.diskTable.ListDisksByDiskTbl(0, 0, func(i interface{}) {
		ret = append(ret, i.(*BlobNodeDiskInfoRecord))
	})
	return ret, err
}

func (b *BlobNodeDiskTable) ListDisk(opt *clustermgr.ListOptionArgs) ([]*BlobNodeDiskInfoRecord, error) {
	ret := make([]*BlobNodeDiskInfoRecord, 0)
	err := b.diskTable.ListDisk(opt, func(i interface{}) {
		ret = append(ret, i.(*BlobNodeDiskInfoRecord))
	})
	return ret, err
}

func (b *BlobNodeDiskTable) AddDisk(disk *BlobNodeDiskInfoRecord) error {
	return b.diskTable.AddDisk(disk.DiskID, disk)
}

func (b *BlobNodeDiskTable) UpdateDisk(diskID proto.DiskID, disk *BlobNodeDiskInfoRecord) error {
	return b.diskTable.UpdateDisk(diskID, disk)
}

func (b *BlobNodeDiskTable) UpdateDiskStatus(diskID proto.DiskID, status proto.DiskStatus) error {
	return b.diskTable.UpdateDiskStatus(diskID, status)
}

func (b *BlobNodeDiskTable) unmarshalRecord(data []byte) (interface{}, error) {
	version := data[0]
	if version == DiskInfoVersionNormal {
		ret := &BlobNodeDiskInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid disk info version")
}

func (b *BlobNodeDiskTable) marshalRecord(v interface{}) ([]byte, error) {
	info := v.(*BlobNodeDiskInfoRecord)
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func (b *BlobNodeDiskTable) diskID(i interface{}) proto.DiskID {
	return i.(*BlobNodeDiskInfoRecord).DiskID
}

func (b *BlobNodeDiskTable) diskInfo(i interface{}) *DiskInfoRecord {
	return &i.(*BlobNodeDiskInfoRecord).DiskInfoRecord
}
