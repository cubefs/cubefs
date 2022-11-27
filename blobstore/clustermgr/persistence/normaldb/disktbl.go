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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	// special seperate char for index key
	seperateChar = "-=-"

	DiskInfoVersionNormal = iota + 1
)

var (
	diskStatusIndex  = "Status"
	diskHostIndex    = "Host"
	diskIDCIndex     = "Idc"
	diskIDCRACKIndex = "Idc-Rack"
)

type DiskInfoRecord struct {
	Version      uint8            `json:"-"`
	DiskID       proto.DiskID     `json:"disk_id"`
	ClusterID    proto.ClusterID  `json:"cluster_id"`
	Idc          string           `json:"idc"`
	Rack         string           `json:"rack"`
	Host         string           `json:"host"`
	Path         string           `json:"path"`
	Status       proto.DiskStatus `json:"status"`
	Readonly     bool             `json:"readonly"`
	MaxChunkCnt  int64            `json:"max_chunk_cnt"`
	FreeChunkCnt int64            `json:"free_chunk_cnt"`
	UsedChunkCnt int64            `json:"used_chunk_cnt"`
	CreateAt     time.Time        `json:"create_time"`
	LastUpdateAt time.Time        `json:"last_update_time"`
	Size         int64            `json:"size"`
	Used         int64            `json:"used"`
	Free         int64            `json:"free"`
}

type DiskTable struct {
	tbl     kvstore.KVTable
	indexes map[string]indexItem
}

type indexItem struct {
	indexNames []string
	tbl        kvstore.KVTable
}

func OpenDiskTable(db kvstore.KVStore, ensureIndex bool) (*DiskTable, error) {
	if db == nil {
		return nil, errors.New("OpenScopeTable failed: db is nil")
	}
	table := &DiskTable{
		tbl: db.Table(diskCF),
		indexes: map[string]indexItem{
			diskStatusIndex:  {indexNames: []string{diskStatusIndex}, tbl: db.Table(diskStatusIndexCF)},
			diskHostIndex:    {indexNames: []string{diskHostIndex}, tbl: db.Table(diskHostIndexCF)},
			diskIDCIndex:     {indexNames: []string{diskIDCIndex}, tbl: db.Table(diskIDCIndexCF)},
			diskIDCRACKIndex: {indexNames: strings.Split(diskIDCRACKIndex, "-"), tbl: db.Table(diskIDCRackIndexCF)},
		},
	}
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

func (d *DiskTable) GetDisk(diskID proto.DiskID) (info *DiskInfoRecord, err error) {
	key := diskID.Encode()
	v, err := d.tbl.Get(key)
	if err != nil {
		return nil, err
	}
	info, err = decodeDiskInfoRecord(v)
	if err != nil {
		return
	}

	return
}

// return all disk info in memory
func (d *DiskTable) GetAllDisks() ([]*DiskInfoRecord, error) {
	return d.listDisksByDiskTbl(0, 0)
}

func (d *DiskTable) ListDisk(opt *clustermgr.ListOptionArgs) ([]*DiskInfoRecord, error) {
	if opt == nil {
		return nil, errors.New("invalid list option")
	}

	var (
		tbl            kvstore.KVTable
		indexKeyPrefix = ""
		seekKey        = ""
		ret            = make([]*DiskInfoRecord, 0)
		indexNames     []string
		useIndex       = false
		diskInfo       *DiskInfoRecord
		err            error
	)

	tbl = d.tbl
	if opt.Status.IsValid() {
		tbl = d.indexes[diskStatusIndex].tbl
		indexNames = d.indexes[diskStatusIndex].indexNames
		indexKeyPrefix = genIndexKey(indexNames[0], opt.Status)
		useIndex = true
	}
	if opt.Idc != "" {
		index := diskIDCIndex
		if opt.Rack != "" {
			index = diskIDCRACKIndex
		}
		tbl = d.indexes[index].tbl
		indexNames = d.indexes[index].indexNames
		indexKeyPrefix = genIndexKey(indexNames[0], opt.Idc)
		if opt.Rack != "" {
			indexKeyPrefix += genIndexKey(indexNames[1], opt.Rack)
		}
		useIndex = true
	}
	if opt.Host != "" {
		tbl = d.indexes[diskHostIndex].tbl
		indexNames = d.indexes[diskHostIndex].indexNames
		indexKeyPrefix = genIndexKey(indexNames[0], opt.Host)
		useIndex = true
	}

	if !useIndex {
		return d.listDisksByDiskTbl(opt.Marker, opt.Count)
	}

	seekKey += indexKeyPrefix
	if opt.Marker != proto.InvalidDiskID {
		seekKey += opt.Marker.ToString()
	}

	snap := tbl.NewSnapshot()
	defer tbl.ReleaseSnapshot(snap)
	iter := tbl.NewIterator(snap)
	defer iter.Close()
	iter.Seek([]byte(seekKey))

	if opt.Marker != proto.InvalidDiskID && iter.Valid() {
		iter.Next()
	}
	count := opt.Count
	for ; count > 0 && iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return nil, errors.Info(iter.Err(), "disk table iterate failed")
		}
		if iter.Key().Size() != 0 && iter.Value().Size() != 0 {
			// index iterate mode, we should check if iterate to the end
			// eg: Status-1-101, Status-2-102, if iterate Status-1-, then the Status-2-102 will be iterate too
			// so we need to stop when iterate to Status-2
			if !iter.ValidForPrefix([]byte(indexKeyPrefix)) {
				iter.Key().Free()
				iter.Value().Free()
				return ret, nil
			}

			var diskID proto.DiskID
			diskID = diskID.Decode(iter.Value().Data())
			diskInfo, err = d.GetDisk(diskID)
			if err != nil {
				iter.Key().Free()
				iter.Value().Free()
				return nil, errors.Info(err, "disk table iterate failed")
			}

			// two part of detail filter
			if opt.Host != "" && diskInfo.Host != opt.Host {
				goto FREE
			}
			if opt.Idc != "" && diskInfo.Idc != opt.Idc {
				goto FREE
			}
			if opt.Rack != "" && diskInfo.Rack != opt.Rack {
				goto FREE
			}
			if opt.Status.IsValid() && diskInfo.Status != opt.Status {
				goto FREE
			}
			ret = append(ret, diskInfo)
			count--
		}
	FREE:
		iter.Value().Free()
		iter.Key().Free()
	}
	return ret, nil
}

func (d *DiskTable) AddDisk(info *DiskInfoRecord) error {
	key := info.DiskID.Encode()
	value, err := encodeDiskInfoRecord(info)
	if err != nil {
		return err
	}

	batch := d.tbl.NewWriteBatch()
	defer batch.Destroy()

	// use reflect to build index
	// one batch write to build index and write data
	reflectValue := reflect.ValueOf(info).Elem()
	for i := range d.indexes {
		indexKey := ""
		for j := range d.indexes[i].indexNames {
			indexName := d.indexes[i].indexNames[j]
			reflectValue.FieldByName(indexName).Interface()
			indexKey += genIndexKey(indexName, reflectValue.FieldByName(indexName).Interface())
		}
		indexKey += info.DiskID.ToString()
		batch.PutCF(d.indexes[i].tbl.GetCf(), []byte(indexKey), key)
	}
	batch.PutCF(d.tbl.GetCf(), key, value)

	return d.tbl.DoBatch(batch)
}

func (d *DiskTable) UpdateDisk(diskID proto.DiskID, info *DiskInfoRecord) error {
	key := info.DiskID.Encode()
	value, err := encodeDiskInfoRecord(info)
	if err != nil {
		return err
	}
	return d.tbl.Put(kvstore.KV{Key: key, Value: value})
}

// update disk status should remove old index and insert new index
func (d *DiskTable) UpdateDiskStatus(diskID proto.DiskID, status proto.DiskStatus) error {
	key := diskID.Encode()
	value, err := d.tbl.Get(key)
	if err != nil {
		return errors.Info(err, "get disk failed").Detail(err)
	}

	info, err := decodeDiskInfoRecord(value)
	if err != nil {
		return errors.Info(err, "decode disk failed").Detail(err)
	}

	// delete old index and insert new index and disk info
	// we put all this on a write batch
	batch := d.tbl.NewWriteBatch()
	defer batch.Destroy()

	// generate old and new index key
	oldIndexKey := ""
	newIndexKey := ""
	indexName := d.indexes[diskStatusIndex].indexNames[0]
	oldIndexKey += genIndexKey(indexName, info.Status)
	oldIndexKey += diskID.ToString()
	newIndexKey += genIndexKey(indexName, status)
	newIndexKey += diskID.ToString()

	batch.DeleteCF(d.indexes[diskStatusIndex].tbl.GetCf(), []byte(oldIndexKey))
	batch.PutCF(d.indexes[diskStatusIndex].tbl.GetCf(), []byte(newIndexKey), key)

	info.Status = status
	value, err = encodeDiskInfoRecord(info)
	if err != nil {
		return errors.Info(err, "encode disk failed").Detail(err)
	}
	batch.PutCF(d.tbl.GetCf(), key, value)

	return d.tbl.DoBatch(batch)
}

func (d *DiskTable) DeleteDisk(diskID proto.DiskID) error {
	key := diskID.Encode()
	value, err := d.tbl.Get(key)
	if err != nil && err != kvstore.ErrNotFound {
		return err
	}
	// already delete, then return
	if err == kvstore.ErrNotFound {
		return nil
	}

	info, err := decodeDiskInfoRecord(value)
	if err != nil {
		return err
	}

	batch := d.tbl.NewWriteBatch()
	defer batch.Destroy()

	// use reflect to build index
	// one batch write to delete index and data
	reflectValue := reflect.ValueOf(info).Elem()
	for i := range d.indexes {
		indexKey := ""
		for j := range d.indexes[i].indexNames {
			indexName := d.indexes[i].indexNames[j]
			reflectValue.FieldByName(indexName).Interface()
			indexKey += genIndexKey(indexName, reflectValue.FieldByName(indexName).Interface())
		}
		indexKey += diskID.ToString()
		batch.DeleteCF(d.indexes[i].tbl.GetCf(), []byte(indexKey))
	}
	batch.DeleteCF(d.tbl.GetCf(), key)

	return d.tbl.DoBatch(batch)
}

func (d *DiskTable) listDisksByDiskTbl(marker proto.DiskID, count int) ([]*DiskInfoRecord, error) {
	snap := d.tbl.NewSnapshot()
	defer d.tbl.ReleaseSnapshot(snap)
	iter := d.tbl.NewIterator(snap)
	defer iter.Close()
	ret := make([]*DiskInfoRecord, 0)

	iter.SeekToFirst()
	if marker != proto.InvalidDiskID {
		iter.Seek(marker.Encode())
		if iter.Valid() {
			iter.Next()
		}
	}

	for i := 1; iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return nil, errors.Info(iter.Err(), "disk table iterate failed")
		}
		info, err := decodeDiskInfoRecord(iter.Value().Data())
		if err != nil {
			return nil, errors.Info(err, "decode disk info db failed").Detail(err)
		}
		iter.Key().Free()
		iter.Value().Free()
		ret = append(ret, info)
		if count != 0 && i >= count {
			return ret, nil
		}
		i++
	}
	return ret, nil
}

func encodeDiskInfoRecord(info *DiskInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{byte(info.Version)}, data...)
	return data, nil
}

func decodeDiskInfoRecord(data []byte) (*DiskInfoRecord, error) {
	version := uint8(data[0])
	if version == DiskInfoVersionNormal {
		ret := &DiskInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid disk info version")
}

func genIndexKey(indexName string, indexValue interface{}) string {
	return fmt.Sprintf(indexName+seperateChar+"%v"+seperateChar, indexValue)
}
