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
	"fmt"
	"reflect"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var _ diskRecordDescriptor = (*BlobNodeDiskTable)(nil)

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
	CreateAt     time.Time        `json:"create_time"`
	LastUpdateAt time.Time        `json:"last_update_time"`
	DiskSetID    proto.DiskSetID  `json:"disk_set_id"`
	NodeID       proto.NodeID     `json:"node_id"`
}

type diskRecordDescriptor interface {
	unmarshalRecord(v []byte) (interface{}, error)
	marshalRecord(i interface{}) ([]byte, error)
	diskID(i interface{}) proto.DiskID
	diskInfo(i interface{}) *DiskInfoRecord
}

type diskTable struct {
	tbl     kvstore.KVTable
	indexes map[string]indexItem

	rd diskRecordDescriptor
}

type indexItem struct {
	indexNames []string
	tbl        kvstore.KVTable
}

func (d *diskTable) GetDisk(diskID proto.DiskID) (info interface{}, err error) {
	key := diskID.Encode()
	v, err := d.tbl.Get(key)
	if err != nil {
		return nil, err
	}
	info, err = d.rd.unmarshalRecord(v)
	if err != nil {
		return
	}

	return
}

func (d *diskTable) ListDisk(opt *clustermgr.ListOptionArgs, listCallback func(i interface{})) error {
	if opt == nil {
		return errors.New("invalid list option")
	}

	var (
		tbl            kvstore.KVTable
		indexKeyPrefix = ""
		seekKey        = ""
		indexNames     []string
		useIndex       = false
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
		return d.ListDisksByDiskTbl(opt.Marker, opt.Count, listCallback)
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
			return errors.Info(iter.Err(), "list disk table iterate failed")
		}
		if iter.Key().Size() != 0 && iter.Value().Size() != 0 {
			// index iterate mode, we should check if iterate to the end
			// eg: Status-1-101, Status-2-102, if iterate Status-1-, then the Status-2-102 will be iterate too
			// so we need to stop when iterate to Status-2
			if !iter.ValidForPrefix([]byte(indexKeyPrefix)) {
				iter.Key().Free()
				iter.Value().Free()
				return nil
			}

			var diskID proto.DiskID
			diskID = diskID.Decode(iter.Value().Data())
			record, err := d.GetDisk(diskID)
			if err != nil {
				iter.Key().Free()
				iter.Value().Free()
				return errors.Info(err, "list disk table iterate failed")
			}

			diskInfo := d.rd.diskInfo(record)
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
			listCallback(record)
			count--
		}
	FREE:
		iter.Value().Free()
		iter.Key().Free()
	}
	return nil
}

func (d *diskTable) AddDisk(diskID proto.DiskID, info interface{}) error {
	key := diskID.Encode()
	value, err := d.rd.marshalRecord(info)
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
		indexKey += diskID.ToString()
		batch.PutCF(d.indexes[i].tbl.GetCf(), []byte(indexKey), key)
	}
	batch.PutCF(d.tbl.GetCf(), key, value)

	return d.tbl.DoBatch(batch)
}

func (d *diskTable) UpdateDisk(diskID proto.DiskID, info interface{}) error {
	key := diskID.Encode()
	value, err := d.rd.marshalRecord(info)
	if err != nil {
		return err
	}
	return d.tbl.Put(kvstore.KV{Key: key, Value: value})
}

// UpdateDiskStatus update disk status should remove old index and insert new index
func (d *diskTable) UpdateDiskStatus(diskID proto.DiskID, status proto.DiskStatus) error {
	key := diskID.Encode()
	value, err := d.tbl.Get(key)
	if err != nil {
		return errors.Info(err, "get disk failed").Detail(err)
	}

	info, err := d.rd.unmarshalRecord(value)
	if err != nil {
		return errors.Info(err, "decode disk failed").Detail(err)
	}
	diskInfo := d.rd.diskInfo(info)

	// delete old index and insert new index and disk info
	// we put all this on a write batch
	batch := d.tbl.NewWriteBatch()
	defer batch.Destroy()

	// generate old and new index key
	oldIndexKey := ""
	newIndexKey := ""
	indexName := d.indexes[diskStatusIndex].indexNames[0]
	oldIndexKey += genIndexKey(indexName, diskInfo.Status)
	oldIndexKey += diskID.ToString()
	newIndexKey += genIndexKey(indexName, status)
	newIndexKey += diskID.ToString()

	batch.DeleteCF(d.indexes[diskStatusIndex].tbl.GetCf(), []byte(oldIndexKey))
	batch.PutCF(d.indexes[diskStatusIndex].tbl.GetCf(), []byte(newIndexKey), key)

	diskInfo.Status = status
	value, err = d.rd.marshalRecord(info)
	if err != nil {
		return errors.Info(err, "encode disk failed").Detail(err)
	}
	batch.PutCF(d.tbl.GetCf(), key, value)

	return d.tbl.DoBatch(batch)
}

func (d *diskTable) DeleteDisk(diskID proto.DiskID) error {
	key := diskID.Encode()
	value, err := d.tbl.Get(key)
	if err != nil && errors.Is(err, kvstore.ErrNotFound) {
		return err
	}
	// already delete, then return
	if errors.Is(err, kvstore.ErrNotFound) {
		return nil
	}

	info, err := d.rd.unmarshalRecord(value)
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

func (d *diskTable) ListDisksByDiskTbl(marker proto.DiskID, count int, listCallback func(i interface{})) error {
	snap := d.tbl.NewSnapshot()
	defer d.tbl.ReleaseSnapshot(snap)
	iter := d.tbl.NewIterator(snap)
	defer iter.Close()
	// ret := make([]*DiskInfoRecord, 0)

	iter.SeekToFirst()
	if marker != proto.InvalidDiskID {
		iter.Seek(marker.Encode())
		if iter.Valid() {
			iter.Next()
		}
	}

	for i := 1; iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return errors.Info(iter.Err(), "list by disk table iterate failed")
		}
		info, err := d.rd.unmarshalRecord(iter.Value().Data())
		if err != nil {
			return errors.Info(err, "decode disk info db failed").Detail(err)
		}
		iter.Key().Free()
		iter.Value().Free()
		listCallback(info)
		if count != 0 && i >= count {
			return nil
		}
		i++
	}
	return nil
}

func genIndexKey(indexName string, indexValue interface{}) string {
	return fmt.Sprintf(indexName+seperateChar+"%v"+seperateChar, indexValue)
}
