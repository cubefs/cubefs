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

var (
	_          recordDescriptor = (*BlobNodeDiskTable)(nil)
	uselessVal                  = []byte("1")
)

const (
	// special seperate char for index key
	seperateChar = "-=-"

	DiskInfoVersionNormal = iota + 1
)

const (
	NodeInfoVersionNormal = iota + 1
)

var (
	diskStatusIndex  = "Status"
	diskHostIndex    = "Host"
	diskIDCIndex     = "Idc"
	diskIDCRACKIndex = "Idc-Rack"
)

/*
The Idc/Rack/Host of DiskInfoRecord is assigned to those of NodeInfoRecord. These fields of DiskInfoRecord are retained
to facilitate the construction of disk indexes, and these information will not be stored during actual marshal.
*/
type DiskInfoRecord struct {
	Version      uint8            `json:"-"`
	DiskID       proto.DiskID     `json:"disk_id"`
	ClusterID    proto.ClusterID  `json:"cluster_id"`
	Idc          string           `json:"idc,omitempty"`
	Rack         string           `json:"rack,omitempty"`
	Host         string           `json:"host,omitempty"`
	Path         string           `json:"path"`
	Status       proto.DiskStatus `json:"status"`
	Readonly     bool             `json:"readonly"`
	CreateAt     time.Time        `json:"create_time"`
	LastUpdateAt time.Time        `json:"last_update_time"`
	DiskSetID    proto.DiskSetID  `json:"disk_set_id"`
	NodeID       proto.NodeID     `json:"node_id"`
}

type NodeInfoRecord struct {
	Version   uint8            `json:"-"`
	NodeID    proto.NodeID     `json:"node_id"`
	NodeSetID proto.NodeSetID  `json:"node_set_id"`
	ClusterID proto.ClusterID  `json:"cluster_id"`
	DiskType  proto.DiskType   `json:"disk_type"`
	Idc       string           `json:"idc"`
	Rack      string           `json:"rack"`
	Host      string           `json:"host"`
	Role      proto.NodeRole   `json:"role"`
	Status    proto.NodeStatus `json:"status"`
}

type recordDescriptor interface {
	unmarshalDiskRecord(v []byte) (interface{}, error)
	marshalDiskRecord(i interface{}) ([]byte, error)
	diskID(i interface{}) proto.DiskID
	diskInfo(i interface{}) *DiskInfoRecord
	unmarshalNodeRecord(v []byte) (interface{}, error)
	marshalNodeRecord(i interface{}) ([]byte, error)
	nodeInfo(i interface{}) *NodeInfoRecord
}

type nodeDiskTable struct {
	nodeTbl        kvstore.KVTable
	droppedNodeTbl kvstore.KVTable
	diskTbl        kvstore.KVTable
	droppedDiskTbl kvstore.KVTable
	indexes        map[string]indexItem

	rd recordDescriptor
}

type indexItem struct {
	indexNames []string
	tbl        kvstore.KVTable
}

func (d *nodeDiskTable) GetDisk(diskID proto.DiskID) (info interface{}, err error) {
	key := diskID.Encode()
	v, err := d.diskTbl.Get(key)
	if err != nil {
		return nil, err
	}
	info, err = d.rd.unmarshalDiskRecord(v)
	if err != nil {
		return
	}

	return
}

func (d *nodeDiskTable) ListDisk(opt *clustermgr.ListOptionArgs, listCallback func(i interface{})) error {
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

	tbl = d.diskTbl
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

func (d *nodeDiskTable) AddDisk(diskID proto.DiskID, info interface{}) error {
	key := diskID.Encode()
	value, err := d.rd.marshalDiskRecord(info)
	if err != nil {
		return err
	}

	batch := d.diskTbl.NewWriteBatch()
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
	batch.PutCF(d.diskTbl.GetCf(), key, value)

	return d.diskTbl.DoBatch(batch)
}

func (d *nodeDiskTable) UpdateDisk(diskID proto.DiskID, info interface{}) error {
	key := diskID.Encode()
	value, err := d.rd.marshalDiskRecord(info)
	if err != nil {
		return err
	}
	return d.diskTbl.Put(kvstore.KV{Key: key, Value: value})
}

// UpdateDiskStatus update disk status should remove old index and insert new index
func (d *nodeDiskTable) UpdateDiskStatus(diskID proto.DiskID, status proto.DiskStatus) error {
	key := diskID.Encode()
	value, err := d.diskTbl.Get(key)
	if err != nil {
		return errors.Info(err, "get disk failed").Detail(err)
	}

	info, err := d.rd.unmarshalDiskRecord(value)
	if err != nil {
		return errors.Info(err, "decode disk failed").Detail(err)
	}
	diskInfo := d.rd.diskInfo(info)

	// delete old index and insert new index and disk info
	// we put all this on a write batch
	batch := d.diskTbl.NewWriteBatch()
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
	diskInfo.LastUpdateAt = time.Now()
	value, err = d.rd.marshalDiskRecord(info)
	if err != nil {
		return errors.Info(err, "encode disk failed").Detail(err)
	}
	batch.PutCF(d.diskTbl.GetCf(), key, value)

	return d.diskTbl.DoBatch(batch)
}

func (d *nodeDiskTable) DeleteDisk(diskID proto.DiskID) error {
	key := diskID.Encode()
	value, err := d.diskTbl.Get(key)
	if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
		return err
	}
	// already delete, then return
	if errors.Is(err, kvstore.ErrNotFound) {
		return nil
	}

	info, err := d.rd.unmarshalDiskRecord(value)
	if err != nil {
		return err
	}

	batch := d.diskTbl.NewWriteBatch()
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
	batch.DeleteCF(d.diskTbl.GetCf(), key)

	return d.diskTbl.DoBatch(batch)
}

func (d *nodeDiskTable) ListDisksByDiskTbl(marker proto.DiskID, count int, listCallback func(i interface{})) error {
	snap := d.diskTbl.NewSnapshot()
	defer d.diskTbl.ReleaseSnapshot(snap)
	iter := d.diskTbl.NewIterator(snap)
	defer iter.Close()

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
		info, err := d.rd.unmarshalDiskRecord(iter.Value().Data())
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

// GetAllDroppingDisk return all drop disk in memory
func (d *nodeDiskTable) GetAllDroppingDisk() ([]proto.DiskID, error) {
	iter := d.droppedDiskTbl.NewIterator(nil)
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
func (d *nodeDiskTable) AddDroppingDisk(diskID proto.DiskID) error {
	key := diskID.Encode()
	return d.droppedDiskTbl.Put(kvstore.KV{Key: key, Value: uselessVal})
}

// DroppedDisk finish dropping in a disk and set disk status dropped
func (d *nodeDiskTable) DroppedDisk(diskID proto.DiskID) error {
	status := proto.DiskStatusDropped
	key := diskID.Encode()
	value, err := d.diskTbl.Get(key)
	if err != nil {
		return errors.Info(err, "get disk failed").Detail(err)
	}

	info, err := d.rd.unmarshalDiskRecord(value)
	if err != nil {
		return errors.Info(err, "decode disk failed").Detail(err)
	}
	diskInfo := d.rd.diskInfo(info)

	// delete old index and insert new index and disk info
	// we put all this on a write batch
	batch := d.diskTbl.NewWriteBatch()
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
	diskInfo.LastUpdateAt = time.Now()
	value, err = d.rd.marshalDiskRecord(info)
	if err != nil {
		return errors.Info(err, "encode disk failed").Detail(err)
	}
	batch.PutCF(d.diskTbl.GetCf(), key, value)

	// delete dropping disk
	batch.DeleteCF(d.droppedDiskTbl.GetCf(), key)

	return d.diskTbl.DoBatch(batch)
}

// IsDroppingDisk find a dropping disk if exist
func (d *nodeDiskTable) IsDroppingDisk(diskID proto.DiskID) (exist bool, err error) {
	key := diskID.Encode()
	_, err = d.droppedDiskTbl.Get(key)
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

func (d *nodeDiskTable) GetAllNodes(listCallback func(i interface{})) error {
	iter := d.nodeTbl.NewIterator(nil)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}
		if iter.Key().Size() > 0 {
			info, err := d.rd.unmarshalNodeRecord(iter.Value().Data())
			iter.Key().Free()
			iter.Value().Free()
			if err != nil {
				return errors.Info(err, "decode node info db failed").Detail(err)
			}
			listCallback(info)
		}
	}
	return nil
}

func (d *nodeDiskTable) UpdateNode(nodeID proto.NodeID, info interface{}) error {
	key := nodeID.Encode()
	value, err := d.rd.marshalNodeRecord(info)
	if err != nil {
		return err
	}

	err = d.nodeTbl.Put(kvstore.KV{Key: key, Value: value})
	if err != nil {
		return err
	}
	return nil
}

func (d *nodeDiskTable) GetNode(nodeID proto.NodeID) (info interface{}, err error) {
	key := nodeID.Encode()
	v, err := d.nodeTbl.Get(key)
	if err != nil {
		return nil, err
	}
	info, err = d.rd.unmarshalNodeRecord(v)
	if err != nil {
		return
	}

	return
}

// GetAllDroppingNode return all drop node in memory
func (d *nodeDiskTable) GetAllDroppingNode() ([]proto.NodeID, error) {
	iter := d.droppedNodeTbl.NewIterator(nil)
	defer iter.Close()
	ret := make([]proto.NodeID, 0)
	var nodeID proto.NodeID
	iter.SeekToFirst()
	for iter.Valid() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		ret = append(ret, nodeID.Decode(iter.Key().Data()))
		iter.Key().Free()
		iter.Value().Free()
		iter.Next()
	}
	return ret, nil
}

// AddDroppingNode add a dropping node
func (d *nodeDiskTable) AddDroppingNode(nodeID proto.NodeID) error {
	key := nodeID.Encode()
	return d.droppedNodeTbl.Put(kvstore.KV{Key: key, Value: uselessVal})
}

// DroppedNode finish dropping node and set status dropped
func (d *nodeDiskTable) DroppedNode(nodeID proto.NodeID) error {
	batch := d.nodeTbl.NewWriteBatch()
	defer batch.Destroy()

	key := nodeID.Encode()
	value, err := d.nodeTbl.Get(key)
	if err != nil {
		return errors.Info(err, "get node failed").Detail(err)
	}
	info, err := d.rd.unmarshalNodeRecord(value)
	if err != nil {
		return errors.Info(err, "decode node failed").Detail(err)
	}
	nodeInfo := d.rd.nodeInfo(info)
	nodeInfo.Status = proto.NodeStatusDropped
	value, err = d.rd.marshalNodeRecord(info)
	if err != nil {
		return err
	}
	batch.PutCF(d.nodeTbl.GetCf(), key, value)

	// delete dropping node
	batch.DeleteCF(d.droppedNodeTbl.GetCf(), key)

	return d.nodeTbl.DoBatch(batch)
}

// IsDroppingNode find a dropping node if exist
func (d *nodeDiskTable) IsDroppingNode(nodeID proto.NodeID) (exist bool, err error) {
	key := nodeID.Encode()
	_, err = d.droppedNodeTbl.Get(key)
	if err == kvstore.ErrNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}
	exist = true
	return
}

// UpdateNodeHostAndRack update node host and rack, and simultaneously updates the host and rack indexes of the disks
func (d *nodeDiskTable) UpdateNodeHostAndRack(node clustermgr.NodeInfo, diskIDs []proto.DiskID) error {
	nodeID := node.NodeID
	host := node.Host
	rack := node.Rack
	key := nodeID.Encode()
	v, err := d.nodeTbl.Get(key)
	if err != nil {
		return err
	}
	info, err := d.rd.unmarshalNodeRecord(v)
	if err != nil {
		return err
	}
	nodeInfo := d.rd.nodeInfo(info)
	nodeInfo.Host = host
	nodeInfo.Rack = rack
	value, err := d.rd.marshalNodeRecord(info)
	if err != nil {
		return err
	}

	// update node info and delete old index and insert new index
	// we put all these in a write batch
	batch := d.nodeTbl.NewWriteBatch()
	defer batch.Destroy()

	batch.PutCF(d.nodeTbl.GetCf(), key, value)

	for _, diskID := range diskIDs {
		key = diskID.Encode()
		value, err = d.diskTbl.Get(key)
		if err != nil {
			return errors.Info(err, "get disk failed").Detail(err)
		}
		info, err = d.rd.unmarshalDiskRecord(value)
		if err != nil {
			return errors.Info(err, "decode disk failed").Detail(err)
		}
		diskInfo := d.rd.diskInfo(info)

		// generate old and new index key
		oldIndexKey := ""
		newIndexKey := ""
		indexName := d.indexes[diskHostIndex].indexNames[0]
		oldIndexKey += genIndexKey(indexName, diskInfo.Host)
		oldIndexKey += diskID.ToString()
		newIndexKey += genIndexKey(indexName, host)
		newIndexKey += diskID.ToString()
		batch.DeleteCF(d.indexes[diskHostIndex].tbl.GetCf(), []byte(oldIndexKey))
		batch.PutCF(d.indexes[diskHostIndex].tbl.GetCf(), []byte(newIndexKey), key)

		oldIndexKey = ""
		newIndexKey = ""
		indexName = d.indexes[diskIDCRACKIndex].indexNames[0]
		oldIndexKey += genIndexKey(indexName, diskInfo.Idc)
		newIndexKey += genIndexKey(indexName, diskInfo.Idc)
		indexName = d.indexes[diskIDCRACKIndex].indexNames[1]
		oldIndexKey += genIndexKey(indexName, diskInfo.Rack)
		newIndexKey += genIndexKey(indexName, rack)
		oldIndexKey += diskID.ToString()
		newIndexKey += diskID.ToString()
		batch.DeleteCF(d.indexes[diskIDCRACKIndex].tbl.GetCf(), []byte(oldIndexKey))
		batch.PutCF(d.indexes[diskIDCRACKIndex].tbl.GetCf(), []byte(newIndexKey), key)
	}

	return d.nodeTbl.DoBatch(batch)
}

func genIndexKey(indexName string, indexValue interface{}) string {
	return fmt.Sprintf(indexName+seperateChar+"%v"+seperateChar, indexValue)
}
