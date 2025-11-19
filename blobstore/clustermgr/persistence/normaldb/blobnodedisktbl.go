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

type BlobNodeInfoRecord struct {
	NodeInfoRecord
}

type BlobNodeDiskInfoRecord struct {
	DiskInfoRecord
	MaxChunkCnt          int64 `json:"max_chunk_cnt"`
	FreeChunkCnt         int64 `json:"free_chunk_cnt"`
	OversoldFreeChunkCnt int64 `json:"oversold_free_chunk_cnt"`
	UsedChunkCnt         int64 `json:"used_chunk_cnt"`
	Size                 int64 `json:"size"`
	Used                 int64 `json:"used"`
	Free                 int64 `json:"free"`
}

func OpenBlobNodeDiskTable(db kvstore.KVStore, ensureIndex bool) (*BlobNodeDiskTable, error) {
	if db == nil {
		return nil, errors.New("OpenBlobNodeDiskTable failed: db is nil")
	}
	table := &BlobNodeDiskTable{
		nodeDiskTable: &nodeDiskTable{
			nodeTbl:        db.Table(nodeCF),
			droppedNodeTbl: db.Table(nodeDropCF),
			diskTbl:        db.Table(diskCF),
			droppedDiskTbl: db.Table(diskDropCF),
			indexes: map[string]indexItem{
				diskStatusIndex:  {indexNames: []string{diskStatusIndex}, tbl: db.Table(diskStatusIndexCF)},
				diskHostIndex:    {indexNames: []string{diskHostIndex}, tbl: db.Table(diskHostIndexCF)},
				diskIDCIndex:     {indexNames: []string{diskIDCIndex}, tbl: db.Table(diskIDCIndexCF)},
				diskIDCRACKIndex: {indexNames: strings.Split(diskIDCRACKIndex, "-"), tbl: db.Table(diskIDCRackIndexCF)},
			},
		},
	}
	table.nodeDiskTable.rd = table

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
	nodeDiskTable *nodeDiskTable
}

func (b *BlobNodeDiskTable) GetDisk(diskID proto.DiskID) (info *BlobNodeDiskInfoRecord, err error) {
	ret, err := b.nodeDiskTable.GetDisk(diskID)
	if err != nil {
		return nil, err
	}
	return ret.(*BlobNodeDiskInfoRecord), nil
}

func (b *BlobNodeDiskTable) GetAllDisks() ([]*BlobNodeDiskInfoRecord, error) {
	ret := make([]*BlobNodeDiskInfoRecord, 0)
	err := b.nodeDiskTable.ListDisksByDiskTbl(0, 0, func(i interface{}) {
		ret = append(ret, i.(*BlobNodeDiskInfoRecord))
	})
	return ret, err
}

func (b *BlobNodeDiskTable) ListDisk(opt *clustermgr.ListOptionArgs) ([]*BlobNodeDiskInfoRecord, error) {
	ret := make([]*BlobNodeDiskInfoRecord, 0)
	err := b.nodeDiskTable.ListDisk(opt, func(i interface{}) {
		ret = append(ret, i.(*BlobNodeDiskInfoRecord))
	})
	return ret, err
}

func (b *BlobNodeDiskTable) AddDisk(disk *BlobNodeDiskInfoRecord) error {
	return b.nodeDiskTable.AddDisk(disk.DiskID, disk)
}

func (b *BlobNodeDiskTable) UpdateDisk(diskID proto.DiskID, disk *BlobNodeDiskInfoRecord) error {
	return b.nodeDiskTable.UpdateDisk(diskID, disk)
}

func (b *BlobNodeDiskTable) UpdateDiskStatus(diskID proto.DiskID, status proto.DiskStatus) error {
	return b.nodeDiskTable.UpdateDiskStatus(diskID, status)
}

// GetAllDroppingDisk return all drop disk in memory
func (b *BlobNodeDiskTable) GetAllDroppingDisk() ([]proto.DiskID, error) {
	return b.nodeDiskTable.GetAllDroppingDisk()
}

// AddDroppingDisk add a dropping disk
func (b *BlobNodeDiskTable) AddDroppingDisk(diskID proto.DiskID) error {
	return b.nodeDiskTable.AddDroppingDisk(diskID)
}

// DroppedDisk finish dropping in a disk and set disk status dropped
func (b *BlobNodeDiskTable) DroppedDisk(diskID proto.DiskID) error {
	return b.nodeDiskTable.DroppedDisk(diskID)
}

// IsDroppingDisk find a dropping disk if exist
func (b *BlobNodeDiskTable) IsDroppingDisk(diskID proto.DiskID) (exist bool, err error) {
	return b.nodeDiskTable.IsDroppingDisk(diskID)
}

func (b *BlobNodeDiskTable) GetAllNodes() ([]*BlobNodeInfoRecord, error) {
	ret := make([]*BlobNodeInfoRecord, 0)
	err := b.nodeDiskTable.GetAllNodes(func(i interface{}) {
		ret = append(ret, i.(*BlobNodeInfoRecord))
	})
	return ret, err
}

func (b *BlobNodeDiskTable) UpdateNode(info *BlobNodeInfoRecord) error {
	return b.nodeDiskTable.UpdateNode(info.NodeID, info)
}

// GetAllDroppingNode return all drop node in memory
func (b *BlobNodeDiskTable) GetAllDroppingNode() ([]proto.NodeID, error) {
	return b.nodeDiskTable.GetAllDroppingNode()
}

// AddDroppingNode add a dropping node
func (b *BlobNodeDiskTable) AddDroppingNode(nodeID proto.NodeID) error {
	return b.nodeDiskTable.AddDroppingNode(nodeID)
}

// DroppedNode finish dropping node and set status dropped
func (b *BlobNodeDiskTable) DroppedNode(nodeID proto.NodeID) error {
	return b.nodeDiskTable.DroppedNode(nodeID)
}

// IsDroppingNode find a dropping node if exist
func (b *BlobNodeDiskTable) IsDroppingNode(nodeID proto.NodeID) (exist bool, err error) {
	return b.nodeDiskTable.IsDroppingNode(nodeID)
}

// UpdateNodeHostAndRack update node host and rack
func (b *BlobNodeDiskTable) UpdateNodeHostAndRack(nodeInfo clustermgr.NodeInfo, diskIDs []proto.DiskID) error {
	return b.nodeDiskTable.UpdateNodeHostAndRack(nodeInfo, diskIDs)
}

func (b *BlobNodeDiskTable) unmarshalDiskRecord(data []byte) (interface{}, error) {
	version := data[0]
	if version == DiskInfoVersionNormal {
		ret := &BlobNodeDiskInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		if err != nil {
			return nil, err
		}
		ret.Version = version
		// compatible case
		n, err := b.nodeDiskTable.GetNode(ret.NodeID)
		if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
			return nil, err
		}
		if errors.Is(err, kvstore.ErrNotFound) {
			return ret, nil
		}
		nodeInfo := n.(*BlobNodeInfoRecord)
		ret.Idc = nodeInfo.Idc
		ret.Rack = nodeInfo.Rack
		ret.Host = nodeInfo.Host
		return ret, nil
	}
	return nil, errors.New("invalid disk info version")
}

func (b *BlobNodeDiskTable) marshalDiskRecord(v interface{}) ([]byte, error) {
	info := *v.(*BlobNodeDiskInfoRecord)
	if _, err := b.nodeDiskTable.GetNode(info.NodeID); err == nil { // compatible case
		info.Idc, info.Rack, info.Host = "", "", ""
	}
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

func (b *BlobNodeDiskTable) unmarshalNodeRecord(data []byte) (interface{}, error) {
	version := data[0]
	if version == NodeInfoVersionNormal {
		ret := &BlobNodeInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid node info version")
}

func (b *BlobNodeDiskTable) marshalNodeRecord(v interface{}) ([]byte, error) {
	info := v.(*BlobNodeInfoRecord)
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func (b *BlobNodeDiskTable) nodeInfo(i interface{}) *NodeInfoRecord {
	return &i.(*BlobNodeInfoRecord).NodeInfoRecord
}
