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

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	NodeInfoVersionNormal = iota + 1
)

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

type BlobNodeInfoRecord struct {
	NodeInfoRecord
}

type BlobNodeTable struct {
	nodeTbl        kvstore.KVTable
	droppedNodeTbl kvstore.KVTable
}

func OpenBlobNodeTable(db kvstore.KVStore) (*BlobNodeTable, error) {
	if db == nil {
		return nil, errors.New("OpenBlobNodeTable failed: db is nil")
	}
	table := &BlobNodeTable{
		nodeTbl:        db.Table(nodeCF),
		droppedNodeTbl: db.Table(nodeDropCF),
	}
	return table, nil
}

func (s *BlobNodeTable) GetAllNodes() ([]*BlobNodeInfoRecord, error) {
	iter := s.nodeTbl.NewIterator(nil)
	defer iter.Close()

	ret := make([]*BlobNodeInfoRecord, 0)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		if iter.Key().Size() > 0 {
			info, err := s.decodeNodeInfoRecord(iter.Value().Data())
			if err != nil {
				return nil, errors.Info(err, "decode node info db failed").Detail(err)
			}
			ret = append(ret, info)
			iter.Key().Free()
			iter.Value().Free()
		}
	}
	return ret, nil
}

func (s *BlobNodeTable) UpdateNode(info *BlobNodeInfoRecord) error {
	key := info.NodeID.Encode()
	value, err := s.encodeNodeInfoRecord(info)
	if err != nil {
		return err
	}

	err = s.nodeTbl.Put(kvstore.KV{Key: key, Value: value})
	if err != nil {
		return err
	}
	return nil
}

// GetAllDroppingNode return all drop node in memory
func (s *BlobNodeTable) GetAllDroppingNode() ([]proto.NodeID, error) {
	iter := s.droppedNodeTbl.NewIterator(nil)
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
func (s *BlobNodeTable) AddDroppingNode(nodeID proto.NodeID) error {
	key := nodeID.Encode()
	return s.droppedNodeTbl.Put(kvstore.KV{Key: key, Value: uselessVal})
}

// DroppedNode finish dropping node and set status dropped
func (s *BlobNodeTable) DroppedNode(nodeID proto.NodeID) error {
	batch := s.nodeTbl.NewWriteBatch()
	defer batch.Destroy()

	key := nodeID.Encode()
	value, err := s.nodeTbl.Get(key)
	if err != nil {
		return errors.Info(err, "get node failed").Detail(err)
	}
	info, err := s.decodeNodeInfoRecord(value)
	if err != nil {
		return errors.Info(err, "decode node failed").Detail(err)
	}
	info.Status = proto.NodeStatusDropped
	value, err = s.encodeNodeInfoRecord(info)
	if err != nil {
		return err
	}
	batch.PutCF(s.nodeTbl.GetCf(), key, value)

	// delete dropping disk
	batch.DeleteCF(s.droppedNodeTbl.GetCf(), key)

	return s.nodeTbl.DoBatch(batch)
}

// IsDroppingNode find a dropping node if exist
func (s *BlobNodeTable) IsDroppingNode(nodeID proto.NodeID) (exist bool, err error) {
	key := nodeID.Encode()
	_, err = s.droppedNodeTbl.Get(key)
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

func (s *BlobNodeTable) encodeNodeInfoRecord(info *BlobNodeInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func (s *BlobNodeTable) decodeNodeInfoRecord(data []byte) (*BlobNodeInfoRecord, error) {
	version := data[0]
	if version == NodeInfoVersionNormal {
		ret := &BlobNodeInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid node info version")
}
