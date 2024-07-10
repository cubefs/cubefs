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
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type ShardNodeInfoRecord struct {
	NodeInfoRecord
	RaftHost string `json:"raft_host"`
}

type ShardNodeTable struct {
	tbl kvstore.KVTable
}

func OpenShardNodeTable(db kvstore.KVStore) (*ShardNodeTable, error) {
	if db == nil {
		return nil, errors.New("OpenBlobNodeTable failed: db is nil")
	}
	table := &ShardNodeTable{
		tbl: db.Table(nodeCF),
	}
	return table, nil
}

func (s *ShardNodeTable) GetAllNodes() ([]*ShardNodeInfoRecord, error) {
	iter := s.tbl.NewIterator(nil)
	defer iter.Close()

	ret := make([]*ShardNodeInfoRecord, 0)
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

func (s *ShardNodeTable) UpdateNode(info *ShardNodeInfoRecord) error {
	key := info.NodeID.Encode()
	value, err := s.encodeNodeInfoRecord(info)
	if err != nil {
		return err
	}

	err = s.tbl.Put(kvstore.KV{Key: key, Value: value})
	if err != nil {
		return err
	}
	return nil
}

func (s *ShardNodeTable) encodeNodeInfoRecord(info *ShardNodeInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func (s *ShardNodeTable) decodeNodeInfoRecord(data []byte) (*ShardNodeInfoRecord, error) {
	version := data[0]
	if version == NodeInfoVersionNormal {
		ret := &ShardNodeInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid node info version")
}
