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

package base

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
)

type SnapshotDB interface {
	GetAllCfNames() []string
	kvstore.KVStore
}

type raftSnapshot struct {
	name       string
	current    int
	items      []SnapshotItem
	dbs        map[string]SnapshotDB
	applyIndex uint64
	patchNum   int

	lock          sync.RWMutex
	closeCallback func()
}

type SnapshotItem struct {
	DbName string `json:"db_name"`
	CfName string `json:"cf_name"`
	iter   kvstore.Iterator
	snap   *kvstore.Snapshot
}

type SnapshotData struct {
	Header SnapshotItem
	Key    []byte
	Value  []byte
}

func (r *raftSnapshot) Read() (data []byte, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

ITERATE:

	dbName := r.items[r.current].DbName
	cfName := r.items[r.current].CfName
	iter := r.items[r.current].iter

	for i := 0; i < r.patchNum; i++ {
		if !iter.Valid() {
			break
		}
		if iter.Err() != nil {
			return nil, err
		}
		snapData := &SnapshotData{
			Header: SnapshotItem{
				DbName: dbName,
				CfName: cfName,
			},
			Key:   iter.Key().Data(),
			Value: iter.Value().Data(),
		}
		one, err := EncodeSnapshotData(snapData)
		iter.Key().Free()
		iter.Value().Free()
		if err != nil {
			return nil, err
		}
		data = append(data, one...)
		iter.Next()
	}
	// have data now, we can just return the result
	if len(data) > 0 {
		return
	}

	// get nothing above, and if not end, then iterate the next snapshot item
	if r.current < len(r.items)-1 {
		r.current += 1
		goto ITERATE
	}
	return nil, io.EOF
}

func (r *raftSnapshot) Name() string {
	return r.name
}

// close all rocksdb snapshot and iterator
func (r *raftSnapshot) Close() {
	// release all iterator and snapshot
	for _, item := range r.items {
		item.iter.Close()
		r.dbs[item.DbName].ReleaseSnapshot(item.snap)
	}
	// snapshot close callback
	r.closeCallback()
}

func (r *raftSnapshot) Index() uint64 {
	return r.applyIndex
}

func EncodeSnapshotData(src *SnapshotData) ([]byte, error) {
	b := bytes.NewBuffer(nil)
	dbNameSize := int32(len(src.Header.DbName))
	cfNameSize := int32(len(src.Header.CfName))
	keySize := int32(len(src.Key))
	valueSize := int32(len(src.Value))
	var err error

	if err = binary.Write(b, binary.BigEndian, dbNameSize); err != nil {
		return nil, err
	}
	if _, err = b.Write([]byte(src.Header.DbName)); err != nil {
		return nil, err
	}

	if err = binary.Write(b, binary.BigEndian, cfNameSize); err != nil {
		return nil, err
	}
	if _, err = b.Write([]byte(src.Header.CfName)); err != nil {
		return nil, err
	}

	if err = binary.Write(b, binary.BigEndian, keySize); err != nil {
		return nil, err
	}
	if _, err = b.Write(src.Key); err != nil {
		return nil, err
	}

	if err = binary.Write(b, binary.BigEndian, valueSize); err != nil {
		return nil, err
	}
	if _, err = b.Write(src.Value); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func DecodeSnapshotData(reader io.Reader) (ret *SnapshotData, err error) {
	_ret := &SnapshotData{}
	dbNameSize := int32(0)
	cfNameSize := int32(0)
	keySize := int32(0)
	valueSize := int32(0)

	if err = binary.Read(reader, binary.BigEndian, &dbNameSize); err != nil {
		return
	}
	dbName := make([]byte, dbNameSize)
	if _, err = reader.Read(dbName); err != nil {
		return
	}
	_ret.Header.DbName = string(dbName)

	if err = binary.Read(reader, binary.BigEndian, &cfNameSize); err != nil {
		return
	}
	cfName := make([]byte, cfNameSize)
	if _, err = reader.Read(cfName); err != nil {
		return
	}
	_ret.Header.CfName = string(cfName)

	if err = binary.Read(reader, binary.BigEndian, &keySize); err != nil {
		ret = nil
		return
	}
	key := make([]byte, keySize)
	if _, err = reader.Read(key); err != nil {
		return
	}
	_ret.Key = key

	if err = binary.Read(reader, binary.BigEndian, &valueSize); err != nil {
		ret = nil
		return
	}
	value := make([]byte, valueSize)
	if _, err = reader.Read(value); err != nil {
		return
	}
	_ret.Value = value

	ret = _ret
	return
}
