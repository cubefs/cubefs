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

package kvmgr

import (
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/kvdb"
)

const moduleName = "kv manager"

var (
	defaultMaxListCount     = 1000
	defaultApplyConcurrency = 20
)

type KvMgrAPI interface {
	Get(key string) (val []byte, err error)
	Set(key string, value []byte) (err error)
	List(opt *clustermgr.ListKvOpts) (ret *clustermgr.ListKvRet, err error)
	Delete(key string) (err error)
}

type KvMgr struct {
	module   string
	tbl      *kvdb.KvTable
	taskPool *base.TaskDistribution
}

func NewKvMgr(db *kvdb.KvDB) (*KvMgr, error) {
	kvTbl, err := kvdb.OpenKvTable(db)
	if err != nil {
		return nil, err
	}
	t := &KvMgr{
		tbl:      kvTbl,
		module:   moduleName,
		taskPool: base.NewTaskDistribution(defaultApplyConcurrency, 1),
	}
	return t, nil
}

func (t *KvMgr) Get(key string) (val []byte, err error) {
	val, err = t.tbl.Get([]byte(key))
	return
}

func (t *KvMgr) Set(key string, value []byte) (err error) {
	err = t.tbl.Set([]byte(key), value)
	return
}

func (t *KvMgr) List(opts *clustermgr.ListKvOpts) (ret *clustermgr.ListKvRet, err error) {
	if opts == nil {
		return nil, nil
	}
	if opts.Count > defaultMaxListCount {
		opts.Count = defaultMaxListCount
	}
	if opts.Count <= 0 {
		opts.Count = 10
	}
	kvs, err := t.tbl.List(opts)
	if err != nil {
		return
	}

	ret = &clustermgr.ListKvRet{
		Kvs:    kvs,
		Marker: "",
	}
	if len(kvs) == opts.Count {
		ret.Marker = kvs[len(kvs)-1].Key
	}

	return
}

func (t *KvMgr) Delete(key string) (err error) {
	return t.tbl.Delete([]byte(key))
}
