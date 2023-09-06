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

package db

import (
	"context"
	"errors"
	"os"
	"sync"

	bncom "github.com/cubefs/cubefs/blobstore/blobnode/base"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var (
	ErrStopped         = errors.New("db: err stopped")
	ErrShardMetaNotDir = errors.New("db: shard meta not directory")
	ErrWrongConfig     = errors.New("db: wrong config item")
)

type MetaHandler interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, kv rdb.KV) error
	Delete(ctx context.Context, key []byte) error
	DeleteRange(ctx context.Context, start, end []byte) error
	Flush(ctx context.Context) error
	NewIterator(ctx context.Context, opts ...rdb.OpOption) rdb.Iterator
	SetHandleIOError(handler func(err error))
	Close(ctx context.Context) error
}

type metadb struct {
	once          sync.Once
	db            rdb.KVStore
	path          string
	config        MetaConfig
	handleIOError func(err error)
}

func (md *metadb) handleError(err error) {
	if bncom.IsEIO(err) && md.handleIOError != nil {
		md.handleIOError(err)
	}
}

func (md *metadb) SetHandleIOError(handler func(err error)) {
	md.handleIOError = handler
}

func (md *metadb) Get(ctx context.Context, key []byte) (value []byte, err error) {
	value, err = md.db.Get(key)
	md.handleError(err)
	return
}

func (md *metadb) Put(ctx context.Context, kv rdb.KV) (err error) {
	err = md.db.Put(kv)
	md.handleError(err)
	return
}

func (md *metadb) Delete(ctx context.Context, key []byte) (err error) {
	err = md.db.Delete(key)
	md.handleError(err)
	return
}

func (md *metadb) Flush(ctx context.Context) (err error) {
	err = md.db.Flush()
	md.handleError(err)
	return
}

func (md *metadb) DeleteRange(ctx context.Context, start, end []byte) (err error) {
	err = md.db.DeleteRange(start, end)
	md.handleError(err)
	return
}

func (md *metadb) NewIterator(ctx context.Context, opts ...rdb.OpOption) rdb.Iterator {
	return md.db.NewIterator(nil, opts...)
}

func (md *metadb) Close(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	md.once.Do(func() {
		span.Infof("=== meta db:%v close ===", md.path)

		err = md.db.Close()
		if err != nil {
			span.Errorf("Failed close meta:%s, err:%v", md.path, err)
		}
	})

	return err
}

func newRocksDB(path string, conf MetaConfig) (db rdb.KVStore, err error) {
	if path == "" {
		return nil, bloberr.ErrInvalidParam
	}

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return nil, ErrShardMetaNotDir
	}

	db, err = rdb.OpenDB(path, rdb.WithCatchSize(conf.LRUCacheSize), rdb.WithSyncMode(conf.Sync))
	if err != nil {
		return
	}

	return db, nil
}

func newMetaDB(path string, config MetaConfig) (md *metadb, err error) {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "NewKVDB "+path)

	span.Infof("path:%s, config:%v", path, config)

	err = initConfig(&config)
	if err != nil {
		span.Errorf("Failed initconfig. err:%v", err)
		return
	}

	rocksdb, err := newRocksDB(path, config)
	if err != nil {
		span.Errorf("Failed New Rocksdb %v", err)
		return nil, err
	}

	md = &metadb{
		db:     rocksdb,
		path:   path,
		config: config,
	}

	span.Debugf("New KV(%s) DB(%v) success", path, config)

	return md, nil
}

func NewMetaHandler(dirpath string, config MetaConfig) (mh MetaHandler, err error) {
	return newMetaDB(dirpath, config)
}
