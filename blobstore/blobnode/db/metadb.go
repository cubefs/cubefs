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
	"time"

	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	bncom "github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/limitio"
	pri "github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	_limited = "metalimited"
)

const (
	_bufferDepth = 128
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
	SetIOStat(stat *flow.IOFlowStat)
	SetHandleIOError(handler func(err error))
	Close(ctx context.Context) error
}

type MetaDBWapper struct {
	*metadb
}

type metadb struct {
	lock sync.RWMutex
	db   rdb.KVStore
	path string

	writeReqs chan Request
	delReqs   chan Request
	config    MetaConfig

	iostat  *flow.IOFlowStat // io visualization
	limiter []*rate.Limiter  // io limiter

	closeCh chan struct{}
	closed  bool

	onClosed      func()
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

func (md *metadb) SetIOStat(stat *flow.IOFlowStat) {
	md.iostat = stat
}

func (md *metadb) Get(ctx context.Context, key []byte) (value []byte, err error) {
	mgr, iot := md.getiotype(ctx)

	mgr.ReadBegin(1)
	defer mgr.ReadEnd(time.Now())

	md.applyToken(ctx, iot)

	value, err = md.db.Get(key)
	md.handleError(err)
	return
}

func (md *metadb) DirectPut(ctx context.Context, kv rdb.KV) (err error) {
	err = md.db.Put(kv)
	md.handleError(err)
	return
}

func (md *metadb) DirectDelete(ctx context.Context, key []byte) (err error) {
	err = md.db.Delete(key)
	md.handleError(err)
	return
}

func (md *metadb) Flush(ctx context.Context) (err error) {
	err = md.db.Flush()
	md.handleError(err)
	return
}

func (md *metadb) Put(ctx context.Context, kv rdb.KV) (err error) {
	req := Request{
		Type: msgPut,
		Data: kv,
	}

	mgr, iot := md.getiotype(ctx)

	mgr.WriteBegin(1)
	defer mgr.WriteEnd(time.Now())

	resCh := req.Register()

	md.applyToken(ctx, iot)
	md.writeReqs <- req

	select {
	case <-md.closeCh:
		return ErrStopped
	case x := <-resCh:
		return x.err
	}
}

func (md *metadb) Delete(ctx context.Context, key []byte) (err error) {
	req := Request{
		Type: msgDel,
		Data: rdb.KV{Key: key},
	}

	mgr, iot := md.getiotype(ctx)

	mgr.WriteBegin(1)
	defer mgr.WriteEnd(time.Now())

	resCh := req.Register()

	md.applyToken(ctx, iot)
	md.delReqs <- req

	select {
	case <-md.closeCh:
		return ErrStopped
	case x := <-resCh:
		return x.err
	}
}

func (md *metadb) DeleteRange(ctx context.Context, start, end []byte) (err error) {
	req := Request{
		Type: msgDelRange,
		Data: rdb.Range{Start: start, Limit: end},
	}

	mgr, iot := md.getiotype(ctx)

	mgr.WriteBegin(1)
	defer mgr.WriteEnd(time.Now())

	resCh := req.Register()

	md.applyToken(ctx, iot)
	md.delReqs <- req

	select {
	case <-md.closeCh:
		return ErrStopped
	case x := <-resCh:
		return x.err
	}
}

func (md *metadb) NewIterator(ctx context.Context, opts ...rdb.OpOption) rdb.Iterator {
	return md.db.NewIterator(nil, opts...)
}

func (md *metadb) Close(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	md.lock.Lock()
	defer md.lock.Unlock()

	span.Infof("=== meta db:%v close ===", md.path)

	if md.closed {
		span.Panicf("can not happened. meta:%v", md.path)
		return
	}

	if md.onClosed != nil {
		md.onClosed()
	}

	if md.closeCh != nil {
		close(md.closeCh)
	}

	db := md.db
	md.db = nil

	err = db.Close()
	if err != nil {
		span.Errorf("Failed close meta:%s, err:%v", md.path, err)
	}

	md.closed = true

	return err
}

func (md *metadb) loopWorker() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop "+md.path)

	for {
		var reqs []Request

		select {
		case <-md.closeCh:
			span.Warn("end the loop.")
			return
		case req := <-md.writeReqs:
			reqs = append(reqs, req)
			md.processRequests(ctx, reqs)
		case req := <-md.delReqs:
			reqs = append(reqs, req)
			md.processRequests(ctx, reqs)
		}
	}
}

func batchPopChannel(list []Request, reqChan chan Request, limit int) []Request {
	if limit < 1 {
		limit = 1
	}
	for i := 0; i < limit; i++ {
		var done bool
		select {
		case req := <-reqChan:
			list = append(list, req)
		default:
			done = true
		}
		if done {
			break
		}
	}

	return list
}

func (md *metadb) processRequests(ctx context.Context, reqs []Request) {
	span := trace.SpanFromContextSafe(ctx)

	batchCnt := int(md.config.BatchProcessCount)
	writeCnt := int(float64(batchCnt) * md.config.WritePriRatio)

	limit := writeCnt
	reqs = batchPopChannel(reqs, md.writeReqs, limit)

	limit = batchCnt - len(reqs)
	reqs = batchPopChannel(reqs, md.delReqs, limit)

	if err := md.doBatch(ctx, reqs); err != nil {
		span.Errorf("Failed doBatch reqs:%d, writeCnt:%d err:%v", len(reqs), writeCnt, err)
	}
}

func (md *metadb) doBatch(ctx context.Context, reqs []Request) (err error) {
	writeBatch := md.db.NewWriteBatch()
	defer writeBatch.Destroy()

	for _, req := range reqs {
		switch req.Type {
		case msgPut:
			kv := req.Data.(rdb.KV)
			writeBatch.Put(kv.Key, kv.Value)
		case msgDel:
			kv := req.Data.(rdb.KV)
			writeBatch.Delete(kv.Key)
		case msgDelRange:
			r := req.Data.(rdb.Range)
			writeBatch.DeleteRange(r.Start, r.Limit)
		}
	}

	err = md.db.DoBatch(writeBatch)
	if err != nil {
		md.handleError(err)
	}

	for _, req := range reqs {
		req.Trigger(Result{err: err})
	}

	return err
}

func (md *metadb) getiotype(ctx context.Context) (iostat.StatMgrAPI, bnapi.IOType) {
	iot := bnapi.Getiotype(ctx)
	mgr := md.iostat.GetStatMgr(iot)
	return mgr, iot
}

func (md *metadb) applyToken(ctx context.Context, iot bnapi.IOType) (n int64) {
	priority := pri.GetPriority(iot)

	limiter := md.limiter[priority]
	if limiter == nil {
		return
	}
	now := time.Now()
	reserve := limiter.ReserveN(now, 1)
	delay := reserve.DelayFrom(now)
	if delay == 0 {
		return
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	addTrackTag(ctx, _limited)

	select {
	case <-t.C:
		return
	case <-ctx.Done():
		reserve.Cancel()
		return
	}
}

func addTrackTag(ctx context.Context, name string) {
	if open := limitio.IsLimitTrack(ctx); open {
		limitio.AddTrackTag(ctx, name)
	}
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

	db, err = rdb.OpenDB(path, conf.Sync, func(option *rdb.RocksDBOption) {
		*option = conf.RocksdbOption
	})
	if err != nil {
		return
	}

	return db, nil
}

func newMetaDB(dirpath string, config MetaConfig) (md *metadb, err error) {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "NewKVDB "+dirpath)

	span.Infof("dirpath:%s, config:%v", dirpath, config)

	err = initConfig(&config)
	if err != nil {
		span.Errorf("Failed initconfig. err:%v", err)
		return
	}

	rocksdb, err := newRocksDB(dirpath, config)
	if err != nil {
		span.Errorf("Failed New Rocksdb %v", err)
		return nil, err
	}

	md = &metadb{
		db:        rocksdb,
		path:      dirpath,
		writeReqs: make(chan Request, _bufferDepth),
		delReqs:   make(chan Request, _bufferDepth),
		closeCh:   make(chan struct{}),
		config:    config,
	}

	priLevels := pri.GetLevels()
	md.limiter = make([]*rate.Limiter, len(priLevels))

	for priority, name := range priLevels {
		para, exist := config.MetaQos[name]
		if !exist || para.Iops <= 0 {
			// No flow control by default without configuration
			continue
		}
		controller := rate.NewLimiter(rate.Limit(para.Iops), 2*int(para.Iops))
		md.limiter[priority] = controller
	}

	span.Debugf("New KV(%s) DB(%v) success", dirpath, config)

	go md.loopWorker()

	return md, nil
}

func NewMetaHandler(dirpath string, config MetaConfig) (mh MetaHandler, err error) {
	md, err := newMetaDB(dirpath, config)
	if err != nil {
		return nil, err
	}

	w := &MetaDBWapper{metadb: md}

	return w, nil
}
