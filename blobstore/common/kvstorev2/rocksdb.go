// Copyright 2023 The Cuber Authors.
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

package kvstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/util"

	rdb "github.com/tecbot/gorocksdb"
)

type (
	rocksdb struct {
		path        string
		db          *rdb.DB
		cfHandles   map[CF]*rdb.ColumnFamilyHandle
		handleError HandleError

		optHelper *optHelper
		opt       *rdb.Options
		ro        *rdb.ReadOptions
		wo        *rdb.WriteOptions
		fo        *rdb.FlushOptions
		lock      sync.RWMutex

		wg sync.WaitGroup

		wTaskPool   sync.Pool
		wchans      []chan *writeTask
		writeReqCnt int64

		rTaskPool  sync.Pool
		rchans     []chan *readTask
		readReqCnt int64
	}
	readOption struct {
		db   *rdb.DB
		snap *rdb.Snapshot
		opt  *rdb.ReadOptions
	}
	writeOption struct {
		opt *rdb.WriteOptions
	}
	lruCache struct {
		cache *rdb.Cache
	}
	writeBufferManager struct {
		manager *rdb.WriteBufferManager
	}
	optHelper struct {
		db   *rdb.DB
		opt  *Option
		lock sync.RWMutex
	}
	snapshot struct {
		db   *rdb.DB
		snap *rdb.Snapshot
	}
	listReader struct {
		iterator    *rdb.Iterator
		prefix      []byte
		marker      []byte
		filterKeys  [][]byte
		isFirst     bool
		handleError HandleError
	}
	keyGetter struct {
		key *rdb.Slice
	}
	valueGetter struct {
		index int
		value *rdb.Slice
	}
	env struct {
		*rdb.Env
	}
	sstFileManager struct {
		*rdb.SstFileManager
	}
	writeBatch struct {
		s     *rocksdb
		batch *rdb.WriteBatch
	}
)

type opType int

const (
	cfPutEvent opType = iota + 1
	cfDeleteEvent
	cfRangeDeleteEvent
	batchEvent
	cfGetRaw
	cfGet
	read
	multiGet
)

func newRocksdb(ctx context.Context, path string, option *Option) (Store, error) {
	if path == "" {
		return nil, errors.New("path is empty")
	}
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		return nil, err
	}

	dbOpt := genRocksdbOpts(option)

	cfNum := len(option.ColumnFamily) + 1
	cols := make([]CF, 0, cfNum)
	cols = append(cols, defaultCF)
	cols = append(cols, option.ColumnFamily...)

	cfNames := make([]string, 0, cfNum)
	cfOpts := make([]*rdb.Options, 0, cfNum)
	for i := 0; i < cfNum; i++ {
		cfNames = append(cfNames, cols[i].String())
		cfOpts = append(cfOpts, dbOpt)
	}

	db, cfhs, err := rdb.OpenDbColumnFamilies(dbOpt, path, cfNames, cfOpts)
	if err != nil {
		return nil, err
	}

	cfhMap := make(map[CF]*rdb.ColumnFamilyHandle)
	for i, h := range cfhs {
		cfhMap[cols[i]] = h
	}

	wo := rdb.NewDefaultWriteOptions()
	wo.DisableWAL(option.DisableWal)
	if option.Sync {
		wo.SetSync(option.Sync)
	}
	ro := rdb.NewDefaultReadOptions()

	option.ReadConcurrency = util.Max(defaultReadConcurrency, option.ReadConcurrency)
	option.ReadQueueLen = util.Max(defaultReadQueueLen, option.ReadQueueLen)
	option.WriteConcurrency = util.Max(defaultWriteConcurrency, option.WriteConcurrency)
	option.WriteQueueLen = util.Max(defaultWriteQueueLen, option.WriteQueueLen)

	ins := &rocksdb{
		db:          db,
		path:        path,
		optHelper:   &optHelper{db: db, opt: option},
		opt:         dbOpt,
		ro:          ro,
		wo:          wo,
		fo:          rdb.NewDefaultFlushOptions(),
		cfHandles:   cfhMap,
		handleError: option.HandleError,

		rTaskPool: sync.Pool{New: func() interface{} {
			return &readTask{retChan: make(chan readRet, 1)}
		}},

		rchans: make([]chan *readTask, option.ReadConcurrency),

		wTaskPool: sync.Pool{New: func() interface{} {
			return &writeTask{err: make(chan error, 1)}
		}},

		wchans: make([]chan *writeTask, option.WriteConcurrency),
	}
	for i := 0; i < option.WriteConcurrency; i++ {
		ins.wchans[i] = make(chan *writeTask, option.WriteQueueLen)
		ins.wg.Add(1)
		idx := i
		go ins.writeLoop(ins.wchans[idx])
	}

	for i := 0; i < option.ReadConcurrency; i++ {
		ins.rchans[i] = make(chan *readTask, option.ReadQueueLen)
		ins.wg.Add(1)
		idx := i
		go ins.readLoop(ins.rchans[idx])
	}
	return ins, nil
}

func (s *rocksdb) writeLoop(ch chan *writeTask) {
	defer s.wg.Done()

	tasks := make([]*writeTask, 0)
	wb := s.NewWriteBatch()

	for task := range ch {
		tasks = append(tasks, task)
		budget := len(ch)
		for budget > 0 {
			task := <-ch
			tasks = append(tasks, task)
			budget--
		}

		for i, task := range tasks {
			switch task.typ {
			case cfPutEvent:
				data := task.data
				wb.Put(data.cf, data.key, data.value)
			case cfDeleteEvent:
				data := task.data
				wb.Delete(data.cf, data.key)
			case cfRangeDeleteEvent:
				data := task.data
				wb.DeleteRange(data.cf, data.start, data.end)
			case batchEvent:
				tasks[i] = nil
				b := task.batch
				err := s.write(task.ctx, b, nil)
				task.err <- err
				s.reduceWriteReqCnt(1)
			default:
				// never to here
				panic("invalid type")
			}
		}
		if wb.Count() > 0 {
			err := s.write(context.Background(), wb, nil)
			for i := 0; i < len(tasks); i++ {
				if tasks[i] == nil {
					continue
				}
				tasks[i].err <- err
			}
			s.reduceWriteReqCnt(wb.Count())
		}
		wb.Clear()
		tasks = tasks[:0]
	}
}

func (s *rocksdb) readLoop(ch chan *readTask) {
	defer s.wg.Done()

	tasks := make([]*readTask, 0)
	idxes := make([]int, 0)
	cfs := make([]CF, 0)
	keys := make([][]byte, 0)

	for task := range ch {
		tasks = append(tasks, task)
		budget := len(ch)
		for budget > 0 {
			task := <-ch
			tasks = append(tasks, task)
			budget--
		}
		for i, task := range tasks {
			switch task.typ {
			case cfGet, cfGetRaw:
				// add to read
				idxes = append(idxes, i)
				keys = append(keys, task.key...)
				cfs = append(cfs, task.cf...)
			case read:
				vgs, err := s.read(task.ctx, task.cf, task.key, task.ro.opt)
				ret := readRet{
					value: vgs,
					err:   err,
				}
				task.retChan <- ret
				s.reduceReadReqCnt(1)
			case multiGet:
				vgs, err := s.multiGet(task.ctx, task.cf[0], task.key, task.ro.opt)
				ret := readRet{
					value: vgs,
					err:   err,
				}
				task.retChan <- ret
				s.reduceReadReqCnt(1)
			default:
				// never to here
				panic("invalid type")
			}
		}
		vgs, err := s.read(context.Background(), cfs, keys, nil)
		for i, idx := range idxes {
			ret := readRet{
				value: vgs[i],
				err:   err,
			}
			tasks[idx].retChan <- ret
		}

		s.reduceReadReqCnt(len(idxes))
		idxes = idxes[:0]
		keys = keys[:0]
		cfs = cfs[:0]
		tasks = tasks[:0]
	}
}

func (s *rocksdb) GetOptionHelper() OptionHelper {
	return s.optHelper
}

func (s *rocksdb) NewReadOption() ReadOption {
	opt := rdb.NewDefaultReadOptions()
	return &readOption{
		db:  s.db,
		opt: opt,
	}
}

func (s *rocksdb) NewWriteOption() WriteOption {
	return &writeOption{
		opt: rdb.NewDefaultWriteOptions(),
	}
}

func (s *rocksdb) NewSnapshot() Snapshot {
	return &snapshot{db: s.db, snap: s.db.NewSnapshot()}
}

func (ro *readOption) SetSnapShot(snap Snapshot) {
	ro.snap = snap.(*snapshot).snap
	ro.opt.SetSnapshot(ro.snap)
}

func (ro *readOption) Close() {
	ro.opt.Destroy()
}

func (wo *writeOption) SetSync(value bool) {
	wo.opt.SetSync(value)
}

func (wo *writeOption) DisableWAL(value bool) {
	wo.opt.DisableWAL(value)
}

func (wo *writeOption) Close() {
	wo.opt.Destroy()
}

func (c *lruCache) GetUsage() uint64 {
	return c.cache.GetUsage()
}

func (c *lruCache) GetPinnedUsage() uint64 {
	return c.cache.GetPinnedUsage()
}

func (c *lruCache) Close() {
	c.cache.Destroy()
}

func (m *writeBufferManager) Close() {
	m.manager.Destroy()
}

func (e *env) SetLowPriorityBackgroundThreads(n int) {
	e.SetBackgroundThreads(n)
}

func (e *env) Close() {
	e.Destroy()
}

func (e *sstFileManager) Close() {
	e.Destroy()
}

func (ss *snapshot) Close() {
	ss.db.ReleaseSnapshot(ss.snap)
}

func (kg keyGetter) Key() []byte {
	return kg.key.Data()
}

func (kg keyGetter) Close() {
	kg.key.Free()
}

func (vg *valueGetter) Value() []byte {
	return vg.value.Data()
}

func (vg *valueGetter) Read(b []byte) (n int, err error) {
	if vg.index >= len(vg.Value()) {
		return 0, io.EOF
	}
	n = copy(b, vg.Value()[vg.index:])
	vg.index += n
	return
}

func (vg *valueGetter) Size() int {
	return vg.value.Size()
}

func (vg *valueGetter) Close() {
	vg.value.Free()
}

func (lr *listReader) ReadNext() (key KeyGetter, val ValueGetter, err error) {
	if !lr.isFirst {
		// move into next kv
		lr.iterator.Next()
	}
	if err = lr.iterator.Err(); err != nil {
		lr.handleError(context.TODO(), err)
		return nil, nil, err
	}
	if !lr.iterator.Valid() {
		return nil, nil, nil
	}
	if lr.prefix == nil || lr.iterator.ValidForPrefix(lr.prefix) {
		kg := keyGetter{key: lr.iterator.Key()}
		vg := &valueGetter{value: lr.iterator.Value()}
		lr.isFirst = false
		if lr.filterKey(kg) {
			return lr.ReadNext()
		}
		return kg, vg, nil
	}
	return nil, nil, nil
}

func (lr *listReader) ReadNextCopy() (key []byte, value []byte, err error) {
	kg, vg, err := lr.ReadNext()
	if err != nil {
		lr.handleError(context.TODO(), err)
		return nil, nil, err
	}
	if kg != nil && vg != nil {
		key = make([]byte, len(kg.Key()))
		value = make([]byte, vg.Size())
		copy(key, kg.Key())
		copy(value, vg.Value())
		kg.Close()
		vg.Close()
		return
	}
	return
}

func (lr *listReader) ReadPrev() (key KeyGetter, val ValueGetter, err error) {
	if !lr.isFirst {
		// move into prev kv
		lr.iterator.Prev()
	}
	if err = lr.iterator.Err(); err != nil {
		lr.handleError(context.TODO(), err)
		return nil, nil, err
	}
	if !lr.iterator.Valid() {
		return nil, nil, nil
	}
	if lr.prefix == nil || lr.iterator.ValidForPrefix(lr.prefix) {
		kg := keyGetter{key: lr.iterator.Key()}
		vg := &valueGetter{value: lr.iterator.Value()}
		lr.isFirst = false
		if lr.filterKey(kg) {
			return lr.ReadPrev()
		}
		return kg, vg, nil
	}
	return nil, nil, nil
}

func (lr *listReader) ReadPrevCopy() (key []byte, value []byte, err error) {
	kg, vg, err := lr.ReadPrev()
	if err != nil {
		lr.handleError(context.TODO(), err)
		return nil, nil, err
	}
	if kg != nil && vg != nil {
		key = make([]byte, len(kg.Key()))
		value = make([]byte, vg.Size())
		copy(key, kg.Key())
		copy(value, vg.Value())
		kg.Close()
		vg.Close()
		return
	}
	return
}

func (lr *listReader) ReadLast() (key KeyGetter, val ValueGetter, err error) {
	lr.iterator.SeekToLast()
	for {
		if err = lr.iterator.Err(); err != nil {
			return
		}
		if !lr.iterator.Valid() {
			return
		}
		if lr.prefix != nil && !lr.iterator.ValidForPrefix(lr.prefix) {
			lr.iterator.Prev()
			continue
		}

		if lr.filterKey(keyGetter{key: lr.iterator.Key()}) {
			lr.iterator.Prev()
			continue
		}

		break
	}
	key = keyGetter{key: lr.iterator.Key()}
	val = &valueGetter{value: lr.iterator.Value()}
	return
}

func (lr *listReader) SeekToLast() {
	lr.iterator.SeekToLast()
}

func (lr *listReader) SeekForPrev(key []byte) (err error) {
	lr.iterator.SeekForPrev(key)
	if lr.prefix == nil || lr.marker != nil {
		return
	}
	for {
		if err = lr.iterator.Err(); err != nil {
			lr.handleError(context.TODO(), err)
			return
		}
		if !lr.iterator.Valid() {
			return
		}
		if lr.iterator.ValidForPrefix(lr.prefix) {
			return
		}
		lr.iterator.Prev()
	}
}

func (lr *listReader) Seek(key []byte) {
	lr.isFirst = true
	lr.iterator.Seek(key)
}

func (lr *listReader) SetFilterKey(key []byte) {
	lr.filterKeys = append(lr.filterKeys, key)
}

func (lr *listReader) Close() {
	lr.iterator.Close()
}

func (lr *listReader) filterKey(kg keyGetter) bool {
	if lr.filterKeys != nil {
		for i := range lr.filterKeys {
			if bytes.Equal(lr.filterKeys[i], kg.Key()) {
				return true
			}
		}
	}
	return false
}

func (w *writeBatch) Put(col CF, key, value []byte) {
	cf := w.s.getColumnFamily(col)
	w.batch.PutCF(cf, key, value)
}

func (w *writeBatch) Delete(col CF, key []byte) {
	cf := w.s.getColumnFamily(col)
	w.batch.DeleteCF(cf, key)
}

func (w *writeBatch) DeleteRange(col CF, startKey, endKey []byte) {
	cf := w.s.getColumnFamily(col)
	w.batch.DeleteRangeCF(cf, startKey, endKey)
}

func (w *writeBatch) Data() []byte {
	return w.batch.Data()
}

func (w *writeBatch) From(data []byte) {
	w.batch = rdb.WriteBatchFrom(data)
}

func (w *writeBatch) Count() int {
	return w.batch.Count()
}

func (w *writeBatch) Clear() {
	w.batch.Clear()
}

func (w *writeBatch) Close() {
	w.batch.Destroy()
}

func (s *rocksdb) NewWriteBatch() WriteBatch {
	return &writeBatch{
		s:     s,
		batch: rdb.NewWriteBatch(),
	}
}

func (s *rocksdb) CreateColumn(col CF) error {
	s.lock.Lock()
	if s.cfHandles[col] != nil {
		s.lock.Unlock()
		return nil
	}
	h, err := s.db.CreateColumnFamily(s.opt, col.String())
	if err != nil {
		s.lock.Unlock()
		return err
	}
	s.cfHandles[col] = h
	s.lock.Unlock()
	return nil
}

func (s *rocksdb) GetAllColumns() (ret []CF) {
	s.lock.RLock()
	for col := range s.cfHandles {
		ret = append(ret, col)
	}
	s.lock.RUnlock()
	return
}

func (s *rocksdb) CheckColumns(col CF) bool {
	if col == "" {
		return true
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.cfHandles[col]
	return ok
}

func (s *rocksdb) Get(ctx context.Context, col CF, key []byte, opts ...ReadOptFunc) (value ValueGetter, err error) {
	ro := &readOpts{}
	ro.applyOptions(opts)
	if ro.withNoMerge {
		return s.get(ctx, col, key, ro.opt)
	}

	task := s.newReadTask(ctx)
	task.typ = cfGet
	task.ro = ro
	task.cf = []CF{col}
	task.key = [][]byte{key}

	ch := s.acquireReadChan()
	ch <- task
	ret := <-task.retChan
	s.releaseReadTask(task)

	v := ret.value
	err = ret.err

	if v == nil {
		return nil, ErrNotFound
	}
	value = ret.value.(ValueGetter)
	return value, err
}

func (s *rocksdb) GetRaw(ctx context.Context, col CF, key []byte, opts ...ReadOptFunc) (value []byte, err error) {
	ro := &readOpts{}
	ro.applyOptions(opts)
	if ro.withNoMerge {
		return s.getRaw(ctx, col, key, ro.opt)
	}

	task := s.newReadTask(ctx)
	task.typ = cfGetRaw
	task.ro = ro
	task.cf = []CF{col}
	task.key = [][]byte{key}

	ch := s.acquireReadChan()
	ch <- task
	ret := <-task.retChan
	s.releaseReadTask(task)

	v := ret.value
	err = ret.err

	if v == nil {
		return nil, ErrNotFound
	}
	_value := v.(ValueGetter)
	value = make([]byte, _value.Size())
	copy(value, _value.Value())
	_value.Close()
	return value, err
}

func (s *rocksdb) MultiGet(ctx context.Context, col CF, keys [][]byte, opts ...ReadOptFunc) (values []ValueGetter, err error) {
	ro := &readOpts{}
	ro.applyOptions(opts)
	if ro.withNoMerge {
		return s.multiGet(ctx, col, keys, ro.opt)
	}

	task := s.newReadTask(ctx)
	task.typ = multiGet
	task.ro = ro
	task.cf = []CF{col}
	task.key = keys

	ch := s.acquireReadChan()
	ch <- task
	ret := <-task.retChan
	s.releaseReadTask(task)

	v := ret.value
	err = ret.err

	values = v.([]ValueGetter)
	return values, err
}

func (s *rocksdb) SetRaw(ctx context.Context, col CF, key []byte, value []byte, opts ...WriteOptFunc) error {
	wo := &writeOpts{}
	wo.applyOptions(opts)
	if wo.withNoMerge {
		return s.set(ctx, col, key, value, wo.opt)
	}

	task := s.newWriteTask(ctx)
	task.typ = cfPutEvent
	task.wo = wo
	task.data = putData{
		cf:    col,
		key:   key,
		value: value,
	}

	ch := s.acquireWriteChan()
	ch <- task
	err := <-task.err
	s.releaseWriteTask(task)
	return err
}

func (s *rocksdb) Delete(ctx context.Context, col CF, key []byte, opts ...WriteOptFunc) error {
	wo := &writeOpts{}
	wo.applyOptions(opts)
	if wo.withNoMerge {
		return s.delete(ctx, col, key, wo.opt)
	}

	task := s.newWriteTask(ctx)
	task.typ = cfDeleteEvent
	task.ctx = ctx
	task.wo = wo
	task.data = putData{
		cf:  col,
		key: key,
	}

	ch := s.acquireWriteChan()
	ch <- task
	err := <-task.err
	s.releaseWriteTask(task)
	return err
}

func (s *rocksdb) DeleteRange(ctx context.Context, col CF, start, end []byte, opts ...WriteOptFunc) error {
	wo := &writeOpts{}
	wo.applyOptions(opts)
	if wo.withNoMerge {
		return s.deleteRange(ctx, col, start, end, wo.opt)
	}

	task := s.newWriteTask(ctx)
	task.typ = cfRangeDeleteEvent
	task.wo = wo
	task.data = putData{
		cf:    col,
		start: start,
		end:   end,
	}

	ch := s.acquireWriteChan()
	ch <- task
	err := <-task.err
	s.releaseWriteTask(task)
	return err
}

func (s *rocksdb) List(ctx context.Context, col CF, prefix []byte, marker []byte, readOpt ReadOption) ListReader {
	cf := s.getColumnFamily(col)

	ro := s.ro
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	t := s.db.NewIteratorCF(ro, cf)
	if len(marker) > 0 {
		t.Seek(marker)
	} else if prefix != nil {
		t.Seek(prefix)
	} else {
		t.SeekToFirst()
	}

	lr := &listReader{
		iterator: t,
		marker:   marker,
		prefix:   prefix,
		isFirst:  true,
	}
	return lr
}

func (s *rocksdb) Write(ctx context.Context, batch WriteBatch, opts ...WriteOptFunc) error {
	wo := &writeOpts{}
	wo.applyOptions(opts)
	if wo.withNoMerge {
		return s.write(ctx, batch, wo.opt)
	}

	task := s.newWriteTask(ctx)
	task.typ = batchEvent
	task.wo = wo
	task.batch = batch.(*writeBatch)

	ch := s.acquireWriteChan()
	ch <- task
	err := <-task.err
	s.releaseWriteTask(task)
	return err
}

func (s *rocksdb) Read(ctx context.Context, cols []CF, keys [][]byte, opts ...ReadOptFunc) (values []ValueGetter, err error) {
	ro := &readOpts{}
	ro.applyOptions(opts)
	if ro.withNoMerge {
		return s.read(ctx, cols, keys, ro.opt)
	}

	task := s.newReadTask(ctx)
	task.typ = read
	task.ro = ro
	task.cf = cols
	task.key = keys

	ch := s.acquireReadChan()
	ch <- task

	ret := <-task.retChan
	s.releaseReadTask(task)

	v := ret.value
	err = ret.err
	values = v.([]ValueGetter)
	return values, err
}

func (s *rocksdb) FlushCF(ctx context.Context, col CF) error {
	cf := s.getColumnFamily(col)
	if err := s.db.FlushCF(s.fo, cf); err != nil {
		s.handleError(ctx, err)
		return err
	}
	return nil
}

func (s *rocksdb) Stats(ctx context.Context) (stats Stats, err error) {
	var (
		size                     int64
		totalIndexAndFilterUsage uint64
		totalMemtableUsage       uint64
	)
	files := s.db.GetLiveFilesMetaData()
	for i := range files {
		size += files[i].Size
	}

	for _, cf := range s.cfHandles {
		indexAndFilterUsage, _ := strconv.ParseUint(s.db.GetPropertyCF("rocksdb.estimate-table-readers-mem", cf), 10, 64)
		memtableUsage, _ := strconv.ParseUint(s.db.GetPropertyCF("rocksdb.cur-size-all-mem-tables", cf), 10, 64)
		totalIndexAndFilterUsage += indexAndFilterUsage
		totalMemtableUsage += memtableUsage
	}
	blockCacheUsage, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.block-cache-usage"), 10, 64)
	blockPinnedUsage, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.block-cache-pinned-usage"), 10, 64)
	level0Num, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.num-files-at-level0"), 10, 64)
	delay, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.actual-delayed-write-rate"), 10, 64)
	writeStop, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.is-write-stopped"), 10, 64)
	runningCompaction, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.num-running-compactions"), 10, 64)
	runningFlush, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.num-running-flushes"), 10, 64)
	backgroundErr, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.background-errors"), 10, 64)
	pendingCompaction, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.compaction-pending"), 10, 64)
	pendingFlush, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.mem-table-flush-pending"), 10, 64)
	stats = Stats{
		Used:              uint64(size),
		Level0FileNum:     level0Num,
		WriteSlowdown:     delay != 0,
		WriteStop:         writeStop != 0,
		RunningCompaction: runningCompaction,
		RunningFlush:      runningFlush,
		BackgroundErrors:  backgroundErr,
		PendingCompaction: pendingCompaction != 0,
		PendingFlush:      pendingFlush != 0,
		MemoryUsage: MemoryUsage{
			BlockCacheUsage:     blockCacheUsage,
			IndexAndFilterUsage: totalIndexAndFilterUsage,
			MemtableUsage:       totalMemtableUsage,
			BlockPinnedUsage:    blockPinnedUsage,
			Total:               blockCacheUsage + totalIndexAndFilterUsage + totalMemtableUsage + blockPinnedUsage,
		},
	}
	return
}

func (s *rocksdb) Close() {
	for i := range s.wchans {
		close(s.wchans[i])
	}
	for i := range s.rchans {
		close(s.rchans[i])
	}
	s.wg.Wait()
	s.wo.Destroy()
	s.ro.Destroy()
	s.opt.Destroy()
	s.fo.Destroy()
	for i := range s.cfHandles {
		s.cfHandles[i].Destroy()
	}
	s.db.Close()
}

type (
	putData struct {
		cf    CF
		key   []byte
		value []byte
		start []byte
		end   []byte
	}
	writeTask struct {
		ctx   context.Context
		typ   opType
		wo    *writeOpts
		data  putData
		batch *writeBatch
		err   chan error
	}
	readTask struct {
		ctx     context.Context
		typ     opType
		ro      *readOpts
		cf      []CF
		key     [][]byte
		prefix  []byte
		marker  []byte
		retChan chan readRet
	}
	readRet struct {
		value interface{}
		err   error
	}
)

func (s *rocksdb) acquireReadChan() chan *readTask {
	idx := atomic.AddInt64(&s.readReqCnt, 1) % int64(len(s.rchans))
	return s.rchans[idx]
}

func (s *rocksdb) acquireWriteChan() chan *writeTask {
	idx := atomic.AddInt64(&s.writeReqCnt, 1) % int64(len(s.wchans))
	return s.wchans[idx]
}

func (s *rocksdb) reduceWriteReqCnt(n int) {
	atomic.AddInt64(&s.writeReqCnt, int64(-n))
}

func (s *rocksdb) reduceReadReqCnt(n int) {
	atomic.AddInt64(&s.readReqCnt, int64(-n))
}

func (s *rocksdb) newWriteTask(ctx context.Context) *writeTask {
	t := s.wTaskPool.Get().(*writeTask)
	t.ctx = ctx
	return t
}

func (s *rocksdb) releaseWriteTask(t *writeTask) {
	t.wo = nil
	t.data = putData{}
	t.batch = nil
	s.wTaskPool.Put(t)
}

func (s *rocksdb) newReadTask(ctx context.Context) *readTask {
	t := s.rTaskPool.Get().(*readTask)
	t.ctx = ctx
	return t
}

func (s *rocksdb) releaseReadTask(t *readTask) {
	t.ro = nil
	t.cf = nil
	t.key = nil
	t.prefix = nil
	t.marker = nil
	s.rTaskPool.Put(t)
}

func (s *rocksdb) get(ctx context.Context, col CF, key []byte, readOpt ReadOption) (value ValueGetter, err error) {
	var v *rdb.Slice
	cf := s.getColumnFamily(col)
	ro := s.ro
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	if v, err = s.db.GetCF(ro, cf, key); err != nil {
		s.handleError(ctx, err)
		return nil, err
	}
	if !v.Exists() {
		return nil, ErrNotFound
	}
	value = &valueGetter{value: v}
	return value, err
}

func (s *rocksdb) getRaw(ctx context.Context, col CF, key []byte, readOpt ReadOption) (value []byte, err error) {
	var v *rdb.Slice
	cf := s.getColumnFamily(col)
	ro := s.ro
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	if v, err = s.db.GetCF(ro, cf, key); err != nil {
		s.handleError(ctx, err)
		return nil, err
	}
	if !v.Exists() {
		return nil, ErrNotFound
	}
	value = make([]byte, v.Size())
	copy(value, v.Data())
	v.Free()
	return value, nil
}

func (s *rocksdb) read(ctx context.Context, cols []CF, keys [][]byte, readOpt ReadOption) (values []ValueGetter, err error) {
	ro := s.ro
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	cfhs := make([]*rdb.ColumnFamilyHandle, len(cols))
	for i, col := range cols {
		cfhs[i] = s.getColumnFamily(col)
	}
	_values, err := s.db.MultiGetCFMultiCF(ro, cfhs, keys)
	if err != nil {
		s.handleError(ctx, err)
		return nil, err
	}
	values = make([]ValueGetter, len(_values))
	for i := range _values {
		if !_values[i].Exists() {
			values[i] = nil
			continue
		}
		values[i] = &valueGetter{value: _values[i]}
	}
	return
}

func (s *rocksdb) multiGet(ctx context.Context, col CF, keys [][]byte, readOpt ReadOption) (values []ValueGetter, err error) {
	ro := s.ro
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	cfh := s.getColumnFamily(col)
	_values, err := s.db.MultiGetCF(ro, cfh, keys...)
	if err != nil {
		s.handleError(ctx, err)
		return nil, err
	}
	values = make([]ValueGetter, len(_values))
	for i := range _values {
		if !_values[i].Exists() {
			values[i] = nil
			continue
		}
		values[i] = &valueGetter{value: _values[i]}
	}
	return
}

func (s *rocksdb) set(ctx context.Context, col CF, key []byte, value []byte, writeOpt WriteOption) error {
	wo := s.wo
	cf := s.getColumnFamily(col)
	if writeOpt != nil {
		wo = writeOpt.(*writeOption).opt
	}
	if err := s.db.PutCF(wo, cf, key, value); err != nil {
		s.handleError(ctx, err)
		return err
	}
	return nil
}

func (s *rocksdb) delete(ctx context.Context, col CF, key []byte, writeOpt WriteOption) error {
	wo := s.wo
	cf := s.getColumnFamily(col)
	if writeOpt != nil {
		wo = writeOpt.(*writeOption).opt
	}
	if err := s.db.DeleteCF(wo, cf, key); err != nil {
		s.handleError(ctx, err)
		return err
	}
	return nil
}

func (s *rocksdb) deleteRange(ctx context.Context, col CF, start, end []byte, writeOpt WriteOption) error {
	wo := s.wo
	cf := s.getColumnFamily(col)
	if writeOpt != nil {
		wo = writeOpt.(*writeOption).opt
	}
	b := rdb.NewWriteBatch()
	b.DeleteRangeCF(cf, start, end)
	if err := s.db.Write(wo, b); err != nil {
		s.handleError(ctx, err)
		return err
	}
	return nil
}

func (s *rocksdb) write(ctx context.Context, batch WriteBatch, writeOpt WriteOption) error {
	wo := s.wo
	if writeOpt != nil {
		wo = writeOpt.(*writeOption).opt
	}
	_batch := batch.(*writeBatch)
	if err := s.db.Write(wo, _batch.batch); err != nil {
		s.handleError(ctx, err)
		return err
	}
	return nil
}

func (s *rocksdb) getColumnFamily(col CF) *rdb.ColumnFamilyHandle {
	if col == "" {
		col = defaultCF
	}
	s.lock.RLock()
	cf, ok := s.cfHandles[col]
	if !ok {
		s.lock.RUnlock()
		panic(fmt.Sprintf("col:%s not exist", col.String()))
	}
	s.lock.RUnlock()
	return cf
}

func newRocksdbLruCache(ctx context.Context, size uint64) LruCache {
	return &lruCache{
		cache: rdb.NewLRUCache(size),
	}
}

func newRocksdbWriteBufferManager(ctx context.Context, bufferSize uint64) WriteBufferManager {
	return &writeBufferManager{
		manager: rdb.NewWriteBufferManager(bufferSize),
	}
}

func newRocksdbEnv(ctx context.Context) Env {
	return &env{rdb.NewDefaultEnv()}
}

func newRocksdbSstFileManager(ctx context.Context, e Env) SstFileManager {
	return &sstFileManager{rdb.NewSstFileManager(e.(*env).Env)}
}
