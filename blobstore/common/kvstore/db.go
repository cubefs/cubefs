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

package kvstore

import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	rdb "github.com/tecbot/gorocksdb"
)

var (
	ErrInvalidCFName = errors.New("invalid column family name")
	ErrNotFound      = os.ErrNotExist
)

type KVStore interface {
	KVStorage
	GetDB() *rdb.DB
	Table(name string) KVTable
	Close() error
}

type Iterator interface {
	SeekToFirst()
	SeekToLast()
	Seek([]byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
	Key() *rdb.Slice
	Next()
	Value() *rdb.Slice
	// destroy iterator and read option
	Close()
	Err() error
}

type WriteBatch struct {
	b []batch
}

type Snapshot rdb.Snapshot

type ReadOptions struct {
	*rdb.ReadOptions
}

type WriteOptions struct {
	*rdb.WriteOptions
}

type Range rdb.Range

type KV struct {
	Key   []byte
	Value []byte
}

type opType int

const (
	putEvent opType = iota + 1
	deleteEvent
	rangeDeleteEvent
	cfPutEvent
	cfDeleteEvent
	cfRangeDeleteEvent
	batchEvent
	flushEvent
)

type batch struct {
	typ  opType
	data interface{}
}

type put struct {
	key   []byte
	value []byte
}

type cfPut struct {
	cf    *rdb.ColumnFamilyHandle
	key   []byte
	value []byte
}

type del struct {
	key []byte
}

type cfDel struct {
	cf  *rdb.ColumnFamilyHandle
	key []byte
}

type rangeDelete struct {
	start []byte
	end   []byte
}

type cfRangeDelete struct {
	cf    *rdb.ColumnFamilyHandle
	start []byte
	end   []byte
}

type writeTask struct {
	typ  opType
	data interface{}
	err  chan error
}

type instance struct {
	db *rdb.DB
	ro *rdb.ReadOptions
	wo *rdb.WriteOptions
	fo *rdb.FlushOptions

	cfTables map[string]*table
	opt      *rdb.Options

	lock  sync.RWMutex
	once  sync.Once
	wchan chan *writeTask
	rpool taskpool.TaskPool
	wg    sync.WaitGroup
}

type iterator struct {
	rpool *taskpool.TaskPool
	ro    *rdb.ReadOptions
	once  sync.Once
	iter  *rdb.Iterator
}

func (i *iterator) SeekToFirst() {
	done := make(chan struct{})
	i.rpool.Run(func() {
		defer close(done)
		i.iter.SeekToFirst()
	})
	<-done
}

func (i *iterator) SeekToLast() {
	done := make(chan struct{})
	i.rpool.Run(func() {
		defer close(done)
		i.iter.SeekToLast()
	})
	<-done
}

func (i *iterator) Seek(key []byte) {
	done := make(chan struct{})
	i.rpool.Run(func() {
		defer close(done)
		i.iter.Seek(key)
	})
	<-done
}

func (i *iterator) Valid() bool {
	return i.iter.Valid()
}

func (i *iterator) ValidForPrefix(prefix []byte) bool {
	return i.iter.ValidForPrefix(prefix)
}

func (i *iterator) Key() *rdb.Slice {
	return i.iter.Key()
}

func (i *iterator) Next() {
	done := make(chan struct{})
	i.rpool.Run(func() {
		defer close(done)
		i.iter.Next()
	})
	<-done
}

func (i *iterator) Value() *rdb.Slice {
	return i.iter.Value()
}

func (i *iterator) Err() error {
	return i.iter.Err()
}

func (i *iterator) Close() {
	i.once.Do(func() {
		i.iter.Close()
		i.ro.Destroy()
	})
}

func OpenDBWithCF(path string, cfnames []string, dbOpts ...DbOptions) (KVStore, error) {
	if path == "" {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}

	err := os.MkdirAll(path, 0o755)
	if err != nil {
		panic(err)
	}

	dbOpt := defaultRocksDBOption
	dbOpt.applyOpts(dbOpts)

	if len(cfnames) == 0 {
		return nil, ErrInvalidCFName
	}
	cfnames = append([]string{"default"}, cfnames...)

	opts := genRocksdbOpts(&dbOpt)

	cfopts := make([]*rdb.Options, len(cfnames))
	for i := 0; i < len(cfnames); i++ {
		cfopts[i] = opts
	}

	db, cfs, err := rdb.OpenDbColumnFamilies(opts, path, cfnames, cfopts)
	if err != nil {
		opts.Destroy()
		if strings.HasSuffix(err.Error(), "does not exist (create_if_missing is false)") {
			err = ErrNotFound
		}
		return nil, err
	}

	ro := rdb.NewDefaultReadOptions()
	ro.SetVerifyChecksums(true)
	wo := rdb.NewDefaultWriteOptions()
	wo.SetSync(dbOpt.sync)
	fo := rdb.NewDefaultFlushOptions()

	cfTables := make(map[string]*table)
	ins := &instance{
		db:       db,
		ro:       ro,
		wo:       wo,
		fo:       fo,
		cfTables: cfTables,
		opt:      opts,
		lock:     sync.RWMutex{},
		once:     sync.Once{},
		rpool:    taskpool.New(dbOpt.readCocurrency, dbOpt.queueLen),
		wchan:    make(chan *writeTask, dbOpt.queueLen),
	}

	for i := range cfs {
		cfTables[cfnames[i]] = &table{
			name: cfnames[i],
			cf:   cfs[i],
			ins:  ins,
			db:   ins.db,
			ro:   ro,
			wo:   wo,
			fo:   fo,
		}
	}

	ins.wg.Add(1)
	go ins.writeLoop()

	return ins, nil
}

func OpenDB(path string, dbOpts ...DbOptions) (KVStore, error) {
	if path == "" {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		panic(err)
	}
	dbOpt := defaultRocksDBOption
	dbOpt.applyOpts(dbOpts)

	opts := genRocksdbOpts(&dbOpt)
	db, err := rdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		if strings.HasSuffix(err.Error(), "does not exist (create_if_missing is false)") {
			err = ErrNotFound
		}
		return nil, err
	}

	ro := rdb.NewDefaultReadOptions()
	ro.SetVerifyChecksums(true)
	wo := rdb.NewDefaultWriteOptions()
	wo.SetSync(dbOpt.sync)
	fo := rdb.NewDefaultFlushOptions()

	ins := &instance{
		db:       db,
		ro:       ro,
		wo:       wo,
		fo:       fo,
		cfTables: make(map[string]*table),
		opt:      opts,
		lock:     sync.RWMutex{},
		once:     sync.Once{},
		rpool:    taskpool.New(dbOpt.readCocurrency, dbOpt.queueLen),
		wchan:    make(chan *writeTask, dbOpt.queueLen),
	}
	ins.wg.Add(1)
	go ins.writeLoop()
	return ins, nil
}

func (s *instance) writeLoop() {
	defer s.wg.Done()
	for task := range s.wchan {
		tasks := []*writeTask{task}
		n := len(s.wchan)
		for i := 0; i < n; i++ {
			task = <-s.wchan
			tasks = append(tasks, task)
		}
		wb := rdb.NewWriteBatch()
		ii := 0
		for i, task := range tasks {
			switch task.typ {
			case putEvent:
				putData := task.data.(put)
				wb.Put(putData.key, putData.value)
			case deleteEvent:
				delData := task.data.(del)
				wb.Delete(delData.key)
			case rangeDeleteEvent:
				rangeDeleteData := task.data.(rangeDelete)
				wb.DeleteRange(rangeDeleteData.start, rangeDeleteData.end)
			case cfPutEvent:
				putData := task.data.(cfPut)
				wb.PutCF(putData.cf, putData.key, putData.value)
			case cfDeleteEvent:
				delData := task.data.(cfDel)
				wb.DeleteCF(delData.cf, delData.key)
			case cfRangeDeleteEvent:
				rangeDeleteData := task.data.(cfRangeDelete)
				wb.DeleteRangeCF(rangeDeleteData.cf, rangeDeleteData.start, rangeDeleteData.end)
			case batchEvent:
				b := task.data.([]batch)
				s.handleBatchEvent(b, wb)
			case flushEvent:
				if wb.Count() > 0 {
					err := s.db.Write(s.wo, wb)
					for ; ii < i; ii++ {
						tasks[ii].err <- err
					}
					wb.Clear()
				}
				tasks[i].err <- s.db.Flush(s.fo)
				ii = i + 1
			default:
				// never to here
				panic("invalid type")
			}
		}
		if wb.Count() > 0 {
			err := s.db.Write(s.wo, wb)
			for ; ii < len(tasks); ii++ {
				tasks[ii].err <- err
			}
		}
		wb.Destroy()
	}
}

func (s *instance) handleBatchEvent(b []batch, wb *rdb.WriteBatch) {
	for _, item := range b {
		switch item.typ {
		case putEvent:
			putData := item.data.(put)
			wb.Put(putData.key, putData.value)
		case deleteEvent:
			delData := item.data.(del)
			wb.Delete(delData.key)
		case rangeDeleteEvent:
			rangeDeleteData := item.data.(rangeDelete)
			wb.DeleteRange(rangeDeleteData.start, rangeDeleteData.end)
		case cfPutEvent:
			putData := item.data.(cfPut)
			wb.PutCF(putData.cf, putData.key, putData.value)
		case cfDeleteEvent:
			delData := item.data.(cfDel)
			wb.DeleteCF(delData.cf, delData.key)
		case cfRangeDeleteEvent:
			rangeDeleteData := item.data.(cfRangeDelete)
			wb.DeleteRangeCF(rangeDeleteData.cf, rangeDeleteData.start, rangeDeleteData.end)
		default:
			// never to here
			panic("invalid type")
		}
	}
}

func (s *instance) Table(name string) KVTable {
	s.lock.RLock()
	if t, ok := s.cfTables[name]; ok {
		s.lock.RUnlock()
		return t
	}
	s.lock.RUnlock()
	return nil
}

func (s *instance) Name() string {
	return s.db.Name()
}

func (s *instance) GetDB() *rdb.DB {
	return s.db
}

func (s *instance) Get(key []byte) (data []byte, err error) {
	done := make(chan struct{})
	s.rpool.Run(func() {
		defer close(done)
		data, err = s.db.GetBytes(s.ro, key)
		if err == nil && data == nil {
			err = ErrNotFound
		}
	})
	<-done
	return
}

func (s *instance) Put(kv KV) error {
	task := &writeTask{
		typ:  putEvent,
		data: put{kv.Key, kv.Value},
		err:  make(chan error, 1),
	}
	s.wchan <- task
	return <-task.err
}

func (s *instance) Delete(key []byte) (err error) {
	task := &writeTask{
		typ:  deleteEvent,
		data: del{key},
		err:  make(chan error, 1),
	}
	s.wchan <- task
	return <-task.err
}

func (s *instance) DeleteRange(start, end []byte) error {
	task := &writeTask{
		typ:  rangeDeleteEvent,
		data: rangeDelete{start, end},
		err:  make(chan error, 1),
	}
	s.wchan <- task
	return <-task.err
}

func (s *instance) DeleteBatch(keys [][]byte, safe bool) (err error) {
	b := []batch{}
	for _, key := range keys {
		if safe {
			_, err := s.Get(key)
			if err != nil {
				return err
			}
		}
		b = append(b, batch{
			typ:  deleteEvent,
			data: del{key},
		})
	}
	if len(b) == 0 {
		return
	}
	task := &writeTask{
		typ:  batchEvent,
		data: b,
		err:  make(chan error, 1),
	}
	s.wchan <- task
	return <-task.err
}

func (s *instance) WriteBatch(kvs []KV, safe bool) (err error) {
	b := []batch{}
	for _, kv := range kvs {
		key := kv.Key
		if safe {
			v, err := s.db.GetBytes(s.ro, key)
			if err != nil {
				return err
			}
			if v != nil &&
				!bytes.Equal(kv.Value, v) {
				msg := fmt.Sprintf("table(%v): conflict data at row %v\n old buf: %v, new buf: %v",
					s.db.Name(), kv.Key, v, kv.Value)
				return errors.New(msg)
			}
		}
		b = append(b, batch{
			typ:  putEvent,
			data: put{kv.Key, kv.Value},
		})
	}
	if len(b) == 0 {
		return
	}
	task := &writeTask{
		typ:  batchEvent,
		data: b,
		err:  make(chan error, 1),
	}
	s.wchan <- task
	return <-task.err
}

func (s *instance) NewSnapshot() *Snapshot {
	return (*Snapshot)(s.db.NewSnapshot())
}

func (s *instance) ReleaseSnapshot(snapshot *Snapshot) {
	s.db.ReleaseSnapshot((*rdb.Snapshot)(snapshot))
}

func (s *instance) NewIterator(snapshot *Snapshot, opts ...OpOption) Iterator {
	var op Op

	op.applyOpts(opts)
	ro := op.Ro

	if ro == nil {
		ro = NewReadOptions()
	}
	if snapshot != nil {
		ro.SetSnapshot((*rdb.Snapshot)(snapshot))
	}

	return &iterator{rpool: &s.rpool, ro: ro.ReadOptions, iter: s.db.NewIterator(ro.ReadOptions), once: sync.Once{}}
}

func (s *instance) Flush() (err error) {
	task := &writeTask{
		typ: flushEvent,
		err: make(chan error, 1),
	}
	s.wchan <- task
	return <-task.err
}

func (s *instance) Close() error {
	s.once.Do(func() {
		s.rpool.Close()
		close(s.wchan)
		s.wg.Wait()
		s.ro.Destroy()
		s.wo.Destroy()
		s.lock.RLock()
		for i := range s.cfTables {
			s.cfTables[i].cf.Destroy()
		}

		s.db.Close()
		s.lock.RUnlock()
	})

	return nil
}

func (s *instance) NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

func (s *instance) DoBatch(wb *WriteBatch) error {
	if len(wb.b) == 0 {
		return nil
	}
	task := &writeTask{
		typ:  batchEvent,
		data: wb.b,
		err:  make(chan error, 1),
	}
	s.wchan <- task
	return <-task.err
}

func (wb *WriteBatch) Count() int {
	return len(wb.b)
}

func (wb *WriteBatch) Destroy() {
	// do nothing
}

func (wb *WriteBatch) Clear() {
	wb.b = wb.b[:0]
}

func (wb *WriteBatch) Put(key, value []byte) {
	wb.b = append(wb.b, batch{putEvent, put{key, value}})
}

func (wb *WriteBatch) PutCF(cf *rdb.ColumnFamilyHandle, key, value []byte) {
	wb.b = append(wb.b, batch{cfPutEvent, cfPut{cf, key, value}})
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.b = append(wb.b, batch{deleteEvent, del{key}})
}

func (wb *WriteBatch) DeleteCF(cf *rdb.ColumnFamilyHandle, key []byte) {
	wb.b = append(wb.b, batch{cfDeleteEvent, cfDel{cf, key}})
}

func (wb *WriteBatch) DeleteRange(start, end []byte) {
	wb.b = append(wb.b, batch{rangeDeleteEvent, rangeDelete{start, end}})
}

func (wb *WriteBatch) DeleteRangeCF(cf *rdb.ColumnFamilyHandle, start, end []byte) {
	wb.b = append(wb.b, batch{cfRangeDeleteEvent, cfRangeDelete{cf, start, end}})
}

func NewReadOptions() *ReadOptions {
	return &ReadOptions{rdb.NewDefaultReadOptions()}
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{rdb.NewDefaultWriteOptions()}
}
