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

package raft

import (
	"context"
	"io"
	"strings"
	"sync"

	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type testKV struct {
	key   string
	value string
}

func (k *testKV) Marshal() []byte {
	return []byte(k.key + "/" + k.value)
}

func (k *testKV) Unmarshal(raw []byte) {
	str := string(raw)
	strArr := strings.Split(str, "/")
	k.key = strArr[0]
	k.value = strArr[1]
}

func newTestStateMachine(storage *testStorage) *testStateMachine {
	return &testStateMachine{
		storage:      storage,
		waitLeaderCh: make(chan struct{}),
	}
}

type testStateMachine struct {
	appliedIndex uint64
	leader       uint64
	storage      *testStorage
	waitLeaderCh chan struct{}
	once         sync.Once

	sync.Mutex
}

func (t *testStateMachine) Apply(cxt context.Context, pd []ProposalData, index uint64) (rets []interface{}, err error) {
	t.Lock()
	defer t.Unlock()

	batch := t.storage.NewBatch()
	for i := range pd {
		kv := &testKV{}
		kv.Unmarshal(pd[i].Data)
		batch.Put([]byte(kv.key), []byte(kv.value))
		rets = append(rets, kv.key)
	}
	if err := t.storage.Write(batch); err != nil {
		return nil, err
	}

	t.appliedIndex = index

	return rets, nil
}

func (t *testStateMachine) LeaderChange(peerID uint64) error {
	log.Infof("receive leader change notify: %d", peerID)
	if peerID == 0 {
		return nil
	}

	t.leader = peerID
	t.once.Do(func() {
		close(t.waitLeaderCh)
	})

	return nil
}

func (t *testStateMachine) WaitLeaderChange() {
	<-t.waitLeaderCh
}

func (t *testStateMachine) ApplyMemberChange(m *Member, index uint64) error {
	log.Infof("receice member change: %+v", m)
	return nil
}

func (t *testStateMachine) Snapshot() Snapshot {
	t.Lock()
	defer t.Unlock()

	snap := &testSnapshot{
		index:        t.appliedIndex,
		stg:          t.storage,
		iter:         t.storage.Iter(nil),
		maxBatchSize: 10,
	}

	return snap
}

func (t *testStateMachine) ApplySnapshot(s Snapshot) error {
	t.Lock()
	defer t.Unlock()
	defer s.Close()

	// drop all data first
	/*if err := t.storage.Drop(); err != nil {
		return err
	}*/

	for {
		batch, err := s.ReadBatch()
		if err != nil && err != io.EOF {
			return err
		}

		if batch != nil {
			if err = t.storage.Write(batch); err != nil {
				batch.Close()
				return err
			}
			batch.Close()
		}
		if err == io.EOF {
			break
		}
	}

	t.appliedIndex = s.Index()
	return nil
}

type testSnapshot struct {
	index        uint64
	stg          Storage
	iter         Iterator
	maxBatchSize int
}

func (t *testSnapshot) ReadBatch() (Batch, error) {
	batch := t.stg.NewBatch()

	for i := 0; i < t.maxBatchSize; i++ {
		keyGetter, valGetter, err := t.iter.ReadNext()
		if err != nil {
			return nil, err
		}
		if valGetter == nil {
			return batch, io.EOF
		}

		batch.Put(keyGetter.Key(), valGetter.Value())
	}

	return batch, nil
}

func (t *testSnapshot) Index() uint64 {
	return t.index
}

func (t *testSnapshot) Close() error {
	t.iter.Close()
	return nil
}

type testStorage struct {
	cf      kvstore.CF
	kvStore kvstore.Store
}

func (t *testStorage) Get(key []byte) (ValGetter, error) {
	return t.kvStore.Get(context.TODO(), t.cf, key, nil)
}

func (t *testStorage) Iter(prefix []byte) Iterator {
	return &testIterator{lr: t.kvStore.List(context.TODO(), t.cf, prefix, nil, nil)}
}

func (t *testStorage) NewBatch() Batch { return &testBatch{cf: t.cf, batch: t.kvStore.NewWriteBatch()} }

func (t *testStorage) Write(b Batch) error {
	return t.kvStore.Write(context.TODO(), b.(*testBatch).batch, nil)
}

func (t *testStorage) Put(key, value []byte) error {
	return t.kvStore.SetRaw(context.TODO(), t.cf, key, value, nil)
}

type testIterator struct {
	lr kvstore.ListReader
}

func (i *testIterator) SeekTo(key []byte) { i.lr.Seek(key) }

func (i *testIterator) SeekForPrev(prev []byte) error { return i.lr.SeekForPrev(prev) }

func (i *testIterator) ReadNext() (key KeyGetter, val ValGetter, err error) { return i.lr.ReadNext() }

func (i *testIterator) ReadPrev() (key KeyGetter, val ValGetter, err error) { return i.lr.ReadPrev() }

func (i *testIterator) Close() { i.lr.Close() }

type testBatch struct {
	cf    kvstore.CF
	batch kvstore.WriteBatch
}

func (t *testBatch) Put(key, value []byte) { t.batch.Put(t.cf, key, value) }

func (t *testBatch) Delete(key []byte) { t.batch.Delete(t.cf, key) }

func (t *testBatch) DeleteRange(start []byte, end []byte) {
	t.batch.DeleteRange(t.cf, start, end)
}

func (t *testBatch) Data() []byte { return t.batch.Data() }

func (t *testBatch) From(data []byte) { t.batch.From(data) }

func (t *testBatch) Close() { t.batch.Close() }
