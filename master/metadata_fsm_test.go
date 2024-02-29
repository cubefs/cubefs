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

package master

import (
	"encoding/binary"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/stretchr/testify/require"
)

func newFsmForMetadataFsmTest(t *testing.T) (mf *MetadataFsm) {
	storeDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	db, err := raftstore_db.NewRocksDBStoreAndRecovery(storeDir, LRUCacheSize, WriteBufferSize)
	require.NoError(t, err)
	mf = &MetadataFsm{
		store: db,
	}
	mf.snapshotHandler = func() {
		t.Logf("snapshot success")
	}
	return
}

func prepareDbForFSMTest(t *testing.T, db *raftstore_db.RocksDBStore) {
	const dataCount = 10000
	randGen := rand.New(rand.NewSource(time.Now().UnixMilli()))
	for i := 0; i < dataCount; i++ {
		key := strconv.FormatInt(randGen.Int63(), 10)
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, randGen.Uint64())
		_, err := db.Put(key, value, false)
		require.NoError(t, err)
	}
}

func TestFsmApplySnapshot(t *testing.T) {
	fsm := newFsmForMetadataFsmTest(t)
	prepareDbForFSMTest(t, fsm.store)
	leaderFsm := newFsmForMetadataFsmTest(t)
	key := "Hello"
	value := "World"
	_, err := leaderFsm.store.Put(key, []byte(value), true)
	require.NoError(t, err)
	snap, err := leaderFsm.Snapshot()
	require.NoError(t, err)
	fsm.ApplySnapshot(nil, snap)
	cnt := 0
	snapshot := fsm.store.RocksDBSnapshot()
	defer fsm.store.ReleaseSnapshot(snapshot)
	iter := fsm.store.Iterator(snapshot)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		require.EqualValues(t, iter.Key().Data(), []byte(key))
		require.EqualValues(t, iter.Value().Data(), []byte(value))
		t.Logf("seek %v:%v", iter.Key(), iter.Value())
		cnt++
	}
	require.EqualValues(t, 1, cnt)
}
