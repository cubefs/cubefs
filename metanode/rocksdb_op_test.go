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

package metanode_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/stretchr/testify/require"
)

const (
	MultiCountPerOp = 1000
	TestCont        = 1000000
	TestTable       = metanode.InodeTable
	TestKeySize     = 9 // 1 + sizeof(uint64)
	TestCFName      = "test-cf"
)

func getRocksdbPathForTest() (path string) {
	path = fmt.Sprintf("/tmp/cfs/rocksdb_test/%v", time.Now().UnixMilli())
	return
}

func TestOpenDb(t *testing.T) {
	path := getRocksdbPathForTest()
	os.RemoveAll(path)
	db := metanode.NewRocksdb()
	defer os.RemoveAll(path)
	err := db.OpenDb(path, 0, 0, 0, 0, 0, 0)
	require.NoError(t, err)

	err = db.CloseDb()
	require.NoError(t, err)
}

func TestReopneDb(t *testing.T) {
	path := getRocksdbPathForTest()
	os.RemoveAll(path)
	db := metanode.NewRocksdb()
	defer os.RemoveAll(path)
	err := db.OpenDb(path, 0, 0, 0, 0, 0, 0)
	require.NoError(t, err)

	err = db.CloseDb()
	require.NoError(t, err)

	err = db.ReOpenDb(path, 0, 0, 0, 0, 0, 0)
	require.NoError(t, err)

	err = db.CloseDb()
	require.NoError(t, err)
}

func updateKey(t *testing.T, keyBuf []byte, id uint64) {
	require.EqualValues(t, TestKeySize, len(keyBuf))
	keyBuf[0] = byte(TestTable)
	binary.BigEndian.PutUint64(keyBuf[1:], id)
}

func extractIdFromKey(t *testing.T, keyBuf []byte) (id uint64) {
	require.EqualValues(t, TestKeySize, len(keyBuf))
	require.EqualValues(t, TestTable, keyBuf[0])
	id = binary.BigEndian.Uint64(keyBuf[1:])
	return
}

func insertByItem(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	i := 0
	key := make([]byte, 9)

	value := make([]byte, 1)

	for ; i < TestCont; i++ {
		updateKey(t, key, uint64(i))
		err := db.Put(cf, key, value)
		require.NoError(t, err)
	}
}

func insertByMultiItem(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	i := 0
	key := make([]byte, TestKeySize)
	value := make([]byte, 1)
	keys := make([]byte, TestKeySize*MultiCountPerOp)
	handle, err := db.CreateBatchHandler()
	require.NoError(t, err)

	for ; i < TestCont; i++ {
		updateKey(t, key, uint64(i))

		if i != 0 && i%MultiCountPerOp == 0 {
			err = db.CommitBatchAndRelease(handle, cf)
			require.NoError(t, err)

			handle, err = db.CreateBatchHandler()
			require.NoError(t, err)
		}
		offset := (i % MultiCountPerOp) * TestKeySize
		copy(keys[offset:offset+TestKeySize], key)
		db.AddItemToBatch(handle, cf, keys[offset:offset+TestKeySize], value)
	}
	err = db.CommitBatchAndRelease(handle, cf)
	require.NoError(t, err)
}

func rangeTest(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	stKey := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(0)

	stKey[0] = byte(TestTable)
	endKey[0] = byte(TestTable + 1)

	db.Range(cf, stKey, endKey, func(k, v []byte) (bool, error) {
		require.EqualValues(t, TestTable, k[0])
		index := extractIdFromKey(t, k)
		require.EqualValues(t, index, i)
		i++
		return true, nil
	})

	require.LessOrEqual(t, i, uint64(TestCont))
}

func snapRangeTest(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	stKey := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(0)
	snap := db.OpenSnap()
	start := time.Now()
	defer db.ReleaseSnap(snap)

	stKey[0] = byte(TestTable)
	endKey[0] = byte(TestTable + 1)

	db.RangeWithSnap(cf, stKey, endKey, snap, func(k, v []byte) (bool, error) {
		require.EqualValues(t, TestTable, k[0])
		index := extractIdFromKey(t, k)
		require.EqualValues(t, index, i)
		i++
		return true, nil
	})

	require.LessOrEqual(t, i, uint64(TestCont))
	t.Logf("snap range %v items cost:%v", i, time.Since(start))
}

func descRangeTest(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	stKey := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(TestCont - 1)

	stKey[0] = byte(TestTable)
	endKey[0] = byte(TestTable + 1)

	db.DescRange(cf, stKey, endKey, func(k, v []byte) (bool, error) {
		require.EqualValues(t, TestTable, k[0])
		index := extractIdFromKey(t, k)
		require.EqualValues(t, index, i)
		i--
		return true, nil
	})

	if i != math.MaxUint64 {
		t.Errorf("range failed, total record:%d, but read:%d", TestCont, TestCont+(math.MaxUint64-i))
	}
}

func snapDescRangeTest(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	stKey := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(TestCont - 1)
	snap := db.OpenSnap()
	defer db.ReleaseSnap(snap)

	stKey[0] = byte(TestTable)
	endKey[0] = byte(TestTable + 1)

	db.DescRangeWithSnap(cf, stKey, endKey, snap, func(k, v []byte) (bool, error) {
		require.EqualValues(t, TestTable, k[0])
		index := extractIdFromKey(t, k)
		require.EqualValues(t, index, i)
		i--
		return true, nil
	})

	if i != math.MaxUint64 {
		t.Errorf("range failed, total record:%d, but read:%d", TestCont, TestCont+(math.MaxUint64-i))
	}
}

func genenerData(t *testing.T, db *metanode.RocksdbOperator) {
	start := time.Now()
	insertByMultiItem(t, db)
	t.Logf("insert by multi items cost:%v", time.Since(start))
}

func deleteDataByItem(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	stKey := make([]byte, 1)
	endKey := make([]byte, 1)

	stKey[0] = byte(TestTable)
	endKey[0] = byte(TestTable + 1)
	start := time.Now()
	var i = 0

	err = db.Range(cf, stKey, endKey, func(k, v []byte) (bool, error) {
		require.EqualValues(t, TestTable, k[0])
		i++
		db.Del(cf, k)
		return true, nil
	})

	t.Logf("********range %v items delete used:%v , err:%v\n", i, time.Since(start), err)
}

func deleteDataByMultiItems(t *testing.T, db *metanode.RocksdbOperator) {
	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	stKey := make([]byte, 1)
	endKey := make([]byte, 1)

	keys := make([]byte, TestKeySize*1000)

	stKey[0] = byte(TestTable)
	endKey[0] = byte(TestTable + 1)
	start := time.Now()
	var i = 0
	handle, err := db.CreateBatchHandler()
	require.NoError(t, err)

	err = db.Range(cf, stKey, endKey, func(k, v []byte) (bool, error) {
		require.EqualValues(t, TestTable, k[0])
		index := i % 1000
		if i%1000 == 0 {
			if i != 0 {
				_ = db.CommitBatchAndRelease(handle, cf)
				handle, _ = db.CreateBatchHandler()
			}
		}
		i++
		copy(keys[index*TestKeySize:(index+1)*TestKeySize], k)
		db.DelItemToBatch(handle, cf, keys[index*TestKeySize:(index+1)*TestKeySize])
		return true, nil
	})
	require.NoError(t, err)

	if handle != nil {
		db.CommitBatchAndRelease(handle, cf)
	}

	t.Logf("********delete %v items by multi used:%v , err:%v\n", i, time.Since(start), err)
}

func TestOps(t *testing.T) {
	path := getRocksdbPathForTest()
	// NOTE: sleep 1 sec to get diff paths
	time.Sleep(1 * time.Second)
	path2 := getRocksdbPathForTest()
	os.RemoveAll(path)
	os.RemoveAll(path2)
	defer os.RemoveAll(path)
	defer os.RemoveAll(path2)
	db := metanode.NewRocksdb()
	_ = db.OpenDb(path, 0, 0, 0, 0, 0, 0)

	start := time.Now()
	insertByItem(t, db)
	t.Logf("insert by item cost:%v", time.Since(start))

	start = time.Now()
	rangeTest(t, db)
	t.Logf("range test by item cost:%v", time.Since(start))

	start = time.Now()
	rangeTest(t, db)
	t.Logf("range test by item cost:%v", time.Since(start))

	runtime.GC()
	time.Sleep(time.Second * 3)
	start = time.Now()
	descRangeTest(t, db)
	t.Logf("desc range test by item cost:%v", time.Since(start))
	deleteDataByMultiItems(t, db)
	deleteDataByItem(t, db)

	genenerData(t, db)
	runtime.GC()
	time.Sleep(time.Second * 3)
	t.Logf("delete multi items begin\n")
	start = time.Now()
	deleteDataByItem(t, db)
	t.Logf("del by item cost:%v", time.Since(start))

	deleteDataByItem(t, db)
	genenerData(t, db)
	runtime.GC()
	time.Sleep(time.Second * 3)
	t.Logf("delete multi items begin\n")
	start = time.Now()
	deleteDataByMultiItems(t, db)
	t.Logf("del by multi item cost:%v", time.Since(start))

	deleteDataByItem(t, db)
	runtime.GC()
	time.Sleep(time.Second * 3)
	db2 := metanode.NewRocksdb()
	_ = db2.OpenDb(path2, 0, 0, 0, 0, 0, 0)
	genenerData(t, db2)
	start = time.Now()

	db2.CloseDb()
	err := db.CloseDb()
	require.NoError(t, err)
	os.Rename(path2, path)
	db.ReOpenDb(path, 0, 0, 0, 0, 0, 0)
	t.Logf("reopen db delete used:%v \n", time.Since(start))
	rangeTest(t, db)
	deleteDataByMultiItems(t, db)
	err = db.CloseDb()
	require.NoError(t, err)
}

func batchAbortTest(t *testing.T, db *metanode.RocksdbOperator) {
	var (
		handle interface{}
		err    error
	)

	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	i := 0
	key := make([]byte, TestKeySize)
	value := make([]byte, 1)
	keys := make([]byte, TestKeySize*MultiCountPerOp)

	handle = nil
	db.AddItemToBatch(handle, cf, key, value)
	db.DelItemToBatch(handle, cf, key)
	db.CommitBatchAndRelease(handle, cf)
	db.ReleaseBatchHandle(handle)

	handle, err = db.CreateBatchHandler()

	if err != nil {
		t.Errorf("insert multi failed:%s", err.Error())
	}

	for i = TestCont; i < TestCont+TestCont; i++ {
		updateKey(t, key, uint64(i))

		if i != 0 && i%MultiCountPerOp == 0 {
			if err = db.ReleaseBatchHandle(handle); err != nil {
				t.Errorf("insert multi failed:commit failed:%s", err.Error())
				return
			}
			if handle, err = db.CreateBatchHandler(); err != nil {
				t.Errorf("insert multi failed:create batch handle failed:%s", err.Error())
				return
			}
		}
		offset := (i % MultiCountPerOp) * TestKeySize
		copy(keys[offset:offset+TestKeySize], key)
		db.AddItemToBatch(handle, cf, keys[offset:offset+TestKeySize], value)
	}

	db.ReleaseBatchHandle(handle)
	t.Logf("********batchAbortTest  finished")
}

func TestAbortOps(t *testing.T) {
	t.Logf("************ abort test begin *************")
	path := getRocksdbPathForTest()
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	db := metanode.NewRocksdb()
	_ = db.OpenDb(path, 0, 0, 0, 0, 0, 0)
	genenerData(t, db)
	rangeTest(t, db)

	batchAbortTest(t, db)
	rangeTest(t, db)

	go snapRangeTest(t, db)
	go deleteDataByMultiItems(t, db)
	time.Sleep(50 * time.Millisecond)
	err := db.CloseDb()
	require.NoError(t, err)
	snap := db.OpenSnap()
	require.Nil(t, snap)
}

func TestRocksDB_accessDB(t *testing.T) {
	path := getRocksdbPathForTest()
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	db := metanode.NewRocksdb()
	_ = db.OpenDb(path, 0, 0, 0, 0, 0, 0)
	genenerData(t, db)
	dbSnap := db.OpenSnap()
	require.NotNil(t, dbSnap)
	go func() {
		time.Sleep(time.Millisecond * 1000)
		db.CloseDb()
	}()

	cf, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	startKey, endKey := []byte{byte(TestTable)}, []byte{byte(TestTable + 1)}
	err = db.RangeWithSnap(cf, startKey, endKey, dbSnap, func(k, v []byte) (bool, error) {
		return true, nil
	})
	require.NoError(t, err)
}

func getSSTCount(t *testing.T, path string) (count int) {
	dentries, err := os.ReadDir(path)
	require.NoError(t, err)

	for _, dentry := range dentries {
		if strings.HasSuffix(dentry.Name(), ".sst") {
			count++
		}
	}
	return
}

func TestFlushSingleCF(t *testing.T) {
	path := getRocksdbPathForTest()
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	db := metanode.NewRocksdb()

	err := db.OpenDb(path, 0, 0, 0, 0, 0, 0)
	require.NoError(t, err)

	cfHandle, err := db.OpenColumnFamily(TestCFName)
	require.NoError(t, err)

	// genenerData(t, db)
	err = db.Put(cfHandle, []byte("Hello"), []byte("World"))
	require.NoError(t, err)

	count := getSSTCount(t, path)
	require.EqualValues(t, 0, count)

	err = db.CompactRange(cfHandle, []byte{0}, []byte{255})
	require.NoError(t, err)

	count = getSSTCount(t, path)
	require.NotEqualValues(t, 0, count)

	err = db.CloseDb()
	require.NoError(t, err)
}
