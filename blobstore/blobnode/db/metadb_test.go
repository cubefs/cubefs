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
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func TestNewKVDB(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbNew")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(t, err)

	md, err := NewMetaHandler(diskdir, MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, md)
}

func TestKVDB_DirectOP(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbDirectOp")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(t, err)

	md, err := newMetaDB(diskdir, MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, md)

	expectedKey := []byte("testkey")
	expectedValue := []byte{0x1, 0x2, 0x3}

	kv := rdb.KV{
		Key:   expectedKey,
		Value: expectedValue,
	}

	ctx := context.Background()

	err = md.DirectPut(ctx, kv)
	require.NoError(t, err)

	value, err := md.Get(ctx, expectedKey)
	require.NoError(t, err)
	require.Equal(t, expectedValue, value)

	err = md.DirectDelete(ctx, expectedKey)
	require.NoError(t, err)

	_, err = md.Get(ctx, expectedKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file does not exist")

	err = md.Close(ctx)
	require.NoError(t, err)
}

func TestKVDB_BatchWrite(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbBatchWrite")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(t, err)

	md, err := newMetaDB(diskdir, MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, md)

	expectedKey := []byte("testkey")
	expectedValue := []byte{0x1, 0x2, 0x3}

	kv := rdb.KV{
		Key:   expectedKey,
		Value: expectedValue,
	}

	// 1
	err = md.Put(ctx, kv)
	require.NoError(t, err)

	value, err := md.Get(ctx, expectedKey)
	require.NoError(t, err)
	require.Equal(t, expectedValue, value)

	err = md.Delete(ctx, expectedKey)
	require.NoError(t, err)

	_, err = md.Get(ctx, expectedKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file does not exist")

	// 2
	err = md.Put(ctx, kv)
	require.NoError(t, err)

	value, err = md.Get(ctx, expectedKey)
	require.NoError(t, err)
	require.Equal(t, expectedValue, value)

	err = md.Delete(ctx, expectedKey)
	require.NoError(t, err)

	_, err = md.Get(ctx, expectedKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file does not exist")

	// 3
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(expectedKey), i)
			value := []byte(key)
			kv := rdb.KV{
				Key:   []byte(key),
				Value: value,
			}
			err := md.Put(ctx, kv)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	err = md.Flush(ctx)
	require.NoError(t, err)

	span.Infof("batch put success")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s-%d", string(expectedKey), i)
		expValue := []byte(key)
		value, err = md.Get(ctx, []byte(key))
		require.NoError(t, err)
		require.Equal(t, expValue, value)
	}

	// end
	err = md.Close(ctx)
	require.NoError(t, err)
}

func TestKVDB_BatchDelete(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbBatchDelete")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(t, err)

	md, err := newMetaDB(diskdir, MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, md)

	keyPrefix := []byte("testkey")

	wg := sync.WaitGroup{}
	// 1
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(keyPrefix), i)
			value := []byte(key)
			kv := rdb.KV{
				Key:   []byte(key),
				Value: value,
			}
			err := md.Put(ctx, kv)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	err = md.Flush(ctx)
	require.NoError(t, err)

	// 2
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				return
			}
			key := fmt.Sprintf("%s-%d", string(keyPrefix), i)
			err := md.Delete(ctx, []byte(key))
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	span.Infof("batch delete success")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s-%d", string(keyPrefix), i)
		expValue := []byte(key)
		value, err := md.Get(ctx, []byte(key))
		if i%2 == 0 {
			require.NoError(t, err)
			require.Equal(t, expValue, value)
		} else {
			require.Error(t, err)
			require.Contains(t, err.Error(), "file does not exist")
		}
	}

	// end
	err = md.Close(ctx)
	require.NoError(t, err)
}

func TestKVDB_Close(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "Metadb")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "metadb")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(t, err)

	md, err := NewMetaHandler(diskdir, MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, md)

	var cnt int
	done := make(chan struct{})

	md.(*MetaDBWapper).onClosed = func() {
		cnt++
		close(done)
	}

	// Trigger Close
	err = md.Close(context.Background())
	require.NoError(t, err)

	select {
	case <-done:
		span.Infof("success gc")
	case <-time.After(10 * time.Second):
		t.Fail()
	}

	require.Equal(t, 1, cnt)
}

func TestDeleteRange(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "DeleteRange")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(t, err)

	md, err := newMetaDB(diskdir, MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, md)

	prefix := []byte("shards")
	keyPrefix1 := append(prefix, []byte{0xa}...)
	keyPrefix2 := append(prefix, []byte{0xb}...)
	keyBufLen := len(keyPrefix1) + 8

	expectedValue := []byte{0x1, 0x2, 0x3}

	// 3
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			defer wg.Done()
			keyBuf := make([]byte, keyBufLen)

			if i%2 == 0 {
				copy(keyBuf, keyPrefix1)
				binary.BigEndian.PutUint64(keyBuf[len(keyPrefix1):], uint64(i))
			} else {
				copy(keyBuf, keyPrefix2)
				binary.BigEndian.PutUint64(keyBuf[len(keyPrefix2):], uint64(i))
			}

			value := append([]byte(expectedValue), []byte{byte(i)}...)
			kv := rdb.KV{
				Key:   []byte(keyBuf),
				Value: value,
			}
			err := md.Put(ctx, kv)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	err = md.Flush(ctx)
	require.NoError(t, err)

	span.Infof("batch put success")

	minKey := make([]byte, keyBufLen)
	maxKey := make([]byte, keyBufLen)

	copy(minKey, keyPrefix1)
	binary.BigEndian.PutUint64(minKey[len(keyPrefix1):], uint64(0))

	copy(maxKey, keyPrefix1)
	binary.BigEndian.PutUint64(maxKey[len(keyPrefix1):], math.MaxUint64)

	t.Logf("minKey:%v, maxKey:%v", minKey, maxKey)

	err = md.DeleteRange(ctx, minKey, maxKey)
	require.NoError(t, err)

	// check
	for i := 0; i < 1000; i++ {
		keyBuf := make([]byte, keyBufLen)

		if i%2 == 0 {
			copy(keyBuf, keyPrefix1)
			binary.BigEndian.PutUint64(keyBuf[len(keyPrefix1):], uint64(i))
		} else {
			copy(keyBuf, keyPrefix2)
			binary.BigEndian.PutUint64(keyBuf[len(keyPrefix2):], uint64(i))
		}

		value, err := md.Get(ctx, []byte(keyBuf))
		if i%2 == 0 {
			require.Error(t, err)
			require.Contains(t, err.Error(), "file does not exist")
		} else {
			expValue := append([]byte(expectedValue), []byte{byte(i)}...)
			require.NoError(t, err)
			require.Equal(t, expValue, value)
		}
	}

	// end
	err = md.Close(ctx)
	require.NoError(t, err)
}
