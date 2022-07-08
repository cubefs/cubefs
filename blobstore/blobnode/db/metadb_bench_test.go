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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func BenchmarkKVDB_CoPut(b *testing.B) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbCoPut")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	log.SetOutputLevel(log.Lerror)
	defer log.SetOutputLevel(log.Linfo)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(b, err)

	md, err := newMetaDB(diskdir, MetaConfig{Sync: true})
	require.NoError(b, err)
	require.NotNil(b, md)

	expectedKey := []byte("testkey")

	// 3
	wg := sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(expectedKey), i)
			value := [64]byte{}
			kv := rdb.KV{
				Key:   []byte(key),
				Value: value[:],
			}
			err = md.Put(ctx, kv)
			require.NoError(b, err)
		}(i)
	}
	wg.Wait()

	err = md.Flush(ctx)
	require.NoError(b, err)

	span.Infof("batch put success")

	// end
	err = md.Close(ctx)
	require.NoError(b, err)
}

func BenchmarkKVDB_DirectPut(b *testing.B) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbBenchmarkPut")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	log.SetOutputLevel(log.Lerror)
	defer log.SetOutputLevel(log.Linfo)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(b, err)

	md, err := newMetaDB(diskdir, MetaConfig{Sync: true})
	require.NoError(b, err)
	require.NotNil(b, md)

	expectedKey := []byte("testkey")

	// 3
	wg := sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(expectedKey), i)
			value := [64]byte{}
			kv := rdb.KV{
				Key:   []byte(key),
				Value: value[:],
			}
			err = md.DirectPut(ctx, kv)
			require.NoError(b, err)
		}(i)
	}
	wg.Wait()

	err = md.Flush(ctx)
	require.NoError(b, err)

	span.Infof("batch put success")

	// end
	err = md.Close(ctx)
	require.NoError(b, err)
}

func BenchmarkKVDB_CoDelete(b *testing.B) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbCoDelete")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	log.SetOutputLevel(log.Lerror)
	defer log.SetOutputLevel(log.Linfo)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(b, err)

	md, err := newMetaDB(diskdir, MetaConfig{Sync: true})
	require.NoError(b, err)
	require.NotNil(b, md)

	expectedKey := []byte("testkey")

	// 3
	wg := sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(expectedKey), i)
			value := [64]byte{}
			kv := rdb.KV{
				Key:   []byte(key),
				Value: value[:],
			}
			err = md.Put(ctx, kv)
			require.NoError(b, err)
		}(i)
	}
	wg.Wait()

	span.Infof("co batch put success")

	wg = sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(expectedKey), i)
			err = md.Delete(ctx, []byte(key))
			require.NoError(b, err)
		}(i)
	}
	wg.Wait()

	span.Infof("co batch delete success")

	// end
	err = md.Close(ctx)
	require.NoError(b, err)
}

func BenchmarkKVDB_DirectDelete(b *testing.B) {
	testDir, err := ioutil.TempDir(os.TempDir(), "kvdbDelete")
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	log.SetOutputLevel(log.Lerror)
	defer log.SetOutputLevel(log.Linfo)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "kvloop")

	diskdir := filepath.Join(testDir, "disk1/")
	err = os.MkdirAll(diskdir, 0o755)
	require.NoError(b, err)

	md, err := newMetaDB(diskdir, MetaConfig{Sync: true})
	require.NoError(b, err)
	require.NotNil(b, md)

	expectedKey := []byte("testkey")

	// 3
	wg := sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(expectedKey), i)
			value := [64]byte{}
			kv := rdb.KV{
				Key:   []byte(key),
				Value: value[:],
			}
			err = md.DirectPut(ctx, kv)
			require.NoError(b, err)
		}(i)
	}
	wg.Wait()

	span.Infof("batch put success")

	wg = sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", string(expectedKey), i)
			err = md.DirectDelete(ctx, []byte(key))
			require.NoError(b, err)
		}(i)
	}
	wg.Wait()

	span.Infof("batch delete success")

	// end
	err = md.Close(ctx)
	require.NoError(b, err)
}
