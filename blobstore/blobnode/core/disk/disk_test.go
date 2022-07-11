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

package disk

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func getDiskIDFn(ctx context.Context) (proto.DiskID, error) {
	return 101, nil
}

func handleIOErrorFn(ctx context.Context, diskID proto.DiskID, diskErr error) {
}

func setChunkCompactFn(ctx context.Context, args *cmapi.SetCompactChunkArgs) (err error) {
	return
}

func TestNewDiskStorage(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "NewDiskStorage")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	cs, err := ds.CreateChunk(ctx, proto.Vuid(1), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, proto.Vuid(2), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	require.Equal(t, len(ds.Chunks), 2)

	done := make(chan struct{})
	ds.OnClosed = func() {
		close(done)
	}

	ds.ResetChunks(ctx)

	ds = nil

	// tigger gc
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(time.Second * 1)
	}

	select {
	case <-done:
		span.Infof("gc success")
	case <-time.After(10 * time.Second):
		t.Fail()
	}

	// second time. reload
	ds, err = NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.Equal(t, 2, len(ds.Chunks))
	defer ds.ResetChunks(ctx)

	ds.runCompactFiles()

	span.Infof("1. done")

	// err condition

	diskConfig.MustMountPoint = true
	_, err = NewDiskStorage(ctx, diskConfig)
	require.Error(t, err)

	diskConfig.Path = "/tmp/test.txt"
	_, err = NewDiskStorage(ctx, diskConfig)
	require.Error(t, err)
}

func TestRunCompact(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "TestRunCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	ds.Conf.CompactEmptyRateThreshold = 0

	cs, err := ds.CreateChunk(ctx, proto.Vuid(1), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	shardData := []byte("test")
	shard := &core.Shard{
		Bid:  1,
		Vuid: cs.Vuid(),
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(shardData)),
		Body: bytes.NewReader(shardData),
	}

	// write data
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	err = cs.MarkDelete(ctx, 1)
	require.NoError(t, err)
	err = cs.Delete(ctx, 1)
	require.NoError(t, err)

	go func() {
		vuid := <-ds.compactCh
		fmt.Println(vuid)
	}()

	defer ds.ResetChunks(ctx)
	ds.runCompactFiles()
}

func TestDiskStorage_UpdateChunkStatus(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "UpdateChunkStatus")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(context.TODO(), diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	defer ds.ResetChunks(context.TODO())

	cs, err := ds.CreateChunk(context.TODO(), proto.Vuid(1), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	err = ds.UpdateChunkStatus(context.TODO(), cs.Vuid(), bnapi.ChunkStatusReadOnly)
	require.NoError(t, err)

	chunks, err := ds.ListChunks(context.TODO())
	require.NoError(t, err)

	require.Equal(t, 1, len(chunks))
	require.Equal(t, bnapi.ChunkStatusReadOnly, chunks[0].Status)

	ds = nil
	cs = nil
}

func TestSuperBlock_UpdateDiskStatus(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "UpdateDiskStatus")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)
	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	mockClusterMgr := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

	clusterMgrConfig := &cmapi.Config{}
	clusterMgrConfig.Hosts = []string{mockClusterMgr.URL}
	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	defer ds.ResetChunks(ctx)

	cs, err := ds.CreateChunk(ctx, proto.Vuid(1), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	err = ds.UpdateDiskStatus(ctx, proto.DiskStatusBroken)
	require.NoError(t, err)

	diskmeta, err := ds.LoadDiskInfo(ctx)
	require.NoError(t, err)

	require.Equal(t, int(proto.DiskStatusBroken), int(diskmeta.Status))
}

func TestDiskStorage_CompactChunkFile2(t *testing.T) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "NewBlobNodeService")

	testDir, err := ioutil.TempDir(os.TempDir(), "CompactChunkFile2")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	defer ds.ResetChunks(ctx)

	vuid := proto.Vuid(101)

	cs, err := ds.CreateChunk(context.TODO(), vuid, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	// write data
	testData := []byte("TestData")
	dataSize := uint32(len(testData))
	count := 283
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(testData)
	require.NoError(t, err)
	require.Equal(t, dataSize, uint32(n))

	span.Infof("write shard to cs")
	for i := 1; i <= count; i++ {
		shard := &core.Shard{
			Bid:  proto.BlobID(i),
			Vuid: vuid,
			Flag: bnapi.ShardStatusNormal,
			Size: dataSize,
			Body: bytes.NewReader(testData),
		}
		err = cs.Write(ctx, shard)
		require.NoError(t, err)
	}

	shardinfos, _, err := cs.ListShards(ctx, proto.InValidBlobID, 1024, 0)
	require.NoError(t, err)
	require.Equal(t, len(shardinfos), count)

	// delete [1-10]
	for i := 1; i <= 10; i++ {
		err = cs.MarkDelete(ctx, proto.BlobID(i))
		require.NoError(t, err)
		err = cs.Delete(ctx, proto.BlobID(i))
		require.NoError(t, err)
	}

	shardinfos, _, err = cs.ListShards(ctx, proto.InValidBlobID, 1024, 0)
	require.NoError(t, err)
	require.Equal(t, len(shardinfos), count-10)

	oldchunkid := cs.ID()

	// compact
	err = ds.ExecCompactChunk(vuid)
	require.NoError(t, err)

	newchunkid := cs.ID()
	require.NotEqual(t, oldchunkid, newchunkid)

	shardinfos, _, err = cs.ListShards(ctx, proto.InValidBlobID, 1024, 0)
	require.NoError(t, err)
	require.Equal(t, len(shardinfos), count-10)
}

func TestExecCompact(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "ExecCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	mockClusterMgrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/chunk/set/compact") {
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	cc := &cmapi.Config{}
	cc.Hosts = []string{mockClusterMgrServer.URL}

	cmClient := cmapi.New(cc)

	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: cmClient.SetCompactChunk,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	defer ds.ResetChunks(ctx)

	vuid1 := proto.Vuid(2001)
	vuid2 := proto.Vuid(2002)

	cs, err := ds.CreateChunk(ctx, vuid1, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, vuid2, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)
	require.Equal(t, len(ds.Chunks), 2)

	err = ds.ExecCompactChunk(vuid1)
	require.Error(t, err)

	err = ds.ExecCompactChunk(proto.Vuid(2004))
	require.Error(t, err)
}

func TestCleanChunk(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "CleanChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		RuntimeConfig: core.RuntimeConfig{
			ChunkCleanIntervalSec: 1,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	vuid1 := proto.Vuid(2001)
	vuid2 := proto.Vuid(2002)

	cs, err := ds.CreateChunk(ctx, vuid1, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, vuid2, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	require.Equal(t, len(ds.Chunks), 2)

	time.Sleep(2 * time.Second)

	err = ds.ReleaseChunk(ctx, vuid1, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unexpected")

	err = ds.UpdateChunkStatus(ctx, vuid1, bnapi.ChunkStatusReadOnly)
	require.NoError(t, err)

	err = ds.ReleaseChunk(ctx, vuid1, false)
	require.NoError(t, err)

	ds.Conf.ChunkReleaseProtectionM = 0
	err = ds.cleanReleasedChunks()
	require.NoError(t, err)

	ds.ResetChunks(ctx)
	ds = nil
	runtime.GC()
}

func TestCheckChunkFile(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "CheckChunkFile")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	defer ds.ResetChunks(ctx)

	vuid1 := proto.Vuid(2001)
	vuid2 := proto.Vuid(2002)

	cs, err := ds.CreateChunk(ctx, vuid1, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, vuid2, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	require.Equal(t, len(ds.Chunks), 2)

	path, err := filepath.Abs(diskpath)
	require.NoError(t, err)

	mockChunkFileName := filepath.Join(core.GetDataPath(path), bnapi.NewChunkId(proto.Vuid(2003)).String())
	mockChunkData, err := os.Create(mockChunkFileName)
	require.NoError(t, err)
	defer mockChunkData.Close()

	_, err = ds.GcRubbishChunk(ctx)
	require.NoError(t, err)
}

func TestDiskstorage_Finalizer(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "NewDiskStorage")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	cs, err := ds.CreateChunk(ctx, proto.Vuid(1), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, proto.Vuid(2), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	path := ds.GetMetaPath()
	require.NotNil(t, path)

	require.Equal(t, len(ds.Chunks), 2)

	var cnt int
	done := make(chan struct{})
	ds.OnClosed = func() {
		cnt++
		close(done)
	}

	ds.ResetChunks(ctx)
	ds = nil
	// Trigger recycling
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		runtime.GC()
	}

	select {
	case <-done:
		span.Infof("success gc")
	case <-time.After(10 * time.Second):
		t.Fail()
	}

	require.Equal(t, 1, cnt)
}

func TestDiskStorageWrapper_CreateChunk(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "CreateChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	cs, err := ds.CreateChunk(ctx, proto.Vuid(102), MaxChunkSize+1)
	require.Error(t, err)
	require.Nil(t, cs)
}

func TestDiskStorage_ReleaseChunk(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "CreateChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	vuid := proto.Vuid(1002)
	cs, err := ds.CreateChunk(ctx, vuid, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	err = cs.SetStatus(bnapi.ChunkStatusRelease)
	require.NoError(t, err)
	err = ds.ReleaseChunk(ctx, vuid, false)
	require.Error(t, err)

	err = ds.ReleaseChunk(ctx, proto.Vuid(103), true)
	require.Error(t, err)

	ds.status = proto.DiskStatusRepairing
	err = ds.ReleaseChunk(ctx, vuid, true)
	require.Error(t, err)
}

func TestDiskStorage_UpdateChunkStatus2(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "CreateChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	vuid := proto.Vuid(1002)
	cs, err := ds.CreateChunk(ctx, vuid, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	err = cs.SetStatus(bnapi.ChunkStatusRelease)
	require.NoError(t, err)
	err = ds.UpdateChunkStatus(ctx, vuid, bnapi.ChunkStatusRelease)
	require.Nil(t, err)

	err = ds.UpdateChunkStatus(ctx, vuid, bnapi.ChunkStatusNormal)
	require.Error(t, err)

	err = ds.UpdateChunkStatus(ctx, proto.Vuid(1003), bnapi.ChunkStatusReadOnly)
	require.Error(t, err)

	err = ds.UpdateChunkStatus(ctx, vuid, 6)
	require.Error(t, err)
}
