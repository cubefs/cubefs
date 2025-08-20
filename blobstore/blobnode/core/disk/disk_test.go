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
	"hash/crc32"
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
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func getDiskIDFn(ctx context.Context) (proto.DiskID, error) {
	return 101, nil
}

func handleIOErrorFn(ctx context.Context, diskID proto.DiskID, diskErr error) {
	// do nothing
}

func setChunkCompactFn(ctx context.Context, args *cmapi.SetCompactChunkArgs) (err error) {
	return
}

func getGlobalConfigFn(ctx context.Context, key string) (val string, err error) {
	if key == proto.ChunkOversoldRatioKey {
		return "0.5", nil
	}
	return "", nil
}

func TestNewDiskStorage(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestNewDiskStorage")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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
	testDir, err := os.MkdirTemp(os.TempDir(), "TestRunCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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
		log.Info(vuid)
	}()

	defer ds.ResetChunks(ctx)
	ds.runCompactFiles()
}

func TestDiskStorage_UpdateChunkStatus(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestUpdateChunkStatus")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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

	err = ds.UpdateChunkStatus(context.TODO(), cs.Vuid(), cmapi.ChunkStatusReadOnly)
	require.NoError(t, err)

	chunks, err := ds.ListChunks(context.TODO())
	require.NoError(t, err)

	require.Equal(t, 1, len(chunks))
	require.Equal(t, cmapi.ChunkStatusReadOnly, chunks[0].Status)

	ds = nil
	cs = nil
}

func TestSuperBlock_UpdateDiskStatus(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestUpdateDiskStatus")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)
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

	testDir, err := os.MkdirTemp(os.TempDir(), "TestCompactChunkFile2")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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
	diskConfig.WaitPendingReqIntervalSec = 1
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	defer ds.ResetChunks(ctx)

	vuid := proto.Vuid(101)

	ds.Conf.CompactTriggerThreshold = 1
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

	ctx = bnapi.SetIoType(ctx, bnapi.WriteIO)
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
	testDir, err := os.MkdirTemp(os.TempDir(), "TestExecCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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
	vuid3 := proto.Vuid(2003)

	cs, err := ds.CreateChunk(ctx, vuid1, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, vuid2, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, vuid3, 4096)
	require.NoError(t, err)
	require.NotNil(t, cs)

	require.Equal(t, len(ds.Chunks), 3)

	// dont need compact
	err = ds.ExecCompactChunk(vuid3)
	require.NoError(t, err)

	// 501, http.StatusNotImplemented
	ds.Conf.CompactTriggerThreshold = 1
	err = ds.ExecCompactChunk(vuid1)
	require.Error(t, err)
	code := rpc.DetectStatusCode(err)
	require.Equal(t, http.StatusNotImplemented, code)

	// not found
	err = ds.ExecCompactChunk(proto.Vuid(2004))
	require.Error(t, err)
	require.ErrorIs(t, bloberr.ErrNoSuchVuid, err)
}

func TestCleanChunk(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestCleanChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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

	time.Sleep(1 * time.Second)

	err = ds.ReleaseChunk(ctx, vuid1, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unexpected")

	err = ds.UpdateChunkStatus(ctx, vuid1, cmapi.ChunkStatusReadOnly)
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
	testDir, err := os.MkdirTemp(os.TempDir(), "TestCheckChunkFile")
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

	mockChunkFileName := filepath.Join(core.GetDataPath(path), cmapi.NewChunkID(proto.Vuid(2003)).String())
	mockChunkData, err := os.Create(mockChunkFileName)
	require.NoError(t, err)
	defer mockChunkData.Close()

	_, err = ds.GcRubbishChunk(ctx)
	require.NoError(t, err)
}

func TestDiskstorage_Finalizer(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestNewDiskStorage")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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
	testDir, err := os.MkdirTemp(os.TempDir(), "TestCreateChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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

func TestDiskStorageWrapper_CreateChunkOversold(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestCreateChunkOversold")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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
		GetGlobalConfig:  getGlobalConfigFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	t.Logf("%+v", ds.Stats())
	// no error when enable oversold
	for i := 0; i < 50; i++ {
		_, err := ds.CreateChunk(ctx, proto.Vuid(100+i), MaxChunkSize)
		require.NoError(t, err)
	}
}

func TestDiskStorage_ReleaseChunk(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestCreateChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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

	err = cs.SetStatus(cmapi.ChunkStatusRelease)
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
	testDir, err := os.MkdirTemp(os.TempDir(), "TestCreateChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

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

	err = cs.SetStatus(cmapi.ChunkStatusRelease)
	require.NoError(t, err)
	err = ds.UpdateChunkStatus(ctx, vuid, cmapi.ChunkStatusRelease)
	require.Nil(t, err)

	err = ds.UpdateChunkStatus(ctx, vuid, cmapi.ChunkStatusNormal)
	require.Error(t, err)

	err = ds.UpdateChunkStatus(ctx, proto.Vuid(1003), cmapi.ChunkStatusReadOnly)
	require.Error(t, err)

	err = ds.UpdateChunkStatus(ctx, vuid, 6)
	require.Error(t, err)
}

func TestDiskStorage_RedundantChunks(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestRedundantChunks")
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

	ds, err := newDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	vuid := proto.Vuid(1001)
	chunkOld := cmapi.NewChunkID(vuid)
	metaRedundant := core.VuidMeta{
		DiskID:     101,
		Vuid:       vuid,
		ChunkID:    chunkOld,
		Mtime:      time.Now().UnixNano(),
		Compacting: true,
		Status:     cmapi.ChunkStatusNormal, // 1
	}
	err = ds.SuperBlock.UpsertChunk(ctx, metaRedundant.ChunkID, metaRedundant)
	require.NoError(t, err)

	chunkNew := cmapi.NewChunkID(vuid)
	metaNew := metaRedundant
	metaNew.ChunkID = chunkNew
	metaNew.Mtime = metaRedundant.Mtime + time.Second.Nanoseconds()
	metaNew.Compacting = false
	err = ds.SuperBlock.UpsertChunk(ctx, metaNew.ChunkID, metaNew)
	require.NoError(t, err)
	err = ds.SuperBlock.BindVuidChunk(ctx, vuid, metaNew.ChunkID)
	require.NoError(t, err)

	chunkOldFile := filepath.Join(ds.DataPath, chunkOld.String())
	_, err = os.OpenFile(chunkOldFile, os.O_CREATE, 0o644)
	require.NoError(t, err)
	chunkNewFile := filepath.Join(ds.DataPath, chunkNew.String())
	_, err = os.OpenFile(chunkNewFile, os.O_CREATE, 0o644)
	require.NoError(t, err)

	dsw := &DiskStorageWrapper{DiskStorage: ds}
	err = dsw.RestoreChunkStorage(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(dsw.Chunks))

	cs, found := dsw.GetChunkStorage(vuid)
	require.True(t, found)
	require.NotNil(t, cs)
	require.Equal(t, chunkNew, cs.ID())

	vuid2Chunk, err := ds.SuperBlock.ListVuids(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(vuid2Chunk))

	vuidMetas, err := ds.SuperBlock.ListChunks(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(vuidMetas))

	meta := vuidMetas[chunkOld]
	require.Equal(t, chunkOld, meta.ChunkID)
	require.Equal(t, cmapi.ChunkStatusRelease, meta.Status)

	meta = vuidMetas[chunkNew]
	require.Equal(t, chunkNew, meta.ChunkID)
	require.Equal(t, cmapi.ChunkStatusNormal, meta.Status)
}
