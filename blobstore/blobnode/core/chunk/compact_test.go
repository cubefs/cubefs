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

package chunk

import (
	"bytes"
	"context"
	"hash/crc32"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/iopool"
)

type diskMock struct {
	diskID   proto.DiskID
	conf     *core.Config
	dataPath string
	metaPath string
	stats    core.DiskStats
	ioQos    qos.Qos
	status   proto.DiskStatus
}

func (mock *diskMock) ID() proto.DiskID {
	return mock.diskID
}

func (mock *diskMock) Status() (status proto.DiskStatus) {
	return mock.status
}

func (mock *diskMock) DiskInfo() (info bnapi.DiskInfo) {
	return
}

func (mock *diskMock) Stats() (stat core.DiskStats) {
	return mock.stats
}

func (mock *diskMock) GetChunkStorage(vuid proto.Vuid) (cs core.ChunkAPI, found bool) {
	return
}

func (mock *diskMock) GetConfig() (config *core.Config) {
	return mock.conf
}

func (mock *diskMock) GetIoQos() (ioQos qos.Qos) {
	return mock.ioQos
}

func (mock *diskMock) GetDataPath() (path string) {
	return mock.dataPath
}

func (mock *diskMock) GetMetaPath() (path string) {
	return mock.metaPath
}

func (mock *diskMock) SetStatus(status proto.DiskStatus) {
	mock.status = status
}

func (mock *diskMock) LoadDiskInfo(ctx context.Context) (dm core.DiskMeta, err error) {
	return
}

func (mock *diskMock) UpdateDiskStatus(ctx context.Context, status proto.DiskStatus) (err error) {
	return
}

func (mock *diskMock) CreateChunk(ctx context.Context, vuid proto.Vuid, chunksize int64) (cs core.ChunkAPI, err error) {
	return
}

func (mock *diskMock) ReleaseChunk(ctx context.Context, vuid proto.Vuid, force bool) (err error) {
	return
}

func (mock *diskMock) UpdateChunkStatus(ctx context.Context, vuid proto.Vuid, status bnapi.ChunkStatus) (err error) {
	return
}

func (mock *diskMock) UpdateChunkCompactState(ctx context.Context, vuid proto.Vuid, compacting bool) (err error) {
	return
}

func (mock *diskMock) ListChunks(ctx context.Context) (chunks []core.VuidMeta, err error) {
	return
}

func (mock *diskMock) EnqueueCompact(ctx context.Context, vuid proto.Vuid) {
}

func (mock *diskMock) GcRubbishChunk(ctx context.Context) (mayBeLost []bnapi.ChunkId, err error) {
	return
}

func (mock *diskMock) WalkChunksWithLock(ctx context.Context, fn func(cs core.ChunkAPI) error) (err error) {
	return
}

func (mock *diskMock) ResetChunks(ctx context.Context) {
}

func (mock *diskMock) Close(ctx context.Context) {
}

func ensureTestDir(t *testing.T, diskRoot string) (root, meta, data string) {
	err := core.EnsureDiskArea(diskRoot, "")
	require.NoError(t, err)

	root = diskRoot
	data = core.GetDataPath(diskRoot)
	meta = core.GetMetaPath(diskRoot, "")
	return
}

func createTestChunk(t *testing.T, ctx context.Context, diskRoot string, vuid proto.Vuid, readScheduler iopool.IoScheduler, writeScheduler iopool.IoScheduler) (cs *Chunk) {
	_, metaPath, dataPath := ensureTestDir(t, diskRoot)

	dbHandler, err := db.NewMetaHandler(metaPath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, dbHandler)

	chunkId := bnapi.NewChunkId(vuid)
	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  12,
		ChunkId: chunkId,
		Mtime:   time.Now().UnixNano(),
		Status:  bnapi.ChunkStatusNormal,
	}

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
			CompactBatchSize:      core.DefaultCompactBatchSize,
			NeedCompactCheck:      true,
			BlockBufferSize:       64 * 1024,
		},
	}
	ioQos, _ := qos.NewQosManager(qos.Config{})
	chunk, err := NewChunkStorage(ctx, dataPath, vm, readScheduler, writeScheduler, func(option *core.Option) {
		option.Conf = conf
		option.DB = dbHandler
		option.CreateDataIfMiss = true
		option.Disk = &diskMock{dataPath: dataPath, conf: conf, ioQos: ioQos}
		option.IoQos = ioQos
	})
	require.NoError(t, err)
	require.NotNil(t, chunk)

	return chunk
}

func TestChunk_StartCompact(t *testing.T) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "BlobNodeService")

	testDir, err := ioutil.TempDir(os.TempDir(), "StartCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	vuid := proto.Vuid(1024)
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs := createTestChunk(t, ctx, testDir, vuid, readScheduler, writeScheduler)
	require.NotNil(t, cs)
	cs.onClosed = func() {
		span.Infof("=== gc ===")
	}

	// keep raw
	rawStg := cs.getStg()

	// ================= Scene: no data ================
	// --------------- start compact ---------------
	newcs, err := cs.StartCompact(ctx)
	require.NoError(t, err)
	require.NotNil(t, newcs)

	replStg := cs.getStg()
	require.NotEqual(t, rawStg, replStg)
	require.Equal(t, rawStg, replStg.RawStorage())
	require.Equal(t, true, cs.compacting)

	// --------------- stop compact ---------------
	err = cs.StopCompact(ctx, newcs)
	require.NoError(t, err)
	require.Equal(t, false, cs.compacting)
	require.Equal(t, rawStg, cs.getStg())
	require.Equal(t, nil, cs.getStg().RawStorage())

	// -------------- destroy new stg -----------------
	newcs.(*chunk).Destroy(ctx)
	require.Equal(t, true, newcs.IsClosed())

	// ================= Scene: with data ===============
	// prepare data
	span.Infof("write shard to cs")
	shardData := []byte("TestData")
	shardSize := uint32(len(shardData))
	shardCrc := crc32.NewIEEE()
	n, err := shardCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, shardSize, uint32(n))

	shardCnt := 128
	for i := 1; i <= shardCnt; i++ {
		shard := &core.Shard{
			Bid:  proto.BlobID(i),
			Vuid: vuid,
			Flag: bnapi.ShardStatusNormal,
			Size: shardSize,
			Body: bytes.NewReader(shardData),
		}
		err = cs.Write(ctx, shard)
		require.NoError(t, err)
	}

	// start compact
	newcs, err = cs.StartCompact(ctx)
	require.NoError(t, err)
	require.NotNil(t, newcs)

	replStg = cs.getStg()
	require.NotEqual(t, rawStg, replStg)
	require.Equal(t, rawStg, replStg.RawStorage())
	require.Equal(t, true, cs.compacting)

	// source list check
	sis, _, err := cs.ListShards(ctx, 0, 4096, bnapi.ShardStatusNormal)
	require.NoError(t, err)
	require.Equal(t, shardCnt, len(sis))

	// new stg list check
	sis, _, err = newcs.ListShards(ctx, 0, 4096, bnapi.ShardStatusNormal)
	require.NoError(t, err)
	require.Equal(t, shardCnt, len(sis))

	// value deep check
	for i := 1; i <= shardCnt; i++ {
		srcShard, err := cs.NewReader(ctx, proto.BlobID(i))
		require.NoError(t, err)
		srcData, err := ioutil.ReadAll(srcShard.Body)
		require.NoError(t, err)
		require.Equal(t, true, reflect.DeepEqual(shardData, srcData))

		dstShard, err := newcs.(*chunk).NewReader(ctx, proto.BlobID(i))
		require.NoError(t, err)
		dstData, err := ioutil.ReadAll(dstShard.Body)
		require.NoError(t, err)

		require.Equal(t, true, reflect.DeepEqual(srcData, dstData))
	}

	// -------- stop compact --------
	err = cs.StopCompact(ctx, newcs)
	require.NoError(t, err)
	require.Equal(t, false, cs.compacting)
	require.Equal(t, rawStg, cs.getStg())
	require.Equal(t, nil, cs.getStg().RawStorage())

	// -------------- destroy new stg -----------------
	newcs.(*chunk).Destroy(ctx)
	require.Equal(t, true, newcs.IsClosed())

	// ======== Scene: write when compacting ============
	wg := sync.WaitGroup{}
	chunkCh := make(chan core.ChunkAPI)
	// start compacting
	wg.Add(1)
	go func() {
		wg.Done()
		newcs, err = cs.StartCompact(ctx)
		require.NoError(t, err)
		require.NotNil(t, newcs)
		chunkCh <- newcs
	}()

	// start write(over)
	shardDataNew := []byte("new")
	// overwrite
	for i := 1; i <= shardCnt; i++ {
		index := i
		if index%2 != 0 {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shard := &core.Shard{
				Bid:  proto.BlobID(i),
				Vuid: vuid,
				Flag: bnapi.ShardStatusNormal,
				Size: uint32(len(shardDataNew)),
				Body: bytes.NewReader(shardDataNew),
			}
			err := cs.Write(ctx, shard)
			require.NoError(t, err)
		}(index)
	}
	// add shard
	for i := shardCnt + 1; i <= shardCnt+10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shard := &core.Shard{
				Bid:  proto.BlobID(i),
				Vuid: vuid,
				Flag: bnapi.ShardStatusNormal,
				Size: uint32(len(shardDataNew)),
				Body: bytes.NewReader(shardDataNew),
			}
			err := cs.Write(ctx, shard)
			require.NoError(t, err)
		}(i)
	}

	// wait join
	wg.Wait()
	// happen-before
	newcs = <-chunkCh

	// source list check
	sis, _, err = cs.ListShards(ctx, 0, 4096, bnapi.ShardStatusNormal)
	require.NoError(t, err)
	require.Equal(t, shardCnt+10, len(sis))

	// new stg list check
	sis, _, err = newcs.ListShards(ctx, 0, 4096, bnapi.ShardStatusNormal)
	require.NoError(t, err)
	require.Equal(t, shardCnt+10, len(sis))

	// value deep check
	for i := 1; i <= shardCnt+10; i++ {
		srcShard, err := cs.NewReader(ctx, proto.BlobID(i))
		require.NoError(t, err)
		srcData, err := ioutil.ReadAll(srcShard.Body)
		require.NoError(t, err)

		dstShard, err := newcs.(*chunk).NewReader(ctx, proto.BlobID(i))
		require.NoError(t, err)
		dstData, err := ioutil.ReadAll(dstShard.Body)
		require.NoError(t, err)

		require.Equal(t, true, reflect.DeepEqual(srcData, dstData))

		// New request
		if i > shardCnt {
			require.Equal(t, true, reflect.DeepEqual(shardDataNew, srcData))
			require.Equal(t, true, reflect.DeepEqual(shardDataNew, dstData))
			continue
		}

		if i%2 != 0 {
			require.Equal(t, true, reflect.DeepEqual(shardData, srcData))
		} else {
			// modify
			require.Equal(t, true, reflect.DeepEqual(shardDataNew, srcData))
		}
	}

	// -------- commit compact --------
	newStg := newcs.(*chunk).getStg()
	err = cs.CommitCompact(ctx, newcs)
	require.NoError(t, err)
	require.Equal(t, true, cs.compacting)
	require.Equal(t, newStg, cs.getStg())
	require.Equal(t, nil, cs.getStg().RawStorage())

	// -------- stop compact --------
	err = cs.StopCompact(ctx, newcs)
	require.NoError(t, err)
	require.Equal(t, false, cs.compacting)
	require.Equal(t, newStg, cs.getStg())
	require.Equal(t, nil, cs.getStg().RawStorage())

	// -------------- destroy new stg -----------------
	newcs.(*chunk).Destroy(ctx)
	require.Equal(t, true, newcs.IsClosed())

	// ======== Scene: write when compacting ============

	// vuid not match
	//dstChunkStorage.vuid = vuid + 1
	//
	//err = srcChunkStorageWapper.chunk.doCompact(ctx, dstChunkStorage)
	//require.Error(t, err)
	//dstChunkStorage.vuid = vuid
	//
	//require.NotNil(t, srcChunkStorageWapper)

	// --------------- release ---------------
	require.NotNil(t, cs)
	cs = nil
	runtime.GC()
	runtime.GC()
}

func TestChunkStorage_StopCompact(t *testing.T) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "BlobNodeService")

	testDir, err := ioutil.TempDir(os.TempDir(), "StopCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	vuid := proto.Vuid(1024)
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs := createTestChunk(t, ctx, testDir, vuid, readScheduler, writeScheduler)
	require.NotNil(t, cs)
	cs.onClosed = func() {
		span.Infof("=== gc ===")
	}

	shardData := []byte("TestData")
	shardSize := uint32(len(shardData))

	wg := sync.WaitGroup{}
	for i := 1; i <= 128; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shard := &core.Shard{
				Bid:  proto.BlobID(i),
				Vuid: vuid,
				Flag: bnapi.ShardStatusNormal,
				Size: shardSize,
				Body: bytes.NewReader(shardData),
			}
			err := cs.Write(ctx, shard)
			require.NoError(t, err)
		}(i)
	}
	time.Sleep(time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		task := cs.compactTask.Load().(*compactTask)
		task.once.Do(func() {
			close(task.stopCh)
		})
	}()

	time.Sleep(time.Second)

	newcs, err := cs.StartCompact(ctx)
	require.Nil(t, newcs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stopped")

	wg.Wait()

	// --------------- release ---------------
	require.NotNil(t, cs)
	cs = nil
	runtime.GC()
	runtime.GC()
}

func TestChunkStorage_CompactCheck(t *testing.T) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "BlobNodeService")

	testDir, err := ioutil.TempDir(os.TempDir(), "CompactCheck")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	vuid := proto.Vuid(1024)
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs := createTestChunk(t, ctx, testDir, vuid, readScheduler, writeScheduler)
	require.NotNil(t, cs)

	done := make(chan struct{})
	cs.onClosed = func() {
		span.Infof("=== gc ===")
		close(done)
	}

	shardData := []byte("TestData")
	shardSize := uint32(len(shardData))

	shardData1 := []byte("TestDataxxxxxxxxxx")

	runtime.GC()
	time.Sleep(time.Second)

	// prepare write data
	count := 50
	// [30, 30+50)
	for i := 30; i < 30+count; i++ {
		shard := &core.Shard{
			Bid:  proto.BlobID(i),
			Vuid: vuid,
			Flag: bnapi.ShardStatusNormal,
			Size: shardSize,
			Body: bytes.NewReader(shardData),
		}
		err = cs.Write(ctx, shard)
		require.NoError(t, err)
	}

	sis, _, err := cs.ListShards(ctx, 0, count, bnapi.ShardStatusNormal)
	require.NoError(t, err)
	require.Equal(t, count, len(sis))

	// ========== Scene: normal [30, 30+50) ===================
	newcs, err := cs.StartCompact(ctx)
	require.NoError(t, err)

	// must *chunk type
	_, ok := newcs.(*chunk)
	require.Equal(t, true, ok)

	// repl write
	err = newcs.Write(ctx, &core.Shard{
		Bid:  proto.BlobID(30005),
		Vuid: vuid,
		Flag: bnapi.ShardStatusNormal,
		Size: shardSize,
		Body: bytes.NewReader(shardData),
	})
	require.NoError(t, err)

	// equal src == dest
	err = cs.compactCheck(ctx, newcs.(*chunk))
	require.NoError(t, err)

	// clean new cs
	err = cs.StopCompact(ctx, newcs)
	require.NoError(t, err)
	newcs.(*chunk).Destroy(ctx)
	require.Equal(t, true, newcs.IsClosed())

	// ========== Scene: overwrite [10, 128) when compacting ===================
	span.Info("Scene: overwrite [10, 100) when compacting")
	wg := sync.WaitGroup{}
	for i := 1; i < 128; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := shardData
			size := len(shardData)
			if i%2 == 0 {
				body = shardData1
				size = len(shardData1)
			}
			shard := &core.Shard{
				Bid:  proto.BlobID(i),
				Vuid: vuid,
				Flag: bnapi.ShardStatusNormal,
				Size: uint32(size),
				Body: bytes.NewReader(body),
			}
			err := cs.Write(ctx, shard)
			require.NoError(t, err)
		}(i)
	}

	newcs, err = cs.StartCompact(ctx)
	if err != nil {
		debug.PrintStack()
	}
	require.NoError(t, err)

	err = cs.compactCheck(ctx, newcs.(*chunk))
	require.NoError(t, err)

	wg.Wait()

	// ========== Scene: overwrite [10, 256) when compact check ===================
	for i := 10; i < 256; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := shardData
			size := len(shardData)
			if i%2 == 0 {
				body = shardData1
				size = len(shardData1)
			}
			shard := &core.Shard{
				Bid:  proto.BlobID(i),
				Vuid: vuid,
				Flag: bnapi.ShardStatusNormal,
				Size: uint32(size),
				Body: bytes.NewReader(body),
			}
			err := cs.Write(ctx, shard)
			require.NoError(t, err)
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cs.compactCheck(ctx, newcs.(*chunk))
		require.NoError(t, err)
	}()
	wg.Wait()

	// ========== Scene: compact check failed ===================
	err = newcs.Write(ctx, &core.Shard{
		Bid:  proto.BlobID(10),
		Vuid: vuid,
		Flag: bnapi.ShardStatusMarkDelete,
		Size: 1,
		Body: bytes.NewReader([]byte("1")),
	})
	require.NoError(t, err)

	err = cs.compactCheck(ctx, newcs.(*chunk))
	require.Error(t, err)

	// --------------- release ---------------
	require.NotNil(t, cs)
	cs = nil
	runtime.GC()
	runtime.GC()
}
