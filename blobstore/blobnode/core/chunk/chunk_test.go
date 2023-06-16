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
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/iopool"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	defaultDiskTestDir = "NodeDiskTestDir"
)

func TestNewChunkStorage(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"NewChunkStorage")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := bnapi.NewChunkId(vuid)

	err = core.EnsureDiskArea(testDir, "")
	require.NoError(t, err)

	datapath := core.GetDataPath(testDir)
	log.Info(datapath)
	metapath := core.GetMetaPath(testDir, "")
	log.Info(metapath)

	kvdb, err := db.NewMetaHandler(metapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)

	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  12,
		ChunkId: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  bnapi.ChunkStatusNormal,
	}

	ioQos, _ := qos.NewQosManager(qos.Config{})
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs, err := NewChunkStorage(context.TODO(), datapath, vm, readScheduler, writeScheduler, func(option *core.Option) {
		option.Conf = conf
		option.DB = kvdb
		option.CreateDataIfMiss = true
		option.IoQos = ioQos
	})
	require.NoError(t, err)
	require.NotNil(t, cs)

	_ = cs.ChunkInfo(context.TODO())

	_ = cs.VuidMeta()

	swap := cs.CasDirty(0, 1)
	require.Equal(t, true, swap)

	closeStatus := cs.IsClosed()
	require.Equal(t, false, closeStatus)

	err = cs.UnmarshalJSON([]byte("!"))
	require.Error(t, err)

	cs = nil
	runtime.GC()
	time.Sleep(1 * time.Second)
}

func TestChunkStorage_ReadWrite(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkStorageRW")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
			BlockBufferSize:       64 * 1024,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := bnapi.NewChunkId(vuid)

	err = core.EnsureDiskArea(testDir, "")
	require.NoError(t, err)

	datapath := core.GetDataPath(testDir)
	log.Info(datapath)
	metapath := core.GetMetaPath(testDir, "")
	log.Info(metapath)

	kvdb, err := db.NewMetaHandler(metapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)

	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  12,
		ChunkId: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  bnapi.ChunkStatusNormal,
	}

	ioQos, _ := qos.NewQosManager(qos.Config{})
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, readScheduler, writeScheduler, func(option *core.Option) {
		option.Conf = conf
		option.DB = kvdb
		option.CreateDataIfMiss = true
		option.IoQos = ioQos
	})
	require.NoError(t, err)
	require.NotNil(t, cs)

	// build shard data
	shardData := []byte("test data")
	shardSize := len(shardData)
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, shardSize, n)

	bid := proto.BlobID(1024)

	// write failed. argument check
	shard := &core.Shard{
		Bid:  bid,
		Vuid: 10,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(shardData)),
		Body: bytes.NewReader(shardData),
	}

	err = cs.Write(ctx, shard)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not match")

	// normal write
	shard = &core.Shard{
		Bid:  bid,
		Vuid: vuid,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(shardData)),
		Body: bytes.NewReader(shardData),
	}

	// write data
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	// read data and check
	rs, err := cs.NewReader(ctx, bid)
	require.NoError(t, err)
	require.NotNil(t, rs)

	require.Equal(t, shard.Bid, rs.Bid)
	require.Equal(t, shard.Vuid, rs.Vuid)
	require.Equal(t, shard.Flag, rs.Flag)
	require.Equal(t, shard.Size, rs.Size)

	rd, err := ioutil.ReadAll(rs.Body)
	require.NoError(t, err)
	require.Equal(t, shardData, rd)

	// read meta
	me, err := cs.ReadShardMeta(ctx, bid)
	require.NoError(t, err)
	require.NotNil(t, me)

	require.Equal(t, false, me.Inline)
	require.Equal(t, shard.Size, me.Size)

	// calc crc32
	expectedCrc := crc32.ChecksumIEEE(shardData)
	require.Equal(t, expectedCrc, shard.Crc)
	log.Info(expectedCrc)

	from, to := int64(0), int64(1)
	_, err = cs.NewRangeReader(ctx, shard.Bid, from, to)
	require.NoError(t, err)

	cs.compacting = true
	shard.Body = bytes.NewReader(shardData)
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	cs = nil
	runtime.GC()
	time.Sleep(1 * time.Second)
}

func TestChunkStorage_ReadWriteInline(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkStorageRWInline")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
		},
		MetaConfig: db.MetaConfig{
			SupportInline: true,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := bnapi.NewChunkId(vuid)

	err = core.EnsureDiskArea(testDir, "")
	require.NoError(t, err)

	datapath := core.GetDataPath(testDir)
	log.Info(datapath)
	metapath := core.GetMetaPath(testDir, "")
	log.Info(metapath)

	kvdb, err := db.NewMetaHandler(metapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)

	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  12,
		ChunkId: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  bnapi.ChunkStatusNormal,
	}

	ioQos, _ := qos.NewQosManager(qos.Config{})
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, readScheduler, writeScheduler, func(option *core.Option) {
		option.Conf = conf
		option.DB = kvdb
		option.CreateDataIfMiss = true
		option.IoQos = ioQos
	})
	require.NoError(t, err)
	require.NotNil(t, cs)

	// build shard data
	shardData := []byte("test")

	bid := proto.BlobID(1024)

	// normal write
	shard := &core.Shard{
		Bid:  bid,
		Vuid: vuid,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(shardData)),
		Body: bytes.NewReader(shardData),
	}

	// write data
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	// read data and check
	rs, err := cs.NewReader(ctx, bid)
	require.NoError(t, err)
	require.NotNil(t, rs)

	require.Equal(t, shard.Bid, rs.Bid)
	require.Equal(t, shard.Vuid, rs.Vuid)
	require.Equal(t, shard.Flag, rs.Flag)
	require.Equal(t, shard.Size, rs.Size)

	rd, err := ioutil.ReadAll(rs.Body)
	require.NoError(t, err)
	require.Equal(t, shardData, rd)

	// range read
	rs1, err := cs.NewRangeReader(ctx, bid, 1, 3)
	require.NoError(t, err)
	require.NotNil(t, rs1)

	require.Equal(t, shard.Bid, rs1.Bid)
	require.Equal(t, shard.Vuid, rs1.Vuid)
	require.Equal(t, shard.Flag, rs1.Flag)
	require.Equal(t, shard.Size, rs1.Size)

	rd, err = ioutil.ReadAll(rs1.Body)
	require.NoError(t, err)
	require.Equal(t, shardData[1:3], rd)

	// read meta
	me, err := cs.ReadShardMeta(ctx, bid)
	require.NoError(t, err)
	require.NotNil(t, me)

	require.Equal(t, true, me.Inline)
	require.Equal(t, shard.Size, me.Size)

	// calc crc32
	expectedCrc := crc32.ChecksumIEEE(shardData)
	require.Equal(t, expectedCrc, shard.Crc)
	log.Info(expectedCrc)

	from, to := int64(0), int64(1)
	_, err = cs.NewRangeReader(ctx, shard.Bid, from, to)
	require.NoError(t, err)

	cs.compacting = true
	shard.Body = bytes.NewReader(shardData)
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	cs = nil
	runtime.GC()
	time.Sleep(1 * time.Second)
}

func TestChunkStorage_DeleteOp(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkStorageDelete")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
			BlockBufferSize:       64 * 1024,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := bnapi.NewChunkId(vuid)

	err = core.EnsureDiskArea(testDir, "")
	require.NoError(t, err)

	datapath := core.GetDataPath(testDir)
	log.Info(datapath)
	metapath := core.GetMetaPath(testDir, "")
	log.Info(metapath)

	kvdb, err := db.NewMetaHandler(metapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)

	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  12,
		ChunkId: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  bnapi.ChunkStatusNormal,
	}

	ioQos, _ := qos.NewQosManager(qos.Config{})
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, readScheduler, writeScheduler, func(option *core.Option) {
		option.Conf = conf
		option.DB = kvdb
		option.CreateDataIfMiss = true
		option.IoQos = ioQos
	})
	require.NoError(t, err)
	require.NotNil(t, cs)

	// normal write
	shardData := []byte("test data")
	shardSize := len(shardData)
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, shardSize, n)

	bid := proto.BlobID(1024)

	shard := &core.Shard{
		Bid:  bid,
		Vuid: vuid,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(shardSize),
		Body: bytes.NewReader(shardData),
	}

	// write data
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	// delete failed
	err = cs.Delete(ctx, 1025)
	require.Error(t, err)

	// delete failed
	err = cs.Delete(ctx, bid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mark delete")

	// compacting will failed
	cs.compacting = true
	err = cs.MarkDelete(ctx, bid)
	require.Error(t, err)
	cs.compacting = false

	// first mark delete
	err = cs.MarkDelete(ctx, bid)
	require.NoError(t, err)

	// read an verify
	sm, err := cs.ReadShardMeta(ctx, bid)
	require.NoError(t, err)
	require.Equal(t, int(bnapi.ShardStatusMarkDelete), int(sm.Flag))

	// compacting will failed
	cs.compacting = true
	err = cs.Delete(ctx, bid)
	require.Error(t, err)
	cs.compacting = false

	// delete
	err = cs.Delete(ctx, bid)
	require.NoError(t, err)

	// read. will not exist
	_, err = cs.ReadShardMeta(ctx, bid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not exist")

	cs = nil
	runtime.GC()
	time.Sleep(1 * time.Second)
}

func TestChunkStorage_Finalizer(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"Finalizer")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := bnapi.NewChunkId(vuid)

	err = core.EnsureDiskArea(testDir, "")
	require.NoError(t, err)

	datapath := core.GetDataPath(testDir)
	log.Info(datapath)
	metapath := core.GetMetaPath(testDir, "")
	log.Info(metapath)

	metadb, err := db.NewMetaHandler(metapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, metadb)

	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  12,
		ChunkId: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  bnapi.ChunkStatusNormal,
	}
	ioQos, _ := qos.NewQosManager(qos.Config{})
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, readScheduler, writeScheduler, func(option *core.Option) {
		option.Conf = conf
		option.DB = metadb
		option.CreateDataIfMiss = true
		option.IoQos = ioQos
	})
	require.NoError(t, err)
	require.NotNil(t, cs)

	var cnt int32
	done := make(chan struct{})

	cs.onClosed = func() {
		atomic.AddInt32(&cnt, 1)
		close(done)
	}

	require.Equal(t, int32(0), cnt)

	cs = nil
	runtime.GC()

	select {
	case <-done:
		span.Infof("success gc")
	case <-time.After(10 * time.Second):
		t.Fail()
	}

	require.Equal(t, int32(1), atomic.LoadInt32(&cnt))
}
