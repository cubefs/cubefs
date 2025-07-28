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
	"io"
	"math/rand"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

const (
	defaultDiskTestDir = "NodeDiskTestDir"
)

func newIoPoolMock(t *testing.T) map[qos.IOTypeRW]taskpool.IoPool {
	ctr := gomock.NewController(t)
	ioPool := mocks.NewMockIoPool(ctr)
	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args taskpool.IoPoolTaskArgs) { args.TaskFn() }).AnyTimes()
	return map[qos.IOTypeRW]taskpool.IoPool{
		qos.IOTypeRead:  ioPool,
		qos.IOTypeWrite: ioPool,
		qos.IOTypeDel:   ioPool,
	}
}

func TestNewChunkStorage(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"NewChunkStorage")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := clustermgr.NewChunkID(vuid)

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
		ChunkID: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  clustermgr.ChunkStatusNormal,
	}
	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cs, err := NewChunkStorage(context.TODO(), datapath, vm, ioPools, func(option *core.Option) {
		option.Conf = conf
		option.DB = kvdb
		option.CreateDataIfMiss = true
		option.IoQos = ioQos
	})
	require.NoError(t, err)
	require.NotNil(t, cs)

	_ = cs.ChunkInfo(context.TODO())

	_ = cs.VuidMeta()

	closeStatus := cs.IsClosed()
	require.Equal(t, false, closeStatus)

	err = cs.UnmarshalJSON([]byte("!"))
	require.Error(t, err)
}

func TestChunkStorage_ReadWrite(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkStorageRW")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	ctx = bnapi.SetIoType(ctx, bnapi.WriteIO)

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
			BlockBufferSize:       64 * 1024,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := clustermgr.NewChunkID(vuid)

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
		ChunkID: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  clustermgr.ChunkStatusNormal,
	}

	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, ioPools, func(option *core.Option) {
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

	rd, err := io.ReadAll(rs.Body)
	require.NoError(t, err)
	require.Equal(t, shardData, rd[:])

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

	from, to := int64(1), int64(2)
	rs, err = cs.NewRangeReader(ctx, shard.Bid, from, to)
	require.NoError(t, err)
	require.Equal(t, shard.Bid, rs.Bid)
	require.Equal(t, shard.Size, rs.Size)

	rd, err = io.ReadAll(rs.Body)
	require.NoError(t, err)
	require.Equal(t, int(to-from), len(rd))
	require.Equal(t, shardData[from:to], rd[:])

	// read
	shard2 := *shard
	wb := bytes.NewBuffer([]byte{})
	shard2.Writer = wb
	rn, err := cs.Read(ctx, &shard2)
	require.NoError(t, err)
	require.Equal(t, shard.Size, uint32(rn))
	require.Equal(t, shardData, wb.Bytes())

	cs.compacting = true
	shard.Body = bytes.NewReader(shardData)
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	// 128B, 32KB, 64KB-44, 64KB-4, 64KB, 128KB, 1MB, random
	allInOneBlock := 64*1024 - core.CrcSize - core.HeaderSize - core.FooterSize
	arr := []int{128, 32 * 1024, allInOneBlock, 64*1024 - 4, 64 * 1024, 128 * 1024, 1 * 1024 * 1024, rand.Intn(1*1024*1024) + 1}
	for _, size := range arr {
		shardData = make([]byte, size)
		for i := 0; i < size; i++ {
			shardData[i] = '0' + byte(i%10)
		}

		bid++
		shard = &core.Shard{
			Bid:  bid,
			Vuid: vuid,
			Flag: bnapi.ShardStatusNormal,
			Size: uint32(len(shardData)),
			Body: bytes.NewReader(shardData),
		}

		err = cs.Write(ctx, shard)
		require.NoError(t, err)

		// read all
		rs, err = cs.NewReader(ctx, bid)
		require.NoError(t, err)
		require.Equal(t, shard.Bid, rs.Bid)
		require.Equal(t, shard.Vuid, rs.Vuid)
		require.Equal(t, shard.Flag, rs.Flag)
		require.Equal(t, shard.Size, rs.Size)

		rd, err = io.ReadAll(rs.Body)
		require.NoError(t, err)
		require.Equal(t, int(shard.Size), len(rd))
		require.Equal(t, shardData, rd)

		// read
		shard3 := *shard
		wb = bytes.NewBuffer([]byte{})
		shard3.Writer = wb
		rn, err = cs.Read(ctx, &shard3)
		require.NoError(t, err)
		require.Equal(t, shard.Size, uint32(rn))
		require.Equal(t, shardData, wb.Bytes())

		// range read
		from, to = int64(1), int64(size-2)
		rs, err = cs.NewRangeReader(ctx, shard.Bid, from, to)
		require.NoError(t, err)
		require.Equal(t, shard.Bid, rs.Bid)
		require.Equal(t, shard.Size, rs.Size)

		rd, err = io.ReadAll(rs.Body)
		require.NoError(t, err)
		require.Equal(t, int(to-from), len(rd))
		require.Equal(t, shardData[from:to], rd)
	}
}

func TestChunkStorage_ReadWriteInline(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkStorageRWInline")
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
	chunkid := clustermgr.NewChunkID(vuid)

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
		ChunkID: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  clustermgr.ChunkStatusNormal,
	}

	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, ioPools, func(option *core.Option) {
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

	rd, err := io.ReadAll(rs.Body)
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

	rd, err = io.ReadAll(rs1.Body)
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
}

func TestChunkStorage_DeleteOp(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkStorageDelete")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	ctx = bnapi.SetIoType(ctx, bnapi.WriteIO)

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
			BlockBufferSize:       64 * 1024,
		},
	}

	vuid := proto.Vuid(1)
	chunkid := clustermgr.NewChunkID(vuid)

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
		ChunkID: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  clustermgr.ChunkStatusNormal,
	}

	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, ioPools, func(option *core.Option) {
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
	ctx = bnapi.SetIoType(ctx, bnapi.DeleteIO)
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
}

func TestChunkStorage_Finalizer(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"Finalizer")
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
	chunkid := clustermgr.NewChunkID(vuid)

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
		ChunkID: chunkid,
		Mtime:   time.Now().UnixNano(),
		Status:  clustermgr.ChunkStatusNormal,
	}
	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cs, err := NewChunkStorage(ctx, datapath, vm, ioPools, func(option *core.Option) {
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
	ioQos.Close()
}
