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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/iopool"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	defaultDiskTestDir = "NodeDiskTestDir"
)

func TestNewChunkData(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"NewChunkData")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	conf := &core.Config{}
	chunkid := bnapi.NewChunkId(0)
	chunkname := chunkid.String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	ctx := context.Background()
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()

	_, err = NewChunkData(ctx, core.VuidMeta{}, "", nil, false, nil, readScheduler, writeScheduler)
	require.Error(t, err)

	_, err = NewChunkData(ctx, core.VuidMeta{}, "/tmp/mock/file/path", conf, false, nil, readScheduler, writeScheduler)
	require.Error(t, err)

	// case: format data when first creating chunkdata
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, conf, true, nil, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	_, err = cd.Read(ctx, nil, 1, 1)
	require.Error(t, err)

	_, err = cd.Read(ctx, &core.Shard{}, 1, 1)
	require.Error(t, err)

	err = cd.Delete(ctx, &core.Shard{
		Offset: _chunkHeaderSize - 1,
	})
	require.Error(t, err)

	cdRo, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, conf, true, nil, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cdRo)
	defer cdRo.Close()

	log.Infof("chunkdata: \n%s", cdRo)

	require.Equal(t, cd.header.version, cdRo.header.version)
	require.Equal(t, cd.wOff, cdRo.wOff)
	require.Equal(t, cd.File, cdRo.File)
}

func TestChunkData_Write(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkDataWrite")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := bnapi.NewChunkId(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig: core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{
			BlockBufferSize: 64 * 1024,
		},
	}

	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()

	ioQos, _ := qos.NewQosManager(qos.Config{})
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	require.Equal(t, int32(cd.wOff), int32(4096))

	sharddata := []byte("test data")

	body := bytes.NewBuffer(sharddata)

	// build shard data
	shard := &core.Shard{
		Bid:  1024,
		Vuid: 10,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(sharddata)),
		Body: body,
	}

	// write data
	err = cd.Write(ctx, shard)
	require.NoError(t, err)

	require.Equal(t, int32(shard.Offset), int32(4096))
	require.Equal(t, int32(cd.wOff), int32(8192))

	// read crc
	r, err := cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)
	rd, err := ioutil.ReadAll(r)
	require.NoError(t, err)

	log.Infof("read: %s", string(rd))
	log.Infof("shard:%s", shard)

	require.Equal(t, sharddata, rd)

	expectedOff := core.AlignSize(
		shard.Offset+core.GetShardHeaderSize()+core.GetShardFooterSize()+crc32block.EncodeSize(int64(shard.Size), core.CrcBlockUnitSize),
		_pageSize)

	require.Equal(t, expectedOff, cd.wOff)
}

func TestChunkData_ConcurrencyWrite(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkDataWriteCon")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := bnapi.NewChunkId(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{BlockBufferSize: 64 * 1024},
	}

	ioQos, _ := qos.NewQosManager(qos.Config{})
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	require.Equal(t, int32(cd.wOff), int32(4096))

	concurrency := 10
	shards := make([]*core.Shard, 0)
	sharddatas := make([][]byte, 0)
	for i := 0; i < concurrency; i++ {
		sharddata := []byte(fmt.Sprintf("test data: %d", i))
		sharddatas = append(sharddatas, sharddata)

		body := bytes.NewBuffer(sharddata)

		shard := &core.Shard{
			Bid:  1024,
			Vuid: 10,
			Flag: bnapi.ShardStatusNormal,
			Size: uint32(len(sharddata)),
			Body: body,
		}
		shards = append(shards, shard)
	}

	require.Equal(t, len(shards), concurrency)

	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(i int, shard *core.Shard) {
			defer wg.Done()
			err := cd.Write(ctx, shard)
			require.NoError(t, err)

			r, err := cd.Read(ctx, shard, 0, shard.Size)
			require.NoError(t, err)
			rd, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			log.Infof("read: %s", string(rd))
			log.Infof("shard:%s", shard)

			require.Equal(t, sharddatas[i], rd)
		}(i, shards[i])
	}
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		log.Infof("shard[%d] offset:%d", i, shards[i].Offset)
		require.True(t, shards[i].Offset%_pageSize == 0)
	}

	log.Infof("chunkdata: \n%s", cd)

	expectedOff := 4096 + 4096*10
	require.Equal(t, int64(expectedOff), int64(cd.wOff))
}

func TestChunkData_Delete(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkDataDelete")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := bnapi.NewChunkId(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{BlockBufferSize: 64 * 1024},
	}
	ioQos, _ := qos.NewQosManager(qos.Config{})
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	require.Equal(t, int32(cd.wOff), int32(4096))

	concurrency := 5
	shards := make([]*core.Shard, 0)
	sharddatas := make([][]byte, 0)
	for i := 0; i < concurrency; i++ {
		// 1M buf
		sharddata := make([]byte, 1*1024*1024)
		sharddata[i] = byte(i)

		sharddatas = append(sharddatas, sharddata)

		body := bytes.NewBuffer(sharddata)

		shard := &core.Shard{
			Bid:  1024,
			Vuid: 10,
			Flag: bnapi.ShardStatusNormal,
			Size: uint32(len(sharddata)),
			Body: body,
		}
		shards = append(shards, shard)
	}

	require.Equal(t, len(shards), concurrency)

	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(i int, shard *core.Shard) {
			defer wg.Done()
			err := cd.Write(ctx, shard)
			require.NoError(t, err)

			r, err := cd.Read(ctx, shard, 0, shard.Size)
			require.NoError(t, err)
			rd, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			log.Infof("read: %s", string(rd))
			log.Infof("shard:%s", shard)

			require.Equal(t, sharddatas[i], rd)
		}(i, shards[i])
	}
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		log.Infof("shard[%d] offset:%d", i, shards[i].Offset)
		require.True(t, shards[i].Offset%_pageSize == 0)
	}

	log.Infof("chunkdata: \n%s", cd)

	for i := 0; i < concurrency; i++ {
		err = cd.Delete(ctx, shards[i])
		require.NoError(t, err)
	}

	stat, err := cd.ef.SysStat()
	require.NoError(t, err)
	log.Infof("stat: %v", stat)
	log.Infof("blksize: %d", stat.Blocks)

	require.Equal(t, true, int(stat.Blocks) >= 8)
	require.Equal(t, true, int(stat.Blocks) < (1+len(shards))*8)

	shardData := []byte("test")
	// normal write
	shard := &core.Shard{
		Bid:  proto.BlobID(2),
		Vuid: proto.Vuid(11),
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(shardData)),
		Body: bytes.NewReader(shardData),
	}

	// write data, offset:5267456
	err = cd.Write(ctx, shard)
	require.NoError(t, err)

	f, err := os.OpenFile(chunkname, 2, 0o644)
	require.NoError(t, err)
	defer f.Close()

	shard.Size = uint32(len(shardData) + 1)
	err = cd.Delete(ctx, shard)
	require.Error(t, err)

	// bad shard magic
	badMagic := []byte{0xaa, 0xaa, 0xaa, 0xaa}
	_, err = f.WriteAt(badMagic, 5267460)
	require.NoError(t, err)

	err = cd.Delete(ctx, shard)
	require.Error(t, err)
}

func TestChunkData_Destroy(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkDataDestroy")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := bnapi.NewChunkId(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{},
	}
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	cd, err := NewChunkData(context.TODO(), core.VuidMeta{}, chunkname, diskConfig, true, nil, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	err = cd.Destroy(ctx)
	require.NoError(t, err)
}

func TestFlush(t *testing.T) {
	cd := &datafile{conf: &core.Config{
		BaseConfig: core.BaseConfig{
			DisableSync: true,
		},
	}}
	err := cd.Flush()
	require.Nil(t, err)
}

func TestChunkData_Close(t *testing.T) {
	cd := &datafile{conf: &core.Config{
		BaseConfig: core.BaseConfig{
			DisableSync: true,
		},
	}}

	cd.Close()
}

func TestParseMeta(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ParseMeta")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := bnapi.NewChunkId(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{},
	}

	ctime := time.Now().UnixNano()
	meta := core.VuidMeta{
		Version:     0x1,
		ParentChunk: bnapi.ChunkId{0x8},
		Ctime:       ctime,
	}
	readScheduler := iopool.NewSharedIoScheduler(core.DefaultReadThreadCnt, core.DefaultReadQueueDepth)
	defer readScheduler.Close()
	writeScheduler := iopool.NewPartitionIoScheduler(core.DefaultWriteThreadCnt, core.DefaultWriteQueueDepth)
	defer writeScheduler.Close()
	// scene 1
	cd, err := NewChunkData(ctx, meta, chunkname, diskConfig, true, nil, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	cd1, err := NewChunkData(ctx, meta, chunkname, diskConfig, false, nil, readScheduler, writeScheduler)
	require.NoError(t, err)
	require.NotNil(t, cd1)
	defer cd1.Close()

	require.Equal(t, cd.header, cd1.header)
	require.Equal(t, cd1.header.magic, chunkHeaderMagic)
	require.Equal(t, cd1.header.version, uint8(0x1))
	require.Equal(t, cd1.header.parentChunk, bnapi.ChunkId{0x8})
	require.Equal(t, cd1.header.createTime, ctime)

	// scene 2
	f, err := os.OpenFile(chunkname, 2, 0o644)
	require.NoError(t, err)
	defer f.Close()
	buffer := make([]byte, _chunkHeaderSize)
	n, err := f.ReadAt(buffer, 0)
	require.NoError(t, err)
	require.Equal(t, n, _chunkHeaderSize)

	hdr := ChunkHeader{}
	err = hdr.Unmarshal(buffer)
	require.NoError(t, err)
	require.Equal(t, cd.header, hdr)

	// bad magic
	badMagic := []byte{0x20, 0x21, 0x03, 0x19}
	_, err = f.WriteAt(badMagic, 0)
	require.NoError(t, err)
	err = cd.parseMeta()
	require.Error(t, err)
}

func TestChunkHeader(t *testing.T) {
	magic := chunkHeaderMagic
	version := byte(0x2)
	parent := bnapi.ChunkId{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x0}
	createTime := time.Now().UnixNano()

	hdr := ChunkHeader{
		magic:       magic,
		version:     version,
		parentChunk: parent,
		createTime:  createTime,
	}

	buffer, err := hdr.Marshal()
	require.NoError(t, err)
	require.NotNil(t, buffer)

	hdr1 := ChunkHeader{}
	err = hdr1.Unmarshal(buffer)
	require.NoError(t, err)
	require.Equal(t, hdr, hdr1)

	chunkHeader := ChunkHeader{}
	s := chunkHeader.String()
	require.NotNil(t, s)
}
