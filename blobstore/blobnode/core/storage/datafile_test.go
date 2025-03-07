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
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

const (
	defaultDiskTestDir = "NodeDiskTestDir"
)

func newIoPoolMock(t *testing.T) taskpool.IoPool {
	ctr := gomock.NewController(t)
	ioPool := mocks.NewMockIoPool(ctr)
	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args taskpool.IoPoolTaskArgs) { args.TaskFn() }).AnyTimes()

	return ioPool
}

func TestNewChunkData(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"NewChunkData")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	conf := &core.Config{}
	chunkid := bnapi.NewChunkId(0)
	chunkname := chunkid.String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	ctx := context.Background()

	_, err = NewChunkData(ctx, core.VuidMeta{}, "", nil, false, nil, nil, nil)
	require.Error(t, err)

	_, err = NewChunkData(ctx, core.VuidMeta{}, "/tmp/mock/file/path", conf, false, nil, nil, nil)
	require.Error(t, err)

	ioPool := newIoPoolMock(t)
	// case: format data when first creating chunkdata
	cd, err := NewChunkData(ctx, core.VuidMeta{ChunkId: chunkid, DiskID: 1, Version: 2, Ctime: 3}, chunkname, conf, true, nil, ioPool, ioPool)
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

	cdRo, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, conf, true, nil, ioPool, ioPool)
	require.NoError(t, err)
	require.NotNil(t, cdRo)
	defer cdRo.Close()

	log.Infof("chunkdata: \n%s", cdRo)

	require.Equal(t, cd.header.version, cdRo.header.version)
	require.Equal(t, cd.wOff, cdRo.wOff)
	require.Equal(t, cd.File, cdRo.File)
}

func TestChunkData_Write(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataWrite")
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

	ioPool := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPool, ioPool)
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

	// write data, size 9
	err = cd.Write(ctx, shard)
	require.NoError(t, err)

	require.Equal(t, int32(shard.Offset), int32(4096))
	require.Equal(t, int32(cd.wOff), int32(8192))

	crcNum := crc32.ChecksumIEEE(sharddata)
	require.Equal(t, uint32(3540561586), crcNum)

	buf := bytespool.Alloc(core.HeaderSize)
	defer bytespool.Free(buf) // nolint: staticcheck
	_, err = cd.ef.ReadAt(buf, shard.Offset)
	require.NoError(t, err)

	shard2 := core.Shard{}
	err = shard2.ParseHeader(buf)
	require.NoError(t, err)
	require.Equal(t, shard.Bid, shard2.Bid)
	require.Equal(t, shard.Vuid, shard2.Vuid)
	require.Equal(t, shard.Size, shard2.Size)

	// read data
	r, err := cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	dst := make([]byte, shard.Size)
	n, err := io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, shard.Size, uint32(n))

	log.Infof("read: %s", string(dst))
	log.Infof("shard:%s", shard)

	require.Equal(t, sharddata, dst)
	require.Equal(t, uint32(3540561586), shard.Crc) // d3 08 ae b2, 3540561586

	// range read
	r, err = cd.Read(ctx, shard, 1, 2)
	require.NoError(t, err)

	dst = make([]byte, 2-1)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, byte('e'), dst[0])

	expectedOff := core.AlignSize(
		shard.Offset+core.GetShardHeaderSize()+core.GetShardFooterSize()+crc32block.EncodeSize(int64(shard.Size), core.CrcBlockUnitSize),
		_pageSize)

	require.Equal(t, expectedOff, cd.wOff)

	// write 32KB
	cd.wOff = 65536
	shard.Bid++
	data2 := make([]byte, 32*1024)
	for i := range data2 {
		data2[i] = '0' + byte(i%10)
	}
	shard.Body = bytes.NewBuffer(data2)
	shard.Size = uint32(len(data2))
	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int32(shard.Offset), int32(65536))
	require.Equal(t, int32(cd.wOff), int32(65536+4096+32768))

	r, err = cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	dst = make([]byte, shard.Size)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(shard.Size), n)
	require.Equal(t, data2, dst)
	require.Equal(t, uint32(629387998), shard.Crc) // de b2 83 25 crc 629387998

	// write header+crc+data+footer in 64KB
	shard.Bid++
	data3 := make([]byte, 64*1024-32-4-8)
	data3[0] = byte('1')
	data3[64*1024-1-32-4-8] = byte('2')
	shard.Body = bytes.NewBuffer(data3)
	shard.Size = uint32(len(data3))

	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int32(65536+4096+32768), int32(shard.Offset))
	require.Equal(t, int32(65536+4096+32768+64*1024), int32(cd.wOff))

	r, err = cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	// n, err = io.ReadFull(r, dst)
	// copyN, err = io.CopyN(bytes.NewBuffer(dst[:0]), r, int64(shard.Size+core.CrcSize))
	dst = make([]byte, shard.Size)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(shard.Size), n)
	require.Equal(t, data3[0], dst[0])
	require.Equal(t, len(data3), len(dst))
	require.Equal(t, data3[len(data3)-1], dst[len(data3)-1]) // runtime error: index out of range [65491] with length 65440
	require.Equal(t, uint32(1855488240), shard.Crc)          // 6e 98 80 f0  crc 1855488240

	// write, only footer in next block
	shard.Bid++
	data4 := make([]byte, 64*1024-4)
	data4[0] = byte('1')
	data4[64*1024-1-4] = byte('2')
	shard.Body = bytes.NewBuffer(data4)
	shard.Size = uint32(len(data4))

	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int32(65536+4096+32768+65536), int32(shard.Offset))
	require.Equal(t, int32(65536+4096+32768+65536+64*1024+4096), int32(cd.wOff))

	r, err = cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	dst = make([]byte, shard.Size)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(shard.Size), n)
	require.Equal(t, data4[0], dst[0])
	require.Equal(t, data4[len(data4)-1], dst[len(data4)-1])
	require.Equal(t, uint32(3135759619), shard.Crc) // ba e7 e5 03  crc 3135759619

	// write a little data, and footer in next block. 2 block
	shard.Bid++
	data5 := make([]byte, 64*1024)
	data5[0] = byte('1')
	data5[64*1024-1] = byte('2')
	shard.Body = bytes.NewBuffer(data5)
	shard.Size = uint32(len(data5))

	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int32(4096*2+32768+65536*3), int32(shard.Offset))
	require.Equal(t, int32(4096*2+32768+65536*3+65536+4096), int32(cd.wOff))

	r, err = cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	dst = make([]byte, shard.Size)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(shard.Size), n)
	require.Equal(t, data5[0], dst[0])
	require.Equal(t, data5[len(data5)-1], dst[len(data5)-1])
	require.Equal(t, uint32(3043354470), shard.Crc) // B5 65 E7 66  crc 3043354470

	// write, some footer in next block
	shard.Bid++
	data6 := make([]byte, 64*1024-6)
	data6[0] = byte('1')
	data6[64*1024-1-6] = byte('2')
	shard.Body = bytes.NewBuffer(data6)
	shard.Size = uint32(len(data6))

	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int32(4096*3+32768+65536*4), int32(shard.Offset))
	require.Equal(t, int32(4096*3+32768+65536*4+65536+4096), int32(cd.wOff))

	r, err = cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	dst = make([]byte, shard.Size)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(shard.Size), n)
	require.Equal(t, data6[0], dst[0])
	require.Equal(t, data6[len(data6)-1], dst[len(data6)-1])
	require.Equal(t, uint32(2785608964), shard.Crc) // a6 09 05 04  crc 2785608964

	// write 1MB, 17 data block
	shard.Bid++
	data7 := make([]byte, 1*1024*1024)
	data7[0] = byte('1')
	data7[64*1024-4-1] = byte('2')
	data7[64*1024-4] = byte('3')
	data7[64*2*1024-4*2-1] = byte('4')
	data7[64*2*1024-4*2] = byte('5')
	data7[64*3*1024-4*3-1] = byte('6')
	data7[1*1024*1024-1] = byte('0')
	shard.Body = bytes.NewBuffer(data7)
	shard.Size = uint32(len(data7))

	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int64(4096*4+32768+65536*5), shard.Offset)
	require.Equal(t, int64(4096*4+32768+65536*5+1024*1024+4096), cd.wOff)

	r, err = cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	dst = make([]byte, shard.Size)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(shard.Size), n)
	require.Equal(t, data7[0], dst[0])
	require.Equal(t, data7[64*1024-4-1], dst[64*1024-4-1])
	require.Equal(t, data7[64*1024-4], dst[64*1024-4])
	require.Equal(t, data7[64*2*1024-4-1-4], dst[64*2*1024-4-1-4])
	require.Equal(t, data7[len(data7)-1], dst[len(data7)-1])
	require.Equal(t, uint32(3977273324), shard.Crc) // ed 10 5f ec  crc 3977273324

	// range read
	from, to := 64*2*1024-4*2-1, 64*3*1024-4*3 // '4', '6'
	r, err = cd.Read(ctx, shard, uint32(from), uint32(to))
	require.NoError(t, err)

	dst = make([]byte, to-from)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(to-from), n)
	require.Equal(t, byte('4'), dst[0])
	require.Equal(t, byte('6'), dst[len(dst)-1])

	// range read
	from, to = 64*1024-4, 1*1024*1024 // '3', '0'
	r, err = cd.Read(ctx, shard, uint32(from), uint32(to))
	require.NoError(t, err)

	dst = make([]byte, to-from)
	n, err = io.ReadFull(r, dst)
	require.NoError(t, err)
	require.Equal(t, int(to-from), n)
	require.Equal(t, byte('3'), dst[0])
	require.Equal(t, byte('0'), dst[len(dst)-1])

	// write error, because read unexpect
	shard.Bid = 1
	data := []byte("test_error")
	shard.Body = bytes.NewBuffer(data)
	shard.Size = uint32(len(data) + 1)
	err = cd.Write(ctx, shard)
	require.NotNil(t, err)
}

func TestChunkData_ConcurrencyWrite(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataWriteCon")
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

	concurrency := 10
	ioPool := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: int32(concurrency), WriteQueueDepth: int32(concurrency), WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPool, ioPool)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	require.Equal(t, int32(cd.wOff), int32(4096))

	shards := make([]*core.Shard, 0)
	sharddatas := make([][]byte, 0)
	for i := 0; i < concurrency; i++ {
		sharddata := []byte(fmt.Sprintf("test data: %d", i))
		sharddatas = append(sharddatas, sharddata)

		body := bytes.NewBuffer(sharddata)

		shard := &core.Shard{
			Bid:  proto.BlobID(1024 + i),
			Vuid: 10,
			Flag: bnapi.ShardStatusNormal,
			Size: uint32(len(sharddata)),
			Body: body,
		}
		shards = append(shards, shard)
	}

	require.Equal(t, len(shards), concurrency)

	retCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int, shard *core.Shard) {
			var err error
			defer func() {
				retCh <- err
			}()

			err = cd.Write(ctx, shard)
			require.NoError(t, err)

			r, err := cd.Read(ctx, shard, 0, shard.Size)
			require.NoError(t, err)
			// dst, err = io.ReadAll(r)
			dst := make([]byte, shard.Size)
			n, err := io.ReadFull(r, dst)
			require.NoError(t, err)
			require.Equal(t, int(shard.Size), n)

			log.Infof("read: %s", string(dst[:]))
			log.Infof("shard:%s", shard)

			require.Equal(t, sharddatas[i], dst[:])
		}(i, shards[i])
	}

	for i := 0; i < concurrency; i++ {
		log.Infof("shard[%d] offset:%d", i, shards[i].Offset)
		require.True(t, shards[i].Offset%_pageSize == 0)
		err := <-retCh
		require.NoError(t, err)
	}

	log.Infof("chunkdata: \n%s", cd)

	expectedOff := 4096 + 4096*10
	require.Equal(t, int64(expectedOff), int64(cd.wOff))
}

func TestChunkData_Delete(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataDelete")
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
	ioPool := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 100, WriteQueueDepth: 100, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPool, ioPool)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	require.Equal(t, int32(cd.wOff), int32(4096))

	// normal write
	shardData := []byte("test")
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

	// concurrency write, delete ok
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
			Bid:  proto.BlobID(1024 + i),
			Vuid: 10,
			Flag: bnapi.ShardStatusNormal,
			Size: uint32(len(sharddata)),
			Body: body,
		}
		shards = append(shards, shard)
	}

	require.Equal(t, len(shards), concurrency)

	retCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int, shard *core.Shard) {
			var err error
			defer func() {
				retCh <- err
			}()
			err = cd.Write(ctx, shard)
			require.NoError(t, err)

			r, err := cd.Read(ctx, shard, 0, shard.Size)
			require.NoError(t, err)

			dst := make([]byte, shard.Size)
			n, err := io.ReadFull(r, dst)
			require.NoError(t, err)
			require.Equal(t, int(shard.Size), n)

			log.Infof("read: %s", string(dst))
			log.Infof("shard:%s", shard)

			require.Equal(t, len(sharddatas[i]), len(dst))
			require.Equal(t, sharddatas[i], dst)
		}(i, shards[i])
	}

	for i := 0; i < concurrency; i++ {
		log.Infof("shard[%d] offset:%d", i, shards[i].Offset)
		require.True(t, shards[i].Offset%_pageSize == 0)
		err := <-retCh
		require.NoError(t, err)
	}

	log.Infof("chunkdata: \n%s", cd)
	statBefore, err := cd.ef.SysStat()
	require.NoError(t, err)

	for i := 0; i < concurrency; i++ {
		err = cd.Delete(ctx, shards[i])
		require.NoError(t, err)
	}

	stat, err := cd.ef.SysStat() // after delete
	require.NoError(t, err)
	log.Infof("stat: %v", stat)
	log.Infof("blksize: %d", stat.Blocks)

	require.Equal(t, true, int(stat.Blocks) >= 8)
	require.Less(t, stat.Blocks, statBefore.Blocks)
}

func TestChunkData_Destroy(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataDestroy")
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
	ioPool := newIoPoolMock(t)
	cd, err := NewChunkData(context.TODO(), core.VuidMeta{}, chunkname, diskConfig, true, nil, ioPool, ioPool)
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
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ParseMeta")
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

	ioPool := newIoPoolMock(t)
	// scene 1
	cd, err := NewChunkData(ctx, meta, chunkname, diskConfig, true, nil, ioPool, ioPool)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	cd1, err := NewChunkData(ctx, meta, chunkname, diskConfig, false, nil, ioPool, ioPool)
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
