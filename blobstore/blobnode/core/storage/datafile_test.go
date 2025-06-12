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
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	bnmock "github.com/cubefs/cubefs/blobstore/testing/mockblobnode"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
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

func TestNewChunkData(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"NewChunkData")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	conf := &core.Config{}
	chunkid := clustermgr.NewChunkID(0)
	chunkname := chunkid.String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	ctx := context.Background()

	_, err = NewChunkData(ctx, core.VuidMeta{}, "", nil, false, nil, nil)
	require.Error(t, err)

	_, err = NewChunkData(ctx, core.VuidMeta{}, "/tmp/mock/file/path", conf, false, nil, nil)
	require.Error(t, err)

	ioPools := newIoPoolMock(t)
	// case: format data when first creating chunkdata
	cd, err := NewChunkData(ctx, core.VuidMeta{ChunkID: chunkid, DiskID: 1, Version: 2, Ctime: 3}, chunkname, conf, true, nil, ioPools)
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

	cdRo, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, conf, true, nil, ioPools)
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

	chunkname := clustermgr.NewChunkID(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig: core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{
			BlockBufferSize: 64 * 1024,
		},
	}

	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPools)
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
	require.ErrorIs(t, err, bloberr.ErrReaderError)
}

func TestChunkData_ConcurrencyWrite(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataWriteCon")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := clustermgr.NewChunkID(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{BlockBufferSize: 64 * 1024},
	}

	concurrency := 10
	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: int32(concurrency), WriteQueueDepth: int32(concurrency), WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPools)
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
			Vuid: 11,
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

	time.Sleep(time.Millisecond * 100)
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

func TestChunkData_ConcurrencyWriteRead(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataWriteReadCon")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := clustermgr.NewChunkID(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{BlockBufferSize: 64 * 1024},
	}

	concurrency := 20
	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: int32(concurrency), WriteQueueDepth: int32(concurrency), WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPools)
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
			Vuid: 12,
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
		}(i, shards[i])
	}

	for i := 0; i < concurrency; i++ {
		log.Infof("shard[%d] offset:%d", i, shards[i].Offset)
		require.True(t, shards[i].Offset%_pageSize == 0)
		err := <-retCh
		require.NoError(t, err)
	}

	// read , write
	var wg sync.WaitGroup
	wg.Add(concurrency * 2)
	for i := 0; i < concurrency; i++ {
		// read
		go func(i int, shard *core.Shard) {
			defer wg.Done()

			r, err := cd.Read(ctx, shard, 0, shard.Size)
			require.NoError(t, err)

			// delay read
			time.Sleep(time.Millisecond * 300)
			dst := make([]byte, shard.Size)
			n, err := io.ReadFull(r, dst)

			require.NoError(t, err)
			require.Equal(t, int(shard.Size), n)
			require.Equal(t, sharddatas[i], dst[:])
		}(i, shards[i])

		// write
		go func(i int, shard *core.Shard) {
			defer wg.Done()

			_shard := *shard
			_shard.Bid += proto.BlobID(concurrency)
			_shard.Body = bytes.NewBuffer([]byte(fmt.Sprintf("test data: %d", i+concurrency+1)))

			err = cd.Write(ctx, &_shard)
			require.NoError(t, err)
		}(i, shards[i])
	}
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		log.Infof("shard[%d] offset:%d", i, shards[i].Offset)
		require.True(t, shards[i].Offset%_pageSize == 0)
	}
	log.Infof("chunkdata: \n%s", cd)

	expectedOff := 4096 + 4096*concurrency*2
	require.Equal(t, int64(expectedOff), int64(cd.wOff))
}

func TestChunkData_BatchRead(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataBatchRead")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)
	ctx := context.Background()
	chunkname := clustermgr.NewChunkID(0).String()
	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)
	diskConfig := &core.Config{
		BaseConfig: core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{
			BlockBufferSize:          64 * 1024,
			BatchBufferSize:          1024 * 1024 * 1,
			BatchBufferHoleThreshold: 128 * 1024,
		},
	}

	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPools)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	shardNum := 10

	require.Equal(t, int32(cd.wOff), int32(4096))
	bidInfo := make([]bnapi.BidInfo, shardNum)
	shards := make([]*core.Shard, 0)
	sharddatas := make([][]byte, 0)
	rand.Seed(time.Now().UnixNano())
	// write 10 bids, more than 1MB data, and buffer size set 1MB
	for i := 0; i < shardNum; i++ {
		sharddata := make([]byte, 150*1024)
		for k := range sharddata {
			sharddata[k] = byte(rand.Intn(100)) // 填充0到99的随机数
		}
		sharddata[0] = byte(i + 1)
		body := bytes.NewBuffer(sharddata)
		shard := &core.Shard{
			Bid:  proto.BlobID(1024 + i),
			Vuid: 12,
			Flag: bnapi.ShardStatusNormal,
			Size: uint32(len(sharddata)),
			Body: body,
		}
		// write data
		err = cd.Write(ctx, shard)
		require.NoError(t, err)
		bidInfo[i] = bnapi.BidInfo{
			Bid:    proto.BlobID(1024 + i),
			Size:   int64(uint32(len(sharddata))),
			Offset: shard.Offset,
			Crc:    shard.Crc,
		}
		shards = append(shards, shard)
		sharddatas = append(sharddatas, sharddata)
	}
	batchShard, err := core.NewBatchShardReader(bidInfo, 12, nil, diskConfig.BatchBufferSize)
	require.NoError(t, err)
	rc, err := cd.BatchRead(ctx, batchShard)
	require.NoError(t, err)
	defer rc.Close()
	all := bytes.NewBuffer(nil)
	rc.WriteTo(all)
	if tr, ok := rc.(interface{ Duration() time.Duration }); ok {
		duration := tr.Duration()
		t.Logf("read time: %v", duration)
	}
	var header bnapi.ShardsHeader
	for i := 0; i < shardNum; i++ {
		n, err := io.ReadFull(all, header[:])
		require.NoError(t, err)
		require.Equal(t, n, len(header))
		require.Equal(t, header.Get(), 200)
		dst := make([]byte, shards[i].Size)
		n, err = io.ReadFull(all, dst)
		require.NoError(t, err)
		require.Equal(t, n, int(shards[i].Size))
		require.Equal(t, sharddatas[i], dst)
	}

	// bid8 has deleted
	bidInfos1 := make([]bnapi.BidInfo, 0)
	bidInfos1 = append(bidInfos1, bidInfo[:8]...)
	bidInfos1 = append(bidInfos1, bidInfo[9:]...)

	batchShard, err = core.NewBatchShardReader(bidInfos1, 12, nil, diskConfig.BatchBufferSize)
	require.NoError(t, err)
	rc, err = cd.BatchRead(ctx, batchShard)
	require.NoError(t, err)
	all.Reset()
	rc.WriteTo(all)
	for i := 0; i < shardNum-1; i++ {
		j := i
		if j >= 8 {
			j = i + 1
		}
		n, err := io.ReadFull(all, header[:])
		require.NoError(t, err)
		require.Equal(t, n, len(header))
		require.Equal(t, header.Get(), 200)
		dst := make([]byte, shards[j].Size)
		n, err = io.ReadFull(all, dst)
		require.NoError(t, err)

		require.Equal(t, n, int(shards[j].Size))
		require.Equal(t, sharddatas[j], dst)
	}
	// read code by part
	batchShard, err = core.NewBatchShardReader([]bnapi.BidInfo{bidInfo[0]}, 12, nil, diskConfig.BatchBufferSize)
	require.NoError(t, err)
	rc, err = cd.BatchRead(ctx, batchShard)
	require.NoError(t, err)
	all.Reset()
	rc.WriteTo(all)
	p := make([]byte, 1)
	n, err := io.ReadFull(all, p)
	require.NoError(t, err)
	require.Equal(t, n, 1)
	header[0] = p[0]
	p = make([]byte, 4)
	n, err = io.ReadFull(all, p)
	require.NoError(t, err)
	require.Equal(t, n, 4)
	n = copy(header[1:], p)
	require.Equal(t, n, 3)
	require.Equal(t, byte(1), p[3])
	require.Equal(t, header.Get(), 200)

	// read data by part
	batchShard, err = core.NewBatchShardReader([]bnapi.BidInfo{bidInfo[0]}, 12, nil, diskConfig.BatchBufferSize)
	require.NoError(t, err)
	rc, err = cd.BatchRead(ctx, batchShard)
	require.NoError(t, err)
	all.Reset()
	rc.WriteTo(all)
	n, err = io.ReadFull(all, header[:])
	require.NoError(t, err)
	require.Equal(t, n, 4)
	require.Equal(t, header.Get(), 200)
	need := bidInfo[0].Size
	data := make([]byte, need)
	read := int64(0)
	for need > 0 {
		toread := rand.Int63n(64 * 1024)
		if toread > need {
			toread = need
		}
		n, err = io.ReadFull(all, data[read:read+toread])
		require.NoError(t, err)
		require.Equal(t, n, int(toread))
		need -= int64(n)
		read += int64(n)
	}
	require.Equal(t, sharddatas[0], data)

	// bid not match
	errBid := bidInfo[0]
	errBid.Bid += 1
	batchShard, err = core.NewBatchShardReader([]bnapi.BidInfo{errBid}, 12, nil, diskConfig.BatchBufferSize)
	require.NoError(t, err)
	rc, err = cd.BatchRead(ctx, batchShard)
	require.NoError(t, err)
	all.Reset()
	_, err = rc.WriteTo(all)
	require.Error(t, err)
	_, err = io.ReadFull(all, header[:])
	require.NoError(t, err)
	require.Equal(t, header.Get(), bloberr.CodeBidNotMatch)

	// continue read should return EOF
	n, err = io.ReadFull(all, data)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)
}

func TestChunkData_ReadWrite(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := clustermgr.NewChunkID(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig: core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{
			BlockBufferSize: 64 * 1024,
		},
	}

	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPools)
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

	// mock compact
	rc, err := cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)
	shard = &core.Shard{
		Bid:  proto.BlobID(1024 + 1),
		Vuid: 11,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(sharddata)),
		Body: rc,
	}

	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int32(shard.Offset), int32(8192))
	require.Equal(t, int32(cd.wOff), int32(12288))

	// check data ok
	rc, err = cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)
	dst := make([]byte, shard.Size)
	n, err := io.ReadFull(rc, dst)
	require.NoError(t, err)
	require.Equal(t, int(shard.Size), n)
	require.Equal(t, sharddata, dst)
}

func TestChunkData_Delete(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"ChunkDataDelete")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	chunkname := clustermgr.NewChunkID(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{BlockBufferSize: 64 * 1024, EnableDeleteShardVerify: true},
	}
	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 100, WriteQueueDepth: 100, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPools)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	log.Infof("chunkdata: \n%s", cd)

	require.Equal(t, int32(cd.wOff), int32(4096))

	// normal write
	shardData := []byte("test")
	shard := &core.Shard{
		Bid:  proto.BlobID(2),
		Vuid: proto.Vuid(13),
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
			Vuid: 14,
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

	chunkname := clustermgr.NewChunkID(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{},
	}
	ioPools := newIoPoolMock(t)
	cd, err := NewChunkData(context.TODO(), core.VuidMeta{}, chunkname, diskConfig, true, nil, ioPools)
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

	chunkname := clustermgr.NewChunkID(0).String()

	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig:    core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{},
	}

	ctime := time.Now().UnixNano()
	meta := core.VuidMeta{
		Version:     0x1,
		ParentChunk: clustermgr.ChunkID{0x8},
		Ctime:       ctime,
	}

	ioPools := newIoPoolMock(t)
	// scene 1
	cd, err := NewChunkData(ctx, meta, chunkname, diskConfig, true, nil, ioPools)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	cd1, err := NewChunkData(ctx, meta, chunkname, diskConfig, false, nil, ioPools)
	require.NoError(t, err)
	require.NotNil(t, cd1)
	defer cd1.Close()

	require.Equal(t, cd.header, cd1.header)
	require.Equal(t, cd1.header.magic, chunkHeaderMagic)
	require.Equal(t, cd1.header.version, uint8(0x1))
	require.Equal(t, cd1.header.parentChunk, clustermgr.ChunkID{0x8})
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
	parent := clustermgr.ChunkID{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x0}
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

func TestChunkData_WriteReadCancel(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), defaultDiskTestDir+"WriteCancel")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()
	chunkname := clustermgr.NewChunkID(0).String()
	chunkname = filepath.Join(testDir, chunkname)
	log.Info(chunkname)

	diskConfig := &core.Config{
		BaseConfig: core.BaseConfig{Path: testDir},
		RuntimeConfig: core.RuntimeConfig{
			BlockBufferSize: 64 * 1024,
		},
	}

	ioPools := newIoPoolMock(t)
	ioQos, _ := qos.NewIoQueueQos(qos.Config{ReadQueueDepth: 2, WriteQueueDepth: 2, WriteChanQueCnt: 2})
	defer ioQos.Close()
	cd, err := NewChunkData(ctx, core.VuidMeta{}, chunkname, diskConfig, true, ioQos, ioPools)
	require.NoError(t, err)
	require.NotNil(t, cd)
	defer cd.Close()

	// mock
	backup := cd.ef
	ctr := gomock.NewController(t)
	cd.ef = bnmock.NewMockBlobFile(ctr)
	a := gomock.Any()

	log.Infof("chunkdata: \n%s", cd)
	require.Equal(t, int32(cd.wOff), int32(4096))
	sharddata := []byte("test data")

	// build shard data
	shard := &core.Shard{
		Bid:  5,
		Vuid: 10,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(sharddata)),
		Body: bytes.NewBuffer(sharddata),
	}

	// write ok, size 9.
	cd.ef.(*bnmock.MockBlobFile).EXPECT().WriteAtCtx(a, a, a).DoAndReturn(func(ctx context.Context, b []byte, off int64) (n int, err error) {
		return len(b), nil
	})
	err = cd.Write(ctx, shard)
	require.NoError(t, err)
	require.Equal(t, int32(shard.Offset), int32(4096))
	require.Equal(t, int32(cd.wOff), int32(8192))

	// fail, ctx cancel, before enqueue
	ctx, cancel := context.WithCancel(context.Background())
	shard2 := &core.Shard{
		Bid:  6,
		Vuid: 10,
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(sharddata)),
		Body: bytes.NewBuffer(sharddata),
	}

	cancel()
	cd.ef.(*bnmock.MockBlobFile).EXPECT().WriteAtCtx(a, a, a).DoAndReturn(func(ctx context.Context, b []byte, off int64) (n int, err error) {
		return 0, context.Canceled
	})
	err = cd.Write(ctx, shard2)
	require.NotNil(t, err)

	// fail, ctx cancel, after dequeue
	ctx, cancel = context.WithCancel(context.Background())
	shard2.Body = bytes.NewBuffer(sharddata)

	cd.ef.(*bnmock.MockBlobFile).EXPECT().WriteAtCtx(ctx, a, a).DoAndReturn(func(ctx context.Context, b []byte, off int64) (n int, err error) {
		cancel()

		select {
		case <-ctx.Done():
			n, err = 0, ctx.Err()
			return
		default:
		}
		return len(b), nil
	})
	err = cd.Write(ctx, shard2)
	require.NotNil(t, err)
	require.ErrorIs(t, err, bloberr.ErrIOCtxCancel)

	// read fail, cancel
	ctx, cancel = context.WithCancel(context.Background())
	readBuf := bytes.NewBuffer(nil)
	shard.Writer = readBuf

	rc, err := cd.Read(ctx, shard, 0, shard.Size)
	require.NoError(t, err)

	tw := base.NewTimeWriter(shard.Writer)
	tr := base.NewTimeReader(rc)

	cancel()
	cd.ef.(*bnmock.MockBlobFile).EXPECT().ReadAtCtx(a, a, a).DoAndReturn(func(ctx context.Context, b []byte, off int64) (n int, err error) {
		return 0, context.Canceled
	}).Times(1)
	n, err := io.CopyN(tw, tr, int64(len(sharddata)))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(0), n)

	// resume
	cd.ef = backup
}
