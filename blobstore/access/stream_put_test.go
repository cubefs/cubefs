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

package access

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"hash/crc32"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestAccessStreamPutBase(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamPutBase")
	vuidController.SetBNRealError(true)
	defer func() {
		vuidController.SetBNRealError(false)
	}()

	// 0
	{
		size := 0
		_, err := streamer.Put(ctx(), newReader(size), int64(size), nil)
		require.Error(t, err)
	}
	// 1 byte
	{
		size := 1
		loc, err := streamer.Put(ctx(), newReader(size), int64(size), nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(loc.Blobs))
		require.Equal(t, uint32(1), loc.Blobs[0].Count)
		// time wait the punished services
		time.Sleep(time.Second * time.Duration(punishServiceS))
	}
	// <4M
	{
		size := 1 << 18
		loc, err := streamer.Put(ctx(), newReader(size), int64(size), nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(loc.Blobs))
		require.Equal(t, uint32(1), loc.Blobs[0].Count)
		time.Sleep(time.Second * time.Duration(punishServiceS))
	}
	// 8M + 1k
	{
		size := (1 << 23) + 1024
		loc, err := streamer.Put(ctx(), newReader(size), int64(size), nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(loc.Blobs))
		require.Equal(t, uint32(2), loc.Blobs[1].Count)
		time.Sleep(time.Second * time.Duration(punishServiceS))
	}
	// max size + 1
	{
		size := defaultMaxObjectSize + 1
		_, err := streamer.Put(ctx(), nil, int64(size), nil)
		require.EqualError(t, errcode.ErrAccessExceedSize, err.Error())
	}

	dataShards.clean()
}

func TestAccessStreamPutSum(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamPutSum")
	sumChecker := func(data []byte) {
		dataShards.clean()

		hasherMap := access.HasherMap{
			access.HashAlgDummy:  access.HashAlgDummy.ToHasher(),
			access.HashAlgCRC32:  access.HashAlgCRC32.ToHasher(),
			access.HashAlgMD5:    access.HashAlgMD5.ToHasher(),
			access.HashAlgSHA1:   access.HashAlgSHA1.ToHasher(),
			access.HashAlgSHA256: access.HashAlgSHA256.ToHasher(),
		}
		hashSumMap := make(access.HashSumMap, len(hasherMap))

		_, err := streamer.Put(ctx(), bytes.NewReader(data), int64(len(data)), hasherMap)
		require.NoError(t, err)
		for alg, hasher := range hasherMap {
			hashSumMap[alg] = hasher.Sum(nil)
		}

		bytesMD5 := md5.Sum(data)
		bytesSHA1 := sha1.Sum(data)
		bytesSHA256 := sha256.Sum256(data)

		require.Equal(t, hashSumMap.GetSumVal(access.HashAlgCRC32), crc32.ChecksumIEEE(data))
		require.Equal(t, hashSumMap.GetSumVal(access.HashAlgMD5), sum2Str(bytesMD5[:]))
		require.Equal(t, hashSumMap.GetSumVal(access.HashAlgSHA1), sum2Str(bytesSHA1[:]))
		require.Equal(t, hashSumMap.GetSumVal(access.HashAlgSHA256), sum2Str(bytesSHA256[:]))
	}

	shardsChecher := func(shardSize int, buff []byte, bid proto.BlobID) {
		require.Equal(t, buff[:shardSize], dataShards.get(1001, bid))
		require.Equal(t, buff[shardSize:shardSize*2], dataShards.get(1002, bid))
		require.Equal(t, buff[shardSize*2:shardSize*3], dataShards.get(1003, bid))
		require.Equal(t, buff[shardSize*3:shardSize*4], dataShards.get(1004, bid))
		require.Equal(t, 0, len(dataShards.get(1005, 10000)))
		require.Equal(t, buff[shardSize*5:shardSize*6], dataShards.get(1006, bid))
		require.NotEqual(t, ([]byte)(nil), dataShards.get(1007, bid))
		require.Equal(t, shardSize, len(dataShards.get(1007, bid)))
		require.Equal(t, shardSize, len(dataShards.get(1012, bid)))
	}

	// 1 byte
	{
		data := []byte("x")
		sumChecker(data)

		shardSize := getBufSizes(len(data)).ShardSize
		zeroShard := make([]byte, shardSize)
		shard1 := make([]byte, shardSize)
		copy(shard1, data)

		require.Equal(t, shard1, dataShards.get(1001, 10000))
		require.Equal(t, zeroShard, dataShards.get(1002, 10000))
		require.Equal(t, zeroShard, dataShards.get(1004, 10000))
		require.Equal(t, 0, len(dataShards.get(1005, 10000)))
		require.NotEqual(t, ([]byte)(nil), dataShards.get(1007, 10000))
	}
	// 7 bytes
	{
		data := []byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6}
		sumChecker(data)

		sizes := getBufSizes(len(data))
		buff := make([]byte, sizes.ECDataSize)
		copy(buff, data)
		shardsChecher(sizes.ShardSize, buff, 10000)
	}
	// 4K + 511B, aligned with MinShardSize
	{
		size := (1 << 12) + 511
		data := make([]byte, size)
		rand.Read(data)
		sumChecker(data)

		sizes := getBufSizes(len(data))
		buff := make([]byte, sizes.ECDataSize)
		copy(buff, data)
		shardsChecher(sizes.ShardSize, buff, 10000)
	}
	// 4M
	{
		size := 1 << 22
		data := make([]byte, size)
		rand.Read(data)
		sumChecker(data)

		sizes := getBufSizes(len(data))
		buff := make([]byte, sizes.ECDataSize)
		copy(buff, data)
		shardsChecher(sizes.ShardSize, buff, 10000)
	}
	// 4M + 1B
	{
		size := (1 << 22) + 1
		data := make([]byte, size)
		rand.Read(data)
		sumChecker(data)

		sizes := getBufSizes(blobSize)
		buff := make([]byte, sizes.ECDataSize)
		copy(buff, data[:blobSize])
		shardsChecher(sizes.ShardSize, buff, 10000)

		data = data[blobSize:]
		sizes = getBufSizes(len(data))
		buff = make([]byte, sizes.ECDataSize)
		copy(buff, data)
		shardsChecher(sizes.ShardSize, buff, 20000)
	}
	// 6M + 1K + 7B
	{
		size := (1<<20)*6 + 1024 + 7
		data := make([]byte, size)
		rand.Read(data)
		sumChecker(data)

		sizes := getBufSizes(blobSize)
		buff := make([]byte, sizes.ECDataSize)
		copy(buff, data[:blobSize])
		shardsChecher(sizes.ShardSize, buff, 10000)

		data = data[blobSize:]
		sizes = getBufSizes(len(data))
		buff = make([]byte, sizes.ECDataSize)
		copy(buff, data)
		shardsChecher(sizes.ShardSize, buff, 20000)
	}

	dataShards.clean()
}

func TestAccessStreamPutShardTimeout(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamPutShardTimeout")
	dataShards.clean()
	vuidController.Block(1001)
	vuidController.Unbreak(1005)
	defer func() {
		vuidController.Unblock(1001)
		vuidController.Unblock(1002)
		vuidController.Break(1005)
		dataShards.clean()
	}()

	size := 1 << 22
	buff := make([]byte, size)
	rand.Read(buff)
	startTime := time.Now()
	loc, err := streamer.Put(ctx(), bytes.NewReader(buff), int64(size), nil)
	require.NoError(t, err)

	// response immediately if had quorum shards
	duration := time.Since(startTime)
	require.GreaterOrEqual(t, time.Second/2, duration, "greater duration: ", duration)

	transfer, err := streamer.Get(ctx(), bytes.NewBuffer(nil), *loc, uint64(size), 0)
	require.NoError(t, err)
	err = transfer()
	require.NoError(t, err)

	// wait all shards if no quorum shards
	vuidController.Block(1002)
	{
		startTime := time.Now()
		_, err := streamer.Put(ctx(), bytes.NewReader(buff), int64(size), nil)
		require.Error(t, err)

		duration := time.Since(startTime)
		minDuration := vuidController.duration*3 + time.Millisecond*(200+400) // retry ExponentialBackoff(3, 200)
		maxDuration := minDuration + minDuration/2
		t.Log(duration, minDuration, maxDuration)
		require.LessOrEqual(t, minDuration, duration, "less duration: ", duration)
		require.GreaterOrEqual(t, maxDuration, duration, "greater duration: ", duration)
	}
}

func TestAccessStreamPutQuorum(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamPutQuorum")
	defer func() {
		dataShards.clean()
		for ii := 0; ii < len(allID); ii++ {
			vuidController.Unbreak(proto.Vuid(allID[ii]))
		}
		vuidController.Break(1005)
	}()

	dataShards.clean()
	vuidController.Unbreak(1005)
	cases := []struct {
		ids      []proto.Vuid
		hasError bool
	}{
		// indexes by AZ
		// [[0 1 6 7] [2 3 8 9] [4 5 10 11]]
		{[]proto.Vuid{1001}, false},
		{[]proto.Vuid{1001, 1002}, true},              // az-0 instable
		{[]proto.Vuid{1001, 1002, 1007, 1008}, false}, // az-0 down
		{[]proto.Vuid{1003}, false},
		{[]proto.Vuid{1003, 1010}, true},              // az-1 instable
		{[]proto.Vuid{1003, 1004, 1009, 1010}, false}, // az-1 down
		{[]proto.Vuid{1005}, false},
		{[]proto.Vuid{1005, 1011}, true},              // az-2 instable
		{[]proto.Vuid{1005, 1006, 1011, 1012}, false}, // az-2 down
		{[]proto.Vuid{1001, 1011}, true},
		{[]proto.Vuid{1006, 1010}, true},
		{[]proto.Vuid{1008, 1010, 1012}, true},
	}

	size := 1 << 22
	buff := make([]byte, size)
	rand.Read(buff)
	for _, cs := range cases {
		for _, id := range cs.ids {
			vuidController.Break(id)
		}

		_, err := streamer.Put(ctx(), bytes.NewReader(buff), int64(size), nil)
		if cs.hasError {
			require.NotNil(t, err)
		} else {
			require.NoError(t, err)
		}

		time.Sleep(time.Second * time.Duration(punishServiceS))
		for _, id := range cs.ids {
			vuidController.Unbreak(id)
		}
	}
}

func BenchmarkAccessStreamPut(b *testing.B) {
	ctx := ctxWithName("BenchmarkAccessStreamPut")()
	vuidController.Unbreak(1005)
	defer func() {
		vuidController.Break(1005)
		dataShards.clean()
	}()

	cases := []struct {
		name string
		size int
	}{
		{"1B", 1},
		{"1KB", 1 << 10},
		{"1MB", 1 << 20},
		{"4MB", 1 << 22},
		{"8MB", 1 << 23},
	}

	buff := make([]byte, 1<<23)
	rand.Read(buff)
	for _, cs := range cases {
		b.ResetTimer()
		b.Run(cs.name, func(b *testing.B) {
			for ii := 0; ii <= b.N; ii++ {
				streamer.Put(ctx, bytes.NewReader(buff[:cs.size]), int64(cs.size), nil)
			}
		})
	}
}

func sum2Str(b []byte) string {
	return hex.EncodeToString(b[:])
}
