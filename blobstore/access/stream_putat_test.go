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
	"crypto/rand"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
)

func TestAccessStreamPutAtBase(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamPutAtBase")
	sumChecker := func(size int, data []byte) {
		dataShards.clean()

		hasherMap := access.HasherMap{
			access.HashAlgCRC32: access.HashAlgCRC32.ToHasher(),
		}
		hashSumMap := make(access.HashSumMap, len(hasherMap))
		err := streamer.PutAt(ctx(), bytes.NewReader(data), clusterID, 1, 10000, int64(size), hasherMap)
		require.Nil(t, err)
		for alg, hasher := range hasherMap {
			hashSumMap[alg] = hasher.Sum(nil)
		}
		require.Equal(t, hashSumMap.GetSumVal(access.HashAlgCRC32), crc32.ChecksumIEEE(data[:size]))
	}
	// 0
	{
		size := 0
		err := streamer.PutAt(ctx(), newReader(size), clusterID, 1, 10000, int64(size), nil)
		require.NotNil(t, err)
	}
	// 1 byte
	{
		size := 1
		data := make([]byte, size)
		rand.Read(data)
		sumChecker(size, data)

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
	// 4M per shard
	{
		size := (1 << 22) * 6
		data := make([]byte, size)
		rand.Read(data)
		sumChecker(size, data)

		require.Equal(t, 1<<22, len(dataShards.get(1001, 10000)))
	}
	// 4M + 1B, put 1B
	{
		size := (1 << 22) + 1
		data := make([]byte, size)
		rand.Read(data)
		sumChecker(1, data)

		shardSize := getBufSizes(1).ShardSize
		require.Equal(t, shardSize, len(dataShards.get(1001, 10000)))
	}
	// 4M, error
	{
		dataShards.clean()
		size := 1 << 22
		err := streamer.PutAt(ctx(), newReader(size), clusterID, 1, 10000, int64(size)+1, nil)
		require.NotNil(t, err)
		require.Equal(t, 0, len(dataShards.get(1001, 10000)))
	}

	dataShards.clean()
}
