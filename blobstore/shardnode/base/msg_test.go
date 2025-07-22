// Copyright 2025 The CubeFS Authors.
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

package base

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestMsgKeyGenerator(t *testing.T) {
	t.Run("NewMsgKeyGenerator", func(t *testing.T) {
		// Test with zero timestamp
		g := NewMsgKeyGenerator(0)
		require.NotNil(t, g)
		require.True(t, g.CurrentTs() > 0)

		// Test with future timestamp
		futureTs := Ts(time.Now().Add(1*time.Hour).Unix() << 32)
		g = NewMsgKeyGenerator(futureTs)
		require.Equal(t, futureTs, g.CurrentTs())
	})

	t.Run("generateTs", func(t *testing.T) {
		g := NewMsgKeyGenerator(0)
		initialTs := g.CurrentTs()

		// First call should return current time
		ts1 := g.generateTs()
		require.True(t, ts1 >= initialTs)

		// Subsequent calls should increment
		ts2 := g.generateTs()
		require.Equal(t, ts1+1, ts2)
	})

	t.Run("EncodeDelMsgKey", func(t *testing.T) {
		g := NewMsgKeyGenerator(0)
		vid := proto.Vid(123)
		bid := proto.BlobID(456)
		shardKeys := [][]byte{[]byte("part1"), []byte("part2")}

		ts, key := g.EncodeDelMsgKey(vid, bid, shardKeys)
		require.True(t, ts > 0)
		require.NotEmpty(t, key)

		// Verify decoded values
		decodedTs, decodedVid, decodedBid := g.decodeMsgKey(key)
		require.Equal(t, ts, decodedTs)
		require.Equal(t, vid, decodedVid)
		require.Equal(t, bid, decodedBid)
	})

	t.Run("EncodeDelMsgKeyWithTimeUnix", func(t *testing.T) {
		g := NewMsgKeyGenerator(0)
		vid := proto.Vid(789)
		bid := proto.BlobID(101112)
		shardKeys := [][]byte{[]byte("part3"), []byte("part4")}
		testTime := time.Now().Unix()

		key := g.EncodeDelMsgKeyWithTimeUnix(testTime, vid, bid, shardKeys)
		require.NotEmpty(t, key)

		// Verify timestamp
		decodedTs, _, _ := g.decodeMsgKey(key)
		require.Equal(t, Ts(testTime<<32), decodedTs)
	})

	t.Run("EncodeRawDelMsgKey", func(t *testing.T) {
		g := NewMsgKeyGenerator(0)
		vid := proto.Vid(131415)
		bid := proto.BlobID(161718)
		tagNum := 4 // 2 real shard keys + 2 empty

		ts, key, shardKeys := g.EncodeRawDelMsgKey(vid, bid, tagNum)
		require.True(t, ts > 0)
		require.NotEmpty(t, key)
		require.Len(t, shardKeys, tagNum)

		// First two shard keys should contain vid and bid
		require.Equal(t, uint32(vid), binary.BigEndian.Uint32(shardKeys[0]))
		require.Equal(t, uint64(bid), binary.BigEndian.Uint64(shardKeys[1]))

		_shardKeys := shardnode.ParseShardKeys(key, tagNum)
		for i := 0; i < tagNum; i++ {
			require.Equal(t, _shardKeys[i], shardKeys[i])
		}
	})

	t.Run("decodeMsgKey", func(t *testing.T) {
		g := NewMsgKeyGenerator(0)
		testTs := Ts(123456789 << 32)
		testVid := proto.Vid(987)
		testBid := proto.BlobID(654321)
		testKey := g.EncodeDelMsgKeyWithTs(testTs, testVid, testBid, [][]byte{})

		ts, vid, bid := g.decodeMsgKey(testKey)
		require.Equal(t, testTs, ts)
		require.Equal(t, testVid, vid)
		require.Equal(t, testBid, bid)
	})

	t.Run("CurrentTs", func(t *testing.T) {
		g := NewMsgKeyGenerator(0)
		initialTs := g.CurrentTs()

		// After generating a timestamp, current should update
		g.generateTs()
		require.True(t, g.CurrentTs() > initialTs)
	})

	t.Run("CompositeOrdering", func(t *testing.T) {
		g := NewMsgKeyGenerator(0)
		ts1, key1 := g.EncodeDelMsgKey(200, 50, [][]byte{[]byte("part1"), []byte("z")})
		time.Sleep(time.Millisecond)
		ts2, key2 := g.EncodeDelMsgKey(100, 40, [][]byte{[]byte("part1"), []byte("a")})

		require.True(t, ts1 < ts2)
		require.Equal(t, -1, bytes.Compare(key1, key2))
	})
}

func Benchmark(b *testing.B) {
	ctx := context.Background()
	tmp := path.Join(os.TempDir(), fmt.Sprintf("msg_key_test_%d", rand.Int31n(10000)+10000))
	defer os.RemoveAll(tmp)

	store, err := kvstore.NewKVStore(ctx, tmp, kvstore.RocksdbLsmKVType, &kvstore.Option{
		CreateIfMissing: true,
	})
	require.Nil(b, err)
	defer store.Close()

	g := NewMsgKeyGenerator(0)
	b.ResetTimer()
	b.Run("Generate message key and store", func(b *testing.B) {
		_, key, _ := g.EncodeRawDelMsgKey(proto.Vid(1), proto.BlobID(100), 2)
		err = store.SetRaw(ctx, "default", key, []byte("value"))
		require.Nil(b, err)
	})
}
