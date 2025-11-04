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

package blobdeleter

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
)

func TestEncodeDecodeDelMsgKey(t *testing.T) {
	t.Run("encodeDelMsgKey", func(t *testing.T) {
		g := base.NewTsGenerator(0)
		ts := g.GenerateTs()
		vid := proto.Vid(123)
		bid := proto.BlobID(456)
		shardKeys := []string{"part1", "part2"}

		key := encodeDelMsgKey(ts, vid, bid, shardKeys)
		require.True(t, ts > 0)
		require.NotEmpty(t, key)

		// Verify decoded values
		mk := newMsgKey()
		defer mk.release()
		mk.setKey(key)
		err := mk.decode(len(shardKeys))
		require.Nil(t, err)
		require.Equal(t, ts, mk.ts)
		require.Equal(t, vid, mk.vid)
		require.Equal(t, bid, mk.bid)
		for i := 0; i < len(shardKeys); i++ {
			require.Equal(t, shardKeys[i], mk.shardKeys[i])
		}
	})

	t.Run("encodeRawDelMsgKey", func(t *testing.T) {
		g := base.NewTsGenerator(0)
		ts := g.GenerateTs()
		vid := proto.Vid(365)
		bid := proto.BlobID(45542013)
		tagNum := 4 // 2 real shard keys + 2 empty

		key, shardKeys := encodeRawDelMsgKey(ts, vid, bid, tagNum)
		require.True(t, ts > 0)
		require.NotEmpty(t, key)
		require.Len(t, shardKeys, tagNum)

		// Verify decoded values
		mk := newMsgKey()
		defer mk.release()
		mk.setKey(key)
		err := mk.decode(len(shardKeys))
		require.Nil(t, err)
		require.Equal(t, ts, mk.ts)
		require.Equal(t, vid, mk.vid)
		require.Equal(t, bid, mk.bid)
		for i := 0; i < len(shardKeys); i++ {
			require.Equal(t, shardKeys[i], mk.shardKeys[i])
		}
	})

	t.Run("CompositeOrdering", func(t *testing.T) {
		g := base.NewTsGenerator(0)
		ts1 := g.GenerateTs()
		key1 := encodeDelMsgKey(ts1, 200, 50, []string{"part1", "z"})
		time.Sleep(time.Millisecond)
		ts2 := g.GenerateTs()
		key2 := encodeDelMsgKey(ts2, 100, 40, []string{"part1", "a"})

		require.True(t, ts1 < ts2)
		require.Equal(t, -1, bytes.Compare([]byte(key1), []byte(key2)))
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

	g := base.NewTsGenerator(0)
	b.ResetTimer()
	b.Run("Generate message key and store", func(b *testing.B) {
		ts := g.GenerateTs()
		key, _ := encodeRawDelMsgKey(ts, proto.Vid(1), proto.BlobID(100), 2)
		err = store.SetRaw(ctx, "default", key, []byte("value"))
		require.Nil(b, err)
	})
}

func TestMsgKeyReuse(t *testing.T) {
	key1 := newMsgKey()
	key1.setMsgType(snproto.MessageTypeDelete)
	key1.setTier(snproto.TierSingleIdx)
	key1.setTs(base.Ts(12345))
	key1.setVid(proto.Vid(100))
	key1.setBid(proto.BlobID(200))
	key1.setShardKeys([]string{"test1", "test2"})
	encoded := key1.encode()
	require.NotEmpty(t, encoded)

	key1.decode(2)
	require.Equal(t, key1.vid, proto.Vid(100))

	key1.setVid(proto.Vid(200))
	encoded = key1.encode()
	require.NotEmpty(t, encoded)
	key1.decode(2)
	require.Equal(t, key1.vid, proto.Vid(200))
	key1.release()
}

func TestMsgKeyPoolReuse(t *testing.T) {
	// Test that sync.Pool correctly reuses msgKey objects
	key1 := newMsgKey()
	key1.setMsgType(snproto.MessageTypeDelete)
	key1.setTier(snproto.TierSingleIdx)
	key1.setTs(base.Ts(12345))
	key1.setVid(proto.Vid(100))
	key1.setBid(proto.BlobID(200))
	key1.setShardKeys([]string{"test1", "test2"})

	// Get the pointer address
	ptr1 := key1

	// Return to pool
	key1.release()

	// Get a new one from pool (should be the same object after reset)
	key2 := newMsgKey()

	// They should be the same underlying object
	require.Same(t, ptr1, key2, "sync.Pool should reuse the same object")

	// The fields should be reset
	require.Equal(t, snproto.MessageType(0), key2.msgType)
	require.Equal(t, snproto.MessageTier(0), key2.tier)
	require.Equal(t, base.Ts(0), key2.ts)
	require.Equal(t, proto.Vid(0), key2.vid)
	require.Equal(t, proto.BlobID(0), key2.bid)
	require.Len(t, key2.shardKeys, 0)
	require.Len(t, key2.key, 0)

	key2.release()
}

func encodeDelMsgKey(ts base.Ts, vid proto.Vid, bid proto.BlobID, shardKeys []string) []byte {
	mk := newMsgKey()
	defer mk.release()

	mk.setMsgType(snproto.MessageTypeDelete)
	mk.setTier(snproto.TierSingleIdx)
	mk.setTs(ts)
	mk.setVid(vid)
	mk.setBid(bid)
	mk.setShardKeys(shardKeys)
	encoded := mk.encode()

	// Copy the result before returning to pool
	result := make([]byte, len(encoded))
	copy(result, encoded)
	return result
}
