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

package ec_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	rp "github.com/cubefs/cubefs/blobstore/common/resourcepool"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

const (
	kb    = 1 << 10
	kb4   = 1 << 12
	kb64  = 1 << 16
	kb512 = 1 << 19
	mb    = 1 << 20
)

var memPool = rp.NewMemPool(map[int]int{kb64: 1, mb: 1}, false)

func init() {
	mrand.Seed(time.Now().Unix())
}

func poolRunning(p *rp.MemPool) int {
	n := 0
	for _, st := range p.Status() {
		n += st.Running
	}
	return n
}

func newTestEncoder(t *testing.T, tactic codemode.Tactic) ec.Encoder {
	t.Helper()
	enc, err := ec.NewEncoder(ec.Config{CodeMode: tactic})
	require.NoError(t, err)
	return enc
}

func TestNewBuffer(t *testing.T) {
	cm := codemode.EC6P6.Tactic()
	buffer, err := ec.NewBuffer(context.Background(), kb, cm, memPool)
	require.NoError(t, err)
	defer buffer.Release()
	require.Equal(t, kb, len(buffer.DataBuf))
	require.Equal(t, kb64, cap(buffer.DataBuf))
	require.Equal(t, cap(buffer.ECDataBuf), cap(buffer.DataBuf))
}

func TestNewBufferWaitOnLimit(t *testing.T) {
	cm := codemode.EC6P6.Tactic()

	poolNoWait := rp.NewMemPool(map[int]int{kb64: 1}, false)
	buf1, err := ec.NewBuffer(context.Background(), kb, cm, poolNoWait)
	require.NoError(t, err)
	defer buf1.Release()
	buf2, err := ec.NewBuffer(context.Background(), kb, cm, poolNoWait)
	require.NoError(t, err)
	defer buf2.Release()

	poolWait := rp.NewMemPool(map[int]int{kb64: 1}, true)
	buf3, err := ec.NewBuffer(context.Background(), kb, cm, poolWait)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		buf, err := ec.NewBuffer(context.Background(), kb, cm, poolWait)
		if err == nil {
			err = buf.Release()
		}
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
		t.Fatal("NewBuffer should wait when acquire is enabled")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, buf3.Release())
	require.NoError(t, <-done)
}

func TestNewRangedBuffer(t *testing.T) {
	cm := codemode.EC16P20L2.Tactic()
	buffer, err := ec.NewRangeBuffer(context.Background(), kb4, kb, 2*kb+2, cm, memPool)
	require.NoError(t, err)
	require.Equal(t, kb+2, len(buffer.DataBuf))
	require.Nil(t, buffer.ECDataBuf)
	require.Equal(t, kb64, cap(buffer.DataBuf))
	buffer.Release()
}

func TestGetBufferSize(t *testing.T) {
	cm := codemode.EC6P6.Tactic()
	sizes, err := ec.GetBufferSizes(kb, cm)
	shardSize := (kb + cm.N - 1) / cm.N
	if shardSize < cm.MinShardSize {
		shardSize = cm.MinShardSize
	}
	require.NoError(t, err)
	require.Equal(t, shardSize, sizes.ShardSize)
	require.Equal(t, kb, sizes.DataSize)
	require.Equal(t, shardSize*cm.N, sizes.ECDataSize)
	require.Equal(t, shardSize*(cm.N+cm.M+cm.L), sizes.ECSize)

	cm = codemode.EC16P20L2.Tactic()
	sizes, err = ec.GetBufferSizes(kb512, cm)
	shardSize = (kb512 + cm.N - 1) / cm.N
	if shardSize < cm.MinShardSize {
		shardSize = cm.MinShardSize
	}
	require.NoError(t, err)
	require.Equal(t, shardSize, sizes.ShardSize)
	require.Equal(t, kb512, sizes.DataSize)
	require.Equal(t, shardSize*cm.N, sizes.ECDataSize)
	require.Equal(t, shardSize*(cm.N+cm.M+cm.L), sizes.ECSize)

	_, err = ec.GetBufferSizes(0, cm)
	require.ErrorIs(t, err, ec.ErrShortData)
	_, err = ec.GetBufferSizes(-1, cm)
	require.ErrorIs(t, err, ec.ErrShortData)
}

func TestBufferResize(t *testing.T) {
	cm := codemode.EC6P6.Tactic()
	buffer, err := ec.NewBuffer(context.Background(), kb, cm, memPool)
	require.NoError(t, err)
	defer func() {
		buffer.Release()
	}()
	require.Equal(t, kb, len(buffer.DataBuf))
	require.Equal(t, kb64, cap(buffer.DataBuf))

	// // pool limited
	// _, err = ec.NewBuffer(context.Background(), kb, cm, memPool)
	// require.ErrorIs(t, err, rp.ErrPoolLimit)

	err = buffer.Resize(context.Background(), kb+512)
	require.NoError(t, err)
	require.Equal(t, kb+512, len(buffer.DataBuf))
	require.Equal(t, kb64, cap(buffer.DataBuf))

	// // pool limited
	// _, err = ec.NewBuffer(context.Background(), kb, cm, memPool)
	// require.ErrorIs(t, err, rp.ErrPoolLimit)

	// still single-block after resize (12KiB ECSize < 128KiB)
	const data12k = 12 * kb
	err = buffer.Resize(context.Background(), data12k)
	require.NoError(t, err)
	require.NotNil(t, buffer.DataBuf)
	require.Equal(t, data12k, len(buffer.DataBuf))
	require.Equal(t, kb64, cap(buffer.DataBuf))

	buff, err := ec.NewBuffer(context.Background(), kb, cm, memPool)
	require.NoError(t, err)
	buff.Release()
}

func TestBufferRelease(t *testing.T) {
	t.Run("NilReceiver", func(t *testing.T) {
		var nilBuf *ec.Buffer
		require.NoError(t, nilBuf.Release())
	})

	t.Run("EmptyBuffer", func(t *testing.T) {
		empty := &ec.Buffer{}
		require.NoError(t, empty.Release())
	})

	t.Run("SingleBlock", func(t *testing.T) {
		pool := rp.NewMemPool(map[int]int{128 << 10: 4}, false)
		cm := codemode.EC6P6.Tactic()
		const data12k = 12 * kb

		before := poolRunning(pool)
		buf, err := ec.NewBuffer(context.Background(), data12k, cm, pool)
		require.NoError(t, err)
		require.NotNil(t, buf.DataBuf)
		require.Equal(t, before+1, poolRunning(pool))

		enc := newTestEncoder(t, cm)
		_, err = buf.Split(enc)
		require.NoError(t, err)
		require.NoError(t, buf.ReleaseShard(0))
		require.Equal(t, before+1, poolRunning(pool))

		require.NoError(t, buf.Release())
		require.Equal(t, before, poolRunning(pool))
		require.NoError(t, buf.Release())
		require.Equal(t, before, poolRunning(pool))
	})

	t.Run("PerShard", func(t *testing.T) {
		pool := rp.NewMemPool(map[int]int{16 << 10: 20}, false)
		cm := codemode.EC6P6.Tactic()
		const data64k = 64 * kb
		nShards := cm.N + cm.M + cm.L

		before := poolRunning(pool)
		buf, err := ec.NewBuffer(context.Background(), data64k, cm, pool)
		require.NoError(t, err)
		require.Nil(t, buf.DataBuf)
		require.Equal(t, before+nShards, poolRunning(pool))

		require.NoError(t, buf.ReleaseShard(0))
		require.Equal(t, before+nShards-1, poolRunning(pool))
		require.NoError(t, buf.ReleaseShard(0))
		require.Equal(t, before+nShards-1, poolRunning(pool))

		require.NoError(t, buf.Release())
		require.Equal(t, before, poolRunning(pool))
	})
}

func TestBufferDataPadding(t *testing.T) {
	// new a mempool while resize will alloc oversize buffer
	memPool := rp.NewMemPool(map[int]int{kb64: 1, mb: 1}, false)
	// random read
	for _, size := range []int{kb64, mb} {
		buf, err := memPool.Get(context.Background(), size)
		require.NoError(t, err)
		rand.Read(buf)
		memPool.Put(buf)
	}

	zero := make([]byte, mb)

	cm := codemode.EC6P6.Tactic()
	buffer, err := ec.NewBuffer(context.Background(), kb, cm, memPool)
	require.NoError(t, err)
	defer func() {
		buffer.Release()
	}()

	buf := buffer.DataBuf[:cap(buffer.DataBuf)]
	require.Equal(t, zero[:buffer.ShardSize*cm.N-buffer.DataSize],
		buf[buffer.DataSize:buffer.ShardSize*cm.N])

	for ii := 0; ii < 100; ii++ {
		// Keep ECSize below per-shard threshold (single-block padding path).
		nextSize := mrand.Intn(8*kb) + 1
		err = buffer.Resize(context.Background(), nextSize)
		require.NoError(t, err)
		require.NotNil(t, buffer.DataBuf)
		buf = buffer.DataBuf[:cap(buffer.DataBuf)]
		require.Equal(t, zero[:buffer.ShardSize*cm.N-buffer.DataSize],
			buf[buffer.DataSize:buffer.ShardSize*cm.N])
	}
}

func TestBufferPerShardAllocNFailure(t *testing.T) {
	cm := codemode.EC6P6.Tactic()
	const data64k = 64 * kb
	pool := rp.NewMemPool(map[int]int{16 << 10: cm.N + cm.M + cm.L - 1}, true)

	before := poolRunning(pool)
	_, err := ec.NewBuffer(context.Background(), data64k, cm, pool)
	require.Error(t, err)
	require.Equal(t, before, poolRunning(pool))
}

func legacySingleBlockEncode(t *testing.T, pool *rp.MemPool, enc ec.Encoder, tactic codemode.Tactic, payload []byte) [][]byte {
	t.Helper()
	sizes, err := ec.GetBufferSizes(len(payload), tactic)
	require.NoError(t, err)

	ctx := context.Background()
	buf, err := pool.Get(ctx, sizes.ECSize)
	if err == rp.ErrNoSuitableSizeClass {
		buf, err = pool.Alloc(ctx, sizes.ECSize)
	}
	require.NoError(t, err)
	defer pool.Put(buf)

	pool.Zero(buf[sizes.DataSize:sizes.ECDataSize])
	_, err = io.ReadFull(bytes.NewReader(payload), buf[:sizes.DataSize])
	require.NoError(t, err)

	shards, err := enc.Split(buf[:sizes.ECDataSize])
	require.NoError(t, err)
	require.NoError(t, enc.Encode(shards))
	return shards
}

func TestBufferReadSplitEncodeMatchesSingleBlock(t *testing.T) {
	cases := []struct {
		name     string
		tactic   codemode.Tactic
		dataSize int
	}{
		{"ec6p6-12k", codemode.EC6P6.Tactic(), 12 * kb},
		{"ec6p6-64k", codemode.EC6P6.Tactic(), 64 * kb},
		{"ec16p20l2-12k", codemode.EC16P20L2.Tactic(), 12 * kb},
		{"ec16p20l2-64k", codemode.EC16P20L2.Tactic(), 64 * kb},
	}

	pool := rp.NewMemPool(map[int]int{
		2 << 10:   64,
		16 << 10:  64,
		128 << 10: 64,
		512 << 10: 64,
		1 << 20:   64,
	}, false)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := make([]byte, tc.dataSize)
			_, err := rand.Read(payload)
			require.NoError(t, err)

			enc := newTestEncoder(t, tc.tactic)
			refShards := legacySingleBlockEncode(t, pool, enc, tc.tactic, payload)

			buf, err := ec.NewBuffer(context.Background(), tc.dataSize, tc.tactic, pool)
			require.NoError(t, err)
			defer buf.Release()
			n, err := buf.Read(bytes.NewReader(payload))
			require.NoError(t, err)
			require.Equal(t, tc.dataSize, n)
			shards, err := buf.Split(enc)
			require.NoError(t, err)
			require.NoError(t, enc.Encode(shards))

			require.Len(t, shards, len(refShards))
			for i := range shards {
				require.Equal(t, refShards[i], shards[i])
			}
		})
	}
}
