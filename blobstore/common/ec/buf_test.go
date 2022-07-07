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
	"crypto/rand"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	rp "github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	kb    = 1 << 10
	kb4   = 1 << 12
	kb64  = 1 << 16
	kb512 = 1 << 19
	mb    = 1 << 20
)

var memPool = rp.NewMemPool(map[int]int{kb64: 1, mb: 1})

func init() {
	mrand.Seed(time.Now().Unix())
	log.SetOutputLevel(log.Lfatal)
}

func TestNewBuffer(t *testing.T) {
	cm := codemode.EC6P6.Tactic()
	buffer, err := ec.NewBuffer(kb, cm, memPool)
	require.NoError(t, err)
	defer buffer.Release()
	require.Equal(t, kb, len(buffer.DataBuf))
	require.Equal(t, kb64, cap(buffer.DataBuf))
	require.Equal(t, cap(buffer.ECDataBuf), cap(buffer.DataBuf))

	// mb pool
	cm = codemode.EC16P20L2.Tactic()
	buffer, err = ec.NewBuffer(kb64, cm, memPool)
	require.NoError(t, err)
	defer buffer.Release()
	require.Equal(t, kb64, len(buffer.DataBuf))
	require.Equal(t, mb, cap(buffer.DataBuf))
	require.Equal(t, cap(buffer.ECDataBuf), cap(buffer.DataBuf))

	// alloc new bytes when no suitable pool, do not put back
	buffer, err = ec.NewBuffer(kb512, cm, memPool)
	shardSize := (kb512 + cm.N - 1) / cm.N
	require.NoError(t, err)
	require.Equal(t, kb512, len(buffer.DataBuf))
	require.Equal(t, shardSize*(cm.N+cm.M+cm.L), cap(buffer.DataBuf))
	require.Equal(t, shardSize*cm.N, len(buffer.ECDataBuf))
	require.Equal(t, shardSize*(cm.N+cm.M+cm.L), cap(buffer.ECDataBuf))
}

func TestNewRangedBuffer(t *testing.T) {
	cm := codemode.EC6P6.Tactic()
	buffer, err := ec.NewBuffer(kb, cm, memPool)
	require.NoError(t, err)
	require.Equal(t, kb, len(buffer.DataBuf))
	require.NotNil(t, buffer.ECDataBuf)
	require.Equal(t, kb64, cap(buffer.DataBuf))
	buffer.Release()

	// kb pool in ranged
	cm = codemode.EC16P20L2.Tactic()
	buffer, err = ec.NewRangeBuffer(kb4, kb, 2*kb+2, cm, memPool)
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
	require.ErrorIs(t, ec.ErrShortData, err)
	_, err = ec.GetBufferSizes(-1, cm)
	require.ErrorIs(t, ec.ErrShortData, err)
}

func TestBufferResize(t *testing.T) {
	cm := codemode.EC6P6.Tactic()
	buffer, err := ec.NewBuffer(kb, cm, memPool)
	require.NoError(t, err)
	defer func() {
		buffer.Release()
	}()
	require.Equal(t, kb, len(buffer.DataBuf))
	require.Equal(t, kb64, cap(buffer.DataBuf))

	// // pool limited
	// _, err = ec.NewBuffer(kb, cm, memPool)
	// require.ErrorIs(t, rp.ErrPoolLimit, err)

	err = buffer.Resize(kb + 512)
	require.NoError(t, err)
	require.Equal(t, kb+512, len(buffer.DataBuf))
	require.Equal(t, kb64, cap(buffer.DataBuf))

	// // pool limited
	// _, err = ec.NewBuffer(kb, cm, memPool)
	// require.ErrorIs(t, rp.ErrPoolLimit, err)

	// mb pool, release kb64 pool
	err = buffer.Resize(kb64)
	require.NoError(t, err)
	require.Equal(t, kb64, len(buffer.DataBuf))
	require.Equal(t, mb, cap(buffer.DataBuf))

	// old kb64 pool was released
	buff, err := ec.NewBuffer(kb, cm, memPool)
	require.NoError(t, err)
	buff.Release()
}

func TestBufferRelease(t *testing.T) {
	{
		var buffer *ec.Buffer
		require.NoError(t, buffer.Release())
	}
	{
		buffer := &ec.Buffer{}
		require.NoError(t, buffer.Release())
	}
	{
		buffer, err := ec.NewBuffer(kb, codemode.EC6P6.Tactic(), memPool)
		require.NoError(t, err)
		require.NoError(t, buffer.Release())
	}
}

func TestBufferDataPadding(t *testing.T) {
	// new a mempool while resize will alloc oversize buffer
	memPool := rp.NewMemPool(map[int]int{kb64: 1, mb: 1})
	// random read
	for _, size := range []int{kb64, mb} {
		buf, err := memPool.Get(size)
		require.NoError(t, err)
		rand.Read(buf)
		memPool.Put(buf)
	}

	zero := make([]byte, mb)

	cm := codemode.EC6P6.Tactic()
	buffer, err := ec.NewBuffer(kb, cm, memPool)
	require.NoError(t, err)
	defer func() {
		buffer.Release()
	}()

	buf := buffer.DataBuf[:cap(buffer.DataBuf)]
	require.Equal(t, zero[:buffer.ShardSize*cm.N-buffer.DataSize],
		buf[buffer.DataSize:buffer.ShardSize*cm.N])

	for ii := 0; ii < 100; ii++ {
		err = buffer.Resize(mrand.Intn(mb*6) + 1)
		require.NoError(t, err)
		buf = buffer.DataBuf[:cap(buffer.DataBuf)]
		require.Equal(t, zero[:buffer.ShardSize*cm.N-buffer.DataSize],
			buf[buffer.DataSize:buffer.ShardSize*cm.N])
	}
}
