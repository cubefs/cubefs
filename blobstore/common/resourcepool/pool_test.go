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

package resourcepool_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	rp "github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

func TestPoolBase(t *testing.T) {
	p := rp.NewPool(func() interface{} {
		return 1
	}, 2)

	require.Equal(t, 2, p.Cap())
	require.Equal(t, 0, p.Len())
	require.Equal(t, 2, p.Idle())

	_, err := p.Get()
	require.NoError(t, err)
	_, err = p.Get()
	require.NoError(t, err)

	require.Equal(t, 2, p.Len())
	require.Equal(t, 0, p.Idle())

	_, err = p.Get()
	require.Equal(t, rp.ErrPoolLimit, err)
	_, err = p.Get()
	require.Equal(t, rp.ErrPoolLimit, err)

	p.Put(1)
	_, err = p.Get()
	require.NoError(t, err)
}

func TestPoolNoLimit(t *testing.T) {
	{
		p := rp.NewPool(func() interface{} {
			return 1
		}, 0)
		require.Equal(t, 0, p.Cap())
		require.Equal(t, 0, p.Len())
		require.Equal(t, 0, p.Idle())

		_, err := p.Get()
		require.ErrorIs(t, rp.ErrPoolLimit, err)
	}
	{
		p := rp.NewPool(func() interface{} {
			return 1
		}, -10)
		require.Equal(t, -10, p.Cap())
		require.Equal(t, 0, p.Len())
		require.Equal(t, -1, p.Idle())

		tasks := make([]func() error, 0, 10000)
		for range [10000]struct{}{} {
			tasks = append(tasks, func() error {
				_, err := p.Get()
				return err
			})
		}

		err := task.Run(context.Background(), tasks...)
		require.NoError(t, err)
	}
}

func BenchmarkPoolGetPut(b *testing.B) {
	benchmarkPoolGetPut(b, func(size, capacity int) rp.Pool {
		return rp.NewPool(func() interface{} {
			return make([]byte, size)
		}, capacity)
	})
}

func benchmarkPoolGetPut(b *testing.B, newPool func(zize, capacity int) rp.Pool) {
	const kb = 1 << 10
	const mb = 1 << 20

	cases := []struct {
		size int
	}{
		{kb * 2},
		{kb * 64},
		{kb * 512},
		{mb},
		{mb * 2},
		{mb * 4},
	}

	for _, cs := range cases {
		runtime.GC()
		b.ResetTimer()
		b.Run(fmt.Sprintf("%s-%s", humman(cs.size), "unlimited"), func(b *testing.B) {
			pool := newPool(cs.size, -1)
			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				if val, err := pool.Get(); err == nil {
					pool.Put(val)
				}
			}
		})

		b.ResetTimer()
		b.Run(fmt.Sprintf("%s-%s", humman(cs.size), "with_1m"), func(b *testing.B) {
			pool := newPool(cs.size, 1000000)
			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				if val, err := pool.Get(); err == nil {
					pool.Put(val)
				}
			}
		})

		b.ResetTimer()
		b.Run(fmt.Sprintf("%s-%s", humman(cs.size), "make"), func(b *testing.B) {
			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				_ = make([]byte, cs.size)
			}
		})
	}
}
