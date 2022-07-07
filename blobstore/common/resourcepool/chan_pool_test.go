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
	"math/rand"
	"testing"
	"time"

	rp "github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/stretchr/testify/require"
)

func TestChanPoolSizeLimited(t *testing.T) {
	p := rp.NewChanPool(func() []byte {
		return make([]byte, 1)
	}, -1)
	require.Equal(t, -1, p.Cap())

	b := make([]byte, 1)
	for range [1 << 23]struct{}{} {
		p.Put(b)
	}
	require.Equal(t, 1<<22, p.Idle())
}

func TestChanPoolRelease(t *testing.T) {
	rp.SetReleaseInterval(200 * time.Millisecond)
	defer rp.SetReleaseInterval(time.Minute * 2)

	p := rp.NewChanPool(func() []byte {
		return make([]byte, 1024)
	}, -1)

	for i := range [1000]struct{}{} {
		go func(i int) {
			buf, err := p.Get()
			require.NoError(t, err)
			time.Sleep(1 + time.Millisecond*time.Duration(rand.Intn(2000)))
			p.Put(buf)
		}(i)
	}
	time.Sleep(time.Second)
}

func TestChanPoolBase(t *testing.T) {
	makeN := 0
	p := rp.NewChanPool(func() []byte {
		makeN++
		return make([]byte, 1024)
	}, 2)

	require.Equal(t, 2, p.Cap())
	require.Equal(t, 0, p.Len())
	require.Equal(t, 0, p.Idle())

	_, err := p.Get()
	require.NoError(t, err)
	_, err = p.Get()
	require.NoError(t, err)

	require.Equal(t, 2, p.Cap())
	require.Equal(t, 2, p.Len())
	require.Equal(t, 0, p.Idle())

	buf, err := p.Get()
	require.NoError(t, err)
	require.Equal(t, 3, makeN)

	p.Put(buf)
	require.Equal(t, 1, p.Idle())
	_, err = p.Get()
	require.NoError(t, err)
	require.Equal(t, 3, makeN)
	require.Equal(t, 0, p.Idle())
}

func TestChanPoolNoLimit(t *testing.T) {
	{
		makeN := 0
		p := rp.NewChanPool(func() []byte {
			makeN++
			return make([]byte, 1024)
		}, 0)
		require.Equal(t, 0, p.Cap())

		_, err := p.Get()
		require.NoError(t, err)
		require.Equal(t, 1, makeN)
	}
	{
		p := rp.NewChanPool(func() []byte {
			return make([]byte, 1024)
		}, -1)
		require.Equal(t, -1, p.Cap())
		require.Equal(t, 0, p.Idle())

		for range [1000]struct{}{} {
			buf, err := p.Get()
			require.NoError(t, err)
			p.Put(buf)
		}
		require.Equal(t, 1, p.Idle())
	}
}

func BenchmarkChanPoolGetPut(b *testing.B) {
	benchmarkPoolGetPut(b, func(size, capacity int) rp.Pool {
		return rp.NewChanPool(func() []byte {
			return make([]byte, size)
		}, capacity)
	})
}
