// Copyright 2023 The CubeFS Authors.
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

package buf_test

import (
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/stretchr/testify/require"
)

const checkPoolLoopCount = buf.HeaderBufferPoolSize

func checkPool(t *testing.T, pool *buf.BufferPool, size int) {
	first, err := pool.Get(size)
	require.NoError(t, err)
	second, err := pool.Get(size)
	require.NoError(t, err)
	// check size
	require.Equal(t, len(first), len(second))
	require.Equal(t, len(first), size)
	if &first[0] == &second[0] {
		t.Errorf("two Get() should not returns one buffer")
		return
	}
	oldAddr := &second[0]
	pool.Put(second)
	success := false
	for i := 0; i < checkPoolLoopCount; i++ {
		second, err = pool.Get(size)
		require.NoError(t, err)
		require.NotSame(t, &second[0], &first[0])
		if oldAddr == &second[0] {
			success = true
			break
		}
	}
	require.Equal(t, true, success)
}

func TestBufferPool(t *testing.T) {
	pool := buf.NewBufferPool()
	sizes := []int{
		util.PacketHeaderSize,
		util.BlockSize,
		util.DefaultTinySizeLimit,
	}
	for _, size := range sizes {
		checkPool(t, pool, size)
	}
}
