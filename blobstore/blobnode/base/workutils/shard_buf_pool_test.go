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

package workutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardBufPool(t *testing.T) {
	bufSize := 10
	poolSize := 4
	bufPool := NewByteBufferPool(bufSize, poolSize)
	require.Equal(t, bufSize, bufPool.GetBufSize())

	b, err := bufPool.Get()
	require.NoError(t, err)
	require.Equal(t, bufSize, len(b))
	bufPool.Put(b)

	allocBufs := make([][]byte, poolSize)
	for i := 0; i < poolSize; i++ {
		b, err = bufPool.Get()
		require.NoError(t, err)
		require.Equal(t, bufSize, len(b))
		allocBufs[i] = b
		t.Logf("cap %d len %d", cap(b), len(b))
	}

	b, err = bufPool.Get()
	t.Logf("cap %d len %d", cap(b), len(b))
	require.Error(t, err)
	require.EqualError(t, ErrMemNotEnough, err.Error())

	for i := 0; i < poolSize; i++ {
		bufPool.Put(allocBufs[i])
	}
}
