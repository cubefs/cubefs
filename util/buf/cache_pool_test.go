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

	"github.com/cubefs/cubefs/util/buf"
	"github.com/stretchr/testify/require"
)

func checkCachePool(t *testing.T, pool *buf.FileCachePool) {
	first := pool.Get()
	second := pool.Get()
	require.Equal(t, len(first), len(second))
	require.NotSame(t, &first[0], &second[0])
	oldAddr := &second[0]
	pool.Put(second)
	second = pool.Get()
	require.NotSame(t, &second[0], &first[0])
	require.Same(t, oldAddr, &second[0])
}

func TestCachePool(t *testing.T) {
	if buf.CachePool == nil {
		buf.InitCachePool(8388608)
	}
	checkCachePool(t, buf.CachePool)
}
