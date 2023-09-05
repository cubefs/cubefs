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
)

func checkPool(t *testing.T, pool *buf.BufferPool, size int) (success bool) {
	first, err := pool.Get(size)
	if err != nil {
		t.Errorf("failed to get buffer from pool %v", err)
		return
	}
	second, err := pool.Get(size)
	if err != nil {
		t.Errorf("failed to get buffer from pool %v", err)
		return
	}
	if len(first) != len(second) {
		t.Errorf("two buffer should have same length")
		return
	}
	if len(first) != size {
		t.Errorf("buffer length should be %v", size)
		return
	}
	if &first[0] == &second[0] {
		t.Errorf("two Get() should not returns one buffer")
		return
	}
	success = true
	return
}

func TestBufferPool(t *testing.T) {
	pool := buf.NewBufferPool()
	sizes := []int{
		util.PacketHeaderSize,
		util.BlockSize,
		util.DefaultTinySizeLimit,
	}
	for _, size := range sizes {
		if !checkPool(t, pool, size) {
			return
		}
	}
}
