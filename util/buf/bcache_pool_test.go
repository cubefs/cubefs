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

	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/util/buf"
)

func TestFileBCachePool(t *testing.T) {
	if buf.BCachePool == nil {
		buf.InitbCachePool(bcache.MaxBlockSize)
	}
	firstVal := buf.BCachePool.Get()
	secondVal := buf.BCachePool.Get()
	if len(firstVal) != len(secondVal) {
		t.Errorf("Get() should return buffer with same length")
		return
	}
	if &firstVal[0] == &secondVal[0] {
		t.Errorf("two Get() should not return one buffer")
		return
	}
}
