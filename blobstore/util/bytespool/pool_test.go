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

package bytespool_test

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

func TestUtilBytespool(t *testing.T) {
	run := func(size int) {
		buff := bytespool.Alloc(size)
		if len(buff) != size {
			t.Fatal(size)
		}
		bytespool.Zero(buff)
		bytespool.Free(buff)
		if size == 0 {
			return
		}
		size--
		buff = bytespool.Alloc(size)
		if len(buff) != size {
			t.Fatal(size)
		}
		bytespool.Zero(buff)
		bytespool.Free(buff)
	}
	run(0)
	for bits := range [27]struct{}{} {
		run(1 << bits)
	}
}

func BenchmarkBytespool(b *testing.B) {
	var buff []byte
	for ii := 0; ii < b.N; ii++ {
		buff = bytespool.Alloc(1 << (ii % 20))
		bytespool.Free(buff)
	}
}
