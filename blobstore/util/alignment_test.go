// Copyright 2024 The Cuber Authors.
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

package util

import (
	"math/rand"
	"testing"
	"unsafe"
)

func TestUtilAlignment(t *testing.T) {
	for _, head := range []int{0, 4, 8, 127} {
		for _, alignment := range []int{128, 512} {
			for range [100]struct{}{} {
				capacity := rand.Int31n(1 << 20)
				buff := AlignedBuffer(head, int(capacity), alignment)
				addr := uintptr(unsafe.Pointer(&buff[0])) + uintptr(head)
				if addr%uintptr(alignment) != 0 {
					panic(addr)
				}
			}
		}
	}
}
