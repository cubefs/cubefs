// Copyright 2024 The CubeFS Authors.
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

	"github.com/stretchr/testify/require"
)

func TestUtilAlignmentBuffer(t *testing.T) {
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

func TestUtilAlignmentHeadTail(t *testing.T) {
	require.Panics(t, func() { AlignedHead(-1, 128) })
	require.Panics(t, func() { AlignedHead(1, 0) })
	require.Panics(t, func() { AlignedTail(-1, 128) })
	require.Panics(t, func() { AlignedTail(1, 0) })
	require.Equal(t, 0, AlignedHead(0, 128))
	require.Equal(t, int64(1), AlignedHead[int64](513, 512))
	require.Equal(t, uint64(0), AlignedTail[uint64](512, 512))
	require.Equal(t, uint(1023), AlignedTail[uint](1025, 1024))
	for range [100]struct{}{} {
		size := rand.Int63() + 1
		alignment := rand.Int63n(1<<20) + 1
		if rand.Int63()%7 == 0 {
			size -= size % alignment
		}
		head := AlignedHead(size, alignment)
		tail := AlignedTail(size, alignment)
		if head == 0 {
			require.Equal(t, int64(0), head+tail)
		} else {
			require.Equal(t, alignment, head+tail)
		}
	}
}

func TestUtilAlignmentBlock(t *testing.T) {
	require.Equal(t, 0, AlignedFull(0, 128))
	require.Equal(t, 0, AlignedBlocks(0, 128))
	require.Equal(t, int64(1024), AlignedFull[int64](513, 512))
	require.Equal(t, int64(2), AlignedBlocks[int64](513, 512))
	require.Equal(t, uint64(512), AlignedFull[uint64](512, 512))
	require.Equal(t, uint64(1), AlignedBlocks[uint64](512, 512))
	for range [100]struct{}{} {
		size := rand.Int63() + 1
		alignment := int64(32 << 10)
		if rand.Int63()%7 == 0 {
			size -= size % alignment
		}
		full := AlignedFull(size, alignment)
		blocks := AlignedBlocks(size, alignment)
		require.Equal(t, full, blocks*alignment)
		if tail := AlignedTail(size, alignment); tail == 0 {
			require.Equal(t, size, full)
			require.Equal(t, size/alignment, blocks)
		} else {
			require.Equal(t, size+tail, full)
			require.Equal(t, size/alignment+1, blocks)
		}
	}
	for range [100]struct{}{} {
		size := rand.Int63() + 1
		alignment := rand.Int63n(1<<20) + 1
		if rand.Int63()%7 == 0 {
			size -= size % alignment
		}
		full := AlignedFull(size, alignment)
		blocks := AlignedBlocks(size, alignment)
		require.Equal(t, full, blocks*alignment)
		if tail := AlignedTail(size, alignment); tail == 0 {
			require.Equal(t, size, full)
			require.Equal(t, size/alignment, blocks)
		} else {
			require.Equal(t, size+tail, full)
			require.Equal(t, size/alignment+1, blocks)
		}
	}
}
