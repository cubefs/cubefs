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
	"fmt"
	"unsafe"
)

// AlignedBuffer address of first byte of buff wa aligned.
// - - - - - - - - - - - - - - - - - - - -
// | head  |<- aligned address buffer -> |
// - - - - - - - - - - - - - - - - - - - -
func AlignedBuffer(head, capacity, alignment int) []byte {
	buff := make([]byte, head+capacity+alignment)
	addr := uintptr(unsafe.Pointer(&buff[0]))
	low := addr & uintptr(alignment-1)
	offset := alignment - int(low) - head
	if offset < 0 {
		offset += alignment
	}
	capa := offset + head + capacity
	return buff[offset:capa:capa]
}

// AlignedHead head with alignment.
func AlignedHead[I Integer](size, alignment I) I {
	assert(size, alignment)
	return size % alignment
}

// AlignedTail tail with alignment.
func AlignedTail[I Integer](size, alignment I) I {
	assert(size, alignment)
	return (alignment - size%alignment) % alignment
}

// AlignedFull full with alignment.
func AlignedFull[I Integer](size, alignment I) I {
	assert(size, alignment)
	if (alignment & (alignment - 1)) == 0 {
		return (size + (alignment - 1)) & (^(alignment - 1))
	}
	return AlignedBlocks(size, alignment) * alignment
}

// AlignedBlocks number of blocks with alignment.
func AlignedBlocks[I Integer](size, alignment I) I {
	assert(size, alignment)
	return (size + (alignment - 1)) / alignment
}

func assert[I Integer](size, alignment I) {
	if size < 0 {
		panic(fmt.Sprintf("util-align: size(%d) < 0", size))
	}
	if alignment <= 0 {
		panic(fmt.Sprintf("util-align: alignment(%d) <= 0", alignment))
	}
}
