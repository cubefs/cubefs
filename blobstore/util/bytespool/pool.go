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

package bytespool

import (
	"bytes"
	"sync"
)

func newBytes(size int) func() interface{} {
	return func() interface{} {
		return make([]byte, size)
	}
}

const (
	zeroSize   = 1 << 14 // 16 KB
	maxSizeBit = 24      // 16 MB
	maxSize    = 1 << maxSizeBit
)

var (
	debruijinPosition = [...]byte{
		0, 9, 1, 10, 13, 21, 2, 29, 11, 14,
		16, 18, 22, 25, 3, 30, 8, 12, 20, 28,
		15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31,
	}
	zero  = make([]byte, zeroSize)
	pools [maxSizeBit + 1]*sync.Pool
)

func init() {
	for idx := range pools {
		bits := idx
		pools[idx] = &sync.Pool{
			New: newBytes(1 << bits),
		}
	}
}

// GetPool returns a sync.Pool that generates bytes slice with the size.
// Return nil if no such pool exists.
func GetPool(size int) *sync.Pool {
	if size < 0 || size > maxSize {
		return nil
	}
	bits := msb(size)
	idx := bits
	if size != 1<<bits {
		idx++
	}
	return pools[idx]
}

// Alloc returns a bytes slice with the size.
// Make a new bytes slice if oversize.
func Alloc(size int) []byte {
	if pool := GetPool(size); pool != nil {
		b := pool.Get().([]byte)
		return b[:size]
	}
	return make([]byte, size)
}

// Free puts the bytes slice into suitable pool.
// Discard the bytes slice if oversize.
func Free(b []byte) {
	size := cap(b)
	bits := msb(size)
	if size > maxSize || size != 1<<bits {
		return
	}
	b = b[0:size]
	pools[bits].Put(b) // nolint: staticcheck
}

// Zero clean up the bytes slice b to zero.
func Zero(b []byte) {
	for len(b) > 0 {
		n := copy(b, zero)
		b = b[n:]
	}
}

// msb return the pos of most significiant bit
// http://supertech.csail.mit.edu/papers/debruijn.pdf
func msb(size int) byte {
	v := uint32(size)
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	return debruijinPosition[(v*0x07C4ACDD)>>27]
}

func AllocWithZeroLengthBuffer(size int) *bytes.Buffer {
	byteSlice := AllocWithZeroLength(size)
	buffer := bytes.NewBuffer(byteSlice)
	return buffer
}

func AllocWithZeroLength(size int) []byte {
	if pool := GetPool(size); pool != nil {
		b := pool.Get().([]byte)
		return b[:0]
	}
	return make([]byte, 0, size)
}
