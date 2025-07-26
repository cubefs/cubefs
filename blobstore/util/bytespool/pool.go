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

import "sync"

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
	pools [maxSizeBit + 2]*sync.Pool
)

func newBytesPoolFunc(cap int) func() interface{} {
	return func() interface{} {
		nb := make([]byte, cap)
		return &nb
	}
}

func doInit() {
	// Initialize pools[0] with cap=0
	pools[0] = &sync.Pool{
		New: newBytesPoolFunc(0),
	}

	// Initialize pools[1..maxSizeBit+1] with cap=2^(i-1)
	for i := 1; i <= (maxSizeBit + 1); i++ {
		cap := 1 << (i - 1)
		pools[i] = &sync.Pool{
			New: newBytesPoolFunc(cap),
		}
	}
}

func init() {
	doInit()
}

// GetPool returns a sync.Pool that generates bytes slice with the size.
// Return nil if no such pool exists.
func GetPool(size int) *sync.Pool {
	if size < 0 || size > maxSize {
		return nil
	}
	if size == 0 {
		return pools[0]
	}
	bits := msb(size)
	if size != 1<<bits {
		bits++
	}
	idx := bits + 1
	return pools[idx]
}

// Alloc returns a bytes slice with the size.
// Make a new bytes slice if oversize.
func Alloc(size int) []byte {
	return *AllocPointer(size)
}

// AllocPointer returns a pointer bytes slice with the size.
// Make a new pointer bytes slice if oversize.
func AllocPointer(size int) *[]byte {
	if pool := GetPool(size); pool != nil {
		bp := pool.Get().(*[]byte)
		*bp = (*bp)[:size]
		return bp
	}
	nb := make([]byte, size)
	return &nb
}

// Free puts the underlying array of the slice into a suitable pool.
// Discard the bytes slice if oversize or cap == 0.
// Note: This function does not reuse the original slice header
func Free(b []byte) {
	size := cap(b)
	if size == 0 {
		return
	}

	FreePointer(&b)
}

// FreePointer puts the pointer bytes slice into suitable pool.
// Discard the pointer bytes slice if oversize.
func FreePointer(bp *[]byte) {
	if bp == nil {
		return
	}
	size := cap(*bp)
	if size == 0 {
		pools[0].Put(bp)
		return
	}
	bits := msb(size)
	if size > maxSize || size != 1<<bits {
		return
	}

	*bp = (*bp)[:size]

	// cap=2^(i-1) => i = bits + 1
	idx := bits + 1
	pools[idx].Put(bp) // nolint: staticcheck
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
