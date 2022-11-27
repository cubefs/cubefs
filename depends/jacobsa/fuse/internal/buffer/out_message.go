// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"fmt"
	"log"
	"reflect"
	"unsafe"

	"github.com/jacobsa/fuse/internal/fusekernel"
)

// OutMessageHeaderSize is the size of the leading header in every
// properly-constructed OutMessage. Reset brings the message back to this size.
const OutMessageHeaderSize = int(unsafe.Sizeof(fusekernel.OutHeader{}))

// OutMessage provides a mechanism for constructing a single contiguous fuse
// message from multiple segments, where the first segment is always a
// fusekernel.OutHeader message.
//
// Must be initialized with Reset.
type OutMessage struct {
	// The offset into payload to which we're currently writing.
	payloadOffset int

	header  fusekernel.OutHeader
	payload [MaxReadSize]byte
}

// Make sure that the header and payload are contiguous.
func init() {
	a := unsafe.Offsetof(OutMessage{}.header) + uintptr(OutMessageHeaderSize)
	b := unsafe.Offsetof(OutMessage{}.payload)

	if a != b {
		log.Panicf(
			"header ends at offset %d, but payload starts at offset %d",
			a, b)
	}
}

// Reset resets m so that it's ready to be used again. Afterward, the contents
// are solely a zeroed fusekernel.OutHeader struct.
func (m *OutMessage) Reset() {
	// Ideally we'd like to write:
	//
	//     m.payloadOffset = 0
	//     m.header = fusekernel.OutHeader{}
	//
	// But Go 1.8 beta 2 generates bad code for this
	// (https://golang.org/issue/18370). Encourage it to generate the same code
	// as Go 1.7.4 did.
	if unsafe.Offsetof(m.payload) != 24 {
		panic("unexpected OutMessage layout")
	}

	a := (*[3]uint64)(unsafe.Pointer(m))
	a[0] = 0
	a[1] = 0
	a[2] = 0
}

// OutHeader returns a pointer to the header at the start of the message.
func (m *OutMessage) OutHeader() *fusekernel.OutHeader {
	return &m.header
}

// Grow grows m's buffer by the given number of bytes, returning a pointer to
// the start of the new segment, which is guaranteed to be zeroed. If there is
// insufficient space, it returns nil.
func (m *OutMessage) Grow(n int) (p unsafe.Pointer) {
	p = m.GrowNoZero(n)
	if p != nil {
		memclr(p, uintptr(n))
	}

	return
}

// GrowNoZero is equivalent to Grow, except the new segment is not zeroed. Use
// with caution!
func (m *OutMessage) GrowNoZero(n int) (p unsafe.Pointer) {
	// Will we overflow the buffer?
	o := m.payloadOffset
	if len(m.payload)-o < n {
		return
	}

	p = unsafe.Pointer(uintptr(unsafe.Pointer(&m.payload)) + uintptr(o))
	m.payloadOffset = o + n

	return
}

// ShrinkTo shrinks m to the given size. It panics if the size is greater than
// Len() or less than OutMessageHeaderSize.
func (m *OutMessage) ShrinkTo(n int) {
	if n < OutMessageHeaderSize || n > m.Len() {
		panic(fmt.Sprintf(
			"ShrinkTo(%d) out of range (current Len: %d)",
			n,
			m.Len()))
	}

	m.payloadOffset = n - OutMessageHeaderSize
}

// Append is equivalent to growing by len(src), then copying src over the new
// segment. Int panics if there is not enough room available.
func (m *OutMessage) Append(src []byte) {
	p := m.GrowNoZero(len(src))
	if p == nil {
		panic(fmt.Sprintf("Can't grow %d bytes", len(src)))
	}

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	memmove(p, unsafe.Pointer(sh.Data), uintptr(sh.Len))

	return
}

// AppendString is like Append, but accepts string input.
func (m *OutMessage) AppendString(src string) {
	p := m.GrowNoZero(len(src))
	if p == nil {
		panic(fmt.Sprintf("Can't grow %d bytes", len(src)))
	}

	sh := (*reflect.StringHeader)(unsafe.Pointer(&src))
	memmove(p, unsafe.Pointer(sh.Data), uintptr(sh.Len))

	return
}

// Len returns the current size of the message, including the leading header.
func (m *OutMessage) Len() int {
	return OutMessageHeaderSize + m.payloadOffset
}

// Bytes returns a reference to the current contents of the buffer, including
// the leading header.
func (m *OutMessage) Bytes() []byte {
	l := m.Len()
	sh := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&m.header)),
		Len:  l,
		Cap:  l,
	}

	return *(*[]byte)(unsafe.Pointer(&sh))
}
