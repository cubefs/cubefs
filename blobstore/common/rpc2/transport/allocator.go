package transport

import (
	"sync"
	"unsafe"
)

const (
	maxsizebit = 24
	maxsize    = 1 << maxsizebit

	addressAlignment = 512
	addressMask      = addressAlignment - 1

	Alignment = addressAlignment
)

// AssignedBuffer assigned buffer with frame header from Allocator,
// the data section's address is aligned.
type AssignedBuffer interface {
	// Bytes returns plain bytes.
	Bytes() []byte
	// Written sets efficient n of bytes.
	Written(n int)
	// Len returns efficient length of bytes.
	Len() int
	// Free back buffer to allocator.
	Free() error
}

// Allocator reused buffer for frames.
type Allocator interface {
	// Alloc returns size added header of frame length of assigned buffer.
	Alloc(size int) (AssignedBuffer, error)
}

var (
	defaultAllocator Allocator
	debruijinPos     = [...]byte{
		0, 9, 1, 10, 13, 21, 2, 29, 11, 14,
		16, 18, 22, 25, 3, 30, 8, 12, 20, 28,
		15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31,
	}
)

func init() {
	defaultAllocator = NewAllocator()
}

// allocator for incoming frames, optimized to prevent overwriting after zeroing
type allocator struct {
	buffers []sync.Pool
}

// NewAllocator initiates a []byte allocator for frames,
// the waste(memory fragmentation) of space allocation is guaranteed to be
// no more than 50% added an addressAlignment buffer.
//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// | aligned | frame header  |<- aligned address buffer -> | aligned |
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// | random1 |       8       |       power of 2            | random2 |
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
func NewAllocator() Allocator {
	alloc := new(allocator)
	alloc.buffers = make([]sync.Pool, maxsizebit+1) // 1B -> 16MB
	for k := range alloc.buffers {
		i := k
		alloc.buffers[k].New = func() interface{} {
			return alignedBufferWithHeader(1 << i)
		}
	}
	return alloc
}

// Alloc a bytes from pool with most appropriate cap
func (alloc *allocator) Alloc(size int) (AssignedBuffer, error) {
	if size < 0 || size > maxsize {
		return nil, ErrAllocOversize
	}

	bits := msb(size)
	idx := bits
	if size != 1<<bits {
		idx++
	}
	buffer := alloc.buffers[idx].Get().([]byte)[:size+headerSize]
	return &assignedBuffer{alloc: alloc, buffer: buffer}, nil
}

// Free returns a bytes to pool for future use,
// which the cap must be exactly 2^n + header,
// and address of data buffer must be mod of alignment.
func (alloc *allocator) Free(buf []byte) error {
	capa := cap(buf) - headerSize
	if capa < 0 {
		return ErrAllocOversize
	}
	bits := msb(capa)
	if capa == 0 || capa > maxsize || capa != 1<<bits {
		return ErrAllocOversize
	}
	addr := uintptr(unsafe.Pointer(&buf[0])) + headerSize
	if addr%addressAlignment != 0 {
		return ErrAllocAddress
	}
	alloc.buffers[bits].Put(buf) // nolint: staticcheck
	return nil
}

type assignedBuffer struct {
	offset int
	buffer []byte
	alloc  *allocator
}

func (ab *assignedBuffer) Bytes() []byte { return ab.buffer }
func (ab *assignedBuffer) Written(n int) { ab.offset += n }
func (ab *assignedBuffer) Len() int      { return ab.offset }
func (ab *assignedBuffer) Free() (err error) {
	if ab.alloc == nil {
		return
	}
	err = ab.alloc.Free(ab.buffer)
	ab.alloc = nil
	ab.buffer = nil
	return
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
	return debruijinPos[(v*0x07C4ACDD)>>27]
}

func alignedBufferWithHeader(capacity int) []byte {
	buff := make([]byte, capacity+addressAlignment+headerSize)
	addr := uintptr(unsafe.Pointer(&buff[0]))
	low := addr & addressMask
	offset := addressAlignment - int(low) - headerSize
	if offset < 0 {
		offset += addressAlignment
	}
	capa := offset + headerSize + capacity
	return buff[offset:capa:capa]
}
