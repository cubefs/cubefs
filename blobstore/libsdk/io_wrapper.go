package main

/*
#include <stdlib.h> // malloc, free
#include <string.h> // memcpy
*/
import (
	"C"
)

import (
	"errors"
	"io"
	"unsafe"
)

// Mode defines the access mode of the ioWrapper.
type Mode int

const (
	// ReadOnly allows only Read operations.
	ReadOnly Mode = iota
	// WriteOnly allows only Write operations.
	WriteOnly
)

// ioWrapper is a wrapper for a C/C++ I/O buffer.
// NOTE: This type is NOT safe for concurrent access.
// It must be used in a single goroutine or protected by external synchronization mechanisms (e.g., sync.Mutex).
type ioWrapper struct {
	ptr  unsafe.Pointer // Pointer to the C memory buffer
	size int64          // Total size of the buffer
	pos  int64          // Current position in the buffer
	mode Mode           // Access mode (ReadOnly/WriteOnly)
}

// NewIOWrapper creates a new ioWrapper with the given pointer, size, position, and access mode.
// The 'mode' must be either ReadOnly or WriteOnly.
func NewIOWrapper(ptr unsafe.Pointer, size int64, pos int64, mode Mode) (*ioWrapper, error) {
	switch mode {
	case ReadOnly, WriteOnly:
		return &ioWrapper{
			ptr:  ptr,
			size: size,
			pos:  pos,
			mode: mode,
		}, nil
	default:
		return nil, errors.New("invalid ioWrapper mode")
	}
}

// Read reads data from the C buffer into the provided Go byte slice.
func (w *ioWrapper) Read(p []byte) (n int, err error) {
	switch w.mode {
	case ReadOnly:
		return w.readImpl(p)
	case WriteOnly:
		return 0, errors.New("ioWrapper is write-only, cannot read")
	default:
		return 0, errors.New("invalid ioWrapper mode")
	}
}

// Write writes data from the provided Go byte slice into the C buffer.
func (w *ioWrapper) Write(p []byte) (n int, err error) {
	switch w.mode {
	case WriteOnly:
		return w.writeImpl(p)
	case ReadOnly:
		return 0, errors.New("ioWrapper is read-only, cannot write")
	default:
		return 0, errors.New("invalid ioWrapper mode")
	}
}

// readImpl is the implementation of Read for ReadOnly mode.
func (w *ioWrapper) readImpl(p []byte) (int, error) {
	if w.ptr == nil {
		return 0, errors.New("invalid C pointer")
	}
	if w.pos >= w.size {
		return 0, io.EOF
	}
	remaining := w.size - w.pos
	toRead := len(p)
	if int64(toRead) > remaining {
		toRead = int(remaining)
	}

	// Read from C end
	C.memcpy(unsafe.Pointer(&p[0]), unsafe.Pointer(uintptr(w.ptr)+uintptr(w.pos)), C.size_t(toRead))
	w.pos += int64(toRead)
	return toRead, nil
}

// writeImpl is the implementation of Write for WriteOnly mode.
func (w *ioWrapper) writeImpl(p []byte) (int, error) {
	if w.ptr == nil {
		return 0, errors.New("invalid C pointer")
	}
	if w.pos+int64(len(p)) > w.size {
		return 0, io.ErrShortWrite
	}

	// Write to C end
	C.memcpy(unsafe.Pointer(uintptr(w.ptr)+uintptr(w.pos)), unsafe.Pointer(&p[0]), C.size_t(len(p)))
	w.pos += int64(len(p))
	return len(p), nil
}

// allocTestBuffer allocates C memory buffer and returns the pointer
func allocTestBuffer(size int) unsafe.Pointer {
	cPtr := C.malloc(C.size_t(size))
	if cPtr == nil {
		panic("malloc failed")
	}
	return unsafe.Pointer(cPtr)
}

// freeTestBuffer frees the C memory buffer
func freeTestBuffer(ptr unsafe.Pointer) {
	C.free(unsafe.Pointer(ptr))
}
