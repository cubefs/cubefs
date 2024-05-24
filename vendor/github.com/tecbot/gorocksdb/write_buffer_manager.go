package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

// WriteBufferManager manager the whole write buffer between multiple column families or multiple db
type WriteBufferManager struct {
	c *C.rocksdb_write_buffer_manager_t
}

// NewWriteBufferManager creates a WriteBufferManager object.
func NewWriteBufferManager(bufferSize uint64) *WriteBufferManager {
	return &WriteBufferManager{c: C.rocksdb_write_buffer_manager_create(C.size_t(bufferSize))}
}

// Destroy deallocates the WriterBufferManager object.
func (w *WriteBufferManager) Destroy() {
	C.rocksdb_write_buffer_manager_destory(w.c)
	w.c = nil
}
