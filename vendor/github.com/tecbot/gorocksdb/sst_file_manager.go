package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

// SstFileManager manager sst file deletion between multiple column families or multiple db
type SstFileManager struct {
	c *C.rocksdb_sst_file_manager_t
}

// NewSstFileManager creates a SstFileManager object.
func NewSstFileManager(env *Env) *SstFileManager {
	return &SstFileManager{c: C.rocksdb_sst_file_manager_create(env.c)}
}

// Destroy deallocates the SstFileManager object.
func (w *SstFileManager) Destroy() {
	C.rocksdb_sst_file_manager_destory(w.c)
	w.c = nil
}
