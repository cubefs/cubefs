package gorocksdb

// #include "rocksdb/c.h"
import "C"

// IngestExternalFileOptions represents available options when ingesting external files.
type IngestExternalFileOptions struct {
	c *C.rocksdb_ingestexternalfileoptions_t
}

// NewDefaultIngestExternalFileOptions creates a default IngestExternalFileOptions object.
func NewDefaultIngestExternalFileOptions() *IngestExternalFileOptions {
	return NewNativeIngestExternalFileOptions(C.rocksdb_ingestexternalfileoptions_create())
}

// NewNativeIngestExternalFileOptions creates a IngestExternalFileOptions object.
func NewNativeIngestExternalFileOptions(c *C.rocksdb_ingestexternalfileoptions_t) *IngestExternalFileOptions {
	return &IngestExternalFileOptions{c: c}
}

// SetMoveFiles specifies if it should move the files instead of copying them.
// Default to false.
func (opts *IngestExternalFileOptions) SetMoveFiles(flag bool) {
	C.rocksdb_ingestexternalfileoptions_set_move_files(opts.c, boolToChar(flag))
}

// SetSnapshotConsistency if specifies the consistency.
// If set to false, an ingested file key could appear in existing snapshots that were created before the
// file was ingested.
// Default to true.
func (opts *IngestExternalFileOptions) SetSnapshotConsistency(flag bool) {
	C.rocksdb_ingestexternalfileoptions_set_snapshot_consistency(opts.c, boolToChar(flag))
}

// SetAllowGlobalSeqNo sets allow_global_seqno. If set to false,IngestExternalFile() will fail if the file key
// range overlaps with existing keys or tombstones in the DB.
// Default true.
func (opts *IngestExternalFileOptions) SetAllowGlobalSeqNo(flag bool) {
	C.rocksdb_ingestexternalfileoptions_set_allow_global_seqno(opts.c, boolToChar(flag))
}

// SetAllowBlockingFlush sets allow_blocking_flush. If set to false and the file key range overlaps with
// the memtable key range (memtable flush required), IngestExternalFile will fail.
// Default to true.
func (opts *IngestExternalFileOptions) SetAllowBlockingFlush(flag bool) {
	C.rocksdb_ingestexternalfileoptions_set_allow_blocking_flush(opts.c, boolToChar(flag))
}

// Destroy deallocates the IngestExternalFileOptions object.
func (opts *IngestExternalFileOptions) Destroy() {
	C.rocksdb_ingestexternalfileoptions_destroy(opts.c)
	opts.c = nil
}
