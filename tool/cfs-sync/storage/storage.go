package storage

import (
	"context"
	"io"
	"time"
)

// Object represents a file or directory entry in a storage backend.
type Object struct {
	Key   string    // relative path, / separated; directories end with /
	Size  int64
	Mtime time.Time
	IsDir bool
	ETag  string // optional, populated by S3 for --checksum mode
}

// Storage is the unified interface for CubeFS, S3, and local filesystem backends.
type Storage interface {
	// List returns all Objects under prefix recursively, streaming in lexicographic order.
	// The caller must drain both channels; errors channel receives at most one error.
	List(ctx context.Context, prefix string) (<-chan *Object, <-chan error)

	// Get returns a ReadCloser for the given key, starting at off, reading at most size bytes.
	// size <= 0 means read to EOF.
	Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error)

	// Put writes size bytes from r to key.
	Put(ctx context.Context, key string, r io.Reader, size int64) error

	// Delete removes key.
	Delete(ctx context.Context, key string) error

	// MkdirAll ensures the directory path exists (noop for S3).
	MkdirAll(ctx context.Context, key string) error

	// String returns a human-readable name for logs and progress display.
	String() string
}
