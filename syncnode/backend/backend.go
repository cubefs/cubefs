// Copyright 2026 The CubeFS Authors.
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

// Package backend defines the unified Storage backend abstraction used by
// the syncnode task executor. Every supported storage system (cfs, s3,
// local, plus P2 extensions like tos/bos/oss/cos) implements the Backend
// interface; the executor never knows which concrete kind it's talking to.
//
// See design.md §10.6.
package backend

import (
	"context"
	"errors"
	"io"
	"time"
)

// Backend is the contract all storage adapters fulfil. Implementations live
// under subpackages (backend/s3, backend/local, backend/cfs). The interface
// is intentionally small — list + get + put + head + delete + rename are
// the only data-path operations the executor needs.
type Backend interface {
	// Kind returns the registered kind string (e.g. "s3", "local", "cfs").
	// Must match the string used in rule.src.kind / rule.dst.kind.
	Kind() string

	// List enumerates entries under prefix. Implementations stream Entry
	// values on the returned channel and close it on completion. If
	// recursive is false, only immediate children are returned (object
	// stores ignore this; POSIX backends honour it). Errors (including
	// ctx cancellation) cause the channel to close early with an Entry
	// whose Err field is non-nil.
	List(ctx context.Context, prefix string, recursive bool) (<-chan Entry, error)

	// Get returns a reader over key bytes [off, off+size). If size == 0,
	// reads to end of object. Caller must Close() the returned reader.
	// Returns ErrKeyNotFound for missing keys.
	Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error)

	// Head returns metadata for key without fetching content. Returns
	// ErrKeyNotFound if key is missing.
	Head(ctx context.Context, key string) (size int64, etag string, mtime time.Time, err error)

	// Put writes body (size bytes) to key. opts controls multipart /
	// storage class / metadata, and may request a checksum be computed
	// alongside the upload (see PutOptions.ComputeChecksum). Returns a
	// PutResult that always carries the backend-native ETag (where
	// applicable) and optionally a checksum.
	Put(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (PutResult, error)

	// GetChecksum returns the backend-native (or computed) checksum for key.
	// Returns ErrKeyNotFound if missing. `algorithm` is one of "sha256" /
	// "md5" / "crc32" and lets the executor decide cross-endpoint
	// comparability.
	//   - cfs:   伴随文件 sha256，未命中时流式计算并写回；algorithm="sha256"
	//   - s3:    优先读 user metadata syncnode-sha256；fallback 单段 ETag (md5)
	//   - local: 流式 sha256（带轻量 mtime+size 缓存）；algorithm="sha256"
	GetChecksum(ctx context.Context, key string) (sum string, algorithm string, err error)

	// Delete removes key. Idempotent: deleting a missing key is not an
	// error.
	Delete(ctx context.Context, key string) error

	// Rename moves oldKey to newKey atomically. Object stores typically
	// implement this as copy-then-delete (NOT atomic); POSIX implements
	// via syscall.Rename (atomic on same filesystem). Capabilities()
	// reports whether the implementation is atomic.
	Rename(ctx context.Context, oldKey, newKey string) error

	// Capabilities reports static capabilities of this backend.
	Capabilities() Caps

	// Close releases all resources (HTTP clients, mounted streams, etc.).
	// Safe to call multiple times.
	Close() error
}

// Entry is one item from a List call.
type Entry struct {
	Key   string
	Size  int64
	Mtime time.Time
	ETag  string
	IsDir bool  // POSIX-only; object stores always false
	Err   error // non-nil signals the listing terminated due to an error
}

// PutOptions controls how a Put is performed.
type PutOptions struct {
	StorageClass string            // S3 family: STANDARD / STANDARD_IA / GLACIER / etc.
	ContentType  string            // optional MIME type
	Metadata     map[string]string // backend-specific metadata pairs
	Multipart    bool              // force multipart (default: backend decides by size)
	PartSizeMiB  int               // for multipart; 0 = backend default

	// ComputeChecksum tells the backend to compute a sha256 alongside the upload
	// and return it in PutResult. Object stores additionally persist it as user
	// metadata (`x-amz-meta-syncnode-sha256`); POSIX backends just return the value.
	ComputeChecksum bool
}

// PutResult is the result of a Put call. ETag is backend-native (e.g. s3
// etag, empty for POSIX/CFS). Checksum/Algorithm are populated only when
// PutOptions.ComputeChecksum was set. BytesPut is the byte count the
// backend acknowledged writing — useful for sanity checks against the
// declared size.
type PutResult struct {
	ETag      string // backend-native (s3 etag, empty for POSIX/CFS)
	Checksum  string // sha256 hex (only set when PutOptions.ComputeChecksum)
	Algorithm string // "sha256" when Checksum populated; "" otherwise
	BytesPut  int64  // for sanity: bytes the backend acknowledged
}

// Caps reports static backend capabilities. Each Backend returns a fixed
// value (the caps are property of the implementation, not per-call).
type Caps struct {
	RangeRead         bool // Get with non-zero size supported
	Multipart         bool // multipart upload supported
	AtomicRename      bool // Rename is atomic (POSIX yes, object stores no)
	ListMaxKeys       int  // upper bound on List page size; 0 = unlimited
	StrongConsistency bool // PUT followed by GET returns the new bytes immediately

	// NativeChecksum reports whether GetChecksum returns a fast (O(1)) server-side
	// value. s3=true (ETag), cfs=false (companion file), local=false. Hint for
	// executor to skip src-side sha256 compute when both sides are native.
	NativeChecksum bool
}

// Common errors returned by Backend implementations. Callers can use
// errors.Is to match.
var (
	// ErrKeyNotFound is returned by Get / Head / Delete when the requested
	// key does not exist on the backend.
	ErrKeyNotFound = errors.New("backend: key not found")

	// ErrChecksumMismatch is returned by data-integrity checks when the
	// computed (or persisted) checksum disagrees with the expected value.
	ErrChecksumMismatch = errors.New("backend: checksum mismatch")

	// ErrBackendUnsupported is returned by Rename when the implementation
	// does not support the operation (rare; most backends support some
	// form of rename).
	ErrBackendUnsupported = errors.New("backend: operation unsupported")

	// ErrConfigInvalid is returned by a Backend constructor when the
	// provided BackendConfig fails validation specific to that backend.
	ErrConfigInvalid = errors.New("backend: invalid config")
)
