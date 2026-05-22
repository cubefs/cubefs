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

	// SameInstance reports whether other points at the same underlying
	// storage instance as this backend — same endpoint + same credential
	// realm for object stores, same volume for cfs, same allowed root for
	// local. When true, the executor MAY skip Get→Put and ask one side
	// for a server-side copy (see ServerSideCopier and Caps.ServerSideCopy).
	//
	// Conservative-by-default: when in doubt, return false. A false
	// negative only costs an unnecessary streaming round trip; a false
	// positive risks writing to the wrong realm.
	SameInstance(other Backend) bool

	// Close releases all resources (HTTP clients, mounted streams, etc.).
	// Safe to call multiple times.
	Close() error
}

// Stater is an optional capability interface for backends that can
// return full POSIX-style metadata (mode/uid/gid/xattrs) in a single
// call. The executor uses this when any PreserveXxx flag is set on the
// rule; backends that don't implement Stater are treated as if every
// metadata field is unset (zero-value Stat populated from Head).
//
// Implementations:
//   - local: syscall.Lstat + listxattr/getxattr
//   - cfs:   mw.InodeGet + mw.XAttrList_ll/XAttrGet_ll
//   - s3:    HeadObject → parse `x-amz-meta-syncnode-*` (with
//            `x-amz-meta-*` rclone-naked fallback)
type Stater interface {
	// Stat returns full metadata for key. Backends that don't natively
	// support a field leave it at zero/nil; callers must tolerate
	// missing fields. Returns ErrKeyNotFound when key is missing.
	Stat(ctx context.Context, key string) (Stat, error)
}

// Stat carries the full POSIX-style metadata returned by Stater.Stat.
// Fields that the backend does not natively support are left as their
// zero value (nil for the pointer fields). Callers use the pointer
// pattern to distinguish "unset" from "explicitly zero".
type Stat struct {
	Size  int64
	ETag  string
	Mtime time.Time

	Mode   *uint32           // POSIX mode bits; nil when not stored
	UID    *uint32           // POSIX uid; nil when not stored
	GID    *uint32           // POSIX gid; nil when not stored
	Xattrs map[string][]byte // xattr name → raw bytes; nil/empty when none
}

// ServerSideCopier is an optional capability interface for backends that
// can copy bytes between two keys without round-tripping through the
// executor. The executor will only invoke ServerSideCopy after asserting
// both:
//
//   - src.SameInstance(dst) is true (same underlying realm), AND
//   - src.Capabilities().ServerSideCopy is true.
//
// Implementations return ErrBackendUnsupported when the requested copy
// cannot be performed server-side (e.g. cross-credential, object too
// large for the chosen API path) so the executor can fall back to the
// generic Get→Put pipeline.
type ServerSideCopier interface {
	// ServerSideCopy copies srcKey → dstKey inside the same backend
	// instance. opts mirrors PutOptions but only ContentType, StorageClass,
	// Metadata and Mtime are meaningful — ComputeChecksum is treated as a
	// best-effort hint (object stores typically have a native ETag the
	// executor can compare). The returned PutResult carries the same fields
	// as a normal Put: ETag, optional Checksum + Algorithm, BytesPut.
	ServerSideCopy(ctx context.Context, srcKey, dstKey string, opts PutOptions) (PutResult, error)
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

	// Mtime, when non-nil, instructs the backend to persist this modification
	// time on the written object so that subsequent List/Head returns the
	// source mtime instead of the backend's write time. Implementations:
	//   - local: os.Chtimes(dst, now, *Mtime) after rename
	//   - s3:    PutObject metadata `x-amz-meta-syncnode-mtime` (RFC3339Nano);
	//            Head prefers this header over LastModified. ListObjectsV2
	//            does NOT return user-metadata, so List still falls back to
	//            LastModified for that backend.
	//   - cfs:   mw.Setattr with proto.AttrModifyTime after the data stream
	//            is flushed.
	// A nil pointer (default) preserves prior behavior — the backend uses
	// its own write-time.
	Mtime *time.Time

	// Mode, when non-nil, persists the POSIX file mode bits (rwx + setuid/
	// setgid/sticky) on the written object. Implementations:
	//   - local: syscall.Chmod(dst, *Mode) after rename
	//   - cfs:   mw.Setattr with proto.AttrMode
	//   - s3:    user metadata `x-amz-meta-syncnode-mode` (octal string,
	//            e.g. "0644"). Stat falls back to rclone naked
	//            `x-amz-meta-mode` for interop.
	Mode *uint32

	// UID, when non-nil, persists the POSIX uid. Always set together with
	// GID by the executor (PreserveOwner is a single switch); split only
	// here because the backends accept them on separate syscalls.
	//   - local: syscall.Lchown(dst, *UID, *GID)
	//   - cfs:   mw.Setattr with proto.AttrUid
	//   - s3:    user metadata `x-amz-meta-syncnode-uid` (decimal); Stat
	//            falls back to rclone naked `x-amz-meta-uid`.
	UID *uint32

	// GID, when non-nil, persists the POSIX gid. See UID for cross-backend
	// behaviour.
	GID *uint32

	// Xattrs, when non-empty, persists extended attributes alongside the
	// object body. Keys are full xattr names ("user.foo",
	// "system.posix_acl_access", ...). Values are raw bytes — the
	// backend handles base64/hex encoding internally.
	//   - local: syscall.Setxattr(dst, name, value, 0) per entry
	//   - cfs:   mw.XAttrSet_ll per entry
	//   - s3:    single user-metadata header
	//            `x-amz-meta-syncnode-xattrs` =
	//              base64(JSON({name: base64(value), ...}))
	//            S3 user-metadata is capped at 2 KiB total; on overflow
	//            Put returns ErrMetadataTooLarge so the executor can apply
	//            OnMetadataUnsupported (warn/skip/error).
	// Namespace filtering (user.*/system.posix_acl_*/skip security|trusted)
	// is the executor's responsibility — the backend writes whatever it
	// is handed.
	Xattrs map[string][]byte
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

	// NativeMtimeWrite reports whether the backend honors PutOptions.Mtime by
	// persisting the supplied modification time on the written object. All three
	// in-tree backends report true (local via os.Chtimes, s3 via user metadata,
	// cfs via mw.Setattr). Backends that cannot preserve mtime should report
	// false so the executor can warn or fall back.
	NativeMtimeWrite bool

	// ServerSideCopy reports whether the backend implements the
	// ServerSideCopier interface and can copy bytes between two keys
	// inside the same instance without streaming through the executor.
	// Reported true by s3 (CopyObject + multipart UploadPartCopy);
	// reported false by local and cfs (no native API yet — cfs is
	// blocked on a metanode inode-clone API). The executor checks
	// SameInstance AND this flag before attempting a server-side copy.
	ServerSideCopy bool

	// NativeModeWrite reports whether PutOptions.Mode is persisted such
	// that a subsequent Stat returns the same bits. local/cfs report true
	// (syscall.Chmod / mw.Setattr); s3 reports true via user-metadata
	// header but only round-trips through Stat (not List).
	NativeModeWrite bool

	// NativeOwnerWrite reports whether PutOptions.UID/GID are persisted.
	// All three in-tree backends report true; the s3 round-trip is
	// header-based (Stat only, not List).
	NativeOwnerWrite bool

	// NativeXattrWrite reports whether PutOptions.Xattrs survives Put +
	// Stat with full key/value fidelity. local/cfs report true; s3
	// reports true subject to the 2 KiB user-metadata budget (overflow
	// returns ErrMetadataTooLarge from Put).
	NativeXattrWrite bool
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

	// ErrMetadataTooLarge is returned by Put when the requested
	// PutOptions.Metadata + PutOptions.Mtime/Mode/UID/GID/Xattrs encoding
	// exceeds the backend-specific user-metadata budget (S3: 2 KiB total).
	// The executor maps this to the rule's OnMetadataUnsupported policy:
	// warn (default) records a counter and proceeds without metadata;
	// skip aborts the single object; error fails the whole rule.
	ErrMetadataTooLarge = errors.New("backend: metadata too large")
)
