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

// Package local implements the syncnode Backend interface against a local
// POSIX filesystem. It covers both plain local disks and host-mounted
// parallel filesystems (GPFS / Lustre / WekaFS / BeeGFS / Alluxio FUSE) —
// from syncnode's viewpoint they are all just a mount path.
//
// See design.md §3.4 (local kind config), §10.6 (Backend interface),
// §9 Phase C-4 (this implementation).
//
// Safety: every operation that touches a path resolves it under one of the
// configured AllowedRoots and rejects paths that escape via "..", absolute
// override, or symlink. This mirrors the strict policy described in §3.4.
package local

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// partialSuffix is the deterministic suffix appended to a destination key when
// Put writes data into the local FS. Put renames the partial file onto the
// destination on success; on failure (or mid-stream context cancellation) the
// partial is left behind so a subsequent Put with PutOptions.ResumeOffset > 0
// can pick up where the previous attempt left off.
//
// The suffix is deliberately distinctive ("syncnode" prefix) so operator
// sweeps can locate orphans, and so a future local.List filter can exclude
// them from the namespace.
const partialSuffix = ".syncnode.partial"

// kindName is the registered backend kind for local POSIX storage.
const kindName = "local"

// defaultBufferSize is used by Put when neither cfg.DefaultBufferSizeKiB nor
// opts.PartSizeMiB asks for something larger. 4 MiB is the same default the
// existing cfs-sync uses (see tool/cfs-sync/storage/local.go).
const defaultBufferSize = 4 * 1024 * 1024

// minBufferSize is the floor we enforce on the copy buffer. Anything smaller
// silently kneecaps throughput on parallel filesystems.
const minBufferSize = 64 * 1024

// checksumCacheCap caps the in-memory checksum cache. Beyond this, a random
// entry is evicted on insert. We don't run an LRU because:
//   - syncnode hits each key at most a handful of times per task,
//   - the cost of a missed entry is one extra streaming sha256, not a bug,
//   - a real LRU adds ~50 LoC for marginal benefit at this size.
const checksumCacheCap = 1000

// Config configures a local Backend. AllowedRoots are absolute paths every
// key must resolve under; an empty AllowedRoots list is rejected at
// construction time (mirrors design §3.4: "生产部署必须...强制验证").
//
// DefaultBufferSizeKiB / MaxDirDepth carry the syncnode.Config.Posix
// defaults. Per-endpoint overrides (BufferSizeKiB, Concurrency, DirectIO,
// FadviseSequential) are surfaced on PutOptions / GetOptions at call time,
// not on the Backend itself, so a single Backend can be shared across rules
// that point at the same Config.
//
// OnSymlink selects the rule's symlink policy (mirror of
// proto.SyncRuleConfig.OnSymlink). One of: "" / "skip" / "follow" / "error".
// "" is the back-compat alias for "skip"; New normalises it.
type Config struct {
	AllowedRoots         []string
	DefaultBufferSizeKiB int
	MaxDirDepth          int
	OnSymlink            string
}

// Symlink-policy constants. Mirrored from proto.SyncRuleConfig.OnSymlink /
// syncnode.validOnSymlink. Kept here so the backend doesn't import upward.
const (
	OnSymlinkSkip   = "skip"   // default; silently skip symlinks during List
	OnSymlinkFollow = "follow" // deref via os.Stat; final path must stay under AllowedRoots
	OnSymlinkError  = "error"  // emit backend.Entry{Err: ...} for each symlink
)

// Backend is the local-POSIX implementation of backend.Backend.
type Backend struct {
	allowedRoots []string // absolute, cleaned, with trailing separator stripped
	bufferSize   int      // bytes
	maxDirDepth  int
	// onSymlink is one of OnSymlinkSkip / OnSymlinkFollow / OnSymlinkError.
	// Empty config values are normalised to OnSymlinkSkip in New().
	onSymlink string

	// cache stores recently-computed sha256 checksums keyed by the absolute
	// resolved path. Entry validity is gated on (mtimeNs, size); on miss we
	// fall back to a streaming compute. A new sha256 returned by Put is
	// inserted directly so the immediately-following GetChecksum doesn't
	// have to re-read the file.
	cacheMu sync.RWMutex
	cache   map[string]checksumEntry
}

// checksumEntry is one row of the in-memory sha256 cache. mtimeNs+size form
// the staleness key — if either changes between Put and GetChecksum the
// entry is dropped and the file is re-hashed.
type checksumEntry struct {
	mtimeNs int64
	size    int64
	sum     string
}

func init() { backend.Register(kindName, New) }

// New constructs a local Backend from cfg (must be *Config).
func New(cfgI interface{}) (backend.Backend, error) {
	cfg, ok := cfgI.(*Config)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("%w: local backend expects *local.Config, got %T", backend.ErrConfigInvalid, cfgI)
	}
	if len(cfg.AllowedRoots) == 0 {
		return nil, fmt.Errorf("%w: local.Config.AllowedRoots must be non-empty", backend.ErrConfigInvalid)
	}

	roots := make([]string, 0, len(cfg.AllowedRoots))
	for _, r := range cfg.AllowedRoots {
		if r == "" {
			return nil, fmt.Errorf("%w: local.Config.AllowedRoots contains an empty entry", backend.ErrConfigInvalid)
		}
		abs, err := filepath.Abs(r)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve allowedRoot %q: %v", backend.ErrConfigInvalid, r, err)
		}
		// EvalSymlinks lets us reject configs that point AllowedRoots at a
		// symlink that itself escapes. If the directory doesn't exist yet
		// we tolerate that — operators may pre-create mounts later — and
		// fall back to filepath.Clean.
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			abs = resolved
		}
		roots = append(roots, filepath.Clean(abs))
	}

	bufSize := cfg.DefaultBufferSizeKiB * 1024
	if bufSize < minBufferSize {
		bufSize = defaultBufferSize
	}

	// Normalise the symlink policy: "" is the back-compat alias for
	// "skip" (the legacy hard-coded behaviour). Unknown values are
	// rejected here as a third defence line (config-load + executor's
	// validateTask catch them earlier, but a direct caller using
	// local.New could otherwise slip a typo through).
	onSym := cfg.OnSymlink
	switch onSym {
	case "":
		onSym = OnSymlinkSkip
	case OnSymlinkSkip, OnSymlinkFollow, OnSymlinkError:
		// ok
	default:
		return nil, fmt.Errorf("%w: local.Config.OnSymlink must be skip/follow/error, got %q", backend.ErrConfigInvalid, cfg.OnSymlink)
	}

	return &Backend{
		allowedRoots: roots,
		bufferSize:   bufSize,
		maxDirDepth:  cfg.MaxDirDepth,
		onSymlink:    onSym,
		cache:        make(map[string]checksumEntry, 64),
	}, nil
}

// Kind implements backend.Backend.
func (b *Backend) Kind() string { return kindName }

// Capabilities implements backend.Backend.
func (b *Backend) Capabilities() backend.Caps {
	return backend.Caps{
		RangeRead:         true,
		Multipart:         false,
		AtomicRename:      true,
		ListMaxKeys:       0, // unlimited
		StrongConsistency: true,
		// POSIX has no native checksum: GetChecksum streams the file. The
		// in-memory cache helps for repeat lookups but is not server-side.
		NativeChecksum: false,
		// POSIX honors PutOptions.Mtime via os.Chtimes after the rename.
		NativeMtimeWrite: true,
		// No server-side copy fast path today. A local-to-local same-instance
		// move could exploit os.Link / copy_file_range, but that's a separate
		// optimization tracked outside the rclone-gap roadmap and isn't
		// required for the cross-backend correctness story.
		ServerSideCopy: false,
		// POSIX metadata: linux/darwin builds honor mode (syscall.Chmod),
		// owner (os.Lchown), and xattr (unix.Lsetxattr). The non-unix stub
		// reports posixMetaSupported=false so we degrade gracefully.
		NativeModeWrite:  posixMetaSupported,
		NativeOwnerWrite: posixMetaSupported,
		NativeXattrWrite: posixMetaSupported,
		// P2 breakpoint resume: Put honors PutOptions.ResumeOffset by writing
		// into a deterministic `<dst>.syncnode.partial` and stitching the new
		// body onto the existing bytes. See p2-local-resume-fix.md.
		ResumeOffsetWrite: true,
	}
}

// Stat implements backend.Stater. Returns full POSIX metadata
// (mode/uid/gid/xattrs) alongside size/mtime. On platforms without
// xattr support (or filesystems without xattr enabled), Xattrs is nil
// and the rest is still populated.
func (b *Backend) Stat(ctx context.Context, key string) (backend.Stat, error) {
	path, err := b.resolveSafe(key, true)
	if err != nil {
		return backend.Stat{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return backend.Stat{}, backend.ErrKeyNotFound
		}
		return backend.Stat{}, fmt.Errorf("lstat %q: %w", path, err)
	}
	st := backend.Stat{
		Size:  info.Size(),
		Mtime: info.ModTime(),
	}
	mode, uid, gid, xattrs, err := readPosixMeta(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return backend.Stat{}, backend.ErrKeyNotFound
		}
		// readPosixMeta returns a hard error only for stat-level failures;
		// surface it so the executor can fail fast instead of writing a
		// destination with truncated metadata.
		return backend.Stat{}, fmt.Errorf("readPosixMeta %q: %w", path, err)
	}
	m := mode
	u := uid
	g := gid
	st.Mode = &m
	st.UID = &u
	st.GID = &g
	if len(xattrs) > 0 {
		st.Xattrs = xattrs
	}
	return st, nil
}

// SameInstance reports whether other is another local backend rooted at
// the exact same AllowedRoots list. Equality is positional — two backends
// configured with [/a, /b] and [/b, /a] are treated as distinct so we
// don't surprise an operator who reordered roots on purpose.
func (b *Backend) SameInstance(other backend.Backend) bool {
	o, ok := other.(*Backend)
	if !ok || o == nil {
		return false
	}
	if len(b.allowedRoots) != len(o.allowedRoots) {
		return false
	}
	for i, r := range b.allowedRoots {
		if r != o.allowedRoots[i] {
			return false
		}
	}
	return true
}

// ServerSideCopy is declared so local.Backend satisfies the
// ServerSideCopier interface for symmetry with s3.Backend. Local doesn't
// expose a server-side path today, so it returns ErrBackendUnsupported —
// the executor short-circuits on Caps.ServerSideCopy long before reaching
// here.
func (b *Backend) ServerSideCopy(_ context.Context, _, _ string, _ backend.PutOptions) (backend.PutResult, error) {
	return backend.PutResult{}, backend.ErrBackendUnsupported
}

// Close implements backend.Backend. Local backend has no resources to free.
func (b *Backend) Close() error { return nil }

// -----------------------------------------------------------------------
// Path safety helpers
// -----------------------------------------------------------------------

// resolveSafe turns a user-supplied key into an absolute path under one of
// the AllowedRoots. mustExist controls how we handle the missing-file case:
//
//   - mustExist=true  : the path must exist; EvalSymlinks resolves the full
//     path, including any intermediate symlinks. Returns ErrKeyNotFound if
//     the path is missing.
//   - mustExist=false : the path may not exist yet (e.g. Put target). We
//     EvalSymlinks the longest existing prefix and lexically clean the
//     remainder; the result must still be under an AllowedRoot.
//
// In both modes, traversal that escapes (".." or symlink target outside
// AllowedRoots) is rejected with ErrConfigInvalid.
func (b *Backend) resolveSafe(key string, mustExist bool) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: empty key", backend.ErrConfigInvalid)
	}
	// Reject NUL bytes — those crash some filesystems before we even
	// reach the syscall.
	if strings.ContainsRune(key, 0) {
		return "", fmt.Errorf("%w: key contains NUL byte", backend.ErrConfigInvalid)
	}

	// Always anchor under the first AllowedRoot. The key is *always*
	// interpreted relative to AllowedRoots[0]; callers that need to read
	// from a different root configure a separate Backend (or use multiple
	// allowed roots and a key that starts with the desired root, which we
	// also tolerate below).
	clean := filepath.Clean(key)
	var candidate string
	if filepath.IsAbs(clean) {
		candidate = clean
	} else {
		candidate = filepath.Join(b.allowedRoots[0], clean)
	}
	candidate = filepath.Clean(candidate)

	// Phase 1: lexical check against AllowedRoots. This is a fast reject
	// for obvious ".." traversal that escapes before we hit the FS.
	if !b.underAllowedRoot(candidate) {
		return "", fmt.Errorf("%w: path %q is outside allowedRoots %v", backend.ErrConfigInvalid, candidate, b.allowedRoots)
	}

	// Phase 2: resolve symlinks. If the file is missing we walk back to
	// the longest existing prefix and EvalSymlinks that, then re-join the
	// missing tail (which must not contain ".." after Clean — already
	// enforced above).
	resolved, err := filepath.EvalSymlinks(candidate)
	if err == nil {
		// Resolved successfully. Verify resolved path is still under an
		// AllowedRoot — this catches symlinks that point outside.
		if !b.underAllowedRoot(resolved) {
			return "", fmt.Errorf("%w: path %q resolves via symlink to %q which is outside allowedRoots", backend.ErrConfigInvalid, candidate, resolved)
		}
		return resolved, nil
	}
	if !os.IsNotExist(err) {
		// Unexpected error (permission, IO). Surface it.
		return "", fmt.Errorf("eval symlinks for %q: %w", candidate, err)
	}

	// File is missing. If the caller demanded existence, signal ENOENT.
	if mustExist {
		return "", backend.ErrKeyNotFound
	}

	// Walk back to the deepest existing ancestor and EvalSymlinks that —
	// otherwise a symlinked parent directory could let us write into an
	// off-roots target.
	existing := candidate
	missing := ""
	for {
		if _, statErr := os.Lstat(existing); statErr == nil {
			break
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			break // hit FS root without finding any existing prefix
		}
		missing = filepath.Join(filepath.Base(existing), missing)
		existing = parent
	}
	resolvedParent, err := filepath.EvalSymlinks(existing)
	if err != nil {
		// If even the FS root cannot be resolved we treat the whole
		// candidate as outside.
		return "", fmt.Errorf("%w: cannot resolve any existing ancestor of %q", backend.ErrConfigInvalid, candidate)
	}
	final := filepath.Clean(filepath.Join(resolvedParent, missing))
	if !b.underAllowedRoot(final) {
		return "", fmt.Errorf("%w: path %q resolves to %q which is outside allowedRoots", backend.ErrConfigInvalid, candidate, final)
	}
	return final, nil
}

// underAllowedRoot reports whether p is equal to or a descendant of one of
// the configured AllowedRoots. Comparison is lexical on already-cleaned
// paths; callers must clean p first.
func (b *Backend) underAllowedRoot(p string) bool {
	for _, root := range b.allowedRoots {
		if p == root {
			return true
		}
		if strings.HasPrefix(p, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------
// Data plane
// -----------------------------------------------------------------------

// Get implements backend.Backend.
func (b *Backend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	path, err := b.resolveSafe(key, true)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, backend.ErrKeyNotFound
		}
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("seek %q to %d: %w", path, off, err)
		}
	}
	if size > 0 {
		return &limitedFile{f: f, r: io.LimitReader(f, size)}, nil
	}
	return f, nil
}

// Head implements backend.Backend.
func (b *Backend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	path, err := b.resolveSafe(key, true)
	if err != nil {
		return 0, "", time.Time{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", time.Time{}, backend.ErrKeyNotFound
		}
		return 0, "", time.Time{}, fmt.Errorf("stat %q: %w", path, err)
	}
	// POSIX has no etag.
	return info.Size(), "", info.ModTime(), nil
}

// Put implements backend.Backend. The write is atomic at the destination: bytes
// go into a deterministic partial file (`<dst>.syncnode.partial`) and only
// once the body finishes do we os.Rename the partial onto the destination.
//
// Failure semantics differ from a transient temp file: on copy / close / write
// errors we LEAVE the partial behind so a subsequent Put with
// PutOptions.ResumeOffset > 0 can stitch the rest of the body onto the
// already-written prefix. Operators (or a future GC sweep) clean up stale
// partials.
//
// PutOptions.ResumeOffset semantics:
//   - 0 (default): open the partial with O_TRUNC|O_CREATE, write the whole
//     body, rename to dst. Equivalent to the legacy atomic-rename behaviour.
//   - > 0: open the existing partial (Stat must report size >= ResumeOffset,
//     otherwise we return ErrConfigInvalid because the breakpoint is stale).
//     Seek to ResumeOffset, write body, rename to dst. The body must contain
//     exactly the suffix [ResumeOffset, total) — the executor guarantees this
//     by calling t.Src.Get(key, ResumeOffset, 0) upstream.
//
// When opts.ComputeChecksum is true, the body stream is tee'd through a
// sha256 hasher during the copy so the checksum costs no extra read for the
// non-resume path. For a resume Put we first hash the existing partial's
// [0, ResumeOffset) prefix into the same sink, then tee the body — so
// PutResult.Checksum is the sha256 over the WHOLE assembled file (callers
// expect a whole-file digest, not the suffix-only digest).
//
// The resulting digest (whole-file or non-resume single-shot) is returned in
// PutResult.Checksum (algorithm "sha256") and primed into the in-memory cache
// so a subsequent GetChecksum is O(1).
func (b *Backend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	path, err := b.resolveSafe(key, false)
	if err != nil {
		return backend.PutResult{}, err
	}

	// Ensure the parent directory exists and is itself inside AllowedRoots
	// (resolveSafe already enforced the parent's resolved location is
	// safe — we re-verify after MkdirAll in case the directory was just
	// created as a symlink by something racing us).
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return backend.PutResult{}, fmt.Errorf("mkdir parent of %q: %w", path, err)
	}
	if resolvedParent, perr := filepath.EvalSymlinks(parent); perr == nil {
		if !b.underAllowedRoot(filepath.Clean(resolvedParent)) {
			return backend.PutResult{}, fmt.Errorf("%w: parent %q resolves outside allowedRoots", backend.ErrConfigInvalid, parent)
		}
	}

	// Pick the copy buffer. opts.PartSizeMiB wins, otherwise Backend
	// default. We never go below minBufferSize.
	bufSize := b.bufferSize
	if opts.PartSizeMiB > 0 {
		bufSize = opts.PartSizeMiB * 1024 * 1024
	}
	if bufSize < minBufferSize {
		bufSize = minBufferSize
	}

	if opts.ResumeOffset < 0 {
		return backend.PutResult{}, fmt.Errorf("%w: ResumeOffset must be >= 0, got %d", backend.ErrConfigInvalid, opts.ResumeOffset)
	}

	partial := path + partialSuffix

	// Prepare the sha256 sink up front so we can prefill it with the existing
	// partial bytes BEFORE tee'ing the body. For non-resume Puts this is a
	// straight Tee of body; the prefill loop is skipped because the partial
	// gets truncated.
	var (
		hashSink  io.Writer
		sumFn     func() string
		teeBody   io.Reader = body
		startSize int64
	)
	if opts.ComputeChecksum {
		h, fn := backend.NewSHA256Sink()
		hashSink = h
		sumFn = fn
	}

	var f *os.File
	if opts.ResumeOffset > 0 {
		// Stale-breakpoint guard: the partial must exist and be at least as
		// long as ResumeOffset, otherwise the caller's breakpoint claim is
		// inconsistent and continuing would write at a hole or past the end
		// (Seek past end on POSIX is legal but yields a sparse file with
		// undefined content in the gap — exactly the kind of silent
		// corruption we are trying to fix).
		info, statErr := os.Stat(partial)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return backend.PutResult{}, fmt.Errorf("%w: resume requested at offset %d but partial %q does not exist",
					backend.ErrConfigInvalid, opts.ResumeOffset, partial)
			}
			return backend.PutResult{}, fmt.Errorf("stat partial %q: %w", partial, statErr)
		}
		if info.Size() < opts.ResumeOffset {
			return backend.PutResult{}, fmt.Errorf("%w: stale partial %q size=%d < ResumeOffset=%d",
				backend.ErrConfigInvalid, partial, info.Size(), opts.ResumeOffset)
		}
		// Truncate any junk past ResumeOffset (e.g. the previous attempt
		// flushed extra bytes the caller's breakpoint did not yet record).
		// This keeps the post-write file exactly [0, ResumeOffset)+body.
		if info.Size() > opts.ResumeOffset {
			if err := os.Truncate(partial, opts.ResumeOffset); err != nil {
				return backend.PutResult{}, fmt.Errorf("truncate partial %q to %d: %w", partial, opts.ResumeOffset, err)
			}
		}
		startSize = opts.ResumeOffset

		// For ComputeChecksum, hash the existing prefix into the same sink
		// BEFORE writing new bytes. Open a read-only handle to the partial,
		// CopyN(prefix into hashSink), close — then open the write handle.
		// Splitting read/write handles is intentional: the write handle
		// seeks past the prefix and we do not want a shared file offset to
		// confuse the two flows.
		if hashSink != nil {
			rh, oerr := os.Open(partial)
			if oerr != nil {
				return backend.PutResult{}, fmt.Errorf("open partial for checksum prefix %q: %w", partial, oerr)
			}
			if _, cerr := io.CopyN(hashSink, rh, opts.ResumeOffset); cerr != nil {
				_ = rh.Close()
				return backend.PutResult{}, fmt.Errorf("hash partial prefix %q [0,%d): %w", partial, opts.ResumeOffset, cerr)
			}
			_ = rh.Close()
		}

		// Open the partial for append-style write at the resume offset. We
		// do not use O_APPEND because POSIX O_APPEND ignores Seek; an
		// explicit Seek + Write gives us deterministic positioning even if
		// some libc inserts a hidden seek-to-end before the write.
		wh, oerr := os.OpenFile(partial, os.O_WRONLY, 0o644)
		if oerr != nil {
			return backend.PutResult{}, fmt.Errorf("open partial for resume write %q: %w", partial, oerr)
		}
		if _, serr := wh.Seek(opts.ResumeOffset, io.SeekStart); serr != nil {
			_ = wh.Close()
			return backend.PutResult{}, fmt.Errorf("seek partial %q to %d: %w", partial, opts.ResumeOffset, serr)
		}
		f = wh
	} else {
		// Fresh write. O_TRUNC clobbers any stale partial left over from a
		// previous failed Put that the caller did not intend to resume
		// (e.g. ResumeOffset deliberately zeroed). We intentionally do NOT
		// use O_EXCL because a stale partial is the expected resume state
		// and we want to overwrite it cleanly when the caller asks for a
		// fresh start.
		wh, oerr := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if oerr != nil {
			return backend.PutResult{}, fmt.Errorf("create partial %q: %w", partial, oerr)
		}
		f = wh
	}

	if hashSink != nil {
		teeBody = io.TeeReader(body, hashSink)
	}

	written, cerr := copyWithBufferCtx(ctx, f, teeBody, bufSize)
	if cerr != nil {
		_ = f.Close()
		// Intentionally LEAVE the partial behind. Resume hinges on the
		// caller being able to find this file with at least
		// (startSize + written) bytes durably on disk.
		return backend.PutResult{}, fmt.Errorf("copy to %q: %w", partial, cerr)
	}
	if err := f.Close(); err != nil {
		// Same rationale: partial stays on close error.
		return backend.PutResult{}, fmt.Errorf("close partial %q: %w", partial, err)
	}
	if err := os.Rename(partial, path); err != nil {
		return backend.PutResult{}, fmt.Errorf("rename %q -> %q: %w", partial, path, err)
	}
	// Effective bytes-put includes the resumed prefix so callers comparing
	// BytesPut against entry.Size still see equality.
	written += startSize

	// Apply POSIX metadata (mode / uid+gid / xattrs) BEFORE touching atime/mtime
	// so the final Chtimes wins (chmod/chown/setxattr all bump ctime but only
	// some bump mtime depending on the filesystem). A failure here is
	// propagated because the caller explicitly asked for the metadata to be
	// preserved; degrading silently would surprise callers comparing
	// destination state against source.
	if opts.Mode != nil || opts.UID != nil || opts.GID != nil || len(opts.Xattrs) > 0 {
		if err := applyPosixMeta(path, opts.Mode, opts.UID, opts.GID, opts.Xattrs); err != nil {
			return backend.PutResult{}, fmt.Errorf("apply posix metadata %q: %w", path, err)
		}
	}

	// Preserve source mtime if requested. atime is set to now to avoid
	// stamping the access time of the just-written file with a stale value;
	// callers that need atime preserved would need a separate option. A
	// Chtimes failure is propagated because the caller explicitly asked for
	// the mtime to be honored and a silent fallthrough would be worse than a
	// clear error (and would break checksum/idempotency comparisons that key
	// off mtime).
	if opts.Mtime != nil {
		now := time.Now()
		if err := os.Chtimes(path, now, *opts.Mtime); err != nil {
			return backend.PutResult{}, fmt.Errorf("chtimes %q: %w", path, err)
		}
	}

	res := backend.PutResult{BytesPut: written}
	if sumFn != nil {
		res.Checksum = sumFn()
		res.Algorithm = backend.ChecksumAlgorithmSHA256
		// Best-effort prime the cache. We Stat the just-renamed file to
		// pick up the canonical mtime; if Stat fails (extremely unlikely
		// since rename succeeded) we just skip caching.
		if info, statErr := os.Stat(path); statErr == nil {
			b.cachePut(path, checksumEntry{
				mtimeNs: info.ModTime().UnixNano(),
				size:    info.Size(),
				sum:     res.Checksum,
			})
		}
	}
	return res, nil
}

// GetChecksum implements backend.Backend. POSIX has no native checksum so we
// stream the file through sha256. A small in-memory cache keyed on
// (path, mtimeNs, size) keeps repeat lookups O(1); on miss we hash and
// insert.
//
// Returns ErrKeyNotFound when key does not exist.
func (b *Backend) GetChecksum(_ context.Context, key string) (string, string, error) {
	path, err := b.resolveSafe(key, true)
	if err != nil {
		return "", "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", "", backend.ErrKeyNotFound
		}
		return "", "", fmt.Errorf("stat %q: %w", path, err)
	}
	mtimeNs := info.ModTime().UnixNano()
	size := info.Size()

	if cached, ok := b.cacheLookup(path, mtimeNs, size); ok {
		return cached, backend.ChecksumAlgorithmSHA256, nil
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", "", backend.ErrKeyNotFound
		}
		return "", "", fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()
	sum, _, err := backend.SHA256Stream(f)
	if err != nil {
		return "", "", fmt.Errorf("sha256 %q: %w", path, err)
	}
	b.cachePut(path, checksumEntry{mtimeNs: mtimeNs, size: size, sum: sum})
	return sum, backend.ChecksumAlgorithmSHA256, nil
}

// cacheLookup returns the cached sha256 for path if (mtimeNs, size) match.
func (b *Backend) cacheLookup(path string, mtimeNs, size int64) (string, bool) {
	b.cacheMu.RLock()
	e, ok := b.cache[path]
	b.cacheMu.RUnlock()
	if !ok || e.mtimeNs != mtimeNs || e.size != size {
		return "", false
	}
	return e.sum, true
}

// cachePut inserts e for path. When the cache is at capacity, one entry is
// evicted before insert. Map iteration order in Go is unspecified so taking
// the first key amounts to a cheap pseudo-random eviction — adequate for
// this workload (see checksumCacheCap comment).
func (b *Backend) cachePut(path string, e checksumEntry) {
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if len(b.cache) >= checksumCacheCap {
		for k := range b.cache {
			delete(b.cache, k)
			break
		}
	}
	b.cache[path] = e
}

// Delete implements backend.Backend. Deleting a missing key is not an error.
func (b *Backend) Delete(ctx context.Context, key string) error {
	path, err := b.resolveSafe(key, false)
	if err != nil {
		// If the lookup failed because the path doesn't exist (which
		// resolveSafe(false) returns via the missing-ancestor path), we
		// still treat that as success. But ErrConfigInvalid (escape) is
		// always returned.
		if errors.Is(err, backend.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %q: %w", path, err)
	}
	return nil
}

// Rename implements backend.Backend. Both keys are resolved under
// AllowedRoots. Same-filesystem renames are atomic; cross-filesystem
// renames fail (we don't fall back to copy+delete here — callers that
// need that can compose Get/Put themselves).
func (b *Backend) Rename(ctx context.Context, oldKey, newKey string) error {
	oldPath, err := b.resolveSafe(oldKey, true)
	if err != nil {
		return err
	}
	newPath, err := b.resolveSafe(newKey, false)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
		return fmt.Errorf("mkdir parent of %q: %w", newPath, err)
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("rename %q -> %q: %w", oldPath, newPath, err)
	}
	return nil
}

// List implements backend.Backend. The channel is closed when traversal
// completes; on error a final Entry with Err set is emitted before close.
func (b *Backend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	base, err := b.resolveSafe(prefix, true)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, backend.ErrKeyNotFound
		}
		return nil, fmt.Errorf("stat %q: %w", base, err)
	}

	ch := make(chan backend.Entry, 64)
	go func() {
		defer close(ch)
		if !info.IsDir() {
			// Single-file prefix: emit it and stop.
			emit(ctx, ch, backend.Entry{
				Key:   prefix,
				Size:  info.Size(),
				Mtime: info.ModTime(),
			})
			return
		}
		if recursive {
			b.walkRecursive(ctx, base, prefix, ch)
		} else {
			b.walkShallow(ctx, base, prefix, ch)
		}
	}()
	return ch, nil
}

func (b *Backend) walkShallow(ctx context.Context, base, prefix string, ch chan<- backend.Entry) {
	entries, err := os.ReadDir(base)
	if err != nil {
		emit(ctx, ch, backend.Entry{Err: fmt.Errorf("readdir %q: %w", base, err)})
		return
	}
	for _, de := range entries {
		select {
		case <-ctx.Done():
			emit(ctx, ch, backend.Entry{Err: ctx.Err()})
			return
		default:
		}
		// Hide our own resume scratch files. They are an implementation
		// detail of Put's deterministic-partial design; surfacing them
		// would force every caller (executor, dashboard, retention,
		// contract tests) to know to filter them.
		if strings.HasSuffix(de.Name(), partialSuffix) {
			continue
		}
		absPath := filepath.Join(base, de.Name())
		key := joinKey(prefix, de.Name())

		// Symlink handling. de.Type() reflects os.Lstat semantics so it
		// identifies the link itself, not its target.
		if de.Type()&os.ModeSymlink != 0 {
			switch b.onSymlink {
			case OnSymlinkSkip:
				continue
			case OnSymlinkError:
				emit(ctx, ch, backend.Entry{
					Key: key,
					Err: fmt.Errorf("symlink %q encountered with onSymlink=error policy", absPath),
				})
				continue
			case OnSymlinkFollow:
				// Deref to the target via os.Stat. Reject if the target
				// resolves outside AllowedRoots — we never let List leak
				// out-of-roots data.
				resolved, terr := filepath.EvalSymlinks(absPath)
				if terr != nil {
					emit(ctx, ch, backend.Entry{
						Key: key,
						Err: fmt.Errorf("follow symlink %q: %w", absPath, terr),
					})
					continue
				}
				if !b.underAllowedRoot(filepath.Clean(resolved)) {
					emit(ctx, ch, backend.Entry{
						Key: key,
						Err: fmt.Errorf("%w: symlink %q resolves to %q outside allowedRoots", backend.ErrConfigInvalid, absPath, resolved),
					})
					continue
				}
				info, terr := os.Stat(absPath) // deref
				if terr != nil {
					emit(ctx, ch, backend.Entry{
						Key: key,
						Err: fmt.Errorf("stat-deref %q: %w", absPath, terr),
					})
					continue
				}
				entry := backend.Entry{
					Key:   key,
					Size:  info.Size(),
					Mtime: info.ModTime(),
					IsDir: info.IsDir(),
				}
				if info.IsDir() {
					entry.Size = 0
				} else {
					if etag, merr := fileMD5(resolved); merr == nil {
						entry.ETag = etag
					}
				}
				if !emit(ctx, ch, entry) {
					return
				}
				continue
			}
		}

		info, err := de.Info()
		if err != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("stat %q: %w", de.Name(), err)})
			continue
		}
		entry := backend.Entry{
			Key:   key,
			Size:  info.Size(),
			Mtime: info.ModTime(),
			IsDir: de.IsDir(),
		}
		if de.IsDir() {
			entry.Size = 0
		} else {
			if etag, merr := fileMD5(absPath); merr == nil {
				entry.ETag = etag
			}
		}
		if !emit(ctx, ch, entry) {
			return
		}
	}
}

func (b *Backend) walkRecursive(ctx context.Context, base, prefix string, ch chan<- backend.Entry) {
	walkErr := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// Surface the error but keep walking siblings.
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("walk %q: %w", p, err)})
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Skip the base directory itself (no policy applies to it).
		if p == base {
			return nil
		}

		// Hide our own resume scratch files. See walkShallow for rationale.
		if strings.HasSuffix(d.Name(), partialSuffix) {
			return nil
		}

		// Symlink handling per b.onSymlink. d.Type() honours Lstat semantics.
		if d.Type()&os.ModeSymlink != 0 {
			rel, relErr := filepath.Rel(base, p)
			if relErr != nil {
				emit(ctx, ch, backend.Entry{Err: fmt.Errorf("relpath %q: %w", p, relErr)})
				return nil
			}
			key := joinKey(prefix, filepath.ToSlash(rel))
			switch b.onSymlink {
			case OnSymlinkSkip:
				return nil
			case OnSymlinkError:
				emit(ctx, ch, backend.Entry{
					Key: key,
					Err: fmt.Errorf("symlink %q encountered with onSymlink=error policy", p),
				})
				return nil
			case OnSymlinkFollow:
				if ferr := b.followAndEmit(ctx, p, key, base, prefix, ch); ferr != nil {
					emit(ctx, ch, backend.Entry{Key: key, Err: ferr})
				}
				return nil
			}
		}

		// Enforce maxDirDepth if configured. Depth is the number of path
		// separators between base and p.
		if b.maxDirDepth > 0 {
			rel, _ := filepath.Rel(base, p)
			depth := strings.Count(filepath.ToSlash(rel), "/") + 1
			if depth > b.maxDirDepth {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		info, err := d.Info()
		if err != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("stat %q: %w", p, err)})
			return nil
		}
		rel, err := filepath.Rel(base, p)
		if err != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("relpath %q: %w", p, err)})
			return nil
		}
		key := joinKey(prefix, filepath.ToSlash(rel))
		entry := backend.Entry{
			Key:   key,
			Size:  info.Size(),
			Mtime: info.ModTime(),
			IsDir: d.IsDir(),
		}
		if d.IsDir() {
			entry.Size = 0
		} else {
			if etag, merr := fileMD5(p); merr == nil {
				entry.ETag = etag
			}
		}
		if !emit(ctx, ch, entry) {
			return filepath.SkipAll
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, context.Canceled) && !errors.Is(walkErr, context.DeadlineExceeded) {
		emit(ctx, ch, backend.Entry{Err: walkErr})
	}
}

// followAndEmit dereferences a symlink at p (with key `key` in the listing
// space) and emits the target. When the target is a directory, the function
// recurses via filepath.WalkDir on the *resolved* path so symlinked
// directories appear in the listing as a flattened tree under key.
//
// The resolved final path must live under one of the AllowedRoots — this
// keeps the design's "follow may resolve across boundaries, but the final
// resting path must stay inside the union" invariant.
func (b *Backend) followAndEmit(ctx context.Context, p, key, walkBase, walkPrefix string, ch chan<- backend.Entry) error {
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		return fmt.Errorf("follow symlink %q: %w", p, err)
	}
	resolved = filepath.Clean(resolved)
	if !b.underAllowedRoot(resolved) {
		return fmt.Errorf("%w: symlink %q resolves to %q outside allowedRoots", backend.ErrConfigInvalid, p, resolved)
	}
	info, err := os.Stat(p)
	if err != nil {
		return fmt.Errorf("stat-deref %q: %w", p, err)
	}
	if !info.IsDir() {
		entry := backend.Entry{
			Key:   key,
			Size:  info.Size(),
			Mtime: info.ModTime(),
			IsDir: false,
		}
		if etag, merr := fileMD5(resolved); merr == nil {
			entry.ETag = etag
		}
		if !emit(ctx, ch, entry) {
			return nil
		}
		return nil
	}
	// Emit the dir itself.
	if !emit(ctx, ch, backend.Entry{
		Key:   key,
		Size:  0,
		Mtime: info.ModTime(),
		IsDir: true,
	}) {
		return nil
	}
	// Recurse into resolved using its own onSymlink behaviour (still b.onSymlink).
	// We use WalkDir on the resolved path so nested symlinks are subject to
	// the same policy. We re-base under the symlinked key.
	subWalkErr := filepath.WalkDir(resolved, func(rp string, rd fs.DirEntry, werr error) error {
		if werr != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("walk %q: %w", rp, werr)})
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if rp == resolved {
			return nil
		}
		rel, rerr := filepath.Rel(resolved, rp)
		if rerr != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("relpath %q: %w", rp, rerr)})
			return nil
		}
		subKey := joinKey(key, filepath.ToSlash(rel))
		if rd.Type()&os.ModeSymlink != 0 {
			switch b.onSymlink {
			case OnSymlinkSkip:
				return nil
			case OnSymlinkError:
				emit(ctx, ch, backend.Entry{
					Key: subKey,
					Err: fmt.Errorf("symlink %q encountered with onSymlink=error policy", rp),
				})
				return nil
			case OnSymlinkFollow:
				if ferr := b.followAndEmit(ctx, rp, subKey, resolved, key, ch); ferr != nil {
					emit(ctx, ch, backend.Entry{Key: subKey, Err: ferr})
				}
				return nil
			}
		}
		if b.maxDirDepth > 0 {
			depth := strings.Count(filepath.ToSlash(rel), "/") + 1
			if depth > b.maxDirDepth {
				if rd.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		info, ierr := rd.Info()
		if ierr != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("stat %q: %w", rp, ierr)})
			return nil
		}
		entry := backend.Entry{
			Key:   subKey,
			Size:  info.Size(),
			Mtime: info.ModTime(),
			IsDir: rd.IsDir(),
		}
		if rd.IsDir() {
			entry.Size = 0
		} else {
			if etag, merr := fileMD5(rp); merr == nil {
				entry.ETag = etag
			}
		}
		if !emit(ctx, ch, entry) {
			return filepath.SkipAll
		}
		return nil
	})
	if subWalkErr != nil && !errors.Is(subWalkErr, context.Canceled) && !errors.Is(subWalkErr, context.DeadlineExceeded) {
		emit(ctx, ch, backend.Entry{Err: subWalkErr})
	}
	return nil
}

// fileMD5 computes the hex-encoded MD5 checksum of the named file. Used to
// produce a content-based ETag for local POSIX entries so the sync executor
// can perform idempotency checks against S3 ETags.
func fileMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New() //nolint:gosec
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// joinKey appends child to prefix using "/" as the separator (object-store
// style — the executor speaks slash-keys regardless of OS).
func joinKey(prefix, child string) string {
	prefix = strings.TrimRight(filepath.ToSlash(prefix), "/")
	child = strings.TrimLeft(filepath.ToSlash(child), "/")
	if prefix == "" {
		return child
	}
	if child == "" {
		return prefix
	}
	return prefix + "/" + child
}

// emit sends e on ch respecting ctx cancellation. Returns false if the
// caller should stop walking (ctx cancelled).
func emit(ctx context.Context, ch chan<- backend.Entry, e backend.Entry) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- e:
		return true
	}
}

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

// copyWithBufferCtx is io.Copy with an explicit buffer size *and* context
// cancellation between chunks. Plain io.Copy / io.CopyBuffer honour
// ReaderFrom on the destination, which on *os.File ignores our buffer and
// uses its own 32 KiB — that defeats batching for sources like
// io.LimitReader over a network socket. Returns the number of bytes written
// to dst on success.
func copyWithBufferCtx(ctx context.Context, dst io.Writer, src io.Reader, bufSize int) (int64, error) {
	buf := make([]byte, bufSize)
	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			wn, werr := dst.Write(buf[:n])
			written += int64(wn)
			if werr != nil {
				return written, werr
			}
		}
		if rerr == io.EOF {
			return written, nil
		}
		if rerr != nil {
			return written, rerr
		}
	}
}

// limitedFile wraps a *os.File so io.LimitReader bounds reads but Close
// still releases the file descriptor.
type limitedFile struct {
	f *os.File
	r io.Reader
}

func (l *limitedFile) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitedFile) Close() error               { return l.f.Close() }
