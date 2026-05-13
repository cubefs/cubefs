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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// kindName is the registered backend kind for local POSIX storage.
const kindName = "local"

// defaultBufferSize is used by Put when neither cfg.DefaultBufferSizeKiB nor
// opts.PartSizeMiB asks for something larger. 4 MiB is the same default the
// existing cfs-sync uses (see tool/cfs-sync/storage/local.go).
const defaultBufferSize = 4 * 1024 * 1024

// minBufferSize is the floor we enforce on the copy buffer. Anything smaller
// silently kneecaps throughput on parallel filesystems.
const minBufferSize = 64 * 1024

// Config configures a local Backend. AllowedRoots are absolute paths every
// key must resolve under; an empty AllowedRoots list is rejected at
// construction time (mirrors design §3.4: "生产部署必须...强制验证").
//
// DefaultBufferSizeKiB / MaxDirDepth carry the syncnode.Config.Posix
// defaults. Per-endpoint overrides (BufferSizeKiB, Concurrency, DirectIO,
// FadviseSequential) are surfaced on PutOptions / GetOptions at call time,
// not on the Backend itself, so a single Backend can be shared across rules
// that point at the same Config.
type Config struct {
	AllowedRoots         []string
	DefaultBufferSizeKiB int
	MaxDirDepth          int
}

// Backend is the local-POSIX implementation of backend.Backend.
type Backend struct {
	allowedRoots []string // absolute, cleaned, with trailing separator stripped
	bufferSize   int      // bytes
	maxDirDepth  int
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

	return &Backend{
		allowedRoots: roots,
		bufferSize:   bufSize,
		maxDirDepth:  cfg.MaxDirDepth,
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
	}
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

// Put implements backend.Backend. The write is atomic: bytes go into a temp
// file alongside the destination and we os.Rename into place on success.
// A failed Put leaves no half-written destination, only an orphan temp file
// that operators can sweep.
func (b *Backend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (string, error) {
	path, err := b.resolveSafe(key, false)
	if err != nil {
		return "", err
	}

	// Ensure the parent directory exists and is itself inside AllowedRoots
	// (resolveSafe already enforced the parent's resolved location is
	// safe — we re-verify after MkdirAll in case the directory was just
	// created as a symlink by something racing us).
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("mkdir parent of %q: %w", path, err)
	}
	if resolvedParent, perr := filepath.EvalSymlinks(parent); perr == nil {
		if !b.underAllowedRoot(filepath.Clean(resolvedParent)) {
			return "", fmt.Errorf("%w: parent %q resolves outside allowedRoots", backend.ErrConfigInvalid, parent)
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

	tmpName, err := tempName(path)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0o644)
	if err != nil {
		return "", fmt.Errorf("create temp %q: %w", tmpName, err)
	}
	cleanup := func() { _ = os.Remove(tmpName) }

	if cerr := copyWithBufferCtx(ctx, f, body, bufSize); cerr != nil {
		_ = f.Close()
		cleanup()
		return "", fmt.Errorf("copy to %q: %w", tmpName, cerr)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", fmt.Errorf("close temp %q: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return "", fmt.Errorf("rename %q -> %q: %w", tmpName, path, err)
	}
	return "", nil
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
		info, err := de.Info()
		if err != nil {
			emit(ctx, ch, backend.Entry{Err: fmt.Errorf("stat %q: %w", de.Name(), err)})
			continue
		}
		key := joinKey(prefix, de.Name())
		entry := backend.Entry{
			Key:   key,
			Size:  info.Size(),
			Mtime: info.ModTime(),
			IsDir: de.IsDir(),
		}
		if de.IsDir() {
			entry.Size = 0
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
		// Skip symlinks (don't follow). For regular files / dirs continue.
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		// Skip the base directory itself.
		if p == base {
			return nil
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

// tempName returns a sibling path with the form
// "<dst>.tmp.<16-hex>" — atomic with os.Rename on the same filesystem.
func tempName(dst string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("tempName: %w", err)
	}
	return dst + ".tmp." + hex.EncodeToString(b[:]), nil
}

// copyWithBufferCtx is io.Copy with an explicit buffer size *and* context
// cancellation between chunks. Plain io.Copy / io.CopyBuffer honour
// ReaderFrom on the destination, which on *os.File ignores our buffer and
// uses its own 32 KiB — that defeats batching for sources like
// io.LimitReader over a network socket.
func copyWithBufferCtx(ctx context.Context, dst io.Writer, src io.Reader, bufSize int) error {
	buf := make([]byte, bufSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
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
