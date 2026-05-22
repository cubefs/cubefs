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

package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

// newBackend constructs a local Backend rooted at a fresh t.TempDir(). The
// AllowedRoot is resolved through EvalSymlinks (matching New's behaviour)
// so the caller can compare against it without surprises on macOS where
// /tmp is a symlink to /private/tmp.
func newBackend(t *testing.T) (backend.Backend, string) {
	t.Helper()
	root := t.TempDir()
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", root, err)
	}
	b, err := New(&Config{
		AllowedRoots:         []string{root},
		DefaultBufferSizeKiB: 4096,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, resolved
}

// putAndCheck Puts size random bytes under key and Gets them back, asserting
// byte-for-byte equality.
func putAndCheck(t *testing.T, b backend.Backend, key string, size int) {
	t.Helper()
	src := make([]byte, size)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if _, err := b.Put(context.Background(), key, bytes.NewReader(src), int64(size), backend.PutOptions{}); err != nil {
		t.Fatalf("Put(%s, %d bytes): %v", key, size, err)
	}
	rc, err := b.Get(context.Background(), key, 0, 0)
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%s): %v", key, err)
	}
	if len(got) != size {
		t.Fatalf("Get(%s): len = %d, want %d", key, len(got), size)
	}
	if sha256.Sum256(got) != sha256.Sum256(src) {
		t.Fatalf("Get(%s): contents mismatch", key)
	}
}

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------

func TestNew_RejectsEmptyAllowedRoots(t *testing.T) {
	t.Parallel()
	if _, err := New(&Config{}); err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid for empty AllowedRoots, got %v", err)
	}
	if _, err := New(&Config{AllowedRoots: []string{""}}); err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid for empty-string AllowedRoot, got %v", err)
	}
}

func TestNew_RejectsWrongConfigType(t *testing.T) {
	t.Parallel()
	if _, err := New("not a config"); err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid for wrong type, got %v", err)
	}
	if _, err := New(nil); err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid for nil cfg, got %v", err)
	}
}

func TestNew_RegistryEntry(t *testing.T) {
	t.Parallel()
	// The init() function must have registered "local".
	root := t.TempDir()
	b, err := backend.New("local", &Config{AllowedRoots: []string{root}})
	if err != nil {
		t.Fatalf("backend.New(local): %v", err)
	}
	defer b.Close()
	if b.Kind() != "local" {
		t.Errorf("Kind = %q, want local", b.Kind())
	}
}

func TestCapabilities(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	caps := b.Capabilities()
	if !caps.RangeRead {
		t.Error("RangeRead must be true")
	}
	if caps.Multipart {
		t.Error("Multipart must be false")
	}
	if !caps.AtomicRename {
		t.Error("AtomicRename must be true")
	}
	if !caps.StrongConsistency {
		t.Error("StrongConsistency must be true")
	}
	if caps.ListMaxKeys != 0 {
		t.Errorf("ListMaxKeys = %d, want 0", caps.ListMaxKeys)
	}
	if !caps.ResumeOffsetWrite {
		t.Error("ResumeOffsetWrite must be true (local supports partial-file resume)")
	}
}

// -----------------------------------------------------------------------
// Put + Get round-trip across size matrix
// -----------------------------------------------------------------------

func TestPutGet_RoundTrip_Matrix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		size int
	}{
		{"4KiB", 4 * 1024},
		{"4MiB", 4 * 1024 * 1024},
		{"16MiB", 16 * 1024 * 1024},
		{"64MiB", 64 * 1024 * 1024},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			b, _ := newBackend(t)
			putAndCheck(t, b, "matrix/"+c.name+".bin", c.size)
		})
	}
}

func TestGet_RangeRead(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	src := make([]byte, 1<<20) // 1 MiB
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := b.Put(context.Background(), "range.bin", bytes.NewReader(src), int64(len(src)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	const off, size = 100, 4096
	rc, err := b.Get(context.Background(), "range.bin", off, size)
	if err != nil {
		t.Fatalf("Get range: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, src[off:off+size]) {
		t.Fatal("range bytes mismatch")
	}
}

func TestPut_CreatesNestedDirs(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	putAndCheck(t, b, "a/b/c/d/nested.bin", 1024)
}

func TestPut_RespectsPartSizeMiB(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	// Use a tiny PartSizeMiB to force the path through opts.PartSizeMiB.
	// Floor enforcement ensures buffer is bumped to minBufferSize.
	if _, err := b.Put(context.Background(), "tiny.bin", bytes.NewReader(make([]byte, 1024)), 1024, backend.PutOptions{PartSizeMiB: 1}); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// -----------------------------------------------------------------------
// Head, Delete, Rename
// -----------------------------------------------------------------------

func TestHead_SizeAndMtime(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	before := time.Now().Add(-1 * time.Second)
	putAndCheck(t, b, "head.bin", 12345)
	size, etag, mtime, err := b.Head(context.Background(), "head.bin")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if size != 12345 {
		t.Errorf("size = %d, want 12345", size)
	}
	if etag != "" {
		t.Errorf("etag = %q, want empty for POSIX", etag)
	}
	if mtime.Before(before) {
		t.Errorf("mtime %v predates Put start %v", mtime, before)
	}
}

func TestHead_Missing(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	if _, _, _, err := b.Head(context.Background(), "no/such/file"); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestGet_Missing(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	if _, err := b.Get(context.Background(), "no/such/file", 0, 0); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDelete_Missing_NotAnError(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	if err := b.Delete(context.Background(), "absent.bin"); err != nil {
		t.Fatalf("Delete on missing key should be nil, got %v", err)
	}
	// And on a path whose parent also doesn't exist.
	if err := b.Delete(context.Background(), "absent/sub/dir/file.bin"); err != nil {
		t.Fatalf("Delete on missing nested key should be nil, got %v", err)
	}
}

func TestDelete_RemovesFile(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	putAndCheck(t, b, "to-delete.bin", 256)
	if err := b.Delete(context.Background(), "to-delete.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, _, err := b.Head(context.Background(), "to-delete.bin"); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Fatalf("file still exists after Delete: %v", err)
	}
}

func TestRename_AtomicSameFS(t *testing.T) {
	t.Parallel()
	b, root := newBackend(t)
	putAndCheck(t, b, "src.bin", 1024)
	if err := b.Rename(context.Background(), "src.bin", "dst/renamed.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	// Source must be gone, destination must exist and be readable.
	if _, _, _, err := b.Head(context.Background(), "src.bin"); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Fatalf("source still exists after rename: %v", err)
	}
	if _, _, _, err := b.Head(context.Background(), "dst/renamed.bin"); err != nil {
		t.Fatalf("destination missing after rename: %v", err)
	}
	// Verify the file physically landed under root.
	if _, err := os.Stat(filepath.Join(root, "dst", "renamed.bin")); err != nil {
		t.Fatalf("Stat physical path: %v", err)
	}
}

// -----------------------------------------------------------------------
// List: recursive vs shallow
// -----------------------------------------------------------------------

func TestList_Shallow_vs_Recursive(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)

	// Build a tree:
	//   listroot/
	//     a.txt
	//     b.txt
	//     sub/
	//       c.txt
	//       deep/
	//         d.txt
	for _, k := range []string{"listroot/a.txt", "listroot/b.txt", "listroot/sub/c.txt", "listroot/sub/deep/d.txt"} {
		if _, err := b.Put(context.Background(), k, bytes.NewReader([]byte("x")), 1, backend.PutOptions{}); err != nil {
			t.Fatalf("seed Put %s: %v", k, err)
		}
	}

	collect := func(recursive bool) []backend.Entry {
		ch, err := b.List(context.Background(), "listroot", recursive)
		if err != nil {
			t.Fatalf("List(recursive=%v): %v", recursive, err)
		}
		var out []backend.Entry
		for e := range ch {
			if e.Err != nil {
				t.Fatalf("List entry err: %v", e.Err)
			}
			out = append(out, e)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
		return out
	}

	shallow := collect(false)
	wantShallow := map[string]bool{
		"listroot/a.txt": false,
		"listroot/b.txt": false,
		"listroot/sub":   true, // IsDir
	}
	if len(shallow) != len(wantShallow) {
		t.Fatalf("shallow: got %d entries, want %d: %+v", len(shallow), len(wantShallow), shallow)
	}
	for _, e := range shallow {
		wantDir, ok := wantShallow[e.Key]
		if !ok {
			t.Errorf("unexpected shallow entry: %q", e.Key)
			continue
		}
		if e.IsDir != wantDir {
			t.Errorf("entry %q IsDir = %v, want %v", e.Key, e.IsDir, wantDir)
		}
	}

	recursive := collect(true)
	wantRecursive := map[string]bool{
		"listroot/a.txt":          false,
		"listroot/b.txt":          false,
		"listroot/sub":            true,
		"listroot/sub/c.txt":      false,
		"listroot/sub/deep":       true,
		"listroot/sub/deep/d.txt": false,
	}
	if len(recursive) != len(wantRecursive) {
		t.Fatalf("recursive: got %d entries, want %d: %+v", len(recursive), len(wantRecursive), recursive)
	}
	for _, e := range recursive {
		wantDir, ok := wantRecursive[e.Key]
		if !ok {
			t.Errorf("unexpected recursive entry: %q", e.Key)
			continue
		}
		if e.IsDir != wantDir {
			t.Errorf("entry %q IsDir = %v, want %v", e.Key, e.IsDir, wantDir)
		}
	}
}

func TestList_Missing(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	if _, err := b.List(context.Background(), "no/such/dir", true); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// -----------------------------------------------------------------------
// AllowedRoots enforcement: every op must reject escapes
// -----------------------------------------------------------------------

func TestPathEscape_RejectedOnEveryOp(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	ctx := context.Background()
	// "../escape" relative to AllowedRoots[0] points outside.
	escape := "../escape-attempt"

	if _, err := b.Get(ctx, escape, 0, 0); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Get: expected ErrConfigInvalid, got %v", err)
	}
	if _, _, _, err := b.Head(ctx, escape); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Head: expected ErrConfigInvalid, got %v", err)
	}
	if _, err := b.Put(ctx, escape, bytes.NewReader([]byte("x")), 1, backend.PutOptions{}); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Put: expected ErrConfigInvalid, got %v", err)
	}
	if err := b.Delete(ctx, escape); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Delete: expected ErrConfigInvalid, got %v", err)
	}
	if _, err := b.List(ctx, escape, true); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("List: expected ErrConfigInvalid, got %v", err)
	}
	if err := b.Rename(ctx, escape, "ok.bin"); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Rename(oldEscape): expected ErrConfigInvalid, got %v", err)
	}
}

func TestAbsolutePathOutside_Rejected(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	// An absolute path that's clearly outside any reasonable test temp dir.
	outside := "/etc/passwd"
	if _, err := b.Get(context.Background(), outside, 0, 0); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid for absolute outside path, got %v", err)
	}
}

// -----------------------------------------------------------------------
// Symlink-escape rejection
// -----------------------------------------------------------------------

func TestSymlink_PointingOutsideAllowedRoots_Rejected(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root := newBackend(t)

	// Pick a target that we know is outside the test root. /etc is fine
	// — we only need its path; we never actually read it.
	const escapeTarget = "/etc"

	linkPath := filepath.Join(root, "escape-link")
	if err := os.Symlink(escapeTarget, linkPath); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	// Get / Head via the symlinked key must fail with ErrConfigInvalid.
	ctx := context.Background()
	if _, err := b.Get(ctx, "escape-link", 0, 0); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Get via escape symlink: expected ErrConfigInvalid, got %v", err)
	}
	if _, _, _, err := b.Head(ctx, "escape-link"); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Head via escape symlink: expected ErrConfigInvalid, got %v", err)
	}
	if _, err := b.List(ctx, "escape-link", false); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("List via escape symlink: expected ErrConfigInvalid, got %v", err)
	}

	// Also verify a symlink whose parent dir is the escape — writing into
	// that path must be refused (Put resolves the parent and rejects).
	parentLink := filepath.Join(root, "escape-parent")
	if err := os.Symlink(escapeTarget, parentLink); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	if _, err := b.Put(ctx, "escape-parent/file.bin", bytes.NewReader([]byte("x")), 1, backend.PutOptions{}); !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("Put via escape parent symlink: expected ErrConfigInvalid, got %v", err)
	}
}

func TestSymlink_PointingInsideAllowedRoots_Allowed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root := newBackend(t)
	// Create a real file and a symlink to it, both inside the root.
	realPath := filepath.Join(root, "real.bin")
	if err := os.WriteFile(realPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	linkPath := filepath.Join(root, "link.bin")
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	rc, err := b.Get(context.Background(), "link.bin", 0, 0)
	if err != nil {
		t.Fatalf("Get via in-root symlink: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "hello" {
		t.Errorf("contents = %q, want hello", string(got))
	}
}

// -----------------------------------------------------------------------
// OnSymlink policy (子项 1: rclone gap — local symlink handling)
// -----------------------------------------------------------------------

// newBackendWithSymlinkPolicy is newBackend's cousin that lets a test pick
// the OnSymlink policy. Returns the backend, the resolved root path, and
// a tempdir set as a SECOND AllowedRoot so cross-root symlink scenarios
// (target in an allowed neighbour root) can be exercised.
func newBackendWithSymlinkPolicy(t *testing.T, policy string) (backend.Backend, string, string) {
	t.Helper()
	root := t.TempDir()
	neighbour := t.TempDir()
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", root, err)
	}
	resolvedNeighbour, err := filepath.EvalSymlinks(neighbour)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", neighbour, err)
	}
	b, err := New(&Config{
		AllowedRoots:         []string{resolvedRoot, resolvedNeighbour},
		DefaultBufferSizeKiB: 4096,
		OnSymlink:            policy,
	})
	if err != nil {
		t.Fatalf("New(policy=%q): %v", policy, err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, resolvedRoot, resolvedNeighbour
}

// collectList drains a List channel into a slice, separating errors from
// successful entries so individual tests can assert against either.
func collectList(t *testing.T, b backend.Backend, prefix string, recursive bool) ([]backend.Entry, []error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := b.List(ctx, prefix, recursive)
	if err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	var entries []backend.Entry
	var errs []error
	for e := range ch {
		if e.Err != nil {
			errs = append(errs, e.Err)
			continue
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries, errs
}

// keysOf is a tiny helper for table-style assertions.
func keysOf(entries []backend.Entry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Key)
	}
	sort.Strings(out)
	return out
}

func TestOnSymlink_New_RejectsUnknownPolicy(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	_, err := New(&Config{AllowedRoots: []string{root}, OnSymlink: "bogus"})
	if err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid for unknown OnSymlink, got %v", err)
	}
}

// TestOnSymlink_Skip_InRoot_SkipsSymlink covers the default / back-compat
// behaviour: a symlink whose target sits inside AllowedRoots is silently
// omitted from List output.
func TestOnSymlink_Skip_InRoot_SkipsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root, _ := newBackendWithSymlinkPolicy(t, "skip")

	// listroot/
	//   real.bin
	//   link.bin -> real.bin
	dir := filepath.Join(root, "listroot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "real.bin"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "real.bin"), filepath.Join(dir, "link.bin")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	// Recursive list under "listroot" must see real.bin but NOT link.bin.
	entries, errs := collectList(t, b, "listroot", true)
	if len(errs) != 0 {
		t.Fatalf("unexpected list errors: %v", errs)
	}
	got := keysOf(entries)
	wantContains := "listroot/real.bin"
	wantOmits := "listroot/link.bin"
	found, omitted := false, true
	for _, k := range got {
		if k == wantContains {
			found = true
		}
		if k == wantOmits {
			omitted = false
		}
	}
	if !found {
		t.Errorf("recursive list missing %q; got %v", wantContains, got)
	}
	if !omitted {
		t.Errorf("recursive list should omit symlink %q under skip; got %v", wantOmits, got)
	}

	// Shallow list: same expectation.
	entries, errs = collectList(t, b, "listroot", false)
	if len(errs) != 0 {
		t.Fatalf("unexpected shallow list errors: %v", errs)
	}
	got = keysOf(entries)
	for _, k := range got {
		if k == wantOmits {
			t.Errorf("shallow list should omit symlink %q under skip; got %v", wantOmits, got)
		}
	}
}

// TestOnSymlink_Skip_CrossRoot_AlsoSkips covers a symlink whose target sits
// in a SECOND AllowedRoot — under skip the link is still omitted regardless
// of where it points.
func TestOnSymlink_Skip_CrossRoot_AlsoSkips(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root, neighbour := newBackendWithSymlinkPolicy(t, "skip")

	dir := filepath.Join(root, "listroot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	target := filepath.Join(neighbour, "neighbour.bin")
	if err := os.WriteFile(target, []byte("y"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "cross.bin")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	entries, errs := collectList(t, b, "listroot", true)
	if len(errs) != 0 {
		t.Fatalf("unexpected list errors: %v", errs)
	}
	for _, k := range keysOf(entries) {
		if k == "listroot/cross.bin" {
			t.Errorf("skip policy must omit cross-root symlink; got %v", keysOf(entries))
		}
	}
}

// TestOnSymlink_Follow_InRoot_EmitsTarget covers follow on a symlink whose
// target sits in the SAME AllowedRoot: the entry must appear with target
// (deref'd) size + mtime.
func TestOnSymlink_Follow_InRoot_EmitsTarget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root, _ := newBackendWithSymlinkPolicy(t, "follow")

	dir := filepath.Join(root, "listroot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	target := filepath.Join(dir, "real.bin")
	payload := []byte("hello-follow")
	if err := os.WriteFile(target, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "link.bin")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	entries, errs := collectList(t, b, "listroot", true)
	if len(errs) != 0 {
		t.Fatalf("unexpected list errors: %v", errs)
	}
	var linkEntry *backend.Entry
	for i := range entries {
		if entries[i].Key == "listroot/link.bin" {
			linkEntry = &entries[i]
			break
		}
	}
	if linkEntry == nil {
		t.Fatalf("follow policy must emit symlink; got %v", keysOf(entries))
	}
	if linkEntry.Size != int64(len(payload)) {
		t.Errorf("follow link size = %d, want %d (deref'd size)", linkEntry.Size, len(payload))
	}
	if linkEntry.IsDir {
		t.Errorf("follow link must report IsDir=false for file target, got true")
	}

	// Shallow list should also surface the link under follow.
	entries, errs = collectList(t, b, "listroot", false)
	if len(errs) != 0 {
		t.Fatalf("unexpected shallow list errors: %v", errs)
	}
	found := false
	for _, e := range entries {
		if e.Key == "listroot/link.bin" && e.Size == int64(len(payload)) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("shallow follow must surface link.bin with deref'd size; got %v", entries)
	}
}

// TestOnSymlink_Follow_CrossRoot_AllowedRootsUnion covers the design
// invariant: follow may cross an AllowedRoots boundary as long as the
// FINAL resolved path remains under the configured root union.
func TestOnSymlink_Follow_CrossRoot_AllowedRootsUnion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root, neighbour := newBackendWithSymlinkPolicy(t, "follow")

	dir := filepath.Join(root, "listroot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	target := filepath.Join(neighbour, "in-neighbour.bin")
	if err := os.WriteFile(target, []byte("zz"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "cross.bin")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	entries, errs := collectList(t, b, "listroot", true)
	if len(errs) != 0 {
		t.Fatalf("unexpected list errors (cross-root inside union): %v", errs)
	}
	found := false
	for _, e := range entries {
		if e.Key == "listroot/cross.bin" && e.Size == 2 {
			found = true
		}
	}
	if !found {
		t.Errorf("follow across AllowedRoots union must surface cross.bin; got %v", entries)
	}
}

// TestOnSymlink_Follow_OutsideUnion_EmitsError covers the safety check: a
// follow that escapes the AllowedRoots union surfaces an entry with Err
// instead of leaking out-of-root metadata.
func TestOnSymlink_Follow_OutsideUnion_EmitsError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root, _ := newBackendWithSymlinkPolicy(t, "follow")

	dir := filepath.Join(root, "listroot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// /etc lives outside both AllowedRoots in our test (the roots are
	// fresh t.TempDirs).
	if err := os.Symlink("/etc", filepath.Join(dir, "escape.bin")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	entries, errs := collectList(t, b, "listroot", true)
	if len(errs) == 0 {
		t.Fatalf("follow outside union must surface an error; got entries=%v", keysOf(entries))
	}
	// At least one error must mention the symlink path so operators can act.
	matched := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "escape.bin") || strings.Contains(e.Error(), "/etc") {
			matched = true
			break
		}
	}
	if !matched {
		t.Errorf("expected error to reference the escape symlink; got %v", errs)
	}
	// The escape entry must NOT appear as a successful entry.
	for _, e := range entries {
		if e.Key == "listroot/escape.bin" {
			t.Errorf("escape.bin must not be reported as a successful entry under follow; got %v", entries)
		}
	}
}

// TestOnSymlink_Error_InRoot_EmitsErrorEntry covers the error policy: every
// symlink — even an in-root one — surfaces a Entry{Err}, never silently
// skipped or expanded.
func TestOnSymlink_Error_InRoot_EmitsErrorEntry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root, _ := newBackendWithSymlinkPolicy(t, "error")

	dir := filepath.Join(root, "listroot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "real.bin"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "real.bin"), filepath.Join(dir, "link.bin")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	entries, errs := collectList(t, b, "listroot", true)
	if len(errs) == 0 {
		t.Fatalf("error policy must surface an Err entry for in-root symlink; got entries=%v", keysOf(entries))
	}
	// link.bin must NOT appear as a successful entry.
	for _, e := range entries {
		if e.Key == "listroot/link.bin" {
			t.Errorf("error policy must not surface symlink as success entry; got %v", entries)
		}
	}
	// And real.bin must still be listed.
	foundReal := false
	for _, e := range entries {
		if e.Key == "listroot/real.bin" {
			foundReal = true
		}
	}
	if !foundReal {
		t.Errorf("error policy must not block sibling files; missing real.bin in %v", keysOf(entries))
	}

	// Shallow list: same expectation.
	entries, errs = collectList(t, b, "listroot", false)
	if len(errs) == 0 {
		t.Fatalf("shallow error policy must surface an Err entry; got entries=%v", keysOf(entries))
	}
	for _, e := range entries {
		if e.Key == "listroot/link.bin" {
			t.Errorf("shallow error policy must not surface symlink as success entry; got %v", entries)
		}
	}
}

// TestOnSymlink_Error_CrossRoot_EmitsErrorEntry mirrors the previous test
// but with the symlink target in a neighbour AllowedRoot: error policy
// ignores target location and always emits Err.
func TestOnSymlink_Error_CrossRoot_EmitsErrorEntry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require admin on Windows")
	}
	t.Parallel()
	b, root, neighbour := newBackendWithSymlinkPolicy(t, "error")

	dir := filepath.Join(root, "listroot")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	target := filepath.Join(neighbour, "n.bin")
	if err := os.WriteFile(target, []byte("y"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "cross.bin")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	entries, errs := collectList(t, b, "listroot", true)
	if len(errs) == 0 {
		t.Fatalf("error policy must surface Err for cross-root symlink; got entries=%v", keysOf(entries))
	}
	for _, e := range entries {
		if e.Key == "listroot/cross.bin" {
			t.Errorf("error policy must not surface cross.bin as success; got %v", entries)
		}
	}
}

// -----------------------------------------------------------------------
// Mtime preservation
// -----------------------------------------------------------------------

// TestPut_PreservesMtime verifies that supplying PutOptions.Mtime causes the
// backend to chtimes the destination so that subsequent Head returns the
// caller-supplied modification time (truncated only by the local FS's mtime
// resolution).
func TestPut_PreservesMtime(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	ctx := context.Background()

	// Pick a fixed time in the past so we don't race the FS clock.
	want := time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC)
	body := []byte("mtime preservation payload")
	if _, err := b.Put(ctx, "mtime.bin", bytes.NewReader(body), int64(len(body)), backend.PutOptions{Mtime: &want}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, _, got, err := b.Head(ctx, "mtime.bin")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	// Some filesystems (HFS+, FAT) truncate to 1s. Tolerate a small delta.
	const tolerance = time.Second
	if diff := got.Sub(want); diff < -tolerance || diff > tolerance {
		t.Errorf("Head mtime = %s, want %s (delta=%s tolerance=%s)", got, want, diff, tolerance)
	}
}

// TestPut_NoMtimeUsesWallClock verifies that omitting PutOptions.Mtime leaves
// the destination's mtime at the backend's write time (used as a regression
// guard against accidentally writing a zero timestamp).
func TestPut_NoMtimeUsesWallClock(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	ctx := context.Background()

	before := time.Now().Add(-time.Second)
	body := []byte("no mtime supplied")
	if _, err := b.Put(ctx, "wallclock.bin", bytes.NewReader(body), int64(len(body)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	after := time.Now().Add(time.Second)

	_, _, got, err := b.Head(ctx, "wallclock.bin")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if got.Before(before) || got.After(after) {
		t.Errorf("Head mtime = %s outside [%s, %s]", got, before, after)
	}
}

// -----------------------------------------------------------------------
// SameInstance + ServerSideCopy (capability stub)
// -----------------------------------------------------------------------

// TestSameInstance_Local builds the positional truth table: identical
// AllowedRoots → true; reordered, different, or extra entries → false.
func TestSameInstance_Local(t *testing.T) {
	t.Parallel()

	mk := func(roots []string) *Backend {
		// EvalSymlinks isn't required for the equality check (constructor
		// already normalises); we just need a backend with matching roots
		// on disk so New() doesn't reject the config.
		for _, r := range roots {
			if err := os.MkdirAll(r, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", r, err)
			}
		}
		b, err := New(&Config{AllowedRoots: roots, DefaultBufferSizeKiB: 64})
		if err != nil {
			t.Fatalf("New(%v): %v", roots, err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b.(*Backend)
	}

	base := t.TempDir()
	r1 := filepath.Join(base, "r1")
	r2 := filepath.Join(base, "r2")
	r3 := filepath.Join(base, "r3")

	a := mk([]string{r1, r2})
	sameAsA := mk([]string{r1, r2})
	reorder := mk([]string{r2, r1})
	disjoint := mk([]string{r3})

	cases := []struct {
		name string
		o    backend.Backend
		want bool
	}{
		{"identical roots", sameAsA, true},
		{"reordered roots", reorder, false},
		{"disjoint roots", disjoint, false},
		{"nil other", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := a.SameInstance(c.o); got != c.want {
				t.Errorf("SameInstance(%s) = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// TestServerSideCopy_LocalUnsupported pins the contract: local always
// rejects server-side copy with ErrBackendUnsupported so the executor can
// safely fall back to Get/Put.
func TestServerSideCopy_LocalUnsupported(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)
	bb := b.(*Backend)
	_, err := bb.ServerSideCopy(context.Background(), "src", "dst", backend.PutOptions{})
	if !errors.Is(err, backend.ErrBackendUnsupported) {
		t.Errorf("got %v, want ErrBackendUnsupported", err)
	}
	if caps := bb.Capabilities(); caps.ServerSideCopy {
		t.Errorf("Caps.ServerSideCopy = true, want false for local")
	}
}

// -----------------------------------------------------------------------
// ResumeOffset / partial-file resume (P2)
// -----------------------------------------------------------------------

// seedPartial writes prefix bytes to <root>/<key>.syncnode.partial, simulating
// a previous Put that crashed mid-write and left its partial on disk.
func seedPartial(t *testing.T, root, key string, prefix []byte) {
	t.Helper()
	partial := filepath.Join(root, key) + ".syncnode.partial"
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(partial, prefix, 0o644); err != nil {
		t.Fatalf("WriteFile partial: %v", err)
	}
}

// TestPut_ResumeOffset_HappyPath simulates the P2 acceptance scenario:
// 50KiB partial already on disk, caller resumes with ResumeOffset=50KiB and
// streams the remaining 50KiB; the final file must equal the 100KiB source
// byte-for-byte and no partial must remain.
func TestPut_ResumeOffset_HappyPath(t *testing.T) {
	t.Parallel()
	b, root := newBackend(t)

	const half = 50 * 1024
	src := make([]byte, 2*half)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	key := "resume/happy.bin"
	seedPartial(t, root, key, src[:half])

	res, err := b.Put(context.Background(), key, bytes.NewReader(src[half:]), int64(len(src)), backend.PutOptions{
		ResumeOffset: int64(half),
	})
	if err != nil {
		t.Fatalf("Put(resume): %v", err)
	}
	if res.BytesPut != int64(len(src)) {
		t.Errorf("BytesPut = %d, want %d", res.BytesPut, len(src))
	}

	got, err := os.ReadFile(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("dst content mismatch: len=%d want=%d, sha256(got)=%x sha256(src)=%x",
			len(got), len(src), sha256.Sum256(got), sha256.Sum256(src))
	}
	if _, err := os.Stat(filepath.Join(root, key) + ".syncnode.partial"); !os.IsNotExist(err) {
		t.Errorf("partial should be renamed away after success, stat err = %v", err)
	}
}

// TestPut_ResumeOffset_PreservesChecksum verifies that PutResult.Checksum is
// the sha256 of the WHOLE file (prefix + body), not just the resumed body.
// This is critical: data-integrity-p0-p2.md uses this checksum to validate
// the destination against the source after resume.
func TestPut_ResumeOffset_PreservesChecksum(t *testing.T) {
	t.Parallel()
	b, root := newBackend(t)

	const half = 50 * 1024
	src := make([]byte, 2*half)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	key := "resume/checksum.bin"
	seedPartial(t, root, key, src[:half])

	res, err := b.Put(context.Background(), key, bytes.NewReader(src[half:]), int64(len(src)), backend.PutOptions{
		ResumeOffset:    int64(half),
		ComputeChecksum: true,
	})
	if err != nil {
		t.Fatalf("Put(resume,checksum): %v", err)
	}
	if res.Algorithm != backend.ChecksumAlgorithmSHA256 {
		t.Errorf("Algorithm = %q, want sha256", res.Algorithm)
	}
	want := sha256.Sum256(src)
	wantHex := hexEncode(want[:])
	if res.Checksum != wantHex {
		t.Errorf("Checksum = %q, want sha256(whole) = %q", res.Checksum, wantHex)
	}
}

// TestPut_ResumeOffset_StalePartial guards against the caller pointing at a
// partial that does NOT actually contain ResumeOffset bytes. Continuing would
// either Seek past end (sparse hole) or skip a chunk — both silent
// corruption. Local must reject with ErrConfigInvalid.
func TestPut_ResumeOffset_StalePartial(t *testing.T) {
	t.Parallel()
	b, root := newBackend(t)

	const half = 50 * 1024
	src := make([]byte, 2*half)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	key := "resume/stale.bin"
	// Seed a partial that is SHORTER than ResumeOffset.
	seedPartial(t, root, key, src[:half-1024])

	_, err := b.Put(context.Background(), key, bytes.NewReader(src[half:]), int64(len(src)), backend.PutOptions{
		ResumeOffset: int64(half),
	})
	if err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid for stale partial, got %v", err)
	}
}

// TestPut_ResumeOffset_StalePartialMissing covers the boundary where the
// partial file does not exist at all but the caller claims a non-zero
// ResumeOffset (e.g. operator manually wiped /tmp). Must surface as
// ErrConfigInvalid so the executor can decide to restart from offset 0.
func TestPut_ResumeOffset_StalePartialMissing(t *testing.T) {
	t.Parallel()
	b, _ := newBackend(t)

	src := make([]byte, 100*1024)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	_, err := b.Put(context.Background(), "resume/missing.bin", bytes.NewReader(src[50*1024:]), int64(len(src)), backend.PutOptions{
		ResumeOffset: 50 * 1024,
	})
	if err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid when partial missing, got %v", err)
	}
}

// TestPut_ResumeOffset_FreshStartLeavesNoPartial verifies the ResumeOffset==0
// happy path: no pre-seeded partial required, write completes, and the
// partial file is renamed away (i.e. no stale .syncnode.partial residue).
func TestPut_ResumeOffset_FreshStartLeavesNoPartial(t *testing.T) {
	t.Parallel()
	b, root := newBackend(t)

	src := make([]byte, 4*1024)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	key := "resume/fresh.bin"
	if _, err := b.Put(context.Background(), key, bytes.NewReader(src), int64(len(src)), backend.PutOptions{
		ResumeOffset: 0,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("dst content mismatch")
	}
	if _, err := os.Stat(filepath.Join(root, key) + ".syncnode.partial"); !os.IsNotExist(err) {
		t.Errorf("partial should not exist after fresh Put, stat err = %v", err)
	}
}

// TestPut_ResumeOffset_TruncatesExtraBytes ensures that if a previous failed
// attempt wrote MORE bytes than the breakpoint recorded (e.g. crashed after
// io.Copy but before persisting bytesDone), the resume run truncates the
// excess back to ResumeOffset so the final file is exactly prefix+body,
// not prefix+extra+body.
func TestPut_ResumeOffset_TruncatesExtraBytes(t *testing.T) {
	t.Parallel()
	b, root := newBackend(t)

	const half = 50 * 1024
	src := make([]byte, 2*half)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	key := "resume/truncate.bin"
	// Seed partial with half + 1024 random bytes of "junk" that the caller's
	// breakpoint did not record.
	junk := make([]byte, 1024)
	if _, err := rand.Read(junk); err != nil {
		t.Fatalf("rand.Read junk: %v", err)
	}
	seedPartial(t, root, key, append(append([]byte{}, src[:half]...), junk...))

	if _, err := b.Put(context.Background(), key, bytes.NewReader(src[half:]), int64(len(src)), backend.PutOptions{
		ResumeOffset: int64(half),
	}); err != nil {
		t.Fatalf("Put(resume): %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("dst content mismatch: junk was not truncated. len(got)=%d want=%d",
			len(got), len(src))
	}
}

// TestList_HidesPartialFiles ensures the resume scratch suffix
// (.syncnode.partial) does not leak through List. Partial files are an
// implementation detail of Put; surfacing them would force every caller —
// executor, retention, dashboard — to know about the suffix.
func TestList_HidesPartialFiles(t *testing.T) {
	t.Parallel()
	b, root := newBackend(t)
	const base = "listhide"

	if err := os.MkdirAll(filepath.Join(root, base, "nest"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Real, complete file in the listing space.
	if _, err := b.Put(context.Background(), base+"/good.bin", bytes.NewReader([]byte("ok")), 2, backend.PutOptions{}); err != nil {
		t.Fatalf("Put good: %v", err)
	}
	// Drop stray partials straight onto disk (simulating crashed Puts).
	if err := os.WriteFile(filepath.Join(root, base, "stale.bin.syncnode.partial"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("WriteFile partial: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, base, "nest", "deep.bin.syncnode.partial"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("WriteFile nested partial: %v", err)
	}

	// Recursive
	ch, err := b.List(context.Background(), base, true)
	if err != nil {
		t.Fatalf("List recursive: %v", err)
	}
	var keys []string
	for e := range ch {
		if e.Err != nil {
			t.Fatalf("List entry err: %v", e.Err)
		}
		keys = append(keys, e.Key)
	}
	for _, k := range keys {
		if strings.HasSuffix(k, ".syncnode.partial") {
			t.Errorf("List leaked partial file: %q (full set: %v)", k, keys)
		}
	}

	// Shallow
	ch, err = b.List(context.Background(), base, false)
	if err != nil {
		t.Fatalf("List shallow: %v", err)
	}
	for e := range ch {
		if e.Err != nil {
			t.Fatalf("List entry err: %v", e.Err)
		}
		if strings.HasSuffix(e.Key, ".syncnode.partial") {
			t.Errorf("List(shallow) leaked partial file: %q", e.Key)
		}
	}
}

// hexEncode mirrors encoding/hex.EncodeToString without dragging the import
// just for tests — keeps test imports compact.
func hexEncode(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}
	return string(out)
}
