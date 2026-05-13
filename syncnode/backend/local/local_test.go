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
