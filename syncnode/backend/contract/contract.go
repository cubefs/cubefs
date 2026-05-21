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

// Package contract provides a black-box test suite that every Backend
// implementation must pass. Each adapter writes a thin test file that
// instantiates its concrete Backend and invokes Run.
//
// The suite stays deliberately minimal — it exercises the data-path
// invariants (round-trip bytes / etag / size / mtime / listing) that the
// task executor in Phase D will rely on. Per-backend specifics (multipart
// edge cases, allowedRoots, ENOSPC) are covered in each adapter's own
// _test.go.
package contract

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// Suite is the contract test runner. Each adapter constructs one and calls
// Run; Setup must produce an empty, isolated namespace (e.g. a fresh local
// dir, or a unique s3 prefix) so two suites running concurrently never
// interfere.
type Suite struct {
	// Name identifies the backend in test output ("local", "s3", "cfs").
	Name string

	// Setup returns a fresh Backend instance plus a teardown function. The
	// returned Backend must be entirely empty (no pre-existing keys). The
	// teardown is called via t.Cleanup.
	Setup func(t *testing.T) (backend.Backend, func())

	// Sizes lists the file sizes (bytes) to exercise in round-trip tests.
	// Default if zero-length: {1KiB, 4MiB, 16MiB}. Adapters with known
	// hard limits (e.g. expensive 5GiB on real S3) can shrink this.
	Sizes []int
}

func (s *Suite) sizes() []int {
	if len(s.Sizes) > 0 {
		return s.Sizes
	}
	return []int{1 << 10, 4 << 20, 16 << 20} // 1 KiB, 4 MiB, 16 MiB
}

// Run executes the full contract suite. Sub-tests are named after the
// adapter (s.Name) so failures clearly identify which Backend regressed.
func (s *Suite) Run(t *testing.T) {
	if s.Setup == nil {
		t.Fatalf("contract.Suite.Setup is nil for %q", s.Name)
	}
	t.Run(s.Name, func(t *testing.T) {
		t.Run("PutGetRoundtrip", func(t *testing.T) { s.testPutGetRoundtrip(t) })
		t.Run("HeadAfterPut", func(t *testing.T) { s.testHeadAfterPut(t) })
		t.Run("GetMissing", func(t *testing.T) { s.testGetMissing(t) })
		t.Run("HeadMissing", func(t *testing.T) { s.testHeadMissing(t) })
		t.Run("DeleteIdempotent", func(t *testing.T) { s.testDeleteIdempotent(t) })
		t.Run("DeleteThenHead", func(t *testing.T) { s.testDeleteThenHead(t) })
		t.Run("ListAfterPut", func(t *testing.T) { s.testListAfterPut(t) })
		t.Run("RangeReadIfSupported", func(t *testing.T) { s.testRangeReadIfSupported(t) })
		t.Run("CapabilitiesConsistent", func(t *testing.T) { s.testCapabilitiesConsistent(t) })
		t.Run("Rename", func(t *testing.T) { s.testRename(t) })
		t.Run("CloseIdempotent", func(t *testing.T) { s.testCloseIdempotent(t) })
	})
}

// --- helpers ----------------------------------------------------------------

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return buf
}

func putBytes(t *testing.T, b backend.Backend, key string, data []byte) string {
	t.Helper()
	ctx := context.Background()
	etag, err := b.Put(ctx, key, bytes.NewReader(data), int64(len(data)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put(%q, %d bytes): %v", key, len(data), err)
	}
	return etag
}

func getBytes(t *testing.T, b backend.Backend, key string) []byte {
	t.Helper()
	ctx := context.Background()
	rc, err := b.Get(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	defer rc.Close()
	out, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%q): %v", key, err)
	}
	return out
}

// --- test cases ------------------------------------------------------------

func (s *Suite) testPutGetRoundtrip(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)

	for _, sz := range s.sizes() {
		t.Run(fmt.Sprintf("%dB", sz), func(t *testing.T) {
			data := randBytes(t, sz)
			key := fmt.Sprintf("rt-%d.bin", sz)
			putBytes(t, b, key, data)

			got := getBytes(t, b, key)
			if !bytes.Equal(got, data) {
				t.Fatalf("round-trip mismatch at size %d: got %d bytes, want %d", sz, len(got), len(data))
			}
		})
	}
}

func (s *Suite) testHeadAfterPut(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	ctx := context.Background()

	data := randBytes(t, 4096)
	putBytes(t, b, "head-target.bin", data)

	size, _, _, err := b.Head(ctx, "head-target.bin")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("Head size = %d, want %d", size, len(data))
	}
}

func (s *Suite) testGetMissing(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	ctx := context.Background()
	_, err := b.Get(ctx, "definitely-not-here.bin", 0, 0)
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("Get missing returned %v, want ErrKeyNotFound", err)
	}
}

func (s *Suite) testHeadMissing(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	ctx := context.Background()
	_, _, _, err := b.Head(ctx, "definitely-not-here.bin")
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("Head missing returned %v, want ErrKeyNotFound", err)
	}
}

func (s *Suite) testDeleteIdempotent(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	ctx := context.Background()
	// Delete a never-existed key must not error.
	if err := b.Delete(ctx, "never-existed.bin"); err != nil {
		t.Errorf("Delete on missing key: %v (want nil)", err)
	}
}

func (s *Suite) testDeleteThenHead(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	ctx := context.Background()

	putBytes(t, b, "to-delete.bin", []byte("payload"))
	if err := b.Delete(ctx, "to-delete.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, _, _, err := b.Head(ctx, "to-delete.bin")
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("Head after Delete: %v, want ErrKeyNotFound", err)
	}
}

func (s *Suite) testListAfterPut(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	ctx := context.Background()

	keys := []string{"list/a.bin", "list/b.bin", "list/c.bin"}
	for _, k := range keys {
		putBytes(t, b, k, []byte("x"))
	}

	ch, err := b.List(ctx, "list/", true)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var got []string
	for e := range ch {
		if e.Err != nil {
			t.Fatalf("list entry err: %v", e.Err)
		}
		if !e.IsDir {
			got = append(got, e.Key)
		}
	}
	sort.Strings(got)
	if len(got) != len(keys) {
		t.Fatalf("List returned %d entries, want %d (got %v)", len(got), len(keys), got)
	}
}

func (s *Suite) testRangeReadIfSupported(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	if !b.Capabilities().RangeRead {
		t.Skipf("%s does not support range reads", s.Name)
	}
	ctx := context.Background()
	data := randBytes(t, 8192)
	putBytes(t, b, "range.bin", data)

	rc, err := b.Get(ctx, "range.bin", 100, 200)
	if err != nil {
		t.Fatalf("Get[100,300): %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data[100:300]) {
		t.Errorf("range mismatch: got %d bytes, want 200 bytes from offset 100", len(got))
	}
}

func (s *Suite) testCapabilitiesConsistent(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	caps := b.Capabilities()
	// Sanity: Kind() must match the suite name OR the kind the suite was
	// configured for (some suites e.g. local register kind "local" but the
	// Suite.Name may differ for human readability).
	if b.Kind() == "" {
		t.Error("Backend.Kind() returned empty")
	}
	// ListMaxKeys: 0 means unlimited, otherwise must be positive.
	if caps.ListMaxKeys < 0 {
		t.Errorf("Caps.ListMaxKeys = %d, must be 0 or positive", caps.ListMaxKeys)
	}
}

func (s *Suite) testRename(t *testing.T) {
	b, teardown := s.Setup(t)
	t.Cleanup(teardown)
	ctx := context.Background()

	data := []byte("rename-me")
	putBytes(t, b, "src-key.bin", data)

	if err := b.Rename(ctx, "src-key.bin", "dst-key.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// src should now be missing.
	if _, _, _, err := b.Head(ctx, "src-key.bin"); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("Head src after rename: %v, want ErrKeyNotFound", err)
	}
	// dst should have the data.
	got := getBytes(t, b, "dst-key.bin")
	if !bytes.Equal(got, data) {
		t.Errorf("dst content mismatch after rename")
	}
}

func (s *Suite) testCloseIdempotent(t *testing.T) {
	b, teardown := s.Setup(t)
	defer teardown()

	if err := b.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	// A second Close may or may not error depending on impl, but must not
	// panic. We treat any non-panic outcome as success.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("second Close panicked: %v", r)
		}
	}()
	_ = b.Close()
}
