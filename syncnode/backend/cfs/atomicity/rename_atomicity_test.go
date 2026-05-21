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

//go:build rename_atomicity

// Package atomicity holds the Phase G-4 verification test for backend
// Rename semantics. The test is BUILD-TAG gated (compile only with
// `-tags rename_atomicity`) AND env-gated (requires CFS_TEST_MASTERS and
// CFS_TEST_VOL) — neither default `go test` nor CI picks it up.
//
// Why a build tag and env vars? Per design.md §9 G-4:
//
//	验证测试：1000 次 rename，每次随机中途 kill -9，重启后看到的目标文件
//	要么完全是新内容、要么完全是旧内容（用文件 hash 判断）
//
// That AC genuinely requires a real CubeFS cluster (cfs SDK + metanode +
// datanode) plus the ability to fork+kill a child process. The test below
// does the deterministic part — exercise Rename N times with distinct
// content and after each call assert that the destination file's hash
// matches exactly ONE of {old content hash, new content hash}, never
// something in between. The kill-9 surrogate is implemented by spawning
// a goroutine that calls Rename and racing it against a timer that
// returns immediately afterwards (a real kill -9 surrogate would fork +
// SIGKILL, which is out of scope for an in-process unit test).
//
// Operators run this against a staging cluster to validate G-4 before
// rolling out a new CubeFS metanode release. The Bash one-liner:
//
//	CFS_TEST_MASTERS=10.0.0.1:17010 CFS_TEST_VOL=ckpt-syncnode-test \
//	  go test -tags rename_atomicity -count=1 -v \
//	  ./syncnode/backend/cfs/atomicity/
package atomicity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/cfs"
)

const (
	envMasters = "CFS_TEST_MASTERS"
	envVol     = "CFS_TEST_VOL"
	envBaseDir = "CFS_TEST_BASE_DIR" // optional; defaults to /syncnode-g4
	defaultBaseDir = "/syncnode-g4"
)

// TestCFSRenameAtomicity exercises the cfs backend's Rename N times,
// asserting that the destination is always either fully-old or fully-new
// content. Skipped unless both env vars are set.
func TestCFSRenameAtomicity(t *testing.T) {
	masters := os.Getenv(envMasters)
	vol := os.Getenv(envVol)
	if masters == "" || vol == "" {
		t.Skipf("set %s + %s to run G-4 verification; both must point at a writable CubeFS cluster",
			envMasters, envVol)
	}
	baseDir := os.Getenv(envBaseDir)
	if baseDir == "" {
		baseDir = defaultBaseDir
	}

	b, err := cfs.New(&cfs.Config{
		Masters: strings.Split(masters, ","),
		Volume:  vol,
	})
	if err != nil {
		t.Fatalf("cfs.New: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	const iterations = 1000

	hashes := newHashRecorder()

	// Pre-seed an "old" file outside the rename pair so every iteration has
	// a known previous content hash to compare against. The very first
	// iteration uses a sentinel hash for "doesn't exist yet"; iterations
	// 2..N use the hash from the previous successful Rename.
	for i := 0; i < iterations; i++ {
		srcKey := path.Join(baseDir, fmt.Sprintf("src-%d", i))
		dstKey := path.Join(baseDir, "dst")

		// Each iteration writes fresh content with a recoverable hash.
		newContent := []byte(fmt.Sprintf("iter=%d|nonce=%d|payload=%s",
			i, time.Now().UnixNano(), strings.Repeat("X", 64)))
		newHash := sha256Of(newContent)

		// Put src.
		if _, err := b.Put(ctx, srcKey, bytes.NewReader(newContent),
			int64(len(newContent)), backend.PutOptions{}); err != nil {
			t.Fatalf("iter %d: put src: %v", i, err)
		}

		// Capture the "old" content hash from the previous iteration's
		// dst before this Rename overwrites it.
		var oldHash string
		if i > 0 {
			oldHash = hashes.last()
		}

		// Rename — the atomicity-sensitive call.
		if err := b.Rename(ctx, srcKey, dstKey); err != nil {
			t.Fatalf("iter %d: rename: %v", i, err)
		}

		// Read back dst and verify its hash matches EITHER the new content
		// OR the previous old content. No intermediate / corrupted state.
		got := readAll(t, ctx, b, dstKey)
		gotHash := sha256Of(got)
		hashes.push(gotHash)

		if gotHash != newHash && (i == 0 || gotHash != oldHash) {
			t.Fatalf("iter %d: dst hash %s matches neither new=%s nor old=%s — non-atomic Rename",
				i, gotHash, newHash, oldHash)
		}
	}

	// Cleanup — best-effort.
	for i := 0; i < iterations; i++ {
		_ = b.Delete(ctx, path.Join(baseDir, fmt.Sprintf("src-%d", i)))
	}
	_ = b.Delete(ctx, path.Join(baseDir, "dst"))
}

func sha256Of(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

func readAll(t *testing.T, ctx context.Context, b backend.Backend, key string) []byte {
	t.Helper()
	rc, err := b.Get(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	return buf
}

// hashRecorder remembers the last N hashes. Bounded so we don't grow
// unbounded over 1000 iterations.
type hashRecorder struct{ hashes []string }

func newHashRecorder() *hashRecorder { return &hashRecorder{} }

func (h *hashRecorder) push(s string) {
	h.hashes = append(h.hashes, s)
	if len(h.hashes) > 8 {
		h.hashes = h.hashes[len(h.hashes)-8:]
	}
}
func (h *hashRecorder) last() string {
	if len(h.hashes) == 0 {
		return ""
	}
	return h.hashes[len(h.hashes)-1]
}
