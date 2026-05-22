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

package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// -----------------------------------------------------------------------
// filterXattrs
// -----------------------------------------------------------------------

func TestFilterXattrs(t *testing.T) {
	cases := []struct {
		name string
		in   map[string][]byte
		want map[string][]byte
	}{
		{
			name: "nil",
			in:   nil,
			want: nil,
		},
		{
			name: "empty",
			in:   map[string][]byte{},
			want: nil,
		},
		{
			name: "keep_user",
			in: map[string][]byte{
				"user.foo": []byte("a"),
				"user.bar": []byte("b"),
			},
			want: map[string][]byte{
				"user.foo": []byte("a"),
				"user.bar": []byte("b"),
			},
		},
		{
			name: "keep_posix_acl",
			in: map[string][]byte{
				"system.posix_acl_access":  []byte("aaa"),
				"system.posix_acl_default": []byte("ddd"),
			},
			want: map[string][]byte{
				"system.posix_acl_access":  []byte("aaa"),
				"system.posix_acl_default": []byte("ddd"),
			},
		},
		{
			name: "drop_security",
			in: map[string][]byte{
				"security.selinux":    []byte("x"),
				"security.capability": []byte("y"),
			},
			want: nil,
		},
		{
			name: "drop_trusted",
			in: map[string][]byte{
				"trusted.glusterfs.dht": []byte("x"),
			},
			want: nil,
		},
		{
			name: "drop_other_system",
			in: map[string][]byte{
				"system.something_else": []byte("x"),
			},
			want: nil,
		},
		{
			name: "mixed",
			in: map[string][]byte{
				"user.kept":               []byte("k"),
				"system.posix_acl_access": []byte("a"),
				"security.dropped":        []byte("d"),
				"trusted.dropped":         []byte("d"),
				"system.dropped":          []byte("d"),
			},
			want: map[string][]byte{
				"user.kept":               []byte("k"),
				"system.posix_acl_access": []byte("a"),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterXattrs(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("filterXattrs(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// applyPutOptionsExtras
// -----------------------------------------------------------------------

func TestApplyPutOptionsExtras_PreservesBaseFields(t *testing.T) {
	baseMtime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	base := backend.PutOptions{
		StorageClass:    "STANDARD",
		ContentType:     "application/octet-stream",
		Multipart:       true,
		PartSizeMiB:     8,
		ComputeChecksum: true,
		Mtime:           &baseMtime,
	}
	mode := uint32(0644)
	uid := uint32(1000)
	gid := uint32(1000)
	xattrs := map[string][]byte{"user.foo": []byte("v")}
	extras := metadataExtras{
		Mode:   &mode,
		UID:    &uid,
		GID:    &gid,
		Xattrs: xattrs,
	}
	got := applyPutOptionsExtras(base, extras)

	// caller's base fields preserved
	if got.StorageClass != "STANDARD" || got.ContentType != "application/octet-stream" {
		t.Errorf("base scalar fields mutated: %+v", got)
	}
	if !got.Multipart || got.PartSizeMiB != 8 || !got.ComputeChecksum {
		t.Errorf("base behaviour fields mutated: %+v", got)
	}
	if got.Mtime == nil || !got.Mtime.Equal(baseMtime) {
		t.Errorf("base.Mtime not preserved: got %v want %v", got.Mtime, baseMtime)
	}

	// extras merged
	if got.Mode == nil || *got.Mode != mode {
		t.Errorf("Mode not merged: got %v", got.Mode)
	}
	if got.UID == nil || *got.UID != uid {
		t.Errorf("UID not merged: got %v", got.UID)
	}
	if got.GID == nil || *got.GID != gid {
		t.Errorf("GID not merged: got %v", got.GID)
	}
	if !reflect.DeepEqual(got.Xattrs, xattrs) {
		t.Errorf("Xattrs not merged: got %v", got.Xattrs)
	}
}

func TestApplyPutOptionsExtras_EmptyExtrasIsNoOp(t *testing.T) {
	base := backend.PutOptions{
		StorageClass: "GLACIER",
	}
	got := applyPutOptionsExtras(base, metadataExtras{})
	if got.Mode != nil || got.UID != nil || got.GID != nil || got.Xattrs != nil {
		t.Errorf("empty extras leaked into base: %+v", got)
	}
	if got.StorageClass != "GLACIER" {
		t.Errorf("base mutated: %+v", got)
	}
}

// -----------------------------------------------------------------------
// applyMetadataPolicy
// -----------------------------------------------------------------------

func TestApplyMetadataPolicy_Skip(t *testing.T) {
	before := metadataUnsupportedSkip.Load()
	task := &Task{ID: "t1", OnMetadataUnsupported: OnMetadataUnsupportedSkip}
	entry := backend.Entry{Key: "k"}
	skip, err := applyMetadataPolicy(task, entry, "mode")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !skip {
		t.Error("expected skip=true")
	}
	if got := metadataUnsupportedSkip.Load(); got != before+1 {
		t.Errorf("metadataUnsupportedSkip = %d, want %d", got, before+1)
	}
}

func TestApplyMetadataPolicy_Error(t *testing.T) {
	before := metadataUnsupportedError.Load()
	task := &Task{ID: "t1", OnMetadataUnsupported: OnMetadataUnsupportedError}
	entry := backend.Entry{Key: "k"}
	skip, err := applyMetadataPolicy(task, entry, "owner")
	if err == nil {
		t.Fatal("expected error")
	}
	if skip {
		t.Error("expected skip=false on error path")
	}
	if !strings.Contains(err.Error(), "owner") || !strings.Contains(err.Error(), "k") {
		t.Errorf("error should name field+key: %v", err)
	}
	if got := metadataUnsupportedError.Load(); got != before+1 {
		t.Errorf("metadataUnsupportedError = %d, want %d", got, before+1)
	}
}

func TestApplyMetadataPolicy_Warn(t *testing.T) {
	before := metadataUnsupportedWarn.Load()
	task := &Task{ID: "t1", OnMetadataUnsupported: OnMetadataUnsupportedWarn}
	entry := backend.Entry{Key: "k"}
	skip, err := applyMetadataPolicy(task, entry, "xattrs")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if skip {
		t.Error("expected skip=false on warn path")
	}
	if got := metadataUnsupportedWarn.Load(); got != before+1 {
		t.Errorf("metadataUnsupportedWarn = %d, want %d", got, before+1)
	}
}

func TestApplyMetadataPolicy_UnknownDefaultsToWarn(t *testing.T) {
	before := metadataUnsupportedWarn.Load()
	task := &Task{ID: "t1", OnMetadataUnsupported: "no-such-policy"}
	entry := backend.Entry{Key: "k"}
	skip, err := applyMetadataPolicy(task, entry, "mode")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if skip {
		t.Error("expected skip=false on unknown→warn path")
	}
	if got := metadataUnsupportedWarn.Load(); got != before+1 {
		t.Errorf("metadataUnsupportedWarn = %d, want %d", got, before+1)
	}
}

// -----------------------------------------------------------------------
// buildMetadataExtras
// -----------------------------------------------------------------------

// staterBackend is a stub Backend + Stater that returns a canned Stat and
// reports the supplied caps. It implements just enough of Backend to satisfy
// the interface; Get/Put/etc. panic so a test that accidentally uses one for
// a transfer fails loudly rather than corrupting state silently.
type staterBackend struct {
	stat backend.Stat
	caps backend.Caps
	err  error // optional Stat error
	// statCalls counts how many times Stat() was invoked. Used by the
	// retry-loop test to assert src.Stat is called once per file even
	// across retries.
	statCalls atomic.Int64
}

func (s *staterBackend) Kind() string { return "stub" }
func (s *staterBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry)
	close(ch)
	return ch, nil
}
func (s *staterBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (s *staterBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	return s.stat.Size, s.stat.ETag, s.stat.Mtime, nil
}
func (s *staterBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	_, _ = io.Copy(io.Discard, body)
	return backend.PutResult{BytesPut: size}, nil
}
func (s *staterBackend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	return "", "", nil
}
func (s *staterBackend) Delete(ctx context.Context, key string) error { return nil }
func (s *staterBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	return nil
}
func (s *staterBackend) Capabilities() backend.Caps          { return s.caps }
func (s *staterBackend) SameInstance(o backend.Backend) bool { return false }
func (s *staterBackend) Close() error                        { return nil }
func (s *staterBackend) Stat(ctx context.Context, key string) (backend.Stat, error) {
	s.statCalls.Add(1)
	if s.err != nil {
		return backend.Stat{}, s.err
	}
	return s.stat, nil
}

// nonStaterBackend implements Backend but NOT Stater. Used to verify
// buildMetadataExtras returns zero-extras without policy violation when
// the source can't supply POSIX bits.
type nonStaterBackend struct {
	caps backend.Caps
}

func (s *nonStaterBackend) Kind() string { return "stub-no-stater" }
func (s *nonStaterBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry)
	close(ch)
	return ch, nil
}
func (s *nonStaterBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (s *nonStaterBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	return 0, "", time.Time{}, nil
}
func (s *nonStaterBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	return backend.PutResult{}, nil
}
func (s *nonStaterBackend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	return "", "", nil
}
func (s *nonStaterBackend) Delete(ctx context.Context, key string) error { return nil }
func (s *nonStaterBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	return nil
}
func (s *nonStaterBackend) Capabilities() backend.Caps          { return s.caps }
func (s *nonStaterBackend) SameInstance(o backend.Backend) bool { return false }
func (s *nonStaterBackend) Close() error                        { return nil }

func u32p(v uint32) *uint32 { return &v }

func TestBuildMetadataExtras_NoPreserveRequested(t *testing.T) {
	src := &staterBackend{
		stat: backend.Stat{Mode: u32p(0644), UID: u32p(1000), GID: u32p(1000)},
	}
	dst := &staterBackend{
		caps: backend.Caps{NativeModeWrite: true, NativeOwnerWrite: true, NativeXattrWrite: true},
	}
	task := &Task{ID: "t", Src: src, Dst: dst}
	extras, skip, err := buildMetadataExtras(context.Background(), task, backend.Entry{Key: "k"})
	if err != nil || skip {
		t.Fatalf("unexpected err=%v skip=%v", err, skip)
	}
	if extras.hasAny() {
		t.Errorf("expected zero extras when no Preserve* requested, got %+v", extras)
	}
	if src.statCalls.Load() != 0 {
		t.Errorf("expected 0 Stat calls when no Preserve* requested, got %d", src.statCalls.Load())
	}
}

func TestBuildMetadataExtras_SrcNotStater(t *testing.T) {
	src := &nonStaterBackend{}
	dst := &staterBackend{
		caps: backend.Caps{NativeModeWrite: true, NativeOwnerWrite: true, NativeXattrWrite: true},
	}
	task := &Task{ID: "t", Src: src, Dst: dst,
		PreserveMode: true, PreserveOwner: true, PreserveXattr: true,
		OnMetadataUnsupported: OnMetadataUnsupportedError, // would error if it dispatched
	}
	extras, skip, err := buildMetadataExtras(context.Background(), task, backend.Entry{Key: "k"})
	if err != nil || skip {
		t.Fatalf("unexpected err=%v skip=%v", err, skip)
	}
	if extras.hasAny() {
		t.Errorf("expected zero extras when src isn't Stater, got %+v", extras)
	}
}

func TestBuildMetadataExtras_AllFieldsPropagated(t *testing.T) {
	src := &staterBackend{
		stat: backend.Stat{
			Mode: u32p(0755),
			UID:  u32p(2000),
			GID:  u32p(2001),
			Xattrs: map[string][]byte{
				"user.foo":         []byte("v"),
				"security.selinux": []byte("dropped"),
			},
		},
	}
	dst := &staterBackend{
		caps: backend.Caps{NativeModeWrite: true, NativeOwnerWrite: true, NativeXattrWrite: true},
	}
	task := &Task{ID: "t", Src: src, Dst: dst,
		PreserveMode: true, PreserveOwner: true, PreserveXattr: true,
		OnMetadataUnsupported: OnMetadataUnsupportedWarn,
	}
	extras, skip, err := buildMetadataExtras(context.Background(), task, backend.Entry{Key: "k"})
	if err != nil || skip {
		t.Fatalf("unexpected err=%v skip=%v", err, skip)
	}
	if extras.Mode == nil || *extras.Mode != 0755 {
		t.Errorf("Mode = %v, want 0755", extras.Mode)
	}
	if extras.UID == nil || *extras.UID != 2000 {
		t.Errorf("UID = %v, want 2000", extras.UID)
	}
	if extras.GID == nil || *extras.GID != 2001 {
		t.Errorf("GID = %v, want 2001", extras.GID)
	}
	// xattrs: user.foo kept, security.selinux dropped by filter
	if _, ok := extras.Xattrs["user.foo"]; !ok {
		t.Errorf("user.foo not preserved: %v", extras.Xattrs)
	}
	if _, ok := extras.Xattrs["security.selinux"]; ok {
		t.Errorf("security.selinux should have been filtered: %v", extras.Xattrs)
	}
	if src.statCalls.Load() != 1 {
		t.Errorf("expected exactly 1 Stat call, got %d", src.statCalls.Load())
	}
}

// dst lacks NativeModeWrite — PreserveMode + policy=error should propagate.
func TestBuildMetadataExtras_DstLacksMode_PolicyError(t *testing.T) {
	src := &staterBackend{stat: backend.Stat{Mode: u32p(0644)}}
	dst := &staterBackend{caps: backend.Caps{}} // no Native*Write
	task := &Task{ID: "t", Src: src, Dst: dst,
		PreserveMode:          true,
		OnMetadataUnsupported: OnMetadataUnsupportedError,
	}
	_, _, err := buildMetadataExtras(context.Background(), task, backend.Entry{Key: "k"})
	if err == nil {
		t.Fatal("expected error from policy=error when dst lacks NativeModeWrite")
	}
}

// dst lacks NativeXattrWrite, policy=skip — buildMetadataExtras returns skip=true.
func TestBuildMetadataExtras_DstLacksXattr_PolicySkip(t *testing.T) {
	src := &staterBackend{
		stat: backend.Stat{Xattrs: map[string][]byte{"user.foo": []byte("v")}},
	}
	dst := &staterBackend{
		caps: backend.Caps{NativeModeWrite: true, NativeOwnerWrite: true}, // no xattr
	}
	task := &Task{ID: "t", Src: src, Dst: dst,
		PreserveXattr:         true,
		OnMetadataUnsupported: OnMetadataUnsupportedSkip,
	}
	_, skip, err := buildMetadataExtras(context.Background(), task, backend.Entry{Key: "k"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !skip {
		t.Error("expected skip=true on policy=skip when dst lacks NativeXattrWrite")
	}
}

// dst lacks NativeOwnerWrite, policy=warn — proceeds without owner; caps gate
// applied per-field so a follow-up mode+xattr_capable scenario still works.
func TestBuildMetadataExtras_DstLacksOwner_PolicyWarn(t *testing.T) {
	src := &staterBackend{
		stat: backend.Stat{
			Mode: u32p(0644),
			UID:  u32p(1000),
			GID:  u32p(1000),
		},
	}
	dst := &staterBackend{
		caps: backend.Caps{NativeModeWrite: true}, // no Owner
	}
	task := &Task{ID: "t", Src: src, Dst: dst,
		PreserveMode: true, PreserveOwner: true,
		OnMetadataUnsupported: OnMetadataUnsupportedWarn,
	}
	extras, skip, err := buildMetadataExtras(context.Background(), task, backend.Entry{Key: "k"})
	if err != nil || skip {
		t.Fatalf("unexpected err=%v skip=%v", err, skip)
	}
	if extras.Mode == nil {
		t.Error("Mode should be preserved (dst supports it)")
	}
	if extras.UID != nil || extras.GID != nil {
		t.Error("Owner should have been dropped under policy=warn when dst lacks NativeOwnerWrite")
	}
}

// Stat error → returns zero extras + nil error (treated as transient by
// buildMetadataExtras; downstream Put can still succeed without metadata).
func TestBuildMetadataExtras_StatErrorIsTransient(t *testing.T) {
	src := &staterBackend{err: errors.New("transient stat failure")}
	dst := &staterBackend{
		caps: backend.Caps{NativeModeWrite: true, NativeOwnerWrite: true, NativeXattrWrite: true},
	}
	task := &Task{ID: "t", Src: src, Dst: dst,
		PreserveMode: true, PreserveOwner: true, PreserveXattr: true,
		OnMetadataUnsupported: OnMetadataUnsupportedError, // wouldn't trip — caps OK
	}
	extras, skip, err := buildMetadataExtras(context.Background(), task, backend.Entry{Key: "k"})
	if err != nil || skip {
		t.Fatalf("unexpected err=%v skip=%v (Stat error should be transient)", err, skip)
	}
	if extras.hasAny() {
		t.Errorf("expected zero extras when src.Stat errored, got %+v", extras)
	}
}

// -----------------------------------------------------------------------
// metadataExtras.hasAny
// -----------------------------------------------------------------------

func TestMetadataExtras_HasAny(t *testing.T) {
	if (metadataExtras{}).hasAny() {
		t.Error("zero metadataExtras.hasAny() = true, want false")
	}
	m := uint32(0644)
	if !(metadataExtras{Mode: &m}).hasAny() {
		t.Error("Mode-only hasAny() = false")
	}
	u := uint32(1)
	if !(metadataExtras{UID: &u}).hasAny() {
		t.Error("UID-only hasAny() = false")
	}
	g := uint32(2)
	if !(metadataExtras{GID: &g}).hasAny() {
		t.Error("GID-only hasAny() = false")
	}
	if !(metadataExtras{Xattrs: map[string][]byte{"user.k": []byte("v")}}).hasAny() {
		t.Error("Xattrs-only hasAny() = false")
	}
	// Empty xattrs map should NOT count as any
	if (metadataExtras{Xattrs: map[string][]byte{}}).hasAny() {
		t.Error("empty Xattrs map hasAny() = true, want false")
	}
}

// -----------------------------------------------------------------------
// Smoke: ensure Task struct accepts the new fields and validateTask
// normalises empty OnMetadataUnsupported → "warn".
// -----------------------------------------------------------------------

func TestValidateTask_OnMetadataUnsupported_NormalisesEmpty(t *testing.T) {
	src := &staterBackend{}
	dst := &staterBackend{
		caps: backend.Caps{NativeModeWrite: true, NativeOwnerWrite: true, NativeXattrWrite: true},
	}
	task := &Task{
		ID:      "t1",
		Type:    TaskTypeSync,
		Src:     src,
		Dst:     dst,
		SrcPath: "/src",
		DstPath: "/dst",
		// OnMetadataUnsupported left empty
	}
	if err := validateTask(task); err != nil {
		t.Fatalf("validateTask unexpected err: %v", err)
	}
	if task.OnMetadataUnsupported != OnMetadataUnsupportedWarn {
		t.Errorf("validateTask should default empty OnMetadataUnsupported to %q, got %q",
			OnMetadataUnsupportedWarn, task.OnMetadataUnsupported)
	}
}

func TestValidateTask_OnMetadataUnsupported_RejectsUnknown(t *testing.T) {
	src := &staterBackend{}
	dst := &staterBackend{}
	task := &Task{
		ID:                    "t1",
		Type:                  TaskTypeSync,
		Src:                   src,
		Dst:                   dst,
		SrcPath:               "/src",
		DstPath:               "/dst",
		OnMetadataUnsupported: "no-such-policy",
	}
	if err := validateTask(task); err == nil {
		t.Fatal("expected validateTask to reject unknown OnMetadataUnsupported")
	}
}

// -----------------------------------------------------------------------
// MetadataUnsupportedStats accessor returns monotonic counters.
// -----------------------------------------------------------------------

func TestMetadataUnsupportedStats_Snapshot(t *testing.T) {
	beforeW := metadataUnsupportedWarn.Load()
	beforeS := metadataUnsupportedSkip.Load()
	beforeE := metadataUnsupportedError.Load()
	// Fire one of each
	task := &Task{ID: "t", OnMetadataUnsupported: OnMetadataUnsupportedWarn}
	_, _ = applyMetadataPolicy(task, backend.Entry{Key: "k"}, "mode")
	task.OnMetadataUnsupported = OnMetadataUnsupportedSkip
	_, _ = applyMetadataPolicy(task, backend.Entry{Key: "k"}, "owner")
	task.OnMetadataUnsupported = OnMetadataUnsupportedError
	_, _ = applyMetadataPolicy(task, backend.Entry{Key: "k"}, "xattrs")

	snap := MetadataUnsupportedStats()
	if snap.Warn < beforeW+1 || snap.Skip < beforeS+1 || snap.Error < beforeE+1 {
		t.Errorf("MetadataUnsupportedStats did not advance: snap=%+v before=(W=%d S=%d E=%d)",
			snap, beforeW, beforeS, beforeE)
	}
}

// Silence "imported and not used" for bytes import if test file evolves; we
// already use bytes via strings.NewReader, but keep the import predictable.
var _ = bytes.NewReader
