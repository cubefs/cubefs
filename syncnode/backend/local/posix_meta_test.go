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

//go:build linux || darwin

package local

import (
	"bytes"
	"context"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/syncnode/backend"
	"golang.org/x/sys/unix"
)

// TestPut_PreserveModeRoundTrip writes a file via Put with PutOptions.Mode
// set and reads it back via Stat to verify the mode bits survive the
// rename + chmod sequence.
func TestPut_PreserveModeRoundTrip(t *testing.T) {
	b, _ := newBackend(t)
	stater, ok := b.(backend.Stater)
	if !ok {
		t.Fatalf("local backend must implement backend.Stater")
	}

	cases := []uint32{0o600, 0o640, 0o644, 0o755, 0o4755}
	for _, mode := range cases {
		key := "mode_" + modeToOctal(mode)
		body := []byte("mode-test " + key)

		modeIn := mode
		_, err := b.Put(context.Background(), key, bytes.NewReader(body), int64(len(body)), backend.PutOptions{
			Mode: &modeIn,
		})
		if err != nil {
			t.Fatalf("Put(%s, mode=%o): %v", key, mode, err)
		}

		st, err := stater.Stat(context.Background(), key)
		if err != nil {
			t.Fatalf("Stat(%s): %v", key, err)
		}
		if st.Mode == nil {
			t.Fatalf("Stat(%s): Mode is nil", key)
		}
		// Compare only the permission/setuid/setgid/sticky bits — the
		// inode's file-type bits (S_IFREG etc.) are part of st.Mode
		// returned by lstat but not what PutOptions.Mode sets.
		got := *st.Mode & 0o7777
		if got != mode {
			t.Errorf("Stat(%s): mode = %o, want %o", key, got, mode)
		}
	}
}

// TestPut_PreserveOwnerRoundTrip uses the current process's own uid/gid so
// the test does not require root. It just verifies the syscall is exercised
// and the returned Stat carries non-nil UID/GID pointers.
func TestPut_PreserveOwnerRoundTrip(t *testing.T) {
	b, _ := newBackend(t)
	stater, ok := b.(backend.Stater)
	if !ok {
		t.Fatalf("local backend must implement backend.Stater")
	}

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	key := "owner_self"
	body := []byte("owner-test")
	if _, err := b.Put(context.Background(), key, bytes.NewReader(body), int64(len(body)), backend.PutOptions{
		UID: &uid,
		GID: &gid,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st, err := stater.Stat(context.Background(), key)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.UID == nil || st.GID == nil {
		t.Fatalf("Stat: UID/GID must be non-nil on unix")
	}
	if *st.UID != uid {
		t.Errorf("UID = %d, want %d", *st.UID, uid)
	}
	if *st.GID != gid {
		t.Errorf("GID = %d, want %d", *st.GID, gid)
	}
}

// TestPut_PreserveXattrRoundTrip exercises the xattr write path with a
// `user.*` name (the only namespace writeable without CAP_SYS_ADMIN on
// Linux). Skipped on filesystems without xattr support — we detect the
// degraded path by writing once and seeing if Stat returns the value.
func TestPut_PreserveXattrRoundTrip(t *testing.T) {
	b, root := newBackend(t)
	if !filesystemSupportsXattr(t, root) {
		t.Skipf("filesystem at %q does not support user.* xattrs (likely tmpfs without user_xattr)", root)
	}
	stater, ok := b.(backend.Stater)
	if !ok {
		t.Fatalf("local backend must implement backend.Stater")
	}

	xattrs := map[string][]byte{
		"user.syncnode.test":  []byte("hello-world"),
		"user.syncnode.empty": []byte{},
	}
	key := "xattr_user"
	body := []byte("xattr-test")
	if _, err := b.Put(context.Background(), key, bytes.NewReader(body), int64(len(body)), backend.PutOptions{
		Xattrs: xattrs,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st, err := stater.Stat(context.Background(), key)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	for name, want := range xattrs {
		got, ok := st.Xattrs[name]
		if !ok {
			t.Errorf("xattr %q missing from Stat result", name)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("xattr %q = %q, want %q", name, got, want)
		}
	}
}

// TestPut_PreserveAllRoundTrip combines mode + owner + xattr in one call to
// catch ordering bugs (e.g. chmod losing setuid bit after chown).
func TestPut_PreserveAllRoundTrip(t *testing.T) {
	b, root := newBackend(t)
	stater, _ := b.(backend.Stater)
	if stater == nil {
		t.Fatalf("local backend must implement backend.Stater")
	}
	mode := uint32(0o640)
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	opts := backend.PutOptions{
		Mode: &mode,
		UID:  &uid,
		GID:  &gid,
	}
	if filesystemSupportsXattr(t, root) {
		opts.Xattrs = map[string][]byte{"user.syncnode.k": []byte("v")}
	}
	body := []byte("combined-test")
	if _, err := b.Put(context.Background(), "combined", bytes.NewReader(body), int64(len(body)), opts); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st, err := stater.Stat(context.Background(), "combined")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Mode == nil || (*st.Mode&0o7777) != mode {
		t.Errorf("mode mismatch: got %v want %o", st.Mode, mode)
	}
	if st.UID == nil || *st.UID != uid {
		t.Errorf("uid mismatch: got %v want %d", st.UID, uid)
	}
	if st.GID == nil || *st.GID != gid {
		t.Errorf("gid mismatch: got %v want %d", st.GID, gid)
	}
	if opts.Xattrs != nil {
		if v, ok := st.Xattrs["user.syncnode.k"]; !ok || string(v) != "v" {
			t.Errorf("xattr round-trip failed: ok=%v v=%q", ok, v)
		}
	}
}

// modeToOctal renders the mode for use in a file key (avoids '/' / '%').
func modeToOctal(m uint32) string {
	const digits = "01234567"
	var buf [6]byte
	i := len(buf) - 1
	for m > 0 && i >= 0 {
		buf[i] = digits[m&7]
		m >>= 3
		i--
	}
	// Drop leading nul bytes.
	out := string(bytes.TrimLeft(buf[:], "\x00"))
	if out == "" {
		out = "0"
	}
	return out
}

// filesystemSupportsXattr probes whether the temp directory supports
// user.* xattrs. tmpfs on linux honours them only when mounted with
// user_xattr; macOS supports them everywhere.
func filesystemSupportsXattr(t *testing.T, root string) bool {
	t.Helper()
	probe, err := os.CreateTemp(root, "xattr_probe_*")
	if err != nil {
		t.Fatalf("CreateTemp(%q): %v", root, err)
	}
	probe.Close()
	defer os.Remove(probe.Name())

	err = unix.Lsetxattr(probe.Name(), "user.probe", []byte("x"), 0)
	if err == nil {
		return true
	}
	if errIsXattrUnsupported(err) {
		return false
	}
	// Other errors (permission denied on macOS quarantined paths etc.):
	// treat as supported so the test runs; if it then fails for a real
	// reason the developer sees a useful error.
	t.Logf("xattr probe on %q returned %v on %s; assuming supported", root, err, runtime.GOOS)
	return true
}

// errIsXattrUnsupported mirrors isXattrUnsupported but lives in the test
// file to avoid cluttering the production helper with an exported name.
func errIsXattrUnsupported(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range []error{syscall.ENOTSUP, syscall.EOPNOTSUPP, syscall.ENODATA} {
		if isErr(err, e) {
			return true
		}
	}
	// Some filesystems return EPERM rather than ENOTSUP; treat the common
	// "Operation not permitted" message as unsupported too.
	return strings.Contains(err.Error(), "not permitted") || strings.Contains(err.Error(), "not supported")
}

func isErr(err error, target error) bool {
	for cur := err; cur != nil; {
		if cur == target {
			return true
		}
		u, ok := cur.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		cur = u.Unwrap()
	}
	return false
}
