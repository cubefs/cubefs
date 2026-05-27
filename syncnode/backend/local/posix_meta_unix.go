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

// POSIX metadata helpers used by the local backend's Stat / Put extensions.
// Linux + Darwin only because the xattr syscalls have different shapes on
// other platforms; non-unix builds get a stub (see posix_meta_stub.go) that
// preserves API surface but reports "unsupported".
//
// All callers must pass paths already validated by resolveSafe.
package local

import (
	"errors"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// posixMetaSupported reports whether the running platform exposes the full
// mode/uid/gid/xattr stack via syscalls. Used by Capabilities() and tests.
const posixMetaSupported = true

// readPosixMeta returns (mode, uid, gid, xattrs) for path. The caller picks
// which fields to use. Errors from listxattr/getxattr are downgraded to
// "no xattrs" because some filesystems disable xattr without rejecting the
// call cleanly (e.g. tmpfs with nouser_xattr); the stat() pieces are
// authoritative and a missing xattr table is not a fatal error for Stat.
func readPosixMeta(path string) (mode uint32, uid uint32, gid uint32, xattrs map[string][]byte, err error) {
	var st syscall.Stat_t
	if err = syscall.Lstat(path, &st); err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0, nil, os.ErrNotExist
		}
		return 0, 0, 0, nil, err
	}
	mode = uint32(st.Mode)
	uid = uint32(st.Uid)
	gid = uint32(st.Gid)
	xattrs, _ = listAllXattrs(path) // best effort
	return mode, uid, gid, xattrs, nil
}

// applyPosixMeta applies the requested subset to path after the rename. Each
// nil/empty input means "leave unchanged". Errors are returned as a single
// joined error so the caller (Put) can surface every failure at once instead
// of stopping at the first one — useful when an operator misconfigures a
// destination that supports chmod but not xattr.
func applyPosixMeta(path string, mode *uint32, uid *uint32, gid *uint32, xattrs map[string][]byte) error {
	var errs []error

	if mode != nil {
		// os.Chmod cannot set setuid/setgid/sticky bits on every platform; we
		// use syscall.Chmod directly so the full mode word survives.
		if err := syscall.Chmod(path, *mode); err != nil {
			errs = append(errs, &metaErr{op: "chmod", err: err})
		}
	}

	if uid != nil || gid != nil {
		// Either side that is nil is passed as -1 (= "leave unchanged"). We
		// always use Lchown so symlinks don't deref — local backend treats
		// symlinks per OnSymlink policy and never wants Put to dereference.
		newUID := -1
		newGID := -1
		if uid != nil {
			newUID = int(*uid)
		}
		if gid != nil {
			newGID = int(*gid)
		}
		if err := os.Lchown(path, newUID, newGID); err != nil {
			errs = append(errs, &metaErr{op: "lchown", err: err})
		}
	}

	for name, value := range xattrs {
		// Skip empty names defensively. Names with reserved prefixes
		// (security.*, trusted.*) are caller-filtered — backend writes
		// whatever it's handed (see PutOptions.Xattrs Godoc).
		if name == "" {
			continue
		}
		if err := unix.Lsetxattr(path, name, value, 0); err != nil {
			errs = append(errs, &metaErr{op: "setxattr:" + name, err: err})
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// listAllXattrs reads every xattr on path. Returns nil map when path has
// no xattrs or the filesystem doesn't support them; only hard syscall
// failures (other than ENOTSUP / ENODATA / ERANGE-on-empty) are returned
// as errors.
func listAllXattrs(path string) (map[string][]byte, error) {
	// Probe size with an empty buffer first; some platforms (Darwin) return
	// the actual size when called with size=0.
	size, err := unix.Llistxattr(path, nil)
	if err != nil {
		if isXattrUnsupported(err) {
			return nil, nil
		}
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	n, err := unix.Llistxattr(path, buf)
	if err != nil {
		if isXattrUnsupported(err) {
			return nil, nil
		}
		return nil, err
	}
	names := splitNullTerminated(buf[:n])
	if len(names) == 0 {
		return nil, nil
	}
	out := make(map[string][]byte, len(names))
	for _, name := range names {
		// Probe value size.
		vsize, gerr := unix.Lgetxattr(path, name, nil)
		if gerr != nil {
			if isXattrUnsupported(gerr) {
				continue
			}
			return nil, gerr
		}
		val := make([]byte, vsize)
		if vsize > 0 {
			n2, gerr := unix.Lgetxattr(path, name, val)
			if gerr != nil {
				if isXattrUnsupported(gerr) {
					continue
				}
				return nil, gerr
			}
			val = val[:n2]
		}
		out[name] = val
	}
	return out, nil
}

// isXattrUnsupported tells whether err signals "this filesystem / platform
// doesn't support xattr" rather than a true I/O failure. We use this to
// silently degrade to "no xattrs" instead of failing Stat outright.
func isXattrUnsupported(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOTSUP) {
		return true
	}
	// Linux: EOPNOTSUPP for filesystems without xattr support; on some
	// platforms it equals ENOTSUP, on others it's distinct.
	if errors.Is(err, syscall.EOPNOTSUPP) {
		return true
	}
	if errors.Is(err, syscall.ENODATA) {
		// no such attribute — treat as "no xattrs"
		return true
	}
	return false
}

// splitNullTerminated splits a Linux-style \x00-delimited buffer into names.
// Darwin returns the same shape from listxattr.
func splitNullTerminated(buf []byte) []string {
	if len(buf) == 0 {
		return nil
	}
	parts := strings.Split(string(buf), "\x00")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// metaErr wraps a single syscall failure with the operation name so callers
// (and Errors.Join consumers) can surface "which syscall actually failed".
type metaErr struct {
	op  string
	err error
}

func (e *metaErr) Error() string { return "posix-meta " + e.op + ": " + e.err.Error() }
func (e *metaErr) Unwrap() error { return e.err }
