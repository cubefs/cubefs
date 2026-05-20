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

package syncnode

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/cubefs/cubefs/proto"
)

// detectMountPoints walks every path in cfg.Posix.AllowedRoots and resolves
// the underlying fs type by scanning /proc/self/mountinfo (longest-prefix
// match — covers nested mounts and subdir-of-mount cases).
//
// Output preserves the order of allowedRoots so master sees a stable list.
// Entries are dropped when the resolved fs type is one of the kernel's
// pseudo / container-internal types (overlay, tmpfs, proc, …) — those
// surfaces are useless as bench targets and only cluttered the dashboard
// dropdown. Real filesystems (fuse.cubefs, ext4, xfs, gpfs, nfs, lustre,
// ceph, …) and unknown fs types pass through. Entries whose fs type can't
// be determined (e.g. when /proc/self/mountinfo isn't readable on this
// kernel) keep an empty FSType — consoles still get the path string and
// can fall back to a "unknown" classification.
//
// The result is intentionally a plain slice, not a map: bench rule
// targets must round-trip through allowedRoots verbatim, so preserving
// order and case keeps the heartbeat payload minimal and the dashboard
// dropdown deterministic.
func detectMountPoints(allowedRoots []string) []proto.SyncNodeMountPoint {
	if len(allowedRoots) == 0 {
		return nil
	}
	mounts := readMountinfo("/proc/self/mountinfo")

	out := make([]proto.SyncNodeMountPoint, 0, len(allowedRoots))
	for _, root := range allowedRoots {
		clean := filepath.Clean(root)
		fsType := longestPrefixMountType(mounts, clean)
		if isPseudoFSType(fsType) {
			// Drop overlay rootfs subdirs, tmpfs, /proc, /sys, cgroup,
			// etc. — they exist inside every container but make zero
			// sense as bench targets and only pollute the dashboard
			// dropdown. The path is still kept inside the syncnode's
			// allowedRoots so an explicit BenchRule referencing it is
			// not blocked at validation time; only auto-discovery
			// hides it.
			continue
		}
		out = append(out, proto.SyncNodeMountPoint{
			Path:   clean,
			FSType: fsType,
		})
	}
	return out
}

// pseudoFSTypes lists the kernel filesystems that are NEVER real bench
// targets. These come in two flavours:
//
//   - virtual / kernel-only: proc, sysfs, cgroup(2), bpf, tracefs,
//     securityfs, debugfs, configfs, fusectl, mqueue, pstore, ramfs,
//     hugetlbfs, autofs, binfmt_misc, nsfs, rpc_pipefs, selinuxfs,
//     devtmpfs, devpts — entirely synthesised by the kernel
//   - container plumbing: overlay/overlay2 (the container's own root
//     overlay), tmpfs (always backed by RAM, often used for /run, /dev/shm),
//     squashfs (read-only image layers)
//
// Real filesystems intentionally NOT listed (so they DO surface in the
// dropdown): fuse.cubefs, ext4, xfs, btrfs, zfs, nfs(4), gpfs, lustre,
// cifs/smbfs/smb3, glusterfs, ceph/cephfs, fuse.* (other fuse impls),
// 9p, virtiofs.
var pseudoFSTypes = map[string]struct{}{
	"overlay":     {},
	"overlay2":    {},
	"tmpfs":       {},
	"proc":        {},
	"sysfs":       {},
	"cgroup":      {},
	"cgroup2":     {},
	"devtmpfs":    {},
	"devpts":      {},
	"mqueue":      {},
	"pstore":      {},
	"ramfs":       {},
	"bpf":         {},
	"tracefs":     {},
	"securityfs":  {},
	"debugfs":     {},
	"fusectl":     {},
	"configfs":    {},
	"hugetlbfs":   {},
	"autofs":      {},
	"binfmt_misc": {},
	"squashfs":    {},
	"nsfs":        {},
	"rpc_pipefs":  {},
	"selinuxfs":   {},
}

// isPseudoFSType reports whether fsType should be hidden from the
// dashboard mount dropdown. Empty string returns false — unknown types
// are kept so an operator can still spot them and decide.
func isPseudoFSType(fsType string) bool {
	if fsType == "" {
		return false
	}
	_, ok := pseudoFSTypes[fsType]
	return ok
}

// mountInfoEntry is one row from /proc/self/mountinfo we care about.
// Field 5 (mount point) and field 9 (fs type) per kernel docs:
// https://www.kernel.org/doc/Documentation/filesystems/proc.txt
type mountInfoEntry struct {
	mountPoint string
	fsType     string
}

// readMountinfo parses the kernel's mountinfo file. Errors degrade to an
// empty slice — the caller surfaces empty FSType in that case so
// downstream consumers still see the paths.
//
// The file is small (one line per mount) and parsing is one-shot at
// startup, so we avoid bringing in a dependency.
func readMountinfo(path string) []mountInfoEntry {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	out := make([]mountInfoEntry, 0, 16)
	sc := bufio.NewScanner(f)
	// /proc/self/mountinfo lines stay well under 64 KiB even on
	// pathological hosts, but bump the buffer so we don't truncate.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		// Format: ID parentID major:minor root mount-point options - fsType source super-options
		// We split on " - " first to find the fs-type marker, then take
		// the 5th field of the prefix (mount point) and the 1st of the suffix (fs type).
		i := strings.Index(line, " - ")
		if i < 0 {
			continue
		}
		prefix := line[:i]
		suffix := line[i+3:]
		pf := strings.Fields(prefix)
		sf := strings.Fields(suffix)
		if len(pf) < 5 || len(sf) < 1 {
			continue
		}
		out = append(out, mountInfoEntry{
			mountPoint: pf[4],
			fsType:     sf[0],
		})
	}
	return out
}

// longestPrefixMountType returns the fs type of the deepest mount point
// that is a prefix of `path`. This is how the kernel itself resolves
// "which mount does this path live under" — necessary because
// allowedRoots may point at a subdirectory of a mount (e.g.
// `/cfs/posix-bench/scratch` lives on the same fuse.cubefs mount as
// `/cfs/posix-bench` itself).
//
// Empty mounts list (mountinfo unreadable) returns "" — the caller
// surfaces that as FSType="" and the dashboard treats it as unknown.
func longestPrefixMountType(mounts []mountInfoEntry, path string) string {
	bestLen := -1
	bestType := ""
	for _, m := range mounts {
		if !isPathPrefix(m.mountPoint, path) {
			continue
		}
		if len(m.mountPoint) > bestLen {
			bestLen = len(m.mountPoint)
			bestType = m.fsType
		}
	}
	return bestType
}

// isPathPrefix reports whether `prefix` is the same as `path` or a parent
// directory of `path`. We don't use strings.HasPrefix because that would
// treat /cfs/posix-bench-extra as a child of /cfs/posix-bench.
func isPathPrefix(prefix, path string) bool {
	if prefix == path {
		return true
	}
	// Root mount "/" is the prefix of every absolute path. Special-case
	// it so we don't append a trailing "/" and double up.
	if prefix == "/" {
		return strings.HasPrefix(path, "/")
	}
	return strings.HasPrefix(path, prefix+"/")
}
