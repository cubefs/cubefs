//go:build linux

package metanode

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// isNvmeDisk determines whether the filesystem backing dirPath is on an NVMe device.
// It returns (isNvme, mountSourceDevice, error).
//
// Implementation notes:
// - Resolve dirPath mount source from /proc/self/mountinfo (longest mountpoint prefix match).
// - If the mount source is a mapper/dm device, recursively inspect /sys/class/block/<dev>/slaves.
func isNvmeDisk(dirPath string) (bool, string, error) {
	if dirPath == "" {
		return false, "", fmt.Errorf("empty path")
	}

	p := filepath.Clean(dirPath)
	if rp, err := filepath.EvalSymlinks(p); err == nil && rp != "" {
		p = rp
	}

	source, err := mountSourceForPath(p)
	if err != nil {
		return false, "", err
	}

	// Non-block sources (e.g. tmpfs/overlay/nfs) are treated as non-NVMe.
	if !strings.HasPrefix(source, "/dev/") {
		return false, source, nil
	}

	srcResolved := source
	if rr, err := filepath.EvalSymlinks(source); err == nil && rr != "" {
		srcResolved = rr
	}

	block := strings.TrimPrefix(srcResolved, "/dev/")
	if strings.HasPrefix(block, "nvme") {
		return true, source, nil
	}

	// Handle dm-/md-/mapper devices via sysfs recursion.
	if isNvmeBySysfs(block, 4) {
		return true, source, nil
	}
	return false, source, nil
}

type mountInfoEntry struct {
	mountPoint string
	source     string
}

func mountSourceForPath(absPath string) (string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer f.Close()

	var best mountInfoEntry
	bestLen := -1

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		// mountinfo format: https://man7.org/linux/man-pages/man5/proc.5.html
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}

		left := strings.Fields(parts[0])
		right := strings.Fields(parts[1])
		if len(left) < 5 || len(right) < 2 {
			continue
		}

		mp := unescapeMountInfoPath(left[4])
		src := right[1] // right: fstype source superopts...

		// Longest mountpoint prefix match.
		if !pathHasMountPrefix(absPath, mp) {
			continue
		}
		if len(mp) > bestLen {
			best = mountInfoEntry{mountPoint: mp, source: src}
			bestLen = len(mp)
		}
	}

	if err := sc.Err(); err != nil {
		return "", err
	}
	if bestLen < 0 {
		return "", fmt.Errorf("no mountinfo entry matches path: %s", absPath)
	}
	return best.source, nil
}

func pathHasMountPrefix(p, mountPoint string) bool {
	if mountPoint == "/" {
		return true
	}
	if p == mountPoint {
		return true
	}
	// Ensure prefix match aligns on path segment boundary.
	if strings.HasPrefix(p, mountPoint) {
		if len(p) > len(mountPoint) && (p[len(mountPoint)] == '/' || mountPoint[len(mountPoint)-1] == '/') {
			return true
		}
	}
	return false
}

// unescapeMountInfoPath converts mountinfo escaped sequences (e.g. \040 for space).
func unescapeMountInfoPath(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+3 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		// Try octal escape: \XYZ
		oct := s[i+1 : i+4]
		if oct[0] < '0' || oct[0] > '7' || oct[1] < '0' || oct[1] > '7' || oct[2] < '0' || oct[2] > '7' {
			b.WriteByte(s[i])
			continue
		}
		v, err := strconv.ParseInt(oct, 8, 32)
		if err != nil {
			b.WriteByte(s[i])
			continue
		}
		b.WriteByte(byte(v))
		i += 3
	}
	return b.String()
}

func isNvmeBySysfs(block string, depth int) bool {
	if depth <= 0 || block == "" {
		return false
	}
	if strings.HasPrefix(block, "nvme") {
		return true
	}

	slavesDir := filepath.Join("/sys/class/block", block, "slaves")
	ents, err := os.ReadDir(slavesDir)
	if err != nil {
		return false
	}
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}
		if isNvmeBySysfs(ent.Name(), depth-1) {
			return true
		}
	}
	return false
}
