//go:build linux && rdma

package rdma

/*
#include <infiniband/verbs.h>
*/
import "C"

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// RegisterExtentFile opens path, mmap's the entire file (size taken
// from the file's stat — extent files should be at their final size
// before this is called), and registers the mmap region as an MR
// against pd via RegisterFileMR (which tries ODP first, falling
// back to pinned on HCAs that don't support it).
//
// The returned RDMAMem's Free() releases the MR, munmaps the
// region, and closes the fd — callers can drop the RDMAMem and
// stop tracking the file separately.
//
// readOnly=true opens with O_RDONLY + PROT_READ, which is correct
// for the one-sided read fast path (server-side MR exposed for
// client RDMA Reads). Writes to the underlying file from the disk
// path still propagate to the mmap'd view because both share the
// same page cache (file is in the same inode).
func RegisterExtentFile(pd *C.struct_ibv_pd, path string, readOnly bool) (*RDMAMem, bool, error) {
	if pd == nil {
		return nil, false, fmt.Errorf("rdma: RegisterExtentFile: nil pd")
	}
	flags := os.O_RDWR
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	if readOnly {
		flags = os.O_RDONLY
		prot = syscall.PROT_READ
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, false, fmt.Errorf("rdma: RegisterExtentFile: open %s: %w", path, err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, false, fmt.Errorf("rdma: RegisterExtentFile: stat %s: %w", path, err)
	}
	size := int(stat.Size())
	if size <= 0 {
		f.Close()
		return nil, false, fmt.Errorf("rdma: RegisterExtentFile: zero-size file %s", path)
	}

	mmapBuf, err := syscall.Mmap(int(f.Fd()), 0, size, prot, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, false, fmt.Errorf("rdma: RegisterExtentFile: mmap %s (size=%d): %w", path, size, err)
	}

	base := uintptr(unsafe.Pointer(&mmapBuf[0]))
	mem, usedODP, err := RegisterFileMR(pd, base, size)
	if err != nil {
		_ = syscall.Munmap(mmapBuf)
		f.Close()
		return nil, false, fmt.Errorf("rdma: RegisterExtentFile: regMR %s: %w", path, err)
	}
	// Capture the mmap slice + file by value so the closure keeps
	// them alive until Free() runs. Order matters: munmap before
	// f.Close so the kernel doesn't reject either step.
	mem.SetCleanup(func() {
		_ = syscall.Munmap(mmapBuf)
		_ = f.Close()
	})
	return mem, usedODP, nil
}

// RegisterExtentFile is a method form of the package-level function
// above; it pins all of the conn's PD-management inside the package
// so callers don't have to touch *C.struct_ibv_pd directly.
func (c *RDMAConn) RegisterExtentFile(path string, readOnly bool) (*RDMAMem, bool, error) {
	if c.pd == nil {
		return nil, false, fmt.Errorf("rdma: RegisterExtentFile on conn with nil PD")
	}
	return RegisterExtentFile(c.pd, path, readOnly)
}
