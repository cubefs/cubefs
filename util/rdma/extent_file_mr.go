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

// prefaultSink is a package-level byte that the prefault-touch loop
// XORs into. Storing into a package var forces the compiler to
// preserve every memory load in the loop body — without this, Go's
// SSA dead-code elimination would prove the loop pointless and
// remove every page touch, defeating the whole prefault.
var prefaultSink byte

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

	// Prefault every page so the kernel maps the file into page cache
	// synchronously, in user context, BEFORE we hand the buffer to the
	// NIC via ibv_reg_mr(ODP). Without this, the first remote RDMA
	// Read against a not-yet-resident page triggers a NIC-side ODP
	// page fault; on mlx5 that fault path is known to take seconds
	// under load, and the SDK's 5s WR timeout doesn't survive it.
	//
	// Belt-and-suspenders ordering — earlier attempts that used
	// "_ = mmapBuf[off]" alone got compiled away (Go SSA treats the
	// blank-identifier read as dead code; only the bounds check
	// remains, which doesn't touch the page). Production confirmed
	// this: WR timeouts persisted with the bare-touch loop. So:
	//
	//   1) MADV_WILLNEED: kernel hint to read-ahead the range. Async
	//      on its own but kernels actually start the readahead now,
	//      so by the time we get to (2) most pages are already in.
	//   2) Mlock: synchronously page in AND lock pages resident in
	//      RAM. This is the guarantee we need — once mlock returns,
	//      every page in the mmap'd range is in the page table and
	//      stays there until munlock / Munmap. mlock makes the
	//      page-touch loop redundant but we keep both for the case
	//      where mlock returns EAGAIN (transient resource pressure)
	//      or EPERM (RLIMIT_MEMLOCK exhausted).
	//   3) Page-touch loop with sink variable: fallback prefault
	//      that the Go compiler cannot eliminate (atomic-load via
	//      runtime.KeepAlive escape would also work; XOR-accumulate
	//      into a stack var is simpler and equally effective).
	//
	// Cost: prefault adds ~1ms per MB on NVMe-backed page cache;
	// extents in this cluster are typically ~1MB so the per-extent
	// register time goes up by a millisecond or so — trivially
	// dominated by the network RTT of the lookup round-trip the
	// register is serving.
	_ = syscall.Madvise(mmapBuf, syscall.MADV_WILLNEED)
	mlocked := syscall.Mlock(mmapBuf) == nil
	if !mlocked {
		// Mlock failed (probably RLIMIT_MEMLOCK). Fall back to
		// software prefault. sink prevents the compiler from
		// killing the read; the XOR keeps the value live across
		// loop iterations so SSA can't prove it dead.
		var sink byte
		pageSize := os.Getpagesize()
		for off := 0; off < size; off += pageSize {
			sink ^= mmapBuf[off]
		}
		// runtime.KeepAlive isn't strictly required for a stack-local
		// byte that we read after the loop, but make the dependency
		// explicit so future refactors don't lose it.
		prefaultSink ^= sink
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
