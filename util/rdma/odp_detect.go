//go:build linux && rdma

package rdma

/*
#include <infiniband/verbs.h>
*/
import "C"

import (
	"errors"
	"sync"
	"unsafe"
)

// RegisterFileMR registers an externally-allocated buffer (typically
// an mmap'd file) with the given PD, preferring On-Demand Paging
// when the HCA supports it. Strategy:
//
//   1. Try with onDemand=true. If the kernel returns success → ODP
//      is supported on this PD's device → record that for future
//      calls on the same PD and reuse the result.
//   2. If ODP returns an error → fall back to a pinned registration.
//      The caller pays full pinned-memory cost but the read still
//      works.
//
// Returns (mem, usedODP, error). usedODP reports which strategy
// actually succeeded so the caller can include it in metrics / logs
// and decide whether to apply per-region size caps.
//
// The caller MUST keep the underlying memory alive for the lifetime
// of the returned RDMAMem; Free() releases only the MR registration.
func RegisterFileMR(pd *C.struct_ibv_pd, base uintptr, size int) (*RDMAMem, bool, error) {
	if pd == nil {
		return nil, false, errors.New("rdma: RegisterFileMR: nil pd")
	}
	// TEMP DIAG (Phase A debug): force PINNED registration to test
	// whether the 5s RDMA Read timeout is caused by ODP page-fault
	// latency on the server side. Production log shows all rkey/VA/
	// size match end-to-end, so the failure is not in the SDK — most
	// likely the mlx5 ODP fault path. If pinned regs fix the timeout
	// we know ODP is the culprit and can either:
	//   - keep pinned for read-path (cost: more pinned memory)
	//   - add IBV_ADVISE_MR_FLAG_FLUSH prefetch on the MR right after
	//     registration so first-touch is paid up front
	// If pinned regs ALSO time out, ODP is not the issue and we
	// move to NIC counter analysis (ethtool -S mlx5_bond_0).
	const odpForceOff = true
	if !odpForceOff && odpSupported(pd) {
		if mem, err := RegisterRDMABuffer(pd, base, size, true); err == nil {
			return mem, true, nil
		}
		// ODP probe said yes but this specific registration failed
		// (could be size/alignment/etc.). Fall through to pinned
		// instead of failing the caller.
	}
	mem, err := RegisterRDMABuffer(pd, base, size, false)
	if err != nil {
		return nil, false, err
	}
	return mem, false, nil
}

var (
	odpProbeMu    sync.Mutex
	odpProbeCache = map[uintptr]bool{} // pd pointer → ODP support
)

// odpSupported probes whether the given PD can register MRs with
// IBV_ACCESS_ON_DEMAND. The probe registers a 4 KiB MR with ODP
// requested and immediately deregisters it. Results are cached per
// PD so the cost is paid once per device context.
//
// Note: cached results are tied to the PD pointer value; if a PD is
// destroyed and a new one allocated at the same address (rare but
// possible) we'd return a stale cached result. That's a soft
// failure — the worst case is RegisterFileMR retries pinned on a
// genuinely unsupported PD, paying one extra dereg cost.
func odpSupported(pd *C.struct_ibv_pd) bool {
	key := uintptr(unsafe.Pointer(pd))

	odpProbeMu.Lock()
	if v, ok := odpProbeCache[key]; ok {
		odpProbeMu.Unlock()
		return v
	}
	odpProbeMu.Unlock()

	// Probe with a small Go-pinned buffer. The C side keeps the
	// pointer only for the duration of the regMR/dereg pair, so the
	// usual cgo unsafe-pointer rules apply (must stay alive until C
	// returns, which both calls satisfy synchronously).
	probe := make([]byte, 4096)
	mr, err := regMRWithODP(pd, unsafe.Pointer(&probe[0]), len(probe), true)
	supported := err == nil
	if supported {
		C.ibv_dereg_mr(mr)
	}

	odpProbeMu.Lock()
	odpProbeCache[key] = supported
	odpProbeMu.Unlock()
	return supported
}

// resetODPProbeCacheForTest clears the per-PD probe cache. Test-only
// hook so unit tests can rerun the probe under controlled conditions.
func resetODPProbeCacheForTest() {
	odpProbeMu.Lock()
	odpProbeCache = map[uintptr]bool{}
	odpProbeMu.Unlock()
}
