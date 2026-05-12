// Copyright 2018 The CubeFS Authors.
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

package rdma

import (
	"sync"
	"unsafe"
)

// One Protection Domain per device, shared across every conn dialed
// on / accepted from that device.
//
// Why this matters: PD is the verbs abstraction that scopes memory
// registrations. An MR registered against PD X is ONLY addressable
// from QPs that also live under PD X. The naive default of "one PD
// per conn" looks innocuous but means an MR registered while serving
// one conn (e.g. an extent MR created during OpExtentMRLookup on
// the two-sided conn) is INVISIBLE to a different conn from the
// same peer (e.g. the Phase A read-only conn that wants to RDMA
// Read against that rkey). The peer NIC silently NAKs forever and
// surfaces client-side as a 5s timeout — exactly the failure mode
// the Phase A read-only pool deploy hit, after the previous deploy
// had been hiding the same issue behind a single shared QP.
//
// Lifecycle: the existing codebase never called ibv_dealloc_pd
// anywhere, so a singleton with no release path keeps the exact
// same lifetime as before — the OS reclaims the PD at process exit.
// No refcount, no shutdown ordering trap.
//
// Build-tag-free so the cache logic can be unit tested on darwin;
// verbs.go (rdma-tagged) injects the real ibv_alloc_pd allocator.

// pdAllocator opens a fresh PD for the device key. Returns a raw
// unsafe.Pointer so this file doesn't need cgo types and can compile
// without the rdma build tag.
type pdAllocator func(ctxKey uintptr) (unsafe.Pointer, error)

var (
	pdCacheMu sync.Mutex
	pdCache   = map[uintptr]unsafe.Pointer{} // ibv_context* → shared PD
)

// getOrAllocPDCached returns the singleton PD pointer for ctxKey,
// invoking alloc on first miss. Concurrent first-touches on the
// same key are serialised so only one PD ever exists per key.
//
// alloc runs UNDER the cache mutex on purpose: it's a one-time
// syscall per device (typically << 10 of them in a fleet-scale
// process) and holding the mutex avoids the double-alloc race
// that a check-then-alloc-then-store pattern would open.
func getOrAllocPDCached(ctxKey uintptr, alloc pdAllocator) (unsafe.Pointer, error) {
	pdCacheMu.Lock()
	defer pdCacheMu.Unlock()
	if pd, ok := pdCache[ctxKey]; ok {
		return pd, nil
	}
	pd, err := alloc(ctxKey)
	if err != nil {
		return nil, err
	}
	pdCache[ctxKey] = pd
	return pd, nil
}

// resetPDCacheForTest clears the singleton cache. Test-only —
// calling this on a process with live conns would orphan their
// PD pointers and give the next dial a fresh PD that doesn't see
// existing MRs.
func resetPDCacheForTest() {
	pdCacheMu.Lock()
	pdCache = map[uintptr]unsafe.Pointer{}
	pdCacheMu.Unlock()
}
