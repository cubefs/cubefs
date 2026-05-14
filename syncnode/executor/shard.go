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

import "hash/fnv"

// Sharding (Phase P1-7) — file-level fan-out across N syncnodes.
//
// Each shard owner runs a regular Task whose ShardTotal == N and whose
// ShardIndex is its position [0, N). The producer loops in sync_task.go
// and load_task.go filter the source listing through ShouldKeep so each
// owner only transfers the subset of entries whose hashed key maps to
// its ShardIndex.
//
// The hash is FNV-1a over the raw object key, modulo ShardTotal. The
// split is therefore:
//   - deterministic (a re-dispatched sub-task on a new owner sees the
//     same subset),
//   - stateless (no master-side per-entry assignment ledger),
//   - uniform (FNV-1a gives an even distribution across reasonable key
//     populations — verified in shard_test.go).

// shardKey returns the stable shard bucket [0, total) for an object key.
// FNV-1a is fast (one allocation per call worst-case) and uniform enough
// for cross-node distribution. When total <= 1 the function short-circuits
// to 0 so callers don't pay the hash cost on the no-shard path.
func shardKey(key string, total int) int {
	if total <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(total))
}

// ShouldKeep reports whether the entry with the given key belongs in
// the (index, total) shard of a fan-out task. Returns true unconditionally
// when total <= 1 — sharding disabled, every entry stays in scope.
//
// Negative or out-of-range index returns false (defensive default: the
// caller built a malformed Task; rather than corrupt the destination by
// running with no filter, drop everything on the floor and let the
// fan-out coordinator surface the failure via FilesFailed/empty result).
func ShouldKeep(key string, index, total int) bool {
	if total <= 1 {
		return true
	}
	if index < 0 || index >= total {
		return false
	}
	return shardKey(key, total) == index
}
