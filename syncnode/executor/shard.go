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
	"hash/fnv"
	"strings"
)

// Sharding (Phase P1-7 + P2-5) — file-level fan-out across N syncnodes.
//
// Two modes:
//
//  - hash (default): FNV-1a over the raw object key, modulo ShardTotal.
//    Deterministic, stateless, uniform — every shard runs ShouldKeep
//    independently and selects its subset.
//
//  - prefix (P2-5): when Task.ShardPrefixes is non-empty, the shard
//    owns one or more literal prefix strings; ShouldKeep returns true
//    only when the entry key starts with at least one of them. This is
//    populated by the master from explicit operator-declared prefixes
//    (rule.shardPrefixes) or from a backend-probe at fire time (the
//    auto strategy). Hash math is bypassed in this mode.
//
// shardKey + the hash branch are kept identical to P1-7 so existing
// hash-mode dispatches keep working.

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
// the (index, total) shard of a fan-out task.
//
// When prefixes is non-empty, prefix-mode wins: returns true iff the
// key has any of the listed prefixes as a literal prefix. (index, total)
// are still tracked for parent task correlation but no longer affect
// the filter decision.
//
// When prefixes is empty, hash-mode applies:
//   - total <= 1: returns true unconditionally
//   - index out of range: returns false (defensive — caller built a
//     malformed Task)
//   - otherwise: returns true iff shardKey(key, total) == index
func ShouldKeep(key string, index, total int, prefixes []string) bool {
	if len(prefixes) > 0 {
		for _, p := range prefixes {
			if strings.HasPrefix(key, p) {
				return true
			}
		}
		return false
	}
	if total <= 1 {
		return true
	}
	if index < 0 || index >= total {
		return false
	}
	return shardKey(key, total) == index
}
