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
	"fmt"
	"math"
	"testing"
)

func TestShouldKeep_TotalZeroOrOneKeepsEverything(t *testing.T) {
	cases := []int{0, 1}
	for _, total := range cases {
		for _, key := range []string{"", "a", "deep/nested/key", "1234567890"} {
			if !ShouldKeep(key, 0, total, nil) {
				t.Errorf("ShouldKeep(%q, 0, %d, nil) = false, want true", key, total)
			}
		}
	}
}

func TestShouldKeep_NegativeOrOutOfRangeIndexDrops(t *testing.T) {
	// total=4: valid indices are 0..3. Anything outside drops.
	for _, idx := range []int{-1, 4, 100} {
		if ShouldKeep("anything", idx, 4, nil) {
			t.Errorf("ShouldKeep(anything, %d, 4, nil) = true, want false", idx)
		}
	}
}

func TestShardKey_TotalZeroOrOneShortCircuits(t *testing.T) {
	// shardKey is internal; verify the short-circuit branch is exercised
	// so the no-shard fast path stays branch-free.
	for _, total := range []int{0, 1} {
		if got := shardKey("any-key", total); got != 0 {
			t.Errorf("shardKey(any-key, %d) = %d, want 0", total, got)
		}
	}
}

func TestShardKey_Deterministic(t *testing.T) {
	keys := []string{"a", "obj/2026/05/foo.bin", "really-long-key-with-mixed-stuff-123"}
	for _, k := range keys {
		first := shardKey(k, 4)
		for i := 0; i < 10; i++ {
			if got := shardKey(k, 4); got != first {
				t.Errorf("shardKey(%q, 4) = %d on iter %d, want stable %d", k, got, i, first)
			}
		}
	}
}

func TestShardKey_WithinRange(t *testing.T) {
	totals := []int{2, 3, 4, 8, 16}
	for _, total := range totals {
		for i := 0; i < 1000; i++ {
			b := shardKey(fmt.Sprintf("k-%d", i), total)
			if b < 0 || b >= total {
				t.Errorf("shardKey(k-%d, %d) = %d out of range", i, total, b)
			}
		}
	}
}

// TestShouldKeep_PartitionsExactlyOnce verifies that the union of all
// shard outputs covers exactly the input set (no key dropped, no key
// duplicated). This is the property the fan-out coordinator relies on
// for correctness.
func TestShouldKeep_PartitionsExactlyOnce(t *testing.T) {
	total := 4
	const N = 5000
	counts := make([]int, total)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("entry-%d", i)
		hits := 0
		owner := -1
		for shard := 0; shard < total; shard++ {
			if ShouldKeep(key, shard, total, nil) {
				hits++
				owner = shard
			}
		}
		if hits != 1 {
			t.Fatalf("key %q hit %d shards, want exactly 1", key, hits)
		}
		counts[owner]++
	}
}

// TestShouldKeep_UniformDistribution asserts FNV-1a spreads 1000 keys
// across 4 shards within 25% of the per-shard mean. Tighter than the
// spec asks for (spec says 25%) — this leaves some slack but flags
// obvious regressions (e.g. a stuck hash).
func TestShouldKeep_UniformDistribution(t *testing.T) {
	const total = 4
	const N = 1000
	counts := make([]int, total)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("obj-%d", i)
		for shard := 0; shard < total; shard++ {
			if ShouldKeep(key, shard, total, nil) {
				counts[shard]++
				break
			}
		}
	}
	mean := float64(N) / float64(total) // 250
	tol := 0.25                         // ±25%
	for i, c := range counts {
		dev := math.Abs(float64(c)-mean) / mean
		if dev > tol {
			t.Errorf("shard %d: count=%d, mean=%.1f, deviation=%.2f (limit %.2f)",
				i, c, mean, dev, tol)
		}
	}
}

// TestShouldKeep_DistributionLarge confirms the uniformity claim on a
// 100k key population with 4 shards: all shards within 10% of mean.
func TestShouldKeep_DistributionLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skip large-N distribution check in -short mode")
	}
	const total = 4
	const N = 100_000
	counts := make([]int, total)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("longer/key/with/path/segments/obj-%d.bin", i)
		for shard := 0; shard < total; shard++ {
			if ShouldKeep(key, shard, total, nil) {
				counts[shard]++
				break
			}
		}
	}
	mean := float64(N) / float64(total)
	tol := 0.10
	for i, c := range counts {
		dev := math.Abs(float64(c)-mean) / mean
		if dev > tol {
			t.Errorf("shard %d count=%d mean=%.0f dev=%.3f > %.2f",
				i, c, mean, dev, tol)
		}
	}
}

// TestShouldKeep_PrefixMode covers the P2-5 prefix-mode branch: when
// prefixes is non-empty, ShouldKeep matches via strings.HasPrefix and
// IGNORES the hash math.
func TestShouldKeep_PrefixMode(t *testing.T) {
	cases := []struct {
		name     string
		key      string
		prefixes []string
		want     bool
	}{
		{"single prefix hit", "2024/jan/a.bin", []string{"2024/"}, true},
		{"single prefix miss", "2025/jan/a.bin", []string{"2024/"}, false},
		{"multi prefix first hit", "logs/access.log", []string{"logs/", "metrics/"}, true},
		{"multi prefix second hit", "metrics/latency.csv", []string{"logs/", "metrics/"}, true},
		{"multi prefix miss", "junk/x", []string{"logs/", "metrics/"}, false},
		{"empty prefixes falls back to hash", "x", []string{}, true}, // total=0 → keep
		{"nil prefixes falls back to hash", "x", nil, true},          // total=0 → keep
		{"exact prefix match no slash", "abc", []string{"abc"}, true},
		{"shorter key drops", "ab", []string{"abc"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldKeep(tc.key, 0, 0, tc.prefixes); got != tc.want {
				t.Errorf("ShouldKeep(%q, 0, 0, %v) = %v, want %v", tc.key, tc.prefixes, got, tc.want)
			}
		})
	}
}

// TestShouldKeep_PrefixOverridesHash verifies prefix-mode wins when
// both prefixes AND a non-trivial (index, total) are supplied — the
// hash is bypassed.
func TestShouldKeep_PrefixOverridesHash(t *testing.T) {
	target := shardKey("x", 4)
	prefixes := []string{"x"}
	for shard := 0; shard < 4; shard++ {
		if shard == target {
			continue
		}
		if !ShouldKeep("x", shard, 4, prefixes) {
			t.Errorf("shard %d: prefix-mode should accept key x regardless of hash", shard)
		}
	}
}
