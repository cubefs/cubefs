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

package ratelimit

import (
	"context"
	"sync"
	"testing"
)

// TestNewRegistry_NodeBucket — node bucket is always present, mbps wired.
func TestNewRegistry_NodeBucket(t *testing.T) {
	t.Parallel()
	r := NewRegistry(500)
	nb := r.NodeBucket()
	if nb == nil {
		t.Fatal("NodeBucket() returned nil")
	}
	if got := nb.Mbps(); got != 500 {
		t.Errorf("Mbps = %d, want 500", got)
	}

	// Unlimited node.
	r2 := NewRegistry(0)
	if mbps := r2.NodeBucket().Mbps(); mbps != 0 {
		t.Errorf("unlimited node Mbps = %d, want 0", mbps)
	}
}

// TestRegistry_SetGetBackendLimit — installing and reading per-backend
// limits + nil-on-missing-key behaviour.
func TestRegistry_SetGetBackendLimit(t *testing.T) {
	t.Parallel()
	r := NewRegistry(0)
	k := BackendKey{Kind: "s3", Endpoint: "https://s3.example", Region: "us-east-1"}

	// Initially missing.
	if b := r.BackendBucket(k); b != nil {
		t.Errorf("missing key returned %v, want nil", b)
	}

	r.SetBackendLimit(k, 100)
	b := r.BackendBucket(k)
	if b == nil {
		t.Fatal("BackendBucket nil after Set")
	}
	if mbps := b.Mbps(); mbps != 100 {
		t.Errorf("Mbps = %d, want 100", mbps)
	}

	// Updating reuses the same Bucket instance (dynamic retune).
	r.SetBackendLimit(k, 250)
	b2 := r.BackendBucket(k)
	if b != b2 {
		t.Errorf("SetBackendLimit allocated a new bucket; want reuse")
	}
	if mbps := b2.Mbps(); mbps != 250 {
		t.Errorf("after update Mbps = %d, want 250", mbps)
	}

	// Removing.
	r.SetBackendLimit(k, 0)
	if b := r.BackendBucket(k); b != nil {
		t.Errorf("after remove BackendBucket = %v, want nil", b)
	}
}

// TestRegistry_SetNodeLimit — dynamic node-rate change is reflected.
func TestRegistry_SetNodeLimit(t *testing.T) {
	t.Parallel()
	r := NewRegistry(100)
	nb := r.NodeBucket()
	r.SetNodeLimit(750)
	if mbps := nb.Mbps(); mbps != 750 {
		t.Errorf("Mbps after SetNodeLimit = %d, want 750", mbps)
	}
	// Negative disables.
	r.SetNodeLimit(0)
	if err := nb.WaitN(context.Background(), 1<<30); err != nil {
		t.Errorf("WaitN after disable: %v", err)
	}
}

// TestRegistry_DistinctKeys ensures (kind, endpoint, region) is the full
// dedup key — same kind / different endpoint must yield separate buckets.
func TestRegistry_DistinctKeys(t *testing.T) {
	t.Parallel()
	r := NewRegistry(0)
	a := BackendKey{Kind: "s3", Endpoint: "https://a", Region: "r"}
	b := BackendKey{Kind: "s3", Endpoint: "https://b", Region: "r"}
	r.SetBackendLimit(a, 10)
	r.SetBackendLimit(b, 20)
	if ba := r.BackendBucket(a); ba == nil || ba.Mbps() != 10 {
		t.Errorf("bucket a = %v", ba)
	}
	if bb := r.BackendBucket(b); bb == nil || bb.Mbps() != 20 {
		t.Errorf("bucket b = %v", bb)
	}
	if r.BackendBucket(a) == r.BackendBucket(b) {
		t.Error("distinct keys returned the same bucket")
	}
}

// TestRegistry_Snapshot — Snapshot returns a defensive copy of the current
// per-backend rates.
func TestRegistry_Snapshot(t *testing.T) {
	t.Parallel()
	r := NewRegistry(0)
	k1 := BackendKey{Kind: "s3", Endpoint: "e1"}
	k2 := BackendKey{Kind: "s3", Endpoint: "e2"}
	r.SetBackendLimit(k1, 50)
	r.SetBackendLimit(k2, 75)

	snap := r.Snapshot()
	if len(snap) != 2 || snap[k1] != 50 || snap[k2] != 75 {
		t.Errorf("Snapshot = %#v", snap)
	}

	// Mutating the snapshot must not affect the registry.
	snap[k1] = 999
	if got := r.BackendBucket(k1).Mbps(); got != 50 {
		t.Errorf("registry mutated via Snapshot: %d", got)
	}
}

// TestBackendKey_String — basic format coverage.
func TestBackendKey_String(t *testing.T) {
	t.Parallel()
	k := BackendKey{Kind: "s3", Endpoint: "https://s3", Region: "us-east-1"}
	if got := k.String(); got == "" {
		t.Error("String returned empty")
	}
}

// TestParseBackendKey — round-trip String() / ParseBackendKey() and reject
// malformed inputs.
func TestParseBackendKey(t *testing.T) {
	t.Parallel()
	good := []BackendKey{
		{Kind: "s3", Endpoint: "https://s3", Region: "us-east-1"},
		{Kind: "cfs", Endpoint: "", Region: ""},
		{Kind: "local", Endpoint: "/var", Region: ""},
	}
	for _, k := range good {
		parsed, ok := ParseBackendKey(k.String())
		if !ok {
			t.Errorf("ParseBackendKey(%q) ok=false", k.String())
			continue
		}
		if parsed != k {
			t.Errorf("round-trip mismatch: got %#v, want %#v", parsed, k)
		}
	}
	bad := []string{"", "s3|only-two-fields", "||", "|x|y"}
	for _, s := range bad {
		if _, ok := ParseBackendKey(s); ok {
			t.Errorf("ParseBackendKey(%q) should reject", s)
		}
	}
}

// TestRegistry_SetGetRuleLimit — installing and reading per-rule limits +
// nil-on-missing-key behaviour (mirrors backend test shape).
func TestRegistry_SetGetRuleLimit(t *testing.T) {
	t.Parallel()
	r := NewRegistry(0)
	const ruleID = "rule-bandwidth-001"

	// Initially missing.
	if b := r.RuleBucket(ruleID); b != nil {
		t.Errorf("missing rule returned %v, want nil", b)
	}

	r.SetRuleLimit(ruleID, 200)
	b := r.RuleBucket(ruleID)
	if b == nil {
		t.Fatal("RuleBucket nil after Set")
	}
	if mbps := b.Mbps(); mbps != 200 {
		t.Errorf("Mbps = %d, want 200", mbps)
	}

	// Updating reuses the same Bucket instance so in-flight transfers
	// retune dynamically.
	r.SetRuleLimit(ruleID, 400)
	b2 := r.RuleBucket(ruleID)
	if b != b2 {
		t.Errorf("SetRuleLimit allocated a new bucket; want reuse")
	}
	if mbps := b2.Mbps(); mbps != 400 {
		t.Errorf("after update Mbps = %d, want 400", mbps)
	}

	// Removing.
	r.SetRuleLimit(ruleID, 0)
	if b := r.RuleBucket(ruleID); b != nil {
		t.Errorf("after remove RuleBucket = %v, want nil", b)
	}

	// Empty rule ID is a no-op (master must not send blank IDs).
	r.SetRuleLimit("", 100)
	if b := r.RuleBucket(""); b != nil {
		t.Errorf("empty rule ID returned a bucket")
	}
}

// TestRegistry_DistinctRules ensures two rule IDs get distinct buckets.
func TestRegistry_DistinctRules(t *testing.T) {
	t.Parallel()
	r := NewRegistry(0)
	r.SetRuleLimit("a", 10)
	r.SetRuleLimit("b", 20)
	if ba := r.RuleBucket("a"); ba == nil || ba.Mbps() != 10 {
		t.Errorf("bucket a = %v", ba)
	}
	if bb := r.RuleBucket("b"); bb == nil || bb.Mbps() != 20 {
		t.Errorf("bucket b = %v", bb)
	}
	if r.RuleBucket("a") == r.RuleBucket("b") {
		t.Error("distinct rules returned the same bucket")
	}
}

// TestRegistry_RuleSnapshot — Snapshot returns a defensive copy of the
// current per-rule rates.
func TestRegistry_RuleSnapshot(t *testing.T) {
	t.Parallel()
	r := NewRegistry(0)
	r.SetRuleLimit("r1", 50)
	r.SetRuleLimit("r2", 75)
	snap := r.RuleSnapshot()
	if len(snap) != 2 || snap["r1"] != 50 || snap["r2"] != 75 {
		t.Errorf("RuleSnapshot = %#v", snap)
	}
	snap["r1"] = 999
	if got := r.RuleBucket("r1").Mbps(); got != 50 {
		t.Errorf("registry mutated via RuleSnapshot: %d", got)
	}
}

// TestRegistry_ConcurrentRuleSetAndGet — race-detector safety for the
// layer-2 path. Run with -race.
func TestRegistry_ConcurrentRuleSetAndGet(t *testing.T) {
	t.Parallel()
	r := NewRegistry(100)
	ids := []string{"r-a", "r-b", "r-c", "r-d"}
	var wg sync.WaitGroup
	const iters = 200
	for i, id := range ids {
		id := id
		mbps := (i + 1) * 10
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				r.SetRuleLimit(id, mbps+j%5)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = r.RuleBucket(id)
			}
		}()
	}
	wg.Wait()
}

// TestRegistry_ConcurrentSetAndGet — race-detector safety. Run with -race.
func TestRegistry_ConcurrentSetAndGet(t *testing.T) {
	t.Parallel()
	r := NewRegistry(100)
	keys := []BackendKey{
		{Kind: "s3", Endpoint: "e1"},
		{Kind: "s3", Endpoint: "e2"},
		{Kind: "cfs"},
		{Kind: "local"},
	}

	var wg sync.WaitGroup
	const iters = 200
	for i, k := range keys {
		k := k
		mbps := (i + 1) * 10
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				r.SetBackendLimit(k, mbps+j%5)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = r.BackendBucket(k)
				_ = r.NodeBucket()
			}
		}()
	}
	// Concurrent node retune in parallel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < iters; j++ {
			r.SetNodeLimit(j % 1000)
		}
	}()
	wg.Wait()
}
