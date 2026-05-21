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

package backend

import (
	"strings"
	"sync/atomic"
	"testing"
)

// These tests pin the per-bucket / per-volume identity of the Pool's
// PoolKey. Prior to the fix, two rules pointing at the same s3
// endpoint+region with different buckets (or two cfs rules with
// different volumes) shared one cached Backend instance — silently
// routing the second rule's I/O to the first rule's bucket/volume.
// See pool.go PoolKey doc + the bug write-up in design.md.
//
// Reuses fakeBackend / newFake / registerFakes from backend_test.go
// (same package).

func TestPool_KeyIncludesBucket_S3(t *testing.T) {
	registerFakes()
	atomic.StoreInt64(&fakeConstructCount, 0)

	p := NewPool()
	defer p.Close()

	keyA := PoolKey{Kind: "fake-a", Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "bucket-a"}
	keyB := PoolKey{Kind: "fake-a", Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "bucket-b"}

	bA, err := p.Acquire(keyA, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire keyA: %v", err)
	}
	bB, err := p.Acquire(keyB, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire keyB: %v", err)
	}
	if bA == bB {
		t.Fatal("distinct buckets must yield distinct Backend instances (bug: same instance returned)")
	}
	if got := atomic.LoadInt64(&fakeConstructCount); got != 2 {
		t.Errorf("constructor ran %d times, want 2 (one per bucket)", got)
	}
	if p.Size() != 2 {
		t.Errorf("pool size = %d, want 2", p.Size())
	}
}

func TestPool_KeyIncludesVolume_CFS(t *testing.T) {
	registerFakes()
	atomic.StoreInt64(&fakeConstructCount, 0)

	p := NewPool()
	defer p.Close()

	// Endpoint/Region empty for cfs; Bucket holds the volume name.
	keyA := PoolKey{Kind: "fake-a", Bucket: "vol-a"}
	keyB := PoolKey{Kind: "fake-a", Bucket: "vol-b"}

	bA, err := p.Acquire(keyA, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire keyA: %v", err)
	}
	bB, err := p.Acquire(keyB, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire keyB: %v", err)
	}
	if bA == bB {
		t.Fatal("distinct volumes must yield distinct Backend instances (bug: same instance returned)")
	}
	if got := atomic.LoadInt64(&fakeConstructCount); got != 2 {
		t.Errorf("constructor ran %d times, want 2 (one per volume)", got)
	}
	if p.Size() != 2 {
		t.Errorf("pool size = %d, want 2", p.Size())
	}
}

func TestPool_KeySameBucket_ReturnsCached(t *testing.T) {
	registerFakes()
	atomic.StoreInt64(&fakeConstructCount, 0)

	p := NewPool()
	defer p.Close()

	key := PoolKey{Kind: "fake-a", Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "bucket-a"}

	b1, err := p.Acquire(key, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire #1: %v", err)
	}
	b2, err := p.Acquire(key, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire #2: %v", err)
	}
	if b1 != b2 {
		t.Fatal("identical PoolKey must return the cached instance (single-flight)")
	}
	if got := atomic.LoadInt64(&fakeConstructCount); got != 1 {
		t.Errorf("constructor ran %d times, want exactly 1", got)
	}
}

func TestPool_LocalKey_DefaultBucket(t *testing.T) {
	registerFakes()
	atomic.StoreInt64(&fakeConstructCount, 0)

	p := NewPool()
	defer p.Close()

	// local has no per-bucket identity — Bucket is "" by default.
	key := PoolKey{Kind: "fake-a"}

	b1, err := p.Acquire(key, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire #1: %v", err)
	}
	b2, err := p.Acquire(key, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("Acquire #2: %v", err)
	}
	if b1 != b2 {
		t.Fatal("empty-Bucket key must still cache a single instance")
	}
	if got := atomic.LoadInt64(&fakeConstructCount); got != 1 {
		t.Errorf("constructor ran %d times, want 1", got)
	}
	if p.Size() != 1 {
		t.Errorf("pool size = %d, want 1", p.Size())
	}
}

func TestPoolKey_String_IncludesBucket(t *testing.T) {
	cases := []struct {
		name string
		key  PoolKey
		want string
	}{
		{
			name: "s3 with bucket",
			key:  PoolKey{Kind: "s3", Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "bucket-a"},
			want: "s3|https://s3.example.com|us-east-1|bucket-a|",
		},
		{
			name: "cfs with volume in bucket field",
			key:  PoolKey{Kind: "cfs", Bucket: "vol-prod"},
			want: "cfs|||vol-prod|",
		},
		{
			name: "local empty bucket",
			key:  PoolKey{Kind: "local"},
			want: "local||||",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.key.String()
			if got != tc.want {
				t.Errorf("String = %q, want %q", got, tc.want)
			}
			if tc.key.Bucket != "" && !strings.Contains(got, tc.key.Bucket) {
				t.Errorf("String %q must contain Bucket %q (used in logs/metrics)", got, tc.key.Bucket)
			}
		})
	}
}
