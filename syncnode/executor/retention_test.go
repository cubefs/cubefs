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
	"sort"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

func TestRetention_Disabled(t *testing.T) {
	r := Retention{}
	entries := []backend.Entry{
		{Key: "model-step-1.pt", Size: 1024},
		{Key: "model-step-2.pt", Size: 1024},
	}
	got := r.SelectToDelete(entries, time.Now())
	if len(got) != 0 {
		t.Errorf("disabled retention should select 0, got %d", len(got))
	}
}

func TestRetention_KeepLast(t *testing.T) {
	r := Retention{Pattern: "model-step-{N}.pt", KeepLast: 3}
	entries := []backend.Entry{
		{Key: "runs/a/model-step-1.pt", Size: 1},
		{Key: "runs/a/model-step-5.pt", Size: 1},
		{Key: "runs/a/model-step-2.pt", Size: 1},
		{Key: "runs/a/model-step-10.pt", Size: 1},
		{Key: "runs/a/model-step-3.pt", Size: 1},
		{Key: "runs/a/unrelated.log", Size: 1}, // doesn't match pattern
	}
	got := r.SelectToDelete(entries, time.Now())
	gotKeys := keysOf(got)
	sort.Strings(gotKeys)
	want := []string{"runs/a/model-step-1.pt", "runs/a/model-step-2.pt"}
	if !equalStrings(gotKeys, want) {
		t.Errorf("got %v, want %v", gotKeys, want)
	}
}

func TestRetention_KeepLast_FewerThanLimit(t *testing.T) {
	r := Retention{Pattern: "ckpt-{N}.bin", KeepLast: 10}
	entries := []backend.Entry{
		{Key: "ckpt-1.bin", Size: 1},
		{Key: "ckpt-2.bin", Size: 1},
	}
	got := r.SelectToDelete(entries, time.Now())
	if len(got) != 0 {
		t.Errorf("fewer entries than KeepLast: got %d to delete, want 0", len(got))
	}
}

func TestRetention_KeepWithin(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	hoursAgo := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }

	r := Retention{Pattern: "v-{N}.bin", KeepWithin: 24 * time.Hour}
	entries := []backend.Entry{
		{Key: "v-1.bin", Mtime: hoursAgo(48), Size: 1}, // delete
		{Key: "v-2.bin", Mtime: hoursAgo(12), Size: 1}, // keep
		{Key: "v-3.bin", Mtime: hoursAgo(36), Size: 1}, // delete
		{Key: "v-4.bin", Mtime: hoursAgo(1), Size: 1},  // keep
	}
	got := r.SelectToDelete(entries, now)
	gotKeys := keysOf(got)
	sort.Strings(gotKeys)
	want := []string{"v-1.bin", "v-3.bin"}
	if !equalStrings(gotKeys, want) {
		t.Errorf("got %v, want %v", gotKeys, want)
	}
}

func TestRetention_KeepLastAndWithin_Union(t *testing.T) {
	// If either policy keeps an entry, it survives. KeepLast keeps top-N
	// by version; KeepWithin keeps anything within the time window.
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	hoursAgo := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }

	r := Retention{
		Pattern:    "v-{N}.bin",
		KeepLast:   1,            // only v-3 (highest)
		KeepWithin: 12 * time.Hour, // also keep v-2 (10h ago)
	}
	entries := []backend.Entry{
		{Key: "v-1.bin", Mtime: hoursAgo(48)}, // delete
		{Key: "v-2.bin", Mtime: hoursAgo(10)}, // keep (within)
		{Key: "v-3.bin", Mtime: hoursAgo(20)}, // keep (keepLast=1, highest version)
	}
	got := r.SelectToDelete(entries, now)
	gotKeys := keysOf(got)
	if len(gotKeys) != 1 || gotKeys[0] != "v-1.bin" {
		t.Errorf("got %v, want [v-1.bin]", gotKeys)
	}
}

func TestRetention_BadPattern(t *testing.T) {
	// Missing {N} returns 0 to delete (defensive — caller should have
	// caught this in config validation, but we don't crash if it slips).
	r := Retention{Pattern: "no-version.bin", KeepLast: 1}
	got := r.SelectToDelete([]backend.Entry{
		{Key: "no-version.bin", Size: 1},
	}, time.Now())
	if len(got) != 0 {
		t.Errorf("bad pattern should select 0, got %d", len(got))
	}
}

func TestRetention_PatternWithDots(t *testing.T) {
	// Make sure "." in the pattern doesn't accidentally regex-match
	// arbitrary characters.
	r := Retention{Pattern: "model.{N}.pt", KeepLast: 1}
	entries := []backend.Entry{
		{Key: "model.1.pt", Size: 1},
		{Key: "modelX1Xpt", Size: 1}, // looks like it might match if . were not escaped
	}
	got := r.SelectToDelete(entries, time.Now())
	if len(got) != 0 {
		// model.1.pt is the only matching → kept by keepLast=1; the other
		// doesn't match the pattern at all → not subject to deletion.
		t.Errorf("got %v, want []", keysOf(got))
	}
}

// --- helpers --------------------------------------------------------------

func keysOf(es []backend.Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Key
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
