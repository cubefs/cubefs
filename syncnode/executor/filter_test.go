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
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

func TestFilter_Match_DirAlwaysExcluded(t *testing.T) {
	f := Filter{}
	e := backend.Entry{Key: "dir/", IsDir: true}
	if f.Match(e, time.Now()) {
		t.Error("directory entries must never match")
	}
}

func TestFilter_Match_IncludeExclude(t *testing.T) {
	f := Filter{
		Include: []string{"*.pt", "*.safetensors"},
		Exclude: []string{"*.tmp", "*.partial"},
	}
	now := time.Now()
	cases := []struct {
		key   string
		match bool
	}{
		{"model-step-1000.pt", true},
		{"model.safetensors", true},
		{"model.tmp", false},     // excluded
		{"model.pt.tmp", false},  // excluded by tmp
		{"model.partial", false}, // excluded
		{"random.log", false},    // not in include
	}
	for _, c := range cases {
		got := f.Match(backend.Entry{Key: c.key, Size: 1024}, now)
		if got != c.match {
			t.Errorf("Match(%q) = %v, want %v", c.key, got, c.match)
		}
	}
}

func TestFilter_Match_NoInclude_ExcludeOnly(t *testing.T) {
	// Empty Include = match everything (except excludes).
	f := Filter{Exclude: []string{"*.tmp"}}
	now := time.Now()
	if !f.Match(backend.Entry{Key: "anything.bin"}, now) {
		t.Error("no include + non-excluded should match")
	}
	if f.Match(backend.Entry{Key: "x.tmp"}, now) {
		t.Error("excluded should fail")
	}
}

func TestFilter_Match_Size(t *testing.T) {
	f := Filter{MinSize: 1024, MaxSize: 1 << 20}
	now := time.Now()
	cases := []struct {
		size  int64
		match bool
	}{
		{500, false},     // below min
		{1023, false},    // just below min
		{1024, true},     // at min
		{1 << 19, true},  // half MB
		{1 << 20, true},  // at max
		{1<<20 + 1, false}, // above max
	}
	for _, c := range cases {
		got := f.Match(backend.Entry{Key: "x.bin", Size: c.size}, now)
		if got != c.match {
			t.Errorf("size %d: got %v, want %v", c.size, got, c.match)
		}
	}
}

func TestFilter_Match_Age(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	hoursAgo := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }

	f := Filter{MinAge: 1 * time.Hour, MaxAge: 24 * time.Hour}
	cases := []struct {
		mtime time.Time
		match bool
	}{
		{hoursAgo(0), false},   // too new
		{hoursAgo(2), true},    // in range
		{hoursAgo(25), false},  // too old
		{time.Time{}, true},    // zero mtime = skip age check
	}
	for _, c := range cases {
		got := f.Match(backend.Entry{Key: "x.bin", Size: 1, Mtime: c.mtime}, now)
		if got != c.match {
			t.Errorf("mtime %v: got %v, want %v", c.mtime, got, c.match)
		}
	}
}

func TestParseFilter_Sizes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"", 0, false},
		{"1024B", 1024, false},
		{"1KB", 1000, false},
		{"1KiB", 1024, false},
		{"5MB", 5_000_000, false},
		{"5MiB", 5 << 20, false},
		{"1GB", 1_000_000_000, false},
		{"1GiB", 1 << 30, false},
		{"1Megabyte", 0, true},
		{"abc", 0, true},
	}
	for _, c := range cases {
		got, err := parseSize(c.in)
		if (err != nil) != c.err {
			t.Errorf("parseSize(%q) err=%v, wantErr=%v", c.in, err, c.err)
		}
		if !c.err && got != c.want {
			t.Errorf("parseSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseFilter_Durations(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{"", 0, false},
		{"60s", 60 * time.Second, false},
		{"30m", 30 * time.Minute, false},
		{"1h", time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},
		{"2w", 2 * 7 * 24 * time.Hour, false},
		{"1month", 0, true},
		{"abc", 0, true},
	}
	for _, c := range cases {
		got, err := parseDuration(c.in)
		if (err != nil) != c.err {
			t.Errorf("parseDuration(%q) err=%v, wantErr=%v", c.in, err, c.err)
		}
		if !c.err && got != c.want {
			t.Errorf("parseDuration(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestParseFilter_RoundTrip(t *testing.T) {
	f, err := ParseFilter(
		[]string{"*.pt"}, []string{"*.tmp"},
		"1MB", "10GB", "60s", "7d")
	if err != nil {
		t.Fatalf("ParseFilter: %v", err)
	}
	if f.MinSize != 1_000_000 {
		t.Errorf("MinSize = %d", f.MinSize)
	}
	if f.MaxAge != 7*24*time.Hour {
		t.Errorf("MaxAge = %v", f.MaxAge)
	}
}
