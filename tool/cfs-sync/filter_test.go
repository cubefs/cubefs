package main

import (
	"testing"
	"time"
)

func TestFilter_NoRules(t *testing.T) {
	f := &Filter{}
	if !f.Allow("dir/file.go", 100, time.Time{}) {
		t.Error("empty filter should allow everything")
	}
}

func TestFilter_Include(t *testing.T) {
	f := &Filter{includes: []string{"*.pt"}}
	tests := []struct {
		key  string
		want bool
	}{
		{"checkpoints/model.pt", true},
		{"checkpoints/model.bin", false},
		{"data/train.pt", true},
		{"README.md", false},
	}
	for _, tt := range tests {
		got := f.Allow(tt.key, 0, time.Time{})
		if got != tt.want {
			t.Errorf("Allow(%q) = %v, want %v", tt.key, got, tt.want)
		}
	}
}

func TestFilter_MultipleIncludes(t *testing.T) {
	f := &Filter{includes: []string{"*.pt", "*.bin"}}
	if !f.Allow("model.pt", 0, time.Time{}) {
		t.Error("*.pt should match")
	}
	if !f.Allow("model.bin", 0, time.Time{}) {
		t.Error("*.bin should match")
	}
	if f.Allow("model.txt", 0, time.Time{}) {
		t.Error("*.txt should not match")
	}
}

func TestFilter_Exclude(t *testing.T) {
	f := &Filter{excludes: []string{"*.tmp", "*.log"}}
	if !f.Allow("data.csv", 0, time.Time{}) {
		t.Error("data.csv should pass")
	}
	if f.Allow("upload.tmp", 0, time.Time{}) {
		t.Error("upload.tmp should be excluded")
	}
	if f.Allow("error.log", 0, time.Time{}) {
		t.Error("error.log should be excluded")
	}
}

func TestFilter_IncludeAndExclude(t *testing.T) {
	// Include all .go files, but exclude test files.
	f := &Filter{includes: []string{"*.go"}, excludes: []string{"*_test.go"}}
	if !f.Allow("main.go", 0, time.Time{}) {
		t.Error("main.go should pass")
	}
	if f.Allow("main_test.go", 0, time.Time{}) {
		t.Error("main_test.go should be excluded")
	}
	if f.Allow("config.yaml", 0, time.Time{}) {
		t.Error("config.yaml should not match include")
	}
}

func TestFilter_MinSize(t *testing.T) {
	f := &Filter{minSize: 1024}
	if !f.Allow("big.bin", 2048, time.Time{}) {
		t.Error("2048 B file should pass minSize=1024")
	}
	if f.Allow("small.bin", 512, time.Time{}) {
		t.Error("512 B file should fail minSize=1024")
	}
	// exactly at boundary
	if !f.Allow("exact.bin", 1024, time.Time{}) {
		t.Error("1024 B file should pass minSize=1024")
	}
}

func TestFilter_MaxSize(t *testing.T) {
	f := &Filter{maxSize: 1024}
	if !f.Allow("small.bin", 512, time.Time{}) {
		t.Error("512 B file should pass maxSize=1024")
	}
	if f.Allow("huge.bin", 2048, time.Time{}) {
		t.Error("2048 B file should fail maxSize=1024")
	}
	if !f.Allow("exact.bin", 1024, time.Time{}) {
		t.Error("1024 B file should pass maxSize=1024")
	}
}

func TestFilter_MinAge(t *testing.T) {
	// minAge: skip files newer than duration (age < minAge means too new)
	f := &Filter{minAge: time.Hour}
	old := time.Now().Add(-2 * time.Hour)
	newFile := time.Now().Add(-30 * time.Minute)
	if !f.Allow("old.bin", 0, old) {
		t.Error("2h old file should pass minAge=1h")
	}
	if f.Allow("new.bin", 0, newFile) {
		t.Error("30min old file should fail minAge=1h (too new)")
	}
}

func TestFilter_MaxAge(t *testing.T) {
	// maxAge: skip files older than duration (age > maxAge means too old)
	f := &Filter{maxAge: time.Hour}
	recent := time.Now().Add(-30 * time.Minute)
	stale := time.Now().Add(-2 * time.Hour)
	if !f.Allow("recent.bin", 0, recent) {
		t.Error("30min file should pass maxAge=1h")
	}
	if f.Allow("stale.bin", 0, stale) {
		t.Error("2h file should fail maxAge=1h (too old)")
	}
}

func TestFilter_ZeroMtimeSkipsAgeCheck(t *testing.T) {
	f := &Filter{maxAge: time.Second}
	// zero mtime → age check disabled
	if !f.Allow("notime.bin", 0, time.Time{}) {
		t.Error("zero mtime should bypass age filter")
	}
}

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"*.go", "main.go", true},
		{"*.go", "main.py", false},
		{"*.go", "main.go.bak", false},
		{"?oo", "foo", true},
		{"?oo", "fo", false},
		{"data-*", "data-train", true},
		{"data-*", "test-train", false},
		{"*", "anything.txt", true},
		{"[invalid", "x", false}, // invalid pattern → no-match, no panic
	}
	for _, tt := range tests {
		got := matchGlob(tt.pattern, tt.name)
		if got != tt.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
		}
	}
}
