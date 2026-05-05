package main

import (
	"testing"
)

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{int64(1.5 * 1024 * 1024), "1.50 MiB"},
		{1024 * 1024 * 1024, "1.00 GiB"},
		{int64(2.5 * 1024 * 1024 * 1024), "2.50 GiB"},
	}
	for _, tt := range tests {
		got := humanBytes(tt.input)
		if got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestHumanCount(t *testing.T) {
	if got := humanCount(0, "file"); got != "0 files" {
		t.Errorf("got %q", got)
	}
	if got := humanCount(1, "file"); got != "1 file" {
		t.Errorf("got %q", got)
	}
	if got := humanCount(2, "file"); got != "2 files" {
		t.Errorf("got %q", got)
	}
}
