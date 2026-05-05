package main

import (
	"testing"

	"github.com/cubefs/cubefs/tool/cfs-sync/storage"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"512", 512, false},
		{"1K", 1024, false},
		{"1KB", 1024, false},
		{"2M", 2 * 1024 * 1024, false},
		{"2MB", 2 * 1024 * 1024, false},
		{"2MiB", 2 * 1024 * 1024, false},
		{"3G", 3 * 1024 * 1024 * 1024, false},
		{"3GB", 3 * 1024 * 1024 * 1024, false},
		{"3GiB", 3 * 1024 * 1024 * 1024, false},
		{"1T", 1024 * 1024 * 1024 * 1024, false},
		// case-insensitive
		{"10m", 10 * 1024 * 1024, false},
		{"5g", 5 * 1024 * 1024 * 1024, false},
		// invalid
		{"abc", 0, true},
		{"1X", 0, true},
	}
	for _, tt := range tests {
		got, err := parseSize(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseSize(%q): expected error, got nil (value=%d)", tt.input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSize(%q): unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestOpenStorage_LocalPath(t *testing.T) {
	s, err := openStorage("/tmp", nil, storage.S3Config{}, "", "")
	if err != nil {
		t.Fatalf("openStorage local: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil storage")
	}
}

func TestOpenStorage_RelativePath(t *testing.T) {
	s, err := openStorage(".", nil, storage.S3Config{}, "", "")
	if err != nil {
		t.Fatalf("openStorage relative: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil storage")
	}
}

func TestOpenStorage_S3URI(t *testing.T) {
	s, err := openStorage("s3://my-bucket/prefix/", nil, storage.S3Config{Region: "us-east-1"}, "", "")
	if err != nil {
		t.Fatalf("openStorage s3: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil storage")
	}
	if s.String() != "s3://my-bucket" {
		t.Errorf("String() = %q, want %q", s.String(), "s3://my-bucket")
	}
}

func TestOpenStorage_S3URINoBucket(t *testing.T) {
	// s3:// with no bucket part: bucket="" is still accepted by NewS3
	// (would fail at runtime, but no error at construction time)
	_, err := openStorage("s3://bucket-only", nil, storage.S3Config{}, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenStorage_CFSURIMissingMaster(t *testing.T) {
	_, err := openStorage("cfs://my-vol/data", nil, storage.S3Config{}, "", "")
	if err == nil {
		t.Error("expected error for cfs:// with no masters, got nil")
	}
}
