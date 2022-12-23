package data

import (
	"math"
	"testing"
)

var (
	cfg = &ExtentConfig{
		Volume:  "ltptest",
		Masters: []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"},
	}
	testEClient *ExtentClient
)

func TestSetExtentSize(t *testing.T) {
	var err error
	testEClient, err = NewExtentClient(cfg, nil)
	if err != nil {
		t.Fatalf("create new extent client failed: err(%v)", err)
	}
	caseSetExSize := []struct {
		name string
		size int
		want int
	}{
		{
			name: "<0",
			size: -1,
			want: 128 * 1024,
		},
		{
			name: "0",
			size: 0,
			want: 128 * 1024 * 1024,
		},
		{
			name: "<128K",
			size: 64 * 1024,
			want: 128 * 1024,
		},
		{
			name: "128K~128M, normal",
			size: 128 * 1024 * 64,
			want: 128 * 1024 * 64,
		},
		{
			name: "128K~128M, not power of 2",
			size: 128 * 1024 * 64 * 3,
			want: 128 * 1024 * 64 * 4,
		},
		{
			name: ">128M",
			size: 128 * 1024 * 1024 * 2,
			want: 128 * 1024 * 1024,
		},
		{
			name: "MaxInt64",
			size: math.MaxInt64,
			want: 128 * 1024 * 1024,
		},
	}
	for _, tt := range caseSetExSize {
		t.Run(tt.name, func(t *testing.T) {
			if testEClient.SetExtentSize(tt.size); testEClient.extentSize != tt.want {
				t.Fatalf("set[%v], want[%v], but got[%v]",
					tt.size, tt.want, testEClient.extentSize)
			}
		})
	}
}

func TestGetRate(t *testing.T) {
	testEClient, err := NewExtentClient(cfg, nil)
	if err != nil {
		t.Fatalf("create new extent client failed!")
	}
	if testEClient.GetRate() == "" {
		t.Fatalf("GetRate failed")
	}
}

func TestSetReadRate(t *testing.T) {
	testEClient, err := NewExtentClient(cfg, nil)
	if err != nil {
		t.Fatalf("create new extent client failed!")
	}
	val := 10
	if testEClient.SetReadRate(val) == "" {
		t.Fatalf("SetReadRate failed")
	}
}

func TestSetWriteRate(t *testing.T) {
	testEClient, err := NewExtentClient(cfg, nil)
	if err != nil {
		t.Fatalf("create new extent client failed!")
	}
	val := 10
	if testEClient.SetWriteRate(val) == "" {
		t.Fatalf("SetWriteRate failed")
	}
}
