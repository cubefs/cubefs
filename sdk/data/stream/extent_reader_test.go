// Copyright 2018 The CubeFS Authors.
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

package stream

import (
	"reflect"
	"testing"
)

// TestSplitReadChunks covers the pure chunk-splitting logic that
// readViaRDMA relies on. RDMA itself isn't exercised here — those code
// paths live behind a build tag and need real hardware.

func TestSplitReadChunks_EmptyAndZero(t *testing.T) {
	cases := []struct {
		name string
		off  int
		size int
		blk  int
	}{
		{"size==0", 0, 0, 128 << 10},
		{"negative size", 0, -1, 128 << 10},
		{"zero block", 0, 1024, 0},
		{"negative block", 0, 1024, -128},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitReadChunks(tc.off, tc.size, tc.blk)
			if got != nil {
				t.Fatalf("splitReadChunks(%d, %d, %d) = %v; want nil",
					tc.off, tc.size, tc.blk, got)
			}
		})
	}
}

func TestSplitReadChunks_SingleAligned(t *testing.T) {
	got := splitReadChunks(1024, 128*1024, 128*1024)
	want := []readChunkSpec{
		{extentOff: 1024, bufOff: 0, bufSize: 128 * 1024},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
}

func TestSplitReadChunks_MultipleAligned(t *testing.T) {
	// 4 MB read at extent offset 100, BlockSize = 128 KB → 32 chunks
	got := splitReadChunks(100, 4*1024*1024, 128*1024)
	if len(got) != 32 {
		t.Fatalf("got %d chunks; want 32", len(got))
	}
	// Spot-check first, middle, last
	if got[0] != (readChunkSpec{extentOff: 100, bufOff: 0, bufSize: 128 * 1024}) {
		t.Errorf("chunk 0 = %+v", got[0])
	}
	if got[15] != (readChunkSpec{extentOff: 100 + 15*128*1024, bufOff: 15 * 128 * 1024, bufSize: 128 * 1024}) {
		t.Errorf("chunk 15 = %+v", got[15])
	}
	if got[31] != (readChunkSpec{extentOff: 100 + 31*128*1024, bufOff: 31 * 128 * 1024, bufSize: 128 * 1024}) {
		t.Errorf("chunk 31 = %+v", got[31])
	}
}

func TestSplitReadChunks_TailSmallerThanBlock(t *testing.T) {
	// 200 KB read with 128 KB blocks → 128 KB + 72 KB tail
	got := splitReadChunks(0, 200*1024, 128*1024)
	want := []readChunkSpec{
		{extentOff: 0, bufOff: 0, bufSize: 128 * 1024},
		{extentOff: 128 * 1024, bufOff: 128 * 1024, bufSize: 72 * 1024},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
}

func TestSplitReadChunks_SubBlockTotalSize(t *testing.T) {
	// Read smaller than one block fits in a single chunk
	got := splitReadChunks(5000, 8000, 128*1024)
	want := []readChunkSpec{
		{extentOff: 5000, bufOff: 0, bufSize: 8000},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
}

func TestSplitReadChunks_TotalSizeAccounting(t *testing.T) {
	// Property: sum of bufSize across chunks equals input size, and
	// bufOff values cover [0, size) without gaps or overlaps. Probes
	// boundary conditions around BlockSize multiples.
	for _, size := range []int{1, 100, 1024, 128 * 1024, 128*1024 + 1, 4 * 1024 * 1024, 4*1024*1024 - 1, 5*1024*1024 + 333} {
		chunks := splitReadChunks(0, size, 128*1024)
		total := 0
		expected := 0
		for _, c := range chunks {
			if c.bufOff != expected {
				t.Errorf("size=%d: chunk %+v has gap (expected bufOff %d)", size, c, expected)
			}
			total += c.bufSize
			expected += c.bufSize
		}
		if total != size {
			t.Errorf("size=%d: sum of chunk sizes = %d; want %d", size, total, size)
		}
	}
}

func TestSplitReadChunks_ExtentOffsetPropagates(t *testing.T) {
	// extentOff offsets cumulatively just like bufOff — the read at
	// extent offset N is laid out across chunks N, N+block, N+2*block, ...
	const startOff = 12345
	chunks := splitReadChunks(startOff, 300*1024, 128*1024)
	if chunks[0].extentOff != startOff {
		t.Errorf("chunk[0].extentOff = %d; want %d", chunks[0].extentOff, startOff)
	}
	if chunks[1].extentOff != startOff+128*1024 {
		t.Errorf("chunk[1].extentOff = %d; want %d", chunks[1].extentOff, startOff+128*1024)
	}
	if chunks[2].extentOff != startOff+256*1024 {
		t.Errorf("chunk[2].extentOff = %d; want %d", chunks[2].extentOff, startOff+256*1024)
	}
}
