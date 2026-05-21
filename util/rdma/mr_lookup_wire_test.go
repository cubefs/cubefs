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

package rdma

import (
	"bytes"
	"testing"
)

func TestMRLookupReply_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		r    MRLookupReply
	}{
		{"zero values", MRLookupReply{}},
		{"typical", MRLookupReply{
			Rkey:      0x12345678,
			PoolIndex: 17,
			VA:        0x7f1234567890,
			Length:    128 * 1024,
		}},
		{"max uint32", MRLookupReply{
			Rkey:      ^uint32(0),
			PoolIndex: ^uint32(0),
			VA:        ^uint64(0),
			Length:    ^uint64(0),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, MRLookupReplySize)
			if err := tc.r.Marshal(buf); err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var got MRLookupReply
			if err := got.Unmarshal(buf); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if got != tc.r {
				t.Fatalf("round-trip mismatch: got %+v want %+v", got, tc.r)
			}
		})
	}
}

func TestMRLookupReply_ShortBuffer(t *testing.T) {
	r := MRLookupReply{Rkey: 1, VA: 2, Length: 3}
	short := make([]byte, MRLookupReplySize-1)
	if err := r.Marshal(short); err == nil {
		t.Error("Marshal should reject short buffer")
	}
	if err := r.Unmarshal(short); err == nil {
		t.Error("Unmarshal should reject short buffer")
	}
}

func TestMRLookupReply_BigEndianStable(t *testing.T) {
	// Pinned layout: changing it silently breaks cross-build wire
	// compatibility. Verify against an explicit byte sequence so the
	// test catches accidental reordering or endianness flips.
	r := MRLookupReply{
		Rkey:      0x01020304,
		PoolIndex: 0x05060708,
		VA:        0x090a0b0c0d0e0f10,
		Length:    0x1112131415161718,
	}
	buf := make([]byte, MRLookupReplySize)
	if err := r.Marshal(buf); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := []byte{
		0x01, 0x02, 0x03, 0x04, // Rkey
		0x05, 0x06, 0x07, 0x08, // PoolIndex
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, // VA
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // Length
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("layout mismatch:\n got %x\nwant %x", buf, want)
	}
}

func TestMRReleaseArg_RoundTrip(t *testing.T) {
	for _, idx := range []uint32{0, 1, 17, 255, 1024, ^uint32(0)} {
		buf := make([]byte, MRReleaseArgSize)
		in := MRReleaseArg{PoolIndex: idx}
		if err := in.Marshal(buf); err != nil {
			t.Fatalf("Marshal idx=%d: %v", idx, err)
		}
		var out MRReleaseArg
		if err := out.Unmarshal(buf); err != nil {
			t.Fatalf("Unmarshal idx=%d: %v", idx, err)
		}
		if out.PoolIndex != idx {
			t.Errorf("idx=%d: got %d", idx, out.PoolIndex)
		}
	}
}

func TestMRReleaseArg_ShortBuffer(t *testing.T) {
	r := MRReleaseArg{PoolIndex: 42}
	short := make([]byte, MRReleaseArgSize-1)
	if err := r.Marshal(short); err == nil {
		t.Error("Marshal should reject short buffer")
	}
	if err := r.Unmarshal(short); err == nil {
		t.Error("Unmarshal should reject short buffer")
	}
}
