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

func TestExtentMRLookupRequest_RoundTrip(t *testing.T) {
	in := ExtentMRLookupRequest{
		PartitionID:      0x0102030405060708,
		ExtentID:         0x1112131415161718,
		LeaseSecondsHint: 30,
	}
	buf := make([]byte, ExtentMRLookupRequestSize)
	if err := in.Marshal(buf); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out ExtentMRLookupRequest
	if err := out.Unmarshal(buf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestExtentMRLookupRequest_BigEndianStable(t *testing.T) {
	in := ExtentMRLookupRequest{
		PartitionID:      0x0102030405060708,
		ExtentID:         0x1112131415161718,
		LeaseSecondsHint: 0x21222324,
	}
	buf := make([]byte, ExtentMRLookupRequestSize)
	if err := in.Marshal(buf); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x21, 0x22, 0x23, 0x24,
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("got %x want %x", buf, want)
	}
}

func TestExtentMRLookupReply_RoundTrip(t *testing.T) {
	in := ExtentMRLookupReply{
		LeaseID:        42,
		Rkey:           0xDEADBEEF,
		GrantedSeconds: 25,
		VA:             0x7f1234567890,
		Size:           128 * 1024 * 1024,
	}
	buf := make([]byte, ExtentMRLookupReplySize)
	if err := in.Marshal(buf); err != nil {
		t.Fatal(err)
	}
	var out ExtentMRLookupReply
	if err := out.Unmarshal(buf); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestExtentMRRenewRequest_RoundTrip(t *testing.T) {
	in := ExtentMRRenewRequest{LeaseID: 99, LeaseSecondsHint: 60}
	buf := make([]byte, ExtentMRRenewRequestSize)
	if err := in.Marshal(buf); err != nil {
		t.Fatal(err)
	}
	var out ExtentMRRenewRequest
	if err := out.Unmarshal(buf); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestExtentMRRenewReply_RoundTrip(t *testing.T) {
	in := ExtentMRRenewReply{GrantedSeconds: 30}
	buf := make([]byte, ExtentMRRenewReplySize)
	if err := in.Marshal(buf); err != nil {
		t.Fatal(err)
	}
	var out ExtentMRRenewReply
	if err := out.Unmarshal(buf); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestExtentMRWire_ShortBufferRejected(t *testing.T) {
	cases := []struct {
		name    string
		marshal func([]byte) error
		unmar   func([]byte) error
		size    int
	}{
		{"lookup-req",
			func(b []byte) error { return (&ExtentMRLookupRequest{}).Marshal(b) },
			func(b []byte) error { return (&ExtentMRLookupRequest{}).Unmarshal(b) },
			ExtentMRLookupRequestSize,
		},
		{"lookup-reply",
			func(b []byte) error { return (&ExtentMRLookupReply{}).Marshal(b) },
			func(b []byte) error { return (&ExtentMRLookupReply{}).Unmarshal(b) },
			ExtentMRLookupReplySize,
		},
		{"renew-req",
			func(b []byte) error { return (&ExtentMRRenewRequest{}).Marshal(b) },
			func(b []byte) error { return (&ExtentMRRenewRequest{}).Unmarshal(b) },
			ExtentMRRenewRequestSize,
		},
		{"renew-reply",
			func(b []byte) error { return (&ExtentMRRenewReply{}).Marshal(b) },
			func(b []byte) error { return (&ExtentMRRenewReply{}).Unmarshal(b) },
			ExtentMRRenewReplySize,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			short := make([]byte, c.size-1)
			if err := c.marshal(short); err == nil {
				t.Error("Marshal should reject short buffer")
			}
			if err := c.unmar(short); err == nil {
				t.Error("Unmarshal should reject short buffer")
			}
		})
	}
}
