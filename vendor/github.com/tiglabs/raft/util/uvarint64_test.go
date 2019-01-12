// Copyright 2018 The TigLabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package util

import (
	"bytes"
	"testing"
)

var tests = []struct {
	decoded uint64
	n       int
	encoded []byte
}{
	{0, 1, []byte{0x00}},
	{1, 1, []byte{0x01}},
	{240, 1, []byte{0xF0}},
	{241, 2, []byte{0xF1, 0x01}},
	{2287, 2, []byte{0xF8, 0xFF}},

	{2288, 3, []byte{0xF9, 0x00, 0x00}},
	{67823, 3, []byte{0xF9, 0xFF, 0xFF}},
	{67824, 4, []byte{0xFA, 0x01, 0x08, 0xF0}},
	{1<<24 - 1, 4, []byte{0xFA, 0xFF, 0xFF, 0xFF}},
	{1 << 24, 5, []byte{0xFB, 0x01, 0x00, 0x00, 0x00}},

	{1<<32 - 1, 5, []byte{0xFB, 0xFF, 0xFF, 0xFF, 0xFF}},
	{1 << 32, 6, []byte{0xFC, 0x01, 0x00, 0x00, 0x00, 0x00}},
	{1<<40 - 1, 6, []byte{0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	{1 << 40, 7, []byte{0xFD, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00}},
	{1<<48 - 1, 7, []byte{0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},

	{1 << 48, 8, []byte{0xFE, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	{1<<56 - 1, 8, []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	{1 << 56, 9, []byte{0xFF, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	{1<<64 - 1, 9, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
}

func TestUvarint(t *testing.T) {
	for i, test := range tests {
		b := make([]byte, len(test.encoded))
		n := PutUvarint64(b, test.decoded)
		if n != test.n {
			t.Errorf("encode %d: got %d want %d", i, n, test.n)
		}
		if !bytes.Equal(b, test.encoded) {
			t.Errorf("encode %d: got %v want %v", i, b[0:n], test.encoded)
		}
		v, n := Uvarint64(test.encoded)
		if n != test.n {
			t.Errorf("decode %d: got %d want %d", i, n, test.n)
		}
		if v != test.decoded {
			t.Errorf("decode %d: got %d want %d", i, v, test.decoded)
		}
	}
}
