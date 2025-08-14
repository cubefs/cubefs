// Copyright 2025 The CubeFS Authors.
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

package shardnode

import "testing"

func TestParseShardKeys(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected [][]byte
	}{
		{
			name:     "no tags",
			input:    "plaintext",
			expected: [][]byte{[]byte("plaintext"), []byte("plaintext")},
		},
		{
			name:     "single empty tag",
			input:    "{}",
			expected: [][]byte{[]byte(""), []byte("")},
		},
		{
			name:     "single tag",
			input:    "{key}",
			expected: [][]byte{[]byte("key"), []byte("")},
		},
		{
			name:     "multiple tags",
			input:    "{key1}{key2}{key3}",
			expected: [][]byte{[]byte("key1"), []byte("key2")},
		},
		{
			name:     "nested tags",
			input:    "{a{b}c}{d{e}f}",
			expected: [][]byte{[]byte("a{b"), []byte("d{e")},
		},
		{
			name:     "unbalanced tags",
			input:    "{key1}{key2",
			expected: [][]byte{[]byte("key1"), []byte("")},
		},
		{
			name:     "mixed content",
			input:    "prefix{key1}middle{key2}suffix",
			expected: [][]byte{[]byte("key1"), []byte("key2")},
		},
		{
			name:     "empty input",
			input:    "",
			expected: [][]byte{},
		},
		{
			name:     "only opening tag",
			input:    "{key",
			expected: [][]byte{[]byte("{key"), []byte("{key")},
		},
		{
			name:     "only closing tag",
			input:    "key}",
			expected: [][]byte{[]byte("key}"), []byte("key}")},
		},
		{
			name:     "reverse tags",
			input:    "}{",
			expected: [][]byte{[]byte("}{"), []byte("}{")},
		},
		{
			name:     "single1",
			input:    "{",
			expected: [][]byte{[]byte("{"), []byte("{")},
		},
		{
			name:     "single2",
			input:    "}",
			expected: [][]byte{[]byte("}"), []byte("}")},
		},
		{
			name:     "blob name 1",
			input:    "{}{}name",
			expected: [][]byte{[]byte(""), []byte("")},
		},
		{
			name:     "blob name 2",
			input:    "{key1}{}name",
			expected: [][]byte{[]byte("key1"), []byte("")},
		},
		{
			name:     "blob name 3",
			input:    "{key1}{key2}name",
			expected: [][]byte{[]byte("key1"), []byte("key2")},
		},
		{
			name:     "blob name 4",
			input:    "{}{key1}name",
			expected: [][]byte{[]byte(""), []byte("key1")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseShardKeys(tt.input, 2)
			if len(got) != len(tt.expected) {
				t.Errorf("%s: expected %d keys, got %d", tt.name, len(tt.expected), len(got))
				return
			}
			for i := range got {
				if string(got[i]) != string(tt.expected[i]) {
					t.Errorf("%s: at index %d, expected %q, got %q",
						tt.name, i, tt.expected[i], got[i])
				}
			}
		})
	}
}
