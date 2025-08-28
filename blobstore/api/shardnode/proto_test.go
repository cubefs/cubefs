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

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeShardKeys(t *testing.T) {
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
			got := DecodeShardKeys(tt.input, 2)
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

func TestEncodeName(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		keys        []string
		tagNum      int
		expected    string
		shouldPanic bool
	}{
		{
			name:        "basic encoding with 3 placeholders",
			format:      "s%s%sname1%sname2",
			keys:        []string{"key1", "key2", "key3"},
			tagNum:      3,
			expected:    "s{key1}{key2}name1{key3}name2",
			shouldPanic: false,
		},
		{
			name:        "only placeholders",
			format:      "%s%s%s",
			keys:        []string{"key1", "key2", "key3"},
			tagNum:      3,
			expected:    "{key1}{key2}{key3}",
			shouldPanic: false,
		},
		{
			name:        "placeholders at start and end",
			format:      "%sname1%s",
			keys:        []string{"key1", "key2"},
			tagNum:      2,
			expected:    "{key1}name1{key2}",
			shouldPanic: false,
		},
		{
			name:        "single placeholder",
			format:      "%sname",
			keys:        []string{"key1"},
			tagNum:      1,
			expected:    "{key1}name",
			shouldPanic: false,
		},
		{
			name:        "complex path with placeholders",
			format:      "%s%s%spath%sfile",
			keys:        []string{"user", "project", "version", "name"},
			tagNum:      4,
			expected:    "{user}{project}{version}path{name}file",
			shouldPanic: false,
		},
		{
			name:        "empty format string",
			format:      "",
			keys:        []string{},
			tagNum:      0,
			expected:    "",
			shouldPanic: false,
		},
		{
			name:        "no placeholders",
			format:      "static/path",
			keys:        []string{},
			tagNum:      0,
			expected:    "static/path",
			shouldPanic: false,
		},
		{
			name:        "empty keys array",
			format:      "%sname",
			keys:        []string{},
			tagNum:      1,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "zero tagNum",
			format:      "%sname",
			keys:        []string{"key1"},
			tagNum:      0,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "mismatch: more keys than placeholders",
			format:      "%sname",
			keys:        []string{"key1", "key2"},
			tagNum:      2,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "mismatch: fewer keys than placeholders",
			format:      "%s%sname",
			keys:        []string{"key1"},
			tagNum:      2,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "mismatch: tagNum not equal to placeholders",
			format:      "%s%sname",
			keys:        []string{"key1", "key2"},
			tagNum:      3,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "special characters in keys",
			format:      "%s%sname",
			keys:        []string{"key/1", "key\\2"},
			tagNum:      2,
			expected:    "{key/1}{key\\2}name",
			shouldPanic: false,
		},
		{
			name:        "special characters in keys 2",
			format:      "%s%daa%sname",
			keys:        []string{"key1", "key2"},
			tagNum:      2,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "special characters in keys 3",
			format:      "%s%sname%",
			keys:        []string{"key1", "key2"},
			tagNum:      2,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "empty keys",
			format:      "%s%sname",
			keys:        []string{"", ""},
			tagNum:      2,
			expected:    "{}{}name",
			shouldPanic: false,
		},
		{
			name:        "too much '%s' in format",
			format:      "%ssss%s%sss%sname",
			keys:        []string{"key1", "key2"},
			tagNum:      2,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "illegal format string 1",
			format:      "/%s/%s/name{",
			keys:        []string{"key1", "key2"},
			tagNum:      2,
			expected:    "",
			shouldPanic: true,
		},
		{
			name:        "illegal format string 2",
			format:      "/%s/%s/name}",
			keys:        []string{"key1", "key2"},
			tagNum:      2,
			expected:    "",
			shouldPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.shouldPanic {
						t.Errorf("Test %s: unexpected panic: %v", tt.name, r)
					}
				} else if tt.shouldPanic {
					t.Errorf("Test %s: expected panic but none occurred", tt.name)
				}
			}()

			result := EncodeName(tt.format, tt.keys, tt.tagNum)

			if !tt.shouldPanic && result != tt.expected {
				t.Errorf("EncodeName(%q, %v, %d) = %q, want %q",
					tt.format, tt.keys, tt.tagNum, result, tt.expected)
			}

			if !tt.shouldPanic && len(tt.keys) > 0 {
				shardkeys := DecodeShardKeys(result, tt.tagNum)
				for i := range shardkeys {
					require.True(t, bytes.Equal([]byte(shardkeys[i]), []byte(tt.keys[i])))
				}
			}
		})
	}
}
