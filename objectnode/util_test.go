// Copyright 2019 The CubeFS Authors.
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

package objectnode

import "testing"

func TestEncodeKey(t *testing.T) {
	testCases := []struct {
		name         string
		key          string
		encodingType string
		expected     string
	}{
		{
			name:         "raw key when encoding type is empty",
			key:          "中文 文件名.txt",
			encodingType: "",
			expected:     "中文 文件名.txt",
		},
		{
			name:         "utf8 key is percent encoded by byte",
			key:          "中文文件名-20260528-1132.txt",
			encodingType: "url",
			expected:     "%E4%B8%AD%E6%96%87%E6%96%87%E4%BB%B6%E5%90%8D-20260528-1132.txt",
		},
		{
			name:         "space is encoded as percent 20",
			key:          "folder name/file name.txt",
			encodingType: "url",
			expected:     "folder%20name/file%20name.txt",
		},
		{
			name:         "encoding type matching is case insensitive",
			key:          "a+b*c",
			encodingType: "URL",
			expected:     "a%2Bb*c",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := encodeKey(tc.key, tc.encodingType)
			if actual != tc.expected {
				t.Fatalf("unexpected encoded key: expected(%q) actual(%q)", tc.expected, actual)
			}
		})
	}
}
