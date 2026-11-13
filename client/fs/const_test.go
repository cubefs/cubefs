// Copyright 2026 The CubeFS Authors.
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

package fs

import (
	"os"
	"testing"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/proto"
)

func TestParseType(t *testing.T) {
	tests := []struct {
		name     string
		mode     os.FileMode
		expected fuse.DirentType
	}{
		{name: "regular file", mode: 0o644, expected: fuse.DT_File},
		{name: "directory", mode: os.ModeDir | 0o755, expected: fuse.DT_Dir},
		{name: "symbolic link", mode: os.ModeSymlink | 0o777, expected: fuse.DT_Link},
		{name: "named pipe", mode: os.ModeNamedPipe | 0o644, expected: fuse.DT_FIFO},
		{name: "socket", mode: os.ModeSocket | 0o755, expected: fuse.DT_Socket},
		{name: "block device", mode: os.ModeDevice | 0o600, expected: fuse.DT_Block},
		{
			name:     "character device",
			mode:     os.ModeDevice | os.ModeCharDevice | 0o600,
			expected: fuse.DT_Char,
		},
		{name: "unknown", mode: os.ModeIrregular | 0o600, expected: fuse.DT_Unknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := ParseType(proto.Mode(test.mode)); actual != test.expected {
				t.Fatalf("expected %v, got %v", test.expected, actual)
			}
		})
	}
}
