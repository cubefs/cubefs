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

import "testing"

func TestStatfsInodeCounts(t *testing.T) {
	const maxInodeID uint64 = 1<<63 - 1
	tests := []struct {
		name         string
		inodeCount   uint64
		expectedFree uint64
		expectedUsed uint64
	}{
		{
			name:         "no used inodes",
			inodeCount:   0,
			expectedFree: maxInodeID,
			expectedUsed: 0,
		},
		{
			name:         "used inodes",
			inodeCount:   42,
			expectedFree: maxInodeID - 42,
			expectedUsed: 42,
		},
		{
			name:         "all inodes used",
			inodeCount:   maxInodeID,
			expectedFree: 0,
			expectedUsed: maxInodeID,
		},
		{
			name:         "inode count above maximum",
			inodeCount:   maxInodeID + 1,
			expectedFree: 0,
			expectedUsed: maxInodeID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files, free := statfsInodeCounts(test.inodeCount)
			if files != maxInodeID {
				t.Fatalf("expected %d total inodes, got %d", maxInodeID, files)
			}
			if free != test.expectedFree {
				t.Fatalf("expected %d free inodes, got %d", test.expectedFree, free)
			}
			if used := files - free; used != test.expectedUsed {
				t.Fatalf("expected %d used inodes, got %d", test.expectedUsed, used)
			}
		})
	}
}
