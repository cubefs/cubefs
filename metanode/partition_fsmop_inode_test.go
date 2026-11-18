// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"os"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestFSMEvictInodeDirectory(t *testing.T) {
	const inodeID = 101

	tests := []struct {
		name       string
		prepare    func(*Inode)
		wantExists bool
	}{
		{
			name:       "empty directory",
			wantExists: false,
		},
		{
			name: "non-empty directory",
			prepare: func(inode *Inode) {
				inode.NLink = 3
			},
			wantExists: true,
		},
		{
			name: "directory with snapshot",
			prepare: func(inode *Inode) {
				inode.multiSnap = NewMultiSnap(2)
				inode.multiSnap.multiVersions = []*Inode{NewInode(inode.Inode, inode.Type)}
			},
			wantExists: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := &metaPartition{
				inodeTree: NewBtree(),
				freeList:  newFreeList(),
			}
			inode := NewInode(inodeID, uint32(os.ModeDir))
			if test.prepare != nil {
				test.prepare(inode)
			}
			_, inserted := mp.inodeTree.ReplaceOrInsert(inode, false)
			require.True(t, inserted)

			resp := mp.fsmEvictInode(NewInode(inodeID, 0))

			require.EqualValues(t, proto.OpOk, resp.Status)
			got := mp.inodeTree.Get(NewInode(inodeID, 0))
			if test.wantExists {
				require.NotNil(t, got)
				require.False(t, got.(*Inode).ShouldDelete())
			} else {
				require.Nil(t, got)
			}
			require.Zero(t, mp.freeList.Len())
		})
	}
}
