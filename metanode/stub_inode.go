// Copyright 2020 The Chubao Authors.
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

package metanode

type stubInode struct {
	inode uint64 // Inode ID
}

func newStubInode(ino uint64) *stubInode {
	return &stubInode{inode: ino}
}

// Less tests whether the current Inode item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (si *stubInode) Less(than BtreeItem) bool {
	ino, ok := than.(*Inode)
	return ok && si.inode < ino.Inode
}

// Copy returns a copy of the inode.
func (si *stubInode) Copy() BtreeItem {
	return NewInode(si.inode, 0)
}
