// Copyright 2024 The CubeFS Authors.
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

package metanode

// errInjectInodeTree delegates to a real InodeTree and injects errors on selected calls.
type errInjectInodeTree struct {
	InodeTree
	getErr     error
	copyGetErr error
	updateErr  error
}

func (t *errInjectInodeTree) Get(ino *Inode) (*Inode, error) {
	if t.getErr != nil {
		return nil, t.getErr
	}
	return t.InodeTree.Get(ino)
}

func (t *errInjectInodeTree) CopyGet(ino *Inode) (*Inode, error) {
	if t.copyGetErr != nil {
		return nil, t.copyGetErr
	}
	return t.InodeTree.CopyGet(ino)
}

func (t *errInjectInodeTree) Update(handle interface{}, inode *Inode) error {
	if t.updateErr != nil {
		return t.updateErr
	}
	return t.InodeTree.Update(handle, inode)
}
