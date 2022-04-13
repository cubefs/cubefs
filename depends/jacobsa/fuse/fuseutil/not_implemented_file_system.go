// Copyright 2015 Google Inc. All Rights Reserved.
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

package fuseutil

import (
	"context"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

// A FileSystem that responds to all ops with fuse.ENOSYS. Embed this in your
// struct to inherit default implementations for the methods you don't care
// about, ensuring your struct will continue to implement FileSystem even as
// new methods are added.
type NotImplementedFileSystem struct {
}

var _ FileSystem = &NotImplementedFileSystem{}

func (fs *NotImplementedFileSystem) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) RemoveXattr(
	ctx context.Context,
	op *fuseops.RemoveXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) GetXattr(
	ctx context.Context,
	op *fuseops.GetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ListXattr(
	ctx context.Context,
	op *fuseops.ListXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) SetXattr(
	ctx context.Context,
	op *fuseops.SetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) Destroy() {
}
