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

package errorfs

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

const FooContents = "xxxx"

const fooInodeID = fuseops.RootInodeID + 1

var fooAttrs = fuseops.InodeAttributes{
	Nlink: 1,
	Size:  uint64(len(FooContents)),
	Mode:  0444,
}

// A file system whose sole contents are a file named "foo" containing the
// string defined by FooContents.
//
// The file system can be configured to returned canned errors for particular
// operations using the method SetError.
type FS interface {
	fuseutil.FileSystem

	// Cause the file system to return the supplied error for all future
	// operations matching the supplied type.
	SetError(t reflect.Type, err syscall.Errno)
}

func New() (fs FS, err error) {
	fs = &errorFS{
		errors: make(map[reflect.Type]syscall.Errno),
	}

	return
}

type errorFS struct {
	fuseutil.NotImplementedFileSystem

	mu sync.Mutex

	// GUARDED_BY(mu)
	errors map[reflect.Type]syscall.Errno
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) SetError(t reflect.Type, err syscall.Errno) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.errors[t] = err
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) transformError(op interface{}, err *error) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	cannedErr, ok := fs.errors[reflect.TypeOf(op)]
	if ok {
		*err = cannedErr
		return true
	}

	return false
}

////////////////////////////////////////////////////////////////////////
// File system methods
////////////////////////////////////////////////////////////////////////

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	if fs.transformError(op, &err) {
		return
	}

	// Figure out which inode the request is for.
	switch {
	case op.Inode == fuseops.RootInodeID:
		op.Attributes = fuseops.InodeAttributes{
			Mode: os.ModeDir | 0777,
		}

	case op.Inode == fooInodeID:
		op.Attributes = fooAttrs

	default:
		err = fmt.Errorf("Unknown inode: %d", op.Inode)
		return
	}

	return
}

func (fs *errorFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	if fs.transformError(op, &err) {
		return
	}

	// Is this a known inode?
	if !(op.Parent == fuseops.RootInodeID && op.Name == "foo") {
		err = syscall.ENOENT
		return
	}

	op.Entry.Child = fooInodeID
	op.Entry.Attributes = fooAttrs

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	if fs.transformError(op, &err) {
		return
	}

	if op.Inode != fooInodeID {
		err = fmt.Errorf("Unsupported inode ID: %d", op.Inode)
		return
	}

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	if fs.transformError(op, &err) {
		return
	}

	if op.Inode != fooInodeID || op.Offset != 0 {
		err = fmt.Errorf("Unexpected request: %#v", op)
		return
	}

	op.BytesRead = copy(op.Dst, FooContents)

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	if fs.transformError(op, &err) {
		return
	}

	if op.Inode != fuseops.RootInodeID {
		err = fmt.Errorf("Unsupported inode ID: %d", op.Inode)
		return
	}

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *errorFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {
	if fs.transformError(op, &err) {
		return
	}

	if op.Inode != fuseops.RootInodeID || op.Offset != 0 {
		err = fmt.Errorf("Unexpected request: %#v", op)
		return
	}

	op.BytesRead = fuseutil.WriteDirent(
		op.Dst,
		fuseutil.Dirent{
			Offset: 0,
			Inode:  fooInodeID,
			Name:   "foo",
			Type:   fuseutil.DT_File,
		})

	return
}
