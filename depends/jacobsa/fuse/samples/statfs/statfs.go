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

package statfs

import (
	"context"
	"os"
	"sync"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// A file system that allows orchestrating canned responses to statfs ops, for
// testng out OS-specific statfs behavior.
//
// The file system allows opening and writing to any name that is a child of
// the root inode, and keeps track of the most recent write size delivered by
// the kernel (in order to test statfs response block size effects on write
// size, if any).
//
// Safe for concurrent access.
type FS interface {
	fuseutil.FileSystem

	// Set the canned response to be used for future statfs ops.
	SetStatFSResponse(r fuseops.StatFSOp)

	// Set the canned response to be used for future stat ops.
	SetStatResponse(r fuseops.InodeAttributes)

	// Return the size of the most recent write delivered by the kernel, or -1 if
	// none.
	MostRecentWriteSize() int
}

func New() (fs FS) {
	fs = &statFS{
		cannedStatResponse: fuseops.InodeAttributes{
			Mode: 0666,
		},
		mostRecentWriteSize: -1,
	}

	return
}

const childInodeID = fuseops.RootInodeID + 1

type statFS struct {
	fuseutil.NotImplementedFileSystem

	mu                  sync.Mutex
	cannedResponse      fuseops.StatFSOp        // GUARDED_BY(mu)
	cannedStatResponse  fuseops.InodeAttributes // GUARDED_BY(mu)
	mostRecentWriteSize int                     // GUARDED_BY(mu)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func dirAttrs() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Mode: os.ModeDir | 0777,
	}
}

func (fs *statFS) fileAttrs() fuseops.InodeAttributes {
	return fs.cannedStatResponse
}

////////////////////////////////////////////////////////////////////////
// Public interface
////////////////////////////////////////////////////////////////////////

// LOCKS_EXCLUDED(fs.mu)
func (fs *statFS) SetStatFSResponse(r fuseops.StatFSOp) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.cannedResponse = r
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *statFS) SetStatResponse(r fuseops.InodeAttributes) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.cannedStatResponse = r
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *statFS) MostRecentWriteSize() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.mostRecentWriteSize
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

// LOCKS_EXCLUDED(fs.mu)
func (fs *statFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	*op = fs.cannedResponse
	return
}

func (fs *statFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	// Only the root has children.
	if op.Parent != fuseops.RootInodeID {
		err = fuse.ENOENT
		return
	}

	op.Entry.Child = childInodeID
	op.Entry.Attributes = fs.fileAttrs()

	return
}

func (fs *statFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	switch op.Inode {
	case fuseops.RootInodeID:
		op.Attributes = dirAttrs()

	case childInodeID:
		op.Attributes = fs.fileAttrs()

	default:
		err = fuse.ENOENT
	}

	return
}

func (fs *statFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	// Ignore calls to truncate existing files when opening.
	return
}

func (fs *statFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *statFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.mostRecentWriteSize = len(op.Data)
	return
}
