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

package flushfs

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Create a file system whose sole contents are a file named "foo" and a
// directory named "bar".
//
// The file may be opened for reading and/or writing. Its initial contents are
// empty. Whenever a flush or fsync is received, the supplied function will be
// called with the current contents of the file and its status returned.
//
// The directory cannot be modified.
func NewFileSystem(
	reportFlush func(string) error,
	reportFsync func(string) error) (server fuse.Server, err error) {
	fs := &flushFS{
		reportFlush: reportFlush,
		reportFsync: reportFsync,
	}

	server = fuseutil.NewFileSystemServer(fs)
	return
}

const (
	fooID = fuseops.RootInodeID + 1 + iota
	barID
)

type flushFS struct {
	fuseutil.NotImplementedFileSystem

	reportFlush func(string) error
	reportFsync func(string) error

	mu          sync.Mutex
	fooContents []byte // GUARDED_BY(mu)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) rootAttributes() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  0777 | os.ModeDir,
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) fooAttributes() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  0777,
		Size:  uint64(len(fs.fooContents)),
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) barAttributes() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  0777 | os.ModeDir,
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) getAttributes(id fuseops.InodeID) (
	attrs fuseops.InodeAttributes,
	err error) {
	switch id {
	case fuseops.RootInodeID:
		attrs = fs.rootAttributes()
		return

	case fooID:
		attrs = fs.fooAttributes()
		return

	case barID:
		attrs = fs.barAttributes()
		return

	default:
		err = fuse.ENOENT
		return
	}
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fs *flushFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	return
}

func (fs *flushFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Sanity check.
	if op.Parent != fuseops.RootInodeID {
		err = fuse.ENOENT
		return
	}

	// Set up the entry.
	switch op.Name {
	case "foo":
		op.Entry = fuseops.ChildInodeEntry{
			Child:      fooID,
			Attributes: fs.fooAttributes(),
		}

	case "bar":
		op.Entry = fuseops.ChildInodeEntry{
			Child:      barID,
			Attributes: fs.barAttributes(),
		}

	default:
		err = fuse.ENOENT
		return
	}

	return
}

func (fs *flushFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.Attributes, err = fs.getAttributes(op.Inode)
	return
}

func (fs *flushFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ignore any changes and simply return existing attributes.
	op.Attributes, err = fs.getAttributes(op.Inode)

	return
}

func (fs *flushFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Sanity check.
	if op.Inode != fooID {
		err = fuse.ENOSYS
		return
	}

	return
}

func (fs *flushFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure the offset is in range.
	if op.Offset > int64(len(fs.fooContents)) {
		return
	}

	// Read what we can.
	op.BytesRead = copy(op.Dst, fs.fooContents[op.Offset:])

	return
}

func (fs *flushFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure that the contents slice is long enough.
	newLen := int(op.Offset) + len(op.Data)
	if len(fs.fooContents) < newLen {
		padding := make([]byte, newLen-len(fs.fooContents))
		fs.fooContents = append(fs.fooContents, padding...)
	}

	// Copy in the data.
	n := copy(fs.fooContents[op.Offset:], op.Data)

	// Sanity check.
	if n != len(op.Data) {
		panic(fmt.Sprintf("Unexpected short copy: %v", n))
	}

	return
}

func (fs *flushFS) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	err = fs.reportFsync(string(fs.fooContents))
	return
}

func (fs *flushFS) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	err = fs.reportFlush(string(fs.fooContents))
	return
}

func (fs *flushFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Sanity check.
	switch op.Inode {
	case fuseops.RootInodeID:
	case barID:

	default:
		err = fuse.ENOENT
		return
	}

	return
}

func (fs *flushFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Create the appropriate listing.
	var dirents []fuseutil.Dirent

	switch op.Inode {
	case fuseops.RootInodeID:
		dirents = []fuseutil.Dirent{
			fuseutil.Dirent{
				Offset: 1,
				Inode:  fooID,
				Name:   "foo",
				Type:   fuseutil.DT_File,
			},

			fuseutil.Dirent{
				Offset: 2,
				Inode:  barID,
				Name:   "bar",
				Type:   fuseutil.DT_Directory,
			},
		}

	case barID:

	default:
		err = fmt.Errorf("Unexpected inode: %v", op.Inode)
		return
	}

	// If the offset is for the end of the listing, we're done. Otherwise we
	// expect it to be for the start.
	switch op.Offset {
	case fuseops.DirOffset(len(dirents)):
		return

	case 0:

	default:
		err = fmt.Errorf("Unexpected offset: %v", op.Offset)
		return
	}

	// Fill in the listing.
	for _, de := range dirents {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], de)

		// We don't support doing this in anything more than one shot.
		if n == 0 {
			err = fmt.Errorf("Couldn't fit listing in %v bytes", len(op.Dst))
			return
		}

		op.BytesRead += n
	}

	return
}
