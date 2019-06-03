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

package cachingfs

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/syncutil"
)

const (
	// Sizes of the files according to the file system.
	FooSize = 123
	BarSize = 456
)

// A file system with a fixed structure that looks like this:
//
//     foo
//     dir/
//         bar
//
// The file system is configured with durations that specify how long to allow
// inode entries and attributes to be cached, used when responding to fuse
// requests. It also exposes methods for renumbering inodes and updating mtimes
// that are useful in testing that these durations are honored.
//
// Each file responds to reads with random contents. SetKeepCache can be used
// to control whether the response to OpenFileOp tells the kernel to keep the
// file's data in the page cache or not.
type CachingFS interface {
	fuseutil.FileSystem

	// Return the current inode ID of the file/directory with the given name.
	FooID() fuseops.InodeID
	DirID() fuseops.InodeID
	BarID() fuseops.InodeID

	// Cause the inode IDs to change to values that have never before been used.
	RenumberInodes()

	// Cause further queries for the attributes of inodes to use the supplied
	// time as the inode's mtime.
	SetMtime(mtime time.Time)

	// Instruct the file system whether or not to reply to OpenFileOp with
	// FOPEN_KEEP_CACHE set.
	SetKeepCache(keep bool)
}

// Create a file system that issues cacheable responses according to the
// following rules:
//
//  *  LookUpInodeResponse.Entry.EntryExpiration is set according to
//     lookupEntryTimeout.
//
//  *  GetInodeAttributesResponse.AttributesExpiration is set according to
//     getattrTimeout.
//
//  *  Nothing else is marked cacheable. (In particular, the attributes
//     returned by LookUpInode are not cacheable.)
//
func NewCachingFS(
	lookupEntryTimeout time.Duration,
	getattrTimeout time.Duration) (fs CachingFS, err error) {
	roundUp := func(n fuseops.InodeID) fuseops.InodeID {
		return numInodes * ((n + numInodes - 1) / numInodes)
	}

	cfs := &cachingFS{
		lookupEntryTimeout: lookupEntryTimeout,
		getattrTimeout:     getattrTimeout,
		baseID:             roundUp(fuseops.RootInodeID + 1),
		mtime:              time.Now(),
	}

	cfs.mu = syncutil.NewInvariantMutex(cfs.checkInvariants)

	fs = cfs
	return
}

const (
	// Inode IDs are issued such that "foo" always receives an ID that is
	// congruent to fooOffset modulo numInodes, etc.
	fooOffset = iota
	dirOffset
	barOffset
	numInodes
)

type cachingFS struct {
	fuseutil.NotImplementedFileSystem

	/////////////////////////
	// Constant data
	/////////////////////////

	lookupEntryTimeout time.Duration
	getattrTimeout     time.Duration

	/////////////////////////
	// Mutable state
	/////////////////////////

	mu syncutil.InvariantMutex

	// GUARDED_BY(mu)
	keepPageCache bool

	// The current ID of the lowest numbered non-root inode.
	//
	// INVARIANT: baseID > fuseops.RootInodeID
	// INVARIANT: baseID % numInodes == 0
	//
	// GUARDED_BY(mu)
	baseID fuseops.InodeID

	// GUARDED_BY(mu)
	mtime time.Time
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (fs *cachingFS) checkInvariants() {
	// INVARIANT: baseID > fuseops.RootInodeID
	// INVARIANT: baseID % numInodes == 0
	if fs.baseID <= fuseops.RootInodeID || fs.baseID%numInodes != 0 {
		panic(fmt.Sprintf("Bad baseID: %v", fs.baseID))
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *cachingFS) fooID() fuseops.InodeID {
	return fs.baseID + fooOffset
}

// LOCKS_REQUIRED(fs.mu)
func (fs *cachingFS) dirID() fuseops.InodeID {
	return fs.baseID + dirOffset
}

// LOCKS_REQUIRED(fs.mu)
func (fs *cachingFS) barID() fuseops.InodeID {
	return fs.baseID + barOffset
}

// LOCKS_REQUIRED(fs.mu)
func (fs *cachingFS) rootAttrs() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Mode:  os.ModeDir | 0777,
		Mtime: fs.mtime,
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *cachingFS) fooAttrs() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Size:  FooSize,
		Mode:  0777,
		Mtime: fs.mtime,
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *cachingFS) dirAttrs() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  os.ModeDir | 0777,
		Mtime: fs.mtime,
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *cachingFS) barAttrs() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Size:  BarSize,
		Mode:  0777,
		Mtime: fs.mtime,
	}
}

////////////////////////////////////////////////////////////////////////
// Public interface
////////////////////////////////////////////////////////////////////////

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) FooID() fuseops.InodeID {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.fooID()
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) DirID() fuseops.InodeID {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.dirID()
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) BarID() fuseops.InodeID {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.barID()
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) RenumberInodes() {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.baseID += numInodes
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) SetMtime(mtime time.Time) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.mtime = mtime
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) SetKeepCache(keep bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.keepPageCache = keep
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fs *cachingFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the ID and attributes.
	var id fuseops.InodeID
	var attrs fuseops.InodeAttributes

	switch op.Name {
	case "foo":
		// Parent must be the root.
		if op.Parent != fuseops.RootInodeID {
			err = fuse.ENOENT
			return
		}

		id = fs.fooID()
		attrs = fs.fooAttrs()

	case "dir":
		// Parent must be the root.
		if op.Parent != fuseops.RootInodeID {
			err = fuse.ENOENT
			return
		}

		id = fs.dirID()
		attrs = fs.dirAttrs()

	case "bar":
		// Parent must be dir.
		if op.Parent == fuseops.RootInodeID || op.Parent%numInodes != dirOffset {
			err = fuse.ENOENT
			return
		}

		id = fs.barID()
		attrs = fs.barAttrs()

	default:
		err = fuse.ENOENT
		return
	}

	// Fill in the response.
	op.Entry.Child = id
	op.Entry.Attributes = attrs
	op.Entry.EntryExpiration = time.Now().Add(fs.lookupEntryTimeout)

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *cachingFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Figure out which inode the request is for.
	var attrs fuseops.InodeAttributes

	switch {
	case op.Inode == fuseops.RootInodeID:
		attrs = fs.rootAttrs()

	case op.Inode%numInodes == fooOffset:
		attrs = fs.fooAttrs()

	case op.Inode%numInodes == dirOffset:
		attrs = fs.dirAttrs()

	case op.Inode%numInodes == barOffset:
		attrs = fs.barAttrs()
	}

	// Fill in the response.
	op.Attributes = attrs
	op.AttributesExpiration = time.Now().Add(fs.getattrTimeout)

	return
}

func (fs *cachingFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	return
}

func (fs *cachingFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.KeepPageCache = fs.keepPageCache

	return
}

func (fs *cachingFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	op.BytesRead, err = io.ReadFull(rand.Reader, op.Dst)
	return
}
