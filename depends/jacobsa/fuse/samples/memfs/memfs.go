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

package memfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/syncutil"
)

type memFS struct {
	fuseutil.NotImplementedFileSystem

	// The UID and GID that every inode receives.
	uid uint32
	gid uint32

	/////////////////////////
	// Mutable state
	/////////////////////////

	mu syncutil.InvariantMutex

	// The collection of live inodes, indexed by ID. IDs of free inodes that may
	// be re-used have nil entries. No ID less than fuseops.RootInodeID is ever
	// used.
	//
	// All inodes are protected by the file system mutex.
	//
	// INVARIANT: For each inode in, in.CheckInvariants() does not panic.
	// INVARIANT: len(inodes) > fuseops.RootInodeID
	// INVARIANT: For all i < fuseops.RootInodeID, inodes[i] == nil
	// INVARIANT: inodes[fuseops.RootInodeID] != nil
	// INVARIANT: inodes[fuseops.RootInodeID].isDir()
	inodes []*inode // GUARDED_BY(mu)

	// A list of inode IDs within inodes available for reuse, not including the
	// reserved IDs less than fuseops.RootInodeID.
	//
	// INVARIANT: This is all and only indices i of 'inodes' such that i >
	// fuseops.RootInodeID and inodes[i] == nil
	freeInodes []fuseops.InodeID // GUARDED_BY(mu)
}

// Create a file system that stores data and metadata in memory.
//
// The supplied UID/GID pair will own the root inode. This file system does no
// permissions checking, and should therefore be mounted with the
// default_permissions option.
func NewMemFS(
	uid uint32,
	gid uint32) fuse.Server {
	// Set up the basic struct.
	fs := &memFS{
		inodes: make([]*inode, fuseops.RootInodeID+1),
		uid:    uid,
		gid:    gid,
	}

	// Set up the root inode.
	rootAttrs := fuseops.InodeAttributes{
		Mode: 0700 | os.ModeDir,
		Uid:  uid,
		Gid:  gid,
	}

	fs.inodes[fuseops.RootInodeID] = newInode(rootAttrs)

	// Set up invariant checking.
	fs.mu = syncutil.NewInvariantMutex(fs.checkInvariants)

	return fuseutil.NewFileSystemServer(fs)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (fs *memFS) checkInvariants() {
	// Check reserved inodes.
	for i := 0; i < fuseops.RootInodeID; i++ {
		if fs.inodes[i] != nil {
			panic(fmt.Sprintf("Non-nil inode for ID: %v", i))
		}
	}

	// Check the root inode.
	if !fs.inodes[fuseops.RootInodeID].isDir() {
		panic("Expected root to be a directory.")
	}

	// Build our own list of free IDs.
	freeIDsEncountered := make(map[fuseops.InodeID]struct{})
	for i := fuseops.RootInodeID + 1; i < len(fs.inodes); i++ {
		inode := fs.inodes[i]
		if inode == nil {
			freeIDsEncountered[fuseops.InodeID(i)] = struct{}{}
			continue
		}
	}

	// Check fs.freeInodes.
	if len(fs.freeInodes) != len(freeIDsEncountered) {
		panic(
			fmt.Sprintf(
				"Length mismatch: %v vs. %v",
				len(fs.freeInodes),
				len(freeIDsEncountered)))
	}

	for _, id := range fs.freeInodes {
		if _, ok := freeIDsEncountered[id]; !ok {
			panic(fmt.Sprintf("Unexected free inode ID: %v", id))
		}
	}

	// INVARIANT: For each inode in, in.CheckInvariants() does not panic.
	for _, in := range fs.inodes {
		in.CheckInvariants()
	}
}

// Find the given inode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *memFS) getInodeOrDie(id fuseops.InodeID) (inode *inode) {
	inode = fs.inodes[id]
	if inode == nil {
		panic(fmt.Sprintf("Unknown inode: %v", id))
	}

	return
}

// Allocate a new inode, assigning it an ID that is not in use.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *memFS) allocateInode(
	attrs fuseops.InodeAttributes) (id fuseops.InodeID, inode *inode) {
	// Create the inode.
	inode = newInode(attrs)

	// Re-use a free ID if possible. Otherwise mint a new one.
	numFree := len(fs.freeInodes)
	if numFree != 0 {
		id = fs.freeInodes[numFree-1]
		fs.freeInodes = fs.freeInodes[:numFree-1]
		fs.inodes[id] = inode
	} else {
		id = fuseops.InodeID(len(fs.inodes))
		fs.inodes = append(fs.inodes, inode)
	}

	return
}

// LOCKS_REQUIRED(fs.mu)
func (fs *memFS) deallocateInode(id fuseops.InodeID) {
	fs.freeInodes = append(fs.freeInodes, id)
	fs.inodes[id] = nil
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fs *memFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	return
}

func (fs *memFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent directory.
	inode := fs.getInodeOrDie(op.Parent)

	// Does the directory have an entry with the given name?
	childID, _, ok := inode.LookUpChild(op.Name)
	if !ok {
		err = fuse.ENOENT
		return
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return
}

func (fs *memFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the inode.
	inode := fs.getInodeOrDie(op.Inode)

	// Fill in the response.
	op.Attributes = inode.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return
}

func (fs *memFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the inode.
	inode := fs.getInodeOrDie(op.Inode)

	// Handle the request.
	inode.SetAttributes(op.Size, op.Mode, op.Mtime)

	// Fill in the response.
	op.Attributes = inode.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return
}

func (fs *memFS) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		err = fuse.EEXIST
		return
	}

	// Set up attributes from the child.
	childAttrs := fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  op.Mode,
		Uid:   fs.uid,
		Gid:   fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, op.Name, fuseutil.DT_Directory)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return
}

func (fs *memFS) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.Entry, err = fs.createFile(op.Parent, op.Name, op.Mode)
	return
}

// LOCKS_REQUIRED(fs.mu)
func (fs *memFS) createFile(
	parentID fuseops.InodeID,
	name string,
	mode os.FileMode) (entry fuseops.ChildInodeEntry, err error) {
	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(parentID)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		err = fuse.EEXIST
		return
	}

	// Set up attributes for the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Mode:   mode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.uid,
		Gid:    fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, name, fuseutil.DT_File)

	// Fill in the response entry.
	entry.Child = childID
	entry.Attributes = child.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	entry.EntryExpiration = entry.AttributesExpiration

	return
}

func (fs *memFS) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.Entry, err = fs.createFile(op.Parent, op.Name, op.Mode)
	return
}

func (fs *memFS) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		err = fuse.EEXIST
		return
	}

	// Set up attributes from the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Mode:   0444 | os.ModeSymlink,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.uid,
		Gid:    fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

	// Set up its target.
	child.target = op.Target

	// Add an entry in the parent.
	parent.AddChild(childID, op.Name, fuseutil.DT_Link)

	// Fill in the response entry.
	op.Entry.Child = childID
	op.Entry.Attributes = child.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return
}

func (fs *memFS) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		err = fuse.EEXIST
		return
	}

	// Get the target inode to be linked
	target := fs.getInodeOrDie(op.Target)

	// Update the attributes
	now := time.Now()
	target.attrs.Nlink++
	target.attrs.Ctime = now

	// Add an entry in the parent.
	parent.AddChild(op.Target, op.Name, fuseutil.DT_File)

	// Return the response.
	op.Entry.Child = op.Target
	op.Entry.Attributes = target.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return
}

func (fs *memFS) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ask the old parent for the child's inode ID and type.
	oldParent := fs.getInodeOrDie(op.OldParent)
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
		err = fuse.ENOENT
		return
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParent := fs.getInodeOrDie(op.NewParent)
	existingID, _, ok := newParent.LookUpChild(op.NewName)
	if ok {
		existing := fs.getInodeOrDie(existingID)

		var buf [4096]byte
		if existing.isDir() && existing.ReadDir(buf[:], 0) > 0 {
			err = fuse.ENOTEMPTY
			return
		}

		newParent.RemoveChild(op.NewName)
	}

	// Link the new name.
	newParent.AddChild(
		childID,
		op.NewName,
		childType)

	// Finally, remove the old name from the old parent.
	oldParent.RemoveChild(op.OldName)

	return
}

func (fs *memFS) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		err = fuse.ENOENT
		return
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Make sure the child is empty.
	if child.Len() != 0 {
		err = fuse.ENOTEMPTY
		return
	}

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	child.attrs.Nlink--

	return
}

func (fs *memFS) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		err = fuse.ENOENT
		return
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	child.attrs.Nlink--

	return
}

func (fs *memFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// inode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	inode := fs.getInodeOrDie(op.Inode)

	if !inode.isDir() {
		panic("Found non-dir.")
	}

	return
}

func (fs *memFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the directory.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	op.BytesRead = inode.ReadDir(op.Dst, int(op.Offset))

	return
}

func (fs *memFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// inode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	inode := fs.getInodeOrDie(op.Inode)

	if !inode.isFile() {
		panic("Found non-file.")
	}

	return
}

func (fs *memFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode in question.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	op.BytesRead, err = inode.ReadAt(op.Dst, op.Offset)

	// Don't return EOF errors; we just indicate EOF to fuse using a short read.
	if err == io.EOF {
		err = nil
	}

	return
}

func (fs *memFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode in question.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	_, err = inode.WriteAt(op.Data, op.Offset)

	return
}

func (fs *memFS) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode in question.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	op.Target = inode.target

	return
}

func (fs *memFS) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode := fs.getInodeOrDie(op.Inode)
	if value, ok := inode.xattrs[op.Name]; ok {
		op.BytesRead = len(value)
		if len(op.Dst) >= len(value) {
			copy(op.Dst, value)
		} else {
			err = syscall.ERANGE
		}
	} else {
		err = fuse.ENOATTR
	}

	return
}

func (fs *memFS) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode := fs.getInodeOrDie(op.Inode)

	dst := op.Dst[:]
	for key := range inode.xattrs {
		keyLen := len(key) + 1

		if err == nil && len(dst) >= keyLen {
			copy(dst, key)
			dst = dst[keyLen:]
		} else {
			err = syscall.ERANGE
		}
		op.BytesRead += keyLen
	}

	return
}

func (fs *memFS) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	inode := fs.getInodeOrDie(op.Inode)

	if _, ok := inode.xattrs[op.Name]; ok {
		delete(inode.xattrs, op.Name)
	} else {
		err = fuse.ENOATTR
	}
	return
}

func (fs *memFS) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	inode := fs.getInodeOrDie(op.Inode)

	_, ok := inode.xattrs[op.Name]

	switch op.Flags {
	case 0x1:
		if ok {
			err = fuse.EEXIST
		}
	case 0x2:
		if !ok {
			err = fuse.ENOATTR
		}
	}

	if err == nil {
		value := make([]byte, len(op.Value))
		copy(value, op.Value)
		inode.xattrs[op.Name] = value
	}

	return
}
