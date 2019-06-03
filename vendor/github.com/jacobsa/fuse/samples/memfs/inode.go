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
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Common attributes for files and directories.
//
// External synchronization is required.
type inode struct {
	/////////////////////////
	// Mutable state
	/////////////////////////

	// The current attributes of this inode.
	//
	// INVARIANT: attrs.Mode &^ (os.ModePerm|os.ModeDir|os.ModeSymlink) == 0
	// INVARIANT: !(isDir() && isSymlink())
	// INVARIANT: attrs.Size == len(contents)
	attrs fuseops.InodeAttributes

	// For directories, entries describing the children of the directory. Unused
	// entries are of type DT_Unknown.
	//
	// This array can never be shortened, nor can its elements be moved, because
	// we use its indices for Dirent.Offset, which is exposed to the user who
	// might be calling readdir in a loop while concurrently modifying the
	// directory. Unused entries can, however, be reused.
	//
	// INVARIANT: If !isDir(), len(entries) == 0
	// INVARIANT: For each i, entries[i].Offset == i+1
	// INVARIANT: Contains no duplicate names in used entries.
	entries []fuseutil.Dirent

	// For files, the current contents of the file.
	//
	// INVARIANT: If !isFile(), len(contents) == 0
	contents []byte

	// For symlinks, the target of the symlink.
	//
	// INVARIANT: If !isSymlink(), len(target) == 0
	target string

	// extended attributes and values
	xattrs map[string][]byte
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// Create a new inode with the supplied attributes, which need not contain
// time-related information (the inode object will take care of that).
func newInode(
	attrs fuseops.InodeAttributes) (in *inode) {
	// Update time info.
	now := time.Now()
	attrs.Mtime = now
	attrs.Crtime = now

	// Create the object.
	in = &inode{
		attrs:  attrs,
		xattrs: make(map[string][]byte),
	}

	return
}

func (in *inode) CheckInvariants() {
	// INVARIANT: attrs.Mode &^ (os.ModePerm|os.ModeDir|os.ModeSymlink) == 0
	if !(in.attrs.Mode&^(os.ModePerm|os.ModeDir|os.ModeSymlink) == 0) {
		panic(fmt.Sprintf("Unexpected mode: %v", in.attrs.Mode))
	}

	// INVARIANT: !(isDir() && isSymlink())
	if in.isDir() && in.isSymlink() {
		panic(fmt.Sprintf("Unexpected mode: %v", in.attrs.Mode))
	}

	// INVARIANT: attrs.Size == len(contents)
	if in.attrs.Size != uint64(len(in.contents)) {
		panic(fmt.Sprintf(
			"Size mismatch: %d vs. %d",
			in.attrs.Size,
			len(in.contents)))
	}

	// INVARIANT: If !isDir(), len(entries) == 0
	if !in.isDir() && len(in.entries) != 0 {
		panic(fmt.Sprintf("Unexpected entries length: %d", len(in.entries)))
	}

	// INVARIANT: For each i, entries[i].Offset == i+1
	for i, e := range in.entries {
		if !(e.Offset == fuseops.DirOffset(i+1)) {
			panic(fmt.Sprintf("Unexpected offset for index %d: %d", i, e.Offset))
		}
	}

	// INVARIANT: Contains no duplicate names in used entries.
	childNames := make(map[string]struct{})
	for _, e := range in.entries {
		if e.Type != fuseutil.DT_Unknown {
			if _, ok := childNames[e.Name]; ok {
				panic(fmt.Sprintf("Duplicate name: %s", e.Name))
			}

			childNames[e.Name] = struct{}{}
		}
	}

	// INVARIANT: If !isFile(), len(contents) == 0
	if !in.isFile() && len(in.contents) != 0 {
		panic(fmt.Sprintf("Unexpected length: %d", len(in.contents)))
	}

	// INVARIANT: If !isSymlink(), len(target) == 0
	if !in.isSymlink() && len(in.target) != 0 {
		panic(fmt.Sprintf("Unexpected target length: %d", len(in.target)))
	}

	return
}

func (in *inode) isDir() bool {
	return in.attrs.Mode&os.ModeDir != 0
}

func (in *inode) isSymlink() bool {
	return in.attrs.Mode&os.ModeSymlink != 0
}

func (in *inode) isFile() bool {
	return !(in.isDir() || in.isSymlink())
}

// Return the index of the child within in.entries, if it exists.
//
// REQUIRES: in.isDir()
func (in *inode) findChild(name string) (i int, ok bool) {
	if !in.isDir() {
		panic("findChild called on non-directory.")
	}

	var e fuseutil.Dirent
	for i, e = range in.entries {
		if e.Name == name {
			ok = true
			return
		}
	}

	return
}

////////////////////////////////////////////////////////////////////////
// Public methods
////////////////////////////////////////////////////////////////////////

// Return the number of children of the directory.
//
// REQUIRES: in.isDir()
func (in *inode) Len() (n int) {
	for _, e := range in.entries {
		if e.Type != fuseutil.DT_Unknown {
			n++
		}
	}

	return
}

// Find an entry for the given child name and return its inode ID.
//
// REQUIRES: in.isDir()
func (in *inode) LookUpChild(name string) (
	id fuseops.InodeID,
	typ fuseutil.DirentType,
	ok bool) {
	index, ok := in.findChild(name)
	if ok {
		id = in.entries[index].Inode
		typ = in.entries[index].Type
	}

	return
}

// Add an entry for a child.
//
// REQUIRES: in.isDir()
// REQUIRES: dt != fuseutil.DT_Unknown
func (in *inode) AddChild(
	id fuseops.InodeID,
	name string,
	dt fuseutil.DirentType) {
	var index int

	// Update the modification time.
	in.attrs.Mtime = time.Now()

	// No matter where we place the entry, make sure it has the correct Offset
	// field.
	defer func() {
		in.entries[index].Offset = fuseops.DirOffset(index + 1)
	}()

	// Set up the entry.
	e := fuseutil.Dirent{
		Inode: id,
		Name:  name,
		Type:  dt,
	}

	// Look for a gap in which we can insert it.
	for index = range in.entries {
		if in.entries[index].Type == fuseutil.DT_Unknown {
			in.entries[index] = e
			return
		}
	}

	// Append it to the end.
	index = len(in.entries)
	in.entries = append(in.entries, e)
}

// Remove an entry for a child.
//
// REQUIRES: in.isDir()
// REQUIRES: An entry for the given name exists.
func (in *inode) RemoveChild(name string) {
	// Update the modification time.
	in.attrs.Mtime = time.Now()

	// Find the entry.
	i, ok := in.findChild(name)
	if !ok {
		panic(fmt.Sprintf("Unknown child: %s", name))
	}

	// Mark it as unused.
	in.entries[i] = fuseutil.Dirent{
		Type:   fuseutil.DT_Unknown,
		Offset: fuseops.DirOffset(i + 1),
	}
}

// Serve a ReadDir request.
//
// REQUIRES: in.isDir()
func (in *inode) ReadDir(p []byte, offset int) (n int) {
	if !in.isDir() {
		panic("ReadDir called on non-directory.")
	}

	for i := offset; i < len(in.entries); i++ {
		e := in.entries[i]

		// Skip unused entries.
		if e.Type == fuseutil.DT_Unknown {
			continue
		}

		tmp := fuseutil.WriteDirent(p[n:], in.entries[i])
		if tmp == 0 {
			break
		}

		n += tmp
	}

	return
}

// Read from the file's contents. See documentation for ioutil.ReaderAt.
//
// REQUIRES: in.isFile()
func (in *inode) ReadAt(p []byte, off int64) (n int, err error) {
	if !in.isFile() {
		panic("ReadAt called on non-file.")
	}

	// Ensure the offset is in range.
	if off > int64(len(in.contents)) {
		err = io.EOF
		return
	}

	// Read what we can.
	n = copy(p, in.contents[off:])
	if n < len(p) {
		err = io.EOF
	}

	return
}

// Write to the file's contents. See documentation for ioutil.WriterAt.
//
// REQUIRES: in.isFile()
func (in *inode) WriteAt(p []byte, off int64) (n int, err error) {
	if !in.isFile() {
		panic("WriteAt called on non-file.")
	}

	// Update the modification time.
	in.attrs.Mtime = time.Now()

	// Ensure that the contents slice is long enough.
	newLen := int(off) + len(p)
	if len(in.contents) < newLen {
		padding := make([]byte, newLen-len(in.contents))
		in.contents = append(in.contents, padding...)
		in.attrs.Size = uint64(newLen)
	}

	// Copy in the data.
	n = copy(in.contents[off:], p)

	// Sanity check.
	if n != len(p) {
		panic(fmt.Sprintf("Unexpected short copy: %v", n))
	}

	return
}

// Update attributes from non-nil parameters.
func (in *inode) SetAttributes(
	size *uint64,
	mode *os.FileMode,
	mtime *time.Time) {
	// Update the modification time.
	in.attrs.Mtime = time.Now()

	// Truncate?
	if size != nil {
		intSize := int(*size)

		// Update contents.
		if intSize <= len(in.contents) {
			in.contents = in.contents[:intSize]
		} else {
			padding := make([]byte, intSize-len(in.contents))
			in.contents = append(in.contents, padding...)
		}

		// Update attributes.
		in.attrs.Size = *size
	}

	// Change mode?
	if mode != nil {
		in.attrs.Mode = *mode
	}

	// Change mtime?
	if mtime != nil {
		in.attrs.Mtime = *mtime
	}
}
