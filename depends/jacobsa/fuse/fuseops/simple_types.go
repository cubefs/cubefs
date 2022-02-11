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

package fuseops

import (
	"fmt"
	"os"
	"time"

	"github.com/jacobsa/fuse/internal/fusekernel"
)

// InodeID is a 64-bit number used to uniquely identify a file or directory in
// the file system. File systems may mint inode IDs with any value except for
// RootInodeID.
//
// This corresponds to struct inode::i_no in the VFS layer.
// (Cf. http://goo.gl/tvYyQt)
type InodeID uint64

// RootInodeID is a distinguished inode ID that identifies the root of the file
// system, e.g. in an OpenDirOp or LookUpInodeOp. Unlike all other inode IDs,
// which are minted by the file system, the FUSE VFS layer may send a request
// for this ID without the file system ever having referenced it in a previous
// response.
const RootInodeID = 1

func init() {
	// Make sure the constant above is correct. We do this at runtime rather than
	// defining the constant in terms of fusekernel.RootID for two reasons:
	//
	//  1. Users can more clearly see that the root ID is low and can therefore
	//     be used as e.g. an array index, with space reserved up to the root.
	//
	//  2. The constant can be untyped and can therefore more easily be used as
	//     an array index.
	//
	if RootInodeID != fusekernel.RootID {
		panic(
			fmt.Sprintf(
				"Oops, RootInodeID is wrong: %v vs. %v",
				RootInodeID,
				fusekernel.RootID))
	}
}

// InodeAttributes contains attributes for a file or directory inode. It
// corresponds to struct inode (cf. http://goo.gl/tvYyQt).
type InodeAttributes struct {
	Size uint64

	// The number of incoming hard links to this inode.
	Nlink uint32

	// The mode of the inode. This is exposed to the user in e.g. the result of
	// fstat(2).
	//
	// Note that in contrast to the defaults for FUSE, this package mounts file
	// systems in a manner such that the kernel checks inode permissions in the
	// standard posix way. This is implemented by setting the default_permissions
	// mount option (cf. http://goo.gl/1LxOop and http://goo.gl/1pTjuk).
	//
	// For example, in the case of mkdir:
	//
	//  *  (http://goo.gl/JkdxDI) sys_mkdirat calls inode_permission.
	//
	//  *  (...) inode_permission eventually calls do_inode_permission.
	//
	//  *  (http://goo.gl/aGCsmZ) calls i_op->permission, which is
	//     fuse_permission (cf. http://goo.gl/VZ9beH).
	//
	//  *  (http://goo.gl/5kqUKO) fuse_permission doesn't do anything at all for
	//     several code paths if FUSE_DEFAULT_PERMISSIONS is unset. In contrast,
	//     if that flag *is* set, then it calls generic_permission.
	//
	Mode os.FileMode

	// Time information. See `man 2 stat` for full details.
	Atime  time.Time // Time of last access
	Mtime  time.Time // Time of last modification
	Ctime  time.Time // Time of last modification to inode
	Crtime time.Time // Time of creation (OS X only)

	// Ownership information
	Uid uint32
	Gid uint32
}

func (a *InodeAttributes) DebugString() string {
	return fmt.Sprintf(
		"%d %d %v %d %d",
		a.Size,
		a.Nlink,
		a.Mode,
		a.Uid,
		a.Gid)
}

// GenerationNumber represents a generation of an inode. It is irrelevant for
// file systems that won't be exported over NFS. For those that will and that
// reuse inode IDs when they become free, the generation number must change
// when an ID is reused.
//
// This corresponds to struct inode::i_generation in the VFS layer.
// (Cf. http://goo.gl/tvYyQt)
//
// Some related reading:
//
//     http://fuse.sourceforge.net/doxygen/structfuse__entry__param.html
//     http://stackoverflow.com/q/11071996/1505451
//     http://goo.gl/CqvwyX
//     http://julipedia.meroh.net/2005/09/nfs-file-handles.html
//     http://goo.gl/wvo3MB
//
type GenerationNumber uint64

// HandleID is an opaque 64-bit number used to identify a particular open
// handle to a file or directory.
//
// This corresponds to fuse_file_info::fh.
type HandleID uint64

// DirOffset is an offset into an open directory handle. This is opaque to
// FUSE, and can be used for whatever purpose the file system desires. See
// notes on ReadDirOp.Offset for details.
type DirOffset uint64

// ChildInodeEntry contains information about a child inode within its parent
// directory. It is shared by LookUpInodeOp, MkDirOp, CreateFileOp, etc, and is
// consumed by the kernel in order to set up a dcache entry.
type ChildInodeEntry struct {
	// The ID of the child inode. The file system must ensure that the returned
	// inode ID remains valid until a later ForgetInodeOp.
	Child InodeID

	// A generation number for this incarnation of the inode with the given ID.
	// See comments on type GenerationNumber for more.
	Generation GenerationNumber

	// Current attributes for the child inode.
	//
	// When creating a new inode, the file system is responsible for initializing
	// and recording (where supported) attributes like time information,
	// ownership information, etc.
	//
	// Ownership information in particular must be set to something reasonable or
	// by default root will own everything and unprivileged users won't be able
	// to do anything useful. In traditional file systems in the kernel, the
	// function inode_init_owner (http://goo.gl/5qavg8) contains the
	// standards-compliant logic for this.
	Attributes InodeAttributes

	// The FUSE VFS layer in the kernel maintains a cache of file attributes,
	// used whenever up to date information about size, mode, etc. is needed.
	//
	// For example, this is the abridged call chain for fstat(2):
	//
	//  *  (http://goo.gl/tKBH1p) fstat calls vfs_fstat.
	//  *  (http://goo.gl/3HeITq) vfs_fstat eventuall calls vfs_getattr_nosec.
	//  *  (http://goo.gl/DccFQr) vfs_getattr_nosec calls i_op->getattr.
	//  *  (http://goo.gl/dpKkst) fuse_getattr calls fuse_update_attributes.
	//  *  (http://goo.gl/yNlqPw) fuse_update_attributes uses the values in the
	//     struct inode if allowed, otherwise calling out to the user-space code.
	//
	// In addition to obvious cases like fstat, this is also used in more subtle
	// cases like updating size information before seeking (http://goo.gl/2nnMFa)
	// or reading (http://goo.gl/FQSWs8).
	//
	// Most 'real' file systems do not set inode_operations::getattr, and
	// therefore vfs_getattr_nosec calls generic_fillattr which simply grabs the
	// information from the inode struct. This makes sense because these file
	// systems cannot spontaneously change; all modifications go through the
	// kernel which can update the inode struct as appropriate.
	//
	// In contrast, a FUSE file system may have spontaneous changes, so it calls
	// out to user space to fetch attributes. However this is expensive, so the
	// FUSE layer in the kernel caches the attributes if requested.
	//
	// This field controls when the attributes returned in this response and
	// stashed in the struct inode should be re-queried. Leave at the zero value
	// to disable caching.
	//
	// More reading:
	//     http://stackoverflow.com/q/21540315/1505451
	AttributesExpiration time.Time

	// The time until which the kernel may maintain an entry for this name to
	// inode mapping in its dentry cache. After this time, it will revalidate the
	// dentry.
	//
	// As in the discussion of attribute caching above, unlike real file systems,
	// FUSE file systems may spontaneously change their name -> inode mapping.
	// Therefore the FUSE VFS layer uses dentry_operations::d_revalidate
	// (http://goo.gl/dVea0h) to intercept lookups and revalidate by calling the
	// user-space LookUpInode method. However the latter may be slow, so it
	// caches the entries until the time defined by this field.
	//
	// Example code walk:
	//
	//     * (http://goo.gl/M2G3tO) lookup_dcache calls d_revalidate if enabled.
	//     * (http://goo.gl/ef0Elu) fuse_dentry_revalidate just uses the dentry's
	//     inode if fuse_dentry_time(entry) hasn't passed. Otherwise it sends a
	//     lookup request.
	//
	// Leave at the zero value to disable caching.
	//
	// Beware: this value is ignored on OS X, where entry caching is disabled by
	// default. See notes on MountConfig.EnableVnodeCaching for more.
	EntryExpiration time.Time
}
