// Copyright 2018 The Chubao Authors.
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

package fs

import (
	"fmt"
	"os"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	LogTimeFormat = "20060102150405000"
)

// Inode defines the structure of an inode.
type Inode struct {
	ino    uint64
	size   uint64
	nlink  uint32
	uid    uint32
	gid    uint32
	gen    uint64
	ctime  time.Time // time of last inode change
	mtime  time.Time // time of last modification
	atime  time.Time // time of last access
	mode   os.FileMode
	target []byte

	// protected under the inode cache lock
	expiration int64

	// For directory inode only
	dcache *DentryCache
}

// NewInode returns a new inode.
func NewInode(info *proto.InodeInfo) *Inode {
	inode := new(Inode)
	fillInode(inode, info)
	return inode
}

// InodeGet return the inode based on the given inode ID.
func (s *Super) InodeGet(ino uint64) (*Inode, error) {
	inode := s.ic.Get(ino)
	if inode != nil {
		//log.LogDebugf("InodeCache hit: inode(%v)", inode)
		return inode, nil
	}

	info, err := s.mw.InodeGet_ll(ino)
	if err != nil || info == nil {
		log.LogErrorf("InodeGet: ino(%v) err(%v) info(%v)", ino, err, info)
		if err != nil {
			return nil, ParseError(err)
		} else {
			return nil, fuse.ENOENT
		}
	}
	inode = NewInode(info)
	s.ic.Put(inode)
	s.ec.RefreshExtentsCache(ino)
	return inode, nil
}

// String returns the string format of the inode.
func (inode *Inode) String() string {
	return fmt.Sprintf("ino(%v) mode(%v) size(%v) nlink(%v) gen(%v) uid(%v) gid(%v) exp(%v) mtime(%v) target(%v)", inode.ino, inode.mode, inode.size, inode.nlink, inode.gen, inode.uid, inode.gid, time.Unix(0, inode.expiration).Format(LogTimeFormat), inode.mtime, inode.target)
}

func setattr(op *fuseops.SetInodeAttributesOp, inode *Inode) (valid uint32) {
	if op.Mode != nil {
		inode.mode = *op.Mode
		valid |= proto.AttrMode
		op.Attributes.Mode = *op.Mode
	}

	if op.Uid != nil {
		inode.uid = *op.Uid
		valid |= proto.AttrUid
		op.Attributes.Uid = *op.Uid
	}

	if op.Gid != nil {
		inode.gid = *op.Gid
		valid |= proto.AttrGid
		op.Attributes.Gid = *op.Gid
	}

	return
}

func fillInode(inode *Inode, info *proto.InodeInfo) {
	inode.ino = info.Inode
	inode.size = info.Size
	inode.nlink = info.Nlink
	inode.uid = info.Uid
	inode.gid = info.Gid
	inode.gen = info.Generation
	inode.ctime = info.CreateTime
	inode.atime = info.AccessTime
	inode.mtime = info.ModifyTime
	inode.target = info.Target
	inode.mode = proto.OsMode(info.Mode)
}

func fillAttr(attr *fuseops.InodeAttributes, inode *Inode) {
	attr.Nlink = inode.nlink
	attr.Mode = inode.mode
	attr.Size = inode.size
	attr.Atime = inode.atime
	attr.Ctime = inode.ctime
	attr.Mtime = inode.mtime
	attr.Uid = inode.uid
	attr.Gid = inode.gid
}

func fillChildEntry(entry *fuseops.ChildInodeEntry, inode *Inode) {
	entry.Child = fuseops.InodeID(inode.ino)
	entry.AttributesExpiration = time.Now().Add(AttrValidDuration)
	entry.EntryExpiration = time.Now().Add(LookupValidDuration)
	fillAttr(&entry.Attributes, inode)
}

func (inode *Inode) expired() bool {
	// Root inode never gets expired
	if inode.ino != proto.RootIno && time.Now().UnixNano() > inode.expiration {
		return true
	}
	return false
}

func (inode *Inode) setExpiration(t time.Duration) {
	inode.expiration = time.Now().Add(t).UnixNano()
}
