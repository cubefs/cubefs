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

	"bazil.org/fuse"

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
}

// NewInode returns a new inode.
func NewInode(info *proto.InodeInfo) *Inode {
	inode := new(Inode)
	inode.fill(info)
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

func (inode *Inode) setattr(req *fuse.SetattrRequest) (valid uint32) {
	if req.Valid.Mode() {
		inode.mode = req.Mode
		valid |= proto.AttrMode
	}

	if req.Valid.Uid() {
		inode.uid = req.Uid
		valid |= proto.AttrUid
	}

	if req.Valid.Gid() {
		inode.gid = req.Gid
		valid |= proto.AttrGid
	}
	return
}

func (inode *Inode) fill(info *proto.InodeInfo) {
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

func (inode *Inode) fillAttr(attr *fuse.Attr) {
	attr.Valid = AttrValidDuration
	attr.Nlink = inode.nlink
	attr.Inode = inode.ino
	attr.Mode = inode.mode
	attr.Size = inode.size
	attr.Blocks = attr.Size >> 9 // In 512 bytes
	attr.Atime = inode.atime
	attr.Ctime = inode.ctime
	attr.Mtime = inode.mtime
	attr.BlockSize = DefaultBlksize
	attr.Uid = inode.uid
	attr.Gid = inode.gid
}

func (inode *Inode) expired() bool {
	if time.Now().UnixNano() > inode.expiration {
		return true
	}
	return false
}

func (inode *Inode) setExpiration(t time.Duration) {
	inode.expiration = time.Now().Add(t).UnixNano()
}
