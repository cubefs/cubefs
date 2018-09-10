// Copyright 2018 The Containerfs Authors.
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

	"github.com/tiglabs/containerfs/fuse"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
)

const (
	LogTimeFormat = "20060102150405000"
)

type Inode struct {
	ino    uint64
	size   uint64
	mode   uint32
	nlink  uint32
	ctime  time.Time
	mtime  time.Time
	atime  time.Time
	osMode os.FileMode
	target []byte

	// protected under the inode cache lock
	expiration int64
}

func NewInode(info *proto.InodeInfo) *Inode {
	inode := new(Inode)
	inode.fill(info)
	return inode
}

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
	return inode, nil
}

func (inode *Inode) String() string {
	return fmt.Sprintf("ino(%v) mode(%v) size(%v) nlink(%v) exp(%v) mtime(%v)", inode.ino, inode.mode, inode.size, inode.nlink, time.Unix(0, inode.expiration).Format(LogTimeFormat), inode.mtime)
}

func (inode *Inode) fill(info *proto.InodeInfo) {
	inode.ino = info.Inode
	inode.mode = info.Mode
	inode.size = info.Size
	inode.nlink = info.Nlink
	inode.ctime = info.CreateTime
	inode.atime = info.AccessTime
	inode.mtime = info.ModifyTime
	inode.target = info.Target

	if inode.mode == ModeDir {
		inode.osMode = os.ModeDir | os.ModePerm
	} else if inode.mode == ModeSymlink {
		inode.osMode = os.ModeSymlink | os.ModePerm
	} else {
		inode.osMode = os.ModePerm
	}
}

func (inode *Inode) fillAttr(attr *fuse.Attr) {
	attr.Valid = AttrValidDuration
	attr.Nlink = inode.nlink
	attr.Inode = inode.ino
	attr.Mode = inode.osMode
	attr.Size = inode.size
	attr.Blocks = attr.Size >> 9 // In 512 bytes
	attr.Atime = inode.atime
	attr.Ctime = inode.ctime
	attr.Mtime = inode.mtime
	attr.BlockSize = DefaultBlksize
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
