// Copyright 2018 The CubeFS Authors.
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
	"time"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	LogTimeFormat = "20060102150405000"
)

func (s *Super) InodeGet(ino uint64) (*proto.InodeInfo, error) {
	info := s.ic.Get(ino)
	if info != nil {
		return info, nil
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
	s.ic.Put(info)
	node, isFind := s.nodeCache[ino]
	if isFind {
		if proto.IsDir(info.Mode) {
			node.(*Dir).info = info
		} else {
			node.(*File).info = info
		}
	}
	s.ec.RefreshExtentsCache(ino)
	return info, nil
}

func setattr(info *proto.InodeInfo, req *fuse.SetattrRequest) (valid uint32) {
	if req.Valid.Mode() {
		info.Mode = proto.Mode(req.Mode)
		valid |= proto.AttrMode
	}

	if req.Valid.Uid() {
		info.Uid = req.Uid
		valid |= proto.AttrUid
	}

	if req.Valid.Gid() {
		info.Gid = req.Gid
		valid |= proto.AttrGid
	}

	if req.Valid.Atime() {
		info.AccessTime = req.Atime
		valid |= proto.AttrAccessTime
	}

	if req.Valid.Mtime() {
		info.ModifyTime = req.Mtime
		valid |= proto.AttrModifyTime
	}

	return
}

func fillAttr(info *proto.InodeInfo, attr *fuse.Attr) {
	attr.Valid = AttrValidDuration
	attr.Nlink = info.Nlink
	attr.Inode = info.Inode
	attr.Mode = proto.OsMode(info.Mode)
	attr.Size = info.Size
	attr.Blocks = attr.Size >> 9 // In 512 bytes
	attr.Atime = info.AccessTime
	attr.Ctime = info.CreateTime
	attr.Mtime = info.ModifyTime
	attr.BlockSize = DefaultBlksize
	attr.Uid = info.Uid
	attr.Gid = info.Gid
}

func inodeExpired(info *proto.InodeInfo) bool {
	return time.Now().UnixNano() > info.Expiration()
}

func inodeSetExpiration(info *proto.InodeInfo, t time.Duration) {
	info.SetExpiration(time.Now().Add(t).UnixNano())
}
