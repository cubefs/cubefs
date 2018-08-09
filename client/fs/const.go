// Copyright 2018 The ChuBao Authors.
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
	"syscall"
	"time"

	"github.com/chubaoio/cbfs/fuse"

	"github.com/chubaoio/cbfs/proto"
)

const (
	RootInode = proto.RootIno
)

const (
	DIR_NLINK_DEFAULT     = 2
	REGULAR_NLINK_DEFAULT = 1
)

const (
	DefaultBlksize    = uint32(1) << 12
	DefaultMaxNameLen = uint32(256)
)

const (
	ModeRegular = proto.ModeRegular
	ModeDir     = proto.ModeDir
	ModeSymlink = proto.ModeSymlink
)

const (
	LookupValidDuration = 30 * time.Second
	AttrValidDuration   = 30 * time.Second
)

const (
	DefaultInodeExpiration = 120 * time.Second
	MaxInodeCache          = 10000000
)

const (
	DeleteExtentsTimeout = 600 * time.Second
)

func ParseError(err error) fuse.Errno {
	switch v := err.(type) {
	case syscall.Errno:
		return fuse.Errno(v)
	case fuse.Errno:
		return v
	default:
		return fuse.ENOSYS
	}
}

func ParseMode(mode uint32) fuse.DirentType {
	switch mode {
	case ModeDir:
		return fuse.DT_Dir
	case ModeSymlink:
		return fuse.DT_Link
	default:
		return fuse.DT_File
	}
}
