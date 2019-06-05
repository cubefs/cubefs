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
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/chubaofs/chubaofs/proto"
)

const (
	RootInode = proto.RootIno
)

const (
	DefaultBlksize    = uint32(1) << 12
	DefaultMaxNameLen = uint32(256)
)

const (
	DefaultInodeExpiration = 120 * time.Second
	MaxInodeCache          = 10000000 // in terms of the number of items
)

const (
	// the expiration duration of the dentry in the cache (used internally)
	DentryValidDuration = 5 * time.Second
)

const (
	DeleteExtentsTimeout = 600 * time.Second
)

var (
	// The following two are used in the FUSE cache
	// every time the lookup will be performed on the fly, and the result will not be cached
	LookupValidDuration = 5 * time.Second
	// the expiration duration of the attributes in the FUSE cache
	AttrValidDuration = 30 * time.Second
)

// ParseError returns the error type.
func ParseError(err error) syscall.Errno {
	switch v := err.(type) {
	case syscall.Errno:
		return v
	default:
		return fuse.ENOSYS
	}
}

// ParseType returns the dentry type.
func ParseType(t uint32) fuseutil.DirentType {
	if proto.IsDir(t) {
		return fuseutil.DT_Directory
	} else if proto.IsSymlink(t) {
		return fuseutil.DT_Link
	}
	return fuseutil.DT_File
}
