// Copyright 2022 The CubeFS Authors.
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

package proxy

import (
	"github.com/cubefs/cubefs/proto"
)

type DataOp interface {
	OpenStream(inode uint64) error
	Write(inode uint64, offset int, data []byte, flags int) (write int, err error)
	Read(inode uint64, data []byte, offset int, size int) (read int, err error)
	Flush(inode uint64) error
	Truncate(inode uint64, size int) error
}

type MetaOp interface {
	LookupPath(subdir string) (uint64, error)
	Create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
	ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error)
	BatchInodeGetWith(inodes []uint64) (batchInfos []*proto.InodeInfo, err error)
	Delete_ll(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error)
	Truncate(inode, size uint64) error
	Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error)
	Link(parentID uint64, name string, ino uint64) (*proto.InodeInfo, error)
	XAttrSet_ll(inode uint64, name, value []byte) error
	XAttrGet_ll(inode uint64, name string) (*proto.XAttrInfo, error)
	XAttrDel_ll(inode uint64, name string) error
	XAttrsList_ll(inode uint64) ([]string, error)
	Setattr(inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error
	Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, overwritten bool) (err error)
	InodeGet_ll(inode uint64) (*proto.InodeInfo, error)
	ReadDir_ll(parentID uint64) ([]proto.Dentry, error)
}
