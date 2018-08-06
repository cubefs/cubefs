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
	"fmt"
	"time"

	"github.com/chubaoio/cbfs/fuse"
	"github.com/chubaoio/cbfs/fuse/fs"
	"golang.org/x/net/context"

	"github.com/chubaoio/cbfs/sdk/data/stream"
	"github.com/chubaoio/cbfs/sdk/meta"
	"github.com/chubaoio/cbfs/util/log"
)

type Super struct {
	cluster string
	volname string
	ic      *InodeCache
	mw      *meta.MetaWrapper
	ec      *stream.ExtentClient
	orphan  *OrphanInodeList
}

//functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

func NewSuper(volname, master string, bufferSize uint64, icacheTimeout int64) (s *Super, err error) {
	s = new(Super)
	s.mw, err = meta.NewMetaWrapper(volname, master)
	if err != nil {
		log.LogErrorf("NewMetaWrapper failed! %v", err.Error())
		return nil, err
	}

	s.ec, err = stream.NewExtentClient(volname, master, s.mw.AppendExtentKey, s.mw.GetExtents, bufferSize)
	if err != nil {
		log.LogErrorf("NewExtentClient failed! %v", err.Error())
		return nil, err
	}

	s.volname = volname
	s.cluster = s.mw.Cluster()
	inodeExpiration := DefaultInodeExpiration
	if icacheTimeout > 0 {
		inodeExpiration = time.Duration(icacheTimeout) * time.Second
	}
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	//s.orphan = NewOrphanInodeList()
	log.LogInfof("NewSuper: cluster(%v) volname(%v)", s.cluster, s.volname)
	return s, nil
}

func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(RootInode)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	total, used := s.mw.Statfs()
	resp.Blocks = total / uint64(DefaultBlksize)
	resp.Bfree = (total - used) / uint64(DefaultBlksize)
	resp.Bavail = resp.Bfree
	resp.Bsize = DefaultBlksize
	resp.Namelen = DefaultMaxNameLen
	resp.Frsize = DefaultBlksize
	return nil
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%s_fuseclient_%s", s.cluster, act)
}
