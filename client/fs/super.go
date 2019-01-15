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
	"github.com/juju/errors"
	"golang.org/x/net/context"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/tiglabs/containerfs/sdk/data/stream"
	"github.com/tiglabs/containerfs/sdk/meta"
	"github.com/tiglabs/containerfs/util/log"
)

// Super defines the struct of a super block.
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

func NewSuper(volname, master string, icacheTimeout, lookupValid, attrValid int64) (s *Super, err error) {
	s = new(Super)
	s.mw, err = meta.NewMetaWrapper(volname, master)
	if err != nil {
		return nil, errors.Annotate(err, "NewMetaWrapper failed!")
	}

	s.ec, err = stream.NewExtentClient(volname, master, s.mw.AppendExtentKey, s.mw.GetExtents, s.mw.Truncate, s.mw.Open, s.mw.Release)
	if err != nil {
		return nil, errors.Annotate(err, "NewExtentClient failed!")
	}

	s.volname = volname
	s.cluster = s.mw.Cluster()
	inodeExpiration := DefaultInodeExpiration
	if icacheTimeout >= 0 {
		inodeExpiration = time.Duration(icacheTimeout) * time.Second
	}
	if lookupValid >= 0 {
		LookupValidDuration = time.Duration(lookupValid) * time.Second
	}
	if attrValid >= 0 {
		AttrValidDuration = time.Duration(attrValid) * time.Second
	}
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	s.orphan = NewOrphanInodeList()
	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
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

func (s *Super) exporterKey(act string) string {
	return fmt.Sprintf("%s_fuseclient_%s", s.cluster, act)
}

func (s *Super) ClusterName() string {
	return s.cluster
}
