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
	"github.com/juju/errors"
	"golang.org/x/net/context"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chubaofs/cfs/sdk/data/stream"
	"github.com/chubaofs/cfs/sdk/meta"
	"github.com/chubaofs/cfs/util/log"
)

// Super defines the struct of a super block.
type Super struct {
	cluster     string
	volname     string
	owner       string
	ic          *InodeCache
	mw          *meta.MetaWrapper
	ec          *stream.ExtentClient
	orphan      *OrphanInodeList
	enSyncWrite bool
}

// Functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

// NewSuper returns a new Super.
func NewSuper(volname, owner, master string, icacheTimeout, lookupValid, attrValid, enSyncWrite int64) (s *Super, err error) {
	s = new(Super)
	s.mw, err = meta.NewMetaWrapper(volname, owner, master)
	if err != nil {
		return nil, errors.Annotate(err, "NewMetaWrapper failed!")
	}

	s.ec, err = stream.NewExtentClient(volname, master, s.mw.AppendExtentKey, s.mw.GetExtents, s.mw.Truncate, s.mw.Open, s.mw.Release)
	if err != nil {
		return nil, errors.Annotate(err, "NewExtentClient failed!")
	}

	s.volname = volname
	s.owner = owner
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
	if enSyncWrite > 0 {
		s.enSyncWrite = true
	}
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	s.orphan = NewOrphanInodeList()
	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

// Root returns the root directory where it resides.
func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(RootInode)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

// Statfs handles the Statfs request and returns a set of statistics.
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

// ClusterName returns the cluster name.
func (s *Super) ClusterName() string {
	return s.cluster
}
