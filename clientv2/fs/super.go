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
	"golang.org/x/net/context"
	"net/http"
	"strconv"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

type Super struct {
	cluster     string
	localIP     string
	volname     string
	owner       string
	ic          *InodeCache
	hc          *HandleCache
	mw          *meta.MetaWrapper
	ec          *stream.ExtentClient
	orphan      *OrphanInodeList
	enSyncWrite bool
	keepCache   bool
}

var (
	_ fuseutil.FileSystem = (*Super)(nil)
)

func NewSuper(opt *proto.MountOptions) (s *Super, err error) {
	s = new(Super)
	s.mw, err = meta.NewMetaWrapper(opt, true)
	if err != nil {
		return nil, errors.Trace(err, "NewMetaWrapper failed!")
	}

	s.ec, err = stream.NewExtentClient(opt, s.mw.AppendExtentKey, s.mw.GetExtents, s.mw.Truncate)
	if err != nil {
		return nil, errors.Trace(err, "NewExtentClient failed!")
	}

	s.volname = opt.Volname
	s.owner = opt.Owner
	s.cluster = s.mw.Cluster()
	s.localIP = s.mw.LocalIP()
	inodeExpiration := DefaultInodeExpiration
	if opt.IcacheTimeout >= 0 {
		inodeExpiration = time.Duration(opt.IcacheTimeout) * time.Second
	}
	if opt.LookupValid >= 0 {
		LookupValidDuration = time.Duration(opt.LookupValid) * time.Second
	}
	if opt.AttrValid >= 0 {
		AttrValidDuration = time.Duration(opt.AttrValid) * time.Second
	}
	if opt.EnSyncWrite > 0 {
		s.enSyncWrite = true
	}
	s.keepCache = opt.KeepCache
	s.hc = NewHandleCache()
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	s.orphan = NewOrphanInodeList()
	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

func (s *Super) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	total, used := s.mw.Statfs()
	op.BlockSize = uint32(DefaultBlksize)
	op.Blocks = total / uint64(DefaultBlksize)
	op.BlocksFree = (total - used) / uint64(DefaultBlksize)
	op.BlocksAvailable = op.BlocksFree
	op.IoSize = 1 << 20
	op.Inodes = 1 << 50
	op.InodesFree = op.Inodes
	return nil
}

func (s *Super) Destroy() {
}

func (s *Super) ClusterName() string {
	return s.cluster
}

func (s *Super) TokenType() int8 {
	return s.mw.TokenType()
}

func (s *Super) GetRate(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(s.ec.GetRate()))
}

func (s *Super) SetRate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if rate := r.FormValue("read"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set read rate failed\n"))
		} else {
			msg := s.ec.SetReadRate(val)
			w.Write([]byte(fmt.Sprintf("Set read rate to %v successfully\n", msg)))
		}
	}

	if rate := r.FormValue("write"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set write rate failed\n"))
		} else {
			msg := s.ec.SetWriteRate(val)
			w.Write([]byte(fmt.Sprintf("Set write rate to %v successfully\n", msg)))
		}
	}
}

func (s *Super) exporterKey(act string) string {
	return fmt.Sprintf("%s_fuseclient_%s", s.cluster, act)
}
