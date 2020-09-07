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
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
)

// Super defines the struct of a super block.
type Super struct {
	cluster     string
	volname     string
	owner       string
	ic          *InodeCache
	mw          *meta.MetaWrapper
	ec          *stream.ExtentClient
	mc          *masterSDK.MasterClient
	orphan      *OrphanInodeList
	enSyncWrite bool
	keepCache   bool

	nodeCache map[uint64]fs.Node
	fslock    sync.Mutex

	disableDcache bool
	fsyncOnClose  bool
	enableXattr   bool
	rootIno       uint64

	clientCreateRate float64
	clientDeleteRate float64
	clientReadRate   float64
	clientWriteRate  float64

	serverCreateRate float64
	serverDeleteRate float64
	serverReadRate   float64
	serverWriteRate  float64
}

// Functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

// NewSuper returns a new Super.
func NewSuper(opt *proto.MountOptions) (s *Super, err error) {
	s = new(Super)
	var masters = strings.Split(opt.Master, meta.HostsSeparator)
	var metaConfig = &meta.MetaConfig{
		Volume:        opt.Volname,
		Owner:         opt.Owner,
		Masters:       masters,
		Authenticate:  opt.Authenticate,
		TicketMess:    opt.TicketMess,
		ValidateOwner: opt.Authenticate || opt.AccessKey == "",
		CreateRate:        opt.CreateRate,
		DeleteRate:        opt.DeleteRate,
	}
	s.mw, err = meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewMetaWrapper failed!")
	}

	s.volname = opt.Volname
	s.owner = opt.Owner
	s.cluster = s.mw.Cluster()
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
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	s.orphan = NewOrphanInodeList()
	s.nodeCache = make(map[uint64]fs.Node)
	s.disableDcache = opt.DisableDcache
	s.fsyncOnClose = opt.FsyncOnClose
	s.enableXattr = opt.EnableXattr

	var extentConfig = &stream.ExtentConfig{
		Volume:            opt.Volname,
		Masters:           masters,
		FollowerRead:      opt.FollowerRead,
		NearRead:          opt.NearRead,
		ReadRate:          opt.ReadRate,
		WriteRate:         opt.WriteRate,
		OnAppendExtentKey: s.mw.AppendExtentKey,
		OnGetExtents:      s.mw.GetExtents,
		OnTruncate:        s.mw.Truncate,
		OnEvictIcache:     s.ic.Delete,
	}
	s.ec, err = stream.NewExtentClient(extentConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewExtentClient failed!")
	}

	if s.rootIno, err = s.mw.GetRootIno(opt.SubDir); err != nil {
		return nil, err
	}

	s.mc = masterSDK.NewMasterClient(masters, false)

	go s.updateLimiter()

	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

// Root returns the root directory where it resides.
func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(s.rootIno)
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

// ClusterName returns the cluster name.
func (s *Super) ClusterName() string {
	return s.cluster
}

func (s *Super) GetRate(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("create rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n"+
		"delete rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n"+
		"read rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n"+
		"write rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n",
		s.clientCreateRate, s.serverCreateRate, s.mw.CreateLimiter().Limit(),
		s.clientDeleteRate, s.serverDeleteRate, s.mw.DeleteLimiter().Limit(),
		s.clientReadRate, s.serverReadRate, s.ec.ReadLimiter().Limit(),
		s.clientWriteRate, s.serverWriteRate, s.ec.WriteLimiter().Limit())))
}

func (s *Super) SetClientRateValue(createRate, deleteRate, readRate, writeRate float64) {
	s.clientCreateRate = createRate
	s.clientDeleteRate = deleteRate
	s.clientReadRate = readRate
	s.clientWriteRate = writeRate
}

func (s *Super) SetClientRate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if rate := r.FormValue("create"); rate != "" {
		val, err := strconv.ParseFloat(rate, 64)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("Set create rate failed, err:%v\n", err)))
		} else {
			s.clientCreateRate = val
			s.mw.SetCreateRate(s.clientCreateRate, s.serverCreateRate)
			w.Write([]byte(fmt.Sprintf("Set create rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n",
				s.clientCreateRate, s.serverCreateRate, s.mw.CreateLimiter().Limit(),)))
		}
	}

	if rate := r.FormValue("delete"); rate != "" {
		val, err := strconv.ParseFloat(rate, 64)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("Set delete rate failed, err:%v\n", err)))
		} else {
			s.clientDeleteRate = val
			s.mw.SetDeleteRate(s.clientDeleteRate, s.serverDeleteRate)
			w.Write([]byte(fmt.Sprintf("Set delete rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n",
				s.clientDeleteRate, s.serverDeleteRate, s.mw.DeleteLimiter().Limit())))
		}
	}

	if rate := r.FormValue("read"); rate != "" {
		val, err := strconv.ParseFloat(rate, 64)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("Set read rate failed, err:%v\n", err)))
		} else {
			s.clientReadRate = val
			s.ec.SetReadRate(s.clientReadRate, s.serverReadRate)
			w.Write([]byte(fmt.Sprintf("Set read rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n",
				s.clientReadRate, s.serverReadRate, s.ec.ReadLimiter().Limit())))
		}
	}

	if rate := r.FormValue("write"); rate != "" {
		val, err := strconv.ParseFloat(rate, 64)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("Set write rate failed, err:%v\n", err)))
		} else {
			s.clientWriteRate = val
			s.ec.SetWriteRate(s.clientWriteRate, s.serverWriteRate)
			w.Write([]byte(fmt.Sprintf("Set write rate:\tclient(%v)\tserver(%v)\t==>\t[%v]\n",
				s.clientWriteRate, s.serverWriteRate, s.ec.WriteLimiter().Limit())))
		}
	}
}

func (s *Super) exporterKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) handleError(op, msg string) {
	log.LogError(msg)
	ump.Alarm(s.umpKey(op), msg)
}

func (s *Super) updateLimiter() {
	for {
		var (
			view *proto.SimpleVolView
			err  error
		)
		if view, err = s.mc.AdminAPI().GetVolumeSimpleInfo(s.volname); err != nil {
			log.LogWarnf("getSimpleVolView: get volume simple info fail: volume(%v) err(%v)", s.volname, err)
			return
		}

		s.serverCreateRate = view.CreateRate
		s.serverDeleteRate = view.DeleteRate
		s.serverReadRate = view.ReadRate
		s.serverWriteRate = view.WriteRate

		s.mw.SetCreateRate(s.clientCreateRate, s.serverCreateRate)
		s.mw.SetDeleteRate(s.clientDeleteRate, s.serverDeleteRate)
		s.ec.SetReadRate(s.clientReadRate, s.serverReadRate)
		s.ec.SetWriteRate(s.clientWriteRate, s.serverWriteRate)

		time.Sleep(1000 * time.Millisecond)
	}

}
