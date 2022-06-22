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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chubaofs/chubaofs/client/cache"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
	"golang.org/x/net/context"
)

// Super defines the struct of a super block.
type Super struct {
	cluster     string
	modulename  string
	volname     string
	owner       string
	ic          *cache.InodeCache
	mw          *meta.MetaWrapper
	ec          *data.ExtentClient
	orphan      *OrphanInodeList
	enSyncWrite bool
	keepCache   bool

	//nodeCache map[uint64]fs.Node
	//fslock    sync.Mutex

	disableDcache            bool
	fsyncOnClose             bool
	enableXattr              bool
	noBatchGetInodeOnReaddir bool
	rootIno                  uint64

	delProcessPath []string
	wg             sync.WaitGroup
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
		Modulename:    opt.Modulename,
		Volume:        opt.Volname,
		Owner:         opt.Owner,
		Masters:       masters,
		Authenticate:  opt.Authenticate,
		TicketMess:    opt.TicketMess,
		ValidateOwner: true,
	}
	s.mw, err = meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewMetaWrapper failed!")
	}
	inodeExpiration := DefaultInodeExpiration
	if opt.IcacheTimeout >= 0 {
		inodeExpiration = time.Duration(opt.IcacheTimeout) * time.Second
	}
	s.ic = cache.NewInodeCache(inodeExpiration, MaxInodeCache, cache.BgEvictionInterval, true)
	var extentConfig = &data.ExtentConfig{
		Volume:                   opt.Volname,
		Masters:                  masters,
		FollowerRead:             opt.FollowerRead,
		NearRead:                 opt.NearRead,
		ReadRate:                 opt.ReadRate,
		WriteRate:                opt.WriteRate,
		AlignSize:                opt.AlignSize,
		MaxExtentNumPerAlignArea: opt.MaxExtentNumPerAlignArea,
		ForceAlignMerge:          opt.ForceAlignMerge,
		ExtentSize:               int(opt.ExtentSize),
		AutoFlush:                opt.AutoFlush,
		OnInsertExtentKey:        s.mw.InsertExtentKey,
		OnGetExtents:             s.mw.GetExtents,
		OnTruncate:               s.mw.Truncate,
		OnEvictIcache:            s.ic.Delete,
	}
	s.ec, err = data.NewExtentClient(extentConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewExtentClient failed!")
	}
	s.modulename = opt.Modulename
	s.volname = opt.Volname
	s.owner = opt.Owner
	s.cluster = s.mw.Cluster()

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
	s.orphan = NewOrphanInodeList()
	s.disableDcache = opt.DisableDcache
	s.fsyncOnClose = opt.FsyncOnClose
	s.enableXattr = opt.EnableXattr
	s.noBatchGetInodeOnReaddir = opt.NoBatchGetInodeOnReaddir
	if opt.DelProcessPath != "" {
		s.delProcessPath = strings.Split(opt.DelProcessPath, ",")
	}

	if s.rootIno, err = s.mw.GetRootIno(opt.SubDir, opt.AutoMakeSubDir); err != nil {
		return nil, err
	}

	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

// Root returns the root directory where it resides.
func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(context.Background(), s.rootIno)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

func (s *Super) Node(ino uint64, mode uint32) (fs.Node, error) {
	var node fs.Node

	// Create a fake InodeInfo. All File or Dir operations only use
	// InodeInfo.Inode.
	fakeInfo := &proto.InodeInfo{Inode: ino, Mode: mode}
	if proto.OsMode(fakeInfo.Mode).IsDir() {
		node = NewDir(s, fakeInfo)
	} else {
		node = NewFile(s, fakeInfo)
	}
	return node, nil
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

func (s *Super) VolName() string {
	return s.volname
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

func (s *Super) GetOpRate(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(s.mw.GetOpLimitRate()))
}

func (s *Super) exporterKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) umpKey() string {
	return fmt.Sprintf("%v_client_warning", s.cluster)
}

func (s *Super) umpFunctionKey(act string) string {
	return fmt.Sprintf("%s_%s_%s", s.cluster, s.volname, act)
}

func (s *Super) umpFunctionGeneralKey(act string) string {
	return fmt.Sprintf("%s_%s_%s", s.cluster, s.modulename, act)
}

func (s *Super) umpAlarmKey() string {
	return fmt.Sprintf("%s_%s_warning", s.cluster, s.volname)
}

func (s *Super) handleError(op, msg string) {
	log.LogError(msg)

	errmsg1 := fmt.Sprintf("act(%v) - %v", op, msg)
	ump.Alarm(s.umpAlarmKey(), errmsg1)

	errmsg2 := fmt.Sprintf("volume(%v) %v", s.volname, errmsg1)
	ump.Alarm(s.umpKey(), errmsg2)
}

func (s *Super) handleErrorWithGetInode(op, msg string, inode uint64) {
	log.LogError(msg)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// if failed to get inode, judge err and alarm;
		// if succeed to get inode, alarm msg err;
		// if inode not exists, not alarm
		if _, err := s.mw.InodeGet_ll(context.Background(), inode); err != syscall.ENOENT {
			errmsg1 := fmt.Sprintf("act(%v) - %v", op, msg)
			ump.Alarm(s.umpAlarmKey(), errmsg1)

			errmsg2 := fmt.Sprintf("volume(%v) %v", s.volname, errmsg1)
			ump.Alarm(s.umpKey(), errmsg2)
		}
	}()
}

func (s *Super) Close() {
	if s.ec != nil {
		_ = s.ec.Close(context.Background())
	}
	if s.mw != nil {
		_ = s.mw.Close()
	}
	s.ic.Stop()
	s.wg.Wait()
}
