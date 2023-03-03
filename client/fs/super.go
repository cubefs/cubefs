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
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/net/context"
)

// Super defines the struct of a super block.
type Super struct {
	cluster     string
	volname     string
	owner       string
	umpJmtpAddr string
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
	readDirPlus 		 	 int64

	delProcessPath []string
	wg             sync.WaitGroup
	stopC          chan struct{}

	volumeLabelValue exporter.LabelValue
}

type SuperState struct {
	RootIno 	uint64
	ReadDirPlus	int64
}

// Functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

// NewSuper returns a new Super.
func NewSuper(opt *proto.MountOptions, first_start bool, metaState *meta.MetaState,
	dataState *data.DataState, superState *SuperState) (s *Super, err error) {

	s = new(Super)
	var masters = strings.Split(opt.Master, meta.HostsSeparator)
	var metaConfig = &meta.MetaConfig{
		Volume:        opt.Volname,
		Owner:         opt.Owner,
		Masters:       masters,
		Authenticate:  opt.Authenticate,
		TicketMess:    opt.TicketMess,
		ValidateOwner: true,
	}

	if first_start {
		s.mw, err = meta.NewMetaWrapper(metaConfig)
		if err != nil {
			return nil, errors.Trace(err, "NewMetaWrapper failed!")
		}
	} else {
		s.mw = meta.RebuildMetaWrapper(metaConfig, metaState)
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
	if first_start {
		s.ec, err = data.NewExtentClient(extentConfig, nil)
		if err != nil {
			return nil, errors.Trace(err, "NewExtentClient failed!")
		}
	} else {
		s.ec = data.RebuildExtentClient(extentConfig, dataState)
	}

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

	if first_start {
		if s.rootIno, err = s.mw.GetRootIno(opt.SubDir, opt.AutoMakeSubDir); err != nil {
			return nil, err
		}
		s.readDirPlus = opt.EnableReadDirPlus
	} else {
		s.rootIno = superState.RootIno
		s.readDirPlus = superState.ReadDirPlus
	}

	s.volumeLabelValue = exporter.LabelValue{
		Label: "volume",
		Value: s.volname,
	}
	s.stopC = make(chan struct{})
	s.wg.Add(1)
	go s.scheduler()

	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

func (s *Super) scheduler() {
	defer s.wg.Done()
	var (
		volUsageCollectTimer = time.NewTimer(0)
		lvs                  = []exporter.LabelValue{
			{Label: "cluster", Value: s.cluster},
			{Label: "volume", Value: s.volname},
		}
		volUsageUsed  = exporter.NewGauge("volume_usage_used", lvs...)
		volUsageTotal = exporter.NewGauge("volume_usage_total", lvs...)
	)
	for {
		select {
		case <-s.stopC:
			return
		case <-volUsageCollectTimer.C:
			var total, used = s.mw.Statfs()
			if total > 0 {
				volUsageUsed.Set(float64(used))
				volUsageTotal.Set(float64(total))
			}
			volUsageCollectTimer.Reset(time.Minute)
		}

	}
}

func (s *Super) MetaWrapper() *meta.MetaWrapper {
	return s.mw
}

func (s *Super) ExtentClient() *data.ExtentClient {
	return s.ec
}

func (s *Super) SaveSuperState() *SuperState {
	return &SuperState{s.rootIno, s.readDirPlus}
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

func (s *Super) Node(ino uint64, mode uint32) fs.Node {
	var node fs.Node

	// Create a fake InodeInfo. All File or Dir operations only use
	// InodeInfo.Inode.
	fakeInfo := &proto.InodeInfo{Inode: ino, Mode: mode}
	if proto.OsMode(fakeInfo.Mode).IsDir() {
		node = NewDir(s, fakeInfo)
	} else {
		node = NewFile(s, fakeInfo)
	}
	return node
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

func (s *Super) EnableWriteCache() bool {
	return s.ec.EnableWriteCache()
}

func (s *Super) SetEnableWriteCache(writeCache bool) {
	s.ec.SetEnableWriteCache(writeCache)
}

func (s *Super) exporterKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) exporterVolumeLabelValue() exporter.LabelValue {
	return s.volumeLabelValue
}

func (s *Super) umpKey() string {
	return fmt.Sprintf("%v_client_warning", s.cluster)
}

func (s *Super) umpFunctionKey(act string) string {
	return fmt.Sprintf("%s_%s_%s", s.cluster, s.volname, act)
}

func (s *Super) umpAlarmKey() string {
	return fmt.Sprintf("%s_%s_warning", s.cluster, s.volname)
}

func (s *Super) handleError(op, msg string) {
	log.LogError(msg)

	errmsg1 := fmt.Sprintf("act(%v) - %v", op, msg)
	exporter.WarningBySpecialUMPKey(s.umpAlarmKey(), errmsg1)

	errmsg2 := fmt.Sprintf("volume(%v) %v", s.volname, errmsg1)
	exporter.WarningBySpecialUMPKey(s.umpKey(), errmsg2)
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
			exporter.WarningBySpecialUMPKey(s.umpAlarmKey(), errmsg1)

			errmsg2 := fmt.Sprintf("volume(%v) %v", s.volname, errmsg1)
			exporter.WarningBySpecialUMPKey(s.umpKey(), errmsg2)
		}
	}()
}

func (s *Super) Close() {
	close(s.stopC)
	if s.ec != nil {
		_ = s.ec.Close(context.Background())
		s.ec.CloseConnPool()
	}
	if s.mw != nil {
		_ = s.mw.Close()
	}
	s.ic.Stop()
	s.wg.Wait()
}

func (s *Super) SupportJdosKernelWriteBack() bool {
	if _, err := os.Stat(JdosKernelWriteBackControlFile); err != nil {
		log.LogWarnf("SupportJdosKernelWriteBack: stat kernel control file(%v) err(%v)", JdosKernelWriteBackControlFile, err)
		return false
	}
	log.LogInfof("SupportJdosKernelWriteBack: stat kernel control file(%v) ok, enable options -cgwb", JdosKernelWriteBackControlFile)
	return true
}

func (s *Super) EnableJdosKernelWriteBack(enable bool) (err error) {
	var (
		file *os.File
	)
	if file, err = os.OpenFile(JdosKernelWriteBackControlFile, os.O_RDWR, 0644); err != nil {
		if os.IsNotExist(err) {
			log.LogInfof("EnableJdosKernelWriteBack: kernel control file(%v) is not exist", JdosKernelWriteBackControlFile)
			return nil
		}
		return
	}
	defer func() {
		errClose := file.Close()
		if err == nil {
			err = errClose
		}
	}()
	var enableWriteBack string
	if enable {
		enableWriteBack = "1"
	} else {
		enableWriteBack = "0"
	}
	if _, err = file.WriteAt([]byte(enableWriteBack), 0); err != nil {
		return
	}
	return
}

func (s *Super) UmpJmtpAddr() string {
	return s.ec.UmpJmtpAddr()
}

func (s *Super) EnableReadDirPlus() bool {
	return s.readDirPlus > 0
}