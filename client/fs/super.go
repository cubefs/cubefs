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
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"golang.org/x/net/context"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/client/common"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ump"
)

// Super defines the struct of a super block.
type Super struct {
	cluster     string
	volname     string
	masters     string
	mountPoint  string
	subDir      string
	owner       string
	ic          *InodeCache
	dc          *Dcache
	mw          *meta.MetaWrapper
	ec          *stream.ExtentClient
	orphan      *OrphanInodeList
	enSyncWrite bool
	keepCache   bool

	nodeCache map[uint64]fs.Node
	fslock    sync.Mutex

	disableDcache bool
	fsyncOnClose  bool
	enableXattr   bool
	rootIno       uint64

	state     fs.FSStatType
	sockaddr  string
	suspendCh chan interface{}

	//data lake
	volType             int
	ebsEndpoint         string
	CacheAction         int
	CacheThreshold      int
	EbsBlockSize        int
	enableBcache        bool
	bcacheDir           string
	bcacheFilterFiles   string
	bcacheCheckInterval int64
	bcacheBatchCnt      int64

	readThreads  int
	writeThreads int
	bc           *bcache.BcacheClient
	ebsc         *blobstore.BlobStoreClient
	sc           *SummaryCache

	taskPool []common.TaskPool
	closeC   chan struct{}
}

// Functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

const BlobWriterIdleTimeoutPeriod = 10
const DefaultTaskPoolSize = 30

// NewSuper returns a new Super.
func NewSuper(opt *proto.MountOptions) (s *Super, err error) {

	s = new(Super)
	var masters = strings.Split(opt.Master, meta.HostsSeparator)
	var metaConfig = &meta.MetaConfig{
		Volume:          opt.Volname,
		Owner:           opt.Owner,
		Masters:         masters,
		Authenticate:    opt.Authenticate,
		TicketMess:      opt.TicketMess,
		ValidateOwner:   opt.Authenticate || opt.AccessKey == "",
		EnableSummary:   opt.EnableSummary && opt.EnableXattr,
		MetaSendTimeout: opt.MetaSendTimeout,
	}
	s.mw, err = meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewMetaWrapper failed!"+err.Error())
	}

	s.volname = opt.Volname
	s.masters = opt.Master
	s.mountPoint = opt.MountPoint
	s.subDir = opt.SubDir
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
	if opt.MaxStreamerLimit > 0 {
		s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
		s.dc = NewDcache(inodeExpiration, MaxInodeCache)
	} else {
		s.ic = NewInodeCache(inodeExpiration, DefaultMaxInodeCache)
		s.dc = NewDcache(inodeExpiration, DefaultMaxInodeCache)
	}
	s.orphan = NewOrphanInodeList()
	s.nodeCache = make(map[uint64]fs.Node)
	s.disableDcache = opt.DisableDcache
	s.fsyncOnClose = opt.FsyncOnClose
	s.enableXattr = opt.EnableXattr
	s.bcacheCheckInterval = opt.BcacheCheckIntervalS
	s.bcacheFilterFiles = opt.BcacheFilterFiles
	s.bcacheBatchCnt = opt.BcacheBatchCnt
	s.closeC = make(chan struct{}, 1)
	s.taskPool = []common.TaskPool{common.New(DefaultTaskPoolSize, DefaultTaskPoolSize), common.New(DefaultTaskPoolSize, DefaultTaskPoolSize)}

	if s.mw.EnableSummary {
		s.sc = NewSummaryCache(DefaultSummaryExpiration, MaxSummaryCache)
	}

	if opt.MaxStreamerLimit > 0 {
		DisableMetaCache = false
		s.fsyncOnClose = false
	}

	if !strings.HasSuffix(opt.MountPoint, "/") {
		opt.MountPoint = opt.MountPoint + "/"
	}
	if !strings.HasSuffix(opt.SubDir, "/") {
		opt.SubDir = opt.SubDir + "/"
	}
	if opt.BcacheDir != "" && !strings.HasSuffix(opt.BcacheDir, "/") {
		opt.BcacheDir = opt.BcacheDir + "/"
	}

	// use block cache and default use mountPoint as bcache dir
	if opt.EnableBcache && opt.BcacheDir == "" {
		s.bcacheDir = opt.MountPoint
	}

	if s.bcacheDir == opt.MountPoint {
		s.bcacheDir = "/"
	} else {
		s.bcacheDir = strings.ReplaceAll(opt.BcacheDir, opt.MountPoint, "/")
		if s.bcacheDir != "" && !strings.HasSuffix(s.bcacheDir, "/") {
			s.bcacheDir = s.bcacheDir + "/"
		}
	}

	s.volType = opt.VolType
	s.ebsEndpoint = opt.EbsEndpoint
	s.CacheAction = opt.CacheAction
	s.CacheThreshold = opt.CacheThreshold
	s.EbsBlockSize = opt.EbsBlockSize
	s.enableBcache = opt.EnableBcache
	s.readThreads = int(opt.ReadThreads)
	s.writeThreads = int(opt.WriteThreads)

	if s.enableBcache {
		s.bc = bcache.NewBcacheClient()
	}

	var extentConfig = &stream.ExtentConfig{
		Volume:            opt.Volname,
		Masters:           masters,
		FollowerRead:      opt.FollowerRead,
		NearRead:          opt.NearRead,
		ReadRate:          opt.ReadRate,
		WriteRate:         opt.WriteRate,
		VolumeType:        opt.VolType,
		BcacheEnable:      opt.EnableBcache,
		BcacheDir:         opt.BcacheDir,
		MaxStreamerLimit:  opt.MaxStreamerLimit,
		OnAppendExtentKey: s.mw.AppendExtentKey,
		OnGetExtents:      s.mw.GetExtents,
		OnTruncate:        s.mw.Truncate,
		OnEvictIcache:     s.ic.Delete,
		OnLoadBcache:      s.bc.Get,
		OnCacheBcache:     s.bc.Put,
		OnEvictBcache:     s.bc.Evict,

		DisableMetaCache: DisableMetaCache,
	}

	s.ec, err = stream.NewExtentClient(extentConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewExtentClient failed!")
	}
	if proto.IsCold(opt.VolType) {
		s.ebsc, err = blobstore.NewEbsClient(access.Config{
			ConnMode: access.NoLimitConnMode,
			Consul: api.Config{
				Address: opt.EbsEndpoint,
			},
			MaxSizePutOnce: MaxSizePutOnce,
			Logger: &access.Logger{
				Filename: path.Join(opt.Logpath, "client/ebs.log"),
			},
		})
		if err != nil {
			return nil, errors.Trace(err, "NewEbsClient failed!")
		}
	}

	if !opt.EnablePosixACL {
		opt.EnablePosixACL = s.ec.GetEnablePosixAcl()
	}

	if s.rootIno, err = s.mw.GetRootIno(opt.SubDir); err != nil {
		return nil, err
	}

	s.suspendCh = make(chan interface{})
	if proto.IsCold(opt.VolType) {
		go s.scheduleFlush()
	}
	if s.mw.EnableSummary {

		s.sc = NewSummaryCache(DefaultSummaryExpiration, MaxSummaryCache)
	}

	if opt.NeedRestoreFuse {
		atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatRestore))
	}

	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v) state(%v)",
		s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration, s.state)

	go s.loopSyncMeta()

	return s, nil
}

func (s *Super) scheduleFlush() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			ctx := context.Background()
			s.fslock.Lock()
			for ino, node := range s.nodeCache {
				if _, ok := node.(*File); !ok {
					continue
				}
				file := node.(*File)
				if atomic.LoadInt32(&file.idle) >= BlobWriterIdleTimeoutPeriod {
					if file.fWriter != nil {
						atomic.StoreInt32(&file.idle, 0)
						go file.fWriter.Flush(ino, ctx)
					}
				} else {
					atomic.AddInt32(&file.idle, 1)
				}
			}
			s.fslock.Unlock()
		}
	}
}

// Root returns the root directory where it resides.
func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(s.rootIno)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode, inode.Inode, "")
	return root, nil
}

func (s *Super) Node(ino, pino uint64, mode uint32) (fs.Node, error) {
	var node fs.Node

	// Create a fake InodeInfo. All File or Dir operations only use
	// InodeInfo.Inode.
	fakeInfo := &proto.InodeInfo{Inode: ino, Mode: mode}
	if proto.OsMode(fakeInfo.Mode).IsDir() {
		node = NewDir(s, fakeInfo, pino, "")
	} else {
		node = NewFile(s, fakeInfo, DefaultFlag, pino, "")
		// The node is saved in FuseContextNodes list, that means
		// the node is not evict. So we create a streamer for it,
		// and streamer's refcnt is 0.
		file := node.(*File)
		file.Open(nil, nil, nil)
		file.Release(nil, nil)
	}
	s.fslock.Lock()
	s.nodeCache[ino] = node
	s.fslock.Unlock()
	return node, nil
}

// Statfs handles the Statfs request and returns a set of statistics.
func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	const defaultMaxMetaPartitionInodeID uint64 = 1<<63 - 1
	total, used, inodeCount := s.mw.Statfs()
	resp.Blocks = total / uint64(DefaultBlksize)
	resp.Bfree = (total - used) / uint64(DefaultBlksize)
	resp.Bavail = resp.Bfree
	resp.Bsize = DefaultBlksize
	resp.Namelen = DefaultMaxNameLen
	resp.Frsize = DefaultBlksize
	resp.Files = inodeCount
	resp.Ffree = defaultMaxMetaPartitionInodeID - inodeCount
	return nil
}

// ClusterName returns the cluster name.
func (s *Super) ClusterName() string {
	return s.cluster
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
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) handleError(op, msg string) {
	log.LogError(msg)
	ump.Alarm(s.umpKey(op), msg)
}

func replyFail(w http.ResponseWriter, r *http.Request, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(msg))
}

func replySucc(w http.ResponseWriter, r *http.Request, msg string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(msg))
}

func (s *Super) SetSockAddr(addr string) {
	s.sockaddr = addr
}

func (s *Super) SetSuspend(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ret string
	)

	if err = r.ParseForm(); err != nil {
		replyFail(w, r, err.Error())
		return
	}

	sockaddr := r.FormValue("sock")
	if sockaddr == "" {
		err = fmt.Errorf("NeedAfterAlloc parameter 'sock' for IPC")
		replyFail(w, r, err.Error())
		return
	}

	s.fslock.Lock()
	if s.sockaddr != "" ||
		!atomic.CompareAndSwapUint32((*uint32)(&s.state), uint32(fs.FSStatResume), uint32(fs.FSStatSuspend)) {
		s.fslock.Unlock()
		err = fmt.Errorf("Already in suspend: sock '%s', state %v", s.sockaddr, s.state)
		replyFail(w, r, err.Error())
		return
	}
	s.sockaddr = sockaddr
	s.fslock.Unlock()

	// wait
	msg := <-s.suspendCh
	switch msg.(type) {
	case error:
		err = msg.(error)
	case string:
		ret = msg.(string)
	default:
		err = fmt.Errorf("Unknown return type: %v", msg)
	}

	if err != nil {
		s.fslock.Lock()
		atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatResume))
		s.sockaddr = ""
		s.fslock.Unlock()
		replyFail(w, r, err.Error())
		return
	}

	if !atomic.CompareAndSwapUint32((*uint32)(&s.state), uint32(fs.FSStatSuspend), uint32(fs.FSStatShutdown)) {
		s.fslock.Lock()
		atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatResume))
		s.sockaddr = ""
		s.fslock.Unlock()
		err = fmt.Errorf("Invalid old state %v", s.state)
		replyFail(w, r, err.Error())
		return
	}

	replySucc(w, r, fmt.Sprintf("set suspend successfully: %s", ret))
}

func (s *Super) SetResume(w http.ResponseWriter, r *http.Request) {
	s.fslock.Lock()
	atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatResume))
	s.sockaddr = ""
	s.fslock.Unlock()
	replySucc(w, r, "set resume successfully")
}

func (s *Super) EnableAuditLog(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	if err = r.ParseForm(); err != nil {
		auditlog.BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	logPath := r.FormValue("path")
	if logPath == "" {
		err = fmt.Errorf("path cannot be empty")
		auditlog.BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	prefix := r.FormValue("prefix")
	if prefix == "" {
		err = fmt.Errorf("prefix cannot be empty")
		auditlog.BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	logSize := 0

	if logSizeStr := r.FormValue("logsize"); logSizeStr != "" {
		val, err := strconv.Atoi(logSizeStr)
		if err != nil {
			err = fmt.Errorf("logSize error")
			auditlog.BuildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		logSize = val
	} else {
		logSize = auditlog.DefaultAuditLogSize
	}

	err, dir, logModule, logMaxSize := auditlog.GetAuditLogInfo()
	if err != nil {

		_, err = auditlog.InitAuditWithPrefix(logPath, prefix, int64(auditlog.DefaultAuditLogSize),
			auditlog.NewAuditPrefix(s.masters, s.volname, s.subDir, s.mountPoint))
		if err != nil {
			err = errors.NewErrorf("Init audit log fail: %v\n", err)
			auditlog.BuildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}

		info := fmt.Sprintf("audit log is initialized with params: logDir(%v) logModule(%v) logMaxSize(%v)",
			logPath, prefix, logSize)
		auditlog.BuildSuccessResp(w, info)
	} else {
		info := fmt.Sprintf("audit log is already initialized with params: logDir(%v) logModule(%v) logMaxSize(%v)",
			dir, logModule, logMaxSize)
		auditlog.BuildSuccessResp(w, info)
	}

}

func (s *Super) State() (state fs.FSStatType, sockaddr string) {
	return fs.FSStatType(atomic.LoadUint32((*uint32)(&s.state))), s.sockaddr
}

func (s *Super) Notify(stat fs.FSStatType, msg interface{}) {
	if stat == fs.FSStatSuspend {
		s.suspendCh <- msg
	} else if stat == fs.FSStatRestore {
		s.fslock.Lock()
		atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatResume))
		s.sockaddr = ""
		s.fslock.Unlock()
	}
}

func (s *Super) loopSyncMeta() {
	if s.bcacheDir == "" {
		return
	}
	for {
		finishC := s.syncMeta()
		select {
		case <-finishC:
			time.Sleep(time.Second * time.Duration(s.bcacheCheckInterval))
		case <-s.closeC:
			return
		}
	}
}

func (s *Super) syncMeta() <-chan struct{} {

	finishC := make(chan struct{})
	start := time.Now()
	cacheLen := s.ic.lruList.Len()

	allInodes := func() <-chan uint64 {
		out := make(chan uint64)
		go func() {
			for i := s.ic.lruList.Front(); i != nil; i = i.Next() {
				oldInfo := i.Value.(*proto.InodeInfo)
				out <- oldInfo.Inode
			}
			close(out)
		}()
		return out
	}()

	getChanged := func(in <-chan uint64, batchCnt int64) <-chan uint64 {
		out := make(chan uint64)
		changed := make([]uint64, 0)
		s.taskPool[0].Run(func() {
			tmpInodes := make([]uint64, 0, batchCnt)
			for i := range in {
				tmpInodes = append(tmpInodes, i)
				if len(tmpInodes) == int(batchCnt) {
					changed = append(changed, s.getModifyInodes(tmpInodes)...)
					tmpInodes = tmpInodes[:0]
				}
			}
			if len(tmpInodes) != 0 {
				changed = append(changed, s.getModifyInodes(tmpInodes)...)
			}
			for i := range changed {
				out <- changed[i]
			}
			close(out)
		})
		return out
	}

	batCh := make([]<-chan uint64, DefaultTaskPoolSize/3)
	for i := range batCh {
		batCh[i] = getChanged(allInodes, s.bcacheBatchCnt)
	}

	mergeChanged := func(cs []<-chan uint64) <-chan uint64 {
		var wg sync.WaitGroup
		out := make(chan uint64)
		wg.Add(len(cs))
		for _, c := range cs {
			go func(c <-chan uint64) {
				for n := range c {
					out <- n
				}
				wg.Done()
			}(c)
		}
		go func() {
			wg.Wait()
			close(out)
		}()
		return out
	}

	var changeCnt int

	for ino := range mergeChanged(batCh) {
		inode := ino
		changeCnt++
		log.LogDebugf("sync meta,inode:%d changed", inode)
		s.ic.Delete(inode)
		s.taskPool[1].Run(func() {
			common.Timed(3, 100).On(func() error {
				extents := s.ec.GetExtents(inode)
				if err := s.ec.ForceRefreshExtentsCache(inode); err != nil {
					if err != os.ErrNotExist {
						log.LogErrorf("ForceRefreshExtentsCache failed:%v", err)
					}
				}
				log.LogDebugf("inode:%d,extents is :%v", inode, extents)
				for _, extent := range extents {
					cacheKey := util.GenerateRepVolKey(s.volname, inode, extent.PartitionId, extent.ExtentId, extent.FileOffset)
					// retry to make possible evict success
					if s.bc != nil {
						common.Timed(3, 100).On(func() error {
							return s.bc.Evict(cacheKey)
						})
					}

				}
				return nil
			})
		})
	}

	log.LogDebugf("total cache cnt:%d,changedCnt:%d,sync meta cost:%v", cacheLen, changeCnt, time.Since(start))
	close(finishC)

	return finishC
}

func (s *Super) getModifyInodes(inodes []uint64) (changedNodes []uint64) {
	inodeInfos := s.mw.BatchInodeGet(inodes)

	// get deleted files
	if len(inodeInfos) != len(inodes) {
		changedNodes = append(changedNodes, getDelInodes(inodes, inodeInfos)...)
		log.LogDebugf("len inodes is %d, len get inode infos is :%d, del inodes is:%v", len(inodes), len(inodeInfos), changedNodes)
	}

	for _, newInfo := range inodeInfos {
		oldInfo := s.ic.Get(newInfo.Inode)
		if oldInfo == nil {
			continue
		}
		if !oldInfo.ModifyTime.Equal(newInfo.ModifyTime) || newInfo.Generation != s.ec.GetExtentCacheGen(newInfo.Inode) {
			log.LogDebugf("oldInfo:ino(%d) modifyTime(%v) gen(%d),newInfo:ino(%d) modifyTime(%d) gen(%d)", oldInfo.Inode, oldInfo.ModifyTime.Unix(), s.ec.GetExtentCacheGen(newInfo.Inode), newInfo.Inode, newInfo.ModifyTime.Unix(), newInfo.Generation)
			changedNodes = append(changedNodes, newInfo.Inode)
		} else {
			log.LogDebugf("oldInfo:ino(%d) modifyTime(%v) gen(%d),newInfo:ino(%d) modifyTime(%d) gen(%d)", oldInfo.Inode, oldInfo.ModifyTime.Unix(), s.ec.GetExtentCacheGen(newInfo.Inode), newInfo.Inode, newInfo.ModifyTime.Unix(), newInfo.Generation)
		}
	}
	return
}

func getDelInodes(src []uint64, act []*proto.InodeInfo) []uint64 {
	delInodes := make([]uint64, 0)
	m := make(map[uint64]struct{})
	for _, iInfo := range act {
		m[iInfo.Inode] = struct{}{}
	}
	for _, inode := range src {
		if _, ok := m[inode]; !ok {
			delInodes = append(delInodes, inode)
		}
	}
	return delInodes
}

func (s *Super) Close() {
	close(s.closeC)
}
