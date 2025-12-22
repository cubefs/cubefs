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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/client/blockcache/bcache"
	"github.com/cubefs/cubefs/client/common"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/cubefs/cubefs/util/ump"
	"github.com/google/uuid"
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

	// data lake
	volType             int
	ebsEndpoint         string
	EbsBlockSize        int
	enableBcache        bool
	bcacheDir           string
	bcacheFilterFiles   string
	bcacheCheckInterval int64
	bcacheBatchCnt      int64
	runningMonitor      *RunningMonitor
	syncMetaCache       int32

	readThreads  int
	writeThreads int
	bc           *bcache.BcacheClient
	ebsc         *blobstore.BlobStoreClient

	taskPool []common.TaskPool
	closeC   chan struct{}

	// warm up configurable parameters
	readDirLimit          int64
	maxWarmUpConcurrency  int64
	stopWarmMeta          bool
	metaCacheAcceleration bool
	minimumNlinkReadDir   int64
	inodeLruLimit         int64
}

// Functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

const (
	BlobWriterIdleTimeoutPeriod = 10
	DefaultTaskPoolSize         = 30
	WarmUpCheckInterval         = 30 * time.Second
	RemoteMetaCacheDuration     = 48 * time.Hour
)

// NewSuper returns a new Super.
func NewSuper(opt *proto.MountOptions) (s *Super, err error) {
	s = new(Super)
	masters := strings.Split(opt.Master, meta.HostsSeparator)
	metaConfig := &meta.MetaConfig{
		Volume:          opt.Volname,
		Owner:           opt.Owner,
		Masters:         masters,
		Authenticate:    opt.Authenticate,
		TicketMess:      opt.TicketMess,
		ValidateOwner:   opt.Authenticate || opt.AccessKey == "",
		MetaSendTimeout: opt.MetaSendTimeout,
		// EnableTransaction: opt.EnableTransaction,
		SubDir:                     opt.SubDir,
		TrashRebuildGoroutineLimit: int(opt.TrashRebuildGoroutineLimit),
		TrashTraverseLimit:         int(opt.TrashDeleteExpiredDirGoroutineLimit),
	}
	s.mw, err = meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewMetaWrapper failed!"+err.Error())
	}

	s.SetTransaction(opt.EnableTransaction, opt.TxTimeout, opt.TxConflictRetryNum, opt.TxConflictRetryInterval)
	s.mw.EnableQuota = opt.EnableQuota

	s.volname = opt.Volname
	s.masters = opt.Master
	s.mountPoint = opt.MountPoint
	s.subDir = opt.SubDir
	s.owner = opt.Owner
	s.cluster = s.mw.Cluster()
	inodeExpiration := DefaultInodeExpiration
	if opt.MetaCacheAcceleration {
		inodeExpiration = DefaultMetaInodeExpiration
	}
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
	s.ic = NewInodeCache(inodeExpiration, int(opt.InodeLruLimit), s.metaCacheAcceleration)
	s.inodeLruLimit = opt.InodeLruLimit
	if opt.MaxStreamerLimit > 0 || !opt.StopWarmMeta {
		s.dc = NewDcache(inodeExpiration, MaxInodeCache)
	} else {
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
	s.runningMonitor = NewRunningMonitor(opt.ClientOpTimeOut)
	s.runningMonitor.Start()

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
	s.EbsBlockSize = opt.EbsBlockSize
	s.enableBcache = opt.EnableBcache

	s.readThreads = int(opt.ReadThreads)
	s.writeThreads = int(opt.WriteThreads)

	if s.enableBcache {
		s.bc = bcache.NewBcacheClient()
	}

	// Initialize warm up configurable parameters
	s.readDirLimit = opt.ReadDirLimit
	s.maxWarmUpConcurrency = opt.MaxWarmUpConcurrency
	s.stopWarmMeta = opt.StopWarmMeta
	s.metaCacheAcceleration = opt.MetaCacheAcceleration
	s.minimumNlinkReadDir = opt.MinimumNlinkReadDir

	extentConfig := &stream.ExtentConfig{
		Volume:            opt.Volname,
		Masters:           masters,
		FollowerRead:      opt.FollowerRead,
		NearRead:          opt.NearRead,
		MaximallyRead:     opt.MaximallyRead,
		ReadRate:          opt.ReadRate,
		WriteRate:         opt.WriteRate,
		BcacheEnable:      opt.EnableBcache,
		BcacheDir:         opt.BcacheDir,
		MaxStreamerLimit:  opt.MaxStreamerLimit,
		VerReadSeq:        opt.VerReadSeq,
		MetaWrapper:       s.mw,
		OnAppendExtentKey: s.mw.AppendExtentKey,
		OnSplitExtentKey:  s.mw.SplitExtentKey,
		OnGetExtents:      s.mw.GetExtents,
		OnTruncate:        s.mw.Truncate,
		OnEvictIcache:     s.ic.Delete,
		OnLoadBcache:      s.bc.Get,
		OnCacheBcache:     s.bc.Put,
		OnEvictBcache:     s.bc.Evict,

		DisableMetaCache:            DisableMetaCache,
		StreamRetryTimeout:          opt.StreamRetryTimeout,
		OnRenewalForbiddenMigration: s.mw.RenewalForbiddenMigration,
		VolStorageClass:             opt.VolStorageClass,
		VolAllowedStorageClass:      opt.VolAllowedStorageClass,
		OnForbiddenMigration:        s.mw.ForbiddenMigration,

		OnGetInodeInfo:      s.InodeGet,
		BcacheOnlyForNotSSD: opt.BcacheOnlyForNotSSD,

		AheadReadEnable:       opt.AheadReadEnable,
		AheadReadTotalMem:     opt.AheadReadTotalMem,
		AheadReadBlockTimeOut: opt.AheadReadBlockTimeOut,
		AheadReadWindowCnt:    opt.AheadReadWindowCnt,
		MinReadAheadSize:      int(opt.MinReadAheadSize),
		NeedRemoteCache:       true,
		ForceRemoteCache:      opt.ForceRemoteCache,
		EnableAsyncFlush:      opt.EnableAsyncFlush,
		MetaAcceleration:      opt.MetaCacheAcceleration,
	}

	log.LogInfof("ahead info enable %+v, totalMem %+v, timeout %+v, winCnt %+v", opt.AheadReadEnable, opt.AheadReadTotalMem, opt.AheadReadBlockTimeOut, opt.AheadReadWindowCnt)

	s.ec, err = stream.NewExtentClient(extentConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewExtentClient failed!")
	}
	s.mw.VerReadSeq = s.ec.GetReadVer()

	needCreateBlobClient := false
	if !proto.IsValidStorageClass(opt.VolStorageClass) {
		// for compatability: old version server modules has no filed VolStorageClas
		if proto.IsCold(opt.VolType) {
			needCreateBlobClient = true
			log.LogInfof("[NewSuper] to create blobstore client for old fashion cold volume")
		}
	} else {
		if proto.IsVolSupportStorageClass(extentConfig.VolAllowedStorageClass, proto.StorageClass_BlobStore) {
			needCreateBlobClient = true
			log.LogInfof("[NewSuper] to create blobstore client for volume allowed blobstore storageClass")
		}
	}
	if needCreateBlobClient {
		s.ebsc, err = blobstore.NewEbsClient(access.Config{
			ConnMode: access.NoLimitConnMode,
			Consul: access.ConsulConfig{
				Address: opt.EbsEndpoint,
			},
			MaxSizePutOnce: MaxSizePutOnce,
			Logger: &access.Logger{
				Filename: path.Join(opt.Logpath, "client/ebs.log"),
			},
			LogLevel: log.GetBlobLogLevel(),
		})
		if err != nil {
			log.LogErrorf("[NewSuper] create blobstore client err: %v", err)
			return nil, errors.Trace(err, "NewEbsClient failed!")
		}
	}

	s.mw.Client = s.ec

	if !opt.EnablePosixACL {
		opt.EnablePosixACL = s.ec.GetEnablePosixAcl()
	}

	if s.rootIno, err = s.mw.GetRootIno(opt.SubDir); err != nil {
		return nil, err
	}

	s.suspendCh = make(chan interface{})
	if proto.IsCold(opt.VolType) || proto.IsVolSupportStorageClass(opt.VolAllowedStorageClass, proto.StorageClass_BlobStore) {
		go s.scheduleFlush()
	}

	if opt.NeedRestoreFuse {
		atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatRestore))
	}

	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v) state(%v)",
		s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration, s.state)
	stat.PrintModuleStat = func(writer *bufio.Writer) {
		fmt.Fprintf(writer, "ic:%d dc:%d nodecache:%d dircache:%d\n", s.ic.lruList.Len(), s.dc.lruList.Len(), len(s.nodeCache), s.mw.DirCacheLen())
	}
	if !s.metaCacheAcceleration {
		go s.loopSyncMeta()
	}

	// Start warm up meta paths goroutine
	go s.loopWarmUpMetaPaths()

	return s, nil
}

func (s *Super) scheduleFlush() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		{
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
		file.Open(context.TODO(), nil, nil)
		file.Release(context.TODO(), nil)
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

func (s *Super) SetStopWarmMeta(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	stop := r.FormValue("stop")
	if stop == "" {
		replyFail(w, r, "Parameter 'stop' is required")
		return
	}

	stopBool, err := strconv.ParseBool(stop)
	if err != nil {
		replyFail(w, r, "Parameter 'stop' must be 'true' or 'false'")
		return
	}
	s.stopWarmMeta = stopBool
	replySucc(w, r, fmt.Sprintf("Set stopWarmMeta to %v successfully", stopBool))
}

func (s *Super) GetWarmUpMetaPaths(w http.ResponseWriter, r *http.Request) {
	var paths []map[string]interface{}

	s.ec.RemoteCache.WarmUpMetaPaths.Range(func(key, value interface{}) bool {
		warmPath := key.(string)
		warmUpPath := value.(*proto.WarmUpPathInfo)

		pathInfo := map[string]interface{}{
			"path":        warmPath,
			"status":      atomic.LoadInt32(&warmUpPath.Status),
			"flashAddr":   warmUpPath.FlashAddr,
			"loadedCount": warmUpPath.LoadedCount,
		}
		paths = append(paths, pathInfo)
		return true
	})

	result := map[string]interface{}{
		"paths": paths,
		"count": len(paths),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
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
	switch msgVal := msg.(type) {
	case error:
		err = msgVal
	case string:
		ret = msgVal
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

func (s *Super) DisableTrash(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		replyFail(w, r, err.Error())
		return
	}
	flag := r.FormValue("flag")
	switch strings.ToLower(flag) {
	case "true":
		s.mw.DisableTrashByClient(true)
	case "false":
		s.mw.DisableTrashByClient(false)
	default:
		err = fmt.Errorf("flag only can be set :true of false")
		replyFail(w, r, err.Error())
		return
	}
	replySucc(w, r, fmt.Sprintf("set disable flag to %v\n", flag))
}

func (s *Super) QueryTrash(w http.ResponseWriter, r *http.Request) {
	flag := s.mw.QueryTrashDisableByClient()
	if !flag {
		replySucc(w, r, fmt.Sprintf("Trash is now enable interval[%v]\n", s.mw.TrashInterval))
	} else {
		replySucc(w, r, "Trash is now disable\n")
	}
}

func (s *Super) EnableAuditLog(w http.ResponseWriter, r *http.Request) {
	var err error
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

	dir, logModule, logMaxSize, err := auditlog.GetAuditLogInfo()
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
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if log.EnableDebug() {
				log.LogDebugf("loopSyncMeta with config stopWarmMeta:%v", s.stopWarmMeta)
			}
			if !s.stopWarmMeta {
				s.ic.ChangeExpiration(RemoteMetaCacheDuration)
				if s.bcacheCheckInterval == 300 {
					s.bcacheCheckInterval = 1800
				}
			} else {
				s.ic.RecoverExpiration()
			}
			if (s.bcacheDir != "" || !s.stopWarmMeta) && atomic.CompareAndSwapInt32(&s.syncMetaCache, 0, 1) {
				go s.syncMeta()
			}
		case <-s.closeC:
			log.LogInfof("loopSyncMeta: exit")
			return
		}
	}
}

func (s *Super) syncMeta() {
	defer func() {
		atomic.StoreInt32(&s.syncMetaCache, 0)
	}()
	start := time.Now()
	cacheLen := s.ic.lruList.Len()

	allInodes := func() <-chan uint64 {
		out := make(chan uint64)
		go func() {
			for i := s.ic.lruList.Front(); i != nil; i = i.Next() {
				if i.Value != nil {
					oldInfo := i.Value.(*proto.InodeInfo)
					out <- oldInfo.Inode
				}
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
	time.Sleep(time.Second * time.Duration(s.bcacheCheckInterval))
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
		oldGen := s.ec.GetExtentCacheGen(newInfo.Inode)
		if oldGen == 0 {
			oldGen = oldInfo.Generation
		}
		if !oldInfo.ModifyTime.Equal(newInfo.ModifyTime) || newInfo.Generation != oldGen {
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
	s.mw.Close()
}

func (s *Super) SetTransaction(txMaskStr string, timeout int64, retryNum int64, retryInterval int64) {
	// maskStr := proto.GetMaskString(txMask)
	mask, err := proto.GetMaskFromString(txMaskStr)
	if err != nil {
		log.LogErrorf("SetTransaction: err[%v], op[%v], timeout[%v]", err, txMaskStr, timeout)
		return
	}

	s.mw.EnableTransaction = mask
	if timeout <= 0 {
		timeout = proto.DefaultTransactionTimeout
	}
	s.mw.TxTimeout = timeout

	if retryNum <= 0 {
		retryNum = proto.DefaultTxConflictRetryNum
	}
	s.mw.TxConflictRetryNum = retryNum

	if retryInterval <= 0 {
		retryInterval = proto.DefaultTxConflictRetryInterval
	}
	s.mw.TxConflictRetryInterval = retryInterval
	log.LogDebugf("SetTransaction: mask[%v], op[%v], timeout[%v], retryNum[%v], retryInterval[%v ms]",
		mask, txMaskStr, timeout, retryNum, retryInterval)
}

// loopWarmUpMetaPaths periodically checks WarmUpMetaPaths and warms up inode cache
func (s *Super) loopWarmUpMetaPaths() {
	ticker := time.NewTicker(WarmUpCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeC:
			log.LogInfof("loopWarmUpMetaPaths: exit")
			return
		case <-ticker.C:
			if !s.stopWarmMeta {
				s.processWarmUpMetaPaths()
			}
		}
	}
}

// processWarmUpMetaPaths processes all warm up paths in the map
func (s *Super) processWarmUpMetaPaths() {
	var pathsToProcess []proto.WarmUpMetaResource

	// Collect paths to process and expired paths
	s.ec.RemoteCache.WarmUpMetaPaths.Range(func(key, value interface{}) bool {
		warmPath := key.(string)
		warmUpPath := value.(*proto.WarmUpPathInfo)

		// Check if path is ready to be processed
		if atomic.LoadInt32(&warmUpPath.Status) == proto.WarmStatusInitializing {
			pathsToProcess = append(pathsToProcess, proto.WarmUpMetaResource{
				DirPath:    warmPath,
				ServerAddr: warmUpPath.FlashAddr,
			})
		}
		return true
	})

	// Process paths that need warming up
	for _, wp := range pathsToProcess {
		log.LogDebugf("processWarmUpMetaPaths: schedule warmUpPath dir=%s server=%s", wp.DirPath, wp.ServerAddr)
		go s.warmUpPath(wp)
	}
}

// warmUpPath warms up a single path by traversing its directory structure
func (s *Super) warmUpPath(metaResource proto.WarmUpMetaResource) {
	start := time.Now()
	if !atomic.CompareAndSwapInt32(&s.ec.RemoteCache.WarmPathWorked, 0, 1) {
		log.LogDebugf("warmUpPath: other path is working")
		return
	}
	defer atomic.StoreInt32(&s.ec.RemoteCache.WarmPathWorked, 0)
	value, ok := s.ec.RemoteCache.WarmUpMetaPaths.Load(metaResource.DirPath)
	if !ok {
		log.LogDebugf("warmUpPath: path %s not found in WarmUpMetaPaths", metaResource.DirPath)
		return
	}
	warmUpPath := value.(*proto.WarmUpPathInfo)

	// Try to set status to running atomically
	if !atomic.CompareAndSwapInt32(&warmUpPath.Status, proto.WarmStatusInitializing, proto.WarmStatusRunning) {
		log.LogDebugf("warmUpPath: path %s is already being processed", metaResource.DirPath)
		return
	}

	clientId := uuid.New().String()
	log.LogDebugf("warmUpPath: applying warmup meta token for path %s server %s client %s", metaResource.DirPath, metaResource.ServerAddr, clientId)
	success, err := s.ec.RemoteCache.ApplyWarmupMetaToken(metaResource.ServerAddr, clientId, proto.WarmupMetaTokenApply)
	if err != nil {
		_, _ = s.ec.RemoteCache.ApplyWarmupMetaToken(metaResource.ServerAddr, clientId, proto.WarmupMetaTokenRelease)
		log.LogWarnf("warmUpPath: apply warm up meta token failed %v", err)
		atomic.StoreInt32(&warmUpPath.Status, proto.WarmStatusFailed)
		return
	}
	log.LogDebugf("warmUpPath: apply token result success=%v for path %s", success, metaResource.DirPath)
	if !success {
		log.LogDebugf("warmUpPath: token not granted, reinitialize status for path %s", metaResource.DirPath)
		atomic.StoreInt32(&warmUpPath.Status, proto.WarmStatusInitializing)
		return
	}
	// Create channel for renew goroutine coordination
	renewDone := make(chan struct{})
	defer func() {
		close(renewDone)
		_, _ = s.ec.RemoteCache.ApplyWarmupMetaToken(metaResource.ServerAddr, clientId, proto.WarmupMetaTokenRelease)
	}()

	// Start a goroutine to periodically renew the token
	go func() {
		ticker := time.NewTicker(30 * time.Second) // Renew every 30 seconds
		defer ticker.Stop()
		for {
			select {
			case <-s.closeC:
				log.LogDebugf("warmUpPath: renew goroutine stopped due to super close for path %s", metaResource.DirPath)
				return
			case <-renewDone:
				log.LogDebugf("warmUpPath: renew goroutine stopped for path %s", metaResource.DirPath)
				return
			case <-ticker.C:
				// Check if warmup is still running
				if atomic.LoadInt32(&warmUpPath.Status) != proto.WarmStatusRunning {
					log.LogDebugf("warmUpPath: warmup status changed to %d, stopping renew for path %s",
						atomic.LoadInt32(&warmUpPath.Status), metaResource.DirPath)
					return
				}

				// Try to renew the token
				renew, err1 := s.ec.RemoteCache.ApplyWarmupMetaToken(metaResource.ServerAddr, clientId, proto.WarmupMetaTokenRenew)
				if err1 != nil {
					atomic.CompareAndSwapInt32(&warmUpPath.Status, proto.WarmStatusRunning, proto.WarmStatusFailed)
					log.LogWarnf("warmUpPath: renew warmup meta token failed for path %s, err: %v", metaResource.DirPath, err)
					return
				}
				if !renew {
					atomic.CompareAndSwapInt32(&warmUpPath.Status, proto.WarmStatusRunning, proto.WarmStatusFailed)
					log.LogWarnf("warmUpPath: renew warmup meta token failed (client not found) for path %s", metaResource.DirPath)
					return
				}
				log.LogDebugf("warmUpPath: successfully renewed token for path %s", metaResource.DirPath)
			}
		}
	}()

	log.LogInfof("warmUpPath: start warming up path %s", metaResource.DirPath)

	// Get the inode for the path
	ino, err := s.mw.LookupPath(metaResource.DirPath)
	if err != nil {
		log.LogWarnf("warmUpPath: failed to get inode for path %s, err: %v", metaResource.DirPath, err)
		atomic.StoreInt32(&warmUpPath.Status, proto.WarmStatusFailed)
		return
	}
	var dirCount int64 = 1 // include root dir
	var nonDirCount int64 = 0
	defer func() {
		// Set status to completed when done
		atomic.StoreInt32(&warmUpPath.Status, proto.WarmStatusCompleted)
		log.LogInfof("warmUpPath: completed warming up path %s", metaResource.DirPath)
		// audit with counts
		dst := fmt.Sprintf("%s dirs:%d files:%d", metaResource.ServerAddr, atomic.LoadInt64(&dirCount), atomic.LoadInt64(&nonDirCount))
		auditlog.LogClientOp("WarmUpMeta", metaResource.DirPath, dst, nil, time.Since(start).Microseconds(), ino, 0)
	}()
	var (
		wg                  sync.WaitGroup
		currentGoroutineNum int64 = 0
	)
	wg.Add(1)
	atomic.AddInt64(&currentGoroutineNum, 1)
	// Warm up the directory recursively
	s.warmUpDirectory(ino, &currentGoroutineNum, &wg, true, warmUpPath, &dirCount, &nonDirCount)
	wg.Wait()
}

// warmUpDirectory recursively warms up a directory and its contents using batch scanning
func (s *Super) warmUpDirectory(dirIno uint64, currentGoroutineNum *int64, wg *sync.WaitGroup, created bool, warmUpPath *proto.WarmUpPathInfo, dirCnt *int64, nonDirCnt *int64) {
	defer func() {
		if created {
			atomic.AddInt64(currentGoroutineNum, -1)
			wg.Done()
		}
	}()
	// Get directory info and cache it
	dirInfo, err := s.mw.InodeGet_ll(dirIno)
	if err != nil {
		log.LogWarnf("warmUpDirectory: failed to get dir info for ino %d, err: %v", dirIno, err)
		return
	}
	s.ic.Put(dirInfo)
	atomic.AddInt64(&warmUpPath.LoadedCount, 1)

	// Use batch scanning similar to ManualScanner
	marker := ""
	done := false
	var subDirs []uint64
	for !done {
		if s.stopWarmMeta || atomic.LoadInt32(&warmUpPath.Status) != proto.WarmStatusRunning {
			return
		}
		select {
		case <-s.closeC:
			log.LogDebugf("warmUpDirectory: stopped due to super close for dir %v", dirIno)
			return
		default:
		}
		// Read directory contents in batches
		log.LogDebugf("warmUpDirectory: reading dir ino=%d marker=%s limit=%d", dirIno, marker, s.readDirLimit)
		children, err := s.mw.ReadDirLimit_ll(dirIno, marker, uint64(s.readDirLimit))
		if err != nil {
			log.LogWarnf("warmUpDirectory: failed to read dir for ino %d, marker %s, err: %v", dirIno, marker, err)
			return
		}
		log.LogDebugf("warmUpDirectory: read %d entries for dir ino=%d marker=%s", len(children), dirIno, marker)

		if len(children) == 0 {
			break
		}
		atomic.AddInt64(&warmUpPath.LoadedCount, int64(len(children)))
		// Handle marker logic
		if marker != "" {
			if len(children) >= 1 && marker == children[0].Name {
				if len(children) <= 1 {
					break
				} else {
					children = children[1:]
				}
			}
		}

		// Collect inodes for batch processing
		var inodes []uint64
		for _, child := range children {
			inodes = append(inodes, child.Inode)

			// If it's a directory, add to subDirs for recursive processing
			if proto.IsDir(child.Type) {
				subDirs = append(subDirs, child.Inode)
				atomic.AddInt64(dirCnt, 1)
			} else {
				atomic.AddInt64(nonDirCnt, 1)
			}
		}
		// Batch get inode info and cache them
		if len(inodes) > 0 {
			infos := s.mw.BatchInodeGet(inodes)
			for _, info := range infos {
				s.ic.Put(info)
			}
			log.LogDebugf("warmUpDirectory: cached %d inodes for dir %v (batch)", len(infos), dirIno)
		}

		// Check if we've read all entries
		childrenNr := len(children)
		if (marker == "" && childrenNr < int(s.readDirLimit)) || (marker != "" && childrenNr+1 < int(s.readDirLimit)) {
			done = true
		} else {
			marker = children[childrenNr-1].Name
		}
	}
	// Recursively warm up subdirectories
	if len(subDirs) > 0 {
		for _, subDir := range subDirs {
			if atomic.LoadInt64(currentGoroutineNum) < s.maxWarmUpConcurrency {
				atomic.AddInt64(currentGoroutineNum, 1)
				wg.Add(1)
				go s.warmUpDirectory(subDir, currentGoroutineNum, wg, true, warmUpPath, dirCnt, nonDirCnt)
			} else {
				s.warmUpDirectory(subDir, currentGoroutineNum, wg, false, warmUpPath, dirCnt, nonDirCnt)
			}
		}
	}
}
