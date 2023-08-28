// Copyright 2020 The ChubaoFS Authors.
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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	syslog "log"
	_ "net/http/pprof"
	"os"
	gopath "path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"

	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/version"
	"github.com/willf/bitset"
)

const (
	normalExtentSize      = 32 * 1024 * 1024
	defaultBlkSize        = uint32(1) << 12
	maxFdNum         uint = 1024000
	autoOffset            = -1

	appMysql8  = "mysql_8"
	appCoralDB = "coraldb"

	fileBinlog   = "mysql-bin"
	fileRedolog  = "ib_logfile"
	fileRelaylog = "relay-bin"

	fileTypeBinlog   = 1
	fileTypeRedolog  = 2
	fileTypeRelaylog = 3

	// cache
	maxInodeCache         = 10000
	inodeExpiration       = time.Hour
	inodeEvictionInterval = time.Hour
	dentryValidDuration   = time.Hour

	RequestMasterRetryInterval = time.Second * 2
	CommonRetryTime            = 5
	ListenRetryTime            = 300

	DefaultPidFile = "cfs.pid"
)

var (
	gClientManager   *clientManager
	signalIgnoreFunc = func() {}

	sdkInitOnce              sync.Once
	signalIgnoreOnce         sync.Once
	versionReporterStartOnce sync.Once
)

var signalsIgnored = []os.Signal{
	syscall.SIGUSR1,
	syscall.SIGUSR2,
	syscall.SIGPIPE,
	syscall.SIGALRM,
	syscall.SIGXCPU,
	syscall.SIGXFSZ,
	syscall.SIGVTALRM,
	syscall.SIGPROF,
	syscall.SIGIO,
	syscall.SIGPWR,
}

func init() {
	_ = os.Setenv("GODEBUG", "madvdontneed=1")
	data.SetNormalExtentSize(normalExtentSize)
	gClientManager = newClientManager()
}

type clientManager struct {
	moduleName   string
	nextClientID int64
	clients      map[int64]*client
	mu           sync.RWMutex
	profPort     uint64
	stopC        chan struct{}
	wg           sync.WaitGroup
	outputFile   *os.File
}

func (m *clientManager) GetNextClientID() int64 {
	return atomic.AddInt64(&m.nextClientID, 1)
}

func (m *clientManager) PutClient(id int64, c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[id] = c
}

func (m *clientManager) RemoveClient(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, id)
}

func (m *clientManager) GetClient(id int64) (c *client, exist bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, exist = m.clients[id]
	return
}

func newClientManager() *clientManager {
	return &clientManager{
		moduleName: "kbpclient",
		clients:    make(map[int64]*client),
		stopC:      make(chan struct{}),
	}
}

func parsePaths(str string) (res []string) {
	var tmp []string
	tmp = strings.Split(str, ",")
	for _, p := range tmp {
		if p == "" {
			continue
		}
		p2 := gopath.Clean("/" + p)
		res = append(res, p2)
	}
	return
}

func getNextClientID() int64 {
	return gClientManager.GetNextClientID()
}

func getClient(id int64) (c *client, exist bool) {
	return gClientManager.GetClient(id)
}

func putClient(id int64, c *client) {
	gClientManager.PutClient(id, c)
}

func removeClient(id int64) {
	gClientManager.RemoveClient(id)
}

func getVersionInfoString() string {
	cmd, _ := os.Executable()
	if len(os.Args) > 1 {
		cmd = cmd + " " + strings.Join(os.Args[1:], " ")
	}
	pid := os.Getpid()
	return fmt.Sprintf("ChubaoFS %s\nBranch: %s\nVersion: %s\nCommit: %s\nBuild: %s %s %s %s\nCMD: %s\nPID: %d\n",
		gClientManager.moduleName, BranchName, proto.Version, CommitID, runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime, cmd, pid)
}

func startVersionReporter(cluster, volName string, masters []string) {
	versionReporterStartOnce.Do(func() {
		versionInfo := getVersionInfoString()
		cfgStr, _ := json.Marshal(struct {
			ClusterName string `json:"clusterName"`
		}{cluster})
		cfg := config.LoadConfigString(string(cfgStr))
		gClientManager.wg.Add(1)
		go version.ReportVersionSchedule(cfg, masters, versionInfo, volName, "", CommitID, gClientManager.profPort, gClientManager.stopC, &gClientManager.wg)
	})
}

type file struct {
	fd    uint
	ino   uint64
	flags uint32
	mode  uint32
	size  uint64
	pos   uint64

	// save the path for openat, fstat, etc.
	path string
	// symbolic file only
	target []byte
	// dir only
	dirp *dirStream
	// for file write lock
	mu       sync.RWMutex
	locked   bool
	fileType uint8
}

type dirStream struct {
	pos     int
	dirents []proto.Dentry
}

type client struct {
	// client id allocated by libsdk
	id         int64
	configPath string

	// mount config
	masterAddr   string
	volName      string
	owner        string
	followerRead bool
	app          string

	readProcs       map[string]string // key: ip:port, value: register time
	readProcMapLock sync.Mutex

	masterClient    string
	readOnly        bool
	readOnlyExclude []string

	autoFlush bool

	// runtime context
	cwd    string // current working directory
	fdmap  map[uint]*file
	fdset  *bitset.BitSet
	fdlock sync.RWMutex
	inomap map[uint64]map[uint]bool // all open fd of given ino

	// server info
	mc *master.MasterClient
	mw *meta.MetaWrapper
	ec *data.ExtentClient

	// meta cache
	useMetaCache         bool
	inodeCache           *cache.InodeCache
	inodeDentryCache     map[uint64]*cache.DentryCache
	inodeDentryCacheLock sync.RWMutex

	totalState string
	sdkState   string
	closeOnce  sync.Once

	pidFile   string
	localAddr string
	stopC     chan struct{}
	wg        sync.WaitGroup
}

type FileState struct {
	Fd    uint
	Ino   uint64
	Flags uint32
	Mode  uint32
	Size  uint64
	Pos   uint64

	// save the path for openat, fstat, etc.
	Path string
	// symbolic file only
	Target string
	// dir only
	DirPos int
	Locked bool
}

/*
 *old struct
type SDKState struct {
	Cwd            string
	ReadProcErrMap map[string]int
	Files          []FileState
}
*/

type SDKState struct {
	Cwd            string
	ReadProcErrMap map[string]int // to be compatible with old version
	Files          []FileState
	ReadOnly       bool
	ReadProcs      map[string]string
	MetaState      *meta.MetaState
	DataState      *data.DataState
}

func (c *client) saveClientState() *SDKState {
	clientState := new(SDKState)
	clientState.ReadOnly = c.readOnly
	clientState.Cwd = c.cwd
	clientState.ReadProcs = c.readProcs
	clientState.ReadProcErrMap = make(map[string]int)
	for readProc, _ := range c.readProcs {
		clientState.ReadProcErrMap[readProc] = 0
	}

	c.fdlock.Lock()
	fdmap := c.fdmap
	c.fdlock.Unlock()
	files := make([]FileState, 0, len(fdmap))
	for _, v := range fdmap {
		var f FileState
		f.Fd = v.fd
		f.Ino = v.ino
		f.Flags = v.flags
		f.Mode = v.mode
		f.Size = v.size
		f.Pos = v.pos
		f.Path = v.path
		f.Target = string(v.target)
		f.Locked = v.locked
		if v.dirp != nil {
			f.DirPos = v.dirp.pos
		} else {
			f.DirPos = -1
		}
		files = append(files, f)
	}

	clientState.Files = files
	return clientState
}

func (c *client) rebuildClientState(clientState *SDKState) {
	c.cwd = clientState.Cwd
	c.readOnly = clientState.ReadOnly
	if clientState.ReadProcs != nil {
		c.readProcs = clientState.ReadProcs
	} else if clientState.ReadProcErrMap != nil {
		for readProc, _ := range clientState.ReadProcErrMap {
			c.readProcs[readProc] = time.Now().Format("2006-01-02 15:04:05")
		}
	}

	for _, v := range clientState.Files {
		f := &file{fd: v.Fd, ino: v.Ino, flags: v.Flags, mode: v.Mode, size: v.Size, pos: v.Pos, path: v.Path, target: []byte(v.Target), locked: v.Locked}
		if v.DirPos >= 0 {
			f.dirp = &dirStream{}
			f.dirp.pos = v.DirPos
			dentries, err := c.mw.ReadDir_ll(context.Background(), f.ino)
			if err != nil {
				msg := fmt.Sprintf("id(%v) fd(%v) path(%v) ino(%v) err(%v)", c.id, f.fd, f.path, f.ino, err)
				handleError(c, "readDir when newClient", msg)
				continue
			}
			f.dirp.dirents = dentries
		}
		if proto.IsRegular(f.mode) {
			c.openInodeStream(f)
			c.ec.RefreshExtentsCache(nil, f.ino)
		}
		if v.Locked {
			f.mu.Lock()
		}
		c.fdmap[f.fd] = f
		c.fdset.Set(f.fd)
		fdmap, ok := c.inomap[f.ino]
		if !ok {
			fdmap = make(map[uint]bool)
			c.inomap[f.ino] = fdmap
		}
		fdmap[f.fd] = true
	}
}

func (c *client) openInodeStream(f *file) {
	overWriteBuffer := false
	_, name := gopath.Split(f.path)
	nameParts := strings.Split(name, ".")
	if nameParts[0] == fileRelaylog && len(nameParts) > 1 && len(nameParts[1]) > 0 && unicode.IsDigit(rune(nameParts[1][0])) {
		f.fileType = fileTypeRelaylog
	} else if nameParts[0] == fileBinlog && len(nameParts) > 1 && len(nameParts[1]) > 0 && unicode.IsDigit(rune(nameParts[1][0])) {
		f.fileType = fileTypeBinlog
	} else if strings.Contains(nameParts[0], fileRedolog) {
		f.fileType = fileTypeRedolog
		if c.app == appMysql8 || c.app == appCoralDB {
			overWriteBuffer = true
		}
	}
	c.ec.OpenStream(f.ino, overWriteBuffer)
}

func (c *client) absPath(path string) string {
	if !gopath.IsAbs(path) {
		path = gopath.Join(c.cwd, path)
	}
	return gopath.Clean(path)
}

func (c *client) checkVolWriteMutex() (err error) {
	if c.app != appCoralDB {
		return nil
	}

	var volWriteMutexInfo *proto.VolWriteMutexInfo

	for retry := 0; retry < StartRetryMaxCount; retry++ {
		for {
			volWriteMutexInfo, err = c.mc.ClientAPI().GetVolMutex(appCoralDB, c.volName)
			if err == nil && volWriteMutexInfo.Enable && volWriteMutexInfo.Holder == "" {
				time.Sleep(RequestMasterRetryInterval)
				continue
			}
			break
		}
		if err != nil {
			log.LogWarnf("StartClient: checkVolWriteMutex err(%v) retry count(%v)", err, retry)
			time.Sleep(StartRetryIntervalSec * time.Second)
		} else {
			break
		}
	}

	if err == nil && !volWriteMutexInfo.Enable {
		c.readOnly = false
		return nil
	}
	if err != nil {
		return err
	}

	c.masterClient = volWriteMutexInfo.Holder
	syslog.Printf("localAddr: %s, masterClient: %s\n", c.localAddr, c.masterClient)
	if c.masterClient == c.localAddr {
		c.readOnly = false
		c.readProcMapLock.Lock()
		c.readProcs = volWriteMutexInfo.Slaves
		c.readProcMapLock.Unlock()
	}
	return nil
}

func (c *client) updateVolWriteMutexInfo() {
	defer c.wg.Done()

	var (
		err               error
		volWriteMutexInfo *proto.VolWriteMutexInfo
	)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopC:
			return
		case <-ticker.C:
			volWriteMutexInfo, err = c.mc.ClientAPI().GetVolMutex(appCoralDB, c.volName)
			if err != nil {
				continue
			}
			if !volWriteMutexInfo.Enable || volWriteMutexInfo.Holder == "" {
				c.readOnly = false
				c.masterClient = ""
				continue
			}
			if c.masterClient != volWriteMutexInfo.Holder {
				c.masterClient = volWriteMutexInfo.Holder
				if c.masterClient == c.localAddr {
					c.readOnly = false
				} else {
					c.readOnly = true
					c.readProcMapLock.Lock()
					if len(c.readProcs) > 0 {
						c.readProcs = make(map[string]string)
					}
					c.readProcMapLock.Unlock()
				}
			}
			if !c.readOnly {
				c.readProcMapLock.Lock()
				c.readProcs = volWriteMutexInfo.Slaves
				c.readProcMapLock.Unlock()
			}
		}
	}
}

func (c *client) start(first_start bool, sdkState *SDKState) (err error) {
	defer func() {
		if err != nil {
			gClientManager.outputFile.Sync()
			gClientManager.outputFile.Close()
			fmt.Printf("Start client failed: %v. Please check output.log for more details.\n", err)
		}
	}()

	masters := strings.Split(c.masterAddr, ",")
	c.mc = master.NewMasterClient(masters, false)

	var mw *meta.MetaWrapper
	metaConfig := &meta.MetaConfig{
		Volume:        c.volName,
		Masters:       masters,
		ValidateOwner: true,
		Owner:         c.owner,
		InfiniteRetry: true,
	}
	if first_start {
		if mw, err = meta.NewMetaWrapper(metaConfig); err != nil {
			syslog.Println(err)
			return
		}
	} else {
		mw = meta.RebuildMetaWrapper(metaConfig, sdkState.MetaState)
	}
	c.mw = mw

	var ec *data.ExtentClient
	extentConfig := &data.ExtentConfig{
		Volume:            c.volName,
		Masters:           masters,
		FollowerRead:      c.followerRead,
		OnInsertExtentKey: mw.InsertExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		TinySize:          data.NoUseTinyExtent,
		AutoFlush:         c.autoFlush,
		MetaWrapper:       mw,
	}
	if first_start {
		if ec, err = data.NewExtentClient(extentConfig, nil); err != nil {
			syslog.Println(err)
			return
		}
	} else {
		ec = data.RebuildExtentClient(extentConfig, sdkState.DataState)
	}
	c.ec = ec

	// metric
	c.initUmpKeys()
	// different vols write logs to different ump files, otherwise logs may be lost while rotating
	umpFilePrefix := fmt.Sprintf("%v_%v_%v", mw.Cluster(), c.volName, gClientManager.moduleName)
	exporter.Init(exporter.NewOption().WithCluster(mw.Cluster()).WithModule(gClientManager.moduleName).WithUmpFilePrefix(umpFilePrefix))

	// version
	startVersionReporter(mw.Cluster(), c.volName, masters)
	return
}

func (c *client) stop() {
	c.closeOnce.Do(func() {
		if c.ec != nil {
			_ = c.ec.Close(context.Background())
			c.ec.CloseConnPool()
		}
		if c.mw != nil {
			_ = c.mw.Close()
		}

		c.inodeCache.Stop()
		close(c.stopC)
		c.wg.Wait()
	})
}

func (c *client) inodeHasOpenFD(ino uint64) bool {
	c.fdlock.RLock()
	defer c.fdlock.RUnlock()
	fdmap, ok := c.inomap[ino]
	if ok && len(fdmap) != 0 {
		return true
	}
	return false
}

func (c *client) allocFD() int {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	fd, ok := c.fdset.NextClear(0)
	if !ok || fd > maxFdNum {
		return -1
	}
	return int(fd)
}

func (c *client) allocFile(ino uint64, flags, mode uint32, target []byte, fd int) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	var (
		ok      bool
		real_fd uint
	)
	if fd <= 0 {
		real_fd, ok = c.fdset.NextClear(0)
		if !ok || real_fd > maxFdNum {
			return nil
		}
	} else {
		real_fd = uint(fd)
		if c.fdset.Test(real_fd) {
			return nil
		}
	}
	c.fdset.Set(real_fd)
	f := &file{fd: real_fd, ino: ino, flags: flags, mode: mode, target: target}
	c.fdmap[real_fd] = f
	fdmap, ok := c.inomap[ino]
	if !ok {
		fdmap = make(map[uint]bool)
		c.inomap[ino] = fdmap
	}
	fdmap[real_fd] = true
	return f
}

func (c *client) getFile(fd uint) *file {
	c.fdlock.RLock()
	f := c.fdmap[fd]
	c.fdlock.RUnlock()
	return f
}

func (c *client) copyFile(fd uint, newfd uint) uint {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	newfd, ok := c.fdset.NextClear(newfd)
	if !ok || newfd > maxFdNum {
		return 0
	}
	c.fdset.Set(newfd)
	f := c.fdmap[fd]
	if f == nil {
		return 0
	}
	newfile := &file{fd: f.fd, ino: f.ino, flags: f.flags, mode: f.mode, size: f.size, pos: f.pos, path: f.path, target: f.target, dirp: f.dirp}
	newfile.fd = newfd
	c.fdmap[newfd] = newfile
	if proto.IsRegular(newfile.mode) {
		c.ec.OpenStream(newfile.ino, false)
	}
	return newfd
}

func (c *client) create(ctx context.Context, parentID uint64, name string, mode, uid, gid uint32, target []byte) (info *proto.InodeInfo, err error) {
	if info, err = c.mw.Create_ll(nil, parentID, name, mode, uid, gid, target); err != nil {
		return
	}
	c.inodeCache.Delete(nil, parentID)
	c.inodeCache.Put(info)
	c.inodeDentryCacheLock.Lock()
	dentryCache, ok := c.inodeDentryCache[parentID]
	if !ok {
		dentryCache = cache.NewDentryCache(dentryValidDuration, c.useMetaCache)
		c.inodeDentryCache[parentID] = dentryCache
	}
	dentryCache.Put(name, info.Inode)
	c.inodeDentryCacheLock.Unlock()
	return
}

func (c *client) delete(ctx context.Context, parentID uint64, name string, isDir bool) (info *proto.InodeInfo, err error) {
	info, err = c.mw.Delete_ll(nil, parentID, name, isDir)
	c.inodeCache.Delete(nil, parentID)
	c.invalidateDentry(parentID, name)
	return
}

func (c *client) truncate(ctx context.Context, inode uint64, len uint64) (err error) {
	err = c.ec.Truncate(nil, inode, len)
	info := c.inodeCache.Get(nil, inode)
	if info != nil {
		info.Size = uint64(len)
		c.inodeCache.Put(info)
	}
	c.updateSizeByIno(inode, len)
	return
}

func (c *client) flush(ctx context.Context, inode uint64) (err error) {
	err = c.ec.Flush(nil, inode)
	//c.inodeCache.Delete(nil,inode)
	return
}

func (c *client) updateSizeByIno(ino uint64, size uint64) {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	fdmap, ok := c.inomap[ino]
	if !ok {
		return
	}
	for fd := range fdmap {
		file, ok := c.fdmap[fd]
		if !ok {
			continue
		}
		file.size = size
	}
}

func (c *client) releaseFD(fd uint) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	f, ok := c.fdmap[fd]
	if !ok {
		return nil
	}
	fdmap, ok := c.inomap[f.ino]
	if ok {
		delete(fdmap, fd)
	}
	if len(fdmap) == 0 {
		delete(c.inomap, f.ino)
	}
	delete(c.fdmap, fd)
	c.fdset.Clear(fd)
	return f
}

func (c *client) getFileByInode(ino uint64) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()

	fdmap, ok := c.inomap[ino]
	if !ok {
		return nil
	}
	for fd := range fdmap {
		f, ok := c.fdmap[fd]
		if !ok {
			continue
		}
		return f
	}
	return nil
}

func (c *client) getInodeByPath(ctx context.Context, path string) (info *proto.InodeInfo, err error) {
	var ino uint64
	ino, err = c.lookupPath(nil, path)
	if err != nil {
		return
	}
	info, err = c.getInode(nil, ino)
	return
}

func (c *client) lookupPath(ctx context.Context, path string) (ino uint64, err error) {
	ino = proto.RootIno
	if path != "" && path != "/" {
		dirs := strings.Split(path, "/")
		var child uint64
		for _, dir := range dirs {
			if dir == "/" || dir == "" {
				continue
			}
			child, err = c.getDentry(nil, ino, dir, false)
			if err != nil {
				ino = 0
				return
			}
			ino = child
		}
	}
	return
}

func (c *client) getInode(ctx context.Context, ino uint64) (info *proto.InodeInfo, err error) {
	info = c.inodeCache.Get(nil, ino)
	if info != nil {
		return
	}
	info, err = c.mw.InodeGet_ll(nil, ino)
	if err != nil {
		return
	}
	c.inodeCache.Put(info)
	return
}

func (c *client) getDentry(ctx context.Context, parentID uint64, name string, strict bool) (ino uint64, err error) {
	c.inodeDentryCacheLock.Lock()
	defer c.inodeDentryCacheLock.Unlock()

	dentryCache, cacheExists := c.inodeDentryCache[parentID]
	if cacheExists && !strict {
		var ok bool
		if ino, ok = dentryCache.Get(name); ok {
			return
		}
	}

	ino, _, err = c.mw.Lookup_ll(nil, parentID, name)
	if err != nil {
		return
	}

	if !cacheExists {
		dentryCache = cache.NewDentryCache(dentryValidDuration, c.useMetaCache)
		c.inodeDentryCache[parentID] = dentryCache
	}

	dentryCache.Put(name, ino)
	return
}

func (c *client) invalidateDentry(parentID uint64, name string) {
	c.inodeDentryCacheLock.Lock()
	defer c.inodeDentryCacheLock.Unlock()
	dentryCache, parentOk := c.inodeDentryCache[parentID]
	if parentOk {
		dentryCache.Delete(name)
		if dentryCache.Count() == 0 {
			delete(c.inodeDentryCache, parentID)
		}
	}
}

func (c *client) setattr(ctx context.Context, info *proto.InodeInfo, valid uint32, mode, uid, gid uint32, mtime, atime int64) error {
	// Only rwx mode bit can be set
	if valid&proto.AttrMode != 0 {
		fuseMode := mode & uint32(0777)
		mode = info.Mode &^ uint32(0777) // clear rwx mode bit
		mode |= fuseMode
	}
	return c.mw.Setattr(nil, info.Inode, valid, mode, uid, gid, atime, mtime)
}

func (c *client) closeStream(f *file) {
	_ = c.ec.CloseStream(context.Background(), f.ino)
	_ = c.ec.EvictStream(context.Background(), f.ino)
}

func (c *client) broadcastAllReadProcess(ino uint64) {
	c.readProcMapLock.Lock()
	readProcs := c.readProcs
	c.readProcMapLock.Unlock()

	log.LogInfof("broadcastAllReadProcess: readProcessMap(%v)", readProcs)
	for readClient, _ := range readProcs {
		c.broadcastRefreshExtents(readClient, ino)
	}
}

func (c *client) checkReadOnly(absPath string) bool {
	if !c.readOnly {
		return false
	}
	for _, pre := range c.readOnlyExclude {
		if !strings.HasPrefix(absPath, pre) {
			continue
		}
		if pre == "/" || len(absPath) == len(pre) || absPath[len(pre)] == '/' {
			return false
		}
	}
	return true
}

func handleError(c *client, act, msg string) {
	log.LogErrorf("%s: %s", act, msg)
	log.LogFlush()

	if c != nil {
		key1 := fmt.Sprintf("%s_%s_warning", c.mw.Cluster(), c.volName)
		errmsg1 := fmt.Sprintf("act(%s) - %s", act, msg)
		exporter.WarningBySpecialUMPKey(key1, errmsg1)

		key2 := fmt.Sprintf("%s_%s_warning", c.mw.Cluster(), gClientManager.moduleName)
		errmsg2 := fmt.Sprintf("volume(%s) %s", c.volName, errmsg1)
		exporter.WarningBySpecialUMPKey(key2, errmsg2)
		exporter.FlushWarning()
	}
}

func isMysql() bool {
	processName := filepath.Base(os.Args[0])
	return strings.Contains(processName, "mysqld")
}

func useROWNotify() bool {
	processName := filepath.Base(os.Args[0])
	return strings.Contains(processName, "mysqld") || strings.Contains(processName, "innobackupex") || strings.Contains(processName, "xtrabackup")
}
