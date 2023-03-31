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

/*
#define _GNU_SOURCE
#ifndef __USE_LARGEFILE64
#define __USE_LARGEFILE64
#endif
#ifndef FALLOC_FL_KEEP_SIZE
#define FALLOC_FL_KEEP_SIZE 0x01
#endif
#ifndef FALLOC_FL_PUNCH_HOLE
#define FALLOC_FL_PUNCH_HOLE 0x02
#endif
#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

typedef struct {
    int ignore_sighup;
	int ignore_sigterm;
	const char* log_dir;
	const char* log_level;
	const char* prof_port;
} cfs_sdk_init_t;

typedef struct {
	char version[256];
    uint32_t version_len;
	char branch[256];
    uint32_t branch_len;
	char commit_id[256];
	uint32_t commit_id_len;
	char runtime_version[256];
	uint32_t runtime_version_len;
	char goos[256];
	uint32_t goos_len;
	char goarch[256];
	uint32_t goarch_len;
    char build_time[256];
	uint32_t build_time_len;
} cfs_sdk_version_t;

typedef struct {
    const char* master_addr;
    const char* vol_name;
    const char* owner;
    const char* follower_read;
	const char* app;
	const char* auto_flush;
    const char* master_client;
} cfs_config_t;

typedef struct {
	uint64_t total;
	uint64_t used;
} cfs_statfs_t;

typedef struct {
    uint64_t ino;
    char     name[256];
    char     d_type;
    uint32_t     nameLen;
} cfs_dirent_t;

typedef struct {
	int fd;
	int flags;
    int file_type;
    int dup_ref;
    ino_t inode;
	size_t size;
	off_t pos;
} cfs_file_t;

typedef struct {
	off_t 		file_offset;
	size_t 		size;
	uint64_t 	partition_id;
	uint64_t 	extent_id;
	uint64_t 	extent_offset;
	char 	 	dp_host[32];
	int 	 	dp_port;
} cfs_read_req_t;
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	syslog "log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	gopath "path"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/willf/bitset"
	"golang.org/x/sys/unix"
	"gopkg.in/ini.v1"
)

var (
	statusOK = C.int(0)
	// error status must be minus value
	statusEPERM   = errorToStatus(syscall.EPERM)
	statusEIO     = errorToStatus(syscall.EIO)
	statusEINVAL  = errorToStatus(syscall.EINVAL)
	statusEEXIST  = errorToStatus(syscall.EEXIST)
	statusEBADFD  = errorToStatus(syscall.EBADFD)
	statusEACCES  = errorToStatus(syscall.EACCES)
	statusEMFILE  = errorToStatus(syscall.EMFILE)
	statusENOTDIR = errorToStatus(syscall.ENOTDIR)
	statusERANGE  = errorToStatus(syscall.ERANGE)
	statusENODATA = errorToStatus(syscall.ENODATA)
)

/*
 * Library / framework initialization
 * This method will initialize logging and HTTP APIs.
 */
//export cfs_sdk_init
func cfs_sdk_init(t *C.cfs_sdk_init_t) C.int {
	var re C.int
	sdkInitOnce.Do(func() {
		re = initSDK(t)
	})
	return re
}

func initSDK(t *C.cfs_sdk_init_t) C.int {
	var ignoreSigHup = int(t.ignore_sighup)
	var ignoreSigTerm = int(t.ignore_sigterm)

	var logDir = C.GoString(t.log_dir)
	var logLevel = C.GoString(t.log_level)

	gClientManager.profPort, _ = strconv.ParseUint(strings.Split(C.GoString(t.prof_port), ",")[0], 10, 64)

	// Setup signal ignore
	ignoreSignals := make([]os.Signal, 0)
	if ignoreSigHup == 1 {
		ignoreSignals = append(ignoreSignals, syscall.SIGHUP)
	}
	if ignoreSigTerm == 1 {
		ignoreSignals = append(ignoreSignals, syscall.SIGTERM)
	}
	if len(ignoreSignals) > 0 {
		signalIgnoreFunc = func() {
			signal.Ignore(ignoreSignals...)
		}
	}

	signal.Ignore(signalsIgnored...)

	var err error

	// Initialize logging
	level := log.WarnLevel
	if logLevel == "debug" {
		level = log.DebugLevel
	} else if logLevel == "info" {
		level = log.InfoLevel
	} else if logLevel == "warn" {
		level = log.WarnLevel
	} else if logLevel == "error" {
		level = log.ErrorLevel
	}
	if len(logDir) == 0 {
		syslog.Println("no valid log dir specified.\n")
		return C.int(statusEINVAL)
	}
	if _, err = log.InitLog(logDir, gClientManager.moduleName, level, nil); err != nil {
		syslog.Printf("initialize logging failed: %v\n", err)
		return C.int(statusEIO)
	}

	outputFilePath := gopath.Join(logDir, gClientManager.moduleName, "output.log")
	if gClientManager.outputFile, err = os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); err != nil {
		syslog.Printf("open %v for stdout redirection failed: %v", outputFilePath, err)
		return C.int(statusEIO)
	}
	_ = os.Chmod(gopath.Join(logDir, gClientManager.moduleName), 0777)
	_ = os.Chmod(outputFilePath, 0666)
	syslog.SetOutput(gClientManager.outputFile)

	// Initialize HTTP APIs
	if gClientManager.profPort == 0 && useROWNotify() {
		syslog.Printf("prof port is required in mysql but not specified")
		return C.int(statusEINVAL)
	}
	if gClientManager.profPort != 0 {
		log.LogInfof("using prof port: %v", gClientManager.profPort)
		syslog.Printf("using prof port: %v\n", gClientManager.profPort)

		http.HandleFunc(ControlVersion, GetVersionHandleFunc)
		http.HandleFunc(ControlReadProcessRegister, registerReadProcStatusHandleFunc)
		http.HandleFunc(ControlBroadcastRefreshExtents, broadcastRefreshExtentsHandleFunc)
		http.HandleFunc(ControlGetReadProcs, getReadProcsHandleFunc)
		http.HandleFunc(ControlSetReadWrite, setReadWrite)
		http.HandleFunc(ControlSetReadOnly, setReadOnly)
		http.HandleFunc(ControlGetReadStatus, getReadStatus)
		http.HandleFunc(ControlSetUpgrade, SetClientUpgrade)
		http.HandleFunc(ControlUnsetUpgrade, UnsetClientUpgrade)
		http.HandleFunc(ControlCommandGetUmpCollectWay, GetUmpCollectWay)
		http.HandleFunc(ControlCommandSetUmpCollectWay, SetUmpCollectWay)
		server := &http.Server{Addr: fmt.Sprintf(":%v", gClientManager.profPort)}
		var lc net.ListenConfig
		if isMysql() {
			// set socket option SO_REUSEPORT to let multiple processes listen on the same port
			lc = net.ListenConfig{
				Control: func(network, address string, c syscall.RawConn) error {
					var opErr error
					err := c.Control(func(fd uintptr) {
						opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
					})
					if err != nil {
						return err
					}
					return opErr
				},
			}
		}

		var ln net.Listener

		i := 0
		for ; i < ListenRetryTime; i++ {
			ln, err = lc.Listen(context.Background(), "tcp", server.Addr)
			if err == nil {
				break
			}
			if !useROWNotify() {
				syslog.Printf("listen prof port [%v] failed. No listen any port.", gClientManager.profPort)
				gClientManager.profPort = 0
				break
			}
			if i%30 == 0 {
				syslog.Printf("listen prof port [%v] failed: %v, try %d times", gClientManager.profPort, err, i+1)
			}
			time.Sleep(time.Second)
		}
		if i == ListenRetryTime {
			syslog.Printf("listen prof port [%v] failed. exit.", gClientManager.profPort)
			os.Exit(1)
		}
		if gClientManager.profPort != 0 {
			gClientManager.wg.Add(2)
			go func() {
				defer gClientManager.wg.Done()
				for i = 0; i < CommonRetryTime; i++ {
					err = server.Serve(ln)
					if err == http.ErrServerClosed {
						syslog.Printf("Stop listen prof port [%v]", gClientManager.profPort)
						break
					}
				}
				if i == CommonRetryTime {
					syslog.Printf("Serve prof port [%v] failed: %v.", gClientManager.profPort, err)
					if useROWNotify() {
						os.Exit(1)
					}
				}
			}()
			go func() {
				defer gClientManager.wg.Done()
				<-gClientManager.stopC
				server.Shutdown(context.Background())
			}()
		}
	}

	syslog.Printf(getVersionInfoString())
	log.LogDebugf("bypass client started.\n")

	return C.int(0)
}

//export cfs_sdk_version
func cfs_sdk_version(v *C.cfs_sdk_version_t) C.int {

	var copyStringToCCharArray = func(src string, dst *C.char, dstLen *C.uint32_t) {
		var srcLen = len(src)
		if srcLen >= 256 {
			srcLen = 255 - 1
		}
		hdr := (*reflect.StringHeader)(unsafe.Pointer(&src))
		C.memcpy(unsafe.Pointer(dst), unsafe.Pointer(hdr.Data), C.size_t(srcLen))
		*dstLen = C.uint32_t(srcLen)
	}

	// Fill up branch
	copyStringToCCharArray(BranchName, &v.branch[0], &v.branch_len)
	// Fill up commit ID
	copyStringToCCharArray(CommitID, &v.commit_id[0], &v.commit_id_len)
	// Fill up version
	copyStringToCCharArray(proto.BaseVersion, &v.version[0], &v.version_len)
	// Fill up runtime version
	copyStringToCCharArray(runtime.Version(), &v.runtime_version[0], &v.runtime_version_len)
	// Fill up GOOS
	copyStringToCCharArray(runtime.GOOS, &v.goos[0], &v.goos_len)
	// Fill up GOARCH
	copyStringToCCharArray(runtime.GOARCH, &v.goarch[0], &v.goarch_len)
	// Fill up build time
	copyStringToCCharArray(BuildTime, &v.build_time[0], &v.build_time_len)
	return C.int(0)
}

/*
 * Client operations
 */

//export cfs_new_client
func cfs_new_client(conf *C.cfs_config_t, configPath, str *C.char) C.int64_t {
	first_start := C.GoString(str) == ""
	sdkState := &SDKState{}
	if !first_start {
		err := json.Unmarshal([]byte(C.GoString(str)), sdkState)
		if err != nil {
			syslog.Printf("Unmarshal sdkState err(%v), sdkState(%s)\n", err, C.GoString(str))
			return C.int64_t(statusEIO)
		}
	}
	c := newClient(conf, C.GoString(configPath))

	if !first_start && sdkState.MetaState != nil && sdkState.MetaState.LocalIP != "" {
		c.localAddr = fmt.Sprintf("%s:%d", sdkState.MetaState.LocalIP, gClientManager.profPort)
	} else {
		localIp, err := iputil.GetLocalIPByDial(strings.Split(c.masterAddr, ","), iputil.GetLocalIPTimeout)
		if err != nil && c.app == appCoralDB {
			syslog.Printf("GetLocalIpAddr err: %v", err)
			os.Exit(1)
		}
		c.localAddr = fmt.Sprintf("%s:%d", localIp, gClientManager.profPort)
	}

	if isMysql() {
		if c.pidFile == "" {
			c.pidFile = path.Join(path.Dir(C.GoString(configPath)), DefaultPidFile)
		}
	}
	if first_start {
		if err := lockPidFile(c.pidFile); err != nil {
			syslog.Printf("lock pidFile %s failed: %v\n", c.pidFile, err)
			return C.int64_t(statusEIO)
		}
	}
	if err := c.start(sdkState.MetaState == nil, sdkState); err != nil {
		return C.int64_t(statusEIO)
	}

	if !first_start {
		c.rebuildClientState(sdkState)
	}
	putClient(c.id, c)

	if first_start {
		if err := c.checkVolWriteMutex(); err != nil {
			syslog.Printf("checkVolWriteMutex error: %v\n", err)
			return C.int64_t(statusEIO)
		}
		if useROWNotify() {
			c.registerReadProcStatus(true)
		}
	}
	if c.app == appCoralDB {
		c.wg.Add(1)
		go c.updateVolWriteMutexInfo()
	}

	return C.int64_t(c.id)
}

func newClient(conf *C.cfs_config_t, configPath string) *client {
	id := getNextClientID()
	c := &client{
		id:               id,
		configPath:       configPath,
		fdmap:            make(map[uint]*file),
		fdset:            bitset.New(maxFdNum),
		inomap:           make(map[uint64]map[uint]bool),
		cwd:              "/",
		inodeDentryCache: make(map[uint64]*cache.DentryCache),
		stopC:            make(chan struct{}),
	}

	if len(configPath) == 0 {
		c.masterAddr = C.GoString(conf.master_addr)
		c.volName = C.GoString(conf.vol_name)
		c.owner = C.GoString(conf.owner)
		c.followerRead, _ = strconv.ParseBool(C.GoString(conf.follower_read))
		c.app = C.GoString(conf.app)
		c.useMetaCache = (c.app != appCoralDB)
		c.autoFlush, _ = strconv.ParseBool(C.GoString(conf.auto_flush))
		c.masterClient = C.GoString(conf.master_client)
	} else {
		cfg, err := ini.Load(configPath)
		if err != nil {
			syslog.Printf("load config file %s err: %v", configPath, err)
			os.Exit(1)
		}
		c.masterAddr = cfg.Section("").Key("masterAddr").String()
		c.volName = cfg.Section("").Key("volName").String()
		c.owner = cfg.Section("").Key("owner").String()
		c.followerRead = cfg.Section("").Key("followerRead").MustBool(false)
		c.app = cfg.Section("").Key("app").String()
		c.useMetaCache = (c.app != appCoralDB)
		c.readOnly = (c.app == appCoralDB)
		c.readOnlyExclude = parsePaths(cfg.Section("").Key("readOnlyExclude").String())
		c.autoFlush = cfg.Section("").Key("autoFlush").MustBool(false)
		c.masterClient = cfg.Section("").Key("masterClient").String()
		if c.masterAddr == "" || c.volName == "" || c.owner == "" {
			syslog.Println("Check CFS config file for masterAddr, volName or owner.")
			os.Exit(1)
		}

		c.pidFile = cfg.Section("").Key("pidFile").String()
		if c.pidFile != "" && c.pidFile[0] != os.PathSeparator {
			syslog.Printf("pidFile(%s) must be a absolute path", c.pidFile)
			os.Exit(1)
		}
	}

	c.inodeCache = cache.NewInodeCache(inodeExpiration, maxInodeCache, inodeEvictionInterval, c.useMetaCache)
	c.readProcs = make(map[string]string)

	// Just skip fd 0, 1, 2, to avoid confusion.
	c.fdset.Set(0).Set(1).Set(2)

	return c
}

//export cfs_close_client
func cfs_close_client(id C.int64_t) {
	if c, exist := getClient(int64(id)); exist {
		removeClient(int64(id))
		c.stop()
	}
}

//export cfs_sdk_close
func cfs_sdk_close() {
	log.LogDebugf("close bypass client.\n")
	for id, c := range gClientManager.clients {
		removeClient(id)
		c.stop()
	}
	gClientManager.outputFile.Sync()
	gClientManager.outputFile.Close()
	close(gClientManager.stopC)
	gClientManager.wg.Wait()
	gClientManager = nil
	exporter.Stop()
	log.LogClose()
	runtime.GC()
}

//export cfs_statfs
func cfs_statfs(id C.int64_t, stat *C.cfs_statfs_t) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	total, used := c.mw.Statfs()
	stat.total = C.uint64_t(total)
	stat.used = C.uint64_t(used)
	return 0
}

//export cfs_flush_log
func cfs_flush_log() {
	log.LogFlush()
}

//export cfs_sdk_state
func cfs_sdk_state(id C.int64_t, buf unsafe.Pointer, size C.size_t) C.size_t {
	c, exist := getClient(int64(id))
	if !exist {
		return 0
	}
	if c.sdkState != "" {
		if int(size) < len(c.sdkState)+1 {
			return C.size_t(len(c.sdkState) + 1)
		}
		var buffer []byte
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
		hdr.Data = uintptr(buf)
		hdr.Len = len(c.sdkState) + 1
		hdr.Cap = len(c.sdkState) + 1
		copy(buffer, c.sdkState)
		copy(buffer[len(c.sdkState):], "\000")
		c.sdkState = ""
		return 0
	}

	sdkState := c.saveClientState()
	sdkState.MetaState = c.mw.SaveMetaState()
	sdkState.DataState = c.ec.SaveDataState()
	state, err := json.Marshal(sdkState)
	if err != nil {
		log.LogErrorf("Marshal sdkState err(%v), sdkState(%v)\n", err, sdkState)
		return 0
	}
	c.sdkState = string(state)
	log.LogDebugf("cfs sdkState: %s\n", c.sdkState)
	return C.size_t(len(c.sdkState) + 1)
}

//export cfs_ump
func cfs_ump(id C.int64_t, umpType C.int, sec C.int, nsec C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return
	}
	t := time.Unix(int64(sec), int64(nsec))
	tpObject1 := exporter.NewCustomKeyTPUsWithStartTime(c.umpFunctionKeyFast(int(umpType)), t)
	tpObject2 := exporter.NewCustomKeyTPUsWithStartTime(c.umpFunctionGeneralKeyFast(int(umpType)), t)
	tpObject1.Set(nil)
	tpObject2.Set(nil)
}

/*
 * File operations
 */

//export cfs_close
func cfs_close(id C.int64_t, fd C.int) (re C.int) {
	var (
		path string
		ino  uint64
	)
	defer func() {
		if log.IsDebugEnabled() {
			log.LogDebugf("cfs_close: id(%v) fd(%v) path(%v) ino(%v)", id, fd, path, ino)
		}
	}()
	c, exist := getClient(int64(id))
	if !exist {
		return statusOK
	}
	f := c.releaseFD(uint(fd))
	if f == nil {
		return statusOK
	}
	path = f.path
	ino = f.ino

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_close))
	defer tpObject.Set(nil)

	c.flush(nil, f.ino)
	c.closeStream(f)
	return statusOK
}

//export cfs_open
func cfs_open(id C.int64_t, path *C.char, flags C.int, mode C.mode_t) (re C.int) {
	return _cfs_open(id, path, flags, mode, -1)
}

func _cfs_open(id C.int64_t, path *C.char, flags C.int, mode C.mode_t, fd C.int) (re C.int) {
	var (
		c   *client
		f   *file
		ino uint64
		err error
	)
	defer func() {
		if re < 0 && err == nil {
			err = syscall.Errno(-re)
		}
		r := recover()
		hasErr := r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT))
		if !hasErr && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) path(%v) ino(%v) flags(%v) mode(%v) fd(%v) re(%v) err(%v)", id, C.GoString(path), ino, flags, mode, fd, re, err)
		if hasErr {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_open", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_open: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_open))
	defer tpObject.Set(nil)

	fuseMode := uint32(mode) & uint32(0777)
	fuseFlags := uint32(flags) &^ uint32(0x8000)
	accFlags := fuseFlags & uint32(C.O_ACCMODE)
	absPath := c.absPath(C.GoString(path))

	if fuseFlags&(uint32(C.O_WRONLY)|uint32(C.O_CREAT)) != 0 {
		if c.checkReadOnly(absPath) {
			return statusEPERM
		}
	}

	var info *proto.InodeInfo

	// According to POSIX, flags must include one of the following
	// access modes: O_RDONLY, O_WRONLY, or O_RDWR.
	// But when using glibc, O_CREAT can be used independently (e.g. MySQL).
	if fuseFlags&uint32(C.O_CREAT) != 0 {
		dirpath, name := gopath.Split(absPath)
		dirInode, err := c.lookupPath(nil, dirpath)
		if err != nil {
			return errorToStatus(err)
		}
		if len(name) == 0 {
			return statusEINVAL
		}
		inode, err := c.getDentry(nil, dirInode, name, false)
		var newInfo *proto.InodeInfo
		if err == nil {
			if fuseFlags&uint32(C.O_EXCL) != 0 {
				return statusEEXIST
			} else {
				newInfo, err = c.getInode(nil, inode)
			}
		} else if err == syscall.ENOENT {
			newInfo, err = c.create(nil, dirInode, name, fuseMode, uint32(os.Getuid()), uint32(os.Getgid()), nil)
			if err != nil {
				return errorToStatus(err)
			}
		} else {
			return errorToStatus(err)
		}
		info = newInfo
	} else {
		var newInfo *proto.InodeInfo
		for newInfo, err = c.getInodeByPath(nil, absPath); err == nil && fuseFlags&uint32(C.O_NOFOLLOW) == 0 && proto.IsSymlink(newInfo.Mode); {
			absPath := c.absPath(string(newInfo.Target))
			newInfo, err = c.getInodeByPath(nil, absPath)
		}
		if err != nil {
			return errorToStatus(err)
		}
		info = newInfo
	}

	ino = info.Inode
	f = c.allocFD(info.Inode, fuseFlags, info.Mode, info.Target, int(fd))
	if f == nil {
		return statusEMFILE
	}
	f.size = info.Size
	f.path = absPath

	if proto.IsRegular(info.Mode) {
		c.openInodeStream(f)
		if fuseFlags&uint32(C.O_TRUNC) != 0 {
			if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return statusEACCES
			}
			if err = c.truncate(nil, f.ino, 0); err != nil {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return statusEIO
			}
			info.Size = 0
		}
		c.ec.RefreshExtentsCache(nil, f.ino)
	}
	f.size = info.Size
	f.path = absPath
	return C.int(f.fd)
}

//export cfs_openat
func cfs_openat(id C.int64_t, dirfd C.int, path *C.char, flags C.int, mode C.mode_t) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}

	absPathC := C.CString(absPath)
	re = _cfs_open(id, absPathC, flags, mode, -1)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_openat_fd
func cfs_openat_fd(id C.int64_t, dirfd C.int, path *C.char, flags C.int, mode C.mode_t, fd C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}

	absPathC := C.CString(absPath)
	re = _cfs_open(id, absPathC, flags, mode, fd)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_rename
func cfs_rename(id C.int64_t, from *C.char, to *C.char) (re C.int) {
	var (
		c        *client
		notEvict bool
		err      error
	)
	defer func() {
		r := recover()
		hasErr := r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT))
		if !hasErr && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) from(%v) to(%v) re(%v) err(%v)", id, C.GoString(from), C.GoString(to), re, err)
		if hasErr {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_rename", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_rename: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_rename))
	defer tpObject.Set(nil)

	absFrom := c.absPath(C.GoString(from))
	absTo := c.absPath(C.GoString(to))
	if c.checkReadOnly(absFrom) || c.checkReadOnly(absTo) {
		return statusEPERM
	}
	if absFrom == absTo || strings.HasPrefix(absTo, absFrom+string(os.PathSeparator)) {
		// 不允许源路径和目标路径一样，或将源路径移动到自身子目录的操作
		return statusEINVAL
	}

	srcDirPath, srcName := gopath.Split(absFrom)
	srcDirInode, err := c.lookupPath(nil, srcDirPath)
	if err != nil {
		return errorToStatus(err)
	}
	// mv /d/child /d
	if srcDirPath == (absTo + "/") {
		return statusOK
	}

	c.invalidateDentry(srcDirInode, srcName)
	c.inodeCache.Delete(nil, srcDirInode)
	dstInfo, err := c.getInodeByPath(nil, absTo)
	if err == nil && proto.IsDir(dstInfo.Mode) {
		err = c.mw.Rename_ll(nil, srcDirInode, srcName, dstInfo.Inode, srcName, notEvict)
		if err != nil {
			return errorToStatus(err)
		}
		return statusOK
	}

	dstDirPath, dstName := gopath.Split(absTo)
	dstDirInode, err := c.lookupPath(nil, dstDirPath)
	if err != nil {
		return errorToStatus(err)
	}
	// If dstName exist when renaming, the inode of the dstName will be updated to the inode of the srcName.
	// So, the dstName shuold be invalidated, too,
	c.invalidateDentry(dstDirInode, dstName)
	c.inodeCache.Delete(nil, dstDirInode)
	if dstInfo != nil && c.inodeHasOpenFD(dstInfo.Inode) {
		notEvict = true
	}
	err = c.mw.Rename_ll(nil, srcDirInode, srcName, dstDirInode, dstName, notEvict)
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_renameat
func cfs_renameat(id C.int64_t, fromDirfd C.int, from *C.char, toDirfd C.int, to *C.char) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absFromPath, err := c.absPathAt(fromDirfd, from)
	if err != nil {
		return statusEINVAL
	}

	absToPath, err := c.absPathAt(toDirfd, to)
	if err != nil {
		return statusEINVAL
	}

	absFromPathC := C.CString(absFromPath)
	absToPathC := C.CString(absToPath)
	re = cfs_rename(id, absFromPathC, absToPathC)
	C.free(unsafe.Pointer(absFromPathC))
	C.free(unsafe.Pointer(absToPathC))
	return
}

//export cfs_truncate
func cfs_truncate(id C.int64_t, path *C.char, len C.off_t) (re C.int) {
	var (
		c     *client
		inode uint64
		err   error
	)
	defer func() {
		r := recover()
		hasErr := r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT))
		if !hasErr && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) path(%v) ino(%v) len(%v) re(%v) err(%v)", id, C.GoString(path), inode, len, re, err)
		if hasErr {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_truncate", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_truncate: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_truncate))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	inode, err = c.lookupPath(nil, absPath)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.truncate(nil, inode, uint64(len))
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_ftruncate
func cfs_ftruncate(id C.int64_t, fd C.int, len C.off_t) (re C.int) {
	var (
		c    *client
		path string
		ino  uint64
		err  error
	)
	defer func() {
		r := recover()
		hasErr := r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT))
		if !hasErr && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) fd(%v) path(%v) ino(%v) len(%v) re(%v) err(%v)", id, fd, path, ino, len, re, err)
		if hasErr {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_ftruncate", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_ftruncate: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	path = f.path
	ino = f.ino
	if c.checkReadOnly(path) {
		return statusEPERM
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_ftruncate))
	defer tpObject.Set(nil)

	err = c.truncate(nil, f.ino, uint64(len))
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_fallocate
func cfs_fallocate(id C.int64_t, fd C.int, mode C.int, offset C.off_t, len C.off_t) (re C.int) {
	var (
		c         *client
		path      string
		ino, size uint64
		err       error
	)
	defer func() {
		r := recover()
		if r == nil && re >= 0 && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) fd(%v) path(%v) ino(%v) size(%v) mode(%v) offset(%v) len(%v) re(%v) err(%v)", id, fd, path, ino, size, mode, offset, len, re, err)
		if r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_fallocate", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_fallocate: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	path = f.path
	ino = f.ino
	if c.checkReadOnly(path) {
		return statusEPERM
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_fallocate))
	defer tpObject.Set(nil)

	info, err := c.getInode(nil, f.ino)
	if err != nil {
		return errorToStatus(err)
	}
	size = info.Size

	if uint32(mode) == 0 {
		if uint64(offset+len) <= info.Size {
			return statusOK
		}
	} else if uint32(mode) == uint32(C.FALLOC_FL_KEEP_SIZE) ||
		uint32(mode) == uint32(C.FALLOC_FL_KEEP_SIZE|C.FALLOC_FL_PUNCH_HOLE) {
		// CFS does not support FALLOC_FL_PUNCH_HOLE for now. We cheat here.
		return statusOK
	} else {
		// unimplemented
		return statusEINVAL
	}

	err = c.truncate(nil, info.Inode, uint64(offset+len))
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_posix_fallocate
func cfs_posix_fallocate(id C.int64_t, fd C.int, offset C.off_t, len C.off_t) (re C.int) {
	var (
		c         *client
		path      string
		ino, size uint64
		err       error
	)
	defer func() {
		r := recover()
		if r == nil && re >= 0 && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) fd(%v) path(%v) ino(%v) size(%v) offset(%v) len(%v) re(%v) err(%v)", id, fd, path, ino, size, offset, len, re, err)
		if r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_posix_fallocate", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_posix_fallocate: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	path = f.path
	ino = f.ino
	if c.checkReadOnly(path) {
		return statusEPERM
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_posix_fallocate))
	defer tpObject.Set(nil)

	info, err := c.getInode(nil, f.ino)
	if err != nil {
		return errorToStatus(err)
	}
	size = info.Size

	if uint64(offset+len) <= info.Size {
		return statusOK
	}

	err = c.truncate(nil, info.Inode, uint64(offset+len))
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_flush
func cfs_flush(id C.int64_t, fd C.int) (re C.int) {
	var (
		c     *client
		path  string
		ino   uint64
		err   error
		start time.Time
	)
	defer func() {
		r := recover()
		if r == nil && re >= 0 && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) fd(%v) path(%v) ino(%v) re(%v) err(%v)", id, fd, path, ino, re, err)
		if r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_flush", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_flush: %s time(%v)", msg, time.Since(start).Microseconds())
		}
	}()

	start = time.Now()
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	path = f.path
	ino = f.ino

	if c.checkReadOnly(path) { // read maybe call cfs_flush
		return statusOK
	}
	if !proto.IsRegular(f.mode) {
		// Some application may call fdatasync() after open a directory.
		// In this situation, CFS will do nothing.
		return statusOK
	}

	act := ump_cfs_flush
	if f.fileType == fileTypeRedolog {
		act = ump_cfs_flush_redolog
	} else if f.fileType == fileTypeBinlog {
		act = ump_cfs_flush_binlog
	} else if f.fileType == fileTypeRelaylog {
		act = ump_cfs_flush_relaylog
	}
	tpObject1 := exporter.NewCustomKeyTPUs(c.umpFunctionKeyFast(act))
	tpObject2 := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(act))
	defer func() {
		tpObject1.Set(nil)
		tpObject2.Set(nil)
	}()

	if err = c.flush(nil, f.ino); err != nil {
		return statusEIO
	}
	return statusOK
}

//export cfs_get_file
func cfs_get_file(id C.int64_t, fd C.int, file *C.cfs_file_t) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	file.fd = fd
	file.inode = C.ino_t(f.ino)
	file.flags = C.int(f.flags)
	file.size = C.size_t(f.size)
	file.pos = C.off_t(f.pos)
	file.file_type = C.int(f.fileType)
	file.dup_ref = 1
	return statusOK
}

/*
 * Directory operations
 */

//export cfs_mkdirs
func cfs_mkdirs(id C.int64_t, path *C.char, mode C.mode_t) (re C.int) {
	var (
		c   *client
		err error
	)
	defer func() {
		if r := recover(); r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			msg := fmt.Sprintf("id(%v) path(%v) mode(%v) re(%v) err(%v)%s", id, C.GoString(path), mode, re, err, stack)
			handleError(c, "cfs_mkdirs", msg)
		} else {
			if log.IsDebugEnabled() {
				msg := fmt.Sprintf("id(%v) path(%v) mode(%v) re(%v) err(%v)", id, C.GoString(path), mode, re, err)
				log.LogDebugf("cfs_mkdirs: %s", msg)
			}
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		re = statusEINVAL
		return
	}

	dirpath := c.absPath(C.GoString(path))
	if dirpath == "/" {
		re = statusEEXIST
		return
	}
	if c.checkReadOnly(dirpath) {
		return statusEPERM
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_mkdirs))
	defer tpObject.Set(nil)

	pino := proto.RootIno
	dirs := strings.Split(dirpath, "/")
	fuseMode := uint32(mode)&0777 | uint32(os.ModeDir)
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		var child uint64
		child, err = c.getDentry(nil, pino, dir, true)
		if err != nil && err != syscall.ENOENT {
			re = errorToStatus(err)
			return
		}
		if err == syscall.ENOENT {
			var info *proto.InodeInfo
			info, err = c.create(nil, pino, dir, fuseMode, uid, gid, nil)
			if err != nil && err != syscall.ENOENT {
				re = errorToStatus(err)
				return
			}
			if err == syscall.EEXIST {
				if child, err = c.getDentry(nil, pino, dir, true); err != nil {
					re = errorToStatus(err)
					return
				}
			} else {
				child = info.Inode
			}
		}
		pino = child
	}

	re = statusOK
	return
}

//export cfs_mkdirsat
func cfs_mkdirsat(id C.int64_t, dirfd C.int, path *C.char, mode C.mode_t) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}
	absPathC := C.CString(absPath)
	re = cfs_mkdirs(id, absPathC, mode)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_rmdir
func cfs_rmdir(id C.int64_t, path *C.char) (re C.int) {
	var (
		c   *client
		err error
	)
	defer func() {
		r := recover()
		if r == nil && re < 0 && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) path(%v) re(%v) err(%v)", id, C.GoString(path), re, err)
		if r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_rmdir", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_rmdir: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_rmdir))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	if absPath == "/" {
		return statusOK
	}
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	dirpath, name := gopath.Split(absPath)
	dirInode, err := c.lookupPath(nil, dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	_, err = c.delete(nil, dirInode, name, true)
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_getcwd
func cfs_getcwd(id C.int64_t) *C.char {
	c, exist := getClient(int64(id))
	if !exist {
		return C.CString("")
	}
	return C.CString(c.cwd)
}

//export cfs_chdir
func cfs_chdir(id C.int64_t, path *C.char) (re C.int) {
	var ino uint64
	defer func() {
		if log.IsDebugEnabled() {
			log.LogDebugf("cfs_chdir: id(%v) path(%v) ino(%v) re(%v)", id, C.GoString(path), ino, re)
		}
	}()
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	cwd := c.absPath(C.GoString(path))
	dirInfo, err := c.getInodeByPath(nil, cwd)
	if err != nil {
		return errorToStatus(err)
	}
	ino = dirInfo.Inode
	if !proto.IsDir(dirInfo.Mode) {
		return statusENOTDIR
	}
	c.cwd = cwd
	return statusOK
}

//export cfs_fchdir
func cfs_fchdir(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.int) (re C.int) {
	var (
		path string
		ino  uint64
	)
	defer func() {
		if log.IsDebugEnabled() {
			log.LogDebugf("cfs_fchdir: id(%v) fd(%v) path(%v) ino(%v)", id, fd, path, ino)
		}
	}()
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil || f.path == "" {
		return statusEBADFD
	}
	path = f.path
	ino = f.ino

	if !proto.IsDir(f.mode) {
		return statusENOTDIR
	}

	if int(size) < len(f.path)+1 {
		return statusERANGE
	}

	if buf != nil {
		var buffer []byte
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
		hdr.Data = uintptr(buf)
		hdr.Len = len(f.path) + 1
		hdr.Cap = len(f.path) + 1
		copy(buffer, f.path)
		copy(buffer[len(f.path):], "\000")
	}

	c.cwd = f.path
	return statusOK
}

//export cfs_readdir
func cfs_readdir(id C.int64_t, fd C.int, dirents []C.cfs_dirent_t, count C.int) (n C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return C.int(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.int(statusEBADFD)
	}

	if f.dirp == nil {
		f.dirp = &dirStream{}
		dentries, err := c.mw.ReadDir_ll(context.Background(), f.ino)
		if err != nil {
			return errorToStatus(err)
		}
		f.dirp.dirents = dentries
	}

	dirp := f.dirp
	for dirp.pos < len(dirp.dirents) && n < count {
		// fill up ino
		dirents[n].ino = C.uint64_t(dirp.dirents[dirp.pos].Inode)

		// fill up d_type
		if proto.IsRegular(dirp.dirents[dirp.pos].Type) {
			dirents[n].d_type = C.DT_REG
		} else if proto.IsDir(dirp.dirents[dirp.pos].Type) {
			dirents[n].d_type = C.DT_DIR
		} else if proto.IsSymlink(dirp.dirents[dirp.pos].Type) {
			dirents[n].d_type = C.DT_LNK
		} else {
			dirents[n].d_type = C.DT_UNKNOWN
		}

		// fill up name
		nameLen := len(dirp.dirents[dirp.pos].Name)
		if nameLen >= 256 {
			nameLen = 255
		}
		hdr := (*reflect.StringHeader)(unsafe.Pointer(&dirp.dirents[dirp.pos].Name))
		C.memcpy(unsafe.Pointer(&dirents[n].name[0]), unsafe.Pointer(hdr.Data), C.size_t(nameLen))
		dirents[n].name[nameLen] = 0
		dirents[n].nameLen = C.uint32_t(nameLen)
		// advance cursor
		dirp.pos++
		n++
	}

	return C.int(n)
}

//export cfs_getdents
func cfs_getdents(id C.int64_t, fd C.int, buf unsafe.Pointer, count C.int) (n C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	if f.dirp == nil {
		f.dirp = &dirStream{}
		dentries, err := c.mw.ReadDir_ll(nil, f.ino)
		if err != nil {
			return errorToStatus(err)
		}
		f.dirp.dirents = dentries
	}

	dirp := f.dirp
	var dp *C.struct_dirent
	for dirp.pos < len(dirp.dirents) && n < count {
		// the d_name member in struct dirent is array of length 256,
		// so the bytes beyond 255 are truncated
		nameLen := len(dirp.dirents[dirp.pos].Name)
		if nameLen >= 256 {
			nameLen = 255
		}

		// the file name may be shorter than the predefined d_name
		align := unsafe.Alignof(*(*C.struct_dirent)(nil))
		size := unsafe.Sizeof(*(*C.struct_dirent)(nil)) + uintptr(nameLen+1-256)
		reclen := C.int(math.Ceil(float64(size)/float64(align))) * C.int(align)
		if n+reclen > count {
			if n > 0 {
				return n
			} else {
				return statusEINVAL
			}
		}

		dp = (*C.struct_dirent)(unsafe.Pointer(uintptr(buf) + uintptr(n)))
		dp.d_ino = C.ino_t(dirp.dirents[dirp.pos].Inode)
		// the d_off is an opaque value in modern filesystems
		dp.d_off = 0
		dp.d_reclen = C.ushort(reclen)
		if proto.IsRegular(dirp.dirents[dirp.pos].Type) {
			dp.d_type = C.DT_REG
		} else if proto.IsDir(dirp.dirents[dirp.pos].Type) {
			dp.d_type = C.DT_DIR
		} else if proto.IsSymlink(dirp.dirents[dirp.pos].Type) {
			dp.d_type = C.DT_LNK
		} else {
			dp.d_type = C.DT_UNKNOWN
		}

		hdr := (*reflect.StringHeader)(unsafe.Pointer(&dirp.dirents[dirp.pos].Name))
		C.memcpy(unsafe.Pointer(&dp.d_name), unsafe.Pointer(hdr.Data), C.size_t(nameLen))
		dp.d_name[nameLen] = 0

		// advance cursor
		dirp.pos++
		n += C.int(dp.d_reclen)
	}

	return n
}

/*
 * Link operations
 */

//export cfs_link
func cfs_link(id C.int64_t, oldpath *C.char, newpath *C.char) (re C.int) {
	var (
		c   *client
		err error
	)
	defer func() {
		if r := recover(); r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			msg := fmt.Sprintf("id(%v) oldpath(%v) newpath(%v) re(%v) err(%v)%s", id, C.GoString(oldpath), C.GoString(newpath), re, err, stack)
			handleError(c, "cfs_link", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_link))
	defer tpObject.Set(nil)

	inode, err := c.lookupPath(nil, c.absPath(C.GoString(oldpath)))
	if err != nil {
		return errorToStatus(err)
	}

	absPath := c.absPath(C.GoString(newpath))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	dirPath, name := gopath.Split(absPath)
	dirInode, err := c.lookupPath(nil, dirPath)
	if err != nil {
		return errorToStatus(err)
	}

	_, err = c.mw.Link(nil, dirInode, name, inode)
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_linkat
func cfs_linkat(id C.int64_t, oldDirfd C.int, oldPath *C.char,
	newDirfd C.int, newPath *C.char, flags C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absOldPath, err := c.absPathAt(oldDirfd, oldPath)
	if err != nil {
		return statusEINVAL
	}

	absNewPath, err := c.absPathAt(newDirfd, newPath)
	if err != nil {
		return statusEINVAL
	}
	absOldPathC := C.CString(absOldPath)
	absNewPathC := C.CString(absNewPath)
	re = cfs_link(id, absOldPathC, absNewPathC)
	C.free(unsafe.Pointer(absOldPathC))
	C.free(unsafe.Pointer(absNewPathC))
	return
}

//export cfs_symlink
func cfs_symlink(id C.int64_t, target *C.char, linkPath *C.char) (re C.int) {
	var (
		c   *client
		err error
	)
	defer func() {
		if r := recover(); r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			msg := fmt.Sprintf("id(%v) target(%v) linkPath(%v) re(%v) err(%v)%s", id, C.GoString(target), C.GoString(linkPath), re, err, stack)
			handleError(c, "cfs_symlink", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_symlink))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(linkPath))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	dirpath, name := gopath.Split(absPath)
	dirInode, err := c.lookupPath(nil, dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	_, err = c.getDentry(nil, dirInode, name, false)
	if err == nil {
		return statusEEXIST
	} else if err != syscall.ENOENT {
		return errorToStatus(err)
	}

	_, err = c.create(nil, dirInode, name, proto.Mode(os.ModeSymlink|os.ModePerm), uint32(os.Getuid()), uint32(os.Getgid()), []byte(c.absPath(C.GoString(target))))
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_symlinkat
func cfs_symlinkat(id C.int64_t, target *C.char, dirfd C.int, linkPath *C.char) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, linkPath)
	if err != nil {
		return statusEINVAL
	}
	absPathC := C.CString(absPath)
	re = cfs_symlink(id, target, absPathC)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_unlink
func cfs_unlink(id C.int64_t, path *C.char) (re C.int) {
	var (
		c   *client
		ino uint64
		err error
	)
	defer func() {
		if r := recover(); r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT)) {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			msg := fmt.Sprintf("id(%v) path(%v) ino(%v) re(%v) err(%v)", id, C.GoString(path), ino, re, err)
			handleError(c, "cfs_unlink", fmt.Sprintf("%s%s", msg, stack))
		} else {
			if log.IsDebugEnabled() {
				msg := fmt.Sprintf("id(%v) path(%v) ino(%v) re(%v) err(%v)", id, C.GoString(path), ino, re, err)
				log.LogDebugf("cfs_unlink: %s", msg)
			}
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_unlink))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	info, err := c.getInodeByPath(nil, absPath)
	if err != nil {
		return errorToStatus(err)
	}
	ino = info.Inode
	if proto.IsDir(info.Mode) {
		return statusEPERM
	}

	dirpath, name := gopath.Split(absPath)
	dirInode, err := c.lookupPath(nil, dirpath)
	if err != nil {
		return errorToStatus(err)
	}
	info, err = c.delete(nil, dirInode, name, false)
	if err != nil {
		return errorToStatus(err)
	}
	if info != nil && !c.inodeHasOpenFD(info.Inode) {
		c.mw.Evict(nil, info.Inode, false)
	}
	return 0
}

//export cfs_unlinkat
func cfs_unlinkat(id C.int64_t, dirfd C.int, path *C.char, flags C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}

	absPathC := C.CString(absPath)
	if uint32(flags)&uint32(C.AT_REMOVEDIR) != 0 {
		re = cfs_rmdir(id, absPathC)
	} else {
		re = cfs_unlink(id, absPathC)
	}
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_readlink
func cfs_readlink(id C.int64_t, path *C.char, buf *C.char, size C.size_t) (re C.ssize_t) {
	var (
		c   *client
		ino uint64
		err error
	)
	defer func() {
		msg := fmt.Sprintf("id(%v) path(%v) ino(%v) size(%v) re(%v) err(%v)", id, C.GoString(path), ino, size, re, err)
		if r := recover(); r != nil || (re < 0 && re != C.ssize_t(errorToStatus(syscall.ENOENT)) && re != C.ssize_t(statusEINVAL)) {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_readlink", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_readlink: %s", msg)
		}
	}()

	if int(size) < 0 {
		return C.ssize_t(statusEINVAL)
	}
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_readlink))
	defer tpObject.Set(nil)

	info, err := c.getInodeByPath(nil, c.absPath(C.GoString(path)))
	if err != nil {
		return C.ssize_t(errorToStatus(err))
	}
	if !proto.IsSymlink(info.Mode) {
		return C.ssize_t(statusEINVAL)
	}
	ino = info.Inode

	if len(info.Target) < int(size) {
		size = C.size_t(len(info.Target))
	}
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&info.Target))
	C.memcpy(unsafe.Pointer(buf), unsafe.Pointer(hdr.Data), size)
	return C.ssize_t(size)
}

//export cfs_readlinkat
func cfs_readlinkat(id C.int64_t, dirfd C.int, path *C.char, buf *C.char, size C.size_t) (re C.ssize_t) {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return C.ssize_t(statusEINVAL)
	}
	absPathC := C.CString(absPath)
	re = cfs_readlink(id, absPathC, buf, size)
	C.free(unsafe.Pointer(absPathC))
	return
}

/*
 * Basic file attributes
 */

/*
 * Although there is no device belonging to CFS, value of stat.st_dev MUST be set.
 * Sometimes, this value may be used to determine the identity of a file.
 * (e.g. in Mysql initialization stage, storage\myisam\mi_open.c
 * mi_open_share() -> my_is_same_file())
 */

//export cfs_stat
func cfs_stat(id C.int64_t, path *C.char, stat *C.struct_stat) C.int {
	return _cfs_stat(id, path, stat, 0)
}

func _cfs_stat(id C.int64_t, path *C.char, stat *C.struct_stat, flags C.int) (re C.int) {
	var (
		c         *client
		ino, size uint64
		err       error
	)
	defer func() {
		r := recover()
		hasErr := r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT))
		if !hasErr && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) path(%v) ino(%v) flags(%v) size(%v) re(%v) err(%v)", id, C.GoString(path), ino, flags, size, re, err)
		if hasErr {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_stat", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_stat: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_stat))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	var info *proto.InodeInfo
	for info, err = c.getInodeByPath(nil, absPath); err == nil && (uint32(flags)&uint32(C.AT_SYMLINK_NOFOLLOW) == 0) && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}
	ino = info.Inode
	size = info.Size

	// fill up the stat
	stat.st_dev = 0
	stat.st_ino = C.ino_t(info.Inode)
	stat.st_size = C.off_t(info.Size)
	stat.st_nlink = C.nlink_t(info.Nlink)
	stat.st_blksize = C.blksize_t(defaultBlkSize)
	stat.st_uid = C.uid_t(info.Uid)
	stat.st_gid = C.gid_t(info.Gid)

	if info.Size%512 != 0 {
		stat.st_blocks = C.blkcnt_t(info.Size>>9) + 1
	} else {
		stat.st_blocks = C.blkcnt_t(info.Size >> 9)
	}
	// fill up the mode
	if proto.IsRegular(info.Mode) {
		stat.st_mode = C.mode_t(C.S_IFREG) | C.mode_t(info.Mode&0777)
	} else if proto.IsDir(info.Mode) {
		stat.st_mode = C.mode_t(C.S_IFDIR) | C.mode_t(info.Mode&0777)
	} else if proto.IsSymlink(info.Mode) {
		stat.st_mode = C.mode_t(C.S_IFLNK) | C.mode_t(info.Mode&0777)
	} else {
		stat.st_mode = C.mode_t(C.S_IFSOCK) | C.mode_t(info.Mode&0777)
	}

	// fill up the time struct
	var st_atim, st_mtim, st_ctim C.struct_timespec
	t := info.AccessTime.UnixNano()
	st_atim.tv_sec = C.time_t(t / 1e9)
	st_atim.tv_nsec = C.long(t % 1e9)
	stat.st_atim = st_atim

	t = info.ModifyTime.UnixNano()
	st_mtim.tv_sec = C.time_t(t / 1e9)
	st_mtim.tv_nsec = C.long(t % 1e9)
	stat.st_mtim = st_mtim

	t = info.CreateTime.UnixNano()
	st_ctim.tv_sec = C.time_t(t / 1e9)
	st_ctim.tv_nsec = C.long(t % 1e9)
	stat.st_ctim = st_ctim
	return statusOK
}

//export cfs_stat64
func cfs_stat64(id C.int64_t, path *C.char, stat *C.struct_stat64) C.int {
	return _cfs_stat64(id, path, stat, 0)
}

func _cfs_stat64(id C.int64_t, path *C.char, stat *C.struct_stat64, flags C.int) (re C.int) {
	var (
		c         *client
		ino, size uint64
		err       error
	)
	defer func() {
		r := recover()
		hasErr := r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT))
		if !hasErr && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) path(%v) ino(%v) flags(%v) size(%v) re(%v) err(%v)", id, C.GoString(path), ino, flags, size, re, err)
		if hasErr {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_stat64", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_stat64: %s", msg)
		}
	}()

	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_stat64))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	var info *proto.InodeInfo
	for info, err = c.getInodeByPath(nil, absPath); err == nil && (uint32(flags)&uint32(C.AT_SYMLINK_NOFOLLOW) == 0) && proto.IsSymlink(info.Mode); {
		absPath = c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}
	ino = info.Inode
	size = info.Size

	// fill up the stat
	stat.st_dev = 0
	stat.st_ino = C.ino64_t(info.Inode)
	stat.st_size = C.off64_t(info.Size)
	stat.st_nlink = C.nlink_t(info.Nlink)
	stat.st_blksize = C.blksize_t(defaultBlkSize)
	stat.st_uid = C.uid_t(info.Uid)
	stat.st_gid = C.gid_t(info.Gid)

	if info.Size%512 != 0 {
		stat.st_blocks = C.blkcnt64_t(info.Size>>9) + 1
	} else {
		stat.st_blocks = C.blkcnt64_t(info.Size >> 9)
	}
	// fill up the mode
	if proto.IsRegular(info.Mode) {
		stat.st_mode = C.mode_t(C.S_IFREG) | C.mode_t(info.Mode&0777)
	} else if proto.IsDir(info.Mode) {
		stat.st_mode = C.mode_t(C.S_IFDIR) | C.mode_t(info.Mode&0777)
	} else if proto.IsSymlink(info.Mode) {
		stat.st_mode = C.mode_t(C.S_IFLNK) | C.mode_t(info.Mode&0777)
	} else {
		stat.st_mode = C.mode_t(C.S_IFSOCK) | C.mode_t(info.Mode&0777)
	}

	// fill up the time struct
	var st_atim, st_mtim, st_ctim C.struct_timespec
	t := info.AccessTime.UnixNano()
	st_atim.tv_sec = C.time_t(t / 1e9)
	st_atim.tv_nsec = C.long(t % 1e9)
	stat.st_atim = st_atim

	t = info.ModifyTime.UnixNano()
	st_mtim.tv_sec = C.time_t(t / 1e9)
	st_mtim.tv_nsec = C.long(t % 1e9)
	stat.st_mtim = st_mtim

	t = info.CreateTime.UnixNano()
	st_ctim.tv_sec = C.time_t(t / 1e9)
	st_ctim.tv_nsec = C.long(t % 1e9)
	stat.st_ctim = st_ctim
	return statusOK
}

//export cfs_lstat
func cfs_lstat(id C.int64_t, path *C.char, stat *C.struct_stat) C.int {
	return _cfs_stat(id, path, stat, C.AT_SYMLINK_NOFOLLOW)
}

//export cfs_lstat64
func cfs_lstat64(id C.int64_t, path *C.char, stat *C.struct_stat64) C.int {
	return _cfs_stat64(id, path, stat, C.AT_SYMLINK_NOFOLLOW)
}

//export cfs_fstat
func cfs_fstat(id C.int64_t, fd C.int, stat *C.struct_stat) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	pathC := C.CString(f.path)
	re = _cfs_stat(id, pathC, stat, 0)
	C.free(unsafe.Pointer(pathC))
	return
}

//export cfs_fstat64
func cfs_fstat64(id C.int64_t, fd C.int, stat *C.struct_stat64) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	pathC := C.CString(f.path)
	re = _cfs_stat64(id, pathC, stat, 0)
	C.free(unsafe.Pointer(pathC))
	return
}

//export cfs_fstatat
func cfs_fstatat(id C.int64_t, dirfd C.int, path *C.char, stat *C.struct_stat, flags C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}
	absPathC := C.CString(absPath)
	re = _cfs_stat(id, absPathC, stat, flags)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_fstatat64
func cfs_fstatat64(id C.int64_t, dirfd C.int, path *C.char, stat *C.struct_stat64, flags C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}
	absPathC := C.CString(absPath)
	re = _cfs_stat64(id, absPathC, stat, flags)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_chmod
func cfs_chmod(id C.int64_t, path *C.char, mode C.mode_t) C.int {
	return _cfs_chmod(id, path, mode, 0)
}

func _cfs_chmod(id C.int64_t, path *C.char, mode C.mode_t, flags C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_chmod))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	var info *proto.InodeInfo
	var err error
	for info, err = c.getInodeByPath(nil, absPath); err == nil && (uint32(flags)&uint32(C.AT_SYMLINK_NOFOLLOW) == 0) && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(nil, info, proto.AttrMode, uint32(mode), 0, 0, 0, 0)
	if err != nil {
		return errorToStatus(err)
	}
	fuseMode := uint32(mode) & uint32(0777)
	info.Mode = info.Mode &^ uint32(0777) // clear rwx mode bit
	info.Mode |= fuseMode
	c.inodeCache.Put(info)
	return statusOK
}

//export cfs_fchmod
func cfs_fchmod(id C.int64_t, fd C.int, mode C.mode_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	if c.checkReadOnly(f.path) {
		return statusEPERM
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_fchmod))
	defer tpObject.Set(nil)

	info, err := c.getInode(nil, f.ino)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(nil, info, proto.AttrMode, uint32(mode), 0, 0, 0, 0)
	if err != nil {
		return errorToStatus(err)
	}
	fuseMode := uint32(mode) & uint32(0777)
	info.Mode = info.Mode &^ uint32(0777) // clear rwx mode bit
	info.Mode |= fuseMode
	c.inodeCache.Put(info)
	return statusOK
}

//export cfs_fchmodat
func cfs_fchmodat(id C.int64_t, dirfd C.int, path *C.char, mode C.mode_t, flags C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}
	absPathC := C.CString(absPath)
	re = _cfs_chmod(id, absPathC, mode, flags)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_chown
func cfs_chown(id C.int64_t, path *C.char, uid C.uid_t, gid C.gid_t) C.int {
	return _cfs_chown(id, path, uid, gid, 0)
}

//export cfs_lchown
func cfs_lchown(id C.int64_t, path *C.char, uid C.uid_t, gid C.gid_t) C.int {
	return _cfs_chown(id, path, uid, gid, C.AT_SYMLINK_NOFOLLOW)
}

func _cfs_chown(id C.int64_t, path *C.char, uid C.uid_t, gid C.gid_t, flags C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_chown))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	var info *proto.InodeInfo
	var err error
	for info, err = c.getInodeByPath(nil, absPath); err == nil && (uint32(flags)&uint32(C.AT_SYMLINK_NOFOLLOW) == 0) && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(nil, info, proto.AttrUid|proto.AttrGid, 0, uint32(uid), uint32(gid), 0, 0)
	if err != nil {
		return errorToStatus(err)
	}
	info.Uid = uint32(uid)
	info.Gid = uint32(gid)
	c.inodeCache.Put(info)
	return statusOK
}

//export cfs_fchown
func cfs_fchown(id C.int64_t, fd C.int, uid C.uid_t, gid C.gid_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	if c.checkReadOnly(f.path) {
		return statusEPERM
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_fchown))
	defer tpObject.Set(nil)

	info, err := c.getInode(nil, f.ino)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(nil, info, proto.AttrUid|proto.AttrGid, 0, uint32(uid), uint32(gid), 0, 0)
	if err != nil {
		return errorToStatus(err)
	}
	info.Uid = uint32(uid)
	info.Gid = uint32(gid)
	c.inodeCache.Put(info)
	return statusOK
}

//export cfs_fchownat
func cfs_fchownat(id C.int64_t, dirfd C.int, path *C.char, uid C.uid_t, gid C.gid_t, flags C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}
	absPathC := C.CString(absPath)
	re = _cfs_chown(id, absPathC, uid, gid, flags)
	C.free(unsafe.Pointer(absPathC))
	return
}

//export cfs_utimens
func cfs_utimens(id C.int64_t, path *C.char, times *C.struct_timespec, flags C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_utimens))
	defer tpObject.Set(nil)

	absPath := c.absPath(C.GoString(path))
	var info *proto.InodeInfo
	var err error
	for info, err = c.getInodeByPath(nil, absPath); err == nil && (uint32(flags)&uint32(C.AT_SYMLINK_NOFOLLOW) == 0) && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}

	var atime, mtime int64
	var ap, mp *C.struct_timespec
	var ts C.struct_timespec
	ap = times
	mp = (*C.struct_timespec)(unsafe.Pointer(uintptr(unsafe.Pointer(times)) + unsafe.Sizeof(ts)))
	// CFS time precision is second
	now := time.Now().Unix()
	if times == nil {
		atime = now
	} else if ap.tv_nsec == C.UTIME_NOW {
		atime = now
	} else if ap.tv_nsec == C.UTIME_OMIT {
		atime = 0
	} else {
		atime = int64(ap.tv_sec)
	}
	if times == nil {
		mtime = now
	} else if mp.tv_nsec == C.UTIME_NOW {
		mtime = now
	} else if mp.tv_nsec == C.UTIME_OMIT {
		mtime = 0
	} else {
		mtime = int64(mp.tv_sec)
	}
	err = c.setattr(nil, info, proto.AttrAccessTime|proto.AttrModifyTime, 0, 0, 0, mtime, atime)
	if err != nil {
		return errorToStatus(err)
	}
	info.AccessTime = time.Unix(atime, 0)
	info.ModifyTime = time.Unix(mtime, 0)
	c.inodeCache.Put(info)

	return statusOK
}

//export cfs_futimens
func cfs_futimens(id C.int64_t, fd C.int, times *C.struct_timespec) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	pathC := C.CString(f.path)
	re = cfs_utimens(id, pathC, times, 0)
	C.free(unsafe.Pointer(pathC))
	return
}

//export cfs_utimensat
func cfs_utimensat(id C.int64_t, dirfd C.int, path *C.char, times *C.struct_timespec, flags C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}

	absPathC := C.CString(absPath)
	re = cfs_utimens(id, absPathC, times, flags)
	C.free(unsafe.Pointer(absPathC))
	return
}

/*
 * In access like functiuons, permission check is ignored, only existence check
 * is done. The responsibility of file permissions is left to upper applications.
 */

//export cfs_access
func cfs_access(id C.int64_t, path *C.char, mode C.int) C.int {
	return cfs_faccessat(id, C.AT_FDCWD, path, mode, 0)
}

//export cfs_faccessat
func cfs_faccessat(id C.int64_t, dirfd C.int, path *C.char, mode C.int, flags C.int) (re C.int) {
	var (
		c   *client
		err error
	)
	defer func() {
		if r := recover(); r != nil || (re < 0 && re != errorToStatus(syscall.ENOENT)) {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			msg := fmt.Sprintf("id(%v) dirfd(%v) path(%v) mode(%v) flags(%v) re(%v) err(%v)%s", id, dirfd, C.GoString(path), mode, flags, re, err, stack)
			handleError(c, "cfs_faccessat", msg)
		}
	}()

	re = statusOK
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	tpObject := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(ump_cfs_faccessat))
	defer tpObject.Set(nil)

	absPath, err := c.absPathAt(dirfd, path)
	if err != nil {
		return statusEINVAL
	}
	inode, err := c.lookupPath(nil, absPath)
	var info *proto.InodeInfo
	for err == nil && (uint32(flags)&uint32(C.AT_SYMLINK_NOFOLLOW) == 0) {
		info, err = c.getInode(nil, inode)
		if err != nil {
			return errorToStatus(err)
		}
		if !proto.IsSymlink(info.Mode) {
			break
		}
		absPath = c.absPath(string(info.Target))
		inode, err = c.lookupPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

/*
 * Extended file attributes
 */

//export cfs_setxattr
func cfs_setxattr(id C.int64_t, path *C.char, name *C.char, value unsafe.Pointer, size C.size_t, flags C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	var info *proto.InodeInfo
	var err error
	for info, err = c.getInodeByPath(nil, absPath); err == nil && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(value)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	err = c.mw.XAttrSet_ll(nil, info.Inode, []byte(C.GoString(name)), buffer)
	if err != nil {
		return statusEIO
	}

	return statusOK
}

//export cfs_lsetxattr
func cfs_lsetxattr(id C.int64_t, path *C.char, name *C.char, value unsafe.Pointer, size C.size_t, flags C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}

	inode, err := c.lookupPath(nil, absPath)
	if err != nil {
		return errorToStatus(err)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(value)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	err = c.mw.XAttrSet_ll(nil, inode, []byte(C.GoString(name)), buffer)
	if err != nil {
		return statusEIO
	}

	return statusOK
}

//export cfs_fsetxattr
func cfs_fsetxattr(id C.int64_t, fd C.int, name *C.char, value unsafe.Pointer, size C.size_t, flags C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	if c.checkReadOnly(f.path) {
		return statusEPERM
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(value)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	err := c.mw.XAttrSet_ll(nil, f.ino, []byte(C.GoString(name)), buffer)
	if err != nil {
		return statusEIO
	}

	return statusOK
}

//export cfs_getxattr
func cfs_getxattr(id C.int64_t, path *C.char, name *C.char, value unsafe.Pointer, size C.size_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	absPath := c.absPath(C.GoString(path))
	var info *proto.InodeInfo
	var err error
	for info, err = c.getInodeByPath(nil, absPath); err == nil && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return C.ssize_t(errorToStatus(err))
	}

	xattr, err := c.mw.XAttrGet_ll(nil, info.Inode, C.GoString(name))
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	val, ok := xattr.XAttrs[C.GoString(name)]
	if !ok {
		return C.ssize_t(statusENODATA)
	}

	if int(size) == 0 {
		return C.ssize_t(len(val))
	} else if int(size) < len(val) {
		return C.ssize_t(statusERANGE)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(value)
	hdr.Len = int(size)
	hdr.Cap = int(size)
	copy(buffer, val)

	return C.ssize_t(len(val))
}

//export cfs_lgetxattr
func cfs_lgetxattr(id C.int64_t, path *C.char, name *C.char, value unsafe.Pointer, size C.size_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	absPath := c.absPath(C.GoString(path))
	inode, err := c.lookupPath(nil, absPath)
	if err != nil {
		return C.ssize_t(errorToStatus(err))
	}
	xattr, err := c.mw.XAttrGet_ll(nil, inode, C.GoString(name))
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	val, ok := xattr.XAttrs[C.GoString(name)]
	if !ok {
		return C.ssize_t(statusENODATA)
	}

	if int(size) == 0 {
		return C.ssize_t(len(val))
	} else if int(size) < len(val) {
		return C.ssize_t(statusERANGE)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(value)
	hdr.Len = int(size)
	hdr.Cap = int(size)
	copy(buffer, val)

	return C.ssize_t(len(val))
}

//export cfs_fgetxattr
func cfs_fgetxattr(id C.int64_t, fd C.int, name *C.char, value unsafe.Pointer, size C.size_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}

	xattr, err := c.mw.XAttrGet_ll(nil, f.ino, C.GoString(name))
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	val, ok := xattr.XAttrs[C.GoString(name)]
	if !ok {
		return C.ssize_t(statusENODATA)
	}

	if int(size) == 0 {
		return C.ssize_t(len(val))
	} else if int(size) < len(val) {
		return C.ssize_t(statusERANGE)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(value)
	hdr.Len = int(size)
	hdr.Cap = int(size)
	copy(buffer, val)

	return C.ssize_t(len(val))
}

//export cfs_listxattr
func cfs_listxattr(id C.int64_t, path *C.char, list *C.char, size C.size_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	absPath := c.absPath(C.GoString(path))
	var info *proto.InodeInfo
	var err error
	for info, err = c.getInodeByPath(nil, absPath); err == nil && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return C.ssize_t(errorToStatus(err))
	}

	names, err := c.mw.XAttrsList_ll(nil, info.Inode)
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	total := 0
	for _, val := range names {
		total += len(val) + 1
	}
	if int(size) == 0 {
		return C.ssize_t(total)
	} else if int(size) < total {
		return C.ssize_t(statusERANGE)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(unsafe.Pointer(list))
	hdr.Len = int(size)
	hdr.Cap = int(size)
	offset := 0
	for _, val := range names {
		copy(buffer[offset:], val)
		offset += len(val)
		copy(buffer[offset:], "\x00")
		offset += 1
	}

	return C.ssize_t(total)
}

//export cfs_llistxattr
func cfs_llistxattr(id C.int64_t, path *C.char, list *C.char, size C.size_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	absPath := c.absPath(C.GoString(path))
	inode, err := c.lookupPath(nil, absPath)
	if err != nil {
		return C.ssize_t(errorToStatus(err))
	}
	names, err := c.mw.XAttrsList_ll(nil, inode)
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	total := 0
	for _, val := range names {
		total += len(val) + 1
	}
	if int(size) == 0 {
		return C.ssize_t(total)
	} else if int(size) < total {
		return C.ssize_t(statusERANGE)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(unsafe.Pointer(list))
	hdr.Len = int(size)
	hdr.Cap = int(size)
	offset := 0
	for _, val := range names {
		copy(buffer[offset:], val)
		offset += len(val)
		copy(buffer[offset:], "\x00")
		offset += 1
	}

	return C.ssize_t(total)
}

//export cfs_flistxattr
func cfs_flistxattr(id C.int64_t, fd C.int, list *C.char, size C.size_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}

	info, err := c.getInode(context.Background(), f.ino)
	if err != nil {
		return C.ssize_t(errorToStatus(err))
	}

	names, err := c.mw.XAttrsList_ll(context.Background(), info.Inode)
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	total := 0
	for _, val := range names {
		total += len(val) + 1
	}
	if int(size) == 0 {
		return C.ssize_t(total)
	} else if int(size) < total {
		return C.ssize_t(statusERANGE)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(unsafe.Pointer(list))
	hdr.Len = int(size)
	hdr.Cap = int(size)
	offset := 0
	for _, val := range names {
		copy(buffer[offset:], val)
		offset += len(val)
		copy(buffer[offset:], "\x00")
		offset += 1
	}

	return C.ssize_t(total)
}

//export cfs_removexattr
func cfs_removexattr(id C.int64_t, path *C.char, name *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	var info *proto.InodeInfo
	var err error
	for info, err = c.getInodeByPath(nil, absPath); err == nil && proto.IsSymlink(info.Mode); {
		absPath := c.absPath(string(info.Target))
		info, err = c.getInodeByPath(nil, absPath)
	}
	if err != nil {
		return errorToStatus(err)
	}

	err = c.mw.XAttrDel_ll(context.Background(), info.Inode, C.GoString(name))
	if err != nil {
		return statusEIO
	}

	return statusOK
}

//export cfs_lremovexattr
func cfs_lremovexattr(id C.int64_t, path *C.char, name *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(C.GoString(path))
	if c.checkReadOnly(absPath) {
		return statusEPERM
	}
	inode, err := c.lookupPath(context.Background(), absPath)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.mw.XAttrDel_ll(context.Background(), inode, C.GoString(name))
	if err != nil {
		return statusEIO
	}

	return statusOK
}

//export cfs_fremovexattr
func cfs_fremovexattr(id C.int64_t, fd C.int, name *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	if c.checkReadOnly(f.path) {
		return statusEPERM
	}

	err := c.mw.XAttrDel_ll(context.Background(), f.ino, C.GoString(name))
	if err != nil {
		return statusEIO
	}

	return statusOK
}

/*
 * File descriptor manipulations
 */

//export cfs_fcntl
func cfs_fcntl(id C.int64_t, fd C.int, cmd C.int, arg C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	if cmd == C.F_DUPFD || cmd == C.F_DUPFD_CLOEXEC {
		newfd := c.copyFile(uint(fd), uint(arg))
		if newfd == 0 {
			return statusEINVAL
		}
		return C.int(newfd)
	} else if cmd == C.F_SETFL {
		// According to POSIX, F_SETFL will replace the flags with exactly
		// the provided, i.e. someone should call F_GETFL before F_SETFL.
		// But some applications (e.g. Mysql) don't call F_GETFL before F_SETFL.
		// We compromise with such applications here.
		f.flags |= uint32(arg) & uint32((C.O_APPEND | C.O_ASYNC | C.O_DIRECT | C.O_NOATIME | C.O_NONBLOCK))
		return statusOK
	}

	// unimplemented
	return statusEINVAL
}

//export cfs_fcntl_lock
func cfs_fcntl_lock(id C.int64_t, fd C.int, cmd C.int, lk *C.struct_flock) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	if (cmd == C.F_SETLK || cmd == C.F_SETLKW) && lk.l_whence == C.SEEK_SET && lk.l_start == 0 && lk.l_len == 0 {
		if lk.l_type == C.F_WRLCK {
			f.mu.Lock()
			f.locked = true
		} else if lk.l_type == C.F_UNLCK {
			f.mu.Unlock()
			f.locked = false
		} else {
			return statusEINVAL
		}
		return statusOK
	}

	// unimplemented
	return statusEINVAL
}

/*
 * Read & Write
 */

/*
 * https://man7.org/linux/man-pages/man2/pwrite.2.html
 * POSIX requires that opening a file with the O_APPEND flag should have
 * no effect on the location at which pwrite() writes data.  However, on
 * Linux, if a file is opened with O_APPEND, pwrite() appends data to
 * the end of the file, regardless of the value of offset.
 *
 * CFS complies with POSIX
 */

//export cfs_read
func cfs_read(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t) C.ssize_t {
	return _cfs_read(id, fd, buf, size, C.off_t(autoOffset))
}

//export cfs_pread
func cfs_pread(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) C.ssize_t {
	return _cfs_read(id, fd, buf, size, off)
}

//export cfs_read_requests
func cfs_read_requests(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t, requests unsafe.Pointer, req_count C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	// off >= 0 stands for pread
	offset := uint64(off)
	if off < 0 {
		offset = f.pos
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("cfs_read_requests read: id(%v) fd(%v) path(%v) ino(%v) size(%v) offset(%v)", id, fd, f.path, f.ino, len(buffer), offset)
	}
	readRequests, fileSize, err := c.ec.GetReadRequests(nil, f.ino, buffer, offset, len(buffer))
	if err != nil {
		return statusEIO
	}
	if len(readRequests) > int(req_count) {
		return -C.int(len(readRequests))
	}

	var goRequests []C.cfs_read_req_t
	hdrReq := (*reflect.SliceHeader)(unsafe.Pointer(&goRequests))
	hdrReq.Data = uintptr(requests)
	hdrReq.Len, hdrReq.Cap = int(req_count), int(req_count)

	for i, readReq := range readRequests {
		if log.IsDebugEnabled() {
			log.LogDebugf("cfs_read_requests read: index(%v) req(%v) id(%v) fd(%v) path(%v) ino(%v) size(%v) offset(%v)", i, readReq, id, fd, f.path, f.ino, len(buffer), offset)
		}
		goRequests[i].file_offset = C.off_t(readReq.Req.FileOffset)
		goRequests[i].size = C.size_t(readReq.Req.Size)
		if readReq.Req.ExtentKey == nil {
			// hole
			goRequests[i].partition_id = C.uint64_t(0)
			goRequests[i].extent_id = C.uint64_t(0)
			goRequests[i].extent_offset = C.uint64_t(0)
			if readReq.Req.FileOffset+uint64(readReq.Req.Size) > fileSize {
				if readReq.Req.FileOffset >= fileSize {
					goRequests[i].size = C.size_t(0)
				} else {
					goRequests[i].size = C.size_t(fileSize - readReq.Req.FileOffset)
				}
				return C.int(i + 1)
			}
		} else {
			goRequests[i].partition_id = C.uint64_t(readReq.Req.ExtentKey.PartitionId)
			goRequests[i].extent_id = C.uint64_t(readReq.Req.ExtentKey.ExtentId)
			goRequests[i].extent_offset = C.uint64_t(uint64(readReq.Req.FileOffset) - readReq.Req.ExtentKey.FileOffset + readReq.Req.ExtentKey.ExtentOffset)
			leaderAddr := readReq.Partition.GetLeaderAddr()
			if leaderAddr != "" {
				addrArr := strings.Split(leaderAddr, ":")
				// max length of dp_host is 32, defined in cfs_read_req_t
				hdr := (*reflect.StringHeader)(unsafe.Pointer(&addrArr[0]))
				addrLen := len(addrArr[0])
				if addrLen > 32-1 {
					addrLen = 32 - 1
				}
				C.memcpy(unsafe.Pointer(&goRequests[i].dp_host), unsafe.Pointer(hdr.Data), C.size_t(addrLen))
				goRequests[i].dp_host[addrLen] = 0
				port, _ := strconv.ParseInt(addrArr[1], 10, 64)
				goRequests[i].dp_port = C.int(port)
			}
		}
	}
	return C.int(len(readRequests))
}

//export cfs_refresh_eks
func cfs_refresh_eks(id C.int64_t, ino C.ino_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}
	err := c.ec.RefreshExtentsCache(nil, uint64(ino))
	if err != nil {
		return C.ssize_t(statusEINVAL)
	}
	size, _, valid := c.ec.FileSize(uint64(ino))
	if !valid {
		return C.ssize_t(statusEBADFD)
	}
	return C.ssize_t(size)
}

func _cfs_read(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) (re C.ssize_t) {
	var (
		c      *client
		path   string
		ino    uint64
		err    error
		offset uint64
		start  time.Time
	)
	defer func() {
		r := recover()
		if r == nil && re >= 0 && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) fd(%v) path(%v) ino(%v) size(%v) offset(%v) re(%v) err(%v)", id, fd, path, ino, size, offset, re, err)
		if r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_read", fmt.Sprintf("%s%s", msg, stack))
		} else {
			log.LogDebugf("cfs_read: %s time(%v)", msg, time.Since(start).Microseconds())
		}
	}()

	start = time.Now()
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}
	path = f.path
	ino = f.ino
	if off < 0 && off != C.off_t(autoOffset) {
		return C.ssize_t(statusEINVAL)
	}

	act := ump_cfs_read
	if f.fileType == fileTypeBinlog {
		act = ump_cfs_read_binlog
	} else if f.fileType == fileTypeRelaylog {
		act = ump_cfs_read_relaylog
	}
	tpObject1 := exporter.NewCustomKeyTPUs(c.umpFunctionKeyFast(act))
	tpObject2 := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(act))
	defer func() {
		tpObject1.Set(nil)
		tpObject2.Set(nil)
	}()

	accFlags := f.flags & uint32(C.O_ACCMODE)
	if accFlags == uint32(C.O_WRONLY) {
		return C.ssize_t(statusEACCES)
	}

	var buffer []byte

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	// off >= 0 stands for pread
	offset = uint64(off)
	if off < 0 {
		offset = f.pos
	}
	n, hasHole, err := c.ec.Read(nil, f.ino, buffer, offset, len(buffer))
	if err != nil && err != io.EOF {
		return C.ssize_t(statusEIO)
	}
	if n < int(size) || hasHole {
		c.flush(nil, f.ino)
		c.ec.RefreshExtentsCache(nil, f.ino)
		n, _, err = c.ec.Read(nil, f.ino, buffer, offset, len(buffer))
	}
	if err != nil && err != io.EOF {
		return C.ssize_t(statusEIO)
	}
	if err != nil && err != io.EOF {
		return C.ssize_t(statusEIO)
	}

	if off < 0 {
		f.pos += uint64(n)
	}
	return C.ssize_t(n)
}

//export cfs_readv
func cfs_readv(id C.int64_t, fd C.int, iov *C.struct_iovec, iovcnt C.int) C.ssize_t {
	return _cfs_readv(id, fd, iov, iovcnt, -1)
}

//export cfs_preadv
func cfs_preadv(id C.int64_t, fd C.int, iov *C.struct_iovec, iovcnt C.int, off C.off_t) C.ssize_t {
	return _cfs_readv(id, fd, iov, iovcnt, off)
}

func _cfs_readv(id C.int64_t, fd C.int, iov *C.struct_iovec, iovcnt C.int, off C.off_t) C.ssize_t {
	var iovbuf []C.struct_iovec
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&iovbuf))
	hdr.Data = uintptr(unsafe.Pointer(iov))
	hdr.Len = int(iovcnt)
	hdr.Cap = int(iovcnt)

	var size int
	for i := 0; i < int(iovcnt); i++ {
		size += int(iovbuf[i].iov_len)
	}
	buffer := make([]byte, size, size)
	hdr = (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	ssize := _cfs_read(id, fd, unsafe.Pointer(hdr.Data), C.size_t(size), off)
	if ssize < 0 {
		return ssize
	}

	var buf []byte
	var offset int
	for i := 0; i < int(iovcnt); i++ {
		buf = make([]byte, iovbuf[i].iov_len, iovbuf[i].iov_len)
		hdr = (*reflect.SliceHeader)(unsafe.Pointer(&buf))
		hdr.Data = uintptr(iovbuf[i].iov_base)
		copy(buf, buffer[offset:offset+int(iovbuf[i].iov_len)])
		offset += int(iovbuf[i].iov_len)
	}
	return ssize
}

//export cfs_write
func cfs_write(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t) C.ssize_t {
	return _cfs_write(id, fd, buf, size, C.off_t(autoOffset))
}

//export cfs_pwrite
func cfs_pwrite(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) C.ssize_t {
	return _cfs_write(id, fd, buf, size, off)
}

func _cfs_write(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) (re C.ssize_t) {
	var (
		c       *client
		f       *file
		path    string
		ino     uint64
		err     error
		offset  uint64
		flagBuf bytes.Buffer
		start   time.Time
	)
	defer func() {
		var fileSize uint64 = 0
		if f != nil {
			fileSize = f.size
		}
		r := recover()
		if r == nil && re == C.ssize_t(size) && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) fd(%v) path(%v) ino(%v) size(%v) offset(%v) flag(%v) fileSize(%v) re(%v) err(%v)", id, fd, path, ino, size, offset, strings.Trim(flagBuf.String(), "|"), fileSize, re, err)
		if r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_write", fmt.Sprintf("%s%s", msg, stack))
		} else if re < C.ssize_t(size) {
			log.LogWarnf("cfs_write: %s", msg)
		} else {
			log.LogDebugf("cfs_write: %s time(%v)", msg, time.Since(start).Microseconds())
		}
	}()

	signalIgnoreOnce.Do(signalIgnoreFunc)

	start = time.Now()
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f = c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}
	path = f.path
	ino = f.ino
	if c.checkReadOnly(path) {
		return C.ssize_t(statusEPERM)
	}
	if log.IsDebugEnabled() {
		if f.flags&uint32(C.O_DIRECT) != 0 {
			flagBuf.WriteString("O_DIRECT|")
		} else if f.flags&uint32(C.O_SYNC) != 0 {
			flagBuf.WriteString("O_SYNC|")
		} else if f.flags&uint32(C.O_DSYNC) != 0 {
			flagBuf.WriteString("O_DSYNC|")
		}
	}
	overWriteBuffer := false
	act := ump_cfs_write
	if f.fileType == fileTypeBinlog {
		act = ump_cfs_write_binlog
	} else if f.fileType == fileTypeRelaylog {
		act = ump_cfs_write_relaylog
	} else if f.fileType == fileTypeRedolog {
		act = ump_cfs_write_redolog
		if c.app == appMysql8 || c.app == appCoralDB {
			overWriteBuffer = true
		}
	}
	if off < 0 && off != C.off_t(autoOffset) {
		return C.ssize_t(statusEINVAL)
	}
	tpObject1 := exporter.NewCustomKeyTPUs(c.umpFunctionKeyFast(act))
	tpObject2 := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(act))
	defer func() {
		tpObject1.Set(nil)
		tpObject2.Set(nil)
	}()

	accFlags := f.flags & uint32(C.O_ACCMODE)
	if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
		return C.ssize_t(statusEACCES)
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	var flush bool
	if f.flags&uint32(C.O_SYNC) != 0 || f.flags&uint32(C.O_DSYNC) != 0 {
		flush = true
	}

	// off >= 0 stands for pwrite
	offset = uint64(off)
	if off < 0 {
		if f.flags&uint32(C.O_APPEND) != 0 {
			f.pos = f.size
		}
		offset = f.pos
	}

	n, isROW, err := c.ec.Write(nil, f.ino, offset, buffer, false, overWriteBuffer)
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	if flush {
		if err = c.flush(nil, f.ino); err != nil {
			return C.ssize_t(statusEIO)
		}
	}

	if isROW && useROWNotify() {
		c.broadcastAllReadProcess(f.ino)
	}

	if off < 0 {
		f.pos += uint64(n)
		if f.size < f.pos {
			c.updateSizeByIno(f.ino, f.pos)
		}
	} else {
		if f.size < uint64(off)+uint64(n) {
			c.updateSizeByIno(f.ino, uint64(off)+uint64(n))
		}
	}
	info := c.inodeCache.Get(nil, f.ino)
	if info != nil {
		info.Size = f.size
		c.inodeCache.Put(info)
	}
	return C.ssize_t(n)
}

//export cfs_pwrite_inode
func cfs_pwrite_inode(id C.int64_t, ino C.ino_t, buf unsafe.Pointer, size C.size_t, off C.off_t) (re C.ssize_t) {
	var (
		c      *client
		err    error
		offset uint64
		start  time.Time
	)
	defer func() {
		r := recover()
		if r == nil && re == C.ssize_t(size) && !log.IsDebugEnabled() {
			return
		}
		msg := fmt.Sprintf("id(%v) ino(%v) size(%v) offset(%v) re(%v) err(%v)", id, ino, size, offset, re, err)
		if r != nil || re < 0 {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			handleError(c, "cfs_pwrite_inode", fmt.Sprintf("%s%s", msg, stack))
		} else if re < C.ssize_t(size) {
			log.LogWarnf("cfs_write: %s", msg)
		} else {
			log.LogDebugf("cfs_write: %s time(%v)", msg, time.Since(start).Microseconds())
		}
	}()

	signalIgnoreOnce.Do(signalIgnoreFunc)

	start = time.Now()
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}
	f := c.getFileByInode(uint64(ino))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}

	overWriteBuffer := false
	act := ump_cfs_write
	if f.fileType == fileTypeBinlog {
		act = ump_cfs_write_binlog
	} else if f.fileType == fileTypeRelaylog {
		act = ump_cfs_write_relaylog
	}
	tpObject1 := exporter.NewCustomKeyTPUs(c.umpFunctionKeyFast(act))
	tpObject2 := exporter.NewCustomKeyTPUs(c.umpFunctionGeneralKeyFast(act))
	defer func() {
		tpObject1.Set(nil)
		tpObject2.Set(nil)
	}()

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	// off >= 0 stands for pwrite
	offset = uint64(off)
	n, isROW, err := c.ec.Write(nil, uint64(ino), offset, buffer, false, overWriteBuffer)
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	if isROW && useROWNotify() {
		c.broadcastAllReadProcess(uint64(ino))
	}

	info := c.inodeCache.Get(nil, uint64(ino))
	if info != nil && info.Size < (uint64(off)+uint64(n)) {
		info.Size = uint64(off) + uint64(n)
		c.inodeCache.Put(info)
	}
	return C.ssize_t(n)
}

//export cfs_writev
func cfs_writev(id C.int64_t, fd C.int, iov *C.struct_iovec, iovcnt C.int) C.ssize_t {
	return _cfs_writev(id, fd, iov, iovcnt, -1)
}

//export cfs_pwritev
func cfs_pwritev(id C.int64_t, fd C.int, iov *C.struct_iovec, iovcnt C.int, off C.off_t) C.ssize_t {
	return _cfs_writev(id, fd, iov, iovcnt, off)
}

func _cfs_writev(id C.int64_t, fd C.int, iov *C.struct_iovec, iovcnt C.int, off C.off_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}
	if c.checkReadOnly(f.path) {
		return C.ssize_t(statusEPERM)
	}

	accFlags := f.flags & uint32(C.O_ACCMODE)
	if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
		return C.ssize_t(statusEACCES)
	}

	var iovbuf []C.struct_iovec
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&iovbuf))
	hdr.Data = uintptr(unsafe.Pointer(iov))
	hdr.Len = int(iovcnt)
	hdr.Cap = int(iovcnt)

	var size int
	for i := 0; i < int(iovcnt); i++ {
		size += int(iovbuf[i].iov_len)
	}

	buffer := make([]byte, size, size)
	var buf []byte
	var offset int
	for i := 0; i < int(iovcnt); i++ {
		buf = make([]byte, iovbuf[i].iov_len, iovbuf[i].iov_len)
		hdr = (*reflect.SliceHeader)(unsafe.Pointer(&buf))
		hdr.Data = uintptr(iovbuf[i].iov_base)
		copy(buffer[offset:offset+int(iovbuf[i].iov_len)], buf)
		offset += int(iovbuf[i].iov_len)
	}
	hdr = (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	return _cfs_write(id, fd, unsafe.Pointer(hdr.Data), C.size_t(size), off)
}

//export cfs_lseek
func cfs_lseek(id C.int64_t, fd C.int, offset C.off64_t, whence C.int) (re C.off64_t) {
	var (
		path string
		ino  uint64
	)
	defer func() {
		if log.IsDebugEnabled() {
			log.LogDebugf("cfs_lseek: id(%v) fd(%v) path(%v) ino(%v) offset(%v) whence(%v) re(%v)", id, fd, path, ino, offset, whence, re)
		}
	}()
	c, exist := getClient(int64(id))
	if !exist {
		return C.off64_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.off64_t(statusEBADFD)
	}
	path = f.path
	ino = f.ino

	if whence == C.int(C.SEEK_SET) {
		f.pos = uint64(offset)
	} else if whence == C.int(C.SEEK_CUR) {
		f.pos += uint64(offset)
	} else if whence == C.int(C.SEEK_END) {
		f.pos = f.size + uint64(offset)
	}
	return C.off64_t(f.pos)
}

//export cfs_batch_stat
func cfs_batch_stat(id C.int64_t, inosp unsafe.Pointer, stats []C.struct_stat, count C.int) (re C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return C.int(statusEINVAL)
	}

	var inodes []uint64
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&inodes))
	hdr.Data = uintptr(inosp)
	hdr.Len, hdr.Cap = int(count), int(count)

	if log.IsDebugEnabled() {
		log.LogDebugf("cfs_batch_stat: client(%v) inos[%v], count[%v] re[%v]", int64(id), inodes, int(count), int(re))
	}

	infos := c.mw.BatchInodeGet(context.Background(), inodes)
	infoMap := make(map[uint64]*proto.InodeInfo)
	for _, info := range infos {
		infoMap[info.Inode] = info
	}

	var found int
	for i := 0; i < int(count); i++ {
		ino := uint64(inodes[i])
		info, exist := infoMap[ino]
		if !exist {
			continue
		}

		stats[i].st_dev = 0
		stats[i].st_ino = C.ino_t(ino)
		stats[i].st_size = C.off_t(info.Size)
		stats[i].st_nlink = C.nlink_t(info.Nlink)
		stats[i].st_blksize = C.blksize_t(defaultBlkSize)
		stats[i].st_uid = C.uid_t(info.Uid)
		stats[i].st_gid = C.gid_t(info.Gid)

		if info.Size%512 != 0 {
			stats[i].st_blocks = C.blkcnt_t(info.Size>>9) + 1
		} else {
			stats[i].st_blocks = C.blkcnt_t(info.Size >> 9)
		}

		if proto.IsRegular(info.Mode) {
			stats[i].st_mode = C.mode_t(C.S_IFREG) | C.mode_t(info.Mode&0777)
		} else if proto.IsDir(info.Mode) {
			stats[i].st_mode = C.mode_t(C.S_IFDIR) | C.mode_t(info.Mode&0777)
		} else if proto.IsSymlink(info.Mode) {
			stats[i].st_mode = C.mode_t(C.S_IFLNK) | C.mode_t(info.Mode&0777)
		} else {
			stats[i].st_mode = C.mode_t(C.S_IFSOCK) | C.mode_t(info.Mode&0777)
		}

		var st_atim, st_mtim, st_ctim C.struct_timespec
		t := info.AccessTime.UnixNano()
		st_atim.tv_sec = C.time_t(t / 1e9)
		st_atim.tv_nsec = C.long(t % 1e9)
		stats[i].st_atim = st_atim

		t = info.ModifyTime.UnixNano()
		st_mtim.tv_sec = C.time_t(t / 1e9)
		st_mtim.tv_nsec = C.long(t % 1e9)
		stats[i].st_mtim = st_mtim

		t = info.CreateTime.UnixNano()
		st_ctim.tv_sec = C.time_t(t / 1e9)
		st_ctim.tv_nsec = C.long(t % 1e9)
		stats[i].st_ctim = st_ctim

		found++
	}

	return C.int(found)
}

/*
 * internals
 */

func errorToStatus(err error) C.int {
	if err == nil {
		return 0
	}
	if errno, is := err.(syscall.Errno); is {
		return -C.int(errno)
	}
	return -C.int(syscall.EIO)
}

func (c *client) absPathAt(dirfd C.int, path *C.char) (string, error) {
	useDirfd := !gopath.IsAbs(C.GoString(path)) && (dirfd != C.AT_FDCWD)
	var absPath string
	if useDirfd {
		f := c.getFile(uint(dirfd))
		if f == nil || f.path == "" {
			return "", fmt.Errorf("invalid dirfd: %d", dirfd)
		}
		absPath = gopath.Clean(gopath.Join(f.path, C.GoString(path)))
	} else {
		absPath = c.absPath(C.GoString(path))
	}

	return absPath, nil
}
