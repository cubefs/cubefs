// Copyright 2020 The CubeFS Authors.
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
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>

struct cfs_stat_info {
    uint64_t ino;
    uint64_t size;
    uint64_t blocks;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
    uint32_t atime_nsec;
    uint32_t mtime_nsec;
    uint32_t ctime_nsec;
    mode_t   mode;
    uint32_t nlink;
    uint32_t blk_size;
    uint32_t uid;
    uint32_t gid;
};

struct cfs_summary_info {
    int64_t files;
    int64_t subdirs;
    int64_t fbytes;
};

struct cfs_dirent {
    uint64_t ino;
    char     name[256];
    char     d_type;
    uint32_t     nameLen;
};

struct cfs_hdfs_stat_info {
    uint64_t size;
    uint64_t atime;
    uint64_t mtime;
    uint32_t atime_nsec;
    uint32_t mtime_nsec;
    mode_t   mode;
};

struct cfs_dirent_info {
    struct   cfs_hdfs_stat_info stat;
    char     d_type;
    char     name[256];
    uint32_t     nameLen;
};

*/
import "C"

import (
	"context"
	"fmt"
	"io"
	syslog "log"
	"os"
	gopath "path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/bits-and-blooms/bitset"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/client/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	defaultBlkSize = uint32(1) << 12

	maxFdNum uint = 10240000

	MaxSizePutOnce = int64(1) << 23
)

var gClientManager *clientManager

var (
	statusOK = C.int(0)
	// error status must be minus value
	statusEIO     = errorToStatus(syscall.EIO)
	statusEINVAL  = errorToStatus(syscall.EINVAL)
	statusEEXIST  = errorToStatus(syscall.EEXIST)
	statusEBADFD  = errorToStatus(syscall.EBADFD)
	statusEACCES  = errorToStatus(syscall.EACCES)
	statusEMFILE  = errorToStatus(syscall.EMFILE)
	statusENOTDIR = errorToStatus(syscall.ENOTDIR)
	statusEISDIR  = errorToStatus(syscall.EISDIR)
)
var once sync.Once

func init() {
	gClientManager = &clientManager{
		clients: make(map[int64]*client),
	}
}

func errorToStatus(err error) C.int {
	if err == nil {
		return 0
	}
	if errno, is := err.(syscall.Errno); is {
		return -C.int(errno)
	}
	return -C.int(syscall.EIO)
}

type clientManager struct {
	nextClientID int64
	clients      map[int64]*client
	mu           sync.RWMutex
}

type pushConfig struct {
	PushAddr string `json:"pushAddr"`
}

func newClient() *client {
	id := atomic.AddInt64(&gClientManager.nextClientID, 1)
	c := &client{
		id:                  id,
		fdmap:               make(map[uint]*file),
		fdset:               bitset.New(maxFdNum),
		dirChildrenNumLimit: proto.DefaultDirChildrenNumLimit,
		cwd:                 "/",
		sc:                  fs.NewSummaryCache(fs.DefaultSummaryExpiration, fs.MaxSummaryCache),
		ic:                  fs.NewInodeCache(fs.DefaultInodeExpiration, fs.MaxInodeCache),
		dc:                  fs.NewDentryCache(),
	}

	gClientManager.mu.Lock()
	gClientManager.clients[id] = c
	gClientManager.mu.Unlock()

	return c
}

func getClient(id int64) (c *client, exist bool) {
	gClientManager.mu.RLock()
	defer gClientManager.mu.RUnlock()
	c, exist = gClientManager.clients[id]
	return
}

func removeClient(id int64) {
	gClientManager.mu.Lock()
	defer gClientManager.mu.Unlock()
	delete(gClientManager.clients, id)
}

type file struct {
	fd    uint
	ino   uint64
	pino  uint64
	flags uint32
	mode  uint32

	// dir only
	dirp *dirStream

	//rw
	fileWriter *blobstore.Writer
	fileReader *blobstore.Reader
}

type dirStream struct {
	pos     int
	dirents []proto.Dentry
}

type client struct {
	// client id allocated by libsdk
	id int64

	// mount config
	volName             string
	masterAddr          string
	followerRead        bool
	logDir              string
	logLevel            string
	ebsEndpoint         string
	servicePath         string
	volType             int
	cacheAction         int
	ebsBlockSize        int
	enableBcache        bool
	readBlockThread     int
	writeBlockThread    int
	cacheRuleKey        string
	cacheThreshold      int
	enableSummary       bool
	secretKey           string
	accessKey           string
	subDir              string
	pushAddr            string
	cluster             string
	dirChildrenNumLimit uint32
	enableAudit         bool

	// runtime context
	cwd    string // current working directory
	fdmap  map[uint]*file
	fdset  *bitset.BitSet
	fdlock sync.RWMutex

	// server info
	mw   *meta.MetaWrapper
	ec   *stream.ExtentClient
	ic   *fs.InodeCache
	dc   *fs.DentryCache
	bc   *bcache.BcacheClient
	ebsc *blobstore.BlobStoreClient
	sc   *fs.SummaryCache
}

//export cfs_new_client
func cfs_new_client() C.int64_t {
	c := newClient()
	// Just skip fd 0, 1, 2, to avoid confusion.
	c.fdset.Set(0).Set(1).Set(2)
	return C.int64_t(c.id)
}

//export cfs_set_client
func cfs_set_client(id C.int64_t, key, val *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	k := C.GoString(key)
	v := C.GoString(val)

	switch k {
	case "volName":
		c.volName = v
	case "masterAddr":
		c.masterAddr = v
	case "followerRead":
		if v == "true" {
			c.followerRead = true
		} else {
			c.followerRead = false
		}
	case "logDir":
		c.logDir = v
	case "logLevel":
		c.logLevel = v
	case "enableBcache":
		if v == "true" {
			c.enableBcache = true
		} else {
			c.enableBcache = false
		}
	case "readBlockThread":
		rt, err := strconv.Atoi(v)
		if err == nil {
			c.readBlockThread = rt
		}
	case "writeBlockThread":
		wt, err := strconv.Atoi(v)
		if err == nil {
			c.writeBlockThread = wt
		}
	case "enableSummary":
		if v == "true" {
			c.enableSummary = true
		} else {
			c.enableSummary = false
		}
	case "accessKey":
		c.accessKey = v
	case "secretKey":
		c.secretKey = v
	case "pushAddr":
		c.pushAddr = v
	case "enableAudit":
		if v == "true" {
			c.enableAudit = true
		} else {
			c.enableAudit = false
		}
	default:
		return statusEINVAL
	}
	return statusOK
}

//export cfs_start_client
func cfs_start_client(id C.int64_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	err := c.start()
	if err != nil {
		syslog.Println(err)
		return statusEIO
	}
	return statusOK
}

//export cfs_close_client
func cfs_close_client(id C.int64_t) {
	if c, exist := getClient(int64(id)); exist {
		if c.ec != nil {
			_ = c.ec.Close()
		}
		if c.mw != nil {
			_ = c.mw.Close()
		}
		removeClient(int64(id))
	}
	auditlog.StopAudit()
	log.LogFlush()
}

//export cfs_chdir
func cfs_chdir(id C.int64_t, path *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	cwd := c.absPath(C.GoString(path))
	dirInfo, err := c.lookupPath(cwd)
	if err != nil {
		return errorToStatus(err)
	}
	if !proto.IsDir(dirInfo.Mode) {
		return statusENOTDIR
	}
	c.cwd = cwd
	return statusOK
}

//export cfs_getcwd
func cfs_getcwd(id C.int64_t) *C.char {
	c, exist := getClient(int64(id)) // client's working directory
	if !exist {
		return C.CString("")
	}
	return C.CString(c.cwd)
}

//export cfs_getattr
func cfs_getattr(id C.int64_t, path *C.char, stat *C.struct_cfs_stat_info) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	info, err := c.lookupPath(c.absPath(C.GoString(path)))
	if err != nil {
		return errorToStatus(err)
	}

	// fill up the stat
	stat.ino = C.uint64_t(info.Inode)
	stat.size = C.uint64_t(info.Size)
	stat.nlink = C.uint32_t(info.Nlink)
	stat.blk_size = C.uint32_t(defaultBlkSize)
	stat.uid = C.uint32_t(info.Uid)
	stat.gid = C.uint32_t(info.Gid)

	if info.Size%512 != 0 {
		stat.blocks = C.uint64_t(info.Size>>9) + 1
	} else {
		stat.blocks = C.uint64_t(info.Size >> 9)
	}
	// fill up the mode
	if proto.IsRegular(info.Mode) {
		stat.mode = C.uint32_t(C.S_IFREG) | C.uint32_t(info.Mode&0777)
	} else if proto.IsDir(info.Mode) {
		stat.mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(info.Mode&0777)
	} else if proto.IsSymlink(info.Mode) {
		stat.mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(info.Mode&0777)
	} else {
		stat.mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(info.Mode&0777)
	}

	// fill up the time struct
	t := info.AccessTime.UnixNano()
	stat.atime = C.uint64_t(t / 1e9)
	stat.atime_nsec = C.uint32_t(t % 1e9)

	t = info.ModifyTime.UnixNano()
	stat.mtime = C.uint64_t(t / 1e9)
	stat.mtime_nsec = C.uint32_t(t % 1e9)

	t = info.CreateTime.UnixNano()
	stat.ctime = C.uint64_t(t / 1e9)
	stat.ctime_nsec = C.uint32_t(t % 1e9)

	return statusOK
}

//export cfs_setattr
func cfs_setattr(id C.int64_t, path *C.char, stat *C.struct_cfs_stat_info, valid C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	info, err := c.lookupPath(c.absPath(C.GoString(path)))
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(info, uint32(valid), uint32(stat.mode), uint32(stat.uid), uint32(stat.gid), int64(stat.atime), int64(stat.mtime))

	if err != nil {
		return errorToStatus(err)
	}
	c.ic.Delete(info.Inode)
	return statusOK
}

//export cfs_open
func cfs_open(id C.int64_t, path *C.char, flags C.int, mode C.mode_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	start := time.Now()

	fuseMode := uint32(mode) & uint32(0777)
	fuseFlags := uint32(flags) &^ uint32(0x8000)
	accFlags := fuseFlags & uint32(C.O_ACCMODE)

	absPath := c.absPath(C.GoString(path))

	var info *proto.InodeInfo
	var parentIno uint64

	/*
	 * Note that the rwx mode is ignored when using libsdk
	 */

	if fuseFlags&uint32(C.O_CREAT) != 0 {
		if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
			return statusEACCES
		}
		dirpath, name := gopath.Split(absPath)
		dirInfo, err := c.lookupPath(dirpath)
		if err != nil {
			return errorToStatus(err)
		}
		parentIno = dirInfo.Inode
		defer func() {
			if info != nil {
				auditlog.FormatLog("Create", dirpath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
			} else {
				auditlog.FormatLog("Create", dirpath, "nil", err, time.Since(start).Microseconds(), 0, 0)
			}
		}()
		newInfo, err := c.create(dirInfo.Inode, name, fuseMode)
		if err != nil {
			if err != syscall.EEXIST {
				return errorToStatus(err)
			}
			newInfo, err = c.lookupPath(absPath)
			if err != nil {
				return errorToStatus(err)
			}
		}
		info = newInfo
	} else {
		dirpath, _ := gopath.Split(absPath)
		dirInfo, err := c.lookupPath(dirpath)
		if err != nil {
			return errorToStatus(err)
		}
		parentIno = dirInfo.Inode // parent inode
		newInfo, err := c.lookupPath(absPath)
		if err != nil {
			return errorToStatus(err)
		}
		info = newInfo
	}
	var fileCache bool
	if c.cacheRuleKey == "" {
		fileCache = false
	} else {
		fileCachePattern := fmt.Sprintf(".*%s.*", c.cacheRuleKey)
		fileCache, _ = regexp.MatchString(fileCachePattern, absPath)
	}
	f := c.allocFD(info.Inode, fuseFlags, fuseMode, fileCache, info.Size, parentIno)
	if f == nil {
		return statusEMFILE
	}

	if proto.IsRegular(info.Mode) {
		c.openStream(f)
		if fuseFlags&uint32(C.O_TRUNC) != 0 {
			if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return statusEACCES
			}
			if err := c.truncate(f, 0); err != nil {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return statusEIO
			}
		}
	}

	return C.int(f.fd)
}

//export cfs_flush
func cfs_flush(id C.int64_t, fd C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	err := c.flush(f)
	if err != nil {
		return statusEIO
	}
	c.ic.Delete(f.ino)
	return statusOK
}

//export cfs_close
func cfs_close(id C.int64_t, fd C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return
	}
	f := c.releaseFD(uint(fd))
	if f != nil {
		c.flush(f)
		c.closeStream(f)
	}
}

//export cfs_write
func cfs_write(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}

	accFlags := f.flags & uint32(C.O_ACCMODE)
	if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
		return C.ssize_t(statusEACCES)
	}

	var buffer []byte

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	var flags int
	var wait bool

	if f.flags&uint32(C.O_DIRECT) != 0 || f.flags&uint32(C.O_SYNC) != 0 || f.flags&uint32(C.O_DSYNC) != 0 {
		if proto.IsHot(c.volType) {
			wait = true
		}
	}
	if f.flags&uint32(C.O_APPEND) != 0 || proto.IsCold(c.volType) {
		flags |= proto.FlagsAppend
		flags |= proto.FlagsSyncWrite
	}

	n, err := c.write(f, int(off), buffer, flags)
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	if wait {
		if err = c.flush(f); err != nil {
			return C.ssize_t(statusEIO)
		}
	}

	return C.ssize_t(n)
}

//export cfs_read
func cfs_read(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}

	accFlags := f.flags & uint32(C.O_ACCMODE)
	if accFlags == uint32(C.O_WRONLY) {
		return C.ssize_t(statusEACCES)
	}

	var buffer []byte

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	n, err := c.read(f, int(off), buffer)
	if err != nil {
		return C.ssize_t(statusEIO)
	}

	return C.ssize_t(n)
}

//export cfs_batch_get_inodes
func cfs_batch_get_inodes(id C.int64_t, fd C.int, iids unsafe.Pointer, stats []C.struct_cfs_stat_info, count C.int) (n C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	var inodeIDS []uint64

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&inodeIDS))
	hdr.Data = uintptr(iids)
	hdr.Len = int(count)
	hdr.Cap = int(count)

	infos := c.mw.BatchInodeGet(inodeIDS)
	if len(infos) > int(count) {
		return statusEINVAL
	}

	for i := 0; i < len(infos); i++ {
		// fill up the stat
		stats[i].ino = C.uint64_t(infos[i].Inode)
		stats[i].size = C.uint64_t(infos[i].Size)
		stats[i].blocks = C.uint64_t(infos[i].Size >> 9)
		stats[i].nlink = C.uint32_t(infos[i].Nlink)
		stats[i].blk_size = C.uint32_t(defaultBlkSize)
		stats[i].uid = C.uint32_t(infos[i].Uid)
		stats[i].gid = C.uint32_t(infos[i].Gid)

		// fill up the mode
		if proto.IsRegular(infos[i].Mode) {
			stats[i].mode = C.uint32_t(C.S_IFREG) | C.uint32_t(infos[i].Mode&0777)
		} else if proto.IsDir(infos[i].Mode) {
			stats[i].mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(infos[i].Mode&0777)
		} else if proto.IsSymlink(infos[i].Mode) {
			stats[i].mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(infos[i].Mode&0777)
		} else {
			stats[i].mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(infos[i].Mode&0777)
		}

		// fill up the time struct
		t := infos[i].AccessTime.UnixNano()
		stats[i].atime = C.uint64_t(t / 1e9)
		stats[i].atime_nsec = C.uint32_t(t % 1e9)

		t = infos[i].ModifyTime.UnixNano()
		stats[i].mtime = C.uint64_t(t / 1e9)
		stats[i].mtime_nsec = C.uint32_t(t % 1e9)

		t = infos[i].CreateTime.UnixNano()
		stats[i].ctime = C.uint64_t(t / 1e9)
		stats[i].ctime_nsec = C.uint32_t(t % 1e9)
	}

	n = C.int(len(infos))
	return
}

//export cfs_refreshsummary
func cfs_refreshsummary(id C.int64_t, path *C.char, goroutine_num C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	if !c.enableSummary {
		return statusEINVAL
	}
	info, err := c.lookupPath(c.absPath(C.GoString(path)))
	var ino uint64
	if err != nil {
		ino = proto.RootIno
	} else {
		ino = info.Inode
	}
	goroutineNum := int32(goroutine_num)
	err = c.mw.RefreshSummary_ll(ino, goroutineNum)
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

/*
 * Note that readdir is not thread-safe according to the POSIX spec.
 */

//export cfs_readdir
func cfs_readdir(id C.int64_t, fd C.int, dirents []C.struct_cfs_dirent, count C.int) (n C.int) {
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
		dentries, err := c.mw.ReadDir_ll(f.ino)
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

	return n
}

//export cfs_lsdir
func cfs_lsdir(id C.int64_t, fd C.int, direntsInfo []C.struct_cfs_dirent_info, count C.int) (n C.int) {
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
		dentries, err := c.mw.ReadDir_ll(f.ino)
		if err != nil {
			return errorToStatus(err)
		}
		f.dirp.dirents = dentries
	}

	dirp := f.dirp
	inodeIDS := make([]uint64, count, count)
	inodeMap := make(map[uint64]C.int)
	for dirp.pos < len(dirp.dirents) && n < count {
		inodeIDS[n] = dirp.dirents[dirp.pos].Inode
		inodeMap[dirp.dirents[dirp.pos].Inode] = n
		// fill up d_type
		if proto.IsRegular(dirp.dirents[dirp.pos].Type) {
			direntsInfo[n].d_type = C.DT_REG
		} else if proto.IsDir(dirp.dirents[dirp.pos].Type) {
			direntsInfo[n].d_type = C.DT_DIR
		} else if proto.IsSymlink(dirp.dirents[dirp.pos].Type) {
			direntsInfo[n].d_type = C.DT_LNK
		} else {
			direntsInfo[n].d_type = C.DT_UNKNOWN
		}
		nameLen := len(dirp.dirents[dirp.pos].Name)
		if nameLen >= 256 {
			nameLen = 255
		}
		hdr := (*reflect.StringHeader)(unsafe.Pointer(&dirp.dirents[dirp.pos].Name))

		C.memcpy(unsafe.Pointer(&direntsInfo[n].name[0]), unsafe.Pointer(hdr.Data), C.size_t(nameLen))
		direntsInfo[n].name[nameLen] = 0
		direntsInfo[n].nameLen = C.uint32_t(nameLen)

		// advance cursor
		dirp.pos++
		n++
	}
	if n == 0 {
		return n
	}
	infos := c.mw.BatchInodeGet(inodeIDS)
	if len(infos) != int(n) {
		return statusEIO
	}
	for i := 0; i < len(infos); i++ {
		// fill up the stat
		index := inodeMap[infos[i].Inode]
		direntsInfo[index].stat.size = C.uint64_t(infos[i].Size)

		// fill up the mode
		if proto.IsRegular(infos[i].Mode) {
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFREG) | C.uint32_t(infos[i].Mode&0777)
		} else if proto.IsDir(infos[i].Mode) {
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(infos[i].Mode&0777)
		} else if proto.IsSymlink(infos[i].Mode) {
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(infos[i].Mode&0777)
		} else {
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(infos[i].Mode&0777)
		}

		// fill up the time struct
		t := infos[index].AccessTime.UnixNano()
		direntsInfo[index].stat.atime = C.uint64_t(t / 1e9)
		direntsInfo[index].stat.atime_nsec = C.uint32_t(t % 1e9)

		t = infos[index].ModifyTime.UnixNano()
		direntsInfo[index].stat.mtime = C.uint64_t(t / 1e9)
		direntsInfo[index].stat.mtime_nsec = C.uint32_t(t % 1e9)
	}
	return n

}

//export cfs_mkdirs
func cfs_mkdirs(id C.int64_t, path *C.char, mode C.mode_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	start := time.Now()
	var gerr error
	var gino uint64

	dirpath := c.absPath(C.GoString(path))
	if dirpath == "/" {
		return statusEEXIST
	}

	defer func() {
		if gerr == nil {
			auditlog.FormatLog("Mkdir", dirpath, "nil", gerr, time.Since(start).Microseconds(), gino, 0)
		} else {
			auditlog.FormatLog("Mkdir", dirpath, "nil", gerr, time.Since(start).Microseconds(), 0, 0)
		}
	}()

	pino := proto.RootIno
	dirs := strings.Split(dirpath, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err := c.mw.Lookup_ll(pino, dir)
		if err != nil {
			if err == syscall.ENOENT {
				info, err := c.mkdir(pino, dir, uint32(mode))

				if err != nil {
					if err != syscall.EEXIST {
						gerr = err
						return errorToStatus(err)
					}
				} else {
					child = info.Inode
				}
			} else {
				gerr = err
				return errorToStatus(err)
			}
		}
		pino = child
		gino = child
	}

	return 0
}

//export cfs_rmdir
func cfs_rmdir(id C.int64_t, path *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	start := time.Now()
	var err error
	var info *proto.InodeInfo

	absPath := c.absPath(C.GoString(path))
	defer func() {
		if info == nil {
			auditlog.FormatLog("Rmdir", absPath, "nil", err, time.Since(start).Microseconds(), 0, 0)
		} else {
			auditlog.FormatLog("Rmdir", absPath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
		}
	}()
	dirpath, name := gopath.Split(absPath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, true)
	c.ic.Delete(dirInfo.Inode)
	c.dc.Delete(absPath)
	return errorToStatus(err)
}

//export cfs_unlink
func cfs_unlink(id C.int64_t, path *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	start := time.Now()
	var err error
	var info *proto.InodeInfo

	absPath := c.absPath(C.GoString(path))
	dirpath, name := gopath.Split(absPath)

	defer func() {
		if info == nil {
			auditlog.FormatLog("Unlink", absPath, "nil", err, time.Since(start).Microseconds(), 0, 0)
		} else {
			auditlog.FormatLog("Unlink", absPath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
		}
	}()
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	_, mode, err := c.mw.Lookup_ll(dirInfo.Inode, name)
	if err != nil {
		return errorToStatus(err)
	}
	if proto.IsDir(mode) {
		return statusEISDIR
	}

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, false)
	if err != nil {
		return errorToStatus(err)
	}

	if info != nil {
		_ = c.mw.Evict(info.Inode)
		c.ic.Delete(info.Inode)
	}
	return 0
}

//export cfs_rename
func cfs_rename(id C.int64_t, from *C.char, to *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	start := time.Now()
	var err error

	absFrom := c.absPath(C.GoString(from))
	absTo := c.absPath(C.GoString(to))

	defer func() {
		auditlog.FormatLog("Rename", absFrom, absTo, err, time.Since(start).Microseconds(), 0, 0)
	}()

	srcDirPath, srcName := gopath.Split(absFrom)
	dstDirPath, dstName := gopath.Split(absTo)

	srcDirInfo, err := c.lookupPath(srcDirPath)
	if err != nil {
		return errorToStatus(err)
	}
	dstDirInfo, err := c.lookupPath(dstDirPath)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.mw.Rename_ll(srcDirInfo.Inode, srcName, dstDirInfo.Inode, dstName, false)
	c.ic.Delete(srcDirInfo.Inode)
	c.ic.Delete(dstDirInfo.Inode)
	c.dc.Delete(absFrom)
	return errorToStatus(err)
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

	info, err := c.mw.InodeGet_ll(f.ino)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(info, proto.AttrMode, uint32(mode), 0, 0, 0, 0)
	if err != nil {
		return errorToStatus(err)
	}
	c.ic.Delete(info.Inode)
	return statusOK
}

//export cfs_getsummary
func cfs_getsummary(id C.int64_t, path *C.char, summary *C.struct_cfs_summary_info, useCache *C.char, goroutine_num C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	info, err := c.lookupPath(c.absPath(C.GoString(path)))
	if err != nil {
		return errorToStatus(err)
	}

	if strings.ToLower(C.GoString(useCache)) == "true" {
		cacheSummaryInfo := c.sc.Get(info.Inode)
		if cacheSummaryInfo != nil {
			summary.files = C.int64_t(cacheSummaryInfo.Files)
			summary.subdirs = C.int64_t(cacheSummaryInfo.Subdirs)
			summary.fbytes = C.int64_t(cacheSummaryInfo.Fbytes)
			return statusOK
		}
	}

	if !proto.IsDir(info.Mode) {
		return statusENOTDIR
	}
	goroutineNum := int32(goroutine_num)
	summaryInfo, err := c.mw.GetSummary_ll(info.Inode, goroutineNum)
	if err != nil {
		return errorToStatus(err)
	}
	if strings.ToLower(C.GoString(useCache)) != "false" {
		c.sc.Put(info.Inode, &summaryInfo)
	}
	summary.files = C.int64_t(summaryInfo.Files)
	summary.subdirs = C.int64_t(summaryInfo.Subdirs)
	summary.fbytes = C.int64_t(summaryInfo.Fbytes)
	return statusOK
}

// internals

func (c *client) absPath(path string) string {
	p := gopath.Clean(path)
	if !gopath.IsAbs(p) {
		p = gopath.Join(c.cwd, p)
	}
	return gopath.Clean(p)
}

func (c *client) start() (err error) {
	var masters = strings.Split(c.masterAddr, ",")
	if c.logDir != "" {
		if c.logLevel == "" {
			c.logLevel = "WARN"
		}
		level := parseLogLevel(c.logLevel)
		log.InitLog(c.logDir, "libcfs", level, nil)
		stat.NewStatistic(c.logDir, "libcfs", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
	}
	proto.InitBufferPool(int64(32768))
	if c.readBlockThread == 0 {
		c.readBlockThread = 10
	}
	if c.writeBlockThread == 0 {
		c.writeBlockThread = 10
	}
	if err = c.loadConfFromMaster(masters); err != nil {
		return
	}
	if err = c.checkPermission(); err != nil {
		err = errors.NewErrorf("check permission failed: %v", err)
		syslog.Println(err)
		return
	}

	if c.enableAudit {
		_, err = auditlog.InitAudit(c.logDir, "clientSdk", int64(auditlog.DefaultAuditLogSize))
		if err != nil {
			log.LogWarnf("Init audit log fail: %v", err)
		}
	}

	if c.enableSummary {
		c.sc = fs.NewSummaryCache(fs.DefaultSummaryExpiration, fs.MaxSummaryCache)
	}
	if c.enableBcache {
		c.bc = bcache.NewBcacheClient()
	}
	var ebsc *blobstore.BlobStoreClient
	if c.ebsEndpoint != "" {
		if ebsc, err = blobstore.NewEbsClient(access.Config{
			ConnMode: access.NoLimitConnMode,
			Consul: access.ConsulConfig{
				Address: c.ebsEndpoint,
			},
			MaxSizePutOnce: MaxSizePutOnce,
			Logger: &access.Logger{
				Filename: gopath.Join(c.logDir, "libcfs/ebs.log"),
			},
		}); err != nil {
			return
		}
	}
	var mw *meta.MetaWrapper
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        c.volName,
		Masters:       masters,
		ValidateOwner: false,
		EnableSummary: c.enableSummary,
	}); err != nil {
		log.LogErrorf("newClient NewMetaWrapper failed(%v)", err)
		return err
	}
	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:            c.volName,
		VolumeType:        c.volType,
		Masters:           masters,
		FollowerRead:      c.followerRead,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		BcacheEnable:      c.enableBcache,
		OnLoadBcache:      c.bc.Get,
		OnCacheBcache:     c.bc.Put,
		OnEvictBcache:     c.bc.Evict,
		DisableMetaCache:  true,
	}); err != nil {
		log.LogErrorf("newClient NewExtentClient failed(%v)", err)
		return
	}
	//if c.pushAddr == "" {
	//	c.pushAddr = "cfs-push.oppo.local"
	//}
	//pushCfg := pushConfig{
	//	PushAddr: c.pushAddr,
	//}
	//cfgJson, err := json.Marshal(pushCfg)
	//if err != nil {
	//	log.LogErrorf("newClient NewExtentClient: marsh push addr fail.(%v)", err)
	//}
	//cfg := config.LoadConfigString(string(cfgJson))
	//once.Do(func() {
	//	exporter.Init("hadoop-client", cfg)
	//	exporter.RegistConsul(c.cluster, "hadoop-client", cfg)
	//})

	c.mw = mw
	c.ec = ec
	c.ebsc = ebsc
	return nil
}

func (c *client) checkPermission() (err error) {
	if c.accessKey == "" || c.secretKey == "" {
		err = errors.New("invalid AccessKey or SecretKey")
		return
	}

	// checkPermission
	var mc = masterSDK.NewMasterClientFromString(c.masterAddr, false)
	var userInfo *proto.UserInfo
	if userInfo, err = mc.UserAPI().GetAKInfo(c.accessKey); err != nil {
		return
	}
	if userInfo.SecretKey != c.secretKey {
		err = proto.ErrNoPermission
		return
	}
	var policy = userInfo.Policy
	if policy.IsOwn(c.volName) {
		return
	}
	// read write
	if policy.IsAuthorized(c.volName, c.subDir, proto.POSIXWriteAction) &&
		policy.IsAuthorized(c.volName, c.subDir, proto.POSIXReadAction) {
		return
	}
	// read only
	if policy.IsAuthorized(c.volName, c.subDir, proto.POSIXReadAction) &&
		!policy.IsAuthorized(c.volName, c.subDir, proto.POSIXWriteAction) {
		return
	}
	err = proto.ErrNoPermission
	return
}

func (c *client) allocFD(ino uint64, flags, mode uint32, fileCache bool, fileSize uint64, parentInode uint64) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	fd, ok := c.fdset.NextClear(0)
	if !ok || fd > maxFdNum {
		return nil
	}
	c.fdset.Set(fd)
	f := &file{fd: fd, ino: ino, flags: flags, mode: mode, pino: parentInode}
	if proto.IsCold(c.volType) {
		clientConf := blobstore.ClientConfig{
			VolName:         c.volName,
			VolType:         c.volType,
			BlockSize:       c.ebsBlockSize,
			Ino:             ino,
			Bc:              c.bc,
			Mw:              c.mw,
			Ec:              c.ec,
			Ebsc:            c.ebsc,
			EnableBcache:    c.enableBcache,
			WConcurrency:    c.writeBlockThread,
			ReadConcurrency: c.readBlockThread,
			CacheAction:     c.cacheAction,
			FileCache:       fileCache,
			FileSize:        fileSize,
			CacheThreshold:  c.cacheThreshold,
		}

		switch flags & 0xff {
		case syscall.O_RDONLY:
			f.fileReader = blobstore.NewReader(clientConf)
		case syscall.O_WRONLY:
			f.fileWriter = blobstore.NewWriter(clientConf)
		case syscall.O_RDWR:
			f.fileReader = blobstore.NewReader(clientConf)
			f.fileWriter = blobstore.NewWriter(clientConf)
		default:
			f.fileWriter = blobstore.NewWriter(clientConf)
		}
	}
	c.fdmap[fd] = f
	return f
}

func (c *client) getFile(fd uint) *file {
	c.fdlock.Lock()
	f := c.fdmap[fd]
	c.fdlock.Unlock()
	return f
}

func (c *client) releaseFD(fd uint) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	f, ok := c.fdmap[fd]
	if !ok {
		return nil
	}
	delete(c.fdmap, fd)
	c.fdset.Clear(fd)
	c.ic.Delete(f.ino)
	return f
}

func (c *client) lookupPath(path string) (*proto.InodeInfo, error) {
	ino, ok := c.dc.Get(gopath.Clean(path))
	if !ok {
		inoInterval, err := c.mw.LookupPath(gopath.Clean(path))
		if err != nil {
			return nil, err
		}
		c.dc.Put(gopath.Clean(path), inoInterval)
		ino = inoInterval
	}
	info := c.ic.Get(ino)
	if info != nil {
		return info, nil
	}
	info, err := c.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, err
	}
	c.ic.Put(info)

	return info, nil
}

func (c *client) setattr(info *proto.InodeInfo, valid uint32, mode, uid, gid uint32, atime, mtime int64) error {
	// Only rwx mode bit can be set
	if valid&proto.AttrMode != 0 {
		fuseMode := mode & uint32(0777)
		mode = info.Mode &^ uint32(0777) // clear rwx mode bit
		mode |= fuseMode
	}
	return c.mw.Setattr(info.Inode, valid, mode, uid, gid, atime, mtime)
}

func (c *client) create(pino uint64, name string, mode uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil)
}

func (c *client) mkdir(pino uint64, name string, mode uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeDir)
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil)
}

func (c *client) openStream(f *file) {
	_ = c.ec.OpenStream(f.ino)
}

func (c *client) closeStream(f *file) {
	_ = c.ec.CloseStream(f.ino)
	_ = c.ec.EvictStream(f.ino)
	f.fileWriter = nil
	f.fileReader = nil
}

func (c *client) flush(f *file) error {
	if proto.IsHot(c.volType) {
		return c.ec.Flush(f.ino)
	} else {
		if f.fileWriter != nil {
			return f.fileWriter.Flush(f.ino, c.ctx(c.id, f.ino))
		}
	}
	return nil
}

func (c *client) truncate(f *file, size int) error {
	err := c.ec.Truncate(c.mw, f.pino, f.ino, size)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) write(f *file, offset int, data []byte, flags int) (n int, err error) {
	if proto.IsHot(c.volType) {
		c.ec.GetStreamer(f.ino).SetParentInode(f.pino) // set the parent inode
		n, err = c.ec.Write(f.ino, offset, data, flags)
	} else {
		n, err = f.fileWriter.Write(c.ctx(c.id, f.ino), offset, data, flags)
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *client) read(f *file, offset int, data []byte) (n int, err error) {
	if proto.IsHot(c.volType) {
		n, err = c.ec.Read(f.ino, data, offset, len(data))
	} else {
		n, err = f.fileReader.Read(c.ctx(c.id, f.ino), data, offset, len(data))
	}
	if err != nil && err != io.EOF {
		return 0, err
	}
	return n, nil
}

func (c *client) ctx(cid int64, ino uint64) context.Context {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", fmt.Sprintf("cid=%v,ino=%v", cid, ino))
	return ctx
}

func (c *client) loadConfFromMaster(masters []string) (err error) {
	mc := masterSDK.NewMasterClient(masters, false)
	var volumeInfo *proto.SimpleVolView
	volumeInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(c.volName)
	if err != nil {
		return
	}
	c.volType = volumeInfo.VolType
	c.ebsBlockSize = volumeInfo.ObjBlockSize
	c.cacheAction = volumeInfo.CacheAction
	c.cacheRuleKey = volumeInfo.CacheRule
	c.cacheThreshold = volumeInfo.CacheThreshold

	var clusterInfo *proto.ClusterInfo
	clusterInfo, err = mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return
	}
	c.ebsEndpoint = clusterInfo.EbsAddr
	c.servicePath = clusterInfo.ServicePath
	c.cluster = clusterInfo.Cluster
	c.dirChildrenNumLimit = clusterInfo.DirChildrenNumLimit
	buf.InitCachePool(c.ebsBlockSize)
	return
}

func parseLogLevel(loglvl string) log.Level {
	var level log.Level
	switch strings.ToLower(loglvl) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}
	return level
}

func (c *client) fileSize(ino uint64) (size int, gen uint64) {
	size, gen, valid := c.ec.FileSize(ino)
	log.LogDebugf("fileSize: ino(%v) fileSize(%v) gen(%v) valid(%v)", ino, size, gen, valid)

	if !valid {
		info := c.ic.Get(ino)
		if info != nil {
			return int(info.Size), info.Generation
		}
		if info, err := c.mw.InodeGet_ll(ino); err == nil {
			size = int(info.Size)
			gen = info.Generation
		}
	}
	return
}

func main() {}
