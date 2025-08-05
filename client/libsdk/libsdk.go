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
	int64_t filesHdd;
    int64_t filesSsd;
    int64_t filesBlobStore;
    int64_t fbytesHdd;
    int64_t fbytesSsd;
    int64_t fbytesBlobStore;
    int64_t subdirs;
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

struct cfs_vol_info {
       char name[256];
       char owner[256];
       int64_t create_time;
       uint8_t status;
       uint64_t total_size;
       uint64_t used_size;
};

struct cfs_access_file_info {
    char dir[256];
    char accessFileCountSsd[256];
    char accessFileSizeSsd[256];
    char accessFileCountHdd[256];
    char accessFileSizeHdd[256];
    char accessFileCountBlobStore[256];
    char accessFileSizeBlobStore[256];
};

*/
import "C"

import (
	"context"
	"fmt"
	"io"
	syslog "log"
	"os"
	"path"
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
	"github.com/cubefs/cubefs/client/blockcache/bcache"
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
	statusENOSPC  = errorToStatus(syscall.ENOSPC)
	statusEPERM   = errorToStatus(syscall.EPERM)
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

	// rw
	fileWriter *blobstore.Writer
	fileReader *blobstore.Reader

	path         string
	storageClass uint32
	openForWrite bool
}

type dirStream struct {
	pos     int
	dirents []proto.Dentry
}

type client struct {
	// client id allocated by libsdk
	id int64

	// mount config
	volName                string
	masterAddr             string
	followerRead           bool
	logDir                 string
	logLevel               string
	ebsEndpoint            string
	servicePath            string
	volType                int
	cacheAction            int
	ebsBlockSize           int
	enableBcache           bool
	readBlockThread        int
	writeBlockThread       int
	cacheRuleKey           string
	cacheThreshold         int
	enableSummary          bool
	secretKey              string
	accessKey              string
	subDir                 string
	pushAddr               string
	cluster                string
	dirChildrenNumLimit    uint32
	enableAudit            bool
	volStorageClass        uint32
	volAllowedStorageClass []uint32
	cacheDpStorageClass    uint32
	enableInnerReq         bool

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
	mu   sync.Mutex
}

//export cfs_get_xattr
func cfs_get_xattr(id C.int64_t, path *C.char, key *C.char) *C.char {
	dstPath := C.GoString(path)
	xattrKey := C.GoString(key)
	log.LogDebugf("cfs_get_xattr path(%v) key(%v) begin", dstPath, xattrKey)

	c, exist := getClient(int64(id))
	if !exist {
		log.LogErrorf("cfs_get_xattr path(%v) key(%v) failed, client not exist", dstPath, xattrKey)
		return C.CString("")
	}

	info, err := c.lookupPath(c.absPath(dstPath))
	if err != nil {
		log.LogErrorf("cfs_get_xattr path(%v) key(%v) failed, not found path", dstPath, xattrKey)
		return C.CString("")
	}

	ino := info.Inode
	xattrInfo, err := c.mw.XAttrGet_ll(ino, xattrKey)
	if err != nil {
		log.LogErrorf("cfs_get_xattr path(%v) ino(%v) key(%v) failed, err(%v)", dstPath, ino, xattrKey, err)
		return C.CString("")
	}
	value := xattrInfo.Get(xattrKey)
	log.LogDebugf("cfs_get_xattr path(%v) ino(%v) key(%v) value(%s) success,", dstPath, ino, xattrKey, value)
	return C.CString(string(value))
}

//export cfs_get_accessFiles
func cfs_get_accessFiles(id C.int64_t, path *C.char, depth C.int, goroutine_num C.int, accessFileInfo []C.struct_cfs_access_file_info, count C.int) (n C.int) {
	dstPath := C.GoString(path)
	maxDepth := int32(depth)
	goroutineNum := int32(goroutine_num)
	log.LogDebugf("cfs_get_accessFiles path(%v) depth(%v)", dstPath, depth)

	c, exist := getClient(int64(id))
	if !exist {
		log.LogErrorf("cfs_get_accessFiles path(%v) failed, client not exist", dstPath)
		return -1
	}

	inodeInfo, err := c.lookupPath(c.absPath(dstPath))
	if err != nil {
		log.LogErrorf("cfs_get_accessFiles path(%v) failed, not found path", dstPath)
		return -1
	}

	ino := inodeInfo.Inode

	infos, _ := c.mw.GetAccessFileInfo(dstPath, ino, maxDepth, goroutineNum)

	n = 0
	for i, info := range infos {
		if i >= int(count) {
			log.LogInfof("cfs_get_accessFiles too many infos, len(%v) count(%v)", len(infos), int(count))
			break
		}
		C.memcpy(unsafe.Pointer(&accessFileInfo[i].dir[0]), C.CBytes([]byte(info.Dir)), C.size_t(len(info.Dir)))
		C.memcpy(unsafe.Pointer(&accessFileInfo[i].accessFileCountSsd[0]), C.CBytes([]byte(info.AccessFileCountSsd)), C.size_t(len(info.AccessFileCountSsd)))
		C.memcpy(unsafe.Pointer(&accessFileInfo[i].accessFileSizeSsd[0]), C.CBytes([]byte(info.AccessFileSizeSsd)), C.size_t(len(info.AccessFileSizeSsd)))
		C.memcpy(unsafe.Pointer(&accessFileInfo[i].accessFileCountHdd[0]), C.CBytes([]byte(info.AccessFileCountHdd)), C.size_t(len(info.AccessFileCountHdd)))
		C.memcpy(unsafe.Pointer(&accessFileInfo[i].accessFileSizeHdd[0]), C.CBytes([]byte(info.AccessFileSizeHdd)), C.size_t(len(info.AccessFileSizeHdd)))
		C.memcpy(unsafe.Pointer(&accessFileInfo[i].accessFileCountBlobStore[0]), C.CBytes([]byte(info.AccessFileCountBlobStore)), C.size_t(len(info.AccessFileCountBlobStore)))
		C.memcpy(unsafe.Pointer(&accessFileInfo[i].accessFileSizeBlobStore[0]), C.CBytes([]byte(info.AccessFileSizeBlobStore)), C.size_t(len(info.AccessFileSizeBlobStore)))
		n++
	}

	return n
}

//export cfs_list_vols
func cfs_list_vols(id C.int64_t, volsInfo []C.struct_cfs_vol_info, count C.int) (n C.int) {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	vols, err := c.mw.ListVols("")
	if err != nil {
		return errorToStatus(err)
	}

	n = 0
	for i, vol := range vols {
		if i >= int(count) {
			break
		}
		C.memcpy(unsafe.Pointer(&volsInfo[i].name[0]), C.CBytes([]byte(vol.Name)), C.size_t(len(vol.Name)))
		C.memcpy(unsafe.Pointer(&volsInfo[i].owner[0]), C.CBytes([]byte(vol.Owner)), C.size_t(len(vol.Owner)))
		volsInfo[i].create_time = C.int64_t(vol.CreateTime)
		volsInfo[i].status = C.uint8_t(vol.Status)
		volsInfo[i].total_size = C.uint64_t(vol.TotalSize)
		volsInfo[i].used_size = C.uint64_t(vol.UsedSize)
		n++
	}

	return n
}

//export cfs_IsDir
func cfs_IsDir(mode C.mode_t) C.int {
	var isDir uint8 = 0
	if (mode & C.S_IFMT) == C.S_IFDIR {
		isDir = 1
	}
	return C.int(isDir)
}

//export cfs_IsRegular
func cfs_IsRegular(mode C.mode_t) C.int {
	var isRegular uint8 = 0
	if (mode & C.S_IFMT) == C.S_IFREG {
		isRegular = 1
	}
	return C.int(isRegular)
}

//export cfs_symlink
func cfs_symlink(id C.int64_t, src_path *C.char, dst_path *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	fullSrcPath := c.absPath(C.GoString(src_path))
	fullDstPath := c.absPath(C.GoString(dst_path))
	parent_dir := path.Dir(fullDstPath)
	filename := path.Base(fullDstPath)

	info, err := c.lookupPath(parent_dir)
	if err != nil {
		return errorToStatus(err)
	}

	parentIno := info.Inode
	info, err = c.mw.Create_ll(parentIno, filename, proto.Mode(os.ModeSymlink|os.ModePerm), 0, 0, []byte(fullSrcPath), fullDstPath, false)
	if err != nil {
		log.LogErrorf("Symlink: parent(%v) NewName(%v) err(%v)\n", parentIno, filename, err)
		return errorToStatus(err)
	}

	c.ic.Put(info)
	log.LogDebugf("Symlink: src_path(%s) dst_path(%s)\n", fullSrcPath, fullDstPath)

	return statusOK
}

//export cfs_link
func cfs_link(id C.int64_t, src_path *C.char, dst_path *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	fullSrcPath := c.absPath(C.GoString(src_path))
	info, err := c.lookupPath(fullSrcPath)
	if err != nil {
		return errorToStatus(err)
	}

	src_ino := info.Inode
	if !proto.IsRegular(info.Mode) {
		log.LogErrorf("Link: not regular, src_path(%s) src_ino(%v) mode(%v)\n", fullSrcPath, src_ino, proto.OsMode(info.Mode))
		return statusEPERM
	}

	fullDstPath := c.absPath(C.GoString(dst_path))
	parent_dir := path.Dir(fullDstPath)
	filename := path.Base(fullDstPath)
	info, err = c.lookupPath(parent_dir)
	if err != nil {
		return errorToStatus(err)
	}
	parentIno := info.Inode

	info, err = c.mw.Link(parentIno, filename, src_ino, fullDstPath)
	if err != nil {
		log.LogErrorf("Link: src_path(%s) src_ino(%v) dst_path(%s) parent(%v) err(%v)\n", fullSrcPath, src_ino, fullDstPath, parentIno, err)
		return errorToStatus(err)
	}

	c.ic.Put(info)
	log.LogDebugf("Link: src_path(%s) src_ino(%v) dst_path(%s) dst_ino(%v) parent(%v)\n", fullSrcPath, src_ino, fullDstPath, info.Inode, parentIno)

	return statusOK
}

//export cfs_get_dir_lock
func cfs_get_dir_lock(id C.int64_t, path *C.char, lock_id *C.int64_t, valid_time **C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	log.LogDebugf("cfs_get_dir_lock begin path(%s)\n", c.absPath(C.GoString(path)))

	info, err := c.lookupPath(c.absPath(C.GoString(path)))
	if err != nil {
		return errorToStatus(err)
	}

	ino := info.Inode
	dir_lock, err := c.getDirLock(ino)
	if err != nil {
		log.LogErrorf("getDirLock failed, path(%s) ino(%v) err(%v)", c.absPath(C.GoString(path)), ino, err)
		return errorToStatus(err)
	}
	if len(dir_lock) == 0 {
		log.LogDebugf("dir(%s) is not locked\n", c.absPath(C.GoString(path)))
		return errorToStatus(syscall.ENOENT)
	}

	parts := strings.Split(string(dir_lock), "|")
	lockIdStr := parts[0]
	lease := parts[1]
	lockId, _ := strconv.Atoi(lockIdStr)
	*lock_id = C.int64_t(lockId)
	*valid_time = C.CString(lease)

	log.LogDebugf("cfs_get_dir_lock end path(%s) lock_id(%d) lease(%s)\n", c.absPath(C.GoString(path)), lockId, lease)
	return statusOK
}

//export cfs_lock_dir
func cfs_lock_dir(id C.int64_t, path *C.char, lease C.uint64_t, lockId C.int64_t) C.int64_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.int64_t(statusEINVAL)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	dirpath := c.absPath(C.GoString(path))

	log.LogDebugf("cfs_lock_dir path(%s) lease(%d) lockId(%d)\n", dirpath, lease, lockId)

	info, err := c.lookupPath(dirpath)
	if err != nil {
		log.LogErrorf("cfs_lock_dir lookupPath failed, err%v\n", err)
		return C.int64_t(errorToStatus(err))
	}

	ino := info.Inode
	retLockId, err := c.lockDir(ino, uint64(lease), int64(lockId))
	if err != nil {
		log.LogErrorf("cfs_lock_dir failed, dir(%s) ino(%v) err(%v)", dirpath, ino, err)
		return C.int64_t(errorToStatus(err))
	}

	log.LogDebugf("cfs_lock_dir success dir(%s) ino(%v) retLockId(%d)\n", dirpath, ino, retLockId)
	return C.int64_t(retLockId)
}

//export cfs_unlock_dir
func cfs_unlock_dir(id C.int64_t, path *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	info, err := c.lookupPath(c.absPath(C.GoString(path)))
	if err != nil {
		return errorToStatus(err)
	}

	ino := info.Inode
	if err = c.unlockDir(ino, 0); err != nil {
		log.LogErrorf("unlockDir failed, ino(%v) err(%v)", ino, err)
		return errorToStatus(err)
	}

	return statusOK
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
	case "enableInnerReq":
		if v == "true" {
			c.enableInnerReq = true
		} else {
			c.enableInnerReq = false
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
		stat.mode = C.uint32_t(C.S_IFREG) | C.uint32_t(info.Mode&0o777)
	} else if proto.IsDir(info.Mode) {
		stat.mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(info.Mode&0o777)
	} else if proto.IsSymlink(info.Mode) {
		stat.mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(info.Mode&0o777)
	} else {
		stat.mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(info.Mode&0o777)
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

	fuseMode := uint32(mode) & uint32(0o777)
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
				auditlog.LogClientOp("Create", dirpath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
			} else {
				auditlog.LogClientOp("Create", dirpath, "nil", err, time.Since(start).Microseconds(), 0, 0)
			}
		}()
		newInfo, err := c.create(dirInfo.Inode, name, fuseMode, absPath)
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
	f := c.allocFD(info.Inode, fuseFlags, fuseMode, fileCache, info.Size, parentIno, absPath, info.StorageClass)
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

	f := c.getFile(uint(fd))
	if f == nil {
		return
	}

	info := c.ic.Get(f.ino)
	if info == nil {
		info, _ = c.mw.InodeGet_ll(f.ino)
	}

	f = c.releaseFD(uint(fd))
	// Consistent with cfs open, do close and closeStream only if f is regular file
	if f != nil && info != nil && proto.IsRegular(info.Mode) {
		c.flush(f)
		c.closeStream(f)
	}
}

//export cfs_truncate
func cfs_truncate(id C.int64_t, fd C.int, size C.size_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}
	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}
	if err := c.truncate(f, int(size)); err != nil {
		return statusEIO
	}
	return statusOK
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
		if proto.IsHot(c.volType) || proto.IsStorageClassReplica(f.storageClass) {
			wait = true
		}
	}
	if f.flags&uint32(C.O_APPEND) != 0 || proto.IsCold(c.volType) || proto.IsStorageClassBlobStore(f.storageClass) {
		flags |= proto.FlagsAppend
		flags |= proto.FlagsSyncWrite
	}

	n, err := c.write(f, int(off), buffer, flags)
	if err != nil {
		if err == syscall.ENOSPC {
			return C.ssize_t(statusENOSPC)
		}
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
			stats[i].mode = C.uint32_t(C.S_IFREG) | C.uint32_t(infos[i].Mode&0o777)
		} else if proto.IsDir(infos[i].Mode) {
			stats[i].mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(infos[i].Mode&0o777)
		} else if proto.IsSymlink(infos[i].Mode) {
			stats[i].mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(infos[i].Mode&0o777)
		} else {
			stats[i].mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(infos[i].Mode&0o777)
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
func cfs_refreshsummary(id C.int64_t, path *C.char, goroutine_num C.int, unit *C.char, split *C.char) C.int {
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
	err = c.mw.RefreshSummary_ll(ino, goroutineNum, C.GoString(unit), C.GoString(split))
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
	inodeIDS := make([]uint64, count)
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
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFREG) | C.uint32_t(infos[i].Mode&0o777)
		} else if proto.IsDir(infos[i].Mode) {
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(infos[i].Mode&0o777)
		} else if proto.IsSymlink(infos[i].Mode) {
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(infos[i].Mode&0o777)
		} else {
			direntsInfo[index].stat.mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(infos[i].Mode&0o777)
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
			auditlog.LogClientOp("Mkdir", dirpath, "nil", gerr, time.Since(start).Microseconds(), gino, 0)
		} else {
			auditlog.LogClientOp("Mkdir", dirpath, "nil", gerr, time.Since(start).Microseconds(), 0, 0)
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
				info, err := c.mkdir(pino, dir, uint32(mode), dirpath)

				if err != nil {
					if err != syscall.EEXIST {
						gerr = err
						return errorToStatus(err)
					}
					// if dir already exist, lookup and assign to child
					child_ino, _, err := c.mw.Lookup_ll(pino, dir)
					if err != nil {
						gerr = err
						return errorToStatus(err)
					}
					child = child_ino
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
			auditlog.LogClientOp("Rmdir", absPath, "nil", err, time.Since(start).Microseconds(), 0, 0)
		} else {
			auditlog.LogClientOp("Rmdir", absPath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
		}
	}()
	dirpath, name := gopath.Split(absPath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, true, absPath)
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
			auditlog.LogClientOp("Unlink", absPath, "nil", err, time.Since(start).Microseconds(), 0, 0)
		} else {
			auditlog.LogClientOp("Unlink", absPath, "nil", err, time.Since(start).Microseconds(), info.Inode, 0)
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

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, false, absPath)
	if err != nil {
		return errorToStatus(err)
	}

	if info != nil {
		_ = c.mw.Evict(info.Inode, absPath)
		c.ic.Delete(info.Inode)
		c.dc.Delete(absPath)
	}
	return 0
}

//export cfs_rename
func cfs_rename(id C.int64_t, from *C.char, to *C.char, overwritten bool) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	c.mu.Lock()
	start := time.Now()
	var err error

	absFrom := c.absPath(C.GoString(from))
	absTo := c.absPath(C.GoString(to))

	defer func() {
		defer c.mu.Unlock()
		auditlog.LogClientOp("Rename", absFrom, absTo, err, time.Since(start).Microseconds(), 0, 0)
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

	err = c.mw.Rename_ll(srcDirInfo.Inode, srcName, dstDirInfo.Inode, dstName, absFrom, absTo, overwritten)
	c.ic.Delete(srcDirInfo.Inode)
	c.ic.Delete(dstDirInfo.Inode)
	c.dc.Delete(absFrom)
	c.dc.Delete(absTo)
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
			summary.filesHdd = C.int64_t(cacheSummaryInfo.FilesHdd)
			summary.filesSsd = C.int64_t(cacheSummaryInfo.FilesSsd)
			summary.filesBlobStore = C.int64_t(cacheSummaryInfo.FilesBlobStore)
			summary.fbytesHdd = C.int64_t(cacheSummaryInfo.FbytesHdd)
			summary.fbytesSsd = C.int64_t(cacheSummaryInfo.FbytesSsd)
			summary.fbytesBlobStore = C.int64_t(cacheSummaryInfo.FbytesBlobStore)
			summary.subdirs = C.int64_t(cacheSummaryInfo.Subdirs)
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

	summary.filesHdd = C.int64_t(summaryInfo.FilesHdd)
	summary.filesSsd = C.int64_t(summaryInfo.FilesSsd)
	summary.filesBlobStore = C.int64_t(summaryInfo.FilesBlobStore)
	summary.fbytesHdd = C.int64_t(summaryInfo.FbytesHdd)
	summary.fbytesSsd = C.int64_t(summaryInfo.FbytesSsd)
	summary.fbytesBlobStore = C.int64_t(summaryInfo.FbytesBlobStore)
	summary.subdirs = C.int64_t(summaryInfo.Subdirs)
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
	masters := strings.Split(c.masterAddr, ",")
	if c.logDir != "" {
		if c.logLevel == "" {
			c.logLevel = "WARN"
		}
		level := parseLogLevel(c.logLevel)
		log.InitLog(c.logDir, "libcfs", level, nil, log.DefaultLogLeftSpaceLimitRatio)
		once.Do(func() {
			stat.NewStatistic(c.logDir, "libcfs", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
			log.LogDebugf("start NewStatistic")
		})
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
		InnerReq:      c.enableInnerReq,
	}); err != nil {
		log.LogErrorf("newClient NewMetaWrapper failed(%v)", err)
		return err
	}
	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:                      c.volName,
		Masters:                     masters,
		FollowerRead:                c.followerRead,
		OnAppendExtentKey:           mw.AppendExtentKey,
		OnSplitExtentKey:            mw.SplitExtentKey,
		OnGetExtents:                mw.GetExtents,
		OnTruncate:                  mw.Truncate,
		BcacheEnable:                c.enableBcache,
		OnLoadBcache:                c.bc.Get,
		OnCacheBcache:               c.bc.Put,
		OnEvictBcache:               c.bc.Evict,
		DisableMetaCache:            true,
		VolStorageClass:             c.volStorageClass,
		VolAllowedStorageClass:      c.volAllowedStorageClass,
		VolCacheDpStorageClass:      c.cacheDpStorageClass,
		OnRenewalForbiddenMigration: mw.RenewalForbiddenMigration,
		OnForbiddenMigration:        mw.ForbiddenMigration,
		MetaWrapper:                 mw,
	}); err != nil {
		log.LogErrorf("newClient NewExtentClient failed(%v)", err)
		return
	}

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
	mc := masterSDK.NewMasterClientFromString(c.masterAddr, false)
	var userInfo *proto.UserInfo
	if userInfo, err = mc.UserAPI().GetAKInfo(c.accessKey); err != nil {
		return
	}
	if userInfo.SecretKey != c.secretKey {
		err = proto.ErrNoPermission
		return
	}
	policy := userInfo.Policy
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

func (c *client) allocFD(ino uint64, flags, mode uint32, fileCache bool, fileSize uint64, parentInode uint64, path string, storageClass uint32) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	fd, ok := c.fdset.NextClear(0)
	if !ok || fd > maxFdNum {
		return nil
	}
	c.fdset.Set(fd)
	f := &file{fd: fd, ino: ino, flags: flags, mode: mode, pino: parentInode, path: path, storageClass: storageClass}
	if flags&0x0f != syscall.O_RDONLY {
		f.openForWrite = true
	}
	if proto.IsCold(c.volType) || proto.IsStorageClassBlobStore(storageClass) {
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
			StorageClass:    storageClass,
		}
		f.fileWriter.FreeCache()
		switch flags & 0xff {
		case syscall.O_RDONLY:
			f.fileReader = blobstore.NewReader(clientConf)
			f.fileWriter = nil
		case syscall.O_WRONLY:
			f.fileWriter = blobstore.NewWriter(clientConf)
			f.fileReader = nil
		case syscall.O_RDWR:
			f.fileReader = blobstore.NewReader(clientConf)
			f.fileWriter = blobstore.NewWriter(clientConf)
		default:
			f.fileWriter = blobstore.NewWriter(clientConf)
			f.fileReader = nil
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

func (c *client) lockDir(ino uint64, lease uint64, lockId int64) (retLockId int64, err error) {
	return c.mw.LockDir(ino, lease, lockId)
}

func (c *client) unlockDir(ino uint64, lockId int64) error {
	_, err := c.mw.LockDir(ino, 0, lockId)
	return err
}

func (c *client) getDirLock(ino uint64) ([]byte, error) {
	info, err := c.mw.XAttrGet_ll(ino, "dir_lock")
	if err != nil {
		log.LogErrorf("getDirLock failed, ino(%v) err(%v)", ino, err)
		return []byte(""), err
	}
	value := info.Get("dir_lock")
	log.LogDebugf("getDirLock success, ino(%v) value(%s)", ino, string(value))
	return value, nil
}

func (c *client) setattr(info *proto.InodeInfo, valid uint32, mode, uid, gid uint32, atime, mtime int64) error {
	// Only rwx mode bit can be set
	if valid&proto.AttrMode != 0 {
		fuseMode := mode & uint32(0o777)
		mode = info.Mode &^ uint32(0o777) // clear rwx mode bit
		mode |= fuseMode
	}
	return c.mw.Setattr(info.Inode, valid, mode, uid, gid, atime, mtime)
}

func (c *client) create(pino uint64, name string, mode uint32, fullPath string) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0o777
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil, fullPath, false)
}

func (c *client) mkdir(pino uint64, name string, mode uint32, fullPath string) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0o777
	fuseMode |= uint32(os.ModeDir)
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil, fullPath, false)
}

func (c *client) openStream(f *file) {
	isCache := false
	if proto.IsCold(c.volType) || proto.IsStorageClassBlobStore(f.storageClass) {
		isCache = true
	}
	_ = c.ec.OpenStream(f.ino, f.openForWrite, isCache)
}

func (c *client) closeStream(f *file) {
	_ = c.ec.CloseStream(f.ino)
	_ = c.ec.EvictStream(f.ino)
	f.fileWriter.FreeCache()
	f.fileWriter = nil
	f.fileReader = nil
}

func (c *client) flush(f *file) error {
	if proto.IsHot(c.volType) || proto.IsStorageClassReplica(f.storageClass) {
		return c.ec.Flush(f.ino)
	} else {
		if f.fileWriter != nil {
			return f.fileWriter.Flush(f.ino, c.ctx(c.id, f.ino))
		}
	}
	return nil
}

func (c *client) truncate(f *file, size int) error {
	err := c.ec.Truncate(c.mw, f.pino, f.ino, size, f.path)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) write(f *file, offset int, data []byte, flags int) (n int, err error) {
	if proto.IsHot(c.volType) || proto.IsStorageClassReplica(f.storageClass) {
		c.ec.GetStreamer(f.ino).SetParentInode(f.pino) // set the parent inode
		checkFunc := func() error {
			if !c.mw.EnableQuota {
				return nil
			}

			if ok := c.ec.UidIsLimited(0); ok {
				return syscall.ENOSPC
			}

			if c.mw.IsQuotaLimitedById(f.ino, true, false) {
				return syscall.ENOSPC
			}
			return nil
		}
		n, err = c.ec.Write(f.ino, offset, data, flags, checkFunc, f.storageClass, false)
	} else {
		n, err = f.fileWriter.Write(c.ctx(c.id, f.ino), offset, data, flags)
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *client) read(f *file, offset int, data []byte) (n int, err error) {
	if proto.IsHot(c.volType) || proto.IsStorageClassReplica(f.storageClass) {
		n, err = c.ec.Read(f.ino, data, offset, len(data), f.storageClass, false)
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
	c.volStorageClass = volumeInfo.VolStorageClass
	c.volAllowedStorageClass = volumeInfo.AllowedStorageClass
	c.cacheDpStorageClass = volumeInfo.CacheDpStorageClass

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

func main() {
	// do nothing
}
