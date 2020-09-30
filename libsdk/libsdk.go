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

struct cfs_dirent {
    uint64_t ino;
	char     d_type;
    uint32_t     nameLen;
    char     name[256];
};

*/
import "C"

import (
	"fmt"
	"io"
	"os"
	gopath "path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/willf/bitset"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	attrMode uint32 = 1 << iota
	attrUid
	attrGid
	attrModifyTime
	attrAccessTime
	attrSize
)

const (
	defaultBlkSize = uint32(1) << 12

	maxFdNum uint = 1024000
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
)

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
		id:    id,
		fdmap: make(map[uint]*file),
		fdset: bitset.New(maxFdNum),
		cwd:   "/",
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
	flags uint32
	mode  uint32
	size  uint64

	// dir only
	dirp *dirStream
}

type dirStream struct {
	pos     int
	dirents []proto.Dentry
}

type client struct {
	// client id allocated by libsdk
	id int64

	// mount config
	volName      string
	masterAddr   string
	followerRead bool
	logDir       string
	logLevel     string

	// runtime context
	cwd    string // current working directory
	fdmap  map[uint]*file
	fdset  *bitset.BitSet
	fdlock sync.RWMutex

	// server info
	mw *meta.MetaWrapper
	ec *stream.ExtentClient
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
	c, exist := getClient(int64(id))
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

	err = c.setattr(info, uint32(valid), uint32(stat.mode), uint32(stat.uid), uint32(stat.gid), int64(stat.mtime)*1e9+int64(stat.mtime_nsec), int64(stat.atime)*1e9+int64(stat.atime_nsec))

	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

//export cfs_setattr_by_path
func cfs_setattr_by_path(id C.int64_t, path *C.char, stat *C.struct_cfs_stat_info, valid C.int) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	newpath := c.absPath(C.GoString(path))
	if newpath == "/" {
		return statusEINVAL
	}

	info, err := c.lookupPath(newpath)
	if err != nil {
		fmt.Println("In getattr operation, Failed to lookup path ", path, " error:  ", err)
		return errorToStatus(err)
	}

	if uint32(valid)&attrSize != 0 {
		err := c.mw.Truncate(info.Inode, uint64(stat.size))
		if err != nil {
			fmt.Println("Failed to truncate path: ", path, " error: ", err)
			return errorToStatus(err)
		}
	}

	if valid != 0 {
		err = c.setattr(info, uint32(valid), uint32(stat.mode), uint32(stat.uid), uint32(stat.gid), int64(stat.mtime), int64(stat.atime))
		if err != nil {
			fmt.Println("Failed to Setattr path: ", path, " error: ", err)
			return errorToStatus(err)
		}
	}

	return statusOK
}

//export cfs_open
func cfs_open(id C.int64_t, path *C.char, flags C.int, mode C.mode_t, uid, gid C.uint32_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	fuseMode := uint32(mode) & uint32(0777)
	fuseFlags := uint32(flags) &^ uint32(0x8000)
	accFlags := fuseFlags & uint32(C.O_ACCMODE)

	absPath := c.absPath(C.GoString(path))

	var info *proto.InodeInfo

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
		if len(name) == 0 {
			return statusEINVAL
		}
		newInfo, err := c.create(dirInfo.Inode, name, fuseMode, uint32(uid), uint32(gid))
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
		newInfo, err := c.lookupPath(absPath)
		if err != nil {
			return errorToStatus(err)
		}
		info = newInfo
	}

	f := c.allocFD(info.Inode, fuseFlags, fuseMode)
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
			info.Size = 0
		}
	}
	f.size = info.Size
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

// Note: The file size is not change
//export cfs_file_size
func cfs_file_size(id C.int64_t, fd C.int) C.ssize_t {
	c, exist := getClient(int64(id))
	if !exist {
		return C.ssize_t(statusEINVAL)
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return C.ssize_t(statusEBADFD)
	}
	return C.ssize_t(f.size)
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
		wait = true
	}
	if f.flags&uint32(C.O_APPEND) != 0 {
		flags |= proto.FlagsAppend
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

//export cfs_mkdirs
func cfs_mkdirs(id C.int64_t, path *C.char, mode C.mode_t, uid, gid C.uint32_t) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	dirpath := c.absPath(C.GoString(path))
	if dirpath == "/" {
		return statusEEXIST
	}

	pino := proto.RootIno
	dirs := strings.Split(dirpath, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err := c.mw.Lookup_ll(pino, dir)
		if err != nil {
			if err == syscall.ENOENT {
				info, err := c.mkdir(pino, dir, uint32(mode), uint32(uid), uint32(gid))
				if err != nil {
					return errorToStatus(err)
				}
				child = info.Inode
			} else {
				return errorToStatus(err)
			}
		}
		pino = child
	}

	return 0
}

//export cfs_rmdir
func cfs_rmdir(id C.int64_t, path *C.char, recursive bool) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(C.GoString(path))
	dirpath, name := gopath.Split(absPath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	if recursive {
		info, err := c.lookupPath(absPath)
		if err != nil {
			return errorToStatus(err)
		}
		if err != nil {
			return errorToStatus(err)
		}
		err = rmdir_recursive(c, info)
		if err != nil {
			fmt.Println("In rmdir operation, failed to rmdir_recursive: ", name, " error: ", err)
			return errorToStatus(err)
		}
	}

	if absPath == "/" {
		return statusOK
	}

	_, err = c.mw.Delete_ll(dirInfo.Inode, name, true)
	if err != nil {
		fmt.Println("In rmdir operation, failed to delete: ", name, " error: ", err)
		return errorToStatus(err)
	}
	return statusOK

	_, err = c.mw.Delete_ll(dirInfo.Inode, name, true)
	return errorToStatus(err)
}

func rmdir_recursive(c *client, info *proto.InodeInfo) (err error) {
	if info.Nlink <= 2 {
		return nil
	}

	dentries, err := c.mw.ReadDir_ll(info.Inode)
	if err != nil {
		fmt.Println("In rmdir_recursive operation, failed to readdir: ", info.Inode, " error: ", err)
		return err
	}

	for _, child := range dentries {
		var inode *proto.InodeInfo

		inode, err = c.mw.InodeGet_ll(child.Inode)
		if err != nil {
			fmt.Println("In rmdir_recursive operation, failed to inodeget: ", child.Inode, " error: ", err)
			return err
		}

		if proto.IsDir(inode.Mode) {
			err = rmdir_recursive(c, inode)
			if err != nil {
				fmt.Println("In rmdir_recursive operation, failed to invoke self: ", child.Name, " error: ", err)
				return err
			}

			_, err = c.mw.Delete_ll(info.Inode, child.Name, true)
			if err != nil {
				fmt.Println("In rmdir_recursive operation, failed to rmdir: ", child.Name, " error: ", err)
				return err
			}
		} else {
			_, err = c.mw.Delete_ll(info.Inode, child.Name, false)
			if err != nil {
				fmt.Println("In rmdir_recursive operation, failed to unlink: ", child.Name, " error: ", err)
				return err
			}
		}
	}

	return
}

//export cfs_unlink
func cfs_unlink(id C.int64_t, path *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(C.GoString(path))

	info, err := c.lookupPath(absPath)
	if err != nil {
		fmt.Println("In unlink operation, failed to lookup the parent dir: ", absPath, " error:", err)
		return errorToStatus(err)
	}
	if proto.IsDir(info.Mode) {
		return statusEINVAL
	}

	dirpath, name := gopath.Split(absPath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, false)
	if err != nil {
		fmt.Println("Failed to Delete_ll:", absPath, " error:", err)
		return errorToStatus(err)
	}

	_ = c.mw.Evict(info.Inode)
	return 0
}

//export cfs_rename
func cfs_rename(id C.int64_t, from *C.char, to *C.char) C.int {
	c, exist := getClient(int64(id))
	if !exist {
		return statusEINVAL
	}

	absFrom := c.absPath(C.GoString(from))
	absTo := c.absPath(C.GoString(to))
	if strings.Contains(absTo, absFrom) {
		if absTo == absFrom {
			return statusEINVAL
		}
		// handle the /a and /aa
		if absTo[len(absFrom)] == '/' {
			return statusEINVAL
		}
	}

	srcDirPath, srcName := gopath.Split(absFrom)
	srcDirInfo, err := c.lookupPath(srcDirPath)
	if err != nil {
		return errorToStatus(err)
	}

	dstDirPath, dstName := gopath.Split(absTo)

	// mv /d/child /d
	if srcDirPath == (absTo + "/") {
		return statusOK
	}

	dstInfo, err := c.lookupPath(absTo)
	if err == nil && proto.IsDir(dstInfo.Mode) {
		err = c.mw.Rename_ll(srcDirInfo.Inode, srcName, dstInfo.Inode, srcName)
		if err != nil {
			fmt.Println("In rename operation, failed to rename ", srcName, " to ", srcName, " error:", err)
			return errorToStatus(err)
		}
		return statusOK
	}

	dstDirInfo, err := c.lookupPath(dstDirPath)
	if err != nil {
		return errorToStatus(err)
	}
	err = c.mw.Rename_ll(srcDirInfo.Inode, srcName, dstDirInfo.Inode, dstName)
	if err != nil {
		fmt.Println("In rename operation, failed to rename ", srcName, " to ", dstName, " error:", err)
		return errorToStatus(err)
	}
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

	info, err := c.mw.InodeGet_ll(f.ino)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(info, proto.AttrMode, uint32(mode), 0, 0, 0, 0)
	if err != nil {
		return errorToStatus(err)
	}
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
		log.InitLog(c.logDir, "libcfs", log.InfoLevel, nil)
	}

	var mw *meta.MetaWrapper
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        c.volName,
		Masters:       masters,
		ValidateOwner: false,
	}); err != nil {
		return
	}

	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:            c.volName,
		Masters:           masters,
		FollowerRead:      c.followerRead,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
	}); err != nil {
		return
	}

	c.mw = mw
	c.ec = ec
	return nil
}

func (c *client) allocFD(ino uint64, flags, mode uint32) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	fd, ok := c.fdset.NextClear(0)
	if !ok || fd > maxFdNum {
		return nil
	}
	c.fdset.Set(fd)
	f := &file{fd: fd, ino: ino, flags: flags, mode: mode}
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
	return f
}

func (c *client) lookupPath(path string) (*proto.InodeInfo, error) {
	ino, err := c.mw.LookupPath(gopath.Clean(path))
	if err != nil {
		return nil, err
	}
	info, err := c.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (c *client) setattr(info *proto.InodeInfo, valid uint32, mode, uid, gid uint32, mtime, atime int64) error {
	// Only rwx mode bit can be set
	if valid&proto.AttrMode != 0 {
		fuseMode := mode & uint32(0777)
		mode = info.Mode &^ uint32(0777) // clear rwx mode bit
		mode |= fuseMode
	}
	return c.mw.Setattr(info.Inode, valid, mode, uid, gid, atime, mtime)
}

func (c *client) create(pino uint64, name string, mode, uid, gid uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	return c.mw.Create_ll(pino, name, fuseMode, uid, gid, nil)
}

func (c *client) mkdir(pino uint64, name string, mode uint32, uid, gid uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeDir)
	return c.mw.Create_ll(pino, name, fuseMode, uid, gid, nil)
}

func (c *client) openStream(f *file) {
	_ = c.ec.OpenStream(f.ino)
}

func (c *client) closeStream(f *file) {
	_ = c.ec.CloseStream(f.ino)
	_ = c.ec.EvictStream(f.ino)
}

func (c *client) flush(f *file) error {
	return c.ec.Flush(f.ino)
}

func (c *client) truncate(f *file, size int) error {
	return c.ec.Truncate(f.ino, size)
}

func (c *client) write(f *file, offset int, data []byte, flags int) (n int, err error) {
	n, err = c.ec.Write(f.ino, offset, data, flags)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *client) fileSize(f *file) (size int, err error) {
	valid := false
	size, _, valid = c.ec.FileSize(f.ino)
	if !valid {
		info, e := c.mw.InodeGet_ll(f.ino)
		if e != nil {
			err = e
			return
		}
		size = int(info.Size)
	}
	return
}

func (c *client) read(f *file, offset int, data []byte) (n int, err error) {
	n, err = c.ec.Read(f.ino, data, offset, len(data))
	if err != nil && err != io.EOF {
		return 0, err
	}
	return n, nil
}

func main() {}
