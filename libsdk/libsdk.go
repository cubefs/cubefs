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
	uint32_t valid;
	uint32_t nameLen;
    char name[256];
};

struct cfs_dirent {
    uint64_t ino;
    char     name[256];
	char     d_type;
};

struct cfs_open_res {
	uint64_t fd;
	uint64_t size;
	uint64_t pos;
};

struct cfs_countdir_res {
	uint64_t fd;
	uint32_t num;
};


*/
import "C"

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"os"
	gopath "path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
)

const (
	slots = 4
)

const (
	defaultBlkSize = uint32(1) << 12
)

var (
	nextId        uint64
	clientBuckets [slots]*clientBucket
)

var (
	statusOK = 0
	// error status must be minus value
	statusEIO         = -errorToStatus(syscall.EIO)
	statusEINVAL      = -errorToStatus(syscall.EINVAL)
	statusEEXIST      = -errorToStatus(syscall.EEXIST)
	statusEBADFD      = -errorToStatus(syscall.EBADFD)
	statusEACCES      = -errorToStatus(syscall.EACCES)
	statusINVALIDPATH = -1024
)

const (
	attrMode uint32 = 1 << iota
	attrUid
	attrGid
	attrModifyTime
	attrAccessTime
	attrSize
)

func init() {
	for i := 0; i < slots; i++ {
		clientBuckets[i] = &clientBucket{
			clients: make(map[uint64]*client),
		}
	}
}

func errorToStatus(err error) int {
	if err == nil {
		return 0
	}
	if errno, is := err.(syscall.Errno); is {
		return int(errno)
	}
	return int(syscall.EIO)
}

type clientBucket struct {
	clients map[uint64]*client
	mu      sync.RWMutex
}

func (m *clientBucket) get(id uint64) (client *client, exist bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, exist = m.clients[id]
	return
}

func (m *clientBucket) put(id uint64, c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[id] = c
}

func (m *clientBucket) remove(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, id)
}

func putClient(id uint64, c *client) {
	clientBuckets[id%slots].put(id, c)
}

func getClient(id uint64) (c *client, exist bool) {
	c, exist = clientBuckets[id%slots].get(id)
	return
}

func removeClient(id uint64) {
	clientBuckets[id%slots].remove(id)
}

type file struct {
	fd    uint64
	ino   uint64
	flags uint32
	mode  uint32

	// dir only
	dirp *dirStream
}

type dirStream struct {
	pos      int
	dirents  []proto.Dentry
	namesMap map[uint64]string
	inodes   []proto.InodeInfo
}

type client struct {
	// client id allocated by libsdk
	id uint64

	// mount config
	volName      string
	masterAddr   string
	followerRead bool

	// runtime context
	maxfd  uint64
	fdmap  map[uint64]*file
	fdlock sync.RWMutex

	// server info
	mw *meta.MetaWrapper
	ec *stream.ExtentClient
}

//export cfs_new_client
func cfs_new_client() (id uint64) {
	id = atomic.AddUint64(&nextId, 1)
	c := &client{
		id:    id,
		fdmap: make(map[uint64]*file),
	}
	clientBuckets[id%slots].put(id, c)
	return id
}

//export cfs_set_client
func cfs_set_client(id uint64, key, val string) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}
	switch key {
	case "volName":
		c.volName = val
	case "masterAddr":
		c.masterAddr = val
	case "followerRead":
		if val == "true" {
			c.followerRead = true
		} else {
			c.followerRead = false
		}
	default:
		return statusEINVAL
	}
	return statusOK
}

//export cfs_start_client
func cfs_start_client(id uint64) int {
	c, exist := getClient(id)
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
func cfs_close_client(id uint64) {
	if c, exist := getClient(id); exist {
		if c.ec != nil {
			_ = c.ec.Close()
		}
		if c.mw != nil {
			_ = c.mw.Close()
		}
		removeClient(id)
	}
}

//export cfs_getattr
func cfs_getattr(id uint64, path string, stat *C.struct_cfs_stat_info) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	newpath, err := validPath(path)
	if err != nil {
		return statusINVALIDPATH
	}

	info, err := c.lookupPath(newpath)
	if err != nil {
		fmt.Println("In getattr operation, Failed to lookup path: ", path, " error: ", err)
		return -errorToStatus(err)
	}

	// fill up the stat
	stat.ino = C.uint64_t(info.Inode)
	stat.size = C.uint64_t(info.Size)
	stat.blocks = C.uint64_t(info.Size >> 9)
	stat.nlink = C.uint32_t(info.Nlink)
	stat.blk_size = C.uint32_t(defaultBlkSize)
	stat.uid = C.uint32_t(info.Uid)
	stat.gid = C.uint32_t(info.Gid)

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

	fmt.Println("info.mode", info.Mode, " mode:", stat.mode, " type:", stat.mode&C.S_IFDIR)
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

//export cfs_setattr_by_path
func cfs_setattr_by_path(id uint64, path string, stat *C.struct_cfs_stat_info) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	newpath, err := validPath(path)
	if err != nil {
		return statusINVALIDPATH
	}
	if newpath == "/" {
		return statusINVALIDPATH
	}

	info, err := c.lookupPath(newpath)
	if err != nil {
		fmt.Println("In getattr operation, Failed to lookup path ", path, " error:  ", err)
		return -errorToStatus(err)
	}

	if uint32(stat.valid)&attrSize != 0 {
		err := c.mw.Truncate(info.Inode, uint64(stat.size))
		if err != nil {
			fmt.Println("Failed to truncate path: ", path, " error: ", err)
			return -errorToStatus(err)
		}
	}

	valid := resetAttr(info, stat)
	if valid != 0 {
		err := c.mw.Setattr(info.Inode, valid, info.Mode, info.Uid, info.Gid, info.AccessTime.Unix(),
			info.ModifyTime.Unix())
		if err != nil {
			fmt.Println("Failed to Setattr path: ", path, " error: ", err)
			return -errorToStatus(err)
		}
	}

	return statusOK
}

//export cfs_open
func cfs_open(id uint64, path string, flags int, mode C.mode_t, uid, gid uint32, res *C.struct_cfs_open_res) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	newpath, err := validPath(path)
	if err != nil {
		return statusINVALIDPATH
	}
	if newpath == "/" {
		return statusINVALIDPATH
	}

	fuseMode := uint32(mode) & uint32(0777)
	fuseFlags := uint32(flags &^ 0x8000)
	accFlags := fuseFlags & uint32(C.O_ACCMODE)

	var info *proto.InodeInfo

	fmt.Printf("flags:%d\n", flags)
	fmt.Printf("fuseFlags:%d\n", fuseFlags)
	fmt.Printf("accFlags:%d\n", accFlags)

	fmt.Printf("C.O_RDONLY:%d\n", C.O_RDONLY)
	fmt.Printf("C.O_WRONLY:%d\n", C.O_WRONLY)
	fmt.Printf("C.O_RDWR:%d\n", C.O_RDWR)
	fmt.Printf("C.O_CREAT:%d\n", C.O_CREAT)
	fmt.Printf("C.O_APPEND:%d\n", C.O_APPEND)
	fmt.Printf("C.O_TRUNC:%d\n", C.O_TRUNC)

	fmt.Printf("S_IFREG:%d\n", C.S_IFREG)
	fmt.Printf("S_IFDIR:%d\n", C.S_IFDIR)
	fmt.Printf("S_IFLNK:%d\n", C.S_IFLNK)

	/*
	 * Note that the rwx mode is ignored when using libsdk
	 */
	//if fuseFlags&uint32(C.O_CREAT) != 0 && fuseFlags&uint32(C.O_APPEND) == 0 {
	if fuseFlags&uint32(C.O_CREAT) != 0 {
		if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
			return statusEACCES
		}
		dirpath, name := gopath.Split(newpath)
		dirInfo, err := c.lookupPath(dirpath)
		if err != nil {
			fmt.Println("Failed to lookup the parent dir: ", dirpath, " error: ", err)
			return -errorToStatus(err)
		}
		newInfo, err := c.create(dirInfo.Inode, name, fuseMode, uid, gid)
		if err != nil {
			fmt.Println("Failed to lookup the create file: ", name, " error: ", err)
			return -errorToStatus(err)
		}
		info = newInfo
	} else {
		newInfo, err := c.lookupPath(newpath)
		if err != nil {
			fmt.Println("In cfs_open operation, failed to lookup the path: ", path, " error: ", err)
			return -errorToStatus(err)
		}
		info = newInfo
	}
	fmt.Println("Succ to open: ", path, " info: ", info)

	f := c.allocFD(info.Inode, fuseFlags, fuseMode)

	if proto.IsRegular(info.Mode) {
		c.openStream(f)
		if accFlags == uint32(C.O_WRONLY) || accFlags == uint32(C.O_RDWR) {
			if fuseFlags&uint32(C.O_TRUNC) != 0 && fuseFlags&uint32(C.O_APPEND) == 0 {
				if err := c.truncate(f, 0); err != nil {
					c.closeStream(f)
					c.releaseFD(int(f.fd))
					return statusEIO
				}
				res.size = 0
				res.pos = 0
			} else if fuseFlags&uint32(C.O_TRUNC) == 0 && fuseFlags&uint32(C.O_APPEND) != 0 {
				res.size = C.uint64_t(info.Size)
				res.pos = C.uint64_t(info.Size)
			} else { // O_CREAT
				res.size = 0
				res.pos = 0
			}
		} else { // O_RDONLY
			res.size = C.uint64_t(info.Size)
			res.pos = 0
		}
	}
	fmt.Println("Succ to open: ", path, " fd: ", f.fd, " size:", res.size, " pos:", res.pos)
	res.fd = C.uint64_t(f.fd)
	return statusOK
}

//export cfs_flush
func cfs_flush(id uint64, fd uint64) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(fd)
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
func cfs_close(id uint64, fd uint64) {
	c, exist := getClient(id)
	if !exist {
		return
	}
	f := c.releaseFD(int(fd))
	if f != nil {
		c.flush(f)
		c.closeStream(f)
	}
}

//export cfs_write
func cfs_write(id, fd uint64, off C.off_t, buff *C.char, size C.size_t) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(fd)
	if f == nil {
		fmt.Println("Not found the fd:", fd)
		return statusEBADFD
	}

	accFlags := f.flags & uint32(C.O_ACCMODE)
	if accFlags != uint32(C.O_WRONLY) && accFlags != uint32(C.O_RDWR) {
		return statusEACCES
	}

	var data []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	hdr.Data = uintptr(unsafe.Pointer(buff))
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

	n, err := c.write(f, int(off), data, flags)
	if err != nil {
		fmt.Println("Failed to write to ", f.ino, " error: ", err)
		return statusEIO
	}

	if wait {
		if err = c.flush(f); err != nil {
			return statusEIO
		}
	}

	return n
}

//export cfs_read
func cfs_read(id uint64, fd uint64, off C.size_t, buf *C.char, size C.off_t) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(fd)
	if f == nil {
		return statusEBADFD
	}

	accFlags := f.flags & uint32(C.O_ACCMODE)
	if accFlags == uint32(C.O_WRONLY) {
		return statusEACCES
	}

	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(unsafe.Pointer(buf))
	hdr.Len = int(size)
	hdr.Cap = int(size)

	n, err := c.read(f, int(off), buffer, int(size))
	if n > 0 {
		//fmt.Println("buffer:", buffer)
		return n
	}
	if err != nil {
		fmt.Println("Failed to read, error:", err, " size:", n)
		return statusEIO
	}
	return n
}

/*
 * Note that readdir is not thread-safe according to the POSIX spec.
 */

//export cfs_readdir
func cfs_readdir(id uint64, fd uint64, dirents []C.struct_cfs_dirent, count int) (n int) {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(fd)
	if f == nil {
		return statusEBADFD
	}

	if f.dirp == nil {
		f.dirp = &dirStream{}
		dentries, err := c.mw.ReadDir_ll(f.ino)
		if err != nil {
			return -errorToStatus(err)
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

		// advance cursor
		dirp.pos++
		n++
	}

	return n
}

//export cfs_countdir
func cfs_countdir(id uint64, path string, res *C.struct_cfs_countdir_res) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	newpath, err := validPath(path)
	if err != nil {
		return statusINVALIDPATH
	}

	info, err := c.lookupPath(newpath)
	if err != nil {
		fmt.Println("In countdir operation, failed to lookup: ", path, " error: ", err)
		return -errorToStatus(err)
	}

	dentries, err := c.mw.ReadDir_ll(info.Inode)
	if err != nil {
		fmt.Println("In countdir listattr operation, failed to readdir: ", path, " error:", err)
		return -errorToStatus(err)
	}

	res.fd = C.uint64_t(info.Inode)
	res.num = C.uint32_t(len(dentries))

	return statusOK
}

//export cfs_listattr
func cfs_listattr(id, ino uint64, num uint32, stats *C.struct_cfs_stat_info) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	dentries, err := c.mw.ReadDir_ll(ino)
	if err != nil {
		fmt.Println("In listattr operation, failed to readdir: ", ino, " error:", err)
		return -errorToStatus(err)
	}

	if int(num) != len(dentries) {
		return statusEINVAL
	}

	namesMap := make(map[uint64]string)
	inodes := make([]uint64, 0, len(dentries))
	for _, child := range dentries {
		inodes = append(inodes, child.Inode)
		namesMap[child.Inode] = child.Name
	}

	size := unsafe.Sizeof(*stats)
	infos := c.mw.BatchInodeGet(inodes)

	for i := 0; i < len(inodes); i++ {
		// fill up the stat
		stats.ino = C.uint64_t(infos[i].Inode)
		stats.size = C.uint64_t(infos[i].Size)
		stats.blocks = C.uint64_t(infos[i].Size >> 9)
		stats.nlink = C.uint32_t(infos[i].Nlink)
		stats.blk_size = C.uint32_t(defaultBlkSize)
		stats.uid = C.uint32_t(infos[i].Uid)
		stats.gid = C.uint32_t(infos[i].Gid)

		// fill up the mode
		if proto.IsRegular(infos[i].Mode) {
			stats.mode = C.uint32_t(C.S_IFREG) | C.uint32_t(infos[i].Mode&0777)
		} else if proto.IsDir(infos[i].Mode) {
			stats.mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(infos[i].Mode&0777)
		} else if proto.IsSymlink(infos[i].Mode) {
			stats.mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(infos[i].Mode&0777)
		} else {
			stats.mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(infos[i].Mode&0777)
		}

		// fill up the time struct
		t := infos[i].AccessTime.UnixNano()
		stats.atime = C.uint64_t(t / 1e9)
		stats.atime_nsec = C.uint32_t(t % 1e9)

		t = infos[i].ModifyTime.UnixNano()
		stats.mtime = C.uint64_t(t / 1e9)
		stats.mtime_nsec = C.uint32_t(t % 1e9)

		t = infos[i].CreateTime.UnixNano()
		stats.ctime = C.uint64_t(t / 1e9)
		stats.ctime_nsec = C.uint32_t(t % 1e9)

		name, ok := namesMap[infos[i].Inode]
		if !ok {
			fmt.Println("Not found the name by inodeid:", infos[i].Inode)
			return statusEINVAL
		}
		// fill up name
		nameLen := len(name)
		if nameLen >= 256 {
			nameLen = 255
		}
		hdr := (*reflect.StringHeader)(unsafe.Pointer(&name))
		C.memcpy(unsafe.Pointer(&stats.name[0]), unsafe.Pointer(hdr.Data), C.size_t(nameLen))
		stats.name[nameLen] = 0
		stats.nameLen = C.uint32_t(nameLen)
		stats = (*C.struct_cfs_stat_info)(unsafe.Pointer(uintptr(unsafe.Pointer(stats)) + size))
	}

	return len(dentries)
}

//export cfs_mkdirs
func cfs_mkdirs(id uint64, path string, mode C.mode_t, uid, gid uint32) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	dirpath, err := validPath(path)
	if err != nil {
		return statusINVALIDPATH
	}
	if dirpath == "/" {
		return statusOK
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
				info, err := c.mkdir(pino, dir, uint32(mode), uid, gid)
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
func cfs_rmdir(id uint64, path string, recursive bool) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	newpath, err := validPath(path)
	if err != nil {
		return statusINVALIDPATH
	}

	dirpath, name := gopath.Split(newpath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	if recursive {
		info, err := c.lookupPath(newpath)
		if err != nil {
			return errorToStatus(err)
		}
		if err != nil {
			return errorToStatus(err)
		}
		err = rmdir_recursive(c, info)
		if err != nil {
			fmt.Println("In rmdir operation, failed to rmdir_recursive: ", name, " error: ", err)
			return -errorToStatus(err)
		}
	}

	if newpath == "/" {
		return statusOK
	}

	_, err = c.mw.Delete_ll(dirInfo.Inode, name, true)
	if err != nil {
		fmt.Println("In rmdir operation, failed to delete: ", name, " error: ", err)
		return -errorToStatus(err)
	}
	return statusOK
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
func cfs_unlink(id uint64, path string) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	newpath, err := validPath(path)
	if err != nil {
		return statusINVALIDPATH
	}
	if newpath == "/" {
		return statusINVALIDPATH
	}

	info, err := c.lookupPath(newpath)
	if err != nil {
		fmt.Println("In unlink operation, failed to lookup the parent dir: ", newpath, " error:", err)
		return -errorToStatus(err)
	}
	if proto.IsDir(info.Mode) {
		return statusEINVAL
	}

	dirpath, name := gopath.Split(newpath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		fmt.Println("In unlink operation, failed to lookup the parent dir: ", dirpath, " error:", err)
		return -errorToStatus(err)
	}

	info, err = c.mw.Delete_ll(dirInfo.Inode, name, false)
	if err != nil {
		fmt.Println("Failed to unlink: ", name, " error:", err)
		return -errorToStatus(err)
	}

	_ = c.mw.Evict(info.Inode)
	return statusOK
}

//export cfs_rename
func cfs_rename(id uint64, from, to string) int {
	c, exist := getClient(id)
	if !exist {
		return statusEINVAL
	}

	newfrom, err := validPath(from)
	if err != nil {
		return statusINVALIDPATH
	}
	if newfrom == "/" {
		return statusINVALIDPATH
	}

	newto, err := validPath(to)
	if err != nil {
		return statusINVALIDPATH
	}
	if newto == "/" {
		return statusINVALIDPATH
	}

	if strings.Contains(newto, newfrom) {
		if newto == newfrom {
			return statusEINVAL
		}

		if newto[len(newfrom)] == '/' {
			return statusEINVAL
		}
	}

	srcDirPath, srcName := gopath.Split(newfrom)
	srcDirInfo, err := c.lookupPath(srcDirPath)
	if err != nil {
		return errorToStatus(err)
	}

	dstDirPath, dstName := gopath.Split(newto)

	// mv /d/child /d
	if srcDirPath == (newto + "/") {
		return statusOK
	}

	dstInfo, err := c.lookupPath(newto)
	if err == nil && proto.IsDir(dstInfo.Mode) {
		err = c.mw.Rename_ll(srcDirInfo.Inode, srcName, dstInfo.Inode, srcName)
		if err != nil {
			fmt.Println("In rename operation, failed to rename ", srcName, " to ", srcName, " error:", err)
			return -errorToStatus(err)
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
		return -errorToStatus(err)
	}
	return statusOK
}

// internals

func (c *client) start() (err error) {
	var masters = strings.Split(c.masterAddr, ",")

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
	fd := atomic.AddUint64(&c.maxfd, 1)
	f := &file{fd: fd, ino: ino, flags: flags, mode: mode}
	c.fdlock.Lock()
	c.fdmap[fd] = f
	c.fdlock.Unlock()
	return f
}

func (c *client) getFile(fd uint64) *file {
	c.fdlock.Lock()
	f := c.fdmap[fd]
	c.fdlock.Unlock()
	return f
}

func (c *client) releaseFD(fd int) *file {
	c.fdlock.Lock()
	f, ok := c.fdmap[uint64(fd)]
	if !ok {
		c.fdlock.Unlock()
		return nil
	}
	delete(c.fdmap, uint64(fd))
	c.fdlock.Unlock()
	return f
}

func (c *client) lookupPath(path string) (*proto.InodeInfo, error) {
	ino, err := c.mw.LookupPath(path)
	if err != nil {
		return nil, err
	}
	info, err := c.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (c *client) create(pino uint64, name string, mode, uid, gid uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode
	return c.mw.Create_ll(pino, name, fuseMode, uid, gid, nil)
}

func (c *client) mkdir(pino uint64, name string, mode, uid, gid uint32) (info *proto.InodeInfo, err error) {
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

func (c *client) read(f *file, offset int, data []byte, size int) (n int, err error) {
	n, err = c.ec.Read(f.ino, data, offset, size)
	if err != nil {
		return n, err
	}
	return n, nil
}

func resetAttr(info *proto.InodeInfo, stat *C.struct_cfs_stat_info) (valid uint32) {
	statValid := uint32(stat.valid)
	if statValid&attrMode != 0 {
		info.Mode = proto.Mode(os.FileMode(stat.mode))
		valid |= proto.AttrMode
	}

	if statValid&attrUid != 0 {
		info.Uid = uint32(stat.uid)
		valid |= proto.AttrUid
	}

	if statValid&attrGid != 0 {
		info.Gid = uint32(stat.gid)
		valid |= proto.AttrGid
	}

	if statValid&attrAccessTime != 0 {
		info.AccessTime = time.Unix(int64(stat.atime), 0)
		valid |= proto.AttrAccessTime
	}

	if statValid&attrModifyTime != 0 {
		info.ModifyTime = time.Unix(int64(stat.mtime), 0)
		valid |= proto.AttrModifyTime
	}

	return valid
}

func validPath(path string) (newpath string, err error) {
	if path[0] != '/' {
		fmt.Println("Is not begin with '/")
		err = errors.New("Path should be beign with /.")
		newpath = ""
		return
	}

	return gopath.Clean(path), nil
}

func main() {}
