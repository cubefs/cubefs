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
	"C"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
)

const (
	buckets = 4
)

var (
	nextId        uint64
	volumeBuckets [buckets]*volumeBucket // mapping: volume name -> volume instance
	statusOK      = 0
	statusEIO     = errorToStatus(syscall.EIO)
)

func init() {
	for i := 0; i < buckets; i++ {
		volumeBuckets[i] = &volumeBucket{
			volumes: make(map[uint64]*volume),
		}
	}
}

type volume struct {
	id uint64
	mw *meta.MetaWrapper
	ec *stream.ExtentClient
}

type volumeBucket struct {
	volumes map[uint64]*volume
	mu      sync.RWMutex
}

func (m *volumeBucket) get(id uint64) (client *volume, exist bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, exist = m.volumes[id]
	return
}

func (m *volumeBucket) put(id uint64, c *volume) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.volumes[id] = c
}

func (m *volumeBucket) remove(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.volumes, id)
}

func putClient(id uint64, c *volume) {
	volumeBuckets[id%buckets].put(id, c)
}

func getClient(id uint64) (c *volume, exist bool) {
	c, exist = volumeBuckets[id%buckets].get(id)
	return
}

func removeClient(id uint64) {
	volumeBuckets[id%buckets].remove(id)
}

//export ClientCount
func ClientCount() int {
	total := 0
	for i := 0; i < buckets; i++ {
		total += len(volumeBuckets[i].volumes)
	}
	return total
}

//export NewClient
func NewClient(volumeName *C.char, followerRead bool, masterAddr *C.char) (clientId uint64, err error) {
	clientId = atomic.AddUint64(&nextId, 1)
	var masters = strings.Split(C.GoString(masterAddr), ",")
	var mw *meta.MetaWrapper
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        C.GoString(volumeName),
		Masters:       masters,
		ValidateOwner: false,
	}); err != nil {
		return
	}
	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:            C.GoString(volumeName),
		Masters:           masters,
		FollowerRead:      followerRead,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
	}); err != nil {
		return
	}
	var c = &volume{
		id: clientId,
		mw: mw,
		ec: ec,
	}
	volumeBuckets[clientId%buckets].put(clientId, c)
	return
}

//export CloseClient
func CloseClient(clientId uint64) {
	if c, exist := getClient(clientId); exist {
		_ = c.ec.Close()
		_ = c.mw.Close()
		removeClient(clientId)
	}
}

//export Create
func Create(clientId uint64, parentId uint64, name *C.char, mode, uid, gid uint32, target *C.char) (inode uint64, status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return 0, statusEIO
	}
	var err error
	var inodeInfo *proto.InodeInfo
	if inodeInfo, err = c.mw.Create_ll(parentId, C.GoString(name), mode, uid, gid, []byte(C.GoString(target))); err != nil {
		return 0, statusEIO
	}
	inode = inodeInfo.Inode
	return
}

//export Lookup
func Lookup(clientId uint64, parentId uint64, name *C.char) (inode uint64, mode uint32, status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return 0, 0, statusEIO
	}
	var err error
	if inode, mode, err = c.mw.Lookup_ll(parentId, C.GoString(name)); err != nil {
		status = errorToStatus(err)
	}
	return
}

//export InodeGet
func InodeGet(clientId uint64, inode uint64) (mode uint32, size uint64, gid, uid uint32, at, mt, ct int64, target *C.char, status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		status = statusEIO
		return
	}
	var err error
	var inodeInfo *proto.InodeInfo
	if inodeInfo, err = c.mw.InodeGet_ll(inode); err != nil {
		status = errorToStatus(err)
		return
	}
	mode = inodeInfo.Mode
	gid = inodeInfo.Gid
	uid = inodeInfo.Uid
	size = inodeInfo.Size
	at = inodeInfo.AccessTime.Unix()
	mt = inodeInfo.ModifyTime.Unix()
	ct = inodeInfo.CreateTime.Unix()
	target = C.CString(string(inodeInfo.Target))
	return
}

//export Delete
func Delete(clientId uint64, parentId uint64, name *C.char, isDir bool) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	var err error
	if _, err = c.mw.Delete_ll(parentId, C.GoString(name), isDir); err != nil {
		status = errorToStatus(err)
	}
	return
}

//export Evict
func Evict(clientId uint64, inode uint64) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	var err error
	if err = c.mw.Evict(inode); err != nil {
		status = errorToStatus(err)
	}
	return
}

//export Rename
func Rename(clientId uint64, srcParentID uint64, srcName *C.char, dstParentID uint64, dstName *C.char) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	var err error
	if err = c.mw.Rename_ll(srcParentID, C.GoString(srcName), dstParentID, C.GoString(dstName)); err != nil {
		status = errorToStatus(err)
	}
	return
}

//export Readdir
func Readdir(clientId uint64, parentId uint64) (result *C.char, status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return C.CString(""), statusEIO
	}
	var err error
	var dentries []proto.Dentry
	if dentries, err = c.mw.ReadDir_ll(parentId); err != nil {
		return C.CString(""), errorToStatus(err)
	}
	var encoded []byte
	if encoded, err = json.Marshal(dentries); err != nil {
		return C.CString(""), statusEIO
	}
	result = C.CString(string(encoded))
	return
}

//export OpenStream
func OpenStream(clientId uint64, inode uint64) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	return errorToStatus(c.ec.OpenStream(inode))
}

//export CloseStream
func CloseStream(clientId uint64, inode uint64) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	return errorToStatus(c.ec.CloseStream(inode))
}

//export EvictStream
func EvictStream(clientId uint64, inode uint64) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	return errorToStatus(c.ec.EvictStream(inode))
}

//export Write
func Write(clientId uint64, inode uint64, offset int, data *string) (write int, status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return 0, statusEIO
	}
	var err error
	if write, err = c.ec.Write(inode, offset, []byte(*data), false); err != nil {
		status = errorToStatus(err)
	}
	return
}

//export Read
func Read(clientId uint64, inode uint64, data *string, offset int, size int) (read int, status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return 0, statusEIO
	}
	var err error
	if read, err = c.ec.Read(inode, []byte(*data), offset, size); err != nil {
		status = errorToStatus(err)
	}
	return
}

//export Truncate
func Truncate(clientId uint64, inode uint64, size int) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	return errorToStatus(c.ec.Truncate(inode, size))
}

//export Flush
func Flush(clientId uint64, inode uint64) (status int) {
	var c *volume
	var exist bool
	if c, exist = getClient(clientId); !exist {
		return statusEIO
	}
	return errorToStatus(c.ec.Flush(inode))
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

func main() {
}
