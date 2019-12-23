// Copyright 2018 The Chubao Authors.
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

package meta

import (
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"sort"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

// Low-level API, i.e. work with inode

const (
	BatchIgetRespBuf = 1000
)

const (
	OpenRetryInterval = 5 * time.Millisecond
	OpenRetryLimit    = 1000
)

func (mw *MetaWrapper) Statfs() (total, used uint64) {
	total = atomic.LoadUint64(&mw.totalSize)
	used = atomic.LoadUint64(&mw.usedSize)
	return
}

func (mw *MetaWrapper) Create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error) {
	var (
		status       int
		err          error
		info         *proto.InodeInfo
		mp           *MetaPartition
		rwPartitions []*MetaPartition
	)

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Create_ll: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	// Create Inode

	//	mp = mw.getLatestPartition()
	//	if mp != nil {
	//		status, info, err = mw.icreate(mp, mode, target)
	//		if err == nil {
	//			if status == statusOK {
	//				goto create_dentry
	//			} else if status == statusFull {
	//				mw.UpdateMetaPartitions()
	//			}
	//		}
	//	}
	//
	//	rwPartitions = mw.getRWPartitions()
	//	for _, mp = range rwPartitions {
	//		status, info, err = mw.icreate(mp, mode, target)
	//		if err == nil && status == statusOK {
	//			goto create_dentry
	//		}
	//	}

	rwPartitions = mw.getRWPartitions()
	length := len(rwPartitions)
	epoch := atomic.AddUint64(&mw.epoch, 1)
	for i := 0; i < length; i++ {
		index := (int(epoch) + i) % length
		mp = rwPartitions[index]
		status, info, err = mw.icreate(mp, mode, uid, gid, target)
		if err == nil && status == statusOK {
			goto create_dentry
		}
	}
	return nil, syscall.ENOMEM

create_dentry:
	status, err = mw.dcreate(parentMP, parentID, name, info.Inode, mode)
	if err != nil || status != statusOK {
		if status == statusExist {
			return nil, syscall.EEXIST
		} else {
			mw.iunlink(mp, info.Inode)
			mw.ievict(mp, info.Inode)
			return nil, statusToErrno(status)
		}
	}
	return info, nil
}

func (mw *MetaWrapper) Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Lookup_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return 0, 0, syscall.ENOENT
	}

	status, inode, mode, err := mw.lookup(parentMP, parentID, name)
	if err != nil || status != statusOK {
		return 0, 0, statusToErrno(status)
	}
	return inode, mode, nil
}

func (mw *MetaWrapper) InodeGet_ll(inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeGet_ll: No such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.iget(mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	log.LogDebugf("InodeGet_ll: info(%v)", info)
	return info, nil
}

func (mw *MetaWrapper) BatchInodeGet(inodes []uint64) []*proto.InodeInfo {
	var wg sync.WaitGroup

	batchInfos := make([]*proto.InodeInfo, 0)
	resp := make(chan []*proto.InodeInfo, BatchIgetRespBuf)

	mw.RLock()
	for _, mp := range mw.partitions {
		wg.Add(1)
		go mw.batchIget(&wg, mp, inodes, resp)
	}
	mw.RUnlock()

	go func() {
		wg.Wait()
		close(resp)
	}()

	for infos := range resp {
		batchInfos = append(batchInfos, infos...)
	}
	return batchInfos
}

// InodeDelete_ll is a low-level api that removes specified inode immediately
// and do not effect extent data managed by this inode.
func (mw *MetaWrapper) InodeDelete_ll(inode uint64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeDelete: No such partition, ino(%v)", inode)
		return syscall.ENOENT
	}
	status, err := mw.idelete(mp, inode)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("InodeDelete_ll: inode(%v)", inode)
	return nil
}

func (mw *MetaWrapper) BatchGetXAttr(inodes []uint64, keys []string) ([]*proto.XAttrInfo, error) {
	batchInfos := make([]*proto.XAttrInfo, 0)
	var wg sync.WaitGroup
	var errGlobal error

	for _, mp := range mw.partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			xAttrInfos, err := mw.batchGetXAttr(mp, inodes, keys)
			if err != nil {
				errGlobal = err
				log.LogErrorf("BatchGetXAttr: Get xattr from partion %s in batch failed cause : %s", mp.PartitionID, err)
				return
			}
			batchInfos = append(batchInfos, xAttrInfos...)
		}(mp)
	}

	wg.Wait()
	if errGlobal != nil {
		return nil, errGlobal
	}
	log.LogDebugf("BatchGetXAttr: inodes(%v) xattrInfos(%v)", len(inodes), len(batchInfos))
	return batchInfos, nil
}

/*
 * Note that the return value of InodeInfo might be nil without error,
 * and the caller should make sure InodeInfo is valid before using it.
 */
func (mw *MetaWrapper) Delete_ll(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error) {
	var (
		status int
		inode  uint64
		mode   uint32
		err    error
		info   *proto.InodeInfo
		mp     *MetaPartition
	)

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Delete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	if isDir {
		status, inode, mode, err = mw.lookup(parentMP, parentID, name)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		if !proto.IsDir(mode) {
			return nil, syscall.EINVAL
		}
		mp = mw.getPartitionByInode(inode)
		if mp == nil {
			log.LogErrorf("Delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
			return nil, syscall.EAGAIN
		}
		status, info, err = mw.iget(mp, inode)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		if info == nil || info.Nlink > 2 {
			return nil, syscall.ENOTEMPTY
		}
	}

	status, inode, err = mw.ddelete(parentMP, parentID, name)
	if err != nil || status != statusOK {
		if status == statusNoent {
			return nil, nil
		}
		return nil, statusToErrno(status)
	}

	// dentry is deleted successfully but inode is not, still returns success.
	mp = mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
		return nil, nil
	}

	status, info, err = mw.iunlink(mp, inode)
	if err != nil || status != statusOK {
		return nil, nil
	}
	return info, nil
}

func (mw *MetaWrapper) Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string) (err error) {
	var oldInode uint64

	srcParentMP := mw.getPartitionByInode(srcParentID)
	if srcParentMP == nil {
		return syscall.ENOENT
	}
	dstParentMP := mw.getPartitionByInode(dstParentID)
	if dstParentMP == nil {
		return syscall.ENOENT
	}

	// look up for the src ino
	status, inode, mode, err := mw.lookup(srcParentMP, srcParentID, srcName)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	srcMP := mw.getPartitionByInode(inode)
	if srcMP == nil {
		return syscall.ENOENT
	}

	status, _, err = mw.ilink(srcMP, inode)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}

	// create dentry in dst parent
	status, err = mw.dcreate(dstParentMP, dstParentID, dstName, inode, mode)
	if err != nil {
		mw.iunlink(srcMP, inode)
		return syscall.EAGAIN
	}

	// Note that only regular files are allowed to be overwritten.
	if status == statusExist && proto.IsRegular(mode) {
		status, oldInode, err = mw.dupdate(dstParentMP, dstParentID, dstName, inode)
		if err != nil {
			mw.iunlink(srcMP, inode)
			return syscall.EAGAIN
		}
	}

	if status != statusOK {
		mw.iunlink(srcMP, inode)
		return statusToErrno(status)
	}

	// delete dentry from src parent
	status, _, err = mw.ddelete(srcParentMP, srcParentID, srcName)
	if err != nil || status != statusOK {
		var (
			sts int
			e   error
		)
		if oldInode == 0 {
			sts, _, e = mw.ddelete(dstParentMP, dstParentID, dstName)
		} else {
			sts, _, e = mw.dupdate(dstParentMP, dstParentID, dstName, oldInode)
		}
		if e == nil && sts == statusOK {
			mw.iunlink(srcMP, inode)
		}
		return statusToErrno(status)
	}

	mw.iunlink(srcMP, inode)

	if oldInode != 0 {
		inodeMP := mw.getPartitionByInode(oldInode)
		if inodeMP != nil {
			mw.iunlink(inodeMP, oldInode)
		}
	}

	return nil
}

func (mw *MetaWrapper) ReadDir_ll(parentID uint64) ([]proto.Dentry, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, err := mw.readdir(parentMP, parentID)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return children, nil
}

func (mw *MetaWrapper) DentryCreate_ll(parentID uint64, name string, inode uint64, mode uint32) error {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}
	var err error
	var status int
	if status, err = mw.dcreate(parentMP, parentID, name, inode, mode); err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) DentryUpdate_ll(parentID uint64, name string, inode uint64) (oldInode uint64, err error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		err = syscall.ENOENT
		return
	}
	var status int
	status, oldInode, err = mw.dupdate(parentMP, parentID, name, inode)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
		return
	}
	return
}

// Used as a callback by stream sdk
func (mw *MetaWrapper) AppendExtentKey(inode uint64, ek proto.ExtentKey) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.appendExtentKey(mp, inode, ek)
	if err != nil || status != statusOK {
		log.LogErrorf("AppendExtentKey: inode(%v) ek(%v) err(%v) status(%v)", inode, ek, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("AppendExtentKey: ino(%v) ek(%v)", inode, ek)
	return nil
}

// AppendExtentKeys append multiple extent key into specified inode with single request.
func (mw *MetaWrapper) AppendExtentKeys(inode uint64, eks []proto.ExtentKey) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.appendExtentKeys(mp, inode, eks)
	if err != nil || status != statusOK {
		log.LogErrorf("AppendExtentKeys: inode(%v) extentKeys(%v) err(%v) status(%v)", inode, eks, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("AppendExtentKeys: ino(%v) extentKeys(%v)", inode, eks)
	return nil
}

func (mw *MetaWrapper) GetExtents(inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return 0, 0, nil, syscall.ENOENT
	}

	status, gen, size, extents, err := mw.getExtents(mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("GetExtents: ino(%v) err(%v) status(%v)", inode, err, status)
		return 0, 0, nil, statusToErrno(status)
	}
	log.LogDebugf("GetExtents: ino(%v) gen(%v) size(%v)", inode, gen, size)
	return gen, size, extents, nil
}

func (mw *MetaWrapper) Truncate(inode, size uint64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Truncate: No inode partition, ino(%v)", inode)
		return syscall.ENOENT
	}

	status, err := mw.truncate(mp, inode, size)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil

}

func (mw *MetaWrapper) Link(parentID uint64, name string, ino uint64) (*proto.InodeInfo, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Link: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	mp := mw.getPartitionByInode(ino)
	if mp == nil {
		log.LogErrorf("Link: No target inode partition, ino(%v)", ino)
		return nil, syscall.ENOENT
	}

	// increase inode nlink
	status, info, err := mw.ilink(mp, ino)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	// create new dentry and refer to the inode
	status, err = mw.dcreate(parentMP, parentID, name, ino, info.Mode)
	if err != nil || status != statusOK {
		if status == statusExist {
			return nil, syscall.EEXIST
		} else {
			mw.iunlink(mp, ino)
			return nil, syscall.EAGAIN
		}
	}
	return info, nil
}

func (mw *MetaWrapper) Evict(inode uint64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogWarnf("Evict: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}

	status, err := mw.ievict(mp, inode)
	if err != nil || status != statusOK {
		log.LogWarnf("Evict: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) Setattr(inode uint64, valid, mode, uid, gid uint32) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Setattr: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}

	status, err := mw.setattr(mp, inode, valid, mode, uid, gid)
	if err != nil || status != statusOK {
		log.LogErrorf("Setattr: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}

	return nil
}

func (mw *MetaWrapper) InodeCreate_ll(mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error) {
	var (
		status       int
		err          error
		info         *proto.InodeInfo
		mp           *MetaPartition
		rwPartitions []*MetaPartition
	)

	rwPartitions = mw.getRWPartitions()
	length := len(rwPartitions)
	epoch := atomic.AddUint64(&mw.epoch, 1)
	for i := 0; i < length; i++ {
		index := (int(epoch) + i) % length
		mp = rwPartitions[index]
		status, info, err = mw.icreate(mp, mode, uid, gid, target)
		if err == nil && status == statusOK {
			return info, nil
		}
	}
	return nil, syscall.ENOMEM
}

// InodeUnlink_ll is a low-level api that makes specified inode link value +1.
func (mw *MetaWrapper) InodeLink_ll(inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeLink_ll: No such partition, ino(%v)", inode)
		return nil, syscall.EINVAL
	}
	status, info, err := mw.ilink(mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("InodeLink_ll: ino(%v) err(%v) status(%v)", inode, err, status)
		return nil, statusToErrno(status)
	}
	return info, nil
}

// InodeUnlink_ll is a low-level api that makes specified inode link value -1.
func (mw *MetaWrapper) InodeUnlink_ll(inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeUnlink_ll: No such partition, ino(%v)", inode)
		return nil, syscall.EINVAL
	}
	status, info, err := mw.iunlink(mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("InodeUnlink_ll: ino(%v) err(%v) status(%v)", inode, err, status)
		return nil, statusToErrno(status)
	}
	return info, nil
}

func (mw *MetaWrapper) InitMultipart_ll(path string, parentId uint64) (multipartId string, err error) {
	mp := mw.getPartitionByInode(parentId)
	if mp == nil {
		log.LogErrorf("InitMultipart: No such partition, ino(%v)", parentId)
		return "", syscall.EINVAL
	}

	status, sessionId, err := mw.createSession(mp, path)
	if err != nil || status != statusOK {
		log.LogErrorf("InitMultipart: err(%v) status(%v)", err, status)
		return "", statusToErrno(status)
	}
	return sessionId, nil
}

func (mw *MetaWrapper) GetMultipart_ll(multipartId string, parentId uint64) (info *proto.MultipartInfo, err error) {
	mp := mw.getPartitionByInode(parentId)
	if mp == nil {
		log.LogErrorf("GetMultipartRequest: No such partition, ino(%v)", parentId)
		return nil, syscall.EINVAL
	}

	status, multipartInfo, err := mw.getMultipart(mp, multipartId)
	if err != nil || status != statusOK {
		log.LogErrorf("GetMultipartRequest: err(%v) status(%v)", err, status)
		return nil, statusToErrno(status)
	}
	return multipartInfo, nil
}

func (mw *MetaWrapper) AddMultipartPart_ll(multipartId string, parentId uint64, partId uint16, size uint64, md5 string, inode uint64) error {
	mp := mw.getPartitionByInode(parentId)
	if mp == nil {
		log.LogErrorf("AddMultipartPart: No such partition, ino(%v)", parentId)
		return syscall.EINVAL
	}
	status, err := mw.addMultipartPart(mp, multipartId, partId, size, md5, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("AddMultipartPart: err(%v) status(%v)", err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) RemoveMultipart_ll(multipartID string, parentId uint64) error {
	mp := mw.getPartitionByInode(parentId)
	if mp == nil {
		log.LogErrorf("RemoveMultipart_ll: no such partition, ino(%v)", parentId)
		return syscall.EINVAL
	}

	status, err := mw.removeMultipart(mp, multipartID)
	if err != nil || status != statusOK {
		log.LogErrorf("RemoveMultipart_ll: partition get multipart fail, partitionID(%v) multipartID(%v) err(%v) status(%v)",
			mp.PartitionID, multipartID, err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) ListMultipart_ll(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error) {
	partitions := mw.partitions
	var wg = sync.WaitGroup{}
	//var prefixes = make([]string, 0)
	var sessions = make([]*proto.MultipartInfo, 0)
	//var allSessions = make([]*proto.ListMultipartResponse, 0)

	for _, mp := range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			status, response, err := mw.listMultiparts(mp, prefix, delimiter, keyMarker, multipartIdMarker, maxUploads+1)
			if err != nil || status != statusOK {
				log.LogErrorf("ListMultipart: partition list multipart fail, partitionID(%v) err(%v) status(%v)",
					mp.PartitionID, err, status)
				err = statusToErrno(status)
				return
			}
			//allSessions = append(allSessions, response)
			sessions = append(sessions, response.Multiparts...)
		}(mp)
	}

	// combine sessions from per partition
	wg.Wait()

	// reorder sessions by path
	sort.SliceStable(sessions, func(i, j int) bool {
		return sessions[i].Path < sessions[j].Path
	})
	return sessions, nil
}

func (mw *MetaWrapper) XAttrSet_ll(inode uint64, name, value []byte) error {
	var err error
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("XAttrSet_ll: no such partition, inode(%v)", inode)
		return syscall.ENOENT
	}
	var status int
	status, err = mw.setXAttr(mp, inode, name, value)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("XAttrSet_ll: set xattr, inode(%v) name(%v) value(%v) status(%v)", inode, name, value, status)
	return nil
}

func (mw *MetaWrapper) XAttrGet_ll(inode uint64, name string) (*proto.XAttrInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeGet_ll: no such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	value, status, err := mw.getXAttr(mp, inode, name)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	xAttrValues := make(map[string]string)
	xAttrValues[name] = string(value)

	xAttr := &proto.XAttrInfo{
		Inode:  inode,
		XAttrs: xAttrValues,
	}

	log.LogDebugf("XAttrGet_ll: get xattr, inode(%v) name(%v) value(%v)", inode, string(name), string(value))
	return xAttr, nil
}

// XAttrDel_ll is a low-level meta api that deletes specified xattr.
func (mw *MetaWrapper) XAttrDel_ll(inode uint64, name string) error {
	var err error
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("XAttrDel_ll: no such partition, inode(%v)", inode)
		return syscall.ENOENT
	}
	var status int
	status, err = mw.removeXAttr(mp, inode, name)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("XAttrDel_ll: remove xattr, inode(%v) name(%v) status(%v)", inode, name, status)
	return nil
}
