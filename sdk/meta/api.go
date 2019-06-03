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
