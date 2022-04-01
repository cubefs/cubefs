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
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	syslog "log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/util/ump"
)

// Low-level API, i.e. work with inode

const (
	BatchIgetRespBuf      = 1000
	CreateSubDirDeepLimit = 5
)

const (
	OpenRetryInterval = 5 * time.Millisecond
	OpenRetryLimit    = 1000
)

func (mw *MetaWrapper) GetRootIno(subdir string, autoMakeDir bool) (uint64, error) {
	rootIno := proto.RootIno
	if subdir == "" || subdir == "/" {
		return rootIno, nil
	}

	dirs := strings.Split(subdir, "/")
	dirDeep := len(dirs)
	for idx, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, mode, err := mw.Lookup_ll(context.Background(), rootIno, dir)
		if err != nil {
			if autoMakeDir && err == syscall.ENOENT && (dirDeep-idx) < CreateSubDirDeepLimit {
				// create directory
				if rootIno, err = mw.MakeDirectory(rootIno, dir); err == nil {
					continue
				}
			}
			return 0, fmt.Errorf("GetRootIno: Lookup failed, subdir(%v) idx(%v) dir(%v) err(%v)", subdir, idx, dir, err)
		}
		if !proto.IsDir(mode) {
			return 0, fmt.Errorf("GetRootIno: not directory, subdir(%v) idx(%v) dir(%v) child(%v) mode(%v) err(%v)", subdir, idx, dir, child, mode, err)
		}
		rootIno = child
	}
	syslog.Printf("GetRootIno: inode(%v) subdir(%v)\n", rootIno, subdir)
	return rootIno, nil
}

// Looks up absolute path and returns the ino
func (mw *MetaWrapper) LookupPath(ctx context.Context, subdir string) (uint64, error) {
	ino := proto.RootIno
	if subdir == "" || subdir == "/" {
		return ino, nil
	}

	dirs := strings.Split(subdir, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err := mw.Lookup_ll(ctx, ino, dir)
		if err != nil {
			return 0, err
		}
		ino = child
	}
	return ino, nil
}

func (mw *MetaWrapper) MakeDirectory(parIno uint64, dirName string) (uint64, error) {
	inodeInfo, err := mw.Create_ll(context.Background(), parIno, dirName, proto.Mode(os.ModeDir|0755), 0, 0, nil)
	if err != nil {
		if err == syscall.EEXIST {
			existInode, existMode, e := mw.Lookup_ll(context.Background(), parIno, dirName)
			if e == nil && proto.IsDir(existMode) {
				return existInode, nil
			}
			return 0, fmt.Errorf("MakeDirectory failed: create err(%v) lookup err(%v) existInode(%v) existMode(%v)", err, e, existInode, existMode)
		}
		return 0, fmt.Errorf("MakeDirectory failed: create err(%v)", err)
	}
	if !proto.IsDir(inodeInfo.Mode) {
		return 0, fmt.Errorf("MakeDirectory failed: inode mode invalid")
	}
	return inodeInfo.Inode, nil
}

func (mw *MetaWrapper) Statfs() (total, used uint64) {
	total = atomic.LoadUint64(&mw.totalSize)
	used = atomic.LoadUint64(&mw.usedSize)
	return
}

func (mw *MetaWrapper) Create_ll(ctx context.Context, parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error) {
	var (
		status       int
		err          error
		info         *proto.InodeInfo
		mp           *MetaPartition
		rwPartitions []*MetaPartition
	)

	if mw.volNotExists {
		return nil, proto.ErrVolNotExists
	}

	parentMP := mw.getPartitionByInode(ctx, parentID)
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

	retryCount := 0
	for {
		retryCount++
		rwPartitions = mw.getRWPartitions()
		length := len(rwPartitions)
		epoch := atomic.AddUint64(&mw.epoch, 1)
		for i := 0; i < length; i++ {
			index := (int(epoch) + i) % length
			mp = rwPartitions[index]
			status, info, err = mw.icreate(ctx, mp, mode, uid, gid, target)
			if err == nil && status == statusOK {
				goto create_dentry
			}
		}
		if !mw.InfiniteRetry {
			return nil, syscall.ENOMEM
		}
		log.LogWarnf("Create_ll: create inode failed, err(%v) status(%v) parentID(%v) name(%v) retry time(%v)", err, status, parentID, name, retryCount)
		umpMsg := fmt.Sprintf("CreateInode err(%v) status(%v) parentID(%v) name(%v) retry time(%v)", err, status, parentID, name, retryCount)
		handleUmpAlarm(mw.cluster, mw.volname, "CreateInode", umpMsg)
		time.Sleep(SendRetryInterval)
	}

create_dentry:
	status, err = mw.dcreate(ctx, parentMP, parentID, name, info.Inode, mode)
	if err == nil && status == statusOK {
		return info, nil
	}

	if err == nil && status == statusExist {
		newStatus, oldInode, mode, newErr := mw.lookup(ctx, parentMP, parentID, name)
		if newErr == nil && newStatus == statusOK {
			if oldInode == info.Inode {
				return info, nil
			}
			if mw.InodeNotExist(ctx, oldInode) {
				updateStatus, _, updateErr := mw.dupdate(ctx, parentMP, parentID, name, info.Inode)
				if updateErr == nil && updateStatus == statusOK {
					log.LogWarnf("Create_ll: inode(%v) is not exist, update dentry to new inode(%v) parentID(%v) name(%v)",
						oldInode, info.Inode, parentID, name)
					return info, nil
				}
				log.LogWarnf("Create_ll: update_dentry failed, status(%v), err(%v)", updateStatus, updateErr)
			} else {
				mw.iunlink(ctx, mp, info.Inode, false)
				mw.ievict(ctx, mp, info.Inode, false)
				log.LogWarnf("Create_ll: dentry has allready been created by other client, curInode(%v), mode(%v)",
					oldInode, mode)
			}
		} else {
			log.LogWarnf("Create_ll: check create_dentry failed, status(%v), err(%v)", newStatus, newErr)
		}
	}

	// Unable to determin create dentry status, rollback may cause unexcepted result.
	// So we return status error, user should check opration result manually or retry.
	log.LogErrorf("Create_ll: create_dentry failed, err(%v), status(%v), parentMP(%v), parentID(%v), name(%v), "+
		"info.Inode(%v), mode(%v)", err, status, parentMP, parentID, name, info.Inode, mode)
	return nil, statusToErrno(status)
}

func (mw *MetaWrapper) InodeNotExist(ctx context.Context, inode uint64) bool {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return false
	}
	status, _, err := mw.iget(ctx, mp, inode)
	if err == nil && status == statusNoent {
		return true
	}
	return false
}

func (mw *MetaWrapper) Lookup_ll(ctx context.Context, parentID uint64, name string) (inode uint64, mode uint32, err error) {
	if mw.volNotExists {
		return 0, 0, proto.ErrVolNotExists
	}

	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		log.LogErrorf("Lookup_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return 0, 0, syscall.ENOENT
	}

	status, inode, mode, err := mw.lookup(ctx, parentMP, parentID, name)
	if err != nil || status != statusOK {
		return 0, 0, statusToErrno(status)
	}
	return inode, mode, nil
}

func (mw *MetaWrapper) InodeGet_ll(ctx context.Context, inode uint64) (*proto.InodeInfo, error) {
	if mw.volNotExists {
		return nil, proto.ErrVolNotExists
	}

	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("InodeGet_ll: No such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.iget(ctx, mp, inode)
	if err != nil || status != statusOK {
		if status == statusNoent {
			// For NOENT error, pull the latest mp and give it another try,
			// in case the mp view is outdated.
			mw.triggerAndWaitForceUpdate()
			return mw.doInodeGet(ctx, inode)
		}
		return nil, statusToErrno(status)
	}
	if proto.IsSymlink(info.Mode) {
		info.Size = uint64(len(info.Target))
	}
	log.LogDebugf("InodeGet_ll: info(%v)", info)
	return info, nil
}

// Just like InodeGet but without retry
func (mw *MetaWrapper) doInodeGet(ctx context.Context, inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("InodeGet_ll: No such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.iget(ctx, mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	log.LogDebugf("doInodeGet: info(%v)", info)
	return info, nil
}

func (mw *MetaWrapper) BatchInodeGet(ctx context.Context, inodes []uint64) []*proto.InodeInfo {
	var wg sync.WaitGroup

	batchInfos := make([]*proto.InodeInfo, 0)
	resp := make(chan []*proto.InodeInfo, BatchIgetRespBuf)
	candidates := make(map[uint64][]uint64)

	// Target partition does not have to be very accurate.
	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	var needForceUpdate = false
	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			needForceUpdate = true
			continue
		}
		wg.Add(1)
		go mw.batchIget(ctx, &wg, mp, inos, resp)
	}
	if needForceUpdate {
		mw.triggerForceUpdate()
	}

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
func (mw *MetaWrapper) InodeDelete_ll(ctx context.Context, inode uint64) error {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("InodeDelete: No such partition, ino(%v)", inode)
		return syscall.ENOENT
	}
	status, err := mw.idelete(ctx, mp, inode)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("InodeDelete_ll: inode(%v)", inode)
	return nil
}

func (mw *MetaWrapper) BatchGetXAttr(ctx context.Context, inodes []uint64, keys []string) ([]*proto.XAttrInfo, error) {
	// Collect meta partitions
	var (
		mps      = make(map[uint64]*MetaPartition) // Mapping: partition ID -> partition
		mpInodes = make(map[uint64][]uint64)       // Mapping: partition ID -> inodes
	)
	for _, ino := range inodes {
		var mp = mw.getPartitionByInode(ctx, ino)
		if mp != nil {
			mps[mp.PartitionID] = mp
			mpInodes[mp.PartitionID] = append(mpInodes[mp.PartitionID], ino)
		}
	}

	var (
		xattrsCh = make(chan *proto.XAttrInfo, len(inodes))
		errorsCh = make(chan error, len(inodes))
	)

	var wg sync.WaitGroup
	for pID := range mps {
		wg.Add(1)
		go func(mp *MetaPartition, inodes []uint64, keys []string) {
			defer wg.Done()
			xattrs, err := mw.batchGetXAttr(ctx, mp, inodes, keys)
			if err != nil {
				errorsCh <- err
				log.LogErrorf("BatchGetXAttr: get xattr fail: volume(%v) partitionID(%v) inodes(%v) keys(%v) err(%s)",
					mw.volname, mp.PartitionID, inodes, keys, err)
				return
			}
			for _, info := range xattrs {
				xattrsCh <- info
			}
		}(mps[pID], mpInodes[pID], keys)
	}
	wg.Wait()

	close(xattrsCh)
	close(errorsCh)

	if len(errorsCh) > 0 {
		return nil, <-errorsCh
	}

	var xattrs = make([]*proto.XAttrInfo, 0, len(inodes))
	for {
		info := <-xattrsCh
		if info == nil {
			break
		}
		xattrs = append(xattrs, info)
	}
	return xattrs, nil
}

/*
 * Note that the return value of InodeInfo might be nil without error,
 * and the caller should make sure InodeInfo is valid before using it.
 */
func (mw *MetaWrapper) Delete_ll(ctx context.Context, parentID uint64, name string, isDir bool) (*proto.InodeInfo, error) {
	var (
		status int
		inode  uint64
		mode   uint32
		err    error
		info   *proto.InodeInfo
		mp     *MetaPartition
	)

	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		log.LogErrorf("Delete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	if isDir {
		status, inode, mode, err = mw.lookup(ctx, parentMP, parentID, name)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		if !proto.IsDir(mode) {
			return nil, syscall.EINVAL
		}
		mp = mw.getPartitionByInode(ctx, inode)
		if mp == nil {
			log.LogErrorf("Delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
			return nil, syscall.EAGAIN
		}
		status, info, err = mw.iget(ctx, mp, inode)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		if info == nil || info.Nlink > 2 {
			return nil, syscall.ENOTEMPTY
		}
	}

	status, inode, err = mw.ddelete(ctx, parentMP, parentID, name, true)
	if err != nil || status != statusOK {
		if status == statusNoent {
			return nil, nil
		}
		return nil, statusToErrno(status)
	}

	// dentry is deleted successfully but inode is not, still returns success.
	mp = mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("Delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
		return nil, nil
	}

	status, info, err = mw.iunlink(ctx, mp, inode, true)
	if err != nil || status != statusOK {
		return nil, nil
	}
	return info, nil
}

func (mw *MetaWrapper) Rename_ll(ctx context.Context, srcParentID uint64, srcName string, dstParentID uint64, dstName string) (err error) {
	var oldInode uint64

	srcParentMP := mw.getPartitionByInode(ctx, srcParentID)
	if srcParentMP == nil {
		return syscall.ENOENT
	}
	dstParentMP := mw.getPartitionByInode(ctx, dstParentID)
	if dstParentMP == nil {
		return syscall.ENOENT
	}

	// look up for the src ino
	status, inode, mode, err := mw.lookup(ctx, srcParentMP, srcParentID, srcName)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	srcMP := mw.getPartitionByInode(ctx, inode)
	if srcMP == nil {
		return syscall.ENOENT
	}

	status, _, err = mw.ilink(ctx, srcMP, inode)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}

	// create dentry in dst parent
	status, err = mw.dcreate(ctx, dstParentMP, dstParentID, dstName, inode, mode)
	if err != nil {
		return syscall.EAGAIN
	}

	// Note that only regular files and symbolic links are allowed to be overwritten.
	if status == statusExist && (proto.IsRegular(mode) || proto.IsSymlink(mode)) {
		status, oldInode, err = mw.dupdate(ctx, dstParentMP, dstParentID, dstName, inode)
		if err != nil {
			return syscall.EAGAIN
		}
	}

	if status != statusOK {
		mw.iunlink(ctx, srcMP, inode, false)
		return statusToErrno(status)
	}

	// delete dentry from src parent
	status, _, err = mw.ddelete(ctx, srcParentMP, srcParentID, srcName, false)
	// Unable to determin delete dentry status, rollback may cause unexcepted result.
	// So we return error, user should check opration result manually or retry.
	if err != nil || (status != statusOK && status != statusNoent) {
		log.LogWarnf("Rename_ll: delete_dentry failed, can't determin dentry status, err(%v), status(%v), srcparentMP(%v),"+
			" srcParentID(%v), srcName(%v)", err, status, srcParentMP, srcParentID, srcName)
		return statusToErrno(status)
	}

	mw.iunlink(ctx, srcMP, inode, false)

	// As update dentry may be try, the second op will be the result which old inode be the same inode
	if oldInode != 0 && oldInode != inode {
		inodeMP := mw.getPartitionByInode(ctx, oldInode)
		if inodeMP != nil {
			mw.iunlink(ctx, inodeMP, oldInode, false)
			// evict oldInode to avoid oldInode becomes orphan inode
			mw.ievict(ctx, inodeMP, oldInode, false)
		}
	}

	return nil
}

func (mw *MetaWrapper) ReadDir_ll(ctx context.Context, parentID uint64) ([]proto.Dentry, error) {
	if mw.volNotExists {
		return nil, proto.ErrVolNotExists
	}

	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, err := mw.readdir(ctx, parentMP, parentID)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return children, nil
}

func (mw *MetaWrapper) DentryCreate_ll(ctx context.Context, parentID uint64, name string, inode uint64, mode uint32) error {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}
	var err error
	var status int
	if status, err = mw.dcreate(ctx, parentMP, parentID, name, inode, mode); err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) DentryUpdate_ll(ctx context.Context, parentID uint64, name string, inode uint64) (oldInode uint64, err error) {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		err = syscall.ENOENT
		return
	}
	var status int
	status, oldInode, err = mw.dupdate(nil, parentMP, parentID, name, inode)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
		return
	}
	return
}

// Used as a callback by stream sdk
//func (mw *MetaWrapper) AppendExtentKey(ctx context.Context, inode uint64, ek proto.ExtentKey) error {
//	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.AppendExtentKey")
//	defer tracer.Finish()
//	ctx = tracer.Context()
//
//	mp := mw.getPartitionByInode(ctx, inode)
//	if mp == nil {
//		return syscall.ENOENT
//	}
//
//	status, err := mw.appendExtentKey(ctx, mp, inode, ek)
//	if err != nil || status != statusOK {
//		log.LogErrorf("AppendExtentKey: inode(%v) ek(%v) err(%v) status(%v)", inode, ek, err, status)
//		return statusToErrno(status)
//	}
//	log.LogDebugf("AppendExtentKey: ino(%v) ek(%v)", inode, ek)
//	return nil
//}

// AppendExtentKeys append multiple extent key into specified inode with single request.
func (mw *MetaWrapper) AppendExtentKeys(ctx context.Context, inode uint64, eks []proto.ExtentKey) error {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.appendExtentKeys(ctx, mp, inode, eks)
	if err != nil || status != statusOK {
		log.LogErrorf("AppendExtentKeys: inode(%v) extentKeys(%v) err(%v) status(%v)", inode, eks, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("AppendExtentKeys: ino(%v) extentKeys(%v)", inode, eks)
	return nil
}

func (mw *MetaWrapper) InsertExtentKey(ctx context.Context, inode uint64, ek proto.ExtentKey, isPreExtent bool) error {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.insertExtentKey(ctx, mp, inode, ek, isPreExtent)
	if err != nil || status != statusOK {
		log.LogWarnf("InsertExtentKey: inode(%v) ek(%v) isPreExtent(%v) err(%v) status(%v)", inode, ek, isPreExtent, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("InsertExtentKey: ino(%v) ek(%v) isPreExtent(%v)", inode, ek, isPreExtent)
	return nil

}

func (mw *MetaWrapper) GetExtents(ctx context.Context, inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return 0, 0, nil, syscall.ENOENT
	}

	status, gen, size, extents, err := mw.getExtents(ctx, mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("GetExtents: ino(%v) err(%v) status(%v)", inode, err, status)
		return 0, 0, nil, statusToErrno(status)
	}
	log.LogDebugf("GetExtents: ino(%v) gen(%v) size(%v)", inode, gen, size)
	return gen, size, extents, nil
}

func (mw *MetaWrapper) Truncate(ctx context.Context, inode, oldSize, size uint64) error {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("Truncate: No inode partition, ino(%v)", inode)
		return syscall.ENOENT
	}

	status, err := mw.truncate(ctx, mp, inode, oldSize, size)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil

}

func (mw *MetaWrapper) Link(ctx context.Context, parentID uint64, name string, ino uint64) (*proto.InodeInfo, error) {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		log.LogErrorf("Link: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	mp := mw.getPartitionByInode(ctx, ino)
	if mp == nil {
		log.LogErrorf("Link: No target inode partition, ino(%v)", ino)
		return nil, syscall.ENOENT
	}

	// increase inode nlink
	status, info, err := mw.ilink(ctx, mp, ino)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	// create new dentry and refer to the inode
	status, err = mw.dcreate(ctx, parentMP, parentID, name, ino, info.Mode)
	if err != nil {
		return nil, statusToErrno(status)
	} else if status != statusOK {
		if status != statusExist {
			mw.iunlink(ctx, mp, ino, false)
		}
		return nil, statusToErrno(status)
	}
	return info, nil
}

func (mw *MetaWrapper) Evict(ctx context.Context, inode uint64, trashEnable bool) error {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogWarnf("Evict: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}

	status, err := mw.ievict(ctx, mp, inode, trashEnable)
	if err != nil || status != statusOK {
		log.LogWarnf("Evict: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) Setattr(ctx context.Context, inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("Setattr: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}

	status, err := mw.setattr(ctx, mp, inode, valid, mode, uid, gid, atime, mtime)
	if err != nil || status != statusOK {
		log.LogErrorf("Setattr: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}

	return nil
}

func (mw *MetaWrapper) InodeCreate_ll(ctx context.Context, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error) {
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
		status, info, err = mw.icreate(ctx, mp, mode, uid, gid, target)
		if err == nil && status == statusOK {
			return info, nil
		}
	}
	return nil, syscall.ENOMEM
}

// InodeUnlink_ll is a low-level api that makes specified inode link value +1.
func (mw *MetaWrapper) InodeLink_ll(ctx context.Context, inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("InodeLink_ll: No such partition, ino(%v)", inode)
		return nil, syscall.EINVAL
	}
	status, info, err := mw.ilink(ctx, mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("InodeLink_ll: ino(%v) err(%v) status(%v)", inode, err, status)
		return nil, statusToErrno(status)
	}
	return info, nil
}

// InodeUnlink_ll is a low-level api that makes specified inode link value -1.
func (mw *MetaWrapper) InodeUnlink_ll(ctx context.Context, inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("InodeUnlink_ll: No such partition, ino(%v)", inode)
		return nil, syscall.EINVAL
	}
	status, info, err := mw.iunlink(ctx, mp, inode, false)
	if err != nil || status != statusOK {
		log.LogErrorf("InodeUnlink_ll: ino(%v) err(%v) status(%v)", inode, err, status)
		err = statusToErrno(status)
		if err == syscall.EAGAIN {
			err = nil
		}
		return nil, err
	}
	return info, nil
}

func (mw *MetaWrapper) InitMultipart_ll(ctx context.Context, path string, extend map[string]string) (multipartId string, err error) {
	var (
		status       int
		mp           *MetaPartition
		rwPartitions = mw.getRWPartitions()
		length       = len(rwPartitions)
	)
	if length <= 0 {
		log.LogErrorf("InitMultipart: no writable partitions, path(%v)", path)
		return "", syscall.ENOENT
	}

	epoch := atomic.AddUint64(&mw.epoch, 1)
	for i := 0; i < length; i++ {
		index := (int(epoch) + i) % length
		mp = rwPartitions[index]
		log.LogDebugf("InitMultipart_ll: mp(%v), index(%v)", mp, index)
		status, sessionId, err := mw.createMultipart(ctx, mp, path, extend)
		if err == nil && status == statusOK && len(sessionId) > 0 {
			return sessionId, nil
		} else {
			log.LogErrorf("InitMultipart: create multipart id fail, path(%v), mp(%v), status(%v), err(%v)",
				path, mp, status, err)
		}
	}
	log.LogErrorf("InitMultipart: create multipart id fail, path(%v), status(%v), err(%v)", path, status, err)
	if err != nil {
		return "", err
	} else {
		return "", statusToErrno(status)
	}
}

func (mw *MetaWrapper) GetMultipart_ll(ctx context.Context, path, multipartId string) (info *proto.MultipartInfo, err error) {
	var (
		mpId  uint64
		found bool
	)
	mpId, found = util.MultipartIDFromString(multipartId).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartId, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		info, _, err = mw.broadcastGetMultipart(ctx, path, multipartId)
		return
	}
	var mp = mw.getPartitionByIDWithAutoRefresh(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, multipartInfo, err := mw.getMultipart(ctx, mp, path, multipartId)
	if err != nil || status != statusOK {
		log.LogErrorf("GetMultipartRequest: err(%v) status(%v)", err, status)
		return nil, statusToErrno(status)
	}
	return multipartInfo, nil
}

func (mw *MetaWrapper) AddMultipartPart_ll(ctx context.Context, path, multipartId string, partId uint16, size uint64, md5 string, inode uint64) (err error) {

	var (
		mpId  uint64
		found bool
	)
	mpId, found = util.MultipartIDFromString(multipartId).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartId, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		if _, mpId, err = mw.broadcastGetMultipart(ctx, path, multipartId); err != nil {
			return
		}
	}
	var mp = mw.getPartitionByIDWithAutoRefresh(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, err := mw.addMultipartPart(ctx, mp, path, multipartId, partId, size, md5, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("AddMultipartPart_ll: err(%v) status(%v)", err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) RemoveMultipart_ll(ctx context.Context, path, multipartID string) (err error) {
	var (
		mpId  uint64
		found bool
	)
	mpId, found = util.MultipartIDFromString(multipartID).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartID, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		if _, mpId, err = mw.broadcastGetMultipart(ctx, path, multipartID); err != nil {
			return
		}
	}
	var mp = mw.getPartitionByIDWithAutoRefresh(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, err := mw.removeMultipart(ctx, mp, path, multipartID)
	if err != nil || status != statusOK {
		log.LogErrorf(" RemoveMultipart_ll: partition remove multipart fail: "+
			"volume(%v) partitionID(%v) multipartID(%v) err(%v) status(%v)",
			mw.volname, mp.PartitionID, multipartID, err, status)
		return statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) broadcastGetMultipart(ctx context.Context, path, multipartId string) (info *proto.MultipartInfo, mpID uint64, err error) {
	log.LogInfof("broadcastGetMultipart: find meta partition broadcast multipartId(%v)", multipartId)
	partitions := mw.partitions
	var (
		mp *MetaPartition
	)
	var wg = new(sync.WaitGroup)
	var resultMu sync.Mutex
	for _, mp = range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			status, multipartInfo, err := mw.getMultipart(ctx, mp, path, multipartId)
			if err == nil && status == statusOK && multipartInfo != nil && multipartInfo.ID == multipartId {
				resultMu.Lock()
				mpID = mp.PartitionID
				info = multipartInfo
				resultMu.Unlock()
			}
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("broadcastGetMultipart: get multipart fail: partitionId(%v) multipartId(%v)",
					mp.PartitionID, multipartId)
			}
		}(mp)
	}
	wg.Wait()

	resultMu.Lock()
	defer resultMu.Unlock()
	if info == nil {
		err = syscall.ENOENT
		return
	}
	return
}

func (mw *MetaWrapper) ListMultipart_ll(ctx context.Context, prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error) {
	partitions := mw.partitions
	var wg = sync.WaitGroup{}
	var wl = sync.Mutex{}
	var sessions = make([]*proto.MultipartInfo, 0)

	for _, mp := range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			status, response, err := mw.listMultiparts(ctx, mp, prefix, delimiter, keyMarker, multipartIdMarker, maxUploads+1)
			if err != nil || status != statusOK {
				log.LogErrorf("ListMultipart: partition list multipart fail, partitionID(%v) err(%v) status(%v)",
					mp.PartitionID, err, status)
				err = statusToErrno(status)
				return
			}
			wl.Lock()
			defer wl.Unlock()
			sessions = append(sessions, response.Multiparts...)
		}(mp)
	}

	// combine sessions from per partition
	wg.Wait()

	// reorder sessions by path
	sort.SliceStable(sessions, func(i, j int) bool {
		return (sessions[i].Path < sessions[j].Path) || ((sessions[i].Path == sessions[j].Path) && (sessions[i].ID < sessions[j].ID))
	})
	return sessions, nil
}

func (mw *MetaWrapper) XAttrSet_ll(ctx context.Context, inode uint64, name, value []byte) error {
	var err error
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("XAttrSet_ll: no such partition, inode(%v)", inode)
		return syscall.ENOENT
	}
	var status int
	status, err = mw.setXAttr(ctx, mp, inode, name, value)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("XAttrSet_ll: set xattr: volume(%v) inode(%v) name(%v) value(%v) status(%v)",
		mw.volname, inode, name, value, status)
	return nil
}

func (mw *MetaWrapper) XAttrGet_ll(ctx context.Context, inode uint64, name string) (*proto.XAttrInfo, error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("InodeGet_ll: no such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	value, status, err := mw.getXAttr(ctx, mp, inode, name)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	xAttrValues := make(map[string]string)
	xAttrValues[name] = string(value)

	xAttr := &proto.XAttrInfo{
		Inode:  inode,
		XAttrs: xAttrValues,
	}

	log.LogDebugf("XAttrGet_ll: get xattr: volume(%v) inode(%v) name(%v) value(%v)",
		mw.volname, inode, string(name), string(value))
	return xAttr, nil
}

// XAttrDel_ll is a low-level meta api that deletes specified xattr.
func (mw *MetaWrapper) XAttrDel_ll(ctx context.Context, inode uint64, name string) error {
	var err error
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("XAttrDel_ll: no such partition, inode(%v)", inode)
		return syscall.ENOENT
	}
	var status int
	status, err = mw.removeXAttr(ctx, mp, inode, name)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("XAttrDel_ll: remove xattr, inode(%v) name(%v) status(%v)", inode, name, status)
	return nil
}

func (mw *MetaWrapper) XAttrsList_ll(ctx context.Context, inode uint64) ([]string, error) {
	var err error
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("XAttrsList_ll: no such partition, inode(%v)", inode)
		return nil, syscall.ENOENT
	}
	keys, status, err := mw.listXAttr(ctx, mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	return keys, nil
}

func (mw *MetaWrapper) GetExtentsWithMp(ctx context.Context, mpId uint64, inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {

	mp := mw.getPartitionByID(mpId)
	if mp == nil {
		log.LogErrorf("GetCmpInode_ll: no such partition(%v)", mpId)
		return 0, 0, nil, fmt.Errorf("no such partition(%v)", mpId)
	}

	status, gen, size, extents, err := mw.getExtents(ctx, mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("GetExtents: ino(%v) err(%v) status(%v)", inode, err, status)
		return 0, 0, nil, statusToErrno(status)
	}
	log.LogDebugf("GetExtents: ino(%v) gen(%v) size(%v)", inode, gen, size)
	return gen, size, extents, nil
}

func (mw *MetaWrapper) GetCmpInode_ll(ctx context.Context, mpId uint64, inos []uint64, cnt int, minEkSize int, minInodeSize uint64, maxEkAvgSize uint64) ([]*proto.CmpInodeInfo,  error) {
	mp := mw.getPartitionByID(mpId)
	if mp == nil {
		log.LogErrorf("GetCmpInode_ll: no such partition(%v)", mpId)
		return nil, fmt.Errorf("no such partition(%v)", mpId)
	}
	inodes, err := mw.getCmpInodes(ctx, mp, inos, cnt, minEkSize, minInodeSize, maxEkAvgSize)
	if err != nil {
		return nil, err
	}

	return inodes, nil
}

func (mw *MetaWrapper) InodeMergeExtents_ll(ctx context.Context, ino uint64, oldEks []proto.ExtentKey, newEks []proto.ExtentKey) error {
	mp := mw.getPartitionByInode(ctx, ino)
	if mp == nil {
		log.LogErrorf("GetCmpInode_ll: no such ino(%v)", ino)
		return fmt.Errorf("no such ino(%v)", ino)
	}
	err := mw.mergeInodeExtents(ctx, mp, ino, oldEks, newEks)
	if err != nil {
		return err
	}

	return nil
}

func (mw *MetaWrapper) getTargetHosts(ctx context.Context, mp *MetaPartition, members []string, judgeErrNum int) (targetHosts []string, isErr bool) {
	log.LogDebugf("getTargetHosts because of no leader: mp[%v] members[%v] judgeErrNum[%v]", mp, members, judgeErrNum)
	appliedIDslice := make(map[string]uint64, len(members))
	errSlice := make(map[string]bool)
	isErr = false
	var (
		wg           sync.WaitGroup
		lock         sync.Mutex
		maxAppliedID uint64
	)
	for _, addr := range members {
		wg.Add(1)
		go func(curAddr string) {
			appliedID, err := mw.getAppliedID(ctx, mp, curAddr)
			ok := false
			lock.Lock()
			if err != nil {
				errSlice[curAddr] = true
			} else {
				appliedIDslice[curAddr] = appliedID
				ok = true
			}
			lock.Unlock()
			log.LogDebugf("getTargetHosts: get apply id[%v] ok[%v] from host[%v], pid[%v]", appliedID, ok, curAddr, mp.PartitionID)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	if len(errSlice) >= judgeErrNum {
		isErr = true
		log.LogWarnf("getTargetHosts err: mp[%v], hosts[%v], appliedID[%v], judgeErrNum[%v]",
			mp.PartitionID, members, appliedIDslice, judgeErrNum)
		return
	}
	targetHosts, maxAppliedID = getMaxApplyIDHosts(appliedIDslice)
	log.LogDebugf("getTargetHosts: get max apply id[%v] from hosts[%v], pid[%v]", maxAppliedID, targetHosts, mp.PartitionID)
	return targetHosts, isErr
}

func excludeLearner(mp *MetaPartition) (members []string) {
	members = make([]string, 0)
	for _, host := range mp.Members {
		if !contains(mp.Learners, host) {
			members = append(members, host)
		}
	}
	return members
}

func getMaxApplyIDHosts(appliedIDslice map[string]uint64) (targetHosts []string, maxID uint64) {
	maxID = uint64(0)
	targetHosts = make([]string, 0)
	for _, id := range appliedIDslice {
		if id >= maxID {
			maxID = id
		}
	}
	for addr, id := range appliedIDslice {
		if id == maxID {
			targetHosts = append(targetHosts, addr)
		}
	}
	return
}

func handleUmpAlarm(cluster, vol, act, msg string) {
	umpKeyCluster := fmt.Sprintf("%s_client_warning", cluster)
	umpMsgCluster := fmt.Sprintf("volume(%s) %s", vol, msg)
	ump.Alarm(umpKeyCluster, umpMsgCluster)

	umpKeyVol := fmt.Sprintf("%s_%s_warning", cluster, vol)
	umpMsgVol := fmt.Sprintf("act(%s) - %s", act, msg)
	ump.Alarm(umpKeyVol, umpMsgVol)
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}
func (mw *MetaWrapper) LookupDeleted_ll(ctx context.Context, parentID uint64, name string, startTime, endTime int64) (
	dentrys []*proto.DeletedDentry, err error) {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		log.LogErrorf("LookupDeleted_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	var status int
	status, err, dentrys = mw.lookupDeleted(ctx, parentMP, parentID, name, startTime, endTime)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) ReadDirDeleted(ctx context.Context, parentID uint64) ([]*proto.DeletedDentry, error) {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	var (
		name      string
		timestamp int64
	)

	res := make([]*proto.DeletedDentry, 0)
	for {
		status, children, err := mw.readDeletedDir(ctx, parentMP, parentID, name, timestamp)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		for _, child := range children {
			res = append(res, child)
		}
		if len(children) < proto.ReadDeletedDirBatchNum {
			break
		}
		name = children[proto.ReadDeletedDirBatchNum-1].Name
		timestamp = children[proto.ReadDeletedDirBatchNum-1].Timestamp
	}

	return res, nil
}

func (mw *MetaWrapper) RecoverDeletedDentry(ctx context.Context, parentID, inodeID uint64, name string, timestamp int64) error {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}

	status, err := mw.recoverDentry(ctx, parentMP, parentID, inodeID, name, timestamp)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) BatchRecoverDeletedDentry(ctx context.Context, dens []*proto.DeletedDentry) (
	res map[uint64]int, err error) {
	if dens == nil || len(dens) == 0 {
		err = errors.New("BatchRecoverDeletedDentry, the dens is 0.")
		return
	}

	log.LogDebugf("BatchRecoverDeletedDentry: len(dens): %v", len(dens))
	for index, den := range dens {
		log.LogDebugf("index: %v, den: %v", index, den)
	}
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedDentryRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]*proto.DeletedDentry)

	// Target partition does not have to be very accurate.
	for _, den := range dens {
		mp := mw.getPartitionByInode(ctx, den.ParentID)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]*proto.DeletedDentry, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], den)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchRecoverDeletedDentry(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, den := range infos.Dens {
			res[den.Den.Inode] = parseStatus(den.Status)
		}
	}
	return

}

func (mw *MetaWrapper) RecoverDeletedInode(ctx context.Context, inodeID uint64) error {
	mp := mw.getPartitionByInode(ctx, inodeID)
	if mp == nil {
		log.LogErrorf("RecoverDeleteInode: No such partition, ino(%v)", inodeID)
		return syscall.ENOENT
	}

	status, err := mw.recoverDeletedInode(ctx, mp, inodeID)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) BatchRecoverDeletedInode(ctx context.Context, inodes []uint64) (res map[uint64]int, err error) {
	log.LogDebugf("BatchRecoverDeletedInode: len(dens): %v", len(inodes))

	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedINodeRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchRecoverDeletedInode(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, ino := range infos.Inos {
			res[ino.Inode.Inode] = parseStatus(ino.Status)
		}
	}

	return
}

func (mw *MetaWrapper) BatchCleanDeletedDentry(ctx context.Context, dens []*proto.DeletedDentry) (res map[uint64]int, err error) {
	if dens == nil || len(dens) == 0 {
		err = errors.New("BatchCleanDeletedDentry, the dens is 0.")
		return
	}

	log.LogDebugf("BatchCleanDeletedDentry: len(dens): %v", len(dens))
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedDentryRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]*proto.DeletedDentry)

	for _, den := range dens {
		mp := mw.getPartitionByInode(ctx, den.ParentID)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]*proto.DeletedDentry, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], den)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchCleanDeletedDentry(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, d := range infos.Dens {
			res[d.Den.Inode] = parseStatus(d.Status)
		}
	}
	return
}

func (mw *MetaWrapper) CleanDeletedDentry(ctx context.Context, parentID, inodeID uint64, name string, timestamp int64) error {
	parentMP := mw.getPartitionByInode(ctx, parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}

	status, err := mw.cleanDeletedDentry(ctx, parentMP, parentID, inodeID, name, timestamp)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) BatchCleanDeletedInode(ctx context.Context, inodes []uint64) (res map[uint64]int, err error) {
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchOpDeletedINodeRsp, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchCleanDeletedInode(ctx, &wg, mp, inos, batchChan)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, ino := range infos.Inos {
			res[ino.Inode.Inode] = parseStatus(ino.Status)
		}
	}

	return
}

func (mw *MetaWrapper) CleanDeletedInode(ctx context.Context, inodeID uint64) error {
	mp := mw.getPartitionByInode(ctx, inodeID)
	if mp == nil {
		log.LogErrorf("RecoverDeleteInode: No such partition, ino(%v)", inodeID)
		return syscall.ENOENT
	}

	status, err := mw.cleanDeletedInode(ctx, mp, inodeID)
	if err != nil {
		return err
	}
	if status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) GetDeletedInode(ctx context.Context, inode uint64) (*proto.DeletedInodeInfo, error) {
	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("GetDeletedInode: No such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.getDeletedInodeInfo(ctx, mp, inode)
	if err != nil {
		return nil, err
	}
	if status != statusOK {
		err = statusToErrno(status)
		return nil, err
	}
	log.LogDebugf("GetDeletedInode: info(%v)", info)
	return info, nil
}

func (mw *MetaWrapper) BatchGetDeletedInode(ctx context.Context, inodes []uint64) map[uint64]*proto.DeletedInodeInfo {
	var wg sync.WaitGroup
	batchInfos := make(map[uint64]*proto.DeletedInodeInfo, 0)
	resp := make(chan []*proto.DeletedInodeInfo, BatchIgetRespBuf)
	candidates := make(map[uint64][]uint64)

	// Target partition does not have to be very accurate.
	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchGetDeletedInodeInfo(ctx, &wg, mp, inos, resp)
	}

	go func() {
		wg.Wait()
		close(resp)
	}()

	for infos := range resp {
		for _, di := range infos {
			batchInfos[di.Inode] = di
		}
	}
	return batchInfos
}

func (mw *MetaWrapper) StatDeletedFileInfo(ctx context.Context, pid uint64) (resp *proto.StatDeletedFileInfoResponse, err error) {
	mp := mw.getPartitionByID(pid)
	if mp == nil {
		log.LogErrorf("StatDeletedFileInfo: No such partition, pid(%v)", pid)
		return nil, syscall.ENOENT
	}

	status := statusOK
	resp, status, err = mw.statDeletedFileInfo(ctx, mp)
	if err != nil {
		return
	}
	if status != statusOK {
		err = statusToErrno(status)
		return
	}
	return
}

func (mw *MetaWrapper) CleanExpiredDeletedDentry(ctx context.Context, pid uint64, deadline uint64) (err error) {
	mp := mw.getPartitionByID(pid)
	if mp == nil {
		log.LogErrorf("CleanExpiredDeletedDentry: No such partition, pid(%v)", pid)
		return syscall.ENOENT
	}

	st := statusOK
	st, err = mw.cleanExpiredDeletedDentry(ctx, mp, deadline)
	if err != nil {
		return
	}
	if st != statusOK {
		err = statusToErrno(st)
		return
	}
	return
}

func (mw *MetaWrapper) CleanExpiredDeletedInode(ctx context.Context, pid uint64, deadline uint64) (err error) {
	mp := mw.getPartitionByID(pid)
	if mp == nil {
		log.LogErrorf("CleanExpiredDeletedInode: No such partition, pid(%v)", pid)
		return syscall.ENOENT
	}

	st := statusOK
	st, err = mw.cleanExpiredDeletedInode(ctx, mp, deadline)
	if err != nil {
		log.LogErrorf("CleanExpiredDeletedInode, err: %v", err.Error())
		return
	}
	if st != statusOK {
		err = statusToErrno(st)
		return
	}
	return
}

func (mw *MetaWrapper) BatchUnlinkInodeUntest(ctx context.Context, inodes []uint64, trashEnable bool) (res map[uint64]int, err error) {
	log.LogDebugf("BatchUnlinkInodeUntest: len(dens): %v", len(inodes))
	for index, ino := range inodes {
		log.LogDebugf("index: %v, den: %v", index, ino)
	}

	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchUnlinkInodeResponse, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchUnlinkInodeUntest(ctx, &wg, mp, inos, batchChan, trashEnable)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, it := range infos.Items {
			res[it.Info.Inode] = parseStatus(it.Status)
		}
	}

	for id := range res {
		val, _ := res[id]
		log.LogDebugf("==> BatchUnlinkInodeUntest: ino: %v, status: %v", id, val)
	}
	return
}

func (mw *MetaWrapper) BatchEvictInodeUntest(ctx context.Context, inodes []uint64, trashEnable bool) (res map[uint64]int, err error) {
	log.LogDebugf("BatchEvictInodeUntest: len(dens): %v", len(inodes))
	for index, ino := range inodes {
		log.LogDebugf("index: %v, den: %v", index, ino)
	}

	var wg sync.WaitGroup
	batchChan := make(chan int, len(inodes))
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]uint64)

	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ctx, ino)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]uint64, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], ino)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchEvictInodeUntest(ctx, &wg, mp, inos, batchChan, trashEnable)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for st := range batchChan {
		if st != statusOK {
			err = fmt.Errorf(" status: %v", st)
			break
		}
	}

	return
}

func (mw *MetaWrapper) BatchDeleteDentryUntest(ctx context.Context, pid uint64, dens []proto.Dentry, trashEnable bool) (
	res map[uint64]int, err error) {
	if dens == nil || len(dens) == 0 {
		err = errors.New("BatchDeleteDentryUntest, the dens is 0.")
		return
	}

	log.LogDebugf("BatchDeleteDentryUntest: len(dens): %v", len(dens))
	for index, den := range dens {
		log.LogDebugf("index: %v, den: %v", index, den)
	}
	var wg sync.WaitGroup
	batchChan := make(chan *proto.BatchDeleteDentryResponse, 1000)
	res = make(map[uint64]int, 0)
	candidates := make(map[uint64][]proto.Dentry)

	// Target partition does not have to be very accurate.
	for _, den := range dens {
		mp := mw.getPartitionByInode(ctx, pid)
		if mp == nil {
			continue
		}
		if _, ok := candidates[mp.PartitionID]; !ok {
			candidates[mp.PartitionID] = make([]proto.Dentry, 0, 256)
		}
		candidates[mp.PartitionID] = append(candidates[mp.PartitionID], den)
	}

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchDeleteDentryUntest(ctx, &wg, mp, pid, inos, batchChan, trashEnable)
	}

	go func() {
		wg.Wait()
		close(batchChan)
	}()

	for infos := range batchChan {
		for _, it := range infos.Items {
			res[it.Inode] = parseStatus(it.Status)
		}
	}
	return
}
