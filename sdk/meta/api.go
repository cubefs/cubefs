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
	"github.com/chubaofs/chubaofs/util/tracing"
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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.LookupPath").
		SetTag("volume", mw.volname).
		SetTag("subdir", subdir)
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Create_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
		newStatus, newInode, mode, newErr := mw.lookup(ctx, parentMP, parentID, name)
		if newErr == nil && newStatus == statusOK {
			if newInode == info.Inode {
				return info, nil
			} else {
				mw.iunlink(ctx, mp, info.Inode)
				mw.ievict(ctx, mp, info.Inode)
				log.LogWarnf("Create_ll: dentry has allready been created by other client, newInode(%v), mode(%v)",
					newInode, mode)
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

func (mw *MetaWrapper) Lookup_ll(ctx context.Context, parentID uint64, name string) (inode uint64, mode uint32, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Lookup_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.InodeGet_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.doInodeGet")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.BatchInodeGet")
	defer tracer.Finish()
	ctx = tracer.Context()

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

	for id, inos := range candidates {
		mp := mw.getPartitionByID(id)
		if mp == nil {
			continue
		}
		wg.Add(1)
		go mw.batchIget(ctx, &wg, mp, inos, resp)
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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.InodeDelete_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.BatchGetXAttr")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Delete_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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

	status, inode, err = mw.ddelete(ctx, parentMP, parentID, name)
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

	status, info, err = mw.iunlink(ctx, mp, inode)
	if err != nil || status != statusOK {
		return nil, nil
	}
	return info, nil
}

func (mw *MetaWrapper) Rename_ll(ctx context.Context, srcParentID uint64, srcName string, dstParentID uint64, dstName string) (err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Rename_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
		mw.iunlink(ctx, srcMP, inode)
		return statusToErrno(status)
	}

	// delete dentry from src parent
	status, _, err = mw.ddelete(ctx, srcParentMP, srcParentID, srcName)
	// Unable to determin delete dentry status, rollback may cause unexcepted result.
	// So we return error, user should check opration result manually or retry.
	if err != nil || (status != statusOK && status != statusNoent) {
		log.LogWarnf("Rename_ll: delete_dentry failed, can't determin dentry status, err(%v), status(%v), srcparentMP(%v),"+
			" srcParentID(%v), srcName(%v)", err, status, srcParentMP, srcParentID, srcName)
		return statusToErrno(status)
	}

	mw.iunlink(ctx, srcMP, inode)

	// As update dentry may be try, the second op will be the result which old inode be the same inode
	if oldInode != 0 && oldInode != inode {
		inodeMP := mw.getPartitionByInode(ctx, oldInode)
		if inodeMP != nil {
			mw.iunlink(ctx, inodeMP, oldInode)
			// evict oldInode to avoid oldInode becomes orphan inode
			mw.ievict(ctx, inodeMP, oldInode)
		}
	}

	return nil
}

func (mw *MetaWrapper) ReadDir_ll(ctx context.Context, parentID uint64) ([]proto.Dentry, error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.ReadDir_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.DentryCreate_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.DentryUpdate_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.AppendExtentKeys")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.InsertExtentKey").
		SetTag("inode", inode).
		SetTag("ek.PartitionId", ek.PartitionId).
		SetTag("ek.FileOffset", ek.FileOffset).
		SetTag("ek.ExtentId", ek.ExtentId).
		SetTag("ek.ExtentOffset", ek.ExtentOffset).
		SetTag("ek.Size", ek.Size)
	defer tracer.Finish()
	ctx = tracer.Context()

	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.insertExtentKey(ctx, mp, inode, ek, isPreExtent)
	if err != nil || status != statusOK {
		log.LogWarnf("InsertExtentKey: inode(%v) ek(%v) err(%v) status(%v)", inode, ek, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("InsertExtentKey: ino(%v) ek(%v)", inode, ek)
	return nil

}

func (mw *MetaWrapper) GetExtents(ctx context.Context, inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.GetExtents")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Truncate")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Link")
	defer tracer.Finish()
	ctx = tracer.Context()

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
			mw.iunlink(ctx, mp, ino)
		}
		return nil, statusToErrno(status)
	}
	return info, nil
}

func (mw *MetaWrapper) Evict(ctx context.Context, inode uint64) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Evict")
	defer tracer.Finish()
	ctx = tracer.Context()

	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogWarnf("Evict: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}

	status, err := mw.ievict(ctx, mp, inode)
	if err != nil || status != statusOK {
		log.LogWarnf("Evict: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) Setattr(ctx context.Context, inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.Setattr")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.InodeCreate_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.InodeLink_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.InodeUnlink_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

	mp := mw.getPartitionByInode(ctx, inode)
	if mp == nil {
		log.LogErrorf("InodeUnlink_ll: No such partition, ino(%v)", inode)
		return nil, syscall.EINVAL
	}
	status, info, err := mw.iunlink(ctx, mp, inode)
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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.InitMultipart_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.GetMultipart_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var mp = mw.getPartitionByID(mpId)
	status, multipartInfo, err := mw.getMultipart(ctx, mp, path, multipartId)
	if err != nil || status != statusOK {
		log.LogErrorf("GetMultipartRequest: err(%v) status(%v)", err, status)
		return nil, statusToErrno(status)
	}
	return multipartInfo, nil
}

func (mw *MetaWrapper) AddMultipartPart_ll(ctx context.Context, path, multipartId string, partId uint16, size uint64, md5 string, inode uint64) (err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.AddMultipartPart_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var mp = mw.getPartitionByID(mpId)
	status, err := mw.addMultipartPart(ctx, mp, path, multipartId, partId, size, md5, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("AddMultipartPart_ll: err(%v) status(%v)", err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) RemoveMultipart_ll(ctx context.Context, path, multipartID string) (err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.RemoveMultipart_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var mp = mw.getPartitionByID(mpId)
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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.broadcastGetMultipart")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.ListMultipart_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.XAttrSet_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.XAttrGet_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.XAttrDel_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.XAttrsList_ll")
	defer tracer.Finish()
	ctx = tracer.Context()

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

func (mw *MetaWrapper) GetMaxAppliedIDHosts(ctx context.Context, mp *MetaPartition) (targetHosts []string, isErr bool) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("MetaWrapper.GetMaxAppliedIDHosts")
	defer tracer.Finish()
	ctx = tracer.Context()

	log.LogDebugf("getMaxAppliedIDHosts because of no leader: pid[%v], hosts[%v]", mp.PartitionID, mp.Members)
	appliedIDslice := make(map[string]uint64, len(mp.Members))
	errSlice := make(map[string]bool)
	isErr = false
	var (
		wg           sync.WaitGroup
		lock         sync.Mutex
		maxAppliedID uint64
	)
	for _, addr := range mp.Members {
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
			log.LogDebugf("getMaxAppliedIDHosts: get apply id[%v] ok[%v] from host[%v], pid[%v]", appliedID, ok, curAddr, mp.PartitionID)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	if len(errSlice) >= (len(mp.Members)+1)/2 {
		isErr = true
		log.LogWarnf("getMaxAppliedIDHosts err: mp[%v], hosts[%v], appliedID[%v]", mp.PartitionID, mp.Members, appliedIDslice)
		return
	}
	targetHosts, maxAppliedID = getMaxApplyIDHosts(appliedIDslice)
	log.LogDebugf("getMaxAppliedIDHosts: get max apply id[%v] from hosts[%v], pid[%v]", maxAppliedID, targetHosts, mp.PartitionID)
	return
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
