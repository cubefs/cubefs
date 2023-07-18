// Copyright 2018 The CubeFS Authors.
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
	"fmt"
	syslog "log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// Low-level API, i.e. work with inode

const (
	OpenRetryInterval = 5 * time.Millisecond
	OpenRetryLimit    = 1000
	maxUniqID         = 5000
)

const (
	BatchIgetRespBuf       = 1000
	MaxSummaryGoroutineNum = 120
	BatchGetBufLen         = 500
	UpdateSummaryRetry     = 3
	SummaryKey             = "DirStat"
	ChannelLen             = 100
	BatchSize              = 200
	MaxGoroutineNum        = 5
)

func mapHaveSameKeys(m1, m2 map[uint32]*proto.MetaQuotaInfo) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k := range m1 {
		if _, ok := m2[k]; !ok {
			return false
		}
	}

	return true
}

func (mw *MetaWrapper) GetRootIno(subdir string) (uint64, error) {
	rootIno, err := mw.LookupPath(subdir)
	if err != nil {
		return 0, fmt.Errorf("GetRootIno: Lookup failed, subdir(%v) err(%v)", subdir, err)
	}
	info, err := mw.InodeGet_ll(rootIno)
	if err != nil {
		return 0, fmt.Errorf("GetRootIno: InodeGet failed, subdir(%v) err(%v)", subdir, err)
	}
	if !proto.IsDir(info.Mode) {
		return 0, fmt.Errorf("GetRootIno: not directory, subdir(%v) mode(%v) err(%v)", subdir, info.Mode, err)
	}
	syslog.Printf("GetRootIno: %v\n", rootIno)
	return rootIno, nil
}

// Looks up absolute path and returns the ino
func (mw *MetaWrapper) LookupPath(subdir string) (uint64, error) {
	ino := proto.RootIno
	if subdir == "" || subdir == "/" {
		return ino, nil
	}

	dirs := strings.Split(subdir, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err := mw.Lookup_ll(ino, dir)
		if err != nil {
			return 0, err
		}
		ino = child
	}
	return ino, nil
}

func (mw *MetaWrapper) Statfs() (total, used, inodeCount uint64) {
	total = atomic.LoadUint64(&mw.totalSize)
	used = atomic.LoadUint64(&mw.usedSize)
	inodeCount = atomic.LoadUint64(&mw.inodeCount)
	return
}

func (mw *MetaWrapper) Create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error) {
	//if mw.EnableTransaction {
	txMask := proto.TxOpMaskOff
	if proto.IsRegular(mode) {
		txMask = proto.TxOpMaskCreate
	} else if proto.IsDir(mode) {
		txMask = proto.TxOpMaskMkdir
	} else if proto.IsSymlink(mode) {
		txMask = proto.TxOpMaskSymlink
	} else {
		txMask = proto.TxOpMaskMknod
	}
	txType := proto.TxMaskToType(txMask)
	if mw.enableTx(txMask) && txType != proto.TxTypeUndefined {
		return mw.txCreate_ll(parentID, name, mode, uid, gid, target, txType)
	} else {
		return mw.create_ll(parentID, name, mode, uid, gid, target)
	}
}

func (mw *MetaWrapper) txCreate_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte, txType uint32) (info *proto.InodeInfo, err error) {
	var (
		status       int
		mp           *MetaPartition
		rwPartitions []*MetaPartition
	)

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("txCreate_ll: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	quotaInfos, err := mw.getInodeQuota(parentMP, parentID)
	if err != nil {
		log.LogErrorf("Create_ll: get parent quota fail, parentID(%v) err(%v)", parentID, err)
		return nil, syscall.ENOENT
	}
	var quotaIds []uint32
	for quotaId := range quotaInfos {
		quotaIds = append(quotaIds, quotaId)
	}
	rwPartitions = mw.getRWPartitions()
	length := len(rwPartitions)
	var tx *Transaction

	defer func() {
		if tx != nil {
			err = tx.OnDone(err, mw)
		}
	}()

	epoch := atomic.AddUint64(&mw.epoch, 1)
	for i := 0; i < length; i++ {
		index := (int(epoch) + i) % length
		mp = rwPartitions[index]
		tx, err = NewCreateTransaction(parentMP, mp, parentID, name, mw.TxTimeout, txType)
		if err != nil {
			return nil, syscall.EAGAIN
		}

		status, info, err = mw.txIcreate(tx, mp, mode, uid, gid, target, quotaIds)
		if err == nil && status == statusOK {
			goto create_dentry
		} else if status == statusNoSpace {
			log.LogErrorf("Create_ll status %v", status)
			return nil, statusToErrno(status)
		} else {
			//sync cancel previous transaction before retry
			tx.Rollback(mw)
		}
	}
	return nil, syscall.ENOMEM

create_dentry:
	if log.EnableDebug() {
		log.LogDebugf("txCreate_ll: tx.txInfo(%v)", tx.txInfo)
	}

	status, err = mw.txDcreate(tx, parentMP, parentID, name, info.Inode, mode, quotaIds)
	if err != nil || status != statusOK {
		return nil, statusErrToErrno(status, err)
	}

	if log.EnableDebug() {
		log.LogDebugf("txCreate_ll: tx.txInfo(%v)", tx.txInfo)
	}

	if mw.EnableSummary {
		var filesInc, dirsInc int64
		if proto.IsDir(mode) {
			dirsInc = 1
		} else {
			filesInc = 1
		}
		//go mw.UpdateSummary_ll(parentID, filesInc, dirsInc, 0)
		job := func() {
			mw.UpdateSummary_ll(parentID, filesInc, dirsInc, 0)
		}
		tx.SetOnCommit(job)
	}

	return info, nil
}

func (mw *MetaWrapper) create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error) {
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

	status, info, err = mw.iget(parentMP, parentID)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	quota := atomic.LoadUint32(&mw.DirChildrenNumLimit)
	if info.Nlink >= quota {
		log.LogErrorf("Create_ll: parent inode's nlink quota reached, parentID(%v)", parentID)
		return nil, syscall.EDQUOT
	}
	rwPartitions = mw.getRWPartitions()
	length := len(rwPartitions)
	epoch := atomic.AddUint64(&mw.epoch, 1)
	var quotaIds []uint32
	if mw.EnableQuota {
		quotaInfos, err := mw.getInodeQuota(parentMP, parentID)
		if err != nil {
			log.LogErrorf("Create_ll: get parent quota fail, parentID(%v) err(%v)", parentID, err)
			return nil, syscall.ENOENT
		}
		for quotaId := range quotaInfos {
			quotaIds = append(quotaIds, quotaId)
		}

		for i := 0; i < length; i++ {
			index := (int(epoch) + i) % length
			mp = rwPartitions[index]
			status, info, err = mw.quotaIcreate(mp, mode, uid, gid, target, quotaIds)
			if err == nil && status == statusOK {
				goto create_dentry
			} else if status == statusNoSpace {
				log.LogErrorf("Create_ll status %v", status)
				return nil, statusToErrno(status)
			}
		}
	} else {
		for i := 0; i < length; i++ {
			index := (int(epoch) + i) % length
			mp = rwPartitions[index]
			status, info, err = mw.icreate(mp, mode, uid, gid, target)
			if err == nil && status == statusOK {
				goto create_dentry
			} else if status == statusNoSpace {
				log.LogErrorf("Create_ll status %v", status)
				return nil, statusToErrno(status)
			}
		}

	}
	return nil, syscall.ENOMEM
create_dentry:
	if mw.EnableQuota {
		status, err = mw.quotaDcreate(parentMP, parentID, name, info.Inode, mode, quotaIds)
	} else {
		status, err = mw.dcreate(parentMP, parentID, name, info.Inode, mode)
	}
	if err != nil {
		if status == statusOpDirQuota || status == statusNoSpace {
			mw.iunlink(mp, info.Inode)
			mw.ievict(mp, info.Inode)
		}
		return nil, statusToErrno(status)
	} else if status != statusOK {
		if status != statusExist {
			mw.iunlink(mp, info.Inode)
			mw.ievict(mp, info.Inode)
		}
		return nil, statusToErrno(status)
	}
	if mw.EnableSummary {
		var filesInc, dirsInc int64
		if proto.IsDir(mode) {
			dirsInc = 1
		} else {
			filesInc = 1
		}
		go mw.UpdateSummary_ll(parentID, filesInc, dirsInc, 0)
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
		if status == statusNoent {
			// For NOENT error, pull the latest mp and give it another try,
			// in case the mp view is outdated.
			mw.triggerAndWaitForceUpdate()
			return mw.doInodeGet(inode)
		}
		return nil, statusToErrno(status)
	}
	log.LogDebugf("InodeGet_ll: info(%v)", info)
	return info, nil
}

// Just like InodeGet but without retry
func (mw *MetaWrapper) doInodeGet(inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeGet_ll: No such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.iget(mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	log.LogDebugf("doInodeGet: info(%v)", info)
	return info, nil
}

func (mw *MetaWrapper) BatchInodeGet(inodes []uint64) []*proto.InodeInfo {
	var wg sync.WaitGroup

	batchInfos := make([]*proto.InodeInfo, 0)
	resp := make(chan []*proto.InodeInfo, BatchIgetRespBuf)
	candidates := make(map[uint64][]uint64)

	// Target partition does not have to be very accurate.
	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ino)
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
		go mw.batchIget(&wg, mp, inos, resp)
	}

	go func() {
		wg.Wait()
		close(resp)
	}()

	for infos := range resp {
		batchInfos = append(batchInfos, infos...)
	}

	log.LogDebugf("BatchInodeGet: inodesCnt(%d)", len(inodes))
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
	// Collect meta partitions
	var (
		mps      = make(map[uint64]*MetaPartition) // Mapping: partition ID -> partition
		mpInodes = make(map[uint64][]uint64)       // Mapping: partition ID -> inodes
	)
	for _, ino := range inodes {
		var mp = mw.getPartitionByInode(ino)
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
			xattrs, err := mw.batchGetXAttr(mp, inodes, keys)
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

func (mw *MetaWrapper) Delete_ll(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error) {
	if mw.enableTx(proto.TxOpMaskRemove) {
		return mw.txDelete_ll(parentID, name, isDir)
	} else {
		return mw.delete_ll(parentID, name, isDir)
	}
}

func (mw *MetaWrapper) txDelete_ll(parentID uint64, name string, isDir bool) (info *proto.InodeInfo, err error) {
	var (
		status int
		inode  uint64
		mode   uint32
		mp     *MetaPartition
	)

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("txDelete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	var tx *Transaction
	defer func() {
		if tx != nil {
			err = tx.OnDone(err, mw)
		}
	}()

	status, inode, mode, err = mw.lookup(parentMP, parentID, name)
	if err != nil || status != statusOK {
		return nil, statusErrToErrno(status, err)
	}

	mp = mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("txDelete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
		return nil, syscall.EINVAL
	}

	if isDir && !proto.IsDir(mode) {
		return nil, syscall.EINVAL
	}

	if isDir && mw.EnableQuota {
		quotaInfos, err := mw.GetInodeQuota_ll(inode)
		if err != nil {
			log.LogErrorf("get inode [%v] quota failed [%v]", inode, err)
			return nil, syscall.ENOENT
		}
		for _, info := range quotaInfos {
			if info.RootInode {
				log.LogErrorf("can not remove quota Root inode equal inode [%v]", inode)
				return nil, syscall.EACCES
			}
		}
	}

	tx, err = NewDeleteTransaction(parentMP, parentID, name, mp, inode, mw.TxTimeout)
	if err != nil {
		return nil, syscall.EAGAIN
	}

	status, err = mw.txCreateTX(tx, parentMP)
	if status != statusOK || err != nil {
		return nil, statusErrToErrno(status, err)
	}

	funcs := make([]func() (int, error), 0)

	funcs = append(funcs, func() (int, error) {
		status, inode, err = mw.txDdelete(tx, parentMP, parentID, inode, name)
		return status, err
	})

	funcs = append(funcs, func() (int, error) {
		status, info, err = mw.txIunlink(tx, mp, inode)
		return status, err
	})

	// 2. prepare transaction
	var preErr error
	wg := sync.WaitGroup{}
	for _, fc := range funcs {
		wg.Add(1)
		go func(f func() (int, error)) {
			defer wg.Done()
			tStatus, tErr := f()
			if tStatus != statusOK || tErr != nil {
				preErr = statusErrToErrno(tStatus, tErr)
			}
		}(fc)
	}
	wg.Wait()

	if preErr != nil {
		return info, preErr
	}

	if mw.EnableSummary {
		var job func()
		//go func() {
		if proto.IsDir(mode) {
			job = func() {
				mw.UpdateSummary_ll(parentID, 0, -1, 0)
			}
		} else {
			job = func() {
				mw.UpdateSummary_ll(parentID, -1, 0, -int64(info.Size))
			}
		}

		tx.SetOnCommit(job)
	}

	return info, preErr
}

/*
 * Note that the return value of InodeInfo might be nil without error,
 * and the caller should make sure InodeInfo is valid before using it.
 */
func (mw *MetaWrapper) delete_ll(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error) {
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
		log.LogErrorf("delete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
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
			log.LogErrorf("delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
			return nil, syscall.EAGAIN
		}
		status, info, err = mw.iget(mp, inode)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		if info == nil || info.Nlink > 2 {
			return nil, syscall.ENOTEMPTY
		}
		if mw.EnableQuota {
			quotaInfos, err := mw.GetInodeQuota_ll(inode)
			if err != nil {
				log.LogErrorf("get inode [%v] quota failed [%v]", inode, err)
				return nil, syscall.ENOENT
			}
			for _, info := range quotaInfos {
				if info.RootInode {
					log.LogErrorf("can not remove quota Root inode equal inode [%v]", inode)
					return nil, syscall.EACCES
				}
			}
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
		log.LogErrorf("delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
		return nil, nil
	}

	status, info, err = mw.iunlink(mp, inode)
	if err != nil || status != statusOK {
		return nil, nil
	}

	if mw.EnableSummary {
		go func() {
			if proto.IsDir(mode) {
				mw.UpdateSummary_ll(parentID, 0, -1, 0)
			} else {
				mw.UpdateSummary_ll(parentID, -1, 0, -int64(info.Size))
			}
		}()
	}

	return info, nil
}

func (mw *MetaWrapper) Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, overwritten bool) (err error) {
	if mw.enableTx(proto.TxOpMaskRename) {
		return mw.txRename_ll(srcParentID, srcName, dstParentID, dstName, overwritten)
	} else {
		return mw.rename_ll(srcParentID, srcName, dstParentID, dstName, overwritten)
	}
}

func (mw *MetaWrapper) txRename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, overwritten bool) (err error) {
	var tx *Transaction
	defer func() {
		if tx != nil {
			err = tx.OnDone(err, mw)
		}
	}()

	srcParentMP := mw.getPartitionByInode(srcParentID)
	if srcParentMP == nil {
		return syscall.ENOENT
	}
	dstParentMP := mw.getPartitionByInode(dstParentID)
	if dstParentMP == nil {
		return syscall.ENOENT
	}
	// look up for the src ino
	status, srcInode, srcMode, err := mw.lookup(srcParentMP, srcParentID, srcName)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}

	tx, err = NewRenameTransaction(srcParentMP, srcParentID, srcName, dstParentMP, dstParentID, dstName, mw.TxTimeout)
	if err != nil {
		return syscall.EAGAIN
	}

	funcs := make([]func() (int, error), 0)

	status, dstInode, dstMode, err := mw.lookup(dstParentMP, dstParentID, dstName)
	if err == nil && status == statusOK {

		// Note that only regular files are allowed to be overwritten.
		if !proto.IsRegular(dstMode) || !overwritten || !proto.IsRegular(srcMode) {
			return syscall.EEXIST
		}

		oldInodeMP := mw.getPartitionByInode(dstInode)
		if oldInodeMP == nil {
			return syscall.EAGAIN
		}

		err = RenameTxReplaceInode(tx, oldInodeMP, dstInode)
		if err != nil {
			return syscall.EAGAIN
		}

		funcs = append(funcs, func() (int, error) {
			status, _, err := mw.txDupdate(tx, dstParentMP, dstParentID, dstName, srcInode, dstInode)
			return status, err
		})

		funcs = append(funcs, func() (int, error) {
			status, _, err := mw.txIunlink(tx, oldInodeMP, dstInode)
			if status == statusNoent {
				return statusOK, nil
			}
			return status, err
		})

		if log.EnableDebug() {
			log.LogDebugf("txRename_ll: tx(%v), pid:%v, name:%v, old(ino:%v) is replaced by src(new ino:%v)",
				tx.txInfo, dstParentID, dstName, dstInode, srcInode)
		}
	} else if status == statusNoent {
		funcs = append(funcs, func() (int, error) {
			status, err := mw.txDcreate(tx, dstParentMP, dstParentID, dstName, srcInode, srcMode, []uint32{})
			return status, err
		})

	} else {
		return statusToErrno(status)
	}

	//var inode uint64
	funcs = append(funcs, func() (int, error) {
		status, _, err := mw.txDdelete(tx, srcParentMP, srcParentID, srcInode, srcName)
		return status, err
	})

	if log.EnableDebug() {
		log.LogDebugf("txRename_ll: tx(%v), pid:%v, name:%v, old(ino:%v) is replaced by src(new ino:%v)",
			tx.txInfo, dstParentID, dstName, dstInode, srcInode)
	}

	// 1. create transaction
	status, err = mw.txCreateTX(tx, dstParentMP)
	if status != statusOK || err != nil {
		return statusErrToErrno(status, err)
	}

	// 2. prepare transaction
	var preErr error
	wg := sync.WaitGroup{}
	for _, fc := range funcs {
		wg.Add(1)
		go func(f func() (int, error)) {
			defer wg.Done()
			tStatus, tErr := f()
			if tStatus != statusOK || tErr != nil {
				preErr = statusErrToErrno(tStatus, tErr)
			}
		}(fc)
	}
	wg.Wait()

	if preErr != nil {
		return preErr
	}

	//update summary
	var job func()
	if mw.EnableSummary {
		var srcInodeInfo *proto.InodeInfo
		var dstInodeInfo *proto.InodeInfo

		srcInodeInfo, _ = mw.InodeGet_ll(srcInode)
		if dstInode != 0 {
			dstInodeInfo, _ = mw.InodeGet_ll(dstInode)
			sizeInc := srcInodeInfo.Size - dstInodeInfo.Size
			job = func() {
				mw.UpdateSummary_ll(srcParentID, -1, 0, -int64(srcInodeInfo.Size))
				mw.UpdateSummary_ll(dstParentID, 0, 0, int64(sizeInc))
			}
			tx.SetOnCommit(job)
			return
		} else {
			sizeInc := int64(srcInodeInfo.Size)
			if proto.IsRegular(srcMode) {
				log.LogDebugf("txRename_ll: update summary when file dentry is replaced")
				job = func() {
					mw.UpdateSummary_ll(srcParentID, -1, 0, -sizeInc)
					mw.UpdateSummary_ll(dstParentID, 1, 0, sizeInc)
				}
			} else {
				log.LogDebugf("txRename_ll: update summary when dir dentry is replaced")
				job = func() {
					mw.UpdateSummary_ll(srcParentID, 0, -1, 0)
					mw.UpdateSummary_ll(dstParentID, 0, 1, 0)
				}
			}
			tx.SetOnCommit(job)
		}
	}

	// TODO
	// job = func() {
	// 	var inodes []uint64
	// 	inodes = append(inodes, srcInode)
	// 	srcQuotaInfos, err := mw.GetInodeQuota_ll(srcParentID)
	// 	if err != nil {
	// 		log.LogErrorf("rename_ll get src parent inode [%v] quota fail [%v]", srcParentID, err)
	// 	}

	// 	destQuotaInfos, err := mw.getInodeQuota(dstParentMP, dstParentID)
	// 	if err != nil {
	// 		log.LogErrorf("rename_ll: get dst partent inode [%v] quota fail [%v]", dstParentID, err)
	// 	}

	// 	if mapHaveSameKeys(srcQuotaInfos, destQuotaInfos) {
	// 		return
	// 	}

	// 	for quotaId := range srcQuotaInfos {
	// 		mw.BatchDeleteInodeQuota_ll(inodes, quotaId)
	// 	}

	// 	for quotaId, info := range destQuotaInfos {
	// 		log.LogDebugf("BatchSetInodeQuota_ll inodes [%v] quotaId [%v] rootInode [%v]", inodes, quotaId, info.RootInode)
	// 		mw.BatchSetInodeQuota_ll(inodes, quotaId, false)
	// 	}
	// }
	// tx.SetOnCommit(job)
	return nil
}

func (mw *MetaWrapper) rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, overwritten bool) (err error) {
	var oldInode uint64

	srcParentMP := mw.getPartitionByInode(srcParentID)
	if srcParentMP == nil {
		return syscall.ENOENT
	}
	dstParentMP := mw.getPartitionByInode(dstParentID)
	if dstParentMP == nil {
		return syscall.ENOENT
	}

	status, info, err := mw.iget(dstParentMP, dstParentID)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}

	quota := atomic.LoadUint32(&mw.DirChildrenNumLimit)
	if info.Nlink >= quota {
		log.LogErrorf("rename_ll: dst parent inode's nlink quota reached, parentID(%v)", dstParentID)
		return syscall.EDQUOT
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
		if status == statusOpDirQuota {
			return statusToErrno(status)
		}
		return syscall.EAGAIN
	}
	var srcInodeInfo *proto.InodeInfo
	var dstInodeInfo *proto.InodeInfo
	if mw.EnableSummary {
		srcInodeInfo, _ = mw.InodeGet_ll(inode)
	}

	// Note that only regular files are allowed to be overwritten.
	if status == statusExist && (proto.IsSymlink(mode) || proto.IsRegular(mode)) {
		if !overwritten {
			return syscall.EEXIST
		}

		status, oldInode, err = mw.dupdate(dstParentMP, dstParentID, dstName, inode)
		if err != nil {
			return syscall.EAGAIN
		}
		if mw.EnableSummary {
			dstInodeInfo, _ = mw.InodeGet_ll(oldInode)
		}
	}

	if status != statusOK {
		mw.iunlink(srcMP, inode)
		return statusToErrno(status)
	}

	// delete dentry from src parent
	status, _, err = mw.ddelete(srcParentMP, srcParentID, srcName)
	if err != nil {
		log.LogErrorf("mw.ddelete(srcParentMP, srcParentID, %s) failed.", srcName)
		return statusToErrno(status)
	} else if status != statusOK {
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
		// overwritten
		inodeMP := mw.getPartitionByInode(oldInode)
		if inodeMP != nil {
			mw.iunlink(inodeMP, oldInode)
			// evict oldInode to avoid oldInode becomes orphan inode
			mw.ievict(inodeMP, oldInode)
		}
		if mw.EnableSummary {
			sizeInc := srcInodeInfo.Size - dstInodeInfo.Size
			go func() {
				mw.UpdateSummary_ll(srcParentID, -1, 0, -int64(srcInodeInfo.Size))
				mw.UpdateSummary_ll(dstParentID, 0, 0, int64(sizeInc))
			}()
		}
	} else {
		if mw.EnableSummary {
			sizeInc := int64(srcInodeInfo.Size)
			if proto.IsRegular(mode) {
				// file
				go func() {
					mw.UpdateSummary_ll(srcParentID, -1, 0, -sizeInc)
					mw.UpdateSummary_ll(dstParentID, 1, 0, sizeInc)
				}()
			} else {
				// dir
				go func() {
					mw.UpdateSummary_ll(srcParentID, 0, -1, 0)
					mw.UpdateSummary_ll(dstParentID, 0, 1, 0)
				}()
			}
		}
	}
	// TODO
	// var inodes []uint64
	// inodes = append(inodes, inode)
	// srcQuotaInfos, err := mw.GetInodeQuota_ll(srcParentID)
	// if err != nil {
	// 	log.LogErrorf("rename_ll get src parent inode [%v] quota fail [%v]", srcParentID, err)
	// }

	// destQuotaInfos, err := mw.getInodeQuota(dstParentMP, dstParentID)
	// if err != nil {
	// 	log.LogErrorf("rename_ll: get dst partent inode [%v] quota fail [%v]", dstParentID, err)
	// }

	// if mapHaveSameKeys(srcQuotaInfos, destQuotaInfos) {
	// 	return nil
	// }

	// for quotaId := range srcQuotaInfos {
	// 	mw.BatchDeleteInodeQuota_ll(inodes, quotaId)
	// }

	// for quotaId, info := range destQuotaInfos {
	// 	log.LogDebugf("BatchSetInodeQuota_ll inodes [%v] quotaId [%v] rootInode [%v]", inodes, quotaId, info.RootInode)
	// 	mw.BatchSetInodeQuota_ll(inodes, quotaId, false)
	// }

	return nil
}

// Read all dentries with parentID
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

// Read limit count dentries with parentID, start from string
func (mw *MetaWrapper) ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, err := mw.readdirlimit(parentMP, parentID, from, limit)
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

// Allocated as a callback by stream sdk
func (mw *MetaWrapper) AppendExtentKey(parentInode, inode uint64, ek proto.ExtentKey, discard []proto.ExtentKey) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}
	var oldInfo *proto.InodeInfo
	if mw.EnableSummary {
		oldInfo, _ = mw.InodeGet_ll(inode)
	}

	status, err := mw.appendExtentKey(mp, inode, ek, discard)
	if err != nil || status != statusOK {
		log.LogErrorf("AppendExtentKey: inode(%v) ek(%v) local discard(%v) err(%v) status(%v)", inode, ek, discard, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("AppendExtentKey: ino(%v) ek(%v) discard(%v)", inode, ek, discard)

	if mw.EnableSummary {
		go func() {
			newInfo, _ := mw.InodeGet_ll(inode)
			if oldInfo != nil && newInfo != nil {
				if int64(oldInfo.Size) < int64(newInfo.Size) {
					mw.UpdateSummary_ll(parentInode, 0, 0, int64(newInfo.Size)-int64(oldInfo.Size))
				}
			}
		}()
	}

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

// AppendObjExtentKeys append multiple obj extent key into specified inode with single request.
func (mw *MetaWrapper) AppendObjExtentKeys(inode uint64, eks []proto.ObjExtentKey) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.appendObjExtentKeys(mp, inode, eks)
	if err != nil || status != statusOK {
		log.LogErrorf("AppendObjExtentKeys: inode(%v) objextentKeys(%v) err(%v) status(%v)", inode, eks, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("AppendObjExtentKeys: ino(%v) objextentKeys(%v)", inode, eks)
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
	log.LogDebugf("GetExtents: ino(%v) gen(%v) size(%v) extents(%v)", inode, gen, size, extents)
	return gen, size, extents, nil
}

func (mw *MetaWrapper) GetObjExtents(inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, objExtents []proto.ObjExtentKey, err error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return 0, 0, nil, nil, syscall.ENOENT
	}

	status, gen, size, extents, objExtents, err := mw.getObjExtents(mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("GetObjExtents: ino(%v) err(%v) status(%v)", inode, err, status)
		return 0, 0, nil, nil, statusToErrno(status)
	}
	log.LogDebugf("GetObjExtents: ino(%v) gen(%v) size(%v) extents(%v) objextents(%v)", inode, gen, size, extents, objExtents)
	return gen, size, extents, objExtents, nil
}

// func (mw *MetaWrapper) DelExtentKeys(inode uint64, eks []proto.ExtentKey) error {
// 	mp := mw.getPartitionByInode(inode)
// 	if mp == nil {
// 		return syscall.ENOENT
// 	}

// 	status, err := mw.delExtentKey(mp, inode, eks)
// 	if err != nil || status != statusOK {
// 		log.LogErrorf("DelExtentKeys: inode(%v) eks(%v)err(%v) status(%v)", inode, eks, err, status)
// 		return statusToErrno(status)
// 	}
// 	log.LogDebugf("DelExtentKeys: ino(%v) eks(%v)", inode, eks)
// 	return nil
// }

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
	//if mw.EnableTransaction {
	if mw.EnableTransaction&proto.TxOpMaskLink > 0 {
		return mw.txLink(parentID, name, ino)
	} else {
		return mw.link(parentID, name, ino)
	}
}

func (mw *MetaWrapper) txLink(parentID uint64, name string, ino uint64) (info *proto.InodeInfo, err error) {
	//var err error
	var status int
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("txLink: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	status, info, err = mw.iget(parentMP, parentID)
	if err != nil || status != statusOK {
		return nil, statusErrToErrno(status, err)
	}

	quota := atomic.LoadUint32(&mw.DirChildrenNumLimit)
	if info.Nlink >= quota {
		log.LogErrorf("txLink: parent inode's nlink quota reached, parentID(%v)", parentID)
		return nil, syscall.EDQUOT
	}

	mp := mw.getPartitionByInode(ino)
	if mp == nil {
		log.LogErrorf("txLink: No target inode partition, ino(%v)", ino)
		return nil, syscall.ENOENT
	}
	var tx *Transaction

	defer func() {
		if tx != nil {
			err = tx.OnDone(err, mw)
		}
	}()

	tx, err = NewLinkTransaction(parentMP, parentID, name, mp, ino, mw.TxTimeout)
	if err != nil {
		return nil, syscall.EAGAIN
	}

	status, err = mw.txCreateTX(tx, parentMP)
	if status != statusOK || err != nil {
		return nil, statusErrToErrno(status, err)
	}

	funcs := make([]func() (int, error), 0)

	funcs = append(funcs, func() (int, error) {
		status, info, err = mw.txIlink(tx, mp, ino)
		return status, err
	})

	funcs = append(funcs, func() (int, error) {
		quotaInfos, err := mw.getInodeQuota(parentMP, parentID)
		if err != nil {
			log.LogErrorf("link: get parent quota fail, parentID(%v) err(%v)", parentID, err)
			return statusError, syscall.ENOENT
		}

		var quotaIds []uint32
		for quotaId := range quotaInfos {
			quotaIds = append(quotaIds, quotaId)
		}

		status, err = mw.txDcreate(tx, parentMP, parentID, name, ino, info.Mode, quotaIds)
		return status, err
	})

	// 2. prepare transaction
	var preErr error
	wg := sync.WaitGroup{}
	for _, fc := range funcs {
		wg.Add(1)
		go func(f func() (int, error)) {
			defer wg.Done()
			tStatus, tErr := f()
			if tStatus != statusOK || tErr != nil {
				preErr = statusErrToErrno(tStatus, tErr)
			}
		}(fc)
	}
	wg.Wait()

	if preErr != nil {
		return nil, preErr
	}

	return info, nil
}

func (mw *MetaWrapper) link(parentID uint64, name string, ino uint64) (*proto.InodeInfo, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Link: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.iget(parentMP, parentID)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	quota := atomic.LoadUint32(&mw.DirChildrenNumLimit)
	if info.Nlink >= quota {
		log.LogErrorf("link: parent inode's nlink quota reached, parentID(%v)", parentID)
		return nil, syscall.EDQUOT
	}

	mp := mw.getPartitionByInode(ino)
	if mp == nil {
		log.LogErrorf("Link: No target inode partition, ino(%v)", ino)
		return nil, syscall.ENOENT
	}

	// increase inode nlink
	status, info, err = mw.ilink(mp, ino)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	if mw.EnableQuota {
		quotaInfos, err := mw.getInodeQuota(parentMP, parentID)
		if err != nil {
			log.LogErrorf("link: get parent quota fail, parentID(%v) err(%v)", parentID, err)
			return nil, syscall.ENOENT
		}
		var quotaIds []uint32
		for quotaId := range quotaInfos {
			quotaIds = append(quotaIds, quotaId)
		}
		// create new dentry and refer to the inode
		status, err = mw.quotaDcreate(parentMP, parentID, name, ino, info.Mode, quotaIds)
	} else {
		status, err = mw.dcreate(parentMP, parentID, name, ino, info.Mode)
	}
	if err != nil {
		return nil, statusToErrno(status)
	} else if status != statusOK {
		if status != statusExist {
			mw.iunlink(mp, ino)
		}
		return nil, statusToErrno(status)
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

func (mw *MetaWrapper) Setattr(inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Setattr: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}

	status, err := mw.setattr(mp, inode, valid, mode, uid, gid, atime, mtime)
	if err != nil || status != statusOK {
		log.LogErrorf("Setattr: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}

	return nil
}

func (mw *MetaWrapper) InodeCreate_ll(parentID uint64, mode, uid, gid uint32, target []byte, quotaIds []uint64) (*proto.InodeInfo, error) {
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
	if mw.EnableQuota && parentID != 0 {
		var quotaIds []uint32
		parentMP := mw.getPartitionByInode(parentID)
		if parentMP == nil {
			log.LogErrorf("InodeCreate_ll: No parent partition, parentID(%v)", parentID)
			return nil, syscall.ENOENT
		}
		quotaInfos, err := mw.getInodeQuota(parentMP, parentID)
		if err != nil {
			log.LogErrorf("InodeCreate_ll: get parent quota fail, parentID(%v) err(%v)", parentID, err)
			return nil, syscall.ENOENT
		}
		for quotaId := range quotaInfos {
			quotaIds = append(quotaIds, quotaId)
		}

		for i := 0; i < length; i++ {
			index := (int(epoch) + i) % length
			mp = rwPartitions[index]
			status, info, err = mw.quotaIcreate(mp, mode, uid, gid, target, quotaIds)
			if err == nil && status == statusOK {
				return info, nil
			} else if status == statusNoSpace {
				log.LogErrorf("InodeCreate_ll status %v", status)
				return nil, statusToErrno(status)
			}
		}
	} else {
		for i := 0; i < length; i++ {
			index := (int(epoch) + i) % length
			mp = rwPartitions[index]
			status, info, err = mw.icreate(mp, mode, uid, gid, target)
			if err == nil && status == statusOK {
				return info, nil
			} else if status == statusNoSpace {
				log.LogErrorf("InodeCreate_ll status %v", status)
				return nil, statusToErrno(status)
			}
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

func (mw *MetaWrapper) InodeClearPreloadCache_ll(inode uint64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeClearPreloadCache_ll: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}
	status, err := mw.iclearCache(mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("InodeClearPreloadCache_ll: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) InitMultipart_ll(path string, extend map[string]string) (multipartId string, err error) {
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
		status, sessionId, err := mw.createMultipart(mp, path, extend)
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

func (mw *MetaWrapper) GetMultipart_ll(path, multipartId string) (info *proto.MultipartInfo, err error) {
	var (
		mpId  uint64
		found bool
	)
	mpId, found = util.MultipartIDFromString(multipartId).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartId, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		info, _, err = mw.broadcastGetMultipart(path, multipartId)
		return
	}
	var mp = mw.getPartitionByID(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, multipartInfo, err := mw.getMultipart(mp, path, multipartId)
	if err != nil || status != statusOK {
		log.LogErrorf("GetMultipartRequest: err(%v) status(%v)", err, status)
		return nil, statusToErrno(status)
	}
	return multipartInfo, nil
}

func (mw *MetaWrapper) AddMultipartPart_ll(path, multipartId string, partId uint16, size uint64, md5 string, inodeInfo *proto.InodeInfo) (oldInode uint64, updated bool, err error) {
	var (
		mpId  uint64
		found bool
	)
	mpId, found = util.MultipartIDFromString(multipartId).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartId, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		if _, mpId, err = mw.broadcastGetMultipart(path, multipartId); err != nil {
			log.LogErrorf("AddMultipartPart_ll: broadcast get multipart fail: multipartId(%v) err(%v)", multipartId, err)
			return
		}
	}
	var mp = mw.getPartitionByID(mpId)
	if mp == nil {
		log.LogWarnf("AddMultipartPart_ll: has no meta partition: multipartId(%v) mpId(%v)", multipartId, mpId)
		err = syscall.ENOENT
		return
	}
	status, oldInode, updated, err := mw.addMultipartPart(mp, path, multipartId, partId, size, md5, inodeInfo)
	if err != nil || status != statusOK {
		log.LogErrorf("AddMultipartPart_ll: err(%v) status(%v)", err, status)
		return 0, false, statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) RemoveMultipart_ll(path, multipartID string) (err error) {
	var (
		mpId  uint64
		found bool
	)
	mpId, found = util.MultipartIDFromString(multipartID).PartitionID()
	if !found {
		log.LogDebugf("AddMultipartPart_ll: meta partition not found by multipart id, multipartId(%v), err(%v)", multipartID, err)
		// If meta partition not found by multipart id, broadcast to all meta partitions to find it
		if _, mpId, err = mw.broadcastGetMultipart(path, multipartID); err != nil {
			return
		}
	}
	var mp = mw.getPartitionByID(mpId)
	if mp == nil {
		err = syscall.ENOENT
		return
	}
	status, err := mw.removeMultipart(mp, path, multipartID)
	if err != nil || status != statusOK {
		log.LogErrorf(" RemoveMultipart_ll: partition remove multipart fail: "+
			"volume(%v) partitionID(%v) multipartID(%v) err(%v) status(%v)",
			mw.volname, mp.PartitionID, multipartID, err, status)
		return statusToErrno(status)
	}
	return
}

func (mw *MetaWrapper) broadcastGetMultipart(path, multipartId string) (info *proto.MultipartInfo, mpID uint64, err error) {
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
			status, multipartInfo, err := mw.getMultipart(mp, path, multipartId)
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

func (mw *MetaWrapper) ListMultipart_ll(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error) {
	partitions := mw.partitions
	var wg = sync.WaitGroup{}
	var wl = sync.Mutex{}
	var sessions = make([]*proto.MultipartInfo, 0)

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
	log.LogDebugf("XAttrSet_ll: set xattr: volume(%v) inode(%v) name(%v) value(%v) status(%v)",
		mw.volname, inode, name, value, status)
	return nil
}

func (mw *MetaWrapper) BatchSetXAttr_ll(inode uint64, attrs map[string]string) error {
	var err error
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("XAttrSet_ll: no such partition, inode(%v)", inode)
		return syscall.ENOENT
	}
	var status int
	status, err = mw.batchSetXAttr(mp, inode, attrs)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	log.LogDebugf("BatchSetXAttr_ll: set xattr: volume(%v) inode(%v) attrs(%v) status(%v)",
		mw.volname, inode, attrs, status)
	return nil
}

func (mw *MetaWrapper) XAttrGetAll_ll(inode uint64) (*proto.XAttrInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("XAttrGetAll_ll: no such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	attrs, status, err := mw.getAllXAttr(mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	xAttr := &proto.XAttrInfo{
		Inode:  inode,
		XAttrs: attrs,
	}

	log.LogDebugf("XAttrGetAll_ll: volume(%v) inode(%v) attrs(%v)",
		mw.volname, inode, attrs)
	return xAttr, nil
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

	log.LogDebugf("XAttrGet_ll: get xattr: volume(%v) inode(%v) name(%v) value(%v)",
		mw.volname, inode, string(name), string(value))
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

func (mw *MetaWrapper) XAttrsList_ll(inode uint64) ([]string, error) {
	var err error
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("XAttrsList_ll: no such partition, inode(%v)", inode)
		return nil, syscall.ENOENT
	}
	keys, status, err := mw.listXAttr(mp, inode)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	return keys, nil
}

func (mw *MetaWrapper) UpdateSummary_ll(parentIno uint64, filesInc int64, dirsInc int64, bytesInc int64) {
	if filesInc == 0 && dirsInc == 0 && bytesInc == 0 {
		return
	}
	mp := mw.getPartitionByInode(parentIno)
	if mp == nil {
		log.LogErrorf("UpdateSummary_ll: no such partition, inode(%v)", parentIno)
		return
	}
	for cnt := 0; cnt < UpdateSummaryRetry; cnt++ {
		err := mw.updateXAttrs(mp, parentIno, filesInc, dirsInc, bytesInc)
		if err == nil {
			return
		}
	}
	return
}

func (mw *MetaWrapper) ReadDirOnly_ll(parentID uint64) ([]proto.Dentry, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, err := mw.readdironly(parentMP, parentID)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return children, nil
}

type SummaryInfo struct {
	Files   int64
	Subdirs int64
	Fbytes  int64
}

func (mw *MetaWrapper) GetSummary_ll(parentIno uint64, goroutineNum int32) (SummaryInfo, error) {
	if goroutineNum > MaxSummaryGoroutineNum {
		goroutineNum = MaxSummaryGoroutineNum
	}
	if goroutineNum <= 0 {
		goroutineNum = 1
	}
	var summaryInfo SummaryInfo
	errCh := make(chan error)
	var wg sync.WaitGroup
	var currentGoroutineNum int32 = 0
	if mw.EnableSummary {
		inodeCh := make(chan uint64, ChannelLen)
		wg.Add(1)
		atomic.AddInt32(&currentGoroutineNum, 1)
		inodeCh <- parentIno
		go mw.getDentry(parentIno, inodeCh, errCh, &wg, &currentGoroutineNum, true, goroutineNum)
		go func() {
			wg.Wait()
			close(inodeCh)
		}()

		go mw.getDirSummary(&summaryInfo, inodeCh, errCh)
		for err := range errCh {
			return SummaryInfo{0, 0, 0}, err
		}
		return summaryInfo, nil
	} else {
		summaryCh := make(chan SummaryInfo, ChannelLen)
		wg.Add(1)
		atomic.AddInt32(&currentGoroutineNum, 1)
		go mw.getSummaryOrigin(parentIno, summaryCh, errCh, &wg, &currentGoroutineNum, true, goroutineNum)
		go func() {
			wg.Wait()
			close(summaryCh)
		}()
		go func(summaryInfo *SummaryInfo) {
			for summary := range summaryCh {
				summaryInfo.Files = summaryInfo.Files + summary.Files
				summaryInfo.Subdirs = summaryInfo.Subdirs + summary.Subdirs
				summaryInfo.Fbytes = summaryInfo.Fbytes + summary.Fbytes
			}
			close(errCh)
		}(&summaryInfo)
		for err := range errCh {
			return SummaryInfo{0, 0, 0}, err
		}
		return summaryInfo, nil
	}
}

func (mw *MetaWrapper) getDentry(parentIno uint64, inodeCh chan<- uint64, errCh chan<- error, wg *sync.WaitGroup, currentGoroutineNum *int32, newGoroutine bool, goroutineNum int32) {
	defer func() {
		if newGoroutine {
			atomic.AddInt32(currentGoroutineNum, -1)
			wg.Done()
		}
	}()
	entries, err := mw.ReadDirOnly_ll(parentIno)
	if err != nil {
		errCh <- err
		return
	}
	for _, entry := range entries {
		inodeCh <- entry.Inode
		if atomic.LoadInt32(currentGoroutineNum) < goroutineNum {
			wg.Add(1)
			atomic.AddInt32(currentGoroutineNum, 1)
			go mw.getDentry(entry.Inode, inodeCh, errCh, wg, currentGoroutineNum, true, goroutineNum)
		} else {
			mw.getDentry(entry.Inode, inodeCh, errCh, wg, currentGoroutineNum, false, goroutineNum)
		}
	}
}

func (mw *MetaWrapper) getDirSummary(summaryInfo *SummaryInfo, inodeCh <-chan uint64, errch chan<- error) {
	var inodes []uint64
	var keys []string
	for inode := range inodeCh {
		inodes = append(inodes, inode)
		keys = append(keys, SummaryKey)
		if len(inodes) < BatchSize {
			continue
		}
		xattrInfos, err := mw.BatchGetXAttr(inodes, keys)
		if err != nil {
			errch <- err
			return
		}
		inodes = inodes[0:0]
		keys = keys[0:0]
		for _, xattrInfo := range xattrInfos {
			if xattrInfo.XAttrs[SummaryKey] != "" {
				summaryList := strings.Split(xattrInfo.XAttrs[SummaryKey], ",")
				files, _ := strconv.ParseInt(summaryList[0], 10, 64)
				subdirs, _ := strconv.ParseInt(summaryList[1], 10, 64)
				fbytes, _ := strconv.ParseInt(summaryList[2], 10, 64)
				summaryInfo.Files += files
				summaryInfo.Subdirs += subdirs
				summaryInfo.Fbytes += fbytes
			}
		}
	}
	xattrInfos, err := mw.BatchGetXAttr(inodes, keys)
	if err != nil {
		errch <- err
		return
	}
	for _, xattrInfo := range xattrInfos {
		if xattrInfo.XAttrs[SummaryKey] != "" {
			summaryList := strings.Split(xattrInfo.XAttrs[SummaryKey], ",")
			files, _ := strconv.ParseInt(summaryList[0], 10, 64)
			subdirs, _ := strconv.ParseInt(summaryList[1], 10, 64)
			fbytes, _ := strconv.ParseInt(summaryList[2], 10, 64)
			summaryInfo.Files += files
			summaryInfo.Subdirs += subdirs
			summaryInfo.Fbytes += fbytes
		}
	}
	close(errch)
	return
}

func (mw *MetaWrapper) getSummaryOrigin(parentIno uint64, summaryCh chan<- SummaryInfo, errCh chan<- error, wg *sync.WaitGroup, currentGoroutineNum *int32, newGoroutine bool, goroutineNum int32) {
	defer func() {
		if newGoroutine {
			atomic.AddInt32(currentGoroutineNum, -1)
			wg.Done()
		}
	}()
	var subdirsList []uint64
	retSummaryInfo := SummaryInfo{
		Files:   0,
		Subdirs: 0,
		Fbytes:  0,
	}
	children, err := mw.ReadDir_ll(parentIno)
	if err != nil {
		errCh <- err
		return
	}
	for _, dentry := range children {
		if proto.IsDir(dentry.Type) {
			retSummaryInfo.Subdirs += 1
			subdirsList = append(subdirsList, dentry.Inode)
		} else {
			fileInfo, err := mw.InodeGet_ll(dentry.Inode)
			if err != nil {
				errCh <- err
				return
			}
			retSummaryInfo.Files += 1
			retSummaryInfo.Fbytes += int64(fileInfo.Size)
		}
	}
	summaryCh <- retSummaryInfo
	for _, subdirIno := range subdirsList {
		if atomic.LoadInt32(currentGoroutineNum) < goroutineNum {
			wg.Add(1)
			atomic.AddInt32(currentGoroutineNum, 1)
			go mw.getSummaryOrigin(subdirIno, summaryCh, errCh, wg, currentGoroutineNum, true, goroutineNum)
		} else {
			mw.getSummaryOrigin(subdirIno, summaryCh, errCh, wg, currentGoroutineNum, false, goroutineNum)
		}
	}
}

func (mw *MetaWrapper) RefreshSummary_ll(parentIno uint64, goroutineNum int32) error {
	if goroutineNum > MaxSummaryGoroutineNum {
		goroutineNum = MaxSummaryGoroutineNum
	}
	if goroutineNum <= 0 {
		goroutineNum = 1
	}
	var wg sync.WaitGroup
	var currentGoroutineNum int32 = 0
	errch := make(chan error)
	wg.Add(1)
	atomic.AddInt32(&currentGoroutineNum, 1)
	go mw.refreshSummary(parentIno, errch, &wg, &currentGoroutineNum, true, goroutineNum)
	go func() {
		wg.Wait()
		close(errch)
	}()
	for err := range errch {
		return err
	}
	return nil
}

func (mw *MetaWrapper) refreshSummary(parentIno uint64, errCh chan<- error, wg *sync.WaitGroup, currentGoroutineNum *int32, newGoroutine bool, goroutineNum int32) {
	defer func() {
		if newGoroutine {
			atomic.AddInt32(currentGoroutineNum, -1)
			wg.Done()
		}
	}()
	summaryXAttrInfo, err := mw.XAttrGet_ll(parentIno, SummaryKey)
	if err != nil {
		errCh <- err
		return
	}
	oldSummaryInfo := SummaryInfo{0, 0, 0}
	if summaryXAttrInfo.XAttrs[SummaryKey] != "" {
		summaryList := strings.Split(summaryXAttrInfo.XAttrs[SummaryKey], ",")
		files, _ := strconv.ParseInt(summaryList[0], 10, 64)
		subdirs, _ := strconv.ParseInt(summaryList[1], 10, 64)
		fbytes, _ := strconv.ParseInt(summaryList[2], 10, 64)
		oldSummaryInfo = SummaryInfo{
			Files:   files,
			Subdirs: subdirs,
			Fbytes:  fbytes,
		}
	} else {
		oldSummaryInfo = SummaryInfo{0, 0, 0}
	}

	newSummaryInfo := SummaryInfo{0, 0, 0}

	var subdirsList []uint64
	children, err := mw.ReadDir_ll(parentIno)
	if err != nil {
		errCh <- err
		return
	}
	for _, dentry := range children {
		if proto.IsDir(dentry.Type) {
			newSummaryInfo.Subdirs += 1
			subdirsList = append(subdirsList, dentry.Inode)
		} else {
			fileInfo, err := mw.InodeGet_ll(dentry.Inode)
			if err != nil {
				errCh <- err
				return
			}
			newSummaryInfo.Files += 1
			newSummaryInfo.Fbytes += int64(fileInfo.Size)
		}
	}
	go mw.UpdateSummary_ll(
		parentIno,
		newSummaryInfo.Files-oldSummaryInfo.Files,
		newSummaryInfo.Subdirs-oldSummaryInfo.Subdirs,
		newSummaryInfo.Fbytes-oldSummaryInfo.Fbytes,
	)

	for _, subdirIno := range subdirsList {
		if atomic.LoadInt32(currentGoroutineNum) < goroutineNum {
			wg.Add(1)
			atomic.AddInt32(currentGoroutineNum, 1)
			go mw.refreshSummary(subdirIno, errCh, wg, currentGoroutineNum, true, goroutineNum)
		} else {
			mw.refreshSummary(subdirIno, errCh, wg, currentGoroutineNum, false, goroutineNum)
		}
	}
}

func (mw *MetaWrapper) BatchSetInodeQuota_ll(inodes []uint64, quotaId uint32, IsRoot bool) (ret map[uint64]uint8, err error) {
	batchInodeMap := make(map[uint64][]uint64)
	ret = make(map[uint64]uint8, 0)
	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ino)
		if mp == nil {
			continue
		}
		if _, isFind := batchInodeMap[mp.PartitionID]; !isFind {
			batchInodeMap[mp.PartitionID] = make([]uint64, 0, 128)
		}
		batchInodeMap[mp.PartitionID] = append(batchInodeMap[mp.PartitionID], ino)
	}

	for id, inos := range batchInodeMap {
		mp := mw.getPartitionByID(id)
		resp, err := mw.batchSetInodeQuota(mp, inos, quotaId, IsRoot)
		if err != nil {
			log.LogErrorf("batchSetInodeQuota quota [%v] inodes [%v] err [%v]", quotaId, inos, err)
			return ret, err
		}
		for k, v := range resp.InodeRes {
			ret[k] = v
		}
	}

	log.LogInfof("set subInode quota [%v] inodes [%v] ret [%v] success.", quotaId, inodes, ret)
	return
}

func (mw *MetaWrapper) GetPartitionByInodeId_ll(inodeId uint64) (mp *MetaPartition) {
	return mw.getPartitionByInode(inodeId)
}

func (mw *MetaWrapper) BatchDeleteInodeQuota_ll(inodes []uint64, quotaId uint32) (ret map[uint64]uint8, err error) {
	batchInodeMap := make(map[uint64][]uint64)
	ret = make(map[uint64]uint8, 0)
	for _, ino := range inodes {
		mp := mw.getPartitionByInode(ino)
		if mp == nil {
			continue
		}
		if _, isFind := batchInodeMap[mp.PartitionID]; !isFind {
			batchInodeMap[mp.PartitionID] = make([]uint64, 0, 128)
		}
		batchInodeMap[mp.PartitionID] = append(batchInodeMap[mp.PartitionID], ino)
	}
	for id, inos := range batchInodeMap {
		mp := mw.getPartitionByID(id)
		resp, err := mw.batchDeleteInodeQuota(mp, inos, quotaId)
		if err != nil {
			log.LogErrorf("batchDeleteInodeQuota quota [%v] inodes [%v] err [%v]", quotaId, inos, err)
			return ret, err
		}
		for k, v := range resp.InodeRes {
			ret[k] = v
		}
	}

	log.LogInfof("delete subInode inodes [%v] quota [%v] ret [%v] success.", inodes, quotaId, ret)
	return
}

func (mw *MetaWrapper) GetInodeQuota_ll(inode uint64) (quotaInfos map[uint32]*proto.MetaQuotaInfo, err error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		err = fmt.Errorf("get partition by inode [%v] failed", inode)
		return nil, err
	}

	quotaInfos, err = mw.getInodeQuota(mp, inode)
	if err != nil {
		log.LogErrorf("GetInodeQuota_ll get inode [%v] quota failed [%v]", inode, err)
		return
	}

	return
}

func (mw *MetaWrapper) ApplyQuota_ll(parentIno uint64, quotaId uint32, maxConcurrencyInode uint64) (numInodes uint64, err error) {
	inodes := make([]uint64, 0, maxConcurrencyInode)
	var curInodeCount uint64
	err = mw.applyQuota(parentIno, quotaId, &numInodes, &curInodeCount, &inodes, maxConcurrencyInode, true)
	return
}

func (mw *MetaWrapper) RevokeQuota_ll(parentIno uint64, quotaId uint32, maxConcurrencyInode uint64) (numInodes uint64, err error) {
	inodes := make([]uint64, 0, maxConcurrencyInode)
	var curInodeCount uint64
	err = mw.revokeQuota(parentIno, quotaId, &numInodes, &curInodeCount, &inodes, maxConcurrencyInode, true)
	return
}
