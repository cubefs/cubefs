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
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// Low-level API, i.e. work with inode

const (
	OpenRetryInterval = 5 * time.Millisecond
	OpenRetryLimit    = 1000
	maxUniqID         = 5000
)

const (
	BatchIgetRespBuf            = 1000
	MaxSummaryGoroutineNum      = 120
	BatchGetBufLen              = 500
	UpdateSummaryRetry          = 3
	SummaryKey                  = "DirStat"
	UpdateAccessFileRetry       = 3
	AccessFileCountSsdKey       = "AccessFileCountSsd"
	AccessFileSizeSsdKey        = "AccessFileSizeSsd"
	AccessFileCountHddKey       = "AccessFileCountHdd"
	AccessFileSizeHddKey        = "AccessFileSizeHdd"
	AccessFileCountBlobStoreKey = "AccessFileCountBlobStore"
	AccessFileSizeBlobStoreKey  = "AccessFileSizeBlobStore"
	ChannelLen                  = 100
	BatchSize                   = 200
	MaxGoroutineNum             = 5
	InodeFullMaxRetryTime       = 2
	ForceUpdateRWMP             = "ForceUpdateRWMP"
)

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
	mw.rootIno = rootIno
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

func (mw *MetaWrapper) Create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte, fullPath string, ignoreExist bool) (*proto.InodeInfo, error) {
	// if mw.EnableTransaction {
	var txMask proto.TxOpMask
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
		return mw.txCreate_ll(parentID, name, mode, uid, gid, target, txType, fullPath, ignoreExist)
	} else {
		return mw.create_ll(parentID, name, mode, uid, gid, target, fullPath, ignoreExist)
	}
}

func (mw *MetaWrapper) txCreate_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte, txType uint32,
	fullPath string, ignoreExist bool,
) (info *proto.InodeInfo, err error) {
	var (
		status int
		// err          error
		// info         *proto.InodeInfo
		mp           *MetaPartition
		rwPartitions []*MetaPartition
	)

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("txCreate_ll: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	var quotaIds []uint32
	if mw.EnableQuota {
		var quotaInfos map[uint32]*proto.MetaQuotaInfo
		quotaInfos, err = mw.getInodeQuota(parentMP, parentID)
		if err != nil {
			log.LogErrorf("Create_ll: get parent quota fail, parentID(%v) err(%v)", parentID, err)
			return nil, syscall.ENOENT
		}

		for quotaId := range quotaInfos {
			quotaIds = append(quotaIds, quotaId)
		}
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

		status, info, err = mw.txIcreate(tx, mp, mode, uid, gid, target, quotaIds, fullPath)
		if err == nil && status == statusOK {
			goto create_dentry
		} else if status == statusNoSpace || status == statusForbid {
			log.LogErrorf("Create_ll status %v", status)
			return nil, statusToErrno(status)
		} else {
			// sync cancel previous transaction before retry
			tx.Rollback(mw)
		}
	}
	return nil, syscall.ENOMEM

create_dentry:
	if log.EnableDebug() {
		log.LogDebugf("txCreate_ll: tx.txInfo(%v)", tx.txInfo)
	}

	status, err = mw.txDcreate(tx, parentMP, parentID, name, info.Inode, mode, quotaIds, fullPath, ignoreExist)
	if err != nil || status != statusOK {
		return nil, statusErrToErrno(status, err)
	}

	if log.EnableDebug() {
		log.LogDebugf("txCreate_ll: tx.txInfo(%v)", tx.txInfo)
	}

	if mw.EnableSummary {
		var filesHddInc, filesSsdInc, filesBlobStoreInc, dirsInc int64
		if proto.IsDir(mode) {
			dirsInc = 1
		} else {
			if info.StorageClass == proto.StorageClass_Replica_HDD {
				filesHddInc = 1
			}
			if info.StorageClass == proto.StorageClass_Replica_SSD {
				filesSsdInc = 1
			}
			if info.StorageClass == proto.StorageClass_BlobStore {
				filesBlobStoreInc = 1
			}
		}
		// go mw.UpdateSummary_ll(parentID, filesInc, dirsInc, 0)
		job := func() {
			mw.UpdateSummary_ll(parentID, filesHddInc, filesSsdInc, filesBlobStoreInc, 0, 0, 0, dirsInc)
		}
		tx.SetOnCommit(job)
	}

	return info, nil
}

func (mw *MetaWrapper) create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte,
	fullPath string, ignoreExist bool,
) (*proto.InodeInfo, error) {
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

	status, info, err = mw.iget(parentMP, parentID, mw.LastVerSeq)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}

	quota := atomic.LoadUint32(&mw.DirChildrenNumLimit)
	if info.Nlink >= quota {
		log.LogErrorf("Create_ll: parent inode's nlink quota reached, parentID(%v)", parentID)
		return nil, syscall.EDQUOT
	}

get_rwmp:
	rwPartitions = mw.getRWPartitions()
	length := len(rwPartitions)
	epoch := atomic.AddUint64(&mw.epoch, 1)
	retryTime := 0
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
			status, info, err = mw.quotaIcreate(mp, mode, uid, gid, target, quotaIds, fullPath)
			if err == nil && status == statusOK {
				goto create_dentry
			} else if status == statusFull {
				if retryTime >= InodeFullMaxRetryTime {
					break
				}
				retryTime++
				log.LogWarnf("Mp(%v) inode is full, trigger rwmp get and retry(%v)", mp, retryTime)
				mw.singleflight.Do(ForceUpdateRWMP, func() (interface{}, error) {
					mw.triggerAndWaitForceUpdate()
					return nil, nil
				})
				goto get_rwmp
			} else if status == statusNoSpace || status == statusForbid {
				log.LogErrorf("Create_ll status %v", status)
				return nil, statusToErrno(status)
			}
		}
	} else {
		for i := 0; i < length; i++ {
			index := (int(epoch) + i) % length
			mp = rwPartitions[index]
			status, info, err = mw.icreate(mp, mode, uid, gid, target, fullPath)
			if err == nil && status == statusOK {
				goto create_dentry
			} else if status == statusFull {
				if retryTime >= InodeFullMaxRetryTime {
					break
				}
				retryTime++
				log.LogWarnf("Mp(%v) inode is full, trigger rwmp get and retry(%v)", mp, retryTime)
				mw.singleflight.Do(ForceUpdateRWMP, func() (interface{}, error) {
					mw.triggerAndWaitForceUpdate()
					return nil, nil
				})
				goto get_rwmp
			} else if status == statusNoSpace || status == statusForbid {
				log.LogErrorf("Create_ll status %v", status)
				return nil, statusToErrno(status)
			}
		}
	}
	return nil, syscall.ENOMEM
create_dentry:
	log.LogDebugf("Create_ll name %v ino %v", name, info.Inode)
	if mw.EnableQuota {
		status, err = mw.quotaDcreate(parentMP, parentID, name, info.Inode, mode, quotaIds, fullPath, ignoreExist)
	} else {
		status, err = mw.dcreate(parentMP, parentID, name, info.Inode, mode, fullPath, ignoreExist)
	}
	if err != nil {
		if status == statusOpDirQuota || status == statusNoSpace {
			mw.iunlink(mp, info.Inode, mw.Client.GetLatestVer(), 0, fullPath)
			mw.ievict(mp, info.Inode, fullPath)
		}
		return nil, statusToErrno(status)
	} else if status != statusOK {
		mw.iunlink(mp, info.Inode, mw.Client.GetLatestVer(), 0, fullPath)
		mw.ievict(mp, info.Inode, fullPath)
		return nil, statusToErrno(status)
	}
	if mw.EnableSummary {
		var filesHddInc, filesSsdInc, filesBlobStoreInc, dirsInc int64
		if proto.IsDir(mode) {
			dirsInc = 1
		} else {
			if info.StorageClass == proto.StorageClass_Replica_HDD {
				filesHddInc = 1
			}
			if info.StorageClass == proto.StorageClass_Replica_SSD {
				filesSsdInc = 1
			}
			if info.StorageClass == proto.StorageClass_BlobStore {
				filesBlobStoreInc = 1
			}
		}
		go mw.UpdateSummary_ll(parentID, filesHddInc, filesSsdInc, filesBlobStoreInc, 0, 0, 0, dirsInc)
	}
	return info, nil
}

func (mw *MetaWrapper) Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Lookup_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return 0, 0, syscall.ENOENT
	}

	status, inode, mode, err := mw.lookup(parentMP, parentID, name, mw.VerReadSeq)
	if err != nil || status != statusOK {
		return 0, 0, statusToErrno(status)
	}
	// only save dir
	if proto.IsDir(mode) {
		mw.AddInoInfoCache(inode, parentID, name)
	}
	return inode, mode, nil
}

func (mw *MetaWrapper) BatchGetExpiredMultipart(prefix string, days int) (expiredIds []*proto.ExpiredMultipartInfo, err error) {
	partitions := mw.partitions
	var mp *MetaPartition
	wg := new(sync.WaitGroup)
	var resultMu sync.Mutex
	log.LogDebugf("BatchGetExpiredMultipart: mp num(%v) prefix(%v) days(%v)", len(partitions), prefix, days)
	for _, mp = range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			status, infos, err := mw.getExpiredMultipart(prefix, days, mp)
			if err == nil && status == statusOK {
				resultMu.Lock()
				expiredIds = append(expiredIds, infos...)
				resultMu.Unlock()
			}
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("batchGetExpiredMultipart: get expired multipart fail: partitionId(%v)",
					mp.PartitionID)
			}
		}(mp)
	}
	wg.Wait()

	resultMu.Lock()
	defer resultMu.Unlock()
	if len(expiredIds) == 0 {
		err = syscall.ENOENT
		return
	}
	return
}

func (mw *MetaWrapper) InodeGet_ll(inode uint64) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeGet_ll: No such partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.iget(mp, inode, mw.VerReadSeq)
	if err != nil || status != statusOK {
		if status == statusNoent {
			// For NOENT error, pull the latest mp and give it another try,
			// in case the mp view is outdated.
			mw.triggerAndWaitForceUpdate()
			return mw.doInodeGet(inode)
		}
		return nil, statusToErrno(status)
	}
	if mw.EnableQuota {
		if len(info.QuotaInfos) != 0 && proto.IsDir(info.Mode) {
			var qinfo QuotaCacheInfo
			qinfo.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
			qinfo.quotaInfos = info.QuotaInfos
			qinfo.inode = inode
			mw.qc.Put(inode, &qinfo)
		}
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

	status, info, err := mw.iget(mp, inode, mw.VerReadSeq)
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
func (mw *MetaWrapper) InodeDelete_ll(inode uint64, fullPath string) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeDelete: No such partition, ino(%v)", inode)
		return syscall.ENOENT
	}
	status, err := mw.idelete(mp, inode, fullPath)
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
		mp := mw.getPartitionByInode(ino)
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

	xattrs := make([]*proto.XAttrInfo, 0, len(inodes))
	for {
		info := <-xattrsCh
		if info == nil {
			break
		}
		xattrs = append(xattrs, info)
	}
	return xattrs, nil
}

func (mw *MetaWrapper) shouldNotMoveToTrash(parentMP *MetaPartition, parentIno uint64, entry string, isDir bool) (error, bool) {
	log.LogDebugf("action[shouldNotMoveToTrash]: parentIno(%v) entry(%v)", parentIno, entry)

	status, inode, mode, err := mw.lookup(parentMP, parentIno, entry, mw.LastVerSeq)
	if err != nil || status != statusOK {
		return statusToErrno(status), false
	}
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("shouldNotMoveToTrash: No inode partition, parentID(%v) name(%v) ino(%v)", parentIno, entry, inode)
		return syscall.EAGAIN, false
	}
	status, info, err := mw.iget(mp, inode, mw.LastVerSeq)
	if err != nil || status != statusOK {
		return statusToErrno(status), false
	}

	if isDir {
		if !proto.IsDir(mode) {
			return syscall.EINVAL, false
		}
		if info == nil || info.Nlink > 2 {
			return syscall.ENOTEMPTY, false
		}
		if mw.EnableQuota {
			quotaInfos, err := mw.GetInodeQuota_ll(inode)
			if err != nil {
				log.LogErrorf("get inode [%v] quota failed [%v]", inode, err)
				return syscall.ENOENT, false
			}
			for _, info := range quotaInfos {
				if info.RootInode {
					log.LogErrorf("can not remove quota Root inode equal inode [%v]", inode)
					return syscall.EACCES, false
				}
			}
			mw.qc.Delete(inode)
		}
	}
	if mw.volDeleteLockTime > 0 {
		if ok, err := mw.canDeleteInode(mp, info, inode); !ok {
			return err, false
		}
	}
	// remove .Trash directly
	if mw.trashPolicy.IsTrashRoot(parentIno, entry) {
		return syscall.EOPNOTSUPP, false
	}
	// check if is sub dir of .Trash
	// get parent path to mount sub
	currentPath := mw.getCurrentPathToMountSub(parentIno)
	if strings.Contains(currentPath, UnknownPath) {
		return nil, true
	}
	// log.LogDebugf("action[shouldNotMoveToTrash]: parentIno(%v) entry(%v) currentPath(%v)", parentIno, entry, currentPath)

	subs := strings.Split(currentPath, "/")
	if len(subs) == 1 {
		// should never happen: file in root trash
		if subs[0] == TrashPrefix {
			log.LogDebugf("action[shouldNotMoveToTrash]: currentPath(%v) should not remove ", subs[0])
			return nil, true
		}
	} else {
		if subs[0] == TrashPrefix && strings.HasPrefix(subs[1], ExpiredPrefix) {
			log.LogDebugf("action[shouldNotMoveToTrash]: currentPath(%v)is expired ", currentPath)
			return nil, true
		}
		// should never happen: dir in root trash
		if subs[0] == TrashPrefix {
			log.LogDebugf("action[shouldMoveToTrash]: currentPath(%v) not legal ", currentPath)
			return nil, true
		}
	}
	// log.LogDebugf("action[shouldMoveToTrash]: currentPath(%v) is not expired ", currentPath)
	return nil, false
}

/*
 * Note that the return value of InodeInfo might be nil without error,
 * and the caller should make sure InodeInfo is valid before using it.
 */
func (mw *MetaWrapper) Delete_ll(parentID uint64, name string, isDir bool, fullPath string) (*proto.InodeInfo, error) {
	if mw.enableTx(proto.TxOpMaskRemove) {
		return mw.txDelete_ll(parentID, name, isDir, fullPath)
	} else {
		return mw.Delete_ll_EX(parentID, name, isDir, 0, fullPath)
	}
}

func (mw *MetaWrapper) DeleteWithCond_ll(parentID, cond uint64, name string, isDir bool, fullPath string) (*proto.InodeInfo, error) {
	return mw.deletewithcond_ll(parentID, cond, name, isDir, fullPath)
}

func (mw *MetaWrapper) Delete_Ver_ll(parentID uint64, name string, isDir bool, verSeq uint64, fullPath string) (*proto.InodeInfo, error) {
	if verSeq == 0 {
		verSeq = math.MaxUint64
	}
	log.LogDebugf("Delete_Ver_ll.parentId %v name %v isDir %v verSeq %v", parentID, name, isDir, verSeq)
	return mw.Delete_ll_EX(parentID, name, isDir, verSeq, fullPath)
}

func (mw *MetaWrapper) txDelete_ll(parentID uint64, name string, isDir bool, fullPath string) (info *proto.InodeInfo, err error) {
	var (
		status int
		inode  uint64
		mode   uint32
		mp     *MetaPartition
	)
	start := time.Now()
	defer func() {
		log.LogDebugf("Delete_ll: consume %v", time.Since(start).Seconds())
	}()

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("txDelete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	if !mw.disableTrash && !mw.disableTrashByClient {
		if mw.trashPolicy == nil {
			log.LogDebugf("TRACE Remove:TrashPolicy is nil")
			mw.enableTrash()
		}
		// cannot delete .Trash
		err, ret := mw.shouldNotMoveToTrash(parentMP, parentID, name, isDir)
		if err != nil {
			log.LogErrorf("Delete_ll: shouldNotMoveToTrash failed:%v", err)
			return nil, err
		}
		if !ret {
			parentPathAbsolute := mw.getCurrentPath(parentID)
			err = mw.trashPolicy.MoveToTrash(parentPathAbsolute, parentID, name, isDir)
			if err != nil {
				log.LogErrorf("Delete_ll: MoveToTrash name %v  failed %v", name, err)
			}
			return nil, err
		}

	}

	var tx *Transaction
	defer func() {
		if tx != nil {
			err = tx.OnDone(err, mw)
		}
	}()

	status, inode, mode, err = mw.lookup(parentMP, parentID, name, mw.LastVerSeq)
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
		var newSt int
		var newErr error

		newSt, _, newErr = mw.txDdelete(tx, parentMP, parentID, inode, name, fullPath)
		return newSt, newErr
	})

	funcs = append(funcs, func() (int, error) {
		var newSt int
		var newErr error

		newSt, info, newErr = mw.txIunlink(tx, mp, inode, fullPath)
		return newSt, newErr
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
		// go func() {
		if proto.IsDir(mode) {
			job = func() {
				mw.UpdateSummary_ll(parentID, 0, 0, 0, 0, 0, 0, -1)
			}
		} else {
			job = func() {
				if info.StorageClass == proto.StorageClass_Replica_HDD {
					mw.UpdateSummary_ll(parentID, -1, 0, 0, -int64(info.Size), 0, 0, 0)
				}
				if info.StorageClass == proto.StorageClass_Replica_SSD {
					mw.UpdateSummary_ll(parentID, 0, -1, 0, 0, -int64(info.Size), 0, 0)
				}
				if info.StorageClass == proto.StorageClass_BlobStore {
					mw.UpdateSummary_ll(parentID, 0, 0, -1, 0, 0, -int64(info.Size), 0)
				}
			}
		}
		tx.SetOnCommit(job)
	}
	// clear trash cache
	if mw.trashPolicy != nil && !mw.disableTrash {
		mw.trashPolicy.CleanTrashPatchCache(mw.getCurrentPath(parentID), name)
	}
	return info, preErr
}

/*
 * Note that the return value of InodeInfo might be nil without error,
 * and the caller should make sure InodeInfo is valid before using it.
 */

func (mw *MetaWrapper) Delete_ll_EX(parentID uint64, name string, isDir bool, verSeq uint64, fullPath string) (*proto.InodeInfo, error) {
	var (
		status          int
		inode           uint64
		mode            uint32
		err             error
		info            *proto.InodeInfo
		mp              *MetaPartition
		inodeCreateTime int64
		denVer          uint64
	)

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("delete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	if !mw.disableTrash && !mw.disableTrashByClient {
		if mw.trashPolicy == nil {
			log.LogDebugf("TRACE Remove:TrashPolicy is nil")
			mw.enableTrash()
		}
		// cannot delete .Trash
		err, ret := mw.shouldNotMoveToTrash(parentMP, parentID, name, isDir)
		if err != nil {
			log.LogErrorf("Delete_ll: shouldNotMoveToTrash name %v failed %v", name, err)
			return nil, err
		}
		if !ret {
			parentPathAbsolute := mw.getCurrentPath(parentID)
			err = mw.trashPolicy.MoveToTrash(parentPathAbsolute, parentID, name, isDir)
			if err != nil {
				log.LogErrorf("Delete_ll: MoveToTrash name %v  failed %v", name, err)
			}

			return nil, err
		}

	}

	if isDir {
		status, inode, mode, err = mw.lookup(parentMP, parentID, name, verSeq)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		if !proto.IsDir(mode) {
			return nil, syscall.EINVAL
		}

		if verSeq == 0 {
			mp = mw.getPartitionByInode(inode)
			if mp == nil {
				log.LogErrorf("Delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
				return nil, syscall.EAGAIN
			}
			status, info, err = mw.iget(mp, inode, verSeq)
			if err != nil || status != statusOK {
				return nil, statusToErrno(status)
			}
			if info == nil || info.Nlink > 2 {
				return nil, syscall.ENOTEMPTY
			}
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
			mw.qc.Delete(inode)
		}
		if mw.volDeleteLockTime > 0 {
			inodeCreateTime = info.CreateTime.Unix()
			if ok, err := mw.canDeleteInode(mp, info, inode); !ok {
				return nil, err
			}
		}
	} else {
		if mw.volDeleteLockTime > 0 {
			status, inode, _, err = mw.lookup(parentMP, parentID, name, verSeq)
			if err != nil || status != statusOK {
				return nil, statusToErrno(status)
			}
			mp = mw.getPartitionByInode(inode)
			if mp == nil {
				log.LogErrorf("delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
				return nil, syscall.EAGAIN
			}
			status, info, err = mw.iget(mp, inode, verSeq)
			if err != nil || status != statusOK {
				return nil, statusToErrno(status)
			}
			inodeCreateTime = info.CreateTime.Unix()
			if ok, err := mw.canDeleteInode(mp, info, inode); !ok {
				return nil, err
			}
		}
	}

	log.LogDebugf("action[Delete_ll] parentID %v name %v verSeq %v", parentID, name, verSeq)

	status, inode, _, err = mw.ddelete(parentMP, parentID, name, inodeCreateTime, verSeq, fullPath)
	if err != nil || status != statusOK {
		if status == statusNoent {
			log.LogDebugf("action[Delete_ll] parentID %v name %v verSeq %v", parentID, name, verSeq)
			return nil, nil
		}
		log.LogDebugf("action[Delete_ll] parentID %v name %v verSeq %v", parentID, name, verSeq)
		return nil, statusToErrno(status)
	}
	log.LogDebugf("action[Delete_ll] parentID %v name %v verSeq %v", parentID, name, verSeq)
	// dentry is deleted successfully but inode is not, still returns success.
	mp = mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
		return nil, nil
	}
	log.LogDebugf("action[Delete_ll] parentID %v name %v verSeq %v", parentID, name, verSeq)
	status, info, err = mw.iunlink(mp, inode, verSeq, denVer, fullPath)
	if err != nil || status != statusOK {
		log.LogDebugf("action[Delete_ll] parentID %v inode %v name %v verSeq %v err %v", parentID, inode, name, verSeq, err)
		return nil, nil
	}

	if verSeq == 0 && mw.EnableSummary {
		go func() {
			if proto.IsDir(mode) {
				mw.UpdateSummary_ll(parentID, 0, 0, 0, 0, 0, 0, -1)
			} else {
				if info.StorageClass == proto.StorageClass_Replica_HDD {
					mw.UpdateSummary_ll(parentID, -1, 0, 0, -int64(info.Size), 0, 0, 0)
				}
				if info.StorageClass == proto.StorageClass_Replica_SSD {
					mw.UpdateSummary_ll(parentID, 0, -1, 0, 0, -int64(info.Size), 0, 0)
				}
				if info.StorageClass == proto.StorageClass_BlobStore {
					mw.UpdateSummary_ll(parentID, 0, 0, -1, 0, 0, -int64(info.Size), 0)
				}
			}
		}()
	}

	return info, nil
}

func isObjectLocked(mw *MetaWrapper, inode uint64, name string) error {
	xattrInfo, err := mw.XAttrGet_ll(inode, "oss:lock")
	if err != nil {
		log.LogErrorf("isObjectLocked: check ObjectLock err(%v) name(%v)", err, name)
		return err
	}
	retainUntilDate := xattrInfo.Get("oss:lock")
	if len(retainUntilDate) > 0 {
		retainUntilDateInt64, err := strconv.ParseInt(string(retainUntilDate), 10, 64)
		if err != nil {
			return err
		}
		if retainUntilDateInt64 > time.Now().UnixNano() {
			log.LogWarnf("isObjectLocked: object is locked, retainUntilDate(%v) name(%v)", retainUntilDateInt64, name)
			return errors.New("Access Denied")
		}
	}
	return nil
}

func (mw *MetaWrapper) deletewithcond_ll(parentID, cond uint64, name string, isDir bool, fullPath string) (*proto.InodeInfo, error) {
	err := isObjectLocked(mw, cond, name)
	if err != nil {
		return nil, err
	}

	var (
		status int
		inode  uint64
		mode   uint32
		info   *proto.InodeInfo
		mp     *MetaPartition
	)

	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("delete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	if isDir {
		status, inode, mode, err = mw.lookup(parentMP, parentID, name, mw.LastVerSeq)
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
		status, info, err = mw.iget(mp, inode, mw.VerReadSeq)
		if err != nil || status != statusOK {
			return nil, statusToErrno(status)
		}
		if info == nil || info.Nlink > 2 {
			return nil, syscall.ENOTEMPTY
		}
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

	dentry := []proto.Dentry{
		{
			Name:  name,
			Inode: cond,
			Type:  mode,
		},
	}
	status, resp, err := mw.ddeletes(parentMP, parentID, dentry, []string{fullPath})
	if err != nil || status != statusOK {
		if status == statusNoent {
			return nil, nil
		}
		return nil, statusToErrno(status)
	}
	status = parseStatus(resp.Items[0].Status)
	if status != statusOK {
		if status == statusNoent {
			return nil, nil
		}
		return nil, statusToErrno(status)
	}

	mp = mw.getPartitionByInode(resp.Items[0].Inode)
	if mp == nil {
		log.LogErrorf("delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
		return nil, nil
	}

	status, info, err = mw.iunlink(mp, resp.Items[0].Inode, 0, 0, fullPath)
	if err != nil || status != statusOK {
		return nil, nil
	}
	log.LogDebugf("delete_ll name %v ino %v", name, info.Inode)
	if mw.EnableSummary {
		go func() {
			if proto.IsDir(mode) {
				mw.UpdateSummary_ll(parentID, 0, 0, 0, 0, 0, 0, -1)
			} else {
				if info.StorageClass == proto.StorageClass_Replica_HDD {
					mw.UpdateSummary_ll(parentID, -1, 0, 0, -int64(info.Size), 0, 0, 0)
				}
				if info.StorageClass == proto.StorageClass_Replica_SSD {
					mw.UpdateSummary_ll(parentID, 0, -1, 0, 0, -int64(info.Size), 0, 0)
				}
				if info.StorageClass == proto.StorageClass_BlobStore {
					mw.UpdateSummary_ll(parentID, 0, 0, -1, 0, 0, -int64(info.Size), 0)
				}
			}
		}()
	}
	// clear trash cache
	if mw.trashPolicy != nil && !mw.disableTrash {
		mw.trashPolicy.CleanTrashPatchCache(mw.getCurrentPath(parentID), name)
	}

	return info, nil
}

func (mw *MetaWrapper) Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, srcFullPath string, dstFullPath string, overwritten bool) (err error) {
	if mw.enableTx(proto.TxOpMaskRename) {
		return mw.txRename_ll(srcParentID, srcName, dstParentID, dstName, srcFullPath, dstFullPath, overwritten)
	} else {
		return mw.rename_ll(srcParentID, srcName, dstParentID, dstName, srcFullPath, dstFullPath, overwritten)
	}
}

func (mw *MetaWrapper) txRename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, srcFullPath string, dstFullPath string, overwritten bool) (err error) {
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
	status, srcInode, srcMode, err := mw.lookup(srcParentMP, srcParentID, srcName, mw.LastVerSeq)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}

	tx, err = NewRenameTransaction(srcParentMP, srcParentID, srcName, dstParentMP, dstParentID, dstName, mw.TxTimeout)
	if err != nil {
		return syscall.EAGAIN
	}

	funcs := make([]func() (int, error), 0)

	status, dstInode, dstMode, err := mw.lookup(dstParentMP, dstParentID, dstName, mw.LastVerSeq)
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
			var newSt int
			var newErr error
			newSt, _, newErr = mw.txDupdate(tx, dstParentMP, dstParentID, dstName, srcInode, dstInode, dstFullPath)
			return newSt, newErr
		})

		funcs = append(funcs, func() (int, error) {
			var newSt int
			var newErr error
			newSt, _, newErr = mw.txIunlink(tx, oldInodeMP, dstInode, dstFullPath)
			if newSt == statusNoent {
				return statusOK, nil
			}
			return newSt, newErr
		})

		if log.EnableDebug() {
			log.LogDebugf("txRename_ll: tx(%v), pid:%v, name:%v, old(ino:%v) is replaced by src(new ino:%v)",
				tx.txInfo, dstParentID, dstName, dstInode, srcInode)
		}
	} else if status == statusNoent {
		funcs = append(funcs, func() (int, error) {
			var newSt int
			var newErr error
			newSt, newErr = mw.txDcreate(tx, dstParentMP, dstParentID, dstName, srcInode, srcMode, []uint32{}, dstFullPath, false)
			return newSt, newErr
		})
	} else {
		return statusToErrno(status)
	}

	// var inode uint64
	funcs = append(funcs, func() (int, error) {
		var newSt int
		var newErr error
		newSt, _, newErr = mw.txDdelete(tx, srcParentMP, srcParentID, srcInode, srcName, srcFullPath)
		return newSt, newErr
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

	// update summary
	var job func()
	if mw.EnableSummary {
		var srcInodeInfo *proto.InodeInfo
		var dstInodeInfo *proto.InodeInfo

		srcInodeInfo, _ = mw.InodeGet_ll(srcInode)
		if dstInode != 0 {
			dstInodeInfo, _ = mw.InodeGet_ll(dstInode)
			sizeInc := srcInodeInfo.Size - dstInodeInfo.Size
			job = func() {
				if dstInodeInfo.StorageClass == proto.StorageClass_Replica_HDD {
					mw.UpdateSummary_ll(srcParentID, -1, 0, 0, -int64(srcInodeInfo.Size), 0, 0, 0)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, int64(sizeInc), 0, 0, 0)
				}
				if dstInodeInfo.StorageClass == proto.StorageClass_Replica_SSD {
					mw.UpdateSummary_ll(srcParentID, -1, 0, 0, 0, -int64(srcInodeInfo.Size), 0, 0)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, 0, int64(sizeInc), 0, 0)
				}
				if dstInodeInfo.StorageClass == proto.StorageClass_BlobStore {
					mw.UpdateSummary_ll(srcParentID, -1, 0, 0, 0, 0, -int64(srcInodeInfo.Size), 0)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, 0, 0, int64(sizeInc), 0)
				}
			}
			tx.SetOnCommit(job)
			return
		} else {
			sizeInc := int64(srcInodeInfo.Size)
			if proto.IsRegular(srcMode) {
				log.LogDebugf("txRename_ll: update summary when file dentry is replaced")
				job = func() {
					if srcInodeInfo.StorageClass == proto.StorageClass_Replica_HDD {
						mw.UpdateSummary_ll(srcParentID, -1, 0, 0, -sizeInc, 0, 0, 0)
						mw.UpdateSummary_ll(dstParentID, 1, 0, 0, sizeInc, 0, 0, 0)
					}
					if srcInodeInfo.StorageClass == proto.StorageClass_Replica_SSD {
						mw.UpdateSummary_ll(srcParentID, 0, -1, 0, 0, -sizeInc, 0, 0)
						mw.UpdateSummary_ll(dstParentID, 0, 1, 0, 0, sizeInc, 0, 0)
					}
					if srcInodeInfo.StorageClass == proto.StorageClass_BlobStore {
						mw.UpdateSummary_ll(srcParentID, 0, 0, -1, 0, 0, -sizeInc, 0)
						mw.UpdateSummary_ll(dstParentID, 0, 0, 1, 0, 0, sizeInc, 0)
					}
				}
			} else {
				log.LogDebugf("txRename_ll: update summary when dir dentry is replaced")
				job = func() {
					mw.UpdateSummary_ll(srcParentID, 0, 0, 0, 0, 0, 0, -1)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, 0, 0, 0, 1)
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

func (mw *MetaWrapper) rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, srcFullPath string, dstFullPath string, overwritten bool) (err error) {
	var (
		oldInode   uint64
		lastVerSeq uint64
	)
	start := time.Now()
	defer func() {
		if err != nil {
			log.LogErrorf("Rename_ll: srcName %v srcFullPath %v dstName %v dstFullPath %v err %v",
				srcName, srcFullPath, dstName, dstFullPath, err)
		}
		log.LogDebugf("Rename_ll: consume %v", time.Since(start).Seconds())
	}()

	srcParentMP := mw.getPartitionByInode(srcParentID)
	if srcParentMP == nil {
		return syscall.ENOENT
	}
	dstParentMP := mw.getPartitionByInode(dstParentID)
	if dstParentMP == nil {
		return syscall.ENOENT
	}

	status, info, err := mw.iget(dstParentMP, dstParentID, mw.VerReadSeq)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}

	quota := atomic.LoadUint32(&mw.DirChildrenNumLimit)
	if info.Nlink >= quota {
		log.LogErrorf("Rename_ll: dst parent inode's nlink quota reached, parentID(%v) srcFullPath(%v)", dstParentID, srcFullPath)
		return syscall.EDQUOT
	}

	// look up for the src ino
	status, inode, mode, err := mw.lookup(srcParentMP, srcParentID, srcName, mw.VerReadSeq)
	if err != nil || status != statusOK {
		log.LogErrorf("Rename_ll: no such srcParentID %v srcFullPath(%v) failed %v", srcParentID, srcFullPath, err)
		return statusToErrno(status)
	}

	srcMP := mw.getPartitionByInode(inode)
	if srcMP == nil {
		return syscall.ENOENT
	}

	status, _, err = mw.ilink(srcMP, inode, srcFullPath)
	if err != nil || status != statusOK {
		log.LogErrorf("Rename_ll:ilink srcParentID %v srcFullPath(%v) failed %v ", srcParentID, srcFullPath, err)
		return statusToErrno(status)
	}

	// create dentry in dst parent
	status, err = mw.dcreate(dstParentMP, dstParentID, dstName, inode, mode, dstFullPath, false)
	if err != nil {
		if status == statusOpDirQuota {
			log.LogErrorf("Rename_ll:dcreate srcParentID %v srcFullPath(%v) failed %v ", srcParentID, srcFullPath, err)
			return statusToErrno(status)
		}
		return syscall.EAGAIN
	}
	var srcInodeInfo *proto.InodeInfo
	var dstInodeInfo *proto.InodeInfo
	srcInodeInfo, _ = mw.InodeGet_ll(inode)

	// Note that only regular files are allowed to be overwritten.
	if status == statusExist && (proto.IsSymlink(mode) || proto.IsRegular(mode)) {
		if !overwritten {
			return syscall.EEXIST
		}

		status, oldInode, err = mw.dupdate(dstParentMP, dstParentID, dstName, inode, dstFullPath)
		if err != nil {
			return syscall.EAGAIN
		}
		dstInodeInfo, _ = mw.InodeGet_ll(oldInode)
		// delete old cache
		mw.DeleteInoInfoCache(oldInode)
	}

	if status != statusOK {
		mw.iunlink(srcMP, inode, lastVerSeq, 0, srcFullPath)
		return statusToErrno(status)
	}
	var denVer uint64
	// delete dentry from src parent

	status, _, denVer, err = mw.ddelete(srcParentMP, srcParentID, srcName, 0, lastVerSeq, srcFullPath)

	if err != nil {
		log.LogErrorf("Rename_ll:ddelete srcParentID %v srcFullPath(%v) failed %v ", srcParentID, srcFullPath, err)
		return statusToErrno(status)
	} else if status != statusOK {
		var (
			sts int
			e   error
		)
		if oldInode == 0 {
			sts, inode, denVer, e = mw.ddelete(dstParentMP, dstParentID, dstName, 0, lastVerSeq, dstFullPath)
		} else {
			sts, denVer, e = mw.dupdate(dstParentMP, dstParentID, dstName, oldInode, dstFullPath)
		}
		if e == nil && sts == statusOK {
			mw.iunlink(srcMP, inode, lastVerSeq, denVer, srcFullPath)
		}
		log.LogErrorf("Rename_ll:#### srcParentID %v srcFullPath(%v) failed %v ", srcParentID, srcFullPath, err)
		return statusToErrno(status)
	}

	mw.iunlink(srcMP, inode, lastVerSeq, denVer, srcFullPath)

	if oldInode != 0 {
		// overwritten
		inodeMP := mw.getPartitionByInode(oldInode)
		if inodeMP != nil {
			mw.iunlink(inodeMP, oldInode, lastVerSeq, 0, dstFullPath)
			// evict oldInode to avoid oldInode becomes orphan inode
			mw.ievict(inodeMP, oldInode, dstFullPath)
		}
		if mw.EnableSummary {
			sizeInc := srcInodeInfo.Size - dstInodeInfo.Size
			go func() {
				if dstInodeInfo.StorageClass == proto.StorageClass_Replica_HDD {
					mw.UpdateSummary_ll(srcParentID, -1, 0, 0, -int64(srcInodeInfo.Size), 0, 0, 0)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, int64(sizeInc), 0, 0, 0)
				}
				if dstInodeInfo.StorageClass == proto.StorageClass_Replica_SSD {
					mw.UpdateSummary_ll(srcParentID, -1, 0, 0, 0, -int64(srcInodeInfo.Size), 0, 0)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, 0, int64(sizeInc), 0, 0)
				}
				if dstInodeInfo.StorageClass == proto.StorageClass_BlobStore {
					mw.UpdateSummary_ll(srcParentID, -1, 0, 0, 0, 0, -int64(srcInodeInfo.Size), 0)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, 0, 0, int64(sizeInc), 0)
				}
			}()
		}
	} else {
		if mw.EnableSummary {
			sizeInc := int64(srcInodeInfo.Size)
			if proto.IsRegular(mode) {
				// file
				go func() {
					if srcInodeInfo.StorageClass == proto.StorageClass_Replica_HDD {
						mw.UpdateSummary_ll(srcParentID, -1, 0, 0, -sizeInc, 0, 0, 0)
						mw.UpdateSummary_ll(dstParentID, 1, 0, 0, sizeInc, 0, 0, 0)
					}
					if srcInodeInfo.StorageClass == proto.StorageClass_Replica_SSD {
						mw.UpdateSummary_ll(srcParentID, 0, -1, 0, 0, -sizeInc, 0, 0)
						mw.UpdateSummary_ll(dstParentID, 0, 1, 0, 0, sizeInc, 0, 0)
					}
					if srcInodeInfo.StorageClass == proto.StorageClass_BlobStore {
						mw.UpdateSummary_ll(srcParentID, 0, 0, -1, 0, 0, -sizeInc, 0)
						mw.UpdateSummary_ll(dstParentID, 0, 0, 1, 0, 0, sizeInc, 0)
					}
				}()
			} else {
				// dir
				go func() {
					mw.UpdateSummary_ll(srcParentID, 0, 0, 0, 0, 0, 0, -1)
					mw.UpdateSummary_ll(dstParentID, 0, 0, 0, 0, 0, 0, 1)
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

	// log.LogDebugf("Rename_ll: dstInodeInfo %v", dstInodeInfo)
	// mw.AddInoInfoCache(dstInodeInfo.Inode, dstParentID, dstName)
	mw.DeleteInoInfoCache(srcInodeInfo.Inode)
	if proto.IsDir(srcInodeInfo.Mode) {
		mw.AddInoInfoCache(srcInodeInfo.Inode, dstParentID, dstName)
	}
	return nil
}

// Read all dentries with parentID
func (mw *MetaWrapper) ReadDir_ll(parentID uint64) ([]proto.Dentry, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, err := mw.readDir(parentMP, parentID)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return children, nil
}

// Read limit count dentries with parentID, start from string
func (mw *MetaWrapper) ReadDirLimitForSnapShotClean(parentID uint64, from string, limit uint64, verSeq uint64, idDir bool) ([]proto.Dentry, error) {
	if verSeq == 0 {
		verSeq = math.MaxUint64
	}
	log.LogDebugf("action[ReadDirLimit_ll] parentID %v from %v limit %v verSeq %v", parentID, from, limit, verSeq)
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}
	var opt uint8
	opt |= uint8(proto.FlagsSnapshotDel)
	if idDir {
		opt |= uint8(proto.FlagsSnapshotDelDir)
	}
	status, children, err := mw.readDirLimit(parentMP, parentID, from, limit, verSeq, opt)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	for _, den := range children {
		log.LogDebugf("ReadDirLimitForSnapShotClean. get dentry %v", den)
	}
	return children, nil
}

// Read limit count dentries with parentID, start from string
func (mw *MetaWrapper) ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error) {
	log.LogDebugf("action[ReadDirLimit_ll] parentID %v from %v limit %v", parentID, from, limit)
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, err := mw.readDirLimit(parentMP, parentID, from, limit, mw.VerReadSeq, 0)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	return children, nil
}

func (mw *MetaWrapper) DentryCreate_ll(parentID uint64, name string, inode uint64, mode uint32, fullPath string) error {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return syscall.ENOENT
	}
	var err error
	var status int
	if status, err = mw.dcreate(parentMP, parentID, name, inode, mode, fullPath, false); err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) DentryUpdate_ll(parentID uint64, name string, inode uint64, fullPath string) (oldInode uint64, err error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		err = syscall.ENOENT
		return
	}
	var status int
	status, oldInode, err = mw.dupdate(parentMP, parentID, name, inode, fullPath)
	if err != nil || status != statusOK {
		err = statusToErrno(status)
		return
	}
	return
}

func (mw *MetaWrapper) SplitExtentKey(parentInode, inode uint64, ek proto.ExtentKey, storageClass uint32) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}
	var oldInfo *proto.InodeInfo
	if mw.EnableSummary {
		oldInfo, _ = mw.InodeGet_ll(inode)
	}

	status, err := mw.appendExtentKey(mp, inode, ek, nil, true, false, storageClass, false)
	if err != nil || status != statusOK {
		log.LogErrorf("SplitExtentKey: inode(%v) ek(%v) err(%v) status(%v)", inode, ek, err, status)
		return statusToErrno(status)
	}
	log.LogDebugf("SplitExtentKey: ino(%v) ek(%v)", inode, ek)

	if mw.EnableSummary {
		go func() {
			newInfo, _ := mw.InodeGet_ll(inode)
			if oldInfo != nil && newInfo != nil {
				if int64(oldInfo.Size) < int64(newInfo.Size) {
					incSize := int64(newInfo.Size) - int64(oldInfo.Size)
					if newInfo.StorageClass == proto.StorageClass_Replica_HDD {
						mw.UpdateSummary_ll(parentInode, 0, 0, 0, incSize, 0, 0, 0)
					}
					if newInfo.StorageClass == proto.StorageClass_Replica_SSD {
						mw.UpdateSummary_ll(parentInode, 0, 0, 0, 0, incSize, 0, 0)
					}
					if newInfo.StorageClass == proto.StorageClass_BlobStore {
						mw.UpdateSummary_ll(parentInode, 0, 0, 0, 0, 0, incSize, 0)
					}
				}
			}
		}()
	}

	return nil
}

// Used as a callback by stream sdk
func (mw *MetaWrapper) AppendExtentKey(parentInode, inode uint64, ek proto.ExtentKey, discard []proto.ExtentKey,
	isCache bool, storageClass uint32, isMigration bool,
) (int, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return statusError, syscall.ENOENT
	}
	var oldInfo *proto.InodeInfo
	if mw.EnableSummary {
		oldInfo, _ = mw.InodeGet_ll(inode)
	}

	status, err := mw.appendExtentKey(mp, inode, ek, discard, false, isCache, storageClass, isMigration)
	if err != nil || status != statusOK {
		log.LogErrorf("MetaWrapper AppendExtentKey: inode(%v) ek(%v) local discard(%v) err(%v) status(%v)", inode, ek, discard, err, status)
		return status, statusToErrno(status)
	}
	log.LogDebugf("MetaWrapper AppendExtentKey: ino(%v) ek(%v) discard(%v)", inode, ek, discard)

	if mw.EnableSummary {
		go func() {
			newInfo, _ := mw.InodeGet_ll(inode)
			if oldInfo != nil && newInfo != nil {
				if int64(oldInfo.Size) < int64(newInfo.Size) {
					incSize := int64(newInfo.Size) - int64(oldInfo.Size)
					if newInfo.StorageClass == proto.StorageClass_Replica_HDD {
						mw.UpdateSummary_ll(parentInode, 0, 0, 0, incSize, 0, 0, 0)
					}
					if newInfo.StorageClass == proto.StorageClass_Replica_SSD {
						mw.UpdateSummary_ll(parentInode, 0, 0, 0, 0, incSize, 0, 0)
					}
					if newInfo.StorageClass == proto.StorageClass_BlobStore {
						mw.UpdateSummary_ll(parentInode, 0, 0, 0, 0, 0, incSize, 0)
					}
				}
			}
		}()
	}

	return statusOK, nil
}

// AppendExtentKeys append multiple extent key into specified inode with single request.
func (mw *MetaWrapper) AppendExtentKeys(inode uint64, eks []proto.ExtentKey, storageClass uint32) error {
	if storageClass != proto.MediaType_SSD && storageClass != proto.MediaType_HDD {
		return errors.New(fmt.Sprintf("Current storage class %v do not support AppendExtentKeys",
			storageClass))
	}
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.appendExtentKeys(mp, inode, eks, storageClass)
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

func (mw *MetaWrapper) GetExtents(inode uint64, isCache, openForWrite,
	isMigration bool,
) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return 0, 0, nil, syscall.ENOENT
	}

	resp, err := mw.getExtents(mp, inode, isCache, openForWrite, isMigration)
	if err != nil {
		if !strings.Contains(err.Error(), "OpMismatchStorageClass") {
			if resp != nil {
				err = statusToErrno(resp.Status)
			}
		}
		log.LogErrorf("GetExtents: ino(%v) err(%v)", inode, err)
		return 0, 0, nil, err
	}
	extents = resp.Extents
	gen = resp.Generation
	size = resp.Size

	// log.LogDebugf("GetObjExtents stack[%v]", string(debug.Stack()))
	if log.EnableDebug() {
		log.LogDebugf("GetExtents: ino(%v) gen(%v) size(%v) extents(%v)", inode, gen, size, extents)
	}
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

func (mw *MetaWrapper) Truncate(inode, size uint64, fullPath string) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Truncate: No inode partition, ino(%v)", inode)
		return syscall.ENOENT
	}

	status, err := mw.truncate(mp, inode, size, fullPath)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) Link(parentID uint64, name string, ino uint64, fullPath string) (*proto.InodeInfo, error) {
	// if mw.EnableTransaction {
	if mw.EnableTransaction&proto.TxOpMaskLink > 0 {
		return mw.txLink(parentID, name, ino, fullPath)
	} else {
		return mw.link(parentID, name, ino, fullPath)
	}
}

func (mw *MetaWrapper) txLink(parentID uint64, name string, ino uint64, fullPath string) (info *proto.InodeInfo, err error) {
	// var err error
	var status int
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("txLink: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
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
		var newSt int
		var newErr error
		newSt, info, newErr = mw.txIlink(tx, mp, ino, fullPath)
		return newSt, newErr
	})

	funcs = append(funcs, func() (int, error) {
		var newSt int
		var newErr error
		var quotaIds []uint32
		var ifo *proto.InodeInfo

		if mw.EnableQuota {
			quotaInfos, newErr := mw.getInodeQuota(parentMP, parentID)
			if newErr != nil {
				log.LogErrorf("link: get parent quota fail, parentID(%v) err(%v)", parentID, newErr)
				return statusError, syscall.ENOENT
			}

			for quotaId := range quotaInfos {
				quotaIds = append(quotaIds, quotaId)
			}
		}

		newSt, ifo, newErr = mw.iget(mp, ino, 0)
		if newErr != nil || newSt != statusOK {
			return newSt, newErr
		}

		newSt, newErr = mw.txDcreate(tx, parentMP, parentID, name, ino, ifo.Mode, quotaIds, fullPath, false)
		return newSt, newErr
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

func (mw *MetaWrapper) link(parentID uint64, name string, ino uint64, fullPath string) (*proto.InodeInfo, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Link: No parent partition, parentID(%v)", parentID)
		return nil, syscall.ENOENT
	}

	status, info, err := mw.iget(parentMP, parentID, mw.VerReadSeq)
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
	status, info, err = mw.ilink(mp, ino, fullPath)
	if err != nil || status != statusOK {
		return nil, statusToErrno(status)
	}
	if mw.EnableQuota {
		var quotaInfos map[uint32]*proto.MetaQuotaInfo
		quotaInfos, err = mw.getInodeQuota(parentMP, parentID)
		if err != nil {
			log.LogErrorf("link: get parent quota fail, parentID(%v) err(%v)", parentID, err)
			return nil, syscall.ENOENT
		}
		var quotaIds []uint32
		for quotaId := range quotaInfos {
			quotaIds = append(quotaIds, quotaId)
		}
		// create new dentry and refer to the inode
		status, err = mw.quotaDcreate(parentMP, parentID, name, ino, info.Mode, quotaIds, fullPath, false)
	} else {
		status, err = mw.dcreate(parentMP, parentID, name, ino, info.Mode, fullPath, false)
	}
	if err != nil {
		return nil, statusToErrno(status)
	} else if status != statusOK {
		mw.iunlink(mp, ino, mw.Client.GetLatestVer(), 0, fullPath)
		return nil, statusToErrno(status)
	}
	return info, nil
}

func (mw *MetaWrapper) Evict(inode uint64, fullPath string) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogWarnf("Evict: No such partition, ino(%v)", inode)
		return syscall.EINVAL
	}

	status, err := mw.ievict(mp, inode, fullPath)
	if err != nil || status != statusOK {
		log.LogWarnf("Evict: ino(%v) err(%v) status(%v)", inode, err, status)
		return statusToErrno(status)
	}
	mw.DeleteInoInfoCache(inode)
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

func (mw *MetaWrapper) InodeCreate_ll(parentID uint64, mode, uid, gid uint32, target []byte, quotaIds []uint64, fullPath string) (*proto.InodeInfo, error) {
	var (
		status       int
		err          error
		info         *proto.InodeInfo
		mp           *MetaPartition
		rwPartitions []*MetaPartition
	)

get_rwmp:
	rwPartitions = mw.getRWPartitions()
	length := len(rwPartitions)
	epoch := atomic.AddUint64(&mw.epoch, 1)
	retryTime := 0
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
			status, info, err = mw.quotaIcreate(mp, mode, uid, gid, target, quotaIds, fullPath)
			if err == nil && status == statusOK {
				return info, nil
			} else if status == statusFull {
				if retryTime >= InodeFullMaxRetryTime {
					break
				}
				retryTime++
				log.LogWarnf("Mp(%v) inode is full, trigger rwmp get and retry(%v)", mp, retryTime)
				mw.singleflight.Do(ForceUpdateRWMP, func() (interface{}, error) {
					mw.triggerAndWaitForceUpdate()
					return nil, nil
				})
				goto get_rwmp
			} else if status == statusNoSpace || status == statusForbid {
				log.LogErrorf("InodeCreate_ll status %v", status)
				return nil, statusToErrno(status)
			}
		}
	} else {
		for i := 0; i < length; i++ {
			index := (int(epoch) + i) % length
			mp = rwPartitions[index]
			status, info, err = mw.icreate(mp, mode, uid, gid, target, fullPath)
			if err == nil && status == statusOK {
				return info, nil
			} else if status == statusFull {
				if retryTime >= InodeFullMaxRetryTime {
					break
				}
				retryTime++
				log.LogWarnf("Mp(%v) inode is full, trigger rwmp get and retry(%v)", mp, retryTime)
				mw.singleflight.Do(ForceUpdateRWMP, func() (interface{}, error) {
					mw.triggerAndWaitForceUpdate()
					return nil, nil
				})
				goto get_rwmp
			} else if status == statusNoSpace || status == statusForbid {
				log.LogErrorf("InodeCreate_ll status %v", status)
				return nil, statusToErrno(status)
			}
		}
	}
	return nil, syscall.ENOMEM
}

// InodeUnlink_ll is a low-level api that makes specified inode link value +1.
func (mw *MetaWrapper) InodeLink_ll(inode uint64, fullPath string) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeLink_ll: No such partition, ino(%v)", inode)
		return nil, syscall.EINVAL
	}
	status, info, err := mw.ilink(mp, inode, fullPath)
	if err != nil || status != statusOK {
		log.LogErrorf("InodeLink_ll: ino(%v) err(%v) status(%v)", inode, err, status)
		return nil, statusToErrno(status)
	}
	return info, nil
}

// InodeUnlink_ll is a low-level api that makes specified inode link value -1.
func (mw *MetaWrapper) InodeUnlink_ll(inode uint64, fullPath string) (*proto.InodeInfo, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("InodeUnlink_ll: No such partition, ino(%v)", inode)
		return nil, syscall.EINVAL
	}
	var ver uint64
	if mw.Client != nil {
		ver = mw.Client.GetLatestVer()
	}
	status, info, err := mw.iunlink(mp, inode, ver, 0, fullPath)
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
	mp := mw.getPartitionByID(mpId)
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

func (mw *MetaWrapper) AddMultipartPart_ll(path, multipartId string, partId uint16, size uint64, md5 string,
	inodeInfo *proto.InodeInfo,
) (oldInode uint64, updated bool, err error) {
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
	mp := mw.getPartitionByID(mpId)
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
	mp := mw.getPartitionByID(mpId)
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
	var mp *MetaPartition
	wg := new(sync.WaitGroup)
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
	wg := sync.WaitGroup{}
	wl := sync.Mutex{}
	sessions := make([]*proto.MultipartInfo, 0)

	for _, mp := range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			status, response, err := mw.listMultiparts(mp, prefix, delimiter, keyMarker, multipartIdMarker, maxUploads+1)
			if err != nil || status != statusOK {
				log.LogErrorf("ListMultipart: partition list multipart fail, partitionID(%v) err(%v) status(%v)",
					mp.PartitionID, err, status)
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
	xAttrValues[name] = value

	xAttr := &proto.XAttrInfo{
		Inode:  inode,
		XAttrs: xAttrValues,
	}

	log.LogDebugf("XAttrGet_ll: get xattr: volume(%v) inode(%v) name(%v) value(%v)",
		mw.volname, inode, name, value)
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

func (mw *MetaWrapper) UpdateSummary_ll(parentIno uint64, filesHddInc int64, filesSsdInc int64, filesBlobStoreInc int64,
	bytesHddInc int64, bytesSsdInc int64, bytesBlobStoreInc int64, dirsInc int64,
) {
	if filesHddInc == 0 && filesSsdInc == 0 && filesBlobStoreInc == 0 && bytesHddInc == 0 && bytesSsdInc == 0 && bytesBlobStoreInc == 0 && dirsInc == 0 {
		return
	}
	mp := mw.getPartitionByInode(parentIno)
	if mp == nil {
		log.LogErrorf("UpdateSummary_ll: no such partition, inode(%v)", parentIno)
		return
	}
	for cnt := 0; cnt < UpdateSummaryRetry; cnt++ {
		err := mw.updateXAttrs(mp, parentIno, filesHddInc, filesSsdInc, filesBlobStoreInc,
			bytesHddInc, bytesSsdInc, bytesBlobStoreInc, dirsInc)
		if err == nil {
			return
		}
	}
}

func (mw *MetaWrapper) UpdateAccessFileInfo_ll(parentIno uint64, valueCountSsd string, valueSizeSsd string,
	valueCountHdd string, valueSizeHdd string, valueCountBlobStore string, valueSizeBlobStore string,
) {
	log.LogDebugf("UpdateAccessFileInfo_ll: ino(%v) valueCountSsd(%v) valueSizeSsd(%v) valueCountHdd(%v) valueSizeHdd(%v) valueCountBlobStore(%v) valueSizeBlobStore(%v)",
		parentIno, valueCountSsd, valueSizeSsd, valueCountHdd, valueSizeHdd, valueCountBlobStore, valueSizeBlobStore)

	attrs := make(map[string]string)
	attrs[AccessFileCountSsdKey] = valueCountSsd
	attrs[AccessFileSizeSsdKey] = valueSizeSsd
	attrs[AccessFileCountHddKey] = valueCountHdd
	attrs[AccessFileSizeHddKey] = valueSizeHdd
	attrs[AccessFileCountBlobStoreKey] = valueCountBlobStore
	attrs[AccessFileSizeBlobStoreKey] = valueSizeBlobStore

	for cnt := 0; cnt < UpdateAccessFileRetry; cnt++ {
		err := mw.BatchSetXAttr_ll(parentIno, attrs)
		if err == nil {
			return
		}
	}
}

func (mw *MetaWrapper) InodeAccessTimeGet(ino uint64) (accessTime time.Time, err error) {
	mp := mw.getPartitionByInode(ino)
	if mp == nil {
		log.LogErrorf("InodeAccessTimeGet: no such partition, inode(%v)", ino)
		return accessTime, errors.NewErrorf("no such partition, inode(%v)", ino)
	}
	status, info, err := mw.inodeAccessTimeGet(mp, ino)
	if err != nil || status != statusOK {
		return accessTime, errors.NewErrorf("inodeAccessTimeGet failed, err(%v) status(%v)", err, status)
	}
	accessTime = info.AccessTime
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
	Subdirs         int64
	FilesHdd        int64
	FilesSsd        int64
	FilesBlobStore  int64
	FbytesHdd       int64
	FbytesSsd       int64
	FbytesBlobStore int64
}

type AccessTimeConfig struct {
	Unit  string
	Split string
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
			return SummaryInfo{0, 0, 0, 0, 0, 0, 0}, err
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
				summaryInfo.FilesHdd = summaryInfo.FilesHdd + summary.FilesHdd
				summaryInfo.FilesSsd = summaryInfo.FilesSsd + summary.FilesSsd
				summaryInfo.FilesBlobStore = summaryInfo.FilesBlobStore + summary.FilesBlobStore
				summaryInfo.FbytesHdd = summaryInfo.FbytesHdd + summary.FbytesHdd
				summaryInfo.FilesSsd = summaryInfo.FilesSsd + summary.FilesSsd
				summaryInfo.FbytesBlobStore = summaryInfo.FbytesBlobStore + summary.FbytesBlobStore
				summaryInfo.Subdirs = summaryInfo.Subdirs + summary.Subdirs
			}
			close(errCh)
		}(&summaryInfo)
		for err := range errCh {
			return SummaryInfo{0, 0, 0, 0, 0, 0, 0}, err
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
				filesHdd, _ := strconv.ParseInt(summaryList[0], 10, 64)
				filesSsd, _ := strconv.ParseInt(summaryList[1], 10, 64)
				filesBlobStore, _ := strconv.ParseInt(summaryList[2], 10, 64)
				fbytesHdd, _ := strconv.ParseInt(summaryList[3], 10, 64)
				fbytesSsd, _ := strconv.ParseInt(summaryList[4], 10, 64)
				fbytesBlobStore, _ := strconv.ParseInt(summaryList[5], 10, 64)
				subdirs, _ := strconv.ParseInt(summaryList[6], 10, 64)

				summaryInfo.FilesHdd += filesHdd
				summaryInfo.FilesSsd += filesSsd
				summaryInfo.FilesBlobStore += filesBlobStore
				summaryInfo.FbytesHdd += fbytesHdd
				summaryInfo.FbytesSsd += fbytesSsd
				summaryInfo.FbytesBlobStore += fbytesBlobStore
				summaryInfo.Subdirs += subdirs
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
			filesHdd, _ := strconv.ParseInt(summaryList[0], 10, 64)
			filesSsd, _ := strconv.ParseInt(summaryList[1], 10, 64)
			filesBlobStore, _ := strconv.ParseInt(summaryList[2], 10, 64)
			fbytesHdd, _ := strconv.ParseInt(summaryList[3], 10, 64)
			fbytesSsd, _ := strconv.ParseInt(summaryList[4], 10, 64)
			fbytesBlobStore, _ := strconv.ParseInt(summaryList[5], 10, 64)
			subdirs, _ := strconv.ParseInt(summaryList[6], 10, 64)

			summaryInfo.FilesHdd += filesHdd
			summaryInfo.FilesSsd += filesSsd
			summaryInfo.FilesBlobStore += filesBlobStore
			summaryInfo.FbytesHdd += fbytesHdd
			summaryInfo.FbytesSsd += fbytesSsd
			summaryInfo.FbytesBlobStore += fbytesBlobStore
			summaryInfo.Subdirs += subdirs
		}
	}
	close(errch)
}

func (mw *MetaWrapper) getSummaryOrigin(parentIno uint64, summaryCh chan<- SummaryInfo, errCh chan<- error, wg *sync.WaitGroup, currentGoroutineNum *int32, newGoroutine bool, goroutineNum int32) {
	defer func() {
		if newGoroutine {
			atomic.AddInt32(currentGoroutineNum, -1)
			wg.Done()
		}
	}()
	var subdirsList []uint64
	retSummaryInfo := SummaryInfo{0, 0, 0, 0, 0, 0, 0}
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
			if fileInfo.StorageClass == proto.StorageClass_Replica_HDD {
				retSummaryInfo.FilesHdd += 1
				retSummaryInfo.FbytesHdd += int64(fileInfo.Size)
			}
			if fileInfo.StorageClass == proto.StorageClass_Replica_SSD {
				retSummaryInfo.FilesSsd += 1
				retSummaryInfo.FbytesSsd += int64(fileInfo.Size)
			}
			if fileInfo.StorageClass == proto.StorageClass_BlobStore {
				retSummaryInfo.FilesBlobStore += 1
				retSummaryInfo.FbytesBlobStore += int64(fileInfo.Size)
			}
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

func (mw *MetaWrapper) RefreshSummary_ll(parentIno uint64, goroutineNum int32, unit string, split string) error {
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

	accessFileInfo := AccessTimeConfig{Unit: unit, Split: split}
	log.LogDebugf("RefreshSummary_ll accessFileInfo(%v)", accessFileInfo)
	go mw.refreshSummary(parentIno, errch, &wg, &currentGoroutineNum, true, goroutineNum, accessFileInfo)
	go func() {
		wg.Wait()
		close(errch)
	}()
	for err := range errch {
		return err
	}
	return nil
}

func updateLocalSummary(inodeInfos []*proto.InodeInfo, splits []string, timeUnit string, newSummaryInfo *SummaryInfo,
	accessFileCountSsd []int32, accessFileSizeSsd []uint64, accessFileCountHdd []int32, accessFileSizeHdd []uint64,
	accessFileCountBlobStore []int32, accessFileSizeBlobStore []uint64,
) {
	currentTime := time.Now()
	for _, inodeInfo := range inodeInfos {
		if inodeInfo.StorageClass == proto.StorageClass_Replica_HDD {
			newSummaryInfo.FilesHdd += 1
			newSummaryInfo.FbytesHdd += int64(inodeInfo.Size)
		}
		if inodeInfo.StorageClass == proto.StorageClass_Replica_SSD {
			newSummaryInfo.FilesSsd += 1
			newSummaryInfo.FbytesSsd += int64(inodeInfo.Size)
		}
		if inodeInfo.StorageClass == proto.StorageClass_BlobStore {
			newSummaryInfo.FilesBlobStore += 1
			newSummaryInfo.FbytesBlobStore += int64(inodeInfo.Size)
		}
		accessTime := inodeInfo.AccessTime
		// log.LogDebugf("updateLocalSummary ino(%v) inode.AccessTime(%v)", inodeInfo.Inode, inodeInfo.AccessTime.Format("2006-01-02 15:04:05"))
		duration := currentTime.Sub(accessTime)
		for i, split := range splits {
			num, _ := strconv.ParseInt(split, 10, 64)
			var thresholdHours float64
			switch timeUnit {
			case "hour":
				thresholdHours = float64(num)
			case "day":
				thresholdHours = float64(24 * num)
			case "week":
				thresholdHours = float64(7 * 24 * num)
			case "month":
				thresholdHours = float64(30 * 24 * num)
			}

			if duration.Hours() < thresholdHours {
				if inodeInfo.StorageClass == proto.StorageClass_Replica_SSD {
					accessFileCountSsd[i]++
					accessFileSizeSsd[i] += inodeInfo.Size
				}
				if inodeInfo.StorageClass == proto.StorageClass_Replica_HDD {
					accessFileCountHdd[i]++
					accessFileSizeHdd[i] += inodeInfo.Size
				}
				if inodeInfo.StorageClass == proto.StorageClass_BlobStore {
					accessFileCountBlobStore[i]++
					accessFileSizeBlobStore[i] += inodeInfo.Size
				}
			}
		}
	}
}

func (mw *MetaWrapper) refreshSummary(parentIno uint64, errCh chan<- error, wg *sync.WaitGroup, currentGoroutineNum *int32, newGoroutine bool,
	goroutineNum int32, accessTimeCfg AccessTimeConfig,
) {
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
	oldSummaryInfo := SummaryInfo{0, 0, 0, 0, 0, 0, 0}
	if summaryXAttrInfo.XAttrs[SummaryKey] != "" {
		summaryList := strings.Split(summaryXAttrInfo.XAttrs[SummaryKey], ",")
		filesHdd, _ := strconv.ParseInt(summaryList[0], 10, 64)
		filesSsd, _ := strconv.ParseInt(summaryList[1], 10, 64)
		filesBlobStore, _ := strconv.ParseInt(summaryList[2], 10, 64)
		fbytesHdd, _ := strconv.ParseInt(summaryList[3], 10, 64)
		fbytesSsd, _ := strconv.ParseInt(summaryList[4], 10, 64)
		fbytesBlobStore, _ := strconv.ParseInt(summaryList[5], 10, 64)
		subdirs, _ := strconv.ParseInt(summaryList[6], 10, 64)
		oldSummaryInfo = SummaryInfo{
			FilesHdd:        filesHdd,
			FilesSsd:        filesSsd,
			FilesBlobStore:  filesBlobStore,
			FbytesHdd:       fbytesHdd,
			FbytesSsd:       fbytesSsd,
			FbytesBlobStore: fbytesBlobStore,
			Subdirs:         subdirs,
		}
	} else {
		oldSummaryInfo = SummaryInfo{0, 0, 0, 0, 0, 0, 0}
	}

	newSummaryInfo := SummaryInfo{0, 0, 0, 0, 0, 0, 0}

	splits := strings.Split(accessTimeCfg.Split, ",")
	accessFileCountSsd := make([]int32, len(splits))
	accessFileSizeSsd := make([]uint64, len(splits))
	accessFileCountHdd := make([]int32, len(splits))
	accessFileSizeHdd := make([]uint64, len(splits))
	accessFileCountBlobStore := make([]int32, len(splits))
	accessFileSizeBlobStore := make([]uint64, len(splits))

	noMore := false
	from := ""
	var children []proto.Dentry
	for !noMore {
		batches, err := mw.ReadDirLimit_ll(parentIno, from, DefaultReaddirLimit)
		if err != nil {
			log.LogErrorf("ReadDirLimit_ll: ino(%v) err(%v) from(%v)", parentIno, err, from)
			errCh <- err
			return
		}

		batchNr := uint64(len(batches))
		if batchNr == 0 || (from != "" && batchNr == 1) {
			break
		} else if batchNr < DefaultReaddirLimit {
			noMore = true
		}
		if from != "" {
			batches = batches[1:]
		}
		children = append(children, batches...)
		from = batches[len(batches)-1].Name
	}

	var inodeList []uint64
	var subdirsList []uint64
	for _, dentry := range children {
		if proto.IsDir(dentry.Type) {
			newSummaryInfo.Subdirs += 1
			subdirsList = append(subdirsList, dentry.Inode)
		} else {
			inodeList = append(inodeList, dentry.Inode)
			if len(inodeList) < BatchSize {
				continue
			}
			inodeInfos := mw.BatchInodeGet(inodeList)
			updateLocalSummary(inodeInfos, splits, accessTimeCfg.Unit, &newSummaryInfo, accessFileCountSsd, accessFileSizeSsd,
				accessFileCountHdd, accessFileSizeHdd, accessFileCountBlobStore, accessFileSizeBlobStore)
			inodeList = inodeList[0:0]
		}
	}
	if len(inodeList) > 0 {
		inodeInfos := mw.BatchInodeGet(inodeList)
		updateLocalSummary(inodeInfos, splits, accessTimeCfg.Unit, &newSummaryInfo, accessFileCountSsd, accessFileSizeSsd,
			accessFileCountHdd, accessFileSizeHdd, accessFileCountBlobStore, accessFileSizeBlobStore)
	}

	go mw.UpdateSummary_ll(
		parentIno,
		newSummaryInfo.FilesHdd-oldSummaryInfo.FilesHdd,
		newSummaryInfo.FilesSsd-oldSummaryInfo.FilesSsd,
		newSummaryInfo.FilesBlobStore-oldSummaryInfo.FilesBlobStore,
		newSummaryInfo.FbytesHdd-oldSummaryInfo.FbytesHdd,
		newSummaryInfo.FbytesSsd-oldSummaryInfo.FbytesSsd,
		newSummaryInfo.FbytesBlobStore-oldSummaryInfo.FbytesBlobStore,
		newSummaryInfo.Subdirs-oldSummaryInfo.Subdirs,
	)

	// access file info for ssd
	var resultCountSsd []string
	for _, count := range accessFileCountSsd {
		resultCountSsd = append(resultCountSsd, strconv.FormatInt(int64(count), 10))
	}
	// append total files count of ssd at last
	resultCountSsd = append(resultCountSsd, strconv.FormatInt(newSummaryInfo.FilesSsd, 10))
	valueCountSsd := strings.Join(resultCountSsd, ",")

	var resultSizeSsd []string
	for _, size := range accessFileSizeSsd {
		resultSizeSsd = append(resultSizeSsd, strconv.FormatUint(size, 10))
	}
	// append total bytes of ssd at last
	resultSizeSsd = append(resultSizeSsd, strconv.FormatInt(newSummaryInfo.FbytesSsd, 10))
	valueSizeSsd := strings.Join(resultSizeSsd, ",")

	// access file info for hdd
	var resultCountHdd []string
	for _, count := range accessFileCountHdd {
		resultCountHdd = append(resultCountHdd, strconv.FormatInt(int64(count), 10))
	}
	// append total files at last
	resultCountHdd = append(resultCountHdd, strconv.FormatInt(newSummaryInfo.FilesHdd, 10))
	valueCountHdd := strings.Join(resultCountHdd, ",")

	var resultSizeHdd []string
	for _, size := range accessFileSizeHdd {
		resultSizeHdd = append(resultSizeHdd, strconv.FormatUint(size, 10))
	}
	resultSizeHdd = append(resultSizeHdd, strconv.FormatInt(newSummaryInfo.FbytesHdd, 10))
	valueSizeHdd := strings.Join(resultSizeHdd, ",")

	// access file info for blobStore
	var resultCountBlobStore []string
	for _, count := range accessFileCountBlobStore {
		resultCountBlobStore = append(resultCountBlobStore, strconv.FormatInt(int64(count), 10))
	}
	// append total files at last
	resultCountBlobStore = append(resultCountBlobStore, strconv.FormatInt(newSummaryInfo.FilesBlobStore, 10))
	valueCountBlobStored := strings.Join(resultCountBlobStore, ",")

	var resultSizeBlobStore []string
	for _, size := range accessFileSizeBlobStore {
		resultSizeBlobStore = append(resultSizeBlobStore, strconv.FormatUint(size, 10))
	}
	resultSizeBlobStore = append(resultSizeBlobStore, strconv.FormatInt(newSummaryInfo.FbytesBlobStore, 10))
	valueSizeBlobStore := strings.Join(resultSizeBlobStore, ",")
	go mw.UpdateAccessFileInfo_ll(parentIno, valueCountSsd, valueSizeSsd, valueCountHdd, valueSizeHdd, valueCountBlobStored, valueSizeBlobStore)

	for _, subdirIno := range subdirsList {
		if atomic.LoadInt32(currentGoroutineNum) < goroutineNum {
			wg.Add(1)
			atomic.AddInt32(currentGoroutineNum, 1)
			go mw.refreshSummary(subdirIno, errCh, wg, currentGoroutineNum, true, goroutineNum, accessTimeCfg)
		} else {
			mw.refreshSummary(subdirIno, errCh, wg, currentGoroutineNum, false, goroutineNum, accessTimeCfg)
		}
	}
}

func (mw *MetaWrapper) BatchSetInodeQuota_ll(inodes []uint64, quotaId uint32, IsRoot bool) (ret map[uint64]uint8, err error) {
	batchInodeMap := make(map[uint64][]uint64)
	ret = make(map[uint64]uint8)
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
	ret = make(map[uint64]uint8)
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

const UnknownPath = "unknown"

func (mw *MetaWrapper) getCurrentPathToMountSub(parentIno uint64) (parentPath string) {
	currentPath := mw.getCurrentPath(parentIno)
	if strings.Contains(currentPath, UnknownPath) {
		return UnknownPath
	}
	log.LogDebugf("action[getCurrentPathToMountSub]: currentPath(%v)  parentIno(%v)", currentPath, parentIno)

	subs := strings.Split(currentPath, "/")
	index := 0
	for i, sub := range subs {
		if sub == mw.subDir {
			index = i
			break
		}
	}
	for i, sub := range subs {
		if i >= index {
			parentPath = path.Join(parentPath, sub)
		}
	}
	log.LogDebugf("action[getCurrentPathToMountSub]: parentPath(%v)", parentPath)
	return
}

func (mw *MetaWrapper) getCurrentPath(parentIno uint64) string {
	if parentIno == mw.rootIno {
		return "/"
	}

	mw.inoInfoLk.RLock()
	node, ok := mw.dirCache[parentIno]
	mw.inoInfoLk.RUnlock()
	if !ok {
		log.LogDebugf("action[getCurrentPath]: parentIno(%v)return UnknownPath", parentIno)
		return UnknownPath
	}
	return path.Join(mw.getCurrentPath(node.parentIno), node.name)
}

func (mw *MetaWrapper) AddInoInfoCache(ino, parentIno uint64, name string) {
	mw.inoInfoLk.Lock()
	defer mw.inoInfoLk.Unlock()
	mw.dirCache[ino] = dirInfoCache{ino: ino, parentIno: parentIno, name: name}

	log.LogDebugf("action[AddInoInfoCache]: ino(%v) cache(%v)", ino, mw.dirCache[ino])
}

func (mw *MetaWrapper) DeleteInoInfoCache(ino uint64) {
	mw.inoInfoLk.Lock()
	defer mw.inoInfoLk.Unlock()
	delete(mw.dirCache, ino)
}

func (mw *MetaWrapper) DisableTrashByClient(flag bool) {
	mw.disableTrashByClient = flag
}

func (mw *MetaWrapper) QueryTrashDisableByClient() bool {
	return mw.disableTrashByClient
}

func (mw *MetaWrapper) LockDir(ino uint64, lease uint64, lockId int64) (retLockId int64, err error) {
	mp := mw.getPartitionByInode(ino)
	if mp == nil {
		log.LogErrorf("LockDir: no such partition, ino(%v)", ino)
		err = syscall.ENOENT
		return
	}

	retLockId, err = mw.lockDir(mp, ino, lease, lockId)
	return
}

func (mw *MetaWrapper) GetStorageClass() uint32 {
	return atomic.LoadUint32(&mw.DefaultStorageClass)
}

func (mw *MetaWrapper) RenewalForbiddenMigration(inode uint64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("[RenewalForbiddenMigration]: No inode partition, ino(%v)", inode)
		return syscall.ENOENT
	}

	status, err := mw.renewalForbiddenMigration(mp, inode)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) UpdateExtentKeyAfterMigration(inode uint64, storageType uint32, objExtentKeys []proto.ObjExtentKey,
	leaseExpire uint64, delayDelMinute uint64, fullPath string,
) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		err := fmt.Errorf("not found mp by inode(%v)", inode)
		log.LogErrorf("UpdateExtentKeyAfterMigration: inode(%v) storageType(%v) extentKeys(%v) leaseExpire(%v) err: %v",
			inode, storageType, objExtentKeys, leaseExpire, err.Error())
		return err
	}
	status, err := mw.updateExtentKeyAfterMigration(mp, inode, storageType, objExtentKeys, leaseExpire, delayDelMinute, fullPath)
	if err != nil || status != statusOK {
		msg := fmt.Sprintf("UpdateExtentKeyAfterMigration: inode(%v) storageType(%v) extentKeys(%v) leaseExpire(%v) status(%v) err: %v",
			inode, storageType, objExtentKeys, leaseExpire, status, err)
		if status == statusLeaseOccupiedByOthers {
			log.LogWarn(msg)
			return fmt.Errorf("statusLeaseOccupiedByOthers: %v", err)
		}
		if status == statusLeaseGenerationNotMatch {
			log.LogWarn(msg)
			return fmt.Errorf("statusLeaseGenerationNotMatch: %v", err)
		}
		log.LogError(msg)
		return err
	}
	return nil
}

func (mw *MetaWrapper) DeleteMigrationExtentKey(inode uint64, fullPath string) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}
	status, err := mw.deleteMigrationExtentKey(mp, inode, fullPath)
	if err != nil || status != statusOK {
		log.LogErrorf("DeleteMigrationExtentKey: inode(%v)  err(%v) status(%v)",
			inode, err, status)
		return statusToErrno(status)
	}
	return nil
}

func (mw *MetaWrapper) ListVols(keywords string) (volsInfo []*proto.VolInfo, err error) {
	volsInfo, err = mw.mc.AdminAPI().ListVols(keywords)
	return
}

func (mw *MetaWrapper) ForbiddenMigration(inode uint64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("[ForbiddenMigration]: No inode partition, ino(%v)", inode)
		return syscall.ENOENT
	}

	status, err := mw.forbiddenMigration(mp, inode)
	if err != nil || status != statusOK {
		return statusToErrno(status)
	}
	return nil
}

type AccessFileInfo struct {
	Dir                      string
	AccessFileCountSsd       string
	AccessFileSizeSsd        string
	AccessFileCountHdd       string
	AccessFileSizeHdd        string
	AccessFileCountBlobStore string
	AccessFileSizeBlobStore  string
}

func (mw *MetaWrapper) GetAccessFileInfo(parentPath string, parentIno uint64, maxDepth int32, goroutineNum int32) (info []AccessFileInfo, err error) {
	if goroutineNum > MaxSummaryGoroutineNum {
		goroutineNum = MaxSummaryGoroutineNum
	}
	if goroutineNum <= 0 {
		goroutineNum = 1
	}
	var wg sync.WaitGroup
	var currentGoroutineNum int32 = 0
	var currentDepth int32 = 1
	errch := make(chan error)
	wg.Add(1)
	accessInfo := make([]AccessFileInfo, 0, 100)

	go mw.getDirAccessFileInfo(parentPath, parentIno, maxDepth, &currentDepth, &accessInfo, errch, &wg, &currentGoroutineNum, true, goroutineNum)
	go func() {
		wg.Wait()
		close(errch)
	}()
	for err := range errch {
		return nil, err
	}
	log.LogDebugf("GetAccessFileInfo finish, AccessFileInfo(%v)", accessInfo)
	return accessInfo, nil
}

func (mw *MetaWrapper) getDirAccessFileInfo(parentPath string, parentIno uint64, maxDepth int32, currentDepth *int32, info *[]AccessFileInfo,
	errCh chan<- error, wg *sync.WaitGroup, currentGoroutineNum *int32, newGoroutine bool, goroutineNum int32,
) {
	defer func() {
		if newGoroutine {
			atomic.AddInt32(currentGoroutineNum, -1)
			wg.Done()
		}
		log.LogDebugf("getDirAccessFileInfo finish, parentPath(%v) parentIno(%v) info:(%v)", parentPath, parentIno, info)
	}()

	log.LogDebugf("getDirAccessFileInfo: parentPath(%v) parentIno(%v) maxDepth(%v) currentDepth(%v)", parentPath, parentIno, maxDepth, *currentDepth)
	if *currentDepth > maxDepth {
		log.LogDebugf("getDirAccessFileInfo: reached max depth, parentPath(%v) parentIno(%v) maxDepth(%v) currentDepth(%v)", parentPath, parentIno, maxDepth, *currentDepth)
		return
	}
	// get file count of ssd, just for split count of file numbers
	xattrInfo, err := mw.XAttrGet_ll(parentIno, AccessFileCountSsdKey)
	if err != nil {
		log.LogErrorf("GetAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileCountSsdKey, parentIno, err)
		errCh <- err
		return
	}
	valueCountSsd := xattrInfo.Get(AccessFileCountSsdKey)
	log.LogDebugf("getDirAccessFileInfo: key(%v) value:(%v) len(%v)", AccessFileCountSsdKey, string(valueCountSsd), len(string(valueCountSsd)))
	if len(string(valueCountSsd)) == 0 {
		log.LogErrorf("getDirAccessFileInfo: XAttrGet_ll empty, key(%v) ino(%v) err(%v)", AccessFileCountSsdKey, parentIno, err)
		errCh <- err
		return
	}
	// get file size of ssd, just for split count of file capacity
	xattrInfo, err = mw.XAttrGet_ll(parentIno, AccessFileSizeSsdKey)
	if err != nil {
		log.LogErrorf("GetAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileSizeSsdKey, parentIno, err)
		errCh <- err
		return
	}
	valueSizeSsd := xattrInfo.Get(AccessFileSizeSsdKey)
	log.LogDebugf("getDirAccessFileInfo: key(%v) value:(%v) len(%v)", AccessFileSizeSsdKey, string(valueSizeSsd), len(string(valueSizeSsd)))
	if len(string(valueSizeSsd)) == 0 {
		log.LogErrorf("getDirAccessFileInfo: XAttrGet_ll empty, key(%v) ino(%v) err(%v)", AccessFileSizeSsdKey, parentIno, err)
		errCh <- err
		return
	}

	accessCountList := strings.Split(string(valueCountSsd), ",")
	accessSizeList := strings.Split(string(valueSizeSsd), ",")
	countLen := len(accessCountList)
	sizeLen := len(accessSizeList)

	accessCountSsd := make([]int64, countLen)
	accessSizeSsd := make([]uint64, sizeLen)
	accessCountHdd := make([]int64, countLen)
	accessSizeHdd := make([]uint64, sizeLen)
	accessCountBlobStore := make([]int64, countLen)
	accessSizeBlobStore := make([]uint64, sizeLen)

	mw.getAccessFileInfo(parentPath, parentIno, errCh, &accessCountSsd, &accessSizeSsd, &accessCountHdd, &accessSizeHdd, &accessCountBlobStore, &accessSizeBlobStore)

	var resultCountSsd []string
	for _, count := range accessCountSsd {
		resultCountSsd = append(resultCountSsd, strconv.FormatInt(int64(count), 10))
	}
	resultCountSsdStr := strings.Join(resultCountSsd, ",")

	var resultSizeSsd []string
	for _, size := range accessSizeSsd {
		resultSizeSsd = append(resultSizeSsd, strconv.FormatUint(size, 10))
	}
	resultSizeSsdStr := strings.Join(resultSizeSsd, ",")

	var resultCountHdd []string
	for _, count := range accessCountHdd {
		resultCountHdd = append(resultCountHdd, strconv.FormatInt(int64(count), 10))
	}
	resultCountHddStr := strings.Join(resultCountHdd, ",")

	var resultSizeHdd []string
	for _, size := range accessSizeHdd {
		resultSizeHdd = append(resultSizeHdd, strconv.FormatUint(size, 10))
	}
	resultSizeHddStr := strings.Join(resultSizeHdd, ",")

	var resultCountBlobStore []string
	for _, count := range accessCountBlobStore {
		resultCountBlobStore = append(resultCountBlobStore, strconv.FormatInt(int64(count), 10))
	}
	resultCountBlobStoreStr := strings.Join(resultCountBlobStore, ",")

	var resultSizeBlobStore []string
	for _, size := range accessSizeBlobStore {
		resultSizeBlobStore = append(resultSizeBlobStore, strconv.FormatUint(size, 10))
	}
	resultSizeBlobStoreStr := strings.Join(resultSizeBlobStore, ",")

	log.LogDebugf("getDirAccessFileInfo: parentPath(%v) parentIno(%v) accessCountSsd(%v) accessSizeSsd(%v) accessCountHdd(%v) accessSizeHdd(%v) accessCountBlobStore(%v) accessSizeBlobStore(%v)",
		parentPath, parentIno, accessCountSsd, accessSizeSsd, accessCountHdd, accessSizeHdd, accessCountBlobStore, accessSizeBlobStore)

	*info = append(*info, AccessFileInfo{
		parentPath, resultCountSsdStr, resultSizeSsdStr,
		resultCountHddStr, resultSizeHddStr,
		resultCountBlobStoreStr, resultSizeBlobStoreStr,
	})

	children, err := mw.ReadDir_ll(parentIno)
	if err != nil {
		errCh <- err
		return
	}
	for _, dentry := range children {
		if proto.IsDir(dentry.Type) {
			subPath := path.Join(parentPath, dentry.Name)
			newDepth := *currentDepth + 1
			if atomic.LoadInt32(currentGoroutineNum) < goroutineNum {
				wg.Add(1)
				atomic.AddInt32(currentGoroutineNum, 1)
				go mw.getDirAccessFileInfo(subPath, dentry.Inode, maxDepth, &newDepth, info, errCh, wg, currentGoroutineNum, true, goroutineNum)
			} else {
				mw.getDirAccessFileInfo(subPath, dentry.Inode, maxDepth, &newDepth, info, errCh, wg, currentGoroutineNum, false, goroutineNum)
			}
		}
	}
}

func (mw *MetaWrapper) getAccessFileInfo(parentPath string, parentIno uint64, errCh chan<- error, accessCountSsd *[]int64, accessSizeSsd *[]uint64,
	accessCountHdd *[]int64, accessSizeHdd *[]uint64, accessCountBlobStore *[]int64, accessSizeBlobStore *[]uint64,
) {
	log.LogDebugf("getAccessFileInfo: parentPath(%v) parentIno(%v)", parentPath, parentIno)
	xattrInfo, err := mw.XAttrGet_ll(parentIno, AccessFileCountSsdKey)
	if err != nil {
		log.LogErrorf("getAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileCountSsdKey, parentIno, err)
		errCh <- err
		return
	}
	value := xattrInfo.Get(AccessFileCountSsdKey)
	log.LogDebugf("getAccessFileInfo: key(%v) value(%v) len(%v)", AccessFileCountSsdKey, string(value), len(string(value)))
	if len(string(value)) > 0 {
		accessList := strings.Split(string(value), ",")
		for i := 0; i < len(accessList); i++ {
			val, _ := strconv.ParseInt(accessList[i], 10, 64)
			(*accessCountSsd)[i] += val
		}
	}

	xattrInfo, err = mw.XAttrGet_ll(parentIno, AccessFileSizeSsdKey)
	if err != nil {
		log.LogErrorf("getAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileSizeSsdKey, parentIno, err)
		errCh <- err
		return
	}
	value = xattrInfo.Get(AccessFileSizeSsdKey)
	log.LogDebugf("getAccessFileInfo: key(%v) value(%v) len(%v)", AccessFileSizeSsdKey, string(value), len(string(value)))
	if len(string(value)) > 0 {
		accessList := strings.Split(string(value), ",")
		for i := 0; i < len(accessList); i++ {
			val, _ := strconv.ParseUint(accessList[i], 10, 64)
			(*accessSizeSsd)[i] += val
		}
	}

	xattrInfo, err = mw.XAttrGet_ll(parentIno, AccessFileCountHddKey)
	if err != nil {
		log.LogErrorf("getAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileCountHddKey, parentIno, err)
		errCh <- err
		return
	}
	value = xattrInfo.Get(AccessFileCountHddKey)
	log.LogDebugf("getAccessFileInfo: key(%v) value(%v) len(%v)", AccessFileCountHddKey, string(value), len(string(value)))
	if len(string(value)) > 0 {
		accessList := strings.Split(string(value), ",")
		for i := 0; i < len(accessList); i++ {
			val, _ := strconv.ParseInt(accessList[i], 10, 64)
			(*accessCountHdd)[i] += val
		}
	}

	xattrInfo, err = mw.XAttrGet_ll(parentIno, AccessFileSizeHddKey)
	if err != nil {
		log.LogErrorf("getAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileSizeHddKey, parentIno, err)
		errCh <- err
		return
	}
	value = xattrInfo.Get(AccessFileSizeHddKey)
	log.LogDebugf("getAccessFileInfo: key(%v) value(%v) len(%v)", AccessFileSizeHddKey, string(value), len(string(value)))
	if len(string(value)) > 0 {
		accessList := strings.Split(string(value), ",")
		for i := 0; i < len(accessList); i++ {
			val, _ := strconv.ParseUint(accessList[i], 10, 64)
			(*accessSizeHdd)[i] += val
		}
	}

	xattrInfo, err = mw.XAttrGet_ll(parentIno, AccessFileCountBlobStoreKey)
	if err != nil {
		log.LogErrorf("getAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileCountBlobStoreKey, parentIno, err)
		errCh <- err
		return
	}
	value = xattrInfo.Get(AccessFileCountBlobStoreKey)
	log.LogDebugf("getAccessFileInfo: key(%v) value(%v) len(%v)", AccessFileCountBlobStoreKey, string(value), len(string(value)))
	if len(string(value)) > 0 {
		accessList := strings.Split(string(value), ",")
		for i := 0; i < len(accessList); i++ {
			val, _ := strconv.ParseInt(accessList[i], 10, 64)
			(*accessCountBlobStore)[i] += val
		}
	}

	xattrInfo, err = mw.XAttrGet_ll(parentIno, AccessFileSizeBlobStoreKey)
	if err != nil {
		log.LogErrorf("getAccessFileInfo: XAttrGet_ll failed, key(%v) ino(%v) err(%v)", AccessFileSizeBlobStoreKey, parentIno, err)
		errCh <- err
		return
	}
	value = xattrInfo.Get(AccessFileSizeBlobStoreKey)
	log.LogDebugf("getAccessFileInfo: key(%v) value(%v) len(%v)", AccessFileSizeBlobStoreKey, string(value), len(string(value)))
	if len(string(value)) > 0 {
		accessList := strings.Split(string(value), ",")
		for i := 0; i < len(accessList); i++ {
			val, _ := strconv.ParseUint(accessList[i], 10, 64)
			(*accessSizeBlobStore)[i] += val
		}
	}

	noMore := false
	from := ""
	var children []proto.Dentry
	for !noMore {
		batches, err := mw.ReadDirLimit_ll(parentIno, from, DefaultReaddirLimit)
		if err != nil {
			log.LogErrorf("ReadDirLimit_ll: ino(%v) err(%v) from(%v)", parentIno, err, from)
			errCh <- err
			return
		}

		batchNr := uint64(len(batches))
		if batchNr == 0 || (from != "" && batchNr == 1) {
			break
		} else if batchNr < DefaultReaddirLimit {
			noMore = true
		}
		if from != "" {
			batches = batches[1:]
		}
		children = append(children, batches...)
		from = batches[len(batches)-1].Name
	}

	for _, dentry := range children {
		if proto.IsDir(dentry.Type) {
			subPath := path.Join(parentPath, dentry.Name)
			mw.getAccessFileInfo(subPath, dentry.Inode, errCh, accessCountSsd, accessSizeSsd, accessCountHdd, accessSizeHdd, accessCountBlobStore, accessSizeBlobStore)
		}
	}
}
