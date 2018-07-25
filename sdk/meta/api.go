package meta

import (
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

// TODO: High-level API, i.e. work with absolute path

// Low-level API, i.e. work with inode

const (
	BatchIgetRespBuf = 1000
)

func (mw *MetaWrapper) Statfs() (total, used uint64) {
	total = atomic.LoadUint64(&mw.totalSize)
	used = atomic.LoadUint64(&mw.usedSize)
	return
}

func (mw *MetaWrapper) Open_ll(inode uint64) error {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Open_ll: No such partition, ino(%v)", inode)
		return syscall.ENOENT
	}

	status, err := mw.open(mp, inode)
	if err != nil {
		return syscall.EAGAIN
	}
	if status != statusOK {
		if status == statusNoent {
			return syscall.ENOENT
		} else {
			return syscall.EPERM
		}
	}
	return nil
}

func (mw *MetaWrapper) Create_ll(parentID uint64, name string, mode uint32) (*proto.InodeInfo, error) {
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

	mp = mw.getLatestPartition()
	if mp != nil {
		status, info, err = mw.icreate(mp, mode)
		if err == nil {
			if status == statusOK {
				goto create_dentry
			} else if status == statusFull {
				mw.UpdateMetaPartitions()
			}
		}
	}

	rwPartitions = mw.getRWPartitions()
	for _, mp = range rwPartitions {
		status, info, err = mw.icreate(mp, mode)
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
			mw.idelete(mp, info.Inode)
			return nil, syscall.EAGAIN
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
	if err != nil {
		return 0, 0, syscall.EAGAIN
	}
	if status != statusOK {
		return 0, 0, syscall.ENOENT
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
	if err != nil {
		return nil, syscall.EAGAIN
	}
	if status != statusOK {
		return nil, syscall.ENOENT
	}
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

func (mw *MetaWrapper) Delete_ll(parentID uint64, name string) ([]proto.ExtentKey, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		log.LogErrorf("Delete_ll: No parent partition, parentID(%v) name(%v)", parentID, name)
		return nil, syscall.ENOENT
	}

	status, inode, err := mw.ddelete(parentMP, parentID, name)
	if err != nil {
		return nil, syscall.EAGAIN
	}
	if status != statusOK {
		return nil, syscall.ENOENT
	}

	// dentry is deleted successfully but inode is not, still returns success.
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Delete_ll: No inode partition, parentID(%v) name(%v) ino(%v)", parentID, name, inode)
		return nil, nil
	}

	status, extents, err := mw.idelete(mp, inode)
	if err != nil || status != statusOK {
		return nil, nil
	}
	return extents, nil
}

func (mw *MetaWrapper) Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string) (extents []proto.ExtentKey, err error) {
	var oldInode uint64

	srcParentMP := mw.getPartitionByInode(srcParentID)
	if srcParentMP == nil {
		return nil, syscall.ENOENT
	}
	dstParentMP := mw.getPartitionByInode(dstParentID)
	if dstParentMP == nil {
		return nil, syscall.ENOENT
	}

	// look up for the ino
	status, inode, mode, err := mw.lookup(srcParentMP, srcParentID, srcName)
	if err != nil {
		return nil, syscall.EAGAIN
	}
	if status != statusOK {
		return nil, syscall.ENOENT
	}
	// create dentry in dst parent
	status, err = mw.dcreate(dstParentMP, dstParentID, dstName, inode, mode)
	if err != nil {
		return nil, syscall.EAGAIN
	}

	if status == statusExist {
		status, oldInode, err = mw.dupdate(dstParentMP, dstParentID, dstName, inode)
		if err != nil {
			return nil, syscall.EAGAIN
		}
	}

	if status != statusOK {
		return nil, statusToErrno(status)
	}

	// delete dentry from src parent
	status, _, err = mw.ddelete(srcParentMP, srcParentID, srcName)
	if err != nil || status != statusOK {
		if oldInode == 0 {
			mw.ddelete(dstParentMP, dstParentID, dstName)
		} else {
			mw.dupdate(dstParentMP, dstParentID, dstName, oldInode)
		}
		return nil, syscall.EAGAIN
	}

	if oldInode != 0 {
		inodeMP := mw.getPartitionByInode(oldInode)
		if inodeMP != nil {
			status, extents, err = mw.idelete(inodeMP, oldInode)
		}
	}

	return extents, nil
}

func (mw *MetaWrapper) ReadDir_ll(parentID uint64) ([]proto.Dentry, error) {
	parentMP := mw.getPartitionByInode(parentID)
	if parentMP == nil {
		return nil, syscall.ENOENT
	}

	status, children, err := mw.readdir(parentMP, parentID)
	if err != nil {
		return nil, syscall.EAGAIN
	}
	if status != statusOK {
		return nil, syscall.EPERM
	}
	return children, nil
}

// Used as a callback by stream sdk
func (mw *MetaWrapper) AppendExtentKey(inode uint64, ek proto.ExtentKey) error {
	log.LogDebugf("AppendExtentKey: inode(%v) ek(%v)", inode, ek)

	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return syscall.ENOENT
	}

	status, err := mw.appendExtentKey(mp, inode, ek)
	if err != nil || status != statusOK {
		log.LogErrorf("AppendExtentKey: inode(%v) ek(%v) err(%v) status(%v)", inode, ek, err, status)
		if status == statusNoent {
			return syscall.ENOENT
		} else {
			return syscall.EPERM
		}
	}
	return nil
}

func (mw *MetaWrapper) GetExtents(inode uint64) ([]proto.ExtentKey, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return nil, syscall.ENOENT
	}

	status, extents, err := mw.getExtents(mp, inode)
	if err != nil || status != statusOK {
		log.LogErrorf("GetExtents: err(%v) status(%v)", err, status)
		return nil, syscall.EPERM
	}
	return extents, nil
}

func (mw *MetaWrapper) Truncate(inode uint64) ([]proto.ExtentKey, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		log.LogErrorf("Truncate: No inode partition, ino(%v)", inode)
		return nil, syscall.ENOENT
	}

	status, extents, err := mw.truncate(mp, inode)
	if err != nil {
		return nil, syscall.EAGAIN
	}
	if status != statusOK {
		return nil, statusToErrno(status)
	}
	return extents, nil

}
