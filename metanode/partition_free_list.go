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

package metanode

import (
	"fmt"
	"net"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	AsyncDeleteInterval           = 10 * time.Second
	BatchCounts                   = 128
	OpenRWAppendOpt               = os.O_CREATE | os.O_RDWR | os.O_APPEND
	TempFileValidTime             = 86400 // units: sec
	DeleteInodeFileExtension      = "INODE_DEL"
	DeleteWorkerCnt               = 10
	InodeNLink0DelayDeleteSeconds = 24 * 3600
	DeleteInodeFileRollingSize    = 500 * util.MB
)

func (mp *metaPartition) openDeleteInodeFile() (err error) {
	if mp.delInodeFp, err = os.OpenFile(path.Join(mp.config.RootDir,
		DeleteInodeFileExtension), OpenRWAppendOpt, 0o644); err != nil {
		log.LogErrorf("[openDeleteInodeFile] failed to open delete inode file, err(%v)", err)
		return
	}
	return
}

func (mp *metaPartition) startFreeList() (err error) {
	if err = mp.openDeleteInodeFile(); err != nil {
		return
	}

	go mp.updateVolWorker()
	go mp.deleteWorker()
	go mp.startRecycleInodeDelFile()
	mp.startToDeleteExtents()
	return
}

func (mp *metaPartition) UpdateVolumeView(dataView *proto.DataPartitionsView, volumeView *proto.SimpleVolView) {
	convert := func(view *proto.DataPartitionsView) *DataPartitionsView {
		newView := &DataPartitionsView{
			DataPartitions: make([]*DataPartition, len(view.DataPartitions)),
		}
		for i := 0; i < len(view.DataPartitions); i++ {
			if len(view.DataPartitions[i].Hosts) < 1 {
				log.LogErrorf("action[UpdateVolumeView] dp id(%v) is invalid, DataPartitionResponse detail[%v]",
					view.DataPartitions[i].PartitionID, view.DataPartitions[i])
				continue
			}
			newView.DataPartitions[i] = &DataPartition{
				PartitionID: view.DataPartitions[i].PartitionID,
				Status:      view.DataPartitions[i].Status,
				Hosts:       view.DataPartitions[i].Hosts,
				ReplicaNum:  view.DataPartitions[i].ReplicaNum,
				IsDiscard:   view.DataPartitions[i].IsDiscard,
			}
		}
		return newView
	}
	mp.vol.UpdatePartitions(convert(dataView))
	mp.vol.volDeleteLockTime = volumeView.DeleteLockTime
	mp.enablePersistAccessTime = volumeView.EnablePersistAccessTime
	if volumeView.AccessTimeInterval <= proto.MinAccessTimeValidInterval {
		volumeView.AccessTimeInterval = proto.MinAccessTimeValidInterval
	}
	atomic.StoreUint64(&mp.accessTimeValidInterval, uint64(volumeView.AccessTimeInterval))
}

func (mp *metaPartition) updateVolView(convert func(view *proto.DataPartitionsView) *DataPartitionsView) (err error) {
	volName := mp.config.VolName
	dataView, err := masterClient.ClientAPI().EncodingGzip().GetDataPartitions(volName)
	if err != nil {
		err = fmt.Errorf("updateVolWorker: get data partitions view fail: volume(%v) err(%v)",
			volName, err)
		log.LogErrorf(err.Error())
		return
	}
	mp.vol.UpdatePartitions(convert(dataView))

	volView, err := masterClient.AdminAPI().GetVolumeSimpleInfo(volName)
	if err != nil {
		err = fmt.Errorf("updateVolWorker: get volumeinfo fail: volume(%v)  err(%v)", volName, err)
		log.LogErrorf(err.Error())
		return
	}
	mp.vol.volDeleteLockTime = volView.DeleteLockTime
	return nil
}

func (mp *metaPartition) updateVolWorker() {
	t := time.NewTicker(UpdateVolTicket)
	convert := func(view *proto.DataPartitionsView) *DataPartitionsView {
		newView := &DataPartitionsView{
			DataPartitions: make([]*DataPartition, len(view.DataPartitions)),
		}
		for i := 0; i < len(view.DataPartitions); i++ {
			if len(view.DataPartitions[i].Hosts) < 1 {
				log.LogErrorf("updateVolWorker dp id(%v) is invalid, DataPartitionResponse detail[%v]",
					view.DataPartitions[i].PartitionID, view.DataPartitions[i])
				continue
			}
			newView.DataPartitions[i] = &DataPartition{
				PartitionID: view.DataPartitions[i].PartitionID,
				Status:      view.DataPartitions[i].Status,
				Hosts:       view.DataPartitions[i].Hosts,
				ReplicaNum:  view.DataPartitions[i].ReplicaNum,
				IsDiscard:   view.DataPartitions[i].IsDiscard,
			}
		}
		return newView
	}
	mp.updateVolView(convert)
	for {
		select {
		case <-mp.stopC:
			t.Stop()
			return
		case <-t.C:
			mp.updateVolView(convert)
		}
	}
}

const (
	MinDeleteBatchCounts = 100
	MaxSleepCnt          = 10
)

func (mp *metaPartition) deleteWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]uint64, 0, DeleteBatchCount())
	var sleepCnt uint64
	for {
		buffSlice = buffSlice[:0]
		select {
		case <-mp.stopC:
			log.LogDebugf("[deleteWorker] stop partition: %v", mp.config.PartitionId)
			return
		default:
		}

		if _, isLeader = mp.IsLeader(); !isLeader {
			time.Sleep(AsyncDeleteInterval)
			continue
		}

		// add sleep time value
		DeleteWorkerSleepMs()

		isForceDeleted := sleepCnt%MaxSleepCnt == 0
		if !isForceDeleted && mp.freeList.Len() < MinDeleteBatchCounts {
			time.Sleep(AsyncDeleteInterval)
			sleepCnt++
			continue
		}

		// do nothing.
		if mp.freeList.Len() == 0 {
			log.LogInfof("[deleteWorker] vol(%v) mp(%v) skip, free list is empty", mp.config.VolName, mp.config.PartitionId)
			time.Sleep(time.Minute)
			continue
		}

		batchCount := DeleteBatchCount()
		delayDeleteInos := make([]uint64, 0)
		for idx = 0; idx < int(batchCount); idx++ {
			// batch get free inode from the freeList
			ino := mp.freeList.Pop()
			if ino == 0 {
				break
			}
			//TODO:tangjingyu，distinguished remove inode and migrateEks
			log.LogDebugf("action[deleteWorker]: vol(%v) mp(%v) remove inode(%v)", mp.config.VolName, mp.config.PartitionId, ino)

			// check inode nlink == 0 and deleteMarkFlag unset
			if inode, ok := mp.inodeTree.Get(&Inode{Inode: ino}).(*Inode); ok {
				inTx, _ := mp.txProcessor.txResource.isInodeInTransction(inode)
				if inode.ShouldDelayDelete() || inTx {
					// TODO:tangjingyu too many logs leads to high mem usage.
					//if log.EnableDebug() {
					//	log.LogDebugf("[deleteWorker] mpId(%v) delay to remove inode(%v) Nlink(%v) migrateStorageClass(%v), inTx %v",
					//		mp.config.PartitionId, inode.Inode, inode.HybridCouldExtentsMigration.storageClass, inode.NLink, inTx)
					//}
					delayDeleteInos = append(delayDeleteInos, ino)
					continue
				}
			}

			buffSlice = append(buffSlice, ino)
		}

		// delay
		for _, delayDeleteIno := range delayDeleteInos {
			mp.freeList.Push(delayDeleteIno)
		}
		log.LogDebugf("[deleteWorker] metaPartition[%v] inodes[%v]", mp.config.PartitionId, buffSlice)

		mp.persistDeletedInodes(buffSlice)
		mp.deleteMarkedInodes(buffSlice)
		sleepCnt++
	}
}

// delete Extents by Partition,and find all successDelete inode
func (mp *metaPartition) batchDeleteExtentsByPartition(partitionDeleteExtents map[uint64][]*proto.DelExtentParam,
	allInodes []*Inode, isCache bool, isMigration bool,
) (shouldCommit []*Inode, shouldPushToFreeList []*Inode) {
	occurErrors := make(map[uint64]error)
	shouldCommit = make([]*Inode, 0, len(allInodes))
	shouldPushToFreeList = make([]*Inode, 0)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)

	// wait all Partition do BatchDeleteExtents finish
	for partitionID, extents := range partitionDeleteExtents {
		dp := mp.vol.GetPartition(partitionID)
		// NOTE: if dp is discard, skip it
		if dp.IsDiscard {
			log.LogWarnf("action[batchDeleteExtentsByPartition] dp(%v) is discard, skip extents count(%v)", partitionID, len(extents))
			continue
		}
		if log.EnableDebug() {
			log.LogDebugf("batchDeleteExtentsByPartition partitionID %v extents %v", partitionID, len(extents))
		}
		wg.Add(1)
		go func(partitionID uint64, extents []*proto.DelExtentParam) {
			defer wg.Done()
			_, perr := mp.batchDeleteExtentsByDp(partitionID, extents)
			lock.Lock()
			occurErrors[partitionID] = perr
			lock.Unlock()
		}(partitionID, extents)
	}
	wg.Wait()

	// range AllNode,find all Extents delete success on inode,it must to be append shouldCommit
	for i := 0; i < len(allInodes); i++ {
		successDeleteExtentCnt := 0
		inode := allInodes[i]
		var extents = NewSortedExtents()
		if isCache {
			extents = inode.Extents
		} else if isMigration {
			if inode.HybridCouldExtentsMigration.sortedEks != nil {
				extents = inode.HybridCouldExtentsMigration.sortedEks.(*SortedExtents)
			}
		} else {
			if inode.HybridCouldExtents.sortedEks != nil {
				extents = inode.HybridCouldExtents.sortedEks.(*SortedExtents)
			}
		}
		extents.Range(func(_ int, ek proto.ExtentKey) bool {
			if occurErrors[ek.PartitionId] == nil {
				successDeleteExtentCnt++
				return true
			} else {
				log.LogWarnf("batchDeleteExtentsByPartition: deleteInode Inode(%v) error(%v)", inode.Inode, occurErrors[ek.PartitionId])
				return false
			}
			successDeleteExtentCnt++
			return true
		})
		if successDeleteExtentCnt == extents.Len() {
			shouldCommit = append(shouldCommit, inode)
			log.LogDebugf("action[batchDeleteExtentsByPartition]: delete vol(%v) mp(%v) inode(%v) success", mp.config.VolName, mp.config.PartitionId, inode.Inode)
		} else {
			shouldPushToFreeList = append(shouldPushToFreeList, inode)
			log.LogDebugf("action[batchDeleteExtentsByPartition]: delete vol(%v) mp(%v) inode(%v) fail", mp.config.VolName, mp.config.PartitionId, inode.Inode)
		}
	}

	return
}

func (mp *metaPartition) deleteMarkedInodes(inoSlice []uint64) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			log.LogErrorf(fmt.Sprintf("metaPartition(%v) deleteMarkedInodes panic (%v)\nstack:%v",
				mp.config.PartitionId, r, stack))
		}
	}()

	if len(inoSlice) == 0 {
		return
	}

	log.LogDebugf("deleteMarkedInodes. mp %v inoSlice [%v]", mp.config.PartitionId, inoSlice)
	var (
		replicaInodes          = make([]uint64, 0)
		ebsInodes              = make([]uint64, 0)
		shouldRePushToFreeList = make([]*Inode, 0)
		allInodes              = make([]*Inode, 0)
	)
	for _, ino := range inoSlice {
		ref := &Inode{Inode: ino}
		inode, ok := mp.inodeTree.Get(ref).(*Inode)
		if !ok {
			log.LogDebugf("deleteMarkedInodes. mp %v inode [%v] not found", mp.config.PartitionId, ino)
			continue
		}
		if inode.IsDeleteMigrationExtentKeyOnly() {
			allInodes = append(allInodes, inode)
			log.LogDebugf("deleteMarkedInodes. skip mp %v inode [%v] for deleting hybrid cloud extent key", mp.config.PartitionId, ino)
			continue
		}
		if proto.IsStorageClassReplica(inode.StorageClass) {
			replicaInodes = append(replicaInodes, inode.Inode)
		} else {
			ebsInodes = append(ebsInodes, inode.Inode)
		}
	}
	// step1: delete inode by current storage class
	if len(replicaInodes) > 0 {
		shouldCommitReplicaInode, shouldRePushToFreeListReplicaInode :=
			mp.deleteMarkedReplicaInodes(replicaInodes, false, false)
		allInodes = append(allInodes, shouldCommitReplicaInode...)
		shouldRePushToFreeList = append(shouldRePushToFreeList, shouldRePushToFreeListReplicaInode...)
	}
	if len(ebsInodes) > 0 {
		shouldCommitEbsInode, shouldRePushToFreeListEbsInode := mp.deleteMarkedEBSInodes(ebsInodes, false)
		allInodes = append(allInodes, shouldCommitEbsInode...)
		shouldRePushToFreeList = append(shouldRePushToFreeList, shouldRePushToFreeListEbsInode...)
	}

	// step2: delete inode by migration storage class
	replicaInodes = make([]uint64, 0)
	ebsInodes = make([]uint64, 0)
	leftInodes := make([]*Inode, 0) //
	for _, ino := range allInodes {
		if proto.IsStorageClassReplica(ino.HybridCouldExtentsMigration.storageClass) {
			replicaInodes = append(replicaInodes, ino.Inode)
		} else if proto.IsStorageClassBlobStore(ino.HybridCouldExtentsMigration.storageClass) {
			ebsInodes = append(ebsInodes, ino.Inode)
		} else { //StorageClass_Unspecified
			leftInodes = append(leftInodes, ino)
		}
	}
	allInodes = make([]*Inode, 0)
	allInodes = append(allInodes, leftInodes...)
	if len(replicaInodes) > 0 {
		shouldCommitReplicaInode, shouldRePushToFreeListReplicaInode :=
			mp.deleteMarkedReplicaInodes(replicaInodes, false, true)
		allInodes = append(allInodes, shouldCommitReplicaInode...)
		shouldRePushToFreeList = append(shouldRePushToFreeList, shouldRePushToFreeListReplicaInode...)
	}
	if len(ebsInodes) > 0 {
		shouldCommitEbsInode, shouldRePushToFreeListEbsInode := mp.deleteMarkedEBSInodes(ebsInodes, true)
		allInodes = append(allInodes, shouldCommitEbsInode...)
		shouldRePushToFreeList = append(shouldRePushToFreeList, shouldRePushToFreeListEbsInode...)
	}
	// step3: delete inode by migration cache storage class
	// cache storage class can only be replica system for now
	replicaInodes = make([]uint64, 0)
	leftInodes = make([]*Inode, 0)
	for _, ino := range allInodes {
		if ino.IsDeleteMigrationExtentKeyOnly() {
			leftInodes = append(leftInodes, ino)
			continue
		}
		if len(ino.Extents.eks) != 0 {
			replicaInodes = append(replicaInodes, ino.Inode)
		} else {
			leftInodes = append(leftInodes, ino)
		}
	}
	allInodes = make([]*Inode, 0)
	allInodes = append(allInodes, leftInodes...)
	if len(replicaInodes) > 0 {
		shouldCommitReplicaInode, shouldRePushToFreeListReplicaInode :=
			mp.deleteMarkedReplicaInodes(replicaInodes, true, false)
		allInodes = append(allInodes, shouldCommitReplicaInode...)
		shouldRePushToFreeList = append(shouldRePushToFreeList, shouldRePushToFreeListReplicaInode...)
	}

	// notify follower to clear migration extent key
	deleteInodes := make([]*Inode, 0)
	deleteMigrationEkInodes := make([]*Inode, 0)
	for _, inode := range allInodes {
		if inode.IsDeleteMigrationExtentKeyOnly() {
			deleteMigrationEkInodes = append(deleteMigrationEkInodes, inode)
		} else {
			deleteInodes = append(deleteInodes, inode)
		}
	}

	if len(deleteInodes) > 0 {
		bufSlice := make([]byte, 0, 8*len(deleteInodes))
		for _, inode := range deleteInodes {
			bufSlice = append(bufSlice, inode.MarshalKey()...)
		}
		err := mp.syncToRaftFollowersFreeInode(bufSlice)
		if err != nil {
			log.LogWarnf("[deleteMarkedInodes] raft commit free inode list: %v, "+
				"response %s", deleteInodes, err.Error())
		}
		for _, inode := range deleteInodes {
			if err != nil {
				mp.freeList.Push(inode.Inode)
			}
		}
	}
	//only reset migration extent key
	if len(deleteMigrationEkInodes) > 0 {
		bufSlice := make([]byte, 0, 8*len(deleteMigrationEkInodes))
		for _, inode := range deleteMigrationEkInodes {
			bufSlice = append(bufSlice, inode.MarshalKey()...)
		}
		err := mp.syncToRaftFollowersFreeInodeMigrationExtentKey(bufSlice)
		if err != nil {
			log.LogWarnf("[deleteMarkedInodes] raft commit free inode migration extent key list: %v, "+
				"response %s", deleteMigrationEkInodes, err.Error())
		}
		for _, inode := range deleteMigrationEkInodes {
			if err != nil {
				mp.freeList.Push(inode.Inode)
			}
		}
	}

	log.LogInfof("[deleteMarkedInodes] metaPartition(%v) deleteInodeCnt(%v) inodeCnt(%v)",
		mp.config.PartitionId, len(allInodes), mp.inodeTree.Len())

	for _, inode := range shouldRePushToFreeList {
		mp.freeList.Push(inode.Inode)
	}
	// try again.
	if len(shouldRePushToFreeList) > 0 && deleteWorkerSleepMs == 0 {
		time.Sleep(time.Duration(1000) * time.Millisecond)
	}
}

func (mp *metaPartition) deleteMarkedReplicaInodes(inoSlice []uint64, isCache,
	isMigration bool) (shouldCommit []*Inode, shouldPushToFreeList []*Inode) {
	log.LogDebugf("[deleteMarkedReplicaInodes] mp[%v] inoSlice[%v] isCache[%v] isMigration[%v]",
		mp.config.PartitionId, inoSlice, isCache, isMigration)
	deleteExtentsByPartition := make(map[uint64][]*proto.DelExtentParam)
	allInodes := make([]*Inode, 0)
	for _, ino := range inoSlice {
		ref := &Inode{Inode: ino}
		inode, ok := mp.inodeTree.Get(ref).(*Inode)
		if !ok {
			log.LogDebugf("[deleteMarkedReplicaInodes] mp[%v] inode [%v] not found", mp.config.PartitionId, ino)
			continue
		}

		if !inode.ShouldDelete() && !inode.ShouldDeleteMigrationExtentKey(isMigration) {
			log.LogWarnf("[deleteMarkedReplicaInodes] inode should not be deleted, isMigration[%v] mp[%v] ino %s",
				isMigration, mp.config.PartitionId, inode.String())
			continue
		}

		log.LogDebugf("[deleteMarkedReplicaInodes] isMigration[%v] mp[%v] inode[%v] inode.Extents: %v, ino verList: %v",
			isMigration, mp.config.PartitionId, ino, inode.Extents, inode.GetMultiVerString())

		if inode.getLayerLen() > 0 {
			log.LogErrorf("[deleteMarkedReplicaInodes] deleteMarkedInodes. mp[%v] inode[%v] verlist len %v should not drop",
				mp.config.PartitionId, ino, inode.getLayerLen())
			return
		}

		extInfo := inode.GetAllExtsOfflineInode(mp.config.PartitionId, isCache, isMigration)
		for dpID, inodeExts := range extInfo {
			exts, ok := deleteExtentsByPartition[dpID]
			if !ok {
				exts = make([]*proto.DelExtentParam, 0)
			}
			for _, ext := range inodeExts {
				exts = append(exts, &proto.DelExtentParam{
					ExtentKey:          ext,
					IsSnapshotDeletion: ext.IsSplit(),
				})
			}
			log.LogWritef("[deleteMarkedReplicaInodes] mp[%v] ino(%v) deleteExtent(%v) by dp(%v) isCache(%v) isMigration(%v)",
				mp.config.PartitionId, inode.Inode, len(inodeExts), dpID, isCache, isMigration)
			deleteExtentsByPartition[dpID] = exts
		}
		allInodes = append(allInodes, inode)
	}
	shouldCommit, shouldPushToFreeList = mp.batchDeleteExtentsByPartition(deleteExtentsByPartition, allInodes,
		isCache, isMigration)
	return
}

func (mp *metaPartition) deleteMarkedEBSInodes(inoSlice []uint64, isMigration bool) (shouldCommit []*Inode, shouldPushToFreeList []*Inode) {
	log.LogDebugf("deleteMarkedEBSInodes. mp %v inoSlice [%v] isMigration %v",
		mp.config.PartitionId, inoSlice, isMigration)
	allInodes := make([]*Inode, 0)
	for _, ino := range inoSlice {
		ref := &Inode{Inode: ino}
		inode, ok := mp.inodeTree.Get(ref).(*Inode)
		if !ok {
			log.LogDebugf("deleteMarkedEBSInodes. mp %v inode [%v] not found", mp.config.PartitionId, ino)
			continue
		}
		if !inode.ShouldDelete() && !inode.ShouldDeleteMigrationExtentKey(isMigration) {
			log.LogWarnf("[deleteMarkedReplicaInodes] : inode should not be deleted, mp[%v] ino %s",
				mp.config.PartitionId, inode.String())
			continue
		}

		log.LogDebugf("deleteMarkedEBSInodes. mp %v inode [%v] inode.Extents %v, ino verlist %v",
			mp.config.PartitionId, ino, inode.Extents, inode.GetMultiVerString())
		if inode.getLayerLen() > 1 {
			log.LogErrorf("deleteMarkedEBSInodes. mp %v inode [%v] verlist len %v should not drop",
				mp.config.PartitionId, ino, inode.getLayerLen())
			return
		}

		allInodes = append(allInodes, inode)
	}
	shouldCommit, shouldPushToFreeList = mp.doBatchDeleteObjExtentsInEBS(allInodes, isMigration)
	return
}

//// Delete the marked inodes.
//func (mp *metaPartition) deleteMarkedInodes(inoSlice []uint64) {
//	defer func() {
//		if r := recover(); r != nil {
//			log.LogErrorf(fmt.Sprintf("metaPartition(%v) deleteMarkedInodes panic (%v)", mp.config.PartitionId, r))
//		}
//	}()
//
//	if len(inoSlice) == 0 {
//		return
//	}
//	log.LogDebugf("deleteMarkedInodes. mp %v inoSlice [%v]", mp.config.PartitionId, inoSlice)
//	shouldCommit := make([]*Inode, 0, DeleteBatchCount())
//	shouldRePushToFreeList := make([]*Inode, 0)
//	deleteExtentsByPartition := make(map[uint64][]*proto.ExtentKey)
//	allInodes := make([]*Inode, 0)
//	//TODO:!!这里有问题
//	for _, ino := range inoSlice {
//		ref := &Inode{Inode: ino}
//		inode, ok := mp.inodeTree.Get(ref).(*Inode)
//		if !ok {
//			log.LogDebugf("deleteMarkedInodes. mp %v inode [%v] not found", mp.config.PartitionId, ino)
//			continue
//		}
//
//		log.LogDebugf("deleteMarkedInodes. mp %v inode [%v] inode.Extents %v, ino verlist %v", mp.config.PartitionId, ino, inode.Extents, inode.multiSnap.multiVersions)
//		if inode.getLayerLen() > 1 {
//			log.LogErrorf("deleteMarkedInodes. mp %v inode [%v] verlist len %v should not drop",
//				mp.config.PartitionId, ino, inode.getLayerLen())
//			return
//		}
//
//		extInfo := inode.GetAllExtsOfflineInode(mp.config.PartitionId)
//		for dpID, inodeExts := range extInfo {
//			exts, ok := deleteExtentsByPartition[dpID]
//			if !ok {
//				exts = make([]*proto.ExtentKey, 0)
//			}
//			exts = append(exts, inodeExts...)
//			log.LogWritef("mp(%v) ino(%v) deleteExtent(%v)", mp.config.PartitionId, inode.Inode, len(inodeExts))
//			deleteExtentsByPartition[dpID] = exts
//		}
//
//		allInodes = append(allInodes, inode)
//	}
//	//delete ek first than cache key
//	var (
//		replicaInodes = make([]*Inode, 0)
//		ebsInodes     = make([]*Inode, 0)
//	)
//	for _, inode := range allInodes {
//		if inode.storeInReplicaSystem() {
//			replicaInodes = append(replicaInodes, inode)
//		} else {
//			ebsInodes = append(ebsInodes, inode)
//		}
//	}
//	allInodes = allInodes[:0]
//	if len(ebsInodes) > 0 {
//		shouldCommit, shouldRePushToFreeList = mp.doBatchDeleteObjExtentsInEBS(ebsInodes)
//		log.LogInfof("[doBatchDeleteObjExtentsInEBS] metaPartition(%v) deleteInodeCnt(%d) shouldRePush(%d)",
//			mp.config.PartitionId, len(shouldCommit), len(shouldRePushToFreeList))
//		for _, inode := range shouldRePushToFreeList {
//			mp.freeList.Push(inode.Inode)
//		}
//		allInodes = append(allInodes, shouldCommit...)
//	}
//
//	if len(replicaInodes) > 0 {
//		shouldCommit, shouldRePushToFreeList = mp.batchDeleteExtentsByPartition(deleteExtentsByPartition, replicaInodes)
//		log.LogInfof("[doBatchDeleteExtentsInReplica] metaPartition(%v) deleteInodeCnt(%d) shouldRePush(%d)",
//			mp.config.PartitionId, len(shouldCommit), len(shouldRePushToFreeList))
//		for _, inode := range shouldRePushToFreeList {
//			mp.freeList.Push(inode.Inode)
//		}
//		allInodes = append(allInodes, shouldCommit...)
//	}
//
//	//if proto.IsCold(mp.volType) {
//	//	// delete ebs obj extents
//	//	shouldCommit, shouldRePushToFreeList = mp.doBatchDeleteObjExtentsInEBS(allInodes)
//	//	log.LogInfof("[doBatchDeleteObjExtentsInEBS] metaPartition(%v) deleteInodeCnt(%d) shouldRePush(%d)",
//	//		mp.config.PartitionId, len(shouldCommit), len(shouldRePushToFreeList))
//	//	for _, inode := range shouldRePushToFreeList {
//	//		mp.freeList.Push(inode.Inode)
//	//	}
//	//	allInodes = shouldCommit
//	//}
//	log.LogInfof("[doBatchDeleteCacheExtentsInEBS] metaPartition(%v) deleteExtentsByPartition(%v) allInodes(%v)",
//		mp.config.PartitionId, deleteExtentsByPartition, allInodes)
//	shouldCommit, shouldRePushToFreeList = mp.batchDeleteExtentsByPartition(deleteExtentsByPartition, allInodes)
//	bufSlice := make([]byte, 0, 8*len(shouldCommit))
//	for _, inode := range shouldCommit {
//		bufSlice = append(bufSlice, inode.MarshalKey()...)
//	}
//
//	err := mp.syncToRaftFollowersFreeInode(bufSlice)
//	if err != nil {
//		log.LogWarnf("[deleteInodeTreeOnRaftPeers] raft commit inode list: %v, "+
//			"response %s", shouldCommit, err.Error())
//	}
//
//	for _, inode := range shouldCommit {
//		if err == nil {
//			mp.internalDeleteInode(inode)
//		} else {
//			mp.freeList.Push(inode.Inode)
//		}
//	}
//
//	log.LogInfof("metaPartition(%v) deleteInodeCnt(%v) inodeCnt(%v)", mp.config.PartitionId, len(shouldCommit), mp.inodeTree.Len())
//	for _, inode := range shouldRePushToFreeList {
//		mp.freeList.Push(inode.Inode)
//	}
//
//	// try again.
//	if len(shouldRePushToFreeList) > 0 && deleteWorkerSleepMs == 0 {
//		time.Sleep(time.Duration(1000) * time.Millisecond)
//	}
//}

func (mp *metaPartition) syncToRaftFollowersFreeInode(hasDeleteInodes []byte) (err error) {
	if len(hasDeleteInodes) == 0 {
		return
	}
	_, err = mp.submit(opFSMInternalDeleteInode, hasDeleteInodes)

	return
}

func (mp *metaPartition) syncToRaftFollowersFreeInodeMigrationExtentKey(hasDeleteInodes []byte) (err error) {
	if len(hasDeleteInodes) == 0 {
		return
	}
	_, err = mp.submit(opFSMInternalFreeInodeMigrationExtentKey, hasDeleteInodes)

	return
}

func (mp *metaPartition) notifyRaftFollowerToFreeInodes(wg *sync.WaitGroup, target string, hasDeleteInodes []byte) (err error) {
	var conn *net.TCPConn
	conn, err = mp.config.ConnPool.GetConnect(target)
	defer func() {
		wg.Done()
		if err != nil {
			log.LogWarnf(err.Error())
			mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		} else {
			mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
		}
	}()
	if err != nil {
		return
	}
	request := NewPacketToFreeInodeOnRaftFollower(mp.config.PartitionId, hasDeleteInodes)
	if err = request.WriteToConn(conn); err != nil {
		return
	}

	if err = request.ReadFromConnWithVer(conn, proto.NoReadDeadlineTime); err != nil {
		return
	}

	if request.ResultCode != proto.OpOk {
		err = fmt.Errorf("request(%v) error(%v)", request.GetUniqueLogId(), string(request.Data[:request.Size]))
	}

	return
}

func (mp *metaPartition) doBatchDeleteExtentsByPartition(partitionID uint64, exts []*proto.DelExtentParam) (err error) {
	// get the data node view
	dp := mp.vol.GetPartition(partitionID)
	if dp == nil {
		if proto.IsCold(mp.volType) {
			log.LogInfof("[doBatchDeleteExtentsByPartition] vol(%v) mp(%v) dp(%d) is already been deleted, not delete any more", mp.config.VolName, mp.config.PartitionId, partitionID)
			return
		}

		err = errors.NewErrorf("unknown dataPartitionID=%d in vol",
			partitionID)
		return
	}

	for _, ext := range exts {
		if ext.PartitionId != partitionID {
			err = errors.NewErrorf("BatchDeleteExtent do batchDelete on PartitionID(%v) but unexpect Extent(%v)", partitionID, ext)
			return
		}
	}

	defer func() {
		if err != nil && dp.IsDiscard {
			log.LogWarnf("doBatchDeleteExtentsByPartition: dp is already discard, no need to delete any more. dp(%d), err %s", dp.PartitionID, err.Error())
			err = nil
		}
	}()

	// delete the data node
	if len(dp.Hosts) < 1 {
		log.LogErrorf("[doBatchDeleteExtentsByPartition] vol(%v) mp(%v) dp id(%v) is invalid, detail[%v]", mp.config.VolName, mp.config.PartitionId, partitionID, dp)
		err = errors.NewErrorf("dp id(%v) is invalid", partitionID)
		return
	}
	addr := util.ShiftAddrPort(dp.Hosts[0], smuxPortShift)
	conn, err := smuxPool.GetConnect(addr)
	log.LogInfof("[doBatchDeleteExtentsByPartition] mp(%v) GetConnect (%v)", mp.config.PartitionId, addr)

	ResultCode := proto.OpOk

	defer func() {
		smuxPool.PutConnect(conn, ForceClosedConnect)
		log.LogInfof("[doBatchDeleteExtentsByPartition] mp(%v) PutConnect (%v)", mp.config.PartitionId, addr)
	}()

	if err != nil {
		err = errors.NewErrorf("get conn from pool %s, "+
			"extents partitionId=%d",
			err.Error(), partitionID)
		return
	}
	p := NewPacketToBatchDeleteExtent(dp, exts)
	if err = p.WriteToConn(conn); err != nil {
		err = errors.NewErrorf("write to dataNode %s, %s", p.GetUniqueLogId(),
			err.Error())
		return
	}
	if err = p.ReadFromConnWithVer(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		err = errors.NewErrorf("read response from dataNode %s, %s",
			p.GetUniqueLogId(), err.Error())
		return
	}

	ResultCode = p.ResultCode

	if ResultCode == proto.OpTryOtherAddr && proto.IsCold(mp.volType) {
		log.LogInfof("[doBatchDeleteExtentsByPartition] deleteOp retrun tryOtherAddr code means dp is deleted for LF vol, dp(%d)", partitionID)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("[deleteMarkedInodes] %s response: %s", p.GetUniqueLogId(),
			p.GetResultMsg())
	}

	return
}

const maxDelCntOnce = 512

func (mp *metaPartition) doBatchDeleteObjExtentsInEBS(allInodes []*Inode, isMigration bool) (shouldCommit []*Inode, shouldPushToFreeList []*Inode) {
	shouldCommit = make([]*Inode, 0, len(allInodes))
	shouldPushToFreeList = make([]*Inode, 0)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)

	for _, inode := range allInodes {
		wg.Add(1)
		objExtents := NewSortedObjExtents()
		inode.RLock()
		if isMigration {
			if inode.HybridCouldExtentsMigration.sortedEks != nil {
				objExtents = inode.HybridCouldExtentsMigration.sortedEks.(*SortedObjExtents)
			}
		} else {
			if inode.HybridCouldExtents.sortedEks != nil {
				objExtents = inode.HybridCouldExtents.sortedEks.(*SortedObjExtents)
			}
		}

		objExtents.RLock()
		go func(ino *Inode, oeks []proto.ObjExtentKey) {
			defer wg.Done()
			log.LogDebugf("[doBatchDeleteObjExtentsInEBS] ino(%d) delObjEks[%d] isMigration(%v)",
				ino.Inode, len(oeks), isMigration)
			err := mp.deleteObjExtents(oeks)

			lock.Lock()
			if err != nil {
				shouldPushToFreeList = append(shouldPushToFreeList, ino)
				log.LogErrorf("[doBatchDeleteObjExtentsInEBS] delete ebs eks fail, ino(%d), cnt(%d), err(%s)", ino.Inode, len(oeks), err.Error())
			} else {
				shouldCommit = append(shouldCommit, ino)
			}
			lock.Unlock()

			objExtents.RUnlock()
			ino.RUnlock()
		}(inode, objExtents.eks)
	}

	wg.Wait()

	return
}

func (mp *metaPartition) deleteObjExtents(oeks []proto.ObjExtentKey) (err error) {
	total := len(oeks)

	for i := 0; i < total; i += maxDelCntOnce {
		max := util.Min(i+maxDelCntOnce, total)
		if mp.ebsClient == nil {
			err = fmt.Errorf("(%v) error ebsClient nil", mp.config.PartitionId)
			log.LogErrorf("deleteObjExtents %v", err)
			return
		}
		err = mp.ebsClient.Delete(oeks[i:max])
		if err != nil {
			log.LogErrorf("[deleteObjExtents] vol(%v) mp(%v) delete ebs eks fail, cnt(%d), err(%s)", mp.config.VolName, mp.config.PartitionId, max-i, err.Error())
			return err
		}
	}

	return err
}

func (mp *metaPartition) startRecycleInodeDelFile() {
	timer := time.NewTicker(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-mp.stopC:
			return
		case <-timer.C:
			mp.recycleInodeDelFile()
		}
	}
}

func (mp *metaPartition) recycleInodeDelFile() {
	if !mp.recycleInodeDelFileFlag.TestAndSet() {
		return
	}
	defer mp.recycleInodeDelFileFlag.Release()
	// NOTE: get all files
	dentries, err := os.ReadDir(mp.config.RootDir)
	if err != nil {
		log.LogErrorf("[recycleInodeDelFile] mp(%v) failed to read dir(%v)", mp.config.PartitionId, mp.config.RootDir)
		return
	}
	inodeDelFiles := make([]string, 0)
	for _, dentry := range dentries {
		if strings.HasPrefix(dentry.Name(), DeleteInodeFileExtension) && strings.HasSuffix(dentry.Name(), ".old") {
			inodeDelFiles = append(inodeDelFiles, dentry.Name())
		}
	}
	// NOTE: sort files
	sort.Slice(inodeDelFiles, func(i, j int) bool {
		// NOTE: date format satisfies dictionary order
		return inodeDelFiles[i] < inodeDelFiles[j]
	})

	// NOTE: check disk space and recycle files
	for len(inodeDelFiles) > 0 {
		diskSpaceLeft := int64(0)
		stat, err := fileutil.Statfs(mp.config.RootDir)
		if err != nil {
			log.LogErrorf("[recycleInodeDelFile] mp(%v) failed to get fs info", mp.config.PartitionId)
			return
		}
		diskSpaceLeft = int64(stat.Bavail * uint64(stat.Bsize))
		// NOTE: 5% of disk space
		spaceWaterMark := int64(stat.Blocks*uint64(stat.Bsize)) / 20
		if spaceWaterMark > 50*util.GB {
			spaceWaterMark = 50 * util.GB
		}
		if diskSpaceLeft >= spaceWaterMark && len(inodeDelFiles) < 5 {
			log.LogDebugf("[recycleInodeDelFile] mp(%v) not need to recycle, return", mp.config.PartitionId)
			return
		}
		// NOTE: delete a file and pop an item
		oldestFile := inodeDelFiles[0]
		inodeDelFiles = inodeDelFiles[1:]
		err = os.Remove(oldestFile)
		if err != nil {
			log.LogErrorf("[recycleInodeDelFile] mp(%v) failed to remove file(%v)", mp.config.PartitionId, oldestFile)
			return
		}
	}
}

func (mp *metaPartition) persistDeletedInode(ino uint64, currentSize *uint64) {
	if *currentSize >= DeleteInodeFileRollingSize {
		fileName := fmt.Sprintf("%v.%v.%v", DeleteInodeFileExtension, time.Now().Format(log.FileNameDateFormat), "old")
		if err := mp.delInodeFp.Sync(); err != nil {
			log.LogErrorf("[persistDeletedInode] vol(%v) mp(%v) failed to sync delete inode file, err(%v), inode(%v)", mp.config.VolName, mp.config.PartitionId, err, ino)
			return
		}
		mp.delInodeFp.Close()
		mp.delInodeFp = nil
		// NOTE: that is ok, if rename fails
		// we will re-open it in next line
		fileName = path.Join(mp.config.RootDir, fileName)
		err := os.Rename(path.Join(mp.config.RootDir, DeleteInodeFileExtension), fileName)
		if err != nil {
			log.LogErrorf("[persistDeletedInode] vol(%v) mp(%v) failed to rename delete inode file, err(%v)", mp.config.VolName, mp.config.PartitionId, err)
		} else {
			*currentSize = 0
			mp.recycleInodeDelFile()
		}
		if err = mp.openDeleteInodeFile(); err != nil {
			log.LogErrorf("[persistDeletedInode] vol(%v) mp(%v) failed to open delete inode file, err(%v), inode(%v)", mp.config.VolName, mp.config.PartitionId, err, ino)
			return
		}
	}
	content := fmt.Sprintf("%v\n", ino)
	*currentSize += uint64(len(content))
	if _, err := mp.delInodeFp.WriteString(content); err != nil {
		log.LogErrorf("[persistDeletedInode] failed to persist ino(%v), err(%v)", ino, err)
		return
	}
}

func (mp *metaPartition) persistDeletedInodes(inos []uint64) {
	log.LogDebugf("[persistDeletedInodes] vol(%v) mp(%v) inos [%v]", mp.config.VolName, mp.config.PartitionId, inos)
	if mp.delInodeFp == nil {
		// NOTE: hope it can re-open file
		if err := mp.openDeleteInodeFile(); err != nil {
			log.LogErrorf("[persistDeletedInodes] vol(%v) mp(%v)  delete inode file is not open, err(%v), inodes(%v)", mp.config.VolName, mp.config.PartitionId, err, inos)
			return
		}
		log.LogWarnf("[persistDeletedInodes] vol(%v) mp(%v) re-open file success", mp.config.VolName, mp.config.PartitionId)
	}
	info, err := mp.delInodeFp.Stat()
	if err != nil {
		log.LogErrorf("[persistDeletedInodes] vol(%v) mp(%v) failed to get size of delete inode file, err(%v), inodes(%v)", mp.config.VolName, mp.config.PartitionId, err, inos)
		return
	}
	currSize := uint64(info.Size())
	for _, ino := range inos {
		mp.persistDeletedInode(ino, &currSize)
	}
}

func (mp *metaPartition) syncToRaftFollowersFreeForbiddenMigrationInode(hasDeleteInodes []byte) (err error) {
	if len(hasDeleteInodes) == 0 {
		return
	}
	_, err = mp.submit(opFSMInternalFreeForbiddenMigrationInode, hasDeleteInodes)

	return
}
