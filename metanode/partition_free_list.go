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
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	AsyncDeleteInterval           = 10 * time.Second
	UpdateVolTicket               = 2 * time.Minute
	BatchCounts                   = 128
	OpenRWAppendOpt               = os.O_CREATE | os.O_RDWR | os.O_APPEND
	TempFileValidTime             = 86400 // units: sec
	DeleteInodeFileExtension      = "INODE_DEL"
	DeleteWorkerCnt               = 10
	InodeNLink0DelayDeleteSeconds = 24 * 3600
	DeleteInodeFileRollingSize    = 500 * util.MB
)

var _deleteInodeFileRollingSize uint64 = DeleteInodeFileRollingSize

func (mp *metaPartition) openDeleteInodeFile() (err error) {
	if mp.delInodeFp, err = os.OpenFile(path.Join(mp.config.RootDir,
		DeleteInodeFileExtension), OpenRWAppendOpt, 0o644); err != nil {
		log.Errorf("[openDeleteInodeFile] failed to open delete inode file, err(%v)", err)
		return
	}
	return
}

func (mp *metaPartition) startFreeList() (err error) {
	if err = mp.openDeleteInodeFile(); err != nil {
		return
	}

	// start vol update ticket
	go mp.updateVolWorker()
	go mp.deleteWorker()
	mp.startToDeleteExtents()
	return
}

func (mp *metaPartition) updateVolView(ctx context.Context, convert func(view *proto.DataPartitionsView) *DataPartitionsView) (err error) {
	volName := mp.config.VolName
	dataView, err := masterClient.ClientAPI().EncodingGzip().GetDataPartitions(ctx, volName)
	if err != nil {
		err = fmt.Errorf("updateVolWorker: get data partitions view fail: volume(%v) err(%v)", volName, err)
		return
	}
	mp.vol.UpdatePartitions(ctx, convert(dataView))

	volView, err := masterClient.AdminAPI().GetVolumeSimpleInfo(ctx, volName)
	if err != nil {
		err = fmt.Errorf("updateVolWorker: get volumeinfo fail: volume(%v)  err(%v)", volName, err)
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
				log.Errorf("updateVolWorker dp id(%v) is invalid, DataPartitionResponse detail[%v]",
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
	for {
		span, ctx := spanContextPrefix("update-")
		if err := mp.updateVolView(ctx, convert); err != nil {
			span.Error(err)
		}

		select {
		case <-mp.stopC:
			t.Stop()
			return
		case <-t.C:
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
			log.Infof("deleteWorker stop partition: %v", mp.config)
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
			time.Sleep(time.Minute)
			continue
		}

		span, ctx := spanContextPrefix("delete-")

		batchCount := DeleteBatchCount()
		delayDeleteInos := make([]uint64, 0)
		for idx = 0; idx < int(batchCount); idx++ {
			// batch get free inode from the freeList
			ino := mp.freeList.Pop()
			if ino == 0 {
				break
			}
			span.Debugf("deleteWorker remove inode(%v)", ino)

			// check inode nlink == 0 and deleteMarkFlag unset
			if inode, ok := mp.inodeTree.Get(&Inode{Inode: ino}).(*Inode); ok {
				inTx, _ := mp.txProcessor.txResource.isInodeInTransction(inode)
				if inode.ShouldDelayDelete() || inTx {
					span.Debugf("[metaPartition] deleteWorker delay to remove inode: %v as NLink is 0, inTx %v", inode, inTx)
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
		span.Debugf("metaPartition. buff slice [%v]", buffSlice)

		mp.persistDeletedInodes(ctx, buffSlice)
		mp.deleteMarkedInodes(ctx, buffSlice)
		sleepCnt++
	}
}

// delete Extents by Partition,and find all successDelete inode
func (mp *metaPartition) batchDeleteExtentsByPartition(ctx context.Context, partitionDeleteExtents map[uint64][]*proto.ExtentKey, allInodes []*Inode,
) (shouldCommit []*Inode, shouldPushToFreeList []*Inode) {
	span := getSpan(ctx).WithOperation("batchDeleteExtentsByPartition")
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
			span.Warnf("dp(%v) is discard, skip extents count(%v)", partitionID, len(extents))
			continue
		}
		span.Debugf("partitionID %v extents %v", partitionID, extents)
		wg.Add(1)
		go func(partitionID uint64, extents []*proto.ExtentKey) {
			defer wg.Done()
			perr := mp.doBatchDeleteExtentsByPartition(ctx, partitionID, extents)
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
		inode.Extents.Range(func(_ int, ek proto.ExtentKey) bool {
			if occurErrors[ek.PartitionId] != nil {
				span.Warnf("deleteInode inode[%v] error(%v)", inode.Inode, occurErrors[ek.PartitionId])
				return false
			}
			successDeleteExtentCnt++
			return true
		})
		if successDeleteExtentCnt == inode.Extents.Len() {
			shouldCommit = append(shouldCommit, inode)
			span.Debugf("delete inode(%v) success", inode)
		} else {
			shouldPushToFreeList = append(shouldPushToFreeList, inode)
			span.Debugf("delete inode(%v) fail", inode)
		}
	}
	return
}

// Delete the marked inodes.
func (mp *metaPartition) deleteMarkedInodes(ctx context.Context, inoSlice []uint64) {
	span := spanOperationf(getSpan(ctx), "deleteMarkedInodes-mp(%d)", mp.config.PartitionId)
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			span.Errorf("panic (%v)\nstack:%v", r, stack)
		}
	}()

	if len(inoSlice) == 0 {
		return
	}
	span.Debugf("inoSlice [%v]", inoSlice)
	deleteExtentsByPartition := make(map[uint64][]*proto.ExtentKey)
	allInodes := make([]*Inode, 0)
	for _, ino := range inoSlice {
		ref := &Inode{Inode: ino}
		inode, ok := mp.inodeTree.Get(ref).(*Inode)
		if !ok {
			span.Debugf("inode[%v] not found", ino)
			continue
		}

		if !inode.ShouldDelete() {
			span.Warnf("inode should not be deleted, ino %s", inode.String())
			continue
		}

		span.Debugf("inode[%v] inode.Extents: %v, ino verList: %v", ino, inode.Extents, inode.GetMultiVerString())
		if inode.getLayerLen() > 0 {
			span.Errorf("inode[%v] verlist len %v should not drop", ino, inode.getLayerLen())
			return
		}

		extInfo := inode.GetAllExtsOfflineInode(ctx, mp.config.PartitionId)
		for dpID, inodeExts := range extInfo {
			exts, ok := deleteExtentsByPartition[dpID]
			if !ok {
				exts = make([]*proto.ExtentKey, 0)
			}
			exts = append(exts, inodeExts...)
			// NOTICE: log.LogWritef
			span.Infof("[WRITE] ino(%v) deleteExtent(%v)", inode.Inode, len(inodeExts))
			deleteExtentsByPartition[dpID] = exts
		}

		allInodes = append(allInodes, inode)
	}

	var shouldCommit []*Inode
	var shouldRePushToFreeList []*Inode
	if proto.IsCold(mp.volType) {
		// delete ebs obj extents
		shouldCommit, shouldRePushToFreeList = mp.doBatchDeleteObjExtentsInEBS(ctx, allInodes)
		span.Infof("deleteInodeCnt(%d) shouldRePush(%d)", len(shouldCommit), len(shouldRePushToFreeList))
		for _, inode := range shouldRePushToFreeList {
			mp.freeList.Push(inode.Inode)
		}
		allInodes = shouldCommit
	}
	span.Infof("deleteExtentsByPartition(%v) allInodes(%v)", deleteExtentsByPartition, allInodes)
	shouldCommit, shouldRePushToFreeList = mp.batchDeleteExtentsByPartition(ctx, deleteExtentsByPartition, allInodes)
	bufSlice := make([]byte, 0, 8*len(shouldCommit))
	for _, inode := range shouldCommit {
		bufSlice = append(bufSlice, inode.MarshalKey()...)
	}

	err := mp.syncToRaftFollowersFreeInode(ctx, bufSlice)
	if err != nil {
		span.Warnf("raft commit inode list: %v, response %s", shouldCommit, err.Error())
	}

	for _, inode := range shouldCommit {
		if err == nil {
			mp.internalDeleteInode(inode)
		} else {
			mp.freeList.Push(inode.Inode)
		}
	}

	span.Infof("deleteInodeCnt(%v) inodeCnt(%v)", len(shouldCommit), mp.inodeTree.Len())
	for _, inode := range shouldRePushToFreeList {
		mp.freeList.Push(inode.Inode)
	}

	// try again.
	if len(shouldRePushToFreeList) > 0 && deleteWorkerSleepMs == 0 {
		time.Sleep(time.Duration(1000) * time.Millisecond)
	}
}

func (mp *metaPartition) syncToRaftFollowersFreeInode(ctx context.Context, hasDeleteInodes []byte) (err error) {
	if len(hasDeleteInodes) == 0 {
		return
	}
	_, err = mp.submit(ctx, opFSMInternalDeleteInode, hasDeleteInodes)
	return
}

func (mp *metaPartition) notifyRaftFollowerToFreeInodes(wg *sync.WaitGroup, target string, hasDeleteInodes []byte) (err error) {
	var conn *net.TCPConn
	conn, err = mp.config.ConnPool.GetConnect(target)
	defer func() {
		wg.Done()
		if err != nil {
			log.Warn(err.Error())
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

func (mp *metaPartition) doDeleteMarkedInodes(ctx context.Context, ext *proto.ExtentKey) (err error) {
	span := getSpan(ctx).WithOperation("doDeleteMarkedInodes")
	// get the data node view
	dp := mp.vol.GetPartition(ext.PartitionId)
	span.Debugf("dp(%v) status (%v)", dp.PartitionID, dp.Status)
	if dp == nil {
		if proto.IsCold(mp.volType) {
			span.Infof("ext(%s) is already been deleted, not delete any more", ext.String())
			return
		}
		err = errors.NewErrorf("unknown dataPartitionID=%d in vol", ext.PartitionId)
		return
	}

	// delete the data node
	if len(dp.Hosts) < 1 {
		span.Errorf("dp id(%v) is invalid, detail[%v]", ext.PartitionId, dp)
		err = errors.NewErrorf("dp id(%v) is invalid", ext.PartitionId)
		return
	}
	// NOTE: if all replicas in dp is dead
	// skip send request to dp leader
	if dp.Status == proto.Unavailable {
		return
	}
	addr := util.ShiftAddrPort(dp.Hosts[0], smuxPortShift)
	conn, err := smuxPool.GetConnect(addr)
	span.Infof("mp(%v) GetConnect (%v), ext(%s)", mp.config.PartitionId, addr, ext.String())

	defer func() {
		smuxPool.PutConnect(conn, ForceClosedConnect)
		span.Infof("mp(%v) PutConnect (%v), ext(%s)", mp.config.PartitionId, addr, ext.String())
	}()

	if err != nil {
		err = errors.NewErrorf("get conn from pool %s, extent(%s))", err.Error(), ext.String())
		return
	}
	var (
		p       *Packet
		invalid bool
	)
	if p, invalid = NewPacketToDeleteExtent(ctx, dp, ext); invalid {
		p.ResultCode = proto.OpOk
		return
	}
	if err = p.WriteToConn(conn); err != nil {
		err = errors.NewErrorf("write to dataNode %s, %s", p.GetUniqueLogId(), err.Error())
		return
	}

	if err = p.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
		err = errors.NewErrorf("read response from dataNode %s, %s", p.GetUniqueLogId(), err.Error())
		return
	}

	if p.ResultCode == proto.OpTryOtherAddr && proto.IsCold(mp.volType) {
		span.Infof("deleteOp retrun tryOtherAddr code means dp is deleted for LF vol, ext(%s)", ext.String())
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("[deleteMarkedInodes] %s response: %s", p.GetUniqueLogId(), p.GetResultMsg())
	}
	return
}

func (mp *metaPartition) doBatchDeleteExtentsByPartition(ctx context.Context, partitionID uint64, exts []*proto.ExtentKey) (err error) {
	// get the data node view
	span := getSpan(ctx).WithOperation("doBatchDeleteExtentsByPartition")
	dp := mp.vol.GetPartition(partitionID)
	if dp == nil {
		if proto.IsCold(mp.volType) {
			span.Infof("dp(%d) is already been deleted, not delete any more", partitionID)
			return
		}
		err = errors.NewErrorf("unknown dataPartitionID=%d in vol", partitionID)
		return
	}

	for _, ext := range exts {
		if ext.PartitionId != partitionID {
			err = errors.NewErrorf("BatchDeleteExtent do batchDelete on PartitionID(%v) but unexpect Extent(%v)", partitionID, ext)
			return
		}
	}

	// delete the data node
	if len(dp.Hosts) < 1 {
		span.Errorf("dp id(%v) is invalid, detail[%v]", partitionID, dp)
		err = errors.NewErrorf("dp id(%v) is invalid", partitionID)
		return
	}
	addr := util.ShiftAddrPort(dp.Hosts[0], smuxPortShift)
	conn, err := smuxPool.GetConnect(addr)
	span.Infof("mp(%v) GetConnect (%v)", mp.config.PartitionId, addr)

	ResultCode := proto.OpOk
	defer func() {
		smuxPool.PutConnect(conn, ForceClosedConnect)
		span.Infof("mp(%v) PutConnect (%v)", mp.config.PartitionId, addr)
	}()

	if err != nil {
		err = errors.NewErrorf("get conn from pool %s, extents partitionId=%d", err.Error(), partitionID)
		return
	}
	p := NewPacketToBatchDeleteExtent(dp, exts)
	if err = p.WriteToConn(conn); err != nil {
		err = errors.NewErrorf("write to dataNode %s, %s", p.GetUniqueLogId(), err.Error())
		return
	}
	if err = p.ReadFromConnWithVer(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		err = errors.NewErrorf("read response from dataNode %s, %s", p.GetUniqueLogId(), err.Error())
		return
	}

	ResultCode = p.ResultCode
	if ResultCode == proto.OpTryOtherAddr && proto.IsCold(mp.volType) {
		span.Infof("deleteOp retrun tryOtherAddr code means dp is deleted for LF vol, dp(%d)", partitionID)
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("[deleteMarkedInodes] %s response: %s", p.GetUniqueLogId(), p.GetResultMsg())
	}
	return
}

const maxDelCntOnce = 512

func (mp *metaPartition) doBatchDeleteObjExtentsInEBS(ctx context.Context, allInodes []*Inode) (shouldCommit []*Inode, shouldPushToFreeList []*Inode) {
	span := getSpan(ctx).WithOperation("doBatchDeleteObjExtentsInEBS")
	shouldCommit = make([]*Inode, 0, len(allInodes))
	shouldPushToFreeList = make([]*Inode, 0)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)

	for _, inode := range allInodes {
		wg.Add(1)

		inode.RLock()
		inode.ObjExtents.RLock()
		go func(ino *Inode, oeks []proto.ObjExtentKey) {
			defer wg.Done()
			span.Debugf("ino(%d) delObjEks[%d]", ino.Inode, len(oeks))
			err := mp.deleteObjExtents(ctx, oeks)

			lock.Lock()
			if err != nil {
				shouldPushToFreeList = append(shouldPushToFreeList, ino)
				span.Errorf("delete ebs eks fail, ino(%d), cnt(%d), err(%s)", ino.Inode, len(oeks), err.Error())
			} else {
				shouldCommit = append(shouldCommit, ino)
			}
			lock.Unlock()

			ino.ObjExtents.RUnlock()
			ino.RUnlock()
		}(inode, inode.ObjExtents.eks)
	}

	wg.Wait()

	return
}

func (mp *metaPartition) deleteObjExtents(ctx context.Context, oeks []proto.ObjExtentKey) (err error) {
	span := getSpan(ctx).WithOperation("deleteObjExtents")
	total := len(oeks)
	for i := 0; i < total; i += maxDelCntOnce {
		max := util.Min(i+maxDelCntOnce, total)
		err = mp.ebsClient.Delete(ctx, oeks[i:max])
		if err != nil {
			span.Errorf("delete ebs eks fail, cnt(%d), err(%s)", max-i, err.Error())
			return err
		}
	}
	return err
}

func (mp *metaPartition) recycleInodeDelFile(ctx context.Context) {
	span := getSpan(ctx).WithOperation("recycleInodeDelFile")
	// NOTE: get all files
	dentries, err := os.ReadDir(mp.config.RootDir)
	if err != nil {
		span.Errorf("mp(%v) failed to read dir(%v)", mp.config.PartitionId, mp.config.RootDir)
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
			span.Errorf("mp(%v) failed to get fs info", mp.config.PartitionId)
			return
		}
		diskSpaceLeft = int64(stat.Bavail * uint64(stat.Bsize))
		if diskSpaceLeft >= 50*util.GB && len(inodeDelFiles) < 5 {
			span.Debugf("mp(%v) not need to recycle, return", mp.config.PartitionId)
			return
		}
		// NOTE: delete a file and pop an item
		oldestFile := inodeDelFiles[len(inodeDelFiles)-1]
		inodeDelFiles = inodeDelFiles[:len(inodeDelFiles)-1]
		err = os.Remove(oldestFile)
		if err != nil {
			span.Errorf("mp(%v) failed to remove file(%v)", mp.config.PartitionId, oldestFile)
			return
		}
	}
}

func (mp *metaPartition) persistDeletedInode(ctx context.Context, ino uint64, currentSize *uint64) {
	span := getSpan(ctx).WithOperation("persistDeletedInode")
	if *currentSize >= _deleteInodeFileRollingSize {
		fileName := fmt.Sprintf("%v.%v.%v", DeleteInodeFileExtension, time.Now().Format(log.FileNameDateFormat), "old")
		if err := mp.delInodeFp.Sync(); err != nil {
			span.Errorf("failed to sync delete inode file, err(%v), inode(%v)", err, ino)
			return
		}
		mp.delInodeFp.Close()
		mp.delInodeFp = nil
		// NOTE: that is ok, if rename fails
		// we will re-open it in next line
		fileName = path.Join(mp.config.RootDir, fileName)
		err := os.Rename(path.Join(mp.config.RootDir, DeleteInodeFileExtension), fileName)
		if err != nil {
			span.Errorf("failed to rename delete inode file, err(%v)", err)
		} else {
			*currentSize = 0
			mp.recycleInodeDelFile(ctx)
		}
		if err = mp.openDeleteInodeFile(); err != nil {
			span.Errorf("failed to open delete inode file, err(%v), inode(%v)", err, ino)
			return
		}
	}
	// NOTE: += sizeof(uint64)
	*currentSize += 8
	if _, err := mp.delInodeFp.WriteString(fmt.Sprintf("%v\n", ino)); err != nil {
		span.Errorf("failed to persist ino(%v), err(%v)", ino, err)
		return
	}
}

func (mp *metaPartition) persistDeletedInodes(ctx context.Context, inos []uint64) {
	span := getSpan(ctx).WithOperation("persistDeletedInodes")
	span.Debugf("inos [%v]", inos)
	if mp.delInodeFp == nil {
		// NOTE: hope it can re-open file
		if err := mp.openDeleteInodeFile(); err != nil {
			span.Errorf("delete inode file is not open, err(%v), inodes(%v)", err, inos)
			return
		}
		span.Warn("re-open file success")
	}
	info, err := mp.delInodeFp.Stat()
	if err != nil {
		span.Errorf("failed to get size of delete inode file, err(%v), inodes(%v)", err, inos)
		return
	}
	currSize := uint64(info.Size())
	for _, ino := range inos {
		mp.persistDeletedInode(ctx, ino, &currSize)
	}
}
