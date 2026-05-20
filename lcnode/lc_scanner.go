// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"golang.org/x/time/rate"
)

const (
	pathSep = "/"
)

type LcScanner struct {
	ID             string
	Volume         string
	mw             MetaWrapper
	lcnode         *LcNode
	transitionMgr  *TransitionMgr
	adminTask      *proto.AdminTask
	rule           *proto.Rule
	dirChan        *unboundedchan.UnboundedChan
	fileChan       chan interface{}
	dirRPool       *routinepool.RoutinePool
	fileRPool      *routinepool.RoutinePool
	currentStat    *proto.LcNodeRuleTaskStatistics
	limiter        *rate.Limiter
	now            time.Time
	receiveStop    bool
	receiveStopC   chan bool
	stopC          chan bool
	scanMpFinished bool
}

func NewS3Scanner(adminTask *proto.AdminTask, l *LcNode) (*LcScanner, error) {
	request := adminTask.Request.(*proto.LcNodeRuleTaskRequest)
	scanTask := request.Task
	var err error

	metaConfig := &meta.MetaConfig{
		Volume:               scanTask.VolName,
		Masters:              l.masters,
		Authenticate:         false,
		ValidateOwner:        false,
		InnerReq:             true,
		MetaSendTimeout:      600,
		DisableTrashByClient: true,
	}
	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		log.LogErrorf("NewMetaWrapper err: %v", err)
		return nil, err
	}

	scanner := &LcScanner{
		ID:           scanTask.Id,
		Volume:       scanTask.VolName,
		lcnode:       l,
		mw:           metaWrapper,
		adminTask:    adminTask,
		rule:         scanTask.Rule,
		dirChan:      unboundedchan.NewUnboundedChan(defaultUnboundedChanInitCapacity),
		fileChan:     make(chan interface{}, simpleQueueInitCapacity),
		dirRPool:     routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		fileRPool:    routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		currentStat:  &proto.LcNodeRuleTaskStatistics{},
		limiter:      rate.NewLimiter(lcScanLimitPerSecond, defaultLcScanLimitBurst),
		now:          time.Now(),
		receiveStopC: make(chan bool),
		stopC:        make(chan bool),
	}

	var ebsClient *blobstore.BlobStoreClient
	var toEbs bool
	if scanner.rule.Transitions != nil {
		for _, sc := range scanner.rule.Transitions {
			if sc.StorageClass == proto.OpTypeStorageClassEBS {
				toEbs = true
			}
		}
	}
	if toEbs {
		ebsConfig := access.Config{
			ConnMode: access.NoLimitConnMode,
			Consul: access.ConsulConfig{
				Address: l.ebsAddr,
			},
			MaxSizePutOnce: MaxSizePutOnce,
			Logger: &access.Logger{
				Filename: path.Join(l.logDir, "ebs.log"),
			},
		}
		if ebsClient, err = blobstore.NewEbsClient(ebsConfig); err != nil {
			log.LogErrorf("NewEbsClient err: %v, rule id: %v", err, scanner.rule.ID)
			return nil, err
		}
		log.LogInfof("NewEbsClient success, %v", scanner.ID)
	}

	var volumeInfo *proto.SimpleVolView
	volumeInfo, err = l.mc.AdminAPI().GetVolumeSimpleInfo(scanner.Volume)
	if err != nil {
		log.LogErrorf("NewVolume: get volume info from master failed: volume(%v) err(%v)", scanner.Volume, err)
		return nil, err
	}
	if volumeInfo.Status == 1 {
		log.LogWarnf("NewVolume: volume has been marked for deletion: volume(%v) status(%v - 0:normal/1:markDelete)",
			scanner.Volume, volumeInfo.Status)
		return nil, proto.ErrVolNotExists
	}
	extentConfig := &stream.ExtentConfig{
		Volume:                      scanner.Volume,
		Masters:                     l.masters,
		FollowerRead:                false,
		OnAppendExtentKey:           metaWrapper.AppendExtentKey,
		OnSplitExtentKey:            metaWrapper.SplitExtentKey,
		OnGetExtents:                metaWrapper.GetExtents,
		OnTruncate:                  metaWrapper.Truncate,
		OnRenewalForbiddenMigration: metaWrapper.RenewalForbiddenMigration,
		VolStorageClass:             volumeInfo.VolStorageClass,
		VolAllowedStorageClass:      volumeInfo.AllowedStorageClass,
		OnForbiddenMigration:        metaWrapper.ForbiddenMigration,
		InnerReq:                    true,
		MetaWrapper:                 metaWrapper,
	}
	log.LogInfof("[NewS3Scanner] extentConfig: vol(%v) volStorageClass(%v) allowedStorageClass(%v), followerRead(%v)",
		extentConfig.Volume, extentConfig.VolStorageClass, extentConfig.VolAllowedStorageClass, extentConfig.FollowerRead)
	var extentClient *stream.ExtentClient
	if extentClient, err = stream.NewExtentClient(extentConfig); err != nil {
		log.LogErrorf("NewExtentClient err: %v", err)
		return nil, err
	}
	var extentClientForW *stream.ExtentClient
	if extentClientForW, err = stream.NewExtentClient(extentConfig); err != nil {
		log.LogErrorf("NewExtentClient err: %v", err)
		return nil, err
	}

	scanner.transitionMgr = &TransitionMgr{
		volume:    scanner.Volume,
		ec:        extentClient,
		ecForW:    extentClientForW,
		ebsClient: ebsClient,
		meta:      metaWrapper,
	}

	return scanner, nil
}

func (l *LcNode) startLcScan(adminTask *proto.AdminTask) (err error) {
	request := adminTask.Request.(*proto.LcNodeRuleTaskRequest)
	log.LogInfof("startLcScan: scan task(%v) received!", request.Task)
	resp := &proto.LcNodeRuleTaskResponse{}
	adminTask.Response = resp

	l.scannerMutex.Lock()
	if _, ok := l.lcScanners[request.Task.Id]; ok {
		log.LogInfof("startLcScan: scan task(%v) is already running!", request.Task)
		l.scannerMutex.Unlock()
		return
	}

	var scanner *LcScanner
	scanner, err = NewS3Scanner(adminTask, l)
	if err != nil {
		log.LogErrorf("startLcScan: NewS3Scanner err(%v)", err)
		resp.ID = request.Task.Id
		resp.Volume = request.Task.VolName
		resp.Rule = request.Task.Rule
		resp.LcNode = l.localServerAddr
		resp.Status = proto.TaskFailed
		resp.Done = true
		resp.StartErr = err.Error()
		l.scannerMutex.Unlock()
		return
	}
	l.lcScanners[scanner.ID] = scanner
	l.scannerMutex.Unlock()

	err = scanner.Start()
	auditlog.LogMasterOp("LcScanStart", fmt.Sprintf("ID(%v), from master(%v)", scanner.ID, request.MasterAddr), err)

	return
}

func (s *LcScanner) Start() (err error) {
	response := s.adminTask.Response.(*proto.LcNodeRuleTaskResponse)
	parentId, prefixDirs, err := s.FindPrefixInode()
	if err != nil {
		log.LogErrorf("startScan err(%v): volume(%v), rule id(%v), scanning done!",
			err, s.Volume, s.rule.ID)
		response.ID = s.ID
		response.LcNode = s.lcnode.localServerAddr
		response.StartTime = &s.now
		response.Volume = s.Volume
		response.Rule = s.rule
		response.Status = proto.TaskFailed
		response.Done = true
		response.StartErr = err.Error()

		s.lcnode.scannerMutex.Lock()
		s.Stop()
		delete(s.lcnode.lcScanners, s.ID)
		s.lcnode.scannerMutex.Unlock()
		return
	}

	go s.handleFileChan()
	go s.handleDirChan()

	response.StartTime = &s.now

	if s.rule.Filter != nil && s.rule.Filter.ByMp == proto.ScanByMp {
		go s.scanInodesByMp()
		go s.checkScanning()
		return
	}

	var currentPath string
	if len(prefixDirs) > 0 {
		currentPath = strings.Join(prefixDirs, pathSep)
	}

	firstDentry := &proto.ScanDentry{
		Inode: parentId,
		Path:  strings.TrimPrefix(currentPath, pathSep),
		Type:  uint32(os.ModeDir),
	}
	// response.StartTime = &s.now

	s.firstIn(firstDentry)

	go s.checkScanning()

	return
}

func (s *LcScanner) firstIn(d *proto.ScanDentry) {
	select {
	case <-s.stopC:
		log.LogInfof("receive stop, stop firstIn %v", s.ID)
		return
	default:
		s.dirChan.In <- d
		log.LogInfof("startScan(%v): first dir dentry(%v) in!", s.ID, d)
	}
}

func (s *LcScanner) FindPrefixInode() (inode uint64, prefixDirs []string, err error) {
	prefixDirs = make([]string, 0)
	prefix := s.rule.GetPrefix()

	var dirs []string
	if prefix != "" {
		dirs = strings.Split(prefix, "/")
		log.LogInfof("FindPrefixInode: volume(%v), prefix(%v), dirs(%v), len(%v)", s.Volume, prefix, dirs, len(dirs))
	}
	if len(dirs) <= 1 {
		return proto.RootIno, prefixDirs, nil
	}

	parentId := proto.RootIno
	for index, dir := range dirs {

		// Because lookup can only retrieve dentry whose name exactly matches,
		// so do not lookup the last part.
		if index+1 == len(dirs) {
			break
		}

		curIno, curMode, err := s.mw.Lookup_ll(parentId, dir, true)

		// If the part except the last part does not match exactly the same dentry, there is
		// no path matching the path prefix. An ENOENT error is returned to the caller.
		if err == syscall.ENOENT {
			log.LogErrorf("FindPrefixInode: find directories fail ENOENT: parentId(%v) dir(%v)", parentId, dir)
			return 0, nil, syscall.ENOENT
		}

		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("FindPrefixInode: find directories fail: prefix(%v) err(%v)", prefix, err)
			return 0, nil, err
		}

		// Because the file cannot have the next level members,
		// if there is a directory in the middle of the prefix,
		// it means that there is no file matching the prefix.
		if !os.FileMode(curMode).IsDir() {
			return 0, nil, syscall.ENOENT
		}

		prefixDirs = append(prefixDirs, dir)
		parentId = curIno
	}
	inode = parentId

	return
}

func (s *LcScanner) scanInodesByMp() {
	auditlog.LogMasterOp("LcScanStart", fmt.Sprintf("ID(%v), from master(%v)", s.ID, s.lcnode.localServerAddr), nil)
	log.LogInfof("scanInodesByMp: scan inodes by mp %v", s.ID)
	defer func() {
		log.LogInfof("scanInodesByMp: exit scan inodes by mp %v", s.ID)
		auditlog.LogMasterOp("LcScanFinish", fmt.Sprintf("ID(%v), from master(%v)", s.ID, s.lcnode.localServerAddr), nil)
		s.scanMpFinished = true
	}()

	rule := s.rule
	minSize := rule.MinSize()

	// Collect all unique FromPoolIds from transitions
	fromPoolIdMap := make(map[uint8]bool)
	if rule.Transitions != nil {
		for _, transition := range rule.Transitions {
			if transition.FromPoolId != 0 {
				fromPoolIdMap[transition.FromPoolId] = true
			}
		}
	}

	if len(fromPoolIdMap) == 0 {
		log.LogWarnf("scanInodesByMp: no valid FromPoolId found in transitions")
		return
	}

	// Get all meta partitions
	mps, err := s.lcnode.mc.ClientAPI().GetMetaPartitions(s.Volume)
	if err != nil {
		log.LogErrorf("scanInodesByMp: get meta partitions err %v", err)
		return
	}

	// Scan each mp for each fromPoolId
	for _, mp := range mps {
		for fromPoolId := range fromPoolIdMap {
			// Check if we should stop
			select {
			case <-s.stopC:
				log.LogInfof("scanInodesByMp: receive stop signal, exit")
				return
			default:
			}

			s.scanInodesByMpAndPool(mp.PartitionID, fromPoolId, minSize)
		}
	}
}

func (s *LcScanner) scanInodesByMpAndPool(partitionID uint64, poolId uint8, minSize uint64) {
	log.LogInfof("scanInodesByMpAndPool: partitionID(%v) poolId(%v) minSize(%v)", partitionID, poolId, minSize)

	var startInode uint64 = 0
	pageSize := uint32(10000) // Max page size

	for {
		// Check if we should stop
		select {
		case <-s.stopC:
			log.LogInfof("scanInodesByMpAndPool: receive stop signal, exit")
			return
		default:
		}

		// Build request
		req := &proto.ScanInodeByPoolRequest{
			PartitionID: partitionID,
			PoolId:      poolId,
			PageSize:    pageSize,
			StartInode:  startInode,
			MinSize:     minSize,
			CheckLease:  true,
		}

		// Call ScanInodeByPool
		resp, err := s.mw.ScanInodeByPool(req)
		if err != nil {
			log.LogWarnf("scanInodesByMpAndPool: ScanInodeByPool failed partitionID(%v) poolId(%v) startInode(%v) err: %v",
				partitionID, poolId, startInode, err)
			return
		}

		if resp == nil {
			log.LogDebugf("scanInodesByMpAndPool: no response partitionID(%v) poolId(%v)", partitionID, poolId)
			return
		}

		atomic.AddInt64(&s.currentStat.TotalMPScannedInodeNum, int64(resp.TotalScanned))

		if len(resp.Inodes) == 0 {
			log.LogDebugf("scanInodesByMpAndPool: no inodes found partitionID(%v) poolId(%v), resp(%v)", partitionID, poolId, resp.String())
			return
		}

		log.LogInfof("scanInodesByMpAndPool: scan inodes by mp and pool partitionID(%v) poolId(%v) inodes(%v), resp(%v)",
			partitionID, poolId, len(resp.Inodes), resp.String())

		// Convert InodeInfo to ScanDentry and send to fileChan
		for _, inode := range resp.Inodes {
			// Check if we should stop
			select {
			case <-s.stopC:
				log.LogInfof("scanInodesByMpAndPool: receive stop signal, exit")
				return
			default:
			}

			// Create ScanDentry from InodeInfo
			dentry := &proto.ScanDentry{
				Inode: inode,
				Path:  "",
			}

			// Send to fileChan
			select {
			case <-s.stopC:
				log.LogInfof("scanInodesByMpAndPool: receive stop signal, exit")
				return
			case s.fileChan <- dentry:
				// Successfully sent
			}
		}

		// Check if there are more inodes to scan
		if !resp.HasMore || resp.NextInode == 0 {
			log.LogDebugf("scanInodesByMpAndPool: no more inodes partitionID(%v) poolId(%v) nextInode(%v) hasMore(%v)",
				partitionID, poolId, resp.NextInode, resp.HasMore)
			return
		}

		// Update startInode for next page
		startInode = resp.NextInode
		log.LogDebugf("scanInodesByMpAndPool: continue scanning partitionID(%v) poolId(%v) nextInode(%v) totalScanned(%v)",
			partitionID, poolId, startInode, resp.TotalScanned)
	}
}

func (s *LcScanner) handleFileChan() {
	log.LogInfof("Enter handleFileChan, %+v", s)
	defer func() {
		log.LogInfof("Exit handleFileChan, %+v", s)
	}()

	prefix := s.rule.GetPrefix()

	for {
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop handleFileChan %v", s.ID)
			return
		case val, ok := <-s.fileChan:
			if !ok {
				log.LogWarnf("fileChan closed, id(%v)", s.ID)
				return
			}
			dentry := val.(*proto.ScanDentry)
			if !strings.HasPrefix(dentry.Path, prefix) {
				continue
			}

			job := func() {
				s.handleFile(dentry)
			}
			_, err := s.fileRPool.Submit(job)
			if err != nil {
				log.LogWarnf("fileRPool.Submit err(%v), id(%v)", err, s.ID)
			}
		}
	}
}

func (s *LcScanner) handleDirChan() {
	log.LogInfof("Enter handleDirChan, %+v", s)
	defer func() {
		log.LogInfof("Exit handleDirChan, %+v", s)
	}()

	for {
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop handleDirChan %v", s.ID)
			return
		case val, ok := <-s.dirChan.Out:
			if !ok {
				log.LogWarnf("dirChan closed, id(%v)", s.ID)
				return
			}
			dentry := val.(*proto.ScanDentry)

			var job func()
			if s.dirChan.Len() > maxDirChanNum {
				job = func() {
					s.handleDirLimitDepthFirst(dentry)
				}
			} else {
				job = func() {
					s.handleDirLimitBreadthFirst(dentry)
				}
			}
			_, err := s.dirRPool.Submit(job)
			if err != nil {
				log.LogWarnf("dirRPool.Submit err(%v), id(%v)", err, s.ID)
			}
		}
	}
}

func (s *LcScanner) handleFile(dentry *proto.ScanDentry) {
	log.LogInfof("handleFile: %v, fileChan: %v", dentry, len(s.fileChan))
	atomic.AddInt64(&s.currentStat.TotalFileScannedNum, 1)

	s.limiter.Wait(context.Background())
	start := time.Now()

	// Get inode info from meta again
	info, err := s.mw.InodeGet_ll(dentry.Inode, true)
	if err != nil {
		log.LogWarnf("handleFile InodeGet_ll err: %v, dentry: %+v", err, dentry)
		return
	}

	if info != nil && info.Size < s.rule.MinSize() {
		log.LogInfof("handleFile: %+v, minSize(%d) size(%v) no need to process", dentry, s.rule.MinSize(), info.Size)
		return
	}

	op := s.inodeExpired(info, s.rule.Expiration, s.rule.Transitions, dentry)
	dentry.Op = op

	if op == "" {
		log.LogInfof("handleFile: %+v, ctime(%v), atime(%v), is not expired", dentry, info.CreateTime, info.AccessTime)
		return
	}

	atomic.AddInt64(&s.currentStat.TotalFileExpiredNum, 1)
	log.LogInfof("handleFile: %+v, ctime(%v), atime(%v), is expired", dentry, info.CreateTime, info.AccessTime)

	defer func() {
		auditlog.LogLcNodeOp(op, s.Volume, dentry.Name, dentry.Path, dentry.ParentId, dentry.Inode, dentry.Size, dentry.LeaseExpire,
			dentry.HasMek, dentry.SrcPoolId, dentry.DstPoolId, time.Since(start).Milliseconds(), err)
	}()

	if info.IsDeletingMigrationExtent() {
		log.LogInfof("handleFile: %+v, is deleting migration extent, skip", dentry)
		err = fmt.Errorf("skip (%v), inode is deleting migration extent", info.String())
		atomic.AddInt64(&s.currentStat.ExpiredSkipNum, 1)
		return
	}

	switch op {
	case proto.OpTypeDelete:
		_, err = s.mw.DeleteWithCond_ll(dentry.ParentId, dentry.Inode, dentry.Name, os.FileMode(dentry.Type).IsDir(), dentry.Path, true)
		if err != nil {
			atomic.AddInt64(&s.currentStat.ErrorDeleteNum, 1)
			log.LogWarnf("delete DeleteWithCond_ll err: %v, dentry: %+v", err, dentry)
			return
		}
		if err = s.mw.Evict(dentry.Inode, dentry.Path, false); err != nil {
			log.LogWarnf("delete Evict err: %v, dentry: %+v", err, dentry)
		}
		atomic.AddInt64(&s.currentStat.ExpiredDeleteNum, 1)

	case proto.OpTypeStorageClassHDD:
		if dentry.HasMek {
			if err = s.mw.DeleteMigrationExtentKey(dentry.Inode, dentry.Path); err != nil {
				log.LogErrorf("DeleteMigrationExtentKey err: %v, dentry: %+v", err, dentry)
			}
			err = fmt.Errorf("skip (%v)", "inode has mek")
			atomic.AddInt64(&s.currentStat.ExpiredSkipNum, 1)
			return
		}
		err = s.transitionMgr.migrate(dentry)
		if err != nil {
			if isSkipErr(err) {
				err = fmt.Errorf("skip (%v)", err)
				atomic.AddInt64(&s.currentStat.ExpiredSkipNum, 1)
				return
			}
			atomic.AddInt64(&s.currentStat.ErrorMToHddNum, 1)
			atomic.AddInt64(&s.currentStat.ErrorMNum, 1)
			log.LogErrorf("migrate err: %v, dentry: %+v", err, dentry)
			return
		}
		// Use DelayDelMinute from dentry, or system default if not set
		delayDel := dentry.DelayDelMinute
		if delayDel == 0 {
			delayDel = delayDelMinute // Use system default from config
		}
		err = s.mw.UpdateExtentKeyAfterMigration(dentry.Inode, proto.OpTypeToStorageType(op), nil, dentry.DstPoolId, dentry.LeaseExpire, delayDel, dentry.Path)
		if err != nil {
			if isSkipErr(err) {
				err = fmt.Errorf("skip (%v)", err)
				atomic.AddInt64(&s.currentStat.ExpiredSkipNum, 1)
				return
			}
			atomic.AddInt64(&s.currentStat.ErrorMToHddNum, 1)
			atomic.AddInt64(&s.currentStat.ErrorMNum, 1)
			err = fmt.Errorf("UpdateExtentKeyAfterMigration err(%v)", err)
			log.LogErrorf("%v, dentry: %+v", err, dentry)
			return
		}

		atomic.AddInt64(&s.currentStat.ExpiredMToHddNum, 1)
		atomic.AddInt64(&s.currentStat.ExpiredMNum, 1)
		atomic.AddInt64(&s.currentStat.ExpiredMToHddBytes, int64(dentry.Size))
		atomic.AddInt64(&s.currentStat.ExpiredMBytes, int64(dentry.Size))

	case proto.OpTypeStorageClassEBS:
		if dentry.HasMek {
			if err = s.mw.DeleteMigrationExtentKey(dentry.Inode, dentry.Path); err != nil {
				log.LogErrorf("DeleteMigrationExtentKey err: %v, dentry: %+v", err, dentry)
			}
			err = fmt.Errorf("skip (%v)", "inode has mek")
			atomic.AddInt64(&s.currentStat.ExpiredSkipNum, 1)
			return
		}
		var oek []proto.ObjExtentKey
		oek, err = s.transitionMgr.migrateToEbs(dentry)
		if err != nil {
			if isSkipErr(err) {
				err = fmt.Errorf("skip (%v)", err)
				atomic.AddInt64(&s.currentStat.ExpiredSkipNum, 1)
				return
			}
			atomic.AddInt64(&s.currentStat.ErrorMToBlobstoreNum, 1)
			log.LogErrorf("migrate blobstore err: %v, dentry: %+v", err, dentry)
			return
		}
		// Use DelayDelMinute from dentry, or system default if not set
		delayDel := dentry.DelayDelMinute
		if delayDel == 0 {
			delayDel = delayDelMinute // Use system default from config
		}
		err = s.mw.UpdateExtentKeyAfterMigration(dentry.Inode, proto.OpTypeToStorageType(op), oek, dentry.DstPoolId, dentry.LeaseExpire, delayDel, dentry.Path)
		if err != nil {
			if isSkipErr(err) {
				err = fmt.Errorf("skip (%v)", err)
				atomic.AddInt64(&s.currentStat.ExpiredSkipNum, 1)
				return
			}
			atomic.AddInt64(&s.currentStat.ErrorMToBlobstoreNum, 1)
			err = fmt.Errorf("UpdateExtentKeyAfterMigration err(%v)", err)
			log.LogErrorf("%v, dentry: %+v", err, dentry)
			return
		}
		atomic.AddInt64(&s.currentStat.ExpiredMToBlobstoreNum, 1)
		atomic.AddInt64(&s.currentStat.ExpiredMToBlobstoreBytes, int64(dentry.Size))

	default:
		log.LogWarnf("invalid op: %v", dentry)
	}
}

func isSkipErr(err error) bool {
	if strings.Contains(err.Error(), "statusLeaseOccupiedByOthers") {
		return true
	}
	if strings.Contains(err.Error(), "statusLeaseGenerationNotMatch") {
		return true
	}
	if strings.Contains(err.Error(), "can not find inode") {
		return true
	}
	if strings.Contains(err.Error(), "no such file or directory") {
		return true
	}
	if strings.Contains(err.Error(), "ExtentNotFoundError") {
		return true
	}
	if strings.Contains(err.Error(), "file modified when migrating") {
		return true
	}
	if strings.Contains(err.Error(), "NotExistErr") {
		return true
	}
	return false
}

func (s *LcScanner) inodeExpired(info *proto.InodeInfo, condE *proto.Expiration, condT []*proto.Transition, dentry *proto.ScanDentry) (op string) {
	if info == nil {
		log.LogInfof("inodeExpired: inode not found, dentry: %+v", dentry)
		return
	}

	dentry.Size = info.Size
	dentry.StorageClass = info.StorageClass
	dentry.LeaseExpire = info.LeaseExpireTime
	dentry.HasMek = info.HasMigrationEk
	dentry.InodeInfo = info

	if info.ForbiddenLc {
		log.LogWarnf("ForbiddenLc, lease is occupied, inode: %+v, LeaseExpireTime(%v)", info.Inode, info.LeaseExpireTime)
		return
	}

	// execute expiration priority
	if condE != nil {
		if expired(info, s.now.Unix(), condE.Days, condE.Date) {
			op = proto.OpTypeDelete
			return
		}
	}

	for _, cond := range condT {
		if info.PoolId != cond.FromPoolId {
			continue
		}

		if expired(info, s.now.Unix(), cond.Days, cond.Date) {
			op = proto.OpTypeStorageClassHDD
			dentry.DstPoolId = cond.ToPoolId
			dentry.SrcPoolId = cond.FromPoolId
			// Set DelayDelMinute from transition, or use system default if not specified
			if cond.DelayDelMinute != nil {
				dentry.DelayDelMinute = *cond.DelayDelMinute
			} else {
				// Use system default from config (default is 7 days = 10080 minutes)
				dentry.DelayDelMinute = delayDelMinute
			}
			return
		}
	}
	return
}

func expired(inode *proto.InodeInfo, now int64, days *int, date *time.Time) bool {
	if days != nil && *days > 0 {
		// Avoid the impact of time jitter between nodes
		if inode.AccessTime.Add(time.Second * 10).Before(inode.CreateTime) {
			log.LogWarnf("AccessTime before CreateTime, skip, inode: %+v, LeaseExpireTime(%v), AccessTime(%v), CreateTime(%v)", inode, inode.LeaseExpireTime, inode.AccessTime, inode.CreateTime)
			return false
		}

		inodeTime := inode.AccessTime.Unix()
		if useCreateTime {
			inodeTime = inode.CreateTime.Unix()
		}
		if now-inodeTime > int64(*days*24*60*60) {
			return true
		}
	}
	if date != nil {
		if now > date.Unix() {
			return true
		}
	}
	return false
}

// scan dir tree in depth when size of dirChan.In grow too much.
// consider 40 Bytes is the ave size of dentry, 100 million ScanDentries may take up to around 4GB of Memory
func (s *LcScanner) handleDirLimitDepthFirst(dentry *proto.ScanDentry) {
	log.LogInfof("handleDirLimitDepthFirst dentry: %+v, dirChan.Len: %v", dentry, s.dirChan.Len())

	if dentry.Name == DirTrashSkip {
		log.LogInfof("handleDirLimitDepthFirst skip read dir %+v", dentry)
		return
	}

	marker := ""
	done := false
	for !done {
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop handleDirLimitDepthFirst %v", s.ID)
			return
		default:
		}

		children, err := s.mw.ReadDirLimit_ll(dentry.Inode, marker, uint64(defaultReadDirLimit), true)
		if err != nil && err != syscall.ENOENT {
			atomic.AddInt64(&s.currentStat.ErrorReadDirNum, 1)
			log.LogErrorf("handleDirLimitDepthFirst ReadDirLimit_ll err(%v), dentry(%v), marker(%v)", err, dentry, marker)
			return
		}

		if marker == "" {
			atomic.AddInt64(&s.currentStat.TotalDirScannedNum, 1)
		}

		if err == syscall.ENOENT {
			break
		}

		if marker != "" {
			if len(children) >= 1 && marker == children[0].Name {
				if len(children) <= 1 {
					break
				} else {
					children = children[1:]
				}
			}
		}

		scanDentries := s.batchGetFileInodeInfo(dentry.Inode, children, dentry.Path)

		files := make([]*proto.ScanDentry, 0)
		dirs := make([]*proto.ScanDentry, 0)
		for _, dentry := range scanDentries {

			childDentry := dentry
			if os.FileMode(childDentry.Type).IsDir() {
				dirs = append(dirs, childDentry)
			} else {
				files = append(files, childDentry)
			}
		}

		for _, file := range files {
			s.fileChan <- file
		}
		for _, dir := range dirs {
			s.handleDirLimitDepthFirst(dir)
		}

		childrenNr := len(children)
		if (marker == "" && childrenNr < defaultReadDirLimit) || (marker != "" && childrenNr+1 < defaultReadDirLimit) {
			done = true
		} else {
			marker = children[childrenNr-1].Name
		}

	}
}

func (s *LcScanner) handleDirLimitBreadthFirst(dentry *proto.ScanDentry) {
	log.LogInfof("handleDirLimitBreadthFirst dentry: %+v, dirChan.Len: %v", dentry, s.dirChan.Len())

	if dentry.Name == DirTrashSkip {
		log.LogInfof("handleDirLimitBreadthFirst skip read dir %+v", dentry)
		return
	}

	marker := ""
	done := false
	for !done {
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop handleDirLimitBreadthFirst %v", s.ID)
			return
		default:
		}

		children, err := s.mw.ReadDirLimit_ll(dentry.Inode, marker, uint64(defaultReadDirLimit), true)
		if err != nil && err != syscall.ENOENT {
			atomic.AddInt64(&s.currentStat.ErrorReadDirNum, 1)
			log.LogErrorf("handleDirLimitBreadthFirst ReadDirLimit_ll err(%v), dentry(%v), marker(%v)", err, dentry, marker)
			return
		}

		if marker == "" {
			atomic.AddInt64(&s.currentStat.TotalDirScannedNum, 1)
		}

		if err == syscall.ENOENT {
			break
		}

		if marker != "" {
			if len(children) >= 1 && marker == children[0].Name {
				if len(children) <= 1 {
					break
				} else {
					children = children[1:]
				}
			}
		}

		scanDentries := s.batchGetFileInodeInfo(dentry.Inode, children, dentry.Path)
		for _, dentry := range scanDentries {
			childDentry := dentry
			if !os.FileMode(childDentry.Type).IsDir() {
				s.fileChan <- childDentry
			} else {
				s.dirChan.In <- childDentry
			}
		}

		childrenNr := len(children)
		if (marker == "" && childrenNr < defaultReadDirLimit) || (marker != "" && childrenNr+1 < defaultReadDirLimit) {
			done = true
		} else {
			marker = children[childrenNr-1].Name
		}

	}
}

func (s *LcScanner) checkScanning() {
	dur := time.Second * time.Duration(scanCheckInterval)
	taskCheckTimer := time.NewTimer(dur)
	for {
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop checkScanning %v", s.ID)
			return
		case <-s.receiveStopC:
			log.LogInfof("receive receiveStopC %v", s.ID)
			s.receiveStop = true
			s.Stop()

			t := time.Now()
			response := s.adminTask.Response.(*proto.LcNodeRuleTaskResponse)
			response.EndTime = &t
			response.Status = proto.TaskSucceeds
			response.Done = true
			response.ID = s.ID
			response.LcNode = s.lcnode.localServerAddr
			response.Volume = s.Volume
			response.RcvStop = s.receiveStop
			response.Rule = s.rule
			response.ExpiredDeleteNum = s.currentStat.ExpiredDeleteNum
			response.ExpiredMToHddNum = s.currentStat.ExpiredMToHddNum
			response.ExpiredMNum = s.currentStat.ExpiredMNum
			response.ExpiredMToBlobstoreNum = s.currentStat.ExpiredMToBlobstoreNum
			response.ExpiredMBytes = s.currentStat.ExpiredMBytes
			response.ExpiredMToHddBytes = s.currentStat.ExpiredMToHddBytes
			response.ExpiredMToBlobstoreBytes = s.currentStat.ExpiredMToBlobstoreBytes
			response.ExpiredSkipNum = s.currentStat.ExpiredSkipNum
			response.TotalFileScannedNum = s.currentStat.TotalFileScannedNum
			response.TotalFileExpiredNum = s.currentStat.TotalFileExpiredNum
			response.TotalDirScannedNum = s.currentStat.TotalDirScannedNum
			response.TotalMPScannedInodeNum = s.currentStat.TotalMPScannedInodeNum
			response.ErrorDeleteNum = s.currentStat.ErrorDeleteNum
			response.ErrorMToHddNum = s.currentStat.ErrorMToHddNum
			response.ErrorMNum = s.currentStat.ErrorMNum
			response.ErrorMToBlobstoreNum = s.currentStat.ErrorMToBlobstoreNum
			response.ErrorReadDirNum = s.currentStat.ErrorReadDirNum

			log.LogInfof("receive receiveStopC response(%+v)", response)

			s.lcnode.scannerMutex.Lock()
			delete(s.lcnode.lcScanners, s.ID)
			s.lcnode.scannerMutex.Unlock()
			log.LogInfof("receive receiveStopC already stop %v", s.ID)

			s.lcnode.respondToMaster(s.adminTask)
			return
		case <-taskCheckTimer.C:
			if s.DoneScanning() {
				log.LogInfof("checkScanning completed for task(%v)", s.adminTask)
				taskCheckTimer.Stop()
				t := time.Now()
				response := s.adminTask.Response.(*proto.LcNodeRuleTaskResponse)
				response.EndTime = &t
				response.Status = proto.TaskSucceeds
				response.Done = true
				response.ID = s.ID
				response.LcNode = s.lcnode.localServerAddr
				response.Volume = s.Volume
				response.Rule = s.rule
				response.ExpiredDeleteNum = s.currentStat.ExpiredDeleteNum
				response.ExpiredMToHddNum = s.currentStat.ExpiredMToHddNum
				response.ExpiredMNum = s.currentStat.ExpiredMNum
				response.ExpiredMBytes = s.currentStat.ExpiredMBytes
				response.ExpiredMToBlobstoreNum = s.currentStat.ExpiredMToBlobstoreNum
				response.ExpiredMToHddBytes = s.currentStat.ExpiredMToHddBytes
				response.ExpiredMToBlobstoreBytes = s.currentStat.ExpiredMToBlobstoreBytes
				response.ExpiredSkipNum = s.currentStat.ExpiredSkipNum
				response.TotalFileScannedNum = s.currentStat.TotalFileScannedNum
				response.TotalFileExpiredNum = s.currentStat.TotalFileExpiredNum
				response.TotalDirScannedNum = s.currentStat.TotalDirScannedNum
				response.TotalMPScannedInodeNum = s.currentStat.TotalMPScannedInodeNum
				response.ErrorDeleteNum = s.currentStat.ErrorDeleteNum
				response.ErrorMToHddNum = s.currentStat.ErrorMToHddNum
				response.ErrorMNum = s.currentStat.ErrorMNum
				response.ErrorMToBlobstoreNum = s.currentStat.ErrorMToBlobstoreNum
				response.ErrorReadDirNum = s.currentStat.ErrorReadDirNum
				log.LogInfof("checkScanning completed response(%+v)", response)

				s.lcnode.scannerMutex.Lock()
				// ensure stop only once if heartbeat timeout now
				if _, ok := s.lcnode.lcScanners[s.ID]; ok {
					s.Stop()
					delete(s.lcnode.lcScanners, s.ID)
				}
				s.lcnode.scannerMutex.Unlock()

				s.lcnode.respondToMaster(s.adminTask)
				return
			}
			taskCheckTimer.Reset(dur)
		}
	}
}

func (s *LcScanner) DoneScanning() bool {
	log.LogInfof("dirChan.Len(%v) fileChan.Len(%v) fileRPool.RunningNum(%v) dirRPool.RunningNum(%v) scanMpFinished(%v)",
		s.dirChan.Len(), len(s.fileChan), s.fileRPool.RunningNum(), s.dirRPool.RunningNum(), s.scanMpFinished)
	return s.dirChan.Len() == 0 && len(s.fileChan) == 0 && s.fileRPool.RunningNum() == 0 && s.dirRPool.RunningNum() == 0 && s.DoneScanningMp()
}

func (s *LcScanner) DoneScanningMp() bool {
	if s.rule.Filter == nil || s.rule.Filter.ByMp != proto.ScanByMp {
		return true
	}

	return s.scanMpFinished
}

func (s *LcScanner) Stop() {
	start := time.Now()
	close(s.stopC)
	s.clearFileChan() // clear fileChan avoid blocking dirRPool
	s.fileRPool.WaitAndClose()
	s.dirRPool.WaitAndClose()
	close(s.dirChan.In)
	close(s.fileChan)
	s.mw.Close()
	s.transitionMgr.ec.Close()
	s.transitionMgr.ecForW.Close()
	log.LogInfof("stop: scanner(%v) stopped", s.ID)
	auditlog.LogMasterOp("LcScanStop ", fmt.Sprintf("ID(%v), receiveStop(%v), %v", s.ID, s.receiveStop, time.Since(start).String()), nil)
}

func (s *LcScanner) clearFileChan() {
	var num int
	for {
		select {
		case <-s.fileChan:
			num++
		default:
			log.LogInfof("stop: clearFileChan clear num(%v)", num)
			return
		}
	}
}

// batchGetFileInodeInfo builds ScanDentry list from ReadDirLimit_ll result without prefetching inode info.
func (s *LcScanner) batchGetFileInodeInfo(parentId uint64, dentries []proto.Dentry, parentPath string) []*proto.ScanDentry {
	if len(dentries) == 0 {
		return make([]*proto.ScanDentry, 0)
	}

	// Build ScanDentry list for all dentries (files and dirs)
	result := make([]*proto.ScanDentry, 0, len(dentries))
	for i := range dentries {
		child := &dentries[i]
		childPath := strings.TrimPrefix(parentPath+pathSep+child.Name, pathSep)

		scanDentry := &proto.ScanDentry{
			ParentId: parentId,
			Inode:    child.Inode,
			Name:     child.Name,
			Path:     childPath,
			Type:     child.Type,
		}

		result = append(result, scanDentry)
	}

	return result
}
