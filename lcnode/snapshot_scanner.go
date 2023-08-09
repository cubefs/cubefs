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
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
)

const (
	SnapScanTypeOnlyFile        int = 1
	SnapScanTypeOnlyDirAndDepth int = 2
)

type SnapshotScanner struct {
	ID          string
	Volume      string
	mw          MetaWrapper
	lcnode      *LcNode
	adminTask   *proto.AdminTask
	verDelReq   *proto.SnapshotVerDelTaskRequest
	inodeChan   *unboundedchan.UnboundedChan
	rPoll       *routinepool.RoutinePool
	currentStat *proto.SnapshotStatistics
	scanType    int
	stopC       chan bool
}

func NewSnapshotScanner(adminTask *proto.AdminTask, l *LcNode) (*SnapshotScanner, error) {
	request := adminTask.Request.(*proto.SnapshotVerDelTaskRequest)
	var err error
	var metaConfig = &meta.MetaConfig{
		Volume:        request.Task.VolName,
		Masters:       l.masters,
		Authenticate:  false,
		ValidateOwner: false,
	}

	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return nil, err
	}

	scanner := &SnapshotScanner{
		ID:          request.Task.Key(),
		Volume:      request.Task.VolName,
		mw:          metaWrapper,
		lcnode:      l,
		adminTask:   adminTask,
		verDelReq:   request,
		inodeChan:   unboundedchan.NewUnboundedChan(defaultUnboundedChanInitCapacity),
		rPoll:       routinepool.NewRoutinePool(snapshotRoutineNumPerTask),
		currentStat: &proto.SnapshotStatistics{},
		stopC:       make(chan bool, 0),
	}
	return scanner, nil
}

func (l *LcNode) startSnapshotScan(adminTask *proto.AdminTask) (err error) {
	request := adminTask.Request.(*proto.SnapshotVerDelTaskRequest)
	log.LogInfof("startSnapshotScan: scan task(%v) received!", request.Task)
	response := &proto.SnapshotVerDelTaskResponse{}
	adminTask.Response = response

	l.scannerMutex.Lock()
	if _, ok := l.snapshotScanners[request.Task.Key()]; ok {
		log.LogInfof("startSnapshotScan: scan task(%v) is already running!", request.Task)
		l.scannerMutex.Unlock()
		return
	}

	var scanner *SnapshotScanner
	scanner, err = NewSnapshotScanner(adminTask, l)
	if err != nil {
		log.LogErrorf("startSnapshotScan: NewSnapshotScanner err(%v)", err)
		response.Status = proto.TaskFailed
		response.Result = err.Error()
		l.scannerMutex.Unlock()
		return
	}
	l.snapshotScanners[request.Task.Key()] = scanner
	l.scannerMutex.Unlock()

	go scanner.Start()
	return
}

func (s *SnapshotScanner) getTaskVerSeq() uint64 {
	return s.verDelReq.Task.VerSeq
}

func (s *SnapshotScanner) Stop() {
	close(s.stopC)
	s.rPoll.WaitAndClose()
	close(s.inodeChan.In)
	log.LogDebugf("snapshot scanner(%v) stopped", s.ID)
}

func (s *SnapshotScanner) Start() {
	response := s.adminTask.Response.(*proto.SnapshotVerDelTaskResponse)
	t := time.Now()
	response.StartTime = &t

	//1. delete all files
	log.LogDebugf("snapshot startScan: first round files start!")
	s.scanType = SnapScanTypeOnlyFile
	go s.scan()
	prefixDentry := &proto.ScanDentry{
		Inode: proto.RootIno,
		Type:  proto.Mode(os.ModeDir),
	}
	log.LogDebugf("snapshot startScan: scan type(%v), first dir entry(%v) in!", s.scanType, prefixDentry)
	s.inodeChan.In <- prefixDentry
	s.checkScanning(false)

	//2. delete all dirs
	log.LogDebugf("snapshot startScan: second round dirs start!")
	s.scanType = SnapScanTypeOnlyDirAndDepth
	log.LogDebugf("snapshot startScan: scan type(%v), first dir entry(%v) in!", s.scanType, prefixDentry)
	s.inodeChan.In <- prefixDentry
	s.checkScanning(true)
}

func (s *SnapshotScanner) getDirJob(dentry *proto.ScanDentry) (job func()) {
	switch s.scanType {
	case SnapScanTypeOnlyDirAndDepth:
		log.LogDebug("getDirJob: SnapScanTypeOnlyDirAndDepth")
		job = func() {
			s.handlVerDelDepthFirst(dentry)
		}
	case SnapScanTypeOnlyFile:
		if s.inodeChan.Len() > maxDirChanNum {
			log.LogDebug("getDirJob: SnapScanTypeOnlyFile DepthFirst")
			job = func() {
				s.handlVerDelDepthFirst(dentry)
			}
		} else {
			log.LogDebug("getDirJob: SnapScanTypeOnlyFile BreadthFirst")
			job = func() {
				s.handlVerDelBreadthFirst(dentry)
			}
		}
	default:
		log.LogErrorf("getDirJob: invalid scanType: %v", s.scanType)
	}
	return
}

func (s *SnapshotScanner) scan() {
	log.LogDebugf("SnapshotScanner Enter scan")
	defer func() {
		log.LogDebugf("SnapshotScanner Exit scan")
	}()
	for {
		select {
		case <-s.stopC:
			return
		case val, ok := <-s.inodeChan.Out:
			if !ok {
				log.LogWarnf("inodeChan closed")
			} else {
				dentry := val.(*proto.ScanDentry)
				job := s.getDirJob(dentry)
				_, err := s.rPoll.Submit(job)
				if err != nil {
					log.LogErrorf("handlVerDel failed, err(%v)", err)
				}
			}
		}
	}
}

func (s *SnapshotScanner) handlVerDelDepthFirst(dentry *proto.ScanDentry) {
	var (
		children []proto.Dentry
		ino      *proto.InodeInfo
		err      error
	)
	onlyDir := s.scanType == SnapScanTypeOnlyDirAndDepth

	if os.FileMode(dentry.Type).IsDir() {
		marker := ""
		done := false

		for !done {
			children, err = s.mw.ReadDirLimitByVer(dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), onlyDir)
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("action[handlVerDelDepthFirst] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
					dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), err)
				return
			}
			log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimitByVer parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
				dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), len(children))

			if err == syscall.ENOENT {
				done = true
				log.LogErrorf("action[handlVerDelDepthFirst] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
					dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), err)
				break
			}

			if marker != "" {
				if len(children) >= 1 && marker == children[0].Name {
					if len(children) <= 1 {
						done = true
						log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll done, parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
							dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), children)
						break
					} else {
						skippedChild := children[0]
						children = children[1:]
						log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll skip last marker[%v], parent[%v] verSeq[%v] skippedName[%v]",
							marker, dentry.Inode, s.getTaskVerSeq(), skippedChild.Name)
					}
				}
			}

			files := make([]*proto.ScanDentry, 0)
			dirs := make([]*proto.ScanDentry, 0)

			for _, child := range children {
				childDentry := &proto.ScanDentry{
					ParentId: dentry.Inode,
					Name:     child.Name,
					Inode:    child.Inode,
					Type:     child.Type,
				}
				if os.FileMode(childDentry.Type).IsDir() {
					dirs = append(dirs, childDentry)
				} else {
					files = append(files, childDentry)
				}
			}

			for _, file := range files {
				if ino, err = s.mw.Delete_Ver_ll(file.ParentId, file.Name, false, s.getTaskVerSeq()); err != nil {
					log.LogErrorf("action[handlVerDelDepthFirst] Delete_Ver_ll failed, file(parent[%v] child name[%v]) verSeq[%v] err[%v]",
						file.ParentId, file.Name, s.getTaskVerSeq(), err)
					atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
				} else {
					log.LogDebugf("action[handlVerDelDepthFirst] Delete_Ver_ll success, file(parent[%v] child name[%v]) verSeq[%v] ino[%v]",
						file.ParentId, file.Name, s.getTaskVerSeq(), ino)
					atomic.AddInt64(&s.currentStat.FileNum, 1)
					atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
				}
			}

			for _, dir := range dirs {
				s.handlVerDelDepthFirst(dir)
			}

			childrenNr := len(children)
			if (marker == "" && childrenNr < snapshotRoutineNumPerTask) || (marker != "" && childrenNr+1 < snapshotRoutineNumPerTask) {
				log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll done, parent[%v]",
					dentry.Inode)
				done = true
			} else {
				marker = children[childrenNr-1].Name
				log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll next marker[%v] parent[%v]", marker, dentry.Inode)
			}
		}

	}

	if onlyDir {
		if ino, err = s.mw.Delete_Ver_ll(dentry.ParentId, dentry.Name, os.FileMode(dentry.Type).IsDir(), s.getTaskVerSeq()); err != nil {
			if dentry.ParentId >= 1 {
				log.LogErrorf("action[handlVerDelDepthFirst] Delete_Ver_ll failed, dir(parent[%v] child name[%v]) verSeq[%v] err[%v]",
					dentry.ParentId, dentry.Name, s.getTaskVerSeq(), err)
				atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
			}
		} else {
			log.LogDebugf("action[handlVerDelDepthFirst] Delete_Ver_ll success, dir(parent[%v] child name[%v]) verSeq[%v] ino[%v]",
				dentry.ParentId, dentry.Name, s.getTaskVerSeq(), ino)
			atomic.AddInt64(&s.currentStat.DirNum, 1)
			atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
		}
	}

}

func (s *SnapshotScanner) handlVerDelBreadthFirst(dentry *proto.ScanDentry) {
	var (
		children []proto.Dentry
		ino      *proto.InodeInfo
		err      error
	)

	if !os.FileMode(dentry.Type).IsDir() {
		return
	}

	scanDentries := make([]*proto.ScanDentry, 0)
	totalChildDirNum := 0
	totalChildFileNum := 0
	marker := ""
	done := false

	for !done {
		children, err = s.mw.ReadDirLimitByVer(dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), false)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("action[handlVerDelBreadthFirst] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
				dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), err)
			return
		}
		log.LogDebugf("action[handlVerDelBreadthFirst] ReadDirLimitByVer parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
			dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), len(children))

		if err == syscall.ENOENT {
			done = true
			log.LogErrorf("action[handlVerDelBreadthFirst] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
				dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), err)
			break
		}

		if marker != "" {
			if len(children) >= 1 && marker == children[0].Name {
				if len(children) <= 1 {
					done = true
					log.LogDebugf("action[handlVerDelBreadthFirst] ReadDirLimit_ll done, parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
						dentry.Inode, marker, uint64(snapshotRoutineNumPerTask), s.getTaskVerSeq(), children)
					break
				} else {
					skippedChild := children[0]
					children = children[1:]
					log.LogDebugf("action[handlVerDelBreadthFirst] ReadDirLimit_ll skip last marker[%v], parent[%v] verSeq[%v] skippedName[%v]",
						marker, dentry.Inode, s.getTaskVerSeq(), skippedChild.Name)
				}
			}
		}

		for _, child := range children {
			childDentry := &proto.ScanDentry{
				ParentId: dentry.Inode,
				Name:     child.Name,
				Inode:    child.Inode,
				Type:     child.Type,
			}
			if os.FileMode(childDentry.Type).IsDir() {
				s.inodeChan.In <- childDentry
				totalChildDirNum++
				log.LogDebugf("action[handlVerDelBreadthFirst] push dir(parent[%v] child name[%v] ino[%v]) in channel",
					childDentry.ParentId, childDentry.Name, childDentry.Inode)
			} else {
				scanDentries = append(scanDentries, childDentry)
			}
		}

		for _, file := range scanDentries {
			if ino, err = s.mw.Delete_Ver_ll(file.ParentId, file.Name, false, s.getTaskVerSeq()); err != nil {
				log.LogErrorf("action[handlVerDelBreadthFirst] Delete_Ver_ll failed, file(parent[%v] child name[%v]) verSeq[%v] err[%v]",
					file.ParentId, file.Name, s.getTaskVerSeq(), err)
				atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
			} else {
				totalChildFileNum++
				log.LogDebugf("action[handlVerDelBreadthFirst] Delete_Ver_ll success, file(parent[%v] child name[%v]) verSeq[%v] ino[%v]",
					file.ParentId, file.Name, s.getTaskVerSeq(), ino)
				atomic.AddInt64(&s.currentStat.FileNum, 1)
				atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
			}

		}
		scanDentries = scanDentries[:0]
		childrenNr := len(children)
		if (marker == "" && childrenNr < snapshotRoutineNumPerTask) || (marker != "" && childrenNr+1 < snapshotRoutineNumPerTask) {
			log.LogDebugf("action[handlVerDelBreadthFirst] ReadDirLimit_ll done, parent[%v] total childrenNr[%v] marker[%v]",
				dentry.Inode, totalChildFileNum+totalChildDirNum, marker)
			done = true
		} else {
			marker = children[childrenNr-1].Name
			log.LogDebugf("action[handlVerDelBreadthFirst] ReadDirLimit_ll next marker[%v] parent[%v]", marker, dentry.Inode)
		}
	}

}

func (s *SnapshotScanner) DoneScanning() bool {
	log.LogDebugf("inodeChan.Len(%v) rPoll.RunningNum(%v)", s.inodeChan.Len(), s.rPoll.RunningNum())
	return s.inodeChan.Len() == 0 && s.rPoll.RunningNum() == 0
}

func (s *SnapshotScanner) checkScanning(report bool) {
	dur := time.Second * time.Duration(scanCheckInterval)
	taskCheckTimer := time.NewTimer(dur)
	for {
		select {
		case <-s.stopC:
			log.LogDebugf("stop checking scan")
			return
		case <-taskCheckTimer.C:
			if s.DoneScanning() {
				taskCheckTimer.Stop()
				if report {
					t := time.Now()
					response := s.adminTask.Response.(*proto.SnapshotVerDelTaskResponse)
					response.EndTime = &t
					response.Status = proto.TaskSucceeds
					response.Done = true
					response.ID = s.ID
					response.VolName = s.Volume
					response.VerSeq = s.getTaskVerSeq()
					response.FileNum = s.currentStat.FileNum
					response.DirNum = s.currentStat.DirNum
					response.TotalInodeNum = s.currentStat.TotalInodeNum
					response.ErrorSkippedNum = s.currentStat.ErrorSkippedNum
					s.lcnode.scannerMutex.Lock()
					delete(s.lcnode.snapshotScanners, s.ID)
					s.Stop()
					s.lcnode.scannerMutex.Unlock()

					s.lcnode.respondToMaster(s.adminTask)
					log.LogInfof("scan completed for task(%v)", s.adminTask)
				} else {
					log.LogInfof("first round scan completed for task(%v) without report", s.adminTask)
				}

				return
			}
			taskCheckTimer.Reset(dur)
		}
	}

}
