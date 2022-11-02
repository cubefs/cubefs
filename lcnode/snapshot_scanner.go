package lcnode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	//snap scan type
	SnapScanTypeAll             int = 0
	SnapScanTypeOnlyFile        int = 1
	SnapScanTypeOnlyDepth       int = 2
	SnapScanTypeOnlyDir         int = 3
	SnapScanTypeOnlyDirAndDepth int = 4
)

type SnapshotScanner struct {
	ID          string
	Volume      string
	mw          *meta.MetaWrapper
	lcnode      *LcNode
	adminTask   *proto.AdminTask
	inodeChan   *unboundedchan.UnboundedChan
	rPoll       *routinepool.RoutinePool
	currentStat proto.SnapshotStatistics
	scanFunc    func()
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
		inodeChan:   unboundedchan.NewUnboundedChan(defaultUChanInitCapacity),
		rPoll:       routinepool.NewRoutinePool(snapShotRoutingNumPerTask),
		currentStat: proto.SnapshotStatistics{},
		scanType:    SnapScanTypeAll,
		stopC:       make(chan bool, 0),
	}
	return scanner, nil
}

func (s *SnapshotScanner) getTaskVolName() string {
	request := s.adminTask.Request.(*proto.SnapshotVerDelTaskRequest)
	return request.Task.VolName
}

func (s *SnapshotScanner) getTaskVerSeq() uint64 {
	request := s.adminTask.Request.(*proto.SnapshotVerDelTaskRequest)
	return request.Task.VerSeq
}

func (s *SnapshotScanner) Stop() {
	close(s.stopC)
	s.rPoll.WaitAndClose()
	close(s.inodeChan.In)
	log.LogDebugf("snapshot scanner(%v) stopped", s.ID)
}

func (s *SnapshotScanner) Start() (err error) {
	//1. first delete all files
	if err = s.startScan(SnapScanTypeOnlyFile, true, false); err != nil {
		return err
	}
	log.LogDebugf("startScan: first round files done!")
	//2. depth first delete all dirs
	if err = s.startScan(SnapScanTypeOnlyDirAndDepth, false, true); err != nil {
		return err
	}
	log.LogDebugf("startScan: second round dirs done!")
	return
}

func (s *SnapshotScanner) startScan(scanType int, syncWait bool, report bool) (err error) {
	response := s.adminTask.Response.(*proto.SnapshotVerDelTaskResponse)

	s.scanType = scanType
	if s.scanFunc == nil {
		log.LogDebugf("startScan: start scanFunc")
		s.scanFunc = s.scan
		go s.scanFunc()
	}

	prefixDentry := &proto.ScanDentry{
		Inode: proto.RootIno,
		Type:  proto.Mode(os.ModeDir),
	}
	t := time.Now()
	response.StartTime = &t
	log.LogDebugf("startScan: scan type(%v), first dir entry(%v) in!", scanType, prefixDentry)
	s.inodeChan.In <- prefixDentry

	if syncWait {
		s.checkScanning(report)
	} else {
		go s.checkScanning(report)
	}

	return
}

func (s *SnapshotScanner) getDirJob(dentry *proto.ScanDentry) (job func()) {
	//no need to lock
	if s.scanType == SnapScanTypeOnlyDepth || s.scanType == SnapScanTypeOnlyDirAndDepth {
		log.LogDebugf("getDirJob: Only depth first job ")
		job = func() {
			s.handlVerDelDepthFirst(dentry)
		}
		return
	}

	if s.inodeChan.Len()+snapShotRoutingNumPerTask > defaultMaxChanUnm {
		log.LogDebugf("getDirJob: Depth first job")
		job = func() {
			s.handlVerDelDepthFirst(dentry)
		}
	} else {
		log.LogDebugf("getDirJob: Breadth first job")
		job = func() {
			s.handlVerDel(dentry)
		}
	}
	return
}

func (s *SnapshotScanner) scan() {
	log.LogDebugf("Enter scan")
	defer func() {
		log.LogDebugf("Exit scan")
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

	//scanDentries := make([]*proto.ScanDentry, 0)
	if os.FileMode(dentry.Type).IsDir() {
		marker := ""
		done := false

		onlyDir := s.scanType == SnapScanTypeOnlyDir || s.scanType == SnapScanTypeOnlyDirAndDepth

		for !done {
			children, err = s.mw.ReadDirLimitByVer(dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), onlyDir)
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("action[handlVerDelDepthFirst] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
					dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), err)
				return
			}
			log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimitByVer parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
				dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), len(children))

			if err == syscall.ENOENT {
				done = true
				log.LogErrorf("action[handlVerDelDepthFirst] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
					dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), marker, err)
				break
			}

			if marker != "" {
				if len(children) >= 1 && marker == children[0].Name {
					if len(children) <= 1 {
						done = true
						log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll done, parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
							dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), children)
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
					DelInode: false,
					ParentId: dentry.Inode,
					Name:     child.Name,
					Inode:    child.Inode,
					Type:     child.Type,
				}
				if os.FileMode(childDentry.Type).IsDir() {
					//s.inodeChan.In <- childDentry
					dirs = append(dirs, childDentry)
				} else {
					//scanDentries = append(scanDentries, childDentry)
					files = append(files, childDentry)
				}
			}

			//for _, file := range scanDentries {
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
			//scanDentries = scanDentries[:0]
			for _, dir := range dirs {
				s.handlVerDelDepthFirst(dir)
			}

			childrenNr := len(children)
			if (marker == "" && childrenNr < snapShotRoutingNumPerTask) || (marker != "" && childrenNr+1 < snapShotRoutingNumPerTask) {
				log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll done, parent[%v]",
					dentry.Inode)
				done = true
			} else {
				marker = children[childrenNr-1].Name
				log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll next marker[%v] parent[%v]", marker, dentry.Inode)
			}
		}

	}

	if s.scanType != SnapScanTypeOnlyFile {
		if ino, err = s.mw.Delete_Ver_ll(dentry.ParentId, dentry.Name, false, s.getTaskVerSeq()); err != nil {
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

//func (s *SnapshotScanner) handlVerDel(dentry *proto.ScanDentry, scanType int) {
func (s *SnapshotScanner) handlVerDel(dentry *proto.ScanDentry) {
	var (
		children []proto.Dentry
		ino      *proto.InodeInfo
		err      error
	)

	scanDentries := make([]*proto.ScanDentry, 0)
	if os.FileMode(dentry.Type).IsDir() {
		//totalChildrenNum := 0
		totalChildDirNum := 0
		totalChildFileNum := 0
		marker := ""
		done := false

		onlyDir := s.scanType == SnapScanTypeOnlyDir || s.scanType == SnapScanTypeOnlyDirAndDepth

		for !done {
			children, err = s.mw.ReadDirLimitByVer(dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), onlyDir)
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("action[handlVerDel] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
					dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), err)
				return
			}
			log.LogDebugf("action[handlVerDel] ReadDirLimitByVer parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
				dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), len(children))

			if err == syscall.ENOENT {
				done = true
				log.LogErrorf("action[handlVerDel] ReadDirLimitByVer failed, parent[%v] maker[%v] limit[%v] verSeq[%v] err[%v]",
					dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), marker, err)
				break
			}

			if marker != "" {
				if len(children) >= 1 && marker == children[0].Name {
					if len(children) <= 1 {
						done = true
						log.LogDebugf("action[handlVerDel] ReadDirLimit_ll done, parent[%v] maker[%v] limit[%v] verSeq[%v] children[%v]",
							dentry.Inode, marker, uint64(snapShotRoutingNumPerTask), s.getTaskVerSeq(), children)
						break
					} else {
						skippedChild := children[0]
						children = children[1:]
						log.LogDebugf("action[handlVerDel] ReadDirLimit_ll skip last marker[%v], parent[%v] verSeq[%v] skippedName[%v]",
							marker, dentry.Inode, s.getTaskVerSeq(), skippedChild.Name)
					}
				}
			}

			for _, child := range children {
				childDentry := &proto.ScanDentry{
					DelInode: false,
					ParentId: dentry.Inode,
					Name:     child.Name,
					Inode:    child.Inode,
					Type:     child.Type,
				}
				if os.FileMode(childDentry.Type).IsDir() {
					s.inodeChan.In <- childDentry
					totalChildDirNum++
					log.LogDebugf("action[handlVerDel] push dir(parent[%v] child name[%v] ino[%v]) in channel",
						childDentry.ParentId, childDentry.Name, childDentry.Inode)
				} else {
					scanDentries = append(scanDentries, childDentry)
				}
			}

			for _, file := range scanDentries {
				if ino, err = s.mw.Delete_Ver_ll(file.ParentId, file.Name, false, s.getTaskVerSeq()); err != nil {
					log.LogErrorf("action[handlVerDel] Delete_Ver_ll failed, file(parent[%v] child name[%v]) verSeq[%v] err[%v]",
						file.ParentId, file.Name, s.getTaskVerSeq(), err)
					atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
				} else {
					totalChildFileNum++
					log.LogDebugf("action[handlVerDel] Delete_Ver_ll success, file(parent[%v] child name[%v]) verSeq[%v] ino[%v]",
						file.ParentId, file.Name, s.getTaskVerSeq(), ino)
					atomic.AddInt64(&s.currentStat.FileNum, 1)
					atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
				}

			}
			scanDentries = scanDentries[:0]
			childrenNr := len(children)
			if (marker == "" && childrenNr < snapShotRoutingNumPerTask) || (marker != "" && childrenNr+1 < snapShotRoutingNumPerTask) {
				log.LogDebugf("action[handlVerDel] ReadDirLimit_ll done, parent[%v] total childrenNr[%v] marker[%v]",
					dentry.Inode, totalChildFileNum+totalChildDirNum, marker)
				done = true
			} else {
				marker = children[childrenNr-1].Name
				log.LogDebugf("action[handlVerDel] ReadDirLimit_ll next marker[%v] parent[%v]", marker, dentry.Inode)
			}
		}

	}

	if s.scanType != SnapScanTypeOnlyFile {
		if ino, err = s.mw.Delete_Ver_ll(dentry.ParentId, dentry.Name, os.FileMode(dentry.Type).IsDir(), s.getTaskVerSeq()); err != nil {
			if dentry.ParentId >= 1 {
				log.LogErrorf("action[handlVerDel] Delete_Ver_ll failed, dir(parent[%v] child name[%v]) verSeq[%v] err[%v]",
					dentry.ParentId, dentry.Name, s.getTaskVerSeq(), err)
				atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
			}
		} else {
			log.LogDebugf("action[handlVerDel] Delete_Ver_ll success, dir(parent[%v] child name[%v]) verSeq[%v] ino[%v]",
				dentry.ParentId, dentry.Name, s.getTaskVerSeq(), ino)
			atomic.AddInt64(&s.currentStat.DirNum, 1)
			atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
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
					response.VolName = s.getTaskVolName()
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
