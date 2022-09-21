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

type SnapshotScanner struct {
	ID          string
	Volume      string
	mw          *meta.MetaWrapper
	lcnode      *LcNode
	adminTask   *proto.AdminTask
	inodeChan   *unboundedchan.UnboundedChan
	rPoll       *routinepool.RoutinePool
	currentStat proto.SnapshotStatistics
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
		rPoll:       routinepool.NewRoutinePool(defaultRoutingNumPerTask),
		currentStat: proto.SnapshotStatistics{},
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
	response := s.adminTask.Response.(*proto.SnapshotVerDelTaskResponse)
	go s.scan()

	prefixDentry := &proto.ScanDentry{
		Inode: proto.RootIno,
		Type:  proto.Mode(os.ModeDir),
	}
	t := time.Now()
	response.StartTime = &t
	log.LogDebugf("startScan: first dir entry(%v) in!", prefixDentry)
	s.inodeChan.In <- prefixDentry

	go s.checkScanning()
	return
}

func (s *SnapshotScanner) getDirJob(dentry *proto.ScanDentry) (job func()) {
	//no need to lock
	if s.inodeChan.Len()+defaultRoutingNumPerTask > defaultMaxChanUnm {
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

		for !done {
			log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll parent %v verSeq %v", dentry.Inode, s.getTaskVerSeq())
			children, err = s.mw.ReadDirLimitByVer(dentry.Inode, marker, uint64(defaultRoutingNumPerTask), s.getTaskVerSeq())
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("action[handlVerDelDepthFirst] parent %v verSeq %v err %v", dentry.Inode, s.getTaskVerSeq(), err)
				return
			}

			if err == syscall.ENOENT {
				done = true
				break
			}

			if marker != "" {
				if len(children) <= 1 {
					done = true
					break
				} else {
					children = children[1:]
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
					log.LogErrorf("action[handlVerDelDepthFirst] parent %v Delete_ll_EX child name %v verSeq %v err %v",
						file.ParentId, file.Name, s.getTaskVerSeq(), err)
					atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
				} else {
					log.LogDebugf("action[handlVerDelDepthFirst] parent %v Delete_ll_EX child name %v verSeq %v ino %v success",
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
			if (marker == "" && childrenNr < defaultRoutingNumPerTask) || (marker != "" && childrenNr+1 < defaultRoutingNumPerTask) {
				log.LogDebugf("action[handlVerDelDepthFirst] ReadDirLimit_ll parent %v done", dentry.Inode)
				done = true
			} else {
				marker = children[childrenNr-1].Name
			}
		}

	}

	if ino, err = s.mw.Delete_Ver_ll(dentry.ParentId, dentry.Name, false, s.getTaskVerSeq()); err != nil {
		log.LogErrorf("action[handlVerDelDepthFirst] parent %v Delete_ll_EX child name %v verSeq %v err %v",
			dentry.ParentId, dentry.Name, s.getTaskVerSeq(), err)
		atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
	} else {
		log.LogDebugf("action[handlVerDelDepthFirst] parent %v Delete_ll_EX child name %v verSeq %v ino %v success",
			dentry.ParentId, dentry.Name, s.getTaskVerSeq(), ino)
		atomic.AddInt64(&s.currentStat.DirNum, 1)
		atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
	}
}

func (s *SnapshotScanner) handlVerDel(dentry *proto.ScanDentry) {
	var (
		children []proto.Dentry
		ino      *proto.InodeInfo
		err      error
	)

	scanDentries := make([]*proto.ScanDentry, 0)
	if os.FileMode(dentry.Type).IsDir() {
		marker := ""
		done := false

		for !done {
			log.LogDebugf("action[handlVerDel] ReadDirLimit_ll parent %v verSeq %v", dentry.Inode, s.getTaskVerSeq())
			children, err = s.mw.ReadDirLimitByVer(dentry.Inode, marker, uint64(defaultRoutingNumPerTask), s.getTaskVerSeq())
			if err != nil && err != syscall.ENOENT {
				log.LogErrorf("action[handlVerDel] parent %v verSeq %v marker %v err %v", dentry.Inode, s.getTaskVerSeq(), marker, err)
				return
			}

			if err == syscall.ENOENT {
				done = true
				log.LogErrorf("action[handlVerDel] parent %v verSeq %v marker %v err %v", dentry.Inode, s.getTaskVerSeq(), marker, err)
				break
			}

			if marker != "" {
				if len(children) <= 1 {
					done = true
					log.LogDebugf("action[handlVerDel] parent %v verSeq %v children %v marker %v", dentry.Inode, s.getTaskVerSeq(), children, marker)
					break
				} else {
					children = children[1:]
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
				} else {
					scanDentries = append(scanDentries, childDentry)
				}
			}

			for _, file := range scanDentries {
				if ino, err = s.mw.Delete_Ver_ll(file.ParentId, file.Name, false, s.getTaskVerSeq()); err != nil {
					log.LogErrorf("action[handlVerDel] parent %v Delete_ll_EX child name %v verSeq %v err %v",
						file.ParentId, file.Name, s.getTaskVerSeq(), err)
					atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
				} else {
					log.LogDebugf("action[handlVerDel] parent %v Delete_ll_EX child name %v verSeq %v ino %v success",
						file.ParentId, file.Name, s.getTaskVerSeq(), ino)
					atomic.AddInt64(&s.currentStat.FileNum, 1)
					atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
				}

			}
			scanDentries = scanDentries[:0]
			childrenNr := len(children)
			if (marker == "" && childrenNr < defaultRoutingNumPerTask) || (marker != "" && childrenNr+1 < defaultRoutingNumPerTask) {
				log.LogDebugf("action[handlVerDel] ReadDirLimit_ll parent %v done marker %v", dentry.Inode, marker)
				done = true
			} else {
				marker = children[childrenNr-1].Name
			}
		}

	}

	if ino, err = s.mw.Delete_Ver_ll(dentry.ParentId, dentry.Name, os.FileMode(dentry.Type).IsDir(), s.getTaskVerSeq()); err != nil {
		if dentry.ParentId >= 1 {
			log.LogErrorf("action[handlVerDel] parent %v Delete_ll_EX child name %v verSeq %v err %v",
				dentry.ParentId, dentry.Name, s.getTaskVerSeq(), err)
			atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
		}
	} else {
		log.LogDebugf("action[handlVerDel] parent %v Delete_ll_EX child name %v verSeq %v ino %v success",
			dentry.ParentId, dentry.Name, s.getTaskVerSeq(), ino)
		atomic.AddInt64(&s.currentStat.DirNum, 1)
		atomic.AddInt64(&s.currentStat.TotalInodeNum, 1)
	}

}

func (s *SnapshotScanner) DoneScanning() bool {
	log.LogDebugf("inodeChan.Len(%v) rPoll.RunningNum(%v)", s.inodeChan.Len(), s.rPoll.RunningNum())
	return s.inodeChan.Len() == 0 && s.rPoll.RunningNum() == 0
}

func (s *SnapshotScanner) checkScanning() {
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
				return
			}
			taskCheckTimer.Reset(dur)
		}
	}

}
