package lcnode

import (
	"errors"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	defaultUChanInitCapacity int = 10000
	defaultRoutingNumPerTask int = 100
	defaultMaxChanUnm        int = 10000000 //worst case: may take up around 400MB of heap space
)

type TaskScanner struct {
	ID        string
	RoutineID int64
	Volume    string
	mw        *meta.MetaWrapper
	//ScanTask *proto.RuleTask
	filter        *proto.ScanFilter
	abortFilter   *proto.AbortIncompleteMultiPartFilter
	dirChan       *unboundedchan.UnboundedChan
	fileChan      *unboundedchan.UnboundedChan
	fileRPoll     *routinepool.RoutinePool
	dirRPoll      *routinepool.RoutinePool
	batchDentries *fileDentries
	currentStat   proto.TaskStatistics
	stopC         chan bool
}

func NewTaskScanner(scanTask *proto.RuleTask, RoutineID int64, masters []string) (*TaskScanner, error) {
	var err error
	filter, abortFilter := scanTask.Rule.GetScanFilter()
	if filter == nil && abortFilter == nil {
		return nil, errors.New("invalid rule")
	}
	var metaConfig = &meta.MetaConfig{
		Volume:        scanTask.VolName,
		Masters:       masters,
		Authenticate:  false,
		ValidateOwner: false,
	}

	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return nil, err
	}

	scanner := &TaskScanner{
		ID:          scanTask.Id,
		RoutineID:   RoutineID,
		Volume:      scanTask.VolName,
		mw:          metaWrapper,
		filter:      filter,
		abortFilter: abortFilter,
		dirChan:     unboundedchan.NewUnboundedChan(defaultUChanInitCapacity),
		fileChan:    unboundedchan.NewUnboundedChan(defaultUChanInitCapacity),
		fileRPoll:   routinepool.NewRoutinePool(defaultRoutingNumPerTask),
		dirRPoll:    routinepool.NewRoutinePool(defaultRoutingNumPerTask),
		stopC:       make(chan bool, 0),
	}
	scanner.batchDentries = newFileDentries()

	return scanner, nil
}

func (s *TaskScanner) FindPrefixInode() (inode uint64, mode uint32, err error) {
	// if prefix and marker are both not empty, use marker
	var dirs []string
	if s.filter.Prefix != "" {
		dirs = strings.Split(s.filter.Prefix, "/")
		log.LogDebugf("FindPrefixInode: Prefix(%v), dirs(%v) dirs len(%v)", s.filter.Prefix, dirs, len(dirs))
	}
	if len(dirs) <= 0 {
		return proto.RootIno, 0, nil
	}
	var parentId = proto.RootIno
	for _, dir := range dirs {
		if dir == "" {
			continue
		}

		curIno, curMode, err := s.mw.Lookup_ll(parentId, dir)

		// If the part except the last part does not match exactly the same dentry, there is
		// no path matching the path prefix. An ENOENT error is returned to the caller.
		if err == syscall.ENOENT {
			log.LogErrorf("FindPrefixInode: find directories fail: parentId(%v) dir(%v)", parentId, dir)
			return 0, 0, syscall.ENOENT
		}

		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("FindPrefixInode: find directories fail: prefix(%v) err(%v)", s.filter.Prefix, err)
			return 0, 0, err
		}

		//if !os.FileMode(curMode).IsDir() {
		//	return 0, syscall.ENOENT
		//}

		parentId = curIno
		inode = curIno
		mode = curMode
	}

	return
}

func (s *TaskScanner) Stop() {
	close(s.stopC)
	s.fileRPoll.WaitAndClose()
	s.dirRPoll.WaitAndClose()
	close(s.dirChan.In)
	close(s.fileChan.In)
	log.LogDebugf("scanner(%v) stopped", s.ID)
}

func (s *TaskScanner) DoneScanning() bool {
	log.LogDebugf("dirChan.Len(%v) fileChan.Len(%v) fileRPoll.RunningNum(%v) dirRPoll.RunningNum(%v)",
		s.dirChan.Len(), s.fileChan.Len(), s.fileRPoll.RunningNum(), s.dirRPoll.RunningNum())
	return s.dirChan.Len() == 0 && s.fileChan.Len() == 0 && s.fileRPoll.RunningNum() == 0 && s.dirRPoll.RunningNum() == 0
}

func (s *TaskScanner) batchAbortIncompleteMultiPart(expriredInfos []*proto.ExpiredMultipartInfo, resp *proto.RuleTaskResponse) (err error) {

	for _, info := range expriredInfos {
		// release part data
		for _, inode := range info.Inodes {
			log.LogWarnf("BatchAbortIncompleteMultiPart: unlink part inode: path(%v) multipartID(%v) inode(%v)",
				info.Path, info.MultipartId, inode)
			if _, err = s.mw.InodeUnlink_ll(inode); err != nil {
				log.LogErrorf("BatchAbortIncompleteMultiPart: meta inode unlink fail: path(%v) multipartID(%v)inode(%v) err(%v)",
					info.Path, info.MultipartId, inode, err)
			}
			log.LogWarnf("BatchAbortIncompleteMultiPart: evict part inode: path(%v) multipartID(%v) inode(%v)",
				info.Path, info.MultipartId, inode)
			if err = s.mw.Evict(inode); err != nil {
				log.LogErrorf("BatchAbortIncompleteMultiPart: meta inode evict fail: path(%v) multipartID(%v)inode(%v) err(%v)",
					info.Path, info.MultipartId, inode, err)
			}
			log.LogDebugf("BatchAbortIncompleteMultiPart: multipart part data released: path(%v) multipartID(%v)inode(%v)",
				info.Path, info.MultipartId, inode)
		}

		if err = s.mw.RemoveMultipart_ll(info.Path, info.MultipartId); err != nil {
			log.LogErrorf("BatchAbortIncompleteMultiPart: meta abort multipart fail: path(%v) multipartID(%v) err(%v)",
				info.Path, info.MultipartId, err)
			return err
		}
		atomic.AddInt64(&resp.AbortedIncompleteMultipartNum, 1)
	}

	return nil
}

func (s *TaskScanner) incompleteMultiPartScan(resp *proto.RuleTaskResponse) {
	expriredInfos, err := s.mw.BatchGetExpiredMultipart(s.abortFilter.Prefix, s.abortFilter.DaysAfterInitiation)
	if err == syscall.ENOENT {
		log.LogDebugf("incompleteMultiPartScan: no expired multipart")
		return
	}

	_ = s.batchAbortIncompleteMultiPart(expriredInfos, resp)
}

func (s *TaskScanner) handFile(dentry *proto.ScanDentry) {
	log.LogDebugf("scan file dentry(%v)", dentry)
	if dentry.DelInode {
		_, err := s.mw.Delete_ll(dentry.ParentId, dentry.Name, false)
		if err != nil {
			log.LogErrorf("delete dentry(%v) failed, err(%v)", dentry, err)
		} else {
			log.LogDebugf("dentry(%v) deleted!", dentry)
			//atomic.AddInt64(&resp.ExpiredNum, 1)
			atomic.AddInt64(&s.currentStat.ExpiredNum, 1)
		}

	} else {
		s.batchDentries.Append(dentry)
		if s.batchDentries.Len() >= batchExpirationGetNum {
			expireInfos := s.mw.BatchInodeExpirationGet(s.batchDentries.GetDentries(), s.filter.ExpireCond)
			for _, info := range expireInfos {
				if info.Expired {
					info.Dentry.DelInode = true
					s.fileChan.In <- info.Dentry
				}
			}

			var scannedNum int64 = int64(s.batchDentries.Len())
			atomic.AddInt64(&s.currentStat.FileScannedNum, scannedNum)
			atomic.AddInt64(&s.currentStat.TotalInodeScannedNum, scannedNum)
			s.batchDentries.Clear()
		}
	}
}

func (s *TaskScanner) handleDir(dentry *proto.ScanDentry, resp *proto.RuleTaskResponse) {
	log.LogDebugf("scan dir dentry(%v)", dentry)
	children, err := s.mw.ReadDir_ll(dentry.Inode)
	if err != nil && err != syscall.ENOENT {
		atomic.AddInt64(&resp.ErrorSkippedNum, 1)
		log.LogWarnf("ReadDir_ll failed")
		return
	}
	if err == syscall.ENOENT {
		return
	}

	atomic.AddInt64(&resp.DirScannedNum, 1)
	atomic.AddInt64(&resp.TotalInodeScannedNum, 1)

	for _, child := range children {
		childDentry := &proto.ScanDentry{
			DelInode: false,
			ParentId: dentry.Inode,
			Name:     child.Name,
			Inode:    child.Inode,
			Type:     child.Type,
		}
		if os.FileMode(childDentry.Type).IsDir() {
			s.dirChan.In <- childDentry
		} else {
			s.fileChan.In <- childDentry
		}
	}
}

func (s *TaskScanner) handleDirLimit(dentry *proto.ScanDentry) {
	log.LogDebugf("scan dir dentry(%v)", dentry)

	marker := ""
	done := false
	for !done {
		children, err := s.mw.ReadDirLimit_ll(dentry.Inode, marker, uint64(defaultRoutingNumPerTask))
		if err != nil && err != syscall.ENOENT {
			atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
			log.LogWarnf("ReadDir_ll failed")
			return
		}

		if marker == "" {
			atomic.AddInt64(&s.currentStat.DirScannedNum, 1)
			atomic.AddInt64(&s.currentStat.TotalInodeScannedNum, 1)
		}

		if err == syscall.ENOENT {
			done = true
			break
		}

		if marker != "" {
			children = children[1:]
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
				s.dirChan.In <- childDentry
			} else {
				s.fileChan.In <- childDentry
			}
		}

		childrenNr := len(children)
		if (marker == "" && childrenNr < defaultRoutingNumPerTask) || (marker != "" && childrenNr+1 < defaultRoutingNumPerTask) {
			done = true
		} else {
			marker = children[childrenNr-1].Name
		}

	}

}

//handleDirLimitDepthFirst used to scan dir tree in depth when size of dirChan.In grow too much.
//consider 40 Bytes is the ave size of proto.ScanDentry, 100 million ScanDentries may take up to around 4GB of Memory
func (s *TaskScanner) handleDirLimitDepthFirst(dentry *proto.ScanDentry) {
	log.LogDebugf("scan dir dentry(%v)", dentry)

	marker := ""
	done := false
	for !done {
		children, err := s.mw.ReadDirLimit_ll(dentry.Inode, marker, uint64(defaultRoutingNumPerTask))
		if err != nil && err != syscall.ENOENT {
			atomic.AddInt64(&s.currentStat.ErrorSkippedNum, 1)
			log.LogWarnf("ReadDir_ll failed")
			return
		}

		if marker == "" {
			atomic.AddInt64(&s.currentStat.DirScannedNum, 1)
			atomic.AddInt64(&s.currentStat.TotalInodeScannedNum, 1)
		}

		if err == syscall.ENOENT {
			done = true
			break
		}

		if marker != "" {
			children = children[1:]
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
				dirs = append(dirs, childDentry)
			} else {
				files = append(files, childDentry)
			}
		}

		for _, file := range files {
			s.fileChan.In <- file
		}
		for _, dir := range dirs {
			s.handleDirLimitDepthFirst(dir)
		}

		childrenNr := len(children)
		if (marker == "" && childrenNr < defaultRoutingNumPerTask) || (marker != "" && childrenNr+1 < defaultRoutingNumPerTask) {
			done = true
		} else {
			marker = children[childrenNr-1].Name
		}

	}

}

func (s *TaskScanner) getDirJob(dentry *proto.ScanDentry) (job func()) {
	//no need to lock
	if s.dirChan.Len()+defaultRoutingNumPerTask > defaultMaxChanUnm {
		log.LogDebugf("getDirJob: Depth first job")
		job = func() {
			s.handleDirLimitDepthFirst(dentry)
		}
	} else {
		log.LogDebugf("getDirJob: Breadth first job")
		job = func() {
			s.handleDirLimit(dentry)
		}
	}
	return
}

func (s *TaskScanner) scan() {
	log.LogDebugf("Enter scan")
	defer func() {
		log.LogDebugf("Exit scan")
	}()
	for {
		select {
		case <-s.stopC:
			return
		case val, ok := <-s.fileChan.Out:
			if !ok {
				log.LogWarnf("fileChan closed")
			} else {
				dentry := val.(*proto.ScanDentry)
				job := func() {
					s.handFile(dentry)
				}

				_, err := s.fileRPoll.Submit(job)
				if err != nil {
					log.LogErrorf("handFile failed, err(%v)", err)
				}

			}
		default:
			select {
			case <-s.stopC:
				return
			case val, ok := <-s.fileChan.Out:
				if !ok {
					log.LogWarnf("fileChan closed")
				} else {
					dentry := val.(*proto.ScanDentry)
					job := func() {
						s.handFile(dentry)
					}

					_, err := s.fileRPoll.Submit(job)
					if err != nil {
						log.LogErrorf("handFile failed, err(%v)", err)
					}

				}
			case val, ok := <-s.dirChan.Out:
				if !ok {
					log.LogWarnf("dirChan closed")
				} else {
					dentry := val.(*proto.ScanDentry)
					job := s.getDirJob(dentry)

					_, err := s.dirRPoll.Submit(job)
					if err != nil {
						log.LogErrorf("handleDir failed, err(%v)", err)
					}
				}
			}
		}
	}
}

func (s *TaskScanner) checkScanning(adminTask *proto.AdminTask, resp *proto.RuleTaskResponse, l *LcNode) {
	dur := time.Second * time.Duration(scanCheckInterval)
	taskCheckTimer := time.NewTimer(dur)
	for {
		select {
		case <-s.stopC:
			log.LogDebugf("stop checking scan")
			return
		case <-taskCheckTimer.C:
			if s.DoneScanning() {
				if s.batchDentries.Len() > 0 {
					log.LogDebugf("last batch dentries scan")
					expireInfos := s.mw.BatchInodeExpirationGet(s.batchDentries.GetDentries(), s.filter.ExpireCond)
					for _, info := range expireInfos {
						if info.Expired {
							info.Dentry.DelInode = true
							s.fileChan.In <- info.Dentry
						}
					}

					var scannedNum int64 = int64(s.batchDentries.Len())
					atomic.AddInt64(&s.currentStat.FileScannedNum, scannedNum)
					atomic.AddInt64(&s.currentStat.TotalInodeScannedNum, scannedNum)
					s.batchDentries.Clear()
				} else {
					log.LogInfof("scan completed for task(%v)", adminTask)
					taskCheckTimer.Stop()
					t := time.Now()
					resp.EndTime = &t
					resp.Status = proto.TaskSucceeds
					resp.Done = true
					resp.ID = s.ID
					resp.RoutineID = s.RoutineID
					resp.Volume = s.Volume
					resp.Prefix = s.filter.Prefix
					resp.ExpiredNum = s.currentStat.ExpiredNum
					resp.FileScannedNum = s.currentStat.FileScannedNum
					resp.DirScannedNum = s.currentStat.DirScannedNum
					resp.TotalInodeScannedNum = s.currentStat.TotalInodeScannedNum
					resp.AbortedIncompleteMultipartNum = s.currentStat.AbortedIncompleteMultipartNum
					resp.ErrorSkippedNum = s.currentStat.ErrorSkippedNum

					l.scannerMutex.Lock()
					delete(l.scanners, s.ID)
					l.scannerMutex.Unlock()

					l.respondToMaster(adminTask)
					return
				}

			}
			taskCheckTimer.Reset(dur)
		}
	}

}
