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

type S3Scanner struct {
	ID            string
	RoutineID     int64
	Volume        string
	mw            *meta.MetaWrapper
	lcnode        *LcNode
	adminTask     *proto.AdminTask
	filter        *proto.ScanFilter
	abortFilter   *proto.AbortIncompleteMultiPartFilter
	dirChan       *unboundedchan.UnboundedChan
	fileChan      *unboundedchan.UnboundedChan
	fileRPoll     *routinepool.RoutinePool
	dirRPoll      *routinepool.RoutinePool
	batchDentries *fileDentries
	currentStat   proto.S3TaskStatistics
	stopC         chan bool
}

//func NewS3Scanner(scanTask *proto.RuleTask, RoutineID int64, l *LcNode) (*S3Scanner, error) {
func NewS3Scanner(adminTask *proto.AdminTask, l *LcNode) (*S3Scanner, error) {
	request := adminTask.Request.(*proto.RuleTaskRequest)
	scanTask := request.Task
	var err error
	filter, abortFilter := scanTask.Rule.GetScanFilter()
	if filter == nil && abortFilter == nil {
		return nil, errors.New("invalid rule")
	}
	var metaConfig = &meta.MetaConfig{
		Volume:        scanTask.VolName,
		Masters:       l.masters,
		Authenticate:  false,
		ValidateOwner: false,
	}

	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return nil, err
	}

	scanner := &S3Scanner{
		ID:          scanTask.Id,
		RoutineID:   request.RoutineID,
		Volume:      scanTask.VolName,
		mw:          metaWrapper,
		lcnode:      l,
		adminTask:   adminTask,
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

func (s *S3Scanner) FindPrefixInode() (inode uint64, mode uint32, err error) {
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

func (s *S3Scanner) Stop() {
	close(s.stopC)
	s.fileRPoll.WaitAndClose()
	s.dirRPoll.WaitAndClose()
	close(s.dirChan.In)
	close(s.fileChan.In)
	log.LogDebugf("scanner(%v) stopped", s.ID)
}

func (s *S3Scanner) Start() (err error) {
	request := s.adminTask.Request.(*proto.RuleTaskRequest)
	response := s.adminTask.Response.(*proto.RuleTaskResponse)
	perfixInode, mode, err := s.FindPrefixInode()
	if err != nil {
		log.LogWarnf("startScan: node path(%v) found in volume(%v), err(%v), scanning done!",
			s.filter.Prefix, request.Task.VolName, err)
		t := time.Now()
		response.EndTime = &t
		response.Status = proto.TaskSucceeds

		s.lcnode.scannerMutex.Lock()
		delete(s.lcnode.s3Scanners, s.ID)
		s.lcnode.scannerMutex.Unlock()

		//l.respondToMaster(s.adminTask)
		return
	}

	go s.scan()

	prefixDentry := &proto.ScanDentry{
		Inode: perfixInode,
		Type:  mode,
	}

	t := time.Now()
	response.StartTime = &t
	if os.FileMode(mode).IsDir() {
		log.LogDebugf("startScan: first dir entry(%v) in!", prefixDentry)
		s.dirChan.In <- prefixDentry
	} else {
		log.LogDebugf("startScan: first file entry(%v) in!", prefixDentry)
		s.fileChan.In <- prefixDentry
	}

	if s.abortFilter != nil {
		scanMultipart := func() {
			s.incompleteMultiPartScan(response)
		}
		_, _ = s.fileRPoll.Submit(scanMultipart)
	}

	go s.checkScanning()
	return
}

func (s *S3Scanner) DoneScanning() bool {
	log.LogDebugf("dirChan.Len(%v) fileChan.Len(%v) fileRPoll.RunningNum(%v) dirRPoll.RunningNum(%v)",
		s.dirChan.Len(), s.fileChan.Len(), s.fileRPoll.RunningNum(), s.dirRPoll.RunningNum())
	return s.dirChan.Len() == 0 && s.fileChan.Len() == 0 && s.fileRPoll.RunningNum() == 0 && s.dirRPoll.RunningNum() == 0
}

func (s *S3Scanner) batchAbortIncompleteMultiPart(expriredInfos []*proto.ExpiredMultipartInfo, resp *proto.RuleTaskResponse) (err error) {

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

func (s *S3Scanner) incompleteMultiPartScan(resp *proto.RuleTaskResponse) {
	expriredInfos, err := s.mw.BatchGetExpiredMultipart(s.abortFilter.Prefix, s.abortFilter.DaysAfterInitiation)
	if err == syscall.ENOENT {
		log.LogDebugf("incompleteMultiPartScan: no expired multipart")
		return
	}

	_ = s.batchAbortIncompleteMultiPart(expriredInfos, resp)
}

func (s *S3Scanner) handlFile(dentry *proto.ScanDentry) {
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

func (s *S3Scanner) handleDir(dentry *proto.ScanDentry, resp *proto.RuleTaskResponse) {
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

func (s *S3Scanner) handleDirLimit(dentry *proto.ScanDentry) {
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
			if len(children) >= 1 && marker == children[0].Name {
				if len(children) <= 1 {
					done = true
					break
				} else {
					children = children[1:]
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
func (s *S3Scanner) handleDirLimitDepthFirst(dentry *proto.ScanDentry) {
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
			if len(children) >= 1 && marker == children[0].Name {
				if len(children) <= 1 {
					done = true
					break
				} else {
					children = children[1:]
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

func (s *S3Scanner) getDirJob(dentry *proto.ScanDentry) (job func()) {
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

func (s *S3Scanner) scan() {
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
					s.handlFile(dentry)
				}

				_, err := s.fileRPoll.Submit(job)
				if err != nil {
					log.LogErrorf("handlFile failed, err(%v)", err)
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
						s.handlFile(dentry)
					}

					_, err := s.fileRPoll.Submit(job)
					if err != nil {
						log.LogErrorf("handlFile failed, err(%v)", err)
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

//func (s *S3Scanner) checkScanning(adminTask *proto.AdminTask, resp *proto.RuleTaskResponse) {
func (s *S3Scanner) checkScanning() {
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
					log.LogInfof("scan completed for task(%v)", s.adminTask)
					taskCheckTimer.Stop()
					t := time.Now()
					response := s.adminTask.Response.(*proto.RuleTaskResponse)
					response.EndTime = &t
					response.Status = proto.TaskSucceeds
					response.Done = true
					response.ID = s.ID
					response.RoutineID = s.RoutineID
					response.Volume = s.Volume
					response.Prefix = s.filter.Prefix
					response.ExpiredNum = s.currentStat.ExpiredNum
					response.FileScannedNum = s.currentStat.FileScannedNum
					response.DirScannedNum = s.currentStat.DirScannedNum
					response.TotalInodeScannedNum = s.currentStat.TotalInodeScannedNum
					response.AbortedIncompleteMultipartNum = s.currentStat.AbortedIncompleteMultipartNum
					response.ErrorSkippedNum = s.currentStat.ErrorSkippedNum

					s.lcnode.scannerMutex.Lock()
					delete(s.lcnode.s3Scanners, s.ID)
					s.Stop()
					s.lcnode.scannerMutex.Unlock()

					s.lcnode.respondToMaster(s.adminTask)
					return
				}

			}
			taskCheckTimer.Reset(dur)
		}
	}

}
