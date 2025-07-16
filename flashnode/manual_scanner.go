package flashnode

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"golang.org/x/time/rate"
)

const (
	pathSeparator                    = "/"
	defaultUnboundedChanInitCapacity = 10000
	defaultReadDirLimit              = 1000
	maxDirChanNum                    = 100000
)

type ManualScanner struct {
	ID             string
	Volume         string
	mw             MetaWrapper
	ec             ExtentApi
	RemoteCache    *stream.RemoteCache
	flashNode      *FlashNode
	adminTask      *proto.AdminTask
	manualTask     *proto.FlashManualTask
	limiter        *rate.Limiter
	prepareLimiter *rate.Limiter
	createTime     time.Time
	currentStat    *proto.ManualTaskStatistics
	dirChan        *unboundedchan.UnboundedChan
	fileChan       chan interface{}
	dirRPool       *routinepool.RoutinePool
	fileRPool      *routinepool.RoutinePool
	receiveStop    bool
	receiveStopC   chan struct{}
	stopC          chan struct{}
	receivePauseC  chan struct{}
	receiveResumeC chan struct{}
	pause          int32
	pauseCond      *sync.Cond
	closeOnce      sync.Once
	mu             sync.Mutex
}

func NewManualScanner(adminTask *proto.AdminTask, f *FlashNode, metaWrapper *meta.MetaWrapper, extentClient *stream.ExtentClient) *ManualScanner {
	request := adminTask.Request.(*proto.FlashNodeManualTaskRequest)
	scanTask := request.Task
	scanRoutineNum := scanTask.ManualTaskConfig.TraverseFileConcurrency
	if scanRoutineNum <= 0 {
		scanRoutineNum = f.scanRoutineNumPerTask
	}
	handlerFileRoutineNum := scanTask.ManualTaskConfig.HandlerFileConcurrency
	if handlerFileRoutineNum <= 0 {
		handlerFileRoutineNum = f.handlerFileRoutineNumPerTask
	}
	scanner := &ManualScanner{
		ID:             scanTask.Id,
		Volume:         scanTask.VolName,
		flashNode:      f,
		adminTask:      adminTask,
		manualTask:     scanTask,
		mw:             metaWrapper,
		dirChan:        unboundedchan.NewUnboundedChan(defaultUnboundedChanInitCapacity),
		fileChan:       make(chan interface{}, 2*handlerFileRoutineNum),
		dirRPool:       routinepool.NewRoutinePool(scanRoutineNum),
		fileRPool:      routinepool.NewRoutinePool(handlerFileRoutineNum),
		currentStat:    &proto.ManualTaskStatistics{FlashNode: f.localAddr},
		limiter:        rate.NewLimiter(rate.Limit(f.manualScanLimitPerSecond), _defaultManualScanLimitBurst),
		prepareLimiter: rate.NewLimiter(rate.Limit(f.prepareLimitPerSecond), int(f.prepareLimitPerSecond/2)),
		createTime:     time.Now(),
		receiveStopC:   make(chan struct{}),
		stopC:          make(chan struct{}),
		receivePauseC:  make(chan struct{}),
		receiveResumeC: make(chan struct{}),
		pause:          0,
		pauseCond:      sync.NewCond(&sync.Mutex{}),
	}
	if extentClient != nil {
		scanner.ec = extentClient
		scanner.RemoteCache = &extentClient.RemoteCache
	}
	return scanner
}

func (s *ManualScanner) Start() (err error) {
	response := s.adminTask.Response.(*proto.FlashNodeManualTaskResponse)
	parentId, prefixDirs, err := s.FindPrefixInode()
	log.LogInfof("startScan with parentId(%v) dirs(%v) dirlen(%v)", parentId, prefixDirs, len(prefixDirs))
	if err != nil {
		log.LogErrorf("startScan err(%v): volume(%v), task id(%v), scanning done!",
			err, s.Volume, s.manualTask.Id)
		response.ID = s.ID
		response.FlashNode = s.flashNode.localAddr
		response.StartTime = &s.createTime
		response.Volume = s.Volume
		response.Status = proto.TaskFailed
		response.Done = true
		response.StartErr = err.Error()
		s.Stop()
		s.flashNode.manualScanners.Delete(s.ID)
		return
	}

	go s.handleFileChan()
	go s.handleDirChan()

	var currentPath string
	if len(prefixDirs) > 0 {
		currentPath = strings.Join(prefixDirs, pathSeparator)
	}

	firstDentry := &proto.ScanItem{
		Inode: parentId,
		Path:  strings.TrimPrefix(currentPath, pathSeparator),
		Type:  uint32(os.ModeDir),
	}
	response.StartTime = &s.createTime

	s.writeDirChan(firstDentry)

	go s.checkScanning()

	return
}

func (s *ManualScanner) checkScanning() {
	minuteProgressCount := 60 / s.flashNode.scanCheckInterval
	repeatMinute := 0
	dur := time.Second * time.Duration(s.flashNode.scanCheckInterval)
	taskCheckTimer := time.NewTimer(dur)
	for {
		repeatMinute++
		if minuteProgressCount > 0 && repeatMinute >= minuteProgressCount {
			repeatMinute = 0
			s.currentStat.LastCacheSize = s.currentStat.TotalCacheSize
		}
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop checkScanning %v", s.ID)
			return
		case <-s.receivePauseC:
			log.LogInfof("receive receivePauseC %v", s.ID)
			atomic.StoreInt32(&s.pause, 1)
		case <-s.receiveResumeC:
			log.LogInfof("receive receiveResumeC %v", s.ID)
			atomic.StoreInt32(&s.pause, 0)
			s.pauseCond.Broadcast()
		case <-s.receiveStopC:
			log.LogInfof("receive receiveStopC %v", s.ID)

			s.receiveStop = true
			s.Stop()

			t := time.Now()
			response := s.copyResponse()
			response.EndTime = &t
			response.Status = proto.TaskStop
			response.Done = true
			log.LogInfof("receive receiveStopC response(%+v)", response)

			s.flashNode.manualScanners.Delete(s.ID)
			log.LogInfof("receive receiveStopC already stop %v", s.ID)

			s.flashNode.respondToMaster(s.adminTask)
			return
		case <-taskCheckTimer.C:
			if s.DoneScanning() {
				log.LogInfof("checkScanning completed for task(%v) eklen(%v)", s.adminTask, s.currentStat.TotalExtentKeyNum)
				taskCheckTimer.Stop()
				t := time.Now()
				response := s.copyResponse()
				response.EndTime = &t
				response.Status = proto.TaskSucceeds
				response.Done = true
				log.LogInfof("checkScanning completed response(%+v)", response)
				s.Stop()
				msg := fmt.Sprintf("task(%+v) produce completed", response)
				auditlog.LogFlashNodeOp("Warmup", msg, nil)
				s.flashNode.manualScanners.Delete(s.ID)
				s.flashNode.respondToMaster(s.adminTask)
				return
			}
			taskCheckTimer.Reset(dur)
		}
	}
}

func (s *ManualScanner) copyResponse() *proto.FlashNodeManualTaskResponse {
	response := s.adminTask.Response.(*proto.FlashNodeManualTaskResponse)
	response.ID = s.ID
	response.FlashNode = s.flashNode.localAddr
	response.Volume = s.Volume
	response.RcvStop = s.receiveStop
	response.TotalFileScannedNum = s.currentStat.TotalFileScannedNum
	response.TotalFileCachedNum = s.currentStat.TotalFileCachedNum
	response.TotalDirScannedNum = s.currentStat.TotalDirScannedNum
	response.ErrorCacheNum = s.currentStat.ErrorCacheNum
	response.ErrorReadDirNum = s.currentStat.ErrorReadDirNum
	response.TotalCacheSize = s.currentStat.TotalCacheSize
	response.LastCacheSize = s.currentStat.LastCacheSize
	return response
}

func (s *ManualScanner) DoneScanning() bool {
	log.LogInfof("dirChan.Len(%v) fileChan.Len(%v) fileRPool.RunningNum(%v) dirRPool.RunningNum(%v) pause(%v)",
		s.dirChan.Len(), len(s.fileChan), s.fileRPool.RunningNum(), s.dirRPool.RunningNum(), s.pause)
	return s.dirChan.Len() == 0 && len(s.fileChan) == 0 && s.fileRPool.RunningNum() == 0 && s.dirRPool.RunningNum() == 0 && atomic.LoadInt32(&s.pause) == 0 && len(s.RemoteCache.PrepareCh) == 0
}

func (s *ManualScanner) handleFileChan() {
	log.LogInfof("Enter handleFileChan, %+v", s)
	defer func() {
		log.LogInfof("Exit handleFileChan, %+v", s)
	}()

	prefix := s.manualTask.GetPathPrefix()

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

			dentry := val.(*proto.ScanItem)
			s.applyPauseIfEnabled("handleFileChan", dentry.Name)
			if !strings.HasPrefix(dentry.Path, prefix) {
				continue
			}

			job := func(d *proto.ScanItem) func() {
				return func() {
					s.handleFile(d)
				}
			}(dentry)
			_, err := s.fileRPool.Submit(job)
			if err != nil {
				log.LogWarnf("fileRPool.Submit err(%v), id(%v)", err, s.ID)
			}
		}
	}
}

func (s *ManualScanner) applyPauseIfEnabled(method, location string) {
	if atomic.LoadInt32(&s.pause) == 1 {
		log.LogInfof("method %v pause at location: %v", method, location)
		s.pauseCond.L.Lock()
		s.pauseCond.Wait()
		s.pauseCond.L.Unlock()
		log.LogInfof("method %v resume at location: %v", method, location)
	}
}

func (s *ManualScanner) handleFile(dentry *proto.ScanItem) {
	log.LogInfof("handleFile: %v, fileChan: %v", dentry, len(s.fileChan))
	s.applyPauseIfEnabled("handleFile", dentry.Name)
	atomic.AddInt64(&s.currentStat.TotalFileScannedNum, 1)
	s.limiter.Wait(context.Background())
	info, err := s.mw.InodeGet_ll(dentry.Inode)
	if err != nil {
		log.LogErrorf("handleFile InodeGet_ll err: %v, dentry: %+v", err, dentry)
		return
	}
	op := s.manualTask.Action
	dentry.StorageClass = info.StorageClass
	dentry.Size = info.Size
	dentry.WriteGen = info.Generation
	dentry.Op = op
	log.LogInfof("handleFile: %+v, ctime(%v), atime(%v), is warming up", dentry, info.CreateTime, info.AccessTime)
	switch op {
	case proto.FlashManualWarmupAction:
		err = s.warmUp(dentry)
		if err != nil {
			atomic.AddInt64(&s.currentStat.ErrorCacheNum, 1)
			log.LogErrorf("warmup err: %v, dentry: %+v", err, dentry)
			return
		}
	case proto.FlashManualClearAction:
	case proto.FlashManualCheckAction:
	default:
		log.LogWarnf("invalid op: %v", dentry)
	}
}

func (s *ManualScanner) warmUp(i *proto.ScanItem) error {
	var (
		err     error
		extents []proto.ExtentKey
	)
	if i.Size == 0 {
		log.LogInfof("skip warmUp, size=0, inode(%v)", i.Inode)
		return nil
	}
	_, _, extents, err = s.mw.GetExtents(i.Inode, false, false, false)
	if err != nil {
		log.LogWarnf("warmUp: mw GetExtents fail, inode(%v) err: %v ", i.Inode, err)
		return err
	}
	if len(extents) == 0 {
		log.LogInfof("skip warmUp, len of extents=0, inode(%v)", i.Inode)
		return nil
	}
	if err = s.ec.OpenStream(i.Inode, false, false, ""); err != nil {
		log.LogWarnf("warmUp: ec OpenStream fail, inode(%v) err: %v", i.Inode, err)
		return err
	}
	if err = s.ec.ForceRefreshExtentsCache(i.Inode); err != nil {
		log.LogWarnf("warmUp: ec ForceRefreshExtentsCache fail, inode(%v) err: %v", i.Inode, err)
		return err
	}
	for _, extent := range extents {
		s.prepareLimiter.Wait(context.Background())
		prepareReq := stream.NewPrepareRemoteCacheRequest(i.Inode, extent, true, i.WriteGen)
		s.RemoteCache.PrepareCh <- prepareReq
		atomic.AddInt64(&s.currentStat.TotalExtentKeyNum, 1)
		atomic.AddInt64(&s.currentStat.TotalCacheSize, int64(extent.Size))
	}
	atomic.AddInt64(&s.currentStat.TotalFileCachedNum, 1)
	s.stopIfOverflow()
	return nil
}

func (s *ManualScanner) stopIfOverflow() {
	if s.manualTask.ManualTaskConfig.TotalFileSizeLimit != 0 && atomic.LoadInt64(&s.currentStat.TotalCacheSize) >= s.manualTask.ManualTaskConfig.TotalFileSizeLimit {
		if !s.receiveStop {
			log.LogInfof("scaner[%v] %v exceed %v: , close receiveStop", atomic.LoadInt64(&s.currentStat.TotalCacheSize), s.manualTask.ManualTaskConfig.TotalFileSizeLimit, s.ID)
			s.closeReceiveStop()
		} else {
			log.LogInfof("scaner[%v], already receiveStop", s.ID)
		}
	}
}

func (s *ManualScanner) handleDirChan() {
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
			dentry := val.(*proto.ScanItem)

			var job func()
			if s.dirChan.Len() > maxDirChanNum {
				job = func(d *proto.ScanItem) func() {
					return func() {
						s.handleDirLimitDepthFirst(d)
					}
				}(dentry)
			} else {
				job = func(d *proto.ScanItem) func() {
					return func() {
						s.handleDirLimitBreadthFirst(d)
					}
				}(dentry)
			}
			_, err := s.dirRPool.Submit(job)
			if err != nil {
				log.LogWarnf("dirRPool.Submit err(%v), id(%v)", err, s.ID)
			}
		}
	}
}

func (s *ManualScanner) handleDirLimitBreadthFirst(dentry *proto.ScanItem) {
	log.LogInfof("handleDirLimitBreadth dentry: %+v, dirChan.Len: %v", dentry, s.dirChan.Len())

	marker := ""
	done := false
	for !done {
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop handleDirLimitBreadth %v", s.ID)
			return
		default:
		}
		s.applyPauseIfEnabled("handleDirLimitBreadthFirst", dentry.Name)
		children, err := s.mw.ReadDirLimit_ll(dentry.Inode, marker, uint64(defaultReadDirLimit))
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

		for _, child := range children {
			select {
			case <-s.stopC:
				log.LogInfof("receive stop, stop handleDirLimitBreadth iterator children %v", s.ID)
				return
			default:
			}
			childDentry := &proto.ScanItem{
				ParentId: dentry.Inode,
				Name:     child.Name,
				Inode:    child.Inode,
				Path:     strings.TrimPrefix(dentry.Path+pathSeparator+child.Name, pathSeparator),
				Type:     child.Type,
			}
			if !os.FileMode(childDentry.Type).IsDir() {
				s.fileChan <- childDentry
			} else {
				s.writeDirChan(childDentry)
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

func (s *ManualScanner) handleDirLimitDepthFirst(dentry *proto.ScanItem) {
	log.LogInfof("handleDirLimitDepthFirst dentry: %+v, dirChan.Len: %v", dentry, s.dirChan.Len())

	marker := ""
	done := false
	for !done {
		select {
		case <-s.stopC:
			log.LogInfof("receive stop, stop handleDirLimitDepthFirst %v", s.ID)
			return
		default:
		}
		s.applyPauseIfEnabled("handleDirLimitDepthFirst", dentry.Name)

		children, err := s.mw.ReadDirLimit_ll(dentry.Inode, marker, uint64(defaultReadDirLimit))
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

		files := make([]*proto.ScanItem, 0)
		dirs := make([]*proto.ScanItem, 0)
		for _, child := range children {
			select {
			case <-s.stopC:
				log.LogInfof("receive stop, stop handleDirLimitDepthFirst iterator %v", s.ID)
				return
			default:
			}
			childDentry := &proto.ScanItem{
				ParentId: dentry.Inode,
				Name:     child.Name,
				Inode:    child.Inode,
				Path:     strings.TrimPrefix(dentry.Path+pathSeparator+child.Name, pathSeparator),
				Type:     child.Type,
			}

			if os.FileMode(childDentry.Type).IsDir() {
				dirs = append(dirs, childDentry)
			} else {
				files = append(files, childDentry)
			}
		}

		for _, file := range files {
			select {
			case <-s.stopC:
				log.LogInfof("receive stop, stop handleDirLimitDepthFirst iterator files %v", s.ID)
				return
			default:
			}
			s.fileChan <- file
		}
		for _, dir := range dirs {
			select {
			case <-s.stopC:
				log.LogInfof("receive stop, stop handleDirLimitDepthFirst iterator dirs %v", s.ID)
				return
			default:
			}
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

func (s *ManualScanner) FindPrefixInode() (inode uint64, prefixDirs []string, err error) {
	prefixDirs = make([]string, 0)
	prefix := s.manualTask.GetPathPrefix()

	var dirs []string
	if prefix != "" {
		dirs = strings.Split(prefix, pathSeparator)
		log.LogInfof("FindPrefixInode: volume(%v), prefix(%v), dirs(%v), len(%v)", s.Volume, prefix, dirs, len(dirs))
	} else {
		return proto.RootIno, prefixDirs, nil
	}
	parentId := proto.RootIno
	for _, dir := range dirs {
		curIno, curMode, err := s.mw.Lookup_ll(parentId, dir)

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

func (s *ManualScanner) writeDirChan(d *proto.ScanItem) {
	select {
	case <-s.stopC:
		log.LogInfof("receive stop, stop write dir chan %v", s.ID)
		return
	default:
		s.dirChan.In <- d
		log.LogInfof("writeDirChan(%v): dir dentry(%v) !", s.ID, d)
	}
}

func (s *ManualScanner) Stop() {
	close(s.stopC)
	s.clearBufferEvent() //  avoid consume event
	s.dirRPool.WaitAndClose()
	s.fileRPool.WaitAndClose()
	close(s.dirChan.In)
	close(s.fileChan)
	s.mw.Close()
	if s.ec != nil {
		s.ec.Close()
	}
	log.LogInfof("stop: scanner(%v) stopped", s.ID)
}

func (s *ManualScanner) closeReceiveStop() {
	s.closeOnce.Do(func() {
		close(s.receiveStopC)
	})
}

func (s *ManualScanner) clearBufferEvent() {
	var num int
	for {
		select {
		case <-s.fileChan:
			num++
		default:
			log.LogInfof("stop: clearChan clear num(%v)", num)
			return
		}
	}
}

func (s *ManualScanner) processCommand(command string) {
	s.mu.Lock()
	switch command {
	case "stop":
		if !s.receiveStop {
			log.LogInfof("receive httpServiceScanner: %v, close receiveStop", s.ID)
			s.closeReceiveStop()
		} else {
			log.LogInfof("receive httpServiceScanner: %v, already receiveStop", s.ID)
		}
	case "pause":
		log.LogInfof("receive event to httpServiceScanner: %v, receivePauseC ", s.ID)
		s.receivePauseC <- struct{}{}
	case "resume":
		log.LogInfof("receive event to httpServiceScanner: %v, receiveResumeC ", s.ID)
		s.receiveResumeC <- struct{}{}
	default:
		log.LogInfof("invalid task opCode: %v tid %v", command, s.ID)
	}
	s.mu.Unlock()
}
