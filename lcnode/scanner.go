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
)

var (
	defaultUChanInitCapacity int = 1000
	defaultRoutingNumPerTask int = 100
)

type TaskScanner struct {
	ID        string
	RoutineID int64
	mw        *meta.MetaWrapper
	//ScanTask *proto.RuleTask
	filter        *proto.ScanFilter
	abortFilter   *proto.AbortIncompleteMultiPartFilter
	dirChan       *unboundedchan.UnboundedChan
	fileChan      *unboundedchan.UnboundedChan
	rPoll         *routinepool.RoutinePool
	batchDentries *fileDentries
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
		mw:          metaWrapper,
		filter:      filter,
		abortFilter: abortFilter,
		dirChan:     unboundedchan.NewUnboundedChan(defaultUChanInitCapacity),
		fileChan:    unboundedchan.NewUnboundedChan(defaultUChanInitCapacity),
		rPoll:       routinepool.NewRoutinePool(defaultRoutingNumPerTask),
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
	log.LogDebugf("stopping scanner(%v)", s.ID)
	s.stopC <- true
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

func (s *TaskScanner) scan(resp *proto.RuleTaskResponse) {
	fileChanClosed := false
	dirChanClosed := false
	log.LogDebugf("Enter scan")
	defer func() {
		log.LogDebugf("Exit scan")
	}()
	for {
		select {
		case <-s.stopC:
			close(s.dirChan.In)
			close(s.fileChan.In)
			//close(s.stopC)
			s.rPoll.WaitAndClose()
			return
		case val, ok := <-s.fileChan.Out:
			if !ok {
				log.LogWarnf("fileChan closed")
				fileChanClosed = true
				if dirChanClosed {
					return
				}
			} else {

				handFile := func() {
					dentry := val.(*proto.ScanDentry)
					log.LogDebugf("scan file dentry(%v)", dentry)
					if dentry.DelInode {
						_, err := s.mw.Delete_ll(dentry.ParentId, dentry.Name, false)
						if err != nil {
							log.LogErrorf("delete dentry(%v) failed, err(%v)", dentry, err)
						} else {
							log.LogDebugf("dentry(%v) deleted!", dentry)
							atomic.AddInt64(&resp.ExpiredNum, 1)
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
							atomic.AddInt64(&resp.FileScannedNum, scannedNum)
							atomic.AddInt64(&resp.TotalInodeScannedNum, scannedNum)
							s.batchDentries.Clear()
						}
					}
				}

				_, err := s.rPoll.Submit(handFile)
				if err != nil {
					log.LogErrorf("handFile failed, err(%v)", err)
				}

			}
		case val, ok := <-s.dirChan.Out:
			if !ok {
				log.LogWarnf("dirChan closed")
				dirChanClosed = true
				if fileChanClosed {
					return
				}

			} else {

				handleDir := func() {
					dentry := val.(*proto.ScanDentry)
					log.LogDebugf("scan dir dentry(%v)", dentry)
					children, err := s.mw.ReadDir_ll(dentry.Inode)
					if err != nil && err != syscall.ENOENT {
						atomic.AddInt64(&resp.ErrorSkippedNum, 1)
						log.LogWarnf("ReadDir_ll failed")
						return
					}
					if err == syscall.ENOENT {
						atomic.AddInt64(&resp.ErrorSkippedNum, 1)
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

				_, err := s.rPoll.Submit(handleDir)
				if err != nil {
					log.LogErrorf("handleDir failed, err(%v)", err)
				}
			}

		}
	}
}
