package meta

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/google/uuid"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// put deleted files or directories into this folder under Trash
const (
	CurrentName         = "Current"
	TrashPrefix         = ".Trash"
	ExpiredPrefix       = "Expired"
	ParentDirPrefix     = "|__|"
	ExpiredTimeFormat   = "2006-01-02-150405"
	FileNameLengthMax   = 255
	LongNamePrefix      = "LongName____"
	OriginalName        = "OriginalName"
	DefaultReaddirLimit = 4096
	TrashPathIgnore     = "trashPathIgnore"
	LockExpireSeconds   = 3600       // 1 hour
	BucketRootPrefix    = ".buckets" // bucket container under Current/Expired
	BucketHashWidth     = 2          // first 2 hex chars
	RebuildDirName      = "rebuild"  // rebuilt result zone
	ensureBucketBackoff = 10 * time.Millisecond
)

const (
	DisableTrash = "/trash/disable"
	QueryTrash   = "/trash/query"
)

type Trash struct {
	mw                        *MetaWrapper
	mountPath                 string
	trashRoot                 string
	trashRootIno              uint64
	deleteInterval            int64
	currentReady              chan struct{}
	done                      chan struct{}
	deleteWorkerStop          chan struct{}
	trashRootMode             uint32
	trashRootUid              uint32
	trashRootGid              uint32
	subDirCache               *DirInodeCache
	traverseDirGoroutineLimit chan bool
	rebuildGoroutineLimit     int
	rebuildStatus             int32

	getLock    bool
	lockId     int64
	getLockMux sync.Mutex
}

const (
	rebuildStop    int32 = 0
	rebuildRunning int32 = 1
)

func NewTrash(mw *MetaWrapper, interval int64, subDir string, traverseLimit int, rebuildGoroutineLimit int) (*Trash, error) {
	if subDir == "" {
		subDir = "/"
	}
	trash := &Trash{
		mw:                        mw,
		mountPath:                 subDir,
		deleteInterval:            interval,
		currentReady:              make(chan struct{}, 1),
		done:                      make(chan struct{}, 1),
		deleteWorkerStop:          make(chan struct{}, 1),
		traverseDirGoroutineLimit: make(chan bool, traverseLimit),
		rebuildGoroutineLimit:     rebuildGoroutineLimit,
		subDirCache:               NewDirInodeCache(DefaultDirInodeExpiration, DefaultMaxDirInode),
	}
	atomic.StoreInt32(&trash.rebuildStatus, rebuildStop)
	// create trash root
	if err := trash.InitTrashRoot(); err != nil {
		return nil, err
	}

	return trash, nil
}

// hashBucket returns the bucket name (hex) for a file, using parent path + file name to keep distribution stable.
func (trash *Trash) hashBucket(parentPathAbsolute, fileName string) string {
	h := md5.Sum([]byte(path.Join(parentPathAbsolute, fileName)))
	return hex.EncodeToString(h[:])[:BucketHashWidth]
}

// ensureChildDir creates a child directory under parentPath if absent, returning its full path and inode info.
func (trash *Trash) ensureChildDir(parentPath, name string, isAsync bool) (string, *proto.InodeInfo, error) {
	full := path.Join(parentPath, name)
	if info := trash.subDirCache.Get(full); info != nil {
		return full, info, nil
	}
	exists, err := trash.pathIsExist(full, isAsync)
	if err != nil {
		return "", nil, err
	}
	if exists {
		ino, err := trash.mw.LookupPath(full, isAsync)
		if err != nil {
			return "", nil, err
		}
		info, err := trash.mw.InodeGet_ll(ino, isAsync)
		if err != nil {
			return "", nil, err
		}
		trash.subDirCache.Put(full, info)
		return full, info, nil
	}

	parentInfo, err := trash.LookupPath(parentPath, true, isAsync)
	if err != nil {
		return "", nil, err
	}
	created, err := trash.CreateDirectory(parentInfo.Inode, name, parentInfo.Mode, parentInfo.Uid, parentInfo.Gid, full, true, isAsync)
	if err != nil && err != syscall.EEXIST {
		return "", nil, err
	}
	if created == nil {
		ino, err := trash.mw.LookupPath(full, isAsync)
		if err != nil {
			return "", nil, err
		}
		created, err = trash.mw.InodeGet_ll(ino, isAsync)
		if err != nil {
			return "", nil, err
		}
	}
	trash.subDirCache.Put(full, created)
	return full, created, nil
}

// ensureBucketDir prepares the bucket directory under Current (or provided root).
func (trash *Trash) ensureBucketDir(bucketName string, isAsync bool) (string, *proto.InodeInfo, error) {
	currentPath := path.Join(trash.trashRoot, CurrentName)
	if err := trash.createCurrent(true, isAsync); err != nil {
		return "", nil, err
	}
	bucketRoot, _, err := trash.ensureChildDir(currentPath, BucketRootPrefix, isAsync)
	if err != nil {
		return "", nil, err
	}
	return trash.ensureChildDir(bucketRoot, bucketName, isAsync)
}

// ensureRebuildRoot prepares the rebuild result root under the given base (Current or Expired_xxx).
func (trash *Trash) ensureRebuildRoot(base string) (string, error) {
	_, _, err := trash.ensureChildDir(base, RebuildDirName, true)
	return path.Join(base, RebuildDirName), err
}

func (trash *Trash) StartScheduleTask() {
	go trash.deleteWorker()
	go trash.buildDeletedFileParentDirsBackground()
	go trash.refreshTrashLock()
}

func (trash *Trash) InitTrashRoot() (err error) {
	// trash.trashRoot = path.Join(trash.mountPoint, trash.mountPath, TrashPrefix)
	trash.trashRoot = path.Join(trash.mountPath, TrashPrefix)
	log.LogDebugf("action[InitTrashRoot] %v ", trash.trashRoot)
	// check trash root exist
	exists, err := trash.pathIsExist(trash.trashRoot, false)
	if err != nil {
		return err
	}
	if exists {
		if err = trash.initTrashRootInodeInfo(); err != nil {
			return err
		}
		log.LogDebugf("action[InitTrashRoot] trash root is exist")
		return nil
	}

	parentDirInfo, err := trash.LookupPath(path.Clean(trash.mountPath), true, false)
	if err != nil {
		log.LogErrorf("action[InitTrashRoot]LookupPath trash parent failed: %v", err.Error())
		return err
	}
	_, err = trash.CreateDirectory(parentDirInfo.Inode, TrashPrefix,
		parentDirInfo.Mode, parentDirInfo.Uid, parentDirInfo.Gid, TrashPrefix, false, false)
	if err != nil {
		log.LogErrorf("action[InitTrashRoot]create trash root failed: %v", err.Error())
		return err
	}
	return trash.initTrashRootInodeInfo()
}

func (trash *Trash) initTrashRootInodeInfo() error {
	trashRootInfo, err := trash.LookupPath(trash.trashRoot, true, false)
	if err != nil {
		return err
	}
	trash.trashRootIno = trashRootInfo.Inode
	trash.trashRootMode = trashRootInfo.Mode
	trash.trashRootUid = trashRootInfo.Uid
	trash.trashRootGid = trashRootInfo.Gid
	return nil
}

func (trash *Trash) createCurrent(ignoreExist bool, isAsync bool) (err error) {
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	log.LogDebugf("action[createCurrent] enter")
	exists, err := trash.pathIsExist(trashCurrent, isAsync)
	if err != nil {
		return err
	}
	if exists {
		// cache trashCurrent if not cached
		if value := trash.subDirCache.Get(trashCurrent); value == nil {
			ino, _ := trash.mw.LookupPath(trashCurrent, isAsync)
			info, err := trash.mw.InodeGet_ll(ino, isAsync)
			if err != nil {
				log.LogWarnf("action[createCurrent] get %v inode info failed:%v", trashCurrent, err.Error())
				return err
			} else {
				trash.subDirCache.Put(trashCurrent, info)
				log.LogDebugf("action[createCurrent] store %v info %v", trashCurrent, info)
			}
		}
		return nil
	}
	inodeInfo, err := trash.CreateDirectory(trash.trashRootIno, CurrentName,
		trash.trashRootMode, trash.trashRootUid, trash.trashRootGid, path.Join(TrashPrefix, CurrentName), ignoreExist, isAsync)
	if err != nil {
		if err != syscall.EEXIST {
			log.LogErrorf("action[createCurrent]create trash current failed: %v", err.Error())
		} else {
			return nil
		}
		return err
	}
	trash.subDirCache.Put(trashCurrent, inodeInfo)
	log.LogDebugf("action[createCurrent] store %v info %v", trashCurrent, inodeInfo)
	return nil
}

func (trash *Trash) generateTmpFileName(parentPathAbsolute string) string {
	if parentPathAbsolute == "" {
		return ParentDirPrefix
	} else {
		replacedStr := strings.ReplaceAll(parentPathAbsolute, "/", ParentDirPrefix)
		return replacedStr[len(ParentDirPrefix):] + ParentDirPrefix
	}
}

func (trash *Trash) CleanTrashPatchCache(parentPathAbsolute string, fileName string) {
	dstPath := path.Join(trash.mountPath, parentPathAbsolute, fileName)
	trash.subDirCache.Delete(dstPath)
	log.LogDebugf("CleanTrashPatchCache: CleanTrashPatchCache(%v)  ", dstPath)
}

func (trash *Trash) MoveToTrash(parentPathAbsolute string, parentIno uint64, fileName string, isDir bool, isAsync bool) (err error) {
	start := time.Now()
	defer func() {
		log.LogDebugf("action[MoveToTrash] parentPathAbsolute(%v) fileName(%v) consume %v", parentPathAbsolute, fileName, time.Since(start).Seconds())
	}()
	log.LogDebugf("action[MoveToTrash] parentPathAbsolute(%v) fileName(%v) parentIno(%v)", parentPathAbsolute, fileName, parentIno)

	var (
		bucketDir  string
		bucketInfo *proto.InodeInfo
	)

	bucketName := trash.hashBucket(parentPathAbsolute, fileName)
	for {
		bucketDir, bucketInfo, err = trash.ensureBucketDir(bucketName, isAsync)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			log.LogWarnf("action[MoveToTrash] ensureBucketDir failed: %v", err)
			return err
		}
		time.Sleep(ensureBucketBackoff)
	}
	trashCurrentIno := bucketInfo.Inode
	srcPath := path.Join(trash.mountPath, parentPathAbsolute, fileName)
	// generate tmp file name
	tmpFileName := fmt.Sprintf("%v%v", trash.generateTmpFileName(parentPathAbsolute), fileName)
	dstPath := path.Join(bucketDir, tmpFileName)
	startCheck := time.Now()
	for {
		exists, checkErr := trash.pathIsExistInTrash(dstPath, isAsync)
		if checkErr != nil {
			log.LogWarnf("action[MoveToTrash] check path in trash failed: %v", checkErr)
			return checkErr
		}
		if exists {
			if !isDir {
				// ignore dir rename
				dstPath = fmt.Sprintf("%s_%v", dstPath, time.Now().Unix())
				//		log.LogDebugf("action[MoveToTrash]filePathInTrash rename to %v", dstPath)
			} else {
				// delete src dir directly
				err := trash.deleteSrcDirDirectly(parentIno, fileName, srcPath, isAsync)
				if err != nil {
					return err
				}
				break
			}
		} else {
			log.LogDebugf("action[MoveToTrash] break")
			break
		}
	}
	log.LogDebugf("action[MoveToTrash] startCheck: srcPath(%v) dstPath(%v) consume %v", srcPath, dstPath, time.Since(startCheck).Seconds())
	startRename := time.Now()
	var (
		needStoreXattr = false
		originName     string
	)

	if len(path.Base(dstPath)) > FileNameLengthMax {
		needStoreXattr = true
		dstPath, originName = transferLongFileName(dstPath)
	}
	err = trash.renameToTrashTempFile(parentIno, trashCurrentIno, srcPath, dstPath, isAsync)
	log.LogDebugf("action[MoveToTrash]  rename: srcPath(%v) dstPath(%v) consume %v", srcPath, dstPath, time.Since(startRename).Seconds())
	if err != nil {
		log.LogWarnf("action[MoveToTrash] rename %v to %v failed:%v", srcPath, dstPath, err.Error())
		return err
	}
	if needStoreXattr {
		go func(name, dstPath string, parentID uint64) {
			var (
				info *proto.InodeInfo
				err  error
			)
			info, err = trash.LookupEntry(parentID, path.Base(dstPath), isAsync)
			if err != nil {
				log.LogWarnf("action[MoveToTrash] LookupEntry %v failed:%v", dstPath, err.Error())
				return
			}

			err = trash.mw.XAttrSet_ll(info.Inode, []byte(OriginalName), []byte(name), isAsync)
			if err != nil {
				log.LogWarnf("action[MoveToTrash] set xattr for %v[%v] failed:%v", dstPath, info.Inode, err.Error())
				return
			}
			log.LogDebugf("action[MoveToTrash] set xattr for %v [%v]success:%v", dstPath, info.Inode, name)
		}(originName, dstPath, trashCurrentIno)
	}
	// nil to check tmp file exist
	trash.subDirCache.Put(dstPath, &proto.InodeInfo{})
	log.LogDebugf("action[MoveToTrash] rename %v to %v success", srcPath, dstPath)
	return nil
}

func transferLongFileName(filePath string) (newName, oldName string) {
	oldName = path.Base(filePath)
	parentPath := path.Dir(filePath)
	newName = strings.TrimPrefix(oldName, ParentDirPrefix)
	newName = strings.ReplaceAll(newName, ParentDirPrefix, "/")
	newBaseName := path.Base(newName)
	return path.Join(parentPath, LongNamePrefix+newBaseName+ParentDirPrefix+uuid.New().String()), oldName
}

func (trash *Trash) getDeleteInterval() int64 {
	checkPointInterval := atomic.LoadInt64(&trash.deleteInterval) / 4
	if checkPointInterval <= 0 {
		checkPointInterval = 1
	}
	rand.Seed(time.Now().UnixNano())
	return checkPointInterval
}

func (trash *Trash) UpdateDeleteInterval(interval int64) {
	log.LogDebugf("action[UpdateDeleteInterval] new interval is %v", interval)
	if atomic.LoadInt64(&trash.deleteInterval) == interval {
		log.LogDebugf("action[UpdateDeleteInterval] interval in not changed")
		return
	}
	trash.stopDeleteWorker()
	log.LogDebugf("action[UpdateDeleteInterval] deleteWorker is stopped")
	atomic.StoreInt64(&trash.deleteInterval, interval)
	go trash.deleteWorker()
}

func (trash *Trash) stopDeleteWorker() {
	trash.done <- struct{}{}
	<-trash.deleteWorkerStop
}

func (trash *Trash) tryGetLock() {
	trash.getLockMux.Lock()
	defer trash.getLockMux.Unlock()

	log.LogDebugf("tryGetLock: try get root dir lock for trash, path %s, vol %s, ino %d",
		trash.mountPath, trash.mw.volname, trash.trashRootIno)

	retId, err := trash.mw.LockDir(trash.trashRootIno, LockExpireSeconds, trash.lockId, true)
	if err != nil {
		log.LogWarnf("tryGetLock: get dir lock failed for trash, ino %d, id %d, err %v", trash.trashRootIno, trash.lockId, err)
		trash.getLock = false
		trash.lockId = 0
		return
	}

	trash.getLock = true
	trash.lockId = retId
	log.LogWarnf("tryGetLock: try get root dir lock for trash success, path %s, vol %s, ino %d",
		trash.mountPath, trash.mw.volname, trash.trashRootIno)
}

func (trash *Trash) refreshTrashLock() {
	leaseTicker := time.NewTicker(LockExpireSeconds / 2 * time.Second)
	defer leaseTicker.Stop()

	trash.tryGetLock()

	for {
		select {
		case <-trash.mw.closeCh:
			if !trash.getLock {
				log.LogWarnf("releaseTrashLock: no need to relase lock, ino %d", trash.trashRootIno)
				return
			}
			trash.getLock = false

			_, err := trash.mw.LockDir(trash.trashRootIno, 0, trash.lockId, true)
			if err != nil {
				log.LogErrorf("releaseTrashLock: relase failed, ino %d, id %d, err %s", trash.trashRootIno, trash.lockId, err)
				return
			}

			log.LogWarnf("releaseTrashLock: trash is closed now, try release lock success, ino %d", trash.trashRootIno)
			return
		case <-leaseTicker.C:
			trash.tryGetLock()
		}
	}
}

func (trash *Trash) deleteWorker() {
	checkPointInterval := trash.getDeleteInterval()
	t := time.NewTicker(time.Duration(checkPointInterval) * time.Minute)
	log.LogDebugf("action[deleteWorker] enter interval is %v minute", checkPointInterval)
	defer t.Stop()
	for {
		select {
		case <-trash.done:
			log.LogWarnf("traverse stopped!")
			trash.deleteWorkerStop <- struct{}{}
			return
		case <-t.C:

			if !trash.getLock {
				log.LogDebugf("deleteWorker: trash get lock failed, not execute")
				continue
			}

			// delete expired directory
			trash.deleteExpiredData()
			// rename current directory(expired_timestamp)
			trash.renameCurrent()
			checkPointInterval = trash.getDeleteInterval()
			t.Reset(time.Duration(checkPointInterval) * time.Minute)
		}
	}
}

func (trash *Trash) renameCurrent() {
	log.LogDebugf("action[renameCurrent]enter")
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	exists, err := trash.pathIsExist(trashCurrent, true)
	if err != nil {
		log.LogWarnf("action[renameCurrent] check trashCurrent failed: %v", err)
		return
	}
	if !exists {
		return
	}
	// if current is rebuilding
	for {
		if atomic.LoadInt32(&trash.rebuildStatus) == rebuildRunning {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	// keep current for 1/4 delete interval
	ino, _ := trash.mw.LookupPath(trashCurrent, true)
	inoInfo, err := trash.mw.InodeGet_ll(ino, true)
	if err != nil {
		log.LogWarnf("action[renameCurrent]get inode for trashCurrent failed %v", err.Error())
		return
	}
	checkPointInterval := trash.deleteInterval / 4
	if time.Since(inoInfo.CreateTime) < time.Duration(checkPointInterval)*time.Minute {
		log.LogDebugf("action[renameCurrent]trashCurrent keep for 1/4 interval %v", time.Since(inoInfo.CreateTime).String())
		return
	}
	// ensure files in current is rebuild
	trash.buildDeletedFileParentDirs()
	trashModifyTime := inoInfo.ModifyTime
	for {
		expiredTrash := fmt.Sprintf("%s_%v", ExpiredPrefix, trashModifyTime.Format(ExpiredTimeFormat))
		if err := trash.rename(trashCurrent, path.Join(trash.trashRoot, expiredTrash)); err != nil {
			// if err := trash.mw.Rename_ll(trash.trashRootIno, CurrentName, trash.trashRootIno,
			//	expiredTrash, false); err != nil
			log.LogDebugf("action[renameCurrent]rename current failed: %v", err.Error())
			time.Sleep(time.Millisecond * 100)
			trashModifyTime = trashModifyTime.Add(100 * time.Millisecond)
		} else {
			log.LogDebugf("action[renameCurrent]rename current completed")
			// clear cache
			trash.subDirCache.Clear()
			break
		}
	}
}

func (trash *Trash) deleteExpiredData() {
	defer log.LogDebugf("action[deleteExpiredData]exit")
	log.LogDebugf("action[deleteExpiredData]enter")
	// read trash root
	entries, err := trash.mw.ReadDir_ll(trash.trashRootIno, true)
	if err != nil {
		log.LogWarnf("action[deleteExpiredData]ReadDir trashRoot  failed: %v", err.Error())
		return
	}
	now := time.Now()
	for _, entry := range entries {
		log.LogDebugf("action[deleteExpiredData]check %s is dir %v", entry.Name, proto.IsDir(entry.Type))
		if !proto.IsDir(entry.Type) {
			continue
		}
		// skip current
		if strings.Compare(entry.Name, CurrentName) == 0 {
			continue
		}
		// extract timestamp from name
		checkPoint, err := trash.extractTimeStampFromName(entry.Name)
		if err != nil {
			log.LogWarnf("action[deleteExpiredData]Extract timestamp from  %s failed: %v", entry.Name, err.Error())
			continue
		}
		if now.Sub(time.Unix(checkPoint, 0)) > (time.Duration(trash.deleteInterval) * time.Minute) {
			log.LogDebugf("action[deleteExpiredData]delete  %s ", entry.Name)
			trash.mw.AddInoInfoCache(entry.Inode, trash.trashRootIno, entry.Name)
			trash.removeAll(entry.Name, entry.Inode)
			trash.deleteTask(trash.trashRootIno, entry.Name, proto.IsDir(entry.Type), path.Join(TrashPrefix, entry.Name))
		}
	}
}

func (trash *Trash) removeAll(dirName string, dirIno uint64) {
	log.LogDebugf("action[removeAll]start delete %v", dirName)
	var (
		wg     sync.WaitGroup
		noMore = false
		from   = ""
	)
	for !noMore {
		batches, err := trash.mw.ReadDirLimit_ll(dirIno, from, DefaultReaddirLimit, true)
		if err != nil {
			log.LogErrorf("action[removeAll] ReadDirLimit_ll: ino(%v) err(%v) from(%v)", dirIno, err, from)
			return
		}
		batchNr := uint64(len(batches))
		if batchNr == 0 || (from != "" && batchNr == 1) {
			break
		} else if batchNr < DefaultReaddirLimit {
			noMore = true
		}
		if from != "" {
			batches = batches[1:]
		}
		for _, entry := range batches {
			log.LogDebugf("action[deleteDir]traverse  %v", entry.Name)
			if !proto.IsDir(entry.Type) {
				continue
			}
			trash.mw.AddInoInfoCache(entry.Inode, dirIno, entry.Name)
			select {
			case trash.traverseDirGoroutineLimit <- true:
				log.LogDebugf("action[deleteDir]launch goroutine  %v", entry.Name)
				wg.Add(1)
				go func(dirName string, dirIno uint64) {
					defer wg.Done()
					trash.removeAll(dirName, dirIno)
					trash.releaseTraverseToken()
				}(entry.Name, entry.Inode)
			default:
				log.LogDebugf("action[deleteDir]execute local  %v", entry.Name)
				trash.removeAll(entry.Name, entry.Inode)
			}
		}
		wg.Wait()
		from = batches[len(batches)-1].Name
	}
	noMore = false
	from = ""
	for !noMore {
		batches, err := trash.mw.ReadDirLimit_ll(dirIno, from, DefaultReaddirLimit, true)
		if err != nil {
			log.LogErrorf("action[removeAll] ReadDirLimit_ll: ino(%v) err(%v) from(%v)", dirIno, err, from)
			return
		}
		batchNr := uint64(len(batches))
		if batchNr == 0 || (from != "" && batchNr == 1) {
			break
		} else if batchNr < DefaultReaddirLimit {
			noMore = true
		}
		if from != "" {
			batches = batches[1:]
		}
		for _, entry := range batches {
			select {
			case trash.traverseDirGoroutineLimit <- true:
				wg.Add(1)
				go func(parentIno uint64, entry string, isDir bool, fullPath string) {
					defer wg.Done()
					trash.deleteTask(parentIno, entry, isDir, fullPath)
					trash.releaseTraverseToken()
				}(dirIno, entry.Name, proto.IsDir(entry.Type), path.Join(dirName, entry.Name))
			default:
				trash.deleteTask(dirIno, entry.Name, proto.IsDir(entry.Type), path.Join(dirName, entry.Name))
			}
		}
		wg.Wait()
		from = batches[len(batches)-1].Name
	}
	//entries, err := trash.mw.ReadDir_ll(dirIno)
	//if err != nil {
	//	log.LogWarnf("action[deleteDir]delete %v failed: %v", dirName, err)
	//	return
	//}
	//delete sub files
	//for _, entry := range entries {
	//	log.LogDebugf("action[deleteDir]traverse  %v", entry.Name)
	//	if !proto.IsDir(entry.Type) {
	//		continue
	//	}
	//	trash.mw.AddInoInfoCache(entry.Inode, dirIno, entry.Name)
	//	select {
	//	case trash.traverseDirGoroutineLimit <- true:
	//		log.LogDebugf("action[deleteDir]launch goroutine  %v", entry.Name)
	//		wg.Add(1)
	//		go func(dirName string, dirIno uint64) {
	//			defer wg.Done()
	//			trash.removeAll(dirName, dirIno)
	//			trash.releaseTraverseToken()
	//		}(entry.Name, entry.Inode)
	//	default:
	//		log.LogDebugf("action[deleteDir]execute local  %v", entry.Name)
	//		trash.removeAll(entry.Name, entry.Inode)
	//	}
	//}
	//wg.Wait()
	////all sub files is deleted
	//for _, entry := range entries {
	//	select {
	//	case trash.traverseDirGoroutineLimit <- true:
	//		wg.Add(1)
	//		go func(parentIno uint64, entry string, isDir bool) {
	//			defer wg.Done()
	//			trash.deleteTask(parentIno, entry, isDir)
	//		}(dirIno, entry.Name, proto.IsDir(entry.Type))
	//	default:
	//		trash.deleteTask(dirIno, entry.Name, proto.IsDir(entry.Type))
	//	}
	//}
	//wg.Wait()
	log.LogDebugf("action[deleteDir] delete complete %v", dirName)
}

func (trash *Trash) extractTimeStampFromName(fileName string) (timeStamp int64, err error) {
	subs := strings.Split(fileName, "_")
	if len(subs) != 2 {
		return 0, errors.New("fileName format is not valid")
	}

	parsedTime, err := time.ParseInLocation(ExpiredTimeFormat, subs[1], time.Local)
	if err != nil {
		return 0, errors.New("fileName format is not valid")
	}
	return parsedTime.Unix(), nil
}

func (trash *Trash) pathIsExist(path string, isAsync bool) (bool, error) {
	// check cache first
	if value := trash.subDirCache.Get(path); value != nil {
		return true, nil
	}
	// check path exist but not in cache
	_, err := trash.mw.LookupPath(path, isAsync)
	if err != nil {
		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			log.LogDebugf("action[pathIsExist] %v not exist: %v", path, err.Error())
			return false, nil
		}
		return false, err
	}
	// info, err := trash.mw.InodeGet_ll(ino)
	// if err != nil {
	//	log.LogWarnf("action[pathIsExist] get %v inode info failed:%v", path, err.Error())
	// }
	// trash.subDirCache.Store(path, info)
	return true, nil
}

func (trash *Trash) pathIsExistInTrash(filePath string, isAsync bool) (bool, error) {
	// check cache first
	if value := trash.subDirCache.Get(filePath); value != nil {
		return true, nil
	}
	// check trashCurrent cache
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	if info := trash.subDirCache.Get(trashCurrent); info == nil {
		// current is rename
		return false, nil
	} else {
		currentIno := info.Inode
		_, _, err := trash.mw.Lookup_ll(currentIno, path.Base(filePath), isAsync)
		if err != nil {
			if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
}

func (trash *Trash) CreateDirectory(pino uint64, name string, mode, uid, gid uint32, fullName string, ignoreExist bool, isAsync bool) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0o777
	fuseMode |= uint32(os.ModeDir)
	return trash.mw.Create_ll(pino, name, fuseMode, uid, gid, nil, fullName, ignoreExist, isAsync)
}

func (trash *Trash) LookupEntry(parentID uint64, name string, isAsync bool) (*proto.InodeInfo, error) {
	child, _, err := trash.mw.Lookup_ll(parentID, name, isAsync)
	if err != nil {
		log.LogWarnf("action[LookupEntry] Lookup_ll %v failed:%v", name, err)
		return nil, err
	}
	info, err := trash.mw.InodeGet_ll(child, isAsync)
	if err != nil {
		log.LogWarnf("action[LookupEntry] InodeGet_ll %v failed:%v", name, err)
		return nil, err
	}
	return info, nil
}

func (trash *Trash) LookupPath(path string, byCache bool, isAsync bool) (*proto.InodeInfo, error) {
	if byCache {
		value := trash.subDirCache.Get(path)
		if value != nil {
			return value, nil
		}
	}
	log.LogDebugf("LookupPath miss   path %v ", path)
	// Use async version for trash operations to improve performance
	ino, err := trash.mw.LookupPath(path, isAsync)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("lookupPath path %v  failed:%v", path, err.Error()))
	}

	info, err := trash.mw.InodeGet_ll(ino, isAsync)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("InodeGet_ll path %v  failed:%v", path, err.Error()))
	}
	trash.subDirCache.Put(path, info)
	return info, nil
}

func (trash *Trash) createParentPathInTrash(parentPath, rootDir string) (err error) {
	// check .Trash/Current first
	// log.LogDebugf(string(debug.Stack()))
	var trashCurrent string
	if rootDir == CurrentName {
		trashCurrent = path.Join(trash.trashRoot, CurrentName)
		if err = trash.createCurrent(true, true); err != nil {
			return
		}
	} else {
		trashCurrent = rootDir
	}

	log.LogDebugf("action[createParentPathInTrash] ready to create %v in trash %v", parentPath, trashCurrent)
	subDirs := strings.Split(parentPath, "/")
	cur := trashCurrent
	trashCurrentIno := trash.subDirCache.Get(trashCurrent)
	if trashCurrentIno == nil {
		ino, _ := trash.mw.LookupPath(cur, true)
		trashCurrentIno, err = trash.mw.InodeGet_ll(ino, true)
		if err != nil {
			log.LogWarnf("action[createParentPathInTrash] get %v inode info failed:%v", cur, err.Error())
			return err
		}
		trash.subDirCache.Put(cur, trashCurrentIno)
	}
	parentIno := trashCurrentIno.Inode
	var info, parentInfo *proto.InodeInfo
	for _, sub := range subDirs {
		parentPath := cur
		cur = path.Join(cur, sub)
		log.LogDebugf("action[createParentPathInTrash] try to create %v ", cur)
		exists, checkErr := trash.pathIsExist(cur, true)
		if checkErr != nil {
			log.LogWarnf("action[createParentPathInTrash] check %v failed:%v", cur, checkErr)
			return checkErr
		}
		if exists {
			info := trash.subDirCache.Get(cur)
			if info == nil {
				ino, _ := trash.mw.LookupPath(cur, true)
				inoInfo, err := trash.mw.InodeGet_ll(ino, true)
				if err != nil {
					log.LogWarnf("action[createParentPathInTrash] get %v inode info failed:%v", cur, err.Error())
					return err
				}
				trash.subDirCache.Put(cur, inoInfo)
				parentIno = inoInfo.Inode
			} else {
				log.LogDebugf("action[createParentPathInTrash] pathIsExist  %v ", cur)
				parentIno = info.Inode
			}
			continue
		}
		// create sub parent
		log.LogDebugf("action[createParentPathInTrash] parentPath %v ", parentPath)
		info, err = trash.LookupPath(parentPath, true, true)
		if err != nil {
			log.LogWarnf("action[createParentPathInTrash] LookupPath origin %v failed:%v", parentPath, err.Error())
			// log.LogDebugf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
			return
		}
		if info == nil {
			panic(fmt.Sprintf("info should not be nil for parentPath %v", parentPath))
		}
		parentInfo, err = trash.CreateDirectory(parentIno, sub, info.Mode, info.Uid, info.Gid, path.Join(parentPath, sub), true, true)
		if err != nil {
			if err == syscall.EEXIST {
				log.LogDebugf("action[createParentPathInTrash] CreateDirectory  %v may be created by other routine", cur)
				// may be created by other routine
				info := trash.subDirCache.Get(path.Join(parentPath, sub))
				if info == nil {
					ino, _ := trash.mw.LookupPath(path.Join(parentPath, sub), true)
					inoInfo, err := trash.mw.InodeGet_ll(ino, true)
					if err != nil {
						log.LogWarnf("action[createParentPathInTrash] get %v inode info failed:%v", path.Join(parentPath, sub), err.Error())
						return err
					}
					trash.subDirCache.Put(path.Join(parentPath, sub), inoInfo)
					parentIno = inoInfo.Inode
				} else {
					log.LogDebugf("action[createParentPathInTrash] pathIsExist  %v ", path.Join(parentPath, sub))
					parentIno = info.Inode
				}
				continue
			} else if strings.Contains(err.Error(), syscall.EDQUOT.Error()) ||
				strings.Contains(err.Error(), syscall.ENOSPC.Error()) ||
				strings.Contains(err.Error(), syscall.ENOMEM.Error()) {
				trash.deleteTask(parentIno, sub, true, path.Join(parentPath, sub))
				return
			} else {
				log.LogWarnf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
				return
			}
		}
		if parentInfo == nil {
			panic(fmt.Sprintf("parentInfo should not be nil for parentPath %v", parentPath))
		}
		parentIno = parentInfo.Inode
		trash.subDirCache.Put(cur, parentInfo)
		log.LogDebugf("action[createParentPathInTrash] CreateDirectory  %v success", cur)
	}
	return
}

func (trash *Trash) renameToTrashTempFile(parentIno, currentIno uint64, oldPath, newPath string, isAsync bool) error {
	err := trash.mw.Rename_ll(parentIno, path.Base(oldPath), currentIno, path.Base(newPath), oldPath, newPath, true, isAsync)
	if err == syscall.ENOENT {
		log.LogErrorf("action[renameToTrashTempFile] rename src %v err ENOENT", oldPath)
		srcParentMP := trash.mw.getPartitionByInode(parentIno)
		if srcParentMP == nil {
			return syscall.ENOENT
		}
		status, _, _, _ := trash.mw.lookup(srcParentMP, parentIno, path.Base(oldPath), trash.mw.LastVerSeq, isAsync)
		if status == statusNoent {
			return nil
		}
	}
	return err
}

func (trash *Trash) rename(oldPath, newPath string) error {
	oldParent := path.Dir(oldPath)
	newParent := path.Dir(newPath)
	start := time.Now()
	oldInfo, err := trash.LookupPath(oldParent, true, true)
	log.LogDebugf("action[rename] LookupPath oldParent %v consume %v", oldParent, time.Since(start).Seconds())
	if err != nil {
		log.LogWarnf("action[rename] lookup %v failed %v", oldParent, err.Error())
		return err

	}
	start = time.Now()
	newInfo, err := trash.LookupPath(newParent, true, true)
	log.LogDebugf("action[rename] LookupPath newParent %v consume %v", newParent, time.Since(start).Seconds())
	if err != nil {
		log.LogWarnf("action[rename] lookup %v failed %v", newParent, err.Error())
		return err
	}

	return trash.mw.Rename_ll(oldInfo.Inode, path.Base(oldPath), newInfo.Inode, path.Base(newPath), oldPath, newPath, true, true)
}

func (trash *Trash) IsTrashRoot(parentIno uint64, name string) (bool, error) {
	info, err := trash.LookupPath(trash.mountPath, true, false)
	if err != nil {
		return false, err
	}
	if info != nil && info.Inode == parentIno && name == TrashPrefix {
		return true, nil
	}
	return false, nil
}

func (trash *Trash) ReadDir(path string) ([]proto.Dentry, error) {
	info, err := trash.LookupPath(path, true, false)
	if err != nil {
		log.LogWarnf("lookupPath %v failed:%v", path, err.Error())
		return nil, err
	}
	return trash.mw.ReadDir_ll(info.Inode, true)
}

func (trash *Trash) deleteTask(parentIno uint64, entry string, isDir bool, fullPath string) {
	info, err := trash.mw.Delete_ll(parentIno, entry, isDir, fullPath, true)
	if err != nil {
		log.LogWarnf("Delete_ll %v failed:%v", entry, err.Error())
		return
	}
	if !isDir {
		if info == nil {
			log.LogErrorf("deleteTask unexpected nil info %v %v", parentIno, entry)
			return
		}
		trash.mw.Evict(info.Inode, fullPath, true)
	}
	log.LogDebugf("Delete_ll %v success", entry)
}

func (trash *Trash) releaseTraverseToken() {
	select {
	case <-trash.traverseDirGoroutineLimit:
		return
	default:
		return
	}
}

func (trash *Trash) buildDeletedFileParentDirsBackground() {
	rebuildTicker := time.NewTicker(5 * time.Second)
	defer rebuildTicker.Stop()
	for range rebuildTicker.C {
		if !trash.getLock {
			log.LogDebugf("buildDeletedFileParentDirsBackground: trash get lock failed, not execute")
			continue
		}

		trash.buildDeletedFileParentDirs()
		trash.buildDeletedFileParentDirsForExpired()
	}
}

type RebuildTask struct {
	Name    string
	Type    uint32
	Inode   uint64
	FileIno uint64
	SrcDir  string
}

func (trash *Trash) buildDeletedFileParentDirs() {
	if atomic.LoadInt32(&trash.rebuildStatus) == rebuildRunning {
		log.LogDebugf("action[buildDeletedFileParentDirs] is running")
		return
	}
	atomic.StoreInt32(&trash.rebuildStatus, rebuildRunning)
	defer atomic.StoreInt32(&trash.rebuildStatus, rebuildStop)
	log.LogDebugf("action[buildDeletedFileParentDirs] start")

	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	exists, err := trash.pathIsExist(trashCurrent, true)
	if err != nil || !exists {
		return
	}

	targetRoot, err := trash.ensureRebuildRoot(trashCurrent)
	if err != nil {
		log.LogWarnf("action[buildDeletedFileParentDirs] ensureRebuildRoot failed: %v", err)
		return
	}

	bucketRoot := path.Join(trashCurrent, BucketRootPrefix)
	exists, err = trash.pathIsExist(bucketRoot, true)
	if err != nil || !exists {
		log.LogDebugf("action[buildDeletedFileParentDirs] bucketRoot %v not exist", bucketRoot)
		return
	}

	taskCh := make(chan RebuildTask, 1024)
	wg := sync.WaitGroup{}
	rebuildTaskFunc := func() {
		defer wg.Done()
		for task := range taskCh {
			if proto.IsDir(task.Type) {
				trash.rebuildDir(task, targetRoot)
			} else {
				trash.rebuildFile(task, targetRoot)
			}
		}
	}

	for i := 0; i < trash.rebuildGoroutineLimit; i++ {
		wg.Add(1)
		go rebuildTaskFunc()
	}

	// iterate buckets
	bucketInfo, err := trash.LookupPath(bucketRoot, true, true)
	if err != nil {
		log.LogWarnf("action[buildDeletedFileParentDirs] lookup bucketRoot failed: %v", err)
		close(taskCh)
		wg.Wait()
		return
	}
	bucketEntries, err := trash.mw.ReadDir_ll(bucketInfo.Inode, true)
	if err != nil {
		log.LogWarnf("action[buildDeletedFileParentDirs] read bucketRoot failed: %v", err)
		close(taskCh)
		wg.Wait()
		return
	}
	for _, bucket := range bucketEntries {
		if !proto.IsDir(bucket.Type) {
			continue
		}
		bucketPath := path.Join(bucketRoot, bucket.Name)
		var (
			noMore = false
			from   = ""
		)
		for !noMore {
			batches, err := trash.mw.ReadDirLimit_ll(bucket.Inode, from, DefaultReaddirLimit, true)
			if err != nil {
				log.LogWarnf("action[buildDeletedFileParentDirs] ReadDirLimit_ll: bucket(%v) err(%v) from(%v)", bucketPath, err, from)
				break
			}
			batchNr := uint64(len(batches))
			if batchNr == 0 || (from != "" && batchNr == 1) {
				break
			} else if batchNr < DefaultReaddirLimit {
				noMore = true
			}
			if from != "" {
				batches = batches[1:]
			}
			for _, child := range batches {
				log.LogDebugf("action[buildDeletedFileParentDirs] rebuild %v type %v", child.Name, child.Type)
				if strings.Contains(child.Name, ParentDirPrefix) || strings.Contains(child.Name, LongNamePrefix) {
					taskCh <- RebuildTask{
						Name:    child.Name,
						Type:    child.Type,
						Inode:   bucket.Inode,
						FileIno: child.Inode,
						SrcDir:  bucketPath,
					}
				}
			}
			from = batches[len(batches)-1].Name
		}
	}

	close(taskCh)
	wg.Wait()
	log.LogDebugf("action[buildDeletedFileParentDirs] end")
}

func (trash *Trash) buildDeletedFileParentDirsForExpired() {
	defer log.LogDebugf("action[buildDeletedFileParentDirsForExpired]exit")
	log.LogDebugf("action[buildDeletedFileParentDirsForExpired]enter")
	// read trash root
	entries, err := trash.mw.ReadDir_ll(trash.trashRootIno, true)
	if err != nil {
		log.LogWarnf("action[buildDeletedFileParentDirsForExpired]ReadDir trashRoot  failed: %v", err.Error())
		return
	}

	for _, entry := range entries {
		if !proto.IsDir(entry.Type) {
			continue
		}
		// only rebuild expired dir
		if !strings.HasPrefix(entry.Name, ExpiredPrefix) {
			continue
		}
		bucketRoot := path.Join(trash.trashRoot, entry.Name, BucketRootPrefix)
		exists, err := trash.pathIsExist(bucketRoot, true)
		if err != nil || !exists {
			continue
		}
		targetRoot, err := trash.ensureRebuildRoot(path.Join(trash.trashRoot, entry.Name))
		if err != nil {
			log.LogWarnf("action[buildDeletedFileParentDirsForExpired] ensureRebuildRoot failed:%v", err)
			continue
		}
		bucketRootInfo, err := trash.LookupPath(bucketRoot, true, true)
		if err != nil {
			log.LogWarnf("action[buildDeletedFileParentDirsForExpired] lookup bucketRoot failed: %v", err)
			continue
		}
		bucketEntries, err := trash.mw.ReadDir_ll(bucketRootInfo.Inode, true)
		if err != nil {
			log.LogWarnf("action[buildDeletedFileParentDirsForExpired] read bucketRoot failed: %v", err)
			continue
		}
		var (
			taskCh = make(chan RebuildTask, 1024)
			wg     = sync.WaitGroup{}
		)
		rebuildTaskFunc := func() {
			defer wg.Done()
			for task := range taskCh {
				if proto.IsDir(task.Type) {
					trash.rebuildDir(task, targetRoot)
				} else {
					trash.rebuildFile(task, targetRoot)
				}
			}
		}
		for i := 0; i < trash.rebuildGoroutineLimit; i++ {
			wg.Add(1)
			go rebuildTaskFunc()
		}
		for _, bucket := range bucketEntries {
			if !proto.IsDir(bucket.Type) {
				continue
			}
			bucketPath := path.Join(bucketRoot, bucket.Name)
			var (
				noMore = false
				from   = ""
			)
			for !noMore {
				batches, err := trash.mw.ReadDirLimit_ll(bucket.Inode, from, DefaultReaddirLimit, true)
				if err != nil {
					log.LogErrorf("action[buildDeletedFileParentDirsForExpired] ReadDirLimit_ll: bucket(%v) err(%v) from(%v)", bucketPath, err, from)
					break
				}
				batchNr := uint64(len(batches))
				if batchNr == 0 || (from != "" && batchNr == 1) {
					break
				} else if batchNr < DefaultReaddirLimit {
					noMore = true
				}
				if from != "" {
					batches = batches[1:]
				}
				for _, child := range batches {
					log.LogDebugf("action[buildDeletedFileParentDirsForExpired] rebuild %v type %v", child.Name, child.Type)
					if strings.Contains(child.Name, ParentDirPrefix) || strings.Contains(child.Name, LongNamePrefix) {
						taskCh <- RebuildTask{Name: child.Name, Type: child.Type, Inode: bucket.Inode, FileIno: child.Inode, SrcDir: bucketPath}
					}
				}
				from = batches[len(batches)-1].Name
			}
		}
		close(taskCh)
		wg.Wait()
		log.LogDebugf("action[buildDeletedFileParentDirsForExpired] %v end", entry.Name)
	}
}

func (trash *Trash) rebuildFile(task RebuildTask, targetRoot string) {
	log.LogDebugf("action[rebuildFile]: rebuild file %v in %v", task.Name, task.SrcDir)
	originName := task.Name
	fileName := trash.recoverPosixPathName(task.Name, task.FileIno)
	log.LogDebugf("action[rebuildFile]: recover  %v to  %v", originName, fileName)
	parentDir := path.Dir(fileName)
	baseName := path.Base(fileName)
	srcPath := path.Join(task.SrcDir, originName)

	if parentDir == "." { // file in trash root
		dstPath := path.Join(targetRoot, baseName)
		exists, err := trash.pathIsExist(dstPath, true)
		if err != nil {
			return
		}
		if exists {
			dstPath = path.Join(targetRoot, fmt.Sprintf("%s_%v", baseName, time.Now().Unix()))
		}
		if err := trash.rename(srcPath, dstPath); err != nil {
			log.LogWarnf("action[rebuildFile]: recover  %v to  %v failed:err %v",
				srcPath, dstPath, err)
		}
	} else {
		exists, err := trash.pathIsExist(path.Join(targetRoot, parentDir), true)
		if err != nil {
			return
		}
		if !exists {
			if err := trash.createParentPathInTrash(parentDir, targetRoot); err != nil {
				return
			}
		}
		dstPath := path.Join(targetRoot, parentDir, baseName)
		exists, err = trash.pathIsExist(dstPath, true)
		if err != nil {
			return
		}
		if exists {
			dstPath = path.Join(targetRoot, parentDir, fmt.Sprintf("%s_%v", baseName, time.Now().Unix()))
		}
		if err := trash.rename(srcPath, dstPath); err != nil {
			log.LogWarnf("action[rebuildFile]: recover  %v to  %v failed:err %v",
				srcPath, dstPath, err.Error())
		}
	}
}

func (trash *Trash) rebuildDir(task RebuildTask, targetRoot string) {
	log.LogDebugf("action[rebuildDir]: rebuild dir %v in %v", task.Name, task.SrcDir)
	originName := task.Name
	dirName := trash.recoverPosixPathName(task.Name, task.FileIno)
	var err error
	err = trash.createParentPathInTrash(dirName, targetRoot)
	if err != nil {
		log.LogDebugf("action[rebuildDir]: createParentPathInTrash %v failed:err %v",
			dirName, err)
		return
	}
	log.LogDebugf("action[rebuildDir]: delete dir %v in %v[%v]", dirName, targetRoot, task.Inode)
	if _, err = trash.mw.Delete_ll(task.Inode, path.Base(originName), true, path.Join(task.SrcDir, originName), true); err != nil {
		log.LogDebugf("action[rebuildDir]: delete encoded dir %v failed: %v", path.Join(task.SrcDir, originName), err)
	}
}

func (trash *Trash) recoverPosixPathName(fileName string, fileIno uint64) string {
	if strings.HasPrefix(fileName, LongNamePrefix) {
		log.LogDebugf("action[recoverPosixPathName] %v is long ino %v", fileName, fileIno)
		info, err := trash.mw.InodeGet_ll(fileIno, true)
		if err != nil {
			log.LogWarnf("action[recoverPosixPathName]:InodeGet_ll for %v[%v] failed:%v",
				fileName, fileIno, err.Error())
			fileName = strings.ReplaceAll(fileName, LongNamePrefix, "/")
			// remove uuid
			return strings.Split(fileName, ParentDirPrefix)[0]
		} else {
			log.LogDebugf("action[recoverPosixPathName]:XAttrGet_ll for %v", fileName)
			attrInfo, err := trash.mw.XAttrGet_ll(info.Inode, OriginalName, true)
			if err != nil {
				log.LogWarnf("action[recoverPosixPathName]:XAttrGet_ll for %v[%v] failed:%v",
					fileName, fileIno, err.Error())
				log.LogDebugf("action[recoverPosixPathName]:XAttrGet_ll for %v[%v] failed:%v",
					fileName, fileIno, err.Error())
				// remove uuid
				fileName = strings.ReplaceAll(fileName, LongNamePrefix, "/")
				return strings.Split(fileName, ParentDirPrefix)[0]
			}
			newFileName := attrInfo.XAttrs[OriginalName]
			if newFileName == "" {
				log.LogWarnf("action[recoverPosixPathName]:XAttrGet_ll get empty name for %v", fileName)
				fileName = strings.ReplaceAll(fileName, LongNamePrefix, "/")
				fileName = strings.Split(fileName, ParentDirPrefix)[0]
			} else {
				fileName = newFileName
			}
			log.LogDebugf("action[recoverPosixPathName] fileName %v is read from xattr ", fileName)
		}
	}
	fileName = strings.TrimPrefix(fileName, ParentDirPrefix)
	fileName = strings.ReplaceAll(fileName, ParentDirPrefix, "/")
	return fileName
}

func (trash *Trash) deleteSrcDirDirectly(parentIno uint64, fileName, fullPath string, isAsync bool) error {
	srcParentMP := trash.mw.getPartitionByInode(parentIno)
	if srcParentMP == nil {
		return syscall.ENOENT
	}
	status, _, _, err := trash.mw.ddelete(srcParentMP, parentIno, fileName, 0, trash.mw.LastVerSeq, fullPath, isAsync)
	if err != nil {
		log.LogErrorf("deleteSrcDirDirectly delete %v failed.err %v", fullPath, err)
		return statusToErrno(status)
	}
	return nil
}
