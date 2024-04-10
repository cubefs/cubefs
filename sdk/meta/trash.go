package meta

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"

	//"syscall"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"os"
	"path"
	"strings"
	"time"
)

// put deleted files or directories into this folder under Trash
const CurrentName = "Current"
const TrashPrefix = ".Trash"
const ExpiredPrefix = "Expired"
const ParentDirPrefix = "|__|"
const ExpiredTimeFormat = "2006-01-02-150405"
const FileNameLengthMax = 255
const LongNamePrefix = "LongName____"
const OriginalName = "OriginalName"
const DefaultReaddirLimit = 4096
const TrashPathIgnore = "trashPathIgnore"
const OneDayMinutes = 24 * 60

const (
	DisableTrash = "/trash/disable"
	QueryTrash   = "/trash/query"
)

type Trash struct {
	mw                        *MetaWrapper
	mountPath                 string
	mountPoint                string
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
	//create trash root
	if err := trash.InitTrashRoot(); err != nil {
		return nil, err
	}
	go trash.deleteWorker()
	go trash.buildDeletedFileParentDirsBackground()
	return trash, nil
}

func (trash *Trash) InitTrashRoot() (err error) {
	//trash.trashRoot = path.Join(trash.mountPoint, trash.mountPath, TrashPrefix)
	trash.trashRoot = path.Join(trash.mountPath, TrashPrefix)
	log.LogDebugf("action[InitTrashRoot] %v ", trash.trashRoot)
	//check trash root exist
	if trash.pathIsExist(trash.trashRoot) {
		trash.initTrashRootInodeInfo()
		log.LogDebugf("action[InitTrashRoot] trash root is exist")
		return nil
	}

	parentDirInfo, err := trash.LookupPath(path.Clean(trash.mountPath), true)
	if err != nil {
		log.LogErrorf("action[InitTrashRoot]LookupPath trash parent failed: %v", err.Error())
		return err
	}
	_, err = trash.CreateDirectory(parentDirInfo.Inode, TrashPrefix,
		parentDirInfo.Mode, parentDirInfo.Uid, parentDirInfo.Gid, TrashPrefix, false)
	if err != nil {
		log.LogErrorf("action[InitTrashRoot]create trash root failed: %v", err.Error())
		return err
	}
	trash.initTrashRootInodeInfo()
	return nil
}

func (trash *Trash) initTrashRootInodeInfo() {
	trashRootInfo, _ := trash.LookupPath(trash.trashRoot, true)
	trash.trashRootIno = trashRootInfo.Inode
	trash.trashRootMode = trashRootInfo.Mode
	trash.trashRootUid = trashRootInfo.Uid
	trash.trashRootGid = trashRootInfo.Gid
}

func (trash *Trash) createCurrent(ingoreExist bool) (err error) {
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	log.LogDebugf("action[createCurrent] enter")
	if trash.pathIsExist(trashCurrent) {
		//cache trashCurrent if not cached
		if value := trash.subDirCache.Get(trashCurrent); value == nil {
			ino, _ := trash.mw.LookupPath(trashCurrent)
			info, err := trash.mw.InodeGet_ll(ino)
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
		trash.trashRootMode, trash.trashRootUid, trash.trashRootGid, path.Join(TrashPrefix, CurrentName), ingoreExist)
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

func (trash *Trash) findFileFromExpired(fileName string) (info *proto.InodeInfo, err error) {
	log.LogDebugf("action[findFileFromExpired]find %v", fileName)
	entries, err := trash.mw.ReadDir_ll(trash.trashRootIno)
	if err != nil {
		log.LogWarnf("action[findFileFromExpired]ReadDir trashRoot  failed: %v", err.Error())
		return nil, err
	}
	for _, entry := range entries {
		if !proto.IsDir(entry.Type) {
			continue
		}
		//skip current
		if strings.Compare(entry.Name, CurrentName) == 0 {
			continue
		}
		dstPath := path.Join(trash.trashRoot, entry.Name, fileName)
		log.LogDebugf("action[findFileFromExpired]try find %v", dstPath)
		info, err = trash.LookupPath(dstPath, false)
		if err != nil {
			log.LogDebugf("action[findFileFromExpired]find  %v failed: %v", dstPath, err.Error())
			continue
		} else {
			return info, nil
		}
	}
	return nil, err
}

func (trash *Trash) MoveToTrash(parentPathAbsolute string, parentIno uint64, fileName string, isDir bool) (err error) {
	start := time.Now()
	defer func() {
		log.LogDebugf("action[MoveToTrash] : parentPathAbsolute(%v) fileName(%v) consume %v", parentPathAbsolute, fileName, time.Since(start).Seconds())
	}()
	log.LogDebugf("action[MoveToTrash] : parentPathAbsolute(%v) fileName(%v) parentIno（%v）", parentPathAbsolute, fileName, parentIno)
	if err = trash.createCurrent(true); err != nil {
		return err
	}
	//save current ino to prevent renaming current to expired
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	trashCurrentIno := trash.subDirCache.Get(trashCurrent).Inode
	srcPath := path.Join(trash.mountPath, parentPathAbsolute, fileName)
	//generate tmp file name
	tmpFileName := fmt.Sprintf("%v%v", trash.generateTmpFileName(parentPathAbsolute), fileName)
	dstPath := path.Join(trash.trashRoot, CurrentName, tmpFileName)
	startCheck := time.Now()
	for {
		if trash.pathIsExistInTrash(dstPath) {
			if !isDir {
				// ignore dir rename
				dstPath = fmt.Sprintf("%s_%v", dstPath, time.Now().Unix())
				//		log.LogDebugf("action[MoveToTrash]filePathInTrash rename to %v", dstPath)
			} else {
				// delete src dir directly
				err := trash.deleteSrcDirDirectly(parentIno, fileName, srcPath)
				if err != nil {
					return err
				}
				break
			}
		} else {
			log.LogDebugf("action[MoveToTrash]break")
			break
		}
	}
	log.LogDebugf("action[MoveToTrash]  startCheck: srcPath(%v) dstPath(%v) consume %v", srcPath, dstPath, time.Since(startCheck).Seconds())
	startRename := time.Now()
	var (
		needStoreXattr = false
		originName     string
	)

	if len(path.Base(dstPath)) > FileNameLengthMax {
		needStoreXattr = true
		dstPath, originName = transferLongFileName(dstPath)
	}
	err = trash.renameToTrashTempFile(parentIno, trashCurrentIno, srcPath, dstPath)
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
			info, err = trash.LookupEntry(parentID, path.Base(dstPath))
			if err != nil {
				log.LogWarnf("action[MoveToTrash] LookupEntry %v failed:%v", dstPath, err.Error())
				return
			}

			err = trash.mw.XAttrSet_ll(info.Inode, []byte(OriginalName), []byte(originName))
			if err != nil {
				log.LogWarnf("action[MoveToTrash] set xattr for %v[%v] failed:%v", dstPath, info.Inode, err.Error())
				return
			}
			log.LogDebugf("action[MoveToTrash] set xattr for %v [%v]success:%v", dstPath, info.Inode, originName)
		}(originName, dstPath, trashCurrentIno)
	}
	//nil to check tmp file exist
	trash.subDirCache.Put(dstPath, &proto.InodeInfo{})
	log.LogDebugf("action[MoveToTrash] rename %v to %v success", srcPath, dstPath)
	return nil
}

func transferLongFileName(filePath string) (newName, oldName string) {
	oldName = path.Base(filePath)
	parentPath := path.Dir(filePath)
	if strings.HasPrefix(oldName, ParentDirPrefix) {
		newName = strings.TrimPrefix(oldName, ParentDirPrefix)
	}
	newName = strings.ReplaceAll(oldName, ParentDirPrefix, "/")
	newBaseName := path.Base(newName)
	return path.Join(parentPath, LongNamePrefix+newBaseName+ParentDirPrefix+uuid.New().String()), oldName
}
func (trash *Trash) getDeleteInterval() int64 {
	checkPointInterval := atomic.LoadInt64(&trash.deleteInterval) / 4
	if checkPointInterval == 0 {
		checkPointInterval = 1
	}
	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(50)
	//If the time interval is longer than one day
	if checkPointInterval > OneDayMinutes {
		checkPointInterval += int64(randomNumber * 30)
	} else {
		checkPointInterval += int64(randomNumber)
	}
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
			//delete expired directory
			trash.deleteExpiredData()
			//rename current directory(expired_timestamp)
			trash.renameCurrent()
			checkPointInterval = trash.getDeleteInterval()
			t.Reset(time.Duration(checkPointInterval) * time.Minute)
		}
	}
}

func (trash *Trash) renameCurrent() {
	log.LogDebugf("action[renameCurrent]enter")
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	if !trash.pathIsExist(trashCurrent) {
		return
	}
	//if current is rebuilding
	for {
		if atomic.LoadInt32(&trash.rebuildStatus) == rebuildRunning {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	//keep current for 1/4 delete interval
	ino, _ := trash.mw.LookupPath(trashCurrent)
	inoInfo, err := trash.mw.InodeGet_ll(ino)
	if err != nil {
		log.LogWarnf("action[renameCurrent]get inode for trashCurrent failed %v", err.Error())
		return
	}
	checkPointInterval := trash.deleteInterval / 4
	if time.Now().Sub(inoInfo.CreateTime) < (time.Duration(checkPointInterval) * time.Minute) {
		log.LogDebugf("action[renameCurrent]trashCurrent keep for 1/4 interval %v", time.Now().Sub(inoInfo.CreateTime).String())
		return
	}
	//ensure files in current is rebuild
	trash.buildDeletedFileParentDirs()
	trashModifyTime := inoInfo.ModifyTime
	for {
		expiredTrash := fmt.Sprintf("%s_%v", ExpiredPrefix, trashModifyTime.Format(ExpiredTimeFormat))
		if err := trash.rename(trashCurrent, path.Join(trash.trashRoot, expiredTrash)); err != nil {
			//if err := trash.mw.Rename_ll(trash.trashRootIno, CurrentName, trash.trashRootIno,
			//	expiredTrash, false); err != nil
			log.LogDebugf("action[renameCurrent]rename current  failed: %v", err.Error())
			time.Sleep(time.Millisecond * 100)
			trashModifyTime.Add(100 * time.Millisecond)
		} else {
			log.LogDebugf("action[renameCurrent]rename current  completed")
			//clear cache
			trash.subDirCache.Clear()
			break
		}
	}
}

func (trash *Trash) deleteExpiredData() {
	defer log.LogDebugf("action[deleteExpiredData]exit")
	log.LogDebugf("action[deleteExpiredData]enter")
	//read trash root
	entries, err := trash.mw.ReadDir_ll(trash.trashRootIno)
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
		//skip current
		if strings.Compare(entry.Name, CurrentName) == 0 {
			continue
		}
		//extract timestamp from name
		err, checkPoint := trash.extractTimeStampFromName(entry.Name)
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
		batches, err := trash.mw.ReadDirLimit_ll(dirIno, from, DefaultReaddirLimit)
		if err != nil {
			log.LogErrorf("action[removeAll] ReadDirLimit_ll: ino(%v) err(%v) from(%v)", dirIno, err, from)
			return
		}
		batchNr := uint64(len(batches))
		if batchNr == 0 || (from != "" && batchNr == 1) {
			noMore = true
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
		batches, err := trash.mw.ReadDirLimit_ll(dirIno, from, DefaultReaddirLimit)
		if err != nil {
			log.LogErrorf("action[removeAll] ReadDirLimit_ll: ino(%v) err(%v) from(%v)", dirIno, err, from)
			return
		}
		batchNr := uint64(len(batches))
		if batchNr == 0 || (from != "" && batchNr == 1) {
			noMore = true
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

func (trash *Trash) extractTimeStampFromName(fileName string) (err error, timeStamp int64) {
	subs := strings.Split(fileName, "_")
	if len(subs) != 2 {
		return errors.New(fmt.Sprintf("fileName format is not valid")), 0
	}

	parsedTime, err := time.ParseInLocation(ExpiredTimeFormat, subs[1], time.Local)
	if err != nil {
		return errors.New(fmt.Sprintf("fileName format is not valid")), 0
	}
	return nil, parsedTime.Unix()
}

func (trash *Trash) pathIsExist(path string) bool {
	//check cache first
	if value := trash.subDirCache.Get(path); value != nil {
		return true
	}
	//check path exist but not in cache
	_, err := trash.mw.LookupPath(path)
	if err != nil {
		log.LogDebugf("action[pathIsExist] %v not exist: %v", path, err.Error())
		return false
	}
	//info, err := trash.mw.InodeGet_ll(ino)
	//if err != nil {
	//	log.LogWarnf("action[pathIsExist] get %v inode info failed:%v", path, err.Error())
	//}
	//trash.subDirCache.Store(path, info)
	return true
}

func (trash *Trash) pathIsExistInTrash(filePath string) bool {
	//check cache first
	if value := trash.subDirCache.Get(filePath); value != nil {
		return true
	}
	//check trashCurrent cache
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	if info := trash.subDirCache.Get(trashCurrent); info == nil {
		//current is rename
		return false
	} else {
		currentIno := info.Inode
		_, _, err := trash.mw.Lookup_ll(currentIno, path.Base(filePath))
		if err != nil {
			return false
		}
		return true
	}
}

func (trash *Trash) IsDir(path string) bool {
	info, err := trash.LookupPath(path, true)
	if err != nil {
		log.LogWarnf("action[IsDir]%v err:%v ", path, err.Error())
		return false
	}
	return proto.IsDir(info.Mode)

}

func (trash *Trash) CreateDirectory(pino uint64, name string, mode, uid, gid uint32, fullName string, ignoreExist bool) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeDir)
	return trash.mw.Create_ll(pino, name, fuseMode, uid, gid, nil, fullName, ignoreExist)
}

func (trash *Trash) LookupEntry(parentID uint64, name string) (*proto.InodeInfo, error) {
	child, _, err := trash.mw.Lookup_ll(parentID, name)
	if err != nil {
		log.LogWarnf("action[LookupEntry] Lookup_ll %v failed:%v", name, err)
		return nil, err
	}
	info, err := trash.mw.InodeGet_ll(child)
	if err != nil {
		log.LogWarnf("action[LookupEntry] InodeGet_ll %v failed:%v", name, err)
		return nil, err
	}
	return info, nil
}
func (trash *Trash) LookupPath(path string, byCache bool) (*proto.InodeInfo, error) {
	if byCache {
		value := trash.subDirCache.Get(path)
		if value != nil {
			return value, nil
		}
	}
	log.LogDebugf("LookupPath miss   path %v ", path)
	ino, err := trash.mw.LookupPath(path)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("lookupPath path %v  failed:%v", path, err.Error()))
	}

	info, err := trash.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("InodeGet_ll path %v  failed:%v", path, err.Error()))
	}
	//trash.subDirCache.Store(path, info)
	return info, nil
}

func (trash *Trash) createParentPathInTrash(parentPath, rootDir string) (err error) {
	//check .Trash/Current first
	//log.LogDebugf(string(debug.Stack()))
	var trashCurrent string
	if rootDir == CurrentName {
		trashCurrent = path.Join(trash.trashRoot, CurrentName)
		if err = trash.createCurrent(true); err != nil {
			return
		}
	} else {
		trashCurrent = rootDir
	}

	log.LogDebugf("action[createParentPathInTrash] ready  to create %v in trash %v", parentPath, trashCurrent)
	subDirs := strings.Split(parentPath, "/")
	cur := trashCurrent
	trashCurrentIno := trash.subDirCache.Get(trashCurrent)
	if trashCurrentIno == nil {
		ino, _ := trash.mw.LookupPath(cur)
		trashCurrentIno, err = trash.mw.InodeGet_ll(ino)
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
		log.LogDebugf("action[createParentPathInTrash] try  to create %v ", cur)
		if trash.pathIsExist(cur) {
			info := trash.subDirCache.Get(cur)
			if info == nil {
				ino, _ := trash.mw.LookupPath(cur)
				inoInfo, err := trash.mw.InodeGet_ll(ino)
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
		//create sub parent
		log.LogDebugf("action[createParentPathInTrash] parentPath %v ", parentPath)
		info, err = trash.LookupPath(parentPath, true)
		if err != nil {
			log.LogWarnf("action[createParentPathInTrash] LookupPath origin %v failed:%v", parentPath, err.Error())
			//log.LogDebugf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
			return
		}
		if info == nil {
			panic(fmt.Sprintf("info should not be nil for parentPath %v", parentPath))
			return
		}
		parentInfo, err = trash.CreateDirectory(parentIno, sub, info.Mode, info.Uid, info.Gid, path.Join(parentPath, sub), true)
		if err != nil {
			if err == syscall.EEXIST {
				log.LogDebugf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
			} else {
				log.LogWarnf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
			}
			return
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

func (trash *Trash) renameToTrashTempFile(parentIno, currentIno uint64, oldPath, newPath string) error {
	err := trash.mw.Rename_ll(parentIno, path.Base(oldPath), currentIno, path.Base(newPath), oldPath, newPath, true)
	if err == syscall.ENOENT {
		log.LogErrorf("action[renameToTrashTempFile] rename src %v err ENOENT", oldPath)
		srcParentMP := trash.mw.getPartitionByInode(parentIno)
		if srcParentMP == nil {
			return syscall.ENOENT
		}
		status, _, _, _ := trash.mw.lookup(srcParentMP, parentIno, path.Base(oldPath))
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
	oldInfo, err := trash.LookupPath(oldParent, true)
	log.LogDebugf("action[rename]  LookupPath  oldParent %v consume %v", oldParent, time.Since(start).Seconds())
	if err != nil {
		log.LogWarnf("action[rename] lookup  %v failed %v", oldParent, err.Error())
		return err

	}
	start = time.Now()
	newInfo, err := trash.LookupPath(newParent, true)
	log.LogDebugf("action[rename] LookupPath  newParent %v consume %v", newParent, time.Since(start).Seconds())
	if err != nil {
		log.LogWarnf("action[rename] lookup  %v failed %v", newParent, err.Error())
		return err
	}

	return trash.mw.Rename_ll(oldInfo.Inode, path.Base(oldPath), newInfo.Inode, path.Base(newPath), oldPath, newPath, true)
}

func (trash *Trash) deleteSrcDir(dirPath string) error {
	parentDir := path.Dir(dirPath)
	parentInfo, err := trash.LookupPath(parentDir, true)
	if err != nil {
		log.LogDebugf("action[deleteSrcDir] lookup  %v failed %v", parentDir, err.Error())
		return err

	}
	_, err = trash.mw.Delete_ll(parentInfo.Inode, path.Base(dirPath), true, dirPath)
	return err
}

func (trash *Trash) IsTrashRoot(parentIno uint64, name string) bool {
	info, _ := trash.LookupPath(trash.mountPath, true)
	if info.Inode == parentIno && name == TrashPrefix {
		return true
	}
	return false
}

func (trash *Trash) ReadDir(path string) ([]proto.Dentry, error) {
	info, err := trash.LookupPath(path, true)
	if err != nil {
		log.LogWarnf("lookupPath %v failed:%v", path, err.Error())
		return nil, err
	}
	return trash.mw.ReadDir_ll(info.Inode)
}

func (trash *Trash) deleteTask(parentIno uint64, entry string, isDir bool, fullPath string) {
	info, err := trash.mw.Delete_ll(parentIno, entry, isDir, fullPath)
	if err != nil {
		log.LogWarnf("Delete_ll %v failed:%v", entry, err.Error())
		return
	}
	if !isDir {
		if info == nil {
			log.LogErrorf("deleteTask unexpected nil info %v %v", parentIno, entry)
			return
		}
		trash.mw.Evict(info.Inode, fullPath)
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
	rebuildTimer := time.NewTimer(5 * time.Second)
	defer rebuildTimer.Stop()
	for {
		select {
		case <-rebuildTimer.C:
			trash.buildDeletedFileParentDirs()
			trash.buildDeletedFileParentDirsForExpired()
			rebuildTimer.Reset(5 * time.Second)
		case <-trash.done:
			log.LogWarnf("buildDeletedFileParentDirs stopped!")
			return
		}
	}
}

type RebuildTask struct {
	Name    string
	Type    uint32
	Inode   uint64
	FileIno uint64
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
	if !trash.pathIsExist(trashCurrent) {
		//log.LogWarnf("action[buildDeletedFileParentDirs] trashCurrent is not exist")
		return
	}
	//readdir
	var (
		trashInfo *proto.InodeInfo
		err       error
		taskCh    = make(chan RebuildTask, 1024)
		wg        = sync.WaitGroup{}
	)
	if value := trash.subDirCache.Get(trashCurrent); value == nil {
		ino, _ := trash.mw.LookupPath(trashCurrent)
		trashInfo, err = trash.mw.InodeGet_ll(ino)
		if err != nil {
			log.LogWarnf("action[buildDeletedFileParentDirs] get %v inode info failed:%v", trashCurrent, err.Error())
			return
		} else {
			trash.subDirCache.Put(trashCurrent, trashInfo)
			log.LogDebugf("action[buildDeletedFileParentDirs] store %v info %v", trashCurrent, trashInfo)
		}
	} else {
		trashInfo = value
	}
	if trashInfo == nil {
		log.LogWarnf("action[buildDeletedFileParentDirs] trashInfo is nil %v", trashCurrent)
		return
	}
	var (
		noMore = false
		from   = ""
		//children []proto.Dentry
	)
	//children, err := trash.mw.ReadDir_ll(trashInfo.Inode)
	//if err != nil {
	//	log.LogWarnf("action[buildDeletedFileParentDirs] ReadDir  %v  failed:%v", trashCurrent, err.Error())
	//	return
	//}
	rebuildTaskFunc := func() {
		defer wg.Done()
		for task := range taskCh {
			if proto.IsDir(task.Type) {
				trash.rebuildDir(task.Name, trashCurrent, task.Inode, task.FileIno, false)
			} else {
				trash.rebuildFile(task.Name, trashCurrent, task.FileIno, false)
			}
		}
	}

	for i := 0; i < trash.rebuildGoroutineLimit; i++ {
		wg.Add(1)
		go rebuildTaskFunc()
	}
	for !noMore {
		batches, err := trash.mw.ReadDirLimit_ll(trashInfo.Inode, from, DefaultReaddirLimit)
		if err != nil {
			log.LogErrorf("action[buildDeletedFileParentDirs] ReadDirLimit_ll: ino(%v) err(%v) from(%v)", trashInfo.Inode, err, from)
			return
		}
		batchNr := uint64(len(batches))
		if batchNr == 0 || (from != "" && batchNr == 1) {
			noMore = true
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
				taskCh <- RebuildTask{Name: child.Name, Type: child.Type, Inode: trashInfo.Inode, FileIno: child.Inode}
			}
		}
		from = batches[len(batches)-1].Name
	}

	close(taskCh)
	wg.Wait()
	log.LogDebugf("action[buildDeletedFileParentDirs] end")
}

func (trash *Trash) buildDeletedFileParentDirsForExpired() {
	defer log.LogDebugf("action[buildDeletedFileParentDirsForExpired]exit")
	log.LogDebugf("action[buildDeletedFileParentDirsForExpired]enter")
	//read trash root
	entries, err := trash.mw.ReadDir_ll(trash.trashRootIno)
	if err != nil {
		log.LogWarnf("action[buildDeletedFileParentDirsForExpired]ReadDir trashRoot  failed: %v", err.Error())
		return
	}

	for _, entry := range entries {
		if !proto.IsDir(entry.Type) {
			continue
		}
		//only rebuild expired dir
		if !strings.HasPrefix(entry.Name, ExpiredPrefix) {
			continue
		}
		children, err := trash.mw.ReadDir_ll(entry.Inode)
		if err != nil {
			log.LogWarnf("action[buildDeletedFileParentDirsForExpired] ReadDir  %v  failed:%v", entry.Name, err.Error())
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
					trash.rebuildDir(task.Name, path.Join(trash.trashRoot, entry.Name), task.Inode, task.FileIno, true)
				} else {
					trash.rebuildFile(task.Name, path.Join(trash.trashRoot, entry.Name), task.FileIno, true)
				}
			}
		}
		for i := 0; i < trash.rebuildGoroutineLimit; i++ {
			wg.Add(1)
			go rebuildTaskFunc()
		}
		for _, child := range children {
			log.LogDebugf("action[buildDeletedFileParentDirsForExpired] rebuild %v type %v", child.Name, child.Type)
			if strings.Contains(child.Name, ParentDirPrefix) || strings.Contains(child.Name, LongNamePrefix) {
				taskCh <- RebuildTask{Name: child.Name, Type: child.Type, Inode: entry.Inode, FileIno: child.Inode}
			}
		}
		close(taskCh)
		wg.Wait()
		log.LogDebugf("action[buildDeletedFileParentDirs] %v end", entry.Name)
	}
}
func (trash *Trash) rebuildFile(fileName, trashCurrent string, fileIno uint64, forExpired bool) {
	log.LogDebugf("action[rebuildFile]: rebuild file %v in %v", fileName, trashCurrent)
	originName := fileName
	fileName = trash.recoverPosixPathName(fileName, fileIno)
	log.LogDebugf("action[rebuildFile]: recover  %v to  %v", originName, fileName)
	parentDir := path.Dir(fileName)
	baseName := path.Base(fileName)

	if parentDir == "." { //file in trash root
		if trash.pathIsExist(path.Join(trashCurrent, baseName)) {
			baseName = fmt.Sprintf("%s_%v", baseName, time.Now().Unix())
		}
		if err := trash.rename(path.Join(trashCurrent, originName), path.Join(trashCurrent, baseName)); err != nil {
			log.LogWarnf("action[rebuildFile]: recover  %v to  %v failed:err %v",
				path.Join(trashCurrent, originName), path.Join(trashCurrent, baseName), err)
		}
	} else {
		//
		if forExpired {
			if !trash.pathIsExist(path.Join(trashCurrent, parentDir)) {
				//create in expired
				trash.createParentPathInTrash(parentDir, trashCurrent)
				log.LogDebugf("action[rebuildFile]: build parentDir  %v in  %v", parentDir, trashCurrent)
			}
		} else {
			if !trash.pathIsExistInTrash(parentDir) {
				trash.createParentPathInTrash(parentDir, CurrentName)
			}
		}
		if trash.pathIsExist(path.Join(trashCurrent, fileName)) {
			baseName = fmt.Sprintf("%s_%v", baseName, time.Now().Unix())
		}
		if err := trash.rename(path.Join(trashCurrent, originName), path.Join(trashCurrent, parentDir, baseName)); err != nil {
			log.LogWarnf("action[rebuildFile]: recover  %v to  %v failed:err %v",
				path.Join(trashCurrent, originName), path.Join(trashCurrent, parentDir, baseName), err.Error())
		}
	}
}

func (trash *Trash) rebuildDir(dirName, trashCurrent string, ino uint64, fileIno uint64, forExpired bool) {
	log.LogDebugf("action[rebuildDir]: rebuild dir %v in %v", dirName, trashCurrent)
	originName := dirName
	dirName = trash.recoverPosixPathName(dirName, fileIno)
	if forExpired {
		trash.createParentPathInTrash(dirName, trashCurrent)
	} else {
		trash.createParentPathInTrash(dirName, CurrentName)
	}
	log.LogDebugf("action[rebuildDir]: delete dir %v in %v[%v]", dirName, trashCurrent, ino)
	_, err := trash.mw.Delete_ll(ino, path.Base(originName), true, path.Join(trashCurrent, dirName))
	if err != nil {
		log.LogDebugf("action[rebuildDir]: delete dir %v in %v[%v] failed:err %v", dirName, trashCurrent, ino, err)
	}
}

func (trash *Trash) recoverPosixPathName(fileName string, fileIno uint64) string {
	if strings.HasPrefix(fileName, LongNamePrefix) {
		log.LogDebugf("action[recoverPosixPathName] %v is long ino %v", fileName, fileIno)
		info, err := trash.mw.InodeGet_ll(fileIno)
		if err != nil {
			log.LogWarnf("action[recoverPosixPathName]:InodeGet_ll for %v[%v] failed:%v",
				fileName, fileIno, err.Error())
			fileName = strings.ReplaceAll(fileName, LongNamePrefix, "/")
			//remove uuid
			return strings.Split(fileName, ParentDirPrefix)[0]
		} else {
			log.LogDebugf("action[recoverPosixPathName]:XAttrGet_ll for %v", fileName)
			attrInfo, err := trash.mw.XAttrGet_ll(info.Inode, OriginalName)
			if err != nil {
				log.LogWarnf("action[recoverPosixPathName]:XAttrGet_ll for %v[%v] failed:%v",
					fileName, fileIno, err.Error())
				log.LogDebugf("action[recoverPosixPathName]:XAttrGet_ll for %v[%v] failed:%v",
					fileName, fileIno, err.Error())
				//remove uuid
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
	if strings.HasPrefix(fileName, ParentDirPrefix) {
		fileName = strings.TrimPrefix(fileName, ParentDirPrefix)
	}
	fileName = strings.ReplaceAll(fileName, ParentDirPrefix, "/")
	return fileName
}

func (trash *Trash) deleteSrcDirDirectly(parentIno uint64, fileName, fullPath string) error {
	srcParentMP := trash.mw.getPartitionByInode(parentIno)
	if srcParentMP == nil {
		return syscall.ENOENT
	}
	status, _, err := trash.mw.ddelete(srcParentMP, parentIno, fileName, 0, fullPath)
	if err != nil {
		log.LogErrorf("deleteSrcDirDirectly delete %v failed.err %v", fullPath, err)
		return statusToErrno(status)
	}
	return nil
}
