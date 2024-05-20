package meta

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
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
const ParentDirPrefix = "____"
const ExpiredTimeFormat = "2006-01-02-150405"
const FileNameLengthMax = 255
const LongNamePrefix = "LongName____"
const OriginalName = "OriginalName"

const (
	DisableTrash = "/trash/disable"
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
		traverseDirGoroutineLimit: make(chan bool, traverseLimit),
		rebuildGoroutineLimit:     rebuildGoroutineLimit,
		subDirCache:               NewDirInodeCache(DefaultDirInodeExpiration, DefaultMaxDirInode),
	}
	atomic.StoreInt32(&trash.rebuildStatus, rebuildStop)
	//create trash root
	if err := trash.InitTrashRoot(); err != nil {
		return nil, err
	}
	//发布前修改
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
		parentDirInfo.Mode, parentDirInfo.Uid, parentDirInfo.Gid)
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

func (trash *Trash) createCurrent() (err error) {
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
		trash.trashRootMode, trash.trashRootUid, trash.trashRootGid)
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
func (trash *Trash) MoveToTrash(parentPathAbsolute string, parentIno uint64, fileName string, isDir bool) (err error) {
	start := time.Now()
	defer func() {
		log.LogDebugf("action[MoveToTrash] : parentPathAbsolute(%v) fileName(%v) consume %v", parentPathAbsolute, fileName, time.Since(start).Seconds())
	}()
	log.LogDebugf("action[MoveToTrash] : parentPathAbsolute(%v) fileName(%v) parentIno（%v）", parentPathAbsolute, fileName, parentIno)
	if err = trash.createCurrent(); err != nil {
		return err
	}

	srcPath := path.Join(trash.mountPath, parentPathAbsolute, fileName)
	//generate tmp file name
	tmpFileName := fmt.Sprintf("%v%v", trash.generateTmpFileName(parentPathAbsolute), fileName)
	dstPath := path.Join(trash.trashRoot, CurrentName, tmpFileName)
	startCheck := time.Now()
	for {
		if trash.pathIsExistInTrash(dstPath) {
			if !isDir {
				//ignore dir rename
				dstPath = fmt.Sprintf("%s_%v", dstPath, time.Now().Unix())
				log.LogDebugf("action[MoveToTrash]filePathInTrash rename to %v", dstPath)
			} else {
				log.LogWarnf("action[MoveToTrash]ignore dir already created %v", dstPath)
				return
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
	err = trash.renameToTrashTempFile(parentIno, srcPath, dstPath)
	log.LogDebugf("action[MoveToTrash]  rename: srcPath(%v) dstPath(%v) consume %v", srcPath, dstPath, time.Since(startRename).Seconds())
	if err != nil {
		log.LogDebugf("action[MoveToTrash] rename %v to %v failed:%v", srcPath, dstPath, err.Error())
		return err
	}
	if needStoreXattr {
		go func(name, dstPath string) {
			var (
				info *proto.InodeInfo
				err  error
			)
			info, err = trash.LookupPath(dstPath, false)
			if err != nil {
				log.LogWarnf("action[MoveToTrash] LookupPath %v failed:%v", dstPath, err.Error())
				return
			}
			err = trash.mw.XAttrSet_ll(info.Inode, []byte(OriginalName), []byte(name))
			if err != nil {
				log.LogWarnf("action[MoveToTrash] set xattr for %v failed:%v", dstPath, err.Error())
				return
			}
		}(originName, dstPath)

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
	return path.Join(parentPath, LongNamePrefix+newBaseName), oldName
}
func (trash *Trash) getDeleteInterval() int64 {
	checkPointInterval := atomic.LoadInt64(&trash.deleteInterval) / 4
	if checkPointInterval == 0 {
		checkPointInterval = 1
	}
	return checkPointInterval
}

func (trash *Trash) deleteWorker() {
	log.LogDebugf("action[deleteWorker] enter")
	checkPointInterval := trash.getDeleteInterval()
	t := time.NewTicker(time.Duration(checkPointInterval) * time.Minute)
	///t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-trash.done:
			log.LogWarnf("traverse stopped!")
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
	//if current has
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
			trash.deleteTask(trash.trashRootIno, entry.Name, proto.IsDir(entry.Type))
		}
	}
}

func (trash *Trash) removeAll(dirName string, dirIno uint64) {
	var wg sync.WaitGroup
	log.LogDebugf("action[deleteDir]start delete %v", dirName)
	entries, err := trash.mw.ReadDir_ll(dirIno)
	if err != nil {
		log.LogWarnf("action[deleteDir]delete %v failed: %v", dirName, err)
		return
	}
	//delete sub files
	for _, entry := range entries {
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
	//all sub files is deleted
	for _, entry := range entries {
		select {
		case trash.traverseDirGoroutineLimit <- true:
			wg.Add(1)
			go func(parentIno uint64, entry string, isDir bool) {
				defer wg.Done()
				trash.deleteTask(parentIno, entry, isDir)
			}(dirIno, entry.Name, proto.IsDir(entry.Type))
		default:
			trash.deleteTask(dirIno, entry.Name, proto.IsDir(entry.Type))
		}
	}
	wg.Wait()
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

func (trash *Trash) CreateDirectory(pino uint64, name string, mode, uid, gid uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeDir)
	return trash.mw.Create_ll(pino, name, fuseMode, uid, gid, nil)
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

func (trash *Trash) createParentPathInTrash(parentPath string) (err error) {
	//check .Trash/Current first
	//log.LogDebugf(string(debug.Stack()))
	trashCurrent := path.Join(trash.trashRoot, CurrentName)
	if err = trash.createCurrent(); err != nil {
		return
	}
	log.LogDebugf("action[createParentPathInTrash] ready  to create %v in trash ", parentPath)
	subDirs := strings.Split(parentPath, "/")
	cur := trashCurrent
	trashCurrentIno := trash.subDirCache.Get(trashCurrent)
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
			log.LogDebugf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
			return
		}
		if info == nil {
			panic(fmt.Sprintf("info should not be nil for parentPath %v", parentPath))
			return
		}
		parentInfo, err = trash.CreateDirectory(parentIno, sub, info.Mode, info.Uid, info.Gid)
		if err != nil {
			log.LogWarnf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
			log.LogDebugf("action[createParentPathInTrash] CreateDirectory  %v in trash failed: %v", cur, err.Error())
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

func (trash *Trash) renameToTrashTempFile(parentIno uint64, oldPath, newPath string) error {
	newParent := path.Dir(newPath)
	start := time.Now()
	newInfo, err := trash.LookupPath(newParent, true)
	log.LogDebugf("MoveToTrash: rename  LookupPath  newParent %v consume %v", newParent, time.Since(start).Seconds())
	if err != nil {
		log.LogDebugf("action[rename] lookup  %v failed %v", newParent, err.Error())
		return err
	}

	return trash.mw.Rename_ll(parentIno, path.Base(oldPath), newInfo.Inode, path.Base(newPath), true)
}

func (trash *Trash) rename(oldPath, newPath string) error {
	oldParent := path.Dir(oldPath)
	newParent := path.Dir(newPath)
	start := time.Now()
	oldInfo, err := trash.LookupPath(oldParent, true)
	log.LogDebugf("MoveToTrash: rename  LookupPath  oldParent %v consume %v", oldParent, time.Since(start).Seconds())
	if err != nil {
		log.LogDebugf("action[rename] lookup  %v failed %v", oldParent, err.Error())
		return err

	}
	start = time.Now()
	newInfo, err := trash.LookupPath(newParent, true)
	log.LogDebugf("MoveToTrash: rename  LookupPath  newParent %v consume %v", newParent, time.Since(start).Seconds())
	if err != nil {
		log.LogWarnf("action[rename] lookup  %v failed %v", newParent, err.Error())
		return err
	}

	return trash.mw.Rename_ll(oldInfo.Inode, path.Base(oldPath), newInfo.Inode, path.Base(newPath), true)
}

func (trash *Trash) deleteSrcDir(dirPath string) error {
	parentDir := path.Dir(dirPath)
	parentInfo, err := trash.LookupPath(parentDir, true)
	if err != nil {
		log.LogDebugf("action[deleteSrcDir] lookup  %v failed %v", parentDir, err.Error())
		return err

	}
	_, err = trash.mw.Delete_ll(parentInfo.Inode, path.Base(dirPath), true)
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

func (trash *Trash) deleteTask(parentIno uint64, entry string, isDir bool) {
	info, err := trash.mw.Delete_ll(parentIno, entry, isDir)
	if err != nil {
		log.LogWarnf("Delete_ll %v failed:%v", entry, err.Error())
		return
	}
	if !isDir {
		trash.mw.Evict(info.Inode)
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
	//定时任务，仿照拷贝文件
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
	children, err := trash.mw.ReadDir_ll(trashInfo.Inode)
	if err != nil {
		log.LogWarnf("action[buildDeletedFileParentDirs] ReadDir  %v  failed:%v", trashCurrent, err.Error())
		return
	}
	rebuildTaskFunc := func() {
		defer wg.Done()
		for task := range taskCh {
			if proto.IsDir(task.Type) {
				trash.rebuildDir(task.Name, task.Inode, task.FileIno)
			} else {
				trash.rebuildFile(task.Name, trashCurrent, task.FileIno)
			}
		}
	}

	for i := 0; i < trash.rebuildGoroutineLimit; i++ {
		wg.Add(1)
		go rebuildTaskFunc()
	}
	for _, child := range children {
		log.LogDebugf("action[buildDeletedFileParentDirs] rebuild %v type %v", child.Name, child.Type)
		if strings.Contains(child.Name, ParentDirPrefix) || strings.Contains(child.Name, LongNamePrefix) {
			taskCh <- RebuildTask{Name: child.Name, Type: child.Type, Inode: trashInfo.Inode, FileIno: child.Inode}
		}
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
					trash.rebuildDir(task.Name, task.Inode, task.FileIno)
				} else {
					trash.rebuildFile(task.Name, path.Join(trash.trashRoot, entry.Name), task.FileIno)
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
func (trash *Trash) rebuildFile(fileName, trashCurrent string, fileIno uint64) {
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
		trash.rename(path.Join(trashCurrent, originName), path.Join(trashCurrent, baseName))
	} else {
		//
		if !trash.pathIsExistInTrash(parentDir) {
			trash.createParentPathInTrash(parentDir)
		}
		if trash.pathIsExist(path.Join(trashCurrent, fileName)) {
			baseName = fmt.Sprintf("%s_%v", baseName, time.Now().Unix())
		}
		trash.rename(path.Join(trashCurrent, originName), path.Join(trashCurrent, parentDir, baseName))
	}
}

func (trash *Trash) rebuildDir(dirName string, ino uint64, fileIno uint64) {
	log.LogDebugf("action[rebuildDir]: rebuild dir %v in %v", dirName, ino)
	originName := dirName
	dirName = trash.recoverPosixPathName(dirName, fileIno)
	trash.createParentPathInTrash(dirName)
	trash.mw.Delete_ll(ino, path.Base(originName), true)
}

func (trash *Trash) recoverPosixPathName(fileName string, fileIno uint64) string {
	if strings.HasPrefix(fileName, LongNamePrefix) {
		log.LogDebugf("action[recoverPosixPathName] %v is long ", fileName)
		info, err := trash.mw.InodeGet_ll(fileIno)
		if err != nil {
			log.LogWarnf("action[recoverPosixPathName]:InodeGet_ll for %v[%v] failed:%v",
				fileName, fileIno, err.Error())
			fileName = strings.ReplaceAll(fileName, LongNamePrefix, "/")
			return fileName
		} else {
			log.LogDebugf("action[recoverPosixPathName]:XAttrGet_ll for %v", fileName)
			attrInfo, err := trash.mw.XAttrGet_ll(info.Inode, OriginalName)
			if err != nil {
				log.LogWarnf("action[recoverPosixPathName]:XAttrGet_ll for %v[%v] failed:%v",
					fileName, fileIno, err.Error())
				fileName = strings.ReplaceAll(fileName, LongNamePrefix, "/")
				return fileName
			}
			fileName = attrInfo.XAttrs[OriginalName]
			log.LogDebugf("action[recoverPosixPathName] fileName %v is read from xattr ", fileName)
		}
	}
	if strings.HasPrefix(fileName, ParentDirPrefix) {
		fileName = strings.TrimPrefix(fileName, ParentDirPrefix)
	}
	fileName = strings.ReplaceAll(fileName, ParentDirPrefix, "/")
	return fileName
}
