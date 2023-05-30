package data

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

const (
	CubeInfoDir		 = ".cube_torch"
	CubeInfoFileName = ".cube_info"

	DefaultIndexDentryExpiration = 60 * time.Minute
)

type CubeInfo struct {
	Prof uint64 `json:"prof"`
}

type PrefetchManager struct {
	volName			 string
	mountPoint       string
	ec               *ExtentClient
	indexFileInfoMap sync.Map // key: index file path, value: *IndexInfo
	indexInfoChan    chan *IndexInfo
	filePathChan     chan string
	appPid           sync.Map
	dcacheMap        sync.Map // key: parent Inode ID, value: *IndexDentryInfo

	wg     sync.WaitGroup
	closeC chan struct{}
}

type IndexInfo struct {
	path        string
	datasetCnt	string
	ttl         int64
	validMinute int64
	fileInfoMap []*FileInfo
}

func (info *IndexInfo) String() string {
	if info == nil {
		return ""
	}
	return fmt.Sprintf("Path(%v) DatasetCnt(%v) TTL(%v) InfoCnt(%v)", info.path, info.datasetCnt, info.ttl, len(info.fileInfoMap))
}

type IndexDentryInfo struct {
	path   string
	dcache *cache.DentryCache
}

func (info *IndexDentryInfo) String() string {
	if info == nil {
		return ""
	}
	return fmt.Sprintf("Path(%v) Expiration(%v)", info.path, info.dcache.Expiration())
}

type FileInfo struct {
	path  string
	inoID uint64
}

type DentryInfo struct {
	parIno uint64
	dcache *cache.DentryCache
}

func NewPrefetchManager(ec *ExtentClient, volName, mountPoint string, prefetchThreads int64) *PrefetchManager {
	pManager := &PrefetchManager{
		volName: 		volName,
		mountPoint:     mountPoint,
		ec:             ec,
		closeC:         make(chan struct{}, 1),
		indexInfoChan:  make(chan *IndexInfo, 1024),
		filePathChan:   make(chan string, 10240000),
	}
	for i := int64(0); i < prefetchThreads; i++ {
		pManager.wg.Add(1)
		go func(id int64) {
			pManager.backGroundPrefetchWorker(id)
		}(i)
	}
	pManager.wg.Add(1)
	go pManager.cleanExpiredIndexInfo()
	return pManager
}

func (pManager *PrefetchManager) Close() {
	close(pManager.closeC)
	pManager.wg.Wait()
}

func (pManager *PrefetchManager) GenerateCubeInfo(localIP string, port uint64) (err error) {
	var (
		cubeInfoBytes []byte
		fd            *os.File
	)
	cubeInfo := &CubeInfo{Prof: port}
	if cubeInfoBytes, err = json.Marshal(cubeInfo); err != nil {
		log.LogErrorf("GenerateCubeInfo: info(%v) json marshal err(%v)", cubeInfo, err)
		return
	}
	cubeInfoDir := path.Join(pManager.mountPoint, CubeInfoDir, localIP)
	if err = os.MkdirAll(cubeInfoDir, 0777); err != nil {
		log.LogErrorf("GenerateCubeInfo: mkdir(%v) err(%v)", cubeInfoDir, err)
		return
	}
	cubeInfoPath := path.Join(cubeInfoDir, CubeInfoFileName)
	if fd, err = os.OpenFile(cubeInfoPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666); err != nil {
		log.LogErrorf("GenerateCubeInfo: open file(%v) err(%v)", cubeInfoPath, err)
		return
	}
	defer fd.Close()

	if _, err = fd.WriteAt(cubeInfoBytes, 0); err != nil {
		log.LogErrorf("GenerateCubeInfo: write file(%v) err(%v) info(%v)", cubeInfoPath, err, cubeInfo)
		return
	}
	return fd.Sync()
}

func (pManager *PrefetchManager) backGroundPrefetchWorker(id int64) {
	defer func() {
		if r := recover(); r != nil {
			stack := fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			log.LogErrorf("backGroundPrefetchWorker panic: %s\n", stack)
		}
	}()

	defer pManager.wg.Done()

	log.LogInfof("backGroundPrefetchWorker: start worker(%v)", id)

	readData := make([]byte, 128*1024)
	for {
		select {
		case <-pManager.closeC:
			log.LogInfof("backGroundPrefetchWorker: close worker(%v)", id)
			return

		case filePath := <-pManager.filePathChan:
			start := time.Now()
			tpObject := exporter.NewVolumeTP("Prefetch", pManager.volName)
			inoID, err := pManager.OsReadFile(path.Join(pManager.mountPoint, filePath), readData)
			if err != nil {
				log.LogWarnf("backGroundPrefetchWorker: ino(%v) path(%v) prefetch err(%v)", inoID, filePath, err)
			}
			tpObject.Set(err)
			if log.IsDebugEnabled() {
				log.LogDebugf("backGroundPrefetchWorker: id(%v) prefetch ino(%v) path(%v) cost(%v) pathChan(%v)", id, inoID, filePath, time.Since(start), len(pManager.filePathChan))
			}

		case indexInfo := <-pManager.indexInfoChan:
			start := time.Now()
			if err := pManager.LoadIndex(indexInfo); err != nil {
				log.LogWarnf("backGroundPrefetchWorker: index(%v) read err(%v)", indexInfo, err)
			}
			if log.IsDebugEnabled() {
				log.LogDebugf("backGroundPrefetchWorker: id(%v) read index(%v) cost(%v)", id, indexInfo, time.Since(start))
			}

		}
	}
}

func (pManager *PrefetchManager) cleanExpiredIndexInfo() {
	defer func() {
		if r := recover(); r != nil {
			stack := fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			log.LogErrorf("cleanExpiredIndexInfo panic: %s\n", stack)
		}
	}()

	defer pManager.wg.Done()

	log.LogInfof("cleanExpiredIndexInfo: start")

	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	t1 := time.NewTicker(time.Second)
	defer t1.Stop()
	for {
		select {
		case <-pManager.closeC:
			log.LogInfof("cleanExpiredIndexInfo: stop")
			return
		case <-t1.C:
			log.LogInfof("PrefetchManager: pathChan(%v)", len(pManager.filePathChan))
		case <-t.C:
			pManager.indexFileInfoMap.Range(func(key, value interface{}) bool {
				indexInfo := value.(*IndexInfo)
				if indexInfo.ttl > 0 && time.Now().Unix() > indexInfo.ttl {
					pManager.indexFileInfoMap.Delete(key)
					log.LogInfof("cleanExpiredIndexInfo: index(%v)", indexInfo)
				}
				if log.IsDebugEnabled() {
					log.LogDebugf("cleanExpiredIndexInfo: check index(%v)", indexInfo)
				}
				return true
			})
			pManager.dcacheMap.Range(func(key, value interface{}) bool {
				dCache := value.(*IndexDentryInfo).dcache
				if dCache.IsExpired() {
					pManager.dcacheMap.Delete(key)
					log.LogInfof("cleanExpiredIndexInfo: dcache parent id(%v)", key)
				}
				return true
			})
		}
	}
}

func (pManager *PrefetchManager) OsReadFile(filePath string, readData []byte) (ino uint64, err error) {
	var fh *os.File
	if fh, err = os.Open(filePath); err != nil {
		return
	}
	defer fh.Close()

	//if fInfo, statErr := fh.Stat(); statErr == nil {
	//	ino = fInfo.Sys().(*syscall.Stat_t).Ino
	//}

	for {
		readSize, readErr := fh.Read(readData)
		if readSize == 0 || readErr != nil {
			if readErr != io.EOF {
				err = readErr
			}
			return
		}
	}
}

func (pManager *PrefetchManager) AddIndexFilepath(datasetCnt, path string, ttlMinute int64) error {
	indexInfo := &IndexInfo{path: path, datasetCnt: datasetCnt}
	if ttlMinute > 0 {
		indexInfo.ttl = time.Now().Add(time.Duration(ttlMinute) * time.Minute).Unix()
		indexInfo.validMinute = ttlMinute
	}
	actual, loaded := pManager.indexFileInfoMap.LoadOrStore(path, indexInfo)
	oldIndexInfo := actual.(*IndexInfo)
	if loaded && pManager.resetExistedIndexInfo(oldIndexInfo, indexInfo, ttlMinute) {
		return nil
	}

	select {
	case <-pManager.closeC:
		return fmt.Errorf("Prefetch threads are closed")
	case pManager.indexInfoChan <- indexInfo:
	}
	return nil
}

func (pManager *PrefetchManager) LoadIndex(indexInfo *IndexInfo) (err error) {
	start1 := time.Now()
	if err = pManager.readIndexFileByLine(indexInfo); err != nil {
		pManager.indexFileInfoMap.Delete(indexInfo.path)
		log.LogWarnf("LoadIndex: err(%v) path(%v)", err, indexInfo)
		return
	}
	actual, loaded := pManager.indexFileInfoMap.LoadOrStore(indexInfo.path, indexInfo)
	if loaded {
		actual.(*IndexInfo).fileInfoMap = indexInfo.fileInfoMap
	}
	log.LogInfof("LoadIndex: store index path info(%v) loaded(%v) cost(%v)", actual, loaded, time.Since(start1))

	start2 := time.Now()
	// pathDentryInfoMap store all dcache to find needed dentry. key: dirPath in CFS, value: dir inodeID and dcache
	pathDentryInfoMap := make(map[string]*DentryInfo)
	ctx := context.Background()
	for index, fInfo := range actual.(*IndexInfo).fileInfoMap {
		if fInfo.path == "" {
			continue
		}
		if fInfo.inoID, err = pManager.getFileInode(ctx, pathDentryInfoMap, fInfo.path, actual.(*IndexInfo)); err != nil {
			log.LogWarnf("LoadIndex: index(%v) line(%v) path(%v) get inode err(%v)", indexInfo, index, fInfo.path, err)
		}
	}
	log.LogInfof("LoadIndex: store index inode info(%v) cost(%v)", actual, time.Since(start2))

	return nil
}

func (pManager *PrefetchManager) readIndexFileByLine(indexInfo *IndexInfo) error {
	f, err := os.OpenFile(indexInfo.path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	infoNum := int64(0)
	if datasetCnt, lineErr := strconv.ParseInt(indexInfo.datasetCnt, 10, 64); lineErr == nil {
		infoNum = datasetCnt
	}
	indexInfo.fileInfoMap = make([]*FileInfo, 0, infoNum)

	scanner := bufio.NewScanner(f)
	setBuf := make([]byte, 128*1024)
	scanner.Buffer(setBuf, len(setBuf))
	index := uint64(0)
	for scanner.Scan() {
		line := scanner.Text()
		cfsPath := strings.TrimSpace(strings.Replace(line, pManager.mountPoint, "", 1))
		if log.IsDebugEnabled() {
			log.LogDebugf("readIndexFileByLine: filepath(%v) index(%v) cfs path(%v)", indexInfo.path, index, cfsPath)
		}
		indexInfo.fileInfoMap = append(indexInfo.fileInfoMap, &FileInfo{path: cfsPath})
		index++
	}
	if err := scanner.Err(); err != nil {
		log.LogWarnf("readIndexFileByLine: scan err(%v) indexInfo(%v)", err, indexInfo)
		return err
	}

	return nil
}

func (pManager *PrefetchManager) getFileInode(ctx context.Context, pathDentryInfoMap map[string]*DentryInfo, cfsPath string, indexInfo *IndexInfo) (inoID uint64, err error) {
	dirPath, fileName := path.Split(cfsPath)
	dInfo, exist := pathDentryInfoMap[dirPath]
	if !exist {
		parIno, allDcache, tmpErr := pManager.getDirInfo(ctx, dirPath)
		if tmpErr != nil {
			err = tmpErr
			return
		}
		dInfo = &DentryInfo{parIno: parIno, dcache: allDcache}
		pathDentryInfoMap[dirPath] = dInfo
	}
	if ino, exist := dInfo.dcache.Get(fileName); exist {
		value, ok := pManager.dcacheMap.Load(dInfo.parIno)
		if !ok {
			valid := DefaultIndexDentryExpiration
			if indexInfo.validMinute > 0 {
				valid = time.Duration(indexInfo.validMinute) * time.Minute
			}
			indexDentryInfo := &IndexDentryInfo{
				path:   indexInfo.path,
				dcache: cache.NewDentryCache(valid, true),
			}
			value, ok = pManager.dcacheMap.LoadOrStore(dInfo.parIno, indexDentryInfo)
			if log.IsDebugEnabled() {
				log.LogDebugf("getFileInode: store parIno(%v) dcache info(%v) loaded(%v)", dInfo.parIno, value, ok)
			}
		}
		dcache := value.(*IndexDentryInfo).dcache
		dcache.Put(fileName, ino)
		inoID = ino
	} else {
		err = fmt.Errorf("get inode id failed")
	}
	return
}

func (pManager *PrefetchManager) getDirInfo(ctx context.Context, path string) (parIno uint64, dcache *cache.DentryCache, err error) {
	start := time.Now()
	if log.IsDebugEnabled() {
		log.LogDebugf("getDirInfo enter: cfs path(%v)", path)
	}
	parIno, err = pManager.ec.metaWrapper.LookupPath(ctx, path)
	if err != nil {
		return
	}
	var children []proto.Dentry
	if children, err = pManager.ec.metaWrapper.ReadDir_ll(ctx, parIno); err != nil {
		return
	}
	dcache = cache.NewDentryCache(30*time.Minute, true)
	for _, child := range children {
		dcache.Put(copyString(child.Name), child.Inode)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("getDirInfo: parIno(%v) cfs path(%v) children(%v) cost(%v)", parIno, path, dcache.Count(), time.Since(start))
	}
	return
}

func (pManager *PrefetchManager) resetExistedIndexInfo(oldIndexInfo, newIndexInfo *IndexInfo, ttlMinute int64) bool {
	// reset expiration for existed index info
	dcacheValid := DefaultIndexDentryExpiration
	if oldIndexInfo.ttl > 0 && time.Now().Unix() > oldIndexInfo.ttl {
		pManager.indexFileInfoMap.Store(newIndexInfo.path, newIndexInfo)
		log.LogInfof("resetExistedIndexInfo: store new index(%v)", newIndexInfo)
		return false
	}
	if ttlMinute == 0 {
		oldIndexInfo.ttl = 0
		oldIndexInfo.validMinute = 0
	} else {
		oldIndexInfo.ttl = time.Now().Add(time.Duration(ttlMinute) * time.Minute).Unix()
		oldIndexInfo.validMinute = ttlMinute
		dcacheValid = time.Duration(ttlMinute) * time.Minute
	}
	log.LogInfof("resetExistedIndexInfo: reset index(%v) file count(%v)", oldIndexInfo, len(oldIndexInfo.fileInfoMap))

	pManager.dcacheMap.Range(func(key, value interface{}) bool {
		indexDentryInfo := value.(*IndexDentryInfo)
		if indexDentryInfo.path == oldIndexInfo.path {
			indexDentryInfo.dcache.ResetExpiration(dcacheValid)
			log.LogInfof("resetExistedIndexInfo: reset dcache(%v) valid(%v)", key, dcacheValid)
		}
		return true
	})
	return true
}

func (pManager *PrefetchManager) GetDentryCache(parIno uint64) *cache.DentryCache {
	if pManager == nil {
		return nil
	}
	value, ok := pManager.dcacheMap.Load(parIno)
	if !ok {
		return nil
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("GetDentryCache: parIno(%v) value(%v)", parIno, value)
	}
	dcache := value.(*IndexDentryInfo).dcache
	if !dcache.IsExpired() {
		return dcache
	}
	pManager.dcacheMap.Delete(parIno)
	return nil
}

func (pManager *PrefetchManager) PrefetchIndex(datasetCnt string, index uint64) (err error) {
	pManager.indexFileInfoMap.Range(func(key, value interface{}) bool {
		if value.(*IndexInfo).datasetCnt != datasetCnt {
			return true
		}

		fileInfoMap := value.(*IndexInfo).fileInfoMap
		if index >= uint64(len(fileInfoMap)) {
			return true
		}
		fileInfo := fileInfoMap[index]
		if fileInfo.path == "" {
			log.LogWarnf("PrefetchIndex: filepath(%v) has no line(%v)", key, index)
			return true
		}
		select {
		case <-pManager.closeC:
			err = fmt.Errorf("Prefetch threads are closed")
			return false
		case pManager.filePathChan <- fileInfo.path:
			return true
		}
	})
	return
}

func (pManager *PrefetchManager) PrefetchInodeInfo(datasetCnt string, batchArr [][]uint64) {
	if pManager.ec.putIcache == nil {
		return
	}
	var inodes []uint64
	for _, indexArr := range batchArr {
		for _, index := range indexArr {
			pManager.indexFileInfoMap.Range(func(key, value interface{}) bool {
				if value.(*IndexInfo).datasetCnt != datasetCnt {
					return true
				}

				fileInfoMap := value.(*IndexInfo).fileInfoMap
				if index >= uint64(len(fileInfoMap)) {
					return true
				}
				fileInfo := fileInfoMap[uint32(index)]
				if fileInfo.inoID != 0 {
					inodes = append(inodes, fileInfo.inoID)
				} else {
					log.LogWarnf("PrefetchInodeInfo: filepath(%v) has no line(%v)", key, index)
				}
				return true
			})
		}
	}
	if len(inodes) == 0 {
		return
	}
	infos := pManager.ec.metaWrapper.BatchInodeGet(context.Background(), inodes)
	for _, info := range infos {
		pManager.ec.putIcache(*info)
	}
}

func (pManager *PrefetchManager) PutAppPid(pid uint32) {
	if pManager == nil {
		return
	}
	if _, ok := pManager.appPid.Load(pid); !ok {
		pManager.appPid.Store(pid, true)
		if log.IsDebugEnabled() {
			log.LogDebugf("PutAppPid: (%v)", pid)
		}
	}
}

func (pManager *PrefetchManager) ContainsAppPid(pid uint32) bool {
	if pManager == nil {
		return false
	}
	_, ok := pManager.appPid.Load(pid)
	return ok
}

func copyString(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}
