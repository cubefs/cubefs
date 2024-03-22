package stream

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	CubeInfoDir      = ".cube_torch"
	CubeInfoFileName = ".cube_info"

	DefaultIndexDentryExpiration = 60 * time.Minute
)

const BatchDownloadV1 = 0

type CubeInfo struct {
	Prof       uint64 `json:"prof"`
	MountPoint string `json:"mount_point"`
	LocalIP    string `json:"local_ip"`
	VolName    string `json:"vol_name"`
}

type PrefetchManager struct {
	volName          string
	mountPoint       string
	rootIno			 uint64
	ec               *ExtentClient
	indexFileInfoMap sync.Map // key: index file path, value: *IndexInfo
	indexInfoChan    chan *IndexInfo
	filePathChan     chan *FileInfo
	downloadChan     chan *DownloadFileInfo
	lookupDcache     sync.Map // key: inode ID, value: dcache
	appPid           sync.Map
	dcacheMap        sync.Map // key: parent Inode ID, value: *IndexDentryInfo

	metrics *PrefetchMetrics

	wg        sync.WaitGroup
	closeC    chan struct{}
	isClosed  bool
	closeLock sync.RWMutex
}

type PrefetchMetrics struct {
	appReadCount   uint64
	totalReadCount uint64
}

type IndexInfo struct {
	path        string
	datasetCnt  string
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
	isAbs bool
}

type DentryInfo struct {
	parIno uint64
	dcache *cache.DentryCache
}

func NewPrefetchManager(ec *ExtentClient, volName, mountPoint string, rootIno uint64, prefetchThreads int64) *PrefetchManager {
	InitReadBlockPool()
	pManager := &PrefetchManager{
		volName:       volName,
		mountPoint:    mountPoint,
		rootIno: 	   rootIno,
		ec:            ec,
		closeC:        make(chan struct{}, 1),
		indexInfoChan: make(chan *IndexInfo, 1024),
		filePathChan:  make(chan *FileInfo, 10240000),
		downloadChan:  make(chan *DownloadFileInfo, 10240000),
		metrics:       &PrefetchMetrics{0, 0},
	}
	log.LogInfof("NewPrefetchManager: start prefetch threads(%v)", prefetchThreads)
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
	pManager.closeLock.Lock()
	pManager.isClosed = true
	pManager.closeLock.Unlock()

	close(pManager.closeC)
	pManager.wg.Wait()
	pManager.clearDownloadChan()
}

const (
	Cube_Torch_ConfigFile = "/tmp/cube_torch.config"
)

func (pManager *PrefetchManager) GenerateCubeInfo(port uint64) (err error) {
	var (
		cubeInfoBytes []byte
	)
	cubeInfo := &CubeInfo{Prof: port, MountPoint: pManager.mountPoint, VolName: pManager.volName}
	if cubeInfoBytes, err = json.Marshal(cubeInfo); err != nil {
		log.LogErrorf("GenerateCubeInfo: info(%v) json marshal err(%v)", cubeInfo, err)
		return
	}
	cubeTorchConfig := fmt.Sprintf("%v.%v", Cube_Torch_ConfigFile, pManager.volName)
	err = ioutil.WriteFile(cubeTorchConfig, cubeInfoBytes, 0666)
	if err != nil {
		log.LogErrorf("Generate Cube_Torch_ConfigFile(%v): info(%v) json marshal err(%v)", Cube_Torch_ConfigFile, cubeInfo, err)
	}
	return
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

		case fileInfo := <-pManager.filePathChan:
			start := time.Now()
			var absFilepath string
			if fileInfo.isAbs {
				absFilepath = fileInfo.path
			} else {
				absFilepath = path.Join(pManager.mountPoint, fileInfo.path)
			}
			err := pManager.OsReadFile(absFilepath, readData)
			if err != nil {
				log.LogWarnf("backGroundPrefetchWorker: ino(%v) path(%v) prefetch err(%v)", fileInfo.inoID, absFilepath, err)
			}
			if log.EnableDebug() {
				log.LogDebugf("backGroundPrefetchWorker: id(%v) prefetch ino(%v) path(%v) cost(%v) pathChan(%v)", id, fileInfo.inoID, absFilepath, time.Since(start), len(pManager.filePathChan))
			}

		case indexInfo := <-pManager.indexInfoChan:
			start := time.Now()
			if err := pManager.LoadIndex(indexInfo); err != nil {
				log.LogWarnf("backGroundPrefetchWorker: index(%v) read err(%v)", indexInfo, err)
			}
			if log.EnableDebug() {
				log.LogDebugf("backGroundPrefetchWorker: id(%v) read index(%v) cost(%v)", id, indexInfo, time.Since(start))
			}

		case dInfo := <-pManager.downloadChan:
			start := time.Now()
			err := pManager.ReadData(dInfo)
			if err != nil {
				log.LogWarnf("backGroundPrefetchWorker: file(%v) get data err(%v)", dInfo, err)
			}
			if log.EnableDebug() {
				log.LogDebugf("backGroundPrefetchWorker: id(%v) send file(%v) cost(%v)", id, dInfo, time.Since(start))
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
			totalRead := atomic.SwapUint64(&pManager.metrics.totalReadCount, 0)
			appRead := atomic.SwapUint64(&pManager.metrics.appReadCount, 0)
			hitPercent := computeHitPercent(totalRead, appRead)
			log.LogInfof("PrefetchManager: totalRead(%v) appRead(%v) hitCache(%.2f%%) pathChan(%v) batchDownloadChan(%v)",
				totalRead, appRead, hitPercent, len(pManager.filePathChan), len(pManager.downloadChan))
		case <-t.C:
			pManager.indexFileInfoMap.Range(func(key, value interface{}) bool {
				indexInfo := value.(*IndexInfo)
				if indexInfo.ttl > 0 && time.Now().Unix() > indexInfo.ttl {
					pManager.indexFileInfoMap.Delete(key)
					log.LogInfof("cleanExpiredIndexInfo: index(%v)", indexInfo)
				}
				if log.EnableDebug() {
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
			pManager.lookupDcache.Range(func(key, value interface{}) bool {
				dcache := value.(*cache.DentryCache)
				if dcache.IsExpired() {
					pManager.lookupDcache.Delete(key)
					log.LogInfof("cleanExpiredIndexInfo: lookupDcache parent id(%v)", key)
				}
				return true
			})
		}
	}
}

func computeHitPercent(totalRead, appRead uint64) float64 {
	hitPercent := float64(100)
	if appRead >= totalRead {
		return 0
	}
	if totalRead != 0 {
		hitPercent = float64(totalRead-appRead) / float64(totalRead) * 100
	}
	return hitPercent
}

func (pManager *PrefetchManager) OsReadFile(filePath string, readData []byte) (err error) {
	var fh *os.File
	if fh, err = os.Open(filePath); err != nil {
		return
	}
	defer fh.Close()

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

	if _, err := os.Stat(path); os.IsNotExist(err) {
		pManager.indexFileInfoMap.Delete(path)
		return err
	}
	return pManager.putIndexInfoChan(indexInfo)
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
	for index, fInfo := range actual.(*IndexInfo).fileInfoMap {
		if fInfo.path == "" {
			continue
		}
		if fInfo.inoID, err = pManager.getFileInode(pathDentryInfoMap, fInfo.path, actual.(*IndexInfo)); err != nil {
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
		if log.EnableDebug() {
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

func (pManager *PrefetchManager) getFileInode(pathDentryInfoMap map[string]*DentryInfo, cfsPath string, indexInfo *IndexInfo) (inoID uint64, err error) {
	dirPath, fileName := path.Split(cfsPath)
	dInfo, exist := pathDentryInfoMap[dirPath]
	if !exist {
		parIno, allDcache, tmpErr := pManager.getDirInfo(dirPath)
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
				dcache: cache.NewDentryCache(valid),
			}
			value, ok = pManager.dcacheMap.LoadOrStore(dInfo.parIno, indexDentryInfo)
			if log.EnableDebug() {
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

func (pManager *PrefetchManager) getDirInfo(path string) (parIno uint64, dcache *cache.DentryCache, err error) {
	start := time.Now()
	if log.EnableDebug() {
		log.LogDebugf("getDirInfo enter: cfs path(%v)", path)
	}
	parIno, err = pManager.ec.lookupPath(pManager.rootIno, path)
	if err != nil {
		return
	}
	var children []proto.Dentry
	if children, err = pManager.ec.readdir(parIno); err != nil {
		return
	}
	dcache = cache.NewDentryCache(30 * time.Minute)
	for _, child := range children {
		dcache.Put(copyString(child.Name), child.Inode)
	}
	if log.EnableDebug() {
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
	if log.EnableDebug() {
		log.LogDebugf("GetDentryCache: parIno(%v) value(%v)", parIno, value)
	}
	dcache := value.(*IndexDentryInfo).dcache
	if !dcache.IsExpired() {
		return dcache
	}
	pManager.dcacheMap.Delete(parIno)
	return nil
}

func (pManager *PrefetchManager) PrefetchByIndex(datasetCnt string, index uint64) (err error) {
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
			log.LogWarnf("PrefetchByIndex: filepath(%v) has no line(%v)", key, index)
			return true
		}
		if err = pManager.putFilePathChan(fileInfo); err != nil {
			return false
		}
		return true
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
	infos := pManager.ec.batchInodeGet(inodes)
	for _, info := range infos {
		pManager.ec.putIcache(*info)
	}
}

func (pManager *PrefetchManager) PrefetchByPath(filepath string) error {
	fileInfo := &FileInfo{
		path:  filepath,
		isAbs: true,
	}
	return pManager.putFilePathChan(fileInfo)
}

type DownloadFileInfo struct {
	absPath  string
	fileInfo *FileInfo
	resp     *BatchDownloadRespWriter
}

func (d *DownloadFileInfo) String() string {
	if d == nil {
		return ""
	}
	return fmt.Sprintf("path(%v) info(%v)", d.absPath, d.fileInfo)
}

type DownloadResult struct {
	RespData []byte
	DataLen  int
}

type BatchDownloadRespWriter struct {
	Wg      sync.WaitGroup
	ResChan chan *DownloadResult
}

func (pManager *PrefetchManager) GetBatchFileInfos(batchArr [][]uint64, datasetCnt string) (infos []*FileInfo, err error) {
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
				fileInfo := fileInfoMap[index]
				if fileInfo.path == "" {
					log.LogWarnf("PrefetchByIndex: filepath(%v) has no line(%v)", key, index)
					return true
				}
				infos = append(infos, fileInfo)
				return true
			})
		}
	}
	return
}

func (pManager *PrefetchManager) DownloadData(fileInfo *FileInfo, respData *BatchDownloadRespWriter) {
	dInfo := &DownloadFileInfo{
		absPath:  path.Join(pManager.mountPoint, fileInfo.path),
		fileInfo: fileInfo,
		resp:     respData,
	}
	if err := pManager.putDownloadChan(dInfo); err != nil {
		log.LogWarnf("DownloadData: err(%v)", err)
	}
	return
}

func (pManager *PrefetchManager) DownloadPath(filePath string, respData *BatchDownloadRespWriter) {
	dInfo := &DownloadFileInfo{
		absPath: filePath,
		resp:    respData,
	}
	if err := pManager.putDownloadChan(dInfo); err != nil {
		log.LogWarnf("DownloadPath: err(%v)", err)
	}
	return
}

func (pManager *PrefetchManager) ReadData(dInfo *DownloadFileInfo) (err error) {
	var (
		content  []byte
		readSize int
		inodeID  uint64
	)
	defer func() {
		if err == nil && readSize > 0 {
			dRes := &DownloadResult{}
			filePath := dInfo.absPath
			dRes.DataLen = 8 + len(filePath) + 8 + readSize
			dRes.RespData = GetBlockBuf(dRes.DataLen)
			binary.BigEndian.PutUint64(dRes.RespData[0:8], uint64(len(filePath)))
			copy(dRes.RespData[8:8+len(filePath)], filePath)
			binary.BigEndian.PutUint64(dRes.RespData[8+len(filePath):16+len(filePath)], uint64(readSize))
			copy(dRes.RespData[16+len(filePath):dRes.DataLen], content[:readSize])
			if log.EnableDebug() {
				log.LogDebugf("ReadData: ino(%v) resp data len(%v) pathLen(%v) readSize(%v)", inodeID, dRes.DataLen, len(filePath), readSize)
			}
			dInfo.resp.ResChan <- dRes
		}
		dInfo.resp.Wg.Done()
		if len(content) > 0 {
			PutBlockBuf(content)
		}
	}()

	if dInfo.fileInfo == nil || dInfo.fileInfo.inoID == 0 {
		if inodeID, err = pManager.LookupPathByCache(dInfo.absPath); err != nil {
			return
		}
	} else {
		inodeID = dInfo.fileInfo.inoID
	}
	if err = pManager.ec.OpenStream(inodeID); err != nil {
		return
	}
	defer func() {
		pManager.ec.CloseStream(inodeID)
	}()

	fileSize, _, valid := pManager.ec.FileSize(inodeID)
	if !valid || fileSize == 0 {
		return fmt.Errorf("file size is (%v)", fileSize)
	}

	content = GetBlockBuf(int(fileSize))
	if readSize, err = pManager.ec.Read(inodeID, content[:fileSize], 0, int(fileSize)); err != nil {
		err = fmt.Errorf("read err(%v) offset(%v) size(%v) readBytes(%v)", err, 0, fileSize, readSize)
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("prefetchData: ino(%v) read size(%v)", inodeID, readSize)
	}
	return
}

func (pManager *PrefetchManager) PutAppPid(pid uint32) {
	if pManager == nil {
		return
	}
	if _, ok := pManager.appPid.Load(pid); !ok {
		pManager.appPid.Store(pid, true)
		if log.EnableDebug() {
			log.LogDebugf("PutAppPid: (%v)", pid)
		}
	}
}

func (pManager *PrefetchManager) DeleteAppPid(pid uint32) {
	if pManager == nil {
		return
	}
	pManager.appPid.Delete(pid)
	if log.EnableDebug() {
		log.LogDebugf("DeleteAppPid: (%v)", pid)
	}
}

func (pManager *PrefetchManager) ContainsAppPid(pid uint32) bool {
	if pManager == nil {
		return false
	}
	_, ok := pManager.appPid.Load(pid)
	return ok
}

func (pManager *PrefetchManager) AddTotalReadCount() {
	if pManager == nil {
		return
	}
	atomic.AddUint64(&pManager.metrics.totalReadCount, 1)
}

func (pManager *PrefetchManager) AddAppReadCount() {
	if pManager == nil {
		return
	}
	atomic.AddUint64(&pManager.metrics.appReadCount, 1)
}

func (pManager *PrefetchManager) LookupPathByCache(absPath string) (uint64, error) {
	if !strings.HasPrefix(absPath, pManager.mountPoint) {
		return 0, fmt.Errorf("not cfs path")
	}
	ino := pManager.rootIno
	subDir := strings.Replace(absPath, pManager.mountPoint, "", 1)
	if subDir == "" || subDir == "/" {
		return 0, fmt.Errorf("not cfs file")
	}
	dirs := strings.Split(subDir, "/")
	for index, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		var dcache *cache.DentryCache
		value, ok := pManager.lookupDcache.Load(ino)
		if ok {
			dcache = value.(*cache.DentryCache)
			if child, exist := dcache.Get(dir); exist {
				ino = child
				continue
			}
		} else {
			value, _ = pManager.lookupDcache.LoadOrStore(ino, cache.NewDentryCache(DefaultIndexDentryExpiration))
			dcache = value.(*cache.DentryCache)
		}
		child, _, err := pManager.ec.lookup(ino, dir)
		if err != nil {
			return 0, err
		}
		if index != len(dirs)-1 {
			dcache.Put(dir, child)
		}
		ino = child
	}
	if log.EnableDebug() {
		log.LogDebugf("LookupPathByCache: get inode(%v) of path(%v)", ino, absPath)
	}
	return ino, nil
}

func (pManager *PrefetchManager) putIndexInfoChan(indexInfo *IndexInfo) error {
	pManager.closeLock.RLock()
	defer pManager.closeLock.RUnlock()

	if pManager.isClosed {
		return fmt.Errorf("Prefetch threads are closed")
	}
	pManager.indexInfoChan <- indexInfo
	return nil
}

func (pManager *PrefetchManager) putFilePathChan(fileInfo *FileInfo) error {
	pManager.closeLock.RLock()
	defer pManager.closeLock.RUnlock()

	if pManager.isClosed {
		return fmt.Errorf("Prefetch threads are closed")
	}
	pManager.filePathChan <- fileInfo
	return nil
}

func (pManager *PrefetchManager) putDownloadChan(dInfo *DownloadFileInfo) error {
	pManager.closeLock.RLock()
	defer pManager.closeLock.RUnlock()

	if pManager.isClosed {
		return fmt.Errorf("Prefetch threads are closed")
	}
	dInfo.resp.Wg.Add(1)
	pManager.downloadChan <- dInfo
	return nil
}

func (pManager *PrefetchManager) clearDownloadChan() {
	for {
		select {
		case dInfo := <-pManager.downloadChan:
			dInfo.resp.Wg.Done()
			log.LogInfof("clearDownloadChan: info(%v)", dInfo)
		default:
			return
		}
	}
}

func copyString(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}
