package data

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/client/cache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

const (
	CubeInfoDir		 = ".cube_torch"
	CubeInfoFileName = ".cube_info"

	DefaultIndexDentryExpiration = 60 * time.Minute

	DownloadFileSizeThreshold = 2 * unit.MB
)

const BatchDownloadV1 = 0

type CubeInfo struct {
	Prof uint64 `json:"prof"`
	MountPoint string `json:"mount_point"`
	LocalIP string 	`json:"local_ip"`
	VolName string  `json:"vol_name"`
}

type PrefetchManager struct {
	volName			 string
	mountPoint       string
	ec               *ExtentClient
	indexFileInfoMap sync.Map // key: index file path, value: *IndexInfo
	indexInfoChan    chan *IndexInfo
	filePathChan     chan *FileInfo
	downloadChan     chan *DownloadFileInfo
	appPid           sync.Map
	dcacheMap        sync.Map // key: parent Inode ID, value: *IndexDentryInfo

	metrics			 *PrefetchMetrics

	wg     sync.WaitGroup
	closeC chan struct{}
}

type PrefetchMetrics struct {
	appReadCount	uint64
	totalReadCount	uint64
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
	path  	string
	inoID 	uint64
	isAbs	bool
}

type DentryInfo struct {
	parIno uint64
	dcache *cache.DentryCache
}

func NewPrefetchManager(ec *ExtentClient, volName, mountPoint string, prefetchThreads int64) *PrefetchManager {
	InitReadBlockPool()
	pManager := &PrefetchManager{
		volName: 		volName,
		mountPoint:     mountPoint,
		ec:             ec,
		closeC:         make(chan struct{}, 1),
		indexInfoChan:  make(chan *IndexInfo, 1024),
		filePathChan:   make(chan *FileInfo, 10240000),
		downloadChan:   make(chan *DownloadFileInfo, 10240000),
		metrics: 		&PrefetchMetrics{0, 0},
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

const(
	Cube_Torch_ConfigFile="/tmp/cube_torch.config"
)

func (pManager *PrefetchManager) GenerateCubeInfo(localIP string, port uint64) (err error) {
	var (
		cubeInfoBytes []byte
		fd            *os.File
	)
	cubeInfo := &CubeInfo{Prof: port,MountPoint:pManager.mountPoint,LocalIP: localIP,VolName: pManager.volName}
	if cubeInfoBytes, err = json.Marshal(cubeInfo); err != nil {
		log.LogErrorf("GenerateCubeInfo: info(%v) json marshal err(%v)", cubeInfo, err)
		return
	}
	cubeTorchConfig:=fmt.Sprintf("%v.%v",Cube_Torch_ConfigFile,pManager.volName)
	err=ioutil.WriteFile(cubeTorchConfig,cubeInfoBytes,0666)
	if err!=nil{
		log.LogErrorf("Generate Cube_Torch_ConfigFile(%v): info(%v) json marshal err(%v)", Cube_Torch_ConfigFile,cubeInfo, err)
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
	ctx := context.Background()
	for {
		select {
		case <-pManager.closeC:
			log.LogInfof("backGroundPrefetchWorker: close worker(%v)", id)
			return

		case fileInfo := <-pManager.filePathChan:
			start := time.Now()
			tpObject := exporter.NewVolumeTP("Prefetch", pManager.volName)
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
			tpObject.Set(err)
			if log.IsDebugEnabled() {
				log.LogDebugf("backGroundPrefetchWorker: id(%v) prefetch ino(%v) path(%v) cost(%v) pathChan(%v)", id, fileInfo.inoID, absFilepath, time.Since(start), len(pManager.filePathChan))
			}

		case indexInfo := <-pManager.indexInfoChan:
			start := time.Now()
			if err := pManager.LoadIndex(indexInfo); err != nil {
				log.LogWarnf("backGroundPrefetchWorker: index(%v) read err(%v)", indexInfo, err)
			}
			if log.IsDebugEnabled() {
				log.LogDebugf("backGroundPrefetchWorker: id(%v) read index(%v) cost(%v)", id, indexInfo, time.Since(start))
			}

		case dInfo := <-pManager.downloadChan:
			start := time.Now()
			tpObject := exporter.NewVolumeTP("Download", pManager.volName)
			err := pManager.ReadData(ctx, dInfo)
			if err != nil {
				log.LogWarnf("backGroundPrefetchWorker: file(%v) get data err(%v)", dInfo, err)
			}
			tpObject.Set(err)
			if log.IsDebugEnabled() {
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
			hitPercent := float64(100)
			if totalRead != 0 {
				hitPercent = float64(totalRead - appRead) / float64(totalRead) * 100
			}
			log.LogInfof("PrefetchManager: totalRead(%v) appRead(%v) hitCache(%.2f%%) pathChan(%v) batchDownloadChan(%v)",
				totalRead, appRead, hitPercent, len(pManager.filePathChan), len(pManager.downloadChan))
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
			log.LogWarnf("PrefetchByIndex: filepath(%v) has no line(%v)", key, index)
			return true
		}
		select {
		case <-pManager.closeC:
			err = fmt.Errorf("Prefetch threads are closed")
			return false
		case pManager.filePathChan <- fileInfo:
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

func (pManager *PrefetchManager) PrefetchByPath(filepath string) (err error) {
	fileInfo := &FileInfo{
		path:  filepath,
		isAbs: true,
	}
	select {
	case <-pManager.closeC:
		return fmt.Errorf("Prefetch threads are closed")
	case pManager.filePathChan <- fileInfo:
		return nil
	}
}

type DownloadFileInfo struct {
	absPath		string
	fileInfo	*FileInfo
	resp		*BatchDownloadRespWriter
}

func (d *DownloadFileInfo) String() string {
	if d == nil {
		return ""
	}
	return fmt.Sprintf("path(%v) info(%v)", d.absPath, d.fileInfo)
}

type BatchDownloadRespWriter struct {
	sync.Mutex
	Wg		sync.WaitGroup
	Writer	io.Writer
}

func (resp *BatchDownloadRespWriter) write(data []byte) {
	resp.Lock()
	// todo err
	resp.Writer.Write(data)
	resp.Writer.(http.Flusher).Flush()
	resp.Unlock()
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
	var noData []byte
	dInfo := &DownloadFileInfo{
		absPath:	path.Join(pManager.mountPoint, fileInfo.path),
		fileInfo: 	fileInfo,
		resp:     	respData,
	}
	respData.Wg.Add(1)
	select {
	case <-pManager.closeC:
		if len(noData) == 0 {
			noData = make([]byte, 16)
			binary.BigEndian.PutUint64(noData[0:8], uint64(0))
			binary.BigEndian.PutUint64(noData[8:16], uint64(0))
		}
		respData.write(noData)
		log.LogWarnf("DownloadData: threads are closed")
		respData.Wg.Done()
	case pManager.downloadChan <- dInfo:
	}
	return
}

func (pManager *PrefetchManager) DownloadPath(filePath string, respData *BatchDownloadRespWriter) {
	var noData []byte
	dInfo := &DownloadFileInfo{
		absPath:	filePath,
		resp:   	respData,
	}
	respData.Wg.Add(1)
	select {
	case <-pManager.closeC:
		if len(noData) == 0 {
			noData = make([]byte, 16)
			binary.BigEndian.PutUint64(noData[0:8], uint64(0))
			binary.BigEndian.PutUint64(noData[8:16], uint64(0))
		}
		respData.write(noData)
		log.LogWarnf("DownloadData: threads are closed")
		respData.Wg.Done()
	case pManager.downloadChan <- dInfo:
	}
	return
}

func (pManager *PrefetchManager) ReadData(ctx context.Context, dInfo *DownloadFileInfo) (err error) {
	var (
		content 	[]byte
		readSize	int
	)
	defer func() {
		filePath := dInfo.absPath
		dataLen := 8 + len(filePath) + 8 + readSize
		respData := GetBlockBuf(dataLen)
		binary.BigEndian.PutUint64(respData[0:8], uint64(len(filePath)))
		copy(respData[8:8+len(filePath)], filePath)
		binary.BigEndian.PutUint64(respData[8+len(filePath):16+len(filePath)], uint64(readSize))
		if readSize > 0 {
			copy(respData[16+len(filePath):dataLen], content[:readSize])
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("ReadData: resp data len(%v) pathLen(%v) readSize(%v)", dataLen, len(filePath), readSize)
		}
		dInfo.resp.write(respData[:dataLen])
		dInfo.resp.Wg.Done()

		PutBlockBuf(respData)
		if len(content) > 0 {
			PutBlockBuf(content)
		}
	}()

	var inodeID uint64
	if dInfo.fileInfo == nil || dInfo.fileInfo.inoID == 0 {
		var statInfo os.FileInfo
		if statInfo, err = os.Stat(dInfo.absPath); err != nil {
			return
		}
		inodeID = statInfo.Sys().(*syscall.Stat_t).Ino
	} else {
		inodeID = dInfo.fileInfo.inoID
	}
	if err = pManager.ec.OpenStream(inodeID, false); err != nil {
		return
	}
	defer func() {
		pManager.ec.CloseStream(ctx, inodeID)
	}()

	fileSize, _, valid := pManager.ec.FileSize(inodeID)
	if !valid || fileSize == 0 || fileSize > DownloadFileSizeThreshold {
		return fmt.Errorf("file size is (%v)", fileSize)
	}

	content = GetBlockBuf(int(fileSize))
	if readSize, _, err = pManager.ec.Read(ctx, inodeID, content[:fileSize], 0, int(fileSize)); err != nil {
		err = fmt.Errorf("read err(%v) offset(%v) size(%v) readBytes(%v)", err, 0, fileSize, readSize)
		return
	}
	if log.IsDebugEnabled() {
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
		if log.IsDebugEnabled() {
			log.LogDebugf("PutAppPid: (%v)", pid)
		}
	}
}

func (pManager *PrefetchManager) DeleteAppPid(pid uint32) {
	if pManager == nil {
		return
	}
	pManager.appPid.Delete(pid)
	if log.IsDebugEnabled() {
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

func copyString(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}
