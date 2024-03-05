// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/infra"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ttlstore"
	"github.com/cubefs/cubefs/util/unit"
	"golang.org/x/time/rate"
)

const (
	extentCRCHeaderFilename         = "EXTENT_CRC"
	baseExtentIDFilename            = "EXTENT_META"
	tinyDeleteFileOpenFlag          = os.O_CREATE | os.O_RDWR | os.O_APPEND
	tinyDeleteFilePerm              = 0666
	tinyExtentDeletedFilename       = "TINYEXTENT_DELETE"
	tinyExtentDeletedFilenameBackup = "TINYEXTENT_DELETE_BACK"
	inodeIndexFilename              = "INODE_INDEX"
	MaxExtentCount                  = 20000
	MinNormalExtentID               = 1024
	DeleteTinyRecordSize            = 24
	UpdateCrcInterval               = 600
	RepairInterval                  = 10
	RandomWriteType                 = 2
	AppendWriteType                 = 1
	BaseExtentIDPersistStep         = 500

	deletionQueueDirname                 = "Deletion"
	deletionQueueFilesize    int64       = 1024 * 1024 * 4
	deletionQueueRetainFiles RetainFiles = 10

	LoadInProgress int32 = 0
	LoadFinish     int32 = 1
)

var (
	RegexpExtentFile, _ = regexp.Compile("^(\\d)+$")
	SnapShotFilePool    = &sync.Pool{New: func() interface{} {
		return new(proto.File)
	}}
	ValidateCrcInterval = int64(20 * RepairInterval)
)

var (
	PartitionIsLoaddingErr = fmt.Errorf("partition is loadding")
)

type LimiterFunc func(ctx context.Context, op int, size uint32, bandType string) (err error)

func GetSnapShotFileFromPool() (f *proto.File) {
	f = SnapShotFilePool.Get().(*proto.File)
	return
}

func PutSnapShotFileToPool(f *proto.File) {
	SnapShotFilePool.Put(f)
}

type ExtentFilter func(info *ExtentInfoBlock) bool

// Filters
var (
	NormalExtentFilter = func() ExtentFilter {
		nowUnix := time.Now().Unix()
		return func(ei *ExtentInfoBlock) bool {
			return !proto.IsTinyExtent(ei[FileID]) && nowUnix-int64(ei[ModifyTime]) > RepairInterval && ei[Size] > 0
		}
	}

	TinyExtentFilter = func(filters []uint64) ExtentFilter {
		return func(ei *ExtentInfoBlock) bool {
			if !proto.IsTinyExtent(ei[FileID]) {
				return false
			}
			for _, filterID := range filters {
				if filterID == ei[FileID] {
					return true
				}
			}
			return false
		}
	}

	ExtentFilterForValidateCRC = func() ExtentFilter {
		nowUnix := time.Now().Unix()
		return func(ei *ExtentInfoBlock) bool {
			return nowUnix-int64(ei[ModifyTime]) > ValidateCrcInterval && ei[Size] > 0
		}
	}
)

// ExtentStore defines fields used in the storage engine.
// Packets smaller than 128K are stored in the "tinyExtent", a place to persist the small files.
// packets larger than or equal to 128K are stored in the normal "extent", a place to persist large files.
// The difference between them is that the extentID of a tinyExtent starts at 5000000 and ends at 5000128.
// Multiple small files can be appended to the same tinyExtent.
// In addition, the deletion of small files is implemented by the punch hole from the underlying file system.
type ExtentStore struct {
	dataPath              string
	baseExtentID          uint64 // TODO what is baseExtentID
	baseExtentIDPersistMu sync.Mutex
	//extentInfoMap                     sync.Map // map that stores all the extent information
	infoStore                         *ExtentInfoStore
	inodeIndex                        *inodeIndex
	cache                             *ExtentCache // extent cache
	mutex                             sync.Mutex
	storeSize                         int      // size of the extent store
	metadataFp                        *os.File // metadata file pointer?
	tinyExtentDeleteFp                *os.File
	closeC                            chan bool
	closed                            bool
	availableTinyExtentC              chan uint64 // available tinyExtent channel
	availableTinyExtentMap            sync.Map
	availableTinyExtentMutex          sync.Mutex
	brokenTinyExtentC                 chan uint64 // broken tinyExtent channel
	brokenTinyExtentMap               sync.Map
	brokenTinyExtentMutex             sync.Mutex
	blockSize                         int
	partitionID                       uint64
	verifyExtentFp                    *os.File
	hasAllocSpaceExtentIDOnVerfiyFile uint64
	loadStatus                        int32
	loadMux                           sync.Mutex
	tinyExtentDeleteMutex             sync.Mutex
	recentDeletedExtents              sync.Map

	deletionQueue *ExtentQueue

	interceptors IOInterceptors

	ttlStore      *ttlstore.TTLStore
	ttlStoreMutex sync.Mutex

	localFlushDeleteC chan struct{}
}

func MkdirAll(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func NewExtentStore(dataDir string, partitionID uint64, storeSize int,
	cacheCapacity int, ln CacheListener, isCreatePartition bool, ioi IOInterceptors) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataPath = dataDir
	s.partitionID = partitionID
	s.infoStore = NewExtentInfoStore(partitionID)
	s.interceptors = ioi
	s.ttlStore = ttlstore.NewTTLStore()
	s.localFlushDeleteC = make(chan struct{}, 1)
	if err = MkdirAll(dataDir); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}
	if s.tinyExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, tinyExtentDeletedFilename), tinyDeleteFileOpenFlag, tinyDeleteFilePerm); err != nil {
		return
	}
	stat, err := s.tinyExtentDeleteFp.Stat()
	if err != nil {
		return
	}
	if stat.Size()%DeleteTinyRecordSize != 0 {
		needWriteEmpty := DeleteTinyRecordSize - (stat.Size() % DeleteTinyRecordSize)
		data := make([]byte, needWriteEmpty)
		s.tinyExtentDeleteFp.Write(data)
	}
	if s.verifyExtentFp, err = os.OpenFile(path.Join(s.dataPath, extentCRCHeaderFilename), os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	if s.metadataFp, err = os.OpenFile(path.Join(s.dataPath, baseExtentIDFilename), os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	if s.inodeIndex, err = openInodeIndex(path.Join(s.dataPath, inodeIndexFilename)); err != nil {
		return
	}

	if s.deletionQueue, err = OpenExtentQueue(path.Join(s.dataPath, deletionQueueDirname), deletionQueueFilesize, deletionQueueRetainFiles); err != nil {
		return
	}

	s.cache = NewExtentCache(cacheCapacity, time.Minute*5, ln)
	if err = s.initBaseFileID(); err != nil {
		err = fmt.Errorf("init base field ID: %v", err)
		return
	}
	if err = s.loadRecentDeletedExtents(); err != nil {
		return
	}
	s.hasAllocSpaceExtentIDOnVerfiyFile = s.allocatedExtentHeader()
	s.storeSize = storeSize
	s.closeC = make(chan bool, 1)
	s.closed = false
	err = s.initTinyExtent()
	if err != nil {
		return
	}
	if isCreatePartition {
		atomic.StoreInt32(&s.loadStatus, LoadFinish)
	} else {
		atomic.StoreInt32(&s.loadStatus, LoadInProgress)
	}

	return
}

func (s *ExtentStore) WalkExtentsInfo(f func(info *ExtentInfoBlock)) {
	s.infoStore.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		f(ei)
	})
}

// SnapShot returns the information of all the extents on the current data partition.
// When the master sends the loadDataPartition request, the snapshot is used to compare the replicas.
func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var (
		normalExtentSnapshot, tinyExtentSnapshot []ExtentInfoBlock
	)

	if normalExtentSnapshot, err = s.GetAllWatermarks(proto.NormalExtentType, NormalExtentFilter()); err != nil {
		return
	}

	files = make([]*proto.File, 0, len(normalExtentSnapshot))
	for _, ei := range normalExtentSnapshot {
		file := GetSnapShotFileFromPool()
		file.Name = strconv.FormatUint(ei[FileID], 10)
		file.Size = uint32(ei[Size])
		file.Modified = int64(ei[ModifyTime])
		file.Crc = uint32(ei[Crc])
		files = append(files, file)
	}
	tinyExtentSnapshot = s.getTinyExtentInfo()
	for _, ei := range tinyExtentSnapshot {
		file := GetSnapShotFileFromPool()
		file.Name = strconv.FormatUint(ei[FileID], 10)
		file.Size = uint32(ei[Size])
		file.Modified = int64(ei[ModifyTime])
		file.Crc = 0
		files = append(files, file)
	}

	return
}

// Create creates an extent.
func (s *ExtentStore) Create(extentID, inode uint64, putCache bool) (err error) {
	if s.IsExists(extentID) || s.IsDeleted(extentID) {
		err = ExtentExistsError
		return err
	}
	var e *Extent
	name := path.Join(s.dataPath, strconv.Itoa(int(extentID)))

	var headerHandler = s.getExtentHeaderHandler(extentID)
	if e, err = CreateExtent(name, extentID, headerHandler, s.interceptors); err != nil {
		return err
	}
	if putCache {
		s.cache.Put(e)
		defer func() {
			if err != nil {
				s.cache.Del(extentID)
			}
		}()
	} else {
		defer func() {
			_ = e.Close(false)
		}()
	}
	if err = s.inodeIndex.Put(extentID, inode); err != nil {
		return
	}
	s.infoStore.Create(extentID, inode)
	s.infoStore.Update(extentID, 0, uint64(time.Now().Unix()), 0)
	_ = s.AdvanceBaseExtentID(extentID)
	return
}

const (
	BaseExtentAddNumOnInitExtentStore = 1000
)

func (s *ExtentStore) initBaseFileID() (err error) {
	var (
		baseFileID uint64
	)
	baseFileID, _ = s.GetPersistenceBaseExtentID()
	dirFd, err := os.Open(s.dataPath)
	if err != nil {
		return
	}
	defer func() {
		dirFd.Close()
	}()
	names, err := dirFd.Readdirnames(-1)
	var (
		extentID uint64
		isExtent bool
	)
	for _, name := range names {
		if extentID, isExtent = s.ExtentID(name); !isExtent {
			continue
		}
		if !proto.IsTinyExtent(extentID) && extentID > baseFileID {
			baseFileID = extentID
		}
		switch {
		case proto.IsTinyExtent(extentID):
			s.infoStore.Create(extentID, 0)
			name := path.Join(s.dataPath, strconv.FormatUint(extentID, 10))
			info, statErr := os.Stat(name)
			if statErr != nil {
				statErr = fmt.Errorf("restore from file %v system: %v", name, statErr)
				log.LogWarnf(statErr.Error())
				continue
			}
			watermark := info.Size()
			if watermark%PageSize != 0 {
				watermark = watermark + (PageSize - watermark%PageSize)
			}
			s.infoStore.Update(extentID, uint64(watermark), uint64(info.ModTime().Unix()), 0)
		case !s.IsDeleted(extentID):
			var inode uint64
			if inode, err = s.inodeIndex.Get(extentID); err != nil {
				return
			}
			s.infoStore.Create(extentID, inode)
		default:
		}
	}
	baseFileID = uint64(math.Max(float64(MinNormalExtentID), float64(baseFileID))) + BaseExtentAddNumOnInitExtentStore
	if err = s.AdvanceBaseExtentID(baseFileID); err != nil {
		return
	}
	log.LogInfof("datadir(%v) maxBaseId(%v)", s.dataPath, baseFileID)
	return nil
}

func (s *ExtentStore) loadRecentDeletedExtents() (err error) {
	var nowUnixSec = time.Now().Unix()
	err = s.deletionQueue.Walk(WalkAll, func(ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
		if !proto.IsTinyExtent(extent) {
			if s.IsExists(extent) {
				s.cache.Del(extent)
				s.infoStore.Delete(extent)
			}
			if nowUnixSec-timestamp < 3600*24 {
				s.recentDeletedExtents.Store(extent, timestamp)
				if log.IsDebugEnabled() {
					log.LogDebugf("Store(%v) register recent deleted NormalExtent: extent=%v, ino=%v, deletetime=%v", s.partitionID, extent, ino, timestamp)
				}
			}
		}
		return true, nil
	})
	return
}

// Load 加载存储引擎剩余未加载的必要信息.
func (s *ExtentStore) Load() {

	s.loadMux.Lock()
	defer s.loadMux.Unlock()

	if atomic.LoadInt32(&s.loadStatus) == LoadFinish {
		return
	}

	s.infoStore.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		if proto.IsTinyExtent(extentID) {
			return
		}
		extentAbsPath := path.Join(s.dataPath, strconv.FormatUint(extentID, 10))
		info, err := os.Stat(extentAbsPath)
		if err != nil {
			log.LogWarnf("asyncLoadExtentSize extentPath(%v) error(%v)", extentAbsPath, err)
			return
		}
		s.infoStore.Update(extentID, uint64(info.Size()), uint64(info.ModTime().Unix()), 0)
	})

	// Mark the load progress of this extent store has been finished.
	atomic.StoreInt32(&s.loadStatus, LoadFinish)
}

func (s *ExtentStore) IsFinishLoad() bool {
	return atomic.LoadInt32(&s.loadStatus) == LoadFinish
}

func (s *ExtentStore) getExtentInfoByExtentID(eid uint64) (ei *ExtentInfoBlock, ok bool) {
	ei, ok = s.infoStore.Load(eid)
	return
}

// Write writes the given extent to the disk.
func (s *ExtentStore) Write(ctx context.Context, extentID uint64, offset, size int64, data []byte, crc uint32, writeType int, isSync bool) (err error) {
	var (
		e *Extent
	)

	ei, ok := s.getExtentInfoByExtentID(extentID)
	if !ok || s.IsDeleted(extentID) {
		err = proto.ExtentNotFoundError
		return
	}
	e, err = s.ExtentWithHeader(ei)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return err
	}
	err = e.Write(data, offset, size, crc, writeType, isSync)
	if err != nil {
		return err
	}
	s.infoStore.UpdateInfoFromExtent(e, 0)
	return nil
}

func (s *ExtentStore) checkOffsetAndSize(extentID uint64, offset, size int64) error {
	if proto.IsTinyExtent(extentID) {
		return nil
	}
	if offset+size > unit.BlockSize*unit.BlockCount {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= unit.BlockCount*unit.BlockSize || size == 0 {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}

	//if size > unit.BlockSize {
	//	return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	//}
	return nil
}

// Read reads the extent based on the given id.
func (s *ExtentStore) Read(extentID uint64, offset, size int64, nbuf []byte, isRepairRead bool) (crc uint32, err error) {
	var e *Extent
	ei, _ := s.getExtentInfoByExtentID(extentID)
	if e, err = s.ExtentWithHeader(ei); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return
	}
	crc, err = e.Read(nbuf, offset, size, isRepairRead)

	return
}

func (s *ExtentStore) Fingerprint(extentID uint64, offset, size int64, strict bool) (fingerprint Fingerprint, err error) {
	if proto.IsTinyExtent(extentID) {
		return
	}
	var e *Extent
	ei, _ := s.getExtentInfoByExtentID(extentID)
	if e, err = s.ExtentWithHeader(ei); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return
	}
	fingerprint, err = e.Fingerprint(offset, size, strict)
	return
}

func (s *ExtentStore) tinyDelete(e *Extent, offset, size int64) (err error) {
	if offset+size > e.dataSize {
		return
	}
	var (
		hasDelete bool
	)
	if hasDelete, err = e.DeleteTiny(offset, size); err != nil {
		return
	}
	if hasDelete {
		return
	}
	if err = s.RecordTinyDelete(e.extentID, offset, size); err != nil {
		return
	}
	return
}

// __markDeleteOne is inner private function marks one given extent as deleted.
func (s *ExtentStore) __markDeleteOne(inode, extentID uint64, offset, size int64) (err error) {

	defer func() {
		if err != nil {
			log.LogErrorf("Store(%v) mark delete failed: ino=%v, extent=%v, offset=%v, size=%v, error=%v",
				s.partitionID, inode, extentID, offset, size, err)
			return
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("Store(%v) mark delete: ino=%v, extent=%v, offset=%v, size=%v",
				s.partitionID, inode, extentID, offset, size)
		}
	}()

	var nowUnixSec = time.Now().Unix()
	if !proto.IsTinyExtent(extentID) {
		if s.IsDeleted(extentID) {
			// Skip cause target have been marked as deleted.
			return
		}
		if ei, exists := s.getExtentInfoByExtentID(extentID); exists {
			if inode != 0 && ei[Inode] != 0 && inode != ei[Inode] {
				return NewParameterMismatchErr(fmt.Sprintf("inode mismatch: expected %v, actual %v", ei[Inode], inode))
			}
			s.cache.Del(extentID)
			s.infoStore.Delete(extentID)
		}
		s.recentDeletedExtents.Store(extentID, nowUnixSec)
	}
	err = s.deletionQueue.Produce(inode, extentID, offset, size, nowUnixSec)
	return
}

// __markDeleteOne is inner private function marks multiple given extent as deleted.
func (s *ExtentStore) __markDeleteMore(marker Marker) (err error) {
	var producer *BatchProducer
	if producer, err = s.deletionQueue.BatchProduce(marker.Len()); err != nil {
		return
	}

	var nowUnixSec = time.Now().Unix()
	marker.Walk(func(_ int, ino, extent uint64, offset, size int64) bool {
		if !proto.IsTinyExtent(extent) {
			if s.IsDeleted(extent) {
				// Skip cause target normal extent have been marked as deleted.
				return true
			}
			if ei, exist := s.getExtentInfoByExtentID(extent); exist {
				if ino != 0 && ei[Inode] != 0 && ino != ei[Inode] {
					return true
				}
				s.cache.Del(extent)
				s.infoStore.Delete(extent)
			}
			s.recentDeletedExtents.Store(extent, nowUnixSec)
		}
		producer.Add(ino, extent, offset, size, nowUnixSec)
		if log.IsDebugEnabled() {
			log.LogDebugf("Store(%v) batchMarker mark delete: ino=%v, extent=%v, offset=%v, size=%v",
				s.partitionID, ino, extent, offset, size)
		}
		return true
	})
	err = producer.Submit()
	return
}

// MarkDelete is exported public method marks given marked extent(s) as deleted.
func (s *ExtentStore) MarkDelete(marker Marker) (err error) {

	defer func() {
		if err != nil {
			log.LogErrorf("Store(%v) batchMarker mark delete failed: batchsize=%v, error=%v", s.partitionID, marker.Len(), err)
			return
		}
		if log.IsDebugEnabled() && marker.Len() > 0 {
			log.LogDebugf("Store(%v) batchMarker mark delete extents: batchsize=%v", s.partitionID, marker.Len())
		}
	}()

	switch marker.Len() {
	case 0:
		return
	case 1:
		err = s.__markDeleteOne(marker.Get(0))
	default:
		err = s.__markDeleteMore(marker)
	}
	return
}

func (s *ExtentStore) TransferDeleteV0() (archives int, err error) {
	pathV0 := path.Join(s.dataPath, ExtentReleasorDirName)
	if _, err = os.Stat(pathV0); os.IsNotExist(err) {
		return 0, nil
	}
	releaser, err := NewExtentReleaserV0(pathV0, s)
	if err != nil {
		return
	}
	return releaser.TransferDelete()
}

// FlushDelete is exported public method removes and releases storage space resource through delete marked extents.
// Parameter:
//   - interceptor:
//     The caller can inject an interceptor to track the execution of the delete operation.
//   - limit:
//     limit on the number of deletions to be performed.
//     When it is less than or equal to 0, it means there is no limit.
func (s *ExtentStore) FlushDelete(interceptor Interceptor, limit int) (deleted, remain int, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("Store(%v) flush delete failed: %v", s.partitionID, err)
			return
		}
		if log.IsDebugEnabled() && deleted > 0 {
			log.LogDebugf("Store(%v) flush delete complete, limit %v, deleted %v, remain %v",
				s.partitionID, limit, deleted, remain)
		}
	}()

	var release = s.__lockFlushDelete(false)
	if release == nil {
		return
	}
	defer release()

	if interceptor == nil {
		interceptor = noopInterceptor
	}

	err = s.deletionQueue.Consume(func(ino, extent uint64, offset, size, _ int64) (bool, error) {
		var err error
		var ctx context.Context
		if ctx, err = interceptor.Before(); err != nil {
			return false, err
		}
		defer func() {
			interceptor.After(ctx, 0, err)
		}()
		if err = s.__deleteExtent(ino, extent, offset, size); err != nil {
			if isIOError(err) {
				return false, err
			}
			err = nil
			return true, nil
		}
		deleted++
		return limit <= 0 || deleted < limit, nil
	})
	remain = s.deletionQueue.Remain()
	return
}

func (s *ExtentStore) __deleteExtent(ino, extent uint64, offset, size int64) (err error) {
	if proto.IsTinyExtent(extent) {
		if log.IsDebugEnabled() {
			log.LogDebugf("Store(%v) flush delete TinyExtent: extent=%v, offset=%v, size=%v",
				s.partitionID, extent, offset, size)
		}
		var info, ok = s.getExtentInfoByExtentID(extent)
		if !ok {
			return
		}
		var e *Extent
		if e, err = s.ExtentWithHeader(info); err != nil {
			return
		}
		if err = s.tinyDelete(e, offset, size); err != nil {
			log.LogErrorf("Store(%v) delete TinyExtent data failed: ino=%v, extent=%v, offset=%v, size=%v, error=%v",
				s.partitionID, ino, extent, offset, size, err)
		}
		return
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("Store(%v) flush delete NormalExtent: ino=%v, extent=%v",
			s.partitionID, ino, extent)
	}
	var filepath = path.Join(s.dataPath, strconv.FormatUint(extent, 10))
	var interceptor = s.interceptors.Get(IORemove)
	var ctx context.Context
	if ctx, err = interceptor.Before(); err != nil {
		return
	}
	if err = os.Remove(filepath); err != nil && os.IsNotExist(err) {
		err = nil
	}
	interceptor.After(ctx, 0, err)
	if err != nil {
		log.LogErrorf("Store(%v) remove NormalExtent file failed: extent=%v, ino=%v, filepath=%v, error=%v", s.partitionID, extent, ino, filepath, err)
	}
	if err = s.removeExtentHeader(extent); err != nil && log.IsWarnEnabled() {
		log.LogWarnf("Store(%v) remove block CRC info failed: extent=%v, ino=%v, error=%v", s.partitionID, extent, ino, err)
	}
	return
}

// PersistTinyDeleteRecord marks the given extent as deleted.
func (s *ExtentStore) PersistTinyDeleteRecord(extentID uint64, offset, size int64) (err error) {
	ei, ok := s.getExtentInfoByExtentID(extentID)
	if !ok {
		err = proto.ExtentNotFoundError
		return
	}
	if proto.IsTinyExtent(extentID) {
		var e *Extent
		if e, err = s.ExtentWithHeader(ei); err != nil {
			return
		}
		return s.RecordTinyDelete(e.extentID, offset, size)
	}
	return
}

// Close closes the extent store.
func (s *ExtentStore) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.closed {
		return
	}

	// Release cache
	s.deletionQueue.Close()
	_ = s.cache.Flusher().Flush(nil, nil)
	s.cache.Close()
	s.tinyExtentDeleteFp.Sync()
	s.tinyExtentDeleteFp.Close()
	s.verifyExtentFp.Sync()
	s.verifyExtentFp.Close()
	s.closed = true
	close(s.closeC)
}

// Watermark returns the extent info of the given extent on the record.
func (s *ExtentStore) Watermark(extentID uint64) (ei *ExtentInfoBlock, err error) {
	if !proto.IsTinyExtent(extentID) && !s.IsFinishLoad() {
		err = PartitionIsLoaddingErr
		return
	}
	ei, ok := s.getExtentInfoByExtentID(extentID)
	if !ok || ei == nil {
		err = fmt.Errorf("e %v not exist", s.getExtentKey(extentID))
		return
	}
	return
}

// Watermark returns the extent info of the given extent on the record.
func (s *ExtentStore) ForceWatermark(extentID uint64) (ei *ExtentInfoBlock, err error) {
	var e *Extent
	ei, ok := s.getExtentInfoByExtentID(extentID)
	if !ok || ei == nil {
		err = fmt.Errorf("e %v not exist", s.getExtentKey(extentID))
		return
	}
	if !s.IsFinishLoad() {
		e, err = s.ExtentWithHeader(ei)
		if err != nil {
			return
		}
		s.infoStore.UpdateInfoFromExtent(e, 0)
	}
	return
}

// GetTinyExtentOffset returns the offset of the given extent.
func (s *ExtentStore) GetTinyExtentOffset(extentID uint64) (watermark int64, err error) {
	einfo, err := s.Watermark(extentID)
	if err != nil {
		return
	}
	watermark = int64(einfo[Size])
	if watermark%PageSize != 0 {
		watermark = watermark + (PageSize - watermark%PageSize)
	}

	return
}

// GetAllWatermarks returns all the watermarks.
func (s *ExtentStore) GetAllWatermarks(extentType uint8, filter ExtentFilter) (extents []ExtentInfoBlock, err error) {
	extents = make([]ExtentInfoBlock, 0)
	s.infoStore.RangeDist(extentType, func(extentID uint64, ei *ExtentInfoBlock) {
		if filter != nil && !filter(ei) {
			return
		}
		extents = append(extents, *ei)
	})
	return
}

func (s *ExtentStore) GetAllWatermarksWithByteArr(extentType uint8, filter ExtentFilter) (tinyDeleteFileSize int64, data []byte, err error) {
	needSize := 0
	extents := make([]uint64, 0)
	s.infoStore.RangeDist(extentType, func(extentID uint64, ei *ExtentInfoBlock) {
		if filter != nil && !filter(ei) {
			return
		}
		needSize += 16

		extents = append(extents, ei[FileID])
		return
	})
	data = make([]byte, needSize)
	index := 0

	for _, eid := range extents {
		ei, ok := s.infoStore.Load(eid)
		if !ok {
			continue
		}
		binary.BigEndian.PutUint64(data[index:index+8], ei[FileID])
		index += 8
		binary.BigEndian.PutUint64(data[index:index+8], ei[Size])
		index += 8
	}
	data = data[:index]
	tinyDeleteFileSize, err = s.LoadTinyDeleteFileOffset()

	return
}

func (s *ExtentStore) GetAllExtentInfoWithByteArr(filter ExtentFilter) (data []byte, err error) {
	needSize := 0
	extents := make([]uint64, 0)
	s.infoStore.RangeDist(proto.AllExtentType, func(extentID uint64, ei *ExtentInfoBlock) {
		if filter != nil && !filter(ei) {
			return
		}
		needSize += 20
		extents = append(extents, ei[FileID])
		return
	})

	data = make([]byte, needSize)
	index := 0
	for _, eid := range extents {
		ei, ok := s.infoStore.Load(eid)
		if !ok {
			continue
		}
		binary.BigEndian.PutUint64(data[index:index+8], ei[FileID])
		index += 8
		binary.BigEndian.PutUint64(data[index:index+8], ei[Size])
		index += 8
		binary.BigEndian.PutUint32(data[index:index+4], uint32(ei[Crc]))
		index += 4
	}
	data = data[:index]
	return
}

func (s *ExtentStore) getTinyExtentInfo() (extents []ExtentInfoBlock) {
	extents = make([]ExtentInfoBlock, 0)
	s.infoStore.RangeTinyExtent(func(_ uint64, ei *ExtentInfoBlock) {
		extents = append(extents, *ei)
	})
	return
}

// ExtentID return the extent ID.
func (s *ExtentStore) ExtentID(filename string) (extentID uint64, isExtent bool) {
	if isExtent = RegexpExtentFile.MatchString(filename); !isExtent {
		return
	}
	var (
		err error
	)
	if extentID, err = strconv.ParseUint(filename, 10, 64); err != nil {
		isExtent = false
		return
	}
	isExtent = true
	return
}

func (s *ExtentStore) initTinyExtent() (err error) {
	s.availableTinyExtentC = make(chan uint64, proto.TinyExtentCount)
	s.brokenTinyExtentC = make(chan uint64, proto.TinyExtentCount)
	var extentID uint64

	for extentID = proto.TinyExtentStartID; extentID < proto.TinyExtentStartID+proto.TinyExtentCount; extentID++ {
		err = s.Create(extentID, 0, false)
		if err == nil || strings.Contains(err.Error(), syscall.EEXIST.Error()) || err == ExtentExistsError {
			err = nil
			s.brokenTinyExtentC <- extentID
			s.brokenTinyExtentMap.Store(extentID, true)
			continue
		}
		return err
	}

	return
}

// GetAvailableTinyExtent returns the available tiny extent from the channel.
func (s *ExtentStore) GetAvailableTinyExtent() (extentID uint64, err error) {
	s.availableTinyExtentMutex.Lock()
	defer s.availableTinyExtentMutex.Unlock()
	select {
	case extentID = <-s.availableTinyExtentC:
		s.availableTinyExtentMap.Delete(extentID)
		return
	default:
		return 0, NoAvailableExtentError

	}
}

// SendToAvailableTinyExtentC sends the extent to the channel that stores the available tiny extents.
func (s *ExtentStore) SendToAvailableTinyExtentC(extentID uint64) {
	s.availableTinyExtentMutex.Lock()
	defer s.availableTinyExtentMutex.Unlock()
	if _, ok := s.availableTinyExtentMap.Load(extentID); !ok {
		s.availableTinyExtentC <- extentID
		s.availableTinyExtentMap.Store(extentID, true)
	}
}

// AvailableTinyExtentCnt returns the count of the available tiny extents.
func (s *ExtentStore) AvailableTinyExtentCnt() int {
	return len(s.availableTinyExtentC)
}

// BrokenTinyExtentCnt returns the count of the broken tiny extents.
func (s *ExtentStore) BrokenTinyExtentCnt() int {
	return len(s.brokenTinyExtentC)
}

// MoveAllToBrokenTinyExtentC moves all the tiny extents to the channel stores the broken extents.
func (s *ExtentStore) MoveAllToBrokenTinyExtentC(cnt int) {
	for i := 0; i < cnt; i++ {
		extentID, err := s.GetAvailableTinyExtent()
		if err != nil {
			return
		}
		s.SendToBrokenTinyExtentC(extentID)
	}
}

// SendToBrokenTinyExtentC sends the given extent id to the channel.
func (s *ExtentStore) SendToBrokenTinyExtentC(extentID uint64) {
	s.brokenTinyExtentMutex.Lock()
	defer s.brokenTinyExtentMutex.Unlock()
	if _, ok := s.brokenTinyExtentMap.Load(extentID); !ok {
		s.brokenTinyExtentC <- extentID
		s.brokenTinyExtentMap.Store(extentID, true)
	}
}

// SendAllToBrokenTinyExtentC sends all the extents to the channel that stores the broken extents.
func (s *ExtentStore) SendAllToBrokenTinyExtentC(extentIds []uint64) {
	s.brokenTinyExtentMutex.Lock()
	defer s.brokenTinyExtentMutex.Unlock()
	for _, extentID := range extentIds {
		if _, ok := s.brokenTinyExtentMap.Load(extentID); !ok {
			s.brokenTinyExtentC <- extentID
			s.brokenTinyExtentMap.Store(extentID, true)
		}
	}
}

// GetBrokenTinyExtent returns the first broken extent in the channel.
func (s *ExtentStore) GetBrokenTinyExtent() (extentID uint64, err error) {
	s.brokenTinyExtentMutex.Lock()
	defer s.brokenTinyExtentMutex.Unlock()
	select {
	case extentID = <-s.brokenTinyExtentC:
		s.brokenTinyExtentMap.Delete(extentID)
		return
	default:
		return 0, NoBrokenExtentError

	}
}

// StoreSizeExtentID returns the size of the extent store
func (s *ExtentStore) StoreSizeExtentID(maxExtentID uint64) (totalSize uint64) {
	s.infoStore.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		if extentID <= maxExtentID {
			totalSize += ei[Size]
		}
		return
	})

	return totalSize
}

// StoreSizeExtentID returns the size of the extent store
func (s *ExtentStore) GetMaxExtentIDAndPartitionSize() (maxExtentID, totalSize uint64) {
	s.infoStore.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		if ei[FileID] > maxExtentID {
			maxExtentID = ei[FileID]
		}
		totalSize += ei[Size]
		return
	})
	return maxExtentID, totalSize
}

func MarshalTinyExtent(extentID uint64, offset, size int64) (data []byte) {
	data = make([]byte, DeleteTinyRecordSize)
	binary.BigEndian.PutUint64(data[0:8], extentID)
	binary.BigEndian.PutUint64(data[8:16], uint64(offset))
	binary.BigEndian.PutUint64(data[16:DeleteTinyRecordSize], uint64(size))
	return data
}

func UnMarshalTinyExtent(data []byte) (extentID, offset, size uint64) {
	extentID = binary.BigEndian.Uint64(data[0:8])
	offset = binary.BigEndian.Uint64(data[8:16])
	size = binary.BigEndian.Uint64(data[16:DeleteTinyRecordSize])
	return
}

func (s *ExtentStore) RecordTinyDelete(extentID uint64, offset, size int64) (err error) {
	record := MarshalTinyExtent(extentID, offset, size)
	stat, err := s.tinyExtentDeleteFp.Stat()
	if err != nil {
		return
	}
	if stat.Size()%DeleteTinyRecordSize != 0 {
		s.tinyExtentDeleteMutex.Lock()
		if stat.Size()%DeleteTinyRecordSize != 0 {
			needWriteEmpty := DeleteTinyRecordSize - (stat.Size() % DeleteTinyRecordSize)
			data := make([]byte, needWriteEmpty)
			s.tinyExtentDeleteFp.Write(data)
		}
		s.tinyExtentDeleteMutex.Unlock()
	}
	_, err = s.tinyExtentDeleteFp.Write(record)
	if err != nil {
		return
	}

	return
}

func (s *ExtentStore) ReadTinyDeleteRecords(offset, size int64, data []byte) (crc uint32, err error) {
	_, err = s.tinyExtentDeleteFp.ReadAt(data[:size], offset)
	if err == nil || err == io.EOF {
		err = nil
		crc = crc32.ChecksumIEEE(data[:size])
	}
	return
}

func (s *ExtentStore) CheckIsAvaliRandomWrite(extentID uint64, offset, size int64) (err error) {
	var e *Extent
	e, err = s.extentWithHeaderByExtentID(extentID)
	if err != nil {
		return
	}
	err = e.checkWriteParameter(offset, size, RandomWriteType)
	return
}

func (s *ExtentStore) LoadExtentWaterMark(extentID uint64) (size int64, err error) {
	var e *Extent
	e, err = s.extentWithHeaderByExtentID(extentID)
	if err != nil {
		return
	}
	return e.Size(), nil
}

// NextExtentID returns the next extentID. When the client sends the request to create an extent,
// this function generates an unique extentID within the current partition.
// This function can only be called by the leader.
func (s *ExtentStore) NextExtentID() (extentID uint64, err error) {
	s.baseExtentIDPersistMu.Lock()
	extentID = atomic.AddUint64(&s.baseExtentID, 1)
	err = s.persistenceBaseExtentID(extentID)
	s.baseExtentIDPersistMu.Unlock()
	return
}

func (s *ExtentStore) LoadTinyDeleteFileOffset() (offset int64, err error) {
	stat, err := s.tinyExtentDeleteFp.Stat()
	if err == nil {
		offset = stat.Size()
	}
	return
}

func (s *ExtentStore) LockFlushDelete() (release func()) {
	return s.__lockFlushDelete(true)
}

func (s *ExtentStore) DropTinyDeleteRecord() (err error) {
	err = s.tinyExtentDeleteFp.Truncate(0)
	if err != nil {
		return
	}
	_, err = s.tinyExtentDeleteFp.Seek(0, io.SeekStart)
	return err
}

func (s *ExtentStore) getExtentKey(extent uint64) string {
	return fmt.Sprintf("extent %v_%v", s.partitionID, extent)
}

// AdvanceBaseExtentID updates the base extent ID.
func (s *ExtentStore) AdvanceBaseExtentID(id uint64) (err error) {
	if proto.IsTinyExtent(id) {
		return
	}
	s.baseExtentIDPersistMu.Lock()
	for {
		current := atomic.LoadUint64(&s.baseExtentID)
		if id <= current {
			s.baseExtentIDPersistMu.Unlock()
			return
		}
		if atomic.CompareAndSwapUint64(&s.baseExtentID, current, id) {
			break
		}
	}
	err = s.persistenceBaseExtentID(id)
	s.baseExtentIDPersistMu.Unlock()
	s.allocateExtentHeader(atomic.LoadUint64(&s.baseExtentID))
	return
}

func (s *ExtentStore) extent(extentID uint64) (e *Extent, err error) {
	if e, err = s.loadExtentFromDisk(extentID, false); err != nil {
		err = fmt.Errorf("load extent from disk: %v", err)
		return nil, err
	}
	return
}

func (s *ExtentStore) ExtentWithHeader(ei *ExtentInfoBlock) (e *Extent, err error) {
	var ok bool
	if ei == nil {
		err = proto.ExtentNotFoundError
		return
	}
	if e, ok = s.cache.Get(ei[FileID]); !ok {
		if e, err = s.loadExtentFromDisk(ei[FileID], true); err != nil {
			err = fmt.Errorf("load  %v from disk: %v", s.getExtentKey(ei[FileID]), err)
			return nil, err
		}
	}
	return
}

func (s *ExtentStore) extentWithHeaderByExtentID(extentID uint64) (e *Extent, err error) {
	var ok bool
	if e, ok = s.cache.Get(extentID); !ok {
		if e, err = s.loadExtentFromDisk(extentID, true); err != nil {
			err = fmt.Errorf("load  %v from disk: %v", s.getExtentKey(extentID), err)
			return nil, err
		}
	}
	return
}

// IsExists tells if the extent store has the extent with the given ID
func (s *ExtentStore) IsExists(extentID uint64) (exist bool) {
	_, ok := s.getExtentInfoByExtentID(extentID)
	return ok
}

// IsDeleted tells if the normal extent is deleted recently, true-it must has been deleted, false-it may not be deleted
func (s *ExtentStore) IsDeleted(extentID uint64) (deleted bool) {
	_, ok := s.recentDeletedExtents.Load(extentID)
	return ok
}

// GetExtentCount returns the seq of extents in the extentInfoMap
func (s *ExtentStore) GetExtentCount() (count int) {
	return s.infoStore.Len()
}

func (s *ExtentStore) loadExtentFromDisk(extentID uint64, putCache bool) (e *Extent, err error) {
	name := path.Join(s.dataPath, strconv.Itoa(int(extentID)))
	var headerHandler = s.getExtentHeaderHandler(extentID)
	if e, err = OpenExtent(name, extentID, headerHandler, s.interceptors); err != nil {
		return
	}
	if !putCache {
		return
	}
	err = nil
	s.cache.Put(e)

	return
}

func (s *ExtentStore) getExtentHeaderHandler(extentID uint64) HeaderHandler {
	var fOff = int64(extentID * unit.BlockHeaderSize)
	var load = func(b []byte, off, size int64) (err error) {
		_, err = s.verifyExtentFp.ReadAt(b[:size], fOff+off)
		if err == io.EOF {
			err = nil
		}
		return
	}
	var save = func(b []byte, off, size int64) (err error) {
		_, err = s.verifyExtentFp.WriteAt(b[:size], fOff+off)
		return
	}
	return NewFuncHeaderHandler(load, save)
}

func (s *ExtentStore) ScanBlocks(extentID uint64) (bcs []*BlockCrc, err error) {
	var blockCnt int
	bcs = make([]*BlockCrc, 0)
	ei, _ := s.getExtentInfoByExtentID(extentID)
	e, err := s.ExtentWithHeader(ei)
	if err != nil {
		return bcs, err
	}
	blockCnt = int(e.Size() / unit.BlockSize)
	if e.Size()%unit.BlockSize != 0 {
		blockCnt += 1
	}
	for blockNo := 0; blockNo < blockCnt; blockNo++ {
		blockCrc := binary.BigEndian.Uint32(e.header[blockNo*unit.PerBlockCrcSize : (blockNo+1)*unit.PerBlockCrcSize])
		bcs = append(bcs, &BlockCrc{BlockNo: blockNo, Crc: blockCrc})
	}
	sort.Sort(BlockCrcArr(bcs))

	return
}

type ExtentInfoArr []*ExtentInfoBlock

func (arr ExtentInfoArr) Len() int           { return len(arr) }
func (arr ExtentInfoArr) Less(i, j int) bool { return arr[i][FileID] < arr[j][FileID] }
func (arr ExtentInfoArr) Swap(i, j int)      { arr[i], arr[j] = arr[j], arr[i] }

func (s *ExtentStore) AutoComputeExtentCrc() {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	needUpdateCrcExtents := make([]*ExtentInfoBlock, 0)
	var nowUnix = time.Now().Unix()
	s.infoStore.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		if !proto.IsTinyExtent(ei[FileID]) && nowUnix-int64(ei[ModifyTime]) > UpdateCrcInterval &&
			ei[Size] > 0 && ei[Crc] == 0 {
			needUpdateCrcExtents = append(needUpdateCrcExtents, ei)
		}
		return
	})
	sort.Sort(ExtentInfoArr(needUpdateCrcExtents))
	for _, ei := range needUpdateCrcExtents {
		e, err := s.ExtentWithHeader(ei)
		extentCrc, err := e.autoComputeExtentCrc()
		if err != nil {
			continue
		}
		s.infoStore.UpdateInfoFromExtent(e, extentCrc)
		time.Sleep(time.Millisecond * 100)
	}
}

func (s *ExtentStore) TinyExtentRecover(extentID uint64, offset, size int64, data []byte, crc uint32, isEmptyPacket bool) (err error) {
	if !proto.IsTinyExtent(extentID) {
		return fmt.Errorf("extent %v not tinyExtent", extentID)
	}

	var (
		e  *Extent
		ei *ExtentInfoBlock
	)
	ei, _ = s.getExtentInfoByExtentID(extentID)
	if e, err = s.ExtentWithHeader(ei); err != nil {
		return nil
	}

	if err = e.TinyExtentRecover(data, offset, size, isEmptyPacket); err != nil {
		return err
	}
	s.infoStore.UpdateInfoFromExtent(e, 0)

	return nil
}

func (s *ExtentStore) TinyExtentGetFinfoSize(extentID uint64) (size uint64, err error) {
	var (
		e *Extent
	)
	if !proto.IsTinyExtent(extentID) {
		return 0, fmt.Errorf("unavali extent id (%v)", extentID)
	}
	ei, _ := s.getExtentInfoByExtentID(extentID)
	if e, err = s.ExtentWithHeader(ei); err != nil {
		return
	}

	finfo, err := e.file.Stat()
	if err != nil {
		return 0, err
	}
	size = uint64(finfo.Size())

	return
}

func (s *ExtentStore) ComputeMd5Sum(extentID, offset, size uint64) (md5Sum string, err error) {
	extentIDAbsPath := path.Join(s.dataPath, strconv.FormatUint(extentID, 10))
	fp, err := os.Open(extentIDAbsPath)
	if err != nil {
		err = fmt.Errorf("open %v error %v", extentIDAbsPath, err)
		return
	}
	defer func() {
		fp.Close()
	}()
	md5Writer := md5.New()
	stat, err := fp.Stat()
	if err != nil {
		err = fmt.Errorf("stat %v error %v", extentIDAbsPath, err)
		return
	}
	if size == 0 {
		size = uint64(stat.Size())
	}
	if offset != 0 {
		_, err = fp.Seek(int64(offset), 0)
		if err != nil {
			err = fmt.Errorf("seek %v error %v", extentIDAbsPath, err)
			return
		}
	}
	_, err = io.CopyN(md5Writer, fp, int64(size))
	if err != nil {
		err = fmt.Errorf("ioCopy %v error %v", extentIDAbsPath, err)
		return
	}
	md5Sum = hex.EncodeToString(md5Writer.Sum(nil))

	return
}

func (s *ExtentStore) TinyExtentAvaliOffset(extentID uint64, offset int64) (newOffset, newEnd int64, err error) {
	var e *Extent
	if !proto.IsTinyExtent(extentID) {
		return 0, 0, fmt.Errorf("unavali extent(%v)", extentID)
	}
	ei, _ := s.getExtentInfoByExtentID(extentID)
	if e, err = s.ExtentWithHeader(ei); err != nil {
		return
	}

	defer func() {
		if err != nil && strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			newOffset = e.dataSize
			newEnd = e.dataSize
			err = nil
		}

	}()
	newOffset, newEnd, err = e.tinyExtentAvaliOffset(offset)

	return
}

const (
	DiskSectorSize = 512
)

func (s *ExtentStore) GetStoreUsedSize() (used int64) {
	used = int64(s.infoStore.NormalUsed())
	for eid := proto.TinyExtentStartID; eid < proto.TinyExtentStartID+proto.TinyExtentCount; eid++ {
		stat := new(syscall.Stat_t)
		err := syscall.Stat(fmt.Sprintf("%v/%v", s.dataPath, eid), stat)
		if err != nil {
			continue
		}
		used += stat.Blocks * DiskSectorSize
	}
	return
}

func (s *ExtentStore) GetBaseExtentID() uint64 {
	return atomic.LoadUint64(&s.baseExtentID)
}

func (s *ExtentStore) EvictExpiredCache() {
	s.cache.EvictExpired()
}

func (s *ExtentStore) ForceEvictCache(ratio unit.Ratio) {
	s.cache.ForceEvict(ratio)
}

// Flush 强制下刷存储引擎当前所有打开的FD，保证这些FD的数据在内核PageCache里的脏页全部回写.
// Deprecated: use Flusher instead.
func (s *ExtentStore) Flush(limiter *rate.Limiter) (err error) {
	if err = s.inodeIndex.Flush(); err != nil {
		return
	}
	if err = s.metadataFp.Sync(); err != nil {
		return
	}
	s.cache.Flush(limiter)
	return
}

func (s *ExtentStore) __innerFpFlusher() infra.Flusher {
	var flushFunc = func(opsLimiter, bpsLimiter *rate.Limiter) (err error) {
		if opsLimiter != nil {
			_ = opsLimiter.Wait(context.Background())
		}
		if err = s.inodeIndex.Flush(); err != nil {
			return
		}

		if opsLimiter != nil {
			_ = opsLimiter.Wait(context.Background())
		}
		if err = s.metadataFp.Sync(); err != nil {
			return
		}
		return nil
	}
	var countFunc = func() int {
		return 2
	}
	return infra.NewFuncFlusher(flushFunc, countFunc)
}

func (s *ExtentStore) Flusher() infra.Flusher {
	return infra.NewMultiFlusher(s.__innerFpFlusher(), s.cache.Flusher())
}

func (s *ExtentStore) EvictExpiredNormalExtentDeleteCache(expireTime int64) {
	var count int
	s.recentDeletedExtents.Range(func(key, value interface{}) bool {
		timeDelete := value.(int64)
		if timeDelete < time.Now().Unix()-expireTime {
			s.recentDeletedExtents.Delete(key)
			count++
		}
		return true
	})
	log.LogDebugf("action[EvictExpiredNormalExtentDeleteCache] Partition(%d) (%d) extent delete cache has been evicted.", s.partitionID, count)
}

func (s *ExtentStore) PlaybackTinyDelete(offset int64) (err error) {
	var (
		recordFileInfo os.FileInfo
		recordData           = make([]byte, DeleteTinyRecordSize)
		readOff        int64 = 0
		readN                = 0
		badCount             = 0
		successCount         = 0
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[PlaybackTinyDelete] partition(%v) err:%v", s.partitionID, err)
		}
		log.LogInfof("action[PlaybackTinyDelete] partition(%v), offset(%v), offCount(%v), badCount(%v), successCount(%v)", s.partitionID, offset, offset/DeleteTinyRecordSize, badCount, successCount)
	}()
	if offset%DeleteTinyRecordSize != 0 {
		offset = offset + (DeleteTinyRecordSize - offset%DeleteTinyRecordSize)
	}
	if recordFileInfo, err = s.tinyExtentDeleteFp.Stat(); err != nil {
		return
	}
	log.LogInfof("action[PlaybackTinyDelete] partition(%v) record file size(%v)", s.partitionID, recordFileInfo.Size())
	for readOff = offset; readOff < recordFileInfo.Size(); readOff += DeleteTinyRecordSize {
		readN, err = s.tinyExtentDeleteFp.ReadAt(recordData, readOff)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}
		if readN != DeleteTinyRecordSize {
			return
		}
		extentID, offset, size := UnMarshalTinyExtent(recordData[:readN])
		if !proto.IsTinyExtent(extentID) {
			badCount++
			continue
		}
		ei, ok := s.getExtentInfoByExtentID(extentID)
		if !ok {
			badCount++
			continue
		}
		var e *Extent
		if e, err = s.ExtentWithHeader(ei); err != nil {
			return
		}
		if _, err = e.DeleteTiny(int64(offset), int64(size)); err != nil {
			return
		}
		successCount++
	}
	return
}

func (s *ExtentStore) GetRealBlockCnt(extentID uint64) (block int64, err error) {
	ei, _ := s.getExtentInfoByExtentID(extentID)
	e, err := s.ExtentWithHeader(ei)
	if err != nil {
		return
	}
	block = e.getRealBlockCnt()
	return
}

func (s *ExtentStore) LockExtents(extentLockInfo proto.ExtentLockInfo) (err error) {
	s.ttlStoreMutex.Lock()
	defer s.ttlStoreMutex.Unlock()
	for _, extentKey := range extentLockInfo.ExtentKeys {
		if _, ok := s.ttlStore.Load(extentKey.ExtentId); ok {
			err = fmt.Errorf("%v, extentId(%v)", ExtentLockedError, extentKey.ExtentId)
			return
		}
	}
	for _, extentKey := range extentLockInfo.ExtentKeys {
		s.ttlStore.Store(extentKey.ExtentId, struct{}{}, extentLockInfo.LockTime)
	}
	return
}

func (s *ExtentStore) UnlockExtents(extentLockInfo proto.ExtentLockInfo) {
	s.ttlStoreMutex.Lock()
	defer s.ttlStoreMutex.Unlock()
	for _, extentKey := range extentLockInfo.ExtentKeys {
		s.ttlStore.Delete(extentKey.ExtentId)
	}
}

func (s *ExtentStore) LoadExtentLockInfo(extentId uint64) (value *ttlstore.Val, ok bool) {
	value, ok = s.ttlStore.Load(extentId)
	return
}

func (s *ExtentStore) RangeExtentLockInfo(f func(key interface{}, value *ttlstore.Val) bool) {
	s.ttlStore.Range(func(key interface{}, value *ttlstore.Val) bool {
		return f(key, value)
	})
}

func (s *ExtentStore) FreeExtentLockInfo() {
	s.ttlStore.FreeLockInfo()
	return
}

// __lockFlushDelete try lock or wait the deletion flushing process.
func (s *ExtentStore) __lockFlushDelete(wait bool) (release func()) {
	if wait {
		s.localFlushDeleteC <- struct{}{}
		return func() {
			<-s.localFlushDeleteC
		}
	}
	select {
	case s.localFlushDeleteC <- struct{}{}:
		return func() {
			<-s.localFlushDeleteC
		}
	default:
	}
	return nil
}
