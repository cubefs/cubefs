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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

//TODO: remove this later.
//go:generate golangci-lint run --issues-exit-code=1 -D errcheck -E bodyclose ./...

const (
	ExtCrcHeaderFileName     = "EXTENT_CRC"
	ExtBaseExtentIDFileName  = "EXTENT_META"
	TinyDeleteFileOpt        = os.O_CREATE | os.O_RDWR | os.O_APPEND
	TinyExtDeletedFileName   = "TINYEXTENT_DELETE"
	NormalExtDeletedFileName = "NORMALEXTENT_DELETE"
	MaxExtentCount           = 20000
	TinyExtentCount          = 64
	TinyExtentStartID        = 1
	MinExtentID              = 1024
	DeleteTinyRecordSize     = 24
	UpdateCrcInterval        = 600
	RepairInterval           = 60
	RandomWriteType          = 2
	AppendWriteType          = 1
	AppendRandomWriteType    = 4

	NormalExtentDeleteRetainTime = 3600 * 4

	StaleExtStoreBackupSuffix = ".old"
	StaleExtStoreTimeFormat   = "20060102150405.000000000"
)

var (
	RegexpExtentFile, _ = regexp.Compile(`^(\d)+$`)
	SnapShotFilePool    = &sync.Pool{New: func() interface{} {
		return new(proto.File)
	}}
)

func GetSnapShotFileFromPool() (f *proto.File) {
	f = SnapShotFilePool.Get().(*proto.File)
	return
}

func PutSnapShotFileToPool(f *proto.File) {
	SnapShotFilePool.Put(f)
}

type ExtentFilter func(info *ExtentInfo) bool

// Filters
var (
	NormalExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(ei *ExtentInfo) bool {
			return !IsTinyExtent(ei.FileID) && now.Unix()-ei.ModifyTime > RepairInterval && !ei.IsDeleted
		}
	}

	TinyExtentFilter = func(filters []uint64) ExtentFilter {
		return func(ei *ExtentInfo) bool {
			if !IsTinyExtent(ei.FileID) {
				return false
			}
			for _, filterID := range filters {
				if filterID == ei.FileID {
					return true
				}
			}
			return false
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
	dataPath               string
	baseExtentID           uint64                 // TODO what is baseExtentID
	extentInfoMap          map[uint64]*ExtentInfo // map that stores all the extent information
	eiMutex                sync.RWMutex           // mutex for extent info
	cache                  *ExtentCache           // extent cache
	mutex                  sync.Mutex
	storeSize              int      // size of the extent store
	metadataFp             *os.File // metadata file pointer?
	tinyExtentDeleteFp     *os.File
	normalExtentDeleteFp   *os.File
	closeC                 chan bool
	closed                 bool
	availableTinyExtentC   chan uint64 // available tinyExtent channel
	availableTinyExtentMap sync.Map
	brokenTinyExtentC      chan uint64 // broken tinyExtent channel
	brokenTinyExtentMap    sync.Map
	// blockSize                         int
	partitionID    uint64
	verifyExtentFp *os.File

	verifyExtentFpAppend              []*os.File
	hasAllocSpaceExtentIDOnVerfiyFile uint64
	hasDeleteNormalExtentsCache       sync.Map
	partitionType                     int
	ApplyId                           uint64
	ApplyIdMutex                      sync.RWMutex
}

func MkdirAll(name string) (err error) {
	return os.MkdirAll(name, 0o755)
}

func NewExtentStore(dataDir string, partitionID uint64, storeSize, dpType int, isCreate bool) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataPath = dataDir
	s.partitionType = dpType
	s.partitionID = partitionID

	if isCreate {
		if err = s.renameStaleExtentStore(); err != nil {
			return
		}
		if err = MkdirAll(dataDir); err != nil {
			return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
		}

		if s.tinyExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, TinyExtDeletedFileName), TinyDeleteFileOpt, 0o666); err != nil {
			return
		}
		if s.verifyExtentFp, err = os.OpenFile(path.Join(s.dataPath, ExtCrcHeaderFileName), os.O_CREATE|os.O_RDWR, 0o666); err != nil {
			return
		}
		if s.metadataFp, err = os.OpenFile(path.Join(s.dataPath, ExtBaseExtentIDFileName), os.O_CREATE|os.O_RDWR, 0o666); err != nil {
			return
		}
		if s.normalExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, NormalExtDeletedFileName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666); err != nil {
			return
		}
	} else {
		if err = MkdirAll(dataDir); err != nil {
			return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
		}
		if s.tinyExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, TinyExtDeletedFileName), os.O_RDWR|os.O_APPEND, 0o666); err != nil {
			return
		}
		if s.verifyExtentFp, err = os.OpenFile(path.Join(s.dataPath, ExtCrcHeaderFileName), os.O_RDWR, 0o666); err != nil {
			return
		}
		if s.metadataFp, err = os.OpenFile(path.Join(s.dataPath, ExtBaseExtentIDFileName), os.O_RDWR, 0o666); err != nil {
			return
		}
		if s.normalExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, NormalExtDeletedFileName), os.O_RDWR|os.O_APPEND, 0o666); err != nil {
			return
		}
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

	log.LogDebugf("NewExtentStore.partitionID [%v] dataPath %v verifyExtentFp init", partitionID, s.dataPath)
	if s.verifyExtentFp, err = os.OpenFile(path.Join(s.dataPath, ExtCrcHeaderFileName), os.O_CREATE|os.O_RDWR, 0o666); err != nil {
		return
	}

	aId := 0
	var vFp *os.File
	for {
		dataPath := path.Join(s.dataPath, ExtCrcHeaderFileName+"_"+strconv.Itoa(aId))
		if _, err = os.Stat(dataPath); err != nil {
			log.LogDebugf("NewExtentStore. partitionID [%v] dataPath not exist err %v. verifyExtentFpAppend init return", partitionID, err)
			break
		}
		if vFp, err = os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0o666); err != nil {
			log.LogErrorf("NewExtentStore. partitionID [%v] dataPath exist but open err %v. verifyExtentFpAppend init return", partitionID, err)
			return
		}
		log.LogDebugf("NewExtentStore. partitionID [%v] dataPath exist and opened id %v", partitionID, aId)
		s.verifyExtentFpAppend = append(s.verifyExtentFpAppend, vFp)
		aId++
	}
	if s.metadataFp, err = os.OpenFile(path.Join(s.dataPath, ExtBaseExtentIDFileName), os.O_CREATE|os.O_RDWR, 0o666); err != nil {
		return
	}
	if s.normalExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, NormalExtDeletedFileName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666); err != nil {
		return
	}

	s.extentInfoMap = make(map[uint64]*ExtentInfo)
	s.cache = NewExtentCache(100)
	if err = s.initBaseFileID(); err != nil {
		err = fmt.Errorf("init base field ID: %v", err)
		return
	}
	s.hasAllocSpaceExtentIDOnVerfiyFile = s.GetPreAllocSpaceExtentIDOnVerifyFile()
	s.storeSize = storeSize
	s.closeC = make(chan bool, 1)
	s.closed = false
	err = s.initTinyExtent()
	if err != nil {
		return
	}
	return
}

func (ei *ExtentInfo) UpdateExtentInfo(extent *Extent, crc uint32) {
	extent.Lock()
	defer extent.Unlock()

	if time.Now().Unix()-extent.ModifyTime() <= UpdateCrcInterval {
		crc = 0
	}

	ei.Size = uint64(extent.dataSize)
	ei.SnapshotDataOff = extent.snapshotDataOff

	log.LogInfof("action[ExtentInfo.UpdateExtentInfo] ei info [%v]", ei.String())

	if !IsTinyExtent(ei.FileID) {
		atomic.StoreUint32(&ei.Crc, crc)
		ei.ModifyTime = extent.ModifyTime()
	}
}

// SnapShot returns the information of all the extents on the current data partition.
// When the master sends the loadDataPartition request, the snapshot is used to compare the replicas.
func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var normalExtentSnapshot, tinyExtentSnapshot []*ExtentInfo

	// compute crc again to guarantee crc and applyID is the newest
	s.autoComputeExtentCrc()

	if normalExtentSnapshot, _, err = s.GetAllWatermarks(NormalExtentFilter()); err != nil {
		log.LogErrorf("SnapShot GetAllWatermarks err %v", err)
		return
	}

	files = make([]*proto.File, 0, len(normalExtentSnapshot))
	for _, ei := range normalExtentSnapshot {
		file := GetSnapShotFileFromPool()
		file.Name = strconv.FormatUint(ei.FileID, 10)
		file.Size = uint32(ei.Size)
		file.Modified = ei.ModifyTime
		file.Crc = atomic.LoadUint32(&ei.Crc)
		file.ApplyID = ei.ApplyID
		log.LogDebugf("partitionID %v ExtentStore set applyid %v partition %v", s.partitionID, s.ApplyId, s.partitionID)
		files = append(files, file)
	}
	tinyExtentSnapshot = s.getTinyExtentInfo()
	for _, ei := range tinyExtentSnapshot {
		file := GetSnapShotFileFromPool()
		file.Name = strconv.FormatUint(ei.FileID, 10)
		file.Size = uint32(ei.Size)
		file.Modified = ei.ModifyTime
		file.Crc = 0
		files = append(files, file)
	}

	return
}

// Create creates an extent.
func (s *ExtentStore) Create(extentID uint64) (err error) {
	var e *Extent
	name := path.Join(s.dataPath, strconv.Itoa(int(extentID)))
	if s.HasExtent(extentID) {
		err = ExtentExistsError
		return err
	}

	e = NewExtentInCore(name, extentID)
	e.header = make([]byte, util.BlockHeaderSize)
	err = e.InitToFS()
	if err != nil {
		return err
	}

	s.cache.Put(e)
	extInfo := &ExtentInfo{FileID: extentID}
	extInfo.UpdateExtentInfo(e, 0)

	atomic.StoreInt64(&extInfo.AccessTime, e.accessTime)
	s.eiMutex.Lock()
	s.extentInfoMap[extentID] = extInfo
	s.eiMutex.Unlock()

	s.UpdateBaseExtentID(extentID)
	return
}

func (s *ExtentStore) initBaseFileID() error {
	var baseFileID uint64
	baseFileID, _ = s.GetPersistenceBaseExtentID()
	files, err := os.ReadDir(s.dataPath)
	if err != nil {
		return err
	}

	var (
		extentID uint64
		isExtent bool
		e        *Extent
		ei       *ExtentInfo
		loadErr  error
	)
	for _, f := range files {
		if extentID, isExtent = s.ExtentID(f.Name()); !isExtent {
			continue
		}

		if e, loadErr = s.extent(extentID); loadErr != nil {
			log.LogError("[initBaseFileID] load extent error", loadErr)
			continue
		}

		ei = &ExtentInfo{FileID: extentID}
		ei.UpdateExtentInfo(e, 0)
		atomic.StoreInt64(&ei.AccessTime, e.accessTime)

		s.eiMutex.Lock()
		s.extentInfoMap[extentID] = ei
		s.eiMutex.Unlock()

		e.Close()
		if !IsTinyExtent(extentID) && extentID > baseFileID {
			baseFileID = extentID
		}
	}
	if baseFileID < MinExtentID {
		baseFileID = MinExtentID
	}
	atomic.StoreUint64(&s.baseExtentID, baseFileID)
	log.LogInfof("datadir(%v) maxBaseId(%v)", s.dataPath, baseFileID)
	runtime.GC()
	return nil
}

// Write writes the given extent to the disk.
func (s *ExtentStore) Write(extentID uint64, offset, size int64, data []byte, crc uint32, writeType int, isSync bool, isHole bool) (status uint8, err error) {
	var (
		e  *Extent
		ei *ExtentInfo
	)
	status = proto.OpOk
	s.eiMutex.Lock()
	ei = s.extentInfoMap[extentID]
	e, err = s.extentWithHeader(ei)
	s.eiMutex.Unlock()
	if err != nil {
		return status, err
	}
	// update access time
	atomic.StoreInt64(&ei.AccessTime, time.Now().Unix())
	log.LogDebugf("action[Write] dp %v extentID %v offset %v size %v writeTYPE %v", s.partitionID, extentID, offset, size, writeType)
	if err = s.checkOffsetAndSize(extentID, offset, size, writeType); err != nil {
		log.LogInfof("action[Write] path %v err %v", e.filePath, err)
		return status, err
	}

	status, err = e.Write(data, offset, size, crc, writeType, isSync, s.PersistenceBlockCrc, ei, isHole)
	if err != nil {
		log.LogInfof("action[Write] path %v err %v", e.filePath, err)
		return status, err
	}

	ei.UpdateExtentInfo(e, 0)
	return status, nil
}

func (s *ExtentStore) checkOffsetAndSize(extentID uint64, offset, size int64, writeType int) error {
	if IsTinyExtent(extentID) {
		return nil
	}
	// random write pos can happen on modAppend partition of extent
	if writeType == RandomWriteType {
		return nil
	}
	if writeType == AppendRandomWriteType {
		if offset < util.ExtentSize {
			return newParameterError("writeType=%d offset=%d size=%d", writeType, offset, size)
		}
		return nil
	}
	if size == 0 || size > util.BlockSize ||
		offset >= util.BlockCount*util.BlockSize ||
		offset+size > util.BlockCount*util.BlockSize {
		return newParameterError("offset=%d size=%d", offset, size)
	}
	return nil
}

// IsTinyExtent checks if the given extent is tiny extent.
func IsTinyExtent(extentID uint64) bool {
	return extentID >= TinyExtentStartID && extentID < TinyExtentStartID+TinyExtentCount
}

// Read reads the extent based on the given id.
func (s *ExtentStore) Read(extentID uint64, offset, size int64, nbuf []byte, isRepairRead bool) (crc uint32, err error) {
	var e *Extent
	s.eiMutex.RLock()
	ei := s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()

	if ei == nil {
		return 0, errors.Trace(ExtentHasBeenDeletedError, "[Read] extent[%d] is already been deleted", extentID)
	}

	// update extent access time
	atomic.StoreInt64(&ei.AccessTime, time.Now().Unix())

	if e, err = s.extentWithHeader(ei); err != nil {
		return
	}

	//if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
	//	return
	//}
	crc, err = e.Read(nbuf, offset, size, isRepairRead)

	return
}

func (s *ExtentStore) DumpExtents() (extInfos SortedExtentInfos) {
	s.eiMutex.RLock()
	for _, v := range s.extentInfoMap {
		extInfos = append(extInfos, v)
	}
	s.eiMutex.RUnlock()
	return
}

func (s *ExtentStore) punchDelete(extentID uint64, offset, size int64) (err error) {
	e, err := s.extentWithHeaderByExtentID(extentID)
	if err != nil {
		return nil
	}
	if offset+size > e.dataSize {
		return
	}
	var hasDelete bool
	if hasDelete, err = e.punchDelete(offset, size); err != nil {
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

// MarkDelete marks the given extent as deleted.
func (s *ExtentStore) MarkDelete(extentID uint64, offset, size int64) (err error) {
	var ei *ExtentInfo
	s.eiMutex.RLock()
	ei = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if ei == nil || ei.IsDeleted {
		return
	}
	log.LogDebugf("action[MarkDelete] extentID %v offset %v size %v ei(size %v snapshotSize %v)",
		extentID, offset, size, ei.Size, ei.SnapshotDataOff)

	funcNeedPunchDel := func() bool {
		return offset != 0 || (size != 0 && ((ei.Size != uint64(size) && ei.SnapshotDataOff == util.ExtentSize) ||
			(ei.SnapshotDataOff != uint64(size) && ei.SnapshotDataOff > util.ExtentSize)))
	}

	if IsTinyExtent(extentID) || funcNeedPunchDel() {
		log.LogDebugf("action[MarkDelete] extentID %v offset %v size %v ei(size %v snapshotSize %v)",
			extentID, offset, size, ei.Size, ei.SnapshotDataOff)
		return s.punchDelete(extentID, offset, size)
	}

	extentFilePath := path.Join(s.dataPath, strconv.FormatUint(extentID, 10))
	log.LogDebugf("action[MarkDelete] extentID %v offset %v size %v ei(size %v extentFilePath %v)",
		extentID, offset, size, ei.Size, extentFilePath)
	if err = os.Remove(extentFilePath); err != nil && !os.IsNotExist(err) {
		// NOTE: if remove failed
		// we meet a disk error
		err = BrokenDiskError
		return
	}
	if err = s.PersistenceHasDeleteExtent(extentID); err != nil {
		err = BrokenDiskError
		return
	}
	ei.IsDeleted = true
	ei.ModifyTime = time.Now().Unix()
	s.cache.Del(extentID)
	if err = s.DeleteBlockCrc(extentID); err != nil {
		err = BrokenDiskError
		return
	}
	s.PutNormalExtentToDeleteCache(extentID)

	s.eiMutex.Lock()
	delete(s.extentInfoMap, extentID)
	s.eiMutex.Unlock()

	return
}

func (s *ExtentStore) PutNormalExtentToDeleteCache(extentID uint64) {
	s.hasDeleteNormalExtentsCache.Store(extentID, time.Now().Unix())
}

func (s *ExtentStore) IsDeletedNormalExtent(extentID uint64) (ok bool) {
	_, ok = s.hasDeleteNormalExtentsCache.Load(extentID)
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
	s.cache.Flush()
	s.cache.Clear()
	s.tinyExtentDeleteFp.Sync()
	s.tinyExtentDeleteFp.Close()
	s.normalExtentDeleteFp.Sync()
	s.normalExtentDeleteFp.Close()
	s.verifyExtentFp.Sync()
	s.verifyExtentFp.Close()
	for _, vFp := range s.verifyExtentFpAppend {
		if vFp != nil {
			vFp.Sync()
			vFp.Close()
		}
	}
	s.closed = true
}

// Watermark returns the extent info of the given extent on the record.
func (s *ExtentStore) Watermark(extentID uint64) (ei *ExtentInfo, err error) {
	var has bool
	s.eiMutex.RLock()
	ei, has = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if !has {
		err = fmt.Errorf("e %v not exist", s.getExtentKey(extentID))
		return
	}
	return
}

// GetTinyExtentOffset returns the offset of the given extent.
func (s *ExtentStore) GetTinyExtentOffset(extentID uint64) (watermark int64, err error) {
	einfo, err := s.Watermark(extentID)
	if err != nil {
		return
	}
	watermark = int64(einfo.Size)
	if watermark%util.PageSize != 0 {
		watermark = watermark + (util.PageSize - watermark%util.PageSize)
	}

	return
}

// GetTinyExtentOffset returns the offset of the given extent.
func (s *ExtentStore) GetExtentSnapshotModOffset(extentID uint64, allocSize uint32) (watermark int64, err error) {
	einfo, err := s.Watermark(extentID)
	if err != nil {
		return
	}
	log.LogDebugf("action[ExtentStore.GetExtentSnapshotModOffset] extId %v SnapshotDataOff %v SnapPreAllocDataOff %v allocSize %v",
		extentID, einfo.SnapshotDataOff, einfo.SnapPreAllocDataOff, allocSize)

	if einfo.SnapPreAllocDataOff == 0 {
		einfo.SnapPreAllocDataOff = einfo.SnapshotDataOff
	}
	watermark = int64(einfo.SnapPreAllocDataOff)
	//if watermark%util.PageSize != 0 {
	//	watermark = watermark + (util.PageSize - watermark%util.PageSize)
	//}
	einfo.SnapPreAllocDataOff += uint64(allocSize)

	return
}

// Sector size
const (
	DiskSectorSize = 512
)

func (s *ExtentStore) GetStoreUsedSize() (used int64) {
	extentInfoSlice := make([]*ExtentInfo, 0, s.GetExtentCount())
	s.eiMutex.RLock()
	for _, extentID := range s.extentInfoMap {
		extentInfoSlice = append(extentInfoSlice, extentID)
	}
	s.eiMutex.RUnlock()
	for _, einfo := range extentInfoSlice {
		if einfo.IsDeleted {
			continue
		}
		if IsTinyExtent(einfo.FileID) {
			stat := new(syscall.Stat_t)
			err := syscall.Stat(fmt.Sprintf("%v/%v", s.dataPath, einfo.FileID), stat)
			if err != nil {
				continue
			}
			used += stat.Blocks * DiskSectorSize
		} else {
			used += int64(einfo.Size + (einfo.SnapshotDataOff - util.ExtentSize))
		}
	}
	return
}

// GetAllWatermarks returns all the watermarks.
func (s *ExtentStore) GetAllWatermarks(filter ExtentFilter) (extents []*ExtentInfo, tinyDeleteFileSize int64, err error) {
	extents = make([]*ExtentInfo, 0, len(s.extentInfoMap))
	extentInfoSlice := make([]*ExtentInfo, 0, len(s.extentInfoMap))
	s.eiMutex.RLock()
	for _, extentID := range s.extentInfoMap {
		extentInfoSlice = append(extentInfoSlice, extentID)
	}
	s.eiMutex.RUnlock()

	for _, extentInfo := range extentInfoSlice {
		if filter != nil && !filter(extentInfo) {
			continue
		}
		if extentInfo.IsDeleted {
			continue
		}
		extents = append(extents, extentInfo)
	}
	tinyDeleteFileSize, err = s.LoadTinyDeleteFileOffset()

	return
}

func (s *ExtentStore) getTinyExtentInfo() (extents []*ExtentInfo) {
	extents = make([]*ExtentInfo, 0)
	s.eiMutex.RLock()
	var extentID uint64
	for extentID = TinyExtentStartID; extentID < TinyExtentCount+TinyExtentStartID; extentID++ {
		ei := s.extentInfoMap[extentID]
		if ei == nil {
			continue
		}
		extents = append(extents, ei)
	}
	s.eiMutex.RUnlock()

	return
}

// ExtentID return the extent ID.
func (s *ExtentStore) ExtentID(filename string) (extentID uint64, isExtent bool) {
	if isExtent = RegexpExtentFile.MatchString(filename); !isExtent {
		return
	}
	var err error
	if extentID, err = strconv.ParseUint(filename, 10, 64); err != nil {
		isExtent = false
		return
	}
	isExtent = true
	return
}

func (s *ExtentStore) initTinyExtent() (err error) {
	s.availableTinyExtentC = make(chan uint64, TinyExtentCount)
	s.brokenTinyExtentC = make(chan uint64, TinyExtentCount)
	var extentID uint64

	for extentID = TinyExtentStartID; extentID < TinyExtentStartID+TinyExtentCount; extentID++ {
		err = s.Create(extentID)
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
	select {
	case extentID = <-s.availableTinyExtentC:
		log.LogDebugf("dp %v GetAvailableTinyExtent. extentID %v", s.partitionID, extentID)
		s.availableTinyExtentMap.Delete(extentID)
		return
	default:
		log.LogDebugf("dp %v GetAvailableTinyExtent not found", s.partitionID)
		return 0, NoAvailableExtentError

	}
}

// SendToAvailableTinyExtentC sends the extent to the channel that stores the available tiny extents.
func (s *ExtentStore) SendToAvailableTinyExtentC(extentID uint64) {
	log.LogDebugf("dp %v action[SendToAvailableTinyExtentC] extentid %v", s.partitionID, extentID)
	if _, ok := s.availableTinyExtentMap.Load(extentID); !ok {
		log.LogDebugf("dp %v SendToAvailableTinyExtentC. extentID %v", s.partitionID, extentID)
		s.availableTinyExtentC <- extentID
		s.availableTinyExtentMap.Store(extentID, true)
	} else {
		log.LogDebugf("dp %v action[SendToAvailableTinyExtentC] extentid %v already exist", s.partitionID, extentID)
	}
}

// SendAllToBrokenTinyExtentC sends all the extents to the channel that stores the broken extents.
func (s *ExtentStore) SendAllToBrokenTinyExtentC(extentIds []uint64) {
	for _, extentID := range extentIds {
		if _, ok := s.brokenTinyExtentMap.Load(extentID); !ok {
			s.brokenTinyExtentC <- extentID
			s.brokenTinyExtentMap.Store(extentID, true)
		}
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
	if _, ok := s.brokenTinyExtentMap.Load(extentID); !ok {
		s.brokenTinyExtentC <- extentID
		s.brokenTinyExtentMap.Store(extentID, true)
	}
}

// GetBrokenTinyExtent returns the first broken extent in the channel.
func (s *ExtentStore) GetBrokenTinyExtent() (extentID uint64, err error) {
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
	extentInfos := make([]*ExtentInfo, 0)
	s.eiMutex.RLock()
	for _, extentInfo := range s.extentInfoMap {
		if extentInfo.FileID <= maxExtentID {
			extentInfos = append(extentInfos, extentInfo)
		}
	}
	s.eiMutex.RUnlock()
	for _, extentInfo := range extentInfos {
		totalSize += extentInfo.TotalSize()
		log.LogDebugf("ExtentStore.StoreSizeExtentID dp %v extentInfo %v totalSize %v", s.partitionID, extentInfo, extentInfo.TotalSize())
	}

	return totalSize
}

// StoreSizeExtentID returns the size of the extent store
func (s *ExtentStore) GetMaxExtentIDAndPartitionSize() (maxExtentID, totalSize uint64) {
	extentInfos := make([]*ExtentInfo, 0)
	s.eiMutex.RLock()
	for _, extentInfo := range s.extentInfoMap {
		extentInfos = append(extentInfos, extentInfo)
	}
	s.eiMutex.RUnlock()
	for _, extentInfo := range extentInfos {
		if extentInfo.FileID > maxExtentID {
			maxExtentID = extentInfo.FileID
		}
		totalSize += extentInfo.TotalSize()
	}
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
		needWriteEmpty := DeleteTinyRecordSize - (stat.Size() % DeleteTinyRecordSize)
		data := make([]byte, needWriteEmpty)
		s.tinyExtentDeleteFp.Write(data)
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

type ExtentDeleted struct {
	ExtentID uint64 `json:"extentID"`
	Offset   uint64 `json:"offset"`
	Size     uint64 `json:"size"`
}

func (s *ExtentStore) GetHasDeleteTinyRecords() (extentDes []ExtentDeleted, err error) {
	data := make([]byte, DeleteTinyRecordSize)
	offset := int64(0)

	for {
		_, err = s.tinyExtentDeleteFp.ReadAt(data, offset)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}

		extent := ExtentDeleted{}
		extent.ExtentID, extent.Offset, extent.Size = UnMarshalTinyExtent(data)
		extentDes = append(extentDes, extent)
		offset += DeleteTinyRecordSize
	}
}

// NextExtentID returns the next extentID. When the client sends the request to create an extent,
// this function generates an unique extentID within the current partition.
// This function can only be called by the leader.
func (s *ExtentStore) NextExtentID() (extentID uint64, err error) {
	extentID = atomic.AddUint64(&s.baseExtentID, 1)
	err = s.PersistenceBaseExtentID(extentID)
	return
}

func (s *ExtentStore) LoadTinyDeleteFileOffset() (offset int64, err error) {
	stat, err := s.tinyExtentDeleteFp.Stat()
	if err == nil {
		offset = stat.Size()
	}
	return
}

func (s *ExtentStore) getExtentKey(extent uint64) string {
	return fmt.Sprintf("extent %v_%v", s.partitionID, extent)
}

// UpdateBaseExtentID updates the base extent ID.
func (s *ExtentStore) UpdateBaseExtentID(id uint64) (err error) {
	if IsTinyExtent(id) {
		return
	}
	if id > atomic.LoadUint64(&s.baseExtentID) {
		atomic.StoreUint64(&s.baseExtentID, id)
		err = s.PersistenceBaseExtentID(atomic.LoadUint64(&s.baseExtentID))
	}
	s.PreAllocSpaceOnVerfiyFile(atomic.LoadUint64(&s.baseExtentID))

	return
}

func (s *ExtentStore) extent(extentID uint64) (e *Extent, err error) {
	if e, err = s.LoadExtentFromDisk(extentID, false); err != nil {
		err = fmt.Errorf("load extent from disk: %v", err)
		return nil, err
	}
	return
}

func (s *ExtentStore) extentWithHeader(ei *ExtentInfo) (e *Extent, err error) {
	var ok bool
	if ei == nil || ei.IsDeleted {
		err = ExtentNotFoundError
		return
	}
	if e, ok = s.cache.Get(ei.FileID); !ok {
		if e, err = s.LoadExtentFromDisk(ei.FileID, true); err != nil {
			err = fmt.Errorf("load  %v from disk: %v", s.getExtentKey(ei.FileID), err)
			return nil, err
		}
	}
	return
}

func (s *ExtentStore) extentWithHeaderByExtentID(extentID uint64) (e *Extent, err error) {
	var ok bool
	if e, ok = s.cache.Get(extentID); !ok {
		if e, err = s.LoadExtentFromDisk(extentID, true); err != nil {
			err = fmt.Errorf("load  %v from disk: %v", s.getExtentKey(extentID), err)
			return nil, err
		}
	}
	return
}

// HasExtent tells if the extent store has the extent with the given ID
func (s *ExtentStore) HasExtent(extentID uint64) (exist bool) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()
	_, exist = s.extentInfoMap[extentID]
	return
}

// GetExtentCount returns the number of extents in the extentInfoMap
func (s *ExtentStore) GetExtentCount() (count int) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()
	return len(s.extentInfoMap)
}

func (s *ExtentStore) LoadExtentFromDisk(extentID uint64, putCache bool) (e *Extent, err error) {
	name := path.Join(s.dataPath, fmt.Sprintf("%v", extentID))
	e = NewExtentInCore(name, extentID)
	if err = e.RestoreFromFS(); err != nil {
		err = fmt.Errorf("restore from file %v putCache %v system: %v", name, putCache, err)
		return
	}

	if !putCache {
		return
	}

	if !IsTinyExtent(extentID) && proto.IsNormalDp(s.partitionType) {
		e.header = make([]byte, util.BlockHeaderSize)
		if _, err = s.verifyExtentFp.ReadAt(e.header, int64(extentID*util.BlockHeaderSize)); err != nil && err != io.EOF {
			return
		}
		emptyHeader := make([]byte, util.BlockHeaderSize)
		log.LogDebugf("LoadExtentFromDisk. partition id %v extentId %v, snapshotOff %v, append fp cnt %v",
			s.partitionID, extentID, e.snapshotDataOff, len(s.verifyExtentFpAppend))
		if e.snapshotDataOff > util.ExtentSize {
			for id, vFp := range s.verifyExtentFpAppend {
				if uint64(id) > (e.snapshotDataOff-util.ExtentSize)/util.ExtentSize {
					log.LogDebugf("LoadExtentFromDisk. partition id %v extentId %v, snapshotOff %v id %v out of extent range",
						s.partitionID, extentID, e.snapshotDataOff, id)
					break
				}
				log.LogDebugf("LoadExtentFromDisk. partition id %v extentId %v, snapshotOff %v id %v", s.partitionID, extentID, e.snapshotDataOff, id)
				header := make([]byte, util.BlockHeaderSize)
				if _, err = vFp.ReadAt(header, int64(extentID*util.BlockHeaderSize)); err != nil && err != io.EOF {
					log.LogDebugf("LoadExtentFromDisk. partition id %v extentId %v, read at %v err %v",
						s.partitionID, extentID, extentID*util.BlockHeaderSize, err)
					return
				}
				if bytes.Equal(emptyHeader, header) {
					log.LogErrorf("LoadExtentFromDisk. partition id %v extent %v hole at id %v", s.partitionID, e, id)
				}
				e.header = append(e.header, header...)
			}
			if len(s.verifyExtentFpAppend) < int(e.snapshotDataOff-1)/util.ExtentSize {
				log.LogErrorf("LoadExtentFromDisk. extent %v need fp %v out of range %v", e, int(e.snapshotDataOff-1)/util.ExtentSize, len(s.verifyExtentFpAppend))
			}
		}
	}

	err = nil
	s.cache.Put(e)

	return
}

func (s *ExtentStore) ScanBlocks(extentID uint64) (bcs []*BlockCrc, err error) {
	if !proto.IsNormalDp(s.partitionType) {
		return
	}

	var blockCnt int
	bcs = make([]*BlockCrc, 0)
	ei := s.extentInfoMap[extentID]
	e, err := s.extentWithHeader(ei)
	if err != nil {
		return bcs, err
	}

	extSize := e.Size()
	if e.snapshotDataOff > util.ExtentSize {
		extSize = int64(e.snapshotDataOff)
	}
	blockCnt = int(extSize / util.BlockSize)

	if e.Size()%util.BlockSize != 0 {
		blockCnt += 1
	}
	for blockNo := 0; blockNo < blockCnt; blockNo++ {
		blockCrc := binary.BigEndian.Uint32(e.header[blockNo*util.PerBlockCrcSize : (blockNo+1)*util.PerBlockCrcSize])
		bcs = append(bcs, &BlockCrc{BlockNo: blockNo, Crc: blockCrc})
	}
	sort.Sort(BlockCrcArr(bcs))

	return
}

type ExtentInfoArr []*ExtentInfo

func (arr ExtentInfoArr) Len() int           { return len(arr) }
func (arr ExtentInfoArr) Less(i, j int) bool { return arr[i].FileID < arr[j].FileID }
func (arr ExtentInfoArr) Swap(i, j int)      { arr[i], arr[j] = arr[j], arr[i] }

func (s *ExtentStore) BackendTask() {
	s.autoComputeExtentCrc()
	s.cleanExpiredNormalExtentDeleteCache()
}

func (s *ExtentStore) cleanExpiredNormalExtentDeleteCache() {
	s.hasDeleteNormalExtentsCache.Range(func(key, value interface{}) bool {
		deleteTime := value.(int64)
		extentID := key.(uint64)
		if time.Now().Unix()-deleteTime > NormalExtentDeleteRetainTime {
			s.hasDeleteNormalExtentsCache.Delete(extentID)
		}
		return true
	})
}

func (s *ExtentStore) autoComputeExtentCrc() {
	if !proto.IsNormalDp(s.partitionType) {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	extentInfos := make([]*ExtentInfo, 0)
	deleteExtents := make([]*ExtentInfo, 0)
	s.eiMutex.RLock()
	for _, ei := range s.extentInfoMap {
		extentInfos = append(extentInfos, ei)
		if ei.IsDeleted && time.Now().Unix()-ei.ModifyTime > UpdateCrcInterval {
			deleteExtents = append(deleteExtents, ei)
		}
	}
	s.eiMutex.RUnlock()

	if len(deleteExtents) > 0 {
		s.eiMutex.Lock()
		for _, ei := range deleteExtents {
			delete(s.extentInfoMap, ei.FileID)
		}
		s.eiMutex.Unlock()
	}

	sort.Sort(ExtentInfoArr(extentInfos))

	for _, ei := range extentInfos {
		s.ApplyIdMutex.RLock()
		if ei == nil {
			s.ApplyIdMutex.RUnlock()
			continue
		}

		if !IsTinyExtent(ei.FileID) && time.Now().Unix()-ei.ModifyTime > UpdateCrcInterval &&
			!ei.IsDeleted && ei.Size > 0 && ei.Crc == 0 {

			e, err := s.extentWithHeader(ei)
			if err != nil {
				log.LogWarnf("[autoComputeExtentCrc] get extent error (%v)", err)
				s.ApplyIdMutex.RUnlock()
				continue
			}

			extentCrc, err := e.autoComputeExtentCrc(s.PersistenceBlockCrc)
			if err != nil {
				log.LogError("[autoComputeExtentCrc] compute crc fail", err)
				s.ApplyIdMutex.RUnlock()
				continue
			}

			ei.UpdateExtentInfo(e, extentCrc)
			ei.ApplyID = s.ApplyId
			time.Sleep(time.Millisecond * 100)
		}
		s.ApplyIdMutex.RUnlock()
	}

	time.Sleep(time.Second)
}

func (s *ExtentStore) TinyExtentRecover(extentID uint64, offset, size int64, data []byte, crc uint32, isEmptyPacket bool) (err error) {
	if !IsTinyExtent(extentID) {
		return fmt.Errorf("extent %v not tinyExtent", extentID)
	}

	var (
		e  *Extent
		ei *ExtentInfo
	)

	s.eiMutex.RLock()
	ei = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if e, err = s.extentWithHeader(ei); err != nil {
		return nil
	}

	if err = e.TinyExtentRecover(data, offset, size, crc, isEmptyPacket); err != nil {
		return err
	}
	ei.UpdateExtentInfo(e, 0)

	return nil
}

func (s *ExtentStore) GetExtentFinfoSize(extentID uint64) (size uint64, err error) {
	var e *Extent
	s.eiMutex.RLock()
	ei := s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if e, err = s.extentWithHeader(ei); err != nil {
		return
	}

	finfo, err := e.file.Stat()
	if err != nil {
		return 0, err
	}
	size = uint64(finfo.Size())

	return
}

func (s *ExtentStore) GetExtentWithHoleAvailableOffset(extentID uint64, offset int64) (newOffset, newEnd int64, err error) {
	var e *Extent
	s.eiMutex.RLock()
	ei := s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if e, err = s.extentWithHeader(ei); err != nil {
		return
	}

	defer func() {
		if err != nil && strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			newOffset = e.dataSize
			newEnd = e.dataSize
			err = nil
		}
	}()
	newOffset, newEnd, err = e.getExtentWithHoleAvailableOffset(offset)
	return
}

func (s *ExtentStore) renameStaleExtentStore() (err error) {
	// create: move current folder to .old and create a new folder
	if _, err = os.Stat(s.dataPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
	}

	curTime := time.Now().Format(StaleExtStoreTimeFormat)
	staleExtStoreDirName := s.dataPath + "_" + curTime + StaleExtStoreBackupSuffix

	if err = os.Rename(s.dataPath, staleExtStoreDirName); err != nil {
		return
	}
	return
}
