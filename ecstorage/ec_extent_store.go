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

package ecstorage

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"hash/crc32"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"path"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

const ( //rocksdb persist str
	baseMapTable             = uint64(iota)
	OriginExtentSizeMapTable //key: +partitionId+extentId                       value: extent size
	TinyDeleteMapTable       //key: +partitionId+hostIndex+extentId+offset   value: delete info
	OriginTinyDeleteMapTable //key: +partitionId+extentId                    value: origin holes info
	CrcMapTable              //key: +partitionId+extentId+blockNum           value: crc and size
)

const (
	BaseDeleteMark = uint32(iota)
	TinyDeleteMark
	TinyDeleting
	TinyDeleted
)

type EcTinyDeleteRecord struct {
	ExtentID     uint64 `json:"extent_id"`
	DeleteStatus uint32 `json:"delete_status"`
	HostIndex    uint32 `json:"host_index"`
	Offset       uint64 `json:"offset"`
	Size         uint64 `json:"size"`
}

const (
	ExtBaseExtentIDFileName  = "EXTENT_META"
	NormalExtDeletedFileName = "NORMALEXTENT_DELETE"
	MaxExtentCount           = 20000
	TinyExtentCount          = 64
	TinyExtentStartID        = 1
	MinExtentID              = 1024
	UpdateCrcInterval        = 600
	UpdateEcCrcInterval      = 300
	RandomWriteType          = 2
	AppendWriteType          = 1
)

var (
	RegexpExtentFile, _ = regexp.Compile("^(\\d)+$")
)

type ExtentFilter func(info *ExtentInfo) bool

// ExtentStore defines fields used in the storage engine.
// Packets smaller than 128K are stored in the "tinyExtent", a place to persist the small files.
// packets larger than or equal to 128K are stored in the normal "extent", a place to persist large files.
// The difference between them is that the extentID of a tinyExtent starts at 5000000 and ends at 5000128.
// Multiple small files can be appended to the same tinyExtent.
// In addition, the deletion of small files is implemented by the punch hole from the underlying file system.
type ExtentStore struct {
	dataPath                          string
	baseExtentID                      uint64                 // TODO what is baseExtentID
	extentInfoMap                     map[uint64]*ExtentInfo // map that stores all the extent information
	eiMutex                           sync.RWMutex           // mutex for extent info
	cache                             *ExtentCache           // extent cache
	mutex                             sync.Mutex
	storeSize                         int      // size of the extent store
	metadataFp                        *os.File // metadata file pointer?
	normalExtentDeleteFp              *os.File
	closeC                            chan bool
	closed                            bool
	blockSize                         int
	partitionID                       uint64
	ecDb                              *EcRocksDbInfo
	hasAllocSpaceExtentIDOnVerfiyFile uint64
}

func (s *ExtentStore) SetEcDb(db *EcRocksDbInfo) {
	s.ecDb = db

}

func MkdirAll(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func (ei *ExtentInfo) UpdateExtentInfo(extent *Extent, crc uint32) {
	extent.Lock()
	defer extent.Unlock()
	if time.Now().Unix()-extent.ModifyTime() <= UpdateCrcInterval {
		crc = 0
	}
	ei.Size = uint64(extent.dataSize)
	if !IsTinyExtent(ei.FileID) {
		atomic.StoreUint32(&ei.Crc, crc)
		ei.ModifyTime = extent.ModifyTime()
	}
}

const (
	BaseExtentAddNumOnInitExtentStore = 1000
)

func (s *ExtentStore) initBaseFileID() (err error) {
	var (
		baseFileID uint64
	)
	baseFileID, _ = s.GetPersistenceBaseExtentID()
	files, err := ioutil.ReadDir(s.dataPath)
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
		if e, loadErr = s.initExtentInfo(extentID, f); loadErr != nil {
			continue
		}
		ei = &ExtentInfo{FileID: extentID}
		ei.UpdateExtentInfo(e, 0)

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
	baseFileID += BaseExtentAddNumOnInitExtentStore
	atomic.StoreUint64(&s.baseExtentID, baseFileID)
	log.LogInfof("datadir(%v) maxBaseId(%v)", s.dataPath, baseFileID)
	runtime.GC()
	return nil
}

var (
	EcExtentFilter = func() ExtentFilter {
		return func(ei *ExtentInfo) bool {
			return ei.IsDeleted == false
		}
	}
)

func NewEcExtentStore(dataDir string, partitionID uint64, storeSize int, cacheCapacity int, ln CacheListener) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataPath = dataDir
	s.partitionID = partitionID
	if err = MkdirAll(dataDir); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}
	if s.metadataFp, err = os.OpenFile(path.Join(s.dataPath, ExtBaseExtentIDFileName), os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	if s.normalExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, NormalExtDeletedFileName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); err != nil {
		return
	}

	s.extentInfoMap = make(map[uint64]*ExtentInfo, 0)
	s.cache = NewExtentCache(cacheCapacity, time.Minute*5, ln)
	log.LogInfof("NewExtentStore partitionID[%v]", partitionID)
	if err = s.initBaseFileID(); err != nil {
		err = fmt.Errorf("init base field ID: %v", err)
		return
	}
	s.hasAllocSpaceExtentIDOnVerfiyFile = s.GetPreAllocSpaceExtentIDOnVerfiyFile()
	s.storeSize = storeSize
	s.closeC = make(chan bool, 1)
	s.closed = false
	return
}

func (ei *ExtentInfo) UpdateEcExtentInfo(extent *Extent, crc uint32) {
	extent.Lock()
	defer extent.Unlock()
	ei.Size = uint64(extent.dataSize)
	atomic.StoreUint32(&ei.Crc, crc)
	ei.ModifyTime = extent.ModifyTime()
}

// EcCreate creates an extent.
func (s *ExtentStore) EcCreate(extentID, originExtentSize uint64, putCache bool) (err error) {
	var e *Extent
	name := path.Join(s.dataPath, strconv.Itoa(int(extentID)))
	e = NewExtentInCore(name, extentID)
	err = e.InitToFS()
	if err != nil {
		return
	}
	if putCache {
		s.cache.Put(e)
	} else {
		defer func() {
			_ = e.Close()
		}()
	}
	extInfo := &ExtentInfo{FileID: extentID}
	extInfo.OriginExtentSize = originExtentSize
	extInfo.UpdateEcExtentInfo(e, 0)
	s.eiMutex.Lock()
	s.extentInfoMap[extentID] = extInfo
	s.eiMutex.Unlock()

	s.UpdateBaseExtentID(extentID)
	return
}

// EcWrite writes the given extent to the disk.
func (s *ExtentStore) EcWrite(extentID uint64, offset, size int64, data []byte, crc uint32, writeType int, isSync bool) (err error) {
	var (
		e  *Extent
		ei *ExtentInfo
	)
	s.eiMutex.RLock()
	ei, _ = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	e, err = s.extentWithHeader(ei)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return err
	}
	err = e.EcWrite(data, offset, size, writeType, isSync)
	if err != nil {
		return err
	}
	err = s.EcUpdateCrc(s.partitionID, extentID, offset, size, crc, s.ecDb.PersistenceEcBlockCrc)
	if err != nil {
		return
	}
	ei.UpdateEcExtentInfo(e, 0)
	return
}

func (s *ExtentStore) EcUpdateCrc(partitionId, extentId uint64, offset, size int64, crc uint32, crcFunc EcUpdateCrcFunc) (err error) {
	var exist bool
	blockSize := uint32(0)
	blockNo := uint64(offset / unit.EcBlockSize)
	offsetInBlock := offset % unit.EcBlockSize
	_, blockSize, exist, err = s.ecDb.GetEcBlockCrcInfo(partitionId, extentId, blockNo)
	if err != nil {
		return
	}

	log.LogDebugf("EcUpdateCrc offset(%v) size(%v) blockSize(%v)",
		offset, size, blockSize)
	if !exist {
		err = crcFunc(partitionId, extentId, blockNo, uint32(size), crc)
		return
	}

	if offsetInBlock == 0 && size == int64(blockSize) { //randomWrite all block
		err = crcFunc(partitionId, extentId, blockNo, uint32(size), crc)
		return
	}

	//delete tinyExtent update parityNode data trigger
	if offsetInBlock+size <= unit.EcBlockSize {
		err = crcFunc(partitionId, extentId, blockNo, 0, 0)
		return
	}

	if err = crcFunc(partitionId, extentId, blockNo, 0, 0); err == nil {
		err = crcFunc(partitionId, extentId, blockNo+1, 0, 0)
	}
	return
}

func (s *ExtentStore) computeEcExtentCrc(partitionId, extentId, offset, size uint64, deleteTiny bool, crcFunc EcUpdateCrcFunc) (err error) {
	var (
		blockSize uint32
		blockCrc  uint32
		exist     bool
	)
	blockStartNo := offset / unit.EcBlockSize
	endIndex := offset + size
	blockEndNo := (offset + size) / unit.EcBlockSize
	if endIndex%unit.EcBlockSize != 0 {
		blockEndNo += 1
	}

	readN := 0
	bdata := make([]byte, unit.EcBlockSize)
	e, err := s.extentWithHeaderByExtentID(extentId)
	if err != nil {
		return
	}

	for blockNo := blockStartNo; blockNo < blockEndNo; blockNo++ {
		blockCrc, blockSize, exist, err = s.ecDb.GetEcBlockCrcInfo(partitionId, extentId, blockNo)
		if err != nil || !exist {
			log.LogError(err)
			continue
		}
		if deleteTiny {
			log.LogDebugf("computeEcExtentCrc blockNo(%v) blockSize(%v)", blockNo, blockSize)
			err = crcFunc(partitionId, extentId, blockNo, blockSize, 0)
			if err != nil {
				log.LogError(err)
			}
			continue
		}

		if blockCrc == 0 { //tiny block need update crc
			blockOffset := int64(blockNo * unit.EcBlockSize)
			_, blockCrc, err = e.EcReadTiny(bdata[:blockSize], int64(blockSize), blockOffset, true)
			log.LogDebugf("computeEcExtentCrc extentId(%v) blockNo(%v) newCrc(%v)", e.extentID, blockNo, blockCrc)
			if err != nil {
				log.LogErrorf("computeEcExtentCrc read err(%v)", readN, err)
				continue
			}
			err = crcFunc(partitionId, extentId, blockNo, blockSize, blockCrc)
			if err != nil {
				log.LogError(err)
			}
		}
	}
	return
}

// DeleteEcTiny deletes a tiny extent.
func (e *Extent) DeleteEcTiny(offset, size int64) (hasDelete bool, err error) {
	if int(offset)%PageSize != 0 {
		log.LogErrorf("extentId(%v) DeleteTiny offset(%v)", e.extentID, offset)
		return false, ParameterMismatchError
	}

	if int(size)%PageSize != 0 {
		size += int64(PageSize - int(size)%PageSize)
	}
	if int(size)%PageSize != 0 {
		log.LogErrorf("DeleteTiny size(%v)", size)
		return false, ParameterMismatchError
	}

	newOffset, err := e.file.Seek(offset, SEEK_DATA)
	if err != nil {
		if strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			return true, nil
		}
		return false, err
	}
	if newOffset-offset >= size {
		hasDelete = true
		return true, nil
	}
	err = fallocate(int(e.file.Fd()), FallocFLPunchHole|FallocFLKeepSize, offset, size)
	return
}

func (s *ExtentStore) EcTinyDelete(e *Extent, offset, size int64) (hasDelete bool, err error) {
	if offset+size > e.dataSize {
		log.LogInfof("partition(%v) extentId(%v) offset(%v) size(%v) dataSize(%v)",
			s.partitionID, e.extentID, offset, size, e.dataSize)
		return
	}
	if hasDelete, err = e.DeleteEcTiny(offset, size); err != nil {
		return
	}
	if hasDelete {
		return
	}

	atomic.StoreInt64(&e.modifyTime, time.Now().Unix())
	err = s.computeEcExtentCrc(s.partitionID, e.extentID, uint64(offset), uint64(size), true, s.ecDb.PersistenceEcBlockCrc)
	return
}

func (s *ExtentStore) EcDelTinyExtentFile(ei *ExtentInfo, extentID uint64) (err error) {
	log.LogDebugf("EcDelTinyExtentFile extentId(%v) ", extentID)
	err = s.ecDb.DeleteExtentMeta(s.partitionID, extentID)
	if err != nil {
		return
	}
	s.cache.Del(extentID)
	extentFilePath := path.Join(s.dataPath, strconv.FormatUint(extentID, 10))
	if err = os.Remove(extentFilePath); err != nil {
		return
	}
	ei.IsDeleted = true
	ei.ModifyTime = time.Now().Unix()

	s.eiMutex.Lock()
	delete(s.extentInfoMap, extentID)
	s.eiMutex.Unlock()

	return
}

func (s *ExtentStore) TinyExtentCanDelete(extentID, offset, size uint64) (err error) {
	s.eiMutex.RLock()
	ei := s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if ei == nil || ei.IsDeleted {
		err = ExtentNotFoundError
		return
	}

	e, err := s.extentWithHeader(ei)
	if err != nil {
		return
	}
	if int64(offset)+int64(size) > e.dataSize {
		err = errors.NewErrorf("TinyDeleteCanDelete offset(%v) + size(%v) > e.dataSize()%v",
			offset, size, e.dataSize)
		return
	}
	return
}

// EcMarkDelete marks the given extent as deleted.
func (s *ExtentStore) EcMarkDelete(extentID uint64, hostIndex uint32, offset, size int64) (err error) {
	var (
		ei        *ExtentInfo
		e         *Extent
		hasDelete bool
	)
	log.LogDebugf("delete partition(%v) extent(%v) offset(%v) size(%v)",
		s.partitionID, extentID, offset, size)
	s.eiMutex.RLock()
	ei = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if ei == nil || ei.IsDeleted {
		err = ExtentNotFoundError
		return
	}

	if IsTinyExtent(extentID) {
		if offset == 0 && size == 0 { //migrate failed retry need delete tiny extent
			return s.EcDelTinyExtentFile(ei, extentID)
		}
		if e, err = s.extentWithHeader(ei); err != nil {
			return
		}
		hasDelete, err = s.EcTinyDelete(e, offset, size)
		if err == nil && !hasDelete {
			ei.ModifyTime = time.Now().Unix()
		}
		return
	}
	if e, err = s.extentWithHeader(ei); err != nil {
		return
	}

	s.ecDb.DeleteExtentMeta(s.partitionID, ei.FileID)
	s.cache.Del(extentID)
	extentFilePath := path.Join(s.dataPath, strconv.FormatUint(extentID, 10))
	if err = os.Remove(extentFilePath); err != nil {
		return
	}
	s.PersistenceHasDeleteExtent(extentID)
	ei.IsDeleted = true
	ei.ModifyTime = time.Now().Unix()

	s.eiMutex.Lock()
	delete(s.extentInfoMap, extentID)
	s.eiMutex.Unlock()

	return
}

func (s *ExtentStore) IsExtentNotExistErr(errMsg string) bool {
	return strings.Contains(errMsg, ExtentNotFoundError.Error())
}

func (s *ExtentStore) EcRead(extentID uint64, offset, size int64, data []byte, isRepairRead bool) (dataSize int, crc uint32, err error) {
	var e *Extent
	s.eiMutex.RLock()
	ei := s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	e, err = s.extentWithHeader(ei)
	if err != nil {
		return
	}
	dataSize, crc, err = e.EcRead(data, size, offset, isRepairRead)
	return
}

func (s *ExtentStore) checkOffsetAndSize(extentID uint64, offset, size int64) error {
	if IsTinyExtent(extentID) {
		return nil
	}
	if offset+size > unit.BlockSize*unit.BlockCount {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= unit.BlockCount*unit.BlockSize || size == 0 {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	return nil
}

// IsTinyExtent checks if the given extent is tiny extent.
func IsTinyExtent(extentID uint64) bool {
	return extentID >= TinyExtentStartID && extentID < TinyExtentStartID+TinyExtentCount
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
	s.normalExtentDeleteFp.Sync()
	s.normalExtentDeleteFp.Close()
	s.closed = true
}

// EcWatermark returns the extent info of the given extent on the record.
func (s *ExtentStore) EcWatermark(extentID uint64) (ei *ExtentInfo, err error) {
	var (
		has bool
	)
	s.eiMutex.RLock()
	ei, has = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if !has {
		err = fmt.Errorf("e %v not exist", s.getExtentKey(extentID))
		return
	}
	return
}

// GetAllWatermarks returns all the watermarks.
func (s *ExtentStore) GetAllWatermarks(filter ExtentFilter) (extents []*ExtentInfo, tinyDeleteCount int64, err error) {
	extents = make([]*ExtentInfo, 0)
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
	tinyDeleteCount, err = s.ecDb.getEcTinyDeleteCount(s.partitionID)
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
		totalSize += extentInfo.Size
	}

	return totalSize
}

// GetExtentSize returns the size of the extent used
func (s *ExtentStore) GetExtentSize(extentID uint64) (extentSize uint64) {
	extentInfos := make([]*ExtentInfo, 0)
	s.eiMutex.RLock()
	for _, extentInfo := range s.extentInfoMap {
		extentInfos = append(extentInfos, extentInfo)
	}
	s.eiMutex.RUnlock()
	for _, extentInfo := range extentInfos {
		if extentInfo.FileID == extentID {
			extentSize = extentInfo.Size
			return extentSize
		}
	}
	return extentSize
}

// GetMaxExtentIDAndPartitionSize returns the size of the extent store
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
		totalSize += extentInfo.Size
	}

	return maxExtentID, totalSize
}

// NextExtentID returns the next extentID. When the client sends the request to create an extent,
// this function generates an unique extentID within the current partition.
// This function can only be called by the leader.
func (s *ExtentStore) NextExtentID() (extentID uint64, err error) {
	extentID = atomic.AddUint64(&s.baseExtentID, 1)
	err = s.PersistenceBaseExtentID(extentID)
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
	//	s.PreAllocSpaceOnVerfiyFile(atomic.LoadUint64(&s.baseExtentID))

	return
}

func (s *ExtentStore) extent(extentID uint64) (e *Extent, err error) {
	if e, err = s.loadExtentFromDisk(extentID, false); err != nil {
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
		if e, err = s.loadExtentFromDisk(ei.FileID, true); err != nil {
			err = fmt.Errorf("load  %v from disk: %v", s.getExtentKey(ei.FileID), err)
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

func (s *ExtentStore) loadExtentFromDisk(extentID uint64, putCache bool) (e *Extent, err error) {
	name := path.Join(s.dataPath, strconv.Itoa(int(extentID)))
	e = NewExtentInCore(name, extentID)
	if err = e.RestoreFromFS(); err != nil {
		err = fmt.Errorf("restore from file %v putCache %v system: %v", name, putCache, err)
		return
	}
	if !putCache {
		return
	}
	s.cache.Put(e)
	return
}

func (s *ExtentStore) initExtentInfo(extentID uint64, f os.FileInfo) (e *Extent, err error) {
	name := path.Join(s.dataPath, strconv.Itoa(int(extentID)))
	e = NewExtentInCore(name, extentID)
	if err = e.getStatFromFS(f); err != nil {
		err = fmt.Errorf("restore from file %v system: %v", name, err)
		return
	}

	return
}

func (s *ExtentStore) EvictExpiredCache() {
	s.cache.EvictExpired()
}

func (s *ExtentStore) ForceEvictCache(ratio Ratio) {
	s.cache.ForceEvict(ratio)
}

func (s *ExtentStore) GetExtentCrcResponse(extentId, stripeCount, stripeUnitSize, stripeUnitFileSize uint64, crc uint32) (crcResp proto.ExtentCrcResponse, err error) {
	if stripeUnitFileSize == 0 {
		crcResp.CRC = crc
		crcResp.FinishRead = true
		return
	}
	var (
		curReadSize uint32
		offset      = (stripeCount - 1) * stripeUnitSize
	)
	if stripeUnitFileSize > stripeCount*stripeUnitSize {
		curReadSize = uint32(stripeUnitSize)
	} else {
		curReadSize = uint32(stripeUnitFileSize - (stripeCount-1)*stripeUnitSize)
	}
	data := make([]byte, curReadSize)
	log.LogDebugf("GetExtentCrcResponse stripeUnitSize(%v) offset(%v) curReadSize(%v) stripeUnitFileSize(%v)",
		stripeUnitSize, offset, curReadSize, stripeUnitFileSize)
	realReadSize, _, err := s.EcRead(extentId, int64(offset), int64(curReadSize), data, false)
	if err != nil || realReadSize != int(curReadSize) {
		return
	}

	log.LogDebugf("GetExtentCrcResponse stripeUnitSize(%v) offset(%v) curReadSize(%v) realReadSize(%v)",
		stripeUnitSize, offset, curReadSize, realReadSize)
	crcResp.CRC = crc32.Update(crc, crc32.IEEETable, data)
	crcResp.FinishRead = realReadSize < int(stripeUnitSize)
	return
}

func (s *ExtentStore) GetEcTinyDeleteCount() (count int64, err error) {
	return s.ecDb.getEcTinyDeleteCount(s.partitionID)
}

func (s *ExtentStore) GetAllEcTinyDeleteRecord() (tinyDeleteRecords []*EcTinyDeleteRecord, err error) {
	tinyDeleteRecords = make([]*EcTinyDeleteRecord, 0)
	err = s.ecDb.tinyDelInfoRange(s.partitionID, BaseDeleteMark, func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
		log.LogDebugf("GetAllEcTinyDeleteRecord hostIndex(%v) extentId(%v) offset(%v) size(%v) deleteStatus(%v)",
			hostIndex, extentId, offset, size, deleteStatus)
		recordInfo := EcTinyDeleteRecord{
			ExtentID:     extentId,
			DeleteStatus: deleteStatus,
			HostIndex:    hostIndex,
			Offset:       offset,
			Size:         size,
		}
		tinyDeleteRecords = append(tinyDeleteRecords, &recordInfo)
		return
	})
	return
}

func (s *ExtentStore) EcRecordTinyDelete(extentID, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
	return s.ecDb.ecRecordTinyDelete(s.partitionID, extentID, offset, size, deleteStatus, hostIndex)
}

func (s *ExtentStore) PersistMapInfo(mapTable, extentId uint64, data []byte) (err error) {
	err = s.ecDb.persistMapInfo(mapTable, s.partitionID, extentId, data)
	return
}

func (s *ExtentStore) DeleteMapInfo(mapTable, extentId uint64) (err error) {
	err = s.ecDb.deleteMapInfo(mapTable, s.partitionID, extentId)
	return
}

func (s *ExtentStore) SetOriginExtentSize(extentId, originExtentSize uint64) (err error) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()
	ei, ok := s.extentInfoMap[extentId]
	if !ok {
		err = ExtentNotFoundError
		return
	}
	ei.OriginExtentSize = originExtentSize
	return
}

func (s *ExtentStore) needComputeEcExtentCrc(hosts []string, localHost string, hostIndex int, extentId uint64, dataNum, parityNum uint32) bool {
	e, err := s.extentWithHeaderByExtentID(extentId)
	if err != nil {
		return false
	}
	if time.Now().Unix()-e.modifyTime < UpdateEcCrcInterval {
		modifyTime := time.Unix(e.modifyTime, 0).Format("2006-01-02 15:04:05")
		log.LogDebugf("needComputeEcExtentCrc extent(%v) modifyTime(%v)", e.extentID, modifyTime)
		return false
	}
	nodeIndex, exist := proto.GetEcNodeIndex(localHost, hosts)
	if !exist {
		return false
	}
	if proto.IsEcParityNode(hosts, localHost, e.extentID, uint64(dataNum), uint64(parityNum)) {
		return true
	}
	if hostIndex == nodeIndex {
		return true
	}
	return false
}

func (s *ExtentStore) AutoComputeEcExtentCrc(hosts []string, localHost string, dataNum, parityNum uint32) {
	err := s.TinyDelInfoRange(TinyDeleted, func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
		if s.needComputeEcExtentCrc(hosts, localHost, int(hostIndex), extentId, dataNum, parityNum) {
			err = s.computeEcExtentCrc(s.partitionID, extentId, offset, size, false, s.ecDb.PersistenceEcBlockCrc)
			if err != nil {
				log.LogWarnf("computeEcExtentCrc err(%v)", err)
			}
		}
		return
	})
	if err != nil {
		log.LogWarn(err)
	}
}

func (s *ExtentStore) PersistMapRange(mapTable uint64, cb func(extentId uint64, v []byte) (err error)) (err error) {
	err = s.ecDb.persistMapRange(mapTable, s.partitionID, cb)
	return
}

func (s *ExtentStore) TinyDelInfoRange(deleteStatus uint32, cb func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error)) (err error) {
	err = s.ecDb.tinyDelInfoRange(s.partitionID, deleteStatus, cb)
	return
}

func (s *ExtentStore) TinyDelInfoExtentIdRange(extentId uint64, deleteStatus uint32, cb func(offset, size uint64, hostIndex uint32) (err error)) (err error) {
	err = s.ecDb.tinyDelInfoExtentIdRange(s.partitionID, extentId, deleteStatus, cb)
	return
}

func (s *ExtentStore) PersistBlockCrcRange(extentId uint64, cb func(blockNum uint64, BlockCrc, blockSize uint32) (err error)) (err error) {
	err = s.ecDb.persistBlockCrcRange(s.partitionID, extentId, cb)
	return
}
