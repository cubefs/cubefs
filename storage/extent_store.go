// Copyright 2018 The Chubao Authors.
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
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"path"
	"regexp"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"hash/crc32"
	"io"
	"runtime"
	"sort"
	"strings"
	"syscall"
)

const (
	ExtCrcHeaderFileName     = "EXTENT_CRC"
	ExtBaseExtentIDFileName  = "EXTENT_META"
	TinyDeleteFileOpt        = os.O_CREATE | os.O_RDWR
	TinyExtDeletedFileName   = "TINYEXTENT_DELETE"
	NormalExtDeletedFileName = "NORMALEXTENT_DELETE"
	MaxExtentCount           = 20000
	TinyExtentCount          = 64
	TinyExtentStartID        = 1
	MinExtentID              = 1024
	DeleteTinyRecordSize     = 24
	UpdateCrcInterval        = 600
)

var (
	RegexpExtentFile, _ = regexp.Compile("^(\\d)+$")
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
			return !IsTinyExtent(ei.FileID) && now.Unix()-ei.ModifyTime > UpdateCrcInterval && ei.IsDeleted == false && ei.Size > 0
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
	dataPath                          string
	baseExtentID                      uint64                 // TODO what is baseExtentID
	extentInfoMap                     map[uint64]*ExtentInfo // map that stores all the extent information
	eiMutex                           sync.RWMutex           // mutex for extent info
	cache                             *ExtentCache           // extent cache
	mutex                             sync.Mutex
	storeSize                         int      // size of the extent store
	metadataFp                        *os.File // metadata file pointer?
	tinyExtentDeleteFp                *os.File
	normalExtentDeleteFp              *os.File
	baseTinyDeleteOffset              int64
	closeC                            chan bool
	closed                            bool
	availableTinyExtentC              chan uint64 // available tinyExtent channel
	brokenTinyExtentC                 chan uint64 // broken tinyExtent channel
	blockSize                         int
	partitionID                       uint64
	verifyExtentFp                    *os.File
	hasAllocSpaceExtentIDOnVerfiyFile uint64
}

func MkdirAll(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func NewExtentStore(dataDir string, partitionID uint64, storeSize int) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataPath = dataDir
	s.partitionID = partitionID
	if err = MkdirAll(dataDir); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}
	if s.tinyExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, TinyExtDeletedFileName), TinyDeleteFileOpt, 0666); err != nil {
		return
	}
	finfo, err := s.tinyExtentDeleteFp.Stat()
	if err != nil {
		return
	}
	s.baseTinyDeleteOffset = finfo.Size()
	if s.verifyExtentFp, err = os.OpenFile(path.Join(s.dataPath, ExtCrcHeaderFileName), os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	if s.metadataFp, err = os.OpenFile(path.Join(s.dataPath, ExtBaseExtentIDFileName), os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	if s.normalExtentDeleteFp, err = os.OpenFile(path.Join(s.dataPath, NormalExtDeletedFileName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); err != nil {
		return
	}

	s.extentInfoMap = make(map[uint64]*ExtentInfo, 0)
	s.cache = NewExtentCache(100)
	if err = s.initBaseFileID(); err != nil {
		err = fmt.Errorf("init base field ID: %v", err)
		return
	}
	s.hasAllocSpaceExtentIDOnVerfiyFile = s.GetPreAllocSpaceExtentIDOnVerfiyFile()
	s.storeSize = storeSize
	s.closeC = make(chan bool, 1)
	s.closed = false
	err = s.initTinyExtent()
	if err != nil {
		return
	}
	go s.loopAutoComputeExtentCrc()
	return
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

// SnapShot returns the information of all the extents on the current data partition.
// When the master sends the loadDataPartition request, the snapshot is used to compare the replicas.
func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var (
		normalExtentSnapshot, tinyExtentSnapshot []*ExtentInfo
	)

	if normalExtentSnapshot, _, err = s.GetAllWatermarks(NormalExtentFilter()); err != nil {
		return
	}

	files = make([]*proto.File, 0, len(normalExtentSnapshot))
	for _, ei := range normalExtentSnapshot {
		file := GetSnapShotFileFromPool()
		file.Name = strconv.FormatUint(ei.FileID, 10)
		file.Size = uint32(ei.Size)
		file.Modified = ei.ModifyTime
		file.Crc = atomic.LoadUint32(&ei.Crc)
		files = append(files, file)
	}
	tinyExtentSnapshot = s.getTinyExtentInfo()
	for _, ei := range tinyExtentSnapshot {
		file := GetSnapShotFileFromPool()
		file.Name = strconv.FormatUint(ei.FileID, 10)
		file.Size = uint32(ei.Size)
		file.Modified = ei.ModifyTime
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
	s.eiMutex.Lock()
	s.extentInfoMap[extentID] = extInfo
	s.eiMutex.Unlock()

	s.UpdateBaseExtentID(extentID)
	return
}

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
		if e, loadErr = s.extent(extentID); loadErr != nil {
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
	atomic.StoreUint64(&s.baseExtentID, baseFileID)
	log.LogInfof("datadir(%v) maxBaseId(%v)", s.dataPath, baseFileID)
	runtime.GC()
	return nil
}

// Write writes the given extent to the disk.
func (s *ExtentStore) Write(extentID uint64, offset, size int64, data []byte, crc uint32, isUpdateSize bool, isSync bool) (err error) {
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
	err = e.Write(data, offset, size, crc, isUpdateSize, isSync, s.PersistenceBlockCrc, ei)
	if err != nil {
		return err
	}
	ei.UpdateExtentInfo(e, 0)

	return nil
}

func (s *ExtentStore) checkOffsetAndSize(extentID uint64, offset, size int64) error {
	if IsTinyExtent(extentID) {
		return nil
	}
	if offset+size > util.BlockSize*util.BlockCount {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= util.BlockCount*util.BlockSize || size == 0 {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}

	if size > util.BlockSize {
		return NewParameterMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
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
	if e, err = s.extentWithHeader(ei); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return
	}
	crc, err = e.Read(nbuf, offset, size, isRepairRead)

	return
}

func (s *ExtentStore) tinyDelete(e *Extent, offset, size, tinyDeleteFileOffset int64) (err error) {
	if offset+size > e.dataSize {
		return
	}
	var (
		hasDelete bool
	)
	if hasDelete,err = e.DeleteTiny(offset, size); err != nil {
		return
	}
	if hasDelete {
		return
	}
	if err = s.RecordTinyDelete(e.extentID, offset, size, tinyDeleteFileOffset); err != nil {
		return
	}
	return
}

// MarkDelete marks the given extent as deleted.
func (s *ExtentStore) MarkDelete(extentID uint64, offset, size, tinyDeleteFileOffset int64) (err error) {
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

	if IsTinyExtent(extentID) {
		return s.tinyDelete(e, offset, size, tinyDeleteFileOffset)
	}
	e.Close()
	s.cache.Del(extentID)
	extentFilePath := path.Join(s.dataPath, strconv.FormatUint(extentID, 10))
	if err = os.Remove(extentFilePath); err != nil {
		return
	}
	s.PersistenceHasDeleteExtent(extentID)
	ei.IsDeleted = true
	ei.ModifyTime = time.Now().Unix()
	s.cache.Del(e.extentID)
	s.DeleteBlockCrc(extentID)

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
	s.closed = true
}

// Watermark returns the extent info of the given extent on the record.
func (s *ExtentStore) Watermark(extentID uint64) (ei *ExtentInfo, err error) {
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

// GetTinyExtentOffset returns the offset of the given extent.
func (s *ExtentStore) GetTinyExtentOffset(extentID uint64) (watermark int64, err error) {
	einfo, err := s.Watermark(extentID)
	if err != nil {
		return
	}
	watermark = int64(einfo.Size)
	if watermark%PageSize != 0 {
		watermark = watermark + (PageSize - watermark%PageSize)
	}

	return
}

// GetAllWatermarks returns all the watermarks.
func (s *ExtentStore) GetAllWatermarks(filter ExtentFilter) (extents []*ExtentInfo, tinyDeleteFileSize int64, err error) {
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
	tinyDeleteFileSize = s.LoadTinyDeleteFileOffset()

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
	s.availableTinyExtentC = make(chan uint64, TinyExtentCount)
	s.brokenTinyExtentC = make(chan uint64, TinyExtentCount)
	var extentID uint64

	for extentID = TinyExtentStartID; extentID < TinyExtentStartID+TinyExtentCount; extentID++ {
		err = s.Create(extentID)
		if err == nil || strings.Contains(err.Error(), syscall.EEXIST.Error()) || err == ExtentExistsError {
			err = nil
			s.brokenTinyExtentC <- extentID
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
		return
	default:
		return 0, NoAvailableExtentError

	}
}

// SendToAvailableTinyExtentC sends the extent to the channel that stores the available tiny extents.
func (s *ExtentStore) SendToAvailableTinyExtentC(extentID uint64) {
	s.availableTinyExtentC <- extentID
}

// SendAllToBrokenTinyExtentC sends all the extents to the channel that stores the broken extents.
func (s *ExtentStore) SendAllToBrokenTinyExtentC(extentIds []uint64) {
	for _, extentID := range extentIds {
		s.brokenTinyExtentC <- extentID
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
	s.brokenTinyExtentC <- extentID
}

// GetBrokenTinyExtent returns the first broken extent in the channel.
func (s *ExtentStore) GetBrokenTinyExtent() (extentID uint64, err error) {
	select {
	case extentID = <-s.brokenTinyExtentC:
		return
	default:
		return 0, NoBrokenExtentError

	}
}

// StoreSize returns the size of the extent store
func (s *ExtentStore) StoreSize() (totalSize uint64) {
	extentInfos := make([]*ExtentInfo, 0)
	s.eiMutex.RLock()
	for _, extentInfo := range s.extentInfoMap {
		extentInfos = append(extentInfos, extentInfo)
	}
	s.eiMutex.RUnlock()
	for _, extentInfo := range extentInfos {
		totalSize += extentInfo.Size
	}

	return totalSize
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

func (s *ExtentStore) RecordTinyDelete(extentID uint64, offset, size, tinyDeleteFileOffset int64) (err error) {
	record := MarshalTinyExtent(extentID, offset, size)
	_, err = s.tinyExtentDeleteFp.WriteAt(record, tinyDeleteFileOffset)
	if err != nil {
		return
	}
	if atomic.LoadInt64(&s.baseTinyDeleteOffset) < tinyDeleteFileOffset {
		atomic.StoreInt64(&s.baseTinyDeleteOffset, tinyDeleteFileOffset)
	}

	return
}

func (s *ExtentStore) ReadTinyDeleteRecords(offset, size int64, data []byte) (crc uint32, err error) {
	_, err = s.tinyExtentDeleteFp.ReadAt(data[:size], offset)
	if err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data[:size])
	return
}

// NextExtentID returns the next extentID. When the client sends the request to create an extent,
// this function generates an unique extentID within the current partition.
// This function can only be called by the leader.
func (s *ExtentStore) NextExtentID() (extentID uint64,err error) {
	extentID=atomic.AddUint64(&s.baseExtentID, 1)
	err=s.PersistenceBaseExtentID(extentID)
	return
}

func (s *ExtentStore) NextTinyDeleteFileOffset() (offset int64) {
	return atomic.AddInt64(&s.baseTinyDeleteOffset, DeleteTinyRecordSize)
}

func (s *ExtentStore) LoadTinyDeleteFileOffset() (offset int64) {
	return atomic.LoadInt64(&s.baseTinyDeleteOffset)
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
	if !IsTinyExtent(extentID) {
		e.header = make([]byte, util.BlockHeaderSize)
		if _, err = s.verifyExtentFp.ReadAt(e.header, int64(extentID*util.BlockHeaderSize)); err != nil && err != io.EOF {
			return
		}
	}
	err = nil
	s.cache.Put(e)

	return
}

func (s *ExtentStore) loopAutoComputeExtentCrc() {
	go func() {
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-s.closeC:
				return
			case <-ticker.C:
				s.autoComputeExtentCrc()
			}
		}
	}()
}

func (s *ExtentStore) ScanBlocks(extentID uint64) (bcs []*BlockCrc, err error) {
	var blockCnt int
	bcs = make([]*BlockCrc, 0)
	ei := s.extentInfoMap[extentID]
	e, err := s.extentWithHeader(ei)
	if err != nil {
		return bcs, err
	}
	blockCnt = int(e.Size() / util.BlockSize)
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

func (s *ExtentStore) autoComputeExtentCrc() {
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
		if ei == nil {
			continue
		}
		if !IsTinyExtent(ei.FileID) && time.Now().Unix()-ei.ModifyTime > UpdateCrcInterval && ei.IsDeleted == false && ei.Size > 0 {
			if atomic.LoadUint32(&ei.Crc) == 0 {
				continue
			}
			e, err := s.extentWithHeader(ei)
			if err != nil {
				continue
			}
			extentCrc, err := e.autoComputeExtentCrc(s.PersistenceBlockCrc)
			if err != nil {
				continue
			}
			ei.UpdateExtentInfo(e, extentCrc)
		}
		time.Sleep(time.Millisecond * 100)
	}

}
