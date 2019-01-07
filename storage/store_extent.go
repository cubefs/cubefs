// Copyright 2018 The Container File System Authors.
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
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"path"
	"regexp"
	"time"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"syscall"
)


// TODO should we name this file as "extent_store.go" instead of "store_extent.go" ?

const (

	// TODO what does ext mean in the following names?
	ExtMetaFileName        = "EXTENT_META"  // TODO we should use "metadata" or "md" instead of "meta" for naming
	ExtCrcHeaderFileName   = "EXTENT_CRC"
	ExtMetaFileOpt         = os.O_CREATE | os.O_RDWR
	ExtDeleteFileName      = "EXTENT_DELETE"
	ExtDeleteFileOpt       = os.O_CREATE | os.O_RDWR | os.O_APPEND
	ExtMetaBaseIDOffset    = 0
	ExtMetaBaseIDSize      = 8
	ExtMetaDeleteIdxOffset = 8
	ExtMetaDeleteIdxSize   = 8
	ExtMetaFileSize        = ExtMetaBaseIDSize + ExtMetaDeleteIdxSize // TODO explain
	MaxExtentId            = 40000
	ExtCrcHeaderSize       = MaxExtentId * util.BlockHeaderSize
	TinyExtentCount        = 64 // TODO total number of tiny extents that each partition can have ?
	TinyExtentStartID      = 50000000
	MinExtentID            = 2
)

var (
	RegexpExtentFile, _ = regexp.Compile("^(\\d)+$")
)

type ExtentFilter func(info *ExtentInfo) bool

// Filters
var (
	NormalExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(extent *ExtentInfo) bool {
			return !IsTinyExtent(extent.FileID) && now.Unix() - extent.ModifyTime.Unix() > 10 * 60 && extent.IsDeleted == false && extent.Size > 0
		}
	}

	TinyExtentFilter = func(filters []uint64) ExtentFilter {
		return func(extent *ExtentInfo) bool {
			if !IsTinyExtent(extent.FileID) {
				return false
			}
			for _, filterID := range filters {
				if filterID == extent.FileID {
					return true
				}
			}
			return false
		}
	}

	// TODO not used. remove?
	GetAllExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(info *ExtentInfo) bool {
			return IsTinyExtent(info.FileID) || now.Unix()-info.ModifyTime.Unix() > 60*60 && info.IsDeleted == false && info.Size == 0
		}
	}
)

// ExtentStore defines fields used in the storage engine.
// Packets smaller than 128K are stored in the "tinyExtent", a place to persist small files.
// packets larger than or equal to 128K are stored in the normal "extent", a place to persist large files.
// The difference between them is that the extentID of a tinyExtent starts at 5000000 and ends at 5000128.
// Multiple small files can be appended to the same tinyExtent.
// In addition, the deletion of small files is implemented by the punch hole from the underlying file system.
type ExtentStore struct {
	dataDir       string                 // TODO why not call it dataPath?
	baseExtentID  uint64                 // TODO what is baseExtentID
	extentInfoMap map[uint64]*ExtentInfo // map that stores all the extent information
	eiMutex       sync.RWMutex           // mutex for extent info
	cache         *ExtentCache           // extent cache
	mutex         sync.Mutex             // TODO we should not call it mutex. maybe just "mutex"?
	storeSize     int                    // TODO what is store size
	metaFp        *os.File               // TODO metadata file pointer?
	deleteFp      *os.File               // TODO store delete extent history?  what does this mean?
	verifyCrcFp   *os.File
	closeC        chan bool
	closed        bool
	// avaliTinyExtentCh   chan uint64 //avali tinyExtent chan
	// unavaliTinyExtentCh chan uint64 //unavali tinyExtent chan
	goodTinyExtentC chan uint64 // available tinyExtent chan TODO I thought the names of all the channels should be ended with "C"
	badTinyExtentC  chan uint64 // unavailable tinyExtent chan
	blockSize           int
	partitionID         uint64 //partitionID
}

func MkdirAll(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func NewExtentStore(dataDir string, partitionID uint64, storeSize int) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataDir = dataDir
	s.partitionID = partitionID
	if err = MkdirAll(dataDir); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}

	// TODO rename metaFilePath -> filePath ?
	metaFilePath := path.Join(s.dataDir, ExtMetaFileName)
	if s.metaFp, err = os.OpenFile(metaFilePath, ExtMetaFileOpt, 0666); err != nil {
		return
	}
	if err = s.metaFp.Truncate(ExtMetaFileSize); err != nil {
		return
	}

	if s.verifyCrcFp, err = os.OpenFile(path.Join(s.dataDir, ExtCrcHeaderFileName), ExtMetaFileOpt, 0666); err != nil {
		return
	}
	syscall.Fallocate(int(s.verifyCrcFp.Fd()), 1, 0, ExtCrcHeaderSize)

	// TODO rename deleteIdxFilePath
	// Load EXTENT_DELETE
	deleteIdxFilePath := path.Join(s.dataDir, ExtDeleteFileName)
	if s.deleteFp, err = os.OpenFile(deleteIdxFilePath, ExtDeleteFileOpt, 0666); err != nil {
		return
	}
	s.extentInfoMap = make(map[uint64]*ExtentInfo, 200)
	s.cache = NewExtentCache(40)
	if err = s.initBaseFileID(); err != nil {
		err = fmt.Errorf("init base field ID: %v", err)
		return
	}
	s.storeSize = storeSize
	s.closeC = make(chan bool, 1)
	s.closed = false
	err = s.initTinyExtent()
	return
}

// SnapShot returns the information of all the extents on the current data partition.
// When the master sends the loadDataPartition request, the snapshot is used to compare the replicas.
func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var (
		extentInfoSlice []*ExtentInfo
	)

	if extentInfoSlice, err = s.GetAllWatermarks(NormalExtentFilter()); err != nil {
		return
	}

	files = make([]*proto.File, 0, len(extentInfoSlice))
	for _, extentInfo := range extentInfoSlice {
		file := &proto.File{
			Name:     strconv.FormatUint(extentInfo.FileID, 10),
			Crc:      extentInfo.Crc,
			Size:     uint32(extentInfo.Size),
			Modified: extentInfo.ModifyTime.Unix(),
		}
		files = append(files, file)
	}
	return
}

// NextExtentID returns the next extentID. When the client sends the request to create an extent,
// this function generates an unique extentID within the current partition.
// This function can only be called by the leader.
func (s *ExtentStore) NextExtentID() (extentID uint64) {
	return atomic.AddUint64(&s.baseExtentID, 1)
}

func (s *ExtentStore) getExtentKey(extent uint64) string {
	return fmt.Sprintf("extent %v_%v", s.partitionID, extent)
}

// Create creates an extent.
func (s *ExtentStore) Create(extentID uint64, inode uint64) (err error) {
	var extent *Extent
	name := path.Join(s.dataDir, strconv.Itoa(int(extentID)))
	if s.HasExtent(extentID) {
		err = ExtentExistsError
		return err
	}
	if !IsTinyExtent(extentID) && extentID >= MaxExtentId {
		return UnavailableExtentError
	}
	extent = NewExtentInCore(name, extentID)
	err = extent.InitToFS(inode, false)
	if err != nil {
		return err
	}
	s.cache.Put(extent)

	extInfo := &ExtentInfo{}
	extInfo.FromExtent(extent)
	s.eiMutex.Lock()
	s.extentInfoMap[extentID] = extInfo
	s.eiMutex.Unlock()

	s.UpdateBaseExtentID(extentID)
	return
}

func (s *ExtentStore) UpdateBaseExtentID(id uint64) (err error) {
	if IsTinyExtent(id) {
		return
	}
	if id >= atomic.LoadUint64(&s.baseExtentID) {
		atomic.StoreUint64(&s.baseExtentID, id)
		baseExtentIDBytes := make([]byte, ExtMetaBaseIDSize)
		binary.BigEndian.PutUint64(baseExtentIDBytes, atomic.LoadUint64(&s.baseExtentID))
		if _, err = s.metaFp.WriteAt(baseExtentIDBytes, ExtMetaBaseIDOffset); err != nil {
			return
		}
	}
	return
}

func (s *ExtentStore) extent(extentID uint64) (e *Extent, err error) {
	if e, err = s.loadExtentFromDisk(extentID, false); err != nil {
		err = fmt.Errorf("load extent from disk: %v", err)
		return nil, err
	}
	return
}

func (s *ExtentStore) extentWithHeader(extentID uint64) (e *Extent, err error) {
	var ok bool
	if e, ok = s.cache.Get(extentID); !ok {
		if e, err = s.loadExtentFromDisk(extentID, true); err != nil {
			err = fmt.Errorf("load  %v from disk: %v", s.getExtentKey(extentID), err)
			return nil, err
		}
	}
	return
}

func (s *ExtentStore) loadExtentHeader(extentId uint64, e *Extent) (err error) {
	if extentId >= MaxExtentId && !IsTinyExtent(extentId) {
		return UnavailableExtentError
	}
	offset := extentId * util.BlockHeaderSize
	_, err = s.verifyCrcFp.ReadAt(e.header, int64(offset))
	return
}

// HasExtent tells if the extent store has the extent with the given ID
func (s *ExtentStore) HasExtent(extentID uint64) (exist bool) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()
	_, exist = s.extentInfoMap[extentID]
	return
}

// ExtentCount returns the number of extents in the extentInfoMap
func (s *ExtentStore) ExtentCount() (count int) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()
	return len(s.extentInfoMap)
}

func (s *ExtentStore) loadExtentFromDisk(extentID uint64, loadHeader bool) (e *Extent, err error) {
	name := path.Join(s.dataDir, strconv.Itoa(int(extentID)))
	e = NewExtentInCore(name, extentID)
	if err = e.RestoreFromFS(loadHeader); err != nil {
		err = fmt.Errorf("restore from file %v loadHeader %v system: %v", name, loadHeader, err)
		return
	}
	if loadHeader {
		s.loadExtentHeader(extentID, e)
		s.cache.Put(e)
	}

	return
}

func (s *ExtentStore) initBaseFileID() (err error) {
	var (
		baseFileID uint64
	)
	baseFileIDBytes := make([]byte, ExtMetaBaseIDSize)
	if _, err = s.metaFp.ReadAt(baseFileIDBytes, ExtMetaBaseIDOffset); err == nil {
		baseFileID = binary.BigEndian.Uint64(baseFileIDBytes)
	}
	files, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return err
	}

	data := make([]byte, ExtCrcHeaderSize)
	_, err = s.verifyCrcFp.ReadAt(data, 0)
	if err != io.EOF {
		return
	}

	var (
		extentID   uint64
		isExtent   bool
		extent     *Extent
		extentInfo *ExtentInfo
		loadErr    error
	)
	for _, f := range files {
		if extentID, isExtent = s.ExtentID(f.Name()); !isExtent {
			continue
		}
		if extentID < MinExtentID {
			continue
		}
		if extent, loadErr = s.extent(extentID); loadErr != nil {
			continue
		}
		if !IsTinyExtent(extentID) {
			extent.header = make([]byte, util.BlockHeaderSize)
			copy(extent.header, data[extentID*util.BlockHeaderSize:(extentID+1)*util.BlockHeaderSize])
		}
		extentInfo = &ExtentInfo{}
		extentInfo.FromExtent(extent)
		s.eiMutex.Lock()
		s.extentInfoMap[extentID] = extentInfo
		s.eiMutex.Unlock()
		extent.Close()
		if !IsTinyExtent(extentID) && extentID > baseFileID {
			baseFileID = extentID
		}
	}
	if baseFileID < MinExtentID {
		baseFileID = MinExtentID
	}
	atomic.StoreUint64(&s.baseExtentID, baseFileID)
	log.LogInfof("datadir(%v) maxBaseId(%v)", s.dataDir, baseFileID)
	return nil
}

// TODO what is block CRC?
func (s *ExtentStore) updateBlockCrc(extentID uint64, blockNo int, crc uint32, e *Extent) (err error) {
	startIdx := util.BlockHeaderCrcIndex + blockNo*util.PerBlockCrcSize
	endIdx := startIdx + util.PerBlockCrcSize
	binary.BigEndian.PutUint32(e.header[startIdx:endIdx], crc)
	verifyStart := startIdx + int(util.BlockHeaderSize*extentID)
	if _, err = s.verifyCrcFp.WriteAt(e.header[startIdx:endIdx], int64(verifyStart)); err != nil {
		return
	}
	e.modifyTime = time.Now()

	return
}

func (s *ExtentStore) Write(extentID uint64, offset, size int64, data []byte, crc uint32) (err error) {
	var (
		has        bool
		extent     *Extent
		blocks     []int
		blockCrc   []uint32
		extentInfo *ExtentInfo
	)
	s.eiMutex.RLock()
	extentInfo, has = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if !has {
		err = ExtentNotFoundError
		return
	}
	extent, err = s.extentWithHeader(extentID)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return err
	}
	if extent.HasBeenMarkedAsDeleted() {
		return ExtentHasBeenDeletedError
	}
	blocks, blockCrc, err = extent.Write(data, offset, size, crc)
	for index := 0; index < len(blocks); index++ {
		s.updateBlockCrc(extentID, blocks[index], blockCrc[index], extent)
	}
	if err != nil {
		return err
	}
	extentInfo.FromExtent(extent)
	return nil
}

func (s *ExtentStore) TinyExtentRepairWrite(extentID uint64, offset, size int64, data []byte, crc uint32) (err error) {
	var (
		extentInfo *ExtentInfo
		has        bool
		extent     *Extent
	)
	if !IsTinyExtent(extentID) {
		return fmt.Errorf("extent %v not tinyExtent", extentID)
	}
	s.eiMutex.RLock()
	extentInfo, has = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", extentID)
		return
	}
	extent, err = s.extentWithHeader(extentID)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return err
	}
	if extent.HasBeenMarkedAsDeleted() {
		return ExtentHasBeenDeletedError
	}
	if err = extent.RepairWriteTiny(data, offset, size, crc); err != nil {
		return err
	}
	extentInfo.FromExtent(extent)
	return
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

func IsTinyExtent(extentID uint64) bool {
	return extentID >= TinyExtentStartID && extentID < TinyExtentStartID+TinyExtentCount
}

func (s *ExtentStore) Read(extentID uint64, offset, size int64, nbuf []byte, isRepairRead bool) (crc uint32, err error) {
	var extent *Extent
	if extent, err = s.extentWithHeader(extentID); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return
	}
	if extent.HasBeenMarkedAsDeleted() {
		err = ExtentHasBeenDeletedError
		return
	}
	crc, err = extent.Read(nbuf, offset, size, isRepairRead)
	return
}

func (s *ExtentStore) MarkDelete(extentID uint64, offset, size int64) (err error) {
	var (
		extent     *Extent
		extentInfo *ExtentInfo
		has        bool
	)

	s.eiMutex.RLock()
	extentInfo, has = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if !has {
		return
	}

	if extent, err = s.extentWithHeader(extentID); err != nil {
		return nil
	}

	if IsTinyExtent(extentID) {
		return extent.DeleteTiny(offset, size)
	}

	if err = extent.MarkDelete(); err != nil {
		return
	}
	extentInfo.FromExtent(extent)
	extentInfo.IsDeleted = true

	s.cache.Del(extent.ID())

	s.eiMutex.Lock()
	delete(s.extentInfoMap, extentID)
	s.eiMutex.Unlock()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, extentID)
	if _, err = s.deleteFp.Write(buf); err != nil {
		return
	}

	return
}

func (s *ExtentStore) FlushDelete() (err error) {
	var (
		delIdxOff uint64
		stat      os.FileInfo
		readN     int
		extentID  uint64
		opErr     error
	)
	// Load delete index offset from EXTENT_META
	delIdxOffBytes := make([]byte, ExtMetaDeleteIdxSize)
	if _, err = s.metaFp.ReadAt(delIdxOffBytes, ExtMetaDeleteIdxOffset); err == nil {
		delIdxOff = binary.BigEndian.Uint64(delIdxOffBytes)
	} else {
		delIdxOff = 0
	}

	// Check EXTENT_DELETE
	if stat, err = s.deleteFp.Stat(); err != nil {
		return
	}

	// Read data from EXTENT_DELETE and remove files.
	readBuf := make([]byte, stat.Size()-int64(delIdxOff))
	if readN, err = s.deleteFp.ReadAt(readBuf, int64(delIdxOff)); err != nil && err != io.EOF {
		return
	}
	reader := bytes.NewReader(readBuf[:readN])
	for {
		opErr = binary.Read(reader, binary.BigEndian, &extentID)
		if opErr != nil && opErr != io.EOF {
			break
		}
		if opErr == io.EOF {
			err = nil
			break
		}
		delIdxOff += 8
		_, err = s.extentWithHeader(extentID)
		if err != nil {
			continue
		}
		s.cache.Del(extentID)
		extentFilePath := path.Join(s.dataDir, strconv.FormatUint(extentID, 10))
		if opErr = os.Remove(extentFilePath); opErr != nil {
			continue
		}
	}

	// Store offset of EXTENT_DELETE into EXTENT_META
	binary.BigEndian.PutUint64(delIdxOffBytes, delIdxOff)
	if _, err = s.metaFp.WriteAt(delIdxOffBytes, ExtMetaDeleteIdxOffset); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.closed {
		return
	}

	// Release cache
	s.cache.Flush()
	s.cache.Clear()

	// Release meta file
	s.metaFp.Sync()
	s.metaFp.Close()

	s.verifyCrcFp.Sync()
	s.verifyCrcFp.Close()

	// Release delete index file
	s.deleteFp.Sync()
	s.deleteFp.Close()
	s.closed = true
}

// TODO what is the formal definition of the watermark?
func (s *ExtentStore) Watermark(extentID uint64, reload bool) (extentInfo *ExtentInfo, err error) {
	var (
		has    bool
		extent *Extent
	)
	s.eiMutex.RLock()
	extentInfo, has = s.extentInfoMap[extentID]
	s.eiMutex.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", s.getExtentKey(extentID))
		return
	}
	if reload {
		if extent, err = s.extentWithHeader(extentID); err != nil {
			return
		}
		extentInfo.FromExtent(extent)
	}
	return
}

func (s *ExtentStore) GetTinyExtentoffset(extentID uint64) (watermark int64, err error) {
	einfo, err := s.Watermark(extentID, false)
	if err != nil {
		return
	}
	watermark = int64(einfo.Size)
	if watermark % PageSize != 0 {
		watermark = watermark + (PageSize - watermark % PageSize)
	}

	return
}

func (s *ExtentStore) GetAllWatermarks(filter ExtentFilter) (extents []*ExtentInfo, err error) {
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
		extents = append(extents, extentInfo)
	}
	return
}

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
	s.goodTinyExtentC = make(chan uint64, TinyExtentCount)
	s.badTinyExtentC = make(chan uint64, TinyExtentCount)
	var extentID uint64

	// TODO buffer the value of TinyExtentStartID + TinyExtentCount
	for extentID = TinyExtentStartID; extentID < TinyExtentStartID + TinyExtentCount; extentID++ {
		err = s.Create(extentID, 0)
		if err == nil || err == ExtentExistsError {
			err = nil
			s.badTinyExtentC <- extentID
			continue
		}
		return err
	}

	return
}

func (s *ExtentStore) GetGoodTinyExtent() (extentID uint64, err error) {
	select {
	case extentID = <-s.goodTinyExtentC:
		return
	default:
		return 0, NoAvailableExtentError

	}
}

func (s *ExtentStore) SendToGoodTinyExtentC(extentID uint64) {
	s.goodTinyExtentC <- extentID
}

func (s *ExtentStore) SendAllToBadTinyExtentC(extentIds []uint64) {
	for _, extentID := range extentIds {
		s.badTinyExtentC <- extentID
	}
}

func (s *ExtentStore) GoodTinyExtentCnt() int {
	return len(s.goodTinyExtentC)
}

func (s *ExtentStore) BadTinyExtentCnt() int {
	return len(s.badTinyExtentC)
}

func (s *ExtentStore) MoveAllToBadTinyExtentC(cnt int) {
	for i := 0; i < cnt; i++ {
		extentID, err := s.GetGoodTinyExtent()
		if err != nil {
			return
		}
		s.SendToBadTinyExtentC(extentID)
	}
}

func (s *ExtentStore) SendToBadTinyExtentC(extentID uint64) {
	s.badTinyExtentC <- extentID
}

func (s *ExtentStore) GetBadTinyExtent() (extentID uint64, err error) {
	select {
	case extentID = <-s.badTinyExtentC:
		return
	default:
		return 0, NoUnavailableExtentError

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
