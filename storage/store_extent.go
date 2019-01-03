// Copyright 2018 The Containerfs Authors.
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

const (
	ExtMetaFileName        = "EXTENT_META"
	ExtCrcHeaderFileName   = "EXTENT_CRC"
	ExtMetaFileOpt         = os.O_CREATE | os.O_RDWR
	ExtDeleteFileName      = "EXTENT_DELETE"
	ExtDeleteFileOpt       = os.O_CREATE | os.O_RDWR | os.O_APPEND
	ExtMetaBaseIDOffset    = 0
	ExtMetaBaseIDSize      = 8
	ExtMetaDeleteIdxOffset = 8
	ExtMetaDeleteIdxSize   = 8
	ExtMetaFileSize        = ExtMetaBaseIDSize + ExtMetaDeleteIdxSize
	MaxExtentId            = 40000
	ExtCrcHeaderSize       = MaxExtentId * util.BlockHeaderSize
	TinyExtentCount        = 64
	TinyExtentStartID      = 50000000
	MinExtentID            = 2
)

var (
	RegexpExtentFile, _ = regexp.Compile("^(\\d)+$")
)

type ExtentFilter func(info *ExtentInfo) bool

// Filters
var (
	GetStableExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(info *ExtentInfo) bool {
			return !IsTinyExtent(info.FileID) && now.Unix()-info.ModTime.Unix() > 10*60 && info.Deleted == false && info.Size > 0
		}
	}

	GetStableTinyExtentFilter = func(filters []uint64) ExtentFilter {
		return func(info *ExtentInfo) bool {
			if !IsTinyExtent(info.FileID) {
				return false
			}
			for _, filterID := range filters {
				if filterID == info.FileID {
					return true
				}
			}
			return false
		}
	}

	GetAllExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(info *ExtentInfo) bool {
			return IsTinyExtent(info.FileID) || now.Unix()-info.ModTime.Unix() > 60*60 && info.Deleted == false && info.Size == 0
		}
	}
)

/*
ExtentStore introduction:
For packets smaller than 128K, the client considers it to be a small file and should be stored in tinyExtent
and the other should be stored as an extent file. The difference between the two is that the small tinyExtent's extentID
starts at 5000000 and ends at 5000128. Each small file is constantly append to tinyExtent. When deleting,
the deletion of small files is removed by purgehole, while the large file extent deletes the extent directly.
*/
type ExtentStore struct {
	dataDir             string                 //dataPartition store dataPath
	baseExtentID        uint64                 //based extentID
	extentInfoMap       map[uint64]*ExtentInfo //all extentInfo
	extentInfoMux       sync.RWMutex           //lock
	cache               *ExtentCache           //extent cache
	lock                sync.Mutex
	storeSize           int      //dataPartion store size
	metaFp              *os.File //store dataPartion meta
	deleteFp            *os.File //store delete extent history
	verifyCrcFp         *os.File
	closeC              chan bool
	closed              bool
	avaliTinyExtentCh   chan uint64 //avali tinyExtent chan
	unavaliTinyExtentCh chan uint64 //unavali tinyExtent chan
	blockSize           int
	partitionID         uint64 //partitionID
}

func CheckAndCreateSubdir(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func NewExtentStore(dataDir string, partitionID uint64, storeSize int) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataDir = dataDir
	s.partitionID = partitionID
	if err = CheckAndCreateSubdir(dataDir); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}

	// Load EXTENT_META
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

/*
SnapShot This function is used to obtain the fileInfo information of all extents of the current datapartition.
When the master sends the loadDataPartition, it is used to compare
the replica members of the three dataPartitions.
*/
func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var (
		extentInfoSlice []*ExtentInfo
	)
	if extentInfoSlice, err = s.GetAllExtentWatermark(GetAllExtentFilter()); err != nil {
		return
	}
	files = make([]*proto.File, 0, len(extentInfoSlice))
	for _, extentInfo := range extentInfoSlice {
		file := &proto.File{
			Name:     strconv.FormatUint(extentInfo.FileID, 10),
			Crc:      extentInfo.Crc,
			Size:     uint32(extentInfo.Size),
			Modified: extentInfo.ModTime.Unix(),
		}
		files = append(files, file)
	}
	return
}

/*NextExtentID This function is used to get the next extentID. When the client sends the creation of the extent,
this function is used to generate a unique extentID inside the current partition. T
his function is only called on the leader.*/
func (s *ExtentStore) NextExtentID() (extentID uint64) {
	return atomic.AddUint64(&s.baseExtentID, 1)
}

func (s *ExtentStore) getExtentKey(extent uint64) string {
	return fmt.Sprintf("extent %v_%v", s.partitionID, extent)
}

/*
Create This function is used to create an extent id
*/
func (s *ExtentStore) Create(extentID uint64, inode uint64) (err error) {
	var extent *Extent
	name := path.Join(s.dataDir, strconv.Itoa(int(extentID)))
	if s.IsExistExtent(extentID) {
		err = ErrorExtentHasExsit
		return err
	}
	if extentID >= MaxExtentId {
		return ErrorUnavaliExtent
	}
	extent = NewExtentInCore(name, extentID)
	err = extent.InitToFS(inode, false)
	if err != nil {
		return err
	}
	s.cache.Put(extent)

	extInfo := &ExtentInfo{}
	extInfo.FromExtent(extent)
	s.extentInfoMux.Lock()
	s.extentInfoMap[extentID] = extInfo
	s.extentInfoMux.Unlock()

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

func (s *ExtentStore) getExtent(extentID uint64) (e *Extent, err error) {
	if e, err = s.loadExtentFromDisk(extentID, false); err != nil {
		err = fmt.Errorf("load extent from disk: %v", err)
		return nil, err
	}
	return
}

func (s *ExtentStore) getExtentWithHeader(extentID uint64) (e *Extent, err error) {
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
		return ErrorUnavaliExtent
	}
	offset := extentId * util.BlockHeaderSize
	_, err = s.verifyCrcFp.ReadAt(e.header, int64(offset))
	return
}

func (s *ExtentStore) IsExistExtent(extentID uint64) (exist bool) {
	s.extentInfoMux.RLock()
	defer s.extentInfoMux.RUnlock()
	_, exist = s.extentInfoMap[extentID]
	return
}

func (s *ExtentStore) GetExtentCount() (count int) {
	s.extentInfoMux.RLock()
	defer s.extentInfoMux.RUnlock()
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
		if extentID, isExtent = s.ParseExtentID(f.Name()); !isExtent {
			continue
		}
		if extentID < MinExtentID {
			continue
		}
		if extent, loadErr = s.getExtent(extentID); loadErr != nil {
			continue
		}
		if !IsTinyExtent(extentID) {
			extent.header = make([]byte, util.BlockHeaderSize)
			copy(extent.header, data[extentID*util.BlockHeaderSize:(extentID+1)*util.BlockHeaderSize])
		}
		extentInfo = &ExtentInfo{}
		extentInfo.FromExtent(extent)
		s.extentInfoMux.Lock()
		s.extentInfoMap[extentID] = extentInfo
		s.extentInfoMux.Unlock()
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
		has      bool
		extent   *Extent
		blocks   []int
		blockCrc []uint32
		einfo    *ExtentInfo
	)
	s.extentInfoMux.RLock()
	einfo, has = s.extentInfoMap[extentID]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", extentID)
		return
	}
	extent, err = s.getExtentWithHeader(extentID)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return err
	}
	if extent.IsMarkDelete() {
		return ErrorExtentHasDelete
	}
	blocks, blockCrc, err = extent.Write(data, offset, size, crc)
	for index := 0; index < len(blocks); index++ {
		s.updateBlockCrc(extentID, blocks[index], blockCrc[index], extent)
	}
	if err != nil {
		return err
	}
	einfo.FromExtent(extent)
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
	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentID]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", extentID)
		return
	}
	extent, err = s.getExtentWithHeader(extentID)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return err
	}
	if extent.IsMarkDelete() {
		return ErrorExtentHasDelete
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
		return NewParamMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	if offset >= util.BlockCount*util.BlockSize || size == 0 {
		return NewParamMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}

	if size > util.BlockSize {
		return NewParamMismatchErr(fmt.Sprintf("offset=%v size=%v", offset, size))
	}
	return nil
}

func IsTinyExtent(extentID uint64) bool {
	return extentID >= TinyExtentStartID && extentID < TinyExtentStartID+TinyExtentCount
}

func (s *ExtentStore) Read(extentID uint64, offset, size int64, nbuf []byte, isRepairRead bool) (crc uint32, err error) {
	var extent *Extent
	if extent, err = s.getExtentWithHeader(extentID); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(extentID, offset, size); err != nil {
		return
	}
	if extent.IsMarkDelete() {
		err = ErrorExtentHasDelete
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

	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentID]
	s.extentInfoMux.RUnlock()
	if !has {
		return
	}

	if extent, err = s.getExtentWithHeader(extentID); err != nil {
		return nil
	}

	if IsTinyExtent(extentID) {
		return extent.DeleteTiny(offset, size)
	}

	if err = extent.MarkDelete(); err != nil {
		return
	}
	extentInfo.FromExtent(extent)
	extentInfo.Deleted = true

	s.cache.Del(extent.ID())

	s.extentInfoMux.Lock()
	delete(s.extentInfoMap, extentID)
	s.extentInfoMux.Unlock()

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
		_, err = s.getExtentWithHeader(extentID)
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
	s.lock.Lock()
	defer s.lock.Unlock()
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

func (s *ExtentStore) GetWatermark(extentID uint64, reload bool) (extentInfo *ExtentInfo, err error) {
	var (
		has    bool
		extent *Extent
	)
	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentID]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", s.getExtentKey(extentID))
		return
	}
	if reload {
		if extent, err = s.getExtentWithHeader(extentID); err != nil {
			return
		}
		extentInfo.FromExtent(extent)
	}
	return
}

func (s *ExtentStore) TinyExtentWritePrepare(extentID uint64) (watermark int64, err error) {
	einfo, err := s.GetWatermark(extentID, false)
	if err != nil {
		return
	}
	watermark = int64(einfo.Size)
	if watermark%PageSize != 0 {
		watermark = watermark + (PageSize - watermark%PageSize)
	}

	return
}

func (s *ExtentStore) GetAllExtentWatermark(filter ExtentFilter) (extents []*ExtentInfo, err error) {
	extents = make([]*ExtentInfo, 0)
	extentInfoSlice := make([]*ExtentInfo, 0, len(s.extentInfoMap))
	s.extentInfoMux.RLock()
	for _, extentID := range s.extentInfoMap {
		extentInfoSlice = append(extentInfoSlice, extentID)
	}
	s.extentInfoMux.RUnlock()

	for _, extentInfo := range extentInfoSlice {
		if filter != nil && !filter(extentInfo) {
			continue
		}
		extents = append(extents, extentInfo)
	}
	return
}

func (s *ExtentStore) ParseExtentID(filename string) (extentID uint64, isExtent bool) {
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

/*
  this function is used for write Tiny File
*/

func (s *ExtentStore) initTinyExtent() (err error) {
	s.avaliTinyExtentCh = make(chan uint64, TinyExtentCount)
	s.unavaliTinyExtentCh = make(chan uint64, TinyExtentCount)
	var extentID uint64
	for extentID = TinyExtentStartID; extentID < TinyExtentStartID+TinyExtentCount; extentID++ {
		err = s.Create(extentID, 0)
		if err == nil || err == ErrorExtentHasExsit {
			err = nil
			s.unavaliTinyExtentCh <- extentID
			continue
		}
		return err
	}

	return
}

func (s *ExtentStore) GetAvaliTinyExtent() (extentID uint64, err error) {
	select {
	case extentID = <-s.avaliTinyExtentCh:
		return
	default:
		return 0, ErrorNoAvaliExtent

	}
}

func (s *ExtentStore) PutTinyExtentToAvaliCh(extentID uint64) {
	s.avaliTinyExtentCh <- extentID
}

func (s *ExtentStore) PutTinyExtentsToUnAvaliCh(extentIds []uint64) {
	for _, extentID := range extentIds {
		s.unavaliTinyExtentCh <- extentID
	}
}

func (s *ExtentStore) GetAvaliExtentLen() int {
	return len(s.avaliTinyExtentCh)
}

func (s *ExtentStore) GetUnAvaliExtentLen() int {
	return len(s.unavaliTinyExtentCh)
}

func (s *ExtentStore) MoveAvaliExtentToUnavali(cnt int) {
	for i := 0; i < cnt; i++ {
		extentID, err := s.GetAvaliTinyExtent()
		if err != nil {
			return
		}
		s.PutTinyExtentToUnavaliCh(extentID)
	}
}

func (s *ExtentStore) PutTinyExtentToUnavaliCh(extentID uint64) {
	s.unavaliTinyExtentCh <- extentID
}

func (s *ExtentStore) GetUnavaliTinyExtent() (extentID uint64, err error) {
	select {
	case extentID = <-s.unavaliTinyExtentCh:
		return
	default:
		return 0, ErrorNoUnAvaliExtent

	}
}

func (s *ExtentStore) GetStoreSize() (totalSize uint64) {
	extentInfos := make([]*ExtentInfo, 0)
	s.extentInfoMux.RLock()
	for _, extentInfo := range s.extentInfoMap {
		extentInfos = append(extentInfos, extentInfo)
	}
	s.extentInfoMux.RUnlock()
	for _, extentInfo := range extentInfos {
		totalSize += extentInfo.Size
	}

	return totalSize
}
