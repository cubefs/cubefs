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
)

const (
	ExtMetaFileName        = "EXTENT_META"
	ExtMetaFileOpt         = os.O_CREATE | os.O_RDWR
	ExtDeleteFileName      = "EXTENT_DELETE"
	ExtDeleteFileOpt       = os.O_CREATE | os.O_RDWR | os.O_APPEND
	ExtMetaBaseIdOffset    = 0
	ExtMetaBaseIdSize      = 8
	ExtMetaDeleteIdxOffset = 8
	ExtMetaDeleteIdxSize   = 8
	ExtMetaFileSize        = ExtMetaBaseIdSize + ExtMetaDeleteIdxSize
	TinyExtentCount        = 128
	TinyExtentStartId      = 50000000
	MinExtentId            = 2
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
			return !IsTinyExtent(info.FileId) && now.Unix()-info.ModTime.Unix() > 10*60 && info.Deleted == false && info.Size > 0
		}
	}

	GetStableTinyExtentFilter = func(filters []uint64) ExtentFilter {
		return func(info *ExtentInfo) bool {
			if !IsTinyExtent(info.FileId) {
				return false
			}
			for _, filterId := range filters {
				if filterId == info.FileId {
					return true
				}
			}
			return false
		}
	}

	GetEmptyExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(info *ExtentInfo) bool {
			return !IsTinyExtent(info.FileId) && now.Unix()-info.ModTime.Unix() > 60*60 && info.Deleted == false && info.Size == 0
		}
	}
)

/*
Extent store introduction:
For packets smaller than 128K, the client considers it to be a small file and should be stored in tinyExtent
and the other should be stored as an extent file. The difference between the two is that the small tinyExtent's extentId
starts at 5000000 and ends at 5000128. Each small file is constantly append to tinyExtent. When deleting,
the deletion of small files is removed by purgehole, while the large file extent deletes the extent directly.
*/
type ExtentStore struct {
	dataDir             string                 //dataPartition store dataPath
	baseExtentId        uint64                 //based extentId
	extentInfoMap       map[uint64]*ExtentInfo //all extentInfo
	extentInfoMux       sync.RWMutex           //lock
	cache               *ExtentCache           //extent cache
	lock                sync.Mutex
	storeSize           int      //dataPartion store size
	metaFp              *os.File //store dataPartion meta
	deleteFp            *os.File //store delete extent history
	closeC              chan bool
	closed              bool
	avaliTinyExtentCh   chan uint64 //avali tinyExtent chan
	unavaliTinyExtentCh chan uint64 //unavali tinyExtent chan
	blockSize           int
	partitionId         uint64 //partitionId
}

func CheckAndCreateSubdir(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func NewExtentStore(dataDir string, partitionId uint64, storeSize int) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataDir = dataDir
	s.partitionId = partitionId
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

	// Load EXTENT_DELETE
	deleteIdxFilePath := path.Join(s.dataDir, ExtDeleteFileName)
	if s.deleteFp, err = os.OpenFile(deleteIdxFilePath, ExtDeleteFileOpt, 0666); err != nil {
		return
	}
	s.extentInfoMap = make(map[uint64]*ExtentInfo, 40)
	s.cache = NewExtentCache(40)
	if err = s.initBaseFileId(); err != nil {
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
 This function is used to obtain the fileInfo information of all extents of the current datapartition.
When the master sends the loadDataPartition, it is used to compare
the replica members of the three dataPartitions.
*/
func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var (
		extentInfoSlice []*ExtentInfo
	)
	if extentInfoSlice, err = s.GetAllWatermark(GetStableExtentFilter()); err != nil {
		return
	}
	files = make([]*proto.File, 0, len(extentInfoSlice))
	for _, extentInfo := range extentInfoSlice {
		if extentInfo.Size == 0 || time.Now().Unix()-extentInfo.ModTime.Unix() < 5*60 {
			continue
		}
		file := &proto.File{
			Name:     strconv.FormatUint(extentInfo.FileId, 10),
			Crc:      extentInfo.Crc,
			Size:     uint32(extentInfo.Size),
			Modified: extentInfo.ModTime.Unix(),
		}
		files = append(files, file)
	}
	return
}

/*This function is used to get the next extentId. When the client sends the creation of the extent,
this function is used to generate a unique extentId inside the current partition. T
his function is only called on the leader.*/
func (s *ExtentStore) NextExtentId() (extentId uint64) {
	return atomic.AddUint64(&s.baseExtentId, 1)
}

func (s *ExtentStore) getExtentKey(extent uint64) string {
	return fmt.Sprintf("extent %v_%v", s.partitionId, extent)
}

/*
  This function is used to create an extent id
*/
func (s *ExtentStore) Create(extentId uint64, inode uint64) (err error) {
	var extent *Extent
	name := path.Join(s.dataDir, strconv.Itoa(int(extentId)))
	if s.IsExistExtent(extentId) {
		err = ErrorExtentHasExsit
		return err
	} else {
		extent = NewExtentInCore(name, extentId)
		err = extent.InitToFS(inode, false)
		if err != nil {
			return err
		}
	}
	s.cache.Put(extent)

	extInfo := &ExtentInfo{}
	extInfo.FromExtent(extent)
	s.extentInfoMux.Lock()
	s.extentInfoMap[extentId] = extInfo
	s.extentInfoMux.Unlock()

	s.UpdateBaseExtentId(extentId)
	return
}

func (s *ExtentStore) UpdateBaseExtentId(id uint64) (err error) {
	if IsTinyExtent(id) {
		return
	}
	if id >= atomic.LoadUint64(&s.baseExtentId) {
		atomic.StoreUint64(&s.baseExtentId, id)
		baseExtentIdBytes := make([]byte, ExtMetaBaseIdSize)
		binary.BigEndian.PutUint64(baseExtentIdBytes, atomic.LoadUint64(&s.baseExtentId))
		if _, err = s.metaFp.WriteAt(baseExtentIdBytes, ExtMetaBaseIdOffset); err != nil {
			return
		}
		err = s.metaFp.Sync()
	}
	return
}

func (s *ExtentStore) getExtent(extentId uint64) (e *Extent, err error) {
	if e, err = s.loadExtentFromDisk(extentId, false); err != nil {
		err = fmt.Errorf("load extent from disk: %v", err)
		return nil, err
	}
	return
}

func (s *ExtentStore) getExtentWithHeader(extentId uint64) (e *Extent, err error) {
	var ok bool
	if e, ok = s.cache.Get(extentId); !ok {
		if e, err = s.loadExtentFromDisk(extentId, true); err != nil {
			err = fmt.Errorf("load  %v from disk: %v", s.getExtentKey(extentId), err)
			return nil, err
		}
	}
	return
}

func (s *ExtentStore) IsExistExtent(extentId uint64) (exist bool) {
	s.extentInfoMux.RLock()
	defer s.extentInfoMux.RUnlock()
	_, exist = s.extentInfoMap[extentId]
	return
}

func (s *ExtentStore) GetExtentCount() (count int) {
	s.extentInfoMux.RLock()
	defer s.extentInfoMux.RUnlock()
	return len(s.extentInfoMap)
}

func (s *ExtentStore) loadExtentFromDisk(extentId uint64, loadHeader bool) (e *Extent, err error) {
	name := path.Join(s.dataDir, strconv.Itoa(int(extentId)))
	e = NewExtentInCore(name, extentId)
	if err = e.RestoreFromFS(loadHeader); err != nil {
		err = fmt.Errorf("restore from file %v loadHeader %v system: %v", name, loadHeader, err)
		return
	}
	if loadHeader {
		s.cache.Put(e)
	}

	return
}
func (s *ExtentStore) initBaseFileId() (err error) {
	var (
		baseFileId uint64
	)
	baseFileIdBytes := make([]byte, ExtMetaBaseIdSize)
	if _, err = s.metaFp.ReadAt(baseFileIdBytes, ExtMetaBaseIdOffset); err == nil {
		baseFileId = binary.BigEndian.Uint64(baseFileIdBytes)
	}
	files, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return err
	}
	var (
		extentId   uint64
		isExtent   bool
		extent     *Extent
		extentInfo *ExtentInfo
		loadErr    error
	)
	for _, f := range files {
		if extentId, isExtent = s.ParseExtentId(f.Name()); !isExtent {
			continue
		}
		if extentId < MinExtentId {
			continue
		}
		if extent, loadErr = s.getExtent(extentId); loadErr != nil {
			continue
		}
		extentInfo = &ExtentInfo{}
		extentInfo.FromExtent(extent)
		extentInfo.Crc = 0
		s.extentInfoMux.Lock()
		s.extentInfoMap[extentId] = extentInfo
		s.extentInfoMux.Unlock()
		if !IsTinyExtent(extentId) && extentId > baseFileId {
			baseFileId = extentId
		}
	}
	if baseFileId < MinExtentId {
		baseFileId = MinExtentId
	}
	atomic.StoreUint64(&s.baseExtentId, baseFileId)
	log.LogInfof("datadir(%v) maxBaseId(%v)", s.dataDir, baseFileId)
	return nil
}

func (s *ExtentStore) Write(extentId uint64, offset, size int64, data []byte, crc uint32) (err error) {
	var (
		has    bool
		extent *Extent
	)
	s.extentInfoMux.RLock()
	_, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", extentId)
		return
	}
	extent, err = s.getExtentWithHeader(extentId)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentId, offset, size); err != nil {
		return err
	}
	if extent.IsMarkDelete() {
		return ErrorExtentHasDelete
	}
	if err = extent.Write(data, offset, size, crc); err != nil {
		return err
	}
	s.extentInfoMux.RLock()
	s.extentInfoMap[extentId].FromExtent(extent)
	s.extentInfoMux.RUnlock()
	return nil
}

func (s *ExtentStore) TinyExtentRecover(extentId uint64, offset, size int64, data []byte, crc uint32) (err error) {
	var (
		extentInfo *ExtentInfo
		has        bool
		extent     *Extent
	)
	if !IsTinyExtent(extentId) {
		return fmt.Errorf("extent %v not tinyExtent", extentId)
	}
	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", extentId)
		return
	}
	extent, err = s.getExtentWithHeader(extentId)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(extentId, offset, size); err != nil {
		return err
	}
	if extent.IsMarkDelete() {
		return ErrorExtentHasDelete
	}
	if err = extent.TinyRecover(data, offset, size, crc); err != nil {
		return err
	}
	extentInfo.FromExtent(extent)
	return
}

func (s *ExtentStore) checkOffsetAndSize(extentId uint64, offset, size int64) error {
	if IsTinyExtent(extentId) {
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

func IsTinyExtent(extentId uint64) bool {
	return extentId >= TinyExtentStartId && extentId < TinyExtentStartId+TinyExtentCount
}

func (s *ExtentStore) Read(extentId uint64, offset, size int64, nbuf []byte) (crc uint32, err error) {
	var extent *Extent
	if extent, err = s.getExtentWithHeader(extentId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(extentId, offset, size); err != nil {
		return
	}
	if extent.IsMarkDelete() {
		err = ErrorExtentHasDelete
		return
	}
	crc, err = extent.Read(nbuf, offset, size)
	return
}

func (s *ExtentStore) MarkDelete(extentId uint64, offset, size int64) (err error) {
	var (
		extent     *Extent
		extentInfo *ExtentInfo
		has        bool
	)

	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if !has {
		return
	}

	if extent, err = s.getExtentWithHeader(extentId); err != nil {
		return nil
	}

	if IsTinyExtent(extentId) {
		return extent.DeleteTiny(offset, size)
	}

	if err = extent.MarkDelete(); err != nil {
		return
	}
	extentInfo.FromExtent(extent)
	extentInfo.Deleted = true

	s.cache.Del(extent.ID())

	s.extentInfoMux.Lock()
	delete(s.extentInfoMap, extentId)
	s.extentInfoMux.Unlock()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, extentId)
	if _, err = s.deleteFp.Write(buf); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Cleanup() {
	extentInfoSlice, _ := s.GetAllWatermark(GetEmptyExtentFilter())
	if len(extentInfoSlice) == 0 {
		return
	}
	for _, extentInfo := range extentInfoSlice {
		if IsTinyExtent(extentInfo.FileId) {
			continue
		}
		if extentInfo.Size == 0 {
			log.LogWarnf("start delete empty  %v", s.getExtentKey(extentInfo.FileId))
			extent, err := s.getExtentWithHeader(extentInfo.FileId)
			if err != nil {
				log.LogWarnf("delete empty  %v error %v", s.getExtentKey(extentInfo.FileId), err.Error())
				continue
			}
			if extent.Size() == 0 && !extent.IsMarkDelete() {
				err = s.DeleteDirtyExtent(extent.ID())
				if err != nil {
					log.LogWarnf("delete empty  %v error %v", s.getExtentKey(extentInfo.FileId), err.Error())
				} else {
					log.LogWarnf("delete empty  %v success", s.getExtentKey(extentInfo.FileId))

				}
			}
		}
	}
}

func (s *ExtentStore) FlushDelete() (err error) {
	var (
		delIdxOff uint64
		stat      os.FileInfo
		readN     int
		extentId  uint64
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
		opErr = binary.Read(reader, binary.BigEndian, &extentId)
		if opErr != nil && opErr != io.EOF {
			break
		}
		if opErr == io.EOF {
			err = nil
			break
		}
		delIdxOff += 8
		_, err = s.getExtentWithHeader(extentId)
		if err != nil {
			continue
		}
		s.cache.Del(extentId)
		extentFilePath := path.Join(s.dataDir, strconv.FormatUint(extentId, 10))
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

func (s *ExtentStore) Sync(extentId uint64) (err error) {
	var extent *Extent
	if extent, err = s.getExtentWithHeader(extentId); err != nil {
		return
	}
	return extent.Flush()
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

	// Release delete index file
	s.deleteFp.Sync()
	s.deleteFp.Close()
	s.closed = true
}

func (s *ExtentStore) GetWatermark(extentId uint64, reload bool) (extentInfo *ExtentInfo, err error) {
	var (
		has    bool
		extent *Extent
	)
	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", s.getExtentKey(extentId))
		return
	}
	if reload {
		if extent, err = s.getExtentWithHeader(extentId); err != nil {
			return
		}
		extentInfo.FromExtent(extent)
	}
	return
}

func (s *ExtentStore) GetWatermarkForWrite(extentId uint64) (watermark int64, err error) {
	einfo, err := s.GetWatermark(extentId, false)
	if err != nil {
		return
	}
	watermark = int64(einfo.Size)
	if watermark%PageSize != 0 {
		watermark = watermark + (PageSize - watermark%PageSize)
	}

	return
}

func (s *ExtentStore) GetAllWatermark(filter ExtentFilter) (extents []*ExtentInfo, err error) {
	extents = make([]*ExtentInfo, 0)
	extentInfoSlice := make([]*ExtentInfo, 0, len(s.extentInfoMap))
	s.extentInfoMux.RLock()
	for _, extentId := range s.extentInfoMap {
		extentInfoSlice = append(extentInfoSlice, extentId)
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

func (s *ExtentStore) BackEndLoadExtent() {
	extentInfoSlice := make([]*ExtentInfo, 0, len(s.extentInfoMap))
	s.extentInfoMux.RLock()
	for _, extentId := range s.extentInfoMap {
		extentInfoSlice = append(extentInfoSlice, extentId)
	}
	s.extentInfoMux.RUnlock()
	for _, extentInfo := range extentInfoSlice {
		if extentInfo.Size == 0 {
			continue
		}
		if IsTinyExtent(extentInfo.FileId) {
			continue
		}
		e, err := s.getExtentWithHeader(extentInfo.FileId)
		if err != nil {
			continue
		}
		extentInfo.FromExtent(e)
		s.extentInfoMux.Lock()
		s.extentInfoMap[extentInfo.FileId] = extentInfo
		s.extentInfoMux.Unlock()
		time.Sleep(time.Millisecond * 5)
	}
	log.LogInfof("BackEnd Load datapartition (%v) success", s.dataDir)
	return
}

func (s *ExtentStore) ParseExtentId(filename string) (extentId uint64, isExtent bool) {
	if isExtent = RegexpExtentFile.MatchString(filename); !isExtent {
		return
	}
	var (
		err error
	)
	if extentId, err = strconv.ParseUint(filename, 10, 64); err != nil {
		isExtent = false
		return
	}
	isExtent = true
	return
}

func (s *ExtentStore) DeleteDirtyExtent(extentId uint64) (err error) {
	var (
		extent     *Extent
		extentInfo *ExtentInfo
		has        bool
	)

	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if !has {
		return nil
	}

	if extent, err = s.getExtentWithHeader(extentId); err != nil {
		return
	}
	if extent.Size() != 0 {
		return fmt.Errorf("size %v donnot zeor ", extent.Size())
	}

	extentInfo.FromExtent(extent)
	s.cache.Del(extent.ID())

	s.extentInfoMux.Lock()
	delete(s.extentInfoMap, extentId)
	s.extentInfoMux.Unlock()

	extentFilePath := path.Join(s.dataDir, strconv.FormatUint(extentId, 10))
	if err = os.Remove(extentFilePath); err != nil {
		return
	}

	return
}

/*
  this function is used for write Tiny File
*/

func (s *ExtentStore) initTinyExtent() (err error) {
	s.avaliTinyExtentCh = make(chan uint64, TinyExtentCount)
	s.unavaliTinyExtentCh = make(chan uint64, TinyExtentCount)
	var extentId uint64
	for extentId = TinyExtentStartId; extentId < TinyExtentStartId+TinyExtentCount; extentId++ {
		err = s.Create(extentId, 0)
		if err==nil || err==ErrorExtentHasExsit{
			err = nil
			s.unavaliTinyExtentCh <- extentId
			continue
		}
		return err
	}

	return
}

func (s *ExtentStore) GetAvaliTinyExtent() (extentId uint64, err error) {
	select {
	case extentId = <-s.avaliTinyExtentCh:
		return
	default:
		return 0, ErrorNoAvaliExtent

	}
}

func (s *ExtentStore) PutTinyExtentToAvaliCh(extentId uint64) {
	s.avaliTinyExtentCh <- extentId
}

func (s *ExtentStore) PutTinyExtentsToUnAvaliCh(extentIds []uint64) {
	for _, extentId := range extentIds {
		s.unavaliTinyExtentCh <- extentId
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
		extentId, err := s.GetAvaliTinyExtent()
		if err != nil {
			return
		}
		s.PutTinyExtentToUnavaliCh(extentId)
	}
}

func (s *ExtentStore) PutTinyExtentToUnavaliCh(extentId uint64) {
	s.unavaliTinyExtentCh <- extentId
}

func (s *ExtentStore) GetUnavaliTinyExtent() (extentId uint64, err error) {
	select {
	case extentId = <-s.unavaliTinyExtentCh:
		return
	default:
		return 0, ErrorNoUnAvaliExtent

	}
}
