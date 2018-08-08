// Copyright 2018 The ChuBao Authors.
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
	"errors"
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

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util"
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
)

var (
	RegexpExtentFile, _ = regexp.Compile("^(\\d)+$")
)

type ExtentFilter func(info *FileInfo) bool

// Filters
var (
	GetStableExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(info *FileInfo) bool {
			return now.Unix()-info.ModTime.Unix() > 30*60 && !info.Deleted && info.Size > 0
		}
	}
	GetEmptyExtentFilter = func() ExtentFilter {
		now := time.Now()
		return func(info *FileInfo) bool {
			return now.Unix()-info.ModTime.Unix() > 30*60 && !info.Deleted && info.Size == 0
		}
	}
)

type ExtentStore struct {
	dataDir       string
	baseExtentId  uint64
	extentInfoMap map[uint64]*FileInfo
	extentInfoMux sync.RWMutex
	cache         ExtentCache
	lock          sync.Mutex
	storeSize     int
	metaFp        *os.File
	deleteFp      *os.File
	closeC        chan bool
	closed        bool
}

func NewExtentStore(dataDir string, storeSize int) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataDir = dataDir
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
	s.extentInfoMap = make(map[uint64]*FileInfo, 40)
	s.cache = NewExtentCache(40)
	if err = s.initBaseFileId(); err != nil {
		err = fmt.Errorf("init base field ID: %v", err)
		return
	}
	s.storeSize = storeSize
	s.closeC = make(chan bool, 1)
	s.closed = false
	go s.cleanupScheduler()
	return
}

func (s *ExtentStore) DeleteStore() (err error) {
	s.cache.Clear()
	err = os.RemoveAll(s.dataDir)
	return
}

func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var (
		extentInfoSlice []*FileInfo
	)
	if extentInfoSlice, err = s.GetAllWatermark(GetStableExtentFilter()); err != nil {
		return
	}
	files = make([]*proto.File, 0, len(extentInfoSlice))
	for _, extentInfo := range extentInfoSlice {
		file := &proto.File{
			Name:      strconv.Itoa(extentInfo.FileId),
			Crc:       extentInfo.Crc,
			Size:      uint32(extentInfo.Size),
			MarkDel:   extentInfo.Deleted,
			NeedleCnt: 1,
		}
		files = append(files, file)
	}
	return
}

func (s *ExtentStore) NextExtentId() (extentId uint64) {
	return atomic.AddUint64(&s.baseExtentId, 1)
}

func (s *ExtentStore) Create(extentId uint64, inode uint64, overwrite bool) (err error) {
	var extent Extent
	name := path.Join(s.dataDir, strconv.Itoa(int(extentId)))
	if s.IsExistExtent(extentId) {
		if !overwrite {
			err = errors.New("extent already exist")
			return
		}
		if extent, err = s.getExtent(extentId); err != nil {
			return
		}
		extent.InitToFS(extentId, true)
	} else {
		extent = NewExtentInCore(name, extentId)
		if err = extent.InitToFS(inode, false); err != nil {
			return
		}
	}
	s.cache.Put(extent)

	extInfo := &FileInfo{}
	extInfo.FromExtent(extent)
	s.extentInfoMux.Lock()
	s.extentInfoMap[extentId] = extInfo
	s.extentInfoMux.Unlock()

	s.UpdateBaseExtentId(extentId)
	return
}

func (s *ExtentStore) UpdateBaseExtentId(id uint64) (err error) {
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

func (s *ExtentStore) getExtent(extentId uint64) (e Extent, err error) {
	var ok bool
	if e, ok = s.cache.Get(extentId); !ok {
		if e, err = s.loadExtentFromDisk(extentId); err != nil {
			err = fmt.Errorf("load extent from disk: %v", err)
			return
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

func (s *ExtentStore) loadExtentFromDisk(extentId uint64) (e Extent, err error) {
	name := path.Join(s.dataDir, strconv.Itoa(int(extentId)))
	e = NewExtentInCore(name, extentId)
	if err = e.RestoreFromFS(); err != nil {
		err = fmt.Errorf("restore from file system: %v", err)
		return
	}
	s.cache.Put(e)
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
	if TinyChunkCount > baseFileId {
		baseFileId = uint64(TinyChunkCount)
	}
	files, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return err
	}
	var (
		extentId   uint64
		isExtent   bool
		extent     Extent
		extentInfo *FileInfo
		loadErr    error
	)
	for _, f := range files {
		if extentId, isExtent = s.parseExtentId(f.Name()); !isExtent {
			continue
		}
		if extent, loadErr = s.getExtent(extentId); loadErr != nil {
			continue
		}
		extentInfo = &FileInfo{}
		extentInfo.FromExtent(extent)
		s.extentInfoMux.Lock()
		s.extentInfoMap[extentId] = extentInfo
		s.extentInfoMux.Unlock()
		if extentId > baseFileId {
			baseFileId = extentId
		}
	}
	atomic.StoreUint64(&s.baseExtentId, baseFileId)
	return nil
}

func (s *ExtentStore) Write(extentId uint64, offset, size int64, data []byte, crc uint32) (err error) {
	var (
		extentInfo *FileInfo
		has        bool
	)
	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", extentId)
		return
	}
	extent, err := s.getExtent(extentId)
	if err != nil {
		return err
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return err
	}
	if extent.IsMarkDelete() {
		return ErrorHasDelete
	}
	if err = extent.Write(data, offset, size, crc); err != nil {
		return
	}
	extentInfo.FromExtent(extent)
	return
}

func (s *ExtentStore) checkOffsetAndSize(offset, size int64) error {
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

func (s *ExtentStore) Read(extentId uint64, offset, size int64, nbuf []byte) (crc uint32, err error) {
	var extent Extent
	if extent, err = s.getExtent(extentId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if extent.IsMarkDelete() {
		err = ErrorHasDelete
		return
	}
	crc, err = extent.Read(nbuf, offset, size)
	return
}

func (s *ExtentStore) MarkDelete(extentId uint64) (err error) {
	var (
		extent     Extent
		extentInfo *FileInfo
		has        bool
	)

	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if !has {
		err = fmt.Errorf("extent %v not exist", extentId)
		s.extentInfoMux.RUnlock()
		return
	}

	if extent, err = s.getExtent(extentId); err != nil {
		return nil
	}
	if err = extent.MarkDelete(); err != nil {
		return
	}
	extentInfo.FromExtent(extent)

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

func (s *ExtentStore) cleanupScheduler() {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ticker.C:
			s.cleanup()
		case <-s.closeC:
			ticker.Stop()
			return
		}
	}
}

func (s *ExtentStore) cleanup() {

	extentInfoSlice, err := s.GetAllWatermark(GetEmptyExtentFilter())
	if err != nil {
		return
	}
	for _, extentInfo := range extentInfoSlice {
		if extentInfo.Size == 0 {
			s.MarkDelete(uint64(extentInfo.FileId))
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
		_, err = s.getExtent(extentId)
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
	var extent Extent
	if extent, err = s.getExtent(extentId); err != nil {
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
	close(s.closeC)

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

func (s *ExtentStore) GetWatermark(extentId uint64, reload bool) (extentInfo *FileInfo, err error) {
	var (
		has    bool
		extent Extent
	)
	s.extentInfoMux.RLock()
	extentInfo, has = s.extentInfoMap[extentId]
	s.extentInfoMux.RUnlock()
	if  !has {
		err = fmt.Errorf("extent %v not exist", extentId)
		return
	}
	if reload {
		if extent, err = s.getExtent(extentId); err != nil {
			return
		}
		extentInfo.FromExtent(extent)
	}
	return
}

func (s *ExtentStore) GetAllWatermark(filter ExtentFilter) (extents []*FileInfo, err error) {
	extents = make([]*FileInfo, 0)
	extentInfoSlice := make([]*FileInfo, 0, len(s.extentInfoMap))
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

func (s *ExtentStore) parseExtentId(filename string) (extentId uint64, isExtent bool) {
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
	isExtent = extentId > TinyChunkCount
	return
}

func (s *ExtentStore) UsedSize() (size int64) {
	if fInfoArray, err := ioutil.ReadDir(s.dataDir); err == nil {
		for _, fInfo := range fInfoArray {
			if fInfo.IsDir() {
				continue
			}
			if _, isExtent := s.parseExtentId(fInfo.Name()); !isExtent {
				continue
			}
			size += fInfo.Size()
		}
	}
	return
}

func (s *ExtentStore) GetDelObjects() (extents []uint64) {
	extents = make([]uint64, 0)
	var (
		offset   int64
		extendId uint64
	)
	for {
		buf := make([]byte, util.MB*10)
		read, err := s.deleteFp.ReadAt(buf, offset)
		if read == 0 {
			break
		}
		offset += int64(read)
		byteBuf := bytes.NewBuffer(buf[:read])
		for {
			if err := binary.Read(byteBuf, binary.BigEndian, &extendId); err != nil {
				break
			}
			extents = append(extents, extendId)
		}
		if err == io.EOF || read == 0 {
			break
		}
	}
	return
}
