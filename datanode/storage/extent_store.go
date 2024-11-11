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
	"io/fs"
	"math/rand"
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
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/cubefs/cubefs/util/strutil"
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
	CacheFlushMinInterval        = 5 * time.Minute
	CacheFlushMaxInterval        = 15 * time.Minute
	ExtentReadDirHint            = "READDIR_HINT"
	ExtentReadDirHintV2          = "READDIR_HINT_V2"
	ExtentReadDirHintTemp        = "READDIR_HINT.tmp"

	StaleExtStoreBackupSuffix = ".old"
	StaleExtStoreTimeFormat   = "20060102150405.000000000"
)

var ErrStoreAlreadyClosed = errors.New("extent store already closed")

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
	eiMutex                sync.RWMutex
	cache                  *ExtentCache // extent cache
	mutex                  sync.Mutex
	storeSize              int      // size of the extent store
	metadataFp             *os.File // metadata file pointer?
	tinyExtentDeleteFp     *os.File
	normalExtentDeleteFp   *os.File
	closed                 int32
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
	extentLockMap                     map[uint64]proto.GcFlag
	elMutex                           sync.RWMutex
	extentLock                        bool
	stopMutex                         sync.RWMutex
	stopC                             chan interface{}
	ApplyId                           uint64
}

func MkdirAll(name string) (err error) {
	return os.MkdirAll(name, 0o755)
}

func NewExtentStore(dataDir string, partitionID uint64, storeSize, dpType, cap int, isCreate bool) (s *ExtentStore, err error) {
	begin := time.Now()
	defer func() {
		log.LogInfof("[NewExtentStore] load dp(%v) new extent store using time(%v)", partitionID, time.Since(begin))
	}()
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

	stat, err := s.tinyExtentDeleteFp.Stat()
	if err != nil {
		return
	}
	if stat.Size()%DeleteTinyRecordSize != 0 {
		needWriteEmpty := DeleteTinyRecordSize - (stat.Size() % DeleteTinyRecordSize)
		data := make([]byte, needWriteEmpty)
		s.tinyExtentDeleteFp.Write(data)
		log.LogInfof("[NewExtentStore] load dp(%v) write zero buffer", partitionID)
	}

	s.extentInfoMap = make(map[uint64]*ExtentInfo)
	s.extentLockMap = make(map[uint64]proto.GcFlag)
	s.cache = NewExtentCache(cap)
	if err = s.initBaseFileID(); err != nil {
		err = fmt.Errorf("init base field ID: %v", err)
		return
	}
	s.hasAllocSpaceExtentIDOnVerfiyFile = s.GetPreAllocSpaceExtentIDOnVerifyFile()
	s.storeSize = storeSize
	s.closed = 0
	err = s.initTinyExtent()
	if err != nil {
		return
	}
	s.stopC = make(chan interface{})
	go func() {
		time.Sleep(15 * time.Minute)
		s.startFlushCache()
	}()
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

	log.LogDebugf("action[ExtentInfo.UpdateExtentInfo] ei info [%v]", ei.String())

	if !IsTinyExtent(ei.FileID) {
		atomic.StoreUint32(&ei.Crc, crc)
		ei.ModifyTime = extent.ModifyTime()
	}
}

// SnapShot returns the information of all the extents on the current data partition.
// When the master sends the loadDataPartition request, the snapshot is used to compare the replicas.
func (s *ExtentStore) SnapShot() (files []*proto.File, err error) {
	var normalExtentSnapshot, tinyExtentSnapshot []*ExtentInfo
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

func (s *ExtentStore) startFlushCache() {
	// NOTE: flush extents in cache
	randGen := rand.New(rand.NewSource(time.Now().UnixMicro()))
	for {
		select {
		case <-s.stopC:
			return
		default:
			log.LogInfof("[startFlushCache] flush extent cache")
			s.cache.CopyAndFlush(30 * time.Second)
		}
		tmp := randGen.Int63n(int64(CacheFlushMaxInterval - CacheFlushMinInterval))
		randSleep := time.Duration(tmp) + CacheFlushMinInterval
		log.LogDebugf("[startFlushCache] startFlushCache sleep time(%v)", randSleep)
		time.Sleep(randSleep)
	}
}

func (s *ExtentStore) IsClosed() (closed bool) {
	closed = atomic.LoadInt32(&s.closed) == 1
	return
}

func (s *ExtentStore) setClosed(v bool) {
	closed := int32(0)
	if v {
		closed = 1
	}
	atomic.StoreInt32(&s.closed, closed)
}

// Create creates an extent.
func (s *ExtentStore) Create(extentID uint64) (err error) {
	s.stopMutex.RLock()
	defer s.stopMutex.RUnlock()

	if s.IsClosed() {
		err = ErrStoreAlreadyClosed
		log.LogErrorf("[Create] store(%v) failed to create extent(%v), err(%v)", s.dataPath, extentID, err)
		return
	}

	var e *Extent
	name := path.Join(s.dataPath, strconv.Itoa(int(extentID)))
	if s.HasExtent(extentID) {
		err = ExtentExistsError
		return err
	}

	stat.RecordStat(s.partitionID, "Create", s.dataPath)

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

func (s *ExtentStore) GetExtentInfoFromDisk(id uint64) (ei *ExtentInfo, err error) {
	retry := 0
	const maxRetry = 3

	for retry < maxRetry {
		var stat fs.FileInfo
		name := path.Join(s.dataPath, fmt.Sprint(id))
		stat, err = os.Stat(name)
		if err != nil {
			retry++
			continue
		}

		ino := stat.Sys().(*syscall.Stat_t)
		ei = &ExtentInfo{
			FileID:          id,
			Size:            uint64(stat.Size()),
			Crc:             0,
			IsDeleted:       false,
			AccessTime:      time.Unix(int64(ino.Atim.Sec), int64(ino.Atim.Nsec)).Unix(),
			ModifyTime:      stat.ModTime().Unix(),
			Source:          "",
			SnapshotDataOff: util.ExtentSize,
		}
		if IsTinyExtent(id) {
			watermark := ei.Size
			if watermark%util.PageSize != 0 {
				watermark = watermark + (util.PageSize - watermark%util.PageSize)
			}
			ei.Size = watermark
		}
		// NOTE: init snapshot offset
		if !IsTinyExtent(id) {
			if stat.Size() > int64(ei.SnapshotDataOff) {
				ei.SnapshotDataOff = uint64(stat.Size())
			}
		}
		return
	}
	return
}

func (s *ExtentStore) GetExtentInfo(id uint64) (ei *ExtentInfo, ok bool) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()
	ei, ok = s.extentInfoMap[id]
	return
}

func (s *ExtentStore) SetExtentInfo(id uint64, ei *ExtentInfo) {
	s.eiMutex.Lock()
	defer s.eiMutex.Unlock()
	s.extentInfoMap[id] = ei
}

func (s *ExtentStore) RangeExtentInfo(iter func(id uint64, ei *ExtentInfo) (ok bool, err error)) (err error) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()

	var ok bool
	for id, v := range s.extentInfoMap {
		ok, err = iter(id, v)
		if err != nil || !ok {
			return
		}
	}
	return
}

func (s *ExtentStore) DeleteExtentInfo(id uint64) {
	s.eiMutex.Lock()
	defer s.eiMutex.Unlock()

	stat.RecordStat(s.partitionID, "DeleteExtentInfo", s.dataPath)
	delete(s.extentInfoMap, id)
}

func (s *ExtentStore) GetExtentInfoCount() (count int) {
	s.eiMutex.RLock()
	defer s.eiMutex.RUnlock()
	count = len(s.extentInfoMap)
	return
}

func (s *ExtentStore) writeReadDirHint() (err error) {
	begin := time.Now()
	defer func() {
		log.LogInfof("[writeReadDirHint] store(%v) write dir hint extent cnt(%v) using time(%v), slow(%v)", s.dataPath, s.GetExtentCount(), time.Since(begin), time.Since(begin) > 100*time.Millisecond)
	}()

	hintTempPath := path.Join(s.dataPath, ExtentReadDirHintTemp)
	hintPath := path.Join(s.dataPath, ExtentReadDirHintV2)
	buff := bytes.NewBuffer([]byte{})
	err = s.RangeExtentInfo(func(id uint64, ei *ExtentInfo) (ok bool, err error) {
		err = ei.MarshalBinaryWithBuffer(buff)
		if err != nil {
			return
		}
		return true, nil
	})
	if err != nil {
		log.LogErrorf("[writeReadDirHint] store(%v) failed to marshal hint, err(%v)", s.dataPath, err)
		return
	}
	if err = os.WriteFile(hintTempPath, buff.Bytes(), 0o666); err != nil {
		log.LogErrorf("[writeReadDirHint] store(%v) failed to write readdir hint, err(%v)", s.dataPath, err)
		return
	}
	err = os.Rename(hintTempPath, hintPath)
	if err != nil {
		log.LogErrorf("[writeReadDirHint] store(%v) failed to rename readdir hint, err(%v)", s.dataPath, err)
		return
	}
	return
}

func (s *ExtentStore) readReadDirHint() (extMap map[uint64]*ExtentInfo, err error) {
	var data []byte
	begin := time.Now()
	defer func() {
		size := 0
		cnt := 0
		if data != nil {
			size = len(data)
		}
		if extMap != nil {
			cnt = len(extMap)
		}
		slow := time.Since(begin) > 1*time.Second
		log.LogInfof("[readReadDirHint] store(%v) read hint file using time(%v), read size(%v), cnt(%v), slow(%v)", s.dataPath, time.Since(begin), size, cnt, slow)
	}()

	hintPath := path.Join(s.dataPath, ExtentReadDirHintV2)
	data, err = os.ReadFile(hintPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		log.LogErrorf("[readReadDirHint] store(%v) failed to read hint file, err(%v)", s.dataPath, err)
		return
	}
	extMap = make(map[uint64]*ExtentInfo)
	buff := bytes.NewBuffer(data)
	for buff.Len() != 0 {
		ei := &ExtentInfo{}
		err = ei.UnmarshalBinaryWithBuffer(buff)
		if err != nil {
			log.LogErrorf("[readReadDirHint] store(%v) failed to unmarshal hint, err(%v)", s.dataPath, err)
			return
		}
		extMap[ei.FileID] = ei
	}
	return
}

func (s *ExtentStore) removeReadDirHint() (err error) {
	hintPath := path.Join(s.dataPath, ExtentReadDirHintV2)
	err = os.Remove(hintPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		log.LogErrorf("[removeReadDirHint] store(%v) failed to remove read dir hint(%v), err(%v)", s.dataPath, hintPath, err)
		return
	}
	// NOTE: delete old version
	hintPath = path.Join(s.dataPath, ExtentReadDirHint)
	err = os.Remove(hintPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		log.LogErrorf("[removeReadDirHint] store(%v) failed to remove read dir hint(%v), err(%v)", s.dataPath, hintPath, err)
		return
	}
	return
}

func (s *ExtentStore) initBaseFileID() error {
	var extNum int
	begin := time.Now()
	defer func() {
		log.LogInfof("[initBaseFileID] store(%v) init base file id using time(%v), count(%v)", s.dataPath, time.Since(begin), extNum)
	}()
	var baseFileID uint64
	baseFileID, _ = s.GetPersistenceBaseExtentID()
	log.LogInfof("[initBaseFileID] store(%v) init base file to persistence base extent id using time(%v)", s.dataPath, time.Since(begin))

	// NOTE: try to read hint
	var err error
	var extMap map[uint64]*ExtentInfo
	extMap, err = s.readReadDirHint()
	if err != nil {
		log.LogErrorf("[initBaseFileID] store(%v) failed to read hint, err(%v)", s.dataPath, err)
	}
	// NOTE: remove hint
	if err = s.removeReadDirHint(); err != nil {
		log.LogErrorf("[initBaseFileID] store(%v) failed to remove hint, err(%v)", s.dataPath, err)
		return err
	}

	if len(extMap) != 0 {
		log.LogInfof("[initBaseFileID] store(%v) init base file to read hint using time(%v)", s.dataPath, time.Since(begin))
		// NOTE: fast path
		for id := range extMap {
			if !IsTinyExtent(id) && id > baseFileID {
				baseFileID = id
			}
		}
		s.extentInfoMap = extMap
	} else {
		// NOTE: slow path
		files, err := fileutil.ReadDir(s.dataPath)
		if err != nil {
			return err
		}
		log.LogInfof("[initBaseFileID] store(%v) init base file to read dir using time(%v)", s.dataPath, time.Since(begin))

		var ei *ExtentInfo
		for _, f := range files {
			extentID, isExtent := s.ExtentID(f)
			if !isExtent {
				continue
			}

			extNum++
			ei, err = s.GetExtentInfoFromDisk(extentID)
			if err != nil {
				log.LogErrorf("[initBaseFileID] store(%v) failed to load extent(%v), err(%v)", s.dataPath, extentID, err)
				return err
			}
			s.extentInfoMap[extentID] = ei

			if !IsTinyExtent(extentID) && extentID > baseFileID {
				baseFileID = extentID
			}
		}
	}
	log.LogInfof("[initBaseFileID] store(%v) init base file to load loop using time(%v)", s.dataPath, time.Since(begin))
	if baseFileID < MinExtentID {
		baseFileID = MinExtentID
	}
	atomic.StoreUint64(&s.baseExtentID, baseFileID)
	log.LogInfof("datadir(%v) maxBaseId(%v)", s.dataPath, baseFileID)
	return nil
}

// Write writes the given extent to the disk.
func (s *ExtentStore) Write(param *WriteParam) (status uint8, err error) {
	s.stopMutex.RLock()
	defer s.stopMutex.RUnlock()
	if s.IsClosed() {
		err = ErrStoreAlreadyClosed
		log.LogErrorf("[Write] store(%v) failed to write param(%v), err(%v)", s.dataPath, param, err)
		return
	}

	var (
		e  *Extent
		ei *ExtentInfo
	)

	s.elMutex.RLock()
	if param.IsBackupWrite {
		// NOTE: meet an error is impossible
		_, ok := s.GetExtentInfo(param.ExtentID)
		if !ok {
			s.elMutex.RUnlock()
			err = fmt.Errorf("extent(%v) is not locked", param.ExtentID)
			log.LogErrorf("[Write] gc_extent[%d] is not locked", param.ExtentID)
			return
		}
	} else {
		if s.extentLock {
			if flag, ok := s.extentLockMap[param.ExtentID]; ok {
				log.LogErrorf("[Write] gc_extent_lock[%d] is locked, path %s", param.ExtentID, s.dataPath)
				if flag == proto.GcDeleteFlag {
					s.elMutex.RUnlock()
					err = fmt.Errorf("extent(%v) is locked", param.ExtentID)
					return
				}
			}
		}
	}
	s.elMutex.RUnlock()

	s.eiMutex.Lock()
	status = proto.OpOk
	ei = s.extentInfoMap[param.ExtentID]
	e, err = s.extentWithHeader(ei)
	s.eiMutex.Unlock()
	if err != nil {
		return status, err
	}
	// update access time
	atomic.StoreInt64(&ei.AccessTime, time.Now().Unix())
	log.LogDebugf("action[Write] dp %v write param(%v)", s.partitionID, param)
	if err = s.checkOffsetAndSize(param); err != nil {
		log.LogInfof("action[Write] path %v err %v", e.filePath, err)
		return status, err
	}

	op := "Write"
	if param.IsRepair {
		op = "WriteRepair"
	}
	stat.RecordStat(s.partitionID, op, s.dataPath)

	status, err = e.Write(param, s.PersistenceBlockCrc)
	if err != nil {
		log.LogInfof("action[Write] path %v err %v", e.filePath, err)
		return status, err
	}

	ei.UpdateExtentInfo(e, 0)
	return status, nil
}

func (s *ExtentStore) checkOffsetAndSize(param *WriteParam) error {
	if IsTinyExtent(param.ExtentID) {
		return nil
	}
	// random write pos can happen on modAppend partition of extent
	if param.WriteType == RandomWriteType {
		return nil
	}
	if param.WriteType == AppendRandomWriteType {
		if param.Offset < util.ExtentSize {
			return newParameterError("Write param error(%v)", param)
		}
		return nil
	}
	if param.Size == 0 ||
		param.Offset >= util.BlockCount*util.BlockSize ||
		param.Offset+param.Size > util.BlockCount*util.BlockSize {
		return newParameterError("offset=%d size=%d", param.Offset, param.Size)
	}
	if !param.IsRepair && param.Size > util.BlockSize {
		return newParameterError("offset=%d size=%d", param.Offset, param.Size)
	}
	return nil
}

// IsTinyExtent checks if the given extent is tiny extent.
func IsTinyExtent(extentID uint64) bool {
	return extentID >= TinyExtentStartID && extentID < TinyExtentStartID+TinyExtentCount
}

// Read reads the extent based on the given id.
func (s *ExtentStore) Read(extentID uint64, offset, size int64, nbuf []byte, isRepairRead bool, isBackupRead bool) (crc uint32, err error) {
	var e *Extent
	begin := time.Now()
	log.LogDebugf("[Read] dp %v extent[%d] offset[%d] size[%d] isRepairRead[%v] extentLock[%v]",
		s.partitionID, extentID, offset, size, isRepairRead, s.extentLock)
	defer func() {
		if log.EnableDebug() {
			log.LogDebugf("[Read] dp %v extent[%d] offset[%d] size[%d] isRepairRead[%v] extentLock[%v] cost %v",
				s.partitionID, extentID, offset, size, isRepairRead, s.extentLock, time.Since(begin).String())
		}
	}()

	ei, _ := s.GetExtentInfo(extentID)
	if ei == nil {
		return 0, errors.Trace(ExtentHasBeenDeletedError, "[Read] dp %v extent[%d] is already been deleted", s.partitionID, extentID)
	}

	s.elMutex.RLock()
	if isBackupRead {
		if _, ok := s.extentLockMap[extentID]; !ok {
			s.elMutex.RUnlock()
			err = fmt.Errorf("extent(%v) is not locked", extentID)
			log.LogErrorf("[Read]dp %v gc_extent_no_lock[%d] is not locked", s.partitionID, extentID)
			return
		}
	} else {
		if s.extentLock {
			if _, ok := s.extentLockMap[extentID]; ok && !isRepairRead {
				log.LogErrorf("[Read]dp %v gc_extent_lock[%d] is locked， should not be read.", s.partitionID, extentID)
			}
		}
	}
	s.elMutex.RUnlock()

	// update extent access time
	atomic.StoreInt64(&ei.AccessTime, time.Now().Unix())

	if e, err = s.extentWithHeader(ei); err != nil {
		return
	}

	op := "Read"
	if isRepairRead {
		op = "ReadRepair"
	}
	stat.RecordStat(s.partitionID, op, s.dataPath)

	begin2 := time.Now()
	log.LogDebugf("[Read]dp %v extent %v offset %v size %v  ei.Size %v e.dataSize %v isRepairRead %v",
		s.partitionID, extentID, offset, size, ei.Size, e.dataSize, isRepairRead)
	crc, err = e.Read(nbuf, offset, size, isRepairRead)
	if log.EnableDebug() {
		log.LogDebugf("[Read]dp %v extent %v offset %v size %v  ei.Size %v e.dataSize %v isRepairRead %v,cost %v",
			s.partitionID, extentID, offset, size, ei.Size, e.dataSize, isRepairRead, time.Since(begin2).String())
	}

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

func (s *ExtentStore) CanGcDelete(extId uint64) bool {
	ei, _ := s.GetExtentInfo(extId)
	if ei == nil || ei.IsDeleted {
		return true
	}

	s.elMutex.RLock()
	defer s.elMutex.RUnlock()

	flag, ok := s.extentLockMap[extId]
	return ok && flag == proto.GcDeleteFlag
}

func (s *ExtentStore) GetGcFlag(extId uint64) proto.GcFlag {
	s.elMutex.RLock()
	defer s.elMutex.RUnlock()

	flag, ok := s.extentLockMap[extId]
	if !ok {
		return proto.GcNormal
	}
	return flag
}

// MarkDelete marks the given extent as deleted.
func (s *ExtentStore) MarkDelete(extentID uint64, offset, size int64) (err error) {
	s.stopMutex.RLock()
	defer s.stopMutex.RUnlock()

	if s.IsClosed() {
		err = ErrStoreAlreadyClosed
		log.LogErrorf("[MarkDelete] store(%v) failed to mark delete extent(%v), err(%v)", s.dataPath, extentID, err)
		return
	}

	var ei *ExtentInfo

	if IsTinyExtent(extentID) {
		return s.punchDelete(extentID, offset, size)
	}

	ei, _ = s.GetExtentInfo(extentID)
	if err != nil {
		log.LogErrorf("[MarkDelete] failed to mark delete extent(%v), err(%v)", extentID, err)
		return
	}
	if ei == nil || ei.IsDeleted {
		return
	}
	log.LogDebugf("action[MarkDelete] extentID %v offset %v size %v ei(size %v snapshotSize %v)",
		extentID, offset, size, ei.Size, ei.SnapshotDataOff)

	funcNeedPunchDel := func() bool {
		if offset != 0 {
			return true
		}
		if size != 0 {
			if ei.Size != uint64(size) && ei.SnapshotDataOff == util.ExtentSize {
				return true
			}

			if ei.SnapshotDataOff != uint64(size) && ei.SnapshotDataOff > util.ExtentSize {
				return true
			}
		}
		return false
	}

	stat.RecordStat(s.partitionID, "MarkDelete", s.dataPath)

	log.LogInfof("[MarkDelete] store(%v) mark del extent(%v) offset(%v) size(%v), ei size(%v) ei snapshotOff(%v), tiny(%v), funcNeedPunchDel(%v)", s.dataPath, extentID, offset, size, ei.Size, ei.SnapshotDataOff, IsTinyExtent(extentID), funcNeedPunchDel())
	if IsTinyExtent(extentID) || funcNeedPunchDel() {
		log.LogDebugf("action[MarkDelete] extentID %v offset %v size %v ei(size %v snapshotSize %v), tiny(%v), snapshot punch(%v)",
			extentID, offset, size, ei.Size, ei.SnapshotDataOff, IsTinyExtent(extentID), funcNeedPunchDel())
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

func (s *ExtentStore) Flush() {
	begin := time.Now()
	defer func() {
		log.LogInfof("[Flush] flush extent store using time(%v)", time.Since(begin))
	}()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.IsClosed() {
		return
	}
	s.cache.Flush()
}

// Close closes the extent store.
func (s *ExtentStore) Close() {
	begin := time.Now()
	defer func() {
		log.LogInfof("[Close] store(%v) close extent store using time(%v), slow(%v)", s.dataPath, time.Since(begin), time.Since(begin) > 100*time.Millisecond)
	}()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.IsClosed() {
		return
	}
	close(s.stopC)

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

	s.stopMutex.Lock()
	defer s.stopMutex.Unlock()
	s.setClosed(true)
	if err := s.writeReadDirHint(); err != nil {
		log.LogErrorf("[Close] store(%v) failed to write extent hint, err(%v)", s.dataPath, err)
	}
}

// Watermark returns the extent info of the given extent on the record.
func (s *ExtentStore) Watermark(extentID uint64) (ei *ExtentInfo, err error) {
	var has bool
	ei, has = s.GetExtentInfo(extentID)
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

func (s *ExtentStore) getFileDiskUsed(name string) (size int64, err error) {
	stat := syscall.Stat_t{}
	err = syscall.Stat(name, &stat)
	if err != nil {
		return
	}
	size = stat.Blocks * DiskSectorSize
	return
}

func (s *ExtentStore) GetStoreUsedSize() (used int64) {
	extentInfoSlice := make([]*ExtentInfo, 0, s.GetExtentCount())
	s.eiMutex.RLock()
	for _, extentID := range s.extentInfoMap {
		extentInfoSlice = append(extentInfoSlice, extentID)
	}
	s.eiMutex.RUnlock()
	tinyTotal := uint64(0)
	normalTotal := uint64(0)
	for _, einfo := range extentInfoSlice {
		if einfo.IsDeleted {
			continue
		}
		if IsTinyExtent(einfo.FileID) {
			size, err := s.getFileDiskUsed(path.Join(s.dataPath, strconv.FormatInt(int64(einfo.FileID), 10)))
			if err != nil {
				log.LogErrorf("[GetStoreUsedSize] store(%v) failed to get tiny extent(%v) disk used", s.dataPath, einfo.FileID)
				continue
			}
			if log.EnableDebug() {
				log.LogDebugf("[GetStoreUsedSize] store(%v) tiny extent(%v) size(%v) raw(%v)", s.dataPath, einfo.FileID, strutil.FormatSize(uint64(size)), size)
			}
			used += size
			tinyTotal += uint64(size)
		} else {
			// NOTE: for debug
			size := int64(einfo.TotalSize())
			if log.EnableDebug() {
				if size < 0 {
					log.LogErrorf("[GetStoreUsedSize] store(%v) extent(%v) size(%v) is < 0, extent size(%v) snap off(%v)", s.dataPath, einfo.FileID, size, einfo.Size, einfo.SnapshotDataOff)
				}
				actualSize, err := s.getFileDiskUsed(path.Join(s.dataPath, strconv.FormatInt(int64(einfo.FileID), 10)))
				if err != nil {
					if os.IsNotExist(err) {
						log.LogInfof("[GetStoreUsedSize] store(%v) extent(%v) already be deleted, skip", s.dataPath, einfo.FileID)
						continue
					}
					log.LogErrorf("[GetStoreUsedSize] store(%v) failed to get normal extent(%v) disk used, err(%v)", s.dataPath, einfo.FileID, err)
					continue
				}
				log.LogDebugf("[GetStoreUsedSize] store(%v) normal extent(%v) size(%v) raw(%v), actual size(%v) raw(%v)", s.dataPath, einfo.FileID, strutil.FormatSize(uint64(size)), size, strutil.FormatSize(uint64(actualSize)), actualSize)
				if actualSize < size {
					log.LogWarnf("[GetStoreUsedSize] store(%v) normal extent(%v) actual size(%v), size(%v) einfo size(%v) snapshot off(%v)", s.dataPath, einfo.FileID, strutil.FormatSize(uint64(actualSize)), strutil.FormatSize(uint64(size)), strutil.FormatSize(einfo.Size), strutil.FormatSize(einfo.SnapshotDataOff))
				}
			}
			used += size
			normalTotal += uint64(size)
		}
	}
	if log.EnableInfo() {
		log.LogInfof("[GetStoreUsedSize] store(%v) total size(%v) raw(%v) tiny total(%v) raw(%v) normal total(%v) raw(%v)", s.dataPath, strutil.FormatSize(uint64(used)), used, strutil.FormatSize(tinyTotal), tinyTotal, strutil.FormatSize(normalTotal), normalTotal)
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
	var extentID uint64
	for extentID = TinyExtentStartID; extentID < TinyExtentCount+TinyExtentStartID; extentID++ {
		var ei *ExtentInfo
		ei, _ = s.GetExtentInfo(extentID)
		if ei == nil {
			continue
		}
		extents = append(extents, ei)
	}
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
	begin := time.Now()
	defer func() {
		log.LogInfof("[initTinyExtent] init tiny extent using time(%v)", time.Since(begin))
	}()
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
		log.LogDebugf("GetMaxExtentIDAndPartitionSize dp %v add extentInfo %v size %v", s.partitionID,
			extentInfo.FileID, extentInfo.TotalSize())
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
	_, exist = s.GetExtentInfo(extentID)
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
		if strings.Contains(err.Error(), ExtentNotFoundError.Error()) {
			s.DeleteExtentInfo(extentID)
			log.LogWarnf("LoadExtentFromDisk. partition id %v delete missed extentId %v",
				s.partitionID, extentID)
		}
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
	ei, _ := s.GetExtentInfo(extentID)
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
		if ei == nil {
			continue
		}

		if !IsTinyExtent(ei.FileID) && time.Now().Unix()-ei.ModifyTime > UpdateCrcInterval &&
			!ei.IsDeleted && ei.Size > 0 && ei.Crc == 0 {

			e, err := s.extentWithHeader(ei)
			if err != nil {
				log.LogWarnf("[autoComputeExtentCrc] get extent error:%+v", err)
				continue
			}
			extSize := e.Size()
			if e.snapshotDataOff > util.ExtentSize {
				extSize = int64(e.snapshotDataOff)
			}
			extentCrc, err := e.autoComputeExtentCrc(extSize, s.PersistenceBlockCrc)
			if err != nil {
				log.LogError("[autoComputeExtentCrc] compute crc fail", err)
				continue
			}
			ei.ApplySize = extSize
			ei.UpdateExtentInfo(e, extentCrc)
			atomic.StoreUint64(&ei.ApplyID, s.ApplyId)
			time.Sleep(time.Millisecond * 100)
		}
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

	ei, _ = s.GetExtentInfo(extentID)
	if e, err = s.extentWithHeader(ei); err != nil {
		return nil
	}
	log.LogDebugf("[TinyExtentRecover] dp %v extent %v offset %v size %v: ei.Size %v cache.dataSize %v",
		s.partitionID, extentID, offset, size, ei.Size, e.dataSize)
	if err = e.TinyExtentRecover(data, offset, size, crc, isEmptyPacket); err != nil {
		return err
	}
	ei.UpdateExtentInfo(e, 0)

	return nil
}

func (s *ExtentStore) TinyExtentGetFinfoSize(extentID uint64) (size uint64, err error) {
	var e *Extent
	if !IsTinyExtent(extentID) {
		return 0, fmt.Errorf("unavali extent id (%v)", extentID)
	}
	ei, _ := s.GetExtentInfo(extentID)
	if err != nil {
		log.LogErrorf("[TinyExtentGetFinfoSize] failed to get extent(%v) info, err(%v)", extentID, err)
		return
	}
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

func (s *ExtentStore) GetAllExtents(beforeTime int64) (extents []*ExtentInfo, err error) {
	start := time.Now()
	files, err := os.ReadDir(s.dataPath)
	if err != nil {
		log.LogErrorf("GetAllExtents: read dir extents failed, path %s, err %s", s.dataPath, err.Error())
		return nil, err
	}

	extents = make([]*ExtentInfo, 0, len(files))
	for _, f := range files {
		extId, isExtent := s.ExtentID(f.Name())
		if !isExtent {
			continue
		}

		ifo, err1 := f.Info()
		if err1 != nil {
			log.LogWarnf("GetAllExtents: get extent info failed, path %s, ext %s, err %s", s.dataPath, f.Name(), err1.Error())
			continue
		}

		modTime := ifo.ModTime().Unix()
		if modTime >= beforeTime {
			continue
		}

		extents = append(extents, &ExtentInfo{
			FileID:     extId,
			Size:       uint64(ifo.Size()),
			ModifyTime: modTime,
		})
	}

	log.LogWarnf("GetAllExtents: path:%v, beforeTime:%v, extents len:%v, cost %d",
		s.dataPath, beforeTime, len(extents), time.Since(start).Milliseconds())

	return
}

func (s *ExtentStore) ExtentBatchLockNormalExtent(gcLockEks *proto.GcLockExtents) (err error) {
	s.elMutex.Lock()
	s.extentLock = true
	s.elMutex.Unlock()

	s.elMutex.Lock()
	defer s.elMutex.Unlock()

	if gcLockEks.IsCreate {
		for _, e := range gcLockEks.Eks {
			s.extentLockMap[e.ExtentId] = gcLockEks.Flag
			log.LogDebugf("[ExtentBatchLockNormalExtent] lock extent(%v)", e.ExtentId)
		}
		return nil
	}

	// return all extents
	exts, err := s.GetAllExtents(time.Now().Unix() + 1000)
	if err != nil {
		return err
	}

	extMap := make(map[uint64]*ExtentInfo, len(exts))
	for _, e := range exts {
		extMap[e.FileID] = e
	}

	for _, e := range gcLockEks.Eks {
		extent, ok := extMap[e.ExtentId]
		if !ok {
			log.LogWarnf("[ExtentBatchLockNormalExtent] extent already not exist %s, eId %d", s.dataPath, e.ExtentId)
			continue
		}

		if e.Size != uint32(extent.Size) {
			err = fmt.Errorf("extent size not match, path %s, extentID(%v), extentSize(%v), extentKeySize(%v)",
				s.dataPath, e.ExtentId, extent.Size, e.Size)
			log.LogErrorf("[ExtentBatchLockNormalExtent] msg %s", err.Error())
			return err
		}

		s.extentLockMap[e.ExtentId] = gcLockEks.Flag
		if log.EnableDebug() {
			log.LogDebugf("[ExtentBatchLockNormalExtent] path %s, lock extent(%v)", s.dataPath, e.ExtentId)
		}
	}
	return
}

func (s *ExtentStore) ExtentBatchUnlockNormalExtent(ext []*proto.ExtentKey) {
	s.elMutex.Lock()
	defer s.elMutex.Unlock()

	s.extentLockMap = make(map[uint64]proto.GcFlag)
	s.extentLock = false
}

func (s *ExtentStore) GetExtentCountWithoutLock() (count int) {
	return len(s.extentInfoMap)
}
