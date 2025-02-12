// Copyright 2024 The CubeFS Authors.
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

package store

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
	"github.com/cubefs/cubefs/blobstore/blobnode/sys"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
)

const (
	defaultFreeSliceSplitMapNum = 64
)

type Store interface {
	Load(ctx context.Context) error
	// LayoutInfo:  return layout info
	// LayoutInfo()
	Format(ctx context.Context, dm core.DiskMeta) error
	LoadFormat(ctx context.Context) core.DiskMeta
	UpdateFormatInfo(ctx context.Context, diskID proto.DiskID, dm core.DiskMeta) error
	// OpenChunk return chunk with specified vuid and chunkID,
	// it may create new chunk when specified chunk ID not exist
	OpenChunk(ctx context.Context, chunkID clustermgr.ChunkID, chunkSize int64) (ChunkHandler, error)
	// GetChunkMeta return chunk meta info with specified chunkID,
	// return os.ErrNotExist when chunk not exist
	GetChunkMeta(ctx context.Context, chunkID clustermgr.ChunkID) (core.VuidMeta, error)
	// UpdateChunkMeta save chunk meta into persistence after chunk has been used
	UpdateChunkMeta(ctx context.Context, chunkID clustermgr.ChunkID, vm core.VuidMeta) error
	// GetVuidBind return the bind chunkID on this vuid
	GetVuidBind(ctx context.Context, vuid proto.Vuid) (id clustermgr.ChunkID, err error)
	// DeleteChunk delete chunk data and meta
	DeleteChunk(ctx context.Context, id clustermgr.ChunkID) (err error)
	// ListChunkMetas return all chunk meta info
	ListChunkMetas(ctx context.Context) (chunks map[clustermgr.ChunkID]core.VuidMeta, err error)
	// ListVuidMetas return all vuid meta info
	ListVuidMetas(ctx context.Context) (vuids map[proto.Vuid]clustermgr.ChunkID, err error)
	Close(ctx context.Context) error
}

func NewStore(ctx context.Context, cfg core.StoreConfig) (Store, error) {
	var (
		err      error
		ioEngine iouring.Engine
	)
	if cfg.UseMockIOURINGEngine {
		ioEngine, err = iouring.NewMockEngine(cfg.EngineConfig)
	} else {
		ioEngine, err = iouring.NewEngine(cfg.EngineConfig)
	}

	if err != nil {
		return nil, errors.Info(err, "new io engine failed")
	}

	layout := rawStoreFormatV1Layout
	// load super block
	superBlockBuf := util.AllocAlignedBlock(int(layout.superBlockSize), deviceSectorSize)
	if err := ioEngine.Read(superBlockBuf, layout.startOffset, len(superBlockBuf)); err != nil {
		return nil, errors.Info(err, "read super block failed")
	}
	sb := superBlock{}
	if err := sb.Unmarshal(superBlockBuf); err != nil {
		return nil, errors.Info(err, "unmarshal super block failed. raw: ", superBlockBuf)
	}

	rs := &rawStore{
		superBlock:          sb,
		chunkOperateLimiter: keycount.NewBlockingKeyCountLimit(1),

		layout:   layout,
		ioEngine: ioEngine,
		cfg:      cfg,
		closer:   closer.New(),
	}
	rs.chunksMu.chunks = make(map[clustermgr.ChunkID]*chunk)
	rs.chunksMu.vuids = make(map[proto.Vuid]clustermgr.ChunkID)
	rs.availableChunksMu.freeList = list.New()

	return rs, nil
}

type rawStore struct {
	superBlock superBlock
	// chunksMu maintains all chunks in used
	chunksMu struct {
		chunks          map[clustermgr.ChunkID]*chunk
		vuids           map[proto.Vuid]clustermgr.ChunkID
		todoCleanChunks []*chunk
		sync.RWMutex
	}
	// availableChunksMu maintains the available chunk from recycle or allocatable
	availableChunksMu struct {
		currentChunkIndex uint32
		freeList          *list.List
		sync.RWMutex
	}
	// chunk open/update/delete with single limiter key running
	chunkOperateLimiter limit.Limiter

	slicesMu struct {
		splitSliceNumPerArray uint32
		// slice index store in the array sorted by slice index incrementally
		// suppose sliceNumPerArray is 1000, slice index spread like this:
		// [0-1000) [1000-2000) [2000-3000) ...
		slices [defaultFreeSliceSplitMapNum][]*SliceMeta
		locks  [defaultFreeSliceSplitMapNum]sync.RWMutex

		// checkpoint buffer: 1MB
		checkpointBuff []byte
	}
	// sliceAllocator maintains the available slice from recycle or allocatable
	sliceAllocator *sliceAllocator
	// A/B log arena
	logMgr *logMgr

	layout   rawStoreFormatLayout
	ioEngine iouring.Engine
	cfg      core.StoreConfig
	closer   closer.Closer
}

func (s *rawStore) Load(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)

	if !s.superBlock.IsFormatted() {
		return nil
	}
	span.Debugf("start to load: %+v", s.superBlock.LayoutInfo)

	// 1. calculate splitSliceNumPerArray by max slice count
	splitSliceNumPerArray := uint32(s.superBlock.LayoutInfo.MaxSliceCount / defaultFreeSliceSplitMapNum)
	s.slicesMu.splitSliceNumPerArray = splitSliceNumPerArray
	s.slicesMu.checkpointBuff = util.AllocAlignedBlock(1<<20, deviceSectorSize)
	// initial slice meta array
	for i := range s.slicesMu.slices {
		s.slicesMu.slices[i] = make([]*SliceMeta, s.slicesMu.splitSliceNumPerArray)
	}

	// 2. initial slice allocator
	maxSliceIndex := sliceIndex(s.superBlock.LayoutInfo.MaxSliceCount - 1)
	s.sliceAllocator = newSliceAllocator(0, maxSliceIndex)

	// 3. load all chunk meta
	span.Debug("start to load chunks")
	if err := s.loadChunks(ctx); err != nil {
		return errors.Info(err, "load chunks failed")
	}

	// 4. load all slice meta
	span.Debug("start to load slices")
	if err := s.loadSlices(ctx); err != nil {
		return errors.Info(err, "load slices failed")
	}

	// 5. initial log and replay
	lc1 := logConfig{
		logArenaSize:  s.layout.logArenaSize,
		startOffset:   s.superBlock.LayoutInfo.LogArenaStart,
		logHeaderSize: s.layout.logHeaderSize,
		logRecordSize: s.layout.logRecordSize,
		ioEngine:      s.ioEngine,
	}
	lc2 := lc1
	lc2.startOffset = s.superBlock.LayoutInfo.LogArenaStart + s.layout.logArenaSize
	lm, err := newLogMgr(logMgrConfig{logConfigs: []logConfig{lc1, lc2}})
	if err != nil {
		return errors.Info(err, "initial log failed")
	}
	s.logMgr = lm

	// replay log
	span.Debug("start to replay log")
	if err := s.replayLog(ctx); err != nil {
		return errors.Info(err, "replay log failed")
	}

	// 6. do checkpoint after log replayed
	if err := (*rawStoreSliceHandler)(s).doCheckpoint(); err != nil {
		return errors.Info(err, "do checkpoint failed")
	}
	// update log header flag finally
	if err := s.logMgr.CheckpointDone(0); err != nil {
		return errors.Info(err, "mark log arena A checkpoint done failed")
	}
	if err := s.logMgr.CheckpointDone(1); err != nil {
		return errors.Info(err, "mark log arena B checkpoint done failed")
	}

	// 7. do chunk stat refresh immediately after all slice and log loaded
	s.refreshChunks(ctx)

	// start background loop
	go s.loopCleanChunk()

	span.Debug("load success")

	return nil
}

func (s *rawStore) Format(ctx context.Context, dm core.DiskMeta) error {
	span := trace.SpanFromContext(ctx)
	span.Infof("start to format raw store: %+v", dm)

	switch dm.Format {
	case core.FormatMetaTypeV2:
		s.layout = rawStoreFormatV1Layout
		s.formatV1(ctx, dm)
	default:
		panic(fmt.Sprintf("unsupported format type: %s", dm.Format))
	}

	span.Infof("format raw store success")
	return nil
}

func (s *rawStore) LoadFormat(ctx context.Context) (dm core.DiskMeta) {
	return s.superBlock.DiskMeta
}

func (s *rawStore) UpdateFormatInfo(ctx context.Context, diskID proto.DiskID, dm core.DiskMeta) error {
	span := trace.SpanFromContext(ctx)
	span.Infof("start to format raw store: %+v", dm)

	switch dm.Format {
	case core.FormatMetaTypeV2:
		super := s.superBlock
		super.DiskMeta = dm
		if err := s.upsertSuperBlock(super); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unsupported format type: %s", dm.Format))
	}

	span.Infof("update format info success")
	return nil
}

func (s *rawStore) OpenChunk(ctx context.Context, chunkID clustermgr.ChunkID, chunkSize int64) (ChunkHandler, error) {
	s.chunkOperateLimiter.Acquire(chunkID)
	defer s.chunkOperateLimiter.Release(chunkID)

	chunk, err := s.getChunk(chunkID)
	if err == nil {
		return chunk, nil
	}

	chunk, err = s.allocChunk()
	if err != nil {
		return nil, err
	}

	cm := chunk.GetMetaInfo()
	cm.ChunkID = chunkID
	cm.Status = clustermgr.ChunkStatusNormal
	cm.Epoch++
	// do persistence
	if err := s.upsertChunkMeta(cm); err != nil {
		s.freeChunk(chunk)
		return nil, err
	}

	chunk.UpdateMetaInfo(cm)
	s.addChunk(chunkID, chunk)
	return chunk, nil
}

func (s *rawStore) GetChunkMeta(ctx context.Context, chunkID clustermgr.ChunkID) (meta core.VuidMeta, err error) {
	chunk, err := s.getChunk(chunkID)
	if err != nil {
		return
	}
	return chunk.GetMetaInfo().VuidMeta, nil
}

func (s *rawStore) GetVuidBind(ctx context.Context, vuid proto.Vuid) (id clustermgr.ChunkID, err error) {
	return s.getVuid(vuid)
}

func (s *rawStore) DeleteChunk(ctx context.Context, chunkID clustermgr.ChunkID) (err error) {
	s.chunkOperateLimiter.Acquire(chunkID)
	defer s.chunkOperateLimiter.Release(chunkID)

	s.chunksMu.Lock()
	chunk, ok := s.chunksMu.chunks[chunkID]
	if !ok {
		s.chunksMu.Unlock()
		return ErrChunkNotFound
	}

	cm := chunk.GetMetaInfo()
	cm.Status = clustermgr.ChunkStatusRelease

	if err := s.upsertChunkMeta(cm); err != nil {
		s.chunksMu.Unlock()
		return err
	}

	if s.chunksMu.vuids[cm.Vuid] == chunkID {
		delete(s.chunksMu.vuids, cm.Vuid)
	}
	delete(s.chunksMu.chunks, chunkID)
	chunk.UpdateMetaInfo(cm)
	s.chunksMu.todoCleanChunks = append(s.chunksMu.todoCleanChunks, chunk)
	s.chunksMu.Unlock()

	return
}

func (s *rawStore) UpdateChunkMeta(ctx context.Context, chunkID clustermgr.ChunkID, vm core.VuidMeta) error {
	s.chunkOperateLimiter.Acquire(chunkID)
	defer s.chunkOperateLimiter.Release(chunkID)

	chunk, err := s.getChunk(chunkID)
	if err != nil {
		return err
	}
	cm := chunk.GetMetaInfo()
	cm.VuidMeta = vm
	// do persistence
	if err := s.upsertChunkMeta(cm); err != nil {
		return err
	}
	// update vuids and chunk meta in memory
	chunk.UpdateMetaInfo(cm)
	s.addVuid(vm.Vuid, chunkID)
	return nil
}

func (s *rawStore) ListChunkMetas(ctx context.Context) (chunks map[clustermgr.ChunkID]core.VuidMeta, err error) {
	chunks = make(map[clustermgr.ChunkID]core.VuidMeta)
	s.chunksMu.RLock()
	for chunkID, chunk := range s.chunksMu.chunks {
		chunks[chunkID] = chunk.GetMetaInfo().VuidMeta
	}
	s.chunksMu.RUnlock()
	return
}

func (s *rawStore) ListVuidMetas(ctx context.Context) (vuids map[proto.Vuid]clustermgr.ChunkID, err error) {
	vuids = make(map[proto.Vuid]clustermgr.ChunkID)
	s.chunksMu.RLock()
	for vuid := range s.chunksMu.vuids {
		vuids[vuid] = s.chunksMu.vuids[vuid]
	}
	s.chunksMu.RUnlock()
	return
}

func (s *rawStore) Close(ctx context.Context) error {
	s.closer.Close()

	// todo: close raw store with pending stop state
	// todo: waiting all request done

	if err := s.ioEngine.Close(); err != nil {
		return err
	}
	return nil
}

/*
|super block | ----------log A--------- | ---log B--- | --chunk meta-- | ---slice meta--- | --------------chunk-------------- | chunk | ... |

	4MB		 | header|record|record|... |             | 4KB|4KB|4KB|...| 512|512|512|...  |		         16GB                 |
			 |  4KB  |  4KB |  4KB |... |		                                          | --slice data-- |slice data|  ...  |
	                                                                            		  |     4MB        |
																						  | block|block|...|
	                                                                                        32KB | 32KB
*/
func (s *rawStore) formatV1(ctx context.Context, dm core.DiskMeta) error {
	span := trace.SpanFromContextSafe(ctx)

	// get disk info by raw device
	diskInfo, err := sys.GetDiskInfo(dm.Path)
	if err != nil {
		return errors.Info(err, "get disk info failed")
	}
	span.Infof("disk info: %+v", diskInfo)

	// write log A header which means use log A arena
	lh := logHeader{
		ver:  initLogHeaderVer,
		flag: logHeaderFlagCheckpointDone,
	}
	raw, err := lh.Marshal()
	if err != nil {
		return errors.Info(err, "marshal log header failed")
	}
	err = s.ioEngine.Write(raw, s.layout.startOffset+s.layout.superBlockSize, len(raw))
	if err != nil {
		return errors.Info(err, "write format info log header A failed")
	}

	// write log B header
	lh = logHeader{
		ver:  initLogHeaderVer - 1,
		flag: logHeaderFlagCheckpointDone,
	}
	raw, err = lh.Marshal()
	if err != nil {
		return errors.Info(err, "marshal log header failed")
	}
	err = s.ioEngine.Write(raw, s.layout.startOffset+s.layout.superBlockSize+s.layout.logArenaSize, len(raw))
	if err != nil {
		return errors.Info(err, "write format info log header B failed")
	}

	// calculate chunk count
	availableSize := diskInfo.Total - s.layout.superBlockSize - s.layout.logArenaSize*2
	maxChunkCount := availableSize / s.layout.chunkArenaSize
	chunkMetaSize := maxChunkCount * s.layout.chunkMetaSize
	sliceMetaSize := maxChunkCount * s.layout.chunkArenaSize / s.layout.sliceSize * s.layout.sliceMetaSize
	// padding to valid disk sliceSize size range
	for chunkMetaSize+sliceMetaSize+maxChunkCount*s.layout.chunkArenaSize > availableSize {
		maxChunkCount -= 1
		chunkMetaSize = maxChunkCount * s.layout.chunkMetaSize
		sliceMetaSize = maxChunkCount * s.layout.chunkArenaSize / s.layout.sliceSize * s.layout.sliceMetaSize
	}
	// calculate slice count
	maxSliceCount := maxChunkCount * s.layout.chunkArenaSize / s.layout.sliceSize

	// write header finally which means format has been done
	super := superBlock{
		DiskMeta: dm,
		LayoutInfo: layoutInfo{
			LogArenaStart:  s.layout.startOffset + s.layout.superBlockSize,
			ChunkMetaStart: s.layout.startOffset + s.layout.superBlockSize + s.layout.logArenaSize*2,
			SliceMetaStart: s.layout.startOffset + s.layout.superBlockSize + s.layout.logArenaSize*2 + chunkMetaSize,
			SliceDataStart: s.layout.startOffset + s.layout.superBlockSize + s.layout.logArenaSize*2 + chunkMetaSize + sliceMetaSize,
			MaxChunkCount:  maxChunkCount,
			MaxSliceCount:  maxSliceCount,
		},
	}

	span.Infof("super block info: %+v", super)

	return s.upsertSuperBlock(super)
}

func (s *rawStore) loadChunks(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)

	chunkMetaBuff := util.AllocAlignedBlock(int(s.layout.chunkMetaSize), deviceSectorSize)
	currentChunkIndex := uint64(0)

	for {
		offset := s.superBlock.LayoutInfo.ChunkMetaStart + currentChunkIndex*s.layout.chunkMetaSize
		if err := s.ioEngine.Read(chunkMetaBuff, offset, len(chunkMetaBuff)); err != nil {
			return errors.Info(err, "read chunk meta failed")
		}
		cm := ChunkMeta{}
		if err := cm.Unmarshal(chunkMetaBuff); err != nil {
			return errors.Info(err, "unmarshal from chunk meta buffer failed")
		}

		// break when iterate to the latest chunk meta
		if cm.IsEmpty() {
			break
		}
		currentChunkIndex++

		span.Infof("load chunk: %+v", cm)
		chunk := newChunk(chunkConfig{
			formatSliceSize: uint32(s.layout.sliceSize),
			formatBlockSize: uint32(s.layout.blockSize),
			meta:            cm,
			sliceHandler:    (*rawStoreSliceHandler)(s),
			ioEngine:        s.ioEngine,
		})
		if cm.IsFree() {
			s.freeChunk(chunk)
			continue
		}
		if cm.IsReleasing() {
			s.chunksMu.todoCleanChunks = append(s.chunksMu.todoCleanChunks, chunk)
			continue
		}

		s.addChunk(cm.ChunkID, chunk)
		s.addVuid(cm.Vuid, cm.ChunkID)
		// in the end of chunk meta
		if currentChunkIndex == s.superBlock.LayoutInfo.MaxChunkCount-1 {
			break
		}
	}

	// reset the currentChunkIndex after chunks loaded
	s.availableChunksMu.currentChunkIndex = uint32(currentChunkIndex)

	return nil
}

func (s *rawStore) loadSlices(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)

	currentSliceIndex := uint64(0)
	sliceBatchNum := 1024
	sliceMetaBuff := util.AllocAlignedBlock(int(s.layout.sliceMetaSize*uint64(sliceBatchNum)), deviceSectorSize)

READ:
	for {
		span.Debugf("start to read slice meta buff")
		offset := s.superBlock.LayoutInfo.SliceMetaStart + currentSliceIndex*s.layout.sliceMetaSize
		if err := s.ioEngine.Read(sliceMetaBuff, offset, len(sliceMetaBuff)); err != nil {
			return errors.Info(err, "read slice meta failed")
		}

		span.Debugf("start to unmarshal slice meta")
		raw := sliceMetaBuff
		for i := 0; i < sliceBatchNum; i++ {
			sm := newSliceMeta(0)
			if err := sm.Unmarshal(raw); err != nil {
				return errors.Info(err, "unmarshal from slice buffer failed", raw)
			}

			if sm.IsEmpty() {
				currentSliceIndex += uint64(i)
				break READ
			}

			(*rawStoreSliceHandler)(s).upsertSliceMetaInMemory(sm)
			if sm.IsNormal() {
				// add slice into chunk manager
				chunk, err := s.getChunkByVuid(sm.Vuid)
				if err != nil {
					return errors.Info(err, "get chunk failed", sm.Vuid)
				}
				chunk.AddSlice(sm)
			} else {
				s.sliceAllocator.free(sm.Index)
			}

			raw = raw[sm.GetSize():]
			if len(raw) == 0 {
				break
			}
		}

		currentSliceIndex += uint64(sliceBatchNum)
		// in the end of slice meta
		if currentSliceIndex == s.superBlock.LayoutInfo.MaxSliceCount-1 {
			break
		}
	}

	// reset the currentSliceIndex after slices loaded
	s.sliceAllocator.resetCurrentSliceIndex(sliceIndex(currentSliceIndex))

	return nil
}

func (s *rawStore) replayLog(ctx context.Context) error {
	currentSliceIndex := s.sliceAllocator.getCurrentSliceIndex()

	err := s.logMgr.Replay(func(le logEntry) error {
		sm := le.(logSliceMeta).SliceMeta
		// update slice meta, the chunk's slice will be updated too
		(*rawStoreSliceHandler)(s).upsertSliceMetaInMemory(sm)

		chunk, err := s.getChunkByVuid(sm.Vuid)
		if err != nil {
			return errors.Info(err, "get chunk failed", sm.Vuid)
		}

		if !sm.IsNormal() {
			// delete slice from chunk and free to allocator when slice is deleted
			chunk.DelSlice(sm.ID)
			s.sliceAllocator.free(sm.Index)
			return nil
		}
		// add slice into chunk when slice not found on chunk
		_, err = chunk.GetSlice(sm.ID)
		if errors.Is(err, ErrSliceNotFound) {
			chunk.AddSlice(sm)
			// calculate current allocated slice index in log replay
			if sm.Index > currentSliceIndex {
				currentSliceIndex = sm.Index
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// update slice allocator current slice index after log replay
	s.sliceAllocator.resetCurrentSliceIndex(currentSliceIndex)

	return nil
}

func (s *rawStore) upsertSuperBlock(super superBlock) error {
	raw, err := super.Marshal()
	if err != nil {
		return errors.Info(err, "marshal format info failed")
	}

	err = s.ioEngine.Write(raw, s.layout.startOffset, len(raw))
	if err != nil {
		return errors.Info(err, "write format info header failed")
	}

	s.superBlock = super
	return nil
}

func (s *rawStore) refreshChunks(ctx context.Context) {
}

// loopCleanChunk clean chunk's slice meta and add into free list
func (s *rawStore) loopCleanChunk() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "loop-clean-chunk")
	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-ticker.C:
			s.chunksMu.RLock()
			todo := s.chunksMu.todoCleanChunks
			s.chunksMu.todoCleanChunks = []*chunk{}
			s.chunksMu.RUnlock()

			for _, chunk := range todo {
				// get recycle chunk's slice and add into slice free list
				chunk.RangeSlice(func(si *slice) bool {
					sm := si.GetShardMeta()
					_sm := *sm
					(*rawStoreSliceHandler)(s).DeleteSlice(&_sm)
					return true
				})

				// free to chunk list
				cm := chunk.GetMetaInfo()
				cm.Status = clustermgr.ChunkStatusDefault
				if err := s.upsertChunkMeta(cm); err != nil {
					span.Errorf("upsert chunk meta failed: %s", err)
					continue
				}
				chunk.UpdateMetaInfo(cm)
				s.freeChunk(chunk)
			}
		case <-s.closer.Done():
			return
		}
	}
}

func (s *rawStore) allocChunk() (*chunk, error) {
	s.availableChunksMu.Lock()
	defer s.availableChunksMu.Unlock()

	// get from free list firstly
	e := s.availableChunksMu.freeList.Front()
	if e != nil {
		s.availableChunksMu.freeList.Remove(e)
		return e.Value.(*chunk), nil
	}
	// alloc from chunk index
	if s.availableChunksMu.currentChunkIndex < uint32(s.superBlock.LayoutInfo.MaxChunkCount) {
		s.availableChunksMu.currentChunkIndex += 1
		return newChunk(chunkConfig{
			formatSliceSize: uint32(s.layout.sliceSize),
			formatBlockSize: uint32(s.layout.blockSize),
			meta: ChunkMeta{
				Index: chunkIndex(s.availableChunksMu.currentChunkIndex - 1),
			},
			sliceHandler: (*rawStoreSliceHandler)(s),
			ioEngine:     s.ioEngine,
		}), nil
	}

	return nil, ErrNoChunkAvailable
}

func (s *rawStore) freeChunk(chunk *chunk) {
	s.availableChunksMu.Lock()
	s.availableChunksMu.freeList.PushBack(chunk)
	s.availableChunksMu.Unlock()
}

func (s *rawStore) upsertChunkMeta(cm ChunkMeta) error {
	raw := util.AllocAlignedBlock(int(s.layout.chunkMetaSize), deviceSectorSize)
	err := cm.MarshalTo(raw)
	if err != nil {
		return errors.Info(err, "marshal chunk meta info failed")
	}

	err = s.ioEngine.Write(raw, s.superBlock.LayoutInfo.ChunkMetaStart+uint64(cm.Index)*s.layout.chunkMetaSize, len(raw))
	if err != nil {
		return errors.Info(err, "write chunk meta info failed")
	}
	return nil
}

func (s *rawStore) getChunk(chunkID clustermgr.ChunkID) (*chunk, error) {
	s.chunksMu.RLock()
	chunk := s.chunksMu.chunks[chunkID]
	s.chunksMu.RUnlock()
	if chunk == nil {
		return nil, ErrChunkNotFound
	}
	return chunk, nil
}

func (s *rawStore) getChunkByVuid(vuid proto.Vuid) (*chunk, error) {
	s.chunksMu.RLock()
	chunkID := s.chunksMu.vuids[vuid]
	chunk := s.chunksMu.chunks[chunkID]
	s.chunksMu.RUnlock()
	if chunk == nil {
		return nil, ErrChunkNotFound
	}
	return chunk, nil
}

func (s *rawStore) addChunk(chunkID clustermgr.ChunkID, chunk *chunk) {
	s.chunksMu.Lock()
	s.chunksMu.chunks[chunkID] = chunk
	s.chunksMu.Unlock()
}

func (s *rawStore) getVuid(vuid proto.Vuid) (clustermgr.ChunkID, error) {
	s.chunksMu.RLock()
	chunkID, exist := s.chunksMu.vuids[vuid]
	s.chunksMu.RUnlock()
	if exist {
		return clustermgr.ChunkID{}, ErrChunkNotFound
	}
	return chunkID, nil
}

func (s *rawStore) addVuid(vuid proto.Vuid, chunkID clustermgr.ChunkID) {
	s.chunksMu.Lock()
	s.chunksMu.vuids[vuid] = chunkID
	s.chunksMu.Unlock()
}

type rawStoreSliceHandler rawStore

func (r *rawStoreSliceHandler) AllocSlice(id proto.BlobID, vuid proto.Vuid, ChunkEpoch uint32) (*SliceMeta, error) {
	sliceIndex, err := r.sliceAllocator.alloc()
	if err != nil {
		return nil, err
	}

	idx := uint32(sliceIndex) / r.slicesMu.splitSliceNumPerArray
	r.slicesMu.locks[idx].Lock()
	sm := r.slicesMu.slices[idx][uint32(sliceIndex)%r.slicesMu.splitSliceNumPerArray]
	if sm == nil {
		sm = newSliceMeta(sliceIndex)
		r.slicesMu.slices[idx][uint32(sliceIndex)%r.slicesMu.splitSliceNumPerArray] = sm
	}
	// fill id, offset and belong
	sm.ID = id
	sm.Vuid = vuid
	sm.ChunkEpoch = ChunkEpoch
	sm.Flag = blobnode.ShardStatusNormal
	sm.Offset = int64(r.superBlock.LayoutInfo.SliceDataStart + uint64(sliceIndex)*r.layout.sliceSize)
	r.slicesMu.locks[idx].Unlock()

	// no need to update, the slice meta will be updated and saved in persistence after first write/append
	// if err := r.UpdateSlice(sm); err != nil {
	// 	return nil, err
	// }

	return sm, nil
}

func (r *rawStoreSliceHandler) UpdateSlice(sm *SliceMeta) error {
	// save slice meta in persistence
	if err := r.upsertSliceMetaInPersistence(sm); err != nil {
		return err
	}
	r.upsertSliceMetaInMemory(sm)

	return nil
}

func (r *rawStoreSliceHandler) DeleteSlice(sm *SliceMeta) error {
	sm.ResetToDelete()
	// save slice meta in persistence
	if err := r.upsertSliceMetaInPersistence(sm); err != nil {
		return err
	}

	// save slice into memory
	idx := uint32(sm.Index) / r.slicesMu.splitSliceNumPerArray
	r.slicesMu.locks[idx].Lock()
	_sm := r.slicesMu.slices[idx][uint32(sm.Index)%r.slicesMu.splitSliceNumPerArray]
	*_sm = *sm
	r.slicesMu.locks[idx].Unlock()

	// add into slice allocator
	r.sliceAllocator.free(sm.Index)

	return nil
}

func (r *rawStoreSliceHandler) upsertSliceMetaInPersistence(sm *SliceMeta) error {
	// merge meta request into log handler to save IOPS cost
	lsm := newLogSliceMeta(sm)
	ret, err := r.logMgr.Submit(lsm)
	if err != nil && !errors.Is(err, errLogArenaWriteFull) {
		return err
	}
	lsm.Free()

	// start background checkpoint
	if ret.checkpoint {
		go func() {
			span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "checkpoint-"+r.superBlock.DiskMeta.DiskID.ToString())
			if err := r.doCheckpoint(); err != nil {
				span.Errorf("do checkpoint failed: %s", errors.Detail(err))
				return
			}
			// update log header flag finally
			if err := r.logMgr.CheckpointDone(ret.idx); err != nil {
				span.Errorf("mark log arena checkpoint done failed: %s", err)
			}
		}()
	}

	return err
}

func (r *rawStoreSliceHandler) upsertSliceMetaInMemory(sm *SliceMeta) {
	// save slice into memory
	idx := uint32(sm.Index) / r.slicesMu.splitSliceNumPerArray
	r.slicesMu.locks[idx].Lock()
	_sm := r.slicesMu.slices[idx][uint32(sm.Index)%r.slicesMu.splitSliceNumPerArray]
	if _sm == nil {
		_sm = &SliceMeta{}
		r.slicesMu.slices[idx][uint32(sm.Index)%r.slicesMu.splitSliceNumPerArray] = _sm
	}
	*_sm = *sm
	r.slicesMu.locks[idx].Unlock()
}

func (r *rawStoreSliceHandler) doCheckpoint() error {
	startOff := r.superBlock.LayoutInfo.SliceMetaStart
	buff := r.slicesMu.checkpointBuff
	sliceCount, sliceIndex := uint64(0), uint64(0)

LOOP:
	for _, slices := range r.slicesMu.slices {
		for i, sm := range slices {
			if sm.GetSize() > len(buff) || i == int(r.slicesMu.splitSliceNumPerArray-1) || sm == nil || sm.IsEmpty() {
				startOff = r.superBlock.LayoutInfo.SliceMetaStart + uint64(i)*uint64(r.slicesMu.splitSliceNumPerArray)*deviceSectorSize
				if err := r.ioEngine.Write(buff, startOff+sliceIndex*deviceSectorSize, int(sliceIndex*deviceSectorSize)); err != nil {
					return errors.Info(err, "write slice metas failed")
				}

				if sm == nil || sm.IsEmpty() {
					break LOOP
				}

				// reset buffer and counter
				buff = r.slicesMu.checkpointBuff
				sliceIndex += sliceCount
				sliceCount = 0
			}
			if err := sm.MarshalTo(buff); err != nil {
				return errors.Info(err, "marshal slice meta failed")
			}
			buff = buff[deviceSectorSize:]
			sliceCount++
		}
	}

	return nil
}

type freeElement struct {
	cell    uint64
	cellIdx uint32
}

func newSliceAllocator(currentSliceIndex, maxSliceIndex sliceIndex) *sliceAllocator {
	allocator := &sliceAllocator{
		currentSliceIndex:     currentSliceIndex,
		maxSliceIndex:         maxSliceIndex,
		splitSliceNumPerArray: uint32(maxSliceIndex) / defaultFreeSliceSplitMapNum,
		frees: struct {
			sliceIndexes [defaultFreeSliceSplitMapNum]struct {
				list    *list.List
				indexes []struct {
					cell uint64
					e    *list.Element
				}
			}
			locks [defaultFreeSliceSplitMapNum]sync.RWMutex
		}{},
	}

	for i := range allocator.frees.sliceIndexes {
		allocator.frees.sliceIndexes[i].list = list.New()
		allocator.frees.sliceIndexes[i].indexes = make([]struct {
			cell uint64
			e    *list.Element
		}, allocator.splitSliceNumPerArray/64)
	}

	return allocator
}

type sliceAllocator struct {
	robinCount        uint32
	currentSliceIndex sliceIndex
	maxSliceIndex     sliceIndex
	// splitSliceNumPerList means slice num
	splitSliceNumPerArray uint32
	frees                 struct {
		// slice index store in the array sorted by slice index incrementally,
		// every uint64 element hold available slice index,
		// suppose sliceNumPerArray is 1000, slice index spread like this:
		// [0-1000)            [1000-2000)              [2000-3000) ...
		// [0-64) [64-128) ... [1000-1064) [1064-1128)  ...
		sliceIndexes [defaultFreeSliceSplitMapNum]struct {
			list    *list.List
			indexes []struct {
				cell uint64
				e    *list.Element
			}
		}
		locks [defaultFreeSliceSplitMapNum]sync.RWMutex
	}
	lock sync.RWMutex
}

func (s *sliceAllocator) alloc() (ret sliceIndex, err error) {
	// alloc from free list first
	startIdx := atomic.AddUint32(&s.robinCount, 1) / s.splitSliceNumPerArray
	freeIdx := startIdx
	for {
		s.frees.locks[freeIdx].Lock()

		list := s.frees.sliceIndexes[freeIdx].list
		if list.Len() == 0 {
			s.frees.locks[freeIdx].Unlock()
			// try next free list
			freeIdx = (freeIdx + 1) % defaultFreeSliceSplitMapNum
			if freeIdx == startIdx {
				break
			}
			continue
		}

		e := list.Front()
		freeEle := e.Value.(freeElement)
		bitIndex := s.trailingZeros64(freeEle.cell)

		ret = sliceIndex(freeIdx*s.splitSliceNumPerArray + uint32(freeEle.cellIdx)*64 + uint32(bitIndex))
		freeEle.cell >>= uint(bitIndex + 1)
		s.frees.sliceIndexes[freeIdx].indexes[freeEle.cellIdx].cell = freeEle.cell
		// remove element when cell bit is all used
		if freeEle.cell == 0 {
			list.Remove(e)
		}

		s.frees.locks[freeIdx].Unlock()

		return
	}

	// alloc from unused slice secondly
	s.lock.Lock()
	result := s.currentSliceIndex + 1
	if result > s.maxSliceIndex {
		s.lock.Unlock()
		return 0, ErrNoSliceAvailable
	}
	s.currentSliceIndex = result
	s.lock.Unlock()

	return result, nil
}

// [0-1000]          [1000-2000]
// [64] [64] [64] []
func (s *sliceAllocator) free(si sliceIndex) {
	freeArrIdx := uint32(si) / s.splitSliceNumPerArray
	cellIdx := (uint32(si) % s.splitSliceNumPerArray) / 64
	cellBit := (uint32(si) % s.splitSliceNumPerArray) % 64

	s.frees.locks[freeArrIdx].Lock()
	target := s.frees.sliceIndexes[freeArrIdx].indexes[cellIdx]
	target.cell |= 1 << cellBit
	freeEle := freeElement{cellIdx: cellIdx, cell: target.cell}
	if target.e == nil {
		target.e = s.frees.sliceIndexes[freeArrIdx].list.PushBack(freeEle)
	} else {
		target.e.Value = freeEle
	}
	s.frees.sliceIndexes[freeArrIdx].indexes[cellIdx] = target
	s.frees.locks[freeArrIdx].Unlock()
}

func (s *sliceAllocator) getCurrentSliceIndex() sliceIndex {
	return s.currentSliceIndex
}

func (s *sliceAllocator) resetCurrentSliceIndex(idx sliceIndex) {
	s.currentSliceIndex = idx
}

const deBruijn64 = 0x03f79d71b4ca8b09

var deBruijn64tab = [64]byte{
	0, 1, 56, 2, 57, 49, 28, 3, 61, 58, 42, 50, 38, 29, 17, 4,
	62, 47, 59, 36, 45, 43, 51, 22, 53, 39, 33, 30, 24, 18, 12, 5,
	63, 55, 48, 27, 60, 41, 37, 16, 46, 35, 44, 21, 52, 32, 23, 11,
	54, 26, 40, 15, 34, 20, 31, 10, 25, 14, 19, 9, 13, 8, 7, 6,
}

// trailingZeros64 returns the number of trailing zero bits in x; the result is 64 for x == 0.
func (s *sliceAllocator) trailingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	// If popcount is fast, replace code below with return popcount(^x & (x - 1)).
	//
	// x & -x leaves only the right-most bit set in the word. Let k be the
	// index of that bit. Since only a single bit is set, the value is two
	// to the power of k. Multiplying by a power of two is equivalent to
	// left shifting, in this case by k bits. The de Bruijn (64 bit) constant
	// is such that all six bit, consecutive substrings are distinct.
	// Therefore, if we have a left shifted version of this constant we can
	// find by how many bits it was shifted by looking at which six bit
	// substring ended up at the top of the word.
	// (Knuth, volume 4, section 7.3.1)
	return int(deBruijn64tab[(x&-x)*deBruijn64>>(64-6)])
}
