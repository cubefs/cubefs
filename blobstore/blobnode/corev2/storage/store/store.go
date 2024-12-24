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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
	"github.com/cubefs/cubefs/blobstore/blobnode/sys"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit"
)

const (
	defaultFreeSliceSplitMapNum = 64
)

type Config struct {
	Path         string         `json:"path"`
	EngineConfig iouring.Config `json:"engine_config"`
}

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

func NewStore(ctx context.Context, cfg Config) (Store, error) {
	ioEngine, err := iouring.NewEngine(cfg.EngineConfig)
	if err != nil {
		return nil, errors.Info(err, "new io engine failed")
	}

	return &rawStore{
		layout:   rawStoreFormatV1Layout,
		ioEngine: ioEngine,
		cfg:      cfg,
	}, nil
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
		currentChunkIndex uint64
		freeList          list.List
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
	cfg      Config
	closer   closer.Closer
}

// todo
func (s *rawStore) Load(ctx context.Context) error {
	// load all chunk meta

	// load all slice meta

	// replay log

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
	cm.VuidMeta.ChunkID = chunkID
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
	// todo: get disk info by raw device
	diskInfo, err := sys.GetDiskInfo(dm.Path)
	if err != nil {
		return errors.Info(err, "get disk info failed")
	}

	// write log A header which means use log A arena
	lh := logHeader{
		ver: initLogHeaderVer,
	}
	raw, err := lh.Marshal()
	if err != nil {
		return errors.Info(err, "marshal log header failed")
	}
	err = s.ioEngine.Write(raw, s.layout.startOffset+s.layout.superBlockSize, len(raw))
	if err != nil {
		return errors.Info(err, "write format info header failed")
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
	// write header finally which means format has been done
	super := superBlock{
		DiskMeta: dm,
		LayoutInfo: layoutInfo{
			LogArenaStart:  s.layout.startOffset + s.layout.superBlockSize,
			ChunkMetaStart: s.layout.superBlockSize + s.layout.logArenaSize*2,
			SliceMetaStart: s.layout.superBlockSize + s.layout.logArenaSize*2 + chunkMetaSize,
			SliceDataStart: s.layout.superBlockSize + s.layout.logArenaSize*2 + chunkMetaSize + sliceMetaSize,
			MaxChunkCount:  maxChunkCount,
		},
	}
	return s.upsertSuperBlock(super)
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
	if s.availableChunksMu.currentChunkIndex < s.superBlock.LayoutInfo.MaxChunkCount {
		s.availableChunksMu.currentChunkIndex += 1
		return newChunk(), nil
	}

	return nil, ErrNoChunkAvailable
}

func (s *rawStore) freeChunk(chunk *chunk) {
	s.availableChunksMu.Lock()
	chunk.Reset()
	s.availableChunksMu.freeList.PushBack(chunk)
	s.availableChunksMu.Unlock()
}

func (s *rawStore) upsertChunkMeta(cm ChunkMeta) error {
	raw, err := cm.Marshal()
	if err != nil {
		return errors.Info(err, "marshal chunk meta info failed")
	}

	err = s.ioEngine.Write(raw, s.superBlock.LayoutInfo.ChunkMetaStart+uint64(cm.Index)*s.layout.chunkMetaSize, len(raw))
	if err != nil {
		return errors.Info(err, "write chunk meta info failed")
	}
	return nil
}

// loopCleanChunk clean chunk's slice meta and add into free list
func (s *rawStore) loopCleanChunk() {
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
					s.sliceAllocator.free(si.GetShardMeta().Index)
					return true
				})

				// free to chunk list
				s.freeChunk(chunk)
			}
		case <-s.closer.Done():
			return
		}
	}
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
	r.slicesMu.locks[idx].RLock()
	sm := r.slicesMu.slices[idx][uint32(sliceIndex)%r.slicesMu.splitSliceNumPerArray]
	r.slicesMu.locks[idx].RUnlock()

	sm.ID = id
	sm.Vuid = vuid
	sm.ChunkEpoch = ChunkEpoch
	// no need to update, the slice meta will be updated and saved in persistence after first write/append
	// if err := r.UpdateSlice(sm); err != nil {
	// 	return nil, err
	// }

	return sm, nil
}

func (r *rawStoreSliceHandler) UpdateSlice(sm *SliceMeta) error {
	// save slice meta in persistence
	if err := r.upsertSliceMeta(sm); err != nil {
		return err
	}

	// save slice into memory
	idx := uint32(sm.Index) / r.slicesMu.splitSliceNumPerArray
	r.slicesMu.locks[idx].Lock()
	_sm := r.slicesMu.slices[idx][uint32(sm.Index)%r.slicesMu.splitSliceNumPerArray]
	*_sm = *sm
	r.slicesMu.locks[idx].Unlock()

	return nil
}

func (r *rawStoreSliceHandler) DeleteSlice(sm *SliceMeta) error {
	// save slice meta in persistence
	if err := r.upsertSliceMeta(sm); err != nil {
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

func (r *rawStoreSliceHandler) upsertSliceMeta(sm *SliceMeta) error {
	// merge meta request into log handler to save IOPS cost
	ret, err := r.logMgr.Submit(logSliceMeta{SliceMeta: sm})
	if err != nil && !errors.Is(err, errLogArenaWriteFull) {
		return err
	}

	// start background checkpoint
	if ret.checkpoint {
		go r.doCheckpoint(ret.idx)
	}

	return err
}

func (r *rawStoreSliceHandler) doCheckpoint(logArenaIdx uint32) {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "")

	startOff := r.superBlock.LayoutInfo.SliceMetaStart
	buff := r.slicesMu.checkpointBuff
	sliceCount, sliceIndex := uint64(0), uint64(0)

	for _, slices := range r.slicesMu.slices {
		for _, sm := range slices {
			if sm.GetSize() > len(buff) {
				if err := r.ioEngine.Write(buff, startOff+sliceIndex*deviceSectorSize, cap(buff)); err != nil {
					span.Errorf("write slice metas failed: %s", err)
					return
				}
				// reset buffer and counter
				buff = r.slicesMu.checkpointBuff
				sliceIndex += sliceCount
				sliceCount = 0
			}
			if err := sm.MarshalTo(buff); err != nil {
				span.Errorf("marshal slice meta failed: %s", err)
				return
			}
			buff = buff[deviceSectorSize:]
			sliceCount++
		}
	}

	// update log header flag finally
	if err := r.logMgr.CheckpointDone(logArenaIdx); err != nil {
		span.Errorf("mark log arena checkpoint done failed: %s", err)
	}
}

type freeElement struct {
	cell    uint64
	cellIdx uint32
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
