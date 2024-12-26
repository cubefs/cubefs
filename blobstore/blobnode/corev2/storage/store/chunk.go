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
	"context"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"io"
	"sync"
)

const (
	defaultSliceSplitMapNum = 64
)

type ChunkHandler interface {
	String() string
	Read(ctx context.Context, slice *core.Shard) (r io.ReadCloser, err error)
	Write(ctx context.Context, slice *core.Shard) (n int, err error)
	Delete(ctx context.Context, slice *core.Shard) (err error)
	Flush(ctx context.Context) (err error)
	Stat(ctx context.Context) (stat core.StorageStat, err error)
	MetaHandler() MetaHandler
	Close(ctx context.Context) error
	//Fd() uintptr
	//Name() string
	//Stat() (info os.FileInfo, err error)
	//Sync() error
	//Allocate(id proto.BlobID, size int64) (Slice, error)

	//ReadAtCtx(ctx context.Context, b []byte, off int64) (n int, err error)
	//WriteAtCtx(ctx context.Context, b []byte, off int64) (n int, err error)
	//Allocate(off int64, size int64) (err error)
	//Discard(off int64, size int64) (err error)
	//Delete(ctx context.Context, shard *core.Shard) (err error)
	//Discard(s Slice) (err error)
	//SysStat() (sysstat syscall.Stat_t, err error)
	//MetaHandler() MetaHandler
	//Close() error
}

type sliceHandler interface {
	// AllocSlice alloc new slice from available
	AllocSlice(id proto.BlobID, vuid proto.Vuid, ChunkEpoch uint32) (*SliceMeta, error)
	// UpdateSlice update slice meta info in persistence
	UpdateSlice(sm *SliceMeta) error
	// DeleteSlice delete slice in persistence
	DeleteSlice(sm *SliceMeta) error
}

type chunkConfig struct {
	formatSliceSize uint32
	formatBlockSize uint32
	meta            ChunkMeta
	sliceHandler    sliceHandler
	ioEngine        iouring.Engine
}

func newChunk(cfg chunkConfig) *chunk {
	return &chunk{
		meta:            cfg.meta,
		formatSliceSize: cfg.formatSliceSize,
		formatBlockSize: cfg.formatBlockSize,
		sliceHandler:    cfg.sliceHandler,
		ioEngine:        cfg.ioEngine,
	}
}

type chunk struct {
	meta ChunkMeta
	// 1MB/2MB/4MB...
	formatSliceSize uint32
	// 32KB
	formatBlockSize uint32

	slicesMu struct {
		slices [defaultSliceSplitMapNum]map[proto.BlobID]*slice
		locks  [defaultSliceSplitMapNum]sync.RWMutex
	}
	sliceHandler sliceHandler
	ioEngine     iouring.Engine
}

func (c *chunk) String() string {
	return c.meta.ChunkID.String()
}

func (c *chunk) Read(ctx context.Context, read *core.Shard) (r io.ReadCloser, err error) {
	slice, err := c.GetSlice(read.Bid)
	if err != nil {
		return nil, err
	}
	return c.sliceReader(slice, read), nil
}

// Write append write slice data into slice data arena
// avoiding append write in one slice concurrently
func (c *chunk) Write(ctx context.Context, append *core.Shard) (int, error) {
	// todo: slice concurrency limit

	slice, err := c.GetSlice(append.Bid)
	if err != nil {
		// alloc slice
		slice, err = c.allocSlice(append.Bid)
		if err != nil {
			return 0, err
		}
	}

	sw := c.sliceWriter(slice, append)

	// write data
	n, err := io.Copy(sw, append.Body)
	if err != nil {
		return 0, err
	}
	if n != int64(append.Size) {
		return 0, io.ErrShortWrite
	}
	sw.Close()

	// update slice meta
	sm := slice.GetShardMeta()
	_sm := *sm

	_sm.Size += append.Size
	// fix append size by decrease crc size when last written is not align with block size
	if _sm.Size%c.formatBlockSize != 0 {
		_sm.Size -= crcSize
	}
	_sm.Size += append.Size
	_sm.LastBlockCrc = sw.lastBlockCrc
	if err := c.sliceHandler.UpdateSlice(&_sm); err != nil {
		return 0, err
	}

	// as sm is a pointer to the store's slice meta, we don't need to update sm here
	// sm.Size = _sm.Size
	// sm.LastBlockCrc = _sm.LastBlockCrc

	//update slice last sector
	slice.lastSector = sw.lastSector

	return int(n), nil
}

func (c *chunk) Delete(ctx context.Context, delete *core.Shard) (err error) {
	// todo: slice concurrency limit

	slice, err := c.GetSlice(delete.Bid)
	if err != nil {
		return err
	}
	sm := slice.GetShardMeta()
	_sm := *sm
	_sm.Reset()
	if err := c.sliceHandler.DeleteSlice(&_sm); err != nil {
		return err
	}
	c.delSlice(delete.Bid)
	return nil
}

func (c *chunk) Flush(ctx context.Context) (err error) {
	return nil
}

func (c *chunk) Stat(ctx context.Context) (stat core.StorageStat, err error) {
	return
}

func (c *chunk) MetaHandler() MetaHandler {
	return nil
}

func (c *chunk) Close(ctx context.Context) error {
	return nil
}

// -----------------------------------------------internal api----------------------------------------------------

// UpdateMetaInfo update chunk meta info in memory
func (c *chunk) UpdateMetaInfo(m ChunkMeta) {
	c.meta = m
}

func (c *chunk) GetMetaInfo() ChunkMeta {
	return c.meta
}

func (c *chunk) RangeSlice(fn func(s *slice) bool) {
	for i, m := range c.slicesMu.slices {
		c.slicesMu.locks[i].RLock()
		for _, s := range m {
			if !fn(s) {
				c.slicesMu.locks[i].RUnlock()
				return
			}
		}
		c.slicesMu.locks[i].RUnlock()
	}
}

func (c *chunk) allocSlice(id proto.BlobID) (*slice, error) {
	idx := id % defaultSliceSplitMapNum
	c.slicesMu.locks[idx].Lock()
	if slice := c.slicesMu.slices[idx][id]; slice != nil {
		c.slicesMu.locks[idx].Unlock()
		return slice, nil
	}

	sm, err := c.sliceHandler.AllocSlice(id, c.meta.Vuid, c.meta.Epoch)
	if err != nil {
		c.slicesMu.locks[idx].Unlock()
		return nil, err
	}
	slice := newSlice(sm)
	c.slicesMu.slices[idx][id] = slice
	c.slicesMu.locks[idx].Unlock()

	return slice, nil
}

func (c *chunk) GetSlice(id proto.BlobID) (*slice, error) {
	idx := id % defaultSliceSplitMapNum
	c.slicesMu.locks[idx].RLock()
	sm := c.slicesMu.slices[idx][id]
	c.slicesMu.locks[idx].RUnlock()
	if sm == nil {
		return sm, ErrSliceNotFound
	}
	return sm, nil
}

func (c *chunk) AddSlice(sm *SliceMeta) {
	idx := sm.ID % defaultSliceSplitMapNum
	c.slicesMu.locks[idx].Lock()
	c.slicesMu.slices[idx][sm.ID] = newSlice(sm)
	c.slicesMu.locks[idx].Unlock()
}

func (c *chunk) delSlice(id proto.BlobID) {
	idx := id % defaultSliceSplitMapNum
	c.slicesMu.locks[idx].Lock()
	delete(c.slicesMu.slices[idx], id)
	c.slicesMu.locks[idx].Unlock()
}

func (c *chunk) sliceReader(slice *slice, read *core.Shard) *sliceReader {
	sr := sliceReaderPool.Get().(*sliceReader)
	sr.read = read
	sr.slice = slice
	sr.next = 0
	sr.sliceSize = c.formatSliceSize
	sr.ioEngine = c.ioEngine
	return sr
}

func (c *chunk) sliceWriter(s *slice, append *core.Shard) *sliceWriter {
	sw := sliceWriterPool.Get().(*sliceWriter)
	sw.slice = s
	sw.append = append
	sw.next = 0
	sw.ioEngine = c.ioEngine
	sw.lastSector = s.lastSector
	sw.sliceSize = c.formatSliceSize
	sw.blockSize = c.formatBlockSize
	sw.lastSector = s.lastSector

	return sw
}
