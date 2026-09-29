// Copyright 2026 The CubeFS Authors.
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

package disk

import (
	"context"
	"errors"
	"os"
	"sync"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var errInspectStoreClosed = errors.New("inspect state store: superblock closed")

// inspectState returns the disk's in-memory inspect state store. It is created
// and loaded once at construction (newDiskStorage), so accessors can use it
// directly.
func (ds *DiskStorage) inspectState() *inspectStateStore {
	return ds.inspectStore
}

// inspectStateStore keeps ds's data-inspect state in memory instead of accessing
// the superblock (rocksdb meta) on every read/write. Reads and writes only touch
// memory; FlushInspectState persists dirty entries to the superblock in batches,
// triggered by the service-layer DataInspectMgr after each chunk traversal and at
// the end of each inspect round. Nothing is persisted while the disk or its
// superblock is closing/closed.
type inspectStateStore struct {
	mu         sync.RWMutex
	superBlock *SuperBlock
	diskID     proto.DiskID

	disk       core.InspectDiskState
	diskDirty  bool
	chunks     map[proto.Vuid]*core.InspectChunkState
	chunkDirty map[proto.Vuid]struct{}
}

// compile-time assertion: inspectStateStore implements core.InspectStateStore.
var _ core.InspectStateStore = (*inspectStateStore)(nil)

func copyChunkState(st *core.InspectChunkState) *core.InspectChunkState {
	if st == nil {
		return nil
	}
	cp := *st
	if st.BadBids != nil {
		cp.BadBids = make(map[proto.BlobID]core.BadBidMeta, len(st.BadBids))
		for bid, meta := range st.BadBids {
			cp.BadBids[bid] = meta
		}
	}
	return &cp
}

// load reads the persisted inspect state from the superblock. It is called once
// at disk startup (newDiskStorage); accessors assume the store is loaded. The
// chunks map doubles as the loaded marker.
func (st *inspectStateStore) load(ctx context.Context) error {
	sb := st.superBlock
	if !sb.tryLock() {
		return errInspectStoreClosed
	}
	defer sb.unlock()
	span := trace.SpanFromContextSafe(ctx)

	diskState, err := sb.ReadInspectDiskState(ctx)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// no persisted disk state yet: first inspect cycle will create it
		st.disk = core.InspectDiskState{}
	} else {
		st.disk = diskState
	}

	chunks := make(map[proto.Vuid]*core.InspectChunkState)
	if err := sb.RangeInspectChunkState(ctx, func(cs *core.InspectChunkState) bool {
		chunks[cs.Vuid] = copyChunkState(cs)
		return true
	}); err != nil {
		span.Warnf("load inspect chunk states failed: %+v", err)
		return err
	}

	st.chunks = chunks
	st.chunkDirty = make(map[proto.Vuid]struct{})
	return nil
}

func (st *inspectStateStore) LoadInspectDiskState(ctx context.Context) (core.InspectDiskState, error) {
	if !bnapi.IsValidDiskID(st.diskID) {
		return core.InspectDiskState{}, bloberr.ErrInvalidParam
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	if !bnapi.IsValidDiskID(st.disk.DiskID) {
		// initialize zero state
		return core.InspectDiskState{DiskID: st.diskID}, nil
	}
	return st.disk, nil
}

func (st *inspectStateStore) StoreInspectDiskState(ctx context.Context, state core.InspectDiskState) error {
	if !bnapi.IsValidDiskID(state.DiskID) {
		return bloberr.ErrInvalidParam
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.disk = state
	st.diskDirty = true
	return nil
}

func (st *inspectStateStore) LoadInspectChunkState(ctx context.Context, vuid proto.Vuid) (core.InspectChunkState, error) {
	if !vuid.IsValid() {
		return core.InspectChunkState{}, bloberr.ErrInvalidParam
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	cs, exist := st.chunks[vuid]
	if !exist {
		// initialized zero state
		return core.InspectChunkState{Vuid: vuid, CycleCnt: -1}, nil
	}
	// copy-out to protect store's map through BadBids
	return *copyChunkState(cs), nil
}

func (st *inspectStateStore) StoreInspectChunkState(ctx context.Context, state core.InspectChunkState) error {
	if !state.Vuid.IsValid() {
		return bloberr.ErrInvalidParam
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	// BadBids are managed exclusively through AddBadBid/DeleteBadBid so that
	// fine-grained concurrent updates (e.g. from reportBadShard or onBadBidRepaired)
	// are never silently overwritten by a bulk store call that carries a stale
	// snapshot. Carry forward whatever the store currently holds when the entry
	// already exists; a brand-new entry keeps the caller-provided BadBids.
	if existing, ok := st.chunks[state.Vuid]; ok && existing != nil {
		state.BadBids = existing.BadBids // copyChunkState below will deep-copy
	}
	st.chunks[state.Vuid] = copyChunkState(&state)
	st.chunkDirty[state.Vuid] = struct{}{}
	return nil
}

// AddBadBid records a bad bid in the chunk's inspect state. The existing records
// are left untouched (FoundAt/Reason are not refreshed).
func (st *inspectStateStore) AddBadBid(ctx context.Context, vuid proto.Vuid, bid proto.BlobID, meta core.BadBidMeta) (bool, error) {
	span := trace.SpanFromContextSafe(ctx)
	if !vuid.IsValid() {
		return false, bloberr.ErrInvalidParam
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	cs, exist := st.chunks[vuid]
	if !exist {
		// chunk not inspected yet: initialize the zero state so the record
		// survives until the next inspect round initializes the chunk.
		cs = &core.InspectChunkState{Vuid: vuid, CycleCnt: -1}
		st.chunks[vuid] = cs
	}
	if _, ok := cs.BadBids[bid]; ok {
		return false, nil
	}
	if len(cs.BadBids) >= core.MaxInspectBadBids {
		span.Warnf("inspect vuid:%d bad bids reach limit %d, drop bid:%d",
			vuid, core.MaxInspectBadBids, bid)
		return false, nil
	}
	if cs.BadBids == nil {
		cs.BadBids = make(map[proto.BlobID]core.BadBidMeta)
	}
	cs.BadBids[bid] = meta
	// mark to trigger flush the state to rocksdb
	st.chunkDirty[vuid] = struct{}{}
	return true, nil
}

func (st *inspectStateStore) DeleteBadBid(ctx context.Context, vuid proto.Vuid, bid proto.BlobID) (bool, error) {
	if !vuid.IsValid() {
		return false, bloberr.ErrInvalidParam
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	cs, exist := st.chunks[vuid]
	if !exist {
		return false, nil
	}
	if _, ok := cs.BadBids[bid]; !ok {
		return false, nil
	}
	delete(cs.BadBids, bid)
	// mark to trigger flush the state to rocksdb
	st.chunkDirty[vuid] = struct{}{}
	return true, nil
}

func (st *inspectStateStore) DeleteChunkState(ctx context.Context, vuid proto.Vuid) error {
	if !vuid.IsValid() {
		return bloberr.ErrInvalidParam
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.chunks, vuid)
	st.chunkDirty[vuid] = struct{}{}
	return nil
}

func (st *inspectStateStore) RangeInspectChunkState(ctx context.Context, fn func(st *core.InspectChunkState) bool) error {
	st.mu.RLock()
	defer st.mu.RUnlock()
	for _, cs := range st.chunks {
		if !fn(copyChunkState(cs)) {
			return errInspectChunkStateRange
		}
	}
	return nil
}

// FlushInspectState persists dirty inspect state. It is a no-op once the
// superblock is closed (tryLock fails). tryLock/unlock (CAS 0↔1) makes the db
// use exclusive with Close (CAS 0→2), so Close cannot nil the db mid-flush.
func (st *inspectStateStore) FlushInspectState(ctx context.Context) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if !st.diskDirty && len(st.chunkDirty) == 0 {
		return
	}

	span := trace.SpanFromContextSafe(ctx)
	sb := st.superBlock
	if !sb.tryLock() { // disk may be closed
		return
	}
	defer sb.unlock()
	if st.diskDirty {
		if err := sb.UpsertInspectDiskState(ctx, st.disk); err != nil {
			span.Errorf("flush inspect disk state failed: %+v", err)
		} else {
			st.diskDirty = false
		}
	}
	for vuid := range st.chunkDirty {
		cs, exist := st.chunks[vuid]
		if !exist {
			// if chunk inspect state was deleted in mem before
			if err := sb.DeleteInspectChunkState(ctx, vuid); err != nil {
				span.Errorf("flush delete inspect chunk state vuid:%d failed: %+v", vuid, err)
				continue
			}
			delete(st.chunkDirty, vuid)
			continue
		}
		if err := sb.UpsertInspectChunkState(ctx, *cs); err != nil {
			span.Errorf("flush inspect chunk state vuid:%d failed: %+v", vuid, err)
			continue
		}
		delete(st.chunkDirty, vuid)
	}
}
