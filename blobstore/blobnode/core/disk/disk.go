// Copyright 2022 The CubeFS Authors.
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
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	bncom "github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/chunk"
	myos "github.com/cubefs/cubefs/blobstore/blobnode/sys"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
)

const (
	MaxChunkSize    = int64(1024 << 30) // 1024 GiB
	RandomIntervalS = 30
)

var StateTransitionRules = map[clustermgr.ChunkStatus][]clustermgr.ChunkStatus{
	clustermgr.ChunkStatusDefault:  {clustermgr.ChunkStatusNormal},
	clustermgr.ChunkStatusNormal:   {clustermgr.ChunkStatusNormal, clustermgr.ChunkStatusReadOnly},
	clustermgr.ChunkStatusReadOnly: {clustermgr.ChunkStatusNormal, clustermgr.ChunkStatusReadOnly, clustermgr.ChunkStatusRelease},
}

var (
	_chunkVer = []byte{0x1}
	_diskVer  = []byte{0x1}
)

type DiskStorageWrapper struct {
	*DiskStorage
}

/*
 * 1. Create a new chunk
 * 2. bind it to vuid
 */
func (dsw *DiskStorageWrapper) CreateChunk(ctx context.Context, vuid proto.Vuid, chunksize int64) (
	cs core.ChunkAPI, err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	ds := dsw.DiskStorage

	if chunksize < 0 || chunksize > MaxChunkSize {
		return nil, bloberr.ErrInvalidParam
	}

	if ds.isChunksExceeded(ctx, chunksize) {
		return nil, bloberr.ErrTooManyChunks
	}

	stats := ds.stats.Load().(*core.DiskStats)
	if ds.isMountPoint && stats.Free < chunksize {
		return nil, bloberr.ErrDiskNoSpace
	}

	// The following logic, for the same vuid, only allows serial execution
	if ds.ChunkLimitPerKey.Acquire(vuid) != nil {
		return nil, bloberr.ErrOverload
	}
	defer ds.ChunkLimitPerKey.Release(vuid)

	ds.Lock.RLock()
	_, exist := ds.Chunks[vuid]
	ds.Lock.RUnlock()
	if exist {
		span.Errorf("vuid:%v alread exist.", vuid)
		return nil, bloberr.ErrAlreadyExist
	}

	super := ds.SuperBlock
	chunkId := clustermgr.NewChunkID(vuid)
	nowtime := time.Now().UnixNano()

	vm := core.VuidMeta{
		Version:   _chunkVer[0],
		Vuid:      vuid,
		DiskID:    ds.DiskID,
		ChunkID:   chunkId,
		ChunkSize: chunksize,
		Ctime:     nowtime,
		Mtime:     nowtime,
		Status:    clustermgr.ChunkStatusNormal,
	}

	// create chunk storage
	cs, err = chunk.NewChunkStorage(ctx, ds.DataPath, vm, dsw.ioPools, func(option *core.Option) {
		option.CreateDataIfMiss = true
		option.DB = ds.SuperBlock.db
		option.Conf = ds.Conf
		option.IoQos = ds.dataQos
		option.Disk = dsw
	})
	if err != nil {
		span.Errorf("Failed new chunk:<%s>, err:%v", ds.DataPath, err)
		return nil, err
	}

	// save to superBlock
	err = super.UpsertChunk(ctx, vm.ChunkID, vm)
	if err != nil {
		span.Errorf("Failed upsert chunk<%s>, err:%v", vm.ChunkID, err)
		return nil, err
	}

	// update bind it to vuid
	err = super.BindVuidChunk(ctx, vuid, chunkId)
	if err != nil {
		span.Errorf("Failed vuid<%d>, chunkid<%s>, err:%v", vuid, chunkId, err)
		return nil, err
	}

	// add to map
	ds.Lock.Lock()
	ds.Chunks[vuid] = cs
	ds.Lock.Unlock()

	return cs, nil
}

func (dsw *DiskStorageWrapper) RestoreChunkStorage(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	ds := dsw.DiskStorage
	sb := ds.SuperBlock

	// load chunkmeta
	vuid2Chunk, err := sb.ListVuids(ctx)
	if err != nil {
		span.Errorf("Failed list chunks: %v", err)
		return err
	}

	vuidMetas, err := sb.ListChunks(ctx)
	if err != nil {
		span.Errorf("Failed list chunks: %v", err)
		return err
	}

	// chunkID -> is redundant. we need to find chunkID that is in $vuidMetas but not in $vuid2Chunk
	chunkRedundant := make(map[cmapi.ChunkID]struct{})
	for chunkID := range vuidMetas {
		chunkRedundant[chunkID] = struct{}{}
	}

	chunks := make(map[proto.Vuid]core.ChunkAPI)
	for vuid, chunkid := range vuid2Chunk {
		span.Debugf("vuid:%d, chunkid: %s", vuid, chunkid)
		delete(chunkRedundant, chunkid)

		vm := vuidMetas[chunkid]
		if vm.Status == clustermgr.ChunkStatusRelease {
			span.Warnf("vuid:%d(chunk:%s) status is release", vm.Vuid, vm.ChunkID)
			continue
		}
		if vm.Compacting {
			vm.Compacting = false
			err := sb.UpsertChunk(ctx, chunkid, vm)
			if err != nil {
				span.Errorf("Failed upsert chunk compacting, chunkid:%s, vm:%v", chunkid, vm)
				return err
			}
			err = ds.notifyCompacting(ctx, vuid, false)
			if err != nil {
				span.Errorf("set chunk(%v) compacting false failed: %v", vuid, err)
				return err
			}
		}
		cs, err := chunk.NewChunkStorage(ctx, ds.DataPath, vm, ds.ioPools, func(o *core.Option) {
			o.Conf = ds.Conf
			o.DB = sb.db
			o.Disk = dsw
			o.IoQos = ds.dataQos
			o.CreateDataIfMiss = false
		})
		if err != nil {
			span.Errorf("Failed New chunk, path:%s, vm:%v", ds.DataPath, vm)
			return err
		}

		chunks[vm.Vuid] = cs
	}

	// these chunks ars not in $vuid2Chunk: need mark redundant chunks -> release
	// 1. new chunk is not completed, it is invalid/incomplete file, mark it to release
	// 2. old chunk is completed, it is redundant, mark it to release
	for chunkID := range chunkRedundant {
		csMeta := vuidMetas[chunkID]
		csMeta.Status = cmapi.ChunkStatusRelease
		csMeta.Reason = cmapi.ReleaseForCompact
		csMeta.Mtime = time.Now().UnixNano()
		span.Warnf("chunk(%s) is redundant, mark it to release status", chunkID)

		if err = dsw.SuperBlock.UpsertChunk(ctx, chunkID, csMeta); err != nil {
			span.Errorf("update chunk(%s) status to release failed: %+v", chunkID, err)
			return err
		}
	}

	ds.Lock.Lock()
	ds.Chunks = chunks
	ds.Lock.Unlock()

	span.Debugf("build ChunkStorage success")
	return
}

type DiskStorage struct {
	DiskID proto.DiskID

	Lock       sync.RWMutex
	SuperBlock *SuperBlock
	Chunks     map[proto.Vuid]core.ChunkAPI

	// conf
	Conf     *core.Config
	DataPath string
	MetaPath string

	// limiter
	ChunkLimitPerKey limit.Limiter

	// stats
	stats atomic.Value // *core.DiskStats

	// DataQos (include io visualization function)
	dataQos *qos.QosMgr

	// status
	status       proto.DiskStatus
	isMountPoint bool
	closed       bool

	// chan
	compactCh chan proto.Vuid
	closeCh   chan struct{}

	// ctx is used for initiated requests that
	// may need to be canceled on server shutdown.
	wg  sync.WaitGroup
	ctx context.Context

	// hook fn
	OnClosed func()

	CreateAt     int64
	LastUpdateAt int64

	// io pools
	ioPools map[bnapi.IOType]bncom.IoPool
}

func (ds *DiskStorage) IsRegister() bool {
	return false
}

func (ds *DiskStorage) Close(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	ds.Lock.Lock()
	defer ds.Lock.Unlock()

	if ds.closed {
		span.Panicf("can not happened. diskId:%v", ds.DiskID)
		return
	}

	span.Infof("== closing diskID:%v ==", ds.DiskID)

	if ds.OnClosed != nil {
		ds.OnClosed()
	}

	if ds.closeCh != nil {
		close(ds.closeCh)
	}
	// wait loop in goroutine
	go func() {
		// wait all loop done
		ds.waitAllLoopsStop(ctx)

		// clean chunk map
		ds.Chunks = make(map[proto.Vuid]core.ChunkAPI)

		// clean superblock
		sb := ds.SuperBlock
		if sb != nil {
			sb.Close(ctx)
			ds.SuperBlock = nil
		}

		ds.closed = true
	}()

	for _, pool := range ds.ioPools {
		pool.Close()
	}
	ds.dataQos.Close()
}

func (ds *DiskStorage) DiskInfo() (info clustermgr.BlobNodeDiskInfo) {
	ds.Lock.RLock()
	defer ds.Lock.RUnlock()

	stats := ds.stats.Load().(*core.DiskStats)

	// stats
	info.Used = stats.Used
	info.UsedChunkCnt = int64(len(ds.Chunks))
	// for chunk space
	info.Free = stats.Free - stats.Reserved
	if info.Free < 0 {
		info.Free = 0
	}
	info.Size = stats.TotalDiskSize - stats.Reserved
	if info.Size < 0 {
		info.Size = 0
	}

	// config
	hostInfo := ds.Conf.HostInfo

	info.DiskID = ds.DiskID
	info.ClusterID = hostInfo.ClusterID
	info.Idc = hostInfo.IDC
	info.Rack = hostInfo.Rack
	info.Host = hostInfo.Host
	info.Path = ds.Conf.Path
	info.NodeID = ds.Conf.NodeID

	// status
	info.Status = ds.status

	info.CreateAt = time.Unix(0, ds.CreateAt)
	info.LastUpdateAt = time.Unix(0, ds.LastUpdateAt)

	return
}

func (ds *DiskStorage) Status() (status proto.DiskStatus) {
	ds.Lock.RLock()
	defer ds.Lock.RUnlock()

	return ds.status
}

func (ds *DiskStorage) Stats() (stat core.DiskStats) {
	return *(ds.stats.Load().(*core.DiskStats))
}

func (ds *DiskStorage) GetConfig() (config *core.Config) {
	return ds.Conf
}

func (ds *DiskStorage) GetIoQos() (ioQos *qos.QosMgr) {
	return ds.dataQos
}

func (ds *DiskStorage) GetDataPath() (path string) {
	return ds.DataPath
}

func (ds *DiskStorage) GetMetaPath() (path string) {
	return ds.MetaPath
}

func (ds *DiskStorage) ID() (id proto.DiskID) {
	return ds.DiskID
}

func (ds *DiskStorage) SetStatus(status proto.DiskStatus) {
	ds.Lock.Lock()
	ds.status = status
	ds.Lock.Unlock()
}

func (ds *DiskStorage) ResetChunks(ctx context.Context) {
	ds.Lock.Lock()
	defer ds.Lock.Unlock()

	ds.Chunks = make(map[proto.Vuid]core.ChunkAPI)
}

func (ds *DiskStorage) ReleaseChunk(ctx context.Context, vuid proto.Vuid, force bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// The following logic, for the same vuid, only allows serial execution
	if ds.ChunkLimitPerKey.Acquire(vuid) != nil {
		return bloberr.ErrOverload
	}
	defer ds.ChunkLimitPerKey.Release(vuid)

	// if disk is dropped, it need release
	status := ds.Status()
	if status >= proto.DiskStatusBroken && status <= proto.DiskStatusRepaired {
		return bloberr.ErrDiskBroken
	}

	ds.Lock.RLock()
	cs, exist := ds.Chunks[vuid]
	ds.Lock.RUnlock()
	if !exist {
		span.Errorf("vuid:%v not exist in ds.Chunks", vuid)
		return bloberr.ErrNoSuchVuid
	}

	// can not convert status
	if !force && !isValidStateTransition(cs.Status(), clustermgr.ChunkStatusRelease) {
		span.Errorf("can not release chunk(%s) status:%v", cs.ID(), cs.Status())
		return bloberr.ErrUnexpected
	}

	if cs.HasPendingRequest() {
		span.Errorf("can not happen. chunk:%s has pending reqs", cs.ID())
		return bloberr.ErrChunkInuse
	}

	span.Warnf("will mark vuid(%v)/chunk(%s) destroy. force mode(%v)", vuid, cs.ID(), force)

	// delete node from map
	ds.Lock.Lock()
	delete(ds.Chunks, vuid)
	ds.Lock.Unlock()

	// unbind vuid
	err = ds.SuperBlock.UnbindVuidChunk(ctx, vuid, cs.ID())
	if err != nil {
		span.Errorf("Failed unbind vuid:%d chunk:%s", vuid, cs.ID())
		return err
	}

	// update chunk meta
	vm := cs.VuidMeta()
	vm.Status = clustermgr.ChunkStatusRelease
	vm.Reason = clustermgr.ReleaseForUser
	vm.Mtime = time.Now().UnixNano()

	err = ds.SuperBlock.UpsertChunk(ctx, cs.ID(), *vm)
	if err != nil {
		span.Errorf("update chunk(%s) status to release failed: %v", vm.ChunkID, err)
		return err
	}

	// update ChunkStorage status in memory
	cs.SetStatus(clustermgr.ChunkStatusRelease)

	cs = nil

	span.Infof("release chunk<%s> success", vm.ChunkID)

	return nil
}

/*
 * chunk status changing must call this method
 * first: change persistence status
 * second: change status in memory
 * concurrency safety: only allows serial execution for the same vuid
 */
func (ds *DiskStorage) UpdateChunkStatus(ctx context.Context, vuid proto.Vuid, status clustermgr.ChunkStatus) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	if !bnapi.IsValidChunkStatus(status) {
		span.Errorf("chunk status is invalid: %v", status)
		return bloberr.ErrInvalidParam
	}

	// The following logic, for the same vuid, only allows serial execution
	if ds.ChunkLimitPerKey.Acquire(vuid) != nil {
		return bloberr.ErrOverload
	}
	defer ds.ChunkLimitPerKey.Release(vuid)

	ds.Lock.RLock()
	cs, exist := ds.Chunks[vuid]
	ds.Lock.RUnlock()
	if !exist {
		// superBlock can read such vuid meta, but does not exist in disk.Chunks
		// such vuid have been released
		span.Errorf("disk(%v) no such vuid(%v)", ds.DiskID, vuid)
		return bloberr.ErrNoSuchVuid
	}

	vm := cs.VuidMeta()

	if vm.Status == status {
		span.Debugf("chunk status is same")
		return nil
	}

	// can not convert status
	if !isValidStateTransition(vm.Status, status) {
		span.Errorf("can not convert chunk(%s) status:%v to %v", cs.ID(), vm.Status, status)
		return bloberr.ErrUnexpected
	}

	vm.Status = status
	vm.Mtime = time.Now().UnixNano()

	err = ds.SuperBlock.UpsertChunk(ctx, cs.ID(), *vm)
	if err != nil {
		span.Errorf("update chunk(%s) status to %v failed: %v", vm.ChunkID, status, err)
		return err
	}

	// update ChunkStorage status in memory
	cs.SetStatus(status)

	return nil
}

func (ds *DiskStorage) UpdateChunkCompactState(ctx context.Context, vuid proto.Vuid, compacting bool) (
	err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("update vuid:%v compacting:%v", vuid, compacting)

	// The following logic, for the same vuid, only allows serial execution
	if ds.ChunkLimitPerKey.Acquire(vuid) != nil {
		return bloberr.ErrOverload
	}
	defer ds.ChunkLimitPerKey.Release(vuid)

	ds.Lock.RLock()
	cs, exist := ds.Chunks[vuid]
	ds.Lock.RUnlock()
	if !exist {
		// superBlock can read such vuid meta, but does not exist in disk.Chunks
		// such vuid have been released
		span.Errorf("disk(%v) no such vuid(%v)", ds.DiskID, vuid)
		return bloberr.ErrNoSuchVuid
	}

	vm := cs.VuidMeta()
	vm.Compacting = compacting
	vm.Mtime = time.Now().UnixNano()

	err = ds.SuperBlock.UpsertChunk(ctx, cs.ID(), *vm)
	if err != nil {
		span.Errorf("update chunk(%s) status to %v failed: %v",
			vm.ChunkID, compacting, err)
		return err
	}

	return nil
}

func (ds *DiskStorage) UpdateDiskStatus(ctx context.Context, status proto.DiskStatus) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	if status >= proto.DiskStatusMax {
		return bloberr.ErrInvalidParam
	}

	// read disk meta
	di, err := ds.SuperBlock.LoadDiskInfo(ctx)
	if err != nil {
		return
	}

	// persistence disk status
	di.Status = status
	err = ds.SuperBlock.UpsertDisk(ctx, ds.DiskID, di)
	if err != nil {
		span.Errorf("update disk(%v) persistence status failed: %v", ds.DiskID, err)
		return err
	}
	// modify
	di.Status = status
	di.Mtime = time.Now().UnixNano()

	// disk status in memory
	ds.Lock.Lock()
	ds.status = status
	ds.Lock.Unlock()

	return
}

func (ds *DiskStorage) ListChunks(ctx context.Context) (chunks []core.VuidMeta, err error) {
	chunksmap, err := ds.SuperBlock.ListChunks(ctx)
	if err != nil {
		return nil, err
	}

	chunks = []core.VuidMeta{}
	for _, chunk := range chunksmap {
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func (ds *DiskStorage) LoadDiskInfo(ctx context.Context) (dm core.DiskMeta, err error) {
	return ds.SuperBlock.LoadDiskInfo(ctx)
}

func (ds *DiskStorage) GetChunkStorage(vuid proto.Vuid) (cs core.ChunkAPI, found bool) {
	ds.Lock.RLock()
	defer ds.Lock.RUnlock()

	cs, ok := ds.Chunks[vuid]
	if !ok {
		return nil, false
	}
	return cs, true
}

func (ds *DiskStorage) WalkChunksWithLock(ctx context.Context, walkFn func(cs core.ChunkAPI) error) (err error) {
	ds.Lock.RLock()
	defer ds.Lock.RUnlock()

	for _, cs := range ds.Chunks {
		if err = walkFn(cs); err != nil {
			return err
		}
	}
	return nil
}

func (ds *DiskStorage) IsCleanUp(ctx context.Context) bool {
	span := trace.SpanFromContextSafe(ctx)

	if len(ds.Chunks) != 0 { // all chunks handler in memory
		span.Debugf("diskID:%d is not clean, used chunk cnt:%d", ds.DiskID, len(ds.Chunks))
		return false
	}

	chunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("%v list chunks failed: %+v", ds.MetaPath, err)
		return false
	}

	if len(chunks) != 0 { // all chunks in db
		span.Debugf("diskID:%d is not clean, db chunk file cnt:%d", ds.DiskID, len(chunks))
		return false
	}

	return true
}

func (ds *DiskStorage) IsWritable() bool {
	return ds.Status() == proto.DiskStatusNormal
}

func (ds *DiskStorage) loopAttach(f func()) {
	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		f()
	}()
}

func (ds *DiskStorage) loopCleanChunk() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "CleanChunk"+ds.Conf.Path)
	span.Infof("loop clean chunk start")

	timer := initTimer(ds.Conf.ChunkCleanIntervalSec)
	defer timer.Stop()

	for {
		select {
		case <-ds.closeCh:
			span.Infof("loop clean chunk done")
			return
		case <-timer.C:
			if err := ds.cleanReleasedChunks(); err != nil {
				span.Errorf("Failed exec Cleanchunks. err:%v", err)
			}
			resetTimer(ds.Conf.ChunkCleanIntervalSec, timer)
		}
	}
}

// get disk usage
func (ds *DiskStorage) loopDiskUsage() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "DiskUsage"+ds.Conf.Path)

	span.Infof("loop disk usage start")

	timer := initTimer(ds.Conf.DiskUsageIntervalSec)
	defer timer.Stop()

	for {
		select {
		case <-ds.closeCh:
			span.Infof("loop disk usage  done")
			return
		case <-timer.C:
			if err := ds.fillDiskUsage(ctx); err != nil {
				span.Errorf("Failed exec disk usage. err:%v", err)
			}
			resetTimer(ds.Conf.DiskUsageIntervalSec, timer)
		}
	}
}

func (ds *DiskStorage) waitAllLoopsStop(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	done := make(chan struct{})
	go func() {
		ds.wg.Wait()
		close(done)
	}()

	warnTicker := time.NewTicker(30 * time.Second)
	defer warnTicker.Stop()
	for {
		select {
		case <-warnTicker.C:
			span.Warnf("=== disk<%v> loop wait timed out. ===", ds.DiskID)
		case <-done:
			span.Infof("=== disk<%v> all loops done ===", ds.DiskID)
			return
		}
	}
}

func (ds *DiskStorage) sceneWithoutProtection(ctx context.Context, meta core.VuidMeta) bool {
	span := trace.SpanFromContextSafe(ctx)
	if meta.Reason != clustermgr.ReleaseForCompact {
		return false
	}
	span.Debugf("id:%s meta:%v without protection", meta.ChunkID, meta)
	return true
}

func (ds *DiskStorage) cleanReleasedChunks() (err error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", bncom.BackgroudReqID("CleanChunk"+ds.Conf.Path))

	span.Debugf("come in CleanChunks.")

	// set io type
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)

	protectionPeriod := time.Duration(ds.Conf.ChunkReleaseProtectionM)
	now := time.Now().UnixNano()

	chunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("%v list chunks failed: %v", ds.MetaPath, err)
		return
	}

	for _, ck := range chunks {
		if ck.Status != clustermgr.ChunkStatusRelease {
			continue
		}

		if !ds.sceneWithoutProtection(ctx, ck) && now-ck.Mtime < int64(time.Minute*protectionPeriod) {
			span.Debugf("%s still in protection period", ck.ChunkID)
			continue
		}

		chunkid, err := ds.SuperBlock.ReadVuidBind(ctx, ck.Vuid)
		if err == nil && chunkid == ck.ChunkID {
			span.Warnf("can not happen. vuid:%d bind %s. skip", ck.Vuid, ck.ChunkID)
			continue
		}

		if err = ds.realCleanChunk(ctx, ck.ChunkID); err != nil {
			span.Errorf("failed clean chunk:%s, err:%v", ck.ChunkID, err)
			continue
		}
	}

	return
}

func (ds *DiskStorage) realCleanChunk(ctx context.Context, id clustermgr.ChunkID) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Warnf("will clean chunk:(%s)", id)

	err = ds.cleanChunk(ctx, id, false)
	if err != nil {
		span.Errorf("Failed cleanChunk:%s err:%v", id, err)
		return
	}

	// delete this chunk's meta
	err = ds.SuperBlock.DeleteChunk(ctx, id)
	if err != nil {
		span.Errorf("Failed Delete Chunk:%s err:%v", id, err)
		return
	}

	span.Infof("disk(%v) clean chunk(%v) success", ds.DiskID, id)

	return nil
}

// Clean up the space of a chunk, including metadata and data
// NOTE: Maybe a long time
func (ds *DiskStorage) cleanChunk(ctx context.Context, id clustermgr.ChunkID, toTrash bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// clean meta
	span.Debugf("clean %s chunk meta begin ===", id)
	defer span.Debugf("clean %s chunk data end ===", id)

	err = ds.SuperBlock.CleanChunkSpace(ctx, id)
	if err != nil {
		span.Errorf("clean %s chunk meta failed: %s", id, err)
		return
	}
	span.Debugf("clean %s chunk meta end ===", id)

	// clean data
	span.Debugf("clean %s chunk data begin ===", id)
	chunkDataFile := filepath.Join(ds.DataPath, id.String())

	if !toTrash {
		return os.Remove(chunkDataFile)
	} else {
		return ds.moveToTrash(ctx, chunkDataFile)
	}
}

func (ds *DiskStorage) fillDiskUsage(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("will fill disk usage")

	// load disk info
	rootInfo, err := myos.GetDiskInfo(ds.Conf.Path)
	if err != nil {
		span.Errorf("Failed get [%s] info, err:%v", ds.Conf.Path, err)
		return err
	}

	stats := new(core.DiskStats)
	stats.Reserved = ds.Conf.DiskReservedSpaceB
	stats.Used = int64(rootInfo.Total - rootInfo.Free)
	stats.Free = int64(rootInfo.Free)
	stats.TotalDiskSize = int64(rootInfo.Total)

	ds.stats.Store(stats)

	return nil
}

func (ds *DiskStorage) isChunksExceeded(ctx context.Context, chunksize int64) bool {
	span := trace.SpanFromContextSafe(ctx)

	ds.Lock.RLock()
	defer ds.Lock.RUnlock()

	if len(ds.Chunks) >= int(ds.Conf.MaxChunks) {
		return true
	}

	// unit test skips the following logic
	if os.Getenv("JENKINS_TEST") != "" {
		return false
	}
	if ds.Conf.GetGlobalConfig != nil {
		if val, err := ds.Conf.GetGlobalConfig(ctx, proto.ChunkOversoldRatioKey); err == nil && val != "" {
			return false
		}
	}

	stats := ds.stats.Load().(*core.DiskStats)
	actualTotal := stats.TotalDiskSize - stats.Reserved
	if int64(len(ds.Chunks)) >= (actualTotal / chunksize) {
		span.Errorf("current:%v, total:%v, chunksize:%v", len(ds.Chunks), actualTotal, chunksize)
		return true
	}

	return false
}

func NewDiskStorage(ctx context.Context, conf core.Config) (dsw *DiskStorageWrapper, err error) {
	ds, err := newDiskStorage(ctx, conf)
	if err != nil {
		return nil, err
	}

	dsw = &DiskStorageWrapper{DiskStorage: ds}

	err = dsw.RestoreChunkStorage(ctx)
	if err != nil {
		return nil, err
	}

	// It will be automatically recycled when gc
	runtime.SetFinalizer(dsw, func(wapper *DiskStorageWrapper) {
		wapper.Close(context.Background())
	})

	return dsw, nil
}

// parse disk, make disk storage
func newDiskStorage(ctx context.Context, conf core.Config) (ds *DiskStorage, err error) {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", conf.Path)

	// init config
	err = core.InitConfig(&conf)
	if err != nil {
		return nil, err
	}
	span.Infof("config:%v", conf)

	path, err := filepath.Abs(conf.Path)
	if err != nil {
		return nil, err
	}

	metaRoot := conf.MetaRootPrefix
	if metaRoot != "" {
		if exist, err := bncom.IsFileExists(metaRoot); err != nil || !exist {
			span.Errorf("meta path: %s not exist ( occur err:%v ), exit", metaRoot, err)
			return nil, errors.New("meta root prefix not exist")
		}
	}

	if conf.MustMountPoint {
		if !myos.IsMountPoint(conf.Path) {
			span.Errorf("%s must mount point.", conf.Path)
			return nil, errors.New("must mount point")
		}

		if metaRoot != "" && !myos.IsMountPoint(metaRoot) {
			span.Errorf("%s must mount point.", metaRoot)
			return nil, errors.New("must mount point")
		}
	}

	if conf.AutoFormat {
		span.Warnf("auto format mode, will ensure directory.")
		err = core.EnsureDiskArea(path, metaRoot)
		if err != nil {
			return nil, err
		}
	}

	diskDataPath := core.GetDataPath(path)
	diskMetaPath := core.GetMetaPath(path, metaRoot)

	span.Infof("datapath: %v, metapath:%v", diskDataPath, diskMetaPath)

	// load superblock，create or open
	sb, err := NewSuperBlock(diskMetaPath, &conf)
	if err != nil {
		return nil, err
	}

	var dm core.DiskMeta

	dm, err = sb.LoadDiskInfo(ctx)
	if err != nil {
		if os.IsNotExist(err) {
			span.Warnf("disk not format. will format and register")
		} else {
			return nil, err
		}
		dm = core.DiskMeta{}
	}

	if !dm.Registered {
		exist, err := core.IsFormatConfigExist(path)
		if err != nil || exist {
			span.Errorf("unexpected error. format file ( in %s ) should not exist, but, exist:%v err:%v", path, exist, err)
			return nil, bloberr.ErrUnexpected
		}

		dm, err = registerDisk(ctx, sb, &conf)
		if err != nil {
			span.Errorf("register disk failed: %v", err)
			return nil, err
		}
	}

	span.Infof("diskID:%d", dm.DiskID)

	// check format info
	formatInfo, err := core.ReadFormatInfo(ctx, path)
	if err != nil {
		span.Errorf("Failed read format info, err:%v", err)
		return nil, err
	}
	if formatInfo.DiskID != dm.DiskID {
		span.Errorf("unexpected error. diskId not match. format:%v, dm:%v", formatInfo, dm)
		return nil, bloberr.ErrUnexpected
	}

	// init eio handler
	sb.SetHandlerIOError(func(err error) {
		conf.HandleIOError(context.Background(), dm.DiskID, err)
	})

	// io visualization: init data io stat
	dataIos, err := flow.NewIOFlowStat(dm.DiskID.ToString(), conf.IOStatFileDryRun)
	if err != nil {
		span.Errorf("Failed new dataio flow stat, err:%v", err)
		return nil, err
	}
	diskView := flow.NewDiskViewer(dataIos)

	// init Qos manager
	conf.DataQos.StatGetter = dataIos
	conf.DataQos.DiskViewer = diskView
	conf.DataQos.DiskID = dm.DiskID

	dataQos, err := qos.NewQosMgr(conf.DataQos)
	if err != nil {
		span.Errorf("Failed new io qos, err:%v", err)
		return nil, err
	}

	// setting io pools
	metricConf := bncom.IoPoolMetricConf{
		ClusterID: uint32(conf.HostInfo.ClusterID),
		Host:      conf.HostInfo.Host,
		DiskID:    uint32(dm.DiskID),
	}
	writePool := bncom.NewIOPool(conf.WriteThreadCnt, conf.WriteQueueDepth, bnapi.WriteIO.String(), metricConf)
	readPool := bncom.NewIOPool(conf.ReadThreadCnt, conf.ReadQueueDepth, bnapi.ReadIO.String(), metricConf)
	delPool := bncom.NewIOPool(conf.DeleteThreadCnt, conf.DeleteQueueDepth, bnapi.DeleteIO.String(), metricConf)
	backPool := bncom.NewIOPool(conf.BackgroundThreadCnt, conf.BackgroundQueueDepth, bnapi.BackgroundIO.String(), metricConf)

	ds = &DiskStorage{
		DiskID:           dm.DiskID,
		SuperBlock:       sb,
		DataPath:         diskDataPath,
		MetaPath:         diskMetaPath,
		ChunkLimitPerKey: keycount.NewBlockingKeyCountLimit(1),
		Conf:             &conf,
		closeCh:          make(chan struct{}),
		compactCh:        make(chan proto.Vuid),
		ctx:              ctx,
		status:           dm.Status,
		isMountPoint:     myos.IsMountPoint(conf.Path),
		dataQos:          dataQos,
		CreateAt:         dm.Ctime,
		LastUpdateAt:     dm.Mtime,
		ioPools: map[bnapi.IOType]bncom.IoPool{
			bnapi.ReadIO:       readPool,
			bnapi.WriteIO:      writePool,
			bnapi.DeleteIO:     delPool,
			bnapi.BackgroundIO: backPool,
		},
	}

	if err = ds.fillDiskUsage(ctx); err != nil {
		span.Errorf("Failed fill disk usage, err:%v", err)
		return nil, err
	}

	// background loop
	ds.loopAttach(ds.loopCleanChunk)
	ds.loopAttach(ds.loopCompactFile)
	ds.loopAttach(ds.loopDiskUsage)
	ds.loopAttach(ds.loopCleanTrash)
	ds.loopAttach(ds.loopMetricReport)

	return ds, nil
}

func registerDisk(ctx context.Context, sb *SuperBlock, conf *core.Config) (dm core.DiskMeta, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("disk conf:<%v> auto format", conf)

	// allocate global Uniq diskID
	diskID, err := conf.AllocDiskID(ctx)
	if err != nil {
		span.Errorf("Failed alloc diskId: %d, err:%v", dm.DiskID, err)
		return
	}

	span.Debugf("diskId: <%v>", diskID)

	now := time.Now().UnixNano()

	format := &core.FormatInfo{
		FormatInfoProtectedField: core.FormatInfoProtectedField{
			DiskID:  diskID,
			Version: _diskVer[0],
			Format:  core.FormatMetaTypeV1,
			Ctime:   now,
		},
	}
	checkSum, err := format.CalCheckSum()
	if err != nil {
		span.Errorf("cal format info crc failed: %v", err)
		return
	}
	format.CheckSum = checkSum

	// dm.Host =
	dm = core.DiskMeta{
		FormatInfo: *format,
		Mtime:      now,
		Registered: true,
		Status:     proto.DiskStatusNormal,
		Path:       conf.Path,
	}

	err = sb.UpsertDisk(ctx, dm.DiskID, dm)
	if err != nil {
		span.Errorf("Failed upsert disk: %d, err:%v", dm.DiskID, err)
		return
	}

	err = core.SaveDiskFormatInfo(ctx, conf.Path, format)
	if err != nil {
		span.Errorf("Failed save disk[%s] format info, err:%v", conf.Path, err)
		return
	}

	span.Infof("register disk(%v) success", diskID)
	return
}

func isValidStateTransition(src, dest clustermgr.ChunkStatus) bool {
	validStates, exist := StateTransitionRules[src]
	if !exist {
		return false
	}
	for _, s := range validStates {
		if s == dest {
			return true
		}
	}
	return false
}

func initTimer(ts int64) *time.Timer {
	return time.NewTimer(time.Duration(ts+rand.Int63n(RandomIntervalS)) * time.Second)
}

func resetTimer(ts int64, timer *time.Timer) {
	rand.Seed(time.Now().UnixNano())
	timer.Reset(time.Duration(ts+rand.Int63n(RandomIntervalS)) * time.Second)
}
