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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	bncom "github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/chunk"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/store"
	myos "github.com/cubefs/cubefs/blobstore/blobnode/sys"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
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

type DiskStorage struct {
	DiskID proto.DiskID

	Lock   sync.RWMutex
	Chunks map[proto.Vuid]core.ChunkAPI

	// conf
	Conf *core.Config

	// limiter
	ChunkLimitPerKey limit.Limiter

	// stats
	stats atomic.Value // *core.DiskStats

	// DataQos (include io visualization function)
	dataQos qos.Qos

	// status
	status proto.DiskStatus
	closed bool

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
	writePool taskpool.IoPool
	readPool  taskpool.IoPool

	store store.Store
}

func (ds *DiskStorage) IsRegister() bool {
	return false
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
		ds.closed = true
	}()

	ds.writePool.Close()
	ds.readPool.Close()
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

func (ds *DiskStorage) GetIoQos() (ioQos qos.Qos) {
	return ds.dataQos
}

func (ds *DiskStorage) ID() (id proto.DiskID) {
	return ds.DiskID
}

func (ds *DiskStorage) SetStatus(status proto.DiskStatus) {
	ds.Lock.Lock()
	ds.status = status
	ds.Lock.Unlock()
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

	stats := ds.stats.Load().(*core.DiskStats)
	actualTotal := stats.TotalDiskSize - stats.Reserved
	if int64(len(ds.Chunks)) >= (actualTotal / chunksize) {
		span.Errorf("current:%v, total:%v, chunksize:%v", len(ds.Chunks), actualTotal, chunksize)
		return true
	}

	return false
}

/*
 * 1. Create a new chunk
 * 2. bind it to vuid
 */
func (dsw *DiskStorageWrapper) CreateChunk(ctx context.Context,
	vuid proto.Vuid, chunksize int64) (cs core.ChunkAPI, err error,
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
	if stats.Free < chunksize {
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

	chunkHandler, err := ds.store.OpenChunk(ctx, chunkId, chunksize)
	if err != nil {
		span.Errorf("vuid:%v open:%v", vuid, err)
		return nil, err
	}

	// create chunk storage
	cs, err = chunk.NewChunkStorage(ctx, chunkHandler, vm, dsw.readPool, dsw.writePool, func(option *core.Option) {
		option.CreateDataIfMiss = true
		// option.DB = ds.SuperBlock.db
		option.Conf = ds.Conf
		option.IoQos = ds.dataQos
		option.Disk = dsw
	})
	if err != nil {
		span.Errorf("Failed new chunk:<%s>, err:%v", chunkHandler.String(), err)
		return nil, err
	}

	// update bind it to vuid
	err = ds.store.UpdateChunkMeta(ctx, chunkId, vm)
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

func (ds *DiskStorage) loopAttach(f func()) {
	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		f()
	}()
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

	var sto store.Store
	if sto, err = store.NewStore(ctx, conf.Store); err != nil {
		return nil, err
	}

	dm := sto.LoadFormat(ctx)
	if !dm.Registered {
		dm, err = registerDisk(ctx, sto, &conf)
		if err != nil {
			span.Errorf("register disk failed: %v", err)
			return nil, err
		}
	}
	span.Infof("diskID:%d", dm.DiskID)

	// check format info
	formatInfo := sto.LoadFormat(ctx)
	if formatInfo.DiskID != dm.DiskID {
		span.Errorf("unexpected error. diskId not match. format:%v, dm:%v", formatInfo, dm)
		return nil, bloberr.ErrUnexpected
	}

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

	dataQos, err := qos.NewIoQueueQos(conf.DataQos)
	if err != nil {
		span.Errorf("Failed new io qos, err:%v", err)
		return nil, err
	}

	// setting io pools
	metricConf := taskpool.IoPoolMetricConf{
		ClusterID: uint32(conf.HostInfo.ClusterID),
		IDC:       conf.HostInfo.IDC,
		Rack:      conf.HostInfo.Rack,
		Host:      conf.HostInfo.Host,
		DiskID:    uint32(dm.DiskID),
		Namespace: "blobstore",
		Subsystem: "blobnode",
	}
	// default: 1 queue, 1 thread, 32 depth;  qos max wait cnt 32*2
	writePool := taskpool.NewWritePool(conf.WriteThreadCnt, conf.WriteQueueDepth, metricConf)
	// default: 1 queue, 4 thread, 64 depth;  qos max wait cnt 64*2
	readPool := taskpool.NewReadPool(conf.ReadThreadCnt, conf.ReadQueueDepth, metricConf)

	ds = &DiskStorage{
		DiskID:           dm.DiskID,
		ChunkLimitPerKey: keycount.NewBlockingKeyCountLimit(1),
		Conf:             &conf,
		closeCh:          make(chan struct{}),
		compactCh:        make(chan proto.Vuid),
		ctx:              ctx,
		status:           dm.Status,
		dataQos:          dataQos,
		CreateAt:         dm.Ctime,
		LastUpdateAt:     dm.Mtime,
		writePool:        writePool,
		readPool:         readPool,
		store:            sto,
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

func registerDisk(ctx context.Context, sto store.Store, conf *core.Config) (dm core.DiskMeta, err error) {
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
			Ctime:   now,
		},
	}

	// dm.Host =
	dm = core.DiskMeta{
		FormatInfo: *format,
		Mtime:      now,
		Registered: true,
		Status:     proto.DiskStatusNormal,
		Path:       conf.Path,
	}

	err = sto.Format(ctx, dm)
	if err != nil {
		span.Errorf("Failed upsert disk: %d, err:%v", dm.DiskID, err)
		return
	}
	span.Infof("register disk(%v) success", diskID)
	return
}

func (dsw *DiskStorageWrapper) RestoreChunkStorage(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	if err = dsw.store.Load(ctx); err != nil {
		span.Error("load", err)
		return err
	}

	ds := dsw.DiskStorage
	sto := ds.store

	// load chunkmeta
	vuidMaps, err := sto.ListVuidMetas(ctx)
	if err != nil {
		span.Errorf("Failed list chunks: %v", err)
		return err
	}

	vuidMetas, err := sto.ListChunkMetas(ctx)
	if err != nil {
		span.Errorf("Failed list chunks: %v", err)
		return err
	}

	chunks := make(map[proto.Vuid]core.ChunkAPI)
	for vuid, chunkid := range vuidMaps {
		span.Debugf("vuid:%d, chunkid: %s", vuid, chunkid)

		vm := vuidMetas[chunkid]
		if vm.Status == clustermgr.ChunkStatusRelease {
			span.Warnf("vuid:%d(chunk:%s) status is release", vm.Vuid, vm.ChunkID)
			continue
		}
		if vm.Compacting {
			vm.Compacting = false
			err := sto.UpdateChunkMeta(ctx, chunkid, vm)
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

		// TODO: size
		chunkHandler, err := sto.OpenChunk(ctx, chunkid, 0)
		cs, err := chunk.NewChunkStorage(ctx, chunkHandler, vm, ds.readPool, ds.writePool, func(o *core.Option) {
			o.Conf = ds.Conf
			// o.DB = sb.db
			o.Disk = dsw
			o.IoQos = ds.dataQos
			o.CreateDataIfMiss = false
		})
		if err != nil {
			span.Errorf("Failed New chunk, path:%s, vm:%v", ds.Conf.Path, vm)
			return err
		}

		chunks[vm.Vuid] = cs
	}

	ds.Lock.Lock()
	ds.Chunks = chunks
	ds.Lock.Unlock()

	span.Debugf("build ChunkStorage success")
	return
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

	if err = ds.store.DeleteChunk(ctx, cs.ID()); err != nil {
		span.Errorf("Failed delete vuid:%d chunk:%s", vuid, cs.ID())
		return err
	}

	// delete node from map
	ds.Lock.Lock()
	delete(ds.Chunks, vuid)
	ds.Lock.Unlock()

	span.Infof("release chunk<%s> success", cs.ID())
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
		return nil
	}

	// can not convert status
	if !isValidStateTransition(vm.Status, status) {
		span.Errorf("can not convert chunk(%s) status:%v to %v", cs.ID(), vm.Status, status)
		return bloberr.ErrUnexpected
	}

	vm.Status = status
	vm.Mtime = time.Now().UnixNano()

	err = ds.store.UpdateChunkMeta(ctx, cs.ID(), *vm)
	if err != nil {
		span.Errorf("update chunk(%s) status to %v failed: %v", vm.ChunkID, status, err)
		return err
	}

	// update ChunkStorage status in memory
	cs.SetStatus(status)
	return nil
}

func (ds *DiskStorage) UpdateChunkCompactState(ctx context.Context,
	vuid proto.Vuid, compacting bool) (err error,
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

	err = ds.store.UpdateChunkMeta(ctx, cs.ID(), *vm)
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
	dm := ds.store.LoadFormat(ctx)

	// persistence disk status
	dm.Status = status
	dm.Mtime = time.Now().UnixNano()
	err = ds.store.UpdateFormatInfo(ctx, ds.DiskID, dm)
	if err != nil {
		span.Errorf("update disk(%v) persistence status failed: %v", ds.DiskID, err)
		return err
	}

	// disk status in memory
	ds.Lock.Lock()
	ds.status = status
	ds.Lock.Unlock()

	return
}

func (ds *DiskStorage) ListChunks(ctx context.Context) (chunks []core.VuidMeta, err error) {
	chunksmap, err := ds.store.ListChunkMetas(ctx)
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
	return ds.store.LoadFormat(ctx), nil
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
		span.Errorf("list chunks failed: %v", err)
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

		chunkid, err := ds.store.GetVuidBind(ctx, ck.Vuid)
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

	err = ds.store.DeleteChunk(ctx, id)
	if err != nil {
		span.Errorf("Failed Delete Chunk:%s err:%v", id, err)
		return
	}

	span.Infof("disk(%v) clean chunk(%v) success", ds.DiskID, id)
	return nil
}

func (ds *DiskStorage) GetChunkStorage(vuid proto.Vuid) (cs core.ChunkAPI, found bool) {
	ds.Lock.RLock()
	cs, ok := ds.Chunks[vuid]
	ds.Lock.RUnlock()

	if !ok {
		return nil, false
	}
	return cs, true
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
		span.Errorf("list chunks failed: %v", err)
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
