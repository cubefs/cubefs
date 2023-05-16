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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
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

var StateTransitionRules = map[bnapi.ChunkStatus][]bnapi.ChunkStatus{
	bnapi.ChunkStatusDefault:  {bnapi.ChunkStatusNormal},
	bnapi.ChunkStatusNormal:   {bnapi.ChunkStatusNormal, bnapi.ChunkStatusReadOnly},
	bnapi.ChunkStatusReadOnly: {bnapi.ChunkStatusNormal, bnapi.ChunkStatusReadOnly, bnapi.ChunkStatusRelease},
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
	dataQos qos.Qos

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

		// clean superblock
		sb := ds.SuperBlock
		if sb != nil {
			sb.Close(ctx)
			ds.SuperBlock = nil
		}

		ds.closed = true
	}()
}

func (ds *DiskStorage) DiskInfo() (info bnapi.DiskInfo) {
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
func (dsw *DiskStorageWrapper) CreateChunk(ctx context.Context, vuid proto.Vuid, chunksize int64) (
	cs core.ChunkAPI, err error) {
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
	chunkId := bnapi.NewChunkId(vuid)
	nowtime := time.Now().UnixNano()

	vm := core.VuidMeta{
		Version:   _chunkVer[0],
		Vuid:      vuid,
		DiskID:    ds.DiskID,
		ChunkId:   chunkId,
		ChunkSize: chunksize,
		Ctime:     nowtime,
		Mtime:     nowtime,
		Status:    bnapi.ChunkStatusNormal,
	}

	// create chunk storage
	cs, err = chunk.NewChunkStorage(ctx, ds.DataPath, vm, func(option *core.Option) {
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
	err = super.UpsertChunk(ctx, vm.ChunkId, vm)
	if err != nil {
		span.Errorf("Failed upsert chunk<%s>, err:%v", vm.ChunkId, err)
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

	// io visualization: init meta io stat
	metaios, err := flow.NewIOFlowStat(fmt.Sprintf("md_%v", dm.DiskID), conf.IOStatFileDryRun)
	if err != nil {
		span.Errorf("Failed new io flow stat, err:%v", err)
		return nil, err
	}

	sb.SetIOStat(metaios)

	// io visualization: init data io stat
	dataios, err := flow.NewIOFlowStat(dm.DiskID.ToString(), conf.IOStatFileDryRun)
	if err != nil {
		span.Errorf("Failed new dataio flow stat, err:%v", err)
		return nil, err
	}
	diskView := flow.NewDiskViewer(dataios)

	// init Qos Manager
	conf.DataQos.DiskViewer = diskView
	conf.DataQos.StatGetter = dataios

	dataQos, err := qos.NewQosManager(conf.DataQos)
	if err != nil {
		span.Errorf("Failed new io qos, err:%v", err)
		return nil, err
	}

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

func (dsw *DiskStorageWrapper) RestoreChunkStorage(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	ds := dsw.DiskStorage
	sb := ds.SuperBlock

	// load chunkmeta
	vuidMaps, err := sb.ListVuids(ctx)
	if err != nil {
		span.Errorf("Failed list chunks: %v", err)
		return err
	}

	vuidMetas, err := sb.ListChunks(ctx)
	if err != nil {
		span.Errorf("Failed list chunks: %v", err)
		return err
	}

	chunks := make(map[proto.Vuid]core.ChunkAPI)
	for vuid, chunkid := range vuidMaps {
		span.Debugf("vuid:%d, chunkid: %s", vuid, chunkid)

		vm := vuidMetas[chunkid]
		if vm.Status == bnapi.ChunkStatusRelease {
			span.Warnf("vuid:%d(chunk:%s) status is release", vm.Vuid, vm.ChunkId)
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
		cs, err := chunk.NewChunkStorage(ctx, ds.DataPath, vm, func(o *core.Option) {
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

	if ds.status >= proto.DiskStatusBroken {
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
	if !force && !isValidStateTransition(cs.Status(), bnapi.ChunkStatusRelease) {
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
	vm.Status = bnapi.ChunkStatusRelease
	vm.Reason = bnapi.ReleaseForUser
	vm.Mtime = time.Now().UnixNano()

	err = ds.SuperBlock.UpsertChunk(ctx, cs.ID(), *vm)
	if err != nil {
		span.Errorf("update chunk(%s) status to release failed: %v", vm.ChunkId, err)
		return err
	}

	// update ChunkStorage status in memory
	cs.SetStatus(bnapi.ChunkStatusRelease)

	cs = nil

	span.Infof("release chunk<%s> success", vm.ChunkId)

	return nil
}

/*
 * chunk status changing must call this method
 * first: change persistence status
 * second: change status in memory
 * concurrency safety: only allows serial execution for the same vuid
 */
func (ds *DiskStorage) UpdateChunkStatus(ctx context.Context, vuid proto.Vuid, status bnapi.ChunkStatus) (err error) {
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
		span.Errorf("update chunk(%s) status to %v failed: %v", vm.ChunkId, status, err)
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
			vm.ChunkId, compacting, err)
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
	if meta.Reason != bnapi.ReleaseForCompact {
		return false
	}
	span.Debugf("id:%s meta:%v without protection", meta.ChunkId, meta)
	return true
}

func (ds *DiskStorage) cleanReleasedChunks() (err error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", bncom.BackgroudReqID("CleanChunk"+ds.Conf.Path))

	span.Debugf("come in CleanChunks.")

	// set io type
	ctx = bnapi.Setiotype(ctx, bnapi.InternalIO)

	protectionPeriod := time.Duration(ds.Conf.ChunkReleaseProtectionM)
	now := time.Now().UnixNano()

	chunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("%v list chunks failed: %v", ds.MetaPath, err)
		return
	}

	for _, ck := range chunks {
		if ck.Status != bnapi.ChunkStatusRelease {
			continue
		}

		if !ds.sceneWithoutProtection(ctx, ck) && now-ck.Mtime < int64(time.Minute*protectionPeriod) {
			span.Debugf("%s still in protection period", ck.ChunkId)
			continue
		}

		chunkid, err := ds.SuperBlock.ReadVuidBind(ctx, ck.Vuid)
		if err == nil && chunkid == ck.ChunkId {
			span.Warnf("can not happen. vuid:%d bind %s. skip", ck.Vuid, ck.ChunkId)
			continue
		}

		if err = ds.realCleanChunk(ctx, ck.ChunkId); err != nil {
			span.Errorf("failed clean chunk:%s, err:%v", ck.ChunkId, err)
			continue
		}
	}

	return
}

func (ds *DiskStorage) realCleanChunk(ctx context.Context, id bnapi.ChunkId) (err error) {
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
func (ds *DiskStorage) cleanChunk(ctx context.Context, id bnapi.ChunkId, toTrash bool) (err error) {
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

func (ds *DiskStorage) GetChunkStorage(vuid proto.Vuid) (cs core.ChunkAPI, found bool) {
	ds.Lock.RLock()
	defer ds.Lock.RUnlock()

	cs, ok := ds.Chunks[vuid]
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

func isValidStateTransition(src, dest bnapi.ChunkStatus) bool {
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
