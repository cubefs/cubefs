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

package chunk

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
)

const (
	DefaultChunkExpandRate  = 2
	DefaultMaxChunkFileSize = 8 * (1 << 40) // 8 TiB
)

type Chunk struct {
	*chunk
}

type storageWrapper struct {
	core.Storage
}

type chunk struct {
	version uint8
	conf    *core.Config
	vuid    proto.Vuid
	diskID  proto.DiskID
	lock    sync.RWMutex

	// storageWrapper ( meta & data )
	stg  atomic.Value
	disk core.DiskAPI

	// hook fn
	onClosed func()

	// shard requests
	consistent *core.ConsistencyController

	// statics
	stats    ChunkStats
	fileInfo FileInfo

	// compact
	compacting      bool
	bidlimiter      limit.Limiter
	lastCompactTime int64
	compactTask     atomic.Value

	// status
	dirty          uint32
	status         bnapi.ChunkStatus
	closed         bool
	lastModifyTime int64
}

type FileInfo struct {
	Total uint64 `json:"total"` // ChunkSize (readonly)
	Used  uint64 `json:"used"`  // user data size ( actual use size, phy size, live Update)
	Free  uint64 `json:"free"`  // ChunkSize - Used
	Size  uint64 `json:"size"`  // Chunk File Size ( file logic size)
}

func newChunkStorage(ctx context.Context, dataPath string, vm core.VuidMeta, opts ...core.OptionFunc) (
	cs *chunk, err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	// chunk data
	chunkFile := filepath.Join(dataPath, vm.ChunkId.String())

	opt := core.Option{}
	for _, fn := range opts {
		fn(&opt)
	}

	// new chunkData fd
	cd, err := storage.NewChunkData(ctx, vm, chunkFile, opt.Conf, opt.CreateDataIfMiss, opt.IoQos)
	if err != nil {
		span.Errorf("Failed new chunk data. dp:%s, err:%v", dataPath, err)
		return nil, err
	}

	// create meta fd
	cm, err := storage.NewChunkMeta(ctx, opt.Conf, vm, opt.DB)
	if err != nil {
		span.Errorf("Failed new chunk meta. vm:%v, err:%v", vm, err)
		return nil, err
	}

	cs = &chunk{
		version:        vm.Version,
		vuid:           vm.Vuid,
		diskID:         vm.DiskID,
		disk:           opt.Disk,
		conf:           opt.Conf,
		status:         vm.Status,
		compacting:     vm.Compacting,
		bidlimiter:     keycount.NewBlockingKeyCountLimit(1),
		consistent:     core.NewConsistencyController(),
		lastModifyTime: vm.Mtime,
	}

	// init compact task
	cs.resetCompactTask()

	// init stg
	stg := storage.NewStorage(cm, cd)
	// enhence stg, with inline feat
	stg = storage.NewTinyFileStg(stg, opt.Conf.TinyFileThresholdB)

	cs.setStg(stg)

	cs.fileInfo.Total = uint64(vm.ChunkSize)
	err = cs.refreshFstat(ctx)
	if err != nil {
		span.Errorf("Failed chunk storage init, err:%v", err)
		return nil, err
	}

	return cs, err
}

func NewChunkStorage(ctx context.Context, dataPath string, vm core.VuidMeta, opts ...core.OptionFunc) (
	cs *Chunk, err error,
) {
	c, err := newChunkStorage(ctx, dataPath, vm, opts...)
	if err != nil {
		return nil, err
	}

	cs = &Chunk{chunk: c}

	// It will be automatically recycled when gc
	runtime.SetFinalizer(cs, func(wapper *Chunk) {
		wapper.Close(context.Background())
	})

	return cs, nil
}

func (cs *chunk) MarshalJSON() ([]byte, error) {
	datas := make(map[string]interface{})

	cs.lock.RLock()
	datas["vuid"] = cs.vuid
	datas["stats"] = cs.stats
	datas["chunk_info"] = cs.fileInfo
	datas["compacting"] = cs.compacting
	datas["last_modify_time"] = time.Unix(0, cs.lastModifyTime)
	datas["last_compact_time"] = time.Unix(0, cs.lastCompactTime)
	cs.lock.RUnlock()

	stg := cs.getStg()
	datas["stg_pending_reqs"] = stg.PendingRequest()

	return json.Marshal(datas)
}

func (cs *chunk) UnmarshalJSON(data []byte) (err error) {
	return bloberr.ErrUnexpected
}

func (cs *chunk) resetCompactTask() {
	task := &compactTask{
		stopCh: make(chan struct{}),
	}
	cs.compactTask.Store(task)
}

func (cs *chunk) refreshFstat(ctx context.Context) (err error) {
	stg := cs.getStg()

	stat, err := stg.Stat(ctx)
	if err != nil {
		return err
	}

	fsize, physize := stat.FileSize, stat.PhySize

	info := &cs.fileInfo

	atomic.StoreUint64(&info.Used, uint64(physize))

	total := atomic.LoadUint64(&info.Total)
	used := atomic.LoadUint64(&info.Used)

	free := int64(total - used)
	if free < 0 {
		atomic.StoreUint64(&info.Free, 0)
	} else {
		atomic.StoreUint64(&info.Free, uint64(free))
	}
	atomic.StoreUint64(&info.Size, uint64(fsize))

	return nil
}

func (cs *chunk) SyncData(ctx context.Context) (err error) {
	elem := cs.consistent.Begin(struct{}{})
	defer cs.consistent.End(elem)

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	return stg.SyncData(ctx)
}

func (cs *chunk) Sync(ctx context.Context) (err error) {
	elem := cs.consistent.Begin(struct{}{})
	defer cs.consistent.End(elem)

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	return stg.Sync(ctx)
}

/*
Need Shard:
	- Size
	- Flag
	- Body (Reader)
Fill Shard:
	- Offset
	- Crc
*/
func (cs *chunk) Write(ctx context.Context, b *core.Shard) (err error) {
	if b.Vuid != cs.vuid {
		return bloberr.ErrVuidNotMatch
	}

	elem := cs.consistent.Begin(b.Bid)
	defer cs.consistent.End(elem)

	// statistics
	cs.stats.writeBefore()
	defer cs.stats.writeAfter(uint64(b.Size), time.Now())

	cs.lock.RLock()

	if cs.compacting {
		cs.bidlimiter.Acquire(b.Bid)
		defer cs.bidlimiter.Release(b.Bid)
	}
	stg := cs.GetStg()
	defer cs.PutStg(stg)

	cs.lock.RUnlock()

	if err = stg.Write(ctx, b); err != nil {
		return err
	}

	// update stats
	atomic.AddUint64(&cs.fileInfo.Used, uint64(core.Alignphysize(int64(b.Size))))
	atomic.StoreUint32(&cs.dirty, 1)

	return nil
}

func (cs *chunk) Disk() core.DiskAPI {
	return cs.disk
}

func (cs *chunk) ID() bnapi.ChunkId {
	stg := cs.getStg()
	return stg.ID()
}

func (cs *chunk) Vuid() proto.Vuid {
	return cs.vuid
}

func (cs *chunk) ChunkInfo(ctx context.Context) (info bnapi.ChunkInfo) {
	span := trace.SpanFromContextSafe(ctx)

	err := cs.refreshFstat(ctx)
	if err != nil {
		span.Errorf("Failed refresh fstat, err:%v", err)
		return bnapi.ChunkInfo{}
	}

	cs.lock.RLock()
	defer cs.lock.RUnlock()

	stg := cs.getStg()

	info.Id = stg.ID()
	info.Vuid = cs.vuid
	info.DiskID = cs.diskID

	info.Total = atomic.LoadUint64(&cs.fileInfo.Total)
	info.Used = atomic.LoadUint64(&cs.fileInfo.Used)
	info.Free = atomic.LoadUint64(&cs.fileInfo.Free)
	info.Size = atomic.LoadUint64(&cs.fileInfo.Size)

	info.Status = cs.status
	info.Compacting = cs.compacting

	return info
}

func (cs *chunk) vuidMeta() (vm *core.VuidMeta) {
	stg := cs.getStg()

	stat, _ := stg.Stat(context.TODO())

	vm = &core.VuidMeta{
		Version:     cs.version,
		Vuid:        cs.vuid,
		DiskID:      cs.diskID,
		ChunkId:     stg.ID(),
		ParentChunk: stat.ParentID,
		ChunkSize:   int64(cs.fileInfo.Total),
		Ctime:       stat.CreateTime,
		Mtime:       cs.lastModifyTime,
		Status:      cs.status,
		Compacting:  cs.compacting,
	}
	return vm
}

func (cs *chunk) VuidMeta() (vm *core.VuidMeta) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	return cs.vuidMeta()
}

/*
Fill Shard:
	- Offset
	- Size
	- Crc
	- Flag
	- Body (Reader)
*/
func (cs *chunk) NewReader(ctx context.Context, id proto.BlobID) (s *core.Shard, err error) {
	elem := cs.consistent.Begin(id)
	defer cs.consistent.End(elem)

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	m, err := stg.ReadShardMeta(ctx, id)
	if err != nil {
		return nil, err
	}

	return cs.newRangeReader(ctx, stg, id, m, 0, int64(m.Size))
}

/*
Fill Shard:
	- Offset
	- Size
	- Crc
	- Flag
	- Body (Reader)
*/
func (cs *chunk) NewRangeReader(ctx context.Context, id proto.BlobID, from, to int64) (s *core.Shard, err error) {
	elem := cs.consistent.Begin(id)
	defer cs.consistent.End(elem)

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	m, err := stg.ReadShardMeta(ctx, id)
	if err != nil {
		return nil, err
	}

	if !(from >= 0 && to <= int64(m.Size) && from <= to) {
		return nil, bloberr.ErrInvalidParam
	}

	return cs.newRangeReader(ctx, stg, id, m, from, to)
}

func (cs *chunk) newRangeReader(ctx context.Context, stg core.Storage, id proto.BlobID, sm *core.ShardMeta, from, to int64) (
	s *core.Shard, err error,
) {
	s = core.NewShardReader(id, cs.vuid, from, to, nil)

	s.FillMeta(*sm)

	// read data
	rc, err := stg.NewRangeReader(ctx, s, from, to)
	if err != nil {
		return nil, err
	}

	s.Body = rc

	return s, nil
}

/*
Need Shard:
	- Writer 	(To net)
Fill Shard:
	- From, To 	(may fix)
	- Offset
	- Size
	- Crc
	- Flag
*/
func (cs *chunk) Read(ctx context.Context, b *core.Shard) (n int64, err error) {
	span := trace.SpanFromContextSafe(ctx)

	elem := cs.consistent.Begin(b.Bid)
	defer cs.consistent.End(elem)

	// statistics
	cs.stats.readBefore()

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	start := time.Now()

	// read meta
	m, err := stg.ReadShardMeta(ctx, b.Bid)
	span.AppendTrackLog("md.r", start, err)
	if err != nil {
		cs.stats.readAfter(0, time.Now())
		return 0, err
	}
	defer cs.stats.readAfter(uint64(m.Size), time.Now())

	// fix [from, to)
	b.From, b.To = 0, int64(m.Size)

	return cs.rangeRead(ctx, stg, b, m)
}

/*
Need Shard:
	- From 		(may fix)
	- To 		(may fix)
	- Writer 	(To net)
Fill Shard:
	- Offset
	- Size
	- Crc
	- Flag
*/
func (cs *chunk) RangeRead(ctx context.Context, b *core.Shard) (n int64, err error) {
	span := trace.SpanFromContextSafe(ctx)

	elem := cs.consistent.Begin(b.Bid)
	defer cs.consistent.End(elem)

	// statistics
	cs.stats.rangereadBefore()

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	start := time.Now()

	// read meta
	m, err := stg.ReadShardMeta(ctx, b.Bid)
	span.AppendTrackLog("md.r", start, err)
	if err != nil {
		cs.stats.readAfter(0, time.Now())
		return 0, err
	}
	defer cs.stats.rangereadAfter(uint64(m.Size), time.Now())

	// check [from, to) and modify
	b.From, b.To, err = base.FixHttpRange(b.From, b.To, int64(m.Size))
	if err != nil {
		return 0, bloberr.ErrRequestedRangeNotSatisfiable
	}

	return cs.rangeRead(ctx, stg, b, m)
}

func (cs *chunk) rangeRead(ctx context.Context, stg core.Storage, s *core.Shard, sm *core.ShardMeta) (n int64, err error) {
	span := trace.SpanFromContextSafe(ctx)

	s.FillMeta(*sm)

	from, to := s.From, s.To

	// create reader(data)
	rc, err := stg.NewRangeReader(ctx, s, from, to)
	if err != nil {
		return 0, err
	}

	// begin io
	if s.PrepareHook != nil {
		s.PrepareHook(s)
	}

	// after io
	if s.AfterHook != nil {
		defer s.AfterHook(s)
	}

	tw := base.NewTimeWriter(s.Writer)
	tr := base.NewTimeReader(rc)

	n, err = io.CopyN(tw, tr, int64(to-from))
	span.AppendTrackLogWithDuration("net.w", tw.Duration(), err)
	span.AppendTrackLogWithDuration("dat.r", tr.Duration(), err)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (cs *chunk) MarkDelete(ctx context.Context, bid proto.BlobID) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// statistics
	cs.stats.markdeleteBefore()
	defer cs.stats.markdeleteAfter(time.Now())

	cs.lock.RLock()

	if cs.compacting {
		cs.lock.RUnlock()
		return bloberr.ErrChunkInCompact
	}

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	cs.lock.RUnlock()

	err = stg.MarkDelete(ctx, bid)
	if err != nil {
		span.Errorf("Failed mark delete bid:%d, err:%v", bid, err)
		return err
	}

	return nil
}

func (cs *chunk) Delete(ctx context.Context, bid proto.BlobID) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// statistics
	cs.stats.deleteBefore()
	defer cs.stats.deleteAfter(time.Now())

	cs.lock.RLock()

	if cs.compacting {
		cs.lock.RUnlock()
		return bloberr.ErrChunkInCompact
	}

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	cs.lock.RUnlock()

	n, err := stg.Delete(ctx, bid)
	if err != nil {
		span.Errorf("Failed delete, bid:%v, err:%v", bid, err)
		return err
	}

	// update stats
	atomic.AddUint64(&cs.fileInfo.Used, -uint64(core.Alignphysize(n)))
	atomic.StoreUint32(&cs.dirty, 1)

	return nil
}

func (cs *chunk) ReadShardMeta(ctx context.Context, bid proto.BlobID) (sm *core.ShardMeta, err error) {
	span := trace.SpanFromContextSafe(ctx)

	elem := cs.consistent.Begin(bid)
	defer cs.consistent.End(elem)

	// statistics
	cs.stats.readmetaBefore()
	defer cs.stats.readmetaAfter(time.Now())

	stg := cs.GetStg()
	defer cs.PutStg(stg)

	shard, err := stg.ReadShardMeta(ctx, bid)
	if err != nil {
		span.Errorf("Failed read shardmeta bid:%d, err:%v", bid, err)
		return nil, err
	}

	return shard, nil
}

func (cs *chunk) ListShards(ctx context.Context, startBid proto.BlobID, cnt int, status bnapi.ShardStatus) (
	infos []*bnapi.ShardInfo, next proto.BlobID, err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	elem := cs.consistent.Begin(startBid)
	defer cs.consistent.End(elem)

	// statistics
	cs.stats.listshardBefore()
	defer cs.stats.listshardAfter(time.Now())

	stg := cs.GetStg()
	defer cs.PutStg(stg)
	infos = make([]*bnapi.ShardInfo, 0, cnt)
	fn := func(bid proto.BlobID, shard *core.ShardMeta) error {
		if len(infos) >= cnt {
			return core.ErrEnoughShardNumber
		}
		if shard.Flag != status && status != bnapi.ShardStatusDefault {
			return nil
		}

		infos = append(infos, &bnapi.ShardInfo{
			Vuid:   cs.vuid,
			Bid:    bid,
			Size:   int64(shard.Size),
			Crc:    shard.Crc,
			Flag:   shard.Flag,
			Inline: shard.Inline,
		})

		next = bid

		return nil
	}

	err = stg.ScanMeta(ctx, startBid, cnt, fn)
	if err != nil {
		if err == core.ErrEnoughShardNumber || err == core.ErrChunkScanEOF {
			if err == core.ErrChunkScanEOF {
				next = proto.InValidBlobID
			}
			err = nil
			span.Debugf("finished scan shard info: err: %v", err)
			return
		}
		span.Errorf("scan vuid:%v shard occur error: %v", cs.vuid, err)
		return nil, proto.InValidBlobID, err
	}

	return
}

func (cs *chunk) close(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	if cs.closed {
		span.Panicf("chunk already closed. vuid:%v", cs.vuid)
		return
	}

	if cs.onClosed != nil {
		cs.onClosed()
	}

	stg := cs.getStg()

	span.Infof("== closing vuid:[%v], chunkid:[%v] ==", cs.vuid, stg.ID())

	stg.Close(ctx)

	cs.setStg(nil)

	cs.disk = nil

	cs.closed = true
}

func (cs *chunk) Close(ctx context.Context) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	cs.close(ctx)
}

func (cs *chunk) Destroy(ctx context.Context) {
	stg := cs.getStg()

	stg.Destroy(ctx)

	cs.lock.Lock()
	defer cs.lock.Unlock()

	if !cs.closed {
		cs.close(ctx)
	}
}

func (cs *chunk) AllowModify() (err error) {
	ds := cs.Disk()

	if ds.Status() >= proto.DiskStatusBroken {
		return bloberr.ErrDiskBroken
	}

	// check chunk status
	cs.lock.RLock()
	compacting, status := cs.compacting, cs.status
	cs.lock.RUnlock()

	config := ds.GetConfig()
	if compacting && config.DisableModifyInCompacting {
		return bloberr.ErrChunkInCompact
	}
	if status == bnapi.ChunkStatusReadOnly {
		return bloberr.ErrReadonlyVUID
	}
	if status == bnapi.ChunkStatusRelease {
		return bloberr.ErrReleaseVUID
	}

	return nil
}

func (cs *chunk) HasEnoughSpace(needSize int64) bool {
	size := atomic.LoadUint64(&cs.fileInfo.Size)
	used := atomic.LoadUint64(&cs.fileInfo.Used)
	total := atomic.LoadUint64(&cs.fileInfo.Total)

	if size+uint64(needSize) >= DefaultMaxChunkFileSize {
		return false
	}

	if used+uint64(needSize) >= total*DefaultChunkExpandRate {
		return false
	}

	diskStats := cs.Disk().Stats()
	reserved := cs.conf.DiskReservedSpaceB
	compactReserved := cs.conf.CompactReservedSpaceB

	return needSize+reserved-compactReserved < diskStats.Free
}

func (cs *chunk) SetStatus(status bnapi.ChunkStatus) (err error) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	cs.status = status
	return nil
}

func (cs *chunk) Status() (status bnapi.ChunkStatus) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	return cs.status
}

func (cs *chunk) CasDirty(old, new uint32) (swapped bool) {
	return atomic.CompareAndSwapUint32(&cs.dirty, old, new)
}

func (cs *chunk) SetDirty(dirty bool) {
	var flag uint32
	if dirty {
		flag = 1
	}
	atomic.StoreUint32(&cs.dirty, flag)
}

func (cs *chunk) IsDirty() bool {
	return atomic.LoadUint32(&cs.dirty) != 0
}

func (cs *chunk) IsClosed() bool {
	return cs.closed
}

func (cs *chunk) RefreshFstat(ctx context.Context) (err error) {
	return cs.refreshFstat(ctx)
}

func (cs *chunk) HasPendingRequest() bool {
	return cs.getStg().PendingRequest() != 0
}

func (cs *chunk) GetStg() core.Storage {
	stg := cs.getStg()
	stg.IncrPendingCnt()
	return stg
}

func (cs *chunk) PutStg(stg core.Storage) {
	stg.DecrPendingCnt()
}

func (cs *chunk) getStg() core.Storage {
	stg := cs.stg.Load().(*storageWrapper)
	return stg.Storage
}

func (cs *chunk) setStg(stg core.Storage) {
	if _, ok := stg.(*storageWrapper); ok {
		panic("wrong stg type")
	}

	wrapper := &storageWrapper{Storage: stg}
	cs.stg.Store(wrapper)
}
