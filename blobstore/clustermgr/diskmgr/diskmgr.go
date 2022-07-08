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

package diskmgr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	DiskIDScopeName                 = "diskid"
	defaultRefreshIntervalS         = 300
	defaultHeartbeatExpireIntervalS = 60
	defaultFlushIntervalS           = 600
	defaultApplyConcurrency         = 10
	defaultListDiskMaxCount         = 200
)

var (
	ErrDiskExist                 = errors.New("disk already exist")
	ErrDiskNotExist              = errors.New("disk not exist")
	ErrNoEnoughSpace             = errors.New("no enough space to alloc")
	ErrBlobNodeCreateChunkFailed = errors.New("blob node create chunk failed")
)

var validSetStatus = map[proto.DiskStatus]int{
	proto.DiskStatusNormal:    0,
	proto.DiskStatusBroken:    1,
	proto.DiskStatusRepairing: 2,
	proto.DiskStatusRepaired:  3,
	proto.DiskStatusDropped:   4,
}

type DiskMgrAPI interface {
	// AllocDiskID return a unused disk id
	AllocDiskID(ctx context.Context) (proto.DiskID, error)
	// GetDiskInfo return disk info, it return ErrDiskNotFound if disk not found
	GetDiskInfo(ctx context.Context, id proto.DiskID) (*blobnode.DiskInfo, error)
	// CheckDiskInfoDuplicated return true if disk info already exit, like host and path duplicated
	CheckDiskInfoDuplicated(ctx context.Context, info *blobnode.DiskInfo) bool
	// IsDiskWritable judge disk if writable, disk status unmoral or readonly or heartbeat timeout will return true
	IsDiskWritable(ctx context.Context, id proto.DiskID) (bool, error)
	// SetStatus change disk status, in some case, change status is not allow
	// like change repairing/repaired/dropped into normal
	SetStatus(ctx context.Context, id proto.DiskID, status proto.DiskStatus, isCommit bool) error
	// IsDroppingDisk return true if the specified disk is dropping
	IsDroppingDisk(ctx context.Context, id proto.DiskID) (bool, error)
	// ListDroppingDisk return all dropping disk info
	ListDroppingDisk(ctx context.Context) ([]*blobnode.DiskInfo, error)
	// AllocChunk return available chunks in data center
	AllocChunks(ctx context.Context, policy *AllocPolicy) ([]proto.DiskID, error)
	// ListDiskInfo
	ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (*clustermgr.ListDiskRet, error)
	// Stat return disk statistic info of a cluster
	Stat(ctx context.Context) *clustermgr.SpaceStatInfo
	// SwitchReadonly can switch disk's readonly or writable
	SwitchReadonly(diskID proto.DiskID, readonly bool) error
	// GetHeartbeatChangeDisks return any heartbeat change disks
	GetHeartbeatChangeDisks() []HeartbeatEvent
}

type AllocPolicy struct {
	Idc      string
	Vuids    []proto.Vuid
	Excludes []proto.DiskID
}

type HeartbeatEvent struct {
	DiskID  proto.DiskID
	IsAlive bool
}

type DiskMgrConfig struct {
	RefreshIntervalS         int             `json:"refresh_interval_s"`
	RackAware                bool            `json:"rack_aware"`
	HostAware                bool            `json:"host_aware"`
	HeartbeatExpireIntervalS int             `json:"heartbeat_expire_interval_s"`
	FlushIntervalS           int             `json:"flush_interval_s"`
	ApplyConcurrency         uint32          `json:"apply_concurrency"`
	BlobNodeConfig           blobnode.Config `json:"blob_node_config"`
	AllocTolerateBuffer      int64           `json:"alloc_tolerate_buffer"`
	EnsureIndex              bool            `json:"ensure_index"`

	IDC       []string            `json:"-"`
	CodeModes []codemode.CodeMode `json:"-"`
	ChunkSize int64               `json:"-"`
}

type DiskMgr struct {
	module         string
	allDisks       map[proto.DiskID]*diskItem
	allocators     map[string]*atomic.Value
	taskPool       *base.TaskDistribution
	hostPathFilter sync.Map

	scopeMgr       scopemgr.ScopeMgrAPI
	diskTbl        *normaldb.DiskTable
	droppedDiskTbl *normaldb.DroppedDiskTable
	blobNodeClient blobnode.StorageAPI

	lastFlushTime time.Time
	spaceStatInfo atomic.Value
	metaLock      sync.RWMutex
	closeCh       chan interface{}
	DiskMgrConfig
}

type diskItem struct {
	diskID         proto.DiskID
	info           *blobnode.DiskInfo
	expireTime     time.Time
	lastExpireTime time.Time
	dropping       bool
	lock           sync.RWMutex
}

func (d *diskItem) isExpire() bool {
	if d.expireTime.IsZero() {
		return false
	}
	return time.Since(d.expireTime) > 0
}

func (d *diskItem) isAvailable() bool {
	if d.info.Readonly || d.info.Status != proto.DiskStatusNormal || d.dropping {
		return false
	}
	return true
}

// isWritable return false if disk heartbeat expire or disk status is not normal or disk is readonly or dropping
func (d *diskItem) isWritable() bool {
	if d.isExpire() || !d.isAvailable() {
		return false
	}
	return true
}

func (d *diskItem) needFilter() bool {
	return d.info.Status != proto.DiskStatusRepaired && d.info.Status != proto.DiskStatusDropped
}

func (d *diskItem) genFilterKey() string {
	return d.info.Host + d.info.Path
}

func New(scopeMgr scopemgr.ScopeMgrAPI, db *normaldb.NormalDB, cfg DiskMgrConfig) (*DiskMgr, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewDiskMgr")
	if len(cfg.CodeModes) == 0 {
		return nil, errors.New("code mode can not be nil")
	}
	if len(cfg.IDC) == 0 {
		return nil, errors.New("idc can not be nil")
	}

	diskTbl, err := normaldb.OpenDiskTable(db, cfg.EnsureIndex)
	if err != nil {
		return nil, errors.Info(err, "open disk table failed").Detail(err)
	}

	droppedDiskTbl, err := normaldb.OpenDroppedDiskTable(db)
	if err != nil {
		return nil, errors.Info(err, "open disk drop table failed").Detail(err)
	}

	if cfg.RefreshIntervalS <= 0 {
		cfg.RefreshIntervalS = defaultRefreshIntervalS
	}
	if cfg.HeartbeatExpireIntervalS <= 0 {
		cfg.HeartbeatExpireIntervalS = defaultHeartbeatExpireIntervalS
	}
	if cfg.FlushIntervalS <= 0 {
		cfg.FlushIntervalS = defaultFlushIntervalS
	}
	if cfg.ApplyConcurrency == 0 {
		cfg.ApplyConcurrency = defaultApplyConcurrency
	}
	if cfg.AllocTolerateBuffer >= 0 {
		defaultAllocTolerateBuff = cfg.AllocTolerateBuffer
	}

	allocators := make(map[string]*atomic.Value)
	for _, idc := range cfg.IDC {
		allocators[idc] = &atomic.Value{}
	}

	dm := &DiskMgr{
		allocators: allocators,
		taskPool:   base.NewTaskDistribution(int(cfg.ApplyConcurrency), 1),

		scopeMgr:       scopeMgr,
		diskTbl:        diskTbl,
		droppedDiskTbl: droppedDiskTbl,
		blobNodeClient: blobnode.New(&cfg.BlobNodeConfig),
		closeCh:        make(chan interface{}),
		DiskMgrConfig:  cfg,
	}

	// initial load data
	err = dm.LoadData(ctx)
	if err != nil {
		return nil, err
	}

	_, ctxNew := trace.StartSpanFromContext(context.Background(), "")
	dm.refresh(ctxNew)

	ticker := time.NewTicker(time.Duration(cfg.RefreshIntervalS) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				dm.refresh(ctxNew)
			case <-dm.closeCh:
				return
			}
		}
	}()

	return dm, nil
}

func (d *DiskMgr) Close() {
	close(d.closeCh)
	d.taskPool.Close()
}

func (d *DiskMgr) RefreshExpireTime() {
	d.metaLock.RLock()
	for _, di := range d.allDisks {
		di.lock.Lock()
		di.lastExpireTime = time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second)
		di.expireTime = time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second)
		di.lock.Unlock()
	}
	d.metaLock.RUnlock()
}

func (d *DiskMgr) AllocDiskID(ctx context.Context) (proto.DiskID, error) {
	_, diskID, err := d.scopeMgr.Alloc(ctx, DiskIDScopeName, 1)
	if err != nil {
		return 0, errors.Info(err, "diskMgr.AllocDiskID failed").Detail(err)
	}
	return proto.DiskID(diskID), nil
}

func (d *DiskMgr) GetDiskInfo(ctx context.Context, id proto.DiskID) (*blobnode.DiskInfo, error) {
	diskInfo, ok := d.getDisk(id)
	if !ok {
		return nil, apierrors.ErrCMDiskNotFound
	}

	diskInfo.lock.RLock()
	defer diskInfo.lock.RUnlock()
	newDiskInfo := *(diskInfo.info)
	// need to copy before return, or the higher level may change the disk info by the disk info pointer
	return &(newDiskInfo), nil
}

func (d *DiskMgr) CheckDiskInfoDuplicated(ctx context.Context, info *blobnode.DiskInfo) bool {
	disk := &diskItem{
		info: &blobnode.DiskInfo{Host: info.Host, Path: info.Path},
	}
	_, ok := d.hostPathFilter.Load(disk.genFilterKey())
	return ok
}

func (d *DiskMgr) IsDiskWritable(ctx context.Context, id proto.DiskID) (bool, error) {
	diskInfo, ok := d.getDisk(id)
	if !ok {
		return false, apierrors.ErrCMDiskNotFound
	}

	diskInfo.lock.RLock()
	defer diskInfo.lock.RUnlock()

	return diskInfo.isWritable(), nil
}

func (d *DiskMgr) SetStatus(ctx context.Context, id proto.DiskID, status proto.DiskStatus, isCommit bool) error {
	var (
		beforeSeq int
		afterSeq  int
		ok        bool
		span      = trace.SpanFromContextSafe(ctx)
	)

	if afterSeq, ok = validSetStatus[status]; !ok {
		return apierrors.ErrInvalidStatus
	}

	diskInfo, ok := d.getDisk(id)
	if !ok {
		span.Error("diskMgr.SetStatus disk not found in all disks, diskID: %v, status: %v", id, status)
		return apierrors.ErrCMDiskNotFound
	}

	diskInfo.lock.RLock()
	if diskInfo.info.Status == status {
		diskInfo.lock.RUnlock()
		return nil
	}
	beforeSeq, ok = validSetStatus[diskInfo.info.Status]
	diskInfo.lock.RUnlock()
	if !ok {
		panic(fmt.Sprintf("invalid disk status in disk table, diskid: %d, state: %d", id, status))
	}

	// can't change status back or change status more than 2 motion
	if beforeSeq > afterSeq || (afterSeq-beforeSeq > 1 && status != proto.DiskStatusDropped) {
		// return error in pre set request
		if !isCommit {
			return apierrors.ErrChangeDiskStatusNotAllow
		}
		// return nil in wal log replay situation
		return nil
	}

	if !isCommit {
		return nil
	}

	diskInfo.lock.Lock()
	defer diskInfo.lock.Unlock()
	// concurrent double check
	if diskInfo.info.Status == status {
		return nil
	}

	err := d.diskTbl.UpdateDiskStatus(id, status)
	if err != nil {
		err = errors.Info(err, "diskMgr.SetStatus update disk info failed").Detail(err)
		span.Error(errors.Detail(err))
		return err
	}
	diskInfo.info.Status = status
	if !diskInfo.needFilter() {
		d.hostPathFilter.Delete(diskInfo.genFilterKey())
	}

	return nil
}

func (d *DiskMgr) IsDroppingDisk(ctx context.Context, id proto.DiskID) (bool, error) {
	disk, ok := d.getDisk(id)
	if !ok {
		return false, apierrors.ErrCMDiskNotFound
	}
	disk.lock.RLock()
	defer disk.lock.RUnlock()
	if disk.dropping {
		return true, nil
	}
	return false, nil
}

func (d *DiskMgr) ListDroppingDisk(ctx context.Context) ([]*blobnode.DiskInfo, error) {
	diskIDs, err := d.droppedDiskTbl.GetAllDroppingDisk()
	if err != nil {
		return nil, errors.Info(err, "list dropping disk failed").Detail(err)
	}

	if len(diskIDs) == 0 {
		return nil, nil
	}
	ret := make([]*blobnode.DiskInfo, len(diskIDs))
	for i := range diskIDs {
		info, err := d.GetDiskInfo(ctx, diskIDs[i])
		if err != nil {
			return nil, err
		}
		ret[i] = info
	}
	return ret, nil
}

// AllocChunk return available chunks in data center
func (d *DiskMgr) AllocChunks(ctx context.Context, policy *AllocPolicy) (ret []proto.DiskID, err error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(ctx, "AllocChunks", trace.SpanFromContextSafe(ctx).TraceID()+"/"+policy.Idc)
	v := d.allocators[policy.Idc].Load()
	if v == nil {
		return nil, ErrNoEnoughSpace
	}
	allocator := v.(*idcStorage)

	var excludes map[proto.DiskID]*diskItem
	if len(policy.Excludes) > 0 {
		excludes = make(map[proto.DiskID]*diskItem)
		for _, diskID := range policy.Excludes {
			excludes[diskID], _ = d.getDisk(diskID)
		}
	}

	ret, err = allocator.alloc(ctx, len(policy.Vuids), excludes)
	if err != nil {
		return
	}

	// check if allocated result is host aware or disk aware
	if d.HostAware {
		selectedHost := make(map[string]bool)
		for i := range ret {
			disk, ok := d.getDisk(ret[i])
			if !ok {
				return nil, errors.Info(ErrDiskNotExist, fmt.Sprintf("disk[%d]", ret[i])).Detail(ErrDiskNotExist)
			}
			disk.lock.RLock()
			if selectedHost[disk.info.Host] {
				disk.lock.RUnlock()
				return nil, errors.New(fmt.Sprintf("duplicated host, selected disks: %v", ret))
			}
			selectedHost[disk.info.Host] = true
			disk.lock.RUnlock()
		}
	} else {
		selectedDisk := make(map[proto.DiskID]bool)
		for i := range ret {
			if selectedDisk[ret[i]] {
				return nil, errors.New(fmt.Sprintf("duplicated disk, selected disks: %v", ret))
			}
			selectedDisk[ret[i]] = true
		}
	}

	wg := sync.WaitGroup{}
	for idx := range ret {
		wg.Add(1)
		i := idx

		disk, _ := d.getDisk(ret[i])

		disk.lock.RLock()
		host := disk.info.Host
		disk.lock.RUnlock()
		go func() {
			defer wg.Done()
			blobNodeErr := d.blobNodeClient.CreateChunk(ctx, host,
				&blobnode.CreateChunkArgs{DiskID: ret[i], Vuid: policy.Vuids[i], ChunkSize: d.ChunkSize})
			// record error info and set ret[i] to InvalidDiskID
			if blobNodeErr != nil {
				span.Errorf("allocate chunk from blob node failed, diskID: %v, host: %s, err: %v", ret[i], host, blobNodeErr)
				ret[i] = proto.InvalidDiskID
				err = ErrBlobNodeCreateChunkFailed
			}
		}()
	}
	wg.Wait()

	return ret, err
}

// ListDiskInfo return disk info with specified query condition
func (d *DiskMgr) ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (*clustermgr.ListDiskRet, error) {
	if opt == nil {
		return nil, nil
	}
	span := trace.SpanFromContextSafe(ctx)

	if opt.Count > defaultListDiskMaxCount {
		opt.Count = defaultListDiskMaxCount
	}

	diskInfoDBs, err := d.diskTbl.ListDisk(opt)
	if err != nil {
		span.Error("diskMgr ListDiskInfo failed, err: %v", err)
		return nil, errors.Info(err, "diskMgr ListDiskInfo failed").Detail(err)
	}

	ret := &clustermgr.ListDiskRet{
		Disks: make([]*blobnode.DiskInfo, 0, len(diskInfoDBs)),
	}
	if len(diskInfoDBs) > 0 {
		ret.Marker = diskInfoDBs[len(diskInfoDBs)-1].DiskID
	}

	for i := range diskInfoDBs {
		diskInfo := diskInfoRecordToDiskInfo(diskInfoDBs[i])
		disk, _ := d.getDisk(diskInfo.DiskID)
		disk.lock.RLock()
		diskInfo.FreeChunkCnt = disk.info.FreeChunkCnt
		diskInfo.UsedChunkCnt = disk.info.UsedChunkCnt
		diskInfo.MaxChunkCnt = disk.info.MaxChunkCnt
		diskInfo.Free = disk.info.Free
		diskInfo.Used = disk.info.Used
		diskInfo.Size = disk.info.Size
		disk.lock.RUnlock()

		ret.Disks = append(ret.Disks, diskInfo)
	}
	if len(ret.Disks) == 0 {
		ret.Marker = proto.InvalidDiskID
	}

	return ret, nil
}

// Stat return disk statistic info of a cluster
func (d *DiskMgr) Stat(ctx context.Context) *clustermgr.SpaceStatInfo {
	spaceStatInfo := d.spaceStatInfo.Load().(*clustermgr.SpaceStatInfo)
	ret := *spaceStatInfo
	return &ret
}

// SwitchReadonly can switch disk's readonly or writable
func (d *DiskMgr) SwitchReadonly(diskID proto.DiskID, readonly bool) error {
	diskInfo, _ := d.getDisk(diskID)

	diskInfo.lock.RLock()
	if diskInfo.info.Readonly == readonly {
		diskInfo.lock.RUnlock()
		return nil
	}
	diskInfo.lock.RUnlock()

	diskInfo.lock.Lock()
	defer diskInfo.lock.Unlock()
	diskInfo.info.Readonly = readonly
	err := d.diskTbl.UpdateDisk(diskID, diskInfoToDiskInfoRecord(diskInfo.info))
	if err != nil {
		diskInfo.info.Readonly = !readonly
		return err
	}
	return nil
}

func (d *DiskMgr) GetHeartbeatChangeDisks() []HeartbeatEvent {
	all := d.getAllDisk()
	ret := make([]HeartbeatEvent, 0)
	for _, disk := range all {
		disk.lock.RLock()
		// notify topper level when heartbeat expire or heartbeat recover
		if disk.isExpire() {
			// expired disk has been notified already, then ignore it
			if time.Since(disk.expireTime) >= 2*time.Duration(d.HeartbeatExpireIntervalS)*time.Second {
				disk.lock.RUnlock()
				continue
			}
			ret = append(ret, HeartbeatEvent{DiskID: disk.diskID, IsAlive: false})
			disk.lock.RUnlock()
			continue
		}
		if disk.expireTime.Sub(disk.lastExpireTime) > 1*time.Duration(d.HeartbeatExpireIntervalS)*time.Second {
			ret = append(ret, HeartbeatEvent{DiskID: disk.diskID, IsAlive: true})
		}
		disk.lock.RUnlock()
	}

	return ret
}

// addDisk add a new disk into cluster, it return ErrDiskExist if disk already exist
func (d *DiskMgr) addDisk(ctx context.Context, info blobnode.DiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)

	_, ok := d.getDisk(info.DiskID)
	if ok {
		return ErrDiskExist
	}

	d.metaLock.Lock()
	defer d.metaLock.Unlock()
	// concurrent double check
	_, ok = d.allDisks[info.DiskID]
	if ok {
		return ErrDiskExist
	}

	// calculate free and max chunk count
	info.MaxChunkCnt = info.Size / d.ChunkSize
	info.FreeChunkCnt = info.MaxChunkCnt - info.UsedChunkCnt
	err := d.diskTbl.AddDisk(diskInfoToDiskInfoRecord(&info))
	if err != nil {
		span.Error("diskMgr.addDisk add disk failed: ", err)
		return errors.Info(err, "diskMgr.addDisk add disk failed").Detail(err)
	}
	diskItem := &diskItem{diskID: info.DiskID, info: &info, expireTime: time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second)}
	d.allDisks[info.DiskID] = diskItem
	d.hostPathFilter.Store(diskItem.genFilterKey(), 1)

	return nil
}

// droppingDisk add a dropping disk
func (d *DiskMgr) droppingDisk(ctx context.Context, id proto.DiskID) error {
	disk, ok := d.getDisk(id)
	if !ok {
		return apierrors.ErrCMDiskNotFound
	}

	disk.lock.RLock()
	if disk.dropping {
		disk.lock.RUnlock()
		return nil
	}
	disk.lock.RUnlock()

	disk.lock.Lock()
	defer disk.lock.Unlock()
	err := d.droppedDiskTbl.AddDroppingDisk(id)
	if err != nil {
		return err
	}
	disk.dropping = true

	return nil
}

// droppedDisk set disk dropped
func (d *DiskMgr) droppedDisk(ctx context.Context, id proto.DiskID) error {
	exist, err := d.droppedDiskTbl.IsDroppingDisk(id)
	if err != nil {
		return errors.Info(err, "diskMgr.droppedDisk get dropping disk failed").Detail(err)
	}
	// concurrent dropped request may cost dropping disk not found, don't return error in this situation
	if !exist {
		return nil
	}

	err = d.droppedDiskTbl.DroppedDisk(id)
	if err != nil {
		return errors.Info(err, "diskMgr.droppedDisk dropped disk failed").Detail(err)
	}

	err = d.SetStatus(ctx, id, proto.DiskStatusDropped, true)
	if err != nil {
		err = errors.Info(err, "diskMgr.droppedDisk set disk dropped status failed").Detail(err)
	}

	disk, _ := d.getDisk(id)
	disk.lock.Lock()
	disk.dropping = false
	disk.lock.Unlock()

	return err
}

// heartBeatDiskInfo process disk's heartbeat
func (d *DiskMgr) heartBeatDiskInfo(ctx context.Context, infos []*blobnode.DiskHeartBeatInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	expireTime := time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second)
	for i := range infos {
		info := infos[i]

		diskInfo, ok := d.getDisk(info.DiskID)
		// sometimes, we may not find disk from allDisks
		// it was happened when disk register and heartbeat request very close
		if !ok {
			span.Warnf("disk not found in all disk, diskID: %d", info.DiskID)
			continue
		}
		// memory modify disk heartbeat info, dump into db timely
		diskInfo.lock.Lock()
		diskInfo.info.Free = info.Free
		diskInfo.info.Size = info.Size
		diskInfo.info.Used = info.Used
		diskInfo.info.UsedChunkCnt = info.UsedChunkCnt
		// calculate free and max chunk count
		diskInfo.info.MaxChunkCnt = info.Size / d.ChunkSize
		// use the minimum value as free chunk count
		diskInfo.info.FreeChunkCnt = diskInfo.info.MaxChunkCnt - diskInfo.info.UsedChunkCnt
		freeChunkCnt := info.Free / d.ChunkSize
		if freeChunkCnt < diskInfo.info.FreeChunkCnt {
			span.Debugf("use minimum free chunk count, disk id[%d], free chunk[%d]", diskInfo.diskID, freeChunkCnt)
			diskInfo.info.FreeChunkCnt = freeChunkCnt
		}
		if diskInfo.info.FreeChunkCnt < 0 {
			diskInfo.info.FreeChunkCnt = 0
		}

		diskInfo.lastExpireTime = diskInfo.expireTime
		diskInfo.expireTime = expireTime
		diskInfo.lock.Unlock()
	}
	return nil
}

func (d *DiskMgr) getDisk(diskID proto.DiskID) (disk *diskItem, exist bool) {
	d.metaLock.RLock()
	disk, exist = d.allDisks[diskID]
	d.metaLock.RUnlock()
	return
}

// getAllDisk copy all diskItem pointer array
func (d *DiskMgr) getAllDisk() []*diskItem {
	d.metaLock.RLock()
	total := len(d.allDisks)
	all := make([]*diskItem, 0, total)
	for _, disk := range d.allDisks {
		all = append(all, disk)
	}
	d.metaLock.RUnlock()
	return all
}

func (d *DiskMgr) adminUpdateDisk(ctx context.Context, diskInfo *blobnode.DiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	disk, ok := d.allDisks[diskInfo.DiskID]
	if !ok {
		span.Errorf("admin update disk, diskId:%d not exist", diskInfo.DiskID)
		return ErrDiskNotExist
	}
	disk.lock.Lock()
	if diskInfo.Status.IsValid() {
		disk.info.Status = diskInfo.Status
	}
	if diskInfo.MaxChunkCnt > 0 {
		disk.info.MaxChunkCnt = diskInfo.MaxChunkCnt
	}
	if diskInfo.FreeChunkCnt > 0 {
		disk.info.FreeChunkCnt = diskInfo.FreeChunkCnt
	}
	diskRecord := diskInfoToDiskInfoRecord(disk.info)
	err := d.diskTbl.UpdateDisk(diskInfo.DiskID, diskRecord)
	disk.lock.Unlock()
	return err
}

func diskInfoToDiskInfoRecord(info *blobnode.DiskInfo) *normaldb.DiskInfoRecord {
	return &normaldb.DiskInfoRecord{
		Version:      normaldb.DiskInfoVersionNormal,
		DiskID:       info.DiskID,
		ClusterID:    info.ClusterID,
		Idc:          info.Idc,
		Rack:         info.Rack,
		Host:         info.Host,
		Path:         info.Path,
		Status:       info.Status,
		Readonly:     info.Readonly,
		UsedChunkCnt: info.UsedChunkCnt,
		CreateAt:     info.CreateAt,
		LastUpdateAt: info.LastUpdateAt,
		Used:         info.Used,
		Size:         info.Size,
		Free:         info.Free,
		MaxChunkCnt:  info.MaxChunkCnt,
		FreeChunkCnt: info.FreeChunkCnt,
	}
}

func diskInfoRecordToDiskInfo(infoDB *normaldb.DiskInfoRecord) *blobnode.DiskInfo {
	return &blobnode.DiskInfo{
		ClusterID:    infoDB.ClusterID,
		Idc:          infoDB.Idc,
		Rack:         infoDB.Rack,
		Host:         infoDB.Host,
		Path:         infoDB.Path,
		Status:       infoDB.Status,
		Readonly:     infoDB.Readonly,
		CreateAt:     infoDB.CreateAt,
		LastUpdateAt: infoDB.LastUpdateAt,
		DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
			DiskID:       infoDB.DiskID,
			Used:         infoDB.Used,
			Size:         infoDB.Size,
			Free:         infoDB.Free,
			MaxChunkCnt:  infoDB.MaxChunkCnt,
			UsedChunkCnt: infoDB.UsedChunkCnt,
			FreeChunkCnt: infoDB.FreeChunkCnt,
		},
	}
}
