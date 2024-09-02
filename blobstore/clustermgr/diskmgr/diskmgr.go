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
	"encoding/json"
	"fmt"
	"math/rand"
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
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	DiskIDScopeName                 = "diskid"
	NodeIDScopeName                 = "nodeid"
	defaultRefreshIntervalS         = 300
	defaultHeartbeatExpireIntervalS = 60
	defaultFlushIntervalS           = 600
	defaultApplyConcurrency         = 10
	defaultListDiskMaxCount         = 200
)

// CopySet Config
const (
	NullNodeSetID = proto.NodeSetID(0)

	ecNodeSetID   = proto.NodeSetID(1)
	ecDiskSetID   = proto.DiskSetID(1)
	nullDiskSetID = proto.DiskSetID(0)
)

var (
	ErrDiskExist                 = errors.New("disk already exist")
	ErrDiskNotExist              = errors.New("disk not exist")
	ErrNoEnoughSpace             = errors.New("no enough space to alloc")
	ErrBlobNodeCreateChunkFailed = errors.New("blob node create chunk failed")
	ErrNodeExist                 = errors.New("node already exist")
	ErrNodeNotExist              = errors.New("node not exist")
	ErrInValidAllocPolicy        = errors.New("alloc policy is invalid")
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
	CheckDiskInfoDuplicated(ctx context.Context, info *blobnode.DiskInfo, nodeInfo *blobnode.NodeInfo) error
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
	AllocChunks(ctx context.Context, policy AllocPolicy) ([]proto.DiskID, []proto.Vuid, error)
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
	DiskType proto.DiskType
	CodeMode codemode.CodeMode
	Vuids    []proto.Vuid

	Excludes   []proto.DiskID
	DiskSetID  proto.DiskSetID
	Idc        string
	RetryTimes int
}

type HeartbeatEvent struct {
	DiskID  proto.DiskID
	IsAlive bool
}

type DiskMgrConfig struct {
	RefreshIntervalS         int                 `json:"refresh_interval_s"`
	RackAware                bool                `json:"rack_aware"`
	HostAware                bool                `json:"host_aware"`
	HeartbeatExpireIntervalS int                 `json:"heartbeat_expire_interval_s"`
	FlushIntervalS           int                 `json:"flush_interval_s"`
	ApplyConcurrency         uint32              `json:"apply_concurrency"`
	BlobNodeConfig           blobnode.Config     `json:"blob_node_config"`
	AllocTolerateBuffer      int64               `json:"alloc_tolerate_buffer"`
	EnsureIndex              bool                `json:"ensure_index"`
	IDC                      []string            `json:"-"`
	CodeModes                []codemode.CodeMode `json:"-"`
	ChunkSize                int64               `json:"-"`

	CopySetConfigs map[proto.NodeRole]map[proto.DiskType]CopySetConfig `json:"copy_set_configs"`
}

type CopySetConfig struct {
	NodeSetCap                int `json:"node_set_cap"`
	NodeSetRackCap            int `json:"node_set_rack_cap"`
	DiskSetCap                int `json:"disk_set_cap"`
	DiskCountPerNodeInDiskSet int `json:"disk_count_per_node_in_disk_set"`

	NodeSetIdcCap int `json:"-"`
}

type DiskMgr struct {
	module         string
	allDisks       map[proto.DiskID]*diskItem
	allNodes       map[proto.NodeID]*nodeItem
	topoMgrs       map[proto.NodeRole]*topoMgr
	allocators     map[proto.NodeRole]*atomic.Value
	taskPool       *base.TaskDistribution
	hostPathFilter sync.Map
	pendingEntries sync.Map
	raftServer     raftserver.RaftServer

	scopeMgr       scopemgr.ScopeMgrAPI
	diskTbl        *normaldb.DiskTable
	nodeTbl        *normaldb.NodeTable
	droppedDiskTbl *normaldb.DroppedDiskTable
	blobNodeClient blobnode.StorageAPI

	lastFlushTime time.Time
	spaceStatInfo atomic.Value
	metaLock      sync.RWMutex
	closeCh       chan interface{}
	DiskMgrConfig
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

	nodeTbl, err := normaldb.OpenNodeTable(db)
	if err != nil {
		return nil, errors.Info(err, "open node table failed").Detail(err)
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

	dm := &DiskMgr{
		allocators: map[proto.NodeRole]*atomic.Value{},
		topoMgrs:   map[proto.NodeRole]*topoMgr{proto.NodeRoleBlobNode: newTopoMgr()},
		taskPool:   base.NewTaskDistribution(int(cfg.ApplyConcurrency), 1),

		scopeMgr:       scopeMgr,
		diskTbl:        diskTbl,
		nodeTbl:        nodeTbl,
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

func (c *DiskMgr) SetRaftServer(raftServer raftserver.RaftServer) {
	c.raftServer = raftServer
}

func (d *DiskMgr) AllocDiskID(ctx context.Context) (proto.DiskID, error) {
	_, diskID, err := d.scopeMgr.Alloc(ctx, DiskIDScopeName, 1)
	if err != nil {
		return 0, errors.Info(err, "diskMgr.AllocDiskID failed").Detail(err)
	}
	return proto.DiskID(diskID), nil
}

func (d *DiskMgr) GetDiskInfo(ctx context.Context, id proto.DiskID) (*blobnode.DiskInfo, error) {
	disk, ok := d.getDisk(id)
	if !ok {
		return nil, apierrors.ErrCMDiskNotFound
	}

	var diskInfo blobnode.DiskInfo
	disk.withRLocked(func() error {
		diskInfo = *(disk.info)
		return nil
	})

	if nodeInfo, ok := d.getNode(diskInfo.NodeID); ok {
		diskInfo.Idc = nodeInfo.info.Idc
		diskInfo.Rack = nodeInfo.info.Rack
		diskInfo.Host = nodeInfo.info.Host
	}
	// need to copy before return, or the higher level may change the disk info by the disk info pointer
	return &(diskInfo), nil
}

// judge disk heartbeat interval whether small than HeartbeatNotifyIntervalS
func (d *DiskMgr) IsFrequentHeatBeat(id proto.DiskID, HeartbeatNotifyIntervalS int) (bool, error) {
	diskInfo, ok := d.getDisk(id)
	if !ok {
		return false, apierrors.ErrCMDiskNotFound
	}
	diskInfo.lock.RLock()
	defer diskInfo.lock.RUnlock()

	newExpireTime := time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second)
	if newExpireTime.Sub(diskInfo.expireTime) < time.Duration(HeartbeatNotifyIntervalS)*time.Second {
		return true, nil
	}
	return false, nil
}

func (d *DiskMgr) CheckDiskInfoDuplicated(ctx context.Context, diskInfo *blobnode.DiskInfo, nodeInfo *blobnode.NodeInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	di, ok := d.getDisk(diskInfo.DiskID)
	// compatible case: disk register again to diskSet
	if ok && di.info.NodeID == proto.InvalidNodeID && diskInfo.NodeID != proto.InvalidNodeID &&
		di.info.Host == nodeInfo.Host && di.info.Idc == nodeInfo.Idc && di.info.Rack == nodeInfo.Rack {
		return nil
	}
	if ok { // disk exist
		span.Warn("disk exist")
		return apierrors.ErrExist
	}
	disk := &diskItem{
		info: &blobnode.DiskInfo{Host: nodeInfo.Host, Path: diskInfo.Path},
	}
	if _, ok = d.hostPathFilter.Load(disk.genFilterKey()); ok {
		span.Warn("host and path duplicated")
		return apierrors.ErrIllegalArguments
	}
	return nil
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

	disk, ok := d.getDisk(id)
	if !ok {
		span.Error("diskMgr.SetStatus disk not found in all disks, diskID: %v, status: %v", id, status)
		return apierrors.ErrCMDiskNotFound
	}

	err := disk.withRLocked(func() error {
		if disk.info.Status == status {
			return nil
		}
		// disallow set disk status when disk is dropping, as disk status will be dropped finally
		if disk.dropping && status != proto.DiskStatusDropped {
			if !isCommit {
				return apierrors.ErrChangeDiskStatusNotAllow
			}
			span.Warnf("disk[%d] is dropping, can't set disk status", id)
			return nil
		}

		beforeSeq, ok = validSetStatus[disk.info.Status]
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
			span.Warnf("disallow set disk[%d] status[%d], before seq: %d, after seq: %d", id, status, beforeSeq, afterSeq)
			return nil
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !isCommit {
		return nil
	}

	return disk.withLocked(func() error {
		// concurrent double check
		if disk.info.Status == status {
			return nil
		}

		err := d.diskTbl.UpdateDiskStatus(id, status)
		if err != nil {
			err = errors.Info(err, "diskMgr.SetStatus update disk info failed").Detail(err)
			span.Error(errors.Detail(err))
			return err
		}
		disk.info.Status = status
		if !disk.needFilter() {
			d.hostPathFilter.Delete(disk.genFilterKey())
		}
		if node, ok := d.getNode(disk.info.NodeID); ok && !disk.needFilter() { // compatible case && diskRepaired
			d.topoMgrs[node.info.Role].RemoveDiskFromDiskSet(node.info.DiskType, node.info.NodeSetID, disk)
		}

		return nil
	})
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
func (d *DiskMgr) AllocChunks(ctx context.Context, policy AllocPolicy) ([]proto.DiskID, []proto.Vuid, error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(ctx, "AllocChunks", trace.SpanFromContextSafe(ctx).TraceID())

	var (
		err       error
		allocator = d.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		// to make sure return disks order match with policy.Vuids
		idcVuidIndexMap = make(map[string]map[proto.Vuid]int)
		idcVuidMap      = make(map[string][]proto.Vuid)
		idcDiskMap      = make(map[string][]proto.DiskID)
		ret             = make([]proto.DiskID, len(policy.Vuids))
		retVuids        = make([]proto.Vuid, len(policy.Vuids))
		retryTimes      = policy.RetryTimes
		allocLock       sync.Mutex
	)

	// repair
	if len(policy.Excludes) > 0 {
		ret, err := allocator.ReAlloc(ctx, reAllocPolicy{
			diskType:  policy.DiskType,
			diskSetID: policy.DiskSetID,
			idc:       policy.Idc,
			count:     len(policy.Vuids),
			excludes:  policy.Excludes,
		})
		if err != nil {
			return nil, nil, err
		}
		idcVuidMap[policy.Idc] = policy.Vuids
		idcDiskMap[policy.Idc] = ret
		idcVuidIndexMap[policy.Idc] = make(map[proto.Vuid]int, len(ret))
		for idx, vuid := range policy.Vuids {
			idcVuidIndexMap[policy.Idc][vuid] = idx
		}
	} else {
		tactic := policy.CodeMode.Tactic()
		idcIndexes := tactic.GetECLayoutByAZ()
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idcIndexes), func(i, j int) {
			idcIndexes[i], idcIndexes[j] = idcIndexes[j], idcIndexes[i]
		})
		span.Debugf("idcIndexes is %#v", idcIndexes)

		ret, err := allocator.Alloc(ctx, policy.DiskType, policy.CodeMode)
		if err != nil {
			span.Errorf("create volume alloc first time failed, err: %s", err.Error())
			return nil, nil, err
		}

		for idcIdx, r := range ret {
			idc := r.Idc
			idcDiskMap[idc] = r.Disks
			idcVuidIndexMap[idc] = make(map[proto.Vuid]int)
			for _, vuidIdx := range idcIndexes[idcIdx] {
				vuid := policy.Vuids[vuidIdx]
				idcVuidMap[idc] = append(idcVuidMap[idc], vuid)
				idcVuidIndexMap[idc][vuid] = vuidIdx
				retVuids[vuidIdx] = vuid
			}
		}
	}

	// retry
	for idc, idcDisks := range idcDiskMap {
		vuids := idcVuidMap[idc]
		disks := idcDisks

		diskInfo, _ := d.getDisk(disks[0])
		diskSetID := nullDiskSetID
		if policy.CodeMode.T().IsReplicateMode() {
			diskSetID = diskInfo.info.DiskSetID
		}
		excludes := make([]proto.DiskID, 0)

	RETRY:
		if len(excludes) > 0 {
			disks, err = allocator.ReAlloc(ctx, reAllocPolicy{
				diskType:  policy.DiskType,
				diskSetID: diskSetID,
				idc:       idc,
				count:     len(vuids),
				excludes:  excludes,
			})
			if err != nil {
				return nil, nil, err
			}
		}

		if err := d.validateAllocRet(disks); err != nil {
			return nil, nil, err
		}

		failVuids := make([]proto.Vuid, 0)
		wg := sync.WaitGroup{}
		wg.Add(len(disks))

		for ii := range disks {
			idx := ii

			disk, _ := d.getDisk(disks[idx])
			disk.lock.RLock()
			host := disk.info.Host
			disk.lock.RUnlock()

			go func() {
				defer wg.Done()

				blobNodeErr := d.blobNodeClient.CreateChunk(ctx, host,
					&blobnode.CreateChunkArgs{DiskID: disks[idx], Vuid: vuids[idx], ChunkSize: d.ChunkSize})
				if blobNodeErr != nil {
					vuidPrefix := vuids[idx].VuidPrefix()
					newVuid := proto.EncodeVuid(vuidPrefix, vuids[idx].Epoch()+1)

					allocLock.Lock()
					index := idcVuidIndexMap[idc][vuids[idx]]
					idcVuidIndexMap[idc][newVuid] = index
					failVuids = append(failVuids, newVuid)
					retVuids[index] = newVuid
					allocLock.Unlock()

					span.Errorf("allocate chunk from blob node failed, diskID: %d, host: %s, err: %s", disks[idx], host, blobNodeErr)
					return
				}
				allocLock.Lock()
				excludes = append(excludes, disks[idx])
				ret[idcVuidIndexMap[idc][vuids[idx]]] = disks[idx]
				allocLock.Unlock()
			}()
		}
		wg.Wait()
		vuids = failVuids

		if len(failVuids) > 0 {
			if retryTimes == 0 {
				return nil, nil, ErrBlobNodeCreateChunkFailed
			}
			retryTimes -= 1
			goto RETRY
		}
	}

	err = d.validateAllocRet(ret)
	return ret, retVuids, err
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
	spaceStatInfo := d.spaceStatInfo.Load().(map[proto.NodeRole]map[proto.DiskType]*clustermgr.SpaceStatInfo)
	nodeSpaceInfo := spaceStatInfo[proto.NodeRoleBlobNode]
	diskTypeInfo, ok := nodeSpaceInfo[proto.DiskTypeHDD]
	if !ok {
		return &clustermgr.SpaceStatInfo{}
	}
	ret := *diskTypeInfo
	return &ret
}

// SwitchReadonly can switch disk's readonly or writable
func (d *DiskMgr) SwitchReadonly(diskID proto.DiskID, readonly bool) error {
	disk, _ := d.getDisk(diskID)

	disk.lock.RLock()
	if disk.info.Readonly == readonly {
		disk.lock.RUnlock()
		return nil
	}
	disk.lock.RUnlock()

	disk.lock.Lock()
	defer disk.lock.Unlock()
	disk.info.Readonly = readonly
	err := d.diskTbl.UpdateDisk(diskID, diskInfoToDiskInfoRecord(disk.info))
	if err != nil {
		disk.info.Readonly = !readonly
		return err
	}
	return nil
}

func (d *DiskMgr) GetHeartbeatChangeDisks() []HeartbeatEvent {
	all := d.getAllDisk()
	ret := make([]HeartbeatEvent, 0)
	span := trace.SpanFromContextSafe(context.Background())
	for _, disk := range all {
		disk.lock.RLock()
		span.Debugf("diskId:%d,expireTime:%v,lastExpireTime:%v", disk.diskID, disk.expireTime, disk.lastExpireTime)
		// notify topper level when heartbeat expire or heartbeat recover
		if disk.isExpire() {
			span.Warnf("diskId:%d was expired,expireTime:%v,lastExpireTime:%v", disk.diskID, disk.expireTime, disk.lastExpireTime)

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

func (d *DiskMgr) AllocNodeID(ctx context.Context) (proto.NodeID, error) {
	_, nodeID, err := d.scopeMgr.Alloc(ctx, NodeIDScopeName, 1)
	if err != nil {
		return 0, errors.Info(err, "diskMgr.AllocNodeID failed").Detail(err)
	}
	return proto.NodeID(nodeID), nil
}

func (d *DiskMgr) GetNodeInfo(ctx context.Context, nodeID proto.NodeID) (*blobnode.NodeInfo, error) {
	nodeInfo, ok := d.getNode(nodeID)
	if !ok {
		return nil, apierrors.ErrCMNodeNotFound
	}

	nodeInfo.lock.RLock()
	defer nodeInfo.lock.RUnlock()
	newNodeInfo := *(nodeInfo.info)
	// need to copy before return, or the higher level may change the node info by pointer
	return &(newNodeInfo), nil
}

func (d *DiskMgr) GetTopoInfo(ctx context.Context) *clustermgr.TopoInfo {
	ret := &clustermgr.TopoInfo{
		CurNodeSetIDs: make(map[string]proto.NodeSetID),
		CurDiskSetIDs: make(map[string]proto.DiskSetID),
		AllNodeSets:   make(map[string]map[string]map[proto.NodeSetID]*clustermgr.NodeSetInfo),
	}
	for role, topoMgr := range d.topoMgrs {
		ret.CurNodeSetIDs[role.String()] = topoMgr.GetNodeSetID()
		ret.CurDiskSetIDs[role.String()] = topoMgr.GetDiskSetID()
		if _, ok := ret.AllNodeSets[role.String()]; !ok {
			ret.AllNodeSets[role.String()] = make(map[string]map[proto.NodeSetID]*clustermgr.NodeSetInfo)
		}
		nodeSetsMap := topoMgr.GetAllNodeSets(ctx)
		for diskType, nodeSets := range nodeSetsMap {
			if _, ok := ret.AllNodeSets[role.String()][diskType.String()]; !ok {
				ret.AllNodeSets[role.String()][diskType.String()] = make(map[proto.NodeSetID]*clustermgr.NodeSetInfo)
			}
			for _, nodeSet := range nodeSets {
				nodeSetInfo, ok := ret.AllNodeSets[role.String()][diskType.String()][nodeSet.ID()]
				if !ok {
					nodeSetInfo = &clustermgr.NodeSetInfo{
						ID:       nodeSet.ID(),
						Number:   nodeSet.GetNodeNum(),
						Nodes:    nodeSet.GetNodeIDs(),
						DiskSets: make(map[proto.DiskSetID][]proto.DiskID),
					}
					ret.AllNodeSets[role.String()][diskType.String()][nodeSet.ID()] = nodeSetInfo
				}
				diskSets := nodeSet.GetDiskSets()
				for _, diskSet := range diskSets {
					nodeSetInfo.DiskSets[diskSet.ID()] = diskSet.GetDiskIDs()
				}
			}
		}
	}
	return ret
}

func (d *DiskMgr) IsDroppedNode(ctx context.Context, nodeID proto.NodeID) (bool, error) {
	node, ok := d.getNode(nodeID)
	if !ok {
		return false, apierrors.ErrCMNodeNotFound
	}

	node.lock.RLock()
	defer node.lock.RUnlock()

	for _, disk := range node.disks {
		di := &diskItem{
			diskID: disk.DiskID,
			info:   disk,
		}
		if di.needFilter() {
			return false, apierrors.ErrCMHasDiskNotDroppedOrRepaired
		}
	}
	if node.isUsingStatus() {
		return false, nil
	}
	return true, nil
}

func (d *DiskMgr) CheckNodeInfoDuplicated(ctx context.Context, info *blobnode.NodeInfo) (proto.NodeID, bool) {
	node := &nodeItem{
		info: &blobnode.NodeInfo{Host: info.Host, DiskType: info.DiskType},
	}
	if v, ok := d.hostPathFilter.Load(node.genFilterKey()); ok {
		nodeID := v.(proto.NodeID)
		return nodeID, true
	}
	return proto.InvalidNodeID, false
}

func (d *DiskMgr) ValidateNodeSetID(ctx context.Context, info *blobnode.NodeInfo) error {
	if err := d.topoMgrs[info.Role].ValidateNodeSetID(ctx, info.DiskType, info.NodeSetID); err != nil {
		return err
	}
	return nil
}

func (d *DiskMgr) ValidateNodeInfo(ctx context.Context, info *blobnode.NodeInfo) error {
	if _, ok := d.topoMgrs[info.Role]; !ok {
		return apierrors.ErrIllegalArguments
	}
	if !info.DiskType.IsValid() {
		return apierrors.ErrIllegalArguments
	}
	return nil
}

func (d *DiskMgr) AddDisk(ctx context.Context, args *blobnode.DiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, disk info: %v, error: %v", args, err)
		return errors.Info(apierrors.ErrUnexpected).Detail(err)
	}
	pendingKey := fmtApplyContextKey("disk-add", args.DiskID.ToString())
	d.pendingEntries.Store(pendingKey, nil)
	defer d.pendingEntries.Delete(pendingKey)
	proposeInfo := base.EncodeProposeInfo(d.GetModuleName(), OperTypeAddDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = d.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		return apierrors.ErrRaftPropose
	}
	if v, _ := d.pendingEntries.Load(pendingKey); v != nil {
		return v.(error)
	}
	return nil
}

// addDisk add a new disk into cluster, it return ErrDiskExist if disk already exist
func (d *DiskMgr) addDisk(ctx context.Context, info *blobnode.DiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)

	d.metaLock.Lock()
	defer d.metaLock.Unlock()
	// compatible case: disk register again to diskSet
	di, ok := d.allDisks[info.DiskID]
	if ok && (di.info.NodeID != proto.InvalidNodeID || info.NodeID == proto.InvalidNodeID) {
		return nil
	}
	// alloc diskSetID and compatible case: update follower first
	if node, ok := d.allNodes[info.NodeID]; ok {
		if node.info.Status == proto.NodeStatusDropped {
			span.Warnf("node is dropped, disk info: %v", info)
			pendingKey := fmtApplyContextKey("disk-add", info.DiskID.ToString())
			if _, ok := d.pendingEntries.Load(pendingKey); ok {
				d.pendingEntries.Store(pendingKey, apierrors.ErrCMNodeNotFound)
			}
			return nil
		}
		info.DiskSetID = d.topoMgrs[node.info.Role].AllocDiskSetID(ctx, info, node.info, d.CopySetConfigs[node.info.Role][node.info.DiskType])
	}

	// calculate free and max chunk count
	info.MaxChunkCnt = info.Size / d.ChunkSize
	info.FreeChunkCnt = info.MaxChunkCnt - info.UsedChunkCnt
	err := d.diskTbl.AddDisk(diskInfoToDiskInfoRecord(info))
	if err != nil {
		span.Error("diskMgr.addDisk add disk failed: ", err)
		return errors.Info(err, "diskMgr.addDisk add disk failed").Detail(err)
	}

	disk := &diskItem{diskID: info.DiskID, info: info, expireTime: time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second)}
	if node, ok := d.allNodes[info.NodeID]; ok { // compatible case
		node.disks[info.DiskID] = info
		d.topoMgrs[node.info.Role].AddDiskToDiskSet(node.info.DiskType, node.info.NodeSetID, disk)
	}
	d.allDisks[info.DiskID] = disk
	d.hostPathFilter.Store(disk.genFilterKey(), 1)

	return nil
}

// droppingDisk add a dropping disk
func (d *DiskMgr) droppingDisk(ctx context.Context, id proto.DiskID) error {
	span := trace.SpanFromContext(ctx)

	disk, ok := d.getDisk(id)
	if !ok {
		return apierrors.ErrCMDiskNotFound
	}

	disk.lock.Lock()
	defer disk.lock.Unlock()

	if disk.dropping {
		return nil
	}
	if disk.info.Status != proto.DiskStatusNormal {
		span.Warnf("disk[%d] status is not normal, can't add into dropping disk list", id)
		return nil
	}

	err := d.droppedDiskTbl.AddDroppingDisk(id)
	if err != nil {
		return err
	}
	disk.dropping = true
	// remove disk from diskSet on dropping disk, avoid the new expanded disk not being properly added to the diskSet when dropping node
	if node, ok := d.getNode(disk.info.NodeID); ok { // compatible case
		d.topoMgrs[node.info.Role].RemoveDiskFromDiskSet(node.info.DiskType, node.info.NodeSetID, disk)
	}

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
		DiskSetID:    info.DiskSetID,
		NodeID:       info.NodeID,
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
		DiskSetID: infoDB.DiskSetID,
		NodeID:    infoDB.NodeID,
	}
}

// addNode add a new node into cluster, it returns ErrNodeExist if node already exist
func (d *DiskMgr) addNode(ctx context.Context, info *blobnode.NodeInfo) error {
	span := trace.SpanFromContextSafe(ctx)

	d.metaLock.Lock()
	defer d.metaLock.Unlock()

	// concurrent double check
	_, ok := d.allNodes[info.NodeID]
	if ok {
		return nil
	}

	// alloc NodeSetID
	if info.NodeSetID == NullNodeSetID {
		info.NodeSetID = d.topoMgrs[info.Role].AllocNodeSetID(ctx, info, d.CopySetConfigs[info.Role][info.DiskType], d.RackAware)
	}
	info.Status = proto.NodeStatusNormal

	// add node to nodeTbl and nodeSet
	err := d.nodeTbl.UpdateNode(nodeInfoToNodeInfoRecord(info))
	if err != nil {
		span.Error("diskMgr.addNode add node failed: ", err)
		return errors.Info(err, "diskMgr.addNode add node failed").Detail(err)
	}

	ni := &nodeItem{nodeID: info.NodeID, info: info, disks: make(map[proto.DiskID]*blobnode.DiskInfo)}
	d.topoMgrs[info.Role].AddNodeToNodeSet(ni)
	d.allNodes[info.NodeID] = ni
	d.hostPathFilter.Store(ni.genFilterKey(), ni.nodeID)

	return nil
}

// dropNode set nodeStatus Dropped and remove node from nodeSet
func (d *DiskMgr) dropNode(ctx context.Context, arg *clustermgr.NodeInfoArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	// all nodes will be stored into allNodes
	node, ok := d.getNode(arg.NodeID)
	if !ok {
		return ErrNodeNotExist
	}

	node.lock.Lock()
	defer node.lock.Unlock()

	if !node.isUsingStatus() {
		return nil
	}

	node.info.Status = proto.NodeStatusDropped
	err := d.nodeTbl.UpdateNode(nodeInfoToNodeInfoRecord(node.info))
	if err != nil {
		span.Error("nodeTbl UpdateNode failed: ", err)
		// revert memory nodeStatus when persist failed
		node.info.Status = proto.NodeStatusNormal
		return errors.Info(err, "nodeTbl UpdateNode failed").Detail(err)
	}
	d.topoMgrs[node.info.Role].RemoveNodeFromNodeSet(node)

	return nil
}

func (d *DiskMgr) getNode(nodeID proto.NodeID) (node *nodeItem, exist bool) {
	d.metaLock.RLock()
	node, exist = d.allNodes[nodeID]
	d.metaLock.RUnlock()
	return
}

func (d *DiskMgr) getDiskType(disk *diskItem) proto.DiskType {
	n, _ := d.getNode(disk.info.NodeID)
	if n == nil {
		// compatible
		return proto.DiskTypeHDD
	}
	return n.info.DiskType
}

func (d *DiskMgr) validateAllocRet(disks []proto.DiskID) error {
	if d.HostAware {
		selectedHost := make(map[string]bool)
		for i := range disks {
			disk, ok := d.getDisk(disks[i])
			if !ok {
				return errors.Info(ErrDiskNotExist, fmt.Sprintf("disk[%d]", disks[i])).Detail(ErrDiskNotExist)
			}
			disk.lock.RLock()
			if selectedHost[disk.info.Host] {
				disk.lock.RUnlock()
				return errors.New(fmt.Sprintf("duplicated host, selected disks: %v", disks))
			}
			selectedHost[disk.info.Host] = true
			disk.lock.RUnlock()
		}
		return nil
	}

	selectedDisk := make(map[proto.DiskID]bool)
	for i := range disks {
		if selectedDisk[disks[i]] {
			return errors.New(fmt.Sprintf("duplicated disk, selected disks: %v", disks))
		}
		selectedDisk[disks[i]] = true
	}

	return nil
}

func fmtApplyContextKey(opType, id string) string {
	return fmt.Sprintf("%s-%s", opType, id)
}

func nodeInfoRecordToNodeInfo(infoDB *normaldb.NodeInfoRecord) *blobnode.NodeInfo {
	return &blobnode.NodeInfo{
		NodeID:    infoDB.NodeID,
		NodeSetID: infoDB.NodeSetID,
		ClusterID: infoDB.ClusterID,
		DiskType:  infoDB.DiskType,
		Idc:       infoDB.Idc,
		Rack:      infoDB.Rack,
		Host:      infoDB.Host,
		Role:      infoDB.Role,
		Status:    infoDB.Status,
	}
}

func nodeInfoToNodeInfoRecord(info *blobnode.NodeInfo) *normaldb.NodeInfoRecord {
	return &normaldb.NodeInfoRecord{
		Version:   normaldb.NodeInfoVersionNormal,
		NodeID:    info.NodeID,
		NodeSetID: info.NodeSetID,
		ClusterID: info.ClusterID,
		DiskType:  info.DiskType,
		Idc:       info.Idc,
		Rack:      info.Rack,
		Host:      info.Host,
		Role:      info.Role,
		Status:    info.Status,
	}
}
