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

package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	_ BlobNodeManagerAPI  = (*BlobNodeManager)(nil)
	_ ShardNodeManagerAPI = (*ShardNodeManager)(nil)
)

const (
	defaultRefreshIntervalS         = 300
	defaultHeartbeatExpireIntervalS = 60
	defaultFlushIntervalS           = 600
	defaultApplyConcurrency         = 10
	defaultListDiskMaxCount         = 200
)

// CopySet Config
const (
	ecNodeSetID   = proto.NodeSetID(1)
	ecDiskSetID   = proto.DiskSetID(1)
	nullNodeSetID = proto.NodeSetID(0)
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

type NodeManagerAPI interface {
	// AllocNodeID return a unused node id
	AllocNodeID(ctx context.Context) (proto.NodeID, error)
	// AllocDiskID return a unused disk id
	AllocDiskID(ctx context.Context) (proto.DiskID, error)
	// CheckDiskInfoDuplicated return true if disk info already exit, like host and path duplicated
	CheckDiskInfoDuplicated(ctx context.Context, diskID proto.DiskID, info *clustermgr.DiskInfo, nodeInfo *clustermgr.NodeInfo) error
	// IsDiskWritable judge disk if writable, disk status unmoral or readonly or heartbeat timeout will return true
	IsDiskWritable(ctx context.Context, id proto.DiskID) (bool, error)
	// SetStatus change disk status, in some case, change status is not allow
	// like change repairing/repaired/dropped into normal
	SetStatus(ctx context.Context, id proto.DiskID, status proto.DiskStatus, isCommit bool) error
	// IsDroppingDisk return true if the specified disk is dropping
	IsDroppingDisk(ctx context.Context, id proto.DiskID) (bool, error)
	// Stat return disk statistic info of a cluster
	Stat(ctx context.Context) *clustermgr.SpaceStatInfo
	// GetHeartbeatChangeDisks return any heartbeat change disks
	GetHeartbeatChangeDisks() []HeartbeatEvent
	// ValidateNodeInfo validate node info and return any validation error when validate fail
	ValidateNodeInfo(ctx context.Context, info *clustermgr.NodeInfo) error
	CheckNodeInfoDuplicated(ctx context.Context, info *clustermgr.NodeInfo) (proto.NodeID, bool)
	IsDroppedNode(ctx context.Context, nodeID proto.NodeID) (bool, error)
	RefreshExpireTime()
}

type persistentHandler interface {
	updateDiskNoLocked(di *diskItem) error
	updateDiskStatusNoLocked(id proto.DiskID, status proto.DiskStatus) error
	addDiskNoLocked(di *diskItem) error
	updateNodeNoLocked(n *nodeItem) error
	addDroppingDisk(id proto.DiskID) error
	isDroppingDisk(id proto.DiskID) (bool, error)
	droppedDisk(id proto.DiskID) error
}

type Module struct {
	blobNodeMgr  *BlobNodeManager
	shardNodeMgr *ShardNodeManager
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
	DiskIDScopeName          string              `json:"-"`
	NodeIDScopeName          string              `json:"-"`

	CopySetConfigs map[proto.NodeRole]map[proto.DiskType]CopySetConfig `json:"copy_set_configs"`
}

type CopySetConfig struct {
	NodeSetCap                int `json:"node_set_cap"`
	NodeSetRackCap            int `json:"node_set_rack_cap"`
	DiskSetCap                int `json:"disk_set_cap"`
	DiskCountPerNodeInDiskSet int `json:"disk_count_per_node_in_disk_set"`

	NodeSetIdcCap int `json:"-"`
}

type manager struct {
	module            string
	allDisks          map[proto.DiskID]*diskItem
	allNodes          map[proto.NodeID]*nodeItem
	topoMgr           *topoMgr
	allocator         atomic.Value
	taskPool          *base.TaskDistribution
	hostPathFilter    sync.Map
	pendingEntries    sync.Map
	raftServer        raftserver.RaftServer
	scopeMgr          scopemgr.ScopeMgrAPI
	persistentHandler persistentHandler

	lastFlushTime time.Time
	spaceStatInfo atomic.Value
	metaLock      sync.RWMutex
	closeCh       chan interface{}
	cfg           DiskMgrConfig
}

func (d *manager) Close() {
	close(d.closeCh)
	d.taskPool.Close()
}

func (d *manager) RefreshExpireTime() {
	d.metaLock.RLock()
	for _, di := range d.allDisks {
		di.lock.Lock()
		di.lastExpireTime = time.Now().Add(time.Duration(d.cfg.HeartbeatExpireIntervalS) * time.Second)
		di.expireTime = time.Now().Add(time.Duration(d.cfg.HeartbeatExpireIntervalS) * time.Second)
		di.lock.Unlock()
	}
	d.metaLock.RUnlock()
}

func (d *manager) SetRaftServer(raftServer raftserver.RaftServer) {
	d.raftServer = raftServer
}

func (d *manager) AllocDiskID(ctx context.Context) (proto.DiskID, error) {
	_, diskID, err := d.scopeMgr.Alloc(ctx, d.cfg.DiskIDScopeName, 1)
	if err != nil {
		return 0, errors.Info(err, "diskMgr.AllocDiskID failed").Detail(err)
	}
	return proto.DiskID(diskID), nil
}

// IsFrequentHeatBeat judge disk heartbeat interval whether small than HeartbeatNotifyIntervalS
func (d *manager) IsFrequentHeatBeat(id proto.DiskID, HeartbeatNotifyIntervalS int) (bool, error) {
	diskInfo, ok := d.getDisk(id)
	if !ok {
		return false, apierrors.ErrCMDiskNotFound
	}
	diskInfo.lock.RLock()
	defer diskInfo.lock.RUnlock()

	newExpireTime := time.Now().Add(time.Duration(d.cfg.HeartbeatExpireIntervalS) * time.Second)
	if newExpireTime.Sub(diskInfo.expireTime) < time.Duration(HeartbeatNotifyIntervalS)*time.Second {
		return true, nil
	}
	return false, nil
}

func (d *manager) CheckDiskInfoDuplicated(ctx context.Context, diskID proto.DiskID, diskInfo *clustermgr.DiskInfo, nodeInfo *clustermgr.NodeInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	di, ok := d.getDisk(diskID)
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
		info: diskItemInfo{DiskInfo: clustermgr.DiskInfo{Host: nodeInfo.Host, Path: diskInfo.Path}},
	}
	if _, ok = d.hostPathFilter.Load(disk.genFilterKey()); ok {
		span.Warn("host and path duplicated")
		return apierrors.ErrIllegalArguments
	}
	return nil
}

func (d *manager) IsDiskWritable(ctx context.Context, id proto.DiskID) (bool, error) {
	diskInfo, ok := d.getDisk(id)
	if !ok {
		return false, apierrors.ErrCMDiskNotFound
	}

	diskInfo.lock.RLock()
	defer diskInfo.lock.RUnlock()

	return diskInfo.isWritable(), nil
}

func (d *manager) SetStatus(ctx context.Context, id proto.DiskID, status proto.DiskStatus, isCommit bool) error {
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

		err := d.persistentHandler.updateDiskStatusNoLocked(id, status)
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
			d.topoMgr.RemoveDiskFromDiskSet(node.info.DiskType, node.info.NodeSetID, disk)
		}

		return nil
	})
}

func (d *manager) IsDroppingDisk(ctx context.Context, id proto.DiskID) (bool, error) {
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

// Stat return disk statistic info of a cluster
func (d *manager) Stat(ctx context.Context) *clustermgr.SpaceStatInfo {
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
func (d *manager) applySwitchReadonly(diskID proto.DiskID, readonly bool) error {
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
	err := d.persistentHandler.updateDiskNoLocked(disk)
	if err != nil {
		disk.info.Readonly = !readonly
		return err
	}
	return nil
}

func (d *manager) GetHeartbeatChangeDisks() []HeartbeatEvent {
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
			if time.Since(disk.expireTime) >= 2*time.Duration(d.cfg.HeartbeatExpireIntervalS)*time.Second {
				disk.lock.RUnlock()
				continue
			}
			ret = append(ret, HeartbeatEvent{DiskID: disk.diskID, IsAlive: false})
			disk.lock.RUnlock()
			continue
		}
		if disk.expireTime.Sub(disk.lastExpireTime) > 1*time.Duration(d.cfg.HeartbeatExpireIntervalS)*time.Second {
			ret = append(ret, HeartbeatEvent{DiskID: disk.diskID, IsAlive: true})
		}
		disk.lock.RUnlock()
	}

	return ret
}

func (d *manager) AllocNodeID(ctx context.Context) (proto.NodeID, error) {
	_, nodeID, err := d.scopeMgr.Alloc(ctx, d.cfg.NodeIDScopeName, 1)
	if err != nil {
		return 0, errors.Info(err, "diskMgr.AllocNodeID failed").Detail(err)
	}
	return proto.NodeID(nodeID), nil
}

func (d *manager) GetTopoInfo(ctx context.Context) *clustermgr.TopoInfo {
	ret := &clustermgr.TopoInfo{
		CurNodeSetID: d.topoMgr.GetNodeSetID(),
		CurDiskSetID: d.topoMgr.GetDiskSetID(),
		AllNodeSets:  make(map[string]map[proto.NodeSetID]*clustermgr.NodeSetInfo),
	}

	nodeSetsMap := d.topoMgr.GetAllNodeSets(ctx)
	for diskType, nodeSets := range nodeSetsMap {
		if _, ok := ret.AllNodeSets[diskType.String()]; !ok {
			ret.AllNodeSets[diskType.String()] = make(map[proto.NodeSetID]*clustermgr.NodeSetInfo)
		}
		for _, nodeSet := range nodeSets {
			nodeSetInfo, ok := ret.AllNodeSets[diskType.String()][nodeSet.ID()]
			if !ok {
				nodeSetInfo = &clustermgr.NodeSetInfo{
					ID:       nodeSet.ID(),
					Number:   nodeSet.GetNodeNum(),
					Nodes:    nodeSet.GetNodeIDs(),
					DiskSets: make(map[proto.DiskSetID][]proto.DiskID),
				}
				ret.AllNodeSets[diskType.String()][nodeSet.ID()] = nodeSetInfo
			}
			diskSets := nodeSet.GetDiskSets()
			for _, diskSet := range diskSets {
				nodeSetInfo.DiskSets[diskSet.ID()] = diskSet.GetDiskIDs()
			}
		}
	}
	return ret
}

func (d *manager) IsDroppedNode(ctx context.Context, nodeID proto.NodeID) (bool, error) {
	node, ok := d.getNode(nodeID)
	if !ok {
		return false, apierrors.ErrCMNodeNotFound
	}

	node.lock.RLock()
	defer node.lock.RUnlock()

	for _, disk := range node.disks {
		if disk.needFilter() {
			return false, apierrors.ErrCMHasDiskNotDroppedOrRepaired
		}
	}
	if node.isUsingStatus() {
		return false, nil
	}
	return true, nil
}

func (d *manager) CheckNodeInfoDuplicated(ctx context.Context, info *clustermgr.NodeInfo) (proto.NodeID, bool) {
	node := &nodeItem{
		info: nodeItemInfo{NodeInfo: clustermgr.NodeInfo{Host: info.Host, DiskType: info.DiskType}},
	}
	if v, ok := d.hostPathFilter.Load(node.genFilterKey()); ok {
		nodeID := v.(proto.NodeID)
		return nodeID, true
	}
	return proto.InvalidNodeID, false
}

func (d *manager) ValidateNodeInfo(ctx context.Context, info *clustermgr.NodeInfo) error {
	if !info.DiskType.IsValid() {
		return apierrors.ErrIllegalArguments
	}
	if info.NodeSetID != nullNodeSetID {
		if err := d.topoMgr.ValidateNodeSetID(ctx, info.DiskType, info.NodeSetID); err != nil {
			return err
		}
	}

	return nil
}

// droppingDisk add a dropping disk
func (d *manager) applyDroppingDisk(ctx context.Context, id proto.DiskID) error {
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

	err := d.persistentHandler.addDroppingDisk(id)
	if err != nil {
		return err
	}
	disk.dropping = true
	// remove disk from diskSet on dropping disk, avoid the new expanded disk not being properly added to the diskSet when dropping node
	if node, ok := d.getNode(disk.info.NodeID); ok { // compatible case
		d.topoMgr.RemoveDiskFromDiskSet(node.info.DiskType, node.info.NodeSetID, disk)
	}

	return nil
}

// droppedDisk set disk dropped
func (d *manager) applyDroppedDisk(ctx context.Context, id proto.DiskID) error {
	exist, err := d.persistentHandler.isDroppingDisk(id)
	if err != nil {
		return errors.Info(err, "diskMgr.droppedDisk get dropping disk failed").Detail(err)
	}
	// concurrent dropped request may cost dropping disk not found, don't return error in this situation
	if !exist {
		return nil
	}

	err = d.persistentHandler.droppedDisk(id)
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

func (d *manager) getDisk(diskID proto.DiskID) (disk *diskItem, exist bool) {
	d.metaLock.RLock()
	disk, exist = d.allDisks[diskID]
	d.metaLock.RUnlock()
	return
}

// getAllDisk copy all diskItem pointer array
func (d *manager) getAllDisk() []*diskItem {
	d.metaLock.RLock()
	total := len(d.allDisks)
	all := make([]*diskItem, 0, total)
	for _, disk := range d.allDisks {
		all = append(all, disk)
	}
	d.metaLock.RUnlock()
	return all
}

// dropNode set nodeStatus Dropped and remove node from nodeSet
func (d *manager) applyDropNode(ctx context.Context, arg *clustermgr.NodeInfoArgs) error {
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
	err := d.persistentHandler.updateNodeNoLocked(node)
	if err != nil {
		span.Error("nodeTbl UpdateNode failed: ", err)
		// revert memory nodeStatus when persist failed
		node.info.Status = proto.NodeStatusNormal
		return errors.Info(err, "nodeTbl UpdateNode failed").Detail(err)
	}
	d.topoMgr.RemoveNodeFromNodeSet(node)

	return nil
}

func (d *manager) getNode(nodeID proto.NodeID) (node *nodeItem, exist bool) {
	d.metaLock.RLock()
	node, exist = d.allNodes[nodeID]
	d.metaLock.RUnlock()
	return
}

func (d *manager) getDiskType(disk *diskItem) proto.DiskType {
	n, _ := d.getNode(disk.info.NodeID)
	if n == nil {
		// compatible
		return proto.DiskTypeHDD
	}
	return n.info.DiskType
}

func (d *manager) validateAllocRet(disks []proto.DiskID) error {
	if d.cfg.HostAware {
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
