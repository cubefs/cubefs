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

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
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
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	DiskIDScopeName = "diskid"
	NodeIDScopeName = "nodeid"
)

type BlobNodeManagerAPI interface {
	// GetNodeInfo return node info with specified node id, it return ErrCMNodeNotFound if node not found
	GetNodeInfo(ctx context.Context, nodeID proto.NodeID) (*clustermgr.BlobNodeInfo, error)
	// GetDiskInfo return disk info, it return ErrDiskNotFound if disk not found
	GetDiskInfo(ctx context.Context, id proto.DiskID) (*clustermgr.BlobNodeDiskInfo, error)
	AddDisk(ctx context.Context, args *clustermgr.BlobNodeDiskInfo) error
	// ListDroppingDisk return all dropping disk info
	ListDroppingDisk(ctx context.Context) ([]*clustermgr.BlobNodeDiskInfo, error)
	// ListDiskInfo return disk list with list option
	ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (disks []*clustermgr.BlobNodeDiskInfo, marker proto.DiskID, err error)
	// AllocChunks return available chunks in data center
	AllocChunks(ctx context.Context, policy AllocPolicy) ([]proto.DiskID, []proto.Vuid, error)

	NodeManagerAPI
	persistentHandler
}

func NewBlobNodeMgr(scopeMgr scopemgr.ScopeMgrAPI, db *normaldb.NormalDB, cfg DiskMgrConfig) (*BlobNodeManager, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewBlobNodeMgr")

	cfg.NodeIDScopeName = NodeIDScopeName
	cfg.DiskIDScopeName = DiskIDScopeName
	defaulter.LessOrEqual(&cfg.RefreshIntervalS, defaultRefreshIntervalS)
	defaulter.LessOrEqual(&cfg.HeartbeatExpireIntervalS, defaultHeartbeatExpireIntervalS)
	defaulter.LessOrEqual(&cfg.FlushIntervalS, defaultFlushIntervalS)
	defaulter.LessOrEqual(&cfg.ApplyConcurrency, defaultApplyConcurrency)
	if cfg.AllocTolerateBuffer >= 0 {
		defaultAllocTolerateBuff = cfg.AllocTolerateBuffer
	}

	if len(cfg.CodeModes) == 0 {
		return nil, errors.New("code mode can not be nil")
	}
	if len(cfg.IDC) == 0 {
		return nil, errors.New("idc can not be nil")
	}

	diskTbl, err := normaldb.OpenBlobNodeDiskTable(db, cfg.EnsureIndex)
	if err != nil {
		return nil, errors.Info(err, "open disk table failed").Detail(err)
	}

	nodeTbl, err := normaldb.OpenBlobNodeTable(db)
	if err != nil {
		return nil, errors.Info(err, "open node table failed").Detail(err)
	}

	bm := &BlobNodeManager{
		diskTbl:        diskTbl,
		nodeTbl:        nodeTbl,
		blobNodeClient: blobnode.New(&cfg.BlobNodeConfig),
	}

	m := &manager{
		topoMgr:           newTopoMgr(),
		taskPool:          base.NewTaskDistribution(int(cfg.ApplyConcurrency), 1),
		scopeMgr:          scopeMgr,
		persistentHandler: bm,

		closeCh: make(chan interface{}),
		cfg:     cfg,
	}
	bm.manager = m

	// initial load data
	err = bm.LoadData(ctx)
	if err != nil {
		return nil, err
	}

	_, ctxNew := trace.StartSpanFromContext(context.Background(), "")
	bm.refresh(ctxNew)

	ticker := time.NewTicker(time.Duration(cfg.RefreshIntervalS) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				bm.refresh(ctxNew)
				bm.checkDroppingNode(ctxNew)
			case <-bm.closeCh:
				return
			}
		}
	}()

	return bm, nil
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

type BlobNodeManager struct {
	*manager

	diskTbl        *normaldb.BlobNodeDiskTable
	nodeTbl        *normaldb.BlobNodeTable
	blobNodeClient blobnode.StorageAPI
}

func (b *BlobNodeManager) GetDiskInfo(ctx context.Context, id proto.DiskID) (*clustermgr.BlobNodeDiskInfo, error) {
	disk, ok := b.getDisk(id)
	if !ok {
		return nil, apierrors.ErrCMDiskNotFound
	}

	var diskInfo clustermgr.BlobNodeDiskInfo
	disk.withRLocked(func() error {
		diskInfo.DiskInfo = disk.info.DiskInfo
		diskInfo.DiskHeartBeatInfo = *(disk.info.extraInfo.(*clustermgr.DiskHeartBeatInfo))
		return nil
	})

	if nodeInfo, ok := b.getNode(diskInfo.NodeID); ok {
		diskInfo.Idc = nodeInfo.info.Idc
		diskInfo.Rack = nodeInfo.info.Rack
		diskInfo.Host = nodeInfo.info.Host
	}
	// need to copy before return, or the higher level may change the disk info by the disk info pointer
	return &(diskInfo), nil
}

func (b *BlobNodeManager) ListDroppingDisk(ctx context.Context) ([]*clustermgr.BlobNodeDiskInfo, error) {
	diskIDs, err := b.diskTbl.GetAllDroppingDisk()
	if err != nil {
		return nil, errors.Info(err, "list dropping disk failed").Detail(err)
	}

	if len(diskIDs) == 0 {
		return nil, nil
	}
	ret := make([]*clustermgr.BlobNodeDiskInfo, len(diskIDs))
	for i := range diskIDs {
		info, err := b.GetDiskInfo(ctx, diskIDs[i])
		if err != nil {
			return nil, err
		}
		ret[i] = info
	}
	return ret, nil
}

// ListDiskInfo return disk info with specified query condition
func (b *BlobNodeManager) ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (disks []*clustermgr.BlobNodeDiskInfo, marker proto.DiskID, err error) {
	if opt == nil {
		return nil, 0, nil
	}
	span := trace.SpanFromContextSafe(ctx)

	if opt.Count > defaultListDiskMaxCount {
		opt.Count = defaultListDiskMaxCount
	}

	diskInfoDBs, err := b.diskTbl.ListDisk(opt)
	if err != nil {
		span.Error("diskMgr ListDiskInfo failed, err: %v", err)
		return nil, 0, errors.Info(err, "diskMgr ListDiskInfo failed").Detail(err)
	}

	if len(diskInfoDBs) > 0 {
		marker = diskInfoDBs[len(diskInfoDBs)-1].DiskID
	}

	for i := range diskInfoDBs {
		diskInfo := b.diskInfoRecordToDiskInfo(diskInfoDBs[i])
		disk, _ := b.getDisk(diskInfo.DiskID)
		disk.withRLocked(func() error {
			heartbeatInfo := disk.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			diskInfo.FreeChunkCnt = heartbeatInfo.FreeChunkCnt
			diskInfo.UsedChunkCnt = heartbeatInfo.UsedChunkCnt
			diskInfo.MaxChunkCnt = heartbeatInfo.MaxChunkCnt
			diskInfo.Free = heartbeatInfo.Free
			diskInfo.Used = heartbeatInfo.Used
			diskInfo.Size = heartbeatInfo.Size
			return nil
		})
		disks = append(disks, diskInfo)
	}
	if len(disks) == 0 {
		marker = proto.InvalidDiskID
	}

	return disks, marker, nil
}

func (b *BlobNodeManager) AddDisk(ctx context.Context, args *clustermgr.BlobNodeDiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	node, ok := b.getNode(args.NodeID)
	if !ok {
		span.Warnf("node not exist, disk info: %v", args)
		return apierrors.ErrCMNodeNotFound
	}
	err := node.withRLocked(func() error {
		if node.info.Status == proto.NodeStatusDropped {
			span.Warnf("node is dropped, disk info: %v", args)
			return apierrors.ErrCMNodeNotFound
		}
		if node.dropping {
			span.Warnf("node is dropping, disk info: %v", args)
			return apierrors.ErrCMNodeIsDropping
		}
		if err := b.CheckDiskInfoDuplicated(ctx, args.DiskID, &args.DiskInfo, &node.info.NodeInfo); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, disk info: %v, error: %v", args, err)
		return errors.Info(apierrors.ErrUnexpected).Detail(err)
	}
	pendingKey := fmtApplyContextKey("disk-add", args.DiskID.ToString())
	b.pendingEntries.Store(pendingKey, nil)
	defer b.pendingEntries.Delete(pendingKey)
	proposeInfo := base.EncodeProposeInfo(b.GetModuleName(), OperTypeAddDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = b.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		return apierrors.ErrRaftPropose
	}
	if v, _ := b.manager.pendingEntries.Load(pendingKey); v != nil {
		return v.(error)
	}
	return nil
}

func (b *BlobNodeManager) DropDisk(ctx context.Context, args *clustermgr.DiskInfoArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	isDropping, err := b.applyDroppingDisk(ctx, args.DiskID, false)
	if err != nil {
		span.Warnf("DropDisk error: %v", err)
		return errors.Info(apierrors.ErrUnexpected).Detail(err)
	}
	// is dropping, then return success
	if isDropping {
		return nil
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("DropDisk json marshal failed, args: %v, error: %v", args, err)
		return errors.Info(apierrors.ErrUnexpected).Detail(err)

	}
	pendingKey := fmtApplyContextKey("disk-dropping", args.DiskID.ToString())
	b.pendingEntries.Store(pendingKey, nil)
	defer b.pendingEntries.Delete(pendingKey)
	proposeInfo := base.EncodeProposeInfo(b.GetModuleName(), OperTypeDroppingDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = b.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		return apierrors.ErrRaftPropose
	}
	if v, _ := b.pendingEntries.Load(pendingKey); v != nil {
		return v.(error)
	}
	return nil
}

func (b *BlobNodeManager) DropNode(ctx context.Context, args *clustermgr.NodeInfoArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	isDroppingOrDropped, err := b.applyDroppingNode(ctx, args.NodeID, false)
	if err != nil {
		span.Warnf("DropNode applyDroppingNode err: %v", err)
		return err
	}
	// is dropping or dropped, then return success
	if isDroppingOrDropped {
		return nil
	}
	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("DropNode json marshal failed, args: %v, error: %v", args, err)
		return errors.Info(apierrors.ErrUnexpected).Detail(err)
	}
	pendingKey := fmtApplyContextKey("node-dropping", args.NodeID.ToString())
	b.pendingEntries.Store(pendingKey, nil)
	defer b.pendingEntries.Delete(pendingKey)
	proposeInfo := base.EncodeProposeInfo(b.GetModuleName(), OperTypeDroppingNode, data, base.ProposeContext{ReqID: span.TraceID()})
	err = b.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		return apierrors.ErrRaftPropose
	}
	if v, _ := b.pendingEntries.Load(pendingKey); v != nil {
		return v.(error)
	}
	return nil
}

func (b *BlobNodeManager) GetNodeInfo(ctx context.Context, nodeID proto.NodeID) (*clustermgr.BlobNodeInfo, error) {
	node, ok := b.getNode(nodeID)
	if !ok {
		return nil, apierrors.ErrCMNodeNotFound
	}

	// need to copy before return, or the higher level may change the node info by pointer
	nodeInfo := &clustermgr.BlobNodeInfo{}
	node.withRLocked(func() error {
		nodeInfo.NodeInfo = node.info.NodeInfo
		return nil
	})

	return nodeInfo, nil
}

func (b *BlobNodeManager) AllocChunks(ctx context.Context, policy AllocPolicy) ([]proto.DiskID, []proto.Vuid, error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(ctx, "AllocChunks", trace.SpanFromContextSafe(ctx).TraceID())

	var (
		err       error
		allocator = b.allocator.Load().(*allocator)
		// to make sure return disks order match with policy.Vuids
		idcVuidIndexMap = make(map[string]map[proto.Vuid]int)
		idcVuidMap      = make(map[string][]proto.Vuid)
		idcDiskMap      = make(map[string][]proto.DiskID)
		ret             = make([]proto.DiskID, len(policy.Vuids))
		retVuids        = make([]proto.Vuid, len(policy.Vuids))
		retryTimes      = policy.RetryTimes
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

		diskInfo, _ := b.getDisk(disks[0])
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

		if err := b.validateAllocRet(disks); err != nil {
			return nil, nil, err
		}

		failVuids := make([]proto.Vuid, 0)
		wg := sync.WaitGroup{}
		wg.Add(len(disks))

		for ii := range disks {
			idx := ii

			disk, _ := b.getDisk(disks[idx])
			disk.lock.RLock()
			host := disk.info.Host
			disk.lock.RUnlock()

			go func() {
				defer wg.Done()

				blobNodeErr := b.blobNodeClient.CreateChunk(ctx, host,
					&blobnode.CreateChunkArgs{DiskID: disks[idx], Vuid: vuids[idx], ChunkSize: b.cfg.ChunkSize})
				if blobNodeErr != nil {
					vuidPrefix := vuids[idx].VuidPrefix()
					newVuid := proto.EncodeVuid(vuidPrefix, vuids[idx].Epoch()+1)
					retVuids[idcVuidIndexMap[idc][vuids[idx]]] = newVuid
					failVuids = append(failVuids, newVuid)

					span.Errorf("allocate chunk from blob node failed, diskID: %d, host: %s, err: %s", disks[idx], host, blobNodeErr)
					return
				}
				excludes = append(excludes, disks[idx])
				ret[idcVuidIndexMap[idc][vuids[idx]]] = disks[idx]
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

	return ret, retVuids, err
}

func (b *BlobNodeManager) LoadData(ctx context.Context) error {
	diskDBs, err := b.diskTbl.GetAllDisks()
	if err != nil {
		return errors.Info(err, "get all disks failed").Detail(err)
	}
	droppingDiskDBs, err := b.diskTbl.GetAllDroppingDisk()
	if err != nil {
		return errors.Info(err, "get dropping disks failed").Detail(err)
	}
	nodeDBs, err := b.nodeTbl.GetAllNodes()
	if err != nil {
		return errors.Info(err, "get all nodes failed").Detail(err)
	}
	droppingNodeDBs, err := b.nodeTbl.GetAllDroppingNode()
	if err != nil {
		return errors.Info(err, "get dropping nodes failed").Detail(err)
	}
	droppingDisks := make(map[proto.DiskID]bool)
	for _, diskID := range droppingDiskDBs {
		droppingDisks[diskID] = true
	}
	droppingNodes := make(map[proto.NodeID]bool)
	for _, nodeID := range droppingNodeDBs {
		droppingNodes[nodeID] = true
	}

	allNodes := make(map[proto.NodeID]*nodeItem)
	curNodeSetID := ecNodeSetID
	curDiskSetID := ecDiskSetID
	for _, node := range nodeDBs {
		info := b.nodeInfoRecordToNodeInfo(node)
		ni := &nodeItem{
			nodeID: info.NodeID,
			info:   nodeItemInfo{NodeInfo: info.NodeInfo},
			disks:  make(map[proto.DiskID]*diskItem),
		}
		if droppingNodes[ni.nodeID] {
			ni.dropping = true
		}
		allNodes[info.NodeID] = ni
		b.hostPathFilter.Store(ni.genFilterKey(), ni.nodeID)
		// not filter dropped node to generate nodeSet
		b.topoMgr.AddNodeToNodeSet(ni)
		if info.NodeSetID >= curNodeSetID {
			curNodeSetID = info.NodeSetID
		}
	}
	b.allNodes = allNodes

	allDisks := make(map[proto.DiskID]*diskItem)
	for _, disk := range diskDBs {
		info := b.diskInfoRecordToDiskInfo(disk)
		di := &diskItem{
			diskID:         info.DiskID,
			info:           diskItemInfo{DiskInfo: info.DiskInfo, extraInfo: &info.DiskHeartBeatInfo},
			weightGetter:   blobNodeDiskWeightGetter,
			weightDecrease: blobNodeDiskWeightDecrease,
			// bug fix: do not initial disk expire time, or may cause volume health change when start volume manager
			// lastExpireTime: time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second),
			// expireTime:     time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second),
		}
		if droppingDisks[di.diskID] {
			di.dropping = true
		}
		allDisks[info.DiskID] = di
		if di.needFilter() {
			b.hostPathFilter.Store(di.genFilterKey(), 1)
		}
		ni, ok := b.getNode(info.NodeID)
		if ok { // compatible case and not filter dropped disk to generate diskSet
			b.topoMgr.AddDiskToDiskSet(ni.info.DiskType, ni.info.NodeSetID, di)
			ni.disks[info.DiskID] = di
		}
		if info.DiskSetID > 0 && info.DiskSetID >= curDiskSetID {
			curDiskSetID = info.DiskSetID
		}
	}

	b.allDisks = allDisks
	b.topoMgr.SetNodeSetID(curNodeSetID)
	b.topoMgr.SetDiskSetID(curDiskSetID)

	return nil
}

func (b *BlobNodeManager) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) error {
	span := trace.SpanFromContextSafe(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(operTypes))
	errs := make([]error, len(operTypes))

	for i, t := range operTypes {
		idx := i
		_, taskCtx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[idx].ReqID)

		switch t {
		case OperTypeAddDisk:
			diskInfo := &clustermgr.BlobNodeDiskInfo{}
			err := json.Unmarshal(datas[idx], diskInfo)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			b.taskPool.Run(synchronizedDiskID, func() {
				// add disk run on fixed goroutine synchronously
				err = b.applyAddDisk(taskCtx, diskInfo)
				// don't return error if disk already exist
				if err != nil && !errors.Is(err, ErrDiskExist) {
					errs[idx] = err
				}
				wg.Done()
			})
		case OperTypeSetDiskStatus:
			setStatusArgs := &clustermgr.DiskSetArgs{}
			err := json.Unmarshal(datas[idx], setStatusArgs)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			b.taskPool.Run(b.getTaskIdx(setStatusArgs.DiskID), func() {
				errs[idx] = b.SetStatus(taskCtx, setStatusArgs.DiskID, setStatusArgs.Status, true)
				wg.Done()
			})
		case OperTypeDroppingDisk:
			args := &clustermgr.DiskInfoArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			b.taskPool.Run(b.getTaskIdx(args.DiskID), func() {
				_, errs[idx] = b.applyDroppingDisk(taskCtx, args.DiskID, true)
				wg.Done()
			})
		case OperTypeDroppedDisk:
			args := &clustermgr.DiskInfoArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			b.taskPool.Run(b.getTaskIdx(args.DiskID), func() {
				errs[idx] = b.applyDroppedDisk(taskCtx, args.DiskID)
				wg.Done()
			})
		case OperTypeHeartbeatDiskInfo:
			args := &clustermgr.DisksHeartbeatArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// disk heartbeat has no necessary to run in single goroutine, so we just put it on random goroutine
			b.taskPool.Run(rand.Intn(int(b.cfg.ApplyConcurrency)), func() {
				errs[idx] = b.applyHeartBeatDiskInfo(taskCtx, args.Disks)
				wg.Done()
			})
		case OperTypeSwitchReadonly:
			args := &clustermgr.DiskAccessArgs{}
			err := json.Unmarshal(datas[i], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			b.taskPool.Run(b.getTaskIdx(args.DiskID), func() {
				errs[idx] = b.applySwitchReadonly(args.DiskID, args.Readonly)
				wg.Done()
			})
		case OperTypeAdminUpdateDisk:
			args := &clustermgr.BlobNodeDiskInfo{}
			err := json.Unmarshal(datas[i], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			b.taskPool.Run(b.getTaskIdx(args.DiskID), func() {
				errs[idx] = b.applyAdminUpdateDisk(ctx, args)
				wg.Done()
			})
		case OperTypeAddNode:
			args := &clustermgr.BlobNodeInfo{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// add node run on fixed goroutine synchronously
			b.taskPool.Run(b.getTaskIdx(synchronizedDiskID), func() {
				err = b.applyAddNode(taskCtx, args)
				// don't return error if node already exist
				if err != nil && !errors.Is(err, ErrNodeExist) {
					errs[idx] = err
				}
				wg.Done()
			})
		case OperTypeDroppingNode:
			args := &clustermgr.NodeInfoArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// drop node run on fixed goroutine synchronously
			b.taskPool.Run(b.getTaskIdx(synchronizedDiskID), func() {
				_, err = b.applyDroppingNode(taskCtx, args.NodeID, true)
				if err != nil {
					errs[idx] = err
				}
				wg.Done()
			})
		case OperTypeDroppedNode:
			args := &clustermgr.NodeInfoArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// dropped a node run on fixed goroutine synchronously
			b.taskPool.Run(b.getTaskIdx(synchronizedDiskID), func() {
				err = b.applyDroppedNode(taskCtx, args.NodeID)
				if err != nil {
					errs[idx] = err
				}
				wg.Done()
			})
		default:
		}
	}
	wg.Wait()
	failedCount := 0
	for i := range errs {
		if errs[i] != nil {
			failedCount += 1
			span.Error(fmt.Sprintf("operation type: %d, apply failed => ", operTypes[i]), errors.Detail(errs[i]))
		}
	}
	if failedCount > 0 {
		return errors.New(fmt.Sprintf("batch apply failed, failed count: %d", failedCount))
	}

	return nil
}

// heartBeatDiskInfo process disk's heartbeat
func (b *BlobNodeManager) applyHeartBeatDiskInfo(ctx context.Context, infos []*clustermgr.DiskHeartBeatInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	expireTime := time.Now().Add(time.Duration(b.cfg.HeartbeatExpireIntervalS) * time.Second)
	for i := range infos {
		info := infos[i]

		disk, ok := b.getDisk(info.DiskID)
		// sometimes, we may not find disk from allDisks
		// it was happened when disk register and heartbeat request very close
		if !ok {
			span.Warnf("disk not found in all disk, diskID: %d", info.DiskID)
			continue
		}
		// memory modify disk heartbeat info, dump into db timely
		disk.withLocked(func() error {
			heartbeatInfo := disk.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			heartbeatInfo.Free = info.Free
			heartbeatInfo.Size = info.Size
			heartbeatInfo.Used = info.Used
			heartbeatInfo.UsedChunkCnt = info.UsedChunkCnt
			// calculate free and max chunk count
			heartbeatInfo.MaxChunkCnt = info.Size / b.cfg.ChunkSize
			// use the minimum value as free chunk count
			heartbeatInfo.FreeChunkCnt = heartbeatInfo.MaxChunkCnt - heartbeatInfo.UsedChunkCnt
			freeChunkCnt := info.Free / b.cfg.ChunkSize
			if freeChunkCnt < heartbeatInfo.FreeChunkCnt {
				span.Debugf("use minimum free chunk count, disk id[%d], free chunk[%d]", disk.diskID, freeChunkCnt)
				heartbeatInfo.FreeChunkCnt = freeChunkCnt
			}
			if heartbeatInfo.FreeChunkCnt < 0 {
				heartbeatInfo.FreeChunkCnt = 0
			}

			disk.lastExpireTime = disk.expireTime
			disk.expireTime = expireTime
			return nil
		})

	}
	return nil
}

// applyAddDisk add a new disk into cluster, it return ErrDiskExist if disk already exist
func (b *BlobNodeManager) applyAddDisk(ctx context.Context, info *clustermgr.BlobNodeDiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)

	b.metaLock.Lock()
	defer b.metaLock.Unlock()

	// compatible case: disk register again to diskSet
	di, ok := b.allDisks[info.DiskID]
	if ok && (di.info.NodeID != proto.InvalidNodeID || info.NodeID == proto.InvalidNodeID) {
		return nil
	}
	// alloc diskSetID and compatible case: update follower first
	if node, ok := b.allNodes[info.NodeID]; ok {
		err := node.withRLocked(func() error {
			if node.info.Status == proto.NodeStatusDropped || node.dropping {
				span.Warnf("node is dropped or dropping, disk info: %v", info)
				pendingKey := fmtApplyContextKey("disk-add", info.DiskID.ToString())
				if _, ok := b.pendingEntries.Load(pendingKey); ok {
					b.pendingEntries.Store(pendingKey, apierrors.ErrCMNodeNotFound)
				}
				return apierrors.ErrCMNodeNotFound
			}
			return nil
		})
		// return err by pendingEntries
		if err != nil {
			return nil
		}
		info.DiskSetID = b.topoMgr.AllocDiskSetID(ctx, &info.DiskInfo, &node.info.NodeInfo, b.cfg.CopySetConfigs[node.info.Role][node.info.DiskType])
	}

	// calculate free and max chunk count
	info.MaxChunkCnt = info.Size / b.cfg.ChunkSize
	info.FreeChunkCnt = info.MaxChunkCnt - info.UsedChunkCnt
	err := b.diskTbl.AddDisk(b.diskInfoToDiskInfoRecord(info))
	if err != nil {
		span.Error("diskMgr.addDisk add disk failed: ", err)
		return errors.Info(err, "diskMgr.addDisk add disk failed").Detail(err)
	}

	disk := &diskItem{
		diskID:         info.DiskID,
		info:           diskItemInfo{DiskInfo: info.DiskInfo, extraInfo: &info.DiskHeartBeatInfo},
		weightGetter:   blobNodeDiskWeightGetter,
		weightDecrease: blobNodeDiskWeightDecrease,
		expireTime:     time.Now().Add(time.Duration(b.cfg.HeartbeatExpireIntervalS) * time.Second),
	}
	if node, ok := b.allNodes[info.NodeID]; ok { // compatible case
		node.withLocked(func() error {
			node.disks[info.DiskID] = disk
			return nil
		})
		b.topoMgr.AddDiskToDiskSet(node.info.DiskType, node.info.NodeSetID, disk)
	}
	b.allDisks[info.DiskID] = disk
	b.hostPathFilter.Store(disk.genFilterKey(), 1)

	return nil
}

// applyAddNode add a new node into cluster, it returns ErrNodeExist if node already exist
func (b *BlobNodeManager) applyAddNode(ctx context.Context, info *clustermgr.BlobNodeInfo) error {
	span := trace.SpanFromContextSafe(ctx)

	b.metaLock.Lock()
	defer b.metaLock.Unlock()

	// concurrent double check
	_, ok := b.allNodes[info.NodeID]
	if ok {
		return nil
	}

	// alloc NodeSetID
	if info.NodeSetID == nullNodeSetID {
		info.NodeSetID = b.topoMgr.AllocNodeSetID(ctx, &info.NodeInfo, b.cfg.CopySetConfigs[info.Role][info.DiskType], b.cfg.RackAware)
	}
	info.Status = proto.NodeStatusNormal

	ni := &nodeItem{nodeID: info.NodeID, info: nodeItemInfo{NodeInfo: info.NodeInfo}, disks: make(map[proto.DiskID]*diskItem)}

	// add node to nodeTbl and nodeSet
	err := b.persistentHandler.updateNodeNoLocked(ni)
	if err != nil {
		span.Error("diskMgr.addNode add node failed: ", err)
		return errors.Info(err, "diskMgr.addNode add node failed").Detail(err)
	}

	b.topoMgr.AddNodeToNodeSet(ni)
	b.allNodes[info.NodeID] = ni
	b.hostPathFilter.Store(ni.genFilterKey(), ni.nodeID)

	return nil
}

func (b *BlobNodeManager) applyAdminUpdateDisk(ctx context.Context, diskInfo *clustermgr.BlobNodeDiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	disk, ok := b.allDisks[diskInfo.DiskID]
	if !ok {
		span.Errorf("admin update disk, diskId:%d not exist", diskInfo.DiskID)
		return ErrDiskNotExist
	}

	return disk.withLocked(func() error {
		if diskInfo.Status.IsValid() {
			disk.info.Status = diskInfo.Status
		}

		heartbeatInfo := disk.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
		if diskInfo.MaxChunkCnt > 0 {
			heartbeatInfo.MaxChunkCnt = diskInfo.MaxChunkCnt
		}
		if diskInfo.FreeChunkCnt > 0 {
			heartbeatInfo.FreeChunkCnt = diskInfo.FreeChunkCnt
		}
		diskRecord := b.diskInfoToDiskInfoRecord(&clustermgr.BlobNodeDiskInfo{DiskInfo: disk.info.DiskInfo, DiskHeartBeatInfo: *heartbeatInfo})
		return b.diskTbl.UpdateDisk(diskInfo.DiskID, diskRecord)
	})
}

func (b *BlobNodeManager) diskInfoToDiskInfoRecord(info *clustermgr.BlobNodeDiskInfo) *normaldb.BlobNodeDiskInfoRecord {
	return &normaldb.BlobNodeDiskInfoRecord{
		DiskInfoRecord: normaldb.DiskInfoRecord{
			Version:      normaldb.DiskInfoVersionNormal,
			DiskID:       info.DiskID,
			ClusterID:    info.ClusterID,
			Idc:          info.Idc,
			Rack:         info.Rack,
			Host:         info.Host,
			Path:         info.Path,
			Status:       info.Status,
			Readonly:     info.Readonly,
			CreateAt:     info.CreateAt,
			LastUpdateAt: info.LastUpdateAt,
			DiskSetID:    info.DiskSetID,
			NodeID:       info.NodeID,
		},
		UsedChunkCnt: info.UsedChunkCnt,
		Used:         info.Used,
		Size:         info.Size,
		Free:         info.Free,
		MaxChunkCnt:  info.MaxChunkCnt,
		FreeChunkCnt: info.FreeChunkCnt,
	}
}

func (b *BlobNodeManager) diskInfoRecordToDiskInfo(infoDB *normaldb.BlobNodeDiskInfoRecord) *clustermgr.BlobNodeDiskInfo {
	return &clustermgr.BlobNodeDiskInfo{
		DiskInfo: clustermgr.DiskInfo{
			ClusterID:    infoDB.ClusterID,
			Idc:          infoDB.Idc,
			Rack:         infoDB.Rack,
			Host:         infoDB.Host,
			Path:         infoDB.Path,
			Status:       infoDB.Status,
			Readonly:     infoDB.Readonly,
			CreateAt:     infoDB.CreateAt,
			LastUpdateAt: infoDB.LastUpdateAt,
			DiskSetID:    infoDB.DiskSetID,
			NodeID:       infoDB.NodeID,
		},

		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
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

func (b *BlobNodeManager) nodeInfoRecordToNodeInfo(infoDB *normaldb.BlobNodeInfoRecord) *clustermgr.BlobNodeInfo {
	return &clustermgr.BlobNodeInfo{
		NodeInfo: clustermgr.NodeInfo{
			NodeID:    infoDB.NodeID,
			NodeSetID: infoDB.NodeSetID,
			ClusterID: infoDB.ClusterID,
			DiskType:  infoDB.DiskType,
			Idc:       infoDB.Idc,
			Rack:      infoDB.Rack,
			Host:      infoDB.Host,
			Role:      infoDB.Role,
			Status:    infoDB.Status,
		},
	}
}

func (b *BlobNodeManager) nodeInfoToNodeInfoRecord(info *clustermgr.BlobNodeInfo) *normaldb.BlobNodeInfoRecord {
	return &normaldb.BlobNodeInfoRecord{
		NodeInfoRecord: normaldb.NodeInfoRecord{
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
		},
	}
}

type blobNodePersistentHandler = BlobNodeManager

func (b *blobNodePersistentHandler) updateDiskNoLocked(di *diskItem) error {
	return b.diskTbl.UpdateDisk(di.diskID, b.diskInfoToDiskInfoRecord(&clustermgr.BlobNodeDiskInfo{
		DiskInfo:          di.info.DiskInfo,
		DiskHeartBeatInfo: *di.info.extraInfo.(*clustermgr.DiskHeartBeatInfo),
	}))
}

func (b *blobNodePersistentHandler) updateDiskStatusNoLocked(id proto.DiskID, status proto.DiskStatus) error {
	return b.diskTbl.UpdateDiskStatus(id, status)
}

func (b *blobNodePersistentHandler) addDiskNoLocked(di *diskItem) error {
	return b.diskTbl.AddDisk(b.diskInfoToDiskInfoRecord(&clustermgr.BlobNodeDiskInfo{
		DiskInfo:          di.info.DiskInfo,
		DiskHeartBeatInfo: *di.info.extraInfo.(*clustermgr.DiskHeartBeatInfo),
	}))
}

func (b *blobNodePersistentHandler) updateNodeNoLocked(n *nodeItem) error {
	return b.nodeTbl.UpdateNode(b.nodeInfoToNodeInfoRecord(&clustermgr.BlobNodeInfo{
		NodeInfo: n.info.NodeInfo,
	}))
}

func (b *blobNodePersistentHandler) addDroppingDisk(id proto.DiskID) error {
	return b.diskTbl.AddDroppingDisk(id)
}

func (b *blobNodePersistentHandler) addDroppingNode(id proto.NodeID) error {
	return b.nodeTbl.AddDroppingNode(id)
}

func (b *blobNodePersistentHandler) isDroppingDisk(id proto.DiskID) (bool, error) {
	return b.diskTbl.IsDroppingDisk(id)
}

func (b *blobNodePersistentHandler) isDroppingNode(id proto.NodeID) (bool, error) {
	return b.nodeTbl.IsDroppingNode(id)
}

func (b *blobNodePersistentHandler) droppedDisk(id proto.DiskID) error {
	return b.diskTbl.DroppedDisk(id)
}

func (b *blobNodePersistentHandler) droppedNode(id proto.NodeID) error {
	return b.nodeTbl.DroppedNode(id)
}

func blobNodeDiskWeightGetter(extraInfo interface{}) int64 {
	return extraInfo.(*clustermgr.DiskHeartBeatInfo).FreeChunkCnt
}

func blobNodeDiskWeightDecrease(extraInfo interface{}, num int64) {
	info := extraInfo.(*clustermgr.DiskHeartBeatInfo)
	info.FreeChunkCnt -= num
}
