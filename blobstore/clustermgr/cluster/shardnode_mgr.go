package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	ShardNodeDiskIDScopeName = "sn-diskid"
	ShardNodeIDScopeName     = "sn-nodeid"
)

type ShardNodeManagerAPI interface {
	// GetNodeInfo return node info with specified node id, it returns ErrCMNodeNotFound if node not found
	GetNodeInfo(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error)
	// GetDiskInfo return disk info, it return ErrDiskNotFound if disk not found
	GetDiskInfo(ctx context.Context, id proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error)
	// AddDisk add shardNode disk to CM
	AddDisk(ctx context.Context, args *clustermgr.ShardNodeDiskInfo) error
	// ListDroppingDisk return all dropping disk info
	ListDroppingDisk(ctx context.Context) ([]*clustermgr.ShardNodeDiskInfo, error)
	// ListDiskInfo return disk list with list option
	ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (disks []*clustermgr.ShardNodeDiskInfo, marker proto.DiskID, err error)
	// AllocShards return available disk with specified alloc policy
	AllocShards(ctx context.Context, policy AllocShardsPolicy) ([]proto.DiskID, proto.DiskSetID, error)

	NodeManagerAPI
	persistentHandler
}

type ShardNodeAPI interface {
	AddShard(ctx context.Context, host string, args shardnode.AddShardArgs) error
	GetShardUintInfo(ctx context.Context, host string, args shardnode.GetShardArgs) (ret clustermgr.ShardUnitInfo, err error)
}

func NewShardNodeMgr(scopeMgr scopemgr.ScopeMgrAPI, db *normaldb.NormalDB, cfg DiskMgrConfig) (*ShardNodeManager, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewShardNodeMgr")

	cfg.NodeIDScopeName = ShardNodeIDScopeName
	cfg.DiskIDScopeName = ShardNodeDiskIDScopeName
	defaulter.LessOrEqual(&cfg.RefreshIntervalS, defaultRefreshIntervalS)
	defaulter.LessOrEqual(&cfg.HeartbeatExpireIntervalS, defaultHeartbeatExpireIntervalS)
	defaulter.LessOrEqual(&cfg.FlushIntervalS, defaultFlushIntervalS)
	defaulter.LessOrEqual(&cfg.ApplyConcurrency, defaultApplyConcurrency)
	if cfg.AllocTolerateBuffer >= 0 {
		defaultAllocTolerateBuff = cfg.AllocTolerateBuffer
	}

	if len(cfg.CodeModes) != 1 {
		return nil, errors.New("shardnode code mode length must be 1")
	}
	if len(cfg.IDC) == 0 {
		return nil, errors.New("idc can not be nil")
	}

	diskTbl, err := normaldb.OpenShardNodeDiskTable(db, cfg.EnsureIndex)
	if err != nil {
		return nil, errors.Info(err, "open disk table failed").Detail(err)
	}

	nodeTbl, err := normaldb.OpenShardNodeTable(db)
	if err != nil {
		return nil, errors.Info(err, "open node table failed").Detail(err)
	}

	sm := &ShardNodeManager{
		diskTbl:         diskTbl,
		nodeTbl:         nodeTbl,
		shardNodeClient: shardnode.New(cfg.ShardNodeConfig),
	}

	m := &manager{
		topoMgr:           newTopoMgr(),
		taskPool:          base.NewTaskDistribution(int(cfg.ApplyConcurrency), 1),
		scopeMgr:          scopeMgr,
		persistentHandler: sm,

		closeCh: make(chan interface{}),
		cfg:     cfg,
	}
	sm.manager = m

	// initial load data
	err = sm.LoadData(ctx)
	if err != nil {
		return nil, err
	}

	_, ctxNew := trace.StartSpanFromContext(context.Background(), "")
	sm.refresh(ctxNew)

	ticker := time.NewTicker(time.Duration(cfg.RefreshIntervalS) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sm.refresh(ctxNew)
			case <-sm.closeCh:
				return
			}
		}
	}()

	return sm, nil
}

type AllocShardsPolicy struct {
	DiskType        proto.DiskType
	Suids           []proto.Suid
	Range           sharding.Range
	RouteVersion    proto.RouteVersion
	ExcludeDiskSets []proto.DiskSetID

	RepairUnits  []clustermgr.ShardUnit
	ExcludeDisks []proto.DiskID
	DiskSetID    proto.DiskSetID
	Idc          string
}

type ShardNodeManager struct {
	*manager

	diskTbl         *normaldb.ShardNodeDiskTable
	nodeTbl         *normaldb.ShardNodeTable
	shardNodeClient ShardNodeAPI
}

func (s *ShardNodeManager) GetDiskInfo(ctx context.Context, id proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error) {
	disk, ok := s.getDisk(id)
	if !ok {
		return nil, apierrors.ErrCMDiskNotFound
	}

	var diskInfo clustermgr.ShardNodeDiskInfo
	// need to copy before return, or the higher level may change the disk info by the pointer
	disk.withRLocked(func() error {
		diskInfo.DiskInfo = disk.info.DiskInfo
		diskInfo.ShardNodeDiskHeartbeatInfo = *(disk.info.extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo))
		return nil
	})
	nodeInfo, _ := s.getNode(diskInfo.NodeID)
	diskInfo.Idc = nodeInfo.info.Idc
	diskInfo.Rack = nodeInfo.info.Rack
	diskInfo.Host = nodeInfo.info.Host
	return &(diskInfo), nil
}

func (s *ShardNodeManager) ListDroppingDisk(ctx context.Context) ([]*clustermgr.ShardNodeDiskInfo, error) {
	return nil, nil
}

// ListDiskInfo return disk info with specified query condition
func (s *ShardNodeManager) ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (disks []*clustermgr.ShardNodeDiskInfo, marker proto.DiskID, err error) {
	if opt == nil {
		return nil, 0, nil
	}
	span := trace.SpanFromContextSafe(ctx)

	if opt.Count > defaultListDiskMaxCount {
		opt.Count = defaultListDiskMaxCount
	}

	diskInfoDBs, err := s.diskTbl.ListDisk(opt)
	if err != nil {
		span.Error("shardNodeManager ListDiskInfo failed, err: %v", err)
		return nil, 0, errors.Info(err, "shardNodeManager ListDiskInfo failed").Detail(err)
	}

	if len(diskInfoDBs) > 0 {
		marker = diskInfoDBs[len(diskInfoDBs)-1].DiskID
	}

	for i := range diskInfoDBs {
		diskInfo := s.diskInfoRecordToDiskInfo(diskInfoDBs[i])
		disk, _ := s.getDisk(diskInfo.DiskID)
		disk.withRLocked(func() error {
			heartbeatInfo := disk.info.extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo)
			diskInfo.FreeShardCnt = heartbeatInfo.FreeShardCnt
			diskInfo.UsedShardCnt = heartbeatInfo.UsedShardCnt
			diskInfo.MaxShardCnt = heartbeatInfo.MaxShardCnt
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

func (s *ShardNodeManager) AddDisk(ctx context.Context, args *clustermgr.ShardNodeDiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	node, ok := s.getNode(args.NodeID)
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
		if err := s.CheckDiskInfoDuplicated(ctx, args.DiskID, &args.DiskInfo, &node.info.NodeInfo); err != nil {
			return err
		}
		// disk idc/rack/host uses node one
		args.Idc = node.info.Idc
		args.Rack = node.info.Rack
		args.Host = node.info.Host
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
	s.pendingEntries.Store(pendingKey, nil)
	defer s.pendingEntries.Delete(pendingKey)
	proposeInfo := base.EncodeProposeInfo(s.GetModuleName(), OperTypeAddDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		return apierrors.ErrRaftPropose
	}
	if v, _ := s.manager.pendingEntries.Load(pendingKey); v != nil {
		return v.(error)
	}
	return nil
}

func (s *ShardNodeManager) GetNodeInfo(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error) {
	node, ok := s.getNode(nodeID)
	if !ok {
		return nil, apierrors.ErrCMNodeNotFound
	}

	// need to copy before return, or the higher level may change the node info by pointer
	nodeInfo := &clustermgr.ShardNodeInfo{}
	node.withRLocked(func() error {
		nodeInfo.NodeInfo = node.info.NodeInfo
		nodeInfo.ShardNodeExtraInfo = node.info.extraInfo.(clustermgr.ShardNodeExtraInfo)
		return nil
	})

	return nodeInfo, nil
}

// AllocShards not retry when alloc one shard failed, so the caller can implement the retry logic as needed.
func (s *ShardNodeManager) AllocShards(ctx context.Context, policy AllocShardsPolicy) ([]proto.DiskID, proto.DiskSetID, error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(ctx, "AllocShards", trace.SpanFromContextSafe(ctx).TraceID())

	var (
		err               error
		addShardLock      sync.Mutex
		excludesDiskSetID proto.DiskSetID
		allocator         = s.allocator.Load().(*allocator)
		// to make sure return disks order match with policy.Suids
		suidIndexMap = make(map[proto.Suid]int)
		suidDiskMap  = make(map[proto.Suid]proto.DiskID, len(policy.Suids))
		units        = make([]clustermgr.ShardUnit, s.cfg.CodeModes[0].GetShardNum())
		retDiskIDs   = make([]proto.DiskID, len(policy.Suids))
	)

	// repair shard case
	if len(policy.ExcludeDisks) > 0 {
		ret, err := allocator.ReAlloc(ctx, reAllocPolicy{
			diskType:  policy.DiskType,
			diskSetID: policy.DiskSetID,
			idc:       policy.Idc,
			count:     len(policy.Suids),
			excludes:  policy.ExcludeDisks,
		})
		if err != nil {
			return nil, nullDiskSetID, err
		}
		units = policy.RepairUnits
		for idx, diskID := range ret {
			suid := policy.Suids[idx]
			suidIndexMap[suid] = idx
			suidDiskMap[suid] = ret[idx]
			units = append(units, clustermgr.ShardUnit{
				Suid:    suid,
				DiskID:  diskID,
				Learner: true,
			})
		}
	} else {
		// create shard case
		tactic := s.cfg.CodeModes[0].Tactic()
		idcIndexes := tactic.GetECLayoutByAZ()
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idcIndexes), func(i, j int) {
			idcIndexes[i], idcIndexes[j] = idcIndexes[j], idcIndexes[i]
		})
		span.Debugf("idcIndexes is %#v", idcIndexes)
		// alloc disks in one diskSet
		ret, err := allocator.Alloc(ctx, policy.DiskType, s.cfg.CodeModes[0], policy.ExcludeDiskSets)
		if err != nil {
			span.Errorf("create shard alloc disks failed, err: %s", err.Error())
			return nil, nullDiskSetID, err
		}
		for idcIdx, r := range ret {
			if err := s.validateAllocRet(r.Disks); err != nil {
				return nil, nullDiskSetID, err
			}
			for diskIDIdx, suidIdx := range idcIndexes[idcIdx] {
				suid := policy.Suids[suidIdx]
				suidIndexMap[suid] = suidIdx
				suidDiskMap[suid] = r.Disks[diskIDIdx]
				units[suidIdx] = clustermgr.ShardUnit{
					Suid:    suid,
					DiskID:  r.Disks[diskIDIdx],
					Learner: false,
				}
			}
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(suidDiskMap))
	for suid, diskID := range suidDiskMap {
		disk, _ := s.getDisk(diskID)
		disk.lock.RLock()
		host := disk.info.Host
		diskSetID := disk.info.DiskSetID
		disk.lock.RUnlock()

		go func(_suid proto.Suid, _diskID proto.DiskID) {
			defer wg.Done()
			addShardArgs := shardnode.AddShardArgs{
				DiskID:       _diskID,
				Suid:         _suid,
				Range:        policy.Range,
				Units:        units,
				RouteVersion: policy.RouteVersion,
			}
			shardNodeErr := s.shardNodeClient.AddShard(ctx, host, addShardArgs)
			if shardNodeErr != nil {
				atomic.StoreUint32((*uint32)(&excludesDiskSetID), uint32(diskSetID))
				span.Errorf("alloc shard failed, diskID: %d, diskSetID: %d, host: %s, err: %v", _diskID, diskSetID, host, shardNodeErr)
				return
			}
			addShardLock.Lock()
			retDiskIDs[suidIndexMap[_suid]] = _diskID
			addShardLock.Unlock()
		}(suid, diskID)

	}
	wg.Wait()

	if excludesDiskSetID != nullDiskSetID {
		return nil, excludesDiskSetID, ErrShardNodeCreateShardFailed
	}

	// validate alloc ret again to avoid abnormal situations
	if err := s.validateAllocRet(retDiskIDs); err != nil {
		return nil, nullDiskSetID, err
	}

	return retDiskIDs, nullDiskSetID, err
}

func (s *ShardNodeManager) GetModuleName() string {
	return s.module
}

func (s *ShardNodeManager) LoadData(ctx context.Context) error {
	diskDBs, err := s.diskTbl.GetAllDisks()
	if err != nil {
		return errors.Info(err, "get all disks failed").Detail(err)
	}
	droppingDiskDBs, err := s.diskTbl.GetAllDroppingDisk()
	if err != nil {
		return errors.Info(err, "get dropping disks failed").Detail(err)
	}
	nodeDBs, err := s.nodeTbl.GetAllNodes()
	if err != nil {
		return errors.Info(err, "get all nodes failed").Detail(err)
	}
	droppingDisks := make(map[proto.DiskID]bool)
	for _, diskID := range droppingDiskDBs {
		droppingDisks[diskID] = true
	}

	allNodes := make(map[proto.NodeID]*nodeItem)
	curNodeSetID := ecNodeSetID
	curDiskSetID := ecDiskSetID
	for _, node := range nodeDBs {
		info := s.nodeInfoRecordToNodeInfo(node)
		ni := &nodeItem{
			nodeID: info.NodeID,
			info:   nodeItemInfo{NodeInfo: info.NodeInfo, extraInfo: info.ShardNodeExtraInfo},
			disks:  make(map[proto.DiskID]*diskItem),
		}
		allNodes[info.NodeID] = ni
		s.hostPathFilter.Store(ni.genFilterKey(), ni.nodeID)
		// not filter dropped node to generate nodeSet
		s.topoMgr.AddNodeToNodeSet(ni)
		if info.NodeSetID >= curNodeSetID {
			curNodeSetID = info.NodeSetID
		}
	}
	s.allNodes = allNodes

	allDisks := make(map[proto.DiskID]*diskItem)
	for _, disk := range diskDBs {
		info := s.diskInfoRecordToDiskInfo(disk)
		di := &diskItem{
			diskID:         info.DiskID,
			info:           diskItemInfo{DiskInfo: info.DiskInfo, extraInfo: &info.ShardNodeDiskHeartbeatInfo},
			weightGetter:   shardNodeDiskWeightGetter,
			weightDecrease: shardNodeDiskWeightDecrease,
			// bug fix: do not initial disk expire time, or may cause volume health change when start volume manager
			// lastExpireTime: time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second),
			// expireTime:     time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second),
		}
		if droppingDisks[di.diskID] {
			di.dropping = true
		}
		allDisks[info.DiskID] = di
		if di.needFilter() {
			s.hostPathFilter.Store(di.genFilterKey(), 1)
		}
		ni, ok := s.getNode(info.NodeID)
		if ok { // compatible case and not filter dropped disk to generate diskSet
			s.topoMgr.AddDiskToDiskSet(ni.info.DiskType, ni.info.NodeSetID, di)
			ni.disks[info.DiskID] = di
		}
		if info.DiskSetID > 0 && info.DiskSetID >= curDiskSetID {
			curDiskSetID = info.DiskSetID
		}
	}

	s.allDisks = allDisks
	s.topoMgr.SetNodeSetID(curNodeSetID)
	s.topoMgr.SetDiskSetID(curDiskSetID)

	return nil
}

func (s *ShardNodeManager) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) error {
	span := trace.SpanFromContextSafe(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(operTypes))
	errs := make([]error, len(operTypes))

	for i, t := range operTypes {
		idx := i
		_, taskCtx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[idx].ReqID)

		switch t {
		case OperTypeAddDisk:
			diskInfo := &clustermgr.ShardNodeDiskInfo{}
			err := json.Unmarshal(datas[idx], diskInfo)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			s.taskPool.Run(synchronizedDiskID, func() {
				// add disk run on fixed goroutine synchronously
				err = s.applyAddDisk(taskCtx, diskInfo)
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
			s.taskPool.Run(s.getTaskIdx(setStatusArgs.DiskID), func() {
				errs[idx] = s.SetStatus(taskCtx, setStatusArgs.DiskID, setStatusArgs.Status, true)
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
			s.taskPool.Run(s.getTaskIdx(args.DiskID), func() {
				_, errs[idx] = s.applyDroppingDisk(taskCtx, args.DiskID, true)
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
			s.taskPool.Run(s.getTaskIdx(args.DiskID), func() {
				errs[idx] = s.applyDroppedDisk(taskCtx, args.DiskID)
				wg.Done()
			})
		case OperTypeHeartbeatDiskInfo:
			args := &clustermgr.ShardNodeDisksHeartbeatArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// disk heartbeat has no necessary to run in single goroutine, so we just put it on random goroutine
			s.taskPool.Run(rand.Intn(int(s.cfg.ApplyConcurrency)), func() {
				errs[idx] = s.applyHeartBeatDiskInfo(taskCtx, args.Disks)
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
			s.taskPool.Run(s.getTaskIdx(args.DiskID), func() {
				errs[idx] = s.applySwitchReadonly(args.DiskID, args.Readonly)
				wg.Done()
			})
		case OperTypeAdminUpdateDisk:
			args := &clustermgr.ShardNodeDiskInfo{}
			err := json.Unmarshal(datas[i], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			s.taskPool.Run(s.getTaskIdx(args.DiskID), func() {
				errs[idx] = s.applyAdminUpdateDisk(ctx, args)
				wg.Done()
			})
		case OperTypeAddNode:
			args := &clustermgr.ShardNodeInfo{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// add node run on fixed goroutine synchronously
			s.taskPool.Run(s.getTaskIdx(synchronizedDiskID), func() {
				err = s.applyAddNode(taskCtx, args)
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
			s.taskPool.Run(s.getTaskIdx(synchronizedDiskID), func() {
				_, err = s.applyDroppingNode(taskCtx, args.NodeID, true)
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
func (s *ShardNodeManager) applyHeartBeatDiskInfo(ctx context.Context, infos []clustermgr.ShardNodeDiskHeartbeatInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	expireTime := time.Now().Add(time.Duration(s.cfg.HeartbeatExpireIntervalS) * time.Second)

	for i := range infos {
		info := infos[i]
		disk, ok := s.getDisk(info.DiskID)
		// sometimes, we may not find disk from allDisks
		// it was happened when disk register and heartbeat request very close
		if !ok {
			span.Warnf("disk not found in all disk, diskID: %d", info.DiskID)
			continue
		}
		// modify disk heartbeat memory info, dump into db timely
		disk.withLocked(func() error {
			heartbeatInfo := disk.info.extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo)
			heartbeatInfo.Free = info.Free
			heartbeatInfo.Size = info.Size
			heartbeatInfo.Used = info.Used
			heartbeatInfo.UsedShardCnt = info.UsedShardCnt
			// calculate free and max shard count
			heartbeatInfo.MaxShardCnt = int32(info.Size / proto.MaxShardSize)
			heartbeatInfo.FreeShardCnt = heartbeatInfo.MaxShardCnt - heartbeatInfo.UsedShardCnt
			freeShardCnt := int32(info.Free / proto.MaxShardSize)
			// use the minimum value as free shard count
			if freeShardCnt < heartbeatInfo.FreeShardCnt {
				span.Debugf("use minimum free shard count, disk id[%d], free shard[%d]", disk.diskID, freeShardCnt)
				heartbeatInfo.FreeShardCnt = freeShardCnt
			}
			if heartbeatInfo.FreeShardCnt < 0 {
				heartbeatInfo.FreeShardCnt = 0
			}

			disk.lastExpireTime = disk.expireTime
			disk.expireTime = expireTime
			return nil
		})

	}
	return nil
}

// applyAddDisk add a new disk into cluster, it return ErrDiskExist if disk already exist
func (s *ShardNodeManager) applyAddDisk(ctx context.Context, info *clustermgr.ShardNodeDiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)

	s.metaLock.Lock()
	defer s.metaLock.Unlock()

	// concurrent double check
	_, ok := s.allDisks[info.DiskID]
	if ok {
		return nil
	}

	node, ok := s.allNodes[info.NodeID]
	if !ok {
		return ErrNodeNotExist
	}
	// return err by pendingEntries
	err := node.withRLocked(func() error {
		if node.info.Status == proto.NodeStatusDropped || node.dropping {
			span.Warnf("node is dropped or dropping, disk info: %v", info)
			pendingKey := fmtApplyContextKey("disk-add", info.DiskID.ToString())
			if _, ok := s.pendingEntries.Load(pendingKey); ok {
				s.pendingEntries.Store(pendingKey, apierrors.ErrCMNodeNotFound)
			}
			return apierrors.ErrCMNodeNotFound
		}
		return nil
	})
	if err != nil {
		return nil
	}
	info.DiskSetID = s.topoMgr.AllocDiskSetID(ctx, &info.DiskInfo, &node.info.NodeInfo, s.cfg.CopySetConfigs[node.info.DiskType])

	// calculate free and max chunk count
	info.MaxShardCnt = int32(info.Size / proto.MaxShardSize)
	info.FreeShardCnt = info.MaxShardCnt - info.UsedShardCnt
	err = s.diskTbl.AddDisk(s.diskInfoToDiskInfoRecord(info))
	if err != nil {
		span.Error("ShardNodeManager.addDisk add disk failed: ", err)
		return errors.Info(err, "ShardNodeManager.addDisk add disk failed").Detail(err)
	}
	disk := &diskItem{
		diskID:         info.DiskID,
		info:           diskItemInfo{DiskInfo: info.DiskInfo, extraInfo: &info.ShardNodeDiskHeartbeatInfo},
		weightGetter:   shardNodeDiskWeightGetter,
		weightDecrease: shardNodeDiskWeightDecrease,
		expireTime:     time.Now().Add(time.Duration(s.cfg.HeartbeatExpireIntervalS) * time.Second),
	}

	node.withLocked(func() error {
		node.disks[info.DiskID] = disk
		return nil
	})
	s.topoMgr.AddDiskToDiskSet(node.info.DiskType, node.info.NodeSetID, disk)
	s.allDisks[info.DiskID] = disk
	s.hostPathFilter.Store(disk.genFilterKey(), 1)

	return nil
}

func (s *ShardNodeManager) applyAdminUpdateDisk(ctx context.Context, diskInfo *clustermgr.ShardNodeDiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	disk, ok := s.allDisks[diskInfo.DiskID]
	if !ok {
		span.Errorf("admin update shardnode disk, diskId:%d not exist", diskInfo.DiskID)
		return ErrDiskNotExist
	}

	return disk.withLocked(func() error {
		if diskInfo.Status.IsValid() {
			disk.info.Status = diskInfo.Status
		}

		heartbeatInfo := disk.info.extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo)
		if diskInfo.MaxShardCnt > 0 {
			heartbeatInfo.MaxShardCnt = diskInfo.MaxShardCnt
		}
		if diskInfo.FreeShardCnt > 0 {
			heartbeatInfo.FreeShardCnt = diskInfo.FreeShardCnt
		}
		diskRecord := s.diskInfoToDiskInfoRecord(&clustermgr.ShardNodeDiskInfo{DiskInfo: disk.info.DiskInfo, ShardNodeDiskHeartbeatInfo: *heartbeatInfo})
		return s.diskTbl.UpdateDisk(diskInfo.DiskID, diskRecord)
	})
}

func (s *ShardNodeManager) diskInfoToDiskInfoRecord(info *clustermgr.ShardNodeDiskInfo) *normaldb.ShardNodeDiskInfoRecord {
	return &normaldb.ShardNodeDiskInfoRecord{
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
		Used:         info.Used,
		Size:         info.Size,
		Free:         info.Free,
		MaxShardCnt:  info.MaxShardCnt,
		FreeShardCnt: info.FreeShardCnt,
		UsedShardCnt: info.UsedShardCnt,
	}
}

func (s *ShardNodeManager) diskInfoRecordToDiskInfo(infoDB *normaldb.ShardNodeDiskInfoRecord) *clustermgr.ShardNodeDiskInfo {
	return &clustermgr.ShardNodeDiskInfo{
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
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
			DiskID:       infoDB.DiskID,
			Used:         infoDB.Used,
			Size:         infoDB.Size,
			Free:         infoDB.Free,
			MaxShardCnt:  infoDB.MaxShardCnt,
			FreeShardCnt: infoDB.FreeShardCnt,
			UsedShardCnt: infoDB.UsedShardCnt,
		},
	}
}

func (s *ShardNodeManager) nodeInfoRecordToNodeInfo(infoDB *normaldb.ShardNodeInfoRecord) *clustermgr.ShardNodeInfo {
	return &clustermgr.ShardNodeInfo{
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
		ShardNodeExtraInfo: clustermgr.ShardNodeExtraInfo{
			RaftHost: infoDB.RaftHost,
		},
	}
}

func (s *ShardNodeManager) nodeInfoToNodeInfoRecord(info *clustermgr.ShardNodeInfo) *normaldb.ShardNodeInfoRecord {
	return &normaldb.ShardNodeInfoRecord{
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
		RaftHost: info.RaftHost,
	}
}

type shardNodePersistentHandler = ShardNodeManager

func (s *shardNodePersistentHandler) updateDiskNoLocked(di *diskItem) error {
	return s.diskTbl.UpdateDisk(di.diskID, s.diskInfoToDiskInfoRecord(&clustermgr.ShardNodeDiskInfo{
		DiskInfo:                   di.info.DiskInfo,
		ShardNodeDiskHeartbeatInfo: *di.info.extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo),
	}))
}

func (s *shardNodePersistentHandler) updateDiskStatusNoLocked(id proto.DiskID, status proto.DiskStatus) error {
	return s.diskTbl.UpdateDiskStatus(id, status)
}

func (s *shardNodePersistentHandler) addDiskNoLocked(di *diskItem) error {
	return s.diskTbl.AddDisk(s.diskInfoToDiskInfoRecord(&clustermgr.ShardNodeDiskInfo{
		DiskInfo:                   di.info.DiskInfo,
		ShardNodeDiskHeartbeatInfo: *di.info.extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo),
	}))
}

func (s *shardNodePersistentHandler) updateNodeNoLocked(n *nodeItem) error {
	return s.nodeTbl.UpdateNode(s.nodeInfoToNodeInfoRecord(&clustermgr.ShardNodeInfo{
		NodeInfo:           n.info.NodeInfo,
		ShardNodeExtraInfo: n.info.extraInfo.(clustermgr.ShardNodeExtraInfo),
	}))
}

func (s *shardNodePersistentHandler) addDroppingDisk(id proto.DiskID) error {
	return s.diskTbl.AddDroppingDisk(id)
}

func (s *shardNodePersistentHandler) addDroppingNode(id proto.NodeID) error {
	return nil
}

func (s *shardNodePersistentHandler) isDroppingDisk(id proto.DiskID) (bool, error) {
	return s.diskTbl.IsDroppingDisk(id)
}

func (b *shardNodePersistentHandler) isDroppingNode(id proto.NodeID) (bool, error) {
	return false, nil
}

func (s *shardNodePersistentHandler) droppedDisk(id proto.DiskID) error {
	return s.diskTbl.DroppedDisk(id)
}

func (s *shardNodePersistentHandler) droppedNode(id proto.NodeID) error {
	return nil
}

func shardNodeDiskWeightGetter(extraInfo interface{}) int64 {
	return int64(extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo).FreeShardCnt)
}

func shardNodeDiskWeightDecrease(extraInfo interface{}, num int64) {
	info := extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo)
	info.FreeShardCnt -= int32(num)
}
