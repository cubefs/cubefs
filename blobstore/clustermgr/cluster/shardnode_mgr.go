package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/defaulter"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	shardNodeDiskIDScopeName = "sn-diskid"
	shardNodeIDScopeName     = "sn-nodeid"
)

type ShardNodeManagerAPI interface {
	// GetNodeInfo return node info with specified node id, it return ErrCMNodeNotFound if node not found
	GetNodeInfo(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error)
	// GetDiskInfo return disk info, it return ErrDiskNotFound if disk not found
	GetDiskInfo(ctx context.Context, id proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error)
	// ListDroppingDisk return all dropping disk info
	ListDroppingDisk(ctx context.Context) ([]*clustermgr.ShardNodeDiskInfo, error)
	// ListDiskInfo return disk list with list option
	ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (disks []*clustermgr.ShardNodeDiskInfo, marker proto.DiskID, err error)
	// AllocShards return available disk with specified alloc policy
	AllocShards(ctx context.Context, policy AllocShardsPolicy) ([]proto.DiskID, error)

	NodeManagerAPI
	persistentHandler
}

func NewShardNodeMgr(scopeMgr scopemgr.ScopeMgrAPI, db *normaldb.NormalDB, cfg DiskMgrConfig) (*ShardNodeManager, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewDiskMgr")

	cfg.NodeIDScopeName = shardNodeIDScopeName
	cfg.DiskIDScopeName = shardNodeDiskIDScopeName
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

	diskTbl, err := normaldb.OpenShardNodeDiskTable(db, cfg.EnsureIndex)
	if err != nil {
		return nil, errors.Info(err, "open disk table failed").Detail(err)
	}

	droppedDiskTbl, err := normaldb.OpenShardNodeDroppedDiskTable(db)
	if err != nil {
		return nil, errors.Info(err, "open disk drop table failed").Detail(err)
	}

	nodeTbl, err := normaldb.OpenShardNodeTable(db)
	if err != nil {
		return nil, errors.Info(err, "open node table failed").Detail(err)
	}

	bm := &ShardNodeManager{
		diskTbl:        diskTbl,
		nodeTbl:        nodeTbl,
		droppedDiskTbl: droppedDiskTbl,
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
			case <-bm.closeCh:
				return
			}
		}
	}()

	return bm, nil
}

type AllocShardsPolicy struct{}

type ShardNodeManager struct {
	*manager

	diskTbl        *normaldb.ShardNodeDiskTable
	nodeTbl        *normaldb.ShardNodeTable
	droppedDiskTbl *normaldb.DroppedDiskTable
}

func (s *ShardNodeManager) GetDiskInfo(ctx context.Context, id proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error) {
	return nil, nil
}

func (s *ShardNodeManager) ListDroppingDisk(ctx context.Context) ([]*clustermgr.ShardNodeDiskInfo, error) {
	return nil, nil
}

// ListDiskInfo return disk info with specified query condition
func (s *ShardNodeManager) ListDiskInfo(ctx context.Context, opt *clustermgr.ListOptionArgs) (disks []*clustermgr.ShardNodeDiskInfo, marker proto.DiskID, err error) {
	return
}

func (s *ShardNodeManager) AddDisk(ctx context.Context, args *clustermgr.ShardNodeDiskInfo) error {
	return nil
}

func (s *ShardNodeManager) GetNodeInfo(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error) {
	return nil, nil
}

func (s *ShardNodeManager) AllocShards(ctx context.Context, policy AllocShardsPolicy) ([]proto.DiskID, error) {
	return nil, nil
}

func (s *ShardNodeManager) LoadData(ctx context.Context) error {
	diskDBs, err := s.diskTbl.GetAllDisks()
	if err != nil {
		return errors.Info(err, "get all disks failed").Detail(err)
	}
	droppingDiskDBs, err := s.droppedDiskTbl.GetAllDroppingDisk()
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
			info:   nodeItemInfo{NodeInfo: info.NodeInfo},
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
				errs[idx] = s.applyDroppingDisk(taskCtx, args.DiskID)
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
		case OperTypeDropNode:
			args := &clustermgr.NodeInfoArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// drop node run on fixed goroutine synchronously
			s.taskPool.Run(s.getTaskIdx(synchronizedDiskID), func() {
				err = s.applyDropNode(taskCtx, args)
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
func (s *ShardNodeManager) applyHeartBeatDiskInfo(ctx context.Context, infos []*clustermgr.ShardNodeDiskHeartbeatInfo) error {
	return nil
}

// applyAddDisk add a new disk into cluster, it return ErrDiskExist if disk already exist
func (s *ShardNodeManager) applyAddDisk(ctx context.Context, info *clustermgr.ShardNodeDiskInfo) error {
	return nil
}

// applyAddNode add a new node into cluster, it returns ErrNodeExist if node already exist
func (s *ShardNodeManager) applyAddNode(ctx context.Context, info *clustermgr.ShardNodeInfo) error {
	return nil
}

func (s *ShardNodeManager) applyAdminUpdateDisk(ctx context.Context, diskInfo *clustermgr.ShardNodeDiskInfo) error {
	return nil
}

func (s *ShardNodeManager) refresh(ctx context.Context) {
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
		NodeInfo: n.info.NodeInfo,
	}))
}

func (s *shardNodePersistentHandler) addDroppingDisk(id proto.DiskID) error {
	return s.droppedDiskTbl.AddDroppingDisk(id)
}

func (s *shardNodePersistentHandler) isDroppingDisk(id proto.DiskID) (bool, error) {
	return s.droppedDiskTbl.IsDroppingDisk(id)
}

func (s *shardNodePersistentHandler) droppedDisk(id proto.DiskID) error {
	return s.droppedDiskTbl.DroppedDisk(id)
}

func shardNodeDiskWeightGetter(extraInfo interface{}) int64 {
	info := extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo)
	return info.Free / (info.Used / int64(info.UsedShardCnt))
}

func shardNodeDiskWeightDecrease(extraInfo interface{}, num int64) {
	info := extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo)
	info.UsedShardCnt += int32(num)
}
