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

package storage

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"

	"github.com/cubefs/cubefs/blobstore/util/limit"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	sysRawFSPath = "sys"
	diskMetaFile = "Disk.meta"
)

type (
	DiskConfig struct {
		ClusterID       proto.ClusterID
		NodeID          proto.NodeID
		DiskPath        string
		CheckMountPoint bool
		StoreConfig     store.Config
		RaftConfig      raft.Config
		Transport       base.Transport
		ShardBaseConfig ShardBaseConfig
		HandleEIO       func(ctx context.Context, diskID proto.DiskID, err error)
	}
)

func OpenDisk(ctx context.Context, cfg DiskConfig) (*Disk, error) {
	span := trace.SpanFromContextSafe(ctx)

	if cfg.CheckMountPoint {
		if !store.IsMountPoint(cfg.DiskPath) {
			span.Panicf("Disk path[%s] is not mount point", cfg.DiskPath)
		}
	}

	success := false
	disk := &Disk{}

	cfg.StoreConfig.Path = cfg.DiskPath
	cfg.StoreConfig.KVOption.ColumnFamily = []kvstore.CF{dataCF}
	cfg.StoreConfig.RaftOption.ColumnFamily = []kvstore.CF{raftWalCF}
	cfg.StoreConfig.HandleEIO = func(ctx context.Context, err error) {
		cfg.HandleEIO(ctx, disk.DiskID(), err)
		disk.handleRaftError(0, err)
	}

	store, err := store.NewStore(ctx, &cfg.StoreConfig)
	if err != nil {
		span.Errorf("new store instance failed: %s", errors.Detail(err))
		return nil, err
	}

	// close store engine when open disk failed
	defer func() {
		if !success {
			store.Close()
		}
	}()

	// load Disk meta info
	stats, err := store.Stats()
	if err != nil {
		span.Errorf("stats store info failed: %s", err)
		return nil, err
	}
	diskInfo := clustermgr.ShardNodeDiskInfo{
		DiskInfo: clustermgr.DiskInfo{
			ClusterID:    cfg.ClusterID,
			NodeID:       cfg.NodeID,
			Path:         cfg.DiskPath,
			Status:       proto.DiskStatusNormal,
			CreateAt:     time.Now(),
			LastUpdateAt: time.Now(),
		},
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
			Used: stats.Used,
			Free: stats.Free,
			Size: stats.Total,
		},
	}
	rawFS := store.NewRawFS(sysRawFSPath)
	f, err := rawFS.OpenRawFile(ctx, diskMetaFile)
	if err != nil && !os.IsNotExist(err) {
		span.Errorf("open Disk meta file failed : %s", err)
		return nil, err
	}
	if err == nil {
		b, err := io.ReadAll(f)
		if err != nil {
			span.Errorf("read Disk meta file failed: %s", err)
			return nil, err
		}
		if err := diskInfo.Unmarshal(b); err != nil {
			span.Errorf("unmarshal Disk meta failed: %s, raw: %v", err, b)
			return nil, err
		}
	}

	disk.cfg = cfg
	disk.diskInfo = diskInfo
	disk.store = store
	disk.shardsMu.shards = make(map[proto.Suid]*shard)
	disk.shardsMu.shardCheck = make(map[proto.ShardID]struct{})
	disk.shardOpLimiterPerDisk = keycount.New(1)

	success = true
	return disk, nil
}

func IsEmptyDisk(path string) (bool, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}

	safePattern := `.*`
	if match, _ := regexp.MatchString(safePattern, absPath); !match {
		return false, errors.New("file path is invalid")
	}

	fis, err := os.ReadDir(absPath)
	if err != nil {
		return false, err
	}

	if len(fis) == 0 {
		return true, nil
	}

	sysInitDir := map[string]bool{
		"lost+found": true,
	}

	for _, fi := range fis {
		if !sysInitDir[fi.Name()] {
			return false, nil
		}
	}

	return true, nil
}

type Disk struct {
	diskInfo              clustermgr.ShardNodeDiskInfo
	shardOpLimiterPerDisk limit.Limiter

	shardsMu struct {
		sync.RWMutex
		shards     map[proto.Suid]*shard
		shardCheck map[proto.ShardID]struct{}
	}
	raftManager raft.Manager
	store       *store.Store
	cfg         DiskConfig

	lock               sync.RWMutex
	isRaftErrorHandled bool
}

func (d *Disk) Load(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start load disk[%d]", d.diskInfo.DiskID)
	// initial raft manager
	raftConfig := &d.cfg.RaftConfig
	raftConfig.NodeID = uint64(d.diskInfo.DiskID)
	raftConfig.Storage = &raftStorage{kvStore: d.store.RaftStore()}
	raftConfig.Logger = log.DefaultLogger
	raftConfig.ErrorHandler = d.handleRaftError
	raftManager, err := raft.NewManager(raftConfig)
	if err != nil {
		return err
	}
	d.raftManager = raftManager

	// load all shard from Disk store
	kvStore := d.store.KVStore()
	listKeyPrefix := make([]byte, len(shardInfoPrefix))
	encodeShardInfoListPrefix(listKeyPrefix)
	lr := kvStore.List(ctx, dataCF, listKeyPrefix, nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return errors.Info(err, "read next shard kv failed")
		}
		if kg == nil || vg == nil {
			break
		}

		suid := decodeShardInfoPrefix(kg.Key())
		shardInfo := &shardInfo{}
		if err = shardInfo.Unmarshal(vg.Value()); err != nil {
			span.Warnf("suid[%d] unmarshal shard info failed, err: %v", suid, err)
			kg.Close()
			vg.Close()
			return err
		}
		kg.Close()
		vg.Close()

		shard, err := newShard(ctx, shardConfig{
			suid:            suid,
			diskID:          d.diskInfo.DiskID,
			ShardBaseConfig: &d.cfg.ShardBaseConfig,
			shardInfo:       *shardInfo,
			store:           d.store,
			raftManager:     d.raftManager,
			addrResolver:    raftConfig.TransportConfig.Resolver,
			disk:            d,
		})
		if err != nil {
			span.Warnf("suid[%d] new shard failed, err: %v", suid, err)
			return err
		}

		d.shardsMu.Lock()
		d.shardsMu.shards[suid] = shard
		d.shardsMu.shardCheck[suid.ShardID()] = struct{}{}
		d.shardsMu.Unlock()

		shard.Start()
	}
	span.Infof("load disk[%d] success", d.diskInfo.DiskID)

	return nil
}

func (d *Disk) AddShard(ctx context.Context, suid proto.Suid,
	routeVersion proto.RouteVersion, rg sharding.Range, nodes []clustermgr.ShardUnit,
) error {
	span := trace.SpanFromContext(ctx)
	if err := d.shardOpLimiterPerDisk.Acquire(suid); err != nil {
		return err
	}
	defer d.shardOpLimiterPerDisk.Release(suid)

	if err := d.prepRWCheck(); err != nil {
		return err
	}

	d.shardsMu.Lock()
	defer d.shardsMu.Unlock()

	if _, ok := d.shardsMu.shards[suid]; ok {
		span.Warnf("shard[%d] already exist", suid)
		return nil
	}

	if _, ok := d.shardsMu.shardCheck[suid.ShardID()]; ok {
		span.Errorf("shard[%d] already exist", suid.ShardID())
		return apierr.ErrShardConflicts
	}

	shardUnits := make([]clustermgr.ShardUnit, len(nodes))
	for i := range nodes {
		shardUnits[i] = clustermgr.ShardUnit{
			Suid:    nodes[i].Suid,
			DiskID:  nodes[i].DiskID,
			Learner: nodes[i].Learner,
		}
	}
	shardInfo := &shardInfo{
		ShardID:      suid.ShardID(),
		Range:        rg,
		RouteVersion: routeVersion,
		Units:        shardUnits,
	}

	shard, err := newShard(ctx, shardConfig{
		ShardBaseConfig: &d.cfg.ShardBaseConfig,
		shardInfo:       *shardInfo,
		suid:            suid,
		diskID:          d.diskInfo.DiskID,
		store:           d.store,
		raftManager:     d.raftManager,
		addrResolver:    d.cfg.RaftConfig.TransportConfig.Resolver,
		disk:            d,
	})
	if err != nil {
		err = errors.Info(err, "new shard failed")
		return err
	}

	if err := shard.SaveShardInfo(ctx, false, true); err != nil {
		err = errors.Info(err, "save shard info failed")
		return err
	}

	d.shardsMu.shards[suid] = shard
	d.shardsMu.shardCheck[suid.ShardID()] = struct{}{}
	shard.Start()
	return nil
}

func (d *Disk) UpdateShard(ctx context.Context, suid proto.Suid, op proto.ShardUpdateType, node clustermgr.ShardUnit) error {
	span := trace.SpanFromContextSafe(ctx)
	if err := d.shardOpLimiterPerDisk.Acquire(suid); err != nil {
		return err
	}
	defer d.shardOpLimiterPerDisk.Release(suid)

	if err := d.prepRWCheck(); err != nil {
		return err
	}

	shard, err := d.getShard(suid)
	if err != nil {
		return err
	}

	nodeHost, err := d.cfg.RaftConfig.TransportConfig.Resolver.Resolve(ctx, uint64(node.DiskID))
	if err != nil {
		err = errors.Info(err, fmt.Sprintf("resolve shard unit[%+v] raft host failed", node))
		return err
	}

	if err = shard.UpdateShard(ctx, op, node, nodeHost.String()); err != nil {
		err = errors.Info(err, "raft group update failed")
		return err
	}

	span.Infof("shard[%d] update shard unit: %+v success, op(%d)", suid.ShardID(), node, op)
	return nil
}

func (d *Disk) GetShard(suid proto.Suid) (ShardHandler, error) {
	s, err := d.getShard(suid)
	if err != nil {
		return nil, err
	}
	if err := d.prepRWCheck(); err != nil {
		return nil, err
	}

	return s, nil
}

func (d *Disk) GetShardNoRWCheck(suid proto.Suid) (ShardHandler, error) {
	return d.getShard(suid)
}

func (d *Disk) DeleteShard(ctx context.Context, suid proto.Suid, version proto.RouteVersion) error {
	span := trace.SpanFromContextSafe(ctx)
	if err := d.shardOpLimiterPerDisk.Acquire(suid); err != nil {
		return err
	}
	defer d.shardOpLimiterPerDisk.Release(suid)

	span.Warnf("disk[%d] start delete shard[%d] suid[%d]", d.DiskID(), suid.ShardID(), suid)

	d.shardsMu.RLock()
	shard := d.shardsMu.shards[suid]
	d.shardsMu.RUnlock()

	if shard == nil {
		span.Warnf("shard already deleted: %d", suid)
		return nil
	}

	shard.UpdateShardRouteVersion(version)

	nodeHost, err := d.cfg.RaftConfig.TransportConfig.Resolver.Resolve(ctx, uint64(d.diskInfo.DiskID))
	if err != nil {
		return errors.Info(err, "resolve disk node host failed")
	}

	clearData := true
	if !d.isWritable() {
		clearData = false
	}

	if err := shard.DeleteShard(ctx, nodeHost.String(), clearData); err != nil {
		return errors.Info(err, "delete shard failed")
	}
	span.Warnf("disk[%d] shard[%d] suid[%d] is deleted", d.DiskID(), suid.ShardID(), suid)

	d.shardsMu.Lock()
	defer d.shardsMu.Unlock()

	delete(d.shardsMu.shards, suid)
	delete(d.shardsMu.shardCheck, suid.ShardID())
	span.Warnf("disk[%d] shard[%d], suid[%d] delete success", d.DiskID(), suid.ShardID(), suid)

	return nil
}

func (d *Disk) UpdateShardRouteVersion(ctx context.Context, suid proto.Suid, version proto.RouteVersion) error {
	if err := d.shardOpLimiterPerDisk.Acquire(suid); err != nil {
		return err
	}
	defer d.shardOpLimiterPerDisk.Release(suid)

	shard, err := d.getShard(suid)
	if err != nil {
		return err
	}

	shard.UpdateShardRouteVersion(version)
	return nil
}

func (d *Disk) RangeShard(f func(s ShardHandler) bool) {
	if err := d.prepRWCheck(); err != nil {
		return
	}

	d.shardsMu.RLock()
	for _, shard := range d.shardsMu.shards {
		if !f(shard) {
			break
		}
	}
	d.shardsMu.RUnlock()
}

func (d *Disk) RangeShardNoRWCheck(f func(s ShardHandler) bool) {
	d.shardsMu.RLock()
	for _, shard := range d.shardsMu.shards {
		if !f(shard) {
			break
		}
	}
	d.shardsMu.RUnlock()
}

func (d *Disk) GetDiskInfo() clustermgr.ShardNodeDiskInfo {
	d.lock.RLock()
	ret := d.diskInfo
	d.lock.RUnlock()
	return ret
}

func (d *Disk) SetDiskInfo(info clustermgr.ShardNodeDiskInfo) {
	d.lock.Lock()
	d.diskInfo = info
	d.lock.Unlock()
}

func (d *Disk) GetShardCnt() int {
	d.shardsMu.RLock()
	ret := len(d.shardsMu.shards)
	d.shardsMu.RUnlock()
	return ret
}

func (d *Disk) SaveDiskInfo(ctx context.Context) error {
	if err := d.prepRWCheck(); err != nil {
		return err
	}

	rawFS := d.store.NewRawFS(sysRawFSPath)
	f, err := rawFS.CreateRawFile(ctx, diskMetaFile)
	if err != nil {
		return err
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	b, err := d.diskInfo.Marshal()
	if err != nil {
		return err
	}

	n, err := f.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}

	return f.Close()
}

func (d *Disk) IsRegistered() bool {
	return d.diskInfo.DiskID != 0
}

func (d *Disk) DiskID() proto.DiskID {
	return d.diskInfo.DiskID
}

func (d *Disk) SetDiskID(diskID proto.DiskID) {
	d.diskInfo.DiskID = diskID
}

func (d *Disk) SetBroken() bool {
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.diskInfo.Status == proto.DiskStatusNormal {
		d.diskInfo.Status = proto.DiskStatusBroken
		return true
	}

	return false
}

func (d *Disk) DBStats(ctx context.Context, db string) (stats kvstore.Stats, err error) {
	return d.store.DBStats(ctx, db)
}

func (d *Disk) Close() {
	if d.raftManager != nil {
		d.raftManager.Close()
	}
	if d.store != nil {
		d.store.Close()
	}
	log.Infof("disk[%d] closed", d.DiskID())
}

func (d *Disk) getShard(suid proto.Suid) (*shard, error) {
	d.shardsMu.RLock()
	s := d.shardsMu.shards[suid]
	d.shardsMu.RUnlock()

	if s == nil {
		return nil, apierr.ErrShardDoesNotExist
	}
	return s, nil
}

func (d *Disk) prepRWCheck() error {
	if !d.isWritable() {
		return apierr.ErrDiskBroken
	}

	return nil
}

func (d *Disk) isWritable() bool {
	d.lock.RLock()
	status := d.diskInfo.Status
	d.lock.RUnlock()

	return status == proto.DiskStatusNormal
}

func (d *Disk) handleRaftError(groupID uint64, err error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(
		context.Background(),
		"",
		fmt.Sprintf("handleRaftError-disk[%d]-shard[%d]", d.DiskID(), groupID))
	span.Errorf("raftgroup error: %s", err.Error())

	d.lock.Lock()
	if d.isRaftErrorHandled {
		span.Infof("raftgroup error already handled")
		d.lock.Unlock()
		return
	}
	d.isRaftErrorHandled = true
	d.lock.Unlock()

	// check if shard exist
	d.shardsMu.RLock()
	_, ok := d.shardsMu.shardCheck[proto.ShardID(groupID)]
	if !ok && groupID != 0 {
		span.Warnf("handle shard[%d] not exist", groupID)
		d.shardsMu.RUnlock()
		return
	}
	d.shardsMu.RUnlock()

	if !store.IsEIO(err) {
		// todo: report to monitor if unexpect error
		span.Fatalf("unexpect raftgroup error: %s", err.Error())
	}

	go func() {
		shards := make([]*shard, 0)
		shardList := list.New()

		d.shardsMu.RLock()
		for i := range d.shardsMu.shards {
			shards = append(shards, d.shardsMu.shards[i])
			shardList.PushBack(d.shardsMu.shards[i])
		}
		d.shardsMu.RUnlock()

		start := time.Now()
		for shardList.Len() > 0 {
			e := shardList.Front()
			s := e.Value.(*shard)
			shardList.Remove(e)
			if _err := s.Stop(); _err != nil {
				span.Errorf("stop shard:%d failed: %s", s.suid.ShardID(), _err.Error())
				shardList.PushBack(s)
				continue
			}
		}
		span.Infof("all shards stop success")

		for i := range shards {
			shards[i].WaitStop()
		}
		span.Infof("all shards wait stop success, num:%d, cost: %d us", len(shards), time.Since(start).Microseconds())

		for i := range shards {
			gid := uint64(shards[i].GetSuid().ShardID())
			if _err := d.raftManager.RemoveRaftGroup(ctx, gid, false); _err != nil {
				span.Fatalf("remove raft group:%d failed: %s", gid, _err.Error())
			}
		}

		span.Infof("remove all shards raft group success, num:%d", len(shards))
	}()
}
