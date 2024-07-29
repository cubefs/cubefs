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
	"context"
	"io"
	"os"
	"sync"

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
	}
)

func OpenDisk(ctx context.Context, cfg DiskConfig) *Disk {
	span := trace.SpanFromContext(ctx)

	if cfg.CheckMountPoint {
		if !store.IsMountPoint(cfg.DiskPath) {
			span.Panicf("Disk path[%s] is not mount point", cfg.DiskPath)
		}
	}

	disk := &Disk{}

	cfg.StoreConfig.Path = cfg.DiskPath
	cfg.StoreConfig.KVOption.ColumnFamily = []kvstore.CF{dataCF}
	cfg.StoreConfig.RaftOption.ColumnFamily = []kvstore.CF{raftWalCF}
	cfg.StoreConfig.HandleEIO = func(err error) {
		span.Warnf("handle eio from store layer: %s", err)
		if err := cfg.Transport.SetDiskBroken(ctx, disk.diskInfo.DiskID); err != nil {
			span.Errorf("set Disk[%d] broken failed", disk.diskInfo.DiskID)
		}
	}

	store, err := store.NewStore(ctx, &cfg.StoreConfig)
	if err != nil {
		span.Panicf("new store instance failed: %s", errors.Detail(err))
	}

	// load Disk meta info
	stats, err := store.Stats()
	if err != nil {
		span.Panicf("stats store info failed: %s", err)
	}
	diskInfo := clustermgr.ShardNodeDiskInfo{
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: cfg.ClusterID,
			NodeID:    cfg.NodeID,
			Path:      cfg.DiskPath,
		},
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
			Used: stats.Used,
			Free: stats.Free,
			Size: stats.Total,
		},
	}
	rawFS := store.NewRawFS(sysRawFSPath)
	f, err := rawFS.OpenRawFile(diskMetaFile)
	if err != nil && !os.IsNotExist(err) {
		span.Panicf("open Disk meta file failed : %s", err)
	}
	if err == nil {
		b, err := io.ReadAll(f)
		if err != nil {
			span.Panicf("read Disk meta file failed: %s", err)
		}
		if err := diskInfo.Unmarshal(b); err != nil {
			span.Panicf("unmarshal Disk meta failed: %s, raw: %v", err, b)
		}
	}

	disk.cfg = cfg
	disk.diskInfo = diskInfo
	disk.store = store
	disk.shardsMu.shards = make(map[proto.Suid]*shard)

	return disk
}

type Disk struct {
	diskInfo clustermgr.ShardNodeDiskInfo

	shardsMu struct {
		sync.RWMutex
		// shard id as the map key
		shards map[proto.Suid]*shard
	}
	raftManager raft.Manager
	store       *store.Store
	cfg         DiskConfig

	lock sync.RWMutex
}

func (d *Disk) Load(ctx context.Context) error {
	// initial raft manager
	raftConfig := d.cfg.RaftConfig
	raftConfig.NodeID = uint64(d.diskInfo.DiskID)
	raftConfig.Storage = &raftStorage{kvStore: d.store.RaftStore()}
	raftConfig.Logger = log.DefaultLogger
	raftConfig.Resolver = &addressResolver{t: d.cfg.Transport}
	raftManager, err := raft.NewManager(&raftConfig)
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
			return errors.Info(err, "unmarshal shard info failed")
		}

		shard, err := newShard(ctx, shardConfig{
			suid:            suid,
			diskID:          d.diskInfo.DiskID,
			ShardBaseConfig: &d.cfg.ShardBaseConfig,
			shardInfo:       *shardInfo,
			store:           d.store,
			raftManager:     d.raftManager,
			addrResolver:    raftConfig.Resolver.(*addressResolver),
		})
		if err != nil {
			return errors.Info(err, "new shard failed")
		}

		d.shardsMu.Lock()
		d.shardsMu.shards[suid] = shard
		d.shardsMu.Unlock()

		shard.Start()
	}

	return nil
}

func (d *Disk) AddShard(ctx context.Context, suid proto.Suid,
	epoch uint64, rg sharding.Range, nodes []clustermgr.ShardUnit,
) error {
	span := trace.SpanFromContext(ctx)

	d.shardsMu.Lock()
	defer d.shardsMu.Unlock()

	if _, ok := d.shardsMu.shards[suid]; ok {
		span.Warnf("shard[%d] already exist", suid)
		return nil
	}

	shardUnits := make([]clustermgr.ShardUnit, len(nodes))
	for i := range nodes {
		shardUnits[i] = clustermgr.ShardUnit{
			DiskID:  nodes[i].DiskID,
			Learner: nodes[i].Learner,
		}
	}
	shardInfo := &shardInfo{
		ShardID: suid.ShardID(),
		Range:   rg,
		Epoch:   epoch,
		Units:   shardUnits,
	}

	shard, err := newShard(ctx, shardConfig{
		ShardBaseConfig: &d.cfg.ShardBaseConfig,
		shardInfo:       *shardInfo,
		diskID:          d.diskInfo.DiskID,
		store:           d.store,
		raftManager:     d.raftManager,
		addrResolver:    d.cfg.RaftConfig.Resolver,
	})
	if err != nil {
		return err
	}

	if err := shard.SaveShardInfo(ctx, false, false); err != nil {
		return err
	}

	d.shardsMu.shards[suid] = shard
	shard.Start()
	return nil
}

func (d *Disk) UpdateShard(ctx context.Context, suid proto.Suid, op proto.ShardUpdateType, node clustermgr.ShardUnit) error {
	shard, err := d.GetShard(suid)
	if err != nil {
		return err
	}

	nodeHost, err := d.cfg.RaftConfig.Resolver.Resolve(ctx, uint64(node.DiskID))
	if err != nil {
		return err
	}

	return shard.UpdateShard(ctx, op, node, nodeHost.String())
}

func (d *Disk) GetShard(suid proto.Suid) (*shard, error) {
	d.shardsMu.RLock()
	s := d.shardsMu.shards[suid]
	d.shardsMu.RUnlock()

	if s == nil {
		return nil, apierr.ErrShardDoesNotExist
	}
	return s, nil
}

func (d *Disk) DeleteShard(ctx context.Context, suid proto.Suid) error {
	d.shardsMu.RLock()
	shard := d.shardsMu.shards[suid]
	d.shardsMu.RUnlock()

	if shard == nil {
		return nil
	}

	d.shardsMu.Lock()
	defer d.shardsMu.Unlock()

	if shard == nil {
		return nil
	}

	nodeHost, err := d.cfg.RaftConfig.Resolver.Resolve(ctx, uint64(d.diskInfo.DiskID))
	if err != nil {
		return errors.Info(err, "resolve disk node host failed")
	}

	if err := shard.DeleteShard(ctx, nodeHost.String()); err != nil {
		return errors.Info(err, "delete shard failed")
	}

	// remove raft group
	if err := d.raftManager.RemoveRaftGroup(ctx, uint64(suid.ShardID()), true); err != nil {
		return errors.Info(err, "remove raft group failed")
	}

	delete(d.shardsMu.shards, suid)

	return nil
}

func (d *Disk) RangeShard(f func(s ShardHandler) bool) {
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

func (d *Disk) GetShardCnt() int {
	d.shardsMu.RLock()
	ret := len(d.shardsMu.shards)
	d.shardsMu.RUnlock()
	return ret
}

func (d *Disk) SaveDiskInfo() error {
	rawFS := d.store.NewRawFS(sysRawFSPath)
	f, err := rawFS.CreateRawFile(diskMetaFile)
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
