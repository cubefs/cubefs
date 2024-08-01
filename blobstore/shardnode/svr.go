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

// nolint
package shardnode

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

type Config struct {
	ClusterID   proto.ClusterID
	DisksConfig struct {
		Disks           []string `json:"disks"`
		CheckMountPoint bool     `json:"check_mount_point"`
	} `json:"disks_config"`
	StoreConfig     store.Config             `json:"store_config"`
	CmConfig        clustermgr.Config        `json:"cm_config"`
	NodeConfig      clustermgr.ShardNodeInfo `json:"node_config"`
	RaftConfig      raft.Config              `json:"raft_config"`
	ShardBaseConfig storage.ShardBaseConfig  `json:"shard_base_config"`
	HandleIOError   func(ctx context.Context)
}

func newService() *service {
	return nil
}

type service struct {
	catalog   *catalog.Catalog
	disks     map[proto.DiskID]*storage.Disk
	transport base.Transport
	taskPool  taskpool.TaskPool
	groupRun  singleflight.Group

	cfg    Config
	lock   sync.RWMutex
	closer closer.Closer
}

func (s *service) getDisk(diskID proto.DiskID) (*storage.Disk, error) {
	s.lock.RLock()
	disk := s.disks[diskID]
	s.lock.RUnlock()
	if disk == nil {
		return nil, apierr.ErrShardNodeDiskNotFound
	}
	return disk, nil
}

func (s *service) addDisk(disk *storage.Disk) {
	s.lock.Lock()
	s.disks[disk.DiskID()] = disk
	s.lock.Unlock()
}

func (s *service) getAllDisks() []*storage.Disk {
	s.lock.RLock()
	disks := make([]*storage.Disk, 0, len(s.disks))
	for i := range s.disks {
		disks = append(disks, s.disks[i])
	}
	s.lock.RUnlock()

	return disks
}
