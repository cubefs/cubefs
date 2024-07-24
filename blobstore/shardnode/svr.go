package shardnode

import (
	"sync"

	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog"
	"github.com/cubefs/cubefs/blobstore/util/closer"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
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
}

type service struct {
	catalog   *catalog.Catalog
	disks     map[proto.DiskID]*storage.Disk
	transport *base.Transport

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
