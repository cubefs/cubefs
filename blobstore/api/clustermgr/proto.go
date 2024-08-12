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

package clustermgr

import (
	"context"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	ConsulRegisterPath         = "ebs/%s/clusters/"
	RaftSnapshotIndexHeaderKey = "Raft-Snapshot-Index"
	RaftSnapshotNameHeaderKey  = "Raft-Snapshot-Name"
)

type ClusterInfo struct {
	Region    string          `json:"region"`
	ClusterID proto.ClusterID `json:"cluster_id"`
	Capacity  int64           `json:"capacity"`
	Available int64           `json:"available"`
	Readonly  bool            `json:"readonly"`
	Nodes     []string        `json:"nodes"`
}

type StatInfo struct {
	LeaderHost         string         `json:"leader_host"`
	ReadOnly           bool           `json:"read_only"`
	RaftStatus         interface{}    `json:"raft_status"`
	BlobNodeSpaceStat  SpaceStatInfo  `json:"space_stat"`
	ShardNodeSpaceStat SpaceStatInfo  `json:"shard_node_space_stat"`
	VolumeStat         VolumeStatInfo `json:"volume_stat"`
}

func GetConsulClusterPath(region string) string {
	return fmt.Sprintf(ConsulRegisterPath, region)
}

// ClientAPI all interface of cluster manager
type ClientAPI interface {
	APIAccess
	APIProxy
}

// APIAccess sub of cluster manager api for access
type APIAccess interface {
	GetConfig(ctx context.Context, key string) (string, error)
	GetService(ctx context.Context, args GetServiceArgs) (ServiceInfo, error)
	ListDisk(ctx context.Context, options *ListOptionArgs) (ListDiskRet, error)
	AuthSpace(ctx context.Context, args *AuthSpaceArgs) (err error)
	GetSpaceByName(ctx context.Context, args *GetSpaceByNameArgs) (ret *Space, err error)
	GetCatalogChanges(ctx context.Context, args *GetCatalogChangesArgs) (ret *GetCatalogChangesRet, err error)
	ShardNodeDiskInfo(ctx context.Context, id proto.DiskID) (ret *ShardNodeDiskInfo, err error)
	ListShardNodeDisk(ctx context.Context, options *ListOptionArgs) (ret ListShardNodeDiskRet, err error)
}

// APIProxy sub of cluster manager api for allocator
type APIProxy interface {
	GetConfig(ctx context.Context, key string) (string, error)
	GetVolumeInfo(ctx context.Context, args *GetVolumeArgs) (*VolumeInfo, error)
	DiskInfo(ctx context.Context, id proto.DiskID) (*BlobNodeDiskInfo, error)
	AllocVolume(ctx context.Context, args *AllocVolumeArgs) (AllocatedVolumeInfos, error)
	AllocBid(ctx context.Context, args *BidScopeArgs) (*BidScopeRet, error)
	RetainVolume(ctx context.Context, args *RetainVolumeArgs) (RetainVolumes, error)
	RegisterService(ctx context.Context, node ServiceNode, tickInterval, heartbeatTicks, expiresTicks uint32) error
}

// APIService sub of cluster manager api for service
type APIService interface {
	GetService(ctx context.Context, args GetServiceArgs) (ServiceInfo, error)
}
