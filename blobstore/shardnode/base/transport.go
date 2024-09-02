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

package base

import (
	"context"
	"strconv"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type (
	Transport interface {
		GetConfig(ctx context.Context, key string) (string, error)
		ShardReport(ctx context.Context, reports []clustermgr.ShardUnitInfo) ([]clustermgr.ShardTask, error)
		GetRouteUpdate(ctx context.Context, routeVersion proto.RouteVersion) (proto.RouteVersion, []clustermgr.CatalogChangeItem, error)
		NodeTransport
		SpaceTransport
		AllocVolTransport
	}

	NodeTransport interface {
		GetNode(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error)
		Register(ctx context.Context) error
		GetMyself() *clustermgr.ShardNodeInfo
		NodeID() proto.NodeID

		GetDisk(ctx context.Context, diskID proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error)
		AllocDiskID(ctx context.Context) (proto.DiskID, error)
		RegisterDisk(ctx context.Context, disk *clustermgr.ShardNodeDiskInfo) error
		SetDiskBroken(ctx context.Context, diskID proto.DiskID) error
		ListDisks(ctx context.Context) ([]clustermgr.ShardNodeDiskInfo, error)
		HeartbeatDisks(ctx context.Context, disks []clustermgr.ShardNodeDiskHeartbeatInfo) error
	}

	SpaceTransport interface {
		GetSpace(ctx context.Context, sid proto.SpaceID) (*clustermgr.Space, error)
		GetAllSpaces(ctx context.Context) ([]clustermgr.Space, error)
	}

	AllocVolTransport interface {
		AllocBid(ctx context.Context, count uint64) (proto.BlobID, error)
		AllocVolume(ctx context.Context, isInit bool, mode codemode.CodeMode, count int) (clustermgr.AllocatedVolumeInfos, error)
		RetainVolume(ctx context.Context, tokens []string) (clustermgr.RetainVolumes, error)
	}
)

func NewTransport(cmClient *clustermgr.Client, myself *clustermgr.ShardNodeInfo) Transport {
	return &transport{
		cmClient: cmClient,
		myself:   myself,
	}
}

type transport struct {
	myself   *clustermgr.ShardNodeInfo
	allNodes sync.Map
	allDisks sync.Map
	cmClient *clustermgr.Client

	singleRun singleflight.Group
}

func (t *transport) GetNode(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error) {
	v, ok := t.allNodes.Load(nodeID)
	if ok {
		return v.(*clustermgr.ShardNodeInfo), nil
	}

	v, err, _ := t.singleRun.Do(strconv.Itoa(int(nodeID)), func() (interface{}, error) {
		nodeInfo, err := t.cmClient.ShardNodeInfo(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		t.allNodes.Store(nodeID, nodeInfo)
		return nodeInfo, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*clustermgr.ShardNodeInfo), err
}

func (t *transport) GetDisk(ctx context.Context, diskID proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error) {
	v, ok := t.allDisks.Load(diskID)
	if ok {
		return v.(*clustermgr.ShardNodeDiskInfo), nil
	}

	v, err, _ := t.singleRun.Do(strconv.Itoa(int(diskID)), func() (interface{}, error) {
		diskInfo, err := t.cmClient.ShardNodeDiskInfo(ctx, diskID)
		if err != nil {
			return nil, err
		}
		t.allNodes.Store(diskID, diskInfo)
		return diskInfo, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*clustermgr.ShardNodeDiskInfo), err
}

func (t *transport) AllocDiskID(ctx context.Context) (proto.DiskID, error) {
	return t.cmClient.AllocShardNodeDiskID(ctx)
}

func (t *transport) RegisterDisk(ctx context.Context, disk *clustermgr.ShardNodeDiskInfo) error {
	return t.cmClient.AddShardNodeDisk(ctx, disk)
}

func (t *transport) SetDiskBroken(ctx context.Context, diskID proto.DiskID) error {
	return t.cmClient.SetDisk(ctx, diskID, proto.DiskStatusBroken)
}

func (t *transport) Register(ctx context.Context) error {
	nodeID, err := t.cmClient.AddShardNode(ctx, t.myself)
	if err != nil {
		return err
	}

	t.myself.NodeID = nodeID
	return nil
}

func (t *transport) GetMyself() *clustermgr.ShardNodeInfo {
	node := *t.myself
	return &node
}

func (t *transport) GetSpace(ctx context.Context, sid proto.SpaceID) (*clustermgr.Space, error) {
	v, err, _ := t.singleRun.Do(strconv.Itoa(int(sid)), func() (interface{}, error) {
		space, err := t.cmClient.GetSpaceByID(ctx, &clustermgr.GetSpaceByIDArgs{SpaceID: sid})
		if err != nil {
			return nil, err
		}
		return space, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*clustermgr.Space), nil
}

func (t *transport) GetAllSpaces(ctx context.Context) ([]clustermgr.Space, error) {
	args := &clustermgr.ListSpaceArgs{Count: uint32(10000)}
	args.Count = uint32(10000)

	spaces := make([]clustermgr.Space, 0)
	for {
		ret, err := t.cmClient.ListSpace(ctx, args)
		if err != nil {
			return nil, err
		}
		if len(ret.Spaces) < 1 {
			break
		}
		for _, s := range ret.Spaces {
			spaces = append(spaces, *s)
		}
		args.Marker = ret.Marker
	}
	return spaces, nil
}

func (t *transport) GetRouteUpdate(ctx context.Context, routeVersion proto.RouteVersion) (proto.RouteVersion, []clustermgr.CatalogChangeItem, error) {
	resp, err := t.cmClient.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{RouteVersion: routeVersion, NodeID: t.myself.NodeID})
	if err != nil {
		return 0, nil, err
	}
	return resp.RouteVersion, resp.Items, nil
}

func (t *transport) ShardReport(ctx context.Context, reports []clustermgr.ShardUnitInfo) ([]clustermgr.ShardTask, error) {
	resp, err := t.cmClient.ReportShard(ctx, &clustermgr.ShardReportArgs{
		ShardReport: clustermgr.ShardReport{
			Shards: reports,
		},
	})
	if err != nil {
		return nil, err
	}

	return resp, err
}

func (t *transport) ListDisks(ctx context.Context) ([]clustermgr.ShardNodeDiskInfo, error) {
	args := &clustermgr.ListOptionArgs{
		Host:  t.myself.Host,
		Count: 10000,
	}
	disks := make([]clustermgr.ShardNodeDiskInfo, 0)
	for {
		ret, err := t.cmClient.ListShardNodeDisk(ctx, args)
		if err != nil {
			return nil, err
		}
		if len(ret.Disks) < 1 {
			break
		}
		for _, d := range ret.Disks {
			disks = append(disks, *d)
		}
		args.Marker = ret.Marker
	}
	return disks, nil
}

func (t *transport) HeartbeatDisks(ctx context.Context, disks []clustermgr.ShardNodeDiskHeartbeatInfo) error {
	return t.cmClient.HeartbeatShardNodeDisk(ctx, disks)
}

func (t *transport) NodeID() proto.NodeID {
	return t.myself.NodeID
}

func (t *transport) GetConfig(ctx context.Context, key string) (string, error) {
	return t.cmClient.GetConfig(ctx, key)
}

func (t *transport) AllocBid(ctx context.Context, count uint64) (proto.BlobID, error) {
	ret, err := t.cmClient.AllocBid(ctx, &clustermgr.BidScopeArgs{Count: count})
	if err != nil {
		return proto.InValidBlobID, err
	}
	return ret.StartBid, nil
}

func (t *transport) AllocVolume(ctx context.Context, isInit bool, mode codemode.CodeMode, count int) (clustermgr.AllocatedVolumeInfos, error) {
	args := &clustermgr.AllocVolumeArgs{
		IsInit:   isInit,
		CodeMode: mode,
		Count:    count,
	}
	ret, err := t.cmClient.AllocVolume(ctx, args)
	if err != nil {
		return clustermgr.AllocatedVolumeInfos{}, err
	}
	return ret, nil
}

func (t *transport) RetainVolume(ctx context.Context, tokens []string) (clustermgr.RetainVolumes, error) {
	return t.cmClient.RetainVolume(ctx, &clustermgr.RetainVolumeArgs{Tokens: tokens})
}
