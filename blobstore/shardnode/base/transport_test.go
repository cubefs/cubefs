// Copyright 2025 The CubeFS Authors.
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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

var ctr = gomock.NewController

func TestTransport_GetConfig(t *testing.T) {
	key := "key"
	value := "value"

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, ret *string) error {
		*ret = value
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	_value, err := tp.GetConfig(context.Background(), key)
	require.Nil(t, err)
	require.Equal(t, value, _value)
}

func TestTransport_ShardReport(t *testing.T) {
	task1 := clustermgr.ShardTask{DiskID: 1, Suid: proto.Suid(123)}
	task2 := clustermgr.ShardTask{DiskID: 1, Suid: proto.Suid(456)}
	ret := clustermgr.ShardReportRet{ShardTasks: []clustermgr.ShardTask{task1, task2}}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().PostWith(any, any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ShardReportRet, arg *clustermgr.ShardReportArgs, option ...interface{}) error {
		*info = ret
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	tasks, err := tp.ShardReport(context.Background(), []clustermgr.ShardUnitInfo{{Suid: 123}, {Suid: 456}})
	require.Nil(t, err)
	require.Equal(t, 2, len(tasks))
}

func TestTransport_GetNode(t *testing.T) {
	nodeID := proto.NodeID(1)
	nodeInfo := &clustermgr.ShardNodeInfo{
		NodeInfo: clustermgr.NodeInfo{NodeID: nodeID},
	}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ShardNodeInfo) error {
		*info = *nodeInfo
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	// request
	_info, err := tp.GetNode(context.Background(), nodeID)
	require.Nil(t, err)
	require.Equal(t, nodeInfo, _info)

	// get cache
	_info, err = tp.GetNode(context.Background(), nodeID)
	require.Nil(t, err)
	require.Equal(t, nodeInfo, _info)
}

func TestTransport_Register(t *testing.T) {
	nodeID := proto.NodeID(1)
	myself := clustermgr.ShardNodeInfo{
		NodeInfo: clustermgr.NodeInfo{},
	}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().PostWith(any, any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.NodeIDAllocRet, arg *clustermgr.ShardNodeInfo, option ...interface{}) error {
		info.NodeID = nodeID
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
		Self: &myself,
	})

	err := tp.Register(context.Background())
	require.Nil(t, err)
	require.Equal(t, tp.GetMyself().NodeID, nodeID)
}

func TestTransport_GetDisk(t *testing.T) {
	diskID := proto.DiskID(1)
	diskInfo := &clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: diskID},
	}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ShardNodeDiskInfo) error {
		*info = *diskInfo
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	// request
	_info, err := tp.GetDisk(context.Background(), diskID, false)
	require.Nil(t, err)
	require.Equal(t, diskInfo, _info)

	// use cache
	_info, err = tp.GetDisk(context.Background(), diskID, true)
	require.Nil(t, err)
	require.Equal(t, diskInfo, _info)
}

func TestTransport_ListDisk(t *testing.T) {
	diskInfo1 := &clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: 1},
	}
	diskInfo2 := &clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: 2},
	}
	listInfo := clustermgr.ListShardNodeDiskRet{
		Disks:  []*clustermgr.ShardNodeDiskInfo{diskInfo1, diskInfo2},
		Marker: 0,
	}
	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ListShardNodeDiskRet) error {
		*info = listInfo
		return nil
	})
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ListShardNodeDiskRet) error {
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
		Self: &clustermgr.ShardNodeInfo{},
	})

	disks, err := tp.ListDisks(context.Background())
	require.Nil(t, err)
	require.Equal(t, 2, len(disks))
}

func TestTransport_GetSpace(t *testing.T) {
	spaceID := proto.SpaceID(1)
	space := clustermgr.Space{SpaceID: spaceID}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.Space) error {
		*info = space
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	_space, err := tp.GetSpace(context.Background(), spaceID)
	require.Nil(t, err)
	require.Equal(t, space.SpaceID, _space.SpaceID)
}

func TestTransport_GetAllSpaces(t *testing.T) {
	sp1 := &clustermgr.Space{SpaceID: 1}
	sp2 := &clustermgr.Space{SpaceID: 2}
	listInfo := clustermgr.ListSpaceRet{
		Spaces: []*clustermgr.Space{sp1, sp2},
		Marker: 0,
	}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ListSpaceRet) error {
		*info = listInfo
		return nil
	})
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ListSpaceRet) error {
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	spaces, err := tp.GetAllSpaces(context.Background())
	require.Nil(t, err)
	require.Equal(t, 2, len(spaces))
}

func TestTransport_ListVolume(t *testing.T) {
	mode := codemode.EC3P3
	units := make([]clustermgr.Unit, mode.GetShardNum())
	for i := 0; i < mode.GetShardNum(); i++ {
		units[i] = clustermgr.Unit{
			DiskID: proto.DiskID(i),
		}
	}

	vid := proto.Vid(1)
	volInfo := &clustermgr.VolumeInfo{
		Units:          units,
		VolumeInfoBase: clustermgr.VolumeInfoBase{Vid: vid, CodeMode: codemode.EC3P3},
	}
	listInfo := clustermgr.ListVolumes{
		Volumes: []*clustermgr.VolumeInfo{volInfo},
	}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ListVolumes) error {
		*info = listInfo
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	_listInfo, _vid, err := tp.ListVolume(context.Background(), defaultMarker, 1)
	require.Nil(t, err)
	require.Equal(t, defaultMarker, _vid)
	require.Equal(t, vid, _listInfo[0].Vid)
	require.Equal(t, mode.GetShardNum(), len(_listInfo[0].VunitLocations))
}

func TestTransport_GetVolume(t *testing.T) {
	mode := codemode.EC3P3
	units := make([]clustermgr.Unit, mode.GetShardNum())
	for i := 0; i < mode.GetShardNum(); i++ {
		units[i] = clustermgr.Unit{
			DiskID: proto.DiskID(i),
		}
	}

	vid := proto.Vid(1)
	volInfo := &clustermgr.VolumeInfo{
		Units:          units,
		VolumeInfoBase: clustermgr.VolumeInfoBase{Vid: vid, CodeMode: codemode.EC3P3},
	}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.VolumeInfo) error {
		*info = *volInfo
		return nil
	})

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	simpleInfo, err := tp.GetVolumeInfo(context.Background(), proto.Vid(1))
	require.Nil(t, err)
	require.Equal(t, vid, simpleInfo.Vid)
	require.Equal(t, mode.GetShardNum(), len(simpleInfo.VunitLocations))
}

func TestTransport_GetRepairedDisk(t *testing.T) {
	diskID := proto.DiskID(1)
	diskInfo := &clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: diskID},
		DiskInfo: clustermgr.DiskInfo{
			Status: proto.DiskStatusRepaired,
		},
	}

	mockCli := mocks.NewMockRPCClient(ctr(t))
	mockCli.EXPECT().GetWith(any, any, any).DoAndReturn(func(_ context.Context, _ string, info *clustermgr.ShardNodeDiskInfo) error {
		*info = *diskInfo
		return nil
	}).Times(1)

	tp := NewTransport(TransportConfig{
		CMClient: &clustermgr.Client{
			Client: mockCli,
		},
	})

	repaired, err := tp.IsRepairedDisk(context.Background(), diskID)
	require.Nil(t, err)
	require.True(t, repaired)

	repaired, err = tp.IsRepairedDisk(context.Background(), diskID)
	require.Nil(t, err)
	require.True(t, repaired)
}
