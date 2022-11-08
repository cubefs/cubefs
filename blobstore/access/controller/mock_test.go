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

package controller_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	idc    = "test-idc"
	region = "test_region"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	vid404      = proto.Vid(404)
	errNotFound = errors.New("not found")

	cmcli    cmapi.APIAccess
	proxycli proxy.Cacher

	dataCalled  map[proto.Vid]int
	dataNodes   map[string]cmapi.ServiceInfo
	dataVolumes map[proto.Vid]cmapi.VolumeInfo
	dataDisks   map[proto.DiskID]bnapi.DiskInfo
)

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))

	dataCalled = map[proto.Vid]int{1: 0, 9: 0}

	dataVolumes = make(map[proto.Vid]cmapi.VolumeInfo, 4)
	dataVolumes[1] = cmapi.VolumeInfo{
		VolumeInfoBase: cmapi.VolumeInfoBase{Vid: 1, CodeMode: codemode.EC6P10L2},
		Units: []cmapi.Unit{
			{Vuid: 1011, DiskID: 1021, Host: "1031"},
			{Vuid: 1012, DiskID: 1022, Host: "1032"},
		},
	}
	dataVolumes[9] = cmapi.VolumeInfo{
		VolumeInfoBase: cmapi.VolumeInfoBase{Vid: 9, CodeMode: codemode.EC16P20L2},
		Units: []cmapi.Unit{
			{Vuid: 9011, DiskID: 9021, Host: "9031"},
			{Vuid: 9012, DiskID: 9022, Host: "9032"},
		},
	}
	dataVolumes[vid404] = cmapi.VolumeInfo{VolumeInfoBase: cmapi.VolumeInfoBase{Vid: vid404}}

	dataNodes = make(map[string]cmapi.ServiceInfo)
	dataNodes[proto.ServiceNameProxy] = cmapi.ServiceInfo{
		Nodes: []cmapi.ServiceNode{
			{ClusterID: 1, Name: proto.ServiceNameProxy, Host: "proxy-1", Idc: idc},
			{ClusterID: 1, Name: proto.ServiceNameProxy, Host: "proxy-2", Idc: idc},
		},
	}

	dataDisks = make(map[proto.DiskID]bnapi.DiskInfo)
	dataDisks[10001] = bnapi.DiskInfo{
		ClusterID: 1,
		Idc:       idc,
		Host:      "blobnode-1",
		DiskHeartBeatInfo: bnapi.DiskHeartBeatInfo{
			DiskID: 10001,
		},
	}
	dataDisks[10002] = bnapi.DiskInfo{
		ClusterID: 1,
		Idc:       idc,
		Host:      "blobnode-2",
		DiskHeartBeatInfo: bnapi.DiskHeartBeatInfo{
			DiskID: 10002,
		},
	}

	cli := mocks.NewMockClientAPI(C(&testing.T{}))
	cli.EXPECT().GetConfig(A, A).AnyTimes().Return("abc", nil)
	cli.EXPECT().GetService(A, A).AnyTimes().DoAndReturn(
		func(ctx context.Context, args cmapi.GetServiceArgs) (cmapi.ServiceInfo, error) {
			if val, ok := dataNodes[args.Name]; ok {
				return val, nil
			}
			return cmapi.ServiceInfo{}, errNotFound
		})
	cli.EXPECT().ListDisk(A, A).AnyTimes().Return(cmapi.ListDiskRet{}, nil)
	cmcli = cli

	pcli := mocks.NewMockProxyClient(C(&testing.T{}))
	pcli.EXPECT().GetCacheVolume(A, A, A).AnyTimes().DoAndReturn(
		func(_ context.Context, _ string, args *proxy.CacheVolumeArgs) (*proxy.VersionVolume, error) {
			volume := new(proxy.VersionVolume)
			vid := args.Vid
			dataCalled[vid]++
			if val, ok := dataVolumes[vid]; ok {
				if vid == vid404 {
					return nil, errcode.ErrVolumeNotExist
				}
				volume.VolumeInfo = val
				volume.Version = volume.GetVersion()
				return volume, nil
			}
			return nil, errNotFound
		})
	pcli.EXPECT().GetCacheDisk(A, A, A).AnyTimes().DoAndReturn(
		func(_ context.Context, _ string, args *proxy.CacheDiskArgs) (*bnapi.DiskInfo, error) {
			if val, ok := dataDisks[args.DiskID]; ok {
				return &val, nil
			}
			return nil, errNotFound
		})
	proxycli = pcli

	initCluster()
}
