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

package scheduler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

var (
	leaderServer   *httptest.Server
	followerServer *httptest.Server
)

func runMockLeaderService(s *Service) string {
	router := rpc.New()
	router.Handle(http.MethodGet, api.PathStats, s.HTTPStats, rpc.OptArgsQuery())
	router.Handle(http.MethodGet, api.PathStatsLeader, s.HTTPStats, rpc.OptArgsQuery())
	router.Handle(http.MethodPost, api.PathUpdateVolume, s.HTTPUpdateVolume, rpc.OptArgsBody())
	router.Handle(http.MethodGet, api.PathTaskAcquire, s.HTTPTaskAcquire, rpc.OptArgsQuery())

	leaderServer = httptest.NewServer(rpc.MiddlewareHandlerWith(router, s))
	return leaderServer.URL
}

func runMockFollowerService(s *Service) string {
	router := rpc.New()
	router.Handle(http.MethodGet, api.PathStats, s.HTTPStats, rpc.OptArgsQuery())
	router.Handle(http.MethodPost, api.PathUpdateVolume, s.HTTPUpdateVolume, rpc.OptArgsBody())

	followerServer = httptest.NewServer(rpc.MiddlewareHandlerWith(router, s))
	return followerServer.URL
}

func TestNewService(t *testing.T) {
	testCases := []struct {
		conf *Config
		err  error
	}{
		{
			conf: &Config{},
			err:  errIllegalClusterID,
		},
		{
			conf: &Config{ClusterID: proto.ClusterID(1)},
			err:  errInvalidMembers,
		},
		{
			conf: &Config{
				ClusterID: proto.ClusterID(1),
				Services:  Services{Members: map[uint64]string{1: "127.0.0.1:9800"}},
			},
			err: errInvalidLeader,
		},
		{
			conf: &Config{
				ClusterID: proto.ClusterID(1),
				Services:  Services{Leader: 1, Members: map[uint64]string{1: "127.0.0.1:9800"}},
			},
			err: errInvalidNodeID,
		},
	}
	for _, tc := range testCases {
		_, err := NewService(tc.conf)
		require.Equal(t, true, errors.Is(err, tc.err))
	}
}

func newMockServiceWithOpts(ctr *gomock.Controller, isLeader bool) *Service {
	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	blobDeleteMgr := NewMockTaskRunner(ctr)
	shardRepairMgr := NewMockTaskRunner(ctr)
	diskDropMgr := NewMockMigrater(ctr)
	diskRepairMgr := NewMockMigrater(ctr)
	manualMgr := NewMockMigrater(ctr)
	balanceMgr := NewMockMigrater(ctr)
	inspecterMgr := NewMockVolumeInspector(ctr)
	volumeCache := NewMockVolumeCache(ctr)
	volumeUpdater := NewMockVolumeUpdater(ctr)

	balanceMgr.EXPECT().Close().AnyTimes().Return()
	diskRepairMgr.EXPECT().Close().AnyTimes().Return()
	diskDropMgr.EXPECT().Close().AnyTimes().Return()
	manualMgr.EXPECT().Close().AnyTimes().Return()
	inspecterMgr.EXPECT().Close().AnyTimes().Return()

	balanceMgr.EXPECT().Run().AnyTimes().Return()
	diskDropMgr.EXPECT().Run().AnyTimes().Return()
	diskRepairMgr.EXPECT().Run().AnyTimes().Return()
	inspecterMgr.EXPECT().Run().AnyTimes().Return()
	manualMgr.EXPECT().Run().AnyTimes().Return()

	volumeCache.EXPECT().Load().AnyTimes().Return(nil)
	shardRepairMgr.EXPECT().RunTask().AnyTimes().Return()
	blobDeleteMgr.EXPECT().RunTask().AnyTimes().Return()

	balanceMgr.EXPECT().Load().AnyTimes().Return(nil)
	diskRepairMgr.EXPECT().Load().AnyTimes().Return(nil)
	diskDropMgr.EXPECT().Load().AnyTimes().Return(nil)
	manualMgr.EXPECT().Load().AnyTimes().Return(nil)

	blobDeleteMgr.EXPECT().GetErrorStats().AnyTimes().Return([]string{}, uint64(0))
	blobDeleteMgr.EXPECT().GetTaskStats().AnyTimes().Return([counter.SLOT]int{}, [counter.SLOT]int{})
	blobDeleteMgr.EXPECT().Enabled().AnyTimes().Return(true)
	shardRepairMgr.EXPECT().GetErrorStats().AnyTimes().Return([]string{}, uint64(0))
	shardRepairMgr.EXPECT().GetTaskStats().AnyTimes().Return([counter.SLOT]int{}, [counter.SLOT]int{})
	shardRepairMgr.EXPECT().Enabled().AnyTimes().Return(true)
	diskRepairMgr.EXPECT().Stats().AnyTimes().Return(api.MigrateTasksStat{})
	diskRepairMgr.EXPECT().Progress(any).AnyTimes().Return([]proto.DiskID{proto.DiskID(1)}, 0, 0)
	diskRepairMgr.EXPECT().Enabled().AnyTimes().Return(true)
	diskDropMgr.EXPECT().Stats().AnyTimes().Return(api.MigrateTasksStat{})
	diskDropMgr.EXPECT().Progress(any).AnyTimes().Return([]proto.DiskID{proto.DiskID(1)}, 0, 0)
	diskDropMgr.EXPECT().Enabled().AnyTimes().Return(true)
	balanceMgr.EXPECT().Stats().AnyTimes().Return(api.MigrateTasksStat{})
	balanceMgr.EXPECT().Enabled().AnyTimes().Return(true)
	manualMgr.EXPECT().Stats().AnyTimes().Return(api.MigrateTasksStat{})
	inspecterMgr.EXPECT().GetTaskStats().AnyTimes().Return([counter.SLOT]int{}, [counter.SLOT]int{})
	inspecterMgr.EXPECT().Enabled().AnyTimes().Return(true)

	volumeUpdater.EXPECT().UpdateFollowerVolumeCache(any, any, any).AnyTimes().Return(nil)
	volumeUpdater.EXPECT().UpdateLeaderVolumeCache(any, any).AnyTimes().Return(nil)

	manualMgr.EXPECT().AcquireTask(any, any).AnyTimes().Return(proto.MigrateTask{TaskType: proto.TaskTypeManualMigrate}, nil)

	volumeCache.EXPECT().Update(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)

	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", errMock)
	service := &Service{
		ClusterID:      1,
		leader:         isLeader,
		balanceMgr:     balanceMgr,
		diskDropMgr:    diskDropMgr,
		manualMigMgr:   manualMgr,
		diskRepairMgr:  diskRepairMgr,
		inspectMgr:     inspecterMgr,
		shardRepairMgr: shardRepairMgr,
		blobDeleteMgr:  blobDeleteMgr,
		volCache:       volumeCache,
		volumeUpdater:  volumeUpdater,

		clusterMgrCli: clusterMgrCli,
	}
	return service
}

func TestServer(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)

	leaderServer := newMockServiceWithOpts(ctr, true)
	defer leaderServer.Close()

	leaderHost := runMockLeaderService(leaderServer)

	followerServer := newMockServiceWithOpts(ctr, false)
	defer followerServer.Close()

	followerHost := runMockFollowerService(followerServer)

	leaderServer.leaderHost = leaderHost[len(scheme):]
	followerServer.leaderHost = leaderHost[len(scheme):]
	leaderServer.followerHosts = []string{followerHost[len(scheme):]}
	followerServer.followerHosts = []string{followerHost[len(scheme):]}

	go leaderServer.Run()
	go followerServer.Run()

	go leaderServer.RunTask()
	go followerServer.RunTask()

	kafkaOffset := NewMockClusterMgrAPI(ctr)
	kafkaOffset.EXPECT().GetConsumeOffset(any, any, any).AnyTimes().Return(int64(0), nil)
	kafkaOffset.EXPECT().SetConsumeOffset(any, any, any, any).AnyTimes().Return(nil)

	err := leaderServer.runKafkaMonitor(proto.ClusterID(1), kafkaOffset)
	require.Error(t, err)

	hosts := []string{leaderHost, followerHost}
	for _, host := range hosts {
		clusterMgrCli := mocks.NewMockClientAPI(ctr)
		clusterMgrCli.EXPECT().GetService(any, any).AnyTimes().Return(cmapi.ServiceInfo{Nodes: []cmapi.ServiceNode{{ClusterID: 1, Host: host}}}, nil)
		cli := api.New(&api.Config{}, clusterMgrCli, proto.ClusterID(1))
		_, err = cli.Stats(ctx, host)
		require.NoError(t, err)
		_, err = cli.LeaderStats(ctx)
		require.NoError(t, err)

		err = cli.UpdateVolume(ctx, leaderHost, proto.Vid(1))
		require.NoError(t, err)

		err = cli.UpdateVolume(ctx, followerHost, proto.Vid(1))
		require.NoError(t, err)

		_, err = cli.AcquireTask(ctx, &api.AcquireArgs{IDC: "z0"})
		require.NoError(t, err)
	}

	err = leaderServer.load()
	require.NoError(t, err)
}

func TestServiceSetup(t *testing.T) {
	require.Panics(t, func() {
		setUp()
	})
}

func TestServiceInitConf(t *testing.T) {
	_, err := initConfig([]string{})
	require.Error(t, err)
}

func TestServiceClose(t *testing.T) {
	ctr := gomock.NewController(t)
	service = newMockServiceWithOpts(ctr, false)
	tearDown()
}

func TestServiceRegister(t *testing.T) {
	ctr := gomock.NewController(t)
	service = newMockServiceWithOpts(ctr, false)
	service.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().Register(any, any).Return(nil)
	err := service.register(ServiceRegisterConfig{})
	require.NoError(t, err)
}
