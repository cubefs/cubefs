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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

var serviceName = proto.ServiceNameProxy

type hostSet map[string]struct{}

func (set hostSet) Keys() []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}

var _, serviceCtx = trace.StartSpanFromContext(context.Background(), "TestAccessService")

func TestAccessServiceNew(t *testing.T) {
	{
		sc, err := controller.NewServiceController(
			controller.ServiceConfig{IDC: idc}, cmcli, proxycli, nil)
		require.NoError(t, err)

		_, err = sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
	}
	{
		sc, err := controller.NewServiceController(
			controller.ServiceConfig{IDC: idc + "x", ReloadSec: 1}, cmcli, proxycli, nil)
		require.NoError(t, err)

		_, err = sc.GetServiceHost(serviceCtx, serviceName)
		require.Error(t, err)
	}
}

func TestAccessServiceGetServiceHost(t *testing.T) {
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{IDC: idc, ReloadSec: 1}, cmcli, proxycli, nil)
	require.NoError(t, err)

	keys := make(hostSet)
	for ii := 0; ii < 100; ii++ {
		host, err := sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
		keys[host] = struct{}{}
	}
	require.ElementsMatch(t, keys.Keys(), []string{"proxy-1", "proxy-2"})

	hosts, err := sc.GetServiceHosts(serviceCtx, serviceName)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"proxy-1", "proxy-2"}, hosts)
}

func TestAccessServicePunishService(t *testing.T) {
	stop := closer.New()
	defer stop.Close()
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{IDC: idc, ReloadSec: 1}, cmcli, proxycli, stop.Done())
	require.NoError(t, err)

	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"proxy-1", "proxy-2"})
	}

	sc.PunishService(serviceCtx, serviceName, "proxy-2", 2)
	for ii := 0; ii < 100; ii++ {
		host, err := sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
		require.True(t, host == "proxy-1")
	}

	time.Sleep(time.Millisecond * 1200)
	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"proxy-1", "proxy-2"})
	}
}

func TestAccessServicePunishServiceWithThreshold(t *testing.T) {
	stop := closer.New()
	defer stop.Close()
	threshold := uint32(5)
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{
			IDC:                         idc,
			ReloadSec:                   1,
			ServicePunishThreshold:      threshold,
			ServicePunishValidIntervalS: 2,
		}, cmcli, proxycli, stop.Done())
	require.NoError(t, err)

	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"proxy-1", "proxy-2"})
	}

	// not punish
	for ii := threshold; ii > 1; ii-- {
		sc.PunishServiceWithThreshold(serviceCtx, serviceName, "proxy-1", 2)
	}
	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"proxy-1", "proxy-2"})
	}

	// punished
	sc.PunishServiceWithThreshold(serviceCtx, serviceName, "proxy-1", 2)
	for ii := 0; ii < 100; ii++ {
		host, err := sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
		require.True(t, host == "proxy-2")
	}

	time.Sleep(time.Millisecond * 1200)
	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"proxy-1", "proxy-2"})
	}
}

func TestAccessServiceGetDiskHost(t *testing.T) {
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{IDC: idc, ReloadSec: 1}, cmcli, proxycli, nil)
	require.NoError(t, err)

	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
	}
	{
		_, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10000))
		require.Error(t, err)
	}
}

func TestAccessServiceGetBrokenDiskHost(t *testing.T) {
	brokenRet := cmapi.ListDiskRet{}
	brokenRet.Disks = make([]*bnapi.DiskInfo, 2)
	brokenRet.Disks[0] = &bnapi.DiskInfo{}
	brokenRet.Disks[1] = &bnapi.DiskInfo{}

	cli := mocks.NewMockClientAPI(C(t))
	cli.EXPECT().GetService(A, A).Times(5).DoAndReturn(
		func(_ context.Context, args cmapi.GetServiceArgs) (cmapi.ServiceInfo, error) {
			if val, ok := dataNodes[args.Name]; ok {
				return val, nil
			}
			return cmapi.ServiceInfo{}, errNotFound
		})
	cli.EXPECT().ListDisk(A, A).Times(6).Return(brokenRet, nil)

	pcli := mocks.NewMockProxyClient(C(t))
	pcli.EXPECT().GetCacheDisk(A, A, A).AnyTimes().DoAndReturn(
		func(_ context.Context, _ string, args *proxy.CacheDiskArgs) (*bnapi.DiskInfo, error) {
			if val, ok := dataDisks[args.DiskID]; ok {
				return &val, nil
			}
			return nil, errNotFound
		})

	stop := closer.New()
	defer stop.Close()
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{IDC: idc, ReloadSec: 1, LoadDiskInterval: 1}, cli, pcli, stop.Done())
	require.NoError(t, err)

	{
		host, err := sc.GetDiskHost(serviceCtx, 10001)
		require.NoError(t, err)
		require.False(t, host.Punished)
		host, err = sc.GetDiskHost(serviceCtx, 10002)
		require.NoError(t, err)
		require.False(t, host.Punished)
	}

	brokenRet.Disks[0].DiskID = 10000
	brokenRet.Disks[1].DiskID = 10002
	time.Sleep(1200 * time.Millisecond)
	{
		host, err := sc.GetDiskHost(serviceCtx, 10001)
		require.NoError(t, err)
		require.False(t, host.Punished)
		host, err = sc.GetDiskHost(serviceCtx, 10002)
		require.NoError(t, err)
		require.True(t, host.Punished)
	}

	brokenRet.Disks[1].DiskID = 10001
	time.Sleep(time.Second)
	{
		host, err := sc.GetDiskHost(serviceCtx, 10001)
		require.NoError(t, err)
		require.True(t, host.Punished)
		host, err = sc.GetDiskHost(serviceCtx, 10002)
		require.NoError(t, err)
		require.False(t, host.Punished)
	}

	brokenRet.Disks[0].DiskID = 10000
	brokenRet.Disks[1].DiskID = 10000
	cli.EXPECT().ListDisk(A, A).Times(1).Return(brokenRet, errors.New("list error"))
	time.Sleep(time.Second)
	{
		host, err := sc.GetDiskHost(serviceCtx, 10001)
		require.NoError(t, err)
		require.True(t, host.Punished)
		host, err = sc.GetDiskHost(serviceCtx, 10002)
		require.NoError(t, err)
		require.False(t, host.Punished)
	}

	brokenRet.Disks = brokenRet.Disks[:0]
	cli.EXPECT().ListDisk(A, A).Times(2).Return(brokenRet, nil)
	time.Sleep(time.Second)
	{
		host, err := sc.GetDiskHost(serviceCtx, 10001)
		require.NoError(t, err)
		require.False(t, host.Punished)
		host, err = sc.GetDiskHost(serviceCtx, 10002)
		require.NoError(t, err)
		require.False(t, host.Punished)
	}
}

func TestAccessServicePunishDisk(t *testing.T) {
	stop := closer.New()
	defer stop.Close()
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{IDC: idc, ReloadSec: 1}, cmcli, proxycli, stop.Done())
	require.NoError(t, err)

	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.False(t, host.Punished)
	}
	sc.PunishDisk(serviceCtx, 10001, 1)
	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.True(t, host.Punished)
	}
	time.Sleep(time.Second)
	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.False(t, host.Punished)
	}
	sc.PunishDiskWithThreshold(serviceCtx, 10001, 1)
	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.False(t, host.Punished)
	}
}
