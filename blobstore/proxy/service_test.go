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

package proxy

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

//go:generate mockgen -destination=./mock_cacher_test.go -package=proxy -mock_names Cacher=MockCacher github.com/cubefs/cubefs/blobstore/proxy/cacher Cacher

var (
	A = gomock.Any()

	ctx = context.Background()

	proxyServer *httptest.Server
	once        sync.Once
)

func runMockService(s *Service) string {
	once.Do(func() {
		proxyServer = httptest.NewServer(NewHandler(s))
	})
	return proxyServer.URL
}

func newMockService(t *testing.T) *Service {
	ctr := gomock.NewController(t)

	cacher := NewMockCacher(ctr)
	cacher.EXPECT().GetVolume(A, A).AnyTimes().DoAndReturn(
		func(_ context.Context, args *clustermgr.CacheVolumeArgs) (*clustermgr.VersionVolume, error) {
			volume := new(clustermgr.VersionVolume)
			if args.Vid%2 == 0 {
				volume.Vid = args.Vid
				return volume, nil
			}
			if args.Flush {
				return nil, errcode.ErrVolumeNotExist
			}
			return nil, errors.New("internal error")
		})
	cacher.EXPECT().GetDisk(A, A).AnyTimes().DoAndReturn(
		func(_ context.Context, args *clustermgr.CacheDiskArgs) (*blobnode.DiskInfo, error) {
			disk := new(blobnode.DiskInfo)
			if args.DiskID%2 == 0 {
				disk.DiskID = args.DiskID
				return disk, nil
			}
			if args.Flush {
				return nil, errcode.ErrCMDiskNotFound
			}
			return nil, errors.New("internal error")
		})
	cacher.EXPECT().Erase(A, A).AnyTimes().DoAndReturn(
		func(_ context.Context, key string) error {
			if key == "ALL" {
				return errors.New("internal error")
			}
			return nil
		})

	return &Service{
		Config: Config{},
		cacher: cacher,
	}
}

func newClient() rpc.Client {
	return rpc.NewClient(&rpc.Config{})
}

func TestService_New(t *testing.T) {
	// interface test
	cmcli := mocks.NewMockClientAPI(gomock.NewController(t))
	cmcli.EXPECT().RegisterService(A, A, A, A, A).Return(nil)

	testCases := []struct {
		cfg Config
	}{
		// todo wait cm chang rpc
		{
			cfg: Config{},
		},
	}
	for _, tc := range testCases {
		New(tc.cfg, cmcli)
	}
}

func TestService_CacherVolume(t *testing.T) {
	url := runMockService(newMockService(t)) + "/cache/volume/"
	cli := newClient()
	var volume clustermgr.VolumeInfo
	{
		err := cli.GetWith(ctx, url+"1024", &volume)
		require.NoError(t, err)
		require.Equal(t, proto.Vid(1024), volume.Vid)
	}
	{
		err := cli.GetWith(ctx, url+"111", &volume)
		require.Error(t, err)
	}
	{
		err := cli.GetWith(ctx, url+"111?flush=0", &volume)
		require.Error(t, err)
		require.Equal(t, 500, rpc.DetectStatusCode(err))
	}
	{
		err := cli.GetWith(ctx, url+"111?flush=1", &volume)
		require.Error(t, err)
		require.Equal(t, errcode.CodeVolumeNotExist, rpc.DetectStatusCode(err))
	}
	{
		err := cli.GetWith(ctx, url+"111?flush=true", &volume)
		require.Error(t, err)
		require.Equal(t, errcode.CodeVolumeNotExist, rpc.DetectStatusCode(err))
	}
}

func TestService_CacherDisk(t *testing.T) {
	url := runMockService(newMockService(t)) + "/cache/disk/"
	cli := newClient()
	var disk blobnode.DiskInfo
	{
		err := cli.GetWith(ctx, url+"1024", &disk)
		require.NoError(t, err)
		require.Equal(t, proto.DiskID(1024), disk.DiskID)
	}
	{
		err := cli.GetWith(ctx, url+"111", nil)
		require.Error(t, err)
	}
	{
		err := cli.GetWith(ctx, url+"111?flush=0", nil)
		require.Error(t, err)
		require.Equal(t, 500, rpc.DetectStatusCode(err))
	}
	{
		err := cli.GetWith(ctx, url+"111?flush=true", nil)
		require.Error(t, err)
		require.Equal(t, errcode.CodeCMDiskNotFound, rpc.DetectStatusCode(err))
	}
}

func TestService_CacherErase(t *testing.T) {
	url := runMockService(newMockService(t)) + "/cache/erase/"
	cli := newClient()
	{
		resp, err := cli.Delete(ctx, url+"volume-10")
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		resp.Body.Close()
	}
	{
		resp, err := cli.Delete(ctx, url+"ALL")
		require.NoError(t, err)
		require.Equal(t, 500, resp.StatusCode)
		resp.Body.Close()
	}
}
