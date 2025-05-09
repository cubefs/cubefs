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

package volumemgr

import (
	"context"
	"errors"
	"testing"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	"github.com/cubefs/cubefs/blobstore/clustermgr/mock"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestVolumeMgr_CreateVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockRaftServer.EXPECT().Status().AnyTimes().Return(raftserver.Status{Id: 1})
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockDiskMgr := cluster.NewMockBlobNodeManagerAPI(ctr)
	mockDiskMgr.EXPECT().AllocChunks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy cluster.AllocPolicy) ([]proto.DiskID, []proto.Vuid, error) {
		diskids := make([]proto.DiskID, len(policy.Vuids))
		for i := range diskids {
			diskids[i] = 9999
		}
		return diskids, policy.Vuids, nil
	})
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockVolumeMgr.raftServer = mockRaftServer
	mockVolumeMgr.scopeMgr = mockScopeMgr
	mockVolumeMgr.diskMgr = mockDiskMgr

	// success case
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(31), uint64(31), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(2).Return(nil)
		err := mockVolumeMgr.createVolume(ctx, 1)
		require.NoError(t, err)
	}

	// failed case
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(41), uint64(41), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(1).Return(errors.New("err"))
		err := mockVolumeMgr.createVolume(ctx, 1)
		require.Error(t, err)
	}

	// failed case, create volume exist
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1), uint64(1), nil)
		err := mockVolumeMgr.createVolume(ctx, 1)
		require.Error(t, err)
	}

	vols := generateVolume(codemode.EC15P12, 1, 31)
	// failed case apply create volume
	{
		vols[0].vUnits[0].epoch = proto.MinEpoch - 1
		err := mockVolumeMgr.applyCreateVolume(ctx, vols[0])
		require.Error(t, err)

		// epoch invalid
		vols[0].vUnits[0].epoch = proto.MaxEpoch + 1
		err = mockVolumeMgr.applyCreateVolume(ctx, vols[0])
		require.Error(t, err)

		// vuid invalid
		vols[0].vUnits[0].epoch = 1
		vols[0].vUnits[0].vuInfo.Vuid = 0
		err = mockVolumeMgr.applyCreateVolume(ctx, vols[0])
		require.Error(t, err)
	}

	// az unavailable ,create volume
	{
		testConfig.UnavailableIDC = "z0"
		oldPolicies := testConfig.CodeModePolicies[:]
		defer func() {
			testConfig.UnavailableIDC = ""
			testConfig.CodeModePolicies = oldPolicies
		}()
		testConfig.CodeModePolicies = append(testConfig.CodeModePolicies,
			codemode.Policy{
				ModeName: codemode.EC4P4L2.Name(),
				Enable:   true,
			},
		)
		mockVolumeMgr, clean := initMockVolumeMgr(t)
		defer clean()

		mockVolumeMgr.raftServer = mockRaftServer
		mockVolumeMgr.scopeMgr = mockScopeMgr
		mockVolumeMgr.diskMgr = mockDiskMgr

		// create 2AZ code
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(51), uint64(51), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		err := mockVolumeMgr.createVolume(ctx, 8)
		require.NoError(t, err)

		// one az Unavailable ,create 3AZ code failed
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(52), uint64(52), nil)
		err = mockVolumeMgr.createVolume(ctx, 1)
		require.Error(t, err)

		// one az Unavailable ,create 3AZ replica code failed
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(52), uint64(52), nil)
		err = mockVolumeMgr.createVolume(ctx, 100)
		require.Error(t, err)
	}
}

func TestVolumeMgr_finishLastCreateJob(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockVolumeMgr.raftServer = mockRaftServer
	mockDiskMgr := cluster.NewMockBlobNodeManagerAPI(ctr)
	mockRaftServer.EXPECT().Status().AnyTimes().Return(raftserver.Status{Id: 1})
	allocSuccess := func(n int) {
		mockDiskMgr.EXPECT().AllocChunks(gomock.Any(), gomock.Any()).MaxTimes(n).DoAndReturn(func(ctx context.Context, policy cluster.AllocPolicy) ([]proto.DiskID, []proto.Vuid, error) {
			diskids := make([]proto.DiskID, len(policy.Vuids))
			for i := range diskids {
				diskids[i] = 9999
			}
			return diskids, policy.Vuids, nil
		})
	}
	allocFailed := func(n int) {
		mockDiskMgr.EXPECT().AllocChunks(gomock.Any(), gomock.Any()).MaxTimes(n).Return(nil, nil, cluster.ErrNoEnoughSpace)
	}
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockVolumeMgr.scopeMgr = mockScopeMgr
	mockVolumeMgr.diskMgr = mockDiskMgr

	// success case
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(31), uint64(31), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(1).DoAndReturn(func(ctx context.Context, data []byte) interface{} {
			proposeInfo := base.DecodeProposeInfo(data)
			args := &CreateVolumeCtx{}
			err := args.Decode(proposeInfo.Data)
			require.NoError(t, err)
			volume, err := args.ToVolume(ctx)
			require.NoError(t, err)
			err = mockVolumeMgr.applyInitCreateVolume(ctx, volume)
			require.NoError(t, err)
			return nil
		})
		allocFailed(1)
		err := mockVolumeMgr.createVolume(ctx, codemode.EC15P12)
		require.Error(t, err)
		allocSuccess(1)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(2).Return(nil)
		err = mockVolumeMgr.finishLastCreateJob(ctx)
		require.NoError(t, err)
	}

	// failed case, propose initial create volume failed
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(41), uint64(41), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(1).Return(errors.New("err"))
		err := mockVolumeMgr.createVolume(ctx, codemode.EC15P12)
		require.Error(t, err)
	}

	// failed case, propose increase volume units epoch failed
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(41), uint64(41), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(1).Return(nil)
		allocFailed(1)
		err := mockVolumeMgr.createVolume(ctx, codemode.EC15P12)
		require.Error(t, err)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(1).Return(errors.New("err"))
		err = mockVolumeMgr.finishLastCreateJob(ctx)
		require.Error(t, err)
	}

	// failed case, alloc chunks failed
	{
		allocFailed(1)
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(42), uint64(42), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		err := mockVolumeMgr.createVolume(ctx, codemode.EC15P12)
		require.Error(t, err)
		allocFailed(1)
		err = mockVolumeMgr.finishLastCreateJob(ctx)
		require.Error(t, err)
	}

	// replica failed case, alloc chunks failed
	{
		allocFailed(1)
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(42), uint64(42), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		err := mockVolumeMgr.createVolume(ctx, codemode.Replica3)
		require.Error(t, err)
		allocFailed(1)
		err = mockVolumeMgr.finishLastCreateJob(ctx)
		require.Error(t, err)
	}

	// finish all last create job
	{
		allocSuccess(2)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		err := mockVolumeMgr.finishLastCreateJob(ctx)
		require.NoError(t, err)
	}

	// failed case, create volume exist
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1), uint64(1), nil)
		err := mockVolumeMgr.createVolume(ctx, codemode.EC15P12)
		require.Error(t, err)
	}

	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1), uint64(1), nil)
		err := mockVolumeMgr.createVolume(ctx, codemode.Replica3)
		require.Error(t, err)
	}
}
