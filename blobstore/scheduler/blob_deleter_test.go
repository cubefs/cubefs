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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

func newBlobDeleteMgr(t *testing.T) *BlobDeleteMgr {
	ctr := gomock.NewController(t)
	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)

	clusterTopology := NewMockClusterTopology(ctr)
	clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
		func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
			return &client.VolumeInfoSimple{Vid: vid}, nil
		},
	)
	clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)
	taskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeBlobDelete.String())
	require.NoError(t, err)

	blobnodeCli := NewMockBlobnodeAPI(ctr)
	blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(nil)
	blobnodeCli.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	delLogger := mocks.NewMockRecordLogEncoder(ctr)
	delLogger.EXPECT().Close().AnyTimes().Return(nil)
	delLogger.EXPECT().Encode(any).AnyTimes().Return(nil)
	tp := taskpool.New(2, 2)

	return &BlobDeleteMgr{
		taskSwitch: taskSwitch,
		taskPool:   &tp,

		clusterTopology: clusterTopology,
		blobnodeCli:     blobnodeCli,

		delSuccessCounter:    base.NewCounter(1, "delete", base.KindSuccess),
		delFailCounter:       base.NewCounter(1, "delete", base.KindFailed),
		errStatsDistribution: base.NewErrorStats(),
		delLogger:            delLogger,

		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},

		Closer: closer.New(),
		cfg: &BlobDeleteConfig{
			MessagePunishThreshold: defaultMessagePunishThreshold,
		},
	}
}

func TestBlobDelete(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newBlobDeleteMgr(t)
	{
		// consume success
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology
		msg := &proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456"}
		err := mgr.Delete(ctx, msg)
		require.NoError(t, err)
		mgr.clusterTopology = oldClusterTopology
	}
}

func TestDeleteBlob(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	volume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
	{
		// mark delete failed
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnodeCli

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errMock)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete failed and need update volume cache: update cache failed
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mgr.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, ErrFrequentlyUpdate)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete failed and need update volume cache: update cache success but volume not change
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mgr.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, nil)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete and delete success
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(nil)
		blobnodeCli.EXPECT().Delete(any, any, any).Times(6).Return(nil)
		mgr.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		newVolume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
		newVolume.VunitLocations[5].Vuid += 1
		clusterTopology.EXPECT().UpdateVolume(any).Return(newVolume, nil)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.NoError(t, err)
		require.False(t, doneVolume.EqualWith(volume))
		require.True(t, doneVolume.EqualWith(newVolume))
	}
}
