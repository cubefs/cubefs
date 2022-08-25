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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// vuidPrefix1=4294967296(vid=1,epoch=1)
var vuidPrefix1 proto.VuidPrefix = 4294967296

func TestVolumeMgr_ListVolumeUnitInfo(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	args := &clustermgr.ListVolumeUnitArgs{DiskID: 2}
	ret, err := mockVolumeMgr.ListVolumeUnitInfo(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, volumeCount, len(ret))
}

func TestVolumeMgr_AllocVolumeUnit(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockDiskMgr := NewMockDiskMgrAPI(ctr)
	mockDiskMgr.EXPECT().AllocChunks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy *diskmgr.AllocPolicy) ([]proto.DiskID, error) {
		var diskids []proto.DiskID
		for i := range policy.Vuids {
			diskids = append(diskids, proto.DiskID(i+1))
		}
		return diskids, nil
	})
	mockDiskMgr.EXPECT().Stat(gomock.Any()).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 35})
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockIsDiskWritable)
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockVolumeMgr.diskMgr = mockDiskMgr

	var vuidPrefix proto.VuidPrefix = 4294967296
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// test success alloc volumeUnit
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data []byte) error {
		mockVolumeMgr.pendingEntries.Range(func(key, value interface{}) bool {
			// apply volume unit will store a newvuid
			mockVolumeMgr.pendingEntries.Store(key, proto.EncodeVuid(vuidPrefix, 3))
			return true
		})
		return nil
	})
	mockVolumeMgr.raftServer = mockRaftServer
	ret, err := mockVolumeMgr.AllocVolumeUnit(ctx, proto.EncodeVuid(vuidPrefix, 1))
	require.NoError(t, err)
	require.Equal(t, ret.Vuid, proto.EncodeVuid(vuidPrefix, 3))
	require.NotEqual(t, ret.DiskID, 0)

	// failed case,raft propose error
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(errors.New("error"))
	ret, err = mockVolumeMgr.AllocVolumeUnit(ctx, proto.EncodeVuid(vuidPrefix, 1))
	require.Error(t, err)
	require.Nil(t, ret)

	// failed case:vid not exist
	ret, err = mockVolumeMgr.AllocVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(44, 1), 1))
	require.Error(t, err)
	require.Nil(t, ret)

	// failed case, pendingEntries = 0
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data []byte) error {
		mockVolumeMgr.pendingEntries.Range(func(key, value interface{}) bool {
			// apply volume unit will store a newvuid
			mockVolumeMgr.pendingEntries.Store(key, proto.Vuid(0))
			return true
		})
		return nil
	})
	ret, err = mockVolumeMgr.AllocVolumeUnit(ctx, proto.EncodeVuid(vuidPrefix, 1))
	require.Error(t, err)
	require.Nil(t, ret)

	// failed case , index over
	_, err = mockVolumeMgr.AllocVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(1, 30), 1))
	require.Error(t, err)
}

func TestVolumeMgr_applyAllocVolumeUnit(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	unit, err := mockVolumeMgr.volumeTbl.GetVolumeUnit(vuidPrefix1)
	require.NoError(t, err)
	epoch := unit.Epoch
	allocArgs := &allocVolumeUnitCtx{
		Vuid:           proto.EncodeVuid(vuidPrefix1, epoch),
		NextEpoch:      epoch + 1,
		PendingVuidKey: proto.EncodeVuid(vuidPrefix1, epoch),
	}
	mockVolumeMgr.pendingEntries.Store(allocArgs.PendingVuidKey, proto.Vuid(0))
	err = mockVolumeMgr.applyAllocVolumeUnit(context.Background(), allocArgs)
	require.NoError(t, err)

	vol := mockVolumeMgr.all.getVol(vuidPrefix1.Vid())
	nextEpoch := vol.vUnits[vuidPrefix1.Index()].nextEpoch
	require.NoError(t, err)
	require.Equal(t, epoch+1, nextEpoch)

	err = mockVolumeMgr.applyAllocVolumeUnit(context.Background(), allocArgs)
	require.NoError(t, err)
	vol = mockVolumeMgr.all.getVol(vuidPrefix1.Vid())
	nextEpoch = vol.vUnits[vuidPrefix1.Index()].nextEpoch
	require.NoError(t, err)
	require.Equal(t, epoch+1, nextEpoch)

	// failed case ,vid not exist
	allocArgs.Vuid = proto.Vuid(proto.EncodeVuid(proto.EncodeVuidPrefix(44, 1), epoch))
	err = mockVolumeMgr.applyAllocVolumeUnit(context.Background(), allocArgs)
	require.Error(t, err)
}

func TestVolumeMgr_updateVolumeUnit(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	ctr := gomock.NewController(t)
	dnClient := mocks.NewMockStorageAPI(ctr)
	mockVolumeMgr.blobNodeClient = dnClient
	args := &clustermgr.UpdateVolumeArgs{
		OldVuid:   proto.EncodeVuid(vuidPrefix1, 1),
		NewVuid:   proto.EncodeVuid(vuidPrefix1, 1),
		NewDiskID: 30,
	}
	dnClient.EXPECT().StatChunk(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(
		&blobnode.ChunkInfo{Vuid: args.NewVuid, DiskID: 30}, nil)

	// test preUpdateVolumeUnit()
	{
		// success case
		vol := mockVolumeMgr.all.getVol(2)
		vol.lock.Lock()
		vol.vUnits[0].nextEpoch = 2
		vol.lock.Unlock()
		err := mockVolumeMgr.PreUpdateVolumeUnit(context.Background(), &clustermgr.UpdateVolumeArgs{
			OldVuid:   proto.EncodeVuid(proto.EncodeVuidPrefix(2, 0), 1),
			NewVuid:   proto.EncodeVuid(proto.EncodeVuidPrefix(2, 0), 2),
			NewDiskID: 30,
		})
		require.NoError(t, err)

		// failed case, vid not exist
		err = mockVolumeMgr.PreUpdateVolumeUnit(context.Background(), &clustermgr.UpdateVolumeArgs{
			OldVuid:   proto.EncodeVuid(proto.EncodeVuidPrefix(99, 1), 1),
			NewVuid:   proto.EncodeVuid(vuidPrefix1, 1),
			NewDiskID: 30,
		})
		require.Error(t, err)

		// failed case,old vuid not match
		args.OldVuid = proto.EncodeVuid(vuidPrefix1, 111)
		err = mockVolumeMgr.PreUpdateVolumeUnit(context.Background(), args)
		require.Equal(t, ErrOldVuidNotMatch, err)

		// failed case,new vuid not match
		args.OldVuid = proto.EncodeVuid(vuidPrefix1, 1)
		args.NewVuid = proto.EncodeVuid(vuidPrefix1, 222)
		err = mockVolumeMgr.PreUpdateVolumeUnit(context.Background(), args)
		require.Equal(t, ErrNewVuidNotMatch, err)
	}

	// test applyUpdateVolumeUnit()
	{
		_, ctx := trace.StartSpanFromContext(context.Background(), "applyVolumeUnit")
		beforeUnits, err := mockVolumeMgr.ListVolumeUnitInfo(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: 1})
		require.NoError(t, err)
		require.Equal(t, len(beforeUnits), 30)

		volInfo := mockVolumeMgr.all.getVol(3)
		volInfo.lock.Lock()
		volInfo.vUnits[0].epoch = 2
		volInfo.vUnits[0].nextEpoch = 2
		volInfo.lock.Unlock()

		// success case, vid=1 volume status=active
		err = mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(3, 0), 2), 2)
		require.NoError(t, err)

		// repeat update
		err = mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(3, 0), 2), 2)
		require.NoError(t, err)

		units, err := mockVolumeMgr.ListVolumeUnitInfo(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: 2})
		require.NoError(t, err)
		require.Equal(t, len(units), 31)

		afterUnits, err := mockVolumeMgr.ListVolumeUnitInfo(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: 1})
		require.NoError(t, err)
		require.Equal(t, len(afterUnits), 29)

		// success case, vid=1 volume status=idle
		err = mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(2, 1), 1), 30)
		require.NoError(t, err)

		// success case, epoch= 0 just test next epoch small than  epoch store in db.actually epoch must bigger than 0
		err = mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(2, 1), 0), 30)
		require.NoError(t, err)

		// failed case, vid not exist
		err = mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(44, 1), 1), 30)
		require.Error(t, err)

		// failed case, vuidPrefix not match
		err = mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(2, 55), 1), 30)
		require.Error(t, err)
	}

	// test applyUpdateVolumeUnit, refresh health return err
	{
		_, ctx := trace.StartSpanFromContext(context.Background(), "applyVolumeUnit")
		mockDiskMgr := NewMockDiskMgrAPI(ctr)
		mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, id proto.DiskID) (*blobnode.DiskInfo, error) {
			heatInfo := blobnode.DiskHeartBeatInfo{
				DiskID: id,
			}
			diskInfo := &blobnode.DiskInfo{
				DiskHeartBeatInfo: heatInfo,
				Idc:               "z0",
				Host:              "127.0.0.1",
			}
			return diskInfo, nil
		})
		mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().Return(false, errors.New("err"))
		mockVolumeMgr.diskMgr = mockDiskMgr

		volInfo := mockVolumeMgr.all.getVol(11)
		volInfo.lock.Lock()
		volInfo.vUnits[0].epoch = 2
		volInfo.vUnits[0].nextEpoch = 2
		volInfo.lock.Unlock()

		err := mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(11, 0), 2), 2)
		require.Error(t, err)
	}

	// test applyUpdateVolumeUnit, get diskInfo return error
	{
		_, ctx := trace.StartSpanFromContext(context.Background(), "applyVolumeUnit")
		mockDiskMgr := NewMockDiskMgrAPI(ctr)
		mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errors.New("err"))
		mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().Return(true, errors.New("err"))
		mockVolumeMgr.diskMgr = mockDiskMgr

		volInfo := mockVolumeMgr.all.getVol(12)
		volInfo.lock.Lock()
		volInfo.vUnits[0].epoch = 2
		volInfo.vUnits[0].nextEpoch = 2
		volInfo.lock.Unlock()
		err := mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(proto.EncodeVuidPrefix(12, 0), 2), 2)
		require.Error(t, err)
	}
}

func TestVolumeMgr_applyChunkSetCompact(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	args := &clustermgr.SetCompactChunkArgs{
		Vuid:       proto.EncodeVuid(vuidPrefix1, 1),
		Compacting: true,
	}
	vol := mockVolumeMgr.all.getVol(args.Vuid.Vid())
	err := mockVolumeMgr.applyChunkSetCompact(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, vol.vUnits[args.Vuid.Index()].vuInfo.Compacting, true)

	// success case, already set compacting ,direct return
	args2 := &clustermgr.SetCompactChunkArgs{
		Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 2), 1),
		Compacting: true,
	}
	vol2 := mockVolumeMgr.all.getVol(args2.Vuid.Vid())
	vol2.lock.Lock()
	vol2.vUnits[args2.Vuid.Index()].vuInfo.Compacting = true
	vol2.lock.Unlock()
	err = mockVolumeMgr.applyChunkSetCompact(context.Background(), args2)
	require.NoError(t, err)

	// failed case ,vid not exist
	args.Vuid = proto.Vuid(proto.EncodeVuid(proto.EncodeVuidPrefix(44, 1), 1))
	err = mockVolumeMgr.applyChunkSetCompact(context.Background(), args)
	require.Error(t, err)

	// failed case ,vuid  not match
	args.Vuid = proto.Vuid(proto.EncodeVuid(proto.EncodeVuidPrefix(1, 44), 1))
	err = mockVolumeMgr.applyChunkSetCompact(context.Background(), args)
	require.Error(t, err)
}

func TestVolumeMgr_DiskWritableChange(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	// avoid run loopCreateVolume() background
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockVolumeMgr.raftServer = mockRaftServer

	// success case
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil)
	err := mockVolumeMgr.DiskWritableChange(ctx, 2)
	require.NoError(t, err)

	// failed case
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(errors.New("error"))
	err = mockVolumeMgr.DiskWritableChange(ctx, 2)
	require.Error(t, err)
}

func TestVolumeMgr_applyDiskWritableChange(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	vol, err := mockVolumeMgr.GetVolumeInfo(ctx, vuidPrefix1.Vid())
	require.NoError(t, err)
	require.Equal(t, vol.HealthScore, 0)

	volInfo := mockVolumeMgr.all.getVol(vuidPrefix1.Vid())
	volInfo.lock.Lock()
	volInfo.vUnits[vuidPrefix1.Index()].epoch = 2
	volInfo.vUnits[vuidPrefix1.Index()].nextEpoch = 2
	volInfo.lock.Unlock()
	err = mockVolumeMgr.applyUpdateVolumeUnit(ctx, proto.EncodeVuid(vuidPrefix1, 2), 29)
	require.NoError(t, err)

	// diskID:29 is be set unWritable
	err = mockVolumeMgr.applyDiskWritableChange(context.Background(), []proto.VuidPrefix{vuidPrefix1})
	require.NoError(t, err)
	vol2, err := mockVolumeMgr.GetVolumeInfo(ctx, vuidPrefix1.Vid())
	require.NoError(t, err)
	require.Equal(t, vol2.HealthScore, -1)

	// vid not exist
	err = mockVolumeMgr.applyDiskWritableChange(context.Background(), []proto.VuidPrefix{proto.EncodeVuidPrefix(99, 1)})
	require.Error(t, err)
}

func TestVolumeMgr_applyChunkReport(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	var args []blobnode.ChunkInfo
	for i := 0; i < volumeCount; i++ {
		vuid := proto.EncodeVuid(proto.EncodeVuidPrefix(proto.Vid(i), 2), 1)
		chunk := blobnode.ChunkInfo{
			Vuid:  vuid,
			Total: defaultChunkSize + 1,
			Free:  uint64(1024 * 1024),
			Used:  defaultChunkSize - uint64(1024*i),
		}
		args = append(args, chunk)
	}
	// set  report chunkInfo(vid:1) epoch=2,
	args[1].Vuid = proto.EncodeVuid(proto.EncodeVuidPrefix(proto.Vid(1), 2), 0)
	err := mockVolumeMgr.applyChunkReport(context.Background(), &clustermgr.ReportChunkArgs{ChunkInfos: args})
	require.NoError(t, err)
	for i := 0; i < len(args); i++ {
		vol := mockVolumeMgr.all.getVol(args[i].Vuid.Vid())
		unit := vol.vUnits[args[i].Vuid.Index()]
		require.NoError(t, err)
		// vid=1,report chunk's vuid epoch=2 > 1,will not apply
		if i == 1 {
			require.NotEqual(t, unit.vuInfo.Free, uint64(1024*1024))
			continue
		}
		require.Equal(t, unit.vuInfo.Free, uint64(1024*1024))
	}

	// invalid vuid case
	args[1].Vuid = proto.EncodeVuid(proto.EncodeVuidPrefix(44, 2), 1)
	err = mockVolumeMgr.applyChunkReport(context.Background(), &clustermgr.ReportChunkArgs{ChunkInfos: args})
	require.NoError(t, err)
}

func TestVolumeMgr_ReleaseVolumeUnit(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	// avoid background loopCreate volume
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockVolumeMgr.raftServer = mockRaftServer
	mockDiskMgr := NewMockDiskMgrAPI(ctr)
	mockVolumeMgr.diskMgr = mockDiskMgr
	mockBlobNode := mocks.NewMockStorageAPI(ctr)
	mockVolumeMgr.blobNodeClient = mockBlobNode

	// mockDiskMgr.EXPECT().Stat(gomock.Any()).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 60})
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, id proto.DiskID) (*blobnode.DiskInfo, error) {
		heatInfo := blobnode.DiskHeartBeatInfo{
			DiskID: 1,
		}
		diskInfo := &blobnode.DiskInfo{
			DiskHeartBeatInfo: heatInfo,
			Idc:               "z0",
			Host:              "127.0.0.1",
		}
		return diskInfo, nil
	})
	mockBlobNode.EXPECT().ReleaseChunk(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	err := mockVolumeMgr.ReleaseVolumeUnit(ctx, proto.EncodeVuid(vuidPrefix1, 1), 1, false)
	require.NoError(t, err)

	// failed case ,diskid not exist
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).Return(nil, errors.New("err"))
	err = mockVolumeMgr.ReleaseVolumeUnit(ctx, proto.EncodeVuid(vuidPrefix1, 1), 90, false)
	require.Error(t, err)
}

func BenchmarkVolumeMgr_ChunkReport(b *testing.B) {
	mockVolumeMgr, clean := initMockVolumeMgr(b)
	defer clean()

	var args []blobnode.ChunkInfo
	for i := 0; i < volumeCount; i++ {
		vuid := proto.EncodeVuid(proto.EncodeVuidPrefix(proto.Vid(i), 2), 1)
		chunk := blobnode.ChunkInfo{
			Vuid:  vuid,
			Total: defaultChunkSize + 1,
			Free:  uint64(1024 * 1024),
			Used:  defaultChunkSize - uint64(1024*i),
		}
		args = append(args, chunk)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := mockVolumeMgr.applyChunkReport(context.Background(), &clustermgr.ReportChunkArgs{ChunkInfos: args})
			require.NoError(b, err)
		}
	})
}
