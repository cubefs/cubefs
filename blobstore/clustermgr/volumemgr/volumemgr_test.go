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
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockclustermgr"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

var (
	volumeCount             = 30
	defaultChunkSize uint64 = 1 << 34
	testConfig              = VolumeMgrConfig{
		IDC:                          []string{"z0", "z1", "z2"},
		RetainTimeS:                  100,
		ApplyConcurrency:             10,
		FlushIntervalS:               100,
		VolumeSliceMapNum:            32,
		MinAllocableVolumeCount:      0,
		AllocatableDiskLoadThreshold: 15,
		CodeModePolicies: []codemode.Policy{
			{ModeName: codemode.EC15P12.Name(), Enable: true},
			{ModeName: codemode.Replica3.Name(), Enable: false},
		},
		ShardNum: defaultShardNum,
	}
)

// initMockVolumeMgr gengerate 30 volumes,which vid is [0-29].
// in reality,vid=0 is invalid volume, this vid=0 only use to test
// vid:[0,2,4,...,28] status is VolumeStatusIdle ,which volume is in allocator, can be use to test allocVolume
// vid:[1,3,5,...,29]status is volumeStatusActive,which volume already actives, can be use to test retainVolume
func initMockVolumeMgr(t testing.TB) (*VolumeMgr, func()) {
	dir := path.Join(os.TempDir(), fmt.Sprintf("volumemgr-%d-%010d", time.Now().Unix(), rand.Intn(100000000)))
	volumeDBPPath := path.Join(dir, "volumedb")
	normalDBPath := path.Join(dir, "normaldb")
	succ := false
	defer func() {
		if !succ {
			os.RemoveAll(dir)
		}
	}()

	volumeDB, err := volumedb.Open(volumeDBPPath)
	require.NoError(t, err)
	normalDB, err := normaldb.OpenNormalDB(normalDBPath)
	require.NoError(t, err)

	volTable, err := volumedb.OpenVolumeTable(volumeDB.KVStore)
	require.NoError(t, err)
	// generate 30 volume in db, vid from 0 to 29
	volumeRecords, unitRecords, routeRecords := generateVolumeRecord(codemode.EC15P12, 0, volumeCount)
	volTable.PutVolumesAndUnitsAndRoutes(volumeRecords, unitRecords, routeRecords)
	volTable.PutTokens(generateToken(volumeRecords))

	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockConfigMgr := mock.NewMockConfigMgrAPI(ctr)
	mockDiskMgr := cluster.NewMockBlobNodeManagerAPI(ctr)

	// mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(true)
	mockConfigMgr.EXPECT().Delete(gomock.Any(), "mockKey").AnyTimes().Return(nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeReserveSizeKey).AnyTimes().Return("2097152", nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeChunkSizeKey).AnyTimes().Return("17179869184", nil)
	mockDiskMgr.EXPECT().Stat(gomock.Any(), proto.DiskTypeHDD).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 35})
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockIsDiskWritable)
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockDiskMgr.EXPECT().RegisterDiskUsageCallback(gomock.Any()).Times(1)

	mockVolumeMgr, err := NewVolumeMgr(testConfig, mockDiskMgr, mockScopeMgr, mockConfigMgr, volumeDB)
	require.NoError(t, err)
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockVolumeMgr.SetRaftServer(mockRaftServer)

	succ = true
	return mockVolumeMgr, func() {
		mockVolumeMgr.Close()
		volumeDB.Close()
		normalDB.Close()
		os.RemoveAll(dir)
		initialVolumeStatusStat()
	}
}

func mockIsDiskWritable(_ context.Context, id proto.DiskID) (bool, error) {
	return id != proto.DiskID(29), nil
}

func mockGetDiskInfo(_ context.Context, id proto.DiskID) (*clustermgr.BlobNodeDiskInfo, error) {
	return &clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: id},
		DiskInfo: clustermgr.DiskInfo{
			Idc:  "z0",
			Host: "127.0.0.1",
		},
	}, nil
}

func generateVolume(mode codemode.CodeMode, count int, startVid int) (vols []*volume) {
	for i := startVid; i < count+startVid; i++ {
		volInfo := clustermgr.VolumeInfoBase{
			Vid:         proto.Vid(i),
			CodeMode:    mode,
			HealthScore: 0,
			Status:      proto.VolumeStatusIdle + proto.VolumeStatus(i%2),
			Free:        defaultChunkSize * 12,
			Total:       defaultChunkSize * 12,
			Used:        1024,
		}
		volume := &volume{
			vid:         proto.Vid(i),
			volInfoBase: volInfo,
		}
		vUnits, _, _ := generateVolumeUnit(volume)
		volume.vUnits = vUnits
		vols = append(vols, volume)
	}
	return
}

func generateVolumeRecord(mode codemode.CodeMode, start, end int) (
	volumeRecords []*volumedb.VolumeRecord, unitRecords [][]*volumedb.VolumeUnitRecord, routeRecords []*base.RouteInfoRecord,
) {
	for i := start; i < end; i++ {
		volInfo := clustermgr.VolumeInfoBase{
			Vid:         proto.Vid(i),
			CodeMode:    mode,
			HealthScore: 0,
			Status:      proto.VolumeStatusIdle + proto.VolumeStatus(i%2),
			Free:        defaultChunkSize * 12,
			Total:       defaultChunkSize * 12,
			Used:        1024,
		}
		volume := &volume{
			vid:         proto.Vid(i),
			volInfoBase: volInfo,
		}
		vUnits, records, _ := generateVolumeUnit(volume)
		volume.vUnits = vUnits

		var vuidPrefixs []proto.VuidPrefix
		for _, record := range records {
			vuidPrefixs = append(vuidPrefixs, record.VuidPrefix)
		}
		volRecord := volume.ToRecord()
		volRecord.VuidPrefixs = vuidPrefixs
		volumeRecords = append(volumeRecords, volRecord)
		unitRecords = append(unitRecords, records)
	}
	route := &base.RouteInfoRecord{
		RouteVersion: proto.RouteVersion(1),
		Type:         proto.RouteItemTypeUpdateVolume,
		ItemDetail:   &volumedb.RouteInfoVolumeUpdate{VuidPrefix: proto.EncodeVuidPrefix(1, 1)},
	}
	routeRecords = append(routeRecords, route)
	return
}

func generateVolumeUnit(vol *volume) (volumeUints []*volumeUnit,
	unitRecords []*volumedb.VolumeUnitRecord, units []clustermgr.Unit,
) {
	modeInfo := vol.volInfoBase.CodeMode.Tactic()
	unitsCount := modeInfo.N + modeInfo.M + modeInfo.L
	for i := 0; i < unitsCount; i++ {
		vuInfo := &clustermgr.VolumeUnitInfo{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(vol.vid, uint8(i)), 1),
			DiskID:     proto.DiskID(i + 1),
			Free:       defaultChunkSize * uint64(unitsCount),
			Total:      defaultChunkSize * uint64(unitsCount),
			Used:       1024,
			Compacting: false,
			Host:       "127.0.0.1",
		}

		volumeUnit := &volumeUnit{
			vuidPrefix: proto.EncodeVuidPrefix(vol.vid, uint8(i)),
			epoch:      1,
			nextEpoch:  1,
			vuInfo:     vuInfo,
		}
		unit := clustermgr.Unit{
			Vuid:   vuInfo.Vuid,
			DiskID: vuInfo.DiskID,
		}
		volumeUints = append(volumeUints, volumeUnit)
		unitRecords = append(unitRecords, volumeUnit.ToVolumeUnitRecord())
		units = append(units, unit)
	}
	return
}

func generateToken(volumeRecords []*volumedb.VolumeRecord) (ret []*volumedb.TokenRecord) {
	for _, volume := range volumeRecords {
		if volume.Status == proto.VolumeStatusActive {
			t := "127.0.0.1:8080;" + strconv.FormatUint(uint64(volume.Vid), 10)
			tok := &token{
				vid:        volume.Vid,
				tokenID:    t,
				expireTime: time.Now().Add(time.Duration(10 * time.Second)).UnixNano(),
			}
			tokenRecord := tok.ToTokenRecord()
			ret = append(ret, tokenRecord)
		}
	}
	return
}

func Test_VolumeMgr(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	var count int
	mockVolumeMgr.all.rangeVol(func(v *volume) error {
		count++
		return nil
	})
	require.Equal(t, count, 30)
}

func Test_NewVolumeMgr(t *testing.T) {
	dir := path.Join(os.TempDir(), fmt.Sprintf("volumemgr-%d-%010d", time.Now().Unix(), rand.Intn(100000000)))
	volumeDBPPath := path.Join(dir, "volumedb")
	normalDBPath := path.Join(dir, "normaldb")
	defer initialVolumeStatusStat()
	defer os.RemoveAll(dir)

	volumeDB, err := volumedb.Open(volumeDBPPath)
	require.NoError(t, err)
	defer volumeDB.Close()
	normalDB, err := normaldb.OpenNormalDB(normalDBPath)
	require.NoError(t, err)
	defer normalDB.Close()

	volTable, err := volumedb.OpenVolumeTable(volumeDB.KVStore)
	require.NoError(t, err)
	volumeRecords, unitRecords, routeRecords := generateVolumeRecord(codemode.EC15P12, 0, volumeCount)
	volTable.PutVolumesAndUnitsAndRoutes(volumeRecords, unitRecords, routeRecords)
	volTable.PutTokens(generateToken(volumeRecords))

	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockConfigMgr := mock.NewMockConfigMgrAPI(ctr)
	mockDiskMgr := cluster.NewMockBlobNodeManagerAPI(ctr)

	codeModeConfg := []codemode.Policy{
		{
			ModeName:  codemode.EC15P12.Name(),
			MinSize:   1024,
			MaxSize:   4096,
			SizeRatio: 0.3,
			Enable:    true,
		},
		{
			ModeName:  codemode.EC6P6.Name(),
			MinSize:   1024,
			MaxSize:   4096,
			SizeRatio: 0.2,
			Enable:    false,
		},
	}
	volConfig := VolumeMgrConfig{
		IDC:                         []string{"z0", "z1", "z2"},
		RetainTimeS:                 100,
		ApplyConcurrency:            10,
		FlushIntervalS:              100,
		VolumeSliceMapNum:           32,
		CheckExpiredVolumeIntervalS: 1,
		CodeModePolicies:            codeModeConfg,
		MinAllocableVolumeCount:     30,
	}

	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(true)

	mockConfigMgr.EXPECT().Delete(gomock.Any(), "key1").AnyTimes().Return(nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeReserveSizeKey).AnyTimes().Return("2097152", nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeChunkSizeKey).AnyTimes().Return("17179869184", nil)
	mockConfigMgr.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	mockDiskMgr.EXPECT().Stat(gomock.Any(), proto.DiskTypeHDD).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 100})
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockDiskMgr.EXPECT().HasEnoughSpace(gomock.Any(), gomock.Any()).AnyTimes().Return(true)
	mockDiskMgr.EXPECT().RegisterDiskUsageCallback(gomock.Any()).Times(1)

	mockVolumeMgr, err := NewVolumeMgr(volConfig, mockDiskMgr, mockScopeMgr, mockConfigMgr, volumeDB)
	require.NoError(t, err)
	defer mockVolumeMgr.Close()
	mockVolumeMgr.SetRaftServer(mockRaftServer)

	// test volumeMgr load()
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockIsDiskWritable)
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(true)
	mockRaftServer.EXPECT().Status().AnyTimes().Return(raftserver.Status{Id: 1})
	mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(uint64(31), uint64(31), nil)
	mockDiskMgr.EXPECT().AllocChunks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy cluster.AllocPolicy) ([]proto.DiskID, []proto.Vuid, error) {
		var diskids []proto.DiskID
		for i := range policy.Vuids {
			diskids = append(diskids, proto.DiskID(i+1))
		}
		return diskids, policy.Vuids, nil
	})
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	// start loop create volume /check volume /exec task
	mockVolumeMgr.Start()

	// wait check expired volume,set volume1 expired
	vol1 := mockVolumeMgr.all.getVol(1)
	vol1.lock.Lock()
	vol1.token.expireTime = time.Now().Add(-10 * time.Second).UnixNano()
	vol1.lock.Unlock()

	// test exec task
	args := &ChangeVolStatusCtx{
		Vid:      2,
		TaskID:   uuid.New().String(),
		TaskType: base.VolumeTaskTypeLock,
	}
	err = mockVolumeMgr.applyVolumeTask(context.Background(), args)
	require.NoError(t, err)
	vol2 := mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusLock, vol2.volInfoBase.Status)

	mockVolumeMgr.configMgr.Get(context.Background(), proto.VolumeReserveSizeKey)
	mockVolumeMgr.configMgr.Set(context.Background(), proto.VolumeReserveSizeKey, "2097152")
	mockVolumeMgr.configMgr.Delete(context.Background(), "key1")
}

func TestVolumeMgr_ListVolumeInfo(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "listVolumeInfo")
	args := &clustermgr.ListVolumeArgs{
		Marker: 1,
		Count:  503,
	}
	volInfos, err := mockVolumeMgr.ListVolumeInfo(ctx, args)
	require.NoError(t, err)
	require.Equal(t, len(volInfos), 28)

	args.Count = 3
	volInfos1, err := mockVolumeMgr.ListVolumeInfo(ctx, args)
	require.NoError(t, err)
	require.Equal(t, len(volInfos1), 3)

	args.Marker = 28
	volInfos2, err := mockVolumeMgr.ListVolumeInfo(ctx, args)
	require.NoError(t, err)
	require.Equal(t, len(volInfos2), 1)
	require.Equal(t, volInfos2[0].Vid, proto.Vid(29))

	args.Marker = 29
	volInfos3, err := mockVolumeMgr.ListVolumeInfo(ctx, args)
	require.NoError(t, err)
	require.Nil(t, volInfos3)
}

func TestVolumeMgr_ListVolumeInfoV2(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "listVolumeInfoV2")

	volInfos, err := mockVolumeMgr.ListVolumeInfoV2(ctx, proto.VolumeStatusIdle)
	require.NoError(t, err)
	require.Equal(t, 15, len(volInfos))

	volInfos, err = mockVolumeMgr.ListVolumeInfoV2(ctx, proto.VolumeStatusActive)
	require.NoError(t, err)
	require.Equal(t, 15, len(volInfos))
}

func TestVolumeMgr_GetVolumeInfo(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "GetVolumeInfo")
	// success case
	vid1 := proto.Vid(2)
	volInfo, err := mockVolumeMgr.GetVolumeInfo(ctx, vid1)
	require.NoError(t, err)
	require.Equal(t, volInfo.Vid, vid1)
	// failed case
	volInfo2, err := mockVolumeMgr.GetVolumeInfo(ctx, 31)
	require.Error(t, err)
	require.Nil(t, volInfo2)
}

func TestVolumeMgr_AllocVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	mockRaftServer := mocks.NewMockRaftServer(gomock.NewController(t))
	// new raftServer to mockVolumeMgr, background run loopCreateVolume  use request IsLeader()
	// mockRaftServer.EXPECT().IsLeader()return false will not run createVolume()
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockVolumeMgr.raftServer = mockRaftServer

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	mode := codemode.EC15P12
	args := &AllocVolumeCtx{
		Vids:       []proto.Vid{2, 4},
		Host:       "127.0.0.1:8080",
		ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
	}
	allocVolumeInfos := []clustermgr.AllocVolumeInfo{
		{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:         1,
					HealthScore: 0,
				},
			},
			Token:      "127.0.0.1:8080;1",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
		},
		{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:         3,
					HealthScore: 0,
				},
			},
			Token:      "127.0.0.3:8080;3",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
		},
	}
	volInfos := &clustermgr.AllocatedVolumeInfos{AllocVolumeInfos: allocVolumeInfos}

	// test allocVolume(): success case
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data []byte) error {
			mockVolumeMgr.pendingEntries.Range(func(key, value interface{}) bool {
				mockVolumeMgr.pendingEntries.Store(key, volInfos)
				return true
			})
			return nil
		})
		ret, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.NoError(t, err)
		require.Equal(t, ret.AllocVolumeInfos[0].HealthScore, 0)
		require.Equal(t, len(ret.AllocVolumeInfos), 2)

		// alloc not exist codemode
		mode := codemode.EC6P6Align512
		ret, err = mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.Error(t, err)
		require.Nil(t, ret)
	}

	// failed case , no pending entries
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data []byte) error {
			mockVolumeMgr.pendingEntries.Range(func(key, value interface{}) bool {
				mockVolumeMgr.pendingEntries.Store(key, nil)
				return true
			})
			return nil
		})
		_, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.Error(t, err)
	}

	// failed case ,pending entries length is 0
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data []byte) error {
			mockVolumeMgr.pendingEntries.Range(func(key, value interface{}) bool {
				mockVolumeMgr.pendingEntries.Store(key, &clustermgr.AllocatedVolumeInfos{})
				return true
			})
			return nil
		})
		_, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.Error(t, err)
	}

	// test allocVolume : failed case, raft propose error
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(errors.New("error"))
		ret, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.Error(t, err)
		require.Nil(t, ret)
	}

	// failed case, only volume free space bigger than allocatableSizeThreshold can alloc
	{
		mockVolumeMgr.allocator.allocatableSize = 1 << 42
		ret, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.Error(t, err)
		require.Nil(t, ret)
	}
}

func TestVolumeMgr_applyAllocVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	mode := codemode.EC15P12
	args := &AllocVolumeCtx{
		Vids:       []proto.Vid{2, 4, 6, 8},
		Host:       "127.0.0.1:8080",
		ExpireTime: time.Now().Add(10 * time.Minute).UnixNano(),
	}
	_, ctx := trace.StartSpanFromContext(context.Background(), "applyAllocVolume")
	{
		mockVolumeMgr.pendingEntries.Store(args.PendingAllocVolKey, &clustermgr.AllocatedVolumeInfos{})
		// init status has  15 volumes in allocator, beforeLength is 15
		allocVolLenMap := mockVolumeMgr.allocator.StatAllocatable()
		beforeLength := allocVolLenMap[mode]
		for _, vid := range args.Vids {
			_, err := mockVolumeMgr.applyAllocVolume(ctx, vid, args.Host, args.ExpireTime)
			require.NoError(t, err)
		}

		allocVolLenMap = mockVolumeMgr.allocator.StatAllocatable()
		afterLength := allocVolLenMap[mode]
		require.Equal(t, beforeLength, afterLength+len(args.Vids))

		// test count > len(allocatorVol)
		args.Vids = []proto.Vid{0, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28}
		for _, vid := range args.Vids {
			_, err := mockVolumeMgr.applyAllocVolume(ctx, vid, args.Host, args.ExpireTime)
			require.NoError(t, err)
		}

		// all volumes are active, allocVolLen is 0
		allocVolLenMap = mockVolumeMgr.allocator.StatAllocatable()
		require.Equal(t, 0, allocVolLenMap[mode])
		for _, vid := range args.Vids {
			ret, err := mockVolumeMgr.applyAllocVolume(ctx, vid, args.Host, args.ExpireTime)
			require.NoError(t, err)
			// skip active volume when allocation
			require.Equal(t, 0, len(ret.Units))
		}

		// test vid not exist
		args.Vids = []proto.Vid{44}
		_, err := mockVolumeMgr.applyAllocVolume(ctx, args.Vids[0], args.Host, args.ExpireTime)
		require.Error(t, err)
	}

	// test allocVolume : success case
	{
		args := &AllocVolumeCtx{
			Host:       "127.0.0.1:8080",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
		}
		allocVolumeInfos := []clustermgr.AllocVolumeInfo{
			{
				VolumeInfo: clustermgr.VolumeInfo{
					VolumeInfoBase: clustermgr.VolumeInfoBase{
						Vid:         1,
						HealthScore: 0,
					},
				},
				Token:      "127.0.0.1:8080;1",
				ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
			},
			{
				VolumeInfo: clustermgr.VolumeInfo{
					VolumeInfoBase: clustermgr.VolumeInfoBase{
						Vid:         3,
						HealthScore: 0,
					},
				},
				Token:      "127.0.0.3:8080;3",
				ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
			},
		}
		volInfos := &clustermgr.AllocatedVolumeInfos{AllocVolumeInfos: allocVolumeInfos}

		mockRaftServer := mocks.NewMockRaftServer(gomock.NewController(t))
		mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, data []byte) error {
			mockVolumeMgr.pendingEntries.Range(func(key, value interface{}) bool {
				mockVolumeMgr.pendingEntries.Store(key, volInfos)
				return true
			})
			return nil
		})
		mockVolumeMgr.raftServer = mockRaftServer

		args.Vids = []proto.Vid{10, 12, 14, 16, 18}
		_, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.Error(t, err)
	}
}

func TestVolumeMgr_PreRetainVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	tokens := []string{
		"127.0.0.1:8080;1",
		"127.0.0.1:8080;3",
	}
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ret, err := mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)
	require.Equal(t, len(ret.RetainVolTokens), len(tokens))

	ret, err = mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.2:8080")
	require.NoError(t, err)
	require.Nil(t, ret)

	// vid(2) not has tokenID, should not  retained
	tokens = append(tokens, "127.0.0.1:8080;2")
	ret, err = mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)
	require.Equal(t, len(ret.RetainVolTokens), 2)

	// test invalid tokenID
	tokens = []string{"134"}
	_, err = mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)

	// failed case, vid not exist
	tokens = []string{"127.0.0.1:8080;55"}
	_, err = mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)

	// test retain has expired
	tokens = []string{
		"127.0.0.1:8080;5",
	}
	vol5 := mockVolumeMgr.all.getVol(proto.Vid(5))
	vol5.lock.Lock()
	vol5.token.expireTime = time.Now().Add(-10 * time.Second).UnixNano()
	vol5.lock.Unlock()
	ret, err = mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)
	require.Nil(t, ret)
}

func TestVolumeMgr_applyRetainVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	// success case
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	args := []clustermgr.RetainVolume{
		{
			Token:      "127.0.0.5:8080;5",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Second)).UnixNano(),
		},
		{
			Token:      "127.0.0.7:8080;7",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Second)).UnixNano(),
		},
	}
	err := mockVolumeMgr.applyRetainVolume(ctx, args)
	require.NoError(t, err)

	// fail case,invalid volume
	args = []clustermgr.RetainVolume{
		{
			Token:      "3224",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Second)).UnixNano(),
		},
	}
	err = mockVolumeMgr.applyRetainVolume(ctx, args)
	require.Error(t, err)

	// fail case , vid not exist
	args = []clustermgr.RetainVolume{
		{
			Token:      "127.0.0.7:8080;334",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Second)).UnixNano(),
		},
	}
	err = mockVolumeMgr.applyRetainVolume(ctx, args)
	require.Error(t, err)
}

func TestVolumeMgr_applyExpireVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	vol1 := mockVolumeMgr.all.getVol(proto.Vid(1))
	vol3 := mockVolumeMgr.all.getVol(proto.Vid(3))
	vol5 := mockVolumeMgr.all.getVol(proto.Vid(5))
	require.Equal(t, proto.VolumeStatusActive, vol1.volInfoBase.Status)
	require.Equal(t, proto.VolumeStatusActive, vol3.volInfoBase.Status)
	require.Equal(t, proto.VolumeStatusActive, vol5.volInfoBase.Status)

	vol1.lock.Lock()
	vol1.token.expireTime = time.Now().Add(-10 * time.Second).UnixNano()
	vol1.lock.Unlock()
	vol3.lock.Lock()
	vol3.token.expireTime = time.Now().Add(-10 * time.Second).UnixNano()
	vol3.lock.Unlock()
	vol5.lock.Lock()
	vol5.volInfoBase.Status = proto.VolumeStatusIdle
	vol5.token.expireTime = time.Now().Add(-10 * time.Second).UnixNano()
	vol5.lock.Unlock()

	err := mockVolumeMgr.applyExpireVolume(ctx, []proto.Vid{1, 3, 5})
	require.NoError(t, err)

	vol1 = mockVolumeMgr.all.getVol(proto.Vid(1))
	vol3 = mockVolumeMgr.all.getVol(proto.Vid(3))
	require.Equal(t, proto.VolumeStatusIdle, vol1.volInfoBase.Status)
	require.Equal(t, proto.VolumeStatusIdle, vol3.volInfoBase.Status)
	require.Equal(t, proto.VolumeStatusIdle, vol5.volInfoBase.Status)

	// apply no longer checks if volume expire twice because master-slave clocks may drift
	vol7 := mockVolumeMgr.all.getVol(proto.Vid(7))
	err = mockVolumeMgr.applyExpireVolume(ctx, []proto.Vid{7})
	require.NoError(t, err)
	require.Equal(t, proto.VolumeStatusIdle, vol7.volInfoBase.Status)

	// vid not exist
	err = mockVolumeMgr.applyExpireVolume(ctx, []proto.Vid{77})
	require.Error(t, err)
}

func TestVolumeMgr_ListAllocatedVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ret := mockVolumeMgr.ListAllocatedVolume(ctx, "127.0.0.1:8080", 1)
	require.NotNil(t, ret)
	require.Equal(t, len(ret.AllocVolumeInfos), 15)

	ret = mockVolumeMgr.ListAllocatedVolume(ctx, "127.0.0.1:8080", 2)
	require.NotNil(t, ret)
	require.Equal(t, len(ret.AllocVolumeInfos), 0)

	ret = mockVolumeMgr.ListAllocatedVolume(ctx, "127.0.0.99:8080", 1)
	require.Nil(t, ret.AllocVolumeInfos)
}

func TestVolumeMgr_ApplyAdminUpdateVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	volInfo := &clustermgr.VolumeInfoBase{
		Vid:         1,
		Used:        1000,
		HealthScore: -1,
	}
	err := mockVolumeMgr.applyAdminUpdateVolume(context.Background(), volInfo)
	require.NoError(t, err)
	ret := mockVolumeMgr.all.getVol(1)
	require.Equal(t, ret.volInfoBase.Used, volInfo.Used)
	require.Equal(t, ret.volInfoBase.HealthScore, volInfo.HealthScore)
}

func TestVolumeMgr_ApplyAdminUpdateVolumeUnit(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	unitInfo := &clustermgr.AdminUpdateUnitArgs{
		Epoch:     1,
		NextEpoch: 2,
		VolumeUnitInfo: clustermgr.VolumeUnitInfo{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1),
			DiskID:     88,
			Compacting: true,
		},
	}
	err := mockVolumeMgr.applyAdminUpdateVolumeUnit(context.Background(), unitInfo)
	require.NoError(t, err)

	vol := mockVolumeMgr.all.getVol(1)
	require.Equal(t, vol.vUnits[1].vuInfo.DiskID, unitInfo.DiskID)
	require.Equal(t, vol.vUnits[1].epoch, unitInfo.Epoch)
	require.Equal(t, vol.vUnits[1].nextEpoch, unitInfo.NextEpoch)
	require.Equal(t, vol.vUnits[1].vuInfo.Compacting, unitInfo.Compacting)

	unitRecord, err := mockVolumeMgr.volumeTbl.GetVolumeUnit(proto.EncodeVuidPrefix(1, 1))
	require.NoError(t, err)
	require.Equal(t, unitRecord.Compacting, unitRecord.Compacting)
	require.Equal(t, unitRecord.Epoch, unitRecord.Epoch)
	require.Equal(t, unitRecord.NextEpoch, unitRecord.NextEpoch)
	require.Equal(t, unitRecord.DiskID, unitRecord.DiskID)

	// failed case,diskid = 0 ,not update
	unitInfo1 := &clustermgr.AdminUpdateUnitArgs{
		Epoch:     1,
		NextEpoch: 2,
		VolumeUnitInfo: clustermgr.VolumeUnitInfo{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1),
			DiskID:     0,
			Compacting: true,
		},
	}
	err = mockVolumeMgr.applyAdminUpdateVolumeUnit(context.Background(), unitInfo1)
	require.NoError(t, err)

	// failed case, vid not exist
	unitInfo1.VolumeUnitInfo.Vuid = proto.EncodeVuid(proto.EncodeVuidPrefix(33, 1), 1)
	err = mockVolumeMgr.applyAdminUpdateVolumeUnit(context.Background(), unitInfo1)
	require.Error(t, err)
}

func TestVolumeMgr_ApplyAdminUpdateVolumeUnit_RouteVersion(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	_, ctx := trace.StartSpanFromContext(context.Background(), "adminUpdateVolumeUnitRouteVersion")

	initialRouteVersion := mockVolumeMgr.routeMgr.GetRouteVersion()
	vol := mockVolumeMgr.all.getVol(1)
	initialVolRouteVersion := vol.volInfoBase.RouteVersion
	oldDiskID := vol.vUnits[1].vuInfo.DiskID

	// test case 1: update with same DiskID, RouteVersion should NOT change
	unitInfoSameDisk := &clustermgr.AdminUpdateUnitArgs{
		Epoch:     1,
		NextEpoch: 2,
		VolumeUnitInfo: clustermgr.VolumeUnitInfo{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1),
			DiskID:     oldDiskID, // same disk
			Compacting: true,
		},
	}
	err := mockVolumeMgr.applyAdminUpdateVolumeUnit(ctx, unitInfoSameDisk)
	require.NoError(t, err)

	vol = mockVolumeMgr.all.getVol(1)
	require.Equal(t, initialVolRouteVersion, vol.volInfoBase.RouteVersion)
	require.Equal(t, initialRouteVersion, mockVolumeMgr.routeMgr.GetRouteVersion())

	// test case 2: update with different DiskID, RouteVersion should change
	newDiskID := proto.DiskID(99)
	unitInfoDiffDisk := &clustermgr.AdminUpdateUnitArgs{
		Epoch:     1,
		NextEpoch: 3,
		VolumeUnitInfo: clustermgr.VolumeUnitInfo{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1),
			DiskID:     newDiskID, // different disk
			Compacting: false,
		},
	}
	err = mockVolumeMgr.applyAdminUpdateVolumeUnit(ctx, unitInfoDiffDisk)
	require.NoError(t, err)

	vol = mockVolumeMgr.all.getVol(1)
	newRouteVersion := mockVolumeMgr.routeMgr.GetRouteVersion()
	require.Greater(t, newRouteVersion, initialRouteVersion)
	require.Equal(t, proto.RouteVersion(newRouteVersion), vol.volInfoBase.RouteVersion)
	require.Equal(t, newDiskID, vol.vUnits[1].vuInfo.DiskID)

	ret, err := mockVolumeMgr.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{
		RouteVersion: proto.RouteVersion(initialRouteVersion),
	})
	require.NoError(t, err)
	require.Greater(t, len(ret.Items), 0)

	found := false
	for _, item := range ret.Items {
		if item.Type == proto.RouteItemTypeUpdateVolume {
			found = true
			break
		}
	}
	require.True(t, found, "should have RouteItemTypeUpdateVolume in route items")
}

func TestVolumeMgr_LockVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	// not allow lock active volume
	args := &clustermgr.LockVolumeArgs{
		Vid:   1,
		Epoch: 0,
	}
	err := mockVolumeMgr.LockVolume(context.Background(), args)
	require.Error(t, err)

	// vid not exist
	args = &clustermgr.LockVolumeArgs{
		Vid:   55,
		Epoch: 0,
	}
	err = mockVolumeMgr.LockVolume(context.Background(), args)
	require.Error(t, err)

	mockRaftServer := mocks.NewMockRaftServer(gomock.NewController(t))
	mockVolumeMgr.raftServer = mockRaftServer
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil)

	// lock locked volume
	vol2 := mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusIdle, vol2.volInfoBase.Status)
	args = &clustermgr.LockVolumeArgs{
		Vid:   2,
		Epoch: 0,
	}
	err = mockVolumeMgr.LockVolume(context.Background(), args)
	require.Error(t, err)
	require.Equal(t, proto.VolumeStatusIdle, vol2.volInfoBase.Status)

	ctxArgs := &ChangeVolStatusCtx{
		Vid:      2,
		TaskID:   uuid.New().String(),
		TaskType: base.VolumeTaskTypeLock,
	}
	err = mockVolumeMgr.applyVolumeTask(context.Background(), ctxArgs)
	require.NoError(t, err)
	vol2 = mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusLock, vol2.volInfoBase.Status)

	args = &clustermgr.LockVolumeArgs{
		Vid:   2,
		Epoch: 0,
	}
	err = mockVolumeMgr.LockVolume(context.Background(), args)
	require.NoError(t, err)
	// volume epoch
	volumeInfo, err := mockVolumeMgr.GetVolumeInfo(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint32(1), volumeInfo.Epoch)
	// volume epoch not match
	args = &clustermgr.LockVolumeArgs{
		Vid:   4,
		Epoch: 2,
	}
	err = mockVolumeMgr.LockVolume(context.Background(), args)
	require.Error(t, err)
}

func TestVolumeMgr_UnlockVolume(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	mockRaftServer := mocks.NewMockRaftServer(gomock.NewController(t))
	mockVolumeMgr.raftServer = mockRaftServer
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	// success case: idle status can unlock
	vol2 := mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusIdle, vol2.volInfoBase.Status)
	args := &clustermgr.UnlockVolumeArgs{
		Vid:   2,
		Epoch: 0,
		Force: false,
	}
	err := mockVolumeMgr.UnlockVolume(context.Background(), args)
	require.NoError(t, err)

	// failed case: active status cannot unlock
	args = &clustermgr.UnlockVolumeArgs{
		Vid:   3,
		Epoch: 0,
		Force: false,
	}
	err = mockVolumeMgr.UnlockVolume(context.Background(), args)
	require.Error(t, err)

	// failed case: vid not exist
	args = &clustermgr.UnlockVolumeArgs{
		Vid:   55,
		Epoch: 0,
		Force: false,
	}
	err = mockVolumeMgr.UnlockVolume(context.Background(), args)
	require.Error(t, err)

	vol2.lock.Lock()
	vol2.volInfoBase.Status = proto.VolumeStatusLock
	vol2.lock.Unlock()
	args = &clustermgr.UnlockVolumeArgs{
		Vid:   2,
		Epoch: 0,
		Force: false,
	}
	err = mockVolumeMgr.UnlockVolume(context.Background(), args)
	require.NoError(t, err)
	args = &clustermgr.UnlockVolumeArgs{
		Vid:   2,
		Epoch: 0,
		Force: true,
	}
	err = mockVolumeMgr.UnlockVolume(context.Background(), args)
	require.NoError(t, err)

	ret, err := mockVolumeMgr.GetVolumeInfo(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, proto.VolumeStatusLock, ret.Status)

	ctxArgs := &ChangeVolStatusCtx{
		Vid:      2,
		TaskID:   uuid.New().String(),
		TaskType: base.VolumeTaskTypeUnlock,
		Epoch:    0,
	}
	err = mockVolumeMgr.applyVolumeTask(context.Background(), ctxArgs)
	require.NoError(t, err)

	ret, err = mockVolumeMgr.GetVolumeInfo(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, proto.VolumeStatusUnlocking, ret.Status)
	require.Equal(t, uint32(1), ret.Epoch)

	// epoch not match
	args = &clustermgr.UnlockVolumeArgs{
		Vid:   2,
		Epoch: 0,
		Force: true,
	}
	err = mockVolumeMgr.UnlockVolume(context.Background(), args)
	require.Error(t, err)

	// volume status id idle , cannot apply volume unlock task, direct return but error is nil
	ctxArgs = &ChangeVolStatusCtx{
		Vid:      2,
		TaskID:   uuid.New().String(),
		TaskType: base.VolumeTaskTypeUnlock,
		Epoch:    1,
	}
	err = mockVolumeMgr.applyVolumeTask(context.Background(), ctxArgs)
	require.NoError(t, err)

	vol2.lock.Lock()
	vol2.volInfoBase.Status = proto.VolumeStatusLock
	vol2.lock.Unlock()
	ctxArgs = &ChangeVolStatusCtx{
		Vid:      2,
		TaskID:   uuid.New().String(),
		TaskType: base.VolumeTaskTypeUnlockForce,
		Epoch:    1,
	}
	err = mockVolumeMgr.applyVolumeTask(context.Background(), ctxArgs)
	require.NoError(t, err)

	ret, err = mockVolumeMgr.GetVolumeInfo(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, proto.VolumeStatusUnlocking, ret.Status)

	// volume epoch not match
	pendingKey := uuid.New().String()
	mockVolumeMgr.pendingEntries.Store(pendingKey, nil)
	ctxArgs = &ChangeVolStatusCtx{
		Vid:           2,
		TaskID:        uuid.New().String(),
		TaskType:      base.VolumeTaskTypeUnlockForce,
		Epoch:         100,
		PendingErrKey: pendingKey,
	}
	err = mockVolumeMgr.applyVolumeTask(context.Background(), ctxArgs)
	require.NoError(t, err)
	v, _ := mockVolumeMgr.pendingEntries.Load(pendingKey)
	require.Equal(t, apierrors.ErrVolumeEpochNotMatch, v)
}

func TestVolumeMgr_Report(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	mockVolumeMgr.Report(context.Background(), "test-region", 1)
}

func TestVolumeMgr_PreAlloc(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	testCases := []struct {
		codemode    codemode.CodeMode
		healthScore int
		count       int
		lenVids     int
		diskLoad    int
	}{
		// first have 8 diskload=0 vid,alloc success
		{codemode: 1, healthScore: 0, count: 2, lenVids: 2, diskLoad: mockVolumeMgr.AllocatableDiskLoadThreshold},
		{codemode: 1, healthScore: 0, count: 1, lenVids: 1, diskLoad: mockVolumeMgr.AllocatableDiskLoadThreshold / 2},
		// prealloc's vid(diskload=0) num not match require,should add diskload
		{codemode: 1, healthScore: 0, count: 2, lenVids: 2, diskLoad: mockVolumeMgr.AllocatableDiskLoadThreshold},
		// first add diskLoad,then add healthScore
		{codemode: 1, healthScore: -3, count: 2, lenVids: 2, diskLoad: mockVolumeMgr.AllocatableDiskLoadThreshold},
		// all volume health not match,not add diskLoad
		{codemode: 1, healthScore: -4, count: 5, lenVids: 0, diskLoad: mockVolumeMgr.AllocatableDiskLoadThreshold / 2},
	}
	for _, testCase := range testCases {
		mockVolumeMgr.all.rangeVol(func(v *volume) error {
			v.volInfoBase.HealthScore = testCase.healthScore
			if v.volInfoBase.Status == proto.VolumeStatusIdle {
				for i := range v.vUnits {
					if v.vid%4 == 0 {
						v.vUnits[i].vuInfo.DiskID = proto.DiskID(101 + i)
					}
				}
			}
			return nil
		})
		vids, diskLoad := mockVolumeMgr.allocator.PreAlloc(context.Background(), testCase.codemode, testCase.count)
		require.Equal(t, testCase.lenVids, len(vids))
		require.LessOrEqual(t, testCase.diskLoad, diskLoad)
		for i := 0; i < len(vids)-1; i++ {
			vol := mockVolumeMgr.all.getVol(vids[i])
			nextVol := mockVolumeMgr.all.getVol(vids[i+1])

			var volDiskLoad, nextVolDiskLoad int
			mockVolumeMgr.allocator.actives.RLock()
			for _, unit := range vol.vUnits {
				volDiskLoad += mockVolumeMgr.allocator.actives.diskLoad[unit.vuInfo.DiskID]
			}
			for _, unit := range nextVol.vUnits {
				nextVolDiskLoad += mockVolumeMgr.allocator.actives.diskLoad[unit.vuInfo.DiskID]
			}
			mockVolumeMgr.allocator.actives.RUnlock()
			require.LessOrEqual(t, nextVol.volInfoBase.HealthScore, vol.volInfoBase.HealthScore)
			require.LessOrEqual(t, volDiskLoad, nextVolDiskLoad)
		}
	}
}

func BenchmarkVolumeMgr_AllocVolume(b *testing.B) {
	mockVolumeMgr, clean := initMockVolumeMgr(b)
	defer clean()

	mockRaftServer := mocks.NewMockRaftServer(gomock.NewController(b))
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockVolumeMgr.raftServer = mockRaftServer
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	mode := codemode.EC15P12
	args := &AllocVolumeCtx{
		Vids:       []proto.Vid{2, 4},
		Host:       "127.0.0.1:8080",
		ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
	}
	allocVolumeInfos := []clustermgr.AllocVolumeInfo{
		{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:         1,
					HealthScore: 0,
				},
			},
			Token:      "127.0.0.1:8080;1",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
		},
		{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:         3,
					HealthScore: 0,
				},
			},
			Token:      "127.0.0.3:8080;3",
			ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
		},
	}
	volInfos := &clustermgr.AllocatedVolumeInfos{AllocVolumeInfos: allocVolumeInfos}
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, data []byte) error {
		mockVolumeMgr.pendingEntries.Range(func(key, value interface{}) bool {
			mockVolumeMgr.pendingEntries.Store(key, volInfos)
			return true
		})
		return nil
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		}
	})
}

// TestPreAlloc_HighWatermarkFallback verifies the two-pass watermark behavior:
//  1. When allocating 2 volumes from a pool of 4 high-watermark + 2 low-watermark
//     volumes (all other conditions equal), the 2 low-watermark volumes are returned.
//  2. When allocating all 6, the full set is returned via graceful fallback.
func TestPreAlloc_HighWatermarkFallback(t *testing.T) {
	testConfig.checkAndFix()
	codeModes := make(map[codemode.CodeMode]codeModeConf)
	for _, policy := range testConfig.CodeModePolicies {
		cm := policy.ModeName.GetCodeMode()
		codeModes[cm] = codeModeConf{
			mode:      cm,
			sizeRatio: policy.SizeRatio,
			tactic:    cm.Tactic(),
			enable:    policy.Enable,
		}
	}
	mode := codemode.EC15P12

	newAllocator := func() *volumeAllocator {
		a := newVolumeAllocator(allocConfig{
			codeModes:                    codeModes,
			allocatableSize:              testConfig.AllocatableSize,
			allocFactor:                  testConfig.AllocFactor,
			allocatableDiskLoadThreshold: testConfig.AllocatableDiskLoadThreshold,
			shardNum:                     testConfig.ShardNum,
			diskUsageThreshold:           0.85,
		})
		// vid 1..4: high-watermark disks (ID <= 1000); vid 5..6: low-watermark disks (ID > 1000).
		const watermarkBoundary = proto.DiskID(1000)
		allVols := generateVolume(mode, 6, 1)
		for i, vol := range allVols {
			for j := range vol.vUnits {
				if i < 4 {
					vol.vUnits[j].vuInfo.DiskID = proto.DiskID(j + 1)
				} else {
					vol.vUnits[j].vuInfo.DiskID = watermarkBoundary + proto.DiskID(j+1)
				}
			}
			vol.volInfoBase.Status = proto.VolumeStatusIdle
			a.idles[mode].addAllocatable(vol)
		}
		// mark disks with ID <= watermarkBoundary as high-usage via the heartbeat-driven path.
		for _, vol := range allVols {
			for _, unit := range vol.vUnits {
				var ratio float64
				if unit.vuInfo.DiskID <= watermarkBoundary {
					ratio = 0.9 // above threshold
				} else {
					ratio = 0.5 // below threshold
				}
				a.UpdateDiskHighUsage(unit.vuInfo.DiskID, ratio)
			}
		}
		return a
	}

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	lowWatermarkVids := map[proto.Vid]struct{}{5: {}, 6: {}}

	t.Run("prefer low-watermark volumes", func(t *testing.T) {
		ret, _ := newAllocator().PreAlloc(ctx, mode, 2)
		require.Equal(t, 2, len(ret))
		for _, vid := range ret {
			require.Contains(t, lowWatermarkVids, vid, "expected only low-watermark vids, got vid=%d", vid)
		}
	})

	t.Run("fallback allocates all volumes", func(t *testing.T) {
		ret, _ := newAllocator().PreAlloc(ctx, mode, 6)
		require.Equal(t, 6, len(ret))
	})

	t.Run("disabled when threshold is zero", func(t *testing.T) {
		// diskUsageThreshold=0 → skipDiskUsageCheck=true from the start;
		// even disks at 90% usage are eligible without fallback.
		a := newVolumeAllocator(allocConfig{
			codeModes:                    codeModes,
			allocatableSize:              testConfig.AllocatableSize,
			allocFactor:                  testConfig.AllocFactor,
			allocatableDiskLoadThreshold: testConfig.AllocatableDiskLoadThreshold,
			shardNum:                     testConfig.ShardNum,
			diskUsageThreshold:           0,
		})
		vols := generateVolume(mode, 4, 1)
		for _, vol := range vols {
			vol.volInfoBase.Status = proto.VolumeStatusIdle
			a.idles[mode].addAllocatable(vol)
			for _, unit := range vol.vUnits {
				a.UpdateDiskHighUsage(unit.vuInfo.DiskID, 0.9)
			}
		}
		ret, _ := a.PreAlloc(ctx, mode, 2)
		require.Equal(t, 2, len(ret))
	})

	t.Run("ratio at threshold is not high", func(t *testing.T) {
		// ratio > threshold is strict; ratio == threshold must NOT be treated as high.
		const threshold = 0.85
		a := newVolumeAllocator(allocConfig{
			codeModes:                    codeModes,
			allocatableSize:              testConfig.AllocatableSize,
			allocFactor:                  testConfig.AllocFactor,
			allocatableDiskLoadThreshold: testConfig.AllocatableDiskLoadThreshold,
			shardNum:                     testConfig.ShardNum,
			diskUsageThreshold:           threshold,
		})
		vols := generateVolume(mode, 4, 1)
		for _, vol := range vols {
			vol.volInfoBase.Status = proto.VolumeStatusIdle
			a.idles[mode].addAllocatable(vol)
			for _, unit := range vol.vUnits {
				a.UpdateDiskHighUsage(unit.vuInfo.DiskID, threshold)
			}
		}
		ret, _ := a.PreAlloc(ctx, mode, 2)
		require.Equal(t, 2, len(ret))
	})

	t.Run("disk usage update overrides previous state", func(t *testing.T) {
		// EC15P12: count = N+M+L-PutQuorum = 27-24 = 3 (tolerance).
		// hasHighUsageDisk returns true only when > count (i.e. ≥ 4) disks are high
		// (count<0 condition), meaning fewer than PutQuorum disks remain available.
		const threshold = 0.85
		const (
			diskA = proto.DiskID(9001)
			diskB = proto.DiskID(9002)
			diskC = proto.DiskID(9003)
			diskD = proto.DiskID(9004)
		)
		a := newVolumeAllocator(allocConfig{
			codeModes:                    codeModes,
			allocatableSize:              testConfig.AllocatableSize,
			allocFactor:                  testConfig.AllocFactor,
			allocatableDiskLoadThreshold: testConfig.AllocatableDiskLoadThreshold,
			shardNum:                     testConfig.ShardNum,
			diskUsageThreshold:           threshold,
		})
		units := []*volumeUnit{
			{vuInfo: &clustermgr.VolumeUnitInfo{DiskID: diskA}},
			{vuInfo: &clustermgr.VolumeUnitInfo{DiskID: diskB}},
			{vuInfo: &clustermgr.VolumeUnitInfo{DiskID: diskC}},
			{vuInfo: &clustermgr.VolumeUnitInfo{DiskID: diskD}},
		}

		// 3 high disks == count(3): count reaches 0 but not <0, still allowed.
		a.UpdateDiskHighUsage(diskA, 0.9)
		a.UpdateDiskHighUsage(diskB, 0.9)
		a.UpdateDiskHighUsage(diskC, 0.9)
		require.False(t, a.hasHighUsageDisk(units, mode))

		// 4th disk also high → count goes negative → blocked.
		a.UpdateDiskHighUsage(diskD, 0.9)
		require.True(t, a.hasHighUsageDisk(units, mode))

		// Drop diskA below threshold: only 3 of 4 remain high → unblocked.
		a.UpdateDiskHighUsage(diskA, 0.3)
		require.False(t, a.hasHighUsageDisk(units, mode))
	})
}

func BenchmarkRunCIVolumeMgr_PreAllocVolume(b *testing.B) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	mode := codemode.EC15P12
	testConfig.checkAndFix()
	codeModes := make(map[codemode.CodeMode]codeModeConf)
	for _, policy := range testConfig.CodeModePolicies {
		codeMode := policy.ModeName.GetCodeMode()
		modeConf := codeModeConf{
			mode:      codeMode,
			sizeRatio: policy.SizeRatio,
			tactic:    codeMode.Tactic(),
			enable:    policy.Enable,
		}
		codeModes[codeMode] = modeConf
	}
	allocConfig := allocConfig{
		codeModes:                    codeModes,
		allocatableSize:              testConfig.AllocatableSize,
		allocFactor:                  testConfig.AllocFactor,
		allocatableDiskLoadThreshold: testConfig.AllocatableDiskLoadThreshold,
		shardNum:                     testConfig.ShardNum,
	}
	volAllocator := newVolumeAllocator(allocConfig)
	vols := generateVolume(mode, 200000, 1)
	for _, vol := range vols {
		if vol.volInfoBase.Status == proto.VolumeStatusIdle {
			volAllocator.idles[mode].addAllocatable(vol)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ret, _ := volAllocator.PreAlloc(ctx, mode, 2)
		require.Equal(b, 2, len(ret))
	}
}

func BenchmarkVolumeMgr_PreRetainVolume(b *testing.B) {
	mockVolumeMgr, clean := initMockVolumeMgr(b)
	defer clean()

	tokens := []string{}
	for i := 0; i < 20; i++ {
		tokens = append(tokens, "127.0.0.1:8080;"+strconv.Itoa(i))
	}
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
		}
	})
}

func BenchmarkVolumeMgr_ListVolumeInfo(b *testing.B) {
	mockVolumeMgr, clean := initMockVolumeMgr(b)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "ListVolumeInfo")
	args := &clustermgr.ListVolumeArgs{
		Marker: 1,
		Count:  100,
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mockVolumeMgr.ListVolumeInfo(ctx, args)
		}
	})
}

func TestVolumeStat(t *testing.T) {
	sliceNum := uint32(4)
	freezeSize := uint64(100)
	allocatableSize := uint64(500)

	stat := newVolumeStat(sliceNum, freezeSize, allocatableSize)
	require.NotNil(t, stat)
	require.Equal(t, sliceNum, stat.num)
	require.Equal(t, freezeSize, stat.freezeSizeThreshold)
	require.Equal(t, allocatableSize, stat.allocatableSizeThreshold)
	require.Equal(t, uint64(0), stat.getWriteSpace())

	// test addSize: new vid with idle status and freeSize > allocatableSizeThreshold
	vid1 := proto.Vid(1)
	freeSize1 := uint64(1000)
	stat.addSize(vid1, proto.VolumeStatusIdle, freeSize1)
	require.Equal(t, freeSize1-freezeSize, stat.getWriteSpace())

	// test addSize: same vid with idle status, freeSize increase
	newFreeSize1 := uint64(1200)
	stat.addSize(vid1, proto.VolumeStatusIdle, newFreeSize1)
	require.Equal(t, freeSize1-freezeSize+(newFreeSize1-freeSize1), stat.getWriteSpace())

	// test addSize: same vid with idle status, freeSize decrease
	decreasedFreeSize := uint64(1100)
	expectedSpace := stat.getWriteSpace() - (newFreeSize1 - decreasedFreeSize)
	stat.addSize(vid1, proto.VolumeStatusIdle, decreasedFreeSize)
	require.Equal(t, expectedSpace, stat.getWriteSpace())

	// test addSize: vid becomes non-idle (remove from stats)
	currentSpace := stat.getWriteSpace()
	stat.addSize(vid1, proto.VolumeStatusActive, decreasedFreeSize)
	require.Equal(t, currentSpace-(decreasedFreeSize-freezeSize), stat.getWriteSpace())

	// test addSize: new vid with freeSize <= allocatableSizeThreshold (should not add)
	vid2 := proto.Vid(2)
	smallFreeSize := uint64(400)
	spaceBeforeAdd := stat.getWriteSpace()
	stat.addSize(vid2, proto.VolumeStatusIdle, smallFreeSize)
	require.Equal(t, spaceBeforeAdd, stat.getWriteSpace())

	// test addSize: new vid with non-idle status (should not add)
	vid3 := proto.Vid(3)
	spaceBeforeAdd = stat.getWriteSpace()
	stat.addSize(vid3, proto.VolumeStatusActive, freeSize1)
	require.Equal(t, spaceBeforeAdd, stat.getWriteSpace())
}

func makeBenchVolumeMgr(n int) *VolumeMgr {
	num := uint32(testConfig.VolumeSliceMapNum)
	shards := &shardedVolumes{
		num:   num,
		m:     make(map[uint32]map[proto.Vid]*volume, num),
		locks: make(map[uint32]*sync.RWMutex, num),
	}
	for i := uint32(0); i < num; i++ {
		shards.m[i] = make(map[proto.Vid]*volume)
		shards.locks[i] = &sync.RWMutex{}
	}
	v := &VolumeMgr{all: shards}
	for _, vol := range generateVolume(codemode.EC12P9, n, 1) {
		v.all.putVol(vol) //nolint:errcheck
	}
	return v
}

func BenchmarkRangeUpdateVolume(b *testing.B) {
	cases := []struct {
		name  string
		count int
	}{
		{"1w", 10_000},
		{"10w", 100_000},
		{"100w", 1_000_000},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			mgr := makeBenchVolumeMgr(tc.count)
			ctx := context.Background()

			cache := make(map[proto.Vid]*clustermgr.VolumeBasic, tc.count)
			mgr.RangeUpdateVolume(ctx, cache)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				mgr.RangeUpdateVolume(ctx, cache)
			}
		})
	}
}

// ---- healths accounting ----

// buildIdleVolumes constructs an idleVolumes for EC15P12 with the given shardNum.
func buildIdleVolumes(shardNum int) *idleVolumes {
	mode := codemode.EC15P12
	shards := make([]*list.List, shardNum)
	for i := range shards {
		shards[i] = list.New()
	}
	return &idleVolumes{
		m:                 make(map[proto.Vid]idleItem),
		allocatableShards: shards,
		notAllocatable:    list.New(),
		shardNum:          shardNum,
		healths:           make([]int, mode.GetShardNum()+1),
	}
}

func makeVol(vid proto.Vid, health int) *volume {
	return &volume{
		vid: vid,
		volInfoBase: clustermgr.VolumeInfoBase{
			Vid:         vid,
			HealthScore: health,
			Status:      proto.VolumeStatusIdle,
			Free:        defaultChunkSize * 12,
		},
	}
}

func TestHealthStat_AddAllocatable(t *testing.T) {
	iv := buildIdleVolumes(defaultShardNum)

	iv.addAllocatable(makeVol(1, 0))
	iv.addAllocatable(makeVol(2, -1))
	iv.addAllocatable(makeVol(3, 0))

	require.Equal(t, 2, iv.healths[0]) // health=0: vid1, vid3
	require.Equal(t, 1, iv.healths[1]) // health=-1: vid2
	require.Equal(t, 3-0, iv.statAllocatableNum())
}

func TestHealthStat_AddNotAllocatable(t *testing.T) {
	iv := buildIdleVolumes(defaultShardNum)

	iv.addNotAllocatable(makeVol(1, 0))
	iv.addNotAllocatable(makeVol(2, -2))

	// healths covers all idle entries, including notAllocatable
	require.Equal(t, 1, iv.healths[0])
	require.Equal(t, 1, iv.healths[2])
	// statAllocatableNum excludes notAllocatable
	require.Equal(t, 0, iv.statAllocatableNum())
}

func TestHealthStat_Delete(t *testing.T) {
	iv := buildIdleVolumes(defaultShardNum)

	iv.addAllocatable(makeVol(1, 0))
	iv.addAllocatable(makeVol(2, 0))
	iv.delete(proto.Vid(1))

	require.Equal(t, 1, iv.healths[0])
}

func TestHealthStat_AllocFromOptions(t *testing.T) {
	iv := buildIdleVolumes(defaultShardNum)

	iv.addAllocatable(makeVol(1, 0))
	iv.addAllocatable(makeVol(2, 0))
	iv.addAllocatable(makeVol(3, -1))

	// alloc vid1 and vid3
	got := iv.allocFromOptions([]proto.Vid{1, 3}, 2)
	require.Equal(t, []proto.Vid{1, 3}, got)

	// healths must decrease for each allocated vid
	require.Equal(t, 1, iv.healths[0]) // only vid2 remains
	require.Equal(t, 0, iv.healths[1]) // vid3 removed
	require.Equal(t, 1, iv.statAllocatableNum())
}

func TestHealthStat_MoveAllocatableToNotAllocatable(t *testing.T) {
	iv := buildIdleVolumes(defaultShardNum)

	// add as allocatable then re-add as notAllocatable (health degrades)
	iv.addAllocatable(makeVol(1, 0))
	require.Equal(t, 1, iv.healths[0])

	// health drops, move to notAllocatable with health=-1
	vol := makeVol(1, -1)
	iv.addNotAllocatable(vol)

	// old entry (health=0) removed, new entry (health=-1) added
	require.Equal(t, 0, iv.healths[0])
	require.Equal(t, 1, iv.healths[1])
	require.Equal(t, 0, iv.statAllocatableNum())
}

func TestStatHealthyAllocatable_PrefixSum(t *testing.T) {
	iv := buildIdleVolumes(defaultShardNum)

	iv.addAllocatable(makeVol(1, 0))
	iv.addAllocatable(makeVol(2, 0))
	iv.addAllocatable(makeVol(3, -1))
	iv.addAllocatable(makeVol(4, -2))

	ps := iv.statHealthyAllocatable()
	// ps[0] = count(health=0) = 2
	require.Equal(t, 2, ps[0])
	// ps[1] = count(health=0) + count(health=-1) = 3
	require.Equal(t, 3, ps[1])
	// ps[2] = 3 + count(health=-2) = 4
	require.Equal(t, 4, ps[2])
}

// ---- PreRetainVolume F1 degraded retain ----

func TestPreRetainVolume_DegradedRetain_NormalCase(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// enough health=0 volumes → threshold stays at 0 → health=-1 volume NOT retained
	mode := codemode.EC15P12
	conf := mockVolumeMgr.codeMode[mode]
	conf.sizeRatio = 1.0
	mockVolumeMgr.codeMode[mode] = conf
	mockVolumeMgr.MinAllocableHealthVolumeCount = 1
	mockVolumeMgr.EnableDegradeRetain = true // feature enabled; no degradation because healthy volumes are sufficient

	// vid=1 is active with health=-1
	vol1 := mockVolumeMgr.all.getVol(proto.Vid(1))
	vol1.lock.Lock()
	vol1.volInfoBase.HealthScore = -1
	vol1.lock.Unlock()

	// ensure at least one health=0 allocatable idle volume exists
	mockVolumeMgr.all.rangeVol(func(v *volume) error {
		if v.volInfoBase.Status == proto.VolumeStatusIdle {
			v.lock.Lock()
			v.volInfoBase.HealthScore = 0
			v.lock.Unlock()
			mockVolumeMgr.allocator.idles[mode].addAllocatable(v)
		}
		return nil
	})

	tokens := []string{"127.0.0.1:8080;1"}
	ret, err := mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)
	// health=-1 should NOT be retained when threshold=0 and ratio disables degradation
	require.Nil(t, ret)
}

func TestPreRetainVolume_DegradedRetain_NoHealthyVolumes(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	mode := codemode.EC15P12
	tactic := mode.Tactic()
	degradedThreshold := tactic.PutQuorum - mode.GetShardNum() // -3 for EC15P12

	conf := mockVolumeMgr.codeMode[mode]
	conf.sizeRatio = 1.0
	mockVolumeMgr.codeMode[mode] = conf
	// set MinAllocableHealthVolumeCount higher than actual health=0 count (0 health=0 volumes)
	mockVolumeMgr.MinAllocableHealthVolumeCount = 10
	mockVolumeMgr.EnableDegradeRetain = true

	// drain all health=0 from allocator by moving idle volumes to notAllocatable
	iv := mockVolumeMgr.allocator.idles[mode]
	iv.Lock()
	iv.healths[0] = 0
	iv.Unlock()

	// vid=1: active, health=-2 (within quorum lower bound -3)
	vol1 := mockVolumeMgr.all.getVol(proto.Vid(1))
	vol1.lock.Lock()
	vol1.volInfoBase.HealthScore = -2
	vol1.lock.Unlock()

	tokens := []string{"127.0.0.1:8080;1"}
	ret, err := mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Equal(t, 1, len(ret.RetainVolTokens))

	// verify degraded threshold is correct
	require.Equal(t, degradedThreshold, -3)
}

func TestPreRetainVolume_DegradedRetain_HealthBelowQuorum(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	mode := codemode.EC15P12
	conf := mockVolumeMgr.codeMode[mode]
	conf.sizeRatio = 1.0
	mockVolumeMgr.codeMode[mode] = conf
	mockVolumeMgr.MinAllocableHealthVolumeCount = 10
	mockVolumeMgr.EnableDegradeRetain = true

	mockVolumeMgr.allocator.idles[mode].Lock()
	mockVolumeMgr.allocator.idles[mode].healths[0] = 0
	mockVolumeMgr.allocator.idles[mode].Unlock()

	// vid=3: active, health=-4 (below quorum lower bound -3) → must NOT be retained
	vol3 := mockVolumeMgr.all.getVol(proto.Vid(3))
	vol3.lock.Lock()
	vol3.volInfoBase.HealthScore = -4
	vol3.lock.Unlock()

	tokens := []string{"127.0.0.1:8080;3"}
	ret, err := mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)
	require.Nil(t, ret)
}

func TestPreRetainVolume_DegradedRetain_FeatureDisabled(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	mode := codemode.EC15P12
	conf := mockVolumeMgr.codeMode[mode]
	conf.sizeRatio = 1.0
	mockVolumeMgr.codeMode[mode] = conf
	// EnableDegradeRetain=false → skip calculateThreshold entirely → F1 disabled
	mockVolumeMgr.MinAllocableHealthVolumeCount = 10
	mockVolumeMgr.EnableDegradeRetain = false

	mockVolumeMgr.allocator.idles[mode].Lock()
	mockVolumeMgr.allocator.idles[mode].healths[0] = 0
	mockVolumeMgr.allocator.idles[mode].Unlock()

	// health=-2 volume, would be retained if feature were on
	vol1 := mockVolumeMgr.all.getVol(proto.Vid(1))
	vol1.lock.Lock()
	vol1.volInfoBase.HealthScore = -2
	vol1.lock.Unlock()

	// EnableDegradeRetain=false skips calculateThreshold,
	// so threshold stays at RetainThreshold=0, health=-2 NOT retained
	tokens := []string{"127.0.0.1:8080;1"}
	ret, err := mockVolumeMgr.PreRetainVolume(ctx, tokens, "127.0.0.1:8080")
	require.NoError(t, err)
	require.Nil(t, ret)
}

// ---- StatHealthyAllocable used by F2/F3 ----

func TestStatHealthyAllocable_AfterAlloc(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	mode := codemode.EC15P12

	// set all idle volumes to health=0, rebuild healths
	mockVolumeMgr.all.rangeVol(func(v *volume) error {
		if v.volInfoBase.Status == proto.VolumeStatusIdle {
			v.lock.Lock()
			v.volInfoBase.HealthScore = 0
			v.lock.Unlock()
			mockVolumeMgr.allocator.idles[mode].addAllocatable(v)
		}
		return nil
	})

	beforeCounts := mockVolumeMgr.allocator.StatHealthyAllocable()
	before := beforeCounts[mode]
	require.Greater(t, before, 0)

	// simulate allocation: take two volumes via allocFromOptions
	idleVols := mockVolumeMgr.allocator.idles[mode]
	var twoVids []proto.Vid
	idleVols.RLock()
	for vid := range idleVols.m {
		if idleVols.m[vid].head != idleVols.notAllocatable {
			twoVids = append(twoVids, vid)
			if len(twoVids) == 2 {
				break
			}
		}
	}
	idleVols.RUnlock()

	idleVols.allocFromOptions(twoVids, 2)

	afterCounts := mockVolumeMgr.allocator.StatHealthyAllocable()
	require.Equal(t, before-2, afterCounts[mode])
}

func TestStatHealthyAllocable_HealthDegraded(t *testing.T) {
	// Use a standalone idleVolumes to avoid conflicts with initMockVolumeMgr state.
	iv := buildIdleVolumes(defaultShardNum)

	iv.addAllocatable(makeVol(1, -1))
	iv.addAllocatable(makeVol(2, -1))
	iv.addAllocatable(makeVol(3, -2))

	// health=0 count must be 0
	require.Equal(t, 0, iv.statHealthyAllocatableNum())

	// StatHealthyAllocable via volumeAllocator
	mode := codemode.EC15P12
	cfg := allocConfig{
		codeModes: map[codemode.CodeMode]codeModeConf{
			mode: {mode: mode, tactic: mode.Tactic()},
		},
		allocatableSize: defaultChunkSize,
		shardNum:        defaultShardNum,
	}
	alloc := newVolumeAllocator(cfg)
	alloc.idles[mode].addAllocatable(makeVol(10, -1))
	alloc.idles[mode].addAllocatable(makeVol(11, -2))

	counts := alloc.StatHealthyAllocable()
	require.Equal(t, 0, counts[mode])
}

// TestScenario_LargeCluster_DiskCutReadonly simulates a scaled-down (1:50) large-cluster scenario:
//
// Original scale: 30 machines, 1200+ disks (20T each), ~10000 EC15P12 volumes (analogous to 12+9),
// ~5000 idle allocatable volumes (health=0), ~1000 active volumes.
// Event: 80% of high-watermark disks are set to read-only, causing ~80% of idle volumes to
// degrade from health=0 to health=-1, dropping the health=0 idle count below threshold.
// F1 trigger condition: prefix_sum[abs(RetainThreshold)] = health=0 count < minCount.
//
// Verifies three behaviors:
//   - F1: adaptive retention — prevents active vols from losing their lease (write cliff) when
//     the retain threshold is too strict for degraded volumes
//   - Allocator continuity: degraded volumes remain in the allocatable pool, writes can continue
//   - F2/F3: health-aware volume creation — scarcity detection triggers supplementary creation
func TestScenario_LargeCluster_DiskCutReadonly(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	mode := codemode.EC15P12
	tactic := mode.Tactic()
	shardNum := tactic.N + tactic.M + tactic.L
	// F1 degraded retain threshold = PutQuorum - ShardNum (lowest score still writable)
	degradedThreshold := tactic.PutQuorum - shardNum

	// Scale 1:50 from original cluster
	const (
		newIdleVols       = 100 // 5000/50
		newActiveVols     = 20  // 1000/50
		idleDegradedCount = 100 // all new idle vols degrade health=0 -> -1 when disks go read-only
		healthThreshold   = 40  // MinAllocableHealthVolumeCount: F1/F2/F3 trigger threshold
		existingActiveNum = 15  // initMockVolumeMgr seeds 15 active vols (odd vids), registered in actives.counts
		startVidIdle      = 1000
		startVidActive    = 1100
		testHost          = "127.0.0.1:8080"
	)

	modeConf := mockVolumeMgr.codeMode[mode]
	modeConf.sizeRatio = 1.0
	mockVolumeMgr.codeMode[mode] = modeConf
	mockVolumeMgr.MinAllocableHealthVolumeCount = healthThreshold
	mockVolumeMgr.EnableDegradeRetain = false // F1 disabled initially
	mockVolumeMgr.CheckHealthyVolumeIntervalS = 1
	mockVolumeMgr.RetainThreshold = 0

	// --- build initial state ---
	// inject 100 idle volumes (health=0) into the allocator
	for i := 0; i < newIdleVols; i++ {
		vid := proto.Vid(startVidIdle + i)
		vol := &volume{
			vid: vid,
			volInfoBase: clustermgr.VolumeInfoBase{
				Vid:         vid,
				CodeMode:    mode,
				HealthScore: 0,
				Status:      proto.VolumeStatusIdle,
				Free:        defaultChunkSize * 12,
				Total:       defaultChunkSize * 12,
			},
		}
		require.NoError(t, mockVolumeMgr.all.putVol(vol))
		mockVolumeMgr.allocator.idles[mode].addAllocatable(vol)
	}

	// inject 20 active volumes (health=0) with valid tokens, register in actives.counts
	// via VolumeStatusActiveCallback so calculateThreshold sees the correct active count
	for i := 0; i < newActiveVols; i++ {
		vid := proto.Vid(startVidActive + i)
		tok := proto.EncodeToken(testHost, vid)
		vol := &volume{
			vid: vid,
			volInfoBase: clustermgr.VolumeInfoBase{
				Vid:         vid,
				CodeMode:    mode,
				HealthScore: 0,
				Status:      proto.VolumeStatusActive,
				Free:        defaultChunkSize * 12,
				Total:       defaultChunkSize * 12,
			},
			token: &token{
				vid:        vid,
				tokenID:    tok,
				expireTime: time.Now().Add(time.Hour).UnixNano(),
			},
		}
		require.NoError(t, mockVolumeMgr.all.putVol(vol))
		require.NoError(t, mockVolumeMgr.allocator.VolumeStatusActiveCallback(ctx, vol))
	}

	// --- Phase 1: verify pre-cut state ---
	// initMockVolumeMgr seeds 15 idle volumes (even vids 0,2,...,28), all health=0
	existingIdle := volumeCount / 2 // 15
	// total active after registration: 15 original + 20 new
	totalActive := existingActiveNum + newActiveVols // 35
	initialHealthy := mockVolumeMgr.allocator.StatHealthyAllocable()[mode]
	require.Equal(t, existingIdle+newIdleVols, initialHealthy)
	require.Greater(t, initialHealthy, healthThreshold,
		"pre-cut: health=0 count must exceed threshold")
	t.Logf("[Phase 1] Pre-cut: health=0 idle=%d > threshold=%d, active=%d — sufficient",
		initialHealthy, healthThreshold, totalActive)

	// --- Phase 2: simulate high-watermark disks going read-only ---
	// All 100 new idle vols have units on affected disks; each loses one shard, health=0 -> -1.
	// Only the 15 original idle vols remain health=0 (retain=15 < active=35 → F1 trigger condition).
	for i := 0; i < idleDegradedCount; i++ {
		vid := proto.Vid(startVidIdle + i)
		vol := mockVolumeMgr.all.getVol(vid)
		require.NotNil(t, vol)
		vol.volInfoBase.HealthScore = -1
		require.NoError(t, mockVolumeMgr.allocator.VolumeFreeHealthCallback(ctx, vol))
	}
	// 16 active vols on the same disks also degrade
	activeDegradedCount := newActiveVols * 4 / 5 // 16
	for i := 0; i < activeDegradedCount; i++ {
		vid := proto.Vid(startVidActive + i)
		vol := mockVolumeMgr.all.getVol(vid)
		require.NotNil(t, vol)
		vol.volInfoBase.HealthScore = -1
	}

	postCutHealthy := mockVolumeMgr.allocator.StatHealthyAllocable()[mode]
	postCutAllocatable := mockVolumeMgr.allocator.StatAllocatable()[mode]

	// retain = health=0 idle vols = only the 15 original idle vols
	expectedHealthy := existingIdle // 15
	require.Equal(t, expectedHealthy, postCutHealthy,
		"post-cut: only original idle vols remain health=0")
	require.Less(t, postCutHealthy, healthThreshold,
		"post-cut: health=0 count must be below threshold to trigger F2/F3")
	// F1 trigger: retain(15) <= active(35) and retain(15) <= minCount(40)
	require.Less(t, postCutHealthy, totalActive,
		"post-cut: retain < active, F1 degraded retain triggers")
	// degraded vols: health=-1 >= allocatableThreshold(-3), still in the allocatable pool
	require.Equal(t, existingIdle+newIdleVols, postCutAllocatable,
		"post-cut: all idle vols remain allocatable (health=-1 still writable)")
	t.Logf("[Phase 2] After disk cut: retain(health=0)=%d, active=%d, total_allocatable=%d — F1 triggers",
		postCutHealthy, totalActive, postCutAllocatable)

	// --- Phase 3a: without F1 — retention bottleneck (write cliff) ---
	tokens := make([]string, newActiveVols)
	for i := 0; i < newActiveVols; i++ {
		tokens[i] = proto.EncodeToken(testHost, proto.Vid(startVidActive+i))
	}

	mockVolumeMgr.EnableDegradeRetain = false
	ret, err := mockVolumeMgr.PreRetainVolume(ctx, tokens, testHost)
	require.NoError(t, err)
	retainedNoF1 := 0
	if ret != nil {
		retainedNoF1 = len(ret.RetainVolTokens)
	}
	activeHealthyCount := newActiveVols - activeDegradedCount // 4
	require.Equal(t, activeHealthyCount, retainedNoF1,
		"without F1: only health=0 active vols pass RetainThreshold=0")
	t.Logf("[Phase 3a] Without F1: retained %d/%d active vols — WRITE CLIFF: %d vols dropped",
		retainedNoF1, newActiveVols, activeDegradedCount)

	// --- Phase 3b: F1 enabled — degraded threshold reduces the write cliff ---
	// retain(15) <= active(35): ratio = (active-retain)/(active+retain) = 20/50 = 0.4 per vol.
	// P(none of 16 health=-1 vols retained) = 0.6^16 ≈ 0.03%, reliable for CI.
	mockVolumeMgr.EnableDegradeRetain = true
	ret, err = mockVolumeMgr.PreRetainVolume(ctx, tokens, testHost)
	require.NoError(t, err)
	retainedF1 := 0
	if ret != nil {
		retainedF1 = len(ret.RetainVolTokens)
	}
	require.Greater(t, retainedF1, retainedNoF1,
		"F1 must increase retention count when health>=-1 idle vols are scarce")
	t.Logf("[Phase 3b] With F1: retained %d/%d active vols (vs %d without F1)",
		retainedF1, newActiveVols, retainedNoF1)
	t.Logf("  degraded threshold = %d (PutQuorum=%d, ShardNum=%d)",
		degradedThreshold, tactic.PutQuorum, shardNum)

	// --- Phase 4: verify allocator health accounting ---
	// After disk cut: healths[0]=15 (original idle only), healths[1]=100 (all new idle)
	// prefix_sum[0]=15, prefix_sum[1]=115
	prefixSum := mockVolumeMgr.allocator.StatHealthyAllocables()[mode]
	require.Equal(t, expectedHealthy, prefixSum[0],
		"prefix_sum[0] must equal health=0 count (retain)")
	require.Equal(t, existingIdle+newIdleVols, prefixSum[1],
		"prefix_sum[1] must equal total allocatable (health=0 + health=-1)")
	t.Logf("[Phase 4] Prefix sum: [0]=%d (retain/health=0), [1]=%d (all allocatable)",
		prefixSum[0], prefixSum[1])

	// --- Phase 5: F2/F3 supplementary creation trigger ---
	healthyNow := mockVolumeMgr.allocator.StatHealthyAllocable()[mode]
	supplement := mockVolumeMgr.MinAllocableHealthVolumeCount - healthyNow
	require.Greater(t, supplement, 0, "F2: must trigger creation when health=0 < threshold")
	require.Equal(t, healthThreshold-expectedHealthy, supplement,
		"supplement = threshold - current_healthy")
	t.Logf("[Phase 5] F2/F3: health=0=%d < threshold=%d → %d new vols scheduled (after %ds window)",
		healthyNow, healthThreshold, supplement, mockVolumeMgr.CheckHealthyVolumeIntervalS)

	t.Logf("\n=== Scenario Summary ===")
	t.Logf("Scale 1:50 | EC15P12 | degradedThreshold=%d", degradedThreshold)
	t.Logf("Pre-cut:  health=0 idle=%d | Post-cut: retain(health=0)=%d, active=%d",
		initialHealthy, postCutHealthy, totalActive)
	t.Logf("F1: ratio=(active-retain)/(active+retain)=(%d-%d)/(%d+%d)=%.2f",
		totalActive, expectedHealthy, totalActive, expectedHealthy,
		float64(totalActive-expectedHealthy)/float64(totalActive+expectedHealthy))
	t.Logf("    without=%d retained, with=%d retained (+%d saved from write cliff)",
		retainedNoF1, retainedF1, retainedF1-retainedNoF1)
	t.Logf("F2/F3: supplement=%d vols creation triggered", supplement)
}
