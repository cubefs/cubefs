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
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/mock"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	mockVolumeMgr *VolumeMgr
	volumeDB      *volumedb.VolumeDB
	normalDB      *normaldb.NormalDB
	volTable      *volumedb.VolumeTable

	volumeCount             = 30
	defaultChunkSize uint64 = 1 << 34
	volumeDBPPath           = "/tmp/volumedb/"
	normalDBPath            = "/tmp/normaldb/"
	testConfig              = VolumeMgrConfig{
		IDC:                          []string{"z0", "z1", "z2"},
		RetainTimeS:                  100,
		ApplyConcurrency:             10,
		FlushIntervalS:               100,
		VolumeSliceMapNum:            32,
		MinAllocableVolumeCount:      0,
		AllocatableDiskLoadThreshold: 15,
		CodeModePolicies: []codemode.Policy{{
			ModeName: codemode.EC15P12.Name(),
			Enable:   true,
		}},
	}
)

// initMockVolumeMgr gengerate 30 volumes,which vid is [0-29].
// in reality,vid=0 is invalid volume, this vid=0 only use to test
// vid:[0,2,4,...,28] status is VolumeStatusIdle ,which volume is in allocator, can be use to test allocVolume
// vid:[1,3,5,...,29]status is volumeStatusActive,which volume already actives, can be use to test retainVolume
func initMockVolumeMgr(t testing.TB) {
	volumeDBPPath = "/tmp/volumedb/" + strconv.FormatInt(time.Now().UnixNano(), 10) + strconv.Itoa(rand.Intn(1000000))
	normalDBPath = "/tmp/volumedb/" + strconv.FormatInt(time.Now().UnixNano(), 10) + strconv.Itoa(rand.Intn(1000000))
	generateDataInDB()

	ctr := gomock.NewController(t)
	defer ctr.Finish()

	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockConfigMgr := mock.NewMockConfigMgrAPI(ctr)
	mockDiskMgr := NewMockDiskMgrAPI(ctr)

	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(true)
	mockConfigMgr.EXPECT().List(gomock.Any()).AnyTimes().Return(map[string]string{"key1": "2097152"}, nil)
	mockConfigMgr.EXPECT().Delete(gomock.Any(), "mockKey").AnyTimes().Return(nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeReserveSizeKey).AnyTimes().Return("2097152", nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeChunkSizeKey).AnyTimes().Return("17179869184", nil)
	mockDiskMgr.EXPECT().Stat(gomock.Any()).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 35})
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, id proto.DiskID) (bool, error) {
		if id == proto.DiskID(29) {
			return false, nil
		}
		return true, nil
	})
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

	//  new volumeMgr
	var err error
	mockVolumeMgr, err = NewVolumeMgr(testConfig, mockDiskMgr, mockScopeMgr, mockConfigMgr, volumeDB)
	require.NoError(t, err)
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockVolumeMgr.SetRaftServer(mockRaftServer)
}

func closeTestVolumeMgr() {
	mockVolumeMgr.Close()
	volumeDB.Close()
	normalDB.Close()
	os.RemoveAll(volumeDBPPath)
	os.RemoveAll(normalDBPath)
	initialVolumeStatusStat()
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

func generateVolumeRecord(mode codemode.CodeMode, start, end int) (volumeRecords []*volumedb.VolumeRecord, unitRecords [][]*volumedb.VolumeUnitRecord) {
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

	return
}

func generateVolumeUnit(vol *volume) (volumeUints []*volumeUnit, unitRecords []*volumedb.VolumeUnitRecord, units []clustermgr.Unit) {
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

// generate 30 volume in db, vid from 0 to 29
func generateDataInDB() {
	var err error
	volumeDB, err = volumedb.Open(volumeDBPPath, false)
	if err != nil {
		log.Error("open db error")
	}
	volTable, err = volumedb.OpenVolumeTable(volumeDB.KVStore)
	if err != nil {
		log.Error("open volumeTable error,db is nil")
	}

	normalDB, _ = normaldb.OpenNormalDB(normalDBPath, false)
	volumeRecords, unitRecords := generateVolumeRecord(codemode.EC15P12, 0, volumeCount)
	volTable.PutVolumeAndVolumeUnit(volumeRecords, unitRecords)

	// volUnitTable.PutBatch(generateVolumeUnit(volumeInfos))
	volTable.PutTokens(generateToken(volumeRecords))
}

func Test_VolumeMgr(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()
	var count int
	mockVolumeMgr.all.rangeVol(func(v *volume) error {
		count++
		return nil
	})
	require.Equal(t, count, 30)
}

func Test_NewVolumeMgr(t *testing.T) {
	volumeDBPPath += strconv.Itoa(rand.Intn(100))
	normalDBPath += strconv.Itoa(rand.Intn(100))
	generateDataInDB()
	defer closeTestVolumeMgr()

	ctr := gomock.NewController(t)
	defer ctr.Finish()

	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockConfigMgr := mock.NewMockConfigMgrAPI(ctr)
	mockDiskMgr := NewMockDiskMgrAPI(ctr)

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

	mockConfigMgr.EXPECT().List(gomock.Any()).AnyTimes().Return(map[string]string{"key1": "2097152"}, nil)
	mockConfigMgr.EXPECT().Delete(gomock.Any(), "key1").AnyTimes().Return(nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeReserveSizeKey).AnyTimes().Return("2097152", nil)
	mockConfigMgr.EXPECT().Get(gomock.Any(), proto.VolumeChunkSizeKey).AnyTimes().Return("17179869184", nil)
	mockConfigMgr.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	mockDiskMgr.EXPECT().Stat(gomock.Any()).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 100})
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
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

	// test new volumeMgr
	var err error
	mockVolumeMgr, err = NewVolumeMgr(volConfig, mockDiskMgr, mockScopeMgr, mockConfigMgr, volumeDB)
	require.NoError(t, err)
	mockVolumeMgr.SetRaftServer(mockRaftServer)

	// test volumeMgr load()
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, id proto.DiskID) (bool, error) {
		if id == proto.DiskID(29) {
			return false, nil
		}
		return true, nil
	})
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(true)
	mockRaftServer.EXPECT().Status().AnyTimes().Return(raftserver.Status{Id: 1})
	mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(uint64(31), uint64(31), nil)
	mockDiskMgr.EXPECT().AllocChunks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy *diskmgr.AllocPolicy) ([]proto.DiskID, error) {
		var diskids []proto.DiskID
		for i := range policy.Vuids {
			diskids = append(diskids, proto.DiskID(i+1))
		}
		return diskids, nil
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
	err = mockVolumeMgr.applyVolumeTask(context.Background(), 2, uuid.New().String(), base.VolumeTaskTypeLock)
	require.NoError(t, err)
	vol2 := mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusLock, vol2.volInfoBase.Status)

	mockVolumeMgr.configMgr.Get(context.Background(), proto.VolumeReserveSizeKey)
	mockVolumeMgr.configMgr.Set(context.Background(), proto.VolumeReserveSizeKey, "2097152")
	mockVolumeMgr.configMgr.List(context.Background())
	mockVolumeMgr.configMgr.Delete(context.Background(), "key1")
}

func TestVolumeMgr_AllocChunkForIdcUnits(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()
	ctr := gomock.NewController(t)
	defer ctr.Finish()

	vol := mockVolumeMgr.all.getVol(1)
	require.NotNil(t, vol)
	vuInfos := make(map[proto.VuidPrefix]*clustermgr.VolumeUnitInfo)
	for i := 0; i < 6; i++ {
		vuInfos[vol.vUnits[i].vuidPrefix] = vol.vUnits[i].vuInfo
	}

	mockDiskMgr := NewMockDiskMgrAPI(ctr)

	mockDiskMgr.EXPECT().Stat(gomock.Any()).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 35})
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, id proto.DiskID) (bool, error) {
		if id == proto.DiskID(29) {
			return false, nil
		}
		return true, nil
	})
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
	mockDiskMgr.EXPECT().AllocChunks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy *diskmgr.AllocPolicy) ([]proto.DiskID, error) {
		diskids := make([]proto.DiskID, len(policy.Vuids))
		for i := range diskids {
			if i < 2 {
				diskids[i] = 9999
			} else {
				diskids[i] = 0
			}
		}
		return diskids, errors.New("err")
	})
	_, ctx := trace.StartSpanFromContext(context.Background(), "allocChunkForIdc")
	mockVolumeMgr.diskMgr = mockDiskMgr
	mockVolumeMgr.allocChunkForIdcUnits(ctx, "z1", vuInfos)
	for i := range vuInfos {
		require.Equal(t, vuInfos[i].DiskID, proto.DiskID(9999))
	}
}

func TestVolumeMgr_ListVolumeInfo(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	_, ctx := trace.StartSpanFromContext(context.Background(), "listVolumeInfoV2")

	volInfos, err := mockVolumeMgr.ListVolumeInfoV2(ctx, proto.VolumeStatusIdle)
	require.NoError(t, err)
	require.Equal(t, 15, len(volInfos))

	volInfos, err = mockVolumeMgr.ListVolumeInfoV2(ctx, proto.VolumeStatusActive)
	require.NoError(t, err)
	require.Equal(t, 15, len(volInfos))
}

func TestVolumeMgr_GetVolumeInfo(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	ctr := gomock.NewController(t)
	defer ctr.Finish()

	mockRaftServer := mocks.NewMockRaftServer(ctr)
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

	// failed case, only volume free space bigger than freezeThreshold can alloc
	{
		mockVolumeMgr.allocator.freezeThreshold = 1 << 42
		ret, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
		require.Error(t, err)
		require.Nil(t, ret)
	}
}

func TestVolumeMgr_applyAllocVolume(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	mode := codemode.EC15P12
	args := &AllocVolumeCtx{
		Vids:       []proto.Vid{2, 4, 6, 8},
		Host:       "127.0.0.1:8080",
		ExpireTime: time.Now().Add(time.Duration(10 * time.Minute)).UnixNano(),
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
		allocVolLenMap = mockVolumeMgr.allocator.StatAllocatable()
		// all volume has actives ,allocVolLen is 0
		require.Equal(t, 0, allocVolLenMap[mode])

		// test allocator has 0 volume,
		for _, vid := range args.Vids {
			_, err := mockVolumeMgr.applyAllocVolume(ctx, vid, args.Host, args.ExpireTime)
			require.NoError(t, err)
		}

		// test vid not exist
		args.Vids = []proto.Vid{44}
		_, err := mockVolumeMgr.applyAllocVolume(ctx, args.Vids[0], args.Host, args.ExpireTime)
		require.Error(t, err)
	}

	// test allocVolume : success case
	{
		ctr := gomock.NewController(t)
		defer ctr.Finish()

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
		mockRaftServer := mocks.NewMockRaftServer(ctr)
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
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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

	// double check if not expire ,direct return
	vol7 := mockVolumeMgr.all.getVol(proto.Vid(7))
	err = mockVolumeMgr.applyExpireVolume(ctx, []proto.Vid{7})
	require.NoError(t, err)
	require.Equal(t, proto.VolumeStatusActive, vol7.volInfoBase.Status)

	// vid not exist
	err = mockVolumeMgr.applyExpireVolume(ctx, []proto.Vid{77})
	require.Error(t, err)
}

func TestVolumeMgr_ListAllocatedVolume(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

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

func TestVolumeMgr_LockVolume(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	// not allow lock active volume
	err := mockVolumeMgr.LockVolume(context.Background(), 1)
	require.Error(t, err)

	// vid not exist
	err = mockVolumeMgr.LockVolume(context.Background(), 55)
	require.Error(t, err)

	ctr := gomock.NewController(t)
	defer ctr.Finish()
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockVolumeMgr.raftServer = mockRaftServer
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil)

	// not apply ,
	vol2 := mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusIdle, vol2.volInfoBase.Status)
	err = mockVolumeMgr.LockVolume(context.Background(), 2)
	require.Error(t, err)
	require.Equal(t, proto.VolumeStatusIdle, vol2.volInfoBase.Status)

	err = mockVolumeMgr.applyVolumeTask(context.Background(), 2, uuid.New().String(), base.VolumeTaskTypeLock)
	require.NoError(t, err)
	vol2 = mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusLock, vol2.volInfoBase.Status)

	err = mockVolumeMgr.LockVolume(context.Background(), 2)
	require.NoError(t, err)
}

func TestVolumeMgr_UnlockVolume(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()
	ctr := gomock.NewController(t)
	defer ctr.Finish()
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockVolumeMgr.raftServer = mockRaftServer
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil)

	// failed case: lock status can unlock
	vol2 := mockVolumeMgr.all.getVol(2)
	require.Equal(t, proto.VolumeStatusIdle, vol2.volInfoBase.Status)
	err := mockVolumeMgr.UnlockVolume(context.Background(), 2)
	require.Error(t, err)

	// failed case: vid not exist
	err = mockVolumeMgr.UnlockVolume(context.Background(), 55)
	require.Error(t, err)

	vol2.lock.Lock()
	vol2.volInfoBase.Status = proto.VolumeStatusLock
	vol2.lock.Unlock()
	err = mockVolumeMgr.UnlockVolume(context.Background(), 2)
	require.NoError(t, err)

	ret, err := mockVolumeMgr.GetVolumeInfo(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, proto.VolumeStatusLock, ret.Status)

	err = mockVolumeMgr.applyVolumeTask(context.Background(), 2, uuid.New().String(), base.VolumeTaskTypeUnlock)
	require.NoError(t, err)

	ret, err = mockVolumeMgr.GetVolumeInfo(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, proto.VolumeStatusUnlocking, ret.Status)

	// volume status id idle , cannot apply volume unlock task, direct return but error is nil
	err = mockVolumeMgr.applyVolumeTask(context.Background(), 2, uuid.NewString(), base.VolumeTaskTypeUnlock)
	require.NoError(t, err)
}

func TestVolumeMgr_Report(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	mockVolumeMgr.Report(context.Background(), "test-region", 1)
}

func TestVolumeMgr_PreAlloc(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	testCases := []struct {
		codemode    codemode.CodeMode
		healthScore int
		count       int
		lenVids     int
		diskLoad    int
	}{
		// first have 8 diskload=0 vid,alloc success
		{
			codemode:    1,
			healthScore: 0,
			count:       2,
			lenVids:     2,
			diskLoad:    0,
		},
		{
			codemode:    1,
			healthScore: 0,
			count:       1,
			lenVids:     1,
			diskLoad:    0,
		},
		// prealloc's vid(diskload=0) num not match require,should add diskload
		{
			codemode:    1,
			healthScore: 0,
			count:       2,
			lenVids:     2,
			diskLoad:    mockVolumeMgr.AllocatableDiskLoadThreshold,
		},
		// first add diskLoad,then add healthScore
		{
			codemode:    1,
			healthScore: -3,
			count:       2,
			lenVids:     2,
			diskLoad:    mockVolumeMgr.AllocatableDiskLoadThreshold,
		},
		// all volume health not match,not add diskLoad
		{
			codemode:    1,
			healthScore: -4,
			count:       5,
			lenVids:     0,
			diskLoad:    0,
		},
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
		require.Equal(t, len(vids), testCase.lenVids)
		require.Equal(t, diskLoad, testCase.diskLoad)
	}
}

func BenchmarkVolumeMgr_AllocVolume(b *testing.B) {
	initMockVolumeMgr(b)
	defer closeTestVolumeMgr()

	ctr := gomock.NewController(b)
	defer ctr.Finish()

	mockRaftServer := mocks.NewMockRaftServer(ctr)
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
			ret, err := mockVolumeMgr.AllocVolume(ctx, mode, len(args.Vids), args.Host)
			require.NoError(b, err)
			require.Equal(b, len(ret.AllocVolumeInfos), 2)
		}
	})
}

func BenchmarkVolumeMgr_PreRetainVolume(b *testing.B) {
	initMockVolumeMgr(b)
	defer closeTestVolumeMgr()

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
	initMockVolumeMgr(b)
	defer closeTestVolumeMgr()

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
