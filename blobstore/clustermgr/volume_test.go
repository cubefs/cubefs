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

package clustermgr

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
)

// generate 10 volume in db
func initServiceWithData() (*Service, func()) {
	cfg := *testServiceCfg

	cfg.DBPath = os.TempDir() + "/volume" + uuid.NewString() + strconv.FormatInt(rand.Int63n(math.MaxInt64), 10)
	cfg.VolumeMgrConfig.FlushIntervalS = 600
	cfg.VolumeMgrConfig.MinAllocableVolumeCount = 0
	cfg.BlobNodeDiskMgrConfig.HeartbeatExpireIntervalS = 600
	cfg.ClusterReportIntervalS = 3
	cfg.ClusterCfg[proto.VolumeReserveSizeKey] = "20000000"
	cfg.RaftConfig.ServerConfig.ListenPort = GetFreePort()
	cfg.RaftConfig.ServerConfig.Members = []raftserver.Member{
		{NodeID: 1, Host: fmt.Sprintf("127.0.0.1:%d", GetFreePort()), Learner: false},
	}

	os.Mkdir(cfg.DBPath, 0o755)
	err := generateVolume(cfg.DBPath+"/volumedb", cfg.DBPath+"/normaldb")
	if err != nil {
		panic("generate volume error: " + err.Error())
	}

	testService, _ := New(&cfg)
	cleanWG.Add(1)
	return testService, func() {
		go func() {
			cleanTestService(testService)
			cleanWG.Done()
		}()
	}
}

func TestService_CreateVolume(t *testing.T) {
	testServiceCfg.UnavailableIDC = "z0"
	for i := range testServiceCfg.VolumeCodeModePolicies {
		testServiceCfg.VolumeCodeModePolicies[i].Enable = false
	}

	testServiceCfg.VolumeCodeModePolicies = append(testServiceCfg.VolumeCodeModePolicies,
		codemode.Policy{ModeName: codemode.EC4P4L2.Name(), Enable: true})
	testService, _ := initServiceWithData()
	cleanTestService(testService) // waiting closed
	cleanWG.Done()

	// set EC4P4L2 enable=false
	for i := range testServiceCfg.VolumeCodeModePolicies {
		if testServiceCfg.VolumeCodeModePolicies[i].ModeName == codemode.EC4P4L2.Name() {
			testServiceCfg.VolumeCodeModePolicies[i].Enable = false
		} else {
			testServiceCfg.VolumeCodeModePolicies[i].Enable = true
		}
	}
	_, clean := initServiceWithData()
	defer clean()
}

func TestService_VolumeInfo(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// get volumeInfo
	{
		ret, err := cmClient.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: 1})
		require.NoError(t, err)
		require.NotNil(t, ret)
	}

	// list volumeInfo
	{
		listArgs := &clustermgr.ListVolumeArgs{
			Marker: proto.Vid(0),
			Count:  1,
		}
		list, err := cmClient.ListVolume(ctx, listArgs)
		require.NoError(t, err)
		require.Equal(t, 1, len(list.Volumes))
		require.Equal(t, proto.Vid(1), list.Volumes[0].Vid)

		listArgs.Marker = proto.Vid(1)
		listArgs.Count = 4
		list, err = cmClient.ListVolume(ctx, listArgs)
		require.NoError(t, err)
		require.Equal(t, 4, len(list.Volumes))
		require.Equal(t, proto.Vid(2), list.Volumes[0].Vid)
		require.Equal(t, proto.Vid(5), list.Volumes[3].Vid)
	}

	// list volume info v2
	{
		ret, err := cmClient.ListVolumeV2(ctx, &clustermgr.ListVolumeV2Args{Status: proto.VolumeStatusIdle})
		require.Error(t, err)
		require.Equal(t, 0, len(ret.Volumes))
	}

	// retain volume
	{
		// set volume retain time as 1 second
		testService.VolumeMgr.RetainTimeS = 1
		volInfos, err := cmClient.AllocVolume(ctx, &clustermgr.AllocVolumeArgs{IsInit: false, CodeMode: 1, Count: 2})
		require.NoError(t, err)
		token1 := fmt.Sprintf("127.0.0.1;%d", volInfos.AllocVolumeInfos[0].Vid)
		token2 := fmt.Sprintf("127.0.0.1;%d", volInfos.AllocVolumeInfos[1].Vid)
		args := &clustermgr.RetainVolumeArgs{
			Tokens: []string{token1, token2},
		}
		ret, err := cmClient.RetainVolume(ctx, args)
		require.NoError(t, err)
		require.Equal(t, len(ret.RetainVolTokens), 2)

		// ignore error token
		args.Tokens = append(args.Tokens, "127.0.e8080;11")
		ret, err = cmClient.RetainVolume(ctx, args)
		require.NoError(t, err)
		require.Equal(t, len(ret.RetainVolTokens), 2)

		// volume has expired
		args.Tokens = []string{token2}
		time.Sleep(time.Millisecond * 1100)
		ret, err = cmClient.RetainVolume(ctx, args)
		require.NoError(t, err)
		require.Equal(t, len(ret.RetainVolTokens), 0)
	}
}

func TestService_VolumeAlloc(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// alloc volume
	args := &clustermgr.AllocVolumeArgs{
		CodeMode: 1,
		Count:    1,
	}
	ret, err := cmClient.AllocVolume(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 1, len(ret.AllocVolumeInfos))
	vol := ret.AllocVolumeInfos[0]
	require.Equal(t, vol.Status, proto.VolumeStatusActive)

	args.CodeMode = 2
	ret, err = cmClient.AllocVolume(ctx, args)
	require.Error(t, err)
	require.Equal(t, 0, len(ret.AllocVolumeInfos))

	// failed case, count not invalid
	args = &clustermgr.AllocVolumeArgs{
		CodeMode: 1,
		Count:    0,
	}
	_, err = cmClient.AllocVolume(ctx, args)
	require.Error(t, err)

	// failed case ,code mode not invalid
	args.Count = 1
	args.CodeMode = 9
	_, err = cmClient.AllocVolume(ctx, args)
	require.Error(t, err)
}

// test allov volume with disk load threshold
func TestService_VolumeAlloc2(t *testing.T) {
	testServiceCfg.VolumeMgrConfig.AllocatableDiskLoadThreshold = 10
	// initServiceWithData generate disk_id 1-10
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	args := &clustermgr.AllocVolumeArgs{
		CodeMode: 1,
		Count:    3,
	}
	// first alloc 3 volume, disk_id(1-7)'s load is 9,
	ret, err := cmClient.AllocVolume(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 3, len(ret.AllocVolumeInfos))

	// second request 3 volume will success
	ret, err = cmClient.AllocVolume(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 3, len(ret.AllocVolumeInfos))
}

func TestService_ChunkSetCompact(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// chunk set compact
	{
		args := &clustermgr.SetCompactChunkArgs{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1),
			Compacting: true,
		}
		err := cmClient.SetCompactChunk(ctx, args)
		require.NoError(t, err)

		vol, err := cmClient.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: proto.Vid(1)})
		require.NoError(t, err)
		require.Equal(t, vol.HealthScore, -1)
	}

	// failed case, invalid vid or vuid
	{
		args := &clustermgr.SetCompactChunkArgs{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(9999999, 255), 9999),
			Compacting: true,
		}
		err := cmClient.SetCompactChunk(ctx, args)
		require.Error(t, err)

		args = &clustermgr.SetCompactChunkArgs{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 255), 9999),
			Compacting: true,
		}
		err = cmClient.SetCompactChunk(ctx, args)
		require.Error(t, err)
	}
}

func TestService_UpdateVolume(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// success case
	{
		oldVuid := proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1)
		newVuid := proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1)
		updateArgs := &clustermgr.UpdateVolumeArgs{
			NewVuid:   newVuid,
			NewDiskID: proto.DiskID(19),
			OldVuid:   oldVuid,
		}
		err := cmClient.UpdateVolume(context.Background(), updateArgs)
		require.NoError(t, err)
	}
	// failed case ,update unit next epoch not match
	{
		oldVuid := proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1)
		newVuid := proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 2)
		updateArgs := &clustermgr.UpdateVolumeArgs{
			NewVuid:   newVuid,
			NewDiskID: proto.DiskID(29),
			OldVuid:   oldVuid,
		}
		err := cmClient.UpdateVolume(ctx, updateArgs)
		require.Error(t, err)
	}

	// alloc Volume unit failed case, not blobnode ,alloc unit always failed
	{
		oldVuid := proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1)
		args := &clustermgr.AllocVolumeUnitArgs{
			Vuid: oldVuid,
		}
		// alloc  volume unit failed case
		ret, err := cmClient.AllocVolumeUnit(ctx, args)
		require.NotNil(t, err)
		require.Nil(t, ret)
	}
}

func TestService_VolumeLock(t *testing.T) {
	cleanWG.Wait()
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// lock volume
	{
		args := &clustermgr.LockVolumeArgs{
			Vid: proto.Vid(1),
		}
		err := cmClient.LockVolume(ctx, args)
		require.NoError(t, err)
	}

	// unlock volume
	{
		args := &clustermgr.UnlockVolumeArgs{
			Vid: proto.Vid(1),
		}
		err := cmClient.UnlockVolume(ctx, args)
		require.NoError(t, err)
	}
}

func TestService_VolumeUnitList(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// list volumeUnits
	{
		ret, err := cmClient.ListVolumeUnit(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: proto.DiskID(2)})
		require.NoError(t, err)
		require.NotNil(t, ret)

		_, err = cmClient.ListVolumeUnit(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: proto.DiskID(99)})
		require.NoError(t, err)
		require.Nil(t, err)
	}
}

func TestService_VolumeUnitRelease(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// release volume unit
	{
		vuid := proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1)
		vol, err := cmClient.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: proto.Vid(1)})
		require.NoError(t, err)
		oldDiskID := vol.Units[1].DiskID
		// UT test request to blobnode will return connection refused
		err = cmClient.ReleaseVolumeUnit(ctx, &clustermgr.ReleaseVolumeUnitArgs{Vuid: vuid, DiskID: oldDiskID})
		require.Error(t, err)
	}
}

func TestService_ChunkReport(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// chunk report
	{
		var chunks []clustermgr.ChunkInfo
		for i := 1; i < 11; i++ {
			vuid := proto.EncodeVuid(proto.EncodeVuidPrefix(proto.Vid(i), 2), 1)
			chunk := clustermgr.ChunkInfo{
				Vuid:  vuid,
				Total: uint64(1024 * 2),
				Free:  uint64(1025),
				Used:  uint64(1023),
			}
			chunks = append(chunks, chunk)
		}
		err := cmClient.ReportChunk(ctx, &clustermgr.ReportChunkArgs{ChunkInfos: chunks})
		require.NoError(t, err)
	}
}

func TestService_VolumeAllocatedList(t *testing.T) {
	cleanWG.Wait()

	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	args := &clustermgr.AllocVolumeArgs{
		IsInit:   false,
		CodeMode: 1,
		Count:    3,
	}
	allocVols, err := cmClient.AllocVolume(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 3, len(allocVols.AllocVolumeInfos))

	args.IsInit = true
	initAllocVols, err := cmClient.AllocVolume(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 3, len(initAllocVols.AllocVolumeInfos))

	args.CodeMode = 2
	initAllocVols, err = cmClient.AllocVolume(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 0, len(initAllocVols.AllocVolumeInfos))

	ret, err := cmClient.ListAllocatedVolumes(ctx, &clustermgr.ListAllocatedVolumeArgs{Host: "127.0.0.1", CodeMode: 1})
	require.NoError(t, err)
	require.NotNil(t, ret.AllocVolumeInfos)
	require.Equal(t, 3, len(ret.AllocVolumeInfos))
}

func TestService_AdminUpdateVolume(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	args := &clustermgr.VolumeInfoBase{
		Vid:  1,
		Used: 99,
	}
	err := cmClient.PostWith(ctx, "/admin/update/volume", nil, args)
	require.NoError(t, err)
}

func TestService_AdminUpdateVolumeUnit(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	args := &clustermgr.AdminUpdateUnitArgs{
		Epoch:     3,
		NextEpoch: 5,
		VolumeUnitInfo: clustermgr.VolumeUnitInfo{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 1),
			DiskID:     11,
			Compacting: true,
			Total:      1000,
			Free:       999,
		},
	}
	err := cmClient.PostWith(ctx, "/admin/update/volume/unit", nil, args)
	require.NoError(t, err)
	volInfo, err := cmClient.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: 1})
	require.NoError(t, err)
	require.Equal(t, volInfo.Units[1].DiskID, proto.DiskID(args.DiskID))
	require.Equal(t, volInfo.Units[1].Vuid, proto.EncodeVuid(args.Vuid.VuidPrefix(), args.Epoch))

	// failed case, diskid not exist
	args.VolumeUnitInfo.DiskID = 88
	err = cmClient.PostWith(ctx, "/admin/update/volume/unit", nil, args)
	require.Error(t, err)

	// failed case, vid not exist
	args.VolumeUnitInfo.Vuid = proto.EncodeVuid(proto.EncodeVuidPrefix(99, 1), 1)
	err = cmClient.PostWith(ctx, "/admin/update/volume/unit", nil, args)
	require.Error(t, err)
}

func generateVolume(volumeDBPath, NormalDBPath string) error {
	var (
		unitCount = 27
		tokens    = []*volumedb.TokenRecord{}
		units     = [][]*volumedb.VolumeUnitRecord{}
		volumes   = []*volumedb.VolumeRecord{}
	)
	volumeDB, err := volumedb.Open(volumeDBPath)
	if err != nil {
		return err
	}
	defer volumeDB.Close()
	normalDB, err := normaldb.OpenNormalDB(NormalDBPath)
	if err != nil {
		return err
	}
	defer normalDB.Close()

	volTable, err := volumedb.OpenVolumeTable(volumeDB.KVStore)
	if err != nil {
		return err
	}

	for i := 1; i < 11; i++ {
		vuidPrefixs := make([]proto.VuidPrefix, unitCount)
		unitRecords := make([]*volumedb.VolumeUnitRecord, unitCount)
		for j := 0; j < unitCount; j++ {
			vuidPrefixs[j] = proto.EncodeVuidPrefix(proto.Vid(i), uint8(j))
			unitRecords[j] = &volumedb.VolumeUnitRecord{
				VuidPrefix: vuidPrefixs[j],
				Epoch:      1,
				NextEpoch:  1,
				DiskID:     proto.DiskID(j%10 + 1),
				Free:       1024 * 1024 * 1024 * 1023,
				Used:       1024 * 1024 * 1024,
				Total:      1024 * 1024 * 1024 * 1024,
			}
		}
		vol := &volumedb.VolumeRecord{
			Vid:         proto.Vid(i),
			VuidPrefixs: vuidPrefixs,
			CodeMode:    1,
			Status:      proto.VolumeStatusIdle,
			HealthScore: 0,
			Free:        1024 * 1024 * 1024 * 1023,
			Used:        1024 * 1024 * 1024,
			Total:       1024 * 1024 * 1024 * 1024,
		}

		volumes = append(volumes, vol)
		tokens = append(tokens, &volumedb.TokenRecord{
			Vid:        proto.Vid(i),
			TokenID:    "127.0.0.1;" + strconv.Itoa(i),
			ExpireTime: time.Now().Add(time.Second * 10).UnixNano(),
		})
		units = append(units, unitRecords)
	}
	err = volTable.PutVolumes(volumes, units, tokens)
	if err != nil {
		return err
	}

	nodeTable, err := normaldb.OpenBlobNodeTable(normalDB)
	if err != nil {
		return err
	}

	diskTable, err := normaldb.OpenBlobNodeDiskTable(normalDB, true)
	if err != nil {
		return err
	}
	for i := 1; i <= unitCount+3; i++ {
		dr := &normaldb.BlobNodeDiskInfoRecord{
			DiskInfoRecord: normaldb.DiskInfoRecord{
				Version:      normaldb.DiskInfoVersionNormal,
				DiskID:       proto.DiskID(i),
				ClusterID:    proto.ClusterID(1),
				Path:         "",
				Status:       proto.DiskStatusNormal,
				Readonly:     false,
				CreateAt:     time.Now(),
				LastUpdateAt: time.Now(),
				NodeID:       proto.NodeID(i),
			},
			Used:         0,
			Size:         100000,
			Free:         100000,
			MaxChunkCnt:  10,
			FreeChunkCnt: 10,
			UsedChunkCnt: 0,
		}
		nr := &normaldb.BlobNodeInfoRecord{
			NodeInfoRecord: normaldb.NodeInfoRecord{
				Version:   normaldb.NodeInfoVersionNormal,
				ClusterID: proto.ClusterID(1),
				NodeID:    proto.NodeID(i),
				Idc:       "z0",
				Rack:      "rack1",
				Host:      "http://127.0.0." + strconv.Itoa(i) + ":80800",
				Role:      proto.NodeRoleBlobNode,
				Status:    proto.NodeStatusNormal,
				DiskType:  proto.DiskTypeHDD,
			},
		}
		if i >= 9 && i < 18 {
			dr.Idc = "z1"
			nr.Idc = "z1"
		} else if i >= 18 {
			dr.Idc = "z2"
			nr.Idc = "z2"
		}
		err := diskTable.AddDisk(dr)
		if err != nil {
			return err
		}
		err = nodeTable.UpdateNode(nr)
		if err != nil {
			return err
		}

	}

	return nil
}

func BenchmarkService_ChunkSetCompact(b *testing.B) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		args := &clustermgr.SetCompactChunkArgs{
			Vuid:       proto.EncodeVuid(proto.EncodeVuidPrefix(proto.Vid(i%9+1), 1), 1),
			Compacting: true,
		}
		err := cmClient.SetCompactChunk(ctx, args)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkService_VolumeAlloc(b *testing.B) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	args := &clustermgr.AllocVolumeArgs{
		CodeMode: 1,
		Count:    3,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ret, err := cmClient.AllocVolume(ctx, args)
			require.Equal(b, len(ret.AllocVolumeInfos), 3)
			require.NoError(b, err)
		}
	})
}

func BenchmarkService_VolumeListAndListV2(b *testing.B) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	listArgs := &clustermgr.ListVolumeArgs{
		Marker: proto.Vid(1),
		Count:  10,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cmClient.ListVolume(ctx, listArgs)
		}
	})
}

func BenchmarkService_ChunkReport(b *testing.B) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	var chunks []clustermgr.ChunkInfo
	for i := 1; i < 11; i++ {
		vuid := proto.EncodeVuid(proto.EncodeVuidPrefix(proto.Vid(i), 2), 1)
		chunk := clustermgr.ChunkInfo{
			Vuid:  vuid,
			Total: uint64(1024 * 2),
			Free:  uint64(1025),
			Used:  uint64(1023),
		}
		chunks = append(chunks, chunk)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := cmClient.ReportChunk(ctx, &clustermgr.ReportChunkArgs{ChunkInfos: chunks})
			require.NoError(b, err)
		}
	})
}
