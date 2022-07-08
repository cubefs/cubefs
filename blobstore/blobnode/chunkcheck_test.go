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

package blobnode

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestCheckChunkFile(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "CheckChunkFile")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err := client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	_, err = service.Disks[diskID].GcRubbishChunk(ctx)
	require.NoError(t, err)

	disk, exist := service.Disks[diskID]
	require.True(t, exist)

	cs, exist := disk.GetChunkStorage(vuid)
	require.True(t, exist)

	chunkFile := filepath.Join(disk.GetDataPath(), cs.ID().String())
	err = os.Remove(chunkFile)
	require.NoError(t, err)

	defer func() {
		recover()
	}()
	_ = service.checkAndCleanDiskRubbish(ctx, disk)
	t.Errorf("should panic and recover")
}

func TestCheckChunkFile2(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "CheckChunkFile2")
	defer cleanTestBlobNodeService(service)

	service.GcRubbishChunk()
}

func TestCheckRegisterChunk(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "CheckRegisterChunk")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	ctx := context.TODO()

	diskID := proto.DiskID(101)

	vuid, err := proto.NewVuid(proto.Vid(2001), 1, 1)
	require.NoError(t, err)
	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	vuid, err = proto.NewVuid(proto.Vid(2002), 1, 1)
	require.NoError(t, err)
	createChunkArg.Vuid = vuid
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	diskChunks, err := service.Disks[diskID].ListChunks(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(diskChunks))
	require.Equal(t, bnapi.ChunkStatusNormal, diskChunks[0].Status)
	require.Equal(t, bnapi.ChunkStatusNormal, diskChunks[1].Status)

	_, err = service.ClusterMgrClient.ListVolumeUnit(ctx, &cmapi.ListVolumeUnitArgs{DiskID: diskID})
	require.NoError(t, err)

	service.checkAndCleanGarbageEpoch(ctx)

	chunks := service.copyChunkStorages(ctx)
	require.Equal(t, 2, len(chunks))

	diskChunks, err = service.Disks[diskID].ListChunks(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(diskChunks))

	vuid, err = proto.NewVuid(proto.Vid(2003), 1, 1)
	require.NoError(t, err)
	createChunkArg.Vuid = vuid
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)
	chunks = service.copyChunkStorages(ctx)
	require.Equal(t, 3, len(chunks))

	// Still in the protection period, expect to fail
	service.checkAndCleanGarbageEpoch(ctx)
	chunks = service.copyChunkStorages(ctx)
	require.Equal(t, 3, len(chunks))

	service.Conf.ChunkProtectionPeriodSec = 1
	time.Sleep(time.Second * 2)

	// The protection period has expired, hope to succeed
	service.checkAndCleanGarbageEpoch(ctx)
	chunks = service.copyChunkStorages(ctx)
	require.Equal(t, 2, len(chunks))

	vuid, err = proto.NewVuid(proto.Vid(2004), 1, 1)
	require.NoError(t, err)
	createChunkArg.Vuid = vuid
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)
	service.checkAndCleanGarbageEpoch(ctx)

	service.Disks[diskID].SetStatus(proto.DiskStatusBroken)
	service.checkAndCleanGarbageEpoch(ctx)
}
