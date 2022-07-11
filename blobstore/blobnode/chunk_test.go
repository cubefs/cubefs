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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestCreateChunk(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "CreateChunk")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	ctx := context.TODO()

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: proto.DiskID(101),
		Vuid:   proto.Vuid(2001),
	}
	err := client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	// create error
	createChunkArg1 := &bnapi.CreateChunkArgs{
		DiskID: proto.DiskID(0),
		Vuid:   proto.Vuid(2001),
	}
	err = client.CreateChunk(ctx, host, createChunkArg1)
	require.Error(t, err)

	// create error
	createChunkArg2 := &bnapi.CreateChunkArgs{
		DiskID:    proto.DiskID(11),
		Vuid:      proto.Vuid(2001),
		ChunkSize: -5,
	}
	err = client.CreateChunk(ctx, host, createChunkArg2)
	require.Error(t, err)

	listChunkArg := &bnapi.ListChunkArgs{
		DiskID: proto.DiskID(101),
	}
	cis, err := client.ListChunks(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 1, len(cis))

	err = client.CreateChunk(ctx, host, createChunkArg)
	require.Error(t, err)

	service.Disks[101].GetConfig().MaxChunks = 1
	createChunkArg.Vuid += 1
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.Error(t, err)
	service.Disks[101].GetConfig().MaxChunks = 700

	createChunkArg.DiskID = proto.DiskID(110)
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.Error(t, err)

	createChunkArg.DiskID = proto.DiskID(102)
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodGet, host+"/debug/stat", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
}

func TestSetChunkStatus(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "SetChunkStatus")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)

	statChunkArg := &bnapi.StatChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	_, err := client.StatChunk(ctx, host, statChunkArg)
	require.Error(t, err)

	changeChunkArg := &bnapi.ChangeChunkStatusArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.SetChunkReadonly(ctx, host, changeChunkArg)
	require.Error(t, err)
	err = client.SetChunkReadwrite(ctx, host, changeChunkArg)
	require.Error(t, err)
	err = client.ReleaseChunk(ctx, host, changeChunkArg)
	require.Error(t, err)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	err = client.SetChunkReadonly(ctx, host, changeChunkArg)
	require.NoError(t, err)

	chunkStat, err := client.StatChunk(ctx, host, statChunkArg)
	require.NoError(t, err)
	require.Equal(t, bnapi.ChunkStatusReadOnly, chunkStat.Status)

	err = client.SetChunkReadonly(ctx, host, changeChunkArg)
	require.NoError(t, err)

	err = client.SetChunkReadwrite(ctx, host, changeChunkArg)
	require.NoError(t, err)

	chunkStat, err = client.StatChunk(ctx, host, statChunkArg)
	require.NoError(t, err)
	require.Equal(t, bnapi.ChunkStatusNormal, chunkStat.Status)

	err = client.SetChunkReadwrite(ctx, host, changeChunkArg)
	require.NoError(t, err)

	ds, exist := service.Disks[diskID]
	require.True(t, exist)
	cs, exist := ds.GetChunkStorage(vuid)
	require.True(t, exist)

	cs.SetStatus(bnapi.ChunkStatusRelease)
	err = client.SetChunkReadwrite(ctx, host, changeChunkArg)
	require.Error(t, err)
}

func TestReleaseChunk(t *testing.T) {
	log.SetOutputLevel(log.Ldebug)
	service, _ := newTestBlobNodeService(t, "ReleaseChunk")
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

	listChunkArg := &bnapi.ListChunkArgs{
		DiskID: diskID,
	}
	cis, err := client.ListChunks(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 1, len(cis))

	releaseChunkArgs := &bnapi.ChangeChunkStatusArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.ReleaseChunk(ctx, host, releaseChunkArgs)
	require.Error(t, err)

	releaseChunkArgs1 := &bnapi.ChangeChunkStatusArgs{
		DiskID: proto.DiskID(103),
		Vuid:   vuid,
	}
	err = client.ReleaseChunk(ctx, host, releaseChunkArgs1)
	require.Error(t, err)

	changeChunkArg := &bnapi.ChangeChunkStatusArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.SetChunkReadonly(ctx, host, changeChunkArg)
	require.NoError(t, err)

	err = client.ReleaseChunk(ctx, host, releaseChunkArgs)
	require.NoError(t, err)

	cis, err = client.ListChunks(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 0, len(cis))
}

func TestListChunk(t *testing.T) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "NewBlobNodeService")

	service, _ := newTestBlobNodeService(t, "ListChunk")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	listChunkArg := &bnapi.ListChunkArgs{
		DiskID: proto.DiskID(999),
	}
	_, err := client.ListChunks(ctx, host, listChunkArg)
	require.Error(t, err)

	listChunkArg.DiskID = proto.DiskID(101)
	cis, err := client.ListChunks(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 0, len(cis))

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: proto.DiskID(101),
		Vuid:   proto.Vuid(2001),
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	cis, err = client.ListChunks(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 1, len(cis))
	require.Equal(t, proto.DiskID(101), cis[0].DiskID)
	require.Equal(t, proto.Vuid(2001), cis[0].Vuid)

	createChunkArg.Vuid = proto.Vuid(2002)
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	cis, err = client.ListChunks(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 2, len(cis))
	for _, ci := range cis {
		switch ci.Vuid {
		case proto.Vuid(2001):
			require.Equal(t, proto.DiskID(101), ci.DiskID)
		case proto.Vuid(2002):
			require.Equal(t, proto.DiskID(101), ci.DiskID)
		default:
			require.Equal(t, false, true)
		}
	}
}
