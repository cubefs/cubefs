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
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func TestNewBlobNodeClient(t *testing.T) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "NewBlobNodeService")

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("X-Crc-Encoded") != "" {
			w.Header().Set("X-Ack-Crc-Encoded", "1")
		}
		w.WriteHeader(http.StatusOK)
	}))

	cfg := &Config{}

	cli := New(cfg)

	diskid := proto.DiskID(100002)

	stat, err := cli.Stat(ctx, mockServer.URL)
	require.NoError(t, err)
	span.Infof("stat: %v\n", stat)

	diskStatArgs := &DiskStatArgs{
		DiskID: diskid,
	}
	diskInfo, err := cli.DiskInfo(ctx, mockServer.URL, diskStatArgs)
	require.NoError(t, err)
	span.Infof("disk info: %v\n", diskInfo)

	creteChunkArgs := &CreateChunkArgs{
		DiskID: diskid,
		Vuid:   20001,
	}
	err = cli.CreateChunk(ctx, mockServer.URL, creteChunkArgs)
	require.NoError(t, err)

	changeChunkArgs := &ChangeChunkStatusArgs{
		DiskID: diskid,
		Vuid:   20002,
	}
	err = cli.ReleaseChunk(ctx, mockServer.URL, changeChunkArgs)
	require.NoError(t, err)

	changeChunkArgs.Vuid = 20003
	err = cli.SetChunkReadonly(ctx, mockServer.URL, changeChunkArgs)
	require.NoError(t, err)

	changeChunkArgs.Vuid = 20004
	err = cli.SetChunkReadwrite(ctx, mockServer.URL, changeChunkArgs)
	require.NoError(t, err)

	listChunkArgs := &ListChunkArgs{
		DiskID: diskid,
	}
	chunks, err := cli.ListChunks(ctx, mockServer.URL, listChunkArgs)
	require.NoError(t, err)
	span.Infof("chunks: %v\n", chunks)

	databytes := []byte("test context")
	putShardArgs := &PutShardArgs{
		DiskID: diskid,
		Vuid:   20006,
		Bid:    300002,
		Size:   int64(len(databytes)),
		Body:   bytes.NewReader(databytes),
	}
	_, err = cli.PutShard(ctx, mockServer.URL, putShardArgs)
	require.NoError(t, err)

	getShardArgs := &RangeGetShardArgs{
		GetShardArgs: GetShardArgs{
			DiskID: diskid,
			Vuid:   20007,
			Bid:    300003,
		},
		Offset: 0,
		Size:   4000002,
	}
	body, _, err := cli.RangeGetShard(ctx, mockServer.URL, getShardArgs)
	require.NoError(t, err)
	if body != nil {
		b, _ := ioutil.ReadAll(body)
		span.Infof("body: %s\n", b)
	}

	deleteShardArgs := &DeleteShardArgs{
		DiskID: diskid,
		Vuid:   20008,
		Bid:    300004,
	}
	err = cli.MarkDeleteShard(ctx, mockServer.URL, deleteShardArgs)
	require.NoError(t, err)

	deleteShardArgs.Vuid = 20009
	deleteShardArgs.Bid = 300005
	err = cli.DeleteShard(ctx, mockServer.URL, deleteShardArgs)
	require.NoError(t, err)

	statShardArgs := &StatShardArgs{
		DiskID: diskid,
		Vuid:   10010,
		Bid:    300005,
	}
	blobInfo, err := cli.StatShard(ctx, mockServer.URL, statShardArgs)
	require.NoError(t, err)
	span.Infof("blob info: %v\n", blobInfo)

	listShardsArgs := &ListShardsArgs{
		DiskID:   diskid,
		Vuid:     20011,
		StartBid: 300007,
		Status:   ShardStatusMarkDelete,
		Count:    50,
	}
	bids, _, err := cli.ListShards(ctx, mockServer.URL, listShardsArgs)
	require.NoError(t, err)
	span.Infof("bids: %v\n", bids)

	listShardsArgs.Status = ShardStatusNormal
	bids, _, err = cli.ListShards(ctx, mockServer.URL, listShardsArgs)
	require.NoError(t, err)
	span.Infof("bids: %v\n", bids)

	getShardsArgs := &GetShardsArgs{
		DiskID: diskid,
		Vuid:   20012,
		Bids: []proto.BlobID{
			300008,
			300009,
		},
	}
	blobsBody, err := cli.GetShards(ctx, mockServer.URL, getShardsArgs)
	require.NoError(t, err)
	if body != nil {
		b, _ := ioutil.ReadAll(blobsBody)
		span.Infof("body: %s\n", b)
	}

	_ = cli.String(ctx, mockServer.URL)
	_ = cli.Close(ctx, mockServer.URL)
	_ = cli.IsOnline(ctx, mockServer.URL)
}
