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
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
	"net/http"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
)

func noLimitClient() bnapi.StorageAPI {
	cfg := bnapi.Config{}
	cfg.Config.Tc.IdleConnTimeoutMs = 30 * 1000
	return bnapi.New(&cfg)
}

func assertRequest(t require.TestingT, req *http.Request) {
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
}

func TestShardPutAndGet(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ShardPutAndGet")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	getShardArg := &bnapi.RangeGetShardArgs{
		GetShardArgs: bnapi.GetShardArgs{
			DiskID: diskID,
			Vuid:   vuid,
			Bid:    bid,
		},
		Offset: 0,
		Size:   1,
	}
	_, _, err := client.RangeGetShard(ctx, host, getShardArg)
	require.Error(t, err)

	getShardArg.DiskID = proto.DiskID(0)
	_, _, err = client.RangeGetShard(ctx, host, getShardArg)
	require.Error(t, err)

	getShardArg.DiskID = diskID
	getShardArg.Type = 7
	_, _, err = client.RangeGetShard(ctx, host, getShardArg)
	require.Error(t, err)
	getShardArg.Type = 1

	shardData := []byte("testData")

	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, len(shardData), n)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(len(shardData)),
		Body:   bytes.NewReader(shardData),
	}

	//  /shard/put/diskid/{diskid}/vuid/{vuidValue}/bid/{bidValue}/size/{size}/iotype/{iotype}
	url := fmt.Sprintf("%v/shard/put/diskid/%v/vuid/%v/bid/%v/size/%v/iotype/%d", host,
		proto.DiskID(103), vuid, 1, 10, 1)
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(shardData))
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/put/diskid/%v/vuid/%v/bid/%v/size/%v/iotype/%d", host,
		diskID, vuid, 1, 10, 7)
	request, err = http.NewRequest(http.MethodPost, url, bytes.NewReader(shardData))
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/put/diskid/%v/vuid/%v/bid/%v/size/%v/iotype/%d", host,
		proto.DiskID(0), vuid, 1, 10, 1)
	request, err = http.NewRequest(http.MethodPost, url, bytes.NewReader(shardData))
	require.NoError(t, err)
	assertRequest(t, request)

	putShardArg.Type = 7
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	putShardArg.Type = 1
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	_, err = client.PutShard(ctx, host, &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    proto.InValidBlobID,
		Size:   int64(len(shardData)),
		Body:   bytes.NewReader(shardData),
	})
	require.Error(t, err)

	url = fmt.Sprintf("%v/shard/put/diskid/%v/vuid/%v/bid/%v/size/%v", host, diskID, vuid, bid, math.MaxUint32+1)
	request, err = http.NewRequest(http.MethodPost, url, bytes.NewReader(shardData))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, bloberr.CodeShardSizeTooLarge, resp.StatusCode)

	putShardArg.Body = bytes.NewReader(shardData)
	crc, err := client.PutShard(ctx, host, putShardArg)
	require.NoError(t, err)
	require.Equal(t, dataCrc.Sum32(), crc)

	url = fmt.Sprintf("%v/shard/get/diskid/%v/vuid/%v/bid/%v", host, diskID, vuid, 2002)
	request, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 651, resp.StatusCode)

	url = fmt.Sprintf("%v/shard/get/diskid/%v/vuid/%v/bid/%v", host, proto.DiskID(103), vuid, bid)
	request, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/get/diskid/%v/vuid/%v/bid/%v/iotype/%d",
		host, diskID, vuid, bid, 7)
	request, err = http.NewRequest(http.MethodGet, url, bytes.NewReader(shardData))
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/get/diskid/%v/vuid/%v/bid/%v", host, diskID, vuid, bid)
	request, err = http.NewRequest(http.MethodGet, url, bytes.NewReader(shardData))
	rangeStr := fmt.Sprintf("bytes%v-%v", 0, 10)
	request.Header.Set("Range", rangeStr)
	require.NoError(t, err)
	assertRequest(t, request)

	// /shard/get/diskid/{diskid}/vuid/{vuidValue}/bid/{bidValue}
	url = fmt.Sprintf("%v/shard/get/diskid/%v/vuid/%v/bid/%v", host, proto.DiskID(0), vuid, bid)
	request, err = http.NewRequest(http.MethodGet, url, bytes.NewReader(shardData))
	require.NoError(t, err)
	assertRequest(t, request)

	getShardArg.Vuid = vuid
	getShardArg.Bid = bid
	getShardArg.Size = int64(len(shardData))

	body, crc, err := client.RangeGetShard(ctx, host, getShardArg)
	require.NoError(t, err)
	defer body.Close()
	require.Equal(t, dataCrc.Sum32(), crc)

	getShardData, err := ioutil.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, true, reflect.DeepEqual(shardData, getShardData))

	putShardArg.Size = math.MaxInt64
	putShardArg.Body = bytes.NewReader(shardData)
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	putShardArg.Size = int64(len(shardData))
	putShardArg.Body = bytes.NewReader(shardData)
	service.Disks[diskID].SetStatus(proto.DiskStatusBroken)
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	putShardArg.Body = bytes.NewReader(shardData)
	service.Disks[diskID].SetStatus(proto.DiskStatusNormal)
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Nil(t, err)

	ds := service.Disks[diskID]
	cs, exist := ds.GetChunkStorage(vuid)
	require.True(t, exist)

	putShardArg.Body = bytes.NewReader(shardData)

	putShardArg.Body = bytes.NewReader(shardData)
	cs.SetStatus(bnapi.ChunkStatusRelease)
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	putShardArg.Body = bytes.NewReader(shardData)
	cs.SetStatus(bnapi.ChunkStatusNormal)
	_, _ = client.PutShard(ctx, host, putShardArg)
}

func TestService_CmdShardStat_(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "TestService_CmdShardStat_")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	shardData := []byte("testData")

	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, len(shardData), n)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(len(shardData)),
		Body:   bytes.NewReader(shardData),
	}
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	//  /shard/stat/diskid/{diskid}/vuid/{vuidValue}/bid/{bidValue}
	url := fmt.Sprintf("%v/shard/stat/diskid/%v/vuid/%v/bid/%v", host,
		proto.DiskID(103), vuid, 1)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/stat/diskid/%v/vuid/%v/bid/%v", host,
		proto.DiskID(0), vuid, 1)
	request, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)
}

func TestService_CmdShardMarkdelete_(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "TestService_CmdShardMarkdelete_")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	shardData := []byte("testData")

	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, len(shardData), n)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(len(shardData)),
		Body:   bytes.NewReader(shardData),
	}
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	//  /shard/markdelete/diskid/{diskid}/vuid/{vuid}/bid/{bid}
	url := fmt.Sprintf("%v/shard/markdelete/diskid/%v/vuid/%v/bid/%v", host,
		proto.DiskID(103), vuid, 1)
	request, err := http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/markdelete/diskid/%v/vuid/%v/bid/%v", host,
		proto.DiskID(0), vuid, 1)
	request, err = http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)
}

func TestService_CmdShardDelete_(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "TestService_CmdShardDelete_")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	shardData := []byte("testData")

	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, len(shardData), n)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(len(shardData)),
		Body:   bytes.NewReader(shardData),
	}
	_, err = client.PutShard(ctx, host, putShardArg)
	require.Error(t, err)

	//  /shard/delete/diskid/{diskid}/vuid/{vuid}/bid/{bid}
	url := fmt.Sprintf("%v/shard/delete/diskid/%v/vuid/%v/bid/%v", host,
		proto.DiskID(103), vuid, 1)
	request, err := http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/delete/diskid/%v/vuid/%v/bid/%v", host,
		proto.DiskID(0), vuid, 1)
	request, err = http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)
}

func TestListShards(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ListShards")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(1)
	shardData := []byte("testData")
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, len(shardData), n)

	listChunkArg := &bnapi.ListShardsArgs{
		DiskID:   diskID,
		Vuid:     vuid,
		StartBid: 0,
		Status:   bnapi.ShardStatusNormal,
		Count:    10,
	}

	listChunkArg.DiskID = proto.DiskID(0)
	_, _, err = client.ListShards(ctx, host, listChunkArg)
	require.Error(t, err)

	listChunkArg.DiskID = diskID
	_, _, err = client.ListShards(ctx, host, listChunkArg)
	require.Error(t, err)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(len(shardData)),
		Body:   bytes.NewReader(shardData),
	}
	crc, err := client.PutShard(ctx, host, putShardArg)
	require.NoError(t, err)
	require.Equal(t, dataCrc.Sum32(), crc)

	// /shard/list/diskid/{diskid}/vuid/{vuid}/startbid/{bid}/status/{status}/count/{count}

	url := fmt.Sprintf("%v/shard/list/diskid/%v/vuid/%v/startbid/%v/status/%v/count/%v", host,
		proto.DiskID(103), vuid, 1, 1, 1)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/list/diskid/%v/vuid/%v/startbid/%v/status/%v/count/%v", host,
		proto.DiskID(0), vuid, 1, 1, 1)
	request, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/list/diskid/%v/vuid/%v/startbid/%v/status/%v/count/%v", host,
		diskID, vuid, 1, 1, 65538)
	request, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	url = fmt.Sprintf("%v/shard/list/diskid/%v/vuid/%v/startbid/%v/status/%v/count/%v", host,
		diskID, vuid, 1, 1, -1)
	request, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	assertRequest(t, request)

	sis, _, err := client.ListShards(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 1, len(sis))
	require.Equal(t, int64(len(shardData)), sis[0].Size)
	require.Equal(t, dataCrc.Sum32(), sis[0].Crc)

	count := 99
	for i := 1; i < count; i++ {
		putShardArg.Bid++
		putShardArg.Body = bytes.NewReader(shardData)
		crc, err = client.PutShard(ctx, host, putShardArg)
		require.NoError(t, err)
		require.Equal(t, dataCrc.Sum32(), crc)
	}

	listChunkArg.Count = count
	sis, _, err = client.ListShards(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, count, len(sis))
	for i := range sis {
		require.Equal(t, int64(len(shardData)), sis[i].Size)
		require.Equal(t, dataCrc.Sum32(), sis[i].Crc)
		require.Equal(t, bnapi.ShardStatusNormal, sis[i].Flag)
	}

	listChunkArg.Count = 1
	_, _, err = client.ListShards(ctx, host, listChunkArg)
	require.NoError(t, err)

	cnt := 0
	listChunkArg.Count = 3
	for cnt < count {
		sis, _, err = client.ListShards(ctx, host, listChunkArg)
		require.NoError(t, err)
		cnt += len(sis)
		if len(sis) > 0 {
			listChunkArg.StartBid = sis[len(sis)-1].Bid
		}
	}
	require.Equal(t, cnt, count)

	deleteShardArg := &bnapi.DeleteShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    11,
	}
	deleteShardArg.DiskID = proto.DiskID(0)
	err = client.MarkDeleteShard(ctx, host, deleteShardArg)
	require.Error(t, err)
	err = client.DeleteShard(ctx, host, deleteShardArg)
	require.Error(t, err)

	deleteShardArg.DiskID = diskID
	err = client.MarkDeleteShard(ctx, host, deleteShardArg)
	require.NoError(t, err)
	err = client.DeleteShard(ctx, host, deleteShardArg)
	require.NoError(t, err)

	cnt = 0
	listChunkArg.StartBid = 0
	for cnt < count-1 {
		sis, _, err = client.ListShards(ctx, host, listChunkArg)
		require.NoError(t, err)
		cnt += len(sis)
		if len(sis) > 0 {
			listChunkArg.StartBid = sis[len(sis)-1].Bid
		}
	}
	require.Equal(t, cnt, count-1)

	deleteShardArg = &bnapi.DeleteShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    22,
	}
	err = client.MarkDeleteShard(ctx, host, deleteShardArg)
	require.NoError(t, err)
	err = client.DeleteShard(ctx, host, deleteShardArg)
	require.NoError(t, err)

	cnt = 0
	listChunkArg.StartBid = proto.InValidBlobID
	for cnt < count-2 {
		sis, _, err = client.ListShards(ctx, host, listChunkArg)
		require.NoError(t, err)
		cnt += len(sis)
		if len(sis) > 0 {
			listChunkArg.StartBid = sis[len(sis)-1].Bid
		}
	}
	require.Equal(t, cnt, count-2)

	deleteShardArg.Bid = 33
	err = client.MarkDeleteShard(ctx, host, deleteShardArg)
	require.NoError(t, err)

	listChunkArg.Status = bnapi.ShardStatusNormal
	listChunkArg.StartBid = proto.InValidBlobID
	listChunkArg.Count = 1000
	sis, _, err = client.ListShards(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 96, len(sis))

	listChunkArg.Status = bnapi.ShardStatusMarkDelete
	sis, _, err = client.ListShards(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 1, len(sis))

	listChunkArg.Status = bnapi.ShardStatusDefault
	sis, _, err = client.ListShards(ctx, host, listChunkArg)
	require.NoError(t, err)
	require.Equal(t, 97, len(sis))
}

func TestShardDelete(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ShardDelete")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)
	shardData := []byte("testData")
	shardSize := len(shardData)
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, shardSize, n)

	deleteShardArg := &bnapi.DeleteShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
	}
	err = client.MarkDeleteShard(ctx, host, deleteShardArg)
	require.Error(t, err)
	err = client.DeleteShard(ctx, host, deleteShardArg)
	require.Error(t, err)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(shardSize),
		Body:   bytes.NewReader(shardData),
	}
	crc, err := client.PutShard(ctx, host, putShardArg)
	require.NoError(t, err)
	require.Equal(t, dataCrc.Sum32(), crc)

	err = client.DeleteShard(ctx, host, deleteShardArg)
	require.Error(t, err)

	err = client.MarkDeleteShard(ctx, host, deleteShardArg)
	require.NoError(t, err)

	statShardArg := &bnapi.StatShardArgs{
		DiskID: diskID,
		Vuid:   proto.Vuid(54321),
		Bid:    proto.BlobID(123456),
	}

	statShardArg.DiskID = proto.DiskID(0)
	_, err = client.StatShard(ctx, host, statShardArg)
	require.Error(t, err)

	statShardArg.DiskID = diskID
	_, err = client.StatShard(ctx, host, statShardArg)
	require.Error(t, err)
	statShardArg.Vuid = vuid
	_, err = client.StatShard(ctx, host, statShardArg)
	require.Error(t, err)

	statShardArg.Bid = bid
	shardStat, err := client.StatShard(ctx, host, statShardArg)
	require.NoError(t, err)
	require.Equal(t, bnapi.ShardStatusMarkDelete, shardStat.Flag)

	err = client.DeleteShard(ctx, host, deleteShardArg)
	require.NoError(t, err)

	_, err = client.StatShard(ctx, host, statShardArg)
	require.Error(t, err)

	service.Disks[diskID].SetStatus(proto.DiskStatusBroken)
	err = client.DeleteShard(ctx, host, deleteShardArg)
	require.Error(t, err)
	err = client.MarkDeleteShard(ctx, host, deleteShardArg)
	require.Error(t, err)
}

func TestShardDeleteConcurrency(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ShardDeleteCon")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := noLimitClient()
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)
	shardData := []byte("testData")
	shardSize := len(shardData)
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, shardSize, n)

	deleteShardArg := &bnapi.DeleteShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
	}

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(shardSize),
		Body:   bytes.NewReader(shardData),
	}
	crc, err := client.PutShard(ctx, host, putShardArg)
	require.NoError(t, err)
	require.Equal(t, dataCrc.Sum32(), crc)

	// concurrency
	concurrency := 30
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	errChan := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			err := client.MarkDeleteShard(ctx, host, deleteShardArg)
			errChan <- err
		}()
	}
	wg.Wait()

	errList := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		e := <-errChan
		errList[i] = e
	}

	var alreayDelCnt int
	for _, e := range errList {
		if e != nil && e.Error() == bloberr.ErrShardMarkDeleted.Error() {
			alreayDelCnt++
		}
	}
	require.Equal(t, 29, alreayDelCnt)
}

func TestShardRangeGet(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ShardRangeGet")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)
	shardData := []byte("testData")
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, len(shardData), n)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	putShardArg := &bnapi.PutShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   int64(len(shardData)),
		Body:   bytes.NewReader(shardData),
	}
	crc, err := client.PutShard(ctx, host, putShardArg)
	require.NoError(t, err)
	require.Equal(t, dataCrc.Sum32(), crc)

	getShardArg := &bnapi.RangeGetShardArgs{
		GetShardArgs: bnapi.GetShardArgs{
			DiskID: diskID,
			Vuid:   vuid,
			Bid:    bid,
		},
		Offset: 1,
		Size:   1,
	}
	body, _, err := client.RangeGetShard(ctx, host, getShardArg)
	require.NoError(t, err)

	b, err := ioutil.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, 1, len(b))
	require.Equal(t, byte('e'), byte(b[0]))

	getShardArg.Offset = 4
	getShardArg.Size = 2
	body, _, err = client.RangeGetShard(ctx, host, getShardArg)
	require.NoError(t, err)

	b, err = ioutil.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, 2, len(b))
	require.Equal(t, byte('D'), byte(b[0]))
	require.Equal(t, byte('a'), byte(b[1]))

	//
	args := bnapi.GetShardArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bid:    bid,
	}
	body, _, err = client.GetShard(ctx, host, &args)
	require.NoError(t, err)

	b, err = ioutil.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, 8, len(b))
	require.Equal(t, dataCrc.Sum32(), crc)

	args1 := bnapi.GetShardArgs{
		DiskID: proto.DiskID(0),
		Vuid:   vuid,
		Bid:    bid,
	}
	_, _, err = client.GetShard(ctx, host, &args1)
	require.Error(t, err)

	args1.DiskID = diskID
	args1.Type = bnapi.IOTypeMax
	_, _, err = client.GetShard(ctx, host, &args1)
	require.Error(t, err)
}

func TestShardGetConcurrency(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ShardGetCon")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := noLimitClient()
	ctx := context.TODO()

	service.GetQpsLimitPerKey = keycount.New(1)

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)
	shardData := []byte("testData")
	shardSize := len(shardData)
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, shardSize, n)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	// concurrency
	concurrency := 100

	for i := bid; i < bid+proto.BlobID(concurrency); i++ {
		putShardArg := &bnapi.PutShardArgs{
			DiskID: diskID,
			Vuid:   vuid,
			Bid:    i,
			Size:   int64(shardSize),
			Body:   bytes.NewReader(shardData),
		}
		_, err = client.PutShard(ctx, host, putShardArg)
		require.NoError(t, err)
	}

	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	errChan := make(chan error, concurrency)
	for i := bid; i < bid+proto.BlobID(concurrency); i++ {
		go func(key proto.BlobID) {
			defer wg.Done()
			getShardArg := &bnapi.RangeGetShardArgs{
				GetShardArgs: bnapi.GetShardArgs{
					DiskID: diskID,
					Vuid:   vuid,
					Bid:    bid,
				},
				Offset: 0,
				Size:   int64(len(shardData)),
			}
			body, _, err := client.RangeGetShard(ctx, host, getShardArg)
			if err == nil {
				body.Close() // release connection
			}
			errChan <- err
		}(bid)
	}
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		e := <-errChan
		if e != nil {
			return
		}
	}
	t.Fatalf("get shard concurrency limit failed")
}

func TestShardPutConcurrency(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ShardPutCon")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := noLimitClient()
	ctx := context.TODO()

	service.PutQpsLimitPerDisk = keycount.New(1)

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)
	shardData := []byte("testData")
	shardSize := len(shardData)
	dataCrc := crc32.NewIEEE()
	n, err := dataCrc.Write(shardData)
	require.NoError(t, err)
	require.Equal(t, shardSize, n)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: diskID,
		Vuid:   vuid,
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	// concurrency
	concurrency := 100
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	errChan := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			putShardArg := &bnapi.PutShardArgs{
				Vuid: vuid,
				Bid:  bid,
				Size: int64(shardSize),
				Body: bytes.NewReader(shardData),
			}
			_, err := client.PutShard(ctx, host, putShardArg)
			errChan <- err
		}()
	}
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		e := <-errChan
		if e != nil {
			return
		}
	}
	t.Fatalf("put shard concurrency limit failed")
}
