// Copyright 2024 The CubeFS Authors.
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
	crand "crypto/rand"
	"fmt"
	"hash/crc32"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util"
)

const maxBlock = int64(4<<20) / _blockV2

func randData(size int) []byte {
	buff := make([]byte, size)
	crand.Read(buff)
	return buff
}

func newTestBlobNodeService2(t *testing.T, path string, chunksize int64,
) (*Service, *mockClusterMgr, func()) {
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+path)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(workDir, 0o755))
	storePath := filepath.Join(workDir, "storefile")
	require.NoError(t, os.WriteFile(storePath, nil, 0o644))

	mcm := mockClusterMgr{
		reqIdx: _mockDiskIdBase,
		disks:  []mockDiskInfo{},
	}
	cc := &clustermgr.Config{}
	cc.Hosts = []string{runMockClusterMgr(&mcm)}

	conf := Config{
		HostInfo: core.HostInfo{
			IDC:      "testIdc",
			Rack:     "testRack",
			DiskType: proto.DiskTypeHDD,
		},
		Disks: []core.Config{
			{
				BaseConfig: core.BaseConfig{Path: storePath},
				Store: core.StoreConfig{
					Path:                 storePath,
					EngineConfig:         iouring.Config{FilePath: storePath},
					UseMockIOURINGEngine: true,
				},
			},
		},
		DiskConfig:           core.RuntimeConfig{DiskReservedSpaceB: 1, CompactReservedSpaceB: 1},
		Clustermgr:           cc,
		HeartbeatIntervalSec: 600,
	}
	service, err := NewService(conf)
	require.NoError(t, err)

	// modify mock disk list
	diskInfos := make([]mockDiskInfo, 0)
	for _, ds := range service.Disks {
		di := mockDiskInfo{diskId: ds.ID(), path: ds.GetConfig().Path, status: proto.DiskStatusNormal}
		diskInfos = append(diskInfos, di)
		_, err = ds.CreateChunk(context.Background(), 2001, chunksize)
		require.NoError(t, err)
	}
	mcm.disks = diskInfos

	return service, &mcm, func() { os.RemoveAll(workDir) }
}

func runTestServer2(name string, service *Service) (string, func()) {
	router := &rpc2.Router{}
	router.Register("/v2/shard/put", service.ShardPutV2)
	router.Register("/v2/shard/get", service.ShardGetV2)

	addr := fmt.Sprintf("127.0.0.1:%d", util.GenUnusedPort())

	rpc2Server := &rpc2.Server{
		Name:      name,
		Addresses: []rpc2.NetworkAddress{{Network: "tcp", Address: addr}},
		Handler:   router.MakeHandler(),
	}
	go func() {
		if err := rpc2Server.Serve(); err != nil && err != rpc2.ErrServerClosed {
			panic(fmt.Errorf("rpc2 Server exits, err: %v", err))
		}
	}()
	rpc2Server.WaitServe()
	return addr, func() {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { rpc2Server.Shutdown(ctx) }()
		cancel()
	}
}

func TestBlobnodeShard2Error(t *testing.T) {
	name := "BlobnodeShard2Error"
	service, _, remove := newTestBlobNodeService2(t, name, 1<<20)
	defer cleanTestBlobNodeService(service)
	defer remove()

	host, shutdown := runTestServer2(name, service)
	defer shutdown()

	client := blobnode.New2(rpc2.Client{})
	ctx := context.Background()

	diskid := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	putArgs := func(diskid proto.DiskID, vuid proto.Vuid, bid proto.BlobID, size int64,
	) *blobnode.PutShardArgs {
		return &blobnode.PutShardArgs{
			DiskID: diskid,
			Vuid:   vuid,
			Bid:    bid,
			Size:   size,
			Body:   util.DiscardReader(int(size)),
		}
	}
	for _, args := range []*blobnode.PutShardArgs{
		putArgs(0, vuid, bid, 1),
		putArgs(111, vuid, bid, 1),
		putArgs(diskid, 0, bid, 1),
		putArgs(diskid, 2222, bid, 1),
		putArgs(diskid, vuid, 0, 1),
		putArgs(diskid, vuid, bid, 1<<33),
		putArgs(diskid, vuid, bid, 3<<20),
	} {
		_, err := client.PutShard(ctx, host, args)
		require.Error(t, err)
	}
	{
		args := putArgs(diskid, vuid, bid, 64<<10)
		args.Size++
		_, err := client.PutShard(ctx, host, args)
		require.Error(t, err)
	}

	getArgs := func(diskid proto.DiskID, vuid proto.Vuid, bid proto.BlobID,
	) *blobnode.GetShardArgs {
		return &blobnode.GetShardArgs{
			DiskID: diskid,
			Vuid:   vuid,
			Bid:    bid,
		}
	}
	for _, args := range []*blobnode.GetShardArgs{
		getArgs(0, vuid, bid),
		getArgs(111, vuid, bid),
		getArgs(diskid, 2222, bid),
	} {
		_, _, err := client.GetShard(ctx, host, args)
		require.Error(t, err)
	}
}

func TestBlobnodeShard2Put(t *testing.T) {
	name := "BlobnodeShard2Put"
	service, _, remove := newTestBlobNodeService2(t, name, 1<<30)
	defer cleanTestBlobNodeService(service)
	defer remove()

	host, shutdown := runTestServer2(name, service)
	defer shutdown()

	client := blobnode.New2(rpc2.Client{})
	ctx := context.Background()

	diskid := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	uargs := &blobnode.PutShardArgs{
		DiskID: diskid,
		Vuid:   vuid,
	}
	dargs := &blobnode.RangeGetShardArgs{
		GetShardArgs: blobnode.GetShardArgs{
			DiskID: diskid,
			Vuid:   vuid,
		},
	}

	run := func(size int64) {
		data := randData(int(size))
		uargs.Bid = bid
		uargs.Size = size
		uargs.Body = bytes.NewReader(data)
		crc, err := client.PutShard(ctx, host, uargs)
		require.NoError(t, err)
		require.Equal(t, uint32(0), crc) // TODO: crc

		var body io.ReadCloser
		dargs.Bid = bid
		dargs.Size = size
		if mrand.Int()%2 == 0 {
			dargs.WithCrc = true
		}
		if mrand.Int()%2 == 0 {
			body, _, err = client.GetShard(ctx, host, &dargs.GetShardArgs)
		} else {
			body, _, err = client.RangeGetShard(ctx, host, dargs)
		}
		require.NoError(t, err)

		crcfull := crc32.ChecksumIEEE(data)
		_, err = io.ReadFull(body, data)
		require.NoError(t, err)
		body.Close()
		require.Equal(t, crcfull, crc32.ChecksumIEEE(data))

		bid++
	}

	maxsize := maxBlock * (_blockV2 - 4)
	for _, size := range []int64{
		1, 4, 511, 512, 513,
		_blockV2 - 4,
		_blockV2 - 1,
		_blockV2,
		_blockV2 + 1,
		_blockV2*2 + 1,
		(_blockV2-4)*maxBlock - 1,
		(_blockV2 - 4) * maxBlock,
	} {
		run(size)
	}
	for range [100]struct{}{} {
		run(mrand.Int63n(maxsize) + 1)
	}
}

func TestBlobnodeShard2Get(t *testing.T) {
	name := "BlobnodeShard2Get"
	service, _, remove := newTestBlobNodeService2(t, name, 1<<30)
	defer cleanTestBlobNodeService(service)
	defer remove()

	host, shutdown := runTestServer2(name, service)
	defer shutdown()

	client := blobnode.New2(rpc2.Client{})
	ctx := context.Background()

	diskid := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	dargs := &blobnode.RangeGetShardArgs{
		GetShardArgs: blobnode.GetShardArgs{
			DiskID: diskid,
			Vuid:   vuid,
			Bid:    bid,
		},
	}

	maxsize := maxBlock * (_blockV2 - 4)
	data := randData(int(maxsize))
	_, err := client.PutShard(ctx, host, &blobnode.PutShardArgs{
		DiskID: diskid,
		Vuid:   vuid,
		Bid:    bid,
		Size:   maxsize,
		Body:   bytes.NewBuffer(data),
	})
	require.NoError(t, err)

	run := func(offset, size int64) {
		var body io.ReadCloser
		dargs.Offset = offset
		dargs.Size = size
		if mrand.Int()%2 == 0 {
			dargs.WithCrc = true
		}
		body, _, err = client.RangeGetShard(ctx, host, dargs)
		require.NoError(t, err)

		buff := make([]byte, size)
		_, err = io.ReadFull(body, buff)
		require.NoError(t, err)
		body.Close()
		require.Equal(t, crc32.ChecksumIEEE(data[offset:offset+size]), crc32.ChecksumIEEE(buff))
	}
	run(0, maxsize)
	run(1, 1)
	run(1, maxsize-1)
	run(maxsize-1, 1)
	for range [100]struct{}{} {
		offset := mrand.Int63n(maxsize - 1)
		size := mrand.Int63n(maxsize-offset) + 1
		run(offset, size)
	}
}

func TestBlobnodeShard2Append(t *testing.T) {
	name := "BlobnodeShard2Append"
	service, _, remove := newTestBlobNodeService2(t, name, 1<<30)
	defer cleanTestBlobNodeService(service)
	defer remove()

	host, shutdown := runTestServer2(name, service)
	defer shutdown()

	client := blobnode.New2(rpc2.Client{})
	ctx := context.Background()

	diskid := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	bid := proto.BlobID(30001)

	uargs := &blobnode.PutShardArgs{
		DiskID: diskid,
		Vuid:   vuid,
	}
	dargs := &blobnode.GetShardArgs{
		DiskID: diskid,
		Vuid:   vuid,
	}

	maxsize := maxBlock * (_blockV2 - 4)
	run := func(size int64) {
		data := randData(int(size))
		putData := data[:]
		uargs.Length = 0
		for idx := range [11]struct{}{} {
			if len(putData) == 0 {
				break
			}
			appendSize := mrand.Intn(len(putData)) + 1
			if idx == 9 {
				appendSize = len(putData)
			}
			uargs.Bid = bid
			uargs.Size = int64(appendSize)

			t.Logf("size:%d next:%d append:%d length:%d\n",
				size, uargs.Length+int64(appendSize), appendSize, uargs.Length)

			uargs.Body = bytes.NewReader(putData[:appendSize])
			_, err := client.PutShard(ctx, host, uargs)
			require.NoError(t, err)
			putData = putData[appendSize:]
			uargs.Length += int64(appendSize)
		}

		dargs.Bid = bid
		body, _, err := client.GetShard(ctx, host, dargs)
		require.NoError(t, err)

		crcfull := crc32.ChecksumIEEE(data)
		_, err = io.ReadFull(body, data)
		require.NoError(t, err)
		body.Close()
		require.Equal(t, crcfull, crc32.ChecksumIEEE(data))

		bid++
	}
	run(1)
	run(67)
	run(_blockV2)
	run(maxsize)
	for range [10]struct{}{} {
		run(mrand.Int63n(maxsize) + 1)
	}
}
