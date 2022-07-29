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
	"encoding/json"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestChunkReport(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "ChunkReport")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	ctx := context.TODO()

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)
	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: proto.DiskID(101),
		Vuid:   vuid,
	}

	ds, exist := service.Disks[diskID]
	require.True(t, exist)

	err := client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)

	cs, _ := ds.GetChunkStorage(vuid)
	require.Equal(t, false, cs.IsDirty())

	service.reportChunkInfoToClusterMgr()
	require.Equal(t, false, cs.IsDirty())

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
	require.NoError(t, err)
	require.Equal(t, true, cs.IsDirty())

	service.reportChunkInfoToClusterMgr()
	require.Equal(t, false, cs.IsDirty())

	putShardArg.Bid = proto.BlobID(30002)
	putShardArg.Body = bytes.NewReader(shardData)
	_, err = client.PutShard(ctx, host, putShardArg)
	require.NoError(t, err)

	putShardArg.Bid = proto.BlobID(30003)
	putShardArg.Body = bytes.NewReader(shardData)
	_, err = client.PutShard(ctx, host, putShardArg)
	require.NoError(t, err)

	require.Equal(t, true, cs.IsDirty())

	service.reportChunkInfoToClusterMgr()
	require.Equal(t, false, cs.IsDirty())
}

func TestChunkReport2(t *testing.T) {
	ctx := context.Background()

	lock := sync.Mutex{}
	diskId := proto.DiskID(101)
	mockClusterMgrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/diskid/alloc") {
			lock.Lock()
			defer lock.Unlock()
			b, _ := json.Marshal(cmapi.DiskIDAllocRet{DiskID: diskId})
			_, _ = w.Write(b)
			diskId++
			return
		}
		if strings.HasPrefix(req.URL.Path, "/chunk/report") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	cc := &cmapi.Config{}
	cc.Hosts = []string{mockClusterMgrServer.URL}

	workDir, err := ioutil.TempDir(os.TempDir(), defaultSvrTestDir+"ChunkReport2")
	require.NoError(t, err)

	t.Log("work dir: ", workDir)

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)

	path1 := filepath.Join(workDir, "disk1")
	path2 := filepath.Join(workDir, "disk2")

	// must create meta dir
	err = os.MkdirAll(core.GetMetaPath(path1, ""), 0o755)
	require.NoError(t, err)
	err = os.MkdirAll(core.GetMetaPath(path2, ""), 0o755)
	require.NoError(t, err)

	conf := Config{
		HostInfo: core.HostInfo{
			IDC:  "testIdc",
			Rack: "testRack",
		},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{RocksdbOption: kvstore.RocksDBOption{WriteBufferSize: 1024}}},
			{BaseConfig: core.BaseConfig{Path: path2, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{RocksdbOption: kvstore.RocksDBOption{WriteBufferSize: 1024}}},
		},
		Clustermgr:             cc,
		HeartbeatIntervalSec:   600,
		ChunkReportIntervalSec: 1,
	}
	service, err := NewService(conf)
	require.NoError(t, err)
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	diskID := proto.DiskID(101)
	vuid := proto.Vuid(2001)

	err = client.CreateChunk(ctx, host, &bnapi.CreateChunkArgs{DiskID: proto.DiskID(101), Vuid: vuid})
	require.NoError(t, err)

	ds, exist := service.Disks[diskID]
	require.True(t, exist)

	cs, exist := ds.GetChunkStorage(vuid)
	require.True(t, exist)

	cs.SetDirty(true)

	time.Sleep(2 * time.Second)

	service.reportChunkInfoToClusterMgr()
}
