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
	require.Equal(t, true, cs.IsDirty())

	service.reportChunkInfoToClusterMgr()
	require.Equal(t, true, cs.IsDirty())

	service.Conf.ChunkReportMaxTimeSec = 1

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

	service.chunkReportRecords[vuid] = chunkReportRecord{at: time.Now().Add(-time.Second)}
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

	recorded := service.chunkReportRecords[vuid]
	recorded.at = time.Now().Add(-time.Second)
	service.chunkReportRecords[vuid] = recorded
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
		if implementExtendCodemode(w, req) {
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	cc := &cmapi.Config{}
	cc.Hosts = []string{mockClusterMgrServer.URL}

	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"ChunkReport2")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

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
			IDC:      "testIdc",
			Rack:     "testRack",
			DiskType: proto.DiskTypeHDD,
		},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{}},
			{BaseConfig: core.BaseConfig{Path: path2, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{}},
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
}

func TestChunkReportHit(t *testing.T) {
	total := uint64(1000)
	now := time.Now()
	rule := chunkReportRule{now: now, sizePct: 5, highPct: 95, timeMax: time.Hour}

	require.False(t, rule.hit(0, total, nil))
	require.False(t, rule.hit(40, total, nil))
	require.True(t, rule.hit(50, total, nil))
	require.True(t, rule.hit(950, total, nil))

	record := &chunkReportRecord{used: 100, at: now}
	require.False(t, rule.hit(100, total, record))
	rule.now = now.Add(time.Minute)
	require.False(t, rule.hit(149, total, record))
	require.True(t, rule.hit(150, total, record))
	rule.now = now.Add(time.Hour)
	require.True(t, rule.hit(101, total, record))
	rule.now = now.Add(time.Hour - time.Second)
	require.False(t, rule.hit(101, total, record))
	rule.now = now
	require.True(t, rule.hit(950, total, record))
	require.True(t, rule.hit(50, total, record))
	require.True(t, rule.hit(950, total, &chunkReportRecord{used: 949, at: now}))
	require.False(t, rule.hit(955, total, &chunkReportRecord{used: 960, at: now}))

	disabled := chunkReportRule{now: now, sizePct: -1, highPct: -1, timeMax: time.Hour}
	require.False(t, disabled.hit(950, total, &chunkReportRecord{used: 949, at: now}))
	disabled.highPct = 95
	require.False(t, disabled.hit(150, total, record))
	disabled.highPct = -1
	disabled.timeMax = -time.Second
	disabled.now = now.Add(time.Hour)
	require.False(t, disabled.hit(101, total, record))
	disabled.timeMax = time.Hour
	require.True(t, disabled.hit(101, total, record))
}
