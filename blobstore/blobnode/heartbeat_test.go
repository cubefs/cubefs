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
	"encoding/json"
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

func TestHeartbeat(t *testing.T) {
	service, _ := newTestBlobNodeService(t, "Heartbeat")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	ctx := context.TODO()
	dis, err := client.Stat(ctx, host)
	require.NoError(t, err)
	require.Equal(t, 2, len(dis))

	diskID := proto.DiskID(101)

	require.Equal(t, proto.DiskStatusNormal, service.Disks[diskID].Status())

	service.heartbeatToClusterMgr()
}

func TestHeartbeat2(t *testing.T) {
	diskID := proto.DiskID(101)
	mockClusterMgrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/diskid/alloc") {
			b, _ := json.Marshal(cmapi.DiskIDAllocRet{DiskID: diskID})
			_, _ = w.Write(b)
			diskID++
			return
		}
		if strings.HasPrefix(req.URL.Path, "/disk/heartbeat") {
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

	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"Heartbeat2")
	require.NoError(t, err)
	defer os.Remove(workDir)

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)

	path1 := filepath.Join(workDir, "disk1")

	// must create meta dir
	err = os.MkdirAll(core.GetMetaPath(path1, ""), 0o755)
	require.NoError(t, err)

	conf := Config{
		HostInfo: core.HostInfo{
			IDC:      "testIdc",
			Rack:     "testRack",
			DiskType: proto.DiskTypeHDD,
		},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{}},
		},
		Clustermgr:           cc,
		HeartbeatIntervalSec: 1,
	}
	service, err := NewService(conf)
	require.NoError(t, err)
	defer cleanTestBlobNodeService(service)

	_ = runTestServer(service)

	time.Sleep(2 * time.Second)

	service.heartbeatToClusterMgr()
}

func TestHeartbeat3(t *testing.T) {
	lock := sync.RWMutex{}
	diskID := proto.DiskID(101)
	mockClusterMgrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/diskid/alloc") {
			lock.Lock()
			defer lock.Unlock()
			b, _ := json.Marshal(cmapi.DiskIDAllocRet{DiskID: diskID})
			_, _ = w.Write(b)
			diskID++
			return
		}
		if strings.HasPrefix(req.URL.Path, "/disk/heartbeat") {
			result := &cmapi.DisksHeartbeatRet{
				Disks: []*cmapi.DiskHeartbeatRet{
					{DiskID: proto.DiskID(11111)},
					{DiskID: proto.DiskID(101), Status: proto.DiskStatusNormal, ReadOnly: false},
					{DiskID: proto.DiskID(102), Status: proto.DiskStatusRepairing, ReadOnly: true},
				},
			}
			b, _ := json.Marshal(result)
			_, _ = w.Write(b)
			return
		}
		if implementExtendCodemode(w, req) {
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	cc := &cmapi.Config{}
	cc.Hosts = []string{mockClusterMgrServer.URL}

	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"Heartbeat3")
	require.NoError(t, err)
	defer os.Remove(workDir)

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
		Clustermgr:           cc,
		HeartbeatIntervalSec: 600,
	}
	service, err := NewService(conf)
	require.NoError(t, err)
	defer cleanTestBlobNodeService(service)

	_ = runTestServer(service)

	service.heartbeatToClusterMgr()
}
