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
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

var testServiceCfg = &Config{
	Region:    "test-region",
	IDC:       []string{"z0", "z1", "z2"},
	ClusterID: 1,
	Readonly:  false,
	DBPath:    "/tmp/tmpsvrdb-" + randID(),
	CodeModePolicies: []codemode.Policy{
		{
			ModeName:  codemode.EC15P12.Name(),
			MinSize:   1048577,
			MaxSize:   1073741824,
			SizeRatio: 0.8,
			Enable:    true,
		},
		{
			ModeName:  codemode.EC6P6.Name(),
			MinSize:   0,
			MaxSize:   1048576,
			SizeRatio: 0.2,
			Enable:    true,
		},
	},
	ClusterCfg:               map[string]interface{}{},
	ClusterReportIntervalS:   1,
	MetricReportIntervalM:    1,
	HeartbeatNotifyIntervalS: 1,
	ChunkSize:                17179869184,
	RaftConfig: RaftConfig{
		ServerConfig: raftserver.Config{
			NodeId:       1,
			ListenPort:   GetFreePort(),
			TickInterval: 1,
			ElectionTick: 2,
			WalDir:       "/tmp/tmpsvrraftwal-" + randID(),
			Members:      []raftserver.Member{{NodeID: 1, Host: "127.0.0.1:60110", Learner: false}},

			TickIntervalMs: 20,
		},
	},
	DiskMgrConfig: diskmgr.DiskMgrConfig{
		RefreshIntervalS: 300,
		RackAware:        false,
		HostAware:        true,
	},
}

var (
	cleanWG sync.WaitGroup
	newCtx  = func() context.Context {
		_, ctx := trace.StartSpanFromContext(context.Background(), "")
		return ctx
	}
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	cleanWG.Wait()
	os.Exit(exitCode)
}

func randID() string {
	return fmt.Sprintf("%010d", rand.Intn(100000000))
}

func cleanTestService(testService *Service) {
	testService.Close()
	os.RemoveAll(testService.DBPath)
	os.RemoveAll(testService.RaftConfig.ServerConfig.WalDir)
}

func initTestService(t testing.TB) (*Service, func()) {
	cfg := *testServiceCfg
	cfg.DBPath = "/tmp/tmpsvrdb-" + randID()
	cfg.NormalDBPath = cfg.DBPath + "/normaldb-" + randID()
	cfg.KvDBPath = cfg.DBPath + "/kvdb-" + randID()
	cfg.VolumeMgrConfig.VolumeDBPath = cfg.DBPath + "/volumedb-" + randID()
	cfg.RaftConfig.RaftDBPath = cfg.DBPath + "/raftdb-" + randID()
	cfg.RaftConfig.ServerConfig.WalDir = "/tmp/tmpsvrraftwal-" + randID()
	cfg.RaftConfig.ServerConfig.ListenPort = GetFreePort()
	cfg.RaftConfig.ServerConfig.Members = []raftserver.Member{
		{NodeID: 1, Host: fmt.Sprintf("127.0.0.1:%d", GetFreePort()), Learner: false},
	}
	os.Mkdir(cfg.DBPath, 0o755)
	testService, err := New(&cfg)
	require.NoError(t, err)

	cleanWG.Add(1)
	return testService, func() {
		go func() {
			cleanTestService(testService)
			cleanWG.Done()
		}()
	}
}

func initTestClusterClient(testService *Service) *clustermgr.Client {
	ph := rpc.DefaultRouter.Router.PanicHandler
	rpc.DefaultRouter = rpc.New()
	rpc.DefaultRouter.Router.PanicHandler = ph
	mux := NewHandler(testService)
	server := httptest.NewServer(mux)
	return clustermgr.New(&clustermgr.Config{
		LbConfig: rpc.LbConfig{
			Hosts: []string{server.URL},
		},
	})
}

func TestBidAlloc(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	// test bid alloc
	{
		ret, err := testClusterClient.AllocBid(ctx, &clustermgr.BidScopeArgs{Count: 10})
		require.NoError(t, err)
		require.Equal(t, proto.BlobID(1), ret.StartBid)
		require.Equal(t, proto.BlobID(10), ret.EndBid)

		_, err = testClusterClient.AllocBid(ctx, &clustermgr.BidScopeArgs{Count: 100001})
		require.Error(t, err)
	}
}

type mockWriter struct{}

func (mockWriter) Write(data []byte) (int, error) { return len(data), nil }
func (mockWriter) Header() http.Header            { return http.Header{} }
func (mockWriter) WriteHeader(statusCode int)     {}

func TestNewService(t *testing.T) {
	cfg := *testServiceCfg

	cfg.ClusterReportIntervalS = 0
	cfg.MetricReportIntervalM = 0
	cfg.HeartbeatNotifyIntervalS = 0

	cfg.RaftConfig.ServerConfig.WalDir = "/tmp/tmpsvrraftwal-" + randID()
	cfg.ClusterCfg[proto.VolumeReserveSizeKey] = "20000000"
	os.Mkdir(cfg.DBPath, 0o755)

	testService, err := New(&cfg)
	require.NoError(t, err)
	require.NotNil(t, testService)

	testService.report(context.Background())
	testService.metricReport(context.Background())

	req, err := http.NewRequest(http.MethodPost, "/", nil)
	require.NoError(t, err)

	testService.forwardToLeader(mockWriter{}, req)

	cleanTestService(testService)
}

func GetFreePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
