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
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	defaultSvrTestDir = "BlobNodeSvrTestDir_"
)

var (
	any     = gomock.Any()
	errMock = errors.New("fake error")
)

var cleanWG sync.WaitGroup

func TestMain(m *testing.M) {
	log.Info("start blobnode testing ......")
	exitCode := m.Run()
	log.Info("wait background cleaning ...")
	cleanWG.Wait()
	log.Info("ended blobnode testing ......")
	os.Exit(exitCode)
}

func TestService(t *testing.T) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "TestService")

	service, _ := newTestBlobNodeService(t, "Service")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	dis, err := client.Stat(ctx, host)
	require.NoError(t, err)
	require.Equal(t, 2, len(dis))

	diskInfoArg := &bnapi.DiskStatArgs{
		DiskID: proto.DiskID(101),
	}
	di, err := client.DiskInfo(ctx, host, diskInfoArg)
	require.NoError(t, err)
	require.Equal(t, proto.DiskID(101), di.DiskID)

	diskInfoArg.DiskID = proto.DiskID(10001)
	_, err = client.DiskInfo(ctx, host, diskInfoArg)
	require.Error(t, err)

	req, err := http.NewRequest(http.MethodGet, host+"/debug/stat", nil)
	require.NoError(t, err)
	assertRequest(t, req)

	url := host + "/chunk/compact/vuid/2001"
	req, err = http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	assertRequest(t, req)

	createChunkArg := &bnapi.CreateChunkArgs{
		DiskID: proto.DiskID(101),
		Vuid:   proto.Vuid(2001),
	}
	err = client.CreateChunk(ctx, host, createChunkArg)
	require.NoError(t, err)
	req, err = http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	assertRequest(t, req)

	_ = client.Close(ctx, host)
	_ = client.IsOnline(ctx, host)
	_ = client.String(ctx, host)

	service.handleDiskIOError(ctx, proto.DiskID(10001), bloberr.ErrDiskBroken)
	service.handleDiskIOError(ctx, proto.DiskID(101), bloberr.ErrDiskBroken)
}

func TestHandleDiskIOError(t *testing.T) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "TestService")

	service, _ := newTestBlobNodeService(t, "Service")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})

	dis, err := client.Stat(ctx, host)
	require.NoError(t, err)
	require.Equal(t, 2, len(dis))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			service.handleDiskIOError(ctx, proto.DiskID(101), bloberr.ErrDiskBroken)
		}()
	}
	wg.Wait()

	// 101 broken
	diskInfoArg := &bnapi.DiskStatArgs{
		DiskID: proto.DiskID(101),
	}
	di, err := client.DiskInfo(ctx, host, diskInfoArg)
	require.NoError(t, err)
	require.Equal(t, di.Status, proto.DiskStatusBroken)

	// 102 ok
	diskInfoArg = &bnapi.DiskStatArgs{
		DiskID: proto.DiskID(102),
	}
	di, err = client.DiskInfo(ctx, host, diskInfoArg)
	require.NoError(t, err)
	require.Equal(t, proto.DiskID(102), di.DiskID)
	require.Equal(t, di.Status, proto.DiskStatusNormal)
}

func TestHandleGetGlobalConfig(t *testing.T) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "TestHandleGetGlobalConfig")

	service, _ := newTestBlobNodeService(t, "Service")
	defer cleanTestBlobNodeService(service)

	// get from cm
	val, err := service.getGlobalConfig(ctx, proto.ChunkOversoldRatioKey)
	require.NoError(t, err)
	require.NotEqual(t, "", val)

	// get from cache
	val, err = service.getGlobalConfig(ctx, proto.ChunkOversoldRatioKey)
	require.NoError(t, err)
	require.NotEqual(t, "", val)
}

func TestHandleDiskDrop(t *testing.T) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "TestService")

	service, _ := newTestBlobNodeService(t, "Service")
	defer cleanTestBlobNodeService(service)

	host := runTestServer(service)
	client := bnapi.New(&bnapi.Config{})
	// 2 disk
	dis, err := client.Stat(ctx, host)
	require.NoError(t, err)
	require.Equal(t, 2, len(dis))

	// create chunk in disk 101
	service.lock.RLock()
	ds, exist := service.Disks[proto.DiskID(101)]
	service.lock.RUnlock()
	require.True(t, exist)
	require.Equal(t, proto.DiskID(101), ds.ID())
	delCh := make(chan struct{}, 1)
	cs, err := ds.CreateChunk(ctx, proto.Vuid(146095996936), 8) // creat chunk
	require.NoError(t, err)
	require.Equal(t, int64(1), ds.DiskInfo().UsedChunkCnt) // UsedChunkCnt is len(ds.Chunks

	// relese chunk, and delete db meta
	go func() {
		time.Sleep(time.Millisecond * 100)
		ds.ReleaseChunk(ctx, proto.Vuid(146095996936), true)
		ds.(*disk.DiskStorageWrapper).SuperBlock.DeleteChunk(ctx, cs.ID())
		delCh <- struct{}{}
	}()

	service.lock.RLock()
	ds, exist = service.Disks[proto.DiskID(101)]
	service.lock.RUnlock()
	require.True(t, exist)
	require.Equal(t, proto.DiskID(101), ds.ID())
	service.Conf.DiskStatusCheckIntervalSec = 1
	service.handleDiskDrop(ctx, ds)
	require.Equal(t, ds.Status(), proto.DiskStatusDropped)

	// release, check ds is clean
	<-delCh
	time.Sleep(time.Millisecond * 1100)
	service.lock.RLock()
	ds, exist = service.Disks[proto.DiskID(101)]
	service.lock.RUnlock()
	require.False(t, exist)

	// 101 disk is delete, NoSuchDisk
	diskInfoArg := &bnapi.DiskStatArgs{
		DiskID: proto.DiskID(101),
	}
	_, err = client.DiskInfo(ctx, host, diskInfoArg)
	require.NotNil(t, err)
	require.Equal(t, bloberr.ErrNoSuchDisk.Error(), err.Error())

	// 102 ok
	diskInfoArg = &bnapi.DiskStatArgs{
		DiskID: proto.DiskID(102),
	}
	di, err := client.DiskInfo(ctx, host, diskInfoArg)
	require.NoError(t, err)
	require.Equal(t, proto.DiskID(102), di.DiskID)
	require.Equal(t, di.Status, proto.DiskStatusNormal)
}

func TestServiceError(t *testing.T) {
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"ServiceError")
	require.NoError(t, err)
	defer os.Remove(workDir)

	path1 := filepath.Join(workDir, "disk1")
	path2 := filepath.Join(workDir, "disk2")

	err = os.Mkdir(path1, 0o755)
	require.NoError(t, err)
	err = os.Mkdir(path2, 0o755)
	require.NoError(t, err)

	defer os.Remove(path1)
	defer os.Remove(path2)

	mcm := mockClusterMgr{
		reqIdx: _mockDiskIdBase,
		disks: []mockDiskInfo{
			{diskId: proto.DiskID(_mockDiskIdBase + 1), path: path1, status: proto.DiskStatusNormal},
			{diskId: proto.DiskID(_mockDiskIdBase + 2), path: path2, status: proto.DiskStatusNormal},
		},
	}

	mcmURL := runMockClusterMgr(&mcm)

	cc := &cmapi.Config{}
	cc.Hosts = []string{mcmURL}

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)
	conf := Config{
		HostInfo: core.HostInfo{
			IDC:      "testIdc",
			Rack:     "testRack",
			Host:     "127.0.0.1",
			DiskType: proto.DiskTypeHDD,
		},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{}},
		},
		Clustermgr: cc,
	}
	_, err = NewService(conf)
	require.Error(t, err)
}

func newTestBlobNodeService(t *testing.T, path string) (*Service, *mockClusterMgr) {
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+path)
	require.NoError(t, err)

	path1 := filepath.Join(workDir, "disk1")
	path2 := filepath.Join(workDir, "disk2")

	mcm := mockClusterMgr{
		reqIdx: _mockDiskIdBase,
		disks:  []mockDiskInfo{},
	}

	mcmURL := runMockClusterMgr(&mcm)

	cc := &cmapi.Config{}
	cc.Hosts = []string{mcmURL}

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)

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
		DiskConfig:           core.RuntimeConfig{DiskReservedSpaceB: 1, CompactReservedSpaceB: 1},
		Clustermgr:           cc,
		HeartbeatIntervalSec: 600,
	}
	if path == "iopslimit" {
		ioFlowStat, _ := flow.NewIOFlowStat("default", false)
		ioview := flow.NewDiskViewer(ioFlowStat)
		conf.DiskConfig.DataQos = qos.Config{
			DiskViewer: ioview,
			StatGetter: ioFlowStat,
			FlowConf: qos.FlowConfig{
				Level: map[string]qos.LevelFlowConfig{
					bnapi.ReadIO.String():       {MBPS: 20},
					bnapi.WriteIO.String():      {MBPS: 20},
					bnapi.DeleteIO.String():     {MBPS: 10},
					bnapi.BackgroundIO.String(): {MBPS: 10},
				},
			},
		}
	}
	if path == "bpslimit" {
		ioFlowStat, _ := flow.NewIOFlowStat("default", false)
		ioview := flow.NewDiskViewer(ioFlowStat)
		conf.DiskConfig.DataQos = qos.Config{
			DiskViewer: ioview,
			StatGetter: ioFlowStat,
			FlowConf: qos.FlowConfig{
				Level: map[string]qos.LevelFlowConfig{
					bnapi.ReadIO.String():       {MBPS: 10},
					bnapi.WriteIO.String():      {MBPS: 10},
					bnapi.DeleteIO.String():     {MBPS: 5},
					bnapi.BackgroundIO.String(): {MBPS: 5},
				},
			},
		}
	}
	service, err := NewService(conf)
	require.NoError(t, err)

	// modify mock disk list
	diskInfos := make([]mockDiskInfo, 0)
	for _, ds := range service.Disks {
		di := mockDiskInfo{diskId: ds.ID(), path: ds.GetConfig().Path, status: proto.DiskStatusNormal}
		diskInfos = append(diskInfos, di)
	}
	mcm.disks = diskInfos

	return service, &mcm
}

func TestService_CmdpChunk(t *testing.T) {
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"TestService_CmdpChunkCompact")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "disk1")
	path2 := filepath.Join(workDir, "disk2")

	mcm := mockClusterMgr{
		reqIdx: _mockDiskIdBase,
		disks: []mockDiskInfo{
			{diskId: proto.DiskID(_mockDiskIdBase + 1), path: path1, status: proto.DiskStatusRepaired},
			{diskId: proto.DiskID(_mockDiskIdBase + 2), path: path2, status: proto.DiskStatusRepaired},
		},
	}

	mcmURL := runMockClusterMgr(&mcm)

	cc := &cmapi.Config{}
	cc.Hosts = []string{mcmURL}

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)

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
		DiskConfig:           core.RuntimeConfig{DiskReservedSpaceB: 1, CompactReservedSpaceB: 1},
		Clustermgr:           cc,
		HeartbeatIntervalSec: 600,
	}

	conf.DiskConfig.MustMountPoint = true
	service, err := NewService(conf)
	require.NoError(t, err)
	require.Equal(t, 0, len(service.Disks)) // len(service.Disks) is 0

	conf.DiskConfig.MustMountPoint = false
	service, err = NewService(conf)
	require.NoError(t, err)
	require.Equal(t, 2, len(service.Disks)) // len(conf.Disks) == len(service.Disks), is 2

	ctx := context.Background()
	cs, err := service.Disks[proto.DiskID(101)].CreateChunk(ctx, proto.Vuid(2001), 1000)
	require.NoError(t, err)
	require.NotNil(t, cs)

	testServer := httptest.NewServer(NewHandler(service))
	// /chunk/release/diskid/{diskid}/vuid/{vuid}
	{
		totalUrl := testServer.URL + "/chunk/release/diskid/0/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 614, resp.StatusCode)
		defer resp.Body.Close()
	}

	service.Disks[101].SetStatus(proto.DiskStatusRepairing)
	{
		totalUrl := testServer.URL + "/chunk/release/diskid/101/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 625, resp.StatusCode)
		defer resp.Body.Close()
	}
	service.Disks[101].SetStatus(proto.DiskStatusNormal)
	// /chunk/create/diskid/{diskid}/vuid/{vuid}/chunksize/{chunksize}
	{
		totalUrl := testServer.URL + "/chunk/readonly/diskid/0/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 614, resp.StatusCode)
		defer resp.Body.Close()
	}

	// /chunk/readonly/diskid/{diskid}/vuid/{vuid}
	{
		totalUrl := testServer.URL + "/chunk/readonly/diskid/0/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 614, resp.StatusCode)
		defer resp.Body.Close()
	}

	{
		totalUrl := testServer.URL + "/chunk/readonly/diskid/103/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 611, resp.StatusCode)
		defer resp.Body.Close()
	}

	err = cs.SetStatus(cmapi.ChunkStatusRelease)
	require.NoError(t, err)
	{
		totalUrl := testServer.URL + "/chunk/readonly/diskid/101/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 626, resp.StatusCode)
		defer resp.Body.Close()
	}

	// /chunk/readwrite/diskid/{diskid}/vuid/{vuid}
	{
		totalUrl := testServer.URL + "/chunk/readwrite/diskid/0/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 614, resp.StatusCode)
		defer resp.Body.Close()
	}

	{
		totalUrl := testServer.URL + "/chunk/readwrite/diskid/103/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 611, resp.StatusCode)
		defer resp.Body.Close()
	}

	// /chunk/list error
	{
		totalUrl := testServer.URL + "/chunk/list/diskid/0"
		resp, err := HTTPRequest(http.MethodGet, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 614, resp.StatusCode)
		defer resp.Body.Close()
	}

	// /chunk/stat error
	{
		totalUrl := testServer.URL + "/chunk/stat/diskid/0/vuid/2001"
		resp, err := HTTPRequest(http.MethodGet, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 614, resp.StatusCode)
		defer resp.Body.Close()
	}

	{
		totalUrl := testServer.URL + "/chunk/stat/diskid/103/vuid/2001"
		resp, err := HTTPRequest(http.MethodGet, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 611, resp.StatusCode)
		defer resp.Body.Close()
	}

	{
		totalUrl := testServer.URL + "/chunk/compact/diskid/0/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 614, resp.StatusCode)
		defer resp.Body.Close()
	}

	{
		totalUrl := testServer.URL + "/chunk/compact/diskid/103/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 611, resp.StatusCode)
		defer resp.Body.Close()
	}

	{
		totalUrl := testServer.URL + "/chunk/compact/diskid/101/vuid/2002"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 621, resp.StatusCode)
		defer resp.Body.Close()
	}

	{
		totalUrl := testServer.URL + "/chunk/compact/diskid/101/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 200, resp.StatusCode)
		defer resp.Body.Close()
	}

	service.Conf.DiskConfig.AllowForceCompact = true
	{
		testServer = httptest.NewServer(NewHandler(service))
		totalUrl := testServer.URL + "/chunk/compact/diskid/101/vuid/2001"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		require.Equal(t, 200, resp.StatusCode)
		defer resp.Body.Close()
	}
}

func HTTPRequest(method string, url string) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	return resp, err
}

func cleanTestBlobNodeService(svr *Service) {
	cleanWG.Add(1)
	go cleanTestBlobNodeServiceBg(svr)
}

func cleanTestBlobNodeServiceBg(svr *Service) {
	defer cleanWG.Done()
	svr.Close()

	var dirs []string
	for i := range svr.Conf.Disks {
		_ = os.RemoveAll(svr.Conf.Disks[i].Path)
		dirs = append(dirs, filepath.Dir(svr.Conf.Disks[i].Path))
	}
	for i := range dirs {
		_ = os.RemoveAll(dirs[i])
	}
}

func runTestServer(svr *Service) string {
	testServer := httptest.NewServer(NewHandler(svr))
	return testServer.URL
}

func runMockClusterMgr(mcm *mockClusterMgr) string {
	r := mockClusterMgrRouter(mcm)
	testServer := httptest.NewServer(r)
	return testServer.URL
}

type mockDiskInfo struct {
	path   string
	diskId proto.DiskID
	status proto.DiskStatus
}

var _mockDiskIdBase = int64(100)

type mockClusterMgr struct {
	reqIdx  int64
	nodeIdx int32
	disks   []mockDiskInfo
}

func init() {
	rpc.RegisterArgsParser(&cmapi.ListOptionArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.ListVolumeUnitArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.DiskInfoArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.DisksHeartbeatArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.DiskSetArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.ReportChunkArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.GetVolumeArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.NodeInfoArgs{}, "json")
}

func implementExtendCodemode(w http.ResponseWriter, req *http.Request) bool {
	if strings.HasPrefix(req.URL.Path, "/config/get") {
		w.Write([]byte(`"[]"`))
		return true
	}
	return false
}

func mockClusterMgrRouter(service *mockClusterMgr) *rpc.Router {
	r := rpc.New()
	r.Handle(http.MethodGet, "/disk/list", service.DiskList, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/volume/unit/list", service.VolumeUnitList, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/diskid/alloc", service.DiskIdAlloc)
	r.Handle(http.MethodGet, "/disk/info", service.DiskInfo, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/disk/heartbeat", service.DiskHeartbeat, rpc.OptArgsBody())
	r.Handle(http.MethodPost, "/disk/add", service.DiskAdd, rpc.OptArgsBody())
	r.Handle(http.MethodPost, "/disk/set", service.DiskSet, rpc.OptArgsBody())
	r.Handle(http.MethodPost, "/chunk/report", service.ChunkReport, rpc.OptArgsBody())
	r.Handle(http.MethodGet, "/volume/get", service.VolumeGet, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/service/register", service.ServiceRegister, rpc.OptArgsBody())

	r.Handle(http.MethodPost, "/node/add", service.NodeAdd, rpc.OptArgsBody())
	r.Handle(http.MethodPost, "/node/drop", service.NodeDrop, rpc.OptArgsBody())
	r.Handle(http.MethodGet, "/node/info", service.NodeInfo, rpc.OptArgsQuery())

	r.Handle(http.MethodGet, "/config/get", service.ConfigGet, rpc.OptArgsQuery())

	return r
}

func (mcm *mockClusterMgr) DiskList(c *rpc.Context) {
	args := new(cmapi.ListOptionArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
	specialDiskId := proto.DiskID(math.MaxInt32)

	if args.Marker == specialDiskId {
		c.RespondJSON(&cmapi.ListDiskRet{})
		return
	}
	ret := &cmapi.ListDiskRet{}
	for _, d := range mcm.disks {
		info := &cmapi.BlobNodeDiskInfo{
			DiskInfo: cmapi.DiskInfo{
				Path:   d.path,
				Status: d.status,
			},
		}
		info.DiskID = d.diskId
		ret.Disks = append(ret.Disks, info)
	}

	ret.Marker = specialDiskId
	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) VolumeUnitList(c *rpc.Context) {
	args := new(cmapi.ListVolumeUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
	diskId := args.DiskID

	chunks := make([]*cmapi.VolumeUnitInfo, 0)
	vuid1, _ := proto.NewVuid(proto.Vid(2001), 1, 1)
	vuid2, _ := proto.NewVuid(proto.Vid(2002), 1, 1)
	vuid3, _ := proto.NewVuid(proto.Vid(2003), 1, 1)
	vuid4, _ := proto.NewVuid(proto.Vid(2003), 1, 2)
	chunks = append(chunks, &cmapi.VolumeUnitInfo{
		DiskID: diskId,
		Vuid:   vuid1,
	}, &cmapi.VolumeUnitInfo{
		DiskID: diskId,
		Vuid:   vuid2,
	}, &cmapi.VolumeUnitInfo{
		DiskID: diskId,
		Vuid:   vuid3,
	}, &cmapi.VolumeUnitInfo{
		DiskID: diskId,
		Vuid:   vuid4,
	})
	c.RespondJSON(&cmapi.ListVolumeUnitInfos{VolumeUnitInfos: chunks})
}

func (mcm *mockClusterMgr) DiskIdAlloc(c *rpc.Context) {
	args := new(cmapi.ListVolumeUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
	diskID := atomic.AddInt64(&mcm.reqIdx, 1)

	ret := &cmapi.DiskIDAllocRet{
		DiskID: proto.DiskID(diskID),
	}

	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) DiskInfo(c *rpc.Context) {
	args := new(cmapi.DiskInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
	ret := &cmapi.BlobNodeDiskInfo{}
	ret.DiskID = args.DiskID
	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) DiskHeartbeat(c *rpc.Context) {
	args := new(cmapi.DisksHeartbeatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
	ret := &cmapi.DisksHeartbeatRet{}

	for _, diskInfo := range args.Disks {
		diskRet := &cmapi.DiskHeartbeatRet{
			DiskID:   diskInfo.DiskID,
			Status:   proto.DiskStatusNormal,
			ReadOnly: true,
		}

		ret.Disks = append(ret.Disks, diskRet)
	}
	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) DiskAdd(c *rpc.Context) {
	args := new(cmapi.BlobNodeDiskInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
}

func (mcm *mockClusterMgr) DiskSet(c *rpc.Context) {
	args := new(cmapi.DiskSetArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
}

func (mcm *mockClusterMgr) ChunkReport(c *rpc.Context) {
	// do nothing
}

func (mcm *mockClusterMgr) ServiceRegister(c *rpc.Context) {
	// do nothing
}

func (mcm *mockClusterMgr) VolumeGet(c *rpc.Context) {
	args := new(cmapi.GetVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
	vid1, vid2, vid3 := proto.Vid(2001), proto.Vid(2002), proto.Vid(2003)
	vuid1, _ := proto.NewVuid(vid1, 1, 1)
	vuid2, _ := proto.NewVuid(vid2, 1, 1)
	// epoch expired
	vuid3, _ := proto.NewVuid(vid3, 1, 2)

	vid := args.Vid

	if vid == vid1 {
		c.RespondJSON(&cmapi.VolumeInfo{
			VolumeInfoBase: cmapi.VolumeInfoBase{
				Vid: vid1,
			},
			Units: []cmapi.Unit{{Vuid: 0}, {Vuid: vuid1}},
		})
		return
	}

	if vid == vid2 {
		c.RespondJSON(&cmapi.VolumeInfo{
			VolumeInfoBase: cmapi.VolumeInfoBase{
				Vid: vid2,
			},
			Units: []cmapi.Unit{{Vuid: 0}, {Vuid: vuid2}},
		})
		return
	}

	if vid == vid3 {
		c.RespondJSON(&cmapi.VolumeInfo{
			VolumeInfoBase: cmapi.VolumeInfoBase{
				Vid: vid3,
			},
			Units: []cmapi.Unit{{Vuid: 0}, {Vuid: vuid3}},
		})
		return
	}
	c.RespondError(errors.New("not implement"))
}

func (mcm *mockClusterMgr) NodeAdd(c *rpc.Context) {
	args := new(cmapi.BlobNodeInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}

	nodeID := atomic.AddInt32(&mcm.nodeIdx, 1)
	ret := &cmapi.NodeIDAllocRet{
		NodeID: proto.NodeID(nodeID),
	}

	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) NodeDrop(c *rpc.Context) {
	args := new(cmapi.NodeInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
}

func (mcm *mockClusterMgr) NodeInfo(c *rpc.Context) {
	args := new(cmapi.NodeInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(bloberr.ErrIllegalArguments)
		return
	}
	ret := &cmapi.BlobNodeInfo{}
	ret.NodeID = args.NodeID
	ret.DiskType = proto.DiskTypeHDD

	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) ConfigGet(c *rpc.Context) {
	args := new(cmapi.ConfigArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	switch args.Key {
	case proto.ChunkOversoldRatioKey:
		c.RespondJSON("0.5")
	case proto.CodeModeExtendKey:
		c.RespondJSON("[]")
	default:
		c.RespondError(ErrNotSupportKey)
	}
}

func TestService_ConfigReload(t *testing.T) {
	ctr := gomock.NewController(t)
	ds1 := NewMockDiskAPI(ctr)
	ds2 := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks: map[proto.DiskID]core.DiskAPI{101: ds1, 202: ds2},
		Conf:  &Config{DiskConfig: core.RuntimeConfig{DataQos: qos.Config{}}},
	}
	err := qos.FixQosConfigHotReset(&svr.Conf.DiskConfig.DataQos)
	require.NoError(t, err)

	testServer := httptest.NewServer(NewHandler(svr))

	{
		// error
		totalUrl := testServer.URL + "/config/reload?background_mbps=10"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}

	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}

	// for disk 1
	conf1 := qos.Config{
		StatGetter: iom,
		FlowConf: qos.FlowConfig{
			Level: map[string]qos.LevelFlowConfig{
				bnapi.ReadIO.String():       {MBPS: 5},
				bnapi.WriteIO.String():      {MBPS: 4},
				bnapi.DeleteIO.String():     {MBPS: 1},
				bnapi.BackgroundIO.String(): {MBPS: 1},
			},
		},
	}
	q1, err := qos.NewQosMgr(conf1)
	require.NoError(t, err)

	conf2 := qos.Config{
		StatGetter: iom,
		FlowConf: qos.FlowConfig{
			Level: map[string]qos.LevelFlowConfig{
				bnapi.ReadIO.String():       {MBPS: 4},
				bnapi.WriteIO.String():      {MBPS: 4},
				bnapi.DeleteIO.String():     {MBPS: 1},
				bnapi.BackgroundIO.String(): {MBPS: 3},
			},
		},
	}
	q2, err := qos.NewQosMgr(conf2) // for disk 2
	require.NoError(t, err)
	require.Equal(t, int64(1024), q1.GetConfig().FlowConf.DiskBandwidthMB)

	{
		// only set disk bandwidth
		ds1.EXPECT().GetIoQos().Return(q1).Times(1)
		ds2.EXPECT().GetIoQos().Return(q2).Times(1)
		// totalUrl := testServer.URL + "/config/reload?key=background_mbps&value=10"
		totalUrl := testServer.URL + "/config/reload?key=disk.disk_bandwidth_mb&value=128"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		// only set read bandwidth mbps
		ds1.EXPECT().GetIoQos().Return(q1).Times(1)
		ds2.EXPECT().GetIoQos().Return(q2).Times(1)
		// totalUrl := testServer.URL + "/config/reload?key=background_mbps&value=10"
		totalUrl = testServer.URL + "/config/reload?key=level.read.mbps&value=10"
		resp, err = HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}
}

func TestService_RegisterNode(t *testing.T) {
	ctx := context.Background()

	mcm := mockClusterMgr{
		nodeIdx: 0,
	}
	mcmURL := runMockClusterMgr(&mcm)
	cc := &cmapi.Config{}
	cc.Hosts = []string{mcmURL}
	conf := Config{
		Clustermgr: cc,
	}
	svr := &Service{
		ClusterMgrClient: cmapi.New(cc),
		Conf:             &conf,
	}

	// first register
	svr.Conf.DiskType = proto.DiskTypeHDD
	err := svr.registerNode(ctx, svr.Conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(1), svr.Conf.HostInfo.NodeID)

	// duplicate register

	// register node 2
	conf2 := Config{
		Clustermgr: cc,
	}
	svr2 := &Service{
		ClusterMgrClient: cmapi.New(cc),
		Conf:             &conf2,
	}
	svr2.Conf.DiskType = proto.DiskTypeSSD
	err = svr2.registerNode(ctx, svr2.Conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(2), svr2.Conf.HostInfo.NodeID)
	require.NotEqual(t, svr.Conf.NodeID, svr2.Conf.NodeID)

	svr.Conf.DiskType = 0
	err = svr.registerNode(ctx, svr.Conf)
	require.NotNil(t, err)
}

func TestService_OnlyWorker(t *testing.T) {
	mcm := mockClusterMgr{
		reqIdx: _mockDiskIdBase,
		disks:  []mockDiskInfo{},
	}

	mcmURL := runMockClusterMgr(&mcm)

	cc := &cmapi.Config{}
	cc.Hosts = []string{mcmURL}

	conf := Config{
		Clustermgr: cc,
		StartMode:  proto.ServiceNameWorker,
	}

	svr, err := NewService(conf)
	require.NoError(t, err)
	svr.Close()

	// retart
	_, err = NewService(conf)
	require.NoError(t, err)
}

func TestService_OnlyBlobnode(t *testing.T) {
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"OnlyBlobnode")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "disk1")
	path2 := filepath.Join(workDir, "disk2")

	mcm := mockClusterMgr{
		reqIdx: _mockDiskIdBase,
		disks:  []mockDiskInfo{},
	}

	mcmURL := runMockClusterMgr(&mcm)

	cc := &cmapi.Config{}
	cc.Hosts = []string{mcmURL}

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)

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
		DiskConfig:           core.RuntimeConfig{DiskReservedSpaceB: 1, CompactReservedSpaceB: 1},
		Clustermgr:           cc,
		HeartbeatIntervalSec: 600,
		StartMode:            proto.ServiceNameBlobNode,
	}

	svr, err := NewService(conf)
	require.NoError(t, err)
	svr.Close()

	// restart
	_, err = NewService(conf)
	require.NoError(t, err)
}

func TestService_OnlyBlobnode_OpenFailedEIO(t *testing.T) {
	ctx := context.Background()
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"OnlyBlobnode")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "path1")
	path2 := filepath.Join(workDir, "path2")
	path3 := filepath.Join(workDir, "path3")
	path4 := filepath.Join(workDir, "path4")
	for _, path := range []string{workDir, path1, path2, path3, path4} {
		err = os.MkdirAll(path, 0o755)
		require.NoError(t, err)
	}

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
		DiskConfig:           core.RuntimeConfig{DiskReservedSpaceB: 1, CompactReservedSpaceB: 1},
		HeartbeatIntervalSec: 600,
	}

	// open readFormat eio, report broken disk
	diskInfo1 := &cmapi.BlobNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Path:   path1,
			Status: proto.DiskStatusNormal,
		},
		DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{DiskID: proto.DiskID(1)},
	}
	// open readFormat eio, status repaired, skip
	diskInfo2 := &cmapi.BlobNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Path:   path2,
			Status: proto.DiskStatusRepaired,
		},
		DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{DiskID: proto.DiskID(2)},
	}

	A := gomock.Any()
	ctr := gomock.NewController(t)
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().GetConfig(A, A).Return("[]", nil).AnyTimes()
	cmCli.EXPECT().RegisterService(A, A, A, A, A).Return(nil).Times(2)
	cmCli.EXPECT().AddNode(A, A).Return(proto.NodeID(1), nil).Times(2)
	cmCli.EXPECT().ListHostDisk(A, A).Return([]*cmapi.BlobNodeDiskInfo{diskInfo1, diskInfo2}, nil)
	cmCli.EXPECT().SetDisk(A, A, A).Return(nil)

	patches := gomonkey.ApplyFunc(readFormatInfo, func(ctx context.Context, path string) (*core.FormatInfo, error) {
		if path == path1 || path == path2 {
			return nil, syscall.EIO
		}
		return &core.FormatInfo{CheckSum: 1}, nil
	})
	defer patches.Reset()
	patches2 := gomonkey.ApplyFunc(disk.NewDiskStorage, func(ctx context.Context, diskConf core.Config) (*disk.DiskStorageWrapper, error) {
		if diskConf.Path == path3 || diskConf.Path == path4 {
			return nil, syscall.EIO
		}
		disk2 := &disk.DiskStorageWrapper{DiskStorage: &disk.DiskStorage{DiskID: proto.DiskID(2)}}
		return disk2, nil
	})
	defer patches2.Reset()

	configInit(&conf)
	svr := &Service{
		ClusterMgrClient: cmCli,
		Disks:            make(map[proto.DiskID]core.DiskAPI),
		Conf:             &conf,
		closeCh:          make(chan struct{}),
	}
	svr.ctx, svr.cancel = context.WithCancel(ctx)

	err = startBlobnodeService(ctx, svr, conf)
	require.Nil(t, err)
	require.Equal(t, 0, len(svr.Disks))

	conf.Disks = []core.Config{
		{BaseConfig: core.BaseConfig{Path: path3, AutoFormat: true}, MetaConfig: db.MetaConfig{}},
	}
	svr.Conf = &conf

	// newDiskStorage status repaired, skip
	diskInfo3 := &cmapi.BlobNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Path:   path3,
			Status: proto.DiskStatusRepaired,
		},
		DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{DiskID: proto.DiskID(3)},
	}
	cmCli.EXPECT().ListHostDisk(A, A).Return([]*cmapi.BlobNodeDiskInfo{diskInfo3}, nil)
	err = startBlobnodeService(ctx, svr, conf)
	require.NoError(t, err)
	require.Equal(t, 0, len(svr.Disks))
}

func TestService_OnlyBlobnode_OpenDiskNormal(t *testing.T) {
	ctx := context.Background()
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"OnlyBlobnode")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "path1")
	err = os.MkdirAll(path1, 0o755)
	require.NoError(t, err)

	conf := Config{
		Disks: []core.Config{{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{}}},
	}

	// open disk success
	diskInfo1 := &cmapi.BlobNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Path:   path1,
			Status: proto.DiskStatusRepaired,
		},
		DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{DiskID: proto.DiskID(1)},
	}

	A := gomock.Any()
	ctr := gomock.NewController(t)
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().GetConfig(A, A).Return("[]", nil).AnyTimes()
	cmCli.EXPECT().RegisterService(A, A, A, A, A).Return(nil).Times(1)
	cmCli.EXPECT().AddNode(A, A).Return(proto.NodeID(1), nil).Times(1)
	cmCli.EXPECT().ListHostDisk(A, A).Return([]*cmapi.BlobNodeDiskInfo{diskInfo1}, nil)
	cmCli.EXPECT().AllocDiskID(A).Return(proto.DiskID(101), nil)
	cmCli.EXPECT().AddDisk(A, A).Return(nil)

	configInit(&conf)
	svr := &Service{
		ClusterMgrClient: cmCli,
		Disks:            make(map[proto.DiskID]core.DiskAPI),
		Conf:             &conf,
		closeCh:          make(chan struct{}),
	}
	svr.ctx, svr.cancel = context.WithCancel(ctx)

	err = startBlobnodeService(ctx, svr, conf)
	require.NoError(t, err)
	require.Equal(t, 1, len(svr.Disks))
}

func TestService_OnlyBlobnode_Fatal(t *testing.T) {
	ctx := context.Background()
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"OnlyBlobnode")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "path1")
	path2 := filepath.Join(workDir, "path2")
	for _, path := range []string{path1, path2} {
		err = os.MkdirAll(path, 0o755)
		require.NoError(t, err)
	}

	conf := Config{
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{}},
			// {BaseConfig: core.BaseConfig{Path: path2, AutoFormat: true}, MetaConfig: db.MetaConfig{}},
			// {BaseConfig: core.BaseConfig{Path: "wrongPath", AutoFormat: true}, MetaConfig: db.MetaConfig{}},
		},
	}

	// new disk, read meta fake error
	diskInfo1 := &cmapi.BlobNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Path:   path1,
			Status: proto.DiskStatusRepaired,
		},
		DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{DiskID: proto.DiskID(1)},
	}
	// old disk is repairing
	diskInfo2 := &cmapi.BlobNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Path:   path2,
			Status: proto.DiskStatusRepairing,
		},
		DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{DiskID: proto.DiskID(2)},
	}

	A := gomock.Any()
	ctr := gomock.NewController(t)
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().GetConfig(A, A).Return("[]", nil).AnyTimes()
	cmCli.EXPECT().RegisterService(A, A, A, A, A).Return(nil).Times(1)
	cmCli.EXPECT().AddNode(A, A).Return(proto.NodeID(1), nil).Times(1)
	cmCli.EXPECT().ListHostDisk(A, A).Return([]*cmapi.BlobNodeDiskInfo{diskInfo1, diskInfo2}, nil)
	// cmCli.EXPECT().AllocDiskID(A).Return(proto.DiskID(102), nil)

	patches := gomonkey.ApplyFunc(readFormatInfo, func(ctx context.Context, path string) (*core.FormatInfo, error) {
		if path == path1 {
			return nil, errMock
		}
		return &core.FormatInfo{}, nil
	})
	defer patches.Reset()

	mockSpan := opentracing.GlobalTracer().StartSpan("")
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(mockSpan), "Fatalf", func(xx interface{}, format string, v ...interface{}) {
		fmt.Println("startBlobnodeService fatal")
	})
	defer patches2.Reset()

	configInit(&conf)
	svr := &Service{
		ClusterMgrClient: cmCli,
		Disks:            make(map[proto.DiskID]core.DiskAPI),
		Conf:             &conf,
		closeCh:          make(chan struct{}),
	}
	svr.ctx, svr.cancel = context.WithCancel(ctx)

	// require.Panics(t, func() { startBlobnodeService(ctx, svr, conf) })
	err = startBlobnodeService(ctx, svr, conf)
	require.NoError(t, err)
	require.Equal(t, 0, len(svr.Disks))
}

func TestService_OnlyBlobnode_OpenOldDisk(t *testing.T) {
	ctx := context.Background()
	workDir, err := os.MkdirTemp(os.TempDir(), defaultSvrTestDir+"OnlyBlobnode")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "path1")
	err = os.MkdirAll(path1, 0o755)
	require.NoError(t, err)

	conf := Config{
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{}},
		},
	}

	// old disk, repairing, skip
	diskInfo1 := &cmapi.BlobNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Path:   path1,
			Status: proto.DiskStatusRepairing,
		},
		DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{DiskID: proto.DiskID(1)},
	}

	A := gomock.Any()
	ctr := gomock.NewController(t)
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().GetConfig(A, A).Return("[]", nil).AnyTimes()
	cmCli.EXPECT().RegisterService(A, A, A, A, A).Return(nil).Times(1)
	cmCli.EXPECT().AddNode(A, A).Return(proto.NodeID(1), nil).Times(1)
	cmCli.EXPECT().ListHostDisk(A, A).Return([]*cmapi.BlobNodeDiskInfo{diskInfo1}, nil)

	format := &core.FormatInfo{
		FormatInfoProtectedField: core.FormatInfoProtectedField{
			DiskID:  proto.DiskID(1),
			Version: 1,
			Format:  core.FormatMetaTypeV1,
		},
	}
	checkSum, err := format.CalCheckSum()
	require.NoError(t, err)
	format.CheckSum = checkSum
	err = core.SaveDiskFormatInfo(ctx, path1, format)
	require.NoError(t, err)

	configInit(&conf)
	svr := &Service{
		ClusterMgrClient: cmCli,
		Disks:            make(map[proto.DiskID]core.DiskAPI),
		Conf:             &conf,
		closeCh:          make(chan struct{}),
	}
	svr.ctx, svr.cancel = context.WithCancel(ctx)

	err = startBlobnodeService(ctx, svr, conf)
	require.NoError(t, err)
	require.Equal(t, 0, len(svr.Disks))
}

func TestService_DataInspect(t *testing.T) {
	ctr := gomock.NewController(t)
	ds1 := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks: map[proto.DiskID]core.DiskAPI{2: ds1},
	}
	testServer := httptest.NewServer(NewHandler(svr))

	{
		ds1.EXPECT().ID().Return(proto.DiskID(2))
		ds1.EXPECT().DiskInfo().Return(cmapi.BlobNodeDiskInfo{
			DiskInfo: cmapi.DiskInfo{
				ClusterID: 1,
			},
			DiskHeartBeatInfo: cmapi.DiskHeartBeatInfo{
				DiskID: 2,
			},
		})

		// totalUrl := testServer.URL + "/inspect/cleanmetric?clusterid=1&diskid=2&vuid=3&bids=4,5,6&err=xxx"
		totalUrl := testServer.URL + "/inspect/cleanmetric?clusterid=1&diskid=2"
		resp, err := HTTPRequest(http.MethodPost, totalUrl)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}

	{
		// get inspect stats
		getter := mocks.NewMockAccessor(ctr)
		getter.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
		ts, err := taskswitch.NewSwitchMgr(getter).AddSwitch(proto.TaskSwitchDataInspect.String())
		require.NoError(t, err)
		svr.inspectMgr = &DataInspectMgr{
			// progress:   map[proto.DiskID]int{101: 85, 202: 95},
			taskSwitch: ts,
			conf:       DataInspectConf{RateLimit: 4096},
		}
		svr.inspectMgr.progress.Store(proto.DiskID(101), 88)
		svr.inspectMgr.progress.Store(proto.DiskID(202), 99)

		totalUrl := testServer.URL + "/inspect/stat"
		resp, err := HTTPRequest(http.MethodGet, totalUrl)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		var data DataInspectStat
		err = json.Unmarshal(body, &data)
		require.NoError(t, err)
		fmt.Printf("inspect stat: %+v\n", data)
	}
}
