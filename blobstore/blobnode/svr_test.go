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
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	defaultSvrTestDir = "BlobNodeSvrTestDir_"
)

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

func TestService2(t *testing.T) {
	workDir, err := ioutil.TempDir(os.TempDir(), defaultSvrTestDir+"Service2")
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

	fmt.Println("work dir: ", workDir)

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)
	conf := Config{
		HostInfo: core.HostInfo{
			IDC:  "testIdc",
			Rack: "testRack",
			Host: "127.0.0.1",
		},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1, AutoFormat: true, MaxChunks: 700}, MetaConfig: db.MetaConfig{RocksdbOption: kvstore.RocksDBOption{WriteBufferSize: 1024}}},
		},
		Clustermgr: cc,
	}
	_, err = NewService(conf)
	require.Error(t, err)
}

func newTestBlobNodeService(t *testing.T, path string) (*Service, *mockClusterMgr) {
	workDir, err := ioutil.TempDir(os.TempDir(), defaultSvrTestDir+path)
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

	fmt.Println("work dir: ", workDir)

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)

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
		DiskConfig:           core.RuntimeConfig{DiskReservedSpaceB: 1},
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
	}
	mcm.disks = diskInfos

	return service, &mcm
}

func TestService_CmdpChunk(t *testing.T) {
	workDir, err := ioutil.TempDir(os.TempDir(), defaultSvrTestDir+"TestService_CmdpChunkCompact")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "disk1")
	path2 := filepath.Join(workDir, "disk2")

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

	fmt.Println("work dir: ", workDir)

	err = os.MkdirAll(workDir, 0o755)
	require.NoError(t, err)

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
		DiskConfig:           core.RuntimeConfig{DiskReservedSpaceB: 1},
		Clustermgr:           cc,
		HeartbeatIntervalSec: 600,
	}
	service, err := NewService(conf)
	require.NoError(t, err)

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

	err = cs.SetStatus(bnapi.ChunkStatusRelease)
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
	reqIdx int64
	disks  []mockDiskInfo
}

func mockClusterMgrRouter(service *mockClusterMgr) *rpc.Router {
	r := rpc.New()
	rpc.RegisterArgsParser(&cmapi.ListOptionArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.ListVolumeUnitArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.DiskInfoArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.DisksHeartbeatArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.DiskSetArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.ReportChunkArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.GetVolumeArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.ShardRepairArgs{}, "json")

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
		info := &bnapi.DiskInfo{
			Path:   d.path,
			Status: d.status,
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
	ret := &bnapi.DiskInfo{}
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
	args := new(bnapi.DiskInfo)
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
}

func (mcm *mockClusterMgr) ServiceRegister(c *rpc.Context) {
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
