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

package shardnode

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util"
)

var (
	nodeID         = proto.NodeID(1)
	atomDiskID     uint32
	atomRaftPort   = uint32(6699)
	repairDiskID   = proto.DiskID(1000)
	repairDiskPath = "/tmp/repair_disk"
)

type (
	mockClusterMgr struct {
		once  sync.Once
		disks []mockDisk
	}
	mockDisk struct {
		path   string
		nodeID proto.NodeID
		diskID proto.DiskID
	}
)

func TestSvr_Loop(t *testing.T) {
	cfg := genTestServiceCfg()

	path, err := util.GenTmpPath()
	require.Nil(t, err)
	disks := []string{path}
	cfg.DisksConfig.Disks = disks
	cfg.AllocVolConfig.BidAllocNums = 1000
	defer func() {
		os.RemoveAll(path)
	}()
	cfg.HeartBeatIntervalS = 1
	cfg.ReportIntervalS = 1
	cfg.RouteUpdateIntervalS = 1

	s := newService(cfg)
	time.Sleep(3 * time.Second)
	s.closer.Close()
}

func TestSvr_HandleEIO(t *testing.T) {
	cfg := genTestServiceCfg()
	cfg.WaitReOpenDiskIntervalS = 1
	cfg.WaitRepairCloseDiskIntervalS = 1

	s := newService(cfg)
	disk := &storage.Disk{}
	disk.SetDiskInfo(cmapi.ShardNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Status: proto.DiskStatusNormal,
			Path:   repairDiskPath,
		},
		ShardNodeDiskHeartbeatInfo: cmapi.ShardNodeDiskHeartbeatInfo{
			DiskID: repairDiskID,
		},
	})
	s.disks[repairDiskID] = disk

	os.Mkdir(repairDiskPath, 0o755)
	s.handleEIO(ctx, repairDiskID, syscall.EIO)
	time.Sleep(3 * time.Second)
	os.RemoveAll(repairDiskPath)
}

func init() {
	rpc.RegisterArgsParser(&cmapi.ShardNodeInfo{}, "json")
	rpc.RegisterArgsParser(&cmapi.ListOptionArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.ShardNodeDiskInfo{}, "json")
	rpc.RegisterArgsParser(&cmapi.ListShardArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.ConfigArgs{}, "json")
	rpc.RegisterArgsParser(&cmapi.AllocVolumeArgs{}, "json")
}

func mockClusterMgrRouter(service *mockClusterMgr) *rpc.Router {
	r := rpc.New()

	r.Handle(http.MethodPost, "/shardnode/add", service.AddShardNode, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/shardnode/disk/list", service.ListDisks, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/shardnode/diskid/alloc", service.AllocDiskID)
	r.Handle(http.MethodPost, "/shardnode/disk/add", service.AddShardNodeDisk)
	r.Handle(http.MethodGet, "/space/list", service.ListSpace, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/config/get", service.GetConfig, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/bid/alloc", service.AllocBid)
	r.Handle(http.MethodGet, "/volume/alloc", service.AllocVolume)
	r.Handle(http.MethodPost, "/shardnode/disk/heartbeat", service.HeartBeat)
	r.Handle(http.MethodPost, "/shard/report", service.ShardReport)
	r.Handle(http.MethodPost, "/shardnode/disk/set", service.SetDisk)
	r.Handle(http.MethodGet, "/shardnode/disk/info", service.GetDiskInfo)

	return r
}

func (mcm *mockClusterMgr) AddShardNode(c *rpc.Context) {
	ret := &cmapi.NodeIDAllocRet{
		NodeID: nodeID,
	}
	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) ListDisks(c *rpc.Context) {
	args := new(cmapi.ListOptionArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(errors.ErrIllegalArguments)
		return
	}
	disks := make([]*cmapi.ShardNodeDiskInfo, len(mcm.disks))
	for i, d := range mcm.disks {
		disks[i] = &cmapi.ShardNodeDiskInfo{
			DiskInfo: cmapi.DiskInfo{
				Path:   d.path,
				NodeID: d.nodeID,
			},
			ShardNodeDiskHeartbeatInfo: cmapi.ShardNodeDiskHeartbeatInfo{
				DiskID: d.diskID,
			},
		}
	}
	ret := &cmapi.ListShardNodeDiskRet{
		Disks:  disks,
		Marker: 0,
	}
	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) AllocDiskID(c *rpc.Context) {
	id := atomic.AddUint32(&atomDiskID, 1)
	ret := cmapi.DiskIDAllocRet{DiskID: proto.DiskID(id)}
	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) AddShardNodeDisk(c *rpc.Context) {
	c.Respond()
}

func (mcm *mockClusterMgr) ListSpace(c *rpc.Context) {
	mcm.once.Do(func() {
		args := new(cmapi.ListShardArgs)
		if err := c.ParseArgs(args); err != nil {
			c.RespondError(errors.ErrIllegalArguments)
			return
		}
		ret := cmapi.ListSpaceRet{
			Spaces: []*cmapi.Space{&testSpace},
			Marker: sid,
		}
		raw, _ := ret.Marshal()
		c.RespondWith(200, "", raw)
	})
}

func (mcm *mockClusterMgr) GetConfig(c *rpc.Context) {
	args := new(cmapi.ConfigArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(errors.ErrIllegalArguments)
		return
	}
	key := args.Key
	var value string
	switch key {
	case proto.CodeModeConfigKey:
		policy := []codemode.Policy{
			{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 0, SizeRatio: 0.3, Enable: true},
			{ModeName: codemode.EC15P12.Name(), MinSize: 0, MaxSize: 0, SizeRatio: 0.7, Enable: true},
		}
		data, _ := json.Marshal(policy)
		value = string(data)
	case proto.VolumeReserveSizeKey:
		value = "1024"
	case proto.VolumeChunkSizeKey:
		value = "17179869184"
	default:
	}
	c.RespondJSON(value)
}

func (mcm *mockClusterMgr) AllocBid(c *rpc.Context) {
	ret := &cmapi.BidScopeRet{
		StartBid: proto.BlobID(1),
		EndBid:   proto.BlobID(5001),
	}
	c.RespondJSON(ret)
}

func (mcm *mockClusterMgr) AllocVolume(c *rpc.Context) {
	args := new(cmapi.AllocVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(errors.ErrIllegalArguments)
		return
	}
	mode, count := args.CodeMode, args.Count

	now := time.Now().UnixNano()
	rets := cmapi.AllocatedVolumeInfos{}

	volInfos := make([]cmapi.AllocVolumeInfo, 0)
	for i := 50; i < 50+count; i++ {
		volInfo := cmapi.AllocVolumeInfo{
			VolumeInfo: cmapi.VolumeInfo{
				VolumeInfoBase: cmapi.VolumeInfoBase{
					CodeMode: mode,
					Vid:      proto.Vid(i),
					Free:     16 * 1024 * 1024 * 1024,
				},
			},
			ExpireTime: 800*int64(math.Pow(10, 9)) + now,
		}
		volInfos = append(volInfos, volInfo)
	}
	rets.AllocVolumeInfos = volInfos
	c.RespondJSON(rets)
}

func (mcm *mockClusterMgr) HeartBeat(c *rpc.Context) {
	c.Respond()
}

func (mcm *mockClusterMgr) ShardReport(c *rpc.Context) {
	c.RespondJSON(nil)
}

func (mcm *mockClusterMgr) SetDisk(c *rpc.Context) {
	c.Respond()
}

func (mcm *mockClusterMgr) GetDiskInfo(c *rpc.Context) {
	c.RespondJSON(&cmapi.ShardNodeDiskInfo{
		DiskInfo: cmapi.DiskInfo{
			Status: proto.DiskStatusRepaired,
		},
	})
}

func runMockClusterMgr(mcm *mockClusterMgr) string {
	r := mockClusterMgrRouter(mcm)
	testServer := httptest.NewServer(r)
	return testServer.URL
}

func genTestServiceCfg() *Config {
	mcm := mockClusterMgr{}
	mcmURL := runMockClusterMgr(&mcm)

	cc := cmapi.Config{}
	cc.Hosts = []string{mcmURL}

	raftHost := fmt.Sprintf("127.0.0.1:%d", atomic.AddUint32(&atomRaftPort, 1))
	cfg := &Config{
		NodeConfig: cmapi.ShardNodeInfo{
			NodeInfo: cmapi.NodeInfo{
				Host: "127.0.0.1:9911",
			},
			ShardNodeExtraInfo: cmapi.ShardNodeExtraInfo{
				RaftHost: raftHost,
			},
		},
		CmConfig: cc,
	}
	return cfg
}
