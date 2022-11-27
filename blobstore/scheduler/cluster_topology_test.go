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

package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

var (
	topoDisk1 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		DiskID:       1,
		FreeChunkCnt: 10,
		MaxChunkCnt:  700,
	}
	topoDisk2 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.2:8000",
		DiskID:       2,
		FreeChunkCnt: 100,
		MaxChunkCnt:  700,
	}
	topoDisk3 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z1",
		Rack:         "rack1",
		Host:         "127.0.0.3:8000",
		DiskID:       3,
		FreeChunkCnt: 20,
		MaxChunkCnt:  700,
	}
	topoDisk4 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z1",
		Rack:         "rack2",
		Host:         "127.0.0.4:8000",
		DiskID:       4,
		FreeChunkCnt: 5,
		MaxChunkCnt:  700,
	}
	topoDisk5 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z2",
		Rack:         "rack2",
		Host:         "127.0.0.4:8000",
		DiskID:       5,
		FreeChunkCnt: 200,
		MaxChunkCnt:  700,
	}
	topoDisk6 = &client.DiskInfoSimple{
		ClusterID:    123,
		Idc:          "z2",
		Rack:         "rack2",
		Host:         "127.0.0.4:8000",
		DiskID:       5,
		FreeChunkCnt: 200,
		MaxChunkCnt:  700,
	}

	topoDisks = []*client.DiskInfoSimple{topoDisk1, topoDisk2, topoDisk3, topoDisk4, topoDisk5, topoDisk6}
)

func TestNewClusterTopologyMgr(t *testing.T) {
	clusterTopMgr := &ClusterTopologyMgr{
		taskStatsMgr: base.NewClusterTopologyStatisticsMgr(1, []float64{}),
	}
	clusterTopMgr.buildClusterTopo(topoDisks, 1)
	require.Equal(t, 3, len(clusterTopMgr.GetIDCs()))
	disks := clusterTopMgr.GetIDCDisks("z0")
	require.Equal(t, 2, len(disks))
	disks = clusterTopMgr.GetIDCDisks("z1")
	require.Equal(t, 2, len(disks))
	disks = clusterTopMgr.GetIDCDisks("z2")
	require.Equal(t, 1, len(disks))
	disks = clusterTopMgr.GetIDCDisks("z3")
	require.True(t, disks == nil)

	ctr := gomock.NewController(t)
	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().ListClusterDisks(any).AnyTimes().Return(nil, errMock)
	conf := &clusterTopoConf{
		ClusterID:      1,
		UpdateInterval: 1 * time.Microsecond,
	}
	mgr := NewClusterTopologyMgr(clusterMgrCli, conf)
	defer mgr.Close()

	// wait topology update
	time.Sleep(2 * time.Microsecond)
}
