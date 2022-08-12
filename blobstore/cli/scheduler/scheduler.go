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
	"fmt"
	"strconv"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

const (
	_clusterID = "cluster_id"
	_taskType  = "task_type"
	_taskID    = "task_id"
)

var defaultClusterMgrAddrs = []string{"http://127.0.0.1:9998"}

func newClusterMgrTaskClient(clusterID int) client.ClusterMgrTaskAPI {
	enableAuth := false
	secret := config.ClusterMgrSecret()
	if secret != "" {
		enableAuth = true
	}
	var addrs []string
	addrs = defaultClusterMgrAddrs
	hosts, ok := config.ClusterMgrClusters()[strconv.Itoa(clusterID)]
	if !ok {
		addrs = hosts
	}
	return client.NewClusterMgrClient(&clustermgr.Config{
		LbConfig: rpc.LbConfig{
			Hosts: addrs,
			Config: rpc.Config{
				Tc: rpc.TransportConfig{Auth: auth.Config{EnableAuth: enableAuth, Secret: secret}},
			},
		},
	})
}

func newClusterMgrClient(clusterID string) *clustermgr.Client {
	enableAuth := false
	secret := config.ClusterMgrSecret()
	if secret != "" {
		enableAuth = true
	}
	var addrs []string
	addrs = defaultClusterMgrAddrs
	hosts, ok := config.ClusterMgrClusters()[clusterID]
	if !ok {
		addrs = hosts
	}
	return clustermgr.New(&clustermgr.Config{
		LbConfig: rpc.LbConfig{
			Hosts: addrs,
			Config: rpc.Config{
				Tc: rpc.TransportConfig{Auth: auth.Config{EnableAuth: enableAuth, Secret: secret}},
			},
		},
	})
}

// Register register scheduler
func Register(app *grumble.App) {
	schedulerCommand := &grumble.Command{
		Name: "scheduler",
		Help: "scheduler tools",
	}
	app.AddCommand(schedulerCommand)
	schedulerCommand.AddCommand(&grumble.Command{
		Name: "stat",
		Help: "show leader stat of scheduler",
		Run:  leaderStat,
		Args: func(a *grumble.Args) {
			a.Int(_clusterID, _clusterID, grumble.Default(int(1)))
		},
	})

	addCmdMigrateTask(schedulerCommand)
	addCmdVolumeInspectCheckpointTask(schedulerCommand)
	addCmdKafkaConsumer(schedulerCommand)
}

func leaderStat(c *grumble.Context) error {
	clusterID := c.Args.Int(_clusterID)
	clusterMgrCli := newClusterMgrClient(strconv.Itoa(clusterID))
	cli := scheduler.New(&scheduler.Config{}, clusterMgrCli, proto.ClusterID(clusterID))
	stat, err := cli.LeaderStats(common.CmdContext())
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(stat))
	return nil
}
