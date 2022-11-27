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
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

const (
	_clusterID = "cluster_id"
	_taskType  = "task_type"
	_taskID    = "task_id"
)

var defaultClusterMgrAddrs = []string{"http://127.0.0.1:9998"}

func newClusterMgrClient(clusterID proto.ClusterID) *clustermgr.Client {
	return clustermgr.New(cmConfig(clusterID))
}

func newClusterMgrTaskClient(clusterID proto.ClusterID) client.ClusterMgrTaskAPI {
	return client.NewClusterMgrClient(cmConfig(clusterID))
}

func cmConfig(clusterID proto.ClusterID) *clustermgr.Config {
	addrs := defaultClusterMgrAddrs[:]
	if hosts, ok := config.ClusterMgrClusters()[clusterID.ToString()]; ok {
		addrs = hosts
	}
	secret := config.ClusterMgrSecret()
	cfg := &clustermgr.Config{}
	cfg.LbConfig.Hosts = addrs
	cfg.LbConfig.Config.Tc.Auth = auth.Config{EnableAuth: secret != "", Secret: secret}
	return cfg
}

func clusterFlags(f *grumble.Flags) {
	f.Int("c", _clusterID, -1, "specific clustermgr cluster id")
}

func getClusterID(f grumble.FlagMap) proto.ClusterID {
	clusterID := f.Int(_clusterID)
	if clusterID < 0 {
		return proto.ClusterID(config.DefaultClusterID())
	}
	return proto.ClusterID(clusterID)
}

// Register register scheduler
func Register(app *grumble.App) {
	schedulerCommand := &grumble.Command{
		Name: "scheduler",
		Help: "scheduler tools",
	}
	app.AddCommand(schedulerCommand)
	schedulerCommand.AddCommand(&grumble.Command{
		Name:  "stat",
		Help:  "show leader stat of scheduler",
		Run:   leaderStat,
		Flags: clusterFlags,
	})

	addCmdMigrateTask(schedulerCommand)
	addCmdVolumeInspectCheckpointTask(schedulerCommand)
	addCmdKafkaConsumer(schedulerCommand)
}

func leaderStat(c *grumble.Context) error {
	clusterID := getClusterID(c.Flags)
	clusterMgrCli := newClusterMgrClient(clusterID)
	cli := scheduler.New(&scheduler.Config{}, clusterMgrCli, clusterID)
	stat, err := cli.LeaderStats(common.CmdContext())
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(stat))
	return nil
}
