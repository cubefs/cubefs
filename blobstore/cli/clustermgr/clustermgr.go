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
	"strings"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
)

func newCMClient(f grumble.FlagMap) *clustermgr.Client {
	clusterID := f.String("cluster_id")
	if clusterID == "" {
		clusterID = fmt.Sprintf("%d", config.DefaultClusterID())
	}
	var hosts []string
	if str := strings.TrimSpace(f.String("hosts")); str != "" {
		hosts = strings.Split(str, " ")
	}
	return config.NewCluster(clusterID, hosts, f.String("secret"))
}

func clusterFlags(f *grumble.Flags) {
	f.StringL("cluster_id", "", "specific clustermgr cluster id")
	f.StringL("secret", "", "specific clustermgr secret")
	f.StringL("hosts", "", "specific clustermgr hosts")
}

// Register register cm
func Register(app *grumble.App) {
	cmCommand := &grumble.Command{
		Name: "cm",
		Help: "cluster manager tools",
	}
	app.AddCommand(cmCommand)

	addCmdConfig(cmCommand)
	addCmdBackground(cmCommand)
	addCmdService(cmCommand)
	addCmdWalParse(cmCommand)
	addCmdVolume(cmCommand)
	addCmdListAllDB(cmCommand)
	addCmdDisk(cmCommand)
	addCmdKV(cmCommand)
	addCmdManage(cmCommand)
	addCmdSnapshot(cmCommand)
	addCmdUpdateRaftDB(cmCommand)
	addCmdCatalog(cmCommand)

	cmCommand.AddCommand(&grumble.Command{
		Name:  "stat",
		Help:  "show stat of clustermgr",
		Flags: clusterFlags,
		Run: func(c *grumble.Context) error {
			cli := newCMClient(c.Flags)
			stat, err := cli.Stat(common.CmdContext())
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})
}
