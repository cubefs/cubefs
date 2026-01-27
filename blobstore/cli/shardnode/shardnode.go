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
	"strings"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
)

func Register(app *grumble.App) {
	snCommand := &grumble.Command{
		Name: "shardnode",
		Help: "shardnode manager tools",
	}
	app.AddCommand(snCommand)
	addCmdShard(snCommand)
	addCmdRecover(snCommand)
	addCmdTCMalloc(snCommand)
}

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
