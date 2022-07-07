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
	"fmt"
	"strings"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
)

func newCMClient(secret string, hosts ...string) *clustermgr.Client {
	if len(hosts) == 0 {
		hosts = config.ClusterMgrAddrs()
	}
	if secret == "" {
		secret = config.ClusterMgrSecret()
	}
	enableAuth := false
	if secret != "" {
		enableAuth = true
	}
	return clustermgr.New(&clustermgr.Config{
		LbConfig: rpc.LbConfig{
			Hosts: hosts,
			Config: rpc.Config{
				Tc: rpc.TransportConfig{
					Auth: auth.Config{
						EnableAuth: enableAuth,
						Secret:     secret,
					},
				},
			},
		},
	})
}

func clusterFlags(f *grumble.Flags) {
	f.StringL("host", "", "specific clustermgr host")
	f.StringL("secret", "", "specific clustermgr secret")
}

func specificHosts(f grumble.FlagMap) []string {
	var hosts []string
	for _, host := range strings.Split(f.String("host"), ",") {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if !strings.HasPrefix(host, "http") {
			host = "http://" + host
		}
		hosts = append(hosts, host)
	}
	return hosts
}

// Register register cm
func Register(app *grumble.App) {
	cmCommand := &grumble.Command{
		Name: "cm",
		Help: "cluster manager tools",
	}
	app.AddCommand(cmCommand)

	addCmdConfig(cmCommand)
	addCmdService(cmCommand)
	addCmdWalParse(cmCommand)
	addCmdVolume(cmCommand)
	addCmdListAllDB(cmCommand)
	addCmdDisk(cmCommand)

	cmCommand.AddCommand(&grumble.Command{
		Name: "stat",
		Help: "show stat of clustermgr",
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
		},
		Run: func(c *grumble.Context) error {
			cli := newCMClient(c.Flags.String("secret"), specificHosts(c.Flags)...)
			stat, err := cli.Stat(common.CmdContext())
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})
}
