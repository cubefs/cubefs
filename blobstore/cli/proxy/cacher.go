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

package proxy

import (
	"github.com/desertbit/grumble"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdCacher(cmd *grumble.Command) {
	cacherCommand := &grumble.Command{
		Name: "cacher",
		Help: "proxy cacher tools",
	}
	cmd.AddCommand(cacherCommand)

	cacherCommand.AddCommand(&grumble.Command{
		Name: "volume",
		Help: "get volume from proxy",
		Flags: func(f *grumble.Flags) {
			proxyFlags(f)
			f.Uint64L("vid", 0, "volume id")
			f.Uint64L("version", 0, "version")
			f.BoolL("flush", false, "flush")
		},
		Run: func(c *grumble.Context) error {
			volume, err := proxyCli.GetCacheVolume(common.CmdContext(), c.Flags.String(_host),
				&proxy.CacheVolumeArgs{
					Vid:     proto.Vid(c.Flags.Uint64("vid")),
					Version: uint32(c.Flags.Uint64("version")),
					Flush:   c.Flags.Bool("flush"),
				})
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(volume))
			return nil
		},
	})
	cacherCommand.AddCommand(&grumble.Command{
		Name: "disk",
		Help: "get disk from proxy",
		Flags: func(f *grumble.Flags) {
			proxyFlags(f)
			f.Uint64L("disk_id", 0, "disk id")
			f.BoolL("flush", false, "flush")
		},
		Run: func(c *grumble.Context) error {
			disk, err := proxyCli.GetCacheDisk(common.CmdContext(), c.Flags.String(_host),
				&proxy.CacheDiskArgs{
					DiskID: proto.DiskID(c.Flags.Uint64("disk_id")),
					Flush:  c.Flags.Bool("flush"),
				})
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(disk))
			return nil
		},
	})
	cacherCommand.AddCommand(&grumble.Command{
		Name: "erase",
		Help: "erase cache with key or all",
		Flags: func(f *grumble.Flags) {
			proxyFlags(f)
			f.Int64L("clusterid", 0, "cluster id")
			f.StringL("idc", "", "idc for proxy service, [ALL or xxx]")
		},
		Args: func(a *grumble.Args) {
			a.String("key", "key of diskv [volume-{vid} or disk-{disk_id} or ALL]")
		},
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			if !common.Confirm("to erase key: " + color.RedString("%s", key)) {
				return nil
			}
			if host := c.Flags.String(_host); host != "" {
				return proxyCli.Erase(common.CmdContext(), host, key)
			}

			clusterID := proto.ClusterID(c.Flags.Int64("clusterid"))
			if clusterID <= 0 {
				return fmt.Errorf("setting --clusterid please")
			}
			cmcli := config.NewCluster(clusterID.ToString(), nil, "")
			info, err := cmcli.GetService(common.CmdContext(),
				clustermgr.GetServiceArgs{Name: proto.ServiceNameProxy})
			if err != nil {
				return err
			}
			idc := c.Flags.String("idc")
			hosts := make([]string, 0, len(info.Nodes))
			for _, ii := range info.Nodes {
				if idc == "ALL" || ii.Idc == idc {
					hosts = append(hosts, ii.Host)
				}
			}

			for _, host := range hosts {
				fmt.Printf("to erase host:%s key:%s\n", host, key)
				if err = proxyCli.Erase(common.CmdContext(), host, key); err != nil {
					return err
				}
			}
			return nil
		},
	})
}
