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
	"path/filepath"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
)

const _host = "host"

var proxyCli = proxy.New(&proxy.Config{})

func proxyFlags(f *grumble.Flags) {
	f.StringL(_host, "", "request on proxy host")
}

// Register proxy
func Register(app *grumble.App) {
	proxyCommand := &grumble.Command{
		Name: "proxy",
		Help: "proxy tools",
	}
	app.AddCommand(proxyCommand)

	proxyCommand.AddCommand(&grumble.Command{
		Name: "diskv_path",
		Help: "show relative diskv path of key",
		Args: func(a *grumble.Args) {
			a.String("key", "key of diskv [volume-{vid} or disk-{disk_id}]")
		},
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			prefix := proxy.DiskvPathTransform(key)
			path := filepath.Join(filepath.Join(prefix...), key)
			fmt.Println("relative path:", color.GreenString("%s", path))
			return nil
		},
	})
	proxyCommand.AddCommand(&grumble.Command{
		Name: "alloc",
		Help: "alloc volume",
		Flags: func(f *grumble.Flags) {
			proxyFlags(f)
			f.UintL("code_mode", 0, "codemode uint")
			f.Uint64L("fsize", 0, "file size")
			f.Uint64L("bid_count", 0, "bid count")
		},
		Run: func(c *grumble.Context) error {
			volumes, err := proxyCli.VolumeAlloc(common.CmdContext(), c.Flags.String(_host),
				&proxy.AllocVolsArgs{
					Fsize:    c.Flags.Uint64("fsize"),
					CodeMode: codemode.CodeMode(c.Flags.Uint("code_mode")),
					BidCount: c.Flags.Uint64("bid_count"),
				})
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(volumes))
			return nil
		},
	})

	addCmdCacher(proxyCommand)
}
