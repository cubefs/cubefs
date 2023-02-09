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
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

const _host = "host"

var proxyCli = proxy.New(&proxy.Config{})

func proxyFlags(f *grumble.Flags) {
	f.StringL(_host, "http://127.0.0.1:9600", "request on proxy host")
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

	addCmdAllocator(proxyCommand)
	addCmdCacher(proxyCommand)
}
