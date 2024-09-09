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

package cli

import (
	"os"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

type (
	sCodec = rpc2.AnyCodec[struct{ val string }]
	bCodec = rpc2.AnyCodec[struct{ val []byte }]
)

func cmdRpc2Request(c *grumble.Context) error {
	cli := config.Rpc2Client
	addr, path := c.Args.String("addr"), c.Args.String("path")
	paraVal, rstVal := c.Flags.String("parameter"), c.Flags.String("result")

	if c.Flags.Bool("readable") {
		var para, rst sCodec
		para.Value.val = paraVal
		if err := cli.Request(common.CmdContext(), addr, path, &para, &rst); err != nil {
			return err
		}
		fmt.Println("result:", rst.Value.val)
		return nil
	}

	val, err := os.ReadFile(paraVal)
	if err != nil {
		return err
	}
	var para, rst bCodec
	para.Value.val = val
	if err := cli.Request(common.CmdContext(), addr, path, &para, &rst); err != nil {
		return err
	}
	return os.WriteFile(rstVal, rst.Value.val, 0o644)
}

func registerRpc2(app *grumble.App) {
	rpc2Command := &grumble.Command{
		Name:     "rpc2",
		Help:     "simple client of rpc2",
		LongHelp: "rpc2 simple client struct request and response",
		Args: func(a *grumble.Args) {
			a.String("addr", "request address")
			a.String("path", "request path")
		},
		Flags: func(f *grumble.Flags) {
			f.BoolL("readable", false, "para and args is readable")
			f.StringL("parameter", "", "request parameter")
			f.StringL("result", "", "response result")
		},
		Run: cmdRpc2Request,
	}
	app.AddCommand(rpc2Command)
}
