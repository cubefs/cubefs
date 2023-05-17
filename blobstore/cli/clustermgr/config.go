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
	"net/http"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

func cmdGetConfig(c *grumble.Context) error {
	cli := newCMClient(c.Flags)
	ctx := common.CmdContext()
	verbose := flags.Verbose(c.Flags)
	key := c.Args.String("key")
	value, err := cli.GetConfig(ctx, key)
	if err != nil {
		return err
	}
	showConfig(key, value, verbose)
	return nil
}

func showConfig(key, value string, verbose bool) {
	if verbose && key == "code_mode" {
		policies := make([]codemode.Policy, 0)
		if common.Unmarshal([]byte(value), &policies) == nil {
			value = "\n" + common.Readable(policies)
		}
	}
	fmt.Println("\t", key, ":", value)
}

func addCmdConfig(cmd *grumble.Command) {
	configCommand := &grumble.Command{
		Name:     "config",
		Help:     "config tools",
		LongHelp: "config tools for clustermgr",
	}
	cmd.AddCommand(configCommand)

	configCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "show config of [key]",
		Run:  cmdGetConfig,
		Args: func(a *grumble.Args) {
			a.String("key", "config key", grumble.Default(""))
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})
	configCommand.AddCommand(&grumble.Command{
		Name: "set",
		Help: "set config of key",
		Args: func(a *grumble.Args) {
			a.String("key", "config key")
			a.String("value", "config value")
		},
		Flags: clusterFlags,
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			value := c.Args.String("value")

			cli := newCMClient(c.Flags)
			ctx := common.CmdContext()
			oldV, err := cli.GetConfig(ctx, key)
			if err != nil {
				if rpc.DetectStatusCode(err) != http.StatusNotFound ||
					!common.Confirm("to set new key: "+common.Loaded.Sprint(key)+" ?") {
					return err
				}
			}
			showConfig(key, oldV, true)

			if common.Confirm(fmt.Sprintf(
				"to set key: `%s` from `%s` --> `%s` ?", common.Loaded.Sprint(key),
				common.Danger.Sprint(oldV), common.Normal.Sprint(value))) {
				return cli.SetConfig(ctx, &clustermgr.ConfigSetArgs{
					Key:   key,
					Value: value,
				})
			}
			return nil
		},
	})
	configCommand.AddCommand(&grumble.Command{
		Name: "del",
		Help: "del config of key",
		Args: func(a *grumble.Args) {
			a.String("key", "config key")
		},
		Flags: clusterFlags,
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")

			cli := newCMClient(c.Flags)
			ctx := common.CmdContext()
			oldV, err := cli.GetConfig(ctx, key)
			if err != nil {
				return err
			}
			showConfig(key, oldV, true)

			if common.Confirm("to del key: " + common.Loaded.Sprint(key) + " ?") {
				return cli.DeleteConfig(ctx, key)
			}
			return nil
		},
	})
	addCmdConfigBackground(configCommand)
}
