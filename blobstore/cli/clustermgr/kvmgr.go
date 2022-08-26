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
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdKV(cmd *grumble.Command) {
	kvCommand := &grumble.Command{
		Name: "kv",
		Help: "kv tools",
	}
	cmd.AddCommand(kvCommand)

	kvCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get by key",
		Args: func(a *grumble.Args) {
			a.String("key", "key name")
		},
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
			f.BoolL("migrate_task", false, "show value with migrate task")
		},
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			val, err := newCMClient(c.Flags).GetKV(common.CmdContext(), key)
			if err != nil {
				return err
			}
			fmt.Printf("found key %s : %s\n",
				common.Normal.Sprint(key), decodeValue(val.Value, c.Flags))
			return nil
		},
	})
	kvCommand.AddCommand(&grumble.Command{
		Name: "set",
		Help: "set value to key",
		Args: func(a *grumble.Args) {
			a.String("key", "key name")
			a.String("value", "value of key, string")
		},
		Flags: clusterFlags,
		Run: func(c *grumble.Context) error {
			key, val := c.Args.String("key"), c.Args.String("value")
			if !common.Confirm(fmt.Sprintf("to Set key %s ?", common.Danger.Sprint(key))) {
				return nil
			}
			return newCMClient(c.Flags).SetKV(common.CmdContext(), key, []byte(val))
		},
	})
	kvCommand.AddCommand(&grumble.Command{
		Name: "del",
		Help: "del the key",
		Args: func(a *grumble.Args) {
			a.String("key", "key name")
		},
		Flags: clusterFlags,
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			if !common.Confirm(fmt.Sprintf("to Del key %s ?", common.Danger.Sprint(key))) {
				return nil
			}
			return newCMClient(c.Flags).DeleteKV(common.CmdContext(), key)
		},
	})
	kvCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list keys",
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
			f.StringL("prefix", "", "list option prefix with")
			f.StringL("marker", "", "list option marker with")
			f.IntL("count", 10, "list option page count")
			f.BoolL("migrate_task", false, "show value with migrate task")
		},
		Run: func(c *grumble.Context) error {
			lists, err := newCMClient(c.Flags).ListKV(common.CmdContext(), &clustermgr.ListKvOpts{
				Prefix: c.Flags.String("prefix"),
				Marker: c.Flags.String("marker"),
				Count:  c.Flags.Int("count"),
			})
			if err != nil {
				return err
			}
			for _, val := range lists.Kvs {
				fmt.Println("Key   :", val.Key)
				fmt.Println("Value :", decodeValue(val.Value, c.Flags))
			}
			fmt.Println("next marker:", common.Loaded.Sprint(lists.Marker))
			return nil
		},
	})
}

func decodeValue(val []byte, f grumble.FlagMap) string {
	if f.Bool("migrate_task") {
		var task proto.MigrateTask
		if err := common.Unmarshal(val, &task); err == nil {
			return common.Readable(task)
		}
	}
	return string(val)
}
