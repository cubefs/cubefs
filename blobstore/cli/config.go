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

package cli

import (
	"fmt"
	"sort"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/cli/config"
)

func cmdConfigGet(c *grumble.Context) error {
	fmt.Println("configs:")

	all := config.All()
	keys := c.Args.StringList("keys")
	if len(keys) == 0 {
		keys = make([]string, 0, len(all))
		for key := range all {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		fmt.Printf("\t%s --> %s\n", color.RedString("%-30s", key), color.GreenString("%+v", all[key]))
	}
	return nil
}

func registerConfig(app *grumble.App) {
	configCommand := &grumble.Command{
		Name: "config",
		Help: "manager memory cache of config",
	}
	app.AddCommand(configCommand)

	configCommand.AddCommand(&grumble.Command{
		Name: "type",
		Help: "print type in cache",
		Run: func(c *grumble.Context) error {
			config.PrintType()
			return nil
		},
	})
	configCommand.AddCommand(&grumble.Command{
		Name:     "get",
		Help:     "get config in cache",
		LongHelp: "show all caches if no args",
		Args: func(a *grumble.Args) {
			a.StringList("keys", "config keys", grumble.Default([]string{}), grumble.Max(100))
		},
		Run: cmdConfigGet,
	})
	configCommand.AddCommand(&grumble.Command{
		Name:     "set",
		Help:     "set config to cache",
		LongHelp: "set config to cache, value concat with ',' if []string",
		Args: func(a *grumble.Args) {
			a.String("key", "config key")
			a.String("value", "config value, concat with ',' if []string")
		},
		Run: func(c *grumble.Context) error {
			key, val := c.Args.String("key"), c.Args.String("value")
			fmt.Println("To set Key:", color.RedString("%s", key), "Value:", color.GreenString("%s", val))
			config.SetFrom(key, val)
			return nil
		},
	})
	configCommand.AddCommand(&grumble.Command{
		Name: "del",
		Help: "del config of keys",
		Args: func(a *grumble.Args) {
			a.StringList("keys", "config keys", grumble.Default([]string{}), grumble.Max(100))
		},
		Run: func(c *grumble.Context) error {
			for _, key := range c.Args.StringList("keys") {
				fmt.Println("To del Key:", color.RedString("%s", key))
				config.Del(key)
			}
			return nil
		},
	})
}
