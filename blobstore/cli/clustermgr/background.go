// Copyright 2023 The CubeFS Authors.
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
	"strings"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

const notSet = "<not set>"

var (
	BackgroundTaskTypes = []string{
		string(proto.TaskTypeDiskRepair),
		string(proto.TaskTypeBalance),
		string(proto.TaskTypeDiskDrop),
		string(proto.TaskTypeManualMigrate),
		string(proto.TaskTypeVolumeInspect),
		string(proto.TaskTypeShardRepair),
		string(proto.TaskTypeBlobDelete),
	}
	BackgroundTaskTypeString = "[" + strings.Join(BackgroundTaskTypes, ", ") + "]"
)

func addCmdBackground(cmd *grumble.Command) {
	backgroundCommand := &grumble.Command{
		Name:     "background",
		Help:     "background task switch control tools",
		LongHelp: "Background task switch control for clustermgr, currently supported: " + BackgroundTaskTypeString,
	}
	cmd.AddCommand(backgroundCommand)

	backgroundCommand.AddCommand(&grumble.Command{
		Name:     "status",
		Help:     "show status of a background task switch",
		LongHelp: "Show status of a specific background task type, currently supported: " + BackgroundTaskTypeString,
		Args: func(a *grumble.Args) {
			a.String("task", "background task type")
		},
		Run: cmdListBackgroundStatus,
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})
	backgroundCommand.AddCommand(&grumble.Command{
		Name:     "enable",
		Help:     "enable background task",
		LongHelp: "Enable a specific background task type, currently supported: " + BackgroundTaskTypeString,
		Args: func(a *grumble.Args) {
			a.String("task", "background task type")
		},
		Flags: clusterFlags,
		Run: func(c *grumble.Context) error {
			return cmdEnableDisableBackgroundTask(c, true)
		},
	})
	backgroundCommand.AddCommand(&grumble.Command{
		Name:     "disable",
		Help:     "disable background task",
		LongHelp: "Disable a specific background task type, currently supported: " + BackgroundTaskTypeString,
		Args: func(a *grumble.Args) {
			a.String("task", "background task type")
		},
		Flags: clusterFlags,
		Run: func(c *grumble.Context) error {
			return cmdEnableDisableBackgroundTask(c, false)
		},
	})
}

func cmdEnableDisableBackgroundTask(c *grumble.Context, en bool) error {
	key := c.Args.String("task")

	found := false
	for _, kind := range BackgroundTaskTypes {
		if kind == key {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("Unsupported background task type: %s", key)
	}

	value := "true"
	act, acted := "enable", "enabled"
	if !en {
		value = "false"
		act, acted = "disable", "disabled"
	}

	cli := newCMClient(c.Flags)
	ctx := common.CmdContext()
	oldV, err := cli.GetConfig(ctx, key)
	if err != nil {
		if rpc.DetectStatusCode(err) == http.StatusNotFound {
			oldV = notSet
		} else if !common.Confirm("To " + act + " background task of : " + common.Loaded.Sprint(key) + " ?") {
			return err
		}
	} else if oldV == value {
		fmt.Printf("Background task of type `%s` has already been %s.", key, acted)
		return nil
	}

	fmt.Printf("Current status of background task of `%s`:\n", key)
	showConfig(key, oldV, true)

	if common.Confirm(fmt.Sprintf(
		"To %s background task `%s` from `%s` --> `%s` ?", act, common.Loaded.Sprint(key),
		common.Danger.Sprint(oldV), common.Normal.Sprint(value))) {
		err := cli.SetConfig(ctx, &clustermgr.ConfigSetArgs{
			Key:   key,
			Value: value,
		})
		if err != nil {
			if e, ok := err.(*rpc.Error); ok {
				return fmt.Errorf(e.Code)
			}
		}
		return err
	}
	return nil
}

func cmdListBackgroundStatus(c *grumble.Context) error {
	key := c.Args.String("task")

	found := false
	for _, kind := range BackgroundTaskTypes {
		if kind == key {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("Unsupported background task type: %s", key)
	}

	cli := newCMClient(c.Flags)
	ctx := common.CmdContext()
	verbose := flags.Verbose(c.Flags)

	fmt.Printf("Current status of background task `%s`:\n", key)
	value, err := cli.GetConfig(ctx, key)
	if err != nil {
		if rpc.DetectStatusCode(err) == http.StatusNotFound {
			showConfig(key, notSet, verbose)
			return nil
		}
		return err
	}
	showConfig(key, value, verbose)
	return nil
}
