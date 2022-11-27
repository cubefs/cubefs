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

package scheduler

import (
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdVolumeInspectCheckpointTask(cmd *grumble.Command) {
	inspectCommand := &grumble.Command{
		Name:     "checkpoint",
		Help:     "inspect checkpoint tools",
		LongHelp: "inspect checkpoint tools for scheduler",
	}
	cmd.AddCommand(inspectCommand)

	inspectCommand.AddCommand(&grumble.Command{
		Name:  "get",
		Help:  "get inspect checkpoint",
		Run:   cmdGetInspectCheckpoint,
		Flags: clusterFlags,
	})
	inspectCommand.AddCommand(&grumble.Command{
		Name:  "set",
		Help:  "set inspect checkpoint",
		Run:   cmdSetInspectCheckpoint,
		Flags: clusterFlags,
		Args: func(a *grumble.Args) {
			a.Uint64("volume_id", "set the start volume id")
		},
	})
}

func cmdGetInspectCheckpoint(c *grumble.Context) error {
	clusterMgrCli := newClusterMgrTaskClient(getClusterID(c.Flags))
	ck, err := clusterMgrCli.GetVolumeInspectCheckPoint(common.CmdContext())
	if err != nil {
		return err
	}
	fmt.Println(common.RawString(ck))
	return nil
}

func cmdSetInspectCheckpoint(c *grumble.Context) error {
	clusterMgrCli := newClusterMgrTaskClient(getClusterID(c.Flags))
	vid := proto.Vid(c.Args.Uint64("volume_id"))
	if !common.Confirm(fmt.Sprintf("set volume inspect checkpoint: %d ?", vid)) {
		return nil
	}
	err := clusterMgrCli.SetVolumeInspectCheckPoint(common.CmdContext(), vid)
	if err != nil {
		return err
	}
	fmt.Println("set volume inspect checkpoint successfully")
	return nil
}
