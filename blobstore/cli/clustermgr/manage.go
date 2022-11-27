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
)

func addCmdManage(cmd *grumble.Command) {
	manageCommand := &grumble.Command{
		Name:     "cluster",
		Help:     "cluster tools",
		LongHelp: "cluster tools for clustermgr",
	}

	cmd.AddCommand(manageCommand)

	manageCommand.AddCommand(&grumble.Command{
		Name: "add",
		Help: "add raft member",
		Run:  cmdAddMember,
		Args: func(a *grumble.Args) {
			a.Uint64("peer_id", "peer id")
			a.String("host", "raft host addr")
			a.Int("member_type", "member type")
			a.String("node_host", "service host addr")
		},
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
		},
	})

	manageCommand.AddCommand(&grumble.Command{
		Name: "remove",
		Help: "remove raft member",
		Run:  cmdRemoveMember,
		Args: func(a *grumble.Args) {
			a.Uint64("peer_id", "the peer id to remove")
		},
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
		},
	})

	manageCommand.AddCommand(&grumble.Command{
		Name: "transfer",
		Help: "transfer leadership",
		Run:  cmdTransferLeadership,
		Args: func(a *grumble.Args) {
			a.Uint64("peer_id", "the peer id to be leader")
		},
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
		},
	})
}

func cmdAddMember(c *grumble.Context) error {
	cli := newCMClient(c.Flags)
	ctx := common.CmdContext()

	id := c.Args.Uint64("peer_id")
	host := c.Args.String("host")
	memberType := clustermgr.MemberType(c.Args.Int("member_type"))
	nodeHost := c.Args.String("node_host")

	memberArgs := &clustermgr.AddMemberArgs{
		MemberType: memberType,
		Host:       host,
		PeerID:     id,
		NodeHost:   nodeHost,
	}
	if !common.Confirm("confirm add?") {
		fmt.Println("command canceled")
		return nil
	}
	return cli.AddMember(ctx, memberArgs)
}

func cmdRemoveMember(c *grumble.Context) error {
	cli := newCMClient(c.Flags)
	ctx := common.CmdContext()

	id := c.Args.Uint64("peer_id")
	if !common.Confirm("confirm remove?") {
		fmt.Println("command canceled")
		return nil
	}
	return cli.RemoveMember(ctx, id)
}

func cmdTransferLeadership(c *grumble.Context) error {
	cli := newCMClient(c.Flags)
	ctx := common.CmdContext()

	id := c.Args.Uint64("peer_id")

	if !common.Confirm("confirm transfer leadership?") {
		fmt.Println("command canceled")
		return nil
	}

	return cli.TransferLeadership(ctx, id)
}
