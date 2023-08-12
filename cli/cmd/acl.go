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

package cmd

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/spf13/cobra"
)

const (
	cmdAclUse        = "acl [COMMAND]"
	cmdAclShort      = "Manage cluster volumes acl black list"
	cmdAclAddShort   = "add volume ip"
	cmdAclDelShort   = "del volume ip"
	cmdAclListShort  = "list volume ip list"
	cmdAclCheckShort = "check volume ip"

	//acl op
	CliAclAdd       = "add"
	cliAclListShort = "list"
	CliAclDel       = "del"
	CliAclCheck     = "check"
)

func newAclCmd(client master.IMasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdAclUse,
		Short:   cmdAclShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"acl"},
	}
	cmd.AddCommand(
		newAclAddCmd(client),
		newAclDelCmd(client),
		newAclListCmd(client),
		newAclCheckCmd(client),
	)
	return cmd
}

func newAclAddCmd(client master.IMasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliAclAdd,
		Short:   cmdAclAddShort,
		Aliases: []string{"add"},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(args[0], args[1], util.AclAddIP); err != nil || !aclInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newAclListCmd(client master.IMasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     cliAclListShort,
		Short:   cmdAclListShort,
		Aliases: []string{"list"},
		Args:    cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var volumeName = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(volumeName, "", util.AclListIP); err != nil || !aclInfo.OK {
				stdout("AclOperation return \n")
				return
			}
			stdout("%v\n", volumeAclTableHeader)
			for _, info := range aclInfo.List {
				stdout("%v\n", formatAclInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newAclDelCmd(client master.IMasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliAclDel,
		Short:   cmdAclDelShort,
		Aliases: []string{"del"},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(args[0], args[1], util.AclDelIP); err != nil || !aclInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newAclCheckCmd(client master.IMasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliAclCheck,
		Short:   cmdAclCheckShort,
		Aliases: []string{"check"},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(args[0], args[1], util.AclCheckIP); err != nil || !aclInfo.OK {
				return
			}
			stdout("%v\n", volumeAclTableHeader)
			for _, info := range aclInfo.List {
				stdout("%v\n", formatAclInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}
