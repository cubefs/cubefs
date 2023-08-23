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
	"github.com/spf13/cobra"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
)

const (
	cmdUidUse        = "uid [COMMAND]"
	cmdUidShort      = "Manage cluster volumes uid black list"
	cmdUidAddShort   = "add volume uid size"
	cmdUidDelShort   = "del volume uid"
	cmdUidListShort  = "list volume uid info list"
	cmdUidCheckShort = "check volume uid"

	//uid op
	CliUidAdd       = "add"
	cliUidListShort = "list"
	CliUidDel       = "del"
	CliUidCheck     = "check"

	//param
	uidAll = "all"
)

func newUidCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdUidUse,
		Short:   cmdUidShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"uid"},
	}
	cmd.AddCommand(
		newUidAddCmd(client),
		newUidDelCmd(client),
		newUidListCmd(client),
		newUidCheckCmd(client),
	)
	return cmd
}

func newUidAddCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliUidAdd,
		Short:   cmdUidAddShort,
		Aliases: []string{"add"},
		Args:    cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(args[0], args[1], util.UidAddLimit, args[2]); err != nil || !uidInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newUidListCmd(client *master.MasterClient) *cobra.Command {
	var (
		optKeyword string
		uidListAll bool
	)
	var cmd = &cobra.Command{
		Use:     cliUidListShort,
		Short:   cmdUidListShort,
		Aliases: []string{"list"},
		Args:    cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 2 {
				if args[1] == uidAll {
					uidListAll = true
				}
			}
			var volumeName = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(volumeName, "", util.UidLimitList, ""); err != nil || !uidInfo.OK {
				stdout("UidOperation return \n")
				return
			}
			stdout("%v\n", volumeUidTableHeader)
			for _, info := range uidInfo.UidSpaceArr {
				if !uidListAll && info.Enabled == false {
					continue
				}
				stdout("%v\n", formatUidInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newUidDelCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliUidDel,
		Short:   cmdUidDelShort,
		Aliases: []string{"del"},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(args[0], args[1], util.UidDel, ""); err != nil || !uidInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newUidCheckCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliUidCheck,
		Short:   cmdUidCheckShort,
		Aliases: []string{"check"},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(args[0], args[1], util.UidGetLimit, ""); err != nil || !uidInfo.OK {
				return
			}
			stdout("%v\n", volumeUidTableHeader)
			for _, info := range uidInfo.UidSpaceArr {
				stdout("%v\n", formatUidInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}
