// Copyright 2018 The CubeFS Authors.
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
	"github.com/cubefs/cubefs/cli/api"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdInodeUse   = "inode [command]"
	cmdInodeShort = "Manage inode"
)

func newInodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdInodeUse,
		Short: cmdInodeShort,
	}
	cmd.AddCommand(
		newInodeInfoCmd(client),
		newInodeListCmd(client),
	)
	return cmd
}

const (
	cmdInodeListShort = "List all inodes in a meta partition"
	cmdInodeInfoShort = "Get inode info by inode id"
)

func newInodeListCmd(client *master.MasterClient) *cobra.Command {
	var optMetaAddr string
	var pid uint64
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: cmdInodeListShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			info, err := api.NewMetaHttpClient(optMetaAddr, false).GetAllInodes(pid)
			if err != nil {
				return
			}
			stdout("[Inode List]\n")
			for _, v := range info {
				stdout("%v\n", v)
			}
		},
	}
	cmd.Flags().StringVar(&optMetaAddr, "addr", "", "Meta server address")
	cmd.Flags().Uint64Var(&pid, "pid", 0, "Partition id")
	return cmd
}

func newInodeInfoCmd(client *master.MasterClient) *cobra.Command {
	var optMetaAddr string
	var pid uint64
	var inodeId uint64
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [inodeId]",
		Short: cmdInodeInfoShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			info, err := api.NewMetaHttpClient(optMetaAddr, false).GetInodeInfo(pid, inodeId)
			if err != nil {
				return
			}
			stdout("[Inode info]\n")
			stdout("%v\n", info)
		},
	}
	cmd.Flags().StringVar(&optMetaAddr, "addr", "", "Meta server address")
	cmd.Flags().Uint64Var(&pid, "pid", 0, "Partition id")
	cmd.Flags().Uint64Var(&inodeId, "inodeId", 0, "Inode id")
	return cmd
}
